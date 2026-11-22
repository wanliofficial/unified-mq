# 会话状态复制 Plan 2:透明故障接管 设计

| 项 | 内容 |
|---|---|
| 任务 | MQ-17 产品级清单「集群会话副本的故障注入证明」Plan 2 |
| 关联 | [Plan 1 设计](2026-11-22-session-state-replication-design.md)（共享上下文);Plan 1 实现记录 [plan1-notes](2026-11-22-session-state-replication-plan1-notes.md);[feature-comparison §4.2 #5/#6](../mqtt-feature-comparison.md);ADR-021 §D5（消息复制）、ADR-034 D5/D7（跨节点路由 / 迁移守卫）、ADR-016（指标冻结键集） |
| 日期 | 2026-11-22 |
| 状态 | 待批准 |

## 0. 本文与 Plan 1 spec 的关系

Plan 1（已交付合并 main,HEAD `5112707`）实现了会话状态的**复制管线**:写侧挂钩、`apply_replica/1`、4 张会话表 public 化、多节点 CT 证明副本落盘。它**没有**实现故障接管——复制有了,但「owner 崩溃后谁接管、怎么接管、怎么证明」留待 Plan 2。

本文只设计 Plan 2 的**新增机制**:崩溃检测、所有权迁移（claim)、conn_fsm 就地续传、故障注入 CT。复制管线本身沿用 Plan 1,不重做。凡本文与 Plan 1 spec §3.5/§4 的流程图冲突处,以本文为准,并在 §7 记录偏差（Plan 1 spec 历史正文不改)。

## 1. 目标（Plan 2 部分）

owner 节点崩溃时,持有该客户端**活连接**的节点 N 从**本地副本**就地恢复会话并续传,客户端在 MQTT 层**零感知**（无新 CONNECT、无 CONNACK、TCP 流不中断）。所有权粒度为**分区级**（见 §3),复用 ra 现有元数据/纪元/fencing。

## 2. 现状（已核实,非猜测）

- **ra 元数据机是分区键的**。`broker_cluster_meta_machine`（`broker_cluster_meta_machine.erl:23`）状态 = `#{partitions => #{partition() => meta()}}`,每分区 meta 含 `owner / epoch / replicas / state / index`。纪元单调性在机内强制:每条变更命令过 `E >= Epoch` 守卫,陈旧命令回 `{error, {stale_epoch, E, Epoch}}`（`broker_cluster_meta_machine.erl:83-94`)。
- **meta 不监听 nodedown、不 auto-rebalance**。`broker_cluster_meta` 是 admin-driven `join/leave`（`broker_cluster_meta.erl:65`）。Plan 1 spec §3.5 原本假设「现有成员/纪元机制发现崩溃」——**不成立**,崩溃检测是全新机制。
- **`conn_fsm` 无跨节点感知**。`mqtt_conn_fsm.erl:868` 本地调 `broker_subscription:session(ClientId)` + `attach/3`;无节点路由、无 owner 监视（`monitor_node`)。它假设自己与会话同节点。
- **`session/1` 只读本地 ETS**。`broker_subscription.erl:359` 的 `online` 是 `is_process_alive(本地 pid)`,无跨节点视图。
- **无 session→owner / session→分区映射**。Plan 1 复制无路由键（`broker_cluster_session_rep.erl` best-effort 广播到所有副本集——plan1-notes 债 #1)。
- **dist-CT 有 kill 手法无目标用例**。`broker_cluster_cross_node_dist_SUITE.erl` 用 `erlang:exit(Pid,kill)` / `peer:stop(Pid)`,但没有「kill owner 节点→会话在 N 存活」用例。

**结论**:Plan 2 全部机制都是新建,不是接线。

## 3. 所有权粒度:分区级

会话按 `partition_of(ClientId)` 确定性散列归属一个分区（§5 补映射,还 plan1-notes 债 #1)。该分区的 ra owner 即为其所有会话的权威 owner。owner 崩溃 → 该分区进入迁移 → 持连副本节点事后 claim 整个分区所有权,其上的会话就地续传。选分区级是因为:ra 现有纪元/fencing/ADR-034 D7 本就建在分区粒度上,会话级需另建一套 fencing/快照,改动面大且与 Plan 1 的按分区复制模型不一致（YAGNI)。

## 4. 核心机制:claim（方案 A:ra 选 owner,持连节点事后认领）

**触发链（owner 节点 O 崩溃,客户端活连接在节点 N）:**

1. 各 ra 成员节点 `monitor_node` 所有 peer 节点;N 收到 `{nodedown, O}`。
2. ra 自身故障检测（O 是死亡 voter,失联）触发:提交**新 epoch** + 按副本固定序选新分区 owner。此步纯机器决策,**不感知连接**。需在 meta 层新增 nodedown 触发（今日仅 admin `join/leave`)。
3. N 发现自己持连的会话里,有归属该分区、且 N 持有其本地副本者 → conn_fsm 向 ra 提交 `{claim, P, N, NewEpoch}`。
4. `broker_cluster_meta_machine:apply` 校验 epoch 单调 + 持副本守卫 → 写 `owner := N, state := active`。**ra 串行 apply 保证全局唯一 owner**;旧 owner O 重上线后其陈旧写入被 epoch fencing 拒（spec §3.5 既有 fencing)。

**唯一性**由 ra 命令的单线程 apply 提供;**连接存活判断**完全留在本地（conn_fsm),不进 ra 排序——贴合「ra 不感知连接」的决策。

**claim 命令新分支（扩 `broker_cluster_meta_machine:apply`,不推翻现有):**

```erlang
{claim, P, Node, Epoch} ->
    case maps:find(P, Ps) of
        {ok, #{epoch := E} = M} when E < Epoch, is_member(Node, maps:get(replicas, M, [])) ->
            %% ADR-034 D7 落地:认领者必须已持该分区副本,否则拒绝。
            M1 = M#{owner => Node, epoch => Epoch, state := active, index := Index},
            broker_cluster_cache:put(P, M1),
            {State#{partitions := Ps#{P => M1}}, {ok, Epoch}, []};
        {ok, #{epoch := E}} ->
            {State, {error, {stale_epoch, E, Epoch}}, []};
        error ->
            {State, {error, not_found}, []}
    end.
```

要点:
- `is_member(Node, Replicas)` 即 D7「迁移目标必须已持副本」在会话分区上的落地。
- 现有 `assign/4` 的 `E >= Epoch` 守卫天然防乱序,claim 复用同一不变量。
- 陈旧的 `{claim,...}`（epoch 落后）被拒,与今日 `stale_epoch` 语义一致。

## 5. 会话→分区映射（还 plan1-notes 债 #1）

**已核实的关键约束**:ra 的分区是 `broker_rebalance` 的 `decide/3` 动态、稀疏创建的 `non_neg_integer()` 键（`broker_rebalance.erl:123-128`),**没有固定的稠密分区总数**——不存在 `0..N-1` 的空间可供散列。因此「`partition_of(ClientId)` 对齐分区总数」不成立,需先定义**会话分区空间**本身。

Plan 2 引入一个**固定数量的会话分区**（建议 env `session_partition_count`,默认取一个与集群规模匹配的小值,如节点数),与 `broker_rebalance` 的元数据分区是**两套独立空间**:

- **会话分区**:由 `session_partition_count` 定义稠密空间 `0..C-1`,`partition_of(ClientId) = erlang:phash2(ClientId, C)`。Plan 1 复制据此把会话状态复制到「该会话分区的副本集」;§4 的 claim 据此定位分区。副本集 = 该会话分区在 ra 中对应元数据的 `replicas`（或独立的会话副本组,留 §12 plan 阶段定）。
- **元数据分区**:`broker_rebalance` 现有分区,继续承载 owner/epoch/fencing,**不动**。

> 设计取舍:两套空间避免把会话散列绑死在动态元数据分区上（后者随 rebalance 变,会导致会话分区归属漂移、fencing 抖动)。会话分区空间固定,`phash2` 稳定,epoch fencing 兜底任何陈旧写入。

会话复制因此从 Plan 1 的「广播所有副本集」收紧为「该会话分区的副本集」,顺带提升复制精度。`session_partition_count` 变更（如扩分区）会使会话重新散列,由 epoch fencing 兜底陈旧写入。

## 6. `mqtt_conn_fsm` 改造点

- **现状**:`mqtt_conn_fsm.erl:868` 本地调 `session/1`+`attach/3`,无节点路由、无 owner 监视。
- **新增**:
  - conn_fsm 持 `(ClientId → 分区 P → owner 节点)` 视图,来源 = 本地 `broker_cluster_cache` 读 + 对 owner 节点 `monitor_node/1`。
  - 收到 `{nodedown, O}` 时,筛出「owner=O 且本节点持连且持副本」的会话,提交 `{claim, P, N, NewEpoch}`;claim 成功后**重读本地 `broker_subscription:session(ClientId)`**（N 已是 owner 且持有副本）→ 复用 `attach/3`（`broker_subscription.erl:209`,takeover 语义）就地续传,从本地游标恢复。
  - 客户端零感知:无新 CONNECT、无 CONNACK、TCP 流不中断。
- **退化**:`session_replication` 开关关闭时,conn_fsm 不装 monitor、不发 claim,逐字节退回今日行为（不变量 I2)。该 env 同时门控 Plan 1 钩子与 Plan 2 接管,默认 `false`。

## 7. 与 Plan 1 spec 的偏差（按文档惯例,历史正文不改）

Plan 1 spec §3.5 及其 §4 数据流图把「持连优先」画进 **ra 候选排序内**（「候选排序注入持连优先 → 选 N」)。本轮经 brainstorming 决策改为:**ra 按副本固定序选 owner（不感知连接）→ 持连副本节点事后 claim**。Plan 1 spec 正文不划改,在此追加带日期批注说明修订:

> 【2026-11-22 批注】Plan 2 接管机制由「ra 候选排序内持连优先」修订为「ra 按副本序选 owner + 持连节点事后 claim」(见本设计 §4)。理由:连接存活信息不进 ra 排序,避免把节点级 liveness 拉回集群元数据;ra 串行 apply 仍保证唯一 owner,连接判断留在本地 conn_fsm。Plan 1 复制管线不受影响。

## 8. 不变量

- **I5（claim 持副本）**:任何 `{claim, P, Node, Epoch}` 成功的前提是 `Node` 已在 P 的 `replicas` 集中（扩展 ADR-034 D7 到会话分区）。
- **I6（纪元 fencing）**:旧 owner 重上线后,其 epoch 落后的会话写入被 `broker_cluster_meta_machine` 的 `E >= Epoch` 守卫拒绝（复用 spec §3.5)。
- **I7（透明性判据）**:接管期间客户端无新 CONNECT / 无 CONNACK（CT `session_survives_owner_crash` 实证;与 I4 同)。
- **I1–I4（Plan 1）**:复制入口分离、默认关闭逐字节退化、迁移目标持副本、透明性——全部沿用。

## 9. 故障注入测试

新增 `broker_session_replica_SUITE`（OTP 29.1.2,`port => 0`,`rebar3 ct` 前清 `_build/test/extras`)。沿用 `broker_cluster_cross_node_dist_SUITE` 已验证手法（`peer:stop/1` + `erlang:exit(Pid,kill)`),不引入新混沌框架（YAGNI)。spec §6 五用例:

1. **`session_survives_owner_crash`** — 客户端连 N、订阅、收消息建游标;kill owner O;断言 `session(ClientId)` 仍返回订阅集 + 游标、客户端**未重连**却续收新消息且游标不回退已确认位。
2. **`subscribe_not_lost_on_crash`** — 多数派 ack 前 kill 一半副本;订阅要么完整存在要么整体不存在（无半截行）。
3. **`inflight_dedup_preserved`** — QoS2 in-flight 建好后 kill owner;接管节点 inflight 恢复、重复投递被去重。
4. **`stale_owner_fenced`** — O 重上线后尝试写旧会话;断言被纪元 fencing 拒。
5. **`single_node_unchanged`** — 无 session_replicator 钩子时行为逐字节同今日（回归保护)。

## 10. 指标（ADR-016 冻结键集）

Plan 2 复用 Plan 1 已注册的 `cluster_session_replication_latency_us` / `cluster_session_replication_failed`,新增接管侧冻结键建议:`cluster_session_takeover_total`（按结果 `ok / stale_epoch / no_replica` 分标签)、`cluster_session_takeover_latency_us`。经 `broker_metrics` `init` 注册。

## 11. 错误处理与边界

- **claim 时本节点已非持副本**（replicas 不含 N):`{error, no_replica}`,conn_fsm 不接管,会话留给后续重连/其它副本——诚实失败而非误接管。
- **分区多数派缺失**:ra 无法提交新 epoch,claim 超时;会话留在副本只读态,等 quorum 恢复（同 spec §5「多数派未达」)。
- **多节点同时持连同分区**（N 与 M 都是 P 副本且都持 P 的客户端连接):两者都提交 claim,ra 串行 apply 取先到者,败者见 `owner =/= 自己` 放弃接管,无双写。
- **幂等**:claim 是 epoch 单调写,重复提交同 epoch 回 `stale_epoch`,无副作用。
- **RPO**:接管是本地副本读,订阅/inflight RPO≈0;游标受 Plan 1 异步复制窗口约束。

## 12. 留给 plan 阶段的未决点

- 会话分区的副本集是「复用元数据分区的 `replicas`」还是「独立的会话副本组」——取决于 §5 两套空间的最终落地,plan 阶段结合 `broker_rebalance` 现状定。
- `session_partition_count` 的默认值与集群规模（节点数 / 副本因子）的取法。
- nodedown 触发迁移的触发面与去抖（避免网络抖动误判崩溃,与 ra 自身故障检测的关系）。
