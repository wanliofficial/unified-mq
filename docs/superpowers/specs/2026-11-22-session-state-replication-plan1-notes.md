# 会话状态复制 Plan 1 实现记录

| 项 | 内容 |
|---|---|
| 范围 | Plan 1（共 2 个 Plan）：MQ-17「集群会话副本的故障注入证明」的**复制管线**部分 |
| 关联 spec | [session-state-replication-design](./2026-11-22-session-state-replication-design.md) |
| 关联 plan | [session-state-replication-plan1](../plans/2026-11-22-session-state-replication-plan1.md) |
| 关联缺口 | [MQ-17](../compatibility/unifiedmq-mqtt-family-gap.md)（MQ-17 行，2026-11-22 批注） |
| 关联对比 | [mqtt-feature-comparison §4.1](../mqtt-feature-comparison.md)（P0 集群级 durable session 复制条，2026-11-22 进展注） |
| 日期 | 2026-11-22 |
| 状态 | Plan 1 已交付（管线 + 测试，默认关闭）；**透明接管与故障注入证明属 Plan 2** |

## 0. 一句话摘要

Plan 1 只交付**会话状态复制管线**：订阅 / inflight / 游标作为会话 Op 经 `apply_replica/1` 复制到副本节点的本地 ETS 表，**默认关闭**（env `broker_cluster, session_replication=false`），关闭时单节点行为逐字节不变（不变量 I2）。**透明故障接管 + 故障注入 harness（SIGKILL / 分区下「会话/订阅不丢」的证明）不在 Plan 1**，是 Plan 2。

## 1. 交付清单

### 1.1 指标（`broker_metrics` 冻结键集，ADR-016）

`key_specs/0` 新增两个闭集冻结键（`broker_metrics.erl:436-437`）：

- `cluster_session_replication_latency_us` — histogram，复制往返延迟观测。
- `cluster_session_replication_failed` — counter，多数派未达时 +1。

### 1.2 接收侧 `broker_subscription_worker:apply_replica/1`

签名 `apply_replica(session_op()) -> ok`（`broker_subscription_worker.erl:320`）。**纯写本地 ETS**——不跑所有权守卫、不通知订阅者、不注册 Router、不写权威磁盘（同 `broker_queue:append_replica/1` 契约，不变量 I1）。这是**唯一**一处刻意不经 worker gen_server 的写入：发起侧以 `rpc:call(Node, ?MODULE, apply_replica, [Op])` 投递，本函数在 rpc 调用者进程里执行。

- 会话 Op 协议（`session_op()`，`:84`）：

  ```erlang
  -type session_op() ::
      {subscribe, ClientId :: binary(), SubRef :: term(), Info :: map()}
    | {unsubscribe, ClientId :: binary(), SubRef :: term()}
    | {inflight, ClientId :: binary(), Term :: term()}
    | {cursor, SubRef :: term(), PartitionId :: term(), Row :: term()}.
  ```

- 以整键覆盖写，幂等（跨节点 rpc 可能重复投递，结果一致）；畸形 Op 直接崩溃，由调用侧 rpc 承担失败语义。
- **表访问放宽**：四张会话表 `?ENTITIES / ?MEMBERS / ?CURSORS / ?INFLIGHT` 从 `protected` 改为 `public`（`:1547-1561`），以容纳跨进程 `apply_replica` 写入；副本只落非 owner 节点且只覆盖整键，故不会有两个写者同时写同一行。**`?ATTACHMENTS` 保持 `protected`**——它持本地连接 pid + monitor ref，不在 Op 协议内，public 会让外来进程塞进被守卫信任的 pid。

### 1.3 发起侧 `broker_cluster_session_rep`（新模块，`broker_cluster`）

仿 `broker_cluster_rep`（`broker_cluster_session_rep.erl`）：

- `replicate_session/2,3`（`:12,20`）— 按单一分区查副本集、并行 `rpc:call`、统计多数派 ack（`Needed = length div 2 + 1`），未达 `{error, {replication_failed, Acks, Needed}}` + `inc(cluster_session_replication_failed)`，成功观测 latency。`not_found` / 单节点副本集 → `ok`（单机默认，无元数据即无复制）。供 Plan 2 精确路由使用，并被本模块 4 个单机 CT 覆盖。
- `remote_apply/1`（`:45`）— 副本节点入口，内部转调 `apply_replica/1`，catch 后回 `{error, apply_replica_crashed}`。
- `replicate_session_of/1`（`:58`）— **钩子本体（Plan 1 安装的就是它）**：`spawn` 一个进程做 `replicate_session_best_effort/1`（fire-and-forget），调用方进程永不阻塞在跨节点多数派往返上，恒返回 `ok`。
- `replicate_session_best_effort/1`（`:69`）— Plan 1 对「会话→分区」路由的近似：读 `broker_cluster:partition_view()`，对快照里每个含 `replicas` 的分区各发一遍 `replicate_session/3`。无元数据（空镜像）即单机默认，安全 no-op。

### 1.4 写侧挂钩（`broker_subscription_worker`，单一写入者在**调用者进程**内咨询钩子）

钩子是 `{broker_subscription_worker, session_replicator}` persistent_term 项（`:187`）；absent 时全部复制调用退化为零开销 no-op（不变量 I2）。

- `subscribe/4`（SYNC）— 提交落表/落盘后 `maybe_replicate_subscribe/2`（`:269`）构造 `{subscribe, ClientId, SubRef, Info}` 经 `replicate_sync/1` 复制；钩子 absent 时先门控、不查表不 spawn。
- `unsubscribe/2`（SYNC）— `maybe_replicate_unsubscribe/2`（`:289`）构造 `{unsubscribe, ClientId, {exclusive, ClientId, Filter}}`（与 `apply_replica` 拆除路径命名一致）；本地已拆且不可回滚，故钩子报错只 `log_warning`、调用方仍得 `ok`。
- `inflight/2`（ASYNC）— `maybe_replicate_inflight/2` → `replicate_inflight_async/1`（`:250`）fire-and-forget；conn_fsm ~20ms 调用并忽略结果，故不门控任何 SUBACK。
- 游标 flush（ASYNC）— `replicate_cursor_async/1`（`:210`）每刷一个订阅 spawn 一个进程，发每行一个 `{cursor, SubRef, PartitionId, Row}`；钩子 absent 时连 spawn 都不发生。

### 1.5 安装器（`broker_cluster`）

- `install_session_hook/0`（`broker_cluster.erl:232`）— 由 env `broker_cluster, session_replication`（默认 `false`）控制：开启时装入 `{broker_subscription_worker, session_replicator} = fun(Op) -> broker_cluster_session_rep:replicate_session_of(Op) end`；关闭时确保无残留（`erase`）。
- 在 `install_hooks/0`（`:185`）调用；`uninstall_hooks/0`（`:300`）亦 erase 该项。

## 2. 测试与验证

| SUITE | 节点 | 用例数 | 证明了什么 |
|---|---|---|---|
| `broker_subscription_worker_SUITE` | 单机 | 11 | `apply_replica/1` 四种 Op 的幂等落表（subscribe/idempotent、unsubscribe 清 entity+cursor+member、inflight、cursor）；钩子语义：无钩子 subscribe 行为不变（I2）、有钩子 subscribe SYNC、inflight/游标 ASYNC、钩子报错时 subscribe 失败而 unsubscribe 不失败 |
| `broker_cluster_session_rep_SUITE` | 单机（hermetic） | 4 | `single_node_is_ok`、`no_replicas_is_ok`（无元数据→ok）、`remote_apply_ok`（副本侧 apply）、`majority_failure_counts_acks`（多数派未达计数） |
| `broker_cluster_session_rep_dist_SUITE` | **真实 2 节点** | 2 | `session_op_reaches_replica_when_enabled`：owner 经真实钩子路径 subscribe，断言 entity/member 行落到副本本地表；`no_replication_when_disabled`：开关关闭时钩子为 undefined，副本本地表保持空 |

多节点 dist SUITE 用 `peer:start_link` 起真节点、共享同一 ownership 视图（partition 0 owner=N2、副本含 N1），是 Plan 1 端到端的硬证据：**开启后会话 Op 确实到达副本本地表，关闭后不到达**。

## 3. Plan 1 / Plan 2 边界（诚实记录）

> 以下为 Plan 1 的已知边界，**非缺陷**，是 Plan 1 可独立交付、为 Plan 2 铺路的正确切口。

1. **无「会话→分区」精确映射**：发起侧（`replicate_session_of/1` → `replicate_session_best_effort/1`）没有「client X 的会话归哪个节点所有」的映射，故对快照里**每个**分区的副本集各发一遍（best-effort、节点级去重由 `apply_replica` 幂等覆盖写保证）。`replicate_session/3`（按单一分区）保留给 Plan 2 精确路由。**精确会话归属 + 路由是 Plan 2 核心。**

2. **SYNC 多数派 ack 在 Plan 1 退化为 fire-and-forget（重要诚实说明）**：spec §3.4 设计是订阅/unsubscribe **同步**复制、多数派 ack 后才回 ACK；worker 的 `replicate_sync/1` 也确实会尊重钩子返回的 `{error,_}`。但 Plan 1 实际安装的钩子本体是 `replicate_session_of/1`，它**恒返回 `ok`**（spawn 异步投递）。因此 worker 里 SYNC 的错误分支（`subscribe_fails_when_hook_errors` 测的是钩子直接返回 error 的路径，而真实安装的钩子不会返回 error；`maybe_replicate_unsubscribe/2` 的 warning）在默认配置下是**惰性的**——订阅/unsubscribe 实际上**不是**多数派持久化的。恢复真正的同步多数派 ack（让安装钩子回传 `replicate_session/3` 的结果，或改由 worker 直接同步等多数派）是 Plan 2。

3. **订阅失败但本地订阅已建立的偏差**：若未来 SYNC 订阅钩子报错，subscribe 返回 `{error,_}`，但本地订阅已提交且**不回滚**。Plan 1 可接受，Plan 2 解决。

4. **透明接管 + 故障注入证明 = Plan 2（需先 brainstorm）**：OTP 不支持把活跃 TCP socket 跨节点迁移，故 Plan 2 是「连接/会话解耦」——TCP 留在原节点，把会话的持久状态复制到副本，owner 崩溃 → 持连节点从本地副本恢复续传（spec §3.1/§3.5/§3.6）。需要会话归属 + 持连优先迁移 + conn_fsm 重连机制，以及 SIGKILL/分区 harness 的「节点丢失后 durable 消息与订阅集合不丢」证明（spec §6）。

5. **Plan 1 复制默认关闭**：env `session_replication=false`（`install_session_hook/0` 门控），单节点行为逐字节不变（不变量 I2）。这是「先铺管线、不改变现状」的刻意选择。

## 4. 关联

- 设计 spec：[session-state-replication-design](./2026-11-22-session-state-replication-design.md)（Approach A：会话状态纳入现有 owner/replica/fencing 复制体系）。
- 实现 plan：[session-state-replication-plan1](../plans/2026-11-22-session-state-replication-plan1.md)。
- 缺口表：[unifiedmq-mqtt-family-gap.md](../compatibility/unifiedmq-mqtt-family-gap.md) MQ-17 行（2026-11-22 已追加 Plan 1 进展批注，**未**划掉「集群会话副本的故障注入证明」整项——该项的故障注入证明部分属 Plan 2）。
- 特性对比：[mqtt-feature-comparison.md](../mqtt-feature-comparison.md) §4.1「P0 集群级 durable session 复制」条（2026-11-22 已追加进展注，未勾选 checkbox——SIGKILL harness 与透明接管仍待 Plan 2）。
- 既有 ADR-016/021/034 D5/D7 语义被扩展引用，未推翻。

> 文档惯例：历史实测记录正文不改，只追加带日期批注或划掉并注明日期。本文件为新增实现记录，既有文档仅追加 2026-11-22 批注。
