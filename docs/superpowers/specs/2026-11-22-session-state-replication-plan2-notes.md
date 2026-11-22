# 会话状态复制 Plan 2 实现记录

| 项 | 内容 |
|---|---|
| 范围 | Plan 2（共 2 个 Plan）：MQ-17「集群会话副本的故障注入证明」的**透明接管**部分 |
| 关联 spec | [session-state-replication-design](./2026-11-22-session-state-replication-design.md)（§8 已追加 2026-11-22 批注） |
| 关联 design | [session-state-replication-plan2-design](./2026-11-22-session-state-replication-plan2-design.md) |
| 关联 plan | [session-state-replication-plan2](../plans/2026-11-22-session-state-replication-plan2.md) |
| 关联缺口 | [MQ-17](../../compatibility/unifiedmq-mqtt-family-gap.md)（MQ-17 行） |
| 日期 | 2026-11-22 |
| 状态 | 已交付（默认关闭） |

## 0. 一句话摘要

Plan 2 把 Plan 1 的复制管线接到**接管**上：`mqtt_conn_fsm` 监视会话分区的 owner 节点，owner 崩溃时持连副本节点向 ra 提交 `{claim, P, N, Epoch}` 就地接管，客户端在 MQTT 层零感知（无新 CONNECT、无 CONNACK）。**默认关闭**（env `broker_cluster, session_replication=false`），关闭时行为逐字节不变（不变量 I2）。

## 1. 交付清单

### 1.1 会话→分区映射（还 Plan 1 债 #1）

新模块 `broker_cluster_session_owner`（`apps/broker_cluster/src/`）：

- `partition_of(ClientId, Count)` — `erlang:phash2/2`，确定性散列到 `[0, Count)`。
- `session_partition_count/0` — env `session_partition_count`，默认 **4**；对非法值（非正整数）回退默认，不让配置失误把分区非法化。
- `session_owner(Partition)` — 从**本地** `broker_cluster_cache` 读 owner（`{ok, Owner} | not_found`），数据路径绝不每 append 走 ra。

要点：会话分区空间**独立于** `broker_rebalance` 的动态元数据分区（spec §5/§12）。元数据分区由 rebalance 动态、稀疏创建，没有固定 `0..N-1` 空间可散列；若把会话绑在上面，rebalance 会让会话归属漂移、fencing 抖动。故两套编号并存，epoch fencing 兜底任何陈旧写入。

### 1.2 复制路由（按会话分区，不再广播）

`broker_cluster_session_rep:replicate_session_best_effort/1` 从 Plan 1 的「广播所有副本集」改为：

- `targeted_plan/1`（纯决策，已导出以可单测）→ 用 Op 的 ClientId 散列出会话分区 P，读 P 的副本集，返回 `{targeted, P, Replicas}`；
- cursor Op（无 ClientId）与未知 Op、无元数据、散列异常 → 返回 `broadcast`，退回 Plan 1 行为。

所有失败在该函数内捕获并降级，绝不外抛——调用方是 fire-and-forget 的 spawnee。

### 1.3 claim 命令

- `broker_cluster_meta_machine` 新增 `{claim, P, Node, Epoch}`，**双守卫**：epoch 单调（`is_integer(Epoch), E < Epoch`，同时要求整数，防非整数 epoch 把分区永久卡死）+ `lists:member(Node, replicas)`（ADR-034 D7「迁移目标必须已持副本」在会话分区落地）。
- `broker_cluster:claim/3` 是薄包装 + **结果分类指标**（见 1.6）。守卫在机内，包装层不加逻辑。
- 唯一性来源是 ra 的单线程串行 apply，不是本地判断。

### 1.4 崩溃发现

- `broker_cluster_nodedown:pick_new_owner/2` — 副本固定序里选第一个「非死亡节点、非本节点」的副本作新 owner，epoch+1；无存活副本 → `{error, no_alive_replica}`（quorum 缺失，等恢复）。
- `broker_cluster_nodedown:on_nodedown/1` — 遍历本地镜像里 `Owner =:= DeadNode` 的分区，逐个提交既有 `{migrate, ...}` 命令（复用，不新增命令型）；**每分区独立 try**，单分区失败不影响其它分区。
- `broker_cluster_nodedown_mon` — 真正 `monitor_node` 的 gen_server，由 `broker_cluster_sup` 启动，`session_replication` 门控：开 → 监视当前 ra 成员；关 → 不监视（I2）。收 `{nodedown, O}` 派发 handler，`{nodeup, _}` 重新拉成员并补挂，nodedown 时**显式 demonitor**（挂/撤成对的卫生）。`rearm/0` 供 `install_session_hook/0` 在开关切换后重挂。

### 1.5 conn_fsm 就地接管

- `maybe_monitor_owner/2` — attach 成功后按会话分区读 owner，主动 `monitor_node` owner（owner 是本节点则不装）；整段 try/catch 兜底，`broker_cluster` 未加载时降级为「不装 monitor」。
- `{nodedown, Owner}` 子句**结果感知**：claim 返回 `{ok, _}` 才把 `owner_node` 翻成本节点并记录 info；失败（`stale_epoch` / `not_a_replica` / 应用不可达）只 warning、**不乐观翻转**，连接保持（透明性优先）。
- `takeover_target/2`（已导出）— 纯判定「这次 nodedown 是否属于本连接」，与子句 guard 同义，作为可单测的显式意图断言。一个连接只持一个会话，故只需 `owner_part` / `owner_node` 一对字段，无批量模型。
- `terminate/3` 显式 demonitor，配对 attach 时装的 monitor。
- `{nodeup, _}` 吞掉（no-op）：owner flap 恢复不重装 monitor、不 re-attach（那需要一次全新 CONNECT，与透明性判据相悖）。

### 1.6 指标（ADR-016 冻结键集）

四个闭集冻结键，**一结果一键**（不是标签）：

| 键 | 类型 | 含义 |
|---|---|---|
| `cluster_session_takeover_ok` | counter | claim 成功（`{ok, Epoch}`） |
| `cluster_session_takeover_stale_epoch` | counter | `{error, {stale_epoch, E, Epoch}}` |
| `cluster_session_takeover_no_replica` | counter | `{error, {not_a_replica, Node}}` |
| `cluster_session_takeover_not_found` | counter | `{error, not_found}` |

`broker_cluster:claim/3` 在 `command/1` 返回后按结果 `inc` 对应键，`try/catch` 包一层保证指标永不失败 claim。

**不重复计 `cluster_owner_changes`**：`command/1` 已对任何 `{ok, N}` 回包计一次，claim 走同一条 `command/1`，故 claim 成功只被计一次。若在 `claim/3` 里再 inc，一次接管会被计两次。

### 1.7 故障注入 CT

`broker_session_replica_SUITE`（`apps/broker_cluster/test/`）五用例，跑**真 3 节点 ra 组**（`assign` → 真 kill owner → `claim` → fencing）：

| # | 用例 | 证明 |
|---|---|---|
| 1 | `session_survives_owner_crash` | 订阅 + 游标写在**濒死 owner**上，先轮询副本表确认已复制到达，再 kill owner；接管后订阅集不变、游标不回退（`After >= ReplicatedBefore`）、新 owner 可写 |
| 2 | `subscribe_not_lost_on_crash` | **反面证明**：复制关闭 → 副本无幽灵行，owner 死后诚实地 `not_found` |
| 3 | `inflight_dedup_preserved` | QoS2 inflight blob 先在副本落定，kill owner 后原样恢复 |
| 4 | `stale_owner_fenced` | claim 后旧 owner 用旧 epoch 写被 `{error, {fenced, _}}` 拒；新 owner 接受当前 epoch |
| 5 | `single_node_unchanged` | 开关关闭时无 `session_replicator` 钩子（I2 回归保护） |

用例 1/3 的会话状态**写在濒死 owner 上**、并**先轮询副本表证明经复制到达**才杀节点——这是与「数据本来就在存活节点上」的关键区别。变异验证：把复制改成 no-op 后，用例 1/3 因等不到副本行而超时失败（红），证明这两个用例真的在测复制 + 接管，而非测「没死的节点一直持有数据」。

## 2. 偏差记录

本节记录实现与设计/草案的偏离，**以代码为准**。

1. **`{error, no_replica}` vs `{error, {not_a_replica, Node}}`**：spec §11 写作 `{error, no_replica}`，实现（`broker_cluster_meta_machine:claim/6`）返回 `{error, {not_a_replica, Node}}`。指标键名取 `cluster_session_takeover_no_replica` 以对齐 spec 的**冻结键名**，但 code 的元组形状保留 `not_a_replica`。`claim/3` 的结果分类按**代码形状**匹配，不按文档。
2. **「按 result 分标签」→ 一结果一键**：spec §10 建议 `cluster_session_takeover_total` 带 `result` 标签，与 ADR-016 §16.2「禁止高基数标签」冲突（冻结键集是扁平的 atom 列表，只允许预定义键）。落地为四个独立键。同一理由下 spec §10 建议的 `cluster_session_takeover_latency_us` 本次**未实现**——claim 的 ra 往返延迟暂不观测，留待后续（不影响接管正确性）。
3. **「ra 候选排序内持连优先」→「ra 只串行 apply，不做任何选主」**：Plan 1 spec §3.5/§4 把持连优先画进 ra 候选排序；Plan 2 修订为**两条互不依赖的路径**，ra 完全不做候选排序或副本序选择，只负责把命令串行 apply——① 持连节点观察 `{nodedown, O}` 后提交 `{claim, P, N, Epoch}` 事后认领（主路径，**无任何副本序偏好**，判据只有 epoch 单调 + `N` ∈ `replicas`）；② `broker_cluster_nodedown:pick_new_owner/2` 在**本地**按副本固定序取第一个存活副本再提交 `{migrate, ...}`（兜底路径）。已按文档惯例追加 §8 带日期批注（Plan 1 spec 历史正文未改）。
4. **Test 6 的「cache-only `set_owner` + `claim`」不可行**：plan 草案 Task 6 想让测试只写 `broker_cluster_cache` 指定 owner 以获得确定性时序，但 `claim` 是一条穿过 `ra:process_command/3` 的**真 ra 命令**——机器状态里没有该分区就返回 `{error, not_found}`，cache-only 无法证明「谁被踢出、谁被选入、epoch 如何单调递增」。草案用的 `cursor/4` API 也不存在。实测改为**真 3 节点 ra 组**。
5. **`claims_to_make/3` 被删**：plan 草案 Task 5 设计了「owner 视图 `#{ClientId => {Partition, Owner}}` + 批量 `claims_to_make/3`」。Task 5 review 后删除——per-connection 模型（一个 conn_fsm 一个会话，单 `owner_part` / `owner_node`）已足够，批量视图是多余状态；判定收敛为 `takeover_target/2`。
6. **`broker_cluster:session_owner/1` 包装层被删**：Task 3 review 移除了该包装（层级倒置：`broker_cluster` 反向依赖 `broker_cluster_session_owner`，且两头都做规范化）。唯一访问器是 `broker_cluster_session_owner:session_owner/1`，conn_fsm 直接调它。
7. **Task 4 初版 `on_nodedown/1` 无调用者**：初版只写了模块函数但没接线，等于死代码。review 后补 `broker_cluster_nodedown_mon`（真正 `monitor_node` 的 gen_server）挂到 `broker_cluster_sup`，并在 `install_session_hook/0` 里 `rearm/0`。

## 3. 缺口表（Plan 3 边界债）

| # | 缺口 | 影响 | 后续 |
|---|---|---|---|
| 1 | nodedown 触发无去抖（spec §12 未决点）：网络抖动可能误判崩溃 | 误迁移 + epoch 抖动 | 加去抖窗口（Plan 3） |
| 2 | claim 与 nodedown-migrate 双路径无显式互斥测试 | 极端时序下理论竞争（ra 串行 apply 保证不双 owner，但未用 CT 打时序） | 补并发 CT（Plan 3） |
| 3 | `session_partition_count` 未与集群规模联动（默认 4） | 大集群下分区可能偏少、副本分布不均 | 启动期按节点数/副本因子推导（Plan 3） |
| 4 | conn_fsm 接管路径的**端到端**（MQTT 线级零重连）未在本 Plan 证明 | 本 Plan 只在**会话平面**证明 identity 保持（订阅集/inflight/游标）；conn_fsm 的 nodedown→claim→续传由单元级套件覆盖，未在真 3 节点 CT 里跑活 TCP 连接 | 走 MQTT 协议连接的真集群透明性 CT（Plan 3） |
| 5 | 会话分区与元数据分区**同键**读 replicas 是未决假设（spec §12） | 若将来引入独立会话副本组，`targeted_plan/1` 与 claim 的副本集来源需换 | 定稿 §12 未决点（Plan 3） |

## 4. Plan 1 / Plan 2 对比

| 维度 | Plan 1 | Plan 2 |
|---|---|---|
| 复制路由 | 广播所有副本集（债 #1） | 该会话分区的副本集（`targeted_plan/1`），cursor/未知 Op 退回广播 |
| 所有权 | 无（复制管线与所有权解耦） | 分区级 owner / epoch / fencing，`{claim, ...}` 经 ra 串行 apply |
| 崩溃接管 | 无 | `nodedown` 监控 + `pick_new_owner/2` 迁移 + conn_fsm 事后 claim 双路径 |
| 透明性 | 无客户端面 | conn_fsm 就地接管：无新 CONNECT / 无 CONNACK |
| 故障注入 CT | 2 用例（真实 2 节点，证明 Op 到达副本） | 5 用例（真实 3 节点 ra，kill owner → claim → fencing） |

## 5. 验证

### 5.1 提交

| Task | commit | 内容 |
|---|---|---|
| T1 | `fb8eba8` / `74d09aa` | 会话→分区映射 + 非法 env 回退默认 |
| T2 | `de482d6` / `04000dd` | 按会话分区路由，`targeted_plan/1` 可测 |
| T3 | `d8cc1df` / `21b5465` | `{claim,...}` 双守卫 + 非整数 epoch 守卫 + 删 `session_owner` 包装 |
| T4 | `cdb5d31` / `e4b28df` / `1519c61` / `602b35b` | nodedown 监控 + 挂 sup + demonitor 卫生 |
| T5 | `c972ccb` / `c8a142d` | conn_fsm owner monitor + 结果感知 claim + 删死批量助手 |
| T6 | `95ce80a` / `fbe9116` / `946b17d` | 真 ra 故障注入套件 5 用例 + 「濒死 owner 复制证明」强化 |
| T7 | 本次提交 | 接管指标四键 + spec §8 批注 + 本记录 |

### 5.2 套件

本 Task 实跑的守门套件（`rebar3 ct`，OTP 29.1.2）：

| SUITE | 用例数 | 结果 |
|---|---|---|
| `broker_metrics_SUITE`（含新增 `session_takeover_keys_registered`） | 16 | 全绿 |
| `broker_cluster_meta_SUITE`（claim 四用例） | 4 | 全绿 |
| `mqtt_conn_fsm_takeover_SUITE` | 3 | 全绿 |
| `broker_session_replica_SUITE`（含新增 `takeover_outcome_counters`，锁四键映射） | 6 | 全绿 |
| `broker_cluster_session_rep_SUITE` + `_nodedown_SUITE` + `_session_owner_SUITE` | 12 | 全绿 |

`rebar3 compile` 干净（warnings-as-errors，无告警）。

### 5.3 全量 CT 状态（未在本 Task 重跑）

全量基线为 **1663**（Plan 1 交付时，2026-11-22），Plan 2 在此基础上新增本记录的用例。本 Task **没有**重跑全量 CT，以下为历史记录的修正，不构成新证据：

- `broker_proto_mcp/test/mcp_dispatch_SUITE.erl`：历史记录称其「未跟踪 WIP、编译不过」。**在本分支该文件不存在**（`git ls-files | grep mcp_dispatch` 只有 `src/mcp_dispatch.erl`，测试目录只有 `mcp_listener_SUITE` / `mcp_route_SUITE` / `mcp_stdio_SUITE`），因此它**不是**本分支的失败来源，只是历史记录留下的旧条目。
- `kafka_group_SUITE:session_timeout_sweeps_dead_member`：历史记录称其为既有 `wait_for` 缺陷。本 Task 实测复核：该套件其余 13 用例全绿，唯 `all()` 末位的这一例跑 240s 仍未出结果。原因是 `wait_for(Fun, 2000|4000)` 的第二个参数是**迭代次数**而非毫秒，每次 `timer:sleep(150)`，故实际预算是 300s / 600s 量级——**历史结论仍成立**，是既有缺陷、与 Plan 2 无关，未在本 Task 修复。

## 6. 关联

- Plan 1 实现记录：[session-state-replication-plan1-notes](./2026-11-22-session-state-replication-plan1-notes.md)
- 设计 spec：[session-state-replication-design](./2026-11-22-session-state-replication-design.md)（§8 = 2026-11-22 Plan 2 批注）
- Plan 2 设计：[session-state-replication-plan2-design](./2026-11-22-session-state-replication-plan2-design.md)
- 缺口表：[unifiedmq-mqtt-family-gap.md](../../compatibility/unifiedmq-mqtt-family-gap.md) MQ-17 行
- 既有 ADR-016（冻结键集）/ ADR-021 §D5（消息复制）/ ADR-034 D5/D7（跨节点路由、迁移守卫）语义被扩展引用，未推翻。

> 文档惯例：历史实测记录正文不改，只追加带日期批注或划掉并注明日期。本文件为新增实现记录。
