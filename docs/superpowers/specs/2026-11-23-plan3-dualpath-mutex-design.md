# Plan 3 边界债 #2：claim 与 nodedown-migrate 双路径互斥的并发 CT

> 状态：设计稿（2026-11-23）。对应 `2026-11-22-session-state-replication-plan2-notes.md` §3 缺口表第 2 条。

## 1. 背景与缺口

Plan 2 的透明故障接管有**两条互不依赖**的所有权变更路径（plan2-notes §3 偏差 #3）：

1. **claim 路径**（主路径）：持连副本节点观察到 `{nodedown, O}` 后，提交 `{claim, P, Node, Epoch}` **事后认领**分区所有权。入口 `broker_cluster:claim/3`。
2. **nodedown-migrate 路径**（兜底）：`broker_cluster_nodedown:on_nodedown/1` 遍历本地视图，对已死亡 owner 的分区用 `pick_new_owner/2` 按副本固定序选第一个存活副本，提交 `{migrate, P, From, To, Epoch}`。入口 `broker_cluster:migrate/4`。

两条路径**都**经同一个 `broker_cluster_meta_machine` 的 ra 串行 `apply/3`，理论上不会双 owner。但这一保证**只用单元测试各测各的路径**，从未用 CT 把两条路径放在同一分区、同一 epoch 上真正赛跑：

- `broker_cluster_nodedown_SUITE` 只单测 `pick_new_owner/2`（选主逻辑），不碰 ra。
- `broker_session_replica_SUITE` 用真 3 节点 ra 组，但每条用例只走 claim**单路径**，没有 claim 与 migrate 对撞。

缺口表原文：「claim 与 nodedown-migrate 双路径无显式互斥测试 → 极端时序下理论竞争（ra 串行 apply 保证不双 owner，但未用 CT 打时序）→ 补并发 CT（Plan 3）」。

## 2. 目标与非目标

**目标**：用真 3 节点 ra 组，在**同一分区、同一 epoch** 上让 claim 与 migrate 对撞，证明：

1. 两条路径恰好一个成功（`{ok, Epoch}`）、另一个被 epoch 单调守卫拒绝（`{error, {stale_epoch, E, E}}`）；
2. 无论谁先落地，最终 owner 唯一，且三节点 cache 收敛一致，等于**赢家提议的目标**；
3. 竞态结束后所有权可见后果正确：唯一 owner 以当前 epoch 可写，两个非 owner（含败者）以当前 epoch 被 fencing 拒绝，且赢家再以陈旧 epoch 写也被拒。

**非目标**：

- 不重测 `pick_new_owner/2` 的选主逻辑（已由 `broker_cluster_nodedown_SUITE` 覆盖）。
- 不引入真 kill owner 的端到端集成用例（保真度选择见 §6）；本 Plan 只打**命令层**的 claim-vs-migrate 竞争。
- 不触碰 MQTT wire 层（属缺口 #4 的范围）。

## 3. 互斥的机制依据（为何「一赢一败」是硬保证，而非概率）

`broker_cluster_meta_machine` 的状态机守卫（`apps/broker_cluster/src/broker_cluster_meta_machine.erl`）：

- `assign/8` 及其复用的 `migrate` 分支：现有 epoch 为 `E` 时，`E >= Epoch` 即拒，返回 `{error, {stale_epoch, E, Epoch}}`（严格递增才接受）。
- `claim/6`：`E < Epoch` 才接受；否则同为 `{error, {stale_epoch, E, Epoch}}`。

`migrate` 的 `apply` 走 `assign(P, To, replicas_of(P, Ps), Epoch, migrating, ...)`，与 claim 共享同一 epoch 单调谓词。

因此：对同一分区、同一个提议 epoch `E_new = E_cur + 1`，**先被 ra apply 的那条命令把 epoch 推到 `E_new`**；后到的命令读到现有 epoch 已等于 `E_new`，`E_new >= E_new`（migrate）或 `E_new < E_new` 为假（claim）——**必被拒**。ra 的串行 apply 保证两条命令不会交错到「都读到旧 epoch」。这是确定性互斥，不是概率性去重。

`pick_new_owner/2` 排除**本节点**（`N =/= node()`），故在 N2、N3 上分别触发 `on_nodedown(O)` 时，会各自选出不同目标（N2→N3、N3→N2）——这正是「同 epoch、不同目标」对撞的现实形状，并发循环按此构造。

## 4. 测试设计

**新套件**：`apps/broker_cluster/test/broker_cluster_dualpath_mutex_SUITE.erl`

脚手架复用 `broker_session_replica_SUITE` 的真 3 节点 peer 搭建（`peer:start_link/1` + `prepare_peer/1` + `form_cluster_h/1` + `assign_partition/4`），每台 peer 起 ra 与 `broker_cluster_cache`，元数据服挂成真 ra 组，故 `claim/3` / `migrate/4` 都是穿过 `ra:process_command/3` 的真命令。

### 用例 1：`claim_then_migrate_same_epoch`（确定性正向）

1. `assign(P, N1, Nodes, 1)`（已知起点：owner=N1、epoch=1、replicas=Nodes）。
2. `claim(P, N2, 2)` → 断言 `{ok, 2}`，owner 变 N2。
3. `migrate(P, N1, N3, 2)`（**同 epoch 2**）→ 断言 `{error, {stale_epoch, 2, 2}}`。
4. 断言终态：`owner(P) = N2`（三节点一致）。

### 用例 2：`migrate_then_claim_same_epoch`（确定性反向）

1. `assign(P, N1, Nodes, 1)`。
2. `migrate(P, N1, N3, 2)` → 断言 `{ok, 2}`，owner 变 N3。
3. `claim(P, N2, 2)`（**同 epoch 2**）→ 断言 `{error, {stale_epoch, 2, 2}}`。
4. 断言终态：`owner(P) = N3`。

> 1+2 合起来证明：**不论哪条路径先落地**，另一条同 epoch 必被拒，赢家 owner 稳定不变。这是对撞的核心确定性证明。

### 用例 3：`concurrent_claim_vs_migrate_never_double_owner`（并发循环打时序）

循环 `R = 20` 轮。每轮：

1. `Base = 2 * Round`（偶数，严格递增，避免跨轮 epoch 回退）。`assign(P, N1, Nodes, Base)` 重置到已知 epoch `Base`。
2. 在两个**不同** peer 上**同时** spawn 两条命令，提议 epoch 均为 `Base + 1`、目标不同：
   - 一路：`claim(P, N2, Base + 1)`；
   - 一路：`migrate(P, N1, N3, Base + 1)`。
   用两个独立 `rpc:async_call/4` 发起、`rpc:yield/1` 收齐，制造真实并发。
3. 断言结果集合：
   - **恰好一个** `{ok, Base + 1}`；
   - 另一个 **恰为** `{error, {stale_epoch, Base + 1, Base + 1}}`；
   - 不存在两个 `{ok}`、不存在两个 `{error}`（双成功=双 owner，双失败=互斥过头）。
4. 断言收敛：`wait_until` 三节点 `owner(P)` 一致，且**等于赢家提议的目标**（claim 赢→N2；migrate 赢→N3）。不断言硬编码目标，只看「赢家值 == 收敛值」。
5. 断言 `epoch(P) = Base + 1`（单调，无脏写）。

### 用例 4：`fencing_after_dualpath_race`（互斥的可见后果）

独立跑一轮 claim-vs-migrate 对撞（构造同用例 3 单轮），拿到赢家 owner `W` 后：

1. `authorize_append(P, Base+1)` 在 `W` 上 → `ok`（唯一合法 owner 可写）。
2. `authorize_append(P, Base+1)` 在**两个非 owner**（含败者）上 → 均 `{error, {fenced, _}}`。
3. `authorize_append(P, Base)`（陈旧 epoch）在 `W` 上也 → `{error, {fenced, _}}`。

> 这把「谁是 owner」从内部状态推到**可观测的写权限**层：双路径赛跑后，写权限只有一份。

### 断言纪律

- 单 owner 用**双证**：三节点 cache owner 一致 + 恰好一个 `{ok}`。
- epoch 单调用**精确形状** `{stale_epoch, E, E}`，不用 `>=`。
- 不依赖硬编码赢家，只看「赢家提议值 == 收敛 owner」。

## 5. 隔离与运行约束

- peer 用 ephemeral 端口，不碰任何项目端口；CT `port => 0`。
- `net_kernel:start([<name>, shortnames])` 同其它 dist 套件（`net_kernel:start/1` 后不可重设 cookie）。
- 跑测前 `rm -rf _build/test`（plan2-notes §5.4 的 `_build/test/lib/<app>/test/` 残留陷阱）。
- 与 `broker_session_replica_SUITE` 共享 peer 命名前缀隔离，避免 epmd 节点名冲突（用独立前缀，如 `s3dp_`）。

## 6. 保真度取舍（记录）

选「确定性双向 + 并发循环」，不含真 kill owner 的端到端集成用例。理由：

- 缺口 #2 的诉点是「两条命令路径在同一分区上的互斥」，命令层对撞即可证明；`on_nodedown` 的遍历/选主已被单测覆盖。
- 真 kill 会引入 ra leader 选举等待、时序抖动，显著拉长 CT 且易 flaky；其价值更多属缺口 #4（端到端透明性）。
- 并发循环（20 轮）在同进程内制造真实 ra 并发提交，足以暴露「守卫写错/去重缺失」的回归——若守卫被删成 no-op，双成功或双失败的断言会当场红。

## 7. 验收判据

- 4 个用例在真 3 节点 ra 组上 **全绿**。
- 人为把 claim/migrate 的 epoch 守卫改成「接受相等 epoch」（临时 hack 验证）时，用例 1/2/3 会**变红**（证明测试确实锁住了互斥，而非退化通过）。
- 全量 CT 中本套件不引入新失败（kafka cover 崩溃属既有已知无关项，见 plan2-notes §5.3）。
