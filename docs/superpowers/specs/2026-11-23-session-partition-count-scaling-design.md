# 会话分区数随集群规模缩放 —— 设计（Plan 3 缺口 #3）

## 1. 背景与缺口

### 1.1 现状

会话→分区的映射由 `broker_cluster_session_owner` 承担：

```erlang
partition_of(ClientId, Count) -> erlang:phash2(ClientId, Count).
session_partition_count() ->
    case application:get_env(broker_cluster, session_partition_count) of
        N when is_integer(N), N > 0 -> N;
        _ -> 4
    end.
```

`Count`（下文记 **C**）是**硬编码默认 4**，唯一的覆盖手段是应用环境变量。
它与会话分区空间是一一对应的：`partition_of/2` 把 ClientId 散列到稠密的
`0 .. C-1`，而 `broker_cluster_session_rep:targeted_plan/1` 与
`mqtt_conn_fsm:maybe_monitor_owner/2` 都用它定位会话分区，再读该分区的
`replicas` / `owner`。

缺口表（`2026-11-22-session-state-replication-plan2-notes.md` §3 #3）：
**`session_partition_count` 未与集群规模联动（默认 4）**，影响"大集群下分区
可能偏少、副本分布不均"，后续"启动期按节点数/副本因子推导（Plan 3）"。
设计稿 §12 把它列为未决点："会话分区数的默认值与集群规模（节点数 / 副本因子）
的取法"。

### 1.2 调研中发现的两个前置事实

这两点是原缺口描述未覆盖、但本次落地无法回避的：

**F1：生产侧没有为会话分区建立元数据。** 全仓搜索 `broker_cluster:assign/4`
的非测试调用点只有 `broker_rebalance:run_assign/4` 一处，而它操作的是
**元数据分区**（rebalance 动态稀疏创建）。没有任何启动/bootstrap 代码遍历
`0 .. C-1` 去 `assign` 会话分区。真实部署下，`session_owner(P)` 对任意
ClientId 都是 `not_found`，于是 `mqtt_conn_fsm:maybe_monitor_owner/2` 走
"无 owner → 不装 monitor"分支，**接管路径（缺口 #4 证明的那条）在生产上
从未真正生效**。测试之所以能跑，是 `broker_session_replica_SUITE` /
`broker_cluster_conn_fsm_takeover_dist_SUITE` 在 `init_per_testcase` 里手工
`assign_partition/4` 造出元数据。

**F2：C 必须全集群一致，但当前无法保证。** C 来自各节点的应用环境变量；
运维漏配/错配任何一个节点，同一个 ClientId 在两个节点上散列到不同分区，
复制路由到错误的副本集 → 会话状态分裂。今天靠"所有节点配同一个值"的约定，
没有任何机内保证。

### 1.3 与其它缺口的关系

- **缺口 #4（已交付）**证明 `conn_fsm` 在 owner 崩溃后零重连续期。它要求
  分区元数据**存在**；F1 说明生产的这一前提是空的。#3 的 bootstrap 是该证明
  在生产成立的必要条件。
- **缺口 #5（未交付）**是"会话分区与元数据分区**同键**读 replicas 是未决
  假设"。本设计**沿用同键假设**（会话分区 P 直接读 `cache` 里键 P 的元数据），
  并把该假设显式写成不变量 I8（见 §8），不改变 §12 那条未决点的表述。

## 2. 目标与非目标

### 2.1 目标

1. **C 的权威值进 ra 元数据组**：集群内唯一、随日志复制、随快照恢复，取代
   "各节点各自读 env"的约定。
2. **启动 bootstrap**：`session_replication` 开启时，为 `0 .. C-1` 幂等建立
   会话分区元数据（owner/replicas/epoch），让接管路径在生产真正可运行。
3. **成员变化自动扩容**：成员集合变化后按规则推导新的 C（双向，可增可减），
   经冷却闸门后提交到 ra。默认开启（与 `broker_rebalance` 的 default-closed
   不同，理由见 §3.4）。
4. **平滑迁移**：C 变更带一个单调递增的 `session_partition_epoch`；`conn_fsm`
   在既有 poll tick 上比对版本，变化则重算分区并重挂 monitor，客户端不重连。

### 2.2 非目标（YAGNI）

- 不把 `partition_of/2` 换成 jump consistent hash 或 rendezvous hashing。
  当前 `phash2(Id, C)` 的扩容语义是"全量重散列"，本设计用"conn_fsm 平滑迁移
  + epoch fencing"消化它，不引入新的散列算法（算法切换本身也会全量迁移，
  收益只在"部分迁移"，留待有真实迁移成本压力时再议）。
- 不迁移队列分区空间（`broker_queue` 的 `0..N-1` 是另一套编号，本设计不动）。
- 不做"排空再扩容"（需要排空状态机，与自动扩容的简单性冲突）。
- 不改 `broker_rebalance` 的元数据分区平衡逻辑。
- 不为缩容做数据搬迁：缩容后落入 `[newC, oldC)` 的旧分区元数据由 §3.5 的
  回收规则处理，会话本身靠重散列 + fencing 收敛。

## 3. 设计

### 3.1 C 与会话分区 epoch 进入 ra 状态机

`broker_cluster_meta_machine` 的 state 从

```erlang
#{partitions => #{Partition => meta()}}
```

扩为

```erlang
#{partitions => #{Partition => meta()},
  session_partition_count => pos_integer() | undefined,
  session_partition_epoch => non_neg_integer()}
```

新增一条命令型：

```erlang
{set_session_partition_count, NewCount, NewEpoch}
```

apply 语义（与 `claim` 同风格：epoch 单调守卫，拒绝陈旧写）：

- `NewEpoch =< 当前 session_partition_epoch` → `{error, {stale_epoch, Cur, New}}`；
- `NewCount` 非正整数 → `{error, {bad_count, NewCount}}`；
- 否则写 state 并把 `{session_partition_count, NewCount, NewEpoch}` 镜像进
  `broker_cluster_cache`，返回 `{ok, NewEpoch}`。

**快照兼容**：旧快照里没有这两个键。`snapshot_installed/5` 与 `init/1` 都用
`maps:get(Key, State, Default)` 读取，缺省值分别为 `undefined` 与 `0`——即
"元数据里还没有权威 C"，此时 `session_partition_count/0` 退回 §3.2 的推导，
行为与今天一致。

`broker_cluster_cache` 增加两个读接口（表里存一个保留键，与整数分区键不冲突）：

```erlang
put_session_partition_config(Count, Epoch)
session_partition_config() -> {ok, Count, Epoch} | not_found
```

实现用一个独立的 ETS 行 `{?SESSION_CFG_KEY, Count, Epoch}`，`?SESSION_CFG_KEY`
取一个不可能与 `non_neg_integer()` 分区键相同的原子（如
`'$session_partition_config'`），`partition_view/0` / `all/0` 过滤掉它，
保证 rebalance 与快照遍历看不到这个保留行。

### 3.2 读取路径：cache 优先，env 推导兜底

```erlang
session_partition_count() ->
    case broker_cluster_cache:session_partition_config() of
        {ok, Count, _Epoch} -> Count;
        not_found -> configured_or_derived_count()
    end.
```

`configured_or_derived_count/0` 维持今天的行为：env 里有正整数就用它，否则
用推导值（§3.4 的规则，但**不**写回 ra——读路径绝不触发共识写，遵守
ADR-008 数据路径原则）。

新增：

```erlang
session_partition_epoch() -> non_neg_integer().   %% 无配置时返回 0
```

`conn_fsm` 用它对版本。

### 3.3 启动 bootstrap

新增 `broker_cluster_session_bootstrap` 模块（由 `broker_cluster_sup` 在
Cache 之后、Meta 之后启动，或作为 `broker_cluster_app:start_supervised/0`
里 Meta 起来后的一个步骤；实施计划阶段二选一，见 §7）。

职责（仅在 `session_replication = true` 时生效）：

1. 若 cache 里**没有**权威 C（`session_partition_config` 返回 `not_found`），
   推导一个初始 C（env 覆盖优先，否则按 §3.4 规则），提交
   `{set_session_partition_count, C, 1}`；
2. 对 `0 .. C-1` 每个分区，若 `cache` 里没有该分区元数据**或**该分区元数据
   的 `replicas` 与当前成员不一致，提交 `{assign, P, Owner, Members, Epoch+1}`。
   `Owner` 用与 `broker_cluster_nodedown:pick_new_owner/2` 一致的确定性规则
   （成员排序后的第一个，或按 P 轮转，保证均衡且可复现）。该操作**幂等**：
   重复执行不改变已有一致的元数据，只补齐缺失的部分。
3. 缩容后：`C` 变小，`[newC, oldC)` 的旧分区元数据用既有 `{remove, P, Epoch}`
   命令回收（§3.5）。

**为什么需要它**：F1。没有 bootstrap，`session_owner(P)` 恒为 `not_found`，
缺口 #4 的接管链路在生产永不触发；自动扩容也只是在改一个没人读的 C。

### 3.4 推导规则与自动扩容触发

**规则**（纯函数，可单测）：

```erlang
derive_count(MemberCount) ->
    Target = max(MinCount, MemberCount * PartitionsPerNode),
    next_power_of_two(Target).
```

- `MinCount` 默认 4（env `session_partition_min_count`）；
- `PartitionsPerNode` 默认 4（env `session_partitions_per_node`）；
- 向上取 2 的幂：`phash2/2` 对 2 的幂的分布更均匀，且幂值使分区数变化的最小
  步长有意义（4 → 8 → 16）。

**触发**：`broker_cluster_nodedown_mon` 已持有成员监视（`{nodedown}/{nodeup}`），
在成员集合收敛后调用一个新的 `maybe_rescale/1`：

1. 读当前权威 C 与 `derive_count(length(Members))`；相同 → 不动。
2. 冷却闸门：距上次 C 变更（持久化在 ra 的 `session_partition_epoch` 对应
   时间戳，或 cache 里的 `last_rescale_ms`）不足 `rescale_cooldown_ms`
   （默认 300000）→ 不动。
3. 否则提交 `{set_session_partition_count, NewC, Epoch+1}`，成功后再跑一次
   §3.3 的 bootstrap（补齐新增分区 / 回收缩掉的分区）。

**为什么 default-open**：`broker_rebalance` 的 default-closed 是刻意选择——
元数据分区的**平衡**是运营调优，误动无收益。会话分区数不同：它是**容量正确性**
的一部分（分区太少导致单分区会话过载、副本分布倾斜），且不改变 owner 归属的
语义（只是重散列）。因此本设计选择 default-open，但保留冷却闸门与显式开关
`session_partition_autoscale`（默认 true，可关）。

### 3.5 缩容的分区回收

`C` 从 `OldC` 减小到 `NewC` 后，`[NewC, OldC)` 内的元数据行：

- 若其 `state = active` 且无存活 owner：`{remove, P, Epoch+1}` 直接回收；
- 若仍有存活连接指向该分区（`conn_fsm` 尚未迁移）：**不立即删**，等
  `conn_fsm` 在 poll tick 发现 `session_partition_epoch` 变化、重算到
  `[0, NewC)` 内的新分区后，再由下一次 `maybe_rescale`（或一次显式
  `finish_rescale`）回收。这样避免删掉仍被读的元数据行。
- 实现上不引入"待回收"新状态：用"该分区键 ≥ 当前 C 且无人 monitor"判据，
  由 bootstrap 的清理步骤幂等处理。

### 3.6 conn_fsm 平滑迁移

`#st` 增加 `owner_epoch = 0`（上次见到的 `session_partition_epoch`）。

在既有的 poll tick（`arm_poll/2`，默认 1000ms）里，`connected` 状态下：

```erlang
maybe_migrate_partition(D) ->
    Cur = broker_cluster_session_owner:session_partition_epoch(),
    case Cur =:= D#st.owner_epoch of
        true -> D;
        false -> reattach_partition(D)   %% 重算 P、重挂 monitor、更新 owner_epoch
    end.
```

`reattach_partition/2`：

1. `NewP = partition_of(ClientId, session_partition_count())`（已是新 C）；
2. 旧的 `owner_node` 若在 monitor 中则 `unmonitor_node`；
3. 按 `maybe_monitor_owner/2` 现有逻辑对新 owner 重挂 monitor；
4. 更新 `owner_part` / `owner_epoch`。

**正确性**：迁移后旧分区 `P_old` 上的陈旧写会被 epoch fencing 拒绝——这是
既有 I6 的直接应用，不需要新增机制。连接仍是同一个 socket（缺口 #4 的零重连
性质在本路径上同样成立，因为这里根本不碰 socket）。

**开关联动**：`maybe_migrate_partition/1` 只在 `session_replication = true`
时生效，与现有 `maybe_monitor_owner/2` 的门控一致（I2 退化：关闭时逐字节不变）。

## 4. 数据流

```
启动（session_replication=true）
  └─ bootstrap: 无权威 C → 推导 → ra set_session_partition_count(C,1)
                → for P in 0..C-1: ra assign(P, Owner, Members, E+1)  [幂等]
  └─ cache 镜像 ← set_session_partition_count / assign

成员变化（nodedown_mon 收敛后）
  └─ maybe_rescale: derive_count(members) ≠ C ?
        ├─ 冷却中 → 不动
        └─ 提交 set_session_partition_count(NewC, Epoch+1)
              └─ 再跑 bootstrap：补新分区 / 回收缩掉的分区

运行中连接（conn_fsm）
  └─ poll tick: session_partition_epoch() ≠ owner_epoch ?
        ├─ 否 → 照常 pull
        └─ 是 → partition_of(ClientId, newC) → 重挂 monitor → 更新 owner_epoch
                  （旧分区陈旧写被 epoch fencing 拒绝）
```

## 5. 错误处理与边界

| 场景 | 行为 |
|---|---|
| ra 无 quorum，`set_session_partition_count` 超时 | 返回 `{error, no_quorum}`，C 不变，`maybe_rescale` 静默重试（下个成员事件或下次 tick） |
| cache 未起（单机未启 broker_cluster） | `session_partition_config()` → `not_found`，退回 env/推导，与今天一致 |
| 旧快照无新字段 | `maps:get` 缺省 `undefined`/`0`，`session_partition_count()` 退回推导 |
| 缩容后旧分区仍有连接 | 延迟回收（§3.5），等 conn_fsm 迁移 |
| `NewCount` 非正整数 | `{error, {bad_count, _}}`，C 不变 |
| 成员抖动导致 C 反复变 | 冷却闸门（默认 300s）+ epoch 单调 |
| `session_replication = false` | bootstrap / autoscale / conn_fsm 迁移全部不生效（I2） |

## 6. 测试策略

1. **纯函数单测**（非 dist）：`derive_count/1` 的边界（1 节点、大集群、2 的幂
   取整）、`next_power_of_two`。
2. **state machine 单测**：`{set_session_partition_count, ...}` 的 epoch 单调、
   非法 count、快照往返（含旧快照缺字段）。
3. **cache 保留键单测**：`session_partition_config` 往返；`partition_view/0`
   与 `all/0` **不**包含保留键。
4. **bootstrap CT**（真 3 节点 ra）：启动后 `0..C-1` 全部分区有元数据、
   `session_owner(P)` 非 `not_found`；重复启动幂等（epoch 不无故增长）。
5. **缩放 CT**（真 3 节点 ra）：模拟成员数变化驱动 `maybe_rescale`，断言
   C 按规则变化、epoch 单调、冷却闸门生效（冷却期内不变）、缩容回收旧分区。
6. **conn_fsm 平滑迁移 CT**：连接建立后提交一次新的 C（epoch+1），断言
   poll tick 后 `owner_part` 迁移到新分区、socket 未重连、旧分区陈旧写被拒。
   （可在既有 `broker_cluster_conn_fsm_takeover_dist_SUITE` 之外新建套件，或
   扩展它；实施计划定。）
7. **回归**：`session_replication=false` 时所有既有套件逐字节不变。

## 7. 留给实施计划的决策点

- bootstrap 挂在 `broker_cluster_sup` 子进程还是 `broker_cluster_app:start_supervised/0`
  的内联步骤（涉及 ra 是否已 leader-ready 的时序）。
- 自动扩缩容是否复用 `broker_cluster_nodedown_mon` 还是新起一个 `_rescale_mon`
  （复用省进程，但把两件事混在一个 gen_server 里）。
- conn_fsm 的迁移检查放在 poll tick 还是新增定时器（poll tick 零新增成本，
  但依赖 `pull_poll_interval_ms` 的既有节奏）。
- 缩容回收的具体触发点（bootstrap 清理 vs 独立 `finish_rescale`）。

## 8. 不变量

- **I8（同键假设，承接缺口 #5）**：会话分区 P 的元数据直接复用 cache 里键 P
  的元数据行（`replicas`/`owner`/`epoch`）。本设计不引入独立会话副本组。
- **I9（C 的集群一致性）**：权威 C 只存在于 ra 状态机，所有节点读同一份
  cache 镜像；env 仅作为"尚无权威值"时的初始推导输入。
- **I10（epoch 单调）**：`session_partition_epoch` 单调递增，陈旧
  `set_session_partition_count` 被拒，与既有 claim/assign 守卫同构。
- **I11（默认退化）**：`session_replication = false` 时本设计全部路径不生效，
  行为逐字节同今日（承接 I2）。
- **I1–I7**：沿用既有定义，本设计不改。
