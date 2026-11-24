# MQ-15 共享订阅集群再均衡 —— 设计

> 状态：设计稿（brainstorm 产出，待 review → writing-plans → 实现）
> 缺口出处：`docs/compatibility/unifiedmq-mqtt-family-gap.md` 第 66 行（MQ-15）
> 日期：2026-11-24

## 1. 背景与缺口

### 1.1 现状

`$share/<Group>/<Filter>` 共享订阅在**单节点**完整可用：能建组、多成员竞争消费、
grab-next-free 分配（谁轮询拿下一个空闲 offset）。分配算法在 `broker_cursor` 里是纯函数：

- shared row = `#{kind => shared, low, acked, assigned, retries}`，其中 `assigned` 是
  `#{Offset => ClientId}` 的分配表，`low` 是首个未 ack 的 offset。
- `claim/6` 找空闲 offset 分配给发起 claim 的 Consumer（ClientId）；`ack_shared/3`
  移除 assigned、加入 acked、按连续前缀推进 `low`；`release/2` 移除某 ClientId 的全部
  assigned（成员离开/崩溃时回池）。这些都在 worker-local ETS `?CURSORS` 行上做集合运算。

### 1.2 缺口

`cross_node_subscribe_ok/2`（`broker_subscription_worker.erl:946-963`）**显式拒绝**
跨节点 shared group：覆盖分区里任一非本 owner → `{error, {cross_node_shared_unsupported, P}}`。

拒绝原因注释（`:946-951`）：`claim/4` 从 worker-local ETS 行分配 offset，两个节点独立
claim 会把同一 offset 分给两个消费者——**静默重复投递**（非报错）。exclusive/failover
是单读者、cursor-local，跨节点正确，故不动。

缺口表（`unifiedmq-mqtt-family-gap.md:66`）：**MQ-15 未做完**——共享订阅只在单节点，
"多节点时每个节点一组，不是一个消费组"，缺**集群范围的再均衡**。

### 1.3 关键调研事实（本次 brainstorm 已核实）

**F1：消息体跨节点读取路径已存在，缺的只是分配串行化。**
`broker_queue:read_from/3` 经 `route/3`（`broker_queue.erl:236`）：`cross_node_delivery=true`
时，非 owner 节点收到 `{forward, Owner}`，`read_from` 被 `rpc:call` 到 owner 的
`forwarded/3`。`broker_cluster:route_op/3`（`broker_cluster.erl:241`）从 cache 读 owner 决定
local/forward。所以消费者在节点 A、分区 owner 在节点 B 时，body 读取已能跨节点。
**MQ-15 缺的是分配决策的跨节点一致，不是 body 移动。**

**F2：消费者身份是裸 ClientId（binary），已是集群唯一标识。**
`mqtt_conn_fsm` 调用 `broker_subscription:claim(Ref, P, Max, D#st.client_id)` 与
`release(Ref, P, Missing, D#st.client_id)`（`mqtt_conn_fsm.erl:2375`/`:2412`）。
MQTT clientid 集群唯一（重连踢旧连接），故 `assigned` 表 `{Offset => ClientId}` 可直接进
ra 存储，无需节点限定。

**F3：committed watermark（High）已在队列模块、集群可读。**
`broker_queue:watermarks/1` 给出 `{ok, _Committed, High}`，worker 算 claim 的 `Blocked` 集用。
不涉分配一致性，留原位。

**F4：现有 rebalance 已有可复用的闸门模式。**
`broker_rebalance` 用 `hysteresis`（max-min spread 阈值，默认 1）+ `cooldown_ms`（默认
300000）双闸门；`broker_cluster_nodedown_mon` 做成员变化检测（带去抖窗口）。

## 2. 已定决策（brainstorm 确认）

1. **协调层 = assigned 表进 ra。** 把分配决策提升为 ra 元数据组里的一种键，经 epoch fencing
   串行 apply，任何节点都能 claim，保证不重复投递。
2. **再均衡 = 混合（懒式 grab-next-free + 阈值触发）。** 默认谁轮询拿下一个空闲 offset；
   失衡超阈值时触发一次主动再均衡。

## 3. 核心架构

### 3.1 三条路径的职责分离

沿用两条已有路径、补一条：

- **分配决策**（谁能拿哪个 offset）→ 进 ra，串行化。**【新增】**
- **消息体读取**（offset 对应的消息在哪）→ 沿用 `broker_queue:read_from` 的 `route/3`
  forward 路径。**【已存在，不动】**
- **连接/投递**（消费者在哪个节点、in-flight 窗口）→ conn_fsm 本地。**【已存在，不动】**

正确性根因：ra apply 单线程串行，使「同一 offset 分给两个」在结构上不可能——把
「两节点不竞态」的分布式协调难题，降为「单串行点做集合运算」。

### 3.2 状态字段去向（worker-local shared row）

| 字段 | 含义 | 跨节点一致性需求 | 去向 |
|---|---|---|---|
| `assigned` | offset→consumer 分配表 | **必须全局一致**（两节点独立 claim 会重复投递） | **进 ra**（核心新增） |
| `acked` | 已确认 offset 集合 | 必须全局一致（决定 `low` 推进、committed watermark） | **进 ra** |
| `low` | 首个未 ack offset | 派生自 `acked` | ra apply 内计算，不单独存 |
| `retries` | 订阅侧重试 backoff | **可留本地**（纯性能，丢了只是重复消费，不违反语义） | **留本地快路径** |
| `High` | 队列存储水位（committed watermark） | 已在队列模块、集群可读（forward） | **不动**（读 `broker_queue:watermarks`） |

### 3.3 ra 状态机扩展（`broker_cluster_meta_machine`）

新增键族（与现有 `partitions` 并列）：

```erlang
%% state 增加：
shared_assignments => #{{SubRef, PartitionId} => #{Offset => ClientId}}
```

SubRef = `{shared, Group, Filter}`（shared kind 的订阅三元组）。ClientId 是 binary。

新增命令（`apply/3` 分支，全部 epoch-guarded，串行集合运算）：

- `{shared_claim, SubRef, P, ClientId, Max, High, Blocked}` → 在该 `{SubRef,P}` 的
  assigned 表上跑 claim（找空闲 offset 分配给 ClientId），返回
  `{ok, [Offset], NewAssigned}` 或 `{error, overflow}`。`High`/`Blocked` 由 worker 本地
  预先算好带进命令，ra 内不读队列。
- `{shared_ack, SubRef, P, Offset, ClientId}` → 移除 assigned、加入 acked、按连续前缀推进
  `low`。返回 `{ok, Low}` 或 `unowned`（offset 不属该 ClientId）。
- `{shared_release, SubRef, P, ClientId}` → 移除该 ClientId 的全部 assigned（成员离开/崩溃回池）。
- `{shared_reclaim, SubRef, P, FromClientId, ToClientId, Offsets}` → 阈值触发的主动再均衡：
  把指定 offset 从 From 改派给 To。仅动 assigned 中未 ack 的 offset。

快照/恢复：`snapshot_installed/5` 重建 cache 时一并恢复 `shared_assignments` 到本地镜像。

### 3.4 本地快路径保留 + worker ETS 降级为只读镜像

`broker_cursor:claim/6`、`ack_shared/3`、`release/2`、`release_offset/2`、`settle/2`、
`retry/5`、`blocked/3` **全部保留为纯函数**——现在由 ra apply 内调用，不再由 worker
直接 mutate ETS。

worker 的 ETS `?CURSORS` 行降级为 **ra 分配表的本地只读镜像**（用于本地 reads、
retries/Blocked 计算、low 缓存）。写一律经 ra 回来再更新镜像。

`broker_subscription_worker:handle_call({claim,...})`：从「本地 ETS 算 claim」改为
「`broker_cluster:shared_claim(...)` → ra → 更新本地镜像 → 返回 offsets」。

## 4. 再均衡机制（混合策略）

### 4.1 三个触发面

**(a) 成员变化（join/leave/crash）—— 即时 + 去抖**
- 消费者 join：ra 里出现新 ClientId；其轮询 claim 自然进入分配。
- 消费者 leave：`shared_release` 把该 ClientId 的 offset 回池。
- 节点崩溃：`broker_cluster_nodedown_mon` 去抖窗口确认后派发 `shared_release`（该节点上全部
  ClientId）→ 回池，由存活消费者重新 claim。**复用现有去抖，不新增。**

**(b) 失衡超阈值 —— 周期检查 + hysteresis + cooldown**
- 周期 tick（复用 poll tick 或独立 rebalance tick）计算每个 shared group 的 in-flight 分布
  （`map_size(assigned)` per ClientId，从 ra 分配表读）。
- 当 `max(inflight) - min(inflight) > hysteresis` **且** 距上次再均衡 `> cooldown_ms`：
  触发一次 `shared_reclaim`，把过载消费者超出均值的 offset 改派给空闲消费者。
- **防 churn**：hysteresis 只在真失衡时动；cooldown 防抖动；每次只搬超出均值的部分。

**(c) 懒式 grab-next-free（基线，无需触发）**
- 新消费者轮询时 grab-next-free 自动从空闲池取。这是默认行为，阈值触发只是补充。

### 4.2 跨节点改派（reclaim）的 body 路由

`shared_reclaim` 把 offset O 从节点 A 上的 C1 改派给节点 B 上的 C2：
1. ra 更新分配表 O→C2（串行，保证不会两节点同时认 O）。
2. C2 的 conn_fsm 在 poll tick 读到 ra 分配表里 O 分配给自己 → 经 `broker_queue:read_from(O)`
   forward 路径从分区 owner 读 O 的 body。
3. C1 不再 poll O（ra 已不认它），不会重复发。

**不变量**：一个 offset 在任一时刻只被一个 ClientId 持有（ra 串行保证），body 不会被两个
消费者读到。

### 4.3 reclaim 安全边界
- 只 reclaim `assigned` 但未 ack 的 offset（已 ack 的进 `acked`，不在分配表）。
- 不 reclaim 正处于 `retries` backoff 的 offset（本地 Blocked 集，reclaim 跳过）。
- 改派是「邀请」：ra 改了分配，C2 下次 poll 才真正取走 body；若 C2 取走前也崩了，回池再分配，
  不丢消息。

## 5. rollout / 兼容

### 5.1 与 `cross_node_delivery` 的关系（解耦）

- `cross_node_delivery`（`broker_queue` router 开关，默认 **false**）：控制**消息体**能否跨
 节点 forward。MQ-15 的 body 读取依赖它。
- `cross_node_subscribe_ok` 当前**无条件**拒绝跨节点 shared（不管 `cross_node_delivery`）。
- MQ-15 把分配搬进 ra 后，分配层面重复被根除，这层拒判可放宽：只要 ra 分配路径可用就放行。
  但 body 读取仍受 `cross_node_delivery` 制约。

**结论**：MQ-15 引入自己的 feature flag，与 `cross_node_delivery` 解耦。两者都开才有完整跨
节点 shared body 投递；只开 MQ-15 flag 不开 `cross_node_delivery` 时，分配已集群化但 body
仍限本节点（非 owner 消费者 stall，有指标可见）——可接受的中间态。

### 5.2 Feature flag

新增 `broker_subscription:cluster_shared_rebalance`，默认 **`false`**：

- `false`：现状行为，`cross_node_subscribe_ok` 对跨节点 shared 仍拒绝，单机 shared 完全不变。
  零回归。
- `true`：ra 分配路径启用，`cross_node_subscribe_ok` 对 shared 放宽为「ra 元数据组可用即放行」。

复用现有 env 读取模式（同 `shared_state_max` 等，`broker_subscription_worker.erl:369`）。

### 5.3 `cross_node_subscribe_ok` 改造

现状（`:952-963`）：shared 跨覆盖分区任一非本 owner → `{error, {cross_node_shared_unsupported, P}}`。

改为：
```erlang
cross_node_subscribe_ok(shared, Partitions) ->
    case application:get_env(broker_subscription, cluster_shared_rebalance, false) of
        false ->
            %% 旧行为：ra 分配路径未启用，拒绝跨节点 shared
            case persistent_term:get({broker_subscription, owner}, undefined) of
                Owner when is_function(Owner, 1) ->
                    case [P || P <- Partitions, not owner_is_self(Owner, P)] of
                        [] -> ok;
                        [P | _] -> {error, {cross_node_shared_unsupported, P}}
                    end;
                _ -> ok
            end;
        true ->
            %% ra 分配路径：分配经 ra 串行化，跨节点安全
            case ra_metadata_available() of
                true -> ok;
                false -> {error, {shared_rebalance_unavailable, ...}}
            end
    end.
```

### 5.4 rollout 顺序（plan 阶段细化）

1. ra 命令集 + meta_machine 扩展（`shared_assignments` 键 + 4 命令），默认 flag off，行为不变。
2. worker claim/ack/release 改为经 ra，但 flag off 时走本地 ETS 快路径（双路径，开关切换）。
3. flag on：跨节点 shared 放行，单机/多节点行为验证。
4. 阈值触发再均衡 + 跨节点 body 路由联调。
5. CT：单机 shared 回归（flag off）+ 多节点 shared 分配不重复（flag on）+ reclaim 正确性。

## 6. 不变量（契约 freeze）

1. **互斥不变量**：任一 offset 在任一时刻最多被一个 ClientId 持有（ra 串行 apply 保证）。
2. **no-low-jump**：只有 ack 的连续前缀可推进 `low`；分配/再均衡不碰 `low`。
3. **bounded window**：`map_size(acked) + map_size(assigned) ≤ shared_state_max`；claim 满即
   `{error, overflow}`。
4. **no-duplicate-delivery**：一条 QoS0 消息恰好到达一个 shared 成员。
5. **crash-release**：成员/节点崩溃后其 assigned offset 回池，可被其他成员 claim，不丢、不卡 `low`。
6. **retries 本地化不破坏一致性**：本地 retries/backoff 丢失只影响性能（重复消费），不违反互斥。

## 7. 测试计划

### A. broker_cursor 纯函数单测（ra apply 内复用）
- 现有 `shared_claim_is_disjoint_across_members`、`claim_does_not_move_low`、
  `shared_state_max_refuses_more_claims`、`claim_stops_before_the_watermark`、`a_qos0_publish_reaches_exactly_one_shared_member`、
  `shared_retry_waits_for_the_backoff_before_reclaim` 直接复用（算法没变，只调用点从 worker
  改为 ra apply）。
- 新增：`release`/`release_offset`/`settle` 在 ra 集合运算下的边界（空 assigned、重复 release、
  release 不存在的 ClientId）。

### B. broker_subscription_worker_SUITE（flag off 回归 + 双路径等价）
- 现有全过（flag off 走本地 ETS，行为不变）。
- 新增 flag on：claim/ack/release 经 ra 路径，与本地路径**逐条等价**（同一组操作序列，两种
  模式产出相同 assigned/acked/low）。

### C. 多节点 dist SUITE（新增，核心）
`broker_subscription_shared_dist_SUITE.erl`：
- 两个消费者连不同节点、同一 `$share/group/topic` → 订阅被放行（flag on），分配经 ra。
- 并发 claim：两节点同时 claim → offset 不重叠（互斥不变量，直接断言两 assigned 表无交集）。
- QoS0 发布 → 恰好一个成员收到（no-duplicate）。
- 一节点崩溃 → 其 offset 回池 → 另一节点消费者 claim 到并投递（crash-release，no-loss）。
- 跨节点 body 路由：消费者在节点 A、分区 owner 在节点 B → `read_from` forward 取到 body。

### D. 阈值触发再均衡 SUITE（新增）
- 构造失衡（C1 持有 N 个 in-flight，C2 空闲）+ cooldown 到期 → `shared_reclaim` 把超出均值的
  offset 改派给 C2 → C2 收到、C1 不再发。
- hysteresis/cooldown 闸门：未超阈值不触发、cooldown 内不重复触发。

### E. rollout/兼容
- flag off：跨节点 shared 仍被拒（`{cross_node_shared_unsupported, P}`），单机 shared 全回归绿。
- flag on 但 `cross_node_delivery` off：分配集群化、owner 消费者正常、非 owner 消费者 stall 有
  指标（中间态可观测）。

## 8. 边界与取舍（spec 明列）

- ra 可用性是 flag on 的前置：ra 不可用时跨节点 shared 拒绝（`{shared_rebalance_unavailable, _}`），
  单机不受影响。
- `low` 永不因再均衡回退（再均衡只动 assigned 中的高 offset，已 ack 前缀不动）。
- retries 本地化的已知取舍：节点崩溃丢失本地 backoff，offset 可被立即重新 claim（可能比 backoff
  更早重投），可接受（QoS1 语义允许，QoS0 无所谓）。
- ra 热路径开销：每条 claim/ack/release 过一次 ra（已接受的决策代价），claim 仍按 `pull_page_limit`
  （默认 32）批量摊薄。
- 与 `cross_node_delivery` 的叠加：完整跨节点 body 投递需两者都开；只开 MQ-15 flag 是分配集群化、
  body 本节点的中间态（可观测、可接受）。

## 9. 不在本文

- Plan 阶段细化：ra 命令的精确 epoch 版本号方案、`shared_assignments` 的快照压缩（大 group 时
  assigned 表膨胀）、rebalance tick 的独立进程 vs 复用 poll tick、双路径（flag on/off）的代码
  组织（避免逻辑重复）。
- 实现顺序与 task 拆分：见后续 writing-plans 产出的 plan。
- MQ-09 / MQ-11：独立 spec，另文。
