# MQ-11 per-subscriber 背压（慢消费者隔离）—— 设计

> 状态：**已实现（2026-11-24）**——§2 fair-share 配额 + `shared_consumer_quota_hit` + §3.4 retention lag 观测（`retention_lag_bytes` / `wal_gc:lag_bytes/2`）已落地；见 §10 实现记录。
> 缺口出处：`docs/compatibility/unifiedmq-mqtt-family-gap.md` 第 62 行（MQ-11）
> 日期：2026-11-24

## 1. 背景与三层现状

### 1.1 缺口措辞

缺口表（`unifiedmq-mqtt-family-gap.md:62`）：
> MQ-11 语义缺口：只有停投，没有背压。in-flight 默认 64，满了就停。不通知发布者，
> 不踢慢消费者。生产上慢订阅者会拖住分区。

doc §4.6（`mqtt-feature-comparison.md:166`，标 `[x]`）：
> **停投而非背压（MQ-11）**：in-flight 默认 64，满了即停投、不通知发布者、不踢慢消费者。
> 当前刻意只做「停投」；per-subscriber 背压/慢消费者隔离列为下方 P2 待办。

### 1.2 三层背压的完整现状（已逐层核实）

| 层 | 机制 | 默认值 | 现状语义 | file:line |
|---|---|---|---|---|
| **L1 mailbox（生产者侧）** | `mqtt_backpressure:producer/2`：入站 PUBLISH 前查 conn 邮箱长度，超限**断连**（无 PUBACK） | `publish_mailbox_limit=1000` | 超限 `{stop, {backpressure, mailbox}}` | `mqtt_conn_fsm.erl:1171`、`broker_proto_mqtt/src/mqtt_backpressure.erl` |
| **L2 per-conn in-flight（消费者侧）** | `mqtt_backpressure:consumer/3` + `inflight_limit/1`：pull round 前查 `pending` 数与邮箱；满则**暂停本轮**（`{pause, mailbox\|inflight}`），队列保留 | `max_inflight=64`，`min(max_inflight, receive_maximum)` | 满了停投、queued、不通知发布者 | `mqtt_conn_fsm.erl:2242`、`:1869` |
| **L3 shared 窗口（组级）** | `broker_cursor:claim`：`Room = Limit - (acked+assigned+retries)`，无 per-consumer 上限 | `shared_state_max=4096` | 一人慢占满窗口→全组 `shared_window_full`、都 claim 不到 | `broker_subscription_worker.erl:552`、`:567` |

### 1.3 逐层定性

- **L1（mailbox 断连）**：已有「踢慢消费者」的粗暴机制，有 `backpressure_disconnects` 指标。
  缺陷：只保护 broker 自身内存，不保护同组其他消费者，且是断连而非隔离。
- **L2（per-conn in-flight 停投）**：per-connection 独立，慢消费者只停自己，发布者照常拿 PUBACK
  （已解耦）。这不是缺口——exclusive/failover 已是正确的 per-subscriber 隔离。
- **L3（shared 窗口）**：**真缺口**。`shared_state_max` 是 per-`{SubRef, Partition}` 单行共享，
  `Room` 只看总量、无 per-consumer 上限。一个慢消费者 claim 一堆 offset 不 ack，全钉在
  `assigned`，占满 4096 窗口 → 同组**所有**消费者（含快的）都 `{error, overflow}`，全组停。
  这正是缺口「慢订阅者会拖住分区」的实际机制。

### 1.4 一个关键传导链（纠正直觉）

`inflight_limit = min(max_inflight, receive_maximum)`（默认 64），且 `pull_round` **先**做背压检查
（`consumer_verdict`），`{pause, inflight}` 时**根本不 claim**。所以单个消费者在 shared 窗口里的
`assigned` 自然被限在 ≈ `max_inflight`（64）。

推论：窗口被「一个慢消费者」占满，只在特定条件下发生：
1. `max_inflight` 被配得很大（深窗口部署），单个消费者就能 claim 上千；
2. 消费者数量 N 多（N × 各自 ~64 逼近 4096），总和耗尽；
3. 崩溃释放滞后：断连消费者的 `assigned` 未及时 release，多轮累积。

per-consumer 配额的真正价值：窗口耗尽时保证公平（不让一人拿超过份额）、深窗口部署的安全网、
崩溃累积的有界性。不是「防一个慢消费者拿满 4096」（正常路径 conn 侧已挡）。

## 2. shared 窗口的 per-consumer 隔离

### 2.1 为什么不用固定小上限

若 `shared_state_max=4096` 而 per-consumer 固定 64，2 个消费者的组只能用 `min(4096,128)=128`，
浪费 97% 窗口，严重伤吞吐。固定小值是错的。

### 2.2 fair-share 动态配额（推荐默认）

```
N = length(consumers of SubRef)          %% 从 ?ENTITIES 的 consumers 列表读
PerConsumerQuota =
    case env(shared_per_consumer_max, 0) of
        0 -> max(1, Limit div N)          %% 动态：均分窗口
        Fixed -> max(1, Fixed)            %% 运维显式固定上限
    end
```

- 默认动态均分：N=2 → 每个 2048；N=10 → 每个 ~410。负载均衡时**全窗口被用满**，且**单消费者
  永不超过 1/N**（防垄断）。
- 消费者减少（die/leave）→ N 减小 → 存活者配额自动放大（fair-share 自适应）。
- 可选 `shared_per_consumer_max > 0` 固定覆盖（运维要可预测上界时用）。

### 2.3 claim 算法改造

`broker_cursor:claim` 当前 `Room` 只看组总量，无 per-consumer 上限。改为加 `PerConsumerMax` 参数：

```erlang
%% broker_cursor.erl —— claim/7（或 claim/6 内增参）
claim(Row = #{kind := shared, low := Low, acked := Acked, assigned := Assigned},
      Max, High, Consumer, Limit, Blocked, PerConsumerMax) ->
    Retries = retries(Row),
    GroupRoom = Limit - (map_size(Acked) + map_size(Assigned) + map_size(Retries)),
    HeldByMe  = count_consumer(Assigned, Consumer),
    MyRoom    = PerConsumerMax - HeldByMe,
    if
        GroupRoom =< 0 -> {error, overflow};         %% 组窗口满（现状语义）
        MyRoom    =< 0 -> {ok, [], Row};             %% 本消费者到配额，非组满
        true ->
            Take = min(Max, min(GroupRoom, MyRoom)),
            {Offsets, New} = scan(Low, Take, High, Acked, Assigned, Blocked,
                                  Consumer, []),
            {ok, lists:reverse(Offsets), Row#{assigned := New}}
    end.
```

- `count_consumer(Assigned, Consumer) = length([O || {O,C} <- maps:to_list(Assigned),
  C =:= Consumer])`（O(≤4096)，可接受）。
- worker handler 从 `?ENTITIES` 读 N、算 `PerConsumerQuota`，传给 claim。

### 2.4 关键：区分「组满」与「个人到配额」

- `{error, overflow}`（组满）：保持现状，`shared_window_full` 指标，**全组**都拿不到。
- `{ok, [], Row}`（个人到配额）：**新语义**——该消费者本轮拿不到，但**其他消费者仍能 claim**。
  这是隔离核心：慢消费者到配额后让出窗口，快消费者继续。
- `ok []` 已被 pull 循环当作 `idle`（"the tail: not an error"），自然退避重试，无需新信号。

### 2.5 与 retries 的关系（边界）

shared offset 超时 → `schedule_retry` → `release_offset`（**移出 assigned、释放该消费者配额**）+
`retry`（进组级 `retries`）。所以 `retries` 不占任何消费者的 `PerConsumerMax`（只数 `assigned`）。
合理：超时是「恢复中」的组级状态，由 `backoff` + `max_attempts` → dead-letter 有界。

## 3. L1/L2 保持 + retention 钉住（新发现的真耦合）

### 3.1 L1/L2 保持现状（已正确隔离，不改行为）

- L1（mailbox 断连）：per-connection，超 `publish_mailbox_limit` 断连，保护 broker 内存。保持。
- L2（per-conn in-flight 停投）：per-connection，`{pause, inflight}` 时本轮不 claim。慢消费者只停自己，
  发布者照常 PUBACK。保持。

### 3.2 retention 被慢消费者永久钉住

调研发现一个缺口表没明说、但比 shared 窗口更严重的耦合：

- `wal_gc.erl` 硬规则：`committed = min(live cursor) - 1`（K6），**任何**存活 cursor（含慢消费者）
  钉住 `committed`。
- GC 不能删过 `committed` 的 WAL 段 → **一个慢消费者（或卡死的 exclusive 订阅者）能让磁盘无限
  增长**。
- 现状只有 `retention_blocked_by_slow_consumer` 计数器（观测），**无 remediation**。

这是「慢订阅者拖住分区」的另一面，比 shared 窗口更危险（窗口满只是停投，retention 钉住是磁盘爆）。

### 3.3 三个真耦合的隔离策略

| 耦合 | 机制 | 隔离手段 |
|---|---|---|
| **shared 窗口耗尽** | `shared_state_max` 一人占满全组停 | **per-consumer fair-share 配额**（§2） |
| **retention 钉住** | 慢消费者 cursor 卡住 `committed` → WAL 无限增长 | **retention 保护阈值告警**（本 spec 只观测，见 §3.4） |
| **mailbox 爆炸** | 慢消费者 conn 邮箱堆积 | 已有断连（L1），保持 |

### 3.4 retention：本 spec 只做可观测，不做「跳过」

`wal_gc` 已有 `retention_status/0`（per-partition blocked 原因）。本 spec 增强**只读观测**：

1. 新增 gauge `retention_lag_bytes`：最慢消费者落后 WAL 可删点的字节数。
2. `retention_status/0` 加 lag 字段。
3. 可选 env `retention_lag_warn_bytes`（默认 `0`=关）：落后超此值告警（只观测，不删数据）。
4. **`plan/3` 的删除决策不变**：`committed` 仍钉住，慢消费者钉住时 `deletes` 仍为空。

**`retention_slow_consumer_policy=skip`（跳过慢消费者强删）不纳入本 spec**。诚实记录原因：
- `wal_gc` 核心不变量 `committed` 永不越过任何 live cursor（K6），且 storage **从不反查
  subscription 层**（模块注释明写，避免依赖倒置）。
- `skip` 要实现「越过某慢消费者」，必须让 subscription 层把「哪些 cursor 可放弃」告诉 storage，
  **破坏依赖方向**。
- 这是独立的 retention 专项课题，不应塞进 MQ-11 背压 spec。缺口表标注「retention 钉住：已可观测，
  跳过策略待专项」。

### 3.5 可观测性增强（新增指标）

- `retention_lag_bytes`（gauge）：最慢消费者落后 WAL 可删点的字节数。
- `shared_consumer_assigned`（gauge，per-consumer）：每消费者在 shared 窗口的 `assigned` 数。
- `shared_consumer_quota_hit`（counter）：某消费者 claim 撞到 per-consumer 配额（非组满）次数——
  区分「个人慢」vs「组满」。
- 复用现有 `shared_window_full`（组满）、`backpressure_pauses`、`backpressure_disconnects`。

### 3.6 与 MQ-15 的关系

MQ-15 的 `shared_reclaim` 会把过载消费者的 offset 改派给空闲消费者——也是慢消费者隔离手段。但
MQ-15 是**跨节点再均衡**（阈值触发，为均衡吞吐），MQ-11 配额是**单组窗口公平**（常态，防垄断）。
两者互补：配额先保证不垄断，reclaim 再做精细再平衡。**MQ-11 可独立于 MQ-15 落地**（配额是单组内
的事，不需跨节点协调）。

## 4. rollout / flag / 兼容

### 4.1 env 与默认值

新增两个 env（均在 `broker_subscription`）：

1. **`shared_per_consumer_max`**（默认 **`0` = 动态 fair-share**）：
   - `0`：配额 = `max(1, shared_state_max div N)`，N 从 `?ENTITIES` consumers 读。
   - `>0`：固定每消费者上限。
   - 无「关闭」态：要等价关闭就设成 `shared_state_max`（N=1 的等价行为）。
2. **`retention_lag_warn_bytes`**（默认 `0`=关）：落后超此值告警 + 报 gauge，只观测不影响行为。

> 配额默认开是安全的：窗口未耗尽时 `MyRoom ≥ GroupRoom`，`Take = min(Max, GroupRoom, MyRoom)`
> 退化为现状，逐字节等价。只有窗口将满时才分流（安全网本意）。

### 4.2 兼容性

- L1/L2：零改动，行为不变。
- L3 配额：窗口未耗尽时逐字节等价现状；窗口耗尽时分流（期望语义改变，仅在压力下生效）。
- 无 consumers 列表 / N=0：`PerConsumerQuota = Limit`（退化现状），避免除零。
- 单消费者组：N=1 → 配额 = `Limit`，等价现状。

### 4.3 rollout 顺序

1. `broker_cursor:claim` 加 `PerConsumerMax` + `count_consumer/2`，worker handler 算配额传入。
   **先**用 `PerConsumerMax = Limit`（等价现状）跑全回归，证明零回归。
2. 切 `PerConsumerMax = Limit div N`，新增 `shared_consumer_quota_hit` 指标。
3. retention 观测：`wal_gc` 报 `retention_lag_bytes` gauge + `retention_status/0` 加 lag 字段（只观测）。
4. CT：fair-share 隔离（一慢多快）、单消费者等价、N 变化自适应。

## 5. 不变量（契约 freeze）

1. **窗口未耗尽时零回归**：`PerConsumerMax ≥ GroupRoom` 时 claim 结果与现状逐字节等价。
2. **公平份额**：任一消费者在 shared 窗口的 `assigned` 数 ≤ `PerConsumerQuota`（`Limit div N`
   或固定值）。
3. **组满 vs 个人到配额区分**：组满 → `{error, overflow}`（全组停，现状）；个人到配额 →
   `{ok, [], Row}`（仅该消费者本轮停，他人可继续）。
4. **配额自适应**：N 变化（join/leave/crash）后存活消费者配额按新 N 重算。
5. **无除零**：N=0 → 配额 = `Limit`（退化现状）。
6. **retention 只观测不改删**：本 spec 的 retention 改动只加指标，`plan/3` 删除决策不变。

## 6. 测试计划

### A. broker_cursor 纯函数单测（claim 配额）
- `claim` 带 `PerConsumerMax`：`count_consumer` 正确；`MyRoom = PerConsumerMax - HeldByMe` 生效；
  `PerConsumerMax ≥ GroupRoom` 时等价现状（逐字节断言相同 assigned）。
- 边界：`HeldByMe = 0`、`PerConsumerMax = 0`（守卫）、`GroupRoom` 与 `MyRoom` 谁先耗尽。

### B. broker_subscription_SUITE（worker 层）
- fair-share：N=2、窗口 8、配额 4；A 已持 4 后再 claim → `{ok, []}`（个人到配额），B 仍能 claim。
- 组满仍 `{error, overflow}`（配额之和 < Limit 时，人为构造 acked+assigned+retries 逼近 Limit）。
- 单消费者（N=1）→ 配额 = Limit，等价现状。
- N 变化：A、B 都在 → 配额 4；B leave → 配额变 8。

### C. 集成 / conn 层（mqtt_conn_SUITE 或新 suite）
- 一慢消费者（不 ack、占满自己配额）+ 一快消费者：快消费者持续收到消息，
  `shared_consumer_quota_hit` 增长，`shared_window_full` 不增长（证明隔离，非全组停）。
- L1/L2 回归：mailbox 断连、in-flight 停投语义不变。

### D. retention 观测
- `wal_gc` 报 `retention_lag_bytes` gauge（慢消费者钉住时 > 0）。
- `retention_status/0` 含 lag 字段。
- 断言 `plan/3` 的 `deletes` 在慢消费者钉住时仍为空（**不跳过**，证明只观测）。

### E. 等价性回归（最重要）
- 用「窗口未耗尽」的常规场景跑现有 `broker_subscription_SUITE` 全绿（配额不改变结果）。

## 7. 边界与注意

- 与 MQ-09 协调：重试把 timed-out offset 移出 `assigned`（`schedule_retry` 的 `release_offset`），
  **释放该消费者配额**——MQ-11 配额与 MQ-09 重试天然协同（慢消费者超时后配额被释放，其他消费者可用）。
- `count_consumer/2` 复杂度 O(≤4096)，每次 claim 一次，可接受（claim 本身已是 worker 单点串行）。
- retention `skip` 策略依赖倒置，明确 out-of-scope，留 retention 专项。
- `shared_per_consumer_max` 的无「关闭」态（等价关闭需设 `shared_state_max`）是刻意简化：配额是安全网，
  无需总开关；等价关不掉反而避免运维误配成 0 造成「每消费者配额 0 = 全停」的灾难（守卫 `max(1, ...)`
  已防）。

## 8. 与 MQ-09 / MQ-15 协调（记录）

- **MQ-09**：重试移出 assigned → 释放配额，天然协同（见 §7）。
- **MQ-15**：跨节点再均衡 vs 单组公平配额，互补、独立可落地（见 §3.6）。

## 9. 不在本文

- retention `skip` 策略（依赖倒置，独立专项）。
- Plan 阶段细化：`claim/7` 的精确签名与调用点、`shared_consumer_assigned` 的 per-consumer 上报方式
  （gauge 需带标签还是聚合）、配额与 MQ-15 reclaim 同时开启时的交互细节。
- 实现顺序与 task 拆分：见后续 writing-plans 产出的 plan。

## 10. 实现记录（2026-11-24）

- **`broker_cursor`**：新增 `claim/7`（`PerConsumerMax` 参数）与 `do_claim/7`、`count_consumer/2`；`claim/5`、`claim/6` 保留并委托（`PerConsumerMax =< 0` 等价关闭，逐字节等价现状）。`{ok, quota_hit, Row}` 为「本消费者到配额、组尚有空间」，与 `{error, overflow}`（组满）区分。
- **`broker_subscription_worker`**：`claim` handler 从 `?ENTITIES` 读 `consumers` 算配额 `shared_per_consumer_quota/2`（`shared_per_consumer_max > 0` 固定，否则 `max(1, Limit div N)`；N=0 退化 `Limit`）；`quota_hit` 计 `shared_consumer_quota_hit` 并回空 claim。
- **指标**：新增闭集键 `shared_consumer_quota_hit`（counter）与 `retention_lag_bytes`（gauge）。设计 §3.5 的 `shared_consumer_assigned`（per-consumer gauge）**未做**——它需要 per-consumer 标签，违反 ADR-016 的高基数禁令；按 §9 的 Plan 阶段待定，当前不引入。
- **retention 观测**：`wal_gc:lag_bytes/2`（纯函数，候选段中 `next > committed + 1` 的字节和）+ `retention_lag/0`（per-partition）；`sweep/1` 取最大 lag 写 `retention_lag_bytes`，`retention_lag_warn_bytes`（默认 0=关）超阈值告警。`plan/3` 删除决策未改（invariant 6）。
- **测试**：`broker_subscription_SUITE` 新增 4 个纯函数例 + 3 个 worker 例；`wal_gc_SUITE` 新增 2 例（lag 纯函数 + 进程视图）；`broker_metrics_SUITE` 闭集更新。
- **未做**：`shared_consumer_assigned` per-consumer gauge（高基数，ADR-016）；retention `skip` 强删（依赖倒置，独立专项，§3.4/§9 已声明 out-of-scope）。
