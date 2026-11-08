# ADR-019：Shared 分配、ack 归属与会话文件按 SubRef 布局

- **状态**：**已采纳（S2-D 决策冻结，2026-11-07）**——用户签认 D-1 / D-2 / D-3；后续修改须走 ADR 修订流程
- **日期**：2026-11-07
- **关联**：README §5.2/§7.2；roadmap §4.1；**ADR-013（本 ADR §19.1/§19.2/§19.3 修订其 §13.2/§13.3/§13.5）**、ADR-012（`.inflight` 形状，本 ADR 不动它）、ADR-015（重投递，本 ADR 把重投递的时机划给它）、ADR-010（Router 只要求 `sub_ref()` 不透明）；设计文档 `docs/design/s2-single-node-kernel.md` §4.4
- **修订**：`amends` ADR-013 §13.2（cursor 行形状）、§13.3（Shared 的分配路径）、§13.5（会话文件布局与迁移顺序）

## 背景（Context）

ADR-013 在 S2 M0 冻结了订阅三类型（`exclusive` / `shared` / `failover`）的语义，以及 Shared 的 ack 形状（per-message ack + 最小连续前缀，O1）。它没有冻结的部分在 S2-D 开工时暴露成三个必须定案的缺口：

1. **一条消息怎么落到组内唯一一个 consumer 上**。ADR-013 §13.2 规定 cursor 行是「位图 + 最小连续前缀」，§13.3 只说「按 `order` 轮询选择在线且未超 inflight 上限的 consumer」。但当前实现里，**每个连接从自己 cursor 的 `min` 开始 pull**（J6：cursor 是唯一读位置）。Shared 组共享同一个 SubRef 与同一个 cursor，于是两个成员的连接会从同一个位置读，**读到同一条消息**。轮询「选择」如果没有对应的「领取」动作，就只是决定谁先读到同一条。缺的不是选择规则，是**分配**。
2. **ack 者是谁**。ADR-013 §13.2 明确「不记录是谁 ack 的」，理由是重投递只需知道「是否已 ack」。在 Pulsar 式从 broker 推送的模型里这成立；在本项目「连接自己 pull」的模型里，重投递必须知道**回给谁**——`assigned` 与 `acked` 是两件事，前者是活的分配，后者是已完成的事实。
3. **文件按 SubRef 之后，`.inflight` 怎么办**。ADR-013 §13.5 冻结「文件名按 SubRef 的稳定编码」，但 `.inflight` 是 conn FSM 的不透明 blob，一条 pending 消息可以同时属于多个 SubRef（同一条消息命中同一客户端的两个 filter 时只发一帧、带 `[SubRef]`）。按 SubRef 拆 `.inflight` 无处安放这条 pending。

三项由用户拍板（D-1 / D-2 / D-3），本 ADR 冻结实现契约。

## 决策（Decision）

### 19.1 D-1：Shared 的分配由 worker 原子 claim（修订 §13.2 / §13.3）

**候选与取舍**（签认时的三个选项）：

| 方案 | 机制 | 结论 |
|---|---|---|
| **worker 原子 claim** | conn 每轮向 worker 发一次 `claim/4`，worker 在单个 gen_server 里分配一页 offset 并记入 `assigned => Consumer` | **采纳** |
| dispatch 推指定 offset | 发布路径选好 consumer，把具体 offset 推给它 | 拒绝：与 J6「推送只是提示、cursor 才是读位置」直接冲突，要改 `broker_deliver` 的位置语义 |
| 组内共享 `next_read` 计数器 | `ets:update_counter` 原子取号，无进程调用 | 拒绝：拿不到「谁拿走了哪条」，consumer 死亡后无法精确重投，且与位图/前缀语义对不齐 |

**cursor 行形状**（`?CURSORS` 表值）：

```erlang
%% shared
#{kind     := shared,
  low      := offset(),                      %% 最小未 committed 位置；committed = low - 1
  acked    := #{offset() => true},           %% 已 ack 但还没被前缀吃掉的
  assigned := #{offset() => consumer()}}     %% 已领走、尚未 ack 的

%% exclusive / failover（不变，仅多一个 kind 字段）
#{kind := exclusive | failover,
  offset  := offset(),                       %% 下一个待读位置
  pending := [offset()]}                     %% 乱序 ack 缓冲
```

`kind` 显式入行，避免调用方用「有哪个键」猜类型。`committed` 的算法对 shared 是 `low - 1`、对其余是 `offset - 1`，与 ADR-013 §13.2 的表一致。

**`claim/4` 契约**：

```erlang
%% Consumer 是领取者：`assigned' 必须记下「谁领走了」，否则 ack 归属校验（§19.2）
%% 与成员死亡时的回收（§19.2 第三段）都没有依据。
claim(SubRef, PartitionId, Max, Consumer) -> {ok, [offset()]} | {error, term()}.
```

- 在 worker 进程里执行（`gen_server:call`），**一次调用内完成整页分配**，因此对同一 SubRef 的并发 claim 是串行的、无竞态——这是选它而不是 `ets:update_counter` 的全部理由。
- 从 `low` 起，取前 `Max` 个**既不在 `acked` 也不在 `assigned`** 的 offset，升序返回，并全部记入 `assigned => Consumer`。
- **不推进 `low`**。`low` 只由 ack 的连续前缀推进。分配与完成是两件事。
- 尾部返回 `{ok, []}`（本次没东西可领，不是错误；空页与 `overflow` 是两种不同的信号，前者是常态、后者是拒绝）。
- **不越过分区高水位**：`claim` 在 worker 内取 `broker_queue:watermarks/1` 的 `High`，返回的 offset 一律 `< High + 1`。否则会为日志里还不存在的 offset 建 `assigned` 条目——它们永远不会被 ack，`low` 就此被钉死，`committed` 也不再前进。水印取不到时返回 `{error, {watermark, P, Reason}}`，调用方本轮退化 `idle`（poll tick 重试），不崩。
- 调用方**必须**把领到的 offset 用完或让它们随连接死亡回到未分配；它不得丢弃后重新 claim 同一批（已 `assigned` 的不会被再发出去，重 claim 只会拿到**新**的一页，把上一页变成泄漏的悬挂分配）。
- **返回集合可能不连续**（`acked` 里的空洞会让它跳过几个 offset）。调用方按 `[hd | tl]` 的区间读一次日志再按集合过滤即可：区间宽度 ≤ `Max + |acked|`，而 `|acked|` 已被 `shared_state_max` 收口，所以读取量有界。

**上界（故意的失败模式）**：`shared_state_max`（默认 `4096`）限制 `map_size(acked) + map_size(assigned)`。达到上界时 `claim/4` 返回 `{error, overflow}`，conn 本轮不投递、记 `shared_window_full` 计数并保持连接。**不驱逐、不静默丢**——与 Retain 上限（ADR-018 §18.2）同一姿态：驱逐会把「已 ack 的事实」变成随机值，拒绝是可观测且有界的。这个上界也是「一个卡住的 consumer 不会让组的状态无限增长」的机制回答（ADR-013 §13.2 的动机）。

**`acked` 的表示**：有序 offset 集合（`#{offset() => true}`），**不是** ADR-013 §13.2 写的 256 位分块位图。理由：`shared_state_max` 已经给窗口收口，窗口内是稀疏集合；位图需要块索引、左侧块丢弃和快照编解码三套代码，在低密度下纯亏。语义（per-message ack + 最小连续前缀）不变，**表示**是 S2-I 的事：若压测显示位图值得，按 ADR 修订换表示，不动契约。

**代价（明写）**：pull 路径每**页**多一次 `gen_server` call（不是每条一次）。`pull_page_limit = 32`，所以每 32 条一条 call。worker 因此成为 Shared 组内的唯一分配点——S2-I 必须用「消费者数 × 分区数」两维压测它（Gate 2 的证据之一）。Exclusive 与 Failover 不经过 claim，路径与 S1 完全一致。

**failover 的 active 选择**（补 §13.3 的实现缺口）：不建 ADR-013 §13.1 提到的 `CONSUMERS` 表。成员顺序就是 SubRef 的 `.subs` body 里的有序 `consumers` 列表（加入顺序即 `order`）；active = 列表里**第一个有 live attachment 的**成员。standby 不 pull、也不允许 ack（§19.2）。这样成员表只有一份真相（`.subs` 内容），power 也不依赖进程重启顺序。

### 19.2 D-2：ack 带 consumer、校验归属、死亡清回未分配（修订 §13.3）

**签名**：`ack/3` 改为 `ack/4`，`ack/3` **删除**（不留 wrapper——两条 ack 路径必然漂移）。

```erlang
ack(SubRef, PartitionId, Offset, Consumer) -> ok.
```

**归属校验**：

| Kind | 校验 | 不符时 |
|---|---|---|
| `shared` | `assigned[Offset] =:= Consumer` | `ok` + `ack_unowned` 计数 |
| `exclusive` | `Consumer =:= Key`（Key 就是 ClientId） | 同上 |
| `failover` | `Consumer =:= 当前 active` | 同上 |

**永不返回错误，永不断连**——沿用现有 `ack` 的立场（「ack 者是 view 可能落后一次 UNSUBSCRIBE 的 conn，把竞态变成协议错误不划算」）。不符的 ack 是幂等 no-op：不改 `acked`、不改 `assigned`、不改 `low`。

**consumer 死亡 → 清回未分配**：`do_detach` 与 `'DOWN'` 走同一条路径（`do_detach` 已经是 pid 守卫后的唯一出口）。在该 consumer 名下所有 shared 行的 `assigned` 里删掉它的 offset，回到「既没 acked 也没 assigned」；下一轮别的成员 claim 时自然重投。实现是 fold `?CURSORS`（行数 = 订阅数 × 分区数，有界）；**不建反向索引表**——那是第二份 `assigned` 真相（ADR-013 §13.1）。

**S2-D 不加重试定时器。** 重投只来自两条**已经存在**的路径：consumer 死亡（本节的清理）与连接重连（§4.2 的 session resume）。重试上限、退避、DLQ 是 ADR-015 的 S2-E。理由不是「先省事」——S2-E 的 DLQ 需要按 `{SubRef, Offset}` 记重试次数才能判定「投毒消息」，而时间触发的重投**必须先有那个计数器**，否则它就是一个永远重投、永远不升级的循环。S2-D 先放一个不带计数器的定时器，S2-E 必须把它整个重写；不做。

**wake（`dispatch/2`）的归属**：

- Shared 的**唤醒**发给组内**所有** attached 成员：wake 只是提示（J6），谁真拿到 offset 由 `claim` 决定。多唤醒几个成员的成本是多几次空 claim，换来的是「不依赖某一层猜谁该干活」。
- **QoS0 是例外，必须唯一投递**：QoS0 消息不进日志，没有 offset 可 claim，`dispatch` 的推送**就是**投递。所以 Shared 的 QoS0 只推给 `order` 最小且 attached 的成员（与 failover 的 active 同一规则）。同一规则的两种情况，不是两个逻辑。

### 19.3 D-3：`.subs`/`.cursors` 按 SubRef，`.inflight` 按 client（修订 §13.5）

**文件名**：`"<kind>.<b64url(Key)>.<b64url(Filter)>.subs"`（`.cursors` 同形）。例：`exclusive.YzE.s3BvcnQvIw.subs`。

- 不用 `base64url(term_to_binary(SubRef))` 作名：`term_to_binary` 的输出跨 OTP 版本不保证稳定，而文件名必须能在**任何一次 boot 之后**与旧文件对上；顺带人类可读，运维能按组名/客户端定位。
- `b64url` 字母表不含 `.`，分隔无歧义。名字只是地址，**内容才是真相**（沿用 ADR-013 §13.5 的原则）。

**`.subs` v2 body**：

```erlang
{vsn, 2, SubRef, #{kind       := sub_kind(),
                   key        := binary(),        %% ClientId | Group
                   filter     := binary(),
                   qos        := 0..2,
                   consumers  := [binary()],      %% 有序成员表；exclusive 恒 [Key]
                   init_cursors := [{partition_id(), offset()}],
                   created_at := integer()}}
```

`consumers` 的顺序**就是** failover 的 `order`。不另存 `weight`：S2-D 只按加入顺序选 active/轮询，ADR-013 §13.1 的权重留到有真实需求再按 ADR 修订加，不预埋无用字段。

**`.cursors` v2 body**：

```erlang
{vsn, 2, SubRef, #{kind := sub_kind(),
                   rows := #{partition_id() => row()}}}   %% row() 即 §19.1 的 ETS 行
```

`assigned` **不落盘**。它是活的分配，崩溃后必须全部重投——落盘会把它变成「崩溃前谁该收到」的第二份真相，而那个真相在进程全死之后没有意义，只会把消息永久钉在一个不存在的 consumer 名下。落盘的只有 `low`（或 `offset`）与 `acked`（或 `pending`）。

**`.inflight` 不改**：仍是 `{vsn, 1, ClientId, Term}`，仍按 client 命名，仍是 §5.3 的第三层（R7：不透明，可能回退）。理由在背景第 3 条：它是 conn FSM 的 blob，天然属于连接；按 SubRef 拆无处安放一条属于多个 SubRef 的 pending。

**boot 索引**：从 `.subs` 内容重建 `?MEMBERS`（`{ClientId, SubRef}` 表，给 `subscriptions/1` 与新形状下的 `group_by_client`）。**Group → members 不建索引**：成员顺序已经在 SubRef 的 body 里，再建一份就是第三个真相。

**`list_sessions/0` → `list_subscriptions/0`**：返回 `[{SubRef, Body}]`，只枚举 `*.subs`。`.cursors` 无对应 `.subs` 的是孤儿，boot 时删除（沿用手册：`.subs` 是权威集合，§10.6）。

**v1 → v2 迁移**：`broker_subscription_files:migrate_v1/0`，在 boot step 2 最前面跑一次。

1. 读目录里所有 v1 `.subs` 与 v1 `.cursors`，在内存里算出 v2 全集（v1 的 `{ClientId, Filter}` → `{exclusive, ClientId, Filter}`，`.cursors` 的 per-filter 行原样搬进新文件的 `rows`）。
2. 逐个 `temp + fsync + rename` 写 v2 文件。
3. **全部成功后**才删 v1 文件。
4. 中途崩溃 ⇒ 两边都在，下次 boot 重写**相同内容**并再删 v1（幂等，不丢）。任何一步失败 ⇒ 拒绝启动，错误含路径。

顺序不可颠倒：先写后删，崩溃的两种结果分别是「多做一次」和「丢了一个订阅」，只有前者可接受。

**删除路径**：`delete_client/1` 拆成 `delete_subscription/1`（`<subref>.subs` + `<subref>.cursors`）与 `delete_inflight/1`（`<b64url(ClientId)>.inflight`）。worker 的 clean-session teardown 已经有实体列表，逐个 SubRef 删，**不按文件名模式枚举**——模式枚举会把「名字与内容不一致」的损坏文件一起删掉，而那正是应该被报出来的东西。

### 19.4 未改动

- §13.1 的三种 Kind、`SubRef` 三形状与「cursor 归属 SubRef」不变；`?ENTITIES` 仍以 SubRef 为 key。
- §13.4 的 `$share` 语法仍归 S2.5：S2-D 的 Shared/Failover 只由内部 API 与内核测试覆盖，MQTT 3.1.1 的连接仍然只能创建 Exclusive。
- §13.6 的 committed 仍是全局每分区一个水位，`min(所有 live SubRef 的 committed)`。
- Router 形状不变（ADR-010 只要求 `sub_ref()` 不透明 `term()`）。
- `.inflight` 形状（ADR-018 §18.3 R3）不动。

## 后果（Consequences）

- **+** Shared 有了确定的「一条消息只给一个 consumer」机制，不再依赖两个连接读同一位置后靠时序去重。
- **+** 「cursor 是唯一读位置」（J6）保住：claim 只分配**哪些 offset 归你**，位置仍是 cursor 行。
- **+** `assigned` 不落盘，崩溃后的语义简单一致：所有未 ack 的 offset 全部重投，至少一次。
- **+** 会话语义与文件布局对齐：`SubRef` 是 cursor 的 owner，文件名就是 SubRef；`.inflight` 留在 client，因为它本来就是连接的。
- **+** 迁移是先写后删的幂等流程，崩溃只会多做一次。
- **-** pull 每页多一次 `gen_server` call；worker 是 Shared 组的分配热点（S2-I 量化）。
- **-** `acked` 用集合而非位图，窗口内低密度下内存/编码略大于位图；换表示需 ADR 修订。
- **-** 运维按客户端找文件需要工具（文件名是 SubRef 不是 ClientId）。
- **-** consumer 死亡的重投是**立即**的（下一轮 claim），没有退避；投毒消息在 S2-D 会把组窗口耗尽（`shared_window_full`）而不是被隔离。这是 S2-E 的 DLQ 要解决的，本 ADR 只保证它是**有界且可观测**的失败。
- **-** v1 数据被 S2-D 打开后无法回退到 S1（单向迁移，与 §13.5 一致）。

## 验证（Validation）

- **形状**：`sub_ref()` 三形状的铸造与解析；`{exclusive, ClientId, Filter}` 与 S1 的 `{ClientId, Filter}` 一一对应（迁移等价性）。
- **claim**：并发两个成员各 claim 一页，两组 offset **不交集**；`low` 不动；`{ok, []}` 在尾部；`shared_state_max` 触发 `{error, overflow}` 且计数；claim 返回的 offset 全部 `=< High`（不越过高水位）；返回集合允许不连续但仍升序。
- **committed**：`low` 只在连续前缀前进；乱序 ack（ack 第 5 条再 ack 第 3 条）不推进 `low`；空洞填满后一次性推进；`low` 之前的 ack 幂等忽略。
- **归属**：非持有者 ack → `ok` + `ack_unowned`、状态不变；持有者 ack → 从 `assigned` 移入 `acked`。
- **死亡重投**：成员 A 领了 3 条不 ack 后退出 ⇒ 3 条回到未分配 ⇒ 成员 B 下一轮 claim 全部领到；A 迟到的 ack 不推进任何东西。
- **不重复**：N 个成员对同一分区并发 pull，每条消息**恰好**被投递一次（按 offset 集合断言，不按顺序）。
- **failover**：active 是最低 `order` 的 attached 成员；standby 的 claim/ack 全部 no-op；active 死亡后下一个顶上，从 `committed` 位置继续。
- **QoS0 + Shared**：一次发布只被组内**一个**成员收到。
- **迁移**：v1 `.subs`/`.cursors`（含一个客户端两条 filter）⇒ v2 文件逐个齐全、内容等价、v1 删除；损坏 v1 ⇒ 拒绝启动且错误含路径；迁移中途失败注入 ⇒ 重启后仍能从两份文件之一恢复（幂等）。
- **文件布局**：`.inflight` 仍按 client 命名且内容 vsn 不变；孤儿 `.cursors` 在 boot 时被删除。
- **回归**：Exclusive 的 `rebar3 ct` 全绿、T13 端到端双组 12/12、T14 崩溃矩阵 18/18、T15 真实客户端 10/10 保持不变。
- **文档**：`docs/storage-and-recovery.md` §9 会话文件布局、设计 §4.4 状态、ADR-013 顶部状态行。
