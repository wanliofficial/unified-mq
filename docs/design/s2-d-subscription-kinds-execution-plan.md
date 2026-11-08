# S2-D 执行计划：订阅语义与 cursor 重构

- **状态**：**计划冻结（2026-11-07）**；实现未开始。决策见 [ADR-013](../adr/013-subscription-kinds-and-cursor-ownership.md)（原冻结）与 **[ADR-019](../adr/019-shared-assignment-ack-ownership-and-session-layout.md)**（修订 §13.2/§13.3/§13.5）。
- **对应**：`docs/design/s2-single-node-kernel.md` §4.4 / §5（M3）；roadmap §4.3 Gate S2（M3 结构前提）
- **估算**：10 人日（T-D0 ~ T-D8）
- **退出条件**：三种订阅类型的投递归属有确定性用例；Shared 的「一条消息只投给组内一个 consumer」在 N 个成员并发 pull 下可机械断言；`low` 的连续前缀在乱序 ack / 空洞 / 成员退出三场景正确；`.subs`/`.cursors` v2 与 v1 迁移演练通过；`broker_subscription` 的公共 API 变更在 `mqtt_conn_SUITE` + `s1_e2e_SUITE` 全绿；T14 崩溃矩阵含 Shared 场景；全量回归零 FAILED。

---

## 1. 目标与非目标

**目标**：把 S1 唯一的 `{ClientId, Filter}` 独占语义扩展为 ADR-013 冻结的三种 Kind，并解决 S1 形状在 Shared 上必然暴露的两个结构问题（读位置冲突、组内水位被慢 consumer 钉住）。

1. **`SubRef = {Kind, Key, Filter}`**，cursor 的 owner 就是 SubRef，不出现第二份 cursor 真相。
2. **Shared 的分配**用 worker 原子 `claim/4`：一次领一页 offset 记入 `assigned`，pull 只读领到的那些（ADR-019 §19.1）。
3. **ack 归属**由 `ack/4` 的 consumer 参数校验；consumer 死亡把它名下未 ack 的 offset 清回未分配（ADR-019 §19.2）。
4. **会话文件按 SubRef 布局**，`.inflight` 保持按 client；v1→v2 迁移在 boot step 2 内先写后删、幂等（ADR-019 §19.3）。
5. 保持三条既有硬契约不变：cursor 是唯一读位置（J6）；committed 只在 cursor 文件落盘后推进（K6）；Router 只做 `Filter → [SubRef]` 索引（ADR-010，零改动）。

**非目标**：

- `$share/{group}/{filter}` 线上语法（ADR-013 §13.4 明确归 S2.5）。S2-D 的 Shared/Failover 只由内部 API + 内核测试覆盖；MQTT 3.1.1 连接仍然只能创建 Exclusive。
- 重试定时器、退避、重试计数、DLQ、TTL（ADR-015 的 S2-E）。S2-D 的重投只有两条来源：consumer 死亡、连接重连。
- Shared 位图的位级表示（ADR-019 §19.1：`shared_state_max` 已收口窗口，位图换表示留给 S2-I 的压测结论）。
- 消费者权重（ADR-013 §13.1 的 `weight`）：S2-D 只按入组顺序，不预埋字段。
- 跨节点：`claim` 是单机 gen_server 内的原子操作；分布式分配是 S4 的 Raft 组内事务。

**关键语义（实现据此）**：

| 项 | 冻结值 | 依据 |
|---|---|---|
| Shared 的 `committed` | `low - 1`，`low` 只由连续 acked 前缀推进 | ADR-013 §13.2；ADR-019 §19.1 |
| `assigned` 是否落盘 | **否** | ADR-019 §19.3（落盘会把「谁该收到」变成第二份真相） |
| Shared 状态上界 | `shared_state_max = 4096`，超限 `claim` 返回 `{error, overflow}` | ADR-019 §19.1 |
| 非持有者 ack | `ok` + `ack_unowned` 计数，状态不变，不断连 | ADR-019 §19.2 |
| Shared 的 QoS0 | 只推给 `order` 最小且 attached 的成员 | ADR-019 §19.2（QoS0 无 offset 可 claim，推送即投递） |
| Failover 的 active | 成员列表中第一个有 live attachment 的 | ADR-019 §19.1 |
| Standby | 不 claim、不 pull、ack 一律 no-op | ADR-013 §13.3；ADR-019 §19.1 |
| `.inflight` | 仍按 client、仍 `vsn 1` 信封 | ADR-019 §19.3；ADR-018 §18.3 R3 |

---

## 2. 冻结接口

### 2.1 `sub_ref()`（`broker_subscription`）

```erlang
-type sub_kind() :: exclusive | shared | failover.
-type sub_ref()  :: {sub_kind(), Key :: binary(), Filter :: binary()}.
%% exclusive: Key = ClientId；shared/failover: Key = Group
```

形状确定性由 `{Kind, Key, Filter}` 保证，boot step 2 能从 `.subs` 内容重铸出与崩溃前完全相同的元组（K2 的原始理由不变）。

### 2.2 公共 API（`broker_subscription` 门面）

| 函数 | 语义 |
|---|---|
| `subscribe(ClientId, Filter, QoS) -> {ok, {exclusive, ClientId, Filter}}` | MQTT 3.1.1 的 SUBSCRIBE 走这条，签名不变、返回形状变 |
| `join(Kind, Group, ClientId, Filter, QoS) -> {ok, SubRef}` | Shared/Failover 入组（`Kind ∈ {shared, failover}`）；SubRef 不存在则创建，存在则把 consumer 追加到成员表尾 |
| `unsubscribe(ClientId, Filter) -> ok` | 摘除该客户端的 **exclusive** 订阅（3.1.1 的 UNSUBSCRIBE） |
| `leave(SubRef, ClientId) -> ok` | 从组订阅摘除一个 consumer；最后一人离开时删除 SubRef（实体、cursor、Router、文件） |
| `ack(SubRef, Partition, Offset, Consumer) -> ok` | **`ack/3` 删除**；归属校验见 ADR-019 §19.2 |
| `claim(SubRef, Partition, Max, Consumer) -> {ok, [Offset]} \| {error, term()}` | 只对 `shared` 有效；其余 Kind 返回 `{error, wrong_kind}` |
| `role(SubRef, ClientId) -> active \| standby \| not_consumer` | failover 的 active 判定；exclusive 恒 `active`（当它是 owner） |
| `subscriptions(ClientId) -> [subscription()]` | 该客户端**作为 consumer** 的所有订阅（跨 Kind） |
| `cursor(SubRef, Partition) -> {ok, offset()} \| {error, term()}` | 返回 `low`（shared）或 `offset`（exclusive/failover） |
| `session/1`、`inflight/2`、`attach/3`、`detach/2`、`dispatch/2` | 签名不变，`dispatch/2` 的归属规则见 §2.5 |

`subscription()` 增两个字段（`cursors` 的 offset 语义按 Kind）：

```erlang
#{sub_ref := sub_ref(), kind := sub_kind(), key := binary(),
  filter := binary(), qos := 0..2, cursors := [{partition_id(), offset()}]}
```

### 2.3 cursor 行（`?CURSORS` 表值）

```erlang
%% shared
#{kind := shared, low := offset(),
  acked := #{offset() => true}, assigned := #{offset() => consumer()}}
%% exclusive | failover
#{kind := exclusive | failover, offset := offset(), pending := [offset()]}
```

`kind` 显式入行：调用方不得用「有哪个键」猜类型。

### 2.4 `.subs` / `.cursors` v2（`broker_subscription_files`）

```
文件名： "<kind>.<b64url(Key)>.<b64url(Filter)>.subs" / ".cursors"
.subs   {vsn, 2, SubRef, #{kind, key, filter, qos, consumers, init_cursors, created_at}}
.cursors{vsn, 2, SubRef, #{kind, rows := #{Partition => row()}}}
.inflight  不变：{vsn, 1, ClientId, Term}，文件名 b64url(ClientId)
```

`rows` 里 **不含 `assigned`**。`list_subscriptions/0` 返回 `[{SubRef, Body}]`（只枚举 `*.subs`）；孤儿 `.cursors` 在 boot 时删除。

### 2.5 `dispatch/2` 的归属

| 位置型（`{P, O, Msg}`，durable 消息的 wake） | 非位置型（`{undefined, undefined, Msg}`，QoS0 的**投递**） |
|---|---|
| Exclusive/Failover：发给 owned / active 的 attached pid | 同上 |
| Shared：发给组内**所有** attached 成员（wake 只是提示，`claim` 决定谁拿到） | 只发给 `order` 最小且 attached 的**一个**成员 |

两种位置型的收件人集合按 ClientId 合并（H7）：同一客户端多条 SubRef 只收一帧，QoS 取最大值。

### 2.6 配置与指标

| key | 默认 | 说明 |
|---|---|---|
| `broker_subscription.shared_state_max` | `4096` | 单 `{SubRef, Partition}` 的 `acked ∪ assigned` 上界 |
| `shared_claims` | counter | `claim/4` 成功返回非空页的次数 |
| `shared_ack_unowned` | counter | 归属校验失败的 ack 次数 |
| `shared_window_full` | counter | `claim/4` 因 `shared_state_max` 返回 overflow 的次数 |
| `failover_promotions` | counter | standby 顶上成为 active 的次数 |

---

## 3. 交付物

| 文件 | 变更 |
|---|---|
| `docs/adr/019-*.md`、`docs/adr/README.md`、`docs/adr/013-*.md` | ADR-019 新增；ADR-013 加修订状态行与三处内联注记 |
| `apps/broker_subscription/src/broker_cursor.erl` | **新增**：纯函数行运算（plain advance / shared claim / shared ack / committed / 序列化行） |
| `apps/broker_subscription/src/broker_subscription.erl` | `sub_ref()` 三形状、`join/5`、`leave/2`、`ack/4`、`claim/4`、`role/2`、`subscription()` 增字段、`dispatch/2` 归属 |
| `apps/broker_subscription/src/broker_subscription_worker.erl` | `?MEMBERS` 表、claim/ack/4、dirty 按 SubRef、boot 两张索引、`do_detach` 清 assigned |
| `apps/broker_subscription/src/broker_subscription_files.erl` | SubRef 路径与 v2 编解码、`migrate_v1/0`、`delete_subscription/1`、`delete_inflight/1` |
| `apps/broker_subscription/src/broker_subscription.app.src` | `shared_state_max` |
| `apps/broker_metrics/src/broker_metrics.erl` | 四个新计数键（闭合原子集） |
| `apps/broker_proto_mqtt/src/mqtt_conn_fsm.erl` | `subs_for/2` 带 Kind、shared 的 claim-pull、`ack/4`、teardown 用 `leave/2`、failover 的 standby 跳过 |
| 三份套件 | `broker_subscription_SUITE` 新增 ~12 例；`mqtt_conn_SUITE` 新增 ~6 例（shared 端到端）；`s1_e2e_SUITE` 的文件路径断言 |
| `tools/docker/s1_recovery.py`、`tools/docker/mq_harness.py` | T14 新增 R9（Shared 崩溃重投） |
| `docs/storage-and-recovery.md`、`docs/design/s2-single-node-kernel.md`、`docs/roadmap.md`、`progress.md` | 事实收口 |

---

## 4. 任务拆解

### T-D0 前置核对（0.25 人日）

1. 确认 ADR-019 状态「已采纳」，`docs/adr/README.md` 与 ADR-013 的修订行就位。
2. 记录基线：`rebar3 ct` 322/322、`xref` 46 条、`dialyzer` 干净、`mqtt_conn_SUITE` 50 例、`broker_subscription_SUITE` 22 例。
3. 确认无外部调用方依赖 `broker_subscription_files:{subs_path,cursors_path,delete_client,list_sessions}` 的旧签名：`s1_e2e_SUITE:384`、`broker_subscription_SUITE:147,362`、worker 的 `list_sessions`/`delete_client` 四处，仅此。

### T-D1 `sub_ref()` 三形状 + 门面 API（1.5 人日）

1. `-type sub_ref() :: {sub_kind(), binary(), binary()}`；`subscribe/3` 铸造 `{exclusive, ClientId, Filter}`。
2. 新增 `join/5`、`leave/2`、`ack/4`、`claim/4`、`role/2`；删除 `ack/3`（不留 wrapper）。
3. `subscription()` 增 `kind`/`key`；`cursors_for/1` 按 Kind 取 `low`/`offset`。
4. `group_by_client/1` → `group_by_consumer/1`：从 `?MEMBERS` 找回每个 SubRef 的 consumer 集合，Shared 展开为多个收件人。
5. 用例（`broker_subscription_SUITE`）：`subscribe_mints_a_three_shape_subref`、`join_adds_a_consumer_in_order`、`leave_promotes_the_next_member`、`leave_of_the_last_member_removes_the_subref`。

### T-D2 `broker_cursor` 内部模块（1.5 人日）

按 ADR-013 §13.1「`broker_cursor` 是 `broker_subscription` 的内部模块」落地，避免 worker 从 628 行涨到 900+：

1. `advance_plain(Row, Offset)`：现有 `advance_row/2` 原样搬入（exclusive/failover 共用）。
2. `claim(Row, Max, Consumer) -> {ok, [Offset], Row} | {error, overflow}`：从 `low` 起取前 `Max` 个既不在 `acked` 也不在 `assigned` 的 offset（升序，**允许不连续**），写入 `assigned`；`map_size(acked)+map_size(assigned) =< shared_state_max` 前置判定。高水位不在本模块（它是 IO 边界，由 worker 传给 claim 的候选区间决定）。
3. `ack_shared(Row, Offset, Consumer) -> {ok, Row} | unowned`：命中 `assigned` 且值相等 → 移入 `acked` 并 `promote_low/1`；否则 `unowned`。
4. `promote_low(Row)`：`low` 在 `acked` 中时前移到下一个缺口，同时从 `acked` 删除已吃掉的位置。
5. `committed(Row) -> offset()`：`low - 1` 或 `offset - 1`。
6. `release(Row, Consumer) -> Row`：删掉该 consumer 的全部 `assigned`（成员死亡）。
7. `grow/2`：`acked` 加一个 offset（迁就用例构造状态）。
8. 用例：`shared_claim_is_disjoint_across_members`、`claim_does_not_move_low`、`ack_out_of_order_does_not_promote_low`、`filling_the_hole_promotes_low_once`、`acked_below_low_is_idempotent`、`shared_state_max_refuses_more_claims`、`release_returns_offsets_to_the_pool`。

### T-D3 worker 重写（2 人日）

1. `?MEMBERS`（`{{ClientId, SubRef}}`，`protected` set）；`create_tables/0` 加一项。
2. `handle_call({claim, SubRef, P, Max, Consumer})`：只对 shared 行；`broker_queue:watermarks(P)` 取 `High`，claim 上限不超过 `High`；空页返回 `{ok, []}`。水印失败返回 `{error, {watermark, P, Reason}}`（pull 本轮退化 idle，不崩）。
3. `handle_call({ack, SubRef, P, O, Consumer})`：按 Kind 分派到 `broker_cursor`；`unowned` 记 `shared_ack_unowned` 并返回 `ok`。
4. dirty 账本从 `#{ClientId => Count}` 改为 `#{SubRef => Count}`；`cursors_body(SubRef)`、`flush_sub_cursors/2`。
5. `do_detach/3`：detach 前对每个 shared 行 `broker_cursor:release(Row, ClientId)`；exclusive/failover 不动。
6. boot：`list_subscriptions/0` → 按 SubRef 建实体与 cursor 行；用 body 的 `consumers` 填 `?MEMBERS`；Router 回填沿用 `backfill/1`（`already_registered` 容忍）。
7. clean-session teardown：遍历该 client 的 `?MEMBERS`，exclusive 走 `delete_subscription/1`，组订阅走 `leave/2`；最后 `delete_inflight/1`。
8. 用例：`the_worker_never_claims_past_the_watermark`、`a_dead_consumer_releases_its_whole_page`、`committed_moves_only_after_the_cursor_file_flushes`（沿用既有 K6 用例改 SubRef 键）。

### T-D4 会话文件 v2 与迁移（1.5 人日）

1. `subref_name/1`：`"<kind>.<b64url(Key)>.<b64url(Filter)>"`；`subs_path/1`、`cursors_path/1` 接受 SubRef；`inflight_path/1` 仍接受 ClientId。
2. `encode_v2/2` / `decode/3` 同时认 `{vsn,1,ClientId,Body}` 与 `{vsn,2,SubRef,Body}`。
3. `migrate_v1/0`：扫 `*.subs`，v1 者按 `{Filter,QoS,InitCursors}` 逐条写 v2；v1 `.cursors` 的 per-filter 行搬进各 SubRef 的 `rows`；**全部写成功后**才删 v1 文件；任何写失败即返回 `{error, {migration, Path, Reason}}`，boot 拒绝启动。
4. `delete_subscription/1`、`delete_inflight/1`、`list_subscriptions/0`；删 `delete_client/1`、`list_sessions/0`。
5. boot step 2 顺序：`migrate_v1/0` → `list_subscriptions/0` → 实体/`?MEMBERS` → Router 回填。
6. 用例：`migration_rewrites_v1_subs_and_cursors`、`migration_is_idempotent_after_a_partial_write`、`migration_refuses_to_boot_on_a_corrupt_v1_file`、`orphan_cursors_files_are_removed_at_boot`、`inflight_stays_client_keyed`。

### T-D5 `dispatch/2` 归属（0.5 人日）

1. 按 §2.5 分两张表：位置型按 client 合并（Shared 全成员）；非位置型按 `order` 选一个。
2. 用例：`a_qos0_publish_reaches_exactly_one_shared_member`、`a_durable_wake_reaches_every_shared_member`、`overlapping_filters_of_one_client_merge_into_one_delivery`（既有用例改形状）。

### T-D6 FSM 接线（1.5 人日）

1. `subs_for(P, Subs)` 返回 `{Ref, Kind, Filter, QoS, CursorOrLow}`；`pull_partition/3` 把行分成 `plain`（exclusive + active failover）与 `shared` 两组：
   - `plain`：沿用现有 `min(cursor)` → `read_page` → `handle_stored` 路径。
   - `shared`：对每个 shared ref 调 `claim(Ref, P, PageLimit, ClientId)`，`read_from(P, hd, last-first+1)` 后**只保留领到的 offset**，逐条投递（`QoS = min(sub, msg)`），投完即 `ack/4`（读到但不匹配的也要 ack，否则 `low` 钉死）。
2. `ack_skip/3` → `ack_skip/4`（带 `D#st.client_id`）；QoS0 与 QoS1/2 的 ack 点全部改 `ack/4`。
3. failover：`role/2` 为 `standby` 时该 ref 不进 `plain` 组；active 死亡后的下一轮 `role/2` 返回 `active`，记 `failover_promotions`。
4. `clean_session_teardown/1` 的 `#{sub_ref := {_, Filter}}` 模式改为 `#{sub_ref := Ref, kind := Kind, filter := Filter}`，exclusive 走 `unsubscribe/2`，组订阅走 `leave/2`。
5. `broker_deliver` 的 `_Refs` 保持列表语义不变（wake 不读位置）。
6. 用例：`shared_members_each_receive_distinct_messages`、`a_shared_message_is_delivered_once_across_members`、`a_standby_failover_consumer_receives_nothing`、`failover_promotes_after_the_active_dies`、`shared_ack_from_a_non_holder_does_not_move_low`。

### T-D7 测试与集成回归（1 人日）

1. `s1_e2e_SUITE:384` 的 `subs_path(ClientId)` 改为 `subs_path({exclusive, ClientId, Filter})`；`broker_subscription_SUITE:147,362` 同步。
2. 全套件跑一次，逐个修 `{ClientId, Filter}` 断言（预期集中在 `broker_subscription_SUITE` 的 6 处与 `mqtt_conn_SUITE` 的 2 处）。
3. `dialyzer`：确认 `claim/4` 与 `role/2` 的 success typing 收得住（`{error, wrong_kind}` 必须可达，不得被裁成 `ok` 单分支）。

### T-D8 T14 R9 与文档（0.75 人日）

1. `s1_recovery.py` 新增 `r9`：两成员 Shared 组、发 6 条消息、成员 A ack 其中 3 条后 `kill -9`；重启后断言已 ack 的不重投（`low` 从 `.cursors` 恢复）、未 ack 的重投给存活成员。`mq_harness.py` 的 `SCENARIOS` 加 `r9`。
2. `docs/storage-and-recovery.md` §9 改写为 SubRef 布局 + 迁移流程；`docs/design/s2-single-node-kernel.md` §4.4 从计划改为指向 ADR-019 的实现事实（实现完成前保持「计划」）。
3. `docs/roadmap.md` 文档表加本计划、M3 行状态；`progress.md` 加 S2-D 行。
4. 全量回归 + `rebar3 as t14 release` + T14 全跑。

---

## 5. 场景 / 断言清单

| # | 层 | 场景 | 断言 |
|---|---|---|---|
| D-1 | 形状 | `{Kind, Key, Filter}` 铸造与解析 | exclusive == S1 的 `{ClientId, Filter}` 语义等价 |
| D-2 | cursor | `ack` 只推连续前缀 | 乱序 ack 不动 `low`；空洞填满后一次推进 |
| D-3 | cursor | `low` 之前的 ack | 幂等忽略，不改行 |
| D-4 | claim | 两成员并发 claim | offset 集合不相交；`low` 不变 |
| D-5 | claim | 尾部 | `{ok, []}`，不是错误 |
| D-6 | claim | 上限 | `{error, overflow}` + `shared_window_full` +1 |
| D-6b | claim | 高水位 | 返回的 offset 全部 `=< High`；跨越时不产生幻影 `assigned` |
| D-7 | 归属 | 非持有者 ack | `ok` + `shared_ack_unowned` +1，行不变 |
| D-8 | 死亡 | 成员领 3 条后退出 | 3 条回未分配；另一成员下一轮全领到 |
| D-9 | 投递 | N 成员并发 pull | 每条消息**恰好一次**（按 offset 集合断言） |
| D-10 | QoS0 | Shared 组发布一条 QoS0 | 只有一个成员收到 |
| D-11 | failover | standby | 收不到任何投递；ack no-op |
| D-12 | failover | active 死亡 | 下一成员顶上，从 committed 继续，`failover_promotions` +1 |
| D-13 | 文件 | SubRef 布局 | 文件名可逆、内容即真相；`<subref>.subs` 与 `?MEMBERS` 一致 |
| D-14 | 文件 | `.inflight` | 仍按 client、仍 vsn 1 信封、内容 vsn 2 blob |
| D-15 | 迁移 | v1 → v2 | 内容等价、v1 删除、再启动一次不重复迁移 |
| D-16 | 迁移 | 中途失败 | 重启后从两份文件之一恢复（幂等），不丢订阅 |
| D-17 | 崩溃 | T14 R9 | 已 ack 的不重投；未 ack 的重投；cursor 最终推进 |
| D-18 | 回归 | Exclusive | 322 例基线不降；T13 双组 12/12；T15 10/10 |

---

## 6. 验收命令

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"

rm -rf _build/test/extras          # cp -RpL 覆盖不了 0400 的 .erlang.cookie
rebar3 ct                          # 期望：322 + 新增用例，零 FAILED / 零 Skipped
rebar3 dialyzer
rebar3 xref                        # 预期 46 条基线 + broker_cursor 的新增（同步核对）
rebar3 as t14 release

# 崩溃恢复（含新增 R9）
/usr/bin/python3 tools/docker/s1_recovery.py --scenario r9
/usr/bin/python3 tools/docker/s1_recovery.py --keep-going

# 真实客户端回归（Shared 不涉及线上语法，T15 只为确认 Exclusive 未回归）
/usr/bin/python3 test/protocol-compatibility/mqtt/t15_compat.py
```

**判定口径**：`rebar3 ct` 的用例数必须等于 `all()` 之和；`dialyzer` 不得新增 warning；`xref` 的 unused-export 增量必须是「仅由套件调用」的公开 API。

---

## 7. 风险

| 风险 | 缓解 |
|---|---|
| Shared 的 claim 把 pull 路径改复杂，拖坏 Exclusive（S1 的 50 个 conn 用例依赖它） | T-D6 先按行分组（`plain` / `shared`），`plain` 分支**零逻辑改动**；Exclusive 用例作为回归闸 |
| `assigned` 是内存态，worker 崩溃后被 ack 丢失 ⇒ 消息重投 | 明确接受（至少一次）：重投给存活成员后由它 ack；T-D8 的 R9 断言「不丢、可重复」而不是「不重复」 |
| `low` 被一条投毒消息钉死，`shared_state_max` 耗尽 | 有界拒绝 + `shared_window_full` 可观测；隔离能力归 S2-E 的 DLQ，本计划只保证有界 |
| 迁移先写后删的中间态被误读 | 迁移幂等（重写相同内容）；boot 每次先跑 `migrate_v1/0`；用例 D-15/D-16 钉住 |
| `dispatch/2` 的 Shared 全成员唤醒造成 wake 放大 | 只唤醒 attached 成员；空 claim 的成本是一条 `{ok, []}` 返回；S2-I 量化 |
| `?MEMBERS` 与 `.subs` 内容不一致 | `?MEMBERS` 只在 boot 从 body 重建、只在 join/leave 变更，与 body 同一次调用内写；用例 D-13 钉住 |
| QoS0 与 QoS1 在 Shared 上语义不同被实现者混淆 | §2.5 的两列表格进模块文档注释；用例 D-10 与 D-9 分别钉住 |
| failover 的 standby 也 pull 导致重复投递 | `role/2` 在 `subs_for/2` 之前过滤；用例 D-11 |
| `ack/3` 删除后遗漏调用点 | `rebar3 compile` 会全部报出来（`warnings_as_errors`）；T-D1 一次改完 |

---

## 8. 交付记录

（实现完成后填写：验收结果表、场景 → 真实用例名对照、实现期偏差。）
