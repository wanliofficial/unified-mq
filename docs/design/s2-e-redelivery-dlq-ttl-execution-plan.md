# S2-E 执行计划：重投递 / DLQ / TTL

- **状态**：**已实现（2026-11-08）**。决策见 [ADR-015](../adr/015-redelivery-dlq-and-ttl.md)（S2 M0 冻结）与本计划 §2 的实现期解释（写入 ADR-015 §15.6）。
- **对应**：`docs/design/s2-single-node-kernel.md` §4.5 / §5（M3）；roadmap §4.1（消息生命周期）
- **估算**：6 人日（T-E0 ~ T-E7）
- **退出条件**：退避序列可机械断言（假时钟）；`max_attempts` 后进 DLQ 且不阻塞分区；DLQ 可被普通订阅消费、命名按租户可配；TTL 到期不投递且 cursor 前进；重启后重试进度不丢；全量回归零 FAILED。

---

## 1. 目标与非目标

**目标**：把 S1/S2-D 的「只靠重连重投」补成 Section 15 的完整生命周期：失败一次要退避、退避到上限要进死信、死信要是普通 destination、过期消息不投递但不能钉住水位。

1. **重试状态归 worker**（ADR-015 §15.1）。`mqtt_conn_fsm` 只是执行者；连接死亡不丢重试计划。
2. **退避 `base * 2^(n-1)`、上限可配、`max_attempts` 后转 DLQ**（§15.2）。
3. **DLQ 是普通 destination**：按租户可配命名、复用 `broker_queue:append/3`、可被普通订阅消费（§15.3）。**不引入独立 app**。
4. **TTL**：`expires_at` 生效，过期消息跳过并推进 cursor，不影响 committed（§15.4）。
5. **DLQ 自身失败不丢数据**（§15.5）。

**非目标**

- `$share` / MQTT 5 的 `message expiry` 属性（S2.5）：TTL 来源是 broker 配置与 `expires_at` 槽位，不是线上属性。
- DLQ 的**段级**保留期执行（S2-F / ADR-014）：S2-E 只落配置与计数，GC 在 S2-F 生效。
- 运维重放工具（S6）。

---

## 2. 实现决策（ADR-015 冻结范围内的解释）

> ADR-015 已由用户签认；下面四项是它对实现的留白（「由 S2-E 实现决定」或未言明），在此冻结并写入 ADR-015 §15.6。**均不改变 §15.1~§15.5 的对外契约。**

| # | 决策 | 依据 | 代价 |
|---|---|---|---|
| **E-1** | **失败信号 = ack 超时**（`ack_timeout_ms`，默认 30 s）。检测在连接进程（它已有 `sent_at` 与 poll tick），**决策与计时在 worker**：conn 只在超时时发一次 `retry/5`，worker 记 `attempts`/`next_at` 并负责到点唤醒。 | MQTT 3.1.1 没有 nack，能观测到的失败只有「连接死」与「ack 不来」；前者已由 S2-D 覆盖，后者必须由超时定义，否则 §15.2 的退避没有起点。 | 慢客户端会看到重复 PUBLISH（`DUP=1`，§4.1 允许）；默认 30 s 必须大于正常 ack 延迟。 |
| **E-2** | **重试状态存在 cursor 行的 `retries` 字段**（随 `.cursors` 的窗口落盘），不新建文件。plain 行与 shared 行同形：`#{Offset => #{attempts, first_at, next_at, reason}}`。 | 一份 `{SubRef, Partition}` 的真相已经在 cursor 行里（ADR-013 §13.1）；另起 `{SubRef}.retries` 会产生两份可以互相矛盾的「这条消息怎么样了」。 | 崩溃后 `attempts` 可能回退（窗口内未刷盘）⇒ **多重试几次**，方向安全；文档必须写明。 |
| **E-3** | **TTL 过滤在读者侧执行**（`mqtt_conn_fsm` 的存转投路径），不改 `broker_queue:read_from/3`。 | ADR-011 冻结 `read_from` 的契约（返回日志里存在的连续记录）；在 storage 里过滤会把「跳过」变成一个没有 cursor 语义的静默截断。ADR-015 §15.4 的两个过滤点的**可观测契约**（不投递 + cursor 前进 + 不钉住 committed）由读者侧一次检查同时满足。 | 过期消息仍会被读出一次（多一次 `read_from` 的字节拷贝），换来契约不破。 |
| **E-4** | **DLQ 副本固定 `fsync` 落盘**；副本 `expires_at = undefined`、`created_at = 入队时间`、`message_id` 重新生成，原值进 `headers`（`x-original-message-id`）。 | §15.3/§15.5 把 DLQ 定义为「不丢数据」的兜底：原消息可能只是 `os_buffer` 等级，死信比原件更弱就失去意义。副本若继承 `expires_at`，过期消息进 DLQ 后立刻不可消费，等于丢掉诊断证据。 | 死信路径的延迟 = 一次 fsync（冷路径，可接受）。 |
| **E-5** | **shared 行的 `retries` 计入 `shared_state_max`**（`acked ∪ assigned ∪ retries`）；claim 会跳过 `next_at` 未到的 offset。 | 退避中的 offset 已被释放回池，若不计入窗口，投毒消息可以无限占用新 offset，把内存变成无界（Gate 2 的反例）。 | 一页投毒消息最多把窗口填满一次，`max_attempts` 后转 DLQ，窗口自动回收。 |
| **E-6** | **`dlq_size` 的语义 = 当前保留在 DLQ 里的条数**（append 成功 +1；S2-F 的段删除 -1）。S2-E 只增不减，因此它是「累计死信量」的诚实上界。 | ADR-016 冻结了这个键，但没有冻结它的更新点；「未被消费条数」需要跨 destination 的 ack 聚合，S2-E 没有这个数据流。 | 在 S2-F 之前它单调增长；文档写明。 |

---

## 3. 数据形状

### 3.1 cursor 行（`?CURSORS`）

```erlang
%% plain（exclusive / failover）
#{kind := exclusive | failover,
  offset := offset(),                 % 下一个要读的位置
  pending := [offset()],              % 已投递未 ack（乱序 ack 的缓冲）
  retries := #{offset() => retry()}}  % S2-E

%% shared
#{kind := shared,
  low := offset(),  acked := #{offset() => true},
  assigned := #{offset() => consumer()},
  retries := #{offset() => retry()}}  % S2-E

retry() :: #{attempts := pos_integer(), first_at := integer(),
             next_at := integer(), reason := ack_timeout | dlq_append_failed}
```

- `retries` **落盘**（与 `low`/`offset` 同窗口）；`assigned` 仍不落盘。
- 任何 ack（plain 的 `advance_plain`、shared 的 `ack_shared`）都清掉该 offset 的 retry。
- `broker_cursor:claim/6` 跳过 `retries` 里 `next_at > Now` 的 offset；`persist/1` 保留 `retries`，`restore/1` 保留 `retries`、清 `assigned`。

### 3.2 DLQ 消息

```erlang
#broker_message{
  message_id = crypto:strong_rand_bytes(16),   % 新 ID
  tenant_id = 原消息的 tenant,
  destination = 渲染后的 name_pattern,
  payload = 原 payload（字节不变）,
  headers = 原 headers ∪ #{<<"x-original-destination">> => Dest,
                           <<"x-original-message-id">> => Id,
                           <<"x-failure-reason">> => <<"max_attempts">> | ...,
                           <<"x-attempts">> => N,
                           <<"x-tenant">> => Tenant},
  created_at = now_ms(), expires_at = undefined,
  reliability = fsync}
```

- 命名模板：`dlq_name_pattern`（默认 `"$dlq/{tenant}/{original}"`），变量 `{tenant}` `{original}` `{qos}` `{kind}`；渲染结果含 `+`/`#` 或为空 ⇒ **启动失败**（配置错误不该等到第一条死信才暴露）。

---

## 4. 任务

| # | 任务 | 交付 |
|---|---|---|
| T-E0 | 本计划 + ADR-015 §15.6 实现注记 | 决策冻结 |
| T-E1 | `broker_cursor` 的重试代数 | `retry/5`、`attempts/2`、`due/2`、`blocked/2`、`clear_retry/2`、`claim/6`、`release_offset/2` |
| T-E2 | `broker_dlq` 模块 | 模板渲染与校验、`dead_letter/4`（读原记录 → 包装 → `append(fsync)`）、`is_dlq/1` |
| T-E3 | worker：`retry/5` + 退避 + `retry_tick` + 到点唤醒/DLQ | 状态与调度 |
| T-E4 | conn：ack 超时检测、`{broker_redeliver, Kind, SubRef, P, O}`、shared 回收本地 pending | `mqtt_conn_fsm` |
| T-E5 | TTL：`build_message/7` 填 `expires_at`；投递前跳过并推进 cursor | `mqtt_conn_fsm` + `mqtt_will` |
| T-E6 | 指标与配置 | `redelivery_total`/`dlq_appends`/`dlq_append_failed`/`ttl_expired_total`/`dlq_size`；5 个配置键 |
| T-E7 | 回归 | 全量 CT、dialyzer、xref、T14 主矩阵 |

---

## 5. 测试清单

| # | 场景 | 断言 |
|---|---|---|
| E-1 | 纯行代数 | `retry/5` 累加 attempts、`due/2` 按 `next_at`、ack 清 retry |
| E-2 | claim 屏蔽 | 退避中的 shared offset 不被 claim；到期后重新领到 |
| E-3 | 退避序列 | 假时钟：1s/2s/4s… 到上限后恒定 `retry_max_delay_ms` |
| E-4 | 超限转 DLQ | `max_attempts` 次超时后：DLQ destination 有一条副本、cursor 前进、分区不再被钉住 |
| E-5 | DLQ 内容 | payload 不变、四个 `x-*` 头、新 `message_id`、`expires_at=undefined` |
| E-6 | 命名 | 默认与自定义模板；含 `+`/`#` 的模板启动失败 |
| E-7 | DLQ 可订 | 普通 exclusive 订阅 `$dlq/#` 收到副本 |
| E-8 | DLQ 写失败 | append 失败 ⇒ cursor 不前进、`dlq_append_failed` +1、条目不丢 |
| E-9 | 重启 | `retries` 恢复、`assigned` 清空；退避进度不丢 |
| E-10 | 端到端重投 | 客户端不 ack ⇒ `ack_timeout_ms` 后收到 `DUP=1` 的重发，最终 ack 后 cursor 前进 |
| E-11 | TTL | `expires_at` 已过的记录：不投递、cursor 前进、committed 前进 |
| E-12 | TTL 不设 | `expires_at=undefined` 永不按时间丢弃 |
| E-13 | 回归 | 全量 CT 零 FAILED；T13/T15 不变 |

---

## 6. 验收命令

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
rm -rf _build/test/extras
rebar3 ct                       # 期望：349 + 新增用例，0 FAILED
rebar3 dialyzer
rebar3 xref                     # 与 51 条基线比对
rebar3 as t14 release
/usr/bin/python3 tools/docker/s1_recovery.py --scenario r1a --tier zero   # 冒烟：主路径未回归
```

---

## 7. 风险

1. **重复放大**：`ack_timeout_ms` 小于真实 ack 延迟会把正常流量变成重发风暴。默认 30 s；`redelivery_total` 与 `deliver_total` 的比值是可观测的守门指标（S2-I 写进基准报告）。
2. **重试状态与窗口**：`retries` 落盘在 `.cursors` 里，崩溃后 attempts 可能回退（多重试几次）。方向安全，但文档必须写明。
3. **DLQ 自身成为热点**：死信集中到 `$dlq/{tenant}/...` 会命中单一分区；S2-G 的背压与 S2-F 的独立保留期分别兜住写入速率与磁盘占用。

---

## 8. 交付记录（2026-11-08）

**状态：已实现并验收。** 决策 [ADR-015](../adr/015-redelivery-dlq-and-ttl.md) §15.6（实现注记）；本文 T-E0~T-E7 全部完成。

### 8.1 验收结果

| 项 | 结果 |
|---|---|
| `rebar3 ct` | **360/360，0 FAILED / 0 Skipped**（基线 349 + 11 例） |
| `broker_subscription_SUITE` | **53/53**（新增 9 例：重试代数、退避序列、shared 退避 claim、超限转 DLQ、DLQ 可订阅、模板校验与渲染、坏模板拒绝启动、DLQ 写失败保 cursor、重启保重试进度） |
| `mqtt_conn_SUITE` | **57/57**（新增 2 例：E-10 ack 超时同 packet_id `DUP=1` 重投、E-11 过期消息跳过且 cursor/committed 前进） |
| `broker_metrics_SUITE` | **12/12**（`keys_are_stable` 纳入 `dlq_appends`/`dlq_append_failed`/`ttl_expired_total`） |
| `rebar3 dialyzer` | **干净**（无 warning） |
| `rebar3 xref` | **53 条**（基线 51，净增 2：`broker_cursor:claim/5` 与 `broker_dlq:name/2`，均为**仅套件调用**的公开 API；实现期改由 `claim/6` 承载带 Blocked 集的 claim） |

### 8.2 场景 → 真实用例

| # | 场景 | 用例 |
|---|---|---|
| E-1 | 纯行代数（attempts / due / blocked / ack 清理 / pause） | `broker_subscription_SUITE:retry_algebra_schedules_and_ack_clears` |
| E-2 | claim 屏蔽退避中的 shared offset | `broker_subscription_SUITE:retry_algebra_schedules_and_ack_clears`、`shared_retry_waits_for_the_backoff_before_reclaim` |
| E-3 | 退避序列与上限（假时钟） | `backoff_sequence_is_exponential_and_capped` |
| E-4 | 超限转 DLQ 且 cursor 前进 | `max_attempts_dead_letters_and_advances` |
| E-5 | DLQ 内容（新 ID、原值进 `x-*`、`expires_at=undefined`） | `max_attempts_dead_letters_and_advances`（读回 DLQ 副本断言 headers/payload） |
| E-6 | 命名模板（默认/自定义/坏模板拒绝启动） | `dlq_template_validation_and_rendering`、`a_bad_dlq_template_refuses_boot` |
| E-7 | DLQ 可被普通订阅消费 | `a_dead_letter_is_an_ordinary_subscribable_destination` |
| E-8 | DLQ 写失败不丢数据 | `a_failed_dead_letter_keeps_the_cursor`（`dlq_append_failed` 增长、cursor 不越过原件） |
| E-9 | 重启保重试进度 | `retry_progress_survives_a_restart` |
| E-10 | 端到端 ack 超时重投（`DUP=1`、同 packet_id、最终 ack 前进） | `mqtt_conn_SUITE:a_timed_out_delivery_is_redelivered_with_dup_and_the_same_packet_id` |
| E-11 | TTL 到期不投递、cursor 与 committed 前进 | `mqtt_conn_SUITE:an_expired_message_is_skipped_and_the_cursor_advances` |
| E-12 | `expires_at=undefined` 不按时间丢弃 | S2-C 既有投递用例 + `broker_cursor` 行代数（无 TTL 分支即不跳过） |
| E-13 | 回归 | 全量 360/360；T13/T14/T15 主矩阵未改（本子项目不改存储/会话格式） |

### 8.3 实现期偏差（已写入 ADR-015 §15.6，非计划遗留）

1. **`claim/5` → `claim/6`**：新增 `Blocked :: #{offset() => true}` 参数（由 `broker_cursor:blocked/3` 生成）。退避中的 offset 若不被 claim 跳过，重试时间表就只是纸面承诺（E-5）。`claim/5` 保留为 `Blocked = #{}` 的便捷入口，仅套件使用（见 §8.1 的 xref 说明）。
2. **`retry/4` 是唯一 facade/worker 签名**（`SubRef, PartitionId, Offset, Consumer`）：原计划写作 `/5` 是把内部 `broker_cursor:retry/5` 的参数个数误带到了门面；门面不需要 `Reason` 的第二个业务参数，原因由 worker 统一记为 `ack_timeout`，DLQ 失败路径记 `dlq_append_failed`。
3. **DLQ 配置键落在 `broker_subscription`**（`dlq_name_pattern`），不新建 `broker_dlq` app：DLQ 是普通 destination（§15.3），命名属于「谁决定死信投递」，即订阅 worker。
4. **已在 DLQ 的消息再次超时**返回 `already_dead_letter`，不再嵌套死信，直接推进 cursor：否则 `$dlq/t/$dlq/t/...` 会无限增长，而诊断证据已经在 DLQ 里。
5. **`dlq_size` 是 gauge，S2-E 只增不减**（ADR-015 §15.6 第 4 条；段删除在 S2-F 才发生），语义是「当前保留在 DLQ 的条数」的诚实上界。
