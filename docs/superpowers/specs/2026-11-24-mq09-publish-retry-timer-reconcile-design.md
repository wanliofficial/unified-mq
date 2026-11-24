# MQ-09 发布侧重试定时器 —— 设计（现状核实 + 文档对齐）

> 状态：核实稿（brainstorm 产出）—— 结论：**MQ-09 描述的能力已完整实现且默认启用**，
> 本稿是 doc 对齐 + 子场景核实，不是新功能实现。
> 缺口出处：`docs/compatibility/unifiedmq-mqtt-family-gap.md` 第 60 行（MQ-09）
> 日期：2026-11-24

## 1. 缺口与现状的出入

### 1.1 缺口表 / doc §4.6 的声称

缺口表（`unifiedmq-mqtt-family-gap.md:60`）：
> MQ-09 语义缺口：**没有发布侧重试定时器**；同一条连接对未确认消息只投一次，
> 重发只发生在重连。规范允许发送方重试。慢客户端或丢 PUBACK 时，要断线才补。

doc §4.6（`mqtt-feature-comparison.md:164`，标 `[x]` 已选）：
> **发布侧重试语义（MQ-09）**：同一条连接对未确认消息只投一次，**不挂重传定时器**；
> 重发只在重连时发生。这是刻意取舍——慢客户端或丢 PUBACK 时由断线补投而非连接内
> 定时器，语义已选定并有测试覆盖，非缺陷。

### 1.2 核实结论：连接内重试已完整实现

两处描述都已**过时**。连接内 ack 超时重试不仅存在，而且默认启用、有集成测试。

**完整链路（已逐环核实，全部带 file:line）：**

1. **检测**：`mqtt_conn_fsm:pull_round/1`（每个 `pull_poll_interval_ms`，默认 1000ms）
   调 `report_ack_timeouts/1`（`mqtt_conn_fsm.erl:2199`）。它扫描 `sent_at`（每次
   wire 尝试经 `mark_sent_at/2` 写入，`mqtt_conn_fsm.erl:1879`），凡 `sent_at` 早于
   `ack_timeout_ms`（默认 30000ms，`broker_proto_mqtt.app.src:41` 默认开启）的 Pid，
   且尚未上报过（`retry_reported` 去重），调 `report_timeout/2`。

2. **调度**：`report_timeout/2`（`mqtt_conn_fsm.er:1585`）对非 shared 成员调
   `broker_subscription:retry(Ref, P, O, ClientId)`，把 offset 写进 cursor 的
   `retries` 表，带指数退避 `min(retry_base_ms·2^(n-1), retry_max_delay_ms)`
   （`broker_cursor:backoff/3`，ADR-015 §15.2），attempt 预算 `max_attempts`（默认 10）。

3. **重发**：worker 的 `retry_tick`（默认 1000ms）→ `sweep_retries/1`
   （`broker_subscription_worker.erl:1231`）→ `broker_cursor:due/3` 找退避到点的 offset
   → `wake_executor/4` 发 `{broker_redeliver, ...}` → conn_fsm `redeliver_pid/4`
   （`mqtt_conn_fsm.er:1638`）→ `overdue/2` 复核仍超时 → `resend/4` 用**同一 packet id +
   DUP=1** 重发。**全程在连接内，不经重连。**

4. **终态**：`max_attempts` 用尽 → `dead_letter/6`（`broker_subscription_worker.erl:1274`）
   → 写 DLQ + cursor 前进（消息不无限重试）。另有独立的消息 TTL 过期路径
   （`expires_at` → `expire_local/4`，`mqtt_conn_fsm.er`）。

**已有的集成测试（`mqtt_conn_SUITE.erl`）：**
- `a_timed_out_delivery_is_redelivered_with_dup_and_the_same_packet_id`（:171，E-10）——
  **连接内** ack 超时重发：测试注释明写 "Nothing here is a reconnect -- that path ... is
  a different case"，断言 `redelivery_total >= 1`、同 packet id、DUP=1。
- `an_unacked_message_is_resent_with_dup_and_the_same_packet_id`（:169）—— **重连**路径：
  session_present=1、inflight blob 恢复、DUP=1、同 packet id。
- `qos2_inbound_dup_publish_is_not_redelivered`、`reconnect_before_pubrec_resends_a_dup_publish`——
  QoS2 inbound DUP 与重连 resend。

**已有的纯函数/退避/DLQ 测试（`broker_subscription_SUITE.erl`）：**
- `retry_algebra_schedules_and_ack_clears`、`backoff_sequence_is_exponential_and_capped`、
  `shared_retry_waits_for_the_backoff_before_reclaim`、`max_attempts_dead_letters_and_advances`、
  `a_dead_letter_is_an_ordinary_subscribable_destination`、`a_failed_dead_letter_keeps_the_cursor`、
  `retry_progress_survives_a_restart`。

### 1.3 子场景核实（MQ-09 两种解读都已覆盖）

| 解读 | 现状 | 覆盖测试 |
|---|---|---|
| **下游连接内重试**（broker 发给 sub 的 PUBLISH 丢了 sub 的 PUBACK → 连接内按退避重发 DUP=1） | **已实现，默认启用** | `a_timed_out_delivery_is_redelivered_*` |
| **重连重发**（inflight blob → catch-up round → DUP=1 同 packet id） | **已实现** | `an_unacked_message_is_resent_*` |
| 发布者侧 inbound QoS1 DUP 去重（broker 作为接收方，同一 clientid 重发 PUBLISH） | **已实现**（`received` map 去重） | `qos2_inbound_dup_publish_is_not_redelivered` 等 |

## 2. 一个值得记录的取舍：shared 分支 `drop_local`

`report_timeout/2`（`mqtt_conn_fsm.er:1595`）对 **shared** 成员走 `drop_local/2` 而非重发：
> "A shared offset is re-claimed, never resent: the worker released it back to the pool,
> so this in-flight entry is stale. Dropping it makes the next claim a fresh delivery
> (new packet id) instead of a silent duplicate."

这是**有意为之**，不是缺陷：shared offset 被超时后由 worker release 回池，任何成员可重新
claim（新的 wire 尝试、新 packet id、DUP=0），而非在原地用旧 packet id 重发（会与重新
claim 的成员产生重复）。exclusive/failover 是单读者，原地重发安全，故走 `resend`。

**结论**：shared 的「重claim」与 exclusive/failover 的「原地 resend」是两种不同的、各自
正确的重试形态，符合各自语义。无缺口。

## 3. 本稿要做的改动（仅文档 + 缺口表）

实现侧**零改动**。改动限于把过时描述改为准确机制：

1. **doc §4.6（`mqtt-feature-comparison.md:164`）**：把「不挂重传定时器；重发只在重连时
   发生」改为准确描述——连接内 ack 超时重试已存在（默认启用，`ack_timeout_ms` 默认 30s，
   worker 退避 + retry_tick，同 packet id + DUP=1 重发，`max_attempts` 后 dead-letter），
   重连路径是另一条（inflight blob 恢复）。保留「已选语义 + 测试覆盖」的定调。

2. **缺口表（`unifiedmq-mqtt-family-gap.md:60`）**：MQ-09 状态从「语义缺口 / 没有发布侧重试
   定时器」改为「已交付（连接内 ack 超时重试 + 重连重发，均有测试）」；「后续」列指向
   本核实稿与相关测试。

3. **可选（doc 内）**：在 §4.6 注明两种重试形态（exclusive/failover 原地 resend vs shared
   重claim）的取舍，便于对读 fss-mqtt-broker / robustmq 时说明。

## 4. 不变量（重试机制契约，已成立，本稿只 record）

1. **同 packet id + DUP=1**：重发复用原 packet id 并置 DUP=1，使客户端去重仍能识别
   （R7/§4.1）。
2. **一次 cursor 前进对应一条消息**：无论重试多少次，最终 ack 只推进一次 cursor；
   dead-letter 也只前进一次。
3. **bounded retry**：`max_attempts`（默认 10）用尽即 dead-letter，不无限重试。
4. **连接内优先、重连兜底**：ack 超时在连接内由 worker 退避调度重发；连接断开才走
   inflight blob 的重连 catch-up。两条路径不重复投递（cursor 仍指向未 ack offset，
   重连 round 重读同一 offset）。
5. **shared 不原地重发**：shared offset 超时 release 回池、重claim（新 packet id），
   避免与重新 claim 的成员重复。

## 5. 测试计划

**无需新增测试**（已实现且已覆盖）。本稿的验证是 doc 与代码一致性的核查：

- 确认上述测试的 `=result ok` / `=failed 0`（回归，证明 doc 改动不碰运行逻辑）。
- doc 改动后通读 §4.6 与缺口表，确认措辞与 `mqtt_conn_fsm.erl` / `broker_subscription_worker.erl`
  的实际机制逐条对应（本报告 §1.2 的 file:line 索引即核对基线）。

## 6. 边界与注意

- 本稿不改任何运行逻辑，故无回归风险；只改两处 doc。
- `ack_timeout_ms` 默认 30000ms 较大，生产 ack 超时重试触发较慢；这是可配置的运维参数，
  不是缺陷。若将来要更快重试，调小 `ack_timeout_ms` 即可（无需改设计）。
- 与 MQ-15 的关系：MQ-15 的 shared 再均衡会 release offset 回池，与 MQ-09 的 shared
  `drop_local` 是同一「重claim 不原地重发」哲学的延伸，两者语义一致、互不冲突。
- 与 MQ-11 的关系：MQ-11 的「停投」in-flight 满语义与 MQ-09 的重试正交——停投影响的是
  claim 速率，重试影响的是已发未 ack 的消息，互不影响。
