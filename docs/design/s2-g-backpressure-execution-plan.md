# S2-G 执行计划 — 背压与限流（可观测 + 有界）

- **状态**：执行中（2026-11-08）
- **对应总纲**：[`s2-single-node-kernel.md`](s2-single-node-kernel.md) D5 / §4.7 / §5 M4
- **对应 Gate**：roadmap §4.3 第 2 项（慢消费者 / 热点分区不导致 OOM）的结构前提；数字证据由 S2-I 出
- **决策依据**：M0 D5 已冻结（“先只加可观测 + 有界”）。**不新增协议帧、不新增 ADR、不改 8 个 storage 回调。**

---

## 1. 目标与非目标

### 1.1 目标（本子项目要给出的可机械核对断言）

| 断言 | 形式 |
|---|---|
| 慢消费者不会让连接进程无界增长 | pull round 在 in-flight 窗口满 / 自身 mailbox 超阈值时**整轮跳过**，并计数 |
| 生产者过载不会把写队列变成无界内存 | `wal_writer` 在 `pending` 达到 `max_pending_appends` 时**拒绝新 append**（不消费 offset），连接收到错误后不 PUBACK、直接断开（MQTT 3.1.1 没有背压帧） |
| 过载可观测 | 新增 5 个指标键：`conn_mailbox_len` / `conn_inflight` / `wal_pending_append`（gauge），`backpressure_pauses` / `backpressure_disconnects`（counter） |
| 热点分区不拖住其他分区 | 既有“每轮只访问一个分区 + 按 `round_no` 轮转”保持；单轮读取上限仍是 `pull_page_limit` |
| 连接风暴不击穿 accept 池 | 仍由 ranch 的 `max_connections` / `num_acceptors` 兜底（listener map 已下发），S2-I 压测给数字 |
| 阈值全部可配 | `pull_mailbox_limit` / `publish_mailbox_limit`（`broker_proto_mqtt`）、`wal.max_pending_appends`（`broker_storage`）；`0` = 关闭该阈值 |

### 1.2 非目标

- **不做**精确的公平调度、优先级队列、按租户配额。S2-G 只保证“有界”，不保证“公平”。
- **不做** MQTT 5 的 `0x97 quota exceeded`：S2 只支持 3.1.1（O3 把 MQTT 5 移到 S2.5）。
- **不做** 主动采样上报。指标写入仍是热路径上的 `atomics` 操作，聚合读数由 `snapshot/0` 完成。
- **不根据** 全局 `subscription_backlog` 反压单个连接：那是聚合值，拿它做单连接决策会让一个慢消费者影响其他消费者。单连接只看自己的 mailbox / in-flight。

---

## 2. 冻结的信号、阈值与动作

| 信号 | 采集点 | 指标键 | 默认阈值 | 超阈值动作 |
|---|---|---|---|---|
| `mailbox_len`（消费者） | `pull_round/1` 开头 `process_info(self(), message_queue_len)` | `conn_mailbox_len`（gauge，跨连接求和） | `pull_mailbox_limit = 1000` | 整轮跳过，`backpressure_pauses + 1`；下一轮由 poll tick / 唤醒驱动 |
| `inflight` | `maps:size(#st.pending)` | `conn_inflight`（gauge，跨连接求和） | `max_inflight = 64`（既有） | 整轮跳过，`backpressure_pauses + 1`（此前是“读了页再逐条 stall”，白读盘） |
| `pending_append` | `wal_writer` 的 `#st.pending` 长度 | `wal_pending_append`（gauge，跨分区求和） | `wal.max_pending_appends = 4096` | `append/3` 回 `{error, {overloaded, PartitionId}}`，**不分配 offset** |
| `mailbox_len`（生产者） | `handle_publish/2` 开头 `message_queue_len` | 同上 | `publish_mailbox_limit = 1000` | 不 PUBACK，`{stop, {backpressure, mailbox}}`，`backpressure_disconnects + 1` |
| append 过载 | `store_and_ack/7` 的错误分支 | — | — | 不 PUBACK、断开，`backpressure_disconnects + 1` |

**量纲说明**

- `conn_mailbox_len` / `conn_inflight` / `wal_pending_append` 用 `gauge_add/2` 维护**跨进程求和**：每个进程记住自己上一次上报的值，变化时只加差值，`terminate/2` 时减回。单个 `gauge/2` 会被多个进程互相覆盖，读数没有意义，所以不用它。
- 进程被 `exit(Pid, kill)` 时 `terminate/2` 不跑，和值会漂移；这三个 gauge 的定位是**诊断水位**，不是计费依据。S2-I 的数字用 `erlang:process_info/2` 直接读进程，不依赖这三个 gauge。
- `backpressure_pauses` 只统计“整轮被跳过”，不统计逐条 stall（后者是正常窗口语义）。

---

## 3. 任务

| ID | 任务 | 产物 |
|---|---|---|
| T-G0 | 冻结信号/阈值/动作（本文） | 本文 §2 |
| T-G1 | 纯决策模块 `mqtt_backpressure`（`consumer/3`、`producer/2`） | `apps/broker_proto_mqtt/src/mqtt_backpressure.erl` |
| T-G2 | `broker_metrics` 新增 5 键；`keys_are_stable` 同步 | `broker_metrics.erl`、`broker_metrics_SUITE` |
| T-G3 | `wal_writer`：`max_pending_appends` 拒绝 + `wal_pending_append` gauge + `pending` 进 `stats/1` | `wal_writer.erl` |
| T-G4 | `mqtt_conn_fsm`：pull round 整轮 permit、PUBLISH 前生产者门、过载断开计数、两个 gauge 的差分上报 + terminate 回收 | `mqtt_conn_fsm.erl` |
| T-G5 | 配置：`sys.config.src` / `.app.src` 默认值；`?CONN_KEYS` 让 listener map 可覆盖 | `config/sys.config.src`、`broker_proto_mqtt.app.src`、`broker_proto_mqtt_sup.erl` |
| T-G6 | 测试：纯决策、writer 拒绝、窗口满暂停、慢消费者不增长、多分区隔离 | `mqtt_backpressure_SUITE`、`wal_writer_SUITE`、`mqtt_conn_SUITE` |
| T-G7 | 文档收口：S2 设计 §4.7、ADR-016 §16.2 实现注记、`progress.md`、`roadmap.md`、本文 §8 | 同左 |

---

## 4. 关键实现约束

1. **不改协议**：3.1.1 没有背压帧，过载只能“断开、不 ack”。这是 D5 拍板的形状。断开必须发生在 **PUBACK 之前**，否则客户端会认为消息已持久化。
2. **不消费 offset**：`wal_writer` 的拒绝发生在 `high` 递增之前。被拒的 append 不得在日志里留下空洞，否则 `read_from` 会永远等一个不存在的 offset。
3. **整轮 permit 与逐条 stall 不冲突**：`max_inflight` 仍是逐条交付窗口；新增的“整轮跳过”只是避免在窗口已满时还去读盘。两者的判定条件相同（`maps:size(pending) >= max_inflight`），因此不会出现“整轮放行但逐条全 stall”的新状态。
4. **跳过轮次不能停掉连接**：暂停的轮次不 `schedule_round`，靠 poll tick 和消息唤醒恢复；否则一个暂时超阈值的连接会永久休眠。
5. **指标关闭时行为一致**：`broker_metrics:gauge_add/2` 在 `enabled=false` / app 未启动时返回 `0`，门限判定必须读本地值，不读指标。

---

## 5. 测试

| 断言 | 用例 |
|---|---|
| 消费者门限纯函数：mailbox 优先、窗口满、0=关闭 | `mqtt_backpressure_SUITE:consumer_*` |
| 生产者门限纯函数 | `mqtt_backpressure_SUITE:producer_*` |
| 写队列满时 append 被拒且不消费 offset | `wal_writer_SUITE:appending_above_the_pending_limit_is_refused` |
| 窗口满时整轮跳过并计数；ack 后恢复 | `mqtt_conn_SUITE:a_full_inflight_window_pauses_the_pull_round` |
| 慢消费者（手动逐条 ack）连接 mailbox / in-flight 有上界，broker 进程存活 | `mqtt_conn_SUITE:a_slow_consumer_stays_bounded` |
| 热点分区不阻塞其他分区 | `mqtt_conn_SUITE:a_hot_partition_does_not_block_the_others` |
| 指标键集稳定 | `broker_metrics_SUITE:keys_are_stable` |
| 回归 | 全量 `rebar3 ct`、`dialyzer`、`xref` |

---

## 6. 验收命令

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
rm -rf _build/test/extras
rebar3 ct
rebar3 dialyzer
rebar3 xref        # 与 61 条基线比对
```

---

## 7. 风险

1. **阈值选得不合适会把正常突发判成过载**：默认值来自 S2-I 之前的保守估计（mailbox 1000、pending 4096），S2-I 用基准反推后再调；调整只改配置，不改代码。
2. **`process_info(self(), message_queue_len)` 每次 pull round 一次**：轮次频率由 poll tick / 唤醒决定，不是每消息一次；S2-I 对照埋点开/关的吞吐差异。
3. **gauge 求和漂移**：被 `kill` 的进程不跑 `terminate/2`。已在 §2 声明为诊断水位；S2-I 的 OOM 证据不依赖它。
4. **断开是唯一的 3.1.1 背压表达**：客户端会看到连接被关闭并按自己的重连策略重试；这是 MQTT 3.1.1 的既有行为，文档必须写清楚，不能宣传成“平滑限流”。

---

## 8. 交付记录

**状态：已完成（2026-11-08）。**

### 8.1 落地产物

| 文件 | 变更 |
|---|---|
| `apps/broker_proto_mqtt/src/mqtt_backpressure.erl` | 新增：`consumer/3`、`producer/2` 纯决策（`0` 关闭 mailbox 阈值；`max_inflight=0` 仍是不交付） |
| `apps/broker_metrics/src/broker_metrics.erl` | 新增 5 键：`conn_mailbox_len`、`conn_inflight`、`wal_pending_append`（gauge）、`backpressure_pauses`、`backpressure_disconnects`（counter） |
| `apps/broker_storage/src/wal_writer.erl` | `max_pending_appends`（默认配 4096）、过载在 `high` 递增前拒绝并回 `{error,{overloaded,P}}`、`wal_pending_append` 差值上报 + `terminate/2` 回收、`stats/1` 增 `pending`/`max_pending` |
| `apps/broker_proto_mqtt/src/mqtt_conn_fsm.erl` | pull round 整轮 permit（mailbox 优先、窗口次之）+ `backpressure_pauses`；PUBLISH 前生产者门；`store_and_ack` 过载断开计数；两个 gauge 的差值上报 + `terminate/3` 回收 |
| `apps/broker_proto_mqtt/src/broker_proto_mqtt_sup.erl` | `?CONN_KEYS` 增 `pull_mailbox_limit`、`publish_mailbox_limit` |
| `apps/broker_proto_mqtt/src/broker_proto_mqtt.app.src`、`config/sys.config.src` | 默认值：`pull_mailbox_limit=1000`、`publish_mailbox_limit=1000`、`wal.max_pending_appends=4096` |

### 8.2 验收证据

| 项 | 结果 |
|---|---|
| `mqtt_backpressure_SUITE` | 8/8，覆盖 mailbox 优先、窗口满、`0` 语义、生产者门限 |
| `wal_writer_SUITE` | 19/19，新增「过载 append 被拒且不消费 offset」（2 接收 / 2 拒绝，`high` 停在 3，队列最终清空） |
| `mqtt_conn_SUITE` | 60/60，新增 3 例：窗口满整轮暂停且计数增长、慢消费者（40 条只逐条 ack 5 条）mailbox ≤ 200 / 进程内存 < 4 MiB、热点分区深积压不阻塞冷分区 |
| 全量 `rebar3 ct` | **388/388 passed** |
| `rebar3 dialyzer` | 0 warnings |
| `rebar3 xref` | **61** 条（与 S2-F 基线一致，未新增未使用导出） |

### 8.3 与计划的偏差

- 计划 §2 表格里「`inflight` 超阈值」原本写作「此前是读完页再逐条 stall」，实现把判定提前到 pull round 开头，行为等价但少一次空读；无新状态。
- 阈值仍是 S2-I 之前的保守估计（1000 / 1000 / 4096），**S2-I 用基准反推后再调，只改配置不改代码**。
- Gate S2 第 2 项的 **RSS/延迟数字**仍归 S2-I；本节只提供结构与行为证据。
