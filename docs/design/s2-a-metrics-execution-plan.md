# S2-A 执行计划：`broker_metrics` 可观测性最小集

- **状态**：已完成（2026-11-07；全量 `rebar3 ct` 278/278，零 `FAILED`/零 `Skipped`；`dialyzer` 干净；`as t14 release` 含 `broker_metrics` 且可启动）
- **对应**：`docs/design/s2-single-node-kernel.md` §4.1；`docs/adr/016-observability-minimal-set.md`
- **估算**：3 人日
- **退出条件**：`broker_metrics:snapshot/0` 稳定、埋点开/关功能一致、热路径吞吐差异 ≤2%、周期日志可控

---

## 1. 目标与非目标

**目标**：给 Gate S2 提供可机械复跑的观测底座——连接/吞吐/延迟/积压/存储五组指标，一个纯读 `snapshot/0`，一个可关闭的周期日志。

**非目标（S6 再做）**：HTTP 端点、Prometheus 文本格式、OpenTelemetry、按 destination/client 的高基数标签、告警规则引擎。

---

## 2. 交付物

| 文件 | 动作 | 说明 |
|---|---|---|
| `apps/broker_metrics/src/broker_metrics.app.src` | 新建 | `applications = [kernel, stdlib, sasl]`；无外部依赖 |
| `apps/broker_metrics/src/broker_metrics_app.erl` | 新建 | `start/2` → sup |
| `apps/broker_metrics/src/broker_metrics_sup.erl` | 新建 | `one_for_one`；子：`broker_metrics_server`（仅在 `report_interval_ms > 0` 时启用） |
| `apps/broker_metrics/src/broker_metrics.erl` | 新建 | 公共 API：`inc/1,2`、`observe/2`、`gauge/2`、`gauge_add/2`、`snapshot/0`、`reset/0`、`keys/0`、`p99/1` |
| `apps/broker_metrics/src/broker_metrics_server.erl` | 新建 | 周期日志；读 `snapshot/0`，一行摘要 |
| `apps/broker_metrics/test/broker_metrics_SUITE.erl` | 新建 | 见 §6 |
| `config/sys.config.src` | 修改 | 增加 `{broker_metrics, [{enabled, true}, {report_interval_ms, 60000}]}` |
| `rebar.config` | 修改 | `relx` release 列表加入 `broker_metrics`；`{shell, [{apps, ...}]}` 视需要加入 |
| 各埋点模块 | 修改 | 见 §5 |

---

## 3. 公共 API 契约（冻结）

```erlang
-module(broker_metrics).
-export([inc/1, inc/2, observe/2, gauge/2, gauge_add/2,
         snapshot/0, reset/0, keys/0, p99/1]).

-type key() :: atom().
-type histogram() :: #{count := non_neg_integer(),
                       sum := non_neg_integer(),
                       buckets := [{Boundary :: non_neg_integer(), Count :: non_neg_integer()}]}.

-spec inc(key()) -> ok | {error, term()}.
-spec inc(key(), non_neg_integer()) -> ok | {error, term()}.
-spec observe(key(), non_neg_integer()) -> ok | {error, term()}.   % 微秒
-spec gauge(key(), integer()) -> ok | {error, term()}.
-spec gauge_add(key(), integer()) -> integer() | {error, term()}.
-spec snapshot() -> #{key() => integer() | histogram()}.
-spec reset() -> ok.
-spec keys() -> [key()].
-spec p99(histogram()) -> non_neg_integer().
```

**语义**：

- `enabled = false` 时三个写 API 是 `ok` 空操作，`snapshot/0` 仍返回所有键的零值（键集合稳定）。
- **`gauge_add/2` 是 `gauge/2` 之外的增补**：原子 read-modify-write，返回新值，供 `connections_current` 这类"活计数"使用；`gauge/2` 仍是"写绝对值"（`connections_peak` 用）。`enabled=false` 或未启动时 `gauge_add/2` 返回 `0`（`inc/gauge/observe` 返回 `ok`），调用方因此能区分"没记账"而不必再查开关。
- 未登记的键：写 API 返回 `{error, {unknown_metric, Key}}`，并 `logger:warning` 一次（每进程限频），**不创建新指标**；键存在但 Kind 不符返回 `{error, {wrong_kind, Key, Kind}}`。
- `snapshot/0` 在调用者进程读 `counters`/`atomics`，不经 gen_server。
- `p99/1` 对 `snapshot/0` 里的直方图取近似 99 分位；桶不足时落在 `+Inf` 槽的观测返回最后一个有限边界（不虚报）。
- `reset/0` 仅供测试；生产不调用。

---

## 4. 键登记表

`keys/0` 返回固定列表，实现用 `persistent_term` 缓存 `#{Key => {Kind, Ref, Idx}}`。

| Key | Kind | 埋点位置 |
|---|---|---|
| `connections_current` | gauge | `mqtt_conn_fsm` enter/terminate |
| `connections_peak` | gauge（只在更高时写） | `mqtt_conn_fsm` enter |
| `auth_failures` | counter | `mqtt_conn_fsm` CONNECT 失败分支 |
| `protocol_errors` | counter | `mqtt_conn_fsm` 协议错误断连 |
| `frame_decode_errors` | counter | `mqtt_frame` 解码失败（或 conn 捕获处） |
| `publish_total` | counter | `mqtt_conn_fsm` 收到 PUBLISH |
| `deliver_total` | counter | `mqtt_conn_fsm` 发出 PUBLISH |
| `ack_total` | counter | `mqtt_conn_fsm` 收到 PUBACK |
| `bytes_in` / `bytes_out` | counter | `mqtt_conn_fsm` 收/发帧 |
| `publish_latency_us` | histogram | `mqtt_conn_fsm:store_and_ack/6` 外层（append 调用 → 返回） |
| `deliver_latency_us` | histogram | `#broker_message.created_at` → PUBLISH 上线（`note_delivery/1`） |
| `ack_latency_us` | histogram | PUBLISH 发出 → PUBACK 收到 |
| `wal_sync_latency_us` | histogram | `wal_writer` group commit 的 sync 段（`sync_fd_measured/1`） |
| `queue_depth` | gauge | `wal_writer` append 后写 `high - committed`；**多分区共用一个键，最后写入者覆盖 ⇒ 近似全局值**，分区精确值用 `broker_queue:watermarks/1` |
| `subscription_backlog` | gauge | `broker_subscription_worker:sample_gauges/1`，每次 cursor tick（默认 100 ms）取 `sum(High - cursor)` 聚合值 |
| `cursor_lag` | gauge | 同上，取 `sum(High - committed)`（未被全体订阅者 ack 的窗口） |
| `redelivery_total` | counter | ADR-015 实现后接入（S2-A 只登记键） |
| `dlq_size` | gauge | ADR-015 实现后接入（只登记键） |
| `wal_append_total` / `wal_sync_total` | counter | `wal_writer`（append 成功 / sync 成功） |
| `segment_bytes` | gauge | `wal_writer` 每次 append 后写当前段末尾位置；多分区共用一个键（最后写入者覆盖），只用于粗看，精确值看段文件 |
| `recovery_time_us` | histogram | `broker_storage_wal:recover/2` 外层（含段扫描） |
| `retention_deleted_bytes` | counter | ADR-014 实现后接入（只登记键） |
| `cloudevents_validate_ok` / `_failed` | counter | **只登记，S3 埋点** |

**禁止**：任何 binary 键、动态拼接键、以 destination/client_id/message_id 为维度的键。

> **时点说明**：本表是 S2-A 交付时的**登记注册表**（26 个键）。之后每个阶段继续按同一「封闭 atom 集合」规则登记新键，当前 `broker_metrics:keys()` 共 **58** 个（`broker_metrics_SUITE` 的 `?KEYS` 是机械钉住集合的权威，不是本表）。
>
> 三行当时标注「只登记键」的条目现已接线：`
> redelivery_total` 与 `dlq_size` 在 `broker_subscription_worker:955`（重试调度）与 `broker_dlq:146`（GC 时递减）；`retention_deleted_bytes` 在 `wal_writer:641`；`cloudevents_validate_ok/_failed` 在 `broker_event` 的 `attach_event/2` 与 `reject/1`。

---

## 5. 任务拆解

### T-A1 应用骨架（0.5 人日）

1. 建 `apps/broker_metrics/{src,test}`、`.app.src`、`_app`、`_sup`。
2. `rebar.config` 的 relx release 列表加 `broker_metrics`（放在 `broker_core` 之后）。
3. `config/sys.config.src` 加 `broker_metrics` 段。
4. **验收**：`rebar3 compile` 无 warning；`rebar3 as t14 release` 成功；`application:ensure_all_started(broker_metrics)` 返回 `{ok, _}`。

### T-A2 存储层（0.5 人日）

1. `broker_metrics.erl` 启动时创建 `counters:new/2`（写并发）与 `atomics:new/2`（gauge），键→索引映射存 `persistent_term`。
2. 实现 `inc/1,2`、`gauge/2`；直方图用 `atomics` 数组（count/sum/桶计数）或 `counters` 多槽。
3. 实现 `snapshot/0`：遍历 `keys/0`，按 Kind 读值。
4. **验收**：`broker_metrics_SUITE` 的 API 用例（见 §6）通过。

### T-A3 直方图与桶（0.25 人日）

1. 默认桶：`[100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000]` 微秒。
2. `observe/2` 找到第一个 `Value =< Boundary` 的桶并加一，同时累加 count/sum；超出最大桶计入 `+Inf` 槽。
3. **验收**：边界值（恰好等于桶边界、0、超过最大桶）用例。

### T-A4 埋点（1 人日）

按 §4 表逐点接入，**每次只改一个模块并跑该模块的既有 CT 套件**，确保行为不变：

1. `mqtt_conn_fsm`：连接/认证/协议错误/收发计数/延迟（`publish_latency_us` 在 append 调用外层计时）。
2. `wal_writer`：append/sync 计数与 sync 直方图、`segment_bytes`、`queue_depth`。
3. `broker_subscription_worker`：backlog/cursor lag 的聚合 gauge（每次 cursor tick 采样一次，避免每次读写都算）。
4. `broker_storage_wal:recover/2`：`recovery_time_us`。

**验收**：全量 `rebar3 ct` 278/278 不变；`enabled=false` 时同样通过（写 API 空操作）。

### T-A5 周期日志（0.25 人日）

1. `broker_metrics_server` 每 `report_interval_ms` 输出一行：`broker_metrics: connections=… publish=… deliver=… ack=… wal_sync_p99≈…`。
2. `report_interval_ms = 0` 时不启动该子进程。
3. **验收**：CT 用 `sys:get_state`/捕获日志断言一行且不刷屏。

### T-A6 稳定性与无副作用测试（0.5 人日）

见 §6。

### T-A7 文档（0.25 人日）

1. `docs/architecture.md`：新 app 的进程边界、`snapshot/0` 数据流、配置表。
2. `docs/design/s2-single-node-kernel.md` §4.1 标记"已实现"（完成后）。
3. `progress.md` 任务表新增 S2-A 行。

---

## 6. 测试清单（`broker_metrics_SUITE`）

| 用例 | 断言 |
|---|---|
| `keys_are_stable` | `?KEYS`（测试套件内定义的 58 键）等于 `keys()`，顺序无关；集合变化必须改测试 |
| `inc_accumulates` | `inc(k, 3)` 后 `snapshot()` 为 3 |
| `gauge_overwrites` | `gauge(k, 5)`、`gauge(k, 2)` 后为 2 |
| `gauge_add_accumulates` | `gauge_add/2` 增减与返回值；Kind 不符/未知键被拒 |
| `observe_buckets` | 0、边界值、超最大桶的桶计数正确 |
| `snapshot_is_pure_read` | 连续两次 `snapshot()` 不改变值 |
| `unknown_key_is_rejected` | `inc(bogus)` 返回 `{error, {unknown_metric, bogus}}` |
| `wrong_kind_is_rejected` | 对 counter 调 `gauge/2` 返回 `{error, {wrong_kind, _, _}}` |
| `disabled_is_noop` | `enabled=false` 时写 API 返回 `ok`（`gauge_add` 返回 `0`）、snapshot 全零 |
| `reset_clears` | `reset()` 后 snapshot 全零 |
| `no_high_cardinality_keys` | `keys()` 全是 atom；无 binary/动态键 |
| `report_line_is_single` | 周期日志一行；`0` 时无输出 |

另：在 `s1_e2e_SUITE` 复制一组"同用例 + metrics 开/关"的对照断言（或复用现有套件跑两遍）。

---

## 7. 验收命令

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
rm -rf _build/test/extras
rebar3 ct --suite apps/broker_metrics/test/broker_metrics_SUITE
rebar3 ct                     # 278/278，零 FAILED / 零 Skipped
rebar3 dialyzer
rebar3 xref                   # 只允许预期 unused-export
rebar3 as t14 release
```

**Gate 证据**：S2-I 基准中"埋点开/关"吞吐对照；`snapshot/0` 键表快照；周期日志样例。

---

## 8. 风险

| 风险 | 缓解 |
|---|---|
| `persistent_term` 更新触发全局 GC | 只在启动时写一次；`reset/0` 只清 counters，不重建引用 |
| 直方图原子操作在每消息路径的成本 | 只对 publish/ack/wal_sync 三处做直方图；其余用 counter |
| `snapshot/0` 遍历所有键在键多时变慢 | S2-A 时 ≤ 40 键，O(40) 可接受；S6 后键集合为 58（§4 时点说明），遍历仍是 O(键数)，导出层再加缓存 |
| 埋点遗漏导致 Gate 4 证据不足 | §4 表逐项对照 review；`keys_are_stable` 钉住集合 |
