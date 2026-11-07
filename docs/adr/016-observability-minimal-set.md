# ADR-016：可观测性最小集与「无外部依赖」取舍

- **状态**：拟议（S2 M0 起草，2026-11-07，待评审签认）
- **日期**：2026-11-07
- **关联**：README §10.2（可观测性）/§7.2（背压）；roadmap §4.1（可观测性最小集）/§4.3 Gate S2；设计文档 `docs/design/s2-single-node-kernel.md` D6 / §4.1

## 背景（Context）

S1 的可观测性只有两处：

- `wal_writer:stats/1` 的 `#{records, batches, syncs}`（T6 为证明 group commit 而加）；
- `logger` 结构化日志（`mqtt_conn: ...`、`mqtt_retain_ignored` 等）。

没有统一的计数器、没有快照、没有延迟分位。Gate S2 的第三项（基准报告）和第四项（无单一 GenServer 瓶颈）都需要**可复跑的观测数据**，而 S2-G 的背压也需要"mailbox 水位、积压、pending"这些信号。问题是：S2 是否应该引入 Prometheus / OpenTelemetry / `telemetry` 这类依赖？

## 决策（Decision）

### 16.1 S2 不引入任何外部观测依赖

- **不引入** Prometheus client、`telemetry`、OpenTelemetry、`statix` 等库。
- 新增 `broker_metrics` app，只用 OTP 自带的 `counters` 与 `atomics`。

理由：

1. **Gate S2 只需要"能证明"，不需要"能对接"**。基准与压测脚本在仓库内，直接读 `broker_metrics:snapshot/0` 即可，不需要 HTTP exporter。
2. **依赖会进入发行版**。`rebar3 as t14 release` 目前只有 ranch 等必要依赖；为一个尚未冻结的观测接口增加第三方库，会在 S6 运维化时形成"两套导出"的历史包袱。
3. **热路径可控**。`counters:add/2` 是无锁原子操作；第三方库往往带标签、采样、上报线程，热路径开销不可预测。

HTTP 暴露 / Prometheus / OpenTelemetry 统一留到 **S6（运维化）**；届时只增加导出层（读 `snapshot/0`），**不改计数点**。

### 16.2 指标命名与最小集

`broker_metrics` 的键是 `atom()`，命名与 README §10.2 对齐，分五组：

| 组 | 键（示例） | 类型 |
|---|---|---|
| 连接 | `connections_current`、`connections_peak`、`auth_failures`、`protocol_errors`、`frame_decode_errors` | counter / gauge |
| 吞吐 | `publish_total`、`deliver_total`、`ack_total`、`bytes_in`、`bytes_out` | counter |
| 延迟 | `publish_latency_us`、`deliver_latency_us`、`ack_latency_us`、`wal_sync_latency_us` | histogram（固定桶） |
| 积压 | `queue_depth`、`subscription_backlog`、`cursor_lag`、`dlq_size`、`redelivery_total` | gauge / counter |
| 存储 | `wal_append_total`、`wal_sync_total`、`segment_bytes`、`recovery_time_us`、`retention_deleted_bytes` | counter / gauge |

- **直方图用固定桶**（默认 `[100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000]` 微秒），不依赖外部库；`observe/2` 记录 `{Count, Sum, Buckets}`。
- **禁止高基数标签**：不把 destination、client_id、message_id 作为键；需要按维度拆时用**预定义枚举**（如 `qos`、`kind`），由 `broker_metrics` 显式维护有限键集合。README §10.1 的"高基数指标爆炸"约束在此落地。

### 16.3 API 形状

```erlang
-module(broker_metrics).
-export([inc/1, inc/2, observe/2, gauge/2, snapshot/0, reset/0]).

inc(Key)            -> ok.        % counters:add(C, Idx, 1)
inc(Key, N)         -> ok.
observe(Key, Us)    -> ok.        % 直方图
gauge(Key, Value)   -> ok.        % atomics:put
snapshot()          -> #{atom() => integer() | {Count, Sum, Buckets}}.
reset()             -> ok.        % 测试用
```

- 键必须在**编译期或启动期**登记（`broker_metrics:keys/0` 返回固定列表），未登记键在 `inc/1` 时记 `unknown_metric` 并拒绝，避免拼写错误静默产生新指标。
- `snapshot/0` 是纯读，不阻塞热路径；进程内聚合用 `persistent_term` 指向 ETS/counters 引用，避免每次读数走 gen_server。

### 16.4 埋点边界

- **热路径只调用 `counters:add/2` / `atomics:put/3`**，不分配、不格式化、不打日志。
- **周期日志**由 `broker_metrics_sup` 下的一个 timer 进程每 `report_interval_ms`（默认 60 s）输出一行摘要（`logger:info`），供无监控环境排查；可用 `report_interval_ms = 0` 关闭。
- **不改变行为**：所有埋点必须在功能路径之外，关闭 metrics（`broker_metrics.enabled = false`）时 API 仍可调用但为空操作；CT 套件在开/关两态下功能结果必须一致。

### 16.5 与 S3 的接口

- CloudEvents 的计数点（校验成功/失败、转换失败、未知版本）在 S2 只**预留键名**（不埋点），由 S3 填入。键名前缀 `cloudevents_`。
- 这样 S3 不需要改 `broker_metrics` 的结构，只需在 codec 里调 `inc/2`。

## 后果（Consequences）

- **+** 发行版无新增第三方依赖，安装与审计面不变。
- **+** 指标集合有限且稳定，`snapshot/0` 可被基准脚本直接消费。
- **+** 热路径开销可测（S2-I 前后对照），且可一键关闭。
- **-** 没有开箱即用的 Prometheus 端点；S6 之前对接监控需要自己写导出。
- **-** 固定桶直方图的精度有限；若某延迟分布跨桶严重，需要调整桶边界（配置化）。
- **-** 指标键在启动期登记，新增指标要改代码而非改配置；这是**故意**的（防止高基数）。
- **-** `persistent_term` 在更新引用时会触发全局 GC 暂停；实现应只在启动时写一次，不在运行时更新。

## 验证（Validation）

- **稳定性**：`broker_metrics_SUITE` 的 `keys_are_stable` 用例钉住 `keys/0` 的集合；新增/删除键必须改测试。
- **无副作用**：同一组 CT 用例在 `enabled=true` 与 `enabled=false` 下结果一致；`snapshot/0` 在空负载时返回所有键的零值。
- **热路径开销**：S2-I 基准对照（埋点开/关）吞吐差异 ≤ 2%；超过则改为采样并记录。
- **无高基数**：测试断言键集合是有限原子列表，不含 binary/动态拼接。
- **周期日志**：`report_interval_ms` 生效，输出一行且不刷屏；`0` 时无输出。
- **Gate S2**：本条为 Gate 4（无单一 GenServer 瓶颈）提供"可观测证据"；报告进 `docs/benchmarks/`。
