# ADR-020：CloudEvents 1.0 Codec、策略与 MQTT Binding

- **状态**：已采纳（S3 冻结，2026-11-08）
- **日期**：2026-11-08
- **关联**：README §3/§4/§10.1/§11.2；ADR-004（CloudEvents 可选）、ADR-005（事件 ID 与 Broker ID 分离）、ADR-009（`event :: undefined | map()`）、ADR-015（DLQ 是普通目的地）
- **替代**：ADR-004 的「只实现 pass_through / disabled」过渡口径（策略语义不变，实现补齐）
- **关联文档**：[`docs/design/s3-cloudevents-execution-plan.md`](../design/s3-cloudevents-execution-plan.md)

## 背景（Context）

S1/S2 只实现了 `pass_through` / `disabled` 两个恒等策略，`#broker_message.event` 始终是
`undefined`。S3 要实现 README §3~§4：Codec、Structured/Binary 两种编码、五档策略、
MQTT Binding、转换损失报告、去重窗口和转换失败路径。S3 必须在不破坏 MQTT 3.1.1
字节兼容、不让事件属性参与 ACL 的前提下完成。

## 决策（Decision）

### D1. 规范版本与 JSON 表达

- 只实现 **CloudEvents 1.0 JSON Format**。Avro/Protobuf 不在 S3 范围。
- `specversion` 只接受 `<<"1.0">>`；其他值按「不支持的版本」拒绝。
- 使用 OTP 内置 `json` 模块，不引入第三方 JSON 库。

### D2. `event` 槽的形状

`#broker_message.event` 保持 ADR-009 冻结的 `undefined | map()`，**不**存 `#cloud_event{}`
记录。映射为：

```erlang
#{specversion := binary(),
  id := binary(),
  source := binary(),
  type := binary(),
  subject => binary(),
  time => binary(),              %% RFC3339 字符串，不转成机器时间
  datacontenttype => binary(),
  dataschema => binary(),
  extensions := #{binary() => binary() | boolean() | integer() | float()},
  data => term(),                %% 与 data_base64 互斥
  data_base64 => binary()}
```

可选字段只在输入出现时存在，不写 `undefined` 占位。`#cloud_event{}` 记录保留为
README §3.1 的接口说明，不参与运行时数据流。

### D3. 校验规则

- 必需属性 `specversion/id/source/type` 必须存在且为非空 UTF-8 binary；`specversion`
  必须等于 `1.0`。
- 可选属性 `subject/datacontenttype/dataschema` 必须为 UTF-8 binary；`time` 必须是
  RFC3339 形状（`YYYY-MM-DDTHH:MM:SS[.frac](Z|±HH:MM)`）。
- `data` 与 `data_base64` **互斥**；`data_base64` 必须是合法 base64 字符串。
- 扩展属性：名字只允许 `[a-z0-9]`，长度 ≤ 20；值只允许 string / boolean / integer /
  float；单个事件最多 **32** 个扩展（S0.5 结论），超限拒绝。
- `data` 的原样保留：结构化解码得到 JSON term，重新编码只用于**显式**的 Structured
  输出；`pass_through` / `validate_if_present` 永不改写 payload 字节。

### D4. 检测与策略

检测顺序（只在策略需要解析时执行）：

1. `proto_meta` 中 `mqtt_user_properties` 含 `ce-specversion` → **Binary Mode**；
2. `mqtt_content_type` 等于 `application/cloudevents+json`（忽略 `; charset=...`）
   或 payload 是 JSON 对象且含 `specversion` → **Structured Mode**；
3. 否则视为「不是 CloudEvents」。

策略语义：

| 策略 | 不是 CE | 是 CE 但非法 |
|---|---|---|
| `disabled` | 恒等，不解析 | 恒等，不解析 |
| `pass_through` | 恒等，不解析 | 恒等，不解析 |
| `validate_if_present` | 恒等 | `{error, ...}` |
| `required` | `{error, event_required}` | `{error, ...}` |
| `normalize` | `{error, event_required}` | `{error, ...}`，合法则写 `event` |

`validate_if_present` / `required` 合法时写 `event`；`normalize` 合法时写 `event` 并保持
`payload` 原字节不变（可重建信息留在 `proto_meta`）。

**策略解析**：`{policies, [{Tenant, Destination, Policy}]}`，按列表顺序**首个匹配**；
`Tenant` 为 `<<"*">>` 或精确匹配；`Destination` 支持 MQTT filter 的 `+` / `#` 语义，
也接受 `<<"*">>`。未命中回退到 `{policy, P}`（默认 `pass_through`）。策略在发布路径上
计算，不引入新进程。

### D5. MQTT Binding

CloudEvents MQTT Binding 的落地口径：

- **Structured Mode（3.1.1 与 5.0 都支持）**：payload = JSON 事件；MQTT 5 同时写
  `content_type = <<"application/cloudevents+json">>`。
- **Binary Mode（仅 MQTT 5）**：上下文写 `user_properties` 的 `ce-*` 键
  （`ce-specversion`、`ce-id`、`ce-source`、`ce-type`、可选属性、`ce-<扩展名>`）；
  payload = 事件数据原始字节；`datacontenttype` 映射到 `content_type`。
- **MQTT 3.1.1 的 Binary Mode 不可表达**：返回显式损失报告，列出所有无法表达的
  `ce-*` 属性；不静默丢弃。3.1.1 上推荐 Structured Mode。
- 转换报告形状：
  `#{mode := structured | binary, target := mqtt, version := 4 | 5,
     losses := [binary()], preserved := [binary()]}`。
  `losses` 非空即表示有非无损字段；调用方必须能拿到它。

### D6. 事件 ID 与 Broker ID

`source + id` 是事件身份；`message_id`、offset、MQTT packet id、投递状态互不覆盖
（ADR-005）。Binding 反向映射时，协议侧标识只进 `proto_meta`，永不写 `event.id`。

### D7. 去重窗口

- 默认**关闭**（不永久去重）。
- 配置 `{dedup, [{Tenant, Destination, WindowMs}]}`，首个匹配；键为
  `{Tenant, Destination, source, id}`；窗口过期后同一事件再次出现视为新事件。
- 去重表是 `broker_event_sup` 下的一个受监督 gen_server + ETS，容量有上限；进程只做
  `check/2`（第一次见到返回 `fresh`，窗口内重复返回 `duplicate`），不阻塞发布路径。
- 重投递（QoS1/2 或 broker redelivery）在默认关闭时行为不变；开启后重复事件按配置丢弃
  并由调用方正常 ACK（不产生第二次存储）。

### D8. 转换失败路径

- `apply_policy` 返回 `{error, Reason}`，调用方**不得** ACK。
- MQTT 5：QoS1 回 PUBACK `0x83`（Implementation Specific Error），QoS2 回 PUBREC
  `0x83`；QoS0 直接断开（无拒绝帧）。
- MQTT 3.1.1：无 reason code 可表达，断开且不 ACK，让 QoS1/2 发布者重试。
- 可选的隔离副本：`{quarantine, [{Tenant, Destination, TargetPattern}]}` 命中时，
  在拒绝前把**原始**消息以 `fsync` 追加到 `$invalid/{tenant}/{destination}`（模板可配），
  头部记录拒绝原因。隔离副本失败不改变拒绝结果，只记日志。
- 该路径保证「拒绝不静默丢数据」：发布者拿不到 ACK 会重试；隔离副本供排障。

### D9. ACL 边界

授权只使用认证主体（`broker_security`），**永不**读取 `event` 中的 `source` / `type` /
扩展属性。S3 增加一条回归测试，证明伪造事件属性不能改变租户或授权结果。

## 后果（Consequences）

- **+** README §3/§4 的能力可被测试逐项证明；默认 `pass_through` 不改变现有行为。
- **+** MQTT 3.1.1 线上字节完全不变；Binary Mode 在 5.0 才启用，并有显式损失报告。
- **-** 每次需要解析的发布都付一次 JSON 解码成本；默认关闭，按租户/目的地开启。
- **-** 去重窗口是有界内存表，进程重启后清空（不承诺跨重启去重）。

## 验证（Validation）

- `broker_event_SUITE`：README §11.2 全项——必需属性缺失/类型错误/非法格式、扩展保留、
  未知扩展、时间格式、`data`/`data_base64` 互斥、Structured/Binary 正反向、跨协议
  往返与损失清单、事件 ID 与消息 ID 分离、重复事件、拒绝路径、ACL 不信任事件属性。
- `mqtt_conn_SUITE`：`required` 拒绝路径的 v5/3.1.1 行为，以及默认 `pass_through` 不变。
- 全量 `rebar3 ct`、`dialyzer`、`xref` 通过。
