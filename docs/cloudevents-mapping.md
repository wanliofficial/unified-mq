# CloudEvents 映射（S3 起，S6 补齐六协议；S8 的三条 MQTT 传输见 §3 末注）

本文只写**已实现**的行为：CloudEvents 1.0 JSON Format 的内部模型、五种策略、
去重与隔离、六个协议的 Binding 及非无损点。设计依据是
[ADR-020](adr/020-cloudevents-codec-policy-binding.md)（MQTT 绑定冻结）与
S6 的统一头通道扩展（[`protocol-compatibility.md`](protocol-compatibility.md) §4.10）。

- 代码：`apps/broker_event/src/{broker_event.erl,broker_event_codec.erl,broker_event_binding.erl,broker_event_dedup.erl}`
- 测试：`apps/broker_event/test/broker_event_SUITE.erl`（35 例）
- 范围：只实现 **CloudEvents 1.0 JSON Format**；Avro/Protobuf 格式不在范围。

---

## 1. 内部模型

解码后的事件视图是 `#broker_message{}.event` 上的 map（ADR-020 §D2）：

| 键 | 类型 | 说明 |
|---|---|---|
| `specversion` | binary | 必须为 `<<"1.0">>` |
| `id` / `source` / `type` | binary | 必需，非空 |
| `subject` / `time` / `datacontenttype` / `dataschema` | binary | 可选；存在才写入 |
| `data` | binary \| term | 事件数据；Binary Mode 下为原始字节 |
| `extensions` | map | 扩展属性；**总是存在**，缺省 `#{}` |

可选属性缺省时**不出现**（不是 `undefined`）。`data` 与 `data_base64` 互斥。

**校验**（`broker_event_codec:validate/1`）：

1. 必需属性缺失 → `{error, {invalid_event, {missing, [...]}}}`；
2. 核心属性类型/`specversion`/`time` 格式错误 → 对应 `invalid_event`；
3. `data`/`data_base64` 同时出现、base64 非法、扩展名/数量越界 → 对应错误；
4. S6 限额（`broker_limits:check_event/1`）：属性数 `max_event_attributes`
   （默认 64）、扩展值长度 `max_extension_value_bytes`（4096）、
   `data` 的 JSON 嵌套深度 `max_json_depth`（16）；越界返回
   `{error, {invalid_event, {limit_exceeded, Which, Actual, Max}}}`。

---

## 2. Structured 与 Binary Mode

**Structured Mode**：payload 就是事件 JSON；MQTT 5 同时设置
`content_type = application/cloudevents+json`。反解走
`broker_event_codec:decode_structured/1`。六个协议都支持（payload 逐字节透传）。

**Binary Mode**：payload 是事件数据字节，上下文写协议元数据，键名统一为
`ce-<属性名>`：核心属性（`ce-specversion`、`ce-id`、`ce-source`、`ce-type`、
`ce-subject`、`ce-time`、`ce-dataschema`）、`datacontenttype` 写 `content-type`、
扩展属性写 `ce-<name>`。反解走 `broker_event_codec:decode_binary/3`。

两条通道都识别：

- MQTT 5：`proto_meta.mqtt_user_properties`（ADR-020 §D5）；
- WS / NATS / AMQP / Kafka / Pulsar：`#broker_message.headers` 里的 `ce-*`
  与 `content-type`。

`broker_event_binding:detect/1` 的优先级：MQTT user properties → `ce-*` 头 →
`content-type` 声明 → payload 里的 `specversion`。

---

## 3. 六个协议的 Binding

| 协议 | Binary Mode 通道 | 投递方向 | 证据 |
|---|---|---|---|
| MQTT 5 | User Properties（`ce-*`）+ `content_type` | 同左；3.1.1 返回损失报告 | `mqtt_binary_binding_v5_roundtrip` / `mqtt_binary_binding_311_reports_losses` |
| WebSocket | 应用帧顶层 `ce-*` + `content-type` | 投递帧原样回传 | `ws_conn_SUITE:cloudevent_headers_roundtrip` |
| NATS | `HPUB` 的 `NATS/1.0` 头块 | `HMSG`（客户端须在 CONNECT 声明 `headers: true`） | `nats_conn_SUITE:cloudevent_headers_roundtrip` |
| AMQP | basic properties 的 `headers` table + `content_type` | 投递写回 properties | `amqp_codec_SUITE:properties_roundtrip_with_ce_headers` |
| Kafka | Record Headers | 投递写回 Record Headers | `kafka_codec_SUITE:record_batch_headers_and_timestamps` |
| Pulsar | Message properties | 投递写回 properties | `pulsar_codec_SUITE:message_metadata_roundtrip` |

正反向映射统一走 `broker_event_binding:to_headers/2,3` 与 `from_headers/3`；
五个头通道目标在 `header_binding_roundtrip_for_all_targets` 里逐一往返。

**S8 的三条新传输不在本节范围内**：MQTT over WebSocket 与 MQTT over QUIC 复用同一份 `mqtt_conn_fsm`，因此继承上表 MQTT 行（5.0 的 User Properties 通道、3.1.1 的损失报告）**一字不改**；MQTT-SN 例外——1.2 的 PUBLISH 没有任何用户元数据槽位，`mqtt_sn_conn` 构造的消息 `headers` 恒为 `#{}`，**只有 Structured Mode、没有 Binary Mode 通道**。详见 [`protocol-compatibility.md`](protocol-compatibility.md) §4.10 的 S8-C 行与 §4.11。

---

## 4. 策略

`broker_event:apply_policy/1` 在执行存储前运行，六个协议发布路径都调用它
（S8-C 的 MQTT-SN 也调用同一个 seam，`mqtt_sn_conn.erl:1027`）；
默认 `pass_through` 是恒等变换（不解析、不改字节）。

| 策略 | 行为 |
|---|---|
| `disabled` | 恒等，与 `pass_through` 相同 |
| `pass_through` | 恒等（默认） |
| `validate_if_present` | 是事件就校验；普通消息放行 |
| `required` | 必须是合法事件，否则拒绝 |
| `normalize` | 校验通过后把解码视图写入 `message.event`，payload 不变 |

策略解析顺序：`{Tenant, Destination, Policy}` 列表首个匹配胜出
（`<<"*">>` 为通配，destination 支持 MQTT filter 语义），无匹配回落全局
`broker_event.policy`。

**拒绝路径**按协议语义表达：MQTT 5 `0x83`；WS `cloudevent_rejected` 错误帧；
NATS `-ERR` 并断开；AMQP channel 406；Kafka `INVALID_RECORD`（87）；
Pulsar `SendError`。拒绝的消息不写 WAL、不 ACK。MQTT-SN 没有通用错误通道：
QoS 1 回 PUBACK `NOT_SUPPORTED`，QoS 0/2/−1 静默丢弃（QoS 2 不发 PUBREC，客户端重试）。

---

## 5. 去重与隔离

> **接线范围**：这两条只在 **MQTT 发布路径**（`mqtt_conn_fsm` 的 `handle_publish1`）调用。其余五个协议目前只调用 `broker_event:apply_policy/1`，不调用 `broker_event:duplicate/1` 与 `broker_event:quarantine/2`——即使配了 `dedup` / `quarantine`，WS/NATS/AMQP/Kafka/Pulsar 也不会去重、也不会写隔离副本。把这两个钩子推广到其余五协议是 S7 之后的工作（已列入 `docs/security.md` §12 与 `docs/architecture.md` §9 的遗留清单）。

- **去重键**：`{Tenant, Destination, Source, Id}`，窗口由
  `broker_event.dedup` 的 `{Tenant, Destination, WindowMs}` 配置；
  未配置即不去重。命中重复计 `cloudevents_duplicates` 并按成功处理
  （不重复写日志）。
- **隔离**：`broker_event:quarantine/2` 把非法事件的可持久化副本写到
  `broker_event.quarantine` 配置的目标（`{Tenant, Destination, Target}`，
  `Target` 是含 `{tenant}` / `{destination}` 占位符的普通 destination；
  未配置则返回 `not_configured`，拒绝仍然生效），计
  `cloudevents_quarantined`。副本带 `event_rejection` 头、新 `message_id`、
  `expires_at = undefined`，写失败保留拒绝。隔离目标是普通可订阅 destination，
  不绕过 ACL。它与 S2-E 的订阅重试 DLQ（`dlq_name_pattern`，默认
  `$dlq/{tenant}/{original}`）是两条独立路径。
- **事件 ID ≠ Broker ID**：`source + id` 是事件身份；`message_id`、offset、
  packet id 只进 `proto_meta`，永不写 `event.id`（ADR-005 / ADR-020 §D6）。

---

## 6. 转换报告与已知非无损点

`broker_event_binding:to_mqtt/3` 与 `to_headers/2,3` 返回
`#{mode, target, version, losses, preserved}`；`losses` 非空即表示有非无损字段，
调用方必须读取它，不允许静默丢弃。

| 场景 | 非无损点 |
|---|---|
| 任意协议 Binary Mode | 扩展属性值序列化为文本；`data` 若是 JSON 对象则变为序列化字节 |
| MQTT 3.1.1 Binary Mode | 无元数据通道，全部 `ce-*` 属性列入 `losses`（数据字节仍保留） |
| NATS 未协商 headers | 收 `MSG`，全部 `ce-*` 属性丢失（由 `headers_cap` 决定） |
| AMQP 0-9-1 | `basic.publish` 无逐消息否认；拒绝表现为 channel 406，客户端需重开 channel |

Structured Mode 在六个协议上逐字节无损（payload 不变，因此 `id`/`source`/
`type`/扩展都保留）。

---

## 7. 指标

`cloudevents_validate_ok`、`cloudevents_validate_failed`、
`cloudevents_duplicates`、`cloudevents_quarantined`。指标键是封闭集合，
**不以**任意 `source` / `subject` / `id` 作标签（README §10.1 防高基数）。
