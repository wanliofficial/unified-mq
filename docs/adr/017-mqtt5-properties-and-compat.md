# ADR-017：MQTT 5 properties 的落位与 3.1.1 兼容边界

- **状态**：**已采纳（S2 M0 冻结，2026-11-07；实现排到 S2.5）**——用户签认；后续修改须走 ADR 修订流程
- **日期**：2026-11-07
- **关联**：README §3.4/§11.1；roadmap §4.1（MQTT 5.0 Session 与 User Properties）；ADR-009 §9.2（`headers` vs `proto_meta` 分工）；设计文档 `docs/design/s2-single-node-kernel.md` D8 / §4.8

## 背景（Context）

S1 的 `mqtt_frame` 是 MQTT 3.1.1 的帧集：`#connect{}`、`#publish{}`、`#subscribe{}` 等 record 里**没有 properties 字段**。CONNECT 按 3.1.1 解析，遇到 v5 走拒绝路径（CONNACK `20 03 00 87 00`，`0x87` 为 M16 冻结值）。

S2 的 Gate 四项都不要求 MQTT 5，用户已拍板（O3）**后移到 S2.5**。但 S2 的帧改造（QoS 2、Retain、Will）会碰 `mqtt_frame.hrl`，如果现在不把"properties 放哪、3.1.1 怎么不被破坏"写清楚，S2.5 会面对一个已经被 S2 改过一遍的 record，落位决策会被动。

本 ADR 因此只冻结**接口与边界**，不承诺 S2.5 的实现细节。

## 决策（Decision）

### 17.1 时序：MQTT 5 属于 S2.5，不属于 Gate S2

- S2（M1~M4）**不实现** properties 的解析与语义，只做 17.3 的兼容预留。
- S2.5 的交付范围：CONNECT/PUBLISH/SUBSCRIBE 的 properties、reason codes、session expiry、`$share` 语法。topic alias / request-response 留 S5。
- Gate S2 的验收不引用 MQTT 5；S2.5 失败不回滚 Gate S2。

### 17.2 properties 落 `proto_meta`（协议私有），不进 `headers`

按 ADR-009 §9.2 的判据——"换一个协议后该 key 是否仍有意义"：

| property | 落位 | 理由 |
|---|---|---|
| User Properties | `proto_meta.mqtt_user_properties`（`[{binary(), binary()}]`） | MQTT 5 私有；其他协议有各自的 metadata 机制 |
| Payload Format Indicator | `proto_meta.mqtt_payload_format` | 同上 |
| Message Expiry Interval | 映射到 `#broker_message.expires_at`（`created_at + interval`） | 这是**跨协议有意义**的生命周期，ADR-009 已留槽位 |
| Content Type | `proto_meta.mqtt_content_type`（S3 可映射到 CloudEvents `datacontenttype`，届时走 Binding Mapper） | 3.1.1 无此字段，S3 才需要 |
| Response Topic / Correlation Data | `proto_meta`（S2.5 不做 request/response，仅透传存储） | MQTT 5 私有 |
| Session Expiry / Receive Maximum / Maximum Packet Size | `proto_meta`（CONNECT 级，随会话保存） | 连接协商参数，不是消息元数据 |
| Subscription Identifier | `proto_meta`（SUBSCRIBE 级） | 订阅私有 |

**`headers` 仍然只承载 CloudEvents 扩展属性与未来跨协议公共元数据**，S2.5 **不**把 MQTT properties 塞进 `headers`。这样 ADR-009 的字段冻结不被破坏，S3 的 CloudEvents 映射也不会被 MQTT 私有字段污染。

### 17.3 S2 期间的兼容预留（必须遵守）

1. **record 扩展必须向后兼容**：`mqtt_frame.hrl` 的 record 若在 S2 被改（如 `#publish{}` 加字段），必须给新字段默认值，且 3.1.1 编解码路径不读写它。**不得**改变既有字段的顺序或类型。
2. **v5 CONNECT 拒绝路径保持**：S2 不改 `20 03 00 87 00` 的字节；`0x87` 是冻结值（规范值为 `0x84`，S2.5 语义修正时再处理，见 17.4）。
3. **不嗅探协议版本**：3.1.1 与 5.0 的区分只看 CONNECT 的 `proto_level`（4 或 5），不通过 payload / properties 猜测。
4. **properties 长度前缀预检**：S2 的帧解码器遇到 3.1.1 帧不解析 properties；若帧尾有多余字节，按既有"协议错误断连"处理，不静默截断。

### 17.4 reason code 修正项（S2.5 处理，S2 记录）

- 当前 CONNECT 拒绝使用 `0x87`（Not authorized），规范对"协议版本不支持"应为 `0x84`（Unsupported Protocol Version）。
- S2 **不改**（M16 已冻结、T15 已按 `0x87` 建立兼容性基线）；S2.5 引入 v5 reason code 表时一并修正，并在 `docs/protocol-compatibility.md` 记录迁移说明。
- 该修正是**语义修正**，不改变帧长度（仍是 `20 03 <flags> <reason>`）。

### 17.5 session expiry 与会话文件

- MQTT 5 的 Session Expiry Interval 决定"断开后会话保留多久"。S2.5 落位时，`broker_subscription` 的 `.subs` 记录增加 `expires_at`（会话级），由 boot step 2 清理过期会话。
- S2 的 `.subs` v2（ADR-013）**预留** `expires_at = undefined` 字段位，避免 S2.5 再升一次 vsn。
- `clean_start = 1` 等价于 S1 的 `clean_session = 1`：删除会话，不保留 cursor/inflight。

## 后果（Consequences）

- **+** ADR-009 的 `headers`/`proto_meta` 分工不被破坏，S3 的 CloudEvents 映射有干净边界。
- **+** MQTT 5 移出 Gate 路径，S2 可提前收口；S2.5 有独立的接口契约。
- **+** Message Expiry 直接复用 `expires_at`，与 ADR-015 的 TTL 机制统一。
- **-** `$share` 语法依赖 ADR-013 的 SubRef，S2.5 必须在 S2-D 之后；S2 期间 3.1.1 客户端无法使用 Shared。
- **-** `0x87` 的历史基线需要一次显式迁移说明，真实客户端可能依赖当前字节。
- **-** properties 全进 `proto_meta` 意味着跨协议转换（S3/S5）需要显式映射表，不能自动透传；这是**故意**的（避免私有字段泄漏）。
- **-** 本 ADR 不冻结 S2.5 的全部 properties 语义，只冻结落位与边界；S2.5 开工时另写执行计划。

## 验证（Validation）

- **S2 期间**：
  - `mqtt_frame_SUITE` 断言 record 字段顺序/类型不变（`record_info(fields, publish)` 等），3.1.1 往返字节不变。
  - v5 CONNECT 拒绝帧仍是 `20 03 00 87 00`（T15 基线）。
  - `.subs` v2 记录含 `expires_at = undefined` 字段位。
- **S2.5 开工时**：
  - properties 往返（CONNECT/PUBLISH/SUBSCRIBE）与 paho-mqtt 5 真实客户端。
  - User Properties 不进入 `headers`，`#broker_message.headers` 仍只含 CloudEvents/公共元数据。
  - Message Expiry 映射到 `expires_at` 后被 ADR-015 的 TTL 过滤。
  - `$share/{group}/{filter}` 创建 `{shared, Group, Filter}` 并被 Shared 投递逻辑消费。
  - `0x84` 修正后，v5 客户端拿到正确的 Unsupported Protocol Version，3.1.1 路径字节不变。
- **文档**：`docs/protocol-compatibility.md` 的 MQTT 行拆成 3.1.1 / 5.0 两行；`docs/architecture.md` 的 `proto_meta` 表增加 MQTT 5 键。
