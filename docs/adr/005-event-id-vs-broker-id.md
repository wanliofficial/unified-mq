# ADR-005：事件 ID 与 Broker Message ID 分离

- **状态**：已采纳（S0 定稿）
- **日期**：2026-10-14
- **关联**：README §3.1/§4.1；roadmap §5.1；S5 Binding Mapper

## 背景（Context）

跨协议流转时，协议侧标识（Kafka Offset、Pulsar Message ID、MQTT Packet Identifier）容易错误地覆盖或混入 CloudEvents 的 `id`，导致事件身份在不同协议间漂移。

## 决策（Decision）

- **CloudEvents `source + id`** 表示事件身份，跨协议保持稳定。
- **Broker Message ID / Offset / Pulsar Message ID / 投递状态** 服务于 Broker 内部的生命周期（去重、ACK、游标、重投），**互不覆盖**。
- 去重窗口按租户/目的地配置，键为 `source + id`，**不默认永久去重**。

## 后果（Consequences）

- **+** 跨协议事件身份稳定、可去重、可审计。
- **-** 需要显式的 Binding Mapper 层（S3）处理协议标识 ↔ 事件身份的映射，并明确列出不可表达字段。

## 验证（Validation）

S3 Binding Mapper 正反向映射测试与跨协议往返测试（roadmap Gate S3），转换损失项须有明确清单。
