# ADR-004：CloudEvents 是可选事件标准化层

- **状态**：已采纳（S0 定稿）
- **日期**：2026-10-14
- **关联**：README §3/§4（CloudEvents 模型）、roadmap §2 S0.4/S0.5；S0.5 spike

## 背景（Context）

CloudEvents 提供跨系统的事件标准化，但若强制所有消息都走 CloudEvents，会破坏原生协议（如纯 MQTT/QoS 消息）的兼容性与性能。

## 决策（Decision）

普通消息**继续可用**；CloudEvents 作为**可选**标准化层，其校验 / 透传 / 规范化按 **租户 + 目的地**配置，策略开关包括：`disabled` / `pass_through` / `validate_if_present` / `required` / `normalize`。

## 后果（Consequences）

- **+** 原生协议兼容不被破坏，可渐进启用标准化。
- **+** 不同租户/目的地可独立设定严格度。
- **-** 需要策略开关与转换报告机制（S3）；转换失败路径不能静默丢数据（需 DLQ/隔离）。

## 验证（Validation）

S0.5 spike（`spikes/s0_5_cloudevents/REPORT.md`）：扩展属性成本随数量线性增长，建议默认上限 32 个，避免 CPU DoS 向量。S3 实现 Codec + Binding Mapper + 转换报告。
