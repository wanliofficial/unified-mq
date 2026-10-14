# ADR-001：协议 Adapter 与消息内核分离

- **状态**：已采纳（S0 定稿）
- **日期**：2026-10-14
- **关联**：README §2/§8（目录结构）、roadmap §0 原则 3；S1 三个核心抽象

## 背景（Context）

UnifiedMQ 需同时支持 MQTT / WebSocket / NATS / AMQP / Kafka / Pulsar。各协议的握手、状态机、语义（QoS、ACK、游标、会话）差异巨大。若让协议细节直接耦合到消息内核，任一协议的改动都会波及内核，且难以独立交付与测试。

## 决策（Decision）

内核定义**统一消息模型**（`broker_message` / `cloud_event`）与两类 behavior：`broker_router`（destination → `{partition, subscription}` 列表）与 `broker_storage`（`append / read_from / ack / truncate / recover`）。每个协议实现一个独立 **Adapter**，只负责「协议 ↔ 内核」的语义映射，**不得直接依赖其他 Adapter 的内部代码**。

## 后果（Consequences）

- **+** 协议可按 roadmap §7 顺序独立交付、独立测试，互不阻塞。
- **+** 内核稳定后，S2（单机内核）与 S3（CloudEvents 层）可并行推进。
- **-** 各 Adapter 需重复一部分映射逻辑；需约定统一的错误模型与元数据映射契约。

## 验证（Validation）

由 S1 MQTT 垂直切片验证「三个核心抽象先定死并冻结」（roadmap Gate S1）。后续每个协议在 S5 交付独立测试套件。
