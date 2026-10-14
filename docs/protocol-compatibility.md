# 协议兼容矩阵（Protocol Compatibility）

- **文档版本**：v0.1（S0.4 基线，S1 起持续更新）
- **对应 Roadmap**：§7 S5 协议扩展、§9.2 文档产出物
- **对应 README**：11.1 协议兼容矩阵（验证维度）
- **S5 协议顺序**：MQTT → WebSocket → NATS → AMQP → Kafka → Pulsar

> 本文件是 S5 每个协议兼容性测试的基线。S0.4 仅确定「用什么客户端 / 版本 / 语言」并验证其可获取（见 `spikes/s0_4_clients/REPORT.md`）。协议能力、端到端语义、CloudEvents Binding 由 S5 逐协议填充「支持状态」列。

---

## 0. 客户端与版本矩阵（基线）

| 协议 | S5 顺序 | 推荐客户端 | 语言 | 基线版本 | 验证 | 备注 |
|---|---|---|---|---|---|---|
| MQTT | 1 | Eclipse Paho / mosquitto_pub-sub | Python / CLI | paho-mqtt 2.x | ✅ | S1 已建；QoS 0/1/2、Retain、Will、5.0 在 S2 补 |
| WebSocket | 2 | `ws`（Broker 自有 JSON/二进制帧） | Node | ws 8.x | ✅ | 传输层用 ws，应用层帧由 S5 设计 |
| NATS | 3 | `nats.go` / `nats.js` | Go / JS | nats.go v1.37.0 | ✅ | Pub/Sub、Queue Group、Request/Reply |
| AMQP | 4 | Qpid Proton / amqp091 | Python / Java | proton (qpid) | ✅ | Proton=AMQP 1.0；需 0.9.1 时换 amqp091/pika |
| Kafka | 5 | `kafka-clients` / `librdkafka` | Java / C | kafka-clients 3.7.1 | ✅ | Produce/Fetch/Metadata/Consumer Group |
| Pulsar | 6 | **官方 Pulsar Java Client** | Java | pulsar-client 3.3.1 | ✅ | README 5.1 硬性要求官方 Java Client |

---

## 1. 每个协议须覆盖的验证维度（README 11.1）

S5 为每个协议交付「一份独立测试套件 + 能力矩阵的一行」。测试须覆盖：

1. **连接**：建立、断连恢复、连接中多命令交错
2. **认证**：认证失败路径；ACL 决策仅使用认证主体，未被信任的事件属性影响（README 10.1）
3. **语义与 ACK**：发布 / 订阅 / 消费语义与 ACK 契约一致
4. **CloudEvents Binding**：正反向映射测试；跨协议往返测试；转换损失项有明确清单（roadmap §7）
5. **流控与异常**：协议级流控、异常帧处理
6. **能力标记**：已支持 / 部分支持 / 计划支持

---

## 2. 能力矩阵模板（S5 填充）

每个协议在 S5 末尾在此处追加一行，格式：

```
| <协议> | <已支持/部分支持/计划支持> | <支持的能力> | <已知限制/转换损失> | <测试套件路径> |
```

示例（S5 完成后替换）：

```
| MQTT   | 已支持 | QoS0/1、Retain、Will、5.0 元数据 | — | test/protocol-compatibility/mqtt/ |
| Pulsar | 部分支持 | P1 连通、P2 消费语义 | P3 可靠、P4 扩展待补 | test/protocol-compatibility/pulsar/ |
```

---

## 3. 已知约束

- **不实现自编客户端互测**：S5 Gate 要求「通过真实第三方客户端集成测试，无自编客户端互测」。
- **Pulsar 版本兼容**：`pulsar-client` 版本须与待接入 Pulsar 集群版本兼容；S5 前复查最新稳定版。
- **AMQP 版本分支**：Qpid Proton 为 AMQP 1.0；若目标 Broker 为 RabbitMQ（AMQP 0.9.1），改用 `amqp091` / `pika`。
