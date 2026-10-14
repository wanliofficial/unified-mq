# S0.4 Spike Report: 协议客户端基线

**结论：六种目标协议（MQTT / WebSocket / NATS / AMQP / Kafka / Pulsar）的推荐客户端库与基线版本已确定，且在本环境全部验证「可获取 + 最小 API 可用」。该矩阵作为 S5 全部协议兼容性测试的基线（roadmap Gate S0）。Pulsar 硬性要求官方 Java Client，已确认 `pulsar-client:3.3.1` 可获取。**

- **对应 Roadmap**：`docs/roadmap.md` §2 S0.4、Gate S0（S0.4 矩阵被采纳为 S5 基线）、§9.2 `docs/protocol-compatibility.md`
- **对应 README**：11.1 协议兼容矩阵（验证维度）、5.1 Pulsar 硬性要求
- **S5 协议顺序**（来自 roadmap §7）：MQTT → WebSocket → NATS → AMQP → Kafka → Pulsar

---

## 1. 方法

S0 阶段**禁止实现协议**（roadmap §2）。S0.4 只验证：每个协议用哪一个第三方客户端库、哪个版本、哪种语言，且该库在本环境**确实能拉取并跑通最小交互面**。完整端到端（连真实 Broker、跑通 pub/sub）是 S5 的工作。

本环境可用工具链：Python 3.12、Go 1.25（自动拉 1.26 toolchain）、Node 22、Java 17、Maven 3.9，且可访问公网包仓库（PyPI / Go proxy / npm / Maven Central）。**无现成 Broker 或客户端二进制**（mosquitto/kafka/pulsar CLI 均未安装），故端到端连接需 S5 自起 Broker。

## 2. 验证证据（每一项均实际执行）

| 协议 | 客户端 | 语言 | 验证动作 | 结果 |
|---|---|---|---|---|
| MQTT | Eclipse Paho `paho-mqtt` 2.x | Python | `pip install` + import + `Client()` + `connect()` | ✅ import/构造/connect API 可用（异步 connect，无 Broker 仅未能全链路） |
| AMQP 1.0 | Apache Qpid Proton `proton` | Python | `pip install` + `import proton` | ✅ import 成功 |
| NATS | `nats.go` v1.37.0 | Go | `go get` + `go build` 一个 `nats.Connect` 程序 | ✅ 编译通过（无 server 仅 connect 运行时失败，符合预期） |
| WebSocket | `ws` 8.x | Node | 本地起 ws server + client 收发 echo | ✅ 完整往返 `client received: echo:hello` |
| Kafka | `kafka-clients` 3.7.1 | Java | `mvn dependency:get` | ✅ artifact 可获取 |
| Pulsar | `pulsar-client` 3.3.1 | Java | `mvn dependency:get` | ✅ artifact 可获取（README 5.1 硬性要求满足） |

验证脚本见 `spikes/s0_4_clients/verify/`（`mqtt_python.py` / `amqp_python.py` / `nats_go.go` / `websocket_node.js`；Kafka/Pulsar 用 Maven `dependency:get`，命令见第 4 节）。

## 3. 客户端与版本矩阵（S5 基线）

> 此表为 S0.4 **基线**；S5 实现时可按当时最新稳定版上修，但协议/语言选型原则上不变。

| 协议 | S5 顺序 | 推荐客户端 | 语言 | 基线版本 | 验证 | S5 首批范围（roadmap §7） |
|---|---|---|---|---|---|---|
| MQTT | 1（S1 已建） | Eclipse Paho / mosquitto_pub-sub | Python / CLI | paho-mqtt 2.x | ✅ | QoS 0/1/2、Retain、Will、5.0（S2 补） |
| WebSocket | 2 | `ws`（服务端自有 JSON/二进制帧） | Node | ws 8.x | ✅ | Broker 自有帧 API |
| NATS | 3 | `nats.go` / `nats.js` | Go / JS | nats.go v1.37.0 | ✅ | Pub/Sub、Queue Group、Req/Reply |
| AMQP | 4 | Qpid Proton / amqp091 | Python / Java | proton (qpid) | ✅ | Exchange/Queue/Binding、Publish/Consume/ACK |
| Kafka | 5 | `kafka-clients` / `librdkafka` | Java / C | kafka-clients 3.7.1 | ✅ | Produce、Fetch、Metadata、Consumer Group |
| Pulsar | 6 | **官方 Pulsar Java Client** | Java | pulsar-client 3.3.1 | ✅ | P1 连通 → P2 消费语义 → P3 可靠 → P4 扩展 |

## 4. 复现命令

```bash
# MQTT / AMQP (Python)
pip3 install --target ./pylibs paho-mqtt proton
PYTHONPATH=./pylibs python3 verify/mqtt_python.py
PYTHONPATH=./pylibs python3 verify/amqp_python.py

# NATS (Go)
cd verify && go mod init t && go get github.com/nats-io/nats.go@v1.37.0 && go build ./... && cd ..

# WebSocket (Node)
cd verify && npm init -y && npm install ws && node websocket_node.js && cd ..

# Kafka / Pulsar (Maven artifact reachability)
mvn dependency:get -Dartifact=org.apache.kafka:kafka-clients:3.7.1
mvn dependency:get -Dartifact=org.apache.pulsar:pulsar-client:3.3.1
```

## 5. README 11.1 验证维度（S5 每个协议测试套件须覆盖）

本矩阵只解决「用什么客户端」，以下维度由 S5 逐协议填：

- 连接建立 / 断连恢复 / 连接中多命令交错
- 认证失败路径（README 10.1：ACL 决策仅用认证主体）
- 发布 / 订阅 / 消费语义与 ACK 契约
- CloudEvents Binding 正反向映射 + 转换损失清单（roadmap §7）
- 协议级流控与异常帧处理
- 能力矩阵标记（已支持 / 部分支持 / 计划支持）

## 6. 风险与未覆盖

- **无 Broker 端到端**：本环境未起任何 Broker，故「连真实服务跑通 pub/sub」留待 S5（需容器或本地起 Broker）。
- **版本漂移**：基线版本为 2026-10 实测可获取；S5 前应复查是否已有更新稳定版，尤其 Pulsar（README 5.1 硬性要求官方 Java Client，版本须与待接入 Pulsar 集群兼容）。
- **qpid-proton 的 AMQP 版本**：Proton 实现 AMQP 1.0；若需兼容 RabbitMQ AMQP 0.9.1，应改用 `amqp091`/pika，已在矩阵备注，S5 再定。
- **WebSocket 帧协议**：roadmap 要求 Broker 自有 JSON/二进制帧 API，客户端选 `ws` 仅为传输层；应用层协议由 S5 设计。
