# 协议兼容矩阵（Protocol Compatibility）

- **文档版本**：v1.3
- **最近一次追加**：2026-11-13——新增 §4.20：WebRTC 信令前端（S10-A，只有连接层；CT 45 例，**无真实第三方客户端证据**）+ §2 能力矩阵新增一行。此前一次：2026-11-11——新增 §4.12：MQTT over QUIC（S8-D，EMQX `emqtt` 1.16.1 真实客户端 8/8 + CT 17 例）。再此前：2026-11-10——§4.11 MQTT-SN 1.2（S8-C，`mqtt-sn-tools` 0.0.7 真实客户端 10/10 + CT 84 例）；再此前：§4.13 MQTT over WebSocket（S8-B，paho-mqtt 2.1.0 `transport="websockets"` 8/8 + CT 19 例）。§4/§4.1~§4.10 原文保留为当时事实
- **对应 Roadmap**：§7 S5 协议扩展、§9 S7 身份统一与事件绑定、§10 S8 三条 MQTT 传输（MQTT-SN / MQTT over QUIC / MQTT over WebSocket）、§11 S9 三协议 agent 前端、§12 S10 WebRTC 信令面与 ICE 面
- **对应 README**：11.1 协议兼容矩阵（验证维度）
- **S5 协议顺序**：MQTT → WebSocket → NATS → AMQP → Kafka → Pulsar

> **用例数口径**：本文件的表格里，标「S5 时点」「S3 时点」等字样的是**当时快照**，标「**N 例（当前）**」的是最近一次从 `all/0`（含 `groups/0` 展开）逐套件清点的当前值。**当前全量基线（S10-A 收尾，2026-11-13）是 `rebar3 ct` 1108/1108、71 个套件**（`apps/*/test/` 70 个 + `test/integration/s1_e2e_SUITE` 1 个）。**下一段「口径更正（2026-11-11）」里的 823/823 与 52 套件是 2026-11-11 的历史快照，保留原文不改**（S9 与 S10-A 之后已到 1108/71；逐阶段增量见 `progress.md` 顶部说明）。
> **口径更正（2026-11-11）**：S8-A/B/C 三个快照此前分别写作 711/731/815，**各自多算 9 例，套件数是对的**。多出来的 9 例正好等于 `ws_frame_SUITE` 的用例数：S8-A 把它迁去 `broker_ws` 之后，`_build/test/lib/broker_proto_ws/test/` 里还留着一份旧的 `.beam`，被 CT 一并拾走（残留已删除；此后全量一直是 823/823、52 套件）。按各阶段提交重新逐套件清点（`all/0` 含 `groups/0`，且与 HEAD 的 CT 日志逐套件核对一致）：673 → 702（+15 `ws_handshake_SUITE`、+4 `mqtt_tls_SUITE`、+10 `mqtt_transport_tcp_SUITE`）→ 722（+19 `mqtt_ws_conn_SUITE`、+1 `ws_conn_SUITE`）→ 806（+40/+21/+23 三个 SN 套件）→ 823（+13 `mqtt_quic_transport_SUITE`、+4 config 组）。**QUIC 的真实 QUIC 用例只在 `rebar3 as quic ct` 下展开**（默认 profile 无 quicer，该组不出现），所以默认全量里这个 app 只算 config 组（`mqtt_quic_listener_SUITE` 4 + `mqtt_quic_transport_SUITE` 13 = 17 例）；`rebar3 as quic ct` 下变成 10 + 13 = 23 例（listener 多出 6 例真实 QUIC），全量 823 − 17 + 23 = 829 即 **827 passed + 2 skipped**（`mqtt_quic_transport_SUITE` 的两个「quicer 缺失」用例在 quicer 在场时按设计 skip），零 FAILED。

> 本文件是 S5 每个协议兼容性测试的基线。S0.4 仅确定「用什么客户端 / 版本 / 语言」并验证其可获取（见 `spikes/s0_4_clients/REPORT.md`）。协议能力、端到端语义、CloudEvents Binding 由 S5 逐协议填充「支持状态」列。

---

## 0. 客户端与版本矩阵（基线）

| 协议 | S5 顺序 | 推荐客户端 | 语言 | 基线版本 | 验证 | 备注 |
|---|---|---|---|---|---|---|
| MQTT | 1 | Eclipse Paho / mosquitto_pub-sub | Python / CLI | paho-mqtt 2.x / mosquitto 2.1.2 | ✅（**T15 + S2-C**：10/10 用例） | QoS0/1/2、Retain、Will 已实现并实测；MQTT 5.0 语义在 S2.5 补；逐条限制见本文 §4 |
| WebSocket | 2 | Python `websocket-client`（应用层帧为 Broker 自有 JSON/二进制） | Python | websocket-client 1.7.0 | ✅（**S5-WS：10/10 用例**） | 传输层用 `websocket-client`，应用层帧由 S5 设计，见 §4.5 |
| NATS | 3 | `nats.go` / `nats.js` | Go / JS | nats.go v1.48.0 | ✅（**S5-NATS：11/11 用例**） | core NATS：Pub/Sub、Queue Group、Request/Reply、wildcard、auth、HPUB/HMSG headers；无 JetStream，见 §4.6 |
| AMQP | 4 | Qpid Proton / amqp091 | Python / Java / Go | amqp091-go v1.10.0 | ✅（**S5-AMQP：13/13 用例**） | AMQP 0-9-1 子集：direct/fanout/topic、queue 生命周期、publish/consume/ack/nack；见 §4.7 |
| Kafka | 5 | `kafka-clients` / `librdkafka` | Java / C | kafka-clients 3.7.1 | ✅（**S5-Kafka：12/12 用例**） | Produce/Fetch/Metadata/ListOffsets、经典消费组、OffsetCommit/Fetch、RecordBatch v2；单节点协调器、offset 内存态、无压缩/事务/Admin API，见 §4.8 |
| Pulsar | 6 | **官方 Pulsar Java Client** | Java | pulsar-client 3.3.1 | ✅（**S5-Pulsar：8/8 用例**） | 官方 Java Client 实测：produce/consume、properties、key、batch、ack/redelivery、seek、unsubscribe、producer name；无 BookKeeper/事务/压缩/Key_Shared，见 §4.9 |
| MQTT over WebSocket | S8-B | `paho-mqtt`（`transport="websockets"`） | Python | paho-mqtt 2.1.0 | ✅（**S8-B：8/8 用例** +1 raw framing 探针单列） | 同一 `mqtt_conn_fsm`，RFC 6455 binary 帧；子协议 `mqtt`/`mqttv3.1`/`mqttv3.1.1` 硬门槛，见 §4.13 |
| MQTT-SN 1.2 | S8-C | `mqtt-sn-tools`（`mqtt-sn-pub`/`mqtt-sn-sub`） | C（CLI） | 0.0.7（pinned `a1dd870`） | ✅（**S8-C：10/10 用例**） | UDP 网关，不复用 `mqtt_conn_fsm`；工具无 QoS 2 / 无 Will / `PINGREQ` 不带 client id，见 §4.11 |
| MQTT over QUIC | S8-D | EMQX `emqtt`（`emqtt_quic`，ALPN `mqtt`） | Erlang | emqtt 1.16.1 + quicer 0.4.12 | ✅（**S8-D：8/8 用例**） | CN Draft 3 单流模式；`emqtt` 的 QUIC 是 EMQX 实验特性，无流级关闭 / 无 0-RTT / 无多流，见 §4.12 |

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

## 2. 能力矩阵（S5 完成态；S8-B 追加 MQTT over WebSocket 行，S8-C 追加 MQTT-SN 行，S8-D 追加 MQTT over QUIC 行，S9-D 追加 MCP 行，S9-F 追加 A2A 行，S10-A 追加 WebRTC 信令行）

每个协议一行，逐条能力与限制以对应 §4.x 为准。

| 协议 | 状态 | 支持的能力 | 已知限制 / 转换损失 | 证据路径 |
|---|---|---|---|---|
| MQTT | 已支持 | QoS0/1/2、Retain、Will、clean session/persistent session、离线追赶、MQTT 5.0 子集（session expiry、message expiry、reason codes、properties、`$share/{group}/{filter}` Shared 订阅） | 无 topic alias、无 AUTH 扩展认证、`subscription identifier` 不透传到投递帧；QoS2 跨崩溃仍为至少一次；见 §4/§4.1/§4.2 | `apps/broker_proto_mqtt/test/`、`test/protocol-compatibility/mqtt/` |
| WebSocket | 已支持 | RFC 6455 握手/掩码/分片/控制帧；自有应用帧 `connect`/`subscribe`/`publish`/`ack`/`unsubscribe`/`ping`/`disconnect` | v1 仅 QoS0/1；`shared` 订阅显式拒绝；应用层帧为 Broker 自有格式；见 §4.5 | `apps/broker_proto_ws/test/`、`test/protocol-compatibility/ws/` |
| MQTT over WebSocket | 已支持（S8-B） | RFC 6455 binary 帧承载标准 MQTT（3.1.1 / 5.0 与 1883 明文完全同一份 FSM）：QoS0/1/2、Retain、Will、持久会话、离线追赶、`mqtt`/`mqttv3.1`/`mqttv3.1.1` 子协议协商、任意 URL path、wss | 无 `permessage-deflate`；text 帧 → close 1003、超长帧 → close 1002；缺失/未知子协议 → HTTP 400；背压按帧（`arm_once` no-op）；见 §4.13 | `apps/broker_proto_mqtt_ws/test/`、`test/protocol-compatibility/mqtt_ws/` |
| MQTT-SN | 已支持（S8-C） | MQTT-SN 1.2 over UDP（默认 1884）：全部 27 种报文、QoS −1/0/1/2、Retain、Will（CONNECT 标记 → WILLTOPICREQ/WILLTOPIC/WILLMSGREQ/WILLMSG，含 `WILLTOPICUPD`/`WILLMSGUPD`）、三种 TopicIdType（预定义 / 每客户端 normal 双向 REGISTER / 两字符 short）、休眠与 `PINGREQ(client_id)` 唤醒、SEARCHGW/ADVERTISE 网关发现、keepalive 1.5×/2× | 无 DTLS（明文 UDP）；QoS 0 在休眠期间丢弃；无串口桥 / 转发器（Forwarder Encapsulation）不实现；CONNECT 无凭据字段，`require_auth` 配 `mqtt_sn` 时拒绝启动（见 §4.11、`docs/security.md` §2.2） | `apps/broker_proto_mqtt_sn/test/`、`test/protocol-compatibility/mqtt_sn/` |
| MQTT over QUIC | 已支持（S8-D） | MQTT over QUIC（OASIS TC《Single Stream Mode》CN Draft 3）over UDP（默认 14567）：同一份 `mqtt_conn_fsm`，因此 QoS0/1/2、Retain、Will、持久会话、MQTT 5.0 properties 与 1883 完全一致；ALPN 固定 `mqtt`；一条客户端发起的双向流承载整条 MQTT 连接；流关闭 ≠ 连接关闭（同连接新流 = 新会话）；强制 TLS 1.3 | 无 0-RTT early data；无多流（EMQX 私有扩展）；无运行期流信用补充（`max_streams_per_conn` 建连时给足）；默认不监听（无证书即拒绝启动）；只有 `rebar3 as quic` 构建才带 NIF；见 §4.12 | `apps/broker_proto_mqtt_quic/test/`、`test/protocol-compatibility/mqtt_quic/` |
| NATS | 已支持 | core NATS：PUB/SUB/UNSUB、subject wildcard（`*`/`>`）、Queue Group、Request/Reply、INFO/CONNECT/PING/PONG、auth、HPUB/HMSG headers（`ce-*`） | 无 JetStream、无集群路由；见 §4.6 | `apps/broker_proto_nats/test/`、`test/protocol-compatibility/nats/` |
| AMQP | 支持子集 | AMQP 0-9-1：direct/fanout/topic exchange、queue declare/bind/consume、publish、ack/nack、basic properties | 无事务、无 publisher confirm 全档、无流控 tuning、无 AMQP 1.0；见 §4.7 | `apps/broker_proto_amqp/test/`、`test/protocol-compatibility/amqp/` |
| Kafka | 支持子集 | Produce 8 / Fetch 11 / ListOffsets 5 / Metadata 8 / OffsetCommit 7 / OffsetFetch 5 / FindCoordinator 2 / JoinGroup 5 / Heartbeat 3 / LeaveGroup 3 / SyncGroup 3 / InitProducerId 0；RecordBatch v2（无压缩）；经典消费组 | 单节点协调器、offset 内存态、无压缩/事务/幂等/Admin API；见 §4.8 | `apps/broker_proto_kafka/test/`、`test/protocol-compatibility/kafka/` |
| Pulsar | 支持子集 | 官方 Java Client 路径：Connect、Lookup、PartitionedMetadata、Producer、Send（单条+batch）、Subscribe、Flow、Ack、Seek、Unsubscribe、Ping、GetLastMessageId；properties/key/event_time/sequence_id；CONNECT 凭据（`token` / SASL PLAIN，走统一身份表） | 无 BookKeeper/ledger（ADR-006）、无事务/压缩/Key_Shared/schema；TLS 由 S6 统一覆盖；订阅游标内存态；未配 `require_auth` 时匿名连接按部署默认租户运行、全部写入分区 0；见 §4.9 | `apps/broker_proto_pulsar/test/`、`test/protocol-compatibility/pulsar/` |
| A2A（1.0.0 JSON-RPC + SSE） | 支持子集（S9-A~S9-C，**Broker 作网关**） | JSON-RPC 2.0 传输绑定：`SendMessage`（含 `task_auto_complete` 内建 echo 执行器）、`GetTask`、`ListTasks`、`message/send` 别名；Task 状态机（SUBMITTED→WORKING→COMPLETED / FAILED / CANCELED / REJECTED / INPUT_REQUIRED）+ 事件溯源 + boot 全量重放（无 checkpoint）；Agent Card 发现（`/.well-known/agent-card.json`、`/agents/<id>`，按 tenant 隔离）；SSE 任务事件流（ack-first + 增量 + 优雅关流）；出站 Push Notification（HMAC-SHA256 签名 + SSRF 防护 + 防重放 + DLQ） | 单节点（跨节点 Task 访问无重定向/代理）；全量重放是可扩展性上界；无 JWT/JWKS/OAuth AS（`securitySchemes` 只声明外部 scheme，card 无签名，push 密钥在 env 非 KMS）；失败路径计数器未加（S9-F 定稿不加，见 `docs/security.md` §12.1 第 17 条）；`/agents` 是项目私有接口；默认 `listeners => []`；见 §4.14/§4.15/§4.16/§4.19 | `apps/broker_proto_a2a/test/`、`test/protocol-compatibility/a2a/`（S9-F 官方 SDK 证据在 `test/protocol-compatibility/s9f/a2a/`） |
| MCP（Streamable HTTP + stdio） | 支持子集（S9-D，**Broker 作网关**） | MCP `2026-07-28`：无状态逐请求（无 `initialize`/会话/`Mcp-Session-Id`）；`server/discover` 本地回答；`subscriptions/listen` 长活流（ack-first + 通知 + 心跳 + 优雅关流）；`tools`/`resources`/`prompts`/`completions` 原语**透传**到 `$mcp/<method>` 并取回结果（含 `notifications/progress`）；POST 单端点 `application/json`/`text/event-stream`；stdio 前端（newline 成帧、子进程订阅、`notifications/cancelled`）；HTTP 头门禁 `Origin`/`Accept`/`MCP-Protocol-Version`/`Mcp-Method`/`Mcp-Name` | 不解释/不执行工具语义（R-B1）、无 `sampling`/`elicitation`（-32601）、无 GET 流/`Last-Event-ID`/会话续传、无 `Mcp-Param-*` 强制校验、SSE 无 pending-bytes 上限、stdio 不做认证（进程即凭据）且 stdout 纯净性由部署 logger 配置保证、跨节点 reply 不支持；见 §4.17 | `apps/broker_proto_mcp/test/`、`test/protocol-compatibility/mcp/`（S9-F 官方 SDK 证据在 `test/protocol-compatibility/s9f/mcp/`） |
| ACP（v1，stdio） | 支持子集（S9-E，**Broker 作 Client**） | ACP v1：Broker 作 **Client**、agent 是本地子进程；`initialize`/`session/new`/`session/prompt` 由 Broker 主动发起；`session/request_permission` 是唯一实现的 Client 回调（默认拒绝 + allowlist + 超时 + 每次审计）；`session/cancel` 转发；`session/update` 折叠为 turn 投影写 `$acp/session/<id>`（含 `stopReason`）；prompt 经 `$acp/in/<agent>` 异步回读；`protocolVersion` 只接受 1 | **仅 stdio**（无 HTTP/WS listener、无 `listeners`）、无远程建连、无 `session/load`/`resume`/`list`/`authenticate`、`fs/*`/`terminal/*`/`elicitation/*` 全回 `-32601`（保持会话）、`clientCapabilities={}`、stdio 不做认证（进程即凭据）、跨节点 reply 不支持；见 §4.18 | `apps/broker_proto_acp/test/`、`test/protocol-compatibility/acp/`（S9-F 官方 SDK 证据在 `test/protocol-compatibility/s9f/acp/`） |
| WebRTC 信令（WSS） | **连接层已支持**（S10-A；信令语义未实现） | RFC 6455 over TLS（wss）/明文仅回环；子协议 `webrtc.v1` 强制；认证窗口 `auth_timeout_ms`（超时 close **1008**）；空闲窗口 `idle_timeout_ms`（超时 close **1001**，`infinity` 可关）；帧上限 `max_frame_bytes`（超限 close **1009**）；文本 `ping`→`pong` 与原生 opcode 9→10 并存；binary → close **1003**（`binary_frames => ignore` 可改为忽略）；认证 = JSON `{"event":"authenticate","data":{username,password}}` → `broker_security:authenticate_conn/1`（与其余六协议同一张 `users` 表）；`peer_id` 服务端铸造 | **本阶段只有连接层**：无房间、无 `to` 转发（带 `to` 一律回 `invalid_message`）、无 `joined`/`left`、不做 ACL；`peer_id` 仅本次连接有效；无真实第三方客户端证据、无压测；默认 `listeners => []`；见 §4.20 与 `docs/security.md` §12.2 | `apps/broker_proto_webrtc/test/`（CT 45 例，字节级客户端；S10-D 才有 `aiortc` 第三方证据） |

**S9-F（2026-11-12）真实第三方客户端验证**：三个 agent 协议各用**上游官方 SDK** 跑过端到端证据——
MCP `mcp 2.3.0`（§4.17.9）、ACP `agent-client-protocol 1.0.0`（§4.18.8）、A2A `a2a-sdk 1.3.0`
（§4.19），原始输出与跑法在 [`test/protocol-compatibility/s9f/`](../test/protocol-compatibility/s9f/)。
上表「证据路径」仍是各 app 的 CT 套件；官方 SDK 证据是**额外的、非 CT 的**客户端矩阵，用来回答
「真实第三方客户端能不能跑通」，同时把协议规范偏差（例如 §4.18.8 的 ACP `outcome` 嵌套缺陷）暴露出来。
容量见 [`benchmarks/2026-11-12-s9f-capacity.md`](benchmarks/2026-11-12-s9f-capacity.md)。

---

## 3. 已知约束

- **不实现自编客户端互测**：S5 Gate 要求「通过真实第三方客户端集成测试，无自编客户端互测」。
- **Pulsar 版本兼容**：`pulsar-client` 版本须与待接入 Pulsar 集群版本兼容；S5 前复查最新稳定版。
- **AMQP 版本分支**：Qpid Proton 为 AMQP 1.0；若目标 Broker 为 AMQP 0.9.1，改用 `amqp091` / `pika`。

---

## 4. MQTT 行：S1 预填 + S2-C 追加（非 S5 终态）

> 这一行是 **S1 期间的实现事实记录**，不是 S5 的兼容性结论。T15 已把真实客户端兼容性落成 `test/protocol-compatibility/mqtt/`（paho-mqtt 2.x 宿主 + mosquitto 2.1.2 容器 + 透明 TCP tee + 生产 codec 的逐帧字节对比），本节末尾的 T15 块是其证据。其余证据来自 `apps/broker_proto_mqtt/test/{mqtt_frame,mqtt_conn,mqtt_listener}_SUITE.erl`（真实 TCP + 真实 `mqtt_conn_fsm`；S1 时共 54 例，S2-C 后 81 例，见 §4.1）、`apps/broker_security/test/broker_security_SUITE.erl`（7 例）与 `test/integration/s1_e2e_SUITE.erl`（T13：同一组端到端用例跑 `wal`/`memory`，12 例）。设计条款溯源见 [`design/s1-mqtt-vertical-slice.md`](design/s1-mqtt-vertical-slice.md) §4.1/§4.5 批注与 §13 L1~L20、M1~M16。

> **T13 追加（2026-11-06）**：`test/integration/s1_e2e_SUITE` 把 T12 的 listener 与 §8 的端到端断言接起来：`wal` 组验证 QoS1 的 PUBACK→append→cursor/committed 全链路、非 clean 重连恢复订阅集合、三条离线消息按 offset 序追赶；`memory` 组在同一份用例清单下断言 E12（SUBACK `0x80`、超限 PUBLISH 断连且零 append）与 QoS0 无积压，**不用 skip**。这是 Gate「`broker_storage` 的 WAL 与内存实现可互换」的套件级证据；`kill -9` 级 R1a/R1b/R2~R7 归 T14（已交付，见下条）。

> **T14 追加（2026-11-06）**：崩溃矩阵已在**真实容器**里交付——release（`rebar3 as t14 release`）跑在 `erlang:28.3.1.0`（宿主同版本 OTP 28.3.1 / ERTS 16.2），数据目录 bind-mount 到宿主 ext4（`wal_writer:check_filesystem` 的真实条件），`docker kill -s KILL` 注入崩溃，paho-mqtt 2.x 客户端跑在宿主。**默认档（K=64 / 100 ms）与零回退档（K=1 / 0 ms）各 8/8 PASS、零 FAIL**。与协议对端直接相关的结论：R1a 收过 PUBACK 的消息在 kill 后从 WAL 完整回放；R1b 订阅**集合**在崩溃后免重发 SUBSCRIBE 仍生效（`session_present=1`）；R2 未 ack 的并发发布只可能"全丢或成无空洞前缀"，默认档恢复 31/200、零档 29/200，全部为 `msg-0000..` 前缀；R3 默认档 cursor 退到 33（两次 33/34，重放 18/17 条、零丢失），零回退档**确定地**停在 51=N+1（重放 0、重复 0）——**重复窗口与 `cursor_max_regress` 参数语义一致**；R4 撕裂尾 3196→3198→3196、20 条全回（零档 3198→3200→3198，断言是回到撕裂前大小）；R5 QoS0 零存储零投递；R6 20k 条恢复 208.836/204.706 ms < 500 ms；R7 已投递未 ack 的消息重连后重发、无丢失。偏差 M13（宿主驱动）/M14（R6 只计 `recover/2`）/M15（compose 不可用改 CLI）不影响上述协议行为。

> **T15 追加（2026-11-06）**：真实客户端兼容性套件在 `test/protocol-compatibility/mqtt/` 交付（用法与用例映射见该目录 `README.md`）。拓扑是 T14 的 release（`erlang:28.3.1.0` 容器，端口 `18831`）+ 宿主 `127.0.0.1:18832` 的透明 TCP tee；tee 把双向字节落盘，`t15_capture.erl` 用**生产 `mqtt_frame`** 逐帧 `decode→encode` 并与真实字节比较。**8/8 用例 PASS、46/46 帧 `roundtrip=ok`**（c2s + s2c）。逐项证据：
> - `paho311-full`：CONNECT（clean、keepalive=1、guest/guest）、SUBSCRIBE `t15/paho/#` qos1、QoS1 PUBLISH、**双向 PUBACK**（客户端 ack 投递的 `40020001`，broker ack 发布的 `40020002`）、PINGREQ `c000`/PINGRESP `d000`、DISCONNECT `e000`；CONNACK `20020000`、SUBACK `9003000101`，全部字节一致。
> - `mosq-pubsub`：mosquitto 2.1.2 QoS1 往返，SUBSCRIBE/PUBLISH/PUBACK 字节与 paho 同类帧同构；`mosquitto_sub`/`pub` 必须显式 `-i`（空 ClientID 按 §4.2 拒绝）。
> - `paho-v5-refused` / `mosq-v5-refused`：v5 CONNECT ⇒ CONNACK **`2003008700`**（v5 帧型、零属性、reason `0x87`），paho 与 mosquitto 都读出 **135 = 0x87**（"Not authorized"）后关闭。**M16**：原先的 3.1.1 帧型 `20020087` 会让 paho 在属性解码器抛 `IndexError`、mosquitto 报 network protocol error，拒绝原因根本到不了用户；现已修正（3.1.1 其它响应字节不变）。
> - `paho-qos2-refused`：QoS2 PUBLISH（固定头 `34`）在 parse 期被拒（`bad_publish_qos`），无 PUBREC/PUBCOMP、零存储零投递、连接关闭。
> - `paho-retain`：入站 retain=1（`33`）被接受并投递为 retain=0（`32`），payload 原样。
> - `paho-will-ignored`：Will CONNECT 的 flags=`ce`，异常断连后订阅端**零投递**。
> - `paho-bad-filter`：含 NUL 的 filter（`t15/bad/\x00`）通过 paho 本地校验，broker 在 codec 拒绝（`embedded_null`），**无 SUBACK**、直接断连。
>
> 预期之外没有发现新的协议偏差；M16 是唯一的产品代码改动（仅拒绝帧的帧型）。

> **T12 追加（2026-11-06）**：ranch listener 已上线，装配后的 release 节点用 **paho-mqtt 2.x 真实客户端**跑通 CONNECT/CONNACK、SUBSCRIBE/SUBACK、QoS1 PUBLISH→PUBACK、QoS0 与投递（`RELEASE-PAHO-OK`）。这一步立即暴露出一个**真实 CONNECT flags 位移缺陷**（§13 M3：clean session 位被解成 Will，paho 一连接即被拒），说明"手推黄金向量 + round-trip"只能证明自洽、不能证明合规；T15 的价值因此从"首个真实客户端证据"变成"补齐 mosquitto 与更多包型（含 `0x87` 与非法 filter）的字节级对比"。

| 协议 | 状态 | 已支持 | 已知限制 / 转换损失 | 证据 |
|---|---|---|---|---|
| MQTT 3.1.1 | **QoS0/1/2 + Retain + Will**（S1 垂直切片 + S2-C 补齐） | CONNECT/CONNACK、PUBLISH QoS0+QoS1+**QoS2**、PUBACK、**PUBREC/PUBREL/PUBCOMP**、SUBSCRIBE/SUBACK、UNSUBSCRIBE/UNSUBACK、PINGREQ/PINGRESP、DISCONNECT；topic filter 通配符 `+`/`#`（Annex A 校验）；`clean_session` 两半；`keep_alive`（服务端 clamp）；per-session 订阅集合 + cursor + in-flight 跨连接持久化（R1b/R3/R7）；**retained 树持久化 + 订阅补发 + 零长度清除**；**Will 异常发布**；`mqtt_max_qos` 按后端 capability 派生（E2/E12）；`kill -9` 后恢复行为符合 R1a/R1b/R2~R8（T14，默认档/零回退档双档）；真实 paho 2.x + mosquitto 2.1.2 全链路与逐帧字节一致（T15，10/10） | 见下方 7 条（第 1、5 条已按 S2-C 修订，第 2 条按 S2.5 修订） | `apps/broker_proto_mqtt/test/`、`apps/broker_retain/test/`、`apps/broker_security/test/`、`test/integration/s1_e2e_SUITE.erl`、`test/protocol-compatibility/mqtt/`、`tools/docker/s1_recovery.py` |
| MQTT 5.0（子集） | **S2.5 已实现**（ADR-017） | CONNECT properties（session expiry、receive maximum、maximum packet size、user properties、will delay 存储）、PUBLISH properties（payload format、message expiry、content type、response topic、correlation data、user properties）、SUBSCRIBE subscription identifier、v5 CONNACK/PUBACK/PUBREC/PUBREL/PUBCOMP/SUBACK/UNSUBACK/DISCONNECT reason codes、session expiry 落盘与 boot 清理、`$share/{group}/{filter}` 复用 Shared 投递、CONNECT `Maximum Packet Size` 出站丢弃 | 不实现 topic alias、request/response、AUTH 扩展认证、server-side 属性回填；QoS2 仍**不是 exactly-once**（跨崩溃至少一次）；`$share` 只做语法与建组，不实现 v5 的 subscription identifier 透传到投递帧 | `apps/broker_proto_mqtt/test/mqtt_frame_SUITE.erl`（31 例）、`mqtt_conn_SUITE.erl`（v5 用例）、`docs/adr/017-mqtt5-properties-and-compat.md` |

**逐条限制（协议对端可观察的行为，不是内部 TODO）**：

1. ~~QoS2 / Retain / Will 一律不实现~~ → **S2-C 已实现**（详见 §4.1）。仍然非静默的只有**超限**：超过 `mqtt_max_qos` 的 PUBLISH（含 `mqtt_max_qos=1` 时的 QoS2）⇒ **直接断连、不发任何 ack**；SUBSCRIBE 里任一 topic 请求的 QoS 超限 ⇒ SUBACK 对应返回 `0x80` 且不建订阅（其余 topic 不受影响）；QoS2 状态超过 `max_qos2_state_per_session` ⇒ 断连并计 `qos2_state_overflow`；retain 树超过上限 ⇒ 不写入但消息正常转发（计数 `retain_overflow`）。
2. **MQTT 5.0 已接受（S2.5）**：`Protocol Level = 5` 的 CONNECT 正常解析并回 5 字节 v5 CONNACK（`20 03 00 00 00` 形状）。只有 **>= 5 且本实现不认识的级别**才拒绝，回 **`20 03 00 84 00`**（v5 CONNACK：flags=0、reason=`0x84` Unsupported Protocol Version、零属性）；3.1.1 及更旧的级别仍回 3.1.1 帧型 `20 02 00 01`。**v5 客户端的所有拒绝都必须用 v5 帧型**：空 ClientID 回 `20 03 00 85 00`（Client Identifier not valid），认证失败回 `20 03 00 86 00`。旧 T15 的 `paho-v5-refused`/`mosq-v5-refused` 断言（`0x87`）已作废，用例改为 `paho-v5-connect` / `mosq-v5-pubsub`（真实 paho/mosquitto 均连接成功）。
3. **非法 topic filter 的 SUBSCRIBE 是断连，不是 `0x80`**（§13 L13）：filter 在帧解码阶段就校验（Annex A / §4.7 形状），因此整个 SUBSCRIBE 包属于畸形包 ⇒ 关连接、**无 SUBACK**。`0x80` 只覆盖"请求 QoS 超过有效上限"。客户端若指望逐 topic 部分成功，需要自己保证 filter 合法。
4. **离线追赶没有优先级、没有独立队列**（设计 §5.2 E7）：重连后从 cursor 顺序读，追赶期间新到的消息与积压消息**按 offset 混在同一序列**投递；积压极深时首条消息延迟随深度线性增长（单连接顺序读）。段文件在 S1 不删，所以"理论全量可追"，但追赶深度上限要等 S2 的 retention 策略才有明确契约。
5. **at-least-once 的两个形状**（§13 L8 + §5.3 + ADR-018 §18.3）：(a) 一条连接对同一个未确认的消息**只投一次**，没有重试定时器——重发只发生在**重连**（blob 恢复项按 **同一 packet_id + DUP=1** 投一次；QoS2 已 `PUBREC` 的那些改发 `PUBREL`）；(b) `.inflight` 默认攒批刷盘（`inflight_flush_interval_ms`，默认 100 ms），崩溃后 blob 回退可能让**不同消息复用同一 packet_id**（按 id 去重的客户端会误判）。`inflight_flush_interval_ms = 0` 是已实现的零回退档；T14 已按默认档 / 零回退档各跑一轮，两档都不丢消息（R7/R8），默认档允许重发、零回退档把回退窗口压到最小。
6. **QoS2 不是 exactly-once**（ADR-012 §12.1）：线协议的四步握手与跨重连不重复成立；**跨崩溃允许重复**（至少一次 + DUP）。客户端不得依赖"broker 崩溃后消息只到一次"来去重，必须自己按业务键幂等。
7. **没有背压，只有停投**（§13 L14）：per-session in-flight 窗（`max_inflight`，默认 64）满了之后本轮不再投新 QoS1，等 PUBACK 或轮询 tick；不通知发布者、不丢消息、不断连，也**没有慢消费者隔离**（归 S2）。另有一条方向安全的副作用：QoS0 订阅离线时没有连接替它 skip，其分区 `committed` 停在最早未读消息（偏低＝多留数据，绝不 truncate、不丢）。

**认证面**：`broker_security` 是配置驱动桩——`authenticate/2` 失败措辞恒为 `bad_username_or_password`（不区分"用户不存在"与"密码错"），`authorize/2` 一律 `allow`，`tenant_id/1` 从用户表取（ADR-009 的 `tenant_id` 必填）；**匿名 CONNECT（无 username 且无 password）被允许，principal = client_id**（§13 L10）。空 client_id 拒绝（`0x02` Identifier rejected），因为 S1 的会话表以它为键。


### 4.1 S2-C 追加（2026-11-07）：QoS2 / Retain / Will 已实现

> S1 的「拒绝策略」是当时的**事实记录**，已被本节取代。三条能力现在都是实现，而不是拒绝；证据是帧级 CT 用例 + 真实客户端用例 + 崩溃恢复用例三层。

**QoS2（ADR-012 §12.3 + ADR-018 §18.3）**

- 帧层：`PUBREC(5)` / `PUBREL(6)` / `PUBCOMP(7)` 双向编解码，固定头 flags 按 §3.6/§3.7 校验（PUBREL 是 `0b0010`，另两个 `0b0000`）；PUBLISH 与 Will 的 QoS 放宽到 0..2。
- 出站：投递 `PUBLISH qos=2` → 收 `PUBREC` 只回 `PUBREL`、**cursor 不动** → 收 `PUBCOMP` 才推进 cursor。半途收到错误的 `PUBACK`（peer bug）不推进 cursor、不断连。
- 入站（Method A）：`PUBLISH qos=2` 先投递再回 `PUBREC`，`received` 只做去重；同一 packet id 的重发只回 `PUBREC`、不再投递第二次；收 `PUBREL` 回 `PUBCOMP`。
- 跨重连：`.inflight` 形状 v2 带 `released`/`received`（见 `storage-and-recovery.md` §9.1）；未 `PUBREC` ⇒ 同 packet id `DUP=1` 原级别重发，已 `PUBREC` 未 `PUBCOMP` ⇒ **只发 `PUBREL`**，不重发 PUBLISH。
- **承诺边界**：线协议四步 + 跨重连不重复；**跨崩溃允许重复**（至少一次 + DUP）。不做 exactly-once，任何文档与对外表述不得暗示 otherwise。
- 幂等（ADR-018 §18.3 R1/R2）：未知 `PUBREL` ⇒ 回 `PUBCOMP`；未知 `PUBREC` ⇒ 回 `PUBREL`；未知 `PUBCOMP` ⇒ 忽略。三者都不断连、不推进 cursor、记 `info` 日志。
- 有界状态：`released ∪ received ≤ max_qos2_state_per_session`（默认 100），超限**断连**并计 `qos2_state_overflow`——静默丢 QoS2 状态等于骗客户端。

**Retain（ADR-018 §18.2）**

- 独立 app `broker_retain`：`put/2`（`sync`｜`async`）、`delete/2`、`lookup/1`、`entries/0`、`stats/0`、`flush/0`；持久化到 `<data_dir>/retain/retain.v1`（整树 + `vsn`，损坏/未知版本即启动失败）。
- 订阅时补发：SUBACK 之后按 filter 匹配 `entries/0`，逐条 `retain=1` 发送，投递 QoS = `min(订阅 QoS, 存储 QoS)`；通配符匹配多个 topic 时按字典序，重叠 filter 去重。
- 转发语义：现有订阅者收到的转发帧 `retain=0`（§3.3.1.3：retain 描述存储状态，不是转发标记）。
- 零长度 payload **清除**该 topic：新订阅者收不到；在线订阅者仍收到这条空消息（不吞）。
- 刷盘时机（O6 已签认）：**QoS1/QoS2 的 `retain=1` PUBLISH 先 fsync 落盘再回 PUBACK/PUBREC**；QoS0 走延迟批量。
- 上限（`max_retained` / `max_retained_bytes` / `max_payload_bytes`）：**拒绝写入但正常转发**，计数 `retain_overflow`；不驱逐旧值（否则"最后一个 retained 值"变成随机）。

**Will（ADR-018 §18.1，O5 已签认）**

- CONNECT 保存 topic/payload/qos/retain；**仅内存，不持久化**——broker 重启不补发（与 mosquitto 默认一致）。
- 发布时机：TCP 异常关闭、keepalive 超时、协议错误、内部失败、takeover 都会发布；**正常 DISCONNECT 与 broker shutdown 不发布**。
- 发布路径与普通 PUBLISH 一致：QoS>0 走 append（fsync）后 dispatch，QoS0 直接 dispatch；`retain=1` 同步更新 retained 树。
- 全程 `catch`：queue 已停时失败只记日志（不能因为遗嘱发布失败而掩盖真正的停机原因）。

**证据**

| 层 | 用例 |
|---|---|
| 帧（`mqtt_frame_SUITE`） | 27 例（S2-C 时点；S2.5 扩 v5 属性/原因码后共 31 例），含 PUBREC/PUBREL/PUBCOMP 双向、QoS2 PUBLISH/Will、reserved flags |
| 连接（`mqtt_conn_SUITE`） | 50 例（S2-C 时点；S2.5/S3/S8-A 追加后共 70 例），其中 QoS2/Retain/Will 共 20 例：四步双向、DUP 去重、重连重发两半、未知帧幂等、状态超限、补发/清除/持久化/上限、Will 四种触发 + 重启不补发 |
| retain app（`broker_retain_SUITE`） | 17 例：可见性、覆盖、清除、排序、同步/异步落盘、三个上限、损坏文件 |
| 崩溃（T14 R8） | QoS2 收 `PUBREC` 后 `kill -9`；重启后客户端重发 `PUBREL`，broker 幂等回 `PUBCOMP`；消息至少投递一次、cursor 最终推进。**默认档与零回退档都 PASS** |
| 真实客户端（T15） | `paho-qos2`（双向四步 + 字节一致）、`paho-retain`（补发 retain=1 / 转发 retain=0 / 空 payload 清除）、`paho-will`（异常断连发布）、`paho-will-skipped`（DISCONNECT 抑制）、`mosq-qos2`（mosquitto 2.1.2 双向） |

---

### 4.2 S2.5 追加（2026-11-08）：MQTT 5.0 子集

> 范围冻结在 [ADR-017](adr/017-mqtt5-properties-and-compat.md) §17.1；未实现的项（topic alias、request-response、AUTH）不在本子集，**S5 也未纳入，仍未实现**（列为 S6 之后的可选项）。

**帧层（`mqtt_frame`）**

- `decode/2` / `encode/2` 接受 `4 | 5`；`decode/1` / `encode/1` 保持 3.1.1 语义，3.1.1 路径从不读取 properties 长度。
- v5 CONNECT/PUBLISH/SUBSCRIBE/UNSUBSCRIBE/PUBACK/PUBREC/PUBREL/PUBCOMP/SUBACK/UNSUBACK/DISCONNECT 解析与编码；CONNACK 带属性长度。
- properties：变长整数长度前缀；未知 id 与重复单值 id **拒绝**；`user_properties` 累积保序。
- 3.1.1 出站字节不变：`encode(F)` 与 `encode(F, 4)` 逐字节相等（`mqtt_frame_SUITE:v5_311_outbound_bytes_unchanged`）。

**连接层（`mqtt_conn_fsm`）**

- CONNECT properties：`session_expiry_interval`、`receive_maximum`、`maximum_packet_size`、`user_properties`；level 5 的出站帧全部走 `encode/2,5`。
- `receive_maximum` 收窄出站窗口：`inflight_limit = min(max_inflight, ReceiveMaximum)`；3.1.1 的默认 65535 使该 min 无效。
- `maximum_packet_size`（服务端配置 `mqtt_max_packet_size`，默认 0=关闭）：入站整帧超限 **断开且不 ack**（`mqtt_packet_too_large`）；出站 PUBLISH 超过客户端上限 **丢弃并计数**（`mqtt_publish_dropped_oversize`），重投递路径仍持有该消息。
- properties → `proto_meta`（§5.1.1）；`message_expiry_interval` 与 `message_ttl_ms` 取更早的绝对期限，出站按剩余秒数回填。
- reason code：v5 PUBACK 无匹配订阅回 `0x10`；CONNACK 拒绝按 3.1.1 码映射（1→`0x84`、2→`0x85`、4→`0x86`）。
- session expiry：`Persist = session_expiry > 0`（3.1.1 仍由 `clean_session` 决定）；断开时把 `session_expires_at` 写入 `.inflight` blob；boot 扫描过期会话并按 clean-session 顺序拆除（disk → Router → memory）。
- `$share/{group}/{filter}`：非空 group、无通配符、合法真实 filter；`join(shared, ...)` 建组，UNSUBSCRIBE 走 `leave`；SUBACK 返回请求 QoS。

**证据**

| 层 | 用例 |
|---|---|
| 帧（`mqtt_frame_SUITE`） | 31 例，含 v5 CONNECT/PUBLISH 属性往返、未知 property 拒绝、level 5 接受、3.1.1 字节不变 |
| 连接（`mqtt_conn_SUITE`） | v5 CONNACK 形状、级别 6 的 `0x84`、properties 送达订阅端、PUBACK `0x10`、`$share` 单成员投递、`receive_maximum=1` 窗口 |
| 全量 | `rebar3 ct` **397/397**；`dialyzer` 0 warnings；`xref` 无新增未使用导出 |

---

### 4.3 S3 追加（2026-11-08）：CloudEvents 1.0 标准化层

> 范围与冻结决策见 [ADR-020](adr/020-cloudevents-codec-policy-binding.md)；执行计划与交付记录见 [`design/s3-cloudevents-execution-plan.md`](design/s3-cloudevents-execution-plan.md)。默认策略仍是 `pass_through`，**MQTT 3.1.1 线上字节不变**；本节只描述启用后新增的能力与限制。

**规范范围**

- 只实现 **CloudEvents 1.0 JSON Format**；Avro / Protobuf 不在 S3。`specversion` 只接受 `"1.0"`，其他值按 `unsupported_specversion` 拒绝。
- 运行时事件视图是 `#broker_message.event` 的 `map()`（ADR-009 的槽位不变）；`#cloud_event{}` 记录仅保留为 README §3.1 接口说明。
- 校验项：必需属性 `specversion/id/source/type` 非空且为 UTF-8；可选属性类型；`time` 为 RFC3339；`data` 与 `data_base64` 互斥；`data_base64` 合法 base64；扩展名 `[a-z0-9]{1,20}`、值限 string/boolean/integer/float、单事件 ≤ 32 个。

**检测与策略（按租户 + 目的地，首个匹配）**

| 策略 | 不是 CloudEvents | 是但非法 |
|---|---|---|
| `disabled` / `pass_through` | 恒等，不解析 | 恒等，不解析 |
| `validate_if_present` | 恒等 | 拒绝，`{error, {event_invalid, Why}}` |
| `required` | 拒绝，`{error, {event_invalid, event_required}}` | 拒绝 |
| `normalize` | 拒绝 | 合法则写 `event`；payload 字节不改写 |

检测顺序：MQTT 5 `user_properties` 含 `ce-specversion` → Binary Mode；`content_type = application/cloudevents+json` 或 payload 是含 `specversion` 的 JSON 对象 → Structured Mode；否则不是 CloudEvents。

**MQTT Binding 与转换损失（README §4.1：不得静默丢弃）**

| 方向 | MQTT 3.1.1 | MQTT 5.0 |
|---|---|---|
| Structured（事件 JSON） | 支持（payload 自描述；无 `content-type` 元数据） | 支持，并写 `content_type = application/cloudevents+json` |
| Binary（`ce-*` 上下文 + 原始 data） | **不可表达**：返回损失报告，列出全部 `ce-*` 属性（含必需四项），data 字节仍传输 | 支持：上下文写 `user_properties` 的 `ce-*`，`datacontenttype` 映射到 `content_type` |

`broker_event_binding:to_mqtt/3` 返回 `#{mode, target := mqtt, version, losses, preserved}`；`losses` 非空即表示该次转换非无损，调用方必须能拿到它。已知非无损项：

1. **3.1.1 的 Binary Mode**：所有事件属性都进 `losses`（协议没有元数据通道）。
2. **Structured → Binary 的 `data`**：JSON term 被重新序列化为字节，字节等价但不是原 JSON 语法（例如空白与键序）；报告记 `data`。
3. **非字符串扩展值**：integer/boolean/float 在 Binary Mode 变成文本（`3` → `"3"`）；报告记 `ce-<name>`。
4. **`data_base64`**：Binary Mode 输出解码后的原始字节，报告记 `data_base64`。
5. **协议侧标识**：offset、Pulsar Message ID、MQTT Packet Identifier 只进 `proto_meta`，**永不覆盖** `event.id`（ADR-005）。

**去重与拒绝**

- 去重默认**关闭**；`{dedup, [{Tenant, Destination, WindowMs}]}` 首个匹配，键 `{Tenant, Destination, source, id}`，窗口过期后同一事件视为新事件。命中时只 ACK、不产生第二次存储；QoS2 仍完成四步握手。窗口是进程内有界表，**不承诺跨重启去重**。
- 转换失败**不 ACK**：MQTT 5 QoS1 回 PUBACK `0x83`、QoS2 回 PUBREC `0x83`（连接保持可用）；MQTT 3.1.1 无拒绝帧，关闭连接让 QoS1/2 发布者重试（Will 仍按异常断连发布）。
- 可选隔离副本：`{quarantine, [{Tenant, Destination, TargetPattern}]}` 命中时把**原始消息**以 `fsync` 追加到目标 destination（默认模板 `$invalid/{tenant}/{destination}`），头部写拒绝原因与原目的地；隔离失败不改变拒绝结果。
- ACL 只用认证主体：策略解析与授权都不读取 `event.source` / `type` / 扩展属性，伪造事件属性不能改变租户或授权结果。

**指标**：`cloudevents_validate_ok`、`cloudevents_validate_failed`、`cloudevents_duplicates`、`cloudevents_quarantined`。

**证据**

| 层 | 用例 |
|---|---|
| codec / 策略 / binding（`broker_event_SUITE`） | 31 例（S3 时点；S5/S6 追加头通道往返后共 35 例）：必需属性缺失、类型错误、非法版本/时间/base64、`data`/`data_base64` 互斥、扩展保留与上限、Structured/Binary 正反向、3.1.1 损失清单、跨协议往返、事件 ID 与 Broker ID 分离、策略解析、去重开/关/过期、隔离副本、ACL 不信任事件属性 |
| 连接（`mqtt_conn_SUITE`） | 2 例（S3 新增）：v5 `required` 拒绝回 `0x83` 且连接继续可用；3.1.1 拒绝关闭且不发 ACK |
| 全量 | `rebar3 ct` **422/422**；`dialyzer` 0 warnings；`xref` 76 条均为预期 unused-export（S3 新增 13 条公开 API/测试入口） |

---

### 4.4 S4 追加（2026-11-08）：复制等级、Fencing 与失去多数派

> 决策见 [ADR-021](adr/021-cluster-control-plane-raft-and-fencing.md)；执行计划与证据见 [`design/s4-cluster-execution-plan.md`](design/s4-cluster-execution-plan.md) §5。**MQTT 线上字节不变**；S4 只改变「什么时候可以 ACK」。

**三档 ACK 与故障边界**

| 等级 | ACK 时机 | 崩溃/分区边界 | 何时可用 |
|---|---|---|---|
| `os_buffer` | 写入 OS 缓冲即 ACK | broker 进程崩溃不丢（页缓存仍在），**主机掉电可能丢** | 始终 |
| `fsync` | 本地 WAL fsync 返回后 ACK | 单机掉电不丢；**网络分区下不保证 split-brain 安全** | 始终 |
| `replicated_commit` | 本地 fsync + 复制到 `replicas`，收到 `quorum = (N div 2) + 1` 个成功（含本地）后 ACK | 少数派副本被隔离时不 ACK；旧 Owner 失去 quorum 时写入被拒 | 仅当 `broker_queue` 已注册复制钩子（`broker_cluster` 启动） |

- `replicated_commit` 失败返回 `{error, {replication_failed, Acks, Needed}}`；本地已 fsync 的副本保留（不回滚），但**调用方不得 ACK 发布者**。
- 默认单副本时 `quorum = 1`，语义等价本地 `fsync`；因此默认单节点行为与 S1~S3 完全一致。
- 未启动 `broker_cluster` 时，队列对 `replicated_commit` 仍返回 `{error, {unsupported_sync_mode, replicated_commit}}`，不做静默降级。

**Fencing**

- 归属变化（assign/migrate/finish）必须携带更大 epoch；状态机拒绝不递增的写入。
- 数据面守卫 `broker_cluster:authorize_append(Partition, Epoch)`：epoch 不匹配或本地不是 owner 时返回 `{error, {fenced, #{partition, expected_epoch, actual_epoch}}}`，队列拒绝 append，发布者拿不到 ACK。
- 分区隔离的旧 Owner 无法更新缓存，但它也拿不到复制 quorum，因此 `replicated_commit` 写入会失败；`fsync` 单节点档不承诺跨分区一致（文档明写，见 ADR-021 §后果）。

**失去多数派**

- **失去多数派时的实际行为**：元数据写（assign/migrate/finish/remove）超时（`quorum_timeout_ms`，默认 5000）返回 `{error, no_quorum}`；`replicated_commit` append 的复制 quorum 拿不到多数派时返回 `{error, {replication_failed, Acks, Needed}}`，发布者拿不到 ACK。`os_buffer`（QoS0）路径根本不进入复制器，因此仍可继续——这是路径分离的结果，不是运行时策略开关。
- CONNACK 拒绝不静默降级：MQTT 5 回 `0x83`，MQTT 3.1.1 断开连接。

> **口径（S6 之后）**：ADR-021 §D4 曾计划 `{on_quorum_loss, reject | os_buffer_only}` 运行时开关。该开关**未实现**，已从 `broker_cluster.app.src` 移除（配置项必须有消费者）；当前行为等价于计划中的 `reject` 档，`os_buffer` 的可用性来自它不经过复制器。

**指标**：`cluster_owner_changes`、`cluster_fencing_rejected`、`cluster_replication_failed`、`cluster_replication_latency_us`、`cluster_no_quorum`。

**证据**

| 层 | 用例 |
|---|---|
| 控制面（`broker_meta_SUITE`） | 10 例：11 表、用户哈希、ACL、partition/node、版本、outbox |
| 单节点集群语义（`broker_cluster_SUITE`） | 11 例（S4 时点；S6 追加 `remote_append_normalises_a_successful_append` 后共 12 例）：assign/epoch、Fencing、迁移两阶段、no-quorum、quorum 前后 ACK、副本不足不 ACK |
| 分布式（`broker_cluster_dist_SUITE`） | 4 例：3 节点组形成/复制、Leader 故障切换、失去多数派、迁移后旧 Owner Fencing |
| 全量 | `rebar3 ct` **447/447**；`dialyzer` 0 warnings；`xref` 121 条均为预期 unused-export |

### 4.5 S5 追加（2026-11-09）：WebSocket 适配器（`broker_proto_ws`）

> 决策与冻结契约见 [`adr/012-qos2-semantics-and-release-state.md`](adr/012-qos2-semantics-and-release-state.md)、[`adr/013-subscription-kinds-and-cursor-ownership.md`](adr/013-subscription-kinds-and-cursor-ownership.md) 与 §2 能力矩阵。**WebSocket 复用 MQTT 的存储/路由/订阅路径**——没有协议专属的存储或投递分支；区别只在「线协议」与「QoS 集合」。

**应用层帧格式**（`apps/broker_proto_ws/src/ws_frame.erl` 拥有 RFC 6455 帧编解码，连接层在 `ws_conn.erl`）：

- 传输层是标准 RFC 6455（握手 + `Sec-WebSocket-Accept` + 帧掩码/分片/close/ping/pong），由 `ws_frame` 自实现，不引入 WS 依赖。
- **文本帧** = 一个带 `op` 字段的 JSON 对象；**二进制帧** = `<<Op:8, HeaderLen:32, Header/binary, Payload/binary>>`，`Header` 是同一份 JSON，`Payload` 是原始消息体。
- 操作：`connect` / `subscribe` / `unsubscribe` / `publish` / `ack` / `ping` / `disconnect`。每条消息经 `broker_router` → `broker_queue`（fsync）→ `broker_subscription`，与 MQTT 完全同路。

**能力矩阵行**（追加进 §2 末尾）：

```
| WebSocket | 已支持 | RFC 6455 传输 + connect/connack、subscribe/suback、publish(qos0/1)/puback、message 二进制投递、ack/ackack、unsubscribe/unsuback、ping/pong、clean_start=false 会话 + 离线追赶、同 client_id takeover | v1 仅 QoS0/1（无 QoS2 握手）；v1 不支持 shared 订阅（明确拒绝）；二进制投递帧（op=3）无 MQTT 风格 QoS2；无 retained/will（归 MQTT 侧） | apps/broker_proto_ws/test/、test/protocol-compatibility/ws/ |
```

**已知限制（协议对端可观察，不是内部 TODO）**：

1. **QoS 仅 0/1**：v1 没有 QoS2 四步握手；`publish` 带 `qos=2` ⇒ `error/bad_qos`。
2. **shared 订阅明确拒绝**：`subscribe` 带 `kind=shared` ⇒ `suback` 返回 `granted=false` + reason（不是断连，也不是静默）。
3. **二进制投递帧无 QoS2 语义**：投递帧（op=3）是「消息」投递，其 `qos` 只取订阅侧 0/1。
4. **retained / will 不在 WS 侧**：这两项由 MQTT 侧 `broker_retain` / 连接 FSM 拥有；WS v1 不复制它们。

**真实第三方客户端证据**（Gate S5「真实第三方客户端集成测试，无自编客户端互测」）：

- 套件在 `test/protocol-compatibility/ws/ws_compat.py`，用 **Python `websocket-client` 1.7.0**（S0.4 客户端矩阵指定的 WS 客户端）驱动一个真实的 `erl -noshell -s ws_broker_boot run` broker 节点（ranch 临时端口、`port=>0`、WAL 后端、ext4 数据目录，启动路径与 `ws_conn_SUITE init_per_testcase` 完全一致）。脚本**不 poke `ws_conn` 内部**，只用浏览器/Node `ws` 会用的同一套 WS 握手与帧 API。
- **10/10 用例 PASS**（`run/report.json`）：`ws-handshake`（RFC 6455 握手 + accept 校验）、`ws-connect-connack`（含重复 connect 拒绝）、`ws-pubsub-qos1`（订阅→qos1 发布→二进制 message→ack→ackack，puback 回显 offsets）、`ws-qos0-no-ack`（qos0 投递 `delivery_id=null`、无需 ack）、`ws-binary-frame`（二进制发布帧 body 字节精确）、`ws-offline-catchup`（`clean_start=false` 重连补发离线消息）、`ws-ping-pong`、`ws-unsubscribe-stops`、`ws-refusals`（非法 filter / shared / qos2 / 未知 op / 坏 JSON / 超限 payload 全部显式拒绝，不崩不挂）、`ws-reconnect-takeover`（同 client_id 二次 connect 触发 takeover，旧连接被关、新连接继续收投递）。
- 套件默认从干净数据目录起步（`--keep-data` 可保留），避免上一次运行的持久会话泄漏进 `ws-offline-catchup`。

**联带修复**（WS 压测暴露的真实缺陷，已修）：`broker_subscription_worker` 的 `cursor_tick` 在快速 connect/subscribe/disconnect churn 下会崩——`cursors_body/1` 对已被 tear-down 的 SubRef 做无条件 `badmatch`（`ets:lookup(?ENTITIES, SubRef)` 返回 `[]`）。修法两处：(a) `flush_sub_cursors/2` 先查 `?ENTITIES`，实体不存在则丢弃 `cursor_dirty` 键并跳过写盘；(b) `teardown_session/2` 改为按 SubRef（而非此前错误的 ClientId）清理 `cursor_dirty`。修复后 `broker_subscription_SUITE` 53/53、全量 CT 无回归。

**证据**

| 层 | 用例 |
|---|---|
| WS 帧编解码（`ws_frame_SUITE`） | 9 例：握手 accept、掩码/分片、控制帧、close 载荷 |
| WS 连接/应用协议（`ws_conn_SUITE`） | **23 例（当前）**：S5 时点 14 例（connect/connack、qos1 往返+ack、qos0 无 ack、二进制帧、unsubscribe、clean=false 追赶、clean 丢弃、非法 filter/shared/超限拒绝、ping/pong、未知 delivery ack、明文 HTTP 拒绝）；S6 追加 `cloudevent_headers_roundtrip`（15）；S7 追加凭据解析、错误凭据拒绝、客户端自带租户被忽略、`require_auth` 四例（19）；trace context 追加往返、`off` 时剥离、非法 `traceparent` 不传播三例（22）；S8-B 追加 `wss_is_served_when_tls_is_enabled`（23，`mqtt_ws` 与自有 `ws` 同端口同 TLS 开关的回归用例） |
| 真实第三方客户端（`ws_compat.py`） | 10/10 PASS（见上） |
| 全量 | `rebar3 ct` **470/470**；`dialyzer` 0 warnings；`xref` 仅预期 unused-export |

---

### 4.6 S5 追加（2026-11-09）：NATS 适配器（`broker_proto_nats`）

> **NATS core 复用 MQTT 的存储/路由/订阅路径**——`PUB` 经 `broker_router` → `broker_queue`（fsync）→ `broker_subscription`，`SUB` 是普通订阅行；没有协议专属存储或投递分支。区别只在「文本线协议」与「at-most-once 语义」。

**协议范围（`apps/broker_proto_nats/src/nats_conn.erl` 自实现 core NATS 文本协议）**：

- 服务端先发 `INFO`（`server_id` / `version` / `proto=1` / `max_payload` / `headers=false` / `auth_required`），随后处理 `CONNECT`、`PING`/`PONG`、`PUB`、`SUB`、`UNSUB`，投递 `MSG <subject> <sid> [reply] <#bytes>`。
- subject 映射：NATS 的 `.` → broker 的 `/`；订阅通配符 `*` → `+`，`>` → `#`。`>` 的父级匹配差异（NATS `foo.>` 不匹配 `foo`，broker `foo/#` 匹配 `foo`）作为转换损失显式记录，不隐藏。
- Queue group：`SUB <subject> <queue> <sid>` 映射为 `shared` 订阅（`broker_subscription:join(shared, Queue, ...)` + claim/read/ack），同一 group 每条消息恰好投递给一个成员。
- Request/Reply：`PUB <subject> <reply> <#bytes>` 的 reply subject 存入 `proto_meta`，投递 `MSG` 时原样回显；适配器本身不发明 request/response 语义。
- `echo=false`：CONNECT 里 `echo=false` 的客户端不会收到自己发布的消息（消息仍持久化并投递给其他订阅者）。
- `auth_token`：配置后 `CONNECT` 必须携带匹配的 `auth_token`，否则 `-ERR 'Authorization Violation'`。
- `verbose=true`：`CONNECT` / `SUB` / `PUB` / `UNSUB` 返回 `+OK`（默认 false，测试用 `PING`/`PONG` 同步）。

**能力矩阵行**（追加进 §2 末尾）：

```
| NATS | 已支持 | core NATS：INFO/CONNECT/PING/PONG、PUB/SUB/UNSUB、MSG 投递、`*`/`>` 通配符、Queue Group（shared）、Request/Reply（reply subject 透传）、echo=false、token 认证、UNSUB max 自动退订、HPUB/HMSG（2.2 headers，`ce-*` → 消息头） | 无 JetStream；core NATS 本就 at-most-once，适配器写完 MSG 立即推进自己的 cursor（broker 仍先 fsync 落盘，强于 NATS core）；`foo.>` 的父级匹配与 NATS 有差异；未协商 `headers` 的客户端收 `MSG`，`ce-*` 属性丢失 | apps/broker_proto_nats/test/、test/protocol-compatibility/nats/ |
```

**已知限制（协议对端可观察，不是内部 TODO）**：

1. **无 JetStream**：不支持 Stream/持久化 consumer 语义。headers 支持 NATS 2.2 的 `HPUB`/`HMSG`（`ce-*` 作为 CloudEvents Binary Mode 通道，见 §4.10）；未在 CONNECT 声明 `headers: true` 的客户端收 `MSG`，`ce-*` 属性丢失。
2. **at-most-once**：core NATS 没有 ack 握手。适配器在 MSG 写出后立即 `broker_subscription:ack/4` 自己的 cursor；连接在写出后崩溃可能丢这一条（与 NATS core 一致），但 broker 侧消息已 fsync，可被后续订阅/其他成员读取。
3. **`>` 父级匹配差异**：broker 的 `#` 匹配父级，NATS 的 `>` 不匹配。已作为转换损失记录。
4. **每连接一个匿名 client_id**：NATS 无持久会话/客户端标识，重连即新 `ClientId`；不支持 MQTT 式 session takeover。
5. **租户来自身份表**：CONNECT 的 `user`/`pass` 经 `broker_security:authenticate_conn/1` 解析，租户取匹配到的用户记录（`docs/security.md` §2.1）；未配 `require_auth` 时不带凭据的连接按部署默认租户匿名运行，客户端不能自称任意租户。

**真实第三方客户端证据**（Gate S5「真实第三方客户端集成测试，无自编客户端互测」）：

- 套件在 `test/protocol-compatibility/nats/`，用 **官方 `nats.go` v1.48.0**（S0.4 客户端矩阵指定的 NATS 客户端）驱动一个真实的 `erl -noshell -s nats_broker_boot run` broker 节点（ranch 临时端口、`port=>0`、WAL 后端、ext4 数据目录、token 认证开启，启动路径与 `nats_conn_SUITE init_per_testcase` 一致）。Go 程序只调用 `nats.Connect` / `SubscribeSync` / `QueueSubscribeSync` / `Publish` / `Request` / `Flush`，不手写线协议。
- **11/11 用例 PASS**（`run/report.json`）：`nats-connect-info`（真实 CONNECT + INFO + PING/PONG）、`nats-pubsub`、`nats-wildcard`（`*` 单 token、`>` 余下）、`nats-queue-group`（两个成员、10 条消息各恰好一次）、`nats-request-reply`、`nats-no-history`（新订阅不回放历史）、`nats-unsubscribe`、`nats-auto-unsubscribe`（`AutoUnsubscribe(1)` 只投一条）、`nats-echo-false`、`nats-ping-flush`（20 个并发 Flush）、`nats-auth-failure`（错误 token 被拒）。
- 套件默认从干净数据目录起步，避免上一次运行的 WAL 泄漏进 `nats-no-history`。

**证据**

| 层 | 用例 |
|---|---|
| NATS 线协议/连接（`nats_conn_SUITE`） | **20 例（当前）**：S5 时点 15 例（INFO、CONNECT+PING/PONG、pub/sub、`*` 通配符、queue group 恰好一次、request/reply、不回放历史、unsubscribe、UNSUB max、echo=false、非法 subject、超限 payload、auth 失败、未知操作、verbose `+OK`）；S6 追加 `cloudevent_headers_roundtrip`（16）；S7 追加有效/错误凭据、`require_auth`（19）；trace context 追加 `HMSG` 往返（20） |
| 真实第三方客户端（`nats_compat.py` + `nats.go`） | 11/11 PASS（见上） |
| 全量 | `rebar3 ct` **485/485**（470 + 15）；`dialyzer` 0 warnings；`xref` 仅预期 unused-export |

**实现期修复**（测试暴露的真实缺陷，已修）：

1. **空 queue 被当成 shared**：`SUB subject  sid`（或 queue 字段为空）被解析成 `Queue = <<>>`，`open_subscription/4` 走了 `join(shared, <<>>, ...)` 分支；shared 的 claim/read 直接按分区读，不做 subject 过滤，于是 `compat/+/leaf` 收到了 `compat/one/two/leaf`。修法：`do_sub(Subject, <<>>, Sid, St)` 归一化为 `undefined`（exclusive）。
2. **`send_raw/2` 返回值**：此前返回 `ok`，`handle_line(PING)` 返回 `{noreply, ok}`，下一轮 `parse/2` 对 `ok` 做记录匹配 ⇒ `{badrecord,ok}`。改为返回更新后的 `#st{}`。
3. **`translate_filter` 兜底**：把普通 token 的兜底子句从 `{error, invalid_subject}` 改为继续累积，避免所有非通配 subject 被拒绝。
4. **测试侧丢消息**：`queue_group_delivers_once` 的 `recv_msg_any2` 同时填两个 socket 的缓冲，却只回传被取消息的那个连接，另一个已读字节被丢弃；改为返回两个更新后的连接。这是测试 harness 缺陷，不是适配器缺陷（服务端日志证明 10 条全部投递）。

---

### 4.7 S5 追加（2026-11-09）：AMQP 0-9-1 适配器（`broker_proto_amqp`）

> **AMQP 复用 MQTT 的存储/路由/订阅路径**——`basic.publish` 经 `broker_router` → `broker_queue`（fsync）→ `broker_subscription`；没有协议专属的消息存储。交换/队列/绑定是本适配器维护的 **瞬态拓扑**（ETS，重启不保留），消息体仍由 `broker_queue` 持久化。

**协议范围（`apps/broker_proto_amqp/src/` 自实现 AMQP 0-9-1 子集）**：

- 握手：协议头 `AMQP 0 0 9 1` → `connection.start`（0/9 + server-properties + `PLAIN` + `en_US`）→ `start-ok` → `tune`（channel-max 2047、frame-max 131072、heartbeat 0）→ `tune-ok` → `open` → `open-ok`；`connection.close` → `close-ok`。
- 通道：`channel.open`/`open-ok`、`channel.close`/`close-ok`（服务端错误以 `channel.close` 回复并关闭该通道，不影响连接）。
- 交换机：`exchange.declare`/`delete`，类型 `direct` / `fanout` / `topic`；`headers` 声明接受但不参与路由。
- 队列：`queue.declare`（含服务端生成名 `amq.gen-*`；`passive` 声明不存在队列返回 404）、`bind`/`unbind`、`purge`、`delete`。
- 发布/消费：`basic.publish`（方法 + content header + body 分帧）、`basic.qos`（prefetch 限流）、`basic.consume`/`cancel`、`basic.deliver`、`basic.ack`/`nack`/`reject`（`requeue` 走 `release`，重新投递）、`basic.recover`（回 `recover-ok`）。
- 路由：queue 映射 destination `amqp/<queue>`，消费者用 `shared` 订阅（`join(shared, Queue, ...)` + claim/read/ack），同一队列的多个消费者获得竞争消费语义（每条消息恰好一个成员）。
- 手动 ack：`delivery-tag` → `{Ref, PartitionId, Offset}` 映射；未 ack 的投递在连接断开时 `release` 回池；`nack(requeue=true)`/`reject(requeue=true)` 立即释放并重新投递。

**能力矩阵行**（追加进 §2 末尾）：

```
| AMQP | 部分支持 | AMQP 0-9-1 子集：connection/channel 握手、`start-ok` 的 SASL/PLAIN 凭据（走统一身份表）、direct/fanout/topic exchange、queue declare/bind/unbind/purge/delete、basic.qos/consume/cancel/publish/deliver/ack/nack/reject/recover、prefetch 限流、多通道隔离 | 无 `basic.get`（回 get-empty）、无 `mandatory` 的 `basic.return`、无 headers exchange 路由、拓扑瞬态（重启不保留 exchange/queue/binding）、无 publisher confirm/事务；未配 `require_auth` 时匿名连接按部署默认租户运行 | apps/broker_proto_amqp/test/、test/protocol-compatibility/amqp/ |
```

**已知限制（协议对端可观察，不是内部 TODO）**：

1. **拓扑瞬态**：exchange/queue/binding 存在 `amqp_topo` 的 ETS 中，broker 重启即丢失；消息体本身仍 fsync 到 `broker_queue`。v1 不承诺 durable queue 的元数据恢复。
2. **无 `basic.get`**：同步拉取回 `basic.get-empty`；请使用 `basic.consume`。
3. **无 `basic.return`**：`mandatory=true` 的不可路由消息被丢弃且不回报（不静默改变路由，但也不回 `basic.return`）。
4. **headers exchange 不路由**：可声明，但不会按 headers 匹配投递。
5. **消费者加入前已入队的消息不回放**：共享游标在消费者 join 时锚定到分区尾部；v1 的消费语义针对“消费者已在线时发布的消息”。这与 durable queue 的完整语义有差距，已显式记录。
6. **无 publisher confirm / 事务**：`start-ok` 现在按 SASL/PLAIN 解析 `\0user\0pass` 并交给 `broker_security:authenticate_conn/1`，租户来自匹配到的用户记录（`docs/security.md` §2.1）；空响应的匿名连接按部署默认租户运行，凭据错误回 `ACCESS_REFUSED` 并关闭连接。

**真实第三方客户端证据**（Gate S5「真实第三方客户端集成测试，无自编客户端互测」）：

- 套件在 `test/protocol-compatibility/amqp/`，用 **`github.com/rabbitmq/amqp091-go` v1.10.0**（S0.4 客户端矩阵指定的 AMQP 0-9-1 客户端）驱动真实的 `erl -noshell -s amqp_broker_boot run` broker 节点（ranch 临时端口、`port=>0`、WAL 后端、ext4 数据目录，启动路径与 `amqp_conn_SUITE init_per_testcase` 一致）。Go 程序只调用 `amqp.Dial` / `Channel` / `ExchangeDeclare` / `QueueDeclare` / `QueueBind` / `Consume` / `Publish` / `Ack` / `Nack` / `Qos`，不手写线协议。
- **13/13 用例 PASS**（`run/report.json`）：`amqp-connect`、`amqp-pubsub`、`amqp-direct`（routing key 命中/不命中）、`amqp-fanout`（3 队列全收）、`amqp-topic`（`*` 单字、`#` 余下）、`amqp-competing-consumers`（两消费者 10 条各恰好一次）、`amqp-manual-ack`、`amqp-nack-requeue`、`amqp-unbind`、`amqp-queue-delete`（删除后 passive 失败）、`amqp-passive-missing`（不存在 exchange/queue 触发通道错误）、`amqp-prefetch-qos`（prefetch=1 一次一条）、`amqp-multi-channel`（两通道隔离）。

**证据**

| 层 | 用例 |
|---|---|
| AMQP 帧/字段表（`amqp_codec_SUITE`） | 14 例（S5 时点；S6 追加 `properties_roundtrip_with_ce_headers` 后共 15 例）：帧往返、半帧、错误帧尾、方法、shortstr/longstr、bits LSB-first、重复 bit、字段表、嵌套表、空表、content header |
| AMQP 连接/通道（`amqp_conn_SUITE`） | **17 例（当前）**：S5 时点 14 例（握手+通道、direct publish/consume/ack、routing key 隔离、fanout、topic、unbind、queue delete、passive 缺失、nack requeue、prefetch、多通道、connection.close、heartbeat、坏帧尾断开）；S7 追加有效/错误凭据、`require_auth` 三例 |
| 真实第三方客户端（`amqp_compat.py` + `amqp091-go`） | 13/13 PASS（见上） |

**实现期修复**（测试暴露的真实缺陷，已修）：

1. **握手返回值**：`send_start(rearm(St1))` 把 `{noreply, St}` 当成状态传给 `send_start/1`，导致 `send_frame/4` 收到元组；同时客户端先发的 8 字节协议头被当帧解析。改为先 `rearm` 等待协议头，`parse/2` 识别 `<<"AMQP",0,0,9,1>>` 后再发 `connection.start`。
2. **`reply_ok/5` 裸返回值**：`exchange.declare` 等路径直接返回 `#st{}` 而非 `{noreply, #st{}}`，连接进程以 `bad_return_value` 崩溃；逐个包成 `{noreply, ...}`。
3. **`basic.consume` 的 `badmap`**：一行遗留的 `maps:get(url, ensure_client(St), undefined)` 对 `#st{}` 做 `maps:get`，删除。
4. **正文分帧**：`send_body_frames/3` 对小于 frame-max 的正文仍用 `<<Chunk:Max/binary, Rest/binary>>` 强匹配，短正文崩溃；改为按长度判断。
5. **`connection.close` 解析**：`binary_part` 结果被当成二元组匹配，删除无用解析，直接回 `close-ok`。
6. **共享订阅跨队列串扰**：同一分区里不同 destination 的消息会被另一个 shared group 的游标扫到，导致 `ct.rr.miss` 收到本该只投给 `ct.rr.hit` 的消息、多通道互相串消息。修法：AMQP/NATS 适配器在 claim 后按 `Msg#broker_message.destination` 过滤，非本队列/本 subject 的 offset 直接 ack（跳过），不投递。MQTT 的 shared 路径本就用 `mqtt_filter:matches/2` 过滤，无需改动。
7. **`basic.qos` prefetch 未逐条生效**：一次 claim 最多 32 条，fold 内不检查 prefetch，prefetch=1 仍会一次投多条；改为按 `prefetch - 未 ack 投递数` 作为 claim 上限。

---

### 4.8 S5 追加（2026-11-09）：Kafka 适配器（`broker_proto_kafka`）

> **Kafka 复用 MQTT 的存储/路由路径**——Produce 经 `broker_queue:append/3` 写入分区日志（fsync），同一 destination 的其它协议订阅者由 `broker_subscription` 唤醒；没有 Kafka 专属存储。反过来，MQTT/AMQP/NATS 发到 `kafka/<topic>` 的消息也能被 Kafka 消费者读到。

**协议范围（`apps/broker_proto_kafka/src/` 自实现 Kafka 线协议子集）**：

- 每个 API 只广告**一个精确的非 flexible 版本**（`ApiVersions` 除外）：Produce 8、Fetch 11、ListOffsets 5、Metadata 8、OffsetCommit 7、**OffsetFetch 5**、FindCoordinator 2、JoinGroup 5、Heartbeat 3、LeaveGroup 3、SyncGroup 3、InitProducerId 0、ApiVersions 0–3。
- **topic 模型**：Kafka topic `t` 映射 destination `kafka/t`；Kafka 分区数 = `broker_queue` 分区数（`partitions` 配置）。Produce(p) 追加到 broker 分区 p，Fetch(p) 读 broker 分区 p 并按 destination 过滤。Metadata 对任意 topic 自动回全量分区（`auto.create.topics` 无需真实建 topic）。
- **offset 语义**：broker 分区被多个 topic 共享，因此 Kafka offset 是「该 topic 在该分区内的记录序号」（从 0 起）。Produce 返回 `base_offset`，Fetch/ListOffsets 用同一换算；消费组提交的 offset 也是这个值。
- **记录**：RecordBatch v2（magic=2、CRC32C 覆盖 attributes 起至 batch 末尾）、单条/多条 record、key、value、headers、timestamp；未压缩（attributes 压缩位非 0 时回 `unsupported_compression`）。
- **消费组**（`kafka_group`）：FindCoordinator 指向本节点；JoinGroup 用 barrier 完成 rebalance（rebalance_timeout 兜底），SyncGroup 下发 leader 的分配，Heartbeat 维持成员，LeaveGroup 移除成员；offset 由客户端提交（`OffsetCommit`/`OffsetFetch`），不做服务端自动提交。
- **幂等生产**：InitProducerId 返回固定 producer id/epoch，`acks=all` 的 Produce 正常应答；不做真正的幂等去重（见限制）。

**能力矩阵行**（追加进 §2 末尾）：

```
| Kafka | 部分支持 | Produce/Fetch/Metadata/ListOffsets、经典消费组（FindCoordinator/JoinGroup/SyncGroup/Heartbeat/LeaveGroup）、OffsetCommit/OffsetFetch、RecordBatch v2（key/value/headers/timestamp）、多分区、`SaslHandshake`/`SaslAuthenticate`（SASL/PLAIN，走统一身份表） | 单节点协调器、offset 内存态（重启丢）、无 `__consumer_offsets`、无事务/幂等去重、无压缩、无 Admin API（CreateTopics/DeleteTopics 等）；TLS 由 S6 的 `broker_security_tls` 统一覆盖；未配 `require_auth` 时不带凭据的连接按部署默认租户匿名运行 | apps/broker_proto_kafka/test/、test/protocol-compatibility/kafka/ |
```

**已知限制（协议对端可观察，不是内部 TODO）**：

1. **offset 是扫描换算，O(n)**：每次 Produce/Fetch/ListOffsets 都要数该 topic 在分区内的记录数；v1 以正确性优先，未做 offset 索引。
2. **并发 Produce 未串行化**：同一 topic-partition 的并发生产可能得到重复/交错的 base_offset；单连接顺序生产正确。
3. **消费组 offset 内存态**：`kafka_group` 的 committed offsets 在 broker 重启后清空；无 `__consumer_offsets` 主题，多节点不共享协调器。
4. **无事务/幂等去重**：InitProducerId 返回固定值，`enable.idempotence=true` 的客户端可连接，但重复批次不会被服务端去重。
5. **无压缩**：客户端若配置 `compression.type` 非 none，Produce 解码返回错误；官方客户端默认 none，故 12 例实测通过。
6. **无 Admin API**：topic 自动可见但不可创建/删除。SASL/PLAIN（`SaslHandshake` + `SaslAuthenticate`）已接入 `broker_security:authenticate_conn/1`，凭据与 MQTT 共用同一张身份表（`docs/security.md` §2.1）；TLS 已由 S6 的 `broker_security_tls` 覆盖（六协议统一开关）。未开启 `require_auth` 时，不带凭据的连接按部署默认租户匿名运行。

**真实第三方客户端证据**（Gate S5「真实第三方客户端集成测试，无自编客户端互测」）：

- 套件在 `test/protocol-compatibility/kafka/`，用 **Apache `kafka-clients` 3.7.1**（S0.4 客户端矩阵指定的 Kafka 客户端，Java 17、离线 `~/.m2` 依赖）驱动真实的 `erl -noshell -s kafka_broker_boot run` broker 节点（ranch 临时端口、`port=>0`、WAL 后端、ext4 数据目录）。Java 程序只用 `KafkaProducer` / `KafkaConsumer` / `AdminClient` 的公开 API，不手写线协议。
- **12/12 用例 PASS**（`run/report.json`）：`kafka-metadata`、`kafka-produce-consume`、`kafka-batch-order`（20 条顺序）、`kafka-explicit-partition`、`kafka-key-roundtrip`、`kafka-headers`、`kafka-timestamp`、`kafka-acks-all-idempotent`、`kafka-beginning-end-offsets`、`kafka-consumer-group`（两消费者、6 条各恰好一次、2 分区）、`kafka-offset-commit`（提交 3、重启消费从 c3 继续）、`kafka-seek-replay`（seek 回放 3 条）。

**证据**

| 层 | 用例 |
|---|---|
| Kafka 原语/RecordBatch（`kafka_codec_SUITE`） | 11 例：uvarint/varint/varlong、string/bytes/array、compact 值、CRC32C 已知向量、批次往返、多记录偏移、headers/timestamp、CRC 篡改检测、截断批次、空批次 |
| Kafka 连接/协议（`kafka_conn_SUITE`） | **17 例（当前）**：S5 时点 14 例（ApiVersions v0/v3、Metadata、Produce→Fetch、topic 相对 offset、批次连续 offset、ListOffsets earliest/latest、空 topic、非法分区、FindCoordinator、Join/Sync/Heartbeat、OffsetCommit/Fetch、LeaveGroup、correlation id 回显）；S7 追加 SASL/PLAIN 握手、错误密码拒绝、未知机制拒绝三例 |
| 真实第三方客户端（`kafka_compat.py` + `kafka-clients` 3.7.1） | 12/12 PASS（见上） |

**实现期修复**（测试暴露的真实缺陷，已修）：

1. **RecordBatch CRC 范围**：CRC32C 原先从 `leaderEpoch` 起算，官方客户端按「attributes 起至 batch 末尾」校验；编码/解码两端统一为 attributes 起。
2. **`decode_record_batches` 返回值**：改为 `{ok, Batches, Tail}` 以支持一个 Fetch 响应里的多个批次；空/`undefined` 仍回 `{ok, []}`。
3. **JoinGroup v5 的 `protocolName`**：错误路径（`MEMBER_ID_REQUIRED`）也必须写非 nullable 字符串，否则客户端解析崩溃。
4. **单成员组 barrier 永不完成**：`begin_rebalance` 会把「刚加入的成员」也标成 `joined => false`；改为先启动 rebalance、再把调用者标 `joined => true`，并且先登记 waiter 再判 barrier。
5. **Heartbeat 的 `maps:find` 解构**：`{maps:find(...), Generation}` 里第一元素是 `{ok, Member}`，原模式 `{ok, M}` 永远不匹配，成员心跳触发 `case_clause`；改为 `{{ok, M}, _}` / `{{ok, _}, G}`。
6. **OffsetFetch 版本**：v7 是 flexible 编码，原实现按非 flexible 解码，客户端请求直接让连接进程 `badmatch`；广告版本降为 v5。
7. **ApiVersions 请求头**：v3 的 header 编码与其它 API 不同，强行解析 body 会崩溃；改为不解析 body，并按请求版本 v0/v1-2/v3 分别编码响应。
8. **Metadata 三元组**：topic 项为 `{Name, Partitions, Error}`，FT 里按二元组匹配导致 `badmatch`。
9. **Fetch 分区项**：v11 在 `high_watermark` 后还有 `last_stable_offset`、`log_start_offset`、aborted transactions、preferred read replica，再才是 records；补读这些字段。
10. **ListOffsets 分区项**：v5 的响应在 `offset` 后还有 `leader_epoch`，漏读会把后续字段错位。

---

### 4.9 S5 追加（2026-11-09）：Pulsar 适配器（`broker_proto_pulsar`）

> **Pulsar 复用 MQTT 的存储/路由路径**——Produce 经 `broker_queue:append/3` 写入分区日志（fsync），订阅游标和消费者在 `pulsar_subscriptions` 中维护；没有 BookKeeper、没有 Pulsar 专属存储（ADR-006）。topic `t` 映射 destination `pulsar/t`，所有写入落在 broker 分区 0，entry id 是 topic 在该分区内的记录序号（与 Kafka 适配器的约定一致）。

**协议范围（`apps/broker_proto_pulsar/src/` 自实现 Pulsar protobuf 线协议子集）**：

- 帧格式：`[total_size:32][command_size:32][BaseCommand][payload?]`；payload 段 `[16#0e01:16][crc32c:32][metadata_size:32][MessageMetadata][payload]`，CRC32C 覆盖 `metadata_size+metadata+payload`（已知向量 `crc32c(<<"123456789">>) = 16#E3069283`）。
- 命令：CONNECT/CONNECTED、PARTITIONED_METADATA(+RESPONSE)、LOOKUP(+RESPONSE)、PRODUCER/PRODUCER_SUCCESS、SEND/SEND_RECEIPT、SUBSCRIBE、FLOW、ACK/ACK_RESPONSE、CLOSE_PRODUCER、CLOSE_CONSUMER、UNSUBSCRIBE、PING/PONG、GET_LAST_MESSAGE_ID(+RESPONSE)、SEEK、SUCCESS、ERROR。
- **协议版本**：CONNECTED 广告 15；`PARTITIONED_METADATA` 回 `partitions=0`（非分区 topic），`LOOKUP` 回 `pulsar://<advertised_host>:<port>`、Connect、authoritative、`proxy_through_service_url=false`（直连 broker，避免代理式反复 lookup）。
- **SEND**：单条与 batch（`num_messages>1` 时 payload 为重复的 `[meta_size][SingleMessageMetadata][payload]`）；`MessageMetadata` 的 properties、partition_key、event_time、sequence_id、publish_time 入 `headers`/`proto_meta`，投递时原样回填。
- **订阅**（`pulsar_subscriptions`）：`{Topic, Subscription}` 拥有 cursor（`scan`/`index`/`next`）、消费者集合、每消费者 flow permits、`unacked` 与 `redeliver`；20 ms sweep 前向读取分区、按 destination 过滤后投递；ack 从 `unacked` 移除；消费者断开时其未 ack 消息进入 `redeliver`，重连后重投（durable 订阅）。
- **InitialPosition**：`earliest` 从 0 开始，`latest` 从当前高水位开始。

**能力矩阵行**（追加进 §2 末尾）：

```
| Pulsar | 部分支持 | 官方 Java Client 的 Connect/Lookup/PartitionedMetadata/Producer/Send（含 batch）/Subscribe/Flow/Ack/Seek/Unsubscribe/Ping/GetLastMessageId；properties、key、event_time、sequence_id；CONNECT `auth_method_name`/`auth_data`（`token` 与 SASL/PLAIN，走统一身份表） | 无 BookKeeper/ledger（ADR-006）；无事务、无压缩、无 Key_Shared hash range、无 read-compacted、无 schema；TLS 由 S6 的 `broker_security_tls` 统一覆盖；订阅游标内存态（重启重置）；未配 `require_auth` 时匿名连接按部署默认租户运行、全部写入分区 0 | apps/broker_proto_pulsar/test/、test/protocol-compatibility/pulsar/ |
```

**已知限制（协议对端可观察，不是内部 TODO）**：

1. **无 BookKeeper/ledger 存储**：消息仍在 `broker_queue`（ADR-006），Pulsar 的 ledger/entry 语义只是 topic 相对序号的映射；`GET_LAST_MESSAGE_ID` 基于当前分区高水位换算。
2. **订阅游标内存态**：`pulsar_subscriptions` 的 cursor/unacked 在 broker 重启后重置；durable 订阅的持久化尚未实现（S6 之后）。
3. **单分区**：所有 topic 都写入 broker 分区 0，`PARTITIONED_METADATA` 回 0；不支持分区 topic 的 per-partition lookup。
4. **无事务/压缩/Key_Shared/schema**：`num_messages>1` 视为 batch；压缩位、事务标记、Key_Shared hash range、schema 均不支持；客户端默认不启用这些特性，故 8 例实测通过。
5. **统一认证已接入 / TLS 已覆盖**：`CONNECT` 的 `auth_method_name`/`auth_data` 现在经 `broker_security:authenticate_conn/1` 校验（`token` 与 SASL/PLAIN 两种承载），租户来自匹配到的用户记录；TLS 由 S6 的 `broker_security_tls` 覆盖。未开启 `require_auth` 时匿名连接按部署默认租户运行，见 `docs/security.md` §2.1。

**真实第三方客户端证据**（Gate S5「真实第三方客户端集成测试，无自编客户端互测」）：

- 套件在 `test/protocol-compatibility/pulsar/`，用 **官方 Apache `pulsar-client` 3.3.1**（S0.4 客户端矩阵指定的 Pulsar 客户端，Java 17、离线 `~/.m2` 依赖）驱动真实的 `erl -noshell -s pulsar_broker_boot run` broker 节点（ranch 临时端口、`port=>0`、WAL 后端、ext4 数据目录）。Java 程序只用 `PulsarClient` / `Producer` / `Consumer` 的公开 API，不手写线协议。
- **8/8 用例 PASS**（`run/report.json`）：`pulsar-produce-consume`、`pulsar-properties`、`pulsar-key`、`pulsar-batch`（3 条批量顺序）、`pulsar-ack-redelivery`（未 ack 关闭后重连重投）、`pulsar-seek`（seek earliest 回放）、`pulsar-unsubscribe`、`pulsar-producer-name`。

**证据**

| 层 | 用例 |
|---|---|
| Pulsar 编解码（`pulsar_codec_SUITE`） | 15 例：CRC32C 已知向量、command_size 前缀、CONNECT/PRODUCER/SUBSCRIBE/SEND/ACK/SEEK 解码、payload 往返、CRC 篡改检测、无 magic 旧格式、batch 负载、MessageMetadata/SingleMessageMetadata 往返、CONNECTED 帧 |
| Pulsar 连接/订阅（`pulsar_conn_SUITE`） | **19 例（当前）**：S5 时点 16 例（connect、partitioned-metadata=0、lookup broker URL、producer success、单发/批量 SEND→receipt、subscribe/flow 顺序投递、ack 后 unacked 清零、ack response 回显 request id、未 ack 关闭后重投、seek 回放、ping/pong、last-message-id、close producer、unsubscribe、未知 producer 报错）；S7 追加 token/SASL-PLAIN 认证、错误 token 拒绝三例 |
| 真实第三方客户端（`pulsar_compat.py` + `pulsar-client` 3.3.1） | 8/8 PASS（见上） |

**实现期修复**（测试暴露的真实缺陷，已修）：

1. **ranch 过滤 `packet` 选项**：`ranch_tcp` 的 `disallowed_listen_options` 含 `packet`，accept 出来的 socket 是 raw；在 `pulsar_conn` 的 handshake 分支对 accepted socket `setopts([{packet,4}])` 后再 arm。
2. **简单命令缺 `command_size` 前缀**：`{packet,4}` 只补外层 `total_size`，Pulsar 还需要 `[command_size][BaseCommand]`；统一由 `frame/2` 加前缀。
3. **`SendReceipt.message_id` 字段号**：在 field 3（field 2 是 sequence id），解码时误读 field 2 导致把整数当嵌套消息；改为按字段位置读取。
4. **`PRODUCER_SUCCESS.schema_version`**：pulsar-client 3.3.1 解码时把 field 4 视为 required，未设置即抛 `IllegalStateException`；现在总是发送空 bytes。
5. **LOOKUP `proxy_through_service_url`**：置 true 会让客户端把返回 URL 当代理并反复 lookup；直连 broker 必须为 false。
6. **record 状态被当 map**：`pulsar_subscriptions` 的 `maps:get(subs, St)` 对 `#st{}` record 调用会 `badmap`；改为 `St#st.subs`。
7. **投递返回嵌套 state**：`{noreply, send(Frame, St)}` 而 `send/2` 已返回 `{noreply, St}`，导致 state 被包成 `{noreply, St}`；改为直接返回 `send(...)`。
8. **投递查找 consumer**：连接侧 consumers 以 `{ConsumerId, RequestId}` 为键，投递处理却用裸 `ConsumerId` 查，永远 miss；改用 `find_consumer/2`。
9. **负 int64 varint**：`MessageId.earliest/latest` 的 `-1` 是 10 字节补码 varint，解码成无符号大数；`message_id_fields/1` 现按 int64 符号扩展。
10. **partition_key 丢失**：投递 metadata 未回填 key；append 时把 `partition_key` 存入 `proto_meta`，投递时写回 `MessageMetadata`。

---

### 4.10 CloudEvents Binding 现状（S5–S7 口径，S8-C 追加 MQTT-SN 行）

ADR-020 冻结了 MQTT 的 Binding 细节（`mqtt_user_properties` 通道）。S6 补齐其余五个协议的 **Binary Mode**，统一采用协议中立的头通道（S8-C 的 MQTT-SN 只能走 Structured，见下表的 S8-C 行）：

> payload = 事件数据字节；上下文写传输协议的元数据字段，键名统一为 `ce-<属性名>`
> （`ce-specversion`、`ce-id`、`ce-source`、`ce-type`、可选属性与 `ce-<扩展名>`），
> `datacontenttype` 写 `content-type`。所有键落在 `#broker_message.headers`，
> 因此同一事件可以不重算映射地在协议之间转发。

| 协议 | Structured Mode（事件 JSON 在消息体） | Binary Mode（上下文 → 协议元数据） | 说明 |
|---|---|---|---|
| MQTT | ✅ 正向 + 反向 + 损失报告 | ✅ 仅 MQTT 5（`ce-*` User Properties）；3.1.1 返回损失清单 | `broker_event_binding`，§4.3。`ce-` 前缀是本 Broker 方言，官方 MQTT 5 Binding 要求属性名**不带前缀**（见下表） |
| WebSocket | ✅ payload 逐字节透传 | ✅ 应用帧顶层 `ce-*` 键 + `content-type`；投递帧原样回传 | `broker_event_binding:to_headers/3` |
| NATS | ✅ payload 逐字节透传 | ✅ `HPUB` 头块 `ce-*`；投递用 `HMSG`（客户端须在 CONNECT 声明 `headers: true`，否则退化为 `MSG` 并丢失属性） | NATS 2.2 headers |
| AMQP | ✅ payload 逐字节透传 | ✅ basic properties 的 `headers` table 放 `ce-*`，`content_type` 放 `content-type`；投递写回 properties | AMQP 0-9-1 |
| Kafka | ✅ payload 逐字节透传 | ✅ Record Headers 放 `ce-*`；投递写回 Record Headers | Kafka 0.11+ record batch |
| Pulsar | ✅ payload 逐字节透传 | ✅ Message properties 放 `ce-*`；投递写回 properties | Pulsar 2.10+ |
| MQTT-SN（S8-C） | ✅ payload 逐字节透传（payload 原样进 `broker_message.payload`） | **❌ 不可表达**：1.2 的 PUBLISH 只有 flags / topicIdType / topicId / msgId / data，没有任何用户元数据槽位，`mqtt_sn_conn` 构造的消息 `headers` 恒为 `#{}` | `mqtt_sn_conn.erl:1087`；与 MQTT 3.1.1 的区别是后者还能返回损失清单，MQTT-SN 连承载损失的字段都没有 |

**统一策略执行**：六个协议在写入前都调用 `broker_event:apply_policy/1`（默认 `pass_through` 为 no-op）。S8-C 的 MQTT-SN 也调用同一个 seam（`mqtt_sn_conn.erl:1027`），因此 `required` / `validate_if_present` 策略对它是生效的——但 MQTT-SN 表达不了 Binary Mode，只能靠 Structured Mode（payload 自描述）判定；拒绝按 MQTT-SN 自己的「无通用错误通道」语义表达：QoS 1 回 PUBACK `NOT_SUPPORTED`，QoS 0/2/−1 静默丢弃（QoS 2 不发 PUBREC 迫使客户端重试）。拒绝路径按协议各自的错误语义表达：MQTT 5 → `0x83`；WS → `{"op":"error","code":"cloudevent_rejected","reason":"…"}`；NATS → `-ERR` 并断开；AMQP → channel error 406；Kafka → `INVALID_RECORD`（87）；Pulsar → SendError（`ERROR_UNKNOWN`）。

**已声明的非无损点**（`report/5` 的 `losses` 必须被调用方读取）：

- Binary Mode 的扩展属性值被序列化为文本；`data` 若是 JSON 对象则变为序列化字节。
- NATS 客户端未声明 `headers` 时，`ce-*` 全部丢失；这是协议能力限制，不是静默丢弃（适配器在 CONNECT 后按 `headers_cap` 决定 MSG/HMSG）。
- AMQP 0-9-1 的 `basic.publish` 没有逐消息否认；策略拒绝表现为 channel error 406，客户端必须重新打开 channel。

「逐字节透传」意味着：一个合法的 Structured CloudEvents JSON 通过这五个协议（加上 MQTT-SN）发布再消费，**字节不变**（因此 `id`/`source`/`type`/扩展都保留）。

> **S7 结案（2026-11-10）**：上面是统一 `ce-*` 头通道的现状，**不是**各协议官方 Binding 的逐字实现。逐协议的官方条文核对结论已写入 `docs/roadmap.md` §9.1（Gate S7 第 3 项已勾选），要点：

| 协议 | 官方规范要求 | 与本节实现的关系 |
|---|---|---|
| MQTT 5 | §3.1.3.1 属性名 **MUST 不加前缀**（`specversion`、`id`…）；`datacontenttype` → `Content Type` | 我们用 `ce-*` 前缀，**非逐字** |
| MQTT 3.1.1 | §1.3 内容模式**恒为 structured**，无 binary mode | 我们的 structured 合规；binary 是额外扩展（有 `losses` 报告） |
| WebSocket | §1.3 恒 structured + §1.5 `Sec-WebSocket-Protocol: cloudevents.json` 子协议协商 | 我们的 WS 是并列的自有协议，**未实现**官方绑定 |
| NATS | §3.1.3.1 前缀 `ce-`；§3.1.3.2 值 **MUST** percent-encode | 键名一致，**缺 percent-encoding** |
| Kafka | §3.2.3.1 前缀 **`ce_`（下划线）** | 我们用 `ce-`，**非逐字** |
| AMQP | 只有 **AMQP 1.0** 规范（`cloudEvents_`/`cloudEvents:` 前缀，application-properties） | 我们是 0-9-1 + `headers` table，**规范不覆盖** |
| Pulsar | 官方 bindings 目录**无此文件** | 无官方 Binding |

结论：统一 `ce-*` 头通道**是本 Broker 的项目定义**，六个协议共享它以保证跨协议零重映射转发；逐字实现各协议官方绑定**不立项**（与统一通道互斥，且会破坏既有真实客户端套件）。上表的差异是显式声明的，不是待办遗漏。本节的等价映射与非无损点继续有效，不因结案而改写。

> **W3C trace context（README §10.2）**：`traceparent` / `tracestate` 与 CloudEvents 属性共用同一张 `headers` map，但键名独立（无 `ce-` 前缀），由 `broker_event_trace` 在 `broker_event:apply_policy/2` 上统一校验与门控。WS/NATS 的白名单头通道只在开启传播时放行这两个键；AMQP/Kafka/Pulsar 全量透传 headers，也由同一 seam 在 `off` 时剥离；MQTT 5 走 ADR-020 的 User Property 通道，`mqtt_conn_fsm` 入站把这两个键提到 `headers`、投递时回填，同样受 `off` 门控（3.1.1 无此通道）。安全边界见 `docs/security.md` §5.1。

---

### 4.11 S8-C 追加（2026-11-10）：MQTT-SN 1.2（`broker_proto_mqtt_sn`）

**是什么**：MQTT-SN 1.2（MQTT for Sensor Networks，UDP 上的 MQTT）网关。默认 **UDP 1884**（MQTT 1883 的邻居），CT 与兼容性套件都用 `port => 0` 让内核分配。实现的是**完整 1.2 语义**，不是「MQTT 的 UDP 转发壳」：27 种报文、QoS −1/0/1/2、Will、Retain、三种 TopicIdType、休眠与唤醒、网关发现。

**为什么不是一个 UDP socket 一个进程**：UDP 没有连接，一个网关只有**一个** socket 服务所有客户端。`gen_udp` 用 `{active, 100}` 保证 reader 可读且邮箱有界，会话按 `{IP, Port}` 索引、`client_id` 在 CONNECT 时绑定；会话回包走同一个 socket 句柄（UDP 会话没有自己的 socket 可发）。同 `client_id` 从新端点重连 = takeover，且**不发布 Will**（客户端没死，它搬家了）。

**为什么不复用 `mqtt_conn_fsm`**：FSM 的整个连接生命周期（半开、按 socket 接管、broker 重启后按持久会话恢复）在 UDP 上不适用——这里没有「连接」可丢；入站解码也不是字节流而是数据报。复用只会把两套生命周期缠在一起。会话只复用**下层**：`broker_subscription` / `broker_queue` / `broker_router` / `broker_retain`，投递走与其它协议相同的 `{broker_deliver, ClientId, MaxQoS, Refs, {P, O, Msg}}`。

| 能力 | 状态 | 说明 |
|---|---|---|
| 报文集 | 已支持 | 27 种全实现：ADVERTISE/SEARCHGW/GWINFO、CONNECT/CONNACK、WILLTOPICREQ/WILLTOPIC/WILLMSGREQ/WILLMSG、REGISTER/REGACK、PUBLISH/PUBACK/PUBREC/PUBREL/PUBCOMP、SUBSCRIBE/SUBACK、UNSUBSCRIBE/UNSUBACK、PINGREQ/PINGRESP、DISCONNECT、WILLTOPICUPD/WILLTOPICRESP/WILLMSGUPD/WILLMSGRESP |
| 两种长度头 | 已支持 | 首字节 `<256` 即总长（含头）；否则首字节 `1` + 2 字节大端总长。三字节头要求 `Len >= 4`、单字节头要求 `Len >= 2` |
| QoS −1 | 已支持 | 线上是两位 `3`；不进会话状态、不产生 ACK、不重放。**无会话也受理**：只有 predefined/short topic 可用，在 listener 进程里 live push，不落盘 |
| QoS 0 | 已支持 | 不走日志（与 MQTT 一致），休眠期间**丢弃**（能力矩阵如实声明，不是缺陷） |
| QoS 1 / 2 | 已支持 | 与 MQTT 同序：`append(fsync)` → `retain(sync)` → ACK → dispatch（ADR-018 §18.2）；QoS2 走 PUBREC/PUBREL/PUBCOMP。下行未 ACK 窗口 `max_inflight`（默认 20），满则暂不发，下一次 PINGREQ 从 cursor 重试 |
| Retain | 已支持 | SUBACK 之后重放；retained 存储是 `broker_retain`，与其它协议共享 |
| Will | 已支持 | CONNECT 的 Will 位置位 → WILLTOPICREQ/WILLTOPIC/WILLMSGREQ/WILLMSG；`WILLTOPICUPD`/`WILLMSGUPD` 更新。异常死亡才发布，takeover 与 clean DISCONNECT 不发布 |
| TopicIdType | 已支持 | **predefined**（`predefined_topics` 配置一次、全局只读、启动即校验 0/0xFFFE/0xFFFF）；**normal**（每客户端独立、双向 REGISTER：SUBACK 分配一个给订阅方，客户端 PUBLISH 前先自己 REGISTER，网关下行遇到客户端未知 topic 时先发 REGISTER 再 PUBLISH）；**short**（两字符名字**就是** id，不分配；非恰好 2 字节直接拒绝，不存成客户端永远叫不出名字的 filter） |
| topic id 上限 | 已支持 | `max_topic_ids_per_client`（默认 1024）；上行超限回错误码，**下行超限即断开会话**——网关自己编不出合法 PUBLISH，不能假装成功 |
| 休眠 / 唤醒 | 已支持 | DISCONNECT(duration) = detach：保留订阅与 cursor，不新建缓冲；`PINGREQ(client_id)` 从**新端点**唤醒并追赶积压（端点 rebind）。休眠登记上限 `max_asleep_clients`，超限按正常断开并计数（`mqtt_sn_sleep_denied`），不静默丢弃 |
| keepalive | 已支持 | 每会话两个 timer：1.5× 静默发 PINGREQ 探测，2× 未响应判死并 detach（`mqtt_sn_keepalive_timeout`）。**没有逐报文重传 timer**：MQTT-SN 只有客户端的 DUP 位是诚实的重传信号 |
| 网关发现 | 已支持 | SEARCHGW 恒回 GWINFO；`advertise_interval_ms > 0` 时按间隔广播 ADVERTISE 到 `advertise_address`:`advertise_port`（默认 `{255,255,255,255}`:1884——**目的地是客户端监听端口，不是本监听器端口**） |
| 认证边界 | 显式失败 | MQTT-SN 1.2 的 CONNECT **没有用户名/密码字段**。`broker_security:auth_required/1` 对 `mqtt_sn` 恒 `false`（不在 `credential_capable/0`），这对认证是对的、对配置是错的：部署若要求所有 listener 出示凭据，必须被告知这个 listener 做不到。`broker_proto_mqtt_sn_app` 因此直接读 `require_auth` 原始列表，命中 `mqtt_sn` 就**拒绝启动监听器**并返回 `{require_auth_unsupported, mqtt_sn}`，不静默忽略。见 `docs/security.md` §2.2 |
| 指标 | 已支持 | 新增三个封闭集 key：`mqtt_sn_sleep_denied`、`mqtt_sn_keepalive_timeout`（counter）、`mqtt_sn_asleep`（gauge，由 listener 维护 delta，会话死亡也归零） |

**证据**：

| 类型 | 位置 | 规模 |
|---|---|---|
| CT 套件（字节级自测） | `apps/broker_proto_mqtt_sn/test/mqtt_sn_codec_SUITE.erl` | **40 例**：27 种报文往返、两种长度头边界、保留位/保留类型号/截断/尾随字节的拒绝路径、QoS −1 的两位 `3`、PUBLISH 短 topic 的 2 字节 binary |
| CT 套件（topic id 表） | `apps/broker_proto_mqtt_sn/test/mqtt_sn_topics_SUITE.erl` | **21 例**：predefined 校验与保留 id、normal 双向分配与耗尽、short 的两字符约束 |
| CT 套件（真实 UDP） | `apps/broker_proto_mqtt_sn/test/mqtt_sn_conn_SUITE.erl` | **23 例**：CONNECT/CONNACK、clean session teardown、三种 topic id、QoS −1/0/1/2 双向、UNSUBSCRIBE、retained 重放、Will vs. takeover、休眠 + 新端点唤醒 + 积压、keepalive 判死、SEARCHGW/ADVERTISE、坏数据报丢弃、`require_auth` 拒绝启动 |
| 真实第三方客户端 | `test/protocol-compatibility/mqtt_sn/`（`mqtt-sn-tools` **0.0.7**，pinned `a1dd870`） | **10/10** 第三方用例：connect、QoS0、QoS1、client REGISTER、**gateway 下行 REGISTER**（wildcard 订阅 + 先 REGISTER 再 PUBLISH）、short topic、predefined topic、retain、sleep disconnect、QoS −1 无会话发布 |

**该套件抓到的真实缺陷**：首次运行 10 例全红（`Failed to connect`）。`mqtt_sn_codec` 把 CONNECT 的 Will / CleanSession 读在 bit 7 / 6（MQTT 3.1.1 的 CONNECT 布局），而 MQTT-SN 1.2 §5.4.4 定义在 **bit 3 / 2**——`mqtt-sn-tools` 发 `0x04` 表示 clean session，于是每个合规客户端都被判 `{reserved_bits, 0x04}`。编解码两侧均已修正，`mqtt_sn_codec_SUITE` 增加两例钉死工具真实发出的字节。这类缺陷**不可能**由自编客户端发现（两侧用同一份错误布局即可互相通过）。

**已声明的限制 / 边界**：

- **无 DTLS**：本阶段只跑明文 UDP；DTLS 列为后续待办（设计 §1.2），因此这里是唯一**明文且没有 TLS 路径**的 listener（QUIC 的 14567 也只把 `tls.enabled` 当证书来源，但它强制 TLS 1.3）。
- **无 Forwarder Encapsulation**（1.2 §5.5）与串口桥：设计 §1.2 明确不做。
- **休眠期 QoS 0 丢弃**：QoS 0 从不进存储，睡眠期间无处可取；要「一条不丢」就用 QoS 1/2。
- **第三方客户端覆盖不到三处**：`mqtt-sn-tools` 0.0.7 **无 QoS 2 代码路径**、**无 Will 选项**、其 PINGREQ 不带 client id（无法按名唤醒休眠会话）。这三项只由 `mqtt_sn_conn_SUITE` 覆盖，Gate 文本单列，不以「第三方全绿」含混带过。
- **每客户端 normal topic id 是私有的**：订阅方 SUBACK 拿到的 id 在发布方的表里毫无意义——这是协议本身的性质，不是实现取巧；发布方必须先自己 REGISTER。

---

### 4.13 S8-B 追加（2026-11-10）：MQTT over WebSocket（`broker_proto_mqtt_ws`）

> 编号说明：§4.11 是 S8-C（MQTT-SN，已落地），§4.12 是 S8-D（MQTT over QUIC，已落地），本节按 S8-B 执行计划落在 §4.13。

**是什么**：MQTT 3.1.1 §6 / MQTT 5.0 §6 定义的 MQTT over WebSocket。同一条 `mqtt_conn_fsm`（与 1883 明文 MQTT、打开 `tls.enabled` 后同一端口的 TLS MQTT 完全同一份代码）跑在 RFC 6455 **binary** 帧里；WebSocket 只当字节管道，协议语义、QoS、会话、retain、Will 全部由 MQTT 引擎负责。默认端口 **8083**，TLS 跟随 `broker_security.tls.enabled`（wss）。

**为什么不并进 8080**：`broker_proto_ws`（S5）是 Broker 自己的 JSON/binary 消息 API，与标准 MQTT over WebSocket 的应用协议完全不同。合并会让 S5 已钉死的线上字节行为与 MQTT 客户端需求互相牵制，因此是两个 app、两个端口。

| 能力 | 状态 | 说明 |
|---|---|---|
| RFC 6455 握手 + 子协议协商 | 已支持 | 只在 `Sec-WebSocket-Protocol` 命中 `mqtt` / `mqttv3.1` / `mqttv3.1.1` 时升级；缺失或不匹配 → **HTTP 400** |
| URL path | 任意 | 协议规定的是子协议不是路径；`/`、`/mqtt`、`/ws/x` 都可（客户端库的 path 与部署约定一致即可） |
| 报文与帧对齐 | 无要求 | 一个报文可跨多个帧（交 FSM 的 `buf` 重组）；一个帧可含多个报文（逐条处理） |
| 数据帧类型 | 仅 binary | text 帧 → close **1003**，不把其 payload 当 MQTT 字节 |
| 控制帧 | 已支持 | ping → pong、close 回 close 后断开；与 MQTT keepalive 相互独立 |
| 帧大小上限 | 已支持 | `max_frame_bytes`（默认 16 MiB）；超限 → close **1002**（`ws_frame` 对每个 decode 错误统一 1002）。报文级上限仍是 `mqtt_max_packet_size` |
| MQTT 3.1.1 / 5.0 | 已支持 | CONNECT/CONNACK、PUBLISH 双向 QoS0/1/2、SUBSCRIBE/SUBACK、UNSUBSCRIBE、PINGREQ/PINGRESP、DISCONNECT、retain、Will、持久会话与离线追赶、takeover 全部复用 FSM |
| TLS（wss） | 已支持 | 与其他 listener 同一开关；`mqtt_ws_conn` 显式完成 `Transport:handshake/2` |
| 背压 | 刻意缺省 | `mqtt_transport_ws:arm_once/1` 是 no-op，WS 层每帧后自行 re-arm；`max_inflight` / `receive_maximum` 是唯一窗口控制 |
| `permessage-deflate` | **不支持** | 设计 §2 明确排除；客户端提出该扩展时得到的是未压缩连接 |
| MQTT-SN / QUIC | 不在本节 | 分别见 S8-C（§4.11）与 S8-D（§4.12） |

**证据**：

| 类型 | 位置 | 规模 |
|---|---|---|
| CT 套件（字节级自测） | `apps/broker_proto_mqtt_ws/test/mqtt_ws_conn_SUITE.erl` | **19 例**（子协议协商 + 两条 400 拒绝、CONNECT、QoS1/QoS2 双向、retained 后到订阅、跨帧、单帧多报文、WS ping/pong、text→1003、client close、超长帧、任意 path、Will、wss、明文 TCP 打 wss 端口） |
| 真实第三方客户端 | `test/protocol-compatibility/mqtt_ws/`（paho-mqtt **2.1.0**，`transport="websockets"`） | **8/8** 第三方用例：connect、QoS0/QoS1/QoS2 往返、retain、Will、MQTT 5.0、非根 path |
| 传输探针（**非**第三方证据） | 同上脚本的 `ws-raw-framing` | 1 例：跨帧重组 + text 帧 → close 1003；报告内 `third_party: false`、`client: "raw-socket-probe"` |
| 回归（顺带修复） | `apps/broker_proto_ws/test/ws_conn_SUITE.erl` | 新增 `wss_is_served_when_tls_is_enabled`；撤掉 `ws_conn` 的 `Transport:handshake/2` 该例必 FAILED（已实测） |

**已声明的限制 / 边界**：

- **子协议是硬门槛**：没有 `Sec-WebSocket-Protocol` 的握手被 400 拒绝，不「猜」对方是 MQTT。浏览器随手连的 WebSocket 因此不会被当成 MQTT 连接。
- **无 `permessage-deflate`**：MQTT over WebSocket 的压缩扩展未实现，带宽敏感部署需在客户端关闭该扩展。
- **背压按帧而非按报文**：一个超大帧会一次性交给 FSM；窗口控制仍靠 `max_inflight` / `receive_maximum`，与 TCP 传输一致。
- **帧上限与报文上限是两个旋钮**：`max_frame_bytes`（WS 层）与 `mqtt_max_packet_size`（MQTT 层）分别生效，调小前者不会改变 MQTT 报文的法定上限。

---

### 4.12 S8-D 追加（2026-11-11）：MQTT over QUIC（`broker_proto_mqtt_quic`）

**是什么**：OASIS MQTT TC 的 [MQTT over QUIC — Single Stream Mode（Committee Note Draft 3）](https://github.com/oasis-tcs/mqtt/) 实现。规范的核心只有一句：**MQTT 报文零改动**，整条 MQTT Network Connection 走**一条客户端发起的双向流**。所以本仓库没有为它写第二个 MQTT 引擎——`mqtt_conn_fsm` 与 1883 / 8083 是同一份代码，`mqtt_transport_quic` 是第三个传输实现（TCP/TLS → WebSocket → QUIC）。默认 **UDP 14567**（EMQX 的 QUIC 惯用端口，便于对照）。

**与前三节的差别在哪**：QUIC 上「流关闭」不等于「连接关闭」（CN Draft 3 §2.1）。客户端关掉流只结束这条 MQTT 连接，它可以在同一条 QUIC 连接上新开一条流重新 CONNECT。因此比 TCP 多一个进程层次：listener → acceptor → 每连接一个 `mqtt_quic_conn`（拥有连接句柄）→ 每流一个 `mqtt_conn_fsm`。分层细节见 `docs/architecture.md` §3.12。

**构建门控**：`quicer`（msquic + quictls 的 NIF）是 `quic` profile 的依赖，默认 `rebar3 compile/ct/release` 完全不拉它；`broker_proto_mqtt_quic` 在默认 profile 里照常编译、照常启动，只是没有 listener。手工打开会得到 `{error, quicer_not_available}`（`optional_applications => [quicer]`）。

| 能力 | 状态 | 说明 |
|---|---|---|
| MQTT 3.1.1 / 5.0 | 已支持 | 同一份 `mqtt_conn_fsm`：CONNECT/CONNACK、PUBLISH 双向 QoS0/1/2、SUBACK、UNSUBSCRIBE、PINGREQ/PINGRESP、DISCONNECT、retain、Will、持久会话与离线追赶、takeover |
| ALPN | 仅 `mqtt` | CN Draft 3 §2.1 唯一值。客户端带别的 ALPN → 握手失败（`{error, transport_down, #{status := alpn_neg_failure}}`），不静默降级 |
| 传输 | 强制 TLS 1.3 | QUIC 没有明文模式。证书取自 listener map 或 `broker_security.tls`；都没有 → listener 拒绝启动 `{quic_requires_tls, Key}` |
| 单流模式 | 已支持 | 一条客户端发起的 bidi 流承载整条连接；`peer_unidi_stream_count => 0`，服务端不发流 |
| 流关闭 ≠ 连接关闭 | 已支持 | 同一条 QUIC 连接上开新流 = 新 MQTT 会话（`a_new_stream_on_the_same_connection_is_a_new_session`） |
| 流信用 | 建连时一次性授予 | `max_streams_per_conn`（默认 4）= **同一连接上的重连次数**，不是并发流数；quicer 0.4.12 运行期补信用无效（实测），超出后新流不到达并记 `quic: stream credit exhausted` |
| 会话状态 | 与 1883 同域 | client id 全局唯一，QUIC 与 TCP 同 id 是 takeover；订阅/游标/retain 全部复用 `broker_subscription` / `broker_queue` / `broker_retain` |
| 背压 | 按流 | `arm_once/1` = `quicer:setopt(Stream, active, once)`；`max_inflight` / `receive_maximum` 仍是窗口控制 |
| 0-RTT early data | **不支持** | CN 里的可选项，本轮不接收 |
| 多流模式 | **不支持** | EMQX 私有扩展，不在规范里 |
| datagram / unidi 流 | **不支持** | 非单流模式语义 |

**证据**：

| 类型 | 位置 | 规模 |
|---|---|---|
| CT 套件（传输契约，纯函数） | `apps/broker_proto_mqtt_quic/test/mqtt_quic_transport_SUITE.erl` | **13 例**：`decode_msg/2` 的 binary/atom 分派、按 stream 匹配、关闭类事件归一、`send`/`arm_once`/`close` 在缺 NIF 时的明确错误 |
| CT 套件（listener） | `apps/broker_proto_mqtt_quic/test/mqtt_quic_listener_SUITE.erl` | **config 4 例**（无证书拒绝、证书来自 `broker_security.tls`、默认值、默认不监听）+ **quic 6 例**（临时端口、ALPN `mqtt` 握手、非 `mqtt` 被拒、真实 CONNECT→CONNACK、**PINGREQ 不是协议错误**、同连接新流 = 新会话）。默认 profile 只有 config 组；`rebar3 as quic ct` 才展开 quic 组 |
| 真实第三方客户端 | `test/protocol-compatibility/mqtt_quic/`（EMQX `emqtt` **1.16.1** + `quicer` 0.4.12，ALPN `mqtt`） | **8/8**：connect、QoS0、QoS1、QoS2、retain、Will、MQTT 5.0、持久会话续连。被测节点**只**起 QUIC listener，端口是内核分配的临时 UDP 端口 |

**`emqtt` 的能力边界（Gate 文本必须照实写）**：`emqtt` 的 QUIC 支持是 EMQX 侧的**实验特性**——它按 `peer_bidi_stream_count => 3`、`active => 1`、异步发送工作；公开 API **没有流级关闭**（`close/1` 会连连接一起关），因此「关掉流、在同一条连接的新流上重新 CONNECT」这半边断言在本仓库的 CT 里（raw `quicer` + 同一引擎），不在第三方驱动里。同理，0-RTT 与多流两边都没有实现，第三方套件也不声称覆盖。诚实的结论是：**一个独立客户端的实验性 QUIC 路径在本 Broker 上跑通上述 8 个用例**，而不是「MQTT over QUIC 对所有客户端互通」。

**顺带修掉的两个真实问题**（都写在代码注释里，撤掉修复即复现）：

1. `arm_once/1` 曾写 `setopt(Stream, active, 1)`。quicer 把整数 active 当 `{active, N}`（multi 模式），收满 N 条后向 owner 发 `{quic, passive, Stream, undefined}`——FSM 不认识它，**每读一个报文记一条 `unexpected event` 并 `protocol_errors` +1**。改用原子 `once` 后消失，回归断言是 `a_pingreq_is_not_a_protocol_error`（报文本身在坏版本里照常回，只能对计数器断言）。
2. `broker_cluster_dist_SUITE` 的 peer 代码路径过滤写死了 `_build/test`。CT 的构建目录按 profile 命名，`rebar3 as quic ct` 下该串不匹配，于是 peer 只拿到 suite 目录、连 `ra.app` 都找不到，四个分布式用例全部在 `init_per_testcase` 失败。改为「以 `ebin` 结尾」后与 profile 无关。

**已声明的限制 / 边界**：

- **默认不监听**：QUIC 无证书即拒绝启动，而拒绝启动是该 app 的启动失败，release 会起不来。启用顺序见 `docs/operations.md` §1.2。
- **流信用是配额不是补充**：同一 QUIC 连接上重连超过 `max_streams_per_conn` 次后，客户端必须新建 QUIC 连接。这是 quicer 0.4.12 的实测限制，不是设计偏好。
- **只有 `rebar3 as quic` 构建带 NIF**：默认 release 里没有 `quicer`，也就没有 QUIC 端口（`broker_proto_mqtt_quic` 这个 app 本身在两个 release 里都有，只是 `listeners => []`）。运维上要先确认 `rebar3 as quic release` 的产物。
- **DTLS 不相关**：QUIC 自带 TLS 1.3，与 MQTT-SN 的明文 UDP 是两回事；`broker_security.tls.enabled` 对 QUIC 是「证书来源」而不是「开关」。

---

## 4.14 S9-A 追加（2026-11-11）：A2A 1.0.0 JSON-RPC 前端（`broker_proto_a2a`）

**是什么**：A2A `1.0.0` 的 JSON-RPC 传输绑定（spec §9）在统一消息内核之上的一层 adapters。S9-A 只交付**协议底座**：JSON-RPC 2.0 信封、`A2A-Version` 协商、`broker_security` 认证/ACL 接线、HTTP/1.1 与 SSE 的出站原语，以及**一个真实方法** `SendMessage` 的 echo 实现。**没有 Task 状态机、没有 Agent Card、没有流式业务**——那些是 S9-B/S9-C。

**两个 app**（ADR-026 §D1 的依赖方向 `adapter → base → kernel`）：

| app | 形态 | 内容 |
|---|---|---|
| `broker_proto_agent_rpc` | 纯库：无 supervisor、无 listener、无自有 env | `agent_rpc_json`（信封）、`agent_rpc_error`（错误码表）、`agent_rpc_version`（版本协商）、`agent_rpc_http`（HTTP/1.1 帧）、`agent_rpc_sse`（SSE 帧）、`agent_rpc_auth`（凭据 → `broker_security`） |
| `broker_proto_a2a` | app + 监督树，**默认零 listener** | `broker_proto_a2a_app`（boot 校验）、`..._sup`（listener 为最后一项）、`a2a_listener`（ranch_protocol）、`a2a_conn`（每连接 keep-alive 循环）、`a2a_rpc`（方法分派 + 全部门控） |

**能力矩阵（S9-A 完成态）**：

| 能力 | 状态 | 说明 |
|---|---|---|
| JSON-RPC 2.0 信封 | 已支持 | `jsonrpc` 必须 `"2.0"`、`method` 非空字符串、`id` 为 string/number/null、`params` 缺省为 `{}` |
| batch（顶层数组） | **不支持** | 回 `-32600`。A2A 绑定只定义单请求，而「部分成功」在会创建 Task 的方法上没有可解释语义 |
| notification（无 `id`） | **不支持** | 回 `-32600`。A2A 全部方法是请求-响应 |
| `SendMessage` | 已支持（**echo**） | 返回 `SendMessageResponse.message` 分支：`role = "ROLE_AGENT"`、**新的** `messageId`（base64url 128 bit）、沿用或生成 `contextId`、原样带回 `parts`/`metadata`。**不创建 Task、不写日志** |
| 其余 10 个方法（`GetTask`/`ListTasks`/`CancelTask`/`SendStreamingMessage`/`SubscribeToTask`/Push config 三件套/`GetExtendedAgentCard`） | **未实现** | 一律 `-32601`（HTTP 200）。S9-B/S9-C 逐个落地 |
| `A2A-Version` 协商 | 已支持（仅头形式） | 只接受 `Major.Minor`；缺失/空按 `0.3` **读入**，但服务端只声明 `["1.0"]`，因此缺失头 → `-32009`（HTTP 400）。query 参数形式不实现 |
| 认证 | 已支持 | `Authorization: Bearer <token>` 或 `X-API-Key`，解析为 `{token, T}` 交给 `broker_security:authenticate_conn/1`。两头同时存在且不同 → 拒绝。**不做 JWT/JWKS**（ADR-026 §D8） |
| `require_auth` | 默认 `true` | adapter 自有 env，`false` 只在**回环绑定**下允许（boot 校验拒绝宽绑定）；不动 `broker_security` 的 `{require_auth, []}` |
| ACL | 已支持 | `publish` + destination `$a2a/task/echo`（S9-A 固定值，S9-B 换成真实 task id）；拒绝回裸 `403`，不包 JSON-RPC 信封 |
| 请求体上限 | 已支持 | `min(max_body_bytes, broker_limits:max_message_bytes())`，默认 1 MiB；`Content-Length` 超限**不读体**直接 `413` + `Connection: close` |
| chunked 请求体 | 已支持 | 同上限，逐块累加校验 |
| `Expect: 100-continue` | 已支持 | 先回 `HTTP/1.1 100 Continue\r\n\r\n` 再读体 |
| keep-alive | 已支持 | 每连接一个进程循环处理，`Connection: close` 或超时退出；单连接上限 1000 请求 |
| `/health` | 已支持 | `GET /health` → `200 {"status":"ok"}`，不需认证，仅供探活 |
| SSE 出站原语 | 库就绪，**未接方法** | `agent_rpc_sse` 产帧、`agent_rpc_http:chunk/1` 包 chunked；S9-C 才接到流式方法 |
| HTTP/2、gRPC/REST 绑定 | **不支持** | 非目标（ADR-026 §D8） |
| TLS | 随 `broker_security.tls` | enabled 时用 `ranch_ssl`，否则 `ranch_tcp` |
| 默认监听 | **不监听** | `listeners => []`（ADR-026 §D4）。8180/8081/8082 都不由本阶段占用 |

**错误映射**（A2A §5.4 + JSON-RPC 2.0）：

| 情形 | HTTP | JSON-RPC code |
|---|---|---|
| 非 JSON 体 | 400 | `-32700`（`id = null`） |
| 信封不合法 / batch / 无 `id` | 400 | `-32600`（`id = null`） |
| 未知方法 | **200** | `-32601` |
| 方法参数不合法 | **200** | `-32602` |
| `A2A-Version` 不支持（含缺失） | 400 | `-32009`，`data.supported = ["1.0"]`，`id = null` |
| 无/错凭据 | 401 | 裸 HTTP，`WWW-Authenticate: Bearer realm="a2a"` |
| ACL 拒绝 | 403 | 裸 HTTP，空体 |
| 请求体超限 | 413 | 裸 HTTP，`Connection: close` |
| 非 JSON media type | 415 | 裸 HTTP |

**验证顺序即安全边界**：media type → 凭据 → 版本 → JSON-RPC 信封 → 方法分派。无凭据的请求在解析 body 之前就被拒，版本不支持的请求不会走到方法分派。

**证据**：

| 类型 | 位置 | 规模 |
|---|---|---|
| CT 套件（底座） | `apps/broker_proto_agent_rpc/test/` | 6 套件 **69 例**：`json` 16、`error` 8、`version` 12、`http` 13（真实 socket）、`sse` 7、`auth` 13（真实 `broker_security`） |
| CT 套件（listener） | `apps/broker_proto_a2a/test/a2a_listener_SUITE.erl` | **16 例**：echo、版本缺失/不支持、401×2、403、`-32601`、`-32700`、`-32602`、413、415、keep-alive、close、health、匿名回环放行、匿名宽绑定拒绝启动。全部 `port => 0` |
| 全量回归 | `rebar3 ct` | **908 例全通过**（基线 823 + 新增 85，59 套件） |
| 真实客户端 | `curl`（下节原文） | **11 条**请求/响应原文 |

**`curl` 实测（2026-11-11，证据节点 127.0.0.1:18091，`require_auth => true`，用户 `agent-s9a` 持 token）**：

```
$ curl -sS -i -X POST http://127.0.0.1:18091/ \
    -H 'Content-Type: application/json' -H 'A2A-Version: 1.0' \
    -H 'Authorization: Bearer s9a-evidence-token' \
    -d '{"jsonrpc":"2.0","id":1,"method":"SendMessage","params":{"message":{
         "messageId":"m1","role":"ROLE_USER","contextId":"ctx-1",
         "parts":[{"text":"hello agent"}]}}}'
HTTP/1.1 200 OK
connection: keep-alive
content-length: 157
content-type: application/json

{"id":1,"jsonrpc":"2.0","result":{"message":{"contextId":"ctx-1",
 "messageId":"jUvJeI6F5f8-rOBRz4Zc8Q","parts":[{"text":"hello agent"}],
 "role":"ROLE_AGENT"}}}
```

| # | 请求 | 响应 |
|---|---|---|
| 1 | `SendMessage` + Bearer + `A2A-Version: 1.0` | `200`，`result.message.role = "ROLE_AGENT"`，`messageId` 与请求不同（上） |
| 2 | 同上，去掉 `Authorization` | `401`，`www-authenticate: Bearer realm="a2a"`，空体 |
| 3 | `A2A-Version: 2.0` | `400`，`{"error":{"code":-32009,...,"data":{"supported":["1.0"]}},"id":null}` |
| 4 | 不带 `A2A-Version` | `400`，同上 `-32009`（按 0.3 读入，但 0.3 不在支持列表） |
| 5 | `method: "GetTask"` | `200`，`{"error":{"code":-32601,"message":"Method not found"},"id":2}` |
| 6 | `Content-Type: text/plain` | `415`，空体 |
| 7 | `Content-Length: 2000000` | `413`，`connection: close`，空体 |
| 8 | body 为 `not json` | `400`，`{"error":{"code":-32700,"message":"Parse error"},"id":null}` |
| 9 | `GET /health`（无凭据） | `200`，`{"status":"ok"}` |
| 10 | 同一连接连发两次（`curl` 复用） | 两次 `200`，`Connection #0 left intact` / `Re-using existing connection` |
| 11 | ACL 配 `deny $a2a/#`（另一端口 18092） | `403`，空体 |

**已声明的限制 / 边界**：

- **echo 不是 A2A 实现**：`SendMessage` 只回一条消息，不产生 `Task`。任何依赖 `result.task` 的客户端在 S9-A 上拿不到 task，会走 `result.message` 分支。`GetTask` 等一律 `-32601`。
- **无状态**：进程不保留任何会话状态，重启即遗忘；`contextId` 只是回显，S9-B 才有 context 语义。
- **默认不监听**：`listeners => []`，运维显式配置 `#{port => 8081, ref => {a2a, 8081}}` 才开端口；18091 只是本次证据端口。
- **不做 JWT**：只接受 `broker_user` 表里的不透明 token，不验签、不取 JWKS、不实现 OAuth AS。
- **只 HTTP/1.1**：无 HTTP/2、无 gRPC、无 REST 绑定。
- **SSE 未接业务**：库有帧与 chunk 原语，但没有任何方法返回 `text/event-stream`；S9-C 才有流。

---

## 4.15 S9-B 追加（2026-11-12）：A2A Task 生命周期与 Agent Card 发现

**是什么**：S9-B 把 S9-A 的 `SendMessage` echo 换成真正的 **Task 生命周期**，并按 ADR-026 §D3 落地 **Agent Card 发现面**。能力分三块：

1. **Task 状态机 + 事件溯源日志**：Task 的每次状态变化都是一条普通 `#broker_message{}`，destination = `$a2a/task/<TaskId>`，分区由 `broker_router_hash:route/3` 求得（ADR-010 是唯一分区真相），写入走 `broker_queue:append/3`（唯一写路径，ADR-007 的可靠性档与限额原样生效）。**没有第二套日志、没有第二套消息模型、不用 cursor**。
2. **投影是事件的折叠**：`a2a_task_projection` 是纯函数，把 `$a2a/task/<id>` 的事件按序折叠成 Task；投影放在该 Task 所属 shard 的 ETS 里（每 shard 一个 gen_server 串行化），Mnesia 完全不参与。boot 时 `a2a_task_recovery` 全量重放每个分区，把投影重建出来——**"投影等于日志"是可验证的，不是口号**。
3. **Agent Card 与控制面**：`broker_meta` 新增第 12 张表 `agent_card`（ADR-026 §D3 显式修订 ADR-002 的冻结表清单），`/.well-known/agent-card.json`（ETag/`If-None-Match` 304/`Cache-Control`）+ 项目私有 catalog `GET /agents` + JSON-RPC `GetExtendedAgentCard`。

> **对 §4.14 阶段快照的说明**：§4.14 里「`GetTask` 等一律 `-32601`」「echo 不建 Task」「destination 固定 `$a2a/task/echo`」等说法是 **S9-A 完成态**的记录，保留原文不动。S9-B 之后这些都被取代，当前事实以下面各表为准。

### 4.15.1 方法能力矩阵（S9-B 完成态）

| 方法 | 状态 | 语义要点 |
|---|---|---|
| `SendMessage` | **已支持（建 Task）** | 返回 `result.task`（不再走 `result.message` 分支）。`task_auto_complete = true`（默认）时同步推进 `SUBMITTED → WORKING → agent message → COMPLETED` 后才回包，即 ADR-025 §D4 的 `return_immediately = false` 语义 |
| `GetTask` | 已支持 | `params.id` 必填。本租户存在 → 200 + Task；不存在 / 跨租户 → **404 + `-32001`**（跨租户与不存在同码，id 不是存在性预言机） |
| `ListTasks` | 已支持 | `contextId` / `status`（单个或数组，按 proto 枚举名解析）过滤；`pageSize` 默认 50、上限 100；`pageToken` = 下一页起点 TaskId（不透明）。不合法状态 → `-32602` |
| `CancelTask` | 已支持 | 非终态 → 200 + `TASK_STATE_CANCELED`；终态 → **400 + `-32002`**（终态不可变） |
| `GetExtendedAgentCard` | 已支持 | 该 card 有 `extended_card` → 200；无 → **400 + `-32007`**（`extended_agent_card_not_configured`） |
| `SendStreamingMessage` / `SubscribeToTask` / Push config 三件套 | **仍未实现** | 一律 `-32601`（HTTP 200）。S9-C |

**Task 状态机**（`a2a_task.erl`，proto 9 值）：`TASK_STATE_UNSPECIFIED` / `SUBMITTED` / `WORKING` / `INPUT_REQUIRED` / `AUTH_REQUIRED` / `COMPLETED` / `FAILED` / `CANCELED` / `REJECTED`。终态四个（`COMPLETED`/`FAILED`/`CANCELED`/`REJECTED`）**无出边**；中断态两个（`INPUT_REQUIRED`/`AUTH_REQUIRED`）可回到 `WORKING`；`WORKING → WORKING` 允许（进度上报）。迁移表就是 ADR-025 §D3 那一张，`a2a_task_SUITE` 穷举 81 对状态逐对断言。

### 4.15.2 事件日志的形状

一条 Task 事件 = 一条普通消息：

| 字段 | 值 |
|---|---|
| `destination` | `<<"$a2a/task/", TaskId/binary>>` |
| `payload` | A2A `StreamResponse` 分支的线上 JSON（`task` / `statusUpdate` / `artifactUpdate` / `message`），外加项目私有 `_meta.unified-mq = {seq, kind}` |
| `payload_encoding` | `json` |
| `persistence` | `durable` |
| `reliability` | `broker_proto_a2a.task_reliability`（默认 `fsync`）；创建事件与终态事件**同档**（ADR-025 §D4） |
| `partition_id` | `broker_router_hash:route/3` 求出的分区，同 Task 全部事件落同一分区，重放保序 |

`_meta.unified-mq` 是**加法字段**：标准 A2A 客户端忽略它；恢复路径用它区分「创建快照」与「增量」，不用猜 JSON 形状。

**恢复语义**（ADR-025 §D5/§D6）：

- boot 全量重放（从各分区 offset 0 折叠 `$a2a/task/` 前缀事件），**不做增量 checkpoint**（S9-F 定稿：不实现，两个从未被读的 env 已作为死配置删除；见 §4.19 与 `docs/security.md` §12.1）。
- 首条可见事件不是创建快照（创建事件已被 retention GC）→ 补一条服务端生成的 `FAILED` + `metadata.reason = history_lost` 终态事件，**不返回半截 Task**；补写事件自身可被下一次重放识别，因此重复 boot 幂等。
- 分片串行化：`phash2(TaskId, N)` 定 shard，`create`/`cancel`/`apply_event` 都经该 shard 的 gen_server，"读状态→校验迁移→追加事件→更新投影" 对所有同 Task 请求原子。不同 Task 不同 shard 互不等待。
- 一个 shard 崩了 → registry 是 `one_for_all`，整组重启并由 recovery 重放（ETS 缓存本就是缓存）。

### 4.15.3 Agent Card 发现面

| 端点 | 行为 |
|---|---|
| `GET /.well-known/agent-card.json` | RFC 8615 公开卡。`?agent=<id>` 选择；无参取**字母序首个 active** card（确定性，不依赖 Mnesia 行序）。有效凭据且有 extended 变体 → 返回 extended；匿名 → 只回公开卡。`ETag: "<version>-<content hash>"`，`If-None-Match` 命中 → `304` 空体；`Cache-Control: max-age=<agent_card_max_age>`（默认 60）。未知 agent → `404 {"error":"agent_card_not_found"}` |
| `GET /agents` | **项目私有** catalog（A2A spec 未定义注册 API，R-D4）。支持 `skill`（id/name/tags 任一匹配）、`tag`、`provider`、`capability`（`capabilities.<name> === true` 或 `extensions[].uri`）过滤；`pageSize` 默认 50 / 上限 100；`pageToken` = 上一页最后一张卡的 agent id。响应 `{"agents":[...], "nextPageToken"?}` |
| JSON-RPC `GetExtendedAgentCard` | `params.agent` 选择 card；无 extended 变体 → `-32007` |

**身份解析（安全边界）**：`/.well-known/agent-card.json` 与 `/agents` 都**不需要凭据**，但**不是跨租户的**——无凭据时身份是 `agent_rpc_auth:anonymous()`，其 tenant 为 `broker_security:default_tenant()`（默认 `t0`）。所以「匿名可读」= **在默认租户内可读**，看不到 `t1`/`t2` 的卡。带凭据但凭据无效 → `401`（不是降级成匿名）。CT 与 `curl` 证据都按这条断言。

**boot 种子**：`broker_proto_a2a.agent_cards` 是配置里的初始卡列表，`a2a_card:seed/0` 在 app 启动时幂等写入 `broker_meta`——内容与状态未变则**不 bump version**（否则每次重启都让所有客户端缓存失效）；格式不合法（缺 `name`/`description`/`supportedInterfaces`/`version`/`capabilities`/`defaultInputModes`/`defaultOutputModes`/`skills`）→ **启动失败**，不静默跳过。管理面 CRUD **未排期**（S9-F 复核后不做，见 `docs/architecture.md` 的延期表）。

### 4.15.4 错误映射（新增部分）

| 情形 | HTTP | JSON-RPC code |
|---|---|---|
| Task 不存在 / 跨租户 | **404** | `-32001` `task_not_found` |
| 终态再取消 | **400** | `-32002` `task_not_cancelable` |
| card 无 extended 变体 | **400** | `-32007` `extended_agent_card_not_configured` |
| `GetTask`/`CancelTask` 的 `id` 缺失或非字符串 | 200 | `-32602` |
| `ListTasks` 的 `status` 不是合法枚举 | 200 | `-32602` |
| 其余（信封/版本/凭据/media type） | 同 §4.14 | 同 §4.14 |

注意 `task_not_found` 与 `task_not_cancelable` **不再**是 200：A2A §5.4 把它们放在 404/400，只有信封类错误（`-32600`/`-32601`/`-32602`）保持 200。

### 4.15.5 证据

| 类型 | 位置 | 规模 |
|---|---|---|
| CT（控制面） | `apps/broker_meta/test/broker_meta_SUITE.erl` | **12 例**（基线 10 + 2：`agent_card_crud_bumps_version`、`agent_card_registration_is_serialized`（version 1..10 无缺口）；`schema_has_the_frozen_tables` 断言改为 12 表） |
| CT（状态机） | `apps/broker_proto_a2a/test/a2a_task_SUITE.erl` | **11 例**（9 状态、81 对迁移穷举、终态无出边、wire 短名往返） |
| CT（日志） | `apps/broker_proto_a2a/test/a2a_event_SUITE.erl` | **6 例**（destination 往返、payload 是 JSON binary、`_meta` 内容、可靠性档校验） |
| CT（投影） | `apps/broker_proto_a2a/test/a2a_task_projection_SUITE.erl` | **8 例**（折叠、非法迁移、`history_lost` 检测、repair 幂等） |
| CT（端到端） | `apps/broker_proto_a2a/test/a2a_listener_SUITE.erl` | **33 例**（S9-A 16 例改写 + 新增 17：Task 生命周期 11 + Agent Card 6；全部 `port => 0`） |
| 全量回归 | `rebar3 ct` | **952/952、62 套件**（基线 908/59，零 FAILED） |
| 真实客户端 | `curl`（下节原文） | **25 条**请求/响应原文 |

### 4.15.6 `curl` 实测（2026-11-12，证据节点 127.0.0.1:18093）

节点配置：`task_shards => 4`、`task_reliability => fsync`、`require_auth => true`，用户 `agent-s9b`（tenant `t1`）与 `agent-s9b-two`（tenant `t2`），三张种子卡（t1 的 `echo` 带 extended、t1 的 `summarizer` 无 extended、t2 的 `echo`）。

```
$ curl -sS -X POST http://127.0.0.1:18093/ \
    -H 'Content-Type: application/json' -H 'A2A-Version: 1.0' \
    -H 'Authorization: Bearer s9b-evidence-token' \
    -d '{"jsonrpc":"2.0","id":1,"method":"SendMessage","params":{"message":{
         "messageId":"m1","role":"ROLE_USER","contextId":"ctx-s9b",
         "parts":[{"text":"hello agent"}]}}}'
{"id":1,"jsonrpc":"2.0","result":{"task":{
  "contextId":"ctx-s9b",
  "history":[{"contextId":"ctx-s9b","messageId":"m1","parts":[{"text":"hello agent"}],
              "role":"ROLE_USER","taskId":"7SkevM2cFaECEmgbiPa59Q"},
             {"contextId":"ctx-s9b","messageId":"HeJM02ADIKX4B1xOJXNiEw",
              "parts":[{"text":"hello agent"}],"role":"ROLE_AGENT",
              "taskId":"7SkevM2cFaECEmgbiPa59Q"}],
  "id":"7SkevM2cFaECEmgbiPa59Q",
  "status":{"state":"TASK_STATE_COMPLETED","timestamp":"2026-11-11T17:49:34.640Z"}}}}
```

| # | 请求 | 响应 |
|---|---|---|
| 1 | `SendMessage`（`task_auto_complete=true`） | `200`，`result.task.status.state = TASK_STATE_COMPLETED`，`history` 含用户消息 + agent 消息（上） |
| 2 | `GetTask` 同一 id | `200`，与 1 的 task 完全一致（投影不是缓存副本，是同一折叠结果） |
| 3 | `GetTask` 不存在的 id | **`404`** `{"error":{"code":-32001,"message":"Task not found"},"id":3}` |
| 4 | `ListTasks` `{"contextId":"ctx-s9b"}` | `200`，`result.tasks` 含该 context 全部 Task |
| 5 | `ListTasks` `pageSize=1` → 用 `nextPageToken` 取第二页 | 两页各一条，**不重不漏**；单条时不返回 `nextPageToken`（下节分页示例） |
| 6 | `CancelTask` 已 COMPLETED 的 Task | **`400`** `{"error":{"code":-32002,"message":"Task cannot be canceled"},"id":7}` |
| 7 | 用 t2 凭据 `GetTask` t1 的 Task | **`404`** + `-32001`，与 #3 逐字相同（不泄露存在性） |
| 8 | `GET /.well-known/agent-card.json`（匿名） | **`404`** `{"error":"agent_card_not_found"}`——匿名 = 默认租户 `t0`，那里面没有种子卡 |
| 9 | `GET /.well-known/agent-card.json?agent=echo` + t1 凭据 | `200`，返回 extended 卡（`name = "Echo Agent (extended)"`、`description = "internal view"`），带 `etag`/`cache-control: max-age=60` |
| 10 | 同上 + `If-None-Match: <上一步 ETag>` | **`304`**，空体，`etag` 原样返回 |
| 11 | `GET /agents`（t1） | `200`，两张卡（`Echo Agent`、`Summarizer`），`cache-control: max-age=60` |
| 12 | `GET /agents?tag=nlp` | `200`，只剩 `Summarizer` |
| 13 | `GET /agents?capability=streaming` | `200`，只剩 `Summarizer`（唯一 `capabilities.streaming = true`） |
| 14 | `GET /agents?pageSize=1` → `nextPageToken=echo` | 第一页 `Echo Agent` + `nextPageToken`；带 token 的第二页是 `Summarizer` |
| 15 | `GET /agents` + t2 凭据 | `200`，只有 `Tenant Two Echo`（catalog 按租户隔离） |
| 16 | JSON-RPC `GetExtendedAgentCard` `{"agent":"echo"}` | `200`，`result` 是 extended 卡 |
| 17 | `GetExtendedAgentCard` `{"agent":"summarizer"}` | **`400`** `{"error":{"code":-32007,"message":"Extended agent card is not configured"},"id":10}` |
| 18 | 停节点 → 用同一数据目录重启 → `GetTask` 同一 id | `200`，Task 与重启前**逐字段相同**（boot replay 从 WAL 重建投影） |
| 19 | 重启后 `ListTasks` | `200`，任务数 1（只有 WAL 里的真任务，没有重复投影） |
| 20 | `task_auto_complete=false` 下 `SendMessage` | `200`，`status.state = TASK_STATE_SUBMITTED`（留给异步执行器） |
| 21 | `CancelTask` 该非终态 Task | `200`，`status.state = TASK_STATE_CANCELED` |
| 22 | 再 `CancelTask` 同一 Task | **`400`** + `-32002`（终态不可变） |
| 23 | `GetTask` 无凭据 | **`401`**，`www-authenticate: Bearer realm="a2a"`，空体（在解析 body 之前拒） |
| 24 | `GET /health` 无凭据 | `200` `{"status":"ok"}` |
| 25 | `ListTasks` `{"status":"TASK_STATE_BOGUS"}` | `200` `{"error":{"code":-32602,...}}`（未知状态是参数错误，不静默放宽成"全部"） |

分页实测（`pageSize=1` 两页，`ctx-page` 下两个 Task）：

```
$ curl -sS -X POST .../ -d '{"jsonrpc":"2.0","id":3,"method":"ListTasks",
      "params":{"contextId":"ctx-page","pageSize":1}}'
{"id":3,"jsonrpc":"2.0","result":{
  "nextPageToken":"FPQMK-dMUU-bOUpp-IDDrg",
  "tasks":[{"id":"FPQMK-dMUU-bOUpp-IDDrg", ...}]}}

$ curl -sS -X POST .../ -d '{"jsonrpc":"2.0","id":4,"method":"ListTasks",
      "params":{"contextId":"ctx-page","pageSize":1,
                "pageToken":"FPQMK-dMUU-bOUpp-IDDrg"}}'
{"id":4,"jsonrpc":"2.0","result":{
  "tasks":[{"id":"woXcwqJB52z5nsM_8lleXA", ...}]}}
```

### 4.15.7 已声明的限制 / 边界

- **单节点**：投影在本地 shard 的 ETS；跨节点访问同一 Task（非 owner 节点 `GetTask`）没有重定向/代理，是 S9-C+ 的演进位。
- **全量重放是可扩展性上界**：每次 boot 扫全部分区，启动时间随日志量增长（分钟级，见 `docs/operations.md`）；**不做持久化 checkpoint**（S9-F 定稿），S9-B 声明但从未被读的两个 env 已删除。
- **无真实 agent 执行器**：`task_auto_complete=true` 时内建 echo agent 同步跑完；`false` 时 Task 停在 `SUBMITTED`，需要外部执行器调 `a2a_task_registry:apply_event/4`。两者都不是"agent 推理"。
- **失败路径计数器未加**：`a2a_state_violation`/`a2a_task_history_lost`/`a2a_checkpoint_corrupt` 三个键**不加**（S9-F 定稿：不动 `broker_metrics` 冻结键集，见 `docs/security.md` §12.1 第 17 条）。S9-B 用 `logger` 记事件，`snapshot/0` 的键集合不变。
- **`/agents` 是项目私有接口**：A2A spec 没有注册 API；客户端不能假设别家 A2A 服务有这个端点。
- **Agent Card 无签名**：`securitySchemes` 只能声明外部 scheme/URL，不做 JWS/PKI（收敛报告 §13 非目标）。card 里不放静态密钥（R-D6）。
- **默认仍不监听**：`listeners => []`。18093 只是本次证据端口。

---

## 4.16 S9-C 追加（2026-11-12）：SSE 流式任务事件 + 出站 Push Webhook

**是什么**：S9-C 给 S9-B 的 Task 接上两个**都不阻塞**的交互面——SSE 流式只占用客户端一条连接、出站 Push 完全异步：

1. **SSE 流式**（`a2a_stream`）：`SendStreamingMessage` / `SubscribeToTask` 的响应体是 `text/event-stream`；每个 `data:` 帧是一个**完整 JSON-RPC 2.0 Response**（`task` / `statusUpdate` / `artifactUpdate` / `message` 分支，照 R-T2），帧内带项目私有 `_meta.unified-mq = {seq, kind}`。流在**终态/中断态**事件转发后关闭，到达 `sse_max_duration_ms` 同样关闭（响亮失败，不无限挂连接）；空闲期发 SSE 注释行 `: ping`（`sse_heartbeat_ms`）保活。
2. **出站 Push**（`a2a_push` / `a2a_push_auth` / `a2a_push_worker`）：Task 到终态/中断态时，分片把该 Task 的 push config 以 `cast` 交给 worker（**任务终态不等投递**）；worker 侧 `guard`（全局 `egress_allowlist` ∩ 租户 `push_tenant_allowlist`）→ 手写 HTTP/1.1 `POST` 到**已解析并校验过的 IP**（带 `Host` 头）→ 只读状态行、禁 3xx、响应体上限 `push_max_response_bytes`（64 KiB）。失败按 `push_max_attempts` 次指数退避（`push_base_backoff_ms × 2^attempt`，封顶 `push_max_backoff_ms`），超过 `push_ttl_ms` 放弃，耗尽/过期把失败写成 `$a2a/push/dlq/<tenant>` 上的**一条普通消息**。
3. **控制面**：`broker_meta` 新增**第 13 张表 `push_config`**（ADR-026 §D6 amends ADR-002），行内**不存密钥**——密钥只在 `broker_proto_a2a.push_auth_secrets` env，按 scheme 名取用（ADR-026 §D6 第 6 条「不接受客户端明文密钥」）。

> **对 §4.15.1 阶段快照的说明**：§4.15.1 里「`SendStreamingMessage` / `SubscribeToTask` / Push config 三件套 **仍未实现**，一律 `-32601`」是 **S9-B 完成态**的记录，保留原文不动。S9-C 之后这 6 个方法都已接线，当前事实以下面各表为准。

### 4.16.1 方法能力矩阵（S9-C 对 §4.15.1 的增量）

| 方法 | 状态 | 语义要点 |
|---|---|---|
| `SendStreamingMessage` | **已支持（SSE）** | 建 Task 后返回 `{stream, Plan}`，`a2a_conn` 识别后接管 socket 写 `text/event-stream`。`task_auto_complete=true` 时同步推进到终态，事件**逐条**以 `data:` 帧流出，随后关流 |
| `SubscribeToTask` | **已支持（SSE）** | `params.id` 必填，校验 Task 属于本租户。**从 offset 0 重放历史事件**再跟随；未知/跨租户 Task → **404 + `-32001`**（与 `GetTask` 同码） |
| `CreateTaskPushNotificationConfig` | **已支持** | `push_enabled=false` → **400 + `-32003`**（`push_notification_not_supported`）；启用时 upsert 配置，`config.id` **由服务端生成**（`base64url(sha256(Tenant‖0‖Task‖0‖Url))[:9]`，防枚举） |
| `GetTaskPushNotificationConfig` | **已支持** | 按 `{taskId, configId}` 读；跨租户/不存在 → **404 + `-32001`** |
| `ListTaskPushNotificationConfigs` | **已支持** | 按 `taskId` 列出，只含本租户行 |
| `DeleteTaskPushNotificationConfig` | **已支持** | 幂等删除，成功 `result = {}` |

**SSRF 校验在投递路径上，不在配置写入路径上。** `CreateTaskPushNotificationConfig` 只校验「`url` 是 `http`/`https` 且 host 非空」，**不**在写入时拒绝非白名单 host（`curl` 证据 §4.16.6 #11 实测 `http://10.0.0.5/hook` 写入成功）。真正的白名单交集、私网判定、pin IP 都在 worker 的 `prepare` 里做，失败时该条投递被跳过并记日志（`a2a push skipped ... {guard_refused, host_not_allowlisted}`），**不影响任务终态**。这是有意的：配置是声明，投递才是出站动作。

### 4.16.2 SSE 帧与关流语义

| 关注点 | 行为 |
|---|---|
| 响应头 | `content-type: text/event-stream`、`transfer-encoding: chunked`、`cache-control: no-cache`、`connection: keep-alive` |
| 数据帧 | 每事件一帧 `data: <json-rpc-response>\n\n`；`json` 是完整 JSON-RPC 2.0 Response（含 `id`），`result` 里是 `task`/`statusUpdate`/`artifactUpdate`/`message` 分支 |
| 心跳 | 空闲超过 `sse_heartbeat_ms`（默认 15000，证据节点 3000）发一行 `: ping`（SSE 注释，客户端忽略） |
| 关流条件 | ①转发完终态/中断态事件；②到达 `sse_max_duration_ms`（模块默认 600000，出厂 `sys.config` 配 1800000）；③历史 offset 越界（被 retention GC）→ 先发一个 `status=failed` 事件再关 |
| 订阅语义 | **不建 SubRef、不 pin retention**（ADR-025 §D7）。跟随走分区邮箱钩子，落后靠「重订阅从 offset 0 重放」达成等价可用性，不让永不结束的订阅抬高分区水位 |
| 重放 | `SubscribeToTask` 先 `drain` `From` 起的历史事件（`seq` 单调），再 `await` 新事件 |

### 4.16.3 Push 配置行与投递链路

`push_config` 行（10 元组，主键 `{TenantId, TaskId, ConfigId}`）：

| 列 | 值 |
|---|---|
| `{TenantId, TaskId, ConfigId}` | 主键；`ConfigId` 服务端生成 |
| `Url` | binary，`http`/`https` |
| `AuthScheme` | `undefined`（不带认证）或 scheme 名（须在 `push_auth_secrets` 里有对应密钥） |
| `AllowPrivate` | 该配置是否允许连接私网/loopback（`push_tenant_allowlist` 的条目决定） |
| `UpdatedAt` / `Version` | upsert 时刷新、`Version` 单调 +1 |

投递链：

```
a2a_task_shard:maybe_push/2（终态/中断态挂点，cast，不等）
  └─► a2a_push_worker:prepare/4
        ├─► a2a_push:guard/3        allowlist 交集 → validate_url → resolve → is_private_address
        ├─► a2a_push_auth:headers/3 X-A2A-Timestamp / X-A2A-Nonce / X-A2A-Task-Id / X-A2A-Config-Id
        │                           + 声明 scheme 时：Authorization: Bearer … 或 X-A2A-Signature: sha256=<hex>
        └─► a2a_push:deliver/3      连已校验 IP；只读状态行；3xx 即失败；体 ≤ 64 KiB
              └─► 失败 → 退避重试 → 耗尽/过期 → write_dlq → $a2a/push/dlq/<tenant>（普通 #broker_message{}）
```

- **签名口径**：`X-A2A-Signature: sha256=<hex>` 是 HMAC-SHA256(secret, `timestamp ‖ "." ‖ nonce ‖ "." ‖ body`)，`secret` 取 `push_auth_secrets[scheme].secret`。
- **scheme 是配置键名，不是自由字符串**：客户端声明了一个 `push_auth_secrets` 里没有的 scheme → 该条投递被跳过（`{auth_not_configured, not_configured}`），**不是**回落到无签名投递（`curl` 证据 §4.16.6 前的诊断即此行为）。
- **DLQ 消息形状**：`destination = <<"$a2a/push/dlq/", Tenant/binary>>`、`message_id = TaskId`（任务即失败主体）、`payload_encoding = json`、`reliability = fsync`，payload 是 `{taskId, configId, url, attempts, reason, _meta.unified-mq.kind = pushDlq}`。它是**可订阅、可审计的普通消息**，与 `$dlq/...` 同一手法；写失败只 `logger`，不抛。

### 4.16.4 错误映射（新增部分）

| 情形 | HTTP | JSON-RPC code |
|---|---|---|
| `push_enabled=false` 时的 Push 四方法 | **400** | `-32003` `push_notification_not_supported` |
| `SubscribeToTask` / Push 读删的 Task/config 不存在或跨租户 | **404** | `-32001` `task_not_found` |
| 其余（信封/版本/凭据/media type/`-32602`） | 同 §4.14 / §4.15 | 同 §4.14 / §4.15 |

### 4.16.5 证据

| 类型 | 位置 | 规模 |
|---|---|---|
| CT（流式 + Push + guard） | `apps/broker_proto_a2a/test/a2a_stream_push_SUITE.erl` | **20 例**（新套件）：`stream` 7（流式建 Task + 终态关流 / 帧是 JSON-RPC Response / `SubscribeToTask` offset 0 重放 / 未知任务 404 / 心跳保活 / 中断态关流 / 慢读者不 pin retention）、`push` 5（禁用回 `-32003` / CRUD 往返 / 配置租户隔离 / 终态事件投递 / 重试后写 DLQ）、`guard` 8（私网拒绝 / 空 allowlist 拒绝 / 越权 host 拒绝 / 非 https 拒绝 / 重定向拒绝 / 显式 `allow_private` 放行 / 签名头存在且 HMAC 正确 / 未配置 auth scheme 拒绝） |
| CT（控制面） | `apps/broker_meta/test/broker_meta_SUITE.erl` | **12 → 13 例**：冻结表断言改为 13 表；新增 `push_config_crud_and_tenant_isolation`（CRUD + version 单调 + 跨租户不可见） |
| 全量回归 | `rebar3 ct` | **973/973、63 套件**（基线 952/62，零 FAILED；净增 = 新套件 20 + `broker_meta_SUITE` +1） |
| 静态门禁 | `rebar3 dialyzer` / `rebar3 xref` / 两种 release | dialyzer **0 warning**；xref 除既有 unused-export 外无新告警；`rebar3 release` 与 `rebar3 as quic release` 均成功，`lib/` 含 `a2a_push`/`a2a_push_auth`/`a2a_push_worker`/`a2a_stream` |
| 真实客户端 | `curl`（下节原文） | **15 条**请求/响应原文 + 1 个本地 webhook receiver 收到的 POST |

### 4.16.6 `curl` 实测（2026-11-12，证据节点 127.0.0.1:18094 + webhook receiver 127.0.0.1:18095/18096）

节点配置：`task_shards => 4`、`task_reliability => fsync`、`task_auto_complete => false`、`sse_heartbeat_ms => 3000`、`push_enabled => true`、`push_allow_http => true`、`egress_allowlist => [#{host => <<"127.0.0.1">>, allow_private => true}]`、`push_tenant_allowlist => #{<<"t1">> => [#{host => <<"127.0.0.1">>, allow_private => true}]}`、`push_auth_secrets => #{<<"hmac-sha256">> => #{secret => <<"evidence-secret">>, header => none}}`、`push_max_attempts => 2`、`push_base_backoff_ms => 50`、`push_ttl_ms => 30000`，用户 `s9c-evidence`（tenant `t1`）。receiver 1 固定回 200、receiver 2 固定回 500。

```
$ curl -sS -N -D - -X POST http://127.0.0.1:18094/ \
    -H 'Content-Type: application/json' -H 'A2A-Version: 1.0' \
    -H 'Authorization: Bearer s9c-evidence-token' \
    -d '{"jsonrpc":"2.0","id":2,"method":"SendStreamingMessage","params":{"message":{
         "messageId":"m-sse","role":"ROLE_USER","contextId":"ctx-sse",
         "parts":[{"text":"stream me"}]}}}'
HTTP/1.1 200 OK
transfer-encoding: chunked
connection: keep-alive
content-type: text/event-stream
cache-control: no-cache

data: {"id":2,"jsonrpc":"2.0","result":{"_meta":{"unified-mq":{"kind":"task","seq":1}},
  "task":{"contextId":"ctx-sse","history":[{"contextId":"ctx-sse","messageId":"m-sse",
  "parts":[{"text":"stream me"}],"role":"ROLE_USER","taskId":"aFs2j6HItBT957-tSHelsA"}],
  "id":"aFs2j6HItBT957-tSHelsA","status":{"state":"TASK_STATE_SUBMITTED",
  "timestamp":"2026-11-12T03:23:24.993Z"}}}}

: ping

: ping
```

```
$ 本地 webhook receiver 在 CancelTask 后收到的 POST（receiver 1，200）：
POST /hook-ok
x-a2a-config-id: TxPb2ANVDxDx
x-a2a-nonce: +1z0xuCxhY3G4chNaCfcfw==
x-a2a-signature: sha256=1cec33692259a207ce0fcfc6b93829afed2e302be91cc1168864e9760dfc2eb0
x-a2a-task-id: UV-BCkhOYdHpPkCTYmkkZA
x-a2a-timestamp: 1794453819547
body: {"_meta":{"unified-mq":{"kind":"statusUpdate","seq":2}},"statusUpdate":{"contextId":"ctx-s9c",
       "final":true,"status":{"state":"TASK_STATE_CANCELED","timestamp":"..."},"taskId":"..."}}
```

| # | 请求 | 响应 / 观测 |
|---|---|---|
| 1 | `SendMessage`（`task_auto_complete=false`） | `200`，`result.task.status.state = TASK_STATE_SUBMITTED` |
| 2 | `SendStreamingMessage` | `200` + `text/event-stream`，一帧 `data:`（`kind=task`、`seq=1`）后跟 `: ping` 心跳（上） |
| 3 | `SubscribeToTask <id>`（已终态/无新事件） | 首帧 `data:` 是 offset 0 的**历史重放**（与 #1 的 Task 逐字段一致），随后 `: ping` |
| 4 | `SubscribeToTask` 未知 id | **`404`** `{"error":{"code":-32001,"message":"Task not found"}}` |
| 5 | `CreateTaskPushNotificationConfig`（`url=…:18095/hook-ok`、`scheme=hmac-sha256`） | `200`，服务端生成 `id = TxPb2ANVDxDx`，回显 `taskId`/`tenant`/`url`/`authentication` |
| 6 | `GetTaskPushNotificationConfig` | `200`，与 #5 的 config 逐字段相同 |
| 7 | `ListTaskPushNotificationConfigs` | `200`，`result.configs` 含该条 |
| 8 | `CancelTask` | `200`，`status.state = TASK_STATE_CANCELED` |
| 9 | （异步）webhook POST | receiver 收到 `POST /hook-ok`：`x-a2a-*` 头齐全（含 HMAC 签名），body 是 `statusUpdate`，`final=true`（上） |
| 10 | `DeleteTaskPushNotificationConfig` | `200`，`result = {}` |
| 11 | `CreateTaskPushNotificationConfig`（`url=http://10.0.0.5/hook`，不在白名单） | `200` —— **配置写入不校验**，SSRF 校验在投递路径（§4.16.1） |
| 12 | `CancelTask` 该 Task | `200`；节点日志 `a2a push skipped t1/…: {guard_refused,host_not_allowlisted}`——**从未拨号** |
| 13 | `CreateTaskPushNotificationConfig`（`url=…:18096/hook-fail`，receiver 固定回 500）→ `CancelTask` | `200`；receiver 收到 **2** 次 `POST /hook-fail`（`push_max_attempts=2`），两次签名头相同 |
| 14 | `a2a_push_worker:stats/0`（同节点 rpc） | `#{pending => 0, dropped => 1, failed => 1, delivered => 0}`（1 次失败 + 1 次耗尽丢弃） |
| 15 | 同节点读取 `$a2a/push/dlq/t1` | 恰一条，`partition=2 offset=3`，`message_id = <taskId>`，payload `{"_meta":{"unified-mq":{"kind":"pushDlq"}},"attempts":2,"configId":"02RdUqVf_l88","reason":"{unexpected_status,500}","taskId":"…","url":"127.0.0.1"}` |

### 4.16.7 已声明的限制 / 边界

- **Push 默认关闭**：`push_enabled => false` 是出厂值；关闭时推送四方法回 `-32003`。`push_allow_http => true` 只为本地 receiver 证据，生产应保持 `https` 且 `egress_allowlist` 非空。
- **scheme 必须是运维配好的键**：客户端声明未配置的 scheme 会被**跳过**（`auth_not_configured`），不降级为明文投递。
- **DLQ 无消费者进程**：耗尽后写进 `$a2a/push/dlq/<tenant>` 即结束；谁订阅谁负责，Broker 不自动重投。
- **SSE 无中间快照 resync**：目前靠「重订阅 offset 0 重放」；`sse_max_duration_ms` 到点关流，客户端自行重连。
- **仍是单节点**：`SubscribeToTask`/Push 都在 Task 的 owner 节点（本地 shard ETS）；跨节点访问是 S9-C+ 演进位。
- **失败路径计数器仍未加**：`a2a_push_dlq` / `a2a_stream_*` 等键**不加**（S9-F 定稿，见 `docs/security.md` §12.1 第 17 条）；S9-C 只 `logger`。
- **默认仍不监听**：`listeners => []`。18094/18095/18096 只是本次证据端口。

---

## 4.17 S9-D 追加（2026-11-12）：MCP 2026-07-28 前端（Broker 作网关）

**是什么**：`broker_proto_mcp` 把 **MCP `2026-07-28`** 接到统一消息内核，形态是**网关**而不是 MCP Server 执行体：

1. **原语透传**（ADR-027 §D2）：`tools/*`、`resources/*`、`prompts/*`、`completion/complete` 的请求原文发布到 `$mcp/<method>`，后端把结果发布到网关注入的 `$mcp/reply/<tenant>/<token>`；网关只做「门控 → 发布 → 取回 → 补客户端 id」。**Broker 不解释工具/资源/prompt 的语义**（R-B1）。
2. **本地方法**（ADR-027 §D3/§D5）：`server/discover` 由配置（`features`）直接组装；`subscriptions/listen` 映射成内核分区上的瞬态读者，转发 `notifications/<name>`。
3. **两个前端共用一个协议层**：`mcp_dispatch`（信封/`_meta`/路由）、`mcp_route`（method→feature）、`mcp_discover`、`mcp_sub`（listen filter）、`mcp_stream`（call/listen 的流循环）与传输无关；HTTP（`mcp_rpc`/`mcp_conn`/`mcp_listener`，SSE sink）与 stdio（`mcp_stdio`，newline sink）只差成帧与长活订阅的宿主。**无状态**：没有 `initialize` 握手、没有 `Mcp-Session-Id`、没有 GET 流、没有 `Last-Event-ID` 续传；版本与能力逐请求走 `_meta`。

### 4.17.1 方法能力矩阵

| 方法 | 状态 | 语义要点 |
|---|---|---|
| `server/discover` | **本地实现** | 返回 `resultType`、`supportedVersions`（`2026-07-28`）、`capabilities`（由 `features` 推导）、`_meta.io.modelcontextprotocol/serverInfo`、`ttlMs`（`discover_ttl_ms`，默认 3600000）、`cacheScope: public`；`instructions` 仅在配置时输出。不发布到内核 |
| `subscriptions/listen` | **本地实现（长活流）** | 第一帧固定 `notifications/subscriptions/acknowledged`（`subscriptionId` = 请求 id），其后转发客户端订阅的通知；`sse_max_duration_ms` 到点回成功 response（`resultType: complete`）再关流（Graceful Closure） |
| `tools/list`、`tools/call` | **透传（`tools` feature）** | `call` 的 `Mcp-Name` 映 `params.name` |
| `resources/list`、`resources/templates/list`、`resources/read` | **透传（`resources`）** | `read` 的 `Mcp-Name` 映 `params.uri` |
| `prompts/list`、`prompts/get` | **透传（`prompts`）** | `get` 的 `Mcp-Name` 映 `params.name` |
| `completion/complete` | **透传（`completions`）** | — |
| `initialize` | **不支持** | 2026-07-28 已无握手；按未知方法 → **404 + `-32601`** |
| `sampling/*`、`elicitation/*` | **明确拒绝** | server→client 方向；→ **404 + `-32601`**，`data.reason` 为 `"sampling is not supported"` / elicitation 同形（R-B2） |
| 其余任何 method | **不支持** | **404 + `-32601`** |
| 未广告 feature 下的方法 | **门控** | **404 + `-32601`**，`data = {reason:"feature not advertised", feature:<name>}`（见 §4.17.7 #25；ADR-027 原文写 200，落地为 404，见执行计划偏差 2） |

### 4.17.2 Streamable HTTP 门禁顺序与错误映射

单端点（`path`，默认 `/`），**仅 POST**；固定顺序、先传输后信封：

| 顺序 | 门禁 | 失败响应 |
|---|---|---|
| 1 | `Origin`（若存在）必须是 loopback 或 `allowed_origins` 之一 | **403**，**无 body** |
| 2 | `Accept` 必须同时含 `application/json` 与 `text/event-stream`（或 `*/*`） | **406**，无 body |
| 3 | 方法必须是 POST（GET/DELETE 等） | **405** + `Allow: POST` |
| 4 | `Content-Type` 基类型必须是 `application/json` | **415**，无 body |
| 5 | `MCP-Protocol-Version` 必填且 == body `_meta` 版本 | **400 + `-32020`**（缺失与不等同判） |
| 6 | `Mcp-Method` 必填且 == body `method` | **400 + `-32020`** |
| 7 | `Mcp-Name` 必填（仅 `tools/call`/`resources/read`/`prompts/get`）且 == body 值（`=?base64?…?=` 哨兵先解码） | **400 + `-32020`** |
| 8 | 认证（`broker_security` 不透明 bearer token） | **401** + `WWW-Authenticate: Bearer realm="mcp"`，无 body |
| 9 | 信封（`jsonrpc`/`method`/`id`；`id` 必须是 string 或 integer，**`null` 非法**） | **400 + `-32700`/`-32600`** |
| 10 | `_meta` 必填：`protocolVersion` 在 `supportedVersions` 内，否则 `-32022`；`clientCapabilities` 必填，缺 → `-32602` | **400** |
| 11 | 路由：未知/未广告/`sampling`/`elicitation` | **404 + `-32601`** |
| 12 | 参数校验（listen 的 `notifications` 必须是对象等） | **400 + `-32602`** |

HTTP 状态映射是 MCP 局部覆盖（不改 `agent_rpc_error` 的 A2A 语义）：`method_not_found → 404`，`invalid_params`/`invalid_request`/`parse_error → 400`，其余走 `agent_rpc_error:http_status/1`（`internal_error → 500`、`unsupported_protocol_version → 400`）。**通知 POST**（无 `id`）不校验 5–7 三个镜像头（spec 未定义），过 Origin/Accept/Content-Type/认证后回 **202 无 body**。

### 4.17.3 一次透传调用的总线协议

```
客户端 ──POST tools/call──► mcp_rpc（门禁）──► mcp_dispatch:route
   └─ 生成 token / replyTo = $mcp/reply/<tenant>/<token>
   └─ 记录 reply 分区的 tail（发布前）为读起点
   └─ 发布 $mcp/tools/call：payload = 客户端 JSON-RPC 原文 + _meta.unified-mq = {replyTo, requestToken, tenantId}
        └─► 后端（任意语言）：读该 destination，把结果发布到 replyTo
              _meta.unified-mq.kind = rpcResult（缺省）→ 终止帧 {"result":…} 或 {"error":{…}}
                                     = progress   → notifications/progress（token 取请求 _meta.progressToken）
                                     = notification → notifications/<payload.method>
   └─ mcp_stream：瞬态读者（subscribe 分区邮箱 + read_from/3，不建 SubRef、不 pin retention）
        终止帧 → 覆盖客户端 id → 关流；request_timeout_ms 到点 → -32603 + data.reason="timeout"
```

- 请求消息是普通 `#broker_message{}`：`payload_encoding = json`、`persistence = durable`、`reliability = request_reliability`（默认 `fsync`）、分区由 `broker_router_hash:route/3` 求得；`headers` 里另放 `reply_to`/`tenant_id`/`request_token` 供 Erlang 消费者免解析 JSON。
- **SSE 响应头**：`content-type: text/event-stream`、`transfer-encoding: chunked`、`cache-control: no-cache`、`x-accel-buffering: no`；每个 `data:` 帧是一个完整 JSON-RPC 对象（进度/通知是 Notification，终止帧是带客户端 `id` 的 Response）。
- 后端回什么 `id` 都不影响关联：网关在终止帧上用客户端自己的 `id` 覆盖。
- `server/discover` **不发布**，直接单帧 `application/json` 200（不是 SSE）。

### 4.17.4 `subscriptions/listen`

`params.notifications` 的字段映射到内核目的地（`mcp_sub:plan/1`）：

| 请求字段 | 内核目的地 | 转发方法 |
|---|---|---|
| `toolsListChanged: true` | `$mcp/notify/tools/list_changed` | `notifications/tools/list_changed` |
| `promptsListChanged: true` | `$mcp/notify/prompts/list_changed` | `notifications/prompts/list_changed` |
| `resourcesListChanged: true` | `$mcp/notify/resources/list_changed` | `notifications/resources/list_changed` |
| `resourceSubscriptions: [uri]` | `$mcp/notify/resources/updated/<uri>` | `notifications/resources/updated`（`params.uri` 由目的地回填） |

- **第一帧必须是 ack**，`_meta.io.modelcontextprotocol/subscriptionId = <listen 请求 id>`；`notifications` 字段只回显网关**实际同意**的子集（未广告/无法路由的项被丢掉，ack 里不出现）。
- 之后每条转发的通知都带同一 `subscriptionId`；后端发布到通知 destination 的 **payload 就是通知 params 对象**（网关只补 `_meta.subscriptionId`，不重写 payload）。
- 关流条件：客户端断开 / 到达 `sse_max_duration_ms`（回 `resultType: complete` 的成功 response 再关）/ 单帧超 `sse_max_event_bytes`（先发 `-32603` 再关）。空闲期按 `sse_heartbeat_ms` 发 `: ping` 注释行。
- **瞬态读者**：`broker_queue:subscribe/2` + `read_from/3`，读起点是订阅前的分区 tail；不建 SubRef、不写 cursor、不 pin retention（承 ADR-025 §D7）。

### 4.17.5 stdio 前端

- **成帧**：一行一条 JSON-RPC（`io:get_line/1`，trim 后按行解码）；`stdout` 只用 `io:put_chars(standard_io, …)` 写协议帧；坏 JSON 回 `-32700`，**不回**任何非协议文本。
- **身份**：stdio 没有 header/凭据通道，固定 `agent_rpc_auth:anonymous()`（默认租户，默认 `t0`）；信任边界是「谁能启动这个进程」。
- **长活订阅**：`subscriptions/listen` 在 **子进程**（`spawn_link`）里跑 `mcp_stream:run_stdio/2`，与主读循环共享 stdout；主循环继续读其他行。`notifications/cancelled`（`params.requestId`）按 listen 请求 id 给子进程发 `mcp_cancel`，由子进程的 `after` 子句退订。
- **EOF** = 优雅退出：主循环 `shutdown/1` 给所有订阅子进程发取消信号后返回。
- **stdout 纯净性由部署保证**：默认 `logger` handler 写 `standard_io`；生产 stdio 部署必须把 `default` handler 换到 `standard_error`（OTP 不允许原地改 `type`，需 `remove_handler` + `add_handler`）。本 app **不**自建 handler，也不在启动时校验（偏差 13）。

### 4.17.6 证据

| 类型 | 位置 | 规模 |
|---|---|---|
| CT（HTTP） | `apps/broker_proto_mcp/test/mcp_listener_SUITE.erl` | **32 例**：`discover` 6（本地回答 / capabilities 随 features / instructions+缓存提示 / 缺版本 / 版本不支持 / 缺 clientCapabilities）、`headers` 13（401 / 405 / 错 path 404 / Origin 403 / loopback 放行 / Accept 406 / Content-Type 415 / 缺版本头 / 方法头不匹配 / name 必填与校验 / base64 哨兵 / 坏 JSON / 通知 202）、`call` 9（往返 / 进度+结果 / 透传通知 / 后端 error / 超时 / name 头 / 未知方法 404 / 未广告 404 / sampling 拒绝）、`stream` 4（ack-first / tools list_changed / resource updated / 时长上限优雅关流），全部 `port => 0` |
| CT（stdio） | `apps/broker_proto_mcp/test/mcp_stdio_SUITE.erl` | **10 例**：`protocol` 8（discover / 未知方法 / 坏 JSON / 通知无输出 / 缺版本 / 调用往返 / ACL 拒绝 / EOF 退出）、`subscription` 2（ack + 转发 / cancel 结束子进程） |
| 全量回归 | `rebar3 ct` | **1015/1015、65 套件**（基线 973/63；净增 = 32 + 10，零 FAILED） |
| 静态门禁 | `rebar3 dialyzer` / `rebar3 xref` / 两种 release | dialyzer **0 warning**；xref 除既有 unused-export 外无新告警；`rebar3 release` 与 `rebar3 as quic release` 均成功，`lib/` 含 `broker_proto_mcp-0.1.0` |
| 真实请求 | `curl` + 一个假后端进程（读 `$mcp/<method>`、按协议回 progress/result 与通知） | **HTTP 24 条** + **stdio 6 行**（下节原文） |

### 4.17.7 `curl` 实测（2026-11-12，证据节点 `127.0.0.1:18096`）

节点配置：`listeners => [#{port => 18096}]`、`features => [tools, resources, prompts, completions]`、`require_auth => true`、`sse_heartbeat_ms => 3000`、`request_timeout_ms => 5000`、`sse_max_duration_ms => 4000`；用户 `mcp-evidence`（tenant `t1`，token `mcp-evidence-token`）。公共请求头为 `Content-Type: application/json`、`Accept: application/json, text/event-stream`、`MCP-Protocol-Version: 2026-07-28`、`Authorization: Bearer mcp-evidence-token`，body 的 `_meta` 带 `protocolVersion` 与 `clientCapabilities`。

```
$ curl -sS -D - -X POST http://127.0.0.1:18096/ \
    -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
    -H 'MCP-Protocol-Version: 2026-07-28' -H 'Mcp-Method: server/discover' \
    -H 'Authorization: Bearer mcp-evidence-token' \
    -d '{"jsonrpc":"2.0","id":"d1","method":"server/discover","params":{"_meta":{…}}}'
HTTP/1.1 200 OK
connection: keep-alive
content-length: 295
content-type: application/json

{"id":"d1","jsonrpc":"2.0","result":{"_meta":{"io.modelcontextprotocol/serverInfo":{"name":"unified-mq","version":"0.1.0"}},"cacheScope":"public","capabilities":{"completions":{},"prompts":{},"resources":{},"tools":{}},"resultType":"complete","supportedVersions":["2026-07-28"],"ttlMs":3600000}}
```

```
$ curl -sS -N -D - -X POST http://127.0.0.1:18096/ \
    -H 'Mcp-Method: tools/call' -H 'Mcp-Name: echo' \
    -H '…公共头…' \
    -d '{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"echo",
         "arguments":{"text":"hi"},"_meta":{…,"progressToken":"p-1"}}}'
HTTP/1.1 200 OK
transfer-encoding: chunked
connection: keep-alive
content-type: text/event-stream
cache-control: no-cache
x-accel-buffering: no

data: {"jsonrpc":"2.0","method":"notifications/progress","params":{"message":"backend working on tools/call","progress":1,"progressToken":"p-1","total":2}}

data: {"id":7,"jsonrpc":"2.0","result":{"_meta":{"io.modelcontextprotocol/serverInfo":{"name":"unified-mq","version":"0.1.0"}},"content":[{"text":"backend echo: tools/call","type":"text"}],"isError":false}}
```

```
$ curl -sS -N http://127.0.0.1:18096/ … 'Mcp-Method: subscriptions/listen' \
    -d '{"jsonrpc":"2.0","id":"sub-1","method":"subscriptions/listen","params":{
         "notifications":{"toolsListChanged":true},"_meta":{…}}}'
data: {"jsonrpc":"2.0","method":"notifications/subscriptions/acknowledged","params":{"_meta":{"io.modelcontextprotocol/subscriptionId":"sub-1"},"notifications":{"toolsListChanged":true}}}

# 另起一条连接触发 tools/call(name=emit-list-change)，假后端发布 $mcp/notify/tools/list_changed：
data: {"jsonrpc":"2.0","method":"notifications/tools/list_changed","params":{"_meta":{"io.modelcontextprotocol/subscriptionId":"sub-1"},"reason":"tools changed"}}

: ping
```

```
# sse_max_duration_ms=4000：到点先回成功 response，再关流（Graceful Closure）
data: {"jsonrpc":"2.0","method":"notifications/subscriptions/acknowledged","params":{"_meta":{"io.modelcontextprotocol/subscriptionId":"sub-2"},"notifications":{"promptsListChanged":true,"toolsListChanged":true}}}

: ping

data: {"id":"sub-2","jsonrpc":"2.0","result":{"_meta":{"io.modelcontextprotocol/subscriptionId":"sub-2"},"resultType":"complete"}}
```

**stdio 会话**（同一节点配置改成 `stdio_enabled => true`、`listeners => []`，输入 6 行，stdout 原文）：

```
{"id":"s1","jsonrpc":"2.0","result":{"_meta":{"io.modelcontextprotocol/serverInfo":{"name":"unified-mq","version":"0.1.0"}},"cacheScope":"public","capabilities":{"completions":{},"prompts":{},"resources":{},"tools":{}},"resultType":"complete","supportedVersions":["2026-07-28"],"ttlMs":3600000}}
{"jsonrpc":"2.0","method":"notifications/subscriptions/acknowledged","params":{"_meta":{"io.modelcontextprotocol/subscriptionId":"s2"},"notifications":{"toolsListChanged":true}}}
{"jsonrpc":"2.0","method":"notifications/tools/list_changed","params":{"_meta":{"io.modelcontextprotocol/subscriptionId":"s2"},"reason":"tools changed"}}
{"id":"s3","jsonrpc":"2.0","result":{"_meta":{"io.modelcontextprotocol/serverInfo":{"name":"unified-mq","version":"0.1.0"}},"content":[{"text":"backend echo: tools/call","type":"text"}],"isError":false}}
{"error":{"code":-32601,"message":"Method not found"},"id":"s4","jsonrpc":"2.0"}
{"error":{"code":-32700,"message":"Parse error"},"id":null,"jsonrpc":"2.0"}
```

输入的 6 行依次是：`server/discover`（`s1`）、`subscriptions/listen`（`s2`，`toolsListChanged`）、`tools/call name=emit-list-change`（`s3`）、`notifications/cancelled requestId=s2`、未知方法（`s4`）、坏 JSON。stdout **只有上面 6 个协议帧**；同一次运行的日志在 **stderr**（`STDIO_READY`、`mcp evidence backend up …`、`SIGTERM received`），证实「stdout-only」在正确配置 logger 后成立。

| # | 请求 | 响应 / 观测 |
|---|---|---|
| 1 | `server/discover` | `200`，`capabilities` 四对象齐全、`ttlMs=3600000`、`cacheScope=public`（上） |
| 2 | `tools/call` + `progressToken` | `200 text/event-stream`；先 `notifications/progress`，后终止帧 `result`（上） |
| 3 | `tools/nope`（未知方法） | **`404`** `-32601` `{"error":{"code":-32601,"message":"Method not found"}}` |
| 4 | `initialize` | **`404`** `-32601`（无握手） |
| 5 | `sampling/createMessage` | **`404`** `-32601`，`data.reason="sampling is not supported"` |
| 6 | 缺 `MCP-Protocol-Version` 头 | **`400`** `-32020` `MCP-Protocol-Version header is required` |
| 7 | `MCP-Protocol-Version: 2025-06-18` vs body `2026-07-28` | **`400`** `-32020` `…does not match the request body` |
| 8 | 头与 body 同为 `2025-06-18` | **`400`** `-32022`，`data={supported:["2026-07-28"],requested:"2025-06-18"}` |
| 9 | `Mcp-Name: other` vs body `name=echo` | **`400`** `-32020` `Mcp-Name header does not match the request body` |
| 10 | body `_meta` 缺 `clientCapabilities` | **`400`** `-32602`，`data.reason="missing _meta.io.modelcontextprotocol/clientCapabilities"` |
| 11 | `id: null` | **`400`** `-32600`（MCP 的 `id` 不接受 `null`） |
| 12 | 无 `Authorization`（头/体一致） | **`401`**，`www-authenticate: Bearer realm="mcp"`，无 body |
| 13 | `Accept: application/json`（无 SSE） | **`406`**，无 body，`connection: close` |
| 14 | `GET /` | **`405`** + `allow: POST`，无 body |
| 15 | `Origin: http://evil.example` | **`403`**，无 body（DNS-rebinding 防御） |
| 16 | `Content-Type: text/plain` | **`415`**，无 body |
| 17 | 通知 POST（`notifications/cancelled`） | **`202`**，`content-length: 0` |
| 18 | 错误 token | **`401`**，与 #12 同形 |
| 19 | 坏 JSON（请求头未带版本，先到解析） | **`400`** `-32700` `{"error":{"code":-32700,"message":"Parse error"},"id":null,"jsonrpc":"2.0"}` |
| 20 | `subscriptions/listen`（`toolsListChanged`） | `200 text/event-stream`：ack → 通知 → `: ping`（上） |
| 21 | 触发 `tools/call name=emit-list-change` | 自己的流：一帧终止 `result`（无 progressToken 时无进度帧） |
| 22 | 同一 listen（`sse_max_duration_ms=4000`） | ack → `: ping` → `resultType: complete` 后关流（上） |
| 23 | stdio 会话（6 行） | stdout 6 个协议帧、日志全在 stderr（上） |
| 24 | `features => []` 时 `server/discover` | `200`，`capabilities: {}`（诚实空） |
| 25 | `features => []` 时 `tools/list` | **`404`** `-32601`，`data={reason:"feature not advertised",feature:"tools"}` |

### 4.17.8 已声明的限制 / 边界

- **默认零端口**：`listeners => []`、`stdio_enabled => false`。18096 只是本次证据端口；8082 是设计示例，不由本 app 出厂占用。
- **不做 MCP Server 的活**：工具参数校验、资源内容、prompt 模板、`sampling`/`elicitation` 回调都在网关之外（R-B1/R-B2）。
- **无状态**：没有 `initialize`、会话 id、GET 流、断点续传；每个请求自带版本与客户端能力。
- **通知 POST 的镜像头不校验**：spec 未定义，网关只做传输/认证门禁后回 202。
- **`Mcp-Param-*`（源自 `x-mcp-header`）不强制**：网关没有后端 `inputSchema`，无法判断这些头是否该出现；能力矩阵按「accepted, not enforced」记录。
- **SSE 只有一个时长上限**：`sse_max_duration_ms`；没有 pending-bytes / send-timeout 闸门，慢客户端靠 socket 缓冲自然反压（ADR-025 §D7 的 `sse_max_pending_bytes` 未落地）。
- **stdio 不做认证**：身份固定为匿名（默认租户）；进程访问权即凭据。stdout 纯净性依赖部署把 logger 指向 stderr。
- **单节点**：reply 与 listen 都读 owner 节点的本地分区，跨节点 reply/订阅不支持（与 A2A Task 同一限制）。
- **无新增指标键**：失败只 `logger`（S9-F 定稿不加计数器，见 `docs/security.md` §12.1 第 17 条）。
- **真实 MCP 客户端证据见 §4.17.9**（`test/protocol-compatibility/s9f/mcp/`，官方 `mcp 2.3.0`）：能力协商 / 原语透传 / `subscriptions/listen` 三段的 SDK 侧原文。

---

### 4.17.9 官方 MCP SDK 客户端矩阵（S9-F，2026-11-12）

**是什么**：`test/protocol-compatibility/s9f/mcp/mcp_sdk_client.py`，用上游官方 Python SDK
`mcp 2.3.0`（dist 元数据 `LATEST_PROTOCOL_VERSION = 2026-07-28`，与 Broker 实现的版本一致）当
**真实第三方 MCP 客户端**，跑证据节点 `127.0.0.1:18096`（`features => [tools, resources, prompts,
completions]`、`require_auth => true`、用户 `mcp-evidence`/tenant `t1`）。原文输出：
[`test/protocol-compatibility/s9f/mcp/sdk_mcp.out`](../test/protocol-compatibility/s9f/mcp/sdk_mcp.out)。
三阶段分别对应 §4.17.1 的「本地方法」「原语透传」「长活流」。

| SDK 阶段 | 调用 | 官方 SDK 观测 | 落到本实现的哪一条 |
|---|---|---|---|
| A 能力协商 | `Client(mode="auto")` → SDK 自探一次 `server/discover` 并 adopt | `protocol_version=2026-07-28`；`server_info: name='unified-mq', version='0.1.0'`；`capabilities={prompts:{}, resources:{}, tools:{}, completions:{}}`；`instructions: None`；随后 `list_tools()` 解析出 `['emit-list-change']` | §4.17.1 `server/discover` 本地回答 + `_meta.io.modelcontextprotocol/serverInfo`（**由 SDK 从 Broker 的 discover 回复解析，非自报**） |
| B 透传调用 | `mode="2026-07-28"` 直接 adopt（2026 无握手），`list_tools` / `list_resources` / `list_prompts` | 每个结果都带 `_meta.io.modelcontextprotocol/serverInfo`、`ttl_ms=3600000`、`cache_scope=public`、`result_type=complete`；`resources/prompts` 为空数组 | §4.17.3 一次透传调用的总线协议（补 `serverInfo` 的 `with_server_info/1`） |
| B 进度 + 结果 | `call_tool("emit-list-change", {"topic": ...}, progress_callback=…)` | 先 `notifications/progress`（`progress=1.0/2.0`），再终止 `result`（`content[0].text="backend echo: tools/call"`、`is_error=false`） | §4.17.1 `tools/call` 透传（`notifications/progress` 取请求 `_meta.progressToken`） |
| B 未知工具名 | `call_tool("does-not-exist", {})` | SDK 打印 `Tool does-not-exist not listed by server, cannot validate any structured content`，**随后透传成功** | **R-B1 的实测证明**：网关不校验工具名，任意 name 原样发布到 `$mcp/tools/call`，由后端决定；假后端对任意名字都回包，这正是透传网关该有的行为 |
| C 长活流 | `client.listen(tools_list_changed=True)` | `honored: tools_list_changed=True prompts_list_changed=None resources_list_changed=None resource_subscriptions=None`（只回显网关同意的子集）；另一客户端触发 `emit-list-change` 后本流收到 `ToolsListChanged()` 事件；客户端退出时流被 Broker 优雅关闭 | §4.17.4 `subscriptions/listen`（ack-first、honored 子集、转发 `notifications/tools/list_changed`、Graceful Closure） |

**2026-07-28 无状态在 SDK 侧的两种读法**（都要如实记录）：

- `mode="2026-07-28"`（pin）：SDK 不探测，直接 adopt 一个最小 discover 结果，所以进 `__aenter__`
  时 `server_capabilities={}`、`server_info=None`。这不是 Broker 没给能力，而是**该版本协议把
  `serverInfo` 标记成可选**，SDK 对「没探过」的会话如实报空——真正的协商证据在阶段 A 的
  `mode="auto"`。
- `mode="auto"`：SDK 真的发一次 `server/discover`、解析回复并 adopt，能力矩阵（tools/resources/
  prompts/completions 四个对象、无 `list_changed`/`subscribe` 子能力）即 Broker `features` 配置的
  忠实投影。

**版本一致性**：`assert LATEST_PROTOCOL_VERSION == "2026-07-28"` 是客户端脚本自带的门禁；SDK 升版时
该断言先失败，证据不会静默地拿错版本跑。

---

## 4.18 S9-E 追加（2026-11-12）：ACP v1 前端（Broker 作 ACP Client，仅 stdio）

**范围**：`broker_proto_acp`。语义 = **Broker 是 ACP Client，Agent 是 Broker 的本地子进程**（ADR-026 §D5、convergence report §7.1 ACP 侧）。与 MCP（S9-D）相反：MCP 里 Broker 是网关/Server 端、原语透传给远程 server；ACP 里 Broker 是 Client 端，主动向本地 agent 子进程发 `initialize` / `session/new` / `session/prompt`，只接一个强制回调 `session/request_permission`。**仅 stdio**：没有 HTTP/WebSocket listener，没有 `listeners` 配置项，agent 路径由运维在 `agents` 里声明、绝不由远程请求指定。

### 4.18.1 方法与能力矩阵

| 方向 | 方法 | 实现 | 行为 |
|---|---|---|---|
| Client→Agent（Broker 发起） | `initialize` | ✅ 本地实现 | 发 `protocolVersion=1` + `clientCapabilities={}` + `_meta.unified-mq`（`role=client`、`version=1`、`fs/terminal/elicitation=false`）；校验回包 `protocolVersion`，不匹配 → `stop=unsupported_protocol_version` 且**不发** `session/new` |
| | `session/new` | ✅ 本地实现 | 带 `cwd`（来自 agent 配置）+ `mcpServers=[]`，取回 `sessionId`；Broker 用它作后续目的地与状态机锚点 |
| | `session/prompt` | ✅ 本地实现 | 写 `$acp/in/<agent>` → 瞬态读者按 destination 过滤后回读 → 异步发给 agent；结果经 `$acp/session/<id>` 的 turn 投影（含 `stopReason`）送达 |
| Agent→Client（Agent 回调） | `session/request_permission` | ✅ **唯一实现** | 交 `acp_policy`（默认拒绝 + allowlist + 超时）裁决；授权选 `allow_once` 优先、`allow_always` 次之、否则首个 `optionId`；**每次决策**写 `$acp/permission`（transient/`os_buffer`）；拒绝 → `outcome=cancelled`、会话置 `gone`、`stop=refusal` |
| | `fs/read_text_file`、`fs/write_text_file`、`fs/create_directory` | ⛔ 回 `-32601` | `data.reason="capability not advertised"`；**保持会话**（不因单个未支持能力销毁会话，见落地偏差 1） |
| | `terminal/create`、`terminal/output`、`terminal/wait`、`terminal/kill`、`terminal/release` | ⛔ 回 `-32601` | 同上 |
| | `session/request_escalation`（elicitation） | ⛔ 回 `-32601` | 同上 |
| 取消 | `session/cancel`（notification） | ✅ | Client→Agent 方向转发；Broker 侧 turn 投影记 `stopReason=cancelled`，会话回 `ready` |
| 数据面 | `session/update`（notification） | ✅ | Agent→Client 折叠为 turn 投影写 `$acp/session/<id>` |

`clientCapabilities` 恒为空对象 `{}`：Broker 作为 Client 不代理任何文件系统 / 终端 / 提权副作用，全部由编辑器/人 侧原 ACP Client 负责——Broker 只是把 agent 接进内核消息面。`_meta.unified-mq` 显式声明 `fs/terminal/elicitation=false`，与「不广告 = 不承诺」一致。

### 4.18.2 目的地（数据面）

| 目的地 | 方向 | reliability | 承载 |
|---|---|---|---|
| `$acp/in/<agent>` | 入站（生产者→agent 读者） | `os_buffer` | `session/prompt` 请求体（`sessionId` + `prompt` content blocks） |
| `$acp/session/<sessionId>` | 出站（agent→消费者） | `os_buffer` | `session/update` 折叠的 turn 投影（`stopReason`、`counts`、`updates` 全文） |
| `$acp/permission` | 出站（审计） | `os_buffer` | 每次 `session/request_permission` 裁决（请求上下文 + `decision` + 匹配到的 allowlist 条目） |

三者都经 `broker_router_hash:route/3` + `broker_queue:append/3`，与 `$mcp/*` 同一内核消息面。`$acp/session/*` 与 `$acp/permission` 可能与 `$acp/in/<agent>` 同分区，故瞬态读者**必须按 destination 过滤**——只把 `in_destination(name)` 当 prompt，绝不回读自身发布的会话/审计消息（否则自激 prompt 循环）。

### 4.18.3 配置面

`config/sys.config.src` 的 `broker_proto_acp` 段（默认空、开箱不跑任何 agent）：

```erlang
{broker_proto_acp, [
  {agents, []},                 %% 空 = 干净启动但不跑 agent；每条是一个子进程 spec
  {permission_policy, #{
      default      => deny,     %% 默认拒绝 + allowlist + 无等待
      allow        => [],       %% allowlist：#{tool, title}，两轴都须匹配，`any` 通配
      timeout_ms   => 30000,    %% ElapsedMs > timeout_ms 判超时拒绝
      notify_dest  => <<"$acp/permission">>  %% 审计目的地（transient/os_buffer）
  }},
  {supported_protocol_version, 1}
]}.
```

每个 `agents` 条目是 `broker_proto_acp_sup` 启动的一个子进程完整 spec：`#{name => my-agent, command => "/abs/path/agent", args => ["--stdio"], env => [{"K","V"}], cwd => undefined, restart => temporary}`。**`command` 必须绝对路径、无 shell**（`acp_port` 用 `spawn_executable` 直接起，不经 `/bin/sh`，配置里的路径/元字符无法变成命令注入）。`restart => temporary`：一个 agent 崩溃永不重启（重启策略由运维在编排层决定）。`agents` 非良构 map 列表或 allowlist 条目命名非 binary 时，`broker_proto_acp_app:start/2` **启动即拒绝**（一个 misconfig 不该拖到运行期才变成不透明的 spawn 失败）。**无 `require_auth`**：ACP stdio 的信任边界是 OS 级进程访问（同 `mcp_stdio`），身份固定为匿名本地身份。

### 4.18.4 权限策略（`acp_policy`）

`session/request_permission` 的裁决顺序：

1. **allowlist 匹配**：请求体的 `toolCall.kind` + `toolCall.title` 与某条 `#{tool, title}` 两轴都命中（`any` 是通配，不做子串/正则）。命中即授权。
2. **超时拒绝**：`ElapsedMs > timeout_ms`（等于不算超时）直接拒绝。
3. **默认拒绝**：以上都不命中即拒——即使配置写 `default => allow` 也拒绝（冻结姿态：Broker 侧默认 deny，不放宽到「未匹配即授权」）。
4. **授权选项选择**：命中的 allowlist 里优先 `allow_once`、其次 `allow_always`、否则请求带的第一个 `optionId`。
5. **拒绝的落地**：`outcome=cancelled`，会话状态机置 `gone`，turn `stopReason=refusal`。
6. **审计**：无论授权还是拒绝，`$acp/permission` 都写一条（transient/`os_buffer`，绝不阻塞权限回合）；写失败只 `logger:error`。

### 4.18.5 真实 `acp_port` 路径证据（2026-11-12）

- **驱动**：`acp_evidence:run/0`（`/tmp/opencode/acp-evidence/src/acp_evidence.erl`）启动完整 Broker 内核（`broker_security`/`broker_storage`(wal)/`broker_router`/`broker_queue`/`broker_meta`），配置一个名为 `evidence-agent` 的假 ACP agent（`acp_port` 传输、`cwd=/home/wanli/workspace/acp-evidence`），依次跑六个场景并打印落在 `$acp/session/<sid>` 与 `$acp/permission` 的消息。数据目录在 ext4：`/home/wanli/workspace/acp-evidence/data`。
- **假 agent**：`fake_acp_agent.escript`（newline-delimited JSON-RPC，按 prompt 文本选场景，每个收到的帧 `RECV …` 打到 stderr）。`acp_port` **不设** `stderr_to_stdout`，故 agent 的 stderr 与 Broker 的协议 stdout 隔离——证据文件 `evidence.out` 是纯 turn 投影，`client_frames.log` 里的 `RECV` 行是 agent 侧回声，二者不串。
- **结果**（`evidence.out`，exit 0）：

```
== HANDSHAKE ==        state=ready   session_id=<<"acp-evidence-session">>
== plain (4 msg) ==    2×agent_message_chunk + 1×cwd + 1×agent_thought_chunk；turn stopReason=end_turn
== permission(allow) == agent 回 outcome=selected, optionId=allow；$acp/permission 记 decision=allow、匹配 #{tool=edit,title=any}
== fs probe ==         agent 发 fs/read_text_file；Broker 回 -32601 (capability not advertised)；会话保持、turn end_turn
== cancel ==           Broker 发 session/cancel；turn stopReason=cancelled；会话回 ready
== permission(deny) == agent 发 execute/rm -rf /；无 allowlist 命中 → outcome=cancelled；会话 gone；turn stopReason=refusal
```

**client→agent 帧**（`client_to_agent.jsonl`，11 帧，`initialize` 的 `clientCapabilities` 带 `_meta.unified-mq`、`session/new` 带 `cwd`+`mcpServers=[]`、4×`session/prompt`、`session/request_permission`(allow) 回 `selected`、`fs/read_text_file` 回 `-32601`、`session/request_permission`(deny) 回 `cancelled`、`session/cancel`）。

### 4.18.6 已声明的限制 / 边界

- **仅 stdio**：无 HTTP/WebSocket listener、无 `listeners` 配置项、无远程建连。ACP 远程（HTTP/WebSocket）在 ACP 规范里仍 WIP，本实现不覆盖。
- **单节点**：reply / 订阅读 owner 节点的本地分区，跨节点 reply/订阅不支持（与 MCP / A2A Task 同一限制）。
- **只做 Client 侧**：Broker 不扮演 ACP Agent（编辑器侧那端），也不做 `session/load` / `session/resume` / `session/list` / `authenticate`（v1 无鉴权方法）；agent 由运维静态声明。
- **`fs` / `terminal` / `elicitation` 全拒绝**：Broker 不代理文件/终端/提权副作用，回 `-32601` 但保持会话。
- **stdio 无认证**：信任边界是「谁能启动这个进程」，身份为匿名本地身份。
- **无新增指标键**：握手失败 / 权限拒绝 / agent 退出只 `logger`（S9-F 定稿不加计数器，见 `docs/security.md` §12.1 第 17 条）。
- **真实 ACP 客户端证据见 §4.18.8**（`test/protocol-compatibility/s9f/acp/`，官方 `agent-client-protocol 1.0.0` 当 Agent 服务端，Broker 作 Client 经生产 `acp_port` 拉起），并记录了 S9-F 因此修复的一处 wire 缺陷。

### 4.18.7 落地偏差（相对 ADR-026 §D5 冻结文本）

ADR-026 §D5 冻结文本未改，以下为「落地后修正」批注（详见 `docs/design/s9-e-execution-plan.md` 落地偏差记录）：

1. **`fs/*`、`terminal/*`、`elicitation/*` 回 `-32601` 但保持会话**（原文「不广告且回 `-32601` 并结束会话」）：实现选择不结束会话——单个未支持能力不应销毁已协商会话。证据：§4.18.5 `fs` 探针回 `-32601` 后 `session/prompt` 仍正常。
2. **`session/request_permission` 是唯一实现的 Client 回调**，其余 ACP 回调按偏差 1 拒绝；`_meta.unified-mq` 注明 `fs/terminal/elicitation=false`。
3. **`clientCapabilities` 为空对象 `{}`**，`_meta.unified-mq` 声明本 Client 不代做副作用能力。
4. **`protocolVersion` 只接受 1**：不匹配记 `stop=unsupported_protocol_version` 且**不发** `session/new`。
5. **`{line, 0}` 是非法端口选项**：`acp_port` 用 `{line, 1048576}`，超长行以 `{noeol,Chunk}` 分块到达、`feed_bytes/2` 重组；`open_port({spawn_executable,_})` 的 executable/argv/env 必须 string，`acp_port:open/1` 单点 `to_str/1` 把 binary 转 string。
6. **prompt 读者按 destination 过滤**：瞬态读者只处理 `in_destination(name)`，绝不回读自身 `$acp/session/*` / `$acp/permission`（否则自激 prompt 循环）。

### 4.18.8 官方 ACP SDK 真实客户端证据（S9-F，2026-11-12）+ 一处 wire 缺陷修复

**是什么**：`test/protocol-compatibility/s9f/acp/acp_evidence.erl` 用**生产 `acp_port` 传输**拉起
上游官方 ACP Python SDK（`agent-client-protocol 1.0.0`，顶层包名 `acp`，`PROTOCOL_VERSION=1`）当
**Agent 服务端**——即 Zed / 编辑器那一端的真身，只不过跑成一个本地子进程。Broker 是 Client：主动发
`initialize` / `session/new` / `session/prompt`，只接一个强制回调 `session/request_permission`，再读
`$acp/session/<sid>` 的 turn 投影与 `$acp/permission` 的审计。

- **驱动**：`acp_evidence:run/0`，依次跑 5 个场景，退出码 0。数据目录默认在 tmpfs
  （`/tmp/opencode/s9f-evidence/data-acp`，配 `require_durable_fs => false`）——**只用于证据，不代表
  交付配置的持久化性能**；要换持久化目录设 `S9F_ACP_DATA`。
- **帧级证据**：`acp/acp_sdk_agent_tee.py` 是裸 agent 的旁路 tee（SDK 用 `connect_read_pipe(sys.stdin)`
  读 fd 0，Python 层 stdin 包装会被绕过，所以在 fd 0 前接 OS 管道、线程原样落盘），因此
  `acp/client_to_agent.jsonl`（7 帧）是**逐字节的 client→agent 原文**，不含 agent 侧日志。
- **结果**（`acp/sdk_acp.out` 原文）：

```
== HANDSHAKE ==          state=ready   session_id=<<"acp-sdk-session-99a137fd">>
== plain ==              agent_message_chunk×1 + turn stopReason=end_turn
== permission(allow, kind=execute) ==  agent_message_chunk×1 + turn stopReason=end_turn
== permission(allow) ==  $acp/permission: decision=selected, reason="matched allowlist", toolCall.kind=execute
== permission(deny, kind=edit) ==      turn stopReason=refusal, updates=[]
== permission(deny) ==  $acp/permission: decision=cancelled, reason="not in allowlist", toolCall.kind=edit
== FINAL ==              state=gone   stop=<<"refusal">>
```

- **client→agent 帧**（`acp/client_to_agent.jsonl`）：`initialize` → `session/new`（带 `cwd` +
  `mcpServers=[]`）→ 3×`session/prompt` → `session/request_permission`(allow) 回包 id=0 →
  `session/request_permission`(deny) 回包 id=1。**两个回包都是规范嵌套结构**：

```json
{"id": 0, "jsonrpc": "2.0",
 "result": {"_meta": {"unified-mq": {"decision": "allow", "toolCall": {"kind": "execute", ...}}},
            "outcome": {"optionId": "allow-once", "outcome": "selected"},
            "reason": "matched allowlist"}}
{"id": 1, "jsonrpc": "2.0",
 "result": {"_meta": {"unified-mq": {"decision": "deny", "reason": "not in allowlist", "toolCall": {"kind": "edit", ...}}},
            "outcome": {"outcome": "cancelled"},
            "reason": "not in allowlist"}}
```

授权带 `optionId`（SDK 给的 `allow-once`），拒绝**不带** `optionId`——与 ACP v1 `AllowedOutcome` /
`DeniedOutcome` 的形状一一对应。

#### 4.18.8.1 落地偏差：官方 SDK 暴露的 `outcome` 扁平缺陷（已修）

ADR-026 §D5 冻结文本**未改**；本节是「落地后修正」批注。S9-F 第一次用官方 SDK 跑证据时，Agent 侧
以 `model_validate` 校验回包，直接失败：

```
ACP session/prompt failed: #{<<"code">> => -32602, <<"data">> => #{<<"errors">> =>
  [#{<<"input">> => <<"selected">>, <<"loc">> => [<<"outcome">>],
     <<"msg">> => <<"Input should be a valid dictionary or object to extract fields from">>,
     <<"type">> => <<"model_attributes_type">>]}, <<"message">> => <<"Invalid params">>}
```

原因：ACP v1 `RequestPermissionResponse.result.outcome` 是**判别联合**
`DeniedOutcome | AllowedOutcome`（判别键 `outcome`），即一个**对象**：

- `AllowedOutcome = {outcome: "selected", optionId: <str>, _meta?: ...}`
- `DeniedOutcome  = {outcome: "cancelled"}`（**无** `optionId`）

而 Broker 原来按内部 decision 的扁平形状发 `{"outcome": "selected", "optionId": "..."}`——`outcome`
是裸字符串，任何合规 Agent 都会以 `model_attributes_type` 拒绝。这是**只有真实 SDK 客户端能暴露**的
规范偏差（此前 `curl` + 假 agent 都按 Broker 自己的形状回包，自证循环）。

**修复**（语义零变化，只改 wire 投影）：

1. 新增 `acp_policy:wire/1`（并 export）与私有 `wire_meta/2`：把内部扁平 decision 转成规范嵌套
   result 对象；`selected` 带 `optionId`（缺省 `null`），`cancelled` 不带；`reason` 与 `_meta`
   （`_meta.unified-mq` 审计块）作为扩展成员随路——`_meta` 是 ACP 保留给实现自定义元数据的位置，
   合规对端忽略未知成员（SDK 实测 `model_validate` 通过、`field_meta` 保留）。
2. `acp_agent` 写 `session/request_permission` 回包处改调 `acp_policy:wire(Decision)`；**内部
   decision 形状不变**（`decide/2` 的返回值、`audit_body/4`、`$acp/permission` 的投影、以及全部
   依赖扁平 `outcome` 键的断言都保持原样）。
3. 测试：`acp_policy_SUITE` 新增 `wire_result_is_nested`（列入 `policy` group），断言嵌套形状、
   `_meta`/`reason` 随路、拒绝不带 `optionId`；`acp_agent_SUITE` 两处断言从扁平改为嵌套
   （`allowlisted_tool_is_granted` / `non_allowlisted_tool_is_denied`）。
4. **回归**：`rebar3 ct` **1063/1063、68 套件**（基线 1062 + 新增 1），`acp_policy_SUITE` 17 例、
   `acp_agent_SUITE` 17 例；`rebar3 dialyzer` **0 warning**。
5. 修复后重跑官方 SDK 证据：三个场景（plain / permission-allow / permission-deny）全通，帧原文即
   上面 §4.18.8 的两段回包。

**为什么这不算放宽 ADR-026**：ADR-026 §D5 冻结的是「`session/request_permission` 是唯一实现的 Client
回调 + 默认拒绝 + allowlist + 审计」这些**语义**；嵌套 `outcome` 是 ACP v1 对同一语义的**wire 编码**。
原实现编码错了，修正后语义一字未变——§4.18.4 的裁决顺序、§4.18.2 的三个目的地、§4.18.6 的限制全部照旧。


---

## 4.19 S9-F 追加（2026-11-12）：官方 A2A SDK 真实客户端证据

**是什么**：`test/protocol-compatibility/s9f/a2a/a2a_sdk_client.py`，用上游官方 Python SDK
`a2a-sdk 1.3.0`（**protobuf** API，`a2a.types` / `a2a.client.ClientFactory`）当**真实第三方 A2A
客户端**，跑证据节点 `127.0.0.1:18094`（`task_auto_complete => true`、tenant `t2` 的
`evidence-agent` 卡片）。原文输出：
[`test/protocol-compatibility/s9f/a2a/sdk_a2a.out`](../test/protocol-compatibility/s9f/a2a/sdk_a2a.out)。

| SDK 步骤 | 调用 | 官方 SDK 观测 | 落到本实现的哪一条 |
|---|---|---|---|
| 1 解析卡片 | `A2ACardResolver.get_agent_card()` | `name=unified-mq-evidence-agent`、`version=1.0.0`、`capabilities={streaming:false, push_notifications:false}`、`supported_interfaces=[{url, protocol_binding:JSONRPC}]`、`skills=[echo]` | §4.15.3 Agent Card 发现面（`/​.well-known/agent-card.json`，按 tenant 隔离） |
| 2 建连 | `ClientFactory(ClientConfig(streaming=false, supported_protocol_bindings=["JSONRPC"], accepted_output_modes=["text"])).create_from_url(BASE)` | 非流式 `BaseClient` | §4.14 `A2A-Version` 协商 + JSON-RPC 绑定 |
| 3 发任务 | `client.send_message(SendMessageRequest(message=Message(role=ROLE_USER, parts=[Part(text)])))` | `send_message` 返回 `AsyncIterator[StreamResponse]`，取到 `task.status.state=TASK_STATE_COMPLETED`、`history` 2 条（USER + AGENT） | §4.14 `SendMessage` + §4.15 的 `task_auto_complete` 内建执行器（SUBMITTED→WORKING→COMPLETED） |
| 4 二次发任务 | 同上 | 新 `task_id`，状态 `TASK_STATE_COMPLETED` | Task 状态机可重复创建 |
| 5 读回 | `client.get_task(GetTaskRequest(id=task_id))` | 直接返回 `Task`（**非** iterator）：`TASK_STATE_COMPLETED`、`history` 2 条 | §4.15 `GetTask`（按 id 读回事件溯源结果） |
| 6 列举 | `client.list_tasks(ListTasksRequest(page_size=10))` | 直接返回响应：N 个任务**全部** `TASK_STATE_COMPLETED` | §4.15 `ListTasks`（分页 + 状态过滤面） |

**如实记录两点**：

- SDK 1.3.0 是 protobuf 客户端：`Message(message_id, role, parts=[Part(text)])`、
  `SendMessageRequest(message=...)`、`GetTaskRequest(id=...)`（是 `id`，**不是** `name`）、
  `ListTasksRequest(page_size=...)`。`send_message` 是 `AsyncIterator[StreamResponse]`，而
  `get_task` / `list_tasks` 是**直接返回**——这是 SDK 的 API 形状差异，不是 Broker 的差异；
  Broker 侧三种都是同一 JSON-RPC 请求的同步响应。
- 本证据里 `task_auto_complete=true`，Broker 自己跑完生命周期；**没有**外部 agent 执行器，
  因此不测量「真实 agent 推理」。`task_auto_complete=false` 时 Task 停在 `SUBMITTED`，需外部执行器
  调 `a2a_task_registry:apply_event/4`（见 §4.15.7）。

---

## 4.20 S10-A 追加（2026-11-13）：WebRTC 信令前端（`broker_proto_webrtc`，只有连接层）

**是什么**：`apps/broker_proto_webrtc/`，WSS 信令接入前端的**首个代码交付**。它复用
`broker_ws` 的 `ws_handshake` / `ws_frame`（不写第二份 RFC 6455），每连接一个 `webrtc_conn`
gen_server，**不碰消息内核**（ADR-028：不产生 `#broker_message{}`、不调 `broker_queue` /
`broker_router`、无 cursor/retention）。

**证据**：CT `webrtc_conn_SUITE` 27 例（握手 4 / 认证 8 / 帧 10 / 超时 4 / TLS 1，全部
`port => 0`、真实 socket + 字节级客户端）、`webrtc_boundary_SUITE` 6 例（ADR-028 §D7 源码级守卫
+ 依赖账本）、`webrtc_validate_SUITE` 12 例（启动门控）。**没有真实第三方客户端证据**
（零端口、无 `to` 转发，1:1 信令走不通），`aiortc` 证据推到 S10-D。

### 4.20.1 握手与子协议

| 项 | 行为 |
|---|---|
| 升级 | `ws_handshake:accept/2`，返回 `{ok, Response, Subprotocol}`（三元素） |
| 子协议 | `webrtc.v1` 必须在 `subprotocols` 白名单里；缺失或未知 → **HTTP 400 不升级** |
| 服务端优先 | 客户端把 `webrtc.v1` 排在第二位时，服务端仍选中它 |
| path | 任意 path 都接受（子协议才是协议判据，与 `broker_proto_mqtt_ws` 同一姿态） |
| 传输 | `ranch_tcp` 与 `ranch_ssl` **共用同一个连接模块**；wss 走 `broker_security.tls` 的证书 |

### 4.20.2 认证（R-C1 / R-S7）

窗口在**握手完成的那一刻**开始计算（不是收到第一帧时），默认 `auth_timeout_ms = 5000`。

| 客户端发出 | 服务端响应 |
|---|---|
| `{"event":"authenticate","data":{"username":U,"password":P}}`（正确） | `{"event":"authenticated","data":{"peer_id":Id}}`，`Id` 为 16 字符 base64 |
| 同上但口令错 / 用户不存在 | `{"event":"unauthorized"}`，**连接保持**，可以重试 |
| 缺 `username` 或 `password` 字段 | `{"event":"unauthorized"}` |
| 窗口内发任何别的文本帧（含非 JSON） | `{"event":"unauthorized"}` |
| 窗口内什么都不发 | **close 1008**（policy violation）后关 socket |
| `require_auth => false`（仅回环 bind） | 握手完成即**主动**推一条 `authenticated`，随后直接进 `active` |

**`peer_id` 由服务端铸造**（`base64:encode(crypto:strong_rand_bytes(10))`，16 字符），客户端
不能指定；它只对本次连接有效，重连即换新值。这是 R-S7 的落地形态。

**口令不进日志、不进指标**，只在 `webrtc_conn:try_authenticate/2` 的栈上出现；认证走
`broker_security:authenticate_conn({user_pass, U, P})`，与其余六协议同一张 `users` 表。

### 4.20.3 帧策略与保活（R-T3 / R-T4）

| 客户端发出 | 服务端行为 |
|---|---|
| 文本帧 `ping`（裸字符串，非 JSON） | 文本帧 `pong` |
| 原生 opcode 9（ping） | 原生 opcode 10（pong），回显 payload |
| 原生 opcode 10（pong） | 忽略 |
| 文本帧，JSON 里**没有** `to` | `{"event":"invalid_message"}` |
| 文本帧，JSON 里**有** `to` | `{"event":"invalid_message"}`——S10-A 没有房间注册表，诚实说不能路由，不静默丢弃 |
| binary 帧 | **close 1003**；配 `binary_frames => ignore` 时改为丢帧并保持连接（不写日志） |
| 单帧超过 `max_frame_bytes`（默认 262144） | **close 1009** |
| RSV 位非 0 / 非法控制帧 / 未知 opcode | **close 1002** |
| 客户端 close 帧 | 回 close 帧后关（会话结束；本实现不回 close 帧，直接停进程） |
| 分片（`fin => false` 首片 / `opcode 0` 续帧） | **不重组**：首片当完整文本处理（结果多为 `unauthorized` / `invalid_message`），续帧被兜底子句忽略；见 §4.20.5 与 `docs/security.md` §12.2 第 11 条 |
| 已认证后闲置超过 `idle_timeout_ms`（默认 60000） | **close 1001**（`infinity` 可关） |

文本 `ping`/`pong` 与原生 9/10 **并存**，两条路径都有用例钉死（上游只有前者，浏览器只会用后者）。

### 4.20.4 启动门控（`broker_proto_webrtc_app:validate/0`）

启动即拒绝（不静默降级）的六类配置：

| 拒绝 | 触发条件 |
|---|---|
| `{insecure_listener, Port, Ip}` | `require_auth => false` 且任一 listener 绑非回环地址 |
| `{invalid_require_auth, V}` | `require_auth` 不是布尔值（典型误配：从 `broker_security` 抄来一个协议名**列表**） |
| `{invalid_subprotocols, empty \| V}` | 空列表或含非 binary 条目 |
| `{invalid_auth_timeout_ms, V}` | ≤ 0 |
| `{invalid_idle_timeout_ms, V}` | ≤ 0 且不是 `infinity` |
| `{invalid_max_frame_bytes, V}` | ≤ 0 |

**`require_auth` 是本 app 自己的 env 键**（ADR-028 §D5，照 `broker_proto_a2a` /
`broker_proto_mcp`），**不是** `broker_security.require_auth`——后者是「必须认证的协议名列表」，
出厂 `[]`。初版读错对象，导致装配成功的出厂 release 一启动就 `{error,{invalid_require_auth,[]}}`；
已在落地时修正并加两条回归用例（`webrtc_validate_SUITE`）。

### 4.20.5 本阶段明确没有的东西

没有房间、没有成员表、没有 `to` 转发、没有 `joined`/`left`、不调 `broker_security:authorize/2`。另有一处传输层缺口：**未做 WebSocket 分片重组**（`fin => false` 的首片被当作完整文本，`opcode 0` 续帧被忽略），所以分片发送的 `authenticate` 会被判 `unauthorized`——浏览器信令客户端默认不分片，S10-B 补。
因此**跨房间越权在本阶段不存在可被利用的路径**——不是「已修好」，而是「还没有这条路径」。
逐条留痕见 `docs/security.md` §12.2（11 条）。

---

## 5. 冻结契约一致性（T17 签认，2026-11-06）

> 本节是 **Gate S1 的签认记录**，不是 S5 的兼容性结论。它回答一个问题：上表 MQTT 行的语义边界，是否真的由三份冻结 ADR 决定，而不是由实现细节偶然决定。复核对象是 T2~T15 的最终代码（commit `a8608d9` 前的实现状态，文档改动不触及代码）。

### 5.1 机械核对结果

| 冻结契约 | 核对方式 | 结果 |
|---|---|---|
| ADR-009 §9.1 的 `#broker_message{}` | `record_info(fields, broker_message)`（含头文件编译） | 13 个字段**同名同序**，`size=14`；`partition_id()` 仍是 `non_neg_integer()`，`sync_mode()` 仍是三值枚举 |
| ADR-010 §10.1 的 Router behavior | `broker_router:behaviour_info(callbacks)` / `optional_callbacks` | 5 项回调一致；`optional_callbacks = [{on_partition_change,2}]`；`#route_ctx{}` 三字段一致（按 H6 批注放在 `include/broker_router.hrl`） |
| ADR-010 §10.2 的边界 | 源码引用检查 | `apps/broker_router/src/` 对 `broker_storage` / `broker_subscription` 的引用**只在注释**，无调用 |
| ADR-011 §11.1 的 Storage behavior | `broker_storage:behaviour_info(callbacks)` + 两个后端声明 | 恰为 8 项；WAL / memory 都 `-behaviour(broker_storage)` 并导出全部 8 项 |
| ADR-011 §11.4 的 capability | 常量核对 | WAL `[os_buffer, fsync]`、memory `[os_buffer]`；两后端都对超能力 `sync_mode` 报错而非降级 |
| roadmap §3.4 的协议门禁 | `ls apps/broker_proto_*` | 只有 `broker_proto_mqtt` |

结论：**未发现实现偏离冻结 API，无需触发 ADR 修订。**

### 5.2 Gate「订阅位置不丢」措辞（E1）的正式绑定

E1 的解释已写入 [ADR-011 附录 11.7](adr/011-broker-storage-behavior.md#117-附录gate订阅位置不丢的措辞解释v03-复审已确认) 并签认。它约束上表 MQTT 行的三件事，边界如下：

| 承诺 | 断言 | 容忍度 | 对协议对端可见的表现 |
|---|---|---|---|
| 已 ACK 消息不丢（PUBACK 过） | R1a | **零容忍** | 客户端收到 PUBACK 的消息，broker 重启后仍能读出 |
| 订阅**集合**不丢 | R1b | **零容忍** | 非 clean 重连 `session_present=1`，无需重发 SUBSCRIBE |
| 消费 **cursor** 不丢 | R3 | 有界回退 ≤`cursor_max_regress`（默认 64 / 100 ms；可配 1 / 0 逼近零回退） | 崩溃后可能重复投递已 ack 之后的少量消息；不会跳读、不会丢 |
| 未 ACK 消息重投 | R3(ii) + R7 | 至少一次（允许重复） | 重连后按 cursor 重发；同一条未 ack 消息在本连接内只投一次（无重试定时器） |

因此 §4 第 5 条"at-least-once 的 S1 形状"是**契约的一部分**，不是实现缺陷：默认档的重复窗口就是 `cursor_max_regress` / `inflight_flush_interval_ms` 的取值；想要更小窗口就配零回退档，代价是逐条 fsync。Producer 侧的 durability 与消费侧的 cursor 回退是两个独立承诺，不可互相借用措辞。

### 5.3 本节不改变的内容

- §4 的 MQTT 能力集合与 6 条限制**一条未改**；T17 没有新增协议能力，也没有修产品代码。
- S2 的 QoS2 / Retain / Will / MQTT 5 仍按 roadmap §4.1 补齐；届时本文件的 MQTT 行升级为 S5 终态，并重新跑一遍真实客户端套件。
- 三份 ADR 的冻结文本未改，只增加了 T17 复核/签认批注（ADR-009/010 的验证段、ADR-011 §11.7 与验证段）。
