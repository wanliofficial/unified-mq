# 协议兼容矩阵（Protocol Compatibility）

- **文档版本**：v1.2
- **最近一次追加**：2026-11-11——新增 §4.12：MQTT over QUIC（S8-D，EMQX `emqtt` 1.16.1 真实客户端 8/8 + CT 17 例）。此前一次：2026-11-10——§4.11 MQTT-SN 1.2（S8-C，`mqtt-sn-tools` 0.0.7 真实客户端 10/10 + CT 84 例）；再此前：§4.13 MQTT over WebSocket（S8-B，paho-mqtt 2.1.0 `transport="websockets"` 8/8 + CT 19 例）。§4/§4.1~§4.10 原文保留为当时事实
- **对应 Roadmap**：§7 S5 协议扩展、§9 S7 身份统一与事件绑定
- **对应 README**：11.1 协议兼容矩阵（验证维度）
- **S5 协议顺序**：MQTT → WebSocket → NATS → AMQP → Kafka → Pulsar

> **用例数口径**：本文件的表格里，标「S5 时点」「S3 时点」等字样的是**当时快照**，标「**N 例（当前）**」的是最近一次从 `all/0`（含 `groups/0` 展开）逐套件清点的当前值。**当前全量基线（S8-D 收尾，2026-11-11）是 `rebar3 ct` 823/823、52 个套件**（`apps/*/test/` 51 个 + `test/integration/s1_e2e_SUITE` 1 个；S7 结案 673/673、43 套件 → S8-A 711/711、46 套件 → S8-B 731/731、47 套件 → S8-C 815/815、50 套件 → S8-D 823/823、52 套件）。**QUIC 的真实 QUIC 用例只在 `rebar3 as quic ct` 下展开**（默认 profile 无 quicer，该组不出现），所以默认全量数的是 config 组（`broker_proto_mqtt_quic` 17 例）；`rebar3 as quic ct` 下该套件 23 例（多出 6 例真实 QUIC），全量 **827 passed + 2 skipped**（`mqtt_quic_transport_SUITE` 的两个「quicer 缺失」用例在 quicer 在场时按设计 skip），零 FAILED。

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

## 2. 能力矩阵（S5 完成态；S8-B 追加 MQTT over WebSocket 行，S8-C 追加 MQTT-SN 行，S8-D 追加 MQTT over QUIC 行）

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
| 帧（`mqtt_frame_SUITE`） | 27 例，含 PUBREC/PUBREL/PUBCOMP 双向、QoS2 PUBLISH/Will、reserved flags |
| 连接（`mqtt_conn_SUITE`） | 50 例，其中 QoS2/Retain/Will 共 20 例：四步双向、DUP 去重、重连重发两半、未知帧幂等、状态超限、补发/清除/持久化/上限、Will 四种触发 + 重启不补发 |
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
| 连接（`mqtt_conn_SUITE`） | 2 例：v5 `required` 拒绝回 `0x83` 且连接继续可用；3.1.1 拒绝关闭且不发 ACK |
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
| WS 连接/应用协议（`ws_conn_SUITE`） | **22 例（当前）**：S5 时点 14 例（connect/connack、qos1 往返+ack、qos0 无 ack、二进制帧、unsubscribe、clean=false 追赶、clean 丢弃、非法 filter/shared/超限拒绝、ping/pong、未知 delivery ack、明文 HTTP 拒绝）；S6 追加 `cloudevent_headers_roundtrip`（15）；S7 追加凭据解析、错误凭据拒绝、客户端自带租户被忽略、`require_auth` 四例（19）；trace context 追加往返、`off` 时剥离、非法 `traceparent` 不传播三例（22） |
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

### 4.10 CloudEvents Binding 现状（S5–S7 口径）

ADR-020 冻结了 MQTT 的 Binding 细节（`mqtt_user_properties` 通道）。S6 补齐其余五个协议的 **Binary Mode**，统一采用协议中立的头通道：

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

**统一策略执行**：六个协议在写入前都调用 `broker_event:apply_policy/1`（默认 `pass_through` 为 no-op）。拒绝路径按协议各自的错误语义表达：MQTT 5 → `0x83`；WS → `{"op":"error","code":"cloudevent_rejected","reason":"…"}`；NATS → `-ERR` 并断开；AMQP → channel error 406；Kafka → `INVALID_RECORD`（87）；Pulsar → SendError（`ERROR_UNKNOWN`）。

**已声明的非无损点**（`report/5` 的 `losses` 必须被调用方读取）：

- Binary Mode 的扩展属性值被序列化为文本；`data` 若是 JSON 对象则变为序列化字节。
- NATS 客户端未声明 `headers` 时，`ce-*` 全部丢失；这是协议能力限制，不是静默丢弃（适配器在 CONNECT 后按 `headers_cap` 决定 MSG/HMSG）。
- AMQP 0-9-1 的 `basic.publish` 没有逐消息否认；策略拒绝表现为 channel error 406，客户端必须重新打开 channel。

「逐字节透传」意味着：一个合法的 Structured CloudEvents JSON 通过这五个协议发布再消费，**字节不变**（因此 `id`/`source`/`type`/扩展都保留）。

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

- **无 DTLS**：本阶段只跑明文 UDP；DTLS 列为后续待办（设计 §1.2），因此这里是唯一没有 TLS 开关的 listener。
- **无 Forwarder Encapsulation**（1.2 §5.5）与串口桥：设计 §1.2 明确不做。
- **休眠期 QoS 0 丢弃**：QoS 0 从不进存储，睡眠期间无处可取；要「一条不丢」就用 QoS 1/2。
- **第三方客户端覆盖不到三处**：`mqtt-sn-tools` 0.0.7 **无 QoS 2 代码路径**、**无 Will 选项**、其 PINGREQ 不带 client id（无法按名唤醒休眠会话）。这三项只由 `mqtt_sn_conn_SUITE` 覆盖，Gate 文本单列，不以「第三方全绿」含混带过。
- **每客户端 normal topic id 是私有的**：订阅方 SUBACK 拿到的 id 在发布方的表里毫无意义——这是协议本身的性质，不是实现取巧；发布方必须先自己 REGISTER。

---

### 4.13 S8-B 追加（2026-11-10）：MQTT over WebSocket（`broker_proto_mqtt_ws`）

> 编号说明：§4.11 是 S8-C（MQTT-SN，已落地），§4.12 是 S8-D（MQTT over QUIC，已落地），本节按 S8-B 执行计划落在 §4.13。

**是什么**：MQTT 3.1.1 §6 / MQTT 5.0 §6 定义的 MQTT over WebSocket。同一条 `mqtt_conn_fsm`（与 1883 明文 MQTT、8883 TLS MQTT 完全同一份代码）跑在 RFC 6455 **binary** 帧里；WebSocket 只当字节管道，协议语义、QoS、会话、retain、Will 全部由 MQTT 引擎负责。默认端口 **8083**，TLS 跟随 `broker_security.tls.enabled`（wss）。

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
- **只有 `rebar3 as quic` 构建带 NIF**：默认 release 里没有 `quicer`，也就没有 QUIC 端口。运维上要先确认 `rebar3 as quic release` 的产物。
- **DTLS 不相关**：QUIC 自带 TLS 1.3，与 MQTT-SN 的明文 UDP 是两回事；`broker_security.tls.enabled` 对 QUIC 是「证书来源」而不是「开关」。

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
