# 协议兼容矩阵（Protocol Compatibility）

- **文档版本**：v0.3（S0.4 基线 + S1 预填 MQTT 行）
- **最近一次追加**：2026-11-06，§4「S1 预填：MQTT 行」——T12 的 ranch listener + paho 实连证据、CONNECT flags 缺陷（M3）与重启解码缺陷（M10/M11）、T13 的 `wal`/`memory` 双组端到端套件（12/12），以及 T14 的真实容器 `kill -9` 崩溃矩阵（默认档 / 零回退档各 8/8）；S5 的兼容性结论仍待 T15 的真实客户端套件
- **对应 Roadmap**：§7 S5 协议扩展、§9.2 文档产出物
- **对应 README**：11.1 协议兼容矩阵（验证维度）
- **S5 协议顺序**：MQTT → WebSocket → NATS → AMQP → Kafka → Pulsar

> 本文件是 S5 每个协议兼容性测试的基线。S0.4 仅确定「用什么客户端 / 版本 / 语言」并验证其可获取（见 `spikes/s0_4_clients/REPORT.md`）。协议能力、端到端语义、CloudEvents Binding 由 S5 逐协议填充「支持状态」列。

---

## 0. 客户端与版本矩阵（基线）

| 协议 | S5 顺序 | 推荐客户端 | 语言 | 基线版本 | 验证 | 备注 |
|---|---|---|---|---|---|---|
| MQTT | 1 | Eclipse Paho / mosquitto_pub-sub | Python / CLI | paho-mqtt 2.x | ✅（S0.4 基线可获取；本机无 `mosquitto`/`tcpdump`，真实客户端跑通归 T15） | QoS0/1 已在 S1 实现；QoS2、Retain、Will、5.0 语义在 S2 补；逐条限制见本文 §4 |
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
- **AMQP 版本分支**：Qpid Proton 为 AMQP 1.0；若目标 Broker 为 AMQP 0.9.1，改用 `amqp091` / `pika`。

---

## 4. S1 预填：MQTT 行（T8~T10 + T12 交付，非 S5 终态）

> 这一行是 **S1 期间的实现事实记录**，不是 S5 的兼容性结论。真正的客户端兼容性验证（paho Python + `mosquitto_pub/sub`）归 **T15**，测试目录 `test/protocol-compatibility/mqtt/` 也在那时才存在；此处先落的证据来自 `apps/broker_proto_mqtt/test/{mqtt_frame,mqtt_conn,mqtt_listener}_SUITE.erl`（真实 TCP + 真实 `mqtt_conn_fsm`，共 54 例）、`apps/broker_security/test/broker_security_SUITE.erl`（7 例）与 `test/integration/s1_e2e_SUITE.erl`（T13：同一组端到端用例跑 `wal`/`memory`，12 例）。设计条款溯源见 [`design/s1-mqtt-vertical-slice.md`](design/s1-mqtt-vertical-slice.md) §4.1/§4.5 批注与 §13 L1~L20、M1~M12。

> **T13 追加（2026-11-06）**：`test/integration/s1_e2e_SUITE` 把 T12 的 listener 与 §8 的端到端断言接起来：`wal` 组验证 QoS1 的 PUBACK→append→cursor/committed 全链路、非 clean 重连恢复订阅集合、三条离线消息按 offset 序追赶；`memory` 组在同一份用例清单下断言 E12（SUBACK `0x80`、超限 PUBLISH 断连且零 append）与 QoS0 无积压，**不用 skip**。这是 Gate「`broker_storage` 的 WAL 与内存实现可互换」的套件级证据；`kill -9` 级 R1a/R1b/R2~R7 归 T14（已交付，见下条）。

> **T14 追加（2026-11-06）**：崩溃矩阵已在**真实容器**里交付——release（`rebar3 as t14 release`）跑在 `erlang:28.3.1.0`（宿主同版本 OTP 28.3.1 / ERTS 16.2），数据目录 bind-mount 到宿主 ext4（`wal_writer:check_filesystem` 的真实条件），`docker kill -s KILL` 注入崩溃，paho-mqtt 2.x 客户端跑在宿主。**默认档（K=64 / 100 ms）与零回退档（K=1 / 0 ms）各 8/8 PASS、零 FAIL**。与协议对端直接相关的结论：R1a 收过 PUBACK 的消息在 kill 后从 WAL 完整回放；R1b 订阅**集合**在崩溃后免重发 SUBSCRIBE 仍生效（`session_present=1`）；R2 未 ack 的并发发布只可能"全丢或成无空洞前缀"，默认档恢复 31/200、零档 29/200，全部为 `msg-0000..` 前缀；R3 默认档 cursor 退到 33（两次 33/34，重放 18/17 条、零丢失），零回退档**确定地**停在 51=N+1（重放 0、重复 0）——**重复窗口与 `cursor_max_regress` 参数语义一致**；R4 撕裂尾 3196→3198→3196、20 条全回（零档 3198→3200→3198，断言是回到撕裂前大小）；R5 QoS0 零存储零投递；R6 20k 条恢复 208.836/204.706 ms < 500 ms；R7 已投递未 ack 的消息重连后重发、无丢失。偏差 M13（宿主驱动）/M14（R6 只计 `recover/2`）/M15（compose 不可用改 CLI）不影响上述协议行为。

> **T12 追加（2026-11-06）**：ranch listener 已上线，装配后的 release 节点用 **paho-mqtt 2.x 真实客户端**跑通 CONNECT/CONNACK、SUBSCRIBE/SUBACK、QoS1 PUBLISH→PUBACK、QoS0 与投递（`RELEASE-PAHO-OK`）。这一步立即暴露出一个**真实 CONNECT flags 位移缺陷**（§13 M3：clean session 位被解成 Will，paho 一连接即被拒），说明"手推黄金向量 + round-trip"只能证明自洽、不能证明合规；T15 的价值因此从"首个真实客户端证据"变成"补齐 mosquitto 与更多包型（含 `0x87` 与非法 filter）的字节级对比"。

| 协议 | 状态 | 已支持 | 已知限制 / 转换损失 | 证据 |
|---|---|---|---|---|
| MQTT 3.1.1 | **部分支持**（S1 垂直切片） | CONNECT/CONNACK、PUBLISH QoS0+QoS1、PUBACK、SUBSCRIBE/SUBACK、UNSUBSCRIBE/UNSUBACK、PINGREQ/PINGRESP、DISCONNECT；topic filter 通配符 `+`/`#`（Annex A 校验）；`clean_session` 两半；`keep_alive`（服务端 clamp）；per-session 订阅集合 + cursor + in-flight 跨连接持久化（R1b/R3/R7）；`mqtt_max_qos` 按后端 capability 派生（E2/E12）；`kill -9` 后恢复行为符合 R1a/R1b/R2~R7（T14，默认档/零回退档双档） | 见下方 6 条 | `apps/broker_proto_mqtt/test/`、`apps/broker_security/test/`、`test/integration/s1_e2e_SUITE.erl`、`tools/docker/s1_recovery.py` |

**逐条限制（协议对端可观察的行为，不是内部 TODO）**：

1. **QoS2 / Retain / Will 一律不实现，且非静默**（设计 §4.5）：QoS2 PUBLISH 或超过 `mqtt_max_qos` 的 PUBLISH ⇒ **直接断连、不发任何 ack**；QoS2 SUBSCRIBE（或 `0x80` 任一超限 topic）⇒ SUBACK 对应返回 `0x80` 且不建订阅；PUBLISH 的 retain 位 ⇒ 丢弃该位、消息正常流转、日志 `mqtt_retain_ignored`；CONNECT 的 Will ⇒ 解析后忽略、不存储、异常断连时**不会**有遗嘱消息投出，日志 `mqtt_will_ignored`。
2. **CONNACK 返回码 `0x87` 是有意的不对称**（设计 §4.5 + §13 L12）：MQTT 5.0 的 CONNECT（`Protocol Level = 5`）被拒绝时，本 broker 回 `0x87`（Not supported），而 3.1.1 把 1~5 之外的字节列为 "reserved for future versions"。**后果**：严格按 3.1.1 实现的对端可能把这一个字节当作畸形包处理，而不是当作"拒绝连接"的语义应答。T15 需要用 paho 与 mosquitto 各验一次这条路径。
3. **非法 topic filter 的 SUBSCRIBE 是断连，不是 `0x80`**（§13 L13）：filter 在帧解码阶段就校验（Annex A / §4.7 形状），因此整个 SUBSCRIBE 包属于畸形包 ⇒ 关连接、**无 SUBACK**。`0x80` 只覆盖"请求 QoS 超过有效上限"。客户端若指望逐 topic 部分成功，需要自己保证 filter 合法。
4. **离线追赶没有优先级、没有独立队列**（设计 §5.2 E7）：重连后从 cursor 顺序读，追赶期间新到的消息与积压消息**按 offset 混在同一序列**投递；积压极深时首条消息延迟随深度线性增长（单连接顺序读）。段文件在 S1 不删，所以"理论全量可追"，但追赶深度上限要等 S2 的 retention 策略才有明确契约。
5. **at-least-once 的两个 S1 形状**（§13 L8 + §5.3）：(a) 一条连接对同一个未 PUBACK 的消息**只投一次**，S1 没有重试定时器——重发只发生在**重连**（blob 恢复项按 **同一 packet_id + DUP=1** 投一次）；(b) `.inflight` 默认攒批刷盘（`inflight_flush_interval_ms`，默认 100 ms），崩溃后 blob 回退可能让**不同消息复用同一 packet_id**（按 id 去重的客户端会误判）。`inflight_flush_interval_ms = 0` 是已实现的零回退档；T14 已按默认档 / 零回退档各跑一轮，两档都不丢消息（R7），默认档允许重发、零回退档把回退窗口压到最小。
6. **没有背压，只有停投**（§13 L14）：per-session in-flight 窗（`max_inflight`，默认 64）满了之后本轮不再投新 QoS1，等 PUBACK 或轮询 tick；不通知发布者、不丢消息、不断连，也**没有慢消费者隔离**（归 S2）。另有一条方向安全的副作用：QoS0 订阅离线时没有连接替它 skip，其分区 `committed` 停在最早未读消息（偏低＝多留数据，绝不 truncate、不丢）。

**认证面**：`broker_security` 是配置驱动桩——`authenticate/2` 失败措辞恒为 `bad_username_or_password`（不区分"用户不存在"与"密码错"），`authorize/2` 一律 `allow`，`tenant_id/1` 从用户表取（ADR-009 的 `tenant_id` 必填）；**匿名 CONNECT（无 username 且无 password）被允许，principal = client_id**（§13 L10）。空 client_id 拒绝（`0x02` Identifier rejected），因为 S1 的会话表以它为键。

