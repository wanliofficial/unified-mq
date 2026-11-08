# 协议兼容矩阵（Protocol Compatibility）

- **文档版本**：v0.6（S2.5 MQTT 5 子集实现）
- **最近一次追加**：2026-11-08，§4.2「S2.5 追加」——MQTT 5.0 CONNECT/PUBLISH/SUBSCRIBE properties、v5 reason codes、session expiry、`$share` 语法；MQTT 行拆为 3.1.1 / 5.0 两行。§4/§4.1 原文保留为 S1/S2-C 当时事实
- **对应 Roadmap**：§7 S5 协议扩展、§9.2 文档产出物
- **对应 README**：11.1 协议兼容矩阵（验证维度）
- **S5 协议顺序**：MQTT → WebSocket → NATS → AMQP → Kafka → Pulsar

> 本文件是 S5 每个协议兼容性测试的基线。S0.4 仅确定「用什么客户端 / 版本 / 语言」并验证其可获取（见 `spikes/s0_4_clients/REPORT.md`）。协议能力、端到端语义、CloudEvents Binding 由 S5 逐协议填充「支持状态」列。

---

## 0. 客户端与版本矩阵（基线）

| 协议 | S5 顺序 | 推荐客户端 | 语言 | 基线版本 | 验证 | 备注 |
|---|---|---|---|---|---|---|
| MQTT | 1 | Eclipse Paho / mosquitto_pub-sub | Python / CLI | paho-mqtt 2.x / mosquitto 2.1.2 | ✅（**T15 + S2-C**：10/10 用例） | QoS0/1/2、Retain、Will 已实现并实测；MQTT 5.0 语义在 S2.5 补；逐条限制见本文 §4 |
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
2. **MQTT 5.0 已接受（S2.5）**：`Protocol Level = 5` 的 CONNECT 正常解析并回 5 字节 v5 CONNACK（`20 03 00 00 00` 形状）。只有 **>= 5 且本实现不认识的级别**才拒绝，回 **`20 03 00 84 00`**（v5 CONNACK：flags=0、reason=`0x84` Unsupported Protocol Version、零属性）；3.1.1 及更旧的级别仍回 3.1.1 帧型 `20 02 00 01`。旧 T15 的 `paho-v5-refused`/`mosq-v5-refused` 断言（`0x87`）已作废，改由 S2.5 的 v5 连接用例覆盖。
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

> 范围冻结在 [ADR-017](adr/017-mqtt5-properties-and-compat.md) §17.1；未实现的项（topic alias、request-response、AUTH）不在本子集，继续留 S5。

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
