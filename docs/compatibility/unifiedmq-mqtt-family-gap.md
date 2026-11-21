# UnifiedMQ：MQTT / MQTT-SN / MQTT-QUIC / MQTT-WS 差异与未完成项

| 项 | 内容 |
|---|---|
| 日期 | 2026-11-12 |
| 对照 | [wanwuliandong/unified-mq](https://gitee.com/wanwuliandong/unified-mq) 当前 `main`，相对 MQTT 3.1.1、MQTT 5.0、MQTT-SN 1.2、MQTT over WebSocket（3.1.1 §6 / 5.0 §6）、OASIS《MQTT over QUIC — Single Stream Mode》CN Draft 3 |
| 证据 | `docs/protocol-compatibility.md` §4–§4.2、§4.11–§4.13；`docs/design/s2.5-mqtt5-execution-plan.md`；ADR-012/017/018/022/023/024；`apps/broker_proto_mqtt*` |
| 不在本文 | 把本仓库改成 EMQX / mosquitto 源码；规则引擎、Web 控制台、Helm 等产品功能只点到为止 |

## 1. 四者不是四套协议实现

明文 MQTT、MQTT over WebSocket、MQTT over QUIC **共用** `mqtt_conn_fsm` + `mqtt_frame`。QoS、会话、Retain、Will、MQTT 5 子集在这三条上是同一份代码。后两者只换传输。

MQTT-SN **不走** 这台状态机。UDP 没有连接，报文也不是字节流。`mqtt_sn_conn` 自己做会话，只复用下层的 `broker_subscription` / `broker_queue` / `broker_router` / `broker_retain`。

| | 明文 MQTT | MQTT-WS | MQTT-QUIC | MQTT-SN |
|---|---|---|---|---|
| 代码 | `broker_proto_mqtt`，`mqtt_conn_fsm.erl` 为主 | `broker_proto_mqtt_ws`，约 450 行传输壳 | `broker_proto_mqtt_quic`，约 720 行传输壳 | `broker_proto_mqtt_sn`，约 2,300 行，独立连接机 |
| 默认端口 | 1883（TLS 走同一 listener 开关） | 8083 / wss | UDP 14567 | UDP 1884 |
| 引擎 | `mqtt_conn_fsm` | 同一份 | 同一份 | `mqtt_sn_conn` |
| 版本 | 3.1.1 + 5.0 子集 | 同左 | 同左 | MQTT-SN 1.2（对齐 3.1 时代，无 MQTT 5） |
| 第三方证据 | paho 2.x + mosquitto 2.1.2 | paho 2.1.0 websockets，8/8 | EMQX `emqtt` 1.16.1，8/8 | `mqtt-sn-tools` 0.0.7，10/10 |
| 默认就能监听 | 是 | 是；wss 取决于 TLS 开关 | 否。默认构建无 NIF；无证书拒绝启动 | 是，而且是明文 UDP |

因此：明文 MQTT 上没做完的 MQTT 5 行为，WebSocket 和 QUIC **原样继承**。MQTT-SN 要单独看，不能用「1883 测过了」代替。

## 2. 已经做完、不要再当成缺口

三条共用引擎上，3.1.1 的主路径是完成的，不是半截：

- CONNECT / CONNACK、PUBLISH QoS 0/1/2、PUBACK、PUBREC / PUBREL / PUBCOMP、SUBSCRIBE / SUBACK、UNSUBSCRIBE / UNSUBACK、PING、DISCONNECT。
- 通配符 `+` / `#`，非法 filter 在解码期断连。
- clean / 持久会话，订阅、游标、in-flight 跨连接保留；重连后按游标追赶。
- Retain：独立 `broker_retain`，订阅后补发，空 payload 清除，重启还在。
- Will：异常断连、keepalive 超时、协议错误、takeover 会发；正常 DISCONNECT 和 broker 停机不发。
- QoS 2 的四步握手和跨重连不重复。未确认的 QoS 1 在重连时用同一 packet id、`DUP=1` 再投一次。
- MQTT 5 里已经生效的：Session Expiry、Receive Maximum（只收窄出站窗口）、Maximum Packet Size、User Properties、Payload Format、Content Type、Message Expiry、Response Topic 与 Correlation Data 的存储并在投递时写回、v5 reason code、`$share/{group}/{filter}` 的单节点竞争消费。
- 真实客户端字节级对照做过（T15）。`kill -9` 恢复做过（T14）。

MQTT-SN 已实现规范里的 27 种报文、两种长度头、QoS −1/0/1/2、Retain、Will 四步握手与更新（含 v1.2 §5.4.4 Will Delay Interval：异常断连延迟发布、窗口内重连取消）、predefined / normal / short 三种 topic id、休眠与换端口唤醒、SEARCHGW / ADVERTISE、keepalive 1.5× 探测和 2× 判死。

MQTT-WS 已实现子协议 `mqtt` / `mqttv3.1` / `mqttv3.1.1`、任意 path、binary 帧、跨帧重组、单帧多报文、ping/pong、wss。

MQTT-QUIC 已实现 ALPN `mqtt`、单条双向流承载整条 MQTT 连接、流关闭不等于连接关闭、强制 TLS 1.3。

## 3. 明文 MQTT：没做完 / 明确选择的部分

下面这些在 WebSocket 和 QUIC 上同样适用（WS/QUIC 共用 `mqtt_conn_fsm`，行为原样继承）。级别按「客户端能不能看出来」而不是按内部 TODO。标「已支持」的 MQ-01 到 MQ-07 是 MQTT 5 子集的交付记录，保留在此是为了与 MQ-08 之后的范围外/语义选择项对照；MQ-08 到 MQ-14 不是没写完的报文，而是已选定并测试覆盖的语义，只是需对外说明。

| ID | 级别 | 项 | 现在 | 规范或常见 broker |
|---|---|---|---|---|
| MQ-01 | 已支持 | Subscription Identifier | SUBSCRIBE 写入连接状态 `sub_ids`；出站 PUBLISH 经 `matching_ids/3` 把每个匹配且未被 no-local 抑制的订阅 identifier 带进 PUBLISH 属性（§3.3.4） | MQTT 5：匹配该订阅的 PUBLISH 带 identifier |
| MQ-02 | 已支持 | Will Delay | CONNECT 的 `will_delay_interval` 生效：`will_of/1` 保留 `delay`，`publish_will/1` 延迟>0 时停靠 `mqtt_will_server`，重连取消 | 断开后等待 Will Delay，期间重连则取消 |
| MQ-03 | 已支持 | SUBSCRIBE 选项 | `no_local` / `retain_as_published` / `retain_handling` 全部生效：`sub_flags = {NL, RAP}` 驱动 no-local 抑制与 RAP retain；`retain_handling`（RH）在订阅时刻由 `replay_wanted/3` 决定 retained 补发（RH=0 总补、RH=1 仅新订阅、RH=2 不补） | `retain_handling=2` 不再补发 retained；`no_local` 不把本连接发布投回来 |
| MQ-04 | 已支持 | Topic Alias | 入站按 §3.3.2.3.4 校验+解析别名（alias 0 或超过 CONNACK 宣布的上限 → 0x94；非空 topic + 合法 alias 登记/覆盖映射；空 topic + 未登记 alias → 0x94；空 topic 无 alias → 0x91）；出站按 §3.3.2.3.4 为 topic 分配别名写进 PUBLISH 属性，但**上限取自客户端 CONNECT 的 `topic_alias_maximum`**（MQTT-3.3.2-11，客户端未给/0 则完全不出站别名），用尽即停发、不回收；别名映射表只存活于本次网络连接，跨重连即丢弃（MQTT-3.3.2-7），会话 blob 仍是 vsn 3、不含别名键 | env `mqtt_topic_alias_maximum` 默认 **0**（关闭入站别名）：CONNACK 不发该属性、入站 alias 一律 0x94（零回归）。设 >0 时 CONNACK 宣布该上限并启用入站别名解析；出站别名只看各客户端自己在 CONNECT 里给的最大值，与本 env 无关 |
| MQ-05 | 已支持 | AUTH 增强认证 | AUTH 包（type 15，仅 v5）与 challenge/response：`broker_auth` 框架按 `mqtt_auth_methods` 派发到 `broker_auth_<name>`，内置 `plain` 方法做 nonce 绑定的用户名/密码校验并复用既有用户库；会话内 re-auth 成功回 AUTH `0x00`，失败回 DISCONNECT `0x9C`（2026-11-21 落地于 mq-auth-connack） | MQTT 5 AUTH；也没有 OIDC/JWKS |
| MQ-06 | 已支持 | Request Response Information | CONNECT 带 `request_response_information=1` 且 broker 配了 `mqtt_response_information`（默认 undefined 关）时，成功 CONNACK 回填该值作为响应主题前缀；客户端据此拼 `Response Topic = RI ++ Correlation Data`。请求/响应经既有 topic 路由透传 `response_topic`/`correlation_data`，无特殊路由（2026-11-21 落地于 mq-request-response） | 这是 broker 分配响应主题命名空间的能力，不是消息字段透传 |
| MQ-07 | 已支持 | CONNACK 能力回填 | v5 CONNACK 在连接成功（RC=0）时回填**四个**静态能力位：retain / wildcard / subscription-identifier / shared-subscription available 均为 1；**不发 `maximum_qos`**（§3.2.2.3.4 只允许 0/1，值 2 是 Protocol Error；缺省即表示最大 QoS 2，支持 QoS 2 靠省略表达，MQTT-3.2.2-9）；由 `mqtt_connack_props_v5` 开关控制（默认开）。Server Keep Alive、Assigned Client Identifier 仍不回（2026-11-21 落地于 mq-auth-connack，同日按规范修正 maximum_qos） | 不回不等于撒谎；空 ClientID 仍直接拒绝，不会分配一个 |
| MQ-08 | 语义缺口 | QoS 2 跨崩溃 | 崩溃后允许重复，对外明确「至少一次」而非 exactly-once；线协议和跨重连不重复已成立，对外说明已写入 `docs/mqtt-feature-comparison.md`（§3 状态表 + QoS 行 + 已知限制） | 不是 exactly-once。这是设计，不是忘了写 |
| MQ-09 | 语义缺口 | 没有发布侧重试定时器 | 同一条连接对未确认消息只投一次。重发只发生在重连 | 规范允许发送方重试。慢客户端或丢 PUBACK 时，要断线才补。对外说明已写入 `docs/mqtt-feature-comparison.md` §4.6 |
| MQ-10 | 语义缺口 | in-flight 默认 100ms 才刷盘 | 崩溃可能让**不同消息**复用同一个 packet id。`inflight_flush_interval_ms=0` 可以把窗口压到最小 | 按 packet id 去重的客户端会误伤。对外说明已写入 §4.6 |
| MQ-11 | 语义缺口 | 只有停投，没有背压 | in-flight 默认 64，满了就停。不通知发布者，不踢慢消费者 | 生产上慢订阅者会拖住分区。当前「停投」语义已对外写入 §4.6；per-subscriber 背压仍为待办 |
| MQ-12 | 语义缺口 | 超大出站消息被丢弃 | 超过客户端 Maximum Packet Size 的 PUBLISH 丢弃并计数，不拆、不改为错误码通知发布者 | 订阅端会缺消息。对外说明已写入 §4.6 |
| MQ-13 | 语义缺口 | Will 不落盘 | 只在连接进程内存里。broker 重启不补发遗嘱。mosquitto 默认也这样 | 要「进程被 kill 也发遗嘱」则没做。对外说明已写入 §4.6 |
| MQ-14 | 语义缺口 | 离线追赶无优先级 | 积压和新消息按 offset 混在一个序列里，深度越大首条越慢 | 没有独立离线队列。对外说明已写入 §4.6 |
| MQ-15 | 未做完 | 共享订阅只在单节点 | `$share` 能建组、竞争消费。没有集群范围的再均衡 | 多节点时每个节点一组，不是一个消费组 |
| MQ-16 | 未做 | MQTT 3.1 | Protocol Level 3 走 3.1.1 的拒绝帧 `20 02 00 01` | 只保证 3.1.1 和 5.0 子集 |
| MQ-17 | 产品，不是报文缺口 | 桥接、规则引擎、集群会话副本的故障注入证明、吊销踢人、Prometheus/OTel | `docs/mqtt-feature-comparison.md` 里 `#5`–`#28` 在 2026-11-10 仍是 pending | 对标 fss-mqtt-broker / robustmq 时的差距，不是 3.1.1 互操作的前置条件 |

MQ-01、MQ-02、MQ-03、MQ-04 已全部补齐（MQ-01/02/03 由 `120f1c8`「MQTT 5.0 Will Delay and SUBSCRIBE subscription options」落地；MQ-04 Topic Alias 由后续特性落地）。MQ-05 到 MQ-07 已全部落地（MQ-05 AUTH 与 MQ-07 CONNACK 能力回填由 `mq-auth-connack`，MQ-06 Request Response Information 由 `mq-request-response`）。MQ-08 到 MQ-14 是已经选定的语义，测试也覆盖了，对外说明已写入 `docs/mqtt-feature-comparison.md` §4.6（每条标注「刻意取舍/当前语义」，非缺陷），缺的不再是说明，而是一致的产品化取舍。

（2026-11-21 更正：MQ-05 AUTH、MQ-06 Request Response Information、MQ-07 CONNACK 能力回填三项均已落地，见上表；MQTT 5 子集协议特性已全部补齐，剩余范围外项为 MQTT 3.1 与各传输层封装。）

## 4. MQTT-SN：和另外三条的差异，以及没做完的

它不是「把 1883 的字节放进 UDP」。

| 只有 SN 有 | 说明 |
|---|---|
| QoS −1 | 无 CONNECT 也收。只允许 predefined / short topic。不落盘、不进会话。订阅者不在线就没了 |
| Topic id | predefined 要配置；normal 每个客户端一张表，订阅方拿到的 id 对发布方无意义；short 必须刚好两字节 |
| 休眠 | `DISCONNECT(duration)` 只 detach。QoS 1/2 留在分区里，唤醒后按游标追。QoS 0 在睡眠期间丢弃 |
| 发现 | SEARCHGW 回 GWINFO；可广播 ADVERTISE |
| 重传 | **没有**网关侧重传定时器。只对客户端 DUP 做幂等 |

和明文 MQTT 的行为差：

- 不支持 MQTT 5 的任何属性。Session Expiry、User Properties、`$share` 都没有。
- CONNECT 没有用户名和密码。配置里如果把 `mqtt_sn` 放进 `require_auth`，监听器拒绝启动，而不是匿名放行。
- 明文 UDP，不吃 `broker_security.tls.enabled`。这是四条里唯一没有 TLS 路径的端口。
- 同一 `client_id` 换源地址重连算 takeover，**不发 Will**。
- 下行 topic id 超过 `max_topic_ids_per_client`（默认 1024）会拆掉会话，而不是继续用名字发表。

没做完，而且设计写明了不做：

| ID | 项 | 状态 |
|---|---|---|
| SN-01 | DTLS | 已落地（2026-11-17）：`transport => dtls` PSK-only listener，身份即认证；明文 UDP 保持默认，需显式配置 |
| SN-02 | Forwarder Encapsulation（1.2 §5.5） | 未做。转发器后面的传感器网关拓扑覆盖不了 |
| SN-03 | 串口桥、把 SN 再桥到外部 MQTT broker | 未做。这里是终结网关，不是桥 |
| SN-04 | 第三方证据不完整 | `mqtt-sn-tools` 0.0.7 没有 QoS 2、没有 Will、PINGREQ 不带 client id。这三块只有仓内 `mqtt_sn_conn_SUITE`（23 例）。不能把 10/10 理解成工具覆盖了全部语义 |

QoS 0 在休眠时丢弃是协议容量下的明确选择，不是漏实现：QoS 0 本来就不进日志。

## 5. MQTT over WebSocket：传输差在哪，报文没有第二套

语义与 1883 相同，所以第 3 节的缺口全部带着。传输上额外这些：

| ID | 项 | 现在 |
|---|---|---|
| WS-01 | 子协议 | 必须是 `mqtt`、`mqttv3.1` 或 `mqttv3.1.1`，否则 HTTP 400。浏览器随便开一条 WebSocket 不会被当成 MQTT |
| WS-02 | 帧类型 | 只收 binary。text 关连接，close code 1003 |
| WS-03 | 压缩 | `permessage-deflate` 不协商。客户端带了这个扩展也只会得到未压缩连接 |
| WS-04 | 背压 | `arm_once` 在 WS 上是空操作。窗口只有 MQTT 的 `max_inflight` / Receive Maximum |
| WS-05 | 两个上限 | WS 帧默认 16 MiB（超出 close 1002），MQTT 报文另有 `mqtt_max_packet_size`。调一个不会带动另一个 |
| WS-06 | 和自有 WebSocket API 分开 | 8083 是 MQTT。8080 的 `broker_proto_ws` 是另一套 JSON/二进制 API，不能拿 MQTT 客户端去连 |

已完成、不要再列成缺口：任意 URL path、一个 MQTT 包跨多个 WS 帧、一个 WS 帧里多个 MQTT 包、WS ping 与 MQTT keepalive 互不替代、wss。

## 6. MQTT over QUIC：传输差在哪，报文同样没有第二套

语义同样继承第 3 节。相对 TCP 多出来的是连接模型，不是新的 MQTT：

| ID | 项 | 现在 |
|---|---|---|
| QU-01 | 单流 | 一条客户端发起的双向流 = 一条 MQTT 连接。服务端不主动开流 |
| QU-02 | 流和连接 | 关掉流只结束这条 MQTT 会话。同一 QUIC 连接上再开一条流是一次新的 CONNECT。同 client id 与 TCP 会话互相 takeover |
| QU-03 | 0-RTT | 不接收 early data。CN 里是可选项 |
| QU-04 | 多流 / datagram | 不做。多流是 EMQX 私有扩展，不在这份 CN 里 |
| QU-05 | 流信用 | `max_streams_per_conn` 默认 4，在建连时一次给完。quicer 0.4.12 运行中补信用无效。同一连接上重连超过这个次数，必须新开 QUIC 连接。这是库的限制，代码绕不过 |
| QU-06 | 构建与监听 | 只有 `rebar3 as quic` 才带 NIF。没证书监听器拒绝启动，避免把 release 拉起来后静默没有端口 |
| QU-07 | 证据边界 | `emqtt` 的 QUIC 是实验 API，没有「只关流、不关连接」。流级重连只在仓内 CT 里用 raw quicer 证明过，不在 8 个第三方用例里 |
| QU-08 | 连接迁移 | 设计阶段写了不做迁移压测，也没有「换网络后会话还在这条 QUIC 连接上」的承诺 |

TLS 1.3 是强制的，不是可选项。`tls.enabled` 在这里只表示证书从哪读。

## 7. 对照：同一件事四条路上怎样

| 能力 | 明文 | WS | QUIC | SN |
|---|---|---|---|---|
| QoS 0/1/2 | 完成 | 同左 | 同左 | 完成；QoS 2 无第三方客户端证据 |
| QoS −1 | 无此概念 | 无 | 无 | 完成，不落盘 |
| Retain / Will | 完成；Will 不跨 broker 重启 | 同左 | 同左 | 完成；Will 只有仓内测试 |
| 持久会话与离线追赶 | 完成 | 同左 | 同左 | 休眠是另一条路径；QoS 0 睡眠即丢 |
| MQTT 5 子集 | 第 3 节 | 同左 | 同左 | 不适用 |
| Subscription Identifier 下发 | 完成（MQ-01） | 同左 | 同左 | 无 |
| Will Delay | 完成（MQ-02） | 同左 | 同左 | 无此字段（SN 对齐 3.1 时代） |
| SUBSCRIBE 选项（no_local / retain_as_published / retain_handling） | 完成（MQ-03） | 同左 | 同左 | 无 |
| Topic Alias | 已支持（默认关闭，`mqtt_topic_alias_maximum` >0 启用） | 同左 | 同左 | 无 |
| AUTH | 完成（MQ-05） | 同左 | 同左 | 无 |
| Request Response | 完成（MQ-06，默认关闭，`mqtt_response_information` 配置后回填） | 同左 | 同左 | 无 |
| 共享订阅 | 单节点 | 同左 | 同左 | 无 `$share` |
| 用户名密码 | 有 | 有 | 有 | 协议没有；要求认证则拒绝启动 |
| TLS | 可选 | wss 可选 | 强制 TLS 1.3 | 无，DTLS 未做 |
| 默认构建可用 | 是 | 是 | 否 | 是 |

## 8. 结论

功能没有「四套都只写了一半」。3.1.1 的发布、订阅、QoS、Retain、Will、会话，在明文、WebSocket、QUIC 上是做完的。MQTT-SN 1.2 的报文和网关语义也做完了主路径。MQTT 5 子集里 Subscription Identifier 下发、Will Delay、SUBSCRIBE 选项（no_local / retain_as_published / retain_handling）也已补齐（MQ-01/02/03）。

MQ-08 到 MQ-14 这一类不是「没写完的报文」，而是已经选定的语义、测试也覆盖了，只是需要对外说明清楚，避免被当成半成品：

- QoS 2 跨崩溃：线协议和跨重连不重复是成立的，对外明确「至少一次」而非 exactly-once（MQ-08）。
- 发布侧重试：同一连接对未确认消息只投一次，重发只发生在重连；规范允许发送方重试，当前选择不在连接内做重试定时器（MQ-09）。
- in-flight 默认 100ms 才刷盘：崩溃可能让不同消息复用同一 packet id，`inflight_flush_interval_ms=0` 可压到最小；按 packet id 去重的客户端需知晓（MQ-10）。
- 只有停投、没有背压通知：in-flight 满了就停，不通知发布者、不踢慢消费者（MQ-11）。
- 超大出站消息：超过客户端 Maximum Packet Size 的 PUBLISH 丢弃并计数，不拆、不改错误码（MQ-12）。
- Will 不落盘：只在连接进程内存，broker 重启不补发；要「进程被 kill 也发遗嘱」则没做（MQ-13）。
- 离线追赶无优先级：积压和新消息按 offset 混在一个序列，没有独立离线队列（MQ-14）。

已经从范围外补上的：AUTH 增强认证（MQ-05）、CONNACK 能力回填（MQ-07）、Topic Alias（MQ-04，默认关闭，`mqtt_topic_alias_maximum` >0 启用）与 Request Response Information（MQ-06，默认关闭，`mqtt_response_information` 配置后回填），AUTH/CONNACK 于 2026-11-21 落地于 `mq-auth-connack`，MQ-06 于同日落地于 `mq-request-response`。

明确列在范围外、还没做的：MQTT 3.1、SN 的 DTLS 和转发器封装、QUIC 的 0-RTT 和多流、WS 的 `permessage-deflate`。

不要把「paho 8/8」理解成 MQTT 5 完整实现。那些用例覆盖的是连接、QoS、Retain、Will 和一条 5.0 往返，没有覆盖上面这些语义选择与范围外能力。