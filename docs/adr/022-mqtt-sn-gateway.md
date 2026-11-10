# ADR-022：MQTT-SN 网关——单 socket / 多会话、topic id 三表与休眠语义

- **状态**：已采纳（S8-C 冻结，2026-11-10）
- **日期**：2026-11-10
- **关联**：MQTT-SN 1.2 规范（§5.4 报文、§6.2 topic id、§6.13 休眠）；ADR-001（协议适配与内核分离）、ADR-010（路由边界）、ADR-013（订阅与 cursor 归属）、ADR-018（Retain/Will）；ADR-023（WS 两进程分层，反面参照）、ADR-024（QUIC 流分层）
- **关联文档**：[`docs/design/s8-c-mqtt-sn-execution-plan.md`](../design/s8-c-mqtt-sn-execution-plan.md)、[`docs/design/s8-mqtt-sn-mqtt-over-quic-and-mqtt-over-ws.md`](../design/s8-mqtt-sn-mqtt-over-quic-and-mqtt-over-ws.md) §1.2

## 背景（Context）

S8-C 要在既有 MQTT 引擎之上补 MQTT-SN 1.2（UDP）。S8-A 已经把「传输」抽成
`mqtt_transport` 行为（`send`/`arm_once`/`close`/`decode_msg`），S8-B 也已经证明
「同一份 `mqtt_conn_fsm` 换个传输」可行。

但 MQTT-SN 有一条不能照搬的前提。`mqtt_conn_fsm` 的整个生命周期建立在**一条会丢的
TCP 连接**上：半开检测、socket 所有权移交、broker 重启后的会话恢复、`{active, once}`
背压、以及「连接断了就等于会话断了」的隐含假设。UDP 上这些前提一个都不成立：

- 没有连接可拥有，一个 socket 服务所有客户端；
- 客户端可以睡眠几个小时再换个源端口醒来；
- 报文可以重复、乱序、丢失，重传信号只有客户端的 DUP 位；
- `PINGREQ` 可以**只带一个 client id**（用于唤醒，而不是心跳）。

因此问题不是「怎么把 UDP 套进 FSM」，而是「MQTT-SN 的会话该由谁拥有」。

## 决策（Decision）

### D1. 不复用 `mqtt_conn_fsm`

MQTT-SN 网关自己实现会话，**只复用下层**：`broker_router` / `broker_queue` /
`broker_subscription` / `broker_retain` / `broker_event`。

- 投递入口与其它协议完全一致：`{broker_deliver, ClientId, MaxQoS, Refs, {P, O, Msg}}`。
- `ClientId` 与 MQTT **同会话域**：QUIC / TCP / WS 上的 `client-x` 与 MQTT-SN 上的
  `client-x` 是同一个会话，互相 takeover。
- 复用 FSM 的代价是把「连接」这个概念塞进一个没有连接的世界，然后到处特判；不复用的
  代价是重写订阅/游标/retain 的接线，但那些接线本来就只有十来行。

### D2. 一个 listener 一个 UDP socket、一个 reader

```
mqtt_sn_listener (gen_server)
  ├── socket (gen_udp, {active, 100})
  └── 全部会话状态（ETS + 进程内 map）
```

- **单 reader 单进程**：`gen_udp` 用 `{active, 100}`（不是 `true`），邮箱有界；
  收到 `{udp_passive, Sock}` 后读空并 re-arm。用 `active, true` 的无界邮箱在 UDP 上
  等于把背压交给内核丢包。
- **会话按 `{IP, Port}` 索引**，`client_id` 在 CONNECT 时绑定。两个索引都要，因为
  回包要地址、takeover 要 client id。
- **回包直接 `gen_udp:send/4`**：UDP 不检查控制进程所有权，不需要 listener 中转。
- **同 client_id 从新端点连接 = takeover**，且**不发布 Will**（旧客户端的 Will 只在
  它异常消失时发布，被接管不是异常消失）。旧会话的订阅与游标保留，新端点继承。

### D3. topic id 三张表，各自的所有权不同

MQTT-SN 用 16 位 topic id 代替 topic 名字，三种类型语义完全不同：

| TopicIdType | 谁分配 | 作用域 | 生命周期 |
|---|---|---|---|
| predefined | **运维**（配置） | 全网关 | 启动时校验，保留 id `0`/`0xFFFE`/`0xFFFF` 拒绝启动 |
| normal | **网关**（每客户端） | 单客户端 | SUBACK 分配；客户端发布前须自注册；网关下行遇未知 topic 先 REGISTER 再 PUBLISH |
| short | **客户端**（两字符名字即 id） | 单客户端 | 非恰好 2 字节直接拒 |

- normal 表**双向可写**：客户端 REGISTER 上行、网关 REGISTER 下行。只做上行会让第一次
  下行推送失败。
- 上限 `max_topic_ids_per_client`。**下行超限即断开会话**：网关编不出合法的 PUBLISH，
  假装成功就是给客户端喂一条读不懂的报文。

### D4. 休眠是状态，不是连接

`DISCONNECT` 带 duration 时：

- **detach**，不是销毁：保留订阅与游标，任何 QoS1/2 消息继续进原分区队列；
- **不新建缓冲**：休眠期间没有「待发队列」，积压就在分区里，唤醒时按游标追；
- 唤醒靠 `PINGREQ(client_id)`（MQTT-SN 1.2 §5.4.25），不必从原端口；
- QoS 0 在休眠期间丢弃（没有连接可发，也不该无限期留住）；
- 超 `max_asleep_clients` 按正常断开处理并计 `mqtt_sn_sleep_denied`——上限是硬边界，
  不是尽力而为。

### D5. 每会话恰好两个 timer

- keepalive：1.5× 探测、2× 判死（规范允许的区间上限是 1.5×，但 UDP 丢包正常，
  取 2× 判死减少误杀）；
- sleep 到期。

**没有重传 timer**。MQTT-SN 的重传信号只有客户端的 DUP 位；网关对重复 msg id 幂等应答
即可。自己加定时重传会在丢包链路上放大流量。

### D6. QoS −1 不进入会话状态

QoS −1（线上两位 `3`）是「无连接投递」：无 CONNECT 也受理，只在 listener 进程 live
push，不落盘、不进游标、不产生会话。

- 这是 MQTT-SN 的设计意图（传感器单向发数据），把它当 QoS0 处理会产生大量空会话。
- 代价：订阅端离线时这条消息不存在。规范没有承诺，文档写明。

### D7. 认证边界在启动期显式失败

MQTT-SN 1.2 的 CONNECT **没有用户名/密码字段**。因此：

- `broker_security:auth_required(mqtt_sn)` 恒 `false`；
- app 直接读 `require_auth` 原始列表，命中 `mqtt_sn` 就**拒绝启动监听器**
  （`{require_auth_unsupported, mqtt_sn}`）。

拒绝粒度是整个 app：一个 socket 服务所有客户端，没有「只对部分客户端要求凭据」的表达力。
假装支持等于给运维一个静默的安全洞。

## 后果（Consequences）

- **+** 重传/休眠/唤醒/源端口漂移这些 UDP 特性不需要污染 MQTT 引擎。
- **+** 三个协议共用同一份路由/队列/订阅/retain，投递形状一致，会话可以跨协议 takeover。
- **-** 两套会话生命周期逻辑（FSM 一套、SN 一套）需要各自维护；`mqtt_sn_conn` 的
  回归保护只能靠自己的套件（84 例）。
- **-** 没有 DTLS：1884 是**唯一不受** `broker_security.tls.enabled` 控制的端口。
  DTLS 列为后续待办，运维文档必须把它写成「明文 UDP」而不是「默认安全」。
- **-** Forwarder Encapsulation 与串口桥不实现（设计 §1.2）。这两项是 MQTT-SN 的部分
  部署形态，不做意味着不能覆盖「串口传感器 + 转发器」拓扑。

## 验证（Validation）

- `mqtt_sn_codec_SUITE` 40 例：27 种报文往返、两种长度头（首字节定长 / `0x01` + 2 字节
  大端）、保留位/保留类型号/截断/尾随字节拒绝、QoS −1 的两位 `3`、short topic 的 2 字节。
- `mqtt_sn_topics_SUITE` 21 例：三张 topic id 表的所有权、上限、双向 REGISTER、
  predefined 保留 id 拒绝启动。
- `mqtt_sn_conn_SUITE` 23 例（真实 UDP）：CONNECT/teardown、三种 topic id、QoS −1/0/1/2
  双向、UNSUBSCRIBE、retained 重放、Will vs takeover、休眠 + 新端点唤醒 + 积压追赶、
  keepalive 判死、SEARCHGW/ADVERTISE、坏数据报丢弃、`require_auth` 拒绝启动。
- 真实第三方客户端：`mqtt-sn-tools` 0.0.7 **10/10**（`test/protocol-compatibility/mqtt_sn/`）。
  该套件首轮全红，抓到 codec 把 CONNECT 的 Will/CleanSession 读在 bit 7/6（应为 §5.4.4 的
  bit 3/2）导致所有合规客户端被判 `{reserved_bits, 0x04}`——这类缺陷**自编客户端不可能
  发现**，因为两侧共用同一份错误布局即可互测通过。
- 工具能力边界（Gate 文本必须单列）：`mqtt-sn-tools` 0.0.7 无 QoS 2、无 Will、
  `PINGREQ` 不带 client id。QoS2 与休眠/唤醒因此只由仓内套件覆盖。
