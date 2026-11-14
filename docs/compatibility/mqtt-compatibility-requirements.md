# UnifiedMQ MQTT 四协议差异分析与改进需求

## 文档信息

| 属性 | 内容 |
|---|---|
| 项目 | UnifiedMQ |
| 文档名称 | MQTT / MQTT-SN / MQTT over QUIC / MQTT over WebSocket 差异分析与改进需求 |
| 文档版本 | V1.0 |
| 分析基准 | Gitee `main` 分支公开文档，重点参考 2026-11-11 更新的协议兼容矩阵 |
| 编写日期 | 2026-11-12 |
| 目标读者 | 协议研发、Broker 内核、测试、架构评审 |
| 主要代码模块 | `broker_proto_mqtt`、`broker_proto_mqtt_sn`、`broker_proto_mqtt_quic`、`broker_proto_mqtt_ws` |

> 本文以仓库公开的 `docs/protocol-compatibility.md`、`docs/mqtt-feature-comparison.md`、`docs/roadmap.md` 及目录划分为主要证据。当前可确认的是仓库文档中声明的实现能力和测试证据；本文没有在本执行环境中完成 Gitee 仓库的本地克隆、逐文件静态审计或重新运行测试。因此，“未完成”分为三类：**文档明确未实现**、**文档明确存在限制**、**尚缺少足够证据，需要补测/代码审计**。不把第三类直接判定为代码缺失。

---

# 1. 总体结论

UnifiedMQ 的四种 MQTT 系接入并不是四套完全平行、功能相同的 MQTT 协议实现：

1. **MQTT TCP、MQTT over WebSocket、MQTT over QUIC 共用 `mqtt_conn_fsm`**。三者共享 MQTT 3.1.1 / 5.0 的连接状态机和核心消息语义，差异主要在传输适配、握手、帧封装、TLS/QUIC 和流控。
2. **MQTT-SN 1.2 是独立协议实现**。它使用 UDP、短 TopicId、网关发现、睡眠客户端等 MQTT-SN 特有机制，不复用 `mqtt_conn_fsm`，需要独立维护报文状态机和到统一消息内核的适配。
3. 四种实现均已有第三方客户端互操作或专用测试证据，但这不等于通过 OASIS/正式标准认证，也不意味着所有协议扩展和故障场景都已覆盖。
4. 目前最需要优先关注的缺口是：
   - MQTT 5.0 的 Topic Alias、Enhanced Authentication、Subscription Identifier 透传；
   - MQTT QoS 2 在跨进程/节点崩溃后的语义，目前文档明确为“至少一次”，不能宣称跨崩溃 Exactly Once；
   - MQTT-SN 的 DTLS、安全身份接入及 Forwarder Encapsulation/串口桥接；
   - MQTT over QUIC 的运行期 Stream Credit 补充，以及连接迁移、流控和异常恢复的系统测试；
   - MQTT over WebSocket 的背压实现，当前 `arm_once` 为 no-op；
   - 四条传输路径的跨节点会话恢复、异常网络和慢消费者场景仍需形成统一验收矩阵。

## 1.1 四种实现定位

| 协议 | 网络承载 | 协议模型 | 是否复用 MQTT 状态机 | 当前定位 |
|---|---|---|---|---|
| MQTT | TCP（可结合 TLS） | MQTT 3.1.1 / 5.0 | 核心实现 | 主协议入口 |
| MQTT-SN | UDP，默认 1884 | MQTT-SN 1.2 | 否，独立 FSM | 低带宽/受限设备网关 |
| MQTT over QUIC | QUIC over UDP，默认 14567 | MQTT over QUIC Single Stream Mode（CN Draft 3） | 是 | 新型安全传输入口 |
| MQTT over WebSocket | RFC 6455 WebSocket，支持 WSS | MQTT Binary Frame | 是 | 浏览器/受限网络入口 |

## 1.2 架构关系

```text
                      MQTT Clients
                            |
          +-----------------+------------------+
          |                 |                  |
       MQTT/TCP         MQTT/WebSocket      MQTT/QUIC
          |                 |                  |
          |             RFC 6455             QUIC
          |             Binary Frame       TLS 1.3
          |                 |                  |
          +-----------------+------------------+
                            |
                      mqtt_conn_fsm
                            |
               MQTT 3.1.1 / MQTT 5.0 Semantics
                            |
               Session / Subscription / QoS
                            |
                 UnifiedMQ Message Kernel
                            |
                Queue / Router / Storage

       MQTT-SN Client
             |
          UDP/1884
             |
      MQTT-SN Gateway FSM
             |
      TopicId / Sleep / Will / QoS
             |
      MQTT-SN Adapter / Mapping
             |
      UnifiedMQ Message Kernel
```

**架构约束：** MQTT TCP、WS、QUIC 对同一 MQTT 语义的修复应落在共享 MQTT 引擎或明确的传输适配层，不应复制三份实现。MQTT-SN 必须保留自身语义，但在进入统一消息内核时应有明确、可测试的映射规则。

---

# 2. 能力对比总表

状态说明：
- **已实现（文档有测试证据）**：仓库兼容矩阵明确列出能力及对应测试路径。
- **部分实现/有限制**：已有实现，但存在明确的语义或运行条件限制。
- **未实现**：文档明确声明没有实现。
- **待核验**：公开矩阵没有足够细节，需进一步审计代码或补充测试。
- **非必需扩展**：不是对应协议核心互操作的必选项，可根据目标场景排期。

| 能力 | MQTT TCP | MQTT-SN | MQTT over QUIC | MQTT over WebSocket |
|---|---|---|---|---|
| 协议版本 | MQTT 3.1.1 / 5.0 | MQTT-SN 1.2 | MQTT over QUIC Single Stream Mode CN Draft 3，承载 MQTT | MQTT 3.1.1 / 5.0 |
| 传输 | TCP | UDP | QUIC/UDP | WebSocket/TCP |
| 核心 FSM | `mqtt_conn_fsm` | 独立 MQTT-SN FSM | `mqtt_conn_fsm` | `mqtt_conn_fsm` |
| QoS 0/1/2 | 已实现 | 已实现 | 继承 MQTT 引擎 | 继承 MQTT 引擎 |
| MQTT-SN QoS -1 | 不适用 | 已实现 | 不适用 | 不适用 |
| Retain | 已实现 | 已实现 | 继承 MQTT 引擎 | 继承 MQTT 引擎 |
| Will | 已实现 | 已实现，含 Will 更新流程 | 继承 MQTT 引擎 | 继承 MQTT 引擎 |
| Clean/Persistent Session | 已实现 | 有 MQTT-SN 自身睡眠/唤醒语义 | 继承 MQTT 引擎 | 继承 MQTT 引擎 |
| 离线消息追赶 | 已实现 | 睡眠期间 QoS 0 丢弃；QoS 1/2 行为需重点测试 | 继承 MQTT 引擎 | 继承 MQTT 引擎 |
| MQTT 5 Session Expiry | 已实现（子集） | 不适用 | 继承 MQTT 引擎 | 继承 MQTT 引擎 |
| MQTT 5 Message Expiry | 已实现（子集） | 不适用 | 继承 MQTT 引擎 | 继承 MQTT 引擎 |
| MQTT 5 Reason Code / Properties | 已实现（子集） | 不适用 | 继承 MQTT 引擎 | 继承 MQTT 引擎 |
| MQTT 5 Shared Subscription | `$share/{group}/{filter}` 已实现 | 不适用 | 继承 MQTT 引擎 | 继承 MQTT 引擎 |
| Topic Alias | 未实现 | 不适用 | 未实现（共享引擎限制） | 未实现（共享引擎限制） |
| MQTT 5 Enhanced AUTH | 未实现 | 不适用；CONNECT 无凭据字段 | 未实现（共享引擎限制） | 未实现（共享引擎限制） |
| Subscription Identifier 投递透传 | 未透传 | 不适用 | 未透传（共享引擎限制） | 未透传（共享引擎限制） |
| 加密 | TCP TLS 能力需结合统一安全配置核验 | 无 DTLS，明文 UDP | 强制 TLS 1.3 | 支持 WSS |
| 标准协议发现/协商 | MQTT CONNECT | SEARCHGW / ADVERTISE | ALPN 固定 `mqtt` | WebSocket 子协议协商 |
| 高级传输能力 | TCP 流控 | UDP 丢包/乱序处理 | 无 0-RTT、无多流、运行期不补 Stream Credit | 无 `permessage-deflate`，背压有缺口 |
| 真实第三方客户端 | Paho / Mosquitto，10/10 | mqtt-sn-tools，10/10 | EMQX `emqtt`，8/8 | Paho，8/8 |
| 专用 CT | MQTT 多套件，见仓库测试 | 84 例 | 基线 17 例；QUIC profile 额外展开 listener 测试 | 19 例 |

> MQTT TCP 的 TLS、安全配置细节应结合 `docs/security.md` 和实际配置逐项确认；不要因为 MQTT over QUIC 强制 TLS 1.3，就推断普通 MQTT TCP 也采用同样的强制策略。

---

# 3. MQTT TCP（标准 MQTT）实现分析

## 3.1 已实现能力

当前主 MQTT 引擎声明支持 MQTT 3.1.1 和 MQTT 5.0 子集，主要能力包括：

### 连接与会话
- CONNECT / CONNACK 基础连接流程。
- Clean Session / Clean Start 对应的会话处理。
- Persistent Session。
- 离线消息追赶。
- Keep Alive 与连接状态管理。
- 连接断开后的会话处理。

### 发布与订阅
- PUBLISH / SUBSCRIBE / UNSUBSCRIBE。
- QoS 0、QoS 1、QoS 2。
- Retain 保留消息。
- Will 遗嘱消息。
- Topic Filter 与通配符匹配。
- Shared Subscription：`$share/{group}/{filter}`。

### MQTT 5.0 子集
- Session Expiry Interval。
- Message Expiry Interval。
- Reason Codes。
- Properties（子集，具体属性应逐项建立清单）。
- Shared Subscription。

### 验证证据
- 真实客户端基线：Eclipse Paho / Mosquitto。
- 仓库记录的客户端集成验证为 10/10。
- 主要测试目录：`apps/broker_proto_mqtt/test/`、`test/protocol-compatibility/mqtt/`。

## 3.2 明确未实现或存在限制的功能

### MQTT-001：Topic Alias 未实现

MQTT 5.0 Topic Alias 允许在连接范围内用较短别名代替较长 Topic Name，减少重复 Topic 字符串带来的报文开销。

当前状态：未实现。

需要补充：
- CONNECT/CONNACK 相关 Topic Alias Maximum 协商。
- 入站 Topic Alias 表（客户端到服务端）。
- 出站 Topic Alias 表（服务端到客户端）。
- Alias 生命周期与连接隔离。
- Alias 越界、未定义、重复映射等异常处理。
- 断线重连后 Alias 表清空/重建。

优先级：P1。对高频、长 Topic 的 IoT 设备有一定价值，但不应阻塞基本 MQTT 5.0 互操作。

### MQTT-002：MQTT 5.0 Enhanced Authentication 未实现

当前未实现基于 AUTH 报文的增强认证扩展。

需要补充：
- AUTH 报文与认证方法协商。
- Challenge / Response 多轮交互。
- 认证中断、超时和失败 Reason Code。
- 与统一身份表、外部认证插件的关联。
- 认证过程中禁止未授权 Publish / Subscribe。

优先级：P1；对于需要 SASL/OAuth 类挑战响应认证的环境，应提升到 P0。

### MQTT-003：Subscription Identifier 未透传到投递帧

当前文档指出 Subscription Identifier 不会透传到 PUBLISH 投递帧。

影响：客户端无法通过该属性识别一次投递匹配了哪个订阅标识，部分依赖该属性的 MQTT 5.0 应用行为会受限。

需要补充：
- SUBSCRIBE 中 Subscription Identifier 解析和保存。
- 单条发布匹配多个订阅时的标识处理。
- PUBLISH 属性组装与编码。
- 多订阅、共享订阅、重连恢复测试。

优先级：P1。

### MQTT-004：QoS 2 跨崩溃语义降级

当前文档明确指出：QoS 2 跨崩溃仍为至少一次。

这意味着在线正常流程可以实现 QoS 2 握手，但如果在 QoS 2 状态机中途发生进程/节点崩溃，恢复后无法据此承诺严格的跨崩溃 Exactly Once。

需要研究：
- QoS 2 状态持久化点。
- PUBLISH / PUBREC / PUBREL / PUBCOMP 事务状态恢复。
- 接收端和发送端状态恢复。
- Session 与 QoS 2 inflight 状态的一致性。
- 重复报文识别与 Packet Identifier 生命周期。
- 崩溃注入和重连恢复测试。

优先级：P0（若对外宣称 QoS 2 跨故障可靠）；否则必须在产品兼容声明中明确限制。

## 3.3 仍需补齐的核验清单

以下项目不能仅凭当前矩阵判定为未实现，应逐项审计代码和补充测试：

- MQTT 5.0 全部 CONNECT / CONNACK / PUBLISH / PUBACK / PUBREC / PUBREL / PUBCOMP / SUBSCRIBE / SUBACK / UNSUBSCRIBE / UNSUBACK / DISCONNECT 属性覆盖表。
- Server Keep Alive、Maximum Packet Size、Receive Maximum、Maximum QoS、Retain Available、Wildcard Subscription Available、Subscription Identifier Available、Shared Subscription Available 等能力协商。
- PUBLISH Payload Format Indicator 与 Content Type 的传递。
- User Properties 在跨协议转换时的保留策略。
- Will Delay Interval 与 Session Expiry 的组合行为。
- Retain Handling / Retain As Published / No Local 的完整语义。
- 同一客户端 ID 并发连接时旧会话踢除和会话接管。
- 离线队列长度限制、过期清理、磁盘压力控制。
- 共享订阅在集群节点之间的消费者分配与故障恢复。

---

# 4. MQTT-SN 1.2 实现分析

## 4.1 与标准 MQTT 的根本区别

MQTT-SN 不是把 MQTT 二进制包简单改成 UDP。它是面向传感器网络设计的独立协议，采用 TopicId、网关发现、睡眠客户端等机制。

因此 UnifiedMQ 为其建立独立的 `broker_proto_mqtt_sn` Application 和 FSM 是合理的，不应强行复用标准 MQTT 报文状态机。

## 4.2 已实现能力

当前文档声明 MQTT-SN 1.2 over UDP，默认端口 1884，并支持全部 27 种 MQTT-SN 报文。

### 报文与 QoS
- 全部 27 种 MQTT-SN 1.2 报文类型。
- QoS -1 / 0 / 1 / 2。
- Retain。
- Will。
- Will Topic / Will Message 协商。
- WILLTOPICUPD / WILLMSGUPD 更新流程。

### TopicId 管理
- Pre-defined Topic ID。
- Normal Topic ID：每客户端双向 REGISTER。
- Short Topic：两字符短 Topic。

### 网关与睡眠设备
- SEARCHGW / ADVERTISE 网关发现。
- 睡眠客户端支持。
- PINGREQ(client_id) 唤醒流程。
- Keepalive 检测，文档记录采用 1.5× / 2× 时间窗口。

### 验证证据
- 第三方客户端：`mqtt-sn-tools` 0.0.7，固定提交 `a1dd870`。
- 真实客户端集成：10/10。
- CT：84 例。
- 测试目录：`apps/broker_proto_mqtt_sn/test/`、`test/protocol-compatibility/mqtt_sn/`。

## 4.3 明确未实现或存在限制的功能

### MQTTSN-001：DTLS 未实现

当前 MQTT-SN 使用明文 UDP，没有 DTLS。

影响：
- 无法在传感器到网关链路上提供标准传输层机密性和完整性保护。
- 不能依靠传输层身份验证防止伪造或篡改。
- 在不可信网络中存在窃听、重放、伪造和拒绝服务风险。

需求：
- 增加可选 DTLS Listener。
- 支持证书/PSK 等经过评估的认证方式。
- 明确握手超时、重放保护、会话恢复和密钥轮换。
- UDP 明文模式必须显式配置开启，并在启动日志和管理 API 中标识为不安全模式。

优先级：P0（生产环境跨不可信网络部署）；受控隔离网络可以作为有风险的部署选项。

### MQTTSN-002：Forwarder Encapsulation / 串口桥接未实现

当前没有实现 MQTT-SN Forwarder Encapsulation 和串口桥接/转发器能力。

影响：
- 不能直接覆盖需要串口链路、非 IP 传感器网络或通过 Forwarder 汇聚到网关的部署场景。
- MQTT-SN UDP 直连能力不等于完整覆盖传感器网络网关生态。

需求：
- 评估 Forwarder Encapsulation 报文格式与转发器身份管理。
- 支持串口或 TCP 等转发器上行适配（具体链路按目标场景选择）。
- 建立转发器地址、网关 ID、客户端 ID 与会话的映射。
- 防止多个网关之间 TopicId / ClientId 冲突。

优先级：P2，属于场景扩展，不是所有 MQTT-SN 部署的必选项。

### MQTTSN-003：CONNECT 不包含凭据字段，当前不能启用统一 require_auth

当前文档说明 MQTT-SN CONNECT 没有用户名/密码字段；若 `require_auth` 与 `mqtt_sn` 一起配置，Broker 会拒绝启动。

这不是简单的“补一个密码字段”即可解决：必须避免私自扩展标准报文导致客户端不兼容。

建议方案：
- 在 DTLS 层或可信网关层建立设备身份。
- 使用预配置 ClientId 与证书/PSK/设备注册信息绑定。
- 若设计 MQTT-SN 扩展认证，必须通过独立扩展协商，不应改变标准 CONNECT 的解析规则。
- 建立 MQTT-SN ClientId 到统一 Principal 的可信映射。
- ACL 只使用已经验证的身份，不信任客户端自行声明的未认证元数据。

优先级：P0（只要 MQTT-SN 进入生产环境并要求认证授权）。

### MQTTSN-004：睡眠期间 QoS 0 消息丢弃

当前文档明确说明：QoS 0 在客户端睡眠期间丢弃。

这应首先被视为 QoS 0 不提供可靠投递保证的行为，而不能直接认定为实现错误。需求侧需要：
- 明确睡眠期间 QoS 1/2 的缓存及交付行为。
- 明确缓存上限、消息过期和设备唤醒后的发送顺序。
- 对 QoS 0 丢弃行为进行文档说明和指标统计。
- 对睡眠队列溢出、设备长时间不唤醒进行测试。

优先级：P1（主要补齐行为边界、可观测性与测试）。

### MQTTSN-005：真实客户端测试覆盖不足以证明所有高级语义

`mqtt-sn-tools` 0.0.7 当前测试工具本身不支持 QoS 2 和 Will，且其 PINGREQ 不携带 ClientId。

因此：
- 10/10 真实客户端互操作测试不能证明 QoS 2 和 Will 的外部互操作完整性。
- QoS 2、Will 等能力还需要 CT、字节级报文测试或另一款真实 MQTT-SN 客户端验证。
- PINGREQ 的特殊行为需要独立测试，不能只以工具限制作为服务端规范。

优先级：P1。

---

# 5. MQTT over QUIC 实现分析

## 5.1 实现定位

当前实现基于 MQTT over QUIC Single Stream Mode 的 OASIS TC 中国区 Draft 3，使用 QUIC over UDP，默认端口 14567。

它不是将任意 MQTT TCP 字节流简单放到 QUIC 上，而是按 Single Stream Mode 的约定，以一条客户端发起的双向 QUIC Stream 承载一条 MQTT 连接。

核心优势是传输层具备 TLS 1.3、减少 TCP 队头阻塞影响、连接迁移等 QUIC 能力；但这些能力的实际覆盖仍需通过测试确认。

## 5.2 已实现能力

- QUIC Listener 与 Transport。
- ALPN 固定为 `mqtt`。
- 强制 TLS 1.3。
- 单条客户端发起的双向 Stream 承载 MQTT 连接。
- 同一 `mqtt_conn_fsm` 处理 MQTT 消息。
- 继承 MQTT QoS 0/1/2、Retain、Will、持久会话和 MQTT 5.0 Properties 子集。
- Stream 关闭不等同于 QUIC Connection 关闭；同一连接内新 Stream 对应新会话。
- 无证书时默认拒绝启动 QUIC Listener。
- 只有 `rebar3 as quic` 构建配置才带 QUIC NIF。

### 验证证据
- 客户端：EMQX `emqtt` 1.16.1，`quicer` 0.4.12。
- ALPN：`mqtt`。
- 真实客户端集成：8/8。
- QUIC CT：协议兼容矩阵记录 17 例；启用 `quic` profile 后会展开更多真实 Listener 测试。
- 测试目录：`apps/broker_proto_mqtt_quic/test/`、`test/protocol-compatibility/mqtt_quic/`。

## 5.3 明确未实现或存在限制的功能

### QUIC-001：0-RTT Early Data 未实现

当前没有 0-RTT Early Data。

影响：新连接无法利用 0-RTT 提前发送允许的应用数据，连接建立时延可能高于具备该能力的实现。

注意：0-RTT 存在重放风险，尤其不应无条件允许非幂等 Publish。因此未实现 0-RTT 不属于 MQTT 基本互操作缺陷。

需求：
- 暂不将 0-RTT 作为默认开启能力。
- 先定义允许 Early Data 的 MQTT 操作白名单。
- 对 Publish 的重放、重复消息和 QoS 状态进行安全评估。
- 只有在实现 anti-replay / 幂等保障后，才考虑提供可选配置。

优先级：P2（可选性能扩展）。

### QUIC-002：多流（Multi-stream）未实现

当前仅实现 Single Stream Mode，没有 EMQX 私有多流扩展。

影响：不能利用多个并行 QUIC Stream 承载同一连接内多个独立 MQTT 子流或扩展会话。

说明：多流不是当前 Single Stream Mode 基线必须具备的功能，不应把它列为标准兼容阻塞项。

需求：
- 先保证 Single Stream Mode 规范兼容和稳定性。
- 如需多流，应单独设计扩展协议、Stream 与 MQTT Session 映射、流间隔离、关闭语义和公平调度。
- 必须通过扩展协商，不能与标准单流客户端产生歧义。

优先级：P2。

### QUIC-003：运行期 Stream Credit 不补充

当前 `max_streams_per_conn` 在建连时一次性给足，运行期间不做 Stream Credit 补充。

潜在影响：
- 长连接如果持续创建和关闭 Stream，Stream 配额耗尽后可能无法继续创建新 Stream。
- 当前“关闭 Stream 后在同一 QUIC Connection 上创建新 Stream 即新会话”的语义，需要与 Stream Credit 策略一起验证。

需求：
- 监听并处理 peer stream limit 变化。
- 明确本端是否需要增加可创建 Stream 的额度。
- 建立配额耗尽、Stream 关闭、反复建流和长连接测试。
- 增加连接当前 Stream 使用量、剩余额度和拒绝原因指标。

优先级：P1。

### QUIC-004：QUIC 连接迁移及网络变化场景需补充验证

QUIC 的一个重要价值是连接迁移，但当前公开兼容矩阵未将地址迁移、NAT 重绑定、网络切换等列为已验证能力。

不能据此直接断言底层 QUIC 库不支持连接迁移，但 UnifiedMQ 端到端行为仍缺少明确证据。

需求：
- 客户端 IP/端口变化后的连接连续性测试。
- NAT rebinding 测试。
- Wi-Fi 到蜂窝网络切换（客户端与测试环境允许时）。
- 路径验证失败和路径切换超时处理。
- Stream 仍在运行时 QUIC Connection 异常恢复。
- 连接迁移期间 MQTT Session 不被重复创建或意外清除。

优先级：P1（如果把 QUIC 用作移动 IoT 设备的主要接入方式）。

### QUIC-005：构建与部署条件需要产品化

当前 QUIC 依赖 NIF，只有 `rebar3 as quic` profile 才构建包含 quicer 的版本，默认不监听；没有证书时拒绝启动。

这属于明确的构建/部署约束，不是协议实现错误，但需要产品化：
- 提供 QUIC 专用构建指引和可复现构建。
- 提供依赖版本锁定及 ABI 检查。
- 启动时输出明确的 NIF 缺失、证书缺失和监听配置错误。
- 提供 QUIC 开启/关闭的部署配置模板。
- CI 至少保留一个带 quicer 的构建和测试作业。

优先级：P0（对外发行 QUIC 能力时）。

---

# 6. MQTT over WebSocket 实现分析

## 6.1 实现定位

MQTT over WebSocket 通过 RFC 6455 WebSocket 连接承载标准 MQTT 报文。它不是仓库中另一个自定义 WebSocket JSON 协议；后者属于独立的 `broker_proto_ws`，不应与 MQTT over WebSocket 混为一谈。

当前 `broker_proto_mqtt_ws` 使用 WebSocket Binary Frame 承载 MQTT，握手成功后交由共享的 `mqtt_conn_fsm` 处理 MQTT 语义。

## 6.2 已实现能力

### WebSocket 传输层
- RFC 6455 握手。
- 客户端掩码处理。
- 分片帧处理。
- 控制帧处理。
- Binary Frame 承载 MQTT Packet。
- 任意 URL Path。
- `mqtt`、`mqttv3.1`、`mqttv3.1.1` 子协议协商。
- 支持 WSS。

### MQTT 语义
由于复用 `mqtt_conn_fsm`，支持：
- MQTT 3.1.1 / 5.0 子集。
- QoS 0 / 1 / 2。
- Retain。
- Will。
- 持久会话。
- 离线消息追赶。

### 验证证据
- 真实客户端：Paho MQTT 2.1.0，`transport="websockets"`。
- 集成测试：8/8，另有 1 个 raw framing 探针单列。
- CT：19 例。
- 测试目录：`apps/broker_proto_mqtt_ws/test/`、`test/protocol-compatibility/mqtt_ws/`。

## 6.3 明确未实现或存在限制的功能

### MQTTWS-001：permessage-deflate 未实现

当前不支持 WebSocket `permessage-deflate` 扩展。

影响：不能在 WebSocket 握手阶段协商该压缩扩展，重复 Topic、属性及负载中的可压缩内容无法通过 WebSocket 层压缩。

说明：这是可选 WebSocket 扩展，不是 MQTT over WebSocket 的基本互操作必需项。

需求：
- 评估压缩收益、CPU 成本和内存开销。
- 若实现，正确处理 RSV 位、压缩上下文和消息边界。
- 防止压缩炸弹和解压后超限。
- 提供可配置开关和压缩阈值。

优先级：P2。

### MQTTWS-002：背压处理不完整

当前矩阵记录背压按帧处理，`arm_once` 为 no-op。

该行为需要重点审计：WebSocket 收帧、MQTT 解析、消息路由、客户端写队列之间是否形成真正的有界背压链路。

风险：
- 慢消费者可能造成进程邮箱或发送队列积压。
- 大量小帧可能放大调度和内存开销。
- 应用层队列已满时，网络层可能仍持续读取。

需求：
- 定义每连接最大接收帧队列和最大待发送字节数。
- 将 MQTT Receive Maximum / QoS 流控与 WebSocket 传输背压结合。
- 具备暂停读取、恢复读取或等价的有界缓冲策略。
- 明确 WebSocket Ping/Pong、Close 控制帧在过载时的处理优先级。
- 增加慢客户端、突发消息、持续大帧和断网写阻塞测试。
- 记录每连接积压帧数、积压字节、背压次数和丢弃/关闭原因。

优先级：P0（生产环境高并发使用时）。

### MQTTWS-003：文本帧与异常帧处理属于明确策略，需保持回归测试

当前策略：
- Text Frame：Close 1003。
- 超长帧：Close 1002。
- 缺失或未知 MQTT 子协议：HTTP 400。

这些不是待实现缺陷，而是当前明确的输入校验策略。需要保证升级后不发生错误放宽。

需求：
- 对二进制帧、文本帧、Continuation、控制帧、掩码、长度编码等进行字节级测试。
- 验证协议错误时 Close Code 与 HTTP 握手响应。
- 对超长帧与多片聚合后的总消息大小分别设限。

优先级：P1（协议健壮性）。

---

# 7. 四种实现之间的关键差异与影响

## 7.1 协议语义差异

| 维度 | MQTT TCP / WS / QUIC | MQTT-SN |
|---|---|---|
| Topic 表达 | UTF-8 Topic Name / Filter | TopicId（预定义、注册、短 Topic） |
| 连接模型 | TCP/传输连接承载 MQTT Session | UDP 报文与 ClientId/网关状态组合 |
| 会话 | Clean Start/Clean Session、Session Expiry | 睡眠/唤醒、网关保留客户端状态 |
| QoS | 0 / 1 / 2 | -1 / 0 / 1 / 2 |
| 遗嘱 | CONNECT 中 Will 属性 | Will Topic/Message 请求与应答流程 |
| 发现 | 固定 Broker 地址或外部服务发现 | SEARCHGW / ADVERTISE |
| 认证 | MQTT CONNECT 用户名密码及 MQTT 5 AUTH 等 | CONNECT 没有标准用户名/密码字段，需外部身份绑定 |
| 传输可靠性 | TCP 或 QUIC 提供可靠字节流 | UDP，协议需处理请求/响应和重传语义 |

## 7.2 传输层差异

| 维度 | TCP MQTT | MQTT-SN UDP | MQTT QUIC | MQTT WebSocket |
|---|---|---|---|---|
| 连接握手 | TCP + MQTT CONNECT | UDP 报文交互/网关发现 | QUIC + TLS 1.3 + ALPN + Stream | HTTP Upgrade + RFC 6455 + MQTT CONNECT |
| 有序交付 | TCP 保证 | UDP 不保证 | 单 Stream 内有序 | WebSocket 基于 TCP，有序 |
| 多流/多路 | MQTT 多连接/Channel 语义 | 无连接化报文模型 | 当前单流模式 | 单 WebSocket 通道内承载 MQTT |
| NAT/移动网络 | 连接变化通常需重连 | UDP 映射可能变化 | 有潜在连接迁移优势，需实测 | TCP 连接变化通常需重连 |
| TLS | 由 TCP Listener 配置决定 | 当前无 DTLS | 强制 TLS 1.3 | WSS |
| 浏览器支持 | 浏览器不能直接使用原生 TCP | 不适用 | 浏览器支持取决于 WebTransport/客户端栈，不应假定普通 Web 页面可直接用 | 浏览器原生 WebSocket 可用 |
| 典型场景 | 通用设备/服务端 SDK | 低功耗设备、传感器网关 | 移动设备、网络切换、现代传输 | 浏览器、Web 应用、受限网络 |

## 7.3 共享实现带来的工程影响

### 优势
- MQTT 核心语义只需实现一份，减少 TCP、WS、QUIC 间行为漂移。
- QoS、Session、Retain、Will 等核心功能可以共享测试。
- 新传输入口可以复用已验证的 MQTT 状态机。

### 风险
- `mqtt_conn_fsm` 的状态机缺陷会同时影响三种传输。
- 不同传输的连接断开、半关闭、超时、缓冲和背压语义并不完全相同。
- 将传输层事件简单映射成普通 TCP close 可能丢失 QUIC Stream 与 Connection 的区别。

### 必须建立的测试方式
同一组 MQTT 语义用例必须分别通过：
1. TCP MQTT。
2. MQTT over WebSocket。
3. MQTT over QUIC。

对 MQTT-SN 则使用协议独立测试，再针对可映射的语义做端到端对照。不得把 MQTT TCP 测试通过视为 WS / QUIC 自动通过。

---

# 8. 建议补齐的需求清单

优先级定义：
- **P0**：生产安全、消息语义正确性或运行稳定性所必需。
- **P1**：重要互操作能力和可靠性增强。
- **P2**：可选扩展或特定场景增强。
- **AUDIT**：目前证据不足，先做代码审计/测试，不直接判定为缺失。

## 8.1 MQTT 核心需求

| 编号 | 需求 | 当前状态 | 优先级 |
|---|---|---|---|
| MQTT-001 | MQTT 5 Topic Alias | 未实现 | P1 |
| MQTT-002 | MQTT 5 Enhanced Authentication | 未实现 | P1 / 特定企业场景 P0 |
| MQTT-003 | Subscription Identifier 投递透传 | 未实现 | P1 |
| MQTT-004 | QoS 2 跨崩溃状态持久化与恢复 | 当前跨崩溃仅至少一次 | P0 |
| MQTT-005 | MQTT 5 Properties 逐报文、逐属性兼容矩阵 | 需进一步细化 | AUDIT |
| MQTT-006 | MQTT 5 Maximum Packet Size / Receive Maximum 等协商 | 需补充证据 | AUDIT |
| MQTT-007 | Will Delay / Retain Handling / No Local 等组合语义 | 需补充证据 | AUDIT |
| MQTT-008 | Shared Subscription 跨节点分配与故障恢复 | 需补充证据 | AUDIT |
| MQTT-009 | MQTT 会话与 QoS inflight 崩溃恢复测试 | 需加强 | P0 |

## 8.2 MQTT-SN 需求

| 编号 | 需求 | 当前状态 | 优先级 |
|---|---|---|---|
| MQTTSN-001 | DTLS 安全传输 | 未实现 | P0（生产不可信网络） |
| MQTTSN-002 | Forwarder Encapsulation / 串口桥接 | 未实现 | P2 |
| MQTTSN-003 | ClientId 与证书/PSK/网关身份绑定 | 认证限制明确存在 | P0 |
| MQTTSN-004 | 睡眠客户端 QoS 1/2 缓存、过期和唤醒交付矩阵 | 需补充边界测试 | P1 |
| MQTTSN-005 | QoS 2 / Will 的真实第三方客户端互操作测试 | 当前测试工具不支持 | P1 |
| MQTTSN-006 | UDP 丢包、乱序、重复、源地址变化和网关故障测试 | 需补充系统测试 | P1 |
| MQTTSN-007 | TopicId 注册表容量、冲突和重启恢复 | 需补充证据 | AUDIT |
| MQTTSN-008 | UDP 放大攻击、速率限制和资源配额 | 需审计 | P0 |

## 8.3 MQTT over QUIC 需求

| 编号 | 需求 | 当前状态 | 优先级 |
|---|---|---|---|
| QUIC-001 | 0-RTT Early Data | 未实现；可选扩展 | P2 |
| QUIC-002 | Multi-stream 扩展 | 未实现；非当前单流基线必需 | P2 |
| QUIC-003 | 运行期 Stream Credit 补充 | 未实现/有限制 | P1 |
| QUIC-004 | QUIC Connection Migration / NAT Rebinding 测试 | 尚无明确验证证据 | P1 |
| QUIC-005 | Stream 与 MQTT Session 生命周期一致性 | 需要专项验收 | P0 |
| QUIC-006 | QUIC Flow Control / Idle Timeout / Keepalive 测试 | 需要专项验收 | P1 |
| QUIC-007 | NIF、quicer 版本和部署构建产品化 | 有构建条件 | P0（发行 QUIC 版本） |
| QUIC-008 | QUIC 多节点压测与节点故障恢复 | 需要专项验收 | P1 |

## 8.4 MQTT over WebSocket 需求

| 编号 | 需求 | 当前状态 | 优先级 |
|---|---|---|---|
| MQTTWS-001 | permessage-deflate | 未实现；可选扩展 | P2 |
| MQTTWS-002 | 网络读取和发送路径真正有界背压 | 当前 `arm_once` no-op，需完善 | P0 |
| MQTTWS-003 | 慢消费者、写阻塞、突发帧和内存上限测试 | 需要加强 | P0 |
| MQTTWS-004 | 分片聚合大小与单帧大小分别限额 | 需核验 | AUDIT |
| MQTTWS-005 | Origin 校验、跨域和反向代理部署策略 | 需核验 | P1 |
| MQTTWS-006 | WSS 证书轮换及 TLS 策略 | 需结合统一安全配置核验 | AUDIT |

---

# 9. 测试与验收体系

## 9.1 当前已有证据基线

按仓库协议兼容矩阵（2026-11-11）：

| 协议 | 真实客户端 | 集成测试 | CT | 证据目录 |
|---|---|---:|---:|---|
| MQTT | Paho / Mosquitto | 10/10 | 多套件 | `apps/broker_proto_mqtt/test/`、`test/protocol-compatibility/mqtt/` |
| MQTT over WebSocket | Paho 2.1.0 | 8/8，另有 raw framing 探针 | 19 | `apps/broker_proto_mqtt_ws/test/`、`test/protocol-compatibility/mqtt_ws/` |
| MQTT-SN | mqtt-sn-tools 0.0.7 | 10/10 | 84 | `apps/broker_proto_mqtt_sn/test/`、`test/protocol-compatibility/mqtt_sn/` |
| MQTT over QUIC | emqtt 1.16.1 + quicer 0.4.12 | 8/8 | 文档记载 17；quic profile 会额外展开 listener 测试 | `apps/broker_proto_mqtt_quic/test/`、`test/protocol-compatibility/mqtt_quic/` |

仓库记录全量测试基线为 823/823、52 个套件。启用 QUIC profile 后会额外展开真实 QUIC Listener 用例，文档记录该 profile 汇总为 827 passed + 2 skipped。这里要注意默认 profile 与 quic profile 的计数口径不同，不应把两套数字直接相加。

## 9.2 必须新增的跨传输一致性测试

同一 MQTT 行为应对 TCP / WS / QUIC 重复执行：

- CONNECT / CONNACK 属性协商。
- QoS 0/1/2 发布和订阅。
- QoS 1/2 重传及 Packet Identifier 管理。
- Retain 与 Retain Handling。
- Will 与 Will Delay（若支持）。
- Clean Start / Session Expiry / 离线消息。
- Shared Subscription。
- MQTT 5 Properties。
- 最大报文长度与 Receive Maximum。
- 客户端异常断开、服务端重启、网络中断。

要求使用参数化测试或统一外部测试脚本，避免三套传输测试逻辑长期分叉。

## 9.3 MQTT-SN 专项测试

- 全部 27 种报文编码与解码。
- QoS -1 / 0 / 1 / 2。
- TopicId 注册、释放、重注册和冲突。
- Pre-defined / Normal / Short Topic。
- Will 协商和 Will 更新。
- SEARCHGW / ADVERTISE。
- 睡眠、唤醒、长睡眠、睡眠期间消息到达。
- UDP 丢包、乱序、重复和报文截断。
- 网关重启、客户端重启和状态恢复。
- 未认证 ClientId 伪造及 UDP 资源消耗攻击。

## 9.4 QUIC 专项测试

- TLS 1.3 与 ALPN 错误协商。
- 无证书、过期证书、错误 CA。
- Stream 正常关闭与异常关闭。
- QUIC Connection 保持、Stream 重建。
- `max_streams_per_conn` 耗尽与恢复。
- 连接迁移、NAT Rebinding。
- Flow Control、Idle Timeout、Keepalive。
- 客户端断网、服务端重启、NIF 崩溃保护。
- 大量 QUIC 连接和多节点压测。

## 9.5 WebSocket 专项测试

- Upgrade 握手、子协议协商、任意 URL Path。
- Binary / Text Frame。
- Masking、Continuation、Fragmentation。
- Ping / Pong / Close。
- 超长帧、超长聚合消息、畸形帧。
- WSS 握手与证书错误。
- 慢消费者和客户端读取阻塞。
- 大量连接与发送队列上限。
- Web 反向代理、Origin 和跨域策略。

---

# 10. 推荐实施顺序

## 阶段 A：语义正确性与生产安全（P0）

1. 明确 MQTT QoS 2 跨崩溃语义，若要承诺跨故障可靠则持久化 inflight 状态。
2. 完善 MQTT over WebSocket 背压，确保每连接内存和发送队列有上限。
3. MQTT-SN 引入可部署的身份绑定方案；生产不可信网络提供 DTLS。
4. QUIC 构建、NIF、证书和 Listener 的发行流程产品化。
5. 增加 MQTT / WS / QUIC 同语义回归测试。
6. MQTT-SN 增加 UDP 异常、重复、乱序和资源限制测试。

## 阶段 B：协议互操作补齐（P1）

1. MQTT 5 Topic Alias。
2. MQTT 5 Subscription Identifier 投递透传。
3. MQTT 5 Enhanced Authentication。
4. QUIC Stream Credit 运行期管理。
5. QUIC 连接迁移与 NAT Rebinding 测试。
6. MQTT-SN QoS 2 / Will 的独立外部互操作验证。
7. MQTT-SN 睡眠期间 QoS 1/2 消息生命周期完善。
8. 四种协议统一的身份、会话与审计行为核验。

## 阶段 C：可选能力和场景扩展（P2）

1. MQTT over QUIC Multi-stream 扩展（先定义扩展协议和协商）。
2. QUIC 0-RTT（先解决重放与幂等问题）。
3. MQTT over WebSocket `permessage-deflate`。
4. MQTT-SN Forwarder Encapsulation / 串口桥接。
5. 针对极端低功耗、弱网和高丢包传感网络优化 MQTT-SN。

## 阶段 D：补充审计（AUDIT）

1. MQTT 5.0 全部 Properties 的支持/忽略/拒绝矩阵。
2. MQTT 5.0 各类 Reason Code 的准确性。
3. TCP TLS、WSS 与 QUIC 的统一证书和安全配置管理。
4. Shared Subscription 的集群分配和恢复。
5. Session、Retain、Will、QoS inflight 的持久化边界。
6. WebSocket 帧聚合限制和真正的读取侧背压。
7. MQTT-SN TopicId 注册表持久化与网关重启行为。

---

# 11. 设计约束与避免的误区

1. **不要将 MQTT-SN 当作 MQTT UDP 版。** 它有自己的报文、TopicId、网关和睡眠客户端模型。
2. **不要为 TCP、WS、QUIC 维护三套 MQTT QoS 状态机。** 应共享 MQTT 引擎，但传输关闭、重连、流控事件必须由 Adapter 正确映射。
3. **不能因为 QUIC 支持多 Stream 就擅自认为 MQTT 多流已经实现。** 当前是 Single Stream Mode；多流属于未来扩展。
4. **不能把不支持 0-RTT 或 WebSocket 压缩认定为基本协议不合规。** 这是可选传输能力。
5. **不能把 QoS 2 在线握手成功等同于跨崩溃 Exactly Once。** 当前仓库文档明确指出跨崩溃为至少一次。
6. **不能把 mqtt-sn-tools 的客户端能力限制归咎于服务端。** 需要用 CT、字节级报文或其他客户端补足验证。
7. **不能让 MQTT-SN 绕过身份与 ACL。** 即使其 CONNECT 没有标准凭据字段，也必须建立可信的身份来源和授权主体。
8. **不能把 TCP / QUIC / WebSocket 的传输背压混成同一个无差别回调。** 不同传输有不同的流控接口和资源边界。
9. **跨协议映射必须声明损失。** MQTT-SN TopicId、MQTT 5 Properties、QUIC Stream 生命周期等信息不能在适配时静默丢弃。

---

# 12. 最终判断

## 12.1 当前成熟度判断

| 实现 | 当前完成度判断 | 主要剩余工作 |
|---|---|---|
| MQTT TCP | 四者中最成熟，是 MQTT 语义基准 | MQTT 5.0 补齐、QoS 2 跨崩溃可靠性、完整属性审计 |
| MQTT-SN | 基础协议覆盖较广，独立网关功能已经具备 | DTLS、可信认证、Forwarder/串口场景、弱网与睡眠设备验证 |
| MQTT over QUIC | 基础单流承载和客户端互操作已完成 | Stream Credit、连接迁移/流控验证、构建部署产品化；0-RTT/多流为可选扩展 |
| MQTT over WebSocket | 标准 MQTT 语义复用较完整，客户端互操作已通过 | 背压和慢消费者保护优先；压缩为可选扩展 |

## 12.2 研发优先级结论

如果 UnifiedMQ 面向工业 IoT、设备接入和企业消息平台，推荐顺序为：

1. **先保障共享 MQTT Engine 的正确性**：重点处理 QoS 2 状态恢复、MQTT 5.0 语义和会话生命周期。
2. **优先补齐 WebSocket 背压**：浏览器接入容易产生大量连接，且慢客户端可能造成资源堆积。
3. **补齐 MQTT-SN 安全身份与 DTLS**：否则 MQTT-SN 目前更适合受控网络或可信网关后的部署。
4. **完善 QUIC 的 Stream 生命周期和流控**：基础互操作已具备，但要把网络迁移、长连接和资源额度验证清楚。
5. **最后做可选扩展**：Multi-stream、0-RTT、permessage-deflate、Forwarder/串口桥接按产品场景决定。

**最终结论：四种协议的基础接入都已经有实际实现和测试证据；目前最明显的问题不是“某个协议完全没做”，而是 MQTT 核心语义还有明确缺项，MQTT-SN 的安全和网关生态不完整，QUIC 的高级连接/流控能力有限，WebSocket 的背压尚需完善。**

---

# 13. 参考资料

以下为本次分析所依据的仓库文档和代码目录：

1. UnifiedMQ 仓库：<https://gitee.com/wanwuliandong/unified-mq>
2. 协议兼容矩阵：<https://gitee.com/wanwuliandong/unified-mq/blob/main/docs/protocol-compatibility.md>
3. MQTT 特性对比：<https://gitee.com/wanwuliandong/unified-mq/blob/main/docs/mqtt-feature-comparison.md>
4. 协议符合性测试验证方案：<https://gitee.com/wanwuliandong/unified-mq/blob/main/docs/protocol-conformance-validation.md>
5. 开发路线图：<https://gitee.com/wanwuliandong/unified-mq/blob/main/docs/roadmap.md>
6. MQTT 主协议模块：<https://gitee.com/wanwuliandong/unified-mq/tree/main/apps/broker_proto_mqtt>
7. MQTT-SN 模块：<https://gitee.com/wanwuliandong/unified-mq/tree/main/apps/broker_proto_mqtt_sn>
8. MQTT over QUIC 模块：<https://gitee.com/wanwuliandong/unified-mq/tree/main/apps/broker_proto_mqtt_quic>
9. MQTT over WebSocket 模块：<https://gitee.com/wanwuliandong/unified-mq/tree/main/apps/broker_proto_mqtt_ws>

标准参考：
- OASIS MQTT Version 3.1.1：<https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/>
- OASIS MQTT Version 5.0：<https://docs.oasis-open.org/mqtt/mqtt/v5.0/>
- MQTT-SN 1.2：<https://mqtt.org/mqtt-specification/>
- RFC 6455 — The WebSocket Protocol：<https://www.rfc-editor.org/rfc/rfc6455>
- QUIC Transport RFC 9000：<https://www.rfc-editor.org/rfc/rfc9000>
- QUIC TLS RFC 9001：<https://www.rfc-editor.org/rfc/rfc9001>

