# ADR-023：MQTT over WebSocket 的两进程分层

- **状态**：已采纳（S8-B 冻结，2026-11-10）
- **日期**：2026-11-10
- **关联**：MQTT 3.1.1 §6 / MQTT 5.0 §6（MQTT over WebSocket）、RFC 6455；ADR-001（协议适配与内核分离）、ADR-022（MQTT-SN 单 socket，反面参照）、ADR-024（QUIC 流分层）
- **关联文档**：[`docs/design/s8-b-mqtt-over-ws-execution-plan.md`](../design/s8-b-mqtt-over-ws-execution-plan.md)、[`docs/architecture.md`](../architecture.md) §3.10

## 背景（Context）

S8-A 把 `mqtt_transport` 抽成四个回调：

```erlang
send(Handle, IoData)      -> ok | {error, term()}
arm_once(Handle)          -> ok | {error, term()}
close(Handle)             -> ok
decode_msg(Msg, Handle)   -> {data, Bin} | closed | {error, term()} | other
```

`decode_msg/2` 必须是**纯函数**：`(Msg, Handle) -> decision`。这不是风格偏好——FSM 会在
每个事件上调用它，把邮箱消息归一化；任何「读了才知道」的状态都无处安放。

WebSocket 恰好需要一个这样的状态：一个 WS 帧可能只到了半截（长度头读完、payload 还没到），
而 RFC 6455 的升级握手是否完成也是每连接状态。把这两样塞进 `decode_msg/2` 只有两个选择：

1. 进程字典 / ETS：用隐藏状态换掉一个干净的行为契约；
2. 让 FSM 自己攒 buffer：把帧边界知识塞进协议层，传输抽象就没有意义了。

两条都不接受。

同时还有第二个问题：`broker_proto_ws`（S5）已经是一个 WebSocket 前端，跑的是 Broker 自有
JSON/binary 消息 API。把它扩展成「也支持 MQTT over WebSocket」意味着 8080 端口上按子协议
分流，而这条路径的线上字节行为在 S5 已经钉死（有 `ws_conn_SUITE` 23 例和 10/10 真实客户端
证据）。合并会让两边的需求互相牵制。

## 决策（Decision）

### D1. 两个 app、两个端口

- `broker_ws`：**纯 RFC 6455 库**（`ws_frame` 帧编解码 + `ws_handshake` 握手），零进程、
  零配置，`kernel`/`stdlib`/`crypto` 之外无依赖。由 `broker_proto_ws` 与
  `broker_proto_mqtt_ws` 共用。
- `broker_proto_ws`：8080，Broker 自有消息 API（S5 不变）。
- `broker_proto_mqtt_ws`：8083，MQTT over WebSocket（MQTT 3.1.1 §6 / 5.0 §6）。

不合并的理由是应用协议不同，不是端口不够用。

### D2. 每连接两个进程，分工就是设计本身

```
mqtt_ws_conn  (ranch 协议进程)
  - 拥有 socket
  - RFC 6455 升级 + 子协议协商（ws_handshake:accept/2）
  - 帧编解码（ws_frame），维护「半个帧」与「升级是否完成」
  - 只把 binary 帧的 payload 原样转给 MQTT 引擎
        │  {mqtt_ws, Data} / {mqtt_ws, closed}
        ▼
mqtt_conn_fsm  (broker_proto_mqtt 的 mqtt_conn_sup 下)
  - 协议语义、QoS、会话、retain、Will
  - transport => mqtt_transport_ws
  - **handle = mqtt_ws_conn 的 pid**，不是 socket
```

FSM 仍然归 `broker_proto_mqtt` 的 `mqtt_conn_sup` 管（S8-A 特意让监听与引擎分离），
WS app 只负责「把字节变成 MQTT 报文」。这样 WS 进程死了，FSM 拿到 `closed` 并走正常
断开流程（发布 Will、清理会话），而不是跟着一起崩。

### D3. 帧解码不进 `decode_msg/2`

`mqtt_transport_ws:decode_msg/2` 只做一件事：把 WS 进程的消息归一化。

- `{mqtt_ws, Bin}` → `{data, Bin}`
- `{mqtt_ws, closed}` → `closed`
- 别的 → `other`

帧边界、分片、控制帧、掩码全部在 WS 进程里处理完。理由见「背景」。

### D4. 线缆规则（不满足就拒绝，不猜测）

| 规则 | 行为 | 依据 |
|---|---|---|
| 子协议必须是 `mqtt` / `mqttv3.1` / `mqttv3.1.1` 之一 | 缺失或不匹配 → **HTTP 400**，不升级 | MQTT 3.1.1 §6 要求子协议 |
| 只接受 **binary** 帧 | text 帧 → close **1003**（unsupported data） | 不把 text payload 当 MQTT 字节 |
| 报文与帧**无对齐要求** | 一个报文可跨多帧（交 FSM 的 `buf` 重组）；一个帧可含多报文（逐条处理） | RFC 6455 只保证帧内完整，不对齐应用消息 |
| 控制帧 | WS ping → pong；close → 回 close 后断开 | 与 MQTT keepalive 相互独立 |
| 帧大小上限 | `max_frame_bytes`（默认 16 MiB）超限 → close **1002** | `ws_frame` 对每个 decode 错误统一 1002；报文级上限仍是 `mqtt_max_packet_size` |
| URL path | 任意 | 协议规定的是子协议，不是路径 |

### D5. 背压刻意缺省

`mqtt_transport_ws:arm_once/1` 是**语义占位 no-op**：WS 层每帧后自行 re-arm，没有逐帧
背压。`send/2` 异步。

- 理由：`gen_tcp:send` 本身是同步的，WS 进程在发送期间阻塞；加一层 `arm_once` 只会
  在不改变实际吞吐的前提下增加状态。
- 真正的窗口控制仍是 `max_inflight` / `receive_maximum`（MQTT 层），它们是唯一承诺。
- 这是**与 TCP 传输的实质差异**，写在这里而不是埋在代码里。

### D6. 对端故障以 `normal` 退出，不作为崩溃报告

RST、TLS 握手不完成、re-arm 失败——这些是对端造成的，不是本进程的 bug。WS 进程以
`normal` 停止并把 `{mqtt_ws_error, self(), Reason}` 交给 FSM 记一条日志。

否则每条坏 TCP 连接都会产生一份 OTP 崩溃报告，把真正的缺陷淹掉。

## 后果（Consequences）

- **+** `mqtt_transport` 契约保持纯函数，FSM 不需要知道任何帧边界知识。
- **+** 两个 WS 前端共用 `broker_ws`，RFC 6455 实现只有一份；`ws_frame_SUITE` 9 例覆盖两者。
- **+** 顺带修好既有 `broker_proto_ws` 的 wss：`ws_conn` 的 `{handshake, ...}` 分支从没调过
  `Transport:handshake/2`，`ssl:setopts` 报 `notsup_on_transport_accept_socket`，wss 一直
  不可用。补上后 `ws_conn_SUITE` 23 例（新增 1 例回归）。
- **-** 每连接两个进程（TCP 是一连接一 FSM）。代价是 mailbox 与调度开销；换来的是 WS
  进程可以独立崩溃/重启而不毁掉会话。
- **-** `arm_once` 在 WS 上无实际作用，这是契约的语义稀释。文档写明，不做假背压。
- **-** `permessage-deflate` 不实现（设计 §2 排除）：压缩会引入 per-connection 字典状态，
  与「FSM 不持有帧状态」的决定冲突。

## 验证（Validation）

- `mqtt_ws_conn_SUITE` 19 例：子协议协商 + 两条 400 拒绝、CONNECT、QoS1/QoS2 双向、
  retained 后到订阅、跨帧重组、单帧多报文、WS ping/pong、text → 1003、client close、
  超长帧、任意 path、Will、wss、明文 TCP 打 wss 端口。
- 真实第三方客户端：paho-mqtt **2.1.0** `transport="websockets"` **8/8**
  （`test/protocol-compatibility/mqtt_ws/`），另 1 例 raw socket framing 探针单列
  （`third_party: false`，测的是本仓库自己发的字节）。
- 踩坑记录（都写进套件注释）：
  - 客户端读帧必须按字节精确读——`recv(S, 0)` + decode 丢余量会在服务端把
    CONNACK+SUBACK 写进同一 TCP 段时丢掉第二帧，单跑必过、全量必挂；
  - `PUBREC`/`PUBCOMP` 没有回包（订阅端发 `PUBREC` 被回答的是 `PUBREL`）；
  - paho 2.1.0 的 `wait_for_publish/1` 成功返回 `None`，判据必须用 `is_published()`。
