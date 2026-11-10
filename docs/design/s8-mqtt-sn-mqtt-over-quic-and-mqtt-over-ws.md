# S8 方案设计 — MQTT-SN、MQTT over QUIC、MQTT over WebSocket

- **文档版本**：v0.1（设计评审稿，2026-11-10）
- **对应 Roadmap**：[`docs/roadmap.md`](../roadmap.md) §10（S8 章，含阶段表与 Gate S8）
- **前置**：S0–S7 全部结案（`progress.md`）
- **状态**：**已落地（2026-11-11，S8-A/B/C/D 全部结案）**。本文件保留为当时的设计稿，
  落地期间的偏差只在下面以「落地后修正」批注标出，原文不改。**当前事实以
  [`docs/roadmap.md`](../roadmap.md) §10、[`docs/protocol-compatibility.md`](../protocol-compatibility.md)
  与各子阶段执行计划为准**；本文件中未标注"实测"的数字是当时的占位口径，不作为承诺。

---

## 1. 目标与非目标

### 1.1 新增的三项需求

1. **MQTT-SN 1.2**：UDP 上的传感器网络 MQTT 变体，本 Broker 充当聚合型网关。
2. **MQTT over QUIC**：按 OASIS MQTT TC《MQTT over QUIC — Single Stream Mode》Committee Note（Draft 3）实现，ALPN `mqtt`。
3. **MQTT over WebSocket**：按 MQTT 3.1.1 §6 / MQTT 5.0 §6，`Sec-WebSocket-Protocol: mqtt`，binary 帧承载 MQTT 报文。

三者的共同目标：**复用现有 MQTT 引擎（`mqtt_conn_fsm`）与会话内核，只替换传输/报文层**，不产生第二套 MQTT 语义。

### 1.2 非目标（明确不做，避免被读成"已完成"）

| 项 | 说明 |
|---|---|
| MQTT-SN over DTLS | OTP `ssl` 支持 dtls，但不在本轮 Gate；列入后续待办 |
| MQTT-SN 串口桥 / 下游 QTT 转发 | 只做网关终结，不做 bridge 到外部 MQTT Broker |
| QUIC 多流模式 | EMQX 私有扩展，非 OASIS CN 内容 |
| QUIC 0-RTT early data | CN 中为可选；本轮不接收 early data |
| QUIC 连接迁移压测、跨 QUIC 重连的会话状态保持 | EMQX 自身明确未支持后者 |
| WebSocket `permessage-deflate` | 不协商压缩扩展 |
| MQTT `require_auth` 接线 | 仍留在 `docs/security.md` §12 待办，本阶段不扩面（见 §9） |

---

## 2. 已实测的事实（不是推测）

| 事实 | 证据 |
|---|---|
| `quicer 0.4.12` 可在本机编译 | hex 包 + 源码编译 msquic v2.5.11（quictls）；本机 deepin 25 无预编译包，冷编译约 15 分钟 |
| NIF 不依赖外部 `libmsquic.so` | `ldd libquicer_nif.so` 仅 `libnuma`、`libc`、`ld-linux` |
| QUIC 服务端必须显式握手 | `quicer:listen` → `accept` → **`quicer:handshake/2`** → `accept_stream`；缺 handshake 时 TLS 不完成、客户端 `connect` 永挂起 |
| 数据与事件同邮箱 | `{quic, Bin, Stream, _}` 是数据；`{quic, EventAtom, Stream, _}` 是事件（如 `send_shutdown_complete`），必须按 `is_binary/1` 分派 |
| 单流 ALPN `mqtt` 端到端可用 | 自建 listener + 客户端，ALPN `mqtt`、一条 bidi 流双向收发 `ping`/`hello` 成功 |
| 真实 QUIC 客户端可对齐 | `emqtt`（EMQX 官方 Erlang 客户端）`src/emqtt_quic.erl`：`{alpn, ["mqtt"]}`、`quicer:start_stream` 单流、`active=1` |
| `mqtt-sn-tools` 可用 | 0.0.7，`make` 直接构建 `mqtt-sn-pub`/`mqtt-sn-sub`；支持 QoS 0/1/-1、预定义 topic id、短 topic、`-e` 休眠；**不支持 QoS 2** |
| `ws_frame` 可抽库 | 纯 RFC 6455 编解码，仅导出 `decode/2`、`encode/2,3`、`close_payload/2`、`close_code/1`，无 broker 依赖 |
| MQTT over WebSocket 客户端现成 | 本机 `paho-mqtt 2.1.0` 支持 `transport="websockets"` + `ws_set_options(path=...)` |
| 现有 MQTT/TLS 路径是坏的 | `mqtt_conn_fsm` 只用 `gen_tcp:send/2` 与 `inet:setopts/2`，从不引用 ranch 传入的 `Transport`；`broker_security_tls_SUITE` 只验证 ssl listener 可建，未验证 MQTT 帧。S8 的传输抽象顺带修复（见 §4） |

---

## 3. 应用布局与依赖

```
apps/broker_ws/                新增：纯 RFC 6455 库（ws_frame + ws_handshake）
apps/broker_proto_ws/          既有：改为引用 broker_ws，行为与对端字节不变
apps/broker_proto_mqtt/        既有：新增 mqtt_transport 行为 + tcp/ws 无关的 FSM 改造
apps/broker_proto_mqtt_sn/     新增：MQTT-SN 1.2 网关（gen_udp）
apps/broker_proto_mqtt_quic/   新增：quicer 监听 + 单流模式（profile 门控依赖）
apps/broker_proto_mqtt_ws/     新增：WebSocket 监听 + `mqtt` 子协议
```

依赖方向（只允许单向，箭头指向被依赖方）：

```
broker_proto_ws        →  broker_ws
broker_proto_mqtt_ws   →  broker_ws, broker_proto_mqtt
broker_proto_mqtt_quic →  broker_proto_mqtt, quicer(profile 门控)
broker_proto_mqtt_sn   →  broker_subscription, broker_queue,
                          broker_router, broker_retain
```

`broker_proto_mqtt` 不被任何新 app 反向依赖，也不依赖 `broker_ws`/`quicer`：它只定义传输行为与 MQTT 引擎。

理由：`mqtt_conn_fsm` 是唯一的 MQTT 状态机；新 app 只是"一个传输/一个依赖"的薄适配层，与既有 `broker_proto_*` 一协议一 app 的边界不冲突。

---

## 4. 传输抽象：`mqtt_transport`

### 4.1 行为

在 `broker_proto_mqtt` 中新增行为模块 `mqtt_transport`：

| 回调 | 语义 |
|---|---|
| `send(SockOrStream, iodata())` | 发送已编码字节 |
| `arm_once(SockOrStream)` | 进入 active-once 收包模式 |
| `close(SockOrStream)` | 关闭传输 |
| `decode_msg(Msg, SockOrStream)` | 把邮箱消息归一化为 `{data, Bin}` / closed / `other` |

实现（**模块放在各自 app 内，`broker_proto_mqtt` 不反向依赖它们**）：

- `mqtt_transport_tcp`（在 `broker_proto_mqtt`）：`gen_tcp:send`、`inet:setopts(active,once)`、`gen_tcp:close`；识别 `{tcp,S,D}`、`{tcp_closed,S}`、`{ssl,S,D}`、`{ssl_closed,S}`。**同时覆盖 ranch_tcp 与 ranch_ssl**，因此顺带修复 MQTT over TLS。
- `mqtt_transport_ws`（在 `broker_proto_mqtt_ws`）：发送走 WS binary 帧，收包把 WS 帧 payload 当作字节流喂给 FSM。
- `mqtt_transport_quic`（在 `broker_proto_mqtt_quic`）：`quicer:send/2`、`quicer:setopt(Stream, active, once)`、`quicer:close_stream/2`；识别 `{quic, Bin, Stream, _}`（数据）与 `{quic, Event, Stream, _}`（事件）。

FSM 只持有传输模块名与 handle，通过行为回调调用，编译期不需要任何新 app 存在。

### 4.2 FSM 改造约束

- `#st{}` 增加 `transport` 字段，默认 `mqtt_transport_tcp`；既有测试助手（直接传 socket + `connect` 事件）**无需改动**。
- 只改 5 个调用点：`send_frame/2`、`arm_socket/2`、`active_once/1`、`terminate/3` 的 close、`{tcp,...}`/`{tcp_closed,...}` 事件入口。
- 既有 673 例必须零回归；新增 `mqtt_transport_tcp_SUITE` 覆盖两档传输的消息归一化。

> **落地后修正（2026-11-11）**：实际只抽了 TCP/TLS 一档实现（`mqtt_transport_tcp`，同时覆盖
> `ranch_tcp` 与 `ranch_ssl`），WS 与 QUIC 的实现分别住在各自的 app 里，因此套件名是
> `mqtt_transport_tcp_SUITE`（10 例）而不是「三实现共用一份」。FSM 的 5 个调用点改动与这里
> 描述一致。

---

## 5. MQTT-SN 1.2（`broker_proto_mqtt_sn`）

### 5.1 模块

| 模块 | 职责 |
|---|---|
| `mqtt_sn_listener` | `gen_udp` 收包、按 `{IP, Port}` 找/建会话、把 `client_id` 与端点绑定 |
| `mqtt_sn_codec` | 全报文集编解码（含 1 字节/3 字节长度头、非法长度拒绝） |
| `mqtt_sn_conn` | 每客户端会话进程（连接态、QoS 状态、keepalive、休眠） |
| `mqtt_sn_topics` | 预定义 topic id 表 + 每客户端动态分配 + 短 topic 处理 |
| `mqtt_sn_sup` | 监督树（listener + 会话） |

**落地后修正（2026-11-11）**：实际模块与上表有两处差异。

- **监督树是两层**：`broker_proto_mqtt_sn_sup`（app 级）下挂 `mqtt_sn_listener` 与
  `mqtt_sn_conn_sup`（会话 supervisor，`simple_one_for_one` 形态）；没有叫 `mqtt_sn_sup`
  的模块。会话是短命的，和 listener 分开监管才能让会话崩溃不拖垮整个网关的 socket。
- **多一个 `mqtt_sn_filter`**：`validate_filter/1` + `matches/2` + `validate_topic/1`，
  是 `mqtt_filter` 的**刻意重写**（20 行，只留网关要用的三个函数），因为
  `broker_proto_mqtt_sn` 不允许依赖 `broker_proto_mqtt`（ADR-001 的 app 边界）。

### 5.2 报文集

CONNECT/CONNACK、WILLTOPICREQ/WILLTOPIC、WILLMSGREQ/WILLMSG、WILLTOPICUPD/WILLTOPICRESP、WILLMSGUPD/WILLMSGRESP、REGISTER/REGACK、PUBLISH/PUBACK/PUBREC/PUBREL/PUBCOMP、SUBSCRIBE/SUBACK、UNSUBSCRIBE/UNSUBACK、PINGREQ/PINGRESP、DISCONNECT、ADVERTISE/SEARCHGW/GWINFO。

- QoS -1：只下行/上行一次，不进会话状态，不产生 ACK。
- QoS 0/1/2：与 MQTT 对齐，QoS 2 走 PUBREC/PUBREL/PUBCOMP。
- Topic id 规则：`0x0000` 非法；`0xFFFE`/`0xFFFF` 保留；normal/predefined/short 三种 TopicIdType。

### 5.3 会话与休眠

- 会话键 = `client_id`，直接复用 `broker_subscription`/`broker_queue`/`broker_router`/`broker_retain`，与 MQTT/WS 同一会话空间（同 `client_id` 重复 CONNECT 触发 takeover）。
- **休眠不新建缓冲**：`DISCONNECT(duration)` 后会话保留订阅与游标但不 claim；QoS1/2 消息留在分区内，`PINGREQ` 唤醒后继续拉取。QoS0 在休眠期间丢弃（与 QoS0 语义一致，写入能力矩阵）。
- keepalive：1.5×duration 无流量 → 网关发 PINGREQ；2× 未响应 → 判死并 detach。
- 休眠登记有上限 `max_asleep_clients`，超限拒绝进入休眠（CONNACK 失败码），不静默丢弃。

### 5.4 认证边界（必须显式）

MQTT-SN 1.2 的 CONNECT **没有用户名/密码字段**，协议上无法承载凭据。因此：

- `broker_security.require_auth` 中出现 `<<"mqtt_sn">>` 时，`broker_proto_mqtt_sn` **拒绝启动自己的监听器**并给出 `{require_auth_unsupported, mqtt_sn}`，不静默忽略（与 MQTT 现有的"配了但无效"形成对照，见 `docs/security.md` §2.2）。
- SN 连接一律按 `broker_security:default_tenant/0` 匿名运行；ACL 仍按该 principal 生效。

### 5.5 配置与端口

```erlang
{broker_proto_mqtt_sn, [
  {listeners, [#{port => 1884, num_workers => 4, max_clients => 1024}]},
  {predefined_topics, #{1 => <<"t0/sensor/temp">>}},
  {gateway_id, 1},
  {advertise_interval_ms, 0},      %% 0 = 只应答 SEARCHGW，不主动广播
  {max_asleep_clients, 1024},
  {max_topic_ids_per_client, 1024}
]}
```

UDP `1884`（MQTT TCP 是 1883）；CT 用临时端口。

> **落地后修正（2026-11-11）**：上表与实现有两处差异。
>
> 1. `listeners` 里**只有 `port`**：`num_workers` / `max_clients` 没有实现，也不该有——收包是
>    **一个 UDP socket + 一个 reader 进程**（`gen_udp` + `{active, 100}`），没有「监听 worker
>    池」可配；每个会话倒确实是一个进程（`mqtt_sn_conn` 挂在 `mqtt_sn_conn_sup` 下），但它由
>    端点按需拉起，不归 listener 的 map 管。死配置已删掉。
> 2. 实际**多出**三个键：`advertise_address`（默认广播地址，单机部署指向 `127.0.0.1`）、
>    `advertise_port`（默认 1884，ADVERTISE 的落点）、`max_duration_s`（默认 3600，把客户端
>    请求的休眠时长封顶，否则一个荒谬值能把会话钉住一周）、`max_inflight`（默认 20）。
>
> 另有一条语义修正：休眠登记超限不是「CONNACK 失败码」而是**按正常断开处理**并计
> `mqtt_sn_sleep_denied`——上限是硬边界，但 1.2 的 CONNACK 没有「休眠被拒」这个码可用。

---

## 6. MQTT over QUIC（`broker_proto_mqtt_quic`）

### 6.1 规范对齐（OASIS CN Draft 3）

- ALPN `mqtt`；**一个客户端发起的双向流**承载一条 MQTT Network Connection 的全部报文。
- 流关闭 ≠ 连接关闭；重连在新流上进行。
- 服务端初始只授予 1 条 bidi 流（`peer_bidi_stream_count => 1`），旧流关闭后再补 1 条。
- MQTT 报文格式零改动 → MQTT 3.1.1 与 5.0 同时支持。
- 不做：多流、0-RTT early data。

> **落地后修正（2026-11-11）**：「旧流关闭后再补 1 条」**做不到**，这是本设计里唯一被实测推翻
> 的一条。quicer 0.4.12 / msquic 2.5.11 下运行期
> `setopt(Conn, settings, #{peer_bidi_stream_count => N})` 返回 `ok`，但新的 `MAX_STREAMS` 不会
> 让客户端的新流到达（listen 时授 4 → 4 条顺序到达；握手后追加 → 永不到达）。因此改为
> **握手时一次性授予 `max_streams_per_conn`（默认 4）**：它是同一连接上的**重连配额**，不是
> 并发流数（Single Stream Mode 同一时刻只有一条活动 MQTT 流）。`peer_unidi_stream_count => 0`。
> 详见 ADR-024 D6。

### 6.2 监听与握手

`gen_server` 持有 listener：`quicer:listen/2` → accept 循环 → `quicer:handshake/2`（**必须显式**）→ `quicer:accept_stream/3` → 起 `mqtt_conn_fsm`，transport = `mqtt_transport_quic`。

- 数据/事件分派、流关闭 → 结束该连接会话；连接关闭 → 清理。
- 握手超时或证书不可用 → 记日志并关闭流，不影响其它连接。

> **落地后修正（2026-11-11）**：进程结构比这里多一层。`quicer:accept/3` 只报告**传输层**
> 连接，而 `{quic, connected, ...}` 投给连接的 **owner**，所以握手只能在拿到句柄之后做；
> 而「流会关、连接不会」要求两次流之间有个进程活着持有连接句柄。实际是：
>
> ```
> mqtt_quic_listener → acceptor → mqtt_quic_conn（每连接一个，拥有连接句柄，循环接新流）
>                                     └── mqtt_conn_fsm（每流一个，handle = stream）
> ```
>
> `mqtt_quic_conn` 只负责「接受新流并交接」，**不读 MQTT 字节**；流的 handle 经
> `controlling_process/2` 交给 `mqtt_conn_fsm`，FSM 成为流的 owner。交接顺序是「先移所有权、
> 再通知」，与 S8-A 的 `{handshake, ...}` 同理。
>
> 另有一条实测缺陷：`arm_once/1` 必须传**原子 `once`**，不能传整数 `1`——quicer 把整数 active
> 当 `{active, N}`（multi 模式），收满后向 owner 发 `{quic, passive, Stream, undefined}`，
> FSM 每读一包记一条 `unexpected event` 并 `protocol_errors` +1。回归断言
> `a_pingreq_is_not_a_protocol_error`。

### 6.3 TLS 与依赖形态

- QUIC 只能跑 TLS 1.3：`certfile`/`keyfile` 默认取 `broker_security.tls` 配置；**未配置证书时 QUIC 监听器拒绝启动**（`{quic_requires_tls, CertKey}`），其它协议不受影响。
- `quicer` 为 **profile 门控依赖**：

```erlang
{profiles, [
  {quic, [
    {deps, [{quicer, "0.4.12"}]},
    {relx, [{release, {unified_mq, "0.1.0"}, [... , broker_proto_mqtt_quic]}]}
  ]}
]}.
```

- 默认 `rebar3 compile|ct|release` 保持纯 BEAM、673 例可复现（**落地后：823/823、52 套件**；`broker_proto_mqtt_quic` 本身进两个 release，只是默认 release 不带 NIF）；`rebar3 as quic ct` / `as quic release` 才编 NIF 并让其进 release。

> **落地后修正（2026-11-11）**：上面的 `relx` 片段里那个 `...` 不能省略——profile 的 `relx`
> **替换**顶层那份，`quic` profile 必须复制**完整** app 列表，否则装出来的是一个只有 QUIC 的
> release。实际写法照抄顶层列表再追加 `quicer`。另外顶层列表原本漏了
> `broker_proto_mqtt_quic` 本身，已补（`1cd4cf5`），否则默认 release 的启动行为与手工构建
> 不一致。
- 运行期校验：`code:ensure_loaded(quicer)` 失败时只拒绝 QUIC 监听器并给出明确原因。
- 构建前提写入 `docs/operations.md`：需要 `cmake`/`gcc`/`ninja` 与网络（quicer 首次构建拉取 msquic 源码；本机 distro 无预编译包）。

### 6.4 配置与端口

```erlang
{broker_proto_mqtt_quic, [
  {listeners, [#{port => 14567,
                 certfile => undefined,   %% undefined = 取 broker_security.tls
                 keyfile  => undefined,
                 handshake_timeout_ms => 5000,
                 idle_timeout_ms => 30000,
                 max_streams_per_conn => 4,
                 max_connections => 1024}]}
]}
```

UDP `14567`（EMQX 默认 QUIC 端口，便于客户端对照）。

**落地后修正（2026-11-11）**：`alpn` 不是 listener 的键而是**这个 app 的 env**（默认
`[<<"mqtt">>]`，CN Draft 3 §2.1 的唯一值，listener map 不覆盖它——否则「固定 `mqtt`」这句
就不成立）。`max_streams_per_conn` 也已在 app env 里给默认值 4，listener map 可覆盖。发布
配置里 `listeners` 默认是 `[]`（见 `config/sys.config.src`），示例行是注释掉的：QUIC 无证书
必然拒绝启动，默认开会让不带证书的 release 起不来。

---

## 7. MQTT over WebSocket（`broker_proto_mqtt_ws`）

### 7.1 规范对齐（MQTT 3.1.1 §6 / MQTT 5.0 §6）

- 握手必须协商子协议：`mqtt`（3.1.1 与 5.0 都接受），兼容 `mqttv3.1`/`mqttv3.1.1`。
- 客户端**未提供** `Sec-WebSocket-Protocol` 或都不匹配 → HTTP 400 拒绝握手。
- 只接受 **binary** 帧；text 帧 → close 1003。
- 报文与帧无对齐要求：一个帧可含多个 MQTT 报文，一个报文可跨帧（FSM 按字节流缓冲）。
- 控制帧（ping/pong/close）由 WS 层处理，与 MQTT keepalive 独立。
- 不协商 `permessage-deflate`。

### 7.2 结构

- `broker_ws` 抽取：`ws_frame`（原样搬移）+ 新增 `ws_handshake`（请求解析、`Sec-WebSocket-Accept`、子协议选择、101 响应）。`broker_proto_ws` 改为调用同一套代码，**对端字节与行为不变**。
- `broker_proto_mqtt_ws`：ranch listener（TLS 走 `broker_security_tls`，即 wss 自动可用）→ 握手 → 每连接起 `mqtt_conn_fsm`（transport = `mqtt_transport_ws`）→ WS payload 注入 FSM。
- 与 `broker_proto_ws` 的自有 JSON/二进制 API **完全隔离**：不同 app、不同端口、不同子协议。

### 7.3 配置与端口

```erlang
{broker_proto_mqtt_ws, [
  {listeners, [#{port => 8083, ref => {mqtt_ws, 8083},
                 num_acceptors => 10, max_connections => 1024}]},
  {subprotocols, [<<"mqtt">>, <<"mqttv3.1">>, <<"mqttv3.1.1">>]},
  {max_frame_bytes, 16777216}
]}
```

TCP `8083`（`broker_proto_ws` 自有 API 是 8080），任意 path 均可（不绑定 `/mqtt`）。

**落地后修正（2026-11-11）**：上表的键与实现一致（`ref` 默认 `{mqtt_ws, Port}`，
`num_acceptors` 10、`max_connections` 1024，TLS 由 `broker_security_tls:transport/0` 决定，
所以 wss 自动可用）。两处需要补：

- **`arm_once/1` 在 WS 上是语义占位 no-op**：WS 层每帧后自行 re-arm，没有逐帧背压，
  `send/2` 异步。这是与 TCP 传输的实质差异，已写进 ADR-023 D5，不做假背压；真正的窗口
  控制仍是 `max_inflight` / `receive_maximum`。
- **帧解码不在 `decode_msg/2` 里**：`mqtt_transport_ws:decode_msg/2` 只把
  `{mqtt_ws, Bin}` / `{mqtt_ws, closed}` 归一化，帧边界与「升级是否完成」这些每连接状态
  留在 `mqtt_ws_conn`。理由与代价见 ADR-023。

---

## 8. 安全边界汇总

| 协议 | 凭据来源 | `require_auth` | TLS |
|---|---|---|---|
| MQTT-SN | 协议无凭据字段 | 配置即拒绝启动（显式） | 本轮无（DTLS 待办） |
| MQTT over QUIC | MQTT CONNECT | 沿用 MQTT 现状（`authenticate/2`；`require_auth` 缺口仍留 §12） | 强制 TLS 1.3 |
| MQTT over WS | MQTT CONNECT | 同上 | 可选，走 `broker_security_tls` |

三条新协议都复用统一身份表与 ACL；事件属性不参与身份/ACL 决策的既有约束不变。

---

## 9. 测试计划

### 9.1 仓内 CT（新增套件，均需 `all/0` 含 group）

| 套件 | 覆盖 |
|---|---|
| `ws_frame_SUITE`（迁移到 `broker_ws`） | 原有 9 例零回归（编解码、长度、掩码、分片、控制帧、超长、保留位、未知 opcode） |
| `ws_handshake_SUITE`（新增） | Accept 计算、子协议选择、缺失/不匹配子协议、超大头部（15 例） |
| `mqtt_transport_tcp_SUITE`（新增，原计划名 `mqtt_transport_SUITE`） | TCP/TLS 两档的消息归一化与 close 语义（10 例） |
| `mqtt_sn_codec_SUITE` | 全报文编解码、两种长度头、非法长度/未知类型、QoS -1（40 例） |
| `mqtt_sn_topics_SUITE` | 预定义 id、动态分配上限、短 topic、非法 id（21 例） |
| `mqtt_sn_conn_SUITE` | CONNECT/接管、pub/sub QoS0/1/2、will、retain、REGISTER 双向、休眠与唤醒、keepalive 判死、`require_auth` 拒绝启动（23 例） |
| `mqtt_quic_listener_SUITE` | 未装 quicer 时的降级、无证书拒绝启动、ALPN、单流接受、事件/数据分派（config 4 + quic 6） |
| `mqtt_quic_transport_SUITE` | `decode_msg/2` 分派、流关闭映射、缺 NIF 时的明确错误（13 例） |
| `mqtt_ws_conn_SUITE` | `mqtt` 子协议握手、text 帧拒绝、跨帧报文、多报文单帧、wss 路径（19 例） |

**落地后修正（2026-11-11）**：本表用例数已按各套件 `all/0`（含 `groups/0` 展开）回填。
两处与计划不符：

- `mqtt_transport_SUITE` 落地为 **`mqtt_transport_tcp_SUITE`**（只实现 TCP/TLS 一档，名字照实）。
- `ws_frame_SUITE` 是 **9 例**，而计划里的「22 例」从一开始就写错了：核对 S7 结案提交
  `42f84f0` 的 `apps/broker_proto_ws/test/ws_frame_SUITE.erl`，`all/0` 当时就是这 9 个
  （`encode_decode_roundtrip` … `close_payload_roundtrip`）。22 是同期 `ws_conn_SUITE` 的
  用例数，被误记到了帧编解码套件上。迁移本身零改动，9 例原样搬进 `broker_ws`。

另外 WS 侧新增的 `ws_handshake_SUITE` 15 例、S8-A 顺带修 wss 时给 `ws_conn_SUITE` 补的 1 例
（22 → 23），都不在本表的原始计划里。

用例数在实现完成后按 `all/0` 实数回填，不预先承诺总量。

**profile 与分组约束**：`broker_proto_mqtt_quic` 在默认 profile 下也会被编译（app 目录在 `apps/` 内），因此它只通过裸消息元组与 `quicer` 交互，**不 include 任何 quicer 头文件**，并把 `quicer` 写进 `optional_applications`。相应地，`mqtt_quic_*_SUITE` 在默认 profile 下只跑"降级 / 配置校验"组（按 `code:ensure_loaded(quicer)` 分组），真实 QUIC 组只在 `rebar3 as quic ct` 下执行。

**落地后修正（2026-11-11）**：这条落实为——`broker_proto_mqtt_quic` **出现在两个 release 的
app 列表里**（默认 release 只是不含 `quicer-0.4.12`，`.app` 带
`optional_applications => [quicer]` 与 `env listeners => []`）；`rebar3 as quic release` 下
`lib/quicer-0.4.12/priv/libquicer_nif.so` 确实存在（实测）。默认全量因此从 673 变为
**823/823、52 套件**（不是"673 口径不受影响"——`mqtt_quic_listener_SUITE` 的 config 组 4 例与
`mqtt_quic_transport_SUITE` 的 13 例确实加进了默认全量）；`rebar3 as quic ct` 为
**827 passed + 2 skipped**（两个「quicer 缺失」用例在 NIF 在场时按设计 skip）。

### 9.2 真实第三方客户端（Gate 证据）

| 协议 | 客户端 | 用例（草案） |
|---|---|---|
| MQTT-SN | `mqtt-sn-tools` 0.0.7（`mqtt-sn-pub`/`mqtt-sn-sub`） | connect、QoS0/1 pub/sub、预定义 topic id、短 topic、REGISTER、will、`-e` 休眠与唤醒、keepalive、QoS -1；**QoS2 无第三方客户端，只能用仓内驱动**，如实标注 |
| MQTT over QUIC | `emqtt`（EMQX 官方 Erlang 客户端，ALPN `mqtt` 单流） | connect、QoS0/1/2、retain、will、takeover、重连新流、异常流关闭、5.0 属性 |
| MQTT over WS | `paho-mqtt 2.1.0`（`transport="websockets"`） | connect、QoS0/1/2、retain、will、跨帧报文、text 帧被拒、5.0 |

证据目录沿用既有形态：`test/protocol-compatibility/{mqtt_sn,mqtt_quic,mqtt_ws}/`（boot 助手 + 驱动脚本 + `run/` 日志 + `report.json` + README）。

**落地后修正（2026-11-11）**：三份证据都落了盘，但用例清单各有出入，以
`docs/protocol-compatibility.md` §4.11~§4.13 为准。

- **MQTT-SN**：`mqtt-sn-tools` 0.0.7 **10/10**，但工具本身无 QoS 2、无 Will、
  `PINGREQ` 不带 client id——这三项只能由仓内套件覆盖，Gate 文本单列了这条边界。
- **QUIC**：`emqtt` 1.16.1 **8/8**（connect、QoS0/1/2、retain、Will、MQTT5、持久会话续连）。
  上表里的「takeover」「异常流关闭」**没有**进第三方驱动：`emqtt` 公开 API 没有「只关流」的
  入口（`close/1` 连连接一起关），塞进去只会得到一个测不出结论的用例。这两条改由 CT 用 raw
  `quicer` 断言（`a_new_stream_on_the_same_connection_is_a_new_session`）。
- **WS**：paho-mqtt 2.1.0 **8/8**；「text 帧被拒」由仓内 `mqtt_ws_conn_SUITE` 覆盖（paho
  不会发 text 帧）。另 1 例 raw framing 探针单列，标 `third_party: false`。

### 9.3 回归门槛

- `rebar3 ct`：既有 673 例零回归（**落地后：默认全量 823/823、52 套件**，零 FAILED / 零 Skipped）。
- `rebar3 as quic ct`：新增 QUIC 套件全绿（含 NIF 加载）——**落地后：827 passed + 2 skipped**。
- `rebar3 dialyzer`、`rebar3 xref` 干净（xref 仅允许 `is unused export`）——**落地后：0 warnings / 175 条均为预期 unused-export**。
- `rebar3 release` 与 `rebar3 as quic release` 均可装配；后者 release 内含 `libquicer_nif.so`——**已实测**。

---

## 10. Gate S8（草案，落地时按可核验证据勾选）

> **以 [`docs/roadmap.md`](../roadmap.md) §10.2 的 Gate S8 为准**——那里是逐项带证据
> 路径的定稿。本节的复选框已按同一份证据勾选，措辞保持草案原样，方便对照设计意图。

- [x] MQTT-SN：`mqtt-sn-tools` 真实客户端用例全绿，日志与 `report.json` 落盘——`test/protocol-compatibility/mqtt_sn/`，**10/10**
- [x] MQTT-SN：QoS2 与休眠/唤醒由仓内套件覆盖并有明确说明——`mqtt_sn_conn_SUITE`；工具能力边界（无 QoS2 / 无 Will / `PINGREQ` 不带 client id）在 `docs/protocol-compatibility.md` §4.11 单列
- [x] MQTT over QUIC：`emqtt` 单流客户端用例全绿，ALPN `mqtt` 有握手证据——`test/protocol-compatibility/mqtt_quic/` **8/8**；ALPN 由 `mqtt_quic_listener_SUITE` 的「ALPN `mqtt` 可握手」与「非 `mqtt` 被拒」两例钉死
- [x] MQTT over WebSocket：`paho-mqtt` websocket 传输用例全绿，子协议与 binary 帧有证据——`test/protocol-compatibility/mqtt_ws/` **8/8**（另 1 例 raw framing 探针单列）
- [x] 三协议各一行能力矩阵 + 逐协议限制清单（`docs/protocol-compatibility.md` §2 与 §4.11~§4.13）
- [x] `rebar3 ct` 零回归；`as quic ct` 全绿；dialyzer/xref 干净——**823/823、52 套件**（零 FAILED / 零 Skipped）；`as quic ct` **827 passed + 2 skipped**；`dialyzer` 0 warnings；`xref` 175 条均为预期 unused-export
- [x] `rebar3 release` 与 `as quic release` 均装配成功——后者含 `lib/quicer-0.4.12/priv/libquicer_nif.so`
- [x] 文档同步：roadmap、architecture、operations、security、README、progress

---

## 11. 文档同步清单

| 文件 | 变更 |
|---|---|
| `docs/roadmap.md` | 新增 §10 S8 章（目标/范围/阶段表/Gate），阶段总览表加一行，依赖图加 S7→S8，附录重编号 10→11 |
| `docs/protocol-compatibility.md` | §0 客户端矩阵三行；§2 能力矩阵三行；新增 §4.11/§4.12/§4.13 |
| `docs/architecture.md` | app 列表 +3、`broker_ws` 库、传输抽象、端口表 |
| `docs/operations.md` | 端口 1884/udp、14567/udp、8083；QUIC 构建前提与证书要求 |
| `docs/security.md` | SN 无凭据边界、QUIC 强制 TLS、WS 子协议 |
| `README.md` | 协议清单/需求与状态行 |
| `progress.md` | S8 进展与用例数 |
| `docs/adr/022`、`023`、`024` | SN 网关模型；quicer/单流/profile；RFC6455 抽库与 MQTT-over-WS 子协议 |

---

## 12. 风险与前提

| 风险 | 应对 |
|---|---|
| quicer 冷编译约 15 分钟且需网络 | profile 门控，默认构建不受影响；`operations.md` 写清前提 |
| `emqtt` 客户端若只实现多流 | 已核实其源码为单流（`start_stream` 一次、`active=1`）；Gate 首步先跑通握手再扩用例 |
| 传输抽象改造触碰 673 例的核心 FSM | 先加行为与 tcp 实现等价改造（纯重命名 + 归一化），全量回归通过后再接 QUIC/WS |
| `ws_frame` 迁移影响既有 WS 套件 | 只搬模块不改语义，`ws_frame_SUITE` 原样迁移（**落地后为 9 例**，计划里写的 22 是误记的 `ws_conn_SUITE` 数） |
| MQTT-SN QoS2 缺第三方客户端 | Gate 明确标注证据来源，不把仓内用例冒充真实客户端 |
| MQTT/TLS 修复引入行为变化 | 该路径当前不可用；补一条 TLS 回归用例作为证据 |

---

## 13. 实施顺序（供后续写执行计划）

1. `broker_ws` 抽库（`ws_frame` 迁移 + `ws_handshake`），既有 WS 套件零回归。
2. `mqtt_transport` 行为 + tcp/ssl 实现，`rebar3 ct` 全绿（含新增 TLS 回归）。
3. `broker_proto_mqtt_ws` + `mqtt_ws_conn_SUITE` + paho 真实客户端。
4. `broker_proto_mqtt_sn`（codec → topics → conn → listener）+ 套件 + `mqtt-sn-tools`。
5. `broker_proto_mqtt_quic`（transport → listener）+ profile + 套件 + `emqtt`。
6. 文档与 Gate 勾选、ADR 定稿。
