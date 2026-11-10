# S8-B 执行计划 — MQTT over WebSocket（`broker_proto_mqtt_ws`）

- **对应设计**：[`docs/design/s8-mqtt-sn-mqtt-over-quic-and-mqtt-over-ws.md`](../design/s8-mqtt-sn-mqtt-over-quic-and-mqtt-over-ws.md) §4、§7、§9.1、§9.2、§10
- **基线（改动前）**：46 套件 / 702 例（`all/0` 含 `groups/0`），dialyzer/xref/release 通过；HEAD `4a357aa`
- **范围**：只实现 MQTT over WebSocket。**不改** `mqtt_conn_fsm` 的协议语义（S8-A 已把传输抽成 `mqtt_transport`），不改 `broker_proto_ws` 的自有 JSON/二进制 API。
- **阶段顺序**：设计 §13 第 3 步。C（MQTT-SN）、D（MQTT over QUIC）各自另写计划
- **执行约束**（仓库既定规则）：
  - 编译/测试前 `export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"`
  - `rebar3 ct` 前 `rm -rf _build/test/extras`
  - 套件用例数一律由 `all/0`（含 `groups/0`）实数回填，不预先承诺
  - 不杀用户进程或容器（`18831`/`18841`/`18081` 被占用），CT 一律用 `port => 0`
  - 本阶段**不新增任何运行时依赖**（`broker_ws` 是纯 BEAM 库）
  - 提交一律用显式路径，不 `git add -A`

---

## 关键架构决定（写代码前必须先定）

### 决定 1：两个进程还是一条连接一个进程？

`mqtt_conn_fsm` 是协议状态机（TCP 字节流），WebSocket 是带握手与帧边界的**另一层**协议。两者不能塞进同一个回调循环而不让 FSM 知道 WS。方案只有两个：

| 方案 | 结构 | 代价 |
|---|---|---|
| **A（选定）** | `mqtt_ws_conn` = ranch 协议进程，拥有 socket，做 HTTP 升级 + WS 帧编解码；`mqtt_conn_fsm` 在 `mqtt_conn_sup` 下另起，`transport = mqtt_transport_ws`，handle = `mqtt_ws_conn` 的 pid | 每连接 2 个进程 + 一次消息跳转 |
| B | FSM 自己拥有 socket，`mqtt_transport_ws:decode_msg/2` 里就地解 WS 帧 | WS 分帧状态（半帧缓冲、握手态）无处安放：`decode_msg/2` 是纯函数、handle 在 FSM 里不可变；只能塞进 process dictionary 或 ETS，破坏 `mqtt_transport` 的契约 |

**选 A**。理由：
- 握手态/半帧缓冲天然属于一个进程的循环状态，`mqtt_transport_ws` 的四个回调保持纯函数语义（与 `mqtt_transport_tcp` 一致）。
- 控制帧（ping/pong/close）与 MQTT keepalive 必须独立（设计 §7.1 明文要求），只有独立的 WS 层进程能做到。
- 设计 §7.2 原话就是"每连接起 `mqtt_conn_fsm`（transport = `mqtt_transport_ws`）→ WS payload 注入 FSM"。

**`mqtt_transport_ws` 的 handle = `mqtt_ws_conn` 的 pid**，四个回调：

| 回调 | 实现 |
|---|---|
| `send(WsPid, IoData)` | `WsPid ! {mqtt_ws_send, iolist_to_binary(IoData)}`；返回 `ok`（异步，与 `gen_tcp:send` 的同步语义有差异，见决定 4） |
| `arm_once(WsPid)` | `WsPid ! {mqtt_ws_arm, self()}`；`ok`（WS 层按自己的节奏读 socket，见决定 4） |
| `close(WsPid)` | `WsPid ! mqtt_ws_close`；`ok` |
| `decode_msg({mqtt_ws_data, WsPid, Bin}, WsPid)` | `{data, Bin}` |
| `decode_msg({mqtt_ws_closed, WsPid}, WsPid)` | `closed` |
| `decode_msg({mqtt_ws_error, WsPid, R}, WsPid)` | `{error, R}` |
| `decode_msg(_, _)` | `other` |

### 决定 2：WS 层用 `ws_frame` + `ws_handshake:accept/2`，不重写 RFC 6455

- 握手：`ws_handshake:accept(Head, #{supported_subprotocols => Subprotocols})`。
  - `{ok, Response, Selected}` → 回 101（响应里已按服务端偏好写入第一个交集子协议）。
  - `{error, Response}` → 原样回该 HTTP 响应（400/426），关闭连接。
  - 头部超 `ws_handshake:max_handshake()` → 回 431（与 `ws_conn` 一致）。
- 取子协议列表：`application:get_env(broker_proto_mqtt_ws, subprotocols, [<<"mqtt">>, <<"mqttv3.1">>, <<"mqttv3.1.1">>])`。
- 帧：`ws_frame:decode/2`（`max_frame_bytes` 来自 app env）+ `ws_frame:encode/2`。

### 决定 3：**WSS 必须显式做 TLS 握手**（既有 `broker_proto_ws` 的缺陷，本阶段不复制）

ranch 2.x 的协议侧必须自己调 `RanchTransport:handshake/2`；否则 `ssl:setopts/2` 会因 `notsup_on_transport_accept_socket` 失败。`mqtt_conn_fsm` 已经这么做（`awaiting_owner` 的 `{handshake, ...}` 子句），但**既有的 `apps/broker_proto_ws/src/ws_conn.erl` 从不调用它**。

本阶段的两条动作：

1. `mqtt_ws_conn` 收到 `{handshake, Ref, RanchTransport, Socket, Timeout}` 后**第一步**就 `RanchTransport:handshake(Socket, Timeout)`（TCP no-op、SSL 真握手），失败则记日志并停止。
2. **已核实（Task 1 实测）**：既有 `broker_proto_ws` 的 wss **确实完全不可用**。自签证书 + `ssl:connect` + 一次 WS 握手，结果：
   ```
   exception exit: {socket_error,notsup_on_transport_accept_socket}  (ws_conn:init/1)
   Ranch listener {ws,0} had connection process started with ws_listener:start_link/3
     exit with reason: {socket_error,notsup_on_transport_accept_socket}
   客户端侧: {error,closed}
   ```
   根因确认：`ws_conn` 的 `{handshake, ...}` 子句只做 `Transport:setopts/2`，从不调 `RanchTransport:handshake/2`。**这是既有缺陷，不是 S8-B 引入的。**
3. 处理方式：本阶段 `mqtt_ws_conn` 从一开始就正确握手（已跑通 wss 用例）；既有 `ws_conn` 的同一缺口在 Task 4 修复并补回归用例——与 S8-A"顺带修复 MQTT over TLS"同一处理方式（既有缺陷、零行为变化、有证据）。
   - **不在本阶段**把 `ws_conn` 的 wss 缺口写成"已支持"。

### 决定 4：背压与关闭语义（必须写清楚，否则以后会被当成 bug）

- **WS 层自己管读节奏**：`mqtt_ws_conn` 收完 HTTP 升级后即进入 `{active, once}` 自循环，每收一段就重新 arm；它**不等待** FSM 的 `arm_once`。
  - 因此 `mqtt_transport_ws:arm_once/1` 是**语义占位**：它接受并返回 `ok`，但不会暂停 socket。原因：FSM 的 `active_once/1` 在每条数据后都会调用它，若真按此节流，则 WS 层必须先缓存再等信号，收益为零（同进程内 FSM 消费是微秒级），复杂度不成比例。
  - 这条**必须**写进模块 doc 与 `progress.md`：WS 传输没有逐条背压，MQTT 的 `receive_maximum`/`max_inflight` 仍是唯一窗口控制。
- **关闭顺序**：WS close 帧（opcode 8）→ 回 close → 给 FSM 发 `{mqtt_ws_closed, self()}` → 停止。FSM 主动关闭（`terminate/3` → `close/1`）→ WS 层发 close 帧（1000）后关闭 socket。
- **Ping/pong**：opcode 9 → 回 opcode 10，与 MQTT PINGREQ 完全独立。

### 决定 5：报文与帧无对齐

`mqtt_ws_conn` 把每个 binary 数据帧的 payload **原样**转给 FSM（`{mqtt_ws_data, self(), Payload}`），**不做**"一帧一报文"假设。FSM 的 `#st.buf` 负责跨帧拼接、单帧多报文。这是设计 §7.1 的硬要求，必须有用例。

### 决定 6：text 帧 → close 1003

opcode 1（text）→ 回 close 1003 后停止。**不**把 text payload 当 MQTT 字节喂给 FSM。

---

## Task 1 — `broker_proto_mqtt_ws` app 骨架 + `mqtt_ws_conn`（WS 层）+ `mqtt_transport_ws`

**目标**：一条连接跑通 `WS 握手 → binary 帧 → mqtt_conn_fsm → binary 帧`，MQTT 语义全部复用 FSM。

**1a. 新建 app**

```
apps/broker_proto_mqtt_ws/
├── src/broker_proto_mqtt_ws.app.src
├── src/broker_proto_mqtt_ws_app.erl
├── src/broker_proto_mqtt_ws_sup.erl
├── src/mqtt_ws_listener.erl
├── src/mqtt_ws_conn.erl
├── src/mqtt_transport_ws.erl
└── test/mqtt_ws_conn_SUITE.erl
```

`broker_proto_mqtt_ws.app.src`（照 `broker_proto_ws` 的形态）：

```erlang
{application, broker_proto_mqtt_ws, [
    {description, "MQTT over WebSocket: RFC 6455 transport for the MQTT engine"},
    {vsn, "0.1.0"},
    {registered, [broker_proto_mqtt_ws_sup]},
    {mod, {broker_proto_mqtt_ws_app, []}},
    {applications, [kernel, stdlib, sasl, ranch, broker_ws, broker_proto_mqtt,
                    broker_core, broker_metrics, broker_storage, broker_router,
                    broker_queue, broker_subscription, broker_event,
                    broker_security, broker_retain]},
    {env, [{listeners, []},
           {subprotocols, [<<"mqtt">>, <<"mqttv3.1">>, <<"mqttv3.1.1">>]},
           {max_frame_bytes, 16777216}]}
]}.
```

`broker_proto_mqtt_ws_app` 与 `_sup` 照 `broker_proto_ws_*`；`_sup` 的 `listener_spec/1`：

```erlang
listener_spec(Spec) when is_map(Spec) ->
    Port = maps:get(port, Spec),
    Ref = maps:get(ref, Spec, {mqtt_ws, Port}),
    {Transport, TlsOpts} = broker_security_tls:transport(),
    TransOpts = #{socket_opts => [{port, Port} | TlsOpts],
                  num_acceptors => maps:get(num_acceptors, Spec, 10),
                  max_connections => maps:get(max_connections, Spec, 1024)},
    (ranch:child_spec(Ref, Transport, TransOpts, mqtt_ws_listener, #{}))#{
        restart => transient
    }.
```

**1b. `mqtt_ws_listener`（ranch 协议）**

```erlang
-module(mqtt_ws_listener).
-behaviour(ranch_protocol).
-export([start_link/3]).

start_link(Ref, Transport, Opts) ->
    mqtt_ws_conn:start_link(Ref, Transport, Opts).
```

**1c. `mqtt_ws_conn`（gen_server，WS 层）**

状态：

```erlang
-record(st, {
    ref :: term(),
    transport :: module() | undefined,   %% ranch_tcp | ranch_ssl
    socket :: term() | undefined,
    phase = handshake :: handshake | mqtt,
    buffer = <<>> :: binary(),
    fsm :: pid() | undefined,
    frag = undefined :: undefined | {0 | 1 | 2, [binary()]}
}).
```

流程：

1. `init` → `{ok, #st{ref = Ref, transport = Transport}}`。
2. `handle_info({handshake, _Ref, RanchTransport, Socket, Timeout}, #st{phase = handshake} = St)`：
   - `case RanchTransport:handshake(Socket, Timeout) of {ok, Sock} -> ...; {error, R} -> stop end`（**决定 3**）。
   - `arm(Sock)`（`Transport:setopts(Sock, [{active, once}])`）。
3. `handle_info({tcp|ssl, Socket, Data}, #st{socket = Socket})` → `on_data(Data, St)`：
   - `phase = handshake`：
     - 缓冲累积；超过 `ws_handshake:max_handshake()` → 回 431 + 停止。
     - `binary:split(Buffer, <<"\r\n\r\n">>)`：不完整 → 存缓冲、重新 arm；完整 → `ws_handshake:accept(Head, #{supported_subprotocols => subprotocols()})`：
       - `{ok, Response, _Selected}` → `raw_send(Response)`；`phase = mqtt`；`start_fsm()`；把 `Rest` 当作 MQTT 数据继续处理。
       - `{error, Response}` → `raw_send(Response)`，停止（`normal`）。
   - `phase = mqtt`：`parse_frames(Buffer, St)`。
4. `parse_frames`：`ws_frame:decode(Buffer, max_frame_bytes())`
   - `{ok, Frame, Rest}` → `handle_frame/2`，递归。
   - `{more, _}` → 存缓冲、重新 arm。
   - `{error, Reason}` → 回 close 1002，停止。
5. `handle_frame/2`：
   - opcode 8 → 回 close（同 code），给 FSM `{mqtt_ws_closed, self()}`，停止。
   - opcode 9 → 回 pong（同 payload），继续。
   - opcode 10 → 忽略，继续。
   - opcode 1（text）→ close **1003**（决定 6），给 FSM `{mqtt_ws_closed, ...}`，停止。
   - opcode 2 / 0（binary / continuation）→ 分片重组；`fin = true` 时把完整 payload 发给 FSM。
     - **简化点（必须有测试）**：`ws_frame:decode` 已返回每帧 payload，WS 层只做 `fin` 重组；重组完成后 `FSM ! {mqtt_ws_data, self(), Payload}`（**不**拼接多帧、**不**等待 FSM 消费）。
   - 未知 opcode → close 1002。
6. `start_fsm()`：
   - `Opts = #{transport => mqtt_transport_ws}`；
   - `{ok, Pid} = mqtt_conn_sup:start_conn(self(), Opts)`（把**自己的 pid** 作为 socket handle 传进去）；
   - `true = link(Pid)`；
   - `Pid ! connect`（把 FSM 从 `awaiting_owner` 推进到 `awaiting_connect`；`arm_socket/2` 会对 handle 调 `arm_once/1`，即给 WS 层发 `{mqtt_ws_arm, _}`）。
   - 记录 `fsm = Pid`。
7. 其它消息：
   - `{mqtt_ws_send, Bin}` → `raw_send(ws_frame:encode(2, Bin))`。
   - `{mqtt_ws_arm, _Fsm}` → 忽略（决定 4），返回后继续。
   - `mqtt_ws_close` → 回 close 1000，停止。
   - `{'EXIT', Fsm, _Reason}` → 停止（FSM 死了，连接没有意义）。**需要 `process_flag(trap_exit, true)`**，否则 link 会把 WS 层直接打死而没有 close 帧。
8. `terminate/2`：`Transport:close(Socket)`；若 FSM 还活着且不是 FSM 主动关的，`FSM ! {mqtt_ws_closed, self()}`（避免 FSM 悬挂）。

   注：**`gen_statem` / `gen_server` 默认都不 trap exit**（这条写错过一次，以实测为准）。方向是单向的：WS 层 `process_flag(trap_exit, true)` 并 `link(Fsm)`，所以 FSM 死时 WS 层收得到 `{'EXIT', Fsm, Reason}`；WS 层自己正常退出时 link 不会打死 FSM，因此必须靠 `terminate/2` 里的 `{mqtt_ws_closed, self()}` 让 FSM 走正常关闭路径——只有走那条路径才会发布 Will。

**1d. `mqtt_transport_ws`**

```erlang
-module(mqtt_transport_ws).
-behaviour(mqtt_transport).
-export([send/2, arm_once/1, close/1, decode_msg/2]).
```

实现见"决定 1"表。`decode_msg/2` 必须**按 handle 匹配**（与 `mqtt_transport_tcp` 的纪律一致：别的连接的字节不能串进来）。

**1e. `rebar.config`**

- `relx.release` 列表在 `broker_proto_mqtt` 之后、`broker_proto_ws` 附近加入 `broker_proto_mqtt_ws`（release 要能装配）。
- 不需要新依赖（`broker_ws`、`broker_proto_mqtt` 都已在）。

**1f. `config/sys.config.src`**

在 `broker_proto_ws` 条目后加：

```erlang
{broker_proto_mqtt_ws, [
  %% MQTT over WebSocket (S8-B). Separate app and port from the broker's own
  %% ws API (8080): the two speak different protocols and never share a
  %% listener. Any URL path is accepted; the subprotocol does the negotiation.
  {listeners, [
    #{port => 8083,
      ref => {mqtt_ws, 8083},
      num_acceptors => 10,
      max_connections => 1024}
  ]},
  {subprotocols, [<<"mqtt">>, <<"mqttv3.1">>, <<"mqttv3.1.1">>]},
  {max_frame_bytes, 16777216}
]},
```

**1g. 验证**

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
rebar3 compile
```

`mqtt_ws_conn` 写完后先手工跑一次（用 `rebar3 shell` 或临时 `erl -noshell`），确认 `ws_handshake:accept/2` 的 101 字节与 paho 期望一致，再写套件。

**已执行结果**：`rebar3 compile` 干净；临时探针（`/tmp/opencode/smoke_ws.erl`、`smoke_ws_tls.erl`）六项全过——`connect_and_connack`、`missing_subprotocol_refused`(400)、`text_frame_closed_1003`、`packet_split_across_frames`、`two_packets_one_frame`、`wss_connect_connack`（101 含 `Sec-WebSocket-Protocol: mqtt`）。`rebar3 as prod release` 装配成功且 release 内含 `broker_proto_mqtt_ws-0.1.0`；`rebar3 dialyzer` 0 warning（首轮 4 条：`start_conn/2` spec 不收非 socket handle、`{noreply, _}` 不可达分支、`reason_bin/1` 死子句，均已修）。

**1h. 提交**

```bash
git commit -m "feat(s8-b): WebSocket transport for MQTT (broker_proto_mqtt_ws)"
```

---

## Task 2 — `mqtt_ws_conn_SUITE`

**目标**：字节级覆盖 WS 层语义，不依赖任何第三方客户端。

套件骨架照 `test/integration/s1_e2e_SUITE.erl` / `apps/broker_proto_ws/test/ws_conn_SUITE.erl`：`init_per_testcase` 逐用例写全 env、`port => 0`、`stop_all/0` 顺序、`create_partitions/0`。

**注意（S8-A 的教训）**：本套件会设 `broker_security.tls`，必须在 `init_per_testcase` **快照原始 tls 值**并用 Config 透传到 `end_per_testcase` 恢复那个快照（不能 teardown 时重读），否则会泄漏到同一 CT 节点的后续套件。

已实现：**19 例**（`all/0` 实数，19/19 绿，连跑 3 次稳定；全量 CT 待 Task 4 复跑，届时基线 702 + 本轮 19）。

| 用例 | 断言 |
|---|---|
| `subprotocol_mqtt_is_negotiated` | 只提供 `mqtt` → 101 且响应回 `mqtt`，随后 CONNECT 拿到 CONNACK |
| `subprotocol_mqttv311_is_negotiated` | 只提供 `mqttv3.1.1` → 101 且回 `mqttv3.1.1` |
| `the_server_preference_wins_over_the_client_order` | 客户端 `mqttv3.1, mqtt` → 仍选 `mqtt`（服务端偏好顺序） |
| `missing_subprotocol_is_refused` | 不带该头 → HTTP 400（不升级） |
| `unknown_subprotocol_is_refused` | `Sec-WebSocket-Protocol: chat` → HTTP 400 |
| `connect_returns_connack` | binary 帧装 CONNECT → CONNACK（`session_present = 0`, `return_code = 0`） |
| `publish_qos1_roundtrip_and_ack` | 订阅 + QoS1 发布 → PUBACK + 投递 PUBLISH，回 PUBACK |
| `publish_qos2_roundtrip_and_ack` | QoS2 双向握手：PUBLISH→PUBREC→PUBREL→PUBCOMP，回程 PUBREC→PUBREL→PUBCOMP |
| `a_retained_publish_reaches_a_late_subscriber` | retain=1 发布后，后到的订阅者收到 `retain = 1` 的保留消息 |
| `packet_split_across_frames_is_reassembled` | 一个 CONNECT 拆成两个 binary 帧 → 正常 CONNACK（跨帧，FSM `buf` 拼接） |
| `several_packets_in_one_frame_are_all_handled` | CONNECT + SUBSCRIBE 粘在一个帧 → CONNACK + SUBACK（单帧多报文，且证明 CONNECT 真的推进了状态） |
| `ws_ping_is_answered_with_pong` | CONNECT 之前发 WS ping → 收回同 payload 的 pong |
| `a_text_frame_is_closed_with_1003` | 发 text 帧 → close 1003 |
| `a_client_close_frame_ends_the_session` | 客户端 close → 服务端回 close，随后 socket 关闭 |
| `an_oversized_frame_closes_with_1002` | 帧超过 `max_frame_bytes` → close 1002（`ws_frame` 对每个 decode 错误统一 1002） |
| `any_path_is_accepted` | `/`、`/ws`、`/mqtt`、`/deep/nested/path` 都能升级并 CONNECT |
| `will_is_published_when_the_socket_drops` | 不发 close 直接断 socket → Will 投递给订阅者（验证 `terminate/2` 通知到了 FSM） |
| `wss_connect_returns_connack` | 自签证书 + TLS enabled → `ssl:connect` + 同一套 WS 握手 → CONNACK（**决定 3**） |
| `a_plain_tcp_client_on_a_wss_port_is_refused` | 明文 TCP 打到 wss 端口 → TLS 握手失败，连接结束且 `mqtt_conn_sup` 下无残留（**决定 7**） |

**wss 用例**复用 `apps/broker_proto_mqtt/test/mqtt_tls_SUITE.erl` 的 `write_self_signed/2` openssl 手法（`-days 1 -subj /CN=localhost -addext subjectAltName=DNS:localhost`），客户端用 `ssl:connect("localhost", Port, [binary, {active, false}, {verify, verify_none}])`。

**WS 客户端助手**（放在套件内，不建公共库）：`open/1,2`（TCP + HTTP 升级 + 校验 101）、`handshake/3,4`（可选 TLS）、`send_frame/3`（`ws_frame:encode(Op, Payload, true)`，客户端必须 mask）、`recv_frame/1,2`、`request/2`、`send_only/2`。

**决定 7（Task 2 期间新增并落地）：对端造成的失败，WS 层以 `normal` 退出**

`mqtt_ws_conn` 遇到 TCP RST、TLS 握手不完成、re-arm 失败时，不再以 `{socket_error, _}` 作为**自己的退出原因**，而是 `stop normal` + 把细节放进 state，由 `terminate/2` 发 `{mqtt_ws_error, self(), Reason}` 给 FSM（FSM 记一条日志后正常关闭，Will 照常发布）。理由：

- 对端重置连接是常态；非 `normal` 退出会让 ranch/监督树为**每条坏 TCP** 打一份崩溃报告。
- `mqtt_transport` 的 `{error, Reason}` 语义本来就有（"also a disconnect, logged"），WS 侧不发它会让 `mqtt_transport_ws:decode_msg/2` 的那个分支成为死代码。
- 是为了让 WS 与 `mqtt_transport_tcp` 的 `{tcp_error, ...}` 行为一致。

**实测踩到的两个坑**（都已固化进套件）：

1. **读帧必须按字节精确读**（`gen_tcp:recv(S, N)` 逐段读 header/长度/mask/payload）。用 `recv(S, 0)` + `ws_frame:decode` 再丢掉余量，会在服务端把两个回包（CONNACK + SUBACK）写进同一个 TCP 段时丢掉第二帧，表现为下一读永久阻塞——单跑必过、全量必挂。
2. **`PUBREC`/`PUBCOMP` 没有回包**：用 `request/2` 会阻塞到超时；这类包用 `send_only/2`。订阅端发 `PUBREC` 被回答的是 `PUBREL`（不是 `PUBREC`）。

**验证**

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
rm -rf _build/test/extras
rebar3 ct --suite=apps/broker_proto_mqtt_ws/test/mqtt_ws_conn_SUITE.erl
```

**提交**

```bash
git commit -m "test(s8-b): cover the MQTT-over-WebSocket transport"
```

---

## Task 3 — 真实客户端 Gate 证据（paho-mqtt 2.1.0）

**目标**：用 `paho-mqtt 2.1.0` 的 `transport="websockets"` 证明互操作，落盘 `test/protocol-compatibility/mqtt_ws/`。

目录形态照 `test/protocol-compatibility/ws/`：

```
test/protocol-compatibility/mqtt_ws/
├── mqtt_ws_broker_boot.erl     # 起一个只有 MQTT-WS 监听器的节点，打印 PORT
├── mqtt_ws_compat.py           # paho-mqtt 驱动，--all / --case <name>
├── README.md
├── run/                        # gitignore：broker.log + report.json
└── data/                       # gitignore：WAL / sessions
```

**boot 助手**照 `ws_broker_boot.erl`：逐 app `application:load`、`broker_storage/wal`、`create_partition`、`application:ensure_all_started(broker_proto_mqtt_ws)`（`broker_proto_mqtt` 只作为依赖被启动，**自己的 TCP 监听器保持空**——这样任何用例都不可能偷偷走明文 MQTT）、`ranch:get_port({mqtt_ws, 0})` 回读、打印 `PORT <n>` + `MQTT-WS-BROKER-READY`。

**驱动脚本**用 `paho.mqtt.client.Client(CallbackAPIVersion.VERSION2, transport="websockets")` + `ws_set_options(path="/mqtt")`（顺带验证任意 path）。

已实现：**9 例**（`--all` 连跑 3 次全绿；8 例 paho 第三方 + 1 例原始 socket 探针），报告 `run/report.json` 里 `third_party_cases: 8` / `transport_probes: 1`。

| 用例 | 客户端 | 断言 |
|---|---|---|
| `ws-connect` | paho-mqtt 2.1.0 | `connect()` 成功，`on_connect` reason_code == 0 |
| `ws-pubsub-qos0` | paho-mqtt | QoS0 订阅 + 发布往返，`qos == 0` |
| `ws-pubsub-qos1` | paho-mqtt | QoS1 往返，双向 PUBACK，`info.is_published()` |
| `ws-pubsub-qos2` | paho-mqtt | QoS2 四步握手（PUBREC/PUBREL/PUBCOMP）双向完成 |
| `ws-retain` | paho-mqtt | retained 消息在新订阅者连上后即时投递，`retain = 1` |
| `ws-will` | paho-mqtt | 不发 DISCONNECT 直接关 socket，订阅者收到 Will |
| `ws-mqtt5` | paho-mqtt | `MQTTv5` + `transport="websockets"` connect + pub/sub |
| `ws-path` | paho-mqtt | `ws_set_options(path="/mqtt")` 被接受 |
| `ws-raw-framing` | **原始 socket 探针** | 跨帧重组 + text 帧 → close 1003；报告内 `third_party: false` |

**跨帧与 text 帧证据怎么做（paho 不会故意做这两件事）**：`mqtt_ws_compat.py` 里有一个**独立的原始 socket 子检查**（同一脚本、同一 broker，但自己 `socket` + `struct` 手工掩码发帧，**不用** `ws_frame`）：它只用来证明"服务端接受跨帧 / 拒绝 text 帧"，报告里单列 `ws-raw-framing`、`client: "raw-socket-probe"`、`third_party: false`。**不**把 raw 探针算作第三方客户端证据。

**实测踩到的坑（paho 2.1.0）**：`MQTTMessageInfo.wait_for_publish/1` 成功时返回 `None`（只在部分错误上抛异常），所以 `check(info.wait_for_publish(...))` 恒假——判据必须用 `info.is_published()`。

`report.json` 每条用例记 `case` / `client`（`paho-mqtt` 或 `raw-socket-probe`）/ `seconds` / `evidence`（失败则 `error`），顶层另有 `third_party_cases` 与 `transport_probes` 两个计数。README 写清客户端版本、路径、paho 覆盖的用例、raw 探针的边界。

**验证**

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
/usr/bin/python3 test/protocol-compatibility/mqtt_ws/mqtt_ws_compat.py --all
```

**提交**

```bash
git commit -m "test(s8-b): paho-mqtt websocket compatibility evidence"
```

---

## Task 4 — 全量验证与文档同步

**4a. 门禁**

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
rm -rf _build/test/extras
rebar3 ct
rebar3 dialyzer
rebar3 xref          # 仅允许 is-unused-export
rebar3 as prod release
```

**4b. 核实并处理 `broker_proto_ws` 的 wss 缺口（决定 3 第 2 条）**

若 Task 1 的核实确认既有 `ws_conn` 的 wss 不可用，Task 4 里补 `RanchTransport:handshake/2` 并加一条回归用例（`ws_conn_SUITE` 加一例 wss）。若实测能用，只在 `progress.md` 记录核实结论，不改代码。

**已执行**：缺口确认存在（Task 1 实测 + Task 4 复现），已修 `apps/broker_proto_ws/src/ws_conn.erl`：`{handshake, _Ref, Transport, Socket, Timeout}` 分支先 `Transport:handshake(Socket, Timeout)`，失败 → `{stop, {tls_handshake_failed, Reason}, _}`；`ranch_tcp:handshake/2` 是 no-op 返回原 socket，所以一个分支覆盖两种 transport。回归用例 `ws_conn_SUITE:wss_is_served_when_tls_is_enabled`（自签证书 + TLS 快照/恢复 + `ssl:connect` 后走同一套 WS 握手拿 CONNACK），套件 22 → **23 例**。**反向验证**：`git stash` 掉 `ws_conn.erl` 的修复后该例 FAILED，恢复后通过。`docs/security.md` §9 按"例外"条款补了「协议侧必须真的把 TLS 握手跑完」一段。

**4c. 回填用例数（强制）**

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
erl -noshell -pa _build/test/lib/*/ebin -eval '
  Suites = [mqtt_ws_conn_SUITE],
  [io:format("~s ~p~n", [S, length(S:all())]) || S <- Suites],
  halt().'
```

`rebar3 ct` 通过的总例数 = 702 + 本阶段新增，两者都要写（区分"基线"与"S8-B 新增"）。

**已执行**：`mqtt_ws_conn_SUITE` 19 例、`ws_conn_SUITE` 23 例（+1）；全量 **722/722**（47 套件，基线 702 + 19 + 1），零 FAILED；`dialyzer` 0 warnings；`xref` 仅预期 unused-export；`rebar3 as prod release` 装配成功，`_build/prod/rel/unified_mq/lib/broker_proto_mqtt_ws-0.1.0` 存在。

**4d. 文档同步（本阶段只动这 4 个文件）**

| 文件 | 变更 |
|---|---|
| `docs/architecture.md` | app 列表加 `broker_proto_mqtt_ws`；§3.9 传输实现列表加 `mqtt_transport_ws`；端口表加 TCP 8083 |
| `docs/operations.md` | 释放端口 8083；`broker_proto_mqtt_ws` 配置块（`listeners`/`subprotocols`/`max_frame_bytes`） |
| `docs/protocol-compatibility.md` | 新增 §4.13（MQTT over WS）能力矩阵行 + 限制清单 |
| `progress.md` | S8 进展 + 本阶段用例数 + 下一步（C：MQTT-SN） |

**本阶段不动**：`docs/roadmap.md` 的 §S8 Gate 勾选、`docs/security.md`（WS 子协议段留到 B/C/D 全部结案时统一写）、`README.md`、ADR（统一收尾）。
**例外**：设计 §7 说"TLS 走 `broker_security_tls`"，若核实出 wss 缺口并修复，`docs/security.md` §9 需要同步一句——此时才动。

**4e. 提交**

```bash
git commit -m "docs(s8-b): record MQTT over WebSocket in architecture, ops and progress"
```

---

## 完成判据（Definition of Done）

- [x] `broker_proto_mqtt_ws` app 建立，监听 TCP 8083（CT 用 `port => 0`），TLS 走 `broker_security_tls`
- [x] `mqtt_ws_conn` 完成 HTTP 升级（子协议协商，缺失/不匹配 → 400）、binary 帧解码、text 帧 → close 1003、控制帧独立处理
- [x] `mqtt_transport_ws` 实现 `mqtt_transport` 四回调，FSM 零改动接入
- [x] `mqtt_ws_conn_SUITE` 全绿（19/19，连跑 3 次稳定；含 wss、跨帧、单帧多报文、text 帧拒绝、明文 TCP 打 wss 端口）
- [x] `paho-mqtt 2.1.0` websocket 用例全绿（8/8，另有 1 例 raw framing 探针单列），`report.json` / `broker.log` 落盘
- [x] `rebar3 ct` 总例数 = 722（基线 702 + 本套件 19 + ws_conn 回归 1），零 FAILED
- [x] dialyzer / xref 干净，`rebar3 as prod release` 装配成功（release 内含 `broker_proto_mqtt_ws`）
- [x] `docs/architecture.md` / `operations.md` / `protocol-compatibility.md` §4.13 / `progress.md` 同步，用例数回填
- [x] 既有 `broker_proto_ws` 的 wss 缺口已修复并加回归（见 4b）

## 不在本阶段（明确留给后续）

| 项 | 阶段 |
|---|---|
| `broker_proto_mqtt_sn`（MQTT-SN 1.2 网关 + `mqtt-sn-tools`） | **S8-C** |
| `broker_proto_mqtt_quic`（quicer + 单流 + `emqtt`） | **S8-D** |
| `permessage-deflate` 压缩扩展 | 不做（设计 §2 明确排除） |
| MQTT `require_auth` 接线 | 仍留 `docs/security.md` §12 |
| `docs/roadmap.md` §S8 Gate 勾选、ADR-022/023/024、`README.md` | B/C/D 全部结案后的统一收尾 |
