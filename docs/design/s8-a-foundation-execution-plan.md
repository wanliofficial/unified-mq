# S8-A 执行计划 — 基础设施：`broker_ws` 库抽取 + `mqtt_transport` 传输抽象（含 MQTT/TLS 修复）

- **对应设计**：[`docs/design/s8-mqtt-sn-mqtt-over-quic-and-mqtt-over-ws.md`](../design/s8-mqtt-sn-mqtt-over-quic-and-mqtt-over-ws.md) §4、§7.2、§12 第 4 行
- **基线（改动前）**：43 套件 / 673 例（`all/0` 含 `groups/0`），dialyzer/xref/release 通过
- **范围**：本阶段只做**协议无关的底座**。不实现任何新协议，但必须修好 MQTT over TLS（设计 §2 最后一行的既有缺陷）
- **阶段顺序**：这是设计 §13 的第 1、2 步。B（MQTT over WS）、C（MQTT-SN）、D（MQTT over QUIC）各自另写计划
- **执行约束**（来自仓库既定规则）：
  - 编译/测试前 `export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"`
  - `rebar3 ct` 前 `rm -rf _build/test/extras`
  - 套件用例数一律由 `all/0`（含 `groups/0`）实数回填，不预先承诺
  - 本阶段**不新增任何运行时依赖**；`broker_ws` 是纯 BEAM 库
  - 不杀用户进程或容器（`18831`/`18841`/`18081` 被占用），CT 一律用 `port => 0`

---

## Task 1 — 抽取 `broker_ws`（纯 RFC 6455 库）

**目标**：把 `ws_frame` 搬成独立无进程库 app，并新增 `ws_handshake`；`broker_proto_ws` 改为引用同一套代码，对端字节与行为不变。

**1a. 新建 app 骨架**

```
apps/broker_ws/
├── rebar.config                     # {project_app_dirs, [...]} 不需要；直接 {erl_first, ...} 见说明
├── src/broker_ws.app.src
├── src/ws_frame.erl                 # 从 apps/broker_proto_ws/src/ws_frame.erl 原样搬移
├── src/ws_handshake.erl             # 新增
├── test/ws_frame_SUITE.erl          # 从 apps/broker_proto_ws/test/ 原样搬移
└── test/ws_handshake_SUITE.erl      # 新增
```

`apps/broker_ws/src/broker_ws.app.src`：

```erlang
{application, broker_ws, [
    {description, "Pure RFC 6455: frame codec and handshake, no broker dependency"},
    {vsn, "0.1.0"},
    {registered, []},
    {mod, {broker_ws_app, []}},
    {applications, [kernel, stdlib, crypto, public_key]},
    {env, []}
]}.
```

`apps/broker_ws/src/broker_ws_app.erl`：

```erlang
-module(broker_ws_app).
-behaviour(application).
-export([start/2, stop/1]).
start(_Type, _Args) -> {ok, self()}.
stop(_State) -> ok.
```

**为什么需要 `broker_ws_app`**：`rebar.config` 的 `{app_dirs, [...]}` / `relx` 会按 `src/*.app.src` 生成 `.app` 并放进 `ebin`，但没有 `mod` 也可以。仓库既有约定（`broker_ws` 与 `broker_proto_ws` 一致）是给出一个无监督树的 application 模块，`application:ensure_all_started/1` 时它是空操作。若 Task 1 的实测显示不需要 `mod`，就删掉 `broker_ws_app` 并从 `.app.src` 移除 `{mod, ...}`，**不要为凑形式保留死代码**。

**1b. 搬移 `ws_frame`**

```
git mv apps/broker_proto_ws/src/ws_frame.erl apps/broker_ws/src/ws_frame.erl
git mv apps/broker_proto_ws/test/ws_frame_SUITE.erl apps/broker_ws/test/ws_frame_SUITE.erl
```

模块内容**零改动**（含 `-export([decode/2, encode/2, encode/3, close_payload/2, close_code/1])` 与 `-export_type([frame/0])`）。

**1c. 新增 `ws_handshake`**

从 `apps/broker_proto_ws/src/ws_conn.erl` 中**按现有实现等价搬迁**（不是重写）：`validate_upgrade/1`、`headers/1`、`lower/1`、`trim/1`、`?GUID`、`?MAX_HANDSHAKE`、`accept/1`。

新增 API（设计 §7.1 要求的两条，本阶段先实现签名与行为，B 阶段才会用）：

```erlang
-export([accept/1, accept/2]).

%% 未配置 supported_subprotocols 时：只计算 Accept、不回发 Sec-WebSocket-Protocol。
-spec accept(binary()) -> {ok, binary()} | {error, term()}.
accept(Req) -> accept(Req, #{}).

%% 子协议选择：客户端请求的子协议与本地配置按序匹配；不匹配或缺失 -> {error, no_subprotocol}
-spec accept(binary(), map()) -> {ok, binary(), binary()} | {error, term()}.
%%   返回 {Response, SelectedSubprotocol}（SelectedSubprotocol 为 binary() 或 undefined）
```

**`accept/2` 的语义约束（B 阶段依赖，本阶段写测试）**：

| 条件 | 结果 |
|---|---|
| `Upgrade` 不是 `websocket` | `{error, not_websocket}` |
| `Connection` 不含 `upgrade` token | `{error, not_upgrade}` |
| 缺 `Sec-WebSocket-Key` | `{error, missing_key}` |
| 请求头超过 `?MAX_HANDSHAKE` | `{error, headers_too_large}`（调用方负责回 431） |
| 配置了 `supported_subprotocols` 且客户端未提供 | `{error, no_subprotocol}` |
| 配置了 `supported_subprotocols` 但无交集 | `{error, no_subprotocol}` |
| 未配置 `supported_subprotocols` | 正常返回 101 响应，**不**写 `Sec-WebSocket-Protocol` 头 |
| 配置了 `supported_subprotocols` 且有交集 | 在 101 响应中写第一个交集子协议 |

**关键约束**：`broker_proto_ws` 现有的握手路径**不得**因为引入 `ws_handshake` 而改变字节输出。因此 1c 的第一步是把 `ws_conn` 的内联实现**替换**为 `ws_handshake:accept/1`，并加一条「字节完全一致」的对照断言（见 1e），而不是新增一条并行路径。

**1d. `broker_proto_ws` 改引用**

- `apps/broker_proto_ws/src/broker_proto_ws.app.src` 的 `applications` 加入 `broker_ws`
- `ws_conn.erl`：`ws_frame:...` 调用点保持不变（同名模块），握手部分改为调用 `ws_handshake:accept/1`（无第二参数）
- `rebar.config` 的 `relx.release` 列表加入 `broker_ws`（位置在 `broker_proto_ws` 之前）

**1e. 新增 `ws_handshake_SUITE`**

覆盖 1c 表中全部 7 行 + 大小写不敏感的 `Sec-Websocket-Key` + `Connection: keep-alive, Upgrade` 多 token。

**1f. 验证**

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
rebar3 compile
rm -rf _build/test/extras
rebar3 ct --suite=apps/broker_ws/test/ws_frame_SUITE.erl
rebar3 ct --suite=apps/broker_ws/test/ws_handshake_SUITE.erl
rebar3 ct --suite=apps/broker_proto_ws/test/ws_conn_SUITE.erl
```

`ws_frame_SUITE` 必须保持**原样 9 例**（注意：设计文档 §9.1 写的「原有 22 例」是笔误，22 是 `ws_conn_SUITE` 的例数；以 `all/0` 实数为准）。`ws_conn_SUITE` 不得减少任何一例。

**1g. 提交**

```bash
git commit -m "refactor(s8): extract broker_ws RFC 6455 library"
```

---

## Task 2 — `mqtt_transport` 行为 + tcp/ssl 实现 + MQTT FSM 改造

**目标**：把 `mqtt_conn_fsm` 里 5 处硬编码 socket 调用换成传输行为，使 TCP 与 SSL 走同一代码路径。

**2a. 新增行为与实现**

`apps/broker_proto_mqtt/src/mqtt_transport.erl`：

```erlang
-module(mqtt_transport).
-type handle() :: term().
-export_type([handle/0, decision/0]).

%% 归一化邮箱消息
-callback decode_msg(term(), handle()) -> decision().
-type decision() :: {data, binary()} | closed | {error, term()} | other.

-callback send(handle(), iodata()) -> ok | {error, term()}.
-callback arm_once(handle()) -> ok | {error, term()}.
-callback close(handle()) -> ok.
```

`apps/broker_proto_mqtt/src/mqtt_transport_tcp.erl`（**同时覆盖 ranch_tcp 与 ranch_ssl**）：

```erlang
-module(mqtt_transport_tcp).
-behaviour(mqtt_transport).
-export([send/2, arm_once/1, close/1, decode_msg/2]).

send(S, IoData) -> gen_tcp:send(S, IoData).
arm_once(S) -> inet:setopts(S, [{active, once}]).
close(S) -> gen_tcp:close(S).

decode_msg({tcp, S, D}, S) when is_binary(D) -> {data, D};
decode_msg({ssl, S, D}, S) when is_binary(D) -> {data, D};
decode_msg({tcp_closed, S}, S) -> closed;
decode_msg({ssl_closed, S}, S) -> closed;
decode_msg({tcp_error, S, R}, S) -> {error, R};
decode_msg({ssl_error, S, R}, S) -> {error, R};
decode_msg(_, _) -> other.
```

**设计 §4.1 与实现的偏差说明**：设计写的是 `arm_once/2`。实现用 `arm_once/1`，因为 handle 已经携带了 socket，多传一个 `Sock` 参数是冗余的（且 FSM 的 `#st.socket` 同时是 transport 的 handle）。**这是有意的一参数简化**，在本任务提交信息与设计文档 §4.1 都注明。

**2b. `mqtt_conn_fsm` 改造（严格 5 处 + 1 条事件子句）**

`#st{}` 增加字段：

```erlang
transport = mqtt_transport_tcp :: module(),
```

改造点：

| 位置 | 原 | 改 |
|---|---|---|
| `send_frame/2`（L2138 附近） | `gen_tcp:send(Sock, Bin)` | `(D#st.transport):send(Sock, Bin)` |
| `arm_socket/2`（L322） | `inet:setopts(Sock, [{active, once}])` | `(D#st.transport):arm_once(Sock)` |
| `active_once/1`（L328） | `inet:setopts(Sock, [{active, once}])` | 通过 `D#st.transport` —— 因此 `active_once/1` 改为 `active_once(#st{})` 并从 2 处调用点传 `D` |
| `terminate/3`（L2266） | `gen_tcp:close(D#st.socket)` | `(D#st.transport):close(D#st.socket)` |
| 事件入口（L119/L146） | `handle_event(_Et, {tcp_closed, _}, ...)` 与 `event({tcp, ...})` | 改为调 `(D#st.transport):decode_msg/2` 分派，见下 |

**事件分派改造（L119/L146 的具体写法）**：

保留 `handle_event/4` 的 5 个 `{tcp_closed,_}`/`{tcp_error,_,_}` 直连子句（它们在任何状态都合法，先于 `event/3` 匹配），把 L146 的 `event({tcp, Sock, Data}, ...)` 改为：

```erlang
event(Msg, StateName, #st{transport = T, socket = Sock, buf = Buf} = D0) ->
    case T:decode_msg(Msg, Sock) of
        {data, Data} ->
            broker_metrics:inc(bytes_in, byte_size(Data)),
            D = D0#st{buf = <<Buf/binary, Data/binary>>},
            case tcp_frames(D, StateName) of
                {state, S, D1} -> active_once(D1), {state, S, ka_arm(S, D1)};
                {stop, Reason, D1} -> {stop, Reason, D1}
            end;
        closed -> {stop, normal, D0};
        {error, Reason} -> {stop, normal, D0#st{last_error = Reason}};   %% 不新增字段则直接 log
        other -> {keep_state, D0}
    end.
```

**注意**：`{data, Data}` 分支里 `active_once/1` 的调用点必须传 `D1`（改造后的 `active_once/1` 接受 `#st{}`）。`broker_metrics:inc(bytes_in, ...)` 的语义不变。

**既有测试助手无需改动**：`mqtt_client.hrl` 直接传 socket 并 `! connect`，`transport` 默认 `mqtt_transport_tcp`，`decode_msg({tcp, S, D}, S)` 返回 `{data, D}` —— 与原 `event({tcp, Sock, Data}, ...)` 行为完全等价。

**2c. `mqtt_listener` 注入 transport**

`apps/broker_proto_mqtt/src/mqtt_listener.erl` 的 `start_link/3` 目前忽略 `Transport`。改为从 `Opts` 读 `transport`（默认 `mqtt_transport_tcp`），传给 `mqtt_conn_sup:start_conn/2`（它已经把 `Opts` 透传到 FSM 的 `#st.opts`，因此 FSM 从 `Opts` 取或直接由 listener 传）。

**推荐做法**：`mqtt_conn_sup:start_conn/2` 的签名不动（避免动测试助手），由 `mqtt_listener` 把 `transport` 键并进 `Opts` 里传给 `start_conn/2`；`mqtt_conn_fsm:init/1` 从 `Opts` 取出 `transport`，默认 `mqtt_transport_tcp`，写入 `#st.transport`。

`broker_proto_mqtt_sup:listener_spec/1` 无需改动（它交给 ranch 的 `Transport` 是 `ranch_tcp`/`ranch_ssl`；FSM 只关心 handle 类型，两者都由 `mqtt_transport_tcp` 覆盖）。

**2d. 新增 `mqtt_transport_tcp_SUITE`**

覆盖 `decode_msg/2` 的全部 8 个分支（含 `{ssl,...}` 四元组）+ `arm_once`/`send`/`close` 在真实 `gen_tcp` socket 上的往返。

**2e. 新增 `mqtt_tls_SUITE`（设计 §2 最后一行 + §9.1 的回归）**

复用 `broker_security_tls_SUITE` 已验证的 openssl 自签手法（`write_self_signed/2`，`-days 1 -subj /CN=localhost`）。本套件断言**真实的 MQTT 帧在 TLS 上跑通**，而不是只断言 listener 可建：

| 用例 | 断言 |
|---|---|
| `tls_listener_starts_with_mqtt` | `broker_security.tls` enabled + cert/key 有效时，listener 的 socket 是 `{sslsocket, ...}`（`ranch_ssl`），且 FSM 的 `arm_once` 成功 |
| `connect_over_tls_completes_the_handshake` | 用 `ssl:connect/4` + `mqtt_frame:encode(#connect{})` 发 CONNECT，收到合法 CONNACK |
| `publish_over_tls_roundtrip` | TLS 上一个 QoS1 pub/sub 往返（客户端用 `ssl:send/recv`，FSM 侧走 `mqtt_transport_tcp`） |
| `tls_disabled_is_plain_tcp` | `tls.enabled => false` 时 `transport() =:= {ranch_tcp, []}`（回归 `broker_security_tls_SUITE` 已有断言，这里只补 listener 侧的 `gen_tcp` 端口） |

**套件骨架参照** `test/integration/s1_e2e_SUITE.erl`（`init_per_suite` 逐 app `application:load`、`stop_all/0` 顺序、`init_per_testcase` 逐用例写全 env、`port => 0`）。数据目录在 `/tmp/opencode` 下，**不在 release 树内**。

**2f. 验证**

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
rebar3 compile
rm -rf _build/test/extras
rebar3 ct --suite=apps/broker_proto_mqtt/test/mqtt_transport_tcp_SUITE.erl
rebar3 ct --suite=apps/broker_proto_mqtt/test/mqtt_tls_SUITE.erl
rebar3 ct      # 全量回归，基线 673 例零回归
```

`mqtt_conn_SUITE` 一个用例都不能少。

**2g. 提交**

```bash
git commit -m "refactor(s8): mqtt_transport behaviour, fix MQTT over TLS"
```

---

## Task 3 — 全量验证与文档同步

**3a. 门禁**

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
rm -rf _build/test/extras
rebar3 ct
rebar3 dialyzer
rebar3 xref          # 仅允许 is-unused-export
rebar3 as prod release
```

**3b. 回填用例数（强制）**

按以下顺序取实数并写进 `progress.md`、`docs/roadmap.md` §S8 的「已交付」行：

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
erl -noshell -pa _build/test/lib/*/ebin -eval '
  Suites = [ws_frame_SUITE, ws_handshake_SUITE, ws_conn_SUITE,
            mqtt_transport_tcp_SUITE, mqtt_tls_SUITE],
  [io:format("~s ~p~n", [S, length(S:all())]) || S <- Suites],
  halt().'
```

`rebar3 ct` 通过的总例数 = 673 + 本阶段新增，两者都要写（区分「基线」与「S8-A 新增」）。

**3c. 文档同步（本阶段只动 3 个文件，其余留到 B/C/D 结案时统一写）**

| 文件 | 变更 |
|---|---|
| `docs/architecture.md` | app 列表加 `broker_ws`；传输抽象段加 `mqtt_transport` 行为与三实现；端口表加 `broker_ws` 无监听器说明 |
| `docs/operations.md` | 依赖图新增 `broker_ws`；说明 `broker_ws` 无运行时配置 |
| `progress.md` | S8 进展 + 本阶段用例数 + 下一步（B：MQTT over WS） |

`docs/roadmap.md` / `docs/protocol-compatibility.md` / `docs/security.md` / `README.md` / ADR **本阶段不动**，因为这三项内容要到 B/C/D 全部结案时才能一次写全、避免反复改写。

**3d. 提交**

```bash
git commit -m "docs(s8-a): record broker_ws and mqtt_transport in architecture and progress"
```

---

## 完成判据（Definition of Done）

- [ ] `broker_ws` app 建立，`ws_frame` 原样迁入，`ws_frame_SUITE` 9 例零回归
- [ ] `ws_handshake:accept/1,2` 实现并通过 `ws_handshake_SUITE`（7 行语义 + 大小写 + 多 token）
- [ ] `broker_proto_ws` 握手字节与改造前一致（`ws_conn_SUITE` 一例不少）
- [ ] `mqtt_transport` 行为落地，`mqtt_conn_fsm` 5 处 + 1 条事件路径改造完成
- [ ] `mqtt_tls_SUITE` 通过：TLS 上的 MQTT 握手与 QoS1 往返**真实成功**（修复设计 §2 指出的既有缺陷）
- [ ] `rebar3 ct` 总例数 = 673 + 新增，零 FAILED，零新增 Skipped
- [ ] dialyzer / xref 干净，release 装配成功
- [ ] `docs/architecture.md` / `operations.md` / `progress.md` 同步，用例数回填

## 不在本阶段（明确留给后续阶段）

| 项 | 阶段 |
|---|---|
| `broker_proto_mqtt_ws`（MQTT over WS 监听器、`mqtt_transport_ws`、paho 客户端证据） | **S8-B** |
| `broker_proto_mqtt_sn`（codec / topics / conn / listener、mqtt-sn-tools 证据） | **S8-C** |
| `broker_proto_mqtt_quic`（listener / transport、profile 门控、emqtt 证据） | **S8-D** |
| `docs/roadmap.md` §S8 正式条目、`protocol-compatibility.md` §4.11~§4.13、ADR-022/023/024 | **B/C/D 全部结案后的统一收尾** |
| MQTT `require_auth` 接线 | 不在本阶段（仍 `docs/security.md` §12） |
