# S8-D 执行计划 — MQTT over QUIC（`broker_proto_mqtt_quic`）

- **前置**：S8-A（`mqtt_transport` 行为）、S8-B（WS 前端）、S8-C（MQTT-SN）已结案；
  设计见 [`s8-mqtt-sn-mqtt-over-quic-and-mqtt-over-ws.md`](s8-mqtt-sn-mqtt-over-quic-and-mqtt-over-ws.md) §6。
- **规范基线**：OASIS MQTT TC《MQTT over QUIC — Single Stream Mode》Committee Note Draft 3。
- **执行方式**：inline 逐 Task，每个 Task 一次提交（与 S8-A/B/C 相同）。
- **用例数**：所有"N 例"以 `all/0` 实数回填，不预先承诺。

---

## 关键决定（写代码前先定）

### 决定 1：`quicer` 是 **profile 门控**依赖，默认构建纯 BEAM

`broker_proto_mqtt_quic` 的源码目录在 `apps/` 内，因此**默认 `rebar3 compile` 也会编它**。
所以：

- 它只通过**裸消息元组**（`{quic, Bin, Stream, Props}`）与 quicer 交互，**不 `-include` 任何
  quicer 头文件**，也不引用 quicer 的 record/type；
- 所有 quicer 调用经一个薄壳 `mqtt_quic_nif`（`apply` + `code:ensure_loaded/1` 前置检查），
  因此默认 profile 下 `xref`/`dialyzer` 都不会报「调用不存在的模块」；
- `quicer` 写进 `broker_proto_mqtt_quic.app.src` 的 `optional_applications`，不进 `applications`
  ——否则 `application:ensure_all_started` 会在没有 quicer 的节点上失败；
- `quicer` 只在 `quic` profile 的 `deps` 里，`broker_proto_mqtt_quic` 只在 `quic` profile 的
  `relx` 里。

默认 `rebar3 ct` 的用例数口径因此完全不受影响：真实 QUIC 组按
`code:ensure_loaded(quicer)` 分组，缺 quicer 时**该组不展开**（不是 skip，是 `all/0` 里根本
没有它），降级组照常跑。

### 决定 2：一条客户端发起的双向流承载一条 MQTT Network Connection

CN Draft 3 §2.2：MQTT 报文**零改动**，全部报文走**一条**客户端发起的 bidi 流。因此：

- 同一时刻一条 QUIC 连接上只有**一条**活动 MQTT 流；服务端 `peer_unidi_stream_count => 0`，
  不接受服务端发起的流，也不做多流（EMQX 私有扩展）、不做 0-RTT early data（CN 可选，本轮不接收）；
- 流关闭（`{quic, peer_send_shutdown, Stream, _}` / `{quic, stream_closed, ...}`）**不等于**
  连接关闭：连接留在 accepted 状态，客户端要重连就在**新流**上发 CONNECT；
- 流信用在**握手时一次性授予**（`max_streams_per_conn`，默认 4），而不是「旧流关闭后再补 1 条」。

> **实测依据（推翻了初稿的「关流后补 1 条」）**：quicer 0.4.12 / msquic 2.5.11 下，运行期
> `quicer:setopt(Conn, settings, #{peer_bidi_stream_count => N})` 返回 `ok`，但新的 MAX_STREAMS
> 帧**不会**让客户端的新流到达服务端（`/tmp/opencode/qflow6.erl` 复现：listen 时授 4 条 → 4 条
> 顺序到达；握手后追加 → 永不到达）。既然信用只能在建连接时给，就按「一次给够」实现：4 是
> 重连配额而不是并发目标，客户端仍按 Single Stream Mode 只用一条活动流。

### 决定 3：QUIC 就是一条「有边界的字节通道」，MQTT 引擎零改动

`mqtt_transport_quic` 实现 `mqtt_transport` 的四个回调，`mqtt_conn_fsm` 完全复用
（与 1883 / wss 是同一份代码）：

| 回调 | QUIC 实现 |
|---|---|
| `send(Bin, Stream)` | `quicer:send(Stream, Bin)` |
| `arm_once(Stream)` | `quicer:setopt(Stream, active, once)`（**原子 `once`**，不是整数 `1`，见下） |
| `close(Stream)` | `quicer:close_stream(Stream)`，失败忽略 |
| `decode_msg(Msg, Stream)` | 按 `is_binary/1` 分派：`{quic, Bin, Stream, _}` → `{data, Bin}`；`{quic, closed\|peer_send_shutdown\|stream_closed, Stream, _}` → `closed`；`{quic, Event, Stream, _}`（其他 atom 事件）→ `other`；别的进程/别的 stream 的消息 → `other` |

**`is_binary/1` 是必须的分派依据**：quicer 的数据与事件进**同一个邮箱**，两者都是
`{quic, X, Stream, Props}`，只有 `X` 的类型能区分。把 `{quic, send_shutdown_complete, ...}`
当成数据喂给 `mqtt_frame` 就是「一半的包被当成协议违规」。

> **实测偏差（`active, once` vs `active, 1`）**：quicer 对整数 active 值走 `{active, N}`
> （multi 模式）：收满 N 条后它会向 owner 发 `{quic, passive, Stream, undefined}` 并把流切回
> passive。初稿写 `active, 1`，于是**每读一个 MQTT 报文就多一个 FSM 不认识的 atom 事件** ——
> `mqtt_conn_fsm` 记一条 `unexpected event in connected` 并把 `protocol_errors` +1，属于
> 「传输层细节泄漏成协议错误」。改用原子 `once`（`ACCEPTOR_RECV_MODE_ONCE`）后同样是一读一
> arm，但不发 passive 事件。回归断言：
> `mqtt_quic_listener_SUITE:a_pingreq_is_not_a_protocol_error` 检查 `protocol_errors` 在
> CONNECT+PINGREQ 前后不变 —— 报文本身在坏版本里也照常回，所以只能对计数器断言。

### 决定 4：QUIC 强制 TLS 1.3，无证书就**拒绝启动**这个 listener

QUIC 的握手就是 TLS 1.3，没有明文模式可退。因此：

- `certfile`/`keyfile` 未在 listener map 里给出时，取 `broker_security.tls` 的配置；
- 两者仍为 `undefined` → 该 listener 的 `init/1` 返回
  `{stop, {quic_requires_tls, CertKey}}`，**其它协议 listener 不受影响**（QUIC 是独立 app）；
- 这是唯一一个「TLS 不由 `tls.enabled` 开关决定、而是由证书存在与否决定」的 listener。
  运维文档必须把这条写清楚：把 `tls.enabled` 关掉不会让 QUIC 变成明文，它只会让 QUIC 起不来
  （如果它还从 `broker_security.tls` 取证书的话）。

### 决定 5：ALPN 必须是 `mqtt`

`quicer:listen/2` 的 `alpn` 配成 `["mqtt"]`（CN Draft 3 §2.1 规定的唯一值）。客户端不提供
`mqtt` 时握手失败——这是唯一正确的行为，静默接受一个别的 ALPN 等于让客户端以为自己在说
另一种协议。

---

## Task 1 — `mqtt_transport_quic` + 套件（纯函数层，不需要真 QUIC）

**1a.** `apps/broker_proto_mqtt_quic/src/mqtt_transport_quic.erl`：四个回调 + `decode_msg/2`
的完整分派（决定 3 的表）。`decode_msg/2` 是纯函数，可以在没有 quicer 的节点上直接测。

**1b.** `mqtt_quic_nif.erl`：所有 `quicer:` 调用的唯一入口（`ensure/0` + `apply/3` 薄壳），
默认 profile 下 `code:ensure_loaded(quicer)` 返回 `{error, nofile}`，调用方拿到
`{error, quicer_not_available}` 而不是 `undef` 崩溃。

**1c.** `mqtt_quic_transport_SUITE`：

- 数据/事件分派的每一条分支（`{quic, Bin, S, _}`、`stream_closed`、`peer_send_shutdown`、
  `closed`、`send_shutdown_complete`、别的 stream 的消息、别的进程的消息）；
- `send`/`arm_once`/`close` 在 quicer 缺失时返回明确错误而不是崩溃；
- `decode_msg/2` 对 handle 的匹配（A 流的事件不会喂进 B 流）。

**1d. 提交**：`feat(s8-d): mqtt_transport_quic, the QUIC transport for the MQTT engine`

---

## Task 2 — `mqtt_quic_listener` + app 骨架 + profile

**2a.** app 骨架：`broker_proto_mqtt_quic.app.src`（`optional_applications = [quicer]`、
`applications` 含 `broker_proto_mqtt`）、`_app`、`_sup`（`one_for_all, 1/5s`）。

**2b.** `mqtt_quic_listener`（gen_server）：

- `init/1`：证书校验（决定 4）→ `quicer:listen(Port, #{alpn => ["mqtt"], cert, key, ...})`
  → 起 acceptor（`spawn_link` 一条 accept 循环，或 `quicer:accept` 回调）；
- 每个 accepted connection：`quicer:handshake/2`（**必须显式**，缺它客户端 `connect` 永挂起）
  → `quicer:accept_stream/3` 拿客户端发起的那一条 bidi 流 → 起 `mqtt_conn_fsm`
  （`transport => mqtt_transport_quic`，handle = Stream）；
- 握手超时（`handshake_timeout_ms`，默认 5000）→ 关连接、记日志、**不影响其它连接**；
- `max_connections` 上限；`idle_timeout_ms` 透传给 quicer 的 `idle_timeout_ms`；
- 流关闭 → FSM 收到 `closed` 后自行结束，listener 只清理自己的表。

**2c.** `config/sys.config.src`：`{broker_proto_mqtt_quic, [{listeners, []}, ...]}`，示例
listener 写 `#{port => 14567}`（14567 用 EMQX 的 QUIC 默认端口便于对照）并注释掉。

> **与初稿的偏差（照实记录）**：初稿是「默认就开 14567」。实测发现，默认 `broker_security.tls`
> 是 `enabled => false` / `certfile => undefined`，而 QUIC listener 无证书必然拒绝启动、拒绝
> 启动又是该 app 的启动失败——于是 `rebar3 as quic release` 装出来的 release **开不起来**。
> 因此默认 `listeners => []`（app 照常启动，只是不开监听），配置注释里写清「先配好 TLS 再打开」。

**2d.** `rebar.config`：`quic` profile —— `{deps, [{quicer, "0.4.12"}]}` +
`relx release` 追加 `broker_proto_mqtt_quic`。默认 profile 不变。

**2e.** `mqtt_quic_listener_SUITE`（默认 profile 下跑的组）：无证书拒绝启动
（`{quic_requires_tls, _}`）、quicer 缺失时的明确错误、配置校验、ALPN 配置项存在。

**2f. 提交**：`feat(s8-d): MQTT over QUIC listener (broker_proto_mqtt_quic)`

---

## Task 3 — 真实 QUIC 证据（`emqtt` 客户端）

`test/protocol-compatibility/mqtt_quic/`（boot 助手 + 驱动 + README，`run/`/`data/`/`_build/` gitignore）。

客户端：**`emqtt`**（EMQX 官方 Erlang MQTT 客户端，`src/emqtt_quic.erl` 走 `quicer` + ALPN
`mqtt` + 单流 `active=1`）——第三方、非本仓库代码。

| 用例 | 说明 |
|---|---|
| `connect` | CONNECT/CONNACK over QUIC + PINGREQ/PINGRESP |
| `qos0` / `qos1` / `qos2` | 上下行三档 |
| `retain` | retained 投递 |
| `will` | 未发 DISCONNECT 就断连后的 Will |
| `mqtt5` | 5.0 properties 同一条流 |
| `reconnect` | 持久会话（`clean_start => false` + 同 client id）由**第二个连接**续上 |

**流关闭 ≠ 连接关闭** 的「同连接新流」断言放在 CT（
`mqtt_quic_listener_SUITE:a_new_stream_on_the_same_connection_is_a_new_session`）：
`emqtt` 的公开 API 没有「只关流」的入口（`close/1` 会连连接一起关），把这条硬塞进第三方
驱动只会得到一个测不出结论的用例。

**明确标注**：`emqtt` 的 QUIC 支持是 EMQX 侧的实验特性，覆盖不到的部分（0-RTT、多流、
流级关闭）本仓库本就不做或另有断言，Gate 文本如实写。

---

## Task 4 — 全量验证与文档同步

`rebar3 ct`（默认 profile，基线 815 + Task 1/2 新增，不得回归）、`rebar3 as quic ct`（含真实
QUIC 组）、`dialyzer`、`xref`、`rebar3 release` 与 `rebar3 as quic release`；同步 `progress.md`、
`docs/architecture.md`（app 列表/表格/§3.12）、`docs/operations.md`（UDP 14567 + 证书要求 + 构建
前提）、`docs/protocol-compatibility.md` §4.12、`docs/security.md`（QUIC 强制 TLS 1.3）。

**已回填（2026-11-11）**：默认 `rebar3 ct` **823/823、52 套件**（零 FAILED、零 Skipped；`broker_proto_mqtt_quic` 17 例 = config 4 + 传输契约 13）；`rebar3 as quic ct` **827 passed + 2 skipped**（该套件 23 例，多出 6 例真实 QUIC，两个「quicer 缺失」用例按设计 skip），零 FAILED。

---

## 完成判据（Definition of Done）

- [ ] `broker_proto_mqtt_quic` 默认 profile 可编译、可 dialyzer、可 xref（不 include quicer 头、不进 `applications`）
- [ ] `quic` profile 下 `rebar3 as quic ct` 真实 QUIC 组全绿（NIF 加载 + 握手 + 单流）
- [ ] ALPN 必须是 `mqtt`；非 `mqtt` 的客户端握手失败
- [ ] 无证书时该 listener 拒绝启动 `{quic_requires_tls, _}`，其它协议不受影响
- [ ] 流关闭 ≠ 连接关闭（新流重连有断言）
- [ ] `emqtt` 真实客户端用例全绿
- [ ] 默认 `rebar3 ct` 零 FAILED 且用例数按 `all/0` 回填；dialyzer / xref 干净
- [ ] `rebar3 release` 与 `rebar3 as quic release` 均装配成功
- [ ] 四份文档同步，`docs/roadmap.md`/ADR 统一留到 B/C/D 全部结案

## 不在本阶段（明确留给后续）

| 项 | 阶段 |
|---|---|
| QUIC 多流模式（EMQX 私有扩展） | 不做（设计 §1.2） |
| QUIC 0-RTT early data | 不做（CN 中为可选） |
| 跨 QUIC 重连的会话状态保持盲区压测 | 不做（EMQX 自身亦未支持） |
| `docs/roadmap.md` Gate S8 勾选、ADR-022/023/024 | B/C/D 全部结案后一次写全 |
