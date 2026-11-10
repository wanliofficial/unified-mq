# ADR-024：MQTT over QUIC——连接/流两层、profile 门控依赖与流信用

- **状态**：已采纳（S8-D 冻结，2026-11-11）
- **日期**：2026-11-11
- **关联**：OASIS MQTT TC《MQTT over QUIC — Single Stream Mode》Committee Note Draft 3（§2.1 单流与 ALPN、§2.2 流与连接的关系）；ADR-001（协议适配与内核分离）、ADR-022（MQTT-SN 单 socket）、ADR-023（WS 两进程）
- **关联文档**：[`docs/design/s8-d-mqtt-over-quic-execution-plan.md`](../design/s8-d-mqtt-over-quic-execution-plan.md)、[`docs/architecture.md`](../architecture.md) §3.12、[`docs/operations.md`](../operations.md) §1.2

## 背景（Context）

S8-D 要在既有 MQTT 引擎之上补 MQTT over QUIC。规范（CN Draft 3）的核心只有一句：
**MQTT 报文一个字节不改**，整条 MQTT Network Connection 走**一条客户端发起的双向流**。

所以这不是「新协议」，也不是「新引擎」：`mqtt_conn_fsm` 与 1883 / 8083 是同一份代码，
`mqtt_transport_quic` 是第三个传输实现（TCP/TLS → WebSocket → QUIC）。

真正需要决策的是三件事：

1. **谁拥有 QUIC 连接**——QUIC 的流会关，连接不会（CN Draft 3 §2.1）；
2. **quicer（msquic + quictls 的 NIF）怎么进构建**——它需要 C 工具链、编 10~15 分钟，
   且不是所有部署都需要 QUIC；
3. **流信用怎么给**——quicer 0.4.12 的行为与「按需补充」的直觉相反。

## 决策（Decision）

### D1. 比 TCP 多一层进程：连接在流之间存活

```
broker_proto_mqtt_quic_sup (one_for_all, 1/5s)
└── mqtt_quic_listener × N        % 每个配置的 listener 一个；拥有 quicer listener 句柄
      ├── acceptor 进程           % quicer:accept/3 循环；把每条连接交接出去
      └── mqtt_quic_conn × N      % 每条 QUIC 连接一个；拥有连接句柄，循环处理新流
            └── mqtt_conn_fsm     % 每条流一个（mqtt_conn_sup 的子进程），handle = stream
```

理由：客户端关掉一条流只结束这条 MQTT 连接（发布 Will、清理会话），它可以在**同一条
QUIC 连接**上新开一条流重新 CONNECT。所以两次流之间必须有个进程活着，拥有连接句柄。

`mqtt_quic_conn` 的职责只有「接受新流并交接」，**不读 MQTT 字节**：流的 handle 交给
`mqtt_conn_fsm`（经 `mqtt_conn_sup`），FSM 成为流的 owner 并直接接收
`{quic, Bin, Stream, Props}`——与 TCP 的「FSM 拥有 socket」、WS 的「FSM 拥有 pid」
是同一种所有权形状。

交接顺序是「先移所有权、再通知」：`controlling_process/2` 之后才 `Pid ! {quic_conn, Conn}`。
与 S8-A 的 `{handshake, ...}` 同理——一句 `Pid ! connect` 之前，FSM 不许碰它不拥有的句柄。

### D2. 服务端必须自己完成 TLS 握手

`quicer:accept/3` 只报告**传输层**连接；`quicer:handshake/2` 才真正跑 TLS。而
`{quic, connected, ...}` 投给连接的 **owner**——所以握手只能在 `mqtt_quic_conn` 拿到
句柄之后做。

握手失败（ALPN 不匹配、明文探测、客户端中途走掉）是常态：进程以 `normal` 停止并记一条
info，不占用监督树的重启强度（`one_for_all, 1/5s`）。

### D3. ALPN 固定 `mqtt`，不静默降级

CN Draft 3 §2.1 只定义一个 ALPN 值：`mqtt`。客户端不带它就**握手失败**
（实测 quicer 返回 `{error, transport_down, #{status := alpn_neg_failure}}`）。

静默接受一个别的 ALPN 等于让客户端以为自己在说另一种协议——比失败更糟。

### D4. 强制 TLS 1.3，无证书即拒绝启动

QUIC 的握手**就是** TLS 1.3，没有明文模式可退。因此证书来源与其它 listener 不同：

1. listener map 自带 `certfile`/`keyfile`；
2. 否则取 `broker_security.tls`；
3. 两者都没有 → `{stop, {quic_requires_tls, certfile | keyfile}}`，**只让这一个 app 起不来**。

`broker_security.tls.enabled` 对 QUIC 不是「开关」而是「证书来源」：关掉它不会让 14567
变明文，只会让 listener 找不到证书而拒绝启动。这是唯一一个这样的 listener，运维文档必须
写明。

### D5. quicer 是 profile 门控依赖，不是默认依赖

- 默认 profile（`rebar3 compile/ct/release`）**完全不拉** `quicer`：`broker_proto_mqtt_quic`
  照常编译、照常启动，只是 `listeners => []`；手工开 listener 会得到
  `{error, quicer_not_available}`（`optional_applications => [quicer]`）。
- `quic` profile 才把 `quicer` 放进 `deps` 与 `relx`。
- **`quic` profile 的 `relx` 必须复制完整 app 列表**：profile 的 `relx` 会**替换**顶层那份，
  漏掉就等于装出一个只有 QUIC 的 release。
- 所有 `quicer:` 调用集中在 `mqtt_quic_nif` 一个模块；本 app **不 include quicer 头**，
  只通过裸消息元组（`{quic, Bin, Stream, Props}`）与它交互。

### D6. 流信用在握手时一次性授予

`peer_bidi_stream_count => max_streams_per_conn`（默认 4）在 `quicer:listen/2` 时给足；
`peer_unidi_stream_count => 0`（服务端不发流）。

本想「关一条补一条」，但实测：

- quicer 0.4.12 / msquic 2.5.11 下，运行期 `setopt(Conn, settings, #{peer_bidi_stream_count => N})`
  返回 `ok`，但新的 `MAX_STREAMS` 帧**不会**让客户端的新流到达；
- listen 时授 4 → 4 条顺序到达；握手后追加 → 永不到达。

因此 4 是**重连配额**而不是并发目标：Single Stream Mode 同一时刻只有一条活动 MQTT 流。
`{quic, peer_needs_streams, ...}` 只记一条 warning，不假装能补。

### D7. 传输层细节不得泄漏成协议错误

`arm_once/1` 必须传**原子 `once`**，不能传整数 `1`：

- quicer 把整数 active 值当 `{active, N}`（multi 模式）：收满 N 条后向 owner 发
  `{quic, passive, Stream, undefined}` 并把流切回 passive；
- `mqtt_conn_fsm` 不认识这个消息，于是**每读一个报文记一条 `unexpected event` 并
  `protocol_errors` +1**；
- 原子 `once`（`ACCEPTOR_RECV_MODE_ONCE`）是一读一 arm，且不发 passive 事件。

这条是实测抓到的真实缺陷，不是理论风险。回归断言见下。

### D8. `decode_msg/2` 按 `is_binary/1` 分派

quicer 把 payload 与事件塞进**同一个** `{quic, X, Stream, Props}` 形状，只有 `X` 的类型
能区分：

- `{quic, Bin, Stream, _} when is_binary(Bin)` → `{data, Bin}`
- `{quic, closed | peer_send_shutdown | stream_closed, Stream, _}` → `closed`
- 其它 atom 事件（`send_shutdown_complete`、`new_stream` 等）→ `other`
- 别的进程 / 别的 stream 的消息 → `other`

把 `{quic, send_shutdown_complete, ...}` 当数据喂给 `mqtt_frame` 就是「一半的包被当成
协议违规」。

## 后果（Consequences）

- **+** MQTT 引擎零改动，QoS/会话/retain/Will 与 1883 完全一致——这是「同一份代码」而非
  「行为对齐」。
- **+** 默认构建保持纯 BEAM，CI 与日常开发不需要 C 工具链；QUIC 能力按 profile 显式获得。
- **-** 运维要理解两件事：`rebar3 as quic release` 才带 NIF，以及证书必须先配好。
  两件事都写进 `docs/operations.md` §1.2，但仍是额外的部署复杂度。
- **-** 同一 QUIC 连接上重连超过 `max_streams_per_conn` 次后客户端必须新建连接。这是
  quicer 0.4.12 的实测限制，不是设计选择。
- **-** 0-RTT early data 与多流模式不实现：前者是 CN 的可选项，后者是 EMQX 私有扩展。
  两者都写在 `docs/security.md` §12 与 §4.12 限制表。
- **-** 第三方证据的边界：`emqtt` 的 QUIC 支持是 EMQX 侧的实验特性，公开 API 没有流级
  关闭（`close/1` 会连连接一起关）。所以「关流、同连接新流重连」的断言在 CT 里（raw
  `quicer`），不在第三方驱动里。Gate 文本必须照实写，不能说成「对所有客户端互通」。

## 验证（Validation）

- `mqtt_quic_transport_SUITE` 13 例（纯函数，无需真 QUIC）：`decode_msg/2` 的 binary/atom
  分派、按 stream 匹配、关闭类事件归一、`send`/`arm_once`/`close` 在缺 NIF 时的明确错误。
- `mqtt_quic_listener_SUITE` **config 4 例**（全 profile 都跑：无证书拒绝、证书来自
  `broker_security.tls`、默认值、默认不监听）+ **quic 6 例**（仅 `quicer` 可加载时）：
  临时端口、ALPN `mqtt` 握手、非 `mqtt` 被拒、真实 CONNECT→CONNACK、
  `a_pingreq_is_not_a_protocol_error`（D7 的回归）、
  `a_new_stream_on_the_same_connection_is_a_new_session`（D1 的回归）。
- 真实第三方客户端：EMQX **`emqtt` 1.16.1** + quicer 0.4.12，**8/8**
  （`test/protocol-compatibility/mqtt_quic/`）：connect、QoS0/1/2、retain、Will、MQTT 5.0、
  持久会话续连。被测节点**只**起 QUIC listener、端口是内核分配的临时 UDP 端口，所以不可能
  悄悄走别的协议。
- 顺带修掉的第二个真实缺陷（不在 QUIC app 内，但由 `as quic ct` 暴露）：
  `broker_cluster_dist_SUITE:start_peers/2` 的代码路径过滤写死 `_build/test`，
  `rebar3 as quic ct` 下该串不匹配，peer 连 `ra.app` 都找不到，四个分布式用例全部在
  `init_per_testcase` 失败。改为「以 `ebin` 结尾」后与 profile 无关。
