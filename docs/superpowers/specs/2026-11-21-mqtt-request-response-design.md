# MQTT 5.0 Request Response Information 设计

| 项 | 内容 |
|---|---|
| ID | MQ-06（Request Response Information） |
| 协议 | MQTT 5.0（明文 1883 / WS / QUIC 共用 `mqtt_conn_fsm`，自动继承） |
| 关联 | [ADR-017](../adr/017-mqtt5-subset.md)（MQTT 5 子集）；[MQ-06 缺口](../../compatibility/unifiedmq-mqtt-family-gap.md#L57) |
| 日期 | 2026-11-21 |
| 状态 | 待批准 |

## 1. 目标

补齐 MQTT 5 子集中最后一个标「明确未做」的协议特性 **MQ-06 Request Response Information**：让客户端在 CONNECT 里用 `Request Response Information` 标志请求后，broker 在 CONNACK 里回填一个 `Response Information` 值（一个响应主题前缀串），客户端据此构造 `Response Topic = ResponseInformation ++ Correlation Data` 发布请求，订阅了该主题的响应者即可收到——请求/响应闭环成立。

## 2. 现状（已核实，非猜测）

- **codec 已就绪两个属性**：
  - `mqtt_frame.erl:783` 已解 CONNECT 的 `request_response_information`(0x19)；
  - `mqtt_frame.erl:784` 已解 CONNACK 的 `response_information`(0x1A, `utf8_prop`)；
  - 编码侧 `mqtt_frame.erl:1275/1277` 已能编这两个属性。
  - `#publish{}` 记录（`mqtt_frame.hrl:38-44`）带 `props = #{}`，`response_topic`(0x08) / `correlation_data`(0x09) 已在其中随 PUBLISH 透传。
- **请求/响应的「路由」已经成立**：`mqtt_conn_SUITE.erl:775` 的 `v5_publish_properties_reach_the_subscriber` 已证明——订阅者能原样收到 PUBLISH 的 `response_topic` / `correlation_data`（`:793-794`）。也就是说，响应者只要订阅 `response_topic`，请求就会经既有的 topic-match + 透传路径送达。**broker 不需要为此写新的路由代码。**
- **缺口只在 CONNACK 回填**：FSM 从不读 CONNECT 的 `request_response_information`，也从不发 `response_information`。`mqtt_conn_fsm.erl:452` 的 `handle_connect/2` 把 CONNECT props 解进 `#st{}`（`session_expiry` / `receive_maximum` / `max_packet_size` / `topic_alias_max`），唯独没有 `request_response_information`。
- **CONNACK props 装配点**：`mqtt_conn_fsm.erl:2670` 的 `connack_bin/3` 已经把 `connack_capability_props(RC, D)`（MQ-07）和 `AliasProps`（MQ-04 `topic_alias_maximum`）`maps:merge` 进 props。MQ-06 在这里再加第三个来源即可。

**结论**：MQ-06 是一个「codec 已就绪、只差 FSM 接线」的特性，与 MQ-07 同构（config 开关 + CONNACK props 单源 + 零回归），外加一个端到端测试证明请求/响应闭环。

## 3. 设计

### 3.1 语义（按 MQTT-5 §3.1.2.7.6）

`Request Response Information` 是 CONNECT 上的一个布尔标志。客户端置 1 表示「我想做请求/响应，请在 CONNACK 里给我一个 Response Information 值」。broker **可以**（MAY）返回一个 `Response Information` 字符串；客户端把它当作响应主题前缀，发布请求时 `Response Topic = ResponseInformation ++ Correlation Data`。该值对 broker 是信息性的——broker 不据它特殊路由，只用它标识响应命名空间（可用于后续授权）。**broker 不返回 = 零影响**（客户端退回自己拼 topic，行为同今天）。

### 3.2 配置键

新增 app.env 键 `mqtt_response_information`（`broker_proto_mqtt.app.src` env + `config/sys.config.src`）：
- 默认 **`undefined`**（关闭）：CONNACK 永不回填 `response_information`，无论 CONNECT 标志如何——**零回归**。
- 设为一个二进制前缀串（如 `<<"mqtt-response/">>`）时，是 broker 分配给所有请求响应客户端的 Response Information 值。

选「静态配置前缀」而非「每连接派生」的理由：MQTT-5 允许任意值；静态前缀足以让客户端和响应者就命名空间达成一致，且可审计、可跨重连稳定（不引入 per-connection 状态）。每连接随机前缀会破坏「响应者预先订阅」的能力。

### 3.3 `#st{}` 新字段（`mqtt_conn_fsm.erl`，record 末尾 `auth_connect` 之后）

```erlang
%% MQ-06 (spec §3.1): the client asked for request/response info (CONNECT flag)
%% and the broker has a configured Response Information to hand back. Both must
%% hold for the CONNACK to carry response_information. `undefined' = not requested
%% or not configured (no property emitted, zero regression).
response_info_requested = false :: boolean(),
response_information = undefined :: undefined | binary(),
```

### 3.4 CONNECT 接线（`handle_connect/2`，`:452` 的 `#st{}` 初始化处）

在既有的 `#st{...}` 构造里加两字段：
```erlang
response_info_requested =
    maps:get(request_response_information, Props, 0) =:= 1,
response_information = opt(mqtt_response_information, undefined, D0),
```
`opt/3` 既有（`D0` 已在手）。注意 `D0` 此时是入参（`opt` 用 `#st.opts`，构造前的 `D0` 已有 opts）——与 `topic_alias_max = opt(mqtt_topic_alias_maximum, 0, D0)` 同一写法。

### 3.5 CONNACK 回填（`connack_bin/3`，`:2670`）

加第三个 props 来源，仅在 v5 + RC=0 + 客户端请求了 + broker 配置了值 时并入：
```erlang
ResponseInfoProps =
    case vsn(D) of
        5 when D#st.response_info_requested,
                D#st.response_information =/= undefined ->
            #{response_information => D#st.response_information};
        _ ->
            #{}
    end,
Props = maps:merge(maps:merge(connack_capability_props(RC, D),
                              AliasProps),
                    ResponseInfoProps),
```
门控与 MQ-07 一致：只在成功连接（RC=0）回填，拒绝 CONNACK 字节不变；3.1.1（vsn≠5）永不回填。

### 3.6 端到端请求/响应（证明「闭环 + 路由」）

broker 不特殊路由；透传已把 `response_topic`/`correlation_data` 送到订阅者。端到端测试复刻 `v5_publish_properties_reach_the_subscriber` 的形态：requester 用 `request_response_information=1` 连，收到 CONNACK 里的 `response_information`，构造 `response_topic = RI ++ correlation`，发布请求；responder 订阅该 `response_topic`，收到请求（props 含 `response_topic`/`correlation_data`），据此往同主题回一条响应；requester 订阅该主题收到响应。断言链路两端都通。

## 4. 测试计划（CT，TDD）

新增到 `mqtt_conn_SUITE.erl`，注册进 `all()`：

1. `v5_request_response_info_is_returned_when_requested` — `mqtt_response_information` 设 `<<"mqtt-response/">>`，CONNECT 带 `request_response_information => 1`，断言 CONNACK `response_information == <<"mqtt-response/">>`。
2. `v5_request_response_info_absent_when_not_requested` — 配置同上，CONNECT **不带**该标志（默认 0），断言 CONNACK props 里**没有** `response_information`（零回归：客户端没请求就不发）。
3. `v5_request_response_info_off_by_default` — 配置键保持默认 `undefined`，CONNECT 带 `request_response_information => 1`，断言 CONNACK **不含** `response_information`（broker 没配就不发）。
4. `v5_request_response_roundtrip` — 端到端：requester 带标志连、收 CONNACK 的 RI；responder 订阅 `RI ++ <<\"c1\">>`；requester 发一条请求 PUBLISH（`response_topic = RI ++ <<"c1">>`, `correlation_data = <<1>>`）；responder 收到请求后往同主题回响应；requester 收到响应。证明请求/响应闭环经既有路由成立。
5. `v5_request_response_info_absent_on_refusal` — 错密码被 0x86 拒，断言拒绝 CONNACK 不含 `response_information`（门控 RC=0 的正确性）。

辅助：测试在 `init_per_testcase` 里 set_env `mqtt_response_information`（默认 `undefined`，after 复位）；端到端用既有 `mqtt_open_v5` / `mqtt_subscribe_v5` / `mqtt_publish_v5` / `mqtt_recv_v5` / `csock/1` 助手。

## 5. 零回归与不改动项

- **不改动** `mqtt_frame.erl` / `mqtt_frame.hrl`（codec 两个属性 + 透传都已就绪）。
- **不新增** broker 路由逻辑——请求/响应经既有 topic-match + props 透传送达（已有测试 `:775` 证明）。
- 默认 `mqtt_response_information = undefined` ⇒ CONNACK 永不回填，所有既有 v5 CONNACK 断言不变。
- 只在 RC=0 回填 ⇒ 拒绝 CONNACK 字节不变；3.1.1 永不回填。
- WS/QUIC 因共用 `mqtt_conn_fsm` 自动继承，无需单独接线。

## 6. 风险

- **`response_information` 必须是合法 UTF-8 字符串**（codec 用 `utf8_prop`）：配置键由运维提供，文档注明须为 UTF-8 二进制串；非法值会在 CONNACK 编码时触发 `utf8_prop` 的既有校验（编码期报错，不会静默出错帧）。
- **透传已覆盖路由**：本特性不碰路由层，风险集中在 CONNACK 装配与 CONNECT 标志读取，范围小、可单测覆盖。
