# MQTT 5.0 增强认证（AUTH）与 CONNACK 能力回填 设计

| 项 | 内容 |
|---|---|
| ID | MQ-05（AUTH 增强认证）、MQ-07（CONNACK 能力回填） |
| 协议 | MQTT 5.0（明文 1883 / WS / QUIC 共用 `mqtt_conn_fsm`，自动继承） |
| 关联 | [ADR-017](../adr/017-mqtt5-subset.md)（MQTT 5 子集）；[MQ-05/06/07 缺口](../compatibility/unifiedmq-mqtt-family-gap.md#L56) |
| 日期 | 2026-11-21 |
| 状态 | 已批准 |

## 1. 目标

补齐 MQTT 5 子集中两个会让客户端看出来的能力缺口：

- **MQ-07 CONNACK 能力回填**：CONNACK 在 v5 时宣布 broker 真实支持的静态能力位（Maximum QoS / Retain / Wildcard / Subscription Identifier / Shared Sub Available），让合规客户端知道可以怎么用。
- **MQ-05 增强认证（AUTH）**：让客户端能用 MQTT 5 的 AUTH 包做 challenge/response 认证与会话中重认证，而不是只有 CONNECT 里的 username/password。

两者互相独立，都只动明文 MQTT 引擎，WS/QUIC 因共用 `mqtt_conn_fsm` 自动继承。

## 2. 现状（已核实，非猜测）

- `mqtt_frame.erl`：`#connack{}` 记录只有 `session_present` + `return_code`，**无 props**；`p_connack/2`（`:376`）解码只读这两项，不读 properties。encoder 侧的 `prop_bin/2`（`:1236` 起）已能编码任意 props map（覆盖 0x24 Maximum QoS、0x25 Retain Available、0x28 Wildcard Available、0x29 Subscription Identifier Available、0x5C Shared Sub Available；另有 0x15/0x16 authentication_method/data）。
- codec 已能解 CONNECT 上的 `authentication_method`(0x15) / `authentication_data`(0x16)（`mqtt_frame.erl:762-763`），但**没有 AUTH 包**（control type 0x0F），`#connack{}` 无 props。
- `mqtt_conn_fsm.erl`：`connack_bin/3`（`:2423`）恒 `enc(#connack{session_present=SP, return_code=RC}, D)`。`authenticate/2`（CONNECT 内）是**纯同步**调 `broker_security:authenticate(User, Pass)` → principal，无 challenge、无 AUTH 包处理。
- `broker_security.erl`：导出 `authenticate/2`、`authorize/2`、`tenant_id/1`、`mqttsn_psk_for_identity/1`；是同步凭据存储，**无 challenge/异步 API**。不改动它。
- 失败码：v5 CONNACK 拒绝用 `refuse/2`（reason code 0x86 = Not authorized 已有）。

## 3. 设计

### 3.1 MQ-07：CONNACK 能力回填

1. `#connack{}` 记录加 `props = #{} :: map()`。
2. codec `p_connack/2`：解码 SP + RC 后，把剩余字节解析为 properties（复用现有 `read_props/1`；空则 `# {}`）。
3. codec encode：`#connack{}` 已走通用 `enc/2`，props 由 `prop_bin/2` 编码，无需改 encoder 主体。
4. `mqtt_conn_fsm:connack_bin/3`：当 `vsn(D) =:= 5` 且配置 `mqtt_connack_props_v5`（默认 `true`）时，props 加：
   - `maximum_qos => 2`
   - `retain_available => 1`
   - `wildcard_subscription_available => 1`
   - `subscription_identifier_available => 1`
   - `shared_subscription_available => 1`
   - 否则 props 恒 `# {}`。
5. **不做** Server Keep Alive、Assigned Client Identifier（本期范围外，缺口文档已列）。
6. **零回归**：3.1.1（vsn 4）CONNACK 字节与改动前逐字节一致——props 仅在 vsn=5 加。

> 五个能力位属性 id（codec 已就绪）：`maximum_qos`=0x24、`retain_available`=0x25、`wildcard_subscription_available`=0x28、`subscription_identifier_available`=0x29、`shared_subscription_available`=0x2A。

### 3.2 MQ-05：auth-method 框架（可插拔）

新增 `broker_auth.erl`，定义 behaviour 并做 method 注册表：

```erlang
-callback begin(Method :: binary(), InitialData :: binary() | undefined,
                Ctx :: map()) ->
    {ok, Principal :: term(), Ctx :: map()}
  | {continue, ChallengeData :: binary(), AuthState :: term()}
  | {error, Reason :: term()}.
-callback continue(Method :: binary(), AuthState :: term(),
                   ResponseData :: binary() | undefined) ->
    {ok, Principal :: term(), Ctx :: map()}
  | {continue, ChallengeData :: binary(), AuthState :: term()}
  | {error, Reason :: term()}.
-callback reauth(Method :: binary(), Ctx :: map(), Data :: binary() | undefined) ->
    {ok, Ctx :: map()} | {error, Reason :: term()}.
```

- `broker_auth:enabled_methods/0`：读配置 `mqtt_auth_methods`（默认 `["plain"]`）。
- `broker_auth:begin/3`、`continue/3`、`reauth/3`：按 method 名分派到对应模块（`broker_auth_<name>`）。
- **未声明 `authentication_method` 的 CONNECT 走现有同步 `authenticate/2` 路径，完全不变**（零回归）。

### 3.3 首个 method：`broker_auth_plain`（不引入新加密依赖）

nonce 绑定的 username/password 交换，复用现有用户库：

- `begin(Method, undefined, Ctx)`：生成 `Nonce = term_to_binary({unique_integer, monotonic_time})`（无 crypto 依赖），`{continue, Nonce, {Nonce, Ctx}}`。Challenge data = Nonce。
- `continue(Method, {Nonce, Ctx}, ResponseData)`：客户端回 `{User, Nonce, Password}`。校验 response 里的 Nonce 与登记的一致（防重放）且 `broker_security:authenticate(User, Password)` 返回 `{ok, Principal}` → `{ok, Principal, Ctx}`；否则 `{error, unauthorized}`。
- `reauth(Method, Ctx, {User, Password})`：`broker_security:authenticate(U,P)` 通过 → `{ok, Ctx}`；否则 `{error, unauthorized}`。
- **明确边界**：这是「nonce 绑定的明文凭据交换」，证明 AUTH 报文 + 多轮状态机真实可用、端到端可测；**刻意不实现 SCRAM 等加密质询**（无新依赖、无 OIDC/JWKS，均已在缺口文档范围外）。加密 method 可作为后续插件接入同一 framework。

### 3.4 MQ-05：连接层状态机与报文语义

- **CONNECT 带 `authentication_method`**（`handle_connect/2` 的认证分支）：
  - `broker_auth:begin(Method, AuthData, #{client_id => ..., tenant => ...})`：
    - `{ok, Principal, Ctx}` → 走现有成功路径（attach session、CONNACK 0）。
    - `{continue, Challenge, AuthState}` → `#st{auth_state = {Method, AuthState}, auth_method = Method}`，发 **AUTH(0x18 Continue authentication)**，props 含 `authentication_method=Method` + `authentication_data=Challenge`，进入 `awaiting_auth` 子状态。
    - `{error, _}` → `refuse(16#86, D)`（Not authorized）。
- **`awaiting_auth` 子状态收到 AUTH** → `broker_auth:continue/3`：
  - `{ok, Principal, Ctx}` → 走成功路径（此时 session 尚未 attach，用 Ctx 里的 client_id/tenant 续接 `attach_session` 等）。
  - `{continue, Challenge2, AuthState2}` → 更新 auth_state，再发 AUTH 0x18。
  - `{error, _}` → `refuse(16#86, D)`。
- **AUTH 超时**：`awaiting_auth` 期间复用/臂一个 `auth_timeout_ms`（默认 30s），超时 disconnect。spec 标注为秒级。
- **会话中 re-auth**（已 established 的连接收到 AUTH）：`broker_auth:reauth/3`：
  - `{ok, Ctx}` → 回 **AUTH(0x00 Success)**。
  - `{error, _}` → **DISCONNECT 0x9C**（Not authorized）。
- **FSM 路由**：新增 `handle_auth/2`，在 `awaiting_auth` 与 established 两种情形下分别处理。

### 3.5 will / retain / 会话不受影响

- CONNACK props 加法不改变 session_present / return_code 语义。
- AUTH 握手发生在 session attach 之前，与 Will 注册、retain、订阅互不影响（沿用现有 `handle_connect` 成功尾段的顺序）。

## 4. 不做（明确边界）

- SCRAM 等加密质询、OIDC/JWKS（缺口文档范围外；framework 预留，method 后补）。
- Server Keep Alive、Assigned Client Identifier（MQ-07 只做五项静态能力位）。
- MQTT 3.1（无这些概念，MQ-16 关注点）。
- 不改 `broker_security` 的同步语义（它仍是凭据存储；auth method 内部调它）。

## 5. 影响面

- `mqtt_frame.erl`：`#connack{}` 加 props + 解码；新增 `#auth{}` 记录 + encode/decode（control 0x0F）。
- `mqtt_conn_fsm.erl`：`connack_bin/3` 能力位；CONNECT 的 AUTH 分支 + `awaiting_auth` 子状态；`handle_auth/2`；`#st{}` 加 `auth_state`/`auth_method`/`auth_deadline`。
- `broker_auth.erl`（新）、`broker_auth_plain.erl`（新）。
- `config/sys.config.src`：`mqtt_connack_props_v5`（默认 true）、`mqtt_auth_methods`（默认 `["plain"]`）、`mqtt_auth_timeout_ms`（默认 30000）。
- `broker_metrics` 键集不变；无新 app、无新端口、无新依赖。
- 文档：`docs/protocol-compatibility.md` §4.11、`docs/compatibility/unifiedmq-mqtt-family-gap.md`（MQ-05/07 改「已支持」）。

## 6. 验证基线

- compile 0 warning、dialyzer 0 warning、xref 无新增 unused-export。
- 全量 `rebar3 ct` 在 1579 基线上 +8~10 例，零回归。
- 合到 main 后复跑同基线。

## 7. 验收（测试先行，TDD）

在 `apps/broker_proto_mqtt/test/mqtt_conn_SUITE.erl`（全 `port => 0`）增：

MQ-07：
1. `v5_connack_advertises_capabilities`：v5 CONNACK props 含 `maximum_qos=2` + 四个 available=1。
2. `v3_connack_has_no_capability_props`：3.1.1 CONNACK 字节与基线一致（无新增属性）。

MQ-05：
3. `auth_challenge_continues_then_succeeds`：CONNECT 带 method → 收 AUTH 0x18 + challenge → 回正确 response → CONNACK 成功、principal 正确。
4. `auth_wrong_password_is_not_authorized`：回错误 password → CONNACK 0x86。
5. `auth_replayed_or_unknown_nonce_is_not_authorized`：response 带不匹配/重放 nonce → 0x86。
6. `auth_reauth_success_returns_auth_success`：established 会话中发 AUTH re-auth 成功 → 回 AUTH 0x00。
7. `auth_reauth_failure_disconnects_0x9c`：re-auth 失败 → DISCONNECT 0x9C。
8. `auth_plain_connect_without_method_unchanged`：不带 method 的 CONNECT 走现有同步路径，零回归。
