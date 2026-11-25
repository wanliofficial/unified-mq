# 安全与治理（S6）

- **状态**：S6 交付事实文档 v1.0（2026-11-09）；S7 追加 §2.1（六协议统一身份表）；S9-F 追加 §12.1（三协议 18 条未实现项）；S10-A 追加 §2.1（WebRTC 信令共用同一张 `users` 表）、§9 的 TLS 调用者清单与 §12.2（信令面未实现项）；S10-B 更新 §12.2（房间/`to` 转发/ACL/分片重组四条已实现）；**S10-C 追加 §2.1（TURN 长期凭据同表派生）、§12.2 第 12 条结案与新的 §12.3（ICE 面 8 条未实现项）**
- **对应**：README §10.1 / §10.2；`docs/roadmap.md` §8.1；Gate S6 第 1 项
- **实现**：`apps/broker_security/`（`broker_security`、`broker_limits`、`broker_security_tls`）
- **测试**：`broker_security_SUITE`、`broker_security_acl_SUITE`、`broker_limits_SUITE`、`broker_security_tls_SUITE`、`broker_security_audit_SUITE`

本文只写已经实现的机制与已经测过的边界。没有落地的能力单列在 §8，不冒充已支持。

---

## 1. 威胁模型与边界

| 资产 | 威胁 | 本版本的防线 |
|---|---|---|
| 租户数据 | 一个租户读/写另一个租户的主题 | 认证身份绑定租户；租户隔离先于 ACL；路由与订阅索引按租户隔离 |
| 主题 | 越权发布/订阅 | 有序 first-match ACL，无匹配即拒绝 |
| 事件上下文 | 用 CloudEvents `source`/`subject`/扩展属性伪造身份或越权 | ACL 在消息目标上判定，事件属性永远不改变身份、租户或目标 |
| 资源 | 超大消息/属性/嵌套 JSON 打爆内存或 CPU | 可配置上限，在 append 与事件校验入口统一拒绝 |
| 指标 | 任意 `source`/`subject`/`id` 变成 Prometheus 标签导致基数爆炸 | 指标 key 是封闭集合，没有用户可控标签 |
| 管理面 | 未授权创建备份、重载证书 | 默认只绑回环；非空 token 时除健康检查外全部要求 Bearer |
| 传输 | 明文窃听 | 可选 TLS（所有协议 listener 同一个开关），支持 PEM 轮换 |

不在本版本承诺之内：跨节点传输加密、细粒度到消息级别的授权、密钥管理系统（KMS）集成、审计日志的签名/防篡改。见 §8。

---

## 2. 认证

`broker_security:authenticate/2` 从 `broker_security.users` 读取 `{Username, #{password, tenant_id}}`：

- 成功返回 `{ok, Username}`；`Username` 即 principal。
- 失败恒返回 `{error, bad_username_or_password}`——**不区分**「用户不存在」与「密码错」，避免用户名枚举。

### 2.1 六个协议共用一张身份表（S7）

`broker_security:authenticate_conn/1` 是**除 MQTT 外**五个协议适配器的统一认证入口（MQTT 走下面「现状边界」里那条等价路径）。六种协议各自的握手形状不同，归一化成两种凭据形态：

| 形态 | 载体 | 谁用 |
|---|---|---|
| `{user_pass, Name, Secret}` | 用户名 + 密码 | WS `connect` 帧、NATS CONNECT、AMQP `start-ok` 的 PLAIN、Kafka SASL/PLAIN、Pulsar `plain`（MQTT 见「现状边界」） |
| `{token, Secret}` | 单一 bearer 值 | Pulsar `token` 认证方法、NATS `auth_token` |

两者都查**同一张** `users` 表，因此**一个身份列表服务六个协议**。表的每条记录可以带 `password` 和/或 `token`：

```erlang
{users, [
  {<<"guest">>, #{password => <<"guest">>, tenant_id => <<"t0">>}},
  {<<"svc">>,  #{token => <<"tok">>,    tenant_id => <<"t3">>}}
]}
```

**租户来自记录，不来自客户端。** 这是本次修复的核心：修复前 WS/NATS/AMQP/Kafka/Pulsar 的租户来自协议字段或硬编码的 `t0`，客户端可以自称任何租户；修复后租户一律由匹配到的用户记录给出，协议帧里的 `tenant` 字段被忽略。

`authenticate_conn/1` 的失败形态同样统一：未知用户、错误密码、未知 token、错误 token、以及两种形态之间的差异，对外都是同一个 `bad_username_or_password`，探测方无法据此枚举。

**S10-A 再加一个调用者（2026-11-13）**：WebRTC 信令前端 `webrtc_conn` 在认证窗口内收到 `{"event":"authenticate","data":{"username":…,"password":…}}` 后调用同一个 `authenticate_conn({user_pass, U, P})`，因此信令面也与上述协议**共用同一张 `users` 表**，租户同样只来自用户记录。它不引入新的凭据形态、不读 `broker_security.require_auth`（那是「必须认证的协议名列表」），而是用自己的 `broker_proto_webrtc.require_auth` 开关，见 `docs/architecture.md` §3.16 与本文件 §12.2。S9 的 A2A / MCP 经由 `agent_rpc_auth` 也在同一张表上取身份（不重复计入这里的「六协议」口径）。

**S10-C 再加一个派生者（2026-11-14），但走的是另一条函数**：TURN 的长期凭据（RFC 5766 §4）不是「校验口令」而是「与客户端派生同一个 key」，因此 `turn_auth` 调用新增的 `broker_security:long_term_key(User, Realm)` → `key = MD5(username ":" realm ":" password)`，口令仍**只**从 `user_record/1` 取；认证之后仍然走 `authorize/2`，只是新增了五个 `turn_*` action、主体不带 `tenant_id`（TURN 报文里没有租户字段，填 `default_tenant()` 会把默认租户部署的请求整片拒掉）。**`authenticate_conn/1` 与 `authorize/2` 的既有语义一字未动**；口令与派生 key 都不进日志（`turn_auth_SUITE:no_password_or_key_reaches_the_log/1` 带 probe 断言）。ICE 面的未实现项见 §12.3。

### 2.2 匿名连接是部署决策

`broker_security.require_auth` 是一个协议名列表：

- 不在列表里的协议接受匿名连接，租户是 `broker_security.default_tenant`（默认 `t0`）。
- 在列表里的协议必须出示凭据，否则连接被拒。
- **握手里带不了凭据的协议永远不会被要求**——要求一个协议出示它根本没法出示的东西只会把客户端全锁在外面，而不是变得更安全。

各协议拒绝认证失败时的表现沿用它们各自的语义：MQTT 回 CONNACK 拒绝码、WS 回 `unauthorized` 错误帧、NATS 回 `-ERR 'Authorization Violation'`、AMQP 发 `connection.close`（握手级失败，通道从未打开）、Kafka SASL 回 `SASL_AUTHENTICATION_FAILED`、Pulsar 直接断开连接。

NATS 的 `auth_token`（`broker_proto_nats.auth_token`）是 NATS 协议原生的共享密钥，语义与身份表不同，因此保持原样；NATS 的 `user`/`pass` 走身份表。

**现状边界（必须如实说明）**：

1. **MQTT 不走 `authenticate_conn/1`**：`mqtt_conn_fsm` 的私有函数 `authenticate/2` 直接调 `broker_security:authenticate/2`（`mqtt_conn_fsm.erl:471`）。代码路径不同、语义等价——两者读同一张 `users` 表、比对同一 `password` 字段、失败回同一个 `bad_username_or_password`；MQTT 的凭据形态只有 user/pass 一种，归一化没有增益，因此没有为统一而统一。
2. **`require_auth` 对 MQTT 无声无效**：`mqtt_conn_fsm` 从不调用 `broker_security:auth_required/1`，所以配置 `{require_auth, [<<"mqtt">>]}` **不会**拒绝匿名 CONNECT（`credential_capable/0` 里含 `<<"mqtt">>` 只是为将来接线性留位）。当前 MQTT 的匿名放行由 CONNECT 里「无用户名/无密码即匿名」这条分支决定。把 `auth_required/1` 接进 CONNECT 路径列为 S7 之后。
3. **MQTT 的匿名 CONNECT 仍把 `client_id` 当作 principal**（S1 冻结的语义，`docs/design/s1-mqtt-vertical-slice.md` §13 L10）。这个 principal 不在身份表里，因此它的租户是 `default` 而不是任何用户记录的租户——这是 S1 的既有行为，本次未改。
4. **MQTT-SN 的 `require_auth` 是显式拒绝启动，不是静默失效**（S8-C）：MQTT-SN 1.2 的 CONNECT **没有用户名/密码字段**，协议上无法承载凭据。`credential_capable/0` 不含 `<<"mqtt_sn">>`，所以 `broker_security:auth_required(mqtt_sn)` 恒为 `false` —— 这对**认证**是对的（要求一个协议出示它没有的东西只会锁死所有客户端），对**配置**是错的：一个要求「所有 listener 必须出示凭据」的部署，必须被告知这个 listener 给不了。`broker_proto_mqtt_sn_app` 因此不经过 `auth_required/1`，直接读 `require_auth` 原始 app env；一旦列表里出现 `mqtt_sn`，app **拒绝启动监听器**并返回 `{require_auth_unsupported, mqtt_sn}`。与第 2 条形成对照：MQTT 是「配了但无声无效」（历史遗留，仍待修），MQTT-SN 是「配了就不启动」（新代码不接受这种配置形态）。注意这里的拒绝粒度是整个 `broker_proto_mqtt_sn` app，而不是单个 listener —— MQTT-SN 的网关只有一个 socket 服务全部客户端，没有「只对某些客户端要求凭据」的表达力，也没有单 listener 的失败隔离可言。
5. **吊销（revocation）目前只作用于 MQTT**（2026-11-22，MQ-17 #10）：`broker_revocation` 注册表 + `broker_security:revoke/2` / `clear_revocation/1` / `revoked/1` 门面提供按 principal 的运行时吊销；`mqtt_conn_fsm` 在 CONNECT 认证（`authenticate/2`）与会话登记后复查（`connect_success/2`）两处查 `revoked/1`，并在吊销时向该 principal 的全部活动连接发 DISCONNECT（正常关闭语义，**抑制 Will**）后关闭。**边界**：其余协议走 `broker_security:authenticate/2` / `authenticate_conn/1`，二者不查吊销表，故非 MQTT 连接既不会被驱逐、也不会被拒绝重连；吊销状态是**节点本地**的（不入 ra，与 admission/drain 一致）；粒度为 principal，不支持按租户或按 client_id。详见 `docs/operations.md` §2「吊销」与 `docs/superpowers/specs/2026-11-22-mqtt-session-revocation-design.md`。

---

## 3. 租户隔离

两层落地：

1. **授权层**：`broker_security:authorize/2` 先比较请求的 `tenant_id` 与 principal 自身的租户。不同即 `deny`，**先于**任何 ACL 规则。ACL 只能放宽主题，永远不能越过租户边界。
2. **数据层**：`broker_router_hash_store` 的索引键是 `{Tenant, Filter}`；`broker_router_hash:route/3` 按 `Ctx.tenant_id` 过滤。订阅实体与 `.subs` 持久化都带 `tenant_id`，重启回填也按租户重建。`<<"*">>` 是 legacy 通配（S1 单租户布局），不是「跨租户读」。

MQTT 连接在 CONNECT 时把 `broker_security:tenant_id(Principal)` 写进连接状态，之后 PUBLISH/SUBSCRIBE 都用它构造 `#route_ctx{}` 与授权请求。

证据：`broker_security_audit_SUITE` 的 `tenant_escalation_publish_is_denied` / `tenant_escalation_subscribe_is_denied`（即使 ACL 是 `allow * * #` 也拒绝跨租户）。

---

## 4. ACL

`broker_security.acl` 的取值：

- 原子 `allow`：保持 S1 的放行姿态（默认值）。**匿名主体（`undefined`）也一律放行。**
- 原子 `deny`：fail-closed 姿态，不带任何 allow 规则，**一切请求（具名主体与匿名主体）都被拒绝**。这是 deny-by-default 的开关：它同时关掉匿名（未认证）通道（如明文 CoAP，见 `coap_dtls_SUITE:plain_udp_unaffected` 反向钉住）。
- 规则列表：有序 first-match，无匹配即 `deny`。**匿名主体不进入 ACL**——未认证报文没有主体可匹配，因此在规则列表下默认放行（与 `allow` 一致；这是 ADR-031 的设计，明文通道刻意不纳入 ACL）。要在 `deny` 姿态下按需放行具名主体，改用规则列表写 allow 规则。
- 其它非列表值：按 `allow` 处理（向后兼容），不做隐式「规则写错就全拒」以外的解释。

一条规则的形状：

```erlang
#{effect    => allow | deny,
  principal => <<"alice">> | <<"*">>,
  action    => publish | subscribe | <<"*">>,
  pattern   => <<"t1/app/#">>}
```

`pattern` 的匹配语义：

| 形状 | 含义 | 例 |
|---|---|---|
| 精确串 | 完全相等 | `t1/app/x` |
| 尾 `/` | 前缀 | `t1/app/` 匹配 `t1/app/x`，不匹配 `t1/app` |
| 尾 `*` 或 `#` | 前缀通配 | `t1/#` 匹配 `t1/a/b` |
| `*` / `#` / 空 | 全部 | 任何目标 |

`*` 对 principal 和 action 也是通配。非法规则被跳过而不是让发布者崩溃；**只要有规则存在（或姿态为 `deny`），未匹配就是拒绝**，这是安全方向。匿名（未认证）通道不携带主体，故在 `allow` 与规则列表下放行，仅 `deny` 姿态拒绝——这是 deny-by-default 对未认证报文的唯一收口。

证据：`broker_security_acl_SUITE`（14 例，含 `deny_posture_refuses_named_principal_without_a_rule`、`deny_posture_refuses_anonymous_without_a_rule`、`deny_posture_is_explicitly_not_the_permissive_default` 钉住 `deny` 姿态对具名与匿名主体一律拒绝、与 `allow` 姿态有别）+ `broker_security_audit_SUITE` 的 `acl_bypass_by_unmatched_rule_is_denied`、`acl_bypass_by_action_swap_is_denied`、`acl_deny_precedes_a_later_allow`、`legacy_allow_is_explicit_not_a_fallback`；明文 CoAP 通道不被 ACL 拒绝由 `coap_dtls_SUITE:plain_udp_unaffected` 钉住。

---

## 5. CloudEvents 属性不绕过 ACL

- ACL 判定使用 broker 消息的 `tenant_id` + `destination`，在事件解码之前完成。
- 事件策略（`disabled` / `pass_through` / `validate_if_present` / `required` / `normalize`）返回的消息保持原租户与目标；§5 的 `event_policy_preserves_tenant_and_destination` 对五档逐一断言。
- 事件里的 `source` / `subject` / 自定义扩展永远不参与身份判定；伪造 `source=bob` 不能借用 bob 的规则。

证据：`broker_security_audit_SUITE` 的 `event_attribute_cannot_change_the_acl_decision`、`forged_event_source_cannot_impersonate_another_principal`。

### 5.1 Trace context 不参与身份判定（README §10.2）

`broker_event_trace` 接在 `broker_event:apply_policy/2`（六协议共用的入站 seam）上，按 W3C Trace Context 校验 `traceparent` / `tracestate`：只接受 version 00、非全零的有效值并规范化为小写；`tracestate` 受 32 成员 / 512 字节上限约束；没有有效 parent 时整个 state 被忽略。合法值留在 `#broker_message.headers` 里随 WAL 持久化、投递时由各协议原样回填——这就是 §10.2 要的「传播」，没有给冻结的 `#broker_message` 加字段。

**隐私与信任边界**：`broker_event.trace_context` 默认 `off`，客户端提供的 trace 头被剥离而不是转发；`on` 全量传播，`{trusted_tenants, [...]}` 只对列出的租户传播（「可信上下文」与「按需」）。WebSocket / NATS 的头通道是白名单，未开启传播时连入站都不放行这两个键；AMQP / Kafka / Pulsar 全量透传 headers，由同一 seam 在 `off` 时剥离。

**MQTT 5 的通道**：ADR-017 §17.2 把 PUBLISH 属性放在 `proto_meta` 而非 `headers`，所以 `mqtt_conn_fsm` 在构造消息时把 User Properties 里的 `traceparent` / `tracestate` 提到 `headers`、从属性表里剔除，投递时再从（已校验/规范化的）`headers` 回填到 User Properties——`off` 时同样不会经原属性往返泄漏客户端值。3.1.1 没有 properties，天然无此通道。六个协议至此都受同一个门控。

**与 ACL 的同构约束**：trace 头是客户端可控的，但 `propagate/1` 只改写这两个 header 键，`tenant_id` / `principal` / `destination` 逐字段不变；`broker_security:authorize/2` 只读 `action` / `tenant_id` / `destination`，从不读取 headers。伪造 `traceparent` 或在头里夹带 `tenant_id` / `principal` 都不能借用别人的租户或身份。

证据：`broker_event_trace_SUITE`（10 例，含 `trace_headers_cannot_escalate_the_tenant`），以及 `ws_conn_SUITE` / `nats_conn_SUITE` / `mqtt_conn_SUITE` 的端到端往返用例（含 `off` 时剥离、非法值剔除、大写规范化）。OTel 等导出层仍未实现（`docs/architecture.md` §9）。

---

## 6. 资源上限（README §10.1）

`broker_limits` 从 `broker_security.limits` 读取，缺失键用默认值，值畸形则大声失败（不静默关闭）：

| 键 | 默认 | 作用点 |
|---|---|---|
| `max_message_bytes` | 1 MiB | `broker_queue:append/3` 前置检查 |
| `max_header_count` | 32 | 同上 |
| `max_header_value_bytes` | 8192 | 同上（名字与值分别计） |
| `max_event_attributes` | 64 | `broker_event_codec:validate/1`（核心 + 扩展） |
| `max_extension_value_bytes` | 4096 | 同上 |
| `max_json_depth` | 16 | 同上（`data` 的嵌套深度） |
| `max_connections` | `infinity`（闸门关闭） | `broker_admission:try_acquire/0`，节点级准入（S12-B / ADR-032 D7） |

**`max_connections` 是节点级的，不是每 listener 的。**架构里每个协议前端的 ranch listener 还有一个自己的 `max_connections`（默认 1024），但它**只在 listener 范围内**生效，且 ranch 到限时并不拒绝连接——它让 acceptor 睡下，对端 TCP 连上却永远收不到任何协议应答。`broker_security.limits.max_connections` 补上的是**跨 listener 的节点级预算**：超限时各前端按自己的协议显式拒绝（MQTT `rc=3` / MQTT 5 `0x89` / MQTT-SN `3` / HTTP 类 503 + `Retry-After` / NATS `-ERR` / Kafka `56` / AMQP `541` / Pulsar `ERROR_UNKNOWN` / QUIC `{error, too_many_connections}`）。详见 `docs/design/s12b-admission-gate-spike.md`。

默认 `infinity` 是刻意的：升级不该把一台原本没有节点级上限的机器变成会上限拒绝的机器。**被拒绝的连接只写 warning 日志，不新增 metrics 键**（`broker_metrics` 键集封闭，R-LB9 的扩展项标「S12 不交付」），排障要靠 `max_connections` 的当前值与日志。

证据：`broker_admission_SUITE`（14 例：并发不超发、死持有者名额回收、活持有者不被回收、lease 换主、drain 优先、`infinity` 不计），以及 12 个前端各自的负控（第二条连接必须拿到**可观测的拒绝帧**）与正控（释放后可再进）。

所有协议都经 `broker_queue:append/3`，因此消息级上限对六个协议一致生效；事件级上限只在 CloudEvents 解码路径生效。超限返回 `{error, {limit_exceeded, Which, Actual, Max}}`，由调用者拒绝（MQTT 不 ACK）。

证据：`broker_limits_SUITE`（11 例）。

---

## 7. 指标防高基数

`broker_metrics` 的 key 是**封闭集合**（`key_specs/0` + `key_types/0`），`snapshot/0` 只返回这些 key；没有 `source` / `subject` / `id` / 目的地等用户可控字符串作为标签。管理 API `/metrics` 的渲染也只遍历这个集合。

新增的 `authz_failures` 与 `auth_failures` 分开计数：前者是「认证成功但授权失败」（ACL/租户），后者是凭据错误，便于区分策略问题与账号问题。

---

## 8. 错误策略（README §10.1）

| 场景 | 行为 | 证据 |
|---|---|---|
| 非法事件（缺少核心属性、类型错、`data`/`data_base64` 互斥、未知 specversion） | 解码返回 `{error, {invalid_event, Why}}`；`required`/`normalize` 策略拒绝，MQTT 不 ACK（v5 `0x83` / 3.1.1 断连） | `broker_event_SUITE` |
| 未知扩展属性 | 保留为扩展（名字/值形状合法时）；超过数量或长度上限则 `limit_exceeded`；非法扩展名/值类型拒绝 | `broker_event_codec` + `broker_limits_SUITE` |
| Schema 不匹配 | `dataschema` 只做 URI 形状校验；本版本**不做** Schema 内容校验（没有 Schema Registry） | 见下 |
| 编码失败 | `encode_structured` / `encode_binary` 先 `validate/1`，非法事件不产出字节 | `broker_event_SUITE` |
| 策略未知 | 返回 `{error, {invalid_policy, P}}`，不静默降级 | `broker_event_SUITE` |

**未实现（不冒充）**：Schema Registry 内容校验、未知扩展属性的「拒绝还是保留」按租户可配——当前是「保留 + 上限」。这两项列入 §12。

---

## 9. TLS 与密钥轮换

`broker_security.tls`：

```erlang
{tls, #{enabled => false,
        certfile => "config/tls/server.crt",
        keyfile => "config/tls/server.key",
        cacertfile => undefined,          % 客户端 CA（mTLS 时必填）
        verify => verify_none,            % verify_peer 开启双向认证
        fail_if_no_peer_cert => false,
        versions => ['tlsv1.2', 'tlsv1.3']}}
```

- `broker_security_tls:transport/0` 返回 `{ranch_tcp, []}`（禁用）或 `{ranch_ssl, Opts}`（启用）。**十个**协议 supervisor 都调用它（MQTT、WebSocket、MQTT over WebSocket、NATS、AMQP、Kafka、Pulsar，以及 S9 的 A2A、MCP 与 S10-A 的 WebRTC 信令），**一个开关覆盖所有 listener**，没有哪个 listener 能「知道 TLS 但不消费配置」。**三个**不在这条链上：MQTT-SN 走 UDP（要 DTLS，见 §12 第 8 条），MQTT over QUIC 只把证书配置当作**来源**读（握手本身就是 TLS 1.3，没有明文模式），S10-C 的 `broker_ice`（STUN/TURN over UDP，5349/TLS 与 DTLS 都不做，见 §12.3 第 1 条）。
- **协议侧还要真的把 TLS 握手跑完**：ranch 对 TLS 只做 `transport_accept`，协议进程收到 `{handshake, Ref, Transport, Sock, Timeout}` 后必须自己调 `Transport:handshake/2`（`ranch_tcp` 是 no-op，`ranch_ssl` 才完成握手），否则首个 `ssl:setopts/2` 报 `notsup_on_transport_accept_socket`。这正是 S8-B 修掉的既有缺陷：`broker_proto_ws` 的 wss 在此前**完全不可用**（listener 明明是 `ranch_ssl`），现由 `ws_conn_SUITE` 的 wss 用例钉住；`broker_proto_mqtt_ws` 从第一版起就正确握手。
- 启动时 `broker_security_app:start/2` 校验：启用且缺 PEM 文件 → 应用启动失败；禁用永远合法。
- 轮换：替换同路径 PEM，调用 `broker_security_tls:reload/0`（管理 API `POST /api/v1/tls/reload`），清 `ssl` PEM 缓存，下一个握手用新证书；已建立连接保持原证书（标准 TLS 语义）。
- 脱敏：`redacted_config/0` 不返回密钥内容。

证据：`broker_security_tls_SUITE`（12 例，含真实 `ssl:listen` + 握手 + `transport/0`）。

**S10-C 的 `broker_ice` 不是这个清单里的调用者**：它的 3478/UDP 与 relay 段全是**明文 UDP**（ADR-029 §D2 明确不做 5349/TLS 与 DTLS），`broker_security.tls.enabled` 对它没有任何影响。TURN 的密码学边界只有 `MESSAGE-INTEGRITY` 的**完整性**保护（口令不过线，但 username/realm/relay 流量在链路上是明文），见 §12.3 第 2 条。

---

## 10. 管理面鉴权

`broker_admin_http`：

- 默认 `bind = {127,0,0,1}`、`port = 8090`、`token = <<>>`（空 token = 不鉴权，只允许回环）。**非回环 `bind` 配空 `token` 现在启动即失败**（`{insecure_bind, Bind, empty_token}`，`broker_admin_sup:check_bind_auth/0`，S12-A 落地，ADR-032 D2）——这条不变式此前只写在注释与文档里；`enabled => false` 不校验（没有 listener 就没有暴露）。
- 非空 token 时，除 `/healthz`、`/readyz` 外全部要求 `Authorization: Bearer <token>` 或 `X-Admin-Token`。
- token 比较是常量时间（`secure_equal/2`），避免逐字节时序猜测。
- 响应经 `json_safe/1` 收敛，避免 pid/ref/tuple 导致 500。

证据：`broker_admin_SUITE`（26 例，含 401、常量时间分支、脱敏配置、drain 三态与严格入参校验、非回环+空 token 拒启、吊销端点的入参校验与路径段百分号解码）与 `broker_admin_dist_SUITE`（3 peer 多节点视图一致性 + drain 节点本地性，S12-A）。drain / `undrain` / `revoke` 都在 `/api/v1/` 下，因此与其他管理端点一样要求鉴权；`/healthz`、`/readyz` 不要求。

---

## 11. 配置审计

`broker_config_audit` 在 boot 时写一份**脱敏**的有效配置快照 + 规范化 SHA-256：

- 每个模块自己暴露 `redacted_config/0`，审计模块不接触明文密码。
- 快照经 `/api/v1/config` 暴露；`<data_dir>/config-audit.log` 追加一行 JSON。
- 审计失败只记日志、**不阻塞启动**（审计是证据，不是门禁）。

---

## 12. 明确的未实现项（S7 之后）

**S7 的全部验收项已于 2026-11-10 结案**（`docs/roadmap.md` §9 的 Gate S7 已无未勾选项）：身份表统一见 §2.1；各协议标准 CloudEvents Binding 的结论见 `docs/roadmap.md` §9.1 与 `docs/protocol-compatibility.md` §4.10——**结论是「统一 `ce-*` 头通道是本 Broker 的项目定义，没有任何协议按官方条文逐字实现」**，差异逐条列明（MQTT 5 规范要求不加前缀、Kafka 用 `ce_`、NATS 需 percent-encoding、WebSocket 需子协议协商、AMQP 仅 1.0 有规范、Pulsar 无规范）。逐字实现**不立项**，因此不再作为待办。

**S7 之后明确未实现**（不作为 Gate 依据，`progress.md` 有同一份清单）：

1. ~~跨节点/复制通道的 TLS。~~ **已实现（2026-11-20，ADR-035）**：`broker_cluster.dist_tls` + OTP `-proto_dist inet_tls` + mTLS，默认关闭。**边界照实说**：CA 信任是双向准入闸门；**拨出方向按 SAN 绑定对端身份（证书 SAN 必须写对），拨入方向不校验客户端 SAN**；开关是全组重启，不能灰度混跑；证书轮换需重启。见 `docs/operations.md` §5.5。
2. Schema Registry 内容校验。
3. 审计日志签名与防篡改、外部 KMS。
4. 每条规则可配置的「未知扩展属性拒绝」策略。
5. `broker_event:duplicate/1` / `quarantine/2` 仅接在 MQTT 发布路径，其余五协议与 S8-C 的 MQTT-SN 都未接（见 `docs/cloudevents-mapping.md` 的「接线范围」注）。
6. 可观测性导出层（OTel 等；`broker_metrics:snapshot/0` 之上）。
7. `require_auth` 尚未接进 MQTT 的 CONNECT 路径（`mqtt_conn_fsm` 不调用 `auth_required/1`，见 §2.2 现状边界第 2 条）；`broker_meta` 的 `broker_user` 与数据面认证的接线（ADR-021 §D7 冻结后补充）；`topic_meta` / `queue_meta` / `binding_meta` 三张表的运行时读取方。
8. **MQTT-SN：明文 UDP 默认 + DTLS-PSK 可选（2026-11-17 落地）**：`broker_security.tls.enabled` 仍不作用于 1884（该端口不用证书体系）；listener spec `transport => dtls` 启用 S11-C 模式的 DTLS-PSK——**身份即用户名**（`broker_security.mqttsn_psk_for_identity/1`，label `unifiedmq-mqttsn-psk`，realm 在 `broker_security.mqttsn #{realm => ...}`），握手证明的 identity 就是 principal、tenant 来自用户记录，ACL 生效；users 表为空时 DTLS listener 拒绝启动；明文 `transport => udp`（默认）行为不变，`require_auth` 含 `mqtt_sn` 仍整体拒绝启动。DTLS 断开 = 异常死亡（Will 触发）。边界与 §12.6 同口径：PSK-only 无证书、无会话恢复、握手串行、ADVERTISE 广播仅明文 listener、DTLS 证据为仓内 CT（mqtt-sn-tools 无 DTLS）。
9. **MQTT over QUIC 的 0-RTT early data 与多流模式未实现**（S8-D 只做 CN Draft 3 的单流模式）：0-RTT 是 CN 的可选项，本轮不接收；多流是 EMQX 私有扩展，不在规范内。QUIC 本身**强制 TLS 1.3**（无明文模式），证书来源见 §9 与 `docs/operations.md` §1.2——`tls.enabled` 对它是「证书来源」而不是开关。另：quicer 0.4.12 不支持运行期补充流信用，`max_streams_per_conn` 是建连时的一次性配额。

六协议统一走 `broker_security` 身份表已于 2026-11-09 完成，见 §2.1，不再列为待办。

### 12.1 S9-F（2026-11-12）明确未实现项

S9 三协议（A2A / MCP / ACP）接入前端后，以下项**仍未实现**，逐条留痕（不作为 Gate 依据，`progress.md` 有同一份清单）：

**MCP（`broker_proto_mcp`）**

1. 无 `sampling/*`（server→client 反向采样）、无 `elicitation/*`：一律 `-32601`（§4.17.1），网关不代理客户端侧能力。
2. 无 GET 流 / `Last-Event-ID` / 会话续传：2026-07-28 无状态，每请求自带版本与能力（§4.17.8）。
3. `Mcp-Param-*`（源自 `x-mcp-header`）不强制：网关没有后端 `inputSchema`，无法判断该不该出现，只按「accepted, not enforced」记录（§4.17.8）。
4. SSE 无 pending-bytes / send-timeout 闸门：慢客户端靠 socket 缓冲自然反压（ADR-025 §D7 的 `sse_max_pending_bytes` 未落地，§4.17.8）。
5. stdio 无认证：身份固定为匿名本地身份，信任边界是「谁能启动这个进程」；stdout 纯净性靠部署把 logger 指向 stderr（§4.17.8）。
6. 工具名 / 参数不校验：网关只透传不解释（R-B1），校验在后端；S9-F 官方 SDK 证据里 `call_tool("does-not-exist")` 因此**透传成功**，这是设计而非缺陷（§4.17.9）。
7. 无 GET / `initialize` 握手（2026-07-28 无该概念），未广告 feature 下的方法一律 `-32601`（§4.17.1）。

**ACP（`broker_proto_acp`）**

8. 仅 stdio：无 HTTP / WebSocket listener、无 `listeners` 配置项、无远程建连（§4.18.6）。
9. `fs/*`、`terminal/*`、`elicitation/*` 全回 `-32601` 且**保持会话**（Broker 不代理文件/终端/提权副作用，§4.18.1/§4.18.6）。
10. 无沙箱：agent 子进程由运维用绝对路径 + 白名单 env 启动（`acp_port` 不经 shell），但 Broker **不**对 agent 的文件/网络访问做任何隔离——隔离是部署（容器/用户）的职责。
11. `clientCapabilities` 恒为 `{}`，不做 `session/load` / `resume` / `list` / `authenticate`（v1 无鉴权方法，§4.18.1）。
12. `request_permission` 的 wire 形状按 ACP v1 嵌套 `outcome` 发送（`acp_policy:wire/1`，§4.18.8.1）；**v2 未实现**（ACP v2 仍 WIP，`acp/experimental/v2` 不在交付范围）。

**A2A（`broker_proto_a2a`）**

13. 无跨节点：Task 投影在本地 shard 的 ETS，非 owner 节点访问同一 Task 没有重定向/代理（§4.15.7/§4.16.7）。
14. 无 JWT / JWKS / OAuth Authorization Server：`securitySchemes` 只**声明**外部 scheme，card 不做 JWS/PKI，push 密钥在 env 而非 KMS（收敛报告 §13 非目标，§4.15.7）。
15. push 密钥来源是进程环境变量（`push_secret_env`），无 KMS/HSM 集成、无密钥轮换钩子（§4.16.3）。

**三者共同**

16. 单节点、回环 listener（18094/18096 绑 `127.0.0.1`，且默认 `listeners => []` 不监听）。
17. **失败路径计数器未加**：`broker_metrics` 的键集仍是 S9-A 之前的冻结集合；`a2a_state_violation` / `a2a_task_history_lost` / `a2a_checkpoint_corrupt` / `a2a_push_dlq` / `a2a_stream_*` 以及 MCP / ACP 侧的失败事件**只 `logger`**，不进指标。**S9-F 复核后的最终结论是不加**：`broker_metrics` 的冻结键集（ADR-016）不为这三个前端的失败路径扩容，运维按日志特征盯（`docs/operations.md` §告警占位）。
18. 压测/容量证据（`docs/benchmarks/2026-11-12-s9f-capacity.md`）跑在 tmpfs 数据目录 + `require_durable_fs => false` 的证据节点上，只覆盖单节点回环、无 fsync 的端到端往返；**交付配置（ext4 WAL、多节点、公网 TLS）的容量数字需另行测量**，本报告不声称 ext4 持久化性能。

### 12.2 S10 信令面明确未实现项（S10-A 2026-11-13 立，S10-B 同日更新）

S10-A 只交付**信令连接底座**（`broker_proto_webrtc` 的 WSS 接入层），S10-B 补上房间与信令内核。下表逐条留痕当前真实状态（不作为 Gate 依据，`progress.md` 与 `docs/design/s10-execution-plan.md` §8 有同一份口径）：

~~1. **没有房间**：无 `webrtc_room_registry`、无成员表、无 `joined`/`left` 事件。两个已认证客户端之间**无法互相发现**，S10-B 才建注册表。~~
   **已由 S10-B 实现（2026-11-13）**：`webrtc_room_registry`（`phash2(Room,N)+1` 分片 supervisor + 每分片 protected ETS）+ `webrtc_room_shard`；`joined`/`left` 广播集合由 `webrtc_signal_SUITE`（13 例）与 `webrtc_room_SUITE`（14 例）钉死。房间成员是**连接态**、只在内存（ADR-028 §D4），重启后清空是正确状态。

~~2. **没有 `to` 转发**：已认证连接发来的带 `to` 的消息一律回 `invalid_message`（诚实地说「现在不能路由」，不静默丢弃）。跨房间越权因此在本阶段**不存在可被利用的路径**——不是「已修好」，而是「还没这条路径」。~~
   **已由 S10-B 实现**：`webrtc_dispatch:deliver/3` + `relay/3` 进程直投（`lookup/2` + `Pid ! {text, Bin}`），原报文 + 覆盖式 `from`、保留 `to`；`webrtc_route_SUITE` 14 例覆盖 SDP/ICE 字节透传、缺 `to`/空 `to`/未知 `to`/缺 `event` 一律 `invalid_message`、跨房间与未知响应**逐字节相同**（不做 peer 存在性预言机）、伪造 `from` 被覆盖。**S10-D D.3 再补 3 例（共 17 例）**：`to` 为非二进制（JSON 数字/布尔/null/对象）→ `invalid_message`（`deliver/3` 只用 `is_binary/1` 判 `to`，类型混淆穿不过寻址层）、单连接连拒 50 条后合法信令仍正常路由（持续滥用退化探针）、把自己的 `peer_id` 当 `to` → **回环投递一次**（注册表把自己解析到自己的 pid，帧原样写回、`from`=自身；因 `{text, Frame}` 只写出不再转发，恰好一次、无循环、无放大——如实留痕为「不拒绝、良性」而非缺陷）。

~~3. **不做 ACL 判定**：`broker_security:authorize/2` 未被调用。身份只用于认证（`peer_id` 铸造）与租户归属，没有 destination 级策略。ADR-028 §D5 规定将来 ACL 的 destination 只作模式主体。~~
   **已由 S10-B 实现**：join / relay / 服务端 API 三条路径都过 `broker_security:authorize/2`，主体 `#{action => signal, tenant_id, destination => "$webrtc/room/<room>"}`（`destination` **只作 ACL 模式主体**，不进 `broker_router`）。join 拒 → `unauthorized` + close 1008；relay 拒 → `invalid_message`（冻结五类事件无第六类）。`broker_security` 的既有语义**一字未动**。`webrtc_acl_SUITE` 13 例（含 `data`/`event`/消息体 `tenant_id` 都不能绕过 ACL）。**已知简化**：join 与 relay 共用同一主体，因此 ACL 只能对整个房间允许/拒绝，无法「允许 join、拒绝 relay」。

4. **`peer_id` 无跨连接语义**：服务端用 `base64(strong_rand_bytes(10))` 铸造 16 字符 ID，**只为本次连接有效**；重连即换新 ID，无法据此做稳定寻址或审计关联。S10-B 未改这条——`to` 只在同房间、同连接生命周期内可寻址。

5. **无失败路径计数器，也无线协议层日志**：沿用 §12.1 第 17 条的口径，`broker_metrics` 的冻结键集不扩容；`webrtc_conn` / `webrtc_listener` / `webrtc_room_shard` / `webrtc_dispatch` **一次 `logger` 调用都没有**——握手失败、`unauthorized`、`invalid_message`、认证窗口/空闲超时、close 1003/1009/1002、房间满、ACL 拒、未知 `to` 全是**纯状态转移**，不上报。信令面的运维可见性只有 ranch 自身的 accept 计数（`docs/operations.md` §1.6 的排查段）。加日志字段属 S10-D 的 D.4（`room`/`peer_id`/`bytes`，永不记 `data`）。
   **S10-D D.4 已补（2026-11-14）**：`webrtc_conn` 现在发出结构化、**脱敏**的 `?LOG_INFO`（`logger` 直接调用，无 `lager` 运行时依赖）：`authenticated` / `auth_refused`（带 `reason` 粗粒度原子）/ `relay`（`room` + `from` + `bytes`，**永不记 `data`**）/ `signal_refused` / `disconnect`。`broker_metrics` 键集**仍不扩容**（D.4 只加日志、不加指标）。`webrtc_route_SUITE:the_log_carries_room_peer_and_bytes_but_never_the_payload/1` 用 logger handler 捕获全量 debug 日志，断言一条真实 relayed offer 的 SDP sentinel 与口令**都不出现**、而 `room=` / `from=` / `bytes=` 与发送方 `peer_id` 出现。traceparent 仍由 `broker_ws` 在门口按 opt-in（默认 off）接收，连接日志不重复发射；失败路径仍**不进指标**。

6. **`require_auth => false` 是运维开关，不是安全边界**：只在所有 listener 都绑回环时被接受，且它让**任何**能连上该回环端口的人无需凭据即取得 `peer_id`。S10-B 下匿名连接的 ACL 主体是 `principal => <<"anonymous">>, tenant_id => undefined`（填默认租户会因不匹配把全部信令拒掉）。这与 A2A/MCP 的同一开关同姿态，适用于单机开发，不适用于共享主机。

7. **无消息大小之外的反滥用闸门**：只有 `max_frame_bytes`（单帧 262144）、`max_connections`（每 listener 1024）与 `max_peers_per_room`（每房间 2）。**没有速率限制**、没有每 `peer_id` 的令牌桶、没有慢速攻击缓解；`auth_timeout_ms` 只挡「一直不认证」，`idle_timeout_ms` 只挡「认证后长时间不做事」，两者都只关单连接。**房间数没有上限**（房间按 URL 路径即建即用，空房间在最后一个成员离开时消失，但并发房间数不受限）。**S10-D D.3 行为探测的诚实结论**：(a) 单连接持续滥用已被 `webrtc_route_SUITE:fifty_refused_signals_still_forward_a_valid_one` 钉住——连拒 50 条后合法信令仍正常路由，说明每条拒绝是**纯状态转移**、不泄漏累积状态（不会因拒绝计数增长而最终停滞或崩溃）；(b) **没有「JSON 深度」专项防御也不需要**：`json:decode` 对深嵌套不栈溢出（实测 40000 层 240001 字节秒回、2000 层嵌套数组走 `not_a_map` → `invalid_message`），输入大小由 `max_frame_bytes` 在帧层兜住（超限 → 1009），因此深度炸弹不构成 DoS 向量——`webrtc_conn_SUITE` 的 `a_deeply_nested_json_frame_is_invalid_message`（深数组 → `invalid_message`、连接存活、ping → pong）与 `an_oversized_to_field_closes_with_1009`（超大 `to` 撑爆帧 → 1009）分别钉住这两条路径。

8. **TLS 只走 `broker_security.tls`**：没有按 listener 覆盖证书的路径（与 A2A/MCP 一致）；没有客户端证书/mTLS 身份。
   > **2026-11-25 批注**：TCP/TLS listener 的 **mTLS 握手**已具备端到端证据（`mqtt_tls_SUITE` 的 `mtls_with_client_cert_completes_the_handshake` / `mtls_rejects_client_without_cert`：`verify_peer`+`cacertfile`+`fail_if_no_peer_cert` 下 CA 链证书握手成功、无证书客户端被 fatal `certificate_required` 告警拆链）。**但本条结论仍成立**：mTLS 是 opt-in（默认 `verify_none`），且**客户端证书尚未映射到 `principal`/租户**（握手成功不等于建立身份），故「没有 mTLS 身份」的口径不改；distribution 的 mTLS 已由 ADR-035 交付，QUIC 强制 TLS1.3（S8-D）、CoAP 仅 DTLS-PSK 无证书（S11-C）各自独立。

~~9. **没有真实第三方客户端证据**：`test/protocol-compatibility/s10/` 的 `aiortc` / 浏览器 `RTCPeerConnection` 证据仍推到 S10-D。现有证据是 CT 里的字节级客户端（`webrtc_conn_SUITE` 31 例 + 四个 S10-B 套件 54 例）。~~
   **已由 S10-D D.1 交付（2026-11-14）**：`test/protocol-compatibility/s10/` 下 aiortc 1.13.0 两 peer 走完 offer/answer、`connectionState == "connected"`、`RTCDataChannel` 载荷往返一致。**诚实口径**：第三方实现是 aiortc **不是浏览器**（环境无 headless Chromium，已在 README 标注）；**逐项限制清单已成表**（S10-D D.5/D.8：[`docs/protocol-compatibility.md`](protocol-compatibility.md) §4.23 能力/限制总表 + §4.23.5 非目标逐行留痕 13 行）。

   **同批还补上了「不落盘」的运行时证据（D.2，2026-11-14）**：原先只有 `webrtc_boundary_SUITE` 这个**源码级**守卫（它只能证明 `src/*.erl` 里没有 `broker_queue:` 之类的调用点，证明不了运行时一个字节都没写——写可以从依赖里绕进来）。`run_no_persist.sh` 在一个 VM 里起 WAL 存储 + **活的** `broker_subscription`（其 100 ms cursor tick 是唯一会写 `.cursors` 的东西）+ 两个生产监听，seed 一条消息后跑完整 1:1 aiortc 通话，断言 `offsets`（`broker_queue:watermarks/1` 逐分区 `{Committed, High}`）/ `wal_bytes`（`data_dir/wal` 全部普通文件字节和）/ `cursors`（`*.cursors` basename 集合）**三者逐字段不变**（实测 117 → 117 字节、`moved: []`）。**两条负控**证明检测器可证伪：一次 append 使 `{P0,0,2}`→`{P0,0,3}` 且字节 117→228；真订阅 + ack 使 `.cursors` 出现。**边界**：只在这一次运行、这一个节点上成立，不替代源码级扫描，也不覆盖其他 app 的写路径。

~~10. **未做压测 / 容量**：连接数 × 房间数 × 信令速率的分档数字都还没有（`docs/benchmarks/` 无 S10 文件）。S10-B 只证明了语义正确，不证明容量。~~
   **已由 S10-D D.6 交付（2026-11-14）**：[`docs/benchmarks/2026-11-14-s10-capacity.md`](benchmarks/2026-11-14-s10-capacity.md) + 15 个原始 JSON。信令 1–32 房（2–64 连接）顺序/并发 × 速率扫描 10–500/s + 不限速，32 连接并发 1498.7/s（p50 1.6 ms）；ICE allocation 顺序 50/50（p50 893 µs）+ 并发 12/12（p50 3.2 ms）；relay ChannelData 突发 5000 帧 0.77 送达（~5.2k datagrams/s）。**诚实边界**：单节点、回环、tmpfs、非浏览器、非 RTP 媒体面，数字量级可信但不可外推（报告 §5）。

~~11. **未做 WebSocket 分片重组**：`ws_frame:decode/2` 会把 `fin => false` 的帧照实返回（分片是「暴露给连接进程」的，见该模块注释），但 `webrtc_conn:on_frame/2` 不看 `fin`——首片直接当作完整文本送进 `handle_text/2`，`opcode 0` 续帧落到兜底子句被静默忽略。后果：分片发送的 `authenticate` 只会被判 `unauthorized`，分片发送的信令一律 `invalid_message`（不会误路由、不会崩溃），而对照的 `ws_conn` 与 `mqtt_transport_ws` 都做了重组。浏览器信令客户端默认不分片（一个 SDP 远小于 `max_frame_bytes`），所以本阶段不阻塞；S10-B 房间转发落地时一并补，避免把「半条消息」当完整消息路由。~~
   **已由 S10-B 补齐（2026-11-13）**：`webrtc_conn` 增加 `frag = {Op, Acc, Size}` 状态，`fin = false` 的数据帧开一条消息（opcode 决定 text/binary），`opcode 0` 续帧按首帧 opcode 累积，`fin = true` 的续帧才 `handle_message/3`；**无起始帧的续帧、开消息期间又来新的数据帧 → close 1002**；重组过程同样受 `max_frame_bytes` 约束（超限 → 1009）。**客户端 close 帧现在也回显**（同 code；1005/1006/1015 与越界值统一回 1000，RFC 6455 §7.4.1）。`webrtc_conn_SUITE` frames 组 27→31 补 4 例：分片 `authenticate` 重组成功、孤儿续帧 → 1002、close 回显、保留码 → 1000。

~~12. **无 STUN / TURN（ICE 面）**：3478/UDP、TURN 长期凭据、relay 端口段全部属 S10-C（独立 app `broker_ice`，与信令互不依赖）。~~
    **已由 S10-C 实现（2026-11-14）**：独立 app `broker_ice`，STUN Binding（**无认证**——它就是浏览器在拿到凭据前探测 reflexive 地址用的）与 TURN over UDP 全套（`Allocate`/`Refresh`/`CreatePermission`/`ChannelBind`/`ChannelData`/`Send`/`Data`），长期凭据从**同一张 `users` 表**派生（`key = MD5(user ":" realm ":" password)`，新增 `broker_security:long_term_key/2`），ACL 走 `authorize/2`（新增 `turn_allocate`/`turn_refresh`/`turn_permission`/`turn_channel`/`turn_send` 五个 action）。出厂 `enabled => false` **零 socket**。ICE 面自己的未实现项见 §12.3。

13. **S10-D D.3 安全专项的行为探测（2026-11-14）**：对信令面与 ICE 面做了一轮「先探测真实行为、再冻结用例钉住」的安全专项。**实现侧未发现需修补的缺陷**，探测结论全部如实留痕（含良性行为与不构成风险的边界）：
    - **握手头无读超时（缺口，如实留痕）**：`webrtc_conn` 在握手阶段（`ws_handshake:accept` 无超时 + `{active, once}` 只武装一次）**没有读超时**——连上后一直不发包、或只发半截握手头，连接不会被关闭（实测静置 8 s 不关）。`auth_timeout_ms`（默认 5000）**只在 101 之后**的认证窗口生效，挡不住「占着连接不发完整握手」。后果：一个未认证的攻击者可用半开连接占用 ranch acceptor 槽位，直到 TCP 层超时。**这是真实缺口**，但修补需在 `webrtc_conn` 握手路径加超时（动既有实现），按 S10-D「不改既有语义、偏差须记」的纪律，本阶段只留痕、不擅自加；`max_connections` 仍是总量兜底。`webrtc_conn_SUITE` 未为「握手前静置」加用例（避免把当前行为钉成契约），改在本文档与 §8 偏差记录留痕。
    - **self-loop 不拒绝（良性，已留痕）**：见第 2 条 D.3 补充——把自己的 `peer_id` 当 `to` 会回环投递一次，不崩溃、不放大、不循环，属良性行为而非可利用缺陷。
    - **深嵌套 JSON / 非法 UTF-8（无风险，已钉住）**：见第 7 条 D.3 补充——`json:decode` 不栈溢出，输入由 `max_frame_bytes` 兜底，深数组/非法 UTF-8 一律 `invalid_message` 且连接存活，已分别由 `webrtc_conn_SUITE` 两例钉住。
    - **握手头超 16 KiB → 431（已覆盖，非新增）**：`ws_handshake:max_handshake()=16384`，握手头超限在 `on_handshake` 回 `431 Request Header Fields Too Large` 并 `stop_now`；既有 `broker_ws` 层测试与本次探测一致，信令面不另立用例（行为属 `broker_ws` 既有契约）。
    - **TURN 侧的 D.3 探测**见 §12.3 第 9 条。

### 12.3 S10 ICE 面明确未实现项（S10-C 2026-11-14 立）

S10-C 交付 STUN Binding + TURN over UDP。下表逐条留痕本阶段**真实没有**的东西（不作为 Gate 依据，`docs/design/s10-execution-plan.md` §8、§2.3 与 ADR-029「落地偏差记录」有同一份口径）：

1. **只做 UDP，不做 TURN over TCP / TLS / DTLS**：`Allocate` 的 `REQUESTED-TRANSPORT ≠ 17` 直接回 **442**（不是静默降级）。后果：只放行 TCP 出站的企业网络里 TURN 不可用；UDP 被限速时也没有回退路径。5349 是 ADR-029 §D2 明确排到 P2 的项，不允许实现期悄悄补。
2. **长期凭据的派生密钥在明文 UDP 上协商**：`MESSAGE-INTEGRITY` 提供**完整性**（口令本身不过线），但**链路没有加密**——同网段的被动嗅探者能看到 username、realm、allocation 元数据与中继流量。这是「不做 5349」的直接后果，ADR-029「后果」段已冻结：跨公网部署需自行评估。
3. **无 IPv6 候选**：`turn_ip` 必须是 IPv4 四元组，否则启动失败（`{invalid_turn_ip, V}`）；`XOR-MAPPED-ADDRESS` / `XOR-RELAYED-ADDRESS` / `XOR-PEER-ADDRESS` 都只编 IPv4。IPv6 网络里 TURN 不可用（STUN Binding 也回不了 IPv6 mapped address）。
4. **无 ICE-lite、无 ICE 状态机**：本 app 完全是 STUN/TURN 服务端，**不是 ICE agent**——不做候选配对、不做连通性检查、不选 pair。这些全在浏览器/`aiortc` 侧。
5. **单节点语义**：allocation、permission、channel、relay 端口都是节点内资源，无 ra、无 Mnesia、无跨节点归属（ADR-029 §D5）。多节点部署时同一客户端的 `Allocate` 会落到负载均衡器选中的那个节点，**没有**跨节点 relay 复用或迁移。集群化需另立 ADR。
6. **无速率限制与反滥用闸门**：只有 `max_allocations`（默认 1024）、`max_permissions_per_allocation`（16）、`max_channels_per_allocation`（16）与 `allocation_lifetime_s`（600 s）。**没有**每 IP / 每用户 的 allocation 速率限制、没有令牌桶、没有针对「伪造 5-tuple 刷 `Allocate`」的缓解（虽然每条都要过 `MESSAGE-INTEGRITY`，即攻击者必须先有一个有效账号）。relay 端口段是有限的，`508` 比限流更早出现。**`max_allocations` 是全局的，不是按用户分的**。
7. **不写指标、不写协议层日志**：沿用 §12.1 第 17 条与 ADR-016 冻结键集——`broker_metrics` 出现在 `.app.src` 的 `applications` 里但**代码零调用**；`broker_ice_udp` / `turn_allocation` / `turn_auth` **一次 `logger` 调用都没有**（认证失败、`438`、`441`、`486`、`508` 全是纯状态转移）。`turn_auth_SUITE:no_password_or_key_reaches_the_log/1` 用 probe 断言「即使将来加日志也不能带口令或 key」。运维可见性目前只有 `broker_ice_udp:allocations/0` 快照与 `ss -lunp`。
   **S10-D D.4 已补（2026-11-14）**：`turn_allocation` 现在发**两条**低量生命周期 `?LOG_INFO`——`allocated`（`client` 5-tuple + `relay_port` + `username`）与 `released`（同上 + 粗粒度 `reason`），**永不记 key / 口令**（`key` 只在进程状态里，从不进日志；`turn_auth` 仍是零 `logger` 调用，认证失败照旧纯状态转移）。`turn_auth_SUITE:no_password_or_key_reaches_the_log/1` 仍在（现也覆盖 `turn_allocation` 的生命周期行，全量 debug 捕获后仍断言口令/key 缺席）。`broker_metrics` 键集**仍不扩容**，per-packet 不记日志（只记建立/释放，避免高基数与地址泄漏）。
8. ~~没有真实第三方客户端证据 / 未做压测~~：**已由 S10-D 交付（2026-11-14）**——D.1 用 `aiortc` 1.13.0（经底层 `aioice` 设 `transport_policy = RELAY`）拿到 `typ relay` 候选且错凭据 0 候选，**非浏览器**（环境无 headless Chromium，见 `test/protocol-compatibility/s10/README`）；D.6 落盘容量报告（[`docs/benchmarks/2026-11-14-s10-capacity.md`](benchmarks/2026-11-14-s10-capacity.md)：allocation 顺序 50/50、p50 893 µs，并发 12/12、p50 3.2 ms，relay 突发 5000 帧 0.77 送达、~5.2k datagrams/s）。CT 侧证据仍是手写字节的 `ice_test_client`（`stun_codec_SUITE` 21 + `turn_auth_SUITE` 16 + `stun_server_SUITE` 13 + `turn_allocation_SUITE` 25）。**仍未做的**：浏览器 `RTCPeerConnection` 兼容矩阵、relay-to-relay 的 aioice 端到端（aioice 不发 `CreatePermission`，被 RFC 5766 §9 回 403，刻意不为它放宽）。

9. **S10-D D.3 安全专项的 TURN 侧行为探测（2026-11-14）**：对 `turn_allocation` 的边界做了一轮探测并补冻结用例。**实现侧未发现需修补的缺陷**，结论如实留痕：
    - **channel number 区间是闭区间（已钉住）**：`channel_of/1` 取 `>= 16#4000, =< 16#7FFF`。既有 `a_channel_number_outside_the_range_is_a_400` 钉住区间外两值（`0x3FFF`/`0x8000`）→ 400；D.3 新增 `channel_number_boundaries_are_inclusive` 钉住区间两边界值（`0x4000`/`0x7FFF`，各用独立 peer）→ `{ok}`，防未来 off-by-one 悄悄收窄可用区间。**注意一个真实约束**：一个 peer 只能绑一个 channel（RFC 5766 §11.2），同一 peer 绑第二个 channel 会得 400——所以两个边界值必须各用独立 peer 才能都验成 `{ok}`。
    - **畸形 LIFETIME（非 4 字节）→ 400 且 allocation 存活（已钉住）**：`a_malformed_lifetime_on_refresh_is_a_400_and_the_allocation_survives` 用 3 字节 LIFETIME 发 Refresh，得 400、`broker_ice_udp:allocations()` 不变、listener 仍在，且随后一个合法 Refresh 仍 `{ok}`——钉住「畸形属性不会拆掉 relay 或共享 UDP 前端」的健壮性。诚实口径：非 4 字节 LIFETIME 合规客户端产生不了，这是敌意/垃圾输入探针，不是 spec 分支。
    - **超大 UDP 报文不可达（如实留痕，不设用例）**：`recbuf_bytes=262144` 看着像上限，但**单个 UDP 数据报本身最大 ~65507 字节**（IP 20 + UDP 8 头后），且客户端 `gen_udp:send` 发更大的报就返回 `{error, emsgsize}`。所以 relay 前端的 recbuf 永远不会被单个合法数据报撑爆——这是 UDP 协议本身的边界，不是本实现能测到的路径，故不设「超大报文」用例，只在此留痕。
    - **permission 过期重放不可达（如实留痕，不设用例）**：permission 有效期 `?PERMISSION_LIFETIME_S=300 s`，要构造「过期后重放」需在 CT 里等 5 分钟，不现实；本地无法在不改实现（把有效期改短会动既有语义）的前提下触发该分支，故不设用例，只在此留痕。
    - **`Refresh` lifetime 越界（已覆盖，非新增）**：`99999 → 600` 钳制已由既有 `a_refresh_keeps_the_allocation_alive` 钉住；负数/非整数 lifetime 在 wire 上不可达（`stun_codec:lifetime/1` 解的是 32 位无符号，永远是 0..4294967295 的整数，`refresh/2` 的 `case` 已覆盖 0 与正整数），畸形长度走上面的「畸形 LIFETIME」分支。

### 12.4 S11-A CoAP 面明确未实现项（2026-11-15 立）

S11-A 交付了 CoAP（RFC 7252）的请求/响应面：自写编解码、资源映射、UDP listener。下表逐条留痕本阶段**真实没有**的东西（不作为 Gate 依据；`docs/protocol-compatibility.md` §4.24 是同一份事实的协议口径，ADR-031 是本期的冻结合同）。

1. **无 DTLS**：只有明文 UDP（默认 5683）。`principal` 恒 `undefined`，`broker_security:authorize/2` 只在 `acl => allow` 下放行；没有 PSK / 证书 / 自签路径（ADR-031 §D5 明确「默认不自签」，接线在 S11-C）。后果：CoAP 端口与 1883 一样是明文端口，跨不可信网段部署需自行封装。
2. **无 Observe（RFC 7641）**：`observe` option 能解析，但没有注册、通知、取消。ADR-031 §D2 已冻结做法（归 `broker_subscription` + cursor，禁止 `pg2` 扇出、禁止把 `{Port, IP}` 当会话键），实现留 S11-B。后果：CoAP 客户端拿不到推送，只能轮询 GET。**S11-B 已交付（2026-11-15）**：注册/通知/取消已落地，本条对 S11-A 时点仍然成立；S11-B 未覆盖的部分见 §12.5。
3. **~~只有 Block1（请求方向）~~ → Block1 + Block2 均已实现（2026-11-20 更新）**：原状是「服务端不做 Block2 分块响应，响应整条一个数据报」，因此**实际端到端上限约 1 KB，不是 `max_message_bytes`（65535）**——真实客户端（libcoap v4.3.5，默认接收 PDU 1500 B）会丢弃超过约 1.1 KB 的响应（`coap_pdu_resize: pdu too big`），一个 2 KB 的 retained payload 写得进去、读不回来。**2026-11-20 已交付服务端 Block2 分块响应**（RFC 7959 §3）：GET 带 `block2:{Num,_,ReqSize}` 时按客户端请求的块大小切片回送，>1.1 KB 的表示能写也能分块读回；不带该选项的 GET 行为不变（仍是整条一个数据报）。**仍不做**：跨块响应缓存/重传、Observe 通知的分块推送（见 §12.5 第 3 条）。证据：`coap_block2_SUITE` 5 例（真实 UDP），限制口径见 `docs/protocol-compatibility.md` §4.24.4。
4. **Block-wise 已按 RFC 7959 校正，不再是「草案对齐」**：原计划按 `draft-ietf-core-block-18` 实现（ADR-031 §D7），libcoap 实测推翻了这一口径——`2.31 Continue` 的块号语义按 RFC 7959 §3.2 改为回**收到的** `Num`，分块流键按 RFC 9175 §3.1 用 `Request-Tag`（缺省退回 token）。**与 libcoap v4.3.5 已实测互通**（`test/protocol-compatibility/s11c/libcoap/`，8/8 PASS，见 `docs/protocol-compatibility.md` §4.24.9.1）。~~**Block2 仍未实现**，这是真限制，不是口径问题。~~ **（2026-11-24 更正）Block2 已于 2026-11-20 交付**（RFC 7959 §3，见上一条与 `coap_block2_SUITE` 5 例），此处原句已过期；**仍不做**的只是跨块响应缓存/重传与 Observe 通知分块（§12.5 第 3 条）。
5. **无 multicast、无 CoAP over TCP/WebSocket（RFC 8323）、无 OSCORE、无 `If-Match`/`ETag` 条件请求**：均在 ADR-031 的非目标或后续阶段里。
6. **限流不识别身份，且源地址可伪造**：令牌桶按 `{IP, Port}` 计（`max_requests_per_second` 默认 100），`max_endpoints`（默认 4096）满时**拒绝新来源**而不是驱逐已有桶——这是抗伪造源地址洪泛的取舍，代价是 NAT 后的多个客户端共享一个桶。
7. **不写 CoAP 专属指标、无协议层日志**：沿用 ADR-016 冻结键集，只 inc 既有 `bytes_in` / `frame_decode_errors` / `protocol_errors`；没有「哪个目的地被写了几次」这类观测面，需要时只能看内核层的 retain/queue 统计。
8. **单节点**：listener 的三张表（dedup / 令牌桶 / Block1）全在进程内存、不持久化，也不跨节点；retained 与日志语义完全由既有层决定。（S11-B 又加了 `observes` / `pending` 两张，同姿态，见 §12.5 第 1 条。）

**S11-C 收口（2026-11-16）**：上面第 1 条「无 DTLS」已关闭（§4.24.8 交付 DTLS-PSK）；第 6 条「限流不识别身份」在 `coaps` 上已有身份（ACL 生效），但明文 UDP listener 的令牌桶仍按 `{IP,Port}` 计；第 2 条 Observe 已由 S11-B 关闭；~~第 3 条（无 Block2 ⇒ 响应实际上限约 1 KB）~~ **第 3 条已于 2026-11-20 关闭**（服务端 Block2 分块交付，见该条更新）；第 5 条（无 OSCORE/multicast/8323）仍成立，第 4 条已由 libcoap 实测校正为 RFC 7959 口径（不再声称对齐草案）；第 7 条「不写 CoAP 专属指标」仍成立（ADR-016 冻结键集）；第 8 条「单节点 / 不持久化」在 DTLS 会话表上同样成立（§12.6 第 4 条）。

### 12.5 S11-B CoAP Observe 面明确未实现项（2026-11-15 立）

S11-B 交付了 Observe（RFC 7641）：`Observe=0` 注册、内核 dispatch → 通知、`Observe=1`/RST 注销，全部映射到 `broker_subscription` + ADR-013 cursor。下表逐条留痕本阶段**真实没有**的东西（不作为 Gate 依据；`docs/protocol-compatibility.md` §4.24.4/§4.24.6 是同一份事实的协议口径，ADR-031「落地偏差记录」是本期的冻结落点）。

1. **观察关系不跨节点、也不跨崩溃恢复**：`observes`（关系表）与 `pending`（未确认 CON 通知）都在 `coap_listener` 的 `gen_server` 状态里，listener 重启（或 one_for_all 1/5s 重启整棵子树）即空。对端**收不到任何错误**——只会在 `Max-Age` 窗口内静默无通知，需要自己重新 `Observe=0`。多节点部署下客户端被负载均衡到另一节点时同理：另一节点没有这条关系。**没有** ra/Mnesia 参与、没有会话迁移、没有重放。后果：这不是「可靠订阅」，是可丢的实时推送。
2. **通知不是 QoS 1（也不该被当 QoS 1 用）**：CON 通知只有 **datagram 级**重传——`observe_ack_timeout_ms`（默认 2000 ms）× `observe_max_retrans`（默认 3）为上限，耗尽即放弃关系；**不落盘、不在内核重投、不跨重启、不提供「恰好一次」**。NON 通知**至多一次**，丢了就是丢了。两者都**不保证**送达（ADR-031 §D3 冻结口径）。需要业务级可靠投递请走 MQTT QoS 1/2 或内核 cursor 重放，不能靠 Observe。
3. **~~服务端不做 Block2 分块~~ → 响应可分块，但通知仍不分块（2026-11-20 更新）**：**服务端 Block2 已交付**（RFC 7959 §3，见 §12.4 第 3 条）——带 `block2` 选项的 GET 能分块读回大表示。**但通知（`Observe` 主动推送）仍然不分块**：`coap_observe:notify_message/4` 是**单数据报**，单条通知超过客户端接收上限仍没有兜底路径，只能等 `Max-Age` 过期让客户端重新 GET（此时那个 GET **可以**带 `block2` 分块读回，这是新增的兜底）。**单条通知的上限约 1 KB，不是 `max_message_bytes`（65535）**：libcoap v4.3.5 默认接收 PDU 1500 B，超过约 1.1 KB 的响应被丢弃（§4.24.4 实测）。Block1 已按 RFC 7959 / RFC 9175 校正并经 libcoap 实测（§4.24.9.1），与本节无关。
4. **无 OSCORE、无 DTLS**：报文与通知都是明文 UDP；`Observe` 的载荷与序列号在链路上可读、可改。D5 的 DTLS 接线留 S11-C。**没有**重放保护、**没有**消息完整性。
5. **plain UDP 下 `principal = undefined`，ACL 无法拒绝一条 datagram**：`broker_security:authorize/2` 对匿名主体在 `acl => allow` 时直接放行；把 `coap`/`coaps` 列进 `require_auth` 会让 app **拒绝启动**（`coap_app_SUITE` 的 `require_auth_for_coap_is_refused` / `require_auth_for_coaps_is_refused` 钉住）。因此 **S11-B 唯一的 4.03 拒绝路径是容量**（`max_observes` 满），`observe_acl_denied` 这条用例因此在落地时**降级**为 `observe_plain_udp_principal_is_allowed`（如实钉住「ACL 受限部署下仍然可观察」）。基于 ACL 的 4.03 要等 S11-C 的 DTLS 客户端身份。
6. **`observe_max_age_s` 是兜底窗口，不是新鲜度保证**：每关系取客户端 `Max-Age`（缺省 30 s）并钳到 `[1 s, observe_max_age_s]`；钳制的直接后果是——客户端请求 `Max-Age: 0` 拿不到「立刻失效」，请求 `Max-Age: 604800` 也拿不到一周的保活（上限 30 s）。静默超过窗口的关系在下一次 1 s tick 被清扫，**清扫是单向的、没有通知对端**。客户端靠 `Observe` 序列号无法区分「关系被清扫」与「资源没变」，只能靠超时自己重注册。
7. **NSTART=1 意味着慢客户端会丢中间通知**：同一关系的新 CON 通知**替换**仍未确认的旧通知（RFC 7641 §4.5.1），这是 `pending` 有界的代价。客户端如果持续来不及 ACK，看到的是「最新一条」，不是「全部一条不丢」——这与 NON 的至多一次在语义上一致，不是缺陷，但**不能**把 Observe 当消息队列用。
8. **共享匿名会话带来的观测面缺口**：所有观察共用一个 `<<"coap">>` ClientId，因此 `broker_subscription:subscriptions/1` 里看不到「是哪个 UDP 端点在看哪个目的地」；`max_observes` 是**全局**上限，不是每用户/每 IP。没有 CoAP 专属指标（ADR-016 冻结键集），没有观察关系的生命周期日志——运维只能靠 `coap_listener` 的进程状态与内核层 retain/queue 统计间接判断。
9. **`max_observes` 满时只拒绝新的 `Observe=0`**：刷新一条已有观察（同 token re-GET）**不**占新槽、**不**会被拒；也就是说一个已经建立的关系无法被容量压力驱逐，只有 `Max-Age`/重传耗尽/主动注销能让它退出。占满表的攻击者优先于后来者——这是有界表「拒绝新来源而不是驱逐已有」的一贯取舍（与 §12.4 第 6 条同姿态）。

**S11-C 收口（2026-11-16）**：上面第 4、5 两条已被 S11-C 部分关闭——`transport => dtls` 的 listener（`coaps`）现在有真实客户端身份（PSK identity → `principal`/`tenant`），因此 **ACL 在 `coaps` 上真正生效**（`acl_denies_on_dtls` / `tenant_isolation_on_dtls`）；`require_auth` 也不再一刀切拒启，而是分协议门控（`coap` 拒启、`coaps` 允许，见 §12.6）。**未关闭的部分仍成立**：明文 UDP listener 上 `principal` 恒 `undefined`、ACL 无法拒绝一条 datagram；Observe 依旧无跨节点/崩溃恢复，通知依旧不是 QoS 1；~~服务端依旧不做 Block2~~（**2026-11-20 已交付服务端 Block2**，但**通知仍不分块**，见第 3 条更新）；OSCORE 未做。DTLS 面自身的未实现项见 §12.6。

### 12.6 S11-C DTLS（`coaps`）面明确未实现项（2026-11-16 立）

S11-C 交付了 CoAP over DTLS 1.2（PSK-only）：真实 PSK 客户端握手、身份 → `principal`/`tenant`、ACL 生效、会话/每主体双上限、关闭清理、诱饵密钥挡用户名枚举。下表逐条留痕本阶段**真实没有**的东西（不作为 Gate 依据；`docs/protocol-compatibility.md` §4.24.8/§4.24.9 是同一份事实的协议口径，ADR-031「落地偏差记录」是本期的冻结落点）。

1. **只有 PSK，没有证书，也没有 mTLS**：`ssl:listen` 不加载任何证书，服务端从不证明自己，客户端凭 `psk_identity` + 共享密钥证明自己。**没有**证书链校验、**没有**双向证书、**没有**按证书 SAN/CN 做的主体映射、**没有** CRL/OCSP。想用 X.509 的设备接不进来——这是范围决定（D5 只做 PSK），不是缺陷；要证书需另开阶段，且 `broker_security_tls`（TCP TLS 的证书配置）不被复用（realm 放在独立的 `broker_security.coaps`）。
2. **一个 listener 一份 realm，没有 per-listener 多租户 PSK 域**：`coaps` 配置是单个 `#{realm => binary()}`，所有 `coaps` listener 共用一个 realm（除非分别起在不同节点/不同 app env）。PSK 派生绑定 `(Identity, Realm, Password)`，换 realm 等于换整张 PSK 表；**没有**按 listener 配不同 realm 的能力。
3. **串行握手是硬上限（约 41 次/秒）**：一个 acceptor 进程串行 `transport_accept → handshake`。原因是身份归属——PSK identity 只在 `user_lookup_fun` 的同步回调里可见，并发握手时无法判断一条 identity 消息属于哪条连接（实测会串身份）。**直接后果**：同时冷启动的设备会落入 DTLS 客户端重传退避（1/3/7/15 s），实测同时 32 个仅 5 个在 20 s 内成功（错开 150 ms 则 32/32，容量报告 §3.5）。这是**连接延迟**而非请求吞吐（预热后零错误），但对「大批设备同时上线」是真实约束，运维必须错峰。**没有**并发握手、**没有** acceptor 池、**没有**握手排队公平性保证。
4. **会话上限是内存计数，不跨节点**：`max_dtls_sessions` / `dtls_max_sessions_per_principal` 都在 listener 的 `gen_server` 状态里；listener 重启（或 one_for_all 重启）即归零，多节点部署下各节点各算各的。**没有**集群级会话配额、**没有**会话迁移。
5. **没有会话恢复 / 重放窗口之外的保护**：DTLS 1.2 的会话票据（session ticket）与 session resumption 未启用/未配置；**没有**跨重启的会话保持（重启即要求全部客户端重新握手，而重连风暴见第 3 条）。DTLS 自带的重放窗口（record 层）仍在，但**没有** CoAP 层的重放保护、**没有** OSCORE——decrypted 之后的报文在 listener 内部仍可被重放到 dedup 窗口之外（CON 去重只覆盖 `dedup_window_ms`）。
6. **口令仍是共享秘密，没有前向保密语义上的承诺**：PSK 模式下握手的机密性依赖「口令不泄露」；一旦 `users` 表或其派生 PSK 泄露，历史流量在攻击者保存了密文时可被解密（无 (EC)DHE 的纯 PSK 套件尤其如此；本实现优先选 `ecdhe_psk`/`dhe_psk` 套件，但这是套件顺序而非强制）。**没有**密钥轮换 API、**没有** PSK 版本号/吊销列表——换口令需要客户端同步更新。
7. **诚实的边界：密码与 PSK 不落日志，但没有专门的 PSK 审计事件**：`redacted_config/0` 不暴露口令/PSK；`broker_security:credential_capable/0` 含 `<<"coaps">>` 使安全审计面能识别「该协议可带凭据」。但**没有**「某身份在某时刻从某地址用 PSK 建了会话」的专用审计日志——运维只能靠 listener 进程状态与 `broker_metrics` 既有键（`bytes_in`/`frame_decode_errors`/`protocol_errors`，ADR-016 冻结键集，**未扩容**）间接推断。
8. **诱饵密钥是缓解而非消除**：未知身份回诱饵 PSK（而非 alert）消除了「时延/错误形态可区分」这一枚举 oracle，使未知用户与错口令在客户端看来一致。但**这不是**恒定时间保证——派生本身是 SHA-256（快），真正的握手失败仍发生在 Finished 校验；对能观测网络层重传模式的主动攻击者，理论上仍有细微信号。这一条的定位是「把明显的 oracle 关掉」，不是「证明不可枚举」。
9. **DTLS 只在 `coaps` listener 上；明文 listener 不会升级**：同进程可以同时有 `transport => udp` 与 `transport => dtls` 的 listener，但明文 listener 的 datagram 永远不带身份。**没有**「明文进来、按源地址升级到 DTLS」的机制，也**没有**自动重定向（HTTP 式的 301 到 coaps）——客户端必须自己选 `coaps`。

### 12.7 S12-D 跨节点投递的安全边界（2026-11-19 立）

S12-D（ADR-034）多加了一类**节点间**调用：非 owner 上的 publish/read/subscribe 经 `rpc:call` 交给 owner。它**不改变任何协议侧的身份/ACL 语义**（判定仍在前端、转发不携带也不提升 `principal`/`tenant`），但确实扩大了一条**既有**的信任面，因此单独留痕：

1. **节点间 RPC 没有应用层鉴权，信任边界是 Erlang distribution。**任何能连上 distribution 的节点都可以调 `broker_queue:forwarded/3`、`broker_subscription:dispatch_qos0/1` 等导出函数；`forwarded/3` 里的 `'$broker_queue_forwarded'` 标志**只防转发回环**（两个节点缓存不一致时无限转发），不是权限位。开启 `cross_node_delivery` 前请确认 distribution 端口只在受信网络/防火墙内可达；跨节点/复制通道 TLS 仍是 §12 第 1 条（未实现），对它同样适用。（**注（2026-11-20，ADR-035）**：本句写于 S12-D 当时；跨节点/复制通道 TLS 已于当日实现，见本条之后第 5 条与 §12 第 1 条。）关闭开关时这些钩子根本不安装，减少一个可调用面。
2. **owner 不做第二次 ACL 判定，这是与单节点一致的既有分工。**`broker_queue` / `broker_subscription` 从来不做授权，授权在协议前端（§4）。转发调用因此不是「绕过 owner 的鉴权」——owner 本来就没有鉴权层；它也不是「借 owner 的身份」，因为 `rpc` 调用不传身份，消息里的 `tenant`/`principal` 仍是发布方前端写入的原值。要收紧这个模型就得让转发带上调用者身份并由 owner 复核，那是另一个 ADR 的事，不在 S12-D。
3. **跨节点 shared 组拒绝是正确性边界，不是安全控制**（ADR-034 D5）：拒绝的理由是 `claim/4` 在每节点独立发号会重复投递，不是「shared 不安全」。exclusive / failover 跨节点是可用的。**（2026-11-25 批注，MQ-15）**：`broker_subscription.cluster_shared_rebalance = true` 时该拒绝被撤下——分配决策改由 ra 元数据组串行化，重复投递的根因消失。**这仍然不是安全判断**：放开后 shared 组的入组授权与其它订阅一样，仍由协议前端按 `tenant`/`principal` 判定；ra 侧只做分配互斥，不引入第二个鉴权点。默认 `false` 时本条的代码路径不变。
4. **异常探测靠指标而不是日志**：路由视图滞后/陈旧写入被拒时累加 `cluster_fencing_rejected`（键集 S4 就有，S12-D 未新增键）。这个计数在正常迁移窗口也会涨，**不能**单独当成入侵信号；持续高位应查成员视图与 `broker_cluster_cache` 是否停更。
5. **分布现在可以加密了（2026-11-20，ADR-035）**：上面第 1 条描述的信任面——「节点间 RPC 没有应用层鉴权，信任边界是 Erlang distribution」——现在有了一条加固路径：开启 `broker_cluster.dist_tls` 后，distribution 走 mTLS，**只有持有受信 CA 签发证书的节点能建立会话**，且流量加密。这不改变第 1~4 条的任何结论（owner 仍不做二次 ACL、`forwarded` 标志仍不是权限位、shared 拒绝仍是正确性边界），但把「谁能连上 distribution」从「知道 cookie 的人」收紧到「持有受信证书的人」。**CA 信任是双向准入闸门**，同时**拨出方向按 SAN 绑定对端身份**（证书 SAN 写错则握手失败），**拨入方向不校验客户端 SAN**——所以「谁能连上 distribution」从「知道 cookie 的人」收紧到「持有受信证书且能被对端拨到的人」。**默认仍是关闭的**，所以未开启的部署，第 1 条的结论原样成立。
