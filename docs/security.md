# 安全与治理（S6）

- **状态**：S6 交付事实文档 v1.0（2026-11-09）
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

- 原子 `allow`：保持 S1 的放行姿态（默认值）。
- 规则列表：有序 first-match，无匹配即 `deny`。
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

`*` 对 principal 和 action 也是通配。非法规则被跳过而不是让发布者崩溃；**只要有规则存在，未匹配就是拒绝**，这是安全方向。

证据：`broker_security_acl_SUITE`（10 例）+ `broker_security_audit_SUITE` 的 `acl_bypass_by_unmatched_rule_is_denied`、`acl_bypass_by_action_swap_is_denied`、`acl_deny_precedes_a_later_allow`、`legacy_allow_is_explicit_not_a_fallback`。

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

- `broker_security_tls:transport/0` 返回 `{ranch_tcp, []}`（禁用）或 `{ranch_ssl, Opts}`（启用）。**七个**协议 supervisor 都调用它（MQTT、WebSocket、MQTT over WebSocket、NATS、AMQP、Kafka、Pulsar），**一个开关覆盖所有 listener**，没有哪个 listener 能「知道 TLS 但不消费配置」。两个不在这条链上：MQTT-SN 走 UDP（要 DTLS，见 §12 第 8 条），MQTT over QUIC 只把证书配置当作**来源**读（握手本身就是 TLS 1.3，没有明文模式）。
- **协议侧还要真的把 TLS 握手跑完**：ranch 对 TLS 只做 `transport_accept`，协议进程收到 `{handshake, Ref, Transport, Sock, Timeout}` 后必须自己调 `Transport:handshake/2`（`ranch_tcp` 是 no-op，`ranch_ssl` 才完成握手），否则首个 `ssl:setopts/2` 报 `notsup_on_transport_accept_socket`。这正是 S8-B 修掉的既有缺陷：`broker_proto_ws` 的 wss 在此前**完全不可用**（listener 明明是 `ranch_ssl`），现由 `ws_conn_SUITE` 的 wss 用例钉住；`broker_proto_mqtt_ws` 从第一版起就正确握手。
- 启动时 `broker_security_app:start/2` 校验：启用且缺 PEM 文件 → 应用启动失败；禁用永远合法。
- 轮换：替换同路径 PEM，调用 `broker_security_tls:reload/0`（管理 API `POST /api/v1/tls/reload`），清 `ssl` PEM 缓存，下一个握手用新证书；已建立连接保持原证书（标准 TLS 语义）。
- 脱敏：`redacted_config/0` 不返回密钥内容。

证据：`broker_security_tls_SUITE`（12 例，含真实 `ssl:listen` + 握手 + `transport/0`）。

---

## 10. 管理面鉴权

`broker_admin_http`：

- 默认 `bind = {127,0,0,1}`、`port = 8090`、`token = <<>>`（空 token = 不鉴权，只允许回环）。
- 非空 token 时，除 `/healthz`、`/readyz` 外全部要求 `Authorization: Bearer <token>` 或 `X-Admin-Token`。
- token 比较是常量时间（`secure_equal/2`），避免逐字节时序猜测。
- 响应经 `json_safe/1` 收敛，避免 pid/ref/tuple 导致 500。

证据：`broker_admin_SUITE`（11 例，含 401、常量时间分支、脱敏配置）。

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

1. 跨节点/复制通道的 TLS。
2. Schema Registry 内容校验。
3. 审计日志签名与防篡改、外部 KMS。
4. 每条规则可配置的「未知扩展属性拒绝」策略。
5. `broker_event:duplicate/1` / `quarantine/2` 仅接在 MQTT 发布路径，其余五协议与 S8-C 的 MQTT-SN 都未接（见 `docs/cloudevents-mapping.md` 的「接线范围」注）。
6. 可观测性导出层（OTel 等；`broker_metrics:snapshot/0` 之上）。
7. `require_auth` 尚未接进 MQTT 的 CONNECT 路径（`mqtt_conn_fsm` 不调用 `auth_required/1`，见 §2.2 现状边界第 2 条）；`broker_meta` 的 `broker_user` 与数据面认证的接线（ADR-021 §D7 冻结后补充）；`topic_meta` / `queue_meta` / `binding_meta` 三张表的运行时读取方。
8. **MQTT-SN over DTLS 未实现**（S8-C 只跑明文 UDP）：`broker_security.tls.enabled` 不作用于 1884，这是目前唯一**明文且没有 TLS 路径**的协议端口（QUIC 的 14567 也不受该开关当开关用，但它强制 TLS 1.3，见第 9 条）；DTLS 列为后续待办（`docs/design/s8-mqtt-sn-mqtt-over-quic-and-mqtt-over-ws.md` §1.2）。
9. **MQTT over QUIC 的 0-RTT early data 与多流模式未实现**（S8-D 只做 CN Draft 3 的单流模式）：0-RTT 是 CN 的可选项，本轮不接收；多流是 EMQX 私有扩展，不在规范内。QUIC 本身**强制 TLS 1.3**（无明文模式），证书来源见 §9 与 `docs/operations.md` §1.2——`tls.enabled` 对它是「证书来源」而不是开关。另：quicer 0.4.12 不支持运行期补充流信用，`max_streams_per_conn` 是建连时的一次性配额。

六协议统一走 `broker_security` 身份表已于 2026-11-09 完成，见 §2.1，不再列为待办。
