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
- 认证只发生在 MQTT CONNECT（`mqtt_conn_fsm`）。匿名连接（未配置用户名/密码）的 principal 是 `undefined`，在 ACL 判定里保持 S1 的「无规则即放行」姿态（§4）。

**现状边界（必须如实说明）**：WebSocket / NATS / AMQP / Kafka / Pulsar 的连接层目前没有走 `broker_security:authenticate/2`。它们的租户来自协议字段或配置默认值（`t0`），NATS 另有自己的 `auth_token`。因此「所有协议都已统一认证」**不成立**；租户隔离与 ACL 的数据结构对它们已就位，但身份入口尚未统一。这是 S6 之后的明确待办，不是已完成项。

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

- `broker_security_tls:transport/0` 返回 `{ranch_tcp, []}`（禁用）或 `{ranch_ssl, Opts}`（启用）。六个协议的 supervisor 都调用它，**一个开关覆盖所有 listener**，没有哪个 listener 能「知道 TLS 但不消费配置」。
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

## 12. 明确的未实现项（S6 之后）

1. 除 MQTT 外五种协议统一走 `broker_security` 身份表。
2. 跨节点/复制通道的 TLS。
3. Schema Registry 内容校验。
4. 审计日志签名与防篡改、外部 KMS。
5. 每条规则可配置的「未知扩展属性拒绝」策略。
6. 除 MQTT 外五种协议的 **标准** CloudEvents Binding 逐字实现（当前是统一 `ce-*` 头通道的等价映射，非无损点见 `docs/protocol-compatibility.md` §4.10）；统一认证仍只覆盖 MQTT。

这些在 `progress.md` 的 S6 结论里同样标注为待办，不作为 Gate 通过的依据。
