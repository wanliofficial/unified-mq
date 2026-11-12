# ADR-026：Agent 接入前端——应用拆分、HTTP/SSE 选型、控制面落点与边界

- **状态**：已采纳（S9-0 冻结，2026-11-11）
- **日期**：2026-11-11
- **关联**：A2A `1.0.0`（`specification/a2a.proto`、`docs/specification.md` §3.2/§5.3/§5.5/§9）、MCP `2026-07-28`（architecture、`server/discover`）、ACP v1 stable；ADR-001（协议适配与内核分离）、ADR-002（Mnesia 只做控制面）、ADR-007（显式可靠性）、ADR-013（cursor 归属）、ADR-015（重投递/DLQ/TTL）、ADR-016（可观测最小集）、ADR-021（集群控制面）、ADR-023（MQTT over WS 两进程）、ADR-024（relx profile 覆盖）、ADR-025（Task → 内核映射）
- **关联文档**：[`docs/design/s9-0-convergence-report.md`](../design/s9-0-convergence-report.md)（OQ-3/OQ-4/OQ-6/OQ-7/OQ-8/OQ-10）、[`docs/design/s9-agent-protocols.md`](../design/s9-agent-protocols.md) §2.1/§2.2/§2.3/§4.4

## 背景（Context）

S9 要在既有内核之上加 A2A / MCP / ACP 三个接入前端。与 S5（NATS/AMQP/Kafka/Pulsar）、S8（MQTT 系传输）不同，这三个协议的共同底座是 **JSON-RPC 2.0 over HTTP**，A2A 还要求 **SSE 服务端流**。这引出六个必须一起定下来的问题，任何一个单独定都会在下一个子阶段被推翻：

1. **应用拆分**：HTTP/JSON-RPC/SSE 的公共代码放哪，谁来起 listener；
2. **HTTP 前端选型**：仓库当前只有 `broker_admin_listener` 这一份自研 HTTP/1.1（`gen_tcp` + `{packet, http_bin}` + 一请求一连接 + `Connection: close`），而 SSE 需要长连接；引入 cowboy 会打破「仅 ranch + ra」的依赖面；
3. **控制面落点**：Agent Card 是低频、可缓存、按租户隔离的元数据，但 `broker_meta` 现有的 `topic_meta`/`queue_meta`/`binding_meta` 是「key/config」泛型形状且**无运行时读取方**；
4. **装配与门控**：新端口、boot 顺序、`broker_security` 接线、relx 双清单（ADR-024 §D5 已踩过 profile 覆盖）；
5. **ACP 的侧别**：ACP 是双向 JSON-RPC（Agent 与 Client 都能发请求），Broker 可以扮 Agent 也可以扮 Client，二者实现量差一个数量级；
6. **出站与授权边界**：A2A push notification 要求 Broker 主动 POST 到客户端给的 URL（SSRF 面），而 A2A 推荐 OAuth，容易被读成「Broker 要做授权服务器」。

本 ADR 只冻结**结构与边界**；Task 语义在 ADR-025，具体方法实现留给 S9-A~S9-E。

## 决策（Decision）

### D1. 四个 app：一个纯库底座 + 三个互不引用的 adapter

| App | 形态 | 进程 | 依赖 |
|---|---|---|---|
| `broker_proto_agent_rpc` | **纯库**（参照 `broker_ws`：无 supervisor、无 listener、无自有配置） | 无 | `kernel`/`stdlib`/`sasl`/`ssl`/`ranch`/`broker_core`/`broker_security`（认证 helper 与体积上限都走内核门面） |
| `broker_proto_a2a` | adapter | 任务分片 registry + push worker + listener | `broker_proto_agent_rpc`、`broker_meta`、内核门面 |
| `broker_proto_mcp` | adapter | listener（Streamable HTTP），可选 stdio child | `broker_proto_agent_rpc`、内核门面 |
| `broker_proto_acp` | adapter | 仅 stdio child（默认关闭） | `broker_proto_agent_rpc`（只用 JSON-RPC 信封） |

- 公共逻辑（JSON-RPC 2.0 信封、错误模型、版本协商、HTTP/1.1 请求解析、SSE writer、认证 helper）**只**下沉到 `broker_proto_agent_rpc`。
- 三个 adapter **彼此不引用**；公共代码不下沉到 `broker_core`——内核不为某个协议族扩面（ADR-001 的边界）。
- **底座不拥有 listener**：如果底座起 listener，它就必须反向依赖各 adapter 的 handler（或引入注册表），形成「底座 → adapter」耦合。依赖方向固定为 `adapter → 底座 → kernel`，与 `broker_proto_ws → broker_ws` 同构。
- `broker_proto_agent_rpc` 的 `.app.src` 不注册任何进程，`applications` 只列 `kernel, stdlib, sasl, ssl, ranch, broker_core, broker_security`；因此它可以在任何 profile 里被安全地重复启动（与 `broker_ws` 一样）。它**不读**任何 `broker_proto_*` 的环境配置。

### D2. HTTP/1.1 与 SSE：ranch 2.3.0 + `{packet, http_bin}` + OTP 28 `json`

**采纳**：`ranch` 起 acceptor，每个连接一个 `ranch_protocol` 进程用 `{packet, http_bin}` 解析请求行/头，OTP 28 自带 `json` 编解码。**只支持 HTTP/1.1**。

| 候选 | 判定 | 理由 |
|---|---|---|
| cowboy | **否决** | 新增运行时依赖，违背「仅 ranch + ra」的依赖面；A2A/MCP 只需 6~10 个端点，HTTP/2、路由 DSL、middleware 都用不上 |
| 复用 `broker_proto_ws` | **否决** | 它是 RFC 6455 适配器（ADR-023 已冻结两进程形态），把 HTTP 塞进去会让 WS 的 pinned wire behavior 变脆 |
| 复用/扩展 `broker_admin_listener` | **否决** | 管理面与数据面必须分开（`broker_admin` 的 `applications` 刻意很窄）；且它是「一请求一连接 + `Connection: close`」，结构上不支持 SSE 长连接 |
| **ranch + `http_bin` 自建** | **采纳** | ranch 已是依赖；`broker_security_tls:transport/0` 直接给出 `ranch_tcp`/`ranch_ssl`；`broker_admin_listener` 已证明 `{packet, http_bin}` 足以解析请求；每连接一个进程天然适配 SSE |

形态：

```
broker_proto_a2a_sup (one_for_all)
├── a2a_task_registry (N shards)          % 投影与状态机串行化（ADR-025 §D2）
├── a2a_push_worker                        % 出站 webhook（S9-C）
└── a2a_listener × M                       % ranch:child_spec，最后一项
      └── a2a_conn (ranch_protocol)        % 每连接一个；HTTP 解析 + SSE 写
```

- 请求解析沿用 `broker_admin_listener` 的 `{packet, http_bin}` 方式（请求行/头/`content-length`），并补 `Expect: 100-continue` 与 `Transfer-Encoding: chunked` 的请求体上限。
- 响应：普通请求 `Content-Length` + `Connection: keep-alive|close`；SSE `200 OK` + `Content-Type: text/event-stream` + `Transfer-Encoding: chunked` + `Cache-Control: no-cache` + `X-Accel-Buffering: no`，每个 `data:` 是一个完整 JSON-RPC Response，终态/中断态关流。
- 中间件顺序（固定）：请求行/头 → 体积与 JSON 深度上限（`broker_limits`）→ `A2A-Version` 协商 → 认证（`broker_security:authenticate_conn/1`）→ 授权（`authorize/2`）→ 方法/参数 → handler。
- TLS：复用 `broker_security_tls:transport/0`（禁用 → `ranch_tcp`，启用 → `ranch_ssl` + PEM），与七个既有 listener 同一开关；`broker_security.tls.enabled` 对 S9 是真正的开关（不像 MQTT-SN 走 UDP）。
- 不做 HTTP/2、不做 `Upgrade: websocket`；限制写进能力矩阵（`docs/protocol-compatibility.md` 新章）。
- 连接级上限：ranch `max_connections` + 每租户 `max_sse_connections`；超限回 `503`，不排队。

### D3. Agent Card 落在 `broker_meta` 的新表 `agent_card`（amends ADR-002 表清单）

在 `broker_meta` 新增一张表（只**新增**，不修改既有 11 张表的形状）：

```erlang
{agent_card, [agent_card, agent_ref, tenant_id, version, status,
              card, extended_card, updated_at]}
```

- 主键 `agent_ref = {TenantId, AgentId}`；`tenant_id` 另存一列用于 `all/1` 过滤。行形状遵守 `tables/0` 的「表名在第 1 元素、主键在第 2 元素」约定。
- `card` 是公开 Agent Card（map 或 JSON binary），`extended_card` 是认证后版本；`version` 单调递增，是 `ETag` 的来源；`status ∈ active | disabled`。
- 读：`broker_meta:get/2`、`all/1`；catalog 过滤（skill/tag/provider/capability）在 adapter 内做，`page_size` 未指定默认 50、上限 100（与 `ListTasksRequest` 的 proto 默认一致）。
- 写：管理 API（S9-B）与 boot 期配置种子；写前校验 `name`/`description`/`supportedInterfaces`/`version`/`capabilities`/`defaultInputModes`/`defaultOutputModes`/`skills` 全部存在。
- 缓存：`ETag` 由 `version` + 内容哈希构成，`If-None-Match` → `304`，`Cache-Control: max-age=60`（可配）。S9-B **不**加 ETS 缓存：Mnesia `dirty_read` 足够，先测再优化。
- **不复活** `topic_meta`/`queue_meta`/`binding_meta`：它们的 `key/config` 泛型形状承载不了带 schema 的 card，硬塞会把「无运行时读取方」变成「有一个读取方但语义错位」。
- ADR-002 的表清单是冻结项，本 ADR 显式 **amends** 它（新增第 12 张表），既有表与「Mnesia 只做控制面」的结论不变。

### D4. 装配：端口、boot 顺序、relx 双清单、认证接线

**端口**（既有 1883/1884/4223/5673/6650/8080/8083/8090/9093 不动；14567 只是 QUIC 的示例端口，默认 `listeners => []` 不监听）：

| 面 | 端口 | 默认 |
|---|---|---|
| A2A（JSON-RPC + SSE） | `8081` | app env `listeners => []`；release 绑 `{127,0,0,1}` |
| MCP（Streamable HTTP） | `8082` | 同上 |
| ACP（stdio） | 无端口 | `stdio.enabled => false` |

- `listeners => []` 是既有惯例（CT 与库节点不偷偷开端口）；release 配置显式给 `#{port => 8081, ref => {a2a, 8081}, num_acceptors => 10, max_connections => 1024}`，对外暴露必须是显式改动。
- `applications`：`kernel, stdlib, sasl, ranch, broker_core, broker_metrics, broker_storage, broker_router, broker_queue, broker_subscription, broker_event, broker_security, broker_proto_agent_rpc`；`broker_proto_a2a` 另加 `broker_meta`。OTP 拓扑序保证在 `broker_subscription:init/1`（boot step 2/3）之后。
- listener 是各自 supervisor 的**最后一项**，与 `broker_proto_mqtt` 的 boot step 4 同构。listener 绑定失败 → 该 app 起不来，不影响已起的 app（与其他协议一致）。
- **relx 两份清单都要加**：默认 `relx` 与 `quic` profile 的 `relx`（profile 会**替换**顶层清单，ADR-024 §D5 已踩过坑）。
- 认证接线：`Authorization: Bearer <token>` / `X-API-Key` → `broker_security:authenticate_conn({token, Token})` → `broker_user` 命中 → `{principal, tenant_id}`；ACL `action` 用 `publish`（SendMessage/建任务）与 `subscribe`（GetTask/ListTasks/Subscribe）；租户判定沿用 `broker_security`，不在 A2A 内重写。
- `require_auth` 是每个 adapter 自己的 env，**默认 `true`**（新面没有历史兼容包袱，失败关闭）；匿名需显式 `false`，且只在回环监听时允许（启动时校验，否则拒绝启动）。
  - **不**复用 `broker_security:auth_required/1` 作为 S9 的开关：它的 `credential_capable/0` 白名单只含六个既有协议（`mqtt`/`ws`/`nats`/`amqp`/`kafka`/`pulsar`），且 `require_auth` 默认 `[]`（全匿名）。S9-A **不修改** `broker_security` 的这两处语义（改了会动到既有 823 例的行为），只复用 `authenticate_conn/1` + `authorize/2` + `default_tenant/0` 三个纯函数。
  - 结果：`config/sys.config.src` 里 `{require_auth, []}` 保持不变；S9 的「默认必须认证」由 adapter 自己的 env 决定，两套开关不互相覆盖，能力矩阵与 `docs/security.md` 各写一行说明。
- 401/403：认证失败 → `401` + `WWW-Authenticate: Bearer error="invalid_token"`；认证通过但 ACL 拒绝或租户不匹配 → `403`（不泄露资源是否存在）。

### D5. ACP 只做 stdio，Broker 扮演 **Client 侧**

- **只做 stdio**：远程 HTTP/WS 传输在 ACP v2 仍是 Transports WG 草案，规划稿 §1.3 已列为非目标。
- **Broker 是 Client（host）**：
  - stdio 语义决定「谁 spawn 谁就是 Client」；ACP Agent 按设计是 Client 的本地子进程。Broker 是长驻进程，适合做 host。
  - 若 Broker 扮 Agent，就必须实现 `session/prompt` 的 agent 语义（推理/工具执行），直接撞 §1.3 非目标。
- **回调边界**：
  - `session/request_permission` 是 Client baseline，必须实现；策略 = **默认拒绝 + 可配 allowlist（按 tool kind / 标题正则）+ 超时拒绝**，并把请求作为事件暴露到运维通道。**绝不自动放行**。
  - `fs/read_text_file`、`fs/write_text_file`、`terminal/*`、`elicitation/create` **不广告**（capability 缺省）。Agent 若在 `initialize` 后强依赖它们，Broker 回 ACP 错误并结束会话，在能力矩阵逐行留痕。
- **Agent 进程只能由运维配置声明**（命令 + 参数 + 环境变量白名单），**不得**由客户端请求指定；默认关闭；工作目录、uid/gid、资源限制由部署方负责（Broker 不做沙箱，写进 `docs/security.md`）。
- **版本**：只接受 `protocolVersion 1`；不匹配回 ACP 错误。v2 草案特性（`auth/login`、`session/resume` 必需化、`fs/*`/`terminal/*` 移除、ID-keyed upsert）不实现，只做版本门控。
- **stdio 约束**：newline-delimited JSON（一行一条 JSON-RPC，UTF-8）；**stdout 只允许协议帧**，所有 logger handler 必须写 stderr，启动时校验，不满足即拒绝启用 stdio 前端。

> **落地注记（S9-E，2026-11-12；冻结文本不改，偏差在此逐条对照）**
>
> - **第 1 条「只做 stdio」**：落地一致。`broker_proto_acp` 没有任何 listener、没有 `listeners` 配置项；agent 由 `agents` 里的 `command` 经 `acp_port:open/1`（`spawn_executable`，**不经 shell**）作为本地子进程启动。
> - **第 2 条「Broker 是 Client」**：落地一致。`initialize` / `session/new` / `session/prompt` 都由 Broker 主动发；`stopReason` 折叠进 `$acp/session/<id>` 的 turn 投影。
> - **第 3 条「`session/request_permission` 必须实现；默认拒绝 + allowlist（按 tool kind / 标题正则）+ 超时拒绝」**：落地为**默认拒绝 + allowlist 精确匹配**——`acp_policy` 用 `#{tool, title}` 两轴**精确匹配**（`any` 是通配，**不做正则、不做子串**），理由是正则/子串让「放过一个本该拒绝的 tool」成为可能，与「绝不自动放行」冲突。超时判定是 `ElapsedMs > timeout_ms`（等于不算）。即使 `default => allow` 也拒绝（冻结姿态）。每次决策（含拒绝）写 `$acp/permission`（transient/os_buffer），写失败只 `logger:error`。
> - **第 3 条「把请求作为事件暴露到运维通道」**：落地为 `$acp/permission` 目的地上的普通 `#broker_message{}`（与 `$mcp/*` / `$a2a/*` 同一内核消息面），不是单独的管理 API。
> - **第 4 条「`fs/*`、`terminal/*`、`elicitation/create` 不广告；Agent 强依赖则回 ACP 错误并结束会话」**：落地为**回 `-32601` 但保持会话**——单个未支持能力不应销毁一个已协商的会话。`clientCapabilities` 恒为 `{}`，`_meta.unified-mq` 显式注明 `fs/terminal/elicitation=false`。能力矩阵 §4.18.1 已逐行留痕。
> - **第 5 条「agent 只能由运维配置声明；默认关闭；不做沙箱」**：落地一致。出厂 `agents => []`；`command` 必须绝对路径；`restart => temporary`（崩溃不重启）。Broker 不做 uid/gid/资源限制，写进 `docs/operations.md` §1.5 与 `docs/security.md` §12。
> - **第 6 条「只接受 `protocolVersion 1`；不匹配回 ACP 错误」**：落地为不匹配时记 `stop=unsupported_protocol_version` 且**不发** `session/new`（会话无从建立）；v2 草案特性不实现。
> - **第 7 条「stdio 约束：stdout 只允许协议帧；启动时校验 logger 不满足即拒绝」**：**校验未落地**——ACP 的协议帧走**agent 子进程管道**（`acp_port` 的 stdin/stdout），不占 Broker 自身 stdout，因此不存在 MCP stdio 那种「日志污染协议流」的风险。`acp_port` 也**不设** `stderr_to_stdout`，agent 的 stderr 与协议数据隔离。故本条在 ACP 侧的适用性被结构性消解，而非实现了一个校验器。

### D6. 出站 webhook：默认关闭、pin IP、禁重定向

1. **默认关闭**：`push.enabled => false`、`egress_allowlist => []`；空 allowlist 等于拒绝一切 push 配置创建（回 `PushNotificationNotSupportedError` / `-32003`），不是「先接受后丢弃」。
2. **两层 allowlist**：全局 host 后缀表（`api.example.com` 或 `.example.com`）+ 每租户表，两者求交。
3. **解析后校验并 pin IP**：先解析域名，拒绝 loopback/私有/链路本地/组播/保留地址（除非该条目显式 `allow_private => true`），然后**直接连到已校验的 IP** 并带 `Host` 头——消除 DNS rebinding / TOCTOU。
4. **禁重定向**（3xx 一律失败）；连接/发送/读取超时；响应体上限（默认 64 KiB，只需状态码）；只允许 `https`（`http` 需显式开启，仅供本地测试）。
5. **不使用 `httpc`**：它不能在建立连接时 pin 已校验 IP，也没有 per-request egress 钩子；用 `gen_tcp`/`ssl` + 最小 HTTP/1.1 客户端更可控，且与 D2 的「自建 HTTP」一致。
6. **自证与防重放**（S9-C）：`TaskPushNotificationConfig.authentication` 的 Bearer/API key 来自配置，**不接受客户端明文密钥**；出站头带 `X-A2A-Timestamp` + `X-A2A-Nonce`（**项目私有扩展头，不是 A2A 规范字段**），HMAC 可选；轮换靠配置重载/重启。
7. **失败语义**：按 ADR-015 的参数（`max_attempts`、指数退避、TTL）重试；耗尽后写 `$a2a/push/dlq/<tenant>`（普通 destination，可订阅、可审计），**不阻塞任务终态**。
8. **部署边界**：Broker 只保证自身 egress；集群网络策略、NAT、代理由部署方负责，写进 `docs/operations.md`。

> **落地注记（S9-C，2026-11-12；冻结文本不改，偏差在此逐条对照）**
>
> - **第 1 条「空 allowlist 等于拒绝一切 push 配置创建」**：落地为**配置写入不校验 egress**——`CreateTaskPushNotificationConfig` 只校验 `url` 形态（`http`/`https` + host 非空），空 allowlist 时**配置照样写入成功**，真正的拒绝发生在**投递期**：`a2a_push:guard/3` 回 `{guard_refused, empty_allowlist | empty_tenant_allowlist | host_not_allowlisted}`，该条投递被跳过并记日志（`a2a push skipped ...`），**任务终态不受影响**。`-32003` 只用于 `push_enabled => false`。理由：白名单是出站策略，配置是声明；把策略判定放在投递期才能让「先配后开」与配置重载自然工作，也避免把 egress 细节泄进控制面错误码。证据：`docs/protocol-compatibility.md` §4.16.6 #11/#12。
> - **第 6 条「Bearer/API key 来自配置」**：落地为密钥**不在配置行里**——`push_config` 表只有 `{url, auth_scheme, allow_private, updated_at, version}`，密钥单独放 `broker_proto_a2a.push_auth_secrets`，**按 scheme 名取**（`a2a_push_auth:secret_for/1`，键即客户端声明的 `authentication.scheme`，如 `"hmac-sha256"`）。客户端声明了未配置的 scheme → 跳过投递（`{auth_not_configured, not_configured}`），**不降级为无签名投递**。这比原文更严：`broker_meta` 的持久化/备份里不会出现任何密钥。
> - **第 6 条「HMAC 可选」**：落地为「有 scheme 才签」；无 scheme（`undefined`）时仍带 `X-A2A-Timestamp`/`X-A2A-Nonce`/`X-A2A-Task-Id`/`X-A2A-Config-Id` 四个头，防重放材料齐全。
> - **第 7 条「耗尽后写 `$a2a/push/dlq/<tenant>`」**：落地一致，另记两点实现细节——DLQ 消息的 `message_id` 取 **task id**（失败主体），`config_id` 在 payload 里；写入走 `broker_router:env/0` 解包 `{ok, Env}` → `Backend:route/3` → `broker_queue:append/3`（`fsync`），与业务消息同一写路径，写失败只 `logger` 不抛。
> - **第 3 条「先解析域名再 pin IP」**：落地一致；`allow_private => true` 的条目是 loopback/私网目标唯一合法通路（`push_allow_http => true` 只解开明文限制，不解开私网限制）。

### D7. OAuth 授权服务器 / JWKS **不纳入** Broker

- Broker 不是 OAuth AS：不签发 token、不提供 `/.well-known/oauth-authorization-server`、不托管 JWKS。
- 只接受**不透明 bearer token**：`Authorization: Bearer <token>` → `authenticate_conn({token, Token})` → `broker_user` 表命中 → `{principal, tenant_id}`。
- **不做 JWT 验签**（不取 JWKS、不做 introspection）。JWT 形状的 token 若不等于表中 token，一律 `401`；能力矩阵写明「JWT/OIDC 需前置网关」。
- Agent Card 的 `securitySchemes` 可以**声明**外部 `oauth2`/`openIdConnect`（含外部 URL），但那是静态元数据，不代表 Broker 实现该流程。
- 若未来要做，必须新开 ADR（涉及 JWKS 拉取、缓存、轮换、时钟偏移）。

### D8. 非目标边界（本 ADR 负责的「不做」）

| 项 | 说明 |
|---|---|
| HTTP/2、HTTP/3、WebSocket 升级 | 只做 HTTP/1.1；A2A/MCP 的 JSON-RPC binding 不要求 |
| cowboy / 任何第三方 HTTP 框架 | 依赖面保持 ranch + ra |
| A2A gRPC / HTTP+JSON(REST) 绑定 | 只做 JSON-RPC + SSE |
| ACP 远程传输 | v2 WIP，只做 stdio |
| MCP `sampling` / `logging` | 2026-07-28 已弃用 |
| Agent Card JWS/PKI 验签 | 只做 ETag/缓存与按身份裁剪，不做签名链 |
| Broker 作为 OAuth AS / JWKS 托管 | 见 D7 |
| per-task retention pin、Part 内嵌 UI | 见 ADR-025 §D6、规划稿 §1.3 |

## 后果（Consequences）

- **+** 依赖面不扩大：仍是 ranch + ra + OTP 内置 `json`；`rebar3 release` 不需要新 fetch。
- **+** 三个 adapter 互不引用，A2A 的 Task 语义不会渗进 MCP/ACP；底座改动只影响 JSON-RPC/HTTP，不触内核。
- **+** Agent Card 在控制面，与 ADR-002 的「Mnesia 只做控制面」一致；Task 数据面在日志里（ADR-025），二者不混。
- **–** 自建 HTTP/1.1 前端要自己承担 keep-alive、chunked、`Expect: 100-continue`、慢连接超时等细节；这是 S9-A 的主要工作量与风险点，必须用真实客户端（curl/官方 SDK）验证，不能只测自写客户端。
- **–** 只支持 HTTP/1.1 会在 HTTP/2-only 的中间层（部分 gRPC 网关、某些企业代理）后面不可用；能力矩阵必须写清。
- **–** ACP 只做 Client 侧意味着 Broker 不能充当「被编辑器接入的 agent」；这是一个产品级限制，需在 README/架构文档明确，避免误读。
- **–** 默认 `require_auth = true` 会让「本机先跑通」多一步配置；用 release 的 `{127,0,0,1}` 默认值与文档示例抵消。
- **–** push egress 即使做了 pin IP + allowlist，仍是出站能力：部署方必须理解「Broker 会主动连出去」，这写进运维文档与安全清单。

## 验证（Validation）

S9-A/S9-B/S9-D/S9-E 的 Gate 证据必须覆盖：

1. **零回归**：新四个 app 默认零 listener，既有 `rebar3 ct` 仍 823/823、52 套件；dialyzer/xref 干净；`rebar3 release` 与 `as quic release` 都包含新 app。
2. **HTTP 前端**：`curl` 与官方 SDK 各发一次 JSON-RPC 请求；`Expect: 100-continue`、chunked 请求体、超限 `413`、keep-alive 复用、`Connection: close` 各一条用例。
3. **版本协商**：`A2A-Version: 1.0` 通过；缺失按 `0.3`；不支持回 `-32009`。
4. **认证/授权**：无凭据 → `401` + `WWW-Authenticate`；错误 token → `401`；越权租户/ACL → `403`；`require_auth=false` + 非回环监听 → 拒绝启动。
5. **SSE**：`Content-Type: text/event-stream`、chunked、心跳、终态关流；慢客户端不增长 mailbox、不 pin retention（`retention_status/0` 无该连接引起的 `slow_consumer`）。
6. **Agent Card**：`/.well-known/agent-card.json` 与 `ETag`/`304`；`agent_card` 表在 Mnesia 重启后仍在；catalog 过滤与分页边界。
7. **ACP**：`initialize` 版本门控、`session/request_permission` 默认拒绝、未广告 capability 被请求时正确报错、stdout 无日志污染。
8. **egress**：SSRF 专项——私网 IP、重定向、非 https、空 allowlist、DNS 解析后被换 IP 各一条拒绝用例；失败耗尽落 `$a2a/push/dlq/<tenant>`。

## 冻结后补充

（暂无。S9-A 起若发现本 ADR 与实现冲突，按项目惯例在此追加批注，不改冻结正文。）

### 落地批注（S9-B，2026-11-12）

本节只记录**实现与冻结正文的差异/细化**，D1~D8 正文一字未改。代码见 `apps/broker_meta/src/broker_meta.erl`、`apps/broker_proto_a2a/src/a2a_card.erl`、`a2a_rpc.erl`；证据见 `docs/protocol-compatibility.md` §4.15。

| 决策 | 落地情况 | 差异 |
|---|---|---|
| D3 表形状 `{agent_card, agent_ref, tenant_id, version, status, card, extended_card, updated_at}` | 完全照做：`tables/0` 第 12 表逐列一致，主键 `{TenantId, AgentId}`，`status ∈ active | disabled`，`version` 单调 | 无 |
| D3 读 `broker_meta:get/2`、`all/1` | 落地为**四个具名 helper**，不复用 `get/2`/`all/1`（它们是控制面的通用入口，会让 A2A 的形状泄漏到调用方）：`put_agent_card/1`（唯一写入口，`/1` 因为 card 自带 tenant）、`get_agent_card/2`、`delete_agent_card/2`、`agent_cards/1` | 命名与计划不同（计划写的是 `get_agent_card/2` + `all/1` 组合），但**语义一致且更窄**；读路径一律带 tenant 是租户隔离的必要条件 |
| D3 `version` 单调递增是 ETag 来源 | 照做，且**并发安全**：`put_agent_card/1` 在**同一 `mnesia:transaction`** 里 `read(..., write)` 取旧版本再 `+1` 写入，两个并发注册不会都赢（`broker_meta_SUITE` 的 `agent_card_registration_is_serialized` 用并发写验证 `version` 从 1..10 无缺口） | 计划里没有明说事务边界；落地把它写进注释与用例，避免后来者拆成「先读后写」 |
| D3 ETag = `version` + 内容哈希；`If-None-Match` → 304；`Cache-Control: max-age=60` | 照做：`"<version>-<base64url(sha256(card))[:12]>"`；`matches_etag/2` 处理逗号分隔列表、`W/` 弱前缀与 `*`；`agent_card_max_age` 可配（默认 60） | 无。混入内容哈希的理由是「手改表行也能失效缓存」，已写进代码注释 |
| D3 catalog 过滤在 adapter 内做，`page_size` 默认 50 上限 100 | 照做，落在 `GET /agents`：`skill`（匹配 `skill.id` / `skill.name` / `skill.tags` 成员）、`tag`（`skill.tags` 成员）、`provider`（`provider.organization`）、`capability`（`capabilities` 里该键为 `true`，或命中 `capabilities.extensions[].uri`）四类过滤 + 分页 | **`/agents` 是项目私有接口**：A2A spec 未定义注册/检索 API（R-D4 悬置），故在文档与代码注释里显式标注「不是 spec 的一部分」，避免被当成标准端点 |
| D3 写前校验必需字段；boot 期配置种子 | 照做：`a2a_card:validate/1` 校验 `name`/`description`/`supportedInterfaces`/`version`/`capabilities`/`defaultInputModes`/`defaultOutputModes`/`skills`；`agent_cards` env 在 app 启动时 `a2a_card:seed/0` 幂等写入，**内容不变不 bump version**（ETag 稳定），格式不合法则启动失败 | 管理 API 的完整 CRUD 未做（计划写 S9-F）：当前写入路径只有「boot 种子」与「直接操作 Mnesia/管理面脚本」 |
| D3 S9-B **不**加 ETS 缓存 | 照做：每次读都是 Mnesia `dirty_read`；ETag 内容哈希每次重算（card 小，实测 25 条 curl 无压力） | 无 |
| D4 端口：A2A `8081`、MCP `8082`、ACP 无端口；`listeners => []` 默认 | 照做：`broker_proto_a2a.listeners` 出厂 `[]`，CT 与两个 release 的默认配置都**不监听**；端口只在 `curl` 证据节点上临时起（18093），不动 8081/8082 | 无 |
| D4 boot 顺序：listener 是 supervisor 最后一项 | **细化**：`broker_proto_a2a_sup` 的子进程顺序是 `a2a_task_registry` → listener。registry 必须在 listener 之前，否则第一个请求可能落到没有投影的节点上；registry 内部又是 N 个 shard → `a2a_task_recovery`（恢复最后跑，保证「投影 = 日志」在接请求前成立） | 计划只写了「registry + recovery 在 listener 之前」，落地明确为**两级顺序**（sup 级 + registry 级），并写进 `docs/architecture.md` §2.x 监督树表 |
| D4 认证接线：`authenticate_conn/1` + `authorize/2` + `default_tenant/0`，不改 `broker_security` | 照做；`config/sys.config.src` 的 `{require_auth, []}` 与 `credential_capable/0` 白名单**一字未改** | 无。ACL 的 resource 是**真实 destination**（`SendMessage` → `$a2a/task/new`，其余用 `$a2a/task/<id>`），不是方法名；`Get*` 用 `subscribe`、`SendMessage`/`CancelTask` 用 `publish` |
| D4 `require_auth=false` 只在回环绑定下允许 | 照做：boot 时校验所有 listener 的 `ip`，非回环直接 `{error, {insecure_listener, Port, Ip}}`，整个 app 起不来；CT 有 `anonymous_listener_on_wide_bind_is_refused` | 无 |
| D4 401/403 语义 | 照做：无/错凭据 → `401` + `WWW-Authenticate`；ACL 拒绝 → **裸 `403`**（不带 JSON-RPC 信封）；跨租户 `GetTask` → **`404` + `-32001`**（不泄露存在性） | 计划没写跨租户用哪个码；落地选「与不存在同码」，与收敛报告 §3 的「id 不是存在性预言机」一致 |
| D5 / D6 / D7 / D8 | **全部已落地**：D5 → S9-E（ACP Client，`broker_proto_acp`）；D6 → S9-C（webhook/SSRF）；D7 → S9-C（SSE）；D8 → S9-D（MCP） | D8 的非目标边界（无 HTTP/2、无第三方 HTTP 框架、无 gRPC/REST 绑定、无 card 验签、无 OAuth AS）在 S9-E 一仍其旧：依赖面仍是 `ranch 2.3.0` + 内置 `ra 3.2.0` + OTP 28 `json`。D5 的落地偏差见 §D5「落地注记（S9-E）」 |

**与 D3 计划文本的一处口径统一**：计划写「管理 API（S9-B）与 boot 期配置种子」都能写卡；落地只有 boot 种子（+ 直接操作 Mnesia）。管理面 CRUD 的公开 HTTP 接口未做，`docs/roadmap.md` §11.1 的 S9-F 行已记。
