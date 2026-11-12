# ADR-027：MCP 前端——Broker 作为网关的 MCP ↔ 统一消息内核映射

- **状态**：已采纳（S9-D 冻结，2026-11-12）
- **日期**：2026-11-12
- **关联**：ADR-001（协议适配与内核分离）、ADR-002（Mnesia 只做控制面）、ADR-007（显式可靠性）、ADR-009（`broker_message`）、ADR-010（router）、ADR-013（订阅/游标归属）、ADR-015（重投递/DLQ/TTL）、ADR-016（可观测）、ADR-021（集群控制面）、ADR-025（A2A 瞬态读者范式）、ADR-026（Agent 接入前端：四 app 拆分、ranch + `{packet, http_bin}` + OTP 28 `json` 自建 HTTP/1.1/SSE、端口/boot/relx 双清单）
- **关联文档**：[`docs/design/s9-agent-protocols.md`](../design/s9-agent-protocols.md) §2.2/§3.1（R-T3/R-T6/R-T7/R-B1/R-B2）、[`docs/design/s9-0-convergence-report.md`](../design/s9-0-convergence-report.md) §13（第 1/2 条）、[`docs/protocol-compatibility.md`](../protocol-compatibility.md) §4.17（S9-D 证据）
- **协议事实来源**：MCP `2026-07-28`（`basic/index`、`basic/lifecycle`、`basic/transports/streamable-http`、`basic/transports/stdio`、`basic/patterns/subscriptions`、`basic/patterns/progress`、`server/discover`、`schema`）

## 背景（Context）

S9-D 要在 ADR-026 的 Agent 接入前端骨架上加第四个 adapter app `broker_proto_mcp`，把 **MCP `2026-07-28`** 接到统一消息内核。与 A2A（ADR-025）不同，MCP 在 `2026-07-28` 做了两处对 S9-D 影响最大的修订：

1. **无状态化**：`basic/lifecycle` 明确「There is no negotiation handshake. Every request carries its protocol version」——`initialize` 不再是握手，版本/身份/能力**逐请求**走 `_meta`（`io.modelcontextprotocol/protocolVersion|clientInfo|clientCapabilities`）。`streamable-http` 进一步**移除 GET 流端点与协议级会话**（无 `Mcp-Session-Id`、无 `Last-Event-ID` 续传）。
2. **发现内建**：`server/discover` 是 Server **必须**实现的方法，返回 `supportedVersions`/`capabilities`/`serverInfo`/`instructions`/`ttlMs`/`cacheScope`。

同时 S9 的既定边界（收敛报告 §13、ADR-026 §D7）要求：Broker **不解释、不执行工具语义**（R-B1），**不实现 sampling**（R-B2，`2026-07-28` 已弃用）。这两条决定了 MCP 前端的形态——它**不是**一个 MCP Server 的执行体，而是一个 **MCP ↔ 消息内核的网关**：把 MCP 原语请求映射成内核消息、把内核消息映射回 MCP 帧。

必须一起定下来的问题：①应用拆分与依赖面；②网关如何把「一次 MCP 原语调用」落到内核分区上、如何取回结果；③`server/discover` 与 `subscriptions/listen` 这两个 Broker 必须自己回答的方法怎么实现；④Streamable HTTP 的请求头门禁（`Origin`/`Accept`/`MCP-Protocol-Version`/`Mcp-Method`/`Mcp-Name`）与错误码映射；⑤stdio 前端的成帧与长活订阅；⑥能力声明与 feature 门控。

## 决策

### D1. 应用拆分与依赖面：新 app `broker_proto_mcp`，无新增运行时依赖

- 新增 `broker_proto_mcp`（app + supervisor + ranch listener），**依赖 `broker_proto_agent_rpc`**（复用 `agent_rpc_json`/`agent_rpc_http`/`agent_rpc_sse`/`agent_rpc_auth`/`agent_rpc_error`/`agent_rpc_version` 底座）与内核（`broker_core`/`broker_security`/`broker_router`/`broker_queue`）。**不引入任何新运行时依赖**（ADR-026 §D1：仅 ranch + 内置 ra + OTP 28 `json`）。
- listener 默认 `listeners => []`：app 随节点启动但不开端口；部署方配 `listeners` 才开（ADR-026 §D4）。`require_auth` 默认 `true`，非 loopback bind 时启动校验拒绝（同 `broker_proto_a2a`）。
- 8082 只是**示例**端口（收敛报告 OQ-10），不由本 app 在出厂时占用。
- `stdio` 前端由 env `stdio_enabled => false` 门控：默认不启用（一个跑着的 release 通常不该抢占 stdin）。

### D2. 网关语义：Broker 不解释工具语义，原语透传到内核分区（R-B1）

除 `server/discover` 与 `subscriptions/listen`（D3/D5 由 Broker 本地实现）外，其余 MCP 原语一律**透传**：

- **请求方向**：把客户端的 JSON-RPC 请求**原文**（不改客户端可见的 `_meta`）作为 `#broker_message{}` 的 payload，发布到 destination `$mcp/<method>`（method 原样，如 `tools/call`、`resources/read`、`prompts/get`、`completion/complete`）。为让任意后端（含非 Erlang）能取回，payload 的 `_meta` 里**注入**一个网关私有对象 `unified-mq = #{replyTo, requestToken, tenantId}`（`replyTo = $mcp/reply/<tenant>/<token>`，`token` 每请求唯一）；同一信息也放进 `#broker_message.headers`（供 Erlang 消费者免解析 JSON）。`payload_encoding = json`，`reliability = request_reliability`（默认 `fsync`）。
- **回复方向**：后端把 JSON 对象发布到 `replyTo`，其 `_meta.unified-mq.kind` 决定网关如何解释：
  - `kind = rpcResult`（或缺省）：payload 是 `{"result":…}` 或 `{"error":{…}}` → 网关包成 `{"jsonrpc":"2.0","id":<client id>, …}`，**这是终止帧**。
  - `kind = progress`：payload 是 `{"progress":…,"total":…,"message":…}` → 网关合成 `notifications/progress`（`progressToken` 取自请求 `_meta.progressToken`），**非终止**。
  - `kind = notification`：payload 即通知 params → 网关包成 `{"jsonrpc":"2.0","method":"notifications/<name>","params":…}`，**非终止**。
- **请求/回复关联**：前端为每次请求生成唯一 `replyTo`（token 唯一），订阅该 reply 目的地的分区（瞬态读者，D9），只取 `destination =:= replyTo` 的消息；收到第一帧 `rpcResult` 即终止。客户端 JSON-RPC `id` 由前端在收到终止帧时统一覆盖，后端回什么 id 都不影响关联。
- **Broker 绝不解释 `tools`/`resources`/`prompts` 的内容**：tool 的参数校验、资源内容、prompt 模板都在后端。网关只做「method → feature 门控（D6）→ 发布 → 取回 → 包 id」。

### D3. `server/discover` 由 Broker 本地回答（不发布到内核）

`server/discover` 是「这个 Server 支持什么」，Broker 自己就是 Server，因此本地直接构造 `DiscoverResult`：

```json
{"resultType":"complete",
 "supportedVersions":["2026-07-28"],
 "capabilities":{…由 features 推导…},
 "_meta":{"io.modelcontextprotocol/serverInfo":{"name":"unified-mq","version":"0.1.0"}},
 "instructions":"…可选…","ttlMs":<discover_ttl_ms>,"cacheScope":"public"}
```

`capabilities` 由 env `features`（D6）推导：`tools`→`{"tools":{}}`、`resources`→`{"resources":{}}`、`prompts`→`{"prompts":{}}`、`completions`→`{"completions":{}}`；`subscriptions` 能力在 `subscriptions/listen` 被支持时给出。`serverInfo` 是自报身份（spec 明确「not verified, SHOULD NOT change behavior」）。

### D4. 无状态、无会话、无 initialize 握手（2026-07-28）

- **不实现 `initialize`**：`2026-07-28` 无握手。客户端若发 `initialize`，按未知方法处理（`-32601` + 404）或作为普通透传？——**定为 `-32601`**：`initialize` 不是 Server 方法，是旧握手；新协议没有它。
- **无 `Mcp-Session-Id`、无 GET 流端点、无 `Last-Event-ID` 续传**（spec 已移除）。Streamable HTTP **单端点仅接受 POST**。
- **版本协商逐请求**：每请求的 `_meta["io.modelcontextprotocol/protocolVersion"]` 必须在 `supported_versions` 里，否则 `-32022` + `data{supported, requested}`（HTTP 400）。**缺该 `_meta` 键** = 缺必填 → `-32602` + HTTP 400（`basic/index`「缺必填 → -32602」）。HTTP 上另要求请求头 `MCP-Protocol-Version` 与 body `_meta` 一致，否则 `-32020`（D7）。
- **能力逐请求**：`clientCapabilities` 每请求自含，Server **不从历史请求推断**（spec 明确）。这与 A2A「能力在 Agent Card」不同，是 MCP 的有意设计。

### D5. `subscriptions/listen` = 长活 SSE 订阅流（Broker 本地实现，映射到内核订阅）

`subscriptions/listen` 是 2026-07-28 取代 `resources/subscribe` 与 GET 流的长活流。网关本地实现，把 filter 映射成内核订阅目的地：

- 请求 `params.notifications` 的每个字段映射到一个目的地，网关订阅其分区（瞬态读者）并在流上转发：
  - `toolsListChanged` → `$mcp/notify/tools/list_changed` → `notifications/tools/list_changed`
  - `promptsListChanged` → `$mcp/notify/prompts/list_changed` → `notifications/prompts/list_changed`
  - `resourcesListChanged` → `$mcp/notify/resources/list_changed` → `notifications/resources/list_changed`
  - `resourceSubscriptions: [uri]` → `$mcp/notify/resources/updated/<uri>`（uri 原样做目的地后缀，拒绝含 `\0`）→ `notifications/resources/updated`
- **第一帧必须是 `notifications/subscriptions/acknowledged`**，`_meta["io.modelcontextprotocol/subscriptionId"] = <listen 请求的 JSON-RPC id>`，且**先于任何通知**（spec 强制）。`notifications` 字段回显网关**实际同意**的子集（只含已广告且被映射的项）。
- 流上每条通知都带同一 `subscriptionId`（= listen 请求 id），客户端据此 demultiplex。
- **关流**：客户端断开 / 传输关闭 / 到达 `sse_max_duration_ms` / 收到 `notifications/cancelled`（按 subscriptionId）。服务端主动结束（如关停）时**先**回一个 `subscriptions/listen` 的成功 response（`resultType = complete`）再关流（spec 的 Graceful Closure）。
- **HTTP 上 `subscriptions/listen` 独占连接**（同 A2A SSE：`{stream, Plan}` 接管 socket）；并发订阅需多条连接。**stdio 上**订阅以子进程运行、共享 stdout，主读循环继续处理其他行（D8）。

### D6. 能力声明由 env `features` 驱动，未广告即 `-32601`

- env `features => []`（默认，诚实空：没有挂载后端就没有能力）。可配 `[tools, resources, prompts, completions]`。
- `server/discover` 的 `capabilities` 与 `features` 一致（D3）。
- 方法路由表（`mcp_route`）把每个透传 method 归到其 feature：`tools/list`+`tools/call`→`tools`；`resources/list`+`resources/templates/list`+`resources/read`→`resources`；`prompts/list`+`prompts/get`→`prompts`；`completion/complete`→`completions`。请求的 method 所属 feature 未广告 → **`-32601`** + `data.reason = "feature not advertised"`（HTTP 200，与 `MethodNotFoundError` 一致）。这是 spec 明许的「gated behind a server capability the server did not advertise」。
- `sampling/*`（`sampling/createMessage`）与 `elicitation/*`（`elicitation/create`）是 server→client 方向，客户端发来即显式拒绝 `-32601`（R-B2：sampling 不实现）。

### D7. Streamable HTTP 单端点请求头门禁与错误码映射

单端点（默认 path `/`，可配 `path`），**仅 POST**。请求头门禁**固定顺序**（先传输安全、后信封、最后方法语义）：

| 顺序 | 门禁 | 失败响应 |
|---|---|---|
| 1 | `Origin`（若存在）必须在 `allowed_origins` 或 loopback | **403**（无 body） |
| 2 | `Accept` 必须含 `application/json` **与** `text/event-stream`（或 `*/*`） | **406** |
| 3 | 方法为 POST；GET/DELETE/其他 | **405** + `Allow: POST` |
| 4 | `Content-Type` 基类型 `application/json` | **415** |
| 5 | `MCP-Protocol-Version` 头 == body `_meta.protocolVersion` | **400** + `-32020` |
| 6 | `Mcp-Method` 头 == body `method` | **400** + `-32020` |
| 7 | `Mcp-Name`（`tools/call`→`params.name`、`resources/read`→`params.uri`、`prompts/get`→`params.name`）== body 对应值 | **400** + `-32020` |
| 8 | 认证（`broker_security` 不透明 token） | **401** |
| 9 | 信封（`agent_rpc_json:request/1`） | 400 + `-32700`/`-32600` |
| 10 | 方法路由（`mcp_route`）：未知/未广告/sampling/elicitation | 404 + `-32601`；feature 未广告 200 + `-32601` |
| 11 | 参数校验（各 method 的必填） | 200 + `-32602` |

- **未知 method → 404 + `-32601`**（spec 明确）；**feature 未广告 → 200 + `-32601`**（spec 的 capability-gated 语义）。两者 code 相同、HTTP 不同，照 spec。
- **通知（无 `id`）→ 202 无 body**：`notifications/*` 一律 202（网关不回复）。`notifications/cancelled` 额外按 subscriptionId 结束对应订阅（D5）。
- `Mcp-Param-*` 头族（源自 `x-mcp-header`）**识别但不强制**：存在时若与 body 参数冲突 → `-32020`，缺省不校验（能力矩阵标「accepted, not enforced」）。这条留白避免猜一个 spec 未完全钉死的映射。

### D8. stdio 前端：newline-delimited JSON、stdout 只放协议帧、订阅以子进程运行

- **成帧**：一行一条 JSON-RPC（UTF-8，不得含内嵌换行）；`stdout` **只允许协议帧**，一切日志走 `stderr`；EOF / stdin 关闭即优雅退出（`basic/transports/stdio`）。启动时校验该约束，不满足拒绝启用 `stdio_enabled`。
- **无会话**：同 D4，逐请求 `_meta`；`server/discover` 照常本地回答。
- **长活订阅**：`subscriptions/listen` 在 stdio 上以**子进程**运行，写共享 stdout（经一个输出回调），主读循环继续读其他行；`notifications/cancelled` 按 subscriptionId 结束子进程。这与 spec「stdio 上多订阅共享一条 channel、按 subscriptionId demultiplex」一致。
- **测试边界**：stdio 成帧/EOF/通知用可注入的 get/put 回调在进程内端到端测（含一个进程内假后端）；HTTP 与 stdio 共用同一 `mcp_rpc:handle/1` 派发到内核，差别只在成帧与长活订阅的宿主。

### D9. reply/订阅是瞬态读者，不建 SubRef、不 pin retention（承 ADR-025 §D7）

- 取回 reply（D2）与跟随订阅（D5）都走 `broker_queue:subscribe/2`（进程本地邮箱钩子）+ `read_from/3`：不进 `broker_router` 的 `Filter → [SubRef]` 索引、不写 `.cursors`、不参 `min(live cursor)`、不 pin retention。慢/死连接只拖累自己。
- reply 目的地的分区由 `broker_router_hash:route(replyTo,…)` 求得（与发布同一次 route 调用），保证不读错分区。
- 请求/订阅超时用 env `request_timeout_ms`（默认 30000）；超时 → 终止帧 `-32603`（`data.reason = timeout`）。`subscriptions/listen` 无「结果超时」，只有 `sse_max_duration_ms` 上限与心跳 `sse_heartbeat_ms`（同 A2A）。

### D10. 不做（明确裁剪，写进能力矩阵）

不做 JWT/JWKS、HTTP/2、MCP 会话/`Mcp-Session-Id`、GET 服务端流、`Last-Event-ID` 续传、`initialize` 握手、**sampling**（R-B2）、elicitation 回调、工具/资源/prompt 的**语义解释与执行**（R-B1）、`Mcp-Param-*` 的强制校验、跨节点 reply（reply 在 owner 节点本地分区，与 A2A Task 同跨节点限制）。这些都不是「以后自然要做」，而是 S9-D 的有意边界。

## 与 ADR-025/026 的关系

- **承 ADR-025 §D7**：reply 取回与订阅跟随复用「瞬态读者」范式（邮箱钩子 + `read_from/3`），不建 SubRef、不 pin retention；`sse_max_duration_ms`/`sse_heartbeat_ms` 语义照搬。
- **承 ADR-026 §D1/§D2/§D4**：app 拆分、ranch + `{packet, http_bin}` + OTP 28 `json` 自建 HTTP/1.1/SSE、默认 `listeners => []` + `require_auth => true` + 非 loopback 拒绝 + relx 双清单，全部照搬，只换 method 表与错误码族（MCP 的 `-32020/-32021/-32022` 而非 A2A 的 `-32001~-32009`）。
- **修订 ADR-002**：`broker_meta` 是否新增 MCP 相关表？——**不新增**。MCP 无 Task、无 Agent Card、无 Push 配置；`server/discover` 由 env 驱动，订阅/通知全在日志分区。因此 **ADR-002 的表清单不变**（不 amends）。

## 边界与遗留

- **跨节点**：reply 与订阅在 owner 节点的本地分区（同 A2A Task 的跨节点限制，S9-C+ 演进位）。
- **背压**：SSE/订阅写是连接进程内的同步 `send`，socket 发送缓冲自然反压，不堆 mailbox（同 A2A）；`sse_max_event_bytes` 单帧上限另算。
- **失败可观测**：S9-D 不新增 `broker_metrics` 冻结键（同 S9-B/C），失败只 `logger`；与 A2A 失败计数器一并归 S9-F。
- **能力矩阵与证据**：MCP 方法能力矩阵、`server/discover`/`subscriptions/listen`/`notifications/progress`/错误码的逐条限制，以及真实客户端证据，落 `docs/protocol-compatibility.md` §4.17（S9-F 用官方 MCP SDK 补第三方客户端证据）。

## 落地偏差记录（实现后补注，2026-11-12）

> 上文（决策 D1~D10 与「边界与遗留」）为 S9-D 冻结时的原文，**不改**。以下只记录实现落地后与原文/协议事实的差异与补充，证据见 [`docs/protocol-compatibility.md`](../protocol-compatibility.md) §4.17 与本阶段执行计划 [`docs/design/s9-d-execution-plan.md`](../design/s9-d-execution-plan.md)。

1. **§D6「feature 未广告 → 200」落成 404**：`mcp_dispatch:status(method_not_found) -> 404` 覆盖了所有 `-32601`，包括 capability-gated 的 `feature_not_advertised`。`data.reason` 仍区分两者（`"feature not advertised"` + `feature`），HTTP 状态不再区分。§D7 表第 10 行的「200 + `-32601`」同理。
2. **§D3「`subscriptions` 能力」不可表达**：2026-07-28 的 `ServerCapabilities` 没有 `subscriptions` 键；`server/discover` 只按 `features` 输出 `tools`/`resources`/`prompts`/`completions` 四个对象。`subscriptions/listen` 仍始终本地可用（不门控）。
3. **§D3 的 `DiscoverResult` 两个缓存字段是必填**：实现固定输出 `ttlMs`（`discover_ttl_ms`）与 `cacheScope: "public"`；`instructions` 未配置时不出现。
4. **§D4 的必填 `_meta` 有两项**：除 `protocolVersion` 外，`clientCapabilities` 也逐请求必填（缺 → `-32602`）。HTTP 上「缺 `_meta` 版本 → `-32602`」不可达——请求头门禁先判 `-32020`；该路径只在 stdio 生效。
5. **§D7 第 5/6/7 行的三个镜像头是「必填 + 相等」**：缺失与不一致同判 `-32020` + 400；§D4 只写了 `MCP-Protocol-Version` 一致性，`Mcp-Method`/`Mcp-Name` 的必填性以 §D7 为准。
6. **通知 POST 不受 §D7 第 5–7 行约束**（spec 未定义通知的镜像头）；只过 Origin/Accept/Content-Type/认证，回 202。
7. **§D7 第 11 行的参数校验状态改为 400**（原文 200）：MCP 局部覆盖 `invalid_params`/`invalid_request`/`parse_error → 400`、`method_not_found → 404`；`agent_rpc_error:http_status/1` 的 A2A 映射未改。
8. **ACL 不做租户比较**：`mcp_dispatch:authorize/5` 用 `broker_security:authorize(Principal, #{action, destination})`（省略 `tenant_id`），而不是 `agent_rpc_auth:authorize/3`。`$mcp/*` 是网关自己的命名空间，消息租户由网关写入；stdio 的匿名身份用租户比较会让全部请求误判 deny。HTTP ACL 拒绝 → 403 无 body；stdio → `-32603` + `data={reason:"denied by policy", resource}`。
9. **stdio 无认证层（§D8 未提）**：身份固定 `agent_rpc_auth:anonymous()`；`require_auth` 只作用于 HTTP。`notifications/cancelled` 的取消只在 stdio 有实际效果（HTTP 无会话，订阅随连接/时长结束）。
10. **§D8「启动时校验 stdout 约束」未实现**：模块只保证自己用 `standard_io` 写协议帧；默认 `logger` handler 仍写 stdout，部署必须把 `default` handler 换到 `standard_error`（OTP 不允许原地改 `type`，需 `remove_handler` + `add_handler`）。本 app 不自建 handler、不做启动校验。
11. **§D9 的 SSE 上限落地面**：`sse_max_pending_bytes` / `sse_send_timeout_ms` 未落地；实际是 `sse_max_duration_ms`（listen 到期优雅关流）+ `sse_max_event_bytes`（单帧上限）+ call 的 `request_timeout_ms`（超时 → `-32603`，`data.reason="timeout"`）。
12. **§D5 的「按 subscriptionId 取消」只在 stdio**：HTTP 每个 listen 独占一条连接，客户端断开即结束，没有跨连接的取消通道。
13. **`initialize` 明确为未知方法**（§D4 的「定为 `-32601`」落地为 404，与第 1 条同一映射）；`sampling/*`/`elicitation/*` 同样是 404 + `-32601` + `data.reason`，不单独返回 200。
14. **`server/discover` 的 `serverInfo.version`** 取 app vsn 并转成 JSON string（`"0.1.0"`）；`instructions` 为 `undefined` 时省略该键。

