# S9-D 执行计划 — MCP 前端（Broker 作网关：Streamable HTTP + stdio + 原语透传）

- **对应设计**：[`docs/design/s9-agent-protocols.md`](s9-agent-protocols.md) §4.2 S9-D 行、§3.1（R-T3/R-T4/R-T6/R-T7、R-B1/R-B2）、[`docs/design/s/s9-0-convergence-report.md`](../design/s9-0-convergence-report.md) §13（第 1/2 条）；[ADR-027](../adr/027-mcp-front-end-kernel-mapping.md)（MCP 前端 → 内核映射，逐条落地）
- **基线（改动前）**：`rebar3 ct` **973/973**；dialyzer / xref / 两种 release 通过（同 S9-C 收尾）
- **范围**：新增 `broker_proto_mcp`（`broker_proto_mcp.app.src` + `_sup`/`_app`/`mcp_listener`/`mcp_conn`/`mcp_rpc`/`mcp_route`/`mcp_discover`/`mcp_stream`/`mcp_stdio`，默认 `listeners => []`、`stdio_enabled => false`）。不新增 Mnesia 表（**不 amends ADR-002**，见 ADR-027 决策）。
- **阶段顺序**：S9-D 是 S9-E（ACP）的前置。
- **执行约束**（仓库既定规则）：编译/测试前 `export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"`；`rebar3 ct` 前 `rm -rf _build/test/extras`；用例数由 `all/0` 回填；文档只写已实现事实；ADR/设计稿冻结文本不动，偏差加「落地偏差记录」批注；不新增运行时依赖。

## 任务拆分（DoD）

1. **Task 1**：`broker_proto_mcp.app.src` + `_app`/`_sup` + 两个 relx 清单条目 + `config/sys.config.src` 段（`features => []`、`discover_ttl_ms`、`request_timeout_ms`、`sse_*`、`allowed_origins`、stdio 门控）。
2. **Task 2**：`mcp_route`（纯模块，method→feature 表、feature 门控、sampling/elicitation 拒绝、透传判定）+ `mcp_discover`（本地 server/discover 组装、capabilities 推导）。
3. **Task 3**：`mcp_rpc`（信封 + `_meta` 解析 + 请求头门禁 + 认证 + 发布请求 + 取回回复 + `subscriptions/listen` 计划）。
4. **Task 4**：`mcp_stream`（SSE 帧 + 心跳 + 上限，订阅过滤 + `notifications/subscriptions/acknowledged`，承 ADR-025 §D7）。
5. **Task 5**：`mcp_stdio`（newline-delimited 成帧 + stdout-only + 可注入 get/put + 子进程订阅）。
6. **Task 6**：`mcp_listener`/`mcp_conn`（ranch 适配 + per-conn loop，`{json,…}`/`{stream,Plan}` 分流）。
7. **Task 7**：`mcp_listener_SUITE` + `mcp_stdio_SUITE`（全部 `port => 0`）。
8. **Task 8**：文档回填 + 两种 release + dialyzer/xref 复跑。

## DoD

- [x] `mcp_listener_SUITE` 全绿（**32 例**：`discover` 6 / `headers` 13 / `call` 9 / `stream` 4）：`server/discover` 往返（含 `capabilities` 随 `features` 变化、缺 `_meta` → -32602、版本不支持 → -32022、头/体 `_meta` 不一致 → -32020）；`tools/call` 透传端到端（假后端 → `rpcResult` + 超时 `-32603`）；`notifications/progress` 经 SSE；`subscriptions/listen` ack-first + 资源订阅；未广告 method → -32601/404；GET/DELETE → 405；`Origin` 非法 → 403；`Accept` 不含 SSE → 406；`Mcp-Name` 不一致 → -32020；通知 → 202。
- [x] `mcp_stdio_SUITE` 全绿（**10 例**：`protocol` 8 / `subscription` 2）：discover/未知方法/坏 JSON/通知 202 + 可注入 get/put + 订阅子进程。
- [x] `broker_meta` 零改动；`broker_proto_a2a` 零改动（`agent_rpc_error` 只新增两个 MCP 错误码，A2A 分支未动）。
- [x] 零 FAILED；dialyzer/xref 无新增告警；两种 release 装配成功。
- [x] 文档：§4.17 + roadmap §11.1/§11.2 + progress + architecture（§2 应用表 + §3.14）+ 落地偏差记录。

## 落地偏差记录（实现后回填）

1. **`initialize` 不存在**：2026-07-28 无握手，`initialize` 按未知方法处理（**404 + `-32601`**），不是透传。
2. **`feature_not_advertised` 实际是 404，不是 200**：ADR-027 §D6/§D7 第 10 行写「feature 未广告 → 200 + `-32601`」，实现把 `method_not_found` 统一映射成 404（`mcp_dispatch:status/1`），与未知方法同码同状态。证据见 `docs/protocol-compatibility.md` §4.17.7 #25（`features => []` 时 `tools/list` → `404` + `data.reason="feature not advertised"`）。
3. **`sampling/*` / `elicitation/*` 也是 404 + `-32601`**（带 `data.reason`），与未知方法同状态，不单独给 200。
4. **`ServerCapabilities` 没有 `subscriptions` 键**：2026-07-28 的 `ServerCapabilities` 只有 `tools`/`resources`/`prompts`/`completions`/`logging` 等，ADR-027 §D3「`subscriptions` 能力在 listen 被支持时给出」**无法表达**；`server/discover` 只按 `features` 推导四个对象。`subscriptions/listen` 本身不受门控（永远本地可用）。
5. **`DiscoverResult` 必须带 `ttlMs`/`cacheScope`**：实现固定输出 `ttlMs = discover_ttl_ms`（默认 3600000）与 `cacheScope = "public"`；`instructions` 为可选项，未配置时不输出。
6. **`clientCapabilities` 是逐请求必填**：ADR-027 §D4 只把 `protocolVersion` 写成必填；实现按 `basic/index` 的必填表把 `clientCapabilities` 也做成必填，缺失 → `-32602` + 400（stdio 同样）。
7. **HTTP 三个镜像头全部必填**：`MCP-Protocol-Version`、`Mcp-Method`、`Mcp-Name`（仅三个带 name 的方法）缺失与不一致同判 `-32020` + 400。因此「body 缺 `_meta` 版本 → `-32602`」在 HTTP 上**不可达**（header 门禁先命中 `-32020`），该路径只在 stdio 生效；ADR-027 §D4 的表述只在 stdio 面成立。
8. **非请求 POST 不校验三头**：`basic/index` 没定义通知的镜像头要求，HTTP 上通知 POST 只过 Origin/Accept/Content-Type/认证，回 202 无 body。
9. **`invalid_params`/`invalid_request` 用 HTTP 400**（ADR-027 §D7 第 11 行写 200）。MCP 的 HTTP 状态映射是局部覆盖：`method_not_found → 404`、`invalid_params/invalid_request/parse_error → 400`，其余仍走 `agent_rpc_error:http_status/1`（`agent_rpc_error` 自己的 `method_not_found → 200` 保持 A2A 语义，未改）。
10. **ACL 检查不做租户比较**：`mcp_dispatch:authorize/5` 直接调 `broker_security:authorize(Principal, #{action, destination})`（省略 `tenant_id` 即「无隔离检查，只查 ACL」）。原因：`$mcp/*` 是网关自己的命名空间，消息上的租户由网关按身份写入、客户端不可选；用 `agent_rpc_auth:authorize/3` 会让 stdio 的匿名身份（tenant `t0` vs `broker_security:tenant_id(<<"anonymous">>)` 无记录回 `default`）全部误判 deny。
11. **stdio 无认证层**：信任边界是「谁能启动这个进程」，身份固定为 `agent_rpc_auth:anonymous()`（默认租户）；`require_auth` 只作用于 HTTP listener。`notifications/cancelled` 只在 stdio 有实际取消效果（按 listen 请求 id 结束子进程）；HTTP 无会话，订阅随连接断开/时长上限结束。
12. **SSE 不建 SubRef、不 pin retention，也没有 pending-bytes 上限**：ADR-025 §D7 的 `sse_max_pending_bytes`/`sse_send_timeout_ms` 未落地；实际约束是 `sse_max_duration_ms`（listen 优雅关流）+ `sse_max_event_bytes`（单帧上限）+ call 的 `request_timeout_ms`（超时 → `-32603`，`data.reason=timeout`）。
13. **stdio 的 stdout 纯净性由部署保证，不由本 app 校验**：ADR-027 §D8 写「启动时校验该约束，不满足拒绝启用」——未实现。模块只用 `io:put_chars(standard_io, ...)` 写协议帧；默认 `logger` handler 仍写 stdout，运维必须把 `default` handler 指到 `standard_error`（OTP 不允许原地改 `type`，需 `remove_handler` + `add_handler`；证据见 §4.17.7 #23）。

已知留白（同 ADR-027 §D10）：`Mcp-Param-*` 识别不强制（能力矩阵标 accepted-not-enforced）；跨节点 reply 同 A2A 限制（owner 节点本地分区）；`server/discover` 的 `instructions` 默认不输出；不新增 `broker_metrics` 冻结键（失败只 `logger`，与 A2A 一致归 S9-F）。

## 门禁实测表（实现后回填）

| 门禁 | 结果 |
|---|---|
| `rebar3 ct` | **1015/1015、65 套件**（基线 973/63；净增 = `mcp_listener_SUITE` 32 + `mcp_stdio_SUITE` 10） |
| `rebar3 dialyzer` | **0 warning**（修掉 10 条：`req()` 类型改为 `agent_rpc_http:request()`、5 个不可达分支、`mcp_dispatch:authorize/5` 的租户误判见偏差 10） |
| `rebar3 xref` | 除既有 unused-export 外无新告警（无 undefined/deprecated/recursive 调用） |
| `rebar3 release` / `rebar3 as quic release` | 均成功，`lib/` 含 `broker_proto_mcp-0.1.0` |
| 真实请求证据 | `docs/protocol-compatibility.md` §4.17.7：HTTP **24 条** + stdio **6 行**（含 SSE 进度/结果、listen ack+通知+心跳+优雅关流、`features => []` 时的 404） |
