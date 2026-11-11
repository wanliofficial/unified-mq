# S9-A 执行计划 — 协议底座：`broker_proto_agent_rpc` 纯库 + A2A JSON-RPC 前端（echo）

- **对应设计**：[`docs/design/s9-agent-protocols.md`](s9-agent-protocols.md) §4.2 S9-A 行；[ADR-026](../adr/026-agent-adapter-front-end.md) §D1/§D2/§D4/§D7；[《S9-0 收敛报告》](s9-0-convergence-report.md) §2.2/§2.3/§11/§14
- **基线（改动前）**：`rebar3 ct` **823/823、52 套件**；dialyzer / xref / `rebar3 release` 通过；HEAD `b78c527`
- **范围**：只做**协议无关底座** + **一条真实 HTTP 路径**。不建 Task 状态机、不建 Agent Card、不做 SSE 业务流、不接 MCP/ACP
- **阶段顺序**：S9-A 是 S9-B/C/D/E 的共同依赖；B/C/D/E 各自另写计划
- **执行约束**（来自仓库既定规则）：
  - 编译/测试前 `export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"`
  - `rebar3 ct` 前 `rm -rf _build/test/extras`
  - 用例数一律由 `all/0`（含 `groups/0`）实数回填，不预先承诺
  - 不新增运行时依赖：仍只有 `ranch 2.3.0` + 内置 `ra 3.2.0` + OTP 28 自带 `json`
  - 不杀用户进程/容器（`18831`/`18841`/`18081` 占用）；CT 一律 `port => 0`，**不动 8081/8082 的固定端口**
  - 数据目录必须在 ext4 上且位于 release 树外；CT 用 `/tmp/opencode/...`
  - 不改 `broker_security` 的既有语义（`credential_capable/0`、`{require_auth, []}`）

---

## 目标与「不做」

**S9-A 结束时可以证明的事实**（Gate 原文）：一个真实 HTTP 客户端用 `curl` 发 `POST /` + `A2A-Version: 1.0` + `Authorization: Bearer ...`，拿到一个符合 A2A 1.0.0 `SendMessageResponse` 形状的 echo 结果；无凭据 `401`、越权 `403`、版本不支持 `-32009`、方法未知 `-32601`、超限 `413`；既有 823 例零回归。

**明确不做**（留给后续阶段）：

| 项 | 阶段 |
|---|---|
| `Task` 状态机、`GetTask`/`ListTasks`/`CancelTask`、事件溯源投影 | S9-B |
| Agent Card 表、`/.well-known/agent-card.json`、ETag/缓存、catalog | S9-B |
| `SendStreamingMessage`/`SubscribeToTask` 的真实流、Push Notification、出站 webhook | S9-C |
| MCP / ACP app | S9-D / S9-E |
| HTTP/2、gRPC/REST 绑定、JWT/OIDC | 非目标（ADR-026 §D7/§D8） |

---

## Task 1 — `broker_proto_agent_rpc`：零进程纯库

**目标**：把 JSON-RPC 2.0 信封、错误模型、HTTP/1.1 请求解析、SSE 出站、版本协商、认证 helper 做成**无 supervisor、无 listener、无自有 env** 的库 app，供三个 adapter 引用；依赖方向固定 `adapter → 底座 → kernel`（ADR-026 §D1）。

### 1a. 新建 app 骨架

```
apps/broker_proto_agent_rpc/
├── src/broker_proto_agent_rpc.app.src
├── src/broker_proto_agent_rpc_app.erl      # 空 application，照 broker_ws_app
├── src/agent_rpc_json.erl                  # JSON-RPC 2.0 信封
├── src/agent_rpc_error.erl                 # 标准 + A2A 错误码
├── src/agent_rpc_version.erl               # A2A-Version 协商
├── src/agent_rpc_http.erl                  # HTTP/1.1 请求/响应（传输无关）
├── src/agent_rpc_sse.erl                   # SSE/chunked writer
├── src/agent_rpc_auth.erl                  # Bearer / X-API-Key → broker_security
└── test/*_SUITE.erl
```

`.app.src`：

```erlang
{application, broker_proto_agent_rpc, [
    {description, "Agent RPC base: JSON-RPC 2.0, HTTP/1.1, SSE, version and auth helpers"},
    {vsn, "0.1.0"},
    {registered, []},
    {mod, {broker_proto_agent_rpc_app, []}},
    {applications, [kernel, stdlib, sasl, ssl, ranch, crypto, broker_core, broker_security]},
    {env, []}
]}.
```

**验收**：`application:ensure_all_started(broker_proto_agent_rpc)` 返回 `{ok, [...]}`；`broker_proto_agent_rpc:module_info()` 无进程注册；`rebar3 xref` 不报未使用导出（导出即被 adapter 或测试使用）。

### 1b. `agent_rpc_json`：信封与校验

```erlang
-export([decode/1, encode/1, request/1, response_ok/2, response_error/3,
         notification/2]).
-export_type([envelope/0, error/0]).
```

语义（逐条对齐 JSON-RPC 2.0 + A2A §9.3）：

| 输入 | 结果 |
|---|---|
| 合法对象 `{"jsonrpc":"2.0","id":I,"method":M,"params":P}` | `{ok, #{jsonrpc := <<"2.0">>, id := I, method := M, params := P}}` |
| `params` 缺失 | `params => #{}`（统一成 map，方法自己校验） |
| `params` 是数组 | `{ok, ...}` 保留 `params => List`，由方法决定接受与否 |
| `jsonrpc` 不是 `"2.0"` / 缺失 | `{error, invalid_request}` |
| `method` 非字符串 | `{error, invalid_request}` |
| `id` 非 string/number/null | `{error, invalid_request}` |
| 无 `id` | `{error, invalid_request}`（S9 不实现 notification；A2A 方法全是请求-响应） |
| 顶层是数组（batch） | `{error, invalid_request}`，**在 ADR/能力矩阵标注「不支持 batch」** |
| 非 JSON | `{error, parse_error}` |

`encode/1` 用 OTP 28 `json:encode/1`，输出 iolist；`response_error/3` 生成 `{"jsonrpc":"2.0","id":I,"error":{"code":C,"message":M,"data":D}}`（`data` 缺省时不输出该键）。

**为什么 batch 直接拒**：A2A 1.0.0 的 JSON-RPC binding 只定义单请求；实现 batch 会引入「部分成功」语义，而 Task 创建是副作用。留到有真实客户端需求时再开口子（记录在能力矩阵）。

### 1c. `agent_rpc_error`：错误码与 HTTP 映射

```erlang
-export([code/1, http_status/1, message/1, known/1, jsonrpc_error/2]).
```

| 原子 | JSON-RPC code | HTTP | 说明 |
|---|---|---|---|
| `parse_error` | `-32700` | 400 | 响应 `id = null` |
| `invalid_request` | `-32600` | 400 | |
| `method_not_found` | `-32601` | 200 | 方法级错误，HTTP 仍 200（A2A §9.5 的 JSON-RPC 语义） |
| `invalid_params` | `-32602` | 200 | 同上 |
| `internal_error` | `-32603` | 500 | |
| `task_not_found` | `-32001` | 404 | |
| `task_not_cancelable` | `-32002` | 400 | |
| `push_notification_not_supported` | `-32003` | 400 | |
| `unsupported_operation` | `-32004` | 400 | |
| `content_type_not_supported` | `-32005` | 400 | |
| `invalid_agent_response` | `-32006` | 500 | |
| `extended_agent_card_not_configured` | `-32007` | 400 | |
| `extension_support_required` | `-32008` | 400 | |
| `version_not_supported` | `-32009` | 400 | |
| `unauthorized` / `forbidden` / `payload_too_large` | —（非 JSON-RPC） | 401/403/413 | 传输层直接回 HTTP，不包 JSON-RPC 信封 |

**依据**：A2A §5.4 的映射表（`-32001`→404、`-32006`→500、其余 A2A 专用错误→400）；JSON-RPC 标准码的 HTTP 映射按 A2A §9.5 的「方法级错误仍 200」处理。

### 1d. `agent_rpc_version`：`A2A-Version` 协商

```erlang
-export([parse/1, negotiate/2]).
%% parse(undefined) -> {ok, {0,3}}      ; 缺失/空按 0.3（proto/spec 原文）
%% parse(<<"1.0">>) -> {ok, {1,0}}
%% parse(<<"1">>)   -> {error, version_not_supported}   ; 只接受 Major.Minor
%% negotiate({1,0}, [{1,0}]) -> ok
%% negotiate({1,0}, [{0,3}]) -> {error, version_not_supported}
```

- 只做**头字段**形式，`?A2A-Version=1.0` 这类 query 参数形式**不实现**（收敛报告 §13 第 3 条）。
- 支持版本列表来自 adapter env（默认 `[{1,0}, {0,3}]`），但 S9-A 只**声明**支持 `1.0`；0.3 只用于「缺失头」的兼容读法，不承诺 0.3 的全部字段。
- 返回 `-32009` 时 HTTP 400，`data` 带 `supported: ["1.0"]`（字符串数组，非枚举）。

### 1e. `agent_rpc_http`：HTTP/1.1 请求解析（传输无关）

不重新实现 socket 循环，只提供「从 `{packet, http_bin}` 读到的片段组装成请求」的纯函数 + 响应序列化，供 `a2a_listener` 与后续 MCP 前端共用：

```erlang
-export([read_request/2, header/2, accepts_json/1, keep_alive/1,
         response/3, response/4, chunk/1, last_chunk/0]).
```

- `read_request(RecvFun, Limits)`：`RecvFun(0, Timeout)` 由 adapter 注入（内层是 `Transport:recv/3`），因此库本身不持有 socket。
- 必需规则：
  - `Content-Length` 优先；`Transfer-Encoding: chunked` 支持（S9-A 只用于读请求体，且有 `max_body_bytes` 上限）。
  - `Expect: 100-continue` → 先回 `HTTP/1.1 100 Continue\r\n\r\n` 再读体。
  - `Content-Length` 超过 `max_body_bytes` → `{error, payload_too_large}`，**不读体**，直接 `413` + `Connection: close`（防止慢速大包耗尽内存）。
  - `Connection: keep-alive`（HTTP/1.1 默认）与 `close` 都支持；响应头由调用方决定。
  - 头名统一小写；重复头合并为逗号串（A2A §9.2 要求 service parameter 可逗号合并）。
- `response/3,4` 只生成 `{Status, Headers, Body}` iolist，不发送。

**为什么不做 socket 层**：库不持有 socket，才能同时服务 ranch（`Transport:recv`）与测试里的 `gen_tcp`/`ssl`，也避免底座反向依赖 adapter 的 listener（ADR-026 §D1 的依赖方向）。

### 1f. `agent_rpc_sse`：SSE / chunked 写帧

```erlang
-export([event/1, event/2, comment/1, headers/0, heartbeat/0]).
%% event(Json)              -> [<<"data: ">>, Json, <<"\n\n">>]
%% event(<<"message">>, J)  -> 带 event 名
%% comment(<<"ping">>)      -> <<": ping\n\n">>
```

- 只负责**帧格式**，不管 socket；`Transfer-Encoding: chunked` 的 chunk 包装由 listener 调 `agent_rpc_http:chunk/1`。
- 约束：单事件 ≤ 1 MiB（`sse_max_event_bytes`），由调用方在编码后检查；帧内不允许裸 `\n`（JSON 编码器保证）。
- S9-A 只用测试证明帧字节正确，**不接业务流**（S9-C 才接）。

### 1g. `agent_rpc_auth`：凭据提取与 `broker_security` 接线

```erlang
-export([credential/1, authenticate/1, authorize/3, challenge/1]).
```

- `credential(Headers)`：
  - `authorization: Bearer <token>` → `{ok, {token, Token}}`
  - `x-api-key: <token>` → `{ok, {token, Token}}`
  - 两者同时存在且不同 → `{error, ambiguous}`（拒绝，不猜）
  - 都没有 → `none`
- `authenticate({token, T})` → `broker_security:authenticate_conn({token, T})`，返回 `{ok, #{principal, tenant_id}}` 或 `{error, _}`。
- `authorize(Identity, Action, Destination)` → `broker_security:authorize(Principal, #{action => Action, tenant_id => Tenant, destination => Destination})`。
- 401 响应带 `WWW-Authenticate: Bearer realm="a2a"`；403 不带挑战。
- **不**做 JWT 验签、不取 JWKS（ADR-026 §D7）。

### 1h. 套件

| 套件 | 覆盖 |
|---|---|
| `agent_rpc_json_SUITE` | 合法/非法信封 14 例、batch 拒绝、params 数组与缺失、id 类型、encode 往返 |
| `agent_rpc_error_SUITE` | 14 个错误码的 code/HTTP/message 三列一致；未知原子回 `internal_error` |
| `agent_rpc_version_SUITE` | 缺失/空/`1.0`/`1`/`1.0.0`/`2.0`/非 ASCII；协商成功/失败 |
| `agent_rpc_http_SUITE` | 纯函数级：CL/chunked/100-continue/超限/重复头/keep-alive 判定；`response/3` 字节 |
| `agent_rpc_sse_SUITE` | `data:` 帧、event 名、comment/心跳、chunk 包装、1 MiB 边界 |
| `agent_rpc_auth_SUITE` | Bearer / X-API-Key / 同时存在 / 缺失；真实 `broker_security` 用户表 401/403 |

**本 Task 不新建 listener，因此不占用任何端口。**

---

## Task 2 — `broker_proto_a2a`：listener + JSON-RPC echo

**目标**：起一个真实 HTTP/1.1 前端，把 `POST /` 的 JSON-RPC 请求接到一个方法 `SendMessage`（echo），并把认证/版本/限额/错误模型全部跑通。**不建 Task**（S9-B）。

### 2a. app 骨架与监督树

```
apps/broker_proto_a2a/
├── src/broker_proto_a2a.app.src
├── src/broker_proto_a2a_app.erl
├── src/broker_proto_a2a_sup.erl        # listeners 是最后一项
├── src/a2a_listener.erl                # ranch_protocol
├── src/a2a_conn.erl                    # 每连接循环（keep-alive）
├── src/a2a_rpc.erl                     # 方法分派 + 版本/认证/限额
└── test/a2a_listener_SUITE.erl
```

`broker_proto_a2a_sup`：

```erlang
init([]) ->
    Children = [listener_spec(S) || S <- listeners()],
    {ok, {#{strategy => one_for_one, intensity => 1, period => 5}, Children}}.
```

- `listeners()` 默认 `[]`（ADR-026 §D4）；`[]` 时零子进程，既有节点行为不变。
- `listener_spec/1` 照 `broker_proto_ws_sup`：`broker_security_tls:transport()` 决定 `ranch_tcp`/`ranch_ssl`；`socket_opts` 额外带 `{packet, http_bin}`、`{active, false}`、`{reuseaddr, true}`。
- `listener` 为**最后一项**：任务分片/registry（S9-B 才加）排在它前面，listener 起来时依赖已就绪。

### 2b. `a2a_listener` + `a2a_conn`

ranch 2.3 的 `ranch_protocol` 回调：

```erlang
start_link(Ref, _Transport, Opts) ->
    Pid = spawn_link(fun() -> a2a_conn:init(Ref, Opts) end),
    {ok, Pid}.

%% a2a_conn:init/2
{ok, Socket} = ranch:handshake(Ref),      %% 由连接进程自己取 socket
a2a_conn:loop(Socket, Transport, Opts).
```

- `{packet, http_bin}` 让 OTP 解析请求行/头；读体前切 `{packet, raw}`（与 `broker_admin_listener` 同一手法）。
- **每连接一个进程、循环处理多个请求**（keep-alive）；`Connection: close` 或读超时/错误才退出。
- 保留时长：`request_timeout_ms`（默认 30 s）、`idle_timeout_ms`（默认 75 s）。
- 崩溃隔离：handler 内 `try ... catch`，回 `500` 后按 `Connection: close` 关闭，**不**把异常泄成空连接。
- 每连接 `max_requests`（默认 1000）防止单连接无限占用。

### 2c. `a2a_rpc`：唯一方法 `SendMessage`（echo）

路由：

| 请求 | 行为 |
|---|---|
| `POST /` 或 `POST /rpc` | JSON-RPC；其余路径 `404`（S9-B 才加 `/.well-known/agent-card.json`） |
| `GET /health` | `200 {"status":"ok"}`（无认证；仅供探活） |
| 其他方法 | `405` + `Allow: POST, GET` |

处理顺序（顺序即安全边界）：

1. `Content-Type` 必须是 `application/json`（或 `application/a2a+json`）→ 否则 `415`。
2. `Content-Length`/chunked 体 ≤ `min(broker_limits:max_message_bytes(), max_body_bytes)` → 否则 `413`。
3. `require_auth`（adapter env，默认 `true`）：
   - `false` 时：仅当监听地址是回环才允许启动（boot 校验，见 2d）；
   - `true` 时：提取凭据 → `agent_rpc_auth:authenticate/1`；缺失/错误 → `401`。
4. `A2A-Version` 协商 → 不支持 `-32009`（HTTP 400）。
5. JSON-RPC 解析 → `-32700`/`-32600`。
6. `method`：
   - `SendMessage` → 校验 `params.message`（`messageId` 非空、`role` 是允许值、`parts` 是非空数组、每个 Part 恰好一个 content 键）→ `invalid_params` 否则；
   - ACL：`publish` + destination `$a2a/task/echo`（S9-A 的固定 destination；S9-B 换成真实 Task id）；
   - 其他方法 → `-32601`。
7. 成功响应：`{"jsonrpc":"2.0","id":I,"result":{"message":{...}}}`：
   - echo 原 `parts`/`metadata`，`role` 改成 `"ROLE_AGENT"`；
   - `messageId` 用**新的** base64url 随机 id（不能复用客户端 id，否则客户端去重会吞掉响应）；
   - `contextId` 沿用请求里的（没有则生成）；
   - **不**产生 Task、不写日志（S9-B 才写 `$a2a/task/<id>`）。

**为什么 echo 是 `SendMessage` 而不是自定义方法**：A2A 1.0.0 的 `SendMessageResponse` 允许 `message` 分支（proto oneof），所以不建 Task 也能给出符合规范的响应；自定义 `Echo` 方法会污染方法表，跟「11 个 PascalCase 方法」的事实冲突。

### 2d. 配置、boot 顺序与 relx

`config/sys.config.src` 追加（放在 `broker_proto_ws` 之后）：

```erlang
{broker_proto_a2a, [
  %% ADR-026 §D4: 默认不起 listener，release 显式绑定回环。
  {listeners, []},
  %% 默认必须认证：新面没有历史兼容包袱，失败关闭。
  {require_auth, true},
  {max_body_bytes, 1048576},
  {max_connections, 1024},
  {request_timeout_ms, 30000},
  {idle_timeout_ms, 75000},
  {supported_versions, [<<"1.0">>]},
  {sse_max_event_bytes, 1048576},
  {sse_heartbeat_ms, 15000}
]}.
```

- **不**动 `broker_security` 的 `{require_auth, []}` 与 `credential_capable/0`（ADR-026 §D4）。
- boot 校验（`broker_proto_a2a_app:start/2`）：`require_auth = false` 且 listener 的 `ip` 不是回环 → `{error, {insecure_listener, ...}}`，拒绝启动。
- relx：默认清单与 `quic` 清单**两处**都追加 `broker_proto_agent_rpc`、`broker_proto_a2a`（ADR-024 §D5 的教训；profile 的 `relx` 会整份覆盖）。
- `.app.src` 的 `applications` 含 `broker_proto_agent_rpc`、`broker_security`、`broker_limits` 所在 app。

### 2e. `a2a_listener_SUITE`（真实 socket，`port => 0`）

| 用例 | 断言 |
|---|---|
| `echo_roundtrip` | `POST /` + 合法 `SendMessage` + Bearer → 200，`result.message.role = "ROLE_AGENT"`，`parts` 原样，`messageId` 与请求不同 |
| `version_missing_is_0_3` | 不带头 → 仍走 0.3 读法，200（不报错） |
| `version_unsupported` | `A2A-Version: 2.0` → 400，`error.code = -32009` |
| `no_credential_401` | 无 `Authorization` → 401 + `WWW-Authenticate: Bearer` |
| `bad_credential_401` | 错 token → 401，错误体与「无凭据」不可区分 |
| `acl_denied_403` | 用户存在但 ACL 拒绝 `publish` → 403 |
| `unknown_method` | `method: "Nope"` → 200 + `-32601` |
| `parse_error` | 非 JSON 体 → 400 + `-32700`，`id = null` |
| `invalid_params` | `message` 缺 `parts` → `-32602` |
| `payload_too_large_413` | `Content-Length` > 上限 → 413 + `Connection: close`，**不读体** |
| `keep_alive_two_requests` | 同一 socket 连发两个请求，两次都 200 |
| `connection_close_honored` | `Connection: close` 后服务端关连接 |
| `health` | `GET /health` → 200 `{"status":"ok"}`，不需要认证 |
| `require_auth_false_loopback_only` | 非回环 + `require_auth=false` → `application:start` 失败（boot 校验） |

**真实客户端证据**：除 CT 外，用 `curl` 落一条命令与输出到 `docs/protocol-compatibility.md` 的 S9 草稿段（S9-F 再整理成矩阵行）。

---

## Task 3 — 全量验证与文档回填

### 3a. 门禁

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
rm -rf _build/test/extras
rebar3 ct
rebar3 dialyzer
rebar3 xref
rebar3 release
rebar3 as quic release
```

### 3b. 用例数回填（强制）

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
erl -noshell -pa _build/test/lib/*/ebin -eval '
  Suites = [agent_rpc_json_SUITE, agent_rpc_error_SUITE, agent_rpc_version_SUITE,
            agent_rpc_http_SUITE, agent_rpc_sse_SUITE, agent_rpc_auth_SUITE,
            a2a_listener_SUITE],
  [io:format("~s ~p~n", [S, length(S:all())]) || S <- Suites],
  halt().'
```

基线 823 + 本阶段新增，两个数都写，并区分「基线」与「S9-A 新增」。

### 3c. 文档同步（本阶段只动 3 个文件）

| 文件 | 变更 |
|---|---|
| `docs/architecture.md` | app 列表加 `broker_proto_agent_rpc` / `broker_proto_a2a`；boot 顺序；端口表加 8081（默认不监听） |
| `docs/operations.md` | 依赖图；`broker_proto_a2a` 的 env 与「默认 `listeners => []`」；`require_auth` 默认 `true` |
| `progress.md` | S9-A 进展 + 用例数 + 下一步（S9-B） |

`docs/roadmap.md` 的 S9-A 行改「✅ 已交付」并回填证据；`docs/security.md` §12、`README.md`、ADR **不动**（S9-F 统一收口，避免把计划读成现状）。

### 3d. 提交

```bash
git commit -m "feat(s9-a): agent rpc base library and A2A JSON-RPC echo front end"
git commit -m "docs(s9-a): record agent rpc and a2a listener in architecture and progress"
```

---

## 完成判据（Definition of Done）

- [ ] `broker_proto_agent_rpc` 纯库建立：无 supervisor、无 listener、无自有 env；6 个模块 + 6 个套件通过
- [ ] `agent_rpc_json` 拒绝 batch，错误码严格按 JSON-RPC 2.0；`agent_rpc_error` 的 code/HTTP/message 三列与 A2A §5.4 一致
- [ ] `agent_rpc_version` 缺失按 `0.3`，不支持回 `-32009`（只实现头形式）
- [ ] `agent_rpc_http` 覆盖 CL/chunked/100-continue/超限/重复头/keep-alive 纯函数级用例
- [ ] `agent_rpc_sse` 帧格式与 chunk 包装通过，未接业务流
- [ ] `broker_proto_a2a` 起 listener，`SendMessage` echo 返回符合 `SendMessageResponse.message` 形状
- [ ] `401`/`403`/`413`/`415`/`-32009`/`-32601`/`-32602`/`-32700` 各一条用例
- [ ] `require_auth=false` + 非回环 → 拒绝启动
- [ ] `rebar3 ct` 总例数 = 823 + 新增，零 FAILED；dialyzer/xref 干净；两种 release 都含新 app
- [ ] `curl` 证据落盘；`architecture.md` / `operations.md` / `progress.md` / `roadmap.md` 回填

## 风险与对策

| 风险 | 对策 |
|---|---|
| 自建 HTTP/1.1 的 chunked 解析写错（请求体） | 只用它读请求体，且请求体走「CL 优先」；chunked 仅一条用例，失败即回 `400` |
| `{packet, http_bin}` 与 `{active, false}` 混用导致 `recv` 行为不同 | 所有 socket 读取都集中在 `a2a_conn`，库不碰 socket；CT 用真实 socket 验证 |
| ranch 2.3 的 `ranch:handshake/1` 时机 | `a2a_listener:start_link/3` 里 `spawn_link` 后由子进程 `handshake`，与 ranch 文档一致；CT 起真实 listener 验证 |
| echo 方法被误读成「A2A 已实现」 | 文档与能力矩阵写「S9-A 只有 `SendMessage` echo，无 Task」；`GetTask` 等一律 `-32601` |
| 默认 `require_auth=true` 影响既有部署 | 新 app 默认 `listeners => []`，不监听即无影响；既有 823 例不启动该 app |

## 不在本阶段（明确留给后续阶段）

| 项 | 阶段 |
|---|---|
| Task 状态机、`GetTask`/`ListTasks`/`CancelTask`、`$a2a/task/<id>` 日志、投影与恢复 | **S9-B** |
| Agent Card 表、well-known、ETag/缓存 | **S9-B** |
| SSE 业务流、`SubscribeToTask`、Push config、出站 webhook | **S9-C** |
| MCP / ACP | **S9-D / S9-E** |
| 能力矩阵、压测、`security.md` §12、README 状态行 | **S9-F** |
