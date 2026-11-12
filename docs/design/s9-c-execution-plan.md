# S9-C 执行计划 — SSE 流式任务事件 + 出站 Push Webhook（含 SSRF/签名/重试/DLQ）

- **对应设计**：[`docs/design/s9-agent-protocols.md`](s9-agent-protocols.md) §4.4 S9-C 行；[ADR-025](../adr/025-a2a-task-log-mapping.md)（Task → 内核映射，retention pin）；[ADR-026](../adr/026-agent-adapter-front-end.md) §D6（出站 webhook 8 条冻结约束，逐条落地）；[《S9-0 收敛报告》](s9-0-convergence-report.md) §2（以 `spec/a2a.proto` 复核的 wire 事实）
- **基线（改动前）**：`rebar3 ct` **952/952、62 套件**；dialyzer / xref / `rebar3 release` / `rebar3 as quic release` 通过；HEAD `4d2d023`（S9-B 交付点）
- **范围**：把 S9-B 的 Task 生命周期接上两种「不阻塞客户端」的交互面——
  1. **SSE 流式**：`SendStreamingMessage` 与 `SubscribeToTask` 走 `text/event-stream`，每个 `data` 是一个 JSON-RPC 2.0 Response（`Task`/`TaskStatusUpdateEvent`/`TaskArtifactUpdateEvent`），流在终态或中断态关闭；
  2. **出站 Push**：Task 到终态/中断态时向租户配置的 webhook 发 `POST`（带 `X-A2A-Timestamp`/`X-A2A-Nonce`/可选 HMAC 头），带 SSRF 白名单校验、pin IP、禁重定向、重试退避、TTL、耗尽写 DLQ——**不阻塞任务终态**。
- **阶段顺序**：S9-C 是 S9-D（MCP 8082）/S9-E（ACP stdio）的前置。S9-D/E 各自另写计划。
- **执行约束**（仓库既定规则）：
  - 编译/测试前 `export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"`；`rebar3 ct` 前 `rm -rf _build/test/extras`
  - 用例数一律由 `all/0`（含 `groups/0`）实数回填，不预先承诺
  - 不新增运行时依赖：仍只有 `ranch 2.3.0` + 内置 `ra 3.2.0` + OTP 28 自带 `json`
  - 不杀用户进程/容器；CT 一律 `port => 0`，**不动 8081/8082 固定端口**
  - 数据目录在 ext4、release 树外；Python 用 `/usr/bin/python3`
  - 不改 `broker_security` 既有语义（`credential_capable/0`、`{require_auth, []}`）
  - ADR-025/026 冻结文本不改；偏差处按项目惯例加「落地后修正 / 落地偏差记录」批注

---

## 目标与「不做」

**S9-C 结束时可以证明的事实（Gate 原文）**：官方 A2A SDK/sample 跑通流式任务（`SendStreamingMessage` 起流 → 状态/产物事件 → 终态关流）、断线重订阅（`SubscribeToTask` 从 offset 0 重放历史再跟随）；出站 Push webhook 在 SSRF 白名单内投递到本地 receiver、签名头可验、失败按指数退避重试、耗尽写 `$a2a/push/dlq/<tenant>` 且**不阻塞任务终态**；既有 952 例零回归。

**明确不做**（留给后续阶段）：

| 项 | 阶段 |
|---|---|
| MCP app（8082 固定端口 + `/mcp` + JSON-RPC initialize） | S9-D |
| ACP app（stdio 子进程，ACP v2 草案子集） | S9-E |
| 能力矩阵终稿、`security.md` §12、README 状态行 | S9-F |
| a2a_* 失败路径计数器（`a2a_push_dlq`/`a2a_stream_*` 等） | S9-F（统一动 `broker_metrics` 冻结键集） |
| OAuth AS / JWKS / JWT 验签 | 不做（ADR-026 §D7 非目标，永久不做） |

---

## Task 1 — `broker_meta`：新增 `push_config` 表（amends ADR-002）

**目标**：按 ADR-026 §D6 在 `broker_meta` 新增第 13 张表 `push_config`，只新增不改既有 12 张表形状。配置行**不存密钥**（密钥在 `push_auth_secrets` env，按 `config_id` 取用）。

- `tables/0` 追加 `{push_config, [push_config, {TenantId, TaskId, ConfigId}, TenantId, TaskId, ConfigId, Url, AuthScheme, AllowPrivate, UpdatedAt, Version]}`（主键 `{TenantId, TaskId, ConfigId}`；`Url` 为 binary，`AuthScheme` 为 `undefined | binary`）。
- 新增 5 个 helper：
  - `put_push_config/1`（upsert，`version` 单调 +1、`updated_at` 刷新）
  - `get_push_config/3`、`push_configs/2`（按 `{TenantId, TaskId}` 列出，供分片投递时读取）
  - `delete_push_config/3`（按三元组）、`delete_push_configs/1`（按 `{TenantId, TaskId}` 全删）
- 写前 schema 校验在 RPC 层做（`url` 必须 `http`/`https` 且 host 非空），`broker_meta` 只存不校验。
- 更新 `broker_meta_SUITE` 的 `schema_has_the_frozen_tables` 断言：表清单加入 `push_config`（这是 ADR-026 §D6 amends ADR-002 的机械后果）；新增 CRUD 用例。

## Task 2 — `a2a_stream`：SSE 全流程（订阅 → 回放 → 跟随 → 关流）

**目标**：ADR-025 §D7（retention pin 到订阅）+ §D6（终态/中断态关流）。

- `a2a_stream_plan/4`（RPC 层）：校验 `taskId` 属于租户 → 从 `broker_queue` 求 `$a2a/task/<id>` 的当前订阅与 offset → 返回 `{ok, #{id, task_id, destination, partition}}`，由 `a2a_conn` 识别 `{stream, Plan}` 并接管 socket（不建 SubRef、不 pin retention，照 ADR-025 §D7 落地）。
- `a2a_stream:run/3`：
  1. **订阅分区邮箱钩子** → 2. **回放**（`drain`：把 `From` 起的历史事件逐个转成 SSE `data`）→ 3. **跟随**（`await`：阻塞等新事件；心跳走 SSE comment 行 `: ping`，间隔 `sse_heartbeat_ms`）。
  4. 遇到终态/中断态事件 → 转发后**关流**（`closed`/`stopped`）；到达 `sse_max_duration_ms` → 同样关流（响亮失败，不无限挂连接）。
  5. `From` 越界（历史被 GC）→ 发一个 `status: failed` 事件后关流（ADR-025 §D6 `history_lost` 的 wire 表达）。
- SSE 帧：每事件 `data: <json>\n\n`；`json` 是一个完整 JSON-RPC 2.0 Response（照 R-T2）。

## Task 3 — `a2a_push` + `a2a_push_auth`：出站投递与自证/防重放头

**目标**：ADR-026 §D6 第 3/4/6 条（pin IP、禁重定向、只允许状态行、HMAC/Bearer/API key）。

- `a2a_push:guard/3`（纯校验）：
  - 两层 allowlist（`egress_allowlist` 全局 + 租户表）求交；空 allowlist → `empty_tenant_allowlist`。
  - `validate_url/2`：scheme 必须 `https`（`http` 需 `push_allow_http => true`，仅供本地测试）；host 后缀匹配；`resolve` 后逐个 IP 过 `is_private_address/1`（loopback/私有/链路本地/组播/保留），除非该条目 `allow_private => true`。
  - 返回 `#{scheme, host, port, path, addresses, allow_private}`（含已解析地址，供 pin）。
- `a2a_push:deliver/3`：`gen_tcp`/`ssl` 手写 HTTP/1.1 `POST`，**连到已校验 IP**（带 `Host` 头），禁 3xx（收到即失败），只读状态行，响应体上限 `push_max_response_bytes`（64 KiB）；超时用 `push_timeouts`。
- `a2a_push_auth:headers/3`：`X-A2A-Timestamp`（ms）+ `X-A2A-Nonce`（随机）+ 按 scheme 出 `Authorization: Bearer <token>` 或 `X-A2A-Signature: sha256=<hex>`（HMAC 对 `timestamp.nonce.body`，密钥取 `push_auth_secrets[Scheme]`——**scheme 名是配置键名**，不是自由字符串；客户端命名了未配置的 scheme 就跳过投递，不降级为明发）。

## Task 4 — `a2a_push_worker`：重试/退避/TTL/DLQ（不阻塞终态）

**目标**：ADR-026 §D6 第 7 条（失败语义 + DLQ）。

- 仅 `push_enabled => true` 时把 `a2a_push_worker` 挂进 `broker_proto_a2a_sup`（`push_worker_specs/0`）；否则所有 `Create/Get/List/DeleteTaskPushNotificationConfig` 回 `-32003` / HTTP 400（`push_notification_not_supported`）。
- `enqueue/4`（cast）：分片在 `emit/3` 的终态/中断态挂点 `maybe_push/2` 调用——**cast 不阻塞任务终态**；worker 内 `prepare`（guard + auth 头）→ `deliver`。
- 失败按 `push_max_attempts` 次重试，指数退避（`push_base_backoff_ms` × 2，封顶 `push_max_backoff_ms`）；超过 `push_ttl_ms` → 放弃。
- 耗尽/过期 → `write_dlq`：构造 `#broker_message{destination = <<"$a2a/push/dlq/", Tenant/binary>>, payload = JSON(taskId/configId/url/attempts/reason), reliability = fsync}`，`broker_router:env()` 解包 `{ok, Env}` 后 `Backend:route/3` + `broker_queue:append/3`（消息 id = task id）。**DLQ 写失败只 `logger`，不抛**（任务已终态）。

## Task 5 — 方法接线：`SendStreamingMessage`/`SubscribeToTask` + Push CRUD

**目标**：`a2a_rpc.erl` 新增 6 个方法分派。

- `SendStreamingMessage`：创建 Task（`task_auto_complete` 控制同步完成）→ 返回 `{stream, Plan}`（`a2a_conn` 识别后接管 socket 走 SSE）。
- `SubscribeToTask`：`{stream, Plan}`（同上）。
- `Create/Get/List/DeleteTaskPushNotificationConfig`：`push_method_enabled()` 为假 → `-32003`；否则走 Task 4 的 CRUD（`config_id` 由服务端 `push_config_id/2` 生成，防枚举）。
- `classify/1` 新增 `push_notification_not_supported`（映射 `-32003`/HTTP 400）。

## Task 6 — 全量验证与文档回填

- 门禁：`rebar3 ct`（删 `_build/test/extras`）、`dialyzer`、`xref`、`release` + `as quic release`。
- 用例数由 `all/0` 回填；基线 952 区分「基线」与「S9-C 新增」。
- 文档：`docs/protocol-compatibility.md`（S9-C 段：SSE 帧序 + curl 证据 + Push SSRF/签名/DLQ）、`docs/architecture.md`（`broker_meta` 13 表、SSE 数据流、出站 webhook 链路）、`docs/operations.md`（egress 配置 + DLQ 订阅 + retention 估算）、`progress.md`、`docs/roadmap.md` §11.1 S9-C 行。
- ADR-025/026 冻结文本不改；偏差加「落地后修正」批注。

## 完成判据（Definition of Done）

- [x] `broker_meta` 新增 `push_config` 表（13 表），CRUD + 租户隔离 helper 通过；冻结表断言更新
- [x] SSE：`SendStreamingMessage`/`SubscribeToTask` 走 `text/event-stream`，事件是 JSON-RPC Response；终态/中断态关流；心跳 comment；`sse_max_duration_ms` 上限；未知任务 404
- [x] Push 出站：本地 receiver 收到终态事件 POST；SSRF 白名单/pin IP/禁重定向/只读状态行/64 KiB 上限逐项有拒绝用例；签名头可验
- [x] 重试：`push_max_attempts` 次指数退避后写 `$a2a/push/dlq/<tenant>`（可订阅、可审计）；DLQ 写不阻塞任务终态
- [x] `push_enabled => false` 时 worker 不在监督树，4 个 CRUD 方法回 `-32003`/HTTP 400
- [x] `rebar3 ct` 总例数 = 952 + 新增（实际 **973**），零 FAILED；dialyzer/xref 干净；两种 release 都含新代码
- [x] `curl` 真实客户端证据落盘；`protocol-compatibility.md` / `architecture.md` / `operations.md` / `progress.md` / `roadmap.md` 回填

### 用例数（S9-C 交付时点，均由 `all/0`（含 `groups/0`）实数）

| 套件 | 例数 | 说明 |
|---|---|---|
| `a2a_stream_push_SUITE` | **20**（新套件） | 三组：`stream` 7（流式建 Task+终态关流 / 帧是 JSON-RPC Response / `SubscribeToTask` offset 0 重放 / 未知任务 404 / 心跳保活 / 中断态关流 / 慢读者不 pin retention）、`push` 5（禁用回 -32003 / CRUD 往返 / 配置租户隔离 / 终态事件投递 / 重试后写 DLQ）、`guard` 8（私网拒绝 / 空 allowlist 拒绝 / 越权 host 拒绝 / 非 https 拒绝 / 重定向拒绝 / 显式 allow_private 放行 / 签名头存在且 HMAC 正确 / 未配置 auth scheme 拒绝） |
| `broker_meta_SUITE` | 12 → **13** | 冻结表断言改为 13 表；新增 `push_config_crud_and_tenant_isolation`（CRUD + 版本单调 + 跨租户不可见） |

**基线 952 → 973（净增 21、套件 62 → 63）**：新增 1 个套件 20 例（`a2a_stream_push_SUITE`）+ `broker_meta_SUITE` +1 = **21**，逐项相加与总数一致。

### 门禁（实测，2026-11-12）

| 门禁 | 命令 | 结果 |
|---|---|---|
| 全量 CT | `rm -rf _build/test/extras && rebar3 ct` | **973 passed / 0 failed、63 套件**（基线 952/62，零回归；本轮新增 1 套件 + 20 例 + 扩 1 套件 +1） |
| dialyzer | `rebar3 dialyzer` | **0 warning**（过程中修掉 `a2a_push:resolve_and_validate/5` 不可达 `{ok, []}` 分支、`a2a_rpc:a2a_stream_plan/3` 不可达 `_ ->` 分支、`a2a_stream:stream/5` 不可达 `error ->` 分支） |
| xref | `rebar3 xref` | 除既有 unused-export 外无新告警（新代码零告警） |
| release | `rebar3 release` | 成功，`lib/` 含 `a2a_push`/`a2a_push_auth`/`a2a_push_worker`/`a2a_stream` |
| quic release | `rebar3 as quic release` | 成功，同上游 |

## 落地偏差记录（执行时追加）

| # | 计划原文 | 实际落地 | 原因 |
|---|---|---|---|
| 1 | Task 1：配置行含密钥（`TaskPushNotificationConfig.token`） | 配置行**不存密钥**：`push_config` 表列是 `url`/`auth_scheme`/`allow_private` 等 10 列，HMAC 密钥单独放 `push_auth_secrets` env（**按 scheme 名取**，即客户端命名的 `authentication.scheme`） | ADR-026 §D6 第 6 条明令「不接受客户端明文密钥」；密钥落库会把 SSRF 之外的第二类敏感面引入 `broker_meta`（Mnesia 持久化 + 备份外泄），env 隔离更安全 |
| 2 | Task 1：`config_id` 用客户端传入 | 服务端 `push_config_id/2` 生成（`base64url(sha256(Tenant0Task0Url))[:9]`） | 防枚举：客户端自选 id 可被遍历探测他人 webhook |
| 3 | Task 2：`a2a_conn` 为 SSE 建 SubRef 并 pin retention | 照 ADR-025 §D7 **不建 SubRef、不 pin**；只订阅分区邮箱钩子 + 回放 + 跟随；终态/中断态关流 | retention pin 会让一个永不结束的订阅永久抬高分区水位；S9-C 用「重订阅从 offset 0 重放」达成等价可用性，不引入 pin |
| 4 | Task 4：DLQ 消息 id 用 `config_id` | 用 **task id** 作 `message_id`（DLQ destination 是 `$a2a/push/dlq/<tenant>`，单条 destination 容纳所有失败，payload 里标 `taskId`） | task id 全局唯一且即失败主体，便于按任务关联；`config_id` 在 payload 内 |
| 5 | Task 4：`write_dlq` 直接 `broker_queue:append` | 先 `broker_router:env()` 解包 `{ok, Env}` 再 `Backend:route/3` + `append` | `broker_router:env()` 返回 `{ok, Env}` 元组，不解包会 `function_clause`；照 `broker_event:write_quarantine/2` 范式 |
| 6 | Task 3/5：SSRF 校验在**配置写入**时拒绝越权 host | SSRF 校验只在**投递期**（`a2a_push:guard/3`）生效；`CreateTaskPushNotificationConfig` 只校验 `url` 形态，越权 host 写入成功、投递被 `guard_refused` 跳过且从不拨号 | 白名单是出站策略、配置是声明；投递期判定才能让「先配后开」与配置重载自然工作。ADR-026 §D6 第 1 条「空 allowlist 拒绝配置创建」同样按此落地（已在 ADR-026 加落地注记） |
| 7 | Task 2：D7 的 `sse_max_pending_bytes`（1 MiB）/ `sse_send_timeout_ms`（30 s）与 `metadata._a2a.resync` 快照 | **未落地**：改以 `sse_max_duration_ms` 作出一条流的时间上界 + 单帧 `sse_max_event_bytes`；历史越界时发 `status: failed` 后关流（不搭 resync 快照） | SSE 写在同一连接进程内同步 `send`，socket 发送缓冲自然反压、不堆 mailbox，字节水位无落点；resync 要求连接进程读投影（耦合 shard ETS），关流+重订阅用同一路径达成等价可用性。两个 env 已从 `config/sys.config.src` 移除（不留「声明但不读」的死配置） |
