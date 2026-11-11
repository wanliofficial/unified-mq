# S9-0 收敛报告 — Agent 接入层的不确定性收敛

- **文档版本**：v1.0（S9-0 交付，2026-11-11）
- **对应**：[`docs/roadmap.md`](../roadmap.md) §11.1（S9-0）、[`docs/design/s9-agent-protocols.md`](s9-agent-protocols.md) §4.4（OQ-1~OQ-10）
- **状态**：**收敛结论已冻结；S9 代码未开工**。本报告只含协议事实、代码事实与决策；不含任何性能/容量数字，不含「已实现」声明。S9-A 起才产生代码与实测数字。
- **落盘 ADR**：[ADR-025](../adr/025-a2a-task-log-mapping.md)（Task → 内核映射）、[ADR-026](../adr/026-agent-adapter-front-end.md)（HTTP/SSE 前端、Agent Card 落点与边界声明）
- **证据口径**：协议结论以权威原文为准，代码结论以本仓库 HEAD `076228d` 为准；「规划稿口径」指尚未逐字复核的转述，单列在 §13。

---

## 0. 结论速览

| OQ | 问题 | 收敛结论 | 落点 | 是否修正规划稿 |
|---|---|---|---|---|
| OQ-1 | A2A 精确常量/字段/方法/错误码 | 以 `specification/a2a.proto` @ main 为准；9 个 `TASK_STATE_*`、`supportedInterfaces[]`、11 个 PascalCase 方法、错误码 `-32001`~`-32009`；JSON 字段 camelCase、枚举保持 `SCREAMING_SNAKE_CASE` | 本报告 §2 | 是（§2.1 多处按 0.3.0 风格书写） |
| OQ-2 | Task 状态落点与可靠性档 | **事件溯源**：Task 事件是普通 `#broker_message{}`（追加日志），Task 是事件流在 adapter 内的投影；**不进 Mnesia、不用订阅游标**；可靠性档绑 ADR-007；retention 是运维契约，越界「响亮失败」 | **ADR-025** | 是（§1.2 的「订阅游标」不成立） |
| OQ-3 | Agent Card / catalog 落点 | `broker_meta` **新增 `agent_card` 表**（控制面、低频），不复活 `topic_meta`/`queue_meta`/`binding_meta` | **ADR-026 §D3** | 否（沿用 ADR-002 方向，需修订表清单） |
| OQ-4 | HTTP/SSE 服务端选型 | **ranch 2.3.0 + `{packet, http_bin}` + OTP 28 `json`**，自建 HTTP/1.1 前端；不引入 cowboy，不复用 `broker_admin`/`broker_proto_ws` | **ADR-026 §D2** | 否（细化） |
| OQ-5 | SSE 长连接与背压共存 | SSE 是**瞬态读者**，不建 cursor、不 pin retention；出站有界 + `send_timeout`；落后则发 Task 快照 resync；执行与流式解耦 | **ADR-025 §D7**、本报告 §4 | 否（细化） |
| OQ-6 | ACP 侧别与传输 | **只做 stdio；Broker 扮演 Client 侧**；`fs/*`/`terminal/*`/`elicitation` 不广告；`session/request_permission` 默认拒绝 + allowlist；v2 不实现 | **ADR-026 §D5** | 否（冻结侧别） |
| OQ-7 | 出站 webhook egress | 默认关闭；**host allowlist + 解析后校验并 pin IP + 禁重定向 + 超时/响应上限**；不用 `httpc` 作 SSRF 边界 | **ADR-026 §D6** | 否（细化） |
| OQ-8 | OAuth AS / JWKS 是否纳入 | **不纳入**。只接受不透明 bearer token（`broker_user` 表），不做 JWT 验签；Agent Card 只声明外部 AS | **ADR-026 §D7** | 否（冻结边界） |
| OQ-9 | 大 Artifact 与消息上限 | 请求体与单 Part 受 `broker_limits.max_message_bytes`（默认 1 MiB）约束；`raw` base64 膨胀 ~4/3；更大内容走 `url`（Broker **不取回**）；分块 `append` 每块独立受限 | **ADR-025 §D8** | 否（细化） |
| OQ-10 | 三协议与既有端口/listener/装配 | `broker_proto_agent_rpc`（纯库）+ 三个 adapter app；A2A 8081 / MCP 8082（默认 `listeners => []`，release 绑回环）；ACP 无端口；默认与 `quic` 两份 relx 清单都要加 | **ADR-026 §D4** | 否（细化） |

> 读法：本表「收敛结论」是决策，不是现状。S9 的 Gate 项仍全部未勾选；基线仍是 `rebar3 ct` 823/823、52 套件。

---

## 1. 证据与口径

### 1.1 外部规范

| 来源 | 取用部分 | 抓取日期 |
|---|---|---|
| `github.com/a2aproject/A2A` @ `main` | `specification/a2a.proto`（唯一权威数据模型）；`docs/specification.md` §3.3.2/§3.6.2/§5.3/§5.4/§5.5/§8/§9.5 | 2026-11-11 |
| `modelcontextprotocol.io/docs/2026-07-28/learn/architecture` | 数据层/传输层、`server/discover`、`_meta` 无状态、`subscriptions/listen`、`sampling`/`logging` 弃用 | 2026-11-11 |
| `agentclientprotocol.com/protocol/v1/overview` | 方法表、双边请求、JSON-RPC 2.0、`camelCase` 键 / `snake_case` 判别值、绝对路径 | 2026-11-11 |

### 1.2 代码事实（HEAD `076228d`）

| 事实 | 位置 |
|---|---|
| `broker_meta` 有 11 张控制面表，含无运行时读取方的 `topic_meta`/`queue_meta`/`binding_meta` | `apps/broker_meta/src/broker_meta.erl:33-44` |
| `broker_meta` 提供泛型 `put/2`、`get/2`、`all/1`、`delete/2`、`clear/1` 与领域 helper | `apps/broker_meta/src/broker_meta.erl:17-19,111-140` |
| cursor 是 per-SubRef 的**单调消息位置**（`sub_ref() = {sub_kind(), Key, Filter}`），不是任务状态机 | `apps/broker_subscription/src/broker_subscription.erl:52-54`；`broker_cursor.erl:1-19`；ADR-013 §13.2 |
| 新订阅的 cursor 初始化在**分区尾部**（`High`），由消费者 pull/ack 推进 | `broker_subscription_worker.erl:632-660`（`reply_new_subscribe2` + `tail_offsets/2`） |
| 非匹配/过期消息由读者 `ack_skip` 跳过（否则 `committed` 钉在该条），未 pull 的订阅会钉住 retention | `mqtt_conn_fsm.erl:1822-1826,1869-1874`；ADR-014 §14.1 |
| 生产 webhook 无任何出站 HTTP 客户端（无 `httpc`/`hackney`/`gun`，`inets` 也未列依赖） | `grep` 全 `apps/` 与 `rebar.config` |
| `broker_admin_listener` 是自研 HTTP/1.1：`gen_tcp` + `{packet, http_bin}` + 每连接短进程 + `Connection: close` | `apps/broker_admin/src/broker_admin_listener.erl:86-107,175-183` |
| `broker_security_tls:transport/0` 返回 `{ranch_tcp, []}` / `{ranch_ssl, Opts}`，七个协议 supervisor 共用 | `apps/broker_security/src/broker_security_tls.erl:87-92`；`security.md` §9 |
| OTP 28.3.1 自带 `stdlib` 的 `json` 模块；`broker_admin_http` 已用 `json:encode/1` | `erl -eval code:which(json)`；`broker_admin_http.erl:254-255` |
| `broker_limits` 默认 `max_message_bytes => 1048576`、`max_json_depth => 16` | `apps/broker_security/src/broker_limits.erl:31-36` |
| `broker_security:auth_required/1` 只对 `credential_capable/0` 白名单（`mqtt`/`ws`/`nats`/`amqp`/`kafka`/`pulsar`）生效，`require_auth` 默认 `[]`（全匿名） | `apps/broker_security/src/broker_security.erl:137-168`；`config/sys.config.src:120` |
| `broker_queue:append/3` 是唯一的写入闸口，写前调 `broker_limits:check_message/1` | `apps/broker_queue/src/broker_queue.erl:121-136` |
| `broker_ws` 是纯库 app 的范本：无 supervisor、无 `registered`、`start/2` 只返回 `{ok, self()}` | `apps/broker_ws/src/broker_ws.app.src`；`broker_ws_app.erl` |
| 依赖仅 `ranch 2.3.0` + 内置 `ra 3.2.0`；无 cowboy | `rebar.config:19-24` |
| 端口占用：1883（MQTT）/1884（MQTT-SN）/4223（NATS）/5673（AMQP）/6650（Pulsar）/8080（WS）/8083（MQTT-WS）/8090（admin）/9093（Kafka）；8081/8082 未被占用；14567 只在注释里作 QUIC 示例（`listeners => []`） | `config/sys.config.src` |
| `quic` profile 的 `relx` **复制了整份** app 清单（profile 覆盖顶层） | `rebar.config:112-148`；ADR-024 §D5 |

### 1.3 命名口径（A2A）

`a2a.proto` 用 snake_case 字段名，**JSON 序列化必须 camelCase**（spec §5.5），枚举值保持 proto 的 `SCREAMING_SNAKE_CASE`。本报告凡写「proto 字段」指 `.proto` 名，写「JSON 字段」指线上名；ADR 与实现以 JSON 名为准，避免把 `protocol_binding` 直接写上线。

---

## 2. OQ-1：A2A 1.0.0 协议事实复核

### 2.1 数据模型（`a2a.proto` 逐字）

**`TaskState`（9 个，JSON 字符串同常量名）**

| 常量 | 分类 |
|---|---|
| `TASK_STATE_UNSPECIFIED` | 未知 |
| `TASK_STATE_SUBMITTED` | 进行态 |
| `TASK_STATE_WORKING` | 进行态 |
| `TASK_STATE_COMPLETED` | **终态** |
| `TASK_STATE_FAILED` | **终态** |
| `TASK_STATE_CANCELED` | **终态** |
| `TASK_STATE_INPUT_REQUIRED` | **中断态** |
| `TASK_STATE_REJECTED` | **终态** |
| `TASK_STATE_AUTH_REQUIRED` | **中断态** |

**`Task`**：`id`(REQUIRED)、`context_id`→`contextId`、`status`(REQUIRED，含 `state`+`message`+`timestamp`；`timestamp` 是 ISO 8601 字符串)、`artifacts[]`、`history[]`、`metadata`。
**`Message`**：`message_id`→`messageId`(REQUIRED)、`context_id`→`contextId`、`task_id`→`taskId`、`role`(REQUIRED，`ROLE_USER`/`ROLE_AGENT`)、`parts[]`(REQUIRED)、`metadata`、`extensions[]`、`reference_task_ids`→`referenceTaskIds`。
**`Part`**：`oneof content { text | raw | url | data }` **恰好一个**；`metadata`(5)、`filename`(6)、`media_type`(7)。JSON 名 `mediaType`；`raw` 在 JSON 中是 base64 字符串。
**`Artifact`**：`artifact_id`→`artifactId`(REQUIRED)、`name`、`description`、`parts[]`(REQUIRED，至少一个)、`metadata`、`extensions[]`。
**`TaskStatusUpdateEvent`**：`task_id`→`taskId`、`context_id`→`contextId`、`status`、`metadata`（`task_id`/`context_id` 均 REQUIRED）。
**`TaskArtifactUpdateEvent`**：`task_id`→`taskId`、`context_id`→`contextId`、`artifact`、`append`、`last_chunk`→`lastChunk`、`metadata`。
**`TaskPushNotificationConfig`**：`tenant`、`id`、`task_id`→`taskId`、`url`(REQUIRED)、`token`、`authentication`(`scheme`+`credentials`)。

**`AgentCard`（proto 字段 / JSON 字段）**

| 必需 | 可选 |
|---|---|
| `name` / `description` / `supported_interfaces`→`supportedInterfaces` / `version` / `capabilities` / `default_input_modes`→`defaultInputModes` / `default_output_modes`→`defaultOutputModes` / `skills` | `provider`(`url`+`organization`) / `documentation_url`→`documentationUrl` / `security_schemes`→`securitySchemes`(map) / `security_requirements`→`securityRequirements` / `signatures` / `icon_url`→`iconUrl` |

`AgentInterface`：`url`(REQUIRED)、`protocol_binding`→`protocolBinding`(REQUIRED，`JSONRPC`/`GRPC`/`HTTP+JSON`，开放字符串)、`tenant`、`protocol_version`→`protocolVersion`(REQUIRED，如 `"1.0"`)。**首项为首选接口**。
`AgentCapabilities`：`streaming`、`push_notifications`→`pushNotifications`、`extended_agent_card`→`extendedAgentCard`（三个都是 `optional bool`）+ `extensions[]`（`AgentExtension{uri,description,required,params}`，不是 bool）。
`AgentSkill`：`id`/`name`/`description`/`tags`(REQUIRED)、`examples`、`input_modes`、`output_modes`、`security_requirements`。

### 2.2 绑定、方法与版本（spec §5.3/§9）

**11 个 JSON-RPC 方法（v1.0 全部 PascalCase）**

| 方法 | 输入 → 输出 |
|---|---|
| `SendMessage` | `SendMessageRequest` → `SendMessageResponse`（oneof `task` \| `message`） |
| `SendStreamingMessage` | 同上 → SSE 流，每事件 `StreamResponse`（oneof `task`/`message`/`status_update`/`artifact_update`） |
| `GetTask` | `GetTaskRequest{tenant,id,history_length}` → `Task` |
| `ListTasks` | `ListTasksRequest{tenant,context_id,status,page_size,page_token,history_length,status_timestamp_after,include_artifacts}` → `ListTasksResponse{tasks,next_page_token,page_size,total_size}` |
| `CancelTask` | `CancelTaskRequest{tenant,id,metadata}` → `Task` |
| `SubscribeToTask` | `SubscribeToTaskRequest{tenant,id}` → SSE 流（`StreamResponse`） |
| `CreateTaskPushNotificationConfig` / `Get...` / `List...` / `Delete...` | push 配置 CRUD（`TaskPushNotificationConfig` / `ListTaskPushNotificationConfigsResponse` / `google.protobuf.Empty`） |
| `GetExtendedAgentCard` | `GetExtendedAgentCardRequest{tenant}` → `AgentCard` |

`SendMessageConfiguration`（proto 字段 / JSON）：`accepted_output_modes`→`acceptedOutputModes`、`task_push_notification_config`→`taskPushNotificationConfig`、`history_length`→`historyLength`、`return_immediately`→`returnImmediately`（`true` 立即返回并异步继续；`false` 默认等到终态/中断态）。注意 proto 中是 `optional int32 history_length`，「未设置」与「0」语义不同（0 = 不要历史）。

**版本协商**：请求头（或请求参数）`A2A-Version: Major.Minor`（如 `1.0`）；缺失/空按 `0.3` 处理；不支持回 `VersionNotSupportedError`（`-32009`）。Client **MUST** 逐请求发送。

**SSE**：`200 OK` + `Content-Type: text/event-stream`；每个 `data:` 是一个完整 JSON-RPC Response；流在终态/中断态关闭；断线后用 `SubscribeToTask` 重订阅。

### 2.3 错误码（spec §5.4/§9.5）

标准：`-32700` JSONParse、`-32600` InvalidRequest、`-32601` MethodNotFound、`-32602` InvalidParams、`-32603` Internal。

A2A 专用：

| 错误 | code | 错误 | code |
|---|---|---|---|
| `TaskNotFoundError` | `-32001` | `ContentTypeNotSupportedError` | `-32005` |
| `TaskNotCancelableError` | `-32002` | `InvalidAgentResponseError` | `-32006` |
| `PushNotificationNotSupportedError` | `-32003` | `ExtendedAgentCardNotConfiguredError` | `-32007` |
| `UnsupportedOperationError` | `-32004` | `ExtensionSupportRequiredError` | `-32008` |
| | | `VersionNotSupportedError` | `-32009` |

`error.data` 是数组，每项必须有 `@type`；推荐 `google.rpc.ErrorInfo{reason,domain,metadata}`。示例：`{"code":-32001,"data":[{"@type":".../google.rpc.ErrorInfo","reason":"TASK_NOT_FOUND",...}]}`。

### 2.4 与规划稿 §2.1 的偏差

| 规划稿写法 | v1.0.0 实际 | 影响 |
|---|---|---|
| Agent Card 平铺 `url`/`security`/`securitySchemes`/`skills` | `supportedInterfaces[]`（`url`+`protocolBinding`+`tenant`+`protocolVersion`，首项首选）；`securitySchemes`/`securityRequirements`；新增 `defaultInputModes`/`defaultOutputModes`/`provider`/`documentationUrl`/`iconUrl`/`signatures` | R-D1/R-D2/R-D5 字段集 |
| 状态写 `submitted`/`working`… | `TASK_STATE_*` 全名（JSON 也是全名） | R-S2 |
| `Part.mediaType` | proto `media_type`，JSON `mediaType` | R-M2（规划稿的 JSON 名是对的） |
| 错误集只列名称 | 必须带 `-32001`~`-32009` 映射，proto 内**没有**错误码 | R-M4 |
| 操作表无 `ListTasks` | v1.0 新增 `ListTasks`/`ListTasksResponse` | §2.1 操作表、R-S/R-A |
| 未提 `A2A-Version` | 请求头/参数，缺失按 `0.3`，不支持 `-32009` | R-T6 |
| `capabilities.streaming` | 正确（JSON `streaming`），但漏 `extendedAgentCard` | R-D5 |
| `securitySchemes` 大小写 | JSON `securitySchemes` / `securityRequirements`（camelCase） | R-X2 |

### 2.5 对规划稿 §3 的修正结论

1. **R-S2**：枚举用 `TASK_STATE_*`；终态集合 `COMPLETED/FAILED/CANCELED/REJECTED`，中断态 `INPUT_REQUIRED/AUTH_REQUIRED`。
2. **R-D1**：字段集以 `AgentCard` proto 为准；`supportedInterfaces[]` 是 REQUIRED，`url` 不再是顶层字段。
3. **R-D2**：`GET /.well-known/agent-card.json` 不变。
4. **R-D5**：`GetExtendedAgentCard` 带 `tenant`；「扩展 card」由 `capabilities.extendedAgentCard` 声明。
5. **R-M2**：JSON `mediaType`、proto `media_type`；`Part` 恰好一个内容字段。
6. **R-M4**：错误映射取 §2.3，不取 proto。
7. **R-T6**：版本协商必须覆盖 `A2A-Version` 头/参数与 `-32009`。
8. **补 `ListTasks`**：列入 R-S（投影分页）与 R-A（轮询面），并作为 Agent Card `capabilities` 之外的独立能力（v1.0 基础方法）。
9. **R-A4**：配置对象名是 `TaskPushNotificationConfig`（不是 `PushNotification`），字段 `url`/`token`/`authentication`/`id`/`task_id`/`tenant`。

---

## 3. OQ-2：Task 状态落到哪里

### 3.1 候选与判定

| 候选 | 结论 | 理由 |
|---|---|---|
| `broker_meta` 新表（Mnesia） | **否决** | ADR-002：Mnesia 只做控制面；Task 状态是随消息量增长的数据面工作负载，高频写会撞 ADR-002 的吞吐/延迟边界；且表清单是冻结项 |
| 订阅游标（SubRef cursor） | **否决** | ADR-013：cursor 是「消息投递位置」，单调推进、无状态语义；Task 需要状态机、终态不可变、可查询历史。把状态机塞进 cursor 会让 `min(live cursor)` 语义与 retention 一起被任务生命周期污染 |
| adapter 私有第二套日志（DETS/自研 WAL） | **否决** | 违反 ADR-001 的「不写第二套消息内核」；且丢掉 `replicated_commit`（复制编排在 `broker_queue`） |
| **普通消息日志 + adapter 投影（事件溯源）** | **采纳** | 复用 WAL/fsync/`replicated_commit`、retention、DLQ/TTL、CloudEvents 通道；Task 状态机留在 adapter 内，内核不感知 A2A |

### 3.2 决定（详见 ADR-025）

- **D1 事件是消息**：每次状态变更追加一条 `#broker_message{}`，`destination = <<"$a2a/task/", TaskId/binary>>`，`tenant_id` 来自已认证身份，`payload_encoding = json`，`persistence = durable`，`payload` 是 A2A 事件 JSON（`Task`/`TaskStatusUpdateEvent`/`TaskArtifactUpdateEvent`/`Message` 之一 + 项目私有 `_meta` 事件号）。
- **D2 投影在 adapter**：Task 记录 = 按 offset 折叠事件的结果；放在 adapter 的 ETS + 分片 registry（按 `phash2(TaskId, N)`），**不进 Mnesia**。
- **D3 每任务串行化**：所有状态变更走拥有该任务的分片进程；终态后拒绝任何 append（`UnsupportedOperationError`/`TaskNotCancelableError`）。重放时重新校验，违规事件只记日志 + 指标，不静默修正。
- **D4 可靠性档**：创建与终态事件默认 L2 `fsync`；集群装了 replicator 时可配 L3 `replicated_commit`；L1 `os_buffer` 只能显式开启并在运维文档写明「崩溃可丢任务」。HTTP 响应在 append 返回**之后**才发（ADR-007 不得提前 ACK）。
- **D5 恢复**：adapter 维护 `data/a2a/replay.idx`（每分区最后扫描 offset + 投影快照，tmp+rename 原子写）；boot 从 checkpoint 增量重放，无 checkpoint 则从 retention 起点重放。
- **D6 retention 是运维契约**：**不引入 per-task cursor**。Task 事件与普通消息同受 `max_age`/`max_bytes` 约束；上线要求 `retention` 大于最长任务时长，并由 `a2a_task_beyond_retention` 指标 + 告警兜底。重放发现某非终态任务的创建事件已被 GC：补一条 `TASK_STATE_FAILED`（`metadata.reason = history_lost`）并计数，**不静默返回半截状态**。
- **D7 `GetTask`/`ListTasks` 读投影**：`ListTasks` 的 `page_size` 未指定时默认 50、上限 100（proto 原文：`If unspecified, at most 50 tasks`、`maximum value is 100`），`page_token` 不透明（base64 of `{sort_key}`）；`history_length` 只能截短，不能补出已 GC 的历史。
- **D8 大小**：见 OQ-9。

### 3.3 为什么不是「每条任务一个 cursor」

一个任务一个 SubRef 的 cursor 确实能精确钉住该任务的事件，但代价是：每个非终态任务都要在订阅层建实体、写 `.subs`/`.cursors` 文件、在 Router 索引里占一项，并在任务结束时删除；任务数 = 订阅数会把「会话文件」变成高频写路径，且与 ADR-013 的 cursor 语义继续混淆。全局 guard 消费者又必须读完整个分区流才能越过非 A2A 消息（`ack_skip`），成本与收益不匹配。故明确否决，并把「任务比 retention 活得久」变成显式的运维契约。

---

## 4. OQ-5：SSE 长连接与既有背压/慢消费者守卫

### 4.1 决定

1. **SSE 是瞬态读者，不是订阅**：`SendStreamingMessage`/`SubscribeToTask` 在连接进程里从任务日志的某个 offset 开始 `broker_queue:read_from/3`，**不创建 SubRef、不写 cursor 文件、不参与 `min(live cursor)`**。因此一个卡住的 SSE 客户端**不会 pin retention**，也不会拖慢任务执行。
2. **执行与流式解耦**：任务状态变更由任务分片进程 append 到日志；SSE 只是读日志。慢客户端最多落后，不会反压到 agent/发布路径。
3. **有界与超时**：每个 SSE 连接有 `sse_max_pending_bytes`（默认 1 MiB）与 `sse_send_timeout_ms`（默认 30 s）；`gen_tcp:send`/`ssl:send` 超时即关连接，不把数据堆进 mailbox。
4. **落后恢复**：读取位置低于 `committed`（事件已被 retention 删除）时，不发空洞事件，而是发一条**当前 Task 快照**（`StreamResponse.task`，`metadata._a2a.resync = true`）再从尾部继续；客户端按 `Task` 幂等覆盖本地状态。这样不需要发明新的 A2A 错误码。
5. **心跳**：每 `sse_heartbeat_ms`（默认 15 s）发一行 SSE 注释（`: ping`），防中间层 idle 超时；心跳不计入 JSON-RPC 事件。
6. **配额**：监听器级 `max_connections`（ranch）+ 每租户 `max_sse_connections`；超限回 `503`，不是排队。
7. **指标**：`a2a_sse_connections`、`a2a_sse_slow_consumer`（send 超时/落后 resync）、`a2a_sse_bytes_out`；不按 `taskId` 打标签（ADR-016 防高基数）。

### 4.2 与 ADR-014/016 的关系

ADR-014 的慢消费者守卫管的是**订阅 cursor 钉住磁盘**；SSE 不建 cursor，所以不进入那条路径。ADR-016 的「无外部依赖」与最小指标集不变。ADR-015 的重投递/DLQ 只用于**出站 webhook**（见 OQ-7），不用于 SSE。

---

## 5. OQ-3：Agent Card / catalog 落点

### 5.1 决定

- 在 `broker_meta` **新增一张表** `agent_card`：
  `{agent_card, agent_ref, tenant_id, version, status, card, extended_card, updated_at}`，
  主键 `agent_ref = {TenantId, AgentId}`（Mnesia 复合键合法；`tenant_id` 另存一列用于过滤）。
- `card` 是公开 Agent Card JSON（map/binary），`extended_card` 是认证后版本；`version` 单调递增（ETag 来源），`status ∈ active | disabled`。
- 读取：`broker_meta:get/2` + `all/1`；catalog 查询在 adapter 内按 `skill`/`tag`/`provider`/`capability` 过滤，`page_size` 未指定默认 50、上限 100（与 A2A `ListTasksRequest` 的 proto 默认一致）。
- 写入：管理 API（S9-B）与 boot 期配置种子；写前校验必需字段（`name`/`description`/`supportedInterfaces`/`version`/`capabilities`/`defaultInputModes`/`defaultOutputModes`/`skills`）。
- **不复活** `topic_meta`/`queue_meta`/`binding_meta`：它们的 `key/config` 泛型形状不适合承载带 schema 的 card，硬塞会让「无运行时读取方」变成「有一个读取方但语义错位」。
- 缓存：`ETag = <<"\"", Version, "\"">>` + 内容哈希；`If-None-Match` → `304`；`Cache-Control: max-age=60`（可配）。不在 S9-B 引入 ETS 缓存（Mnesia dirty read 足够，先测再优化）。
- `broker_meta` 的表清单是 ADR-002 的冻结项，本决定按 ADR 惯例在 ADR-026 §D3 标注 **amends ADR-002 表清单**（只新增，不修改既有 11 张表的形状）。

### 5.2 安全边界

Agent Card **不得**含静态密钥（R-D6）；`securitySchemes` 只声明 scheme 与外部 AS URL；`extended_card` 按调用方身份选择性披露（逐字段裁剪在 `broker_proto_a2a` 内做，不下沉到 `broker_meta`）。

---

## 6. OQ-4：HTTP/SSE 服务端选型

### 6.1 决定

**新写 `broker_proto_agent_rpc`（纯库 app，无进程、无配置、无 listener），HTTP/1.1 前端用 `ranch 2.3.0` + `{packet, http_bin}` + OTP 28 `json`。**

| 候选 | 判定 | 理由 |
|---|---|---|
| cowboy | **否决** | 新增运行时依赖，违背「仅 ranch + ra」的依赖面；A2A/MCP 只需 6~10 个端点，HTTP/2、路由 DSL、middleware 都用不上 |
| 复用 `broker_proto_ws` | **否决** | 它是 RFC 6455 适配器，不是通用 HTTP 服务；把 HTTP 塞进去会让 WS 的 pinned wire behavior 变脆 |
| 复用/扩展 `broker_admin_listener` | **否决** | 管理面与数据面必须分开（`broker_admin` 的应用依赖刻意很窄）；且它是「一请求一连接 + `Connection: close`」，不支持 SSE 长连接 |
| **ranch + `http_bin` 自建** | **采纳** | ranch 已是依赖，`broker_security_tls:transport/0` 直接给出 `ranch_tcp`/`ranch_ssl`；`broker_admin_listener` 证明 `{packet, http_bin}` 足以解析请求；每连接一个 ranch 协议进程天然适配 SSE |

### 6.2 形态

```
broker_proto_a2a_sup (one_for_all)
├── a2a_task_registry (N shards)        % 投影与状态机串行化
├── a2a_push_worker                      % 出站 webhook（S9-C）
└── a2a_listener × M                     % ranch:start_listener
      └── a2a_conn (ranch_protocol)      % 每连接一个；HTTP 解析 + SSE 写
```

- 请求解析沿用 `broker_admin_listener` 的 `http_bin` 方式（方法/URI/头/`content-length`；补 `Expect: 100-continue` 与 `Transfer-Encoding: chunked` 的请求体上限）。
- 响应：普通请求 `Content-Length` + `Connection: keep-alive|close`；SSE `Transfer-Encoding: chunked` + `Cache-Control: no-cache` + `X-Accel-Buffering: no`。
- **只支持 HTTP/1.1**（不做 HTTP/2、不做 WebSocket）；A2A JSON-RPC binding 不要求 HTTP/2；客户端（curl/官方 SDK）默认 HTTP/1.1。限制写进能力矩阵。
- TLS：复用 `broker_security_tls:transport/0`；sse/普通请求同一 listener；`tls.enabled` 对 S9 是真正的开关（与 MQTT-SN 不同）。
- 中间件顺序：请求行/头 → 体积与深度上限 → `A2A-Version` 协商 → 认证（`authenticate_conn/1`）→ 授权（`authorize/2`）→ 方法与参数 → handler。

### 6.3 为什么不让 `broker_proto_agent_rpc` 拥有 listener

如果公共底座拥有 listener，它就必须反向依赖各 adapter 的 handler（或引入注册表），形成「底座 → adapter」的耦合，违反 ADR-001 的横向解耦。把底座做成纯库（参照 `broker_ws`：零进程、零配置），由每个 adapter 自己起 listener，依赖方向就只剩 adapter → 底座 → kernel。

---

## 7. OQ-6：ACP 侧别与传输

### 7.1 决定

- **只做 stdio**；远程 HTTP/WS 按规划稿 §1.3 非目标不实现（v2 WIP）。
- **Broker 扮演 ACP Client 侧**：
  - ACP Agent 通常是 Client 的子进程；stdio 语义要求「谁 spawn 谁就是 Client」。Broker 作为长驻进程，适合做 host。
  - 若 Broker 扮 Agent，则必须实现 `session/prompt` 的 agent 语义（推理/工具执行），直接撞 §1.3 非目标。
- **Callbacks 边界**：
  - `session/request_permission` 是 Client baseline，必须实现；策略 = **默认拒绝 + 可配 allowlist（按 tool kind/标题正则）+ 超时拒绝**，并把每次请求落成可审计事件（走运维/日志通道，**不**伪装成 `session/update`——那是 Agent→Client 的通知）。绝不自动放行。
  - `fs/read_text_file`/`fs/write_text_file`/`terminal/*`/`elicitation/create` **不广告**（capability 缺省）；Agent 若在 `initialize` 后强依赖它们，Broker 回 ACP 错误并结束会话——在能力矩阵逐行留痕。
- **Agent 进程**：只能由运维配置声明（命令 + 参数 + 环境变量白名单），**不得**由客户端请求指定；默认关闭；工作目录、uid/gid、资源限制由部署方负责（Broker 不做沙箱）。
- **版本**：只接受 `protocolVersion 1`；不匹配回 ACP 错误；v2 的 `auth/login`、`session/resume` 必需化、`fs/*`/`terminal/*` 移除等不实现，版本门控写进能力矩阵。
- **stdio 约束**：newline-delimited JSON（一行一条 JSON-RPC，UTF-8）；stdout 只允许协议帧，**所有 logger handler 必须写 stderr**，启动时校验，不满足即拒绝启用 stdio 前端。

---

## 8. OQ-7：出站 webhook 的 egress 策略

### 8.1 决定

1. **默认关闭**：`push.enabled => false`，`egress_allowlist => []`；空 allowlist 等于拒绝一切 push 配置创建（`PushNotificationNotSupportedError` / `-32003`）。
2. **两层 allowlist**：全局 host 后缀 allowlist（`api.example.com` 或 `.example.com`）+ 每租户 allowlist；两者求交。
3. **解析后校验并 pin IP**：先解析域名，拒绝 loopback/私有/链路本地/组播/保留地址（除非该 host 在 `allow_private => true` 的显式条目里），然后**直接连到已校验的 IP** 并带 `Host` 头——避免 DNS rebinding/TOCTOU。
4. **禁重定向**（3xx 一律失败）；连接/发送/读取超时；响应体上限（默认 64 KiB，只需状态码）；只允许 `https`（`http` 需显式开启，供本地测试）。
5. **不使用 `httpc`**：它不能在建立连接时 pin 已校验 IP，也没有 per-request egress 钩子；用 `gen_tcp`/`ssl` + 手写最小 HTTP/1.1 客户端（约百余行）反而更可控。
6. **自证与防重放**（S9-C）：`TaskPushNotificationConfig.authentication` 的 Bearer/API key 由配置提供，**不接受客户端明文密钥**；出站头带 `X-A2A-Timestamp` + `X-A2A-Nonce`（项目私有扩展头，不属于 A2A 规范，需在能力矩阵注明），HMAC 可选；密钥轮换靠重启/配置重载。
7. **失败语义**：按 ADR-015 的参数（`max_attempts`、指数退避、TTL）重试；耗尽后写 `$a2a/push/dlq/<tenant>`（普通 destination，可订阅、可审计），不阻塞任务终态。
8. **部署边界**：Broker 只保证自身 egress；集群网络策略、NAT、代理由部署方负责，写进 `docs/operations.md`（S9-C/F）。

---

## 9. OQ-8：OAuth 授权服务器 / JWKS 是否纳入 Broker

**决定：不纳入。**

- Broker **不是** OAuth Authorization Server，不签发 token，不提供 `/.well-known/oauth-authorization-server`，不托管 JWKS。
- 只接受**不透明 bearer token**：`Authorization: Bearer <token>` → `broker_security:authenticate_conn({token, Token})` → `broker_user` 表命中 → `{principal, tenant_id}`。`X-API-Key` 同样映射到 `{token, ...}`。
- **不做 JWT 验签**（不取 JWKS、不做 introspection）。JWT 形状的 token 若不等于表中 token，一律 `401` + `WWW-Authenticate: Bearer error="invalid_token"`；能力矩阵写明「JWT/OIDC 需前置网关」。
- Agent Card 的 `securitySchemes` 可以**声明**外部 `oauth2`/`openIdConnect`（含外部 authorization/token/JWKS URL），但那是静态元数据，不代表 Broker 实现该流程。
- 401/403：认证失败 `401`（带 `WWW-Authenticate`）；认证通过但 ACL 拒绝 `403`；租户不匹配按 `403`（不泄露资源是否存在）。
- `require_auth`：每个 S9 adapter **自己的** env，**默认 `true`**（新面没有历史兼容包袱，失败关闭）；匿名模式需显式 `false`，且只在回环监听时允许（启动时校验，否则拒绝启动）。
  - **不复用** `broker_security.auth_required/1` 作为开关：它的 `credential_capable/0` 白名单只含六个既有协议，`require_auth` 默认 `[]`；把 S9 塞进去会改动既有六协议的启动/握手行为。S9 只复用 `authenticate_conn/1`、`authorize/2`、`default_tenant/0` 三个纯函数（[ADR-026 §D4](../adr/026-agent-adapter-front-end.md)）。

---

## 10. OQ-9：大 Artifact 与 Broker 消息大小上限

### 10.1 决定

| 项 | 口径 |
|---|---|
| HTTP 请求体 | ≤ `min(broker_limits.max_message_bytes(), adapter.max_body_bytes)`；默认 1 MiB；超限 `413`（HTTP 层）/ `-32602`（JSON-RPC 层，带 `fieldViolations`） |
| 单 `Part.raw` | 受同一请求体上限约束；base64 膨胀 4/3，故 1 MiB 请求体最多约 **768 KiB** 原始字节 |
| `Part.data` | JSON 值，受 `max_json_depth`（16）与请求体上限约束 |
| 多 Part 消息 | 整个请求体共享一个上限；不按 Part 累加放宽 |
| `Artifact` 分块 | `append=true` 的每块是独立消息，各自 ≤ 上限；总大小 = 块数 × 上限，不设硬上限，由 retention 与任务生命周期约束 |
| `Part.url` | 只做 URL 语法/协议校验（`https`），**Broker 不取回**；内容由客户端自行获取。避免把 Broker 变成 SSRF 跳板 |
| 任务历史 | `history_length` 只能截短；服务端 `MAY` 返回更少（spec 允许） |

### 10.2 依据

`broker_limits.max_message_bytes` 默认 1 048 576，且 `broker_queue:append/3` 在写入前调用 `broker_limits`；NATS `max_payload` 与 WS `max_publish_bytes` 同为 1 MiB，口径一致。A2A 的 `raw` 是 base64 字符串，必须在**解码前**按膨胀后的长度判限，否则 1 MiB 的 body 会解出 786 KiB 以外的额外内存。

---

## 11. OQ-10：三协议与既有端口 / listener / 认证路径的装配

### 11.1 应用拆分

| App | 形态 | 进程 | 依赖 |
|---|---|---|---|
| `broker_proto_agent_rpc` | **纯库**（参照 `broker_ws`：无 supervisor、无 listener） | 无 | kernel/stdlib/sasl/ssl/ranch/broker_core/broker_security；提供 JSON-RPC 信封、错误模型、版本协商、HTTP/1.1 解析、SSE writer、认证 helper |
| `broker_proto_a2a` | adapter | task registry shards + push worker + listener | `broker_proto_agent_rpc` + `broker_meta`（Agent Card）+ 内核门面 |
| `broker_proto_mcp` | adapter | listener（Streamable HTTP）+ 可选 stdio child | `broker_proto_agent_rpc` + 内核门面 |
| `broker_proto_acp` | adapter | 仅 stdio child（默认关闭） | `broker_proto_agent_rpc`（只用 JSON-RPC 信封） |

三个 adapter **彼此不引用**；公共逻辑只下沉到 `broker_proto_agent_rpc`，不下沉到 `broker_core`（内核不为某个协议族扩面）。

### 11.2 `applications` 与启动顺序

```erlang
{applications, [kernel, stdlib, sasl, ranch,
                broker_core, broker_metrics, broker_storage,
                broker_router, broker_queue, broker_subscription,
                broker_event, broker_security,
                broker_proto_agent_rpc]}.
```

- `broker_proto_a2a` 另加 `broker_meta`（Agent Card 表）。
- OTP 拓扑序保证在 `broker_subscription:init/1`（boot step 2/3）之后；listener 是各自 supervisor 的**最后一项**，与 `broker_proto_mqtt` 的 boot step 4 同构。
- 失败语义：listener 绑定失败 → 该 app 起不来（与其他协议一致），不影响已起的 app；`broker_meta` 失败 → A2A app 起不来。

### 11.3 端口与门控

| 面 | 端口 | 默认 |
|---|---|---|
| A2A（JSON-RPC + SSE） | `8081` | app env `listeners => []`；release config 绑 `{127,0,0,1}, 8081` |
| MCP（Streamable HTTP） | `8082` | 同上 |
| ACP（stdio） | 无端口 | `stdio.enabled => false` |
| 既有 | 1883/1884/4223/5673/6650/8080/8083/8090/9093（14567 为 QUIC 示例端口，默认 `listeners => []` 未监听） | 不动 |

`listeners => []` 是「CT 与库节点不偷偷开端口」的既有惯例；release 用回环地址起步，对外暴露必须是显式配置改动。

### 11.4 relx 与零回归

- 默认 `relx` 清单与 `quic` profile 的 **两份** app 清单都要追加四个新 app（profile 的 `relx` 会**替换**顶层清单，ADR-024 §D5 已踩过）。
- 既有 823/823 不受影响的前提：新 app 默认零 listener、不被既有套件启动；`rebar3 ct` 只多编译代码。
- 认证接线：`Authorization: Bearer` / `X-API-Key` → `authenticate_conn({token, _})`；`authorize/2` 的 `action` 用 `publish`（SendMessage/创建任务）与 `subscribe`（GetTask/List/Subscribe）；租户判定沿用 `broker_security`，不在 A2A 内重写。`require_auth` 用 adapter 自己的 env（默认 `true`），**不改** `config/sys.config.src` 的 `{require_auth, []}` 与 `broker_security:credential_capable/0` 白名单——后者只服务六个既有协议。

---

## 12. 对规划稿的「落地后修正」批注清单

按项目惯例，规划稿原文不改，偏差处加批注；批注已落在 `docs/design/s9-agent-protocols.md` 的 §1.2、§2.1 与 §4.4。逐项：

| 位置 | 修正 |
|---|---|
| §1.2 Task 行 | 「订阅游标」不成立：Task 状态是事件溯源投影，不使用 SubRef cursor（ADR-025 §D6） |
| §2.1 Agent Card 字段 | 改 `supportedInterfaces[]`；JSON camelCase；补 `defaultInputModes`/`defaultOutputModes`/`provider`/`documentationUrl`/`iconUrl`/`signatures` |
| §2.1 Task 状态 | `TASK_STATE_*` 全名 |
| §2.1 错误集 | 补 `-32001`~`-32009`；proto 不含错误码 |
| §2.1 操作表 | 补 `ListTasks` |
| §2.1 Part | proto `media_type` / JSON `mediaType` |
| §2.1 保护与缓存 | 补 `A2A-Version` 协商 |
| §3.1 R-T6 / §3.2 R-D1/R-D5 / §3.4 R-M2/R-M4 / §3.5 R-A4 | 按 §2.5 的 9 条结论修订 |

---

## 13. 未决与后续复核项（不阻塞 S9-A）

这些**不是**收敛遗留的架构不确定性，而是必须在对应子阶段用原文再核一遍的细节；现在写死会变成猜测：

1. **MCP `initialize` vs `server/discover`**：2026-07-28 的发现以 `server/discover` + 每请求 `_meta` 为主；`initialize` 是否仍作为握手方法保留，S9-D 开工前逐字核对 `specification/2026-07-28` 的 basic/index 与 lifecycle 页。
2. **MCP `subscriptions/listen` 的 ack 与错误形状**：规划稿 §2.2 未提；S9-D 按原文补。
3. **A2A `A2A-Version` 的请求参数形式**：spec 允许 header 或 request parameter；S9-A 只实现 header，参数形式在能力矩阵标注「未实现」。
4. **`ListTasks` 的 `status_timestamp_after` 语义**：proto 说「>= 该时间」；实现按 `TaskStatus.timestamp` 过滤，S9-B 用真实 SDK 验证。
5. **Push 的 JWS/JWKS 验真**：本阶段只做 HMAC/Bearer + 时间戳/nonce；JWS 与 JWKS 轮换列 S9-C 可选项，默认不做。
6. **HTTP/2 与 `Expect: 100-continue`**：只做 HTTP/1.1；100-continue 在 S9-A 实现并测。
7. **ACP v2**：只做版本门控，不实现 v2 草案特性；远程传输继续跟踪 Transports WG。
8. **JWT/OIDC**：不纳入 Broker（OQ-8）；若未来要做，必须新开 ADR（涉及 JWKS 拉取、缓存、轮换、时钟偏移）。

---

## 14. 下一步（S9-A 入口条件）

S9-A 可以开工的最低条件已满足：

1. 协议事实已按 `a2a.proto` 复核（§2），规划稿已加批注（§12）。
2. 两个核心 ADR 已冻结（ADR-025/026）。
3. 应用拆分、端口、boot 顺序、relx 双清单、认证接线已定（§11）。
4. 明确「不做什么」：JWT/JWKS、HTTP/2、ACP 远程、MCP sampling、Agent Card JWS PKI、A2A gRPC/REST、per-task retention pin。

S9-A 的第一批可验证产出（按 Gate 文本）：单方法 echo + `A2A-Version` 协商 + `401`/`403` + 既有 823/823 零回归；随后 S9-B 才引入 Task 状态机与 Agent Card 表。
