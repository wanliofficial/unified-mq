# ADR-025：A2A Task 状态机到统一消息内核的映射

- **状态**：已采纳（S9-0 冻结，2026-11-11）
- **日期**：2026-11-11
- **关联**：A2A `1.0.0` `specification/a2a.proto`（`Task`/`TaskState`/`StreamResponse`）与 `docs/specification.md` §5.3/§5.4/§9；ADR-001（协议适配与内核分离）、ADR-002（Mnesia 只做控制面）、ADR-007（显式可靠性）、ADR-009（`broker_message`）、ADR-010（Router）、ADR-013（cursor 归属）、ADR-014（retention）、ADR-015（重投递/DLQ/TTL）、ADR-016（可观测最小集）、ADR-026（前端与控制面）
- **关联文档**：[`docs/design/s9-0-convergence-report.md`](../design/s9-0-convergence-report.md)（OQ-2/OQ-5/OQ-9）、[`docs/design/s9-agent-protocols.md`](../design/s9-agent-protocols.md) §3.3/§3.4/§4.4

## 背景（Context）

A2A 的 `Task` 是**有状态工作单元**：有唯一 `id`、`contextId` 分组、`TaskStatus` 状态机、`artifacts` 与 `history`，且**终态不可变**（`COMPLETED`/`FAILED`/`CANCELED`/`REJECTED` 之后不可重启、不可再收消息）。它同时受三个既有约束挤压：

1. **ADR-002**：Mnesia 只承载低频控制面，高频 cursor/ACK/消息正文不进 Mnesia。
2. **ADR-013**：订阅游标（SubRef cursor）是「单调推进的**消息位置**」，不是任务状态机；cursor 属于 SubRef，不属于某个业务实体。
3. **S9 的非目标**：不写第二套消息内核，不绕过 `broker_queue` 另建日志（否则丢掉 `replicated_commit` 的复制编排）。

因此 OQ-2 要回答的不是「用哪张表」，而是「Task 的**事件**与**状态**分别归谁」。本 ADR 冻结：**事件归统一消息日志，状态归 adapter 投影**，并明确可靠性、恢复与 retention 的口径。

## 决策（Decision）

### D1. Task 事件就是普通 `broker_message`

每次状态变更追加**一条不可变消息**：

```erlang
#broker_message{
  message_id   = a2a_event_id(TaskId, Seq),       % 稳定可重放
  tenant_id    = TenantId,                        % 来自已认证身份，不读 payload
  destination  = <<"$a2a/task/", TaskId/binary>>,
  payload      = EventJson,                       % Task / TaskStatusUpdateEvent / TaskArtifactUpdateEvent / Message
  payload_encoding = json,
  headers      = #{},                             % 可放 ce-* 与 trace 头，不放身份
  persistence  = durable,
  reliability  = Reliability,                     % D4
  created_at   = NowMs
  %% partition_id 由 adapter 用 broker_router_hash:route/3 求得（ADR-010：路由是唯一
  %% 分区 oracle），再 broker_queue:append(Partition, Msg, Mode)
}
```

- `destination` 固定前缀 `$a2a/task/`，经 `broker_router` 正常算分区并扇出；同租户的普通订阅者可以观察这些事件（与 `$dlq/...` 同一先例），**跨租户由 Router 的租户过滤 + `broker_security` 双重隔离**。
- 事件的 `payload` 是**A2A 线上 JSON**（`StreamResponse` 的 oneof 之一），另加项目私有 `_meta: {"unified-mq": {"seq": N, "kind": "..."}}`；不发明平行数据模型。
- Broker **不解释** `Task` 语义；它只保证消息被追加、可读、可复制、可保留。状态机在 adapter 内（D2/D3）。

### D2. Task 是事件流的投影，投影在 adapter 内存

- `Task` 记录 = 按分区 offset 顺序折叠该 destination 事件的结果：`{TaskId, TenantId, ContextId, Status, Artifacts, History, CreatedOffset, LastOffset, LastStatusAt}`。
- 投影放在 `broker_proto_a2a` 的 ETS（每个任务分片一张表）+ 分片 `gen_server` 里；**不进 Mnesia**（ADR-002）。
- 分片数默认 `erlang:system_info(schedulers_online)`，任务按 `phash2(TaskId, N)` 归属；同一任务的所有状态变更串行经过它的分片。
- `GetTask`/`ListTasks` 只读投影；`ListTasks` 的分页在内存索引上做（`page_size` 未指定默认 50、上限 100，与 proto 一致；`page_token` 不透明）。

### D3. 终态不可变由「串行化 + 追加前校验 + 重放复核」三层保证

1. 任何会改变状态的请求先进入任务分片；分片校验当前状态与合法迁移。
2. 终态之后拒绝 append：`SendMessage` → `UnsupportedOperationError`（`-32004`）；`CancelTask` → `TaskNotCancelableError`（`-32002`）。
3. boot 重放时重新跑迁移校验；遇到「终态后还有事件」的脏数据，**只记 `a2a_state_violation` 指标 + 日志**，不静默丢弃、不自动改写历史。

合法迁移（与 proto 一致）：

```
UNSPECIFIED → SUBMITTED
SUBMITTED   → WORKING | COMPLETED | FAILED | CANCELED | REJECTED | INPUT_REQUIRED | AUTH_REQUIRED
WORKING     → WORKING | COMPLETED | FAILED | CANCELED | REJECTED | INPUT_REQUIRED | AUTH_REQUIRED
INPUT_REQUIRED / AUTH_REQUIRED → WORKING | CANCELED | FAILED | REJECTED
COMPLETED / FAILED / CANCELED / REJECTED → （终态，无出边）
```

### D4. 可靠性档严格绑定 ADR-007

| 配置 | 行为 | 丢失边界 |
|---|---|---|
| `task_reliability = fsync`（**默认**） | `broker_queue:append(..., fsync)` 返回后才发 HTTP 响应 | 仅磁盘故障 |
| `task_reliability = replicated_commit`（集群装了 replicator 时） | 多数派提交后返回；未装 replicator 时**拒绝启动**（不静默降级） | 多数派同时故障 |
| `task_reliability = os_buffer`（显式开启） | 入缓冲即返回 | 崩溃/节点故障可丢任务；运维文档必须写明 |

- **创建事件与终态事件使用同一档**（不为不同事件设不同档，否则「任务已创建」与「任务已完成」的承诺不可比）。
- `return_immediately = false`（默认）时，HTTP 响应必须等到终态/中断态事件按所选档落盘；`return_immediately = true` 只保证**创建事件**落盘后返回，后续由 `GetTask`/SSE/推送观察。
- 不得提前 ACK：先 append，后响应（与 MQTT QoS 的 PUBACK 时序同一原则）。

### D5. 崩溃恢复 = checkpoint + 增量重放

- adapter 维护 `data/a2a/replay.idx`：每分区最后扫描 offset + 投影快照（`term_to_binary` + `tmp` 写 + `rename` 原子替换）。
- 写入时机：任务进入终态时、周期性（可配，默认 5 s）、正常停机。
- boot：读 checkpoint → 从各分区 offset 起 `broker_queue:read_from/3` 增量重放 → 只处理 `$a2a/task/` 前缀。无 checkpoint 时从 offset `0` 请求：`broker_storage_wal:read_from/3` 会把低于 `TruncateBefore` 的起点抬到 retention 边界（ADR-011 §11.6 的「逻辑边界」语义），因此拿到的第一条记录 offset 可能大于 0——这正是 D6 判定 `history_lost` 的信号。
- 重放不产生新事件、不发推送、不触发副作用；它只重建投影。
- checkpoint 损坏 → 记 `a2a_checkpoint_corrupt` 并退化为全量重放；**不阻止启动**（任务可重建），但必须在告警里可见。

### D6. retention 是运维契约，不引入 per-task cursor

- Task 事件与普通消息**同受** ADR-014 的 `max_age`/`max_bytes` 约束；**不**为每个任务创建 SubRef/cursor 去钉住日志。
- 理由：per-task cursor 会把「非终态任务数」变成订阅实体数（`.subs`/`.cursors` 文件 + Router 索引项），是对 ADR-013 cursor 语义的再次误用；全局 guard 消费者又必须读完整分区流（含非 A2A 消息）才能越过 `ack_skip`，成本与收益不匹配。
- 上线契约：`retention.max_age`/`max_bytes` 必须大于「最长任务时长 × 事件速率」；`docs/operations.md` 给出估算公式，`config/alerts.yml` 增加 `a2a_task_beyond_retention`。
- **响亮失败**：重放发现某非终态任务的创建事件已被 GC（或第一个可见事件不是创建事件），补一条服务端生成的 `TASK_STATE_FAILED` 终态事件，`metadata.reason = history_lost`，并计 `a2a_task_history_lost`。不返回半截 `Task`，不让客户端无限轮询。
- `history_length` 只能截短（spec 明确服务端 `MAY` 返回更少）；不承诺「已 GC 的历史仍可查」。

### D7. SSE 是瞬态读者，不是订阅

- `SendStreamingMessage` / `SubscribeToTask` 的连接进程用 `broker_queue:read_from/3` 从任务日志的某个 offset 开始读，**不创建 SubRef、不写 `.cursors`、不参与 `min(live cursor)`**。
- 因此：慢 SSE 客户端**不 pin retention**、不反压任务执行；它只让自己的连接落后。
- 出站有界：`sse_max_pending_bytes`（默认 1 MiB）与 `sse_send_timeout_ms`（默认 30 s）；超时关连接，不堆 mailbox。
- 落后于 `committed`（事件已被 GC）时，发一条**当前 Task 快照**（`StreamResponse.task`，`metadata._a2a.resync = true`）再从尾部继续；不发明未定义的 A2A 错误码。
- 心跳：每 15 s 一行 SSE 注释；不计入 JSON-RPC 事件。

### D8. 大小上限：Task 事件与 Artifact 共用 Broker 的消息上限

- 单个 HTTP 请求体 ≤ `min(broker_limits:max_message_bytes(), a2a.max_body_bytes)`，默认 1 MiB。
- `Part.raw` 在 JSON 中是 base64，**按解码前长度判限**；1 MiB 请求体最多承载约 768 KiB 原始字节。
- `Part.data` 受 `max_json_depth`（默认 16）约束。
- 更大的内容走 `Part.url`：Broker 只校验 `https` 语法，**不取回**内容（避免变成 SSRF 跳板）。
- `Artifact` 分块（`append = true`）每块是独立消息，各自受限；总量不设硬上限，由任务生命周期与 retention 约束。

### D9. `contextId` / `referenceTaskIds` 只做索引，不做外键

- `contextId` 是投影里的普通字段，`ListTasks` 按它过滤；不建立跨任务的强一致外键。
- `referenceTaskIds` 原样保留在 `Message` 里，Broker 不校验被引用任务是否存在（A2A 的「不透明执行」原则）。
- 同一 `contextId` 下允许并行 follow-up Task（spec）；不串行化 context。

## 后果（Consequences）

- **+** Task 事件复用 WAL/fsync/`replicated_commit`、retention、DLQ/TTL、CloudEvents 与 trace 通道，没有第二套存储/消息内核。
- **+** 内核不感知 A2A：`broker_queue`/`broker_subscription` 的语义不变，既有 823/823 的契约不被任务状态机污染。
- **+** SSE 不 pin retention，长连接不会把磁盘钉死；慢客户端只影响自己。
- **–** 任务状态是 **adapter 内存 + 重放**，不是强一致分布式状态：跨节点访问同一任务需要粘性路由（按 `TaskId` 哈希到 owner），S9-B 必须实现并在能力矩阵写明；非 owner 节点 `GetTask` 返回重定向错误或代理（S9-B 决定，ADR 不冻结传输细节）。
- **–** retention 配置错误会让长任务「历史丢失」并被迫 `FAILED`；这是显式契约，不是静默降级，但需要运维培训与告警。
- **–** boot 全量重放（无 checkpoint 时）的耗时与日志大小成正比；S9-B 必须给出实测数字，不能预先承诺。
- **–** 同租户的普通订阅者可以看到 `$a2a/task/...` 事件（与 `$dlq` 同先例）；需要完全私有的事件必须走独立租户或在 ACL 上拒绝 `$a2a/` 订阅。

## 验证（Validation）

S9-B/S9-C 的 Gate 证据必须覆盖：

1. **状态机**：每个合法迁移一条用例；每个非法迁移返回正确错误码（`-32002`/`-32004`）；终态后 `SendMessage`/`CancelTask` 均被拒。
2. **崩溃恢复**：`kill -9` 后重启，投影与崩溃前一致；checkpoint 损坏时全量重放成功；非终态任务历史被 GC 时补 `FAILED/history_lost` 并计数。
3. **可靠性逐档**：`fsync` 档在 append 返回前断电不丢创建/终态；`os_buffer` 档按文档允许丢失；`replicated_commit` 在失去多数派时拒绝写入（与 ADR-007 的「不得静默成功」一致）。
4. **retention**：非终态任务被 retention 越过时，指标与告警触发；`GetTask` 不返回半截任务。
5. **SSE**：慢客户端不增长 Broker 进程 mailbox；不 pin retention（`retention_status/0` 无该连接导致的 `slow_consumer`）；落后 resync 后状态正确。
6. **大小**：1 MiB 请求体边界、base64 膨胀、`url` 不取回（用不可达 URL 证明 Broker 不发请求）。
7. **真实客户端**：官方 A2A SDK/sample 跑通 Task 全生命周期（S9-B Gate）。

## 冻结后补充

（暂无。S9-A 起若发现本 ADR 与实现冲突，按项目惯例在此追加批注，不改冻结正文。）

### 落地批注（S9-B，2026-11-12）

本节只记录**实现与冻结正文的差异/细化**，D1~D9 正文一字未改。代码见 `apps/broker_proto_a2a/src/`（`a2a_task` / `a2a_event` / `a2a_task_projection` / `a2a_task_shard` / `a2a_task_registry` / `a2a_task_recovery`），证据见 `docs/protocol-compatibility.md` §4.15。

| 决策 | 落地情况 | 差异 |
|---|---|---|
| D1 事件 = 普通 `broker_message` | 完全照做：`$a2a/task/<TaskId>`、`payload_encoding = json`、`persistence = durable`、`reliability = task_reliability`、分区走 `broker_router_hash:route/3`、写入走 `broker_queue:append/3`；`_meta.unified-mq` 带 `seq` + `kind` | 无。`_meta` 的两个键落地为 `<<"_meta">> => #{<<"unified-mq">> => #{<<"seq">>, <<"kind">>}}`，`kind` 用 proto 的 `statusUpdate`/`artifactUpdate` 驼峰名 |
| D2 投影在 adapter 内存（ETS + 分片 gen_server） | 完全照做：每分片一张 ETS `a2a_task_shard_<I>`（`set` + `read_concurrency`）+ 一个 `gen_server`，`phash2(TaskId, N)` 路由，`task_shards` 默认 `schedulers_online` | **`ListTasks` 的分页在 registry 汇总**：全局稳定页无法由各分片各自分页拼出，故 `list/3` 扇出到全部分片、由 registry 合并排序再切页；`page_size` 默认 50 上限 100 按 D2 执行 |
| D3 终态不可变三层（串行化 + 追加前校验 + 重放复核） | 三层都在：分片串行、append 前 `a2a_task:legal_transition/2`、重放复核。非法迁移在**活路径**返回错误码，在**重放路径**只记日志并保留最后一个合法状态 | **`a2a_state_violation` 计数器未加**（`broker_metrics` 键集冻结，统一在 S9-F 动），S9-B 用 `logger:warning`；`WORKING → WORKING` 显式允许（进度上报），已作为迁移表的一行落地 |
| D4 可靠性档 `fsync` 默认、创建与终态同档 | 完全照做：`a2a_event:reliability/0` 读 `task_reliability`（默认 `fsync`），创建事件与终态事件用同一档；`replicated_commit` 且 `broker_queue:reliability() =/= replicated_commit` 时 **boot 拒绝**（`{error, {task_reliability_requires_replicator, _}}`，`a2a_event:validate_reliability/0`） | 无 |
| D5 崩溃恢复 = checkpoint + 增量重放 | **只做了增量重放的退化版：全量重放**。`a2a_task_recovery` 在 `init/1` 里从各分区 offset 0 读起、过滤 `$a2a/task/` 前缀折叠 | **checkpoint 未实现**：`data/a2a/replay.idx` 不存在，`task_checkpoint_dir` / `task_checkpoint_interval_ms` 两个 env 已声明但**不读**。全量重放先作为正确性基线（ADR §D5 的「无 checkpoint 时从 0 请求」分支即本条）；S9-F 定稿**不做** checkpoint 优化并删除这两个死 env，见文末「定稿说明（S9-F）」 |
| D6 retention 契约 + `history_lost` 响亮失败 | `history_lost` 照做：首条可见事件不是创建快照 → 补一条服务端生成的 `TASK_STATE_FAILED` 终态事件（`metadata.reason = history_lost`），且该补写事件自身在下次重放时可被识别，重复 boot 幂等 | **`a2a_task_history_lost` 计数器与 `config/alerts.yml` 的 `a2a_task_beyond_retention` 未加**（同上，归 S9-F）；`docs/operations.md` §5.1 已给容量估算与日志特征对照表作为过渡 |
| D7 SSE 是瞬态读者 | **未落地**（S9-C）：`SendStreamingMessage` / `SubscribeToTask` 仍回 `-32601` | 本 ADR 关于 SSE 的所有约束（不建 SubRef、不 pin retention、resync 快照、`sse_max_pending_bytes`）在 S9-C 生效，S9-B 不产生任何 SSE 路径 |
| D8 大小上限 | `max_body_bytes` 与 `broker_security.limits.max_message_bytes` 取 min；`413` 不读体直接关连接；`Part.url` 不取回 | 无 |
| D9 `contextId` 只做索引 | 完全照做：`ListTasks` 按 `contextId` 过滤（投影字段），无跨任务外键；`referenceTaskIds` 原样保留不校验 | 无 |

### 落地批注（S9-C，2026-11-12）

D7 的 SSE 在 S9-C 落地，代码见 `apps/broker_proto_a2a/src/a2a_stream.erl`（配合 `a2a_rpc:a2a_stream_plan/3`），证据见 `docs/protocol-compatibility.md` §4.16。

| 决策 | 落地情况 | 差异 |
|---|---|---|
| D7 SSE 是瞬态读者、不建 SubRef、不 pin retention | **照做**：连接进程只 `broker_queue:subscribe/2` 挂一个分区邮箱钩子（唤醒用，不是 SubRef），自己按 cursor `read_from/3`；不写 `.cursors`、不进 `min(live cursor)`、不参 `broker_router` 的 `Filter → [SubRef]` 索引 | 无 |
| D7 心跳每 15 s 一行 SSE 注释 | 照做：`sse_heartbeat_ms`（默认 15000）到点发 `: ping` | 无 |
| D7 出站有界 `sse_max_pending_bytes`（1 MiB）+ `sse_send_timeout_ms`（30 s） | **改用时间上限**：`sse_max_duration_ms`（模块默认 600000，出厂 `sys.config` 配 1800000）到点关流；单帧上限 `sse_max_event_bytes`（1 MiB）另算 | `sse_max_pending_bytes` / `sse_send_timeout_ms` **两个 env 未落地也没读**——SSE 写是同一连接进程内的同步 `send`，pending 由 socket 发送缓冲自然反压，不堆 mailbox，故没另设字节水位；真实的上界来自「一条流活多久」。这是对正文的**收窄**（更简单、可测），不是放宽 |
| D7 落后于 `committed` 时发**当前 Task 快照**（`metadata._a2a.resync = true`）再续 | **改为响亮失败**：历史 offset 越界时发一个 `status: failed` 事件后**关流**，客户端自行重连（`SubscribeToTask` 从当前可读起点重放） | **没有 resync 快照那一步**：发快照要求连接进程能读投影（跨到 shard ETS），会把 SSE 与分片耦合；关流+重订阅用同一条「offset 0 重放」路径达成等价可用性，且不新增私有 `_a2a.resync` 元数据键 |

**收尾说明**：D7 已落地（上表）；D5 的 checkpoint、D6 的 `a2a_task_history_lost` 计数器仍「文档承诺、代码未到位」，与 S9-C 新增的 Push/Stream 失败计数器一并归 S9-F，`docs/roadmap.md` §11.1 与 `docs/operations.md` §5.2 已留痕。**S9-C 未落地 D7 的两个 env（`sse_max_pending_bytes` / `sse_send_timeout_ms`）已在 `config/sys.config.src` 中不存在**（不保留「声明但不读」的死配置）。

### 定稿说明（S9-F，2026-11-12；冻结文本不改，只结掉「留 S9-F」的悬空项）

S9-F 的边界是「证据 + 矩阵 + 审计 + 容量 + 文档同步」，**不新增实现**；下列三项按「偏差改文档」结案，均有明确留痕：

| 悬空项 | S9-F 结论 | 留痕位置 |
|---|---|---|
| D5 的持久化 replay checkpoint（`data/a2a/replay.idx`） | **不实现**：恢复始终是分区 offset 0 全量重放（正确性基线，也是当前可扩展性上界——启动时间随日志量增长）。S9-B 声明但从未被读的 `task_checkpoint_dir` / `task_checkpoint_interval_ms` 两个 env **已作为死配置删除** | 本 ADR 上方 D5 行、`docs/architecture.md` 延期表与 env 表、`docs/operations.md` §重启成本、`docs/protocol-compatibility.md` §4.15 |
| D3/D6 的失败路径计数器（`a2a_state_violation` / `a2a_task_history_lost` / `a2a_checkpoint_corrupt`）与 S9-C 的 Push/Stream 失败计数器 | **不加**：`broker_metrics` 键集保持 S9-A 之前的冻结集合（ADR-016 冻结 API），三协议失败路径一律只 `logger` | `docs/security.md` §12.1 第 17 条、`docs/operations.md` §告警占位 |
| D7 的两个 SSE 水位 env（`sse_max_pending_bytes` / `sse_send_timeout_ms`） | 维持 S9-C 的结论：**删除死配置，改用时间上限**（`sse_max_duration_ms` + `sse_max_event_bytes`） | ADR-026 §D7 落地注记（S9-C）、本 ADR D7 行 |
