# S9-B 执行计划 — A2A 核心：Agent Card + Task 状态机 + 事件溯源日志

- **对应设计**：[`docs/design/s9-agent-protocols.md`](s9-agent-protocols.md) §4.3 S9-B 行；[ADR-025](../adr/025-a2a-task-log-mapping.md)（Task → 内核映射，全篇）；[ADR-026](../adr/026-agent-adapter-front-end.md) §D3（Agent Card 落 `broker_meta.agent_card`）；[《S9-0 收敛报告》](s9-0-convergence-report.md) §3（Task 语义）/§5（Agent Card 落点）
- **基线（改动前）**：`rebar3 ct` **908/908、59 套件**；dialyzer / xref / `rebar3 release` / `rebar3 as quic release` 通过；HEAD `7ee3a4e`
- **范围**：把 S9-A 的 `SendMessage` **echo** 升级成真正的 **Task 生命周期**：`SendMessage` 创建 Task（SUBMITTED→WORKING→终态）、`GetTask`/`ListTasks`/`CancelTask` 读投影，Task 事件落 `$a2a/task/<id>` 普通日志、投影在 adapter 内存、boot 重放恢复；Agent Card 落 `broker_meta.agent_card` 新表 + `/.well-known/agent-card.json`（ETag/304）+ catalog。
- **阶段顺序**：S9-B 是 S9-C（SSE/Push）的前置。S9-C/D/E 各自另写计划。
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

**S9-B 结束时可以证明的事实（Gate 原文）**：官方 A2A SDK/sample 跑通 Task 全生命周期（create → 若干状态 → 终态 → `GetTask`/`ListTasks`/`CancelTask`）；Task 事件以普通 `#broker_message{}` 落 `$a2a/task/<id>`，投影是事件折叠、串行化、终态不可变；`kill -9` 后重启投影一致（重放恢复）；Agent Card 在 `/.well-known/agent-card.json` 可取、ETag/304 生效、Mnesia 重启后仍在；既有 908 例零回归。

**明确不做**（留给后续阶段）：

| 项 | 阶段 |
|---|---|
| `SendStreamingMessage`/`SubscribeToTask` 真实 SSE 流、resync 快照 | S9-C |
| Push Notification 出站 webhook、SSRF/签名/防重放、DLQ | S9-C |
| MCP / ACP app | S9-D / S9-E |
| 能力矩阵终稿、`security.md` §12、README 状态行、压测/容量报告 | S9-F |
| a2a_* 失败路径计数器（`a2a_state_violation` 等） | S9-F（统一动 `broker_metrics` 冻结键集时一并加，S9-B 用 `logger` 记事件） |
| 持久化 checkpoint 文件优化（`data/a2a/replay.idx` 增量） | S9-B 用**全量重放**作为正确性基线；checkpoint 优化列 S9-F 演进位 |

---

## Task 1 — `broker_meta`：新增 `agent_card` 表（amends ADR-002）

**目标**：按 ADR-026 §D3 在 `broker_meta` 新增第 12 张表 `agent_card`，只新增不改既有 11 张表形状。提供读写 helper 与 catalog 查询骨架。

- `tables/0` 追加 `{agent_card, [agent_card, agent_ref, tenant_id, version, status, card, extended_card, updated_at]}`（主键 `agent_ref = {TenantId, AgentId}`；`card`/`extended_card` 为 JSON binary 或 map）。
- 新增 helper：
  - `put_agent_card/1`（upsert，写前在同一事务内读取旧行并让 `version` 单调 +1，`updated_at` 刷新——version 是 ETag 的来源，不另设 `bump_agent_card_version/1`）、`get_agent_card/2`、`delete_agent_card/2`（按 `{TenantId, AgentId}`）
  - `agent_cards/1`（按租户过滤，供 adapter catalog）
- 写前 schema 校验在 adapter 内做（`name`/`description`/`supportedInterfaces`/`version`/`capabilities`/`defaultInputModes`/`defaultOutputModes`/`skills` 必需），`broker_meta` 只存不校验。
- 更新 `broker_meta_SUITE` 的 `schema_has_the_frozen_tables` 断言：表清单加入 `agent_card`（这是 ADR-026 显式 amends ADR-002 的机械后果）；新增 CRUD 用例。

## Task 2 — `broker_proto_a2a`：Task 状态机（纯函数）

**目标**：`a2a_task.erl`——无 I/O 的状态机，供 registry 与投影共用。

- `TaskState = unsubmitted | submitted | working | input_required | auth_required | completed | failed | canceled | rejected`（proto `TASK_STATE_*`）。
- `legal_transition/2`：实现 ADR-025 §D3 的迁移表（终态无出边）。
- `is_terminal/1`、`kind/0`（事件 kind 列表）。
- 迁移不合法 → `illegal_transition`（adapter 映射为 `-32002`/`-32004`，见 Task 4）。

## Task 3 — `broker_proto_a2a`：事件源 + 投影 + registry（串行化 + 恢复）

**目标**：Task 事件落普通日志、投影是事件折叠、分片串行化、boot 重放。

- `a2a_event.erl`：
  - `append(TaskId, TenantId, Kind, Payload) -> {ok, Offset} | {error, term()}`：构造 `#broker_message{destination = <<"$a2a/task/", TaskId/binary>>, payload = EventJson (含 _meta), payload_encoding = json, persistence = durable, reliability = task_reliability}`，`broker_router_hash:route/3` 求分区后 `broker_queue:append/3`（照 `broker_event` 的 `write_quarantine/2` 范式）。EventJson = 线上 JSON（`StreamResponse` oneof 之一）+ `_meta: {"unified-mq": {seq, kind}}`。
  - `fold/2`：把一条事件折进投影 map。
- `a2a_task_projection.erl`：纯函数。`fold_events([Event]) -> {Task, Status}` 或 `history_lost`（第一个可见事件不是创建事件 / 创建事件已被 GC）。
- `a2a_task_registry.erl` + `a2a_task_shard.erl`：
  - supervisor 起 N = `broker_proto_a2a.task_shards`（默认 `erlang:system_info(schedulers_online)`）个 shard，每个 shard 拥有 ETS `a2a_task_tab_<N>`（投影）+ 一个 `phash2(TaskId, N)` 路由。
  - API：`create/3`、`get/1`、`list/3`、`cancel/2`、`apply_event/3`（分片串行校验迁移 → append → 折叠投影）。
  - 终态不可变：append 前校验；终态后 `SendMessage`→`-32004`、`CancelTask`→`-32002`。
- `a2a_task_recovery.erl`：boot 时全量重放——遍历所有分区 `broker_queue:read_from(Part, 0, BigLimit)` 折叠 `$a2a/task/` 前缀事件重建投影；无 checkpoint 时按 ADR-025 §D5 从 offset 0 请求，retention 抬起点即 `history_lost` 信号。
- `broker_proto_a2a_sup`：registry（N shard）+ recovery 作为 listener **之前**的子进程；`broker_proto_a2a_app:start/2` 在起 sup 前触发一次 recovery（幂等）。

## Task 4 — 方法接线：`SendMessage`/`GetTask`/`ListTasks`/`CancelTask`

**目标**：`a2a_rpc.erl` 把 echo 换成 Task 生命周期，接 registry。

- `SendMessage`：校验 `message` → `registry:create(TenantId, ContextId, Message)`（SUBMITTED → WORKING → COMPLETED 一次同步推进，echo agent 立即完成；`return_immediately` 语义在 S9-C 细化）→ 返回 `result.task`（含 `id`/`contextId`/`status`/`history`）。
- `GetTask`：`registry:get(TaskId)`；不存在 → `task_not_found`（`-32001`/404）；非本租户 → `task_not_found`（不泄露存在性）。
- `ListTasks`：`registry:list(TenantId, Filters, Page)`——`contextId`/`status` 过滤、`page_size` 默认 50 上限 100、`page_token` 不透明；返回 `{tasks, nextPageToken}`。
- `CancelTask`：`registry:cancel(TaskId)`；非终态 → CANCELED + append 事件；终态 → `task_not_cancelable`（`-32002`/400）。
- `require_auth=false` 时 tenant 用 `broker_security:default_tenant/0`（匿名身份）。

## Task 5 — Agent Card：`/.well-known/agent-card.json` + ETag/304 + catalog

**目标**：ADR-026 §D3 / 收敛报告 §5。

- `a2a_card.erl`：
  - `GET /.well-known/agent-card.json`：无租户上下文时返回**公开** card（跨租户汇总或默认 catalog 首项，按部署语义；S9-B 先做「按租户 + 匿名回环」两档）；`If-None-Match` 匹配 ETag → `304`；`Cache-Control: max-age=60`。
  - ETag = `<<"\"", Version/binary, "\"">>` + 内容哈希；认证请求返回 `extended_card`（按身份裁剪，不下沉到 `broker_meta`）。
  - catalog：`page_size` 默认 50 上限 100，按 skill/tag/provider/capability 过滤在 adapter 内做。
- boot 期配置种子：若 `broker_proto_a2a.agent_cards` env 有初始 card，写入 `broker_meta`（管理 API 完整 CRUD 在 S9-F，S9-B 只 seed + 读）。
- 路由：`a2a_rpc` 的 `handle/1` 已按 path 分派；新增 `/.well-known/agent-card.json` 分支。

## Task 6 — 全量验证与文档回填

- 门禁：`rebar3 ct`（删 `_build/test/extras`）、`dialyzer`、`xref`、`release` + `as quic release`。
- 用例数由 `all/0` 回填；基线 908 区分「基线」与「S9-B 新增」。
- 文档：`docs/protocol-compatibility.md`（S9-B 段：能力矩阵 + Task 生命周期 + Agent Card + curl 证据）、`docs/architecture.md`（`broker_meta` 12 表、A2A Task 数据流 §3.13 扩展、§2.3 依赖）、`docs/operations.md`（retention 估算公式 + `a2a_task_beyond_retention` 告警占位）、`progress.md`、`docs/roadmap.md` §11.1 S9-B 行。
- ADR-025/026 冻结文本不改；偏差加「落地后修正」批注。

## 完成判据（Definition of Done）

- [x] `broker_meta` 新增 `agent_card` 表（12 表），CRUD + catalog helper 通过；冻结表断言更新
- [x] `a2a_task` 状态机：每个合法迁移一用例；每个非法迁移映射正确错误码；终态后 `SendMessage`/`CancelTask` 被拒
- [x] Task 事件落 `$a2a/task/<id>` 普通 `#broker_message{}`；投影是事件折叠；同 Task 串行
- [x] `GetTask`/`ListTasks`/`CancelTask` 全通；`ListTasks` 分页/过滤边界
- [x] boot 重放恢复：`kill -9` 后重启投影一致；非终态任务历史被 GC → `FAILED/history_lost` + 日志
- [x] `SendMessage` 返回 `result.task`（不再 echo `result.message`）；`a2a_listener_SUITE` echo 用例更新为 Task 断言
- [x] Agent Card：`/.well-known/agent-card.json` + ETag/304；Mnesia 重启后仍在；catalog 过滤/分页
- [x] `rebar3 ct` 总例数 = 908 + 新增（实际 952），零 FAILED；dialyzer/xref 干净；两种 release 都含新代码
- [x] `curl` 真实客户端证据落盘；`protocol-compatibility.md` / `architecture.md` / `operations.md` / `progress.md` / `roadmap.md` 回填

### 门禁（实测，2026-11-12）

| 门禁 | 命令 | 结果 |
|---|---|---|
| 全量 CT | `rm -rf _build/test/extras && rebar3 ct` | **952 passed / 0 failed、62 个套件**（基线 908/59，零回归；本轮新增 3 套件 + 扩 2 套件） |
| dialyzer | `rebar3 dialyzer` | **0 warning**（过程中修掉 `broker_message:offset()`/`sync_mode()` 未导入类型、`a2a_event` 不可达 `{error, _}` 分支、`a2a_card` 不可达 `is_atom` 守卫） |
| xref | `rebar3 xref` | 除既有 unused-export 外无新告警（新代码零告警） |
| release | `rebar3 release` | 成功，`lib/` 含 `broker_proto_agent_rpc` + `broker_proto_a2a` |
| quic release | `rebar3 as quic release` | 成功，同上游（`broker_proto_a2a` 与 quic 无耦合） |
| curl 证据 | 18093 临时节点 + `/tmp/opencode/a2a_evidence_run.sh` | **25 条**请求/响应，原文见 `docs/protocol-compatibility.md` §4.15.6 |

### 用例数（S9-B 交付时点，均由 `all/0`（含 `groups/0`）实数）

| 套件 | 例数 | 说明 |
|---|---|---|
| `broker_meta_SUITE` | 10 → **12** | 冻结表断言改为 12 表；新增 `agent_card_crud_bumps_version`（version 单调 +1）、`agent_card_registration_is_serialized`（并发写 version 1..10 无缺口） |
| `a2a_task_SUITE` | **11** | 状态/终态/中断态分类、ADR-025 §D3 迁移表逐项、非法迁移全枚举、终态无出边、`WORKING→WORKING`、中断态回 `WORKING`、wire 名往返、`from_wire` 兼容短名/拒绝未知、事件 kind 往返 |
| `a2a_event_SUITE` | **6** | destination 往返、事件是 durable 普通消息（字段全断言）、`message_id` 跨重放稳定、encode/decode 往返、非 A2A payload 拒绝、`reliability` 默认 `fsync` |
| `a2a_task_projection_SUITE` | **8** | 创建→完成折叠、history/artifacts 跟踪（同 id 替换不重复追加）、活记录非法迁移拒绝、重放记录非法迁移只记日志保留最后合法态、首事件必须是创建快照、`history_lost` 修补幂等、`to_wire` 形状往返、`mark_history_lost` 落终态 `FAILED` |
| `a2a_listener_SUITE` | 16 → **33** | S9-A 的 `echo_roundtrip` 改写为 `send_message_creates_task`（Task 断言）；新增 `GetTask` 往返/404、`ListTasks` 过滤/分页、`CancelTask` 非终态/终态 400、跨租户 404、事件在共享日志、`kill -9` 后重放恢复、`history_lost`、well-known 匿名/认证 extended/ETag 304/未知 agent 404、catalog 过滤分页/租户隔离、`GetExtendedAgentCard`；S9-A 的 15 条门控用例保留 |

**基线 908 → 952（净增 44、套件 59 → 62）**：新增 3 个套件 25 例（`a2a_task_SUITE` 11 + `a2a_event_SUITE` 6 + `a2a_task_projection_SUITE` 8）+ `broker_meta_SUITE` +2 + `a2a_listener_SUITE` +17 = **44**，逐项相加与总数一致（`echo_roundtrip` 是**改写**成 `send_message_creates_task` 而非删除后新增，故不产生额外净增）。

## 落地偏差记录（执行时追加）

| # | 计划原文 | 实际落地 | 原因 |
|---|---|---|---|
| 1 | Task 1：`get_agent_card/2`、`delete_agent_card/2`（按 `{TenantId, AgentId}`）与 `agent_cards/1`（按租户） | 一致；`put_agent_card/1` 是唯一用 `/1` 的写入口（card 里自带 tenant），三个读入口都是 `/2`（`get_agent_card(TenantId, AgentId)`、`delete_agent_card(TenantId, AgentId)`、`agent_cards(TenantId)`） | 读路径必须显式带 tenant 才能做租户隔离；`/1` 只能用于「card 自带身份」的写 |
| 2 | Task 4：`SendMessage` 返回 `result.task`，「`return_immediately` 语义在 S9-C 细化」 | 用 `task_auto_complete`（默认 `true`）覆盖两档：`true` 时同步跑完 `SUBMITTED→WORKING→agent message→COMPLETED`（即 `return_immediately=false`）；`false` 时停在 `SUBMITTED` 交外部执行器，`CancelTask` 可直接取消 | 没有外部执行器时无法验证「异步」路径；用配置开关比伪造执行器更接近真实部署 |
| 3 | Task 3/4：`a2a_state_violation` / `a2a_task_history_lost` / `a2a_checkpoint_corrupt` 计数器 | **未加**，三条路径当前只 `logger:warning/2` | `broker_metrics` 键集冻结（ADR-016），新增键需与 S9-F 的能力矩阵一起动；S9-B 不做半套 |
| 4 | Task 3：`a2a_task_recovery` 从 checkpoint 增量重放 | **全量重放**：遍历所有分区 `read_from(Part, 0, BigLimit)` 折叠 `$a2a/task/` 事件；`task_checkpoint_dir` 已声明但**不读** | 全量重放是正确性基线，先保证「投影 = 日志」；checkpoint 是纯优化，S9-F 演进位 |
| 5 | Task 4：`require_auth=false` 时 tenant 用 `broker_security:default_tenant/0`（匿名身份） | 一致；但**匿名 ≠ 所有租户**——匿名只能看到 `default_tenant`（出厂 `t0`）名下的 Task 与 Agent Card | 收敛报告 §5 的「公开 card」在本阶段解释为「默认租户的 card」，跨租户汇总需要管理面语义（S9-F） |
| 6 | Task 4：`ListTasks` 的 `page_size` 默认 50 上限 100 | 一致；查询串 `pageSize` 按**文本**解析（`string:to_integer/1`），非整数一律按默认 50，不报错 | HTTP 查询串没有类型，宽松解析避免客户端因小失误拿不到列表 |
| 7 | Task 5：catalog（`agent_cards/1` + 过滤） | 落地为项目私有的 `GET /agents`（A2A spec 未定义注册/检索 API），`skill`/`tag`/`provider`/`capability` 四类过滤 + 分页 | 需要给运维/客户端一个可用的发现面，但不愿自造与 spec 冲突的语义，因此在文档里明确标注「项目私有接口」 |

## 风险与对策

| 风险 | 对策 |
|---|---|
| 自建事件源 + 投影的 boot 重放写错，崩溃恢复丢任务 | 投影是纯函数折叠，重放只重建不发事件；CT 用真实 `kill -9` 容器验证（照 S1 T14 范式） |
| `$a2a/task/<id>` 日志被 retention GC 后投影出半截任务 | 重放检测 `history_lost`，补 `FAILED/history_lost`（ADR-025 §D6 响亮失败），不返回半截 Task |
| 跨节点访问同一 Task 的粘性路由未做 | S9-B 单节点；能力矩阵写明「非 owner 节点 GetTask 返回重定向/代理」为 S9-C+ 演进位 |
| 分片 ETS 所有权在 shard 重启时丢投影 | shard 重启即触发局部重放（从 checkpoint/0）；registry 把 shard 设 `transient`，崩溃由 app 重启兜底 |
| `SendMessage` 行为从 echo 变 Task 破坏 S9-A 用例 | 更新 `a2a_listener_SUITE` 的 `echo_roundtrip` 为 Task 断言；S9-A 的 echo 是中间态，本就是 S9-B 要替换的 |
