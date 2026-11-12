# S9-F 执行计划 — 一致性收口（官方 SDK 证据 + 能力矩阵 + 安全审计 + 容量 + 文档同步）

- **对应设计**：[`docs/roadmap.md`](../roadmap.md) §11.1 S9-F 行与 §11.2 Gate S9 全 8 项；[`docs/design/s9-agent-protocols.md`](s9-agent-protocols.md) §4.4（OQ 收敛后无新增开放问题）；[ADR-025](../adr/025-a2a-task-log-mapping.md) / [ADR-026](../adr/026-agent-adapter-front-end.md) / [ADR-027](../adr/027-mcp-front-end-kernel-mapping.md)。
- **基线（改动前）**：`rebar3 ct` **1062/1062、68 套件**；`dialyzer` 0 warning；两种 release 通过（S9-E 收尾，文档 `1abe376`）。
- **范围**：**不新增协议前端、不新增状态机、不新增运行时依赖、不动 `broker_metrics` 冻结键集**。五件事：
  1. 三协议各一份**上游官方 SDK 真实客户端证据** + harness（`test/protocol-compatibility/s9f/`）；
  2. **能力矩阵与限制清单**收口（`docs/protocol-compatibility.md` §2 + 逐协议小节）；
  3. **安全审计**（`docs/security.md` §12.1 三协议明确未实现项）；
  4. **压测 / 容量报告**（`docs/benchmarks/2026-11-12-s9f-{mcp,a2a,acp}.json` + `2026-11-12-s9f-capacity.md`）；
  5. **文档同步**（roadmap / README / progress / architecture / operations）+ ADR 定稿说明。
- **边界**：本阶段允许的**唯一代码改动**是修真实 SDK 暴露的协议偏差（实际发生 1 处：ACP `request_permission` 的 `outcome` 嵌套）；其余偏差一律**改文档**，或**删掉「声明但不生效」的死配置**（实际删 2 个 env）。
- **执行约束**（仓库既定规则）：编译/测试前 `export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"`；`rebar3 ct` 前 `rm -rf _build/test/extras`；Python 一律 `/usr/bin/python3`（3.12）；用例数由 `all/0`（含 `groups/0`）算；ADR 冻结文本不动、偏差处加批注；CT 一律 `port => 0`，不动 8081/8082；数据目录在 tmpfs（`require_durable_fs => false`），报告中不得声称 ext4 持久化性能。

## 任务拆分

1. **Task 1 — 三协议官方 SDK 证据 harness**：`test/protocol-compatibility/s9f/`——`src/s9f_node.erl`（同节点起 MCP 18096 + A2A 18094 + 假 MCP 后端）、`mcp/mcp_sdk_client.py`、`a2a/a2a_sdk_client.py`、`acp/acp_evidence.erl` + `acp/acp_sdk_agent.py` + `acp/acp_sdk_agent_tee.py`、`bench/*.py` + `acp/acp_bench.erl`、`README.md`。数据目录默认 tmpfs，可用 `S9F_DATA` / `S9F_ACP_DATA` / `S9F_ACP_BENCH_DATA` 覆盖；`__pycache__/`、`_build/`、`data*/`、`node.out`/`node.err`、`*.beam` 进 `.gitignore`。
2. **Task 2 — MCP 三阶段证据**（`mcp 2.3.0`）：[A] `Client(mode="auto")` 让 SDK 真发 `server/discover` 并 adopt；[B] `mode="2026-07-28"` pin 下 `list_tools`/`list_resources`/`list_prompts`/`call_tool`（含 `notifications/progress` 与未知工具名）；[C] `client.listen(tools_list_changed=True)` 取 `honored` 子集 + 真实 `ToolsListChanged()` 事件。
3. **Task 3 — A2A 六步证据**（`a2a-sdk 1.3.0`，protobuf）：`A2ACardResolver` 解析 Agent Card → `ClientFactory.create_from_url` → `SendMessage`（`task_auto_complete` 跑完整生命周期）→ 二次 `SendMessage` → `GetTask` → `ListTasks`。
4. **Task 4 — ACP 证据**（`agent-client-protocol 1.0.0`）：Broker 作 Client，经**生产 `acp_port`** `spawn_executable` 拉起官方 SDK Agent 子进程；`acp_sdk_agent_tee.py` 在 fd 0 前接 OS 管道 tee 出逐字节 client→agent 帧。
5. **Task 5 — ACP wire 修复 + 测试**：`acp_policy:wire/1` 在 JSON-RPC 边界把内部扁平 decision 转成 ACP v1 规范嵌套 `outcome`；`acp_agent` 写回包改调它；`acp_policy_SUITE` 新增 `wire_result_is_nested`，`acp_agent_SUITE` 两处断言改嵌套。
6. **Task 6 — 压测 / 容量**：MCP 顺序 200 + 并发 16、A2A 顺序 200 + 并发 16、ACP 顺序 200 + 并发 4；原始 JSON + 报告（含前置约束、机器负载、不可外推声明）。
7. **Task 7 — 文档收口**：`docs/protocol-compatibility.md`（§2 能力矩阵补 A2A 行 + §4.17.9 / §4.18.8 / §4.19）、`docs/security.md` §12.1、`docs/roadmap.md` §11.1 + Gate S9、`README.md` 状态行、`progress.md`、`docs/architecture.md` 延期表 / env 表、`docs/operations.md` 告警占位与重启成本。
8. **Task 8 — ADR 定稿说明**：ADR-025「定稿说明（S9-F）」、ADR-026 §D5「落地后修正（S9-F）」、ADR-027「定稿说明（S9-F）」；冻结文本一律不动。

## DoD（对照 Gate S9）

- [x] **三协议各一份真实第三方客户端证据落盘**：MCP `mcp 2.3.0` 三阶段（`mcp/sdk_mcp.out`）、A2A `a2a-sdk 1.3.0` 六步（`a2a/sdk_a2a.out`）、ACP `agent-client-protocol 1.0.0` 五场景 + 7 帧 `acp/client_to_agent.jsonl`（`acp/sdk_acp.out`）。
- [x] **能力矩阵逐行留痕**：§2 三行（A2A / MCP / ACP）的「已知限制 / 转换损失」列逐条对应到 §4.14~§4.19；`docs/security.md` §12.1 三协议 **18 条**明确未实现项（MCP 7 / ACP 5 / A2A 3 / 三者共同 3）。
- [x] **Task 状态机与终态不可变有自动化测试**：`a2a_task_SUITE` 11 + `a2a_task_projection_SUITE` 8 + `a2a_event_SUITE` 6（S9-B）；`acp_session` 17 + `acp_agent` 17（S9-E）；崩溃恢复由 `a2a_task_recovery` boot 全量重放证明。
- [x] **认证接六协议统一身份表**：`require_auth` 默认 `true`、`false` 只允许回环；MCP 门禁矩阵含 401/403/ACL 越权；A2A push 的 SSRF/签名/防重放专项。
- [x] **Push Notification 专项用例**：`a2a_stream_push_SUITE` `guard` 组 8 例（S9-C）。
- [x] **既有 `rebar3 ct` 零回归**：**1063/1063、68 套件** = 基线 1062 零回归 + `acp_policy_SUITE` 新增 1 例；`dialyzer` 0 warning；`rebar3 release` 装配成功。
- [x] **压测 / 容量报告落盘，数字全部来自实测**：`docs/benchmarks/2026-11-12-s9f-capacity.md` + 三份 JSON。
- [x] **S9 ADR 定稿 + 文档同步**：ADR-025/026/027 定稿说明；roadmap（§1/§3/§11.1/§11.2）、README、progress、architecture、operations、protocol-compatibility、security 全部同步。

## 落地偏差记录

1. **ACP `request_permission` 的 `outcome` 是嵌套对象，不是扁平字符串**（本阶段唯一代码改动）。官方 SDK 以 `RequestPermissionResponse.model_validate` 把 Broker 原发的 `{"outcome":"selected","optionId":...}` 拒为 `model_attributes_type` / `Invalid params` `-32602`；ACP v1 的 `result.outcome` 是判别联合 `DeniedOutcome | AllowedOutcome`。修复：`acp_policy:wire/1`（+ `wire_meta/2`）在 JSON-RPC 边界做嵌套投影（`selected` 带 `optionId`、`cancelled` 不带，`reason`/`_meta` 随路），`acp_agent:312` 改调它；内部 decision 形状（`decide/2`、`audit_body/4`、`$acp/permission` 投影、全部依赖扁平键的断言）不变。ADR-026 §D5 加「落地后修正（S9-F）」批注，`docs/protocol-compatibility.md` §4.18.8.1 记缺陷与修复。
2. **证据与压测跑在 tmpfs 数据目录 + `require_durable_fs => false`**：`/tmp/opencode/s9f-evidence/{data,data-acp,data-acp-bench}`。报告逐处标注「不含 ext4 fsync 开销，不代表交付配置的持久化性能」。
3. **机器有外部负载**：ACP 并发吞吐同参数跑出 81.6 / 95.9 / 105.9 / 263.5 ops/s；报告只给量级、明确不给容量结论。
4. **MCP 顺序 p50 ≈ 31 ms 含空闲 tick**：`mcp_stream:wait_next/2` 的 `heartbeat_ms` 粒度属设计内轮询，非纯 I/O 等待；报告如实说明，未另配对照实验。
5. **MCP `mode="2026-07-28"` pin 时 SDK 报空能力是诚实读法**：2026 版本把 `serverInfo` 标为可选，pin 模式 SDK 不探测；真正的协商证据在 `mode="auto"`。§4.17.9 已写明两种读法。
6. **A2A SDK 1.3.0 的 API 形状差异**：`send_message` 返 `AsyncIterator[StreamResponse]`，`get_task` / `list_tasks` 直接返回；`GetTaskRequest(id=...)` 而非 `name`。这是 SDK 侧差异，不是 Broker 差异（§4.19 已记）。
7. **删掉两个从未被读的死配置**：`broker_proto_a2a.task_checkpoint_dir` / `task_checkpoint_interval_ms`（S9-B 声明、无人读）。S9-F 定稿：**不做持久化 checkpoint**，恢复保持分区 offset 0 全量重放（正确性基线，也是当前可扩展性上界）。同步更新 architecture（延期表 + env 表）、operations（重启成本）、protocol-compatibility（§4.15 两处）与 ADR-025「定稿说明」。
8. **失败路径计数器明确不加**：`broker_metrics` 保持 S9-A 之前的冻结键集；A2A / MCP / ACP 的失败事件只 `logger`。列为 `docs/security.md` §12.1 第 17 条，operations「告警占位」改为「S9-F 定稿：仍不加计数器」并保留日志特征对照表。
9. **Agent Card 管理面 CRUD 未排期**：architecture 延期表由「S9-F」改为「未排期」。
10. **`docs/compatibility/` 属他会话产物**：不提交，也不在本阶段的证据链中引用。

## 证据快照

### MCP（`mcp 2.3.0`，`test/protocol-compatibility/s9f/mcp/sdk_mcp.out`）

| 阶段 | 关键输出 |
|---|---|
| A 协商 | `protocol_version=2026-07-28`；`server_info: name='unified-mq', version='0.1.0'`；`capabilities={prompts:{}, resources:{}, tools:{}, completions:{}}`；`list_tools() → ['emit-list-change']` |
| B 透传 | 每个结果带 `_meta.io.modelcontextprotocol/serverInfo`、`ttl_ms=3600000`、`cache_scope=public`、`result_type=complete`；`call_tool("emit-list-change")` 先收 `notifications/progress`（1.0/2.0）再收终止 result；`call_tool("does-not-exist")` **透传成功**（R-B1 实测） |
| C 长活流 | `honored: tools_list_changed=True`（其余 `None`）；另一客户端触发 `emit-list-change` 后本流收到 `ToolsListChanged()`；退出时 Broker 优雅关流 |

### A2A（`a2a-sdk 1.3.0`，`test/protocol-compatibility/s9f/a2a/sdk_a2a.out`）

Agent Card（`unified-mq-evidence-agent` v1.0.0 / JSONRPC / skill `echo`）→ `ClientFactory.create_from_url` → `SendMessage` 得 `TASK_STATE_COMPLETED` + `history` 2 条 → 二次 `SendMessage` 新 task 同样 COMPLETED → `GetTask` 直接返回 COMPLETED → `ListTasks` 全部 COMPLETED。

### ACP（`agent-client-protocol 1.0.0`，`test/protocol-compatibility/s9f/acp/`）

```
== HANDSHAKE ==          state=ready   session_id=<<"acp-sdk-session-...">>
== plain ==              agent_message_chunk×1 + stopReason=end_turn
== permission(allow) ==  turn end_turn + $acp/permission decision=selected reason="matched allowlist"
== permission(deny) ==   turn stopReason=refusal + $acp/permission decision=cancelled
== FINAL ==              state=gone   stop=<<"refusal">>
```

7 帧 client→agent 原文见 `acp/client_to_agent.jsonl`；两个权限回包都是规范嵌套 `outcome`（`selected` 带 `optionId: "allow-once"`，`cancelled` 不带）。

## 容量结论（只给量级）

| 协议 | 顺序 | 并发 | 备注 |
|---|---|---|---|
| MCP（`tools/call` 全网关） | 30.1 ops/s，p50 30.65 ms | 16 路 79.8 ops/s，p50 183.6 ms | p50 含 `heartbeat_ms` 轮询粒度 |
| A2A（`SendMessage` + 自动完成） | 62.3 ops/s，p50 15.19 ms | 16 路 150.3 ops/s，p50 49.1 ms | 单 POST 建 Task |
| ACP（`session/prompt` 回合投影） | 113.2 ops/s，p50 8.53 ms | 4 路 105.9 ops/s，p50 14.85 ms | 4 个 Python agent 子进程，并发无收益 |

**不外推**：tmpfs 数据目录、无 fsync、单节点回环、有外部负载、样本 200；不声称 ext4 / 集群 / TLS / 公网 RTT 容量。

## 门禁实测表

| 门禁 | 命令 | 结果 |
|---|---|---|
| 全量 CT | `rm -rf _build/test/extras && rebar3 ct` | **1063 passed / 0 failed，68 套件**（`acp_policy_SUITE` 17、`acp_agent_SUITE` 17） |
| 静态分析 | `rebar3 dialyzer` | **0 warning** |
| Release | `rebar3 release` | 装配成功（`lib/` 含 `broker_proto_a2a` / `broker_proto_mcp` / `broker_proto_acp`） |
| MCP 证据 | `PYTHONPATH=/tmp/opencode/s9f-libs /usr/bin/python3 mcp/mcp_sdk_client.py` | 退出码 0，三阶段输出落 `mcp/sdk_mcp.out` |
| A2A 证据 | `PYTHONPATH=/tmp/opencode/s9f-libs /usr/bin/python3 a2a/a2a_sdk_client.py` | 退出码 0，六步输出落 `a2a/sdk_a2a.out` |
| ACP 证据 | `erl -noshell ... -eval 'acp_evidence:run(), init:stop().'` | 退出码 0，五场景 + 7 帧落 `acp/` |
| 压测 | 见 `docs/benchmarks/2026-11-12-s9f-capacity.md` §1 | 三份 JSON 落盘 |
