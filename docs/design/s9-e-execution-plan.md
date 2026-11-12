# S9-E 执行计划 — ACP 前端（Broker 作 ACP Client，仅 stdio）

- **对应设计**：[`docs/design/s9-agent-protocols.md`](s9-agent-protocols.md) §4.2 S9-E 行、§3.1（R-T1/R-T2/R-T9/R-T10、R-B3/R-B4）、[`docs/design/s/s9-0-convergence-report.md`](../design/s9-0-convergence-report.md) §7.1（ACP 侧：Client=Broker）、§13（ACP 相关各条）；[ADR-026](../adr/026-agent-adapter-front-end.md) §D5（ACP 前端语义：Broker 作 Client）。
- **基线（改动前）**：`rebar3 ct` **1015/1015、65 套件**；dialyzer / xref / 两种 release 通过（S9-D 收尾）。
- **范围**：新增 `broker_proto_acp`（`broker_proto_acp.app.src` + `_app`/`_sup` + `acp_transport`（行为）/ `acp_port`（生产子进程传输）/ `acp_fake_transport`（测试态）/ `acp_frame` / `acp_policy` / `acp_turn` / `acp_session` / `acp_event` / `acp_agent`）。不新增 Mnesia 表（**不 amends ADR-002**）。不新增运行时依赖（仅 `ranch 2.3.0` + 内置 `ra 3.2.0` + OTP 28 自带 `json`）。
- **阶段顺序**：S9-E 承接 S9-D（MCP 前端已把「本地子进程 + 内核消息面」范式跑通）；S9-F（能力矩阵 + 压测 + 官方 SDK 真实客户端证据 + `docs/security.md` §12）在 S9-E 之后收口。
- **语义**（ADR-026 §D5 落地）：Broker 是 ACP **Client**，Agent 是 Broker 的**本地子进程**。`initialize` / `session/new` / `session/prompt` 由 Broker（Client）主动发起；`session/request_permission` 是**唯一**实现的 Client 回调，由 `acp_policy`（默认拒绝 + allowlist + 超时）裁决并审计；`fs/*` / `terminal/*` / `elicitation/*` 不广告、回 `-32601` 但**保持会话**；`clientCapabilities` 为空（`_meta.unified-mq` 注明 `fs/terminal/elicitation=false`）；只接受 `protocolVersion 1`，不匹配则记 `stop=unsupported_protocol_version` 且**不发** `session/new`。
- **执行约束**（仓库既定规则）：编译/测试前 `export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"`；`rebar3 ct` 前 `rm -rf _build/test/extras`；用例数由 `all/0` 回填；文档只写已实现事实；ADR/设计稿冻结文本不动，偏差加「落地偏差记录 / 落地注记」批注；不新增运行时依赖；CT 一律 `port => 0`，不动 8081/8082 固定端口；数据目录在 ext4 且位于 release 树外绝对路径。

## 任务拆分（DoD）

1. **Task 1**：`broker_proto_acp.app.src` + `_app`/`_sup` + 两个 relx 清单条目 + `config/sys.config.src` 段（`agents => []`、`permission_policy`（`default/allow/timeout_ms/notify_dest`）、`supported_protocol_version`）。boot 拒绝：agents 非良构 map 列表、allowlist 条目命名非 binary。无 `require_auth` 门控（ACP stdio 的信任边界是 OS 级进程访问，同 `mcp_stdio`；agent 由运维声明，绝不由远程请求指定）。
2. **Task 2**：`acp_frame`（newline-delimited 分帧 + JSON-RPC 2.0 信封 + `kind/1`：`request`/`response`/`notification`/`error`）+ `acp_event`（三个 `$acp/*` 目的地 + `append/3` 写内核 + `decode/1` + `reliability() => os_buffer`）。
3. **Task 3**：`acp_policy`（默认拒绝 + allowlist（`#{tool,title}`，两轴都须匹配，`any` 通配，不做子串/正则）+ 超时拒绝（`ElapsedMs > timeout_ms`）+ 授权选择（`allow_once` 优先、其次 `allow_always`、否则首个 `optionId`）+ 审计体 `audit_body/4`）。
4. **Task 4**：`acp_turn`（`session/update` 通知折叠为 turn 投影，含 `stopReason`；`update/2` 兼容 `params.update` 嵌套）+ `acp_session`（纯状态机：`initializing → ready → prompting → ready → gone`；`prompt/2` 要求已协商 session_id 且相等）。
5. **Task 5**：`acp_transport`（行为）+ `acp_port`（生产：`spawn_executable`，不经 shell，`{line, 1048576}`，binary/args/env 经 `to_str/1`，**不设** `stderr_to_stdout`）+ `acp_fake_transport`（测试态，内存 `{acp_sent, Port, Bin}`）。
6. **Task 6**：`acp_agent`（每连接一个 gen_server：spawn agent → handshake → serve `session/request_permission` → 跑 prompt turn；`feed_bytes/2` 单一字节入口；`$acp/in/<agent>` 瞬态读者按 **destination** 过滤；`run_prompt` 只处理入站目的地，绝不回读自身 `$acp/session/*` / `$acp/permission`）。
7. **Task 7**：`acp_frame_SUITE` + `acp_policy_SUITE` + `acp_agent_SUITE`（全部 `port => 0`）。
8. **Task 8**：真实 `acp_port` 路径证据（假 agent escript 走生产传输）；文档回填 + 两种 release + dialyzer/xref 复跑。

## DoD

- [x] `acp_frame_SUITE` 全绿（**14 例**）：分帧（跨块/多行/空行/`{noeol,Chunk}` 长行重组）、信封 kind 判定（request/response/notification/error/缺 id）、`kind/1` 全覆盖。
- [x] `acp_policy_SUITE` 全绿（**16 例**）：默认拒绝、allowlist 精确匹配（tool+title 两轴）、`any` 通配、无匹配即拒、超时拒绝、授权选择顺序、审计体内容。
- [x] `acp_agent_SUITE` 全绿（**17 例**）：handshake（`initialize` 空能力 + `session/new` 得唯一 session id + `protocolVersion` 1 校验）、`plain`（3 update + turn `end_turn`）、permission allow（`selected` + 审计）、`fs/*` 拒绝（`-32601` 保持会话）、`session/cancel`（turn `cancelled`）、permission deny（`cancelled` + 会话 `gone` + `stop=refusal`）、prompt 目的地过滤（自身会话消息不被当作 prompt 回读）、`prompt/2` 的 `no_session_id`/`session_mismatch` 拒绝。
- [x] 三套件合计 **47/47**；`broker_meta`/`broker_storage`/`broker_queue`/`broker_router`/`broker_proto_mcp`/`broker_proto_a2a` 零改动语义。
- [x] 零 FAILED；dialyzer **0 warning**；xref 除既有 unused-export 类外无新告警（无 undefined/deprecated/recursive 调用）。
- [x] 两种 release（`rebar3 release` / `rebar3 as quic release`）装配成功，`lib/` 含 `broker_proto_acp-0.1.0`。
- [x] 真实 `acp_port` 证据：假 agent 走生产子进程传输，handshake / plain / permission-allow / fs-reject / cancel / permission-deny 六场景全部符合；捕获 **11 帧** client→agent 原文。
- [x] 文档：§4.18 + roadmap §11.1/§11.2 + progress + architecture（§2 应用表 + §3.15）+ ADR-026 §D5 落地注记。

## 落地偏差记录（实现后回填）

1. **`fs/*` / `terminal/*` / `elicitation/*` 拒绝但保持会话**：ADR-026 §D5 写这些「不广告且回 `-32601` 并结束会话」。实现选择**不结束会话**（只回 `-32601` + `data.reason="capability not advertised"`），因为单个未支持的能力不应销毁一个已协商的会话；证据见 `docs/protocol-compatibility.md` §4.18.7（`fs/read_text_file` → `-32601`，随后 `session/prompt` 仍正常）。已按「ADR 冻结文本不动、偏差加批注」规则在 ADR-026 §D5 加「落地注记（S9-E）」。
2. **`session/request_permission` 是唯一实现的 Client 回调**：ADR-026 §D5 与 §7.1 把 fs/terminal/elicitation 列为 Client 会收到的回调；实现只实装 `session/request_permission`（ACP 规范的强制回调），其余按偏差 1 拒绝。`_meta.unified-mq` 明确 `fs/terminal/elicitation=false`，`role=client`，`version=1`。
3. **`clientCapabilities` 为空对象**：ACP `initialize` 的 `clientCapabilities` 规范上是可选；实现发 `{}` 并在 `_meta.unified-mq` 里声明本 Client 不代做任何副作用能力。不广告 = 不承诺，比发全 `false` 更保守。
4. **`protocolVersion` 只接受 1**：`supported_protocol_version` 默认 1。不匹配时记 `state` 保持、`stop=unsupported_protocol_version`，**不发** `session/new`（会话无从建立）。ADR-026 §D5 未规定版本协商失败的具体动作，此为实现自选的保守姿态。
5. **`{line, 0}` 非法端口选项**：`acp_port` 用 `{line, 1048576}`（1 MiB）。超过此长度的行以 `{noeol, Chunk}` 分块到达，`feed_bytes/2` 重组成完整帧——长帧降级为分块读而非失败。`open_port({spawn_executable,_})` 的 executable/argv/env 必须是 string，故 `to_str/1` 在 `acp_port:open/1` 单点把 binary 转 string（配置面说 binary，上线时是 string）。
6. **prompt 读者按 destination 过滤**：`$acp/in/<agent>`、`$acp/session/*`、`$acp/permission` 可能同分区。瞬态读者只处理 `in_destination(name)`，绝不把自身发布的会话/审计消息回读成 prompt（否则自激 prompt 循环）。这是 ADR-025 §D7「瞬态读者」规则在 ACP 数据面的具体落地。
7. **stdio 无认证层、无 logger 校验**：同 `mcp_stdio`，信任边界是「谁能启动这个进程」，身份为匿名本地身份。ADR-026 §D5 提到「stdio 的 stdout 纯净性由部署保证」；本 app 只用 `io:put_chars` 写协议帧、不校验 agent 的 logger 指向（ACP 协议帧走子进程管道，不占 Broker 自身 stdout，故与 MCP stdio 的 stdout 纯净性问题不同）。
8. **不新增 `broker_metrics` 冻结键**：握手失败、权限拒绝、agent 退出只走 `logger`（同 S9-C/S9-D 归 S9-F）。

## 真实 `acp_port` 路径证据（实现后回填）

- **驱动**：`/tmp/opencode/acp-evidence/src/acp_evidence.erl` 启动完整 Broker 内核（`broker_security`/`broker_storage`(wal)/`broker_router`/`broker_queue`/`broker_meta`），配置一个名为 `evidence-agent` 的假 ACP agent（`acp_port` 传输、`cwd=/home/wanli/workspace/acp-evidence`），依次跑六个场景，打印落在 `$acp/session/<sid>` 与 `$acp/permission` 的消息。
- **假 agent**：`/tmp/opencode/acp-evidence/fake_acp_agent.escript`（newline-delimited JSON-RPC，按 prompt 文本选场景；每个收到的帧 `RECV …` 打到 stderr，被 `acp_port` 的 `stderr_to_stdout` **不合并** 规则隔离到 Broker 日志）。
- **结果**（`/tmp/opencode/acp-evidence/evidence.out`，exit 0）：
  - HANDSHAKE → `state=ready`，`session_id=<<"acp-evidence-session">>`；
  - `plain` → 4 条（2×`agent_message_chunk` + 1×`cwd` + 1×`agent_thought_chunk`）+ turn `stopReason=end_turn`；
  - `permission(allow)` → agent 回 `outcome=selected` + `optionId=allow`；`$acp/permission` 审计记 `decision=allow`、匹配 `#{tool=edit,title=any}`；
  - `fs` probe → agent 发 `fs/read_text_file`，Broker 回 `-32601`（`data.reason="capability not advertised"`），会话保持、turn `end_turn`；
  - `cancel` → Broker 发 `session/cancel`，turn `stopReason=cancelled`，会话回 `ready`；
  - `permission(deny)` → agent 发 `execute`/`rm -rf /`，无 allowlist 命中 → Broker 回 `outcome=cancelled`，会话置 `gone`，turn `stopReason=refusal`。
- **client→agent 帧**（`/tmp/opencode/acp-evidence/client_to_agent.jsonl`，11 帧）：`initialize`（空 caps + `_meta.unified-mq`）、`session/new`（`cwd`+`mcpServers=[]`）、4×`session/prompt`、`session/request_permission`（allow）回 `selected`、`fs/read_text_file` 回 `-32601`、`session/request_permission`（deny）回 `cancelled`、`session/cancel`。

## 门禁实测表（实现后回填）

| 门禁 | 结果 |
|---|---|
| `rebar3 ct` | **1062/1062、68 套件**（基线 1015/65；净增 = `acp_frame_SUITE` 14 + `acp_policy_SUITE` 16 + `acp_agent_SUITE` 17） |
| `rebar3 dialyzer` | **0 warning**（修掉 1 条不可达分支 + `acp_port:open/1` 的 binary→string 单点转换） |
| `rebar3 xref` | 除既有 unused-export 外无新告警（无 undefined/deprecated/recursive 调用） |
| `rebar3 release` / `rebar3 as quic release` | 均成功，`lib/` 含 `broker_proto_acp-0.1.0` |
| 真实请求证据 | `docs/protocol-compatibility.md` §4.18.7：真实 `acp_port` 六场景 + **11 帧** client→agent 原文 |
