# S9-F 三协议官方 SDK 真实客户端证据 harness

这一目录是 Gate S9 第 1 项「三协议各一份真实第三方客户端证据」的落地。它**不是** `rebar3 ct`
的一部分：CT 套件覆盖协议状态机与门控顺序（见各 app 的 `test/`），这里覆盖的是
「**用上游官方 SDK 当客户端，能不能真的跑通**」这一层。

三个协议各有一个官方 SDK 客户端/服务端驱动：

| 协议 | 官方 SDK | 版本 | 角色 | 驱动 |
|---|---|---|---|---|
| MCP | [`mcp`](https://pypi.org/project/mcp/)（Python） | 2.3.0 | **客户端**（Broker 作网关/Server 端） | `mcp/mcp_sdk_client.py` |
| A2A | [`a2a-sdk`](https://pypi.org/project/a2a-sdk/)（Python） | 1.3.0 | **客户端**（Broker 作网关） | `a2a/a2a_sdk_client.py` |
| ACP | [`agent-client-protocol`](https://pypi.org/project/agent-client-protocol/)（Python） | 1.0.0 | **Agent 服务端**（Broker 作 Client） | `acp/acp_evidence.erl` 经生产 `acp_port` 拉起 `acp/acp_sdk_agent.py` |

> 官方 SDK 装在哪台机器、哪个 venv 都不进仓库（`__pycache__/`、`_build/`、`data*/` 均被
> `.gitignore` 排除）。跑之前先把包装好，例如：
>
> ```bash
> /usr/bin/python3 -m pip install --target /tmp/opencode/s9f-libs \
>   mcp==2.3.0 a2a-sdk==1.3.0 agent-client-protocol==1.0.0
> ```

## 前置条件

1. **必须用 `/usr/bin/python3`（3.12）**。PATH 上的 `python3` 若解析到 conda 3.13，其
   `pydantic_core` 与本机 `pydantic 2.14.3` 不兼容，会在 `import pydantic_core` 处报
   `cannot import name 'ArgsKwargs'`。
2. 所有 Python 驱动都要带 `PYTHONPATH`，指向 SDK 安装目录（默认 `/tmp/opencode/s9f-libs`）。
3. Broker 代码在仓库里已编译：`rebar3 compile`（OTP 28，`_build/default/lib/*/ebin`）。
4. 证据端口 **18094（A2A）/ 18096（MCP）** 绑定 `127.0.0.1`；**不动 8081/8082**，也默认
   不监听（证据节点显式配置这两个端口）。ACP 无 listener：由 Broker 经 `acp_port`
   `spawn_executable` 拉起本地 Python agent 子进程。

## 跑法

### 0. 起证据节点（MCP + A2A 前端 + 假 MCP 后端）

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
cd test/protocol-compatibility/s9f
LIBS=$(for d in ../../../_build/default/lib/*/ebin; do printf -- "-pa %s " "$d"; done)
erlc -o /tmp/opencode/s9f-ebin $LIBS -I src src/s9f_node.erl src/mcp_evidence_backend.erl
setsid erl -noshell -sname s9fnode $LIBS -pa /tmp/opencode/s9f-ebin -pa src \
  -eval 'logger:remove_handler(default), \
         ok=logger:add_handler(default, logger_std_h, #{config => #{type => standard_error}}), \
         s9f_node:run().' \
  > node.out 2> node.err < /dev/null &
disown
sleep 8 && tail -1 node.out     # 期望: S9F_NODE_READY mcp=18096 a2a=18094
```

数据目录默认 `/tmp/opencode/s9f-evidence/data`（tmpfs，配 `require_durable_fs => false`，
**只用于证据，不代表交付配置的持久化性能**）；要换持久化目录就设 `S9F_DATA`。

### 1. MCP（`mcp 2.3.0`）

```bash
PYTHONPATH=/tmp/opencode/s9f-libs /usr/bin/python3 mcp/mcp_sdk_client.py | tee mcp/sdk_mcp.out
```

三段证据（原文见 `mcp/sdk_mcp.out`）：

- **[A] 能力协商**：`Client(mode="auto")` 让 SDK 自己发一次 `server/discover` 并 adopt，
  打印的 `protocol_version=2026-07-28`、`server_info`、`capabilities={tools,resources,prompts,completions}`
  是官方 SDK 从 Broker 的 discover 回复里**解析出来**的（不是我们自报）。
- **[B] 逐请求**：`mode="2026-07-28"` 直接 adopt（2026 无握手），`list_tools` / `list_resources` /
  `list_prompts` 每帧都带 `_meta.io.modelcontextprotocol/serverInfo` 与 `ttl_ms=3600000`；
  `call_tool(emit-list-change)` 触发后先收到 `notifications/progress` 再收到终止 `result`；
  `call_tool(does-not-exist)` **透传成功**——网关不校验工具名（R-B1：Broker 不解释工具语义），
  假后端对任意名字都回包，这正是透传网关该有的行为。
- **[C] 长活流**：`client.listen(tools_list_changed=True)` 由 SDK 打开 `subscriptions/listen`，
  打印 `honored` 子集；另一个客户端触发 `emit-list-change` 后，本流收到真正的
  `ToolsListChanged()` 事件，随后客户端主动关流、Broker 优雅收尾。

### 2. A2A（`a2a-sdk 1.3.0`，protobuf）

```bash
PYTHONPATH=/tmp/opencode/s9f-libs /usr/bin/python3 a2a/a2a_sdk_client.py | tee a2a/sdk_a2a.out
```

SDK 1.3.0 是 protobuf API（`a2a.types`）：从 `/.well-known/agent-card.json` 解析 Agent Card
（`unified-mq-evidence-agent` v1.0.0、JSONRPC 绑定、skill `echo`），非流式客户端经
`ClientFactory.create_from_url` 建连；`SendMessage`（`task_auto_complete=true`）走完整
SUBMITTED→WORKING→COMPLETED，返回的 `StreamResponse.task.status.state=TASK_STATE_COMPLETED`
且 history 2 条；`GetTask` / `ListTasks` 都直接可用。

### 3. ACP（`agent-client-protocol 1.0.0`，Broker 作 Client）

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
cd test/protocol-compatibility/s9f
LIBS=$(for d in ../../../_build/default/lib/*/ebin; do printf -- "-pa %s " "$d"; done)
erlc -o /tmp/opencode/s9f-ebin $LIBS -I src src/*.erl acp/acp_evidence.erl
rm -rf /tmp/opencode/s9f-evidence/data-acp
S9F_ACP_DATA=/tmp/opencode/s9f-evidence/data-acp \
erl -noshell $LIBS -pa /tmp/opencode/s9f-ebin -pa acp -pa src \
  -eval 'logger:remove_handler(default), \
         ok=logger:add_handler(default, logger_std_h, #{config => #{type => standard_error}}), \
         acp_evidence:run(), init:stop().' 2>&1 | tee acp/sdk_acp.out
```

Broker 是 ACP **Client**：`acp_evidence:run/0` 配置一个 `acp_port` 传输、`command=/usr/bin/python3`
的 agent 子进程（跑 `acp/acp_sdk_agent.py` 这份**官方 SDK Agent**），随后主动发
`initialize` / `session/new` / `session/prompt`，只接一个强制回调 `session/request_permission`，
再读 `$acp/session/<sid>` 的 turn 投影与 `$acp/permission` 的审计。

`acp/acp_sdk_agent_tee.py` 是裸 agent 的旁路 tee：SDK 用 `connect_read_pipe(sys.stdin)` 读
fd 0，Python 层的 stdin 包装会被绕过，所以它在 fd 0 前面接一个 OS 管道、用线程把每个字节
**原样**写进 `acp/client_to_agent.jsonl`（`S9F_AGENT_FRAMES` 可改路径），agent 本身不改。
证据跑的是这个 tee 壳，因此 `client_to_agent.jsonl` 是**逐字节的 client→agent 帧**。

### 4. 压测 / 容量（数字见 `bench/*.json` 与 `docs/benchmarks/2026-11-12-s9f-capacity.md`）

```bash
# MCP / A2A：官方 SDK 客户端，顺序 200 + 并发 16
PYTHONPATH=/tmp/opencode/s9f-libs /usr/bin/python3 bench/mcp_bench.py 200 16 | tee bench/mcp_bench.json
PYTHONPATH=/tmp/opencode/s9f-libs /usr/bin/python3 bench/a2a_bench.py 200 16 | tee bench/a2a_bench.json

# ACP：Broker-as-Client 驱动官方 SDK agent（4 个 agent 进程），顺序 200 + 并发 4
erlc -o /tmp/opencode/s9f-ebin $LIBS -I src src/*.erl acp/acp_bench.erl
S9F_ACP_BENCH_DATA=/tmp/opencode/s9f-evidence/data-acp-bench \
erl -noshell $LIBS -pa /tmp/opencode/s9f-ebin -pa acp -pa src \
  -eval 'acp_bench:run(200, 4), init:stop().'
```

`bench/*.json` 是**最后一次干净跑**的原始输出；同参数重跑吞吐会差 2.5 倍以上（证据机当时
只有约 4 GiB 可用且带外部负载），所以报告里只给量级、不给容量结论。

## 已发现并修复的真实缺陷

官方 ACP SDK 把 Broker 原来发的扁平 `session/request_permission` 结果
（`{"outcome":"selected","optionId":...}`）以
`RequestPermissionResponse.model_validate` 拒绝为判别联合 `DeniedOutcome | AllowedOutcome`
（`model_attributes_type` → `Invalid params` -32602）。这只有真实 SDK 客户端能暴露出来。
修复是 `acp_policy:wire/1` 在 JSON-RPC 边界把内部扁平 decision 转成规范嵌套对象
（`selected` 带 `optionId`、`cancelled` 不带），`acp_agent` 写响应改调它；内部 decision 形状
（测试 + `audit_body`）不变。详见 `docs/protocol-compatibility.md` §4.18.8 与
`docs/design/s9-e-execution-plan.md` 落地偏差记录。
