# S9-F 容量与性能报告（2026-11-12）

- **对应**：[`docs/roadmap.md`](../roadmap.md) §11.2 Gate S9 第 5 项「压测 / 容量报告落盘」；[`docs/protocol-compatibility.md`](../protocol-compatibility.md) §2 能力矩阵 + §4.17.9 / §4.18.8 / §4.19 三协议官方 SDK 客户端矩阵
- **代码版本**：S9-F 工作树（HEAD `1abe376` 之后的 S9-F 改动 + `acp_policy:wire/1` 修复；S9-E 代码 `e23d253`、文档 `1abe376`）
- **机器**：x86_64 / 16 逻辑核 / **14 GiB（当时可用约 4 GiB，机器上有其他负载）** / 系统盘 `/dev/nvme0n1p4` **ext4** / OTP 28（ERTS 16.2）
- **原始数据**：
  - [`2026-11-12-s9f-mcp.json`](2026-11-12-s9f-mcp.json)
  - [`2026-11-12-s9f-a2a.json`](2026-11-12-s9f-a2a.json)
  - [`2026-11-12-s9f-acp.json`](2026-11-12-s9f-acp.json)
- **前置约束（如实标注，不外推）**：
  - 数据目录位于 **tmpfs**（`/tmp/opencode/s9f-evidence/data`、`data-acp-bench`），以 `broker_storage` 的 `require_durable_fs => false` 跑证据。**因此本报告的数字不包含任何 ext4 fsync / 持久化开销，也不代表交付配置的持久化性能**；交付配置容量需按 [`docs/operations.md`](../operations.md) 的 WAL 设置另行测量。
  - 节点是**单节点**、无复制、无集群；所有 listener 端口（18094 / 18096）绑定 `127.0.0.1`，仅本地回环。
  - 每个样本都是一次**完整生产数据路径**（客户端 ↔ 协议前端 ↔ 路由/队列/事件 ↔ 回读），不是 `broker_queue:append/3` 的裸核计时。
  - **机器有外部负载**：ACP 并发吞吐在一次会话里跑出过 81.6 / 95.9 / 105.9 / 263.5 ops/s 四个值（同参数、同代码），说明并发尾延迟被外部负载与调度主导。下面表内数字是**最后一次干净跑**的值，量级可信、单点数值不可外推。

## 1. 复跑命令

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"

# 先起证据节点（MCP 18096 + A2A 18094 双 listener）
cd /tmp/opencode/s9f-evidence
LIBS=$(for d in _build/default/lib/*/ebin; do printf -- "-pa %s " "$d"; done)
setsid erl -noshell -sname s9fnode $LIBS -pa ebin \
  -eval 'logger:remove_handler(default), \
         ok=logger:add_handler(default, logger_std_h, \
             #{config => #{type => standard_error}}), s9f_node:run().' \
  > node.out 2> node.err < /dev/null &

# MCP：官方 SDK 客户端（mcp 2.3.0），顺序 200 + 并发 16
PYTHONPATH=/tmp/opencode/s9f-libs /usr/bin/python3 bench/mcp_bench.py 200 16

# A2A：官方 SDK 客户端（a2a-sdk 1.3.0，protobuf），顺序 200 + 并发 16
PYTHONPATH=/tmp/opencode/s9f-libs /usr/bin/python3 bench/a2a_bench.py 200 16

# ACP：Broker-as-Client 经生产 acp_port 驱动官方 SDK agent（4 进程）
erlc -pa _build/default/lib/*/ebin -o /tmp/opencode/s9f-evidence/acp \
  /tmp/opencode/s9f-evidence/acp/acp_bench.erl
cd /tmp/opencode/s9f-evidence
erl -pa $LIBS -pa acp -noshell \
  -eval 'acp_bench:run(200, 4), init:stop().'
```

> 三者依赖同一套证据节点；ACP 由该节点按需 spawn agent 子进程（`/usr/bin/python3 acp_sdk_agent.py`）。`/tmp/opencode/s9f-evidence/acp/acp_sdk_agent_tee.py` 是裸 agent 的旁路 tee 壳（只加一行日志），压测直接用裸 agent 以免旁路 I/O 污染时序。
> MCP / A2A 脚本必须用 `/usr/bin/python3`（3.12）：`python3` 在 PATH 上解析到 conda 3.13，其 `pydantic_core` 与本机的 `pydantic 2.14` 不兼容。

---

## 2. 结果矩阵（最后一次干净跑）

### MCP — `tools/call` 全网关往返（官方 SDK `mcp 2.3.0`）

| 场景 | 样本 | 吞吐 | p50 | p95 | p99 | 均值 |
|---|---|---|---|---|---|---|
| 顺序 | 200 | 30.1 ops/s | 30.65 ms | 56.0 ms | 67.4 ms | 33.3 ms |
| 并发 16 | 192 | 79.8 ops/s | 183.6 ms | 281.4 ms | 296.2 ms | 192.6 ms |

### A2A — `SendMessage` + 自动完成（官方 SDK `a2a-sdk 1.3.0`，protobuf）

| 场景 | 样本 | 吞吐 | p50 | p95 | p99 | 均值 |
|---|---|---|---|---|---|---|
| 顺序 | 200 | 62.3 ops/s | 15.19 ms | 23.6 ms | 28.3 ms | 16.0 ms |
| 并发 16 | 192 | 150.3 ops/s | 49.1 ms | 257.1 ms | 522.3 ms | 87.6 ms |

### ACP — `session/prompt` 回合投影（Broker-as-Client，官方 SDK agent 子进程）

| 场景 | 样本 | 吞吐 | p50 | p95 | p99 | 均值 |
|---|---|---|---|---|---|---|
| 顺序 | 200 | 113.2 ops/s | 8.53 ms | 16.6 ms | 18.2 ms | 8.83 ms |
| 并发 4 | 200 | 105.9 ops/s | 14.85 ms | 34.5 ms | 97.0 ms | 22.4 ms |

---

## 3. 解读（与既有基线分开标注）

1. **三协议不是「谁更快」的单一排序，工作负载不同**：ACP 样本是 Broker 主动发起、Agent 侧回应的**单轮 turn 投影**，客户端不等 HTTP 响应流，顺序延迟最低（p50 ≈ 8.5 ms）。MCP 样本语义最重：一个 `tools/call` 要经 `mcp_dispatch → mcp_conn → 发布到 `$mcp/tools/call` → 假后端 → 回读 `$mcp/reply/...` 并在流上等待`，顺序 p50 ≈ 31 ms，是三者里最高的顺序延迟（见下条）。A2A 居中（p50 ≈ 15 ms），但其顺序吞吐（62 ops/s）高于 MCP（30 ops/s），因为 A2A 是「一次 POST 建 Task + 自动完成」，MCP 是「POST 后还要在 SSE 流上等回包」。

2. **MCP 顺序延迟的构成不是纯网络开销**：`mcp_stream:wait_next/2` 在 `request_timeout_ms` 到点前用 `after min(Remaining, heartbeat_ms())` 睡到下一次空闲 tick 才醒来检查新消息。p50 ≈ 31 ms 里含有这个 tick 粒度（`sse_heartbeat_ms`）的贡献，属设计内的空闲轮询，不是等待真实 I/O。要压出「无 tick 开销」的下界需另配一组把 `heartbeat_ms` 调小的对照，本次不纳入。

3. **并发下尾延迟普遍放大 5~10 倍**：三协议的并发 p95/p99 都明显高于顺序，符合「单进程队列 + 背压 + 尾延迟」预期。吞吐方面 MCP 30.1 → 79.8 ops/s、A2A 62.3 → 150.3 ops/s（并发 16 约为顺序的 2.4 倍）；**ACP 4 路并发吞吐（105.9）反而低于顺序（113.2）**，p50 也从 8.5 → 14.9 ms——4 个 Python agent 子进程 + tmpfs 数据目录在当时的机器负载下，调度竞争已经压过并发收益。这组数字是「并发没有帮上忙」的实测证据，不是 ACP 的容量上限。

4. **数值不可外推**：机器当时只有约 4 GiB 可用且带外部负载；同参数重跑吞吐差 2.5 倍以上。本报告只承诺「官方 SDK 真实客户端在证据节点上的端到端往返」这一量级，**不**给出容量规划结论。

## 4. 诚实边界

- 本报告**不**声称任何 ext4 持久化性能、集群吞吐、TLS 开销、公网 RTT 下的容量。
- 样本量小（顺序 200 / 并发 192~200），用于给出量级与协议间对比，非容量规划基线。
- A2A 的 `task_auto_complete` 由 broker 自跑 SUBMITTED→WORKING→COMPLETED；不测量外部 agent 真实推理时延。
- MCP 假后端（`mcp_evidence_backend`）只做 `result_for/2` 定形回包，不测量真实 server 处理时延。
- ACP 样本测的是「Broker-as-Client 发 prompt + 收 turn 投影」的往返，不测量 agent 侧真实推理。
- ACP 压测 harness 带一个**协议正确性护栏**：`acp_agent` 在 turn 投影发布**之前**就完成 `publish_turn`、之后才 `acp_session:end_turn`，因此「看到 turn 消息」与「会话回到 ready」之间存在一个极短的窗口；若 harness 在这个窗口里发下一个 prompt，会拿到真实的 `{error,{session_not_ready,prompting}}`（ACP 的「单轮在飞」语义）。harness 检测到该错误即 1 ms 后重试，并把重试次数写进输出；三次干净跑的 `retries=undefined`（零重试），说明窗口在实际负载下极少被踩到。
