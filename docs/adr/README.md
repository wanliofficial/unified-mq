# 架构决策记录（ADR）索引

本目录收录 UnifiedMQ 的关键架构决策（ADR）。ADR-001~008 由 S0 技术验证阶段定稿（roadmap §9.1）；ADR-009~011 为 S1 三个核心抽象的决策，当前状态**已采纳**（T0.5 起草，2026-10-21 经 F1~F6/G1 评审整改后冻结）。

| ADR | 主题 | 关键依据 |
|---|---|---|
| [001](001-protocol-adapter-separation.md) | 协议 Adapter 与消息内核分离 | README §8；S1 三抽象 |
| [002](002-mnesia-control-plane-only.md) | Mnesia 只承担控制面元数据 | README §6.1；S4 控制面 |
| [003](003-use-rabbitmq-ra.md) | 采用 rabbitmq/ra 作为 Raft 实现 | S0.1 spike（OTP 28.3.1 验证） |
| [004](004-cloudevents-optional.md) | CloudEvents 是可选事件标准化层 | S0.5 spike |
| [005](005-event-id-vs-broker-id.md) | 事件 ID 与 Broker Message ID 分离 | S3 Binding Mapper |
| [006](006-pulsar-decoupled-from-storage.md) | Pulsar 协议兼容与原生存储解耦 | S0.4 客户端矩阵；README 5.1 |
| [007](007-explicit-reliability.md) | 可靠性承诺必须显式化（三档等级） | S0.1 / S0.3 spike；README 6.3 |
| [008](008-raft-grouping-and-storage.md) | 分区 Raft 分组模型、故障域与存储方案 A | S0.2 / S0.3 spike |
| [009](009-broker-message-record.md) | 统一消息模型 `broker_message` 记录定稿 | S1 T0.5；ADR-005/007 |
| [010](010-broker-router-behavior.md) | `broker_router` behavior 与路由/订阅索引边界 | S1 T0.5；ADR-001 |
| [011](011-broker-storage-behavior.md) | `broker_storage` behavior、能力模型与磁盘协议 | S1 T0.5；S0.3 spike；ADR-007/008 |

### S2 决策（已采纳，2026-11-07 冻结）

| ADR | 主题 | 关键依据 |
|---|---|---|
| [012](012-qos2-semantics-and-release-state.md) | QoS 2 承诺边界与 release 状态持久化 | MQTT 3.1.1 §4.3.3；ADR-007/009 |
| [013](013-subscription-kinds-and-cursor-ownership.md) | 订阅类型、cursor 归属与会话文件 v2 迁移 | README §5.2；ADR-010；S2 设计 D2/D9 |
| [014](014-retention-and-segment-gc.md) | retention、物理段删除与慢消费者守卫 | ADR-011；README §5.3/§7.2 |
| [015](015-redelivery-dlq-and-ttl.md) | 重投递、DLQ 与消息 TTL 的归属与形状 | ADR-009/013；README §5.2/§10.1 |
| [016](016-observability-minimal-set.md) | 可观测性最小集与「无外部依赖」取舍 | README §10.2/§7.2 |
| [017](017-mqtt5-properties-and-compat.md) | MQTT 5 properties 落位与 3.1.1 兼容边界 | ADR-009 §9.2；README §3.4 |
| [018](018-retain-will-and-qos2-amendments.md) | Retain/Will 语义与持久化边界；QoS 2 握手幂等与 `.inflight` v2 形状修订 | MQTT 3.1.1 §3.1.2.5/§3.3.1.3/§3.6.1；ADR-007/012；S2 设计 §4.3 |
| [019](019-shared-assignment-ack-ownership-and-session-layout.md) | Shared 的 worker 原子 claim、`ack/4` 归属校验与会话文件按 SubRef 布局 | README §5.2；ADR-010/**013（修订 §13.2/§13.3/§13.5）**/015；S2 设计 §4.4 |

### S3 / S4 决策（已采纳，2026-11-08 冻结）

| ADR | 主题 | 关键依据 |
|---|---|---|
| [020](020-cloudevents-codec-policy-binding.md) | CloudEvents 1.0 Codec、五档策略与 MQTT Binding 边界 | README §3/§4/§10.1/§11.2；ADR-004/005/009/015；替代 ADR-004 的 pass_through-only 过渡口径 |
| [021](021-cluster-control-plane-raft-and-fencing.md) | 集群控制面、Metadata Raft 与 Epoch/Fencing | README §6/§11.3；ADR-002/003/007/008/010 |

### S8 决策（已采纳，2026-11-10 / 2026-11-11）

| ADR | 主题 | 关键依据 |
|---|---|---|
| [022](022-mqtt-sn-gateway.md) | MQTT-SN 网关：单 socket / 多会话、topic id 三表与休眠语义；**不复用 `mqtt_conn_fsm`** | MQTT-SN 1.2 §5.4/§6.2/§6.13；ADR-001/010/013/018 |
| [023](023-mqtt-over-websocket-two-processes.md) | MQTT over WebSocket 的两进程分层；帧解码不进 `decode_msg/2` | MQTT 3.1.1 §6 / 5.0 §6；RFC 6455；ADR-001 |
| [024](024-mqtt-over-quic.md) | MQTT over QUIC：连接/流两层、profile 门控依赖、流信用与 `active, once` | OASIS MQTT TC《Single Stream Mode》CN Draft 3 §2.1；ADR-001/022/023 |

### S9 决策（已采纳，2026-11-11 冻结）

| ADR | 主题 | 关键依据 |
|---|---|---|
| [025](025-a2a-task-log-mapping.md) | A2A Task 状态机到统一消息内核的映射：Task 事件 = 普通 `broker_message`（事件溯源），Task = adapter 投影，不用订阅游标；可靠性档绑定 ADR-007；retention 为运维契约 | A2A 1.0.0 `specification/a2a.proto`（`Task`/`TaskState`/`StreamResponse`）、`docs/specification.md` §5.4/§9；ADR-001/002/007/009/013/014/015/026 |
| [026](026-agent-adapter-front-end.md) | Agent 接入前端：四 app 拆分（纯库底座 + 三 adapter）、ranch + `{packet, http_bin}` + OTP 28 `json` 自建 HTTP/1.1/SSE、Agent Card 落 `broker_meta.agent_card`（amends ADR-002）、端口/boot/relx 双清单、ACP 只做 stdio 且 Broker 扮 Client、egress pin IP 与 OAuth 不纳入 | A2A 1.0.0 §5.3/§5.5/§9、MCP 2026-07-28 architecture、ACP v1 overview；ADR-001/002/015/016/021/023/024/025 |

> S9 决策只冻结抽象与边界，S9-A~S9-F 的代码与 Gate 证据尚未产出（roadmap §11.1）；结论与证据汇总见 [`docs/design/s9-0-convergence-report.md`](../design/s9-0-convergence-report.md)。

## Gate S0 验收清单（roadmap §2）

- [x] S0.1（ra 评估）与 S0.3（WAL）产出明确结论（采用 ra / 采用方案 A 自研 WAL）
- [x] S0.4 的客户端与版本矩阵被采纳为 S5 全部协议测试的基线（`docs/protocol-compatibility.md`）
- [x] ADR-001 ~ ADR-008 从 README 摘要升级为 `docs/adr/00N-*.md` 正式文件
- [x] S0.2 产出分区分组决策（默认每组 16 分区，故障域 = 组大小），见 ADR-008

## Gate S1 验收清单（roadmap §3.4）

- [x] ADR-009 / 010 / 011 三份核心抽象决策定稿并冻结（**2026-10-21 冻结**；API 定义已在 OTP 28.3.1 `erlc` 实测通过，见各 ADR「验证」节；**T17 复核无偏离**）
- [x] 真实 MQTT 客户端完成 publish → subscribe → ack 全链路（**T15**：8/8 用例、46/46 帧字节一致）
- [x] `kill -9` 崩溃恢复断言 R1a/R1b/R2~R7 通过且符合契约（**T14**：双档 16/16 PASS）
- [x] `broker_storage` 的 WAL 与内存实现可互换（**T4/T5**：契约 13 例 × 2；**T13**：端到端双组 12/12）
- [x] ADR-011 附录 11.7「订阅位置不丢」措辞解释正式签认（**2026-11-06，T17**）
- [x] **Gate S1 之前不允许实现第二个协议**（`apps/` 始终只有 `broker_proto_mqtt`，T17 复核）

> **Gate S1 于 2026-11-06 通过**，签认记录见 ADR-011 §11.7、`docs/protocol-compatibility.md` §5 与 `progress.md`「Gate S1 验收结论」。

### 冻结后的真相源顺序

`docs/design/s1-mqtt-vertical-slice.md` §3 是 ADR 摘录，ADR-009/010/011 是决策与语义契约，`apps/broker_core/include/*.hrl` 与 `broker_router.erl` / `broker_storage.erl` 是实现真相。三者关系：ADR 定义契约 → 代码必须满足契约 → 文档摘录跟随 ADR。实现偏离冻结 API 即为缺陷（改实现，不改 ADR）；文档摘录滞后时以 ADR 为准。

修改已冻结 API 属破坏式变更（影响所有已落盘 WAL 段与后续全部 Adapter），必须：① 新开 ADR 标注 supersedes/amends；② 在对应 Gate 显式处置；③ 不得在实现 PR 中静默改签名。
