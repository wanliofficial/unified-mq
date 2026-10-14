# 架构决策记录（ADR）索引

本目录收录 UnifiedMQ 的关键架构决策（ADR），由 S0 技术验证阶段定稿（roadmap §9.1）。

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

## Gate S0 验收清单（roadmap §2）

- [x] S0.1（ra 评估）与 S0.3（WAL）产出明确结论（采用 ra / 采用方案 A 自研 WAL）
- [x] S0.4 的客户端与版本矩阵被采纳为 S5 全部协议测试的基线（`docs/protocol-compatibility.md`）
- [x] ADR-001 ~ ADR-008 从 README 摘要升级为 `docs/adr/00N-*.md` 正式文件
- [x] S0.2 产出分区分组决策（默认每组 16 分区，故障域 = 组大小），见 ADR-008
