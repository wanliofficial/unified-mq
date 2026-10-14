# ADR-003：采用 rabbitmq/ra 作为 Raft 实现

- **状态**：已采纳（S0 定稿）
- **日期**：2026-10-14
- **关联**：README §6.2（Raft 职责）、roadmap §2 S0.1；S0.1 spike

## 背景（Context）

共识层需要 Raft，但自行从零实现 Raft 风险与成本极高。需先评估成熟库再决定生产采用方式。

## 决策（Decision）

采用 **rabbitmq/ra 3.2.0** 作为 Metadata Raft Group 与分区复制的共识层。已在 **OTP 28.3.1** 上完成端到端验证：

- 3 节点集群经 `ra:start_or_restart_cluster/5` 正常形成；
- 500/500 复制写入成功，~677 µs/写（1477 写/s）；
- 杀死 Leader 后自动重选，日志索引连续（523→524）；
- **失去多数派时写入被正确拒绝**（返回 `{timeout, _}`），而非静默成功。

> 修正：S0.1 早期曾认为 ra 仅支持 OTP 26/27、需锁 OTP 27。实测 ra 3.2.0 在 OTP 28.3.1 上编译与运行均通过，故全项目统一 OTP 28.3.1（见 S0.1 REPORT §5）。

## 后果（Consequences）

- **+** 免去自研 Raft 的巨大风险与工作量。
- **+** 失去多数派正确拒绝写入，直接支撑 ADR-007 的显式可靠性承诺。
- **-** ra 上游 CI 仅覆盖 OTP 26/27；升级 ra 版本时需复跑 S0.1 harness（`run_cluster.sh all`）确认无回归。
- **-** 每组有固定机器开销，须受 ADR-008 分组策略约束。

## 验证（Validation）

S0.1 spike（`spikes/s0_1_ra/REPORT.md`）：6 个 API 陷阱、上游缺陷、OTP 决策均已记录。
