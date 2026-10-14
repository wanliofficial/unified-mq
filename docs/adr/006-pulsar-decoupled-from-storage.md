# ADR-006：Pulsar 协议兼容与原生存储解耦

- **状态**：已采纳（S0 定稿）
- **日期**：2026-10-14
- **关联**：README §5.1（Pulsar 硬性要求）、roadmap §7 S5；S0.4 客户端矩阵

## 背景（Context）

Pulsar 自带 BookKeeper 作为原生存储。若把「Pulsar 协议兼容」与「集成 BookKeeper 存储」绑定，会让协议兼容目标被存储选型绑架，难以独立验收。

## 决策（Decision）

先验证 **Pulsar Wire Protocol 与客户端语义**的兼容性；客户端**硬性要求使用官方 Pulsar Java Client**（README 5.1，S0.4 已确认 `pulsar-client:3.3.1` 可获取）。是否集成 BookKeeper 是**独立的存储决策**，不在协议兼容范围内——UnifiedMQ 的数据面仍走自有 WAL / Raft 复制（ADR-002/008）。Pulsar 协议适配按四阶段推进：P1 基础连通 → P2 消费语义 → P3 可靠性 → P4 扩展。

## 后果（Consequences）

- **+** 协议兼容目标可独立定义与验收，不被存储绑定。
- **-** 需明确定义 Pulsar 语义（订阅/ACK/重试/租约）到内核的映射，且必须说明与 BookKeeper 原生语义的差异。

## 验证（Validation）

S0.4 客户端与版本矩阵（`docs/protocol-compatibility.md`）；S5 Pulsar 四阶段交付独立测试套件 + 能力矩阵一行。
