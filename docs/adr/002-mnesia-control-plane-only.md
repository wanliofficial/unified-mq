# ADR-002：Mnesia 只承担控制面元数据

- **状态**：已采纳（S0 定稿）
- **日期**：2026-10-14
- **关联**：README §6.1（Mnesia 边界）、roadmap §6.1；S4 控制面

## 背景（Context）

Mnesia 是 Erlang 的分布式 KV/表存储，适合低频、强一致的控制面数据，但不适合高吞吐消息正文与高频 ACK/cursor 更新。若把消息流灌入 Mnesia，会成为吞吐与延迟瓶颈。

## 决策（Decision）

Mnesia **仅**承载控制面元数据：`tenant` / `broker_user` / `acl_rule` / `topic_meta` / `queue_meta` / `binding_meta` / `subscription_meta` / `cluster_node` / `partition_meta` / `config_version` / `config_outbox`。**消息正文、每次 ACK、高频 cursor 更新不进 Mnesia**，进入数据面存储（自研 WAL / 内存，见 ADR-008）。

配置更新采用「版本号 + 事件通知 + 周期性对账」；需向外部发布变更时使用 Outbox 模式，避免双写不一致。

## 后果（Consequences）

- **+** 控制面可读性强、可重建、可分布式一致。
- **+** 数据面吞吐不受 Mnesia 限制（由 WAL/ra 承载）。
- **-** 数据面需独立的、经故障注入验证的存储实现（S0.3 / S4）。

## 验证（Validation）

S4 控制面设计（roadmap §6.1）与 S2 单机内核的 `broker_storage` 实现。
