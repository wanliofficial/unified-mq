# Progress — UnifiedMQ

> 多阶段路线（技术验证 → 原型 → 生产可用）参见 `docs/roadmap.md`。
> 当前进度：**S0（技术验证）全部完成并已推送 `origin/main`（最新 `b55fd89`）。**

## 环境基线

- Erlang/OTP：固定 **OTP 28.3.1**（`/home/wanli/Development/otp-28.3.1`，见 `.tool-versions` / `.envrc`）
- 构建：rebar3 3.27 umbrella 根（`rebar.config`），spikes 目录独立于 umbrella 之外
- 数据目录：运行时写入 `spikes/*/.data/`（已加入 `.gitignore`）

## 阶段状态

| 阶段 | 状态 | 说明 |
|---|---|---|
| S0 技术验证 | ✅ 完成 | 5 个 spike + Gate ADR-001~008 全部落盘并推送 |
| S1 MQTT 垂直切片 | ⬜ 未开始 | roadmap 已定义 Gate 与三个待冻结核心抽象 |
| S2/S3 | ⬜ 未开始 | roadmap 标注 S2/S3 可并行，S3 必须早于 S4 |
| S4+ | ⬜ 未开始 | — |

## S0 交付物（已提交）

| Spike | Commit | 关键结论 |
|---|---|---|
| S0.0 monorepo 骨架 + OTP 28 工具链 | `f0029b7` | rebar3 umbrella 根、`.tool-versions`、`.envrc` 版本守卫、`.gitignore` |
| S0.1 rabbitmq/ra 评估 | `cdf7826` | ra 3.2.0 编译+运行于 **OTP 28.3.1**（修正原"需锁 OTP 27"误判）；3 节点集群、Leader 被杀自动重选、失去多数派正确拒绝写入；~677 µs/写；记录 6 个 API 陷阱 |
| S0.2 Raft 分组模型压测 | `f5b40bd` | 每组固定开销**线性**：~3 进程 / 2 ETS / 128 KB / 5 ms 启动；单写者吞吐不随组数上升（被本地一次 fsync 锁死 ~1070 写/s）。决策：默认**每组 16 分区**、故障域=组大小、单节点组数 ≤256 |
| S0.3 WAL + Group Commit | `f0029b7` | fsync 871 µs/msg；group commit batch=64 达 ~19944 msg/s（17×）；恢复 20k 记录 143 ms；含 tmpfs 防护 |
| S0.4 协议客户端基线 | `7395806` | 6 种协议客户端**全部验证可获取+最小 API 可用**（含 Pulsar 官方 Java Client，README 5.1 硬性要求）；矩阵作为 S5 基线；种子 `docs/protocol-compatibility.md` |
| S0.5 CloudEvents 校验开销 | `f0029b7` | 扩展属性成本线性，建议默认上限 32 |
| Gate S0：ADR-001~008 定稿 | `b55fd89` | README 摘要升级为 `docs/adr/00N-*.md` 正式决策，每条挂 S0 spike 证据；`docs/adr/README.md` 兼作 Gate 验收清单 |

## Gate S0 验收结论

- ADR-001 协议适配层分离 / ADR-002 Mnesia 仅控制面 / ADR-003 采用 rabbitmq/ra 3.2.0
- ADR-004 CloudEvents 可选 / ADR-005 事件 id vs broker id / ADR-006 Pulsar 与存储解耦
- ADR-007 显式三级可靠性（os_buffer / fsync / replicated_commit）及丢失边界
- ADR-008 分组策略（默认 16 分区/组、故障域=组大小）+ 存储方案 A（自研分段 WAL + group commit）

## 过程中修正

1. S0.1 原报告称 ra 仅支持 OTP 27、需锁版本 —— 实测 OTP 28.3.1 完全可用，全项目统一 OTP 28.3.1。
2. `.gitignore` 修正：新增 `spikes/*/.data/` 忽略规则（S0.2 运行时数据目录此前未被忽略）。

## 下一步

进入 **S1（MQTT 垂直切片原型）**：读取 `docs/roadmap.md` 的 S1 Gate 与三个待冻结核心抽象，规划实现。
