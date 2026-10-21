# Progress — UnifiedMQ

> 多阶段路线（技术验证 → 原型 → 生产可用）参见 `docs/roadmap.md`。
> 当前进度：**S0 已完成并推送 `origin/main`；S1 进行中——方案设计 v0.4，T0.5 三份 ADR（009/010/011）已评审冻结，待启动 T1。**

## 环境基线

- Erlang/OTP：固定 **OTP 28.3.1**（`/home/wanli/Development/otp-28.3.1`，见 `.tool-versions` / `.envrc`）
- 构建：rebar3 3.27 umbrella 根（`rebar.config`），spikes 目录独立于 umbrella 之外
- 数据目录：运行时写入 `spikes/*/.data/`（已加入 `.gitignore`）

## 阶段状态

| 阶段 | 状态 | 说明 |
|---|---|---|
| S0 技术验证 | ✅ 完成 | 5 个 spike + Gate ADR-001~008 全部落盘并推送 |
| S1 MQTT 垂直切片 | 🟡 进行中 | 方案设计 v0.4 已定（四轮评审 46 项整改）；**T0.5 ADR-009/010/011 已冻结（状态=已采纳）** → 下一步 T1 实现 |
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

## S1 进展（进行中）

| 步骤 | 状态 | 产物 |
|---|---|---|
| 方案设计 | ✅ v0.4 | `docs/design/s1-mqtt-vertical-slice.md`（四轮评审共 46 项整改：B1~4 / C1~6 / S1~6 / D1~13 / E1~12 / F1~6+G1）；§3 已降级为 ADR 摘录 |
| T0.5 ADR 冻结 | ✅ **已冻结（2026-10-21，状态=已采纳）** | `docs/adr/009-broker-message-record.md`、`010-broker-router-behavior.md`、`011-broker-storage-behavior.md`；三份 API 定义已在 OTP 28.3.1 `erlc` 实测编译通过 |
| T1~T17 实现 | ⬜ 未开始 | 关键路径约 27 人日；T17 评审会转为"实现 ↔ 冻结 ADR 一致性复核" |

### 第二协议门禁（roadmap §3.4 硬约束）

**Gate S1 未通过前，禁止实现第二个协议适配器。** 执行方式：本文件阶段状态表为唯一门禁状态源——S1 未标记 ✅ 时，任何新增 `apps/broker_proto_*`（`broker_proto_mqtt` 除外）的 PR 一律拒绝合入。

### S1 已定的关键决策（摘自冻结 ADR + 设计文档 v0.4）

- **ADR-009**：`partition_id` 为纯整数无语义；`reliability` 为 per-message；`headers`（公共/CloudEvents）与 `proto_meta`（协议私有）分工判据 = "换协议后该 key 是否仍有意义"；S1 悬空槽位 `expires_at` **恒 `undefined`**（不派生占位值，否则 S2 TTL 会静默丢弃已 ACK 消息）。
- **ADR-010**：Router 回调**不线程 `State`**，`route/3` 在调用者进程内直接查 ETS（非 `gen_server:call`）——否则全部 publish 串行化，违反 README §7.2；`SubRef` 由 `broker_subscription` 铸造，Router 仅作索引 key；索引不持久化，重启按固定顺序回填后才放开 listener。
- **ADR-011**：`create/2` 与 `recover/2` **分离**，recover 对缺失分区返回 `{not_found,_}` 且绝不自动创建（防"恢复出空分区并报告健康"的静默数据丢失）；`recover/2` 返回 `CommittedOffset` + `HighOffset`，**不设 `AckRanges`**；水位**只允许偏低**（偏低=重复，允许；偏高=提前 truncate 丢数据，绝对禁止）。
- WAL 磁盘协议独立于 `#broker_message{}`，带 `Vsn` 版本位（取代原 OQ-1）。
- Session 持久化分三层：`.subs` 强一致即改即 fsync；`.inflight` 与 `.cursor` 批量刷盘（避免逐条 fsync 归零 group commit 收益）。
- Gate「订阅位置不丢」措辞解释已复审确认，写入 ADR-011 附录 11.7。
- 多写者并发 group commit 为 S0.3 §6 遗留待办，列为 T5 前置交付项。

## 下一步

ADR-009/010/011 已冻结 → 启动 **T1（rebar3 umbrella apps 骨架 + sys.config + vm.args + application master）**，T2~T5 按依赖图并行推进。T17 评审会不再起草 ADR，改为复核"T2~T7 实现 ↔ 冻结 API"一致性；ADR-011 附录 11.7 的 Gate 措辞在会上正式签认。
