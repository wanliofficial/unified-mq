# Progress — UnifiedMQ

> 多阶段路线（技术验证 → 原型 → 生产可用）参见 `docs/roadmap.md`。
> 当前进度：**S0 已完成并推送 `origin/main`；S1 进行中——ADR-009/010/011 已冻结，T1 骨架 + T3 `broker_router_hash` + T4 `broker_storage_memory` 已实现并实测通过（27 个 CT 用例）。**

## 环境基线

- Erlang/OTP：固定 **OTP 28.3.1**（`/home/wanli/Development/otp-28.3.1`，见 `.tool-versions` / `.envrc`）
- 构建：rebar3 3.27 umbrella 根（`rebar.config`），spikes 目录独立于 umbrella 之外
- 数据目录：运行时写入 `spikes/*/.data/`（已加入 `.gitignore`）

## 阶段状态

| 阶段 | 状态 | 说明 |
|---|---|---|
| S0 技术验证 | ✅ 完成 | 5 个 spike + Gate ADR-001~008 全部落盘并推送 |
| S1 MQTT 垂直切片 | 🟡 进行中 | 方案设计 v0.6（四轮评审 46 项整改 + T1/T3/T4 实现同步 11 项）；ADR 已冻结；**T1/T3/T4 完成**，下一步 T5（WAL，关键路径） |
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
| T1 工程骨架 | ✅ **完成（2026-10-21）** | `apps/` 八个 app（broker_core 纯类型 + 7 个带顶层 supervisor 的 app）、`config/sys.config.src`、`config/vm.args.src`、relx 接线；`rebar3 compile`（warnings_as_errors）通过，`rebar3 release` 节点实测 `ping=pong` 且 `application:get_env` 取到配置 |
| T2 公共类型核对 | ✅ 随 T1/T3/T4 消化 | `broker_message.hrl` 已含类型与错误码约定；两份 behavior 的错误项均为内联元组，无独立错误码模块——**T2 不再有独立工作项，已并入 T3/T4** |
| T3 `broker_router_hash` | ✅ **完成（2026-10-21）** | `apps/broker_router/src/broker_router_hash.erl` + `_store.erl` + `include/broker_router.hrl`；14 个 CT 用例通过，含"挂起写者进程后 `route/3` 仍返回正确结果"——实测证明读路径不经进程（§3.2 冻结点 2 / README §7.2） |
| T4 `broker_storage_memory` | ✅ **完成（2026-10-21）** | `apps/broker_storage/src/broker_storage_memory.erl` + `_store.erl`；13 个 CT 用例通过：recover 绝不建分区、超能力 sync_mode 报错且不消耗 offset、水位不可偏高、truncate 只设逻辑边界、20 路并发 append offset 唯一无洞。**用例已参数化**为 `apps/broker_storage/test/broker_storage_contract.hrl`（后端只供 `?BACKEND`/`?CAPABILITY` + 三个 hook），T5 的 WAL 套件按同一接口跑同一组断言 |
| T5~T17 实现 | ⬜ 未开始 | 关键路径剩余约 22 人日；T5（WAL 重写 + 多写者并发）为最长杆 |

### 第二协议门禁（roadmap §3.4 硬约束）

**Gate S1 未通过前，禁止实现第二个协议适配器。** 执行方式：本文件阶段状态表为唯一门禁状态源——S1 未标记 ✅ 时，任何新增 `apps/broker_proto_*`（`broker_proto_mqtt` 除外）的 PR 一律拒绝合入。

### S1 已定的关键决策（摘自冻结 ADR + 设计文档 v0.6）

- **ADR-009**：`partition_id` 为纯整数无语义；`reliability` 为 per-message；`headers`（公共/CloudEvents）与 `proto_meta`（协议私有）分工判据 = "换协议后该 key 是否仍有意义"；S1 悬空槽位 `expires_at` **恒 `undefined`**（不派生占位值，否则 S2 TTL 会静默丢弃已 ACK 消息）。
- **ADR-010**：Router 回调**不线程 `State`**，`route/3` 在调用者进程内直接查 ETS（非 `gen_server:call`）——否则全部 publish 串行化，违反 README §7.2；`SubRef` 由 `broker_subscription` 铸造，Router 仅作索引 key；索引不持久化，重启按固定顺序回填后才放开 listener。
- **ADR-011**：`create/2` 与 `recover/2` **分离**，recover 对缺失分区返回 `{not_found,_}` 且绝不自动创建（防"恢复出空分区并报告健康"的静默数据丢失）；`recover/2` 返回 `CommittedOffset` + `HighOffset`，**不设 `AckRanges`**；水位**只允许偏低**（偏低=重复，允许；偏高=提前 truncate 丢数据，绝对禁止）。
- WAL 磁盘协议独立于 `#broker_message{}`，带 `Vsn` 版本位（取代原 OQ-1）。
- Session 持久化分三层：`.subs` 强一致即改即 fsync；`.inflight` 与 `.cursor` 批量刷盘（避免逐条 fsync 归零 group commit 收益）。
- Gate「订阅位置不丢」措辞解释已复审确认，写入 ADR-011 附录 11.7。
- 多写者并发 group commit 为 S0.3 §6 遗留待办，列为 T5 前置交付项。
- **T3/T4 实测暴露 6 项（H6~H11，详见设计文档 §13）**：`route_ctx` 原本冻结在 behavior 模块内，record 不跨模块可见 ⇒ 任何调用者都无法构造 Ctx，已搬进 `include/broker_router.hrl`（字段未变，H6）；"Router 返回列表去重"里同 client 重叠订阅的合并做不到（必须解引用 SubRef，§10.2 禁止），归 T7 订阅层（H7）；**§6 boot step 1 没有 `create/2` 的调用者，干净数据目录必然启动失败**——T5 必须先定分区 bootstrap（H8）；offset 基数定为从 1 开始、空分区 `recover` 返回 `0/1`（H9）；`advance_committed`/`truncate` 的方向守卫在冻结签名内落成可机械检查（H10）；**hash 分区 + 通配符订阅 ⇒ 一个订阅跨多分区**（实测 `sport/#` 命中分区 `[0,1,3]`），故 §5.3 的 per-sub 单标量 `.cursor` 不成立，T7 须改为 per `(SubRef, PartitionId)`（H11）。
- **T3/T4 的 S1 已知限制（非缺陷）**：`route/3` 不使用 `Ctx`、`register/4` 不入库 `SubOpts`（security 为单用户桩，多租户 filter 隔离留 S2）；内存后端数据面走 `public` ETS，并发读可能看见"offset 已分配、消息未插入"的瞬时空洞而提前截断一页——真实链路 §5.1(d) 按分区串行化 append 不会触发，但**同一场景是 T5 并发测试必须给 WAL 明确结论的项**。
- **T1 实测修正监督树结构（H1）**：不引入单一 `broker_app_sup`「application master」，改为每 app 一个顶层 supervisor，跨 app 启动顺序由 `.app.src` 的 `applications` 依赖拓扑保证（实测 `sasl → core → storage → router → queue → subscription → event → security → proto_mqtt`）。relx 必须用 `{sys_config,...}`/`{vm_args,...}` 而非 `*_src` 变体，否则 `.src` 原名进 `releases/Vsn/`、启动脚本找不到配置而挂死（H2）。

## 下一步

**T5 `broker_storage_wal` 重写**（关键路径 6.5 人日）：含 S0.3 §6 遗留的多写者并发 group commit 正确性测试，并须先回答 H8（分区 bootstrap 由谁调 `create/2`，不得退化成"recover 顺手创建"）；WAL 后端必须通过 T4 那组水位/truncate 守卫用例——用例已参数化，T5 只需新建 `broker_storage_wal_SUITE` 提供 `?BACKEND`/`?CAPABILITY = [os_buffer, fsync]` 与 `backend_setup/1`/`backend_cleanup/1`/`backend_config/1`（tmp 目录生命周期）再 include 契约头，13 个断言一条都不许改或跳过。T6 分区 Worker 依赖 T3+T4（均已就绪）；T7 须先定 H11 的 per-`(SubRef, PartitionId)` cursor 形态。T17 评审会不再起草 ADR，改为复核"T2~T7 实现 ↔ 冻结 API"一致性；ADR-011 附录 11.7 的 Gate 措辞在会上正式签认。
