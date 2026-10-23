# Progress — UnifiedMQ

> 多阶段路线（技术验证 → 原型 → 生产可用）参见 `docs/roadmap.md`。
> 当前进度：**S0 已完成并推送 `origin/main`；S1 进行中——ADR-009/010/011 已冻结，T1 骨架 + T3 `broker_router_hash` + T4 `broker_storage_memory` + T5 `broker_storage_wal` + T6 `broker_queue` 分区 Worker + T7 `broker_subscription` 订阅层已实现并实测通过（181 个 CT 用例，`rebar3 ct` 零 FAILED 行）。下一步 T9 `broker_proto_mqtt` 连接 FSM。**

## 环境基线

- Erlang/OTP：固定 **OTP 28.3.1**（`/home/wanli/Development/otp-28.3.1`，见 `.tool-versions` / `.envrc`）
- 构建：rebar3 3.27 umbrella 根（`rebar.config`），spikes 目录独立于 umbrella 之外
- 数据目录：运行时写入 `spikes/*/.data/`（已加入 `.gitignore`）

## 阶段状态

| 阶段 | 状态 | 说明 |
|---|---|---|
| S0 技术验证 | ✅ 完成 | 5 个 spike + Gate ADR-001~008 全部落盘并推送 |
| S1 MQTT 垂直切片 | 🟡 进行中 | 方案设计 v0.9（四轮评审 46 项整改 + T1/T3/T4/T5/T6/T7 实现同步 48 项）；ADR 已冻结；**T1/T3/T4/T5/T6/T7 完成**（181 个 CT 用例，零 FAILED 行；release 实测含 T7 的 R1b 节点级预演），下一步 T9（`broker_proto_mqtt` 连接 FSM + pull 投递路径 + packet_id + capability 缓存） |
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
| 方案设计 | ✅ v0.9 | `docs/design/s1-mqtt-vertical-slice.md`（四轮评审共 46 项整改：B1~4 / C1~6 / S1~6 / D1~13 / E1~12 / F1~6+G1；实现期同步 48 项：H1~H11 / I1~I15 / J1~J11 / K0~K10）；§3 已降级为 ADR 摘录 |
| T0.5 ADR 冻结 | ✅ **已冻结（2026-10-21，状态=已采纳）** | `docs/adr/009-broker-message-record.md`、`010-broker-router-behavior.md`、`011-broker-storage-behavior.md`；三份 API 定义已在 OTP 28.3.1 `erlc` 实测编译通过 |
| T1 工程骨架 | ✅ **完成（2026-10-21）** | `apps/` 八个 app（broker_core 纯类型 + 7 个带顶层 supervisor 的 app）、`config/sys.config.src`、`config/vm.args.src`、relx 接线；`rebar3 compile`（warnings_as_errors）通过，`rebar3 release` 节点实测 `ping=pong` 且 `application:get_env` 取到配置 |
| T2 公共类型核对 | ✅ 随 T1/T3/T4 消化 | `broker_message.hrl` 已含类型与错误码约定；两份 behavior 的错误项均为内联元组，无独立错误码模块——**T2 不再有独立工作项，已并入 T3/T4** |
| T3 `broker_router_hash` | ✅ **完成（2026-10-21）** | `apps/broker_router/src/broker_router_hash.erl` + `_store.erl` + `include/broker_router.hrl`；14 个 CT 用例通过，含"挂起写者进程后 `route/3` 仍返回正确结果"——实测证明读路径不经进程（§3.2 冻结点 2 / README §7.2） |
| T4 `broker_storage_memory` | ✅ **完成（2026-10-21）** | `apps/broker_storage/src/broker_storage_memory.erl` + `_store.erl`；13 个 CT 用例通过：recover 绝不建分区、超能力 sync_mode 报错且不消耗 offset、水位不可偏高、truncate 只设逻辑边界、20 路并发 append offset 唯一无洞。**用例已参数化**为 `apps/broker_storage/test/broker_storage_contract.hrl`（后端只供 `?BACKEND`/`?CAPABILITY` + 三个 hook），T5 的 WAL 套件按同一接口跑同一组断言 |
| T5 `broker_storage_wal` | ✅ **完成（2026-10-21）** | 七个模块 `wal_codec`/`wal_segment`/`wal_index`/`wal_meta`/`wal_recovery`/`wal_writer`/`wal_group_committer` + 适配层 `broker_storage_wal.erl` + `broker_storage_wal_sup.erl`（动态 `start_child`，`one_for_one`/`permanent`）。**113 个 CT 用例通过**（后端 19 = 13 契约 + 6 专有；内部 94）：13 个契约断言在 `?CAPABILITY = [os_buffer, fsync]` 下**一条未改未跳**，Gate「WAL 与内存实现可互换」由机械检查证明；**E5/S0.3 §6 遗留的多写者并发已实测**（16 caller 混合 sync_mode → offset 恰为 `1..32`、无 cross-reply、回复严格在 fsync 后）；`kill -9` writer 后 recover 日志完好；跨段 `read_from` 覆盖；tmpfs 数据目录被 `create/2` 拒绝且不建目录。`rebar3 ct` 全套 **141 例通过**，`dialyzer` 干净，`xref` 仅剩预期的 unused-export |
| T6 `broker_queue` 分区 Worker | ✅ **完成（2026-10-22）** | `apps/broker_queue/src/broker_queue.erl`（公共 API + H8 的 `create_partition/1`）+ `broker_queue_store.erl`（`protected` 注册表，单写者）+ `broker_queue_partition.erl`（每分区 worker：recover/句柄刷新/订阅者 monitor/广播）+ `broker_queue_sup.erl`（静态子进程清单，boot step 1 的逐分区 recover 循环）；16 个 CT 用例，全套 **157/157** 零 FAILED 行、`dialyzer` 干净、`xref` 仅剩预期 unused-export。**release 实测两半**：干净数据目录 → 退出码 1 + `{partition_not_found,0}` + **不创建任何目录**；带外建 4 分区后 → 11 app 全起、4 worker + 4 writer、`capability(0)=[os_buffer,fsync]`。**关键偏差 J1**：append **不经** worker，串行化点是单一 `wal_writer`（否则 group commit 退化成每批 1 条 ≈1148 msg/s）——实测 8 caller 停在 writer mailbox 后放行得 `#{records:=8, batches:=1, syncs:=1}` |
| T7 `broker_subscription` 订阅层 | ✅ **完成（2026-10-23）** | `broker_subscription.erl`（facade：写经 worker、读在调用者进程）+ `broker_subscription_worker.erl`（单写者 gen_server，boot step 2+3 全有或全无 + 四张 protected ETS + 刷盘/聚合 tick）+ `broker_subscription_files.erl`（`.subs`/`.cursors`/`.inflight` temp+fsync+rename）；**并修掉 T3 的 Router 装配缺口（K0）**：`broker_router_sup` 起 `broker_router_hash_store` + `current_env/0` + `broker_router:env/0`/`register/3`/`unregister/1`。**24 个新 CT 用例**（订阅 21 + router 监督树 3），全套 **181/181** 零 FAILED 行、`dialyzer` 干净、`xref` 仅剩预期 unused-export（T9/T12 预留 API）。关键落地：K2 SubRef=`{ClientId,Filter}` 确定性铸造、K5 订阅原点内嵌 `.subs`（恢复绝不兜底）、K6 committed = min(活 cursor)−1 且**先刷盘后 advance**、K8 写序"宁可索引多不可索引缺"、K9 QoS0 推送/QoS1 冗余唤醒 + H7 合并、K10 detach pid 守卫。**release 实测 R1b 节点级预演**（ext4）：带外 4 分区 → 11 app 全起 → subscribe 落 `data/sessions/YzE.subs` → bounce app → route 仍含 SubRef（`already_registered` 回填路径）+ cursor 从盘恢复 |
| T8~T17 实现 | ⬜ 未开始 | 关键路径剩余约 8.5 人日（T9 3 + T12 1 + T13 2 + T14 2 + T17 0.5）；下一杆 T9（`broker_proto_mqtt` 连接 FSM：pull 投递路径 + `inflight`/ack 记账 + DUP=1 + packet_id），T12 须顺带修 J8（release 树外绝对 `data_dir`） |

### 第二协议门禁（roadmap §3.4 硬约束）

**Gate S1 未通过前，禁止实现第二个协议适配器。** 执行方式：本文件阶段状态表为唯一门禁状态源——S1 未标记 ✅ 时，任何新增 `apps/broker_proto_*`（`broker_proto_mqtt` 除外）的 PR 一律拒绝合入。

### S1 已定的关键决策（摘自冻结 ADR + 设计文档 v0.8）

- **ADR-009**：`partition_id` 为纯整数无语义；`reliability` 为 per-message；`headers`（公共/CloudEvents）与 `proto_meta`（协议私有）分工判据 = "换协议后该 key 是否仍有意义"；S1 悬空槽位 `expires_at` **恒 `undefined`**（不派生占位值，否则 S2 TTL 会静默丢弃已 ACK 消息）。
- **ADR-010**：Router 回调**不线程 `State`**，`route/3` 在调用者进程内直接查 ETS（非 `gen_server:call`）——否则全部 publish 串行化，违反 README §7.2；`SubRef` 由 `broker_subscription` 铸造，Router 仅作索引 key；索引不持久化，重启按固定顺序回填后才放开 listener。
- **ADR-011**：`create/2` 与 `recover/2` **分离**，recover 对缺失分区返回 `{not_found,_}` 且绝不自动创建（防"恢复出空分区并报告健康"的静默数据丢失）；`recover/2` 返回 `CommittedOffset` + `HighOffset`，**不设 `AckRanges`**；水位**只允许偏低**（偏低=重复，允许；偏高=提前 truncate 丢数据，绝对禁止）。
- WAL 磁盘协议独立于 `#broker_message{}`，带 `Vsn` 版本位（取代原 OQ-1）。
- Session 持久化分三层：`.subs` 强一致即改即 fsync；`.inflight` 与 `.cursor` 批量刷盘（避免逐条 fsync 归零 group commit 收益）。
- Gate「订阅位置不丢」措辞解释已复审确认，写入 ADR-011 附录 11.7。
- 多写者并发 group commit 为 S0.3 §6 遗留待办，列为 T5 前置交付项——**T5 已实测消除**：16 caller 混合 `fsync`/`os_buffer` 并发下 offset 恰为 `1..32`、无 cross-reply、每个回复严格在本批 fsync 之后；未发现需要上调 `batch_delay_ms` 的延迟退化，故 §7.2 默认值不动。
- **T3/T4 实测暴露 6 项（H6~H11，详见设计文档 §13）**：`route_ctx` 原本冻结在 behavior 模块内，record 不跨模块可见 ⇒ 任何调用者都无法构造 Ctx，已搬进 `include/broker_router.hrl`（字段未变，H6）；"Router 返回列表去重"里同 client 重叠订阅的合并做不到（必须解引用 SubRef，§10.2 禁止），归 T7 订阅层（H7）；**§6 boot step 1 没有 `create/2` 的调用者，干净数据目录必然启动失败**——T5 必须先定分区 bootstrap（H8）；offset 基数定为从 1 开始、空分区 `recover` 返回 `0/1`（H9）；`advance_committed`/`truncate` 的方向守卫在冻结签名内落成可机械检查（H10）；**hash 分区 + 通配符订阅 ⇒ 一个订阅跨多分区**（实测 `sport/#` 命中分区 `[0,1,3]`），故 §5.3 的 per-sub 单标量 `.cursor` 不成立，T7 须改为 per `(SubRef, PartitionId)`（H11）。
- **T3/T4 的 S1 已知限制（非缺陷）**：`route/3` 不使用 `Ctx`、`register/4` 不入库 `SubOpts`（security 为单用户桩，多租户 filter 隔离留 S2）；内存后端数据面走 `public` ETS，并发读可能看见"offset 已分配、消息未插入"的瞬时空洞而提前截断一页——真实链路 §5.1(d) 按分区串行化 append 不会触发，但**同一场景是 T5 并发测试必须给 WAL 明确结论的项**。**→ T5 已给结论：WAL 后端不存在该窗口**——`HighOffset` 刻意不进读路径索引，只由 writer 进程状态持有，`read_from/3` 从段文件实际扫到什么就是什么（读到 torn tail 与读到日志末尾不可区分，也不需要区分）。内存后端的空洞源于把"下一个 offset"暴露给了读路径，WAL 侧结构性避开。
- **T1 实测修正监督树结构（H1）**：不引入单一 `broker_app_sup`「application master」，改为每 app 一个顶层 supervisor，跨 app 启动顺序由 `.app.src` 的 `applications` 依赖拓扑保证（实测 `sasl → core → storage → router → queue → subscription → event → security → proto_mqtt`）。relx 必须用 `{sys_config,...}`/`{vm_args,...}` 而非 `*_src` 变体，否则 `.src` 原名进 `releases/Vsn/`、启动脚本找不到配置而挂死（H2）。
- **T5 实测暴露 15 项（I1~I15，详见设计文档 §13），冻结 API 全程未改**。决策性的几条：
  - **H8 已答复**：`create/2` 的唯一入口是带外管理动作 `broker_queue:create_partition/1`（**T6 交付**）；boot step 1 的逐分区 `recover/2` 循环归 `broker_queue_sup`（分区清单 `{partitions, N}` 在它的 env 里，存储层看不到）。原候选方案里的 `partition_bootstrap` 配置与"数据目录首启标记"**均被否决**——两者都让启动路径在目录缺失时自建分区，等价于"recover 顺手创建"的带开关版本。干净数据目录 boot 必失败是 §11.2 要求的**响亮失败**。
  - **模块划分偏离计划**：无 `wal_reader`（读路径 = `broker_storage_wal:read_from/3` 在调用者进程内 `wal_index:locate/2` + `wal_segment:fold/5`）；新增 `wal_meta`（否则 `recover/2` 取 `CommittedOffset` 的代价与日志长度成正比）；`wal_group_committer` 是**纯策略模块**而非 gen_server（批次所有权必须与 offset 分配/段轮转/水位推进同进程，否则 fsync 与 offset 分配之间多一处竞态）。
  - **tmpfs 防护**从 `init/1` 提到 `start_link/3`（放 `init/1` 会让 `create/2` 先起一个注定自杀的子进程，白烧 supervisor intensity 预算）；判据从 S0.3 的"fsync < 100 µs 计时"改为读 `/proc/mounts` **按 fstype** 匹配 `tmpfs`/`ramfs`（计时法在快 SSD 上会把正常盘误判为易失，代价是节点起不来）。
  - **两个真实缺陷**：(a) T4 的 `broker_storage_memory_store` 从未被任何角色启动，装配后的 release 里每次 `broker_storage_memory:create/2` 都是 `noproc`——CT 全绿是因为套件在自己进程里手工建表；现 `broker_storage_sup` 按 `backend` 只起自己需要的那个子进程，并由 `broker_storage_sup_SUITE` 钉住。(b) v0.6 契约头的 tripwire 写成 `[os_buffer | unsupported_modes()]`，隐含"后端只声明 os_buffer"，于是**诚实声明 `fsync` 的 WAL 后端被自己的 tripwire 判失败**；已改为从 `all_sync_modes/0` 派生。
  - **写/fsync 失败致命不重试**（offset 先分配后落盘，失败后继续服务=把已承诺 offset 变成永久空洞，§11.4 禁止静默降级）；读到**完整但不可解码**的记录必须报错而非返回短页（否则订阅者永远等一个确实存在的 offset）。
  - **句柄随 writer 崩溃失效**（索引 ETS 与 writer 同生共死，此后 `read_from/3` 抛 `badarg`）——**T6 的分区 worker 不得跨 writer 重启缓存句柄/pid**，重启后必须重新 `recover/2`。**→ 已由 T6 消化（J7）**：捕获后向 worker 发 `reattach` 重取句柄，返回 `{error,{stale_handle,...}}` 交 caller 决定，**不自动重试**（已抵达 writer 的 append 可能已消耗 offset，静默重试会制造发布者不知情的重复消息）；实测 `kill -9` writer 后第二次 append 得 `{ok,4}`、整页 `[1,2,3,4]` 完好。
- **T6 实测暴露 11 项（J1~J11，详见设计文档 §13），冻结 API 全程未改**。决策性的几条：
  - **J1：§5.1(d) 的"分区 Worker 串行化"读法已修正**——真正的串行化点是**单一的 `wal_writer` 进程**，`broker_queue:append/3` 在**调用者进程内**查 `protected` ETS 注册表拿句柄后直调后端，worker 只管生命周期/元数据/广播，**不在数据路径上**（与 §3.2 冻结点 2 同构）。若照字面做成对 worker 的 `gen_server:call`，每批恒为 1 条 ⇒ 吞吐退回 S0.3 §2.1 的逐条 fsync ~1148 msg/s，正是 ADR-008 自写 WAL 要超越的数字。实测：`sys:suspend(worker)` 后 append 照常返回；8 caller 停进 writer mailbox 再放行 ⇒ `batches=1, syncs=1`。
  - **J3：OTP 28 的 `application:get_env/2` 返回 `{ok, Val}` 而非裸值**（`kernel-10.5/src/application.erl:918` spec + 探针实测），全项目统一改用返回裸值的 `get_env/3`；`config/sys.config.src` 顶部"read with get_env/2"的注释同步修正。仓库级 grep 确认 2 元形式只有 T6 新写的那一处。
  - **J2：`sys.config.src` 的 `{wal, proplist}` 没有任何消费者能读**（`wal_writer`/`wal_group_committer` 全用 `maps:get`/`is_map`，第一条 append 就 `badarg`）。T5 没暴露是因为 CT 自己构造 Config，**T6 是第一个把 application env 直接当 Config 交给后端的消费者**。已把 env 改成 map，**不加翻译层**——翻译层会让"配置写的 key"与"代码读的 key"再次分叉。
  - **J8/J9：带外建分区的两个实测前提**——relx 生成的 `bin/unified_mq` 在 exec 前 `cd "$ROOTDIR"`（脚本第 770 行），故相对 `data_dir` 落在 **release 根目录**（`rebar3 release` 会连带清掉它），带外建分区必须与节点同 CWD；且 `create_partition/1` 需要 `broker_queue` 已 **load**（不必 start），否则 `partition_ids/0` 抛 `{invalid_partition_count, undefined}`。release 无维护命令，操作路径为裸 `erl -pa <rel>/lib/*/ebin -config <rel>/releases/Vsn/sys.config` + `application:load/1`。**T12 须改用 release 树外的绝对 `data_dir`**。
  - **J10：T5 遗留的真实缺陷（被 CT 汇总数字掩盖）**——`broker_storage_wal_SUITE` 有 `init_per_suite/1` 却无 `end_per_suite/1`；CT 只要二者之一被导出就会调 `end_per_suite`（`test_server_ctrl:do_add_init_and_end_per_suite/4` 对两者都做 `function_exported` 检查），于是每轮全套都打一条 `end_per_suite: FAILED {undef,...}`，**但不计入总数**，"All 141 tests passed" 字面为真。已补齐并顺带 `application:stop`，避免把运行中的 storage app 泄漏给同节点后续套件。**验收规则修正：看 `FAILED` 明细行，不能只看汇总数字。**
  - **J6：`{new_messages,_,_,_}` 是 cast = 唤醒信号，不是投递契约**（可乱序/重叠/丢失），dispatcher 必须自己按 cursor `read_from`，cursor 才是唯一真相源——T7 的 dispatcher 按此实现。
  - **J4/J5**：注册表用 `protected` + 单写者（`public` 会让任何进程把某分区指向别人的日志，对齐 ADR-010【F3】）；worker `terminate/2` **保留**注册表条目——擦掉会报 `{unknown_partition}`，那是比"句柄过期但能自愈"更严重的谎。
  - **J11**：本机 `/tmp` 是 tmpfs，持久化守卫（I4/I5）在此拒绝建分区——守卫按设计工作，但 release 级验证必须在 ext4 上跑（`/`、`/home` 均为 ext4）。
- **T7 实测暴露 11 项（K0~K10，详见设计文档 §13），冻结 API 全程未改**。决策性的几条：
  - **K0（真实缺陷，I12 同类）**：`broker_router_sup:init/1` 返回空 children——T3 的索引 store 只在 CT 套件自己进程里手工启动，**装配后的节点里没有路由索引进程**，boot step 3 的 `register/4` 无处可写。现 sup 按 `backend` 起 store，并补 `broker_router_hash:current_env/0`（装配期访问器，不启动进程）与 `broker_router:env/0`/`register/3`/`unregister/1` facade；`broker_router_sup_SUITE` 3 例钉住。
  - **K5+K6（水位方向落公式）**：订阅原点内嵌 `.subs` 并即时 fsync，恢复规则"有 `.cursors` 用其值、否则用内嵌原点，**绝不当场用 Committed/0/High 兜底**"；聚合公式 **committed = min(活 cursor) − 1**，且**先 `.cursors` 落盘、后 `advance_committed`**——乱序会让崩溃后的 cursor 落在 committed 之下，S2 truncate 恰好丢"用户还没 ack"的段，即 §11.3 禁的"偏高"换了个入口。两条都有用例锁死（水位与文件内容同刻观察）。
  - **K8（写序统一）**：subscribe 先 `register` 后写盘（失败回滚 unregister）、unsubscribe 先写盘后 unregister——崩溃窗口里索引**只多不缺**，多的由 dispatch 解引用自己的实体表消化、由 boot 的 `already_registered`=ok 消化。
  - **K9**：§5.1(f) 的"推送"重释为 QoS0 真推送（消息不进存储，无可拉）+ QoS1 冗余唤醒（按 J6 合法）；H7 合并（同 client 重叠 filter 恰一次、取 max QoS）在订阅层做；in-flight 记账归 T9 的 pull 路径。
  - **实现期 OTP 观察**（详见 §13 批注）：`binary_to_term/2` 第二参是**选项列表**，裸 `safe` 抛 badarg 并被 catch-all 伪装成 `corrupt_session`（只在 release 重启里现形）；`application:start/1` 返回裸 `ok`、失败 start 后控制器有 `already_started` settle 窗口；OTP 28 运行节点经 `rpc:call` 远程 apply `code`/`erlang` 系函数得 `undef`——release 探针要写成"对已加载 app 模块的多次 rpc + 数据往返"。

## 下一步

**T9 `broker_proto_mqtt` 连接 FSM**（关键路径 3 人日，依赖 T6/T7 均已就绪）：交付 §9 T9 行内容——连接 FSM + capability 缓存 + keepalive + packet_id 分配 + `mqtt_max_qos` 执行 + **pull 投递循环**（按 `broker_subscription:cursor/2` 逐分区 `read_from`，J6：唤醒信号不可靠，cursor 是唯一真相；K9：QoS0 收推送、QoS1 收冗余唤醒）、投递经 `attach/3` 后的 `{broker_deliver, ...}` 形状与 `session/1` 恢复视图，in-flight 记账在 pull 路径做并写 `inflight/2`（DUP=1 语义归此层）。

T7 交接给 T9 的既定事实（实现期已定，勿再讨论）：SubRef = `{ClientId, Filter}`（K2）；投递形状 `{broker_deliver, ClientId, MaxQoS, [SubRef], {PartitionId, Offset, Msg}}`（K9）；`.inflight` 回退窗口"只多不少"是 R7 契约（MQTT §4.1 允许重发未确认）；dispatcher **内嵌在 conn_fsm**，S1 无独立 dispatcher 进程（§5.2）。

遗留提醒：**T12 须顺带修 J8**（`data_dir` 改 release 树外绝对路径）；tmpfs 守卫未搬到订阅文件层——`.subs` 的"零容忍"只在真实卷上成立，T12/T14 用 docker 卷时记入 S1 已知限制；目录级 fsync（掉电）按 §8.3 统一留 S2；T17 评审会不再起草 ADR，改为复核"T2~T7 实现 ↔ 冻结 API"一致性（ADR-011 附录 11.7 的 Gate 措辞会上正式签认）。
