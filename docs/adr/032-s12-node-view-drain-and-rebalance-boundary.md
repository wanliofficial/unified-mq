# ADR-032：S12 节点视图真相、drain 与再平衡边界——ra 为唯一真相、外部 LB 供给契约、不迁移活进程

- **状态**：已采纳（S12-0 收敛评审签认，2026-11-17）
- **日期**：2026-11-17
- **关联**：ADR-001（协议 Adapter 与消息内核分离）、ADR-002（Mnesia 只承担控制面元数据）、ADR-007（显式可靠性三档）、ADR-008（分区 Raft 分组模型、G=16）、ADR-010（`broker_router` 行为，`route/3` 为纯 `phash2`）、ADR-011（`broker_storage` 行为）、ADR-013（订阅类型与 cursor 归属）、ADR-015（重投递 / DLQ / TTL）、ADR-016（可观测最小集，冻结键集）、ADR-019（Shared 原子 claim 与会话布局）、ADR-021（集群控制面、Metadata Raft 与 Fencing）
- **关联文档**：[`docs/design/s12-0-convergence-report.md`](../design/s12-0-convergence-report.md)（OQ-1~OQ-10 逐条裁决与事实底座）、[`docs/design/s12-infrastructure-ha-and-lb.md`](../design/s12-infrastructure-ha-and-lb.md)（§4 需求 R-*、§5.4 OQ 原文）、四份单仓深挖稿 [`s12-infra-tcpbalance.md`](../design/s12-infra-tcpbalance.md) / [`s12-infra-gen-lb.md`](../design/s12-infra-gen-lb.md) / [`s12-infra-gen-server-cluster.md`](../design/s12-infra-gen-server-cluster.md) / [`s12-infra-nkdist.md`](../design/s12-infra-nkdist.md)（§2.5 的 F- 条目 = 能力矩阵裁剪项）、[`docs/roadmap.md`](../roadmap.md) §14
- **事实来源**：仓库内代码与配置逐行核对（`apps/broker_cluster/**`、`apps/broker_meta/**`、`apps/broker_admin/**`、`apps/broker_metrics/**`、`apps/broker_proto_*/**`、`config/sys.config.src`），行号见 §背景 与收敛报告 §1；不引用任何上游 README 的性能/规模自述

## 背景（Context）

S12 要补三件事：基础设施高可用（G1）、TCP 集群（G2）、负载均衡（G3）。roadmap §14 立了三条硬边界（不引入第二套共识 / 成员真相；不把 Broker 做成四层 TCP 代理；不把最终一致当强一致 HA），但十个架构问题（OQ-1~OQ-10）未决，无法开工。S12-0 的职责是把这十问收敛成可冻结的裁决。

收敛过程中查出五条与 S12 需求文档假设冲突的事实，它们直接决定了本 ADR 的形状：

1. **`broker_meta:reconcile/0` 不存在。** 真实存在的是 `broker_meta_config` 按 `reconcile_ms`（默认 5000）轮询 `config_version`（`broker_meta_config.erl:50,112-120`），它只对账配置版本计数，不碰成员、不碰归属。需求 R-CL3 写「复用 ADR-021 D1 的 `reconcile/0`」是**基于不存在的 API**。
2. **两份平行元数据互不相通。** ra 状态机（`broker_cluster_meta_machine.erl:17-27`）与本地缓存 `broker_cluster_cache`（`cache.erl:58-62`，doc `:1-9`「Reads never go to ra」）是归属真相；Mnesia 的 `partition_meta` / `cluster_node`（`broker_meta.erl:44-45`）是另一份形状重叠的表，**运行时 0 互相引用**（`grep -r broker_meta apps/broker_cluster/` 无匹配），`put_partition/2` / `put_node/2` **无生产调用者**，`cluster_node.status` 被硬编码为 `up`（`broker_meta.erl:215-216`）、无状态迁移、无消费者。这是 R-N6「不得新增第二套控制面元数据」的现成违反。
3. **`/readyz` 查的是「app 已 loaded」而非「已 started」。** `started/1` 用 `application:get_application(App)` 传**原子**（`broker_admin_http.erl:131-140`），该形态等于查 `ac_tab` 的 `{loaded, App}`。实测：`application:load(broker_queue)` 后 `get_application(broker_queue) = {ok, broker_queue}`，而 `application:which_applications()` 不含它。**app 崩溃但未 unload 时 `/readyz` 仍回 200。** R-HA2「`/readyz` 必须与 drain 状态一致」的前提因此不成立。
4. **跨节点投递完全不存在。** `apps/*/src` 中 `pg` / `pg2` / `global` / `monitor_nodes` 零处（`net_kernel` 仅出现在 `broker_cluster_dist_SUITE` 测试）；投递一律本地 `Pid !`（`broker_subscription.erl:222-233`）；副本写入刻意不通知本地订阅者（`broker_cluster_rep.erl:8-10`）。**A 节点订阅者收不到 B 节点 publish。** S12 需求文档的 K1~K8 未列这一项。
5. **无 drain、无节点级容量闸门、无节点视图。** `apps/` 中 `prep_stop` 零处；唯一容量闸门是 ranch per-listener `max_connections`（默认 1024，超限 acceptor 睡下、TCP 侧无显式拒绝，`ranch_conns_sup.erl:269-275`），唯一显式拒绝在 QUIC（`{error, too_many_connections}`，`mqtt_quic_listener.erl:210-213`）；`weight` / `drain` / `capacity` / `nodeup` / `nodedown` 在 `broker_cluster` 与 `broker_meta` 中零处。

本 ADR 只冻结**边界与形状**，不冻结实现细节（不写代码、不开端口、不新增 app、不新增依赖）。

## 决策

### D1. 节点视图的唯一真相 = `broker_cluster` 的 ra 状态机 + 本地 ETS 镜像；Mnesia `cluster_node` / `partition_meta` 退役为遗留视图（回答 OQ-2、R-CL1/R-CL3）

- **集群事实（成员、分区归属、`(owner, epoch, replicas, state, index)`）的唯一真相**是 ra 组 `broker_cluster` 的状态机（`broker_cluster_meta_machine.erl:17-27`）。读路径只走本地镜像 `broker_cluster_cache`（`cache.erl:58-62`），永不回 ra。镜像与真相的对账机制**已经存在**：状态机每次 apply 都写缓存（`meta_machine.erl:41,74-75,92`），快照安装后由 `snapshot_installed/5` 整表重建（`:72-76`）。**不需要新造对账周期**。
- **`broker_meta` 的 `cluster_node` / `partition_meta` 不再是成员/归属真相**：保留表定义（ADR-002 §D1 的 11 张表清单不变、不删表、不破坏既有测试），但语义降级为「遗留形状，生产无写入者」。`put_node/2` / `put_partition/2` 标记为仅测试用，新代码不得调用。
- **节点自身的生命周期状态（`up | draining | down`）是节点本地真相，不进 ra。** 单写者 = 管理 API（D2），存储为节点本地状态，经 8090 既有路由对外暴露。**理由**：drain 必须在「失去多数派」时仍然可用——那恰恰是最需要摘流的时刻；若 drain 需 ra 多数派提交，则失去多数派时既不能写也不能摘流，运维被锁死。
- **不新增 Mnesia 表、不新增 app、不引入第二套成员真相**（`net_kernel` / `global` / ring / `chash` 全部禁止，R-N1）。

### D2. drain 状态机 `up | draining | down` 由管理 API 驱动，`/readyz` 与之对齐；先修 `/readyz` 的 loaded/started 缺陷（回答 R-HA1/R-HA2/R-LB8）

- **前置修复（S12-A 第一个交付，不是可选项）**：`started/1`（`broker_admin_http.erl:131-140`）必须改为判定「**已 started**」而非「已 loaded」。当前 `application:get_application/1` 传原子的形态命中的是 `{loaded, App}`，`orelse` 第二子句在 release 场景几乎不会触发。修法在 S12-A spike 定（候选：只保留 `which_applications/0` 判定，或 `application:get_application` 的模块形态）。**在此修复之前，R-HA2 的一致性承诺不成立，不得声称已对齐。**
- **drain 语义（冻结）**：`draining` = **停止接受新连接，保持在用连接直至自然结束或超时**。实现手段是停止 accept（ranch listener 内嵌在各自 app 的 supervisor 下，停 listener 即停 accept），**不迁移连接、不断开在用连接**。超时阈值属实现参数（`S12-A` 定，须有配置消费者，R-E6）。
- **`/readyz` 与 drain 的对齐**：`draining` 期间 `/readyz` 返回 **503**（附 `status => draining`），`/healthz` 仍返回 200（进程活着）。这是「主动健康供给」的口径——节点**主动声明**自己要去服务化，而不是等 LB 靠连接失败才发现（R-HA1 的反面即 tcpbalance 的被动 down，`s12-infra-tcpbalance.md` F-3）。
- **管理 API**（R-LB8）：`drain` / `undrain` 走 8090 路由，入参严格校验、用显式状态变更事件，禁止假 `EXIT` 消息之类的 hack（`s12-infra-tcpbalance.md` F-6/F-7）。非回环 `bind` 且 `token` 为空时**必须拒绝启动**——当前该不变式只写在注释与文档里（`broker_admin_http.erl:19-22`、`docs/operations.md:9`），无启动校验。

### D3. LB 形态 = 外部 LB + Broker 在 8090 供给节点视图契约；不做四层代理、不做 DNS-SD、不做客户端侧路由（回答 OQ-1、R-LB1、R-N2）

- **Broker 不做四层 TCP 代理**（R-N2）：连接是有状态的（MQTT session、游标、`proto_meta.cluster_epoch`），四层透传会把有状态协议当无状态字节流。`tcpbalance` F-9（单例 `register(balance, self())`）、F-10（状态页实际不可用）已证明该形态不可行。
- **不做 DNS-SD**：发现不是成员管理（S11 已冻结该边界，ADR-030 D4）。
- **不做客户端侧路由**：客户端路由会把集群真相下推到不可控的客户端版本里，与 D1「唯一真相在服务端」矛盾。
- **契约落点 = 8090 既有路由**，不开新端口（R-E6）：`/healthz`（进程活着）、`/readyz`（可否接流量，含 `draining` 503）、`/api/v1/status`（`node` / `uptime_seconds` / `partitions` / `cluster` 快照）已是既有出口（`broker_admin_http.erl:44-50,85-124`）。S12-A 在其上补齐**节点标识、角色、容量/权重、健康状态、epoch**，不新建 listener。
- **外部 LB 是部署侧职责**：其选型与配置不在本仓交付范围；Broker 只保证契约稳定（字段集合、状态枚举、HTTP 语义）。`docs/operations.md:530`（「指标/管理面故障不应把 broker 摘出负载均衡」）是本形态的既有依据。

### D4. 再平衡触发者 = `broker_cluster` 内的新模块；默认关闭、手动可覆盖、分区数均衡、滞后 + 冷却双门限、一次一轮（回答 OQ-3、R-RB1/R-RB2）

- **位置**：`broker_cluster` 内的**新模块**。不开独立控制面进程池、不新增 app、不新增共识（R-N6）。它是 `assign/4` / `migrate/4` / `finish_migration/2` / `remove/1` 的**新调用者**——这四个命令已是唯一写入口，epoch 单调性由状态机检查（`meta_machine.erl:85`，拒旧 epoch），再平衡器不新增写路径。
- **默认关闭**：节点 join/leave **不自动触发**再平衡。仅运维命令显式触发。这是把 ADR-021「S4 不做自动再平衡」的保守性延续到 S12-C 落地：再平衡器是**可选工具**，不是**自动行为**。
- **手动覆盖优先**：运维命令指定的目标归属直接生效，再平衡器不与之争用。
- **目标函数 = 各节点分得的分区数均衡**（`count(partitions where owner = Node)`）。**不含连接数**——`connections_current` 只覆盖 MQTT 系（见 D7），按连接数加权在今天等于按假数据加权；**不含内存/CPU**——这两项在 62 键冻结集里根本没有。
- **稳定条件（不迁移的判据，冻结为双门限）**：① 各节点分区数极差 ≤ `hysteresis`（默认 1）；② 距上次迁移 ≥ `cooldown_ms`（默认 300000）。两者**同时**满足才不动；一次只迁移一个分区（一轮一个 `migrate`）。目的是避免抖动（`s12-infra-nkdist.md` §2.2 的反面教训）。
- **粒度受 G=16 约束**：单 group 内 owner 切换的最小单元是 1 个分区；跨 group 的均衡需运维改配置（ADR-008 `:15-19`），不在自动范围。

### D5. 迁移对象只有分区归属；不迁移任何活进程；不引入一致性哈希、不改 `phash2` 分区编号（回答 OQ-4/OQ-5、R-RB3/R-RB5/R-RB6）

- **迁移的唯一对象**是分区归属 `(owner, epoch, replicas, state, index)`，复用 ADR-021 D3 的两阶段 `migrate`（`state=migrating`，epoch+1，目标成为 owner，源仍可读）→ `finish_migration`（`state=active`，epoch+1）。旧 owner 写入被 `{fenced, _}` 拒绝（`broker_cluster.erl:83,93-96`），**不出现无 Fencing 双写**。
- **不定义活进程的三态迁移契约**：R-RB5（新节点起进程取状态 / 分裂合并 / 旧进程冻结停止）与 R-RB6（handoff 格式版本号）在能力矩阵标「S12 不交付」。理由：ADR-019 `:126` 明确 `assigned` **不落盘**（「落盘会把它变成『崩溃前谁该收到』的第二份真相」），worker 是唯一分配点（`:55`）；`broker_subscription` 的消费者注册表是本地 ETS（`broker_subscription_worker.erl:366-378`），游标/会话是本地 flat file（`broker_subscription_files.erl:74-76`）；ADR-010 `:92` 明确 Router 无自己的持久化状态。**活的 `assigned` 没有合法迁移对象**——迁移它等于凭空制造第二份真相。
- **不引入一致性哈希 / ring / vnode / `chash` / `riak_core_apl` / `nklib`**（R-N1、R-RB3）。再平衡只在「同一 ra 组内的节点之间」换 owner，**不改变 `phash2(Destination, N)` 的分区编号**（ADR-010 `:92` 冻结 `route/3` 为纯函数，迁移只改 `(owner, epoch)`）。分区↔组的 G=16 映射保持运维配置。
- **节点故障/再平衡时消费者会话不迁移**：由客户端重连到目标节点，按 ADR-019 重建（`assigned` 重投，ADR-015 既有重投口径）。

### D6. 迁移与故障的写可用性矩阵：严格绑定 ADR-007 三档，失去多数派一律拒绝且不 ACK（回答 OQ-6、R-HA3/R-CL4/R-RB4）

**冻结矩阵**（「迁移期间」= 旧 owner 仍在、epoch 已 +1；「宕机」= 旧 owner 无法再服务）：

| 可靠性档 | 迁移期间 | 旧 owner 宕机 |
|---|---|---|
| **L1 `os_buffer`** | 写**仍被接受**（QoS0 不进复制器，`broker_cluster.erl:106` 走 `owner_check/1`） | **可丢**（进程崩溃丢失窗口，ADR-007 `:15-19`） |
| **L2 `fsync`** | **成功**：目标 owner 本地 fsync 后返回 `{ok, Offset}` | **不丢**（WAL 已 fsync） |
| **L3 `replicated_commit`** | **成功**：`replicate/2` 收齐 `quorum = N div 2 + 1`（含本地 fsync）才 ACK；旧 owner 已被 `{fenced,_}` 拒绝 | **不丢**（多数派副本仍在，ADR-007 `:19`） |
| **失去多数派（任何档）** | **拒绝**：`{error, {replication_failed, Acks, Needed}}`，**不得 ACK 发布者**（ADR-021 `:79-80`、ADR-007 `:21`） | 同左 |

- **连接处置**：一律由客户端重连。drain 只停 accept、不迁移连接（D2）。因此 R-CL4 的「成员变更不得中断在用连接」解释为：**正常 drain 不中断在用连接；节点宕机由客户端 keepalive/超时发现并重连**。
- **禁止表述**：「零停机」「自动故障转移」（R-HA3/R-N4）。任何可用性承诺必须落到上表的档位与丢失边界。
- **`proto_meta.cluster_epoch` 的适用范围如实收窄**：目前**只有 MQTT 系**打这个字段（`grep -rln cluster_epoch apps/*/src/` 仅 `broker_cluster.erl` 与 `mqtt_conn_fsm.erl`），其余协议只过较弱的 `owner_check/1`（仅校验所有权，不校验 epoch，`broker_cluster.erl:106,109-110`）。**不得**把 R-CL2 表述为「全协议 epoch 校验」。

### D7. 容量与排队：Broker 不做内部排队，超限显式拒绝；连接计数复用 ADR-016 既有键、不扩容、不新建 app（回答 OQ-7/OQ-8、R-LB4/R-LB5/R-LB9）

- **不做 Broker 内部排队**：连接级等待队列长度**恒为 0**。容量超限**显式拒绝**。QUIC 已是唯一既有示范（`{error, too_many_connections}`，`mqtt_quic_listener.erl:210-213`）；TCP 侧需在 ranch per-listener `max_connections`（默认 1024，超限 acceptor 睡下、无显式拒绝）之上补一个**节点级计数闸门**，落地位置在 S12-B spike 定。`tcpbalance` F-4 / `gen_lb` F-3 的无限等待队列是反面教材，R-LB5 禁止 `gen_server:call(..., infinity)` 式无限阻塞。
- **「无可用节点」与「节点恢复」竞态由外部 LB + 客户端重试消化**，不在 Broker 内排队（R-LB6 的用例在 S12-B 落，行为不落 Broker）。
- **指标：复用既有键、修记账错误、不扩容冻结键集。** ADR-016 `:62` 要求键在编译期/启动期登记，`:93` 新增键须改代码（故意，防高基数），`:98` 集合被 `broker_metrics_SUITE` 的 `keys_are_stable` 钉死。因此：
  - `connections_current` / `connections_peak` **就地复用**，不新增 `conn_waiting` / `conn_rejected_total` 等键。
  - **必须修记账错误**：这两个 gauge 目前只有 `mqtt_conn_fsm.erl:2211-2229` 与 `mqtt_sn_conn.erl:141,286,645,684` 在写；`broker_proto_ws` / `broker_proto_mqtt_ws` / `broker_proto_nats` / `broker_proto_amqp` / `broker_proto_kafka` / `broker_proto_pulsar` / `broker_proto_a2a` / `broker_proto_mcp` / `broker_proto_webrtc` / `broker_proto_coap` **一次都不写**，故该 gauge 不代表节点真实连接数。修法是让这些前端在连接生命周期上调用既有 `gauge_add/2`（`broker_metrics.erl:119-134`）——**键不变、集合不变**，属正确性修复而非键集扩展。
  - **R-LB9 的扩展项（被拒连接数、节点上下线次数、迁移次数、等待队列长度）标「S12 不交付」**；键集扩展须独立走 ADR-016 修订流程（连同 `keys_are_stable` 一起改），不与 S12 耦合。
  - 连接计数是**数据面观测**，不进 ra 状态机、不参与再平衡目标函数（D4）。

### D8. ADR-021 的两项遗留不并入 S12（回答 OQ-9、R-HA5）

- **`{on_quorum_loss, reject | os_buffer_only}` 运行时开关不并入 S12。** ADR-021 `:70` 已冻结「开关未实现、配置项已移除（配置项必须有消费者）、当前行为等价 `reject` 档」。而且 `os_buffer_only` 档若实现，即引入「失去多数派时静默降级」，与 ADR-007 `:21`「必须被拒绝或显式降级，不得静默成功」存在实质冲突，需独立评估——不是基础设施性能问题。
- **`broker_security` 的 mnesia 接线不并入 S12。** ADR-021 `:101` 已冻结「未接线、`{source, mnesia}` 配置项从未存在、故不补空壳」。属数据面认证与安全面，与集群/HA/LB 无耦合。
- 两者在能力矩阵逐行留痕为「S12 不交付」，ADR-021 的「列为 S6 之后 / S7 之后」保持不变。

### D9. 跨节点投递不在 S12 范围：G2 的达成口径如实收窄（回答 OQ-10 的一部分，并修正 roadmap §14 的目标表述）

- **冻结事实**：跨节点 publish→subscribe **不存在**（§背景 第 4 条）。因此 **S12 交付后「多节点作为同一个 Broker」仍不成立**——A 节点订阅者收不到 B 节点 publish。
- **S12 交付的是**：单节点接入 + 分区归属迁移 + 外部 LB 契约 + drain + 再平衡器。G2 的达成口径据此收窄为「**多节点接入 + 分区归属可迁移 + 外部 LB 分发 + 会话粘性（R-CL2）**」，**不含**「跨节点消息路由」。
- **跨节点投递若要做，单列 S12-D**：复用分区 owner + 订阅者节点注册表 + 同一内核（不写第二套消息内核），并**新增 ADR**——因为订阅者节点注册表本质上是「消费者在哪个节点」的索引，会触及 ADR-010「Router 只持有 `Filter → [SubRef]` 倒排索引、不持有消费者状态」的冻结边界，必须显式修订而非顺手加上。
- **能力矩阵与 README / roadmap 必须逐字写明这一点**，不得让「S12 交付」被读成「集群 Broker 达成」。

## 后果（Consequences）

- **+** 十个 OQ 全部收敛，S12-A/B/C 有可执行的边界；三件事（HA / 集群 / LB）共用 ra 这一套真相，不再有第二份元数据。
- **+** drain 状态留在节点本地 ⇒ **失去多数派时仍可摘流**，这是运维上最需要的能力。
- **+** 再平衡器只搬 `(owner, epoch)`、不改 `phash2` 分区编号 ⇒ ADR-010 的 `route/3` 纯函数冻结不被破坏，Router 无需感知归属。
- **+** 不迁移活进程与 ADR-019「`assigned` 不落盘」一致，不制造「崩溃前谁该收到」的第二份真相。
- **-** **跨节点投递仍不存在**，G2 只是「多节点接入」而非「一个逻辑 Broker」。这是本 ADR 最主要的限制，且是 S12 需求文档 K1~K8 未列的缺口。
- **-** **S12-A 的 `/readyz` 修复是本 ADR 的前置条件**：在它修好之前，`draining ⇒ /readyz 503` 的对齐承诺不能生效（今天 loaded 但 crashed 的 app 也回 200）。
- **-** **再平衡器默认关闭** ⇒ S12-C 交付的是「能力 + 开关」，不是「自动均衡」；运维不显式触发就不会迁移。
- **-** `connections_current` 在修记账之前**只覆盖 MQTT 系**，任何按连接数加权/告警的判断都是错的；修复需逐个前端接入（10 个 app）。
- **-** `cluster_epoch` 的 Fencing 目前**只有 MQTT 系生效**，其余协议仅所有权校验。R-CL2 的全协议 epoch 校验是**未达成项**。
- **-** 指标键集不扩容 ⇒ 容量控制与再平衡的**可观测性不完整**（无被拒计数、无迁移计数），须等独立的 ADR-016 修订。
- **-** 文档同步义务：`docs/operations.md:640`、`progress.md:79`、S12 需求稿 `:47` 仍写 `broker_meta` 11/12 张表，实际 **13 张**（S9 追加 `agent_card` / `push_config`，ADR-026 §D3/§D6）。

## 验证（Validation）

S12-0 为纯文档阶段，验证即**事实可核对性**：

- **OQ 收敛完整性**：10 个 OQ 在收敛报告 §2 逐条有裁决、理由、代价 + 能力矩阵留痕，与本 ADR 的 D1~D9 一一对应。
- **事实可复现**：§背景 的 5 条冲突事实与收敛报告 §1.2 的 9 条负向发现，全部带 `file:line`，可用 `grep` / 单行 `erl -eval` 复现（`/readyz` 的 loaded/started 已用 `application:load/1` 实测）。
- **不新增的证明**：本阶段无代码改动 ⇒ `rebar3 ct` 基线不变（S11-C 收口 1373/1373、84 套件）；无新增 app / 端口 / 配置项 / 依赖。
- **后续子阶段的验收**（S12-A/B/C，本 ADR 只冻结契约）：
  - S12-A：`/readyz` 在「app loaded 但 crashed」时**必须**回 503（负控用例）；`drain` 期间 `/readyz` 503 且在用连接不中断；非回环 `bind` + 空 `token` **启动即失败**。
  - S12-B：节点级计数闸门超限时**显式拒绝**（不是 acceptor 睡下）；`connections_current` 在 WS/NATS/AMQP/Kafka/Pulsar/CoAP/WebRTC 的连接生命周期上正确增减（负控：断开后归零）。
  - S12-C：扩 `broker_cluster_dist_SUITE`（现有 3 节点 `peer`，`broker_cluster_dist_SUITE.erl:19,141,170`）——节点加入/退出/故障下再平衡器**默认不动**、显式触发后一轮一迁移、旧 owner `{fenced,_}`、无双写、`cooldown_ms` 内不复迁；再平衡器崩溃不影响 `assign/4` 直连路径。
- **能力矩阵**：4 仓全部 F- 条目（各单仓稿 §2.5）逐行留痕（R-N8）；本 ADR 的 9 项「S12 不交付」逐条列出（活进程迁移、handoff 版本号、指标键集扩展、`on_quorum_loss`、`broker_security` mnesia 接线、跨节点投递、全协议 epoch 校验、跨 group 自动均衡、Broker 内部排队）。

## 落地偏差记录

### S12-A 首个代码交付：`/readyz` 的 loaded→started 修复（2026-11-17）

D2 的前置修复已落地，**D2 原文不动**（冻结文本只追加本节）。

- **修法**：`started/1` 改为只判定 `lists:keymember(App, 1, application:which_applications())`。D2 列的候选里选了「只保留 `which_applications/0` 判定」——`application:get_application/1` 的模块形态同样受 loaded 语义影响，不能用来回答「started」。
- **实证**（`application:load(broker_queue)` 后单行 `erl`）：`application:get_application(broker_queue) = {ok, broker_queue}`，而 `which_applications()` 不含它；旧 `started/1` 返回 `true`，新逻辑返回 `false`。用 `/readyz` 实际门控的六个 app 全量复核，旧逻辑对**全部六个**都答 `true`（即「missing 为空 → 200」），新逻辑对全部六个答 `false`（`missing` 为全集 → 503）。
- **测试**：`broker_admin_SUITE:started_predicate_ignores_loaded_not_started_apps`——**走真实 `/readyz` 路由**，不导出私有谓词、不加 `ignore_xref`。做法：把六个必需 app 全部 `application:load/1`（**只 loaded、不 started**），断言 `/readyz` 回 **503** 且 `missing` 恰为「运行时确实未 started 的那一组」。期望集合**运行时推导**，因为 `broker_security` 会随 `broker_admin` 一起启动（我第一次写成硬编码全六个，实测失败在 `[broker_security]`，已改成推导）。
  - **有效性已反证**：把 `started/1` 临时改回旧实现重跑，该用例**失败**（12 例中 1 例挂）；改回修复后 12/12。即这是真回归测试，不是空跑。
  - **为什么不硬编码 `broker_router`**：CT 所有套件共用一个节点，`acp_agent_SUITE` 等会 `ensure_all_started(broker_router)` 且 `end_per_suite` 不停止它，硬编码就变成对「套件执行顺序」的依赖而非性质断言。
  - **为什么不用一次性 app spec**：`{application, App, [{modules, []}, ...]}` 加载后 `application:get_application(App)` 回 `undefined`（实测），旧逻辑同样回 `false`，无法区分新旧实现——那样的负控是假的。
- **未保留多余导出**：初版曾 `-export([started/1])` 供直接单测，但那样会让 `xref` 从 302 涨到 303（新增一条 unused-export）。改为走 HTTP 后撤销导出，`xref` 保持 **302**。
- **验证**：admin 套件 **12/12**；全量 `rebar3 ct` **1374/1374**（基线 1373 + 1 新测试）；`dialyzer` 0 warning；`xref` 302 条全 unused-export（不变）。

### S12-A 第二个代码交付：drain 状态机 + 管理 API + 启动校验（2026-11-17）

D2 的其余三条已落地，**D2 冻结文本仍一字未改**。

- **状态机**：新模块 `broker_drain`（`apps/broker_admin/src/broker_drain.erl`），`up | draining | down`，挂进 `broker_admin_sup`。状态镜像到 `persistent_term`（key `{broker_drain, state}`，默认 `up`），`/readyz` 热路径**不调用**该进程。
- **「停止 accept」的实现**：`ranch:suspend_listener/1`。实测其源码语义（`ranch.erl:234-243`）是 `set_addr(Ref, {undefined,undefined})` + `supervisor:terminate_child(ListenerSup, ranch_acceptors_sup)`——**只终止 acceptor supervisor**，`ranch_conns_sup` 与全部在用连接存活（`ranch_listener_sup.erl:38-46` 的子进程表证实两者并列）。这正是 D2 的「不迁移连接、不断开在用连接」。
  - **管理 API 自身不受影响**：`broker_admin_listener` 是手写 `gen_tcp` acceptor，不是 ranch child，所以 drain 期间 8090 仍可达，`undrain` 随时可调。
  - **覆盖范围如实收窄**：drain 只覆盖 ranch TCP listener。QUIC（`mqtt_quic_listener`）与 CoAP（`coap_listener`）是自定义 `gen_udp`/`gen_server`，**不在挂起范围**。这是一条需要在 S12 后续阶段处理或明确接受的边界，此处如实记录。
- **`/readyz` 对齐**：先看 drain 状态——`draining`/`down` 一律 **503** 且 `status` 直接是该状态；`up` 时维持原有的 `ready` / `not_ready` + `missing` 逻辑。`/healthz` 恒 200（draining 的节点是健康的，只是不接新流量）。
- **管理 API（R-LB8）**：
  - `POST /api/v1/drain`，body 可选：`{}` 或空 = 用配置的 `drain_timeout_ms`；`{"timeout_ms": N}`；`{"timeout_ms": "infinity"}`。
  - **严格校验**：非法 JSON / 未知字段 / 类型错误 → **400**，且**不改变状态**（`drain_rejects_a_malformed_body` 断言拒绝后仍为 `up`）。未知字段回 `{"error":"unknown_field","field":"<名>"}` 而不是丢掉元组的裸串。
  - `POST /api/v1/undrain`；`GET /api/v1/node` 给外部 LB 的契约（`state`、`ready`、`connections{active,total,max}`、本节点拥有的 `partitions`）。连接数直接聚合自 `ranch:info/1`，不是另立计数器；分区归属走 `broker_cluster:owner/1`（本地镜像，**不碰 ra**）。
  - `down` 之后再 `drain` → **409**。`drain/1` 的宽限期上限 `16#FFFFFFFF`（与 `send_after` 同界）。
- **`drain_timeout_ms` 有真实消费者（R-E6）**：`config/sys.config.src` 的 `broker_admin` 块新增该键，默认 `infinity`；到期 `draining -> down`。**到达 `down` 也不断开在用连接**——它只是「可以停进程了」的信号。
- **启动校验（D2 末条）**：`broker_admin_sup:init/1` 在 `bind` 非回环且 `token` 为空时 `{stop, {insecure_bind, Bind, empty_token}}`，应用启动失败。`enabled => false` 不校验（没有 listener 就没有暴露）。这条此前只写在注释（`broker_admin_http.erl:19-22`）与文档（`docs/operations.md:9`）里。
- **测试**（`broker_admin_SUITE`，共 **20** 例，新增 8 例）：
  - 用**真实 ranch listener**（`broker_admin_drain_probe`，`port => 0`）证明：drain 后 `ranch:info(Ref).status` 为 `suspended`、新连接 `econnrefused`、**在用连接仍能收发**；undrain 后恢复。
  - `timeout_ms => 0` 走异步 `send_after`，因此**轮询等状态**而不是假设回复即已迁移。
  - bind/token 用例覆盖两侧：空 token + `{0,0,0,0}` 启动失败且 `broker_admin_listener:port() =:= undefined`；带 token 时同一 bind 可以起来。
  - **反证过四种失效模式**（逐条临时改坏再跑全量套件）：drain 不挂起 → 3 例挂；drain 改成 `stop_listener`（真正断开连接）→ 5 例挂；`/readyz` 不看 drain → 2 例挂；校验放行宽 bind → 1 例挂。即这些测试不是空跑。
  - **另跑了一次真实数据面验证**（不只在测试桩上）：起 `broker_storage` + `broker_proto_mqtt`（真实 ranch listener）+ `broker_admin`，先建一条真 TCP 连接，drain 后该连接**仍能完成 MQTT CONNECT 并收到 CONNACK**，新连接 `econnrefused`，`/readyz` = 503 `draining`，`/healthz` = 200，undrain 后恢复。固定端口（非 `port => 0`）在 drain/undrain 前后端口不变；`port => 0` 的 listener 在 undrain 时会重新取一个临时端口，这是测试桩的产物而非 drain 的语义。
- **顺带修掉的两个真实缺陷**（都由测试逼出来，不是重构）：
  1. `json_safe/1` 没有布尔分支，`true`/`false` 被编码成字符串 `"true"`/`"false"`。`/api/v1/node` 的 `ready` 若回 `"false"`，按布尔比较的 LB 会读反。加了 `json_safe(true|false)` 分支（既有端点未用过布尔，故无行为变化）。
  2. `broker_admin_listener` 未 `trap_exit`，supervisor 关停时 `terminate/2` 不执行，`persistent_term` 留下**过期端口**，`port/0` 会对已无人监听的端口作答。加 `process_flag(trap_exit, true)`。
  3. **重复 drain 不取消旧定时器**（提交后自查发现）：先用 200ms 宽限期 drain、再用 30s 重新 drain，节点仍会在 200ms 后转 `down`——运维的「延长宽限期」被静默忽略。`cancel_timer/1` 不足以修好，因为**已投递到邮箱的消息无法撤回**，所以改为给定时器打**代际号**（`{drain_timeout, Gen}`），`do_drain`/`do_undrain` 各自递增 `gen`，`handle_info` 丢弃代际不匹配的消息。回归测试 `redrain_replaces_the_earlier_grace_period`；**反证**：撤掉代际/取消逻辑后，恰好且仅有该用例失败。
- **重复 drain 的幂等性已核实**（不是假设）：第二次 drain 会重新算一遍 `Suspended` 列表，若此时 `ranch:suspend_listener/1` 对已挂起的 listener 报错，`Suspended` 会变成 `[]`，`undrain` 就不知道该恢复谁——节点会永久失聪。查 `ranch.erl:234-241`：`get_status(Ref)` 为 `suspended` 时直接返回 `ok`，`resume_listener/1` 同理。实测 `drain → drain → undrain` 后 listener 回到 `running`。
- **验证**：admin 套件 **20/20**；全量 `rebar3 ct` **1382/1382**（1374 + 8）；`dialyzer` 0 warning；`xref` **302** 条全 unused-export（不变，新模块的导出计入既有 unused-export 口径，未新增其他类别）。
- **S12-A 收尾（第三个交付，2026-11-17）：多节点视图一致性证据**。D1 的「节点视图唯一真相是 ra」在单节点上无法证明——单节点里「读 ra」和「读本地猜测」给出同样的答案。新增 `apps/broker_admin/test/broker_admin_dist_SUITE.erl`：三个真实 peer（`peer`，OTP 28 stdlib，与 `broker_cluster_dist_SUITE` 同款）组成 ra 组，两个分区分别归属两个节点，然后**走真实 HTTP** 读每个节点的 `/api/v1/node`：
  - 每个节点只报 `broker_cluster:owner(P) =:= {ok, node()}` 的分区，`count` 与 `epoch` 都来自 ra；
  - 全集群的并集恰好是分配的完整分区集，**无分区被两个节点同时认领**（这条是「没有第二套真相」的判据：任何节点若读 Mnesia 或本地猜测，视图就会分歧）；
  - `/api/v1/status` 的配置分区集按 ra 归属过滤后，必须**逐元素等于** `/api/v1/node` 的归属集——两个管理端点对归属给出不同答案，正是 D1 要防的失效；
  - **drain 是节点本地的**：drain 一个节点后，该节点 `/readyz` 503 `draining`，其余节点的 `state` 仍是 `up`、`ready` 仍是 `true`、分区集不变、`/readyz` 也不出现 `draining`；`undrain` 同样不触碰其它节点。
  - **三条断言都做过反证**：去掉 owner 过滤（每个节点认领所有已分配分区）→ 一致性用例挂；`ready` 恒 `false` → 本地性用例挂；`/status` 分区集置空 → 端点一致断言挂。
- **S12-A 已完成**；如实保留的边界：① 本次落的是**节点自身**的 drain 状态与对外视图，D1 里 `broker_meta` 的 `cluster_node`/`partition_meta`「退役为遗留形状」的清理未在本阶段删除（生产已无写入者，删除属独立重构）；② QUIC / CoAP 不归 ranch 管，**不在 drain 挂起范围**，仍需停进程摘流；③ `GET /api/v1/node` 的 `weight` / `role` 属 S12-C 的再平衡器，未提前造常量。

### S12-B 第一项：修 `connections_current` 记账（2026-11-18）

D7 把「这两个 gauge 只有 MQTT 系在写」列为**必须修**，并写死了修法：让没写的那些前端在连接生命周期上调用**既有** `gauge_add/2`，键不变、集合不变。落地如下。

- **修的是记账，不是键集**：`broker_metrics` 新增一对共享辅助 `connection_opened/0 -> boolean()` 与 `connection_closed/1`，两者都只写 `connections_current` / `connections_peak`。没有新增键，`keys_are_stable` 未改。**不给十个前端各写一遍峰值逻辑**，是因为那段「读 current、比峰值、只在更高时写」的代码一旦抄十份，就会有一份漏掉比较或忘了 `-1`。
- **boolean 返回值是这对辅助存在的理由**：`gauge_add/2` 在 metrics 关闭或未启动时返回 `0` 而不是 `ok`（ADR-016 S2-A 注记），所以「加没加上」必须被记录下来。`connection_closed(false)` 是 no-op —— 否则一次 disable/enable 周期会把 gauge 推成负数，而那是一个只有重启才看得出来的坏数。
- **接入的前端（9 个）**：D7 正文列的「一次都不写」名单有 **10 个** app，D7 验证清单只点了其中 7 个（WS / NATS / AMQP / Kafka / Pulsar / CoAP / WebRTC）——少了 `a2a` 与 `mcp`，两者同样是 ranch 前端且同样从不写该 gauge，所以按正文的完整性口径一并接入；名单里的第 10 个 `mqtt_ws` **不计**，理由见下条。
  - 每个连接一个 `gen_server` 且生命周期清晰的：`ws_conn`、`nats_conn`、`amqp_conn`、`kafka_conn`、`pulsar_conn`、`webrtc_conn`（`init/1` 计、`terminate/2` 结）。`webrtc_conn` 有**三个** `terminate/2` 子句（未入房 / 无 socket / 正常），三个都结算，否则「握手完成但从未 authenticate」的路径会漏一行；共用 `uncount/1` 以免三处漂移。
  - **CoAP 只计 DTLS 会话**：明文 UDP 是无状态数据报，没有生命周期可计，这一点**如实写进能力矩阵**而不是拿 socket 数充数。计数点在 `record_session/4`（唯一记录处）与 `drop_session/2`（唯一移除路径），`terminate/2` 里逐个 session 结算，避免 listener 停止时留下残余。
  - **`a2a_conn` / `mcp_conn` 是常函数循环**，没有 `terminate/2`：用 `try ... after` 保证结算，异常也不会漏。它们在 `ranch:handshake/1` 成功后立刻计数 —— 那个 socket 的整个存活期就是这一段循环。
- **两个 app 补了 `broker_metrics` 依赖声明**（`broker_proto_a2a` / `broker_proto_mcp`）：**不是新增运行时依赖**，两者本来就经 `broker_queue` 传递依赖 `broker_metrics`，启动顺序早已在它之后；此前只是没写进自己的 `applications`，而代码现在直接调用它，不声明就是靠运气。
- **`mqtt_ws_conn` 与 `mqtt_quic_conn` 明确不计**：它们底下跑的就是 `mqtt_conn_fsm`（S8 的复用，见两个模块自己的文档头），在那里再加一次就是**双计**。D7 的验证清单里也没有它们，与本结论一致。
- **测试**：每个接入的前端各加**一条负控用例** `connections_current_tracks_the_connection_lifecycle`（CoAP 的是 `..._the_dtls_session_lifecycle`），形态统一：
  - 量**相对基线的增量**，不断言绝对值 —— CT 共用单节点，别的套件可能还握着连接，绝对数会让用例变成顺序依赖；
  - **上下都等**（`wait_until_current/2` 轮询），因为计数写在连接进程的 `init/1` 里、在 ranch 接受之后才执行，连上就断言 +1 会输给调度；
  - 结束时回到基线，这正是 D7 要的「负控：断开后归零」。
  - `broker_metrics_SUITE` 另有 2 例直接钉这对辅助：current 与 peak 的语义、以及「未计数的连接不得被减」。
- **反证（逐条改坏再跑）**：
  - `ws_conn:terminate/2` 不结算 → ws 用例挂；
  - `coap_listener:drop_session/2` 不结算 → CoAP 用例挂；
  - `a2a_conn` / `mcp_conn` 去掉 `try ... after` → 两个用例分别挂。
  四条都是「恰好且仅有对应用例失败」，不是整片红，说明断言指向的是真实现象。
- **验证**：全量 `rebar3 ct` **1395/1395、86 套件**（1384 + 11：`broker_metrics` 2 例 + 9 个前端各 1 例）；`dialyzer` 0 warning；`xref` **302** 全 unused-export（两个新导出均被调用，未增计数）。
   - **仍未做（如实留痕）**：① **节点级准入闸门**的落地位置已由下一节的 spike 定为 conn/FSM 层，但**尚未实现** —— 今天超限仍是 ranch 的 acceptor 睡下，而非显式拒绝；② R-LB9 的扩展项（被拒连接数、节点上下线次数、迁移次数、等待队列长度）按 D7 标「S12 不交付」，一条未加；③ CoAP 明文 UDP 不在计数口径内（无连接语义），能力矩阵须照此写明。

### S12-B 第二项：节点级准入闸门的 spike（2026-11-18）

D7 要求「TCP 侧在 ranch `max_connections` 之上补一个节点级计数闸门，落地位置在 S12-B spike 定」。spike 已成文于 `docs/design/s12b-admission-gate-spike.md`，**结论未实现**，记录于此以免下次重新推导。四条结论都来自本仓库实测，不是从 ranch 文档推断：

- **落点 = conn/FSM 层，不是 listener 层。**实测 `max_connections => 2` 时 4 条 TCP 连接**全部 accept 成功**（`ranch:info` 报 `active_connections => 2`）：`ranch_conns_sup.erl:258-276` 在 `CurConns2 >= MaxConns` 时把 acceptor 放进 `Sleepers`，**连接本身没有被拒**。真实 MQTT 前端同样如此——第 3/4 个客户端 TCP ACCEPTED 但**永远收不到 CONNACK**（静默挂起，`/metrics` 上一切正常）。listener 层还拿不到跨 listener 预算、协议握手尚未解析（无法给对拒绝帧）；而 conn/FSM 层已有全部材料（`mqtt_conn_fsm:refuse/2` + `v5_refuse/1` 两套码表、`coap_listener:admit_session/4` 的两级 cap、QUIC 的 `{error, too_many_connections}`）。CoAP 的既有注释已经把这条裁决写死了：「caps are checked here (not in the acceptor) … a second copy of the counters would be a second truth」。
- **计数不能复用 `connections_current`。**三个独立理由，都实测：① MQTT-SN 的 `enter_sleep/1` 会减计数但**进程仍存活**（保留 `sleep_timer` 与订阅），拿它当预算会漏算所有 sleeping 会话；② MQTT 的 `count_connection/1` 在 `mqtt_conn_fsm.erl:419`，即 CONNECT 解码、认证、`attach_session`、`subscribe_partitions` **全部成功之后**——而并发涌入恰恰是它读数最不准的时刻，闸门会穿透；③ metrics `enabled => false` 时 `gauge_add/2` 返回 `0`（`broker_metrics.erl:120-136`），准入不能依赖一个可选的观测开关。**但这不等于「两套真相」**：gauge 回答「服务着多少连接」（给人和告警），准入计数回答「还有没有名额」（给代码做决定），四行差异（metrics 关闭 / sleep / 计数时机 / 峰值）各自必须选边，合成一个数必然牺牲其中一项。D7 的「不扩容键集」约束的是 `broker_metrics` 的键，不是「节点上只许有一个计数器」——准入计数**不落 metrics**，所以键集仍不变。
- **承载 = `broker_security` 里新增一个纯函数模块**（建议 `broker_admission`），形态照抄同目录既有的 `broker_limits.erl`：`persistent_term` 持有 `atomics`，`compare_exchange` 做「读-判-占」，**无进程、无 supervisor 子项、无 `gen_server:call`**（R-LB5 禁止无限阻塞）。预算读 `broker_security.limits` 的 merge-default（与既有六个 limit 同处一张 map）。13 个接受连接的前端**全部已依赖 `broker_security`**（逐个核对过），故非新增运行时依赖；不新建 app、不新增端口。
- **顺带发现一个既有缺口：drain 对非 ranch 前端是空操作。**`broker_drain:ranch_refs/0`（`:160-168`）只枚举 `ranch:info()`，而 QUIC 用 `quicer:listen/2`（`:111`）、CoAP 用 `gen_udp:open/2`/`ssl:listen/2`（`:138`/`:195`），**都不是 ranch listener**。所以 drain 之后这两个前端仍在接受新连接——S12-A 的验证只用了 ranch listener 探针，未覆盖此路径。闸门应承担此职责：admission 时读 `{broker_drain, state}` 的 `persistent_term` 镜像（`undefined` 视为 `up`），`draining | down` 直接拒绝。**必须读镜像而非调用 `broker_drain`**，否则 `broker_security` 反向依赖 `broker_admin` 会成环；`persistent_term` 是 erts 内建，不新增依赖。
- **MQTT-SN sleeping 会话计入预算**：它占的是节点资源（进程、订阅、timer），不是 TCP fd；D7 防的是节点被压垮。真正的解法是给 MQTT-SN 一个独立 session 上限（今天没有，实测无 `max_session*`），但那属于协议容量策略，**S12-B 不引入**。
- **各协议的显式拒绝**：MQTT 3.1.1 → CONNACK `rc=3`；MQTT 5 → `0x89`（复用 `refuse/2` 的两套码表）；MQTT-SN → CONNACK `return_code=3`（码表即 `<<?CONNACK, RC:8>>`，无需扩展）；WS/WebRTC/MQTT-over-WS → upgrade 阶段回 HTTP 503（尚未发 101，`ws_handshake.erl:84` 是唯一发 101 处）；A2A/MCP → 503；CoAP DTLS → 既有 `{refuse, session_cap}` 路径；CoAP 明文 UDP → 不适用（无连接语义）；QUIC → 既有 `{error, too_many_connections}`。**NATS/AMQP/Kafka/Pulsar 必须如实留痕**：连接刚建立时版本尚未协商，错误帧的合法性取决于客户端第一包，只能在「收到首帧、版本确定后」拒绝；做不到就是 close + 服务端 warning，**能力矩阵须写明这四个协议给不出与 MQTT 同等的可观测性**。
- **不做排队**（D7 明令）：`compare_exchange` 一次尝试，失败即拒，无 retry、无 sleep、等待队列恒为 0。**不新增 metrics 键**：被拒只进 log（warning），这是今天唯一不扩容键集的观测手段；R-LB9 的扩展项仍标「S12 不交付」。

### S12-B 第二项落地：闸门实现（2026-11-18）

spike 的七条结论已按 §7 实现并接入全部前端。落地时有三处必须改写的结论，记在这里，`docs/design/s12b-admission-gate-spike.md` 已同步：

- **契约从 `ok | {error, _}` 改为返回 lease。**spike §7 第 1 条写的 `try_acquire/0 -> ok | {error, over_capacity | draining}` 无法表达「闸门关闭」：`infinity` 时不能假装占了一个名额，否则 `release/0` 会去减一个从未加过的计数、偷走别人的名额。落地为四种形态 `{leased, Key} | unlimited | {rejected, Why} | undefined`，**必须原样回传 `release/1`**；`undefined` 让每个前端只用一条 `terminate`/`after` 就能无条件释放。`release/1` 幂等。
- **只有计数器不够：`terminate/2` 不是可靠释放点。**spike 假设「进程结束就释放」，但 MQTT 连接是 `supervisor` 的 `temporary, brutal_kill` 子进程——被 kill 时 `terminate/3` **不执行**，只靠计数器会让节点缓慢泄漏到「永远满」，比没有闸门更糟（`mqtt_conn_SUITE` 的 `a_released_slot_is_usable_again` 在实现初期就是这样失败的）。因此 `atomics` 之外加一张 `public` ETS 台账（lease key → 持有进程），**仅在拒绝时**清扫已死持有者后再试一次：快速路径仍是一次 `compare_exchange`，O(n) 只出现在「准确性真的可疑」的时刻。`a_killed_owner_does_not_leak_its_slot` 钉死此行为，`a_live_owner_is_not_reclaimed` 钉死另一面（沉默但活着的 MQTT-SN 会话不得被清扫）。
- **QUIC 的 lease 要换主。**闸门在 `mqtt_quic_listener` 的 `{new_conn, _}` 处问（那是唯一还能回 `{error, too_many_connections}` 的点），但连接活在其后创建的 `mqtt_quic_conn` 里。故新增 `adopt/1` 把台账行改写到真正持有资源的进程，配额随连接进程死亡被清扫回收。

其余按 spike 执行：落点 conn/FSM 层、计数不复用 `connections_current`、`persistent_term` + `atomics` 无进程、预算默认 `infinity`、drain 读镜像、不做排队、不新增 metrics 键（被拒只进 warning 日志）。`broker_metrics` 键集未变，R-LB9 扩展项仍为「S12 不交付」。
