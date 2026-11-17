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
- **S12-A 仍未完成**：drain 状态机（`up|draining|down` + `/readyz` 503 一致）、节点视图真相落地、管理 API（R-LB8）、非回环 + 空 token 拒启。D2 的其余三条仍是待办。
