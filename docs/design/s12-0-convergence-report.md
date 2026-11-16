# S12-0 收敛报告 — 基础设施高可用 / TCP 集群 / 负载均衡

- **对应**：[`docs/design/s12-infrastructure-ha-and-lb.md`](s12-infrastructure-ha-and-lb.md) §5.4（OQ-1~OQ-10）、§2（现状与缺口 K1~K8）、§4（需求 R-HA / R-CL / R-LB / R-RB / R-N / R-E）、[`docs/roadmap.md`](../roadmap.md) §14。
- **性质**：**收敛报告草案**。全部结论基于仓库内可核对的代码/配置与上游包元数据，未写实现代码、未开端口、未新增依赖、未新增 app。
- **基线**：S11-C 已收口并在 `main`（三提交 `350fe82` / `b40c0f7` / `d13327e`，未 push）；全量 `rebar3 ct` **1373/1373、84 套件**（含 libcoap v4.3.5 真实客户端证据 8/8 PASS 及据其修复的两条 Block1 缺陷）、`dialyzer` 0 warning、`xref` 全为 unused-export（2026-11-17）。
- **配套**：`docs/design/s12-infra-{tcpbalance,gen-lb,gen-server-cluster,nkdist}.md`（四份单仓深挖稿，§2.5 的 F-1~F-12 是能力矩阵裁剪项来源）。
- **已落 ADR**：[`docs/adr/032-s12-node-view-drain-and-rebalance-boundary.md`](../adr/032-s12-node-view-drain-and-rebalance-boundary.md)（2026-11-17 签认，D1~D9 + 后果 + 验证）。
- **签认状态**：本文 §2 的十项结论表**已全部签认并落入 ADR-032**；§4 的两处待裁决已按推荐项拍板（§4.1 选 **A**：S12 不含跨节点投递、单列 S12-D；§4.2 选 **A**：不扩 ADR-016 键集）。两项决定分别固化为 ADR-032 **D9** 与 **D7**。

---

## 0. 结论速览

| OQ | 问题 | 收敛结论 | 决定性事实 |
|---|---|---|---|
| OQ-1 | LB 形态 | **外部 LB + Broker 在 8090 供给节点视图契约**。Broker 不做四层代理、不做 DNS-SD、不做客户端侧路由 | `/readyz`、`/healthz`、`/api/v1/status` 已存在且是唯一节点身份出口；`docs/operations.md:530` 已隐含外部 LB；R-N2 禁止四层透传；tcpbalance F-9/F-10（单例、状态页不可用）证明内建代理不可行 |
| OQ-2 | 节点视图与 `cluster_node` / `partition_meta` / `owner/1` 如何共用一份真相 | **ra 组为唯一真相；退役平行 Mnesia 视图表**。R-CL3 的「`reconcile/0`」不是「对账两个副本」，而是**本地 ETS 镜像与 ra 状态的对账**；不存在第二个需要互相对账的副本 | `broker_meta:reconcile/0` **不存在**；真实的是 `broker_meta_config` 的 `config_version` 5 s 轮询（`broker_meta_config.erl:50,112-120`），只对账配置版本；`partition_meta`/`cluster_node` 是**生产无写入者的死表**（`put_partition/2`/`put_node/2` 无生产调用者）；`broker_meta` ↔ `broker_cluster` 运行时 0 互相引用 |
| OQ-3 | 再平衡触发者放哪；目标函数与稳定阈值 | **`broker_cluster` 内新增再平衡模块**（不开独立进程池），默认**关闭**，仅运维命令可覆盖；目标函数 = 各节点 `(owner, epoch)` 分得的**分区数均衡**（不含连接数、不含内存）；滞后阈值 + 冷却时间双门限，一次一轮一轮轮转 | ADR-021 `:109-110`「S4 只实现组内归属，不做自动再平衡」把触发者留给 S12；`assign/4`/`migrate/4` 已是唯一写入口（状态机内做 epoch 单调性检查）；`broker_cluster_dist_SUITE` 已有 3 节点 peer 可挂用例 |
| OQ-4 | 归属映射是否引入一致性哈希 | **不引入 `chash` / `riak_core_apl` / `ring`**。再平衡只搬运 `(owner, epoch)`，**不改 `phash2(Destination, N)` 分区编号**。分区↔组的 G=16 映射保持运维配置，再平衡器只在「同一 G 内换 owner」之间选择 | ADR-010 `:92` 冻结 `route/3` 为纯函数 `phash2(Destination, N)`，迁移只改 `(owner, epoch)` 不改分区号；ADR-008 `:15-19`（G=16、禁止大规模 1:1、组数受资源预算约束）；R-RB3 / R-N1 明文禁止 ring/chash |
| OQ-5 | 哪些进程属于「有状态可迁移」范畴 | **只有分区归属（`(owner, epoch, replicas, state, index)`）可迁移，且复用既有两阶段 `migrate → finish_migration`。不迁移任何活进程。**理由：`assigned` 明确不落盘（ADR-019 `:126`），会话游标/消费者状态是本地 ETS + 本地 flat file，无跨节点复制对象；`route/3` 是纯函数无需迁移 | ADR-019 `:55,126`（worker 是唯一分配点、`assigned` 不落盘、崩溃后全部重投）；ADR-010 `:92`（Router 无自己的持久化状态）；`broker_subscription` 消费者注册表是本地 ETS（`broker_subscription_worker.erl:366-378`）、投递是本地 `Pid !`（`broker_subscription.erl:222-233`）；R-HA4 要求状态落 WAL（分区 WAL 已在 `broker_queue_partition.erl` 复制） |
| OQ-6 | 迁移期间连接与在途消息如何处置 | **在途消息绑定 ADR-007 三档显式口径（见 §2.6 的写可用性矩阵）；连接一律由客户端重连（drain 只停止 accept，不迁移连接）。**不存在「迁移连接」的概念 | 无 `prep_stop`、无 drain 状态机；ranch 关闭走 `exit(shutdown)` + 5 s 后 kill；ADR-007 三档 L1/L2/L3 已冻结；ADR-021 `:79-80` 失败路径「不得 ACK 发布者」 |
| OQ-7 | 连接计数 / 等待队列状态落在 `broker_metrics` 还是新 app | **复用既有键、修记账错误，不新增指标键、不新建 app。**`connections_current` 等已在 ADR-016 冻结集中；新增键须改 ADR-016 + `keys_are_stable` 测试，**不在 S12 做** | ADR-016 `:62,93,98`（键编译期/启动期登记、新增键改代码、集合被测试钉死）；`connections_current` 仅 MQTT 系在写（见 §1.5）——这是**修正确性**不是**加键** |
| OQ-8 | 是否需要 Broker 内部排队 | **不做 Broker 内部排队**。队列长度恒为 0：容量超限**显式拒绝**（QUIC 已是唯一实现 `{error, too_many_connections}`；TCP 侧补一个节点级计数闸门）。背压交给既有 per-connection 慢消费者守卫（ADR-016 S2-G 键已存在）。「无可用节点」由外部 LB 与客户端重试消化 | 唯一容量闸门是 ranch per-listener `max_connections`（默认 1024，超限 acceptor 睡下、无显式拒绝）；tcpbalance F-4（无限等待队列）是反面教材；ADR-016 已有 `conn_mailbox_len`/`conn_inflight`/`backpressure_*` 键 |
| OQ-9 | ADR-021 遗留项（`on_quorum_loss` 开关、`broker_security` mnesia 接线）是否并入 S12 | **不并入 S12**。两者在能力矩阵留痕为「S12 不交付」：`on_quorum_loss` 与 ADR-007 `:21`「失去多数派必须被拒绝或显式降级，不得静默成功」冲突（`reject` 档已正确），`broker_security` mnesia 接线属安全面、与集群无关 | ADR-021 `:70`（开关未实现、已移除配置项、当前行为等价 `reject`）、`:101`（mnesia 适配未接线、`users/0` 只读 env、配置项无消费者故不补空壳） |
| OQ-10 | S12 与 S9/S10/S11 的先后 | **依赖已满足，S12 可开工**。S9（身份表）、S10（WebRTC 信令）、S11（协议池筛选）均已收口；三者复用身份表/前端/端口，S12 不修改其任何已冻结裁决 | roadmap §0 阶段依赖图；S9-F/S10-D/S11-C 均已收口（`progress.md` 状态行）；S12 新增的是节点视图/健康/drain/再平衡，与 S9~S11 无代码耦合 |

---

## 1. 事实底座（可复用 / 不可绕过 / 端口依赖 / 上游裁剪）

### 1.1 可复用的既有内核面（S12 只接线、不改语义）

| 能力 | 入口 | 位置 |
|---|---|---|
| 分区归属读（本地镜像） | `broker_cluster:owner/1`、`epoch/1`、`replicas/1`、`snapshot/0` | `broker_cluster.erl:58-68`；缓存 `broker_cluster_cache`（本地 ETS，`cache.erl:58-62`，doc `:1-9`「Reads never go to ra」） |
| 归属写（ra 多数派 + epoch 单调） | `assign/4`、`migrate/4`、`finish_migration/2`、`remove/1` | `broker_cluster.erl:30-45`；状态机 `meta_machine.erl:29-100`（`assign/8` 在 `:85` 拒旧 epoch） |
| Fencing 守卫 | `authorize_append/2`、`guard_append/3`、`owner_check/1` | `broker_cluster.erl:80-124`；`{fenced,_}` 产生点 `:93-96`；已安装为 `{broker_queue, append_guard}` 钩子（`:133-147`） |
| 复制 | `broker_cluster_rep:replicate/2`（`Needed = N div 2 + 1`，`rpc:call` 并行，超时 2000 ms） | `broker_cluster_rep.erl:31-49`；**副本刻意不通知本地订阅者**（`:8-10`） |
| 节点健康/版本出口 | `/healthz`、`/readyz`、`/api/v1/status`（`node`/`uptime_seconds`/`partitions`/`cluster`/`metrics`） | `broker_admin_http.erl:44-50,85-124` |
| Prometheus 文本 | `/metrics`（62 键，无 `# TYPE`/`# HELP`，无标签，除 `le` 外无 label） | `broker_admin_http.erl:148-193`；键集 `broker_metrics.erl:284-394` |
| 订阅投递 | `broker_subscription` 本地 ETS + `Pid ! {broker_deliver,...}` | `broker_subscription.erl:222-233`；注册表 `broker_subscription_worker.erl:366-378` |

### 1.2 不可绕过的事实（负向发现）

1. **跨节点投递不存在。** `apps/*/src` 中 `pg`/`pg2`/`global`/`monitor_nodes`/`net_kernel` **0 处**（`net_kernel` 只在 `broker_cluster_dist_SUITE` 测试里）。投递一律本地 `Pid !`；副本写入刻意不通知本地订阅者。**A 节点订阅者收不到 B 节点 publish。**
2. **`broker_meta:reconcile/0` 不存在。** 真实的是 `broker_meta_config` 的 5 s 轮询（`reconcile_ms` 默认 5000，`broker_meta_config.erl:50`），只对账 `config_version` 计数，**不碰成员、不碰归属**（`:112-120`）。
3. **两份平行元数据互不相通。** ra 组（`broker_cluster` + `broker_cluster_cache`）与 Mnesia `partition_meta`/`cluster_node`（`broker_meta.erl:36-64`）**运行时 0 互相引用**；`put_partition/2`/`put_node/2` 只有测试调用者。`cluster_node.status` 被硬编码 `up`（`:216`），无状态迁移、无 down 标记。
4. **`/readyz` 查的是「app 已 loaded」而非「已 started」。** `started/1` 用 `application:get_application(App)`（`broker_admin_http.erl:131-140`），传原子等于查 `ac_tab` 的 `{loaded, App}`。**实测**：`application:load(broker_queue)` 后 `get_application(broker_queue) = {ok, broker_queue}` 而 `which_applications()` 不含它（`broker_queue` 已 stop）。app 崩溃但未 unload 时 `/readyz` 仍回 200 ⇒ R-HA2「`/readyz` 与 drain 状态一致」的前提不成立，S12-A 必须先修。
5. **`cluster_epoch` 只有 MQTT 打。** `grep -rln cluster_epoch apps/*/src/` 仅 `broker_cluster.erl` 与 `mqtt_conn_fsm.erl`。WS/NATS/AMQP/Kafka/Pulsar/CoAP/MQTT-SN/QUIC/WebRTC publish 不带 epoch ⇒ 只过 `owner_check/1`（仅所有权，非 epoch 校验，`broker_cluster.erl:106,109-110`）。R-CL2 的 epoch 校验**不是全协议生效**。
6. **`connections_current` 只覆盖 MQTT 系。** 仅 `mqtt_conn_fsm.erl:2212`（1883/8083/14567）与 `mqtt_sn_conn.erl:286,645`（1884）在写。WS/NATS/AMQP/Kafka/Pulsar/A2A/MCP/CoAP/WebRTC **一次都不写** ⇒ 该 gauge 不代表节点真实连接数（`broker_proto_ws`/`broker_proto_nats`/`broker_proto_amqp`/`broker_proto_kafka`/`broker_proto_pulsar`/`broker_proto_a2a`/`broker_proto_mcp`/`broker_proto_webrtc` 的 `broker_metrics` 调用为 0）。
7. **无 drain、无节点级容量闸门。** `apps/` 中 `prep_stop` 0 处、drain/quiesce 状态机 0 处；唯一闸门是 ranch per-listener `max_connections`（默认 1024，超限 acceptor 睡下、**无显式拒绝**）；唯一显式拒绝在 QUIC（`{error, too_many_connections}`，`mqtt_quic_listener.erl:210-213`）。
8. **无「节点视图 / weight / drain / capacity / up|down 状态机」。** `apps/broker_cluster` 与 `apps/broker_meta` 中 `weight`/`drain`/`capacity`/`monitor_nodes`/`nodeup`/`nodedown` **0 处**。
9. **`broker_meta` 实际 13 张表，文档写 11/12。** `agent_card`（S9-B/ADR-026 §D3）、`push_config`（S9-C/ADR-026 §D6）是 S9 追加；`docs/operations.md:640`、`progress.md:79`、S12 稿 `:47` 仍写 11/12，陈旧。

### 1.3 端口与依赖账本

| 项 | 现状 | S12 约束 |
|---|---|---|
| 管理面 | `broker_admin` 8090/TCP，`gen_tcp` + `{packet, http_bin}`，**回环默认**（`{127,0,0,1}`），**无 TLS**，token 空 = 无鉴权 | 节点视图契约加在 8090 既有路由上（`broker_admin_http.erl:44-81`），**不开新端口**；非回环绑定时必须拒空 token（当前仅文档约束 `:19-22`，无启动校验） |
| 数据面 listener | MQTT 1883、WS 8080、MQTT-WS 8083、MQTT-SN 1884/UDP、Kafka 9093、AMQP 5673、Pulsar 6650、NATS 4223（`config/sys.config.src`）；A2A/MCP/QUIC 默认 `[]`；CoAP/WebRTC 无 config 段（`.app.src` 默认 `[]`） | drain 通过**停止 accept** 实现（ranch listener 内嵌在 supervisor，停 app 即停 accept），**不改端口矩阵** |
| 集群面 | `broker_cluster.app.src` 仅 `data_dir`（`config/sys.config.src:490-497`），成员来自静态 env `broker_cluster.members`（默认 `[] → [node()]`，`broker_cluster_meta.erl:111-116`） | 再平衡器**不引入新端口、不引入新 app**；成员变更仍走 ra 写路径 |
| 依赖 | **不新增任何运行时依赖**。四仓（tcpbalance/gen_lb/gen_server_cluster/nkdist）的 `git://` 无固定 ref、OTP R9B/旧 API、`inets`/`httpd`、`lager` parse_transform 与 ADR-003 的 `vendor/` + `_checkouts/` 离线构建口径冲突 | R-N3 明文禁止引入任一仓代码/依赖；4 仓**只作设计参考**，§2.5 的 F-1~F-12 逐行写入能力矩阵（R-N8） |

### 1.4 上游四仓的工程事实（一律不引入代码）

| 仓库 | 核心结论 | 关键 F- 裁剪项 |
|---|---|---|
| `tcpbalance` | README 自述「最快空闲后端 / 感知多 CPU」**与源码不符**：first-fit + 手工 `maxconn`；选择是连接级一次性、无会话粘性；后端 down 是被动永久、无主动健康检查；全忙时 `wait_list` 无限排队 + 1 s 超时清理；OTP R9B-0（2003）；单例 `register(balance, self())` | F-1（能力矩阵不得写「最快/加权」）、F-2（连接级一次性）、F-3（被动 down）、F-4（无限等待队列）、F-9（单例）、F-11（全 down + 恢复竞态） |
| `gen_lb` | 连接级选择、无会话粘性；`wait_list` 超时即弃；退避 `2500 * 2^N` **无上限递归** | F-3（等待队列）、F-4（退避无上限）、F-5（半关闭不支持） |
| `gen_server_cluster` | 有状态 gen_server 主备；**借鉴**「只传更新函数」「link 随切换重建」；**不借鉴** `global` 竞争选举 | §2.4 生命周期事件（R-HA6 参考，非实现项） |
| `nkdist` | 进程分布；**借鉴** `start_and_join`/`join` 契约与本地缓存；**不借鉴**最终一致 / riak_core | F-1（不得引入 `chash`/`riak_core_apl`/`ring`）、F-2（handoff 不得无 Fencing 双写）、F-6（迁移三态）、F-8（handoff 格式版本号） |

### 1.5 `connections_current` 记账错误的具体范围（OQ-7 的修复对象）

`connections_current` / `connections_peak` 两个 gauge 的写入者：
- `mqtt_conn_fsm.erl:2211-2229`（CONNECT 成功时 +1，断开时 -1；`:2212-2220` 写 `connections_peak`）
- `mqtt_sn_conn.erl:141,286,645,684`

**未写入者**（即「节点真实连接数 > `connections_current`」的来源）：`broker_proto_ws`（`ws_conn.erl`）、`broker_proto_mqtt_ws`（`mqtt_ws_conn.erl`）、`broker_proto_nats`、`broker_proto_amqp`、`broker_proto_kafka`、`broker_proto_pulsar`、`broker_proto_a2a`、`broker_proto_mcp`、`broker_proto_webrtc`、`broker_proto_coap`。OQ-7 的修复 = 让这些 app 的连接生命周期也调 `broker_metrics` 的既有 `gauge_add/2`（`broker_metrics.erl:119-134`），**不新增键**。

---

## 2. OQ 逐条裁决与理由

### 2.1 OQ-1 — LB 形态：**外部 LB + Broker 8090 节点视图契约**

**裁决**：Broker 不内建四层代理、不做 DNS-SD、不做客户端侧路由。Broker 在既有管理面 8090 上供给**节点视图契约**（节点标识、角色、容量/权重、健康状态 `up|draining|down`、epoch），由外部 LB 消费。`/readyz` 作为 LB 的健康探针，drain 期间返回 503。

**理由**：
- R-N2 明文禁止四层透传；tcpbalance F-9（单例 `register(balance, self())`）、F-10（状态页实际不可用）证明 Broker 内建代理与 OTP 28 多节点部署不兼容。
- `docs/operations.md:530` 已隐含外部 LB 假设（「指标/管理面故障不应把 broker 摘出负载均衡」）。
- 8090 已是唯一节点身份出口（`/api/v1/status` 返回 `node`/`uptime_seconds`/`cluster`），扩展现有路由比开新端口更符合 R-E6（新增 listener 须有消费者、默认关闭）。
- 四仓全部不引入代码（R-N3）。

**代价/留痕**：外部 LB 的选型与配置不在本仓职责内；S12 只保证 Broker 供给的契约稳定（字段集合、状态枚举、HTTP 语义）。

### 2.2 OQ-2 — 节点视图真相：**ra 为唯一真相，退役平行视图表**

**裁决**：
- 节点视图的唯一真相 = `broker_cluster` 的 ra 状态机（`{partitions := #{Partition := #{owner, epoch, replicas, state, index}}}`，`meta_machine.erl:17-27`）+ 本地 `broker_cluster_cache` 镜像（读路径永不回 ra，`cache.erl:1-9`）。
- **R-CL3 的「复用 ADR-021 D1 的 `reconcile/0`」是伪命题**：`broker_meta:reconcile/0` 不存在，且不存在「两个副本需要对账」。要做的是：① 本地缓存与 ra 的 `snapshot_installed/5`（`meta_machine.erl:72-76`）重建路径已覆盖「缓存 vs 真相」对账；② **退役 Mnesia `partition_meta` / `cluster_node` 作为归属/成员视图**（生产无写入者、与 ra 0 互通）。
- 节点身份/成员：当前成员来自静态 env `broker_cluster.members`（`broker_cluster_meta.erl:111-116`）。S12-A 把「成员 = ra 组成员 + 已上报节点的 `up|draining|down`」收敛到 `broker_cluster_cache` 内的一个节点视图表（**不新增 Mnesia 表**，用 ra 状态机承载）。

**理由**：
- ADR-002 `:13` 与 ADR-021 D1 `:19-24` 的 11 张表清单**一致**（控制面元数据），但 `partition_meta`/`cluster_node` 与 ra 状态机**语义重叠**（都声称描述归属/成员）。两份平行、互不相通的元数据是 R-N6「不得新增第二套控制面元数据」的直接违反。
- 把节点视图放进 ra 状态机 = 复用既有 Fencing/epoch/多数派机制，不引入新共识（R-N6）。
- `cluster_node.status` 硬编码 `up`（`broker_meta.erl:215-216`）、无 down 标记、无消费者，证明它从未成为真相。

**代价/留痕**：Mnesia `cluster_node`/`partition_meta` 表**保留定义**（不删，避免破坏 ADR-002 表清单与既有测试），但标记为「S12 起不再是归属/成员真相，仅作遗留兼容」；`broker_meta:put_node/2`/`put_partition/2` 标为仅测试用。能力矩阵留痕。

### 2.3 OQ-3 — 再平衡触发者：**`broker_cluster` 内模块，默认关闭，手动命令可覆盖**

**裁决**：
- 触发者 = `broker_cluster` 内**新增再平衡模块**（不开独立进程池、不新增 app）。它读取 `broker_cluster_cache:all/0` 与节点视图，计算目标归属，调用既有 `assign/4`/`migrate/4`/`finish_migration/2`/`remove/1`。
- **默认关闭**：再平衡器不在节点 join/leave 时自动运行；仅运维命令（经 `broker_admin` 管理 API，R-LB8）显式触发。
- **手动覆盖优先**：运维命令指定的目标归属直接生效，再平衡器不与之争用。
- **目标函数 = 各节点分得的分区数均衡**（`count(partitions where owner = Node)`）。**不含连接数、不含内存、不含 CPU**——因为连接数记账不完整（§1.5）、内存/CPU 无指标（§1.2）。
- **稳定条件（不迁移的判据）**：相邻两次评估间，各节点分区数差 ≤ `hysteresis`（默认 1）**且**距上次迁移 ≥ `cooldown_ms`（默认 300000）时，不迁移。一次只迁移一个分区（一轮一个 `migrate`），避免抖动。

**理由**：
- ADR-021 `:109-110` 把「自动再平衡」明确留给 S12；`assign/4` 等已是唯一写入口且状态机内做 epoch 单调性检查，再平衡器只是新的**调用者**，不新增写路径。
- 目标函数只用「分区数」是因为这是唯一完整、权威、可确定性计算的维度（ra 状态机就是分区归属真相）。连接数加权在 §1.5 的记账错误修复前是假的。
- 默认关闭 + 手动触发，是把 ADR-021「不做自动再平衡」的保守性延续到 S12-C 落地：再平衡器是**可选工具**而非**自动行为**，避免运维事故。

**代价/留痕**：再平衡器默认关闭 ⇒ S12-C 交付后「自动均衡」能力存在但需显式启用；能力矩阵写「再平衡器已实现、默认关闭、手动触发」。

### 2.4 OQ-4 — 一致性哈希：**不引入，只搬 `(owner, epoch)`**

**裁决**：不引入 `chash` / `riak_core_apl` / `ring` / vnode / `nklib`（R-N1）。分区↔组的 G=16 映射保持运维配置不变。再平衡器在**同一 raft group 内的节点之间**选择新 owner（即「G 内换 owner」），不改变 `phash2(Destination, N)` 分区编号，不改变分区到组的映射。

**理由**：
- ADR-010 `:92` 冻结 `route/3` 为 `phash2(Destination, N)` 纯函数，迁移只改 `(owner, epoch)`、不改分区号，Router 无需要更新的映射。一致性哈希会重排分区归属映射，与 ADR-010 直接冲突。
- ADR-008 `:15-19`：G=16 是故障域折中；组数受资源预算约束（≤256 组/节点），超预算时增大组大小而非增组数；禁止大规模 1:1。一致性哈希的「加节点即重排」与「组数稳定」的运维模型冲突。
- R-RB3 / R-N1 明文禁止。

**代价/留痕**：再平衡粒度受 G=16 约束——单个 group 内 owner 切换的最小单元是 1 个分区；跨 group 的均衡需运维改配置（不在 S12 自动范围）。

### 2.5 OQ-5 — 可迁移有状态进程：**只有分区归属，不迁移活进程**

**裁决**：S12 迁移的**唯一对象**是分区归属 `(owner, epoch, replicas, state, index)`，复用既有两阶段 `migrate → finish_migration`（ADR-021 D3，`broker_cluster.erl:34-38`）。**不定义任何活进程的三态迁移契约**（R-RB5 标为「S12 不交付」），**不迁移** `broker_subscription` worker / 消费者注册表 / 游标 / 会话状态。

**理由**：
- ADR-019 `:126`：`assigned` 明确**不落盘**（「落盘会把它变成『崩溃前谁该收到』的第二份真相」），崩溃后必须全部重投；ADR-019 `:55` worker 是唯一分配点（gen_server 串行化 claim）。活的 `assigned` 没有合法的迁移对象。
- ADR-010 `:92`：Router 无自己的持久化状态，`route/3` 是纯函数，无需迁移。
- `broker_subscription` 消费者注册表是本地 ETS（`broker_subscription_worker.erl:366-378`），游标/会话是本地 flat file（`broker_subscription_files.erl:74-76`），**无跨节点复制对象**（R-HA4 的 WAL 复制只复制消息字节，`broker_cluster_rep.erl:8-10`）。
- R-HA4 要求「有状态服务的状态必须落在 WAL」——分区 WAL 已在 `broker_queue_partition.erl` 复制，迁移的是**分区归属**（即「哪个节点的 WAL 是权威」），不是进程。

**代价/留痕**：节点故障/再平衡时，该节点上的消费者会话**不迁移**，由客户端重连到目标节点后按 ADR-019 重建（`assigned` 重投）。R-RB5 / R-RB6 在能力矩阵标「S12 不交付：活进程迁移与 handoff 版本号」。R-CL4 的「在途消息」按 §2.6 处理。

### 2.6 OQ-6 — 迁移期间连接与在途消息：**连接重连 + 三档写可用性矩阵**

**裁决**：
- **连接**：一律由客户端重连。drain 只停止 accept、不迁移连接（无「迁移连接」概念，R-N2）。成员变更/再平衡/升级期间，外部 LB 通过 `/readyz` 503 把新连接导向其他节点；在用连接**不被强制断开**（除非该节点宕机，由客户端 keepalive/超时发现）。
- **在途消息（写可用性矩阵，绑定 ADR-007 三档）**：

| 可靠性档 | 迁移期间（旧 owner 仍在、epoch 已 +1） | 旧 owner 宕机（无法再服务） |
|---|---|---|
| **L1 `os_buffer`** | 写**仍被接受**（QoS0 不进复制器，`broker_cluster.erl:106` 走 `owner_check/1`；若本地非 owner 则按当前归属路由） | **可丢**（进程崩溃丢失窗口，ADR-007 `:15-19`） |
| **L2 `fsync`** | 写**成功**：目标 owner 先本地 fsync，按 ADR-021 D5 返回 `{ok, Offset}`（迁移期间目标成为 owner，`:57-58`） | **不丢**：分区 WAL 已 fsync（含副本 fsync，R-HA4） |
| **L3 `replicated_commit`** | 写**成功**：经 `replicate/2` 收齐 `quorum = N div 2 + 1` 个副本（含本地 fsync）才 ACK；旧 owner 已被 `{fenced,_}` 拒绝（`broker_cluster.erl:83,93-96`） | **不丢**：多数派副本仍在（ADR-007 `:19`） |
| 失去多数派（任何档） | **拒绝**：`replicated_commit` 返回 `{error, {replication_failed, ...}}`，**不得 ACK**（ADR-021 `:79-80`、ADR-007 `:21`） | 同左 |

**理由**：ADR-021 D3 已冻结两阶段迁移与 Fencing；ADR-007 已冻结三档 ACK 时机；R-HA3 禁止「零停机/自动故障转移」含糊表述。把「迁移」与「宕机」分开列，是因为 L1 在两种场景下语义不同（迁移期间 QoS0 可路由、宕机时丢失窗口），这是 ADR-007 丢失边界的直接推论。

**代价/留痕**：R-RB5 的「三态进程迁移」与 R-RB6 的「handoff 格式版本号」不交付（见 §2.5）；客户端需实现重连 + 在途消息重投（ADR-015 既有口径）。

### 2.7 OQ-7 — 连接计数落点：**复用既有键、修记账、不新增键**

**裁决**：不新建 app、不新增指标键。容量控制所需的 active/pending 连接数**复用** `connections_current` / `connections_peak`（ADR-016 已冻结）。R-LB9 的「每节点 active/pending、等待队列长度、被拒连接数、节点上下线次数、迁移次数」**不在 S12 交付**（新增键须改 ADR-016 + `broker_metrics_SUITE` 的 `keys_are_stable`，`broker_metrics.erl:281-283`、`ADR-016:62,93,98`）。

**理由**：
- ADR-016 `:62` 键必须编译期/启动期登记；`:93` 新增指标要改代码（故意，防高基数）；`:98` 集合被 `keys_are_stable` 钉死。S12 不是改冻结键集的合适阶段。
- 「修 `connections_current` 记账」（让 WS/NATS/AMQP/Kafka/Pulsar/CoAP/WebRTC 也调 `gauge_add/2`）是**修正确性**，键不变、集合不变，不需要改 ADR-016。
- OQ-7 的「是否进控制面」：连接计数是**数据面观测**，不进 ra 状态机（不触发 Fencing/再平衡，见 §2.3 目标函数只用分区数）。

**代价/留痕**：R-LB9 在能力矩阵标「S12 不交付：扩展指标键集（须独立走 ADR-016 修订）」。

### 2.8 OQ-8 — Broker 内部排队：**不做，全部显式拒绝**

**裁决**：Broker 内部**不做连接级排队**。容量超限**显式拒绝**：TCP listener 补一个**节点级计数闸门**（在 ranch per-listener `max_connections` 之上，跨 listener 汇总），超限即拒绝新连接（返回连接关闭 + 可选 `cluster_rejected`-语义，但**不新增指标键**——用既有 `auth_failures`/`protocol_errors` 之外的既有键或仅关闭）。「无可用节点」由外部 LB 与客户端重试消化，不在 Broker 内排队。

**理由**：
- 唯一容量闸门是 ranch per-listener `max_connections`（默认 1024，超限 acceptor 睡下、无显式拒绝，`ranch_conns_sup.erl:269-275`）；tcpbalance F-4 / gen_lb F-3（无限等待队列）是反面教材，R-LB5 明文禁止 `gen_server:call(..., infinity)` 式无限阻塞。
- QUIC 已示范显式拒绝（`{error, too_many_connections}`，`mqtt_quic_listener.erl:210-213`）。
- ADR-016 已有 `conn_mailbox_len`/`conn_inflight`/`backpressure_pauses`/`backpressure_disconnects` 键（`:363-373`）覆盖慢消费者背压，Broker 内部排队与既有背压口径重叠。
- 队列长度恒为 0 ⇒ R-LB9 的「等待队列长度」指标无对象（与 §2.7 一致，不交付）。

**代价/留痕**：TCP 侧显式拒绝的**具体实现**（节点级计数闸门放哪——建议放 `broker_cluster` 内的连接计数模块，或各 listener 自报既有 gauge）在 S12-B spike 定；能力矩阵写「Broker 不内部排队，超限显式拒绝」。

### 2.9 OQ-9 — ADR-021 遗留项：**不并入 S12**

**裁决**：`{on_quorum_loss, reject | os_buffer_only}` 运行时开关与 `broker_security` 的 mnesia 接线**不并入 S12**，在能力矩阵留痕为「S12 不交付」。
- `on_quorum_loss`：ADR-021 `:70` 已说明开关未实现、配置项已移除（无消费者）、当前行为等价 `reject` 档；ADR-008 `:35` 把它描述为「L3 不可达时降级到 L1/L2 或拒绝」的意图，但 ADR-021 `:70` 明确「补上策略开关列为 S6 之后」。**与 ADR-007 `:21`「失去多数派必须被拒绝或显式降级，不得静默成功」冲突**：`reject` 档已正确，`os_buffer_only` 档若实现即引入「静默降级」，需独立评估。S12 不碰。
- `broker_security` mnesia 接线：ADR-021 `:101` 已说明未接线、`users/0` 只读 env、`{source, mnesia}` 配置项从未存在（无消费者故不补空壳）。属安全面、与集群无关，S12 不碰。

**理由**：R-HA5 是 P1「应补齐…若本阶段不做，须在能力矩阵留痕」；S12 范围（HA/集群/LB）与这两项无耦合，强行并入会扩大 S12 面、且与 ADR-007/ADR-021 的既有裁决冲突。

**代价/留痕**：能力矩阵两行「S12 不交付」；ADR-021 `:70,101` 的「列为 S6 之后 / S7 之后」保持不变。

### 2.10 OQ-10 — S12 与 S9/S10/S11 先后：**依赖已满足，S12 可开工**

**裁决**：S12 在 S9/S10/S11 全部收口后开工，**无顺序冲突**。roadmap §0 阶段依赖图 `S11 → S12` 满足。S12 新增的节点视图/健康/drain/再平衡与 S9（身份表）、S10（WebRTC 信令）、S11（协议池筛选）无代码耦合。

**理由**：S9-F/S10-D/S11-C 均已收口（`progress.md` 状态行、main 提交）；三者复用身份表/前端/端口，S12 不修改其任何已冻结裁决（ADR-026/028/029/030/031）。S12 复用的是 S4 的 `broker_cluster`（ADR-021）与 S6 的 `broker_admin`（8090），这些早已冻结。

**代价/留痕**：无。roadmap §5.1 的「S9 → S10 → S11 → S12」顺序确认。

---

## 3. ADR-032 冻结内容（2026-11-17 签认并落盘）

正式文本：[`docs/adr/032-s12-node-view-drain-and-rebalance-boundary.md`](../adr/032-s12-node-view-drain-and-rebalance-boundary.md)。九条决策与本文 OQ 的对应关系：

| ADR-032 | 标题 | 对应 OQ |
|---|---|---|
| **D1** | 节点视图唯一真相 = ra 状态机 + 本地 ETS 镜像；Mnesia `cluster_node`/`partition_meta` 退役为遗留视图；**节点自身 `up\|draining\|down` 存本地、不进 ra** | OQ-2 |
| **D2** | drain 状态机由管理 API 驱动；先修 `/readyz` 的 loaded→started 缺陷；`draining` 期间 `/readyz` 503、`/healthz` 200；非回环 + 空 token 拒启 | R-HA1/R-HA2/R-LB8 |
| **D3** | 外部 LB + 8090 供给节点视图契约；不做四层代理 / DNS-SD / 客户端侧路由；不开新端口 | OQ-1 |
| **D4** | 再平衡器 = `broker_cluster` 内新模块；默认关闭、手动可覆盖；目标函数 = 分区数均衡；滞后 + 冷却双门限；一次一轮 | OQ-3 |
| **D5** | 只迁移分区归属；不迁移活进程（R-RB5/R-RB6 不交付）；不引入一致性哈希、不改 `phash2` 分区编号 | OQ-4/OQ-5 |
| **D6** | 迁移/故障写可用性矩阵绑定 ADR-007 三档；失去多数派一律拒绝且不 ACK；`cluster_epoch` 仅 MQTT 生效需如实收窄 | OQ-6/R-HA3 |
| **D7** | 不内部排队、超限显式拒绝；指标复用既有键、只修 `connections_current` 记账、不扩容键集；R-LB9 扩展项不交付 | OQ-7/OQ-8 |
| **D8** | `on_quorum_loss` 开关与 `broker_security` mnesia 接线不并入 S12 | OQ-9 |
| **D9** | 跨节点投递不在 S12 范围；G2 达成口径收窄为「多节点接入 + 归属可迁移 + LB 分发 + 会话粘性」；跨节点投递单列 S12-D 并须新增 ADR | OQ-10 + §4.1 |

**关键后果（正式文本「后果」节）**：① S12-A 必须先修 `/readyz`；② 失去多数派时仍可 drain（这正是把节点生命周期留在本地的理由）；③ 再平衡器默认关闭 ⇒ S12-C 交付「能力 + 开关」而非「自动均衡」；④ **跨节点投递仍不存在** ⇒ G2 不是「一个逻辑 Broker」；⑤ `broker_meta` 表数从文档 11/12 修正为 **13**。

> **④ 是本次收敛最重要的留痕**：G2「多节点作为同一个 Broker」（roadmap §14 目标、K1~K8）在 S12 交付后**仍不成立跨节点投递**——A 节点订阅者收不到 B 节点 publish（§1.2 第 1 条）。S12 交付的是「单节点接入 + 分区归属迁移 + LB 契约 + drain + 再平衡器」，**不是**「跨节点消息路由」。
---

## 4. 已裁决（2026-11-17，两处均取推荐项）

### 4.1 跨节点投递：**裁决 = 选项 A**（S12 不含跨节点投递，单列 S12-D）

**背景**：§1.2 第 1 条 + §3 后果 ④：跨节点投递**完全不存在**（无 `pg`/`global`/`monitor_nodes`，投递一律本地 `Pid !`，副本刻意不通知本地订阅者）。这导致 G2「多节点作为同一个 Broker」**无法**仅靠节点视图 + LB + 会话粘性成立——节点视图 + LB 只解决「新连接路由到哪个节点」与「分区归属在哪个节点」，不解决「A 节点 publish、B 节点订阅者能否收到」。

**裁决**：**选项 A**。S12 收缩为「单节点接入 + 分区归属迁移 + LB 契约 + drain + 再平衡器」，能力矩阵如实写「跨节点投递未做（S12-D）」。G2 的达成口径改为「多节点接入 + 分区归属可迁移 + 外部 LB 分发 + 会话粘性」，**不含**「跨节点消息路由」。跨节点投递单列 **S12-D**（复用分区 owner + 订阅者节点注册表 + 同一内核），并**新增 ADR**——订阅者节点注册表本质是「消费者在哪个节点」的索引，会触及 ADR-010「Router 只持有 `Filter → [SubRef]` 倒排索引、不持有消费者状态」的冻结边界，必须显式修订。

**理由**：跨节点投递是真正的架构增量（订阅者节点注册表 + 跨节点扇出 + ADR-010 边界重划），与「节点视图/drain/再平衡」不是同一复杂度量级；塞进 S12 会让 S12-A/B/C 全部失焦。

**代价**：G2 的完整承诺延后到 S12-D；S12 交付后「多节点 Broker」仍是「多节点接入 + 分区可迁移」，不是「一个逻辑 Broker」。已固化为 **ADR-032 D9**。

**未采用选项（留档）**：

- **选项 B（S12 含 S12-D）**：G2 在 S12 内闭环，但 S12 面显著扩大、S12-A/B/C 节奏被牵制、须同期修订 ADR-010。
- **选项 C（暂缓 S12）**：若 G2 的核心是「一个逻辑 Broker」，则节点视图/drain/再平衡都是为它服务，先做基础设施层是倒置。

### 4.2 是否扩 ADR-016 键集：**裁决 = 选项 A**（不扩键集，走独立 ADR-016 修订）

**背景**：OQ-7 收敛为「复用既有键、不新增键」。但 R-LB4（容量控制）与 R-LB9（被拒连接数、节点上下线次数、迁移次数）在 S12-B 落地时**没有对应键**。

**裁决**：**选项 A**。不扩键集。容量控制用既有 `connections_current` + 节点级计数闸门实现（超限显式拒绝；拒绝即关闭连接，由外部 LB 侧计数）。R-LB9 的扩展项在能力矩阵标「S12 不交付」；键集扩展作为一次**独立的 ADR-016 修订**（连同 `keys_are_stable` 一起改），与 S12 解耦。

**理由**：ADR-016 `:62,93,98` 的键集冻结是有意的（防高基数）；S12 不是打破冻结键集的合适阶段。

**代价**：S12-B 的容量控制与 S12-C 的再平衡**可观测性不完整**（无被拒计数、无迁移计数）。已固化为 **ADR-032 D7**。

**未采用选项（留档）**：

- **选项 B（S12 内一次性扩键）**：一次改冻结集比两次省事，但须同期改 `broker_metrics` + `broker_metrics_SUITE` + ADR-016 + 文档，S12 面扩大且主动打破键集稳定性。


## 5. 风险与反证点

| # | 风险/反证 | 应对 |
|---|---|---|
| 1 | **跨节点投递不存在**（§1.2-1）可能被「S12 交付即 G2 达成」误读 | §4.1 已裁决 S12 不含跨节点投递；ADR-032 **D9** 写清「S12 不交付跨节点投递，单列 S12-D」 |
| 2 | `/readyz` loaded/started 缺陷（§1.2-4）使 R-HA2 前提不成立 | S12-A **第一个** commit 先修 `started/1`（用 `application:which_applications/1` 的 started 判定或 `application:get_application/1` 不传原子改用 started 列表），再叠加 drain |
| 3 | `connections_current` 记账不完整（§1.5）使「按连接数加权」是假的 | OQ-3 目标函数只用分区数（不用连接数）；OQ-7 修记账但不新增键；能力矩阵留痕「连接数曾不完整、S12-B 修复」 |
| 4 | 再平衡器默认关闭（OQ-3）可能被误读为「S12-C 未交付自动均衡」 | §2.3 + §3 后果 ③ 写清「再平衡器已实现、默认关闭、手动触发」；S12-C 交付的是「能力 + 开关」，不是「自动行为」 |
| 5 | `cluster_epoch` 只 MQTT 打（§1.2-5）使 R-CL2 的 epoch 校验不是全协议 | 能力矩阵留痕「epoch 校验仅 MQTT 生效；其余协议只过 `owner_check/1`（所有权，非 epoch）」；S12 不把 R-CL2 表述为「全协议 epoch 校验」 |
| 6 | `broker_meta` 13 张表 vs 文档 11/12（§1.2-9） | 本文 §1.2-9 + ADR-032 后果修正口径；S12-A 同步 `docs/operations.md:640`/`progress.md:79`/S12 稿 `:47` |
| 7 | 两份平行元数据（ra vs Mnesia）若不同步会脑裂 | OQ-2 裁决「ra 唯一真相，Mnesia 视图表退役」；S12-A 删除 `put_partition/put_node` 的生产语义（标测试-only） |
| 8 | 四仓能力矩阵逐行留痕（R-N8）易漏 | §1.4 已抄 F-1~F-12；ADR-032 落盘时把 4 仓 §2.5 全部 F- 条目写入能力矩阵与限制清单（S8 惯例） |
| 9 | drain 只停 accept 不迁移连接（OQ-6）可能在节点宕机时丢在用连接 | §2.6 写清「连接由客户端重连」；ADR-015 既有在途消息重投口径；R-CL4 的「不得中断在用连接」解释为「正常 drain 不中断，宕机由客户端重连发现」 |

---

## 6. 下一子阶段入口条件（ADR-032 已签认，S12-A 可开工）

| 子阶段 | 范围 | 入口条件 |
|---|---|---|
| **S12-A** | 节点视图真相（ra 状态机承载 + 退役 Mnesia 视图表）、`/readyz` 修正（**前置**）、drain 状态机（`up\|draining\|down` + `/readyz` 一致）、节点上下线管理 API（R-LB8） | **已满足**：ADR-032 已签认；§4.1 已裁决 S12 不含跨节点投递。开工第一个动作 = 修 `started/1`（ADR-032 D2） |
| **S12-B** | 外部 LB 节点视图契约（8090 路由）、容量控制（节点级计数闸门 + 显式拒绝，R-LB4/R-LB5）、可插拔节点选择策略（R-LB2/R-LB3）、「无可用节点」竞态用例（R-LB6）、`connections_current` 记账修复 | S12-A 交付；§4.2 已裁决不扩 ADR-016 键集 |
| **S12-C** | 再平衡器（`broker_cluster` 内模块，默认关闭，分区数均衡，滞后+冷却）、两阶段迁移（复用 ADR-021 D3）、写可用性矩阵落地（绑定 ADR-007 三档）、R-RB5/R-RB6 留痕 | S12-B 交付；OQ-3 目标函数与阈值已由 ADR-032 D4 冻结 |
| **S12-D**（单列，未开工） | 跨节点 publish→subscribe | 需新增 ADR（订阅者节点注册表 + 跨节点扇出 + ADR-010 边界重划）；**不在 S12-A/B/C 范围**（ADR-032 D9） |

**S12-0 出口（已达成）**：§2 十项结论表已签认、ADR-032 已落盘 `docs/adr/032-s12-node-view-drain-and-rebalance-boundary.md`、§4 两处裁决已固化。**S12-A 可开工**，第一个动作是修 `started/1`（`/readyz` loaded→started），再做 drain 状态机。

**本阶段未做的文档同步（有意留到 S12-A）**：roadmap §14 的子阶段状态、`progress.md`、README 状态行、`docs/architecture.md` / `docs/operations.md` / `docs/security.md` / `docs/protocol-compatibility.md` 的 S12 相关行——按 S12 需求稿 §5.5 第 3 条的既有决定，等 S12-A 有首个代码交付时统一同步，避免把「计划」读成「现状」。唯一例外：`broker_meta` 表数 11/12 → 13 的陈旧口径须在 S12-A 一并修正。

---

**一句话总结**：S12-0 把 10 个 OQ 收敛成 10 条可冻结裁决——外部 LB + 8090 节点视图契约、ra 为唯一真相（退役 Mnesia 视图表）、再平衡器默认关闭且只搬 `(owner,epoch)`、不迁移活进程、Broker 不内部排队、不新增指标键、ADR-021 遗留项不并入；**跨节点投递不存在**是本次收敛暴露的最大缺口，S12 交付的是「多节点接入 + 分区归属迁移 + LB 契约 + drain」，**不是**「跨节点消息路由」——后者单列 S12-D。两处裁决（跨节点投递范围 §4.1、是否扩 ADR-016 键集 §4.2）均取推荐项，分别固化为 ADR-032 D9 与 D7。
