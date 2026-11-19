# ADR-034：S12-D 本体——跨节点消息投递——owner 权威日志 + 跨节点读写路由（默认关闭）

- **状态**：提案（待评审签认，2026-11-19）——**尚未签认**，本文件不构成冻结裁决
- **日期**：2026-11-19
- **关联**：ADR-032 D9（跨节点投递不在 S12，单列 S12-D 且须新增 ADR）、ADR-033（S12-D 前置·成员在线变更，已交付）、ADR-010（`broker_router` 行为——**本 ADR 不修订其冻结正文**）、ADR-011（`broker_storage` 行为——读/写入口不变）、ADR-013（订阅类型与 cursor 归属——**不变**）、ADR-014（retention「永不删过 `committed`」——**规则不变**，D6 只定义跨节点下 `committed` 的取法）、ADR-016（可观测最小集，冻结键集——不扩容）、ADR-019（Shared 原子 claim 与会话本地——**shared 跨节点不交付**，正文不修订）、ADR-021（集群控制面、Metadata Raft 与 Fencing——复用，不新增共识）、ADR-007（显式可靠性三档——可用性与丢失边界绑定它）、ADR-009（`broker_message` 记录——不变）
- **关联文档**：[`docs/design/s12d-cross-node-delivery-convergence.md`](../design/s12d-cross-node-delivery-convergence.md)（缺四件、A~E 五条互斥路子、ADR-010/019/021 冻结冲突）、[`docs/design/s12-infrastructure-ha-and-lb.md`](../design/s12-infrastructure-ha-and-lb.md)（§4 R-*、§5.3 Gate）、[`docs/roadmap.md`](../roadmap.md) §14
- **事实来源**：仓库内代码逐行核对（`apps/broker_queue/**`、`apps/broker_subscription/**`、`apps/broker_cluster/**`、`apps/broker_router/**`、`apps/broker_storage/**`、`apps/broker_proto_mqtt/**`），行号见 §背景；不引用任何上游 README 的性能/规模自述

## 背景（Context）

ADR-032 D9 冻结了两件事：①跨节点 publish→subscribe 在本仓**不存在**；②若要做，必须**新增 ADR**，且不得写第二套消息内核、不得新增 app / 端口 / 运行时依赖 / 指标键。ADR-033 已交付前置「成员在线变更」，并在立项记录中写明**产品侧确认 G2 终态 =「一个逻辑 Broker」**——因此收敛调查 §3 的路子 **E（不做跨节点投递）被排除**，S12-D 本体必须开工。本 ADR 负责在 A~D 之间裁决。

裁决前必须先钉死「为什么不存在」，全部是当前代码事实：

| 层 | 事实 | 位置 |
|---|---|---|
| **写** | 发布进程在**本地** `broker_router_hash:route/3` 上取 `[{Partition, Refs}]`，然后 `broker_queue:append(Partition, Msg, fsync)` 写**本节点** WAL。15 处发布调用点分布在 14 个模块（`broker_event`、`mqtt`、`amqp`、`kafka`、`nats`、`pulsar`、`ws`、`coap`、`mqtt_sn`、`mcp`、`a2a`×2、`acp`、`mqtt_will`、`broker_dlq`），**全部经 `broker_queue` 门面** | `broker_proto_mqtt/src/mqtt_conn_fsm.erl:752,833`；`broker_queue/src/broker_queue.erl:123` |
| **归属** | 非 owner 的写**已被拒绝**，不是静默本地写：`append_guard/3` → `authorize_append/2` 回 `{error, {fenced, _}}`，且计入 `cluster_fencing_rejected` | `broker_queue.erl:193`；`broker_cluster/src/broker_cluster.erl:113-132,135-141` |
| **读** | 连接进程在**自己的进程里**直接 `broker_queue:read_from/3` / `watermarks/1`；非 owner 节点的同名分区段是空的（每节点都按 `partitions` 建全部段），读到的不是「错误」而是**空页**——这是最危险的一种静默失败 | `broker_queue.erl:215,270`；`broker_proto_amqp/src/amqp_conn.erl:657`；`kafka_conn.erl:605,615`；`mqtt_conn_fsm.erl:1856-1859` |
| **唤醒** | `broker_queue:subscribe(P, Pid)` 把 pid 注册进**本地** partition worker；worker `monitor(process, Pid)`，append 后 cast `{new_messages, P, From, To}` 给每个订阅者。wake 只是提示，真实位置是游标（J6） | `broker_queue_partition.erl:66-78,87-89`；`mqtt_conn_fsm.erl:209,236-240` |
| **扇出** | `broker_subscription:dispatch/2` 查**本地** `?ATTACHMENTS` ETS 后 `Pid ! {broker_deliver, ...}`；durable 位置型的 payload **不被读取**，只当 wake 用 | `broker_subscription/src/broker_subscription.erl:222-233` |
| **复制** | `replicate/2,3` 只在 `replicated_commit` 档把同一消息 `rpc` 到副本集并等多数派；follower 用 `append_replica/3` 写，**不 notify、不 guard、不服务读** | `broker_cluster_rep.erl:21,28,59`；`broker_queue.erl:143` |
| **迁移** | `migrate` **只改** `(owner, epoch, replicas, state=migrating)`，**无字节 handoff** | `broker_cluster_meta_machine.erl:35-36` |
| **retention** | `committed` = `min(活游标) - 1`，GC **永不删过** `committed`；游标是本地文件 / 本地 ETS | `wal_gc.erl:14-16`；`broker_subscription_worker.erl:800` |

**直接推论**：A 节点的订阅者游标挂在 A 的 WAL 段上，B 节点的 publish 写进 B 的 WAL 段；两段从不交换字节，默认 `replicas = [node()]` 时连副本都没有。收敛调查 §2 把它拆成缺四件：①写入路由；②订阅者节点索引；③跨节点扇出 + 全局游标；④迁移时的在途一致性。

## 决策

### D1. 终态形状 = owner 权威日志 + 跨节点读写路由（D′）；不做全量复制、不做 owner 侧代理、**不建订阅者节点索引**

**一句话**：一个分区只有一份权威日志（在 owner 的 WAL 上）；任何节点上的 publish 都写到 owner，任何节点上的订阅者都从 owner 读。消息不复制到订阅者节点，订阅者「过去」。

- **对缺四件的回答**：①写入 → 转发到 owner（D2）；②**不需要**订阅者节点索引——读是订阅者主动 pull，owner 只管日志，不管谁在哪；③**不需要**跨节点扇出与全局游标——offset 空间本来就唯一（只有 owner 追加），游标仍是本地真相（D3/D5）；④迁移 → 复用 ADR-021 的 epoch fencing + ADR-032 D5 的元数据两阶段（D7）。
- **与收敛调查 §3 各路子的关系**：

| 路子 | 本 ADR 的取舍 |
|---|---|
| **A. 连接粘性即一切** | 不做。那是「分区 sharding」，与 ADR-033 立项记录「G2 终态 = 一个逻辑 Broker」矛盾 |
| **B. 全量复制 + 本地扇出** | 不做。写放大 = 节点数 × 写入；要把消息复制到「持有订阅者的节点」就必须先有订阅者节点索引，直接撞 ADR-010 冻结边界；cursor 跨节点对账会催生第二份游标真相 |
| **C. owner 侧扇出代理** | 不做。owner 成为扇出瓶颈与单点；远程代理是有状态连接，迁移时要重挂；远程订阅者注册表撞 ADR-019「消费者注册表本地 ETS」 |
| **D. 显式 sharding + 订阅重定向** | **部分采纳**：复用其余部分（节点视图、drain、再平衡、epoch），但**不把重定向暴露给客户端**（ADR-032 D3 已冻结「不做客户端侧路由」）。重定向发生在 `broker_queue` 门面内部，协议字节不变 |
| **D′（本 ADR）** | owner 权威日志 + 服务端读写路由 + 订阅者 pull。**不新增任何全局索引**，ADR-010/019 冻结正文不改 |
| **E. 不做** | 已被产品侧否决（ADR-033 立项记录） |

- **为什么不碰 ADR-010**：`broker_router` 仍只持有本地 `Filter → [SubRef]` 倒排索引、`route/3` 仍是纯函数、仍不持有消费者状态。跨节点部分既不进 Router，也不给 Router 加「节点」维度——它靠「谁订阅谁自己来读」实现，而不是「把消息推给订阅者所在节点」。
- **代价（如实）**：owner 成为该分区的**读热点**（所有节点的订阅者都向它拉），owner 出口带宽随跨节点订阅者数线性增长；跨节点读写各多一跳 rpc 延迟。这是用「读放大」换「不做全量复制、不建索引」。B 路线是相反的交换。

### D2. 写侧：`broker_queue:append/3` 单点路由；非 owner 转发到 owner，失败一律拒绝且不 ACK，**本地不留写**

- **落点必须唯一**：15 个发布调用点已经全部经过 `broker_queue:append/3`（`broker_queue.erl:123`），因此路由只在这一处接线，**适配器零改动**。
- **新增钩子，而不是改 `append_guard` 语义**：沿用既有 `persistent_term` 钩子模式（`{broker_queue, append_guard}` / `{broker_queue, replicator}`，`broker_queue.erl:193,199`；`broker_cluster.erl:164-172`），新增第三个键 `{broker_queue, append_router}`，签名 `fun(Partition, Msg, Mode) -> local | {forward, node()} | {error, term()}`。`append/3` 的顺序变为：`limits → append_router → (local) append_guard → append_checked`。
  - **为什么不把「转发」塞进 `append_guard`**：guard 是 ADR-021 的**授权/拒绝**语义（epoch fencing）。把它改成「有时转发」会让 fencing 在 owner 端失去执行者；而且 guard 的返回值被 `authorize_append/2` 的调用方（`mqtt_conn_fsm` 的 epoch 分支）依赖。两种职责必须分开。
- **`{forward, Node}` 分支**：`rpc:call(Node, broker_queue, append_forwarded, [Partition, Msg, Mode], Timeout)`。owner 端 `append_forwarded/3` 走**完整本地 `append/3`**（limits → guard → `fsync` / `replicated_commit` 落盘），因此：epoch 校验照旧、复制语义照旧、落盘档位照旧。**「转发」不是旁路，只是把执行位置换到了 owner。**
- **无双重写是机械保证**：`{forward, _}` 分支直接返回，**不调用 `append_checked/3`**，本节点不消费 offset、不写 WAL、不 notify。这是「非 owner 写被拒绝」到「非 owner 写被转发」之间唯一允许的变化。
- **失败语义**：owner 不可达 / rpc 超时 / `{fenced, _}` / 复制失败 → `{error, {cross_node_append_failed, Node, Reason}}` 或原错误透出。适配器既有的「append 失败就不 ACK / 断开」行为（`mqtt_conn_fsm.erl:833-870`）直接生效，**不新增第二种拒绝路径**。不本地缓冲、不重试、不排队（ADR-032 D7「不做内部排队」）。
- **重复窗口如实声明**：rpc 超时但 owner 实际已落盘 → 发布方收到错误、不 ACK、客户端重试 → **重复**。QoS1 本就允许重复；QoS2 的 `received` 去重窗口是每连接的（`mqtt_conn_fsm` `remember_received`），跨节点不共享。不得声称「跨节点 exactly-once」。
- **配置前提**：`Mode` 由发布方按本地 `broker_queue:reliability/0`（`:248`）选择，owner 端按 `append_checked/3` 既有规则执行。两节点 `reliability` 配置不一致属运维错误，本 ADR 不加协商。
- **fencing 纵深**：路由基于本地 cache 的 owner（`broker_cluster_cache.erl:66`），cache 可能滞后。滞后的转发到达一个已不是 owner 的节点时，该节点 `append_guard` 回 `{fenced, _}` → 不写、不 ACK。**宁可失败，不可双写。**

### D3. 读侧：`read_from/3`、`watermarks/1`、`advance_committed/2`、`capability/1`、`subscribe/2`、`unsubscribe/2` 路由到 owner

- **同一个钩子面**，第二个键 `{broker_queue, read_router}`，签名 `fun(Op, Partition, Args) -> local | {forward, node()} | {error, term()}`，`Op ∈ read_from | watermarks | advance_committed | capability | subscribe | unsubscribe`。`local` 时走现有代码路径，逐字不变。
- **为什么 `watermarks` 必须路由**：`broker_subscription_worker` 用 `watermarks` 锚定新订阅的尾部、并在多处算 committed（`:310,480,664,830,927`）。非 owner 节点上它读到的是空段的 `(0,0)`，订阅者会「永远追不上」而不是报错——必须路由，不能依赖本地回退。
- **为什么 `advance_committed` 必须路由**：它是 owner WAL 的 committed 水位，写进非 owner 的段没有意义（D6 另述跨节点聚合）。
- **`subscribe/2` / `unsubscribe/2` 路由后 wake 天然跨节点**：owner 的 `broker_queue_partition` 把远程 pid 存进 `subs` 并 `monitor(process, Pid)`；`Pid ! {new_messages, ...}` 与 `DOWN` 在 Erlang 分布式下原生可用。**不需要新的唤醒机制、不需要新进程、不需要订阅者节点索引**——「谁订阅了这个分区」本来就在 owner 的 worker 里。
- **wake 不承担正确性**：dist 断开时 `!` 静默丢弃、wake 丢失；订阅者靠既有 poll tick（`mqtt_conn_fsm.erl:1570-1574`，默认 1s）从自己的游标重新拉。J6 不变：wake 是提示，游标是真相。
- **读的语义不变**：`read_from` 返回的是 owner 日志的 `{Offset, Msg}`；因为只有 owner 追加，**每分区 offset 是单一单调序列**，游标可跨节点解释。这是 D′ 成立的核心论据。
- **读失败不自动回退**：owner 不可达 → `{error, {owner_unavailable, Node}}`；**不**回退到旧 owner、**不**读本地陈旧段。调用方按既有 `{error, _}` 分支降级（本轮 idle、下个 tick 重试），不崩、不猜测。
- **`capability/1` 也路由**：能力由 owner 的 backend 决定；该调用只在订阅/连接建立时发生并被缓存，成本可忽略。

### D4. QoS0：广播到当前成员，各节点**本地** `route/3` + `dispatch/2`

- QoS0 **不进日志**（§4.4 不变，`mqtt_conn_fsm.erl:821-829`），因此没有 owner 日志可 pull，D3 的读路由对它无效。
- 做法：发布节点完成本地 dispatch 后，把消息**广播给当前成员集**（`broker_cluster_meta:members/0`，ADR-033 已交付）里**除自己之外**的节点；每个节点用**自己的**本地路由索引 `route/3` 决定投给谁，再走 `{undefined, undefined, Msg}` 的非位置型 push（`broker_subscription.erl:222-233`）——与本地 QoS0 完全同形。
- **为什么必须各节点本地 route**：路由索引本来就是每节点一份（ADR-010），发布节点看不到别的节点的订阅者；任何「全局 route」都是发明第二份索引。
- **接线点 = `broker_subscription:dispatch/2`**（唯一的扇出点），用 `persistent_term` 钩子 `{broker_subscription, qos0_fanout}`（由 `broker_cluster` 安装，`broker_subscription` 不新增编译期依赖）。为避免递归，远程落地走内部 `dispatch_local/2`（不触发广播）。广播用 `rpc:cast`，**不阻塞发布路径**（§5.1(e)：PUBACK 先于 dispatch）。
- **可用性**：严格绑定 **ADR-007 L1 尽力而为**——不可达节点静默丢弃、不重试、不补发、不承诺到达。禁止把它表述为「跨节点至少一次」。
- **retain 仍节点本地**：QoS0 的 retained 更新只发生在发布节点，跨节点订阅者读不到它（v1 限制，见 D8）。

### D5. 游标 / 会话 / 消费组仍是本地真相；exclusive/failover 跨节点正确；**shared 组跨节点不交付**

- **不修订 ADR-013 / ADR-019**：`?CURSORS`、`?ATTACHMENTS`、`assigned`（不落盘）、会话文件全部留在原节点。本 ADR 只改「从哪个日志读」，不改「读到哪记在哪」。
- **exclusive / failover 跨节点正确**：exclusive 只有一个消费者、它的游标在它所在的节点，读路由到 owner 后 offset 语义完全一致；failover 只有 active 读、standby 不读，切换后新 active 从自己的游标继续（ADR-019 既有语义）。
- **shared 组跨节点不交付，且必须显式拒绝而不是静默重复**：`claim/4`（`broker_subscription.erl:157`）的 row 分配在 worker 本地 ETS 里做，两个节点会各自从 `low` 开始 claim 同一批 offset → **重复投递**。因此：`cross_node_delivery` 开启时，若某 shared 订阅的分区 owner 不是本节点，SUBSCRIBE 走各协议**既有失败路径**拒绝（MQTT 即 SUBACK `0x80`），返回 `{error, {cross_node_shared_unsupported, Partition}}`。不新增错误帧、不新增指标键。放开它需要分布式 claim（新 ADR），本期不做。
- **为什么这是可以接受的 v1 取舍**：single-consumer 语义（exclusive/failover）覆盖 MQTT 的主流用法；shared 的正确实现必须解决「跨节点租约 + 崩溃回收」，那是 ADR-019 §19.2 级别的修订，不该夹在 S12-D 里偷渡。

### D6. `committed` 的跨节点取法：owner 端对「各节点上报的候选」取最小值；**规则仍是 ADR-014 的「永不删过 committed」**

- **问题**：`broker_subscription_worker.erl:800` 用**本地** `min(游标) - 1` 作候选调 `advance_committed/2`。若节点 A 的候选大于节点 B 的游标，而 A 的候选被直接写进 owner，GC 会删掉 B 还没读的段——**跨节点数据丢失**。
- **裁决**：`committed` 在跨节点下 = **所有活游标的最小值**，由 owner 计算；各节点只**上报候选**，不直接改写。聚合状态放在 `broker_cluster`（内存、不落盘、**不是游标真相**，只用于 GC 下界），形状：`#{Partition => #{Node => Candidate}}`，owner 取 `min`。
- **成员进出**：`leave`（ADR-033）成功 → 清除该节点的候选条目；dist 断开但**仍是成员** → 保留最后值（冻结，宁可少删）；条目缺失时按「无远程候选」处理。**任何情况下都不允许把 committed 推过任一活游标。**
- **递归规避**：聚合后真正落盘的推进走一个**不经过钩子**的内部入口（`advance_committed_local/2`），否则会在 owner 上自我转发。
- **代价**：跨节点订阅存在时 committed 可能长期被最慢节点 pin 住，WAL 段不回收；运维观测沿用既有 `retention_blocked_by_slow_consumer` 与 `broker_queue:retention_status/0`（`broker_queue.erl:93`），**不新增键**。

### D7. 归属迁移：继承 S12-C 的「只改元数据、无字节 handoff」；因此跨节点投递的**正确性前提是副本集覆盖所有可能 owner + `replicated_commit`**

- 迁移仍只改 `(owner, epoch, replicas, state)`（`broker_cluster_meta_machine.erl:35-36`），**不搬字节**。若新 owner 从来没有该分区的数据，迁移后的读会读到「更短/空的日志」——这不是延迟问题，是**数据消失**。
- **硬前提**：`cross_node_delivery = true` 时，任何可能成为 owner 的节点必须在 `replicas` 里，且可靠性档必须是 `replicated_commit`（`broker_queue.erl:146-166,248`）。
- **前置校验而非事后补救**：`cross_node_delivery` 开启时，`assign/4` / `migrate/4` 必须校验「目标 owner ∈ `replicas`」，不满足回 `{error, {cross_node_delivery_requires_replica, Partition, Node}}` 并拒绝提交。启动期也做一次全量校验（成员数 > 1 且存在 `replicas` 不含 owner 的分区 → 拒绝启动）。这是 ADR-007「必须被拒绝或显式降级，不得静默成功」的直接应用。
- **迁移窗口的读写**：路由按本地 cache 的 owner；cache 滞后时写会被 owner 端 `{fenced, _}` 拒绝（D2 纵深），读可能短暂打到旧 owner —— 旧 owner 在 `migrating` 期间仍持有数据，读到的是**较旧但真实**的前缀，不是伪造。`finish_migration` 后 cache 收敛，读切到新 owner。
- **不做自动故障剔除**：owner 宕机后，该分区不可读写，直到运维触发 `migrate`/`assign`（ADR-033 D6 边界不变）。跨节点投递**不提高**任何可用性承诺。

### D8. 开关、默认与边界：默认关闭；关闭时单节点行为字节级不变；不新增 app / 端口 / 依赖 / 指标键

- **开关**：`broker_cluster` env `{cross_node_delivery, false}`（另加 `{cross_node_timeout_ms, 2000}`，有消费者）。默认 `false` ⇒ 三个钩子（`append_router`、`read_router`、`qos0_fanout`）**不安装**，`broker_queue` / `broker_subscription` 的全部路径与今天逐字相同，非 owner 写仍回 `{fenced, _}`。
- **开启但 `broker_cluster` 未运行**：钩子不存在 ⇒ 等价关闭。**诚实降级，不是静默跨节点。**
- **不做什么**：
  - 不做全量复制（B）、不做 owner 侧代理（C）、不做客户端侧重定向（D 的客户端面）、不建订阅者节点索引；
  - 不新增 app / 端口 / 运行时依赖 / 第二套共识（ADR-003、roadmap 三条硬边界）；
  - 不扩 `broker_metrics` 冻结键集（`broker_metrics.erl:408-415`）：跨节点转发**没有专用计数**，观测靠既有 `cluster_fencing_rejected` / `cluster_replication_failed` / `retention_blocked_by_slow_consumer` 与日志。需要新键时独立走 ADR-016 修订；
  - 不改 `broker_security` 语义；不写第二套消息内核（仍是 `broker_queue` + `broker_storage` + `#broker_message{}`）；
  - 不迁移活进程（ADR-032 D5 不变）。
- **信任边界**：`append_forwarded/3`、读端点等 rpc 入口**只在 owner 上服务**（非 owner 回 `{error, {not_owner, Owner}}`；写侧走 guard 的 `{fenced, _}`），不新增认证——沿用 `broker_cluster_rep:remote_append/2`（`broker_cluster_rep.erl:59`）的既有先例：Erlang distribution cookie 即信任边界，部署必须开 dist（epmd）。
- **v1 仍为节点本地的项（如实标注，不得含糊）**：retained 消息（`broker_retain` 单例）、will、`broker_event` 去重窗口、DLQ、会话文件、shared 组的 claim。跨节点投递**不覆盖**这些面，能力矩阵必须逐项写明。

### D9. 可用性与失败矩阵：绑定 ADR-007 三档与 ADR-032 D6，不新增承诺

| 场景 | 行为 | 依据 |
|---|---|---|
| 写，owner 可达，L1 | 转发到 owner，`os_buffer` 落盘即回 offset；owner 崩溃可丢（进程窗口） | ADR-007 L1 |
| 写，owner 可达，L2 | 转发 + owner `fsync` 后回 offset；owner 宕机不丢已 ACK 的 | ADR-007 L2 |
| 写，owner 可达，L3 | 转发 + owner `fsync` + 副本多数派后才回 offset；失去多数派 → `{error, {replication_failed, _, _}}`，**不 ACK** | ADR-007 L3 / ADR-021 |
| 写，owner 不可达 | rpc 超时 → `{error, {cross_node_append_failed, _, timeout}}`，**不 ACK、不本地写、不排队** | ADR-032 D7 |
| 写，cache 滞后 | 目标节点 `{fenced, _}`，**不 ACK、不双写** | ADR-021 / D2 |
| 读，owner 不可达 | `{error, {owner_unavailable, _}}`，本轮 idle、下 tick 重试；**不回退本地段** | D3 |
| 读，迁移窗口 | 可能读到旧 owner 的**真实前缀**；`finish_migration` 后收敛 | D7 |
| QoS0，某成员不可达 | 静默丢弃 | ADR-007 L1 |
| shared 组跨节点 | SUBSCRIBE 显式拒绝 | D5 |

- **禁止表述**：「零停机」「自动故障转移」「跨节点 exactly-once」「跨节点强一致」。任何跨节点承诺必须落到上表的档位与丢失边界。

## 后果（Consequences）

- **+** 消息只有一份权威副本 ⇒ 不存在「两份 WAL 各说各话」；每分区 offset 唯一，游标语义不需要任何改写。
- **+** 不触碰 ADR-010 / ADR-019 冻结正文：Router 仍只持本地倒排索引、不持消费者状态；游标 / 会话仍本地。这条路子把「跨节点」做在 `broker_queue` 的门面里，而不是做进路由器和订阅层。
- **+** 15 个发布点与全部读点**零改动**（路由在唯一门面内），适配器不需要知道集群存在。
- **+** 默认关闭 ⇒ 单节点与既有全量套件（1460/1460）行为不变，回滚只需关配置。
- **-** **owner 是读热点**：跨节点订阅者的每一次分页读都打到 owner，出口带宽与 owner 的 rpc 并发随订阅者数增长。这是 D′ 的主要代价。
- **-** **shared 组跨节点不交付**，显式拒绝；多节点部署下 shared 消费者必须落在 owner 节点。
- **-** **迁移的正确性依赖副本配置**：默认 `replicas = [node()]` 时不存在冗余，跨节点投递必须配副本集 + `replicated_commit`，否则拒绝启动 / 拒绝迁移 —— 运维复杂度上升。
- **-** **retain / will / dedup / DLQ / 会话仍本地** ⇒ 「一个逻辑 Broker」是不完整的，必须逐项写进能力矩阵。
- **-** **跨节点 rpc 依赖 dist**：未开 epmd / 未连通时跨节点不可用（单节点不受影响）；rpc 断开会同时影响写（不 ACK）与读（stall）。

## 验证（Validation）

本 ADR 只冻结形状，**不写实现代码**。实现期必须以 dist CT 落地下列断言（扩既有 `broker_cluster_dist_SUITE` 或新增套件；每例 `port => 0`）：

1. **跨节点写**：A publish、分区 owner = B ⇒ B 的 WAL 有该 offset，`{ok, Offset}` 返回给 A；**A 本地 WAL 无该记录**。反证：`cross_node_delivery = false` 时同一 publish 回 `{error, {fenced, _}}`。
2. **跨节点读**：订阅者在 A、owner = B ⇒ A 的 `read_from/watermarks` 返回 B 的日志与水位。反证：关掉 `read_router` ⇒ 读到空页 / `(0,0)`（钉住「必须路由，不能依赖本地段」）。
3. **跨节点 wake 与清理**：B append 后 A 的订阅进程收到 `{new_messages, ...}`；A 进程崩溃 ⇒ B 的 worker 收到跨节点 `DOWN` 并清理 `subs`（反证：不 monitor 则条目泄漏）。
4. **QoS0 广播**：A publish QoS0、订阅者在 B ⇒ B 收到 `{undefined, undefined, Msg}` push；**两侧 WAL 都不增长**；成员集外的节点收不到；某成员不可达时发布方仍立即返回（不阻塞）。
5. **失败不 ACK、无双写**：owner 不可达时 publish 无 PUBACK、A 本地 WAL 无记录。**反证**：把转发分支改成「先本地写再转发」⇒ 本用例立即红。
6. **迁移与副本前提**：`replicas` 不含目标 owner 时 `migrate` 被拒；满足前提时迁移后 A 的读仍拿到完整日志（含迁移前写入）；旧 owner 的转发被 `{fenced, _}`。
7. **shared 组显式拒绝**：跨节点 shared SUBSCRIBE 得到明确失败（MQTT `0x80`）。**反证**：改成允许 ⇒ 出现同一 offset 被两个节点各投一次的用例（钉住「不能静默重复」）。
8. **committed 聚合**：A 快、B 慢 ⇒ owner 的 `committed` 停在 B 的游标 - 1，慢消费者未读的段不被 GC；B `leave` 后聚合条目清除、committed 可推进。
9. **默认关闭零回归**：`cross_node_delivery = false` 时全量 `rebar3 ct`（基线 1460/1460）逐例不变；`cluster_fencing_rejected` 计数行为不变。
10. **wake 不承担正确性**：人为丢弃所有 `{new_messages, ...}` 后，poll tick 仍能把消息送达（拉模型自证）。

## 待评审签认的四个问题

本 ADR 是**提案**，以下四点需要评审明确签认后才能冻结：

1. 是否接受 **shared 组跨节点不交付**（显式拒绝）作为 v1 边界？
2. 是否接受 **`replicated_commit` + 副本集覆盖可能 owner** 作为跨节点投递的**启动前提**（不满足即拒绝启动 / 拒绝迁移）？
3. 是否接受 **owner 读热点**这一代价（相对 B 路线用写放大换读分散）？
4. 是否确认 `cross_node_delivery` **默认 `false`**，且本期不追求跨节点 exactly-once？

## 落地偏差记录

- **2026-11-19 立项（提案，未签认）**：本 ADR 依据 ADR-032 D9「S12-D 须新增 ADR」与 ADR-033 立项记录「产品侧确认 G2 终态 = 一个逻辑 Broker」起草。裁决形状 D′（owner 权威日志 + 跨节点读写路由），不新增 app / 端口 / 依赖 / 指标键，不改 ADR-010 / ADR-019 冻结正文，不迁移活进程。实现尚未开工；落地偏差、实测数字与任何形状调整将在实现提交后追加到本节，**正文不改**。
- **2026-11-19 实现落地（提案实现先行，仍未签认）**：按 D2 / D3 / D5 / D6 / D8 实现并交付 dist CT。**与正文的偏差如实记录**：
  - **D4（QoS0 跨节点广播）已实现（2026-11-19 补交）**：`broker_subscription:dispatch/2` 拆出 `dispatch_local/2`，非位置型 `{undefined,undefined,Msg}` 分支在投递本地后调 `maybe_fanout_qos0/1`——读 `broker_cluster` 安装的 `{broker_subscription, qos0_fanout}` 钩子（`cross_node_delivery=true` 时装；缺省 no-op），把消息 `rpc:cast` 广播给 `cluster_members/0` 里的其他成员（成员集优先 `broker_cluster_meta:members/0`，未运行时回退 `application:get_env(broker_cluster, members, [node()])`）。每成员走新增的 `dispatch_qos0/1`：用新 facade `broker_router:route(Topic, Tenant)`（additive plain function，解析配置的 backend，不硬编码 `broker_router_hash`）按**自身** Router 索引重路由，再 `dispatch_local/2`——**不经 `dispatch/2`**，避免回声递归。QoS0 不进日志、无 owner 可 pull，故 L1 尽力而为：cast 不阻塞、不可达成员静默丢弃、两侧 WAL 均不增长。`route_ctx` 仅带 tenant + `os_buffer`（读路径 durability-blind，哈希后端只读 tenant）。dist 套件新增例 `a_qos0_publish_on_a_non_owner_reaches_a_subscriber_on_another_member` 验证跨成员投递；全量 `rebar3 ct` **1469/1469**（在 1468 基础上 +1）。
  - **D6 的保守口径**：`committed` 取「各节点上报候选的最小值」，但**聚合入口只在 owner 上对「自身以外的候选」做 min 时，自身候选也必须一并纳入**（`route_op/3` 的 `local_or_floor` 对 `advance_committed` 一律先 `report_committed(本节点)` 再取 min）。实现里 `report_committed/3` 对表缺失容错（返回 `undefined` ⇒ 退化为「仅自身候选」＝单节点规则），成员 `leave` 通过 `forget_committed/1` 清条目。跨节点订阅存在时 `committed` 可能被最慢节点长期 pin 住，WAL 段不回收——这是 D6 明示的代价，非偏差。
  - **D5 的 shared 拒绝落在 `reply_new_subscribe2`**：拒绝点选在「Router 注册之后、建 cursor 之前」，失败路径复用既有 `broker_router:unregister/1` 回滚（K8 orphan 容忍只服务崩溃，不服务可预见的拒绝）。`not_found`（无 owner 元数据）按单节点默认放行，与 `route_op/3` 的 `not_found → local` 一致。
  - **D2 的 `{forward, _}` 不本地写**：`append/3` 的 `{forward, _}` 分支直接返回 rpc 结果，不调用 `append_local/3`，因此不消费 offset、不写 WAL、不 notify——「无双写」是结构保证，非约定。
  - **fencing 纵深**：非 owner 转发到「cache 已非 owner」的节点时，该节点 `route_op/3` 因进程字典 `'$broker_queue_forwarded'` 置位而回 `{error, {not_owner, Owner}}` 并计 `cluster_fencing_rejected`，不静默本地写。
  - **默认关闭零回归**：`cross_node_delivery = false` 时三个钩子（`append_router` / `read_router` 经 `route_op` / `qos0_fanout`）均不安装（D4 的 `qos0_fanout` 同样随开关缺省）；`broker_queue` / `broker_subscription` 全路径与实现前逐字相同。全量 `rebar3 ct` **1469/1469**（实现前基线 1460，ADR-034 D2/D3/D5/D6/D8 +8 例，D4 +1 例）；dialyzer 0 warning；xref 仅既有 unused-export 告警。
  - **`forwarded/3` 的参数不重名**：`forwarded_op/7` 对每个 Op 自行解构参数，避免 `append` 的 `[Msg, Mode]` 与 `read_from` 的 `[From, Limit]` 在同一子句里模式冲突。
