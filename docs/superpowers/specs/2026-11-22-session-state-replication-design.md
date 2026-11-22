# 集群会话状态复制 + 透明故障切换 设计

| 项 | 内容 |
|---|---|
| 任务 | MQ-17 产品级清单「集群会话副本的故障注入证明」（P1） |
| 关联 | [MQ-17 缺口](../compatibility/unifiedmq-mqtt-family-gap.md)（MQ-17 行）；[feature-comparison §4.2 #5/#6](../mqtt-feature-comparison.md)；ADR-021 §D5（消息复制）、ADR-034 D5/D7（跨节点路由 / 迁移守卫）、ADR-016（指标冻结键集） |
| 日期 | 2026-11-22 |
| 状态 | 已批准 |

## 1. 目标

让 MQTT 会话的**持久状态**（订阅集 + 读游标 + in-flight blob）在集群节点间复制，使得当一个会话的 **owner 节点崩溃**时，持有该客户端活连接的副本节点能**就地从本地副本恢复会话并续传**，客户端在 MQTT 层**零感知**（不重连、不收 CONNACK、不丢会话）。

- **范围**：集群场景下会话状态的复制与故障接管；不含活 TCP 跨节点迁移（见 §3.1，OTP 不支持）。
- **一致性**：订阅 / in-flight **同步**复制到多数派（订阅不丢）；游标**异步**批量复制（允许有界回退，MQTT 3.1.1 §4.1 本就允许重复投递）。
- **退化**：默认开关关闭；关闭时全链路逐字节退化为今日单机行为（同 `broker_queue` 的 `replicator` absent 语义）。

## 2. 现状（已核实，非猜测）

- **会话状态住在每节点本地、无跨节点复制。** `broker_subscription_worker` 经 `broker_subscription_app → broker_subscription_sup` 启动，是**每节点各一份的单例**：每节点独立持有五张 ETS 表（`?ENTITIES` / `?MEMBERS` / `?CURSORS` / `?ATTACHMENTS` / `?INFLIGHT`）+ 本地 `sessions/*.subs` 文件（`broker_subscription_worker.erl:55-59`）。集群里它**不共享**——客户端会话只活在持有其连接的节点上。
- **三层会话持久化**（design §5.3）：订阅实体 + 游标 + 消费者列表 fsync 进 `.subs`（`flush_sub_cursors/2`，K6 flush tick）；in-flight blob（packet_id 分配器 + in-flight 集）经 `inflight/2` 写入（`:117`），由 `inflight_tick` 异步 flush（`:437`、`:142`）。`session/1`（`:358`）返回 `{ok, #{subs, inflight, online}}` 或 `{error, not_found}`。
- **队列字节已有成熟复制路径可仿。** `broker_cluster_rep:replicate/2,3`（`broker_cluster_rep.erl:20-92`）：查副本集 → 并行 `rpc:call` 到副本 → 统计多数派 ack → 观测 `cluster_replication_latency_us` / `cluster_replication_failed` 指标。`broker_queue:append_replica/3` 是接收侧契约：**不通知本地订阅者、不跑所有权守卫**，follower 仅为持久性写入。
- **挂钩机制成熟。** `broker_cluster:install_hooks/0`（`broker_cluster.erl:176`）把 `{broker_queue, replicator}` 等装成 persistent_term；`broker_queue:replicator/0`（`:219`）无钩子时 `append` 退化为单机。`cross_node_delivery` env 控制路由钩子安装。
- **所有权 / 迁移成熟。** 元数据组（ADR-021/032/033/034）持有 owner/epoch/replicas 权威视图；`broker_cluster_cache` 是本地镜像；ADR-034 D7 已规定「迁移目标必须已持副本，否则丢分区字节」。`authorize_append/2`（`:123`）用 epoch fencing 拒绝过期 writer。

**结论（缺口）：** 会话状态今天**完全不复制**。owner（= 持连节点，对独占会话而言）崩溃 → 连接随节点消失 → 会话丢失。要达成目标，必须把会话状态纳入与队列字节同构的复制体系。

## 3. 设计

采用 **Approach A：会话状态纳入现有 owner/replica/fencing 复制体系**（经 brainstorming 选定）。

### 3.1 关键前提：TCP 连接无法跨节点存活

Erlang/OTP 不支持把一条活跃 TCP socket（及其 `conn_fsm` 进程）迁移到另一个节点——分布层无原生活跃 fd 交接。因此「owner 崩溃、连接存活」只有在**会话状态归属与连接节点分离**时才成立：会话状态由元数据组指派 owner、复制到副本集；持连节点作为副本持有本地副本，`conn_fsm` 从本地副本 attach。owner 崩溃 → 所有权迁往持连副本 → 会话就地续传。（不做 C_NODE / `socket` fd hack——超出产品清单量级且脆弱。）

### 3.2 `broker_cluster_session_rep`（新模块，`broker_cluster`）— 复制发起侧

API：`replicate_session(Partition, Op) -> ok | {error, term()}`。

- 查本分区副本集：`broker_cluster:replicas(Partition)`。`not_found` 或单节点 → `ok`（单机默认，无元数据即无复制）。
- `Needed = length(Replicas) div 2 + 1`；并行 `rpc:call` 到各副本（`Others = [N || N <- Replicas, N =/= node()]`，受 `?MAX_ACKS` 限）。
- 每条 ack 经 `broker_cluster_session_rep:remote_apply`（接收侧，内部转调 `broker_subscription_worker:apply_replica/1`）；统计多数派。未达 → `{error, {replication_failed, Acks, Needed}}` + `inc(cluster_session_replication_failed)`。
- 成功路径观测 `observe(cluster_session_replication_latency_us, Elapsed)`。

仿 `broker_cluster_rep`：复制只测副本往返（本地已先落盘/落表），多数派未达即失败。

### 3.3 接收侧 `broker_subscription_worker:apply_replica/1`（新入口）

`apply_replica(Op) -> ok`，在副本本地把会话 Op 应用到 ETS（纯内存写入；畸形 Op 直接崩溃，不做防御式返回，由调用侧 rpc 承担失败语义）：

- Op 形如 `{subscribe, ClientId, SubRef, Info}` / `{inflight, ClientId, Term}` / `{unsubscribe, ClientId, SubRef}` / `{cursor, SubRef, PartitionId, Row}`。
- **契约同 `append_replica`**：不跑所有权守卫、不通知订阅者、不回写为权威磁盘（内存为主；本地 `.subs` flush 仍由本地 worker 自管）。
- **幂等**：以 `(ClientId, SubRef)` / `(ClientId)` / `(SubRef)` 为键覆盖写，rpc 重试安全。
- 游标 Op 走异步批量（§3.4），订阅 / in-flight 走同步（§3.4）。

### 3.4 复制粒度与热路径（经 brainstorming 选定：订阅+inflight 同步、游标异步）

- **同步**：`subscribe`/`join`/`unsubscribe`/`inflight` 提交后，单一写入者咨询 `{broker_subscription, session_replicator}` 钩子，同步复制该 Op，**多数派 ack 后**才回上层 ACK（订阅语义：未确认即未建立，客户端可重试）。
- **异步**：游标推进只标 `cursor_dirty`，由 worker 已有的 flush tick（`cursor_flush_interval_ms`，默认 100ms）批量复制。崩溃回退窗口 ≤ flush 间隔；MQTT 3.1.1 §4.1 允许重复投递。
- **默认无钩子即退化**：`session_replicator` absent 时，`subscribe` 等不复制，逐字节同今日单机（同 `broker_queue:replicator/0` 语义）。

### 3.5 所有权迁移接管（`broker_cluster_meta` / `broker_cluster_cache`）

- owner 崩溃由现有成员/纪元机制发现（`broker_cluster_meta`）。
- 迁移候选排序注入**「持连优先」**：优先选择持有该会话客户端活连接的副本节点作为新 owner，使其能就地服务。复用 ADR-034 D7「目标必须已持副本」约束（会话副本同样必须先复制到该节点，否则拒绝迁移）。
- 新 owner 纪元 +1；旧 owner 重上线后被 `authorize_append/2` 的 epoch fencing 拒绝写入陈旧会话。

### 3.6 `mqtt_conn_fsm` attach 路径

- 所有权迁移后，持连节点上 `conn_fsm` 重新 `broker_subscription:session(ClientId)` 读**本地副本**（N 已是副本/新 owner），复用 `attach/3`（`broker_subscription.erl:209`，takeover 语义）绑定，从本地游标续传。
- 客户端无新 CONNECT、无 CONNACK——TCP 流不中断，续传对客户端透明。

## 4. 数据流

**正常态：**
```
客户端 →(TCP)→ 节点N: conn_fsm
  SUBSCRIBE → broker_subscription_worker(单一写入者)
     → 写本地 ETS + fsync .subs
     → 咨询 session_replicator 钩子 → replicate_session(Partition, sub_op)
         → 并行 rpc:call 到副本集 → 各副本 apply_replica/1 → 写本地 ETS
         → 多数派 ack → 回 SUBSCRIBE ACK
  PUBLISH 分发 → cursor 推进 → 标 cursor_dirty
     → flush tick(异步) → 复制游标(不阻塞热路径)
```

**故障态（owner O 崩溃，连接在 N）：**
```
节点O 崩溃(成员/纪元机制发现失联)
  → 触发所有权迁移(ADR-034 D7)
  → 候选排序「持连优先」→ 选 N
  → N 成新 owner，本地已有会话副本
  → N 上 conn_fsm 重新 attach：session(ClientId) 读本地副本 → attach/3 → 从本地游标续传
  → 客户端 MQTT 层零感知
```

**恢复态：** O 重上线后纪元落后，fencing 拒绝其陈旧写入；不自动拿回所有权（除非元数据组再迁移）。

## 5. 错误处理与边界

- **多数派未达（同步路径）**：订阅 / in-flight 返回复制失败 → 上层 SUBSCRIBE 失败（订阅未确认）；游标异步路径丢一次复制无妨。
- **幂等**：`apply_replica/1` 覆盖写，跨节点 rpc 可能重复投递但结果一致。
- **RPO**：订阅 / in-flight ≈ 0（同步）；游标有界（≤ `cursor_flush_interval_ms` 窗口回退）。
- **指标**：新增冻结键 `cluster_session_replication_latency_us` / `cluster_session_replication_failed`（ADR-016 冻结键集，经 `broker_metrics` `init` 注册）。
- **开关**：新 env（建议 `session_replication`，默认 `false`）控制 `session_replicator` 钩子安装；关闭即全链路退化。是否复用 `cross_node_delivery` 还是独立开关，留待 plan 阶段定（倾向独立，避免与路由耦合）。
- **ADR 落点**：新 ADR（会话状态复制，扩展 ADR-021/034 D5/D7 语义），引用既有编号，不推翻既有决策。
- **文档惯例**：历史实测记录正文不改，只追加带日期批注或划掉注明日期。

## 6. 故障注入测试

参照现有 `broker_cluster_cross_node_dist_SUITE` 多节点 CT 范式（`?cluster` 三节点、`stop_node` 注入崩溃），新增 `broker_session_replica_SUITE`：

1. **`session_survives_owner_crash`** — 客户端连 N、订阅、收消息建游标；kill owner O；断言 `session(ClientId)` 仍返回订阅集 + 游标、客户端**未重连**却续收新消息且游标不回退已确认位。
2. **`subscribe_not_lost_on_crash`** — 多数派 ack 前 kill 一半副本；断言订阅要么完整存在要么整体不存在（无半截行）。
3. **`inflight_dedup_preserved`** — QoS2 in-flight 建立后 kill owner；断言接管节点 inflight 恢复、重复投递被去重。
4. **`stale_owner_fenced`** — O 重上线后尝试写入旧会话；断言被纪元 fencing 拒。
5. **`single_node_unchanged`** — 无 session_replicator 钩子时行为逐字节同今日（回归保护）。

故障注入手法沿用现有 SUITE 已验证的节点 kill，不引入新混沌框架（YAGNI）。

## 7. 不变量

- **I1**：接收侧 `apply_replica/1` 与本地写入共用同一组 ETS 表，但入口分离——复制流量**绝不**触发所有权守卫或重复通知（同 `append_replica` 契约）。
- **I2**：默认关闭时，会话路径与今日单机**逐字节一致**（无钩子 = 无复制调用）。
- **I3**：迁移目标必须已持有该会话副本（扩展 ADR-034 D7 约束到会话状态）。
- **I4**：客户端在 owner 崩溃 + 接管期间**无新 CONNECT / 无 CONNACK**（透明性的可验证判据）。

---

## 8. Plan 2 实现期批注（2026-11-22）

【2026-11-22】§3.5 与 §4 数据流中「ra 候选排序内持连优先」的设想，已在 Plan 2 brainstorming 修订为：**ra 按副本固定序选新 owner（不感知连接）→ 持连副本节点观察 `{nodedown, O}` 后向 ra 提交 `{claim, P, N, Epoch}` 事后认领**。理由：连接存活信息不进 ra 排序，避免把节点级 liveness 拉回集群元数据；ra 串行 apply 仍保证全局唯一 owner，连接判断留在本地 conn_fsm。Plan 1 的复制管线不受影响。详见 [Plan 2 设计](2026-11-22-session-state-replication-plan2-design.md) §4 与 §6。
