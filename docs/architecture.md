# UnifiedMQ 架构

- **文档版本**：v1.0（S6，2026-11-09）
- **状态**：描述当前已实现并实测的架构事实（S0–S6）。S6 之后（现归 S7 及之后）明确未实现的方向在 §9 单列，不与现状混写
- **读者**：接手实现的工程师、评审人、需要判断“某个行为归哪一层”的测试作者
- **对应**：`docs/roadmap.md`、`docs/design/`（各阶段设计基线）、ADR-001/002/004/007/009/010/011/016/017/018/019/020/021
- **证据口径**：本文件中“已实测”来自 `rebar3 ct`（**673/673**）、`tools/docker/`（T14/S2 故障注入）、`test/protocol-compatibility/`（六协议真实客户端）、`apps/broker_cluster/test/`（3 节点 peer 的 `broker_cluster_dist_SUITE`）与 `docs/benchmarks/` 的产物；引用处给出路径

> 本文只回答三个问题：**有哪些应用/进程、谁在什么进程里干活、一次消息从进到出走哪条路**。磁盘格式、刷盘与恢复的细节见 [`storage-and-recovery.md`](storage-and-recovery.md)，协议能力与对端兼容性见 [`protocol-compatibility.md`](protocol-compatibility.md)。

---

## 1. 范围与设计约束

当前系统是一个支持 **MQTT 3.1.1/5.0、WebSocket、NATS、AMQP 0-9-1、Kafka、Pulsar** 六协议的 broker，具备本地内存/WAL 两种存储后端、复制提交（Metadata Raft + `replicated_commit`）、CloudEvents 策略层与 S6 管理面。可靠性等级覆盖 ADR-007 的 L1 `os_buffer` 与 L2 `fsync`；跨节点复制与多数派由 S4 的 Raft 控制面/复制路径承担。架构上的硬约束来自已冻结的 ADR：

| 约束 | 来源 | 在代码里的落点 |
|---|---|---|
| 协议适配层不得直接操作磁盘格式 | ADR-001 | `mqtt_conn_fsm` 只经 `broker_router` / `broker_queue` / `broker_subscription` 三个门面接触内核，不 include 任何存储模块（路由只 include `broker_router.hrl` 的 `#route_ctx{}`） |
| 统一消息模型与 CloudEvents 分离，`event` 只是字段位 | ADR-004/009 | `#broker_message{}` 带 `event` 槽；`broker_event` 默认 `pass_through` 恒等，仅在按租户/目的地显式开启时才解析并写 `event`（见 §2.2） |
| 可靠性承诺显式化：ACK 时机与 sync mode 一一对应 | ADR-007 | `mqtt_conn_fsm` 在 `broker_queue:append/3` 返回后才发 PUBACK |
| Router 不串行化，路由在发布者进程内完成 | ADR-010 | 句柄/索引在 `protected` ETS，`route/3` 无 `gen_server:call` |
| 存储只拥有全局 committed 水位，per-sub 位置归订阅层 | ADR-011 | `broker_storage` 的 API 没有订阅概念；cursor 全在 `broker_subscription` |
| 分区创建是带外动作，boot 只 recover | ADR-011 §11.2 | `create_partition/1` 是唯一 `create` 调用者；boot 缺分区即整体启动失败 |
| 协议适配层之间互不引用 | ADR-001 | 六个 `broker_proto_*` 只依赖内核门面，彼此没有依赖边 |

**S7 及之后明确未实现**（免得把"还没做"误读成"没画出来"）：S7 的全部验收项已于 2026-11-10 结案——六协议统一身份表见 `docs/security.md` §2.1，各协议标准 CloudEvents Binding 的结论见 `docs/roadmap.md` §9.1（结论是「统一 `ce-*` 是本 Broker 的项目定义，逐字实现不立项」，逐协议差异已列明）。S7 之后明确未实现的是：跨节点/复制通道 TLS、Schema Registry 内容校验、审计日志签名与外部 KMS、每条规则可配置的「未知扩展属性拒绝」、去重/隔离钩子推广到五协议、OTel 等可观测性导出层，以及 `require_auth` 接进 MQTT CONNECT、`broker_meta` 用户表与数据面认证接线、`topic_meta`/`queue_meta`/`binding_meta` 的运行时读取方。README §10.2 的 trace context **传播本身已实现**（2026-11-09，`broker_event_trace`，见 `docs/security.md` §5.1），未实现的是把指标导出到外部系统。清单与 `docs/security.md` §12、`progress.md` 一致。

---

## 2. 应用划分

### 2.1 18 个应用

```
apps/
├── broker_core/          仅 include/broker_message.hrl 与公共类型；无模块、无进程、无 supervisor
├── broker_metrics/       S2-A：counters/atomics 指标 + snapshot/0 + 周期单行日志（无热路径进程）
├── broker_meta/          S4：Mnesia 控制面 11 表、版本/对账、outbox、用户哈希与 ACL（不含消息正文）
├── broker_cluster/       S4：ra Metadata Raft Group、归属缓存、Epoch Fencing、复制与 quorum
├── broker_storage/       broker_storage behavior + 内存/WAL 两实现 + WAL 内部七个模块
├── broker_router/        broker_router behavior + broker_router_hash 实现（内存索引）
├── broker_queue/         分区层：存储句柄生命周期、append/read 编排、{new_messages} 扇出
├── broker_subscription/  订阅、per-sub cursor、投递唤醒、三层会话持久化
├── broker_retain/        S2-C：每 topic 一条 retained 消息，retain/retain.v1 快照持久化
├── broker_event/         CloudEvents codec/binding/五档策略/去重（S3）
├── broker_security/      username/password 认证与 allow-all ACL 桩（纯函数）
├── broker_admin/         S6：HTTP 管理面（health/metrics/status/config/backup/tls reload）
├── broker_proto_mqtt/    MQTT 3.1.1/5 编解码 + 连接 FSM + ranch listener
├── broker_proto_ws/      S5：WebSocket（RFC 6455）+ JSON/binary 消息 API
├── broker_proto_nats/    S5：NATS core（PUB/SUB、queue group、request/reply）
├── broker_proto_amqp/    S5：AMQP 0-9-1（exchange/queue/binding、publish/consume/ack）
├── broker_proto_kafka/   S5：Kafka wire（Produce/Fetch/Metadata + consumer group）
└── broker_proto_pulsar/  S5：Pulsar binary（produce/consume/ack/seek）
```

| 应用 | 依赖（`.app.src`） | 自己的进程 | 对外入口 | 拥有什么状态 |
|---|---|---|---|---|
| `broker_core` | kernel, stdlib | 无 | `broker_message.hrl` 的 `#broker_message{}`、`partition_id()`、`sync_mode()` | 无（纯类型） |
| `broker_metrics` | kernel, stdlib, sasl | `broker_metrics_server`（仅 `report_interval_ms > 0`；指标本身不在进程里） | `inc/1,2`、`observe/2`、`gauge/2`、`gauge_add/2`、`snapshot/0`、`p99/1`、`keys/0` | `counters`/`atomics` 数组 + `persistent_term` 的 `Key → {Kind,Ref,Idx}` 索引（无 ETS） |
| `broker_meta` | kernel, stdlib, sasl, mnesia | `broker_meta_config`（配置版本 gen_server）、`broker_meta_outbox`；表访问本身由调用方同步执行 | `ensure_schema/0`、`tables/0`、通用 `put/get/delete/all/clear` + 8 张表的类型化访问器（`put_tenant/get_tenant`、`put_user/get_user`、`put_acl/acls`、`put_partition/get_partition`、`put_node/nodes`、`version/bump_version`、`enqueue/pending_outbox/mark_published`）、`reset/0` | Mnesia 表（控制面）；schema 不存在时创建，存在时复用。11 张表**全部建表并可有行**，但 `topic_meta` / `queue_meta` / `binding_meta` 目前**没有运行时消费者**（只有 `tables/0` 与套件引用，泛型 `put/get` 可写可读）——这三张表是控制面预留，接入路由/队列/binding 的读取方列为 S7 之后 |
| `broker_cluster` | kernel, stdlib, sasl, crypto, ra, broker_core, broker_meta, broker_metrics | `broker_cluster_cache`（归属/epoch 缓存 gen_server）与 ra server `{broker_cluster_meta, Node}`（模块 `broker_cluster_meta`，状态机 `broker_cluster_meta_machine`）；`broker_cluster_rep` 是纯函数模块，无进程 | `assign/4`、`migrate/4`、`finish_migration/2`、`remove/1`、`owner/1`、`epoch/1`、`replicas/1`、`authorize_append/2`、`guard_append/3`、`replicate/2`、`snapshot/0` | 本地 ETS 归属/epoch 缓存；ra 日志与快照；`persistent_term` 队列钩子 |
| `broker_storage` | kernel, stdlib, sasl, broker_core, broker_metrics | `broker_storage_wal_sup` + 每分区 `wal_writer`（WAL 档）／`broker_storage_memory_store`（内存档） | `broker_storage` behavior：`create/recover/close/append/read_from/advance_committed/truncate/capability` | WAL 段与 `meta.log`；内存表 |
| `broker_router` | kernel, stdlib, sasl, broker_core | `broker_router_hash_store` | `route/3`（`broker_router` behavior 回调，由 `broker_router_hash` 实现）、`broker_router:env/0`、`register/3`、`unregister/1` | `Filter → [SubRef]` 内存索引（**不持久化**） |
| `broker_queue` | kernel, stdlib, sasl, broker_core, broker_metrics, broker_storage, broker_router, broker_security | `broker_queue_store` + 每分区 `broker_queue_partition` | `append/3`、`read_from/3`、`watermarks/1`、`capability/1`、`subscribe/2`、`create_partition/1` | 分区句柄注册表、每分区订阅者集合 |
| `broker_subscription` | kernel, stdlib, sasl, broker_core, broker_metrics, broker_storage, broker_router, broker_queue | `broker_subscription_worker` | `subscribe/3`、`unsubscribe/2`、`ack/3`、`inflight/2`、`attach/3`、`detach/2`、`dispatch/2`、`cursor/2`、`subscriptions/1`、`session/1` | 五张 ETS（实体 / client→SubRef 成员索引 / 游标 / 连接绑定 / in-flight）+ `data/sessions/*` 三件套 |
| `broker_event` | kernel, stdlib, sasl, broker_core, broker_metrics, broker_router, broker_queue | `broker_event_dedup`（去重窗口 gen_server + ETS） | `policy/0`、`policy_for/1`、`apply_policy/1,2`、`dedup_window/1`、`duplicate/1`、`quarantine/2`、`broker_event_codec`（CE 1.0 JSON / Binary）、`broker_event_binding`（MQTT Binding + 损失报告） | 去重 ETS（`{Tenant,Destination,source,id} → 过期时刻`，默认关闭） |
| `broker_security` | kernel, stdlib, sasl, ssl, broker_core | 空 supervisor | `authenticate/2`、`authorize/2`、`tenant_id/1`（纯函数读 env） | 无（配置即状态） |
| `broker_retain` | kernel, stdlib, sasl, broker_core, broker_metrics | `broker_retain`（gen_server） | `put/2`、`delete/2`、`lookup/1`、`entries/0`、`stats/0`、`flush/0` | 每 topic 一条 retained 消息（ETS/内存树）+ `retain/retain.v1` 快照 |
| `broker_admin` | kernel, stdlib, sasl, crypto, broker_core, broker_metrics, broker_security | `broker_admin_listener`（HTTP accept） | `broker_admin_http:handle/4`、`metrics_text/0`、`broker_backup`、`broker_config_audit`、`broker_version:info/0` | 只读快照；备份清单文件 |
| `broker_proto_mqtt` | kernel, stdlib, sasl, ranch, broker_core, broker_metrics + 其余 6 个 broker app | `mqtt_conn_sup` + 每连接 `mqtt_conn_fsm` + 嵌入的 ranch listener | `mqtt_frame`（编解码）、`mqtt_filter`（Annex A 校验）、`mqtt_conn_fsm`、`mqtt_listener` | 每连接内存视图（packet_id 分配器、in-flight 集合、keepalive 定时器） |
| `broker_proto_ws` | kernel, stdlib, sasl, ranch, broker_core, broker_metrics + 内核 6 app | 每连接 `ws_conn` + ranch listener | `ws_frame`（RFC 6455 编解码）、`ws_conn`、`ws_listener` | 每连接分片缓冲、订阅集合 |
| `broker_proto_nats` | kernel, stdlib, sasl, ranch, broker_core, broker_metrics + 内核 6 app | 每连接 `nats_conn` + 订阅投递进程 + ranch listener | `nats_conn`、`nats_listener` | 每连接 sid 表、queue group 成员关系 |
| `broker_proto_amqp` | kernel, stdlib, sasl, ranch, broker_core, broker_metrics + 内核 6 app | 每连接 `amqp_conn`、`amqp_topo`（exchange/queue/binding 注册表） | `amqp_codec`、`amqp_conn`、`amqp_listener` | exchange/queue/binding 拓扑、delivery tag、unacked 集合 |
| `broker_proto_kafka` | kernel, stdlib, sasl, ranch, broker_core, broker_metrics + 内核 6 app | 每连接 `kafka_conn`、`kafka_group`（consumer group 协调） | `kafka_codec`、`kafka_conn`、`kafka_listener` | 每连接 topic/partition 游标、group 成员与 offset |
| `broker_proto_pulsar` | kernel, stdlib, sasl, ranch, broker_core, broker_metrics + 内核 6 app | 每连接 `pulsar_conn`、`pulsar_subscriptions` | `pulsar_codec`、`pulsar_conn`、`pulsar_listener` | 每连接 producer/consumer 表、subscription 游标 |

### 2.2 `broker_event` 的真实状态（S3 已实现）

S3（ADR-020）把 T11 的恒等桩补成完整的 CloudEvents 1.0 层：

- `broker_event_codec`：Structured JSON 与 Binary `ce-*` 双向编解码、核心/可选/扩展属性校验、`data`/`data_base64` 互斥。
- `broker_event_binding`：MQTT 消息检测（v5 `user_properties` → Binary；`content-type`/payload → Structured）、`decode/1`、`to_mqtt/3` 与 `#{mode, target, version, losses, preserved}` 转换报告。
- `broker_event`：五档策略按租户 + 目的地首个匹配；`apply_policy/1,2` 默认 `pass_through`，对普通消息恒等；`required`/`normalize` 对非事件返回 `{error, {event_invalid, event_required}}`；`normalize` 只写 `event`，**不改 payload 字节**。
- `broker_event_dedup`：受监督的有界去重窗口，默认关闭；键 `{Tenant, Destination, source, id}`，进程重启即清空（不承诺跨重启）。
- `quarantine/2`：可选的隔离副本，把原始消息以 `fsync` 追加到可配置 destination，拒绝本身不受隔离成败影响。

**数据路径**：`mqtt_conn_fsm:handle_publish1/7` 在路由之后、存储之前调用 `broker_event:apply_policy/1`；拒绝不 ACK（v5 QoS1 PUBACK `0x83`、QoS2 PUBREC `0x83`，3.1.1 关闭连接），去重命中只 ACK 不二次存储。默认策略是 `pass_through`，因此 MQTT 3.1.1 出站字节与 S2 行为完全不变；CloudEvents 是按租户/目的地显式开启的能力，不是全局强制（ADR-004）。

### 2.3 依赖方向

```
broker_core  ←── 所有应用（只依赖类型）
broker_metrics ←── broker_storage / broker_queue / broker_subscription / broker_proto_mqtt
                 （只被埋点调用；它自己只依赖 kernel/stdlib/sasl，不反向引用任何 broker app）
broker_storage ←── broker_queue ←── broker_subscription ←── broker_proto_mqtt
broker_router  ←── broker_queue ──┘            ↑
broker_security ───────────────────────────────┘
broker_event ──────────────────────────────────┘（发布路径调用；默认 pass_through 恒等）
broker_meta    ←── broker_cluster             （控制面单向依赖；broker_cluster 不反向依赖协议层）
broker_cluster ──→ broker_queue（仅通过 persistent_term 钩子，未注册时 no-op）
```

依赖只允许**向下**：协议层可以调内核，内核任何一层都不得反向引用 `broker_proto_mqtt`；`broker_core` 不依赖任何兄弟应用。`xref` 是这条约束的机械检查（当前只剩预期的 unused-export 告警）。

---

## 3. 进程模型与监督树

### 3.1 顶层结构

系统**没有伞形 `broker_app_sup`**：除 `broker_core` 外的 17 个 app 各有自己的顶层 supervisor，跨 app 的启动顺序交给 OTP 的 `applications` 拓扑序（设计文档 §6 的 T1 批注）。

```
broker_metrics_app ────► broker_metrics_sup                        one_for_one, 5/10s
                          └── broker_metrics_server                 permanent（仅 report_interval_ms > 0）

broker_storage_app ─────► broker_storage_sup                        one_for_all, 1/5s
                          └── broker_storage_wal_sup                 one_for_one, 4/30s
                              └── wal_writer × N（分区，start_child/permanent）
                          或 ── broker_storage_memory_store          permanent（内存档）

broker_router_app ──────► broker_router_sup                         one_for_all, 1/5s
                          └── broker_router_hash_store              permanent

broker_queue_app ───────► broker_queue_sup                          one_for_one, 5/10s
                          ├── broker_queue_store                    permanent（先起）
                          └── broker_queue_partition × N            permanent

broker_subscription_app ► broker_subscription_sup                   one_for_one, 5/10s
                          └── broker_subscription_worker            permanent（boot step 2+3）

broker_event_app ───────► broker_event_sup                          one_for_all（空）
broker_security_app ────► broker_security_sup                       one_for_all（空）

broker_proto_mqtt_app ──► broker_proto_mqtt_sup                     one_for_all, 1/5s
                          ├── mqtt_conn_sup                         simple_one_for_one
                          │   └── mqtt_conn_fsm × 连接数            temporary
                          └── ranch_embedded_sup                    （ranch:child_spec/5）
                              └── ranch_listener_sup → acceptors / conns sup
                                 （listener child spec restart=transient）
```

8 个顶层 supervisor（`broker_core` 没有）+ 1 个嵌套的 `broker_storage_wal_sup` + ranch 自己的子树。

### 3.2 重启策略与理由

| 进程 | 策略 | 理由 | 崩溃后的可见行为 |
|---|---|---|---|
| `broker_metrics_sup` / `broker_metrics_server` | `one_for_one`, 5 次/10 s | 上报进程崩溃不应连累任何数据面；指标数组在进程外，重启不丢计数 | 最多丢一个上报周期，计数继续累积 |
| `broker_storage_sup`、`broker_router_sup`、`broker_proto_mqtt_sup`、`broker_event_sup`、`broker_security_sup` | `one_for_all`, 1 次/5 s | 子系统内的进程共享一个装配前提（如索引 store 必须先于读路径） | 超过强度则整个 app 退出；若是 boot 期失败，application 起不来 |
| `broker_storage_wal_sup` | `one_for_one`, 4 次/30 s | 单分区崩溃必须自愈，但不能掩盖崩溃循环；与内存 store 隔离 | 该分区 `wal_writer` 重启并从磁盘 `recover`；旧句柄失效 |
| `wal_writer` | `permanent` | 分区日志的写者是必需进程，不能"死了就算" | 重启后 offsets 从磁盘重建；重启前的批以 `{writer_stopped,_}` 失败 |
| `broker_queue_sup` / `broker_subscription_sup` | `one_for_one`, 5 次/10 s | 分区 worker 与订阅 worker 相互独立，单点故障不扩散 | worker `init/1` 重新 recover/rebuild |
| `broker_queue_partition` | `permanent` | 分区生命周期 = 节点生命周期（E11） | 重启后重新 `recover/2` 并覆盖注册表句柄 |
| `broker_subscription_worker` | `permanent` | 表与进程同生共死，重启即从 `.subs` 全量重建 | 连接会看到短暂 `not_found`/重连；订阅集合不丢 |
| `mqtt_conn_fsm` | `temporary`（比"transient"更强） | 连接因对端断开/协议违规而死是常态；重启只会对死 socket 再打一次 | 进程消失，不重启；由客户端重连驱动 |
| listener（`ranch_embedded_sup` 下） | `transient` | 我们自己的故障要重启；正常 stop/升级不复活 | 端口短暂不可用；app stop 时随子树一起收掉 |

### 3.3 三条进程边界规则

这三条决定了"为什么某些操作看起来绕"：

1. **读路径不进 gen_server，写路径只进那一个写者。** `broker_router_hash:route/3`（先 `broker_router:env/0` 取 Env）、`broker_queue:read_from/3`、`broker_subscription:cursor/2`/`subscriptions/1`/`dispatch/2` 全部在**调用者进程**内查 `protected` ETS 或直接读段文件；`broker_queue:append/3` 同样在调用者进程内查注册表、完成 `wal_codec:encode/1`，只把编码后的二进制交给分区唯一的 `wal_writer`（一次 `gen_server:call`）。写者进程只拥有"写"与"生命周期"。**`broker_metrics` 也遵守这一条**：`inc/observe/gauge/gauge_add` 直接打 `counters`/`atomics`，`snapshot/0` 在调用者进程读取，热路径上没有指标进程。
2. **每个可变资源恰有一个写者。** 每分区一个 `wal_writer`、一个 `broker_queue_partition`；索引一个 `broker_router_hash_store`；订阅状态一个 `broker_subscription_worker`；分区注册表一个 `broker_queue_store`。这不是为了整齐，而是 group commit、torn-tail 截断、`min(cursor)-1` 这些算法成立的前提。
3. **通知永远是提示，不是数据。** `{new_messages, P, From, To}` 与 `{broker_deliver, ...}` 都只负责唤醒；真正的读位置只有 `broker_subscription:cursor/2`。通知可以乱序、重叠、丢失（设计文档 §5.2 T6/T9 批注）。

### 3.4 连接进程的生命周期

```
ranch acceptor: accept(Socket)
  → mqtt_listener:start_link/3
      → mqtt_conn_sup:start_conn(undefined, ProtoOpts)     % supervisor 子进程，socket 尚未易主
      → link(Pid)                                          % 让 ranch 的连接计数/优雅关停能管到它
  → ranch: controlling_process(Socket, Pid)
  → Pid 收到 {handshake, Ref, Transport, Socket, Timeout}
      → awaiting_owner →（setopts active once）→ awaiting_connect
      → 第一帧必须是 CONNECT
      → 认证成功 → attached（broker_subscription:attach/3）→ connected
```

一连接一进程，**没有代理进程**；`mqtt_conn_fsm` 内嵌投递循环（pull loop），而不是独立的 dispatcher 进程（S2 再抽）。

---

## 4. 启动顺序与 boot steps

### 4.1 拓扑序（OTP 28.3.1 实测）

```
sasl → broker_core → broker_metrics → broker_storage
     → crypto/asn1/public_key/ssl/ranch
     → broker_router → broker_queue → broker_subscription → broker_event
     → broker_security → broker_proto_mqtt
```

顺序不是装饰：`broker_queue` 在 `init/1` 里 recover 分区，`broker_subscription` 在 `init/1` 里重建订阅并回填 Router，二者都必须早于 listener。`broker_metrics` 必须早于 `broker_storage`（后者是它的调用者），而它本身只起一个可选的上报进程，不参与任何 boot step。

### 4.2 四个 boot step

| step | 执行者 | 做什么 | 失败语义 |
|---|---|---|---|
| 1 | `broker_queue_partition:init/1`（每分区） | `Backend:recover/2`，把句柄写进注册表 | 任一分区 `{not_found, P}` → 该 child 起不来 → `broker_queue_sup` → application 启动失败；**不自动 create** |
| 2 | `broker_subscription_worker:init/1` | 读 `data/sessions/*.subs`（权威）合并 `.cursors` / `.inflight`，重建实体与游标 | 任一 `.subs` 损坏 → `{stop, {subscription_boot_failed,_}}` → application 起不来 |
| 3 | 同一个 `init/1` | 逐条 `broker_router:register/3` 回填索引；`{error,{already_registered,_}}` 视为成功 | Router 拒绝 → 启动失败 |
| 4 | `broker_proto_mqtt_sup:init/1` 的最后一项 | 起 ranch listener，开始 accept | 绑定失败（如 `eaddrinuse`）→ 整个 application 启动失败 |

**为什么必须这个顺序**：Router 索引不落盘（ADR-010 §10.6），listener 先起会让新 SUBSCRIBE 与回填竞争，R1b（订阅集合不丢）就退化成概率性通过。

**首启没有特殊路径**：干净数据目录 boot 一定失败，运维必须先带外 `broker_queue:create_partition/1`（见 `storage-and-recovery.md` §12）。这是 ADR-011 §11.2 要的"响亮失败"，不是缺陷。

---

## 5. 数据流

### 5.1 Publish（QoS1）

```
Client ──PUBLISH(qos=1, pid=P, topic=T)──► mqtt_conn_fsm
                                             │
   ① 连接建立时按后端 capability 推导 max_qos；QoS > max 直接断连（E12），
      真正写入时 broker_queue/wal_writer 仍有 capability 兜底
   ② 构造 #broker_message{reliability=fsync,
                          proto_meta=#{qos=>1, packet_id=>P, retain=>0},
                          tenant_id=Principal 的 tenant}
      S2.5：MQTT 5 PUBLISH properties 只进 proto_meta（ADR-017 §17.2），
      headers 仍是空 map；3.1.1 的 proto_meta 形状完全不变。
   ③ broker_router:env/0 → broker_router_hash:route(T, Ctx, Env)
        → [{PartitionId, [SubRef...]}]        ← 调用者进程内查 ETS
        （`route/3` 是 ADR-010 的唯一分区判据；即使无人订阅也返回
          `{ok,[{HashPartition, []}]}`，绝不返回空列表）
   ④ broker_queue:append(PartitionId, Msg, fsync)
        → 取注册表句柄（ETS 读）
        → Backend:append(Hd, [Msg], fsync)   ← 调用者进程内 encode，再进 wal_writer
             wal_writer 分配 offset（先分配）→ 攒批 → fsync → 逐个 reply
   ⑤ 收到 {ok,[Offset]} 后才发 PUBACK      ← ADR-007 硬契约点
   ⑥ cast {new_messages, P, From, To} 给分区 worker（异步扇出）
```

顺序上只有两条是刚性的：

- **⑤ 依赖 ④ 返回，且先于 ⑥ 的投递**。PUBACK 必须代表"已按声明的可靠性落盘"。
- **⑥ 失败不回滚 ⑤**。下游抖动不撤销生产者已获得的承诺；恢复靠订阅层 cursor 追赶与重投。

`broker_queue:append/3` **不经过** `broker_queue_partition`。真正的串行化点是每分区唯一的 `wal_writer`；若在中间加一次 `gen_server:call`，每批会退化成 1 条，group commit 的 17× 收益归零（设计文档 §5.1 T6 批注，`broker_queue_SUITE` 用挂起 worker 与 writer 计数器两例锁死）。

### 5.1.1 MQTT 5 properties → proto_meta（S2.5）

| MQTT 5 property | `proto_meta` key | 备注 |
|---|---|---|
| User Property | `mqtt_user_properties` | `[{K, V}]`，重复键保序累积 |
| Payload Format Indicator | `mqtt_payload_format` | `0`/`1` |
| Content Type | `mqtt_content_type` | UTF-8 binary |
| Response Topic | `mqtt_response_topic` | UTF-8 binary（不做请求/响应语义） |
| Correlation Data | `mqtt_correlation_data` | binary |
| Message Expiry Interval | 不单独存键 | 与 `message_ttl_ms` 合流成 `expires_at` 的绝对期限，出站按剩余秒数回填 |

出站 PUBLISH 从同一组键重建 properties；3.1.1 编码路径不读这些字段，因此线上字节不变。

### 5.2 Subscribe 与投递

```
SUBSCRIBE(Filter, QoS)
  → broker_subscription:subscribe/3
      ① 铸 SubRef = {ClientId, Filter}（确定性，boot 可重铸）
      ② broker_router:register(Filter, SubRef, #{})   ← 先注册；失败则什么都没写
      ③ 每分区取 live High 作为初始 cursor 原点
      ④ fsync {clientid}.subs（集合 + 原点一起落盘）  ← R1b 零容忍
      ⑤ 写入内存实体/游标行
  → SUBACK（请求 QoS 超过 mqtt_max_qos 时回 0x80，不建订阅）
```

投递侧（`mqtt_conn_fsm` 内的 pull loop）：

1. 每轮先 `broker_subscription:subscriptions(ClientId)` 重读订阅集合（消化并发的 UNSUBSCRIBE）。
2. 起点 = 该分区上本 client 各订阅 cursor 的 `min`；单条消息是否相关由**各行自己的 cursor** 判定。
3. `broker_queue:read_from/3` 按 offset 顺序拉一页（`pull_page_limit`，默认 32）。
4. 命中集合的投递 QoS = `min(1, max(命中订阅的 QoS))`；由于只有 QoS1 发布进日志，纯 QoS0 命中就是 QoS0 直投、不欠 ack。
5. 不命中任何 filter 的消息也要 **read-and-skip** 推进 cursor，否则 `committed = min(活 cursor)-1` 会被第一条不相关消息永久钉住。
6. QoS1 发出后进 in-flight 集合；收到 PUBACK → `broker_subscription:ack/3` → 游标按连续前缀前进 → 周期性 `advance_committed`。
7. 窗满（`max_inflight`）不烧下一轮（`stalled`），等 PUBACK 或哨兵 tick；纯 DUP 重发是 `idle`。这是防重发风暴的机制。

**离线积压（clean_session=false）**：没有 broker 侧内存离线队列，积压就是该订阅 cursor 滞后；重连从 cursor 起 `read_from` 追赶，按 offset 顺序统一投递。

### 5.3 会话建立、接管与断开

| 事件 | 动作 |
|---|---|
| CONNECT（认证通过） | `broker_subscription:attach(ClientId, Pid, Persist=not clean_session)`。返回旧 pid 时说明发生了 **takeover**：新连接保留，向旧 pid 发 `{mqtt_takeover, NewPid}`，旧连接停止且**不写** `.inflight` |
| CONNACK | `session_present=1` 当且仅当 `clean_session=0` 且订阅层能查到该 client 的非空订阅集合（`broker_subscription:session/1`） |
| 正常 DISCONNECT | 停止进程；不发布 Will（v5 `disconnect` 带 `will` 时按约定处理，见 ADR-018 §18.1）；`.inflight` 保留给重连 |
| 异常断连 | 同上；`.inflight` 保留；订阅与游标不动 |
| clean_session=true 断开 | `detach` 拆除整个会话：实体、Router 索引、三个文件 |
| 存储 writer 崩溃 | 调用者拿到 `{error,{stale_handle,...}}`，分区 worker `reattach` 重新 `recover/2`；**不自动重试 append**（可能已消耗 offset，重试会制造重复） |

### 5.4 已知的行为边界（不是 bug）

- 慢消费者没有隔离/背压：一个卡住的连接只会卡住自己的 pull loop，但其 cursor 会钉住该分区 committed（多留数据，不丢）。
- QoS0 订阅离线时没有 conn 替它 skip，cursor 停在原点 → 该分区 committed 偏低（同上，方向是安全的一侧）。
- 没有重试定时器/指数退避：QoS1 重发只在重连或 DUP 路径发生。
- `kill -9` 已验证；掉电（`os_buffer` 真实窗口）由 S2-B 的 `tools/fault/os_buffer_loss.py` / `tools/docker/s2_powerloss.py` 注入验证。

### 5.5 指标采集（S2-A）

```
热路径（调用者进程）                          读路径（任意进程，纯读）
mqtt_conn_fsm ──inc/observe──┐
wal_writer ────inc/observe───┤→ counters/atomics ──► broker_metrics:snapshot/0
broker_storage_wal:recover ──┤                              │
broker_subscription_worker ──┘                              ├→ broker_metrics_server
                                                            │   每 report_interval_ms 一行
                                                            └→ 测试 / S2-I 基准
```

- 写入不经过进程、不分配键：键表在启动时固定，索引存 `persistent_term`；`snapshot/0` 按 `keys/0` 顺序读数组。
- `enabled=false` 时写 API 仍是 `ok`（`gauge_add/2` 返回 0），`snapshot/0` 返回全零，键集合不变。
- `connections_current`/`connections_peak` 是唯一需要读改写的地方：`gauge_add/2`（原子 `add_get`）维护当前数，只有更高时才 `gauge/2` 写峰值。
- `subscription_backlog`/`cursor_lag` 由 `broker_subscription_worker` 每次 cursor tick 采样（默认 100 ms）；`queue_depth`/`segment_bytes` 由分区 `wal_writer` 写，多分区共用一个键，最后写入者覆盖 ⇒ 视为近似全局值，分区精确值读 `broker_queue:watermarks/1`。
- `redelivery_total`/`dlq_size`/`retention_*` 与 `cloudevents_*` 的埋点已分别随 S2-E / S2-F / S3 接入（键集合冻结不变）。

---

## 6. 配置面

系统没有 `broker_config` 应用，所有配置来自 `config/sys.config.src`（release 会复制成 `releases/Vsn/sys.config`），读取一律用 `application:get_env/3`（OTP 28 的 `get_env/2` 返回 `{ok,V}`，设计文档 §13 J3）。关键项：

| 键 | 默认 | 作用 |
|---|---|---|
| `broker_storage.backend` | `broker_storage_wal` | 二选一：`broker_storage_wal` / `broker_storage_memory` |
| `broker_storage.data_dir` | 绝对路径 | WAL 根目录；必须在 release 树外（J8），且不能是 tmpfs/ramfs（J11） |
| `broker_storage.wal` | 见下 | `batch_size=32`、`max_batch_size=256`、`batch_delay_ms=#{os_buffer=>0, fsync=>1}`、`index_anchor_interval=256`、`segment_max_records=100000` |
| `broker_router.backend` / `partitions` | `broker_router_hash` / 4 | 路由实现与分区数；必须与 `broker_queue.partitions` 一致 |
| `broker_queue.partitions` | 4 | 与 `broker_router.partitions` 必须一致；boot 的 recover 清单来源 |
| `broker_subscription.data_dir` | 与 storage 相同 | `.subs`/`.cursors`/`.inflight` 根目录 |
| `broker_subscription.cursor_max_regress` / `cursor_flush_interval_ms` / `inflight_flush_interval_ms` | 64 / 100 / 100 | 有界回退窗口；`1/0/0` 选择近零回退档 |
| `broker_retain.data_dir` | 与 storage 相同 | retained 快照 `retain/retain.v1` 的根目录（S2-C） |
| `broker_meta.data_dir` | `data/mnesia` | Mnesia 控制面根目录；Mnesia 在其下追加 `Mnesia.<node>`（S4） |
| `broker_cluster.data_dir` | `data/ra` | Metadata Raft 根目录；ra 在其下追加 `<node>`（S4） |
| `broker_security.users` / `acl` | `guest/guest`、`allow` | 明文用户表与 allow-all ACL 桩 |
| `broker_event.policy` | `pass_through` | 五档均已实现（S3，ADR-020）：`disabled` / `pass_through` / `validate_if_present` / `required` / `normalize`；未知值在 `apply_policy/2` 报 `{invalid_policy, _}` |
| `broker_event.policies` | `[]` | 按 `{Tenant, Destination, Policy}` 的首次匹配覆盖全局策略；`<<"*">>` 为租户通配，Destination 支持 MQTT `+`/`#` |
| `broker_metrics.enabled` / `report_interval_ms` | `true` / `60000` | 指标总开关；`0` 不启动上报进程，非正整数使 app 启动失败（响亮失败，不静默关掉可观测性） |
| `broker_proto_mqtt.listeners` | release 配 1883；`.app.src` 默认 `[]` | 每个 map 一个 listener；连接级 knob 随 map 下发（L20 seam） |
| `broker_proto_mqtt.mqtt_max_qos` | 1 | 必须与后端 capability 一致；内存档必须为 0 |
| `broker_proto_mqtt.max_inflight` / `pull_page_limit` / `pull_poll_interval_ms` / `inflight_snapshot_ms` / `max_keepalive_ms` | 64 / 32 / 1000 / 20 / 60000 | 投递循环与 keepalive 上限 |

`vm.args.src` 另设 `-name`、`-C multi_time_warp`、`+sbwt none`（避免忙等调度器抢 WAL 写者）、`+Q 1000000`。

---

## 7. 存储与恢复的边界

`broker_storage` 只拥有**分区日志**与**全局水位**；`broker_subscription` 拥有**每个订阅的位置**。这条分界是整个恢复语义的基础：

- 存储侧的 committed 只用于 S2 的 retention/truncate，不参与 MQTT 投递决定；
- 订阅侧的 `.cursors` 是"重连后从哪读"的唯一真相；
- 两者都允许在崩溃后**回退**，但方向只能偏低（多投/重复），绝不允许偏高（丢数据）。

细节（记录格式、段轮转、group commit、双层刷盘、R1a~R7 矩阵）见 [`storage-and-recovery.md`](storage-and-recovery.md)。

---

## 8. 验证与证据索引

| 证据 | 内容 | 位置 |
|---|---|---|
| 单元/契约/集成 | `rebar3 ct` → **673/673**，`apps/*/test/` 与 `test/integration/` 的 **43 个套件全部被执行**（含 `broker_storage_contract.hrl` 驱动的两档后端各 13 例契约），零 `FAILED` 明细、零 `Skipped` | `apps/*/test/`、`test/integration/`、`apps/broker_cluster/test/` |
| 静态检查 | `rebar3 dialyzer` 0 warning；`rebar3 xref` 167 条均为预期 unused-export | CI/本地 |
| 指标端到端 | `broker_metrics_SUITE` + `s1_e2e_SUITE` 的 `metrics_count_publish_deliver_and_connections`（wal/memory 各一次） | `apps/broker_metrics/test/`、`test/integration/` |
| 崩溃恢复 | 真实容器 + 宿主 ext4 + `docker kill -s KILL`：R1a~R7 × 默认/零回退两档；S2 电源/OS 缓冲注入 | `tools/docker/`、`tools/fault/` |
| 真实客户端 | paho-mqtt 2.x / mosquitto 2.1.2（MQTT）、websocket-client、nats.go、amqp091-go、kafka-clients、官方 Pulsar Java Client；六协议全绿 | `test/protocol-compatibility/` |
| 集群 | 3 节点 `peer`：组形成、Leader 切换、失去多数派、迁移后 Fencing | `apps/broker_cluster/test/broker_cluster_dist_SUITE.erl` |
| 性能 | `docs/benchmarks/2026-11-09-s6-capacity.md`、`2026-11-09-s6-gameday.md` | `docs/benchmarks/` |
| 端到端 | `wal`/`memory` 双组同一组 7 用例 = **14/14**（T13 交付时 6 例 = 12/12，S2-A 追加 metrics 端到端后为 7 例） | `test/integration/s1_e2e_SUITE.erl` |

各阶段 Gate 的验收状态见 `docs/roadmap.md` 与 `progress.md`。

---

## 9. S7 及之后的演进位（未实现）

下列方向**尚未实现**，只作为后续演进的边界说明。`docs/security.md` §12 与 `progress.md` 有同一份清单。**S7 的两项交付已全部结案（2026-11-09 / 2026-11-10）**：六协议统一走 `broker_security` 身份表（`docs/security.md` §2.1）；各协议标准 CloudEvents Binding 的结论是「统一 `ce-*` 头通道是本 Broker 的项目定义，逐字实现不立项」，逐协议差异见 `docs/roadmap.md` §9.1 与 `docs/protocol-compatibility.md` §4.10。README §10.2 的 W3C trace context 传播（`docs/security.md` §5.1）也已实现，表格里的「可观测性导出层」特指 OTel 等外部导出器，不是传播本身；「官方 Binding 逐字实现」这一行是**已给出结论、不改实现**的演进位，不是遗漏的待办。

| 方向 | 阶段 | 会动到的结构 | 保持不动的契约 |
|---|---|---|---|
| 官方 CloudEvents Binding 逐字实现（差异已逐条列明） | S7 之后（结论已出，不立项） | 需按协议分流：MQTT 5 去前缀、Kafka 换 `ce_`、NATS 补 percent-encoding、WS 加 `cloudevents.json` 子协议、AMQP 迁 1.0 | 统一 `ce-*` 头通道继续作为默认；非无损点见 `docs/protocol-compatibility.md` §4.10 |
| 跨节点/复制通道 TLS | S7 之后 | `broker_cluster` 的 ra 复制链路 | Metadata Raft 语义与 Fencing 不变 |
| Schema Registry 内容校验 | S7 之后 | `broker_event` 校验链新增校验器 | 五档策略与冻结的指标键不变 |
| 审计日志签名与外部 KMS | S7 之后 | `broker_config_audit`、`broker_security` 的密钥来源 | 现有脱敏快照与 ACL 判定不变 |
| 每条规则可配置的「未知扩展属性拒绝」 | S7 之后 | `broker_event` 策略链 | 五档策略与冻结的指标键不变 |
| 去重/隔离钩子推广到五协议 | S7 之后 | WS/NATS/AMQP/Kafka/Pulsar 发布路径补 `broker_event:duplicate/1` / `quarantine/2` | 五档策略与 MQTT 现有行为不变；见 `docs/cloudevents-mapping.md` |
| OTel 等可观测性导出层 | S7 之后 | 在 `broker_metrics:snapshot/0` 之上加 OTel 等导出 | 计数点与冻结的键集合不变；`broker_event_trace` 的校验与门控已实现，不因导出层而改动 |
| `require_auth` 接进 MQTT CONNECT | S7 之后 | `mqtt_conn_fsm:authenticate/2` 增加 `auth_required(<<"mqtt">>)` 分支 | MQTT 现有的匿名 `client_id` 即 principal 语义（S1 冻结）不变；见 `docs/security.md` §2.2 现状边界 |
| `broker_meta` 用户表接进数据面认证 | S7 之后 | `broker_security:users/0` 增加 mnesia 读取源 | `authenticate/2`、`authenticate_conn/1` 的对外形态与错误语义不变；ADR-021 §D7 冻结后补充 |
| `topic_meta` / `queue_meta` / `binding_meta` 的运行时读取方 | S7 之后 | 路由/队列/Topic-Binding 管理路径 | 三张表已建表、有泛型 CRUD；接入读取方不改变现有路由与队列语义 |

---

## 10. 术语

| 术语 | 含义 |
|---|---|
| SubRef | `{ClientId, Filter}`，订阅层铸造的确定性标识；Router 只存不解释 |
| cursor | "下一条要读的 offset"，per `(SubRef, PartitionId)`，归订阅层 |
| committed | "最后已确认的 offset"，存储层的全局水位；由 `min(活 cursor)-1` 驱动 |
| in-flight | 已发出未 ack 的 QoS1 消息集合，每连接一份，持久化为不透明 blob |
| boot step 1~4 | 见 §4.2；失败一律整体启动失败 |
| stale handle | writer 崩溃后失效的存储句柄（含死 pid / 死 ETS tid），读取会 `badarg` |
| L1 / L2 | ADR-007 的 `os_buffer` / `fsync` 可靠性等级 |
