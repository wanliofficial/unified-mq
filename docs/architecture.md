# UnifiedMQ 架构

- **文档版本**：v1.0（S6，2026-11-09）
- **状态**：描述当前已实现并实测的架构事实（S0–S6）。S6 之后（现归 S7 及之后）明确未实现的方向在 §9 单列，不与现状混写
- **读者**：接手实现的工程师、评审人、需要判断“某个行为归哪一层”的测试作者
- **对应**：`docs/roadmap.md`、`docs/design/`（各阶段设计基线）、ADR-001/002/004/007/009/010/011/016/017/018/019/020/021
- **证据口径**：本文件中“已实测”来自 `rebar3 ct`（**823/823**，**52 个套件**）、`tools/docker/`（T14/S2 故障注入）、`test/protocol-compatibility/`（六协议真实客户端）、`apps/broker_cluster/test/`（3 节点 peer 的 `broker_cluster_dist_SUITE`）与 `docs/benchmarks/` 的产物；引用处给出路径

> 本文只回答三个问题：**有哪些应用/进程、谁在什么进程里干活、一次消息从进到出走哪条路**。磁盘格式、刷盘与恢复的细节见 [`storage-and-recovery.md`](storage-and-recovery.md)，协议能力与对端兼容性见 [`protocol-compatibility.md`](protocol-compatibility.md)。

---

## 1. 范围与设计约束

当前系统是一个支持 **MQTT 3.1.1/5.0、WebSocket、NATS、AMQP 0-9-1、Kafka、Pulsar** 六种应用协议的 broker，S8 又在其上补了三种 **MQTT 系传输**（MQTT over WebSocket、MQTT-SN 1.2、MQTT over QUIC），具备本地内存/WAL 两种存储后端、复制提交（Metadata Raft + `replicated_commit`）、CloudEvents 策略层与 S6 管理面。可靠性等级覆盖 ADR-007 的 L1 `os_buffer` 与 L2 `fsync`；跨节点复制与多数派由 S4 的 Raft 控制面/复制路径承担。架构上的硬约束来自已冻结的 ADR：

| 约束 | 来源 | 在代码里的落点 |
|---|---|---|
| 协议适配层不得直接操作磁盘格式 | ADR-001 | `mqtt_conn_fsm` 只经 `broker_router` / `broker_queue` / `broker_subscription` 三个门面接触内核，不 include 任何存储模块（路由只 include `broker_router.hrl` 的 `#route_ctx{}`） |
| 统一消息模型与 CloudEvents 分离，`event` 只是字段位 | ADR-004/009 | `#broker_message{}` 带 `event` 槽；`broker_event` 默认 `pass_through` 恒等，仅在按租户/目的地显式开启时才解析并写 `event`（见 §2.2） |
| 可靠性承诺显式化：ACK 时机与 sync mode 一一对应 | ADR-007 | `mqtt_conn_fsm` 在 `broker_queue:append/3` 返回后才发 PUBACK |
| Router 不串行化，路由在发布者进程内完成 | ADR-010 | 句柄/索引在 `protected` ETS，`route/3` 无 `gen_server:call` |
| 存储只拥有全局 committed 水位，per-sub 位置归订阅层 | ADR-011 | `broker_storage` 的 API 没有订阅概念；cursor 全在 `broker_subscription` |
| 分区创建是带外动作，boot 只 recover | ADR-011 §11.2 | `create_partition/1` 是唯一 `create` 调用者；boot 缺分区即整体启动失败 |
| 协议适配层之间互不引用 | ADR-001 | 各 `broker_proto_*` 只依赖内核门面，彼此没有依赖边。**ADR-001 的原文是「不得依赖其他 Adapter 的**内部**代码」**，S8-B/S8-D 依赖的是 `broker_proto_mqtt` 的**公开面**（`mqtt_conn_fsm` 与 `mqtt_transport` 行为——它们跑的就是那一份引擎），方向单向、无反向边，没有第二份 MQTT 实现 |

**S7 及之后明确未实现**（免得把"还没做"误读成"没画出来"）：S7 的全部验收项已于 2026-11-10 结案——六协议统一身份表见 `docs/security.md` §2.1，各协议标准 CloudEvents Binding 的结论见 `docs/roadmap.md` §9.1（结论是「统一 `ce-*` 是本 Broker 的项目定义，逐字实现不立项」，逐协议差异已列明）。S7 之后明确未实现的是：跨节点/复制通道 TLS、Schema Registry 内容校验、审计日志签名与外部 KMS、每条规则可配置的「未知扩展属性拒绝」、去重/隔离钩子推广到其余协议（五协议 + S8-C 的 MQTT-SN）、OTel 等可观测性导出层，以及 `require_auth` 接进 MQTT CONNECT、`broker_meta` 用户表与数据面认证接线、`topic_meta`/`queue_meta`/`binding_meta` 的运行时读取方。README §10.2 的 trace context **传播本身已实现**（2026-11-09，`broker_event_trace`，见 `docs/security.md` §5.1），未实现的是把指标导出到外部系统。清单与 `docs/security.md` §12、`progress.md` 一致。

---

## 2. 应用划分

### 2.1 24 个应用

```
apps/
├── broker_core/          仅 include/broker_message.hrl 与公共类型；无模块、无进程、无 supervisor
├── broker_metrics/       S2-A：counters/atomics 指标 + snapshot/0 + 周期单行日志（无热路径进程）
├── broker_meta/          S4：Mnesia 控制面 11 表、版本/对账、outbox、用户哈希与 ACL（不含消息正文）
├── broker_cluster/       S4：ra Metadata Raft Group、归属缓存、Epoch Fencing、复制与 quorum
├── broker_storage/       broker_storage behavior + 内存/WAL 两实现 + WAL 内部八个模块（`wal_codec`/`wal_gc`/`wal_group_committer`/`wal_index`/`wal_meta`/`wal_recovery`/`wal_segment`/`wal_writer`）
├── broker_router/        broker_router behavior + broker_router_hash 实现（内存索引）
├── broker_queue/         分区层：存储句柄生命周期、append/read 编排、{new_messages} 扇出
├── broker_subscription/  订阅、per-sub cursor、投递唤醒、三层会话持久化
├── broker_retain/        S2-C：每 topic 一条 retained 消息，retain/retain.v1 快照持久化
├── broker_event/         CloudEvents codec/binding/五档策略/去重（S3）
├── broker_security/      username/password 认证与 allow-all ACL 桩（纯函数）
├── broker_admin/         S6：HTTP 管理面（health/metrics/status/config/backup/tls reload）
├── broker_proto_mqtt/    MQTT 3.1.1/5 编解码 + 连接 FSM + ranch listener（S8：mqtt_transport 传输抽象，可跑 TCP/TLS/WS/QUIC）
├── broker_ws/            S8-A：纯 RFC 6455 库（ws_frame 帧编解码 + ws_handshake 握手），无进程、无配置，供两个 WS 前端共用
├── broker_proto_ws/      S5：WebSocket（RFC 6455）+ JSON/binary 消息 API
├── broker_proto_mqtt_ws/ S8-B：MQTT over WebSocket（MQTT 3.1.1 §6 / 5.0 §6），RFC 6455 传输跑同一个 mqtt_conn_fsm
├── broker_proto_mqtt_sn/ S8-C：MQTT-SN 1.2 网关（UDP），一个 socket 服务全部会话，不复用 mqtt_conn_fsm
├── broker_proto_mqtt_quic/ S8-D：MQTT over QUIC（CN Draft 3 单流模式），同一 mqtt_conn_fsm 跑在 quicer 流上
├── broker_proto_nats/    S5：NATS core（PUB/SUB、queue group、request/reply）
├── broker_proto_amqp/    S5：AMQP 0-9-1（exchange/queue/binding、publish/consume/ack）
├── broker_proto_kafka/   S5：Kafka wire（Produce/Fetch/Metadata + consumer group）
├── broker_proto_pulsar/  S5：Pulsar binary（produce/consume/ack/seek）
├── broker_proto_agent_rpc/ S9-A：agent 协议纯库（JSON-RPC 2.0 信封、HTTP/1.1 帧、SSE 帧、版本协商、凭据 helper），无进程、无 listener、无自有 env
└── broker_proto_a2a/     S9-A/B/C：A2A 1.0.0 JSON-RPC 前端（Task 生命周期 + Agent Card 发现 + SSE 流式 + 出站 Push webhook），默认 `listeners => []`
```

| 应用 | 依赖（`.app.src`） | 自己的进程 | 对外入口 | 拥有什么状态 |
|---|---|---|---|---|
| `broker_core` | kernel, stdlib | 无 | `broker_message.hrl` 的 `#broker_message{}`、`partition_id()`、`sync_mode()` | 无（纯类型） |
| `broker_metrics` | kernel, stdlib, sasl | `broker_metrics_server`（仅 `report_interval_ms > 0`；指标本身不在进程里） | `inc/1,2`、`observe/2`、`gauge/2`、`gauge_add/2`、`snapshot/0`、`p99/1`、`keys/0` | `counters`/`atomics` 数组 + `persistent_term` 的 `Key → {Kind,Ref,Idx}` 索引（无 ETS） |
| `broker_meta` | kernel, stdlib, sasl, mnesia | `broker_meta_config`（配置版本 gen_server）、`broker_meta_outbox`；表访问本身由调用方同步执行 | `ensure_schema/0`、`tables/0`、通用 `put/get/delete/all/clear` + **13 张表**的类型化访问器（`put_tenant/get_tenant`、`put_user/get_user`、`put_acl/acls`、`put_partition/get_partition`、`put_node/nodes`、`version/bump_version`、`enqueue/pending_outbox/mark_published`、`put_agent_card/1`、`get_agent_card/2`、`delete_agent_card/2`、`agent_cards/1`、`put_push_config/1`、`get_push_config/3`、`push_configs/2`、`delete_push_config/3`、`delete_push_configs/1`）、`reset/0` | Mnesia 表（控制面）；schema 不存在时创建，存在时复用。13 张表**全部建表并可有行**，但 `topic_meta` / `queue_meta` / `binding_meta` 目前**没有运行时消费者**（只有 `tables/0` 与套件引用，泛型 `put/get` 可写可读）——这三张表是控制面预留，接入路由/队列/binding 的读取方列为 S7 之后。第 12 张 `agent_card`（S9-B，ADR-026 §D3 显式修订 ADR-002）有真实消费者：`broker_proto_a2a` 的 well-known/catalog/`GetExtendedAgentCard` 读它，`a2a_card:seed/0` 是唯一写入者。第 13 张 `push_config`（S9-C，ADR-026 §D6 显式修订 ADR-002）由 `broker_proto_a2a` 的 Push 四方法读写，**行内不存密钥**（密钥在 `push_auth_secrets` env，按 scheme 名取） |
| `broker_cluster` | kernel, stdlib, sasl, crypto, ra, broker_core, broker_meta, broker_metrics | `broker_cluster_cache`（归属/epoch 缓存 gen_server）与 ra server `{broker_cluster_meta, Node}`（模块 `broker_cluster_meta`，状态机 `broker_cluster_meta_machine`）；`broker_cluster_rep` 是纯函数模块，无进程 | `assign/4`、`migrate/4`、`finish_migration/2`、`remove/1`、`owner/1`、`epoch/1`、`replicas/1`、`authorize_append/2`、`guard_append/3`、`replicate/2`、`snapshot/0` | 本地 ETS 归属/epoch 缓存；ra 日志与快照；`persistent_term` 队列钩子 |
| `broker_storage` | kernel, stdlib, sasl, broker_core, broker_metrics | `broker_storage_wal_sup` + 每分区 `wal_writer`（WAL 档）／`broker_storage_memory_store`（内存档） | `broker_storage` behavior：`create/recover/close/append/read_from/advance_committed/truncate/capability` | WAL 段与 `meta.log`；内存表 |
| `broker_router` | kernel, stdlib, sasl, broker_core | `broker_router_hash_store` | `route/3`（`broker_router` behavior 回调，由 `broker_router_hash` 实现）、`broker_router:env/0`、`register/3`、`unregister/1` | `Filter → [SubRef]` 内存索引（**不持久化**） |
| `broker_queue` | kernel, stdlib, sasl, broker_core, broker_metrics, broker_storage, broker_router, broker_security | `broker_queue_store` + 每分区 `broker_queue_partition` | `append/3`、`read_from/3`、`watermarks/1`、`capability/1`、`subscribe/2`、`create_partition/1` | 分区句柄注册表、每分区订阅者集合 |
| `broker_subscription` | kernel, stdlib, sasl, broker_core, broker_metrics, broker_storage, broker_router, broker_queue | `broker_subscription_worker` | `subscribe/3,4`、`unsubscribe/2`、`ack/4`、`claim/4`、`release/4`、`retry/4`、`inflight/2`、`attach/3`、`detach/2`、`dispatch/2`、`cursor/2`、`subscriptions/1`、`session/1`、`role/2` | 五张 ETS（实体 / client→SubRef 成员索引 / 游标 / 连接绑定 / in-flight）+ `data/sessions/*` 三件套 |
| `broker_event` | kernel, stdlib, sasl, broker_core, broker_metrics, broker_router, broker_queue | `broker_event_dedup`（去重窗口 gen_server + ETS） | `policy/0`、`policy_for/1`、`apply_policy/1,2`、`dedup_window/1`、`duplicate/1`、`quarantine/2`、`broker_event_codec`（CE 1.0 JSON / Binary）、`broker_event_binding`（MQTT Binding + 损失报告） | 去重 ETS（`{Tenant,Destination,source,id} → 过期时刻`，默认关闭） |
| `broker_security` | kernel, stdlib, sasl, ssl, broker_core | 空 supervisor | `authenticate/2`、`authorize/2`、`tenant_id/1`（纯函数读 env） | 无（配置即状态） |
| `broker_retain` | kernel, stdlib, sasl, broker_core, broker_metrics | `broker_retain`（gen_server） | `put/2`、`delete/2`、`lookup/1`、`entries/0`、`stats/0`、`flush/0` | 每 topic 一条 retained 消息（ETS/内存树）+ `retain/retain.v1` 快照 |
| `broker_admin` | kernel, stdlib, sasl, crypto, broker_core, broker_metrics, broker_security | `broker_admin_listener`（HTTP accept） | `broker_admin_http:handle/4`、`metrics_text/0`、`broker_backup`、`broker_config_audit`、`broker_version:info/0` | 只读快照；备份清单文件 |
| `broker_proto_mqtt` | kernel, stdlib, sasl, ranch, broker_core, broker_metrics, broker_storage, broker_router, broker_queue, broker_subscription, broker_event, broker_security, broker_retain | `mqtt_conn_sup` + 每连接 `mqtt_conn_fsm` + 嵌入的 ranch listener | `mqtt_frame`（编解码）、`mqtt_filter`（Annex A 校验）、`mqtt_conn_fsm`、`mqtt_listener`、`mqtt_transport`（行为）与 `mqtt_transport_tcp`（TCP/TLS 实现） | 每连接内存视图（packet_id 分配器、in-flight 集合、keepalive 定时器）+ 传输 handle |
| `broker_ws` | kernel, stdlib, crypto | **无进程、无配置** | `ws_frame`（RFC 6455 帧编解码）、`ws_handshake`（`accept/1,2` 握手 + 子协议协商）、`max_handshake/0` | 无（纯函数库） |
| `broker_proto_ws` | kernel, stdlib, sasl, ranch, broker_ws, broker_core, broker_metrics + 内核 6 app | 每连接 `ws_conn` + ranch listener | `ws_conn`、`ws_listener`（帧编解码已委托给 `broker_ws` 的 `ws_frame`） | 每连接分片缓冲、订阅集合 |
| `broker_proto_mqtt_ws` | kernel, stdlib, sasl, ranch, broker_ws, broker_proto_mqtt, broker_core, broker_metrics, broker_retain + 内核 6 app | 每连接 `mqtt_ws_conn` + ranch listener；MQTT 引擎 `mqtt_conn_fsm` 由 `broker_proto_mqtt` 的 `mqtt_conn_sup` 托管 | `mqtt_ws_conn`、`mqtt_ws_listener`、`mqtt_transport_ws`（`mqtt_transport` 的 WS 实现） | 每连接 HTTP 升级缓冲、帧分片缓冲；MQTT 状态全在 FSM 里 |
| `broker_proto_mqtt_sn` | kernel, stdlib, sasl, broker_core, broker_metrics, broker_storage, broker_router, broker_queue, broker_subscription, broker_event, broker_security, broker_retain | `mqtt_sn_listener`（一个 UDP socket + 一个 reader）、`mqtt_sn_conn_sup` + 每客户端 `mqtt_sn_conn` | `mqtt_sn_codec`（27 种报文）、`mqtt_sn_topics`（三种 topic id 表）、`mqtt_sn_filter`（本地 filter 校验，不依赖 `broker_proto_mqtt`）、`mqtt_sn_listener`、`mqtt_sn_conn` | `{IP,Port}`→会话、`client_id`→会话、休眠登记三张表在 listener；每会话 topic id 表、in-flight 窗口、两个 timer；订阅/游标/retain 都在下层 app |
| `broker_proto_mqtt_quic` | kernel, stdlib, sasl, broker_proto_mqtt, broker_core, broker_metrics, broker_storage, broker_router, broker_queue, broker_subscription, broker_event, broker_security, broker_retain；`optional_applications: [quicer]` | `mqtt_quic_listener` + 每 listener 一个 acceptor 进程 + 每 QUIC 连接 `mqtt_quic_conn`；MQTT 引擎 `mqtt_conn_fsm` 由 `broker_proto_mqtt` 的 `mqtt_conn_sup` 托管 | `mqtt_quic_listener`、`mqtt_quic_conn`、`mqtt_transport_quic`（`mqtt_transport` 的 QUIC 实现）、`mqtt_quic_nif`（唯一触碰 `quicer` 的模块） | 每 listener 的连接表与配置；每 QUIC 连接的流信用计数；MQTT 状态全在 FSM 里。`quicer` 缺失时整个 app 仍能启动，只是没有 listener |
| `broker_proto_nats` | kernel, stdlib, sasl, ranch, broker_core, broker_metrics + 内核 6 app | 每连接 `nats_conn` + 订阅投递进程 + ranch listener | `nats_conn`、`nats_listener` | 每连接 sid 表、queue group 成员关系 |
| `broker_proto_amqp` | kernel, stdlib, sasl, ranch, broker_core, broker_metrics + 内核 6 app | 每连接 `amqp_conn`、`amqp_topo`（exchange/queue/binding 注册表） | `amqp_codec`、`amqp_conn`、`amqp_listener` | exchange/queue/binding 拓扑、delivery tag、unacked 集合 |
| `broker_proto_kafka` | kernel, stdlib, sasl, ranch, broker_core, broker_metrics + 内核 6 app | 每连接 `kafka_conn`、`kafka_group`（consumer group 协调） | `kafka_codec`、`kafka_conn`、`kafka_listener` | 每连接 topic/partition 游标、group 成员与 offset |
| `broker_proto_pulsar` | kernel, stdlib, sasl, ranch, broker_core, broker_metrics + 内核 6 app | 每连接 `pulsar_conn`、`pulsar_subscriptions` | `pulsar_codec`、`pulsar_conn`、`pulsar_listener` | 每连接 producer/consumer 表、subscription 游标 |
| `broker_proto_agent_rpc` | kernel, stdlib, sasl, ssl, ranch, crypto, broker_core, broker_security | **无进程、无 listener、无自有 env**（空 application，照 `broker_ws`） | `agent_rpc_json`（JSON-RPC 2.0 信封）、`agent_rpc_error`（错误码/HTTP 映射）、`agent_rpc_version`（`A2A-Version` 解析与协商）、`agent_rpc_http`（HTTP/1.1 请求解析 + 响应/chunk 序列化）、`agent_rpc_sse`（SSE 帧）、`agent_rpc_auth`（Bearer/`X-API-Key` → `broker_security`） | 无（纯函数库；socket 由 adapter 持有） |
| `broker_proto_a2a` | kernel, stdlib, sasl, ssl, ranch, crypto, broker_core, broker_security, broker_meta, broker_router, broker_queue, broker_proto_agent_rpc | `broker_proto_a2a_sup` + `a2a_task_registry`（`one_for_all` 监督 N 个 `a2a_task_shard` + `a2a_task_recovery`）+ 每条配置的 listener（`ranch_embedded_sup`，默认零条）+ 每连接一个 `a2a_conn` 循环进程 | `a2a_listener`（ranch_protocol）、`a2a_conn`（keep-alive 循环）、`a2a_rpc:handle/1`（方法分派与全部门控）、`a2a_task_registry`（`create/3`、`get/1`、`list/3`、`cancel/2`、`apply_event/4`）、`a2a_task`（纯状态机）、`a2a_event`（事件 → `#broker_message{}`）、`a2a_task_projection`（纯折叠）、`a2a_card`（well-known/catalog/ETag/seed） | 每连接的请求计数与 socket；**每 shard 一张 ETS 投影表**（Task 视图，evict 由 boot replay 重建）+ 注册表里的分片名单。Task 事件本身**不在**这里——它们是 `$a2a/task/<id>` 上的普通消息，归 `broker_storage`/`broker_queue`；无 cursor、无 Mnesia |

### 2.2 `broker_event` 的真实状态（S3 已实现）

S3（ADR-020）把 T11 的恒等桩补成完整的 CloudEvents 1.0 层：

- `broker_event_codec`：Structured JSON 与 Binary `ce-*` 双向编解码、核心/可选/扩展属性校验、`data`/`data_base64` 互斥。
- `broker_event_binding`：MQTT 消息检测（v5 `user_properties` → Binary；`content-type`/payload → Structured）、`decode/1`、`to_mqtt/3` 与 `#{mode, target, version, losses, preserved}` 转换报告。
- `broker_event`：五档策略按租户 + 目的地首个匹配；`apply_policy/1,2` 默认 `pass_through`，对普通消息恒等；`required`/`normalize` 对非事件返回 `{error, {event_invalid, event_required}}`；`normalize` 只写 `event`，**不改 payload 字节**。
- `broker_event_dedup`：受监督的有界去重窗口，默认关闭；键 `{Tenant, Destination, source, id}`，进程重启即清空（不承诺跨重启）。
- `quarantine/2`：可选的隔离副本，把原始消息以 `fsync` 追加到可配置 destination，拒绝本身不受隔离成败影响。

**数据路径**：`mqtt_conn_fsm` 的私有函数 `handle_publish1/7` 在路由之后、存储之前调用 `broker_event:apply_policy/1`；拒绝不 ACK（v5 QoS1 PUBACK `0x83`、QoS2 PUBREC `0x83`，3.1.1 关闭连接），去重命中只 ACK 不二次存储。默认策略是 `pass_through`，因此 MQTT 3.1.1 出站字节与 S2 行为完全不变；CloudEvents 是按租户/目的地显式开启的能力，不是全局强制（ADR-004）。

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
broker_proto_agent_rpc ←── broker_proto_a2a  （ADR-026 §D1：adapter → base → kernel；
                          base 只依赖 broker_core / broker_security，不引用任何 adapter）
```

依赖只允许**向下**：协议层可以调内核，内核任何一层都不得反向引用 `broker_proto_mqtt`；`broker_core` 不依赖任何兄弟应用。`xref` 是这条约束的机械检查（当前只剩预期的 unused-export 告警）。

S9 的额外约束：三个 agent 前端（A2A / MCP / ACP）**彼此不引用**，只共享 `broker_proto_agent_rpc`。这意味着 `broker_proto_a2a` 的 `.app.src` 里不出现 `broker_proto_mcp`，反之亦然；共享逻辑只能下沉到 base。

---

## 3. 进程模型与监督树

### 3.1 顶层结构

系统**没有伞形 `broker_app_sup`**：除 `broker_core`（纯类型）、`broker_ws`（纯库）与 `broker_proto_agent_rpc`（纯库，S9-A）外的 **22 个 app 各有自己的顶层 supervisor**，跨 app 的启动顺序交给 OTP 的 `applications` 拓扑序（设计文档 §6 的 T1 批注）。

```
broker_metrics_app ────► broker_metrics_sup                        one_for_one, 5/10s
                          └── broker_metrics_server                 permanent（仅 report_interval_ms > 0）

broker_meta_app ───────► broker_meta_sup                           one_for_one, 5/10s
                          ├── broker_meta_config                    permanent
                          └── broker_meta_outbox                    permanent

broker_cluster_app ────► broker_cluster_sup                        one_for_one, 5/10s
                          ├── broker_cluster_cache                  permanent（先起）
                          └── broker_cluster_meta                   permanent（ra 进程包装）

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

broker_retain_app ──────► broker_retain_sup                         one_for_one, 5/10s
                          └── broker_retain                         permanent

broker_event_app ───────► broker_event_sup                          one_for_one, 5/10s
                          └── broker_event_dedup                    permanent
broker_security_app ────► broker_security_sup                       one_for_all（空）

broker_admin_app ───────► broker_admin_sup                          one_for_one, 5/10s
                          └── broker_admin_listener                 permanent

broker_proto_mqtt_app ──► broker_proto_mqtt_sup                     one_for_all, 1/5s
                          ├── mqtt_conn_sup                         simple_one_for_one
                          │   └── mqtt_conn_fsm × 连接数            temporary
                          └── ranch_embedded_sup                    （ranch:child_spec/5）
                              └── ranch_listener_sup → acceptors / conns sup
                                 （listener child spec restart=transient）

broker_proto_mqtt_ws_app ► broker_proto_mqtt_ws_sup                 one_for_one, 1/5s
                          └── ranch_embedded_sup → listener（同 broker_proto_mqtt）

broker_proto_mqtt_quic_app ► broker_proto_mqtt_quic_sup             one_for_all, 1/5s
                          └── mqtt_quic_listener × N                transient
                              ├── acceptor（spawn_link，accept 循环）
                              └── mqtt_quic_conn × 连接（start_link）  link 到 listener，非监督子进程
                                  └── mqtt_conn_fsm × 流              （mqtt_conn_sup 的子进程）

broker_proto_mqtt_sn_app ► broker_proto_mqtt_sn_sup                 one_for_all, 1/5s
                          ├── mqtt_sn_conn_sup                      simple_one_for_one, 10/1s
                          │   └── mqtt_sn_conn × 客户端             temporary
                          └── mqtt_sn_listener × N                  transient（一个 UDP socket）

broker_proto_ws_app ────► broker_proto_ws_sup                       one_for_one, 1/5s
                          └── ranch_embedded_sup → listener
broker_proto_nats_app ──► broker_proto_nats_sup                     one_for_one, 1/5s
                          └── ranch_embedded_sup → listener

broker_proto_amqp_app ──► broker_proto_amqp_sup                     one_for_one, 5/10s
                          ├── amqp_topo                             permanent（先起：会话期 exchange/queue/binding 表）
                          └── ranch_embedded_sup → listener × N
broker_proto_kafka_app ─► broker_proto_kafka_sup                    one_for_one, 5/10s
                          ├── kafka_group                           permanent（先起：消费组协调）
                          └── ranch_embedded_sup → listener × N
broker_proto_pulsar_app ► broker_proto_pulsar_sup                   one_for_one, 5/10s
                          ├── pulsar_subscriptions                  permanent（先起：游标与未 ack 集合）
                          └── ranch_embedded_sup → listener × N

broker_proto_a2a_app ───► broker_proto_a2a_sup                      one_for_one, 1/5s
                          └── ranch_embedded_sup → listener × N（默认 0 条，`listeners => []`）
```

22 个顶层 supervisor（`broker_core` / `broker_ws` / `broker_proto_agent_rpc` 没有）+ 1 个嵌套的 `broker_storage_wal_sup` + ranch 自己的子树。

### 3.2 重启策略与理由

| 进程 | 策略 | 理由 | 崩溃后的可见行为 |
|---|---|---|---|
| `broker_metrics_sup` / `broker_metrics_server` | `one_for_one`, 5 次/10 s | 上报进程崩溃不应连累任何数据面；指标数组在进程外，重启不丢计数 | 最多丢一个上报周期，计数继续累积 |
| `broker_storage_sup`、`broker_router_sup`、`broker_proto_mqtt_sup`、`broker_security_sup`、`broker_proto_mqtt_quic_sup`、`broker_proto_mqtt_sn_sup` | `one_for_all`, 1 次/5 s | 子系统内的进程共享一个装配前提（如索引 store 必须先于读路径；QUIC/SN 的 listener 一死，它背后的连接/会话就没有可读端口） | 超过强度则整个 app 退出；若是 boot 期失败，application 起不来 |
| `broker_proto_mqtt_ws_sup` | `one_for_one`, 1 次/5 s | MQTT over WebSocket 只有 listener 一类子进程，没有需要一起重启的兄弟 | listener 重启；live 连接由 ranch conns sup 随 listener 一起收掉 |
| `broker_proto_ws_sup` / `broker_proto_nats_sup` | `one_for_one`, 1 次/5 s | 同 `broker_proto_mqtt_ws_sup`：只有 listener 一类子进程 | listener 重启；live 连接由 ranch conns sup 随 listener 一起收掉 |
| `broker_proto_a2a_sup` | `one_for_one`, 1 次/5 s | 只有两类子进程，且 registry 必须早于 listener（boot 契约，ADR-026 §D4）；registry 自己崩了由 `one_for_one` 单独重启整棵 registry 子树，不必连累 listener | listener 重启时 live 连接被收掉；registry 重启 = 全量重放重建投影，正在处理的 Task 请求收到 `internal_error` |
| `a2a_task_registry` | **`one_for_all`, 3 次/10 s** | 分片的 ETS 投影是缓存，日志才是真相；单个分片崩了只有整组重启 + `a2a_task_recovery` 重放才能让"投影等于日志"重新成立。`3/10` 让崩溃循环仍然可见 | 全部 Task 投影重建（耗时 = 全量重放）；期间 `create/get/cancel` 调用方收到 `internal_error`；日志不丢，Task 语义不变 |
| `broker_proto_amqp_sup` / `broker_proto_kafka_sup` / `broker_proto_pulsar_sup` | `one_for_one`, 5 次/10 s | listener 之外还有 app 级的共享进程（`amqp_topo` 的拓扑表、`kafka_group` 的消费组协调、`pulsar_subscriptions` 的游标），它们与被 listen 的连接无关，不该跟着 listener 一起重启 | listener 或协调进程单独重启，另一类不受影响 |
| `broker_meta_sup` / `broker_cluster_sup` / `broker_retain_sup` / `broker_admin_sup` / `broker_event_sup` | `one_for_one`, 5 次/10 s | 配置版本、outbox、归属缓存、retain 树、管理 listener、去重窗口彼此独立；retain/admin 的 `permanent` 子进程从文件/配置重建，`broker_event_dedup` 重启只丢窗口内状态 | 单个进程重启，其余不受影响 |
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

### 3.9 传输抽象：`mqtt_transport` 行为（S8-A）

MQTT 本身对传输不可知——3.1.1 §6 与 5.0 §6 允许 TCP / TLS / WebSocket（QUIC 见 OASIS 单流便条），且帧格式完全一致。S8 之前 `mqtt_conn_fsm` 硬编码 `gen_tcp:send/2` 与 `inet:setopts/2`，带来两个代价：多一种传输就要复制一份状态机，而且 ranch 给的传输被忽略——因此 **MQTT over TLS 从未真正工作**（ssl socket 上每次 write 都失败）。

S8-A 把 FSM 与传输的边界收敛为一个模块名 + 一个不透明 handle，走四个回调（`send/2`、`arm_once/1`、`close/1`、`decode_msg/2`），见 [`mqtt_transport.erl`](../apps/broker_proto_mqtt/src/mqtt_transport.erl)：

- `decode_msg/2` 是语义核心：把邮箱里的消息归一为 `{data,Bin}` | `closed` | `{error,R}` | `other`，且**按 handle 匹配本连接的 socket**——一个节点跑多个 listener 时，A 连接的字节绝不会喂进 B 的状态机；`other` 让 FSM 自己的事件（投递、poll tick、`connect` 交接）照常穿过。
- 实现按依赖归属放置：`mqtt_transport_tcp` 在本 app（同时覆盖 `ranch_tcp` 与 `ranch_ssl`，用 `is_port/1` 分派——`gen_tcp` socket 是 port、`ssl` socket 是元组，且只有 `ssl` 导出 `setopts/2`），`mqtt_transport_ws` 在 `broker_proto_mqtt_ws`（S8-B），QUIC 的实现在 `broker_proto_mqtt_quic`（S8-D），这样本 app 不依赖它们。

握手消息 `{handshake, Ref, Transport, Sock, Timeout}` 带的是 **ranch 自己的** transport（`ranch_tcp`/`ranch_ssl`），不是 `mqtt_transport`：ranch 对 TLS 只做 `transport_accept`，因此协议侧必须自己调 `Transport:handshake/2`（TCP 是 no-op、SSL 才真正完成 TLS 握手）后才能 arm，否则首个 `ssl:setopts` 报 `notsup_on_transport_accept_socket`。


### 3.10 MQTT over WebSocket 的两进程分层（S8-B）

`broker_proto_mqtt_ws` 每连接跑**两个进程**，这个划分就是设计本身：

- `mqtt_ws_conn` 是 ranch 协议进程，拥有 socket，负责 RFC 6455 升级（`ws_handshake:accept/2` 做子协议协商）、帧编解码，并把 binary 帧的 payload 原样转给 MQTT 引擎；
- `mqtt_conn_fsm` 仍在 `broker_proto_mqtt` 的 `mqtt_conn_sup` 下，`transport => mqtt_transport_ws`，其 **handle 是 `mqtt_ws_conn` 的 pid**，不是 socket。

不把帧解码塞进 `mqtt_transport_ws:decode_msg/2`，是因为它必须是 `(Msg, Handle)` 的纯函数，而「半个帧」「升级是否完成」是每连接状态；放进进程字典或 ETS 会用隐藏状态换掉一个干净的行为。

WS 层只把 **binary** 帧当 MQTT 字节：text 帧回 close `1003`；报文与帧**无对齐要求**（payload 交给 FSM 的 `buf` 重组，一个帧里的多条报文逐条处理）；ping/pong/close 是 WS 控制流量，与 MQTT keepalive 无关。`max_frame_bytes` 限制的是帧，`mqtt_max_packet_size` 限制的仍是报文。

背压刻意缺省：`mqtt_transport_ws:arm_once/1` 是语义占位 no-op（WS 层每帧投递后自行 re-arm），`send/2` 异步，`max_inflight` / `receive_maximum` 是唯一窗口控制。对端造成的失败（RST、TLS 握手不完成、re-arm 失败）以 `normal` 结束 WS 进程，并把 `{mqtt_ws_error, self(), Reason}` 交给引擎记日志——非 `normal` 退出会为每条坏 TCP 打一份监督树报告，而对端重置是常态。

### 3.11 MQTT-SN 的单 socket / 多会话分层（S8-C）

MQTT over WebSocket 之上是「一连接两进程」（§3.10）；MQTT-SN 恰好相反——**一个网关只有一个 UDP socket**，所有客户端共享它。这个不对称不是为了整齐，而是 UDP 没有连接可拥有：

```
broker_proto_mqtt_sn_sup (one_for_all, 1/5s)
├── mqtt_sn_conn_sup (simple_one_for_one, temporary)
│     └── mqtt_sn_conn × N        % 每个已 CONNECT 的端点一个；回包直接 gen_udp:send/4
└── mqtt_sn_listener × 1          % 打开 socket，{active, 100}，按 {IP,Port} 分派
```

**listener 拥有 socket，会话不拥有。** `mqtt_sn_listener` 用 `gen_udp:open(Port, [binary, {active, ?ACTIVE_N}, {reuseaddr, true}, {broadcast, true}])` 打开一个 socket，收 `{udp, Sock, IP, Port, Bin}` 后解码、按 `{IP, Port}` 找会话，没有就按报文类型决定是新建会话（CONNECT）还是在 listener 进程里就地处理（QoS −1 无会话发布、SEARCHGW）。`{active, 100}` 而不是 `{active, true}`：邮箱必须有界，否则一个打流量的客户端能把 listener 的堆吃干净；读空后由 `{udp_passive, Sock}` 重新 arm。

会话回包**不经 listener 转发**：`gen_udp` 不检查 controlling process，任何进程都能写同一 socket，因此 `mqtt_sn_conn` 直接 `gen_udp:send/4`。这不是省一次消息传递，而是**去掉一个会成为瓶颈的中转进程**——listener 在热路径上只做解码与分派。

**会话键是 `{IP, Port}`，会话身份是 `client_id`。** UDP 端点会变（NAT rebind、客户端换端口），所以这两者必须分开：

- `{IP,Port} → Pid`：同一个端点只跑一个会话，重连前必须先清掉旧 pid；
- `client_id → Pid`：CONNECT 时绑定。同 `client_id` 从**新**端点 CONNECT = takeover，旧会话被关掉且**不发布 Will**（客户端没死，它搬家了）；`{sn_endpoint, Endpoint}` 消息把回包地址改写到新端点。
- `client_id` 与 MQTT 共用一个会话域：一个 MQTT-SN 传感器与一个 MQTT 客户端用同一个 id 就是同一个会话，takeover 照样生效。

**为什么不复用 `mqtt_conn_fsm`。** FSM 的整个生命周期都建立在「有一条会丢的 TCP 连接」上：半开、按 socket 接管、`{handshake, Ref, Transport, Sock, Timeout}` 交接、broker 重启后按持久会话恢复。UDP 上这些前提一个都不成立——没有连接可丢，入站不是字节流而是自带边界的数据报，重传由客户端 DUP 驱动而不是网关定时器。强行合并只会把两套连接语义塞进同一个 state record。会话只复用**下层**：`broker_router`（分区）、`broker_queue`（存储）、`broker_subscription`（游标与唤醒）、`broker_retain`（retained）、`broker_event`（CloudEvents seam），投递走与其它协议完全相同的 `{broker_deliver, ClientId, MaxQoS, Refs, {P, O, Msg}}`。

**三张 topic id 表**，语义各不相同（`mqtt_sn_topics`）：

| 类型 | 归属 | 由谁分配 | 上限 |
|---|---|---|---|
| predefined | 网关全局、只读 | 配置（`predefined_topics`），启动即校验保留 id `0`/`0xFFFE`/`0xFFFF` | 配置规模 |
| normal | **每客户端私有** | 双向 REGISTER：SUBACK 给订阅方分配一个；客户端 PUBLISH 前自己 REGISTER；网关下行遇到客户端未知 topic 时**先发 REGISTER 再发 PUBLISH** | `max_topic_ids_per_client` |
| short | 无表 | 两字符名字**就是** id；非恰好 2 字节直接拒绝 | — |

上行超限回错误码；**下行超限即断开会话**——网关自己编不出合法 PUBLISH，不能假装成功。normal id 的「每客户端私有」是协议性质：订阅方 SUBACK 拿到的 id 在发布方的表里毫无意义。

**休眠是一个状态，不是一条连接。** `DISCONNECT(duration)` = detach：保留订阅与游标，**不新建任何缓冲**（消息留在分区里，靠 cursor 追赶）；`PINGREQ(client_id)` 从新端点唤醒并回放积压。`max_asleep_clients` 超限时按正常断开处理并计 `mqtt_sn_sleep_denied`，不假装睡着再丢掉睡眠期全部消息。

**每会话恰好两个 timer**：keepalive（1.5× 静默发 PINGREQ 探测、2× 判死并触发 Will）与 sleep 到期。没有第三个重传 timer —— MQTT-SN 的重传信号只有客户端的 DUP 位，网关的职责是对重复的 msg id **幂等应答**，而不是自己定时重发。

**认证边界在启动期显式失败。** 1.2 的 CONNECT 没有用户名/密码字段，`broker_security:auth_required(mqtt_sn)` 恒 `false`；`broker_proto_mqtt_sn_app` 因此直接读 `require_auth` 原始列表，命中 `mqtt_sn` 就拒绝启动监听器（`{require_auth_unsupported, mqtt_sn}`）。拒绝粒度是整个 app：一个 socket 服务全部客户端，没有「只对部分客户端要求凭据」的表达力。见 `docs/security.md` §2.2 第 4 条。

### 3.12 MQTT over QUIC 的连接/流两层（S8-D）

OASIS MQTT TC 的《MQTT over QUIC — Single Stream Mode》CN Draft 3 只做一件事：**MQTT 报文一个字节不改**，整条 Network Connection 走**一条客户端发起的双向流**。所以这里既不是「新协议」也不是「新引擎」——`mqtt_conn_fsm` 与 1883/8083 是同一份代码，`mqtt_transport_quic` 是第 3 个传输实现（§3.9）。

多出来的一层来自 QUIC 本身：**流会关，连接不会**。客户端关掉流只是结束这条 MQTT 连接，它可以在同一条 QUIC 连接上新开一条流重新 CONNECT。于是比 TCP 多一个「谁在两次流之间活着」的进程：

```
broker_proto_mqtt_quic_sup (one_for_all, 1/5s)
└── mqtt_quic_listener × N        % 每个配置的 listener 一个；拥有 quicer listener 句柄
      ├── acceptor 进程           % quicer:accept/3 循环；把每条连接交接出去
      └── mqtt_quic_conn × N      % 每条 QUIC 连接一个；拥有连接句柄，循环处理新流
            └── mqtt_conn_fsm     % 每条流一个（mqtt_conn_sup 的子进程），handle = stream
```

**listener 不自己服务连接，acceptor 也不服务**：quicer 的 acceptor 只负责 `accept`，连接句柄的所有权必须显式 `controlling_process/2` 交给 `mqtt_quic_conn`，否则 `{quic, new_stream, ...}` 会落进错误的邮箱。交接顺序是「先移所有权、再通知」（`Pid ! {quic_conn, Conn}`），与 TCP 的 `{handshake, ...}` 同理：一句 `Pid ! connect` 之前，FSM 不许碰它不拥有的句柄。

**服务端必须自己完成 TLS 握手。** `quicer:accept/3` 只报告传输层连接，`quicer:handshake/2` 才真正跑 TLS；而 `{quic, connected, ...}` 投给连接的 owner，所以握手只能由 `mqtt_quic_conn` 在拿到句柄之后做。握手失败（错 ALPN、明文探测、客户端中途走掉）是常态：进程以 `normal` 停止并记一条 info，不占用监督树的重启强度。

**流信用一次性授予，不按需补充。** `peer_bidi_stream_count => max_streams_per_conn`（默认 4）在 `quicer:listen/2` 时给足；`peer_unidi_stream_count => 0`（服务端不发流）。本想「关一条补一条」，但 quicer 0.4.12 / msquic 2.5.11 下运行期 `setopt(Conn, settings, ...)` 返回 `ok` 而新的 MAX_STREAMS 帧**不会**让客户端的新流到达（实测：listen 时授 4 → 4 条顺序到达；握手后追加 → 永不到达）。4 因此是**重连配额**而不是并发目标，Single Stream Mode 仍然只有一条活动流；`{quic, peer_needs_streams, ...}` 只记一条 warning，不假装能补。

**MQTT 层看不见 QUIC 的事件。** quicer 把 payload 与事件塞进同一个 `{quic, X, Stream, Props}` 形状，只有 `X` 的类型能区分，所以 `decode_msg/2` 按 `is_binary/1` 分派（§3.9）。这里还有一处**实测踩到的真缺陷**：`arm_once/1` 最初写 `setopt(Stream, active, 1)`，而 quicer 把整数 active 当 `{active, N}`（multi 模式）——收满 N 条后向 owner 发 `{quic, passive, Stream, undefined}` 并把流切回 passive。FSM 不认识这个消息，于是**每读一个报文记一条 `unexpected event` 并 `protocol_errors` +1**。改用原子 `once`（`ACCEPTOR_RECV_MODE_ONCE`）后一读一 arm 且不再有 passive 事件；回归断言是 `mqtt_quic_listener_SUITE:a_pingreq_is_not_a_protocol_error`（报文在坏版本里也照常回去，只能对计数器断言）。

**QUIC 没有明文模式。** 握手就是 TLS 1.3，因此证书来源与其它 listener 不同：listener map 自带 `certfile`/`keyfile`，否则取 `broker_security.tls`；两者都没有就 `{stop, {quic_requires_tls, certfile | keyfile}}`，**只让这一个 app 起不来**。`broker_security.tls.enabled` 不能把 QUIC 变成明文，只能让它在找不到证书时拒绝启动。ALPN 只提供 `mqtt`（CN Draft 3 §2.1 的唯一值），客户端不带 `mqtt` 就握手失败——静默接受别的 ALPN 等于让客户端以为自己在说另一种协议。

**quicer 是 profile 门控依赖，不是默认依赖。** 默认 `rebar3 compile/ct/release` 完全不拉 `quicer`（`optional_applications => [quicer]`，listener 在缺 NIF 时以 `{error, quicer_not_available}` 干净拒绝）；`rebar3 as quic ...` 才把它编进来，`quic` profile 的 `relx` 因此复制了完整的 release app 列表——profile 的 `relx` 会**替换**顶层那份，漏掉就等于装出一个只有 QUIC 的 release。**`broker_proto_mqtt_quic` 本身在两个 release 的 app 列表里**（它不依赖 NIF 就能启动，只是 `listeners => []`），差别只是 `quic` release 多带 `quicer-0.4.12`；把该 app 从默认列表里去掉会让手工构建与 release 的启动行为不一致。所有 `quicer:` 调用集中在 `mqtt_quic_nif` 一个模块，`include` 也不越界：本 app 只通过裸消息元组与 quicer 交互，没有它的头文件依赖。

### 3.13 A2A 前端的连接模型与 Task 数据流（S9-A / S9-B / S9-C）

与 §3.9–§3.12 的 MQTT 传输抽象无关：A2A 是一条独立的 HTTP/1.1 前端，不经过 `mqtt_transport`，也不复用 `mqtt_conn_fsm`。

```
broker_proto_a2a_sup (one_for_one, 1/5s)
├── a2a_task_registry (one_for_all, 3/10s)         % S9-B：boot 契约的第一项
│     ├── a2a_task_shard_1..N                      % phash2(TaskId, N) 定分片；每分片一张 ETS 投影
│     └── a2a_task_recovery                        % 最后一项：全量重放，重建全部投影
├── a2a_push_worker                                % S9-C：仅 push_enabled => true 时挂载；出站投递/重试/DLQ
└── ranch_embedded_sup → listener × N              % 默认 N = 0（listeners => []）
      └── a2a_conn                                 % 每连接一个进程；循环处理多个请求（keep-alive）
            ├── agent_rpc_http:read_request/3      % packet=http_bin 读头 → raw 读体
            ├── a2a_rpc:handle/1                   % media type → 凭据 → 版本 → 信封 → 方法
            └── a2a_stream:run/3                    % S9-C：{stream, Plan} 时接管 socket 走 SSE
```

**一个连接一个进程，不是一请求一个进程。** `a2a_listener:start_link/3` 只 `spawn_link` 一个 `a2a_conn`，由它自己 `ranch:handshake/1` 接管 socket，然后在同一个进程里循环：读请求、分派、写响应、判断 `keep_alive`。这样 keep-alive 不额外耗进程，客户端掉线也只波及自己这条连接（`temporary` 由 `spawn_link` 的普通进程语义承担，不进监督树）。单连接上限 1000 个请求，防止长连接无限占用 ranch 槽位。

**registry 排在 listener 之前是 boot 契约。** `a2a_task_recovery` 是 registry 的最后一个子进程，在 `init/1` 里完成全量重放，所以 supervisor 报告 registry 就绪时投影已经存在；listener 之后才起，`SendMessage` 不可能与投影重建竞争。恢复失败会 `{stop, {a2a_recovery_failed, _}}`——宁可让 app 起不来，也不带着空投影接请求（ADR-025 §D5）。

**base 库不拥有 socket。** `agent_rpc_http` 收 `Transport` 模块 + `Socket`，内部用 `Transport:recv/3` / `setopts/2`。这让同一份解析代码既能服务 `ranch_tcp` 也能服务 `ranch_ssl`，还能在 CT 里被裸 `gen_tcp` 驱动；反过来，base 一旦持有 accept 循环，就会反向依赖 adapter 的 listener（违反 ADR-026 §D1 的依赖方向）。

**`{packet, http_bin}` / `{active, false}` 由库自己设，不写进 listener 的 `socket_opts`。** ranch 对 `socket_opts` 做严格校验，把这两个（以及 `reuseaddr`）写进去会在每次启动时记 "Transport option ... unknown or invalid" 警告；正确位置是 `read_request/3` 的开头，那里本来就是 HTTP 帧契约的归属地（读完头再切 `{packet, raw}` 读体，与 `broker_admin_listener` 同一手法）。

**boot 校验在 supervisor 之前。** `broker_proto_a2a_app:start/2` 先检查 `require_auth = false` 时是否所有 listener 都绑在回环地址；否则直接 `{error, {insecure_listener, Port, Ip}}` 拒绝启动整个 app。这是「失败关闭」：与其起一个无认证的公网任务 API，不如让节点起不来（`application:start` 返回嵌套的 `{error, {{insecure_listener, ...}, {broker_proto_a2a_app, start, ...}}}`）。

#### 3.13.1 一次 `SendMessage` 的完整数据流（S9-B）

```
a2a_conn ──► a2a_rpc:handle/1            media type → 凭据 → 版本 → 信封（S9-A 的门控顺序不变）
              └─► ACL(a2a publish, "$a2a/task/new")
                    └─► a2a_task_registry:create/3
                          └─► shard_for(NewTaskId)          phash2(TaskId, N) + 1
                                └─► a2a_task_shard:create/5  该分片 gen_server 串行执行：
                                      1. a2a_event:append(task, 快照)      → broker_router_hash:route/3 → broker_queue:append/3
                                      2. a2a_event:append(status_update, WORKING)
                                      3. a2a_event:append(message, agent 回包)
                                      4. a2a_event:append(status_update, COMPLETED)
                                      5. ETS 投影 ← 4 次纯折叠
                                      └─► 回 {ok, Projection} ──► a2a_rpc 转成 result.task
```

**Task 事件与 MQTT 消息走同一条写路径。** 分区由 `broker_router_hash:route/3` 决定（ADR-010 是唯一分区真相），写入 `broker_queue:append/3`（唯一写路径，ADR-007 的 `fsync`/`os_buffer` 与限额原样生效），因此 ADR-013 retention、ADR-014 游标、ADR-011 committed 水位对 A2A 事件同样成立——**没有第二套消息内核**。

**读路径不碰日志。** `GetTask` 只读 ETS 投影；`ListTasks` 扇出到所有分片再在 registry 合并分页（全局稳定页无法由各分片页拼出）。日志只在 boot replay 时被读。

**Task 事件是 `$a2a/task/<id>` 上的普通消息**，因此：

| 关注点 | A2A Task | 说明 |
|---|---|---|
| 分区 | `partition_id` 由 router 求得 | 同 Task 全部事件落同一分区，重放保序 |
| 可靠性 | `task_reliability`（默认 `fsync`） | 创建与终态同档；不因"终态更重要"分档（ADR-025 §D4） |
| retention | 与普通消息同一套 `broker_storage.retention` | 创建事件被 GC 后重放 → `history_lost` 补 `FAILED` |
| cursor/订阅 | **SSE 时才用** | 投影是 folded view，不是订阅者；S9-C 的 `a2a_stream` 才订阅分区邮箱钩子（**不建 SubRef、不 pin retention**，ADR-025 §D7） |
| Mnesia | **不使用** | `broker_meta` 只存 Agent Card 与 Push 配置（控制面），Task 不落 Mnesia（ADR-002 边界） |

#### 3.13.2 一次 `SendStreamingMessage` 的数据流（S9-C）

```
a2a_conn ──► a2a_rpc:handle/1
              └─► a2a_task_registry:create/3        （同 §3.13.1：事件落 $a2a/task/<id>）
              └─► {stream, Plan}  ──► a2a_conn 识别，交出 socket 给 a2a_stream:run/3
                    ├─ 写响应头 text/event-stream / chunked / no-cache
                    ├─ 订阅分区邮箱钩子（broker_queue:subscribe/2，钩子不是 SubRef，不 pin）
                    ├─ drain：From 起的历史事件逐个 → data: <json-rpc-response>
                    └─ await：等新事件；空闲发 ": ping"；终态/中断态转发后关流
```

**SSE 是瞬态读者，不是订阅者。** `a2a_stream` 只挂分区邮箱钩子（拿到"有新事件"的唤醒），自己按 cursor 读 `broker_queue:read_from/3`；它是队列层的普通读者，不进入 `broker_router` 的 `Filter → [SubRef]` 索引，也不产生 retention pin。因此「一个客户端挂住一条流」不会抬高任何分区水位——代价是重连只能**从 offset 0 重放**（`SubscribeToTask` 语义），不是从上次位置续传。

#### 3.13.3 Task 终态后的出站 Push 链路（S9-C）

```
a2a_task_shard:create/… 追加终态事件（已 fsync）
  └─ 追加成功 → a2a_task_shard:maybe_push/2 → a2a_push_worker:enqueue/4   （gen_server:cast，不阻塞终态）
        └─ a2a_push_worker（单进程串行所有投递）
              ├─ prepare：a2a_push:guard/3（allowlist ∩ → validate_url → resolve → is_private_address）
              │           a2a_push_auth:headers/3（timestamp/nonce/task-id/config-id + 可选签名）
              ├─ deliver：a2a_push:deliver/3（gen_tcp/ssl 手写 POST，连已校验 IP，禁 3xx，体 ≤ 64 KiB）
              └─ 失败 → 指数退避重试 → 耗尽/过期 → write_dlq → $a2a/push/dlq/<tenant>（普通消息）
```

**出站是"任务终态已定，投递只是副作用"。** 分片对 worker 只有一次 `cast`，绝不等待返回值；worker 崩溃只由 supervisor 重启（在途投递随队列丢失，代价是漏一次通知，不是任务状态错误）。`push_enabled => false` 时 worker 根本不在监督树里，四个 Push 方法回 `-32003`——**关闭态下连队列都没有**。DLQ 走的是 `broker_router:env/0` + `Backend:route/3` + `broker_queue:append/3`，与业务消息同一条写路径，因此它可被普通订阅消费、被同一套 retention 约束。

---

## 4. 启动顺序与 boot steps

### 4.1 拓扑序（OTP 28.3.1 实测）

```
sasl → broker_core → broker_metrics → broker_storage
     → crypto/asn1/public_key/ssl/ranch
     → broker_router → broker_security → broker_queue → broker_subscription
     → broker_event → broker_retain → broker_proto_mqtt
```

实测启动列表（`application:ensure_all_started(broker_proto_mqtt)`，storage 已先起）：
`[crypto, asn1, public_key, ssl, ranch, broker_router, broker_security, broker_queue, broker_subscription, broker_event, broker_retain, broker_proto_mqtt]`。

顺序不是装饰：`broker_queue` 在 `init/1` 里 recover 分区，`broker_subscription` 在 `init/1` 里重建订阅并回填 Router，二者都必须早于 listener。`broker_metrics` 必须早于 `broker_storage`（后者是它的调用者），而它本身只起一个可选的上报进程，不参与任何 boot step。`broker_security` 必须早于 `broker_queue`——后者的 `.app.src` 把它列进 `applications`（分区层要用 ACL 与租户判定）；`broker_retain` 同理必须早于 `broker_proto_mqtt`。`broker_router` 不依赖 `broker_security`，因此排在它前面。

**S9-A 的两个 app 不在这条链上。** `broker_proto_agent_rpc` 是纯库（无进程），`broker_proto_a2a` 的 `applications` 只有内核门面 + `broker_security` + `broker_meta` + `broker_router` + `broker_queue` + `broker_proto_agent_rpc`，默认 `listeners => []` 时启动即空转，既不读 MQTT 的数据目录也不参与 MQTT 的四个 boot step。**S9-B 起它多了一个自己的 boot step**：`a2a_task_registry` 的最后一项 `a2a_task_recovery` 在各分区 `read_from(P, 0, _)` 上重放 `$a2a/task/` 事件重建投影。这要求 `broker_queue` 已就绪，因此它必须排在 `broker_queue` 之后。单独起它需要：

```erlang
%% 实测（分区已带外创建）：application:ensure_all_started(broker_proto_a2a)
[sasl, broker_core, broker_metrics, broker_storage,
 broker_router, crypto, asn1, public_key, ssl, broker_security, broker_queue,
 ranch, mnesia, broker_meta, broker_proto_agent_rpc, broker_proto_a2a]
```

它按需配置 listener 才开端口，与 MQTT 的 boot 顺序没有相互约束（互不引用）。**分区必须先存在**：干净数据目录下 `broker_queue` 会按 §4.2 的 step 1 失败，A2A 的 recovery 也就无从谈起——这与 MQTT 的"首启必须带外 `create_partition/1`"是同一条规则，不是 A2A 特有的。

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
6. QoS1 发出后进 in-flight 集合；收到 PUBACK → `broker_subscription:ack/4`（带 consumer 做归属校验，ADR-019）→ 游标按连续前缀前进 → 周期性 `advance_committed`。
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
| `broker_storage.backend` | 代码兜底 `broker_storage_wal`（`.app.src` 未声明；release 同为该值） | 二选一：`broker_storage_wal` / `broker_storage_memory` |
| `broker_storage.data_dir` | `.app.src` 未声明，代码兜底 `data` | WAL 根目录；**release 必须配绝对路径**（`config/sys.config.src` 就是），且必须在 release 树外（J8）。tmpfs/ramfs 由 `wal_writer:check_filesystem/2` 在启动时**拒绝**（`{error, {volatile_filesystem, Dir}}`，`require_durable_fs` 默认 `true`，可显式关闭）；留空则落到相对路径 `data`，即 release 树内 |
| `broker_storage.wal` | `.app.src` 未声明；release 配 `batch_size=32`、`max_batch_size=256`、`batch_delay_ms=#{os_buffer=>0, fsync=>1}`、`index_anchor_interval=256`、`segment_max_records=100000`、`max_pending_appends=4096`（代码兜底整体为 `#{}`，即上述各项取各自代码默认，`max_pending_appends` 默认 `0` = 无界；超限回 `{error, {overloaded, PartitionId}}` 且不分配 offset） | WAL 批处理与段参数 |
| `broker_storage.retention` | `#{max_bytes => 0, max_age_ms => 0, check_interval_ms => 60000}` | 整段回收的三项；`0` = 不设限（S2-F） |
| `broker_router.backend` | 代码兜底 `broker_router_hash`（`.app.src` 未声明；release 同为该值） | 路由实现；未知值在 `broker_router_sup:init/1` 报 `{unknown_router_backend, _}`（不启动一个没有索引写入者的 app） |
| `broker_router.partitions` | **无默认，必填**（`.app.src` 的 `env` 为空，代码取 `undefined`；release 配 4） | 分区数。缺失时 `broker_router_hash:current_env/0` 回 `{error, {invalid_partitions, undefined}}`；必须与 `broker_queue.partitions` 一致 |
| `broker_queue.partitions` | 4（`.app.src` 与代码兜底一致） | 与 `broker_router.partitions` 必须一致；boot 的 recover 清单来源（非正整数报 `{invalid_partition_count, _}`） |
| `broker_subscription.data_dir` | `.app.src` 为 `data`；release 配与 storage 相同的绝对路径 | `.subs`/`.cursors`/`.inflight` 根目录 |
| `broker_subscription.cursor_max_regress` / `cursor_flush_interval_ms` / `inflight_flush_interval_ms` | 64 / 100 / 100 | 有界回退窗口；`1/0/0` 选择近零回退档 |
| `broker_retain.data_dir` | `.app.src` 为 `data`；release 配与 storage 相同的绝对路径 | retained 快照 `retain/retain.v1` 的根目录（S2-C） |
| `broker_meta.data_dir` | `.app.src` 未声明，代码兜底 `data`；release 配 `data/mnesia` 的绝对路径 | Mnesia 控制面根目录；Mnesia 在其下追加 `Mnesia.<node>`（S4） |
| `broker_meta.table_storage` | `ram_copies` | 12 张控制面表的副本类型。默认 `ram_copies` = **重启即丢**，生产必须配 `disc_copies`（`maybe_set_dir/1` 也只在 `disc_copies` 时设置 `mnesia.dir`）；见 `docs/operations.md` §5。**`agent_card` 表在 `disc_copies` 下重启后仍在**，是 S9-B 重启证据的一部分 |
| `broker_cluster.data_dir` | `data/ra`（release 配绝对路径） | Metadata Raft 根目录；ra 在其下追加 `<node>`（S4） |
| `broker_security.users` / `acl` | `.app.src` 未声明（代码兜底 `[]` / `allow`）；release 配 `guest/guest`（tenant `t0`）、`allow` | 明文用户表与 allow-all ACL 桩；用户表为空时凭据认证一律失败，匿名连接仍可建立（principal 取 client_id） |
| `broker_event.policy` | `pass_through` | 五档均已实现（S3，ADR-020）：`disabled` / `pass_through` / `validate_if_present` / `required` / `normalize`；未知值在 `apply_policy/2` 报 `{invalid_policy, _}` |
| `broker_event.policies` | `[]` | 按 `{Tenant, Destination, Policy}` 的首次匹配覆盖全局策略；`<<"*">>` 为租户通配，Destination 支持 MQTT `+`/`#` |
| `broker_metrics.enabled` / `report_interval_ms` | 代码兜底 `true` / `60000`（`.app.src` 未声明；release 同为该值） | 指标总开关；`0` 不启动上报进程，非正整数使 app 启动失败（响亮失败，不静默关掉可观测性） |
| `broker_proto_mqtt.listeners` | release 配 1883；`.app.src` 默认 `[]` | 每个 map 一个 listener；连接级 knob 随 map 下发（L20 seam） |
| `broker_proto_mqtt.mqtt_max_qos` | 2 | S2-C 起默认允许 QoS2（ADR-018 §18.4），仍受后端 capability 约束；内存档必须为 0 |
| `broker_proto_mqtt.max_inflight` / `pull_page_limit` / `pull_poll_interval_ms` / `inflight_snapshot_ms` / `max_keepalive_ms` | 64 / 32 / 1000 / 20 / 60000 | 投递循环与 keepalive 上限 |
| `broker_proto_mqtt_ws.listeners` | release 配 8083；`.app.src` 默认 `[]` | S8-B：MQTT over WebSocket 的 ranch listener（`port => 0` 时由 ranch 分配临时端口） |
| `broker_proto_mqtt_ws.subprotocols` / `max_frame_bytes` | `[mqtt, mqttv3.1, mqttv3.1.1]` / 16777216 | 子协议是握手门槛（不匹配 → HTTP 400）；后者限帧，不限 MQTT 报文 |
| `broker_proto_mqtt_sn.listeners` | release 配 1884；`.app.src` 默认 `[]` | S8-C：每条一个 UDP socket（`port => 0` 由内核分配，`mqtt_sn_listener:port/1` 读回）。**没有** `num_acceptors` / `max_connections` 等同 TCP 旋钮 |
| `broker_proto_mqtt_sn.predefined_topics` / `gateway_id` | `#{}` / `1` | 全局只读 topic id 表（保留 id `0`/`0xFFFE`/`0xFFFF` 启动即拒绝）与 GWINFO 的网关 id |
| `broker_proto_mqtt_sn.advertise_interval_ms` / `advertise_address` / `advertise_port` | `0` / `{255,255,255,255}` / `1884` | `0` = 只应答 SEARCHGW；**广播目的地是客户端监听端口，不是本 listener 端口** |
| `broker_proto_mqtt_sn.max_asleep_clients` / `max_topic_ids_per_client` | 1024 / 1024 | 休眠会话登记上限（超限按正常断开并计 `mqtt_sn_sleep_denied`）；每会话 normal topic id 上限（下行超限即断开会话） |
| `broker_proto_mqtt_sn.max_inflight` / `max_duration_s` | 20 / 3600 | 每会话未 ACK 的下行 QoS 1/2 窗口（满则留在分区、下次 PINGREQ 重试）；keepalive/sleep 请求时长上限 |
| `broker_proto_mqtt_quic.listeners` | release 配 `[]`（示例 `#{port => 14567}` 注释掉）；`.app.src` 默认 `[]` | S8-D：每条一个 QUIC listener。**默认关闭是刻意的**：QUIC 无证书必然拒绝启动，而拒绝启动是该 app 的启动失败，默认开会让不带证书的 release 起不来 |
| `broker_proto_mqtt_quic.alpn` | `[<<"mqtt">>]` | CN Draft 3 §2.1 的唯一 ALPN；客户端不带它就握手失败 |
| `broker_proto_mqtt_quic.handshake_timeout_ms` / `idle_timeout_ms` | 5000 / 30000 | TLS 1.3 握手超时与 QUIC idle timeout |
| `broker_proto_mqtt_quic.max_streams_per_conn` | 4 | 每条 QUIC 连接**建连时**授予的双向流信用 = 同一连接上的重连次数（不是并发流数；理由见 `mqtt_quic_listener` 内部 `open/2` 的注释） |
| `broker_proto_a2a.listeners` | release 配 `[]`；`.app.src` 默认 `[]` | S9-A：A2A JSON-RPC 前端的 ranch listener，每个 map 一个（`#{port, ref, num_acceptors, max_connections, ip}`）。**默认不监听**（ADR-026 §D4），`port => 0` 由内核分配临时端口（CT 用） |
| `broker_proto_a2a.require_auth` | `true` | adapter 自有开关，不动 `broker_security` 的 `{require_auth, []}`。为 `false` 时 boot 校验要求所有 listener 绑回环，否则 `{error, {insecure_listener, Port, Ip}}` 拒绝启动 |
| `broker_proto_a2a.max_body_bytes` | 1048576 | 与 `broker_security.limits.max_message_bytes` 取 `min` 作为实际上限；超限不读体直接 `413` |
| `broker_proto_a2a.max_connections` / `request_timeout_ms` | 1024 / 30000 | listener 未自带 `max_connections` 时的兜底值与单请求读超时 |
| `broker_proto_a2a.supported_versions` | `[<<"1.0">>]` | 声明支持的 `A2A-Version`。缺失头按 `0.3` 读入，但 `0.3` 不在此列，故缺失 → `-32009` |
| `broker_proto_a2a.sse_max_event_bytes` / `sse_heartbeat_ms` | 1048576 / 15000 | SSE 单事件上限与心跳间隔；S9-A 只有库原语，未接到任何方法（S9-C 启用） |
| `broker_proto_a2a.task_reliability` | `fsync` | S9-B：Task 事件的 `#broker_message{}.reliability`。创建事件与终态事件**同档**（ADR-025 §D4）。`replicated_commit` 在没有复制器时于 boot 校验被拒（`{error, {task_reliability_requires_replicator, replicated_commit}}`），不静默降级 |
| `broker_proto_a2a.task_shards` | `undefined` → `erlang:system_info(schedulers_online)` | S9-B：Task 分片数（ADR-025 §D2）。每分片一个 gen_server + 一张 ETS 投影；`phash2(TaskId, N)` 定分片。改这个值会改变 Task 的归属分片，**必须配合重启**（boot replay 本来就重建投影） |
| `broker_proto_a2a.task_auto_complete` | `true` | S9-B：`true` 时 `SendMessage` 同步跑完 `SUBMITTED → WORKING → agent message → COMPLETED`（内建 echo agent）；`false` 时停在 `SUBMITTED`，交给外部执行器。不是"agent 推理"开关 |
| `broker_proto_a2a.task_timeout_ms` | 30000 | 分片 gen_server 调用的超时；与 `request_timeout_ms` 独立（一个是分片内串行工作的时间预算，一个是客户端请求的读超时） |
| `broker_proto_a2a.task_checkpoint_dir` / `task_checkpoint_interval_ms` | `undefined` / 5000 | S9-B **已声明但当前不读**：恢复用全量重放（正确性基线）。持久化 checkpoint 是 S9-F 的演进位 |
| `broker_proto_a2a.agent_cards` | `[]` | S9-B：boot 种子卡列表（`#{tenant_id, agent_id, status, card, extended_card}`）。`a2a_card:seed/0` 在 app 启动时幂等写入 `broker_meta.agent_card`；内容未变不 bump version；格式不合法 → 启动失败 |
| `broker_proto_a2a.agent_card_max_age` | 60 | S9-B：`/.well-known/agent-card.json` 与 `/agents` 的 `Cache-Control: max-age`。ETag 由行 `version` + 内容哈希组成，改卡即失效缓存 |

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
| 单元/契约/集成 | `rebar3 ct` → **952/952、62 套件全部被执行**（`apps/*/test/` 61 个 + `test/integration/s1_e2e_SUITE` 1 个；含 `broker_storage_contract.hrl` 驱动的两档后端各 13 例契约），零 `FAILED` 明细、零 `Skipped`。S9-A 新增 7 个套件 85 例（基线 823/52），S9-B 再新增 3 个套件 25 例并改写 1 个（`a2a_task_SUITE` 11 / `a2a_event_SUITE` 6 / `a2a_task_projection_SUITE` 8 / `a2a_listener_SUITE` 16→33，`broker_meta_SUITE` 10→12）。`rebar3 as quic ct` 另有 827 passed + 2 skipped（QUIC 组展开；两个「quicer 缺失」用例按设计 skip） | `apps/*/test/`、`test/integration/`、`apps/broker_cluster/test/` |
| 静态检查 | `rebar3 dialyzer` 0 warning；`rebar3 xref` 仅预期 unused-export（S9-A/B 两个新 app 零非预期告警） | CI/本地 |
| A2A 真实客户端 | `curl` **25 条**（S9-B）：Task 生命周期、`GetTask` 404、`ListTasks` 过滤/分页、`CancelTask` 400、跨租户 404、well-known 匿名/认证 extended、ETag 304、catalog 三类过滤 + 分页 + 租户隔离、`GetExtendedAgentCard` 200 与 `-32007`、**重启后 boot replay 恢复同一 Task**、异步模式 SUBMITTED→CANCELED、401/health/`-32602`（证据端口 18093，默认不监听）。S9-A 的 11 条见 §4.14 | `docs/protocol-compatibility.md` §4.15 |
| 指标端到端 | `broker_metrics_SUITE` + `s1_e2e_SUITE` 的 `metrics_count_publish_deliver_and_connections`（wal/memory 各一次） | `apps/broker_metrics/test/`、`test/integration/` |
| 崩溃恢复 | 真实容器 + 宿主 ext4 + `docker kill -s KILL`：R1a~R7 × 默认/零回退两档；S2 电源/OS 缓冲注入 | `tools/docker/`、`tools/fault/` |
| 真实客户端 | paho-mqtt 2.x / mosquitto 2.1.2（MQTT）、websocket-client、nats.go、amqp091-go、kafka-clients、官方 Pulsar Java Client；六协议全绿；S8 另加 paho-mqtt `transport="websockets"`（8/8）、`mqtt-sn-tools` 0.0.7（10/10）、EMQX `emqtt` 1.16.1（8/8） | `test/protocol-compatibility/` |
| 集群 | 3 节点 `peer`：组形成、Leader 切换、失去多数派、迁移后 Fencing | `apps/broker_cluster/test/broker_cluster_dist_SUITE.erl` |
| 性能 | `docs/benchmarks/2026-11-09-s6-capacity.md`、`2026-11-09-s6-gameday.md` | `docs/benchmarks/` |
| 端到端 | `wal`/`memory` 双组同一组 7 用例 = **14/14**（T13 交付时 6 例 = 12/12，S2-A 追加 metrics 端到端后为 7 例） | `test/integration/s1_e2e_SUITE.erl` |

各阶段 Gate 的验收状态见 `docs/roadmap.md` 与 `progress.md`。

---

## 9. S7 及之后的演进位（未实现）

下列方向**尚未实现**，只作为后续演进的边界说明。`docs/security.md` §12 与 `progress.md` 有同一份清单。**S7 的两项交付已全部结案（2026-11-09 / 2026-11-10）**：六协议统一走 `broker_security` 身份表（`docs/security.md` §2.1）；各协议标准 CloudEvents Binding 的结论是「统一 `ce-*` 头通道是本 Broker 的项目定义，逐字实现不立项」，逐协议差异见 `docs/roadmap.md` §9.1 与 `docs/protocol-compatibility.md` §4.10。README §10.2 的 W3C trace context 传播（`docs/security.md` §5.1）也已实现，表格里的「可观测性导出层」特指 OTel 等外部导出器，不是传播本身；「官方 Binding 逐字实现」这一行是**已给出结论、不改实现**的演进位，不是遗漏的待办。**S8 的三条新传输没有新增未实现的协议面**：MQTT-SN 的 DTLS、QUIC 的 0-RTT 与多流、MQTT-SN 的转发器封装都在表尾单列，各自有 ADR 或设计条款说明为什么不做。

| 方向 | 阶段 | 会动到的结构 | 保持不动的契约 |
|---|---|---|---|
| 官方 CloudEvents Binding 逐字实现（差异已逐条列明） | S7 之后（结论已出，不立项） | 需按协议分流：MQTT 5 去前缀、Kafka 换 `ce_`、NATS 补 percent-encoding、WS 加 `cloudevents.json` 子协议、AMQP 迁 1.0 | 统一 `ce-*` 头通道继续作为默认；非无损点见 `docs/protocol-compatibility.md` §4.10 |
| 跨节点/复制通道 TLS | S7 之后 | `broker_cluster` 的 ra 复制链路 | Metadata Raft 语义与 Fencing 不变 |
| Schema Registry 内容校验 | S7 之后 | `broker_event` 校验链新增校验器 | 五档策略与冻结的指标键不变 |
| 审计日志签名与外部 KMS | S7 之后 | `broker_config_audit`、`broker_security` 的密钥来源 | 现有脱敏快照与 ACL 判定不变 |
| 每条规则可配置的「未知扩展属性拒绝」 | S7 之后 | `broker_event` 策略链 | 五档策略与冻结的指标键不变 |
| 去重/隔离钩子推广到其余协议 | S7 之后 | WS/NATS/AMQP/Kafka/Pulsar 发布路径与 `mqtt_sn_conn` 补 `broker_event:duplicate/1` / `quarantine/2` | 五档策略与 MQTT 现有行为不变；见 `docs/cloudevents-mapping.md` |
| OTel 等可观测性导出层 | S7 之后 | 在 `broker_metrics:snapshot/0` 之上加 OTel 等导出 | 计数点与冻结的键集合不变；`broker_event_trace` 的校验与门控已实现，不因导出层而改动 |
| `require_auth` 接进 MQTT CONNECT | S7 之后 | `mqtt_conn_fsm` 的私有 `authenticate/2` 增加 `auth_required(<<"mqtt">>)` 分支 | MQTT 现有的匿名 `client_id` 即 principal 语义（S1 冻结）不变；见 `docs/security.md` §2.2 现状边界 |
| `broker_meta` 用户表接进数据面认证 | S7 之后 | `broker_security` 的私有 `users/0` 增加 mnesia 读取源 | `authenticate/2`、`authenticate_conn/1` 的对外形态与错误语义不变；ADR-021 §D7 冻结后补充 |
| `topic_meta` / `queue_meta` / `binding_meta` 的运行时读取方 | S7 之后 | 路由/队列/Topic-Binding 管理路径 | 三张表已建表、有泛型 CRUD；接入读取方不改变现有路由与队列语义 |
| MQTT-SN over DTLS | S8 之后 | `broker_proto_mqtt_sn` 的 socket 换成 `ssl` 的 dtls 档 | MQTT-SN 1.2 报文与 topic id 模型不变；1884 目前是唯一**明文且无 TLS 路径**的端口（`docs/security.md` §12 第 8 条） |
| QUIC 0-RTT early data 与多流模式 | 不做（CN 可选项 / EMQX 私有扩展） | — | 单流模式是 CN Draft 3 的全部内容；ADR-024 已冻结该边界 |
| MQTT-SN 转发器封装与串口桥 | 不做（设计 §1.2） | — | 网关只终结，不做 bridge 到外部 MQTT Broker |
| A2A Task 状态机与 `$a2a/task/<id>` 日志 | **已交付（S9-B）** | `broker_proto_a2a` 新增 `a2a_task`（状态机）/`a2a_event`（事件 → `#broker_message{}`）/`a2a_task_projection`（纯折叠）/`a2a_task_shard` + `a2a_task_registry`（分片串行化，listener 之前的子进程）/`a2a_task_recovery`（boot 全量重放）；`GetTask`/`ListTasks`/`CancelTask` 落地 | ADR-025：Task 事件就是普通 `#broker_message{}`，复用 ADR-007 可靠性 / ADR-014 游标 / ADR-013 retention，不引入第二套消息内核（已实现） |
| A2A Agent Card、`/.well-known/agent-card.json`、ETag | **已交付（S9-B）** | `broker_proto_a2a` 新增 `a2a_card`（well-known/catalog/ETag/seed）；`broker_meta` 第 12 表 `agent_card` + 4 个访问器 | 不复活 `topic_meta`/`queue_meta`/`binding_meta`（ADR-026 §D3，已实现） |
| A2A 跨节点 Task 访问（非 owner 节点重定向/代理） | **S9-C 之后** | `a2a_task_registry` 的 `get/list/cancel` 增加跨节点路径，复用 ADR-021 的归属与 epoch | Task 事件与投影形状不变；单节点语义（本节点即 owner）不回退 |
| A2A 持久化 replay checkpoint（`data/a2a/replay.idx`） | **S9-F** | `a2a_task_recovery` 从「分区 offset 0 全量」改为「checkpoint + 尾部增量」；`task_checkpoint_dir`/`task_checkpoint_interval_ms` 两个 env 已经声明 | 重放的**结果**不变（投影仍是日志的折叠）；`history_lost` 判定与 repair 幂等语义不变 |
| A2A 失败路径计数器（`a2a_state_violation`/`a2a_task_history_lost`/`a2a_checkpoint_corrupt`） | **S9-F** | `broker_metrics` 冻结键集的一次统一变更 | 现有键集合与 `snapshot/0` 形状在 S9-F 之前不变；当前用 `logger` 记事件 |
| A2A Agent Card 管理面 CRUD | **S9-F** | 在 `broker_admin` 之上加卡的增删改查；`a2a_card:seed/0` 仍是 boot 期写入者 | 卡表形状、ETag 生成规则、租户隔离语义不变 |
| A2A 流式（SSE）、`SubscribeToTask`、Push Notification / 出站 webhook | **S9-C** | `agent_rpc_sse` 接到方法；`$a2a/push/dlq/<tenant>` | SSE 是瞬态读者：不建 cursor、不 pin retention，落后即发 Task 快照 resync；出站默认关闭、只 `https`、host allowlist + pin IP、禁重定向（ADR-026 §D6/§D7） |
| MCP 2026-07-28 前端 | **S9-D** | 新 app `broker_proto_mcp`，监听 8082 | 与 A2A 共享 `broker_proto_agent_rpc`，两个 adapter 互不引用 |
| ACP v1 前端（stdio，Broker 扮 Client） | **S9-E** | 新 app `broker_proto_acp`，无端口 | 只 stdio；`fs/*`、`terminal/*`、elicitation 不广告；`session/request_permission` 默认拒绝 + allowlist；v2 不实现 |
| A2A gRPC / REST 绑定、HTTP/2 | 非目标（ADR-026 §D8） | — | 只 HTTP/1.1 + JSON-RPC binding |
| JWT / OIDC / JWKS 验签、OAuth AS | 非目标（ADR-026 §D8） | — | 只接受 `broker_user` 里的不透明 bearer token |
| ACP 远程传输、Agent Card JWS/PKI | 非目标 | — | 见收敛报告 §13 |
| Part 内嵌 UI（`data` 里的渲染提示） | 非目标 | — | `Part` 只做内容传输，不解释 `data` 语义 |

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
