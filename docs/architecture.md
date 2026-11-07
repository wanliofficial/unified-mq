# UnifiedMQ 架构（S1 垂直切片）

- **文档版本**：v0.1（T16，2026-11-06）
- **状态**：描述 **S1 已实现并实测**的架构事实；S2 及以后的演进方向只在本文件末尾单列，不与现状混写
- **读者**：接手实现的工程师、评审人、需要判断"某个行为归哪一层"的测试作者
- **对应**：`docs/roadmap.md` §3（S1）、`docs/design/s1-mqtt-vertical-slice.md`（设计基线，下称"设计文档"）、ADR-001/007/009/010/011
- **证据口径**：本文件中"已实测"均指 `rebar3 ct`（264/264）、`tools/docker/`（T14）与 `test/protocol-compatibility/mqtt/`（T15）的产物；引用处给出路径

> 本文只回答三个问题：**有哪些应用/进程、谁在什么进程里干活、一次消息从进到出走哪条路**。磁盘格式、刷盘与恢复的细节见 [`storage-and-recovery.md`](storage-and-recovery.md)，协议能力与对端兼容性见 [`protocol-compatibility.md`](protocol-compatibility.md)。

---

## 1. 范围与设计约束

S1 是一个**单节点、无复制**的 MQTT 3.1.1 broker 垂直切片，只实现两种本地存储后端（内存 / 本地 WAL），可靠性等级只覆盖 ADR-007 的 L1 `os_buffer` 与 L2 `fsync`。架构上的硬约束来自已冻结的 ADR：

| 约束 | 来源 | 在代码里的落点 |
|---|---|---|
| 协议适配层不得直接操作磁盘格式 | ADR-001 | `mqtt_conn_fsm` 只经 `broker_router` / `broker_queue` / `broker_subscription` 三个门面接触内核，不 include 任何存储模块（路由只 include `broker_router.hrl` 的 `#route_ctx{}`） |
| 统一消息模型与 CloudEvents 分离，`event` 只是字段位 | ADR-004/009 | `#broker_message{}` 带 `event` 槽；S1 的 `broker_event` 是纯函数恒等桩，`event` 恒 `undefined`（见 §2.2） |
| 可靠性承诺显式化：ACK 时机与 sync mode 一一对应 | ADR-007 | `mqtt_conn_fsm` 在 `broker_queue:append/3` 返回后才发 PUBACK |
| Router 不串行化，路由在发布者进程内完成 | ADR-010 | 句柄/索引在 `protected` ETS，`route/3` 无 `gen_server:call` |
| 存储只拥有全局 committed 水位，per-sub 位置归订阅层 | ADR-011 | `broker_storage` 的 API 没有订阅概念；cursor 全在 `broker_subscription` |
| 分区创建是带外动作，boot 只 recover | ADR-011 §11.2 | `create_partition/1` 是唯一 `create` 调用者；boot 缺分区即整体启动失败 |
| S1 在 Gate 前不实现第二个协议 | roadmap §3.4 | `apps/` 下只有 `broker_proto_mqtt` |

**S1 明确不在架构内**（免得把"还没做"误读成"没画出来"）：Raft/复制、Mnesia 控制面、CloudEvents codec、TLS/ACL、QoS2/Retain/Will/MQTT 5、多分区调度与慢消费者隔离、retention/TTL/DLQ、第二协议。

---

## 2. 应用划分

### 2.1 8 个应用

```
apps/
├── broker_core/          仅 include/broker_message.hrl 与公共类型；无模块、无进程、无 supervisor
├── broker_storage/       broker_storage behavior + 内存/WAL 两实现 + WAL 内部七个模块
├── broker_router/        broker_router behavior + broker_router_hash 实现（内存索引）
├── broker_queue/         分区层：存储句柄生命周期、append/read 编排、{new_messages} 扇出
├── broker_subscription/  订阅、per-sub cursor、投递唤醒、三层会话持久化
├── broker_event/         CloudEvents 策略接口（S1：`policy/0` + 恒等 `apply_policy/1,2` + 保留 `#cloud_event{}` 接口位）
├── broker_security/      username/password 认证与 allow-all ACL 桩（纯函数）
└── broker_proto_mqtt/    MQTT 3.1.1 编解码 + 连接 FSM + ranch listener
```

| 应用 | 依赖（`.app.src`） | 自己的进程 | 对外入口 | 拥有什么状态 |
|---|---|---|---|---|
| `broker_core` | kernel, stdlib | 无 | `broker_message.hrl` 的 `#broker_message{}`、`partition_id()`、`sync_mode()` | 无（纯类型） |
| `broker_storage` | kernel, stdlib, sasl, broker_core | `broker_storage_wal_sup` + 每分区 `wal_writer`（WAL 档）／`broker_storage_memory_store`（内存档） | `broker_storage` behavior：`create/recover/close/append/read_from/advance_committed/truncate/capability` | WAL 段与 `meta.log`；内存表 |
| `broker_router` | kernel, stdlib, sasl, broker_core | `broker_router_hash_store` | `route/3`（`broker_router` behavior 回调，由 `broker_router_hash` 实现）、`broker_router:env/0`、`register/3`、`unregister/1` | `Filter → [SubRef]` 内存索引（**不持久化**） |
| `broker_queue` | kernel, stdlib, sasl, broker_core, broker_storage, broker_router | `broker_queue_store` + 每分区 `broker_queue_partition` | `append/3`、`read_from/3`、`watermarks/1`、`capability/1`、`subscribe/2`、`create_partition/1` | 分区句柄注册表、每分区订阅者集合 |
| `broker_subscription` | kernel, stdlib, sasl, broker_core, broker_storage, broker_router, broker_queue | `broker_subscription_worker` | `subscribe/3`、`unsubscribe/2`、`ack/3`、`inflight/2`、`attach/3`、`detach/2`、`dispatch/2`、`cursor/2`、`subscriptions/1`、`session/1` | 四张 ETS（实体/游标/连接绑定/in-flight）+ `data/sessions/*` 三件套 |
| `broker_event` | kernel, stdlib, sasl, broker_core | 空 supervisor | `policy/0`、`apply_policy/1,2`（纯函数读 env；S1 恒等透传）、`#cloud_event{}` 接口位 | 无（配置即状态；S1 数据路径无调用） |
| `broker_security` | kernel, stdlib, sasl, broker_core | 空 supervisor | `authenticate/2`、`authorize/2`、`tenant_id/1`（纯函数读 env） | 无（配置即状态） |
| `broker_proto_mqtt` | kernel, stdlib, sasl, ranch, 其余 7 个 broker app | `mqtt_conn_sup` + 每连接 `mqtt_conn_fsm` + 嵌入的 ranch listener | `mqtt_frame`（编解码）、`mqtt_filter`（Annex A 校验）、`mqtt_conn_fsm`、`mqtt_listener` | 每连接内存视图（packet_id 分配器、in-flight 集合、keepalive 定时器） |

### 2.2 `broker_event` 的真实状态（避免误读）

T11 之后 `broker_event` 有一个**纯函数**模块：`policy/0` 读 `broker_event.policy`（3 参 `get_env`），`apply_policy/1,2` 对 `pass_through` / `disabled` 返回**逐项相同**的消息（payload 字节、`headers`、`event`、`proto_meta` 全不动），对 `validate_if_present` / `required` / `normalize` 返回 `{error, {policy_not_implemented, P, s3}}` —— **响亮失败，不静默降级**。`include/cloud_event.hrl` 保留 README §3.1 的 `#cloud_event{}` 作为 S3 codec 的接口位（它与 ADR-009 的 `event :: undefined | map()` 如何对接是 S3 的判决点，头文件里已注明）。

**S1 数据路径仍然不经过它**：`mqtt_conn_fsm:build_message/5` 直接把 `event` 填 `undefined`，所以 `apply_policy/1` 目前在运行时无调用者（`xref` 的 3 条 unused-export 即此）。这不是遗漏，而是 OQ-5 的选择：MQTT 3.1.1 没有 PUBLISH properties，`content-type` 不在 S1 线路上；要"看见" CloudEvents 只能嗅探 payload 字节，而 ADR-004 禁止强制 CloudEvents、S0.5 也把逐事件解析测成 CPU 成本中心（0 扩展 ~53 µs，1024 扩展 8 ms）。

### 2.3 依赖方向

```
broker_core  ←── 所有应用（只依赖类型）
broker_storage ←── broker_queue ←── broker_subscription ←── broker_proto_mqtt
broker_router  ←── broker_queue ──┘            ↑
broker_security ───────────────────────────────┘
broker_event ──────────────────────────────────┘（纯函数接口，数据路径无调用）
```

依赖只允许**向下**：协议层可以调内核，内核任何一层都不得反向引用 `broker_proto_mqtt`；`broker_core` 不依赖任何兄弟应用。`xref` 是这条约束的机械检查（当前只剩预期的 unused-export 告警）。

---

## 3. 进程模型与监督树

### 3.1 顶层结构

S1 **没有伞形 `broker_app_sup`**：除 `broker_core` 外的 7 个 app 各有自己的顶层 supervisor，跨 app 的启动顺序交给 OTP 的 `applications` 拓扑序（设计文档 §6 的 T1 批注）。

```
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

7 个顶层 supervisor（`broker_core` 没有）+ 1 个嵌套的 `broker_storage_wal_sup` + ranch 自己的子树。

### 3.2 重启策略与理由

| 进程 | 策略 | 理由 | 崩溃后的可见行为 |
|---|---|---|---|
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

1. **读路径不进 gen_server，写路径只进那一个写者。** `broker_router_hash:route/3`（先 `broker_router:env/0` 取 Env）、`broker_queue:read_from/3`、`broker_subscription:cursor/2`/`subscriptions/1`/`dispatch/2` 全部在**调用者进程**内查 `protected` ETS 或直接读段文件；`broker_queue:append/3` 同样在调用者进程内查注册表、完成 `wal_codec:encode/1`，只把编码后的二进制交给分区唯一的 `wal_writer`（一次 `gen_server:call`）。写者进程只拥有"写"与"生命周期"。
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
sasl → broker_core → broker_storage → broker_router → broker_queue
     → broker_subscription → broker_event → broker_security → broker_proto_mqtt
```

顺序不是装饰：`broker_queue` 在 `init/1` 里 recover 分区，`broker_subscription` 在 `init/1` 里重建订阅并回填 Router，二者都必须早于 listener。

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
| 正常 DISCONNECT | 停止进程；不发布 Will（S1 无 Will 投递）；`.inflight` 保留给重连 |
| 异常断连 | 同上；`.inflight` 保留；订阅与游标不动 |
| clean_session=true 断开 | `detach` 拆除整个会话：实体、Router 索引、三个文件 |
| 存储 writer 崩溃 | 调用者拿到 `{error,{stale_handle,...}}`，分区 worker `reattach` 重新 `recover/2`；**不自动重试 append**（可能已消耗 offset，重试会制造重复） |

### 5.4 S1 已知的行为边界（不是 bug）

- 慢消费者没有隔离/背压：一个卡住的连接只会卡住自己的 pull loop，但其 cursor 会钉住该分区 committed（多留数据，不丢）。
- QoS0 订阅离线时没有 conn 替它 skip，cursor 停在原点 → 该分区 committed 偏低（同上，方向是安全的一侧）。
- 没有重试定时器/指数退避：QoS1 重发只在重连或 DUP 路径发生。
- `kill -9` 已验证；掉电（`os_buffer` 真实窗口）未模拟，留 S2。

---

## 6. 配置面

S1 没有 `broker_config` 应用，所有配置来自 `config/sys.config.src`（release 会复制成 `releases/Vsn/sys.config`），读取一律用 `application:get_env/3`（OTP 28 的 `get_env/2` 返回 `{ok,V}`，设计文档 §13 J3）。关键项：

| 键 | 默认 | 作用 |
|---|---|---|
| `broker_storage.backend` | `broker_storage_wal` | 二选一：`broker_storage_wal` / `broker_storage_memory` |
| `broker_storage.data_dir` | 绝对路径 | WAL 根目录；必须在 release 树外（J8），且不能是 tmpfs/ramfs（J11） |
| `broker_storage.wal` | 见下 | `batch_size=32`、`max_batch_size=256`、`batch_delay_ms=#{os_buffer=>0, fsync=>1}`、`index_anchor_interval=256`、`segment_max_records=100000` |
| `broker_router.backend` / `partitions` | `broker_router_hash` / 4 | 路由实现与分区数（S1 固定） |
| `broker_queue.partitions` | 4 | 与 `broker_router.partitions` 必须一致；boot 的 recover 清单来源 |
| `broker_subscription.data_dir` | 与 storage 相同 | `.subs`/`.cursors`/`.inflight` 根目录 |
| `broker_subscription.cursor_max_regress` / `cursor_flush_interval_ms` / `inflight_flush_interval_ms` | 64 / 100 / 100 | 有界回退窗口；`1/0/0` 选择近零回退档 |
| `broker_security.users` / `acl` | `guest/guest`、`allow` | 明文用户表与 allow-all ACL 桩 |
| `broker_event.policy` | `pass_through` | S1 只实现 `pass_through` / `disabled`；其余三档在调用点报 `{policy_not_implemented,_,s3}` |
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
| 单元/契约/集成 | `rebar3 ct` → **264/264**，零 `FAILED` 明细、零 `Skipped`；20 个 `*_SUITE.erl` | `apps/*/test/`、`test/integration/` |
| 静态检查 | `rebar3 dialyzer` 0 warning；`rebar3 xref` 仅预期 unused-export | CI/本地 |
| 崩溃恢复 | 真实容器 + 宿主 ext4 + `docker kill -s KILL`，R1a~R7 × 默认/零回退两档 = **16/16 PASS** | `tools/docker/` |
| 真实客户端 | paho-mqtt 2.x + mosquitto 2.1.2，**8/8 用例、46/46 帧字节一致** | `test/protocol-compatibility/mqtt/` |
| 端到端 | `wal`/`memory` 双组同一组 6 用例 = **12/12** | `test/integration/s1_e2e_SUITE.erl` |

当前 Gate S1 的四项验收状态与剩余工作见 `docs/roadmap.md` §3.4 与 `progress.md`。

---

## 9. S2 及以后的演进位（不在 S1 内）

| 方向 | 会动到的结构 | 保持不动的契约 |
|---|---|---|
| 第二协议（AMQP/Kafka/…） | 新增 `broker_proto_*`；连接 FSM 复用 `broker_queue`/`broker_subscription` 门面 | 三个冻结抽象（`#broker_message{}`、Router、Storage behavior） |
| QoS2 / Retain / Will / MQTT 5 | `mqtt_frame` + `mqtt_conn_fsm` 本地；投递循环增加状态 | 存储/订阅层 API 不变 |
| retention / compaction / TTL | `wal_recovery`、`wal_segment`、`wal_meta` 增加删除与截断策略 | 段命名/记录头格式（可加 Vsn 分支） |
| Mnesia 控制面 | 新增表；`.subs`/`.cursors`/`.inflight` 整体迁入 | `broker_subscription` 对外 API |
| 复制（Raft） | `broker_storage` 增加 `replicated_commit` 能力与新后端 | 同一 behavior；ACK 时机仍由 capability 决定 |
| 独立 dispatcher / 背压 | 从 `mqtt_conn_fsm` 抽出投递循环 | `cursor/2` 仍是唯一读位置；通知仍是提示 |
| CloudEvents | `broker_event` 从恒等桩变成 codec + 五档策略（接口位已由 T11 预留） | `event` 字段位与 ADR-004 的可选性 |

---

## 10. 术语

| 术语 | 含义 |
|---|---|
| SubRef | `{ClientId, Filter}`，订阅层铸造的确定性标识；Router 只存不解释 |
| cursor | "下一条要读的 offset"，per `(SubRef, PartitionId)`，归订阅层 |
| committed | "最后已确认的 offset"，存储层的全局水位；S1 由 `min(活 cursor)-1` 驱动 |
| in-flight | 已发出未 ack 的 QoS1 消息集合，每连接一份，持久化为不透明 blob |
| boot step 1~4 | 见 §4.2；失败一律整体启动失败 |
| stale handle | writer 崩溃后失效的存储句柄（含死 pid / 死 ETS tid），读取会 `badarg` |
| L1 / L2 | ADR-007 的 `os_buffer` / `fsync` 可靠性等级 |
