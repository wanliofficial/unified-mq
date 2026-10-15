# S1 方案设计 — MQTT 垂直切片原型

- **文档版本**：v0.1（S1 启动前的方案稿）
- **对应 Roadmap**：[`docs/roadmap.md`](../roadmap.md) §3
- **前置**：S0 已完成，ADR-001~008 已定稿（[`docs/adr/`](../adr/)）
- **目标 Gate**：roadmap §3.4「Gate S1」
- **状态**：待评审 → 通过后进入实现

---

## 1. 目标与非目标

### 1.1 目标

以 MQTT 3.1.1 为首个协议，跑通 README 附录 A 的 7 步端到端闭环：
Producer publish → (S1 阶段 CloudEvents 走 pass_through) → Router 解析 → 分区 → WAL 追加并 fsync → PUBACK → Consumer subscribe/receive → 消费者 ACK → 崩溃后正确恢复。

三个核心抽象（`broker_message` / `broker_router` / `broker_storage`）在 S1 评审后**冻结**，S2+ 只能扩展、不得破坏式修改。

### 1.2 非目标（S1 明确不做）

| 项 | 推迟到 |
|---|---|
| Raft / 分区复制 / 集群成员 / Epoch Fencing | S4 |
| Metadata Raft Group、Mnesia 控制面表（tenant/acl/partition_meta 等） | S4 |
| CloudEvents codec / binding / 校验策略（S1 只保留 `broker_message.event` 字段位与 pass_through 桩） | S3 |
| MQTT QoS 2、Retain、Will、MQTT 5.0、User Properties | S2 |
| TLS、密钥轮换、真实 ACL | S6 |
| 分区 Worker 的多分区调度、热点分区处理、慢消费者隔离 | S2 |
| AMQP / Kafka / NATS / WebSocket / Pulsar 适配器 | S5 |
| DLQ / TTL / retention / 重试上限 / 指数退避 | S2 |
| Prometheus / OpenTelemetry / 管理 API | S2 起最小集，S6 完整 |

S1 只做**单节点、无复制、内存 + 本地 WAL 两种存储实现**，可靠性等级仅覆盖 ADR-007 的 L1 `os_buffer` 与 L2 `fsync`；L3 `replicated_commit` 保留接口位不实现。

---

## 2. 应用与目录布局

按 roadmap §3.1，只建 S1 会实际用到的 app：

```
apps/
├── broker_core/          # 统一消息模型、通用类型、错误码、可靠性等级枚举
├── broker_router/        # destination → {partition, subscription} 解析
├── broker_queue/         # 分区 Worker、追加/读取编排、in-flight 管理
├── broker_subscription/  # 订阅、游标、投递语义（S1 仅 Exclusive-like）
├── broker_storage/       # 存储 behavior + memory 与 WAL 两种实现
├── broker_event/         # S1 仅桩：pass_through；接口位保留 cloud_event 记录
├── broker_security/      # S1 仅桩：username/password 认证回调
└── broker_proto_mqtt/    # MQTT 3.1.1 帧编解码、连接 FSM、会话管理

config/
├── sys.config.src
└── vm.args.src

test/
├── integration/          # common_test：端到端 pub/sub/ack
└── protocol-compatibility/mqtt/  # 真实客户端（paho、mosquitto_pub/sub）
```

`spikes/` 只读，S1 阶段不得被 `apps/*` 引用（roadmap §2 硬约束）。S0.3 的 WAL 代码作为参考，需在 `broker_storage` 中重写为生产版本（不是简单 copy）——原因见 §7.2。

---

## 3. 三个必须冻结的核心抽象

以下三者构成 S2+ 所有协议 / 存储 / 复制实现的挂载点。S1 评审通过后写入 ADR-009/010/011 定稿。

### 3.1 `broker_message` 记录（`broker_core/include/broker_message.hrl`）

在 README §3.1 草案基础上补充可靠性等级、事件槽位、协议元数据边界：

```erlang
-record(broker_message, {
    message_id        :: binary(),           % Broker 内部 ID，与 CloudEvents.id 分离（ADR-005）
    tenant_id         :: binary(),           % S1 默认 <<"default">>
    destination       :: binary(),           % MQTT topic / AMQP routing key / ...
    partition_id      :: partition_ref(),    % {topic_hash, partition_index}
    payload           :: binary(),
    payload_encoding  :: raw | json | binary | text,
    headers           :: map(),              % 协议无关的键值元数据
    proto_meta        :: map(),              % 协议私有：MQTT 存 #{qos, dup, retain, packet_id}
    persistence       :: transient | durable,
    reliability       :: os_buffer | fsync | replicated_commit,  % ADR-007
    created_at        :: integer(),          % ms epoch，Broker 接收时间
    expires_at        :: integer() | undefined,
    event             :: undefined | map()   % S3 填充：cloud_event 视图或原始字节
}).

-type partition_ref() :: {TopicHash :: non_neg_integer(), PartitionIndex :: non_neg_integer()}.
-type offset()        :: non_neg_integer().  % 分区内单调递增，S1 由 storage 分配
```

**冻结点**：
1. `message_id` 与 `event.id` 分离（ADR-005），后续任何协议私有 ID（MQTT packet_id、Pulsar MessageId、Kafka offset）**不得**写入 `message_id`。
2. `reliability` 是**每条消息**的属性，不是全局配置——生产者可在支持时按消息覆盖。
3. `proto_meta` 为协议私有留位，S2 补齐 MQTT QoS2 / Retain 时不破坏记录布局。

### 3.2 `broker_router` behavior（`broker_router/src/broker_router.erl`）

```erlang
-callback init(Config :: map()) -> {ok, State :: term()} | {error, term()}.
-callback route(Destination :: binary(), Ctx :: route_ctx(), State :: term()) ->
    {ok, route_result(), State :: term()} | {error, term()}.
-callback subscribe(Filter :: binary(), SubOpts :: map(), State :: term()) ->
    {ok, SubRef :: term(), State :: term()} | {error, term()}.
-callback unsubscribe(SubRef :: term(), State :: term()) ->
    {ok, State :: term()} | {error, term()}.

-record(route_ctx, { tenant_id, publisher, reliability }).
-type route_result() :: [{Partition :: partition_ref(), SubRefs :: [term()]}].
```

**S1 默认实现**：`broker_router_hash`——topic 字符串直接哈希到 N 个分区（N 由 config 决定，默认 4）；订阅侧实现 MQTT 通配符匹配（`+` 单层、`#` 多层），返回匹配的 SubRef 列表。

**冻结点**：Router **不决定** ACK 时机、**不感知**存储实现、**不持有**订阅游标（游标归 `broker_subscription`）。Router 输出只是"这条消息该写到哪个分区、该分发给哪些订阅"。

### 3.3 `broker_storage` behavior（`broker_storage/src/broker_storage.erl`）

```erlang
-type sync_mode() :: os_buffer | fsync | replicated_commit.   % ADR-007
-type handle()    :: term().                                   % 后端私有

-callback open(PartitionRef, Config :: map()) -> {ok, handle()} | {error, term()}.
-callback close(handle()) -> ok.

-callback append(handle(), [#broker_message{}], sync_mode()) ->
    {ok, [offset()]} | {error, term()}.

-callback read_from(handle(), FromOffset :: offset(), Limit :: pos_integer()) ->
    {ok, [{offset(), #broker_message{}}], NextOffset :: offset()} | {error, term()}.

-callback ack(handle(), offset()) -> ok | {error, term()}.
-callback truncate(handle(), BeforeOffset :: offset()) -> ok | {error, term()}.

-callback recover(PartitionRef, Config :: map()) ->
    {ok, handle(), CommittedOffset :: offset(), AckedOffsets :: [offset()]} | {error, term()}.

-callback sync_capability() -> [sync_mode()].   % memory: [os_buffer]; WAL: [os_buffer, fsync]
```

**S1 交付两份实现**：
- `broker_storage_memory`：ETS + `queue:queue()`，只支持 `os_buffer`，进程崩溃即丢，用作 dev/test 与 Router 层单测。
- `broker_storage_wal`：分段追加日志（S0.3 结论），支持 `os_buffer` / `fsync`，`fsync` 档采用 group commit（默认 batch=32，可配 1~256），恢复走撕裂尾部截断。

**冻结点**：
1. `append/3` 的第三参数是**每条消息**的 sync 决策，storage 层负责按 sync_mode 分路径。
2. `replicated_commit` 在 S1 是"接口位存在但两个后端都不实现"，调用时返回 `{error, unsupported_sync_mode}`；S4 由 `broker_storage_replicated`（新后端，走 ra）实现，不修改 behavior。
3. `ack/2` 语义 = "此 offset 已被下游确认，可在下一次 truncate 时安全丢弃"。S1 不做 retention 策略，ack 只写游标不触发清理。

---

## 4. MQTT 首批范围与实现要点

### 4.1 帧编解码

MQTT 3.1.1 帧结构简单，S1 **不引入外部依赖**，在 `broker_proto_mqtt/src/mqtt_frame.erl` 自研编解码（约 400 行），只覆盖：

| Packet | 方向 | S1 |
|---|---|---|
| CONNECT / CONNACK | C→S / S→C | ✅ |
| PUBLISH QoS0 / QoS1 | 双向 | ✅ |
| PUBACK | 双向 | ✅ |
| SUBSCRIBE / SUBACK | C→S / S→C | ✅ |
| UNSUBSCRIBE / UNSUBACK | C→S / S→C | ✅ |
| PINGREQ / PINGRESP | 双向 | ✅ |
| DISCONNECT | C→S | ✅ |
| PUBLISH QoS2 / PUBREC / PUBREL / PUBCOMP | — | ❌ S2 |

变长 Remaining Length、UTF-8 字符串、topic filter 通配符校验严格按 OASIS MQTT 3.1.1 规范；单元测试用 mosquitto 抓包字节做黄金样本。

### 4.2 连接 FSM 与会话

```
mqtt_listener (ranch/tcp acceptor)
  └── mqtt_conn_fsm (per-connection gen_statem)
        ├── 状态: awaiting_connect → connected → disconnected
        ├── keepalive 定时器 (1.5× KeepAlive 无包即断)
        ├── packet_id 分配器（QoS1 in-flight）
        └── 关联 mqtt_session (per-ClientId, clean_session=false 时跨连接存活)
```

- **`clean_session=true`**：连接建立时新建临时 session，断开即销毁。
- **`clean_session=false`**：session 由 `broker_session_sup` 持有，ClientId → PID 注册在 ETS，跨连接保留订阅与 in-flight QoS1 队列。S1 **不做 session 持久化到磁盘**（README 附录 A 步骤 6 的"订阅位置不丢"由 `broker_subscription` 游标承担，见 §5.3）；session 层的 in-flight packet_id 与订阅列表在 broker 重启后由客户端重连时重新 SUBSCRIBE 恢复——**这是 S1 明确接受的语义损失，需写入 protocol-compatibility.md**。

### 4.3 认证

`broker_security` 桩：`authenticate(Username, Password) -> {ok, Principal} | {error, Reason}`。S1 默认实现从 `sys.config` 读明文用户表；ACL 桩一律返回 `allow`。真实 ACL / TLS 在 S6。

### 4.4 QoS 与可靠性映射

| MQTT QoS | S1 broker 行为 | 对应 ADR-007 等级 |
|---|---|---|
| QoS 0 | 不入 WAL，直接进 Router → 匹配订阅 → 分发（可能丢） | `os_buffer` |
| QoS 1 | 入 WAL，`sync_mode = fsync`，PUBACK 在 fsync 完成后返回 | `fsync` |
| QoS 2 | 拒绝，返回 SUBACK/PUBACK 错误码 | — |

**关键契约**：QoS1 的 PUBACK 必须在 `broker_storage:append(..., fsync)` 返回 `{ok, [Offset]}` 之后发出——不得提前。这是 S1 崩溃恢复测试的核心断言（§8）。

---

## 5. 数据面流程

### 5.1 Publish 路径（QoS1）

```
Client PUBLISH(qos=1, packet_id=P, topic=T, payload=B)
  ↓
mqtt_conn_fsm:handle_publish
  ↓ 构造 #broker_message{reliability=fsync, proto_meta=#{qos=>1,packet_id=>P,...}}
broker_router:route(T, Ctx)  →  [{Partition, [SubRef1, SubRef2]}]
  ↓
broker_queue:append(Partition, Msg, fsync)
  ↓ 分区 Worker GenServer 串行化 append
broker_storage_wal:append(Handle, [Msg], fsync)
  ↓ group commit 或直写 + fsync
{ok, [Offset]}
  ↓
broker_subscription:notify(SubRefs, {Partition, Offset, Msg})
  ↓ 每个订阅的 dispatcher 将消息投递给对应 mqtt_conn_fsm
mqtt_conn_fsm → Client: PUBLISH(qos=1, packet_id=P_sub, ...)   [异步]
mqtt_conn_fsm → Client: PUBACK(packet_id=P)                     [fsync 返回后立即]
```

**顺序约束**：PUBACK **先于** 向订阅者分发。若分发失败不影响 Producer 的可靠性契约（下游重试由订阅层负责）。

### 5.2 Subscribe / Dispatch 路径

- SUBSCRIBE(topic_filter, qos) → `broker_router:subscribe/3` 得 SubRef → 在 `broker_subscription` 注册 `{SubRef, ClientPid, TopicFilter, QoS, Cursor}`。
- 分区 Worker 收到 `append` 成功后，向本分区上所有 SubRef 的 dispatcher 广播 `{new_messages, Partition, FromOffset, ToOffset}`。
- Dispatcher（每个 SubRef 一个 gen_server 或直接嵌在 mqtt_conn_fsm 内，S1 选后者以减进程数）调用 `broker_storage:read_from` 拉取，转成 MQTT PUBLISH 发出，等待客户端 PUBACK 后调 `broker_storage:ack` 与推进 cursor。

### 5.3 游标持久化

- `broker_subscription` 的 cursor 存 ETS（内存）+ 定期 fsync 到 `broker_storage_wal` 的独立"cursor 段"（每 partition 一个 cursor 文件，写入 `{SubRef, Offset}`）。
- 刷盘周期：默认 100 ms 或每 64 条消息一次（可配）；S1 接受"最多丢 100 ms 的游标推进"，表现为**至少一次投递**（客户端可能收到已 PUBACK 过的消息重复），符合 MQTT QoS1 语义。
- S2 会将 cursor 与订阅元数据搬到 Mnesia；S1 用文件即可。

---

## 6. 监督树（S1 精简版）

```
broker_app_sup (application master)
├── broker_config_sup          [permanent]
│   └── broker_config_server
├── broker_storage_sup         [permanent]
│   └── broker_storage_wal_sup (per-partition worker 由 simple_one_for_one 起)
├── broker_router_sup          [permanent]
│   └── broker_router_hash
├── broker_queue_sup           [permanent, simple_one_for_one]
│   └── broker_queue_partition (per-partition GenServer)
├── broker_subscription_sup    [permanent, simple_one_for_one]
│   └── broker_subscription_worker
├── broker_session_sup         [transient, simple_one_for_one]
│   └── mqtt_session (clean_session=false 才有)
├── broker_proto_mqtt_sup
│   ├── ranch_listener_sup     [transient]  % 连接 acceptor
│   └── mqtt_conn_sup          [transient, simple_one_for_one]
│         └── mqtt_conn_fsm
└── broker_security_sup        [permanent]
    └── broker_security_stub
```

重启策略按 README §7.3 / roadmap §4.2 的原则区分：持连接的 transient，持分区/存储状态的 permanent。

---

## 7. 存储层设计

### 7.1 内存后端（`broker_storage_memory`）

ETS 表 `messages` = `{Partition, Offset, Msg}`；`append/3` 忽略 sync_mode，直接返回自增 offset。用途：Router / Subscription 单元测试、dev 环境快速迭代、`os_buffer` 档在 QoS0 场景的等价物。

### 7.2 WAL 后端（`broker_storage_wal`）

S0.3 spike 已验证正确性与性能，但 spike 代码存在下列**不可直接复用**的问题，需重写：

| spike 问题 | S1 修正 |
|---|---|
| 单一模块 `s0_3_wal.erl` 混合了 segment 管理、编码、恢复 | 拆为 `wal_segment` / `wal_writer` / `wal_reader` / `wal_recovery` |
| Group commit 由 bench harness 手动 batch，不是运行时能力 | 引入 `wal_group_committer` gen_server：mailbox 收集 append 请求，按 `batch_size`（默认 32）或 `batch_delay_ms`（默认 1）触发 fsync |
| 无进程模型，crash 会污染调用方 | 每分区一个 `wal_writer` gen_server，`broker_storage_wal_sup` 管理 |
| tmpfs 防护写在 bench 里 | 移到 `wal_writer:init/1`，open 时校验；不通过则拒绝启动（可配置关闭以支持 CI tmpfs） |
| 恢复逻辑只覆盖撕裂尾部截断 | S1 增加：段文件列表校验、offset 索引重建、cursor 段独立恢复 |

**编码格式**：`<<CRC32:32, Len:32, Term::erlang/external>>`，与 spike 一致；后续 S2 需评估切换到 protobuf/CBOR（记录到 open questions）。

### 7.3 段文件命名与保留

`data/wal/{topic_hash}/{partition_index}/segment-{StartOffset:020d}.log`。S1 **不实现段清理**（`truncate/2` 只是记录水位，不删文件），retention 策略在 S2 补齐。

---

## 8. 崩溃恢复验证（Gate S1 硬项）

### 8.1 编排

`test/integration/docker-compose.yml`：
- `broker`：单节点，挂载 `./data:/var/lib/unified-mq`（真实卷，非 tmpfs）
- `mqtt-client`：paho-mqtt Python 客户端 + mosquitto 命令行，通过外部脚本驱动
- 测试驱动脚本从宿主机 `docker compose kill -9 broker` 注入崩溃

### 8.2 断言清单

每条断言独立 common_test case，覆盖：

| # | 场景 | 断言 |
|---|---|---|
| R1 | Producer 收到 PUBACK 后 kill -9 | 重启后该消息在 WAL 中可恢复，Subscriber 重连后能收到（至少一次） |
| R2 | Producer 发送但未收到 PUBACK 时 kill -9 | 允许两种结果：消息完全丢失，或消息完整恢复并可投递；**禁止**出现"部分字节"或"撕裂记录被恢复" |
| R3 | Subscriber 已 PUBACK 后 kill -9 broker | 重启后 cursor 恢复到"最多 100 ms 之前"的位置；同一条消息可能被重投（QoS1 at-least-once 语义允许），但不得丢失后续消息 |
| R4 | 消息 append 中撕裂（模拟 fsync 前 kill） | 恢复时该记录被截断，之前所有已 fsync 记录完整；不产生虚构记录 |
| R5 | QoS0 消息在 kill 后 | 允许丢失（`os_buffer` 契约） |
| R6 | 恢复耗时 | 20k 条消息重放 < 500 ms（S0.3 基线 143 ms，留 3× 冗余） |

### 8.3 掉电模拟（可选，S1 不阻塞 Gate）

S0.3 REPORT §3 明确指出 `kill -9` 无法证明 `os_buffer` 档的真实丢失窗口。S1 沿用 kill -9，掉电模拟（dm-flakey / VM 硬复位）留待 S2 补齐；protocol-compatibility.md 与 ADR-007 已注明。

---

## 9. 任务分解与依赖

按可并行的最小闭环切分。每个任务附预估工作量（人日，含测试）。

| ID | 任务 | 依赖 | 预估 | 冻结产物 |
|---|---|---|---|---|
| T1 | rebar3 umbrella apps 骨架 + sys.config + vm.args + application master | — | 1 | 目录结构 |
| T2 | `broker_core`：`broker_message.hrl`、错误码、可靠性等级枚举、通用类型 | T1 | 0.5 | **ADR-009** |
| T3 | `broker_router` behavior + `broker_router_hash` 实现（含 MQTT 通配符匹配） | T2 | 2 | **ADR-010** |
| T4 | `broker_storage` behavior + `broker_storage_memory` | T2 | 1 | **ADR-011** |
| T5 | `broker_storage_wal` 重写（segment / writer / reader / recovery / group_committer） | T4 | 4 | — |
| T6 | `broker_queue` 分区 Worker（append 串行化 + storage 生命周期 + 通知订阅） | T3, T4 | 2 | — |
| T7 | `broker_subscription` 游标 + 分发 + cursor 定期落盘 | T6 | 2 | — |
| T8 | `broker_proto_mqtt` 帧编解码（含黄金样本单测） | T1 | 3 | — |
| T9 | `broker_proto_mqtt` 连接 FSM + 会话管理 + keepalive + packet_id | T8, T6, T7 | 3 | — |
| T10 | `broker_security` 桩 + 配置驱动用户表 | T1 | 0.5 | — |
| T11 | `broker_event` 桩（pass_through，接口位保留） | T2 | 0.5 | — |
| T12 | 监督树整合 + application 启动流程 + 集成 dev 环境 | T5-T11 | 1 | — |
| T13 | common_test 集成测试套件（端到端 pub/sub/ack、QoS0/1、clean_session） | T12 | 2 | — |
| T14 | docker compose 崩溃恢复测试套件（R1~R6） | T13 | 2 | — |
| T15 | 真实客户端兼容性测试（paho Python、mosquitto_pub/sub） | T13 | 1.5 | `docs/protocol-compatibility.md` MQTT 行 |
| T16 | `docs/architecture.md`（roadmap §9.2 交付物） + ADR-009/010/011 定稿 | T2-T5 | 1 | 三份 ADR |
| T17 | 三个核心抽象的评审会 + 冻结 | T16 | 0.5 | Gate S1 |

**关键路径**：T1 → T2 → T4 → T5 → T6 → T7 → T9 → T12 → T13 → T14 → T17。约 **22 人日**。T8 与 T3/T4 可并行；T10/T11 是低耦合桩，随时插入。

---

## 10. Gate S1 对齐（roadmap §3.4）

| Gate 项 | 由哪个任务证明 |
|---|---|
| 真实 MQTT 客户端完成 publish → subscribe → ack 全链路 | T15（paho + mosquitto） |
| `kill -9` 后重启，消息丢失/重复行为有测试断言且符合契约 | T14（R1~R6） |
| 三个核心抽象完成评审并冻结 | T17 + ADR-009/010/011 |
| `broker_storage` 的 WAL 与内存实现可互换 | T4 + T5 + T13（同一测试套件跑两个后端） |
| 在此 Gate 之前不实现第二个协议 | 硬约束，通过 T1 review 检查 |

---

## 11. 风险与开放问题

| 风险 | 缓解 |
|---|---|
| MQTT 帧自研编解码在边界情况（大 Remaining Length、非 UTF-8 topic、超长 ClientId）出错 | 黄金样本用 mosquitto 实际抓包；模糊测试用 `proper` 或简单 random gen |
| Session 层不持久化 → broker 重启后 clean_session=false 客户端行为与规范不符 | 明确写入 protocol-compatibility.md 的"S1 已知限制"列；S2 用 Mnesia 补齐 |
| Group commit 与 Producer 逐条 fsync 语义冲突（客户端预期每条 PUBACK 都在 fsync 之后） | Group committer 保证"批次内所有请求都在 fsync 返回后才回复"；测试覆盖并发 append |
| WAL 编码用 Erlang external term format，跨语言不可读、版本升级困难 | S1 接受，S2 评估切换到 CBOR/protobuf；开放问题 OQ-1 |
| Cursor 100 ms 刷盘周期 → 客户端可能看到重复 | QoS1 at-least-once 允许；测试断言"至少一次"而非"恰好一次" |
| 单节点分区 Worker 数固定，无法在线扩缩容 | S1 非目标，S4 处理 |

### 开放问题（S1 期间需答复）

- **OQ-1**：WAL 记录编码是否切到 CBOR？影响 S2 快照格式与跨版本升级。
- **OQ-2**：`broker_subscription` cursor 与 S4 Mnesia 迁移的接口边界如何设计，避免 S1 的文件方案变成技术债？
- **OQ-3**：Group commit 的 `batch_delay_ms` 默认值是否需要按可靠性等级区分（L1=0, L2=1）？
- **OQ-4**：MQTT 通配符匹配放在 Router 还是 Subscription？S1 暂定 Router，但 S2 引入共享订阅（Shared）后可能需要下沉。

---

## 12. 交付物清单

代码：`apps/*` 8 个应用；`test/integration/*`；`test/protocol-compatibility/mqtt/*`。

文档：
- `docs/design/s1-mqtt-vertical-slice.md`（本文，评审通过后升级 v1.0）
- `docs/architecture.md`（roadmap §9.2 交付物，覆盖 S1 落地后的实际结构）
- `docs/adr/009-broker-message-record.md`
- `docs/adr/010-broker-router-behavior.md`
- `docs/adr/011-broker-storage-behavior.md`
- `docs/protocol-compatibility.md` 追加 MQTT 行（roadmap §9.2 要求"S1 起持续更新"）
- `progress.md` 更新 S1 状态

测试证据：崩溃恢复测试 R1~R6 的 CI 输出；真实客户端兼容性测试报告。
