# S1 方案设计 — MQTT 垂直切片原型

- **文档版本**：v0.4（ADR 冻结同步版）
- **对应 Roadmap**：[`docs/roadmap.md`](../roadmap.md) §3
- **前置**：S0 已完成，ADR-001~008 已定稿；ADR-009/010/011（三抽象）**已冻结**（[`docs/adr/`](../adr/)）
- **目标 Gate**：roadmap §3.4「Gate S1」
- **状态**：设计评审收敛 + T0.5 三份 ADR 已冻结 + **T1 工程骨架已落地并实测通过**（§2 配置接线、§6 监督树按实测修正）→ 下一步 T2/T3/T4
- **修订记录**：v0.1 → v0.2 应用 27 项整改（B1~B4、C1~C6、S1~S6、D1~D13）；v0.2 → v0.3 应用复审整改 12 项（E1~E12）；v0.3 → v0.4 应用冻结评审 6 项（F1~F6），关键变更见 §13 变更摘要

---

## 1. 目标与非目标

### 1.1 目标

以 MQTT 3.1.1 为首个协议，跑通 README 附录 A 的 7 步端到端闭环：
Producer publish → (S1 阶段 CloudEvents 走 pass_through，见 §11 OQ-5) → Router 解析 → 分区 → WAL 追加并 fsync → PUBACK → Consumer subscribe/receive → 消费者 ACK → 崩溃后正确恢复。

三个核心抽象（`broker_message` / `broker_router` / `broker_storage`）在 S1 评审通过后**冻结**并升级为 ADR-009/010/011，S2+ 只能扩展、不得破坏式修改（保留 optional callback hook 除外，见 §3.2、§3.3）。

### 1.2 非目标（S1 明确不做）

| 项 | 推迟到 |
|---|---|
| Raft / 分区复制 / 集群成员 / Epoch Fencing | S4 |
| Metadata Raft Group、Mnesia 控制面表（tenant/acl/partition_meta 等） | S4 |
| CloudEvents codec / binding / 校验策略（S1 只保留 `broker_message.event` 字段位；策略桩为 `pass_through`，见 OQ-5） | S3 |
| MQTT QoS 2、Retain、Will 投递语义、MQTT 5.0、User Properties | S2 |
| TLS、密钥轮换、真实 ACL | S6 |
| 分区 Worker 的多分区调度、热点分区处理、慢消费者隔离 | S2 |
| AMQP / Kafka / NATS / WebSocket / Pulsar 适配器 | S5 |
| DLQ / TTL 生效 / retention / 重试上限 / 指数退避（`expires_at` 字段保留但不生效，见 §3.1） | S2 |
| Prometheus / OpenTelemetry / 管理 API | S2 起最小集，S6 完整 |

S1 只做**单节点、无复制、内存 + 本地 WAL 两种存储实现**，可靠性等级仅覆盖 ADR-007 的 L1 `os_buffer` 与 L2 `fsync`；L3 `replicated_commit` 保留 behavior 接口位不实现，两个后端 `capability/1` 均不含该模式。

---

## 2. 应用与目录布局

按 roadmap §3.1，只建 S1 会实际用到的 app：

```
apps/
├── broker_core/          # 【仅类型与行为】broker_message.hrl、错误码、可靠性等级枚举、principal/partition_id 等公共类型、behavior 定义
├── broker_router/        # destination → partition_id + SubRef 索引
├── broker_queue/         # 分区 Worker、追加/读取编排、in-flight 管理
├── broker_subscription/  # 订阅、per-sub cursor、投递语义、订阅集合与 in-flight 持久化
├── broker_storage/       # 存储 behavior + memory 与 WAL 两种实现
├── broker_event/         # S1 桩：pass_through；接口位保留 cloud_event 记录
├── broker_security/      # S1 桩：username/password 认证回调
└── broker_proto_mqtt/    # MQTT 3.1.1 帧编解码、连接 FSM、会话管理、消息构造

config/
├── sys.config.src
└── vm.args.src

tools/
└── docker/
    └── docker-compose.s1.yml        # 崩溃恢复测试编排（真实卷，非 tmpfs）

test/
├── integration/          # common_test：端到端 pub/sub/ack + 崩溃恢复断言
└── protocol-compatibility/mqtt/  # 真实客户端（paho、mosquitto_pub/sub）
```

**应用职责边界**：`broker_core` 只提供 header/类型/behavior 定义，**不含业务函数**；消息构造、时间戳生成、ID 分配下沉到各 app 内部（`broker_proto_mqtt` 构造 `#broker_message{}`，`broker_storage` 分配 offset，`broker_subscription` 维护 cursor）。避免 `.hrl` include 依赖导致 app 循环，防止 `broker_core` 变成"上帝 app"。

**配置读取**：S1 无 `broker_config` app，全部通过 `application:get_env/2` 直读 `sys.config`；监督树无 `broker_config_sup`。roadmap §3.1「Mnesia 只放最小配置」在 S1 进一步降级为「无 Mnesia，纯静态配置」。

**T1 实测的 relx 接线**：`config/sys.config.src` 与 `config/vm.args.src` 必须用 relx 的 `{sys_config, ...}` / `{vm_args, ...}` 键接入。用 `{sys_config_src, ...}` / `{vm_args_src, ...}`（rebar3 3.27）会把文件原名拷进 `releases/Vsn/`（仍是 `*.src`），启动脚本找不到而不拉起节点。两文件均不含 `{{...}}` 占位，因此 `rebar3 shell` 的 `{config, "config/sys.config.src"}` 可直接复用同一份；`vm.args` 则只有 release 节点读，shell 不读（实测为 `nonode@nohost`）。

`spikes/` 只读，S1 阶段不得被 `apps/*` 引用（roadmap §2 硬约束）。S0.3 的 WAL 代码作为参考，需在 `broker_storage` 中重写为生产版本，原因见 §7.2。

---

## 3. 三个必须冻结的核心抽象

以下三者构成 S2+ 所有协议 / 存储 / 复制实现的挂载点。**T0.5 已冻结为 [`ADR-009`](../adr/009-broker-message-record.md) / [`ADR-010`](../adr/010-broker-router-behavior.md) / [`ADR-011`](../adr/011-broker-storage-behavior.md)——本节是摘录，任何 API 冲突以 ADR 为准。** S1 编码时以 `apps/broker_core/include/*.hrl` 与 behavior 模块为唯一真相源。**每个数字阈值旁标注 S0 证据来源**。

### 3.1 `broker_message` 记录（`apps/broker_core/include/broker_message.hrl`）

```erlang
-type partition_id() :: non_neg_integer().
-type offset()       :: non_neg_integer().   % 分区内单调递增，由 storage 分配
-type principal()    :: binary().            % 已认证主体；S1 由 broker_security 桩返回
-type sync_mode()    :: os_buffer | fsync | replicated_commit.  % ADR-007 三档

-record(broker_message, {
    message_id        :: binary(),            % Broker 内部 ID，与 CloudEvents.id 分离（ADR-005）
    tenant_id         :: binary(),            % S1 默认 <<"default">>
    destination       :: binary(),            % MQTT topic / AMQP routing key / ...
    partition_id      :: partition_id(),      % 纯整数 ID，不含 hash 语义；hash 属 Router 实现细节
    payload           :: binary(),
    payload_encoding  :: raw | json | binary | text,
    headers           :: map(),               % S1 恒空；语义 = CloudEvents 扩展属性 + 未来跨协议公共元数据
    proto_meta        :: map(),               % S1 所有协议私有字段进此：MQTT 存 #{qos, dup, retain, packet_id}
    persistence       :: transient | durable,
    reliability       :: sync_mode(),         % ADR-007，per-message
    created_at        :: integer(),           % ms epoch，Broker 接收时间
    expires_at        :: integer() | undefined,  % S1 保留但不生效，见冻结点 4
    event             :: undefined | map()    % S3 填充：cloud_event 视图或原始字节
}).
```

**冻结点**：

1. **`message_id` 与 `event.id` 分离**（ADR-005）。后续任何协议私有 ID（MQTT packet_id、Pulsar MessageId、Kafka offset）**不得**写入 `message_id`；协议私有 ID 归 `proto_meta`。
2. **`reliability` 是 per-message 属性**，不是全局配置——生产者可按消息覆盖；storage `append/3` 依此分路径。
3. **`partition_id` 是纯身份整数**：分组信息（哪些 partition 属同一 Raft group，ADR-008 默认 16 分区/组）是 S4 元数据，不进入消息模型；topic→partition 的哈希映射是 Router 私有实现，不暴露给记录。
4. **`expires_at` S1 保留但不生效【F4】**：S1 两种 `persistence` 一律写 `undefined`，dispatcher 不检查、不丢弃；S2 起由写路径赋值、dispatcher 检查过滤。**不得**在 S1 派生 `transient → created_at + 5min` 之类占位值落进不可变 WAL 段——那会让 S2 用从未被任何 ADR 承诺过的时间戳误判过期，等于已 ACK 消息被静默丢弃（详见 ADR-009 §9.3）。字段保留是为了遵守 roadmap §3.2「定稿后不改」。
5. **`headers` 与 `proto_meta` 分工**：`headers` 语义收窄为"CloudEvents 层扩展属性 + 跨协议公共元数据"（S1 恒空）；所有协议私有字段（含 MQTT QoS/packet_id、未来的 Kafka partition key 等）一律进 `proto_meta`。

### 3.2 `broker_router` behavior（`apps/broker_router/src/broker_router.erl`）

> 本节是 ADR-010 的摘录，**以 ADR-010 为准**（T0.5 冻结整改：原 v0.3 文本仍是在冻结前起草的 API，已同步）。

```erlang
-record(route_ctx, {
    tenant_id   :: binary(),
    publisher   :: principal() | undefined,   % 已认证主体；S1 桩阶段可为 undefined
    reliability :: sync_mode()
}).
-type route_ctx() :: #route_ctx{}.   % 【G1】record 名不自动产生同名类型，须显式别名

-type router_env() :: term().   % 实现私有；S1 = 持有 ETS 表的进程标识/表参考
-type sub_ref()    :: term().   % 订阅标识，由调用方生成

%% 回调不接收也不返回 State（【F3】），只吃不透明 env 句柄
-callback init(Config :: map()) -> {ok, router_env()} | {error, term()}.

%% 读路径：发布者进程内直接调用，非 gen_server:call，ETS 并发读
-callback route(Destination :: binary(), Ctx :: route_ctx(), router_env()) ->
    {ok, [{partition_id(), [sub_ref()]}]} | {error, term()}.

%% 写路径：把调用方已生成的 SubRef 登记进 filter 索引；经单一写者进程串行更新
-callback register(router_env(), Filter :: binary(), SubRef :: sub_ref(), SubOpts :: map()) ->
    ok | {error, term()}.

-callback unregister(router_env(), SubRef :: sub_ref()) -> ok | {error, term()}.

%% S4 分区迁移钩子，S1 可选实现
-callback on_partition_change(router_env(), Event :: term()) -> ok | {error, term()}.
-optional_callbacks [on_partition_change/2].   %% 【E4】复数属性名，合法 Erlang
```

**S1 默认实现**：`broker_router_hash`——topic 字符串哈希到 N 个 `partition_id`（N 由 config，默认 4）；订阅侧实现 MQTT 通配符匹配（`+` 单层、`#` 多层，OASIS MQTT 3.1.1 §4.7），返回匹配的 SubRef 列表。

**冻结点**：

1. **Router 职责 = "路由 + 订阅索引"**：SubRef 指向 `broker_subscription` 中登记的订阅实体（含 per-sub cursor、消费者 PID），Router **只维护 filter→SubRef 索引**，不持有 cursor、不感知 storage、不做 ACK 决策；判据可机械检查：`broker_router` 任何实现不得调用 `broker_subscription`/`broker_storage` API。
2. **`route/3` 不做进程串行化【F3】**：回调不接收也不返回 `State`；读路径在调用者进程内并发查 ETS，索引写入才经单一写者。否则所有 publish 串行过一个进程，违反 README §7.2 与 Gate S2 第 4 项。
3. **SubRef 由 `broker_subscription` 铸造【F5】**：流程固定为「创建实体 → `register/4` → `route/3` 原样返回 → `unregister/2`」；Router 从不解引用 SubRef。
4. **通配符匹配暂放 Router**（OQ-4）：S1 保持现状；S2 引入 Shared subscription 后可能下沉到 Subscription，届时通过**新增** optional callback 处理，不修改 route/3 签名。
5. **`on_partition_change/2` 是 optional hook**：S1 实现可 no-op；S4 集群上线时不破坏 behavior。
6. **索引不持久化【F3/F5 连带】**：重启后由 §5.2 的启动顺序（`recover/2` → 读 `sessions/*.subs` → `register/4` 回填 → 才放开 listener）重建，见 ADR-010 §10.6。

### 3.3 `broker_storage` behavior（`apps/broker_storage/src/broker_storage.erl`）

> 本节是 ADR-011 的摘录，**以 ADR-011 为准**（T0.5 冻结整改：原 v0.3 文本含已被 F1/F2 移除的 `AckRanges` 与合并式 `open/2`，已同步）。

```erlang
-type handle() :: term().   % 后端私有；每个 partition 一个 handle

%% 显式创建分区；已存在必须报错，不得静默打开
-callback create(PartitionId :: partition_id(), Config :: map()) ->
    {ok, handle()} | {error, {already_exists, partition_id()}} | {error, term()}.

%% 恢复既有分区：绝不自动创建（【F2】）
%% CommittedOffset = 全局 ack 水位（连续前缀）；HighOffset = 下一条追加位置
-callback recover(PartitionId :: partition_id(), Config :: map()) ->
    {ok, handle(), CommittedOffset :: offset(), HighOffset :: offset()}
      | {error, {not_found, partition_id()}} | {error, term()}.

-callback close(handle()) -> ok.

%% 追加：sync_mode 是 per-message；运行时校验由实现兜底
-callback append(handle(), [#broker_message{}], sync_mode()) ->
    {ok, [offset()]} | {error, {unsupported_sync_mode, sync_mode()}} | {error, term()}.

-callback read_from(handle(), FromOffset :: offset(), Limit :: pos_integer()) ->
    {ok, [{offset(), #broker_message{}}], NextOffset :: offset()} | {error, term()}.

%% storage 只推进"全局 committed watermark"；per-sub cursor 归 broker_subscription 层维护
-callback advance_committed(handle(), Offset :: offset()) -> ok | {error, term()}.

%% 语义 = 逻辑不可读边界，非物理删除
%% 返回 OK 表示此 offset 之前的记录不会再被 read_from；物理清理是 S2 retention 策略职责
-callback truncate(handle(), BeforeOffset :: offset()) -> ok | {error, term()}.

%% capability 是实例级（与 handle 绑定），允许同一后端在不同磁盘/durability 配置下暴露不同能力
-callback capability(handle()) -> [sync_mode()].
```

**S1 交付两份实现**：

- `broker_storage_memory`：ETS + `queue:queue()`，`capability/1` 恒返回 `[os_buffer]`；进程崩溃即丢。用途：dev/test、Router 单测、QoS0 消息路径。**E12**：memory 后端部署时，listener 必须配 `mqtt_max_qos = 0`（见 §4.4），使 QoS1 订阅在 SUBACK 即回 `0x80` 拒绝，不依赖运行时断连。
- `broker_storage_wal`：分段追加日志（沿用 S0.3 §2 编码格式），`capability/1` 返回 `[os_buffer, fsync]`；`fsync` 档走 group commit（batch=32 拐点，S0.3 §2.2；上限 256；延迟 `batch_delay_ms` 按等级区分 L1=0/L2=1，见 §7.2）。

**冻结点**：

1. **`create/2` 与 `recover/2` 必须分离【F2】**：`recover` 缺失时报 `{not_found, PartitionId}`，**绝不自动创建**。合并式 `open` 会把"分区 ID 打错/挂载点丢失"变成"恢复出一个空分区并报告健康"，即静默数据丢失；`not_found` 是可告警的显式失败。
2. **per-message sync**：`append/3` 第三参数决定该批 fsync 行为；storage 层按 sync_mode 分路径。
3. **capability 双层校验（E2 整改）**：
   - **Adapter 缓存（早失败）**：`mqtt_conn_fsm` 启动时从 config 读 listener 绑定后端的 capability 集合并缓存（或经 `broker_queue:capability/1` 查一次），QoS 不兼容在 SUBACK/CONNACK 阶段即拒绝，避免消息进分区才失败；
   - **queue/storage 运行时兜底（defense-in-depth）**：`append/3` 收到超出 capability 的 sync_mode **必须**返回 `{error, {unsupported_sync_mode, _}}`，不得静默降级。缓存仅为优化，正确性由兜底保证，两者不一致时以运行时为准。
4. **L3 接口位存在但不实现**：`replicated_commit` 在 S1 两个后端 `capability/1` 均不返回；S4 由 `broker_storage_replicated`（新后端，走 ra）加进 capability 集合，**不修改 behavior**。
5. **`advance_committed/2` 语义**：只推进"全局 committed watermark"（= 所有订阅都 ack 过的连续前缀），不负责 per-sub cursor。per-sub cursor 由 `broker_subscription` 通过独立文件（S1）/Mnesia（S2+）维护。此切分让 S4 复制层能对齐"共识提交点 = committed watermark"。
6. **水位不对称容错【F1】**：committed watermark **偏低只导致重复投递**（至少一次语义可接受），**偏高会让 truncate 提前吃掉未 ack 消息**，属零容忍。恢复时取 WAL 中可解码记录区间与持久化下界的保守交集，宁可偏低。
7. **不设 `AckRanges`【F1 整改】**：本模型下 storage 只有单一连续前缀水位，非连续 ack 区间集合**没有归属者**（per-sub cursor 在 `broker_subscription`），v0.2 想用 `AckRanges` 解决的"offset 列表内存爆炸"（D10）因此被结构性消除。`recover/2` 补 `HighOffset`，否则恢复后无法确定追加位置。
8. **`truncate/2` 语义**：只设置逻辑不可读边界，**不删文件**。物理段清理在 S2 retention 策略中实现，届时新增 optional callback 而非改现有签名。
9. **`capability/1` 是实例级**：与 handle 绑定，允许同一模块开不同 durability 的实例。

---

## 4. MQTT 首批范围与实现要点

### 4.1 帧编解码

MQTT 3.1.1 帧结构简单，S1 **不引入外部依赖**，在 `apps/broker_proto_mqtt/src/mqtt_frame.erl` 自研编解码（约 400 行），覆盖：

| Packet | 方向 | S1 | 备注 |
|---|---|---|---|
| CONNECT / CONNACK | C→S / S→C | ✅ | 含 Will 字段解析但**忽略**，见 §4.5 |
| PUBLISH QoS0 | 双向 | ✅ | |
| PUBLISH QoS1 / PUBACK | 双向 | ✅ | |
| SUBSCRIBE / SUBACK | C→S / S→C | ✅ | 请求 QoS 超过 `mqtt_max_qos` → SUBACK 对应返回 `0x80` |
| UNSUBSCRIBE / UNSUBACK | C→S / S→C | ✅ | |
| PINGREQ / PINGRESP | 双向 | ✅ | keepalive 1.5× 无包断开 |
| DISCONNECT | C→S | ✅ | |
| PUBLISH QoS2 / PUBREC / PUBREL / PUBCOMP | — | ❌ S2 | 收到即断连，见 §4.5 |

变长 Remaining Length、UTF-8 字符串、topic filter 通配符校验严格按 OASIS MQTT 3.1.1 规范；单元测试用 mosquitto 抓包字节做黄金样本，配合 `proper` 模糊测试。

### 4.2 连接 FSM 与会话

```
mqtt_listener (ranch acceptor)
  └── mqtt_conn_fsm (per-connection gen_statem, transient)
        ├── 状态：awaiting_connect → connected → disconnected
        ├── keepalive 定时器（1.5× KeepAlive）
        ├── packet_id 分配器（QoS1 in-flight，per-session）
        ├── capability 缓存（启动时查一次，E2）
        └── 关联 mqtt_session（per-ClientId，clean_session=false 时跨连接存活 + 持久化）
```

- **`clean_session=true`**：连接建立时新建临时 session，断开即销毁；订阅与 in-flight 不持久化。
- **`clean_session=false`**：session 状态由 `broker_subscription` 持久化（文件布局见 §5.3），跨连接保留：
  1. **订阅集合**（filter list）——每次 SUB/UNSUB 即 fsync，强一致；
  2. **packet_id 分配器 + in-flight 集合**——与 WAL 批量路径同频刷盘，允许回退（E3 整改，理由见 §5.3）。

**Gate 措辞解释（E1，v0.3 复审已确认）**：roadmap §3.4 第 2 条「已 ACK 消息不丢、订阅位置不丢、未 ACK 消息重投」与第 3 条「明确可容忍的重复窗口」合并解释，逐词对应断言如下：

| roadmap §3.4 原文 | 本文档契约 | 断言 | 回退容忍度 |
|---|---|---|---|
| **已 ACK 消息不丢**（Producer 侧 PUBACK 过的消息） | 消息 durability | R1a | **零容忍**，必须可读回 |
| **订阅位置不丢** — 订阅**集合**（filter 列表） | 订阅注册 durability | R1b | **零容忍**，`.subs` 每次变化即 fsync |
| **订阅位置不丢** — 消费 **cursor** | 消费位置 durability | R3 | **有界回退** ≤`cursor_max_regress`（默认 64 条 / 100 ms）——不重置归零、不整体丢失，仅允许窗口内重复 |
| **未 ACK 消息重投** | 至少一次语义 | R3(ii) + R7 | 重投是**契约行为**，非缺陷；in-flight 允许回退，表现为 DUP=1 |

**为何"订阅位置不丢"允许有界回退**：roadmap §3.4 第 3 条明文要求「明确可容忍的重复窗口」——若 cursor 位置是零容忍，重复窗口就无从定义，第 3 条将成为空文。故"不丢"约束的是 **durability**（位置持久存在、崩溃后不回退到订阅起点或归零），而非 **恰好一次投递**。cursor 回退 K 条与 producer 侧"已 ACK 不丢"分属两个不同承诺，不可互借措辞。

**参数化**：`cursor_max_regress`（默认 64）、`cursor_flush_interval_ms`（默认 100）为 config 项；调为 `1 / 0` 即逼近零回退（吞吐代价由调用方承担，见 §5.3）。测试须用**默认值与零回退值各跑一轮 R3**，证明参数语义真实有效，而非只测默认档。

此解释于 v0.3 复审确认，T17 评审会正式签认后写入 ADR-011 附录与 `docs/protocol-compatibility.md`。

### 4.3 认证

`broker_security` 桩：`authenticate(Username, Password) -> {ok, Principal :: principal()} | {error, Reason}`。S1 默认实现从 `sys.config` 读明文用户表，Principal = binary username；ACL 桩一律返回 `allow`。真实 ACL / TLS 在 S6。

### 4.4 QoS 与可靠性映射

| MQTT QoS | S1 broker 行为 | ADR-007 等级 | 校验方式 |
|---|---|---|---|
| QoS 0 | 不入 WAL，直接进 Router → 匹配订阅 → 分发 | `os_buffer` | 无需校验，两后端均支持 |
| QoS 1 | 入 WAL，`sync_mode = fsync`，PUBACK 在 fsync 返回后发出 | `fsync` | **CONNACK 检查（E12）**：`req_qos ≤ mqtt_max_qos`？否 → SUBACK `0x80` 拒绝订阅 / PUBLISH 断连。`mqtt_max_qos` 由 listener config 结合后端 capability 推导（memory 后端必须配 0） |
| QoS 2 | 拒绝，见 §4.5 | — | — |

**关键契约（ADR-007）**：QoS1 的 PUBACK **必须**在 `broker_storage:append(..., fsync)` 返回 `{ok, [Offset]}` 之后发出——不得提前。§5.1 流程图与 §8.2 R1a 断言以此为准。

### 4.5 QoS2 / Retain / Will 拒绝策略

S1 不实现这些语义，但**不得静默**：

- **QoS2 SUBSCRIBE**：SUBACK 对应 topic 返回 `0x80`（Failure），不建立订阅。
- **QoS2 PUBLISH**：立即关闭连接（`gen_statem:stop`），日志记录 `mqtt_qos2_unsupported`；不发 PUBACK/PUBREC。
- **Retain flag on PUBLISH**：日志 `mqtt_retain_ignored`，消息正常处理 retain 位丢弃。
- **CONNECT 携带 Will**：日志 `mqtt_will_ignored`；正常连接建立，**Will 字段（含其 QoS 与 payload）一律忽略，不做任何存储**（E9 整改：删除"Will QoS=1 需 fsync 校验"的矛盾措辞——Will 既不存储，就无存储路径也无校验点）。
- **MQTT 5.0 版本号**：`CONNECT` 里 `Protocol Level = 5` 一律拒绝（CONNACK 返回原因码 `0x87` Not supported，附 log）；强制走 3.1.1。

上述策略在 protocol-compatibility.md 能力矩阵的 MQTT 行明确标注"部分支持"，避免测试断言与实现漂移。

---

## 5. 数据面流程

### 5.1 Publish 路径（QoS1）

```
Client PUBLISH(qos=1, packet_id=P, topic=T, payload=B)
  ↓
mqtt_conn_fsm:handle_publish
  ├─ (a) 查 conn_fsm 本地 capability 缓存（启动时经 broker_queue:capability/1 或 config 取得）；
  │      不支持 fsync → 断连（早失败）；queue/storage 层仍有运行时兜底校验（§3.3 冻结点 3）
  ├─ (b) 构造 #broker_message{reliability=fsync, proto_meta=#{qos=>1, packet_id=>P}, ...}
  ├─ (c) broker_router:route(T, Ctx, Env) → [{PartitionId, [SubRef1, SubRef2]}]
  │        ↓ 在 conn_fsm 自己的进程内直接查 ETS，无 gen_server:call、无串行化（§3.2 冻结点 2）
  ├─ (d) broker_queue:append(PartitionId, Msg, fsync)
  │        ↓ 分区 Worker 串行化（多 conn_fsm 并发投递由 mailbox 吸收）
  │      broker_storage_wal:append(Hd, [Msg], fsync)
  │        ↓ group committer 收集，batch 满 32 或 1 ms 后 fsync
  │      {ok, [Offset]}
  ├─ (e) **fsync 返回后立即发 PUBACK**（ADR-007 硬契约点）
  └─ (f) 异步：broker_subscription:dispatch(SubRefs, {PartitionId, Offset, Msg})
         （in-flight 集合更新写入 session .inflight 缓冲，不逐条 fsync，见 §5.3）
```

**顺序约束**：

- **(e) PUBACK 只依赖 (d) storage append 返回**——**先于** (f) dispatch，不等待 dispatch 完成。
- (f) dispatch 失败（分区 Worker 过载、订阅者断连等）**不回滚** Producer ACK；Producer 已看到的可靠性承诺不因下游抖动而撤销。下游重试由 `broker_subscription` 层的 cursor 与重投机制负责（S2 补齐指数退避）。
- 若未来引入 `replicated_commit`，(e) 等待点变为 Raft commit 返回；behavior 契约不变。

### 5.2 Subscribe / Dispatch 路径

- SUBSCRIBE(topic_filter, qos) → `broker_subscription` **先创建订阅实体**（生成 SubRef，登记 `{SubRef, ClientPid, TopicFilter, QoS, Cursor, InflightSet}` + 持久化订阅集合，§5.3 `.subs` 文件），再调 `broker_router:register(Env, Filter, SubRef, Opts)` 把 SubRef 登记进索引（§3.2 冻结点 3【F5】：SubRef 由订阅层铸造，Router 不生成）。
- 分区 Worker 收到 `append` 成功后，向本分区上所有 SubRef 的 dispatcher 广播 `{new_messages, PartitionId, FromOffset, ToOffset}`。
- Dispatcher **内嵌在 `mqtt_conn_fsm` 内**（S1 选择：减少进程数；S2 引入独立 dispatcher 时通过 optional callback 扩展，不破坏 §3.2/§3.3）。调用 `broker_storage:read_from/3` 拉取 → 转 MQTT PUBLISH → 等客户端 PUBACK → 调 `broker_subscription:ack` → 后者聚合"全部订阅都 ack 的连续前缀" → 定期 `broker_storage:advance_committed/2`。

**离线积压机制（E7 整改）**：clean_session=false 订阅者离线期间**不存在 broker 侧内存离线队列**——消息持久化在 WAL 中，积压仅表现为该订阅 cursor 滞后；重连时 conn_fsm 从 cursor 起 `read_from` 追赶。两条 S1 限制写入 protocol-compatibility.md：
1. 追赶深度受 WAL 段保留约束——S2 retention 策略前，段文件不删、理论全量可追；但追赶本身是单连接顺序读，积压极深时首条消息延迟线性增长（已知行为，非丢消息）。
2. 追赶期间到达的新消息与积压消息按 offset 顺序统一投递，无优先级反转。

### 5.3 游标与订阅持久化（E3 整改：三层更新频率）

`broker_subscription` 维护三类落盘状态，均位于 `data/`：

| 文件 | 内容 | 变化频率 | fsync 策略 | 回退窗口 |
|---|---|---|---|---|
| `sessions/{clientid}.subs` | 订阅集合（filter list） | 仅 SUB/UNSUB | **每次变化即 fsync** | 0（强一致）|
| `sessions/{clientid}.inflight` | packet_id 分配器 + in-flight 集合 | 每条 QoS1 投递/ack | 批量缓冲，随 group commit 节拍刷盘 | ≤64 条 / 100 ms |
| `subscriptions/cursors/{subref}.cursor` | per-sub cursor offset | 每条消费 ack | 每 `cursor_flush_interval_ms`（默认 100）或每 `cursor_max_regress`（默认 64）先到触发 | ≤64 条 / 100 ms |

**为什么分层刷盘**：in-flight 集合每条 QoS1 消息都变化；若"每次变化即 fsync"，等于每条消息额外一次独立 fsync（S0.3 §2.1 实测 ~871 µs），group commit 的 17× 收益直接归零。而 MQTT 3.1.1 §4.1 本就允许 session 恢复时 broker **重发未确认消息**（DUP=1），in-flight 精确性不是规范契约——允许回退只多不少。**订阅集合的强一致（Gate 核心）不受影响。**

**语义总结**：
- 订阅集合强一致 → Gate「集合不丢」（R1b）；
- cursor 与 in-flight 允许有界回退 → **至少一次投递**（R3、R7），重复是契约内行为。
- `broker_storage:advance_committed` 是**独立**的存储水位（S2 retention 与 S4 复制用），不替代 per-sub cursor 文件。

**S2 迁移路径（OQ-2）**：Mnesia 上线后三个文件全部退役迁入表（`subscription_meta`、`cursor_meta`、`session_meta`），`broker_subscription` 对外 API 不变。

**回退窗口可配置到零**：`cursor_max_regress = 1` + `cursor_flush_interval_ms = 0` 时 cursor 逐条 fsync，逼近零回退（代价：每条消费 ack 一次独立 fsync，与 §7.2 group commit 收益对冲）。**默认档为有界回退**，零回退档仅供对重复敏感的调用方显式开启；R3 测试两档都要跑（§8.2），证明该参数语义真实而非纸面。`.inflight` 不提供零回退档——MQTT 3.1.1 §4.1 本就允许 session 恢复时重发未确认消息，精确性非规范契约。

---

## 6. 监督树（S1 精简版）

**T1 实测修正（原图有单一 `broker_app_sup`）**：S1 不引入伞形 application master。8 个 app 各带自己的顶层 supervisor，跨 app 的启动顺序由 `.app.src` 的 `applications` 依赖列表决定，OTP 按拓扑序启动。原 `broker_app_sup` 那一层没有承载者——它若存在，就必须把别的 app 的 supervisor 当自己的 child 起，那些 app 反而不能再作为 application 启动，等于把 OTP 的依赖机制绕开重造一遍。

```
broker_storage_app      → broker_storage_sup                          [one_for_all]
│                         └── broker_storage_wal_sup（T5）             [one_for_one]
│                               └── wal_writer (per partition，静态)
broker_router_app       → broker_router_sup                           [one_for_all]
│                         └── broker_router_hash（T3）
broker_queue_app        → broker_queue_sup                            [one_for_one]  % 【E11】分区数固定（config N=4），静态 child spec
│                         └── broker_queue_partition (per partition)  [permanent]
broker_subscription_app → broker_subscription_sup                     [one_for_one]
│                         └── broker_subscription_worker（T7）
broker_event_app        → broker_event_sup                            [one_for_all]
broker_security_app     → broker_security_sup                         [one_for_all]
broker_proto_mqtt_app   → broker_proto_mqtt_sup                       [one_for_all]
                          ├── ranch_listener_sup（T12）                [transient]
                          └── mqtt_conn_sup                            [transient, simple_one_for_one]  % 连接进程动态
                                └── mqtt_conn_fsm                      [transient]
broker_core：无模块、无 supervisor（纯 `.hrl` 与公共类型，见 §2）
```

实测启动序列（`application:ensure_all_started(broker_proto_mqtt)` 返回的顺序，OTP 28.3.1）：
`sasl → broker_core → broker_storage → broker_router → broker_queue → broker_subscription → broker_event → broker_security → broker_proto_mqtt`。

**重启策略原则（README §7.3 / roadmap §4.2）**：
- 持连接的 → transient（simple_one_for_one 动态起，崩溃不重启，由客户端重连驱动）；
- 持分区/存储/订阅状态的 → **permanent + one_for_one**（E11：这类 Worker 崩溃**必须**重启，`simple_one_for_one` 的 transient 语义会静默丢分区，是错误配置）；
- session 不再是独立子树——session 状态是 `broker_subscription` 持久化数据 + conn_fsm 内存视图，跨连接存活由 `.subs` 文件保证，无需独立 `mqtt_session` 进程（进程级存活本就随 conn 断开而终结）。

**启动顺序（T0.5 冻结连带要求，ADR-010 §10.6 / ADR-011 §11.2）**：Router 的 `Filter → SubRef` 索引**不持久化**，因此不能"起完就接单"。四个 boot step 分别落在依赖链上的 supervisor `init/1` 里（`broker_storage_sup` → `broker_subscription_sup` → `broker_proto_mqtt_sup`），顺序由上面的 `applications` 拓扑保证：

1. `broker_storage_wal` 对各分区 `recover/2`（缺分区即 `{not_found, _}` 报错，**不自动 create**），得到 `CommittedOffset`/`HighOffset`；
2. `broker_subscription` 读 `sessions/*.subs` 重建订阅实体并铸造 SubRef；
3. 逐个 `broker_router:register/4` 回填索引；
4. **以上全部成功后**才启动 `ranch_listener_sup` 接单。

第 4 步的顺序性是 Gate S1「订阅集合不丢」（§8.2 R1b）的实现前提：若 listener 先起，新连接 SUBSCRIBE 会与回填竞争，回填的旧订阅可能覆盖或被旧 SubRef 污染，R1b 变成概率性通过。任何 step 失败 → 整个 application 启动失败并告警，**不得**降级为"部分恢复后继续服务"。

---

## 7. 存储层设计

### 7.1 内存后端（`broker_storage_memory`）

ETS 表 `messages` = `{PartitionId, Offset, Msg}`；`append/3` 只支持 `os_buffer`，caller 传其他 sync_mode 返回 `{error, {unsupported_sync_mode, _}}`；offset 单调递增。`capability/1` → `[os_buffer]`。部署此后端时 listener 必须配 `mqtt_max_qos = 0`（§3.3、§4.4）。

### 7.2 WAL 后端（`broker_storage_wal`）

S0.3 spike 已验证正确性与性能，spike 代码存在下列**不可直接复用**问题，需重写：

| spike 问题 | S1 修正 | 证据 |
|---|---|---|
| 单一模块混合 segment/编码/恢复 | 拆为 `wal_segment` / `wal_writer` / `wal_reader` / `wal_recovery` / `wal_group_committer` / `wal_index` | S0.3 §7 |
| Group commit 由 bench harness 手动 batch，不是运行时能力 | `wal_group_committer` gen_server：mailbox 收集 append 请求，按 `batch_size` 或 `batch_delay_ms` 触发 fsync | S0.3 §2.2 batch=32 拐点 |
| **bench 为单写者模型，多写者并发从未实测**（S0.3 §6 明确待办） | **T5 交付项含并发正确性测试（E5，见 §9）**：多进程交叉 append，断言批次内每个 caller 都在 fsync 完成后才收到回复、回复 offset 单调、无 cross-reply | S0.3 §6 待办 |
| 无进程模型，crash 污染 caller | 每分区一个 `wal_writer` gen_server，`broker_storage_wal_sup` 管理 | — |
| tmpfs 防护写在 bench | 移到 `wal_writer:init/1`；不通过则拒绝启动（可配置关闭以支持 CI） | S0.3 §5 tmpfs 陷阱 |
| 恢复逻辑只覆盖撕裂尾部截断 | 增加：段文件列表校验、offset 索引重建、session/cursor 文件独立恢复 | S0.3 §4 torn_tail_recovery_test |
| 记录编码直接序列化 `#broker_message{}` | 独立于消息模型的紧凑元组编码（见下方） | — |

**Group commit 默认值**：`batch_size = 32`（S0.3 §2.2 拐点）；`batch_delay_ms` 按 sync_mode 区分：`os_buffer = 0`、`fsync = 1 ms`。

**读路径索引（E8 整改）**：每分区维护内存索引 `wal_index`：

```
# 稀疏间隔索引：每 256 条记录一个锚点（锚点间隔可配）
[{StartOffset, SegPath, [{AnchorOffset, FilePos}]}]
```

- `wal_writer` 追加时同步更新当前段锚点；
- `read_from(Offset, Limit)`：索引定位 ≤Offset 的最近锚点 → 从锚点顺序扫到目标 → 流式读取（消息体不驻留索引）；
- `wal_recovery` 重放段头重建索引。
- 无索引则每次 dispatch 全段扫描，group commit 省的 fsync 被读放大吃回去。

**记录编码**：

WAL 记录头 = `<<Magic:8, Vsn:8, CRC32:32, Len:32>>`，Body = 紧凑元组 `erlang:term_to_binary/1`：

```
{Vsn, MsgId, Tenant, Destination, Payload, PayloadEncoding,
 Persistence, ProtoMeta, Headers, Reliability, CreatedAt, ExpiresAt, EventBlob}
```

**E6 整改**：元组补齐 `PayloadEncoding` 与 `Persistence` 两个槽位——`#broker_message{}` 的全部字段可无损往返，S1 的 `expires_at` 恒为 `undefined` 也照原样落盘（【F4】解码侧**不做任何派生**，避免用一个未承诺的时间戳喂给 S2 的 TTL 判定）；`persistence` 不依赖 `reliability` 反推（映射不成立：QoS0 消息同样可以声明 durable 意图）。

**关键规则**：
1. Body 元组**独立于** `#broker_message{}` 记录字段——未来 S2 加字段、S3 填 `event` 视图，只递增 `Vsn` 与增加元组槽位，旧段解码走 `case Vsn of 1 -> ...; 2 -> ...` 兼容分支。
2. `#broker_message{}` 是**内存模型**，`wal_body_tuple` 是**磁盘协议**；两者由 `wal_codec:encode/1` / `wal_codec:decode/2` 桥接。
3. 此决策取代原 OQ-1：S1 保留 Erlang external term format，通过"元组 vs 记录"解耦解决版本兼容；CBOR/protobuf 在 S6 若需跨语言读取再评估。

**旧段兼容**：`wal_recovery` 遇到 `Vsn > 当前支持最大 Vsn` → 拒绝启动并报警；`Vsn < 当前` → 走兼容分支读入，新段用最新 Vsn 写出。

### 7.3 段文件命名与保留

`data/wal/{partition_id}/segment-{StartOffset:020d}.log`。S1 **不实现物理段清理**：`truncate/2` 只设逻辑不可读边界（§3.3 冻结点 5），段文件永不删；retention 策略在 S2 补齐。

---

## 8. 崩溃恢复验证（Gate S1 硬项）

### 8.1 编排

`tools/docker/docker-compose.s1.yml`：
- `broker`：单节点，挂载 `./data:/var/lib/unified-mq`（真实卷，非 tmpfs；由 `wal_writer:init` 校验）
- `mqtt-client`：paho-mqtt Python 客户端 + mosquitto 命令行，脚本驱动
- 测试从宿主机 `docker compose kill -9 broker` 注入崩溃

### 8.2 断言清单

| # | 场景 | 断言 |
|---|---|---|
| **R1a** 消息恢复 | Producer 收 PUBACK 后 kill -9 broker | 该消息**必须**能在 WAL 中读回；不允许部分字节 |
| **R1b** 订阅集合恢复 | clean_session=false 且已 SUBSCRIBE 的客户端在 broker kill 后重连 | 订阅**集合**（filter list）**必须**从 `.subs` 文件完整恢复，无需客户端重发 SUBSCRIBE。注意：本节恢复的是集合，不是消费位置——位置按 R3 判定 |
| R2 | Producer 发送但未收到 PUBACK 时 kill -9 | 允许：消息完全丢失，或消息完整恢复并可投递；**禁止**"部分字节"或"撕裂记录被恢复" |
| R3 | Subscriber 已 PUBACK 后 kill -9 broker | (i) cursor 恢复到 `max(0, last_flushed_offset - K)`，K = `cursor_max_regress`；(ii) 该 offset 之后所有消息**必须**再次可投递；(iii) **允许** offset ≤ last_flushed_offset 的任意子集被重复投递；(iv) **禁止**"客户端已 PUBACK 且 offset > last_flushed_offset 却丢失"。**默认档（K=64/100 ms）与零回退档（`cursor_max_regress=1`、`cursor_flush_interval_ms=0`）各跑一轮**，后者断言重复集为空——证明参数语义真实（E1） |
| R4 | 消息 append 中撕裂（模拟 fsync 前 kill） | 恢复时该记录被截断，之前所有已 fsync 记录完整；不产生虚构记录 |
| R5 | QoS0 消息在 kill 后 | 允许丢失（`os_buffer` 契约） |
| R6 | 恢复耗时 | 20k 条消息重放 < 500 ms（S0.3 §1 基线 143 ms，留 3× 冗余） |
| **R7**（E3 整改，放宽） | QoS1 已发出未收到 PUBACK 时 kill -9 | in-flight 集合**允许回退**至 ≤ 最后 `.inflight` 刷盘点；断言仅两条：(i) 已投递未 ack 的消息在重连后被重发（DUP=1 或按 cursor 追赶等价）；(ii) **不发生丢失**（回退只多不少）。**不断言** in-flight 集合精确恢复——§5.3 设计上已放弃精确性 |

### 8.3 掉电模拟（不阻塞 Gate S1）

S0.3 REPORT §3 明确指出 `kill -9` 无法证明 `os_buffer` 档的真实丢失窗口。S1 沿用 kill -9；掉电模拟（dm-flakey / VM 硬复位）留待 S2 补齐；protocol-compatibility.md 与 ADR-007 已注明。

---

## 9. 任务分解与依赖

| ID | 任务 | 依赖 | 预估 | 冻结产物 |
|---|---|---|---|---|
| T0.5 | 起草 ADR-009/010/011 骨架（先出评审文本再实现）——**已完成，三份 ADR 经 F1~F6 整改后冻结** | — | 1 | ADR-009/010/011（已采纳） |
| T1 | rebar3 umbrella apps 骨架 + sys.config + vm.args + application master——**已完成**：8 app、7 顶层 supervisor、`broker_message.hrl` 与两个 behavior 落盘；`rebar3 compile`（warnings_as_errors）与 `rebar3 release` 节点启动均实测通过 | T0.5 | 1 | 目录结构（§2/§6 已按实测修正） |
| T2 | `broker_core`：`broker_message.hrl` + 公共类型 + 行为定义（仅 header/类型，无函数） | T1 | 0.5 | ADR-009 |
| T3 | `broker_router` behavior + `broker_router_hash`（含 MQTT 通配符匹配）；**读路径为调用者进程内 ETS 并发查，写路径单一写者**（§3.2 冻结点 2） | T2 | 2 | ADR-010 |
| T4 | `broker_storage` behavior + `broker_storage_memory`（含 `create/2` 与 `recover/2` 分离） | T2 | 1.5 | ADR-011 |
| **T5** | `broker_storage_wal` 重写（segment / writer / reader / recovery / group_committer / **index** / codec 兼容层）；**交付项含（E5）：多 conn_fsm 并发 append 正确性测试——批次内每个 caller 回复严格在 fsync 之后、offset 无 cross-reply、混合 sync_mode 并发不互相饥饿** | T4 | **6.5** | — |
| T6 | `broker_queue` 分区 Worker（append 串行化 + storage 生命周期 + `capability/1` 查询 API + 通知订阅） | T3, T4 | 2 | — |
| T7 | `broker_subscription`：per-sub cursor + `.subs`/`.inflight`/`.cursor` 三层持久化（§5.3）+ **SubRef 铸造**与 `register/4` 回填（§3.2 冻结点 3）+ 分发 + advance_committed 聚合 | T6 | 3 | — |
| T8 | `broker_proto_mqtt` 帧编解码（含黄金样本单测 + proper 模糊测试） | T1 | 3 | — |
| T9 | `broker_proto_mqtt` 连接 FSM + capability 缓存 + keepalive + packet_id + `mqtt_max_qos` 执行 | T8, T6, T7 | 3 | — |
| T10 | `broker_security` 桩 + 配置驱动用户表 | T1 | 0.5 | — |
| T11 | `broker_event` 桩（pass_through，接口位保留） | T2 | 0.5 | — |
| T12 | 监督树整合 + application 启动流程（§6 启动顺序 4 步：`recover` → `.subs` 重建 → `register` 回填 → 才放开 listener）+ dev 环境 | T5-T11 | 1 | — |
| T13 | common_test 集成测试套件（端到端 pub/sub/ack、QoS0/1、clean_session、离线追赶） | T12 | 2 | — |
| T14 | docker compose 崩溃恢复测试套件（R1a/R1b/R2~R7） | T13 | 2 | — |
| T15 | 真实客户端兼容性测试（paho Python、mosquitto_pub/sub） | T13 | 1.5 | protocol-compatibility.md MQTT 行 |
| T16 | `docs/architecture.md` + `docs/storage-and-recovery.md` skeleton | T2-T5 | 1.5 | 两份文档 v0.1 |
| T17 | ADR-009/010/011 定稿 + 评审会（E1 Gate 措辞已确认，会上正式签认并入 ADR-011 附录） | T16 | 0.5 | Gate S1 |

**关键路径**：T0.5 → T1 → T2 → T4 → T5 → T6 → T7 → T9 → T12 → T13 → T14 → T17。约 **27 人日**（v0.2 26 → v0.3 27，T5 +0.5 并发测试；另 T7 已含三层持久化，3 人日不变）。T8 与 T3/T4 并行；T10/T11 低耦合桩随时插入。

---

## 10. Gate S1 对齐（roadmap §3.4）

| Gate 项 | 由哪个任务证明 |
|---|---|
| 真实 MQTT 客户端完成 publish → subscribe → ack 全链路 | T15（paho + mosquitto） |
| `kill -9` 后重启，消息丢失/重复行为有测试断言且符合契约 | T14（R1a/R1b/R2~R7） |
| 三个核心抽象完成评审并冻结 | **T0.5 已冻结**（ADR-009/010/011 状态=已采纳）；T17 评审会复核冻结内容与 T2~T7 实际实现是否一致，不一致则按 ADR 修订流程处置 |
| `broker_storage` 的 WAL 与内存实现可互换 | T4 + T5 + T13（同一测试套件跑两个后端；memory 后端下 QoS1 用例按 `mqtt_max_qos=0` 契约执行） |
| **「订阅位置不丢」的解释（E1，v0.3 复审已确认）**：= 订阅集合零容忍（R1b）+ cursor 有界回退 ≤`cursor_max_regress`（R3，默认档与零回退档双档实测）+ in-flight 允许回退仅保证不丢（R7）。依据 roadmap §3.4 第 3 条「明确可容忍的重复窗口」——该条本身即授权有界重复，否则为空文 | **T17 评审会正式签认（措辞已确认，此处仅走流程）→ 写入 ADR-011 附录与 protocol-compatibility.md** |
| 在此 Gate 之前不允许实现第二个协议 | 流程 Gate：progress.md 状态机标记"S1 未通过 → 拒绝合入第二协议 PR"；CI workflow 检查 `apps/broker_proto_*` 中除 `broker_proto_mqtt` 外无新增 app |

---

## 11. 风险与开放问题

### 11.1 风险

| 风险 | 缓解 |
|---|---|
| **多写者并发 group commit 未经 S0 实测**（S0.3 §6 待办原文） | T5 并发正确性测试前置于一切性能断言；若发现延迟退化，batch_delay_ms 上调而非改架构 |
| MQTT 帧自研编解码边界情况出错 | 黄金样本用 mosquitto 抓包；`proper` 随机帧测试 |
| `.inflight` 批量回退窗口内客户端断连时序复杂 | R7 只断言"不丢、可多"；测试用 fault injection 固定断连时序 |
| Group commit 与逐条 fsync 语义冲突 | Group committer 保证批次内所有请求 fsync 完成后回复；测试覆盖并发 append（同 E5） |
| WAL 版本兼容 | §7.2 决策：Vsn 位 + 独立磁盘元组；S6 评估 CBOR 迁移成本 |
| Cursor/in-flight 100 ms 回退 → 客户端看到重复 | QoS1 at-least-once 允许；R3/R7 措辞明确 |
| 单节点分区数固定，无法在线扩缩容 | S1 非目标，S4 处理 |

### 11.2 开放问题（S1 期间需答复）

- **OQ-2**：`broker_subscription` 三个持久化文件与 S2 Mnesia 迁移的接口边界——文件层是否需要抽象成 `broker_subscription_store` behavior？S1 评审期答复。
- **OQ-4**：MQTT 通配符匹配放 Router 还是 Subscription？S1 暂定 Router；S2 引入 Shared subscription 时复议。
- **OQ-5**：`broker_event` pass_through 桩的实际契约——客户端发送 `content-type = application/cloudevents+json` 时 S1 应 (a) 完全不检查直接透传，还是 (b) 检查但仅记录不校验？S1 评审期定案。
- ~~OQ-6（session fsync 性能）~~ → **已解决（E3）**：拆 `.subs` 强一致 / `.inflight` 批量两层，不再逐条 fsync。

### 11.3 已关闭（v0.1/v0.2 遗留）

- ~~OQ-1（WAL 编码是否切 CBOR）~~ → §7.2：external term + 独立磁盘元组 + Vsn 位。
- ~~OQ-3（group commit batch_delay_ms 是否按等级区分）~~ → §7.2：L1=0、L2=1 ms。

---

## 12. 交付物清单

**代码**：`apps/*` 8 个应用；`test/integration/*`（含 T5 并发 append 测试）；`test/protocol-compatibility/mqtt/*`；`tools/docker/docker-compose.s1.yml`。

**文档**：
- `docs/design/s1-mqtt-vertical-slice.md`（本文，评审通过后升级 v1.0）
- `docs/architecture.md`（roadmap §9.2，覆盖 S1 落地后的实际结构）
- `docs/storage-and-recovery.md` skeleton（3~5 页，记录 §7/§8 当前版本）
- `docs/adr/009-broker-message-record.md`
- `docs/adr/010-broker-router-behavior.md`
- `docs/adr/011-broker-storage-behavior.md`（附录含 E1 Gate 措辞解释）
- `docs/protocol-compatibility.md` 追加 MQTT 行（含 §4.5 拒绝策略、§5.2 离线追赶、session 回退限制）
- `progress.md` 更新 S1 状态 + 第二协议门禁标记

**测试证据**：崩溃恢复 R1a/R1b/R2~R7 CI 输出；T5 并发 append 正确性测试报告；真实客户端兼容性测试报告。

---

## 13. 变更摘要

### v0.1 → v0.2（27 项：B1~B4、C1~C6、S1~S6、D1~D13，D8 并入 C4）

| 项 | v0.1 | v0.2 |
|---|---|---|
| B1 Session 不持久化 | 仅内存 | 订阅集合 fsync 持久化 |
| B2 capability 契约不清 | 模块级 | 实例级 + 超能力必须报错 |
| B3 PUBACK 时序矛盾 | 图后文先 | 拆 (a)~(f)，PUBACK 先于 dispatch |
| B4 R3 措辞漏洞 | 单句 | (i)~(iv) 四条断言 |
| C1~C6 | Router/Sub 职责、WAL 版本、R1 拆分、principal、拒绝策略、监督树补全 | 逐项落正文 |
| S1~S6 | docker 位置、T5 估工、T0.5、协议门禁、OQ-3、recovery skeleton | 逐项落正文 |
| D1~D13 | 证据标注、pass_through OQ、core 收窄、config_sup、partition_id、headers 分工、expires_at、optional hook、AckRanges、advance_committed、truncate 语义、capability 实例级 | 逐项落正文 |

### v0.2 → v0.3（12 项复审整改）

| 项 | v0.2 问题 | v0.3 修正 | 影响章节 |
|---|---|---|---|
| **E1** | §4.2 声称「订阅位置不丢」与 cursor 回退矛盾 | 逐词映射 roadmap §3.4：已 ACK 不丢=R1a 零容忍、订阅集合=R1b 零容忍、cursor=`cursor_max_regress` 有界回退、未 ACK 重投=契约行为；依据 §3.4 第 3 条「明确可容忍的重复窗口」；参数化 + R3 双档实测。**复审已确认** | §4.2、§5.3、§8.2、§10 |
| **E2** | conn_fsm 无 handle 却调 `capability(Hd)` | 双层校验：conn_fsm 启动时缓存（经 config/queue 查询）早失败 + queue/storage 运行时兜底 | §3.3、§4.2、§5.1 |
| **E3** | session 逐条 fsync 归零 group commit 收益 | 拆 `.subs`（强一致）/`.inflight`（批量）/`.cursor`（批量）三层；R7 放宽为"不丢、可多" | §5.3、§8.2、§9-T7、OQ-6 关闭 |
| **E4** | `-optional_callback` 非法语法 | 改 `-optional_callbacks` | §3.2 |
| **E5** | S0.3 §6 待办的多写者并发未消化 | T5 交付项含并发 append 正确性测试；风险表首行 | §7.2、§9、§11.1 |
| **E6** | 磁盘元组缺 persistence/payload_encoding | 元组补两个槽位，声明"全字段无损往返"规则 | §7.2 |
| **E7** | 离线积压机制未写 | §5.2 明确"无内存离线队列，cursor 滞后 + 重连追赶"，两条限制入兼容性文档 | §5.2 |
| **E8** | read_from 无索引会全段扫描 | 新增 `wal_index` 稀疏间隔索引（锚点 256 条），writer 更新 / recovery 重建 | §7.2 |
| **E9** | Will 行"QoS=1 需 fsync"自相矛盾 | 改为"Will 字段（含 QoS/payload）一律忽略，不做任何存储" | §4.5 |
| **E10** | 整改计数与摘要表不符 | 版本前言改为"27 项 + 12 项"双阶段计数 | 前言、§13 |
| **E11** | 持状态 Worker 误用 simple_one_for_one | queue/storage/subscription 改 one_for_one 静态 child；仅 conn 用 simple_one_for_one；删除 mqtt_session 进程层 | §6 |
| **E12** | memory 后端下 QoS1 测试行为未闭环 | listener config `mqtt_max_qos`，memory 后端必须配 0，SUBACK 阶段显式拒绝 | §3.3、§4.4、§7.1、§10 |

**工期**：26 → **27 人日**（T5 +0.5）。**新增交付**：T5 并发测试报告、wal_index、R7 放宽版断言。

### v0.3 → v0.4（T0.5 冻结评审 7 项：F1~F6、G1）

| 项 | v0.3 问题 | v0.4 修正 | 影响章节 |
|---|---|---|---|
| **F1** | `recover/2` 返回 `AckRanges`，但单一连续前缀模型下**无人持有**该区间集合；且返回值缺"从哪继续追加"的位置 | 移除 `AckRanges`（D10 的内存爆炸问题被结构性消除而非缓解）；`recover/2` 补 `HighOffset`；写明水位不对称规则：偏低（重复）允许、偏高（提前 truncate 丢数据）绝对禁止 | ADR-011 §11.3/§11.7、§3.3、§8.2 |
| **F2** | `open/2` 合并创建与恢复 | 拆 `create/2` + `recover/2`；`recover` 对缺失分区返回 `{not_found, _}` 且**绝不自动创建**——否则分区 ID 打错/挂载点丢失会表现为"恢复出空分区并报告健康" | ADR-011 §11.2、§3.3、§6、§9-T4 |
| **F3** | Router 回调以 `State` 线程传递，隐含单一 gen_server 串行化全部 publish，违反 README §7.2 | 回调改吃不透明 `router_env()`，不接收/返回 State；`route/3` 在调用者进程内直接查 ETS（并发读），索引写才经单一写者 | ADR-010 §10.1、§3.2、§5.1、§9-T3 |
| **F4** | `expires_at` 在 S1 派生 `transient→created_at+5min` 占位值并落进不可变 WAL | S1 两种 persistence 一律 `undefined`；解码侧不做派生。占位值会让 S2 用未承诺的时间戳误判过期，等价已 ACK 消息静默丢弃 | ADR-009 §9.3、§3.1、§7.2 |
| **F5** | Router `subscribe/3` 自行铸造 SubRef，与"Router 不得调用 subscription API"判据自相矛盾 | SubRef 由 `broker_subscription` 铸造；流程固定「创建实体 → `register/4` → `route/3` 原样返回 → `unregister/2`」，Router 从不解引用 | ADR-010 §10.1/§10.2、§3.2、§5.2、§9-T7 |
| **F6** | roadmap §3.2 的抽象动词表与冻结 API 无溯源指针；设计文档 §3 与 ADR 存在两份真相 | roadmap §3.2 加覆盖指针（批注式，不改其动词基线）；设计文档 §3.2/§3.3 标注"ADR 摘录，冲突以 ADR 为准"；补 §6 启动顺序（recover → 重建订阅 → register 回填 → 才放开 listener），使 R1b 有实现前提 | roadmap §3.2、本文前言/§3/§6 |
| **G1** | ADR-010 §10.1 把 `route_ctx` 只定义成 record，却在 `-callback route/3` 中当类型用；**OTP 28.3.1 `erlc` 实测报 `type route_ctx() undefined`，冻结文本无法编译** | 补 `-type route_ctx() :: #route_ctx{}.'` 与 `-export_type([router_env/0, sub_ref/0, route_ctx/0]).'`；三份 ADR 的 record/behavior 定义已全部 `erlc` 通过并核对 `behaviour_info(callbacks)`/`(optional_callbacks)` | ADR-010 §10.1/验证、§3.2 |

### T1 实现期间发现并修正（2026-10-21，均为实测暴露）

| 项 | 问题 | 修正 |
|---|---|---|
| **H1** | §6 画的单一 `broker_app_sup`「application master」在 8 个独立 OTP app 的结构下没有承载者：它若把别的 app 的 supervisor 当 child 起，那些 app 就不能再作为 application 启动 | 取消该层，改为每 app 一个顶层 supervisor，跨 app 顺序由 `.app.src` 的 `applications` 拓扑保证；实测 `ensure_all_started` 顺序与四个 boot step 一致 |
| **H2** | relx 用 `{sys_config_src, ...}` / `{vm_args_src, ...}` 时，rebar3 3.27 只把文件原名拷进 `releases/Vsn/`（仍带 `.src`），启动脚本找不到配置 → `bin/unified_mq daemon` 挂死、`ping` 无响应 | 改用 `{sys_config, ...}` / `{vm_args, ...}`；实测 `releases/0.1.0/{sys.config,vm.args}` 生成、`daemon`+`ping=pong`+`rpc application:get_env` 全部正确 |
| **H3** | `{extended_effects, true}` 不被该 relx 版本识别，报 `Unknown term found in relx configuration` | 移除 |
| **H4** | `vm.args` 里写的 `+C multi_scheduling` 被 erts 拒绝（`Invalid time warp mode`），节点起不来；`+h`/`+W` 亦非 OTP 28 合法开关 | 改为 `-C multi_time_warp`，删掉未经确认的开关；此后所有 vm.args 选项一律以"能否真启动"为准 |
| **H5** | `sys.config` 里预置的 sasl 文件日志指向不存在的 `log/sasl` 目录 | S1 用默认 logger，文件日志归 T12 dev 环境；`.gitignore` 补 `/data/`、`/log/` |
