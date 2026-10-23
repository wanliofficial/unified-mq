# S1 方案设计 — MQTT 垂直切片原型

- **文档版本**：v0.9（T7 实现同步版）
- **对应 Roadmap**：[`docs/roadmap.md`](../roadmap.md) §3
- **前置**：S0 已完成，ADR-001~008 已定稿；ADR-009/010/011（三抽象）**已冻结**（[`docs/adr/`](../adr/)）
- **目标 Gate**：roadmap §3.4「Gate S1」
- **状态**：设计评审收敛 + T0.5 三份 ADR 已冻结 + **T1 骨架、T3 Router、T4 内存后端、T5 WAL 后端、T6 分区 Worker、T7 订阅层已实现并实测通过**（181 个 CT 用例：既有 157 + T7 订阅 21 + Router 监督树 3，`rebar3 ct` 无一条 FAILED、`xref`/`dialyzer` 干净）；**装配后的 release 节点实测**：T6 的两半（干净目录响亮失败 / 带外建分区后 11 app 全起）复验通过，T7 的 R1b 节点级预演通过——带外建 4 分区 → `bin/unified_mq start` → `subscribe(<<"c1">>, <<"sport/#">>, 1)` → `data/sessions/YzE.subs` 落盘 → `application:stop+ensure_all_started(broker_subscription)` → route 索引仍含该 SubRef（boot step 2+3 回填走 `already_registered` 路径）、`cursor/2` 从 `.subs` 内嵌原点恢复 → 下一步 T9
- **修订记录**：v0.1 → v0.2 应用 27 项整改（B1~B4、C1~C6、S1~S6、D1~D13）；v0.2 → v0.3 应用复审整改 12 项（E1~E12）；v0.3 → v0.4 应用冻结评审 7 项（F1~F6、G1）；v0.4 → v0.5 同步 T1/T3/T4 实测发现（H1~H11），变更摘要见 §13；v0.5 → v0.6 把 T4 的 13 个契约用例参数化为 `apps/broker_storage/test/broker_storage_contract.hrl`，使 Gate「WAL 与内存实现可互换」成为机械检查（§7.1、§9 T5 行）；**v0.6 → v0.7 同步 T5 实测发现（I1~I15，见 §13）：WAL 模块划分、组提交策略模块形态、tmpfs 防护位置、E5 并发结论、以及 H8「分区 bootstrap 由谁做」的归属答复**；**v0.7 → v0.8 同步 T6 实测发现（J1~J11，见 §13）：§5.1(d) 的"分区 Worker 串行化"改为"单一 storage writer 串行化"（否则 group commit 退化成每批 1 条）、H8 的 `create_partition/1` 与 boot step 1 已落地并在 release 上实测两半、OTP 28 `get_env/2` 返回 `{ok,Val}`、relx 脚本 `cd $ROOTDIR` 使相对 `data_dir` 落进 release 树**；**v0.8 → v0.9 同步 T7 实测发现（K0~K10，见 §13）：修掉 T3 遗留的 Router 装配缺口（`broker_router_sup` 从不启动索引 store，I12 同类），§5.3 的 cursor 文件按 H11 落成 per-client 聚合的 `.cursors` 并把订阅原点内嵌进 `.subs`（恢复规则"绝不回退到 Committed/0/High 兜底"），committed 聚合公式定死为 `min(活 cursor) − 1` 且刷盘严格先于 `advance_committed`，OQ-2 答复为"不抽 behavior"**。ADR 侧只加**冻结后批注**，不动已冻结文本

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

**T6 实测补两条 relx/配置事实（2026-10-22，详见 §13 J3/J8）**：(1) 生成的 `bin/unified_mq` 在 exec 前做 `cd "$ROOTDIR"`（脚本第 770 行），所以相对 `data_dir` 落在 **release 根目录**而非调用者 CWD，`rebar3 release` 会连带清掉它——T12 的 dev 环境必须用 release 树外的绝对路径。(2) **OTP 28 的 `application:get_env/2` 返回 `{ok, Val}` 而非裸值**（`kernel-10.5/src/application.erl:918`），全项目统一用返回裸值的 `get_env/3`。

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

**T6 实测批注（2026-10-22）——(d) 的"分区 Worker 串行化"读法已修正**：上面流程图第 (d) 行的「分区 Worker 串行化（多 conn_fsm 并发投递由 mailbox 吸收）」**不成立**，实现里 `broker_queue:append/3` **不经过分区 Worker**：

- **真正的串行化点是单一的 `wal_writer` 进程**（每分区一个），并发投递由**它的** mailbox 吸收，group committer 在那里攒批。分区 Worker 若插在中间做 `gen_server:call`，每个 caller 都会独占 writer 一次往返 ⇒ **每批恒为 1 条**，吞吐退化成 S0.3 §2.1 实测的逐条 fsync ~1148 msg/s——那正是 ADR-008 决定自写 WAL 要超越的数字。
- 实现形态与 §3.2 冻结点 2（Router 不串行化）**同构**：句柄放在 `protected` ETS 注册表里，caller 在自己进程内查表后直接 `Backend:append(Hd, [Msg], Mode)`；分区 Worker 只持有**生命周期与元数据**（storage 句柄刷新、`capability` 缓存来源、订阅者集合与 `{new_messages,_,_,_}` 广播），**不在数据路径上**。
- **实测两例**（`broker_queue_SUITE`）：`the_data_path_does_not_pass_through_the_partition_worker` 把 Worker `sys:suspend` 后两次 append 仍返回 `{ok,1}`/`{ok,2}`（若走 Worker 会挂到 CT 超时）；`eight_concurrent_callers_pay_for_one_fsync` 先把 8 个 caller 停在 writer 的 mailbox 里再放行，读 writer 自己的计数器得 `#{records := 8, batches := 1, syncs := 1}`——**8 条消息付 1 次 fsync**，且因为请求已确证在 mailbox 中，该断言不依赖机器快慢。

**顺序约束**：

- **(e) PUBACK 只依赖 (d) storage append 返回**——**先于** (f) dispatch，不等待 dispatch 完成。
- (f) dispatch 失败（分区 Worker 过载、订阅者断连等）**不回滚** Producer ACK；Producer 已看到的可靠性承诺不因下游抖动而撤销。下游重试由 `broker_subscription` 层的 cursor 与重投机制负责（S2 补齐指数退避）。
- 若未来引入 `replicated_commit`，(e) 等待点变为 Raft commit 返回；behavior 契约不变。

### 5.2 Subscribe / Dispatch 路径

- SUBSCRIBE(topic_filter, qos) → `broker_subscription` **先创建订阅实体**（生成 SubRef，登记 `{SubRef, ClientPid, TopicFilter, QoS, Cursor, InflightSet}` + 持久化订阅集合，§5.3 `.subs` 文件），再调 `broker_router:register(Env, Filter, SubRef, Opts)` 把 SubRef 登记进索引（§3.2 冻结点 3【F5】：SubRef 由订阅层铸造，Router 不生成）。
- 分区 Worker 收到 `append` 成功后，向本分区上所有 SubRef 的 dispatcher 广播 `{new_messages, PartitionId, FromOffset, ToOffset}`。
- Dispatcher **内嵌在 `mqtt_conn_fsm` 内**（S1 选择：减少进程数；S2 引入独立 dispatcher 时通过 optional callback 扩展，不破坏 §3.2/§3.3）。调用 `broker_storage:read_from/3` 拉取 → 转 MQTT PUBLISH → 等客户端 PUBACK → 调 `broker_subscription:ack` → 后者聚合"全部订阅都 ack 的连续前缀" → 定期 `broker_storage:advance_committed/2`。

**T6 实测批注（2026-10-22）——"Worker 收到 append 成功"的措辞**：Worker 不是**处理** append 的一方（见 §5.1 批注），而是被 append 的调用者 `gen_server:cast` **告知**。由此广播的性质必须按下面三条读，T7 的 dispatcher 依赖它们：

1. **`{new_messages, PartitionId, From, To}` 是唤醒信号，不是投递契约**。dispatcher 收到后仍要自己 `read_from(Cursor, Limit)`，不能把通知里的区间当作待投消息集合。
2. **区间可乱序、可重叠、可丢失**：多个 caller 并发 append 时 cast 的到达顺序与 offset 顺序无关；Worker 崩溃期间发出的 cast 直接丢失。恢复路径是 dispatcher 下一次按 cursor 读——cursor 才是唯一真相源，通知只是让它早点醒。
3. **订阅者集合由 Worker 持有并 `monitor`**，进程死亡自动摘除，无需 dispatcher 显式退订（`unsubscribe/2` 仍保留且幂等，因为 MQTT UNSUBSCRIBE 与断连竞争会同时到达）。实测：`a_dead_subscriber_is_dropped_without_disturbing_appends`。

**离线积压机制（E7 整改）**：clean_session=false 订阅者离线期间**不存在 broker 侧内存离线队列**——消息持久化在 WAL 中，积压仅表现为该订阅 cursor 滞后；重连时 conn_fsm 从 cursor 起 `read_from` 追赶。两条 S1 限制写入 protocol-compatibility.md：
1. 追赶深度受 WAL 段保留约束——S2 retention 策略前，段文件不删、理论全量可追；但追赶本身是单连接顺序读，积压极深时首条消息延迟线性增长（已知行为，非丢消息）。
2. 追赶期间到达的新消息与积压消息按 offset 顺序统一投递，无优先级反转。

### 5.3 游标与订阅持久化（E3 整改：三层更新频率）

`broker_subscription` 维护三类落盘状态，均位于 `data/`：

| 文件 | 内容 | 变化频率 | fsync 策略 | 回退窗口 |
|---|---|---|---|---|
| `sessions/{clientid}.subs` | 订阅集合（filter list）**+ 每条订阅各分区的初始 cursor 原点【K5】** | 仅 SUB/UNSUB | **每次变化即 fsync** | 0（强一致）|
| `sessions/{clientid}.inflight` | packet_id 分配器 + in-flight 集合 | 每条 QoS1 投递/ack | 批量缓冲，随 group commit 节拍刷盘 | ≤64 条 / 100 ms |
| `sessions/{clientid}.cursors`【K4：per-client 聚合，取代原 `{subref}.cursor`】 | per-`(SubRef, PartitionId)` cursor（H11）| 每条消费 ack | 每 `cursor_flush_interval_ms`（默认 100）或每 `cursor_max_regress`（默认 64）先到触发 | ≤64 条 / 100 ms |

**T7 实测批注（2026-10-23）**：

- **文件名**：MQTT clientid 是任意二进制（含 `/` 合法），落盘名为 **url-safe base64 编码**后的 id；真实 id 存在文件内容里——`list_sessions/0` 以内容为准，名字只是地址。
- **K4**：cursor 文件按 **per-client 聚合**（一个文件含该 client 全部 `(SubRef, PartitionId)` 行），原子性 = 整文件 temp+fsync+rename。比 per-`(subref,partition)` 多文件少 N×M 倍写放大，回退窗口语义不变——偏离原表格的 `{subref}.cursor` 文本，登记于 §13。
- **K5**：订阅时查各分区 `HighOffset` 并把原点随 `.subs` **即时 fsync**——订阅原点是强一致的。启动恢复规则：有 `.cursors` 用其值，否则用 `.subs` 内嵌原点；**绝不用 Committed/0/High 当场兜底**（回退到订阅起点之前=违 §4.2；跳过未 ack 积压=丢消息）。
- **K6**：`broker_storage:advance_committed` 的聚合公式与次序是刚性的——cursor 语义"下一条要读"、committed 语义"最后已确认"（存储守卫 `Offset =< High-1 andalso >= Committed`）⇒ **committed = min(活 cursor) − 1**，无活 cursor 的分区跳过；且必须**先 `.cursors` 落盘、后 `advance_committed`**——反过来则崩溃后 cursor 回退到 committed 之下，S2 truncate 会丢数（§11.3"偏高绝对禁止"的镜像）。

**为什么分层刷盘**：in-flight 集合每条 QoS1 消息都变化；若"每次变化即 fsync"，等于每条消息额外一次独立 fsync（S0.3 §2.1 实测 ~871 µs），group commit 的 17× 收益直接归零。而 MQTT 3.1.1 §4.1 本就允许 session 恢复时 broker **重发未确认消息**（DUP=1），in-flight 精确性不是规范契约——允许回退只多不少。**订阅集合的强一致（Gate 核心）不受影响。**

**语义总结**：
- 订阅集合强一致 → Gate「集合不丢」（R1b）；
- cursor 与 in-flight 允许有界回退 → **至少一次投递**（R3、R7），重复是契约内行为。
- `broker_storage:advance_committed` 是**独立**的存储水位（S2 retention 与 S4 复制用），不替代 per-sub cursor 文件。

**S2 迁移路径（OQ-2）**：Mnesia 上线后三个文件全部退役迁入表（`subscription_meta`、`cursor_meta`、`session_meta`），`broker_subscription` 对外 API 不变。**→ 已答复（K3，2026-10-23）**：文件层**不抽 behavior**——三个文件是 subscription 的内部实现细节，S2 是整体替换而非并列后端，没有第二个实现者需要插拔；持久化模块命名 `broker_subscription_files`（刻意不用 `_store` 后缀，避免暗示抽象层）。

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

**T5 实测批注（2026-10-21，不改上图基线，只记两处偏差）**：

- **`wal_writer (per partition，静态)` 里的"静态"不成立**：ADR-011 §11.2 把 `create/2` 定为带外动作、`recover/2` 对缺失分区必须报错，于是 boot 时**没有任何可枚举的分区清单**能让 `init/1` 返回。writer 子进程由 `broker_storage_wal_sup:start_partition/3` 用 `supervisor:start_child/2` 动态加入（`id => {wal_writer, PartitionId}`、`restart => permanent`，符合上图的 `one_for_one`）。supervisor 的 `init/1` 返回空 child 列表——这不是遗漏，是"分区清单不在存储层"的结构后果（清单归属见下方 boot step 1 批注）。
- **`broker_storage_sup` 按配置只起自己需要的那一个子进程**（`one_for_all` 不变）：`backend = broker_storage_wal` → 起 `broker_storage_wal_sup`；`backend = broker_storage_memory` → 起 `broker_storage_memory_store`。**这修掉了 T4 的一个真实缺陷**：内存后端此前从未有任何角色启动 `broker_storage_memory_store`，于是 release 里每一次 `broker_storage_memory:create/2` 都是 `noproc`——T4 的 CT 套件因为在自己进程里手工起表而全绿，缺陷只在装配后的 release 中出现。现由 `apps/broker_storage/test/broker_storage_sup_SUITE.erl` 锁住："每个后端启动它的 `create/2` 所需的那个进程"。

**T6 实测批注（2026-10-22，上图 `broker_queue` 那两行成立，补一个未画出的子进程）**：`broker_queue_sup` 的 `init/1` 实际返回 **5 个** child（N=4 时）：`broker_queue_store`（注册表的单写者，**必须排在分区 worker 之前**，否则 worker 的 `init/1` 写句柄时表还不存在）+ 4 个 `broker_queue_partition`（`id => {partition, PartitionId}`、`restart => permanent`）。清单是静态的，因此 I14 实测到的"supervisor `init` 期间子进程失败即整体失败、不进 intensity 重试"正好把 boot step 1 变成全有或全无——任一分区 `recover/2` 得 `{not_found,_}`，整个 application 起不来（release 实测见下方 H8 批注）。

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

**T5 批注 — H8（分区 bootstrap 由谁做）已答复，2026-10-21**：

- **step 1 的执行者不是 `broker_storage_sup`**（上面括号里那串 supervisor 名要按此读）：分区清单 `{partitions, N}` 配在 `broker_queue` 的 application env（见 `config/sys.config.src`），存储层看不到它，所以"对各分区 `recover/2`"这个循环只能落在 **`broker_queue`（T6 交付）**。`broker_storage_sup` 只负责把进程结构立起来（见本节上一批注），不执行 recover。
- **`create/2` 的唯一调用者是带外管理动作**：S1 = 显式调用 `broker_queue:create_partition/1`（T6 交付），**不出现在任何 boot 路径里**。于是"干净数据目录起 broker"会拿到 `{not_found, _}` → application 启动失败并告警。这是 §11.2 要求的**响亮失败**，不是待修的启动 bug，也不得用"recover 顺手创建"或"首启标记"绕开——后者正是 §11.2 要防的"恢复出空分区并报告健康"。
- 推论：**首启与恢复不需要靠数据目录内容来区分**，区别只在于有没有人显式 `create` 过。dev/CI 的起法是先建分区再 boot；S1 不提供 `partition_bootstrap` 之类的启动期自动建分区开关。

**T6 批注 — 上面两条已落地，并在装配后的 release 上实测两半，2026-10-22**：

- **step 1 的实现形态**：`broker_queue_sup:init/1` 返回 `[store_spec() | [partition_spec(Id) || Id <- broker_queue:partition_ids()]]`——**静态子进程清单**，每个分区一个 `broker_queue_partition`，其 `init/1` 只做 `Backend:recover(PartitionId, Config)` 并把句柄写进注册表；`{error,{not_found,Id}}` → `{stop, {partition_not_found, Id}}`。清单是静态的（不是 `start_child`），因此 I14 实测到的"supervisor `init` 期间子进程失败会立刻整体失败、不进入 intensity 重试"正好把 boot step 1 变成**全有或全无**：任一分区缺失，整个 application 起不来。
- **release 实测（前半）**：`rebar3 release` 后在干净目录 `bin/unified_mq console` → 退出码 1，crash report 里是 `{{shutdown,{failed_to_start_child,{partition,0},{partition_not_found,0}}},{broker_queue_app,start,[normal,[]]}}`，且 **`data/` 一个字节都没被创建**（CT 侧同名断言 `boot_fails_loudly_when_a_partition_was_never_created` 锁死"失败且不留痕"）。
- **release 实测（后半）**：带外建好 4 个分区后 `bin/unified_mq start` → `ping = pong`，rpc 查到 11 个 application 全部运行、`broker_queue_sup` 有 4 个 `{partition,N}` + 1 个 `broker_queue_store`、`broker_storage_wal_sup` 有 4 个 `{wal_writer,N}`、`broker_queue:capability(0) = [os_buffer,fsync]`。
- **带外建分区的两个实测前提**（写进 §13 J8/J9，运维脚本必须照做）：
  1. `create_partition/1` 需要 `broker_queue` **已 load**（不必 start）——分区清单在它的 env 里，未 load 时 `partition_ids/0` 抛 `{invalid_partition_count, undefined}`。实测可用的起法：`erl -noshell -pa <rel>/lib/*/ebin -config <rel>/releases/Vsn/sys.config -eval 'application:load(broker_queue), application:ensure_all_started(broker_storage), ...'`。
  2. 该 `erl` 的 **CWD 必须与节点启动后的 CWD 一致**：relx 生成的 `bin/unified_mq` 在 exec 前做 `cd "$ROOTDIR"`，所以 `data_dir = "data"` 实际落在 **release 根目录**，不是调用者所在目录。实测在别处建好的分区，节点起来后一律 `{not_found,_}`。T12 的 dev 环境必须改用 release 树之外的绝对路径。
- **`create_partition/1` 的边界**：只接受 `partition_ids/0` 里的 id（`9` → `{error,{unknown_partition,9}}`），因此不会在"永远没有 worker 去看"的位置留下孤儿目录；对已存在分区返回 `{error,{already_exists,Id}}`（ADR-011 §11.2）。

**T7 实测批注（2026-10-23，boot step 2+3 落地，并修掉一个 T3 遗留的装配缺口）**：

- **K0 — Router 索引 store 在装配后的节点里没人启动（I12 同类缺陷）**：上图 `broker_router_sup └── broker_router_hash（T3）` 那行**不成立**——T3 的 `broker_router_hash:init/1` 会自起一个 `broker_router_hash_store`，但 `broker_router_sup:init/1` 返回空 children，于是带外起的 release 节点里 `register/4` 无处可写；T3 的 CT 全绿是因为套件在自己进程里手工 `?HASH:init/1`。现 `broker_router_sup` 按 `backend` env 起 `broker_router_hash_store`（permanent），由新增的 `broker_router_sup_SUITE`（3 例）钉住"sup 之下有 store、`broker_router:env/0` 可达"。
- **装配期访问器在 behavior 之外**：消费者的 boot 需要的是 sup 已拥有的那个 store 的 env，而 `init/1` 会另起进程（撞注册名或泄漏孤儿）——故 `broker_router_hash` 加 **`current_env/0`**（从注册名拼装 env，不启动任何东西，store 不在则 `{error, router_store_not_started}`），`broker_router` 加 **`env/0` / `register/3` / `unregister/1`** facade。**ADR-010 冻结签名一字未动**：这是对"冻结签名没给装配期访问留位置"这类 I12 型缺口的实现期补足。
- **step 2+3 的实现形态**：落在 `broker_subscription_worker:init/1`（`broker_subscription_sup` 的唯一 child，permanent）——读 `sessions/*.subs`（权威）合并 `.cursors`/`.inflight`，重建实体与 cursor 行进 ETS，再逐条 `broker_router:register(Filter, SubRef, #{})` 回填；`{error,{already_registered,SubRef}}` 视为 ok（K8：崩溃窗口里 router 可能残留同一 SubRef）。任何一步失败 → `{stop, {subscription_boot_failed, Reason}}` → application 启动失败，不留半状态可服务（仿 T6 的全有或全无，release 实测见页首状态行）。
- **K7 — 单写者 + 无锁读**：worker 持 `entities/cursors/attachments/inflight` 四张 `protected` ETS（`read_concurrency`），写（subscribe/ack/attach/…）经 `gen_server:call`，读（`cursor/2`、`subscriptions/1`、`session/1`、`dispatch/2`）在调用者进程内直查——与 J1/J4 给 queue 层定的形态同构；实测挂起 worker 后读路径照常返回。

---

## 7. 存储层设计

### 7.1 内存后端（`broker_storage_memory`）

ETS 表 `messages` = `{PartitionId, Offset, Msg}`；`append/3` 只支持 `os_buffer`，caller 传其他 sync_mode 返回 `{error, {unsupported_sync_mode, _}}`；offset 单调递增。`capability/1` → `[os_buffer]`。部署此后端时 listener 必须配 `mqtt_max_qos = 0`（§3.3、§4.4）。

**T4 实测补充**（`apps/broker_storage/src/broker_storage_memory*.erl`，2026-10-21）：

- 分区注册表由 `broker_storage_memory_store` 持有，但**数据面进程内直跑**：meta 表用 `ets:update_counter/3` 做**块状 offset 原子分配**（一次 append N 条拿连续 N 个），messages 表直接 `ets:insert`，因此 `append/3` 与 `read_from/3` 都不经过任何 gen_server（README §7.2）。只有 create/recover/close/advance_committed/truncate 走写者进程。
- 代价：两张表必须是 `public`，于是并发读可能看见"offset 已分配、消息尚未插入"的瞬时空洞并把该页提前截断。S1 真实链路里 §5.1(d) 已按分区串行化 append，不会触发；**多写者并发下的这个观察窗口是 T5 并发测试必须对 WAL 明确结论的场景之一**。
- offset 基数在 ADR-011 中未冻结，本实现定为 **1 起步**：空分区 `recover` 返回 `CommittedOffset = 0`（什么都没有确认过）、`HighOffset = 1`（下一条写入位置）。
- 在冻结签名内加了两道守卫（error 项是 `term()`，不构成 API 变更）：`advance_committed/2` 拒绝越过 `HighOffset` 与向后退；`truncate/2` 拒绝把边界推到 `Committed + 1` 之后。§11.3 的"偏高绝对禁止"由此从文字变成可机械检查。
- **契约套件已参数化**（2026-10-21）：13 个用例与全部断言搬到 `apps/broker_storage/test/broker_storage_contract.hrl`，后端套件只提供 `?BACKEND`/`?CAPABILITY` 两个宏与 `backend_setup/1`、`backend_cleanup/1`、`backend_config/1` 三个 hook。头文件里不出现任何后端名、表名或目录名（grep 可验）；漏写 hook 是 `function backend_config/1 undefined` 编译告警，`warnings_as_errors` 直接让构建失败——已实测。于是 Gate「WAL 与内存实现可互换」的检验方式是同一组 13 个用例在两个后端各跑一遍，而不是一句设计意图。

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

**T5 实测批注（2026-10-21，逐行对照上表的"S1 修正"列）**：

- **模块划分与表格第一行不同**（表为计划，以下为落地）：`wal_codec` / `wal_segment` / `wal_index` / `wal_meta` / `wal_recovery` / `wal_writer` / `wal_group_committer` 七个模块 + `broker_storage_wal`（behavior 适配层）+ `broker_storage_wal_sup`。**没有 `wal_reader`**：读路径是 `broker_storage_wal:read_from/3` 在**调用者进程内**直接 `wal_index:locate/2` 定位锚点、再 `wal_segment:fold/5` 流式扫描，独立成模块反而会迫使它跨进程传句柄（README §7.2 数据面不经进程）。**新增 `wal_meta`** 是 §11.3 的要求：`recover/2` 必须返回 `CommittedOffset`，而水位若只能从段文件重放推出，恢复代价就与日志长度成正比——`meta.log` 让"读水位"成为一次定长读。
- **`wal_group_committer` 不是 gen_server**（表格第二行的"gen_server：mailbox 收集 append 请求"未落地成独立进程）：它是一个**纯策略模块**（`new/1`、`deadline/3`、`due/4`、`timeout/2`、`needs_sync/1`、`take/2`、`earliest/1`，无状态、无进程），mailbox 收集与批次触发都在 `wal_writer` 的 `handle_call` 里用"延迟回复"完成（`{noreply, ...}` 攒批 → fsync 后逐个 `gen_server:reply`）。理由：批次的所有权必须与 offset 分配、段轮转、水位推进在同一个进程里，否则 fsync 与 offset 分配之间又多一处竞态；而策略独立成模块才能被 `wal_group_committer_SUITE`（11 例）单测，不必起进程。
- **E5（多写者并发）已实测通过**，见 §9 T5 行与 §11.1 首行风险：16 个并发 caller 交叉 `fsync`/`os_buffer` 各 2 条 → 32 个 offset 恰为 `1..32` 无洞无重、`NextOffset = 33`，且**每个 caller 只在自己的 offset 上读到自己那条 payload**（无 cross-reply），每个回复都在该批 fsync 完成之后。
- **tmpfs 防护的落点与判据都改了**（表格第五行）：落在 `wal_writer:start_link/3` 而**不是** `init/1`——因为 `create/2` 不能先起一个注定要立刻崩的子进程再去把它拆掉（那会烧掉 supervisor 的 `intensity` 预算并留下 `already_present` 的 spec）。判据是读 `/proc/mounts` **按 fstype 匹配** `tmpfs`/`ramfs`，而非 S0.3 §4 的"100 µs fsync 计时"启发式：计时法在快的 SSD 上会误判正常盘为易失，而误判的代价是节点起不来。可通过 `require_durable_fs => false` 关闭（CI/测试用），关闭时返回警告而不是错误。

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

**T5 实测批注（索引的实际形态）**：`wal_index` 是一张 **`public` 的 `ordered_set` ETS 表**，由 writer 进程持有、读者在**自己进程内**查（这正是 §3.2 冻结点 2 在存储层的同一手法）；键只有两种：`{meta}`（open/closed 标志 + `truncate_before` 逻辑边界）与 `{anchor, Offset}`（值为 `{SegPath, FilePos}`，锚点自带段路径，所以跨段续读是一次 `locate/2` 而不是一次分区级重扫）。上面伪代码里的 `[{StartOffset, SegPath, [{AnchorOffset, FilePos}]}]` 三段嵌套结构未采用——ETS 的有序键已经提供了"≤Offset 的最近锚点"这个能力，再套一层段分组只会让 `locate/2` 变成两次查找。

**`HighOffset` 刻意不进索引**：§7.1 第 2 条记的内存后端缺陷（并发读看见"offset 已分配、消息尚未插入"的瞬时空洞）根源就是把"下一个 offset"暴露给了读路径。WAL 侧 `high` 只由 writer 进程的状态持有，`read_from/3` 从段文件实际扫到什么就是什么，**读到 torn tail 与读到日志末尾不可区分，也不需要区分**（前者是"writer 正跑在前面"，正常现象）。这一条是 T5 对 §7.1 遗留观察窗口的明确结论：**WAL 后端不存在该窗口**。

**句柄会因 writer 崩溃而失效**：索引表随 writer 进程消亡，此后 `read_from/3` 抛 `badarg` 而非返回 `{error, _}`——与内存后端同性质（§7.1）。调用方（T6 `broker_queue`）在 writer 重启后必须重新 `recover/2` 取新句柄，不得缓存 pid/句柄跨重启使用。

**T6 批注 — 上面这条已按要求落地，2026-10-22**：`broker_queue` 的三个数据面 API 都把 `Backend:*(Hd, ...)` 包在 `try ... catch Class:Reason ->` 里，捕获到就向分区 Worker 发一次 `reattach`（Worker 重新 `recover/2` 并覆盖注册表里的句柄），然后把 `{error, {stale_handle, PartitionId, Class, Reason, Repair}}` 交给调用者。**不做自动重试**：一个已经抵达 writer 的 append 可能已经消耗了 offset，静默重试会在发布者毫不知情的情况下制造重复消息——重试与否是发布语义的决定，归 caller（QoS1 的重发有 packet_id 去重兜底，S1 的 `broker_queue` 不代劳）。实测 `a_crashed_storage_writer_is_reattached_and_the_log_continues`：`kill -9` writer 后第一次 append 报 `{error,{stale_handle,...}}`，第二次直接 `{ok,4}`，整页 `[1,2,3,4]` 完好、末条 payload 正确。Worker 侧的 `reattach` 失败时**保留旧 state 不停机**——否则一次磁盘抖动会变成 worker 重启风暴。

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

**T5 实测批注（写路径的三条落地规则）**：

- **段轮转发生在 writer 内部，且可以在一个批次的中途**：`segment_max_records` 是每段的记录数上限，一个 32 条的批完全可能跨两段写。因此**没有 `wal_segment:rotate/N`**——轮转是 writer 的状态迁移（关旧段 → 以新的 `StartOffset` 命名开新段 → 把新段首记录钉成锚点），`wal_segment` 只提供无状态的文件操作（`path/2`、`list/1`、`open_append/1`、`append/2`、`sync/1`、`close/1`、`truncate_at/2`、`fold/5,6`）。
- **编码在调用者进程执行**：`wal_writer:append/3` 先把 `[#broker_message{}]` 逐条 `wal_codec:encode/1` 成二进制，**再**进 writer。反过来的话，CPU 密集的 `term_to_binary` 全落在单一 writer 进程里，group commit 攒的批越大、该进程越成为吞吐瓶颈——批处理要省的是 fsync 次数，不是把 CPU 串行化。
- **写失败或 fsync 失败是致命错误，不重试**：offset 是"先分配后落盘"发出去的，一次失败后继续服务就等于把已承诺的 offset 变成永久空洞（§11.4 禁止静默降级）。writer 于是 `{stop, Reason, ...}` 自杀，批内所有 caller 收到 `{error, {writer_stopped, Reason}}`，supervisor 重启后从磁盘重建状态。`broker_storage_wal_SUITE` 里的 `a_crashed_writer_is_restarted_and_the_log_survives` 用 `kill -9` 实测了这条路径（重启后 `recover` 得 `{0, 4}`，三条 payload 完好）。

**实测规模**：`wal_codec` 15 / `wal_segment` 13 / `wal_index` 8 / `wal_meta` 9 / `wal_recovery` 19 / `wal_writer` 19 / `wal_group_committer` 11 例（WAL 内部共 94 例），加 `broker_storage_wal_SUITE` 19 例（13 契约 + 6 后端专有）。

### 7.3 段文件命名与保留

`data/wal/{partition_id}/segment-{StartOffset:020d}.log`。S1 **不实现物理段清理**：`truncate/2` 只设逻辑不可读边界（§3.3 冻结点 5），段文件永不删；retention 策略在 S2 补齐。

**T5 实测批注**：每个分区目录内除段文件外还有 **`meta.log`**（`wal_meta`，定长水位记录，见 §7.2）。20 位零填充不是排版偏好而是正确性要求：`wal_segment:list/1` 按**文件名字典序**排序，只有等宽填充才能让字典序等于 offset 数值序，否则 `segment-10.log` 会排在 `segment-2.log` 之前，恢复时"最后一段"就选错了。路径布局由 `broker_storage_wal_SUITE` 的 `the_partition_layout_is_the_frozen_path_from_design_7_3` **独立重算**并断言（用例里重新拼 `data_dir/wal/{id}`，不调用生产代码的拼路径函数），所以将来改布局会红。

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
| T3 | `broker_router` behavior + `broker_router_hash`（含 MQTT 通配符匹配）；**读路径为调用者进程内 ETS 并发查，写路径单一写者**（§3.2 冻结点 2）——**已完成**：14 个 CT 用例通过（§4.7 通配符黄金用例 + 挂起写者后 `route/3` 仍返回正确结果，实测证明读路径不经进程） | T2 | 2 | ADR-010 |
| T4 | `broker_storage` behavior + `broker_storage_memory`（含 `create/2` 与 `recover/2` 分离）——**已完成**：13 个 CT 用例通过（`recover` 绝不建分区、超能力 sync_mode 报错且不消耗 offset、水位不可偏高、truncate 只设逻辑边界）；用例已抽为可复用契约头 `broker_storage_contract.hrl`，T5 的 WAL 套件按同一 hook 接口消费 | T2 | 1.5 | ADR-011 |
| **T5** | `broker_storage_wal` 重写（segment / writer / reader / recovery / group_committer / **index** / codec 兼容层）；**交付项含（E5）：多 conn_fsm 并发 append 正确性测试——批次内每个 caller 回复严格在 fsync 之后、offset 无 cross-reply、混合 sync_mode 并发不互相饥饿**；**WAL 套件必须先通过 `broker_storage_contract.hrl` 的 13 个契约用例（`?CAPABILITY = [os_buffer, fsync]`）**——**已完成**：113 个 CT 用例通过（WAL 后端 19 = 13 契约 + 6 后端专有；内部 94 = codec 15 / segment 13 / index 8 / meta 9 / recovery 19 / writer 19 / group_committer 11）。13 个契约断言**一条未改、一条未跳**地在 WAL 后端跑通，Gate「WAL 与内存实现可互换」由机械检查证明；E5 三项断言全部实测（16 caller 混合 `fsync`/`os_buffer` 并发 → offset 恰为 `1..32`、无 cross-reply、回复严格在 fsync 后）；`kill -9` writer 后重启 recover 日志完好；跨段 `read_from` 已覆盖。**H8 已答复**（§6 批注）：`create/2` 只归带外管理动作，boot step 1 的 recover 循环归 T6 `broker_queue`。**模块划分与本行计划名有偏差**（无 `wal_reader`，新增 `wal_codec`/`wal_meta`，group_committer 为纯策略模块）——详见 §7.2 批注 | T4 | **6.5** | — |
| T6 | `broker_queue` 分区 Worker（append 串行化 + storage 生命周期 + `capability/1` 查询 API + 通知订阅）——**T5 追加两项交付（H8/§6 批注）**：(a) `broker_queue:create_partition/1` 作为 `create/2` 的**唯一**带外入口；(b) boot step 1 的"逐分区 `recover/2`、缺分区即启动失败"循环落在 `broker_queue_sup`（分区清单 `{partitions, N}` 在它的 env 里，存储层看不到）。另需消化 §7.2 批注的"句柄随 writer 崩溃失效"：worker 必须在 writer 重启后重新 `recover/2`<br>**→ 已完成（2026-10-22）**：4 个模块（`broker_queue` 公共 API / `broker_queue_store` protected 注册表单写者 / `broker_queue_partition` 每分区 worker / `broker_queue_sup` 静态子进程清单）+ 16 个 CT 用例，`rebar3 ct` 全套 **157/157** 无一条 FAILED、`dialyzer` 干净、`xref` 仅剩预期的 unused-export。(a)(b) 均落地并在**装配后的 release 上实测两半**（§6 T6 批注）；I15 的句柄失效按"不自动重试 + `reattach`"消化（§7.2 T6 批注）。**与本行计划名有偏差**：`append` **不经** Worker 串行化，串行化点是单一 `wal_writer`——否则 group commit 退化为每批 1 条（§5.1 T6 批注 J1，含 8 caller → `batches=1, syncs=1` 的实测）| T3, T4, **T5** | 2 | — |
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
| **多写者并发 group commit 未经 S0 实测**（S0.3 §6 待办原文）<br>**→ T5 已消除（2026-10-21 实测）**：16 caller 混合 sync_mode 并发下 offset 恰为 `1..32`、无 cross-reply、每个回复严格在本批 fsync 之后；未发现需要上调 `batch_delay_ms` 的延迟退化。证据：`broker_storage_wal_SUITE:mixed_sync_modes_concurrently_yield_dense_offsets_and_readable_records`<br>**→ T6 在 queue 边界复测（2026-10-22）**：32 个 caller 全部经 `broker_queue:append/3`（而非直调后端）并发写入，offset 恰为 `1..32`、`read_from` 整页完整、`NextOffset = 33`。证据：`broker_queue_SUITE:concurrent_appends_through_the_queue_yield_dense_offsets` | T5 并发正确性测试前置于一切性能断言；若发现延迟退化，batch_delay_ms 上调而非改架构 |
| MQTT 帧自研编解码边界情况出错 | 黄金样本用 mosquitto 抓包；`proper` 随机帧测试 |
| `.inflight` 批量回退窗口内客户端断连时序复杂 | R7 只断言"不丢、可多"；测试用 fault injection 固定断连时序 |
| Group commit 与逐条 fsync 语义冲突<br>**→ T5 已消除**：批次内所有请求在同一 fsync 完成后才回复，混合 `os_buffer`/`fsync` 并发互不饥饿（同上用例） | Group committer 保证批次内所有请求 fsync 完成后回复；测试覆盖并发 append（同 E5） |
| WAL 版本兼容 | §7.2 决策：Vsn 位 + 独立磁盘元组；S6 评估 CBOR 迁移成本 |
| Cursor/in-flight 100 ms 回退 → 客户端看到重复 | QoS1 at-least-once 允许；R3/R7 措辞明确 |
| 单节点分区数固定，无法在线扩缩容 | S1 非目标，S4 处理 |

### 11.2 开放问题（S1 期间需答复）

- ~~**OQ-2**：`broker_subscription` 三个持久化文件与 S2 Mnesia 迁移的接口边界——文件层是否需要抽象成 `broker_subscription_store` behavior？~~ → **已答复（K3，T7 实现期 2026-10-23）**：不抽 behavior；见 §5.3 迁移路径批注。
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

### T7 实现期间发现并修正（2026-10-23，均为实测暴露）

冻结 API（ADR-009/010/011）全程未改。K0 是 **T3 遗留的真实装配缺陷**（I12 同类，只在装配后的节点里暴露），K1 是冻结 behavior 没给水位查询留位置的非 behavior 补足，K2~K10 是 §5.2/§5.3 计划文本第一次被真实实现时的偏差——其中 K4/K5 偏离 §5.3 表格文本（已在该表就地批注），K6 把"committed 只允许偏低"（§11.3）从口号落成公式与次序。

| 项 | 问题 | 处置 | 影响 |
|---|---|---|---|
| **K0** | `broker_router_sup:init/1` 返回空 children：T3 的索引 store 只在 CT 套件自己进程里由 `?HASH:init/1` 手工带起，**装配后的节点里根本没有路由索引进程**——boot step 3 的 `register/4` 无处可写，与 I12（内存 store 无人启动）同构 | `broker_router_sup` 按 `backend` env 起 `broker_router_hash_store`（permanent）；新增装配期访问器 `broker_router_hash:current_env/0`（不启动进程，store 不在则 `{error, router_store_not_started}`）与 facade `broker_router:env/0`/`register/3`/`unregister/1`；`broker_router_sup_SUITE` 3 例 + 订阅套件第 19 例钉住。**测试钩子须成对导出**（J10 教训沿用） | §6、`broker_router`、§9-T7 |
| **K1** | 订阅层需要每分区 `(Committed, High)`：新订阅锚定原点（K5）、聚合守卫自检。冻结的 `broker_storage` behavior **没有水位查询**，而 `recover/2` 名义上能拿到但有 reopen 副作用（关掉再重开 writer） | 两后端加**非 behavior 扩展** `watermarks(Hd) -> {ok, Committed, High}`：WAL 读 `wal_writer` 进程状态，内存读 meta 表（`High = Last+1`）；经注册表由 `broker_queue:watermarks/1` 暴露（`stale` 捕获与 append 同路径）。分区清单一律取 `broker_queue:partition_ids()`，不读 router 里重复的 `{partitions}` env | §3.3（文本未动）、§7.1、§7.2 |
| **K2** | ADR-010【F5】只说"SubRef 由订阅层铸造、Router 不解引用"，形状未定 | **`{ClientId, Filter}`**：铸造确定性 ⇒ boot step 2 读 `.subs` 可精确重建；元组自带反向索引 ⇒ H7 合并无需额外查表；**重订阅不碰 router**（同值条目本就在），只更新实体 QoS + 重写 `.subs`（MQTT §4.8 覆盖语义） | §3.2、§5.2 |
| **K3** | **OQ-2 答复**：三个持久化文件要不要抽象成 `broker_subscription_store` behavior | **不抽**。文件层是 subscription 的内部实现细节，S2 换 Mnesia 是整体替换而非并列后端，没有第二个实现者需要插拔；模块命名 `broker_subscription_files`（刻意避开 `_store` 后缀，不暗示抽象层） | §5.3、§11.2 |
| **K4** | §5.3 表格写 `subscriptions/cursors/{subref}.cursor`，H11 之后变成 per-`(SubRef, PartitionId)` 一族文件——一个 client 十几个订阅 × 4 分区就是几十个文件，各自 temp+fsync+rename | **per-client 聚合**成单文件 `sessions/{clientid}.cursors`（一次 rename 原子覆盖全部行），文件名用 url-safe base64(clientid)、真实 id 在文件内容里（clientid 是任意二进制，`/` 合法）；写放大降 N×M 倍，回退窗口语义不变 | §5.3 |
| **K5** | cursor 恢复时的兜底值含糊：`.cursors` 缺失（首启/被清）时用什么填？用 `Committed` 会把已 ack 的窗口重放成"未读"、用 `0` 会回放整个分区（违 §4.2 的"订阅后才有消息"）、用 `High` 会跳过未 ack 积压 | **订阅原点内嵌进 `.subs` 并即时 fsync**（订阅那一刻查全分区 High，强一致）；恢复规则唯一：有 `.cursors` 用其值，否则用 `.subs` 内嵌原点，**不做任何形式的当场兜底** | §5.3 |
| **K6** | "聚合出的 committed 只允许偏低"没有公式与次序；且聚合与刷盘的先后从未写明——乱序时崩溃后 cursor 可能落在 committed 之下，S2 truncate 恰好丢掉那段"用户还没 ack"的数据（§11.3 禁的"偏高"换了个入口进来） | 公式钉死 **committed = min(活 cursor) − 1**（无活 cursor 的分区跳过；守卫 `Offset =< High-1 andalso >= Committed` 反推所得），且**先 `.cursors` 落盘、后 `advance_committed`**——`cursor_tick` 与阈值触发两条路径同序；`advance_committed` 失败不回滚 `last_committed`，下个 tick 重试（幂等：存储守卫拒绝后退）。套件用"水位与文件内容同刻观察"锁死次序 | §5.3、§8.2 R3 |
| **K7** | 订阅层的进程形态未定：独立 dispatcher？每 client 一进程？ | **单 `broker_subscription_worker` 单写者** + 四张 `protected` ETS（entities/cursors/attachments/inflight，`read_concurrency`），仿 J1/J4：写经 `gen_server:call`，读（`cursor/2`、`subscriptions/1`、`session/1`、`dispatch/2`）在调用者进程无锁直查。实测挂起 worker 后读照常（同 T3 的证法） | §5.2、§6 |
| **K8** | 崩溃窗口：`.subs` 已写而 `unregister` 未达 ⇒ router 索引多一条死 SubRef；boot 回填会撞残留条目 | 写序统一成"**宁可索引多、不可索引缺**"（subscribe 先 register 后写盘，失败回滚 unregister；unsubscribe 先写盘后 unregister，残留无害）；`dispatch` 解引用**自己的实体表**跳过未知 SubRef（publisher 不为别层的记账缺口买单）；boot 对 `{already_registered, SubRef}` 视为 ok | §5.1、§5.2 |
| **K9** | §5.1(f) 写"dispatch 推送给订阅者"，但 QoS1 的真实消费是 T9 的 pull 路径（按 cursor `read_from`），推什么？ | **重释为**：QoS0 = 真推送（不进存储，无 offset 可拉）；QoS1 = **冗余唤醒**（按 J6 合法——信号可丢可重，cursor 是唯一真相）；H7 合并（同 client 重叠 filter 恰好一次、取 max QoS）在此做，投递形状 `{broker_deliver, ClientId, MaxQoS, [SubRef], {PartitionId, Offset, Msg}}`；in-flight 记账归 pull 路径（T9） | §5.1、§9-T9 |
| **K10** | attach 与断连竞态：旧连接的迟到 `detach` 会不会摘掉新连接（MQTT 同 Session 接管） | `detach(ClientId, Pid)` **带 pid 守卫**；attach 时 `monitor`，DOWN 自动摘除；`Persist=false`（clean session）断连即拆实体 + router + 文件，**拆序 disk-first**（先删文件后动内存，崩溃后不复活出"内存没有、盘上有"的幽灵会话） | §4.2、§5.2 |

**补充实现期观察（不构成设计偏差，但会重复用到）**：

- `binary_to_term/2` 的第二参数是**选项列表**——裸 `safe` 抛 `badarg`；files 模块的 catch-all 把它变成假的 `{corrupt_session, Path}`，"编程错误伪装成数据损坏"，在 release 级重启冒烟里才现形（字节本身一直是完好的）。跨节点/跨版本解码统一写 `binary_to_term(Bin, [safe])`。
- `application:start/1` 成功返回**裸 `ok`**（不是 `{ok, _}`）；一次失败的 start 之后，ac 短暂处于 settle 窗口、对下一次 start 回 `{error, {already_started, _}}`——测试里"失败后重启"需要按 worker 是否存活做 settle 重试（`start_after_settle` 助手）。
- **release 节点探针的两条实操**：relx CLI `rpc` 的 `Args` 参数按 `erlang:apply/3` 列表文本解析（单个原子也要写成 `[atom]`，裸写会得到 badarg）；且 OTP 28 的运行中节点经 `rpc:call` 远程 apply `code`/`erlang` 系函数会得到 `undef`（`code:add_pa/1`、`erlang:system/1` 实测如此，应用模块不受影响）——验证脚本因此写成"对已加载 app 模块的多次 rpc + 数据往返"（例：`broker_router:env/0` 的 Env map 传回 `route/3`，pid/tid 天然可跨节点）。

### T6 实现期间发现并修正（2026-10-22，均为实测暴露）

冻结 API（ADR-009/010/011）全程未改。下列 11 项里 **J1 是设计文本的读法修正**（§5.1(d) 的串行化点写错了进程），**J2/J3 是真实缺陷**（一个是配置形状没人能读，一个是 OTP 28 的返回值形状），**J10 是 T5 遗留的真实缺陷**（被 CT 的汇总数字掩盖），J8/J9/J11 是 release 级运维路径的实测前提。

| 项 | 问题 | 处置 | 影响 |
|---|---|---|---|
| **J1** | §5.1(d) 写"分区 Worker 串行化（多 conn_fsm 并发投递由 mailbox 吸收）"。若照字面把 append 做成对 Worker 的 `gen_server:call`，每个 caller 都独占 writer 一次往返 ⇒ **每批恒为 1 条**，吞吐退回 S0.3 §2.1 的逐条 fsync ~1148 msg/s，即 ADR-008 自写 WAL 要超越的那个数字 | 串行化点改为**单一的 `wal_writer` 进程**（每分区一个），并发由**它的** mailbox 吸收；`broker_queue:append/3` 在**调用者进程内**查 `protected` ETS 注册表拿句柄后直调 `Backend:append/3`，Worker 只管生命周期/元数据/广播，**不在数据路径上**——与 §3.2 冻结点 2（Router 不串行化）同构。实测两例：`sys:suspend(Worker)` 后 append 仍返回 `{ok,1}`/`{ok,2}`；8 个 caller 先停进 writer mailbox 再放行 ⇒ `#{records := 8, batches := 1, syncs := 1}` | §5.1、§9-T6 |
| **J2** | `config/sys.config.src` 里 `{wal, [...]}` 是 **proplist**，而 `wal_writer`/`wal_group_committer` 全部用 `maps:get`/`is_map` 读它 ⇒ 第一条 append 就 `badarg`。T5 没暴露是因为 CT 自己构造 Config，**T6 是第一个把 application env 直接当 Config 交给后端的消费者** | 把 env 改成 map（`batch_delay_ms` 本身也是 map，因为 `deadline/3` 做 `maps:get(Mode, Delays, 0)`）。**不加"proplist→map 翻译层"**：翻译层会让"配置里写的 key"与"代码里读的 key"再次分叉，正是这个缺陷的成因 | `config/sys.config.src`、§7.2 |
| **J3** | **OTP 28 的 `application:get_env/2` 返回 `'undefined' \| {'ok', Val}`，不是裸值**（`kernel-10.5/src/application.erl:918` 的 spec，:923 直接转发 `application_controller:get_env/2`；探针实测 `{ok,4}`）。按老习惯写 `case application:get_env(App, Key) of N when is_integer(N) ->` 永远落到 else 分支 | 全项目统一用 **`get_env/3`**（返回裸值）。仓库级 grep 确认 2 元形式只有 T6 新写的那一处，既有代码本来就全用 3 元；`config/sys.config.src` 顶部"read with application:get_env/2"的注释同步改为 /3 并写明原因 | §2、`config/sys.config.src` |
| **J4** | 注册表若是 `public` ETS，任何进程都能把某分区的句柄换成指向**别人的日志**——数据面直读 ETS 的代价 | 用 `protected` + 单写者 `broker_queue_store`（`put/2` 走 gen_server），读仍在调用者进程内。与 ADR-010【F3】给 router 索引定的形态一致 | §5.1、§9-T6 |
| **J5** | Worker `terminate/2` 时注册表条目怎么办：擦掉看起来"干净" | **保留**。擦掉后 `lookup/1` 会报 `{unknown_partition, Id}`——那是"这个分区不存在"的谎，比"句柄过期但能自愈"严重得多。附带删除了无调用者的 `broker_queue_store:erase/1`（xref 报出） | §7.2、§9-T6 |
| **J6** | `{new_messages, PartitionId, From, To}` 的语义强度未定，T7 的 dispatcher 会依赖它 | 明确为 **cast = 唤醒信号，不是投递契约**：可乱序、可重叠、Worker 崩溃期间会丢。dispatcher 必须自己按 cursor `read_from`，cursor 才是唯一真相源。订阅者集合由 Worker `monitor` 并自动摘除死亡进程 | §5.2、§9-T7 |
| **J7** | 句柄失效（I15）后是否自动重试 | **不自动重试**。已抵达 writer 的 append 可能已消耗 offset，静默重试会在发布者毫不知情下制造重复消息；重试是发布语义的决定，归 caller。返回 `{error, {stale_handle, PartitionId, Class, Reason, Repair}}`，其中 `Repair` 是本次 `reattach` 的结果。`reattach` 失败时 Worker **保留 state 不停机**，否则一次磁盘抖动变成重启风暴 | §7.2、§9-T6 |
| **J8** | relx 生成的 `bin/unified_mq` 在 exec 前做 **`cd "$ROOTDIR"`**（脚本第 770 行），所以 `data_dir = "data"` 落在 **release 根目录**（`_build/default/rel/unified_mq/data`），不是调用者的 CWD。后果两条：`rebar3 release` 会清掉数据；带外建的分区若不在同一 CWD，节点起来一律 `{not_found,_}` | 实测确认并在 `config/sys.config.src` 的 `data_dir` 旁写明。**T12 的 dev 环境必须改用 release 树之外的绝对路径**；S1 的验证脚本按"与节点同 CWD"执行 | §2、§6、§9-T12 |
| **J9** | `create_partition/1` 号称"app 停着也能用"，但分区清单在 `broker_queue` 的 env 里 ⇒ app **未 load** 时 `partition_ids/0` 抛 `{invalid_partition_count, undefined}`；而 release 又**没有维护命令**（节点根本起不来，`rpc` 无从下手） | 精确化为"需要 **load**，不需要 **start**"。实测可用的带外路径：`erl -noshell -pa <rel>/lib/*/ebin -config <rel>/releases/Vsn/sys.config -eval 'application:load(broker_queue), application:ensure_all_started(broker_storage), broker_queue:create_partition(Id), halt().'`（CWD 见 J8）。**不在 `partition_ids/0` 里隐式 `application:load`**——隐式加载是意外副作用，且会掩盖"配置没读到"这个真问题 | §6、§9-T6 |
| **J10** | **T5 遗留的真实缺陷**：`broker_storage_wal_SUITE` 定义了 `init_per_suite/1` 却没有 `end_per_suite/1`。CT 只要二者**之一**被导出就会调 `end_per_suite`（`test_server_ctrl:do_add_init_and_end_per_suite/4` 对两者都做 `function_exported` 检查），于是每次全套跑都打一条 `%%% undefined ==> end_per_suite: FAILED {undef,...}`——**但 CT 不把它计入总数**，"All 141 tests passed" 字面为真，失败只出现在 `%%%` 明细行里 | 补 `end_per_suite/1`（顺带 `application:stop(broker_storage)`，避免把运行中的 storage app 泄漏给同节点的后续套件）。**验收规则修正：看 `FAILED` 明细行，不能只看汇总数字**；本轮全套 157/157 已确认零 FAILED 行 | §9-T5、验收流程 |
| **J11** | 本机 `/tmp` 是 **tmpfs**，持久化守卫（I4/I5）在 `/tmp` 下拒绝建分区（`{error,{volatile_filesystem,"data/wal/0"}}`） | 守卫**按设计工作**，不是缺陷；但 release 级验证必须在 ext4 上跑（`/`、`/home` 均为 ext4）。记在此处是因为这个坑会重复踩 | §7.2、验收流程 |

### T5 实现期间发现并修正（2026-10-21，均为实测暴露）

冻结 API（ADR-011）全程未改，13 个契约用例在 WAL 后端**一条未改未跳**地跑通。下列 15 项是"计划文本 ↔ 第一次真实实现"的偏差，I11 是 §9-T5 留下的必答项答复，I12/I13 是**真实缺陷**（I12 只在装配后的 release 中出现）。

| 项 | 问题 | 处置 | 影响 |
|---|---|---|---|
| **I1** | §7.2 计划的模块清单含 `wal_reader`，但读路径若独立成进程/模块，就必须跨进程传段句柄 | **不设 `wal_reader`**：读路径 = `broker_storage_wal:read_from/3` 在**调用者进程内** `wal_index:locate/2` 定锚点 + `wal_segment:fold/5` 流式扫描（README §7.2 数据面不经进程） | §7.2、§9-T5 |
| **I2** | §7.2 把 `wal_group_committer` 写成"gen_server：mailbox 收集 append 请求"。批次所有权若与 offset 分配/段轮转/水位推进**不在同一进程**，fsync 与 offset 分配之间就多一处竞态 | 改为**纯策略模块**（`new/1`、`deadline/3`、`due/4`、`timeout/2`、`needs_sync/1`、`take/2`、`earliest/1`，无状态无进程）；mailbox 攒批 + 延迟回复留在 `wal_writer:handle_call`。附带收益：策略可脱离进程单测（11 例） | §7.2、§9-T5 |
| **I3** | §7.2 模块清单缺 `wal_meta`，而 §11.3 要求 `recover/2` 返回 `CommittedOffset`——水位若只能从段重放推出，恢复代价与日志长度成正比 | 新增 `meta.log`（定长水位记录，`wal_meta`）；尾部状态三分类 `clean \| {torn, truncated} \| {corrupt, term()}`，`recover` 读水位成为一次定长读 | §7.2、§7.3 |
| **I4** | §7.2 说 tmpfs 防护"移到 `wal_writer:init/1`"。放 `init/1` 会让 `create/2` 先起一个注定自杀的子进程再拆掉，白烧 supervisor `intensity` 预算并留下 `already_present` 的 spec | 守卫提到 `start_link/3`（`wal_writer:check_filesystem/2`），不通过则**根本不创建进程**，`create/2` 直接返回 `{error, {volatile_filesystem, Dir}}` 且目录不存在 | §7.2 |
| **I5** | S0.3 §4 的 tmpfs 判据是"fsync 计时 < 100 µs"。快 SSD 上会把正常盘误判为易失，而误判的代价是**节点起不来** | 改读 `/proc/mounts` **按 fstype** 匹配 `tmpfs`/`ramfs`；ext4/xfs/btrfs/zfs/overlay 一律采信；`require_durable_fs => false` 可关（CI），关闭时返回**警告**而非错误。`classify_fs/2` 把挂载表文本作参数，因此可在测试里注入而不必伪造宿主机挂载 | §7.2、§8.1 |
| **I6** | §7.2 的索引伪代码是 `[{StartOffset, SegPath, [{AnchorOffset, FilePos}]}]` 三层嵌套。ETS 的有序键本身已提供"≤Offset 的最近锚点"，再套一层段分组只会让 `locate/2` 变两次查找 | 改为 `public` 的 `ordered_set` ETS，键只有 `{meta}`（open/closed + `truncate_before`）与 `{anchor, Offset}`（值含 `SegPath`，故跨段续读是一次 `locate/2` 而非分区级重扫） | §7.2（E8） |
| **I7** | §7.1 第 2 条留的悬案："多写者并发下的观察窗口是 T5 必须对 WAL 明确结论的场景之一" | **结论：WAL 后端不存在该窗口**。`HighOffset` 刻意**不进索引**，只由 writer 进程状态持有；`read_from/3` 扫到 torn tail 与扫到日志末尾不可区分，也不需要区分（前者是"writer 正跑在前面"）。内存后端的空洞源于把"下一个 offset"暴露给读路径，WAL 侧结构性避开 | §7.1、§7.2 |
| **I8** | 计划隐含"轮转是段模块的一个函数" | **没有 `wal_segment:rotate/N`**：`segment_max_records` 是每段记录数上限，一个批可以在中途跨段，所以轮转是 writer 的**状态迁移**（关旧段 → 按新 `StartOffset` 命名开段 → 钉住新段首记录锚点）；`wal_segment` 只暴露无状态文件操作 | §7.2 |
| **I9** | 编码在哪个进程执行未定 | **调用者进程**：`wal_writer:append/3` 先逐条 `wal_codec:encode/1` 成二进制再进 writer。否则 `term_to_binary` 的 CPU 全压在单一 writer 上，group commit 攒的批越大、该进程越成吞吐瓶颈——批处理要省的是 fsync 次数，不是把 CPU 串行化 | §7.2 |
| **I10** | 写/fsync 失败的重试语义未定 | **致命不重试**：offset 是"先分配后落盘"发出去的，失败后继续服务等于把已承诺 offset 变成永久空洞（§11.4 禁止静默降级）。writer `{stop, Reason, ...}` 自杀，批内 caller 收 `{error, {writer_stopped, Reason}}`，supervisor 重启后从磁盘重建 | §7.2、§11.1 |
| **I11** | **H8 未答**：§6 boot step 1 要求逐分区 `recover/2`，但分区清单 `{partitions, N}` 配在 `broker_queue` 的 env 里，**存储层看不到它**；且 §11.2 禁止 recover 顺手 create，于是 S1 没有任何角色负责首次 `create/2` | **归属答复**：(a) boot step 1 的 recover 循环落在 **`broker_queue_sup`**，不是 `broker_storage_sup`（后者只立进程结构）；(b) `create/2` 的**唯一**入口是带外管理动作 `broker_queue:create_partition/1`（**T6 交付**），不出现在任何 boot 路径；(c) 干净数据目录 boot 必失败是 §11.2 要求的**响亮失败**，不引入 `partition_bootstrap`/首启标记之类开关——那等于从后门绕掉守卫。首启与恢复因此无需靠数据目录内容区分 | §6、§9-T6、§11.2 |
| **I12** | **T4 真实缺陷，且只在 release 中暴露**：没有任何角色启动 `broker_storage_memory_store`，于是装配后的节点里每次 `broker_storage_memory:create/2` 都是 `noproc`。T4 的 CT 全绿，是因为套件在自己进程里手工建了表 | `broker_storage_sup:init/1` 按 `backend` **只起自己需要的那一个子进程**（wal → `broker_storage_wal_sup`；memory → `broker_storage_memory_store`）；新增 `broker_storage_sup_SUITE`（1 例）把"每个后端启动它的 `create/2` 所需进程"钉成断言 | §6、§7.1 |
| **I13** | **v0.6 契约头的真实缺陷**：tripwire 写成 `[os_buffer \| unsupported_modes()]`，隐含"后端只声明 os_buffer"。诚实声明 `fsync` 的 WAL 后端**被自己的 tripwire 判失败**（`{badmatch, false}`），而这条断言本意只是防止 `sync_mode()` 集合扩张后漏改 | 引入 `all_sync_modes/0` 作为唯一真相源，`unsupported_modes/0` 从中派生；tripwire 改为"声明的模式必须属于 `all_sync_modes/0`"。WAL 后端 13/13 通过 | §7.1、§7.2 |
| **I14** | OTP 28.3.1 supervisor 的两个行为未在文档里：`start_child` 失败**不留 spec**（所以契约里 `recover(7)` → `create(7)` 的顺序是安全的）；子进程启动函数返回 `{error, Reason}` 会被包成 **`{error, {Reason, #child{}}}`**（9 元组 tag `child`） | `broker_storage_wal:unwrap/1` 剥掉这层包装，保住 ADR-011 §11.2 承诺的 `{error, {not_found, Id}}` 原样出到调用方——否则冻结签名里的错误元组会被 supervisor 悄悄改写 | §3.3、§6 |
| **I15** | 句柄生命周期未定：索引 ETS 随 writer 进程消亡 | **writer 崩溃后旧句柄失效**，`read_from/3` 抛 `badarg` 而非返回 `{error, _}`（与内存后端同性质）。T6 的分区 worker **不得**跨 writer 重启缓存 pid/句柄，重启后必须重新 `recover/2`；`a_crashed_writer_is_restarted_and_the_log_survives` 用 `kill -9` 实测了这条路径（重启后 `recover` 得 `{0, 4}`，三条 payload 完好）<br>**→ 已由 T6 消化（J7）**：`broker_queue` 捕获 `badarg` 后向 Worker 发 `reattach`（重新 `recover/2` 覆盖注册表句柄），返回 `{error,{stale_handle,...}}` 交 caller 决定，**不自动重试**；`broker_queue_SUITE:a_crashed_storage_writer_is_reattached_and_the_log_continues` 实测 `kill -9` 后第二次 append 得 `{ok,4}`、整页 `[1,2,3,4]` 完好 | §7.2、§9-T6 |

### T3/T4 实现期间发现并修正（2026-10-21，均为实测暴露）

冻结文本本身没有 API 错误，但下列 6 项在"第一次被真实实现踩过"时暴露。H6/H7 需要 ADR 批注，H8/H11 是 T5/T7 的必答项。

| 项 | 问题 | 处置 | 影响 |
|---|---|---|---|
| **H6** | `route_ctx` 冻结在 `broker_router.erl` **模块内**——Erlang 的 record 不跨模块可见，于是除该模块外**没有任何调用者能构造 Ctx**，§5.1(c) 的 `route(T, Ctx, Env)` 一行无法写出 | record 原样搬进 `apps/broker_router/include/broker_router.hrl`（字段与类型标注不变），behavior 模块改为 include 该头文件；ADR-010 §10.1 加批注。**API 未变**，仅定义位置变 | ADR-010 §10.1、§3.2 |
| **H7** | ADR-010 验证段把"一条消息命中多订阅时 Router 返回列表完整且**去重**"记在 Router 名下。但一个 SubRef 只登记一个 filter（`register/4` 对同一 SubRef 的第二次登记返回 `{already_registered, _}`，否则索引与 `.subs` 会不一致），跨 filter 的重复在结构上不可能发生；而同一 **client** 的多个重叠 filter（`sport/#` 与 `sport/tennis/+`）会产生多个 SubRef，合并它们必须解引用 SubRef——§10.2 明令禁止 | Router 侧改为可保证的两条：结果**完整**且**同一 SubRef 不重复**（已测）；MQTT §4.8 的"同 client 重叠订阅按 max QoS 投递一次"归 `broker_subscription`（T7）。验证段加勘误批注 | ADR-010 验证、§9-T7、T13 |
| **H8** | §6 boot step 1 要求逐分区 `recover/2` 且"缺分区即报错、不自动 create"（§11.2），但 S1 **没有任何角色负责首次部署时 `create/2`**：干净数据目录起 broker 必然 `{not_found, _}` 而启动失败。内存后端实测同样无法通过 step 1 | 不是代码缺陷，是规格缺口。T5 必须先定"分区 bootstrap 由谁做"（管理命令 / `partition_bootstrap` 配置 / 以数据目录标记区分首启与恢复），**不得**用"recover 顺手创建"绕过 §11.2<br>**→ 已由 I11 答复（2026-10-21）**：`create/2` 只归带外管理动作 `broker_queue:create_partition/1`（T6 交付）；boot step 1 的 recover 循环归 `broker_queue_sup`；三个候选方案里的后两个（`partition_bootstrap` 配置、首启标记）**均被否决**，因为它们等价于从后门绕掉 §11.2 的守卫 | §6、§11.2、T5 |
| **H9** | ADR-011 未冻结 offset 基数，`CommittedOffset`/`HighOffset` 在空分区上的取值含糊（这直接决定 §8.2 R3 的 `max(0, last_flushed - K)` 怎么写） | 定为 offset 从 **1** 开始、`Committed = 0` 表示"从未确认过任何一条"、空分区 `recover` 返回 `0 / 1`；已在 §7.1 落文并由测试锁定。**T5 已在 WAL 后端复验通过**（同一契约用例，13/13） | §7.1、§8.2、T5 |
| **H10** | §11.3 的"水位偏高绝对禁止"只是文字约束，签名层面 `advance_committed/2`、`truncate/2` 都能传任意 offset | 实现加守卫并在 T4 用例锁定（见 §7.1）；T5 WAL 后端必须通过同一组用例，否则视为缺陷。**已实测通过**：13 个契约用例在 WAL 后端一条未改未跳地跑通，水位守卫（偏高拒绝、后退拒绝）与 truncate 边界均由机械检查覆盖 | §7.1、§11.3、T5 |
| **H11** | **hash 分区 + 通配符订阅 ⇒ 一个订阅跨多个分区**。实测 `sport/#` 一个 SubRef 同时命中分区 `[0,1,3]`（N=4）。而 §5.3 规定 per-sub cursor 是 `subscriptions/cursors/{subref}.cursor` 里的**一个标量 offset**——单个标量无法描述"跨 3 个分区各读到哪"，追赶/重投会漏或重复 | 冻结 API 无需改动（per-sub cursor 归 subscription，storage 不感知，正是 §11.3 的切分）。T7 必须把 cursor 落成 **per (SubRef, PartitionId)**（如 `{subref}.{partition}.cursor`）。已列为 T7 前置决策 | §5.2、§5.3、§9-T7 |

### T1 实现期间发现并修正（2026-10-21，均为实测暴露）

| 项 | 问题 | 修正 |
|---|---|---|
| **H1** | §6 画的单一 `broker_app_sup`「application master」在 8 个独立 OTP app 的结构下没有承载者：它若把别的 app 的 supervisor 当 child 起，那些 app 就不能再作为 application 启动 | 取消该层，改为每 app 一个顶层 supervisor，跨 app 顺序由 `.app.src` 的 `applications` 拓扑保证；实测 `ensure_all_started` 顺序与四个 boot step 一致 |
| **H2** | relx 用 `{sys_config_src, ...}` / `{vm_args_src, ...}` 时，rebar3 3.27 只把文件原名拷进 `releases/Vsn/`（仍带 `.src`），启动脚本找不到配置 → `bin/unified_mq daemon` 挂死、`ping` 无响应 | 改用 `{sys_config, ...}` / `{vm_args, ...}`；实测 `releases/0.1.0/{sys.config,vm.args}` 生成、`daemon`+`ping=pong`+`rpc application:get_env` 全部正确 |
| **H3** | `{extended_effects, true}` 不被该 relx 版本识别，报 `Unknown term found in relx configuration` | 移除 |
| **H4** | `vm.args` 里写的 `+C multi_scheduling` 被 erts 拒绝（`Invalid time warp mode`），节点起不来；`+h`/`+W` 亦非 OTP 28 合法开关 | 改为 `-C multi_time_warp`，删掉未经确认的开关；此后所有 vm.args 选项一律以"能否真启动"为准 |
| **H5** | `sys.config` 里预置的 sasl 文件日志指向不存在的 `log/sasl` 目录 | S1 用默认 logger，文件日志归 T12 dev 环境；`.gitignore` 补 `/data/`、`/log/` |
