# ADR-009：统一消息模型 `broker_message` 记录定稿

- **状态**：**已采纳（S1 T0.5 冻结，2026-10-21）**——F1~F6 评审整改已并入；后续修改须走 ADR 修订流程；**T17 复核（2026-11-06）**：`record_info(fields, broker_message)` 与 §9.1 同名同序，未触发修订
- **日期**：2026-10-21
- **关联**：README §3.1（两种模型分离）/§6.3（可靠性等级）/§9.1（发布生命周期）；roadmap §3.2 抽象 1；ADR-005（事件 ID 分离）、ADR-007（三档可靠性）；设计文档 `docs/design/s1-mqtt-vertical-slice.md` §3.1

## 背景（Context）

roadmap §3.2 要求 S1 冻结三个核心抽象，`broker_message` 是第一个：后续所有协议 Adapter（S5 的 AMQP/Kafka/NATS/WebSocket/Pulsar）、存储后端（S2 可插拔、S4 复制层）都挂在这一个记录定义上。roadmap §3.2 明文「定稿后不改」——一旦 S2+ 需要增删字段，影响面是所有已实现的协议与已落盘的 WAL 段。

README §3.1 给了草案但留有三处歧义，必须在 S1 定死而非推迟：

1. **协议私有元数据放哪儿**。草案只有单一 `headers` map，未区分"跨协议公共元数据"与"协议私有字段"（MQTT packet_id、Kafka partition key、Pulsar MessageId）。若混用，S3 引入 CloudEvents 扩展属性后无法区分哪些 key 来自业务、哪些来自协议帧。
2. **分区标识的类型形状**。草案写 `partition_id` 但未定类型。若定义成 `{TopicHash, Index}` 之类带语义的结构，会把"哈希映射"这一 Router 私有实现固化进消息模型；S4 引入 ADR-008 的分组模型（16 分区/组）后，分组的归属信息无处安放。
3. **可靠性等级的落位**。ADR-007 定义了三档，但没说是**配置级**、**连接级**还是**每条消息级**。这决定 Producer 能否按消息选择耐久性，也决定 §5.1 publish 路径的 API 形状。

## 决策（Decision）

### 9.1 记录定义（`apps/broker_core/include/broker_message.hrl`）

```erlang
-type partition_id() :: non_neg_integer().
-type offset()       :: non_neg_integer().
-type principal()    :: binary().
-type sync_mode()    :: os_buffer | fsync | replicated_commit.

-record(broker_message, {
    message_id        :: binary(),
    tenant_id         :: binary(),
    destination       :: binary(),
    partition_id      :: partition_id(),
    payload           :: binary(),
    payload_encoding  :: raw | json | binary | text,
    headers           :: map(),
    proto_meta        :: map(),
    persistence       :: transient | durable,
    reliability       :: sync_mode(),
    created_at        :: integer(),
    expires_at        :: integer() | undefined,
    event             :: undefined | map()
}).
```

公共类型（`partition_id`/`offset`/`principal`/`sync_mode`）与记录同处 `broker_core`，供 Router/Storage/Subscription 引用。

### 9.2 字段语义规则

- **`headers` 与 `proto_meta` 分工**：`headers` 只承载 CloudEvents 扩展属性与未来**跨协议公共元数据**（S1 恒空）；所有协议私有字段一律进 `proto_meta`（S1：MQTT 存 `#{qos, dup, retain, packet_id}`）。判据：换一个协议后该 key 是否仍有意义——无意义则属 `proto_meta`。
- **`partition_id` 是纯身份整数**，不含任何派生语义。topic→partition 的映射算法是 Router 私有实现；分区→Raft 组的归属是 S4 控制面元数据。二者都不得编码进 ID 的结构里。
- **`reliability` 是 per-message 属性**，非全局或连接级配置。Producer 可按消息选择等级；`broker_storage:append/3`（ADR-011）依此参数分路径。
- **`message_id` 由 Broker 分配**，与 CloudEvents `id` 完全分离（ADR-005）。协议私有标识（MQTT packet_id、Pulsar MessageId、Kafka offset）**禁止**写入 `message_id`。
- **`created_at` 是 Broker 接收时间**，与 CloudEvents `time`（事件发生时间）分列（README §4.1），S3 填充 `event` 后二者并存。

### 9.3 S1 保留但不生效的字段

| 字段 | S1 行为 | 生效阶段 |
|---|---|---|
| `expires_at` | **恒 `undefined`**（`durable`/`transient` 都是），dispatcher 不检查、不丢弃 | S2 |
| `event` | 恒 `undefined`；`broker_event` 为 pass_through 桩（OQ-5） | S3 |
| `headers` | 恒空 map | S3 |
| `reliability = replicated_commit` | 类型存在，两个存储后端 `capability/1` 均不返回，运行时报错 | S4 |

保留这些字段是为遵守 roadmap §3.2「定稿后不改」：加字段是破坏式变更（历史 WAL 段解码、所有 Adapter 的构造代码），留空槽位则是非破坏式。

**`expires_at` 不派生占位值**（F4 整改）：TTL 时长是 S2 写路径的策略决定，S1 无从得知。若 S1 就把 `transient → created_at+5min` 这类**编造的**值落进不可变 WAL 段，S2 上线真实 TTL 策略时，历史消息会被一个从未被任何 ADR 承诺过的时间戳误判过期——这是**已 ACK 消息被静默丢弃**，直接违反 Gate S1 的零容忍断言（ADR-011 §11.7）。留 `undefined` 让 S2 显式选择策略（写入时赋值，或对 `undefined` 用默认值），成本仅是 S2 多一行判空。

### 9.4 内存模型 ≠ 磁盘协议

`#broker_message{}` 是**内存模型**。WAL 段中的记录采用独立定义的紧凑元组 + `Vsn` 版本位（见 ADR-011 §11.5），由 `wal_codec:encode/1` / `wal_codec:decode/2` 桥接。记录字段增减不直接改变磁盘格式，磁盘格式演进走 Vsn 兼容分支。

## 后果（Consequences）

- **+** 协议私有元数据有明确归属，S5 六协议接入时无需逐协议重构记录。
- **+** `partition_id` 无语义化，使 S4 分组模型（ADR-008）可自由引入组/Epoch 元数据而不触碰消息模型。
- **+** per-message reliability 让 ADR-007 三档契约在 API 上真实可达，而非停留在文档表述。
- **+** 内存/磁盘解耦使 WAL 格式可独立演进（S0.3 实测的编码格式被版本化封装）。
- **-** 4 个字段在 S1 是悬空槽位，增加实现与阅读成本；必须由 §9.3 表格显式声明"不生效"，否则实现者会误以为已具备 TTL/CloudEvents 能力。
- **-** `proto_meta` 与 `headers` 的分工是一条需要持续守护的约定，PR review 时要逐个 key 追问"这属于哪边"。

## 验证（Validation）

- **语法已实测**（冻结前，OTP 28.3.1 `erlc`）：§9.1 记录含字段级类型标注可编译，且被 ADR-011 的 `-callback append/3` 直接引用通过。
- **S1 T17 冻结契约复核（2026-11-06）**：对 T2~T15 的最终代码做机械核对——包含 `broker_message.hrl` 编译后 `record_info(fields, broker_message)` 返回 `[message_id, tenant_id, destination, partition_id, payload, payload_encoding, headers, proto_meta, persistence, reliability, created_at, expires_at, event]`（`record_info(size, ...) = 14`，即 13 字段 + tag），与 §9.1 **同名同序**；`partition_id()` 仍为 `non_neg_integer()`、`sync_mode()` 仍为三值枚举。**未触发修订**。

- S1 集成测试（T13）：同一测试套件在 memory 与 WAL 两后端下，`#broker_message{}` 往返字段级相等（含空槽位）。
- S1 崩溃恢复测试（T14 R1a/R4）：WAL 记录编解码无损，`Vsn` 往返正确。
- S2 需证明：`expires_at` 从"不生效"切到"生效"时，S1 落的旧 WAL 段仍可解码。
- S3 需证明：`event`/`headers` 填充后不破坏 ADR-005 的 ID 分离断言。
- S5 需证明：AMQP/Kafka/NATS/Pulsar Adapter 均能在不改记录定义的前提下填出合法 `proto_meta`——**若某协议做不到，视为本 ADR 需修订的触发条件**，须在 Gate S5 显式处置，不得静默塞进 `headers`。
