# UnifiedMQ NATS / Kafka / Pulsar 功能语义差异与改进需求

**文档版本：** V1.0  
**编写日期：** 2026-11-12  
**项目：** UnifiedMQ  
**目标：** 梳理 NATS、Kafka、Pulsar 的原生消息语义，评估 UnifiedMQ 当前公开实现证据，识别兼容性边界与后续需求。  
**参考分支：** `main`（以仓库当前公开 README、`docs/architecture.md`、`docs/roadmap.md`、`progress.md` 为基线）

---

## 0. 文档说明与结论摘要

本文不是对 NATS、Kafka、Pulsar 产品功能做简单罗列，而是回答三个问题：

1. 三种协议分别依赖什么消息模型和状态语义？
2. UnifiedMQ 当前公开资料能证明实现到了什么程度？
3. 为了避免“客户端能连上”被误认为“完整兼容”，还需要补齐哪些功能和测试？

### 0.1 总体结论

NATS、Kafka、Pulsar 三者虽然都提供发布/订阅能力，但原生模型差异很大：

- **NATS Core** 是 Subject 驱动的实时消息系统，核心语义是在线订阅者投递、Queue Group 竞争消费和 Request/Reply；Core NATS 本身不提供离线消息持久化及历史回放。JetStream 在其上增加 Stream、Consumer、持久化、ACK、重投递、Replay 和复制。
- **Kafka** 是分区追加日志系统。Topic 被切分为 Partition，每个 Partition 是有序日志；消息由 Offset 定位，Consumer Group 通过协调器分配分区并提交 Offset。消息保留由时间/容量/压缩策略控制，不以消费者 ACK 作为删除的直接依据。
- **Pulsar** 是 Topic + Subscription + Cursor 模型。每个 Subscription 拥有独立消费位置，支持 Exclusive、Failover、Shared、Key_Shared 等消费模式；消息存储与 Broker 服务层分离，原生持久化架构依赖 BookKeeper Managed Ledger，并具有多租户、分层存储和跨集群复制等能力。

**UnifiedMQ 当前已具备三种协议的接入实现及真实第三方客户端测试，但不能由此推断完整原生产品兼容。** 仓库当前公开进度列出 NATS 11/11、Kafka 12/12、Pulsar 8/8 的真实客户端测试；架构文档同时列出 Kafka Group 协调进程和 Pulsar Subscription/Cursor 管理进程。公开材料尚不足以证明 JetStream 全部 API、Kafka 全部 API/事务语义、Pulsar 完整订阅/Schema/事务/BookKeeper 兼容。

### 0.2 当前状态证据口径

本文使用以下标记，避免混淆事实和推测：

| 标记 | 含义 |
|---|---|
| **已证实** | 仓库当前公开文档明确描述实现，或明确列出对应测试证据 |
| **有结构证据** | 架构中存在对应模块/进程，但不能据此断定全部协议语义已经实现 |
| **设计目标** | README / 设计说明中提出的目标，不等同于已经实现 |
| **未见证据** | 在本次可核验的公开架构与进度材料中没有找到足够证据；需查代码、兼容矩阵和测试后才能最终定性 |
| **明确非目标** | 项目公开资料明确说明不等同于原生产品完整架构，或选择了不同实现路线 |

本次核对可见 Gitee README、`docs/architecture.md`、`docs/roadmap.md`、`progress.md`。`docs/protocol-compatibility.md` 的当前正文未能通过本次网页读取完整展开，因此对没有被其他公开资料证明的细项，不冒充已完成或已缺失，而标为“未见证据/待代码核验”。

### 0.3 当前公开实现基线

| 协议 | 真实第三方客户端测试 | 架构中可见的专属运行时组件 | 当前判断 |
|---|---:|---|---|
| NATS | 11/11 | `broker_proto_nats` listener / supervisor | 基础 NATS 客户端交互已有证据；JetStream 未见专属实现证据 |
| Kafka | 12/12 | `broker_proto_kafka` listener；`kafka_group` | 除连接/报文层外，已建立消费组协调状态组件；完整 Kafka API/事务尚不能据此推定 |
| Pulsar | 8/8 | `broker_proto_pulsar` listener；`pulsar_subscriptions`（游标和未 ACK 集合） | 已建立订阅状态管理；原生 BookKeeper 架构明确不是首期要求，完整 Pulsar 高级能力未得到证明 |

注意：这里的测试数字是仓库 `progress.md` 对真实第三方客户端兼容套件的记录，不是官方全量协议测试覆盖率，也不是原生服务器等价性证明。

---

# 1. 三种协议的核心定位

## 1.1 NATS：Subject 与实时分发优先

NATS 的核心地址空间是 Subject，采用点号分层命名，并使用 `*`、`>` 通配符匹配。Core NATS 提供轻量发布/订阅、Queue Group 和 Request/Reply：

- 普通订阅者收到匹配 Subject 的消息副本。
- Queue Group 中同一匹配组通常只有一个成员接收该消息，用于工作负载分摊。
- Request/Reply 通过唯一 Inbox / Reply Subject 进行请求与响应关联。
- Core NATS 对在线订阅者进行实时投递；离线订阅者不会在恢复后补收旧消息。

JetStream 是附加在 NATS 上的持久化层，不能把 Core NATS 的 PUB/SUB 成功等价为 JetStream 的持久化发布确认。JetStream 引入：

- Stream：绑定一个或多个 Subject，保存消息并分配序列号。
- Consumer：服务端保存的消费视图和进度状态。
- ACK / NAK / TERM / IN-PROGRESS 等消费确认动作。
- Durable / Ephemeral Consumer、Push / Pull 消费方式。
- Retention Policy、Replay Policy、Redelivery、复制和去重能力。

因此 NATS 实际上包含两种不同的消息服务语义：Core NATS 的瞬时消息分发，以及 JetStream 的持久化流。

## 1.2 Kafka：Partition Log 与 Offset 优先

Kafka 的核心对象为：

```text
Cluster
  └── Topic
       └── Partition
            └── Append-only Log
                 ├── Offset 0
                 ├── Offset 1
                 ├── Offset 2
                 └── ...
```

主要语义：

- Topic 是逻辑分类；Partition 是并行度、顺序和复制的基本单位。
- 每个 Partition 内消息按 Offset 形成有序日志；Kafka 不提供跨 Partition 的全局顺序保证。
- Producer 可按 Key / Partitioner 决定消息进入哪个 Partition。
- Consumer Group 中的成员分配 Partition，通常一个 Partition 在同一消费组内由一个成员负责消费。
- Consumer 通过提交 Offset 表达消费进度，而不是像 AMQP 一样逐消息 ACK 后把消息从队列删除。
- 消息通常按时间/容量保留；Log Compaction 可按 Key 保留最新值，已消费不意味着消息立即删除。
- Producer 幂等和事务能力是高级可靠性语义，不能仅用 TCP 请求成功或普通 Produce 响应替代。

Kafka 的核心设计是可回放、可定位的分区日志，而不是传统工作队列。

## 1.3 Pulsar：Topic + Subscription + Cursor 优先

Pulsar 的基本结构：

```text
Tenant
  └── Namespace
       └── Topic
            ├── Producer(s)
            └── Subscription(s)
                 ├── Consumer A
                 ├── Consumer B
                 └── Cursor / ACK State
```

主要语义：

- Topic 可为持久化或非持久化，也可为分区 Topic。
- 每个 Subscription 独立维护消费进度；不同 Subscription 可以独立读取同一 Topic。
- Subscription Type 决定消费者竞争方式和顺序约束：Exclusive、Failover、Shared、Key_Shared。
- 支持 Individual ACK、Cumulative ACK（受订阅类型限制）、Negative ACK、ACK Timeout 和重投递。
- 消息 ID 由存储位置表示，通常包含 ledger / entry / partition / batch 等信息，不能简单等同于 Kafka Offset。
- 原生持久化路径基于 BookKeeper Managed Ledger；Broker 提供 Lookup、连接、分发和订阅协调。
- 还包括多租户、Schema、Retention、Backlog Quota、Tiered Storage、Transactions 和 Geo-replication 等能力。

Pulsar 的关键不是“另一种 Kafka”，而是 Subscription Cursor 与存储层分离，以及同一 Topic 上多种不同消费拓扑。

---

# 2. 关键功能语义对比

## 2.1 核心模型对比

| 维度 | NATS Core / JetStream | Kafka | Pulsar |
|---|---|---|---|
| 主地址模型 | Subject | Topic / Partition | Tenant / Namespace / Topic / Partition |
| 通配符/路由 | Subject token wildcard | 通常由 Topic + Key/Partitioner 决定；不是 NATS 式 Subject 路由 | Topic 名称及 Namespace；可订阅多 Topic/模式（客户端能力依版本而定） |
| 默认消息形态 | Core 为实时消息；JetStream 为持久化 Stream | 分区追加日志 | Topic 消息流 + Subscription |
| 顺序范围 | Core 订阅分发顺序；JetStream Stream/Consumer 由配置和消费模式决定 | 单 Partition 内有序 | Topic/Partition 内有序；Shared 等模式可能打散全局/跨消费者处理顺序 |
| 竞争消费 | Queue Group | Consumer Group + Partition Assignment | Shared / Key_Shared 等 Subscription Type |
| 广播消费 | 多个普通 Subscriber 各自接收 | 多 Consumer Group 独立消费同一日志 | 多个独立 Subscription 各自消费 |
| 请求响应 | 原生 Inbox / Reply Subject 模式 | 通常由应用用 Reply Topic + Correlation ID 组合 | 通常由应用自行关联请求和响应 Topic |
| 历史回放 | Core 不支持；JetStream 支持 | 原生 Offset / Seek / Replay | Reader / Subscription Cursor / Seek |
| 离线消费 | Core 不保存；JetStream 可持久化 | 日志按保留策略保留 | 持久 Topic + Subscription Cursor |
| 消息确认 | Core 无持久 ACK；JetStream 有 ACK 协议 | Commit Offset（批次/分区进度） | Message ACK / Cursor ACK |
| 消息删除依据 | Core 不存；JetStream 按 Retention Policy | Retention / Compaction，与消费提交分离 | Cursor、Retention、Backlog 等策略共同决定 |
| 原生持久化 | JetStream Memory/File Store | Partition Log | BookKeeper Managed Ledger（持久 Topic） |
| 多租户 | Account / User / Export / Import | 主要通过 ACL、集群和应用约定实现隔离 | Tenant / Namespace / Role / Policy |

## 2.2 ACK、Offset、Cursor 不能统一成一个字段

这是三种协议最容易在统一 Broker 中被错误简化的地方。

### NATS Core

没有持久化消费确认。消息发给当前在线订阅者后，Core NATS 不跟踪应用处理完成状态。

### NATS JetStream

ACK 是对特定 Stream 消息投递的消费结果；Consumer 的服务端状态决定下一次投递位置。未 ACK 消息可按 AckWait / MaxDeliver 等配置重新投递。

### Kafka

Offset 是某个 Consumer Group 对某个 Partition 的消费位置。提交的是进度水位，不是把某条消息从 Topic 中删除。批量处理时必须处理“已提交 Offset 之前全部消息都已处理”的连续前缀语义。

### Pulsar

Cursor 属于 Subscription。Individual ACK 可记录离散消息处理情况，Cumulative ACK 推进连续位置；未确认消息集合、重投递状态和 Cursor 必须协调恢复。

### UnifiedMQ 设计要求

禁止将三者全部映射为一个简单的 `ack(message_id)`：

- 统一消息存储可以使用统一分区位置 `{partition_id, offset}`。
- NATS JetStream Adapter 仍需维护 Stream 序列、Consumer 进度、ACK 语义与重投递策略。
- Kafka Adapter 仍需维护 `{group, topic, partition} -> committed_offset`、分区分配代数和组成员状态。
- Pulsar Adapter 仍需维护 `{topic, subscription} -> cursor`、individual ACK 集合、consumer ownership 和 subscription type。
- Core NATS 不应被强行制造成持久化 Consumer/Cursor 语义。

---

# 3. NATS：功能差异与未完成项

## 3.1 NATS 原生语义清单

| 能力 | Core NATS | JetStream |
|---|---|---|
| Subject Publish / Subscribe | 核心能力 | 通过 Stream Subject 规则接收 |
| `*` / `>` 通配符 | 核心能力 | Stream Subject Filter / Consumer Filter |
| Queue Group | 核心能力 | 可通过 Consumer 与 Queue Group 模式组合实现工作分发 |
| Request / Reply | Inbox 模式 | 可与持久化 Stream / Consumer 组合，但不能改变 Core 请求响应的语义 |
| Header | NATS Header（HPUB / HMSG） | 消息 Header 存储于 Stream |
| No Responders | Core 请求响应能力 | 不等于持久化 ACK |
| Persistence | 无 | Memory / File Store |
| Durable Consumer | 无 | 有 |
| Pull Consumer | 无 | 有 |
| Push Consumer | 实时 SUB | 有 |
| ACK / NAK / TERM | 无持久化 ACK | 有 |
| Replay | 无 | 按序列、时间及策略重放 |
| Retention | 无 | Limits / Interest / WorkQueue 等策略 |
| Replication | Core 集群路由 | Stream 副本、Leader 与集群复制 |
| Deduplication / Exactly-once 辅助语义 | 不属于 Core | 依赖发布去重窗口及 ACK 确认等组合机制 |
| Mirror / Source | 无 | JetStream Stream 复制/汇聚能力 |

## 3.2 UnifiedMQ 当前状态

### 已证实

- `broker_proto_nats` 独立 Application、Supervisor 和 Listener 已接入运行架构。
- 真实第三方 NATS 客户端兼容测试记录为 **11/11**。
- 五协议统一认证已接入 `broker_security:authenticate_conn/1`，租户来自服务端用户记录，不信任客户端自报 Tenant。
- NATS Header 的 CloudEvents `ce-*` 通道已接入，但官方 NATS Binding 要求属性值按 RFC 3986 Percent-Encoding；仓库当前已明确记录该差异。

### 有结构证据，但仍需按用例确认

- Subject、Subscription、Queue Group、Request/Reply 是项目的 NATS 目标能力，README 将其列为首期/逐步实现范围。
- 仅有 `broker_proto_nats` Listener 的公开监督树信息，没有单独列出 JetStream Stream Store、Consumer State、JetStream API Handler 或 JetStream Raft/Replica 组件。

### 未见证据 / 应暂按未承诺处理

以下能力在当前可核验材料中没有足够证据证明已经实现，不能因 Core NATS 11/11 测试通过就认定支持：

- JetStream `$JS.API` 管理与发布 API 的完整兼容。
- Stream 创建、更新、删除、信息查询和配置校验。
- Durable / Ephemeral Consumer 的完整生命周期。
- Pull Consumer Fetch / Batch / Expire / NoWait。
- Ack Policy（explicit / all / none）、AckWait、MaxDeliver、BackOff。
- MaxAckPending、Flow Control、Heartbeat 与 Consumer 状态恢复。
- Limits / Interest / WorkQueue Retention。
- 按时间/序列 Replay、直接 Get、消息删除/清空。
- Stream Replicas、Leader Election、Mirror / Source。
- Message Deduplication、Double ACK 等 Exactly-Once 辅助语义。
- JetStream KV / Object Store API。

**判断：** 当前可以确认 NATS 协议入口及真实客户端基础互通；JetStream 是单独的能力层，应作为独立产品范围核验，不应被“支持 NATS”一句话覆盖。

## 3.3 NATS 后续需求

### NATS-CORE-001：Wire Protocol 与连接管理（P0）

支持并测试：

- INFO / CONNECT。
- PING / PONG。
- PUB / HPUB。
- SUB / UNSUB。
- MSG / HMSG。
- +OK / -ERR。
- 最大消息体、最大 Header、最大连接数。
- TLS、认证、授权及连接清理。
- 协议错误、非法长度、慢客户端和连接超时。

验收：至少选择 nats.go、nats.java 或官方 CLI 的版本矩阵；对每条支持的命令提供成功和错误用例。

### NATS-CORE-002：Subject 与匹配（P0）

- Subject Token 解析、大小写和合法字符校验。
- `*` 单 Token 匹配。
- `>` 尾部多 Token 匹配。
- 多个 Subject / 多个 Subscription。
- 动态 SUB / UNSUB。
- 同一连接多个 SID。
- Subject 数量、通配符复杂度和资源上限。

### NATS-CORE-003：Queue Group（P0）

- 相同 Subject + Queue Group 作为一组竞争消费者。
- 普通订阅者仍各自收到副本。
- 同组成员增减时正确更新分发集合。
- 对不同 Subject 的同名 Queue Group 不得错误合并。
- 记录每组成员状态、负载和投递失败。
- 明确 Core NATS 失败投递是否重投；不能套用 JetStream ACK。

### NATS-CORE-004：Request / Reply（P1）

- Inbox / Reply Subject。
- Request 超时和取消。
- 多响应 Scatter-Gather。
- Queue Group 单响应负载均衡。
- No Responders（若目标 NATS 版本/客户端使用）。
- 防止客户端构造的 Reply Subject 越权发布。

### NATS-CORE-005：Header 与协议兼容（P1）

- HPUB / HMSG 的 Header 编解码。
- Header 大小限制。
- 重复 Header、大小写和多值语义。
- 与官方客户端对 Header API 的真实互操作。
- CloudEvents Header 值 Percent-Encoding 的单独兼容决策。

### NATS-JS-001：JetStream Stream API（P0，独立立项）

- `$JS.API.STREAM.CREATE.*` / UPDATE / INFO / DELETE。
- Stream Subject 过滤。
- Storage Type：Memory / File（内部可映射至 UnifiedMQ Storage，但对外配置语义保留）。
- Replicas、Max Age、Max Bytes、Max Messages、Max Message Size。
- Discard Policy（Old / New）。
- Retention Policy（Limits / Interest / WorkQueue）。
- Stream Sequence 和 Message ID 映射。
- Publish ACK（PubAck）及重复发布处理。

### NATS-JS-002：JetStream Consumer（P0）

- Durable / Ephemeral Consumer。
- Push / Pull Consumer。
- Deliver Policy（All / Last / New / By Start Sequence / By Start Time / Last Per Subject）。
- Ack Policy（Explicit / All / None）。
- AckWait、MaxDeliver、BackOff。
- MaxAckPending。
- Filter Subject。
- Consumer Info / Pause / Delete。
- Consumer 重启和状态恢复。

### NATS-JS-003：ACK / 重投递 / Exactly-Once 辅助语义（P0）

- ACK、NAK、TERM、IN-PROGRESS。
- AckWait 超时重投递。
- 最大投递次数和终止行为。
- 双 ACK 确认（如实现对应协议版本）。
- 发布端 Message ID 去重窗口。
- 去重窗口与 Stream 持久化恢复一致。
- 明确 Exactly-Once 边界：不能把 Broker 内去重宣传成端到端业务 Exactly-Once。

### NATS-JS-004：Replay 与消费流控（P1）

- 按原始速度 / 尽可能快 Replay。
- Pull Batch / Max Wait / No Wait。
- Push Consumer Flow Control。
- Heartbeat / Idle Heartbeat。
- Slow Consumer 处理。
- Consumer Lag / Pending / Ack Pending 指标。

### NATS-JS-005：复制与高级功能（P2）

- Stream Replica / Quorum。
- Leader 故障切换。
- Mirror / Source。
- KV / Object Store（若纳入兼容目标）。
- 跨 Account Stream / Service Import / Export。

---

# 4. Kafka：功能差异与未完成项

## 4.1 Kafka 原生语义清单

| 能力 | 原生含义 | 与普通 Queue 的区别 |
|---|---|---|
| Topic | 消息逻辑分类 | Topic 不是一条 FIFO Queue |
| Partition | Topic 内的有序日志分片 | 并行度、顺序、复制均以 Partition 为核心 |
| Offset | Partition 内位置 | Consumer 提交进度，不是删除单条消息 |
| Record Key | 分区路由依据 | 同 Key 在分区数固定时可维持分区内顺序 |
| Consumer Group | 一组协作消费者 | 组协调器分配 Partition，并维护 Generation / Member |
| Rebalance | 成员变化时重新分配 Partition | 会影响暂停、撤销、重新分配和 Offset 提交 |
| Offset Commit | 提交消费进度 | 与 Broker 消息保留相互独立 |
| Retention | 时间/容量清理 | 消费者已消费不意味着消息立即删除 |
| Log Compaction | 按 Key 保留最新记录 | 不等价于普通 TTL / DLQ |
| Replication | Leader / Follower / ISR | 与本地 WAL 写入不是同一层次 |
| Producer Idempotence | 防止重试重复写入 | 需要 ProducerId、Epoch、Sequence 等协议状态 |
| Transactions | 原子写入多个 Partition 并与 Offset 关联 | 普通批量 Publish 不等价 |
| Read Committed | 隔离未提交事务记录 | Fetch 需要事务可见性判断 |

## 4.2 UnifiedMQ 当前状态

### 已证实

- `broker_proto_kafka` 独立 Application、Supervisor 和 Listener 已接入。
- 真实第三方 Kafka 客户端兼容测试记录为 **12/12**。
- 存在应用级 `kafka_group` 进程，用于消费组协调；它与 Kafka Listener 独立运行，避免 Listener 重启时消费组协调状态被一起重启。
- 已接入统一身份认证。
- Kafka Record Headers 的 CloudEvents 通道存在，但当前使用 `ce-` 而非官方 Binding 的 `ce_` 下划线前缀；项目已将其标注为内部约定，而非逐字 CloudEvents Binding。

### 有结构证据，但不能推定完整

- 消费组协调进程意味着项目并非只实现 Produce / Fetch 的无状态代理。
- 项目 README 的目标能力包括 Produce、Fetch、Metadata 和 Consumer Group 基础流程。
- 但不能从 `kafka_group` 进程存在推断所有 Group Protocol、Assignment Strategy、Incremental Cooperative Rebalance、Static Membership 或 Share Group 均已实现。

### 未见证据 / 应列为重点核验项

- Kafka Protocol 全量 API Key 覆盖率和版本协商矩阵。
- 完整 Topic Admin API（CreateTopics / DeleteTopics / AlterConfigs / DescribeConfigs 等）。
- 多 Partition 的 Leader / Follower 元数据视图。
- Consumer Group 完整成员状态机、Generation、Heartbeat、JoinGroup / SyncGroup / LeaveGroup。
- Range / RoundRobin / Sticky / Cooperative Sticky 等分配策略兼容范围。
- Offset Commit 的失败恢复、提交 fencing 与提交顺序保证。
- Idempotent Producer（ProducerId / ProducerEpoch / Sequence Number）。
- Transaction Coordinator、事务状态、Txn Marker、跨分区事务。
- `read_committed` / `read_uncommitted` 可见性。
- Log Compaction（保留墓碑记录、按 Key 清理、Cleaner 运行状态）。
- Kafka 原生 ACL / SASL / SCRAM / GSSAPI 等兼容范围。
- Kafka 协议层压缩批次、RecordBatch CRC、时间戳、控制批次和未知版本字段兼容。

**判断：** 已有 Kafka 客户端互通及 Group Coordinator 结构，但离完整 Kafka Broker 兼容主要还差 API 覆盖、Consumer Group 完整协议状态、Partition Leadership / Replica 语义、Idempotence / Transactions 和日志压缩等能力。具体已完成项必须以 `protocol-compatibility.md` 的 API 矩阵及协议回归用例为准。

## 4.3 Kafka 后续需求

### KAFKA-001：协议版本与 API 能力矩阵（P0）

- 实现 ApiVersions 并明确每个 API Key 的支持版本区间。
- 对请求中的 Flexible Version / Tagged Fields 正确解析。
- 未支持 API 返回标准错误码，不能静默返回成功。
- 建立 API Key × Version × Client Version 测试矩阵。
- 记录客户端版本、协商版本和实际执行路径。

**验收：** 至少覆盖选定版本的 kafka-clients、librdkafka 客户端；测试 Produce / Fetch / Metadata / Group 核心路径和不支持 API 的确定性错误行为。

### KAFKA-002：Topic / Partition / Metadata（P0）

- Topic 创建、删除、查询和配置。
- Partition 数量、Leader、Replicas、ISR 元数据。
- Broker Metadata 返回真实可达的节点、Topic 和 Partition。
- Partition Leader 变更、Epoch / Fencing。
- Topic 不存在、Partition 不存在、Leader Not Available 等错误映射。
- Key / Partitioner 的路由结果可复现。
- 增加 Partition 的语义和 Key 重映射风险文档化。

### KAFKA-003：Produce / RecordBatch（P0）

- RecordBatch 编解码。
- CRC 校验。
- Key / Value / Headers。
- Timestamp 与 TimestampType。
- Compression Codec（按承诺版本逐步覆盖）。
- Batch Append 与批量错误处理。
- acks=0 / 1 / all 的确认语义。
- Max Request / Message Bytes。
- 不合法批次和不支持版本返回正确错误。

### KAFKA-004：Fetch / Offset / Replay（P0）

- Fetch Offset、Log End Offset、High Watermark。
- ListOffsets（earliest / latest / timestamp）。
- Fetch Max Bytes / Min Bytes / Max Wait。
- OffsetOutOfRange 行为。
- Consumer Reset Policy 的服务端配合。
- 分区内顺序与跨分区不保证顺序的边界。
- 消息保留后 Offset 截断及查询行为。

### KAFKA-005：Consumer Group 协调（P0）

- FindCoordinator。
- JoinGroup / SyncGroup。
- Heartbeat / LeaveGroup。
- DescribeGroups / ListGroups。
- Member ID / Generation ID。
- Session Timeout / Rebalance Timeout。
- Partition Assignment / Revocation。
- Group Coordinator 故障迁移。
- Offset Commit / Fetch。
- Generation / Epoch fencing，防止旧成员提交。
- Group 状态持久化和重启恢复。

### KAFKA-006：幂等 Producer（P1）

- InitProducerId。
- ProducerId / ProducerEpoch 管理。
- Partition 维度 Sequence Number。
- Duplicate Batch 检测。
- Out-of-Order Sequence 错误处理。
- Producer 重启及 Epoch fencing。
- 与 acks=all / retries / max.in.flight 的配置约束。

### KAFKA-007：事务与 EOS（P1）

- InitProducerId / AddPartitionsToTxn。
- AddOffsetsToTxn / TxnOffsetCommit。
- EndTxn（Commit / Abort）。
- Transaction Coordinator 和事务超时。
- Prepare / Commit / Abort 状态恢复。
- Partition Transaction Marker。
- Read Committed / Read Uncommitted。
- 消费处理结果与 Offset 原子提交。

说明：只实现 Producer 幂等不能宣称 Kafka Exactly-Once；需要事务协调、跨分区提交和消费者隔离级别共同成立。

### KAFKA-008：Replication 与一致性（P1）

- Partition Leader / Follower。
- ISR Membership。
- High Watermark / Log End Offset。
- Replica Fetch / Catch-up。
- Leader Epoch。
- Preferred Leader / Election。
- min.insync.replicas 与 acks=all。
- Fencing 旧 Leader。
- 副本故障、恢复和数据截断规则。

### KAFKA-009：Retention / Compaction（P1）

- Time / Size Retention。
- Delete Retention Policy。
- Log Compaction。
- Tombstone / Delete Retention。
- Cleaner 与 Segment 管理。
- Compaction 与消费者 Offset 的相互作用。
- Remote / Tiered Storage（后续范围）。

### KAFKA-010：安全与管理（P2）

- SASL 机制支持范围。
- Topic / Group / Cluster ACL。
- Quota。
- Admin API 扩展。
- Metrics / Consumer Lag / Request Latency。

---

# 5. Pulsar：功能差异与未完成项

## 5.1 Pulsar 原生语义清单

| 能力 | 原生含义 | 与 Kafka 的区别 |
|---|---|---|
| Tenant | 顶级租户隔离 | Kafka 通常通过 ACL / 集群边界实现，不是同等原生对象 |
| Namespace | Topic 策略和资源管理域 | Kafka Topic 是主要管理对象 |
| Topic | Producer / Subscription 的逻辑消息流 | 一般 Topic 可有多个命名 Subscription |
| Subscription | 独立消费视图 | 不同于一个 Consumer Group 的 Partition 分配结构 |
| Cursor | Subscription 消费位置和 ACK 状态 | 不是 Kafka 单一 committed Offset |
| Exclusive | 单消费者独占 | 不能与 Shared 等价 |
| Failover | 主备消费者接管 | 需要定义活动消费者和备用消费者切换 |
| Shared | 多消费者竞争消息 | 一般不保证同一 Subscription 下的全局顺序 |
| Key_Shared | 按 Key 分发并维持 Key 级顺序 | 类似按 Key 分组，但 ACK / Cursor 仍是 Pulsar 语义 |
| Individual ACK | 单条消息确认 | 可产生非连续 ACK 状态 |
| Cumulative ACK | 确认某条及之前消息 | 受 Subscription Type 限制 |
| Reader | 以位置读取，不等同于普通 Subscription Consumer | 类似显式读取游标，但 API 与状态规则不同 |
| Message ID | Ledger / Entry / Partition / Batch 位置 | 不等同 Kafka Offset |
| Storage | Managed Ledger + BookKeeper | Broker 不需要自己承担所有持久化节点职责 |
| Tiered Storage | 热数据迁往对象/冷存储 | 需要消息 ID、Ledger 生命周期和可读位置连续性 |
| Transactions | 跨 Topic / Partition 原子事务 | 不等同 Kafka Transaction Protocol |
| Schema | Topic Schema / Schema Version | 不是单纯 Headers / Properties |
| Geo-replication | Cluster 间消息复制 | 不等同单集群 Raft 副本 |

## 5.2 UnifiedMQ 当前状态

### 已证实

- `broker_proto_pulsar` 独立 Application、Supervisor 和 Listener 已接入。
- 真实第三方 Pulsar 客户端兼容测试记录为 **8/8**。
- 架构文档明确存在 `pulsar_subscriptions` 进程，用于游标及未 ACK 集合管理。
- 五协议统一认证已接入统一身份表。
- Pulsar Message Properties 接入 UnifiedMQ 的 `ce-*` 事件属性通道。
- 项目 README 明确说明：支持 Pulsar Wire Protocol 不等于实现 Pulsar 原生完整架构，也不要求首期集成 Apache BookKeeper。
- README 提出 Erlang WAL / Segment + Raft 复制为优先验证方案，并把集成 BookKeeper 列为另一个会引入 Java 运行时和运维复杂度的方案。

### 设计目标 / 有结构证据

- Pulsar 二进制协议使用 Protobuf 命令帧。
- 项目设计文档要求考虑 Producer、Consumer、Lookup、ACK、Cursor、Flow、Redelivery。
- 订阅抽象明确列出 Exclusive、Shared、Failover、Key_Shared。
- 独立 `pulsar_subscriptions` 进程说明 Cursor 和 Unacked 已经成为独立运行时状态，不只是普通 Topic Subscribe。

### 未见证据 / 应列为重点核验项

- 四种 Subscription Type 是否均已完整实现、各自并发及顺序约束是否通过测试。
- Partitioned Topic 元数据、自动发现和跨分区消费。
- Lookup / Redirect / Topic Ownership 与 Broker 迁移。
- Individual ACK / Cumulative ACK 的完整边界与崩溃恢复。
- ACK Timeout、Negative ACK 延迟、重投递策略、Dead Letter Policy。
- Flow Permit、Receiver Queue、Consumer Stats 和背压完整语义。
- Reader / Seek / Reset Cursor / Initial Position。
- Batch Message、Chunked Message、Compression 和批次 ACK。
- Message Deduplication、Producer Sequence ID。
- Schema Registry / Schema Version / Schema Compatibility。
- Transaction Coordinator 以及跨 Topic / Partition 原子事务。
- Persistent / Non-persistent Topic 的完整差异。
- Retention、Backlog Quota、Topic Compaction、消息 TTL。
- BookKeeper Client / Managed Ledger / Ledger Rollover / Ensemble / Quorum / Ack Quorum。
- Tiered Storage、S3/GCS 等对象存储归档。
- Geo-replication、Replication Cursor 和跨集群冲突处理。

**判断：** UnifiedMQ 选择的是“Pulsar Wire Protocol 接入 + 自研 Erlang 存储内核”的路线，而不是直接复制 Pulsar Broker + BookKeeper 原生架构。这是合理的技术取舍，但必须在产品文档中明确兼容边界。当前有 Subscription Cursor / Unacked 状态组件及真实 Java 客户端测试证据，尚不能据此宣称完整 Pulsar 语义兼容。

## 5.3 Pulsar 后续需求

### PULSAR-001：协议协商和连接状态机（P0）

- CONNECT / CONNECTED。
- AUTH_CHALLENGE / AUTH_RESPONSE（按目标认证方式）。
- PING / PONG。
- 协议版本与 Feature Negotiation。
- Request ID / Response 关联。
- Error / ServerError 映射。
- Keepalive、连接超时、重连和连接迁移。
- Frame Length / Payload / Checksum / Batch 解析。

### PULSAR-002：Lookup 与 Topic Metadata（P0）

- LOOKUP。
- PARTITIONED_METADATA。
- Topic / Partition 命名及规范化。
- Persistent / Non-persistent Topic 区分。
- Redirect / Connect To Broker。
- Topic Owner / Epoch / Fencing。
- Partitioned Topic 元数据一致性。

### PULSAR-003：Producer / Send（P0）

- PRODUCER 建立与 Producer Name / ID 管理。
- SEND / SEND_RECEIPT。
- Sequence ID 与去重窗口。
- Message ID 生成与编码。
- Properties / Key / Ordering Key。
- Event Time / Replication Metadata。
- Batching / Compression / Chunking（分级实现）。
- Broker Publish ACK 与可靠性等级绑定。

### PULSAR-004：Subscription 类型（P0）

- Exclusive：只允许一个 Consumer。
- Failover：主消费者接收，主节点失效时备用接管。
- Shared：多个 Consumer 竞争消费，单条消息只投递给其中一个消费者。
- Key_Shared：相同 Key 尽量固定到同一 Consumer，保持 Key 级顺序。
- Consumer 增减、断连及重新加入时的 ownership 变更。
- 明确每种类型的 ACK 约束、顺序保证和 Rebalance 行为。

### PULSAR-005：ACK / Cursor / Redelivery（P0）

- Individual ACK。
- Cumulative ACK（仅在允许的 Subscription Type 下）。
- Negative ACK。
- ACK Timeout。
- Unacked Set。
- Redelivery Count / Redelivery Schedule。
- Dead Letter Topic / Max Redelivery。
- Cursor 持久化、快照和恢复。
- 多消费者并发 ACK 的连续进度计算。
- Consumer 重启、Broker 重启和 Leader 切换后的恢复。

### PULSAR-006：Flow Control（P0）

- FLOW Permit。
- Receiver Queue Size。
- Consumer Pending Messages。
- Max Unacked Messages。
- Producer Pending Queue / Send Timeout。
- Slow Consumer / Slow Storage 处理。
- Permit 与实际可投递额度一致。
- 控制层和数据层背压，防止 mailbox 无限增长。

### PULSAR-007：Reader / Replay（P1）

- Reader 创建。
- Message ID 起始位置。
- Earliest / Latest / Specific Message ID。
- Seek / Reset Cursor。
- Reader 与 Subscription Consumer 的语义隔离。
- Reader 是否推进 Subscription Cursor 必须有明确规则。

### PULSAR-008：Partitioned Topic（P1）

- Partitioned Metadata。
- 多 Partition Producer。
- Key / Ordering Key 路由。
- 多 Partition Consumer。
- Subscription 跨 Partition Cursor。
- 某 Partition 暂不可用时的错误与恢复。

### PULSAR-009：Retention / Backlog / Compaction（P1）

- Message TTL。
- Retention Time / Size。
- Backlog Quota。
- Subscription 删除与消息保留规则。
- Topic Compaction。
- 非持久 Topic 的快速投递与消息丢失语义。
- Retention 与 Cursor、未 ACK 状态之间的一致性。

### PULSAR-010：Schema（P1）

- Schema 注册及版本管理。
- Schema Type：String / JSON / Avro / Protobuf 等（按目标版本选取）。
- Schema ID / Version 元信息。
- Producer / Consumer Schema 协商。
- Compatibility Strategy。
- Schema 错误处理和权限控制。

### PULSAR-011：Transactions（P2）

- Transaction Coordinator。
- Transaction ID / Timeout。
- Add Produced Partition。
- Add Acknowledged Subscription。
- Commit / Abort。
- 跨 Topic / Partition 原子性。
- 事务日志持久化与恢复。
- 事务中的 ACK 与普通 ACK 区分。

### PULSAR-012：原生存储/生态扩展（P2）

- 评估继续使用 Erlang WAL / Segment + Raft，还是增加 BookKeeper 后端。
- 如果不实现 BookKeeper，必须定义 Pulsar 协议兼容与存储架构兼容的边界。
- Tiered Storage。
- Geo-replication。
- Namespace Policy。
- Admin REST API。
- Pulsar Functions / IO / SQL 不属于基础 Wire Protocol，单独评估是否纳入产品范围。

---

# 6. 三种协议的统一语义映射要求

## 6.1 统一消息记录不能吞掉协议状态

UnifiedMQ 可以共用一份 Message Record，但必须保留协议原生信息。建议分为：

```text
UnifiedMessage
  ├── message_id                 # Broker 内部唯一标识
  ├── tenant_id                  # 服务端认证得到的租户
  ├── payload                    # 原始负载
  ├── headers                    # 通用属性
  ├── created_at                 # Broker 时间
  ├── persistence_class          # memory / local-durable / replicated
  ├── source_protocol            # nats / kafka / pulsar / ...
  ├── native_metadata            # 协议原生元数据（保留）
  └── storage_position           # 内部 partition + offset
```

`native_metadata` 必须是有版本、可持久化、可校验的类型化结构，不得仅依靠随意拼接的字符串 Header。

## 6.2 三种协议独立的消费状态

```text
NATS Core:
  live subscription match -> delivery
  # 不创建持久 cursor

NATS JetStream:
  stream_sequence
  consumer_config
  delivered_consumer_sequence
  ack_floor
  pending_ack_set
  redelivery_state

Kafka:
  group_id
  member_id
  generation_id
  topic_partition_assignment
  committed_offset
  partition_leader_epoch

Pulsar:
  subscription_name
  subscription_type
  consumer_id / owner
  mark_delete_position / read_position
  individual_ack_set
  unacked_messages
  redelivery_state
```

以上是语义模型建议，具体结构应以协议版本和现有 ADR 为准。

## 6.3 统一内核可以复用的能力

- Append-only WAL / Segment。
- 内部 Partition / Offset。
- 消息正文与元数据存储。
- Group Commit。
- 消息过期和保留执行器。
- 复制和故障恢复设施。
- 统一 ACL / Tenant / Metrics / Tracing。
- 通用 Backpressure / Resource Quota。

## 6.4 不能强行统一的能力

| 原生概念 | 不能简单替换为 |
|---|---|
| NATS Subject Wildcard | 普通 Topic 字符串相等匹配 |
| NATS Queue Group | Kafka Consumer Group（成员与分配语义不同） |
| NATS Inbox / Reply | 普通持久化 Queue |
| JetStream Consumer ACK | Kafka Offset Commit |
| Kafka Partition Offset | Pulsar Message ID / Cursor |
| Kafka ISR / High Watermark | NATS Stream Consumer ACK |
| Pulsar Subscription Type | 统一 `competing=true/false` |
| Pulsar Individual ACK Set | 仅一个连续 Offset 水位 |
| Pulsar Managed Ledger | 普通单文件 WAL |

## 6.5 跨协议桥接规则

跨协议投递必须由明确的 Mapping Policy 决定，默认不得静默降级：

- Kafka Partition / Offset 不等同 Pulsar Message ID。
- NATS Core 消息转入持久化 Stream 后，必须明确“持久化从进入 UnifiedMQ 的时间点开始”，不能伪造原始历史。
- JetStream ACK 转成 Kafka Offset Commit 时，应计算可提交连续前缀，不能把单条 ACK 直接推进 Offset。
- Pulsar Individual ACK 转 Kafka Offset 时，必须等前序消息完成后推进提交水位。
- Kafka Record Key 映射至 Pulsar Ordering Key / NATS Subject 时，必须定义 Key 丢失或路由改变的处理。
- Pulsar Subscription 的多消费者策略映射至 NATS Queue Group 时，必须声明顺序和重投递语义差异。
- 无法保持的语义通过转换报告显式呈现：`losses`、`degradations`、`unsupported_features`。

---

# 7. 需求优先级和实施阶段

## 7.1 阶段 A：协议基础兼容收口

目标：把“真实客户端能连通”提升到“协议基础流程可明确声明”。

| 任务 | 优先级 | 验收重点 |
|---|---|---|
| NATS Protocol / Subject / Wildcard | P0 | 命令覆盖、Subject 边界、错误帧 |
| NATS Queue Group | P0 | 同组竞争、普通订阅广播、成员变化 |
| NATS Request / Reply | P1 | Inbox、超时、多响应、无响应 |
| Kafka API Version Matrix | P0 | ApiVersions、Produce、Fetch、Metadata、错误码 |
| Kafka Consumer Group 基础 | P0 | FindCoordinator、Join/Sync、Heartbeat、Offset Commit |
| Pulsar Wire Protocol Matrix | P0 | 命令版本、Protobuf 帧、请求关联、Lookup |
| Pulsar Subscription Type | P0 | 四种类型的实际支持范围和行为测试 |
| 三协议 ACL / Tenant | P0 | 认证主体、资源 ACL 和跨租户隔离 |

## 7.2 阶段 B：消息可靠性和数据语义

目标：完善持久化、消费进度、重试、恢复及复制契约。

| 任务 | 优先级 | 验收重点 |
|---|---|---|
| Kafka Offset 连续提交与恢复 | P0 | Out-of-order ACK 不得跳过未完成消息 |
| Kafka Group Generation / Fencing | P0 | 旧成员不能提交新 Generation 的 Offset |
| Pulsar Individual / Cumulative ACK | P0 | ACK Set、Cursor 和恢复一致 |
| Pulsar Negative ACK / Timeout Redelivery | P0 | 重投递次数、退避和 DLQ |
| NATS JetStream Stream / Consumer MVP | P0（需单独立项） | Stream 持久化、Consumer Cursor、ACK、Replay |
| NATS PubAck / Dedup | P1 | 去重窗口、消息确认和故障恢复 |
| Kafka Producer Idempotence | P1 | Producer ID / Epoch / Sequence |
| Kafka Replication Semantics | P1 | Leader / ISR / High Watermark |
| Pulsar Message ID / Batch / Flow | P1 | 批次和流控边界 |

## 7.3 阶段 C：高级能力与生态兼容

| 任务 | 优先级 | 验收重点 |
|---|---|---|
| NATS JetStream 高级 Retention / Mirror / Source | P1/P2 | 配置、恢复、复制及第三方客户端 |
| Kafka Transactions / EOS | P1 | 跨分区事务和 Read Committed |
| Kafka Log Compaction | P1 | Key 最新值、Tombstone、Cleaner |
| Pulsar Schema | P1 | Schema Version / Compatibility |
| Pulsar Transactions | P2 | 跨 Topic / Partition 原子提交 |
| Pulsar Tiered Storage | P2 | 冷热迁移和历史读取 |
| Pulsar Geo-replication | P2 | 跨集群复制与冲突/延迟策略 |
| 三协议完整 Admin / Observability | P1/P2 | 管理 API、Metrics、告警和运维 |

---

# 8. 测试与验收要求

## 8.1 NATS 测试矩阵

| 测试分类 | 必测场景 |
|---|---|
| Connection | INFO / CONNECT / PING / PONG / TLS / 认证失败 |
| Publish | PUB、HPUB、消息大小、Header 大小、非法长度 |
| Subscribe | SUB、UNSUB、SID、多个 Subject |
| Subject | 精确匹配、`*`、`>`、多层级和非法 Subject |
| Queue Group | 同组竞争、普通订阅广播、动态增减成员 |
| Request/Reply | Inbox、Reply Subject、超时、多响应、No Responders |
| Error | -ERR、未知命令、权限失败、慢消费者 |
| JetStream（若立项） | Stream CRUD、PubAck、Consumer、ACK、NAK、Replay、Retention、Restart |

## 8.2 Kafka 测试矩阵

| 测试分类 | 必测场景 |
|---|---|
| Negotiation | ApiVersions、多版本协商、未知 API |
| Metadata | Topic / Partition / Leader / Not Leader |
| Produce | 单条、批量、压缩、CRC、Header、Key、acks |
| Fetch | Offset、Max Bytes、High Watermark、错误 Offset |
| Consumer Group | 成员加入/离开、Heartbeat、Rebalance、Generation |
| Offset | Commit / Fetch、批量提交、乱序完成、恢复 |
| Idempotence | 重试、重复序列、Epoch fencing |
| Transaction | Commit / Abort、跨分区、Read Committed |
| Retention | 时间/容量删除、Compaction、Tombstone |
| Failure | Leader 宕机、Coordinator 宕机、Follower 落后、网络分区 |

## 8.3 Pulsar 测试矩阵

| 测试分类 | 必测场景 |
|---|---|
| Connection | CONNECT / CONNECTED、Auth、Ping/Pong、版本协商 |
| Lookup | Topic Lookup、Partition Metadata、Redirect |
| Producer | Producer、Send、Receipt、Sequence / Dedup |
| Consumer | Subscribe、四种 Subscription Type、多个 Consumer |
| ACK | Individual、Cumulative、Negative ACK、ACK Timeout |
| Cursor | 初始位置、恢复、Seek、并发 ACK |
| Flow | Permit、Receiver Queue、Pending、Slow Consumer |
| Batch | Batch、Compression、Chunk（如承诺） |
| Retention | TTL、Backlog Quota、Retention、Compaction |
| Failure | Consumer 断开、Broker 重启、分区/Owner 切换 |
| Advanced | Schema、Transactions、Tiered / Geo（纳入范围时） |

## 8.4 跨协议语义测试

- 一条消息通过 NATS / Kafka / Pulsar 入口写入，分别从三种协议读取。
- 验证 Payload 原始字节不变。
- 验证 Header、Key、Event Time、Message ID 的映射规则。
- 验证某协议 ACK 不会错误推进另一协议的消费位置。
- 验证两个协议对同一内部分区的订阅互不覆盖 Cursor。
- 验证租户 ACL 不会因协议转换而被绕过。
- 验证重复投递、消息过期、DLQ 和删除策略在跨协议路径中有清晰结果。
- 对每种不可映射语义输出转换报告，不允许静默吞字段。

## 8.5 测试证据要求

每一项协议能力必须同时保存：

1. 协议/客户端版本。
2. 真实客户端或官方 CLI 命令。
3. 请求/响应或消息帧日志。
4. 服务端日志和关键指标。
5. 成功与失败测试用例。
6. 重启/断网/超时测试（涉及持久状态时）。
7. 能力矩阵中的实现状态。

CT 用例数量、客户端冒烟通过数和协议覆盖率应分开统计。

---

# 9. 架构设计约束

## 9.1 Adapter 保留原生语义

- NATS Adapter 负责 Subject / SID / Queue Group / Inbox；如支持 JetStream，另负责 Stream / Consumer / ACK API。
- Kafka Adapter 负责 Topic / Partition / RecordBatch / Group Protocol / Offset / Producer Sequence / Transaction。
- Pulsar Adapter 负责 Topic / Subscription Type / Message ID / ACK / Cursor / Flow / Schema / Transaction。

## 9.2 内核行为接口不可只暴露一种队列抽象

建议内核提供可组合能力，而不是单一的 `publish(topic)` / `subscribe(topic)` API：

- Append Log：追加写入和读取。
- Partition Reader：按位置拉取。
- Subscription Cursor：消费位置维护。
- Delivery Tracker：投递与未确认状态。
- Ack Strategy：Individual / Cumulative / Offset Commit / No Ack。
- Dispatch Strategy：Broadcast / Competing / Key Affinity / Failover。
- Retention Strategy：Time / Size / Interest / WorkQueue / Cursor-aware。
- Replay Strategy：Latest / Earliest / Sequence / Timestamp。
- Replication Policy：Local / Quorum / Async Cross-Cluster。

这些应作为能力组合；不意味着所有协议都必须启用全部能力。

## 9.3 协议能力声明

每种协议需要公开：

- 支持的协议版本。
- 支持的 API / Command / Request Key。
- 支持的 Subscription / Consumer 类型。
- ACK / Retry / Redelivery 的边界。
- 持久化、复制和恢复保证。
- 明确不支持的特性。
- 当前限制和兼容性测试客户端版本。

不得只用“支持 NATS/Kafka/Pulsar”这样的无范围描述。

---

# 10. 与原生产品的兼容目标建议

| 协议 | 第一目标 | 第二目标 | 不建议首期承诺 |
|---|---|---|---|
| NATS | Core NATS 协议、Subject、Queue Group、Request/Reply | JetStream Stream / Consumer / ACK / Replay | 全部 NATS 运维生态、跨 Account 全功能和所有 JetStream 高级特性 |
| Kafka | 常见 Producer / Consumer、Metadata、Group、Offset | 幂等 Producer、Transactions、Retention / Compaction | 全量 Kafka API、KRaft Controller 行为和所有 Connect / Streams 生态 |
| Pulsar | Java Client 常见 Producer / Consumer / Lookup / Subscription / ACK / Flow | Partitioned Topic、Schema、Transactions、Retention | BookKeeper 原生集群等价、全部 Tiered Storage / Geo / Functions / IO 生态 |

这里的目标是有边界的协议兼容，不是复制各自完整产品的控制面和生态。

---

# 11. 参考资料

## 11.1 UnifiedMQ 仓库

- [UnifiedMQ Gitee 仓库](https://gitee.com/wanwuliandong/unified-mq)
- [README.md](https://gitee.com/wanwuliandong/unified-mq/blob/main/README.md)
- [docs/architecture.md](https://gitee.com/wanwuliandong/unified-mq/blob/main/docs/architecture.md)
- [docs/roadmap.md](https://gitee.com/wanwuliandong/unified-mq/blob/main/docs/roadmap.md)
- [docs/protocol-compatibility.md](https://gitee.com/wanwuliandong/unified-mq/blob/main/docs/protocol-compatibility.md)
- [progress.md](https://gitee.com/wanwuliandong/unified-mq/blob/main/progress.md)

## 11.2 NATS 官方资料

- [Core NATS](https://docs.nats.io/nats-concepts/core-nats)
- [Core NATS Deep Dive](https://docs.nats.io/learn/core-nats/)
- [Queue Groups](https://docs.nats.io/learn/core-nats/queue-groups)
- [Request-Reply](https://docs.nats.io/concepts/request-reply)
- [JetStream](https://docs.nats.io/concepts/jetstream)
- [JetStream Deep Dive](https://docs.nats.io/learn/jetstream/)
- [NATS Client Protocol](https://docs.nats.io/reference/protocols/client)

## 11.3 Kafka 官方资料

- [Kafka Protocol](https://kafka.apache.org/protocol.html)
- [Kafka Basic Operations](https://kafka.apache.org/43/operations/basic-kafka-operations/)
- [Kafka Producer Configuration](https://kafka.apache.org/43/generated/producer_config.html)
- [Kafka Topic Configuration](https://kafka.apache.org/43/generated/topic_config.html)
- [Kafka Consumer Offset Tracking](https://kafka.apache.org/43/implementation/distribution/)

## 11.4 Pulsar 官方资料

- [Pulsar Overview](https://pulsar.apache.org/docs/5.0.x/concepts-overview/)
- [Pulsar Messaging](https://pulsar.apache.org/docs/5.0.x/concepts-messaging/)
- [Pulsar Architecture Overview](https://pulsar.apache.org/docs/next/concepts-architecture-overview/)
- [Pulsar Transactions](https://pulsar.apache.org/docs/next/transactions/)
- [Pulsar Geo-Replication](https://pulsar.apache.org/docs/5.0.x/concepts-replication/)
- [Pulsar Scalable Topics](https://pulsar.apache.org/docs/5.0.x/concepts-scalable-topics/)

---

# 12. 最终评估与研发建议

## 12.1 三种协议不能采用同一套“完成度”标准

- **NATS：** 首先要把 Core NATS 与 JetStream 分开评估。Core NATS 的完成不代表具备持久化消息能力；JetStream 是一套独立的 Stream / Consumer / ACK / Retention 语义。
- **Kafka：** 首先要确认 Consumer Group / Partition / Offset 的一致性。随后才是 Idempotent Producer、Transaction、Replication 和 Compaction。仅能 Produce / Fetch 不足以替代 Kafka。
- **Pulsar：** 首先要确认四种 Subscription Type、Cursor、Individual / Cumulative ACK、Flow Control 和重投递。BookKeeper 是否集成是架构取舍，但如果不集成，必须用 UnifiedMQ 自己的存储和复制机制兑现兼容承诺。

## 12.2 建议优先级

建议按以下顺序实施：

1. **Kafka Consumer Group / Offset 语义收口**：这是 Kafka 兼容最关键的状态机，错误提交会导致数据重复或跳过。
2. **Pulsar Subscription / ACK / Cursor / Redelivery 收口**：尤其 Individual ACK 集合、累计进度和恢复边界。
3. **NATS Core 语义完整性**：Subject Wildcard、Queue Group、Inbox / Request-Reply、Header 和权限边界。
4. **NATS JetStream MVP 立项**：如果产品目标确实需要替代持久化 NATS，则单独建设 Stream / Consumer 子系统；否则在产品说明中明确仅支持 Core NATS。
5. **Kafka 幂等与事务**：完成基础组协调和 Offset 后再做，避免先做出不可靠的“Exactly Once”承诺。
6. **Pulsar Schema / Transactions / Tiered Storage**：按目标客户和兼容场景分期，不要在核心消费语义未冻结前提前扩张。

## 12.3 最终结论

UnifiedMQ 的价值在于统一消息内核和多协议接入，而不是把 NATS、Kafka、Pulsar 的 API 全部翻译成同一个 Pub/Sub 接口。后续最重要的工程原则是：

> **统一存储与复制设施，保留协议的地址模型、消费者模型、确认模型、游标模型和保留模型。**

只有这样，UnifiedMQ 才能既做到多协议互通，又不破坏各协议客户端原本依赖的消息语义。
