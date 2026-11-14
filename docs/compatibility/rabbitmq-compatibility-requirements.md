# UnifiedMQ 需求规格说明书

## RabbitMQ 兼容能力建设与统一消息 Broker 内核增强

| 文档属性 | 内容 |
|---|---|
| 项目名称 | UnifiedMQ |
| 文档类型 | 产品需求规格说明书（PRD / SRS） |
| 文档版本 | V1.0 |
| 编制日期 | 2026-11-12 |
| 核心技术栈 | Erlang / OTP |
| 核心参考项目 | rabbitmq-server、ra、khepri |
| 建设方向 | RabbitMQ 兼容、可靠消息内核、多协议统一 Broker |

---

## 1. 文档目的与范围

### 1.1 文档目的

本文档用于指导 UnifiedMQ 后续研发，明确 RabbitMQ 兼容能力建设的三个阶段、Queue 内核与消息可靠性要求、多协议原生语义约束，以及需要重点研究的 RabbitMQ 参考项目。本文档可作为：

- 研发需求和技术设计的输入；
- GitHub / Gitee Issue、Milestone 和任务拆分依据；
- AI Coding Agent 执行任务时的需求基线；
- 测试计划、兼容性验证和阶段验收依据。

### 1.2 建设范围

本项目不是逐行复刻 RabbitMQ，也不以达到相同代码行数为目标。建设重点是吸收 RabbitMQ 在协议语义、队列内核、消息可靠性和集群机制方面经过验证的设计，并与 UnifiedMQ 的多协议目标结合。

建设范围分为三个阶段：

| 阶段 | 名称 | 核心目标 |
|---|---|---|
| 第一阶段 | RabbitMQ 基础兼容 | 完善 AMQP 0-9-1 常用基础语义，使主流 RabbitMQ 客户端可以连接并完成典型业务流程 |
| 第二阶段 | RabbitMQ 核心能力兼容 | 建设可靠 Queue 内核、消息持久化、复制、高可用和故障恢复能力 |
| 第三阶段 | RabbitMQ 生态兼容 | 完善管理 API、Web 管理、监控、策略、扩展和运维生态 |

### 1.3 非目标

以下事项不作为本需求的默认承诺：

1. 不要求复制 RabbitMQ 的内部模块划分、代码结构或全部实现细节。
2. 不承诺第一阶段即可替换所有 RabbitMQ 生产集群。
3. 不默认要求兼容 RabbitMQ 全部历史插件及其私有接口。
4. 不将所有协议强制转换为简单的 Topic / Pub-Sub 模型。
5. 不在缺乏事务边界和端到端验证的情况下宣称 Exactly Once。
6. 不把“客户端能够连接”视为“协议完全兼容”或“生产级可替代”。

---

## 2. 项目背景与总体目标

### 2.1 项目背景

UnifiedMQ 定位为基于 Erlang/OTP 的多协议统一消息 Broker。其长期方向是通过协议适配层接入 AMQP、MQTT、Kafka、NATS、Pulsar、WebSocket 等协议，并在共享的消息基础设施之上提供路由、存储、投递、确认、复制、管理和监控能力。

与单协议 Broker 不同，UnifiedMQ 必须同时满足两类要求：

- **统一性：** 连接、身份、基础消息元数据、存储、可观测性和集群资源能够复用。
- **原生性：** 各协议的资源模型、路由规则、消费位置、确认流程、会话状态和投递语义不能因统一抽象而被抹平。

### 2.2 总体建设目标

1. 完善 AMQP 0-9-1 的基础协议和常见 RabbitMQ 使用语义。
2. 建设可独立演进的 Queue Engine，统一管理队列生命周期、消息状态和消费者调度。
3. 建设 WAL、消息存储、索引、恢复和空间回收机制。
4. 建立 Publisher Confirm、ACK、重投递和持久化之间的一致性规则。
5. 借鉴 `ra` 建设基于 Raft 的消息复制和高可用队列能力。
6. 研究 `khepri` 的元数据树、变更通知和分布式元数据管理思路。
7. 建立协议原生语义适配模型，避免所有协议被简化为统一 Pub/Sub。
8. 提供满足生产运维需要的 Management API、Web UI、监控、策略和扩展能力。

### 2.3 核心设计原则

#### 原则 A：协议适配与消息内核分离

协议层负责连接、编解码、协议状态机、协议资源模型和原生确认语义。消息内核负责消息生命周期、持久化、调度和复制。协议层不得直接承担底层 Queue 的全部生命周期管理。

#### 原则 B：共享基础设施，不强制统一语义

允许多个协议复用存储、日志、复制、鉴权和观测能力，但协议资源模型与状态机应由各自适配层维护，必要时通过专用引擎实现。

#### 原则 C：可靠性定义先于实现

每种消息确认必须定义清楚其含义：接收、进入内存、写入本地日志、本地持久化、达到复制提交条件，还是已经交付给消费者。禁止在实现和文档中混淆这些状态。

#### 原则 D：单机与集群架构保持一致

单机模式应使用与集群模式兼容的核心接口。复制层可以在单机模式退化为本地提交实现，但消息状态机和上层 API 不应因此产生两套互不兼容的逻辑。

#### 原则 E：以可验证行为作为完成标准

所有关键能力必须具有协议测试、状态机测试、故障注入或恢复测试。仅有代码实现、编译通过或单个客户端成功连接，不构成阶段验收。

---

## 3. 总体逻辑架构要求

### 3.1 逻辑架构

```text
                         Client Layer
                              |
       +----------+-----------+-----------+----------+
       |          |           |           |          |
      AMQP       MQTT       Kafka        NATS      Pulsar
       |          |           |           |          |
       +----------+-----------+-----------+----------+
                              |
                     Protocol Adapter Layer
                              |
                  Authentication / Authorization
                              |
              +---------------+----------------+
              |                                |
       Protocol-native Model              Common Message Model
              |                                |
              +---------------+----------------+
                              |
                    Message / Queue Kernel
                              |
       +----------------------+----------------------+
       |                      |                      |
   Queue Engine          Router Engine          Consumer Engine
       |                      |                      |
       +----------------------+----------------------+
                              |
                       Storage Layer
                              |
                 WAL / Message Store / Index
                              |
                    Replication Interface
                              |
                         Raft Layer
                              |
              Cluster / Metadata Management
                              |
                    Management / Metrics / UI
```

### 3.2 架构分层职责

| 层级 / 模块 | 主要职责 | 设计约束 |
|---|---|---|
| Protocol Adapter | 协议连接、编解码、状态机、协议资源和确认流程 | 不将协议语义无条件转换为 Pub/Sub |
| Authentication / Authorization | 身份认证、资源权限和租户隔离 | 权限检查应与协议资源模型关联 |
| Protocol-native Model | Exchange、Queue、Topic、Partition、Subscription、Cursor 等协议概念 | 保留协议原生状态及生命周期 |
| Common Message Model | 通用消息正文、基础头信息、追踪上下文等 | 不承载会改变协议语义的隐式转换 |
| Queue Engine | Queue 生命周期、消息状态、消费调度 | 与 AMQP 实现解耦，可被其他适配层按需使用 |
| Router Engine | 路由匹配、Binding、订阅匹配和投递目标选择 | 允许不同协议使用不同路由策略 |
| Consumer Engine | 消费者注册、投递、Prefetch、ACK 跟踪 | 支持协议专属确认适配 |
| Storage Layer | WAL、消息正文、索引、回收与恢复 | 明确持久化边界和数据格式版本 |
| Replication Layer | 日志复制、提交、快照和副本恢复 | 与元数据一致性职责分离 |
| Cluster / Metadata | 节点、资源元数据、成员关系、配置和状态 | 评估 `ra`、`khepri` 与现有 Mnesia 的职责边界 |
| Management / Metrics | 管理 API、Web UI、Metrics、Tracing | 管理接口与协议数据面解耦 |

### 3.3 关键架构约束

1. AMQP Queue、Kafka Partition、Pulsar Subscription、MQTT Session 等不能简单映射为同一种内部对象后丢弃原始属性。
2. Common Message Model 只统一通用数据载体，不负责替代协议原生投递保证。
3. 消息存储、消息索引和消息状态应有明确的数据模型及恢复规则。
4. Raft 元数据复制与消息数据复制应作为不同职责设计；即便底层复用 `ra`，也必须有清晰的 Group、日志和状态机边界。
5. Queue Leader、Metadata Leader、节点管理角色不应混为一个概念。
6. 所有管理操作都应定义权限、审计、并发修改和失败回滚行为。

---

## 4. 第一阶段：RabbitMQ 基础兼容

### 4.1 阶段目标

完善 AMQP 0-9-1 基础协议闭环，使常见 RabbitMQ 客户端能够连接 UnifiedMQ，完成连接、交换机和队列声明、绑定、消息发布、消费及确认等典型工作流。

第一阶段重点是协议层与基础队列语义。此阶段不代表已具备 RabbitMQ 全部持久化、高可用和管理生态能力。

### 4.2 AMQP Connection 与 Channel

#### REQ-AMQP-001：Connection 管理（P0）

- 支持 AMQP 0-9-1 协议头和连接握手。
- 支持 Start / Start-Ok、Tune / Tune-Ok、Open / Open-Ok、Close / Close-Ok。
- 支持 Heartbeat 协商、发送、接收和超时处理。
- 支持连接关闭、TCP 异常、认证失败和资源释放。
- 支持连接参数协商及最大帧尺寸处理。
- 支持 TLS 接入；证书校验和配置方式应有文档。

**验收标准：** 官方或主流客户端能够完成连接建立和正常关闭；Heartbeat 超时能够释放连接资源；连接异常不影响其他连接。

#### REQ-AMQP-002：Channel 管理（P0）

- 支持 Channel Open / Open-Ok、Close / Close-Ok。
- 支持单连接多 Channel。
- 每个 Channel 具有独立协议状态、Delivery Tag 和 Confirm 序列状态。
- 支持 Channel 级异常隔离。
- Channel 关闭时清理关联消费者，并按协议规则处理未确认消息。
- 支持最大 Channel 数量限制。

**验收标准：** 同一 Connection 上多个 Channel 可以独立执行发布和消费；单个 Channel 异常不会无条件关闭整个 Connection。

### 4.3 Exchange 与 Routing

#### REQ-AMQP-003：Exchange 管理（P0）

支持以下标准 Exchange 类型：

- Direct Exchange
- Fanout Exchange
- Topic Exchange
- Headers Exchange

支持操作：Declare、Delete、Bind、Unbind、Passive Declare。

支持属性：Durable、Auto Delete、Internal、Arguments。

**验收标准：** 四种 Exchange 类型按照各自匹配规则路由；声明属性不一致时按协议要求返回正确结果或异常；删除操作正确处理关联 Binding。

#### REQ-AMQP-004：Routing 行为（P0）

- Direct：Routing Key 精确匹配。
- Fanout：向所有绑定目标分发。
- Topic：支持 `*` 和 `#` 通配符及多段 Routing Key 匹配。
- Headers：支持按 Header 条件和 `x-match` 规则匹配。
- 支持默认 Exchange 的队列名称路由行为。
- 支持 Mandatory 发布及 Basic.Return 未路由消息返回。

**验收标准：** 建立覆盖正常匹配、无匹配、多重匹配、空值和边界条件的路由测试集。

### 4.4 Queue 管理与消息收发

#### REQ-AMQP-005：Queue 管理（P0）

支持 Queue Declare、Delete、Bind、Unbind、Purge 和 Passive Declare。

支持属性：Durable、Exclusive、Auto Delete、Arguments。

必须正确处理：

- Durable Queue 的元数据恢复。
- Exclusive Queue 与所属 Connection 的关系。
- Auto Delete Queue 的删除条件。
- Queue 删除时的消费者、Binding 和消息资源清理。
- 被动声明不存在的资源时的协议异常。

#### REQ-AMQP-006：消息发布（P0）

支持 Basic.Publish、消息内容分帧和常用 Basic Properties：

- Delivery Mode
- Content Type / Content Encoding
- Headers
- Correlation ID
- Reply To
- Message ID
- Timestamp
- Type
- App ID
- Priority
- Expiration

支持 Mandatory 未路由返回。对 Immediate 等历史或不支持特性，应按 AMQP 规范定义明确的处理行为，不得静默模拟成功。

#### REQ-AMQP-007：消费者管理（P0）

支持 Basic.Consume、Basic.Cancel、Basic.Deliver、Basic.Get。

支持 Consumer Tag、No-Ack、Exclusive Consumer 和消费者生命周期管理。

消费者断开、Channel 关闭或 Connection 关闭时，应清理消费者注册并按消息确认状态执行恢复。

#### REQ-AMQP-008：ACK / NACK / Reject（P0）

支持 Basic.Ack、Basic.Nack、Basic.Reject、Multiple 和 Requeue。

要求：

- Delivery Tag 按 Channel 作用域管理。
- ACK 只能确认当前 Channel 上合法的投递。
- Multiple ACK/NACK 的范围必须正确。
- Requeue 后消息重新进入可投递状态。
- Requeue=false 的消息按死信策略处理；未配置死信时按协议规则处理。
- 消费者断开后，Unacked 消息应恢复为可投递状态。

#### REQ-AMQP-009：QoS / Prefetch（P0）

支持 Basic.Qos 的 Prefetch Count、Prefetch Size 和 Global 标志对应语义。

必须限制未确认消息数量，防止慢消费者无限积累消息，并验证多消费者、多 Channel 下的作用域行为。

### 4.5 可靠性基础能力

#### REQ-AMQP-010：Publisher Confirm（P0）

支持 Confirm.Select、Confirm.Select-Ok、Basic.Ack、Basic.Nack、序列号跟踪和 Multiple Confirm。

必须定义 Confirm 的确认边界：

- 仅接收或进入内存；
- 写入本地 WAL；
- 本地持久化完成；
- 复制达到提交条件。

每种模式都必须有明确配置和文档。持久化队列场景不得在尚未达到约定持久化边界时提前确认。

#### REQ-AMQP-011：AMQP Transaction（P1）

评估并实现 Tx.Select、Tx.Commit、Tx.Rollback 及对应响应。

要求明确事务支持范围、事务内 Publish 与 ACK 的组合规则、持久化行为和故障后的提交结果。若阶段内不支持，必须返回符合规范的错误，不能伪装为事务成功。

### 4.6 消息策略能力

#### REQ-MSG-001：TTL（P1）

支持 Message TTL 和 Queue TTL，明确过期判定时机、过期清理方式、过期消息是否进入 DLX，以及过期消息的资源回收策略。

#### REQ-MSG-002：Dead Letter Exchange（P1）

支持死信交换机、死信 Routing Key 和死信头信息。覆盖 Reject/NACK 且不重新入队、TTL 过期、队列长度限制等触发条件。

#### REQ-MSG-003：Priority Queue（P1）

支持队列最大优先级配置和消息优先级调度。必须明确优先级与公平性、吞吐量及内存开销之间的权衡。

#### REQ-MSG-004：Queue 参数（P1）

评估并支持 Max Length、Max Length Bytes、Overflow Policy 等常用参数。未支持的参数应在声明时以明确定义的方式处理，不得忽略后返回成功。

### 4.7 身份认证与资源隔离

#### REQ-SEC-001：用户认证（P0）

支持用户管理、密码安全存储、认证失败处理和 TLS。禁止明文存储密码或将认证秘密写入普通日志。

#### REQ-SEC-002：Vhost（P0）

支持 Virtual Host 创建、删除和资源隔离。Exchange、Queue、Binding 和连接应归属明确的 Vhost。

#### REQ-SEC-003：资源权限（P0）

支持 Configure、Write、Read 权限，并将权限检查落实到 Exchange、Queue、Binding、Publish 和 Consume 等操作。

### 4.8 第一阶段交付物与验收

交付物：

1. AMQP 0-9-1 功能兼容矩阵。
2. 协议状态机和错误处理设计。
3. Exchange / Queue / Consumer 基础设计文档。
4. RabbitMQ 客户端兼容测试报告。
5. 不支持特性与已知限制清单。

最低验收：

- Connection / Channel 基础流程通过。
- Direct、Fanout、Topic、Headers Exchange 测试通过。
- Queue 声明、绑定、删除和清理测试通过。
- Publish / Consume / ACK / NACK / Requeue 测试通过。
- Prefetch 限制测试通过。
- Publisher Confirm 行为与文档定义一致。
- 至少覆盖 Go、Java、Python 中的主流 AMQP 客户端。
- 连接异常、消费者异常和未确认消息恢复测试通过。

---

## 5. 第二阶段：RabbitMQ 核心能力兼容

### 5.1 阶段目标

建设独立、可恢复、可扩展的 Queue Engine 和消息存储体系，并完善 Publisher Confirm、ACK、消息复制、Raft 提交和节点故障恢复。该阶段决定 UnifiedMQ 是否具备成为生产级可靠 Broker 的核心基础。

### 5.2 Queue 生命周期与内部模型

#### REQ-QUEUE-001：Queue 生命周期状态机（P0）

Queue 至少覆盖以下逻辑状态：

```text
Create -> Declared -> Active -> Running
                          |         |
                          |         +-> Paused
                          |         +-> Recovering -> Running
                          |         +-> Stopped
                          +---------------> Deleted
```

要求：

- 定义状态及允许的状态转换。
- 创建、启动、停止、恢复和删除操作具备幂等性。
- 元数据与运行时状态分离。
- 删除队列时有序释放消费者、索引、存储和复制资源。
- 节点恢复时能够判断 Queue 应进入 Running、Recovering 或不可用状态。
- 协议层声明、删除等操作必须通过 Queue 管理接口完成，不得直接修改内部结构。

#### REQ-QUEUE-002：消息状态模型（P0）

建议建立以下通用逻辑状态，最终实现时可以采用更精简的物理状态，但必须能够表达并恢复这些语义：

| 状态 | 含义 |
|---|---|
| Received | Broker 已接收消息 |
| Routed | 已完成路由并确定目标 |
| Persisting | 正在持久化 |
| Ready | 已提交并等待投递 |
| Delivered | 已向消费者发出投递 |
| Unacked | 已投递但尚未确认 |
| Acked | 已确认，可执行清理 |
| Requeued | 已重新进入可投递流程 |
| DeadLettered | 已进入死信处理流程 |
| Expired | 已过期 |
| Deleted | 已完成删除或回收 |

要求：

- 定义合法状态转换和禁止状态转换。
- 明确状态是在内存维护、日志重建还是单独持久化。
- 状态变更与消息索引更新保持一致。
- ACK、NACK、Requeue、TTL 和 DLX 操作可恢复。
- 重启和 Leader 切换后不得产生已确认消息重新投递或未确认消息永久丢失等状态错误。

#### REQ-QUEUE-003：Queue 内部数据结构（P0）

Queue Engine 至少需要管理：

- Queue Metadata；
- Ready Messages；
- Unacked Messages；
- Consumer Registry；
- Delivery Tracking；
- Message Index；
- Queue Policy；
- Queue Statistics；
- Recovery State。

要求 Ready 与 Unacked 有清晰的逻辑区分；支持消息定位、批量确认、重投递和过期清理；避免将全部积压消息长期保留在进程内存中。

#### REQ-QUEUE-004：消费者调度（P0）

支持：

- 多消费者竞争消费；
- Round Robin 或等效公平调度；
- Prefetch 限制；
- Consumer Priority；
- Consumer Cancel / Pause / Resume；
- 慢消费者识别及流控；
- 消费者异常退出后的 Unacked 恢复；
- 消费者增加、减少时的负载重新分配。

消费者调度应与消息存储实现解耦，并具有可替换策略接口。不得依赖无限增长的 Erlang Process Mailbox 作为消息积压机制。

#### REQ-QUEUE-005：内存与磁盘管理（P0）

支持内存水位、磁盘水位、队列长度和消息积压监控；支持消息分页、分段读取、批量清理和空间回收。单个 Queue 的持续积压不能无界消耗节点内存。

### 5.3 消息存储与恢复

#### REQ-STORAGE-001：WAL（P0）

完善 Write-Ahead Log：

- 顺序追加写入；
- Group Commit；
- 批量刷盘；
- Segment 滚动；
- CRC / 校验和；
- WAL 重放；
- 尾部撕裂识别；
- Segment 清理和压缩；
- 写入失败和磁盘满处理。

#### REQ-STORAGE-002：Message Store（P0）

实现独立的消息存储接口，覆盖消息正文、元数据、索引、定位、批量写入、批量读取、删除和空间回收。

消息正文与索引可以采用不同的物理组织方式，但必须定义关联标识、更新顺序和崩溃恢复规则。

#### REQ-STORAGE-003：持久化 Queue（P0）

- Durable Queue 元数据可恢复。
- Persistent Message 达到定义的持久化边界后才可确认。
- 重启后恢复 Queue、消息索引和 Ready 消息。
- 对 Unacked 消息执行符合语义的重新投递恢复。
- 能够识别并处理不完整写入和孤立索引。

#### REQ-STORAGE-004：恢复与一致性校验（P0）

覆盖正常关闭、进程异常退出、操作系统重启、磁盘写入失败、WAL 尾部损坏和数据文件损坏场景。提供数据目录检查、恢复日志和必要的人工修复指引。

### 5.4 Publisher Confirm、ACK 与消息可靠性

#### REQ-REL-001：Confirm 与提交边界绑定（P0）

Publisher Confirm 必须反映消息实际达到的提交条件。系统应区分并可观测以下事件：

1. Broker 收到 Publish；
2. 消息通过基础校验并完成路由；
3. 消息进入本地 WAL；
4. 本地持久化达到配置边界；
5. 消息复制达到提交条件；
6. 消息可被消费者读取。

Confirm 的触发点必须依据队列类型和持久化配置定义，不得在不同队列类型下悄然改变含义。

#### REQ-REL-002：ACK / NACK 与未确认消息（P0）

- 支持单条和批量 ACK。
- 支持 NACK、Reject 和 Requeue。
- 跟踪 Delivery Tag 与消费者会话。
- 处理消费者、Channel、Connection 故障。
- Leader 变更后恢复或重建未确认投递状态。
- 防止 ACK 重复执行造成消息状态损坏。

#### REQ-REL-003：投递语义定义（P0）

| 语义 | 要求 |
|---|---|
| At Most Once | 定义可能丢弃消息的确认边界和适用场景 |
| At Least Once | 支持故障重试和重新投递，允许消费者处理重复消息 |
| Exactly Once | 不作为通用跨协议默认保证；需要明确事务边界、去重范围和外部系统协作 |

应提供幂等键或去重扩展能力，但不能仅凭 Message ID 就宣称端到端 Exactly Once。

### 5.5 Raft 与复制队列

#### REQ-RAFT-001：Raft 基础能力（P0）

参考 `ra` 项目，明确 Raft Group 生命周期、Leader Election、日志追加、复制、Commit Index、Snapshot、Log Compaction、成员变更及节点恢复行为。

#### REQ-RAFT-002：复制队列模型（P0）

建设具备多数派提交能力的复制队列，要求：

- 每个复制队列或复制分区具有明确的复制组归属。
- 定义 Leader / Follower 角色。
- 支持日志复制和提交状态跟踪。
- 支持多数派提交。
- Leader 故障后能够重新选举。
- 已提交消息在允许的故障模型内不得丢失。
- Follower 恢复后能够追赶日志或通过快照恢复。
- 暴露副本延迟、成员状态和选举指标。

#### REQ-RAFT-003：消息提交与 Confirm（P0）

```text
Producer
   |
   v
Queue Leader
   |
   v
Append / Persist Log
   |
   +------> Follower A
   |
   +------> Follower B
   |
   v
Majority Replicated
   |
   v
Commit Index Advanced
   |
   v
Message Committed
   |
   v
Publisher Confirm ACK
```

实际确认条件应由复制因子、持久化配置和队列策略决定。未提交日志不得误报为已提交消息。必须覆盖网络分区、Leader 切换、Follower 落后和多数派不可用等场景。

#### REQ-RAFT-004：Raft 与消息存储解耦（P0）

明确比较以下数据复制方式：

1. Raft 日志直接携带消息正文；
2. Raft 日志复制消息引用，正文由独立数据通道复制；
3. 本地 WAL 与复制日志组合。

应通过吞吐量、延迟、磁盘放大、恢复复杂度和一致性测试确定方案。Raft 共识层不应直接承担 Queue 的全部消费调度和协议状态逻辑。

#### REQ-RAFT-005：元数据 Raft 与消息 Raft 分离（P0）

元数据一致性和消息数据一致性必须定义独立的状态机、复制组、容量规划和故障域。允许复用底层 `ra` 库，但不得将元数据提交误认为消息正文已经可靠复制。

### 5.6 集群管理与故障恢复

#### REQ-CLUSTER-001：节点管理（P0）

支持节点注册、发现、上下线、健康检查、资源监控和故障检测。

#### REQ-CLUSTER-002：Leader / Replica 放置（P1）

支持 Queue Leader 与副本分配、节点负载感知、迁移、副本重建和节点重新加入。需要定义放置策略和容量约束。

#### REQ-CLUSTER-003：故障恢复（P0）

覆盖 Leader 宕机、Follower 宕机、网络分区、节点重启、磁盘异常及节点重新加入。恢复结果必须满足已提交数据不丢失的承诺边界。

### 5.7 第二阶段交付物与验收

交付物：

1. Queue Engine 设计文档和状态机图。
2. 消息状态模型及转换测试。
3. WAL / Message Store / Index 设计文档。
4. Publisher Confirm 与 ACK 可靠性说明。
5. Raft 复制队列技术设计。
6. 节点故障、网络分区和数据恢复测试报告。
7. 单机与集群性能基线。

最低验收：

- 持久化消息在进程崩溃后可恢复。
- Unacked 消息在消费者或节点异常后按语义重新投递。
- Confirm 与持久化 / 复制提交边界一致。
- 多数派提交、Leader 故障切换和 Follower 恢复测试通过。
- 已提交消息满足定义的故障模型下不丢失。
- 队列积压有内存和磁盘保护措施。
- 重复 ACK、重复投递和索引不一致等场景有测试覆盖。

---

## 6. 第三阶段：RabbitMQ 生态兼容

### 6.1 阶段目标

完善管理接口、Web 管理控制台、监控、策略、外部认证、插件扩展和运维能力，使 UnifiedMQ 具备被企业持续部署、监控、升级和维护的基础条件。

### 6.2 Management API 与 Web UI

#### REQ-MGMT-001：Management HTTP API（P0）

提供统一管理 API，至少覆盖：

- 节点与集群状态；
- Connection / Channel；
- Exchange / Queue / Binding；
- Consumer；
- 用户、Vhost 和权限；
- Policy 与参数；
- 消息和资源统计；
- 配置和 Definitions 导入导出。

可参考 RabbitMQ Management API 的资源模型和使用体验，但应定义 UnifiedMQ 自身稳定的 API 版本和兼容范围。

#### REQ-MGMT-002：Web Management UI（P0）

提供 Dashboard、节点和集群状态、Connection、Channel、Exchange、Queue、Binding、Consumer、用户、Vhost、Policy 和指标查询界面。

#### REQ-MGMT-003：Definitions（P1）

支持用户、Vhost、Exchange、Queue、Binding、Policy 等定义的导入、导出和环境迁移。导入应有校验、冲突策略和失败报告。

### 6.3 监控、日志与链路追踪

#### REQ-OBS-001：Prometheus Metrics（P0）

至少暴露：

- Connections、Channels、Queues、Consumers；
- Messages Ready、Unacked、Total；
- Publish、Deliver、ACK、NACK、Redelivery Rate；
- Queue Memory、Disk Usage、WAL Size；
- Raft Replication Lag、Leader Changes、Follower State；
- 节点 CPU、内存、文件描述符和运行时指标。

指标必须注明单位、类型、标签维度及高基数风险。

#### REQ-OBS-002：结构化日志（P0）

支持结构化日志和级别配置。日志至少能关联节点、连接、Channel、Queue、消息标识、复制组和故障恢复任务。禁止输出密码、Token 等敏感信息。

#### REQ-OBS-003：OpenTelemetry（P1）

对连接、发布、路由、持久化、复制、投递和确认等关键链路提供可选 Trace。必须控制采样和高吞吐场景下的开销。

### 6.4 Policy 与资源治理

#### REQ-POLICY-001：Policy（P0）

支持策略创建、修改、删除、匹配、优先级和动态更新。策略范围包括 TTL、DLX、Max Length、Overflow、Priority、Replica Placement 等。

#### REQ-POLICY-002：资源限制（P1）

支持连接数、Channel 数、Queue 数、消息尺寸、队列长度、内存和磁盘水位等资源限制，并明确达到限制后的响应行为。

### 6.5 插件与扩展机制

#### REQ-PLUGIN-001：插件框架（P0）

提供稳定的扩展接口，规划以下类别：

- 协议插件；
- 认证与授权插件；
- 存储插件；
- 路由插件；
- 管理 API 插件；
- 指标与观测插件；
- 消息过滤和转换插件。

插件必须定义版本兼容、配置、启停、隔离、错误处理和升级规则。

#### REQ-PLUGIN-002：外部认证（P1）

评估并支持 LDAP、OAuth 2.0、JWT 或自定义认证后端。外部身份认证与 UnifiedMQ 内部资源授权应分层实现。

#### REQ-PLUGIN-003：跨集群与消息搬运（P2）

评估 Federation / Shovel 类能力，支持跨 Broker 或跨集群消息转发、断点恢复、重试、死信和链路监控。

### 6.6 Stream 能力

#### REQ-STREAM-001：持久化日志型消息模型（P1）

建设与传统 Queue 语义区分的 Stream 能力，支持：

- Append-only Log；
- Partition（如适用）；
- Offset；
- Consumer Offset；
- 消息重放；
- 保留策略；
- 顺序读取；
- 副本及故障恢复。

Stream 不应被当作普通 Queue 的一个简单开关。应明确其消费位置、消息保留、并发读取和确认模型。

### 6.7 升级、备份和运维

#### REQ-OPS-001：滚动升级（P1）

定义节点滚动升级、协议版本兼容、数据格式升级、Raft 版本升级和失败回滚方案。

#### REQ-OPS-002：备份恢复（P1）

提供元数据和消息数据的备份、恢复、校验和演练工具。明确一致性备份边界及恢复顺序。

#### REQ-OPS-003：数据迁移（P1）

提供从旧版本 UnifiedMQ 迁移数据的工具或流程；评估 RabbitMQ 消息和定义迁移的范围、限制和操作步骤。

### 6.8 第三阶段交付物与验收

交付物：

1. Management API 文档及 OpenAPI 定义。
2. Web 管理界面。
3. Prometheus 指标说明和仪表盘示例。
4. Policy 和插件开发文档。
5. Stream 设计文档。
6. 部署、备份、恢复、升级和迁移手册。

最低验收：

- 管理 API 覆盖主要资源并通过权限测试。
- Web UI 能完成常用管理操作。
- Prometheus 能抓取核心数据面和控制面指标。
- Policy 修改能按定义生效。
- 插件接口有示例插件和兼容性测试。
- 备份恢复和滚动升级有可重复演练记录。

---

## 7. 三项核心技术借鉴要求

本章是跨阶段的架构约束，第一、二、三阶段均应遵守。

### 7.1 Queue 内核：借鉴 RabbitMQ 的队列设计

参考仓库：<https://github.com/rabbitmq/rabbitmq-server>

重点研究方向：

1. **Queue 生命周期：** 声明、创建、激活、运行、停止、删除、恢复和资源回收。
2. **消息状态：** Ready、Delivered、Unacked、ACK、Requeue、Dead Letter、Expired 等状态如何组织和恢复。
3. **消费者调度：** 消费者注册、取消、Prefetch、公平调度、慢消费者和异常恢复。
4. **消息持久化：** Message Store、Queue Index、WAL、内存 / 磁盘管理、分页和空间回收。
5. **队列类型抽象：** 研究 RabbitMQ 如何抽象不同 Queue Type，避免 AMQP 协议层与某种队列实现强耦合。

**UnifiedMQ 约束：** Queue Engine 必须成为独立内核模块。协议适配层通过稳定接口使用 Queue 能力；其他协议若有不同的消费模型，应保留自己的原生模型或建立专用引擎，不得强行塞入 AMQP Queue。

### 7.2 可靠性机制：借鉴 Publisher Confirm、Quorum Queue、复制和恢复

参考仓库：

- <https://github.com/rabbitmq/rabbitmq-server>
- <https://github.com/rabbitmq/ra>

重点研究方向：

1. Publisher Confirm 的序列号、ACK/NACK、批量确认及确认边界。
2. Quorum Queue 的复制组、Leader / Follower、多数派提交和恢复机制。
3. 消息日志复制、Commit Index、Snapshot 和副本追赶。
4. ACK、Unacked 和消费者故障后的重投递。
5. Leader 切换、网络分区、节点重启和数据恢复。

**UnifiedMQ 约束：** 必须区分元数据 Raft 与消息数据复制。Publisher Confirm 的语义必须明确绑定到本地持久化或多数派提交条件。不能只因消息被某个 Erlang 进程接收，就向生产者报告持久化成功。

### 7.3 协议语义：保留协议原生模型

重点建立两类模型：

**通用消息数据模型**用于承载跨协议可复用的内容：

```text
UnifiedMessage
  - message_id
  - payload
  - headers
  - timestamp
  - content_type
  - source_protocol
  - trace_context
  - extension_metadata
```

**协议原生语义模型**保留各协议自身的资源和状态：

| 协议 | 必须保留的关键语义 |
|---|---|
| AMQP 0-9-1 | Exchange、Queue、Binding、Routing Key、Consumer、Delivery Tag、ACK、Publisher Confirm、Vhost |
| MQTT | Topic、Subscription、QoS 0/1/2、Session、Retain、Will、Packet Identifier |
| Kafka | Topic、Partition、Offset、Consumer Group、Offset Commit、分区顺序 |
| Pulsar | Topic、Partition、Subscription、Subscription Type、Cursor、Ack / Redelivery |
| NATS | Subject、Queue Group；JetStream 场景还需保留 Stream、Consumer、Sequence、Ack Policy |
| WebSocket | 连接和会话生命周期、应用消息边界及背压行为 |

**UnifiedMQ 约束：**

- 统一消息模型只统一通用数据，不统一抹除投递语义。
- 协议专属元数据必须可保存、读取和恢复。
- 协议间转换必须有映射规则和兼容性说明。
- QoS、ACK、Offset、Cursor 等不能互相冒充。
- 无法无损映射的语义必须显式标注为不兼容或降级，而不是静默丢弃。
- CloudEvents 是事件元数据标准，不替代消息协议的路由、确认和持久化机制。

---

## 8. RabbitMQ 参考仓库研究任务

### 8.1 rabbitmq-server：核心服务端

仓库：<https://github.com/rabbitmq/rabbitmq-server>

研究目的：了解成熟 AMQP Broker 的模块边界、Queue 生命周期、消息路由、消费者和持久化实现。

建议重点查看以下模块及其在当前版本中的对应实现（具体目录和名称应以所选 RabbitMQ 源码版本为准）：

| 模块 / 主题 | 研究内容 | UnifiedMQ 对应产出 |
|---|---|---|
| `rabbit_channel` | Channel 状态、Publish、Delivery、ACK、Confirm | AMQP Channel 状态机设计 |
| `rabbit_amqqueue` | Queue 声明、删除、资源管理 | Queue 生命周期设计 |
| `rabbit_queue_type` | Queue 类型抽象及扩展点 | Queue Engine 接口设计 |
| `rabbit_exchange` | Exchange 声明和管理 | Exchange 管理模型 |
| `rabbit_router` | Binding 与路由匹配 | Router Engine 设计 |
| `rabbit_msg_store` | 消息正文存储与恢复 | Message Store 设计 |
| `rabbit_queue_index` | 消息索引与状态定位 | Queue Index 设计 |
| Quorum Queue 相关模块 | Raft 复制、提交和恢复集成 | 复制队列设计 |

研究交付物：

- RabbitMQ 核心模块依赖图；
- Queue、Exchange、Channel、Storage 的职责边界；
- UnifiedMQ 模块映射表；
- 已实现 / 部分实现 / 缺失能力清单；
- 关键数据结构和状态机分析；
- 可借鉴设计与不宜直接照搬的设计说明。

### 8.2 ra：Erlang Raft 共识实现

仓库：<https://github.com/rabbitmq/ra>

研究目的：借鉴 Erlang/OTP 环境下 Raft 状态机和复制组的工程实现。

重点研究：

- Raft Server 和 Group 生命周期；
- Leader Election；
- Log Append / Replication；
- Commit Index；
- Snapshot 与 Log Compaction；
- Membership Change；
- 持久化和重启恢复；
- 性能、背压和故障处理。

研究交付物：

- `ra` 的核心组件和运行模型分析；
- UnifiedMQ 元数据复制和消息复制的 Group 设计；
- Queue Leader / Follower 设计；
- 消息提交与 Publisher Confirm 绑定设计；
- 节点故障和副本恢复方案；
- 引入、复用或适配 `ra` 的技术决策记录。

### 8.3 khepri：分布式元数据存储

仓库：<https://github.com/rabbitmq/khepri>

研究目的：借鉴分布式元数据树、读写、变更通知和一致性管理方式。

重点研究：

- 元数据树和路径组织；
- 元数据读写与查询；
- 元数据变更通知；
- 基于 Raft 的复制和一致性；
- 节点同步和恢复；
- 元数据持久化和版本演进。

UnifiedMQ 候选应用范围：

- Broker 配置；
- 用户、角色和权限；
- Vhost；
- Exchange、Queue、Binding 元数据；
- Policy 和参数；
- 集群成员与节点信息；
- Queue Leader / Replica 放置信息。

研究交付物：

- Khepri 数据模型和 API 分析；
- 与现有 Mnesia 方案的对比；
- 元数据树结构设计；
- 变更通知和缓存失效方案；
- 数据迁移、恢复和版本兼容方案；
- 是否引入 Khepri 的技术决策记录。

### 8.4 参考项目采用原则

1. 研究和借鉴优先于直接复制代码。
2. 记录参考版本、提交号、许可证和依赖约束。
3. 引入第三方项目或代码前，必须完成许可证兼容性审查。
4. 对 `ra`、`khepri` 的版本升级建立兼容性测试。
5. 不因已经使用 Mnesia 或 Raft，就默认不再评估 Khepri；也不因 Khepri 使用 Raft，就默认必须替换现有元数据系统。

---

## 9. 非功能性需求

### 9.1 性能与容量

建立可重复的基准测试体系，至少覆盖：

| 维度 | 测试场景 |
|---|---|
| 消息吞吐 | 单生产者、多生产者、单消费者、多消费者 |
| 消息大小 | 1 KB、4 KB、64 KB、1 MB，并可扩展 |
| 持久化 | 非持久化、本地 WAL、复制队列 |
| 延迟 | P50、P95、P99、P999 |
| 连接规模 | 不同并发 Connection / Channel 数量 |
| Queue 规模 | 不同 Queue 数量及单队列积压规模 |
| 路由规模 | 不同 Binding 数量和 Topic 匹配复杂度 |
| 消费模型 | 竞争消费、广播、多订阅、慢消费者 |
| 集群规模 | 不同节点数和副本数 |
| 恢复 | 节点重启、Leader 切换、Follower 追赶 |

性能目标由基准测试建立基线后逐阶段设定。不得以未经测试的数据作为产品承诺。

### 9.2 稳定性与资源保护

- 进程 Mailbox 必须有监控和背压策略。
- 消息积压不能导致无界内存增长。
- 磁盘接近耗尽时必须有明确告警和写入保护行为。
- WAL、索引和数据段必须有回收策略。
- 长时间运行应具备内存、进程数、文件描述符和磁盘监控。
- 故障恢复应有超时、重试上限和可观测状态。

### 9.3 安全

- TLS 传输保护。
- 密码和 Token 安全存储。
- Vhost / 租户隔离。
- 最小权限原则。
- 管理操作鉴权和审计。
- 敏感信息不写入日志。
- 外部认证失败、超时和不可用时有明确策略。

### 9.4 可观测性

关键数据面和控制面应提供 Metrics、Structured Logging 和可选 Tracing。应能定位消息从接入、路由、持久化、复制、投递到 ACK 的关键节点。

### 9.5 兼容性与升级

- 明确协议版本与支持特性矩阵。
- 明确元数据和消息数据格式版本。
- 规划节点滚动升级和混合版本兼容边界。
- 数据格式升级应有备份和回滚方案。
- 配置变更应有校验和失败回滚策略。

---

## 10. 测试策略与验收方法

### 10.1 测试层次

1. **单元测试：** 编解码、状态转换、路由匹配、索引和策略。
2. **属性测试：** 消息状态机、ACK/NACK、路由和恢复不变量。
3. **协议互操作测试：** 使用主流 AMQP、MQTT、Kafka、NATS、Pulsar 客户端。
4. **集成测试：** 多连接、多 Channel、多消费者、持久化和集群。
5. **故障注入：** `kill -9`、节点断电模拟、网络分区、磁盘错误和进程崩溃。
6. **性能测试：** 吞吐量、延迟、内存、磁盘和复制开销。
7. **长稳测试：** 长时间运行、消息积压、连接频繁建立和释放。
8. **升级测试：** 滚动升级、数据迁移和版本回滚。

### 10.2 关键一致性不变量

至少验证以下不变量：

- 已向生产者返回“达到持久化提交条件”的 Confirm 后，消息在承诺的故障模型内可恢复。
- 未 ACK 的消息不会被无条件当作已消费删除。
- 已 ACK 的消息不会因为重复恢复而无条件重新投递。
- Requeue 后消息重新进入可消费集合，且不会永久丢失。
- 已提交的 Raft 日志不会被合法 Leader 变更覆盖为未提交状态。
- 元数据提交成功不能被误认为消息数据复制成功。
- 队列删除后不会遗留无法回收的消费者、索引或复制资源。
- 协议转换不能静默丢失影响业务语义的关键元数据。

### 10.3 兼容性矩阵

每个协议特性都应维护如下状态之一：

- **Supported：** 已实现并通过自动化测试。
- **Partial：** 已实现部分行为，明确列出限制。
- **Unsupported：** 不支持，客户端调用时有明确行为。
- **Not Applicable：** 不适用于该协议或产品场景。

不得将“能连接”“能发布一条消息”标记为整个协议 Supported。

---

## 11. 阶段优先级与研发任务总表

### 11.1 优先级定义

| 优先级 | 定义 |
|---|---|
| P0 | 阶段目标的必要能力，缺失则不能通过阶段验收 |
| P1 | 重要能力，应在阶段内完成；若延期必须记录风险和替代方案 |
| P2 | 增强能力，可在核心目标稳定后迭代 |

### 11.2 第一阶段任务清单

| 需求项 | 优先级 |
|---|---|
| Connection / Channel 状态机 | P0 |
| Exchange 四种类型 | P0 |
| Queue 声明与生命周期基础行为 | P0 |
| Publish / Consume | P0 |
| ACK / NACK / Reject / Requeue | P0 |
| QoS / Prefetch | P0 |
| Publisher Confirm | P0 |
| Vhost、用户认证和资源权限 | P0 |
| RabbitMQ 主流客户端互操作测试 | P0 |
| AMQP Transaction | P1 |
| TTL / DLX | P1 |
| Priority Queue | P1 |
| 常用 Queue 参数 | P1 |

### 11.3 第二阶段任务清单

| 需求项 | 优先级 |
|---|---|
| Queue 生命周期状态机 | P0 |
| 消息状态机及恢复规则 | P0 |
| Ready / Unacked 管理 | P0 |
| Consumer Scheduler | P0 |
| WAL 增强 | P0 |
| Message Store / Index | P0 |
| 持久化与崩溃恢复 | P0 |
| Publisher Confirm 可靠性边界 | P0 |
| ACK 与故障恢复 | P0 |
| Raft 复制队列 | P0 |
| 多数派提交与 Leader 切换 | P0 |
| Replica Recovery | P0 |
| Backpressure 和资源保护 | P0 |
| 消息分页和空间回收 | P1 |
| Queue 迁移与放置优化 | P1 |

### 11.4 第三阶段任务清单

| 需求项 | 优先级 |
|---|---|
| Management API | P0 |
| Web Management UI | P0 |
| Prometheus Metrics | P0 |
| Policy 管理 | P0 |
| 插件扩展接口 | P0 |
| Definitions 导入导出 | P1 |
| OpenTelemetry | P1 |
| 外部认证 | P1 |
| Streams | P1 |
| 滚动升级 | P1 |
| 备份恢复和数据迁移 | P1 |
| Federation / Shovel 类能力 | P2 |

---

## 12. 研发实施约束

### 12.1 开发前代码审查

每项需求开发前必须先检查现有 UnifiedMQ 实现，并分类：

- **Implemented：** 已实现且有测试，优先复用。
- **Partial：** 部分实现，需要补齐语义和测试。
- **Missing：** 尚未实现，需要新增。
- **Refactor：** 已有实现但架构或语义不满足要求，需要重构。

禁止在未审查现有代码的情况下重复建设已有能力。

### 12.2 需求到代码的追踪

每项需求应关联：

- 需求 ID；
- 设计文档；
- 模块或应用；
- 实现 PR / Commit；
- 自动化测试；
- 验收结果；
- 已知限制。

### 12.3 设计评审要求

以下变更必须经过架构评审：

- Queue 数据结构和状态机；
- 消息存储格式；
- WAL 格式和恢复协议；
- Publisher Confirm 语义；
- 消息复制和 Raft 状态机；
- 元数据存储选型；
- 协议原生模型与统一模型边界；
- 数据迁移和版本兼容方案。

### 12.4 不允许的实现方式

- 不允许用简单 Pub/Sub 替代所有协议的原生模型。
- 不允许把元数据 Raft 复制等同于消息数据复制。
- 不允许在未达到约定提交边界时发送可靠 Confirm。
- 不允许忽略未知 Queue 参数后仍返回成功。
- 不允许让 Queue 积压依赖无限增长的进程邮箱。
- 不允许只用 happy-path 测试宣称可靠性完成。
- 不允许将“至少一次”或“恰好一次”作为宣传词而没有故障模型和测试依据。

---

## 13. 最终建设目标与完成定义

三个阶段完成后，UnifiedMQ 应达到以下能力目标：

| 领域 | 目标 |
|---|---|
| AMQP | 覆盖约定范围内的 AMQP 0-9-1 基础及常用 RabbitMQ 语义 |
| Queue | 独立、可恢复的 Queue Engine，具备生命周期、消息状态和消费者调度 |
| Routing | Direct / Fanout / Topic / Headers 等 AMQP 路由语义 |
| Reliability | Publisher Confirm、ACK、NACK、重投递与持久化边界明确 |
| Storage | WAL、Message Store、Index、恢复和空间回收 |
| Replication | Raft 多数派复制、Leader 切换和副本恢复 |
| Cluster | 节点管理、资源放置、故障恢复和可观测性 |
| Security | 用户、Vhost、ACL、TLS 和外部认证扩展 |
| Management | 管理 API、Web UI、Definitions 和 Policy |
| Observability | Prometheus、结构化日志和 OpenTelemetry |
| Extension | 协议、认证、路由、存储和管理插件扩展能力 |
| Multi-protocol | 保留 AMQP、MQTT、Kafka、Pulsar、NATS 等协议的原生语义 |

**最终完成定义：** 需求清单全部有明确实现状态；P0 项有自动化测试；关键可靠性不变量通过故障注入；协议兼容范围有可复现的客户端测试报告；性能和资源表现有基准数据；部署、升级、备份和恢复有文档及演练记录。

---

## 14. 建议的后续研发顺序

建议不要同时铺开所有协议和管理功能，而是按以下顺序推进：

1. 先完成 RabbitMQ Server 源码模块映射，审查 UnifiedMQ 现有 AMQP、Queue、Storage 和 Cluster 实现。
2. 固化 Queue Engine 的生命周期、消息状态和 Consumer Scheduler 设计。
3. 明确 WAL、Message Store、Index 和恢复流程，再完善 Publisher Confirm 语义。
4. 基于 `ra` 设计复制队列，区分元数据 Raft 和消息复制。
5. 评估 `khepri` 与现有 Mnesia 的职责边界，形成技术决策记录。
6. 补全 AMQP 兼容性矩阵和真实客户端回归测试。
7. 在可靠性内核稳定后，再扩展 Management API、插件、Streams 和生态能力。
8. 最后针对不同协议逐一验证原生语义，避免统一抽象造成能力退化。

这一路径优先解决 UnifiedMQ 最核心的工程问题：**可靠的消息队列内核和分布式消息一致性，而不是单纯增加协议数量或追求代码行数。**

---

## 附录 A：参考仓库

- RabbitMQ Server：<https://github.com/rabbitmq/rabbitmq-server>
- RabbitMQ Ra：<https://github.com/rabbitmq/ra>
- RabbitMQ Khepri：<https://github.com/rabbitmq/khepri>

## 附录 B：术语

| 术语 | 说明 |
|---|---|
| Queue | 队列，保存待投递或待消费消息的资源 |
| Exchange | AMQP 消息交换机，根据类型和 Binding 进行路由 |
| Binding | Exchange 与 Queue 之间的路由关系 |
| Ready | 已准备好但尚未投递的消息 |
| Unacked | 已投递但尚未收到消费者确认的消息 |
| Publisher Confirm | Broker 向生产者确认发布处理结果的机制 |
| Quorum Queue | 通过多数派复制提供高可用的队列类型 |
| WAL | Write-Ahead Log，预写日志 |
| Raft Group | 通过 Raft 共识维护复制状态的一组节点 |
| Commit Index | Raft 中已提交日志条目的索引边界 |
| Vhost | AMQP 虚拟主机，用于资源隔离 |
| DLX | Dead Letter Exchange，死信交换机 |
| Prefetch | 消费者未确认消息数量等投递流控限制 |
| Stream | 基于持久化追加日志和消费位置的消息模型 |
