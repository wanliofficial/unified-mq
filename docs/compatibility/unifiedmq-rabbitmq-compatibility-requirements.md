# UnifiedMQ 与 RabbitMQ Server 兼容差异需求

| 项 | 内容 |
|---|---|
| 文档状态 | 草案，供排期与验收 |
| 日期 | 2026-11-12 |
| 对照对象 | [wanwuliandong/unified-mq](https://gitee.com/wanwuliandong/unified-mq) 当前 `main`（AMQP 适配器 S5/S7）相对官方 [rabbitmq/rabbitmq-server](https://github.com/rabbitmq/rabbitmq-server)（AMQP 0-9-1 行为，以 3.13 / 4.x 客户端可见语义为准） |
| 证据来源 | `apps/broker_proto_amqp/`、`docs/protocol-compatibility.md` §4.7、`test/protocol-compatibility/amqp/` |
| 不在本文范围 | 把 UnifiedMQ 改写成 rabbitmq-server 源码分支；与 RabbitMQ 节点互通组集群 |

## 1. 结论

UnifiedMQ 的「RabbitMQ 兼容」不是 `rabbitmq-server` 的裁剪或移植。仓库里来自 RabbitMQ 组织的代码只有 Raft 库 `vendor/ra`（及 `aten`、`seshat`、`gen_batch_server`），不包含 `rabbit_reader`、`rabbit_channel`、队列进程或管理插件。

AMQP 能力是自研适配器，源码约 1,200 行：

| 文件 | 行数 | 职责 |
|---|---|---|
| `apps/broker_proto_amqp/src/amqp_conn.erl` | 797 | 连接、通道、发布、投递、ack |
| `apps/broker_proto_amqp/src/amqp_codec.erl` | 194 | 帧、字段表、basic properties |
| `apps/broker_proto_amqp/src/amqp_topo.erl` | 156 | exchange / queue / binding，ETS，进程内 |
| `amqp_listener.erl` 与 app/sup | 约 50 | 监听与监督 |

它能让官方 `amqp091-go` v1.10.0 跑通 13 个「先有消费者、再发布」的用例。不能替换生产 RabbitMQ：队列积压、重启后的拓扑、RPC 属性、publisher confirm、队列参数和管理端都不成立。

需求按行为等价写，不要求内部改成 classic queue index 或 quorum queue 进程。存储可以继续走 `broker_queue`，但客户端观察到的结果必须与下面的验收一致。

## 2. 已实现基线（不要重复建设）

以下已有实现和测试，后续改动不得回退。证据：`amqp_conn_SUITE` 17 例、`amqp_codec_SUITE` 15 例、`amqp091-go` 13/13。

- 协议头 `AMQP 0 0 9 1`，随后 `connection.start` / `start-ok` / `tune` / `tune-ok` / `open` / `open-ok`。
- `connection.start` 只宣布机制 `PLAIN`、locale `en_US`。产品标识是 `UnifiedMQ` `0.1.0`。
- SASL PLAIN（`\0user\0password`）走 `broker_security`。未开 `require_auth` 时匿名连接落到默认租户。错误凭据会断开。
- `channel.open` / `channel.close`。通道错误用 `channel.close` 关掉该通道，不断开整条连接。
- 空名字的 default exchange：`basic.publish` 到 `""` 时，routing key 等于队列名才投递。
- `direct` / `fanout` / `topic` 的 declare、delete、bind、unbind。topic 中 `*` 是一个词，`#` 是剩余词。
- `queue.declare`（含 `amq.gen-*` 匿名队列）、passive 不存在时通道 404、durable 标志不一致时 406。
- `basic.publish`（方法 + content header + body 分帧）、`basic.consume` / `cancel` / `deliver`、`basic.ack` / `nack` / `reject`。
- 同一队列多个消费者竞争消费；`basic.qos` 的 prefetch count 限制未 ack 条数；`no-ack` 消费者不受 prefetch 限制。
- 连接断开时，未 ack 投递 `release`。`nack/reject(requeue=true)` 会重新投递。
- `content-type` 与 `headers` 字段表进入统一消息头，投递时写回。CloudEvents `ce-*` 走这张表。
- 消息体经 `broker_router` → `broker_queue`（fsync）→ `broker_subscription`，不是协议私有存储。

## 3. 差异总表

严重级别：P0 会让常见客户端或业务语义直接错误；P1 是 RabbitMQ 上常见的生产特性；P2 是运维与生态；P3 明确可后置或不做。

| ID | 级别 | 差异 | 现在的行为 | RabbitMQ 行为 |
|---|---|---|---|---|
| RQ-01 | P0 | 无消费者时的积压 | 第一个 `shared` 订阅把游标锚在分区尾，之前写入的消息不再投递 | 消息留在队列里，后连上的消费者能读完积压 |
| RQ-02 | P0 | 拓扑持久化 | exchange / queue / binding 在 ETS，重启即失；`durable` 只做标志比对 | durable 队列、交换机、绑定重启仍在，消息还在 |
| RQ-03 | P0 | basic properties | 只保留 `content-type` 和 `headers`；`correlation_id`、`reply_to`、`message_id`、`type`、`app_id`、`expiration`、`timestamp`、`priority`、`delivery_mode`、`content_encoding`、`user_id` 丢弃 | 投递时原样带回 |
| RQ-04 | P0 | 字段表类型 | 只稳定处理 `S t I l F V`。未知类型不跳过值字节。整数回写一律 `I`（32 位） | 支持 `t b B s u i I l f d D s S x A T F V`；长整型与二进制不能把连接打崩 |
| RQ-05 | P0 | 不可路由消息 | `mandatory` / `immediate` 忽略；没有匹配队列时消息丢掉，无 `basic.return` | `mandatory=true` 时 `basic.return` |
| RQ-06 | P0 | vhost | `connection.open` 的 virtual host 读完即丢，所有 vhost 共用一张 ETS | vhost 之间交换机、队列、权限隔离；不存在的 vhost 拒绝 |
| RQ-07 | P0 | publisher confirm | `confirm.select` 被忽略，不回 `select-ok`，也从不发 `basic.ack`（confirm 语义） | 协商成功后，每条发布有 ack 或 nack |
| RQ-08 | P0 | 事务 | `tx.select/commit/rollback` 被忽略 | 选中事务后，commit 前对客户端不可见；与 confirm 互斥 |
| RQ-09 | P1 | `basic.get` | 恒回 `basic.get-empty` | 有消息则 `get-ok` + body，队列中移除或进入 unacked |
| RQ-10 | P1 | `basic.recover` | 只回 `recover-ok`，不重投 | `requeue=true` 时未 ack 消息以 `redelivered=1` 再次投递 |
| RQ-11 | P1 | 重投标志 | `basic.deliver` 的 redelivered 写死 0 | nack/recover/连接断开后再投必须为 1 |
| RQ-12 | P1 | 队列计数与清空 | `declare-ok` 消息数恒 0；`purge` / `delete` 回 0 且不删日志 | 计数反映就绪消息；purge 删除就绪消息；delete 删除队列及其消息 |
| RQ-13 | P1 | 队列参数 | arguments 表解析后丢弃 | TTL、死信、最大长度、溢出策略、队列类型、单活消费者、优先级、备用交换机生效 |
| RQ-14 | P1 | headers 交换机 | 可以声明，路由恒不匹配 | `x-match` = `all` / `any` |
| RQ-15 | P1 | 预置交换机 | 只有 `""` | `amq.direct`、`amq.fanout`、`amq.topic`、`amq.headers`、`amq.match` 存在且不可删除 |
| RQ-16 | P1 | 队列与消费者标志 | `auto-delete`、`exclusive`、`internal`、`if-unused`、`if-empty`、consume `exclusive`、`no-local` 不生效或只看本连接 | 与规范及 RabbitMQ 一致，跨连接生效 |
| RQ-17 | P1 | 删除队列通知 | 队列删除不向其他连接发 `basic.cancel` | `consumer_cancel_notify` |
| RQ-18 | P1 | QoS 与心跳 | prefetch 0 仍按每轮最多 32 条拉取；`prefetch-size`、global QoS 忽略；客户端 heartbeat 忽略，服务端 tune 固定 heartbeat 0 | prefetch 0 表示不限制；心跳按协商值检测死连接 |
| RQ-19 | P1 | 异常方法 | 未知 class/method 静默丢弃；未知 delivery-tag 的 ack 静默成功；`basic.cancel` 未知 tag 无通道错误 | 通道或连接级 close，reply-code 符合规范 |
| RQ-20 | P1 | 握手关闭帧 | 认证失败的 `connection.close` 用 shortstr 装 `"ACCESS_REFUSED"`，缺 class-id / method-id | reply-code 是 short（403），后接 shortstr、class-id、method-id |
| RQ-21 | P1 | 能力声明 | server-properties 无 capabilities | 只宣布已经实现的能力，未实现的不得宣布为 true |
| RQ-22 | P1 | direct reply-to | 无 `amq.rabbitmq.reply-to` | RPC 客户端可用该伪队列，且 `reply_to` 被保留（依赖 RQ-03） |
| RQ-23 | P2 | 流控与密钥轮换 | 无 `channel.flow`、`connection.blocked/unblocked`、`connection.update-secret` | 内存告警时阻塞发布；提供等价的资源保护 |
| RQ-24 | P2 | 权限 | 只有统一身份表的连接级认证，无 configure/write/read 正则权限 | 按 vhost 限制 declare、bind、publish、consume |
| RQ-25 | P2 | 管理与定义 | 无 15672、`/api/*`、definitions 导入导出 | 至少提供队列、绑定、连接的查询与定义恢复 |
| RQ-26 | P2 | 策略与插件行为 | 无 policy、operator policy、federation、shovel、延迟交换机、consistent-hash | 不默认承诺插件生态；若宣称兼容须单独立项 |
| RQ-27 | P3 | 其他协议面 | 无 AMQP 1.0、无 RabbitMQ Stream 协议、无 STOMP、MQTT 是另一套适配器 | 不作为 0-9-1 替换的前置条件 |
| RQ-28 | P3 | 集群互通 | `ra` 只服务 UnifiedMQ 自己的复制，不是 quorum queue，也不能加入 RabbitMQ 集群 | 不做节点间协议兼容 |

## 4. 需求明细

每条都包含现状、目标行为和验收。验收默认使用真实 `amqp091-go`（或 pika / Java 客户端）对已启动的 broker，不手写只照顾实现的假客户端。现有 13 个用例保持通过。

### 4.1 P0：没有这些，就不能称为队列

#### RQ-01 消费者晚于消息加入时仍能读到积压

- 现状：`basic.consume` 调用 `broker_subscription:join(shared, Queue, ...)`。新订阅的游标锚在分区当前尾部（`docs/protocol-compatibility.md` §4.7 第 5 条）。先 publish、后 consume 的消息不会出现在 `basic.deliver` 里。
- 目标：队列已存在时，publish 写入该队列并保持就绪。之后任何消费者都能按入队顺序拿到这些消息，直到 ack。竞争消费者之间仍恰好一人收到。
- 验收：
  1. 声明队列，不挂消费者，发布 100 条，再 consume，收满 100 条且 body 一致。
  2. 两个消费者同时在线时，100 条不重复、不丢失。
  3. 无消费者期间重启进程后，durable 队列仍能读完这 100 条（与 RQ-02 一起测）。

#### RQ-02 durable 拓扑与消息跨重启

- 现状：`amqp_topo` 在 ETS 中保存交换机、队列、绑定，注释写明重启不保留。`durable` 只参与「再次声明时标志必须相同」。`queue.delete` 只删 ETS，不删 `broker_queue` 里的记录。
- 目标：
  - `durable=true` 的 exchange、queue、binding 在进程重启后仍在，passive declare 成功。
  - 非 durable 对象重启后不在。
  - durable 队列中已 ack 的消息不复活；未 ack 与未投递的消息按 RQ-01 可继续投递。
  - 删除队列后，这些消息不再被任何协议读到。
- 验收：声明 durable direct 交换机、durable 队列和绑定，发布并只 ack 一半，`kill -9` 后用 passive declare 看到对象仍在，消费者只收到未 ack 的那一半，且 redelivered=1。

#### RQ-03 basic properties 往返

- 现状：`props_to_headers/1` 只留下 `headers` 和 `content-type`。`deliver/8` 经 `headers_to_props/1` 写回，因此 `correlation_id`、`reply_to` 等字段在消费者侧为空。`delivery_mode` 也不会出现。
- 目标：下列字段发布后原样回到 `basic.deliver` 的 content header：`content-type`、`content-encoding`、`headers`、`delivery-mode`、`priority`、`correlation-id`、`reply-to`、`expiration`、`message-id`、`timestamp`、`type`、`user-id`、`app-id`。`cluster-id` 已废弃，可以不回。
- 验收：同一条消息上把以上字段都设为非空（headers 内含字符串、布尔、32 位与 64 位整数、嵌套表、字节数组），消费者读到的值与发布值相同。这是 RPC 的最低条件。

#### RQ-04 字段表解析不得破坏连接

- 现状：`decode_value/2` 遇到非 `S t I l F V` 的类型时返回 `{undefined, Rest}`，不消耗值的字节，后续键会错位，连接进程可能因匹配失败退出。回写整数一律编码为 32 位 `I`，超过 `2^31-1` 的 long 在回写时可能 `badarg`。
- 目标：实现规范中的字段类型 `t b B s u i I l f d D S x A T F V`（至少覆盖客户端高频的 `S t I l T x A F`）。未知类型在表长范围内跳过或回 503/连接错误，不能让进程崩溃。回写保持原类型，long 不得截成 32 位。
- 验收：用 Java 客户端发送 byte[] header、timestamp、long、数组和嵌套表，连接保持，投递端类型与值一致。故意插入保留类型码时，连接按规范报错或跳过该表，进程仍在。

#### RQ-05 mandatory 与 basic.return

- 现状：`basic.publish` 读了 mandatory、immediate 后丢弃。`queues_for` 为空时不存储、不通知。
- 目标：`mandatory=true` 且路由结果为空时，向同一通道发 `basic.return`（含 reply-code 312、原 routing key、原 body 与 properties）。`mandatory=false` 时静默丢弃。`immediate` 已从主流客户端移除，返回 `540 NOT_IMPLEMENTED` 或按 RabbitMQ 3.x 的拒绝方式处理，并在文档中固定一种。
- 验收：向不存在的 routing key 发布 mandatory 消息，客户端收到 return 且 body 一致；向已绑定队列发布则不收到 return。

#### RQ-06 vhost 隔离

- 现状：`connection.open` 把 vhost 放进 `_VHost`。全节点一张 `amqp_topo_tab`。
- 目标：不同 vhost 的同名交换机和队列互不可见。不存在或无权限的 vhost，`connection.open` 失败，reply-code 403，编码符合 RQ-20。默认至少支持 `/`。
- 验收：vhost `a` 与 `b` 各声明同名队列并各发布一条；对方消费者收不到。打开未创建的 vhost 被拒绝。

#### RQ-07 publisher confirm

- 现状：class 85 落在 `handle_channel_method/5` 的兜底子句，不回复。开启 confirm 的客户端会一直等。
- 目标：`confirm.select` 回 `select-ok`。此后每条 `basic.publish` 在 fsync 成功后发 `basic.ack`（delivery-tag 从 1 递增，支持 multiple）。入库失败发 `basic.nack`。与 `tx.select` 互斥。未 select 时行为与现在相同，不发 confirm ack。
- 验收：select 之后发布 100 条，ack 的 tag 覆盖 1..100；制造存储失败时收到 nack，且消费者收不到该条。未 select 的通道不会把 confirm ack 和消费 ack 混在同一语义里（消费 ack 仍只在 `basic.deliver` 之后）。

#### RQ-08 事务或显式拒绝

- 现状：class 90 被忽略。
- 目标：二选一，并写进能力声明与发布说明。
  - 实现：`tx.select` 后 publish/ack 对其他连接不可见，`tx.commit` 后可见，`tx.rollback` 丢弃。已 confirm 的通道再 `tx.select` 得到 406。
  - 不实现：`tx.select` 立即 `channel.close` 540，不得静默丢弃。
- 验收：与所选方案一致。静默忽略视为失败。

### 4.2 P1：协议面补齐

#### RQ-09 basic.get

- 现状：method 70 恒回 `basic.get-empty`。
- 目标：有就绪消息时回 `get-ok`（delivery-tag、redelivered、exchange、routing-key、message-count）加 content。`no-ack=false` 时该条进入 unacked，之后可以 ack。队列空时才 `get-empty`。
- 验收：发布 2 条，get 两次得到这两条且 message-count 递减；第三次 empty。ack 之后消息不再被 get 到。

#### RQ-10 与 RQ-11 recover 和 redelivered

- 现状：`basic.recover`（60/110）只回 `recover-ok`。`basic.deliver` 把 redelivered 编成 `0`。`nack(requeue=true)` 会 release，但再次投递仍标成未重投。
- 目标：recover 重新投递本通道所有未 ack 消息，redelivered=1，原 delivery-tag 作废并分配新 tag。nack/reject 重入队后再投同样标 1。首次投递为 0。
- 验收：消费不 ack，recover 后同一 body 再次到达且标志为 1。nack requeue 同样。

#### RQ-12 计数、purge、delete 真实作用到消息

- 现状：`queue.declare-ok` 的 message-count 恒为 0，consumer-count 只统计本连接。`queue.purge` 回 0，不删消息。`queue.delete` 回 0，只删 ETS。`if-unused` 只看本连接的消费者。
- 目标：
  - message-count 是就绪条数，不含未 ack。
  - consumer-count 是该队列上所有连接的消费者数。
  - purge 删除就绪消息并返回删除条数，未 ack 的保留。
  - delete 删除队列、绑定和全部剩余消息，返回删除的消息数。
  - `if-unused` / `if-empty` 在仍有消费者或仍有消息时回 406，且对象还在。
- 验收：两连接各挂一个消费者时，任一连接 declare 看到 consumer-count=2。发布 10 条、不 ack 地取走 3 条后 purge，返回 7，随后 ack 的 3 条仍可完成；delete 后再 passive declare 得 404，消息不可再读。

#### RQ-13 队列与消息参数

arguments 今天在 declare/bind/consume 处解析后丢弃（`{_Table, _}`）。`expiration` 也到不了 `expires_at`（发布路径写死 `undefined`）。

至少实现以下 RabbitMQ 常用参数。未实现的参数必须在 declare 时 406，禁止忽略后假装成功。

| 参数 | 要求 |
|---|---|
| `x-message-ttl` | 队列级毫秒 TTL，过期消息不可被投递 |
| `expiration` | 单条消息 TTL，与队列 TTL 取更早者 |
| `x-dead-letter-exchange`、`x-dead-letter-routing-key` | nack/reject 且 requeue=false、过期、队列长度溢出时死信 |
| `x-max-length`、`x-max-length-bytes`、`x-overflow` | `drop-head` 与 `reject-publish` |
| `x-expires` | 队列在无消费者闲置超时后删除 |
| `x-max-priority` | 1..255 内按优先级出队；不设置则保持 FIFO |
| `x-single-active-consumer` | 同时只有一个消费者收到消息，其取消后下一个接手，不丢未投递消息 |
| `alternate-exchange` | 交换机参数；本交换机无法路由时转过去 |
| `x-queue-type` | 见下 |

`x-queue-type`：若内部只有一种持久化队列，则 `classic` 映射到该实现；`quorum` 与 `stream` 在没有等价复制语义之前必须 406，不能建出一个行为不同的队列还回 declare-ok。

验收：TTL 到期进入指定死信交换机；`reject-publish` 在队列满时对已开启 confirm 的发布者 nack；优先级 10 先于优先级 1 出队。

#### RQ-14 headers 交换机

- 现状：类型可以写入 ETS，`key_matches(headers, ...)` 恒为 `false`。绑定表也不保存 arguments（`bind/4` 丢掉 `_Args`）。
- 目标：按 `x-match`=`all`（默认）或 `any` 比较绑定参数与消息 headers。`x-match` 本身不参与比较。
- 验收：all 在缺一个键时不投递；any 在命中一个键时投递。

#### RQ-15 预置交换机

- 目标：启动即存在且 `durable`：`amq.direct`、`amq.fanout`、`amq.topic`、`amq.headers`。删除它们或 default exchange 回 403。`amq.match` 可随 headers 规则一起提供。
- 验收：不声明交换机，直接把队列绑到 `amq.topic` 能收到匹配消息；删除 `amq.fanout` 失败且交换机仍在。

#### RQ-16 声明标志跨连接生效

- `exclusive` 队列：第二连接 declare 或 consume 失败；声明连接关闭后队列消失。
- `auto-delete` 队列：最后一个消费者取消后队列删除。
- consume 的 `exclusive`：已有消费者时第二个 consume 得到 403。
- `no-local`：为 true 时，不把本连接发布的消息投给本连接的这个消费者。
- exchange `internal`：客户端 `basic.publish` 得到 403。
- exchange.delete 的 `if-unused`：仍有绑定则 406。

验收：用两条连接分别覆盖上表每一行。

#### RQ-17 消费者取消通知

- 目标：队列被删除、或独占消费者被抢占时，其他连接收到 `basic.cancel`。server-properties 只有在实现之后才把 `consumer_cancel_notify` 设为 true。
- 验收：连接 A 在消费，连接 B 删除该队列，A 收到 cancel 且 tag 匹配。

#### RQ-18 QoS 与心跳

- 现状：`prefetch=0` 时 `room/3` 返回 32；拉取周期默认 100ms（`poll_ms`）。`basic.qos` 的 prefetch-size 与 global 被丢掉。`tune-ok` 的 heartbeat 写入 `_Heartbeat`，`send_tune/1` 固定发 0。
- 目标：
  - prefetch count 是通道级未 ack 上限。0 表示不限制，实现可以分批读，但不得在仍有就绪消息时停住。
  - 本阶段可以不实现 prefetch-size 与 global；若忽略，客户端设置非 0 的 prefetch-size 或 global=true 时回 540，避免假装生效。
  - heartbeat 取客户端与服务端建议值中的非零较小者。超时未收到帧则关闭连接。heartbeat 0 表示双方都关闭检测。
- 验收：prefetch=1 时第二条 deliver 出现在第一条 ack 之后。发布 1000 条、prefetch=0、消费速度低于生产速度时，最终仍收满。协商 heartbeat=2 秒后，暂停对端读取，服务端在约 2 个心跳周期内断开。

#### RQ-19 协议错误必须可见

- 目标：
  - 未知 method：`channel.close`，reply-code 505（或规范要求的连接级 505/501），带 class-id 与 method-id。
  - ack/nack/reject 未知或重复 delivery-tag：406。
  - `basic.cancel` 未知 consumer tag：404。
  - 软错误关通道，不关连接；帧错、心跳错关连接。
- 验收：对每个错误码有一条客户端用例，断言 close 的 code、class、method，且同连接的其他通道仍可用（通道级错误时）。

#### RQ-20 connection.close 编码

- 现状：认证失败时 reply-code 被 `encode_shortstr(<<"ACCESS_REFUSED">>)` 写成字符串，没有 class-id 和 method-id。正常关闭路径不解析客户端的 close 参数。
- 目标：`connection.close` 与 `channel.close` 均为：reply-code（short）、reply-text（shortstr）、class-id（short）、method-id（short）。认证失败使用 403，并在 close 之后关闭 TCP。成功路径对客户端的 close 回 `close-ok`。
- 验收：错误密码时，amqp091-go 报告的 code 为 403，而不是解码错误。抓包可见 short 403。

#### RQ-21 能力声明与真实行为一致

- 目标：`connection.start` 的 server-properties.capabilities 只包含已完成需求对应的键。建议键：`publisher_confirms`、`basic.nack`、`consumer_cancel_notify`、`exchange_exchange_bindings`、`authentication_failure_close`、`per_consumer_qos`、`direct_reply_to`。未完成的键省略或为 false。产品名可以仍是 UnifiedMQ，不伪装成 RabbitMQ。
- 验收：自动化测试读取 start 帧的表，与一份勾选了的能力清单完全一致。

#### RQ-22 direct reply-to

- 目标：在 RQ-03 完成后实现 `amq.rabbitmq.reply-to`。客户端把 `reply_to` 设为该值即可收到 RPC 应答，无需自己声明回复队列。该伪队列不出现在普通队列列表里。
- 验收：请求端不声明回复队列，服务端把 correlation_id 与 body 发回 `reply_to`，请求端在超时内收到且 correlation_id 匹配。

### 4.3 P2：运维兼容

这些不是线协议最小集。要对外说「可替换 RabbitMQ 节点」时才纳入同一里程碑。

#### RQ-23 资源保护

内存或磁盘超过阈值时，向已连接客户端发 `connection.blocked`，恢复后发 `unblocked`。阻塞期间的 publish 不得静默成功。`connection.update-secret` 若不做，按未知 method 走 RQ-19。

#### RQ-24 授权

在 vhost 内提供 configure、write、read 三类模式（正则或明确声明的等价模型）。无 write 权限的 publish 回 403；无 read 的 consume 回 403；无 configure 的 declare/bind 回 403。认证仍然复用 `broker_security`，不另做一套用户表。

#### RQ-25 管理接口子集

不要求复刻管理 UI。需要一套稳定 HTTP API，至少包括：列出 vhost、队列（名称、消息数、消费者数、参数）、绑定、连接；导入导出 definitions（vhost、权限、exchange、queue、binding）。端口与路径可以不同，但字段含义要在文档中给出到 RabbitMQ `/api/definitions` 的映射。

#### RQ-26 明确不在默认兼容承诺里的组件

下列能力未单独立项前，文档和 capabilities 都不得声称支持：Federation、Shovel、延迟消息插件、consistent-hash 交换机、消息追踪 `amq.rabbitmq.trace`、优先级之外的 quorum queue 复制细节、Stream 协议。

### 4.4 P3：非目标

- 不把 `rabbitmq-server` 源码合并进本仓库，也不追求文件级或模块级对应。
- 不与 RabbitMQ 节点组成混合集群，不实现其分布协议、feature flag 或 mnesia/khepri 模式。
- 不把 AMQP 1.0、RabbitMQ Stream、STOMP 作为 0-9-1 兼容的验收条件。本仓库的 MQTT 适配器单独评估。
- 不承诺 RabbitMQ 插件二进制可加载。
- `vendor/ra` 继续只作为本 Broker 的 Raft 实现，不把它包装成 RabbitMQ quorum queue 的对外类型，除非另起「队列类型」需求并满足 RQ-13 的拒绝规则。

## 5. 建议里程碑

| 里程碑 | 需求 | 完成标准 |
|---|---|---|
| M1 队列语义 | RQ-01、RQ-03、RQ-04、RQ-05、RQ-06、RQ-19、RQ-20、RQ-21 | 先发布后消费、属性与 RPC 关联字段、vhost、错误帧全部有失败即红的客户端测试 |
| M2 确认与取消息 | RQ-07、RQ-08、RQ-09、RQ-10、RQ-11、RQ-12 | confirm、get、recover、真实计数与 purge/delete |
| M3 持久化与参数 | RQ-02、RQ-13、RQ-14、RQ-15、RQ-16、RQ-17、RQ-18 | 重启用例与参数「不支持就 406」用例 |
| M4 RPC 便利与运维 | RQ-22、RQ-23、RQ-24、RQ-25 | direct reply-to 与 definitions 往返 |
| 不做 | RQ-26、RQ-27、RQ-28 | 在发布说明中列出 |

M1 未完成时，发布说明应继续使用现在的表述：AMQP 0-9-1 子集，仅保证「消费者已在线时的 publish / consume / ack」，不能写「兼容 RabbitMQ」。

## 6. 测试要求

在 `test/protocol-compatibility/amqp/` 增加用例，客户端保持 `amqp091-go`，并至少加一个 Java 客户端覆盖字段表类型（RQ-04）和 direct reply-to（RQ-22）。现有 13 个用例继续通过：

`amqp-connect`、`amqp-pubsub`、`amqp-direct`、`amqp-fanout`、`amqp-topic`、`amqp-competing-consumers`、`amqp-manual-ack`、`amqp-nack-requeue`、`amqp-unbind`、`amqp-queue-delete`、`amqp-passive-missing`、`amqp-prefetch-qos`、`amqp-multi-channel`。

每个新需求至少一条用例，断言客户端可见结果，而不是只断言内部 ETS 或游标。重启类用例必须 `kill -9` 数据目录所在节点，不能只停 gen_server。

建议补上的最小用例名：

| 用例 | 覆盖 |
|---|---|
| `amqp-backlog-before-consume` | RQ-01 |
| `amqp-restart-durable` | RQ-02 |
| `amqp-properties-roundtrip` | RQ-03 |
| `amqp-table-types` | RQ-04 |
| `amqp-mandatory-return` | RQ-05 |
| `amqp-vhost-isolation` | RQ-06 |
| `amqp-publisher-confirm` | RQ-07 |
| `amqp-tx-rejected-or-atomic` | RQ-08 |
| `amqp-basic-get` | RQ-09 |
| `amqp-recover-redelivered` | RQ-10、RQ-11 |
| `amqp-purge-counts` | RQ-12 |
| `amqp-ttl-dead-letter` | RQ-13 |
| `amqp-headers-exchange` | RQ-14 |
| `amqp-predeclared-exchange` | RQ-15 |
| `amqp-exclusive-autodelete` | RQ-16 |
| `amqp-cancel-notify` | RQ-17 |
| `amqp-heartbeat` | RQ-18 |
| `amqp-unknown-method` | RQ-19 |
| `amqp-auth-close-code` | RQ-20 |
| `amqp-reply-to` | RQ-22 |

## 7. 参考位置

| 行为 | 位置 |
|---|---|
| 范围说明与已知限制 | `docs/protocol-compatibility.md` §4.7 |
| 游标锚在尾部 | `apps/broker_subscription/src/broker_subscription.erl`、`broker_subscription_worker.erl` |
| 拓扑不落盘 | `apps/broker_proto_amqp/src/amqp_topo.erl` |
| 方法分发与空实现 | `apps/broker_proto_amqp/src/amqp_conn.erl` 中 `handle_channel_method/5` |
| 属性裁剪 | 同文件 `props_to_headers/1`、`headers_to_props/1` |
| 字段表 | `apps/broker_proto_amqp/src/amqp_codec.erl` |
| 现有真实客户端用例 | `test/protocol-compatibility/amqp/README.md` |
| 使用 ra 而非 rabbitmq-server | `docs/adr/003-use-rabbitmq-ra.md` |

规范依据：AMQP 0-9-1（类 connection、channel、exchange、queue、basic、tx、confirm）以及 RabbitMQ 对 0-9-1 的扩展（confirm、nack、mandatory 的 return、队列参数、consumer cancel、direct reply-to）。扩展以 RabbitMQ 3.13 文档为准；与规范冲突时，以 RabbitMQ 客户端实际依赖的行为为准，并在实现说明里记一笔。