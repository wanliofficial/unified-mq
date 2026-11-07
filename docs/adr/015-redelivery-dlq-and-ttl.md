# ADR-015：重投递、死信队列与消息 TTL 的归属与形状

- **状态**：**已采纳（S2 M0 冻结，2026-11-07）**——用户签认；后续修改须走 ADR 修订流程
- **日期**：2026-11-07
- **关联**：README §5.2/§5.3/§10.1；roadmap §4.1（消息生命周期）；ADR-009（`expires_at` 槽位）、ADR-013（SubRef 与 cursor）；设计文档 `docs/design/s2-single-node-kernel.md` D4 / §4.5

## 背景（Context）

S1 的重投递只有一条路径：**断线重连按 cursor 重发**（R7）。没有重试定时器、没有重试上限、没有 DLQ、没有 TTL：

- `#broker_message.expires_at` 被 ADR-009 冻结为"保留字段"，S1 恒 `undefined`。
- 连接进程内没有重试定时器；S1 已知限制 L8 明确记录"连接级定时器会随连接死亡丢失"。
- 投递失败的语义只有"cursor 不前进"，没有"重试 N 次后进死信"。

S2 要引入重投递、DLQ、TTL，必须先冻结三件事：**重试状态归谁**、**DLQ 是什么**、**TTL 以哪个时间戳为准**。

## 决策（Decision）

### 15.1 重试状态归 `broker_redelivery`（`broker_subscription` 内），不在连接进程

- 新表（ETS + 持久化分片）：`{SubRef, Partition, Offset} -> #{attempts, first_at, next_at, reason, consumer}`。
- **定时器由 `broker_subscription_worker` 单进程持有**（或按分区分片），不每个连接一个定时器。连接只是"执行者"：收到唤醒后检查自身是否仍持有该订阅的消费权，再投递。
- 理由：S1 L8 的教训——重试状态若在 `mqtt_conn_fsm` 里，连接死亡 = 重试计划丢失；而重试计划必须比连接活得久（这正是它存在的意义）。
- 持久化：重试状态与 cursor 同层（`.cursors` 或独立分片，由 S2-E 实现决定），但**必须**在重启后恢复，否则重启会丢失退避进度。

### 15.2 退避与上限

- 退避：`delay = min(base_ms * 2^(attempts-1), max_delay_ms)`，`base_ms` 默认 1000，`max_delay_ms` 默认 60000，可配。
- 重试上限：`max_attempts` 默认 10，可配；达到上限后消息进入 DLQ（15.3），cursor 前进，不再阻塞分区。
- 退避使用**可注入时钟**（测试用 `meck` 或显式 `now_ms` 参数），使时序测试无需真实 sleep。

### 15.3 DLQ：按租户可配命名的普通 destination，保留期独立

用户已拍板（O2）：**命名按租户可配 + 独立保留期**。

- **默认形状**：`$dlq/{tenant}/{original_destination}`。
- **可配**：`broker_dlq.name_pattern` 支持模板变量 `{tenant}`、`{original}`、`{qos}`、`{kind}`，例如 `$dlq/{tenant}/{kind}/{original}`。模板渲染结果必须是合法 destination 且不包含通配符 `+`/`#`。
- **复用普通存储**：DLQ 消息走正常 `append`（同一 WAL 路径、同一 `broker_storage`），**不引入第二套 I/O 或独立 app**。这样可以复用恢复、retention、metrics、订阅机制。
- **消息包装**：原消息 payload 保持不变；`headers` 增加 `x-original-destination`、`x-failure-reason`（如 `max_attempts`、`rejected`）、`x-attempts`、`x-tenant`。`message_id` 重新分配（不覆盖原 ID，原 ID 放 `x-original-message-id`），`created_at` 为进入 DLQ 的时间。
- **保留期独立**：`broker_dlq.retention` 单独一组 `max_bytes`/`max_age_ms`（形状同 ADR-014），不与源 destination 共享。DLQ 的 retention 仍受 ADR-014 §14.1 的 committed 约束（DLQ 也是普通 destination，有自己的订阅与 cursor）。
- **可订阅**：DLQ 是普通 destination，客户端可正常 SUBSCRIBE；运维重放由 S6 的工具实现，不在 S2 承诺。

### 15.4 TTL：`expires_at` 生效，比较 `created_at`

- `expires_at` 由发布侧或策略设置（协议层/接入层），单位是绝对毫秒时间戳（与 `created_at` 同基准）。
- **过滤点两处**：
  1. `read_from` 返回前：过期消息不返回，但 cursor 仍前进（视为"已消费"），避免卡住分区；
  2. 投递前（`broker_subscription` → conn）：二次校验，覆盖"读出后、投递前"过期的窗口。
- **retention 协同**：段内消息全部过期且满足 ADR-014 §14.1 时，随段删除；个别过期不触发段删除。
- **不影响 committed**：过期消息被跳过并推进 cursor，committed 正常前进，不会因 TTL 卡住 retention。
- **`expires_at` 为 `undefined`** 表示不过期（S1 默认行为不变）。

### 15.5 拒绝/失败路径不丢数据

DLQ 是"不丢数据"的兜底，但 DLQ 自身 append 失败（磁盘满、storage 不可用）时：

- 消息**不**从原分区删除（不推进该条 cursor）；
- 记 `dlq_append_failed` 计数与错误日志；
- 达到上限的重试条目保持原状，等待下次重试，**不静默丢弃**。

## 后果（Consequences）

- **+** 重试计划比连接活得久，连接风暴/断连不会丢重试进度。
- **+** DLQ 复用普通存储与订阅，恢复路径只有一套；运维可像普通 topic 一样消费 DLQ。
- **+** TTL 在 read 与投递两处过滤，不卡 cursor、不阻塞 retention。
- **+** 按租户可配命名满足多租户隔离，保留期独立避免 DLQ 挤占业务磁盘。
- **-** 重试状态需要持久化，增加 `.cursors`/分片的大小与恢复时间。
- **-** DLQ 是"普通 destination"，意味着它也会被 retention、背压、metrics 一视同仁；需要 S2-G 防止 DLQ 成为热点。
- **-** 模板命名让 destination 集合动态增长，Router 索引与 metrics 标签需注意基数（不得把任意 destination 作为高基数标签）。
- **-** 单进程定时器在订阅数极大时可能成为热点；S2-G 观测后再决定是否分片。

## 验证（Validation）

- **单元（假时钟）**：`broker_subscription_SUITE` 新增——退避序列 `1s, 2s, 4s, ...` 与上限、`max_attempts` 后转 DLQ、连接死亡后重试计划仍在、重启后恢复退避进度。
- **DLQ**：断言默认与自定义 `name_pattern` 的渲染结果、`headers` 四个字段、DLQ 可被普通订阅消费、DLQ append 失败时不推进 cursor 且计数增长。
- **TTL**：`read_from` 跳过过期并推进 cursor；投递前二次过滤；`expires_at = undefined` 不过期；过期不影响 committed。
- **多租户**：两个租户各自的 DLQ destination 与保留期互不影响（O2 的直接验收）。
- **集成**：T13 扩展"消费者持续拒绝 → 重试 → DLQ → 另一订阅消费 DLQ"的端到端链路。
- **文档**：`docs/storage-and-recovery.md` 增加 DLQ 与 TTL 的存储/恢复说明；`docs/architecture.md` 增加 `broker_redelivery` 的进程/数据流。
- **Gate S2**：本条为 Gate 2（慢消费者/热点分区不 OOM）的证据之一——重试与 DLQ 不得导致内存或磁盘无界增长。
