# ADR-021：集群控制面、Metadata Raft 与 Fencing

- **状态**：已采纳（S4 冻结，2026-11-08）
- **日期**：2026-11-08
- **关联**：README §6/§11.3；ADR-002（Mnesia 只做控制面）、ADR-003（ra 3.2.0）、ADR-007（显式可靠性）、ADR-008（分组与存储）、ADR-010（路由边界）
- **关联文档**：[`docs/design/s4-cluster-execution-plan.md`](../design/s4-cluster-execution-plan.md)

## 背景（Context）

S1~S3 是单节点内核。S4 要补齐 README §6 的分布式能力：Mnesia 控制面、Metadata Raft
Group、分区归属/迁移、Epoch Fencing、失去多数派时的写入策略、以及“Producer ACK 与
实际持久化/复制等级一致”的可验证保证。S0.1 已实测 ra 3.2.0 在 OTP 28.3.1 上可形成
3 节点集群、Leader 切换后日志连续、失去多数派时写入被拒绝。

## 决策（Decision）

### D1. 控制面只进 Mnesia（ADR-002 不变）

11 张表：`tenant` / `broker_user` / `acl_rule` / `topic_meta` / `queue_meta` /
`binding_meta` / `subscription_meta` / `cluster_node` / `partition_meta` /
`config_version` / `config_outbox`。

- **不进 Mnesia**：消息正文、每次 ACK、每分区 cursor 的高频更新。它们仍走
  `broker_storage` / `broker_subscription` 的数据面。
- 表按节点列表复制；单节点默认 `ram_copies`（测试）/ `disc_copies`（生产配置）。
  schema 不存在时由 `broker_meta` 创建，存在时复用。
- 配置更新：`config_version` 单行 `{key, Version, UpdatedAt}`；`bump/1` 自增并把
  `{broker_config, Version, Changed}` 广播给 `broker_meta_config` 订阅者；
  `reconcile/0` 周期性对账（`reconcile_ms`，默认 5000）读取表内版本，发现与缓存不一致
  时补发通知。直接写 Mnesia 的另一节点也能被对账收敛。
- Outbox：`enqueue/2` 写一行 `{Seq, Version, Event, CreatedAt, PublishedAt=undefined}`；
  `broker_meta_outbox` 周期扫描未发布行，调用配置的 `{M,F}` 发布器，成功后写
  `PublishedAt`。没有配置发布器时保持 pending（不假装已发布）。

### D2. Metadata Raft Group 承载归属，不承载消息

- ra server id：`{broker_cluster_meta, node()}`；默认单成员 `[node()]`，可用
  `{members, [Node]}` 组 3 节点。
- 状态机状态：`#{partitions := #{Partition => #{owner, epoch, replicas, state}}}`。
- 命令：`{assign, Partition, Owner, Replicas, Epoch}`、`{migrate, Partition, From, To, Epoch}`、
  `{finish_migration, Partition, Epoch}`、`{remove, Partition}`。
- 查询：`owner/1`、`epoch/1`、`replicas/1`、`snapshot/0`；读走本地缓存，写走 ra。
- **每个数据面 append 不做 ra 共识**（ADR-008 §8.4）：ra 只管归属元数据；消息复制在
  `broker_cluster_rep` 里按复制组做批量/单条复制，本地仍是一次 WAL 追加。

### D3. Epoch 与 Fencing

- 每次归属变化（assign/migrate/finish）必须携带**更大的 epoch**；状态机拒绝不递增的写入。
- 状态机 `apply/3` 在**每个副本节点**上更新本地 `broker_cluster_cache`（ETS），因此正常
  在线节点在归属变化被提交后立刻看到新 owner/epoch。
- 数据面守卫：`broker_cluster:authorize_append(Partition, Epoch)`。epoch 不匹配或本地
  不是 owner 时返回 `{error, {fenced, #{partition, expected_epoch, actual_epoch}}}`，
  队列拒绝该次 append（发布者拿不到 ACK）。
- **分区隔离的旧 Owner**：缓存不会更新，单靠缓存不能阻止它继续写。S4 的答案是
  **可靠写入必须走 `replicated_commit`**：旧 Owner 失去多数派后拿不到复制 quorum，
  append 返回错误、不 ACK。`fsync` 单节点档不承诺跨分区一致（文档明写）。
- 归属迁移采用两阶段：`migrate`（标记 `migrating`，epoch+1，目标成为 owner，源仍可读）
  → 数据追上后 `finish_migration`（清状态、epoch+1）。迁移期间写入由目标 owner 接收。

### D4. 失去多数派

- `broker_cluster:write_mode/0` 在 `assign/migrate` 等元数据写入超时（`quorum_timeout_ms`，
  默认 5000）后返回 `{error, no_quorum}`。
- 配置 `{on_quorum_loss, reject | os_buffer_only}`：
  - `reject`（默认）：`replicated_commit` append 直接拒绝；
  - `os_buffer_only`：允许 QoS0（`os_buffer`）继续，可靠等级仍拒绝。
- 连接层不静默降级：拒绝时 MQTT 5 回 `0x83`，3.1.1 断开；不会把可靠写入降成 `fsync`
  再 ACK。

**【冻结后补充，2026-11-09】** 上面两条中，`broker_cluster:write_mode/0` 这一 API 名**未按字面实现**：实际是 `assign/4`、`migrate/4`、`finish_migration/2`、`remove/1` 这些元数据命令在 `quorum_timeout_ms` 超时后返回 `{error, no_quorum}`（`broker_cluster_meta:no_quorum/0`，同时累加 `cluster_no_quorum`）。`{on_quorum_loss, reject | os_buffer_only}` **运行时开关未实现**，已从 `broker_cluster.app.src` 移除（配置项必须有消费者）；当前行为等价于 `reject` 档——`replicated_commit` 在多数派失败时返回 `{error, {replication_failed, Acks, Needed}}` 且不 ACK，而 `os_buffer`（QoS0）之所以能继续，是因为它根本不进入复制器，不是策略分支。补上策略开关列为 S6 之后。

### D5. 复制与 ACK 等级

- `replicated_commit` 的实现：本地 `fsync` 追加成功 → `broker_cluster_rep:replicate/2`
  把同一消息发给 `replicas -- [node()]`，每个副本在本地 `fsync` 追加 → 收到
  `quorum = (N div 2) + 1` 个成功（含本地）才返回 `{ok, Offset}`。
- 副本集合来自 Metadata Raft 的 `replicas`；默认 `[node()]`（单副本时 quorum=1，等价
  本地 fsync）。
- 失败路径返回 `{error, {replication_failed, Acks, Needed}}`；本地已写入的副本保留
  （不会回滚已 fsync 的数据），但**调用方不得 ACK 发布者**，由重试/对账处理。
- 队列不直接依赖 `broker_cluster`：通过 `persistent_term` 注册两个钩子
  `{broker_queue, append_guard}` / `{broker_queue, replicator}`；应用未启动时是 no-op，
  保持 S1~S3 的单节点行为不变。

### D6. Mnesia / ra 边界与失败恢复

- `broker_meta` 启动失败（schema/表损坏）是**启动失败**，不静默重建空表。
- ra 的状态目录由 `ra` 自己管理（`data_dir/ra`）；单节点重启后 `ra:start_server` 恢复
  日志并重放状态机，缓存重建。
- 快照：ra 的 snapshot 命令只包含归属元数据（很小），不包含消息。
- 磁盘满/刷盘失败：数据面 append 返回错误，不 ACK；控制面写入失败返回错误，不广播版本。

### D7. ACL 与租户

- `broker_meta` 的 `broker_user` 只存 `crypto:hash(sha256, Password)`，不存明文；
  `broker_security` 仍是数据面认证入口，S4 增加从 `broker_meta` 读取用户的适配
  （配置 `{source, mnesia}`；默认仍是静态 env，保持 S1~S3 行为）。
- ACL 规则按 `{Tenant, Principal, Action, Resource}` 存储，效果 `allow | deny`；
  授权仍只使用认证主体，不读取事件属性（ADR-020 §D9）。

## 后果（Consequences）

- **+** 归属/epoch 有一致性来源，Fencing 可测试；控制面与数据面解耦。
- **+** `replicated_commit` 的 ACK 语义可被测试证明，不再只有 `os_buffer`/`fsync` 两档。
- **-** 单节点 `fsync` 档在网络分区下不保证 split-brain 安全；文档必须写明，可靠场景要求
  `replicated_commit` + 奇数副本。
- **-** ra 每组有固定开销，分区到组的映射留给运维配置；S4 只实现组内归属，不做自动
  再平衡。
- **-** Mnesia 表结构一旦上线需迁移脚本；S4 以 `vsn` 属性记录表版本。

## 验证（Validation）

- `broker_meta_SUITE`：11 张表、版本广播、周期对账、outbox 发布/重试、用户哈希。
- `broker_cluster_SUITE`：单节点 assign/epoch、Fencing 拒绝旧 epoch、迁移两阶段、
  失去多数派拒绝元数据写、复制 quorum 下 ACK 只会在 quorum 后返回、副本不足时不 ACK。
- 分布式套件：`peer` 起 3 个节点形成 ra 集群；杀 Leader 后重新选举；停止多数派后
  写入被拒；旧 owner 写入被 `{fenced, _}` 拒绝。
- 回归：S1~S3 的 422 个用例保持通过（钩子默认 no-op）。
