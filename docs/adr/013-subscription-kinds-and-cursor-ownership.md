# ADR-013：订阅类型、cursor 归属与会话文件 v2 迁移

- **状态**：**已采纳（S2 M0 冻结，2026-11-07）**——用户签认；后续修改须走 ADR 修订流程
- **修订**：§13.2 的 cursor 行形状、§13.3 的 Shared 分配路径、§13.5 的会话文件布局由 **ADR-019（D-1~D-3）** 修订；本文其余条款继续有效
- **日期**：2026-11-07
- **关联**：README §5.2（订阅与 cursor 抽象）/§7.2；roadmap §4.1（订阅抽象）；ADR-009（`#broker_message{}`）、ADR-010（Router 只要求 `sub_ref()` 不透明）；设计文档 `docs/design/s2-single-node-kernel.md` D2 / D9 / §4.4

## 背景（Context）

S1 只有一种订阅语义：**Exclusive**。`SubRef = {ClientId, Filter}`（K2），cursor 按客户端落 `.cursors`，committed 聚合为 `min(所有 live cursor) - 1`。这个形状有两个在 S2 必然暴露的问题：

1. **Shared 订阅没有位置**。Pulsar 风格的 `Shared` 是"一条消息在组内只投给一个 consumer"，cursor 的 owner 必须是**订阅组**而不是某个客户端；若继续用 `{ClientId, Filter}`，同一组的两个客户端会各自持有 cursor，组内投递与水位无法一致。
2. **Slow consumer 放大重投递窗口**。若 Shared 只维护"组内最慢 consumer 的标量 cursor"，一条卡住的 consumer 会把整个组的水位钉死，所有其他 consumer 的 ack 都无法推进 committed。

用户已就 shared ack 语义拍板（O1）：**per-message ack + 最小连续前缀（Pulsar 风格）**。本 ADR 因此要冻结：`sub_ref` 形状、cursor 的 owner 与数据结构、三种订阅类型的投递规则，以及 `.subs`/`.cursors` 从 v1 到 v2 的迁移方式。

## 决策（Decision）

### 13.1 `SubRef = {Kind, Key, Filter}`，cursor 归属 SubRef

```erlang
-type sub_kind() :: exclusive | shared | failover.
-type sub_ref()  :: {sub_kind(), Key :: binary(), Filter :: binary()}.
```

| Kind | Key | 含义 |
|---|---|---|
| `exclusive` | `ClientId` | S1 语义，一个 SubRef 一个 consumer |
| `shared` | `Group` | 组内竞争消费，一条消息只投给一个 consumer |
| `failover` | `Group` | 组内主备，active consumer 消费，其余 standby |

- **单一真相**：cursor 的 owner 就是 SubRef。不另建"cursor 表按 Group"的第二份真相。
- **`broker_cursor` 是 `broker_subscription` 的内部模块**，不是新 app。roadmap §4.1 提到的 "`broker_subscription` + `broker_cursor`" 在本 ADR 落成"同一 app 内的两个模块/命名空间"，避免出现第二个 cursor owner。
- **Router 不受影响**：ADR-010 §10.1 只要求 `sub_ref()` 是不透明 `term()`；形状改变不触发 ADR-010 修订。Router 仍只做 `Filter -> [SubRef]` 索引。

### 13.2 cursor 数据结构：Exclusive = 标量前缀；Shared = 位图 + 最小连续前缀

> **2026-11-07 修订（ADR-019 §19.1）**：`shared` 行的实际形状是 `#{kind, low, acked, assigned}`，`acked` 是有序 offset 集合而非分块位图（`shared_state_max` 已给窗口收口，位图在低密度下纯亏）。「per-message ack + 最小连续前缀」的语义不变；下面的位图描述保留为**该语义的说明**，不是落盘形状。

cursor 按 `{SubRef, Partition}` 维护，每种 Kind 的推进规则不同：

| Kind | 状态 | `committed` 定义 |
|---|---|---|
| `exclusive` | `Offset`（下一个待读位置） | `Offset - 1`（与 S1 等价） |
| `shared` | `#{Low, Acked :: bitmap/chunked_set, Consumers}` | 从 `Low` 起的**最小连续前缀**末位 |
| `failover` | `Offset`（仅 active consumer 推进） | `Offset - 1`；standby 不推进 |

**Shared 的位图**：

- 每条 offset 记录"是否已被组内某个 consumer ack"，不记录是谁 ack 的（重投递只需要知道"是否已 ack"；消费者归属由 `broker_redelivery` 按需记录，见 ADR-015）。
- 位图按固定块分配（默认 256 位/块，可配 `broker_subscription.shared_bitmap_block_bits`），只保留 `[Low, High]` 窗口内的块；连续前缀推进后丢弃左侧块，避免无限增长。
- `Low` 之前的数据视为已 committed，不再接受 ack（幂等忽略 + 计数）。
- 乱序 ack 不会推进 `Low`；空洞被后续 ack 填满后，前缀一次性前进。

**为什么不用"组内 min cursor"**：一条慢 consumer 会把 min 钉住，导致所有其他 consumer 的 ack 都无效、积压持续增长、retention 无法回收。位图 + 连续前缀让"个别慢 consumer"只影响它自己没 ack 的那几条，不让整组停摆。代价是状态更大，故有块上限与快照策略（13.5）。

### 13.3 投递规则

> **2026-11-07 修订（ADR-019 §19.1）**：「轮询选择 consumer」不足以决定唯一投递——本项目的连接自己 pull，共享 cursor 会被两个成员读到同一条。实际机制是 **worker 原子 `claim/3`**：一次领一页 offset 记入 `assigned`，pull 只读领到的那些。选定 consumer 的动作因此发生在 claim 里，而不是在 dispatch 里。

- **Exclusive**：唯一 consumer；consumer 不在线时消息留在日志里，重连按 cursor 补发（S1 行为）。
- **Shared**：组内按 round-robin（`order` 稳定排序，默认按加入顺序）选择**在线且未超过 inflight 上限**的 consumer；一条消息在组内只投一次。consumer 死亡后其未 ack 的消息进入重投递（ADR-015），不重新广播给全组。
- **Failover**：按 `order` 最小的存活 consumer 作为 active；active 死亡后下一个存活者接管，从**已 committed 位置**继续（不是从 active 死亡瞬间的位置），避免漏消息、允许少量重复。

### 13.4 `$share` 语法与 SUBSCRIBE 解析

- Shared 订阅的线协议入口：MQTT 5 的 `$share/{group}/{filter}`（S2.5 启用）。
- MQTT 3.1.1 **不引入**非标准 `$share`（避免与真实客户端的 topic 命名冲突）；3.1.1 客户端只能创建 Exclusive。S2-D 的 Shared 逻辑先由内核测试与内部 API 覆盖，S2.5 再接线上语法。
- 解析出的 `{shared, Group, Filter}` 直接作为 SubRef；`$share` 前缀不进入 Router 的 filter 索引（Router 看到的是真实 Filter），由 `broker_subscription` 在解析时剥离。

### 13.5 持久化：`.subs` / `.cursors` 升 `vsn 2`

> **2026-11-07 修订（ADR-019 §19.3）**：文件名确定为 `"<kind>.<b64url(Key)>.<b64url(Filter)>.subs"`，不用 `base64url(term_to_binary(SubRef))`（后者跨 OTP 版本不保证稳定）；`.inflight` **保持按 client**，不随 SubRef 拆分（它是 conn FSM 的 blob，一条 pending 可同属多个 SubRef）；`.cursors` 不落 `assigned`；迁移是「先写 v2、全部成功后删 v1」的幂等流程。下面的 v1/v2 形状描述仍有效。

现有文件格式是 `term_to_binary({vsn, 1, ClientId, Body})`。S2 改为：

```erlang
%% .subs
{vsn, 2, SubRef, #{kind, key, filter, qos,
                   consumers => [ClientId],      %% Exclusive 恒一个；Shared/Failover 为组内成员
                   created_at,
                   expires_at => undefined}}     %% S2.5 session expiry 预留位（ADR-017 §17.5）

%% .cursors
{vsn, 2, SubRef, #{partition => committed_or_next_offset,
                   kind => sub_kind(),
                   shared => #{low => Offset, acked => ChunkedBitmap} | undefined,
                   updated_at => Timestamp}}

%% .inflight 的 v2 见 ADR-012 §12.2
```

- 文件名按 **SubRef 的稳定编码**生成（`base64url`，沿用 `broker_subscription_files` 现有的"名字只是地址、内容才是真相"原则）；Exclusive 的 SubRef 含 ClientId，因此与 S1 的文件一一对应。Shared/Failover 的 SubRef 含 Group，组内多个 consumer 的会话信息归并到该 SubRef 的文件，而不是每个 client 一份 cursor。具体编码函数由 `broker_subscription_files` 收敛，本 ADR 只固定"文件名必须能由 SubRef 唯一重建"。
- **写入永远 v2**；**读取接受 v1 与 v2**。
- **v1 → v2 迁移在 boot step 2 内一次性完成**：读到 v1 时按 `{ClientId, Filter}` 构造 `{exclusive, ClientId, Filter}`，写回 v2，删除旧文件。
- **迁移失败拒绝启动**（S1 K5 原则），不回退到空会话；错误信息必须包含文件路径与原因。
- 迁移是**单向**的：S2 写出的数据不能被 S1 读取（S1 会报 `unsupported_vsn`），这是可接受的，因为版本回退不是支持的运维动作。

### 13.6 committed 与 retention 的交互

- committed 仍是**全局每分区一个水位**：`min(所有 live SubRef 的 committed) `。Shared 的 `committed` 是"连续前缀末位"，Failover 只算 active。
- "live" 的定义：有至少一个在线 consumer，或订阅本身仍存在（离线但未过期）。**离线 Shared 组**若没有 live consumer，是否钉住 committed 由配置决定：默认钉住（与 S1 一致，离线会话可追赶），可通过 `broker_subscription.pin_offline_cursors = false` 让离线组不阻塞 retention。
- retention 的物理删除仍不得越过 `min(live committed)`（ADR-014）。

## 后果（Consequences）

- **+** 三种订阅语义有统一的数据模型与单一 cursor 真相。
- **+** Shared 的慢 consumer 只影响自己未 ack 的条目，不会钉住整组。
- **+** SubRef 形状改变不触发 ADR-010 修订；Router 仍是纯索引。
- **+** `.subs`/`.cursors` 已有 `{vsn,1,...}` 迁移位，升级路径自然。
- **-** Shared 位图增加内存与快照成本；积压大时恢复时间上升，需要块上限与 S2-I 压测。
- **-** 文件名从 ClientId 变为 SubRef 编码，运维排查（按客户端找文件）需要工具支持。
- **-** v1 数据一旦被 S2 打开就无法回退到 S1；发布说明必须写清。
- **-** Failover 的"从 committed 续消费"意味着 active 切换允许重复，与 QoS 至少一次一致；若未来要更精确，需引入 per-consumer 位置（本 ADR 不做）。

## 验证（Validation）

- **单元**：`broker_subscription_SUITE` 新增——三种 Kind 的 SubRef 铸造与解析、Shared 位图在"乱序 ack / 空洞填补 / consumer 退出 / 重复 ack"四场景下的 committed 正确性、`Low` 左侧 ack 被幂等忽略。
- **迁移**：构造 v1 的 `.subs`/`.cursors`/`.inflight` 目录，启动后断言全部为 v2、内容等价、旧文件删除；构造损坏 v1 文件，断言**拒绝启动**且错误含路径。
- **投递**：Shared 组内不重复（N consumer 收到 N 条不同消息）、consumer 死亡重投、Failover 主备切换。
- **恢复**：T14 新增/扩展——Shared 位图在 kill -9 后从 `.cursors` 恢复，committed 与崩溃前一致（允许重投未 ack 条目）。
- **路由**：boot step 2+3 回填后 `route/3` 返回新形状的 SubRef，且 Router 代码零改动。
- **性能**：S2-I 用积压 × consumer 数两维压测位图内存；报告进 `docs/benchmarks/`。
- **Gate S2**：本条为 Gate 2（慢消费者/热点分区不 OOM）的结构前提；证据 = Shared 慢 consumer 场景的 RSS/积压曲线。
