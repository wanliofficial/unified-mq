# ADR-011：`broker_storage` behavior、能力模型与磁盘协议

- **状态**：**已采纳（S1 T0.5 冻结，2026-10-21）**——F1/F2 评审整改已并入；附录 11.7 的 Gate 措辞解释随设计文档 v0.3 复审确认，正式签认仍在 T17 评审会
- **日期**：2026-10-21
- **关联**：README §5.3（存储方案）/§6.3（ACK 与持久化承诺）；roadmap §3.2 抽象 3、§3.4 Gate；ADR-007（三档可靠性）、ADR-008（方案 A 自研 WAL）、ADR-009（内存模型）；S0.3 spike；设计文档 `docs/design/s1-mqtt-vertical-slice.md` §3.3、§5.3、§7

## 背景（Context）

`broker_storage` 是三个抽象中影响面最大的：S2 的可插拔存储、S4 的 Raft 复制后端、以及所有协议 Adapter 的 ACK 契约都压在这一个 behavior 上。roadmap §3.2 只给了五个动词「append / read_from / ack / truncate / recover」，S0 的证据与 ADR-007/008 的决策使以下几处必须收紧：

1. **`ack` 的归属含糊**。roadmap 把 `ack` 放进 storage behavior，但 README §5.2 要求订阅侧有 Exclusive/Shared/Failover 多种消费语义，对应**多个独立 cursor**。若 storage 负责逐订阅 ack 状态，则 storage 必须理解订阅模型（职责越界）；若不管，则 `ack` 这个词的含义要重定义。
2. **可靠性等级如何进入 API**。ADR-007 定了 L1/L2/L3 三档并规定「ACK 返回时机严格绑定等级，不得提前 ACK」。但 roadmap §3.2 的 `append` 签名没有等级参数——等级若来自全局配置，per-message 契约就无从表达（ADR-009 §9.2 已定为 per-message）。
3. **恢复返回的形状**。`recover` 若返回"已 ack 的 offset 集合"，在长期运行的分区上是百万级列表，内存不可承受；且 v0.2 草案遗留的 `AckRanges`（非连续区间集合）与本 ADR"storage 只持单一连续前缀"的决策**自相矛盾**，同时 `recover` 未返回续写位置，恢复后无法确定下一个 offset。此外 S0.3 §3 明确记录了 `kill -9` **无法**证明 `os_buffer` 档的丢失窗口，恢复语义必须把"哪些承诺已被实测支持"写清楚。
4. **磁盘格式的演进权**。roadmap §3.2「定稿后不改」若把 `#broker_message{}` 直接序列化落盘，S2 加一个字段就等于宣布历史段不可读。

## 决策（Decision）

### 11.1 behavior 定义（`apps/broker_storage/src/broker_storage.erl`）

```erlang
-type handle() :: term().   % 后端私有；每分区一个

%% 仅在显式供给新分区时调用；目标目录已存在则报错，绝不覆盖
-callback create(PartitionId :: partition_id(), Config :: map()) ->
    {ok, handle()} | {error, {already_exists, partition_id()}} | {error, term()}.

%% 唯一的冷启动入口：重放段文件并返回句柄与两个水位
-callback recover(PartitionId :: partition_id(), Config :: map()) ->
    {ok, handle(),
     CommittedOffset :: offset(),   % 全局已确认水位
     HighOffset :: offset()}        % 下一条追加将使用的 offset
      | {error, {not_found, partition_id()}} | {error, term()}.

-callback close(handle()) -> ok.

-callback append(handle(), [#broker_message{}], sync_mode()) ->
    {ok, [offset()]} | {error, {unsupported_sync_mode, sync_mode()}} | {error, term()}.

-callback read_from(handle(), FromOffset :: offset(), Limit :: pos_integer()) ->
    {ok, [{offset(), #broker_message{}}], NextOffset :: offset()} | {error, term()}.

-callback advance_committed(handle(), Offset :: offset()) -> ok | {error, term()}.

-callback truncate(handle(), BeforeOffset :: offset()) -> ok | {error, term()}.

-callback capability(handle()) -> [sync_mode()].
```

roadmap §3.2 的 `ack` **改名为 `advance_committed`**（语义收紧见 11.2），`open` **拆为 `create` / `recover`**（见 11.3）。

### 11.2 `create` 与 `recover` 必须分离（F2 整改）

**启动协议**：分区 Worker 冷启动**只调 `recover/2`**。`recover` 找不到分区目录时返回 `{error, {not_found, PartitionId}}`，**不得**创建空分区。新建分区是显式的管理动作，只经 `create/2`。

理由：若两者合一（"open 时目录不存在则建空的"），配置里写错一个分区 ID 或挂载点缺失，节点会**恢复出一个空分区并对客户端报告健康**——这是静默数据丢失，不是可观测的启动失败。ADR-008 §8.3 选方案 A 的代价之一就是恢复正确性由本项目自负，此类歧义必须在 API 层堵死。

### 11.3 storage 只管全局 committed watermark

**决策**：storage 不感知订阅模型，只维护一个**全局 committed watermark** = 所有订阅都已确认的连续前缀。

- per-subscription cursor 归 `broker_subscription`（S1 落 `.cursor` 文件，每订阅一个标量 offset；S2 迁 Mnesia），storage 不提供逐订阅 API。
- `advance_committed/2` 由 subscription 层聚合后调用，语义是"此 offset 之前对所有订阅者都已确认"。
- 该切分使 S4 复制层可以自然对齐：**共识提交点 = committed watermark**，无需为 Raft 引入第二套水位概念。

判据：`broker_storage` 的任何实现都不得引用 `SubRef`、`Subscription` 等订阅侧类型。

**watermark 的持久化与非对称容忍**：committed watermark 持久化在分区 `meta` 段，随 group commit 节拍刷盘，不逐条 fsync。回退方向有严格约束——

| 恢复后 watermark 偏差 | 后果 | 允许？ |
|---|---|---|
| **偏低**（实际已确认更多） | 重复投递，至少一次语义内 | ✅ 允许 |
| **偏高**（声称已确认但未确认） | `truncate` 提前丢弃未消费消息 = **数据丢失** | ❌ 绝对禁止 |

实现必须保证 watermark 只在对应 offset 的 ack 证据落盘后才推进，禁止内存领先。此不对称是 ADR-007 精神在消费侧的对应物。

**不设 AckRanges（F1 整改）**：storage 只有单一连续前缀，因此 `recover/2` 返回两个标量水位即可。原 `AckRanges`（非连续已 ack 区间集合）在本 ADR 的模型下**没有归属者**——全局水位是标量、per-sub cursor 是标量、逐 offset ack 状态 storage 不持有。故从 API 移除，v0.2 引入它所要解决的"百万级 offset 列表内存爆炸"问题（D10）在此被**结构性消除**而非缓解。`recover/2` 同时补上 `HighOffset`，否则恢复后无法确定追加位置（原 API 缺口）。

### 11.4 sync_mode 是 per-call 参数 + 能力双层校验

- `append/3` 第三参数即 ADR-007 的等级，**per-message**（ADR-009 §9.2）。
- `capability/1` 是**实例级**（绑定 handle，非模块级）：允许同一后端在不同磁盘/durability 配置下暴露不同能力。
- **双层校验**：
  - Adapter 层缓存能力做**早失败**（MQTT：`mqtt_max_qos` 在 SUBACK 阶段拒绝不支持的 QoS）；
  - storage 层运行时**兜底**：收到超出 capability 的 sync_mode **必须**返回 `{error, {unsupported_sync_mode, _}}`，**禁止静默降级**。
  - 缓存仅为优化，正确性由兜底保证；两者不一致以运行时为准。
- **禁止静默降级是本 ADR 的硬约束**：ADR-007 的整个价值建立在"ACK 时机与等级严格绑定"上，静默降级会让 Producer 拿到超出实际耐久性的承诺。

**S1 两后端能力**：

| 后端 | capability | 说明 |
|---|---|---|
| `broker_storage_memory` | `[os_buffer]` | 部署此后端时 listener 必须配 `mqtt_max_qos = 0` |
| `broker_storage_wal` | `[os_buffer, fsync]` | fsync 走 group commit |
| （S4 新增）`broker_storage_replicated` | 追加 `replicated_commit` | **不改 behavior**，S1 该模式返回 `unsupported_sync_mode` |

### 11.5 磁盘协议独立于内存模型

WAL 记录 = `<<Magic:8, Vsn:8, CRC32:32, Len:32>>` + `erlang:term_to_binary(BodyTuple)`，Body 元组**独立定义**且覆盖 `#broker_message{}` 全字段（含 `payload_encoding`、`persistence`，不得依赖反推）：

```erlang
{Vsn, MsgId, Tenant, Destination, Payload, PayloadEncoding,
 Persistence, ProtoMeta, Headers, Reliability, CreatedAt, ExpiresAt, EventBlob}
```

- `wal_codec:encode/1` / `wal_codec:decode/2` 桥接内存模型与磁盘元组。
- 加字段 = 递增 `Vsn` + 增槽位；解码走 `case Vsn of` 兼容分支。
- `Vsn > 支持的最大版本` → `wal_recovery` **拒绝启动并报警**；`Vsn < 最大` → 兼容读入，新段用最新 Vsn 写出。
- **此决策取代设计文档原 OQ-1**：S1 保留 Erlang external term format，通过"元组 vs 记录"解耦解决版本兼容；CBOR/protobuf 待 S6 有跨语言读取需求时再评估。

### 11.6 `truncate/2` 只设逻辑边界

`truncate` 语义 = "此 offset 之前不再可被 `read_from`"，**不删除文件**。S1 段文件永不物理清理；retention 与段回收在 S2 通过**新增** optional callback 实现，不改现有签名（ADR-008 §8.3 警告不得因使用 Raft 而省略日志段管理，故此处刻意保留演进位）。

配套读路径：每分区维护稀疏间隔索引（默认每 256 条一个 `{AnchorOffset, FilePos}` 锚点，设计文档 §7.2），writer 追加时更新、recovery 重放段头重建。无索引则 `read_from` 退化为全段扫描，group commit 省下的 fsync 被读放大吃回。

### 11.7 附录：Gate「订阅位置不丢」的措辞解释（v0.3 复审已确认）

roadmap §3.4 第 2 条「已 ACK 消息不丢、订阅位置不丢、未 ACK 消息重投」必须与第 3 条「明确可容忍的重复窗口」合并读：

| roadmap 原文 | 契约 | 断言 | 容忍度 |
|---|---|---|---|
| 已 ACK 消息不丢（Producer 侧 PUBACK 过） | 消息 durability | R1a | **零容忍** |
| 订阅位置不丢 — 订阅**集合** | 订阅注册 durability | R1b | **零容忍**（`.subs` 即改即 fsync） |
| 订阅位置不丢 — 消费 **cursor** | 消费位置 durability | R3 | 有界回退 ≤`cursor_max_regress`（默认 64 / 100 ms） |
| 未 ACK 消息重投 | 至少一次语义 | R3(ii)+R7 | 重投是**契约行为**，非缺陷 |

依据：若 cursor 位置零容忍，第 3 条的"可容忍的重复窗口"就无从定义而成空文。故"不丢"约束的是 **durability**（位置持久存在、崩溃后不重置归零），而非 **恰好一次投递**。cursor 回退与 Producer 侧"已 ACK 不丢"分属两个承诺，不可互借措辞。

参数可逼近零回退：`cursor_max_regress=1` + `cursor_flush_interval_ms=0` → cursor 逐条 fsync，代价与 group commit 收益对冲。`.inflight` 不提供零回退档（MQTT 3.1.1 §4.1 本就允许 session 恢复时重发未确认消息）。

## 后果（Consequences）

- **+** storage 与订阅模型解耦，S2 四种订阅语义与 S4 复制都无需改 behavior。
- **+** `capability/1` + 禁止静默降级，使 ADR-007 三档契约在 API 层可被测试逐档证明（S2 Gate 硬项）。
- **+** 磁盘协议版本化，S2 加字段不再是破坏式变更。
- **+** watermark 用标量而非区间集合，恢复内存恒定；`create`/`recover` 分离把"配置写错导致静默空分区"从隐性数据丢失变成显式启动失败。
- **-** `advance_committed` 需 subscription 层聚合连续前缀，是额外逻辑；聚合错误会表现为"提前 truncate 丢消息"，S2 必须重点测。watermark 只能偏低不能偏高（11.3 表）是实现上的持续约束。
- **-** S1 `truncate` 不删文件 → 长跑磁盘占用单调增长，dev/test 环境需手动清理或 S2 前不做长跑。
- **-** 逐条 fsync 的 L2 吞吐仅 ~1148 msg/s（S0.3 §2.1），系统实际能力依赖 group commit 调参正确；参数是全局旋钮，需文档化（ADR-008 同类问题）。
- **-** Erlang external term 磁盘格式跨语言不可读；若未来要求非 Erlang 组件直接消费 WAL，需 S6 重新评估。

## 验证（Validation）

- **语法已实测**（冻结前，OTP 28.3.1 `erlc`）：§11.1 的 8 个 callback 全部编译通过，`behaviour_info(callbacks)` 返回 `[{create,2},{recover,2},{close,1},{append,3},{read_from,3},{advance_committed,2},{truncate,2},{capability,1}]`；`#broker_message{}` 在 callback 类型位中使用亦通过。

- **S0.3 已支持**：fsync 871 µs/msg、group commit batch=32~64 拐点 ~19944 msg/s（17×）、20k 记录恢复 143 ms、撕裂尾部干净截断。
- **S1 T5**：多写者并发 append 正确性——批次内每个 caller 的回复严格发生在 fsync 之后、offset 无 cross-reply、混合 sync_mode 并发不互相饥饿。**这是 S0.3 §6 明确遗留的待办**（spike 为单写者模型），必须在前置给任何性能断言之前完成。
- **S1 T5（F2）**：`recover/2` 指向不存在/空目录时必须返回 `{error, {not_found, _}}` 且**不创建**任何文件；`create/2` 对已存在分区必须拒绝。测试用错误 partition id 与缺失挂载点两个用例锁死该行为。
- **S1 T5（F1）**：`recover/2` 返回的 `HighOffset` 必须等于最后一条已落盘记录的 offset+1，重启后追加不产生 offset 重用或空洞。
- **S1 T14**：崩溃后 committed watermark **只允许偏低**；构造"ack 已发生但 meta 段未刷盘"的注入用例，断言重启后表现为重复投递而非消息缺失。
- **S1 T13**：同一测试套件在 memory 与 WAL 两后端互换通过；`capability` 不匹配时返回错误而非降级。
- **S1 T14（R1a/R1b/R2~R7）**：崩溃恢复契约逐档证明；R3 用默认档与零回退档各跑一轮。
- **S1 边界**：`os_buffer` 档真实丢失窗口**未被证明**（S0.3 §3：`kill -9` 只销毁进程，已 `write(2)` 的数据存活于 page cache）。S1 不得对外声称 os_buffer 档的丢失数字；掉电模拟（dm-flakey / VM 硬复位）在 S2 补齐。
- **S2 需证明**：三档可靠性逐档自动化测试（roadmap §4.3）；`advance_committed` 聚合正确性；retention 的 optional callback 扩展不改现有签名。
- **S4 需证明**：`replicated_commit` 作为新后端 capability 接入；共识提交点与 committed watermark 对齐；旧 Owner Fencing 拒绝写入。
