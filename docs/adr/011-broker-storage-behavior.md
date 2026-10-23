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

**【冻结后缺口批注 T4，2026-10-21｜H8——T5 必答】** 分离本身成立，但本 ADR 与 §6 都没规定**首次部署时谁调 `create/2`**。按 §6 boot step 1"逐分区 `recover/2`，缺分区即失败"，干净数据目录启动的 broker **必然**启动失败；内存后端同样无法通过 step 1（它从来没有人 create 过分区）。T5 必须显式选定一种 bootstrap 路径——管理命令 / `partition_bootstrap` 配置 / 用数据目录里的标记文件区分"首次启动"与"恢复"——并保证选择结果**不是**"recover 顺手创建"，否则本节整改作废。在此之前，`broker_storage_sup` 不接任何后端的启动接线。

**【冻结后批注 T5，2026-10-21｜H8 已答复】** 三个候选里的**管理动作**被选定，另两个被否决：

- **选定**：`create/2` 的唯一入口是带外管理动作 `broker_queue:create_partition/1`（**T6 交付**），不出现在任何 boot 路径。
- **否决 `partition_bootstrap` 配置**与**数据目录首启标记**：两者都让启动路径在"目录不存在"时自己建分区，等价于"recover 顺手创建"的带开关版本，本节整改作废。
- **首启与恢复无需靠数据目录内容区分**——差别只在于有没有人显式 `create` 过。干净数据目录 boot 得到 `{error, {not_found, _}}` → application 启动失败并告警，这是本节要求的**响亮失败**，不是待修 bug。
- **boot step 1 的执行者不是 `broker_storage_sup`**：分区清单 `{partitions, N}` 配在 `broker_queue` 的 application env，存储层看不到它，故"逐分区 `recover/2`"的循环归 `broker_queue_sup`（设计文档 §6 批注）。`broker_storage_sup` 只按 `backend` 起自己需要的那一个子进程。
- **顺带修掉 T4 的真实缺陷**：内存后端此前从未有任何角色启动 `broker_storage_memory_store`，装配后的 release 里每次 `broker_storage_memory:create/2` 都是 `noproc`（CT 全绿，因为套件在自己进程里手工建表）。现由 `broker_storage_sup_SUITE` 钉住"每个后端启动它的 `create/2` 所需进程"。
- **OTP 28.3.1 实测的两个 supervisor 事实**（影响 §11.2 的错误元组能否原样出到调用方）：`start_child` 失败**不留 spec**（所以"先 `recover` 再 `create`"的顺序安全）；子进程启动函数返回 `{error, Reason}` 会被包成 `{error, {Reason, #child{}}}`，适配层必须剥包装才能保住本节承诺的 `{error, {not_found, PartitionId}}`。

**【冻结后批注 T6，2026-10-22｜本节的分离已在装配后的 release 上实测两半】** H8 的答复不再是纸面结论：

- **前半（缺分区必须响亮失败）**：`rebar3 release` 后在干净目录启动 → 退出码 1，crash report 为 `{{shutdown,{failed_to_start_child,{partition,0},{partition_not_found,0}}},{broker_queue_app,start,[normal,[]]}}`，且 **`data/` 未被创建任何内容**。本节要的"启动失败而不是恢复出空分区并报告健康"在真实 release 上成立。CT 侧由 `broker_queue_SUITE:boot_fails_loudly_when_a_partition_was_never_created` 钉住（同时断言"失败**且**不留痕"）。
- **后半（`create/2` 是唯一入口）**：带外 `create_partition(0..3)` 后再启动 → 11 个 application 全部运行，`broker_queue_sup` 有 4 个分区 worker + 1 个注册表，`broker_storage_wal_sup` 有 4 个 writer。`create_partition(9)` → `{error,{unknown_partition,9}}`（不在配置清单里的 id 一律拒绝，因此不会留下"永远没有 worker 去看"的孤儿目录）；对已存在分区 → `{error,{already_exists,0}}`。
- **本节承诺的两个错误元组都原样出到了调用方**：`{not_found, Id}` 未被 supervisor 的 `{Reason, #child{}}` 包装吞掉（上一条批注要求的剥包装已实现），`{already_exists, Id}` 同样原样返回。
- **带外路径的两个实测前提**（设计文档 §13 J8/J9）：`create_partition/1` 需要 `broker_queue` 已 **load**（不必 start）；执行时的 CWD 必须与节点启动后的 CWD 一致，因为 relx 生成的启动脚本会 `cd "$ROOTDIR"`，相对 `data_dir` 因此落在 release 根目录。

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

**【冻结后批注 T4，2026-10-21｜H9/H10】** 上表的两个方向原本只是文字约束，签名层面无从阻止传任意 offset。T4 把它落成可在冻结签名内机械检查的两道守卫（error 项类型是 `term()`，不构成 API 变更）：

- `advance_committed/2` 拒绝 `Offset > High - 1`（尚未写入的 offset 不可能有 ack 证据）与向后退；
- `truncate/2` 拒绝 `Before > Committed + 1`（等价于"丢弃未确认消息"，正是绝对禁止的那一格）。

`broker_storage_memory` 的 T4 用例已锁这三条；**T5 的 WAL 后端必须通过同一组用例**，否则按缺陷处理。同一组用例已抽为可复用契约头 `apps/broker_storage/test/broker_storage_contract.hrl`（后端套件只提供 `?BACKEND`/`?CAPABILITY` 与 `backend_setup/1`、`backend_cleanup/1`、`backend_config/1`；头文件内不出现任何后端名/表名/目录名），所以"必须通过同一组用例"是字面可执行的：T5 交付判据 = 该头文件在 `broker_storage_wal` 上 13/13 通过。另：offset 基数本 ADR 未冻结，S1 实现定为**从 1 开始**，`Committed = 0` 表示"从未确认过任何一条"，空分区 `recover` 返回 `CommittedOffset = 0, HighOffset = 1`——§8.2 R3 的 `max(0, last_flushed_offset - K)` 按此解释。

**【冻结后批注 T5，2026-10-21｜13/13 已实测】** 上述交付判据已满足：`broker_storage_contract.hrl` 的 13 个用例在 `broker_storage_wal`（`?CAPABILITY = [os_buffer, fsync]`）上**一条未改、一条未跳**地通过，水位两向守卫与 `truncate/2` 边界由机械检查覆盖。WAL 侧另有一条本节未预见的实现约束：**committed watermark 不能只靠重放段文件推出**，否则恢复代价与日志长度成正比；故新增 `wal_meta`（分区目录内 `meta.log`，定长水位记录），`recover/2` 读水位成为一次定长读。尾部状态三分类 `clean | {torn, truncated} | {corrupt, term()}`，torn 按 §11.5 截断、corrupt 拒绝启动。

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

**【冻结后批注 T5，2026-10-21｜"禁止静默降级"的三条落地规则】** WAL 后端 `capability/1` 实测返回 `[os_buffer, fsync]`；能力检查发生在 writer **分配 offset 之前**，所以被拒的模式不消耗 offset（下一个 caller 仍拿到它被承诺的号）。本节那句硬约束在 T5 变成了读/写路径上三条可测的规则：

1. **完整但不可解码的记录 → 报错，不返回短页**：`read_from/3` 返回 `{error, {corrupt_record, Path, Reason}}`。把它当日志末尾返回是静默降级——订阅者会永远等一个**确实存在**的 offset。
2. **torn tail（writer 正跑在前面）→ 与日志末尾不可区分，正常返回**：这是并发写的常态而非损坏。`HighOffset` 刻意**不进读路径索引**，因此内存后端那个"offset 已分配、消息尚未插入"的观察窗口（设计文档 §7.1）在 WAL 后端**结构性不存在**。
3. **写或 fsync 失败 → 致命不重试**：offset 是先分配后落盘发出去的，失败后继续服务等于把已承诺的 offset 变成永久空洞。writer 于是 `{stop, Reason, ...}` 自杀，批内所有 caller 收 `{error, {writer_stopped, Reason}}`，由 supervisor 重启后从磁盘重建状态。

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
- **S1 T5**：多写者并发 append 正确性——批次内每个 caller 的回复严格发生在 fsync 之后、offset 无 cross-reply、混合 sync_mode 并发不互相饥饿。**这是 S0.3 §6 明确遗留的待办**（spike 为单写者模型），必须在前置给任何性能断言之前完成。<br>**→ 已满足（2026-10-21 实测）**：`broker_storage_wal_SUITE:mixed_sync_modes_concurrently_yield_dense_offsets_and_readable_records`——16 个并发 caller 交替 `fsync`/`os_buffer` 各 2 条，32 个 offset 恰为 `1..32`（无洞无重）、`NextOffset = 33`、每个 caller 只在自己的 offset 上读到自己那条 payload。未发现需上调 `batch_delay_ms` 的延迟退化，故 §7.2 的默认值不动。
- **S1 T5（F2）**：`recover/2` 指向不存在/空目录时必须返回 `{error, {not_found, _}}` 且**不创建**任何文件；`create/2` 对已存在分区必须拒绝。测试用错误 partition id 与缺失挂载点两个用例锁死该行为。<br>**→ 已满足（13 个契约用例在 WAL 后端原样通过）**：错误 partition id 得 `{error, {not_found, 7}}` 且目录未被创建；已存在分区 `create/2` 得 `{error, {already_exists, 7}}`。**诚实边界**：原文所说的"缺失挂载点"未用真实卸载挂载点的方式实测——它在实现上等价于"目录不存在/为空"，由同一 `not_found` 断言覆盖；另有一条本节未要求的守卫已实测：数据目录落在 `tmpfs`/`ramfs` 上时 `create/2` 返回 `{error, {volatile_filesystem, Dir}}` 且不建目录（S0.3 §5 陷阱）。
- **S1 T5（F1）**：`recover/2` 返回的 `HighOffset` 必须等于最后一条已落盘记录的 offset+1，重启后追加不产生 offset 重用或空洞。<br>**→ 已满足**：除契约用例外，`records_written_through_the_backend_survive_an_independent_recovery` 绕过 behavior 直接调 `wal_recovery:recover/3`，在 3 条 fsync append + `advance_committed(1)` 后得 `#{high => 4, committed => 1, truncate_before => 1}`；`a_crashed_writer_is_restarted_and_the_log_survives` 用 `kill -9` 杀 writer，重启后 `recover` 得 `{0, 4}` 且三条 payload 完好。
- **S1 T14**：崩溃后 committed watermark **只允许偏低**；构造"ack 已发生但 meta 段未刷盘"的注入用例，断言重启后表现为重复投递而非消息缺失。
- **S1 T13**：同一测试套件在 memory 与 WAL 两后端互换通过；`capability` 不匹配时返回错误而非降级。
- **S1 T14（R1a/R1b/R2~R7）**：崩溃恢复契约逐档证明；R3 用默认档与零回退档各跑一轮。
- **S1 边界**：`os_buffer` 档真实丢失窗口**未被证明**（S0.3 §3：`kill -9` 只销毁进程，已 `write(2)` 的数据存活于 page cache）。S1 不得对外声称 os_buffer 档的丢失数字；掉电模拟（dm-flakey / VM 硬复位）在 S2 补齐。
- **S2 需证明**：三档可靠性逐档自动化测试（roadmap §4.3）；`advance_committed` 聚合正确性；retention 的 optional callback 扩展不改现有签名。
- **S4 需证明**：`replicated_commit` 作为新后端 capability 接入；共识提交点与 committed watermark 对齐；旧 Owner Fencing 拒绝写入。
