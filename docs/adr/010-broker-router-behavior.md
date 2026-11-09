# ADR-010：`broker_router` behavior 与路由/订阅索引职责边界

- **状态**：**已采纳（S1 T0.5 冻结，2026-10-21）**——F3/F5 评审整改已并入；后续修改须走 ADR 修订流程；**T17 复核（2026-11-06）**：5 个回调与 `route_ctx` 字段一致，未触发修订；**S4 冻结后补充（2026-11-09）**：分区归属由 `broker_cluster` 承担，`on_partition_change/2` 保持 optional 未实现，见 §10.5 批注
- **日期**：2026-10-21
- **关联**：README §2.1（分层职责）/§9.1（发布生命周期第 6 步）；roadmap §3.2 抽象 2；ADR-001（Adapter 与内核分离）、ADR-008（分组模型）；设计文档 `docs/design/s1-mqtt-vertical-slice.md` §3.2

## 背景（Context）

roadmap §3.2 对 Router 的定义是一行：「destination → `{partition, subscription}` 列表」。这个定义有意含糊，S1 必须收敛三件事，否则 S2/S4/S5 会各自按不同理解实现：

1. **Router 是否持有订阅状态**。返回值里同时出现 partition 与 subscription，可以读成"Router 拥有订阅表"，也可以读成"Router 只提供一个查询结果"。前者让 Router 变成第二个 subscription 模块（两处 cursor/状态真相）；后者职责清晰但要求 Router 至少维护一份 filter→订阅 的索引。**必须选一个并写死。**
2. **ACK 决策、存储选择是否进 Router**。README §2.1 已警告「协议 Adapter 不应直接操作底层磁盘格式；Raft 不应替代每条消息的业务处理」，但未对 Router 划界。若 Router 参与 ACK 时机或可靠性判定，ADR-007 的三档契约就会出现第二个决策点。
3. **分区归属变更如何传播**。ADR-008 定了分组模型与故障域，S4 有分区迁移与 Epoch Fencing；若 Router 的 route/3 签名无法承载"归属变化"事件，S4 就得改签名——而 roadmap §3.2 规定 S1 定稿后不改。

## 决策（Decision）

### 10.1 behavior 定义（`apps/broker_router/src/broker_router.erl`）

```erlang
-record(route_ctx, {
    tenant_id   :: binary(),
    publisher   :: principal() | undefined,   % ADR-009 类型
    reliability :: sync_mode()                % ADR-007/ADR-009
}).
-type route_ctx() :: #route_ctx{}.   % 【G1】记录名不自动产生同名类型，须显式别名

-type router_env() :: term().      % 实现私有；S1 = 持有 ETS 表的进程标识/表参考
-type sub_ref()    :: term().      % 订阅标识，由调用方生成（见 10.2 末）
-export_type([router_env/0, sub_ref/0, route_ctx/0]).

-callback init(Config :: map()) -> {ok, router_env()} | {error, term()}.

%% 读路径：在调用者进程内直接执行，不经过任何 Router 进程
-callback route(Destination :: binary(), Ctx :: route_ctx(), router_env()) ->
    {ok, [{partition_id(), [sub_ref()]}]} | {error, term()}.

%% 写路径：登记已存在的 SubRef 到 filter 索引；SubRef 由调用方生成
-callback register(router_env(), Filter :: binary(), SubRef :: sub_ref(), SubOpts :: map()) ->
    ok | {error, term()}.

-callback unregister(router_env(), SubRef :: sub_ref()) -> ok | {error, term()}.

-callback on_partition_change(router_env(), Event :: term()) -> ok | {error, term()}.
-optional_callbacks [on_partition_change/2].
```

**【冻结后批注 T3，2026-10-21｜H6】`route_ctx` 的定义位置**：上面这段把 record 写在 `broker_router.erl` 模块内，编译能过，但 **record 不跨模块可见**——协议 Adapter 与 `broker_queue` 都无法构造 Ctx，§5.1(c) 的调用形式在代码里写不出来。故 record 与 `-type route_ctx()` 原样移入 `apps/broker_router/include/broker_router.hrl`，behavior 模块 include 该头文件。**字段、类型标注、callback 签名一字未动**，仅定义位置变。（`broker_message.hrl` 必须先 include，本头的字段标注复用 `principal()`/`sync_mode()`。）

**回调不接收也不返回 `State`**（F3 整改）：Router 状态由实现自持（S1 = ETS 表），callback 只吃 `router_env()` 这个不透明句柄。这一点必须钉死，因为它锁定了**读路径不得进程串行化**：

> `route/3` 由发布者进程**直接调用**（普通函数调用 + ETS 并发读），**不是** `gen_server:call` 到 Router 进程。若把路由做成单一 gen_server，所有 publish 会串行化过一个进程，直接违反 README §7.2「避免单一 GenServer 成为所有路由和存储请求的全局瓶颈」与 roadmap §4.3 Gate S2 第 4 项。索引写入（register/unregister）才经单一写者进程串行更新 ETS，读侧用 `ets:lookup` 无锁并发。

**`SubRef` 由调用方生成，Router 不铸造**（F5 整改）：订阅实体的真相在 `broker_subscription`，若 Router 自行生成 SubRef，则两个模块需协商 ID 空间，违反 10.2 的"Router 不得调用 subscription API"判据。流程固定为：

1. `broker_subscription` 创建订阅实体，生成 `SubRef`；
2. `broker_subscription` 调 `broker_router:register(Env, Filter, SubRef, Opts)` 把 SubRef 登记进索引；
3. 发布时 `route/3` 原样返回这些 SubRef；
4. 撤销时 `broker_subscription` 调 `unregister/2`，并在断连/清理时自行删除实体。

Router 因此**从不**解引用 SubRef，只把它当不透明 key 存取。

### 10.2 职责边界（本 ADR 的核心决策）

**Router = 「路由 + 订阅索引」，不是「订阅状态持有者」。**

| Router 负责 | Router 不负责（归属） |
|---|---|
| destination → `partition_id` 的映射算法 | 消息持久化与 sync 路径（ADR-011 storage） |
| filter → `SubRef` 列表的匹配索引 | `SubRef` 对应的 cursor、消费者 PID、in-flight 集合（`broker_subscription`） |
| 通配符/规则匹配的语义正确性 | ACK 时机与可靠性等级判定（ADR-007，由 Adapter + queue 决定） |
| 订阅登记与撤销的**索引维护**（register/unregister） | 订阅实体的创建、销毁与 ID 铸造（`broker_subscription`） |
| 索引读取（`route/3`，调用者进程内并发） | 投递、重投、背压、DLQ（`broker_subscription` / S2 内核） |

`SubRef` 是一个**不透明标识**，由 `broker_subscription` 创建订阅实体时生成（见 10.1 末的流程），Router 只把它作为 key 存取 `Filter → [SubRef]` 倒排索引，**从不**解引用去读 cursor 或消费者状态。判据（可机械检查）：`broker_router` 的任何实现都不得调用 `broker_subscription` 或 `broker_storage` 的 API。

### 10.3 S1 默认实现：`broker_router_hash`

- **路由**：`Destination` 哈希取模到 N 个 `partition_id`，N 由 config（默认 4）。哈希函数选择需在实现中固定并可配（为 S4 分区迁移留位）。
- **匹配**：MQTT 3.1.1 §4.7 通配符——`+` 单层、`#` 多层；`route/3` 返回 topic 命中的所有 SubRef。
- `partition_id` 是纯整数（ADR-009 §9.2），哈希结果不编码进 ID 结构。

### 10.4 通配符匹配归属 Router（暂定，OQ-4）

S1 把 MQTT 通配符匹配放在 Router。已知张力：S2 引入 Shared subscription（Pulsar `Shared`、AMQP 队列竞争消费，README §5.2）后，"消息该投给哪个订阅实例"与"哪些订阅匹配该 topic"会变成两层选择，届时可能需下沉到 Subscription。

**处置方式**：这是**非破坏式**演进——S2 通过**新增** optional callback（如 `select_subscriber/2`）实现下沉，`route/3` 的签名与返回形状不变。因此 S1 可以安全冻结，不需要为未知需求提前抽象。

### 10.5 `on_partition_change/2` 为 optional hook

S4 分区迁移/归属变化需要通知 Router 更新 `Destination → Partition` 映射。S1 该 callback **允许不实现**（`-optional_callbacks`），S4 补 `broker_router_hash` 实现即可，**不修改既有 callback 签名**。这是 roadmap §3.2「定稿后不改」与 S4 已知需求之间的唯一让渡点。

**【冻结后补充 S4，2026-11-09】** 上面「S4 补 `broker_router_hash` 实现」的路线已被 S4 实际实现取代，**冻结文本不改**。S4 没有让 Router 感知分区归属：归属真相在 `broker_cluster`（ra Metadata Raft Group → `broker_cluster_cache` 本地镜像 → `broker_cluster:authorize_append/2`），数据面经 `broker_queue` 的 `persistent_term` 钩子 `{broker_queue, append_guard}` / `{broker_queue, replicator}` 接入（ADR-021 §D2~§D5）。`route/3` 的 `Destination → Partition` 仍是 `phash2(Destination, N)` 纯函数，迁移只改变 `(owner, epoch)`、不改变分区编号，Router 本来就没有需要更新的映射。因此 `on_partition_change/2` 继续作为 `-optional_callbacks` 保持未实现：`broker_router_hash` 不导出它，由 `broker_router_hash_SUITE:on_partition_change_stays_unimplemented_in_s1` 锁定。`route/3` 签名与 behavior 一字未改，§10.5「不修改既有 callback 签名」的目标仍然成立；不再适用的只有「S4 补实现」这一句。

### 10.6 Router 索引不持久化，重启由订阅层重建

Router 的索引状态在内存（ETS），**不落盘**。冷启动顺序固定：

1. `broker_storage:recover/2` 逐分区取回水位（ADR-011 §11.2）；
2. `broker_subscription` 读取全部 `sessions/*.subs` 文件，重建订阅实体并生成 SubRef；
3. 每个实体调 `broker_router:register/4` 回填索引；
4. Router 索引就绪后，listener 才开始接受 CONNECT。

**这是 Gate R1b（订阅集合不丢）的实现路径**：真相源是 `.subs` 文件，Router 只是它的派生索引。若实现者把索引本身持久化到 Router，就会与 `.subs` 形成两份真相并可能在崩溃后不一致——故本节明确 Router **无自己的持久化状态**，`create`/`recover` 一类 callback 不出现在本 behavior 中。

第 4 步的"就绪前不接连接"必须是硬顺序：若 listener 先开，早到的 publish 会路由到空索引，消息虽落 WAL 但无人投递，且 cursor 语义尚未恢复。

## 后果（Consequences）

- **+** 订阅状态只有一个真相来源（`broker_subscription`），Router 是纯索引，无状态复制问题。
- **+** 读路径无进程串行化，满足 README §7.2 与 Gate S2 第 4 项；Router 不构成全局瓶颈。
- **+** 「Router 不得调用 storage/subscription API」是可机械检查的边界（依赖图 + review），ADR-001 的分层约束在 Router 层落地。
- **+** optional hook 让 S4 归属变更在不破坏 behavior 的前提下接入。
- **-** 索引需启动重建，订阅数大时冷启动时间随 `.subs` 文件数线性增长；S1 分区/订阅规模小可接受，S2 若订阅量级上升需评估批量载入。
- **-** 写路径单进程串行：高频 SUBSCRIBE/UNSUBSCRIBE（如连接风暴）会与路由读争用 ETS 写锁；S1 不优化，观测后再决定是否分片索引。
- **-** `route/3` 每次返回 SubRef 列表，热点 topic 订阅数大时列表开销线性增长；S1 不优化，S2 观测后决定是否引入分发缓存。
- **-** 通配符匹配的正确性完全压在 Router 一处，MQTT `+`/`#` 与未来 AMQP routing key、NATS subject 的语义差异必须在各 Router 实现内部消化，Router 抽象本身不提供跨协议统一的匹配语法。
- **-** OQ-4 未决：若 S2 确认匹配需下沉，S1 的 Router 匹配代码部分作废（可接受，重写量小）。

## 验证（Validation）

- **语法已实测**（冻结前，OTP 28.3.1 `erlc` + `behaviour_info`）：`callbacks = [{init,1},{route,3},{register,4},{unregister,2},{on_partition_change,2}]`，`optional_callbacks = [{on_partition_change,2}]`。**G1 整改**：`route_ctx` 仅定义为 record 时 `-callback route(..., Ctx :: route_ctx(), ...)` **编译失败**（`type route_ctx() undefined`——Erlang 的 record 名不自动产生同名类型），故 §10.1 补 `-type route_ctx() :: #route_ctx{}.'` 与 `-export_type`。

- S1 单元测试（T3）：通配符匹配黄金用例——`sport/tennis/player1` vs `sport/tennis/+` vs `sport/+` vs `/#`、`+/+` 与非法 `sport+/` 等 MQTT §4.7 边界。
- S1 集成测试（T13）：一条消息命中多订阅时 Router 返回列表完整且去重。

  **【冻结后勘误 T3，2026-10-21｜H7】** 这条把两件不同的事记在了 Router 名下，其中一半 Router 做不到：
  - **能做到且已测**（T3 单元）：结果完整 + 同一 SubRef 不重复。`register/4` 对已登记的 SubRef 再登记第二个 filter 返回 `{already_registered, _}`（否则索引与 `.subs` 会形成两份真相），所以"同一 SubRef 跨 filter 重复"在结构上不可能发生，去重是**平凡成立**的。
  - **做不到**：同一 client 持有 `sport/#` 与 `sport/tennis/+` 两个重叠订阅时，MQTT §4.8 要求"按 max QoS 投递一次"。这两个订阅是两个 SubRef，合并它们**必须解引用 SubRef**，正被 §10.2 禁止。该职责归 `broker_subscription`（T7 铸造 SubRef 时即知所属 client），Router 只提供完整列表。T13 的断言因此要分两层写。

- **【冻结后实测补充 T3】** `behaviour_info(callbacks)` 返回**含 optional 的全部 5 项**，`optional_callbacks` 返回其子集 `[{on_partition_change,2}]`——核对 optional 语义时以后者为准。`broker_router_hash` 在 S1 不导出 `on_partition_change/2`，由 T3 用例锁定（§10.5）。
- **【S1 T17 冻结契约复核，2026-11-06】** 对最终代码机械核对：`broker_router:behaviour_info(callbacks)` 恰为 `[{init,1},{route,3},{register,4},{unregister,2},{on_partition_change,2}]`，`optional_callbacks = [{on_partition_change,2}]`；`#route_ctx{tenant_id, publisher, reliability}` 字段与类型标注与 §10.1 一致（定义位置按 H6 批注在 `include/broker_router.hrl`，仅位置变）；`apps/broker_router/src/` 对 `broker_storage`/`broker_subscription` 的引用只出现在注释里，§10.2 的边界可机械检查成立。**未触发修订**。
- **【冻结后实测补充 T3｜H9】** `partition_id` 基数冻结文本未定，S1 实现定为 **0-based**（`erlang:phash2(Destination, N)` 天然产出 `0..N-1`，不做 +1 偏移）。`route/3` 只按 destination 决定分区，`Ctx#route_ctx.tenant_id` 在 S1 **不参与路由**（security 是单用户桩），`SubOpts` 亦不入库——多租户的 filter 隔离是 S2 待办，届时 tenant 需进入索引 key。
- S1 集成测试（T13，F5）：SubRef 由 subscription 生成、Router 原样返回；断言 Router 模块中不存在生成 SubRef 的代码路径。
- S1 崩溃恢复（T14 R1b，§10.6）：kill -9 重启后索引由 `.subs` 完整重建，且**在索引就绪前 listener 不接受任何连接**（用启动期立即连接的客户端验证被拒或等待）。
- S1 review（T1）：依赖图检查 `broker_router` 不引用 `broker_subscription`/`broker_storage` 模块（§10.2 边界机械化）。
- S2 需证明：Shared subscription 可在不改 `route/3` 签名的前提下接入（否则本 ADR 需修订）。
- S2 基准需证明：随发布者进程数增加，`route/3` 吞吐线性提升、无单点饱和（README §7.2 四约束逐条检查之一）。
- S4 需证明：分区迁移经 `on_partition_change/2` 生效，且未实现该 callback 的旧 Router 后端在启动时被显式拒绝而非静默错误路由。

  **【冻结后补充 S4，2026-11-09】** 这条按 S1 冻结时的假设书写，S4 实际路线见 §10.5 冻结后补充：分区迁移的生效点与「旧写入被拒绝」都由 `broker_cluster` 承担，而不是 Router。迁移后旧 owner 按错误 epoch 写入返回 `{error, {fenced, #{expected_epoch, actual_epoch, owner}}}`——`broker_cluster_SUITE:fencing_rejects_a_stale_epoch` / `fencing_rejects_a_non_owner`、`broker_cluster_dist_SUITE:old_owner_is_fenced_after_migration`。由于 `on_partition_change/2` 始终 optional 且 Router 不持有归属，不存在「未实现该 callback 的旧 Router 后端」这一对象；「静默错误路由」也不成立：分区编号在迁移前后不变，写错节点会被 `broker_cluster:authorize_append/2`（经 `append_guard` 钩子）拒绝。

- S5 需证明：AMQP routing key / NATS subject / Kafka topic 三种路由模型都能表达为 `route/3` 返回 `[{partition_id, [SubRef]}]`。
