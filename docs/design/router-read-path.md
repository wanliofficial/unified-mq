# 路由读路径：env 串行点与全表扫描的收敛设计

- **文档版本**：v0.2（2026-11-16）
- **状态**：**改动 1 与改动 2 已实现（见 §9 交付表）**。改动 3 只有设计，无代码、无「已实测」性能数字。
- **关联**：ADR-010 §10.1（读路径不得进程串行化）/§10.3（hash 后端）；ADR-008 §8.4（append 路径不得付共识成本）；ADR-016（指标键集）；`docs/architecture.md` §2.1（`broker_router` 行）；`docs/benchmarks/2026-11-08-s2-baseline.md` §154
- **证据口径**：所有代码位置为 2026-11-16 工作区实际行号；所有「未实测」明确标注；不含任何吞吐/延迟数字，性能结论待 §7.3 的压测报告落盘。

---

## 0. 结论先行

路由**回调**（`route/3`）确实是纯 ETS 读，ADR-010 §10.1 的字面要求满足。但把「取 env」与「取索引」两件事连起来看，读路径上有三个事实与 ADR-010 的**意图**不符：

| # | 事实 | 位置 | 后果 |
|---|---|---|---|
| **P1** | 每条 PUBLISH 调 `broker_router:env()`，其内部是 `gen_server:call` | `mqtt_conn_fsm.erl:706` → `broker_router_hash:112-128` → `broker_router_hash_store.erl:30-31` | 全节点所有发布**串行**过一个进程。串行点只是从 `route/3` 挪到了「取 env」 |
| **P2** | `route/3` 用 `ets:tab2list/1` 取整个索引后线性匹配 | `broker_router_hash.erl:75` | 每条消息拷贝全部 `{Tenant,Filter} → [{SubRef,Seq}]`；成本与订阅总数成正比，与本次匹配数无关 |
| **P3** | 匹配结果用 `lists:member` 去重 | `broker_router_hash.erl:196-207` | 匹配数 N 时 O(N²)；单 topic 1000 订阅者即 50 万次比较 |

三项叠加的效果：**订阅规模一旦上来，QoS0 发布吞吐由「索引规模」而不是「CPU/磁盘」决定**，且 P1 让并发度退化为单核。

> **当前状态（2026-11-16）**：P1 已由改动 1 修复（`s11x-router-env-cache`），P2/P3 已由改动 2 修复（`s11x-router-trie-index`）。下文 §1 保留的是修复前的取证，§2/§3 写落地方案。

现有测试没发现，是因为 `broker_router_hash_SUITE:route_reads_are_served_without_the_writer/1` 只挂了写者再调 `route/3`（用的是**已经拿到**的 Env），没有覆盖「拿 env 这一步是否经过写者」；`docs/benchmarks/2026-11-08-s2-baseline.md:154` 的 875→1056 msg/s 量级下三项都不显现。

---

## 1. 取证

### 1.1 P1：取 env 是一次同步调用

```erlang
%% broker_router_hash.erl:110-128（当前实现）
current_env() ->
    Partitions = application:get_env(broker_router, partitions, undefined),
    case {whereis(store_name()), partitions_ok(Partitions)} of
        ...
        {Pid, true} ->
            {ok, #{store => Pid,
                   tab => broker_router_hash_store:table(Pid),   %% ← gen_server:call
                   partitions => Partitions,
                   hash => phash2}}
    end.
```

`broker_router:env()` 被 **17 处**调用（10 个 adapter + `broker_event` + `broker_dlq` + `mqtt_will` 等），其中 `mqtt_conn_fsm:706`、`mqtt_sn_conn:1016`、`coap_resource:181`、`nats_conn:461`、`kafka_conn:910` 都在**发布/请求路径**上，即每条消息一次。

`tab` 是一个 `ets:tid()`，进程启动后**永不改变**。它对每条消息都是常量，却被设计成一次跨进程往返。

### 1.2 P2/P3：索引与匹配

```erlang
%% broker_router_hash.erl:60-77
route(Destination, Ctx, #{tab := Tab, partitions := N, hash := Hash} = Env) ->
    TopicLevels = levels(Destination),
    Matches = lists:flatmap(fun({{EntryTenant, Filter}, Subs}) ->
                                ...
                                case match(TopicLevels, levels(Filter)) of ...
                            end,
                            ets:tab2list(Tab)),      %% ← 全表拷贝
    {ok, [{partition(Destination, N, Hash), ordered_unique(lists:keysort(2, Matches))}]}.
```

`sub_ref()` 的定义（`broker_subscription.erl:53`）是 `{sub_kind(), Key :: binary(), Filter :: binary()}` —— 每个 SubRef 自带两个 binary，所以这条路径的拷贝量比「一个 ref」的直觉大得多。

`ordered_unique/3` 用 `lists:member(SubRef, Seen)` 去重，Seen 是普通 list。

### 1.3 与 ADR-010 的关系

ADR-010 §10.1 原文：

> 索引写入（register/unregister）才经单一写者进程串行更新 ETS，读侧用 `ets:lookup` 无锁并发。

**字节级没被违反**（`route/3` 确实没有 `gen_server:call`），但把「读侧」限定为「`route/3` 内部」是过窄的解释：调用方为了能调 `route/3`，必须先付一次 call。本文按「读路径 = 从收到 PUBLISH 到拿到 `[{Partition, Refs}]`」这一更诚实的口径判断 P1 违规。

因此本文涉及的 ADR 修订只有一处，且是**澄清而非改签名**：

- **需要**：ADR-010 §10.1 增补一句「env 的获取不得是进程调用」。
- **不需要**：5 个回调、`route_ctx` 字段、`broker_router` behavior 的任何签名。

改动 2（索引结构）落在 ADR-010 §10.3 范围内 —— 该节只说「hash 后端 = 目的 hash → 分区」，未规定订阅索引的数据结构，故 trie 化不需要修订 ADR。

---

## 2. 改动 1：env 缓存 + 单调 gen

### 2.1 形状

写者（`broker_router_hash_store`）在 `init/1` 时把 env 发布到 `persistent_term`，并把一个 `atomics` 计数器一并发布；此后**只递增计数器，不再写 persistent_term**。

```erlang
%% 发布（init/1，仅一次）
Gen = atomics:new(1, [{signed, false}]),
persistent_term:put({?MODULE, Name, env}, #{store => self(), tab => Index, gen => Gen}).

%% 失效（每次成功的 register/unregister，在 reply 之前）
_ = atomics:add(Gen, 1, 1).
```

读侧零调用：

```erlang
%% broker_router_hash_store:env/1
env(Name) ->
    case persistent_term:get({?MODULE, Name, env}, undefined) of
        #{store := Pid} = Env ->
            case is_process_alive(Pid) of
                true  -> {ok, Env};
                false -> {error, router_store_not_started}
            end;
        undefined ->
            {error, router_store_not_started}
    end.
```

`broker_router_hash:current_env/0` 变成「读 persistent_term + 读 app env」，`partitions` 与 `hash` 的校验逻辑一字不改。

### 2.2 两条硬约束（写给后来改这段代码的人）

1. **`persistent_term:put` 只允许在 `init/1` 出现**。它的写入会触发全 VM 的 literal 区扫描，写到热路径上等于每条发布一次全局抖动。失效一律走 `atomics`。
2. **不允许在读者进程里 `persistent_term:erase`**。erase 同样是全局操作；读者只负责报告错误，清理留给写者的 `terminate/2`，并用 `is_process_alive/1` 兜住「`terminate` 没跑到」的情况（supervisor 发 `exit(Pid, shutdown)` 时，非 trap_exit 的 gen_server 不走 `terminate/2`）。

### 2.3 为什么 key 要带 `Name`

`broker_router_hash:init/1` 允许自定义 store 名（测试用），一个节点上可能同时存在多个 store。key 用 `{?MODULE, Name, env}` 而不是全局单键，测试之间互不覆盖。

### 2.4 gen 的用途与正确性规则

`gen` 只为改动 3（计划缓存）服务，现在导出但不使用。**如果将来实现缓存，读写顺序必须是**：

```
Gen0 = gen()            %% 1. 先读 gen
Plan = cache[Key]       %% 2. 查缓存
Gen0 =:= tag(Plan) ?    %% 3. gen 未变 → 直接用
    use(Plan)
  ;                     %% 4. gen 变了 → 读索引重建，用【旧】Gen0 打标
    rebuild_and_tag(Gen0)
```

用**旧** gen 打标是安全的关键：若写在「读 gen」与「读索引」之间发生，索引已含新条目，但计划被旧 gen 标记，下一条消息必重建。**只会多重建，绝不漏投。** 这条规则替代了 CloudI 的 `dest_refresh_delay` 那种时间窗口 —— 时间窗口在 AP 系统里可以容忍延迟可见，在 ADR-007 的「SUBACK 之后必须可投递」下不可以。

### 2.5 回滚

改动 1 是三个改动里唯一**不改数据结构**的，回滚 = 把 `current_env/0` 换回 `table/1` 调用。`table/1` 保留不删，就是给这条回滚路径留的。

---

## 3. 改动 2：订阅索引 trie 化（已实现，分支 `s11x-router-trie-index`）

### 3.1 节点行格式

```
ETS trie: set, protected, read_concurrency
{{Tenant, NodeId}, Children, SubsAtNode}
  NodeId    = [Level]                 %% 从根开始的路径，如 [<<"a">>,<<"b">>]
  Children  = #{Level => true}        %% Level 含字面量、<<"+">>、<<"#">>
  SubsAtNode= [{SubRef, Seq}]         %% 已按 Seq 升序，插入即保序
```

`+` / `#` 不做特殊化存储 —— 它们就是 Children map 里两个特殊 key。写文档时曾写成 `#{Level => NodeId}`（显式子节点 id），实现时取 `true`：子节点 id 就是 `Path ++ [Level]`，再存一份只会多一处可能不一致的真相。

行由写者独占（表是 `protected`），`subs` 表保留 `SubRef -> {Tenant, NodeId}` 作为反查，用来支撑 unregister 与 `already_registered` 拒绝。

### 3.2 写路径

`register(Tenant, Filter, SubRef)`：`levels(Filter)` 逐级 link —— 每个祖先节点把自己的子 level 加进 `Children`（已存在则**跳过该行的写**，所以注册同前缀的兄弟 filter 只写一次），末节点 `SubsAtNode ++ [{SubRef, Seq}]`（Seq 来自写者单调计数器，与改动前一致）。

`unregister` 除了删 `subs` 行与末节点条目，还会**剪枝**：节点既无子又无订阅就删掉，并把该 level 从父节点 `Children` 移除，一路向上。不剪的话，长时间运行下每个曾注册过的 filter 都会留一条骨架。

### 3.3 读路径

从根沿 `levels(Destination)` 走，每层最多三支：精确子节点、`<<"+">>` 子节点、`<<"#">>` 子节点。`#` 命中即收该节点全部订阅并终止该分支。

`#` 匹配父层（`sport/#` 匹配 `sport`）由「topic 走完后，再看当前节点有没有 `#` 子节点」覆盖，等价于原 `match/2` 的第一条子句 `match(_TopicLevels, [<<"#">>])`。

复杂度 O(层级 × 分支数)，**与订阅总数无关**；每次跳转是一次 `ets:lookup`（行里同时带 `Children` 与 `SubsAtNode`，所以父节点拿子表、子节点拿订阅各一次）。

### 3.4 去重与排序

同一 SubRef 全局只能注册一次（`subs` 表），所以正常 topic 不会跨节点重复；只有非法 topic 里出现字面量 `+`/`#` 时才可能重复。保留 `ordered_unique/3` + `keysort(2, ...)`：合并多个节点的结果后仍需按 `Seq` 还原全局注册顺序，这是既有契约（`route_lists_every_matching_subref_once` 断言 `[7,8,9]`）。

原设计写的「用 `maps:take/2` 去重、删除 `ordered_unique/3`」不成立：`maps` 去重会丢掉 `Seq` 顺序，而顺序是测试与 `broker_subscription` 都依赖的。折中是保留顺序、把「已见过」的集合从 list 换成 map —— 于是 P3 那个 O(N²) 也一并消失，整体是 `keysort` 的 O(N log N)。

### 3.5 租户与 legacy

`tenant_visible/2` 的语义保留，但落成了「从哪几个根开始走」：读路径先走 `{Tenant, ...}` 根，再走 `{<<"*">>, ...}` 根，结果并集；`Tenant =:= <<"*">>` 时只走一次（否则每条 legacy 订阅会被数两遍）。

### 3.6 成本

- 写侧：register 从 O(1) 变 O(层级) 的 ETS 写，但写是低频路径，且同前缀兄弟只写一次；unregister 多 O(层级) 剪枝。
- 内存：每个中间节点一行，共享前缀只存一份；剪枝保证不随历史注册数单调增长。
  **已实测**（[`2026-11-16-s11x-router-index-memory.md`](../benchmarks/2026-11-16-s11x-router-index-memory.md)）：写放大的上界「每层一行」精确成立且不随规模漂移（4 层无共享前缀 = 4.000 行/filter）；但典型形状远低于上界（`bench/<n>/<m>/dev<i>` 为 1.04 行/filter，1000 topic × 100 订阅的低至 0.021 行/filter）。剪枝后行数回到 0，残留仅为 ETS 固定的 ~2311 word 空闲空间，不随 N 或 churn 轮次增长。
- 迁移：`broker_subscription` 的 backfill（boot step 3）通过同一 `register/4` 入口重建，无需额外迁移代码。

### 3.7 等价性验证

`broker_router_hash_SUITE:trie_matches_the_reference_matcher/1` 把改动前的 `match/2` 原样留在测试里当 oracle：随机生成 400 个合法 filter（含 `+`、`#`、重复 filter、三个租户根）与 400 个 topic，逐个断言 trie 的结果列表与 oracle 完全一致（含顺序）。种子固定，失败可复现。

另外两例：`unregister_prunes_the_trie/1` 逐行断言剪枝后的行数（4 → 5 → 4 → 0，并含「祖先节点保留」一段）；`tenant_roots_are_independent_and_legacy_is_visible/1` 断言同一 filter 在两个租户下互不可见、legacy `*` 对所有人可见。

### 3.8 自查发现的一个真实缺陷

首版实现里，`unregister` 只要末节点 `SubsAtNode` 空了就删整行。同时注册 `a/b` 与 `a/b/c` 时，删掉 `a/b` 会让 `[a]` 到 `[a,b,c]` 之间唯一的链接消失 —— `a/b/c` 的订阅还在 `subs` 表里，却再也走不到。修复后只在「无子且无订阅」时删行，`unregister_prunes_the_trie/1` 补了这一段（删 `a/b` 后行数不变，`a/b/c` 仍能路由到）。


---

## 4. 改动 3：计划缓存（设计，未实现，可选）

只在「高扇出 + 重复 topic」场景有收益（例如 1 万设备都发 `devices/+/status`）。

- 位置：每连接进程内（`mqtt_conn_fsm` 的 `#st{}`）或每调度器一张 ETS；**不做全局单表**，避免又造一个热点。
- 键：`{Tenant, Destination, Gen}`。
- 容量：LRU 上限（配置项），避免高基数 topic 撑爆内存。
- 失效：只靠 gen（§2.4），无 TTL。

**不做 3 也不影响正确性** —— 改动 2 已经把 P2/P3 消掉，3 只是常数因子。

---

## 5. 明确不做的

| 不做 | 理由 |
|---|---|
| 引入 TTL 式路由缓存 | 与 ADR-007「SUBACK 之后必须可投递」冲突。CloudI 能容忍延迟可见是因为它是 AP + 尽力而为 |
| 照搬 CloudI 的 `immediate_* / lazy_*` 配置矩阵 | 那是为「跨节点查 cpg 很贵」设计的；本地索引 + gen 已是精确失效，没有「昂贵的刷新时机」需要区分 |
| 把 `route/3` 改成异步 / 加进程 | 直接违反 ADR-010 §10.1 |
| 把索引持久化 | ADR-010 §10.6 已定：不持久化，由 `broker_subscription` backfill |

---

## 6. 与 CloudI `dest_refresh` 的真实对位

| CloudI | UnifiedMQ 现状 | 结论 |
|---|---|---|
| `dest_refresh` 缓存**目的 pid/节点** | `broker_cluster_cache` 已缓存**分区 owner/epoch**，读不走 ra（ADR-008 §8.4） | **已对位，无需改动** |
| `dest_refresh_delay`（死亡后残留窗口） | 不需要：epoch fencing + `is_process_alive` 已给出确定的失效边界 | 不引入 |
| `lazy_*`（缓存订阅 + 事件失效） | `broker_cluster` 是事件驱动；订阅索引目前是「每次都全扫」 | 改动 1+2 即为此项的对应实现 |
| `services_search`（预演投递面） | 无管理面入口 | 见 §8 OQ-3，属管理 API 缺口，不在本设计范围 |

结论：`dest_refresh` 的机制**一半已经落地**（归属缓存），另一半（订阅索引的事件失效）由改动 1/2 补齐。时间窗口那一半**不该抄**。

---

## 7. 验收

### 7.1 改动 1 的 CT 用例（已实现，见 §9）

| 用例 | 断言 | 对应问题 |
|---|---|---|
| `env_is_served_without_the_writer` | `sys:suspend` 写者后，`broker_router:env()` 仍在 1000ms 有界等待内返回；同时一个 `register` helper 被阻塞在同一写者上 | P1 |
| `gen_advances_on_successful_writes_only` | 成功 register/unregister 使 `gen/0` 严格递增；重复 register（`already_registered`）不递增 | §2.4 规则前提 |
| `env_rejects_a_dead_store` | store 停掉后 `broker_router:env()` 返回 `{error, router_store_not_started}`，而不是返回一个指向死表的 env | §2.2 第 2 条 |

**已跑证据**（2026-11-16，OTP 28.3.1）：`broker_router_hash_SUITE` + `broker_router_sup_SUITE` 共 **20/20 通过**（含新增 3 例）；下游 `broker_subscription_SUITE` + `broker_queue_SUITE` + `mqtt_conn_SUITE` 共 **139/139 通过**。改动 2 分支上：router 两套件 + `broker_queue_SUITE` + `mqtt_conn_SUITE` 共 **109/109**，`broker_subscription_SUITE` **53/53**。未做任何压测，§7.3 的三档数字仍缺。

### 7.2 改动 2 的 CT 用例（已实现）

| 用例 | 断言 |
|---|---|
| `trie_matches_the_reference_matcher` | 400 个随机合法 filter × 400 个随机 topic，trie 结果与改动前 `match/2`（作为 oracle 留在测试里）逐条相等，含顺序；覆盖 `+`/`#`/重复 filter/三个租户根 |
| `unregister_prunes_the_trie` | 注册 `a/b/c` 得 4 行、加 `a/b/d` 得 5 行、删 d 回 4 行、删尽回 0 行 |
| `tenant_roots_are_independent_and_legacy_is_visible` | 同一 filter 在 t1/t2 下互不可见，legacy `*` 对所有租户可见 |

原计划的另两条没有单独成例：**「`reductions` 增量为 0」**已由改动 1 的 `env_is_served_without_the_writer` + `route_reads_are_served_without_the_writer` 覆盖（读路径不经过写者）；**「订阅后立即发布的顺序」**由上面的 oracle 用例的同序断言覆盖（顺序就是 `Seq` 序）。

### 7.3 压测口径（已落盘）

四份报告，均在 `docs/benchmarks/`。**注意：前两份出过一版结论错误的版本，已更正并替换**——初版让 `env` 与 `trie` 调用同一份代码，因此得出「改动 1 独立就够」的错误结论。更正后三个实现各差一个变量（平表+调用 / 平表无调用 / trie 无调用）。

1. **读路径**（[`2026-11-16-s11x-router-read-path.md`](../benchmarks/2026-11-16-s11x-router-read-path.md)）：单进程 `route/3`，1 / 1k / 10k 订阅 × 三种实现。p50：

   | 订阅数 | old | env（仅改动 1） | trie（改动 1+2） |
   |---|---|---|---|
   | 1 | 21 µs | **13 µs** | 19 µs |
   | 1,000 | 2,097 µs | 1,984 µs | **19 µs** |
   | 10,000 | 23,014 µs | 22,510 µs | **19 µs** |

   **改动 1 只在索引极小时有意义**（省一次约 8 µs 的调用）；1k 订阅以上几乎无效（仅快 5%），因为全表拷贝盖过了它。**大表收益全部来自改动 2**（1k: 118x，10k: 1,236x）。trie 在 1 订阅时略慢于平表（19 vs 13 µs），是「按层走 map」的固定成本。

2. **高扇出**（[`2026-11-16-s11x-router-fanout.md`](../benchmarks/2026-11-16-s11x-router-fanout.md)）：N 个订阅者挂同一 filter。1 万扇出下 old/env 均约 6 ops/s（p50 158–165 ms，被 O(N²) 去重拖死），trie **87 ops/s、p50 11.7 ms**，快 15x。差距主因是 map 去重取代 `lists:member`。**计划缓存（改动 3）在此形状下确有可缓存的工作量**，但因失效粒度为全局 `gen`，是否值得取决于「订阅变更频率 / 同 topic 重复发布频率」，该比值尚无数据。

3. **索引内存 / OQ-2**（[`2026-11-16-s11x-router-index-memory.md`](../benchmarks/2026-11-16-s11x-router-index-memory.md)）：10 万 filter 下写放大上界精确成立（无共享前缀 = 4.000 行/filter、约 129 MB），典型形状远低于上界（设备集群 10 万订阅仅 4.7 MB）。剪枝后行数回到 0。**结论：不需要配置项，但需按行数而非订阅数做监控。**

4. **并发扩展性**（[`2026-11-16-s11x-router-concurrency.md`](../benchmarks/2026-11-16-s11x-router-concurrency.md)）：小表下 `old` 的聚合吞吐封顶在 **18.6 万 ops/s / 4.16x（P=16）**，`env`/`trie` 达 7.48x/7.91x（62 万/58 万）。**改动 1 的真正价值是移除这个并发天花板**，而非单进程吞吐。大表下 `old` 也完美线性扩展（8.00x），因为拷贝发生在调用方进程——串行化只限制上限，不拖慢单条。

四份报告都写明没测什么；机器测量期间有外部负载，**绝对数字偏低，引用请复跑**。

---

## 8. 未决问题

| 编号 | 问题 | 需要谁决定 |
|---|---|---|
| OQ-1 | ADR-010 §10.1 的「读侧」定义是否按本文口径收窄/澄清？ | ADR 修订 |
| OQ-2 | trie 化的写入放大（每层一行）在 10 万 filter 下的内存上限是否需要配置项 | **已答（2026-11-16）**：不需要配置项（最坏 129 MB / 10 万 filter），但需导出**行数**而非订阅数作监控指标——二者可差 19 倍。见 [`docs/benchmarks/2026-11-16-s11x-router-index-memory.md`](../benchmarks/2026-11-16-s11x-router-index-memory.md) |
| OQ-3 | `services_search` 等价物是否随 S12-A 的管理 API 一起做 | S12-0 |
| OQ-4 | 跨节点订阅索引（若 S12-B 引入跨节点投递）与本地 trie 的关系 | S12-0 |

---

## 9. 交付表

| 改动 | 状态 | 落点 |
|---|---|---|
| 1. env 缓存 + gen | ✅ 已实现（分支 `s11x-router-env-cache`） | `broker_router_hash_store.erl`（`env/1`、`gen/1`、`bump/1`）、`broker_router_hash.erl`（`current_env/0`、`gen/0`、`init/1`）。`gen/0` 只放在后端模块：行为门面 `broker_router.erl` 未改，ADR-010 冻结的 5 个回调与 `env/0` 签名不动 |
| 2. trie 索引 | ✅ 已实现（分支 `s11x-router-trie-index`，叠在改动 1 之上） | `broker_router_hash_store.erl`（节点行、`link/4`、`prune/3`）、`broker_router_hash.erl`（`route/3` 改为按层走、删除 `match/2` 与 `tenant_visible/3`）；测试见 §7.2 |
| 3. 计划缓存 | ⬜ 未实现（可选） | 本文 §4 |
| ADR-010 §10.1 增补 | ⬜ 未改 | OQ-1 |
