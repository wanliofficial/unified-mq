# 路由读路径：env 串行点与全表扫描的收敛设计

- **文档版本**：v0.1（2026-11-16）
- **状态**：**设计稿 + 改动 1 已实现（见 §9 交付表）**。改动 2 / 改动 3 只有设计，无代码、无「已实测」性能数字。
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

## 3. 改动 2：订阅索引 trie 化（设计，未实现）

### 3.1 节点行格式

```
ETS broker_router_trie: set, protected, read_concurrency
{{Tenant, NodeId}, Children, SubsAtNode}
  NodeId    = [Level]                  %% 从根开始的路径，如 [<<"a">>,<<"b">>]
  Children  = #{Level => NodeId}       %% Level 含字面量、<<"+">>、<<"#">>
  SubsAtNode= [{SubRef, Seq}]          %% 已按 Seq 升序，插入即保序
```

`+` / `#` 不做特殊化存储 —— 它们就是 Children map 里两个特殊 key，这让「以 `+` 开头的 filter」和「字面量恰好叫 `+`」（不存在，MQTT 禁止）共用一条代码路径。

### 3.2 写路径

`register(Tenant, Filter, SubRef)`：`levels(Filter)` 逐级建节点，末节点 `SubsAtNode ++ [{SubRef, Seq}]`（Seq 来自写者单调计数器，与现状一致）。

### 3.3 读路径

从根沿 `levels(Destination)` 走，每层尝试三支：精确子节点、`<<"+">>` 子节点、`<<"#">>` 子节点。`#` 命中即收该节点全部订阅并终止该分支。

`#` 匹配父层（`sport/#` 匹配 `sport`）由「每个节点都检查自己的 `#` 子节点」自然覆盖，等价于现在 `match/2` 的第一条子句 `match(_TopicLevels, [<<"#">>])`。

复杂度 O(层级 × 分支数)，**与订阅总数无关**。

### 3.4 去重

同一 SubRef 在同一节点只出现一次；跨节点重复（如同时订阅 `a/+` 与 `a/#`）用 `maps:take/2` 去重，O(N)。**`ordered_unique/3` 删除**。

### 3.5 租户与 legacy

`tenant_visible/2` 的 `<<"*">>` 语义保留：读路径先查 `{Tenant, ...}` 子树，再查 `{<<"*">>, ...}` 子树，结果并集。

### 3.6 成本

- 写侧：register 从 O(1) 变 O(层级)（建节点），但写是低频路径。
- 内存：每个中间节点一行。filter 层级越浅、共享前缀越多，越省。
- 迁移：`broker_subscription` 的 backfill（boot step 3）通过同一 `register/4` 入口重建，无需额外迁移代码。

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

**已跑证据**（2026-11-16，OTP 28.3.1）：`broker_router_hash_SUITE` + `broker_router_sup_SUITE` 共 **20/20 通过**（含新增 3 例）；下游 `broker_subscription_SUITE` + `broker_queue_SUITE` + `mqtt_conn_SUITE` 共 **139/139 通过**。未做任何压测，§7.3 的三档数字仍缺。

### 7.2 改动 2 的 CT 用例（待实现时补）

- 1 万条 filter、1 万次发布的正确性等价测试（与现有 `match/2` 实现对拍，随机 topic/filter 各 1 万组）；
- `broker_router_hash_store` 的 `reductions` 在 1 万次 `route/3` 后增量为 0（证明零调用）；
- 订阅后立即发布的顺序保证（gen 规则回归）。

### 7.3 压测口径（待落盘）

在 `docs/benchmarks/` 新增一份，必须写清三组数：**1 / 1k / 10k 订阅** 下的 QoS0 发布吞吐与 p99，旧实现 vs 改动 1 vs 改动 1+2。缺任何一档都不算证据 —— 旧实现的成本曲线是本次结论的全部意义。

---

## 8. 未决问题

| 编号 | 问题 | 需要谁决定 |
|---|---|---|
| OQ-1 | ADR-010 §10.1 的「读侧」定义是否按本文口径收窄/澄清？ | ADR 修订 |
| OQ-2 | trie 化的写入放大（每层一行）在 10 万 filter 下的内存上限是否需要配置项 | 改动 2 实现时 |
| OQ-3 | `services_search` 等价物是否随 S12-A 的管理 API 一起做 | S12-0 |
| OQ-4 | 跨节点订阅索引（若 S12-B 引入跨节点投递）与本地 trie 的关系 | S12-0 |

---

## 9. 交付表

| 改动 | 状态 | 落点 |
|---|---|---|
| 1. env 缓存 + gen | ✅ 已实现（分支 `s11x-router-env-cache`） | `broker_router_hash_store.erl`（`env/1`、`gen/1`、`bump/1`）、`broker_router_hash.erl`（`current_env/0`、`gen/0`、`init/1`）。`gen/0` 只放在后端模块：行为门面 `broker_router.erl` 未改，ADR-010 冻结的 5 个回调与 `env/0` 签名不动 |
| 2. trie 索引 | ⬜ 未实现 | 本文 §3 |
| 3. 计划缓存 | ⬜ 未实现（可选） | 本文 §4 |
| ADR-010 §10.1 增补 | ⬜ 未改 | OQ-1 |
