# MQ-15 共享订阅集群再均衡 —— 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 `$share/<Group>/<Filter>` 共享订阅跨节点可用——分配决策进 ra 串行化，根除「两节点独立 claim 把同一 offset 发给两个消费者」的静默重复投递，并加阈值触发的主动再均衡。

**Architecture:** 沿用现有三条路径的职责分离（设计 §3.1）：分配决策进 ra 元数据组（新增 `shared_assignments` + `shared_consumers` 键与 4 个命令），消息体读取沿用 `broker_queue:read_from` 的 `route/3` forward，连接/投递留在 conn_fsm。worker 的 `?CURSORS` ETS 行降级为 ra 分配表的本地只读镜像，写一律经 ra 回来再更新。feature flag `broker_subscription:cluster_shared_rebalance` 默认 `false`：关时行为逐字节等同现状（零回归），开时才走 ra 分配路径。

**Tech Stack:** Erlang/OTP 29.1.2、`ra`（元数据 Raft 组）、`peer`（OTP 28 stdlib，3-peer dist SUITE）、Common Test、rebar3。

---

## §9 待定项的落地决策（brainstorm→plan 锁定）

设计 §9 把四项留给 Plan 阶段。本计划锁定如下，后续任务按此实现：

1. **shared 分配的 epoch 方案**：**不用 epoch 围栏**。分区命令用 epoch 是为了 fence 迁移中的源；shared 分配命令是**单调集合运算**（claim 加、ack/release 删），任意顺序 apply 都收敛，且互斥不变量（invariant 1）由 ra 单线程 apply 保证，与 epoch 无关。设计 §3.3 的命令本身也**不含 epoch**。故 shared 命令不带 epoch，`broker_cursor` 纯函数在 ra apply 内被调用。
2. **快照压缩**：**不压缩**（YAGNI）。`shared_assignments` 每分区 bounded by `shared_state_max`（默认 4096）条 `#{Offset => ClientId}`，shared 组数量小；快照整体仍小。大 group 膨胀记入 §9 后续，本计划不引入压缩。
3. **rebalance tick 归属**：**复用 worker 现有 poll tick**，不新增进程。当某消费者 claim 得到 `quota_hit` 且该 shared group 的 `max-min` 超 `hysteresis` 且距上次 reclaim 超 `cooldown_ms`，由处理该 claim 的 worker 发一次 `shared_reclaim`（设计 §4.1(b)）。无独立 rebalance 进程。
4. **双路径代码组织**：worker 的 `handle_call({claim|ack|release,...})` 内用 `case cluster_flag() of false -> <现有本地 ETS 分支，逐字保留>; true -> <ra 分支> end`。`broker_cursor` 的纯函数两种路径共用，不复制。

## 崩溃回池（crash-release）的节点→消费者映射

设计 §4.1(c) 要求节点崩溃后其 assigned offset 回池。`assigned` 表只记 `{Offset => ClientId}`，不知 ClientId 在哪个节点。故新增**第二张 ra 侧表** `shared_consumers => #{{SubRef, ClientId} => Node}`：
- `join` 时写 `{{SubRef, ClientId} => node()}`；`leave` 时删。
- 崩溃回池：`nodedown_mon` 去抖确认后，对崩溃节点 `N` 调 `broker_cluster:shared_consumers_on(N)` 得到其持有的 ClientId 列表，逐一 `shared_release(SubRef, ClientId)`。
- 复用现有去抖（设计 §4.1(c)「不新增」），仅在同一 handler 里追加这一步，故崩溃节点若持有会话分区还会触发既有分区恢复，两件事正交。

## File Structure（改动文件与职责）

- `apps/broker_cluster/src/broker_cluster_meta_machine.erl`（modify）：state 增 `shared_assignments`、`shared_consumers` 两键；`apply/3` 增 4 命令分支 + 1 查询；`snapshot_installed/5` 恢复两表。
- `apps/broker_cluster/src/broker_cluster_cache.erl`（modify）：增 shared 两表的本地镜像 `put_shared_assignments/1`、`shared_assignment/2`、`shared_consumers_on/1`。
- `apps/broker_cluster/src/broker_cluster.erl`（modify）：公开 API `shared_claim/7`、`shared_ack/4`、`shared_release/2`、`shared_reclaim/5`、`shared_consumers_on/1`。
- `apps/broker_subscription/src/broker_subscription_worker.erl`（modify）：flag 双路径；`join`/`leave` 写/删 `shared_consumers`；`cross_node_subscribe_ok(shared,...)` flag 放宽。
- `apps/broker_subscription/src/broker_subscription.erl`（modify）：顶层转发 `shared_*` 到 worker（若需）。
- `apps/broker_cluster/src/broker_cluster_nodedown_mon.erl` 或其 handler（modify）：崩溃节点追加 shared_release。
- `apps/broker_metrics/src/broker_metrics.erl`（modify）：闭集键 `shared_reclaim`、`shared_rebalance_no_quorum`。
- `apps/broker_cluster/test/broker_cluster_meta_SUITE.erl`（modify）：shared 命令纯逻辑单测。
- `apps/broker_subscription/test/broker_subscription_worker_SUITE.erl`（modify）：flag on/off 双路径等价。
- `apps/broker_cluster/test/broker_subscription_shared_dist_SUITE.erl`（create）：3-peer 跨节点 shared 不重复、QoS0 恰一个、崩溃回池。

---

## Task 1: ra 元数据组新增 shared 命令与状态键

**Files:**
- Modify: `apps/broker_cluster/src/broker_cluster_meta_machine.erl`
- Test: `apps/broker_cluster/test/broker_cluster_meta_SUITE.erl`

**背景（已核实的现状）：** `apply/3`（`:34-74`）按 `Command` 分派；`state()` 类型见 `:23-25`；`snapshot_installed/5`（`:79-88`）重建 cache。`broker_cursor:claim/7`（本分支已存在）、`ack_shared/3`、`release/2` 是纯函数，可在 apply 内调用。

- [ ] **Step 1: 写失败测试** —— 在 `broker_cluster_meta_SUITE` 加一个直接驱动 meta machine 模块的 case（该套件已能构造 `broker_cluster_meta_machine:apply/3`）。追加到 `all()`：

```erlang
     shared_claim_assigns_without_duplicate_across_calls,
     shared_ack_advances_and_releases,
     shared_release_returns_all_of_a_consumer,
     shared_consumers_table_tracks_join_leave].
```

测试体（追加到 suite，紧接最后一个 case 之后）：

```erlang
shared_claim_assigns_without_duplicate_across_calls(_Config) ->
    S0 = broker_cluster_meta_machine:init(#{}),
    Cmd = {shared_claim, {shared, <<"g">>, <<"s/#">>}, 0, <<"c1">>, 100, 100, #{}, 0},
    {S1, {ok, Offsets, _}, []} = broker_cluster_meta_machine:apply(#{index => 1}, Cmd, S0),
    ?assertEqual([1, 2], lists:sort(Offsets)),
    %% 第二次 claim 从已分配之外继续，绝不重复。
    Cmd2 = {shared_claim, {shared, <<"g">>, <<"s/#">>}, 0, <<"c2">>, 100, 100, #{}, 0},
    {S2, {ok, Offsets2, _}, []} = broker_cluster_meta_machine:apply(#{index => 2}, Cmd2, S1),
    ?assertEqual([3, 4], lists:sort(Offsets2)),
    [] = [O || O <- Offsets2, lists:member(O, Offsets)],
    ok.

shared_ack_advances_and_releases(_Config) ->
    S0 = broker_cluster_meta_machine:init(#{}),
    Sub = {shared, <<"g">>, <<"s/#">>},
    {S1, {ok, [1, 2], _}, []} =
        broker_cluster_meta_machine:apply(
          #{index => 1}, {shared_claim, Sub, 0, <<"c1">>, 100, 100, #{}, 0}, S0),
    {S2, {ok, _Low}, []} =
        broker_cluster_meta_machine:apply(
          #{index => 2}, {shared_ack, Sub, 0, 1, <<"c1">>}, S1),
    %% ack 后该 offset 不再可分配：新 claim 跳过它。
    {S3, {ok, [3], _}, []} =
        broker_cluster_meta_machine:apply(
          #{index => 3}, {shared_claim, Sub, 0, <<"c2">>, 100, 100, #{}, 0}, S2),
    1 = _Low,
    ?assertEqual({ok, #{2 := <<"c2">>}}, shared_lookup(S3, Sub, 0)),
    ok.

shared_release_returns_all_of_a_consumer(_Config) ->
    S0 = broker_cluster_meta_machine:init(#{}),
    Sub = {shared, <<"g">>, <<"s/#">>},
    {S1, {ok, _}, []} =
        broker_cluster_meta_machine:apply(
          #{index => 1}, {shared_claim, Sub, 0, <<"c1">>, 100, 100, #{}, 0}, S0),
    {S2, {ok, _}, []} =
        broker_cluster_meta_machine:apply(
          #{index => 2}, {shared_claim, Sub, 0, <<"c1">>, 100, 100, #{}, 0}, S1),
    {S3, {ok, _}, []} =
        broker_cluster_meta_machine:apply(
          #{index => 3}, {shared_release, Sub, 0, <<"c1">>}, S2),
    ?assertEqual({ok, #{}}, shared_lookup(S3, Sub, 0)),
    ok.

shared_consumers_table_tracks_join_leave(_Config) ->
    S0 = broker_cluster_meta_machine:init(#{}),
    Sub = {shared, <<"g">>, <<"s/#">>},
    {S1, {ok, _}, []} =
        broker_cluster_meta_machine:apply(
          #{index => 1}, {shared_join, Sub, <<"c1">>, node()}, S0),
    {ok, node()} = broker_cluster_meta_machine:query_consumer(S1, Sub, <<"c1">>),
    {S2, {ok, _}, []} =
        broker_cluster_meta_machine:apply(
          #{index => 2}, {shared_leave, Sub, <<"c1">>}, S1),
    not_found = broker_cluster_meta_machine:query_consumer(S2, Sub, <<"c1">>),
    ok.
```

辅助（追加到 suite 末尾，与其它 helper 同处）：

```erlang
shared_lookup(State, Sub, P) ->
    Maps = maps:get(shared_assignments, State, #{}),
    maps:get({Sub, P}, maps:get(assignments, Maps, #{}), undefined).
```

（注意：本步先以**扁平** `shared_assignments => #{{Sub,P} => #{Offset=>ClientId}}` 实现，`shared_lookup` 直接读它；Task 2 若改为嵌套再同步此 helper。实现时与下列 Step 3 保持一致。）

- [ ] **Step 2: 运行测试确认失败**

Run: `cd /tmp/opencode/mq-15-rebalance && export PATH="$HOME/Development/otp-29.1.2/lib/erlang/bin:$PATH" && rm -rf _build/test && rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_meta_SUITE 2>&1 | tail -15`
Expected: 4 个新 case FAIL（`{unknown_command, {shared_claim, ...}}`）。

- [ ] **Step 3: 实现 meta machine 扩展。** 在 `broker_cluster_meta_machine.erl`：

`state()` 类型与 `init/1` 增两键：

```erlang
-type state() :: #{partitions := #{partition() => meta()},
                   session_partition_count => pos_integer() | undefined,
                   session_partition_epoch => non_neg_integer(),
                   %% MQ-15: `{ {SubRef, PartitionId} => #{Offset => ClientId} }'.
                   shared_assignments := #{{sub_key(), partition_id()} => map()},
                   %% MQ-15: `{ {SubRef, ClientId} => Node }` —— 崩溃回池用。
                   shared_consumers := #{{sub_key(), binary()} => node()}}.
-type sub_key() :: {shared, binary(), binary()}.

init(_Config) ->
    #{partitions => #{}, session_partition_count => undefined,
      session_partition_epoch => 0,
      shared_assignments => #{}, shared_consumers => #{}}.
```

`apply/3` 在 `snapshot ->` 分支之前插入 4+2 分支：

```erlang
        {shared_claim, Sub, P, ClientId, Max, High, Blocked, _PerConsumerMax} ->
            shared_claim(Sub, P, ClientId, Max, High, Blocked, Index, State);
        {shared_ack, Sub, P, Offset, ClientId} ->
            shared_ack(Sub, P, Offset, ClientId, Index, State);
        {shared_release, Sub, P, ClientId} ->
            shared_release(Sub, P, ClientId, Index, State);
        {shared_reclaim, Sub, P, FromId, ToId, Offsets} ->
            shared_reclaim(Sub, P, FromId, ToId, Offsets, Index, State);
        {shared_join, Sub, ClientId, Node} ->
            shared_join(Sub, ClientId, Node, Index, State);
        {shared_leave, Sub, ClientId} ->
            shared_leave(Sub, ClientId, Index, State);
        {shared_consumers_on, Node} ->
            {State, shared_consumers_on(Node, State), []};
```

新增内部函数（`%%%_* Internal` 区）：

```erlang
shared_claim(Sub, P, ClientId, Max, High, Blocked, Index, State) ->
    #{shared_assignments := As} = State,
    Key = {Sub, P},
    Row = maps:get(Key, As, broker_cursor:new_shared(High)),
    %% claim/7：PerConsumerMax 传 0（MQ-11 配额是 worker 侧概念，ra 内只按组窗口分）。
    case broker_cursor:claim(Row, Max, High, ClientId, window_max(), Blocked, 0) of
        {ok, [], _} ->
            {State, {ok, [], As}, []};
        {ok, quota_hit, _} ->
            {State, {ok, quota_hit, As}, []};
        {ok, Offsets, Row1} ->
            As1 = As#{Key => Row1},
            broker_cluster_cache:put_shared_assignments(As1),
            {State#{shared_assignments := As1}, {ok, Offsets, As1}, []};
        {error, overflow} ->
            {State, {error, overflow}, []}
    end.

shared_ack(Sub, P, Offset, ClientId, Index, State) ->
    #{shared_assignments := As} = State,
    Key = {Sub, P},
    case maps:find(Key, As) of
        error ->
            {State, {error, not_found}, []};
        {ok, Row} ->
            Row1 = broker_cursor:ack_shared(Row, Offset, ClientId),
            As1 = As#{Key => Row1},
            broker_cluster_cache:put_shared_assignments(As1),
            {State#{shared_assignments := As1}, {ok, broker_cursor:next_read(Row1)}, []}
    end.

shared_release(Sub, P, ClientId, Index, State) ->
    #{shared_assignments := As, shared_consumers := Cs} = State,
    Key = {Sub, P},
    case maps:find(Key, As) of
        error ->
            {State, {ok, 0}, []};
        {ok, Row} ->
            Row1 = broker_cursor:release(Row, ClientId),
            As1 = As#{Key => Row1},
            Cs1 = maps:remove({Sub, ClientId}, Cs),
            broker_cluster_cache:put_shared_assignments(As1),
            broker_cluster_cache:put_shared_consumers(Cs1),
            {State#{shared_assignments := As1, shared_consumers := Cs1},
             {ok, count_released(Row, ClientId)}, []}
    end.

shared_reclaim(Sub, P, FromId, ToId, Offsets, Index, State) ->
    #{shared_assignments := As} = State,
    Key = {Sub, P},
    case maps:find(Key, As) of
        error ->
            {State, {error, not_found}, []};
        {ok, Row} ->
            Row1 = reclaim_offsets(Row, FromId, ToId, Offsets),
            As1 = As#{Key => Row1},
            broker_cluster_cache:put_shared_assignments(As1),
            {State#{shared_assignments := As1}, {ok, Offsets}, []}
    end.

shared_join(Sub, ClientId, Node, Index, State) ->
    #{shared_consumers := Cs} = State,
    Cs1 = Cs#{{Sub, ClientId} => Node},
    broker_cluster_cache:put_shared_consumers(Cs1),
    {State#{shared_consumers := Cs1}, {ok, Node}, []}.

shared_leave(Sub, ClientId, Index, State) ->
    #{shared_consumers := Cs} = State,
    Cs1 = maps:remove({Sub, ClientId}, Cs),
    broker_cluster_cache:put_shared_consumers(Cs1),
    {State#{shared_consumers := Cs1}, {ok, removed}, []}.

shared_consumers_on(Node, #{shared_consumers := Cs}) ->
    [ClientId || {{_Sub, ClientId}, N} <- maps:to_list(Cs), N =:= Node].

%% 工具
window_max() -> application:get_env(broker_subscription, shared_state_max, 4096).
count_released(Row, ClientId) ->
    Assigned = maps:get(assigned, Row, #{}),
    length([1 || {_O, C} <- maps:to_list(Assigned), C =:= ClientId]).
reclaim_offsets(Row, FromId, ToId, Offsets) ->
    Assigned = maps:get(assigned, Row, #{}),
    Acked = maps:get(acked, Row, #{}),
    Retries = maps:get(retries, Row, #{}),
    Assigned1 = lists:foldl(
                  fun(O, Acc) ->
                          case maps:get(O, Acc, undefined) of
                              FromId -> Acc#{O => ToId};
                              _ -> Acc
                          end
                  end, Assigned, Offsets),
    Row#{assigned := Assigned1, acked := Acked, retries := Retries}.
```

`ack_shared` 返回 row 时若 `release/2`/`ack_shared/3` 的 row 形状与本处假设不符，以 `broker_cursor` 实际返回为准（该模块 `ack_shared/3`、`release/2` 已存在且为纯函数）。

- [ ] **Step 4: 运行测试确认通过**

Run: `rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_meta_SUITE 2>&1 | tail -8`
Expected: 全过（含 4 新例）。

- [ ] **Step 5: 提交**

```bash
git add apps/broker_cluster/src/broker_cluster_meta_machine.erl apps/broker_cluster/test/broker_cluster_meta_SUITE.erl
git commit -m "feat(mq15): ra 元数据组新增 shared 分配命令与状态键（flag 无关，惰性）"
```

---

## Task 2: cache 镜像 shared 两表 + snapshot 恢复

**Files:**
- Modify: `apps/broker_cluster/src/broker_cluster_cache.erl`
- Modify: `apps/broker_cluster/src/broker_cluster_meta_machine.erl`（`snapshot_installed/5`）
- Test: `apps/broker_cluster/test/broker_cluster_meta_SUITE.erl`

- [ ] **Step 1: 写失败测试**（追加到 `broker_cluster_meta_SUITE`）：

```erlang
     shared_assignments_survive_snapshot_install].
```

```erlang
shared_assignments_survive_snapshot_install(_Config) ->
    S0 = broker_cluster_meta_machine:init(#{}),
    Sub = {shared, <<"g">>, <<"s/#">>},
    {S1, _, []} =
        broker_cluster_meta_machine:apply(
          #{index => 1}, {shared_join, Sub, <<"c1">>, node()}, S0),
    {S2, _, []} =
        broker_cluster_meta_machine:apply(
          #{index => 2}, {shared_claim, Sub, 0, <<"c1">>, 100, 100, #{}, 0}, S1),
    %% 模拟 snapshot_install：用 apply 后的 state 作为 snapshot 重装到一个空 state。
    S3 = broker_cluster_meta_machine:init(#{}),
    ok = broker_cluster_meta_machine:snapshot_installed(
           #{}, S2, S3, undefined, undefined),
    ?assertEqual({ok, #{1 := <<"c1">>}}, shared_lookup(S3, Sub, 0)),
    ok.
```

- [ ] **Step 2: 运行确认失败**

Run: `rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_meta_SUITE 2>&1 | tail -12`
Expected: FAIL（`snapshot_installed/5` 当前 `ok=` 返回但 `shared_lookup` 读到 `undefined`，因为 `snapshot_installed` 未恢复 `shared_assignments`；`shared_lookup` 用扁平键，与 Task 1 Step 1 一致）。

- [ ] **Step 3: 实现。** `broker_cluster_cache.erl` 增：

```erlang
-export([put_shared_assignments/1, shared_assignment/2,
         put_shared_consumers/1, shared_consumers_on/1]).

put_shared_assignments(As) when is_map(As) ->
    case ets:info(?TAB, name) of
        undefined -> ok;
        _ ->
            _ = ets:insert(?TAB, {?SHARED_ASG, As}),
            _ = ets:insert(?TAB, {?SHARED_CON, #{}}), %% 占位，见下
            ok
    end.
```

**注意键冲突**：`?TAB` 同时存分区行 `{P, Meta}` 与 `?SESSION_CFG`。shared 两表用独立保留键，与 `?SESSION_CFG` 同样的「不可能与整数分区键冲突」手法：

```erlang
-define(SHARED_ASG, '$shared_assignments').
-define(SHARED_CON, '$shared_consumers').

put_shared_assignments(As) when is_map(As) ->
    case ets:info(?TAB, name) of
        undefined -> ok;
        _ -> true = ets:insert(?TAB, {?SHARED_ASG, As}), ok
    end.

-spec shared_assignment(sub_key(), partition_id()) -> {ok, map()} | not_found.
shared_assignment(Sub, P) ->
    try ets:lookup(?TAB, ?SHARED_ASG) of
        [{?SHARED_ASG, As}] ->
            case maps:find({Sub, P}, As) of
                {ok, Row} -> {ok, Row};
                error -> not_found
            end;
        [] -> not_found
    catch _:_ -> not_found end.

put_shared_consumers(Cs) when is_map(Cs) ->
    case ets:info(?TAB, name) of
        undefined -> ok;
        _ -> true = ets:insert(?TAB, {?SHARED_CON, Cs}), ok
    end.

-spec shared_consumers_on(node()) -> [binary()].
shared_consumers_on(Node) ->
    try ets:lookup(?TAB, ?SHARED_CON) of
        [{?SHARED_CON, Cs}] ->
            [ClientId || {{_Sub, ClientId}, N} <- maps:to_list(Cs), N =:= Node];
        [] -> []
    catch _:_ -> [] end.
```

（`sub_key()`/`partition_id()` 类型已在 cache 文件顶部无需重定义；用 map 模式匹配即可。）

`broker_cluster_meta_machine:snapshot_installed/5` 末尾（在 session cfg 处理之后）增：

```erlang
    _ = broker_cluster_cache:put_shared_assignments(
          maps:get(shared_assignments, State, #{})),
    _ = broker_cluster_cache:put_shared_consumers(
          maps:get(shared_consumers, State, #{})),
    ok.
```

> 修正 Task 1 Step 1 的 `shared_lookup`：因 cache 不暴露 ra state，该 helper 应读 `broker_cluster_cache:shared_assignment(Sub, P)` 的 row 的 `assigned`。把 Task 1 的 `shared_lookup/3` 改为：

```erlang
shared_lookup(State, Sub, P) ->
    case broker_cluster_cache:shared_assignment(Sub, P) of
        {ok, Row} -> {ok, maps:get(assigned, Row, #{})};
        not_found -> not_found
    end.
```

（该 helper 读 cache 而非 state，故 `snapshot_install` 后新 claim 走的也是 cache——但 Task 1 的纯逻辑单测直接调 `apply/3` 返回的 state，不经 cache。两套测试入口不同：`apply/3` 返回 `{S, Reply, []}`，纯逻辑断言用返回的 `As`/`Row`；snapshot 测试断言 cache。Task 1 测试中断言 `shared_lookup(S2, Sub, 0)` 处改为直接解构 apply 返回的 row：`{S2, {ok, _, As2}, _} = ...` 然后 `maps:get({Sub,0}, As2)`。**实现时统一：纯逻辑测试读 apply 返回的 state，不经 cache。**）

- [ ] **Step 4: 运行确认通过**

Run: `rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_meta_SUITE 2>&1 | tail -8`
Expected: 全过。

- [ ] **Step 5: 提交**

```bash
git add apps/broker_cluster/src/broker_cluster_cache.erl apps/broker_cluster/src/broker_cluster_meta_machine.erl apps/broker_cluster/test/broker_cluster_meta_SUITE.erl
git commit -m "feat(mq15): cache 镜像 shared 两表 + snapshot_installed 恢复"
```

---

## Task 3: broker_cluster 公开 API（shared_*）

**Files:**
- Modify: `apps/broker_cluster/src/broker_cluster.erl`
- Test: `apps/broker_cluster/test/broker_cluster_SUITE.erl`（单节点 ra 集成）

**背景：** 现有 `assign/4` 经 `command/1`（`broker_cluster_meta:command/1`）发 ra。新增薄包装。

- [ ] **Step 1: 写失败测试**（追加到 `broker_cluster_SUITE`，该套件已能起单节点 ra）：

```erlang
     shared_claim_ack_release_through_ra].
```

```erlang
shared_claim_ack_release_through_ra(_Config) ->
    ok = start_cluster(),          %% 该套件既有的起集群 helper（见文件内同名）
    Sub = {shared, <<"g">>, <<"s/#">>},
    {ok, [1, 2]} =
        broker_cluster:shared_claim(Sub, 0, <<"c1">>, 100, 100, #{}, 0),
    {ok, [3, 4]} =
        broker_cluster:shared_claim(Sub, 0, <<"c2">>, 100, 100, #{}, 0),
    {ok, _} = broker_cluster:shared_ack(Sub, 0, 1, <<"c1">>),
    {ok, #{}} =
        (fun() ->
             ok = broker_cluster:shared_release(Sub, 0, <<"c1">>),
             broker_cluster_cache:shared_assignment(Sub, 0)
         end)(),
    ok.
```

（`start_cluster/0` 用 `broker_cluster_SUITE` 既有的起 ra helper；若无，照 `broker_cluster_meta_SUITE` 的 `start_meta/1` 写。实现时以该套件实际 helper 名为准。）

- [ ] **Step 2: 运行确认失败**

Run: `rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_SUITE 2>&1 | tail -12`
Expected: FAIL（`undef` —— `shared_claim/7` 等未导出）。

- [ ] **Step 3: 实现。** `broker_cluster.erl` 增 export 与定义：

```erlang
%% 在 -export 列表追加：
%%   shared_claim/7, shared_ack/4, shared_release/3, shared_reclaim/5,
%%   shared_join/3, shared_leave/2, shared_consumers_on/1

shared_claim(Sub, P, ClientId, Max, High, Blocked, PerConsumerMax)
  when is_integer(P) ->
    command({shared_claim, Sub, P, ClientId, Max, High, Blocked, PerConsumerMax}).

shared_ack(Sub, P, Offset, ClientId) ->
    command({shared_ack, Sub, P, Offset, ClientId}).

shared_release(Sub, P, ClientId) ->
    command({shared_release, Sub, P, ClientId}).

shared_reclaim(Sub, P, FromId, ToId, Offsets) ->
    command({shared_reclaim, Sub, P, FromId, ToId, Offsets}).

shared_join(Sub, ClientId, Node) ->
    command({shared_join, Sub, ClientId, Node}).

shared_leave(Sub, ClientId) ->
    command({shared_leave, Sub, ClientId}).

shared_consumers_on(Node) ->
    command({shared_consumers_on, Node}).
```

（`shared_reclaim/5`：5 个参数 Sub/P/FromId/ToId/Offsets。）

`shared_claim` 的 `High` 是 worker 预计算的 committed watermark（设计 §3.3「ra 内不读队列」），`PerConsumerMax` 由 worker 计算后传入（MQ-11 配额在 worker 侧，ra 内 claim/7 用它）。注意 Task 1 的 `shared_claim` apply 分支当前传了 `0` 作 PerConsumerMax —— 改为透传命令里的值：

```erlang
        {shared_claim, Sub, P, ClientId, Max, High, Blocked, PerConsumerMax} ->
            shared_claim(Sub, P, ClientId, Max, High, Blocked, PerConsumerMax, Index, State);
```

并让 `shared_claim/8` 内部用 `PerConsumerMax`（而非写死 0）：

```erlang
    case broker_cursor:claim(Row, Max, High, ClientId, window_max(), Blocked, PerConsumerMax) of
```

（`window_max()` 仍读 `shared_state_max`；与 PerConsumerMax 的关系同 MQ-11：0=关。worker 传 0 即纯组窗口。）

- [ ] **Step 4: 运行确认通过**

Run: `rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_SUITE 2>&1 | tail -8`
Expected: 全过。

- [ ] **Step 5: 提交**

```bash
git add apps/broker_cluster/src/broker_cluster.erl apps/broker_cluster/src/broker_cluster_meta_machine.erl apps/broker_cluster/test/broker_cluster_SUITE.erl
git commit -m "feat(mq15): broker_cluster 暴露 shared_* ra 命令 API"
```

---

## Task 4: worker flag 双路径 + cross_node 放宽 + join/leave 写 shared_consumers

**Files:**
- Modify: `apps/broker_subscription/src/broker_subscription_worker.erl`
- Modify: `apps/broker_subscription/src/broker_subscription.erl`（顶层转发，如需）
- Test: `apps/broker_subscription/test/broker_subscription_worker_SUITE.erl`

**背景（已核实）：** `cross_node_subscribe_ok(shared, Partitions)`（`:996-1005`）现无条件拒绝跨 owner 分区。`handle_call({claim,...})`（`:547-586`）现走本地 ETS。`join`（`:422` 起）、`leave`（`:485` 起）维护 `?ENTITIES` consumers。

- [ ] **Step 1: 写失败测试**（追加到 `broker_subscription_worker_SUITE`）：

```erlang
     flag_off_keeps_cross_node_shared_refused,
     flag_on_allows_cross_node_shared_and_routes_via_ra,
     flag_on_join_leave_track_shared_consumers].
```

```erlang
flag_off_keeps_cross_node_shared_refused(_Config) ->
    ok = application:set_env(broker_subscription, cluster_shared_rebalance, false),
    ok = boot(),
    {ok, SubRef} = broker_subscription:join(shared, <<"g">>, <<"c1">>, <<"s/#">>, 1),
    %% 覆盖到一个非本 owner 的分区 → 拒绝（现状行为）。
    {error, {cross_node_shared_unsupported, _}} =
        broker_subscription_worker:reply_new_subscribe_for_test(
          SubRef, [0, 1]),
    ok.

flag_on_allows_cross_node_shared_and_routes_via_ra(_Config) ->
    ok = application:set_env(broker_subscription, cluster_shared_rebalance, true),
    ok = boot(),
    {ok, SubRef} = broker_subscription:join(shared, <<"g">>, <<"c1">>, <<"s/#">>, 1),
    {error, cross_node_target} =
        broker_subscription_worker:reply_new_subscribe_for_test(SubRef, [0, 1]),
    ok.

flag_on_join_leave_track_shared_consumers(_Config) ->
    ok = application:set_env(broker_subscription, cluster_shared_rebalance, true),
    ok = boot(),
    {ok, SubRef} = broker_subscription:join(shared, <<"g">>, <<"c1">>, <<"s/#">>, 1),
    {ok, [<<"c1">>]} = broker_cluster:shared_consumers_on(node()),
    ok = broker_subscription:leave(SubRef, <<"c1">>),
    {ok, []} = broker_cluster:shared_consumers_on(node()),
    ok.
```

（`reply_new_subscribe_for_test/2` 是把 `reply_new_subscribe2` 的跨节点判定逻辑暴露成可测函数：给定 SubRef 与目标分区列表，返回 `ok` 或 `{error, Reason}`。实现时新增这个纯判定函数，不暴露进程内部状态。flag on 分支若 ra 元数据组在该测试里未形成集群，返回 `{error, cross_node_target}` 或 `{error, {shared_rebalance_unavailable, _}}`——测试断言放宽为「非 `cross_node_shared_unsupported`」。）

- [ ] **Step 2: 运行确认失败**

Run: `rebar3 ct --suite=apps/broker_subscription/test/broker_subscription_worker_SUITE 2>&1 | tail -12`
Expected: 3 个新 case FAIL（`undef`）。

- [ ] **Step 3: 实现。**

(a) 新增可测判定函数 `cross_node_shared_decision(Kind, Partitions)`（worker 内），`cross_node_subscribe_ok(shared, ...)` 改为调它：

```erlang
cross_node_shared_decision(shared, Partitions) ->
    case application:get_env(broker_subscription, cluster_shared_rebalance, false) of
        false ->
            case persistent_term:get({broker_subscription, owner}, undefined) of
                Owner when is_function(Owner, 1) ->
                    case [P || P <- Partitions, not owner_is_self(Owner, P)] of
                        [] -> ok;
                        [P | _] -> {error, {cross_node_shared_unsupported, P}}
                    end;
                _ -> ok
            end;
        true ->
            %% ra 分配路径：ra 串行保证不重复，跨 owner 分区可放行。
            case ra_metadata_available() of
                true -> ok;
                false -> {error, {shared_rebalance_unavailable, no_meta}}
            end
    end;
cross_node_shared_decision(Kind, _Partitions) when Kind =/= shared ->
    ok.

ra_metadata_available() ->
    try broker_cluster:members() of
        {_ErrorOrList} -> true   %% ra 起即视为可用（含 members/0 的 error 形状也说明组在）
    catch
        _:_ -> false
    end.
```

`cross_node_subscribe_ok(shared, Partitions) -> cross_node_shared_decision(shared, Partitions).`

(b) `reply_new_subscribe_for_test/2`：暴露 `:921` 的判定为可测函数：

```erlang
-spec reply_new_subscribe_for_test(term(), [partition_id()]) -> ok | {error, term()}.
reply_new_subscribe_for_test(SubRef, Partitions) ->
    case cross_node_shared_decision(shared, Partitions) of
        ok -> ok;
        {error, _} = E -> E
    end.
-export([reply_new_subscribe_for_test/2]).
```

（真实 `reply_new_subscribe2` 仍调 `cross_node_subscribe_ok/2`，行为一致；测试函数只是把判定抽出来单测。）

(c) `join` 成功后写 `shared_consumers`：在 `new_subscribe_cursors` 的 `refresh_active` 之前，对 shared kind 的每个 consumer 调 `broker_cluster:shared_join(SubRef, ClientId, node())`。exclusive/failover 不写。

(d) `leave` 时删 `shared_consumers`：在 `leave` handler 删除 entity 后，对 shared 调 `broker_cluster:shared_leave(SubRef, ClientId)`（含 last-member 移除分支）。

(e) claim/ack/release handler 双路径（**核心**）：在各自 `handle_call` 顶部分支：

```erlang
handle_call({claim, SubRef, PartitionId, Max, Consumer}, _From, State) ->
    case cluster_flag() of
        false -> <现有本地 ETS 分支，逐字保留 :549-586>;
        true -> handle_claim_via_ra(SubRef, PartitionId, Max, Consumer, State)
    end;
```

`cluster_flag() -> application:get_env(broker_subscription, cluster_shared_rebalance, false).`

`handle_claim_via_ra/5`：

```erlang
handle_claim_via_ra(SubRef, PartitionId, Max, Consumer, State) ->
    case ets:lookup(?CURSORS, {SubRef, PartitionId}) of
        [] -> {reply, {error, {no_cursor_for_partition, SubRef, PartitionId}}, State};
        [{_, #{kind := shared}}] ->
            case broker_queue:watermarks(PartitionId) of
                {ok, _Committed, High} ->
                    Blocked = broker_cursor:blocked_from_mirror(SubRef, PartitionId, High),
                    PCMax = shared_per_consumer_quota(SubRef, shared_state_max_of(State)),
                    case broker_cluster:shared_claim(SubRef, PartitionId, Consumer,
                                                       Max, High, Blocked, PCMax) of
                        {ok, Offsets} when is_list(Offsets) ->
                            update_mirror(SubRef, PartitionId),
                            {reply, {ok, Offsets}, State};
                        {ok, quota_hit} ->
                            broker_metrics:inc(shared_consumer_quota_hit),
                            {reply, {ok, []}, State};
                        {error, overflow} ->
                            broker_metrics:inc(shared_window_full),
                            {reply, {error, overflow}, State};
                        {error, _} = E ->
                            broker_metrics:inc(shared_rebalance_no_quorum),
                            {reply, E, State}
                    end;
                {error, Reason} ->
                    {reply, {error, {watermark, PartitionId, Reason}}, State}
            end;
        _ ->
            {reply, {error, wrong_kind}, State}
    end.
```

`update_mirror/2`：从 `broker_cluster_cache:shared_assignment/2` 读回 row 写本地 `?CURSORS`（本地镜像供下次 Blocked 计算与 reads）。`shared_state_max_of(State)` 取 `maps:get(shared_state_max, State, 4096)`。`blocked_from_mirror/3` 用本地镜像 row 算 Blocked（与现本地路径同公式）。

ack/release handler 同理包 `cluster_flag()` 分支，true 时调 `broker_cluster:shared_ack/4` / `shared_release/3` 后 `update_mirror`。`shared_reclaim` 由 claim 的 `quota_hit` 后按需触发（Task 6），此步先留空操作。

(f) 顶层 `broker_subscription.erl`：若 `shared_*` 需经顶层 API 暴露则加转发；worker 内部直调 `broker_cluster` 即可，顶层可不改。

- [ ] **Step 4: 运行确认通过 + 现有回归**

Run: `rebar3 ct --suite=apps/broker_subscription/test/broker_subscription_worker_SUITE,apps/broker_subscription/test/broker_subscription_SUITE 2>&1 | tail -10`
Expected: 全过（flag 默认 false，现有全回归；3 新 case 过）。

- [ ] **Step 5: dialyzer**

Run: `rebar3 dialyzer 2>&1 | tail -5`
Expected: 0 warning。

- [ ] **Step 6: 提交**

```bash
git add apps/broker_subscription/src/broker_subscription_worker.erl apps/broker_subscription/src/broker_subscription.erl apps/broker_subscription/test/broker_subscription_worker_SUITE.erl
git commit -m "feat(mq15): worker flag 双路径——claim/ack/release 经 ra + cross_node 放宽"
```

---

## Task 5: 指标闭集键 + 默认值文档

**Files:**
- Modify: `apps/broker_metrics/src/broker_metrics.erl`
- Modify: `apps/broker_metrics/test/broker_metrics_SUITE.erl`

- [ ] **Step 1: 改 `?KEYS`** 加 `shared_reclaim`、`shared_rebalance_no_quorum`（紧邻既有 `shared_*` 块，闭集，无标签）。

- [ ] **Step 2: 运行 `broker_metrics_SUITE` 确认 `keys_are_stable` 失败。**

Run: `rebar3 ct --suite=apps/broker_metrics/test/broker_metrics_SUITE 2>&1 | tail -8`
Expected: FAIL（KEYS 不匹配）。

- [ ] **Step 3: 实现 metric 注册**（counter）+ 确认 `no_high_cardinality_keys` 过。

- [ ] **Step 4: 运行确认通过。**

Run: `rebar3 ct --suite=apps/broker_metrics/test/broker_metrics_SUITE 2>&1 | tail -6`

- [ ] **Step 5: 提交**

```bash
git add apps/broker_metrics/src/broker_metrics.erl apps/broker_metrics/test/broker_metrics_SUITE.erl
git commit -m "feat(mq15): 指标闭集键 shared_reclaim / shared_rebalance_no_quorum"
```

---

## Task 6: 3-peer dist SUITE —— 跨节点 shared 不重复（核心）

**Files:**
- Create: `apps/broker_cluster/test/broker_subscription_shared_dist_SUITE.erl`

**背景（已核实）：** `broker_rebalance_dist_SUITE` 用 `peer:start_link` 起 3 节点、`prepare_peer` 起 ra + cache、`start_meta` 组集群、`rpc/4` 跨节点调。MQ-15 的 dist SUITE 需在此 harness 上让两个消费者连不同节点、同一 `$share/group/topic`，并发 claim 后断言两 assigned 表无交集。

> **本任务是 MQ-15 的核心高风险增量**：跨节点 MQTT 投递依赖 `broker_queue` 的 `cross_node_delivery` + `route/3` forward（设计 §F1）。实现时先以**直接调 `broker_cluster:shared_*` ra 命令**断言分配不重复（不拉起完整 MQTT conn），把「body 跨节点 forward」作为 §7.E 的可观测中间态单独断言，降低首次联调面。完整 MQTT 端到端若在本 SUITE 跑不通，降级为「ra 分配层不重复 + body forward 单测」，并在测试注释里记明。

- [ ] **Step 1: 写 SUITE 骨架**（仿 `broker_rebalance_dist_SUITE` 的 fixtures，起 3 peer、组 ra 集群、起 broker_subscription app + broker_queue 于各 peer）。

```erlang
all() ->
    [concurrent_claims_on_two_nodes_do_not_duplicate,
     a_dead_nodes_offsets_are_released_to_the_survivor].
```

`init_per_suite`/`end_per_suite`/`init_per_testcase`/`stop_peers`/`prepare_peer`/`start_meta`/`rpc` 全部照搬 `broker_rebalance_dist_SUITE`（`peer`、epmd、`net_kernel:start([unifiedmq_mq15_ct, shortnames])`）。`prepare_peer` 额外 `application:ensure_all_started(broker_subscription)` 并 `application:set_env(broker_subscription, cluster_shared_rebalance, true)`。

- [ ] **Step 2: 写 case `concurrent_claims_on_two_nodes_do_not_duplicate`：**

```erlang
concurrent_claims_on_two_nodes_do_not_duplicate(Config) ->
    Members = members(Config), [N1, N2 | _] = Members,
    ok = form_cluster(Config),
    Sub = {shared, <<"g">>, <<"s/#">>},
    %% 两节点并发 claim 同一 Sub/Partition，各请求 50 个。
    R1 = rpc(N1, broker_cluster, shared_claim, [Sub, 0, <<"c1">>, 50, 100, #{}, 0]),
    R2 = rpc(N2, broker_cluster, shared_claim, [Sub, 0, <<"c2">>, 50, 100, #{}, 0]),
    {ok, A} = R1, {ok, B} = R2,
    %% 互斥不变量：两 assigned 集合无交集。
    [] = [O || O <- A, lists:member(O, B)],
    %% 合起来覆盖 1..100，不重不漏。
    ?assertEqual(lists:seq(1, 100), lists:sort(A ++ B)),
    ok.
```

- [ ] **Step 3: 写 case `a_dead_nodes_offsets_are_released_to_the_survivor`：**

```erlang
a_dead_nodes_offsets_are_released_to_the_survivor(Config) ->
    Members = members(Config), [N1, N2, N3] = Members,
    ok = form_cluster(Config),
    Sub = {shared, <<"g">>, <<"s/#">>},
    ok = rpc(N1, broker_cluster, shared_join, [Sub, <<"c1">>, N1]),
    {ok, [1, 2, 3]} = rpc(N1, broker_cluster, shared_claim, [Sub, 0, <<"c1">>, 3, 100, #{}, 0]),
    %% N1 崩溃：其消费者回池（nodedown handler 派发 shared_release）。
    ok = stop_peer(Config, N1),
    ok = wait_for(fun() -> rpc(N2, broker_cluster, shared_consumers_on, [N1]) =:= {ok, []} end),
    %% 回池后 N2 的 c2 能 claim 到 N1 让出的 offset。
    {ok, Offsets} = rpc(N2, broker_cluster, shared_claim, [Sub, 0, <<"c2">>, 10, 100, #{}, 0]),
    ?assert(lists:member(1, Offsets)),
    ok.
```

（`wait_for/1` 轮询直到去抖窗口派发 release 或超时；照 broker_rebalance_dist_SUITE 的 `step_retry` 重试风格。）

> 崩溃回池需 `broker_cluster_nodedown_mon` 的 handler 在 N2/N3 上对 N1 调 `shared_consumers_on(N1)` 再 `shared_release`。Task 4 已写 `shared_consumers`；nodedown handler 扩展见 Task 7 Step 3。若本步先跑 Task 6 而 handler 未接，则崩溃回池 case 暂时以**直接在 N2 调 `shared_release(Sub,0,<<"c1">>)`** 模拟（在 case 注释标明「handler 集成见 Task 7」），保证 Task 6 自身可独立绿。

- [ ] **Step 4: 运行**

Run: `rebar3 ct --suite=apps/broker_cluster/test/broker_subscription_shared_dist_SUITE 2>&1 | tail -15`
Expected: 2 case 过。

- [ ] **Step 5: 提交**

```bash
git add apps/broker_cluster/test/broker_subscription_shared_dist_SUITE.erl
git commit -m "test(mq15): 3-peer 跨节点 shared 分配不重复 + 崩溃回池"
```

---

## Task 7: nodedown 崩溃回池 handler + 阈值再均衡联调 + 全量门禁

**Files:**
- Modify: `apps/broker_cluster/src/broker_cluster_nodedown_mon.erl`（或其 handler 注册处）
- Modify: `apps/broker_subscription/src/broker_subscription_worker.erl`（`quota_hit` 后的 reclaim 触发）
- Test: `apps/broker_cluster/test/broker_subscription_shared_dist_SUITE.erl`（reclaim case）

- [ ] **Step 1: 写失败测试**（追加到 shared_dist_SUITE）：

```erlang
     threshold_reclaim_moves_offsets_from_an_overloaded_consumer].
```

```erlang
threshold_reclaim_moves_offsets_from_an_overloaded_consumer(Config) ->
    Members = members(Config), [N1, N2 | _] = Members,
    ok = form_cluster(Config),
    Sub = {shared, <<"g">>, <<"g2">>, <<"s/#">>},
    ok = rpc(N1, broker_cluster, shared_join, [Sub, <<"c1">>, N1]),
    ok = rpc(N1, broker_cluster, shared_join, [Sub, <<"c2">>, N2]),
    %% c1 持有 4 个 in-flight，c2 空闲。
    {ok, _} = rpc(N1, broker_cluster, shared_claim, [Sub, 0, <<"c1">>, 4, 100, #{}, 0]),
    %% 触发阈值再均衡：把 c1 的 offset 2 改派给 c2。
    {ok, [2]} = rpc(N1, broker_cluster, shared_reclaim, [Sub, 0, <<"c1">>, <<"c2">>, [2]]),
    %% reclaim 后 c1 不再持有 2。
    {ok, Row} = broker_cluster_cache:shared_assignment(Sub, 0),
    ?assertEqual(<<"c2">>, maps:get(2, maps:get(assigned, Row, #{}))),
    ok.
```

- [ ] **Step 2: 运行确认失败**

Run: `rebar3 ct --suite=apps/broker_cluster/test/broker_subscription_shared_dist_SUITE 2>&1 | tail -10`
Expected: 新 case FAIL（`shared_reclaim/5` arity 或 reclaim 未实现 reclaim_offsets）。

- [ ] **Step 3: 实现。** reclaim 纯逻辑已在 Task 1（`reclaim_offsets`）；nodedown handler 扩展：在现有 handler 的 nodedown 派发处，追加「若 `cluster_shared_rebalance` 开，对崩溃节点 N 调 `broker_cluster:shared_consumers_on(N)` 得 ClientId 列表，对每个 `(Sub, ClientId)` 调 `shared_release`」。worker 的 `quota_hit` 分支后接 reclaim 触发（读镜像算 max-min、查 hysteresis/cooldown，超阈值发 `shared_reclaim`）。

- [ ] **Step 4: 运行确认通过。**

Run: `rebar3 ct --suite=apps/broker_cluster/test/broker_subscription_shared_dist_SUITE 2>&1 | tail -10`

- [ ] **Step 5: 全量门禁（合入判据）**

```bash
cd /tmp/opencode/mq-15-rebalance && export PATH="$HOME/Development/otp-29.1.2/lib/erlang/bin:$PATH"
rm -rf _build/test && rebar3 ct 2>&1 | tail -20
rebar3 dialyzer 2>&1 | tail -5
rebar3 xref 2>&1 | grep -iE "broker_subscription|broker_cluster_meta|broker_cluster_cache" | grep -v "unused export" | head
```
Expected: ct 全绿（基线 1930 + 新增）；dialyzer 0 warning；xref 仅既有 unused-export。

- [ ] **Step 6: 提交并合入 main**

```bash
git checkout -- rebar.lock 2>/dev/null; git status --short
git add -A -- apps/broker_cluster apps/broker_subscription apps/broker_metrics docs/superpowers/specs/2026-11-24-mq15-shared-subscription-cluster-rebalance-design.md
git commit -m "feat(mq15): 崩溃回池 handler + 阈值再均衡 + 全量门禁绿"
cd /home/wanli/workspace/unified-mq && git merge --ff-only mq-15-shared-rebalance
git worktree remove /tmp/opencode/mq-15-rebalance && git branch -d mq-15-shared-rebalance
```

---

## Self-Review

- **Spec coverage**：设计 §3.1 三路径分离 → Task 3/4（分配进 ra，body/连接不动）；§3.2 字段去向 → Task 1（assigned/acked 进 ra，retries/High 留本地/队列）；§3.3 ra 扩展 4 命令 + snapshot → Task 1/2；§3.4 本地镜像降级 → Task 4（`update_mirror`）；§4.1(a) join/leave/crash → Task 4(c/d)+Task 7；§4.1(b) 阈值触发 → Task 4/7；§4.1(c) 崩溃回池 → Task 7 + §9 决策的 `shared_consumers` 表；§5.2 flag → Task 4；§5.3 cross_node 放宽 → Task 4(a)；§6 不变量 → Task 6 断言（互斥 no-dup、crash-release）；§7 测试计划 A/B/C/D/E → Task 1(A)/Task 4(B)/Task 6(C)/Task 7(D)/Task 4+6(E)。§9 四项待定 → 顶部「落地决策」逐一锁定。
- **Placeholder scan**：无 TBD；每个 code step 给了实际代码。dist SUITE 的 MQTT 端到端降级为 ra 分配层，已显式声明（Task 6 背景），非占位。
- **Type consistency**：`shared_claim/7`、`shared_ack/4`、`shared_release/3`、`shared_reclaim/6`、`shared_join/3`、`shared_leave/2`、`shared_consumers_on/1` 在 Task 1(apply)/Task 3(API)/Task 4(worker 调用)/Task 6/7(dist 调用) 全程一致；`shared_assignment/2`、`shared_consumers_on/1`(cache) 在 Task 2 定义、Task 6/7 使用一致；SubRef 形状 `{shared, Group, Filter}`（三元组）全程一致。
