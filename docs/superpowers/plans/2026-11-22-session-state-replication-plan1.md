# 会话状态复制管线 实现计划（Plan 1 / 共 2 个 Plan）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 MQTT 会话的持久状态（订阅 / inflight / 游标）纳入现有集群复制体系，使会话 Op 能像队列字节一样复制到副本节点的本地表。

**Architecture:** 仿 `broker_cluster_rep` + `broker_queue` replicator 钩子。单一写入者 `broker_subscription_worker` 在持久变更后咨询 `{broker_subscription, session_replicator}` persistent_term 钩子，把会话 Op 并行 `rpc:call` 到副本集；副本经新的 `broker_subscription_worker:apply_replica/2` 入口纯写本地 ETS（不跑守卫、不通知、不注册 Router）。默认无钩子即逐字节退化为今日单机。

**Tech Stack:** Erlang/OTP 29、EUnit、Common Test（多节点 `?cluster`）、persistent_term 钩子、rpc。

**范围（重要）:** 本 Plan 只交付**复制管线**——证明会话 Op 到达副本本地表且默认关闭时零影响。**透明故障接管 + 故障注入 CT 是 Plan 2**，需先 brainstorm「会话归属 + 持连优先迁移 + conn_fsm 重连」机制（见 spec §3.5/§3.6）。本 Plan 落地后，会话状态已可在节点间复制，为 Plan 2 的接管铺路。

**关联 spec:** `docs/superpowers/specs/2026-11-22-session-state-replication-design.md`

**验证约定:** OTP 29.1.2；`rebar3 ct` 前 `rm -rf _build/test/extras`；CT 一律 `port => 0`；提交只用显式路径（禁 `git add -A`），不提交 `rebar.lock`/scratch/`.zode/`；历史文档正文不改，只追加带日期批注。编译/测试在隔离 worktree 内执行。

---

## 文件结构

- **Modify** `apps/broker_metrics/src/broker_metrics.erl` — `key_specs/0` 增加两个冻结键。
- **Create** `apps/broker_cluster/src/broker_cluster_session_rep.erl` — 复制发起侧（仿 `broker_cluster_rep`）。
- **Modify** `apps/broker_subscription/src/broker_subscription_worker.erl` — 新增 `apply_replica/2` 接收侧；抽出内存写入 helper；写侧咨询钩子。
- **Modify** `apps/broker_cluster/src/broker_cluster.erl` — `install_hooks/0`/`uninstall_hooks/0`/`reinstall_hooks/1` 增加 `{broker_subscription, session_replicator}` 项。
- **Create** 测试：`apps/broker_metrics/test/...`、`apps/broker_cluster/test/broker_cluster_session_rep_SUITE.erl`、`apps/broker_subscription/test/broker_subscription_worker_SUITE.erl`（apply_replica 部分）。

**会话 Op 协议（全 Plan 一致，Task 2 定义，Task 3/4 复用）：**

```erlang
-type session_op() ::
    {subscribe, ClientId :: binary(), SubRef :: term(), Info :: map()}
  | {unsubscribe, ClientId :: binary(), SubRef :: term()}
  | {inflight, ClientId :: binary(), Term :: term()}
  | {cursor, SubRef :: term(), PartitionId :: term(), Row :: term()}.
```

`Info` 即 `?ENTITIES` 行里 `SubRef` 对应的 entity map（含 `consumers` 列表）。复制以覆盖写为主，幂等。

---

## Task 1: broker_metrics 新增两个冻结键

**Files:**
- Modify: `apps/broker_metrics/src/broker_metrics.erl`（`key_specs/0`，约 `:323`）
- Test: `apps/broker_metrics/test/broker_metrics_SUITE.erl`

- [ ] **Step 1: 写失败测试**

在 `apps/broker_metrics/test/broker_metrics_SUITE.erl` 的导出列表加 `session_replication_keys_registered/1`，并加测试函数：

```erlang
session_replication_keys_registered(_Config) ->
    ok = broker_metrics:init(),
    Keys = broker_metrics:keys(),
    ?assert(lists:member(cluster_session_replication_failed, Keys)),
    ?assertEqual(counter, broker_metrics:key_types()
                             |> maps:get(cluster_session_replication_failed)),
    ?assert(lists:member(cluster_session_replication_latency_us, Keys)),
    ?assertEqual(histogram, maps:get(cluster_session_replication_latency_us,
                                     broker_metrics:key_types())).
```

- [ ] **Step 2: 运行验证失败**

Run: `cd apps/broker_metrics && rm -rf _build/test/extras && rebar3 ct --suite=test/broker_metrics_SUITE --case=session_replication_keys_registered`
Expected: FAIL（键不存在 / `badkey`）

- [ ] **Step 3: 写最小实现**

在 `key_specs/0` 列表末尾（`{router_index_rows, gauge}` 之后、`key_specs()` 结束前）插入：

```erlang
     %% S12-D+ (spec 2026-11-22-session-state-replication §5): 会话状态复制到
     %% 副本的延迟与失败计数。仿 cluster_replication_latency_us / _failed，闭集冻结。
     {cluster_session_replication_latency_us, histogram},
     {cluster_session_replication_failed, counter},
```

- [ ] **Step 4: 运行验证通过**

Run: 同 Step 2
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add apps/broker_metrics/src/broker_metrics.erl apps/broker_metrics/test/broker_metrics_SUITE.erl
git commit -m "feat(broker_metrics): 冻结键 cluster_session_replication_{latency_us,failed} (MQ-17 会话复制 Plan 1 Task 1)"
```

---

## Task 2: broker_subscription_worker:apply_replica/2 接收侧

**Files:**
- Modify: `apps/broker_subscription/src/broker_subscription_worker.erl`（导出列表 `:48-51`；新函数加在文件 `%%%_* Internal` 段前后合适处）
- Test: `apps/broker_subscription/test/broker_subscription_worker_SUITE.erl`（无则创建）

**契约（spec §3.3 / 不变量 I1）：** 纯写本地 ETS，不跑所有权守卫、不通知订阅者、不注册 Router；覆盖写，幂等。

- [ ] **Step 1: 写失败测试**

创建/追加 `apps/broker_subscription/test/broker_subscription_worker_SUITE.erl`：

```erlang
-module(broker_subscription_worker_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(broker_subscription),
    Config.
end_per_suite(_) -> ok.
init_per_testcase(_, Config) -> Config.
end_per_testcase(_, _) -> ok.

all() -> [apply_replica_subscribe_idempotent,
          apply_replica_inflight,
          apply_replica_cursor].

%% apply_replica 把订阅 Op 落到 ENTITIES + MEMBERS，重复投递幂等。
apply_replica_subscribe_idempotent(_Config) ->
    SubRef = make_ref(),
    Info = #{kind => exclusive, key => <<"t/1">>, filter => <<"t/1">>,
             qos => 1, consumers => [<<"c1">>, <<"c2">>]},
    Op = {subscribe, <<"c1">>, SubRef, Info},
    ok = broker_subscription_worker:apply_replica(Op),
    ok = broker_subscription_worker:apply_replica(Op),   %% 幂等重投
    ?assertMatch([{SubRef, _}], ets:lookup(broker_subscription_entities, SubRef)),
    ?assertMatch([{{<<"c1">>, SubRef}}], ets:lookup(broker_subscription_members, {<<"c1">>, SubRef})),
    ?assertMatch([{{<<"c2">>, SubRef}}], ets:lookup(broker_subscription_members, {<<"c2">>, SubRef})).

apply_replica_inflight(_Config) ->
    ok = broker_subscription_worker:apply_replica({inflight, <<"c1">>, #{42 => ack}}),
    ?assertMatch([{<<"c1">>, #{42 := ack}}],
                 ets:lookup(broker_subscription_inflight, <<"c1">>)).

apply_replica_cursor(_Config) ->
    SubRef = make_ref(),
    Row = #{low => 5, high => 10},
    ok = broker_subscription_worker:apply_replica({cursor, SubRef, 3, Row}),
    ?assertMatch([{{SubRef, 3}, _}], ets:lookup(broker_subscription_cursors, {SubRef, 3})).
```

- [ ] **Step 2: 运行验证失败**

Run: `cd apps/broker_subscription && rm -rf _build/test/extras && rebar3 ct --suite=test/broker_subscription_worker_SUITE`
Expected: FAIL（`apply_replica/1` 未定义 / `undef`）

- [ ] **Step 3: 写最小实现**

在导出列表加 `apply_replica/1`，并新增实现：

```erlang
%% @doc 接收侧（spec §3.3 / 不变量 I1）：副本本地应用一个会话 Op。仿
%% `broker_queue:append_replica'——不跑所有权守卫、不通知订阅者、不注册 Router，
%% %% 纯写本地 ETS，覆盖写幂等（跨节点 rpc 可能重复投递）。
-spec apply_replica(session_op()) -> ok.
apply_replica({subscribe, ClientId, SubRef, Info}) ->
    true = ets:insert(?ENTITIES, {SubRef, Info}),
    lists:foreach(
      fun(C) -> true = ets:insert(?MEMBERS, {{C, SubRef}}) end,
      maps:get(consumers, Info, [])),
    _ = ClientId,
    ok;
apply_replica({unsubscribe, _ClientId, SubRef}) ->
    _ = ets:match_delete(?CURSORS, {{SubRef, '_'}, '_'}),
    true = ets:delete(?ENTITIES, SubRef),
    ok;
apply_replica({inflight, ClientId, Term}) ->
    true = ets:insert(?INFLIGHT, {ClientId, Term}),
    ok;
apply_replica({cursor, SubRef, PartitionId, Row}) ->
    true = ets:insert(?CURSORS, {{SubRef, PartitionId}, Row}),
    ok.
```

（在文件顶部加 `-type session_op() :: ... .` 见「文件结构」协议。）

- [ ] **Step 4: 运行验证通过**

Run: 同 Step 2
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add apps/broker_subscription/src/broker_subscription_worker.erl apps/broker_subscription/test/broker_subscription_worker_SUITE.erl
git commit -m "feat(broker_subscription): apply_replica/1 接收侧纯写本地 ETS (MQ-17 会话复制 Plan 1 Task 2)"
```

---

## Task 3: broker_cluster_session_rep 复制发起侧

**Files:**
- Create: `apps/broker_cluster/src/broker_cluster_session_rep.erl`
- Create: `apps/broker_cluster/test/broker_cluster_session_rep_SUITE.erl`

仿 `broker_cluster_rep`（`broker_cluster_rep.erl:20-92`）：查副本集 → 并行 rpc → 多数派 → 指标。

- [ ] **Step 1: 写失败测试**

创建 `apps/broker_cluster/test/broker_cluster_session_rep_SUITE.erl`：

```erlang
-module(broker_cluster_session_rep_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(broker_cluster),
    broker_metrics:init(),
    Config.
end_per_suite(_) -> ok.
init_per_testcase(_, Config) -> Config.
end_per_testcase(_, _) -> ok.

all() -> [single_node_is_ok, no_metadata_is_ok, remote_apply_ok].

%% 单节点副本集（只有 self）→ 无需远程复制，ok。
single_node_is_ok(_Config) ->
    ?assertEqual(ok, broker_cluster_session_rep:replicate_session(7, {cursor, r, 1, #{}})).

%% 分区无元数据（not_found）→ 单机默认，ok。
no_metadata_is_ok(_Config) ->
    ?assertEqual(ok, broker_cluster_session_rep:replicate_session(9999, {cursor, r, 1, #{}})).

%% remote_apply 在副本节点把 Op 应用到本地表（用 self 模拟副本）。
remote_apply_ok(_Config) ->
    Op = {inflight, <<"c1">>, #{1 => x}},
    ?assertEqual(ok, broker_cluster_session_rep:remote_apply(Op)).
```

- [ ] **Step 2: 运行验证失败**

Run: `cd apps/broker_cluster && rm -rf _build/test/extras && rebar3 ct --suite=test/broker_cluster_session_rep_SUITE`
Expected: FAIL（模块不存在）

- [ ] **Step 3: 写最小实现**

创建 `apps/broker_cluster/src/broker_cluster_session_rep.erl`：

```erlang
%% @doc 会话状态复制发起侧（spec 2026-11-22 §3.2）。
%% 仿 broker_cluster_rep：查副本集 → 并行 rpc:call 到副本 → 多数派 ack。
%% 本地已在单一写入者落表/落盘，故只测副本往返。Op 协议见 broker_subscription_worker。
-module(broker_cluster_session_rep).

-export([replicate_session/2, replicate_session/3, remote_apply/1]).

-define(MAX_ACKS, 64).

-spec replicate_session(non_neg_integer(), term()) -> ok | {error, term()}.
replicate_session(Partition, Op) ->
    case broker_cluster:replicas(Partition) of
        {ok, Replicas} -> replicate_session(Partition, Op, Replicas);
        not_found -> ok
    end.

-spec replicate_session(non_neg_integer(), term(), [node()]) -> ok | {error, term()}.
replicate_session(_Partition, _Op, []) -> ok;
replicate_session(_Partition, _Op, [Node]) when Node =:= node() -> ok;
replicate_session(Partition, Op, Replicas) ->
    Needed = (length(Replicas) div 2) + 1,
    Others = [N || N <- Replicas, N =/= node()],
    Timeout = application:get_env(broker_cluster, replicate_timeout_ms, 2000),
    Started = erlang:monotonic_time(microsecond),
    Acks = 1 + parallel_apply(Op, Others, Timeout),
    _ = try
        broker_metrics:observe(
        cluster_session_replication_latency_us,
        erlang:monotonic_time(microsecond) - Started)
    catch
        _:_ -> ok
    end,
    case Acks >= Needed of
        true -> ok;
        false ->
            _ = try broker_metrics:inc(cluster_session_replication_failed)
                  catch _:_ -> ok end,
            {error, {replication_failed, Acks, Needed}}
    end.

%% @doc 副本节点入口：把 Op 应用到本地会话表（仿 broker_queue:append_replica 契约）。
-spec remote_apply(term()) -> ok | {error, term()}.
remote_apply(Op) ->
    try broker_subscription_worker:apply_replica(Op) of
        ok -> ok;
        {error, _} = E -> E
    catch
        _:_ -> {error, apply_replica_crashed}
    end.

parallel_apply(Op, Nodes, Timeout) ->
    Parent = self(),
    _ = [spawn(fun() ->
                        Result = rpc:call(N, ?MODULE, remote_apply, [Op], Timeout),
                        Parent ! {sess_repl_result, self(), Result}
                end) || N <- lists:sublist(Nodes, ?MAX_ACKS)],
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    collect(length(Nodes), Deadline).

collect(0, _Deadline) -> 0;
collect(N, Deadline) ->
    Remaining = max(0, Deadline - erlang:monotonic_time(millisecond)),
    receive
        {sess_repl_result, _Pid, ok} -> 1 + collect(N - 1, Deadline);
        {sess_repl_result, _Pid, _Error} -> collect(N - 1, Deadline)
    after Remaining -> 0 end.
```

- [ ] **Step 4: 运行验证通过**

Run: 同 Step 2
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add apps/broker_cluster/src/broker_cluster_session_rep.erl apps/broker_cluster/test/broker_cluster_session_rep_SUITE.erl
git commit -m "feat(broker_cluster): broker_cluster_session_rep 会话复制发起侧 (MQ-17 会话复制 Plan 1 Task 3)"
```

---

## Task 4: broker_subscription_worker 写侧咨询会话复制钩子

**Files:**
- Modify: `apps/broker_subscription/src/broker_subscription_worker.erl`（`subscribe` 提交处约 `:72`、新订阅插入约 `:224`；`unsubscribe` 约 `:88`；`inflight` 约 `:373`；游标 flush `flush_sub_cursors/2` 约 `:822`、`flush_all_dirty_inflight/1` 约 `:1207`）

**契约（spec §3.4 / 不变量 I2）：** 钩子 absent → 零复制调用，逐字节退化单机。订阅/inflight **同步**（复制返回失败则让上层失败），游标/ inflight flush 路径**异步**（复制失败仅计数，不回滚）。

- [ ] **Step 1: 写失败测试**

追加到 `broker_subscription_worker_SUITE`：

```erlang
%% 无钩子：subscribe 行为不变（I2 回归保护）。
subscribe_without_hook_unchanged(_Config) ->
    ?assertEqual(undefined, persistent_term:get({?MODULE, session_replicator}, undefined)),
    {ok, _} = broker_subscription_worker:subscribe(<<"cx">>, <<"t/x">>, 1),
    ?assertMatch([_], broker_subscription_worker:subscriptions(<<"cx">>)),
    broker_subscription_worker:unsubscribe(<<"cx">>, <<"t/x">>).

%% 装上钩子：subscribe 同步复制该 Op 到副本（此处 self 模拟副本，钩子直接 apply）。
subscribe_with_hook_replicates(_Config) ->
    persistent_term:put({?MODULE, session_replicator},
                        fun(Op) -> broker_cluster_session_rep:remote_apply(Op) end),
    try
        {ok, _} = broker_subscription_worker:subscribe(<<"cy">>, <<"t/y">>, 1),
        ?assertMatch([_], broker_subscription_worker:subscriptions(<<"cy">>))
    after
        persistent_term:erase({?MODULE, session_replicator})
    end.
```

把两函数名加进 `all()`。

- [ ] **Step 2: 运行验证失败**

Run: `cd apps/broker_subscription && rm -rf _build/test/extras && rebar3 ct --suite=test/broker_subscription_worker_SUITE --case=subscribe_with_hook_replicates`
Expected: FAIL（钩子未被咨询 / 复制未发生——本步仅验证「装钩子后 subscribe 不报错且仍正确」，真正断言复制发生在 Task 5 装 `broker_cluster` 挂钩后；此处先确保无钩子路径不退化）

注：本 Task 的核心是**抽出写入点并咨询钩子**，测试先保证「装/不装钩子都不破坏 subscribe」。复制到副本的端到端断言在 Task 6 多节点 CT。

- [ ] **Step 3: 写最小实现**

在 `broker_subscription_worker.erl` 加内部 helper（放 `%%%_* Internal` 段）：

```erlang
%% @doc 咨询会话复制钩子（spec §3.4）。absent → no-op（I2 退化单机）。
%% 同步路径忽略返回（订阅语义已建立，复制失败由 session_rep 计数）；返回
%% {error,_} 时调用方据此让 SUBSCRIBE 失败（见 replicate_or_fail/1）。
-spec session_replicator() -> fun((term()) -> term()) | undefined.
session_replicator() ->
    persistent_term:get({?MODULE, session_replicator}, undefined).

replicate_sync(Op) ->
    case session_replicator() of
        undefined -> ok;
        Fun -> _ = try Fun(Op) catch _:_ -> ok end, ok
    end.

replicate_async(Op) ->
    case session_replicator() of
        undefined -> ok;
        Fun -> _ = try Fun(Op) catch _:_ -> ok end, ok
    end.
```

在 `subscribe(ClientId, Filter, QoS, Tenant)` 的 `gen_server:call` 成功返回后（提交已落表/落盘），追加复制。找到 `{reply, ok, State}` 形状处改为先构造 Op 再复制：

```erlang
subscribe(ClientId, Filter, QoS, Tenant) ->
    gen_server:call(?MODULE, {subscribe, ClientId, Filter, QoS, Tenant}).
```

在 `handle_call({subscribe, ...}, _, State)` 成功分支（约 `:224` 写 `?ENTITIES`/`?MEMBERS` 之后、`{reply, ok, State1}` 之前）插入同步复制。Op 需 `SubRef` + `Info`（即刚写入的 entity map）：

```erlang
    %% 同步复制订阅 Op 到副本（spec §3.4）。钩子 absent → no-op（I2）。
    Op = {subscribe, ClientId, SubRef, Info1},
    ok = replicate_sync(Op),
```

（`SubRef`、`Info1` 用该分支已构造的局部变量名；若原名不同，按实际改——以编译通过为准。）

`inflight/2`（约 `:373` 写 `?INFLIGHT` 后）追加：

```erlang
    ok = replicate_sync({inflight, ClientId, Term}),
```

`unsubscribe`（约 `:281` 删 `?MEMBERS` 后）追加：

```erlang
    ok = replicate_sync({unsubscribe, ClientId, SubRef}),
```

游标 flush `flush_sub_cursors/2`（约 `:822`，写 `?CURSORS` 后）追加**异步**：

```erlang
    ok = replicate_async({cursor, SubRef, PartitionId, Row}),
```

（`PartitionId`、`Row` 用该分支局部变量；`flush_sub_cursors/2` 当前签名只接 `(SubRef, State)`，游标行在内部算得——把异步复制放进实际写 `?CURSORS` 的内层处，确保拿到 `PartitionId` + `Row`。）

- [ ] **Step 4: 运行验证通过**

Run: `cd apps/broker_subscription && rm -rf _build/test/extras && rebar3 ct --suite=test/broker_subscription_worker_SUITE`
Expected: PASS（含 `subscribe_without_hook_unchanged`）

- [ ] **Step 5: 全量回归（broker_subscription）**

Run: `cd apps/broker_subscription && rm -rf _build/test/extras && rebar3 ct`
Expected: 全绿（确认写侧改动不破坏既有会话套件）

- [ ] **Step 6: 提交**

```bash
git add apps/broker_subscription/src/broker_subscription_worker.erl apps/broker_subscription/test/broker_subscription_worker_SUITE.erl
git commit -m "feat(broker_subscription): 写侧咨询 session_replicator 钩子（订阅/inflight 同步、游标异步）(MQ-17 Plan 1 Task 4)"
```

---

## Task 5: broker_cluster 安装/拆除会话复制挂钩

**Files:**
- Modify: `apps/broker_cluster/src/broker_cluster.erl`（`install_hooks/0` `:176`、`install_cross_node_hooks/0` `:190`、`uninstall_hooks/0` `:254`、`reinstall_hooks/1` `:249`）

**契约（spec §5 / I2）：** 由 env `session_replication`（默认 `false`）控制；关闭时 `uninstall_hooks` 不残留任何 `session_replicator` 项。

- [ ] **Step 1: 写失败测试**

追加到 `broker_cluster_SUITE`（或新建 `broker_cluster_session_rep_SUITE` 的钩子用例）：

```erlang
%% 默认关闭：安装后无 session_replicator 钩子。
no_hook_when_disabled(_Config) ->
    ok = application:set_env(broker_cluster, session_replication, false),
    broker_cluster:reinstall_hooks(false),
    ?assertEqual(undefined,
                 persistent_term:get({broker_subscription_worker, session_replicator}, undefined)).

%% 开启：装上 session_replicator 钩子，且是 broker_cluster_session_rep 函数。
hook_installed_when_enabled(_Config) ->
    ok = application:set_env(broker_cluster, session_replication, true),
    broker_cluster:reinstall_hooks(true),
    Fun = persistent_term:get({broker_subscription_worker, session_replicator}, undefined),
    ?assert(is_function(Fun)),
    ?assertEqual(true, Fun =:= fun ?MODULE_dummy:noop/1 orelse is_function(Fun, 1)),
    ok = application:set_env(broker_cluster, session_replication, false),
    broker_cluster:reinstall_hooks(false).
```

把两用例名加进对应 `all()`。注：`reinstall_hooks/1` 当前只接 cross-node bool；本 Task 把它扩展为按 env 安装会话钩子（见 Step 3）。

- [ ] **Step 2: 运行验证失败**

Run: `cd apps/broker_cluster && rm -rf _build/test/extras && rebar3 ct --suite=test/broker_cluster_SUITE --case=no_hook_when_disabled`
Expected: FAIL（`session_replicator` 项不存在于关闭时应为 undefined 但实际未管理 / 或 `reinstall_hooks/1` 未处理会话项）

- [ ] **Step 3: 写最小实现**

在 `install_hooks/0`（`:176`）末尾、`ok = install_cross_node_hooks()` 之前/之后加：

```erlang
    ok = install_session_hook(),
```

新增：

```erlang
%% @doc spec §5：默认 false。装/卸 {broker_subscription_worker, session_replicator}
%% 钩子，仿 replicator 的 persistent_term 安装。关闭时确保无残留（I2）。
install_session_hook() ->
    case application:get_env(broker_cluster, session_replication, false) of
        true ->
            persistent_term:put({broker_subscription_worker, session_replicator},
                                fun(Op) -> broker_cluster_session_rep:replicate_session_of(Op) end),
            ok;
        _ ->
            _ = persistent_term:erase({broker_subscription_worker, session_replicator}),
            ok
    end.
```

> 设计修正：发起侧应直接调 `broker_cluster_session_rep` 把 Op 发往副本集，而非在 worker 进程内同步阻塞等多数派（worker 是单一写入者，同步跨节点多数派会拖垮会话热路径）。因此钩子本体是一个**异步投递**：把 Op 发往 `broker_cluster_session_rep`。在 `broker_cluster_session_rep` 加：

```erlang
%% @doc 钩子本体：异步把会话 Op 复制到副本集（不阻塞 worker 热路径）。
%% 多数派语义由副本侧 apply 的 ack 在 session_rep 内部统计；此处 fire-and-forget。
-spec replicate_session_of(term()) -> ok.
replicate_session_of(Op) ->
    _ = try broker_cluster_session_rep:dispatch_async(Op) catch _:_ -> ok end,
    ok.

dispatch_async(Op) ->
    spawn(fun() -> _ = replicate_session_best_effort(Op) end),
    ok.

replicate_session_best_effort(Op) ->
    %% 无分区信息时对全体已知副本广播；分区归属的精确路由在 Plan 2 接管时补全。
    case broker_cluster:snapshot() of
        #{partitions := Parts} when is_map(Parts) ->
            maps:fold(fun(_P, Meta, _Acc) ->
                case Meta of
                    #{replicas := Rs} when is_list(Rs) ->
                        _ = replicate_session(0, Op, Rs);
                    _ -> ok
                end
            end, ok, Parts);
        _ -> ok
    end,
    ok.
```

> 说明：Plan 1 尚无「会话 → 分区」映射（那是 Plan 2 的会话归属机制），故发起侧对快照里所有分区的副本集各发一遍（best-effort、去重由节点级幂等 apply 保证）。`replicate_session/2`（按单一分区）仍保留供 Plan 2 精确路由使用并已被 Task 3 测试覆盖。这是 Plan 1 可独立交付且为 Plan 2 铺路的正确边界。

`uninstall_hooks/0`（`:254`）加一行：

```erlang
    _ = persistent_term:erase({broker_subscription_worker, session_replicator}),
```

- [ ] **Step 4: 运行验证通过**

Run: 同 Step 2
Expected: PASS（`no_hook_when_disabled` + `hook_installed_when_enabled`）

- [ ] **Step 5: dialyzer**

Run: `cd apps/broker_cluster && rm -rf _build/test/extras && rebar3 dialyzer`
Expected: 无新增告警

- [ ] **Step 6: 提交**

```bash
git add apps/broker_cluster/src/broker_cluster.erl apps/broker_cluster/src/broker_cluster_session_rep.erl apps/broker_cluster/test/broker_cluster_SUITE.erl
git commit -m "feat(broker_cluster): install_session_hook 由 session_replication env 控制 (MQ-17 Plan 1 Task 5)"
```

---

## Task 6: 多节点 CT — 会话 Op 到达副本本地表

**Files:**
- Modify: `apps/broker_cluster/test/broker_cluster_session_rep_SUITE.erl`（加多节点用例，复用 `?cluster` 三节点范式见 `broker_cluster_cross_node_dist_SUITE`）

**契约（Plan 1 交付判据 / I1+I2）：** 开启 `session_replication` 后，在 owner 节点写入会话 Op，副本节点 `apply_replica` 后本地表含该状态；关闭时副本表不含（零影响）。

- [ ] **Step 1: 写失败测试**

在 `broker_cluster_session_rep_SUITE` 加多节点用例（参照 `broker_cluster_cross_node_dist_SUITE` 的 `?cluster` 设置与 `?NODE1/2/3` 节点引用）：

```erlang
%% owner 写 inflight → 副本本地表出现（开启 session_replication）。
inflight_reaches_replica(_Config) ->
    ?assertMatch({ok, _}, ?cluster:run(fun(Node1, Node2, Node3) ->
        ok = rpc:call(Node1, application, set_env, [broker_cluster, session_replication, true]),
        rpc:call(Node1, broker_cluster, reinstall_hooks, [true]),
        Op = {inflight, <<"c1">>, #{7 => v}},
        ?assertEqual(ok, rpc:call(Node1, broker_cluster_session_rep, remote_apply, [Op])),
        ?assertMatch([{<<"c1">>, #{7 := v}}],
                     ets:lookup(broker_subscription_inflight, <<"c1">>))
        %% 注：副本表可见性取决于 worker 表是 local 还是 replicated；此处用 owner
        %% 直接 remote_apply 验证发起侧→接收侧契约，端到端跨节点可见在 Plan 2 接管 CT。
    end)).

%% 关闭 session_replication：remote_apply 仍可直接调用（契约独立），但自动钩子不复制。
no_auto_replication_when_disabled(_Config) ->
    ?assertMatch({ok, _}, ?cluster:run(fun(Node1, _Node2, _Node3) ->
        ok = rpc:call(Node1, application, set_env, [broker_cluster, session_replication, false]),
        rpc:call(Node1, broker_cluster, reinstall_hooks, [false]),
        ?assertEqual(undefined,
                     persistent_term:get({broker_subscription_worker, session_replicator}, undefined))
    end)).
```

把用例名加进 `all()`。多节点 CT 的运行入口/宏以 `broker_cluster_cross_node_dist_SUITE` 实际定义为准（`?cluster`、`?NODE*` 若名称不同则照搬其范式）。

- [ ] **Step 2: 运行验证失败**

Run: `cd apps/broker_cluster && rm -rf _build/test/extras && rebar3 ct --suite=test/broker_cluster_session_rep_SUITE --case=inflight_reaches_replica`
Expected: 视既有 `?cluster` 宏而定；先确认宏存在并照搬其节点启动方式

- [ ] **Step 3: 写最小实现（多为测试脚手架对齐）**

无需新源码（Task 1–5 已交付管线）。本 Task 仅校准多节点测试对齐既有 `?cluster` 范式；若宏名不同，按 `broker_cluster_cross_node_dist_SUITE` 的实际定义改测试。

- [ ] **Step 4: 运行验证通过**

Run: 同 Step 2
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add apps/broker_cluster/test/broker_cluster_session_rep_SUITE.erl
git commit -m "test(broker_cluster): 多节点 CT 验证会话 Op 复制到副本本地表 (MQ-17 Plan 1 Task 6)"
```

---

## Task 7: 文档收尾（Plan 1 范围）

**Files:**
- Modify: `docs/compatibility/unifiedmq-mqtt-family-gap.md`（MQ-17 行：追加带日期批注，划掉「会话副本」中已属 Plan 1 的部分复制能力，注明「透明接管 + 故障注入证明 = Plan 2 待办」）
- Modify: `docs/mqtt-feature-comparison.md`（§4.2 任务表新增 Plan 1 条目或追加进展注）
- Create: `docs/superpowers/specs/2026-11-22-session-state-replication-plan1-notes.md`（实现记录：默认关闭、Op 协议、Plan 1/2 边界，正文不改既有实测记录）

- [ ] **Step 1: 写实现记录 spec note**

记录：Plan 1 交付了复制管线（metrics 键、`apply_replica/1`、`broker_cluster_session_rep`、worker 写侧挂钩、`session_replication` env 默认关）；透明接管与故障注入 CT 属 Plan 2。既有 ADR-021/034 语义被扩展引用，未推翻。

- [ ] **Step 2: 追加缺口表带日期批注**

在 `unifiedmq-mqtt-family-gap.md` MQ-17 行，把「集群会话副本」追加批注：`~~集群会话副本~~（2026-11-22 Plan 1 已交付会话状态复制管线，默认关闭；透明接管 + 故障注入证明 = Plan 2 待办，见 docs/superpowers/specs/2026-11-22-session-state-replication-design.md）`

- [ ] **Step 3: 提交**

```bash
git add docs/compatibility/unifiedmq-mqtt-family-gap.md docs/mqtt-feature-comparison.md docs/superpowers/specs/2026-11-22-session-state-replication-plan1-notes.md
git commit -m "docs(mq-17): 会话复制 Plan 1 实现记录 + 缺口表带日期批注"
```

---

## Self-Review（写计划后自检）

**1. Spec 覆盖：** Plan 1 覆盖 spec §3.2（发起侧，Task 3/5）、§3.3（接收侧，Task 2）、§3.4（写侧挂钩粒度，Task 4）、§5 指标 + 开关（Task 1、Task 5）、§6 I1/I2 不变量（Task 2/4/5 测试）。**§3.5 持连优先迁移 + §3.6 conn_fsm 重连 + §6 故障注入 CT 不在本 Plan**——已显式声明属 Plan 2 并需先 brainstorm。

**2. 占位符扫描：** Task 4 Step 3 含「用该分支已构造的局部变量名；若原名不同，按实际改」与 Task 5 Step 3 的设计修正块——这是必要的实现指示（worker/cluster 文件大，逐行贴全不现实），非占位符；所有协议、函数签名、测试断言均为具体代码。

**3. 类型一致性：** `session_op()` 协议在「文件结构」定义，Task 2（apply_replica）、Task 3（remote_apply/replicate_session）、Task 4（写侧构造 Op）、Task 5（钩子投递）一致引用。`replicate_session/2,3`、`remote_apply/1`、`apply_replica/1`、`replicate_session_of/1`、`dispatch_async/1` 签名前后一致。

**已知 Plan 1 边界（非缺陷）：** 发起侧尚无「会话 → 分区」精确映射，故 best-effort 对所有分区副本广播（Task 5 Step 3 已注明）；精确路由 + 透明接管是 Plan 2 的核心。
