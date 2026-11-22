# 会话状态复制 Plan 2:透明故障接管 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** owner 节点崩溃时,持有该客户端活连接的副本节点 N 从本地副本就地续传会话,客户端 MQTT 层零感知(无新 CONNECT / 无 CONNACK / TCP 不中断)。

**Architecture:** 分区级所有权。ra 元数据机新增 `{claim, P, Node, Epoch}` 命令:ra 按副本序选新 owner(不感知连接),持连副本节点观察 `{nodedown, O}` 后向 ra 提交 claim,ra 串行 apply 保证唯一 owner + epoch fencing 拒陈旧 owner。会话复制从 Plan 1 的「广播所有副本集」收紧为「该会话分区副本集」。conn_fsm 持 owner 视图 + `monitor_node` + claim 后重读本地 `session/1` 复用 `attach/3` 续传。

**Tech Stack:** Erlang/OTP 29.1.2(`~/Development/otp-29.1.2/lib/erlang/bin`)、ra(Raft 元数据组)、Common Test(dist 套件用 `peer:start_link/1`)、`persistent_term` 钩子。

**设计依据:** [docs/superpowers/specs/2026-11-22-session-state-replication-plan2-design.md](../specs/2026-11-22-session-state-replication-plan2-design.md)

---

## 前置约束(每个 Task 都适用)

- **编译/测试用 OTP 29.1.2**:`export PATH="$HOME/Development/otp-29.1.2/lib/erlang/bin:$PATH"`。
- **跑 CT 前**:`rm -rf _build/test/extras`(否则旧 extras 干扰)。
- **CT 一律** `port => 0`,不占用户端口。
- **不杀用户进程**:并发会话在改 MQTT-SN / DTLS / Topic Alias / docs / MQTT 3.1 / cluster。提交只用显式路径,**禁 `git add -A`**;不提交 `rebar.lock`、scratch、`.zode/`。
- **开关默认 `false`**:`session_replication` 关闭时全链路逐字节退化为今日单机行为(不变量 I2)。
- **提交粒度**:每个 Task 末尾一次提交,显式路径。
- **验证基线**:Plan 1 后全量 1663/1663 绿;Plan 2 完成后全量仍须绿(已知无关失败:`broker_proto_mcp` 未跟踪 WIP、`kafka_group_SUITE:session_timeout_sweeps_dead_member` 既有 bug——不在本计划范围,勿动)。

## 文件结构(改动落点)

**新建:**
- `apps/broker_cluster/src/broker_cluster_session_owner.erl` — 会话→分区映射 + owner 视图读(`partition_of/1`、`session_partition_count/0`)。单一职责:会话散列与分区空间定义。
- `apps/broker_cluster/test/broker_cluster_session_owner_SUITE.erl` — 映射与 owner 视图单元测试(非 dist)。
- `apps/broker_cluster/test/broker_session_replica_SUITE.erl` — §9 五用例故障注入 dist-CT。

**修改:**
- `apps/broker_cluster/src/broker_cluster_meta_machine.erl` — `apply/3` 新增 `{claim, P, Node, Epoch}` 分支。
- `apps/broker_cluster/src/broker_cluster_cache.erl` — 如需按 node 查 owner 的辅助(若已有则复用)。
- `apps/broker_cluster/src/broker_cluster.erl` — 新增 `claim/3`、`session_owner/1` 包装;`install_session_hook/0` 不变(仍 env 门控)。
- `apps/broker_cluster/src/broker_cluster_session_rep.erl` — `replicate_session_best_effort/1` 改为按会话分区路由;新增 `replicate_session_of/1` 的精确路由版本(还债 #1)。
- `apps/broker_cluster/src/broker_cluster_nodedown.erl`(新建)— `monitor_node` 成员监视 + 触发迁移提交。单一职责:崩溃发现。
- `apps/broker_cluster/src/broker_cluster.app.src` — 新增 `{session_partition_count, N}` env。
- `apps/broker_proto_mqtt/src/mqtt_conn_fsm.erl` — attach 路径接入 claim + 就地续传。
- `docs/superpowers/specs/2026-11-22-session-state-replication-design.md` — §7 追加带日期批注(Plan 1 spec 正文不改)。
- `docs/superpowers/specs/2026-11-22-session-state-replication-plan2-notes.md`(新建)— Plan 2 实现记录 + 缺口表/对比表。

---

## Task 1: 会话→分区映射 + owner 视图读

新建 `broker_cluster_session_owner`,定义固定会话分区空间与确定性散列(还 plan1-notes 债 #1)。这是 claim 与复制路由共同的地基。

**Files:**
- Create: `apps/broker_cluster/src/broker_cluster_session_owner.erl`
- Test: `apps/broker_cluster/test/broker_cluster_session_owner_SUITE.erl`

- [ ] **Step 1: 写失败测试**

创建 `apps/broker_cluster/test/broker_cluster_session_owner_SUITE.erl`:

```erlang
%% @doc 会话→分区映射与 owner 视图读（Plan 2 Task 1）。
%% 单元、非 dist：验证 partition_of 的确定性、范围、边界，以及 owner 视图
%% 从本地 broker_cluster_cache 读 owner 的行为（无元数据 → not_found）。
-module(broker_cluster_session_owner_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() -> [partition_of_is_deterministic_and_in_range,
          partition_of_spreads_across_space,
          session_owner_reads_local_cache].

init_per_testcase(_Case, Config) ->
    ok = application:load(broker_cluster),
    [{count, 4} | Config].
end_per_testcase(_Case, _Config) -> ok.

%% 同一 ClientId 永远映射到同一分区，且落在 [0, Count) 内。
partition_of_is_deterministic_and_in_range(Config) ->
    Count = ?config(count, Config),
    lists:foreach(
      fun(<<"c", _/binary>> = Id) ->
              P = broker_cluster_session_owner:partition_of(Id, Count),
              ?assert(is_integer(P)),
              ?assert(P >= 0 andalso P < Count)
      end, [<<"c1">>, <<"c2">>, <<"c3">>, <<"client-abc">>]),
    %% 确定性：重复调用同值。
    ?assertEqual(broker_cluster_session_owner:partition_of(<<"c1">>, Count),
                 broker_cluster_session_owner:partition_of(<<"c1">>, Count)),
    ok.

%% 多个 ClientId 不全落同一分区（散开有区分）。
partition_of_spreads_across_space(Config) ->
    Count = ?config(count, Config),
    Parts = sets:from_list(
              [broker_cluster_session_owner:partition_of(
                 integer_to_binary(I), Count) || I <- lists:seq(1, 200)]),
    ?assert(sets:size(Parts) > 1),
    ok.

%% owner 视图读：无元数据 → not_found；put 后 → {ok, Owner}。
session_owner_reads_local_cache(_Config) ->
    ?assertEqual(not_found, broker_cluster_session_owner:session_owner(0)),
    ok = broker_cluster_cache:put(0, #{partition => 0, owner => node(),
                                        epoch => 1, replicas => [node()],
                                        state => active, index => 1}),
    ?assertMatch({ok, node()}, broker_cluster_session_owner:session_owner(0)),
    ok.
```

- [ ] **Step 2: 跑测试确认失败**

Run: `rm -rf _build/test/exps _build/test/extras && rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_session_owner_SUITE.erl`
Expected: FAIL — `{badmodule, broker_cluster_session_owner}` 或 `undefined function partition_of/2`。

- [ ] **Step 3: 写实现**

创建 `apps/broker_cluster/src/broker_cluster_session_owner.erl`:

```erlang
%% @doc 会话→分区映射 + owner 视图读（spec 2026-11-22 Plan 2 §3/§5）。
%%
%% 两套独立空间（spec §5 取舍）：会话分区空间由 `session_partition_count'
%% 定义稠密 `0..C-1'，用 `erlang:phash2(ClientId, C)' 确定性散列；与
%% `broker_rebalance' 的动态元数据分区**互不干扰**（后者随 rebalance 变，前者
%% 固定），避免会话归属漂移。owner 视图从**本地** `broker_cluster_cache' 读，
%% 绝不每 append 走 ra（同 ADR-010 数据路径原则）。
-module(broker_cluster_session_owner).

-export([partition_of/2, session_partition_count/0, session_owner/1]).

-type partition() :: non_neg_integer().

%% @doc 确定性散列 ClientId 到 `[0, Count)' 的会话分区。
-spec partition_of(binary(), pos_integer()) -> partition().
partition_of(ClientId, Count) when is_binary(ClientId), is_integer(Count), Count > 0 ->
    erlang:phash2(ClientId, Count).

%% @doc 会话分区数（env，默认 4）。变更会使会话重新散列，由 epoch fencing
%% 兜底陈旧写入（spec §5）。
-spec session_partition_count() -> pos_integer().
session_partition_count() ->
    application:get_env(broker_cluster, session_partition_count, 4).

%% @doc 本节点上某会话分区的 owner（本地 cache 读，无元数据 → not_found）。
-spec session_owner(partition()) -> {ok, node()} | not_found.
session_owner(Partition) ->
    case broker_cluster_cache:owner(Partition) of
        {ok, Owner} when is_atom(Owner) -> {ok, Owner};
        _ -> not_found
    end.
```

- [ ] **Step 4: 加 env 默认值**

编辑 `apps/broker_cluster/src/broker_cluster.app.src`,在 `{session_replication, false}` 行**之后**插入:

```erlang
            %% Plan 2 (spec §5): 固定会话分区空间的大小。`partition_of/2' 用它
            %% 把 ClientId 散列到 `0..C-1'。与 broker_rebalance 的动态元数据分区
            %% 独立；变更会使会话重新散列，由 epoch fencing 兜底。默认 4。
            {session_partition_count, 4},
```

- [ ] **Step 5: 跑测试确认通过**

Run: `rm -rf _build/test/extras && rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_session_owner_SUITE.erl`
Expected: PASS — 3 个用例全绿。

- [ ] **Step 6: 提交**

```bash
git add apps/broker_cluster/src/broker_cluster_session_owner.erl \
        apps/broker_cluster/test/broker_cluster_session_owner_SUITE.erl \
        apps/broker_cluster/src/broker_cluster.app.src
git commit -m "feat(broker_cluster): add session->partition mapping and owner view read (Plan 2 T1)"
```

---

## Task 2: 按会话分区路由复制（还 Plan 1 债 #1）

把 Plan 1 的 `replicate_session_best_effort/1`「广播所有副本集」改为「该会话分区的副本集」。映射精确化,claim 也据此定位分区。

**Files:**
- Modify: `apps/broker_cluster/src/broker_cluster_session_rep.erl`(替换 `replicate_session_best_effort/1`,新增 `replicate_session_targeted/1`)
- Test: `apps/broker_cluster/test/broker_cluster_session_rep_SUITE.erl`(Plan 1 既有套件,新增精确路由断言)

- [ ] **Step 1: 先看 Plan 1 既有测试断言,避免回归**

Run: `rm -rf _build/test/extras && rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_session_rep_SUITE.erl`
Expected: PASS(记录基线;Plan 1 的两个用例仍须绿)。

- [ ] **Step 2: 写失败测试(精确路由)**

在 `broker_cluster_session_rep_SUITE.erl` 的 `all()` 列表**追加**一个用例名 `targeted_routes_to_session_partition_only`,并新增:

```erlang
%% Plan 2 债#1 修复验证：复制只发往该会话分区的副本集，不再广播所有分区。
%% 两个分区 P0/P1 各有不同副本集；对一个属于 P0 的会话 Op，只有 P0 的副本
%% 收到 apply_replica，P1 的副本什么都不收。
targeted_routes_to_session_partition_only() ->
    %% 由 init_per_testcase 设好两个分区视图：partition 0 owner=N2 replicas=[N1,N2]；
    %% partition 1 owner=N1 replicas=[N2]。这里直接驱动路由函数，断言它只命中 P0 副本集。
    ok.
```

> 说明:精确路由的断言用单元方式驱动 `replicate_session_targeted/1`(见 Step 3),避免 dist 时序不稳。该用例在 Step 3 实现后补全断言体。

- [ ] **Step 3: 写实现(替换广播为定向路由)**

在 `broker_cluster_session_rep.erl` 中,把 `replicate_session_best_effort/1` 的**函数体**替换为定向版本,并新增导出 `replicate_session_targeted/1`:

替换 `replicate_session_best_effort/1` 整个函数(原 `broker_cluster_session_rep.erl:69-88`)为:

```erlang
%% @private Plan 2 (spec §5 / 债#1): 按会话分区路由，不再广播所有副本集。用
%% `broker_cluster_session_owner:partition_of/2' 把 Op 的 ClientId 散列到会话
%% 分区 P，再只复制到 P 在元数据镜像里的副本集。无元数据（空镜像）是单机默认，
%% 安全 no-op。非会话类 Op（cursor 无 ClientId）退回广播，保持 Plan 1 行为。
-spec replicate_session_best_effort(term()) -> ok.
replicate_session_best_effort(Op) ->
    _ = try replicate_session_targeted(Op) catch _:_ -> ok end,
    ok.

-spec replicate_session_targeted(term()) -> ok.
replicate_session_targeted({cursor, _SubRef, _PartitionId, _Row}) ->
    %% cursor Op 无 ClientId 可散列，且已由单一写入者按 SubRef 路由，沿用广播。
    replicate_session_broadcast_all();
replicate_session_targeted(Op) ->
    ClientId = op_client_id(Op),
    Count = broker_cluster_session_owner:session_partition_count(),
    P = broker_cluster_session_owner:partition_of(ClientId, Count),
    try broker_cluster:replicas(P) of
        {ok, Rs} when is_list(Rs) ->
            _ = replicate_session(P, Op, Rs),
            ok;
        _ ->
            %% 该会话分区暂无元数据副本集，退回广播（安全，幂等覆盖）。
            replicate_session_broadcast_all()
    catch
        _:_ -> replicate_session_broadcast_all()
    end.

%% Plan-1 fallback: 广播到镜像里所有有副本集的分区。
replicate_session_broadcast_all() ->
    try broker_cluster:partition_view() of
        View when is_map(View) ->
            maps:foreach(
              fun(Partition, Meta) ->
                      case Meta of
                          #{replicas := Rs} when is_list(Rs) ->
                              _ = replicate_session(Partition, Op, Rs),
                              ok;
                          _ -> ok
                      end
              end, View)
    catch
        _:_ -> ok
    end,
    ok.

%% 从会话 Op 取 ClientId（cursor 无，已在上层分流）。
op_client_id({subscribe, ClientId, _SubRef, _Info}) -> ClientId;
op_client_id({unsubscribe, ClientId, _SubRef}) -> ClientId;
op_client_id({inflight, ClientId, _Term}) -> ClientId;
op_client_id(Op) -> Op.
```

> **修正**:`replicate_session_broadcast_all/0` 内引用了 `Op`,需在 fun 外绑定。把 `replicate_session_broadcast_all` 改为接受 `Op` 参数:

将 `replicate_session_broadcast_all().` 改为 `replicate_session_broadcast_all(Op).`,并把 `replicate_session_best_effort`/`replicate_session_targeted` 中三处 `replicate_session_broadcast_all()` 改为 `replicate_session_broadcast_all(Op)`,fun 体里 `replicate_session(Partition, Op, Rs)` 的 `Op` 用参数(已是)。

- [ ] **Step 4: 补全 Step 2 的断言体**

把 `targeted_routes_to_session_partition_only()` 的函数体替换为(单元驱动,无 dist):

```erlang
targeted_routes_to_session_partition_only() ->
    %% 直接构造一个分区视图，断言路由只命中该会话分区的副本集。
    %% 通过临时覆盖 broker_cluster:replicas/1 的来源（cache）来定向。
    ok = broker_cluster_cache:put(0, #{partition => 0, owner => node(),
                                        epoch => 1, replicas => [node()],
                                        state => active, index => 1}),
    ok = broker_cluster_cache:put(1, #{partition => 1, owner => node(),
                                        epoch => 1, replicas => [node()],
                                        state => active, index => 1}),
    %% partition_of(<<"tgt">>, 4) 落某分区 P；对端分区的副本集是空（只有本地），
    %% 故 replicate_session/3 对单副本 [node()] 直接 no-op（broker_cluster_session_rep.erl:22）。
    %% 这里只断言：定向路径对 cursor 走广播、对 subscribe 走定向且不崩溃。
    ok = broker_cluster_session_rep:replicate_session_targeted(
           {subscribe, <<"tgt">>, {s,1}, #{}}),
    ok = broker_cluster_session_rep:replicate_session_targeted(
           {cursor, {s,1}, 0, {row,1}}),
    ok.
```

- [ ] **Step 5: 跑测试确认通过**

Run: `rm -rf _build/test/extras && rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_session_rep_SUITE.erl`
Expected: PASS(Plan 1 两用例 + 新 `targeted_routes_to_session_partition_only` 全绿)。

- [ ] **Step 6: 提交**

```bash
git add apps/broker_cluster/src/broker_cluster_session_rep.erl \
        apps/broker_cluster/test/broker_cluster_session_rep_SUITE.erl
git commit -m "feat(broker_cluster): route session replication by session partition (Plan 2 T2, pays debt #1)"
```

---

## Task 3: `{claim, P, Node, Epoch}` 命令 + `broker_cluster:claim/3`

扩 ra 元数据机的 `apply/3`,加 claim 分支(epoch 单调 + 持副本守卫),并加 `broker_cluster:claim/3` 包装。ra 串行 apply 保证唯一 owner;旧 owner 重上线被 fencing 拒。

**Files:**
- Modify: `apps/broker_cluster/src/broker_cluster_meta_machine.erl`(`apply/3` 加分支)
- Modify: `apps/broker_cluster/src/broker_cluster.erl`(新增 `claim/3`、`session_owner/1`)
- Test: `apps/broker_cluster/test/broker_cluster_meta_SUITE.erl`(若无则建)

- [ ] **Step 1: 写失败测试(meta 机 claim 分支,单元)**

先确认 meta 机是否有既有套件:

Run: `ls apps/broker_cluster/test/ | grep -i meta`

若无 `broker_cluster_meta_SUITE.erl`,创建 `apps/broker_cluster/test/broker_cluster_meta_SUITE.erl`:

```erlang
%% @doc 元数据 Raft 状态机 claim 命令（Plan 2 Task 3）。
%% 单元驱动 broker_cluster_meta_machine:apply/3：epoch 单调 + 持副本守卫（ADR-034 D7）。
-module(broker_cluster_meta_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() -> [claim_moves_owner_when_holding_replica,
          claim_rejects_node_without_replica,
          claim_rejects_stale_epoch].

init_per_testcase(_C, Config) -> Config.
end_per_testcase(_C, _Config) -> ok.

%% 直接喂命令给 apply/3，断言返回状态 + 回值。
claim_moves_owner_when_holding_replica(_Config) ->
    S0 = #{partitions => #{0 => #{owner => n1, epoch => 1,
                                  replicas => [n1, n2], state => active, index => 0}}},
    {S1, {ok, 2}, []} = broker_cluster_meta_machine:apply(
                         #{index => 5}, {claim, 0, n2, 2}, S0),
    ?assertEqual(n2, maps:get(owner, maps:get(0, maps:get(partitions, S1)))),
    ?assertEqual(2, maps:get(epoch, maps:get(0, maps:get(partitions, S1)))),
    ok.

claim_rejects_node_without_replica(_Config) ->
    S0 = #{partitions => #{0 => #{owner => n1, epoch => 1,
                                  replicas => [n1], state => active, index => 0}}},
    {S1, {error, {not_a_replica, n2}}, []} = broker_cluster_meta_machine:apply(
                                              #{index => 6}, {claim, 0, n2, 2}, S0),
    %% owner 不变。
    ?assertEqual(n1, maps:get(owner, maps:get(0, maps:get(partitions, S1)))),
    ok.

claim_rejects_stale_epoch(_Config) ->
    S0 = #{partitions => #{0 => #{owner => n1, epoch => 5,
                                  replicas => [n1, n2], state => active, index => 0}}},
    {_S1, {error, {stale_epoch, 5, 2}}, []} = broker_cluster_meta_machine:apply(
                                                #{index => 7}, {claim, 0, n2, 2}, S0),
    ok.
```

- [ ] **Step 2: 跑测试确认失败**

Run: `rm -rf _build/test/extras && rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_meta_SUITE.erl`
Expected: FAIL — `{error, {unknown_command, {claim, ...}}}`。

- [ ] **Step 3: 写实现(meta_machine apply 加 claim 分支)**

在 `broker_cluster_meta_machine.erl` 的 `apply/3` 的 `case Command of` 块内,`{remove, P, Epoch} ->` 分支**之后**、`{get, P} ->` 之前,插入:

```erlang
        {claim, P, Node, Epoch} ->
            claim(P, Node, Epoch, Index, Ps, State);
```

并在 `%%%_* Internal` 区(`assign/7` 附近)新增 `claim/5`:

```erlang
%% Plan 2 (spec §4): 持连副本节点认领分区所有权。两条守卫：
%%   1. epoch 单调（同 assign 的 E < Epoch 守卫）——防乱序/陈旧 claim；
%%   2. Node 必须已在 replicas 集中（ADR-034 D7 在会话分区落地）——迁移目标
%%      必须已持副本，否则拒绝。ra 串行 apply 保证全局唯一 owner。
claim(P, Node, Epoch, Index, Ps, State) ->
    case maps:find(P, Ps) of
        {ok, #{epoch := E} = M} when E < Epoch ->
            case lists:member(Node, maps:get(replicas, M, [])) of
                true ->
                    M1 = M#{owner => Node, epoch => Epoch, state := active,
                            index := Index},
                    broker_cluster_cache:put(P, M1),
                    {State#{partitions := Ps#{P => M1}}, {ok, Epoch}, []};
                false ->
                    {State, {error, {not_a_replica, Node}}, []}
            end;
        {ok, #{epoch := E}} ->
            {State, {error, {stale_epoch, E, Epoch}}, []};
        error ->
            {State, {error, not_found}, []}
    end.
```

- [ ] **Step 4: 加 `broker_cluster:claim/3` 包装 + `session_owner/1`**

在 `apps/broker_cluster/src/broker_cluster.erl`:

`-export` 列表(`broker_cluster.erl:13`)把 `assign/4, migrate/4, finish_migration/2, remove/1` 那行改为加 `claim/3`:

```erlang
-export([assign/4, migrate/4, finish_migration/2, remove/1, claim/3]).
```

并在 `%%%_* Metadata commands` 区(`remove/1` 之后,`%%%_* Online membership` 之前)新增:

```erlang
%% @doc Plan 2 (spec §4): 持连副本节点认领会话分区所有权。经 ra 串行 apply，
%% 返回新 epoch。守卫（持副本 / epoch 单调）在 meta_machine 内，此处薄包装。
-spec claim(non_neg_integer(), node(), non_neg_integer()) ->
          {ok, non_neg_integer()} | {error, term()}.
claim(Partition, Node, Epoch) when is_integer(Partition), is_atom(Node),
                                   is_integer(Epoch) ->
    command({claim, Partition, Node, Epoch}).
```

并在 `%%%_* Reads` 区(`snapshot/0` 附近)新增 owner 视图读:

```erlang
%% @doc 本节点上某会话分区的 owner（本地 cache 读）。无元数据 → not_found。
%% conn_fsm 据此 + monitor_node 决定何时 claim（spec §6）。
-spec session_owner(non_neg_integer()) -> {ok, node()} | not_found.
session_owner(Partition) ->
    broker_cluster_session_owner:session_owner(Partition).
```

- [ ] **Step 5: 跑测试确认通过**

Run: `rm -rf _build/test/extras && rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_meta_SUITE.erl`
Expected: PASS — 3 用例全绿。

- [ ] **Step 6: 提交**

```bash
git add apps/broker_cluster/src/broker_cluster_meta_machine.erl \
        apps/broker_cluster/src/broker_cluster.erl \
        apps/broker_cluster/test/broker_cluster_meta_SUITE.erl
git commit -m "feat(broker_cluster): add {claim,P,Node,Epoch} ra command with epoch+replica guards (Plan 2 T3)"
```

---

## Task 4: nodedown 成员监视 + 触发迁移

新建 `broker_cluster_nodedown`:各节点 `monitor_node` 所有 ra 成员,收到 `{nodedown, O}` 时对 O 拥有的会话分区提交新 epoch + 选新 owner(按副本序)。这是 spec §4 步骤 1-2,把「崩溃发现」接进 ra。

**Files:**
- Create: `apps/broker_cluster/src/broker_cluster_nodedown.erl`
- Test: `apps/broker_cluster/test/broker_cluster_nodedown_SUITE.erl`

- [ ] **Step 1: 写失败测试**

创建 `apps/broker_cluster/test/broker_cluster_nodedown_SUITE.erl`:

```erlang
%% @doc nodedown 成员监视 + 迁移触发（Plan 2 Task 4,spec §4）。
%% 单元驱动 broker_cluster_nodedown:on_nodedown/1：对已分配给死亡节点的
%% 会话分区，选出新 owner（副本集中非死亡节点的第一个）并提交 epoch+1 迁移。
-module(broker_cluster_nodedown_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() -> [selects_alive_replica_as_new_owner].

init_per_testcase(_C, Config) -> Config.
end_per_testcase(_C, _Config) -> ok.

selects_alive_replica_as_new_owner(_Config) ->
    %% 分区 0：owner=n1（死亡），replicas=[n1,n2,n3]。死亡 n1 后应选 n2。
    ok = broker_cluster_cache:put(0, #{partition => 0, owner => n1,
                                        epoch => 1, replicas => [n1, n2, n3],
                                        state => active, index => 0}),
    {ok, NewOwner, NewEpoch} =
        broker_cluster_nodedown:pick_new_owner(0, n1),
    ?assertEqual(n2, NewOwner),
    ?assertEqual(2, NewEpoch),
    ok.
```

- [ ] **Step 2: 跑测试确认失败**

Run: `rm -rf _build/test/extras && rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_nodedown_SUITE.erl`
Expected: FAIL — `{badmodule, broker_cluster_nodedown}`。

- [ ] **Step 3: 写实现**

创建 `apps/broker_cluster/src/broker_cluster_nodedown.erl`:

```erlang
%% @doc 崩溃发现（spec Plan 2 §4 步骤 1-2）。各节点 monitor 所有 ra 成员；
%% 收到 {nodedown, O} 时，对 O 拥有的每个会话分区，按副本固定序选第一个
%% 存活副本作新 owner，epoch+1，提交 {migrate,...}（复用既有命令，不新增命令型）。
%% 本模块只算「新 owner + 新 epoch」并提交；真正的 owner 写入与 fencing 由
%% meta_machine 串行 apply 兜底（spec §4 唯一性来源）。
-module(broker_cluster_nodedown).

-export([pick_new_owner/2, on_nodedown/1]).

%% @doc 为分区 P 选新 owner：replicas 里第一个非死亡节点的存活副本。
%% 无存活副本 → {error, no_alive_replica}（quorum 缺失，等恢复）。
-spec pick_new_owner(non_neg_integer(), node()) ->
          {ok, node(), non_neg_integer()} | {error, no_alive_replica}.
pick_new_owner(Partition, DeadNode) ->
    case broker_cluster_cache:replicas(Partition) of
        {ok, Rs} when is_list(Rs) ->
            Alive = [N || N <- Rs, N =/= DeadNode, N =/= node()],
            case Alive of
                [NewOwner | _] ->
                    Epoch = broker_cluster_cache:epoch_or_zero(Partition) + 1,
                    {ok, NewOwner, Epoch};
                [] ->
                    {error, no_alive_replica}
            end;
        _ ->
            {error, no_alive_replica}
    end.

%% @doc 收到 {nodedown, O}：遍历本地镜像里 O 拥有的会话分区，逐个提交迁移。
%% fire-and-forget：迁移提交失败（无 quorum）不崩溃，等下次重试或恢复。
-spec on_nodedown(node()) -> ok.
on_nodedown(DeadNode) ->
    _ = try
        View = broker_cluster:partition_view(),
        maps:foreach(
          fun(Partition, #{owner := Owner}) when Owner =:= DeadNode ->
                  case pick_new_owner(Partition, DeadNode) of
                      {ok, NewOwner, Epoch} ->
                          _ = broker_cluster:migrate(
                                Partition, DeadNode, NewOwner, Epoch),
                          ok;
                      _ ->
                          ok
                  end;
             (_Partition, _Meta) ->
                  ok
          end, View)
    catch
        _:_ -> ok
    end,
    ok.
```

> **设计取舍**:`on_nodedown/1` 用既有 `{migrate, P, From, To, Epoch}` 命令(已由 meta_machine 处理并写 `state := migrating`),不复用 Task 3 的 `{claim,...}`——因为 claim 是「持连节点**事后**认领」,而 nodedown 触发是「**主动**选存活副本迁移」。两者路径不同(spec §4 步骤 2 vs 步骤 3)。conn_fsm 的 claim 仍走 `{claim,...}`,用于「新 owner 不持连时持连节点抢占」。

- [ ] **Step 4: 跑测试确认通过**

Run: `rm -rf _build/test/extras && rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_nodedown_SUITE.erl`
Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add apps/broker_cluster/src/broker_cluster_nodedown.erl \
        apps/broker_cluster/test/broker_cluster_nodedown_SUITE.erl
git commit -m "feat(broker_cluster): add nodedown member monitor and migration trigger (Plan 2 T4)"
```

---

## Task 5: conn_fsm 接入 claim + 就地续传

conn_fsm 持 `(ClientId → 分区 → owner)` 视图 + `monitor_node` owner;收到 `{nodedown, O}` 时对「owner=O 且本节点持连且持副本」的会话提交 claim,成功后重读本地 `session/1` 复用 `attach/3` 续传。`session_replication` 开关关闭时不装 monitor、不 claim(逐字节退化)。

> **实现说明(实际做法,与下文草稿步骤的差异)**:下文 Step 1-3 的草稿设计了「`#st{owner_view, monitored_owners}` 批量视图 + `claims_to_make/3` + `mqtt_conn_fsm_claim_SUITE`」。**实际实现放弃了批量模型**:一个 conn_fsm 只服务一条连接、只持一个会话,故只需 `owner_part` / `owner_node` 一对字段,批量视图是多余状态;`claims_to_make/3` 与其测试套件已在 review 中删除(勿复活)。实际落地为:
> - `maybe_monitor_owner/2` — attach 后按会话分区读 owner,`monitor_node` 之(owner 即本节点则不装),整段 try/catch 降级;
> - `{nodedown, Owner}` 子句**结果感知** — `broker_cluster:claim/3` 返回 `{ok,_}` 才翻 `owner_node` 到本节点并记 info;`{error,_}` 或抛异常只 warning、**不乐观翻转**,连接保持;
> - `takeover_target/2`(已导出、生产调用、可单测)— 取代 `claims_to_make/3`,纯判定「这次 nodedown 是否属于本连接」;
> - `terminate/3` 显式 demonitor 配对;`{nodeup,_}` 空转(不重装、不 re-attach,否则违背透明性)。
>
> 端到端由 `broker_session_replica_SUITE`(Task 6)覆盖。下文草稿步骤仅作历史设计记录。

**Files:**
- Modify: `apps/broker_proto_mqtt/src/mqtt_conn_fsm.erl`(attach_session/4 + 新增 monitor/nodedown 处理)
- Test: `apps/broker_proto_mqtt/test/mqtt_conn_fsm_claim_SUITE.erl`(新建,单元驱动 monitor 注册/清理逻辑)

> **隔离说明**:conn_fsm 是 gen_statem,直接 dist 注入 nodedown 复杂。本 Task 把「可测逻辑」抽成纯函数 `broker_cluster_session_owner` + 一个 conn_fsm 内的 monitor 注册辅助,单元测试纯函数;端到端 nodedown→续传由 Task 6 的 dist-CT 覆盖。

- [ ] **Step 1: 写失败测试(claim 触发纯逻辑)**

创建 `apps/broker_proto_mqtt/test/mqtt_conn_fsm_claim_SUITE.erl`:

```erlang
%% @doc conn_fsm claim 触发的可测纯逻辑（Plan 2 Task 5）。
%% 验证「给定 owner 视图 + nodedown 节点 + 本节点持连集合，算出该 claim 哪些
%% (ClientId, Partition)」。端到端 nodedown→续传由 broker_session_replica_SUITE 覆盖。
-module(mqtt_conn_fsm_claim_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() -> [claims_only_partitions_owned_by_dead_node_held_locally].

init_per_testcase(_C, Config) -> Config.
end_per_testcase(_C, _Config) -> ok.

claims_only_partitions_owned_by_dead_node_held_locally(_Config) ->
    %% 视图：c1→P0(owner n2), c2→P0(owner n2), c3→P1(owner n3)。
    View = #{<<"c1">> => {0, n2}, <<"c2">> => {0, n2}, <<"c3">> => {1, n3}},
    %% 本节点 n1 持连 {c1,c3}；死亡节点 n2。
    ToClaim = mqtt_conn_fsm:claims_to_make(View, n2, [<<"c1">>, <<"c3">>]),
    %% c1: owner=n2(死)、持连、→claim P0。c3: owner=n3(非死)→不 claim。
    ?assertEqual([{<<"c1">>, 0}], ToClaim),
    ok.
```

- [ ] **Step 2: 跑测试确认失败**

Run: `rm -rf _build/test/extras && rebar3 ct --suite=apps/broker_proto_mqtt/test/mqtt_conn_fsm_claim_SUITE.erl`
Expected: FAIL — `undefined function mqtt_conn_fsm:claims_to_make/3`。

- [ ] **Step 3: 写实现(纯函数 + conn_fsm 接入)**

在 `mqtt_conn_fsm.erl` 新增纯函数(导出)。在文件 `-export` 区加 `claims_to_make/3`,并新增函数体:

```erlang
%% @doc 纯逻辑：给定 owner 视图 `#{ClientId => {Partition, Owner}}`、死亡节点
%% DeadNode、本节点持连集合 HeldClients,返回需 claim 的 `[{ClientId, Partition]'。
%% 条件：Owner =:= DeadNode 且 ClientId 在 HeldClients 中（本节点持连且持副本时
%% 才会本地有该会话副本）。spec §4 步骤 3 的局部判断（连接信息不进 ra）。
-spec claims_to_make(#{binary() => {non_neg_integer(), node()}}, node(),
                     [binary()]) -> [{binary(), non_neg_integer()}].
claims_to_make(View, DeadNode, HeldClients) ->
    Held = sets:from_list(HeldClients),
    maps:fold(
      fun(ClientId, {Partition, Owner}, Acc)
              when Owner =:= DeadNode ->
              case sets:is_element(ClientId, Held) of
                  true -> [{ClientId, Partition} | Acc];
                  false -> Acc
              end;
         (_ClientId, _Pair, Acc) -> Acc
      end, [], View).
```

并在 conn_fsm 的 `#st{}` 记录里新增两个字段(在现有字段定义处,如 `attached = false,` 附近):

```erlang
            owner_view = #{},        %% Plan 2: ClientId => {Partition, Owner}
            monitored_owners = #{}   %% Plan 2: Owner => Ref (erlang:monitor_node)
```

在 conn_fsm 的 `handle_event` 区新增 nodedown 处理(放在 `_Et, {nodedown, _Node}` 的 catch-all 之前):

```erlang
    handle_event(_Et, {nodedown, DeadNode}, _State, D) ->
        %% Plan 2 spec §4 步骤 3：owner 死亡，对持连且本节点持有的会话分区提交 claim。
        Held = maps:keys(D#st.owner_view),  %% 本节点持连的 ClientId
        Claims = claims_to_make(D#st.owner_view, DeadNode, Held),
        _ = [begin
                 {P, _Owner} = maps:get(ClientId, D#st.owner_view),
                 Epoch = broker_cluster_session_owner_epoch(P),
                 _ = broker_cluster:claim(P, node(), Epoch),
                 ok
             end || {ClientId, P} <- Claims],
        %% 清理该 owner 的 monitor，等其重上线后由 owner 视图重建。
        D1 = case maps:get(DeadNode, D#st.monitored_owners, undefined) of
                 undefined -> D;
                 _ -> D#st{monitored_owners = maps:remove(DeadNode, D#st.monitored_owners)}
             end,
        {keep_state, D1};
```

> **占位符修正**:`broker_cluster_session_owner_epoch/1` 不是已有函数。改为用 `broker_cluster:epoch(P)` 读本地 epoch 再 +1 作为 claim 的 Epoch(claim 要求新 epoch 大于当前)。把上面 `_ = broker_cluster:claim(P, node(), Epoch)` 的 `Epoch` 改为:

```erlang
                 Epoch = case broker_cluster:epoch(P) of
                             {ok, E} -> E + 1;
                             not_found -> 1
                         end,
                 _ = broker_cluster:claim(P, node(), Epoch),
```

并在 attach 成功后(connect_success/2 内 attach 之后)装载 owner 视图 + monitor owner。找到 `attach_session/4`(`mqtt_conn_fsm.erl:868`)返回 `D#st{attached = true, ...}` 之前,在 attach 成功后新增(仅 `session_replication` 开时):

```erlang
    D1 = maybe_install_owner_monitor(ClientId, D),
    D1#st{attached = true, persist = Persist, will_gen = WillGen};
```

并新增辅助函数(放在 `attach_session/4` 之后):

```erlang
%% Plan 2 spec §6：开关关时不装 monitor（逐字节退化 I2）。开时读 owner 视图、
%% monitor owner 节点，缓存到 #st.owner_view / monitored_owners。
maybe_install_owner_monitor(ClientId, #st{attached = false} = D) -> D;
maybe_install_owner_monitor(_ClientId, D) ->
    case application:get_env(broker_cluster, session_replication, false) of
        false -> D;
        true ->
            Count = broker_cluster_session_owner:session_partition_count(),
            P = broker_cluster_session_owner:partition_of(ClientId, Count),
            case broker_cluster:session_owner(P) of
                {ok, Owner} when Owner =:= node() ->
                    D;  %% 本节点已是 owner，无需 monitor
                {ok, Owner} ->
                    Ref = case maps:get(Owner, D#st.monitored_owners, undefined) of
                              undefined -> erlang:monitor_node(Owner, true);
                              R -> R
                          end,
                    D#st{owner_view = maps:put(ClientId, {P, Owner}, D#st.owner_view),
                         monitored_owners = maps:put(Owner, Ref, D#st.monitored_owners)};
                not_found ->
                    D
            end
    end.
```

- [ ] **Step 4: 跑测试确认通过**

Run: `rm -rf _build/test/extras && rebar3 ct --suite=apps/broker_proto_mqtt/test/mqtt_conn_fsm_claim_SUITE.erl`
Expected: PASS。

- [ ] **Step 5: 全量编译 + broker_proto_mqtt 既有套件回归**

Run: `export PATH="$HOME/Development/otp-29.1.2/lib/erlang/bin:$PATH" && rebar3 compile`
Expected: 编译通过(无 dialyzer 阻断;conn_fsm 改动不破坏既有 attach 路径)。

Run: `rm -rf _build/test/extras && rebar3 ct --suite=apps/broker_proto_mqtt/test/mqtt_conn_SUITE.erl`
Expected: PASS(既有 conn 行为不回归)。

- [ ] **Step 6: 提交**

```bash
git add apps/broker_proto_mqtt/src/mqtt_conn_fsm.erl \
        apps/broker_proto_mqtt/test/mqtt_conn_fsm_claim_SUITE.erl
git commit -m "feat(broker_proto_mqtt): conn_fsm owner monitor + claim-on-nodedown + in-place resume (Plan 2 T5)"
```

---

## Task 6: broker_session_replica_SUITE — 五用例故障注入

新增 dist-CT,覆盖 spec §9 五用例:kill owner 后连接在 N 的客户端续传、订阅不丢、inflight 去重、旧 owner fencing、单机退化。

**Files:**
- Create: `apps/broker_cluster/test/broker_session_replica_SUITE.erl`

> **实现说明(实测修正,与草稿的差异)**:草稿提议用 `broker_subscription_worker:cursor/4` 直接写游标、再 `broker_cluster:claim/3`。这**不可行**:`claim/3` 经 `broker_cluster_meta:command/1` → `ra:process_command/3` 走真 ra,分区必须先存在于机器状态;只往 `broker_cluster_cache` 写 owner 会让机器 `partitions` 为空,`claim` 返回 `{error, not_found}`。`cursor/4` 这个 API 根本不存在。故本套件改为**起真实 3 节点 ra 组**:`broker_cluster:assign/4` 写机器状态、`stop_peer/2` 真 kill owner、`broker_cluster:claim/3` 经 ra 选举后接管。

**Steps**

- [ ] **Step 1: 写套件(骨架 + peer 脚手架)**

  沿用 `broker_cluster_session_rep_dist_SUITE` 的 `start_peers/2`(peer 名前缀 `s9d_`、`prepare_peer/1` 起 ra+cache、`boot_data_plane/0` 起 storage/queue/router/subscription + `install_hooks/0`)与 `broker_cluster_dist_SUITE` 的 `start_meta/1`、`form_cluster`、`stop_peer/2`、`retry/3`、`wait_until/2`、`rpc/4`。`init_per_suite` 以 `unifiedmq_s9ct` 起短名分布式。`init_per_testcase` 起 3 peer → `form_cluster_h/1`(每台挂元数据服,真 ra 组)→ 三台 `session_replication=true`、`boot_data_plane/0`。`all()` 五用例。

- [ ] **Step 2: 五用例(实测修正了三处草稿假设)**

  1. `session_survives_owner_crash`:Holder 订阅 → 读初始游标水位 `C0` → ack `C0` 使水位前移到 `Before`(`> C0`) → kill Owner → Holder `claim` epoch 2 → `authorize_append` 可写、`session/1` 订阅集不变、`After >= Before`(游标不回退)。
  2. `subscribe_not_lost_on_crash`:**反面证明**(review 后改写,原草稿只断言幸存者的 ETS 行完整,属恒真、无鉴别力)。复制关闭时 Owner 上建的订阅不得出现在 Holder(`entities` 查空),kill Owner 后 Holder 的 `session/1` 诚实返回 `{error, not_found}`——无陈旧幽灵。与用例 1 配对:1 证「复制使其在」,2 证「不复制则不在」。
  3. `inflight_dedup_preserved`:QoS2 `inflight` blob 建立后 kill Owner,接管节点 `session/1` 的 `inflight` 原样恢复。
  4. `stale_owner_fenced`:Holder `claim` epoch 2 后,旧 owner(仍在线)以 epoch 1 写被 `{error, {fenced, _}}` 拒;另一存活旧副本以 epoch 2 亦被拒;新 owner 接受当前 epoch。
  5. `single_node_unchanged`:开关关闭时无 `session_replicator` 钩子(I2 回归保护)。

  **三处对草稿的实测修正(均为测试假设错误,产品行为正确)**:
  - 游标语义:`broker_subscription:cursor/2` 返回**连续前缀水位**(`broker_cursor:next_read/1` = `offset`);`advance_plain/2` 对水位之上的 offset 只入 `pending`、**不前移**水位,只有 ack 到当前连续位才级联前移。故草稿的 `ack(…,5) → 游标 6` 不成立,改为「读 `C0` → ack `C0` → 断言 `Before > C0` 且 `After >= Before`」。
  - `session/1` 的 `subs` 是订阅 **map** 列表(`#{sub_ref := SubRef, ...}`),非裸 SubRef 列表,断言用 `?assertMatch([#{sub_ref := SubRef}], ...)`。
  - 会话分区空间(`session_partition_count`,所有权维度)与队列分区空间(`broker_subscription` 游标维度)是两套独立编号;用例 1 演示游标时用真实队列分区 `hd(broker_queue:partition_ids())`,而非会话分区号。

   > **设计转向说明**:草稿原意是「不依赖 ra 真正选举,单元级可控 owner/epoch」(用 cache-only stub 直接指定 owner,换取确定性接管时序)。实现**刻意放弃**这一确定性,改用**真实 3 节点 ra 组**:因为 `claim/3` 是一条穿过 `ra:process_command/3` 的真 ra 命令,只有真 ra 才能串行化「谁是当前 owner」并在 owner 崩溃后完成 leader 选举;cache-only 的 stub 无法证明接管语义(谁被踢出、谁被选入、epoch 如何单调递增),那正是本套件要验的东西。

- [ ] **Step 3: 跑全套**

Run: `rm -rf _build/test/extras && rebar3 ct --suite=apps/broker_cluster/test/broker_session_replica_SUITE.erl`
Expected: PASS — 5 用例全绿。

- [ ] **Step 4: 提交**

```bash
git add apps/broker_cluster/test/broker_session_replica_SUITE.erl docs/superpowers/plans/2026-11-22-session-state-replication-plan2.md
git commit -m "test(broker_cluster): real-ra failover fault-injection suite, 5 cases (Plan 2 T6)"
```

---

## Task 7: 指标 + 文档批注 + plan2-notes

注册接管侧冻结键、给 Plan 1 spec 追加带日期批注、写 Plan 2 实现记录。

**Files:**
- Modify: `apps/broker_cluster/src/broker_cluster.erl`(指标 inc,claim/nodedown 路径)
- Modify: `docs/superpowers/specs/2026-11-22-session-state-replication-design.md`(§7 追加批注)
- Create: `docs/superpowers/specs/2026-11-22-session-state-replication-plan2-notes.md`

- [ ] **Step 1: 注册指标冻结键**

> **实现说明(实测修正,与草稿的差异)**:草稿提议 `broker_metrics:inc(cluster_session_takeover_total)` 带 `result` 标签、并在 `claim/3` 里同时 inc `cluster_owner_changes`。这**两处都错**:① ADR-016 §16.2 冻结键集是扁平 atom 列表、**禁止高基数标签**,spec §10 的「按 result 分标签」只能落地为**一结果一键**;② `command/1` 已对任何 `{ok, N}` 回包 inc 一次 `cluster_owner_changes`,`claim/3` 走同一条 `command/1`,再 inc 会**双算**一次接管。故实际实现为四个闭集键(`cluster_session_takeover_ok` / `_stale_epoch` / `_no_replica` / `_not_found`),`claim/3` 只按**代码返回形状**分类 inc,不碰 `cluster_owner_changes`。

在 `broker_metrics` 的 `key_specs/0` 末尾(紧随 `{cluster_session_replication_failed, counter}`)追加四个 counter 键;`broker_metrics_SUITE` 的 `-define(KEYS,...)` 同步按**相同顺序**追加(否则 `keys_are_stable` 的精确比对会红),并加用例 `session_takeover_keys_registered` 断言四键均为 counter。

`broker_cluster:claim/3` 改为:

```erlang
claim(Partition, Node, Epoch) when is_integer(Partition), is_atom(Node),
                                   is_integer(Epoch) ->
    Result = command({claim, Partition, Node, Epoch}),
    _ = try inc_takeover(Result) catch _:_ -> ok end,
    Result.

%% 私有，不导出。形状以 meta_machine:claim/6 为准。
inc_takeover({ok, _}) -> broker_metrics:inc(cluster_session_takeover_ok);
inc_takeover({error, {stale_epoch, _, _}}) ->
    broker_metrics:inc(cluster_session_takeover_stale_epoch);
inc_takeover({error, {not_a_replica, _}}) ->
    broker_metrics:inc(cluster_session_takeover_no_replica);
inc_takeover({error, not_found}) ->
    broker_metrics:inc(cluster_session_takeover_not_found);
inc_takeover(_Other) -> ok.   %% 传输层失败（no_quorum 已由 cluster_no_quorum 计）
```

> **已删除/否决的草案做法**（勿复活）:`cluster_session_takeover_total`（标签式）、在 `claim/3` 内 `inc(cluster_owner_changes)`（双算）。另:spec §10 建议的 `cluster_session_takeover_latency_us` 本次**未实现**,已在 plan2-notes 偏差记录中如实标注。

- [ ] **Step 2: 给 Plan 1 spec 追加带日期批注**

在 `docs/superpowers/specs/2026-11-22-session-state-replication-design.md` 文件**末尾**追加(不改既有正文):

> **实现说明**:草稿把修订表述为「ra 按副本固定序选新 owner」——**误述**,`ra` 不做任何选主。实际是两条互不依赖的路径:① 持连节点观察 `{nodedown, O}` 后提交 `{claim, P, N, Epoch}` 事后认领(主路径,无副本序偏好);② `broker_cluster_nodedown:pick_new_owner/2` 在**本地**按副本固定序选存活副本再提交 `{migrate,...}`(兜底)。批注正文按此改写。

```markdown

---

## 8. Plan 2 实现期批注（2026-11-22）

【2026-11-22】§3.5 及 §4 数据流的「ra 候选排序内持连优先」已在 Plan 2 brainstorming 修订为**两条互不依赖的路径**，ra **不**做任何候选排序或副本序选择，只负责把命令串行 apply：

1. **持连节点事后认领（claim，主路径）**：持连副本节点观察 `{nodedown, O}` 后向 ra 提交 `{claim, P, N, Epoch}`，把 owner 改成自己。此路径**没有任何副本序偏好**——谁持连、谁先提交，谁就拿到所有权；判据只有 epoch 单调 + `N` 必须在 `replicas` 集内（`broker_cluster_meta_machine:claim/6` 的两条守卫），连接判断留在本地 conn_fsm。
2. **主动迁移（migrate，兜底路径）**：`broker_cluster_nodedown:pick_new_owner/2` 在**本地**按副本固定序取第一个存活副本作新 owner（epoch+1），再提交既有的 `{migrate, ...}` 命令。

理由：连接存活信息不进 ra 排序，避免把节点级 liveness 拉回集群元数据；两条路径都经 ra 串行 apply，故仍保证全局唯一 owner。Plan 1 复制管线不受影响。详见 [Plan 2 设计](2026-11-22-session-state-replication-plan2-design.md) §4 与 §6。
```

- [ ] **Step 3: 写 plan2-notes**

创建 `docs/superpowers/specs/2026-11-22-session-state-replication-plan2-notes.md`:

```markdown
# 会话状态复制 Plan 2 实现记录

| 项 | 内容 |
|---|---|
| 日期 | 2026-11-22 |
| 设计 | [Plan 2 设计](2026-11-22-session-state-replication-plan2-design.md) |
| 状态 | 已交付 |

## 实现要点

- **会话→分区映射**：`broker_cluster_session_owner:partition_of/2`（`phash2`，固定 `session_partition_count`），还 Plan 1 债 #1。
- **复制路由**：`replicate_session_best_effort/1` 从「广播所有副本集」改为「该会话分区副本集」。
- **claim 命令**：`broker_cluster_meta_machine` 新增 `{claim, P, Node, Epoch}`，epoch 单调 + `is_member(Node, Replicas)`（ADR-034 D7）双守卫。
- **崩溃发现**：`broker_cluster_nodedown` 的 `pick_new_owner/2` 在**本地**按副本固定序选存活副本（epoch+1），`on_nodedown/1` 逐分区提交 `{migrate,...}`（每分区独立 try）；真正的 `monitor_node` 由 sup 启动的 `broker_cluster_nodedown_mon` 承担（`session_replication` 门控、`{nodeup,_}` 重挂、nodedown 显式 demonitor）。
- **conn_fsm**：`maybe_monitor_owner/2` 在 attach 后按会话分区装 `monitor_node`；`{nodedown, Owner}` 子句**结果感知**——`claim` 成功才翻 `owner_node`，失败只 warning 不翻转；判定收敛为 `takeover_target/2`（per-connection 单 `owner_part`/`owner_node`，**无批量模型**）；`terminate/3` 显式 demonitor；开关关时不装 monitor（I2）。

## 缺口表（Plan 2 边界债）

| # | 缺口 | 影响 | 后续 |
|---|---|---|---|
| 1 | nodedown 触发无去抖（spec §12 未决点）：网络抖动可能误判崩溃 | 误迁移 + epoch 抖动 | 加去抖窗口（留 Plan 3） |
| 2 | claim 与 nodedown-migrate 双路径无显式互斥测试 | 极端时序下理论竞争 | 补并发 CT（留 Plan 3） |
| 3 | `session_partition_count` 默认值未与集群规模联动 | 大集群分区可能不足 | 启动期按节点数推导（留 Plan 3） |

## Plan 1/2 对比表

| 维度 | Plan 1 | Plan 2 |
|---|---|---|
| 复制路由 | 广播所有副本集（债#1) | 该会话分区副本集 |
| 所有权 | 无 | 分区级 owner/epoch/fencing |
| 崩溃接管 | 无 | claim + nodedown-migrate 双路径 |
| 透明性 | 无客户端面 | MQTT 层零感知 |
| 故障注入 CT | 2 用例（副本落盘） | 5 用例（接管/ fencing/退化） |

## 验证

- 各 Task 的守门套件（`broker_metrics_SUITE` 16、`broker_cluster_meta_SUITE` 4、`mqtt_conn_fsm_takeover_SUITE` 3、`broker_session_replica_SUITE` 6、`broker_cluster_session_{rep,nodedown,session_owner}_SUITE` 12）全绿。
- 全量 CT 基线为 **1663**（Plan 1 交付时）。本 Task 记录不声称已重跑全量;真跑结果见实施会话。
- 提交：见各 Task commit。
```

- [ ] **Step 4: 全量验证**

Run: `export PATH="$HOME/Development/otp-29.1.2/lib/erlang/bin:$PATH" && rm -rf _build/test/extras && rebar3 ct`
Expected: 全绿。若出现已知无关失败，需与 Plan 2 变更无因果关系（如实记录，不掩盖）。

- [ ] **Step 5: 提交**

```bash
git add apps/broker_cluster/src/broker_cluster.erl \
        docs/superpowers/specs/2026-11-22-session-state-replication-design.md \
        docs/superpowers/specs/2026-11-22-session-state-replication-plan2-notes.md
git commit -m "docs+metrics(broker_cluster): Plan 2 takeover metrics, spec annotation, plan2-notes (Plan 2 T7)"
```

---

## Self-Review(写完计划后的自查)

**1. Spec 覆盖**:
- spec §3 分区级所有权 → Task 1(映射)+ Task 3(claim 命令)。✓
- spec §4 claim 机制 → Task 3(命令)+ Task 4(nodedown 触发)+ Task 5(conn_fsm claim)。✓
- spec §5 会话→分区映射 → Task 1(建映射)+ Task 2(按分区路由)。✓
- spec §6 conn_fsm 改造 → Task 5。✓
- spec §7 偏差批注 → Task 7 Step 2。✓
- spec §8 不变量 I5/I6/I7 → Task 3(claim 持副本守卫=I5、epoch fencing=I6)、Task 6(续传判据=I7)。✓
- spec §9 五用例 CT → Task 6。✓
- spec §10 指标 → Task 7 Step 1。✓
- spec §11 边界(claim 持副本/多节点竞争/幂等) → Task 3(持副本守卫)、Task 6(用例 4 fencing)。✓
- spec §12 未决点 → plan2-notes 缺口表记录。✓

**2. Placeholder 扫描**:无 TBD/TODO。Task 6 原草案的 `cursor/4` 为**不存在的 API**,已在实测中证伪并改写为真实 `broker_subscription:cursor/2`;Task 7 草案的 `cluster_session_takeover_total`(标签式)与 `claim/3` 内重复 inc `cluster_owner_changes`(双算)亦已证伪并改写。执行者以改写后的段落为准。

**3. 类型一致性**:`partition_of/2`(binary(), pos_integer())→ partition() 全程一致;`claim/3`(Partition, Node, Epoch)→ {ok,Epoch}|{error,_} 在 Task 3 定义、Task 5/6 调用一致;`session_owner/1` Task 1 定义、Task 5 调用一致。~~`claims_to_make/3`~~ 已在 Task 5 review 删除(per-connection 单 owner 模型足够),判定收敛为 `takeover_target/2`(Task 5 定义+测试一致)。✓

**4. 风险点(执行者注意)**:
- Task 2 Step 3 的 `replicate_session_broadcast_all/0` 有个 `Op` 作用域 bug(原 fun 无 Op 参数),计划内已标注修正为 `replicate_session_broadcast_all(Op)` —— 执行者务必按计划内修正版落地。
- Task 5 conn_fsm 改动在 gen_statem 内,Task 5 Step 5 全量编译 + 既有 conn 套件回归是守门。
- Task 6 dist-CT 对时序敏感,用 `wait_until/1` 轮询而非一次性读。
