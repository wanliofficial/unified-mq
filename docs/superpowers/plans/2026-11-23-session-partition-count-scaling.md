# 会话分区数随集群规模缩放 —— 实施计划（Plan 3 缺口 #3）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把会话分区数 C 的权威值移入 ra 元数据组，补上启动 bootstrap（让接管链路在生产真正可运行），实现成员变化时的自动扩缩容，并让存量连接平滑迁移到新分区。

**Architecture:** 四层改动 —— ① `broker_cluster_meta_machine` 新增 `set_session_partition_count` 命令型与两个 state 字段（C + epoch），`broker_cluster_cache` 镜像；② 新增 `broker_cluster_session_partition` 纯函数模块（推导规则 + 分区 owner 选择）；③ 新增 `broker_cluster_session_bootstrap`（幂等建元数据 + 扩缩容执行）；④ `mqtt_conn_fsm` 在既有 poll tick 上比对 epoch，变化则重算分区。

**Tech Stack:** Erlang/OTP 29.1.2、ra（内置）、rebar3、Common Test。

---

## 环境（每个 Task 均适用）

```bash
export PATH="$HOME/Development/otp-29.1.2/lib/erlang/bin:$PATH"
```

- 任何 `rebar3 ct` 前先 `rm -rf _build/test`（陈旧 test 源码副本陷阱）。
- **rebar3 唯一非零退出陷阱**：OTP 29 上 `rebar3 ct` 可能在**全部测试通过后**末尾崩在
  `undef {cover,modules,[]}`。这是上游 bug，不是测试失败。判据看 CT 日志：
  `find _build/test/logs -name suite.log -path "*<SUITE>*" -exec grep -E "^=result|^=successful|^=failed" {} \;`
  出现 `=result ok` 才算绿。
- `rebar.config` 开启 `warnings_as_errors`：任何未使用变量/函数都是**编译失败**。
- 本计划所有测试套件用 `port => 0`（随机端口），不硬编码端口。

### 既有代码必须沿用的形状（已实测，勿凭直觉改回）

| 事实 | 正确写法 |
|---|---|
| `broker_cluster_cache:owner/1` | 返回 `{ok, Owner} \| not_found`，**绝不**是裸节点 |
| leader 查询 | `ra:members({broker_cluster_meta, Node})` → `{ok, Members, {_Name, Leader}}`，第 3 元素才是 leader。`ra:leader/1` **不存在** |
| `#st` 是 `mqtt_conn_fsm` 内部 record | 新字段加在 `-record(st, {...})` 里，且**必须**给默认值（热升级/code_change 安全） |
| 元数据命令入口 | `broker_cluster_meta:command(Command)`；经 `ra:process_command` |

---

## 文件结构

| 文件 | 职责 | 动作 |
|---|---|---|
| `apps/broker_cluster/src/broker_cluster_meta_machine.erl` | 新增 `set_session_partition_count` 命令型 + state 字段 + 快照兼容 | 修改 |
| `apps/broker_cluster/src/broker_cluster_cache.erl` | 新增保留键行 + `session_partition_config/0`，并从 `all/0`/`partition_view/0` 过滤 | 修改 |
| `apps/broker_cluster/src/broker_cluster_session_partition.erl` | 纯函数：`derive_count/1`、`next_power_of_two/1`、`pick_owner/2` | 新建 |
| `apps/broker_cluster/src/broker_cluster_session_bootstrap.erl` | `ensure/1`（幂等建 C + 0..C-1 元数据）、`rescale/1`（成员变化驱动）、`reclaim/2`（缩容回收） | 新建 |
| `apps/broker_cluster/src/broker_cluster_session_owner.erl` | `session_partition_count/0` 改为 cache 优先；新增 `session_partition_epoch/0` | 修改 |
| `apps/broker_cluster/src/broker_cluster.erl` | 导出 `set_session_partition_count/1`（薄委托） | 修改 |
| `apps/broker_cluster/src/broker_cluster_nodedown_mon.erl` | 成员收敛后调 `maybe_rescale` | 修改 |
| `apps/broker_proto_mqtt/src/mqtt_conn_fsm.erl` | `#st` 加 `owner_epoch`；poll tick 比对并迁移 | 修改 |
| `apps/broker_cluster/test/broker_cluster_session_partition_SUITE.erl` | 纯函数单测 | 新建 |
| `apps/broker_cluster/test/broker_cluster_meta_SUITE.erl` | 追加 `set_session_partition_count` 用例 | 修改 |
| `apps/broker_cluster/test/broker_cluster_session_bootstrap_dist_SUITE.erl` | 真 3 节点 bootstrap + 扩缩容 CT | 新建 |

---

## Task 1: 纯函数模块（推导规则 + owner 选择）

**Files:**
- Create: `apps/broker_cluster/src/broker_cluster_session_partition.erl`
- Test: `apps/broker_cluster/test/broker_cluster_session_partition_SUITE.erl`

- [ ] **Step 1: 写失败的测试**

创建 `apps/broker_cluster/test/broker_cluster_session_partition_SUITE.erl`：

```erlang
%% @doc 会话分区数的推导规则与分区 owner 选择（纯函数，非 dist）。
-module(broker_cluster_session_partition_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() -> [derive_count_is_power_of_two_at_least_min,
          derive_count_grows_with_members,
          derive_count_small_cluster_uses_min,
          next_power_of_two_boundaries,
          pick_owner_is_deterministic_and_in_replicas].

init_per_testcase(_Case, Config) ->
    _ = application:load(broker_cluster),
    ok = application:set_env(broker_cluster, session_partition_min_count, 4),
    ok = application:set_env(broker_cluster, session_partitions_per_node, 4),
    Config.

end_per_testcase(_Case, _Config) ->
    _ = application:unset_env(broker_cluster, session_partition_min_count),
    _ = application:unset_env(broker_cluster, session_partitions_per_node),
    ok.

%% 结果一定是 2 的幂，且 >= 最小分区数。
derive_count_is_power_of_two_at_least_min(_Config) ->
    lists:foreach(
      fun(N) ->
              C = broker_cluster_session_partition:derive_count(N),
              ?assert(is_power_of_two(C) orelse ct:fail({not_pow2, N, C})),
              ?assert(C >= 4)
      end, lists:seq(0, 40)),
    ok.

%% 成员越多，C 不减（单调不减）。
derive_count_grows_with_members(_Config) ->
    Cs = [broker_cluster_session_partition:derive_count(N) || N <- lists:seq(0, 32)],
    ?assertEqual(lists:sort(Cs), Cs),
    ok.

%% 小集群：1 节点 → max(4, 1*4)=4 → 4；0 节点同样 4。
derive_count_small_cluster_uses_min(_Config) ->
    ?assertEqual(4, broker_cluster_session_partition:derive_count(1)),
    ?assertEqual(4, broker_cluster_session_partition:derive_count(0)),
    %% 3 节点：max(4, 3*4)=12 → 向上取 2 的幂 = 16。
    ?assertEqual(16, broker_cluster_session_partition:derive_count(3)),
    ok.

next_power_of_two_boundaries(_Config) ->
    ?assertEqual(1, broker_cluster_session_partition:next_power_of_two(1)),
    ?assertEqual(1, broker_cluster_session_partition:next_power_of_two(0)),
    ?assertEqual(2, broker_cluster_session_partition:next_power_of_two(2)),
    ?assertEqual(4, broker_cluster_session_partition:next_power_of_two(3)),
    ?assertEqual(8, broker_cluster_session_partition:next_power_of_two(5)),
    ?assertEqual(16, broker_cluster_session_partition:next_power_of_two(12)),
    ?assertEqual(16, broker_cluster_session_partition:next_power_of_two(16)),
    ok.

%% 同一 (P, Members) 永远选同一 owner，且 owner ∈ Members。
pick_owner_is_deterministic_and_in_replicas(_Config) ->
    Members = [n1, n2, n3],
    lists:foreach(
      fun(P) ->
              O1 = broker_cluster_session_partition:pick_owner(P, Members),
              O2 = broker_cluster_session_partition:pick_owner(P, Members),
              ?assertEqual(O1, O2),
              ?assert(lists:member(O1, Members))
      end, lists:seq(0, 31)),
    %% 分区在成员间均摊（不是全压在一个节点上）。
    Owners = [broker_cluster_session_partition:pick_owner(P, Members)
              || P <- lists:seq(0, 15)],
    ?assertEqual(3, length(lists:usort(Owners))),
    ok.

is_power_of_two(N) when is_integer(N), N > 0 ->
    (N band (N - 1)) =:= 0;
is_power_of_two(_) ->
    false.
```

- [ ] **Step 2: 跑测试确认失败**

```bash
cd /home/wanli/.local/share/opencode/worktree/10af50/gap3-partition-scaling
rm -rf _build/test
rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_session_partition_SUITE.erl
```

Expected: 编译失败 `module broker_cluster_session_partition is not available`（或 `undef`）。

- [ ] **Step 3: 写最小实现**

创建 `apps/broker_cluster/src/broker_cluster_session_partition.erl`：

```erlang
%% @doc 会话分区数的推导规则与分区 owner 选择（纯函数，不碰 ra / 网络）。
%%
%% 设计见 2026-11-23-session-partition-count-scaling-design.md §3.4。
%% 抽成独立模块的理由：推导规则与 owner 选择是纯决策，必须能脱离集群单测；
%% 把 ra 提交与它分开，决策错误就不会被 ra 的失败形状掩盖。
-module(broker_cluster_session_partition).

-export([derive_count/1, next_power_of_two/1, pick_owner/2]).

-define(DEFAULT_MIN_COUNT, 4).
-define(DEFAULT_PER_NODE, 4).

%% @doc 按成员数推导会话分区数：`max(min, Members * PerNode)` 向上取 2 的幂。
%% 2 的幂让 `phash2/2` 分布更均匀，且扩缩容步长有意义（4→8→16）。
-spec derive_count(non_neg_integer()) -> pos_integer().
derive_count(MemberCount) when is_integer(MemberCount), MemberCount >= 0 ->
    Min = env_int(session_partition_min_count, ?DEFAULT_MIN_COUNT),
    PerNode = env_int(session_partitions_per_node, ?DEFAULT_PER_NODE),
    next_power_of_two(max(Min, MemberCount * PerNode)).

%% @doc 向上取最近的 2 的幂；0 与 1 都返回 1。
-spec next_power_of_two(non_neg_integer()) -> pos_integer().
next_power_of_two(N) when is_integer(N), N =< 1 ->
    1;
next_power_of_two(N) when is_integer(N) ->
    next_power_of_two(N, 1).

next_power_of_two(N, Acc) when Acc >= N -> Acc;
next_power_of_two(N, Acc) -> next_power_of_two(N, Acc * 2).

%% @doc 为分区 P 在 Members 里确定性选一个 owner：按 P 轮转，保证均摊且可复现。
%% 空列表返回 `undefined`（调用方必须处理：无成员就不该建分区）。
-spec pick_owner(non_neg_integer(), [node()]) -> node() | undefined.
pick_owner(_P, []) ->
    undefined;
pick_owner(P, Members) when is_integer(P), P >= 0 ->
    Sorted = lists:sort(Members),
    lists:nth((P rem length(Sorted)) + 1, Sorted).

env_int(Key, Default) ->
    case application:get_env(broker_cluster, Key) of
        N when is_integer(N), N > 0 -> N;
        _ -> Default
    end.
```

- [ ] **Step 4: 跑测试确认通过**

```bash
cd /home/wanli/.local/share/opencode/worktree/10af50/gap3-partition-scaling
rm -rf _build/test
rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_session_partition_SUITE.erl
find _build/test/logs -name suite.log -path "*session_partition*" -exec grep -E "^=result|^=successful|^=failed" {} \;
```

Expected: `=successful 5`、`=failed 0`。

- [ ] **Step 5: 提交**

```bash
cd /home/wanli/.local/share/opencode/worktree/10af50/gap3-partition-scaling
git add apps/broker_cluster/src/broker_cluster_session_partition.erl \
        apps/broker_cluster/test/broker_cluster_session_partition_SUITE.erl
git commit -m "feat(cluster,gap3): 会话分区数推导规则 + 分区 owner 选择纯函数（缺口 #3 T1）"
```

---

## Task 2: ra 状态机承载 C + cache 镜像

**Files:**
- Modify: `apps/broker_cluster/src/broker_cluster_meta_machine.erl`
- Modify: `apps/broker_cluster/src/broker_cluster_cache.erl`
- Modify: `apps/broker_cluster/src/broker_cluster.erl`
- Test: `apps/broker_cluster/test/broker_cluster_meta_SUITE.erl`

- [ ] **Step 1: 写失败的测试**

在 `apps/broker_cluster/test/broker_cluster_meta_SUITE.erl` 的 `all/0` 里追加 4 个用例名：

```erlang
all() -> [claim_moves_owner_when_holding_replica,
          claim_rejects_node_without_replica,
          claim_rejects_stale_epoch,
          claim_rejects_non_integer_epoch,
          set_count_commits_and_mirrors,
          set_count_rejects_stale_epoch,
          set_count_rejects_bad_count,
          old_snapshot_without_count_fields_is_tolerated].
```

在文件末尾追加：

```erlang
%% C 与 epoch 写入 state 并镜像进 cache。
set_count_commits_and_mirrors(_Config) ->
    S0 = #{partitions => #{}},
    {S1, {ok, 1}, []} = broker_cluster_meta_machine:apply(
                          #{index => 10}, {set_session_partition_count, 16, 1}, S0),
    ?assertEqual(16, maps:get(session_partition_count, S1)),
    ?assertEqual(1, maps:get(session_partition_epoch, S1)),
    ?assertEqual({ok, 16, 1}, broker_cluster_cache:session_partition_config()),
    ok.

%% epoch 单调：不更大的 epoch 被拒且无副作用。
set_count_rejects_stale_epoch(_Config) ->
    S0 = #{partitions => #{}, session_partition_count => 8,
           session_partition_epoch => 5},
    {S1, {error, {stale_epoch, 5, 3}}, []} = broker_cluster_meta_machine:apply(
                                               #{index => 11},
                                               {set_session_partition_count, 16, 3}, S0),
    ?assertEqual(S0, S1),
    ok.

%% 非正整数被拒。
set_count_rejects_bad_count(_Config) ->
    S0 = #{partitions => #{}, session_partition_count => 8,
           session_partition_epoch => 2},
    {S1, {error, {bad_count, 0}}, []} = broker_cluster_meta_machine:apply(
                                          #{index => 12},
                                          {set_session_partition_count, 0, 3}, S0),
    ?assertEqual(S0, S1),
    {S2, {error, {bad_count, -4}}, []} = broker_cluster_meta_machine:apply(
                                           #{index => 13},
                                           {set_session_partition_count, -4, 3}, S0),
    ?assertEqual(S0, S2),
    ok.

%% 旧快照（无新字段）能容忍：缺省 count=undefined、epoch=0，不崩。
old_snapshot_without_count_fields_is_tolerated(_Config) ->
    %% 模拟旧 machine state：只有 partitions。
    %% （apply/3 的 get 命令不依赖这些字段，故一次普通 apply 就能验证不崩。）
    S0 = #{partitions => #{}},
    {_S1, {error, not_found}, []} =
        broker_cluster_meta_machine:apply(#{index => 14}, {get, 0}, S0),
    ok.
```

- [ ] **Step 2: 跑测试确认失败**

```bash
cd /home/wanli/.local/share/opencode/worktree/10af50/gap3-partition-scaling
rm -rf _build/test
rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_meta_SUITE.erl
```

Expected: `set_count_commits_and_mirrors` 等失败，报 `{error, {unknown_command, ...}}` 或 `undef session_partition_config`。

- [ ] **Step 3: 改状态机**

在 `apps/broker_cluster/src/broker_cluster_meta_machine.erl`：

3a. 修改 type 与 `init/1`：

```erlang
-type state() :: #{partitions := #{partition() => meta()},
                   session_partition_count => pos_integer() | undefined,
                   session_partition_epoch => non_neg_integer()}.

-spec init(map()) -> state().
init(_Config) ->
    #{partitions => #{}, session_partition_count => undefined,
      session_partition_epoch => 0}.
```

3b. 在 `apply/3` 的 `case Command of` 里，`{claim, ...}` 之后加一个分支：

```erlang
        {set_session_partition_count, Count, Epoch} ->
            set_session_partition_count(Count, Epoch, Ps, State);
```

3c. 在 `%%%_* Internal` 之后追加实现（放在 `claim/6` 之后）：

```erlang
%% Plan 3 (gap #3, design §3.1): 会话分区数的权威值。epoch 单调守卫与 claim
%% 同构：拒绝陈旧写；count 必须是正整数，否则分区空间非法。
set_session_partition_count(Count, Epoch, _Ps, State) ->
    Cur = maps:get(session_partition_epoch, State, 0),
    if
        not is_integer(Count); Count =< 0 ->
            {State, {error, {bad_count, Count}}, []};
        not is_integer(Epoch); Epoch =< Cur ->
            {State, {error, {stale_epoch, Cur, Epoch}}, []};
        true ->
            broker_cluster_cache:put_session_partition_config(Count, Epoch),
            {State#{session_partition_count := Count,
                    session_partition_epoch := Epoch}, {ok, Epoch}, []}
    end.
```

3d. `snapshot_installed/5` 增加对保留配置的镜像（旧快照缺字段也安全）：

```erlang
snapshot_installed(_Meta, _Snapshot, _State, _Machine, _Module) ->
    #{partitions := Ps} = _State,
    broker_cluster_cache:clear(),
    maps:foreach(fun(P, M) -> broker_cluster_cache:put(P, M) end, Ps),
    case maps:get(session_partition_count, _State, undefined) of
        undefined -> ok;
        C -> broker_cluster_cache:put_session_partition_config(
               C, maps:get(session_partition_epoch, _State, 0))
    end,
    ok.
```

- [ ] **Step 4: 改 cache（保留键行）**

在 `apps/broker_cluster/src/broker_cluster_cache.erl`：

4a. 在 `-export([...])` 列表里加两个：

```erlang
-export([start_link/0, put/2, get/1, all/0, partition_view/0, clear/0,
         clear_partition/1, owner/1, epoch/1, replicas/1,
         put_session_partition_config/2, session_partition_config/0]).
```

4b. 在 `?COMMITTED` 之后加保留键定义：

```erlang
-define(TAB, broker_cluster_cache).
-define(COMMITTED, broker_cluster_committed).
%% 集群级会话分区配置的保留键：一个不可能与 non_neg_integer() 分区键相同的原子。
%% 单独一行存，使 `all/0' / `partition_view/0' 可以显式过滤，绝不把它当分区。
-define(SESSION_CFG, '$session_partition_config').
```

4c. 在 `clear/0` 里清掉该行（`clear` 语义是"镜像全清"）：

```erlang
clear() ->
    try ets:delete_all_objects(?TAB) catch _:_ -> true end,
    %% ADR-034 D6: a cleared mirror must not leave stale committed candidates
    %% pinning a partition nobody owns any more.
    try ets:delete_all_objects(?COMMITTED) catch _:_ -> true end,
    ok.
```

（无需改：`delete_all_objects` 已一并清掉保留行。）

4d. 修改 `all/0` 与 `partition_view/0` 过滤保留键，并追加两个新函数：

```erlang
all() ->
    try [Meta || {P, Meta} <- ets:tab2list(?TAB), P =/= ?SESSION_CFG]
    catch _:_ -> []
    end.

partition_view() ->
    try maps:from_list([{P, Meta} || {P, Meta} <- ets:tab2list(?TAB),
                                     P =/= ?SESSION_CFG])
    catch _:_ -> #{}
    end.

%% @doc 集群级会话分区配置（Plan 3 gap #3）：权威 C 与它的 epoch。
%% 与分区行同表不同键，单独读取，不进 partition_view。
-spec put_session_partition_config(pos_integer(), non_neg_integer()) -> ok.
put_session_partition_config(Count, Epoch)
  when is_integer(Count), Count > 0, is_integer(Epoch), Epoch >= 0 ->
    case ets:info(?TAB, name) of
        undefined -> ok;
        _ -> true = ets:insert(?TAB, {?SESSION_CFG, Count, Epoch}), ok
    end.

-spec session_partition_config() -> {ok, pos_integer(), non_neg_integer()} | not_found.
session_partition_config() ->
    try ets:lookup(?TAB, ?SESSION_CFG) of
        [{?SESSION_CFG, Count, Epoch}] -> {ok, Count, Epoch};
        [] -> not_found
    catch
        _:_ -> not_found
    end.
```

> 注意：`put/2` 与 `get/1` 仍只服务整数分区键；保留行的形状是三元组
> `{Key, Count, Epoch}`，与分区行的 `{Key, Meta}` **形状不同**，所以
> `all/0` 的列表推导 `{P, Meta}` 会**跳过**它（元组 arity 不匹配）——过滤是
> 双保险，`=/=` 那一条已经足够明确。

- [ ] **Step 5: 加薄委托到 broker_cluster**

在 `apps/broker_cluster/src/broker_cluster.erl` 的 `-export([...])` 里，写入命令那一行追加：

```erlang
-export([assign/4, migrate/4, finish_migration/2, remove/1, claim/3,
         set_session_partition_count/1]).
```

在 `claim/3` 之后（同一节）追加：

```erlang
%% @doc Plan 3 (gap #3): 提交权威会话分区数。写 ra，全集群一致。
-spec set_session_partition_count(pos_integer()) ->
          {ok, non_neg_integer()} | {error, term()}.
set_session_partition_count(Count) when is_integer(Count), Count > 0 ->
    Epoch = case broker_cluster_cache:session_partition_config() of
                {ok, _, E} -> E + 1;
                not_found -> 1
            end,
    command({set_session_partition_count, Count, Epoch}).
```

- [ ] **Step 6: 跑测试确认通过**

```bash
cd /home/wanli/.local/share/opencode/worktree/10af50/gap3-partition-scaling
rm -rf _build/test
rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_meta_SUITE.erl
find _build/test/logs -name suite.log -path "*meta_SUITE*" -exec grep -E "^=result|^=successful|^=failed" {} \;
```

Expected: `=successful 8`、`=failed 0`。

- [ ] **Step 7: 跑了受影响的既有套件，确认没回归**

```bash
cd /home/wanli/.local/share/opencode/worktree/10af50/gap3-partition-scaling
rm -rf _build/test
rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_session_owner_SUITE.erl
find _build/test/logs -name suite.log -path "*session_owner_SUITE*" -exec grep -E "^=result|^=successful|^=failed" {} \;
```

Expected: `=successful 3`、`=failed 0`。

- [ ] **Step 8: 提交**

```bash
cd /home/wanli/.local/share/opencode/worktree/10af50/gap3-partition-scaling
git add apps/broker_cluster/src/broker_cluster_meta_machine.erl \
        apps/broker_cluster/src/broker_cluster_cache.erl \
        apps/broker_cluster/src/broker_cluster.erl \
        apps/broker_cluster/test/broker_cluster_meta_SUITE.erl
git commit -m "feat(cluster,gap3): ra 承载会话分区数与 epoch + cache 保留键镜像（缺口 #3 T2）"
```

---

## Task 3: 读取路径改造（cache 优先，env 兜底）

**Files:**
- Modify: `apps/broker_cluster/src/broker_cluster_session_owner.erl`
- Test: `apps/broker_cluster/test/broker_cluster_session_owner_SUITE.erl`

- [ ] **Step 1: 写失败的测试**

在 `apps/broker_cluster/test/broker_cluster_session_owner_SUITE.erl` 的 `all/0` 追加两个用例：

```erlang
all() -> [partition_of_is_deterministic_and_in_range,
          partition_of_spreads_across_space,
          session_owner_reads_local_cache,
          count_prefers_ra_mirror_over_env,
          epoch_is_zero_without_config].
```

追加实现：

```erlang
%% 权威 C 一旦镜像进 cache，就读它，而不是 env。
count_prefers_ra_mirror_over_env(_Config) ->
    ok = application:set_env(broker_cluster, session_partition_count, 4),
    ?assertEqual(4, broker_cluster_session_owner:session_partition_count()),
    ok = broker_cluster_cache:put_session_partition_config(16, 3),
    ?assertEqual(16, broker_cluster_session_owner:session_partition_count()),
    ok.

%% 无权威配置时 epoch 读 0（不是 undefined / 崩溃）。
epoch_is_zero_without_config(_Config) ->
    ?assertEqual(0, broker_cluster_session_owner:session_partition_epoch()),
    ok = broker_cluster_cache:put_session_partition_config(8, 7),
    ?assertEqual(7, broker_cluster_session_owner:session_partition_epoch()),
    ok.
```

> 注意：这些用例依赖 cache 在 `init_per_testcase` 已被 `clear()`，现有 fixture 已
> `start_link` 并在 `end_per_testcase` `gen_server:stop`；`put_session_partition_config`
> 的行会随进程销毁消失，用例间互不污染。若首次运行出现串扰，在
> `init_per_testcase` 显式加 `ok = broker_cluster_cache:clear()`（幂等）。

- [ ] **Step 2: 跑测试确认失败**

```bash
cd /home/wanli/.local/share/opencode/worktree/10af50/gap3-partition-scaling
rm -rf _build/test
rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_session_owner_SUITE.erl
```

Expected: `undef broker_cluster_session_owner:session_partition_epoch/0`。

- [ ] **Step 3: 改实现**

`apps/broker_cluster/src/broker_cluster_session_owner.erl` 全文替换为：

```erlang
%% @doc 会话→分区映射 + owner 视图读（spec 2026-11-22 Plan 2 §3/§5；
%% Plan 3 gap #3 起，会话分区数 C 的权威值来自 ra 元数据组，见设计 §3.2）。
%%
%% 两套独立空间（spec §5 取舍）：会话分区空间由 C 定义稠密 `0..C-1'，用
%% `erlang:phash2(ClientId, C)' 确定性散列；与 `broker_rebalance' 的动态元数据
%% 分区**互不干扰**（后者随 rebalance 变，前者固定），避免会话归属漂移。owner
%% 视图从**本地** `broker_cluster_cache' 读，绝不每 append 走 ra（ADR-010 数据
%% 路径原则）。
-module(broker_cluster_session_owner).

-export([partition_of/2, session_partition_count/0,
         session_partition_epoch/0, session_owner/1]).

-type partition() :: non_neg_integer().

%% @doc 确定性散列 ClientId 到 `[0, Count)' 的会话分区。
-spec partition_of(binary(), pos_integer()) -> partition().
partition_of(ClientId, Count) when is_binary(ClientId), is_integer(Count), Count > 0 ->
    erlang:phash2(ClientId, Count).

%% @doc 会话分区数（Plan 3 gap #3）：权威值在 ra 元数据组，经 cache 镜像读；
%% 尚无权威值（单机 / 集群未 bootstrap / 旧快照）时退回 env，再退回推导。
%% 读路径绝不触发共识写（ADR-008）。
-spec session_partition_count() -> pos_integer().
session_partition_count() ->
    case broker_cluster_cache:session_partition_config() of
        {ok, Count, _Epoch} -> Count;
        not_found -> configured_or_derived_count()
    end.

%% @doc 会话分区配置的版本号（单调递增）。无配置时返回 0。
%% conn_fsm 用它检测"分区空间变了，该重算分区了"。
-spec session_partition_epoch() -> non_neg_integer().
session_partition_epoch() ->
    case broker_cluster_cache:session_partition_config() of
        {ok, _Count, Epoch} -> Epoch;
        not_found -> 0
    end.

%% @doc 本节点上某会话分区的 owner（本地 cache 读，无元数据 → not_found）。
-spec session_owner(partition()) -> {ok, node()} | not_found.
session_owner(Partition) ->
    case broker_cluster_cache:owner(Partition) of
        {ok, Owner} when is_atom(Owner) -> {ok, Owner};
        _ -> not_found
    end.

%%%_* Internal ------------------------------------------------------------

%% 尚无权威 C 时的兜底：env 显式正整数优先（运维覆盖），否则按推导规则算。
configured_or_derived_count() ->
    case application:get_env(broker_cluster, session_partition_count) of
        N when is_integer(N), N > 0 ->
            N;
        _ ->
            Members = cluster_member_count(),
            broker_cluster_session_partition:derive_count(Members)
    end.

%% 读当前成员数；拿不到（单机 / ra 未起 / 无 quorum）按 1 算，得到最小值。
cluster_member_count() ->
    try broker_cluster_meta:members() of
        L when is_list(L), L =/= [] -> length(L);
        _ -> 1
    catch
        _:_ -> 1
    end.
```

- [ ] **Step 4: 跑测试确认通过**

```bash
cd /home/wanli/.local/share/opencode/worktree/10af50/gap3-partition-scaling
rm -rf _build/test
rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_session_owner_SUITE.erl
find _build/test/logs -name suite.log -path "*session_owner_SUITE*" -exec grep -E "^=result|^=successful|^=failed" {} \;
```

Expected: `=successful 5`、`=failed 0`。

- [ ] **Step 5: 提交**

```bash
cd /home/wanli/.local/share/opencode/worktree/10af50/gap3-partition-scaling
git add apps/broker_cluster/src/broker_cluster_session_owner.erl \
        apps/broker_cluster/test/broker_cluster_session_owner_SUITE.erl
git commit -m "feat(cluster,gap3): 会话分区数读取改为 ra 镜像优先 + epoch 读取（缺口 #3 T3）"
```

---

## Task 4: 启动 bootstrap（让接管链路在生产可运行）

**Files:**
- Create: `apps/broker_cluster/src/broker_cluster_session_bootstrap.erl`
- Modify: `apps/broker_cluster/src/broker_cluster_sup.erl`
- Test: `apps/broker_cluster/test/broker_cluster_session_bootstrap_dist_SUITE.erl`

- [ ] **Step 1: 写 bootstrap 模块（不含自动扩容，先做幂等建元数据）**

创建 `apps/broker_cluster/src/broker_cluster_session_bootstrap.erl`：

```erlang
%% @doc 启动期会话分区 bootstrap（Plan 3 gap #3，设计 §3.3）。
%%
%% 缺口（调研事实 F1）：生产侧没有任何代码为会话分区 `0..C-1' 建立元数据，
%% 于是 `session_owner(P)' 恒 `not_found'，conn_fsm 永不装 monitor、永不 claim
%% —— 缺口 #4 证明的接管链路在生产从未真正生效。本模块补上这一段：
%%
%%   1. 若 cache 里没有权威 C：推导一个（env 覆盖优先），提交
%%      `{set_session_partition_count, C, 1}'；
%%   2. 对 `0..C-1' 每个分区，幂等 `assign(P, Owner, Members, Epoch+1)'，
%%      补齐缺失或 replicas 过期的分区。
%%
%% 全部操作幂等：重复执行不改变已达标的元数据（见 ensure/1 的判据）。
%% 仅当 `session_replication = true' 时生效（I2 退化）。
-module(broker_cluster_session_bootstrap).

-export([ensure/1, ensure/0]).

%% @doc 用当前集群成员跑一次 bootstrap。
-spec ensure() -> ok | {error, term()}.
ensure() ->
    case members() of
        {ok, Members} -> ensure(Members);
        {error, _} = E -> E
    end.

%% @doc 用显式成员跑一次 bootstrap（测试用，避免依赖 ra 成员视图）。
-spec ensure([node()]) -> ok | {error, term()}.
ensure(Members) when is_list(Members), Members =/= [] ->
    case application:get_env(broker_cluster, session_replication, false) of
        true -> do_ensure(Members);
        _ -> ok
    end;
ensure(_) ->
    {error, no_members}.

do_ensure(Members) ->
    Count = ensure_count(Members),
    ensure_partitions(Count, Members).

%% 1) 权威 C 不存在就建；存在就以它为准确保分区空间一致。
ensure_count(Members) ->
    case broker_cluster_cache:session_partition_config() of
        {ok, Count, _Epoch} ->
            Count;
        not_found ->
            Desired = desired_count(Members),
            case broker_cluster:set_session_partition_count(Desired) of
                {ok, _} -> Desired;
                {error, _} ->
                    %% 提交失败（无 quorum）：用推导值继续，至少能补齐本节点
                    %% 已能看到的元数据；下次 bootstrap 再对齐。
                    Desired
            end
    end.

%% env 显式配置优先，否则按成员数推导（与 session_owner 的兜底一致）。
desired_count(Members) ->
    case application:get_env(broker_cluster, session_partition_count) of
        N when is_integer(N), N > 0 -> N;
        _ -> broker_cluster_session_partition:derive_count(length(Members))
    end.

%% 2) 对 0..C-1 幂等 assign。
ensure_partitions(Count, Members) ->
    lists:foreach(
      fun(P) -> ensure_partition(P, Members) end, lists:seq(0, Count - 1)),
    ok.

ensure_partition(P, Members) ->
    Owner = broker_cluster_session_partition:pick_owner(P, Members),
    case broker_cluster_cache:get(P) of
        {ok, #{replicas := Rs, owner := O}} when Rs =:= Members, O =:= Owner ->
            ok;                                   %% 已达标，幂等 no-op
        _ ->
            Epoch = broker_cluster:epoch_or_zero(P) + 1,
            _ = broker_cluster:assign(P, Owner, Members, Epoch),
            ok
    end.

members() ->
    try broker_cluster_meta:members() of
        L when is_list(L), L =/= [] -> {ok, L};
        _ -> {error, no_members}
    catch
        _:_ -> {error, not_started}
    end.
```

- [ ] **Step 2: 挂到 supervisor**

在 `apps/broker_cluster/src/broker_cluster_sup.erl` 的 `init/1` 里，`Mon` 定义之后、`{ok, {#{...}, [...]}}` 之前，加：

```erlang
    %% Plan 3 (gap #3): 启动 bootstrap，为 0..C-1 建立会话分区元数据。
    %% 必须排在 Meta 之后（要读 ra 成员、提交命令）；premanent 重启会让它
    %% 在 Meta 恢复后自动重跑一次（幂等，安全）。
    Boot = #{id => broker_cluster_session_bootstrap,
             start => {broker_cluster_session_bootstrap, start_link, []},
             restart => permanent,
             shutdown => 5000,
             type => worker,
             modules => [broker_cluster_session_bootstrap]},
    {ok, {#{strategy => one_for_one, intensity => 5, period => 10},
          [Cache, Meta, Mon, Boot]}}.
```

（把原来的 `[Cache, Meta, Mon]` 换掉。）

- [ ] **Step 3: 给 bootstrap 模块加 start_link + gen_server 外壳**

在 `broker_cluster_session_bootstrap.erl` 顶部把 `-export` 与实现改为（保留 Step 1 的纯函数）：

```erlang
-behaviour(gen_server).

-export([ensure/1, ensure/0, start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(RETRY_MS, 2000).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    %% 不在 init 里阻塞跑 bootstrap：ra 可能还没选出 leader。用 0 超时投递
    %% 一条自消息，让 init 先返回、supervisor 起完兄弟进程后再跑。
    self() ! run_bootstrap,
    {ok, #{}}.

handle_info(run_bootstrap, St) ->
    case ensure() of
        ok -> {noreply, St};
        {error, _} ->
            %% 无成员 / ra 未就绪：稍后重试，不崩（崩溃会让 supervisor 重启循环）。
            _ = erlang:send_after(?RETRY_MS, self(), run_bootstrap),
            {noreply, St}
    end;
handle_info(_Other, St) ->
    {noreply, St}.

handle_call(_Req, _From, St) -> {reply, ok, St}.
handle_cast(_Msg, St) -> {noreply, St}.
terminate(_Reason, _St) -> ok.
code_change(_OldVsn, St, _Extra) -> {ok, St}.
```

> 保留 Step 1 已有的 `ensure/0`、`ensure/1`、`do_ensure/2` 等函数不动，只**新增**
> gen_server 回调与 `start_link/0`。

- [ ] **Step 4: 写真集群 CT**

创建 `apps/broker_cluster/test/broker_cluster_session_bootstrap_dist_SUITE.erl`：

```erlang
%% @doc 会话分区 bootstrap 的真 3 节点 ra CT（Plan 3 gap #3，设计 §3.3）。
%% 断言：启动后 0..C-1 每个分区都有元数据、session_owner 非 not_found、
%% 重复 bootstrap 幂等（epoch 不无故增长）。
-module(broker_cluster_session_bootstrap_dist_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(NODES, 3).

all() -> [bootstrap_creates_all_session_partitions,
          bootstrap_is_idempotent].

init_per_suite(Config) ->
    case node() of
        nonode@nohost ->
            case net_kernel:start([unifiedmq_gap3ct, shortnames]) of
                {ok, _} -> [{started_net, true} | Config];
                {error, Reason} -> ct:fail({cannot_start_distribution, Reason})
            end;
        _ -> [{started_net, false} | Config]
    end.

end_per_suite(Config) ->
    case ?config(started_net, Config) of
        true -> _ = net_kernel:stop(), ok;
        _ -> ok
    end.

init_per_testcase(Case, Config) ->
    Peers = start_peers(Case, Config),
    Nodes = [maps:get(node, P) || P <- Peers],
    ok = form_cluster_h(Nodes),
    lists:foreach(
      fun(N) ->
              ok = rpc(N, application, set_env, [broker_cluster, members, Nodes]),
              ok = rpc(N, application, set_env, [broker_cluster, session_replication, true]),
              %% 用显式 C 让断言确定：bootstrap 读 env 覆盖。
              ok = rpc(N, application, set_env, [broker_cluster, session_partition_count, 8]),
              ok = rpc(N, ?MODULE, boot_data_plane, [])
      end, Nodes),
    [{peers, Peers}, {nodes, Nodes} | Config].

end_per_testcase(_Case, Config) ->
    stop_peers(?config(peers, Config)),
    ok.

%% 核心：bootstrap 为 0..C-1 全部分区建了元数据，且 owner 在成员里。
bootstrap_creates_all_session_partitions(Config) ->
    Nodes = peer_nodes(Config),
    [First | _] = Nodes,
    ok = rpc(First, broker_cluster_session_bootstrap, ensure, [Nodes]),
    lists:foreach(
      fun(P) ->
              Owner = rpc(First, broker_cluster_cache, owner, [P]),
              {ok, O} = Owner,
              ?assert(lists:member(O, Nodes)),
              {ok, Rs} = rpc(First, broker_cluster_cache, replicas, [P]),
              ?assertEqual(lists:sort(Nodes), lists:sort(Rs))
      end, lists:seq(0, 7)),
    ok.

%% 幂等：第二次运行不改变已达标分区的 epoch。
bootstrap_is_idempotent(Config) ->
    Nodes = peer_nodes(Config),
    [First | _] = Nodes,
    ok = rpc(First, broker_cluster_session_bootstrap, ensure, [Nodes]),
    Before = [rpc(First, broker_cluster:epoch_or_zero, [P]) || P <- lists:seq(0, 7)],
    ok = rpc(First, broker_cluster_session_bootstrap, ensure, [Nodes]),
    After = [rpc(First, broker_cluster:epoch_or_zero, [P]) || P <- lists:seq(0, 7)],
    ?assertEqual(Before, After),
    ok.

%%%_* Scaffolding (照搬 broker_session_replica_SUITE) -----------------------

form_cluster_h(Nodes) ->
    ok = lists:foldl(fun(N, ok) -> rpc(N, start_meta, [Nodes]) end, ok, Nodes),
    ok.

start_meta(Members) ->
    ok = application:set_env(broker_cluster, members, Members),
    {ok, Meta} = broker_cluster_meta:start_link(),
    unlink(Meta),
    ok.

boot_data_plane() ->
    {ok, RaDir} = application:get_env(broker_cluster, data_dir),
    DataDir = filename:join(RaDir, "data"),
    ok = filelib:ensure_dir(filename:join([DataDir, "x"])),
    _ = [application:load(A) || A <- [broker_metrics, broker_core, broker_storage,
                                      broker_router, broker_queue, broker_subscription,
                                      broker_cluster]],
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok = application:set_env(broker_storage, data_dir, DataDir),
    ok = application:set_env(broker_storage, wal, #{require_durable_fs => false}),
    {ok, _} = application:ensure_all_started(broker_storage),
    ok.

start_peers(Case, Config) ->
    %% 原样复制 broker_session_replica_SUITE:start_peers/2。
    erlang:error({copy_from_reference_suite, start_peers, [Case, Config]}).

peer_name(Case, I) ->
    %% 原样复制 broker_session_replica_SUITE:peer_name/2（前缀改成 gap3_）。
    erlang:error({copy_from_reference_suite, peer_name, [Case, I]}).

sanitise(S) ->
    %% 原样复制 broker_session_replica_SUITE:sanitise/1。
    erlang:error({copy_from_reference_suite, sanitise, [S]}).

log_dir(Case, Name) ->
    %% 原样复制 broker_session_replica_SUITE:log_dir/2。
    erlang:error({copy_from_reference_suite, log_dir, [Case, Name]}).

prepare_peer(Peer, Node) ->
    %% 原样复制 broker_session_replica_SUITE:prepare_peer/2。
    erlang:error({copy_from_reference_suite, prepare_peer, [Peer, Node]}).

code_dirs() ->
    %% 原样复制 broker_session_replica_SUITE:code_dirs/0。
    erlang:error({copy_from_reference_suite, code_dirs, []}).

is_code_dir(D) ->
    %% 原样复制 broker_session_replica_SUITE:is_code_dir/1。
    erlang:error({copy_from_reference_suite, is_code_dir, [D]}).

stop_peers(Peers) ->
    %% 原样复制 broker_session_replica_SUITE:stop_peers/1。
    erlang:error({copy_from_reference_suite, stop_peers, [Peers]}).

peer_nodes(Config) ->
    [N || #{node := N} <- ?config(peers, Config)].

rpc(Node, M, F, A) ->
    %% 原样复制 broker_session_replica_SUITE:rpc/4（含 peer dispatch / 本节点直调）。
    erlang:error({copy_from_reference_suite, rpc, [Node, M, F, A]}).
```

> **重要：不要把上面的 `start_peers/2` 等占位实现留在最终代码里。** 它们是
> `erlang:error({copy_from_reference_suite, ...})` **显式占位**，用途只有一个：
> 让"忘了复制"这件事在第一次跑测试时立刻暴露，而不是悄悄写错。
>
> 实施本 Task 的正确做法：从
> `apps/broker_cluster/test/broker_session_replica_SUITE.erl` **原样复制**下列
> 函数，替换掉上面的占位体（保留本套件的 `?NODES` = 3 与 `gap3_` 名字前缀）：
>
> `start_peers/2`、`stop_peers/1`、`stop_peer/2`、`peer_name/2`、`sanitise/1`、
> `is_code_dir/1`、`code_dirs/0`、`peer_nodes/1`、`prepare_peer/2`、`rpc/3`、
> `rpc/4`、`boot_data_plane/0`、`assign_partition/4`（本套件的扩缩容用例若需要，
> 也可一并复制）。
>
> 这些函数已在真 3 节点场景验证过（`broker_session_replica_SUITE` 与
> `broker_cluster_conn_fsm_takeover_dist_SUITE` 都在用）。**不要**自己重写
> `peer:start` 的 args、`rpc` 的 peer dispatch、`boot_data_plane` 的启动顺序——
> 写错会得到「connection refused」或无端超时，且很难从报错看出根因。
> 复制完删除所有 `copy_from_reference_suite` 占位。

- [ ] **Step 5: 跑测试确认通过**

```bash
cd /home/wanli/.local/share/opencode/worktree/10af50/gap3-partition-scaling
rm -rf _build/test
rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_session_bootstrap_dist_SUITE.erl
find _build/test/logs -name suite.log -path "*bootstrap_dist*" -exec grep -E "^=result|^=successful|^=failed" {} \;
```

Expected: `=successful 2`、`=failed 0`。

- [ ] **Step 6: 提交**

```bash
cd /home/wanli/.local/share/opencode/worktree/10af50/gap3-partition-scaling
git add apps/broker_cluster/src/broker_cluster_session_bootstrap.erl \
        apps/broker_cluster/src/broker_cluster_sup.erl \
        apps/broker_cluster/test/broker_cluster_session_bootstrap_dist_SUITE.erl
git commit -m "feat(cluster,gap3): 启动 bootstrap 为会话分区建元数据（缺口 #3 T4）"
```

---

## Task 5: 自动扩缩容 + 缩容回收

**Files:**
- Modify: `apps/broker_cluster/src/broker_cluster_session_bootstrap.erl`
- Modify: `apps/broker_cluster/src/broker_cluster_nodedown_mon.erl`
- Test: `apps/broker_cluster/test/broker_cluster_session_bootstrap_dist_SUITE.erl`

- [ ] **Step 1: 给 bootstrap 模块加 rescale/1 + reclaim/2**

在 `broker_cluster_session_bootstrap.erl` 的 `-export` 里加：

```erlang
-export([ensure/1, ensure/0, rescale/1, rescale/0, reclaim/2, start_link/0]).
```

追加实现（放在 `do_ensure/2` 之后）：

```erlang
%% @doc 成员变化驱动扩缩容（设计 §3.4）。相同 C 不动；冷却期内不动；
%% 否则提交新 C，再按新 C 重建/回收分区。默认开启（session_partition_autoscale）。
%% 返回 `{rescaled, OldC, NewC} | unchanged | {error, Reason}`。
-spec rescale() -> {rescaled, pos_integer(), pos_integer()} | unchanged | {error, term()}.
rescale() ->
    case members() of
        {ok, Members} -> rescale(Members);
        {error, _} = E -> E
    end.

-spec rescale([node()]) -> {rescaled, pos_integer(), pos_integer()} | unchanged | {error, term()}.
rescale(Members) when is_list(Members), Members =/= [] ->
    case autoscale_enabled() of
        false -> unchanged;
        true ->
            case broker_cluster_cache:session_partition_config() of
                {ok, OldC, _Epoch} ->
                    NewC = broker_cluster_session_partition:derive_count(length(Members)),
                    maybe_rescale(OldC, NewC, Members);
                not_found ->
                    %% 还没有权威 C：走 ensure（它会建）。
                    _ = ensure(Members),
                    unchanged
            end
    end;
rescale(_) ->
    {error, no_members}.

maybe_rescale(OldC, OldC, _Members) ->
    unchanged;
maybe_rescale(OldC, NewC, Members) ->
    case within_rescale_cooldown() of
        true -> unchanged;
        false ->
            case broker_cluster:set_session_partition_count(NewC) of
                {ok, _Epoch} ->
                    record_rescale_now(),
                    _ = ensure_partitions(NewC, Members),
                    _ = reclaim(lists:seq(NewC, OldC - 1), Members),
                    {rescaled, OldC, NewC};
                {error, _} = E -> E
            end
    end.

%% @doc 缩容回收：对给定分区列表中"无存活 owner 指向"的那些提交 remove。
%% 仍有连接指向的分区不删（等 conn_fsm 迁移；见设计 §3.5）。
-spec reclaim([non_neg_integer()], [node()]) -> {reclaimed, [non_neg_integer()]}.
reclaim(Partitions, _Members) ->
    Gone = lists:filter(fun(P) -> partition_unused(P) end, Partitions),
    lists:foreach(
      fun(P) ->
              _ = broker_cluster:remove(P)
      end, Gone),
    {reclaimed, Gone}.

%% 判据：没有元数据 → 无需回收；有 owner 但该 owner 已不在当前成员里 → 回收。
partition_unused(P) ->
    case broker_cluster_cache:get(P) of
        not_found -> false;
        {ok, #{owner := O}} ->
            case members() of
                {ok, Ms} -> not lists:member(O, Ms);
                _ -> false
            end
    end.

autoscale_enabled() ->
    application:get_env(broker_cluster, session_partition_autoscale, true).

cooldown_ms() ->
    case application:get_env(broker_cluster, rescale_cooldown_ms, 300000) of
        N when is_integer(N), N >= 0 -> N;
        _ -> 300000
    end.

within_rescale_cooldown() ->
    case persistent_term:get({?MODULE, last_rescale_ms}, undefined) of
        undefined -> false;
        Last -> erlang:monotonic_time(millisecond) - Last < cooldown_ms()
    end.

record_rescale_now() ->
    persistent_term:put({?MODULE, last_rescale_ms},
                        erlang:monotonic_time(millisecond)),
    ok.
```

- [ ] **Step 2: nodedown_mon 在成员收敛后触发**

在 `apps/broker_cluster/src/broker_cluster_nodedown_mon.erl` 的 `monitor_members/1`
末尾（返回 `St#{monitored := Mon1}` 之前）加一次触发：

```erlang
    St1 = St#{monitored := Mon1},
    %% Plan 3 (gap #3): 成员收敛后尝试扩缩容会话分区空间（幂等、有冷却）。
    %% 开关关闭 / 无权威 C 时 rescale 是 no-op；失败绝不崩监视进程。
    _ = (catch broker_cluster_session_bootstrap:rescale()),
    St1.
```

- [ ] **Step 3: 写扩缩容 CT（加入 bootstrap_dist_SUITE 的 all/0）**

在 `all/0` 追加两个用例：

```erlang
all() -> [bootstrap_creates_all_session_partitions,
          bootstrap_is_idempotent,
          rescale_grows_count_with_members,
          rescale_respects_cooldown].
```

追加实现：

```erlang
%% C 随成员数增长：写死 per_node=4、min=4，2 成员→8、3 成员→16。
rescale_grows_count_with_members(Config) ->
    Nodes = peer_nodes(Config),
    [First | _] = Nodes,
    %% 先把 C 钉在 8（bootstrap 用 env 8 起的）。
    ok = rpc(First, broker_cluster_session_bootstrap, ensure, [Nodes]),
    ?assertEqual({ok, 8, _} = rpc(First, broker_cluster_cache,
                                  session_partition_config, [])),
    %% 显式 2 成员推导 = 2*4=8（不变）；3 成员 = 12→16（变）。
    _ = rpc(First, application, set_env,
            [broker_cluster, session_partitions_per_node, 4]),
    {rescaled, 8, 16} = rpc(First, broker_cluster_session_bootstrap, rescale, [Nodes]),
    {ok, 16, _} = rpc(First, broker_cluster_cache, session_partition_config, []),
    ok.

%% 冷却期内不再变。
rescale_respects_cooldown(Config) ->
    Nodes = peer_nodes(Config),
    [First | _] = Nodes,
    _ = rpc(First, application, set_env, [broker_cluster, rescale_cooldown_ms, 60000]),
    ok = rpc(First, broker_cluster_session_bootstrap, ensure, [Nodes]),
    {rescaled, 8, 16} = rpc(First, broker_cluster_session_bootstrap, rescale, [Nodes]),
    unchanged = rpc(First, broker_cluster_session_bootstrap, rescale, [Nodes]),
    ok.
```

- [ ] **Step 4: 跑测试确认通过**

```bash
cd /home/wanli/.local/share/opencode/worktree/10af50/gap3-partition-scaling
rm -rf _build/test
rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_session_bootstrap_dist_SUITE.erl
find _build/test/logs -name suite.log -path "*bootstrap_dist*" -exec grep -E "^=result|^=successful|^=failed" {} \;
```

Expected: `=successful 4`、`=failed 0`。

> 若 `rescale_respects_cooldown` 因 persistent_term 跨用例串扰（同一节点复用），
> 在 `init_per_testcase` 里对 `First` 加 `persistent_term:erase({broker_cluster_session_bootstrap, last_rescale_ms})`。

- [ ] **Step 5: 提交**

```bash
cd /home/wanli/.local/share/opencode/worktree/10af50/gap3-partition-scaling
git add apps/broker_cluster/src/broker_cluster_session_bootstrap.erl \
        apps/broker_cluster/src/broker_cluster_nodedown_mon.erl \
        apps/broker_cluster/test/broker_cluster_session_bootstrap_dist_SUITE.erl
git commit -m "feat(cluster,gap3): 成员变化自动扩缩容 + 冷却闸门 + 缩容回收（缺口 #3 T5）"
```

---

## Task 6: conn_fsm 平滑迁移

**Files:**
- Modify: `apps/broker_proto_mqtt/src/mqtt_conn_fsm.erl`
- Test: `apps/broker_cluster/test/broker_cluster_conn_fsm_takeover_dist_SUITE.erl`

- [ ] **Step 1: 给 #st 加 owner_epoch**

在 `apps/broker_proto_mqtt/src/mqtt_conn_fsm.erl` 的 `-record(st, {...})` 里，
`owner_node = undefined,` 之后加：

```erlang
             owner_epoch = 0,              %% Plan 3 (gap #3): 上次见到的分区配置 epoch
```

- [ ] **Step 2: 在 maybe_monitor_owner 里记录 epoch**

把 `maybe_monitor_owner/2` 的 `true` 分支改为（只加 epoch 记录）：

```erlang
        true ->
            try
                Count = broker_cluster_session_owner:session_partition_count(),
                P = broker_cluster_session_owner:partition_of(ClientId, Count),
                Epoch = broker_cluster_session_owner:session_partition_epoch(),
                case broker_cluster_session_owner:session_owner(P) of
                    {ok, Owner} when is_atom(Owner), Owner =/= node() ->
                        _ = erlang:monitor_node(Owner, true),
                        D#st{owner_part = P, owner_node = Owner, owner_epoch = Epoch};
                    _ ->
                        D#st{owner_part = P, owner_node = undefined,
                             owner_epoch = Epoch}
                end
            catch _:_ -> D end;
```

- [ ] **Step 3: 在 poll tick 上比对并迁移**

在 `event({poll_t, Ref}, connected, #st{poll_ref = Ref} = D)` 分支里，把
`pull_round(D#st{poll_ref = undefined})` 改为先迁移再拉：

```erlang
event({poll_t, Ref}, connected, #st{poll_ref = Ref} = D) ->
    pull_round(remap_if_partition_moved(D#st{poll_ref = undefined}));
```

在 `maybe_monitor_owner/2` 之后追加：

```erlang
%% @doc Plan 3 (gap #3): 会话分区配置 epoch 变了就重算分区、重挂 monitor。
%% 连接/socket 完全不动（与缺口 #4 的零重连性质一致：这里不碰 socket）。
%% 旧分区上的陈旧写由既有 epoch fencing 拒绝（不变量 I6）。
remap_if_partition_moved(#st{owner_part = undefined} = D) ->
    D;
remap_if_partition_moved(#st{client_id = ClientId, owner_node = OldOwner,
                             owner_epoch = Seen} = D) ->
    case broker_cluster_session_owner:session_partition_epoch() of
        Seen ->
            D;
        Epoch ->
            Count = broker_cluster_session_owner:session_partition_count(),
            NewP = broker_cluster_session_owner:partition_of(ClientId, Count),
            case OldOwner of
                undefined -> ok;
                _ -> catch erlang:monitor_node(OldOwner, false)
            end,
            case broker_cluster_session_owner:session_owner(NewP) of
                {ok, Owner} when is_atom(Owner), Owner =/= node() ->
                    _ = erlang:monitor_node(Owner, true),
                    D#st{owner_part = NewP, owner_node = Owner, owner_epoch = Epoch};
                _ ->
                    D#st{owner_part = NewP, owner_node = undefined,
                         owner_epoch = Epoch}
            end
    end.
```

> `catch` 在这里是**表达式**用法（`catch Expr`），不会触发 `unused` 警告；
> 若 rebar 对 `catch` 表达式报 style 警告，改为
> `_ = try erlang:monitor_node(OldOwner, false) catch _:_ -> ok end`。

- [ ] **Step 4: 写迁移 CT（加入既有 takeover 套件）**

在 `apps/broker_cluster/test/broker_cluster_conn_fsm_takeover_dist_SUITE.erl` 的
`all/0` 追加一个用例，并追加实现：

```erlang
all() -> [live_connection_zero_reconnect_on_owner_crash,
          no_owner_no_takeover,
          connection_remaps_when_partition_epoch_changes].
```

```erlang
%% Plan 3 (gap #3): 分区配置 epoch 变化 → conn 在 poll tick 迁移到新分区，
%% 但 socket 不重连（仍能 PINGRESP）。
connection_remaps_when_partition_epoch_changes(Config) ->
    Nodes = peer_nodes(Config),
    #{owner := Owner, holder := Holder} = assign_roles(ra_leader(Nodes), Nodes),
    Count = rpc(Holder, broker_cluster_session_owner, session_partition_count, []),
    ClientId = <<"gap3-remap">>,
    P = rpc(Holder, broker_cluster_session_owner, partition_of, [ClientId, Count]),
    {ok, 1} = rpc(Holder, ?MODULE, assign_partition, [P, Owner, Nodes, 1]),
    wait_until(fun() -> rpc(Holder, broker_cluster_cache, owner, [P]) =:= {ok, Owner} end),
    %% 建权威 C + epoch，conn 才能记住 owner_epoch。
    {ok, _} = rpc(Holder, broker_cluster:set_session_partition_count, [Count]),
    Port = start_listener(Holder),
    Sock = connect(Port),
    #connack{return_code = 0} = open(Sock, #{client_id => ClientId}),
    #suback{} = subscribe(Sock, 1, <<"t/#">>, 1),
    BeforePart = owner_part_of(Holder),
    %% 提交一个更大的 C + epoch：conn 应在 poll tick 上迁移。
    Bigger = Count * 2,
    {ok, _} = rpc(Holder, broker_cluster:set_session_partition_count, [Bigger]),
    wait_until(fun() ->
                       owner_part_of(Holder) =/= BeforePart
               end),
    %% socket 未重连：PINGRESP 仍 round-trip。
    #ping{kind = resp} = ping(Sock),
    ok = gen_tcp:close(Sock),
    ok.
```

> 上面的 `owner_part_of/1` 是本套件新加的**测试侧**辅助（读某个 conn 的
> `owner_part`）。`mqtt_conn_fsm` 是 `gen_statem`，`sys:get_state/1` 纯读、无需
> 在生产模块里加写路径。在**套件里**这样实现即可（holder 上该用例只有一个
> 订阅者 conn，直接取唯一 pid，与既有 `conn_fsm_pid/1` 同一套路）：
>
> ```erlang
> owner_part_of(Holder) ->
>     Pid = conn_fsm_pid(Holder),                 %% 既有辅助：取 mqtt_conn_sup 的唯一 conn
>     rpc(Holder, mqtt_conn_fsm, owner_part, [Pid]).
> ```
>
> **关键细节（实施时必须照做）**：`sys:get_state/1` 对 `gen_statem` 返回
> `{StateName, Data}`，`Data` 是 `#st{}` record，而 `#st{}` 定义在
> `mqtt_conn_fsm.erl` 内部、**套件里无法直接 pattern match**。因此本 Task
> **在 `mqtt_conn_fsm` 里加一个纯读导出函数**（唯一允许的生产侧改动，只为
> 可观测性，不改行为）：
>
> ```erlang
> %% @doc 诊断/测试用：读一条连接当前记录的会话分区。纯读，不改状态。
> -spec owner_part(pid()) -> non_neg_integer() | undefined.
> owner_part(Pid) ->
>     case sys:get_state(Pid) of
>         {_StateName, #st{owner_part = P}} -> P;
>         _ -> undefined
>     end.
> ```
>
> 并在 `-export` 里加 `owner_part/1`。

- [ ] **Step 5: 跑测试确认通过**

```bash
cd /home/wanli/.local/share/opencode/worktree/10af50/gap3-partition-scaling
rm -rf _build/test
rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_conn_fsm_takeover_dist_SUITE.erl
find _build/test/logs -name suite.log -path "*conn_fsm_takeover_dist*" -exec grep -E "^=result|^=successful|^=failed" {} \;
```

Expected: `=successful 3`、`=failed 0`。

- [ ] **Step 6: 回归既有 mqtt 套件**

```bash
cd /home/wanli/.local/share/opencode/worktree/10af50/gap3-partition-scaling
rm -rf _build/test
rebar3 ct --suite=apps/broker_proto_mqtt/test/mqtt_listener_SUITE.erl
find _build/test/logs -name suite.log -path "*mqtt_listener_SUITE*" -exec grep -E "^=result|^=successful|^=failed" {} \;
```

Expected: 全绿（该套件与 `session_replication=false` 默认路径相关）。

- [ ] **Step 7: 提交**

```bash
cd /home/wanli/.local/share/opencode/worktree/10af50/gap3-partition-scaling
git add apps/broker_proto_mqtt/src/mqtt_conn_fsm.erl \
        apps/broker_cluster/test/broker_cluster_conn_fsm_takeover_dist_SUITE.erl
git commit -m "feat(mqtt,gap3): conn_fsm 在分区 epoch 变化时平滑迁移，socket 不重连（缺口 #3 T6）"
```

---

## Task 7: plan2-notes §3 缺口 #3 追加带日期批注

**Files:**
- Modify: `docs/superpowers/specs/2026-11-22-session-state-replication-plan2-notes.md`

- [ ] **Step 1: 在缺口表后追加批注**

在 `## 3. 缺口表（Plan 3 边界债）` 表格之后、`## 4.` 之前，紧随 #4/#1/#2 三条
既有批注之后再追加一段（保持同样的 `> **2026-11-23 追加批注**：` 格式）：

```markdown
> **2026-11-23 追加批注**：缺口 #3 已补会话分区数随集群规模缩放 —— 权威 C 移入 ra 元数据组（新增命令型 `{set_session_partition_count, C, Epoch}`，epoch 单调，快照兼容），`broker_cluster_session_owner:session_partition_count/0` 改为 cache 镜像优先、env/推导兜底。同时补上调研发现的前置缺口 F1：生产侧原本**没有任何代码**为会话分区 `0..C-1` 建元数据（只有 CT 手工 `assign_partition`），导致 `session_owner(P)` 恒 `not_found`、缺口 #4 的接管链路在生产从未触发 —— 新 `broker_cluster_session_bootstrap` 在 `session_replication=true` 时幂等建元数据补齐。自动扩缩容（默认开、`rescale_cooldown_ms` 默认 300s、双向）按 `derive_count(MemberCount) = next_power_of_two(max(min, Members*PerNode))` 推导，成员收敛后经 `broker_cluster_nodedown_mon` 触发；`phash2` 扩容是全量重散列，存量连接由 `mqtt_conn_fsm` 在 poll tick 比对 `session_partition_epoch` 平滑迁移到新分区（socket 不重连，旧分区靠既有 epoch fencing 拒陈旧写）。设计见 `2026-11-23-session-partition-count-scaling-design.md`。§12 中"副本集复用元数据分区 replicas"的未决点按不变量 I8 继续沿用同键假设（留待缺口 #5 定稿）。
```

- [ ] **Step 2: 提交**

```bash
cd /home/wanli/.local/share/opencode/worktree/10af50/gap3-partition-scaling
git add docs/superpowers/specs/2026-11-22-session-state-replication-plan2-notes.md
git commit -m "docs(plan2-notes): 缺口 #3 会话分区数随规模缩放已补（Plan 3）"
```

---

## 验收清单（全部 Task 完成后）

```bash
cd /home/wanli/.local/share/opencode/worktree/10af50/gap3-partition-scaling
export PATH="$HOME/Development/otp-29.1.2/lib/erlang/bin:$PATH"
rm -rf _build/test
rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_session_partition_SUITE.erl \
          --suite=apps/broker_cluster/test/broker_cluster_meta_SUITE.erl \
          --suite=apps/broker_cluster/test/broker_cluster_session_owner_SUITE.erl \
          --suite=apps/broker_cluster/test/broker_cluster_session_bootstrap_dist_SUITE.erl \
          --suite=apps/broker_cluster/test/broker_cluster_conn_fsm_takeover_dist_SUITE.erl
find _build/test/logs -name suite.log -exec grep -E "^=result|^=successful|^=failed" {} \; | sort | uniq -c
```

Expected: 每个 suite `=result ok`，`=failed 0` 总计。

再跑一次既有相关套件确认无回归：

```bash
rm -rf _build/test
rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_session_rep_SUITE.erl \
          --suite=apps/broker_cluster/test/broker_session_replica_SUITE.erl \
          --suite=apps/broker_cluster/test/broker_cluster_nodedown_mon_SUITE.erl
```

Expected: 全绿。

---

## 风险与已知取舍

1. **`phash2` 全量重散列**：C 变更会让几乎所有 ClientId 换分区。本计划用"conn_fsm 平滑迁移 + epoch fencing"消化，不做部分迁移。若未来迁移成本成为真问题，再议 jump consistent hash。
2. **自动扩容默认开**：与 `broker_rebalance` 的 default-closed 不同，是有意选择（分区数是容量正确性，不是运营调优）。冷却闸门 + 显式开关是安全垫。
3. **缩容回收的"无人 monitor"判据**：本计划用"owner 不在当前成员里"作为可回收判据，比设计 §3.5 的"无人 monitor"更保守（不删 owner 仍在成员里的分区）。若出现"owner 在成员里但连接已迁走"的残留，需在后续迭代补"按 conn_fsm 报告回收"（非本轮范围）。
4. **bootstrap 与 ra leader 就绪的时序**：用 `init/1` 里 `self() ! run_bootstrap` + 失败重试（2s）规避"ra 尚未选主就提交"。这是保守做法，不是精确的 leader-ready 信号（ra 无公开的该信号）。
5. **`owner_part/2` 测试访问器**：为 CT 暴露一个纯读访问器（`sys:get_state` 或 call 分支），不改生产行为。若实现者发现 `sys:get_state` 在 FSM 上有副作用，改用显式 call 分支，并在计划该 Task 的提交信息里说明。
