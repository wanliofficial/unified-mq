%% @doc 元数据 Raft 状态机 claim 命令（Plan 2 Task 3）。
%% 单元驱动 broker_cluster_meta_machine:apply/3：epoch 单调 + 持副本守卫（ADR-034 D7）。
-module(broker_cluster_meta_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() -> [claim_moves_owner_when_holding_replica,
          claim_rejects_node_without_replica,
          claim_rejects_stale_epoch,
          claim_rejects_non_integer_epoch,
          set_count_commits_and_mirrors,
          set_count_rejects_stale_epoch,
          set_count_rejects_bad_count,
          old_snapshot_without_count_fields_is_tolerated].

%% 同 broker_cluster_session_owner_SUITE 的约定：apply/3 会镜像到本地 cache，
%% 起一个 cache 让侧写落地（put/2 在无表时也只是 no-op，起进程只是保持
%% 测试环境与其它套件一致、并在用例间清干净）。
init_per_testcase(_C, Config) ->
    _ = application:load(broker_cluster),
    case broker_cluster_cache:start_link() of
        {ok, Cache} ->
            unlink(Cache),
            ok;
        {error, {already_started, _}} ->
            ok
    end,
    %% 起点干净：Fix 2 断言 claim 失败后 cache 未被写（not_found）。
    _ = broker_cluster_cache:clear(),
    Config.
end_per_testcase(_C, _Config) ->
    _ = try broker_cluster_cache:clear() catch _:_ -> ok end,
    _ = try gen_server:stop(broker_cluster_cache) catch _:_ -> ok end,
    ok.

claim_moves_owner_when_holding_replica(_Config) ->
    %% 迁移中（state=migrating）的分区被 claim 抢占：owner 迁到 n2，epoch=2，
    %% state 强制回 active，index 更新；replicas 保留；cache 镜像同步。
    S0 = #{partitions => #{0 => #{owner => n1, epoch => 1,
                                  replicas => [n1, n2], state => migrating,
                                  index => 0}}},
    {S1, {ok, 2}, []} = broker_cluster_meta_machine:apply(
                         #{index => 5}, {claim, 0, n2, 2}, S0),
    M1 = maps:get(0, maps:get(partitions, S1)),
    ?assertMatch(#{owner := n2, epoch := 2, state := active, index := 5,
                   replicas := [n1, n2]}, M1),
    %% cache 镜像同步：authorize_append/2 读 cache，必须随之更新。
    ?assertEqual({ok, n2}, broker_cluster_cache:owner(0)),
    ?assertEqual({ok, 2}, broker_cluster_cache:epoch(0)),
    ok.

claim_rejects_node_without_replica(_Config) ->
    S0 = #{partitions => #{0 => #{owner => n1, epoch => 1,
                                  replicas => [n1], state => active, index => 0}}},
    {S1, {error, {not_a_replica, n2}}, []} = broker_cluster_meta_machine:apply(
                                              #{index => 6}, {claim, 0, n2, 2}, S0),
    ?assertEqual(n1, maps:get(owner, maps:get(0, maps:get(partitions, S1)))),
    ok.

claim_rejects_stale_epoch(_Config) ->
    S0 = #{partitions => #{0 => #{owner => n1, epoch => 5,
                                  replicas => [n1, n2], state => active, index => 0}}},
    {S1, {error, {stale_epoch, 5, 2}}, []} = broker_cluster_meta_machine:apply(
                                                #{index => 7}, {claim, 0, n2, 2}, S0),
    %% 无副作用：状态逐字不变，cache 未被写。
    ?assertEqual(S0, S1),
    ?assertEqual(not_found, broker_cluster_cache:get(0)),
    ok.

%% 非整数 epoch 不得把分区「卡死」（否则合法 claim 永久失败）。
claim_rejects_non_integer_epoch(_Config) ->
    S0 = #{partitions => #{0 => #{owner => n1, epoch => 1,
                                  replicas => [n1, n2], state => active, index => 0}}},
    {S1, {error, {stale_epoch, 1, bogus}}, []} =
        broker_cluster_meta_machine:apply(#{index => 8}, {claim, 0, n2, bogus}, S0),
    ?assertEqual(S0, S1),
    %% 未被 wedge：随后一个合法 claim 仍能成功。
    {_S2, {ok, 2}, []} = broker_cluster_meta_machine:apply(
                          #{index => 9}, {claim, 0, n2, 2}, S1),
    ok.

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

%% 旧快照（无新字段）能容忍：缺字段时 epoch 按 0，命令仍可执行。
old_snapshot_without_count_fields_is_tolerated(_Config) ->
    %% 模拟旧 machine state：只有 partitions，没有 session_partition_* 字段。
    S0 = #{partitions => #{}},
    {S1, {ok, 1}, []} = broker_cluster_meta_machine:apply(
                          #{index => 14}, {set_session_partition_count, 8, 1}, S0),
    ?assertEqual(8, maps:get(session_partition_count, S1)),
    ?assertEqual(1, maps:get(session_partition_epoch, S1)),
    ok.
