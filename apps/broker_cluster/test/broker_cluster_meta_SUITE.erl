%% @doc 元数据 Raft 状态机 claim 命令（Plan 2 Task 3）。
%% 单元驱动 broker_cluster_meta_machine:apply/3：epoch 单调 + 持副本守卫（ADR-034 D7）。
-module(broker_cluster_meta_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() -> [claim_moves_owner_when_holding_replica,
          claim_rejects_node_without_replica,
          claim_rejects_stale_epoch,
          claim_rejects_non_integer_epoch].

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
