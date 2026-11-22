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
    _ = application:load(broker_cluster),
    {ok, Cache} = broker_cluster_cache:start_link(),
    unlink(Cache),
    [{count, 4} | Config].
end_per_testcase(_Case, _Config) ->
    _ = try gen_server:stop(broker_cluster_cache) catch _:_ -> ok end,
    ok.

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

%% 多个 ClientId 不全落同一分区（散列出区分）。
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
    ?assertEqual({ok, node()},
                 broker_cluster_session_owner:session_owner(0)),
    ok.
