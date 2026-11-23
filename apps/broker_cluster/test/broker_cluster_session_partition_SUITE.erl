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
%% 3 节点：max(4, 3*4)=12 → 向上取 2 的幂 = 16。
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
