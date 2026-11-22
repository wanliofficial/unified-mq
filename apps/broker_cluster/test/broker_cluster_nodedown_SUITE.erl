%% @doc nodedown 成员监视 + 迁移触发（Plan 2 Task 4,spec §4）。
%% 单元驱动 broker_cluster_nodedown:pick_new_owner/2：对已分配给死亡节点的
%% 分区，选出新 owner（副本集中非死亡、非本节点的第一个存活副本）并给出 epoch+1。
-module(broker_cluster_nodedown_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() -> [selects_alive_replica_as_new_owner,
          no_alive_replica_when_only_dead,
          excludes_this_node_from_candidates,
          prefers_first_alive_in_replica_order].

init_per_testcase(_C, Config) ->
    case broker_cluster_cache:start_link() of
        {ok, C} -> unlink(C);
        {error, {already_started, _}} -> ok
    end,
    _ = broker_cluster_cache:clear(),
    Config.
end_per_testcase(_C, _Config) ->
    _ = broker_cluster_cache:clear(),
    _ = try gen_server:stop(broker_cluster_cache) catch _:_ -> ok end,
    ok.

%% 分区 0：owner=n1（死亡），replicas=[n1,n2,n3]。死亡 n1 后应选 n2，epoch+1。
selects_alive_replica_as_new_owner(_Config) ->
    ok = broker_cluster_cache:put(0, #{partition => 0, owner => n1,
                                        epoch => 1, replicas => [n1, n2, n3],
                                        state => active, index => 0}),
    {ok, NewOwner, NewEpoch} = broker_cluster_nodedown:pick_new_owner(0, n1),
    ?assertEqual(n2, NewOwner),
    ?assertEqual(2, NewEpoch),
    ok.

%% 所有副本都死亡 → {error, no_alive_replica}。
no_alive_replica_when_only_dead(_Config) ->
    ok = broker_cluster_cache:put(1, #{partition => 1, owner => n1,
                                        epoch => 3, replicas => [n1],
                                        state => active, index => 0}),
    ?assertEqual({error, no_alive_replica},
                 broker_cluster_nodedown:pick_new_owner(1, n1)),
    ok.

%% 副本顺序决定新 owner：取第一个非死亡、非本节点的存活副本（n3），
%% 即副本序 [n3,n1,n2] 里的第一个存活者 n3。
prefers_first_alive_in_replica_order(_Config) ->
    ok = broker_cluster_cache:put(2, #{partition => 2, owner => n1,
                                        epoch => 1, replicas => [n3, n1, n2],
                                        state => active, index => 0}),
    {ok, NewOwner, 2} = broker_cluster_nodedown:pick_new_owner(2, n1),
    ?assertEqual(n3, NewOwner),
    ok.

%% 本节点也在副本集里时也必须被排除：replicas=[dead, node()] 下，dead 已死、
%% node() 是本节点 → 没有其它存活副本 → {error, no_alive_replica}。
excludes_this_node_from_candidates(_Config) ->
    ok = broker_cluster_cache:put(3, #{partition => 3, owner => dead,
                                        epoch => 1, replicas => [dead, node()],
                                        state => active, index => 0}),
    ?assertEqual({error, no_alive_replica},
                 broker_cluster_nodedown:pick_new_owner(3, dead)),
    ok.
