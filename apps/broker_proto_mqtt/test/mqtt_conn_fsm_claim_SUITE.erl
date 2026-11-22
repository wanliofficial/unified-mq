%% @doc conn_fsm claim 触发的可测纯逻辑（Plan 2 Task 5）。
%% 验证「给定 owner 视图 + 死亡节点 + 本节点持连集合，算出该 claim 哪些
%% {ClientId, Partition}」。端到端 nodedown→续传由 Task 6 的 dist-CT 覆盖。
-module(mqtt_conn_fsm_claim_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() -> [claims_only_partitions_owned_by_dead_node,
          no_claim_when_owner_alive,
          no_claim_for_unheld_clients].

init_per_testcase(_C, Config) -> Config.
end_per_testcase(_C, _Config) -> ok.

%% c1→P0(owner n2,死), c2→P0(owner n2,死), c3→P1(owner n3,活)。
%% 本节点持连 {c1,c3}。应只 claim {c1,P0}。
claims_only_partitions_owned_by_dead_node(_Config) ->
    View = #{<<"c1">> => {0, n2}, <<"c2">> => {0, n2}, <<"c3">> => {1, n3}},
    ?assertEqual([{<<"c1">>, 0}],
                 mqtt_conn_fsm:claims_to_make(View, n2, [<<"c1">>, <<"c3">>])),
    ok.

%% 死亡节点不是任何会话的 owner → 无 claim。
no_claim_when_owner_alive(_Config) ->
    View = #{<<"c1">> => {0, n2}},
    ?assertEqual([], mqtt_conn_fsm:claims_to_make(View, n9, [<<"c1">>])),
    ok.

%% owner 死了但本节点不持连该 client → 不 claim（连接不在本地）。
no_claim_for_unheld_clients(_Config) ->
    View = #{<<"c1">> => {0, n2}},
    ?assertEqual([], mqtt_conn_fsm:claims_to_make(View, n2, [])),
    ok.
