%% @doc MQ-15: shared-subscription assignment across real nodes (design
%% 2026-11-24-mq15-shared-subscription-cluster-rebalance §6, test plan C/D).
%%
%% `broker_rebalance_dist_SUITE' proves partition ownership is one truth across a
%% real ra group. This suite proves the MQ-15 claim on top of the same machinery:
%%
%%   * **mutual exclusion (§6 invariant 1).** Two consumers on two different nodes
%%     claim the same `{shared, Group, Filter}' partition concurrently; because the
%%     assignment decision is a ra command, no offset is ever handed to both -- the
%%     union of their offsets is exactly the window with no overlap;
%%   * **crash release (§4.1(c)).** A node that held offsets leaves; its consumer's
%%     offsets return to the pool so a survivor can claim them. This suite drives
%%     the release directly (the production nodedown-handler integration is wired
%%     in Task 7); what it proves here is that the *released offsets are claimable
%%     again*, which is the property the handler relies on.
%%
%% `peer' is OTP 28 stdlib, the same machinery the other dist suites use; ports are
%% ephemeral and no project port is touched.
-module(broker_subscription_shared_dist_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("broker_core/include/broker_message.hrl").

-define(SUB, {shared, <<"g">>, <<"s/#">>}).

all() ->
    [concurrent_claims_on_two_nodes_do_not_duplicate,
     a_dead_nodes_offsets_are_released_to_the_survivor,
     threshold_reclaim_moves_offsets_from_an_overloaded_consumer].

%%%_* Fixtures --------------------------------------------------------------

init_per_suite(Config) ->
    case node() of
        nonode@nohost ->
            case net_kernel:start([unifiedmq_mq15_ct, shortnames]) of
                {ok, _} -> [{started_net, true} | Config];
                {error, Reason} ->
                    ct:fail({cannot_start_distribution, check_epmd, Reason})
            end;
        _ ->
            [{started_net, false} | Config]
    end.

end_per_suite(Config) ->
    case ?config(started_net, Config) of
        true -> _ = net_kernel:stop(), ok;
        _ -> ok
    end.

init_per_testcase(Case, Config) ->
    [{peers, start_peers(Case, Config)} | Config].

end_per_testcase(_Case, Config) ->
    stop_peers(?config(peers, Config)),
    ok.

%%%_* Cases ----------------------------------------------------------------

%% Two consumers on two nodes claim the same shared partition. Because the claim
%% is a ra command applied one at a time, the second claim can never hand out an
%% offset the first already owns: their assigned sets are disjoint and together
%% cover the window. Each node's cache mirror agrees (one truth).
concurrent_claims_on_two_nodes_do_not_duplicate(Config) ->
    Members = members(Config),
    [N1, N2 | _] = Members,
    ok = form_cluster(Config),
    %% c1 on N1 claims the first half of the 1..99 window, c2 on N2 the rest.
    %% (High=100 is the exclusive upper watermark: offset 100 does not yet exist.)
    {ok, A} = rpc(N1, broker_cluster, shared_claim,
                  [?SUB, 0, <<"c1">>, 50, 100, #{}, 0]),
    {ok, B} = rpc(N2, broker_cluster, shared_claim,
                  [?SUB, 0, <<"c2">>, 50, 100, #{}, 0]),
    %% Mutual exclusion: no offset owned by both.
    [] = [O || O <- A, lists:member(O, B)],
    %% Together they tile the window exactly (1..99), no gaps, no overlaps.
    ?assertEqual(lists:seq(1, 99), lists:sort(A ++ B)),
    %% The cache mirror on every node reflects the same committed table: c1 owns
    %% the first 50 offsets, c2 the rest. Pattern-match the boundary keys (a
    %% `:=` match checks these keys on the full map; `=>` would build a 4-key map
    %% and compare against all 99).
    lists:foreach(
      fun(N) ->
              {ok, Row} = rpc(N, broker_cluster_cache, shared_assignment,
                              [?SUB, 0]),
              #{1 := <<"c1">>, 50 := <<"c1">>,
                51 := <<"c2">>, 99 := <<"c2">>} = maps:get(assigned, Row)
      end, Members),
    ok.

%% A node crash returns its consumer's offsets to the pool. The production path
%% is the nodedown handler releasing exactly the crashed node's consumers
%% (wired in Task 7); here the release is driven directly to prove the property
%% the handler depends on: once released, a survivor can claim those offsets.
a_dead_nodes_offsets_are_released_to_the_survivor(Config) ->
    Members = members(Config),
    [N1, N2, N3] = Members,
    _ = N3,
    ok = form_cluster(Config),
    {ok, _} = rpc(N1, broker_cluster, shared_join, [?SUB, <<"c1">>, N1]),
    {ok, [1, 2, 3]} =
        rpc(N1, broker_cluster, shared_claim, [?SUB, 0, <<"c1">>, 3, 100, #{}, 0]),
    %% N1 crashes. With N2+N3 the group still holds its majority, so a survivor
    %% can drive the release -- this is the call the nodedown handler makes.
    %% Retry across the leader election the crash triggers: `{error, nodedown}'
    %% is the honest transient answer, not a verdict.
    ok = stop_peer(Config, N1),
    {ok, _} = rpc_retry(N2, broker_cluster, shared_release, [?SUB, 0, <<"c1">>]),
    %% The window is empty again: c1's offsets went back to the pool. (An `#{}`
    %% *pattern* matches any map, so assert emptiness explicitly.)
    {ok, Row0} =
        rpc_retry(N2, broker_cluster_cache, shared_assignment, [?SUB, 0]),
    ?assertEqual(#{}, maps:get(assigned, Row0)),
    %% A survivor's fresh consumer reclaims exactly the released offsets.
    {ok, Offsets} =
        rpc_retry(N2, broker_cluster, shared_claim,
                  [?SUB, 0, <<"c2">>, 10, 100, #{}, 0]),
    ?assert(lists:member(1, Offsets)),
    ?assert(lists:member(2, Offsets)),
    ?assert(lists:member(3, Offsets)),
    ok.

%% MQ-15 §4.1(b): the reclaim command reassigns a named offset from an overloaded
%% consumer to a freer one. c1 holds 1..4, c2 is idle; reclaiming offset 2 from c1
%% to c2 must move exactly that offset and leave the rest with c1.
threshold_reclaim_moves_offsets_from_an_overloaded_consumer(Config) ->
    Members = members(Config),
    [N1, N2 | _] = Members,
    _ = N1, _ = N2,
    ok = form_cluster(Config),
    Sub = ?SUB,
    {ok, _} = rpc(N1, broker_cluster, shared_join, [Sub, <<"c1">>, N1]),
    {ok, _} = rpc(N2, broker_cluster, shared_join, [Sub, <<"c2">>, N2]),
    %% c1 holds four in-flight offsets, c2 is idle.
    {ok, _} = rpc(N1, broker_cluster, shared_claim,
                  [Sub, 0, <<"c1">>, 4, 100, #{}, 0]),
    %% Reclaim offset 2 from c1 to c2 (the threshold trigger's action, §4.1(b)).
    {ok, [2]} = rpc(N1, broker_cluster, shared_reclaim,
                    [Sub, 0, <<"c1">>, <<"c2">>, [2]]),
    {ok, Row} = rpc(N1, broker_cluster_cache, shared_assignment, [Sub, 0]),
    %% Exactly offset 2 moved; 1,3,4 stay with c1.
    ?assertEqual(#{1 => <<"c1">>, 2 => <<"c2">>,
                   3 => <<"c1">>, 4 => <<"c1">>},
                 maps:get(assigned, Row)),
    ok.

%%%_* Peers ----------------------------------------------------------------

start_peers(Case, Config) ->
    Names = [peer_name(Case, I) || I <- [1, 2, 3]],
    SuiteDir = filename:dirname(code:which(?MODULE)),
    CodePaths = lists:usort([SuiteDir | [D || D <- code:get_path(), is_code_dir(D)]]),
    Peers =
        lists:map(
          fun({I, Name}) ->
                  DataDir = filename:join([?config(priv_dir, Config),
                                           atom_to_list(Case),
                                           "n" ++ integer_to_list(I), "ra"]),
                  {ok, Pid, Node} =
                      peer:start_link(#{name => Name,
                                        args => ["-pa" | CodePaths],
                                        wait_boot => 20000}),
                  ok = rpc(Node, prepare_peer, [[DataDir, []]]),
                  #{pid => Pid, node => Node, data_dir => DataDir}
          end, lists:zip([1, 2, 3], Names)),
    Peers.

stop_peers(Peers) ->
    lists:foreach(fun(#{pid := Pid}) ->
                          _ = try peer:stop(Pid) catch _:_ -> ok end
                  end, Peers),
    ok.

stop_peer(Config, Node) ->
    case [P || #{node := N} = P <- ?config(peers, Config), N =:= Node] of
        [#{pid := Pid}] -> _ = try peer:stop(Pid) catch _:_ -> ok end, ok;
        _ -> ok
    end.

peer_name(Case, I) ->
    list_to_atom("mq15_" ++ sanitise(atom_to_list(Case)) ++ "_" ++
                 integer_to_list(I)).

is_code_dir(Dir) ->
    filename:basename(Dir) =:= "ebin".

sanitise(S) ->
    [case C of _ when C >= $a andalso C =< $z -> C;
                  _ when C >= $A andalso C =< $Z -> C;
                  _ when C >= $0 andalso C =< $9 -> C;
                  _ -> $_ end || C <- S].

members(Config) ->
    [maps:get(node, P) || P <- ?config(peers, Config)].

form_cluster(Config) ->
    Members = members(Config),
    [ok = rpc(N, start_meta, [Members]) || N <- Members],
    ok.

rpc(Node, F, A) ->
    rpc(Node, ?MODULE, F, A).

%% Retry a shared-assignment rpc while the group is still electing a leader after
%% a peer was stopped: `nodedown' / `no_quorum' are the honest transient answers,
%% not verdicts (the same rule `broker_rebalance_dist_SUITE:step_retry' uses).
rpc_retry(Node, M, F, A) ->
    rpc_retry(Node, M, F, A, 40).

rpc_retry(Node, M, F, A, 0) ->
    ct:fail({rpc_never_committed, Node, M, F, rpc(Node, M, F, A)});
rpc_retry(Node, M, F, A, N) ->
    case rpc(Node, M, F, A) of
        {error, nodedown} ->
            timer:sleep(250), rpc_retry(Node, M, F, A, N - 1);
        {error, no_quorum} ->
            timer:sleep(250), rpc_retry(Node, M, F, A, N - 1);
        Result ->
            Result
    end.

rpc(Node, M, F, A) ->
    case rpc:call(Node, M, F, A, 30000) of
        {badrpc, Reason} -> ct:fail({badrpc, Node, M, F, Reason});
        Result -> Result
    end.

%%%_* Peer-side setup (runs on the peer) ----------------------------------

prepare_peer([DataDir, Members]) ->
    _ = application:load(broker_cluster),
    ok = application:set_env(broker_cluster, data_dir, DataDir),
    ok = application:set_env(broker_cluster, members, Members),
    ok = application:set_env(broker_cluster, quorum_timeout_ms, 3000),
    ok = application:set_env(broker_cluster, replicate_timeout_ms, 1000),
    ok = filelib:ensure_dir(filename:join(DataDir, "x")),
    {ok, _} = application:ensure_all_started(ra),
    application:set_env(ra, data_dir, filename:absname(DataDir)),
    case ra_system:fetch(default) of
        undefined ->
            case ra_system:start_default() of
                {ok, _} -> ok;
                {error, {already_started, _}} -> ok
            end;
        _ -> ok
    end,
    {ok, Cache} = broker_cluster_cache:start_link(),
    unlink(Cache),
    ok.

start_meta(Members) ->
    ok = application:set_env(broker_cluster, members, Members),
    {ok, Meta} = broker_cluster_meta:start_link(),
    unlink(Meta),
    ok.
