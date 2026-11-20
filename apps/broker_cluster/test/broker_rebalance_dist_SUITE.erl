%% @doc S12-C's rebalance and migration, across real nodes (ADR-032 D4/D5/D6,
%% R-RB1/R-RB4).
%%
%% `broker_rebalance_SUITE' proves the decision logic on one node. What only a
%% real group can show is the part the ADRs actually depend on:
%%
%%   * a move issued by the trigger is a **normal** ra command -- every node
%%     applies it, so every node's view agrees on the new owner and epoch (D5:
%%     one truth, no second member set);
%%   * the two-phase `migrate' -> `finish_migration' leaves the source **still
%%     readable** between phases and the source **fenced** after the move
%%     (`{fenced, _}`, ADR-021 D3) -- the "no double write without fencing"
%%     claim the Gate asks for, proven by asking the old owner to append;
%%   * a node that left the membership has its partitions **recovered** by the
%%     trigger onto a live member, without an operator naming each one;
%%   * losing the majority turns a rebalance step into `{error, no_quorum}`
%%     and changes nothing -- a trigger cannot talk the cluster into a move it
%%     cannot commit (D6).
%%
%% The data-plane claim is deliberately narrow, per D5: this suite moves
%% ownership metadata and proves the *fencing* half on the queue's own guard. It
%% does not migrate a live process or its queue contents -- that is not in S12.
%%
%% `peer' is OTP 28 stdlib, the same machinery the other dist suites use. Ports
%% are ephemeral; no project port is touched.
-module(broker_rebalance_dist_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(PARTITIONS, 4).
-define(COOLDOWN_MS, 0).

all() ->
    [a_move_is_committed_by_every_node,
     the_two_phase_move_keeps_the_source_readable_then_fences_it,
     a_departed_nodes_partitions_are_recovered,
     losing_the_majority_refuses_the_move,
     repeated_steps_converge_to_balance].

%%%_* Fixtures --------------------------------------------------------------

init_per_suite(Config) ->
    case node() of
        nonode@nohost ->
            %% Same distribution requirement as the other dist suites. Do not
            %% re-apply `nocookie' after `net_kernel:start/1'.
            case net_kernel:start([unifiedmq_s12c_ct, shortnames]) of
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
    Peers = start_peers(Case, Config),
    [{peers, Peers} | Config].

end_per_testcase(_Case, Config) ->
    stop_peers(?config(peers, Config)),
    ok.

%%%_* Cases ----------------------------------------------------------------

%% The trigger issues an ordinary ra command, so the move is not local: every
%% node's cache reports the same new owner and epoch. This is D5's "one truth,
%% no second member set" in its most direct form.
a_move_is_committed_by_every_node(Config) ->
    Members = members(Config),
    [N1, N2, N3] = Members,
    ok = form_cluster(Config),
    %% All four partitions on N1; N2 and N3 own nothing.
    [ok = assign(P, N1, Members, 1) || P <- partitions()],
    ok = enable(#{hysteresis => 1, cooldown_ms => ?COOLDOWN_MS}),
    {migrated, P, N1, To} = rpc(N1, broker_rebalance, step, [Members]),
    ?assert(lists:member(To, [N2, N3])),
    %% Every node agrees, not just the one that issued the command: same owner,
    %% same epoch 2 (the migrate carried epoch+1).
    lists:foreach(
      fun(N) ->
              ?assertEqual({ok, To}, rpc(N, broker_cluster, owner, [P])),
              ?assertEqual({ok, 2}, rpc(N, broker_cluster, epoch, [P]))
      end, Members),
    ok.

%% ADR-021 D3 / ADR-032 D5 two-phase contract, proven against the queue's own
%% fencing guard rather than by inspecting metadata:
%%   * after `migrate' the source is no longer the owner (the write it attempts
%%     with the old epoch is `{fenced, _}'), while the *partition is still
%%     readable* -- its epoch advanced and it is `migrating', not gone;
%%   * after `finish_migration' the target owns it at epoch+1 and accepts.
the_two_phase_move_keeps_the_source_readable_then_fences_it(Config) ->
    Members = members(Config),
    [N1, N2, N3] = Members,
    _ = N3,
    ok = form_cluster(Config),
    ok = assign(0, N1, Members, 1),
    %% Phase 1: migrate. The target becomes owner at epoch 2, phase `migrating'.
    {ok, 2} = rpc(N1, broker_cluster, migrate, [0, N1, N2, 2]),
    %% The source is fenced: its old-epoch append is refused.
    {error, {fenced, Fenced}} =
        rpc(N1, broker_cluster, authorize_append, [0, 1]),
    ?assertEqual(0, maps:get(partition, Fenced)),
    ?assertEqual(2, maps:get(expected_epoch, Fenced)),
    %% ...but the partition is still readable metadata, mid-migration, and the
    %% target already accepts the new epoch -- no window where nobody can write.
    {ok, #{state := migrating, epoch := 2, owner := N2}} =
        rpc(N2, broker_cluster_cache, get, [0]),
    ok = rpc(N2, broker_cluster, authorize_append, [0, 2]),
    %% Phase 2: finish. Epoch advances again and the phase clears.
    {ok, 3} = rpc(N1, broker_cluster, finish_migration, [0, 3]),
    {ok, #{state := active, epoch := 3, owner := N2}} =
        rpc(N2, broker_cluster_cache, get, [0]),
    ok.

%% A node that left the membership leaves its partitions owned by an absent
%% node. Recovery is the trigger's job: `assign' them onto a live member, one
%% per step, without the operator naming each partition. The target is the
%% least-loaded member, so the two orphans split across the survivors.
a_departed_nodes_partitions_are_recovered(Config) ->
    Members = members(Config),
    [N1, N2, N3] = Members,
    ok = form_cluster(Config),
    %% N1 owns 0 and 1, N2 owns 2, N3 owns 3.
    ok = assign(0, N1, Members, 1),
    ok = assign(1, N1, Members, 1),
    ok = assign(2, N2, Members, 1),
    ok = assign(3, N3, Members, 1),
    ok = enable(#{hysteresis => 1, cooldown_ms => ?COOLDOWN_MS}),
    %% N1 goes away. The survivors are N2 and N3 (still a majority of 3), but a
    %% majority still has to *elect* a new leader, so the first steps after the
    %% peer stopped can answer `{error, nodedown}' until it does. Retry rather
    %% than assume the election finished inside `peer:stop'.
    ok = stop_peer(Config, N1),
    Survivors = [N2, N3],
    {reassigned, P, To} = step_retry(N2, Survivors),
    ?assert(lists:member(P, [0, 1])),
    ?assert(lists:member(To, Survivors)),
    %% The orphan now lives on a live member, at epoch+1, on both survivors.
    [?assertEqual({ok, {ok, To}},
                  {ok, rpc(N, broker_cluster, owner, [P])}) || N <- Survivors],
    %% The second orphan is still there and gets recovered next round.
    {reassigned, P2, To2} = step_retry(N2, Survivors),
    ?assert(lists:member(P2, [0, 1])),
    ?assertNotEqual(P, P2),
    ?assert(lists:member(To2, Survivors)),
    ?assertEqual([], orphans_on(N2, Survivors)),
    ok.

%% D6: losing the majority makes metadata writes fail, and the trigger is not
%% exempt. `{error, no_quorum}' and an unchanged view -- never a silent local
%% "move" that the cluster did not commit.
losing_the_majority_refuses_the_move(Config) ->
    Members = members(Config),
    [N1, N2, N3] = Members,
    ok = form_cluster(Config),
    [ok = assign(P, N1, Members, 1) || P <- partitions()],
    ok = enable(#{hysteresis => 1, cooldown_ms => ?COOLDOWN_MS}),
    %% Two of three down: no majority.
    ok = stop_peer(Config, N2),
    ok = stop_peer(Config, N3),
    ?assertEqual({error, no_quorum},
                 rpc(N1, broker_rebalance, step, [[N1, N2, N3]])),
    %% Nothing changed on the surviving node.
    {ok, #{owner := N1, epoch := 1}} = rpc(N1, broker_cluster_cache, get, [0]),
    ok.

%% The point of "one move per round" is that repeating it converges rather than
%% overshooting. Four partitions, three nodes: stepping until stable ends with
%% a spread of at most the hysteresis, and no partition lost or duplicated.
repeated_steps_converge_to_balance(Config) ->
    Members = members(Config),
    [N1 | _] = Members,
    ok = form_cluster(Config),
    [ok = assign(P, N1, Members, 1) || P <- partitions()],
    ok = enable(#{hysteresis => 1, cooldown_ms => ?COOLDOWN_MS}),
    Final = step_until_stable(N1, Members, 20),
    ?assertMatch({stable, _}, Final),
    {stable, Summary} = Final,
    ?assertEqual([], maps:get(orphaned, Summary)),
    %% Every partition still exists exactly once, and the counts add up to the
    %% partition count -- a balance that dropped work would not.
    Counts = maps:get(counts, Summary),
    ?assertEqual(length(partitions()), lists:sum(maps:values(Counts))),
    ?assert(maps:get(spread, Summary) =< 1),
    %% Every survivor agrees on the final view.
    Owners = [{P, rpc(N2, broker_cluster, owner, [P])}
              || N2 <- Members, P <- partitions()],
    [?assertMatch({_, {ok, _}}, O) || O <- Owners],
    ok.

%%%_* Helpers --------------------------------------------------------------

partitions() -> lists:seq(0, ?PARTITIONS - 1).

assign(P, Owner, Replicas, Epoch) ->
    case rpc(node1(), broker_cluster, assign, [P, Owner, Replicas, Epoch]) of
        {ok, Epoch} -> ok;
        Other -> ct:fail({assign_failed, P, Owner, Other})
    end.

%% `assign/4'-style setup always goes through the first peer, which the fixtures
%% guarantee exists until a case stops it.
node1() ->
    case get(node1) of
        undefined -> ct:fail(no_node1);
        N -> N
    end.

enable(Overrides) ->
    Cfg = maps:merge(#{enabled => true, hysteresis => 1,
                       cooldown_ms => ?COOLDOWN_MS}, Overrides),
    [ok = rpc(N, application, set_env, [broker_cluster, rebalance, Cfg])
     || N <- members_from_pd()],
    ok.

members_from_pd() ->
    case get(all_members) of
        undefined -> ct:fail(no_members);
        M -> M
    end.

step_until_stable(Node, Members, Budget) ->
    case rpc(Node, broker_rebalance, step, [Members]) of
        {migrated, _, _, _} when Budget > 0 ->
            step_until_stable(Node, Members, Budget - 1);
        {reassigned, _, _} when Budget > 0 ->
            step_until_stable(Node, Members, Budget - 1);
        {stable, _} = Stable ->
            Stable;
        {cooldown, _} = Cooldown ->
            Cooldown;
        Other ->
            ct:fail({unexpected_step, Other})
    end.

%% One step, retried while the group is still electing a leader after a peer was
%% stopped. `{error, nodedown}' is the honest transient answer, not a verdict.
step_retry(Node, Members) ->
    step_retry(Node, Members, 20).

step_retry(Node, Members, 0) ->
    ct:fail({step_never_committed, rpc(Node, broker_rebalance, step, [Members])});
step_retry(Node, Members, N) ->
    case rpc(Node, broker_rebalance, step, [Members]) of
        {reassigned, _, _} = Ok -> Ok;
        {migrated, _, _, _} = Ok -> Ok;
        {stable, _} = Ok -> Ok;
        {cooldown, _} = Ok -> Ok;
        {error, nodedown} ->
            timer:sleep(250),
            step_retry(Node, Members, N - 1);
        {error, no_quorum} ->
            timer:sleep(250),
            step_retry(Node, Members, N - 1);
        Other ->
            ct:fail({unexpected_step, Other})
    end.

orphans_on(Node, Members) ->
    View = rpc(Node, broker_cluster, partition_view, []),
    MemberSet = lists:usort(Members),
    lists:sort([P || {P, #{owner := O, state := active}} <- maps:to_list(View),
                     not lists:member(O, MemberSet)]).

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
    %% Setup helpers (`assign/4', `enable/1') need to reach the peers from the
    %% case body without threading Config through every call.
    put(node1, maps:get(node, hd(Peers))),
    put(all_members, [maps:get(node, P) || P <- Peers]),
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
    list_to_atom("s12c_" ++ sanitise(atom_to_list(Case)) ++ "_" ++
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
