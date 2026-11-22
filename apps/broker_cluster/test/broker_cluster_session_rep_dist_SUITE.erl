%% @doc MQ-17 (会话复制 Plan 1) Task 6: a MULTI-NODE Common Test proving a real
%% session Op crosses a node boundary and lands in the REPLICA's local table.
%%
%% Two real peer nodes (OTP `peer:start_link/1', the same machinery the other
%% dist suites use) share a `broker_cluster_cache' ownership mirror. The owner's
%% `broker_cluster:partition_view/0' therefore lists both peers as replicas, so
%% the installed session-replication hook (Task 5, `session_replication = true')
%% finds a replica set and broadcasts the Op across the distribution.
%%
%% Two claims, one case each, matching Plan 1 交付判据:
%%
%%   1. positive: with `session_replication = true' on the owner, a real
%%      `broker_subscription_worker:subscribe/3' on the owner fires the hook ->
%%      the Op is replicated cross-node -> the replica's local
%%      `broker_subscription_entities' table ends up containing that SubRef
%%      (and the second `broker_subscription_members' index lands too);
%%   2. negative (I2): with `session_replication = false', the hook is
%%      `undefined' on the owner, so a subscribe does NOT replicate -- the
%%      replica's local table stays empty, and the hook being `undefined' is
%%      asserted directly.
%%
%% The hook is FIRE-AND-FORGET (it spawns a detached best-effort broadcast), so
%% every positive assertion polls with `wait_until/1' rather than reading once.
%%
%% Every port is ephemeral; no project port is touched. `peer' is OTP stdlib,
%% so the peers are contacted by their real names over the distribution.
-module(broker_cluster_session_rep_dist_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(PARTITIONS, 2).
-define(NODES, 2).

all() ->
    [session_op_reaches_replica_when_enabled,
     no_replication_when_disabled].

%%%_* Fixtures --------------------------------------------------------------

init_per_suite(Config) ->
    case node() of
        nonode@nohost ->
            %% Same distribution requirement as the other dist suites: CT runs
            %% un-named by default, and a peer with a name is contacted over the
            %% distribution. See `broker_cluster_cross_node_dist_SUITE' for the
            %% full note (no `nocookie' re-application after `net_kernel:start/1').
            case net_kernel:start([unifiedmq_s6ct, shortnames]) of
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
    Nodes = [maps:get(node, P) || P <- Peers],
    [N1, N2] = Nodes,
    %% Both peers share one ownership view: partition 0 is owned by N2 with both
    %% peers as replicas. The session-replication initiator reads replicas from
    %% this mirror via `broker_cluster:partition_view/0', so it must list both.
    ok = rpc(N1, application, set_env, [broker_cluster, members, Nodes]),
    ok = rpc(N2, application, set_env, [broker_cluster, members, Nodes]),
    ok = rpc(N1, ?MODULE, set_owner, [0, N2, [N1, N2]]),
    ok = rpc(N2, ?MODULE, set_owner, [0, N2, [N1, N2]]),
    [{peers, Peers}, {nodes, Nodes}, {replica, N1}, {owner, N2} | Config].

end_per_testcase(_Case, Config) ->
    stop_peers(?config(peers, Config)),
    ok.

%%%_* Cases ----------------------------------------------------------------

%% Positive (end-to-end through the real hook path). The owner's
%% `broker_subscription_worker:subscribe/3' runs the installed hook, which
%% broadcasts the `{subscribe, ...}' Op to the replica set; the replica applies
%% it into its local `broker_subscription_entities' / `broker_subscription_members'.
session_op_reaches_replica_when_enabled(Config) ->
    Owner = ?config(owner, Config),
    Replica = ?config(replica, Config),
    %% Turn replication on and reinstall so the hook is present on the owner.
    ok = rpc(Owner, application, set_env, [broker_cluster, session_replication, true]),
    ok = rpc(Owner, broker_cluster, install_hooks, []),
    %% A real subscribe on the owner. The SubRef term is created on the owner and
    %% is the SAME term that arrives at the replica (an ETS key is just a term).
    {ok, SubRef} = rpc(Owner, broker_subscription_worker, subscribe,
                       [<<"cx">>, <<"t/x">>, 1]),
    %% The entity row landed in the replica's local table. `wait_until/1' needs a
    %% boolean predicate and returns `ok'; the precise shape is asserted on the
    %% direct read afterwards.
    ok = wait_until(fun() ->
                rpc(Replica, ets, lookup,
                    [broker_subscription_entities, SubRef]) =/= []
            end),
    ?assertMatch([{SubRef, _}],
                 rpc(Replica, ets, lookup, [broker_subscription_entities, SubRef])),
    %% And the second index (one member row per consumer) landed too.
    ok = wait_until(fun() ->
                rpc(Replica, ets, lookup,
                    [broker_subscription_members, {<<"cx">>, SubRef}]) =/= []
            end),
    ?assertMatch([{{<<"cx">>, SubRef}}],
                 rpc(Replica, ets, lookup,
                     [broker_subscription_members, {<<"cx">>, SubRef}])),
    ok.

%% Negative (I2). With the switch off the hook is `undefined', so a subscribe
%% does not replicate: the replica's local table stays empty. The deterministic
%% proof is the hook-undefined assertion; the empty-replica assertion is the
%% behavioural confirmation.
no_replication_when_disabled(Config) ->
    Owner = ?config(owner, Config),
    Replica = ?config(replica, Config),
    ok = rpc(Owner, application, set_env, [broker_cluster, session_replication, false]),
    ok = rpc(Owner, broker_cluster, install_hooks, []),
    %% The hook is absent: asserted directly.
    ?assertEqual(undefined,
                 rpc(Owner, persistent_term, get,
                     [{broker_subscription_worker, session_replicator}, undefined])),
    {ok, SubRef2} = rpc(Owner, broker_subscription_worker, subscribe,
                        [<<"cy">>, <<"t/y">>, 1]),
    %% Give any (erroneous) async replication a brief window to (not) land.
    timer:sleep(500),
    ?assertEqual([], rpc(Replica, ets, lookup,
                         [broker_subscription_entities, SubRef2])),
    ok.

%%%_* Peers ----------------------------------------------------------------

start_peers(Case, Config) ->
    Names = [peer_name(Case, I) || I <- lists:seq(1, ?NODES)],
    SuiteDir = filename:dirname(code:which(?MODULE)),
    CodePaths = lists:usort([SuiteDir | [D || D <- code:get_path(), is_code_dir(D)]]),
    lists:reverse(
      lists:foldl(
        fun({I, Name}, Acc) ->
              DataDir = filename:join([?config(priv_dir, Config),
                                       atom_to_list(Case),
                                       "n" ++ integer_to_list(I), "ra"]),
              try
                  {ok, Pid, Node} = peer:start_link(
                                      #{name => Name,
                                        args => ["-pa" | CodePaths],
                                        wait_boot => 20000}),
                  ok = rpc(Node, prepare_peer, [DataDir]),
                  ok = rpc(Node, start_data_plane, [true]),
                  [#{pid => Pid, node => Node, data_dir => DataDir} | Acc]
              catch
                  Class:Reason:Stack ->
                      stop_peers(Acc),
                      erlang:raise(Class, Reason, Stack)
              end
        end, [], lists:zip(lists:seq(1, ?NODES), Names))).

stop_peers(Peers) ->
    lists:foreach(fun(#{pid := Pid}) ->
                      _ = try peer:stop(Pid) catch _:_ -> ok end
                  end, Peers),
    ok.

peer_name(Case, I) ->
    list_to_atom("s6d_" ++ sanitise(atom_to_list(Case)) ++ "_" ++ integer_to_list(I)).

is_code_dir(Dir) ->
    filename:basename(Dir) =:= "ebin".

sanitise(S) ->
    [case C of _ when C >= $a andalso C =< $z -> C;
                 _ when C >= $A andalso C =< $Z -> C;
                 _ when C >= $0 andalso C =< $9 -> C;
                 _ -> $_ end || C <- S].

%%%_* Peer-side setup (runs on the peer) ----------------------------------

prepare_peer(DataDir) ->
    _ = application:load(broker_cluster),
    ok = application:set_env(broker_cluster, data_dir, DataDir),
    ok = application:set_env(broker_cluster, members, []),
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

%% The data plane without any listener: storage, router, queue, subscription, and
%% the hooks `broker_cluster' installs. No port is opened, so no project port is
%% in play even though the peers are distributed. `install_hooks/0' always
%% installs the fencing guard and the env-gated session-replicator hook.
start_data_plane(Routing) ->
    {ok, RaDir} = application:get_env(broker_cluster, data_dir),
    DataDir = filename:join(RaDir, "data"),
    ok = filelib:ensure_dir(filename:join([DataDir, "x"])),
    _ = [application:load(A)
         || A <- [broker_metrics, broker_core, broker_storage, broker_router,
                  broker_queue, broker_subscription, broker_cluster]],
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok = application:set_env(broker_storage, data_dir, DataDir),
    ok = application:set_env(broker_storage, wal, #{require_durable_fs => false}),
    ok = application:set_env(broker_queue, partitions, ?PARTITIONS),
    ok = application:set_env(broker_router, partitions, ?PARTITIONS),
    ok = application:set_env(broker_subscription, data_dir, DataDir),
    {ok, _} = application:ensure_all_started(broker_storage),
    [ok = broker_queue:create_partition(P) || P <- broker_queue:partition_ids()],
    {ok, _} = application:ensure_all_started(broker_router),
    {ok, _} = application:ensure_all_started(broker_queue),
    {ok, _} = application:ensure_all_started(broker_subscription),
    ok = application:set_env(broker_cluster, cross_node_delivery, Routing),
    ok = application:set_env(broker_cluster, cross_node_timeout_ms, 2000),
    ok = broker_cluster:install_hooks(),
    ok.

%% Runs on one peer: that peer's cache only. The test calls it on every peer so
%% both mirrors agree.
set_owner(Partition, Owner, Nodes) ->
    ok = broker_cluster_cache:put(Partition, #{partition => Partition,
                                               owner => Owner,
                                               epoch => 1,
                                               replicas => Nodes,
                                               state => active}),
    ok.

%%%_* Helpers --------------------------------------------------------------

wait_until(Pred) -> wait_until(Pred, 20000, 100).

wait_until(_Pred, 0, _Step) -> ct:fail({timeout, ?MODULE});
wait_until(Pred, Wait, Step) ->
    try Pred() of
        true -> ok;
        _ -> timer:sleep(Step), wait_until(Pred, Wait - Step, Step)
    catch
        _:_ -> timer:sleep(Step), wait_until(Pred, Wait - Step, Step)
    end.

%%%_* RPC -------------------------------------------------------------------

rpc(Node, F, A) ->
    rpc(Node, ?MODULE, F, A).

rpc(Node, M, F, A) ->
    case rpc:call(Node, M, F, A, 30000) of
        {badrpc, Reason} -> ct:fail({badrpc, Node, M, F, Reason});
        Result -> Result
    end.
