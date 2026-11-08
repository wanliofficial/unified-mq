%% @doc S4 distributed semantics (ADR-021, README §11.3).
%%
%% Three real peer nodes, each with its own `ra' data directory, form one
%% Metadata Raft Group. The suite proves the four distributed claims the
%% roadmap's Gate S4 names:
%%
%%   * a 3-node group forms and every node sees the committed owner;
%%   * killing the Leader leaves a majority that still commits (epoch advances);
%%   * losing the majority turns metadata writes into `{error, no_quorum}';
%%   * a node whose cache still says "I own it" is fenced once ownership moved.
%%
%% `peer' is part of OTP 28's stdlib, so no external orchestration is needed.
-module(broker_cluster_dist_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").

-define(CLUSTER, broker_cluster_meta).

all() ->
    [three_node_group_forms_and_replicates,
     leader_failover_keeps_committing,
     losing_the_majority_rejects_metadata_writes,
     old_owner_is_fenced_after_migration].

%%%_* Fixtures --------------------------------------------------------------

init_per_suite(Config) ->
    %% `peer' needs a distributed origin: a peer with a name is contacted over
    %% distribution. CT normally runs as `nonode@nohost', so this suite starts
    %% short-name distribution itself and stops it again if it did.
    case node() of
        nonode@nohost ->
            %% Do not touch the cookie here: before distribution is up,
            %% `erlang:get_cookie()' answers `nocookie', and re-applying that
            %% after `net_kernel:start/1' leaves the node with a cookie the
            %% peers (which read ~/.erlang.cookie) cannot authenticate
            %% against. The effect is a `peer:start_link' that waits for the
            %% whole `wait_boot' and then exits with `timeout'.
            case net_kernel:start([unifiedmq_s4_ct, shortnames]) of
                {ok, _} ->
                    [{started_net, true} | Config];
                {error, Reason} ->
                    ct:fail({cannot_start_distribution, Reason})
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

three_node_group_forms_and_replicates(Config) ->
    [N1, N2, N3] = peer_nodes(Config),
    %% One call dispatches the whole group; every peer already has its ra
    %% system and cache up (see prepare_peer/1).
    ok = rpc(N1, start_meta, [members(Config)]),
    ok = rpc(N2, start_meta, [members(Config)]),
    ok = rpc(N3, start_meta, [members(Config)]),
    {ok, 1} = rpc(N1, broker_cluster, assign, [0, N1, members(Config), 1]),
    %% The command is replicated: each node's local cache sees the owner.
    lists:foreach(
      fun(N) ->
              {ok, Owner} = rpc(N, broker_cluster, owner, [0]),
              Owner = N1,
              {ok, 1} = rpc(N, broker_cluster, epoch, [0])
      end, [N1, N2, N3]),
    ok.

leader_failover_keeps_committing(Config) ->
    [N1, N2, N3] = peer_nodes(Config),
    ok = form_cluster(Config),
    {ok, 1} = rpc(N1, broker_cluster, assign, [0, N1, members(Config), 1]),
    Leader = leader(Config),
    true = lists:member(Leader, [N1, N2, N3]),
    %% Kill the Leader. The surviving two nodes are still a majority.
    stop_peer(Config, Leader),
    Survivors = [N || N <- [N1, N2, N3], N =/= Leader],
    [S1 | _] = Survivors,
    wait_until(fun() -> is_pid(rpc(S1, erlang, whereis,
                                   [broker_cluster_meta_ctl])) end,
               15000),
    {ok, 2} = retry(fun() ->
                            rpc(S1, broker_cluster, assign,
                                [1, S1, Survivors, 2])
                    end, 20, 250),
    %% Both survivors agree, and the epoch advanced.
    lists:foreach(
      fun(N) ->
              {ok, 2} = rpc(N, broker_cluster, epoch, [1])
      end, Survivors),
    ok.

losing_the_majority_rejects_metadata_writes(Config) ->
    [N1, N2, N3] = peer_nodes(Config),
    _ = N3,
    ok = form_cluster(Config),
    {ok, 1} = rpc(N1, broker_cluster, assign, [0, N1, members(Config), 1]),
    %% Stop two nodes: one left out of three is not a majority.
    stop_peer(Config, N2),
    stop_peer(Config, N3),
    {error, no_quorum} =
        rpc(N1, broker_cluster, assign, [1, N1, [N1], 2]),
    ok.

old_owner_is_fenced_after_migration(Config) ->
    [N1, N2, N3] = peer_nodes(Config),
    _ = N3,
    ok = form_cluster(Config),
    All = members(Config),
    {ok, 1} = rpc(N1, broker_cluster, assign, [0, N1, All, 1]),
    ok = rpc(N1, broker_cluster, authorize_append, [0, 1]),
    %% Move ownership to N2. Every node applies the command, so N1 now knows
    %% it is no longer the owner.
    {ok, 2} = rpc(N2, broker_cluster, migrate, [0, N1, N2, 2]),
    {error, {fenced, Fenced}} = rpc(N1, broker_cluster,
                                    authorize_append, [0, 1]),
    0 = maps:get(partition, Fenced),
    2 = maps:get(expected_epoch, Fenced),
    %% N2, the current owner, accepts the new epoch.
    ok = rpc(N2, broker_cluster, authorize_append, [0, 2]),
    ok.

%%%_* Peers ---------------------------------------------------------------

start_peers(Case, Config) ->
    Names = [peer_name(Case, N) || N <- [1, 2, 3]],
    %% Only the build tree: passing every path CT added (including priv and
    %% log directories) makes peer boot needlessly slow and can time out.
    %% The suite's own beam directory is added explicitly -- `prepare_peer/1'
    %% runs on the peer, and CT keeps test modules outside `ebin'.
    SuiteDir = filename:dirname(code:which(?MODULE)),
    CodePaths = lists:usort(
                  [SuiteDir | [D || D <- code:get_path(),
                                    string:find(D, "_build/test") =/= nomatch]]),
    lists:map(
      fun({I, Name}) ->
              DataDir = filename:join([?config(priv_dir, Config),
                                       atom_to_list(Case),
                                       "n" ++ integer_to_list(I), "ra"]),
              {ok, Pid, Node} =
                  peer:start_link(#{name => Name,
                                    args => ["-pa" | CodePaths],
                                    wait_boot => 20000}),
              %% ra and the cache must be up on the peer before the group is
              %% dispatched (start_cluster/4 starts members over rpc).
              ok = rpc(Node, prepare_peer, [[DataDir, []]]),
              #{pid => Pid, node => Node, data_dir => DataDir}
      end, lists:zip([1, 2, 3], Names)).

stop_peers(Peers) ->
    lists:foreach(
      fun(#{pid := Pid}) -> catch peer:stop(Pid) end, Peers),
    ok.

stop_peer(Config, Node) ->
    Peers = ?config(peers, Config),
    case [P || #{node := N} = P <- Peers, N =:= Node] of
        [#{pid := Pid}] -> catch peer:stop(Pid), ok;
        _ -> ok
    end.

peer_name(Case, I) ->
    list_to_atom("s4_" ++ sanitise(atom_to_list(Case)) ++ "_" ++
                 integer_to_list(I)).

sanitise(S) ->
    [case C of _ when C >= $a andalso C =< $z -> C;
                 _ when C >= $A andalso C =< $Z -> C;
                 _ when C >= $0 andalso C =< $9 -> C;
                 _ -> $_ end || C <- S].

peer_nodes(Config) ->
    [Node || #{node := Node} <- ?config(peers, Config)].

%% The `members' env and the `replicas' argument are plain node atoms;
%% `broker_cluster_meta' wraps them into ra server ids itself.
members(Config) ->
    peer_nodes(Config).

form_cluster(Config) ->
    [N1, N2, N3] = peer_nodes(Config),
    ok = rpc(N1, start_meta, [members(Config)]),
    ok = rpc(N2, start_meta, [members(Config)]),
    ok = rpc(N3, start_meta, [members(Config)]),
    ok.

leader(Config) ->
    [N | _] = peer_nodes(Config),
    case rpc(N, ra, members, [{?CLUSTER, N}, 10000]) of
        {ok, _Members, Leader} when is_atom(Leader) -> Leader;
        {ok, _Members, {?CLUSTER, Leader}} -> Leader;
        Other -> ct:fail({no_leader, Other})
    end.

rpc(Node, F, A) ->
    rpc(Node, ?MODULE, F, A).

rpc(Node, M, F, A) ->
    case rpc:call(Node, M, F, A, 30000) of
        {badrpc, Reason} -> ct:fail({badrpc, Node, M, F, Reason});
        Result -> Result
    end.

retry(_Fun, 0, _Wait) ->
    {error, exhausted};
retry(Fun, N, Wait) ->
    case Fun() of
        {ok, _} = Ok -> Ok;
        _ -> timer:sleep(Wait), retry(Fun, N - 1, Wait)
    end.

wait_until(Fun, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    wait_until_loop(Fun, Deadline).

wait_until_loop(Fun, Deadline) ->
    Now = erlang:monotonic_time(millisecond),
    case Fun() of
        true -> ok;
        _ when Now >= Deadline -> ok;
        _ -> timer:sleep(100), wait_until_loop(Fun, Deadline)
    end.

%%%_* Code executed on each peer ------------------------------------------

%% Every peer must have ra and the cache up *before* the group is dispatched,
%% because `ra:start_cluster/4' starts each member on its own node over rpc.
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
