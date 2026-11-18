%% @doc S12-A's multi-node half: the node view is a *contract*, so it has to be
%% proven across nodes rather than only on one.
%%
%% ADR-032 D1 says the single source of truth for the node view is the ra
%% state machine in `broker_cluster' plus each node's local cache mirror, and
%% D2 says a node's drain state is node-local and never enters ra. Both claims
%% are only meaningful with more than one node, which is what this suite adds:
%%
%%   * every node's `/api/v1/node' reports exactly the partitions that node
%%     owns, so the views agree with ra and with each other (no partition
%%     claimed by two nodes, none missing from the union);
%%   * draining one node leaves every other node's view and readiness alone --
%%     a drain is not a cluster-wide fact, it is precisely the thing that must
%%     still work when the cluster has lost quorum.
%%
%% `peer' is OTP 28 stdlib, same machinery `broker_cluster_dist_SUITE' uses; no
%% external orchestration. Ports are ephemeral (`port => 0') and never the
%% project's fixed ports.
-module(broker_admin_dist_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").

-define(CLUSTER, broker_cluster_meta).
-define(PARTITIONS, 2).

all() ->
    [node_views_agree_with_ra_across_nodes,
     drain_is_node_local_and_does_not_reach_other_nodes].

%%%_* Fixtures --------------------------------------------------------------

init_per_suite(Config) ->
    case node() of
        nonode@nohost ->
            %% Do not touch the cookie: see the note in
            %% `broker_cluster_dist_SUITE:init_per_suite/1'. Re-applying
            %% `nocookie' after `net_kernel:start/1' leaves peers unable to
            %% authenticate.
            case net_kernel:start([unifiedmq_s12a_ct, shortnames]) of
                {ok, _} -> [{started_net, true} | Config];
                {error, Reason} ->
                    %% Most often epmd is not running. Say so: the raw
                    %% `nodistribution' term sends people looking in the wrong
                    %% place. `broker_cluster_dist_SUITE' has the same
                    %% requirement, so this is not a new dependency.
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

%% The union of the three views is exactly the assigned partition set, and each
%% partition is claimed by exactly the node ra assigned it to. This is the
%% "no second source of truth" claim: if any node read Mnesia or a local guess
%% instead of the ra cache, the views would disagree.
node_views_agree_with_ra_across_nodes(Config) ->
    [N1, N2, N3] = peer_nodes(Config),
    ok = form_cluster(Config),

    %% Two partitions, owned by two different nodes.
    {ok, 1} = rpc(N1, broker_cluster, assign, [0, N1, members(Config), 1]),
    {ok, 1} = rpc(N1, broker_cluster, assign, [1, N2, members(Config), 1]),

    Views = [{N, node_view(N)} || N <- [N1, N2, N3]],

    %% Each node advertises exactly the partitions ra says it owns.
    [begin
         #{<<"partitions">> := #{<<"count">> := Count,
                                 <<"partitions">> := Owned}} = View,
         Want = [P || P <- lists:seq(0, ?PARTITIONS - 1),
                      rpc(N, broker_cluster, owner, [P]) =:= {ok, N}],
         length(Want) =:= Count orelse
             ct:fail({count_mismatch, N, Count, Want}),
         lists:sort([P || #{<<"partition">> := P} <- Owned]) =:= lists:sort(Want)
             orelse ct:fail({owned_mismatch, N, Owned, Want}),
         %% Epoch must come from the same ra state, not a default.
         lists:foreach(
           fun(#{<<"partition">> := P} = Entry) ->
                   {ok, E} = rpc(N, broker_cluster, epoch, [P]),
                   maps:get(<<"epoch">>, Entry) =:= E
                       orelse ct:fail({epoch_mismatch, N, P, Entry})
           end, Owned)
     end || {N, View} <- Views],

    %% Exactly one owner per partition across the whole cluster, and the union
    %% is complete. This is the consistency property, not an implementation
    %% detail.
    Claimed = [{P, N} || {N, #{<<"partitions">> :=
                                   #{<<"partitions">> := Owned}}} <- Views,
                         #{<<"partition">> := P} <- Owned],
    lists:sort([P || {P, _} <- Claimed]) =:= lists:seq(0, ?PARTITIONS - 1)
        orelse ct:fail({uncovered_partitions, Claimed}),
    length(Claimed) =:= ?PARTITIONS
        orelse ct:fail({partition_claimed_twice, Claimed}),
    [{0, N1}, {1, N2}] = lists:sort(Claimed),

    %% The older view endpoint and the new one must not disagree: `/api/v1/status'
    %% lists the configured partition set, and `/api/v1/node' lists the subset
    %% this node owns. Filtering the first by ra ownership must reproduce the
    %% second exactly -- two admin endpoints disagreeing about ownership is the
    %% failure D1 exists to prevent.
    [begin
         #{<<"partitions">> := StatusPs} = status(N),
         #{<<"partitions">> := #{<<"partitions">> := Owned}} = node_view(N),
         Want = lists:sort([P || P <- StatusPs,
                                 rpc(N, broker_cluster, owner, [P]) =:= {ok, N}]),
         lists:sort([P || #{<<"partition">> := P} <- Owned]) =:= Want
             orelse ct:fail({endpoints_disagree, N, Owned, Want})
     end || N <- [N1, N2, N3]],
    ok.

%% D1/D2: drain is node-local. Draining one node must not change another
%% node's state or readiness -- and the drained node must report `draining'
%% without the cluster having to agree on it.
drain_is_node_local_and_does_not_reach_other_nodes(Config) ->
    [N1, N2, N3] = peer_nodes(Config),
    ok = form_cluster(Config),
    {ok, 1} = rpc(N1, broker_cluster, assign, [0, N1, members(Config), 1]),

    Others = [N2, N3],
    Before = [{N, node_view(N)} || N <- Others],

    %% Drain exactly one node.
    {200, Drained} = http(N1, post, "/api/v1/drain"),
    #{<<"state">> := <<"draining">>} = Drained,

    %% That node is out of rotation...
    {503, #{<<"status">> := <<"draining">>}} = http(N1, get, "/readyz"),
    %% ...while the others are untouched, in the view and in readiness.
    lists:foreach(
      fun({N, BeforeView}) ->
              After = node_view(N),
              maps:get(<<"state">>, After) =:= <<"up">>
                  orelse ct:fail({drain_leaked, N, After}),
              maps:get(<<"ready">>, After) =:= true
                  orelse ct:fail({not_ready_after_peer_drain, N, After}),
              %% The partition set is not disturbed by a peer draining.
              maps:get(<<"partitions">>, After) =:=
                  maps:get(<<"partitions">>, BeforeView)
                  orelse ct:fail({partitions_changed, N}),
              case http(N, get, "/readyz") of
                  {503, #{<<"status">> := <<"draining">>}} ->
                      ct:fail({drain_leaked_into_readyz, N});
                  _ -> ok
              end
      end, Before),

    %% Undrain brings the first node back without touching the others again.
    {200, Undrained} = http(N1, post, "/api/v1/undrain"),
    #{<<"state">> := <<"up">>} = Undrained,
    {_, Ready} = http(N1, get, "/readyz"),
    maps:get(<<"status">>, Ready) =/= <<"draining">> orelse
        ct:fail(still_draining_after_undrain),
    ok.

%%%_* Peers ---------------------------------------------------------------

start_peers(Case, Config) ->
    Names = [peer_name(Case, N) || N <- [1, 2, 3]],
    SuiteDir = filename:dirname(code:which(?MODULE)),
    CodePaths = lists:usort(
                  [SuiteDir | [D || D <- code:get_path(), is_code_dir(D)]]),
    %% Build the list incrementally and stop whatever was already started if a
    %% later peer fails. A leaked peer keeps its name registered with epmd and
    %% its ports bound, which poisons every later case and suite.
    lists:reverse(
      lists:foldl(
        fun({I, Name}, Acc) ->
              DataDir = filename:join([?config(priv_dir, Config),
                                       atom_to_list(Case),
                                       "n" ++ integer_to_list(I), "ra"]),
              try
                  {ok, Pid, Node} =
                      peer:start_link(#{name => Name,
                                        args => ["-pa" | CodePaths],
                                        wait_boot => 20000}),
                  ok = rpc(Node, prepare_peer, [DataDir]),
                  ok = rpc(Node, start_admin, []),
                  [#{pid => Pid, node => Node, data_dir => DataDir} | Acc]
              catch
                  Class:Reason:Stack ->
                      stop_peers(Acc),
                      erlang:raise(Class, Reason, Stack)
              end
        end, [], lists:zip([1, 2, 3], Names))).

stop_peers(Peers) ->
    lists:foreach(fun(#{pid := Pid}) -> catch peer:stop(Pid) end, Peers),
    ok.

peer_name(Case, I) ->
    list_to_atom("s12a_" ++ sanitise(atom_to_list(Case)) ++ "_" ++
                 integer_to_list(I)).

%% Profile-independent test for "this directory can go on `-pa'".
is_code_dir(Dir) ->
    filename:basename(Dir) =:= "ebin".

sanitise(S) ->
    [case C of _ when C >= $a andalso C =< $z -> C;
                 _ when C >= $A andalso C =< $Z -> C;
                 _ when C >= $0 andalso C =< $9 -> C;
                 _ -> $_ end || C <- S].

peer_nodes(Config) ->
    [Node || #{node := Node} <- ?config(peers, Config)].

members(Config) ->
    peer_nodes(Config).

form_cluster(Config) ->
    [N1, N2, N3] = peer_nodes(Config),
    ok = rpc(N1, start_meta, [members(Config)]),
    ok = rpc(N2, start_meta, [members(Config)]),
    ok = rpc(N3, start_meta, [members(Config)]),
    ok.

rpc(Node, F, A) ->
    rpc(Node, ?MODULE, F, A).

rpc(Node, M, F, A) ->
    case rpc:call(Node, M, F, A, 30000) of
        {badrpc, Reason} -> ct:fail({badrpc, Node, M, F, Reason});
        Result -> Result
    end.

%%%_* HTTP client (real TCP to the peer's loopback listener) ----------------

node_view(Node) ->
    {200, View} = http(Node, get, "/api/v1/node"),
    View.

status(Node) ->
    {200, Status} = http(Node, get, "/api/v1/status"),
    Status.

http(Node, Method, Path) ->
    Port = rpc(Node, broker_admin_listener, port, []),
    true = is_integer(Port),
    {ok, Sock} = gen_tcp:connect({127, 0, 0, 1}, Port,
                                 [binary, {active, false}], 5000),
    MethodBin = case Method of get -> <<"GET">>; post -> <<"POST">> end,
    Req = [MethodBin, " ", Path, " HTTP/1.1\r\n",
           "Host: localhost\r\n", "Content-Length: 0\r\n\r\n"],
    ok = gen_tcp:send(Sock, Req),
    Resp = recv_all(Sock, <<>>),
    _ = gen_tcp:close(Sock),
    parse_response(Resp).

recv_all(Sock, Acc) ->
    case gen_tcp:recv(Sock, 0, 5000) of
        {ok, Data} -> recv_all(Sock, <<Acc/binary, Data/binary>>);
        {error, closed} -> Acc
    end.

parse_response(Resp) ->
    [Head, Body] = binary:split(Resp, <<"\r\n\r\n">>),
    [StatusLine | _] = binary:split(Head, <<"\r\n">>, [global]),
    [_, Code | _] = binary:split(StatusLine, <<" ">>, [global]),
    {binary_to_integer(Code), json:decode(Body)}.

%%%_* Code executed on each peer ------------------------------------------

prepare_peer(DataDir) ->
    _ = application:load(broker_cluster),
    ok = application:set_env(broker_cluster, data_dir, DataDir),
    ok = application:set_env(broker_cluster, members, []),
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

%% The admin app on a peer: ephemeral port, loopback bind, empty token (a
%% loopback bind with an empty token is exactly what the S12-A guard permits),
%% and a partition set the node view can enumerate.
start_admin() ->
    [application:load(A) || A <- [broker_queue, broker_admin]],
    application:set_env(broker_queue, partitions, ?PARTITIONS),
    application:set_env(broker_admin, token, <<>>),
    application:set_env(broker_admin, bind, {127, 0, 0, 1}),
    application:set_env(broker_admin, port, 0),
    application:set_env(broker_admin, backup_dir, "/tmp/opencode/s12a_dist_backups"),
    {ok, _} = application:ensure_all_started(broker_admin),
    ok.
