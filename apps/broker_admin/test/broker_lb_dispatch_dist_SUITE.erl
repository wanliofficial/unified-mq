%% @doc S12-B's dispatch half (ADR-032 D7, docs/design/s12-infrastructure-ha-and-lb.md R-E3):
%% the broker supplies a node view, and an *external* load balancer is what
%% dispatches connections with it. S12 deliberately does not become a four-layer
%% proxy (ADR-032 D3), so the evidence that belongs here is not "the broker
%% balanced traffic" -- it is "a faithful consumer of the published contract
%% makes the right dispatch decisions against real listeners".
%%
%% `broker_admin_dist_SUITE' already proves the exclusion side of the contract:
%% draining one node leaves every other node's view and readiness alone. This
%% suite proves the side that only real traffic can show:
%%
%%   * an equivalent dispatcher that reads each node's real `GET /api/v1/node'
%%     and routes only to `ready => true' nodes spreads real MQTT connections
%%     across the `up' nodes and puts none on a `draining' one;
%%   * draining a node mid-flight retires it from *subsequent* dispatch without
%%     touching the connections it already carries -- which is the whole point of
%%     drain being a "stop taking new work" state rather than a kill;
%%   * undraining puts it back in rotation.
%%
%% The dispatcher below is deliberately dumb: query, filter on `ready', round
%% robin. That is the contract ADR-032 froze -- the broker promises a truthful
%% view, not a policy. Nothing here asserts a balancing *algorithm*, because the
%% algorithm is the operator's LB, not this repo.
%%
%% `peer' is OTP 28 stdlib, same machinery the other dist suites use. Every port
%% is ephemeral (`port => 0') and read back; no project port is touched. The
%% peer's two addresses are resolved once at start-up and carried in the peer
%% map, so the cases talk to the peers over their real HTTP/MQTT ports exactly
%% like an external dispatcher would -- and no test-phase `rpc' is involved.
-module(broker_lb_dispatch_dist_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(PARTITIONS, 2).
-define(NODES, 2).

all() ->
    [the_dispatcher_routes_only_to_ready_nodes,
     a_draining_node_keeps_its_connections_but_gets_no_new_ones,
     undrain_returns_a_node_to_rotation,
     no_ready_node_is_refused_not_queued].

%%%_* Fixtures --------------------------------------------------------------

init_per_suite(Config) ->
    case node() of
        nonode@nohost ->
            %% Same distribution requirement as the other dist suites; see the
            %% note in `broker_cluster_dist_SUITE:init_per_suite/1'. Do not
            %% re-apply `nocookie' after `net_kernel:start/1' -- peers would be
            %% unable to authenticate.
            case net_kernel:start([unifiedmq_s12b_ct, shortnames]) of
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

%% A dispatcher that only knows the published contract spreads real MQTT
%% connections over both nodes. This is the positive half: without a truthful
%% `ready' flag it could not do this, and with a view that lied about `up' the
%% connections would fail to get a CONNACK.
the_dispatcher_routes_only_to_ready_nodes(Config) ->
    Peers = peers(Config),
    ok = form_cluster(Config),
    [?assertEqual(up, ready_state(P)) || P <- Peers],
    Routes = [dispatch(Peers) || _ <- lists:seq(1, 4)],
    %% Every route landed on a node the view called ready, and both nodes got
    %% work -- a dispatcher reading a single node's view would not.
    [?assertEqual(up, ready_state(P)) || {P, _Conn} <- Routes],
    Distinct = lists:usort([maps:get(node, P) || {P, _} <- Routes]),
    ?assertEqual(lists:sort([maps:get(node, P) || P <- Peers]), Distinct),
    %% And each of those connections is a real, served MQTT session.
    [assert_live_mqtt(Conn) || {_, Conn} <- Routes],
    close_all(Routes).

%% The distinction that makes drain useful: existing connections keep working,
%% new ones are not placed here. A dispatcher that only looks at `ready' honours
%% both halves for free, because the node keeps serving while reporting itself
%% out of rotation.
a_draining_node_keeps_its_connections_but_gets_no_new_ones(Config) ->
    Peers = peers(Config),
    ok = form_cluster(Config),
    [Drained, Other] = Peers,
    %% A connection established before the drain must survive it.
    Pre = dispatch([Drained]),
    {DrainedPeer, Conn} = Pre,
    ?assertEqual(<<"draining">>, drain_peer(DrainedPeer)),
    ?assertEqual(draining, ready_state(DrainedPeer)),
    %% Readiness says not-ready so an LB retires the node...
    ?assertEqual(false, node_ready(DrainedPeer)),
    %% ...but the connection already on it is untouched: CONNECT/PING still work.
    assert_live_mqtt(Conn),
    ?assertEqual(1, active_connections(DrainedPeer)),
    %% The dispatcher, seeing `draining', sends the next connections elsewhere.
    NewRoutes = [dispatch(Peers) || _ <- lists:seq(1, 3)],
    [?assertEqual(maps:get(node, Other), maps:get(node, P)) || {P, _} <- NewRoutes],
    [assert_live_mqtt(C) || {_, C} <- NewRoutes],
    %% The pre-drain connection is still alive after all of that.
    assert_live_mqtt(Conn),
    ?assertEqual(<<"up">>, undrain_peer(DrainedPeer)),
    close_all(NewRoutes ++ [Pre]).

%% Drain is a state, not a removal: undraining returns the node to rotation and
%% the dispatcher starts using it again.
undrain_returns_a_node_to_rotation(Config) ->
    Peers = peers(Config),
    ok = form_cluster(Config),
    [Node, _Other] = Peers,
    ?assertEqual(<<"draining">>, drain_peer(Node)),
    ?assertEqual(false, node_ready(Node)),
    %% While draining, a dispatch over both nodes never picks it.
    WhileDraining = [dispatch(Peers) || _ <- lists:seq(1, 4)],
    ?assertNot(lists:member(maps:get(node, Node),
                            [maps:get(node, P) || {P, _} <- WhileDraining])),
    ?assertEqual(<<"up">>, undrain_peer(Node)),
    ?assertEqual(true, node_ready(Node)),
    %% `ranch:suspend_listener/1' stops the acceptors sup, which owns the listen
    %% socket; resuming rebinds it. With `port => 0' that is a *new* ephemeral port,
    %% so re-read it -- a real deployment uses a fixed port and would not need this,
    %% but the suite must not depend on that.
    Rebound = refresh_mqtt_port(Node),
    %% Back in rotation: it can host a connection again.
    {BackPeer, Conn} = dispatch([Rebound]),
    ?assertEqual(maps:get(node, Node), maps:get(node, BackPeer)),
    assert_live_mqtt(Conn),
    close_all(WhileDraining ++ [{BackPeer, Conn}]).

%% ADR-032 D7 puts R-LB6's behaviour *outside* the broker -- "no available node"
%% and "node recovery" are absorbed by the LB and the client's retry, not by
%% internal queueing. The case that belongs here is therefore about the dispatcher:
%% with no ready node it must refuse immediately rather than hold the connection,
%% and once a node recovers the next attempt is a brand-new connection, not a
%% replay of a queued one.
no_ready_node_is_refused_not_queued(Config) ->
    Peers = peers(Config),
    ok = form_cluster(Config),
    [?assertEqual(<<"draining">>, drain_peer(P)) || P <- Peers],
    ?assertEqual(no_available_node, dispatch_or_refuse(Peers)),
    %% Nothing is waiting: bring both nodes back, give any notional queue time to
    %% drain, and neither node may have acquired a connection on its own.
    Rebound = [begin
                   ?assertEqual(<<"up">>, undrain_peer(P)),
                   refresh_mqtt_port(P)
               end || P <- Peers],
    timer:sleep(200),
    [?assertEqual(0, active_connections(P)) || P <- Rebound],
    %% A fresh attempt succeeds -- and it is a new connection, since there was no
    %% queued one to replay.
    {Peer, Conn} = dispatch(Rebound),
    assert_live_mqtt(Conn),
    close_all([{Peer, Conn}]).

%%%_* The equivalent dispatcher ---------------------------------------------
%%
%% This is the "external LB or equivalent simulator" of R-E3, kept as small as
%% the contract allows: read the view, keep the ready nodes, round robin. It runs
%% in the CT node and opens a real TCP connection to the chosen peer's real MQTT
%% listener.

%% Dispatch one connection across the candidate set, honouring the view.
dispatch(Candidates) ->
    case dispatch_or_refuse(Candidates) of
        no_available_node -> ct:fail({expected_a_ready_node, Candidates});
        Routed -> Routed
    end.

%% The LB's own behaviour when nothing is ready: a synchronous refusal. There is
%% no queue anywhere in the broker (ADR-032 D7), so there is nothing to wait on.
dispatch_or_refuse(Candidates) ->
    case [P || P <- Candidates, node_ready(P)] of
        [] ->
            no_available_node;
        Ready ->
            Picked = pick_next(Ready),
            {Picked, open_mqtt(Picked)}
    end.

%% Deterministic stand-in for round robin: the caller drives the rotation by
%% re-reading the view each time, so a simple counter on the process dictionary
%% is enough and keeps the suite free of scheduler-dependent assertions.
pick_next(Ready) ->
    N = case get(lb_cursor) of undefined -> 0; V -> V end,
    _ = put(lb_cursor, N + 1),
    lists:nth((N rem length(Ready)) + 1, Ready).

%% What an LB actually reads. `ready' is the contract's own boolean (state =:= up),
%% so the dispatcher does not have to interpret the tri-state itself. The view is
%% JSON, hence binary keys.
node_ready(Peer) ->
    maps:get(<<"ready">>, node_view(Peer)).

ready_state(Peer) ->
    %% JSON renders the atom state as a string; convert back so the assertions can
    %% speak in the same terms as `broker_drain'.
    binary_to_existing_atom(maps:get(<<"state">>, node_view(Peer)), utf8).

%% The node view's own `connections.active', summed over this node's listeners.
active_connections(Peer) ->
    #{<<"connections">> := #{<<"active">> := Active}} = node_view(Peer),
    Active.

%% Re-read the peer's MQTT port after a suspend/resume cycle (see the note in
%% `undrain_returns_a_node_to_rotation/1').
refresh_mqtt_port(Peer) ->
    Peer#{mqtt_port => rpc(maps:get(node, Peer), mqtt_listener_port, [])}.

%%%_* Real MQTT, over a real socket to the peer ---------------------------

%% Open a TCP connection to the peer's MQTT listener and complete a real CONNECT,
%% so "dispatched" means "served", not "socket accepted".
open_mqtt(Peer) ->
    Port = maps:get(mqtt_port, Peer),
    ?assert(is_integer(Port) andalso Port > 0),
    {ok, S} = gen_tcp:connect({127, 0, 0, 1}, Port,
                              [binary, {active, false}, {nodelay, true}], 5000),
    ClientId = iolist_to_binary(["lb-", integer_to_list(erlang:unique_integer([positive]))]),
    ok = gen_tcp:send(S, connect_packet(ClientId)),
    <<16#20, _Len, _SessionPresent, 0>> = recv_packet(S, <<>>, 5000),
    S.

%% Prove a connection is still a served MQTT session: PINGREQ gets a PINGRESP.
%% A socket the node had quietly stopped serving would time out here instead.
assert_live_mqtt(S) ->
    ok = gen_tcp:send(S, <<16#C0, 0>>),
    <<16#D0, 0>> = recv_packet(S, <<>>, 5000),
    ok.

close_all(Routes) ->
    [gen_tcp:close(S) || {_, S} <- Routes],
    ok.

connect_packet(ClientId) ->
    Payload = <<(byte_size(ClientId)):16, ClientId/binary>>,
    %% Protocol name "MQTT", level 4, clean-session flag, keepalive 60s.
    Variable = <<4:16, "MQTT", 4, 2, 60:16>>,
    Body = <<Variable/binary, Payload/binary>>,
    <<16#10, (remaining_length(byte_size(Body)))/binary, Body/binary>>.

remaining_length(N) when N < 128 -> <<N>>;
remaining_length(N) -> <<(N rem 128 + 128), (remaining_length(N div 128))/binary>>.

%% One MQTT packet: fixed header byte, 1-4 length bytes, then that many bytes.
recv_packet(S, Buf, Timeout) ->
    case packet_complete(Buf) of
        {true, Len} -> binary:part(Buf, 0, Len);
        false ->
            case gen_tcp:recv(S, 0, Timeout) of
                {ok, Data} -> recv_packet(S, <<Buf/binary, Data/binary>>, Timeout);
                {error, Why} -> exit({no_packet, Why, Buf})
            end
    end.

packet_complete(<<_Type:8, Rest/binary>> = All) ->
    case decode_len(Rest, 0, 1) of
        {ok, Len, HeaderBytes} ->
            Total = 1 + HeaderBytes + Len,
            {byte_size(All) >= Total, Total};
        more ->
            false
    end;
packet_complete(_) ->
    false.

decode_len(<<>>, _Acc, _N) -> more;
decode_len(<<B:8, Rest/binary>>, Acc, N) ->
    Acc1 = Acc + ((B band 16#7F) bsl (7 * (N - 1))),
    case B band 16#80 of
        0 -> {ok, Acc1, N};
        _ -> decode_len(Rest, Acc1, N + 1)
    end.

%%%_* Peers ----------------------------------------------------------------

%% Each peer runs the admin app (node view) and a real MQTT listener on an
%% ephemeral port. Two nodes is enough: the claim under test is "dispatch follows
%% the view", not "the view scales".
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
                  {ok, Pid, Node} =
                      peer:start_link(#{name => Name,
                                        args => ["-pa" | CodePaths],
                                        wait_boot => 20000}),
                  ok = rpc(Node, prepare_peer, [DataDir]),
                  ok = rpc(Node, start_runtime, []),
                  %% Resolve both addresses once, while the peer is known good, and
                  %% carry them so the cases need no test-phase `rpc'.
                  AdminPort = rpc(Node, broker_admin_listener, port, []),
                  MqttPort = rpc(Node, mqtt_listener_port, []),
                  [#{pid => Pid, node => Node, data_dir => DataDir,
                     admin_port => AdminPort, mqtt_port => MqttPort} | Acc]
              catch
                  Class:Reason:Stack ->
                      stop_peers(Acc),
                      erlang:raise(Class, Reason, Stack)
              end
        end, [], lists:zip(lists:seq(1, ?NODES), Names))).

stop_peers(Peers) ->
    lists:foreach(fun(#{pid := Pid}) -> catch peer:stop(Pid) end, Peers),
    ok.

peer_name(Case, I) ->
    list_to_atom("s12b_" ++ sanitise(atom_to_list(Case)) ++ "_" ++ integer_to_list(I)).

is_code_dir(Dir) ->
    filename:basename(Dir) =:= "ebin".

sanitise(S) ->
    [case C of _ when C >= $a andalso C =< $z -> C;
                 _ when C >= $A andalso C =< $Z -> C;
                 _ when C >= $0 andalso C =< $9 -> C;
                 _ -> $_ end || C <- S].

peers(Config) ->
    ?config(peers, Config).

members(Config) ->
    [maps:get(node, P) || P <- peers(Config)].

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

%%%_* Admin API (the interface an external LB actually consumes) -----------

%% Drain through the real admin API, not `rpc'. That is what an operator's LB and
%% runbook use, and it keeps this suite from depending on `rpc' being able to
%% carry the drain process's reply.
drain_peer(Peer) ->
    {200, View} = http(Peer, post, "/api/v1/drain", <<>>),
    maps:get(<<"state">>, View).

undrain_peer(Peer) ->
    {200, View} = http(Peer, post, "/api/v1/undrain", <<>>),
    maps:get(<<"state">>, View).

node_view(Peer) ->
    {200, View} = http(Peer, get, "/api/v1/node", <<>>),
    View.

http(Peer, Method, Path, Body) ->
    Port = maps:get(admin_port, Peer),
    true = is_integer(Port),
    {ok, S} = gen_tcp:connect({127, 0, 0, 1}, Port, [binary, {active, false}], 5000),
    Verb = case Method of get -> <<"GET">>; post -> <<"POST">> end,
    Req = [Verb, <<" ">>, Path, <<" HTTP/1.1\r\nHost: localhost\r\n"
                                    "Content-Length: ">>,
           integer_to_binary(byte_size(Body)), <<"\r\n"
                                                 "Connection: close\r\n\r\n">>, Body],
    ok = gen_tcp:send(S, Req),
    Resp = recv_until_closed(S, <<>>),
    gen_tcp:close(S),
    parse_http(Resp).

recv_until_closed(S, Acc) ->
    case gen_tcp:recv(S, 0, 5000) of
        {ok, Data} -> recv_until_closed(S, <<Acc/binary, Data/binary>>);
        {error, closed} -> Acc
    end.

parse_http(Resp) ->
    [Head, Body] = binary:split(Resp, <<"\r\n\r\n">>),
    [StatusLine | _] = binary:split(Head, <<"\r\n">>),
    [_, CodeBin | _] = binary:split(StatusLine, <<" ">>, [global, trim_all]),
    Code = binary_to_integer(CodeBin),
    {Code, json:decode(Body)}.

%%%_* Peer-side setup (runs on the peer) ----------------------------------

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

%% The serving runtime on a peer: the admin app (so `/api/v1/node' and
%% `broker_drain' exist) plus a real MQTT listener on an ephemeral port. The
%% data-plane apps are started so a CONNECT is actually served rather than
%% accepted and dropped.
start_runtime() ->
    {ok, RaDir} = application:get_env(broker_cluster, data_dir),
    DataDir = filename:join(RaDir, "data"),
    ok = filelib:ensure_dir(filename:join([DataDir, "x"])),
    _ = [application:load(A) || A <- core_apps() ++ rest_apps()],
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok = application:set_env(broker_storage, data_dir, DataDir),
    ok = application:set_env(broker_storage, wal, #{require_durable_fs => false}),
    ok = application:set_env(broker_queue, partitions, ?PARTITIONS),
    ok = application:set_env(broker_router, partitions, ?PARTITIONS),
    ok = application:set_env(broker_subscription, data_dir, DataDir),
    ok = application:set_env(broker_retain, data_dir, DataDir),
    ok = application:set_env(broker_security, users, []),
    ok = application:set_env(broker_security, acl, allow),
    ok = application:set_env(broker_security, require_auth, []),
    %% The admission gate stays out of the way here: this suite is about dispatch
    %% following the view, and `broker_admission_SUITE' owns the gate's own
    %% semantics.
    ok = application:set_env(broker_security, limits, #{max_connections => infinity}),
    %% A WAL backend needs its partitions created before `broker_queue' starts:
    %% the queue app opens every configured partition at boot and fails with
    %% `partition_not_found' if the backend has no such directory. Storage must be
    %% up first so the backend can create them.
    {ok, _} = application:ensure_all_started(broker_storage),
    [ok = broker_queue:create_partition(P) || P <- broker_queue:partition_ids()],
    {ok, _} = application:ensure_all_started(broker_router),
    {ok, _} = application:ensure_all_started(broker_queue),
    {ok, _} = application:ensure_all_started(broker_subscription),
    {ok, _} = application:ensure_all_started(broker_retain),
    {ok, _} = application:ensure_all_started(broker_event),
    {ok, _} = application:ensure_all_started(broker_security),
    %% One MQTT listener per peer, ephemeral port, loopback.
    ok = application:set_env(broker_proto_mqtt, listeners,
                             [#{port => 0, ip => {127, 0, 0, 1},
                                num_acceptors => 2, max_connections => 64}]),
    ok = application:set_env(broker_proto_mqtt, mqtt_max_qos, 2),
    ok = application:set_env(broker_proto_mqtt, max_keepalive_ms, 60000),
    ok = application:set_env(broker_proto_mqtt, pull_poll_interval_ms, 1000),
    ok = application:set_env(broker_proto_mqtt, inflight_snapshot_ms, 20),
    {ok, _} = application:ensure_all_started(broker_proto_mqtt),
    %% Admin last: it needs the data plane already up to report readiness truly.
    %% Load it first: `set_env' before `load' is overridden by the `.app' file's
    %% own `port => 8090', which would bind the project's fixed port instead of an
    %% ephemeral one.
    _ = application:load(broker_admin),
    ok = application:set_env(broker_admin, token, <<>>),
    ok = application:set_env(broker_admin, bind, {127, 0, 0, 1}),
    ok = application:set_env(broker_admin, port, 0),
    ok = application:set_env(broker_admin, backup_dir,
                             "/tmp/opencode/s12b_lb_backups"),
    {ok, _} = application:ensure_all_started(broker_admin),
    ok.

core_apps() ->
    [broker_storage, broker_router, broker_queue].

rest_apps() ->
    [broker_subscription, broker_retain, broker_event, broker_security,
     broker_proto_mqtt].

%% Read back the ephemeral port the listener actually bound.
mqtt_listener_port() ->
    ranch:get_port({mqtt, 0}).
