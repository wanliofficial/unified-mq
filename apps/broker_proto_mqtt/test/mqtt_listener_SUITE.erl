%% @doc T12: the real ranch listener on a real TCP port.
%%
%% T9's suite proved the connection FSM over an out-of-band TCP pair
%% (`mqtt_client.hrl'): socket handoff, protocol posture, delivery. What only
%% the listener can add is the path a network client actually takes:
%%
%%   accept -> mqtt_listener:start_link/3 -> mqtt_conn_sup:start_conn/2
%%          -> ranch controlling_process -> {handshake, ...} -> CONNACK
%%
%% The cases pin the three things the boot step buys (design §6):
%%  * CONNECT is answered once the app tree is up, and the process serving it is
%%    a real child of `mqtt_conn_sup' (not an ad-hoc acceptor process);
%%  * the whole publish/subscribe/ack chain works through a listening socket;
%%  * the Router index is already backfilled from `.subs' when the listener
%%    starts accepting — the step-4 ordering (R1b's implementation premise);
%%  * stopping the listener reaps its connections (L15): ranch's conns sup sends
%%    them a shutdown exit, so `application:stop' does not leak sockets.
%%
%% Ports are ephemeral (`port => 0'), so the suite cannot collide with another
%% node's 1883 or with a parallel CT run. The actual port is read back from
%% ranch, never assumed.
-module(mqtt_listener_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include("mqtt_frame.hrl").

-define(N, 2).
-define(REF, {mqtt, 0}).
-define(LISTENERS, [#{port => 0,
                      num_acceptors => 4,
                      mqtt_max_qos => 1,
                      max_keepalive_ms => 60000}]).

all() ->
    [listener_answers_connect_with_connack_zero,
     listener_serves_publish_subscribe_puback_and_delivery,
     restored_subscription_is_indexed_before_the_listener_accepts,
     stopping_the_listener_closes_its_connections].

%%%_* Fixtures ----------------------------------------------------------------

init_per_suite(Config) ->
    %% `_ =' on purpose (J10/L19): another suite on this node has usually
    %% loaded these already, and `{already_loaded, _}' is not a failure here.
    _ = application:load(broker_storage),
    _ = application:load(broker_router),
    _ = application:load(broker_queue),
    _ = application:load(broker_subscription),
    _ = application:load(broker_security),
    _ = application:load(broker_proto_mqtt),
    Config.

end_per_suite(_Config) ->
    stop_all(),
    ok.

init_per_testcase(Case, Config) ->
    stop_all(),
    DataDir = filename:join([?config(priv_dir, Config), atom_to_list(Case), "data"]),
    ok = filelib:ensure_dir(filename:join([DataDir, "x"])),
    %% Every knob the conn reads is written in every case (J10's per-case form):
    %% no earlier case can poison this one's config.
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok = application:set_env(broker_storage, data_dir, DataDir),
    ok = application:set_env(broker_storage, wal, #{require_durable_fs => false}),
    ok = application:set_env(broker_queue, partitions, ?N),
    ok = application:set_env(broker_router, partitions, ?N),
    ok = application:set_env(broker_subscription, data_dir, DataDir),
    %% The retain snapshot is a fourth per-data-dir artifact (ADR-018 §18.2):
    %% leaving it unset would make every case share the repo's data/retain.
    ok = application:set_env(broker_retain, data_dir, DataDir),
    ok = application:set_env(broker_subscription, cursor_max_regress, 64),
    ok = application:set_env(broker_subscription, cursor_flush_interval_ms, 100),
    ok = application:set_env(broker_subscription, inflight_flush_interval_ms, 100),
    ok = application:set_env(broker_security, users, []),
    ok = application:set_env(broker_proto_mqtt, listeners, ?LISTENERS),
    ok = application:set_env(broker_proto_mqtt, pull_page_limit, 32),
    ok = application:set_env(broker_proto_mqtt, max_inflight, 64),
    ok = application:set_env(broker_proto_mqtt, pull_poll_interval_ms, 1000),
    ok = application:set_env(broker_proto_mqtt, inflight_snapshot_ms, 20),
    ok = application:set_env(broker_proto_mqtt, max_keepalive_ms, 60000),
    ok = application:set_env(broker_proto_mqtt, mqtt_max_qos, 1),
    {ok, _Started} = application:ensure_all_started(broker_storage),
    [{data_dir, DataDir} | Config].

end_per_testcase(_Case, _Config) ->
    kill_conns(),
    stop_all(),
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok.

%% The same out-of-band bootstrap as the other suites: partitions first (H8),
%% then one `ensure_all_started' walks storage -> ... -> proto_mqtt, whose last
%% child is the listener (§6 step 4).
boot() ->
    ensure_partitions(),
    {ok, _Started} = application:ensure_all_started(broker_proto_mqtt),
    listener_port().

ensure_partitions() ->
    lists:foreach(
      fun(P) ->
              case broker_queue:create_partition(P) of
                  ok -> ok;
                  {error, {already_exists, P}} -> ok;
                  {error, Other} -> exit({create_partition, P, Other})
              end
      end, broker_queue:partition_ids()).

listener_port() ->
    case ranch:get_port(?REF) of
        Port when is_integer(Port) -> Port;
        Other -> exit({listener_port_unavailable, Other})
    end.

stop_all() ->
    _ = application:stop(broker_proto_mqtt),
    _ = application:stop(broker_retain),
    _ = application:stop(broker_subscription),
    _ = application:stop(broker_queue),
    _ = application:stop(broker_router),
    _ = application:stop(broker_storage),
    cleanup_writers(),
    ok.

kill_conns() ->
    case whereis(mqtt_conn_sup) of
        undefined ->
            ok;
        Sup ->
            lists:foreach(
              fun({_, Pid, worker, _}) when is_pid(Pid) -> exit(Pid, kill);
                 (_) -> ok
              end, supervisor:which_children(Sup))
    end.

cleanup_writers() ->
    case whereis(broker_storage_wal_sup) of
        undefined ->
            ok;
        _Sup ->
            lists:foreach(
              fun({Id, _Child, _Type, _Modules}) ->
                      _ = supervisor:terminate_child(broker_storage_wal_sup, Id),
                      ok = supervisor:delete_child(broker_storage_wal_sup, Id)
              end,
              supervisor:which_children(broker_storage_wal_sup))
    end.

%%%_* Cases -------------------------------------------------------------------

%% The listener is the thing T9 could not test: a socket that arrived from
%% ranch's acceptor, had its ownership transferred by ranch, and was armed by the
%% `{handshake, ...}' message. A CONNACK proves all three happened in order.
listener_answers_connect_with_connack_zero(_Config) ->
    Port = boot(),
    Sock = connect(Port),
    #connack{session_present = 0, return_code = 0} =
        open(Sock, #{client_id => <<"listener-c1">>}),
    %% The serving process is a child of the connection supervisor, so the
    %% listener really went through `start_conn/2' instead of inventing a
    %% parallel connection path.
    [Pid] = conn_pids(),
    true = is_process_alive(Pid),
    #ping{kind = resp} = ping(Sock),
    Monitor = erlang:monitor(process, Pid),
    ok = gen_tcp:close(Sock),
    {ok, Reason} = expect_down(Monitor, Pid),
    true = lists:member(Reason, [normal, noproc]),
    ok.

%% Through the listener only: the subscriber and the publisher are two real TCP
%% clients, and the QoS1 contract is observed on both sockets (PUBACK after the
%% append, PUBLISH then cursor advance after the consumer's ack).
listener_serves_publish_subscribe_puback_and_delivery(_Config) ->
    Port = boot(),
    Sub = connect(Port),
    #connack{return_code = 0} =
        open(Sub, #{client_id => <<"sub">>, clean => false}),
    SubRef = {<<"sub">>, <<"t/#">>},
    #suback{return_codes = [1]} = subscribe(Sub, 1, <<"t/#">>, 1),
    P = route_partition(<<"t/a">>),
    {ok, Anchor} = broker_subscription:cursor(SubRef, P),
    Pub = connect(Port),
    #connack{return_code = 0} = open(Pub, #{client_id => <<"pub">>}),
    ok = send_publish(Pub, 7, <<"t/a">>, <<"hello">>, 1),
    #puback{packet_id = 7} = recv(Pub),
    #publish{qos = 1, dup = 0, topic = <<"t/a">>, payload = <<"hello">>,
             packet_id = WirePid} = recv(Sub),
    ok = send_puback(Sub, WirePid),
    wait_cursor_after(SubRef, P, Anchor),
    ok = gen_tcp:close(Sub),
    ok = gen_tcp:close(Pub),
    ok.

%% Boot step 2+3 before step 4: phase 1 authors a non-clean subscription with no
%% listener running; phase 2 is a cold boot from `.subs' only. The Router index is
%% not persistent (ADR-010), so the SubRef visible after boot can only have come
%% from the subscription worker's boot — which must have completed before the
%% listener accepted the CONNECT below.
restored_subscription_is_indexed_before_the_listener_accepts(_Config) ->
    %% Phase 1: no listener, just enough stack to write `.subs'.
    ok = application:set_env(broker_proto_mqtt, listeners, []),
    ensure_partitions(),
    {ok, _} = application:ensure_all_started(broker_subscription),
    {ok, SubRef} = broker_subscription:subscribe(<<"restored">>, <<"boot/#">>, 1),
    %% Leave the client offline but keep its row: that is exactly the state a
    %% restart must rebuild.
    stop_all(),

    %% Phase 2: the real listener, booting from disk.
    ok = application:set_env(broker_proto_mqtt, listeners, ?LISTENERS),
    Port = boot(),
    {ok, Env} = broker_router:env(),
    {ok, [{_P, Refs} | _]} =
        broker_router_hash:route(<<"boot/x">>, #{}, Env),
    true = lists:member(SubRef, Refs),
    %% ...and the listener is already accepting, with the restored session
    %% visible to the first CONNECT it serves.
    Sock = connect(Port),
    #connack{session_present = 1, return_code = 0} =
        open(Sock, #{client_id => <<"restored">>, clean => false}),
    ok = gen_tcp:close(Sock),
    ok.

%% L15: the connection children are `temporary', so stopping the connection
%% supervisor would let them leak — the listener is what makes `application:stop'
%% reap the fleet, because ranch's conns sup owns an exit path to every protocol
%% pid it started. If the explicit link in `mqtt_listener' were missing, this case
%% would see a live process and a socket that stays open.
stopping_the_listener_closes_its_connections(_Config) ->
    Port = boot(),
    Sock = connect(Port),
    #connack{return_code = 0} = open(Sock, #{client_id => <<"leak-check">>}),
    [Pid] = conn_pids(),
    Monitor = erlang:monitor(process, Pid),
    ok = application:stop(broker_proto_mqtt),
    {ok, Reason} = expect_down(Monitor, Pid),
    true = lists:member(Reason, [normal, shutdown, {shutdown, listener_stopped},
                                 killed]),
    {error, closed} = gen_tcp:recv(Sock, 0, 5000),
    ok.

%%%_* Listener-side MQTT client (real TCP, one frame at a time) ----------------

connect(Port) ->
    {ok, Sock} = gen_tcp:connect({127, 0, 0, 1}, Port,
                                 [binary, {active, false}, {nodelay, true}]),
    Sock.

open(Sock, Knobs) ->
    ok = gen_tcp:send(Sock, mqtt_frame:encode(connect_frame(Knobs))),
    recv(Sock).

connect_frame(Knobs) ->
    #connect{client_id = maps:get(client_id, Knobs),
             clean_session = case maps:get(clean, Knobs, true) of
                                 true -> 1;
                                 false -> 0
                             end,
             keep_alive = maps:get(keep_alive, Knobs, 0),
             username = maps:get(username, Knobs, undefined),
             password = maps:get(password, Knobs, undefined)}.

subscribe(Sock, PacketId, Filter, QoS) ->
    ok = gen_tcp:send(Sock, mqtt_frame:encode(
                             #subscribe{packet_id = PacketId,
                                        topics = [{Filter, QoS}]})),
    recv(Sock).

send_publish(Sock, PacketId, Topic, Payload, QoS) ->
    gen_tcp:send(Sock, mqtt_frame:encode(
                         #publish{qos = QoS, topic = Topic,
                                  packet_id = PacketId, payload = Payload})).

send_puback(Sock, PacketId) ->
    gen_tcp:send(Sock, mqtt_frame:encode(#puback{packet_id = PacketId})).

ping(Sock) ->
    ok = gen_tcp:send(Sock, mqtt_frame:encode(#ping{kind = req})),
    recv(Sock).

%% Accumulate-and-drain, the same shape as `mqtt_client.hrl': one socket read may
%% carry a frame prefix, a whole frame, or several frames.
recv(Sock) ->
    recv(Sock, 5000).

recv(Sock, Timeout) ->
    Buf = buf(Sock),
    case (catch mqtt_frame:decode(Buf)) of
        {ok, Frame, Rest} ->
            set_buf(Sock, Rest),
            Frame;
        {more, _} ->
            case gen_tcp:recv(Sock, 0, Timeout) of
                {ok, Data} ->
                    set_buf(Sock, <<Buf/binary, Data/binary>>),
                    recv(Sock, Timeout);
                {error, Reason} ->
                    exit({listener_recv, Reason, Buf})
            end;
        {error, Reason} ->
            exit({listener_recv_malformed, Reason, Buf})
    end.

buf(Sock) ->
    case erlang:get({listener_buf, Sock}) of
        undefined -> <<>>;
        Buf -> Buf
    end.

set_buf(Sock, Buf) ->
    erlang:put({listener_buf, Sock}, Buf),
    ok.

%%%_* Helpers -----------------------------------------------------------------

conn_pids() ->
    [Pid || {_, Pid, worker, _} <- supervisor:which_children(mqtt_conn_sup),
            is_pid(Pid)].

route_partition(Topic) ->
    {ok, Env} = broker_router:env(),
    {ok, [{Partition, _Refs} | _]} = broker_router_hash:route(Topic, #{}, Env),
    Partition.

wait_cursor_after(SubRef, Partition, Anchor) ->
    wait_cursor_after(SubRef, Partition, Anchor, 3000).

wait_cursor_after(SubRef, Partition, Anchor, Budget) ->
    case broker_subscription:cursor(SubRef, Partition) of
        {ok, Offset} when Offset > Anchor ->
            ok;
        _ when Budget > 50 ->
            timer:sleep(25),
            wait_cursor_after(SubRef, Partition, Anchor, Budget - 25);
        Other ->
            exit({listener_cursor_stuck, SubRef, Partition, Anchor, Other})
    end.

expect_down(Monitor, Pid) ->
    receive
        {'DOWN', Monitor, process, Pid, Reason} -> {ok, Reason}
    after 5000 -> exit({listener_no_down, Pid})
    end.
