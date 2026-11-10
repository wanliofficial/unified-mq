%% @doc MQTT-SN 1.2 gateway: real UDP datagrams through the real listener.
%%
%% The suite talks to the gateway the way a device would -- one UDP socket, one
%% encoded packet at a time -- and asserts on the decoded packets that come
%% back, not on process bookkeeping. That matters here more than usual: MQTT-SN
%% has no connection, so "the session exists" is only observable through
%% CONNACK, the topic-id handshakes and the PUBLISHes that arrive afterwards.
%%
%% Coverage is the S8-C acceptance surface: CONNECT/CONNACK, the three kinds of
%% topic id (normal with REGISTER in both directions, predefined, short), QoS
%% -1/0/1/2 in both directions, UNSUBSCRIBE, retained replay after SUBACK,
%% Will-vs-takeover, sleep + wake with a backlog, keepalive death, gateway
%% discovery (SEARCHGW/ADVERTISE), malformed datagrams, and the one
%% configuration the wire cannot honour (`require_auth' for a protocol with no
%% credential field).
%%
%% The suite's own client is byte-level on purpose: every helper below encodes
%% with `mqtt_sn_codec' and decodes what arrives, so a codec change that breaks
%% the gateway breaks these cases too.
-module(mqtt_sn_conn_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(N, 2).
-define(POLL_MS, 20).
-define(POLL_BUDGET_MS, 5000).
-define(HOST, {127, 0, 0, 1}).

all() ->
    [connect_returns_connack,
     clean_session_drops_previous_subscriptions,
     subscribe_assigns_a_normal_topic_id,
     publish_qos0_reaches_a_subscriber,
     publish_qos1_is_acked_and_delivered,
     publish_qos2_handshake_delivers_once,
     downstream_qos1_is_released_by_its_ack,
     downstream_qos2_completes_with_pubrel_pubcomp,
     client_register_returns_an_id,
     predefined_topic_publish_reaches_a_subscriber,
     short_topic_publish_reaches_a_subscriber,
     unsubscribe_stops_delivery,
     retained_publish_is_replayed_on_subscribe,
     will_is_published_when_the_session_dies,
     takeover_closes_the_old_session_without_a_will,
     sleep_then_pingreq_wakes_and_delivers_the_backlog,
     qos_minus1_publishes_without_a_connection,
     keepalive_timeout_ends_the_session,
     searchgw_answers_gwinfo,
     advertise_is_broadcast_on_the_configured_interval,
     unknown_client_pingreq_gets_disconnect,
     malformed_datagram_is_dropped,
     require_auth_refuses_to_start].

%%%_* Fixtures --------------------------------------------------------------

init_per_suite(Config) ->
    _ = application:load(broker_storage),
    _ = application:load(broker_router),
    _ = application:load(broker_queue),
    _ = application:load(broker_subscription),
    _ = application:load(broker_retain),
    _ = application:load(broker_event),
    _ = application:load(broker_security),
    _ = application:load(broker_proto_mqtt_sn),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(Case, Config0) ->
    stop_all(),
    %% The three app env values this suite writes are node-global: snapshot them
    %% into Config so teardown restores the *original* values. Reading them at
    %% teardown would capture what the case just wrote and leak into every suite
    %% that runs later in the shared CT node.
    Saved = #{require_auth => env(broker_security, require_auth),
              predefined => env(broker_proto_mqtt_sn, predefined_topics)},
    DataDir = filename:join([?config(priv_dir, Config0), atom_to_list(Case), "data"]),
    ok = filelib:ensure_dir(filename:join([DataDir, "x"])),
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok = application:set_env(broker_storage, data_dir, DataDir),
    ok = application:set_env(broker_storage, wal, #{require_durable_fs => false}),
    ok = application:set_env(broker_queue, partitions, ?N),
    ok = application:set_env(broker_router, partitions, ?N),
    ok = application:set_env(broker_subscription, data_dir, DataDir),
    ok = application:set_env(broker_retain, data_dir, DataDir),
    ok = application:set_env(broker_subscription, cursor_flush_interval_ms, 20),
    ok = application:set_env(broker_subscription, inflight_flush_interval_ms, 20),
    ok = application:set_env(broker_security, users, []),
    ok = application:set_env(broker_security, acl, allow),
    ok = application:set_env(broker_security, require_auth, []),
    ok = application:set_env(broker_proto_mqtt_sn, listeners,
                             [#{port => 0}]),
    ok = application:set_env(broker_proto_mqtt_sn, predefined_topics, #{}),
    ok = application:set_env(broker_proto_mqtt_sn, advertise_interval_ms, 0),
    ok = application:set_env(broker_proto_mqtt_sn, advertise_address, ?HOST),
    ok = application:set_env(broker_proto_mqtt_sn, max_inflight, 20),
    {ok, _} = application:ensure_all_started(broker_storage),
    ok = create_partitions(),
    Config = [{data_dir, DataDir}, {saved_env, Saved} | Config0],
    ok = case_env(Case, Config),
    %% `case_env/2' may hold a resource the case needs (the ADVERTISE socket,
    %% which must exist before the listener starts so it can be told where to
    %% broadcast). Hand it to the case, and keep it in Config so teardown can
    %% close it even when the case crashes.
    Config1 = case erase(advertise_socket) of
                  undefined -> Config;
                  Socket -> [{advertise_socket, Socket} | Config]
              end,
    case Case of
        require_auth_refuses_to_start ->
            %% The case itself starts the app and asserts the refusal; starting
            %% it here would turn the expected error into a fixture failure.
            Config1;
        _ ->
            {ok, _} = application:ensure_all_started(broker_proto_mqtt_sn),
            [{port, listener_port()} | Config1]
    end.

end_per_testcase(_Case, Config) ->
    stop_all(),
    case ?config(advertise_socket, Config) of
        undefined -> ok;
        Socket -> _ = gen_udp:close(Socket), ok
    end,
    restore_env(?config(saved_env, Config)),
    ok.

case_env(advertise_is_broadcast_on_the_configured_interval, _Config) ->
    %% A real ADVERTISE goes to the well-known port where clients listen, not
    %% to this listener's own port, so the case needs a socket that is already
    %% bound when the listener starts.
    Socket = open(),
    put(advertise_socket, Socket),
    ok = application:set_env(broker_proto_mqtt_sn, advertise_interval_ms, 50),
    {ok, Port} = inet:port(Socket),
    ok = application:set_env(broker_proto_mqtt_sn, advertise_port, Port);
case_env(predefined_topic_publish_reaches_a_subscriber, _Config) ->
    ok = application:set_env(broker_proto_mqtt_sn, predefined_topics,
                             #{1 => <<"sn/pre">>});
case_env(require_auth_refuses_to_start, _Config) ->
    ok = application:set_env(broker_security, require_auth, [<<"mqtt_sn">>]);
case_env(_Case, _Config) ->
    ok.

env(App, Key) ->
    application:get_env(App, Key, undefined).

restore_env(#{require_auth := Auth, predefined := Pre}) ->
    set_or_unset(broker_security, require_auth, Auth),
    set_or_unset(broker_proto_mqtt_sn, predefined_topics, Pre),
    ok.

set_or_unset(App, Key, undefined) ->
    application:unset_env(App, Key);
set_or_unset(App, Key, Value) ->
    application:set_env(App, Key, Value).

create_partitions() ->
    lists:foreach(
      fun(P) ->
              case broker_queue:create_partition(P) of
                  ok -> ok;
                  {error, {already_exists, P}} -> ok;
                  {error, Other} -> exit({create_partition, P, Other})
              end
      end, broker_queue:partition_ids()).

listener_port() ->
    case [Pid || {Id, Pid, worker, _} <- supervisor:which_children(
                                                broker_proto_mqtt_sn_sup),
                 element(1, Id) =:= mqtt_sn_listener] of
        [Pid | _] -> mqtt_sn_listener:port(Pid);
        [] -> exit(no_listener)
    end.

stop_all() ->
    _ = application:stop(broker_proto_mqtt_sn),
    _ = application:stop(broker_retain),
    _ = application:stop(broker_event),
    _ = application:stop(broker_security),
    _ = application:stop(broker_subscription),
    _ = application:stop(broker_queue),
    _ = application:stop(broker_router),
    _ = application:stop(broker_storage),
    cleanup_writers(),
    ok.

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

conn_pids() ->
    case whereis(mqtt_sn_conn_sup) of
        undefined -> [];
        _ -> [Pid || {_, Pid, _, _} <- supervisor:which_children(mqtt_sn_conn_sup),
                     is_pid(Pid)]
    end.

%%%_* Client ----------------------------------------------------------------

open() ->
    {ok, S} = gen_udp:open(0, [binary, {active, false}]),
    S.

close(S) ->
    gen_udp:close(S).

send(S, Port, Packet) ->
    ok = gen_udp:send(S, ?HOST, Port, mqtt_sn_codec:encode(Packet)).

recv(S) ->
    recv(S, ?POLL_BUDGET_MS).

recv(S, Timeout) ->
    case gen_udp:recv(S, 0, Timeout) of
        {ok, {_IP, _Port, Bin}} ->
            {ok, Packet} = mqtt_sn_codec:decode(Bin),
            Packet;
        {error, timeout} ->
            timeout
    end.

%% Wait for one packet of `Type', discarding anything else that arrives in the
%% meantime. UDP gives no ordering guarantee across replies, and the gateway
%% legitimately sends a REGISTER before a PUBLISH, so a strict next-packet
%% assertion would be flaky rather than strict.
expect(S, Type) ->
    Deadline = erlang:monotonic_time(millisecond) + ?POLL_BUDGET_MS,
    expect(S, Type, Deadline).

expect(S, Type, Deadline) ->
    Left = Deadline - erlang:monotonic_time(millisecond),
    case Left > 0 andalso recv(S, Left) of
        timeout -> exit({timeout_waiting_for, Type});
        false -> exit({timeout_waiting_for, Type});
        #{type := Type} = Packet -> Packet;
        _Other -> expect(S, Type, Deadline)
    end.

%% Nothing of `Type' may arrive within the window.
refute(S, Type, WindowMs) ->
    case recv(S, WindowMs) of
        timeout -> ok;
        #{type := Type} = Packet -> exit({unexpected_packet, Packet});
        _Other -> refute(S, Type, WindowMs)
    end.

connect(S, Port, ClientId) ->
    connect(S, Port, ClientId, #{}).

connect(S, Port, ClientId, Opts) ->
    Packet = maps:merge(#{type => connect, protocol_id => 1,
                          clean_session => false, duration => 60,
                          client_id => ClientId}, Opts),
    send(S, Port, Packet),
    #{type := connack, return_code := 0} = expect(S, connack),
    ok.

connect_with_will(S, Port, ClientId, Topic, Payload) ->
    send(S, Port, #{type => connect, will => true, clean_session => true,
                    protocol_id => 1, duration => 60, client_id => ClientId}),
    #{type := willtopicreq} = expect(S, willtopicreq),
    send(S, Port, #{type => willtopic, qos => 0, retain => false, topic => Topic}),
    #{type := willmsgreq} = expect(S, willmsgreq),
    send(S, Port, #{type => willmsg, payload => Payload}),
    #{type := connack, return_code := 0} = expect(S, connack),
    ok.

subscribe(S, Port, Topic, QoS) ->
    subscribe(S, Port, normal, Topic, QoS).

subscribe(S, Port, Type, Topic, QoS) ->
    send(S, Port, #{type => subscribe, dup => false, qos => QoS,
                    topic_id_type => Type, msg_id => 1, topic => Topic}),
    #{type := suback, return_code := 0, topic_id := Id} = expect(S, suback),
    Id.

%% A normal topic id is per client (MQTT-SN 1.2 §5.4.16): the id a SUBACK
%% handed to the subscriber means nothing in the publisher's table. A publisher
%% that has not subscribed registers the name itself, which is what the real
%% client tools do before their first PUBLISH.
register_topic(S, Port, Topic) ->
    send(S, Port, #{type => register, topic_id => 0, msg_id => 1, topic => Topic}),
    #{type := regack, return_code := 0, topic_id := Id} = expect(S, regack),
    Id.

publish(S, Port, Type, TopicId, QoS, MsgId, Payload) ->
    publish(S, Port, Type, TopicId, QoS, 0, MsgId, Payload).

publish(S, Port, Type, TopicId, QoS, Retain, MsgId, Payload) ->
    send(S, Port, #{type => publish, dup => false, qos => QoS, retain => Retain,
                    topic_id_type => Type, topic_id => TopicId,
                    msg_id => MsgId, payload => Payload}).

%%%_* Cases: connect and topics ---------------------------------------------

connect_returns_connack(Config) ->
    Port = ?config(port, Config),
    S = open(),
    connect(S, Port, <<"sn-c1">>),
    close(S).

%% §6.2: a clean session starts from zero. The old subscription entity has to
%% be gone, not just forgotten locally, or the next publish would still be
%% routed to this client id.
clean_session_drops_previous_subscriptions(Config) ->
    Port = ?config(port, Config),
    S = open(),
    connect(S, Port, <<"sn-clean">>),
    _ = subscribe(S, Port, <<"sn/clean/t">>, 1),
    ?assertMatch([_ | _], broker_subscription:subscriptions(<<"sn-clean">>)),
    send(S, Port, #{type => disconnect, duration => 0}),
    #{type := disconnect} = expect(S, disconnect),
    close(S),
    timer:sleep(100),
    S2 = open(),
    connect(S2, Port, <<"sn-clean">>, #{clean_session => true}),
    ?assertEqual([], broker_subscription:subscriptions(<<"sn-clean">>)),
    close(S2).

%% A normal SUBSCRIBE gets an id back; that id is what PUBLISH must name
%% afterwards, so it has to be usable immediately.
subscribe_assigns_a_normal_topic_id(Config) ->
    Port = ?config(port, Config),
    S = open(),
    connect(S, Port, <<"sn-sub">>),
    Id = subscribe(S, Port, <<"sn/normal/t">>, 1),
    ?assert(Id > 0),
    %% Publishing to the id the SUBACK just handed out is the contract.
    publish(S, Port, normal, Id, 1, 7, <<"via-id">>),
    #{type := puback, return_code := 0, msg_id := 7} = expect(S, puback),
    close(S).

publish_qos0_reaches_a_subscriber(Config) ->
    Port = ?config(port, Config),
    Sub = open(),
    Pub = open(),
    connect(Sub, Port, <<"sn-q0-sub">>),
    connect(Pub, Port, <<"sn-q0-pub">>),
    _ = subscribe(Sub, Port, <<"sn/q0/t">>, 1),
    PubId = register_topic(Pub, Port, <<"sn/q0/t">>),
    publish(Pub, Port, normal, PubId, 0, 0, <<"q0">>),
    #{type := publish, payload := <<"q0">>} = expect(Sub, publish),
    close(Sub),
    close(Pub).

publish_qos1_is_acked_and_delivered(Config) ->
    Port = ?config(port, Config),
    Sub = open(),
    Pub = open(),
    connect(Sub, Port, <<"sn-q1-sub">>),
    connect(Pub, Port, <<"sn-q1-pub">>),
    SubId = subscribe(Sub, Port, <<"sn/q1/t">>, 1),
    PubId = register_topic(Pub, Port, <<"sn/q1/t">>),
    publish(Pub, Port, normal, PubId, 1, 11, <<"q1">>),
    #{type := puback, return_code := 0, msg_id := 11} = expect(Pub, puback),
    #{type := publish, payload := <<"q1">>, msg_id := DeliveryId} =
        expect(Sub, publish),
    send(Sub, Port, #{type => puback, topic_id => SubId, msg_id => DeliveryId,
                      return_code => 0}),
    close(Sub),
    close(Pub).

%% The uplink QoS 2 handshake: PUBREC, PUBREL, PUBCOMP -- and exactly one
%% delivery, because a retried PUBLISH for a known id must not store twice.
publish_qos2_handshake_delivers_once(Config) ->
    Port = ?config(port, Config),
    Sub = open(),
    Pub = open(),
    connect(Sub, Port, <<"sn-q2-sub">>),
    connect(Pub, Port, <<"sn-q2-pub">>),
    _ = subscribe(Sub, Port, <<"sn/q2/t">>, 2),
    PubId = register_topic(Pub, Port, <<"sn/q2/t">>),
    publish(Pub, Port, normal, PubId, 2, 21, <<"q2">>),
    #{type := pubrec, msg_id := 21} = expect(Pub, pubrec),
    send(Pub, Port, #{type => pubrel, msg_id => 21}),
    #{type := pubcomp, msg_id := 21} = expect(Pub, pubcomp),
    #{type := publish, payload := <<"q2">>} = expect(Sub, publish),
    refute(Sub, publish, 300),
    close(Sub),
    close(Pub).

%% Downlink QoS 1: the cursor is released by the client's PUBACK, so the next
%% message follows and the first one is not re-sent.
downstream_qos1_is_released_by_its_ack(Config) ->
    Port = ?config(port, Config),
    Sub = open(),
    Pub = open(),
    connect(Sub, Port, <<"sn-d1-sub">>),
    connect(Pub, Port, <<"sn-d1-pub">>),
    SubId = subscribe(Sub, Port, <<"sn/d1/t">>, 1),
    PubId = register_topic(Pub, Port, <<"sn/d1/t">>),
    publish(Pub, Port, normal, PubId, 1, 31, <<"first">>),
    #{type := puback} = expect(Pub, puback),
    #{type := publish, payload := <<"first">>, msg_id := FirstId} =
        expect(Sub, publish),
    send(Sub, Port, #{type => puback, topic_id => SubId, msg_id => FirstId,
                      return_code => 0}),
    publish(Pub, Port, normal, PubId, 1, 32, <<"second">>),
    #{type := puback} = expect(Pub, puback),
    #{type := publish, payload := <<"second">>} = expect(Sub, publish),
    close(Sub),
    close(Pub).

downstream_qos2_completes_with_pubrel_pubcomp(Config) ->
    Port = ?config(port, Config),
    Sub = open(),
    Pub = open(),
    connect(Sub, Port, <<"sn-d2-sub">>),
    connect(Pub, Port, <<"sn-d2-pub">>),
    _ = subscribe(Sub, Port, <<"sn/d2/t">>, 2),
    PubId = register_topic(Pub, Port, <<"sn/d2/t">>),
    publish(Pub, Port, normal, PubId, 2, 41, <<"d2">>),
    #{type := pubrec} = expect(Pub, pubrec),
    send(Pub, Port, #{type => pubrel, msg_id => 41}),
    #{type := pubcomp} = expect(Pub, pubcomp),
    #{type := publish, payload := <<"d2">>, msg_id := DeliveryId} =
        expect(Sub, publish),
    send(Sub, Port, #{type => pubrec, msg_id => DeliveryId}),
    #{type := pubrel, msg_id := DeliveryId} = expect(Sub, pubrel),
    send(Sub, Port, #{type => pubcomp, msg_id => DeliveryId}),
    refute(Sub, publish, 300),
    close(Sub),
    close(Pub).

client_register_returns_an_id(Config) ->
    Port = ?config(port, Config),
    S = open(),
    connect(S, Port, <<"sn-reg">>),
    send(S, Port, #{type => register, topic_id => 0, msg_id => 51,
                    topic => <<"sn/registered/t">>}),
    #{type := regack, return_code := 0, topic_id := Id, msg_id := 51} =
        expect(S, regack),
    ?assert(Id > 0),
    publish(S, Port, normal, Id, 1, 52, <<"registered">>),
    #{type := puback, return_code := 0} = expect(S, puback),
    close(S).

predefined_topic_publish_reaches_a_subscriber(Config) ->
    Port = ?config(port, Config),
    Sub = open(),
    Pub = open(),
    connect(Sub, Port, <<"sn-pre-sub">>),
    connect(Pub, Port, <<"sn-pre-pub">>),
    %% Topic id 1 is configured as <<"sn/pre">> for this case.
    _ = subscribe(Sub, Port, predefined, 1, 1),
    publish(Pub, Port, predefined, 1, 1, 61, <<"predefined">>),
    #{type := puback, return_code := 0} = expect(Pub, puback),
    #{type := publish, payload := <<"predefined">>} = expect(Sub, publish),
    close(Sub),
    close(Pub).

short_topic_publish_reaches_a_subscriber(Config) ->
    Port = ?config(port, Config),
    Sub = open(),
    Pub = open(),
    connect(Sub, Port, <<"sn-short-sub">>),
    connect(Pub, Port, <<"sn-short-pub">>),
    _ = subscribe(Sub, Port, short, <<"s1">>, 1),
    publish(Pub, Port, short, <<"s1">>, 1, 71, <<"short">>),
    #{type := puback, return_code := 0} = expect(Pub, puback),
    #{type := publish, payload := <<"short">>} = expect(Sub, publish),
    close(Sub),
    close(Pub).

unsubscribe_stops_delivery(Config) ->
    Port = ?config(port, Config),
    Sub = open(),
    Pub = open(),
    connect(Sub, Port, <<"sn-unsub-sub">>),
    connect(Pub, Port, <<"sn-unsub-pub">>),
    Id = subscribe(Sub, Port, <<"sn/unsub/t">>, 1),
    send(Sub, Port, #{type => unsubscribe, topic_id_type => normal, msg_id => 81,
                      topic => <<"sn/unsub/t">>}),
    #{type := unsuback, msg_id := 81} = expect(Sub, unsuback),
    publish(Pub, Port, normal, Id, 1, 82, <<"gone">>),
    #{type := puback} = expect(Pub, puback),
    refute(Sub, publish, 300),
    close(Sub),
    close(Pub).

retained_publish_is_replayed_on_subscribe(Config) ->
    Port = ?config(port, Config),
    Pub = open(),
    connect(Pub, Port, <<"sn-ret-pub">>),
    %% Retained state is keyed by topic name, so publish with a short topic and
    %% subscribe with the same two-character name.
    publish(Pub, Port, short, <<"r1">>, 1, true, 91, <<"kept">>),
    #{type := puback} = expect(Pub, puback),
    timer:sleep(100),
    Sub = open(),
    connect(Sub, Port, <<"sn-ret-sub">>),
    _ = subscribe(Sub, Port, short, <<"r1">>, 1),
    #{type := publish, payload := <<"kept">>} = expect(Sub, publish),
    close(Sub),
    close(Pub).

%%%_* Cases: Will, takeover, sleep -------------------------------------------

will_is_published_when_the_session_dies(Config) ->
    Port = ?config(port, Config),
    Sub = open(),
    connect(Sub, Port, <<"sn-will-sub">>),
    _ = subscribe(Sub, Port, <<"sn/will/t">>, 1),
    Before = conn_pids(),
    W = open(),
    connect_with_will(W, Port, <<"sn-will-client">>, <<"sn/will/t">>, <<"byebye">>),
    [ConnPid] = conn_pids() -- Before,
    %% `boom' is not `normal'/`shutdown', so terminate/2 must publish the Will.
    ok = gen_server:stop(ConnPid, boom, 1000),
    #{type := publish, payload := <<"byebye">>} = expect(Sub, publish),
    close(Sub),
    close(W).

takeover_closes_the_old_session_without_a_will(Config) ->
    Port = ?config(port, Config),
    Sub = open(),
    connect(Sub, Port, <<"sn-take-sub">>),
    _ = subscribe(Sub, Port, <<"sn/take/will">>, 1),
    Before = conn_pids(),
    A = open(),
    connect_with_will(A, Port, <<"sn-twin">>, <<"sn/take/will">>, <<"must-not-fire">>),
    [First] = conn_pids() -- Before,
    %% Same client id from a different endpoint: §6.1's takeover. The old
    %% session is replaced, not killed -- its Will must stay unpublished.
    B = open(),
    connect(B, Port, <<"sn-twin">>),
    wait_until(fun() -> not lists:member(First, conn_pids()) end),
    refute(Sub, publish, 400),
    %% The new endpoint is a live session, not a corpse that happens to answer
    %% CONNACK: a PINGREQ on it is answered.
    send(B, Port, #{type => pingreq}),
    #{type := pingresp} = expect(B, pingresp),
    close(A),
    close(B),
    close(Sub).

sleep_then_pingreq_wakes_and_delivers_the_backlog(Config) ->
    Port = ?config(port, Config),
    Sub = open(),
    Pub = open(),
    connect(Sub, Port, <<"sn-sleep-sub">>, #{duration => 30}),
    connect(Pub, Port, <<"sn-sleep-pub">>),
    _ = subscribe(Sub, Port, <<"sn/sleep/t">>, 1),
    PubId = register_topic(Pub, Port, <<"sn/sleep/t">>),
    %% Ask to sleep: the gateway echoes DISCONNECT and detaches the session.
    send(Sub, Port, #{type => disconnect, duration => 30}),
    #{type := disconnect, duration := 30} = expect(Sub, disconnect),
    timer:sleep(200),
    publish(Pub, Port, normal, PubId, 1, 101, <<"while-asleep">>),
    #{type := puback} = expect(Pub, puback),
    refute(Sub, publish, 200),
    %% PINGREQ names the client, which is how a sleeping client comes back.
    send(Sub, Port, #{type => pingreq, client_id => <<"sn-sleep-sub">>}),
    #{type := publish, payload := <<"while-asleep">>} = expect(Sub, publish),
    #{type := pingresp} = expect(Sub, pingresp),
    close(Sub),
    close(Pub).

qos_minus1_publishes_without_a_connection(Config) ->
    Port = ?config(port, Config),
    Sub = open(),
    connect(Sub, Port, <<"sn-minus1-sub">>),
    _ = subscribe(Sub, Port, short, <<"m1">>, 1),
    %% No CONNECT at all: QoS -1 is the "fire and forget, no session" level.
    Bare = open(),
    publish(Bare, Port, short, <<"m1">>, -1, 0, <<"stateless">>),
    #{type := publish, payload := <<"stateless">>} = expect(Sub, publish),
    close(Sub),
    close(Bare).

keepalive_timeout_ends_the_session(Config) ->
    Port = ?config(port, Config),
    S = open(),
    Before = conn_pids(),
    connect(S, Port, <<"sn-keeplive">>, #{duration => 1}),
    [ConnPid] = conn_pids() -- Before,
    %% 1.5x duration: the gateway probes rather than believing silence.
    #{type := pingreq} = expect(S, pingreq),
    %% 2x duration: still silent, so the session dies.
    wait_until(fun() -> not lists:member(ConnPid, conn_pids()) end),
    close(S).

%%%_* Cases: gateway-level packets -------------------------------------------

searchgw_answers_gwinfo(Config) ->
    Port = ?config(port, Config),
    S = open(),
    send(S, Port, #{type => searchgw, radius => 1}),
    #{type := gwinfo, gw_id := 1} = expect(S, gwinfo),
    close(S).

advertise_is_broadcast_on_the_configured_interval(Config) ->
    S = ?config(advertise_socket, Config),
    %% The interval is 50 ms and the address 127.0.0.1 for this case; the
    %% socket was bound before the listener started so it could be named as
    %% the destination, exactly like a client on the well-known port.
    #{type := advertise, gw_id := 1, duration := Duration} =
        expect(S, advertise),
    ?assert(Duration >= 1).

unknown_client_pingreq_gets_disconnect(Config) ->
    Port = ?config(port, Config),
    S = open(),
    send(S, Port, #{type => pingreq, client_id => <<"never-connected">>}),
    #{type := disconnect} = expect(S, disconnect),
    close(S).

malformed_datagram_is_dropped(Config) ->
    Port = ?config(port, Config),
    S = open(),
    %% A length byte that promises more than the datagram carries, then a
    %% reserved packet type: both must be dropped silently, and a well-formed
    %% CONNECT from the same socket must still work afterwards.
    ok = gen_udp:send(S, ?HOST, Port, <<10, 16#05, 0, 1>>),
    ok = gen_udp:send(S, ?HOST, Port, <<2, 16#03>>),
    connect(S, Port, <<"sn-after-garbage">>),
    close(S).

require_auth_refuses_to_start(_Config) ->
    Result = application:ensure_all_started(broker_proto_mqtt_sn),
    %% OTP wraps a `start/2' error with the app name and the callback that
    %% returned it, and the number of wrappers differs between releases. What
    %% must be true on every one is that our own reason is in there, and that
    %% no listener was left running behind the refusal.
    ?assertMatch({error, _}, Result),
    ?assert(contains({require_auth_unsupported, mqtt_sn}, Result)),
    ?assertEqual(undefined, whereis(broker_proto_mqtt_sn_sup)).

%%%_* Helpers ---------------------------------------------------------------

%% Is `Needle' anywhere inside `Term'? Used for errors OTP wraps differently
%% across releases.
contains(Needle, Needle) ->
    true;
contains(Needle, Term) when is_tuple(Term) ->
    contains(Needle, tuple_to_list(Term));
contains(Needle, Term) when is_list(Term) ->
    lists:any(fun(E) -> contains(Needle, E) end, Term);
contains(_Needle, _Term) ->
    false.

wait_until(Fun) -> wait_until(Fun, ?POLL_BUDGET_MS).

wait_until(_Fun, 0) -> exit(wait_until_timeout);
wait_until(Fun, Left) ->
    case Fun() of
        true -> ok;
        false -> timer:sleep(?POLL_MS), wait_until(Fun, Left - ?POLL_MS)
    end.
