%% @doc T9: the MQTT 3.1.1 connection FSM, over real sockets.
%%
%% Everything here is the protocol-side contract of design §4/§5 read from the
%% outside: bytes go in, bytes (or a closed socket) come back. The layers below
%% the conn already have their own suites, so these cases pin the ORDER and the
%% REFUSALS that only a connection can express:
%%
%%  * §4.5's rejection posture is a per-frame decision, not a config comment:
%%    MQTT 5 -> CONNACK 0x87, any other wrong level -> 0x01, empty ClientID ->
%%    0x02, auth failure -> 0x04, QoS2 / over-cap PUBLISH -> close with no ack,
%%    Retain -> strip and keep flowing, Will -> parse and ignore.
%%  * E2/E12: the session's effective max QoS is min(config, capability), so on
%%    the memory backend QoS1 is refused at the SUBACK (0x80) AND at the PUBLISH
%%    (close) in the same session -- while QoS0 still works end to end.
%%  * R1a/§5.1(e)(f): PUBACK follows the fsync append and nothing else.
%%    `puback_is_not_blocked_by_the_subscription_worker' suspends that worker and
%%    the whole QoS1 round still completes: append, dispatch and the pull loop's
%%    reads all run in the conn's own process.
%%  * J6/K9: cursor/2 is the only read position, so delivery is a PULL. That is
%%    why E7 catch-up, the DUP=1 resend with the SAME packet_id (R7) and the
%%    per-client merge (H7) fall out of one loop instead of three special paths.
%%  * §4.8: the delivery level never rises -- stored traffic matched only by a
%%    QoS0 subscription goes out at QoS0 with no ack owed (and its cursor moves
%%    at send time, which is what K6 needs).
%%  * §4.2/§4.3: clean_session really tears the session down, a non-clean one
%%    really comes back (session_present=1), and a takeover closes the old
%%    connection without letting its late `detach' evict the new row (K10).
-module(mqtt_conn_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include("mqtt_frame.hrl").
-include_lib("broker_core/include/broker_message.hrl").
-include("mqtt_client.hrl").

-define(N, 2).
-define(USERS, [{<<"alice">>, #{password => <<"pw">>, tenant_id => <<"t1">>}},
                {<<"guest">>, #{password => <<"guest">>, tenant_id => <<"t0">>}}]).

all() ->
    [connect_anonymous_is_granted_with_session_present_zero,
     connect_with_bad_password_is_refused_4,
     connect_with_empty_client_id_is_refused_2,
     connect_mqtt5_is_refused,
     connect_wrong_protocol_level_is_refused_1,
     frame_before_connect_closes_the_connection,
     connect_on_an_established_session_closes,
     will_fields_are_parsed_and_ignored,
     keepalive_is_clamped_and_only_peer_traffic_restarts_it,
     pingreq_is_answered_pingresp,
     subscribe_grants_the_requested_qos,
     subscribe_with_an_invalid_filter_closes_without_a_suback,
     unsubscribe_is_acknowledged_and_stops_delivery,
     publish_qos0_loopback_reaches_the_subscriber,
     publish_qos1_is_acked_after_the_append_and_delivered,
     the_consumer_puback_advances_the_cursor,
     puback_is_not_blocked_by_the_subscription_worker,
     qos2_publish_closes_without_an_ack,
     publish_over_the_effective_max_closes_without_an_ack,
     retain_is_stripped_and_the_message_still_flows,
     overlapping_filters_deliver_once_and_both_cursors_move,
     a_qos0_subscription_receives_stored_traffic_at_qos0,
     offline_messages_are_caught_up_in_offset_order,
     an_unacked_message_is_resent_with_dup_and_the_same_packet_id,
     takeover_closes_the_old_connection_and_keeps_the_session,
     a_clean_session_connection_tears_the_previous_session_down,
     the_memory_backend_serves_the_full_e12_posture].

%%%_* Fixtures ----------------------------------------------------------------

init_per_suite(Config) ->
    %% `_ =' on purpose (J10): every suite in this node shares one application
    %% controller, and an earlier suite has usually loaded these already -- a
    %% second `load' would be an error we would rather not assert on.
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
    %% EVERY knob the conn reads is written here, in every case: a previous case
    %% that moved `mqtt_max_qos' or `backend' must not be able to poison the
    %% next one (J10's cross-suite env pollution, in its per-case form).
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok = application:set_env(broker_storage, data_dir, DataDir),
    ok = application:set_env(broker_storage, wal, #{require_durable_fs => false}),
    ok = application:set_env(broker_queue, partitions, ?N),
    ok = application:set_env(broker_router, partitions, ?N),
    ok = application:set_env(broker_subscription, data_dir, DataDir),
    ok = application:set_env(broker_subscription, cursor_max_regress, 64),
    ok = application:set_env(broker_subscription, cursor_flush_interval_ms, 100),
    ok = application:set_env(broker_subscription, inflight_flush_interval_ms, 100),
    ok = application:set_env(broker_security, users, ?USERS),
    ok = application:set_env(broker_proto_mqtt, pull_page_limit, 32),
    ok = application:set_env(broker_proto_mqtt, max_inflight, 64),
    ok = application:set_env(broker_proto_mqtt, pull_poll_interval_ms, 1000),
    ok = application:set_env(broker_proto_mqtt, inflight_snapshot_ms, 20),
    ok = application:set_env(broker_proto_mqtt, max_keepalive_ms, 60000),
    ok = application:set_env(broker_proto_mqtt, mqtt_max_qos, 1),
    ok = case_env(Case),
    {ok, _Started} = application:ensure_all_started(broker_storage),
    [{data_dir, DataDir} | Config].

end_per_testcase(_Case, _Config) ->
    %% The connections are `temporary' children, so stopping the application
    %% does NOT terminate them; kill them first or their sockets and their log
    %% noise outlive the case.
    kill_conns(),
    stop_all(),
    %% Leave the backend as the repo found it. `init_per_testcase' re-writes it
    %% for our own next case, but a later SUITE would inherit whatever this one
    %% last chose, and only a failing case proves why (J10).
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok.

%% The knobs a case deliberately moves off the suite defaults, in one table, so
%% "which case needs which posture" is answerable without reading 27 bodies.
case_env(keepalive_is_clamped_and_only_peer_traffic_restarts_it) ->
    application:set_env(broker_proto_mqtt, max_keepalive_ms, 200);
case_env(publish_over_the_effective_max_closes_without_an_ack) ->
    application:set_env(broker_proto_mqtt, mqtt_max_qos, 0);
case_env(the_memory_backend_serves_the_full_e12_posture) ->
    application:set_env(broker_storage, backend, broker_storage_memory);
case_env(_) ->
    ok.

%% The full consumer chain: partitions out-of-band (H8), then one
%% `ensure_all_started' boots storage -> router -> queue -> subscription ->
%% event -> proto_mqtt, which is §6's order as the app dependency lists encode it.
boot() ->
    [ok = broker_queue:create_partition(P) || P <- broker_queue:partition_ids()],
    {ok, _Started} = application:ensure_all_started(broker_proto_mqtt),
    ok.

stop_all() ->
    _ = application:stop(broker_proto_mqtt),
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

%%%_* CONNECT: the grant and the refusal codes (§4.5) -------------------------

connect_anonymous_is_granted_with_session_present_zero(_Config) ->
    ok = boot(),
    {Conn, #connack{session_present = 0, return_code = 0}} = open(<<"c1">>),
    %% The attachment is real, not implied by the ack: the session view reads
    %% online, and nothing has been subscribed yet.
    {ok, #{online := true, subs := []}} = broker_subscription:session(<<"c1">>),
    {ok, normal} = mqtt_close(Conn),
    ok.

connect_with_bad_password_is_refused_4(_Config) ->
    ok = boot(),
    {Conn, #connack{return_code = 4}} =
        mqtt_open(#{client_id => <<"c1">>, username => <<"alice">>,
                    password => <<"wrong">>}),
    %% §4.3's error may not distinguish "unknown user" from "wrong password",
    %% so both answers are the same byte.
    {Conn2, #connack{return_code = 4}} =
        mqtt_open(#{client_id => <<"c2">>, username => <<"nobody">>,
                    password => <<"pw">>}),
    {Conn3, #connack{return_code = 0}} =
        mqtt_open(#{client_id => <<"c3">>, username => <<"alice">>,
                    password => <<"pw">>}),
    {ok, normal} = mqtt_close(Conn),
    {ok, normal} = mqtt_close(Conn2),
    {ok, normal} = mqtt_close(Conn3),
    ok.

connect_with_empty_client_id_is_refused_2(_Config) ->
    ok = boot(),
    %% The wire CAN express an empty ClientID; this session table is keyed by
    %% it, so it is refused rather than given a server-generated id (which would
    %% make "who owns this session" unanswerable).
    {Conn, #connack{return_code = 2}} = mqtt_open(#{client_id => <<>>}),
    {ok, normal} = mqtt_close(Conn),
    ok.

connect_mqtt5_is_refused(_Config) ->
    ok = boot(),
    {Client, Pid} = mqtt_pair(),
    Monitor = erlang:monitor(process, Pid),
    ok = gen_tcp:send(Client, mqtt_raw_connect(5, <<"c1">>)),
    #connack{return_code = 16#87} = mqtt_recv(Client),
    ok = expect_closed(Client),
    {ok, normal} = expect_down(Monitor, Pid),
    ok.

connect_wrong_protocol_level_is_refused_1(_Config) ->
    ok = boot(),
    %% §4.5: every level that is not 4 is a 0x01, level 5 excepted (the MQTT 5
    %% refusal above). 6 is the interesting one: it is a *future* version, and
    %% the conn must not guess that its own codec applies.
    lists:foreach(
      fun(Level) ->
              {Client, Pid} = mqtt_pair(),
              Monitor = erlang:monitor(process, Pid),
              ok = gen_tcp:send(Client, mqtt_raw_connect(Level, <<"c1">>)),
              #connack{return_code = 1} = mqtt_recv(Client),
              ok = expect_closed(Client),
              {ok, normal} = expect_down(Monitor, Pid)
      end, [0, 3, 6]),
    ok.

frame_before_connect_closes_the_connection(_Config) ->
    ok = boot(),
    {Client, Pid} = mqtt_pair(),
    Monitor = erlang:monitor(process, Pid),
    %% A perfectly legal PINGREQ, sent before the handshake: the stream position
    %% is unknown to us, so there is nothing to answer (§3.1).
    ok = gen_tcp:send(Client, mqtt_frame:encode(#ping{kind = req})),
    ok = expect_closed(Client),
    {ok, normal} = expect_down(Monitor, Pid),
    ok.

connect_on_an_established_session_closes(_Config) ->
    ok = boot(),
    {Conn, _} = open(<<"c1">>),
    Monitor = mqtt_monitor(Conn),
    mqtt_send(Conn, mqtt_frame:encode(mqtt_connect_frame(#{client_id => <<"c1">>}))),
    {ok, normal} = expect_down(Monitor, cpid(Conn)),
    ok.

will_fields_are_parsed_and_ignored(_Config) ->
    ok = boot(),
    {Conn, #connack{return_code = 0}} =
        mqtt_open(#{client_id => <<"c1">>, will_topic => <<"will/c1">>,
                    will_payload => <<"bye">>}),
    %% The codec parsed the Will (a malformed one would have been a decode error
    %% and no CONNACK at all); the conn drops it. No subscription appears for
    %% the Will topic, and there is nothing stored to publish later (§4.5: S1
    %% has no Will, and pretending to support one is worse than refusing).
    [] = broker_subscription:subscriptions(<<"c1">>),
    [] = broker_subscription:subscriptions(<<"will/c1">>),
    {ok, normal} = mqtt_close(Conn),
    ok.

keepalive_is_clamped_and_only_peer_traffic_restarts_it(_Config) ->
    ok = boot(),
    %% keep_alive = 60s, clamped to the 200ms server cap, deadline at 1.5x =
    %% 300ms. A 500ms conversation at 50ms intervals therefore survives it...
    {Conn, _} = open(#{client_id => <<"c1">>, keep_alive => 60}),
    Pid = cpid(Conn),
    Monitor = mqtt_monitor(Conn),
    pinged_while_alive(Conn, Pid, 10),
    true = is_process_alive(Pid),
    %% ...and the silence after it does not: the deadline is measured from the
    %% LAST peer packet. The conn's own timers (poll sentinel, scheduled round,
    %% snapshot tick) must not restart it -- a live pull loop would otherwise
    %% keep a dead TCP peer connected forever.
    {ok, normal} = expect_down(Monitor, Pid),
    ok.

pinged_while_alive(_Conn, _Pid, 0) ->
    ok;
pinged_while_alive(Conn, Pid, N) ->
    #ping{kind = resp} = mqtt_ping(Conn),
    true = is_process_alive(Pid),
    timer:sleep(50),
    pinged_while_alive(Conn, Pid, N - 1).

pingreq_is_answered_pingresp(_Config) ->
    ok = boot(),
    {Conn, _} = open(<<"c1">>),
    #ping{kind = resp} = mqtt_ping(Conn),
    #ping{kind = resp} = mqtt_ping(Conn),
    {ok, normal} = mqtt_close(Conn),
    ok.

%%%_* SUBSCRIBE / UNSUBSCRIBE -------------------------------------------------

subscribe_grants_the_requested_qos(_Config) ->
    ok = boot(),
    {Conn, _} = open(<<"c1">>),
    0 = sub(Conn, 1, <<"a/b">>, 0),
    1 = sub(Conn, 2, <<"sport/tennis/+">>, 1),
    %% Re-subscribing a live filter replaces its QoS and answers the NEW level
    %% (MQTT §4.9); the cursor deliberately keeps its position, which is why a
    %% re-subscribe is not a replay.
    0 = sub(Conn, 3, <<"sport/tennis/+">>, 0),
    Subs = lists:sort(fun(#{filter := A}, #{filter := B}) -> A =< B end,
                      broker_subscription:subscriptions(<<"c1">>)),
    [#{qos := 0, filter := <<"a/b">>},
     #{qos := 0, filter := <<"sport/tennis/+">>}] = Subs,
    {ok, normal} = mqtt_close(Conn),
    ok.

subscribe_with_an_invalid_filter_closes_without_a_suback(_Config) ->
    ok = boot(),
    {Conn, _} = open(<<"c1">>),
    Monitor = mqtt_monitor(Conn),
    %% §4.7's filter grammar is enforced by the frame codec, so `a/#/b' never
    %% becomes a #subscribe{} record for grant/3 to answer with a 0x80: the
    %% packet is malformed and §4.5's posture for that is "close, no ack". MQTT
    %% §4.7 permits the other half (a SUBACK of 0x80s); which one a real client
    %% prefers is T15's compatibility measurement, not a claim to make here.
    ok = mqtt_send(Conn, mqtt_frame:encode(#subscribe{packet_id = 1,
                                                      topics = [{<<"a/#/b">>, 1}]})),
    ok = expect_closed(csock(Conn)),
    {ok, normal} = expect_down(Monitor, cpid(Conn)),
    %% Nothing half-built survived the close.
    [] = broker_subscription:subscriptions(<<"c1">>),
    ok.

unsubscribe_is_acknowledged_and_stops_delivery(_Config) ->
    ok = boot(),
    {Conn, _} = open(<<"c1">>),
    1 = sub(Conn, 1, <<"u/#">>, 1),
    %% UNSUBSCRIBE for a filter that never existed is a no-op, not an error
    %% (MQTT §4.10) -- and the packet still answers.
    #unsuback{packet_id = 2} = mqtt_unsubscribe(Conn, 2, [<<"u/#">>, <<"never/there">>]),
    [] = broker_subscription:subscriptions(<<"c1">>),
    %% The producer's promise is not the consumer's: with no subscription left
    %% the PUBACK still comes, and no PUBLISH follows it.
    #puback{packet_id = 5} = pub(Conn, 5, <<"u/x">>, <<"p">>, 1),
    true = mqtt_silence(csock(Conn), 200),
    {ok, normal} = mqtt_close(Conn),
    ok.

%%%_* PUBLISH inbound: the two levels (§4.4, R1a) -----------------------------

publish_qos0_loopback_reaches_the_subscriber(_Config) ->
    ok = boot(),
    {Conn, _} = open(<<"c1">>),
    0 = sub(Conn, 1, <<"t/#">>, 0),
    %% A QoS0 PUBLISH carries no packet id in either direction.
    ok = mqtt_publish(Conn, undefined, <<"t/a">>, <<"zero">>, 0),
    {Pub, _Seen} = recv_publish(csock(Conn)),
    #publish{qos = 0, dup = 0, retain = 0, topic = <<"t/a">>,
             payload = <<"zero">>, packet_id = undefined} = Pub,
    %% §4.4: QoS0 does not enter the log, so there is nothing readable in any
    %% partition -- the delivery above came out of the unpositioned push.
    [ok = assert_empty_page(P) || P <- broker_queue:partition_ids()],
    {ok, normal} = mqtt_close(Conn),
    ok.

publish_qos1_is_acked_after_the_append_and_delivered(_Config) ->
    ok = boot(),
    {Conn, _} = open(<<"c1">>),
    1 = sub(Conn, 1, <<"t/#">>, 1),
    P = route_partition(<<"t/a">>),
    #puback{packet_id = 9} = pub(Conn, 9, <<"t/a">>, <<"one">>, 1),
    {Pub, _Seen} = recv_publish(csock(Conn)),
    #publish{qos = 1, dup = 0, retain = 0, topic = <<"t/a">>,
             payload = <<"one">>} = Pub,
    %% The ack the client is holding was earned by a durable append: read that
    %% record back out of storage and check the ADR-009 fields the conn fills.
    {ok, [{Offset, Msg}], _Next} = broker_queue:read_from(P, 0, 10),
    #broker_message{destination = <<"t/a">>, payload = <<"one">>,
                    persistence = durable, reliability = fsync,
                    partition_id = P, headers = #{},
                    proto_meta = #{qos := 1, retain := 0, dup := 0,
                                   packet_id := 9},
                    expires_at = undefined, event = undefined} = Msg,
    true = byte_size(Msg#broker_message.message_id) =:= 16,
    true = is_integer(Offset),
    {ok, normal} = mqtt_close(Conn),
    ok.

the_consumer_puback_advances_the_cursor(_Config) ->
    ok = boot(),
    {Conn, _} = open(<<"c1">>),
    1 = sub(Conn, 1, <<"t/#">>, 1),
    {ok, SubRef} = subscribed(<<"c1">>, <<"t/#">>, 1),
    P = route_partition(<<"t/a">>),
    {ok, Anchor} = broker_subscription:cursor(SubRef, P),
    #puback{} = pub(Conn, 2, <<"t/a">>, <<"one">>, 1),
    {Pub, _Seen} = recv_publish(csock(Conn)),
    %% Before the ack the cursor has NOT moved, which is exactly why the next
    %% round will read offset Anchor again: at-least-once, before it is
    %% at-most-once-delivered.
    {ok, Anchor} = broker_subscription:cursor(SubRef, P),
    ok = mqtt_puback(Conn, Pub#publish.packet_id),
    wait_cursor_after(SubRef, P, Anchor),
    {ok, normal} = mqtt_close(Conn),
    ok.

puback_is_not_blocked_by_the_subscription_worker(_Config) ->
    ok = boot(),
    {Conn, _} = open(<<"c1">>),
    1 = sub(Conn, 1, <<"t/#">>, 1),
    Worker = whereis(broker_subscription_worker),
    sys:suspend(Worker),
    try
        %% append -> PUBACK -> dispatch -> pull round -> delivery: none of it
        %% takes a hop through the worker, so a stopped one cannot hold the QoS1
        %% handshake hostage. `ack/3' is the call that WOULD block, and this
        %% path never makes it (a one-message page whose only subscription
        %% matched has nothing to read-and-skip).
        #puback{packet_id = 4} = pub(Conn, 4, <<"t/a">>, <<"blocked?">>, 1),
        {Pub, _Seen} = recv_publish(csock(Conn)),
        #publish{qos = 1, payload = <<"blocked?">>} = Pub,
        true = is_pid(whereis(broker_subscription_worker))
    after
        sys:resume(Worker)
    end,
    {ok, normal} = mqtt_close(Conn),
    ok.

qos2_publish_closes_without_an_ack(_Config) ->
    ok = boot(),
    {Conn, _} = open(<<"c1">>),
    Monitor = mqtt_monitor(Conn),
    %% The reserved QoS bits: the codec has no QoS2 record, so the frame is
    %% refused at parse time, and §4.5's posture for it is "close, no ack" --
    %% a client that retries with a real QoS2 handshake gets the same answer.
    ok = mqtt_send(Conn, mqtt_raw_publish_qos2(<<"t/a">>, <<"two">>)),
    ok = expect_closed(csock(Conn)),
    {ok, normal} = expect_down(Monitor, cpid(Conn)),
    %% Not stored-then-hidden: a PUBLISH that was never acked must not have
    %% entered the log either.
    [ok = assert_empty_page(P) || P <- broker_queue:partition_ids()],
    ok.

publish_over_the_effective_max_closes_without_an_ack(_Config) ->
    %% mqtt_max_qos = 0 on a backend that CAN fsync: here the config, not the
    %% capability, is the binding limit (E2's min() still lands on 0), and both
    %% halves of E12 have to hold in the same session.
    ok = boot(),
    {Conn, _} = open(<<"c1">>),
    Monitor = mqtt_monitor(Conn),
    128 = sub(Conn, 1, <<"t/#">>, 1),
    0 = sub(Conn, 2, <<"u/#">>, 0),
    ok = mqtt_publish(Conn, 3, <<"t/a">>, <<"nope">>, 1),
    ok = expect_closed(csock(Conn)),
    {ok, normal} = expect_down(Monitor, cpid(Conn)),
    %% The refused subscription is not quietly downgraded into a live one.
    [] = broker_subscription:subscriptions(<<"c1">>),
    ok.

retain_is_stripped_and_the_message_still_flows(_Config) ->
    ok = boot(),
    {Conn, _} = open(<<"c1">>),
    1 = sub(Conn, 1, <<"t/#">>, 1),
    ok = mqtt_send(Conn, mqtt_frame:encode(#publish{qos = 1, retain = 1,
                                                    topic = <<"t/a">>,
                                                    packet_id = 6,
                                                    payload = <<"kept">>})),
    #puback{packet_id = 6} = mqtt_recv(csock(Conn)),
    {Pub, _Seen} = recv_publish(csock(Conn)),
    %% There is no retain store in S1: the bit is dropped on the way in and the
    %% delivered frame says so. The message is NOT discarded -- §4.5 strips, it
    %% does not reject.
    #publish{retain = 0, qos = 1, payload = <<"kept">>} = Pub,
    ok = mqtt_puback(Conn, Pub#publish.packet_id),
    {ok, normal} = mqtt_close(Conn),
    ok.

%%%_* Delivery levels and the merge (§4.8, H7) --------------------------------

overlapping_filters_deliver_once_and_both_cursors_move(_Config) ->
    ok = boot(),
    {Conn, _} = open(<<"c1">>),
    1 = sub(Conn, 1, <<"sport/#">>, 1),
    1 = sub(Conn, 2, <<"sport/tennis/+">>, 1),
    {ok, R1} = subscribed(<<"c1">>, <<"sport/#">>, 1),
    {ok, R2} = subscribed(<<"c1">>, <<"sport/tennis/+">>, 1),
    P = route_partition(<<"sport/tennis/a">>),
    {ok, A1} = broker_subscription:cursor(R1, P),
    {ok, A2} = broker_subscription:cursor(R2, P),
    #puback{} = pub(Conn, 3, <<"sport/tennis/a">>, <<"ball">>, 1),
    {Pub, _Seen} = recv_publish(csock(Conn)),
    %% H7: one client, two matching filters, ONE frame on the wire. The merge
    %% happens in dispatch/2's group_by_client and the pull loop re-derives the
    %% same union from the subscription rows, so the two paths cannot disagree.
    ok = mqtt_puback(Conn, Pub#publish.packet_id),
    wait_cursor_after(R1, P, A1),
    wait_cursor_after(R2, P, A2),
    true = mqtt_silence(csock(Conn), 200),
    {ok, normal} = mqtt_close(Conn),
    ok.

a_qos0_subscription_receives_stored_traffic_at_qos0(_Config) ->
    ok = boot(),
    {Conn, _} = open(<<"c1">>),
    0 = sub(Conn, 1, <<"q/#">>, 0),
    {ok, SubRef} = subscribed(<<"c1">>, <<"q/#">>, 0),
    P = route_partition(<<"q/a">>),
    {ok, Anchor} = broker_subscription:cursor(SubRef, P),
    %% A stored QoS1 message matched only by a QoS0 subscription: §4.8 forbids
    %% raising the delivery level, so it goes out with no packet id and no ack
    %% owed -- and its cursor moves at SEND time, because nothing is waiting to
    %% advance it later (K6: an unadvanced QoS0 cursor would pin the committed
    %% watermark forever).
    #puback{} = pub(Conn, 2, <<"q/a">>, <<"loud">>, 1),
    {Pub, _Seen} = recv_publish(csock(Conn)),
    #publish{qos = 0, packet_id = undefined, payload = <<"loud">>} = Pub,
    wait_cursor_after(SubRef, P, Anchor),
    {ok, normal} = mqtt_close(Conn),
    ok.

%%%_* Session resumption: E7 catch-up, R7 DUP, §4.2/§4.3 ----------------------

offline_messages_are_caught_up_in_offset_order(_Config) ->
    ok = boot(),
    {Conn, _} = open(#{client_id => <<"c1">>, clean => false}),
    1 = sub(Conn, 1, <<"e7/#">>, 1),
    {ok, SubRef} = subscribed(<<"c1">>, <<"e7/#">>, 1),
    P = route_partition(<<"e7/a">>),
    {ok, Anchor} = broker_subscription:cursor(SubRef, P),
    %% A transport death, not a DISCONNECT: the session goes offline and stays.
    {ok, normal} = mqtt_close(Conn),
    Offsets = [O || {_, O} <- [offline_publish(<<"e7/a">>, N)
                               || N <- [<<"1">>, <<"2">>, <<"3">>]]],
    %% Reconnect, non-clean: session_present=1 and the backlog arrives through
    %% the same pull loop a live subscriber uses -- one ordered stream, no
    %% separate offline queue (§4.1, E7).
    {Conn2, #connack{session_present = 1, return_code = 0}} =
        open(#{client_id => <<"c1">>, clean => false}),
    Delivered = [recv_publish(csock(Conn2)) || _ <- [1, 2, 3]],
    [<<"1">>, <<"2">>, <<"3">>] =
        [Pub#publish.payload || {Pub, _} <- Delivered],
    %% Acking what was actually read off the wire: three cursors' worth of
    %% offsets, contiguous in this partition, so the cursor lands past the last
    %% one and only the last one.
    lists:foreach(fun({#publish{packet_id = Pid}, _}) ->
                          ok = mqtt_puback(Conn2, Pid)
                  end, Delivered),
    wait_cursor_after(SubRef, P, lists:max(Offsets)),
    true = Anchor =< lists:min(Offsets),
    {ok, normal} = mqtt_close(Conn2),
    ok.

an_unacked_message_is_resent_with_dup_and_the_same_packet_id(_Config) ->
    ok = boot(),
    {Publisher, _} = open(<<"pub">>),
    {Subscriber, _} = open(#{client_id => <<"c1">>, clean => false}),
    1 = sub(Subscriber, 1, <<"d/#">>, 1),
    #puback{} = pub(Publisher, 1, <<"d/a">>, <<"once">>, 1),
    {Pub, _Seen} = recv_publish(csock(Subscriber)),
    #publish{qos = 1, dup = 0, packet_id = Pid} = Pub,
    %% No PUBACK. The connection dies with the message in flight, and what
    %% survives it is the conn's own in-flight set (R7: this layer stores an
    %% opaque term and never re-derives it from storage).
    {ok, normal} = mqtt_close(Subscriber),
    {Subscriber2, #connack{session_present = 1}} =
        open(#{client_id => <<"c1">>, clean => false}),
    {Resent, _} = recv_publish(csock(Subscriber2)),
    %% §4.1: the DUP flag with the SAME packet identifier, so the client's
    %% duplicate check can still recognise the redelivery. A fresh id here would
    %% be a real bug that counting messages alone cannot see.
    #publish{qos = 1, dup = 1, packet_id = Pid, payload = <<"once">>} = Resent,
    ok = mqtt_puback(Subscriber2, Pid),
    {ok, normal} = mqtt_close(Subscriber2),
    {ok, normal} = mqtt_close(Publisher),
    ok.

takeover_closes_the_old_connection_and_keeps_the_session(_Config) ->
    ok = boot(),
    {Old, _} = open(#{client_id => <<"c1">>, clean => false}),
    1 = sub(Old, 1, <<"k/#">>, 1),
    OldPid = cpid(Old),
    Monitor = mqtt_monitor(Old),
    %% §4.3: MQTT has no takeover protocol. attach/3 answers the previous pid,
    %% the NEW conn tells the old one to close, and the old one's own detach
    %% (which runs in its terminate) must not evict the row the new one just
    %% wrote (K10's pid guard is the backstop).
    {New, #connack{return_code = 0}} = open(#{client_id => <<"c1">>,
                                              clean => false}),
    {ok, takeover} = expect_down(Monitor, OldPid),
    ok = expect_closed(csock(Old)),
    %% A late detach from the evicted pid is a no-op, and the session is still
    %% the new connection's -- with its subscription intact.
    ok = broker_subscription:detach(<<"c1">>, OldPid),
    {ok, #{online := true, subs := [_]}} = broker_subscription:session(<<"c1">>),
    %% The new connection serves traffic as if it had always been there.
    0 = sub(New, 2, <<"k/zero">>, 0),
    ok = mqtt_publish(New, undefined, <<"k/zero">>, <<"hello">>, 0),
    {Pub, _} = recv_publish(csock(New)),
    #publish{payload = <<"hello">>} = Pub,
    {ok, normal} = mqtt_close(New),
    ok.

a_clean_session_connection_tears_the_previous_session_down(_Config) ->
    ok = boot(),
    {First, _} = open(#{client_id => <<"c1">>, clean => false}),
    1 = sub(First, 1, <<"a/b">>, 1),
    #puback{} = pub(First, 2, <<"a/b">>, <<"kept">>, 1),
    {_Unacked, _} = recv_publish(csock(First)),
    %% Left unacked on purpose: a clean session has to wipe the in-flight blob
    %% as well as the subscriptions, or the reconnect replays a message into a
    %% session that claims to start from zero.
    {ok, normal} = mqtt_close(First),
    %% First PROVE there was something to resume...
    {Probe, #connack{session_present = 1}} =
        open(#{client_id => <<"c1">>, clean => false}),
    {Dup, _} = recv_publish(csock(Probe)),
    #publish{dup = 1, payload = <<"kept">>} = Dup,
    ok = mqtt_puback(Probe, Dup#publish.packet_id),
    {ok, normal} = mqtt_close(Probe),
    %% ...then wipe it. session_present=0, no subscriptions, and no delivery on
    %% the wire afterwards (the stored message is still in the log: a clean
    %% session discards the SUBSCRIBER's position, not the partition's content).
    {Clean, #connack{session_present = 0, return_code = 0}} =
        open(#{client_id => <<"c1">>, clean => true}),
    [] = broker_subscription:subscriptions(<<"c1">>),
    true = mqtt_silence(csock(Clean), 200),
    {ok, normal} = mqtt_close(Clean),
    ok.

%%%_* E12: the memory backend's full posture ---------------------------------

the_memory_backend_serves_the_full_e12_posture(_Config) ->
    ok = boot(),
    {Conn, _} = open(<<"c1">>),
    %% No fsync in the capability -> the session's effective max QoS is 0 even
    %% though mqtt_max_qos says 1. A contradictory config is not honoured
    %% silently (E2/E12), and it is discovered at CONNECT, not at the first
    %% append that would have failed.
    128 = sub(Conn, 1, <<"t/#">>, 1),
    0 = sub(Conn, 2, <<"u/#">>, 0),
    Monitor = mqtt_monitor(Conn),
    ok = mqtt_publish(Conn, 3, <<"t/a">>, <<"refused">>, 1),
    ok = expect_closed(csock(Conn)),
    {ok, normal} = expect_down(Monitor, cpid(Conn)),
    %% QoS0 still works end to end on the same backend, including the loopback
    %% the plan's §4.4 push path serves.
    {Conn2, _} = open(<<"c2">>),
    0 = sub(Conn2, 1, <<"u/#">>, 0),
    ok = mqtt_publish(Conn2, undefined, <<"u/x">>, <<"fast">>, 0),
    {Pub, _} = recv_publish(csock(Conn2)),
    #publish{qos = 0, payload = <<"fast">>} = Pub,
    [ok = assert_empty_page(P) || P <- broker_queue:partition_ids()],
    {ok, normal} = mqtt_close(Conn2),
    ok.

%%%_* Helpers -----------------------------------------------------------------

%% open/1 asserts the grant: a case that expects a refusal calls mqtt_open/1 and
%% matches the CONNACK code itself.
open(ClientId) when is_binary(ClientId) ->
    open(#{client_id => ClientId});
open(Knobs) ->
    {Conn, #connack{return_code = 0} = Ack} = mqtt_open(Knobs),
    {Conn, Ack}.

csock(#{sock := Sock}) -> Sock.
cpid(#{pid := Pid}) -> Pid.

%% sub/4 answers the single granted code; subscribed/3 answers the SubRef the
%% cursor assertions need (the same {ClientId, Filter} the Router holds, K2).
%% Subscribing through the API as well is deliberate: re-subscribing a live
%% filter is documented to leave the cursor alone, so the two are the same row.
sub(Conn, Pid, Filter, QoS) ->
    #suback{return_codes = [Code]} = mqtt_subscribe(Conn, Pid, [{Filter, QoS}]),
    Code.

subscribed(ClientId, Filter, QoS) ->
    broker_subscription:subscribe(ClientId, Filter, QoS).

pub(Conn, Pid, Topic, Payload, QoS) ->
    ok = mqtt_publish(Conn, Pid, Topic, Payload, QoS),
    mqtt_recv(csock(Conn)).

%% Reading until a PUBLISH arrives, keeping whatever came in between -- a
%% producer's PUBACK legitimately precedes the consumer's delivery, and a case
%% that asserts only "the ack is one of what I saw" is a weaker claim than the
%% one that asserts its packet id.
recv_publish(Client) ->
    recv_publish(Client, []).

recv_publish(Client, Seen) ->
    case mqtt_recv(Client) of
        #publish{} = Pub -> {Pub, Seen};
        Other -> recv_publish(Client, [Other | Seen])
    end.

%% The subscriptions/1 row exists the instant SUBSCRIBE answers; the cursor only
%% moves asynchronously (a pull round, then an ack), so this waits for the move
%% rather than sleeping a guess.
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
            exit({cursor_stuck, SubRef, Partition, Anchor, Other})
    end.

%% The single partition oracle, used the way the conn uses it (ADR-010).
route_partition(Topic) ->
    {ok, Env} = broker_router:env(),
    {ok, [{Partition, _Refs} | _]} = broker_router_hash:route(Topic, #{}, Env),
    Partition.

assert_empty_page(P) ->
    {ok, [], _Next} = broker_queue:read_from(P, 0, 10),
    ok.

%% An out-of-band producer: a durable QoS1 message with no connection involved,
%% which is what a reconnecting subscriber catches up on (E7).
offline_publish(Topic, Payload) ->
    P = route_partition(Topic),
    Msg = #broker_message{message_id = crypto:strong_rand_bytes(16),
                          tenant_id = <<"t1">>, destination = Topic,
                          partition_id = P, payload = Payload,
                          payload_encoding = raw, headers = #{},
                          proto_meta = #{qos => 1},
                          persistence = durable, reliability = fsync,
                          created_at = erlang:system_time(millisecond),
                          expires_at = undefined, event = undefined},
    {ok, Offset} = broker_queue:append(P, Msg, fsync),
    {Topic, Offset}.

expect_closed(Client) ->
    case mqtt_recv_or(Client, 3000, nothing) of
        {error, closed} -> ok;
        nothing -> exit(mqtt_socket_stayed_open);
        Frame -> exit({mqtt_unexpected_frame, Frame})
    end.

expect_down(Monitor, Pid) ->
    receive
        {'DOWN', Monitor, process, Pid, Reason} -> {ok, Reason}
    after 5000 -> exit({mqtt_no_down, Pid})
    end.
