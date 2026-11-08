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
-include_lib("stdlib/include/assert.hrl").
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
     will_is_published_on_tcp_close,
     will_is_not_published_on_disconnect,
     will_is_published_on_takeover,
     will_retain_updates_the_tree,
     will_is_published_on_a_keepalive_timeout,
     will_is_not_republished_after_a_broker_restart,
     keepalive_is_clamped_and_only_peer_traffic_restarts_it,
     pingreq_is_answered_pingresp,
     subscribe_grants_the_requested_qos,
     subscribe_with_an_invalid_filter_closes_without_a_suback,
     unsubscribe_is_acknowledged_and_stops_delivery,
     publish_qos0_loopback_reaches_the_subscriber,
     publish_qos1_is_acked_after_the_append_and_delivered,
     the_consumer_puback_advances_the_cursor,
     puback_is_not_blocked_by_the_subscription_worker,
     reserved_qos_bits_close_without_an_ack,
     publish_over_the_effective_max_closes_without_an_ack,
     qos2_inbound_four_steps_delivers_once,
     qos2_inbound_dup_publish_is_not_redelivered,
     unknown_pubrel_gets_pubcomp,
     unknown_pubrec_gets_pubrel,
     unknown_pubcomp_is_ignored,
     qos2_state_overflow_disconnects,
     qos2_outbound_advances_cursor_only_on_pubcomp,
     puback_for_a_qos2_delivery_does_not_advance_the_cursor,
     reconnect_before_pubrec_resends_a_dup_publish,
     reconnect_after_pubrec_resends_pubrel_only,
     inflight_blob_is_v2_and_skips_retained_entries,
     subscribe_receives_retained_after_suback,
     retained_qos_is_capped_by_the_grant,
     retained_wildcard_delivers_each_topic_in_topic_order,
     retain_flag_is_zero_for_live_subscribers,
     zero_length_retain_clears_for_new_subscribers,
     zero_length_retain_still_reaches_live_subscribers,
     retained_survives_a_retain_app_restart,
     retain_overflow_still_delivers_live,
     overlapping_filters_deliver_once_and_both_cursors_move,
     a_qos0_subscription_receives_stored_traffic_at_qos0,
     offline_messages_are_caught_up_in_offset_order,
     an_unacked_message_is_resent_with_dup_and_the_same_packet_id,
     takeover_closes_the_old_connection_and_keeps_the_session,
     a_clean_session_connection_tears_the_previous_session_down,
     the_memory_backend_serves_the_full_e12_posture,
     %% ADR-019: Shared/Failover have no MQTT 3.1.1 wire syntax (§13.4 puts
     %% `$share/...' in S2.5), so the group is created through the internal API
     %% and the *conn* is the consumer under test: claim, read, ack, redeliver.
     a_shared_group_delivers_each_message_exactly_once,
     a_dead_shared_member_hands_its_page_to_the_survivor,
     a_standby_failover_member_receives_nothing,
     failover_promotes_the_next_member_when_the_active_disconnects,
     a_qos0_publish_reaches_exactly_one_shared_member_over_the_wire].

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
    %% The retain snapshot is a fourth per-data-dir artifact (ADR-018 §18.2):
    %% leaving it unset would make every case share the repo's data/retain.
    ok = application:set_env(broker_retain, data_dir, DataDir),
    ok = application:set_env(broker_subscription, cursor_max_regress, 64),
    ok = application:set_env(broker_subscription, cursor_flush_interval_ms, 100),
    ok = application:set_env(broker_subscription, inflight_flush_interval_ms, 100),
    ok = application:set_env(broker_security, users, ?USERS),
    ok = application:set_env(broker_proto_mqtt, pull_page_limit, 32),
    ok = application:set_env(broker_proto_mqtt, max_inflight, 64),
    ok = application:set_env(broker_proto_mqtt, pull_poll_interval_ms, 1000),
    ok = application:set_env(broker_proto_mqtt, inflight_snapshot_ms, 20),
    ok = application:set_env(broker_proto_mqtt, max_keepalive_ms, 60000),
    ok = application:set_env(broker_proto_mqtt, mqtt_max_qos, 2),
    ok = application:set_env(broker_proto_mqtt, max_qos2_state_per_session, 100),
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
case_env(will_is_published_on_a_keepalive_timeout) ->
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
    %% T15 M16: a v5 client parses CONNACK as flags|reason|properties, so the
    %% refusal is v5-framed (5 bytes, zero properties). A 3.1.1-shaped 4-byte
    %% CONNACK makes paho-mqtt raise in its properties decoder instead of
    %% surfacing 0x87, so the wire bytes are asserted verbatim here and the
    %% 3.1.1 decoder (rightly) cannot read this frame.
    {ok, <<16#20, 3, 0, 16#87, 0>>} = gen_tcp:recv(Client, 5, 3000),
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

%% ADR-018 §18.1: a Will is connection state held in memory, published from
%% `terminate/3' on every ending EXCEPT a clean DISCONNECT and a broker
%% shutdown. The four cases below pin the two "yes" endings and the two "no"
%% ones that do not need a broker restart to observe.
will_is_published_on_tcp_close(_Config) ->
    ok = boot(),
    {Watcher, _} = open(<<"watch">>),
    1 = sub(Watcher, 1, <<"will/#">>, 1),
    {Conn, #connack{return_code = 0}} =
        mqtt_open(#{client_id => <<"c1">>, will_topic => <<"will/c1">>,
                    will_payload => <<"bye">>, will_qos => 1}),
    %% A transport death, not a DISCONNECT: §3.1.2.5 requires the Will.
    {ok, normal} = mqtt_close(Conn),
    {Pub, _} = recv_publish(csock(Watcher)),
    #publish{qos = 1, retain = 0, topic = <<"will/c1">>,
             payload = <<"bye">>} = Pub,
    ok = mqtt_puback(Watcher, Pub#publish.packet_id),
    {ok, normal} = mqtt_close(Watcher),
    ok.

will_is_not_published_on_disconnect(_Config) ->
    ok = boot(),
    {Watcher, _} = open(<<"watch">>),
    1 = sub(Watcher, 1, <<"will/#">>, 1),
    {Conn, _} = open(#{client_id => <<"c1">>, will_topic => <<"will/c1">>,
                       will_payload => <<"bye">>, will_qos => 1}),
    %% §3.14: DISCONNECT exists precisely to say "do not publish my Will".
    ok = mqtt_disconnect(Conn),
    {ok, normal} = mqtt_close(Conn),
    true = mqtt_silence(csock(Watcher), 300),
    {ok, normal} = mqtt_close(Watcher),
    ok.

will_is_published_on_takeover(_Config) ->
    ok = boot(),
    {Watcher, _} = open(<<"watch">>),
    1 = sub(Watcher, 1, <<"will/#">>, 1),
    {Old, _} = open(#{client_id => <<"c1">>, clean => false,
                      will_topic => <<"will/c1">>, will_payload => <<"evicted">>,
                      will_qos => 1}),
    OldPid = cpid(Old),
    Monitor = mqtt_monitor(Old),
    %% §3.1.4 + §3.1.2.5 taken literally: the old connection is closed without
    %% DISCONNECT, so its Will fires even though the client id lives on.
    {_New, _} = open(#{client_id => <<"c1">>, clean => false}),
    {ok, takeover} = expect_down(Monitor, OldPid),
    {Pub, _} = recv_publish(csock(Watcher)),
    #publish{topic = <<"will/c1">>, payload = <<"evicted">>} = Pub,
    {ok, normal} = mqtt_close(Watcher),
    ok.

%% C-16's third trigger: the peer went silent past 1.5x keepalive, which is a
%% connection failure by §3.1.2.10 -- the Will fires even though the socket was
%% never closed by anyone.
will_is_published_on_a_keepalive_timeout(_Config) ->
    ok = boot(),
    %% No watcher connection: with max_keepalive_ms = 200 (`case_env') the
    %% watcher's own deadline would expire in the same instant, so the evidence
    %% is taken from the retain tree -- a retained Will is stored by the same
    %% publish path an ordinary retained PUBLISH uses.
    {Conn, _} = open(#{client_id => <<"c1">>, keep_alive => 1,
                       will_topic => <<"will/ka">>, will_payload => <<"silent">>,
                       will_qos => 1, will_retain => 1}),
    Monitor = mqtt_monitor(Conn),
    %% 1s keepalive is clamped to the 200ms cap, so the deadline is ~300ms and
    %% the connection dies on its own without any further input.
    {ok, normal} = expect_down(Monitor, cpid(Conn)),
    {ok, #broker_message{destination = <<"will/ka">>, payload = <<"silent">>,
                         proto_meta = Meta}} = broker_retain:lookup(<<"will/ka">>),
    ?assertEqual(1, maps:get(qos, Meta)),
    {ok, normal} = mqtt_close(Conn),
    ok.

%% ADR-018 §18.1 / O5: the Will is connection state and dies with the process.
%% A broker restart is NOT a reason to publish it, and nothing on disk can be
%% used to resurrect it -- so a restart must be silent even for a session whose
%% connection was live when the broker stopped.
will_is_not_republished_after_a_broker_restart(_Config) ->
    ok = boot(),
    {_Conn, _} = open(#{client_id => <<"c1">>, clean => false,
                        will_topic => <<"will/restart">>,
                        will_payload => <<"lost">>, will_qos => 1}),
    %% Stop the whole protocol app the way a broker restart does: connections
    %% are terminated with `shutdown', which §18.1 exempts from the Will.
    Before = maps:get(will_published, broker_metrics:snapshot()),
    ok = application:stop(broker_proto_mqtt),
    {ok, _} = application:ensure_all_started(broker_proto_mqtt),
    ?assertEqual(Before, maps:get(will_published, broker_metrics:snapshot())),
    %% And no Will is sitting anywhere waiting to be replayed: the retain tree
    %% does not have it, and the session record (`.inflight' blob shape v2) has
    %% no field that could carry it -- a `will' key there would be exactly the
    %% persistence O5 forbids.
    error = broker_retain:lookup(<<"will/restart">>),
    case broker_subscription:session(<<"c1">>) of
        {ok, Session} -> ?assertNot(maps:is_key(will, Session));
        {error, not_found} -> ok
    end,
    %% The client id is resumable (clean=false) but carries no Will: a
    %% subscriber watching the topic hears nothing after the restart.
    {Watcher2, _} = open(<<"watch2">>),
    1 = sub(Watcher2, 1, <<"will/#">>, 1),
    true = mqtt_silence(csock(Watcher2), 300),
    {ok, normal} = mqtt_close(Watcher2),
    ok.

%% C-11's second half: clearing a retained value must not swallow the empty
%% message for subscribers that are already listening (§3.3.1.3 -- retain is
%% about stored state, not about the live forwarding path).
zero_length_retain_still_reaches_live_subscribers(_Config) ->
    ok = boot(),
    {Conn, _} = open(<<"c1">>),
    1 = sub(Conn, 1, <<"z/#">>, 1),
    %% The empty retained publish is NOT retained, so the live frame is all
    %% there is -- exactly the one the case asserts.
    #puback{} = pub_retain(Conn, 1, <<"z/live">>, <<>>, 1),
    {Pub, _} = recv_publish(csock(Conn)),
    #publish{topic = <<"z/live">>, payload = <<>>, retain = 0} = Pub,
    ok = mqtt_puback(Conn, Pub#publish.packet_id),
    error = broker_retain:lookup(<<"z/live">>),
    {ok, normal} = mqtt_close(Conn),
    ok.

will_retain_updates_the_tree(_Config) ->
    ok = boot(),
    {Conn, _} = open(#{client_id => <<"c1">>, will_topic => <<"will/r">>,
                       will_payload => <<"last">>, will_qos => 1,
                       will_retain => 1}),
    {ok, normal} = mqtt_close(Conn),
    %% The Will goes through the same retain store a live PUBLISH would, and
    %% `terminate/3' runs before the process is gone -- so by the time the DOWN
    %% message arrives the tree already has it.
    {ok, #broker_message{destination = <<"will/r">>, payload = <<"last">>}} =
        broker_retain:lookup(<<"will/r">>),
    %% A late subscriber sees it with retain=1, as retained state always is.
    {Late, _} = open(<<"late">>),
    1 = sub(Late, 1, <<"will/#">>, 1),
    {Pub, _} = recv_publish(csock(Late)),
    #publish{retain = 1, payload = <<"last">>} = Pub,
    ok = mqtt_puback(Late, Pub#publish.packet_id),
    {ok, normal} = mqtt_close(Late),
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

reserved_qos_bits_close_without_an_ack(_Config) ->
    ok = boot(),
    {Conn, _} = open(<<"c1">>),
    Monitor = mqtt_monitor(Conn),
    %% QoS bits = 3 are still reserved (§3.3.1.2): the codec refuses the frame
    %% and §4.5's posture for a malformed packet is "close, no ack".
    ok = mqtt_send(Conn, mqtt_raw_publish_qos3(<<"t/a">>, <<"three">>)),
    ok = expect_closed(csock(Conn)),
    {ok, normal} = expect_down(Monitor, cpid(Conn)),
    %% Not stored-then-hidden: a PUBLISH that was never acked must not have
    %% entered the log either.
    [ok = assert_empty_page(P) || P <- broker_queue:partition_ids()],
    ok.

%% ADR-018 §18.3 R5 (Method A): PUBLISH -> PUBREC is the delivery point, so the
%% subscriber has the message before the publisher's handshake is complete.
qos2_inbound_four_steps_delivers_once(_Config) ->
    ok = boot(),
    {Sub, _} = open(<<"sub">>),
    1 = sub(Sub, 1, <<"q/#">>, 1),
    {Pub2, _} = open(<<"pub2">>),
    P = route_partition(<<"q/a">>),
    ok = mqtt_publish(Pub2, 7, <<"q/a">>, <<"exactly-twice-at-most">>, 2),
    #pubrec{packet_id = 7} = mqtt_recv(csock(Pub2)),
    %% Method A: the dispatch happened at PUBLISH time, before PUBREL.
    {Delivered, _} = recv_publish(csock(Sub)),
    #publish{qos = 1, payload = <<"exactly-twice-at-most">>} = Delivered,
    ok = mqtt_puback(Sub, Delivered#publish.packet_id),
    %% The message is in the log exactly once, and the retain flag it carries
    %% says nothing was retained.
    {ok, [{_Offset, Stored}], _Next} = broker_queue:read_from(P, 0, 10),
    #broker_message{payload = <<"exactly-twice-at-most">>,
                    proto_meta = #{qos := 2, retain := 0}} = Stored,
    ok = mqtt_pubrel(Pub2, 7),
    #pubcomp{packet_id = 7} = mqtt_recv(csock(Pub2)),
    %% Nothing else arrives: the handshake completing does not re-deliver.
    true = mqtt_silence(csock(Sub), 200),
    {ok, normal} = mqtt_close(Pub2),
    {ok, normal} = mqtt_close(Sub),
    ok.

%% R5's dedupe half: the SAME packet id again on the same connection is
%% answered with PUBREC and never dispatched a second time.
qos2_inbound_dup_publish_is_not_redelivered(_Config) ->
    ok = boot(),
    {Sub, _} = open(<<"sub">>),
    1 = sub(Sub, 1, <<"q/#">>, 1),
    {Pub2, _} = open(<<"pub2">>),
    ok = mqtt_publish(Pub2, 9, <<"q/a">>, <<"once">>, 2),
    #pubrec{packet_id = 9} = mqtt_recv(csock(Pub2)),
    {Delivered, _} = recv_publish(csock(Sub)),
    ok = mqtt_puback(Sub, Delivered#publish.packet_id),
    %% The client did not see PUBREC and retries the PUBLISH (dup=1 on the
    %% wire in a real client; the codec's record keeps the id either way).
    ok = mqtt_publish(Pub2, 9, <<"q/a">>, <<"once">>, 2),
    #pubrec{packet_id = 9} = mqtt_recv(csock(Pub2)),
    true = mqtt_silence(csock(Sub), 300),
    ok = mqtt_pubrel(Pub2, 9),
    #pubcomp{packet_id = 9} = mqtt_recv(csock(Pub2)),
    {ok, normal} = mqtt_close(Pub2),
    {ok, normal} = mqtt_close(Sub),
    ok.

%% R1/R2: the three idempotent answers. Each is what keeps a client from
%% looping forever after a crash ate the connection's handshake state.
unknown_pubrel_gets_pubcomp(_Config) ->
    ok = boot(),
    {Conn, _} = open(<<"c1">>),
    %% No such inbound QoS2 message ever existed.
    ok = mqtt_pubrel(Conn, 4242),
    #pubcomp{packet_id = 4242} = mqtt_recv(csock(Conn)),
    %% And the connection is still usable afterwards.
    #ping{kind = resp} = mqtt_ping(Conn),
    {ok, normal} = mqtt_close(Conn),
    ok.

unknown_pubrec_gets_pubrel(_Config) ->
    ok = boot(),
    {Conn, _} = open(<<"c1">>),
    ok = mqtt_pubrec(Conn, 4343),
    #pubrel{packet_id = 4343} = mqtt_recv(csock(Conn)),
    #ping{kind = resp} = mqtt_ping(Conn),
    {ok, normal} = mqtt_close(Conn),
    ok.

unknown_pubcomp_is_ignored(_Config) ->
    ok = boot(),
    {Conn, _} = open(<<"c1">>),
    ok = mqtt_pubcomp(Conn, 4444),
    %% Silence, not a disconnect: there is nothing to answer with, and the id
    %% never had a cursor to advance.
    true = mqtt_silence(csock(Conn), 200),
    #ping{kind = resp} = mqtt_ping(Conn),
    {ok, normal} = mqtt_close(Conn),
    ok.

%% R4: `released ∪ received` over the cap is a deliberate disconnect, never a
%% silent drop of QoS2 state. Cap of 2 makes the third PUBLISH the one that
%% trips it.
qos2_state_overflow_disconnects(_Config) ->
    ok = boot(),
    ok = application:set_env(broker_proto_mqtt, max_qos2_state_per_session, 2),
    {Conn, _} = open(<<"c1">>),
    Monitor = mqtt_monitor(Conn),
    ok = mqtt_publish(Conn, 1, <<"o/a">>, <<"1">>, 2),
    #pubrec{packet_id = 1} = mqtt_recv(csock(Conn)),
    ok = mqtt_publish(Conn, 2, <<"o/b">>, <<"2">>, 2),
    #pubrec{packet_id = 2} = mqtt_recv(csock(Conn)),
    ok = mqtt_publish(Conn, 3, <<"o/c">>, <<"3">>, 2),
    ok = expect_closed(csock(Conn)),
    {ok, qos2_state_overflow} = expect_down(Monitor, cpid(Conn)),
    ok.

%% The outbound direction: PUBREC is a halfway mark, PUBCOMP is the ack. Only
%% the last one may move the cursor -- moving it at PUBREC would lose the
%% message if the client died before completing the handshake.
qos2_outbound_advances_cursor_only_on_pubcomp(_Config) ->
    ok = boot(),
    %% A separate publisher on purpose: a QoS2 exchange on the SUBSCRIBER's own
    %% connection would interleave its PUBLISH delivery with the publisher's
    %% PUBREC/PUBCOMP, and the case would be measuring the wrong handshake.
    {Publisher, _} = open(<<"pub">>),
    {Conn, _} = open(<<"c1">>),
    2 = sub(Conn, 1, <<"q2/#">>, 2),
    {ok, SubRef} = subscribed(<<"c1">>, <<"q2/#">>, 2),
    P = route_partition(<<"q2/a">>),
    {ok, Anchor} = broker_subscription:cursor(SubRef, P),
    ok = pub_qos2(Publisher, 3, <<"q2/a">>, <<"two">>),
    {Pub, _} = recv_publish(csock(Conn)),
    #publish{qos = 2, dup = 0, payload = <<"two">>, packet_id = OutPid} = Pub,
    {ok, Anchor} = broker_subscription:cursor(SubRef, P),
    ok = mqtt_pubrec(Conn, OutPid),
    #pubrel{packet_id = OutPid} = mqtt_recv(csock(Conn)),
    %% Half a handshake is not an ack: the cursor must still be at Anchor.
    {ok, Anchor} = broker_subscription:cursor(SubRef, P),
    ok = mqtt_pubcomp(Conn, OutPid),
    wait_cursor_after(SubRef, P, Anchor),
    {ok, normal} = mqtt_close(Conn),
    {ok, normal} = mqtt_close(Publisher),
    ok.

%% A PUBACK for a QoS2 delivery is a peer bug. It must NOT advance the cursor
%% (that would drop a message the peer never completed) and must not disconnect.
puback_for_a_qos2_delivery_does_not_advance_the_cursor(_Config) ->
    ok = boot(),
    {Publisher, _} = open(<<"pub">>),
    {Conn, _} = open(<<"c1">>),
    2 = sub(Conn, 1, <<"q2/#">>, 2),
    {ok, SubRef} = subscribed(<<"c1">>, <<"q2/#">>, 2),
    P = route_partition(<<"q2/b">>),
    {ok, Anchor} = broker_subscription:cursor(SubRef, P),
    ok = pub_qos2(Publisher, 4, <<"q2/b">>, <<"wrong-ack">>),
    {Pub, _} = recv_publish(csock(Conn)),
    #publish{qos = 2, packet_id = OutPid} = Pub,
    ok = mqtt_puback(Conn, OutPid),
    {ok, Anchor} = broker_subscription:cursor(SubRef, P),
    %% The proper handshake still works on the same entry.
    ok = mqtt_pubrec(Conn, OutPid),
    #pubrel{packet_id = OutPid} = mqtt_recv(csock(Conn)),
    ok = mqtt_pubcomp(Conn, OutPid),
    wait_cursor_after(SubRef, P, Anchor),
    {ok, normal} = mqtt_close(Conn),
    {ok, normal} = mqtt_close(Publisher),
    ok.

%% R3's resume rule, half one: crashed BEFORE the PUBREC, so nothing was
%% released -- the peer gets the same packet id with DUP=1 at the SAME level.
reconnect_before_pubrec_resends_a_dup_publish(_Config) ->
    ok = boot(),
    {Publisher, _} = open(<<"pub">>),
    {Subscriber, _} = open(#{client_id => <<"c1">>, clean => false}),
    2 = sub(Subscriber, 1, <<"q2/#">>, 2),
    ok = pub_qos2(Publisher, 1, <<"q2/a">>, <<"two">>),
    {Pub, _} = recv_publish(csock(Subscriber)),
    #publish{qos = 2, dup = 0, packet_id = Pid} = Pub,
    {ok, normal} = mqtt_close(Subscriber),
    {Subscriber2, #connack{session_present = 1}} =
        open(#{client_id => <<"c1">>, clean => false}),
    {Resent, _} = recv_publish(csock(Subscriber2)),
    %% Not downgraded to QoS1: the client's state machine would reject a level
    %% change under the same id.
    #publish{qos = 2, dup = 1, packet_id = Pid, payload = <<"two">>} = Resent,
    ok = mqtt_pubrec(Subscriber2, Pid),
    #pubrel{packet_id = Pid} = mqtt_recv(csock(Subscriber2)),
    ok = mqtt_pubcomp(Subscriber2, Pid),
    {ok, normal} = mqtt_close(Subscriber2),
    {ok, normal} = mqtt_close(Publisher),
    ok.

%% Half two: crashed AFTER PUBREC, so the peer is past PUBLISH. It gets PUBREL
%% and nothing else -- a second PUBLISH would restart a handshake it already
%% half-completed.
reconnect_after_pubrec_resends_pubrel_only(_Config) ->
    ok = boot(),
    {Publisher, _} = open(<<"pub">>),
    {Subscriber, _} = open(#{client_id => <<"c1">>, clean => false}),
    2 = sub(Subscriber, 1, <<"q2/#">>, 2),
    {ok, SubRef} = subscribed(<<"c1">>, <<"q2/#">>, 2),
    %% The partition comes from THIS topic: the cursor whose advance the case
    %% asserts must be the one the message lands in.
    P = route_partition(<<"q2/a">>),
    {ok, Anchor} = broker_subscription:cursor(SubRef, P),
    ok = pub_qos2(Publisher, 1, <<"q2/a">>, <<"two">>),
    {Pub, _} = recv_publish(csock(Subscriber)),
    #publish{qos = 2, packet_id = Pid} = Pub,
    ok = mqtt_pubrec(Subscriber, Pid),
    #pubrel{packet_id = Pid} = mqtt_recv(csock(Subscriber)),
    %% Died holding the PUBREL, before answering PUBCOMP.
    {ok, normal} = mqtt_close(Subscriber),
    {ok, Anchor} = broker_subscription:cursor(SubRef, P),
    {Subscriber2, #connack{session_present = 1}} =
        open(#{client_id => <<"c1">>, clean => false}),
    #pubrel{packet_id = Pid} = mqtt_recv(csock(Subscriber2)),
    %% And no PUBLISH was replayed first: the very next frame is the PUBREL.
    ok = mqtt_pubcomp(Subscriber2, Pid),
    wait_cursor_after(SubRef, P, Anchor),
    {ok, normal} = mqtt_close(Subscriber2),
    {ok, normal} = mqtt_close(Publisher),
    ok.

%% The blob is what makes the two cases above survive a process death: shape v2
%% carries the level, the release marks and the inbound set, and a retained
%% replay (no {Partition, Offset}) is deliberately absent.
inflight_blob_is_v2_and_skips_retained_entries(_Config) ->
    ok = boot(),
    {Conn, _} = open(<<"c1">>),
    1 = sub(Conn, 1, <<"b/#">>, 1),
    #puback{} = pub(Conn, 1, <<"b/a">>, <<"durable">>, 1),
    {Pub, _} = recv_publish(csock(Conn)),
    #publish{packet_id = OutPid} = Pub,
    %% An inbound QoS2 half-handshake belongs in the same blob. The topic is
    %% deliberately OUTSIDE `b/#' above: a topic the client also subscribes to
    %% would loop back and add an outbound entry of its own.
    ok = mqtt_publish(Conn, 11, <<"in/in">>, <<"inbound">>, 2),
    #pubrec{packet_id = 11} = mqtt_recv(csock(Conn)),
    Blob = wait_inflight(<<"c1">>, fun(B) ->
                                           maps:get(vsn, B, 0) =:= 2 andalso
                                           maps:size(maps:get(released, B, #{})) =:= 0
                                   end),
    #{vsn := 2, pending := Pending, qos := Qos, received := Received} = Blob,
    %% One positional entry: the delivery owed a PUBACK. Its offset is the
    %% stored message's own, so only the partition is pinned here.
    [{OutPid, {[{exclusive, <<"c1">>, <<"b/#">>}], {Part, Off}}}] = Pending,
    ?assertEqual(route_partition(<<"b/a">>), Part),
    ?assert(is_integer(Off)),
    ?assertEqual(#{OutPid => 1}, Qos),
    ?assertEqual(#{11 => true}, Received),
    ok = mqtt_puback(Conn, OutPid),
    ok = mqtt_pubrel(Conn, 11),
    #pubcomp{packet_id = 11} = mqtt_recv(csock(Conn)),
    {ok, normal} = mqtt_close(Conn),
    ok.

%% ADR-018 §18.2: retained replay. Ordered, AFTER the SUBACK, retain=1, capped
%% by the granted QoS, and never merged across topics.
subscribe_receives_retained_after_suback(_Config) ->
    ok = boot(),
    {Publisher, _} = open(<<"pub">>),
    #puback{} = pub_retain(Publisher, 1, <<"r/a">>, <<"kept">>, 1),
    {Conn, _} = open(<<"c1">>),
    1 = sub(Conn, 1, <<"r/#">>, 1),
    {Pub, _} = recv_publish(csock(Conn)),
    #publish{retain = 1, qos = 1, topic = <<"r/a">>, payload = <<"kept">>} = Pub,
    ok = mqtt_puback(Conn, Pub#publish.packet_id),
    {ok, normal} = mqtt_close(Conn),
    {ok, normal} = mqtt_close(Publisher),
    ok.

retained_qos_is_capped_by_the_grant(_Config) ->
    ok = boot(),
    {Publisher, _} = open(<<"pub">>),
    #pubrec{} = pub_retain(Publisher, 1, <<"r/a">>, <<"kept">>, 2),
    ok = mqtt_pubrel(Publisher, 1),
    #pubcomp{packet_id = 1} = mqtt_recv(csock(Publisher)),
    %% A QoS0 subscription gets the same message at QoS0: no packet id, no ack,
    %% and its cursor is not involved either (there is no cursor here at all).
    {Conn, _} = open(<<"c1">>),
    0 = sub(Conn, 1, <<"r/#">>, 0),
    {Pub, _} = recv_publish(csock(Conn)),
    #publish{retain = 1, qos = 0, packet_id = undefined, payload = <<"kept">>} = Pub,
    true = mqtt_silence(csock(Conn), 200),
    {ok, normal} = mqtt_close(Conn),
    {ok, normal} = mqtt_close(Publisher),
    ok.

retained_wildcard_delivers_each_topic_in_topic_order(_Config) ->
    ok = boot(),
    {Publisher, _} = open(<<"pub">>),
    Topics = [<<"w/2">>, <<"w/1">>, <<"w/3">>],
    lists:foreach(
      fun({N, Topic}) ->
              #puback{} = pub_retain(Publisher, N, Topic, Topic, 1)
      end, lists:zip(lists:seq(1, length(Topics)), Topics)),
    {Conn, _} = open(<<"c1">>),
    1 = sub(Conn, 1, <<"w/#">>, 1),
    Delivered = [begin
                     {Pub, _} = recv_publish(csock(Conn)),
                     ok = mqtt_puback(Conn, Pub#publish.packet_id),
                     Pub#publish.topic
                 end || _ <- Topics],
    %% `entries/0' is sorted, so the replay order is the topic order -- not the
    %% order the publishes happened to arrive in.
    ?assertEqual(lists:sort(Topics), Delivered),
    {ok, normal} = mqtt_close(Conn),
    {ok, normal} = mqtt_close(Publisher),
    ok.

retain_flag_is_zero_for_live_subscribers(_Config) ->
    ok = boot(),
    {Conn, _} = open(<<"c1">>),
    1 = sub(Conn, 1, <<"r/#">>, 1),
    %% §3.3.1.3: a retained publish forwarded to an EXISTING subscription is a
    %% live message and must carry retain=0, or clients would re-retain it.
    #puback{} = pub_retain(Conn, 1, <<"r/a">>, <<"live">>, 1),
    {Pub, _} = recv_publish(csock(Conn)),
    #publish{retain = 0, payload = <<"live">>} = Pub,
    ok = mqtt_puback(Conn, Pub#publish.packet_id),
    {ok, normal} = mqtt_close(Conn),
    ok.

zero_length_retain_clears_for_new_subscribers(_Config) ->
    ok = boot(),
    {Publisher, _} = open(<<"pub">>),
    #puback{} = pub_retain(Publisher, 1, <<"z/a">>, <<"v1">>, 1),
    {ok, _} = broker_retain:lookup(<<"z/a">>),
    #puback{} = pub_retain(Publisher, 2, <<"z/a">>, <<>>, 1),
    %% Cleared, and a fresh subscriber gets nothing.
    error = broker_retain:lookup(<<"z/a">>),
    {Conn, _} = open(<<"c1">>),
    1 = sub(Conn, 1, <<"z/#">>, 1),
    true = mqtt_silence(csock(Conn), 300),
    {ok, normal} = mqtt_close(Conn),
    {ok, normal} = mqtt_close(Publisher),
    ok.

retained_survives_a_retain_app_restart(_Config) ->
    ok = boot(),
    {Publisher, _} = open(<<"pub">>),
    #puback{} = pub_retain(Publisher, 1, <<"p/a">>, <<"persisted">>, 1),
    %% A real stop/start of the retain app: the tree comes back from its file
    %% alone, which is what makes the PUBACK above a durable promise.
    ok = application:stop(broker_retain),
    {ok, _} = application:ensure_all_started(broker_retain),
    {ok, #broker_message{payload = <<"persisted">>}} = broker_retain:lookup(<<"p/a">>),
    {Conn, _} = open(<<"c1">>),
    1 = sub(Conn, 1, <<"p/#">>, 1),
    {Pub, _} = recv_publish(csock(Conn)),
    #publish{retain = 1, payload = <<"persisted">>} = Pub,
    ok = mqtt_puback(Conn, Pub#publish.packet_id),
    {ok, normal} = mqtt_close(Conn),
    {ok, normal} = mqtt_close(Publisher),
    ok.

retain_overflow_still_delivers_live(_Config) ->
    ok = boot(),
    ok = application:set_env(broker_retain, max_retained, 1),
    {Conn, _} = open(<<"c1">>),
    1 = sub(Conn, 1, <<"o/#">>, 1),
    #puback{} = pub_retain(Conn, 1, <<"o/a">>, <<"first">>, 1),
    {First, _} = recv_publish(csock(Conn)),
    ok = mqtt_puback(Conn, First#publish.packet_id),
    %% The tree is full: the entry is refused, but the live subscriber still
    %% sees the message and the publisher still gets its ack (ADR-018 §18.2:
    %% bounded refusal, never a dropped publish).
    #puback{packet_id = 2} = pub_retain(Conn, 2, <<"o/b">>, <<"second">>, 1),
    {Second, _} = recv_publish(csock(Conn)),
    #publish{payload = <<"second">>, retain = 0} = Second,
    ok = mqtt_puback(Conn, Second#publish.packet_id),
    error = broker_retain:lookup(<<"o/b">>),
    ?assertEqual(1, maps:get(count, broker_retain:stats())),
    %% C-14's observability half: the refusal is counted, so an operator can
    %% size the cap from data instead of guessing.
    ?assert(maps:get(retain_overflow, broker_metrics:snapshot()) >= 1),
    {ok, normal} = mqtt_close(Conn),
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

%%%_* ADR-019: Shared / Failover over the wire --------------------------------
%%
%% MQTT 3.1.1 has no syntax for a group (the `$share/...' spelling is §13.4's
%% S2.5 work), so the group is created through the internal API and the conn is
%% the consumer under test: claim, read, ack, redeliver. What these cases pin is
%% the end-to-end invariant the wire can see -- "one message, one member", the
%% standby's silence, and promotion after a death -- not the bookkeeping (that
%% is `broker_subscription_SUITE''s job).
%%
%% The group is always joined *after* the connections are up: a clean-session
%% CONNECT deliberately leaves every group the client was in (§4.2), so joining
%% first would be wiped by the very next `clean_session_teardown/1'.

a_shared_group_delivers_each_message_exactly_once(_Config) ->
    ok = boot(),
    {C1, _} = open(<<"c1">>),
    {C2, _} = open(<<"c2">>),
    {ok, SubRef} = join_group(shared, <<"g">>, <<"c1">>, <<"s/#">>, 1),
    {ok, SubRef} = join_group(shared, <<"g">>, <<"c2">>, <<"s/#">>, 1),
    {Publisher, _} = open(<<"pub">>),
    P = route_partition(<<"s/x">>),
    Payloads = [<<"1">>, <<"2">>, <<"3">>, <<"4">>],
    [#puback{} = pub(Publisher, N, <<"s/x">>, Payload, 1)
     || {N, Payload} <- lists:zip([1, 2, 3, 4], Payloads)],
    %% Each offset is claimed by exactly one member (§19.1), so the union of the
    %% two sockets must be the four payloads -- once each, never twice. Which
    %% member won which page is a race and is not asserted.
    Delivered = collect_publishes([C1, C2], 4),
    Payloads = lists:sort([Pub#publish.payload || {_Conn, Pub} <- Delivered]),
    true = lists:all(fun({_Conn, Pub1}) -> Pub1#publish.qos =:= 1 end, Delivered),
    [ok = mqtt_puback(Conn, Pid) || {Conn, #publish{packet_id = Pid}} <- Delivered],
    %% `low' is the contiguous prefix nobody holds any more: with every offset
    %% acked it lands exactly on the stored high watermark.
    wait_cursor_at(SubRef, P, high_watermark(P)),
    {ok, normal} = mqtt_close(C1),
    {ok, normal} = mqtt_close(C2),
    {ok, normal} = mqtt_close(Publisher),
    ok.

a_dead_shared_member_hands_its_page_to_the_survivor(_Config) ->
    ok = boot(),
    %% `clean = false' on purpose: the session has to survive the transport
    %% death, or the teardown would delete the group's last member and with it
    %% the cursor the redelivery is about.
    {C1, _} = open(#{client_id => <<"c1">>, clean => false}),
    {ok, SubRef} = join_group(shared, <<"g">>, <<"c1">>, <<"s/#">>, 1),
    {Publisher, _} = open(<<"pub">>),
    P = route_partition(<<"s/x">>),
    Payloads = [<<"1">>, <<"2">>, <<"3">>, <<"4">>],
    [#puback{} = pub(Publisher, N, <<"s/x">>, Payload, 1)
     || {N, Payload} <- lists:zip([1, 2, 3, 4], Payloads)],
    %% One member, so it takes the whole page -- and never PUBACKs it.
    Delivered = collect_publishes([C1], 4),
    Payloads = lists:sort([Pub#publish.payload || {_, Pub} <- Delivered]),
    {ok, normal} = mqtt_close(C1),
    %% The claim was a lease on a live attachment: the DOWN hands every
    %% un-acked offset back to the pool (§19.2), which is why S2-D needs no
    %% retry timer. A consumer arriving afterwards reads the same offsets.
    {C2, _} = open(<<"c2">>),
    {ok, SubRef} = join_group(shared, <<"g">>, <<"c2">>, <<"s/#">>, 1),
    Redelivered = collect_publishes([C2], 4),
    Payloads = lists:sort([Pub#publish.payload || {_, Pub} <- Redelivered]),
    [ok = mqtt_puback(Conn, Pid) || {Conn, #publish{packet_id = Pid}} <- Redelivered],
    wait_cursor_at(SubRef, P, high_watermark(P)),
    {ok, normal} = mqtt_close(C2),
    {ok, normal} = mqtt_close(Publisher),
    ok.

a_standby_failover_member_receives_nothing(_Config) ->
    ok = boot(),
    {C1, _} = open(<<"c1">>),
    {C2, _} = open(<<"c2">>),
    {ok, SubRef} = join_group(failover, <<"g">>, <<"c1">>, <<"f/#">>, 1),
    {ok, SubRef} = join_group(failover, <<"g">>, <<"c2">>, <<"f/#">>, 1),
    %% The member order is the election (§19.4): first joined, first active.
    active = broker_subscription:role(SubRef, <<"c1">>),
    standby = broker_subscription:role(SubRef, <<"c2">>),
    {Publisher, _} = open(<<"pub">>),
    P = route_partition(<<"f/x">>),
    Payloads = [<<"1">>, <<"2">>, <<"3">>],
    [#puback{} = pub(Publisher, N, <<"f/x">>, Payload, 1)
     || {N, Payload} <- lists:zip([1, 2, 3], Payloads)],
    Delivered = collect_publishes([C1], 3),
    Payloads = lists:sort([Pub#publish.payload || {_, Pub} <- Delivered]),
    %% The standby is not merely skipped at delivery time -- it never gets a
    %% wake, never claims and never reads (subs_for/3 drops it before the read).
    true = mqtt_silence(csock(C2), 500),
    [ok = mqtt_puback(Conn, Pid) || {Conn, #publish{packet_id = Pid}} <- Delivered],
    wait_cursor_at(SubRef, P, high_watermark(P)),
    {ok, normal} = mqtt_close(C1),
    {ok, normal} = mqtt_close(C2),
    {ok, normal} = mqtt_close(Publisher),
    ok.

failover_promotes_the_next_member_when_the_active_disconnects(_Config) ->
    ok = boot(),
    {C1, _} = open(<<"c1">>),
    {C2, _} = open(<<"c2">>),
    {ok, SubRef} = join_group(failover, <<"g">>, <<"c1">>, <<"f/#">>, 1),
    {ok, SubRef} = join_group(failover, <<"g">>, <<"c2">>, <<"f/#">>, 1),
    {Publisher, _} = open(<<"pub">>),
    P = route_partition(<<"f/x">>),
    [#puback{} = pub(Publisher, N, <<"f/x">>, <<"1">>, 1) || N <- [1, 2]],
    First = collect_publishes([C1], 2),
    [ok = mqtt_puback(Conn, Pid) || {Conn, #publish{packet_id = Pid}} <- First],
    %% The active leaves. Its cursor is a plain scalar, so the successor picks
    %% up exactly where the acked prefix ended -- no message is replayed and
    %% none is skipped.
    {ok, normal} = mqtt_close(C1),
    ok = wait_role(SubRef, <<"c2">>, active),
    [#puback{} = pub(Publisher, N, <<"f/x">>, P2, 1)
     || {N, P2} <- [{3, <<"3">>}, {4, <<"4">>}]],
    Second = collect_publishes([C2], 2),
    [<<"3">>, <<"4">>] = lists:sort([Pub#publish.payload || {_, Pub} <- Second]),
    [ok = mqtt_puback(Conn, Pid) || {Conn, #publish{packet_id = Pid}} <- Second],
    wait_cursor_at(SubRef, P, high_watermark(P)),
    {ok, normal} = mqtt_close(C2),
    {ok, normal} = mqtt_close(Publisher),
    ok.

a_qos0_publish_reaches_exactly_one_shared_member_over_the_wire(_Config) ->
    ok = boot(),
    {C1, _} = open(<<"c1">>),
    {C2, _} = open(<<"c2">>),
    {ok, SubRef} = join_group(shared, <<"g">>, <<"c1">>, <<"s/#">>, 1),
    {ok, SubRef} = join_group(shared, <<"g">>, <<"c2">>, <<"s/#">>, 1),
    P = route_partition(<<"s/x">>),
    {ok, Anchor} = broker_subscription:cursor(SubRef, P),
    {Publisher, _} = open(<<"pub">>),
    %% A QoS0 publish never enters the log, so there is no offset to claim: the
    %% push IS the delivery, and pushing to every member would deliver N copies
    %% (§2.5). Exactly one member is picked.
    ok = mqtt_publish(Publisher, undefined, <<"s/x">>, <<"fast">>, 0),
    [{Winner, Pub}] = collect_publishes([C1, C2], 1, 4000),
    #publish{qos = 0, packet_id = undefined, payload = <<"fast">>} = Pub,
    Other = case Winner =:= C1 of
                true -> csock(C2);
                false -> csock(C1)
            end,
    %% ...and the other member sees nothing at all: one copy on the wire, not N.
    true = mqtt_silence(Other, 300),
    %% Nothing was stored, so no cursor moved: a QoS0 message is not backlog.
    {ok, Anchor} = broker_subscription:cursor(SubRef, P),
    [ok = assert_empty_page(Part) || Part <- broker_queue:partition_ids()],
    {ok, normal} = mqtt_close(C1),
    {ok, normal} = mqtt_close(C2),
    {ok, normal} = mqtt_close(Publisher),
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

%% A retained publish: `retain=1' is what makes the conn touch the retain tree
%% (ADR-018 §18.2), and it is not something `mqtt_publish/5' can express.
pub_retain(Conn, Pid, Topic, Payload, QoS) ->
    ok = mqtt_send(Conn, mqtt_frame:encode(
                            #publish{qos = QoS, retain = 1, topic = Topic,
                                     packet_id = Pid, payload = Payload})),
    mqtt_recv(csock(Conn)).

%% A full QoS2 publish handshake from the client side, answering the broker the
%% way a real client does. Returns `ok' once PUBCOMP closed the exchange.
pub_qos2(Conn, Pid, Topic, Payload) ->
    ok = mqtt_publish(Conn, Pid, Topic, Payload, 2),
    #pubrec{packet_id = Pid} = mqtt_recv(csock(Conn)),
    ok = mqtt_pubrel(Conn, Pid),
    #pubcomp{packet_id = Pid} = mqtt_recv(csock(Conn)),
    ok.

%% The in-flight blob is written on a timer (`inflight_snapshot_ms') and again
%% by terminate/3, so a case that wants to read it waits for the shape it
%% expects rather than sleeping a guess.
wait_inflight(ClientId, Pred) ->
    wait_inflight(ClientId, Pred, 3000).

wait_inflight(ClientId, Pred, Budget) ->
    Blob = case broker_subscription:session(ClientId) of
               {ok, #{inflight := B}} -> B;
               _ -> undefined
           end,
    case is_map(Blob) andalso Pred(Blob) of
        true -> Blob;
        false when Budget > 50 ->
            timer:sleep(25),
            wait_inflight(ClientId, Pred, Budget - 25);
        false ->
            exit({inflight_stuck, ClientId, Blob})
    end.

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

%%%_* Group (Shared / Failover) helpers ---------------------------------------
%%
%% The group has to be created through the internal API because MQTT 3.1.1 has no
%% `$share' syntax yet (§13.4 defers it to S2.5). Joining is a worker call, so a
%% conn that was given the same client_id is the consumer these cases drive.

join_group(Kind, Group, ClientId, Filter, QoS) ->
    broker_subscription:join(Kind, Group, ClientId, Filter, QoS).

%% Collect exactly N PUBLISH frames from a group of subscriber sockets. No
%% socket is entrusted with more than it sends: each poll asks every socket in
%% turn for one frame and the answer is appended with the socket it came from,
%% which is what lets a case assert "the whole group saw each message once"
%% without predicting the claim race.
collect_publishes(Conns, N) ->
    collect_publishes(Conns, N, 4000).

collect_publishes(_Conns, N, _Budget) when N =< 0 ->
    [];collect_publishes(Conns, N, Budget) ->
    case poll_one(Conns, min(Budget, 200)) of
        none when Budget > 0 ->
            collect_publishes(Conns, N, Budget - 200);
        none ->
            exit({missing_publishes, N, Conns});
        {Conn, #publish{} = Pub} ->
            [{Conn, Pub} | collect_publishes(Conns, N - 1, Budget)];
        {_Conn, _Other} ->
            %% A ping response or a stray ack is not the frame under test; the
            %% group sockets only ever see publishes, but skipping instead of
            %% dying keeps a future frame type from turning this into a flake.
            collect_publishes(Conns, N, Budget)
    end.

poll_one([], _Timeout) ->
    none;
poll_one([Conn | Rest], Timeout) ->
    case mqtt_recv_or(csock(Conn), Timeout, nothing) of
        nothing -> poll_one(Rest, Timeout);
        {error, closed} -> exit({group_socket_closed, Conn});
        Frame -> {Conn, Frame}
    end.

%% The shared row's `low' lands on the partition's high watermark once every
%% offset of the page has been acked (High = Last + 1, so `next_read' = High).
wait_cursor_at(SubRef, Partition, Target) ->
    wait_cursor_at(SubRef, Partition, Target, 3000).

wait_cursor_at(SubRef, Partition, Target, Budget) ->
    case broker_subscription:cursor(SubRef, Partition) of
        {ok, Target} ->
            ok;
        Other when Budget > 50 ->
            _ = Other,
            timer:sleep(25),
            wait_cursor_at(SubRef, Partition, Target, Budget - 25);
        Other ->
            exit({cursor_never_reached, SubRef, Partition, Target, Other})
    end.

high_watermark(Partition) ->
    {ok, _Committed, High} = broker_queue:watermarks(Partition),
    High.

%% The facade's `role/2' answers from the attachment table (pid liveness), so it
%% flips the instant the old conn is gone; the worker's promotion counter is one
%% DOWN message behind. Polling the role is therefore polling the fact the wire
%% cares about -- who serves the next message.
wait_role(SubRef, ClientId, Role) ->
    wait_role(SubRef, ClientId, Role, 3000).

wait_role(SubRef, ClientId, Role, Budget) ->
    case broker_subscription:role(SubRef, ClientId) of
        Role ->
            ok;
        _ when Budget > 50 ->
            timer:sleep(25),
            wait_role(SubRef, ClientId, Role, Budget - 25);
        Other ->
            exit({role_never_became, SubRef, ClientId, Role, Other})
    end.
