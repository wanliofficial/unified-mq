%% @doc T13: the S1 vertical slice end-to-end over the real listener, with both
%% `broker_storage' backends held to the same suite.
%%
%% Gate S1 asks for "`broker_storage` 的 WAL 与内存实现可互换：同一测试套件跑
%% 两个后端". The suite therefore has two CT groups, `wal' and `memory', with
%% identical case lists; the only thing that changes is the backend env and the
%% listener's `mqtt_max_qos' (E12: a memory backend advertises `[os_buffer]', so
%% the effective max QoS is 0 and QoS1 must be refused at SUBACK / PUBLISH rather
%% than failing later at append time). Cases branch on `max_qos' and assert the
%% contract each backend actually has -- no `skip', so a regressed backend shows
%% up as a failure, not as a quietly skipped case.
%%
%% Everything below goes through a real TCP socket and the ranch listener from
%% T12; nothing pokes `mqtt_conn_fsm' directly. Assertions poll observable state
%% (`broker_queue:watermarks/1', `broker_subscription:cursor/2') instead of
%% sleeping for a fixed time, so a slow machine fails the case, not the contract.
-module(s1_e2e_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("broker_proto_mqtt/include/mqtt_frame.hrl").

-define(N, 2).
%% The listener ref defaults to `{mqtt, Port}'; with `port => 0' T12's suite uses
%% the same constant, and the real port is read back from ranch, never assumed.
-define(REF, {mqtt, 0}).
-define(POLL_MS, 25).
-define(POLL_BUDGET_MS, 5000).
%% Short so a missing wakeup/catch-up shows up inside a case instead of waiting
%% for the production default (1000 ms).
-define(PULL_POLL_MS, 50).

%%%_* CT scaffolding -----------------------------------------------------------

all() ->
    [{group, wal}, {group, memory}].

groups() ->
    Cases = [qos0_publish_subscribe_roundtrip,
             qos1_posture_follows_backend_capability,
             subscription_qos_is_not_escalated,
             clean_session_false_restores_the_subscription_set,
             clean_session_true_discards_the_session,
             offline_catch_up_is_offset_ordered],
    [{wal, [], Cases},
     {memory, [], Cases}].

init_per_group(wal, Config) ->
    [{group_name, wal}, {backend, broker_storage_wal}, {max_qos, 1} | Config];
init_per_group(memory, Config) ->
    [{group_name, memory}, {backend, broker_storage_memory}, {max_qos, 0} | Config].

end_per_group(_Group, _Config) ->
    ok.

init_per_suite(Config) ->
    %% `_ =' on purpose (J10/L19): another suite on this node has usually loaded
    %% these already, and `{already_loaded, _}' is not a failure here.
    _ = application:load(broker_storage),
    _ = application:load(broker_router),
    _ = application:load(broker_queue),
    _ = application:load(broker_subscription),
    _ = application:load(broker_event),
    _ = application:load(broker_security),
    _ = application:load(broker_proto_mqtt),
    Config.

end_per_suite(_Config) ->
    stop_all(),
    ok.

init_per_testcase(Case, Config) ->
    stop_all(),
    Backend = ?config(backend, Config),
    MaxQos = ?config(max_qos, Config),
    Group = atom_to_list(?config(group_name, Config)),
    DataDir = filename:join([?config(priv_dir, Config), Group,
                             atom_to_list(Case), "data"]),
    ok = filelib:ensure_dir(filename:join([DataDir, "x"])),
    %% Every knob both layers read is written in every case: no earlier case can
    %% poison this one. `require_durable_fs => false' is the documented test
    %% posture (J11): the data dir only has to be a real filesystem for the
    %% production release, not for CT's per-case scratch tree.
    ok = application:set_env(broker_storage, backend, Backend),
    ok = application:set_env(broker_storage, data_dir, DataDir),
    ok = application:set_env(broker_storage, wal, #{require_durable_fs => false}),
    ok = application:set_env(broker_queue, partitions, ?N),
    ok = application:set_env(broker_router, partitions, ?N),
    ok = application:set_env(broker_subscription, data_dir, DataDir),
    ok = application:set_env(broker_subscription, cursor_max_regress, 64),
    ok = application:set_env(broker_subscription, cursor_flush_interval_ms, 100),
    ok = application:set_env(broker_subscription, inflight_flush_interval_ms, 100),
    ok = application:set_env(broker_event, strategy, pass_through),
    ok = application:set_env(broker_security, users, []),
    ok = application:set_env(broker_security, acl, allow),
    %% The listener map is the L20 seam: the six connection knobs travel from
    %% here to every conn, so the group's `mqtt_max_qos' reaches the session
    %% without touching the app-level default.
    ok = application:set_env(broker_proto_mqtt, listeners,
                             [#{port => 0,
                                num_acceptors => 4,
                                max_connections => 1024,
                                mqtt_max_qos => MaxQos,
                                max_keepalive_ms => 60000,
                                pull_page_limit => 32,
                                max_inflight => 64,
                                pull_poll_interval_ms => ?PULL_POLL_MS,
                                inflight_snapshot_ms => 20}]),
    ok = application:set_env(broker_proto_mqtt, pull_page_limit, 32),
    ok = application:set_env(broker_proto_mqtt, max_inflight, 64),
    ok = application:set_env(broker_proto_mqtt, pull_poll_interval_ms, ?PULL_POLL_MS),
    ok = application:set_env(broker_proto_mqtt, inflight_snapshot_ms, 20),
    ok = application:set_env(broker_proto_mqtt, max_keepalive_ms, 60000),
    ok = application:set_env(broker_proto_mqtt, mqtt_max_qos, MaxQos),
    %% Boot step 1: storage up, then the out-of-band create (H8) before anything
    %% may recover a partition. `ensure_all_started(broker_proto_mqtt)' then
    %% walks the whole tree and ends with the listener (design §6 step 4).
    {ok, _} = application:ensure_all_started(broker_storage),
    ok = create_partitions(),
    {ok, _} = application:ensure_all_started(broker_proto_mqtt),
    [{data_dir, DataDir}, {port, listener_port()} | Config].

end_per_testcase(_Case, _Config) ->
    kill_conns(),
    stop_all(),
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok.

%%%_* Fixture helpers ---------------------------------------------------------

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
    case ranch:get_port(?REF) of
        Port when is_integer(Port) -> Port;
        Other -> exit({listener_port_unavailable, Other})
    end.

stop_all() ->
    _ = application:stop(broker_proto_mqtt),
    _ = application:stop(broker_event),
    _ = application:stop(broker_security),
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

%% QoS0 is the one delivery class both backends support: no append, no PUBACK,
%% no cursor. The subscriber sees the payload pushed straight from the publisher
%% connection (§4.4), and the partition's watermarks do not move -- that is the
%% difference between "delivered" and "stored", asserted on the backend itself.
qos0_publish_subscribe_roundtrip(Config) ->
    Port = ?config(port, Config),
    Sub = connect(Port),
    #connack{session_present = 0, return_code = 0} =
        open(Sub, #{client_id => <<"q0-sub">>}),
    #suback{return_codes = [0]} = subscribe(Sub, 1, <<"q0/#">>, 0),
    Partition = route_partition(<<"q0/x">>),
    {ok, C0, H0} = broker_queue:watermarks(Partition),
    Pub = connect(Port),
    #connack{return_code = 0} = open(Pub, #{client_id => <<"q0-pub">>}),
    ok = publish(Pub, 1, <<"q0/x">>, <<"volatile">>, 0),
    #publish{qos = 0, dup = 0, retain = 0, packet_id = undefined,
             topic = <<"q0/x">>, payload = <<"volatile">>} = recv(Sub),
    %% No PUBACK: QoS0 is fire-and-forget in both directions.
    ok = expect_silence(Pub, 200),
    %% The log stayed untouched -- the same bytes reached the subscriber through
    %% the unpositioned push, not through a stored record.
    {ok, C0, H0} = broker_queue:watermarks(Partition),
    ok = close_sock(Sub),
    ok = close_sock(Pub).

%% The Gate's "interchangeable backends" line, observed at the protocol edge.
%%
%% WAL: QoS1 is granted, the message is appended+fsynced before PUBACK, the
%% subscriber acknowledges, and the cursor/committed watermark move.
%%
%% memory: the backend capability is `[os_buffer]', so the effective max QoS is 0
%% (E2). A QoS1 SUBSCRIBE is refused with 0x80 and a QoS1 PUBLISH closes the
%% connection with no PUBACK at all (E12) -- never a silent downgrade and never
%% a late failure at append time.
qos1_posture_follows_backend_capability(Config) ->
    MaxQos = ?config(max_qos, Config),
    Port = ?config(port, Config),
    Sub = connect(Port),
    #connack{return_code = 0} = open(Sub, #{client_id => <<"q1-sub">>}),
    Partition = route_partition(<<"q1/x">>),
    SubRef = {<<"q1-sub">>, <<"q1/#">>},
    case MaxQos of
        1 ->
            #suback{return_codes = [1]} = subscribe(Sub, 1, <<"q1/#">>, 1),
            {ok, Anchor} = broker_subscription:cursor(SubRef, Partition),
            {ok, _C0, H0} = broker_queue:watermarks(Partition),
            Pub = connect(Port),
            #connack{return_code = 0} = open(Pub, #{client_id => <<"q1-pub">>}),
            ok = publish(Pub, 7, <<"q1/x">>, <<"durable">>, 1),
            %% PUBACK echoes the client's own packet id and is only sent after
            %% the fsync append returned (R1a, §5.1(e)).
            #puback{packet_id = 7} = recv(Pub),
            #publish{qos = 1, dup = 0, packet_id = WirePid,
                     payload = <<"durable">>} = recv(Sub),
            {ok, _C1, H1} = broker_queue:watermarks(Partition),
            true = H1 =:= H0 + 1,
            ok = puback(Sub, WirePid),
            ok = wait_cursor_after(SubRef, Partition, Anchor),
            %% committed = contiguous acked prefix; once the only cursor has
            %% passed the message, the storage watermark names its offset.
            ok = wait_committed(Partition, H1 - 1),
            ok = close_sock(Sub),
            ok = close_sock(Pub);
        0 ->
            %% E12, SUBACK half: the over-cap QoS is refused, so no entity and
            %% no cursor exist afterwards.
            #suback{return_codes = [128]} = subscribe(Sub, 1, <<"q1/#">>, 1),
            [] = broker_subscription:subscriptions(<<"q1-sub">>),
            {ok, CM0, HM0} = broker_queue:watermarks(Partition),
            Pub = connect(Port),
            #connack{return_code = 0} = open(Pub, #{client_id => <<"q1-pub">>}),
            %% E12, PUBLISH half: close without acking, and nothing appended.
            ok = publish(Pub, 7, <<"q1/x">>, <<"rejected">>, 1),
            ok = expect_closed(Pub),
            {ok, CM0, HM0} = broker_queue:watermarks(Partition),
            ok = close_sock(Sub)
    end.

%% §4.8: delivery QoS is min(publish QoS, max QoS of the subscriptions that
%% hit). A QoS0 subscription that matches a stored QoS1 message gets a QoS0
%% frame with no packet id, and the cursor advances by read-and-skip, so the
%% committed watermark is not pinned behind traffic nobody asked to ack.
subscription_qos_is_not_escalated(Config) ->
    MaxQos = ?config(max_qos, Config),
    Port = ?config(port, Config),
    Sub = connect(Port),
    #connack{return_code = 0} = open(Sub, #{client_id => <<"mix-sub">>}),
    #suback{return_codes = [0]} = subscribe(Sub, 1, <<"mix/#">>, 0),
    Partition = route_partition(<<"mix/x">>),
    SubRef = {<<"mix-sub">>, <<"mix/#">>},
    {ok, Anchor} = broker_subscription:cursor(SubRef, Partition),
    {ok, C0, H0} = broker_queue:watermarks(Partition),
    Pub = connect(Port),
    #connack{return_code = 0} = open(Pub, #{client_id => <<"mix-pub">>}),
    case MaxQos of
        1 ->
            ok = publish(Pub, 11, <<"mix/x">>, <<"stored-as-qos1">>, 1),
            #puback{packet_id = 11} = recv(Pub),
            %% Same message, delivered at the subscription's level.
            #publish{qos = 0, dup = 0, packet_id = undefined,
                     payload = <<"stored-as-qos1">>} = recv(Sub),
            {ok, _C1, H1} = broker_queue:watermarks(Partition),
            true = H1 =:= H0 + 1,
            ok = wait_cursor_after(SubRef, Partition, Anchor),
            %% Read-and-skip moved the cursor, so committed does not stay pinned
            %% at the pre-publish watermark.
            ok = wait_committed(Partition, H1 - 1),
            ok = close_sock(Pub);
        0 ->
            %% A memory backend cannot store QoS1 at all; the publisher is cut
            %% off (the case above pins the absence of PUBACK), and a QoS0
            %% publish on a fresh connection is still delivered at QoS0.
            ok = publish(Pub, 11, <<"mix/x">>, <<"stored-as-qos1">>, 1),
            ok = expect_closed(Pub),
            Pub2 = connect(Port),
            #connack{return_code = 0} = open(Pub2, #{client_id => <<"mix-pub2">>}),
            ok = publish(Pub2, 12, <<"mix/x">>, <<"volatile">>, 0),
            #publish{qos = 0, dup = 0, packet_id = undefined,
                     payload = <<"volatile">>} = recv(Sub),
            %% Neither the rejected QoS1 nor the QoS0 push touched the log.
            {ok, C0, H0} = broker_queue:watermarks(Partition),
            ok = close_sock(Pub2)
    end,
    ok = close_sock(Sub).

%% R1b's protocol-level half: a non-clean session that loses its socket is
%% resumed by the next CONNECT -- `session_present = 1', the same subscription
%% set, and delivery works **without the client resending SUBSCRIBE**. (The
%% broker-kill half is T14's R1b; here the process dies, not the node.)
clean_session_false_restores_the_subscription_set(Config) ->
    MaxQos = ?config(max_qos, Config),
    Port = ?config(port, Config),
    ClientId = <<"persist-sub">>,
    Sub1 = connect(Port),
    #connack{session_present = 0, return_code = 0} =
        open(Sub1, #{client_id => ClientId, clean => false}),
    #suback{return_codes = [MaxQos]} = subscribe(Sub1, 1, <<"keep/#">>, MaxQos),
    [Row] = broker_subscription:subscriptions(ClientId),
    #{filter := <<"keep/#">>, qos := MaxQos} = Row,
    ok = close_sock(Sub1),
    ok = wait_offline(ClientId),
    [_] = broker_subscription:subscriptions(ClientId),

    Sub2 = connect(Port),
    #connack{session_present = 1, return_code = 0} =
        open(Sub2, #{client_id => ClientId, clean => false}),
    %% No SUBSCRIBE in between: the set came back with the session.
    [Row2] = broker_subscription:subscriptions(ClientId),
    #{filter := <<"keep/#">>, qos := MaxQos} = Row2,
    %% ...and the restored subscription is live, not just a stored row.
    Pub = connect(Port),
    #connack{return_code = 0} = open(Pub, #{client_id => <<"keep-pub">>}),
    ok = publish(Pub, 1, <<"keep/x">>, <<"after-reconnect">>, 0),
    #publish{qos = 0, packet_id = undefined, payload = <<"after-reconnect">>} =
        recv(Sub2),
    ok = close_sock(Sub2),
    ok = close_sock(Pub).

%% §4.2 clean-session teardown: CONNECT with clean=1 wipes the old set (the
%% CONNACK says it is not resumed), and the connection's own detach removes the
%% files too -- a clean session leaves nothing for the next boot to find.
clean_session_true_discards_the_session(Config) ->
    Port = ?config(port, Config),
    ClientId = <<"wipe-me">>,
    SubsPath = broker_subscription_files:subs_path(ClientId),
    Sub1 = connect(Port),
    #connack{session_present = 0, return_code = 0} =
        open(Sub1, #{client_id => ClientId, clean => false}),
    #suback{return_codes = [0]} = subscribe(Sub1, 1, <<"wipe/#">>, 0),
    [_] = broker_subscription:subscriptions(ClientId),
    true = filelib:is_regular(SubsPath),
    ok = close_sock(Sub1),
    ok = wait_offline(ClientId),

    Sub2 = connect(Port),
    #connack{session_present = 0, return_code = 0} =
        open(Sub2, #{client_id => ClientId, clean => true}),
    %% The teardown runs before CONNACK: the old entity and its cursors are
    %% already gone even though the connection is still up.
    [] = broker_subscription:subscriptions(ClientId),
    %% Closing the clean connection finishes the job on disk (detach with
    %% persist=false). Poll: nothing may recreate the file.
    ok = close_sock(Sub2),
    ok = wait_until(fun() -> not filelib:is_regular(SubsPath) end,
                   {subs_file_survived_clean_session, SubsPath}),
    {error, not_found} = broker_subscription:session(ClientId).

%% E7: a non-clean subscriber that goes offline while QoS1 traffic arrives is
%% caught up by the reconnect round in offset order, with packet ids of its own
%% and no gap. On memory the same scenario can only be QoS0, which is not
%% stored: the reconnect must see **nothing** -- a backend that quietly invented
%% a backlog would fail here.
offline_catch_up_is_offset_ordered(Config) ->
    MaxQos = ?config(max_qos, Config),
    Port = ?config(port, Config),
    ClientId = <<"catch-up">>,
    SubRef = {ClientId, <<"off/#">>},
    Partition = route_partition(<<"off/x">>),
    Sub1 = connect(Port),
    #connack{session_present = 0, return_code = 0} =
        open(Sub1, #{client_id => ClientId, clean => false}),
    #suback{return_codes = [MaxQos]} = subscribe(Sub1, 1, <<"off/#">>, MaxQos),
    {ok, Anchor} = broker_subscription:cursor(SubRef, Partition),
    ok = close_sock(Sub1),
    ok = wait_offline(ClientId),

    Pub = connect(Port),
    #connack{return_code = 0} = open(Pub, #{client_id => <<"off-pub">>}),
    case MaxQos of
        1 ->
            {ok, _C0, H0} = broker_queue:watermarks(Partition),
            Payloads = [<<"m1">>, <<"m2">>, <<"m3">>],
            ok = publish_many_qos1(Pub, <<"off/x">>, Payloads),
            {ok, _C1, H1} = broker_queue:watermarks(Partition),
            true = H1 =:= H0 + 3,

            Sub2 = connect(Port),
            #connack{session_present = 1, return_code = 0} =
                open(Sub2, #{client_id => ClientId, clean => false}),
            Frames = [recv(Sub2), recv(Sub2), recv(Sub2)],
            Payloads = [F#publish.payload || F <- Frames],
            true = lists:all(fun(F) ->
                                     F#publish.qos =:= 1 andalso F#publish.dup =:= 0
                             end, Frames),
            %% Acknowledge in arrival order; the cursor then names the offset
            %% after the last message, and committed follows it.
            lists:foreach(fun(F) -> ok = puback(Sub2, F#publish.packet_id) end, Frames),
            ok = wait_cursor_at_least(SubRef, Partition, Anchor + 3),
            ok = wait_committed(Partition, H1 - 1),
            ok = close_sock(Sub2);
        0 ->
            {ok, CM0, HM0} = broker_queue:watermarks(Partition),
            ok = publish(Pub, 1, <<"off/x">>, <<"not-stored">>, 0),
            %% QoS0 never enters the log (whatever the backend), so there is
            %% nothing for the reconnect to catch up on.
            {ok, CM0, HM0} = broker_queue:watermarks(Partition),

            Sub2 = connect(Port),
            #connack{session_present = 1, return_code = 0} =
                open(Sub2, #{client_id => ClientId, clean => false}),
            ok = expect_silence(Sub2, 400),
            %% The restored session is at the same cursor: nothing was invented.
            {ok, Anchor} = broker_subscription:cursor(SubRef, Partition),
            ok = close_sock(Sub2)
    end,
    ok = close_sock(Pub).

%%%_* MQTT client over a real socket -------------------------------------------

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

publish(Sock, PacketId, Topic, Payload, QoS) ->
    gen_tcp:send(Sock, mqtt_frame:encode(
                         #publish{qos = QoS, topic = Topic,
                                  packet_id = PacketId, payload = Payload})).

puback(Sock, PacketId) ->
    gen_tcp:send(Sock, mqtt_frame:encode(#puback{packet_id = PacketId})).

%% Three QoS1 publishes at once, ids 1..3; the server's PUBACKs may arrive in
%% any order (each is answered when its own fsync returns), so only the set is
%% asserted.
publish_many_qos1(Sock, Topic, Payloads) ->
    Ids = lists:seq(1, length(Payloads)),
    lists:foreach(fun({Id, Payload}) ->
                          ok = publish(Sock, Id, Topic, Payload, 1)
                  end, lists:zip(Ids, Payloads)),
    Acked = [begin #puback{packet_id = Id} = recv(Sock), Id end
             || _ <- Ids],
    true = lists:sort(Acked) =:= Ids,
    ok.

close_sock(Sock) ->
    gen_tcp:close(Sock).

%% A deliberate close must not produce bytes. Anything that arrives here is a
%% protocol promise being broken, so the value is reported, not discarded.
expect_silence(Sock, Ms) ->
    case recv_or(Sock, Ms) of
        timeout -> ok;
        Other -> ct:fail({expected_silence, Other})
    end.

%% The over-cap QoS1 PUBLISH path must close without a PUBACK. Reading after the
%% publish is what proves the absence: an `{ok, Bytes}' here would be the ack
%% this posture forbids.
expect_closed(Sock) ->
    case gen_tcp:recv(Sock, 0, 5000) of
        {error, closed} -> ok;
        {error, econnreset} -> ok;
        {ok, Bytes} -> ct:fail({unexpected_bytes_before_close, Bytes});
        Other -> ct:fail({expected_close, Other})
    end.

%%%_* Frame reader (accumulate-and-drain) --------------------------------------

recv(Sock) ->
    recv(Sock, 5000).

recv(Sock, Timeout) ->
    case recv_or(Sock, Timeout) of
        timeout -> exit({s1_recv_timeout, Timeout, buf(Sock)});
        {error, closed} -> exit({s1_recv_closed, buf(Sock)});
        Frame -> Frame
    end.

recv_or(Sock, Timeout) ->
    Buf = buf(Sock),
    case (catch mqtt_frame:decode(Buf)) of
        {ok, Frame, Rest} ->
            set_buf(Sock, Rest),
            Frame;
        {more, _} ->
            case gen_tcp:recv(Sock, 0, Timeout) of
                {ok, Data} ->
                    set_buf(Sock, <<Buf/binary, Data/binary>>),
                    recv_or(Sock, Timeout);
                {error, timeout} -> timeout;
                {error, Reason} -> {error, Reason}
            end;
        {error, Reason} ->
            exit({s1_recv_malformed, Reason, Buf});
        {'EXIT', Reason} ->
            exit({s1_recv_crashed, Reason, Buf})
    end.

buf(Sock) ->
    case erlang:get({s1_buf, Sock}) of
        undefined -> <<>>;
        Buf -> Buf
    end.

set_buf(Sock, Buf) ->
    erlang:put({s1_buf, Sock}, Buf),
    ok.

%%%_* Assertions that poll the observable state ---------------------------------

route_partition(Topic) ->
    {ok, Env} = broker_router:env(),
    {ok, [{Partition, _Refs} | _]} = broker_router_hash:route(Topic, #{}, Env),
    Partition.

wait_offline(ClientId) ->
    wait_until(fun() ->
                       case broker_subscription:session(ClientId) of
                           {ok, #{online := false}} -> true;
                           _ -> false
                       end
               end, {session_still_online, ClientId}).

wait_cursor_after(SubRef, Partition, Anchor) ->
    wait_until(fun() ->
                       case broker_subscription:cursor(SubRef, Partition) of
                           {ok, Offset} -> Offset > Anchor;
                           _ -> false
                       end
               end, {cursor_stuck, SubRef, Partition, Anchor}).

wait_cursor_at_least(SubRef, Partition, Target) ->
    wait_until(fun() ->
                       case broker_subscription:cursor(SubRef, Partition) of
                           {ok, Offset} -> Offset >= Target;
                           _ -> false
                       end
               end, {cursor_below, SubRef, Partition, Target}).

wait_committed(Partition, Target) ->
    wait_until(fun() ->
                       case broker_queue:watermarks(Partition) of
                           {ok, Committed, _High} -> Committed >= Target;
                           _ -> false
                       end
               end, {committed_below, Partition, Target}).

wait_until(Fun, What) ->
    wait_until(Fun, What, ?POLL_BUDGET_MS).

wait_until(Fun, What, Budget) ->
    case Fun() of
        true ->
            ok;
        _ when Budget =< 0 ->
            ct:fail({timeout, What});
        _ ->
            timer:sleep(?POLL_MS),
            wait_until(Fun, What, Budget - ?POLL_MS)
    end.
