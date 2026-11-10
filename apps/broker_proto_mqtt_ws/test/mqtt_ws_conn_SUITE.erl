%% @doc MQTT over WebSocket: real MQTT packets inside real RFC 6455 frames, over
%% real sockets, through the real ranch listener.
%%
%% The suite's client is deliberately byte-level (`gen_tcp'/`ssl' plus
%% `ws_frame'), because the properties under test are precisely the ones a
%% client library hides: which subprotocol got negotiated, whether the transport
%% tolerates a packet split across frames, what happens to a text frame. The
%% third-party evidence for this protocol lives in
%% `test/protocol-compatibility/mqtt_ws/' (paho-mqtt); this suite pins the wire
%% contract and the refusal paths.
%%
%% What is asserted is MQTT-level wherever it can be (a CONNACK, a SUBACK, a
%% PUBACK), not "a process was started": the transport's whole job is to be
%% invisible to `mqtt_conn_fsm', and an assertion that cannot see the FSM's
%% behaviour cannot prove that.
-module(mqtt_ws_conn_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("mqtt_frame.hrl").

-define(N, 2).
-define(REF, {mqtt_ws, 0}).
-define(POLL_MS, 25).
-define(POLL_BUDGET_MS, 5000).

all() ->
    [subprotocol_mqtt_is_negotiated,
     subprotocol_mqttv311_is_negotiated,
     the_server_preference_wins_over_the_client_order,
     missing_subprotocol_is_refused,
     unknown_subprotocol_is_refused,
     connect_returns_connack,
     publish_qos1_roundtrip_and_ack,
     publish_qos2_roundtrip_and_ack,
     a_retained_publish_reaches_a_late_subscriber,
     packet_split_across_frames_is_reassembled,
     several_packets_in_one_frame_are_all_handled,
     ws_ping_is_answered_with_pong,
     a_text_frame_is_closed_with_1003,
     a_client_close_frame_ends_the_session,
     an_oversized_frame_closes_with_1002,
     any_path_is_accepted,
     will_is_published_when_the_socket_drops,
     wss_connect_returns_connack,
     a_plain_tcp_client_on_a_wss_port_is_refused].

%%%_* Fixtures --------------------------------------------------------------

init_per_suite(Config) ->
    _ = application:load(broker_storage),
    _ = application:load(broker_router),
    _ = application:load(broker_queue),
    _ = application:load(broker_subscription),
    _ = application:load(broker_retain),
    _ = application:load(broker_event),
    _ = application:load(broker_security),
    _ = application:load(broker_proto_mqtt),
    _ = application:load(broker_proto_mqtt_ws),
    Config.

end_per_suite(_Config) ->
    stop_all(),
    ok.

init_per_testcase(Case, Config0) ->
    stop_all(),
    %% Snapshot the ORIGINAL tls env and thread it through Config so
    %% `end_per_testcase' restores exactly that. Reading it at teardown instead
    %% would capture whatever the wss case left behind (`enabled'), leaking TLS
    %% into every suite that runs after this one in the shared CT node.
    SavedTls = saved_tls(),
    restore_tls(SavedTls),
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
    %% The MQTT engine's own knobs: this suite reuses `mqtt_conn_fsm' verbatim,
    %% so it boots it with the application's defaults, spelled out.
    ok = application:set_env(broker_proto_mqtt, pull_page_limit, 32),
    ok = application:set_env(broker_proto_mqtt, max_inflight, 64),
    ok = application:set_env(broker_proto_mqtt, pull_poll_interval_ms, 100),
    ok = application:set_env(broker_proto_mqtt, max_keepalive_ms, 60000),
    ok = application:set_env(broker_proto_mqtt, mqtt_max_qos, 2),
    ok = application:set_env(broker_proto_mqtt, max_qos2_state_per_session, 100),
    %% An ephemeral port, a small frame cap so the oversized-frame case is about
    %% a few kilobytes rather than 16 MiB, and the release's subprotocol list.
    ok = application:set_env(broker_proto_mqtt_ws, listeners,
                             [#{port => 0, num_acceptors => 4, max_connections => 128}]),
    ok = application:set_env(broker_proto_mqtt_ws, subprotocols,
                             [<<"mqtt">>, <<"mqttv3.1">>, <<"mqttv3.1.1">>]),
    ok = application:set_env(broker_proto_mqtt_ws, max_frame_bytes, 4096),
    {Cert, Key} = self_signed(),
    {ok, _Started} = application:ensure_all_started(broker_storage),
    ok = create_partitions(),
    Config = [{data_dir, DataDir}, {certfile, Cert}, {keyfile, Key},
              {saved_tls, SavedTls} | Config0],
    ok = case_env(Case, Config),
    {ok, _Ws} = application:ensure_all_started(broker_proto_mqtt_ws),
    [{port, listener_port()} | Config].

end_per_testcase(_Case, Config) ->
    kill_conns(),
    stop_all(),
    restore_tls(?config(saved_tls, Config)),
    ok.

%% Which cases need a posture other than the suite default, in one place.
%% TLS has to be on *before* the listener boots: `broker_security_tls:
%% transport/0' is read once, by the supervisor's child spec.
case_env(wss_connect_returns_connack, Config) ->
    enable_tls(Config);
case_env(a_plain_tcp_client_on_a_wss_port_is_refused, Config) ->
    enable_tls(Config);
case_env(_Case, _Config) ->
    ok.

create_partitions() ->
    lists:foreach(
      fun(P) ->
              case broker_queue:create_partition(P) of
                  ok -> ok;
                  {error, {already_exists, P}} -> ok;
                  {error, Other} -> exit({create_partition, P, Other})
              end
      end, broker_queue:partition_ids()).

conn_count() ->
    case whereis(mqtt_conn_sup) of
        undefined -> 0;
        Sup -> length(supervisor:which_children(Sup))
    end.

drain_until_closed(S) ->
    case gen_tcp:recv(S, 0, ?POLL_BUDGET_MS) of
        {ok, _Ignored} -> drain_until_closed(S);
        {error, closed} -> ok;
        {error, Other} -> exit({unexpected_recv_error, Other})
    end.

wait_until(Fun) -> wait_until(Fun, ?POLL_BUDGET_MS).

wait_until(_Fun, 0) -> exit(wait_until_timeout);
wait_until(Fun, Left) ->
    case Fun() of
        true -> ok;
        false -> timer:sleep(?POLL_MS), wait_until(Fun, Left - ?POLL_MS)
    end.

listener_port() ->
    case ranch:get_port(?REF) of
        Port when is_integer(Port) -> Port;
        Other -> exit({listener_port_unavailable, Other})
    end.

stop_all() ->
    _ = application:stop(broker_proto_mqtt_ws),
    %% The MQTT engine is a dependency of the WS app, so it outlives it; stopping
    %% it here is what makes the next case's `ensure_all_started' build a fresh
    %% `mqtt_conn_sup'. `mqtt_conn_SUITE' does the same.
    _ = application:stop(broker_proto_mqtt),
    _ = application:stop(broker_retain),
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

%%%_* Cases: the opening handshake -------------------------------------------

subprotocol_mqtt_is_negotiated(Config) ->
    {ok, S, <<"mqtt">>} = handshake(Config, <<"/mqtt">>, [<<"mqtt">>]),
    #connack{return_code = 0} = request(S, connect_frame(<<"ws-mqtt">>)),
    close(S).

subprotocol_mqttv311_is_negotiated(Config) ->
    {ok, S, <<"mqttv3.1.1">>} =
        handshake(Config, <<"/mqtt">>, [<<"mqttv3.1.1">>]),
    #connack{return_code = 0} = request(S, connect_frame(<<"ws-311">>)),
    close(S).

%% The client lists its preference; RFC 6455 lets the server pick from what it
%% offers. MQTT 5.0 §6 names `mqtt', so that is what we choose even when the
%% client put an older spelling first.
the_server_preference_wins_over_the_client_order(Config) ->
    {ok, S, <<"mqtt">>} =
        handshake(Config, <<"/mqtt">>, [<<"mqttv3.1">>, <<"mqtt">>]),
    close(S).

%% No `Sec-WebSocket-Protocol' at all: an MQTT client that forgot it must not be
%% upgraded -- there is no way to tell apart a browser's arbitrary WebSocket.
missing_subprotocol_is_refused(Config) ->
    {error, Response} = raw_handshake(Config, <<"/mqtt">>, []),
    <<"HTTP/1.1 400", _/binary>> = Response.

unknown_subprotocol_is_refused(Config) ->
    {error, Response} = raw_handshake(Config, <<"/mqtt">>, [<<"chat">>]),
    <<"HTTP/1.1 400", _/binary>> = Response.

%%%_* Cases: MQTT over the frames --------------------------------------------

connect_returns_connack(Config) ->
    S = open(Config),
    #connack{session_present = 0, return_code = 0} =
        request(S, connect_frame(<<"ws-c1">>)),
    close(S).

publish_qos1_roundtrip_and_ack(Config) ->
    Sub = open(Config),
    Pub = open(Config),
    #connack{return_code = 0} = request(Sub, connect_frame(<<"ws-sub1">>)),
    #connack{return_code = 0} = request(Pub, connect_frame(<<"ws-pub1">>)),
    #suback{return_codes = [1]} =
        request(Sub, #subscribe{packet_id = 1, topics = [{<<"ws/t">>, 1}]}),
    #puback{packet_id = 2} =
        request(Pub, #publish{qos = 1, topic = <<"ws/t">>, packet_id = 2,
                              payload = <<"over-ws">>}),
    #publish{topic = <<"ws/t">>, payload = <<"over-ws">>, qos = 1,
             packet_id = DeliveryId} = recv(Sub),
    send_only(Sub, #puback{packet_id = DeliveryId}),
    close(Sub),
    close(Pub).

publish_qos2_roundtrip_and_ack(Config) ->
    Sub = open(Config),
    Pub = open(Config),
    #connack{return_code = 0} = request(Sub, connect_frame(<<"ws-sub2">>)),
    #connack{return_code = 0} = request(Pub, connect_frame(<<"ws-pub2">>)),
    #suback{return_codes = [2]} =
        request(Sub, #subscribe{packet_id = 1, topics = [{<<"ws/q2">>, 2}]}),
    #pubrec{packet_id = 7} =
        request(Pub, #publish{qos = 2, topic = <<"ws/q2">>, packet_id = 7,
                              payload = <<"exactly-once">>}),
    #pubcomp{packet_id = 7} = request(Pub, #pubrel{packet_id = 7}),
    #publish{topic = <<"ws/q2">>, payload = <<"exactly-once">>, qos = 2,
             packet_id = D1} = recv(Sub),
    %% The subscriber's PUBREC is answered by PUBREL, not by PUBREC.
    #pubrel{packet_id = D1} = request(Sub, #pubrec{packet_id = D1}),
    send_only(Sub, #pubcomp{packet_id = D1}),
    close(Sub),
    close(Pub).

a_retained_publish_reaches_a_late_subscriber(Config) ->
    Pub = open(Config),
    #connack{return_code = 0} = request(Pub, connect_frame(<<"ws-rpub">>)),
    #puback{packet_id = 1} =
        request(Pub, #publish{qos = 1, retain = 1, topic = <<"ws/retained">>,
                              packet_id = 1, payload = <<"kept">>}),
    Late = open(Config),
    #connack{return_code = 0} = request(Late, connect_frame(<<"ws-late">>)),
    #suback{return_codes = [1]} =
        request(Late, #subscribe{packet_id = 2, topics = [{<<"ws/retained">>, 1}]}),
    #publish{topic = <<"ws/retained">>, payload = <<"kept">>, retain = 1,
             packet_id = Rd} = recv(Late),
    send_only(Late, #puback{packet_id = Rd}),
    close(Late),
    close(Pub).

%% The whole point of a byte-stream abstraction behind the frames: MQTT packets
%% and WebSocket frames are not aligned (design §7.1). A packet split over two
%% frames must be invisible to the engine.
packet_split_across_frames_is_reassembled(Config) ->
    S = open(Config),
    Bin = mqtt_frame:encode(connect_frame(<<"ws-split">>)),
    {Head, Tail} = split_in_half(Bin),
    send_frame(S, 2, Head),
    send_frame(S, 2, Tail),
    #connack{return_code = 0} = recv(S),
    close(S).

%% The other half of the same property: one frame, several packets -- and the
%% second one is processed only if the first one's CONNECT really moved the
%% machine out of `awaiting_connect'. A SUBSCRIBE is the sharpest probe for
%% that: in the wrong state it is refused, not answered with a SUBACK.
several_packets_in_one_frame_are_all_handled(Config) ->
    S = open(Config),
    Bin = iolist_to_binary([mqtt_frame:encode(connect_frame(<<"ws-multi">>)),
                            mqtt_frame:encode(#subscribe{packet_id = 1,
                                                         topics = [{<<"ws/multi">>, 1}]})]),
    send_frame(S, 2, Bin),
    #connack{return_code = 0} = recv(S),
    #suback{return_codes = [1]} = recv(S),
    close(S).

%%%_* Cases: WebSocket control traffic ---------------------------------------

%% ping/pong is RFC 6455 plumbing, independent of MQTT keepalive: an MQTT client
%% library over WS never sees it, and it must work before CONNECT as well.
ws_ping_is_answered_with_pong(Config) ->
    S = open(Config),
    send_frame(S, 9, <<"are-you-there">>),
    #{opcode := 10, payload := <<"are-you-there">>} = recv_frame(S),
    close(S).

%% Only binary frames carry MQTT. A text frame is a protocol error, and the
%% answer is close 1003 ("cannot accept data type"), not silent ignoring.
a_text_frame_is_closed_with_1003(Config) ->
    S = open(Config),
    _ = gen_tcp:send(S, ws_frame:encode(1, <<"MQTT">>, true)),
    #{opcode := 8, payload := Payload} = recv_frame(S),
    1003 = ws_frame:close_code(Payload),
    ok.

%% A client that says goodbye gets a close frame back and the socket is gone --
%% the session must not linger holding a subscription.
a_client_close_frame_ends_the_session(Config) ->
    S = open(Config),
    #connack{return_code = 0} = request(S, connect_frame(<<"ws-bye">>)),
    _ = gen_tcp:send(S, ws_frame:encode(8, ws_frame:close_payload(1000, <<>>), true)),
    case recv_frame(S) of
        #{opcode := 8} ->
            ok;
        #{opcode := Op} ->
            %% A frame already in flight is possible; the close still follows.
            ct:pal("frame before close: ~p", [Op]),
            #{opcode := 8} = recv_frame(S)
    end,
    ?assertEqual({error, closed}, gen_tcp:recv(S, 0, ?POLL_BUDGET_MS)),
    ok.

%% `max_frame_bytes' bounds a frame, not a packet. A frame over it is a decode
%% error, and `ws_frame' documents the answer to every one of those as 1002 --
%% never a truncated read, never a silent skip.
an_oversized_frame_closes_with_1002(Config) ->
    S = open(Config),
    _ = gen_tcp:send(S, ws_frame:encode(2, binary:copy(<<"x">>, 8192), true)),
    #{opcode := 8, payload := Payload} = recv_frame(S),
    1002 = ws_frame:close_code(Payload),
    ok.

%% The URL path is the deployment's business, not the protocol's: MQTT over
%% WebSocket fixes the subprotocol, not the path.
any_path_is_accepted(Config) ->
    lists:foreach(
      fun(Path) ->
              S = open(Config, Path),
              #connack{return_code = 0} =
                  request(S, connect_frame(<<"ws-path">>)),
              close(S)
      end, [<<"/">>, <<"/ws">>, <<"/mqtt">>, <<"/deep/nested/path">>]).

%% The socket dies without a WebSocket close frame. That is exactly the case a
%% Will exists for, and it is also the test that the WS layer's `terminate/2'
%% reaches the engine.
will_is_published_when_the_socket_drops(Config) ->
    Watcher = open(Config),
    #connack{return_code = 0} = request(Watcher, connect_frame(<<"ws-watch">>)),
    #suback{return_codes = [0]} =
        request(Watcher, #subscribe{packet_id = 1, topics = [{<<"ws/will">>, 0}]}),
    Victim = open(Config),
    #connack{return_code = 0} =
        request(Victim, connect_frame(#{client_id => <<"ws-dying">>,
                                        will_topic => <<"ws/will">>,
                                        will_payload => <<"gone">>})),
    ok = gen_tcp:close(Victim),
    #publish{topic = <<"ws/will">>, payload = <<"gone">>} = recv(Watcher),
    close(Watcher).

%% A plain TCP client on the wss port can never complete the TLS handshake.
%% That failure belongs to the peer, so it must not surface as a broker crash:
%% the WS process stops `normal', hands the detail to the engine, and the engine
%% shuts the connection down. Observable: nothing is left under `mqtt_conn_sup'.
a_plain_tcp_client_on_a_wss_port_is_refused(Config) ->
    {ok, S} = gen_tcp:connect({127, 0, 0, 1}, ?config(port, Config),
                              [binary, {active, false}], 5000),
    ok = gen_tcp:send(S, <<"GET /mqtt HTTP/1.1\r\nHost: localhost\r\n\r\n">>),
    %% The server answers with a TLS alert record and then closes; a plain
    %% client cannot read it as HTTP, which is the point.
    ok = drain_until_closed(S),
    ok = wait_until(fun() -> conn_count() =:= 0 end).

%%%_* Cases: TLS -------------------------------------------------------------

%% wss, end to end: `broker_security_tls:transport/0' picks `ranch_ssl', and
%% `mqtt_ws_conn' completes the TLS handshake before it reads a single byte of
%% HTTP. Skipping that call is a real defect the broker's *other* WS app still
%% has (S8-B plan, Task 4).
wss_connect_returns_connack(Config) ->
    {ok, S, <<"mqtt">>} = handshake(Config, <<"/mqtt">>, [<<"mqtt">>], tls),
    #connack{return_code = 0} = request_tls(S, connect_frame(<<"ws-tls">>)),
    _ = ssl:close(S),
    ok.

%%%_* WebSocket client -------------------------------------------------------

open(Config) ->
    open(Config, <<"/mqtt">>).

open(Config, Path) ->
    {ok, S, _Selected} = handshake(Config, Path, [<<"mqtt">>]),
    S.

handshake(Config, Path, Subprotocols) ->
    handshake(Config, Path, Subprotocols, tcp).

handshake(Config, Path, Subprotocols, Mode) ->
    Open = case Mode of
               tcp -> fun() -> gen_tcp:connect({127, 0, 0, 1}, ?config(port, Config),
                                               [binary, {active, false},
                                                {nodelay, true}], 5000) end;
               tls -> fun() -> ssl:connect("localhost", ?config(port, Config),
                                           [binary, {active, false},
                                            {verify, verify_none}], 5000) end
           end,
    {ok, S} = Open(),
    case raw_handshake_on(S, Mode, Path, Subprotocols) of
        {ok, Selected} -> {ok, S, Selected};
        {error, Response} -> {error, Response}
    end.

raw_handshake(Config, Path, Subprotocols) ->
    {ok, S} = gen_tcp:connect({127, 0, 0, 1}, ?config(port, Config),
                              [binary, {active, false}], 5000),
    Result = raw_handshake_on(S, tcp, Path, Subprotocols),
    _ = gen_tcp:close(S),
    Result.

raw_handshake_on(S, Mode, Path, Subprotocols) ->
    Key = base64:encode(crypto:strong_rand_bytes(16)),
    ProtoHeader = case Subprotocols of
                      [] -> <<>>;
                      _ -> [<<"Sec-WebSocket-Protocol: ">>,
                            lists:join(<<", ">>, Subprotocols), <<"\r\n">>]
                  end,
    Request = [<<"GET ">>, Path, <<" HTTP/1.1\r\nHost: localhost\r\n"
                                   "Upgrade: websocket\r\nConnection: Upgrade\r\n"
                                   "Sec-WebSocket-Key: ">>, Key, <<"\r\n">>,
               ProtoHeader, <<"Sec-WebSocket-Version: 13\r\n\r\n">>],
    ok = send_raw(Mode, S, Request),
    {ok, Response} = recv_until(Mode, S, <<"\r\n\r\n">>, <<>>),
    case Response of
        <<"HTTP/1.1 101", _/binary>> ->
            {ok, response_subprotocol(Response)};
        _ ->
            {error, Response}
    end.

response_subprotocol(Response) ->
    Lower = string:lowercase(Response),
    case binary:split(Lower, <<"sec-websocket-protocol: ">>) of
        [_Head, Tail] ->
            [Value | _] = binary:split(Tail, <<"\r\n">>),
            Value;
        _ ->
            undefined
    end.

%%%_* MQTT client ------------------------------------------------------------

connect_frame(ClientId) when is_binary(ClientId) ->
    connect_frame(#{client_id => ClientId});

connect_frame(#{client_id := ClientId} = Knobs) ->
    #connect{client_id = ClientId,
             clean_session = 1,
             keep_alive = maps:get(keep_alive, Knobs, 0),
             will_flag = case maps:get(will_topic, Knobs, undefined) of
                             undefined -> 0;
                             _ -> 1
                         end,
             will_topic = maps:get(will_topic, Knobs, undefined),
             will_payload = maps:get(will_payload, Knobs, undefined),
             will_qos = maps:get(will_qos, Knobs, 0),
             will_retain = maps:get(will_retain, Knobs, 0)}.

request(S, Frame) ->
    send_frame(S, 2, mqtt_frame:encode(Frame)),
    recv(S).

%% For the packets MQTT defines no answer to (PUBACK, PUBCOMP): sending is the
%% whole assertion, and `recv/1' would block until the budget expired.
send_only(S, Frame) ->
    send_frame(S, 2, mqtt_frame:encode(Frame)).

request_tls(S, Frame) ->
    ok = ssl:send(S, ws_frame:encode(2, mqtt_frame:encode(Frame), true)),
    #{opcode := 2, payload := Payload} = recv_frame(S, tls),
    decode_mqtt(Payload).

send_frame(S, Op, Payload) ->
    ok = gen_tcp:send(S, ws_frame:encode(Op, Payload, true)),
    ok.

recv(S) ->
    #{payload := Payload} = recv_frame(S),
    decode_mqtt(Payload).

decode_mqtt(Bin) ->
    case mqtt_frame:decode(Bin) of
        {ok, Frame, <<>>} -> Frame;
        Other -> exit({unexpected_mqtt_bytes, Other, Bin})
    end.

recv_frame(S) ->
    recv_frame(S, tcp).

%% Read exactly one frame, **byte-exactly**: the client reads the 2-byte header,
%% then the extended length, then the mask key, then exactly `Len' payload bytes.
%%
%% Reading with `recv(S, 0)' and then decoding would be simpler, but the server
%% may write two replies (`several_packets_in_one_frame_are_all_handled' gets a
%% CONNACK and a SUBACK) into one TCP segment; a decode-then-discard reader
%% loses the second frame and the next read blocks forever. This is the same
%% shape `ws_conn_SUITE' uses, for the same reason.
recv_frame(S, Mode) ->
    {ok, <<Fin:1, Rsv:3, Op:4, Mask:1, Len0:7>>} = recv_exact(Mode, S, 2),
    0 = Rsv,
    Len = case Len0 of
              126 -> {ok, <<L:16>>} = recv_exact(Mode, S, 2), L;
              127 -> {ok, <<L:64>>} = recv_exact(Mode, S, 8), L;
              _ -> Len0
          end,
    Payload = payload(Mode, S, Mask, Len),
    #{fin => Fin =:= 1, masked => Mask =:= 1, opcode => Op, payload => Payload}.

%% `gen_tcp:recv(S, 0)' means "whatever is available", which would swallow the
%% next frame's bytes; an empty payload is therefore not a read at all.
payload(_Mode, _S, _Mask, 0) ->
    <<>>;
payload(Mode, S, 1, Len) ->
    {ok, Key} = recv_exact(Mode, S, 4),
    {ok, Masked} = recv_exact(Mode, S, Len),
    unmask(Key, Masked);
payload(Mode, S, 0, Len) ->
    {ok, Plain} = recv_exact(Mode, S, Len),
    Plain.

recv_exact(tcp, S, N) ->
    case gen_tcp:recv(S, N, ?POLL_BUDGET_MS) of
        {ok, Bin} -> {ok, Bin};
        Other -> exit({recv_frame_failed, Other, N})
    end;
recv_exact(tls, S, N) ->
    case ssl:recv(S, N, ?POLL_BUDGET_MS) of
        {ok, Bin} -> {ok, Bin};
        Other -> exit({recv_frame_failed, Other, N})
    end.

unmask(Key, Payload) ->
    Size = byte_size(Payload),
    Stream = binary:copy(Key, (Size div 4) + 1),
    crypto:exor(Payload, binary:part(Stream, 0, Size)).

%% A clean goodbye from the client: close frame, then the socket. Malformed or
%% abrupt teardown is a case's own business (`a_text_frame_is_closed_with_1003',
%% `will_is_published_when_the_socket_drops').
close(S) ->
    catch gen_tcp:send(S, ws_frame:encode(8, ws_frame:close_payload(1000, <<>>), true)),
    catch gen_tcp:close(S),
    ok.

split_in_half(Bin) ->
    Half = byte_size(Bin) div 2,
    {binary:part(Bin, 0, Half), binary:part(Bin, Half, byte_size(Bin) - Half)}.

send_raw(tcp, S, IoData) -> gen_tcp:send(S, IoData);
send_raw(tls, S, IoData) -> ssl:send(S, IoData).

recv_until(Mode, S, Needle, Acc) ->
    case binary:match(Acc, Needle) of
        nomatch ->
            Got = case Mode of
                      tcp -> gen_tcp:recv(S, 0, 5000);
                      tls -> ssl:recv(S, 0, 5000)
                  end,
            case Got of
                {ok, Data} -> recv_until(Mode, S, Needle, <<Acc/binary, Data/binary>>);
                Other -> Other
            end;
        _ ->
            {ok, Acc}
    end.

%%%_* TLS configuration ------------------------------------------------------

enable_tls(Config) ->
    application:set_env(broker_security, tls,
                        #{enabled => true,
                          certfile => ?config(certfile, Config),
                          keyfile => ?config(keyfile, Config),
                          verify => verify_none}).

saved_tls() ->
    application:get_env(broker_security, tls, undefined).

restore_tls(undefined) ->
    application:unset_env(broker_security, tls);
restore_tls(Value) ->
    application:set_env(broker_security, tls, Value).

%% Generated per run so nothing in the repo is a checked-in secret. The openssl
%% shape is the one `mqtt_tls_SUITE' uses, so both fail together if openssl
%% changes its defaults.
self_signed() ->
    Dir = filename:join(["/tmp/opencode", "mqtt_ws_test"]),
    _ = file:del_dir_r(Dir),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    Cert = filename:join(Dir, "cert.pem"),
    Key = filename:join(Dir, "key.pem"),
    Cmd = io_lib:format(
            "openssl req -x509 -newkey rsa:2048 -nodes -days 1 "
            "-subj /CN=localhost -addext subjectAltName=DNS:localhost "
            "-addext extendedKeyUsage=serverAuth "
            "-keyout ~s -out ~s 2>/dev/null",
            [Key, Cert]),
    _ = os:cmd(lists:flatten(Cmd)),
    case {filelib:is_regular(Cert), filelib:is_regular(Key)} of
        {true, true} -> {Cert, Key};
        _ -> exit({openssl_failed, Cert, Key})
    end.
