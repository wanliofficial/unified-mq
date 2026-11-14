%% @doc The WebRTC signaling connection, end to end over a real socket.
%%
%% The client is byte-level on purpose (`gen_tcp' + `ws_frame'): the
%% properties under test are exactly the ones a client library hides --
%% subprotocol negotiation, text vs binary framing, the two keepalive
%% protocols, and the close codes. Third-party browser evidence lives in
%% `test/protocol-compatibility/s10/' and lands in S10-D; this suite pins the
%% wire contract and every refusal path S10-A promises.
%%
%% ADR-028 note: this suite deliberately never touches `broker_queue' or
%% `broker_router'. If a future change makes the connection need them, the
%% suite's fixture (which starts only broker_security) stops working, which is
%% the point.
-module(webrtc_conn_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(REF, {webrtc, 0}).
-define(POLL_MS, 25).
-define(POLL_BUDGET_MS, 5000).
-define(SUBPROTOCOL, <<"webrtc.v1">>).

all() ->
    [{group, handshake},
     {group, auth},
     {group, frames},
     {group, timeouts},
     {group, tls}].

groups() ->
    [{handshake, [sequence],
      [subprotocol_is_negotiated,
       missing_subprotocol_is_refused,
       unknown_subprotocol_is_refused,
       server_preference_wins_over_client_order]},
     {auth, [sequence],
      [authenticate_returns_a_minted_peer_id,
       a_second_connection_gets_a_different_peer_id,
       authenticate_rejects_a_bad_password,
       authenticate_rejects_an_unknown_user,
       authenticate_rejects_a_missing_credential_field,
       a_non_authenticate_message_pre_auth_is_unauthorized,
       a_non_json_text_frame_pre_auth_is_unauthorized,
       auth_disabled_goes_free_at_handshake_time]},
     {frames, [sequence],
      [text_ping_is_answered_with_text_pong,
       native_ping_is_answered_with_native_pong,
       both_ping_protocols_coexist,
       a_binary_frame_is_closed_with_1003,
       a_binary_frame_is_ignored_when_configured,
       a_message_without_to_is_invalid_message,
       a_message_with_an_unknown_to_is_invalid_message,
       a_deeply_nested_json_frame_is_invalid_message,
       an_invalid_utf8_text_frame_is_invalid_message,
       a_client_close_frame_ends_the_session,
       a_client_close_frame_is_echoed,
       a_reserved_close_code_is_normalized_to_1000,
       an_oversized_frame_closes_with_1009,
       an_oversized_to_field_closes_with_1009,
       a_reserved_bit_closes_with_1002,
       a_fragmented_authenticate_frame_is_reassembled,
       a_continuation_without_a_start_frame_closes_with_1002]},
     {timeouts, [sequence],
      [the_auth_window_closes_an_unauthenticated_connection,
       an_idle_connection_is_closed_with_1001,
       a_text_ping_keeps_an_authenticated_connection_alive,
       an_infinite_idle_timeout_never_closes_the_connection]},
     {tls, [sequence],
      [a_wss_connection_authenticates_the_same_way]}].

%%%_* Fixtures --------------------------------------------------------------

init_per_suite(Config) ->
    _ = application:load(broker_security),
    _ = application:load(broker_proto_webrtc),
    {Cert, Key} = self_signed(),
    [{certfile, Cert}, {keyfile, Key} | Config].

end_per_suite(_Config) ->
    _ = application:stop(broker_proto_webrtc),
    _ = application:stop(broker_security),
    ok.

init_per_testcase(Case, Config0) ->
    stop_app(),
    SavedTls = saved_tls(),
    restore_tls(undefined),
    %% One user, no ACL restrictions: the connection layer is what is under
    %% test here. ACL enforcement is S10-B.
    ok = application:set_env(broker_security, users,
                             [{<<"alice">>, #{password => <<"s3cr3t!">>,
                                              tenant_id => <<"t0">>}}]),
    ok = application:set_env(broker_security, acl, allow),
    %% ADR-028 §D5: this app's own switch. `broker_security.require_auth' is a
    %% list of protocol names and is deliberately left alone here.
    ok = application:set_env(broker_proto_webrtc, require_auth, true),
    ok = application:set_env(broker_proto_webrtc, listeners,
                             [#{port => 0, num_acceptors => 4,
                                max_connections => 64}]),
    ok = application:set_env(broker_proto_webrtc, subprotocols, [?SUBPROTOCOL]),
    ok = application:set_env(broker_proto_webrtc, auth_timeout_ms, 5000),
    ok = application:set_env(broker_proto_webrtc, idle_timeout_ms, 60000),
    ok = application:set_env(broker_proto_webrtc, max_frame_bytes, 4096),
    ok = application:set_env(broker_proto_webrtc, binary_frames, reject),
    %% Every case here connects to the same `/websocket/room1', and a few of
    %% them keep more than two sockets open at once. Room capacity is
    %% `webrtc_room_SUITE''s subject, so the ceiling is raised here rather than
    %% letting a full room masquerade as a connection-layer failure.
    ok = application:set_env(broker_proto_webrtc, max_peers_per_room, 32),
    ok = case_env(Case, Config0),
    {ok, _} = application:ensure_all_started(broker_security),
    {ok, _} = application:ensure_all_started(broker_proto_webrtc),
    [{saved_tls, SavedTls}, {port, listener_port()} | Config0].

end_per_testcase(_Case, Config) ->
    stop_app(),
    restore_tls(?config(saved_tls, Config)),
    ok.

case_env(a_binary_frame_is_ignored_when_configured, _Config) ->
    application:set_env(broker_proto_webrtc, binary_frames, ignore);
case_env(an_oversized_frame_closes_with_1009, _Config) ->
    %% Small enough to refuse a big frame, large enough that the
    %% authenticate request itself (~72 bytes) still fits.
    application:set_env(broker_proto_webrtc, max_frame_bytes, 256);
case_env(auth_disabled_goes_free_at_handshake_time, _Config) ->
    application:set_env(broker_proto_webrtc, require_auth, false);
case_env(the_auth_window_closes_an_unauthenticated_connection, _Config) ->
    application:set_env(broker_proto_webrtc, auth_timeout_ms, 200);
case_env(an_idle_connection_is_closed_with_1001, _Config) ->
    application:set_env(broker_proto_webrtc, idle_timeout_ms, 400);
case_env(a_text_ping_keeps_an_authenticated_connection_alive, _Config) ->
    %% Idle window is short; a ping inside it must reset it, and the test
    %% then proves the connection survives past the window.
    application:set_env(broker_proto_webrtc, idle_timeout_ms, 400);
case_env(an_infinite_idle_timeout_never_closes_the_connection, _Config) ->
    application:set_env(broker_proto_webrtc, idle_timeout_ms, infinity);
case_env(a_wss_connection_authenticates_the_same_way, Config) ->
    enable_tls(Config);
case_env(_Case, _Config) ->
    ok.

stop_app() ->
    _ = application:stop(broker_proto_webrtc),
    _ = application:stop(broker_security),
    ok.

listener_port() ->
    case ranch:get_port(?REF) of
        Port when is_integer(Port) -> Port;
        Other -> exit({listener_port_unavailable, Other})
    end.

wait_until(Fun) -> wait_until(Fun, ?POLL_BUDGET_MS).

wait_until(_Fun, 0) -> exit(wait_until_timeout);
wait_until(Fun, Left) ->
    case Fun() of
        true -> ok;
        false -> timer:sleep(?POLL_MS), wait_until(Fun, Left - ?POLL_MS)
    end.

%%%_* Cases: handshake -------------------------------------------------------

subprotocol_is_negotiated(Config) ->
    {ok, S, ?SUBPROTOCOL} = handshake(Config, [?SUBPROTOCOL]),
    close(S).

missing_subprotocol_is_refused(Config) ->
    {error, Response} = raw_handshake(Config, []),
    assert_contains(Response, <<"400 Bad Request">>).

unknown_subprotocol_is_refused(Config) ->
    {error, Response} = raw_handshake(Config, [<<"sip.v2">>]),
    assert_contains(Response, <<"400 Bad Request">>).

server_preference_wins_over_client_order(Config) ->
    %% The client offers ours second; the server must still pick ours, and it
    %% must answer 101 (not 400) because a common protocol exists.
    {ok, S, ?SUBPROTOCOL} = handshake(Config, [<<"sip.v2">>, ?SUBPROTOCOL]),
    close(S).

%%%_* Cases: auth ------------------------------------------------------------

authenticate_returns_a_minted_peer_id(Config) ->
    {ok, S, _} = handshake(Config, [?SUBPROTOCOL]),
    ok = send_text(S, authenticate(<<"alice">>, <<"s3cr3t!">>)),
    #{opcode := 1, payload := P} = recv_frame(S),
    #{<<"event">> := <<"authenticated">>, <<"data">> := Data} = decode(P),
    PeerId = maps:get(<<"peer_id">>, Data),
    %% R-S7: base64(strong_rand_bytes(10)) is 16 characters and is minted by
    %% the server -- the client never sends one.
    ?assertEqual(16, byte_size(PeerId)),
    ?assertNotEqual(nomatch, binary:match(P, <<"authenticated">>)),
    close(S).

a_second_connection_gets_a_different_peer_id(Config) ->
    {ok, S1} = authed(Config),
    Pid1 = authed_peer_id(Config),
    {ok, S2} = authed(Config),
    Pid2 = authed_peer_id(Config),
    ?assertNotEqual(Pid1, Pid2),
    close(S1),
    close(S2).

authed_peer_id(Config) ->
    {ok, S, ?SUBPROTOCOL} = handshake(Config, [?SUBPROTOCOL]),
    ok = send_text(S, authenticate(<<"alice">>, <<"s3cr3t!">>)),
    #{opcode := 1, payload := P} = recv_frame(S),
    #{<<"data">> := #{<<"peer_id">> := Id}} = decode(P),
    close(S),
    Id.

authenticate_rejects_a_bad_password(Config) ->
    {ok, S, _} = handshake(Config, [?SUBPROTOCOL]),
    ok = send_text(S, authenticate(<<"alice">>, <<"wrong">>)),
    #{opcode := 1, payload := P} = recv_frame(S),
    ?assertEqual(#{<<"event">> => <<"unauthorized">>}, decode(P)),
    close(S).

authenticate_rejects_an_unknown_user(Config) ->
    {ok, S, _} = handshake(Config, [?SUBPROTOCOL]),
    ok = send_text(S, authenticate(<<"bob">>, <<"s3cr3t!">>)),
    #{opcode := 1, payload := P} = recv_frame(S),
    ?assertEqual(#{<<"event">> => <<"unauthorized">>}, decode(P)),
    close(S).

authenticate_rejects_a_missing_credential_field(Config) ->
    {ok, S, _} = handshake(Config, [?SUBPROTOCOL]),
    ok = send_text(S, <<"{\"event\":\"authenticate\",\"data\":{\"username\":\"alice\"}}">>),
    #{opcode := 1, payload := P} = recv_frame(S),
    ?assertEqual(#{<<"event">> => <<"unauthorized">>}, decode(P)),
    close(S).

a_non_authenticate_message_pre_auth_is_unauthorized(Config) ->
    {ok, S, _} = handshake(Config, [?SUBPROTOCOL]),
    ok = send_text(S, <<"{\"event\":\"offer\",\"to\":\"someone\",\"data\":{}}">>),
    #{opcode := 1, payload := P} = recv_frame(S),
    ?assertEqual(#{<<"event">> => <<"unauthorized">>}, decode(P)),
    %% ... and the connection is still usable for a real authenticate.
    ok = send_text(S, authenticate(<<"alice">>, <<"s3cr3t!">>)),
    #{opcode := 1, payload := P2} = recv_frame(S),
    ?assertMatch(#{<<"event">> := <<"authenticated">>}, decode(P2)),
    close(S).

a_non_json_text_frame_pre_auth_is_unauthorized(Config) ->
    {ok, S, _} = handshake(Config, [?SUBPROTOCOL]),
    ok = send_text(S, <<"not json at all">>),
    #{opcode := 1, payload := P} = recv_frame(S),
    ?assertEqual(#{<<"event">> => <<"unauthorized">>}, decode(P)),
    close(S).

%% `require_auth => false' is only legal on a loopback bind (validated at
%% boot). When it is used, the connection is `active' the moment the handshake
%% completes: no `authenticate' frame is needed.
%%
%% The server still sends one `authenticated' event, unsolicited, because that
%% is the only place a client can learn its `peer_id' (R-S7: the server mints
%% it, the client may not choose one). The design describes `authenticated' as
%% a *reply* to `authenticate'; with the auth switch off there is no request to
%% reply to, so the event is pushed at handshake time instead.
auth_disabled_goes_free_at_handshake_time(Config) ->
    {ok, S, ?SUBPROTOCOL} = handshake(Config, [?SUBPROTOCOL]),
    #{opcode := 1, payload := AuthP} = recv_frame(S),
    #{<<"event">> := <<"authenticated">>, <<"data">> := #{<<"peer_id">> := Id}} =
        decode(AuthP),
    ?assertEqual(16, byte_size(Id)),
    %% Now the connection behaves like an authenticated one.
    ok = send_text(S, <<"ping">>),
    ?assertEqual({1, <<"pong">>}, frame_of(recv_frame(S))),
    %% ... and a signaling-shaped message gets the S10-A answer, not
    %% `unauthorized'.
    ok = send_text(S, <<"{\"event\":\"offer\",\"to\":\"x\",\"data\":{}}">>),
    #{opcode := 1, payload := P} = recv_frame(S),
    ?assertEqual(#{<<"event">> => <<"invalid_message">>}, decode(P)),
    close(S).

%%%_* Cases: frames ----------------------------------------------------------

text_ping_is_answered_with_text_pong(Config) ->
    {ok, S} = authed(Config),
    ok = send_text(S, <<"ping">>),
    ?assertEqual({1, <<"pong">>}, frame_of(recv_frame(S))),
    close(S).

native_ping_is_answered_with_native_pong(Config) ->
    {ok, S} = authed(Config),
    ok = send_frame(S, 9, <<"probe">>),
    #{opcode := Op, payload := P} = recv_frame(S),
    ?assertEqual(10, Op),
    ?assertEqual(<<"probe">>, P),
    close(S).

both_ping_protocols_coexist(Config) ->
    {ok, S} = authed(Config),
    ok = send_text(S, <<"ping">>),
    ?assertEqual({1, <<"pong">>}, frame_of(recv_frame(S))),
    ok = send_frame(S, 9, <<"x">>),
    #{opcode := Op, payload := P} = recv_frame(S),
    ?assertEqual(10, Op),
    ?assertEqual(<<"x">>, P),
    close(S).

a_binary_frame_is_closed_with_1003(Config) ->
    {ok, S} = authed(Config),
    ok = send_frame(S, 2, <<0, 1, 2, 3>>),
    #{opcode := 8, payload := P} = recv_frame(S),
    ?assertEqual(1003, close_code(P)),
    drain_until_closed(S).

a_binary_frame_is_ignored_when_configured(Config) ->
    {ok, S} = authed(Config),
    ok = send_frame(S, 2, <<0, 1, 2, 3>>),
    %% No close frame: the next thing on the wire is our own probe reply.
    ok = send_text(S, <<"ping">>),
    ?assertEqual({1, <<"pong">>}, frame_of(recv_frame(S))),
    close(S).

a_message_without_to_is_invalid_message(Config) ->
    {ok, S} = authed(Config),
    ok = send_text(S, <<"{\"event\":\"offer\",\"data\":{\"sdp\":\"v=0\"}}">>),
    #{opcode := 1, payload := P} = recv_frame(S),
    ?assertEqual(#{<<"event">> => <<"invalid_message">>}, decode(P)),
    close(S).

a_message_with_an_unknown_to_is_invalid_message(Config) ->
    %% S10-B has a room registry, but the `to' names a peer that does not exist.
    %% The honest answer is `invalid_message', not a silent drop and not a crash.
    {ok, S} = authed(Config),
    ok = send_text(S, <<"{\"event\":\"offer\",\"to\":\"somebody\",\"data\":{}}">>),
    #{opcode := 1, payload := P} = recv_frame(S),
    ?assertEqual(#{<<"event">> => <<"invalid_message">>}, decode(P)),
    close(S).

a_client_close_frame_ends_the_session(Config) ->
    {ok, S} = authed(Config),
    ok = send_frame(S, 8, ws_frame:close_payload(1000, <<>>)),
    drain_until_closed(S).

an_oversized_frame_closes_with_1009(Config) ->
    %% max_frame_bytes is 32 in this case; a 100-byte text frame is refused.
    {ok, S} = authed(Config),
    Big = binary:copy(<<"x">>, 300),
    ok = send_frame(S, 1, Big),
    case recv_frame(S) of
        #{opcode := 8, payload := P} ->
            ?assertEqual(1009, close_code(P));
        Other ->
            exit({expected_close_1009, Other})
    end,
    drain_until_closed(S).

%% S10-D robustness: a text frame holding valid JSON that is deeply nested. The
%% payload stays under `max_frame_bytes' (4096 here: 1000 levels of `[') so the
%% frame is accepted, `json:decode' returns a list, and `decode_json/1' maps a
%% non-object to `not_a_map' -> `invalid_message'. The point is that the decoder
%% does not recurse-overflow on depth -- there is no JSON-depth-specific DoS
%% defence because none is needed: `max_frame_bytes' bounds the input size and
%% the decoder handles the rest. The ping after proves the connection is alive.
a_deeply_nested_json_frame_is_invalid_message(Config) ->
    {ok, S} = authed(Config),
    Deep = deep_array(1000, <<"[">>),
    ?assert(byte_size(Deep) =< 4096),
    ok = send_text(S, Deep),
    #{opcode := 1, payload := P} = recv_frame(S),
    ?assertEqual(#{<<"event">> => <<"invalid_message">>}, decode(P)),
    ok = send_text(S, <<"ping">>),
    #{opcode := 1, payload := Pong} = recv_frame(S),
    ?assertEqual(<<"pong">>, Pong),
    close(S).

%% Invalid UTF-8 in a text frame: `json:decode' throws, `decode_json/1' catches
%% it as `invalid_json' -> `invalid_message'. The connection is not killed by a
%% malformed payload (R-S5 keeps it up for a retry); the ping proves it.
an_invalid_utf8_text_frame_is_invalid_message(Config) ->
    {ok, S} = authed(Config),
    ok = send_frame(S, 1, <<255, 254, 253, 0>>),
    #{opcode := 1, payload := P} = recv_frame(S),
    ?assertEqual(#{<<"event">> => <<"invalid_message">>}, decode(P)),
    ok = send_text(S, <<"ping">>),
    #{opcode := 1, payload := Pong} = recv_frame(S),
    ?assertEqual(<<"pong">>, Pong),
    close(S).

%% The frame-size limit applies to a structured message too: a `to' field large
%% enough to push the whole frame past `max_frame_bytes' (4096) is refused at the
%% frame layer with 1009, before any addressing runs. This is the same size
%% ceiling as `an_oversized_frame_closes_with_1009', reached through a JSON body
%% rather than raw bytes -- the bound is on bytes, not on field count.
an_oversized_to_field_closes_with_1009(Config) ->
    {ok, S} = authed(Config),
    Big = binary:copy(<<"x">>, 8192),
    ok = send_text(S, json:encode(#{event => <<"offer">>, to => Big,
                                    data => #{}})),
    case recv_frame(S) of
        #{opcode := 8, payload := P} ->
            ?assertEqual(1009, close_code(P));
        Other ->
            exit({expected_close_1009, Other})
    end,
    drain_until_closed(S).

a_reserved_bit_closes_with_1002(Config) ->
    %% RSV1 set on a text frame: a protocol violation the codec rejects.
    {ok, S} = authed(Config),
    ok = gen_tcp:send(S, <<16#C1, 0>>),
    case recv_frame(S) of
        #{opcode := 8, payload := P} ->
            ?assertEqual(1002, close_code(P));
        Other ->
            exit({expected_close_1002, Other})
    end,
    drain_until_closed(S).

a_client_close_frame_is_echoed(Config) ->
    %% RFC 6455 §5.5.1: a server that receives a close frame must echo it back
    %% with the same status code before closing the connection.
    {ok, S} = authed(Config),
    ok = send_frame(S, 8, ws_frame:close_payload(1000, <<>>)),
    #{opcode := 8, payload := P} = recv_frame(S),
    ?assertEqual(1000, close_code(P)),
    drain_until_closed(S).

a_reserved_close_code_is_normalized_to_1000(Config) ->
    %% RFC 6455 §7.4.1 reserves status codes 1005, 1006, and 1015; a close frame
    %% with one of these must be answered with 1000 (normal closure).
    {ok, S} = authed(Config),
    ok = send_frame(S, 8, ws_frame:close_payload(1005, <<>>)),
    #{opcode := 8, payload := P} = recv_frame(S),
    ?assertEqual(1000, close_code(P)),
    drain_until_closed(S).

a_fragmented_authenticate_frame_is_reassembled(Config) ->
    %% RFC 6455 §5.4: a message split across multiple frames (opcode 1 with FIN=0,
    %% then opcode 0 with FIN=true) must be reassembled before processing.
    %% `ws_frame:encode/3' always sets FIN=1, so the frames are hand-built here.
    {ok, S, _} = handshake(Config, [?SUBPROTOCOL]),
    Authenticate = iolist_to_binary(authenticate(<<"alice">>, <<"s3cr3t!">>)),
    Size = byte_size(Authenticate),
    Half = Size div 2,
    <<First:Half/binary, Rest/binary>> = Authenticate,
    ok = gen_tcp:send(S, frag_frame(1, First, false)),
    ok = gen_tcp:send(S, frag_frame(0, Rest, true)),
    #{opcode := 1, payload := P} = recv_frame(S),
    #{<<"event">> := <<"authenticated">>} = decode(P),
    close(S).

a_continuation_without_a_start_frame_closes_with_1002(Config) ->
    %% RFC 6455 §5.4: a continuation frame (opcode 0) with no preceding data
    %% frame (opcode 1 or 2) is a protocol violation.
    {ok, S} = authed(Config),
    ok = gen_tcp:send(S, frag_frame(0, <<"orphan">>, true)),
    case recv_frame(S) of
        #{opcode := 8, payload := P} ->
            ?assertEqual(1002, close_code(P));
        Other ->
            exit({expected_close_1002, Other})
    end,
    drain_until_closed(S).

%%%_* Cases: timeouts -------------------------------------------------------

the_auth_window_closes_an_unauthenticated_connection(Config) ->
    {ok, S, _} = handshake(Config, [?SUBPROTOCOL]),
    %% Do not authenticate; the 200 ms window must close with 1008 (policy
    %% violation), so a client can tell "too slow to authenticate" from a
    %% network drop.
    #{opcode := 8, payload := P} = recv_frame(S),
    ?assertEqual(1008, close_code(P)),
    drain_until_closed(S).

an_infinite_idle_timeout_never_closes_the_connection(Config) ->
    %% `idle_timeout_ms => infinity' is a documented value: no timer at all.
    %% The window here is 400 ms for the other cases; with `infinity' the
    %% connection must survive well past it.
    {ok, S} = authed(Config),
    timer:sleep(900),
    ok = send_text(S, <<"ping">>),
    ?assertEqual({1, <<"pong">>}, frame_of(recv_frame(S))),
    close(S).

an_idle_connection_is_closed_with_1001(Config) ->
    {ok, S} = authed(Config),
    %% Say nothing at all; the 400 ms window must close the session with 1001
    %% (going away).
    #{opcode := 8, payload := P} = recv_frame(S),
    ?assertEqual(1001, close_code(P)),
    drain_until_closed(S).

a_text_ping_keeps_an_authenticated_connection_alive(Config) ->
    {ok, S} = authed(Config),
    %% Idle window is 400 ms. Ping at 250 ms (inside the window), then wait
    %% another 250 ms and ping again -- total elapsed > 400 ms, so if the
    %% ping had not reset the timer the socket would already be closed.
    timer:sleep(250),
    ok = send_text(S, <<"ping">>),
    ?assertEqual({1, <<"pong">>}, frame_of(recv_frame(S))),
    timer:sleep(250),
    ok = send_text(S, <<"ping">>),
    ?assertEqual({1, <<"pong">>}, frame_of(recv_frame(S))),
    close(S).

%%%_* Cases: TLS ------------------------------------------------------------

a_wss_connection_authenticates_the_same_way(Config) ->
    {ok, S, ?SUBPROTOCOL} = handshake(Config, [?SUBPROTOCOL], tls),
    ok = ssl:send(S, ws_frame:encode(1, authenticate(<<"alice">>, <<"s3cr3t!">>), true)),
    #{opcode := 1, payload := P} = recv_frame(S, tls),
    ?assertMatch(#{<<"event">> := <<"authenticated">>}, decode(P)),
    ok = ssl:send(S, ws_frame:encode(1, <<"ping">>, true)),
    #{opcode := 1, payload := P2} = recv_frame(S, tls),
    ?assertEqual(<<"pong">>, P2),
    _ = ssl:close(S),
    ok.

%%%_* Helpers: client -------------------------------------------------------

authed(Config) ->
    {ok, S, ?SUBPROTOCOL} = handshake(Config, [?SUBPROTOCOL]),
    ok = send_text(S, authenticate(<<"alice">>, <<"s3cr3t!">>)),
    #{opcode := 1, payload := P} = recv_frame(S),
    #{<<"event">> := <<"authenticated">>, <<"data">> := #{<<"peer_id">> := Id}} =
        decode(P),
    ?assertEqual(16, byte_size(Id)),
    {ok, S}.

authenticate(User, Pass) ->
    json:encode(#{event => authenticate,
                  data => #{username => User, password => Pass}}).

handshake(Config, Subprotocols) ->
    handshake(Config, Subprotocols, tcp).

handshake(Config, Subprotocols, Mode) ->
    Open = case Mode of
               tcp -> fun() -> gen_tcp:connect({127, 0, 0, 1},
                                               ?config(port, Config),
                                               [binary, {active, false},
                                                {nodelay, true}], 5000) end;
               tls -> fun() -> ssl:connect("localhost", ?config(port, Config),
                                           [binary, {active, false},
                                            {verify, verify_none}], 5000) end
           end,
    {ok, S} = Open(),
    case raw_handshake_on(S, Mode, Subprotocols) of
        {ok, Selected} -> {ok, S, Selected};
        {error, Response} -> {error, Response}
    end.

raw_handshake(Config, Subprotocols) ->
    {ok, S} = gen_tcp:connect({127, 0, 0, 1}, ?config(port, Config),
                              [binary, {active, false}], 5000),
    Result = raw_handshake_on(S, tcp, Subprotocols),
    _ = gen_tcp:close(S),
    Result.

raw_handshake_on(S, Mode, Subprotocols) ->
    Key = base64:encode(crypto:strong_rand_bytes(16)),
    ProtoHeader = case Subprotocols of
                      [] -> <<>>;
                      _ -> [<<"Sec-WebSocket-Protocol: ">>,
                            lists:join(<<", ">>, Subprotocols), <<"\r\n">>]
                  end,
    Request = [<<"GET /websocket/room1 HTTP/1.1\r\nHost: localhost\r\n"
                 "Upgrade: websocket\r\nConnection: Upgrade\r\n"
                 "Sec-WebSocket-Key: ">>, Key, <<"\r\n">>,
               ProtoHeader, <<"Sec-WebSocket-Version: 13\r\n\r\n">>],
    ok = send_raw(Mode, S, Request),
    {ok, Response} = recv_header(Mode, S, <<>>),
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

send_text(S, Payload) ->
    gen_tcp:send(S, ws_frame:encode(1, Payload, true)).

send_frame(S, Op, Payload) ->
    gen_tcp:send(S, ws_frame:encode(Op, Payload, true)).

%% Build a single masked client frame with an explicit FIN bit. The shared
%% `ws_frame:encode/3' is a server-side/single-frame encoder (its third argument
%% is the mask bit, and it always sets FIN=1), so a fragmented message has to be
%% assembled by hand. Payloads here are short (< 126 bytes), so a 7-bit length
%% is enough.
frag_frame(Op, Payload, Fin) ->
    Key = crypto:strong_rand_bytes(4),
    Len = byte_size(Payload),
    FinBit = case Fin of true -> 1; false -> 0 end,
    [<<FinBit:1, 0:3, Op:4, 1:1, Len:7>>, Key, unmask(Key, Payload)].

%% A deeply nested JSON array, built recursively. 1000 levels is ~2000 bytes --
%% under the 4096 frame limit -- so the frame is accepted and the question is
%% purely whether the decoder survives the depth.
deep_array(0, Acc) -> <<Acc/binary, "]">>;
deep_array(N, Acc) -> deep_array(N - 1, <<Acc/binary, "[">>).

close(S) ->
    catch gen_tcp:send(S, ws_frame:encode(8, ws_frame:close_payload(1000, <<>>), true)),
    catch gen_tcp:close(S),
    ok.

drain_until_closed(S) ->
    case gen_tcp:recv(S, 0, ?POLL_BUDGET_MS) of
        {ok, _Ignored} -> drain_until_closed(S);
        {error, closed} -> ok;
        {error, Other} -> exit({unexpected_recv_error, Other})
    end.

recv_frame(S) -> recv_frame(S, tcp).

%% Read exactly one frame, byte-exactly: header, extended length, mask, then
%% exactly Len payload bytes. `recv(S, 0)' would swallow the next frame.
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

payload(_Mode, _S, _Mask, 0) -> <<>>;
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

frame_of(#{opcode := Op, payload := P}) -> {Op, P}.

close_code(<<Code:16, _/binary>>) -> Code;
close_code(_) -> undefined.

decode(Bin) -> json:decode(Bin).

send_raw(tcp, S, IoData) -> gen_tcp:send(S, IoData);
send_raw(tls, S, IoData) -> ssl:send(S, IoData).

%% Read the handshake response *byte-exactly*, stopping at the terminating
%% `\r\n\r\n'. A `recv(S, 0)' loop would swallow whatever the server wrote
%% after the 101 -- with `require_auth => false' that is the unsolicited
%% `authenticated' frame, and losing it makes the next `recv_frame/1' block.
recv_header(Mode, S, Acc) ->
    case binary:match(Acc, <<"\r\n\r\n">>) of
        nomatch ->
            {ok, Byte} = recv_exact(Mode, S, 1),
            recv_header(Mode, S, <<Acc/binary, Byte/binary>>);
        _ ->
            {ok, Acc}
    end.

assert_contains(Bin, Needle) ->
    case binary:match(Bin, Needle) of
        nomatch -> exit({missing, Needle, Bin});
        _ -> ok
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

self_signed() ->
    Dir = filename:join(["/tmp/opencode", "webrtc_conn_test"]),
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
