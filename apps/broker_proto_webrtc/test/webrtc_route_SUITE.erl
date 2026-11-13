%% @doc `to' forwarding: process-direct delivery, byte-transparent payloads, and
%% every refusal path (S10-B).
%%
%% R-S2 is the interesting one to pin: the forwarded frame is the client's own
%% message with `from' added -- the `event' name and the `data' payload (SDP text,
%% ICE candidate strings) are never parsed or rewritten. The cases below compare
%% the SDP/candidate values *value for value* through a real socket, which is the
%% only way to notice a server that "helpfully" normalises the payload.
%%
%% R-S5 is the other half: a `to' that cannot be delivered (missing, unknown,
%% other room, dead peer) must produce one explicit event and *not* kill the
%% connection -- the upstream handler crashed the whole WebSocket here (F-1).
-module(webrtc_route_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(REF, {webrtc, 0}).
-define(POLL_MS, 25).
-define(POLL_BUDGET_MS, 5000).
-define(SUBPROTOCOL, <<"webrtc.v1">>).
-define(QUIET_MS, 300).

%% A real-looking SDP body: multi-line, with `\r\n', an embedded non-ASCII
%% session name, and long fingerprint/candidate lines. If the relay ever
%% re-encodes or truncates the payload, these bytes stop matching.
-define(SDP, <<"v=0\r\no=- 4611731400430051336 2 IN IP4 127.0.0.1\r\n"
               "s=-\r\nt=0 0\r\na=group:BUNDLE 0\r\n"
               "a=fingerprint:sha-256 "
               "7B:8B:F0:65:5F:78:E2:51:3B:AC:6F:F3:3F:46:1B:35:"
               "DC:B6:16:A3:1D:df:6F:83:F6:D2:32:97:9F:4D:B4:AA\r\n"
               "m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n"
               "a=ice-ufrag:4ZcD\r\na=ice-pwd:2/1muCWoOi3uLifh0NuRHlZw\r\n">>).

-define(CANDIDATE,
        <<"candidate:842163049 1 udp 1677729535 192.168.1.42 54321 typ srflx "
          "raddr 10.0.0.7 rport 8998 generation 0 ufrag 4ZcD network-id 1">>).

all() ->
    [{group, delivery},
     {group, transparency},
     {group, refusals},
     {group, isolation}].

groups() ->
    [{delivery, [sequence],
      [a_signal_reaches_the_addressed_peer_with_from_added,
       the_forwarded_frame_keeps_its_to_field,
       three_peers_are_addressed_individually]},
     {transparency, [sequence],
      [an_offer_sdp_is_relayed_byte_for_byte,
       an_ice_candidate_string_is_relayed_verbatim,
       an_unknown_event_name_is_relayed_unchanged]},
     {refusals, [sequence],
      [a_missing_to_is_invalid_message,
       an_unknown_to_is_invalid_message,
       an_empty_to_is_invalid_message,
       a_missing_event_is_invalid_message,
       the_connection_survives_a_refused_signal]},
     {isolation, [sequence],
      [a_to_in_another_room_does_not_leak,
       a_peer_id_from_another_room_is_indistinguishable_from_unknown,
       a_client_cannot_forge_its_own_from]}].

%%%_* Delivery --------------------------------------------------------------

a_signal_reaches_the_addressed_peer_with_from_added(Config) ->
    {A, PeerA, B, PeerB} = pair(Config, <<"route-room-1">>),
    ok = signal(A, #{event => <<"offer">>, to => PeerB,
                     data => #{sdp => <<"v=0\r\n">>, type => <<"offer">>}}),
    #{<<"event">> := <<"offer">>,
      <<"from">> := From,
      <<"to">> := To,
      <<"data">> := #{<<"type">> := <<"offer">>}} = recv_event(B, <<"offer">>),
    ?assertEqual(PeerA, From),
    ?assertEqual(PeerB, To),
    close(A), close(B).

%% Design §2.3: the relayed frame keeps the client's `to'. That is a deliberate
%% departure from what the README example shows (it lists only event/from/data),
%% and it is what lets a client render both sides of a session from one frame.
the_forwarded_frame_keeps_its_to_field(Config) ->
    {A, _PeerA, B, PeerB} = pair(Config, <<"route-room-2">>),
    ok = signal(A, #{event => <<"answer">>, to => PeerB,
                     data => #{sdp => <<"v=0\r\n">>}}),
    #{<<"to">> := Kept} = recv_event(B, <<"answer">>),
    ?assertEqual(PeerB, Kept),
    close(A), close(B).

%% One room, three members: `to' must select exactly one, and the others must
%% see nothing. Membership is raised for this case only, so the 1:1 default is
%% still the shipped configuration.
three_peers_are_addressed_individually(Config) ->
    ok = application:set_env(broker_proto_webrtc, max_peers_per_room, 3),
    restart_app(),
    %% The restarted listener binds a new ephemeral port, so the suite must ask
    %% ranch again rather than reuse the one captured in `Config'.
    Port = listener_port(),
    Room = <<"route-room-3">>,
    {ok, A} = connect_to(Port, Room),
    {ok, B} = connect_to(Port, Room),
    {ok, C} = connect_to(Port, Room),
    _PeerA = authenticate(A),
    PeerB = authenticate(B),
    PeerC = authenticate(C),
    %% Each existing member is told about every joiner: A sees B and C, B sees C.
    #{<<"event">> := <<"joined">>} = recv_event(A, <<"joined">>),
    #{<<"event">> := <<"joined">>} = recv_event(A, <<"joined">>),
    #{<<"event">> := <<"joined">>} = recv_event(B, <<"joined">>),
    ok = signal(A, #{event => <<"offer">>, to => PeerC, data => #{n => 1}}),
    #{<<"to">> := PeerC} = recv_event(C, <<"offer">>),
    ?assertEqual(quiet, quiet(B)),
    ok = signal(A, #{event => <<"offer">>, to => PeerB, data => #{n => 2}}),
    #{<<"to">> := PeerB} = recv_event(B, <<"offer">>),
    ?assertEqual(quiet, quiet(C)),
    _ = Config,
    close(A), close(B), close(C).

%%%_* Transparency ---------------------------------------------------------

%% The SDP must arrive value-for-value: same `\r\n' separators, same
%% fingerprint, same trailing fields.
an_offer_sdp_is_relayed_byte_for_byte(Config) ->
    {A, _PeerA, B, PeerB} = pair(Config, <<"route-room-4">>),
    ok = signal(A, #{event => <<"offer">>, to => PeerB,
                     data => #{sdp => ?SDP, type => <<"offer">>}}),
    #{<<"data">> := #{<<"sdp">> := Relayed}} = recv_event(B, <<"offer">>),
    ?assertEqual(?SDP, Relayed),
    close(A), close(B).

an_ice_candidate_string_is_relayed_verbatim(Config) ->
    {A, _PeerA, B, PeerB} = pair(Config, <<"route-room-5">>),
    ok = signal(A, #{event => <<"candidate">>, to => PeerB,
                     data => #{candidate => ?CANDIDATE,
                               sdpMid => <<"0">>,
                               sdpMLineIndex => 0}}),
    #{<<"data">> := Data} = recv_event(B, <<"candidate">>),
    ?assertEqual(?CANDIDATE, maps:get(<<"candidate">>, Data)),
    ?assertEqual(<<"0">>, maps:get(<<"sdpMid">>, Data)),
    ?assertEqual(0, maps:get(<<"sdpMLineIndex">>, Data)),
    close(A), close(B).

%% The server has no allow-list of signaling event names: a client is free to
%% invent one, and it must pass through untouched (a future renegotiation or
%% `bye' event needs this).
an_unknown_event_name_is_relayed_unchanged(Config) ->
    {A, _PeerA, B, PeerB} = pair(Config, <<"route-room-6">>),
    ok = signal(A, #{event => <<"renegotiate">>, to => PeerB,
                     data => #{reason => <<"bitrate">>, nested => #{a => [1, 2, 3]}}}),
    #{<<"event">> := <<"renegotiate">>,
      <<"data">> := #{<<"reason">> := <<"bitrate">>,
                      <<"nested">> := #{<<"a">> := [1, 2, 3]}}} =
        recv_event(B, <<"renegotiate">>),
    close(A), close(B).

%%%_* Refusals -------------------------------------------------------------

%% R-S5: authenticated message without `to' -> `invalid_message', no `data'.
a_missing_to_is_invalid_message(Config) ->
    {A, _PeerA, B, _PeerB} = pair(Config, <<"route-room-7">>),
    ok = send_text(A, json:encode(#{event => <<"offer">>,
                                    data => #{sdp => <<"v=0\r\n">>}})),
    ?assertEqual(#{<<"event">> => <<"invalid_message">>}, recv_frame_event(A)),
    ?assertEqual(quiet, quiet(B)),
    close(A), close(B).

an_unknown_to_is_invalid_message(Config) ->
    {A, _PeerA, B, _PeerB} = pair(Config, <<"route-room-8">>),
    ok = signal(A, #{event => <<"offer">>, to => <<"no-such-peer">>,
                     data => #{}}),
    ?assertEqual(#{<<"event">> => <<"invalid_message">>}, recv_frame_event(A)),
    ?assertEqual(quiet, quiet(B)),
    close(A), close(B).

%% An empty string is not a peer id: it must be refused as a missing `to', not
%% looked up (an empty key would be a confusing `unknown_peer').
an_empty_to_is_invalid_message(Config) ->
    {A, _PeerA, B, _PeerB} = pair(Config, <<"route-room-9">>),
    ok = signal(A, #{event => <<"offer">>, to => <<>>, data => #{}}),
    ?assertEqual(#{<<"event">> => <<"invalid_message">>}, recv_frame_event(A)),
    close(A), close(B).

a_missing_event_is_invalid_message(Config) ->
    {A, _PeerA, B, PeerB} = pair(Config, <<"route-room-10">>),
    ok = send_text(A, json:encode(#{to => PeerB, data => #{}})),
    ?assertEqual(#{<<"event">> => <<"invalid_message">>}, recv_frame_event(A)),
    ?assertEqual(quiet, quiet(B)),
    close(A), close(B).

%% The whole point of R-S5: a refused signal leaves the connection usable. One
%% refusal, then a legal signal on the same socket.
the_connection_survives_a_refused_signal(Config) ->
    {A, PeerA, B, PeerB} = pair(Config, <<"route-room-11">>),
    ok = signal(A, #{event => <<"offer">>, to => <<"nobody">>, data => #{}}),
    ?assertEqual(#{<<"event">> => <<"invalid_message">>}, recv_frame_event(A)),
    ok = signal(A, #{event => <<"candidate">>, to => PeerB,
                     data => #{candidate => ?CANDIDATE}}),
    #{<<"from">> := From,
      <<"data">> := #{<<"candidate">> := Relayed}} = recv_event(B, <<"candidate">>),
    ?assertEqual(PeerA, From),
    ?assertEqual(?CANDIDATE, Relayed),
    close(A), close(B).

%%%_* Isolation ------------------------------------------------------------

%% R-X4: a room is the addressing scope. A peer id that lives in another room is
%% not reachable, and the sender is told so with the same event an unknown peer
%% gets.
a_to_in_another_room_does_not_leak(Config) ->
    {A, _PeerA, B, _PeerB} = pair(Config, <<"route-room-12a">>),
    {ok, X} = connect(Config, <<"route-room-12b">>),
    {ok, Y} = connect(Config, <<"route-room-12b">>),
    _PeerX = authenticate(X),
    PeerY = authenticate(Y),
    %% A is in room 12a, Y in 12b: addressing Y from A must fail.
    ok = signal(A, #{event => <<"offer">>, to => PeerY, data => #{}}),
    ?assertEqual(#{<<"event">> => <<"invalid_message">>}, recv_frame_event(A)),
    ?assertEqual(quiet, quiet(Y)),
    close(A), close(B), close(X), close(Y).

%% The two failures are indistinguishable on the wire: a distinct answer for
%% "exists but elsewhere" would turn `to' into a peer-id existence oracle.
a_peer_id_from_another_room_is_indistinguishable_from_unknown(Config) ->
    {A, _PeerA, B, _PeerB} = pair(Config, <<"route-room-13a">>),
    {ok, X} = connect(Config, <<"route-room-13b">>),
    {ok, Y} = connect(Config, <<"route-room-13b">>),
    _ = authenticate(X),
    _ = authenticate(Y),
    ForeignIds = [Id || #{peer_id := Id}
                            <- webrtc_room_registry:peers(<<"route-room-13b">>)],
    ?assertEqual(2, length(ForeignIds)),
    [OtherId | _] = ForeignIds,
    ok = signal(A, #{event => <<"offer">>, to => OtherId, data => #{}}),
    ForeignAnswer = recv_frame_event(A),
    ok = signal(A, #{event => <<"offer">>, to => <<"definitely-not-a-peer">>,
                     data => #{}}),
    UnknownAnswer = recv_frame_event(A),
    ?assertEqual(UnknownAnswer, ForeignAnswer),
    ?assertEqual(#{<<"event">> => <<"invalid_message">>}, ForeignAnswer),
    close(A), close(B), close(X), close(Y).

%% R-S7: `from' is stamped by the server. A client that puts a `from' in its own
%% message must not be able to speak as somebody else -- the relayed value is the
%% sender's registry peer id, and the forged one is simply overwritten.
a_client_cannot_forge_its_own_from(Config) ->
    {A, PeerA, B, PeerB} = pair(Config, <<"route-room-14">>),
    ok = signal(A, #{event => <<"offer">>, to => PeerB, from => <<"victim">>,
                     data => #{}}),
    #{<<"from">> := From} = recv_event(B, <<"offer">>),
    ?assertEqual(PeerA, From),
    ?assertNotEqual(<<"victim">>, From),
    close(A), close(B).

%%%_* Fixture ---------------------------------------------------------------

init_per_suite(Config) ->
    _ = application:load(broker_security),
    _ = application:load(broker_proto_webrtc),
    Config.

end_per_suite(_Config) ->
    _ = application:stop(broker_proto_webrtc),
    _ = application:stop(broker_security),
    ok.

init_per_testcase(_Case, Config) ->
    stop_app(),
    ok = application:set_env(broker_security, users,
                             [{<<"alice">>, #{password => <<"s3cr3t!">>}},
                              {<<"bob">>,   #{password => <<"s3cr3t!">>}}]),
    ok = application:set_env(broker_security, acl, allow),
    ok = application:set_env(broker_proto_webrtc, require_auth, true),
    ok = application:set_env(broker_proto_webrtc, subprotocols, [?SUBPROTOCOL]),
    ok = application:set_env(broker_proto_webrtc, auth_timeout_ms, 5000),
    ok = application:set_env(broker_proto_webrtc, idle_timeout_ms, 60000),
    ok = application:set_env(broker_proto_webrtc, max_frame_bytes, 262144),
    ok = application:set_env(broker_proto_webrtc, max_peers_per_room, 2),
    start_app(),
    [{port, listener_port()} | Config].

end_per_testcase(_Case, _Config) ->
    stop_app(),
    ok.

start_app() ->
    ok = application:set_env(broker_proto_webrtc, listeners,
                             [#{port => 0, num_acceptors => 4,
                                max_connections => 64}]),
    {ok, _} = application:ensure_all_started(broker_security),
    {ok, _} = application:ensure_all_started(broker_proto_webrtc),
    ok.

restart_app() ->
    stop_app(),
    start_app().

stop_app() ->
    _ = application:stop(broker_proto_webrtc),
    _ = application:stop(broker_security),
    ok.

listener_port() ->
    case ranch:get_port(?REF) of
        Port when is_integer(Port) -> Port;
        Other -> exit({listener_port_unavailable, Other})
    end.

%% Two authenticated members of the same room, plus both peer ids.
pair(Config, Room) ->
    {ok, A} = connect(Config, Room),
    {ok, B} = connect(Config, Room),
    PeerA = authenticate(A),
    PeerB = authenticate(B),
    %% B's join is announced to A; consuming it keeps later reads unambiguous.
    #{<<"event">> := <<"joined">>,
      <<"data">> := #{<<"peer_id">> := PeerB}} = recv_event(A, <<"joined">>),
    {A, PeerA, B, PeerB}.

%%%_* Client helpers --------------------------------------------------------

connect(Config, Room) ->
    connect_to(?config(port, Config), Room).

connect_to(Port, Room) ->
    {ok, S} = gen_tcp:connect({127, 0, 0, 1}, Port,
                              [binary, {active, false}, {nodelay, true}], 5000),
    Key = base64:encode(crypto:strong_rand_bytes(16)),
    Request = [<<"GET /websocket/">>, Room,
               <<" HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\n"
                 "Connection: Upgrade\r\nSec-WebSocket-Key: ">>, Key,
               <<"\r\nSec-WebSocket-Protocol: ">>, ?SUBPROTOCOL,
               <<"\r\nSec-WebSocket-Version: 13\r\n\r\n">>],
    ok = gen_tcp:send(S, Request),
    case recv_until(S, <<"\r\n\r\n">>, <<>>) of
        {ok, <<"HTTP/1.1 101", _/binary>>} -> {ok, S};
        {ok, Resp} -> exit({handshake_refused, Resp});
        Other -> exit({handshake_failed, Other})
    end.

authenticate(S) ->
    ok = send_text(S, json:encode(#{event => authenticate,
                                    data => #{username => <<"alice">>,
                                              password => <<"s3cr3t!">>}})),
    #{<<"event">> := <<"authenticated">>,
      <<"data">> := #{<<"peer_id">> := Id}} = recv_frame_event(S),
    Id.

%% Send one client signal, with the `to' and `event' fields as given.
signal(S, #{event := Event, to := To} = Msg) ->
    Data = maps:get(data, Msg, #{}),
    Body = maps:without([event, to, data], Msg),
    send_text(S, json:encode(Body#{event => Event, to => To, data => Data}));
signal(S, #{event := Event} = Msg) ->
    Data = maps:get(data, Msg, #{}),
    send_text(S, json:encode((maps:without([event, data], Msg))#{
                                event => Event, data => Data})).

send_text(S, Payload) ->
    gen_tcp:send(S, ws_frame:encode(1, Payload, true)).

close(S) ->
    catch gen_tcp:send(S, ws_frame:encode(8, ws_frame:close_payload(1000, <<>>), true)),
    catch gen_tcp:close(S),
    ok.

recv_event(S, Name) -> recv_event(S, Name, 40).

recv_event(_S, Name, 0) ->
    exit({event_never_arrived, Name});
recv_event(S, Name, Left) ->
    Event = recv_frame_event(S),
    case maps:get(<<"event">>, Event, undefined) of
        Name -> Event;
        _ -> recv_event(S, Name, Left - 1)
    end.

recv_frame_event(S) ->
    case recv_frame(S) of
        #{opcode := 1, payload := P} -> json:decode(P);
        Other -> exit({expected_text_frame, Other})
    end.

recv_frame(S) ->
    {ok, <<Fin:1, Rsv:3, Op:4, Mask:1, Len0:7>>} = recv_exact(S, 2),
    0 = Rsv,
    Len = case Len0 of
              126 -> {ok, <<L:16>>} = recv_exact(S, 2), L;
              127 -> {ok, <<L:64>>} = recv_exact(S, 8), L;
              _ -> Len0
          end,
    Payload = payload(S, Mask, Len),
    #{fin => Fin =:= 1, masked => Mask =:= 1, opcode => Op, payload => Payload}.

payload(_S, _Mask, 0) -> <<>>;
payload(S, 1, Len) ->
    {ok, Key} = recv_exact(S, 4),
    {ok, Masked} = recv_exact(S, Len),
    unmask(Key, Masked);
payload(S, 0, Len) ->
    {ok, Plain} = recv_exact(S, Len),
    Plain.

recv_exact(S, N) ->
    case gen_tcp:recv(S, N, ?POLL_BUDGET_MS) of
        {ok, Bin} -> {ok, Bin};
        Other -> exit({recv_failed, Other, N})
    end.

unmask(Key, Payload) ->
    Size = byte_size(Payload),
    Stream = binary:copy(Key, (Size div 4) + 1),
    crypto:exor(Payload, binary:part(Stream, 0, Size)).

recv_until(S, Needle, Acc) ->
    case binary:match(Acc, Needle) of
        nomatch ->
            case gen_tcp:recv(S, 0, 5000) of
                {ok, Data} -> recv_until(S, Needle, <<Acc/binary, Data/binary>>);
                Other -> Other
            end;
        _ ->
            {ok, Acc}
    end.

quiet(S) ->
    case gen_tcp:recv(S, 0, ?QUIET_MS) of
        {error, timeout} -> quiet;
        {error, closed} -> quiet;
        {ok, Bin} -> {unexpected, Bin}
    end.
