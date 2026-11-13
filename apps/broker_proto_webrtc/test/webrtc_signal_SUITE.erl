%% @doc Wire format of the five signaling events, and the R-S3 broadcast sets
%% that decide who receives which of them (S10-B).
%%
%% Two layers under test:
%%
%%   * the *pure* constructors in `webrtc_signal' -- exact JSON bytes, and in
%%     particular that `unauthorized' / `invalid_message' carry no `data' key
%%     at all (a difference a client library would hide);
%%   * the *broadcast sets*, which is where the upstream sample is easy to get
%%     subtly wrong: `joined' goes to the members that were already there (never
%%     to the joiner), `left' to those that remain (never to the one leaving),
%%     and `authenticated' / `unauthorized' / `invalid_message' only to the
%%     requester.
%%
%% The second half runs real socket-level clients because the recipient set is
%% only observable end to end -- a unit test of the registry cannot prove that
%% the joiner got nothing.
-module(webrtc_signal_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(REF, {webrtc, 0}).
-define(POLL_MS, 25).
-define(POLL_BUDGET_MS, 5000).
-define(SUBPROTOCOL, <<"webrtc.v1">>).
-define(QUIET_MS, 300).

all() ->
    [{group, constructors},
     {group, broadcast}].

groups() ->
    [{constructors, [sequence],
      [authenticated_carries_the_peer_id_only,
       unauthorized_has_no_data_field,
       invalid_message_has_no_data_field,
       joined_carries_username_and_peer_id,
       left_carries_username_and_peer_id,
       every_event_is_a_text_frame]},
     {broadcast, [sequence],
      [joined_is_sent_only_to_members_already_present,
       the_joiner_gets_no_joined_event,
       left_is_sent_to_the_remaining_members_only,
       the_leaver_gets_no_left_event,
       authenticated_is_a_reply_to_the_requester_only,
       unauthorized_is_a_reply_to_the_requester_only,
       invalid_message_is_a_reply_to_the_requester_only]}].

%%%_* Constructor cases ------------------------------------------------------

authenticated_carries_the_peer_id_only(_Config) ->
    {text, Bin} = webrtc_signal:authenticated(<<"abc123">>),
    ?assertEqual(#{<<"event">> => <<"authenticated">>,
                   <<"data">> => #{<<"peer_id">> => <<"abc123">>}},
                 decode(Bin)).

unauthorized_has_no_data_field(_Config) ->
    {text, Bin} = webrtc_signal:unauthorized(),
    ?assertEqual(#{<<"event">> => <<"unauthorized">>}, decode(Bin)),
    %% The absence of `data' is the contract, not an incidental omission.
    ?assertEqual(error, maps:find(<<"data">>, decode(Bin))).

invalid_message_has_no_data_field(_Config) ->
    {text, Bin} = webrtc_signal:invalid_message(),
    ?assertEqual(#{<<"event">> => <<"invalid_message">>}, decode(Bin)),
    ?assertEqual(error, maps:find(<<"data">>, decode(Bin))).

joined_carries_username_and_peer_id(_Config) ->
    {text, Bin} = webrtc_signal:joined(<<"alice">>, <<"peer-1">>),
    ?assertEqual(#{<<"event">> => <<"joined">>,
                   <<"data">> => #{<<"username">> => <<"alice">>,
                                   <<"peer_id">> => <<"peer-1">>}},
                 decode(Bin)).

left_carries_username_and_peer_id(_Config) ->
    {text, Bin} = webrtc_signal:left(<<"alice">>, <<"peer-1">>),
    ?assertEqual(#{<<"event">> => <<"left">>,
                   <<"data">> => #{<<"username">> => <<"alice">>,
                                   <<"peer_id">> => <<"peer-1">>}},
                 decode(Bin)).

every_event_is_a_text_frame(_Config) ->
    Events = [webrtc_signal:authenticated(<<"p">>),
              webrtc_signal:unauthorized(),
              webrtc_signal:invalid_message(),
              webrtc_signal:joined(<<"u">>, <<"p">>),
              webrtc_signal:left(<<"u">>, <<"p">>)],
    ?assertEqual([text, text, text, text, text],
                 [Kind || {Kind, _Bin} <- Events]).

%%%_* Broadcast cases -------------------------------------------------------

%% R-S3 / design §2.4: `joined' announces the *new* peer to the *existing* ones.
joined_is_sent_only_to_members_already_present(Config) ->
    {ok, A} = connect(Config, <<"broadcast-room-1">>),
    PeerA = authenticate(A),
    {ok, B} = connect(Config, <<"broadcast-room-1">>),
    PeerB = authenticate(B),
    #{<<"data">> := #{<<"peer_id">> := PeerB}} = recv_event(A, <<"joined">>),
    ?assertNotEqual(PeerA, PeerB),
    close(A), close(B).

the_joiner_gets_no_joined_event(Config) ->
    {ok, A} = connect(Config, <<"broadcast-room-2">>),
    _ = authenticate(A),
    {ok, B} = connect(Config, <<"broadcast-room-2">>),
    _ = authenticate(B),
    %% The joiner already consumed its `authenticated'; the next thing on its
    %% wire must be nothing at all (upstream: it learns its peers from `from').
    ?assertEqual(quiet, quiet(B)),
    close(A), close(B).

left_is_sent_to_the_remaining_members_only(Config) ->
    {ok, A} = connect(Config, <<"broadcast-room-3">>),
    _ = authenticate(A),
    {ok, B} = connect(Config, <<"broadcast-room-3">>),
    PeerB = authenticate(B),
    #{<<"data">> := #{<<"peer_id">> := PeerB}} = recv_event(A, <<"joined">>),
    close(B),
    #{<<"data">> := #{<<"peer_id">> := PeerB, <<"username">> := <<"alice">>}} =
        recv_event(A, <<"left">>),
    close(A).

the_leaver_gets_no_left_event(Config) ->
    {ok, A} = connect(Config, <<"broadcast-room-4">>),
    _ = authenticate(A),
    {ok, B} = connect(Config, <<"broadcast-room-4">>),
    _ = authenticate(B),
    _ = recv_event(A, <<"joined">>),
    %% B leaves; its own `left' must not come back to it -- the socket is
    %% closing, and R-S3 says the remaining members are the recipients.
    close(B),
    ?assertEqual(quiet, quiet(B)),
    #{<<"event">> := <<"left">>} = recv_event(A, <<"left">>),
    close(A).

authenticated_is_a_reply_to_the_requester_only(Config) ->
    {ok, A} = connect(Config, <<"broadcast-room-5">>),
    _ = authenticate(A),
    {ok, B} = connect(Config, <<"broadcast-room-5">>),
    _ = authenticate(B),
    %% A sees only `joined' for B, never B's `authenticated'.
    #{<<"event">> := <<"joined">>} = recv_event(A, <<"joined">>),
    ?assertEqual(quiet, quiet(A)),
    close(A), close(B).

unauthorized_is_a_reply_to_the_requester_only(Config) ->
    {ok, A} = connect(Config, <<"broadcast-room-6">>),
    _ = authenticate(A),
    %% B connects but never authenticates, so nothing joins the room and A has
    %% no `joined' to consume; the only thing that can reach A is a leaked
    %% `unauthorized'.
    {ok, B} = connect(Config, <<"broadcast-room-6">>),
    ok = send_text(B, json:encode(#{event => authenticate,
                                    data => #{username => <<"alice">>,
                                              password => <<"wrong">>}})),
    #{<<"event">> := <<"unauthorized">>} = recv_frame_event(B),
    ?assertEqual(quiet, quiet(A)),
    close(A), close(B).

invalid_message_is_a_reply_to_the_requester_only(Config) ->
    {ok, A} = connect(Config, <<"broadcast-room-7">>),
    _ = authenticate(A),
    {ok, B} = connect(Config, <<"broadcast-room-7">>),
    _ = authenticate(B),
    _ = recv_event(A, <<"joined">>),
    ok = send_text(B, <<"{\"event\":\"offer\",\"data\":{}}">>),
    #{<<"event">> := <<"invalid_message">>} = recv_frame_event(B),
    ?assertEqual(quiet, quiet(A)),
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
                             [{<<"alice">>, #{password => <<"s3cr3t!">>,
                                              tenant_id => <<"t0">>}}]),
    ok = application:set_env(broker_security, acl, allow),
    ok = application:set_env(broker_proto_webrtc, require_auth, true),
    ok = application:set_env(broker_proto_webrtc, listeners,
                             [#{port => 0, num_acceptors => 4,
                                max_connections => 64}]),
    ok = application:set_env(broker_proto_webrtc, subprotocols, [?SUBPROTOCOL]),
    ok = application:set_env(broker_proto_webrtc, auth_timeout_ms, 5000),
    ok = application:set_env(broker_proto_webrtc, idle_timeout_ms, 60000),
    ok = application:set_env(broker_proto_webrtc, max_frame_bytes, 262144),
    ok = application:set_env(broker_proto_webrtc, max_peers_per_room, 8),
    {ok, _} = application:ensure_all_started(broker_security),
    {ok, _} = application:ensure_all_started(broker_proto_webrtc),
    [{port, listener_port()} | Config].

end_per_testcase(_Case, Config) ->
    stop_app(),
    Config.

stop_app() ->
    _ = application:stop(broker_proto_webrtc),
    _ = application:stop(broker_security),
    ok.

listener_port() ->
    case ranch:get_port(?REF) of
        Port when is_integer(Port) -> Port;
        Other -> exit({listener_port_unavailable, Other})
    end.

%%%_* Client helpers --------------------------------------------------------

connect(Config, Room) ->
    {ok, S} = gen_tcp:connect({127, 0, 0, 1}, ?config(port, Config),
                              [binary, {active, false}, {nodelay, true}], 5000),
    Key = base64:encode(crypto:strong_rand_bytes(16)),
    Request = [<<"GET /websocket/">>, Room,
               <<" HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\n"
                 "Connection: Upgrade\r\nSec-WebSocket-Key: ">>, Key,
               <<"\r\nSec-WebSocket-Protocol: ">>, ?SUBPROTOCOL,
               <<"\r\nSec-WebSocket-Version: 13\r\n\r\n">>],
    ok = gen_tcp:send(S, Request),
    {ok, Resp} = recv_until(S, <<"\r\n\r\n">>, <<>>),
    case Resp of
        <<"HTTP/1.1 101", _/binary>> -> {ok, S};
        _ -> exit({handshake_refused, Resp})
    end.

%% Authenticate and return the minted peer id.
authenticate(S) ->
    ok = send_text(S, json:encode(#{event => authenticate,
                                    data => #{username => <<"alice">>,
                                              password => <<"s3cr3t!">>}})),
    #{<<"event">> := <<"authenticated">>,
      <<"data">> := #{<<"peer_id">> := Id}} = recv_frame_event(S),
    Id.

%% Read one event whose `event' is `Name', tolerating anything that arrived
%% before it (a `joined' broadcast may precede a reply the test cares about).
recv_event(S, Name) ->
    recv_event(S, Name, 20).

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

send_text(S, Payload) ->
    gen_tcp:send(S, ws_frame:encode(1, Payload, true)).

close(S) ->
    catch gen_tcp:send(S, ws_frame:encode(8, ws_frame:close_payload(1000, <<>>), true)),
    catch gen_tcp:close(S),
    ok.

%% Nothing must arrive within the quiet window. `closed' counts as quiet: a
%% socket that has been torn down cannot deliver a stray event either, which is
%% exactly what the "no `left' to the leaver / no `joined' to the joiner" cases
%% need to prove.
quiet(S) ->
    case gen_tcp:recv(S, 0, ?QUIET_MS) of
        {error, timeout} -> quiet;
        {error, closed} -> quiet;
        {ok, Bin} -> {unexpected, Bin}
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

decode(Bin) -> json:decode(Bin).
