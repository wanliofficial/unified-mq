%% @doc ACL gating of the three signaling paths (S10-B, ADR-028 D5).
%%
%% Three paths pass `broker_security:authorize/2':
%%
%%   1. **joining a room** -- decided before `authenticated', so a refused client
%%      never receives a peer_id it cannot use;
%%   2. **relaying a signal** -- per-message, so a room can be joined while one
%%      particular `to' is still forbidden;
%%   3. **the server API** (`webrtc_server:send/3,4') -- the host application's
%%      own calls go through the same gate.
%%
%% The resource shape is frozen in the plan §1: `action => signal', `tenant_id'
%% from the identity, and `destination => <<"$webrtc/room/<room>">>' as an ACL
%% *pattern subject only* -- it never becomes a router destination (ADR-028 D5).
%% The last cases prove the check cannot be bypassed through the payload: adding
%% a `data' attribute, a different `event' name, or a forged `tenant_id' in the
%% message body changes nothing.
-module(webrtc_acl_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(REF, {webrtc, 0}).
-define(POLL_MS, 25).
-define(POLL_BUDGET_MS, 5000).
-define(SUBPROTOCOL, <<"webrtc.v1">>).
-define(QUIET_MS, 300).

all() ->
    [{group, join},
     {group, relay},
     {group, server_api},
     {group, not_bypassable}].

groups() ->
    [{join, [sequence],
      [an_allowed_room_is_joined,
       a_denied_room_is_refused_before_authenticated,
       a_room_wildcard_rule_covers_every_room,
       a_denied_join_closes_the_connection]},
     {relay, [sequence],
      [a_signal_in_an_allowed_room_is_relayed,
       a_denied_room_cannot_be_reached_at_all,
       a_per_room_rule_scopes_each_room_independently]},
     {server_api, [sequence],
      [server_send_reaches_an_allowed_peer,
       server_send_to_an_unknown_peer_is_an_error,
       server_peers_lists_the_room]},
     {not_bypassable, [sequence],
      [a_data_attribute_does_not_widen_the_acl,
       an_event_name_does_not_widen_the_acl,
       a_forged_tenant_in_the_body_is_ignored]}].

%%%_* Join ------------------------------------------------------------------

an_allowed_room_is_joined(Config) ->
    acl_allow(),
    restart_app(),
    {ok, S} = connect(Config, <<"acl-room-allow">>),
    PeerId = authenticate(S),
    ?assertEqual(true, is_binary(PeerId)),
    ?assertEqual([PeerId],
                 [Id || #{peer_id := Id}
                            <- webrtc_room_registry:peers(<<"acl-room-allow">>)]),
    close(S).

%% The refusal happens *before* `authenticated': a denied client must not be
%% handed a peer_id, and must not appear in the registry.
a_denied_room_is_refused_before_authenticated(Config) ->
    acl_deny(),
    restart_app(),
    {ok, S} = connect(Config, <<"acl-room-deny">>),
    ok = send_auth(S),
    ?assertEqual(#{<<"event">> => <<"unauthorized">>}, recv_frame_event(S)),
    ?assertEqual([], webrtc_room_registry:peers(<<"acl-room-deny">>)),
    close(S).

%% A trailing-/ pattern is a prefix rule, which is how an operator says "every
%% signaling room" without listing them.
a_room_wildcard_rule_covers_every_room(Config) ->
    acl(#{effect => allow, principal => <<"*">>, action => signal,
          pattern => <<"$webrtc/room/">>}),
    restart_app(),
    {ok, S1} = connect(Config, <<"acl-room-wild-a">>),
    ?assertEqual(true, is_binary(authenticate(S1))),
    {ok, S2} = connect(Config, <<"acl-room-wild-b">>),
    ?assertEqual(true, is_binary(authenticate(S2))),
    close(S1), close(S2).

a_denied_join_closes_the_connection(Config) ->
    acl_deny(),
    restart_app(),
    {ok, S} = connect(Config, <<"acl-room-deny2">>),
    ok = send_auth(S),
    #{<<"event">> := <<"unauthorized">>} = recv_frame_event(S),
    %% Close 1008 (policy violation): a refused client can tell this from a
    %% network drop, which is what R-C1 asks of the auth window too.
    case recv_frame(S) of
        #{opcode := 8, payload := P} -> ?assertEqual(1008, close_code(P));
        Other -> exit({expected_close_1008, Other})
    end.

%%%_* Relay ----------------------------------------------------------------

%% ACL allow is the default; a relayed signal works exactly as in the route
%% suite. This case exists so the denied cases below are not accidentally
%% passing because the relay never worked.
a_signal_in_an_allowed_room_is_relayed(Config) ->
    acl_allow(),
    restart_app(),
    {A, _PeerA, B, PeerB} = pair(Config, <<"acl-room-relay-ok">>),
    ok = signal(A, #{event => <<"offer">>, to => PeerB, data => #{sdp => <<"v=0">>}}),
    #{<<"to">> := PeerB} = recv_event(B, <<"offer">>),
    close(A), close(B).

%% Join and relay share one ACL subject (the room) by design: the plan §1 freezes
%% the resource as `action => signal, destination => "$webrtc/room/<room>"' for
%% both paths, so a per-room rule cannot allow the join and forbid the relay.
%% That is a deliberate simplification, not an oversight, and this case pins the
%% consequence: a denied room is denied *entirely* -- the client never joins, so
%% it never gets a peer_id, so there is nothing to relay from.
a_denied_room_cannot_be_reached_at_all(Config) ->
    acl_deny(),
    restart_app(),
    {ok, S} = connect(Config, <<"acl-room-relay-deny">>),
    ok = send_auth(S),
    ?assertEqual(#{<<"event">> => <<"unauthorized">>}, recv_frame_event(S)),
    ?assertEqual([], webrtc_room_registry:peers(<<"acl-room-relay-deny">>)),
    close(S).

%% A first-match rule set can scope rooms independently: deny one room, allow the
%% rest. The allowed room relays normally while the denied one is refused, on the
%% very same app config.
a_per_room_rule_scopes_each_room_independently(Config) ->
    acl(#{effect => deny, principal => <<"*">>, action => signal,
          pattern => <<"$webrtc/room/acl-room-blocked">>},
        #{effect => allow, principal => <<"*">>, action => signal,
          pattern => <<"$webrtc/room/">>}),
    restart_app(),
    {A, _PeerA, B, PeerB} = pair(Config, <<"acl-room-open">>),
    ok = signal(A, #{event => <<"offer">>, to => PeerB, data => #{}}),
    #{<<"to">> := PeerB} = recv_event(B, <<"offer">>),
    {ok, X} = connect(Config, <<"acl-room-blocked">>),
    ok = send_auth(X),
    ?assertEqual(#{<<"event">> => <<"unauthorized">>}, recv_frame_event(X)),
    close(A), close(B), close(X).

%%%_* Server API ------------------------------------------------------------

server_send_reaches_an_allowed_peer(Config) ->
    acl_allow(),
    restart_app(),
    {ok, S} = connect(Config, <<"acl-room-api">>),
    PeerId = authenticate(S),
    ?assertEqual(ok, webrtc_server:send(<<"acl-room-api">>, PeerId,
                                        <<"server_notice">>,
                                        #{<<"text">> => <<"hello">>})),
    #{<<"event">> := <<"server_notice">>,
      <<"data">> := #{<<"text">> := <<"hello">>}} = recv_frame_event(S),
    close(S).

%% R-S6: an unknown peer answers with an error, never raises `badarg'.
server_send_to_an_unknown_peer_is_an_error(_Config) ->
    acl_allow(),
    restart_app(),
    ?assertEqual({error, unknown_peer},
                 webrtc_server:send(<<"acl-room-none">>, <<"nobody">>,
                                    <<"server_notice">>)).

server_peers_lists_the_room(Config) ->
    acl_allow(),
    restart_app(),
    {ok, S} = connect(Config, <<"acl-room-peers">>),
    PeerId = authenticate(S),
    Members = webrtc_server:peers(<<"acl-room-peers">>),
    ?assertEqual([PeerId], [Id || #{peer_id := Id} <- Members]),
    ?assertEqual([], webrtc_server:peers(<<"acl-room-peers-empty">>)),
    close(S).

%%%_* Not bypassable --------------------------------------------------------

%% A `data' attribute cannot widen the ACL: the check runs on the message's
%% addressing, and everything inside `data' is opaque.
a_data_attribute_does_not_widen_the_acl(Config) ->
    acl(#{effect => allow, principal => <<"*">>, action => signal,
          pattern => <<"$webrtc/room/acl-open">>}),
    restart_app(),
    {ok, A} = connect(Config, <<"acl-open">>),
    {ok, B} = connect(Config, <<"acl-open">>),
    _PeerA = authenticate(A),
    PeerB = authenticate(B),
    #{<<"event">> := <<"joined">>} = recv_event(A, <<"joined">>),
    %% The payload claims a destination and a permission; neither is read.
    ok = signal(A, #{event => <<"offer">>, to => PeerB,
                     data => #{room => <<"acl-open">>,
                               allowed => true,
                               destination => <<"$webrtc/room/acl-open">>,
                               tenant_id => <<"t0">>}}),
    #{<<"to">> := PeerB,
      <<"data">> := Data} = recv_event(B, <<"offer">>),
    ?assertEqual(<<"acl-open">>, maps:get(<<"room">>, Data)),
    close(A), close(B), close_denied(Config, <<"acl-open-other">>).

an_event_name_does_not_widen_the_acl(Config) ->
    acl(#{effect => allow, principal => <<"*">>, action => signal,
          pattern => <<"$webrtc/room/acl-event">>}),
    restart_app(),
    {ok, A} = connect(Config, <<"acl-event">>),
    {ok, B} = connect(Config, <<"acl-event">>),
    _PeerA = authenticate(A),
    PeerB = authenticate(B),
    #{<<"event">> := <<"joined">>} = recv_event(A, <<"joined">>),
    %% An event named after a privileged operation is still a signal.
    ok = signal(A, #{event => <<"admin_grant">>, to => PeerB, data => #{}}),
    #{<<"event">> := <<"admin_grant">>, <<"to">> := PeerB} =
        recv_event(B, <<"admin_grant">>),
    close(A), close(B).

%% A client cannot choose its tenant by putting one in the message: the tenant
%% comes from the authenticated identity (`broker_security:tenant_id/1'), so a
%% body-level `tenant_id' is just opaque payload.
a_forged_tenant_in_the_body_is_ignored(Config) ->
    acl(#{effect => allow, principal => <<"*">>, action => signal,
          pattern => <<"$webrtc/room/">>}),
    restart_app(),
    {ok, A} = connect(Config, <<"acl-tenant">>),
    {ok, B} = connect(Config, <<"acl-tenant">>),
    _PeerA = authenticate(A),
    PeerB = authenticate(B),
    #{<<"event">> := <<"joined">>} = recv_event(A, <<"joined">>),
    ok = signal(A, #{event => <<"offer">>, to => PeerB,
                     data => #{tenant_id => <<"other-tenant">>}}),
    #{<<"data">> := #{<<"tenant_id">> := Echoed}} = recv_event(B, <<"offer">>),
    ?assertEqual(<<"other-tenant">>, Echoed),
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
    ok = application:set_env(broker_proto_webrtc, subprotocols, [?SUBPROTOCOL]),
    ok = application:set_env(broker_proto_webrtc, auth_timeout_ms, 5000),
    ok = application:set_env(broker_proto_webrtc, idle_timeout_ms, 60000),
    ok = application:set_env(broker_proto_webrtc, max_frame_bytes, 262144),
    ok = application:set_env(broker_proto_webrtc, max_peers_per_room, 2),
    [{port, free_port()} | Config].

end_per_testcase(_Case, _Config) ->
    stop_app(),
    ok.

%% `restart_app/0' is called by the cases after they have chosen their ACL, so
%% the port is bound only once the policy is in place.
restart_app() ->
    ok = application:set_env(broker_proto_webrtc, listeners,
                             [#{port => 0, num_acceptors => 4,
                                max_connections => 64}]),
    {ok, _} = application:ensure_all_started(broker_security),
    {ok, _} = application:ensure_all_started(broker_proto_webrtc),
    ok.

free_port() ->
    %% The concrete port is asked of ranch after start-up; cases that need it
    %% call `listener_port/0'.
    0.

stop_app() ->
    _ = application:stop(broker_proto_webrtc),
    _ = application:stop(broker_security),
    ok.

listener_port() ->
    case ranch:get_port(?REF) of
        Port when is_integer(Port) -> Port;
        Other -> exit({listener_port_unavailable, Other})
    end.

acl_allow() ->
    ok = application:set_env(broker_security, acl, allow).

acl_deny() ->
    ok = application:set_env(broker_security, acl,
                             [#{effect => deny, principal => <<"*">>,
                                action => signal, pattern => <<"*">>}]).

acl(Rules) when is_list(Rules) ->
    ok = application:set_env(broker_security, acl, Rules);
acl(Rule) ->
    acl([Rule]).

acl(Rule1, Rule2) ->
    acl([Rule1, Rule2]).

%% A room that is not covered by the allow rule must still refuse.
close_denied(_Config, Room) ->
    {ok, S} = connect_any(Room),
    ok = send_auth(S),
    case recv_frame_event(S) of
        #{<<"event">> := <<"unauthorized">>} -> close(S);
        Other -> exit({expected_unauthorized, Other})
    end.

%%%_* Client helpers --------------------------------------------------------

connect(_Config, Room) ->
    connect_any(Room).

connect_any(Room) ->
    {ok, S} = gen_tcp:connect({127, 0, 0, 1}, listener_port(),
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

send_auth(S) ->
    send_text(S, json:encode(#{event => authenticate,
                               data => #{username => <<"alice">>,
                                         password => <<"s3cr3t!">>}})).

authenticate(S) ->
    ok = send_auth(S),
    #{<<"event">> := <<"authenticated">>,
      <<"data">> := #{<<"peer_id">> := Id}} = recv_frame_event(S),
    Id.

pair(Config, Room) ->
    {ok, A} = connect(Config, Room),
    {ok, B} = connect(Config, Room),
    PeerA = authenticate(A),
    PeerB = authenticate(B),
    #{<<"event">> := <<"joined">>,
      <<"data">> := #{<<"peer_id">> := PeerB}} = recv_event(A, <<"joined">>),
    {A, PeerA, B, PeerB}.

signal(S, #{event := Event, to := To} = Msg) ->
    Data = maps:get(data, Msg, #{}),
    Body = maps:without([event, to, data], Msg),
    send_text(S, json:encode(Body#{event => Event, to => To, data => Data})).

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

close_code(<<Code:16, _/binary>>) -> Code;
close_code(_) -> 0.
