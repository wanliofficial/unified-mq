%% @doc Room registry and membership, at the API level and over real sockets
%% (S10-B).
%%
%% The properties under test are the ones ADR-028 D4 freezes:
%%
%%   * `phash2(Room, Shards)' placement is asserted directly -- the table name
%%     is a function of the shard index, so a wrong hash would move rooms
%%     between tables and break every lookup;
%%   * `join' is idempotent per connection, refuses a taken `peer_id', and
%%     enforces `max_peers_per_room';
%%   * `leave' is idempotent and never raises -- it runs in `terminate/2';
%%   * a connection killed with `exit(Pid, kill)' still produces exactly one
%%     `left' (the shard's monitor, not `terminate/2', is what guarantees it);
%%   * `peers/1' and `rooms/0' describe the current membership.
%%
%% The registry half is exercised through its own API (the connection process is
%% irrelevant there); the `left'-on-kill half needs a real socket, because that
%% is the only path a browser connection can take out of a room.
-module(webrtc_room_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(REF, {webrtc, 0}).
-define(POLL_MS, 25).
-define(POLL_BUDGET_MS, 5000).
-define(SUBPROTOCOL, <<"webrtc.v1">>).
-define(QUIET_MS, 300).

all() ->
    [{group, registry},
     {group, membership},
     {group, capacity},
     {group, lifecycle}].

groups() ->
    [{registry, [sequence],
      [join_and_lookup,
       placement_is_phash2_sharded,
       rooms_lists_only_non_empty_rooms,
       peers_lists_the_whole_room]},
     {membership, [sequence],
      [a_rejoin_by_the_same_pid_is_idempotent,
       a_taken_peer_id_is_refused,
       leave_is_idempotent,
       leaving_an_empty_room_is_a_no_op]},
     {capacity, [sequence],
      [a_full_room_refuses_the_third_join,
       an_explicit_capacity_is_honoured,
       an_unknown_room_has_no_peers]},
     {lifecycle, [sequence],
      [an_orderly_disconnect_sends_left_to_the_remaining_members,
       a_killed_connection_still_sends_exactly_one_left,
       a_killed_connection_leaves_no_ghost_member]}].

%%%_* Registry --------------------------------------------------------------

join_and_lookup(_Config) ->
    {ok, A} = peer(<<"registry-room-1">>, <<"pa">>),
    {ok, B} = peer(<<"registry-room-1">>, <<"pb">>),
    ?assertEqual(A, wait_lookup(<<"registry-room-1">>, <<"pa">>)),
    ?assertEqual(B, wait_lookup(<<"registry-room-1">>, <<"pb">>)),
    ?assertEqual(undefined,
                 webrtc_room_registry:lookup(<<"registry-room-1">>, <<"nobody">>)),
    stop_peers([A, B]).

%% ADR-028 D4 / plan §1: `phash2(Room, Shards)' decides the shard, and the table
%% name is a pure function of the shard index. Asserting the placement directly
%% is what catches a "correct-looking" scheme that would put two rooms on one
%% shard and split a third across two.
%%
%% `room_shards' is read at start-up and cannot change while the shards are
%% running (plan §4.2: 改值必须重启), so this case uses the live shard count
%% instead of poking the env -- doing the latter would make the assertion pass
%% while invalidating the invariant it is supposed to protect.
placement_is_phash2_sharded(_Config) ->
    Shards = webrtc_room_registry:shard_count(),
    ?assert(Shards >= 1),
    ?assertEqual(length(webrtc_room_registry:shard_names()), Shards),
    Rooms = [<<"place-1">>, <<"place-2">>, <<"place-3">>, <<"place-4">>],
    _ = [begin
             {ok, Pid} = peer(R, <<"p-one">>),
             Shard = webrtc_room_registry:shard_for(R),
             %% Shards are numbered from 1, so the 0-based hash is shifted.
             ?assertEqual(Shard,
                          webrtc_room_shard:name(erlang:phash2(R, Shards) + 1)),
             ?assertEqual(true, is_process_alive(whereis(Shard))),
             Table = webrtc_room_registry:table_name(R),
             ?assertEqual(true, lists:member(Table, ets:all())),
             %% `ets:match/2' answers with the bound `$N' values, so the row for
             %% this room is found by binding the peer id position.
             ?assertEqual([[<<"p-one">>]],
                          ets:match(Table, {{R, '$1'}, Pid, '_'})),
             stop_peers([Pid])
         end || R <- Rooms],
    ok.

rooms_lists_only_non_empty_rooms(_Config) ->
    {ok, A} = peer(<<"rooms-room-a">>, <<"pa">>),
    {ok, B} = peer(<<"rooms-room-b">>, <<"pb">>),
    Rooms = webrtc_room_registry:rooms(),
    ?assertEqual(true, lists:member(<<"rooms-room-a">>, Rooms)),
    ?assertEqual(true, lists:member(<<"rooms-room-b">>, Rooms)),
    stop_peers([A, B]),
    %% An empty room is not listed: the room only exists while it has members.
    ?assertEqual(false, lists:member(<<"rooms-room-a">>,
                                     wait_rooms_without(<<"rooms-room-a">>))).

peers_lists_the_whole_room(_Config) ->
    {ok, A} = peer(<<"peers-room-1">>, <<"pa">>),
    {ok, B} = peer(<<"peers-room-1">>, <<"pb">>),
    Members = wait_peers(<<"peers-room-1">>),
    ?assertEqual([A, B], lists:sort([Pid || #{pid := Pid} <- Members])),
    ?assertEqual([<<"pa">>, <<"pb">>],
                 lists:sort([Id || #{peer_id := Id} <- Members])).

%%%_* Membership ------------------------------------------------------------

%% A re-join by the same connection is not a second member: `join/4' is what a
%% reconnect path calls, and doubling the row would make the room hit its
%% capacity with one client.
a_rejoin_by_the_same_pid_is_idempotent(_Config) ->
    {ok, A} = peer(<<"rejoin-room-1">>, <<"pa">>),
    {ok, Present} = webrtc_room_registry:join(<<"rejoin-room-1">>, <<"pa">>, A,
                                              #{username => <<"alice">>}),
    ?assertEqual([], Present),
    ?assertEqual(1, length(webrtc_room_registry:peers(<<"rejoin-room-1">>))).

%% R-S7 mints the peer_id server-side, so a collision "cannot happen"; the
%% refusal exists so the registry can never silently shadow a live peer.
a_taken_peer_id_is_refused(_Config) ->
    {ok, A} = peer(<<"taken-room-1">>, <<"pa">>),
    {ok, B} = peer(<<"taken-room-1">>, <<"pb">>),
    ?assertEqual({error, peer_id_taken},
                 webrtc_room_registry:join(<<"taken-room-1">>, <<"pa">>, B,
                                           #{username => <<"bob">>})),
    ?assertEqual(2, length(webrtc_room_registry:peers(<<"taken-room-1">>))),
    ?assertEqual(A, webrtc_room_registry:lookup(<<"taken-room-1">>, <<"pa">>)).

leave_is_idempotent(_Config) ->
    {ok, A} = peer(<<"leave-room-1">>, <<"pa">>),
    ?assertEqual(ok, webrtc_room_registry:leave(<<"leave-room-1">>, A)),
    ?assertEqual(ok, webrtc_room_registry:leave(<<"leave-room-1">>, A)),
    ?assertEqual([], webrtc_room_registry:peers(<<"leave-room-1">>)).

%% R-S9: `leave' runs in `terminate/2', so it must never raise -- not for a room
%% that never existed, and not for one that already emptied.
leaving_an_empty_room_is_a_no_op(_Config) ->
    ?assertEqual(ok, webrtc_room_registry:leave(<<"never-existed">>, self())),
    {ok, A} = peer(<<"leave-room-2">>, <<"pa">>),
    ?assertEqual(ok, webrtc_room_registry:leave(<<"leave-room-2">>, A)),
    ?assertEqual(ok, webrtc_room_registry:leave(<<"leave-room-2">>, A)),
    ?assertEqual([], webrtc_room_registry:peers(<<"leave-room-2">>)).

%%%_* Capacity --------------------------------------------------------------

%% The app default is 2 (1:1 first phase). The third join is refused and the
%% refused pid is not in the room.
a_full_room_refuses_the_third_join(_Config) ->
    Room = <<"capacity-room-1">>,
    {ok, A} = peer(Room, <<"pa">>),
    {ok, B} = peer(Room, <<"pb">>),
    Refused = spawn(fun() -> receive stop -> ok after 30000 -> ok end end),
    ?assertEqual({error, room_full},
                 webrtc_room_registry:join(Room, <<"pc">>, Refused,
                                           #{username => <<"carol">>})),
    Members = wait_peers(Room),
    ?assertEqual(2, length(Members)),
    ?assertEqual([A, B], lists:sort([Pid || #{pid := Pid} <- Members])),
    ?assertEqual(false,
                 lists:member(Refused, [Pid || #{pid := Pid} <- Members])).

%% The per-join capacity wins over the app default: with `max_peers => 3' the
%% third member is admitted, and the fourth is not.
an_explicit_capacity_is_honoured(_Config) ->
    Room = <<"capacity-room-2">>,
    Opts = #{username => <<"carol">>, max_peers => 3},
    {ok, A} = peer(Room, <<"pa">>),
    {ok, B} = peer(Room, <<"pb">>),
    Third = spawn(fun() -> receive stop -> ok after 30000 -> ok end end),
    ?assertEqual({ok, [<<"pa">>, <<"pb">>]},
                 webrtc_room_registry:join(Room, <<"pc">>, Third, Opts)),
    Fourth = spawn(fun() -> receive stop -> ok after 30000 -> ok end end),
    ?assertEqual({error, room_full},
                 webrtc_room_registry:join(Room, <<"pd">>, Fourth, Opts)),
    ?assertEqual(3, length(webrtc_room_registry:peers(Room))),
    ?assertEqual(A, webrtc_room_registry:lookup(Room, <<"pa">>)),
    ?assertEqual(B, webrtc_room_registry:lookup(Room, <<"pb">>)).

an_unknown_room_has_no_peers(_Config) ->
    ?assertEqual([], webrtc_room_registry:peers(<<"capacity-room-empty">>)),
    ?assertEqual(undefined,
                 webrtc_room_registry:member(<<"capacity-room-empty">>, <<"p">>)).

%%%_* Lifecycle -------------------------------------------------------------

%% The ordinary path: the client closes, `terminate/2' leaves the room, and the
%% remaining member is told once.
an_orderly_disconnect_sends_left_to_the_remaining_members(Config) ->
    Room = <<"lifecycle-room-1">>,
    {ok, A} = connect(Config, Room),
    {ok, B} = connect(Config, Room),
    PeerA = authenticate(A),
    PeerB = authenticate(B),
    #{<<"event">> := <<"joined">>} = recv_event(A, <<"joined">>),
    close(B),
    #{<<"event">> := <<"left">>,
      <<"data">> := #{<<"peer_id">> := PeerB}} = recv_event(A, <<"left">>),
    %% Only the leaver is gone: the remaining member is still present.
    ?assertEqual(undefined, webrtc_room_registry:lookup(Room, PeerB)),
    ?assertEqual([PeerA],
                 [Id || #{peer_id := Id} <- wait_peers(Room)]),
    close(A).

%% The property that separates this design from the upstream handler: a
%% connection killed with `exit(Pid, kill)' never runs `terminate/2', so the
%% `left' must come from the shard's monitor -- exactly once, and only to the
%% member that remains.
a_killed_connection_still_sends_exactly_one_left(Config) ->
    Room = <<"lifecycle-room-2">>,
    {ok, A} = connect(Config, Room),
    {ok, B} = connect(Config, Room),
    _ = authenticate(A),
    PeerB = authenticate(B),
    #{<<"event">> := <<"joined">>} = recv_event(A, <<"joined">>),
    kill(Room, PeerB),
    #{<<"event">> := <<"left">>,
      <<"data">> := #{<<"peer_id">> := PeerB}} = recv_event(A, <<"left">>),
    ?assertEqual(quiet, quiet(A)),
    close(A).

%% A dead member must not stay "present": once the shard reaps it, the room has
%% only the surviving member, and the room disappears from `rooms/0' once that
%% one leaves too.
a_killed_connection_leaves_no_ghost_member(Config) ->
    Room = <<"lifecycle-room-3">>,
    {ok, A} = connect(Config, Room),
    {ok, B} = connect(Config, Room),
    PeerA = authenticate(A),
    PeerB = authenticate(B),
    #{<<"event">> := <<"joined">>} = recv_event(A, <<"joined">>),
    kill(Room, PeerB),
    #{<<"event">> := <<"left">>} = recv_event(A, <<"left">>),
    ?assertEqual([PeerA], [Id || #{peer_id := Id} <- wait_peers(Room)]),
    close(A),
    ?assertEqual(false, lists:member(Room, wait_rooms_without(Room))).

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
    ok = application:set_env(broker_proto_webrtc, listeners,
                             [#{port => 0, num_acceptors => 4,
                                max_connections => 64}]),
    ok = application:set_env(broker_proto_webrtc, subprotocols, [?SUBPROTOCOL]),
    ok = application:set_env(broker_proto_webrtc, auth_timeout_ms, 5000),
    ok = application:set_env(broker_proto_webrtc, idle_timeout_ms, 60000),
    ok = application:set_env(broker_proto_webrtc, max_frame_bytes, 262144),
    ok = application:set_env(broker_proto_webrtc, max_peers_per_room, 2),
    {ok, _} = application:ensure_all_started(broker_security),
    {ok, _} = application:ensure_all_started(broker_proto_webrtc),
    [{port, listener_port()} | Config].

end_per_testcase(_Case, _Config) ->
    stop_app(),
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

%%%_* Registry helpers (API level, no sockets) -------------------------------

%% A fake member: a live process with no socket behind it. The registry only
%% cares that the pid is alive and monitorable, which is exactly what makes the
%% lifecycle properties testable without a listener.
peer(Room, PeerId) ->
    peer(Room, PeerId, <<"alice">>).

peer(Room, PeerId, Username) ->
    Pid = spawn(fun() -> receive stop -> ok after 30000 -> ok end end),
    case webrtc_room_registry:join(Room, PeerId, Pid,
                                   #{username => Username}) of
        {ok, _}    -> {ok, Pid};
        {error, R} -> exit({join_refused, R})
    end.

stop_peers(Pids) ->
    _ = [Pid ! stop || Pid <- Pids],
    wait_until_true(fun() -> not lists:any(fun is_process_alive/1, Pids) end),
    ok.

wait_lookup(Room, PeerId) ->
    wait_until(fun() -> webrtc_room_registry:lookup(Room, PeerId) end).

wait_peers(Room) ->
    wait_until(fun() -> webrtc_room_registry:peers(Room) end).

%% Wait until `Room' is no longer listed, then answer with the room list at that
%% moment (the room table drops empty rooms).
wait_rooms_without(Room) ->
    {ok, Rooms} = wait_until(fun() -> quiet_rooms(Room) end),
    Rooms.

quiet_rooms(Room) ->
    case lists:member(Room, webrtc_room_registry:rooms()) of
        true  -> false;
        %% A tuple, not the raw list: `wait_until/1' treats `[]' as "keep
        %% waiting", and an empty room table is exactly the answer here.
        false -> {ok, webrtc_room_registry:rooms()}
    end.

wait_until(Fun) -> wait_until(Fun, ?POLL_BUDGET_MS).

wait_until(_Fun, 0) -> exit(wait_until_timeout);
wait_until(Fun, Left) ->
    case Fun() of
        undefined -> timer:sleep(?POLL_MS), wait_until(Fun, Left - ?POLL_MS);
        false -> timer:sleep(?POLL_MS), wait_until(Fun, Left - ?POLL_MS);
        [] -> timer:sleep(?POLL_MS), wait_until(Fun, Left - ?POLL_MS);
        Other -> Other
    end.

wait_until_true(Fun) ->
    true = wait_until(Fun),
    ok.

%%%_* Socket helpers --------------------------------------------------------

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

%% Kill the connection process behind `PeerId', *without* a close frame, so the
%% only thing that can produce the `left' is the registry's monitor. The pid
%% comes from the registry itself -- no process-table scan, no guessing which
%% process owns which socket.
kill(Room, PeerId) ->
    Pid = case webrtc_room_registry:lookup(Room, PeerId) of
              undefined -> exit({peer_not_registered, Room, PeerId});
              P -> P
          end,
    exit(Pid, kill),
    wait_until_true(fun() -> not is_process_alive(Pid) end),
    gone = wait_gone(Room, PeerId),
    Pid.

wait_gone(Room, PeerId) ->
    wait_until(fun() ->
                       case webrtc_room_registry:lookup(Room, PeerId) of
                           undefined -> gone;
                           _ -> false
                       end
               end).

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

%% Nothing may arrive within the quiet window; a socket already torn down is
%% quiet too (it cannot deliver a stray event either).
quiet(S) ->
    case gen_tcp:recv(S, 0, ?QUIET_MS) of
        {error, timeout} -> quiet;
        {error, closed} -> quiet;
        {ok, Bin} -> {unexpected, Bin}
    end.
