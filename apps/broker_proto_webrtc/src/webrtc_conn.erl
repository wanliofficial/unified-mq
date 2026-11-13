%% @doc One WebRTC signaling connection (S10-B: rooms and signaling kernel).
%%
%% State machine, all in one process per connection:
%%
%%   handshake -> authenticating -> active
%%
%%   * `handshake' -- collect the RFC 6455 upgrade request head, take the room
%%     from the path (`/websocket/<room>', R-T1), then call
%%     `ws_handshake:accept/2'. A client that offers no common subprotocol is
%%     refused with HTTP 400, never silently upgraded (R-T6); so is a path that
%%     names no room.
%%   * `authenticating' -- the auth window (`auth_timeout_ms', default 5000)
%%     is armed. The only accepted frame is a text frame
%%     `{"event":"authenticate","data":{...}}'; anything else gets an
%%     `unauthorized' reply and the connection stays open for another try.
%%     Timeout -> close 1008.
%%   * `active' -- the connection is in its room. A text frame carrying `to' is
%%     relayed to that peer (`webrtc_dispatch'); a frame without `to' gets
%%     `invalid_message'. Text `ping' is answered with text `pong' at any point;
%%     a native opcode 9 is answered with opcode 10. binary -> close 1003.
%%     Idle window closes a silent connection with 1001.
%%
%% **What this module does not do**: it does not decide who is in a room. That
%% is `webrtc_room_registry', which owns membership, the `joined'/`left'
%% broadcasts and the member monitors. The connection only says "I am here" and
%% "I am gone"; a connection killed with `exit(Pid, kill)' still produces
%% exactly one `left' because the registry's monitor fires.
%%
%% ADR-028: nothing here produces a `#broker_message{}', calls
%% `broker_queue:' / `broker_router', or keeps cursor/retention state.
-module(webrtc_conn).
-behaviour(gen_server).

%% Public entry from webrtc_listener:start_link/3
-export([start_link/3]).
%% gen_server
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).
-define(DEFAULT_SUBPROTOCOLS, [<<"webrtc.v1">>]).
-define(DEFAULT_AUTH_TIMEOUT_MS, 5000).
-define(DEFAULT_IDLE_TIMEOUT_MS, 60000).
-define(DEFAULT_MAX_FRAME_BYTES, 262144).
-define(DEFAULT_MAX_PEERS_PER_ROOM, 2).

-record(st, {
          phase = handshake :: handshake | authenticating | active | stopping,
          transport :: module() | undefined,
          socket :: gen_tcp:socket() | ssl:sslsocket() | undefined,
          buffer = <<>> :: binary(),
          %% Fragmented data message being reassembled: `{Opcode, Chunks, Size}'.
          frag :: undefined | {1 | 2, [binary()], non_neg_integer()},
          subprotocol :: binary() | undefined,
          room :: binary() | undefined,
          principal :: binary() | undefined,
          tenant :: binary() | undefined,
          username :: binary() | undefined,
          peer_id :: binary() | undefined,
          auth_timer :: reference() | undefined,
          idle_timer :: reference() | undefined
         }).

%%%===================================================================
%%% Entry
%%%===================================================================

start_link(Ref, Transport, Opts) ->
    gen_server:start_link(?SERVER, {Ref, Transport, Opts}, []).

%%%===================================================================
%%% gen_server
%%%===================================================================

init({_Ref, Transport, _Opts}) ->
    process_flag(trap_exit, true),
    {ok, #st{transport = Transport}}.

handle_call(_Req, _From, St) ->
    {reply, {error, unsupported}, St}.

handle_cast(_Msg, St) ->
    {noreply, St}.

%% ranch transfers ownership and sends this. For TLS it is also the call that
%% completes the TLS handshake: ranch only does `transport_accept', so skipping
%% it leaves an sslsocket that rejects `setopts/2' with
%% `notsup_on_transport_accept_socket'.
handle_info({handshake, _Ref, RanchTransport, Socket, Timeout},
            #st{phase = handshake} = St) ->
    case RanchTransport:handshake(Socket, Timeout) of
        {ok, HSock} ->
            St1 = St#st{transport = RanchTransport, socket = HSock},
            case set_active(St1) of
                ok -> {noreply, St1};
                {error, Reason} -> {stop, {shutdown, {socket_error, Reason}}, St1}
            end;
        {error, Reason} ->
            %% A peer that cannot complete the TLS handshake (a plain TCP
            %% client on a wss port) never becomes a connection.
            {stop, {shutdown, {tls_handshake_failed, Reason}}, St}
    end;

handle_info({T, _S, Data}, #st{} = St) when T =:= tcp; T =:= ssl ->
    St1 = on_data(<<Data/binary>>, St),
    case St1#st.phase of
        stopping ->
            %% `stop_now/1' has already written the close frame and closed the
            %% socket; the process must now actually exit, or the room would
            %% keep the peer until some timeout and `terminate/2' would never
            %% run its `leave' (R-C2).
            {stop, normal, St1};
        _ ->
            %% `{active, once}' must be re-armed after every delivery, or the
            %% connection would receive exactly one chunk and then go quiet.
            case set_active(St1) of
                ok -> {noreply, St1};
                {error, Reason} -> {stop, {shutdown, {socket_error, Reason}}, St1}
            end
    end;

%% A relayed frame from a peer (or from `webrtc_server'). This is the whole
%% data plane: another connection already validated and addressed it.
handle_info({text, Bin}, #st{phase = active} = St) ->
    _ = send_frame(St, ws_frame:encode(1, Bin)),
    %% A signal from a same-room peer is session activity: do not close a
    %% call's responder just because it has nothing to say of its own.
    {noreply, reset_idle(St)};
handle_info({text, _Bin}, St) ->
    %% Arrives only after the room was left (a `left' broadcast racing the
    %% `leave'). Dropping it is correct: the socket is going away.
    {noreply, St};

handle_info(auth_timeout, #st{phase = authenticating} = St) ->
    %% R-C1: the design specifies close 1008 (policy violation) here, not a
    %% bare TCP close -- a signaling client must be able to tell "you were too
    %% slow to authenticate" from a network drop.
    {stop, normal, close_with(1008, St)};
handle_info(auth_timeout, St) ->
    {noreply, St};

handle_info(idle_timeout, #st{phase = active} = St) ->
    {stop, normal, close_with(1001, St)};
handle_info(idle_timeout, St) ->
    {noreply, St};

handle_info({Closed, _S}, #st{} = St) when Closed =:= tcp_closed;
                                          Closed =:= ssl_closed ->
    {stop, {shutdown, closed}, St};

handle_info({Error, _S, Reason}, #st{} = St) when Error =:= tcp_error;
                                                 Error =:= ssl_error ->
    {stop, {shutdown, {socket_error, Reason}}, St};

handle_info(_Info, St) ->
    {noreply, St}.

%% R-C2: leaving is idempotent and total. A connection killed from outside
%% never reaches here -- the registry's monitor covers that case -- but this is
%% the normal path, and it is the one that must not raise.
terminate(_Reason, #st{room = undefined}) ->
    ok;
terminate(_Reason, #st{room = Room, socket = undefined}) ->
    webrtc_room_registry:leave(Room, self()),
    ok;
terminate(_Reason, #st{room = Room, transport = T, socket = S}) ->
    webrtc_room_registry:leave(Room, self()),
    _ = T:close(S),
    ok.

code_change(_OldVsn, St, _Extra) -> {ok, St}.

%%%===================================================================
%%% Incoming bytes
%%%===================================================================

on_data(Data, #st{phase = handshake, buffer = Buf} = St) ->
    on_handshake(<<Buf/binary, Data/binary>>, St);
on_data(Data, #st{phase = P, buffer = Buf} = St)
  when P =:= authenticating; P =:= active ->
    on_frames(<<Buf/binary, Data/binary>>, St);
on_data(_Data, #st{phase = stopping} = St) ->
    St.

%%%===================================================================
%%% Handshake
%%%===================================================================

on_handshake(Bin, St) ->
    case binary:match(Bin, <<"\r\n\r\n">>) of
        nomatch ->
            case byte_size(Bin) > ws_handshake:max_handshake() of
                true ->
                    _ = send(St, <<"HTTP/1.1 431 Request Header Fields Too Large\r\n"
                                   "Connection: close\r\n\r\n">>),
                    stop_now(St);
                false ->
                    St#st{buffer = Bin}
            end;
        _ ->
            do_handshake(Bin, St)
    end.

do_handshake(Bin, St) ->
    case room_from_head(Bin) of
        {ok, Room} ->
            accept(Bin, Room, St);
        error ->
            %% R-T1: the room is the path binding; a request that names no
            %% room cannot be served, and guessing one would silently merge
            %% two unrelated sessions.
            _ = send(St, <<"HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n">>),
            stop_now(St)
    end.

accept(Bin, Room, St) ->
    case ws_handshake:accept(Bin, #{supported_subprotocols => subprotocols()}) of
        {ok, Resp, Sub} ->
            _ = send(St, iolist_to_binary(Resp)),
            St1 = St#st{buffer = <<>>, subprotocol = Sub, room = Room},
            case require_auth() of
                true ->
                    %% R-C1: the auth window is armed the moment the upgrade
                    %% completes, so an idle unauthenticated socket cannot sit
                    %% there forever.
                    arm_auth(St1#st{phase = authenticating});
                false ->
                    %% ADR-028 §D5 / broker_proto_webrtc_app:validate/0: only a
                    %% loopback bind may reach this branch.
                    go_active(anonymous(), St1)
            end;
        {error, Resp} ->
            _ = send(St, Resp),
            stop_now(St)
    end.

%% `/websocket/<room>' is the one documented shape (R-T1). Anything else is
%% refused rather than guessed at.
room_from_head(Head) ->
    case binary:split(Head, <<"\r\n">>) of
        [RequestLine | _] ->
            case binary:split(RequestLine, <<" ">>, [global]) of
                [<<"GET">>, Path, _Version | _] -> room_from_path(Path);
                _ -> error
            end;
        _ ->
            error
    end.

room_from_path(Path) ->
    Segments = [S || S <- binary:split(Path, <<"/">>, [global]), S =/= <<>>],
    case Segments of
        [<<"websocket">>, Room] -> {ok, Room};
        _ -> error
    end.

%%%===================================================================
%%% Frames
%%%===================================================================

on_frames(<<>>, St) ->
    St;
on_frames(Data, #st{} = St) ->
    case ws_frame:decode(Data, max_frame_bytes()) of
        {more, _Need} ->
            St#st{buffer = Data};
        {error, Reason} ->
            _ = send(St, ws_frame:encode(8, ws_frame:close_payload(
                                              close_code(Reason), <<>>))),
            stop_now(St);
        {ok, Frame, Rest} ->
            St1 = on_frame(Frame, St),
            case St1#st.phase of
                stopping -> St1;
                _ -> on_frames(Rest, St1#st{buffer = <<>>})
            end
    end.

%% ---- fragmentation ------------------------------------------------------
%%
%% `ws_frame:decode/2' validates one frame at a time; reassembly is the
%% connection's job (same split as `ws_conn'). A data frame with FIN=0 opens a
%% message, opcode 0 continues it, and the opcode of the *opening* frame decides
%% whether the reassembled message is text or binary.

on_frame(#{fin := false, opcode := Op, payload := P}, #st{} = St)
  when (Op =:= 1 orelse Op =:= 2), St#st.frag =:= undefined ->
    case byte_size(P) > max_frame_bytes() of
        true -> too_large(St);
        false -> reset_idle(St#st{frag = {Op, [P], byte_size(P)}})
    end;

on_frame(#{fin := false, opcode := 0, payload := P}, #st{frag = {Op, Acc, Size}} = St) ->
    NewSize = Size + byte_size(P),
    case NewSize > max_frame_bytes() of
        true -> too_large(St);
        false -> reset_idle(St#st{frag = {Op, [P | Acc], NewSize}})
    end;

on_frame(#{fin := true, opcode := 0, payload := P},
         #st{frag = {Op, Acc, Size}} = St) ->
    NewSize = Size + byte_size(P),
    case NewSize > max_frame_bytes() of
        true ->
            too_large(St);
        false ->
            Whole = iolist_to_binary(lists:reverse([P | Acc])),
            handle_message(Op, Whole, St#st{frag = undefined})
    end;

%% A continuation with no message in flight, and a *new* data frame while one is
%% still open, are both protocol violations.
on_frame(#{opcode := 0}, #st{frag = undefined} = St) ->
    protocol_error(1002, St);
on_frame(#{opcode := Op}, #st{frag = {_, _, _}} = St) when Op =:= 1; Op =:= 2 ->
    protocol_error(1002, St);

%% ---- ping / pong: two protocols, both supported -------------------------

on_frame(#{opcode := 1, payload := <<"ping">>}, #st{} = St) ->
    %% Upstream keepalive: a text frame whose payload is literally "ping".
    %% Answer with text "pong" (not JSON), per design §2.3.
    _ = send_frame(St, ws_frame:encode(1, <<"pong">>)),
    reset_idle(St);

on_frame(#{opcode := 9, payload := P}, #st{} = St) ->
    %% RFC 6455 §5.5.2 native ping: answer opcode 10, payload echoed.
    _ = send_frame(St, ws_frame:encode(10, P)),
    reset_idle(St);

on_frame(#{opcode := 10}, #st{} = St) ->
    %% A pong we did not ask for; harmless.
    reset_idle(St);

on_frame(#{opcode := 8, payload := Payload}, #st{} = St) ->
    %% Client close: echo the close frame with the same code, then stop. Not
    %% echoing leaves the client waiting for a reply that never comes; echoing
    %% the *client's* code (not a hard-coded 1000) is what RFC 6455 §5.5.1 asks
    %% for and what a browser's `onclose' reports.
    Code = close_code_of(Payload),
    _ = send_frame(St, ws_frame:encode(8, ws_frame:close_payload(Code, <<>>))),
    stop_now(St);

on_frame(#{opcode := 2, payload := Payload}, #st{} = St) ->
    %% R-T3: binary is a protocol decision, not a codec limitation. The
    %% default is to refuse with close 1003 (the upstream warns and ignores);
    %% `ignore' is an explicit opt-out for deployments that need it.
    case binary_frames() of
        ignore ->
            reset_idle(St);
        _ ->
            _ = byte_size(Payload),
            close_with(1003, St)
    end;

%% Any other text frame: a whole message in one frame.
on_frame(#{opcode := 1, payload := Payload}, #st{} = St) ->
    handle_message(1, Payload, St).

%% ---- whole messages ----------------------------------------------------

handle_message(1, <<"ping">>, #st{phase = Phase} = St)
  when Phase =:= authenticating; Phase =:= active ->
    %% A `ping` reassembled from fragments is still the text keepalive.
    _ = send_frame(St, ws_frame:encode(1, <<"pong">>)),
    reset_idle(St);
handle_message(1, Payload, #st{} = St) ->
    handle_text(Payload, St);
handle_message(2, _Payload, #st{} = St) ->
    %% A fragmented binary message: the same decision as a single-frame one.
    case binary_frames() of
        ignore -> reset_idle(St);
        _ -> close_with(1003, St)
    end.

handle_text(Payload, #st{phase = authenticating} = St) ->
    case decode_json(Payload) of
        #{<<"event">> := <<"authenticate">>, <<"data">> := Data}
          when is_map(Data) ->
            try_authenticate(Data, St);
        _ ->
            %% Pre-auth anything else: unauthorized, stay open for a retry.
            send_event(St, webrtc_signal:unauthorized()),
            St
    end;

handle_text(Payload, #st{phase = active} = St) ->
    case decode_json(Payload) of
        Message when is_map(Message) ->
            relay(Message, St);
        _ ->
            send_event(St, webrtc_signal:invalid_message()),
            reset_idle(St)
    end;

handle_text(_Payload, St) ->
    %% handshake / stopping phases do not take text frames
    St.

%% `to' is the only field the server reads; `event' and `data' are opaque.
relay(Message, St = #st{room = Room, peer_id = PeerId}) ->
    case acl(Room, St) of
        allow ->
            case webrtc_dispatch:deliver(Room, Message, PeerId) of
                ok -> reset_idle(St);
                {error, _Reason} ->
                    %% Unknown peer, peer in another room, missing/blank `to'
                    %% or `event'. One answer for all of them: the client is
                    %% told the message was not relayed, the connection stays
                    %% up (R-S5; upstream F-1 crashed the handler here).
                    send_event(St, webrtc_signal:invalid_message()),
                    reset_idle(St)
            end;
        deny ->
            %% No sixth event exists in the frozen set (R-S1), so a refusal
            %% reuses `invalid_message' -- no `data', no hint about what the
            %% ACL allows.
            send_event(St, webrtc_signal:invalid_message()),
            reset_idle(St)
    end.

%%%===================================================================
%%% Authentication
%%%===================================================================

try_authenticate(Data, #st{} = St) ->
    Name = maps:get(<<"username">>, Data, undefined),
    Secret = maps:get(<<"password">>, Data, undefined),
    case {Name, Secret} of
        {U, P} when is_binary(U), is_binary(P) ->
            case broker_security:authenticate_conn({user_pass, U, P}) of
                {ok, Identity} ->
                    go_active(Identity, St);
                {error, _Reason} ->
                    send_event(St, webrtc_signal:unauthorized()),
                    St
            end;
        _ ->
            send_event(St, webrtc_signal:unauthorized()),
            St
    end.

%% Join the room, then announce. Joining before answering is deliberate: a
%% join the ACL or `max_peers_per_room' refuses must *not* be followed by an
%% `authenticated' event that hands the client a peer_id it can never use.
go_active(#{principal := Principal, tenant_id := Tenant} = _Identity, St) ->
    Room = St#st.room,
    case acl(Room, Principal, Tenant) of
        deny ->
            send_event(St, webrtc_signal:unauthorized()),
            close_with(1008, St);
        allow ->
            PeerId = mint_peer_id(),
            Opts = #{username => Principal, max_peers => max_peers_per_room()},
            case webrtc_room_registry:join(Room, PeerId, self(), Opts) of
                {ok, _Present} ->
                    send_event(St, webrtc_signal:authenticated(PeerId)),
                    reset_idle(St#st{phase = active,
                                     peer_id = PeerId,
                                     principal = Principal,
                                     tenant = Tenant,
                                     username = Principal,
                                     auth_timer = disarm(St#st.auth_timer)});
                {error, Reason} ->
                    %% Room full (or the impossible peer-id collision). Same
                    %% refusal shape as the ACL one: a clear event + close,
                    %% never a silent half-join.
                    _ = Reason,
                    send_event(St, webrtc_signal:invalid_message()),
                    close_with(1008, St)
            end
    end.

%% ADR-028 D5: the resource shape is `action => signal' plus the room as an ACL
%% *pattern* subject only. `<<"$webrtc/room/...">>' never becomes a router
%% destination and never reaches a log body.
acl(Room, #st{principal = Principal, tenant = Tenant}) ->
    acl(Room, Principal, Tenant).

acl(Room, Principal, Tenant) ->
    broker_security:authorize(Principal, #{action => signal,
                                           tenant_id => Tenant,
                                           destination => <<"$webrtc/room/",
                                                            Room/binary>>}).

%% R-S7: the server mints the peer_id; a client can never choose its own.
%% base64(strong_rand_bytes(10)) -> 16 chars, the same shape as the upstream
%% sample (`bxCBrwyL3Ar7Nw==' is base64 of 10 random bytes).
-spec mint_peer_id() -> binary().
mint_peer_id() ->
    base64:encode(crypto:strong_rand_bytes(10)).

%% The identity an auth-free listener runs as. `require_auth => false' is only
%% legal on a loopback bind (validated at boot), so this never runs on a wide
%% bind.
%%
%% `tenant_id => undefined' on purpose: with no credential there is no identity,
%% so there is no tenant to isolate *within* -- the ACL by principal
%% (`<<"anonymous">>') is the only gate that exists, and pretending to a tenant
%% would only make `authorize/2' deny every call (`broker_security' derives a
%% principal's tenant from its user record, and an anonymous principal has none).
%% This is the same posture `mcp_dispatch' takes.
anonymous() ->
    #{principal => <<"anonymous">>,
      tenant_id => undefined}.

%%%===================================================================
%%% Timers
%%%===================================================================

arm_auth(#st{auth_timer = undefined} = St) ->
    St#st{auth_timer = start(auth_timeout_ms(), auth_timeout)};
arm_auth(St) ->
    St.

reset_idle(#st{idle_timer = Old} = St) ->
    _ = cancel(Old),
    St#st{idle_timer = start(idle_timeout_ms(), idle_timeout)}.

%% `send_after/3' (not `start_timer/3'): `start_timer' delivers
%% `{timeout, Ref, Msg}', which the `handle_info' clauses above do not match,
%% so a wrong choice here makes both windows silently no-ops.
%%
%% `infinity' is a legal value for `idle_timeout_ms' (the design says so) and
%% means "no idle timer at all", not "a timer that never fires".
-spec start(pos_integer() | infinity, atom()) -> reference() | undefined.
start(infinity, _Msg) -> undefined;
start(Ms, Msg) when is_integer(Ms), Ms > 0 ->
    erlang:send_after(Ms, self(), Msg).

%% `cancel/1' and `disarm/1' exist to keep the record fields typed: the timer
%% APIs return `ok | false | non_neg_integer()', none of which is a valid
%% `timer_ref()'. Only the ref is stored, so the return values are dropped.
-spec cancel(undefined | reference()) -> ok.
cancel(undefined) -> ok;
cancel(Ref) -> _ = erlang:cancel_timer(Ref), ok.

-spec disarm(undefined | reference()) -> undefined.
disarm(undefined) -> undefined;
disarm(Ref) -> _ = erlang:cancel_timer(Ref), undefined.

%%%===================================================================
%%% Socket helpers
%%%===================================================================

send(#st{transport = T, socket = S}, Data) ->
    T:send(S, Data).

send_frame(#st{transport = T, socket = S}, Frame) ->
    T:send(S, Frame).

%% All server events are text frames (design §2.3); there is no binary event in
%% the frozen five, so there is no binary encoder here.
send_event(St, {text, Bin}) ->
    send_frame(St, ws_frame:encode(1, Bin)).

set_active(#st{transport = T, socket = S}) ->
    %% `{active, once}' keeps a slow peer from filling our mailbox; every
    %% inbound message re-arms it.
    T:setopts(S, [{active, once}, {packet, raw}, binary]).

%% Protocol violation from our own frame loop (fragmentation rules).
protocol_error(Code, St) ->
    close_with(Code, St).

too_large(St) ->
    close_with(1009, St).

close_with(Code, St) ->
    _ = send_frame(St, ws_frame:encode(8, ws_frame:close_payload(Code, <<>>))),
    stop_now(St).

stop_now(#st{transport = T, socket = S} = St) ->
    _ = case S of
            undefined -> ok;
            _ -> T:close(S)
        end,
    St#st{phase = stopping, socket = undefined, buffer = <<>>, frag = undefined}.

%%%===================================================================
%%% Config
%%%===================================================================

subprotocols() ->
    case application:get_env(broker_proto_webrtc, subprotocols,
                             ?DEFAULT_SUBPROTOCOLS) of
        [] -> ?DEFAULT_SUBPROTOCOLS;
        List when is_list(List) -> List;
        _ -> ?DEFAULT_SUBPROTOCOLS
    end.

auth_timeout_ms() ->
    application:get_env(broker_proto_webrtc, auth_timeout_ms,
                        ?DEFAULT_AUTH_TIMEOUT_MS).

idle_timeout_ms() ->
    application:get_env(broker_proto_webrtc, idle_timeout_ms,
                        ?DEFAULT_IDLE_TIMEOUT_MS).

max_frame_bytes() ->
    application:get_env(broker_proto_webrtc, max_frame_bytes,
                        ?DEFAULT_MAX_FRAME_BYTES).

max_peers_per_room() ->
    application:get_env(broker_proto_webrtc, max_peers_per_room,
                        ?DEFAULT_MAX_PEERS_PER_ROOM).

binary_frames() ->
    case application:get_env(broker_proto_webrtc, binary_frames, reject) of
        ignore -> ignore;
        _ -> reject
    end.

%% ADR-028 §D5: this app's own switch, never `broker_security.require_auth'
%% (which is a list of protocol names).
require_auth() ->
    application:get_env(broker_proto_webrtc, require_auth, true).

%%%===================================================================
%%% Wire helpers
%%%===================================================================

%% @doc Map a `ws_frame:decode/2' error to a close code.
%%
%% 1009 (message too big) is the one error that needs its own code; every other
%% decode error is a protocol violation and closes with 1002. The catch-all is
%% deliberate: `ws_frame:decode/2' is another app's function, and a future
%% error variant must still produce a clean 1002 close instead of a
%% `function_clause' crash inside the connection process.
-spec close_code(term()) -> 1002 | 1009.
close_code({payload_too_large, _, _}) -> 1009;
close_code(_) -> 1002.

%% Echo a client's close code, but never echo a reserved/invalid one: RFC 6455
%% §7.4.1 forbids sending 1005/1006/1015 and codes outside 1000..4999 on the
%% wire, so those become 1000 (normal).
close_code_of(<<Code:16, _/binary>>) when Code >= 1000, Code =< 4999,
                                          Code =/= 1005, Code =/= 1006,
                                          Code =/= 1015 ->
    Code;
close_code_of(_) ->
    1000.

decode_json(Bin) ->
    try json:decode(Bin) of
        M when is_map(M) -> M;
        _ -> not_a_map
    catch _:_ -> invalid_json
    end.
