%% @doc One WebRTC signaling connection (S10-A: connection base only).
%%
%% State machine, all in one process per connection:
%%
%%   handshake -> authenticating -> active
%%
%%   * `handshake' -- collect the RFC 6455 upgrade request head, then call
%%     `ws_handshake:accept/2'. A client that offers no common subprotocol is
%%     refused with HTTP 400, never silently upgraded (R-T6).
%%   * `authenticating' -- the auth window (`auth_timeout_ms', default 5000)
%%     is armed. The only accepted frame is a text frame
%%     `{"event":"authenticate","data":{...}}'; anything else gets an
%%     `unauthorized' reply and the connection stays open for another try.
%%     Timeout -> close.
%%   * `active' -- text frames are JSON messages; S10-A cannot route `to' yet
%%     (that is S10-B's room registry), so a message with `to' is answered
%%     `invalid_message' honestly rather than silently dropped. Text `ping' is
%%     answered with text `pong' at any point; a native opcode 9 is answered
%%     with opcode 10. binary -> close 1003. Idle window closes a silent
%%     connection.
%%
%% ADR-028: nothing here produces a `#broker_message{}', calls
%% `broker_queue:' / `broker_router', or keeps cursor/retention state. The
%% `broker_boundary_SUITE' guards that with a source-level grep.
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

-record(st, {
          phase = handshake :: handshake | authenticating | active | stopping,
          transport :: module() | undefined,
          socket :: gen_tcp:socket() | ssl:sslsocket() | undefined,
          buffer = <<>> :: binary(),
          subprotocol :: binary() | undefined,
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
            {noreply, St1};
        _ ->
            %% `{active, once}' must be re-armed after every delivery, or the
            %% connection would receive exactly one chunk and then go quiet.
            case set_active(St1) of
                ok -> {noreply, St1};
                {error, Reason} -> {stop, {shutdown, {socket_error, Reason}}, St1}
            end
    end;

handle_info(auth_timeout, #st{phase = authenticating} = St) ->
    %% R-C1: the design specifies close 1008 (policy violation) here, not a
    %% bare TCP close -- a signaling client must be able to tell "you were too
    %% slow to authenticate" from a network drop.
    _ = send_frame(St, ws_frame:encode(8, ws_frame:close_payload(1008, <<>>))),
    stop_now(St);
handle_info(auth_timeout, St) ->
    {noreply, St};

handle_info(idle_timeout, #st{phase = active} = St) ->
    _ = send_frame(St, ws_frame:encode(8, ws_frame:close_payload(1001, <<>>))),
    stop_now(St);
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

terminate(_Reason, #st{socket = undefined}) ->
    ok;
terminate(_Reason, #st{transport = T, socket = S}) ->
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
    case ws_handshake:accept(Bin, #{supported_subprotocols => subprotocols()}) of
        {ok, Resp, Sub} ->
            _ = send(St, iolist_to_binary(Resp)),
            St1 = St#st{buffer = <<>>, subprotocol = Sub},
            case require_auth() of
                true ->
                    %% R-C1: the auth window is armed the moment the upgrade
                    %% completes, so an idle unauthenticated socket cannot sit
                    %% there forever.
                    arm_auth(St1#st{phase = authenticating});
                false ->
                    %% ADR-028 §D5 / broker_proto_webrtc_app:validate/0: only a
                    %% loopback bind may reach this branch.
                    go_active(St1)
            end;
        {error, Resp} ->
            _ = send(St, Resp),
            stop_now(St)
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

on_frame(#{opcode := 2, payload := Payload}, #st{} = St) ->
    %% R-T3: binary is a protocol decision, not a codec limitation. The
    %% default is to refuse with close 1003 (the upstream warns and ignores);
    %% `ignore' is an explicit opt-out for deployments that need it.
    case binary_frames() of
        ignore ->
            reset_idle(St);
        _ ->
            _ = byte_size(Payload),
            _ = send_frame(St, ws_frame:encode(8, ws_frame:close_payload(1003, <<>>))),
            stop_now(St)
    end;

on_frame(#{opcode := 8}, #st{} = St) ->
    %% Client close: stop. ranch closes the socket.
    stop_now(St);

on_frame(#{opcode := 1, payload := Payload}, #st{phase = Phase} = St) ->
    handle_text(Payload, St#st{phase = Phase});

on_frame(#{opcode := Op}, #st{} = St) ->
    %% Continuations and reserved opcodes: the codec already rejected the
    %% illegal ones; anything reaching here is ignored, never crashes.
    _ = Op,
    reset_idle(St).

%% ---- text messages -----------------------------------------------------

handle_text(Payload, #st{phase = authenticating} = St) ->
    case decode_json(Payload) of
        #{<<"event">> := <<"authenticate">>, <<"data">> := Data}
          when is_map(Data) ->
            try_authenticate(Data, St);
        _ ->
            %% Pre-auth anything else: unauthorized, stay open for a retry.
            _ = send_frame(St, ws_frame:encode(1, event_json(unauthorized))),
            St
    end;

handle_text(Payload, #st{phase = active} = St) ->
    case decode_json(Payload) of
        #{<<"to">> := To} when is_binary(To) ->
            %% S10-A has no room registry, so `to' cannot be honoured yet.
            %% Answer honestly instead of pretending the message was routed.
            _ = send_frame(St, ws_frame:encode(1, event_json(invalid_message))),
            reset_idle(St);
        _ ->
            _ = send_frame(St, ws_frame:encode(1, event_json(invalid_message))),
            reset_idle(St)
    end;

handle_text(_Payload, St) ->
    %% handshake / stopping phases do not take text frames
    St.

%% ---- authentication ----------------------------------------------------

try_authenticate(Data, #st{} = St) ->
    Name = maps:get(<<"username">>, Data, undefined),
    Secret = maps:get(<<"password">>, Data, undefined),
    case {Name, Secret} of
        {U, P} when is_binary(U), is_binary(P) ->
            case broker_security:authenticate_conn({user_pass, U, P}) of
                {ok, #{principal := _Principal, tenant_id := _Tenant}} ->
                    go_active(St);
                {error, _Reason} ->
                    _ = send_frame(St, ws_frame:encode(1, event_json(unauthorized))),
                    St
            end;
        _ ->
            _ = send_frame(St, ws_frame:encode(1, event_json(unauthorized))),
            St
    end.

%% A connection that is allowed to run without credentials (only accepted on a
%% loopback bind, see broker_proto_webrtc_app:validate/0) goes straight to
%% `active' at handshake time, and every later text frame is treated the same
%% way an authenticated one is.
go_active(St) ->
    PeerId = mint_peer_id(),
    _ = send_frame(St, ws_frame:encode(1, event_json(
                                     authenticated, #{peer_id => PeerId}))),
    %% The auth window ends at the moment of success: a leftover `auth_timeout'
    %% would stop an authenticated connection for no reason.
    reset_idle(St#st{phase = active, peer_id = PeerId,
                     auth_timer = disarm(St#st.auth_timer)}).

%% R-S7: the server mints the peer_id; a client can never choose its own.
%% base64(strong_rand_bytes(10)) -> 16 chars, the same shape as the upstream
%% sample (`bxCBrwyL3Ar7Nw==` is base64 of 10 random bytes).
-spec mint_peer_id() -> binary().
mint_peer_id() ->
    base64:encode(crypto:strong_rand_bytes(10)).

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

set_active(#st{transport = T, socket = S}) ->
    %% `{active, once}' keeps a slow peer from filling our mailbox; every
    %% inbound message re-arms it.
    T:setopts(S, [{active, once}, {packet, raw}, binary]).

stop_now(#st{transport = T, socket = S} = St) ->
    _ = case S of
            undefined -> ok;
            _ -> T:close(S)
        end,
    St#st{phase = stopping, socket = undefined, buffer = <<>>}.

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
%%% Wire format (design §2.3)
%%%===================================================================

%% @doc `text_event(Event)' -- no `data' field (unauthorized, invalid_message).
event_json(Event) when is_atom(Event) ->
    json:encode(#{event => Event}).

%% @doc `text_event(Event, Data)' -- with `data' (authenticated).
event_json(Event, Data) when is_atom(Event), is_map(Data) ->
    json:encode(#{event => Event, data => Data}).

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

decode_json(Bin) ->
    try json:decode(Bin) of
        M when is_map(M) -> M;
        _ -> not_a_map
    catch _:_ -> invalid_json
    end.
