%% @doc The WebSocket layer of MQTT over WebSocket (MQTT 3.1.1 §6, MQTT 5.0 §6).
%%
%% Two processes per connection, on purpose:
%%
%%   * this one owns the socket, performs the RFC 6455 opening handshake and
%%     turns frames into a byte stream;
%%   * `mqtt_conn_fsm' underneath it is the MQTT state machine, started through
%%     `mqtt_conn_sup' with `transport => mqtt_transport_ws'. Its transport
%%     *handle* is this process's pid.
%%
%% The alternative -- letting the FSM own the socket and decode frames inside
%% `mqtt_transport_ws:decode_msg/2' -- cannot work: `decode_msg/2' is a pure
%% function of `(Msg, Handle)', while half a frame and "has the HTTP upgrade
%% completed yet" are per-connection state. Putting that in the process
%% dictionary or ETS would trade a clean behaviour for hidden state.
%%
%% What MQTT requires from this layer (design §7.1):
%%
%%   * the subprotocol must be negotiated (`mqtt', and the `mqttv3.1' /
%%     `mqttv3.1.1' spellings clients still send); a client that offers none, or
%%     none we know, is refused with HTTP 400 rather than upgraded;
%%   * only **binary** frames carry MQTT -- a text frame is a protocol error and
%%     is answered with close 1003;
%%   * packets and frames are NOT aligned: one frame may carry several MQTT
%%     packets, one packet may span frames. This layer therefore forwards frame
%%     payload bytes verbatim and lets the FSM's own `buf' do all reassembly;
%%   * ping/pong/close are WebSocket control traffic, independent of MQTT
%%     keepalive.
%%
%% A peer-caused failure (TCP reset, a TLS handshake that never completes, a
%% failed re-arm) stops this process with reason `normal' and hands
%% `{mqtt_ws_error, self(), Reason}' to the engine, which logs it once. Exiting
%% non-normally would instead print a supervisor report for every broken TCP
%% connection -- and a reset peer is routine, not a broker fault.
%%
%% Backpressure is deliberately absent. This process re-arms its socket after
%% every delivery instead of waiting for the FSM to ask for more, which is why
%% `mqtt_transport_ws:arm_once/1' is a no-op. A frame is handed to the FSM as one
%% message and consumed in the same scheduler slot, so ack-per-frame would buy
%% nothing but latency; `max_inflight' / `receive_maximum' remain the only
%% window control, exactly as on TCP.
-module(mqtt_ws_conn).

-behaviour(gen_server).

-export([start_link/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(st, {
    ref :: term(),
    %% ranch's own transport module (`ranch_tcp' / `ranch_ssl'): the acceptor
    %% layer, not our `mqtt_transport'.
    transport :: module() | undefined,
    socket :: term() | undefined,
    phase = handshake :: handshake | mqtt,
    buffer = <<>> :: binary(),
    fsm :: pid() | undefined,
    selected :: binary() | undefined,
    frag = undefined :: undefined | {0 | 1 | 2, [binary()]},
    %% A socket-level failure (reset, TLS handshake refused, arm failed). It is
    %% recorded instead of becoming this process's stop reason: a peer that
    %% resets a connection is a fact of life, and a non-`normal' exit here would
    %% print a supervisor report per broken TCP connection. The detail is handed
    %% to the engine instead, which logs it once, exactly like the socket
    %% transport's `{tcp_error, ...}'.
    error = undefined :: term() | undefined
}).

%%%_* Lifecycle --------------------------------------------------------------

start_link(Ref, Transport, Opts) ->
    gen_server:start_link(?MODULE, {Ref, Transport, Opts}, []).

init({Ref, Transport, _Opts}) ->
    %% The MQTT FSM is linked to us, and a link is not enough: on a `normal'
    %% exit it would keep running with no peer to talk to. Trapping lets
    %% `terminate/2' notify the FSM and lets us shut down in order.
    process_flag(trap_exit, true),
    {ok, #st{ref = Ref, transport = Transport}}.

terminate(_Reason, #st{} = St) ->
    %% Tell the FSM the carrier is gone so it stops through its ordinary
    %% shutdown path (which is what publishes the Will). Sending to a dead pid is
    %% harmless: the FSM may have initiated this shutdown itself.
    notify_fsm(St, closing_msg(St)),
    case St#st.socket of
        undefined -> ok;
        Sock -> _ = (catch (St#st.transport):close(Sock)), ok
    end.

%%%_* gen_server -------------------------------------------------------------

handle_call(_Req, _From, St) ->
    {reply, {error, unsupported}, St}.

handle_cast(_Msg, St) ->
    {noreply, St}.

%% ranch transfers socket ownership and then sends this. For TLS it is also the
%% call that completes the TLS handshake: ranch only does `transport_accept', so
%% skipping it leaves an sslsocket that rejects `ssl:setopts/2' with
%% `notsup_on_transport_accept_socket'.
handle_info({handshake, _Ref, RanchTransport, Socket, Timeout},
            #st{phase = handshake} = St) ->
    case RanchTransport:handshake(Socket, Timeout) of
        {ok, HSock} ->
            St1 = St#st{transport = RanchTransport, socket = HSock},
            case set_active(St1) of
                ok -> {noreply, St1};
                {error, Reason} -> stop_with_error({socket_error, Reason}, St1)
            end;
        {error, Reason} ->
            %% A peer that cannot complete the TLS handshake (a plain TCP client
            %% on a wss port, an unsupported cipher) never becomes a connection.
            stop_with_error({tls_handshake_failed, Reason}, St#st{socket = Socket})
    end;

%% Client bytes, before or after the upgrade.
handle_info({tcp, Socket, Data}, #st{socket = Socket} = St) ->
    on_data(Data, St);
handle_info({ssl, Socket, Data}, #st{socket = Socket} = St) ->
    on_data(Data, St);

handle_info({tcp_closed, Socket}, #st{socket = Socket} = St) ->
    {stop, normal, St};
handle_info({ssl_closed, Socket}, #st{socket = Socket} = St) ->
    {stop, normal, St};
handle_info({tcp_error, Socket, Reason}, #st{socket = Socket} = St) ->
    stop_with_error({socket_error, Reason}, St);
handle_info({ssl_error, Socket, Reason}, #st{socket = Socket} = St) ->
    stop_with_error({socket_error, Reason}, St);

%% Requests from `mqtt_transport_ws' (the FSM is the sender, our pid the handle).
handle_info({mqtt_ws_send, Bin}, St) ->
    %% Sending does not touch the read side, so no re-arm here.
    {noreply, send_frame(2, [Bin], St)};
handle_info({mqtt_ws_arm, _Fsm}, St) ->
    %% See the module doc: no per-frame backpressure on this transport.
    rearm_or_stop(St);
handle_info(mqtt_ws_close, St) ->
    _ = send_close(1000, <<>>, St),
    {stop, normal, St};

%% The MQTT FSM died. Nothing is left to carry, and keeping the socket open
%% would let a client believe its session survived.
handle_info({'EXIT', Fsm, Reason}, #st{fsm = Fsm} = St) when is_pid(Fsm) ->
    {stop, {fsm_exit, Reason}, St#st{fsm = undefined}};

handle_info(_Other, St) ->
    {noreply, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%%%_* Input ----------------------------------------------------------------

on_data(Data, #st{phase = Phase} = St) ->
    Buffer = <<(St#st.buffer)/binary, Data/binary>>,
    Result =
        case Phase of
            handshake -> handshake(Buffer, St#st{buffer = Buffer});
            mqtt -> parse_frames(Buffer, St)
        end,
    case Result of
        {noreply, St1} -> rearm_or_stop(St1);
        Other -> Other
    end.

%%%_* Handshake ------------------------------------------------------------

handshake(Buffer, #st{} = St) ->
    case byte_size(Buffer) > ws_handshake:max_handshake() of
        true ->
            _ = raw_send(<<"HTTP/1.1 431 Request Header Fields Too Large\r\n"
                            "Connection: close\r\n\r\n">>, St),
            {stop, normal, St};
        false ->
            handshake_split(Buffer, St)
    end.

handshake_split(Buffer, St) ->
    case binary:split(Buffer, <<"\r\n\r\n">>) of
        [Head, Rest] ->
            Opts = #{supported_subprotocols => subprotocols()},
            case ws_handshake:accept(Head, Opts) of
                {ok, Response, Selected} ->
                    _ = raw_send(Response, St),
                    St1 = St#st{phase = mqtt, buffer = <<>>, selected = Selected},
                    case start_fsm(St1) of
                        {ok, St2} -> parse_frames(Rest, St2);
                        {error, Reason, St2} -> {stop, Reason, St2}
                    end;
                {error, Response} ->
                    %% No common subprotocol, no upgrade: 400 verbatim, close.
                    _ = raw_send(Response, St),
                    {stop, normal, St}
            end;
        _ ->
            %% The HTTP request is still arriving.
            {noreply, St}
    end.

%% The MQTT FSM keeps its own session table, so it is started under
%% `mqtt_conn_sup' (not linked to ranch directly) and then linked here. The
%% `connect' message is the same handoff the CT helper and ranch's
%% `{handshake, ...}' use; our pid is its transport handle.
start_fsm(#st{} = St) ->
    Opts = #{transport => mqtt_transport_ws},
    case mqtt_conn_sup:start_conn(self(), Opts) of
        {ok, Pid} ->
            true = link(Pid),
            Pid ! connect,
            {ok, St#st{fsm = Pid}};
        {error, Reason} ->
            {error, {fsm_start_failed, Reason}, St}
    end.

%%%_* Frames ---------------------------------------------------------------

parse_frames(Buffer, St) ->
    case ws_frame:decode(Buffer, max_frame_bytes()) of
        {ok, Frame, Rest} ->
            case handle_frame(Frame, St) of
                {keep, St1} -> parse_frames(Rest, St1);
                {stop, Reason, St1} -> {stop, Reason, St1}
            end;
        {more, _Need} ->
            {noreply, St#st{buffer = Buffer}};
        {error, Reason} ->
            _ = send_close(1002, reason_bin(Reason), St),
            {stop, {frame_error, Reason}, St}
    end.

handle_frame(#{opcode := 8, payload := Payload}, St) ->
    Code = ws_frame:close_code(Payload),
    _ = send_close(Code, <<>>, St),
    {stop, normal, close_fsm(St)};
handle_frame(#{opcode := 9, payload := Payload}, St) ->
    {keep, send_frame(10, [Payload], St)};
handle_frame(#{opcode := 10, payload := _Pong}, St) ->
    {keep, St};
%% Only binary data frames carry MQTT. A text frame is not "JSON we should
%% parse anyway": it is a protocol error the spec answers with 1003.
handle_frame(#{opcode := 1}, St) ->
    _ = send_close(1003, <<"text frames are not MQTT">>, St),
    {stop, normal, close_fsm(St)};
handle_frame(#{fin := false, opcode := Op, payload := Payload}, St)
  when Op =:= 2; Op =:= 0 ->
    case St#st.frag of
        undefined when Op =:= 2 ->
            {keep, St#st{frag = {2, [Payload]}}};
        {2, Acc} when Op =:= 0 ->
            {keep, St#st{frag = {2, [Payload | Acc]}}};
        _ ->
            _ = send_close(1002, <<"bad continuation">>, St),
            {stop, {fragment_error, unexpected_continuation}, close_fsm(St)}
    end;
handle_frame(#{fin := true, opcode := 0, payload := Payload}, #st{frag = {2, Acc}} = St) ->
    Whole = iolist_to_binary(lists:reverse([Payload | Acc])),
    {keep, deliver(Whole, St#st{frag = undefined})};
handle_frame(#{fin := true, opcode := 2, payload := Payload}, #st{frag = undefined} = St) ->
    {keep, deliver(Payload, St)};
handle_frame(#{opcode := Op}, St) ->
    _ = send_close(1002, <<"unexpected opcode">>, St),
    {stop, {unexpected_opcode, Op}, close_fsm(St)}.

%% Frame payload -> MQTT byte stream. No packet alignment is assumed or
%% enforced here; the FSM's buffer is the only reassembly point.
deliver(Bin, #st{fsm = Fsm} = St) when is_pid(Fsm) ->
    Fsm ! {mqtt_ws_data, self(), Bin},
    St;
deliver(_Bin, St) ->
    St.

close_fsm(#st{fsm = Fsm} = St) when is_pid(Fsm) ->
    Fsm ! {mqtt_ws_closed, self()},
    St#st{fsm = undefined};
close_fsm(St) ->
    St.

%%%_* Wire helpers ---------------------------------------------------------

send_frame(Op, Payload, #st{socket = undefined} = St) ->
    _ = {Op, Payload},
    St;
send_frame(Op, Payload, St) ->
    _ = raw_send(ws_frame:encode(Op, Payload), St),
    St.

%% Server-to-client frames are unmasked (RFC 6455 §5.1).
send_close(Code, Reason, St) ->
    _ = raw_send(ws_frame:encode(8, ws_frame:close_payload(Code, Reason)), St),
    ok.

raw_send(IoData, #st{socket = undefined}) ->
    _ = IoData,
    ok;
raw_send(IoData, #st{transport = T, socket = S}) ->
    T:send(S, IoData).

set_active(#st{transport = T, socket = S}) ->
    T:setopts(S, [{active, once}]).

rearm_or_stop(St) ->
    case set_active(St) of
        ok -> {noreply, St};
        {error, Reason} -> stop_with_error({socket_error, Reason}, St)
    end.

%% Peer-caused failures stop `normal' and carry the detail to the engine.
stop_with_error(Reason, St) ->
    {stop, normal, St#st{error = Reason}}.

closing_msg(#st{error = undefined}) -> {mqtt_ws_closed, self()};
closing_msg(#st{error = Reason}) -> {mqtt_ws_error, self(), Reason}.

notify_fsm(#st{fsm = Fsm}, Msg) when is_pid(Fsm) ->
    Fsm ! Msg,
    ok;
notify_fsm(_St, _Msg) ->
    ok.

%%%_* Config --------------------------------------------------------------

subprotocols() ->
    application:get_env(broker_proto_mqtt_ws, subprotocols,
                        [<<"mqtt">>, <<"mqttv3.1">>, <<"mqttv3.1.1">>]).

max_frame_bytes() ->
    application:get_env(broker_proto_mqtt_ws, max_frame_bytes, 16777216).

reason_bin(Reason) -> iolist_to_binary(io_lib:format("~p", [Reason])).
