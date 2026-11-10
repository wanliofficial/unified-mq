%% @doc One QUIC connection, and the streams it carries.
%%
%% A QUIC connection outlives its streams (CN Draft 3 §2.1): closing the stream
%% ends one MQTT Network Connection, not the QUIC connection, and the client may
%% open a new stream and CONNECT again. So somebody has to stay alive between the
%% two, own the connection handle, and take each new stream. That is this
%% process. It owns nothing else:
%%
%%   * it does **not** read MQTT bytes. The stream handle is handed to
%%     `mqtt_conn_fsm' (through `mqtt_conn_sup'), so the FSM is the stream's
%%     owner and receives `{quic, Bin, Stream, Props}' directly -- the same
%%     ownership shape as TCP (`FSM owns socket') and WS (`FSM owns pid');
%%   * it does **not** complete the TLS handshake before the handoff. The server
%%     side must call `quicer:handshake/2' explicitly (`quicer:accept/3' only
%%     reports the transport-level connection), and the `{quic, connected, ...}'
%%     reply is delivered to the connection's **owner** -- so the handshake can
%%     only run here *after* the listener has transferred the handle.
%%
%% The orphan-stream path is the normal one, not an edge case: a real client
%% opens its stream immediately after `connect', i.e. before this process has
%% enqueued any stream acceptor. quicer then delivers
%% `{quic, new_stream, Stream, #{is_orphan := true}}' to the connection owner and
%% leaves the stream **passive**. Two things follow, and both are load-bearing:
%%
%%   * `accept_stream/3' is useless here -- it queues an acceptor for a stream
%%     that has already arrived. This process handles `new_stream' itself;
%%   * the FSM's first `arm_once/1' (`setopt(Stream, active, 1)') is what
%%     switches the stream from passive to active and releases the bytes already
%%     pinned in its receive chain. No re-arm before the handoff, or that data
%%     would arrive in the wrong process.
%%
%% A handshake failure is routine (a plain client probing the UDP port, a wrong
%% ALPN), so this process stops with `normal' and logs once, rather than letting
%% the supervisor treat a broken peer as a broker fault.
-module(mqtt_quic_conn).

-behaviour(gen_server).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(st, {
    conn :: term() | undefined,
    cfg = #{} :: map(),
    %% `awaiting_owner' until the listener has transferred the connection handle
    %% and sent `{quic_conn, Conn}'; `up' afterwards. Nothing may touch a handle
    %% this process does not own yet.
    phase = awaiting_owner :: awaiting_owner | up,
    %% Live stream FSMs, keyed by the monitor reference so the `DOWN' message
    %% identifies them without a reverse lookup.
    streams = #{} :: #{reference() => pid()}
}).

%%%_* API ---------------------------------------------------------------------

%% Called by the listener's acceptor loop; that loop transfers ownership of
%% `Conn' to the returned process and then sends `{quic_conn, Conn}'.
-spec start_link(map()) -> {ok, pid()} | {error, term()}.
start_link(Cfg) ->
    gen_server:start_link(?MODULE, Cfg, []).

%%%_* Lifecycle ---------------------------------------------------------------

init(Cfg) ->
    process_flag(trap_exit, true),
    {ok, #st{cfg = Cfg}}.

terminate(_Reason, #st{conn = Conn, streams = Streams}) ->
    %% Closing the connection ends every stream it carries; stopping the FSMs
    %% first keeps the shutdown ordered (each one stops its own session and
    %% publishes its Will).
    lists:foreach(fun(Pid) -> exit(Pid, shutdown) end, maps:values(Streams)),
    case Conn of
        undefined -> ok;
        _ -> _ = mqtt_quic_nif:close_connection(Conn)
    end,
    ok.

%%%_* gen_server --------------------------------------------------------------

handle_call(_Req, _From, St) ->
    {reply, {error, unsupported}, St}.

handle_cast(_Msg, St) ->
    {noreply, St}.

%% Ownership arrived: now the handle can be used, and the handshake can run. The
%% server side must do it explicitly (see module doc).
handle_info({quic_conn, closed}, #st{phase = awaiting_owner} = St) ->
    {stop, normal, St};
handle_info({quic_conn, Conn}, #st{phase = awaiting_owner, cfg = Cfg} = St) ->
    Timeout = maps:get(handshake_timeout_ms, Cfg, 5000),
    case mqtt_quic_nif:handshake(Conn, Timeout) of
        {ok, _} ->
            {noreply, St#st{conn = Conn, phase = up}};
        {error, Reason} ->
            %% The peer never became a connection. Not a broker fault, so do not
            %% burn the supervisor's restart intensity on it.
            logger:info("quic: connection handshake failed: ~p", [Reason]),
            _ = mqtt_quic_nif:close_connection(Conn),
            {stop, normal, St}
    end;

%% A client-initiated bidi stream. Each one is a separate MQTT Network
%% Connection, served by its own `mqtt_conn_fsm'.
handle_info({quic, new_stream, Stream, _Props}, #st{phase = up} = St) ->
    {noreply, start_stream(Stream, St)};

%% The peer wants to open another stream and has run out of credit -- i.e. it
%% has reconnected more times than `max_streams_per_conn' allows. quicer 0.4.12
%% answers `setopt(Conn, settings, ...)' with `ok' but does not actually deliver
%% the extra stream (measured; see `mqtt_quic_listener:open/2'), so the honest
%% answer is the one the transport already gives: no credit, no stream. The
%% connection stays up for the streams it already has.
handle_info({quic, peer_needs_streams, _Conn, Kind}, #st{phase = up} = St) ->
    logger:warning("quic: stream credit exhausted (~p); client must open a new "
                   "QUIC connection", [Kind]),
    {noreply, St};

%% Nothing left to serve: the peer closed the connection (or it died).
handle_info({quic, closed, Conn, _Props}, #st{conn = Conn} = St) ->
    {stop, normal, St};
handle_info({quic, shutdown, Conn, _Props}, #st{conn = Conn} = St) ->
    {stop, normal, St};
handle_info({quic, transport_shutdown, Conn, _Props}, #st{conn = Conn} = St) ->
    {stop, normal, St};
handle_info({quic, streams_available, _Conn, _Props}, St) ->
    {noreply, St};
handle_info({quic, dgram_state_changed, _Conn, _Props}, St) ->
    {noreply, St};

%% A stream FSM ended: the MQTT session is over, the connection is not.
handle_info({'DOWN', Ref, process, _Pid, Reason}, #st{streams = Streams} = St) ->
    case maps:take(Ref, Streams) of
        {Pid, Rest} ->
            _ = Pid,
            case Reason of
                normal -> ok;
                _ -> logger:info("quic: stream FSM ended: ~p", [Reason])
            end,
            {noreply, St#st{streams = Rest}};
        error ->
            {noreply, St}
    end;
handle_info({'EXIT', Pid, Reason}, #st{streams = Streams} = St) ->
    case stream_ref(Pid, Streams) of
        {ok, Ref} ->
            case Reason of
                normal -> ok;
                _ -> logger:info("quic: stream FSM exited: ~p", [Reason])
            end,
            {noreply, St#st{streams = maps:remove(Ref, Streams)}};
        error ->
            {noreply, St}
    end;
handle_info(_Msg, St) ->
    {noreply, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%%%_* Streams ----------------------------------------------------------------

%% Start the MQTT engine on this stream, give it the stream, then let it arm.
%%
%% Order matters: the FSM starts in `awaiting_owner' and cannot touch a socket
%% it does not own. `controlling_process/2' is therefore called before the
%% `connect' handoff, and `connect' is what makes the FSM call
%% `arm_once/1' -- the first `active, 1' on the stream, which releases bytes that
%% were already waiting in passive mode.
start_stream(Stream, #st{streams = Streams} = St) ->
    Max = maps:get(max_streams_per_conn, St#st.cfg, 4),
    case map_size(Streams) < Max of
        false ->
            %% Beyond the credit the listener granted. This cannot normally
            %% happen (MsQuic enforces MAX_STREAMS), but a client that ignores
            %% flow control gets its extra stream closed, not an unbounded
            %% number of MQTT sessions.
            logger:warning("quic: refusing extra stream (~p live, max ~p)",
                           [map_size(Streams), Max]),
            _ = mqtt_quic_nif:close_stream(Stream),
            St;
        true ->
            Opts = #{transport => mqtt_transport_quic},
            case mqtt_conn_sup:start_conn(Stream, Opts) of
                {ok, Pid} ->
                    attach_stream(Stream, Pid, St);
                {error, Reason} ->
                    logger:warning("quic: cannot start MQTT conn: ~p", [Reason]),
                    _ = mqtt_quic_nif:close_stream(Stream),
                    St
            end
    end.

attach_stream(Stream, Pid, #st{streams = Streams} = St) ->
    case mqtt_quic_nif:controlling_process(Stream, Pid) of
        ok ->
            true = link(Pid),
            Ref = erlang:monitor(process, Pid),
            Pid ! connect,
            St#st{streams = Streams#{Ref => Pid}};
        {error, Reason} ->
            %% The stream died between arrival and handoff. The FSM would sit in
            %% `awaiting_owner' forever, so it is stopped here -- it is a
            %% `temporary' child of `mqtt_conn_sup', no supervisor restart.
            logger:info("quic: stream handoff failed: ~p", [Reason]),
            _ = mqtt_quic_nif:close_stream(Stream),
            exit(Pid, shutdown),
            St
    end.

stream_ref(Pid, Streams) ->
    case [Ref || {Ref, P} <- maps:to_list(Streams), P =:= Pid] of
        [Ref | _] -> {ok, Ref};
        [] -> error
    end.
