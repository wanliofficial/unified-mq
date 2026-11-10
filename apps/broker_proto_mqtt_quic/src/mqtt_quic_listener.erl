%% @doc QUIC listener for MQTT (Single Stream Mode, CN Draft 3).
%%
%% One `quicer' listener, one acceptor loop, one owner process per QUIC
%% connection, and — under that — the same `mqtt_conn_fsm' that serves 1883 and
%% 8083. Nothing about MQTT changes here; the transport is the only difference.
%%
%% Shape:
%%
%%   mqtt_quic_listener (gen_server, owns the listener handle + the connection count)
%%     └── acceptor process (loops quicer:accept/3, hands each connection over)
%%           └── mqtt_quic_conn (owns one QUIC connection, loops over streams)
%%                 └── mqtt_conn_fsm (one per stream; handle = the stream)
%%
%% Why the connection gets its own process instead of being served in the
%% acceptor: a QUIC connection outlives its stream. CN Draft 3 says closing the
%% stream is **not** closing the connection — the client may open a new stream on
%% the same connection and CONNECT again — so somebody has to stay alive between
%% the two, own the connection handle, and accept the next stream. That is
%% `mqtt_quic_conn'’s entire job.
%%
%% Two refusals are configuration errors, and both fail this listener only:
%%
%%   * no `quicer' (`{quic_not_available}`) — the app is compiled in the default
%%     profile but the NIF is a `quic`-profile dependency;
%%   * no certificate (`{quic_requires_tls, Key}`) — QUIC's handshake *is* TLS
%%     1.3, so unlike every other listener there is no cleartext mode to fall
%%     back to. `broker_security.tls.enabled' cannot turn that off, and this
%%     listener does not pretend otherwise.
-module(mqtt_quic_listener).

-behaviour(gen_server).

-export([start_link/1, port/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(DEFAULT_HANDSHAKE_TIMEOUT_MS, 5000).
-define(DEFAULT_IDLE_TIMEOUT_MS, 30000).
-define(DEFAULT_MAX_CONNECTIONS, 1024).
%% How many client-initiated bidi streams one QUIC connection may open **in
%% sequence**. Four is a reconnect allowance, not a concurrency target: Single
%% Stream Mode says at most one MQTT stream is active at a time, and the client
%% is what enforces that. See `open/2' for why the pool is granted up front.
-define(DEFAULT_MAX_STREAMS_PER_CONN, 4).

-record(st, {
    listener :: term(),
    port :: inet:port_number(),
    acceptor :: pid() | undefined,
    conns = #{} :: #{reference() => pid()},
    cfg = #{} :: map(),
    max_connections = ?DEFAULT_MAX_CONNECTIONS :: non_neg_integer()
}).

%%%_* API ---------------------------------------------------------------------

start_link(Spec) when is_map(Spec) ->
    gen_server:start_link(?MODULE, Spec, []).

%% @doc The port actually bound. `port => 0' asks the kernel, and a QUIC
%% listener is not visible to `inet:i/0', so this readback is the only way a
%% suite can learn where to connect.
-spec port(pid()) -> inet:port_number().
port(Pid) ->
    gen_server:call(Pid, port).

%%%_* Lifecycle ---------------------------------------------------------------

init(Spec) ->
    process_flag(trap_exit, true),
    %% Configuration first, capability second: a missing certificate is a
    %% configuration error that can be diagnosed without the NIF, and it is the
    %% refusal the operator has to act on regardless of what else is missing.
    case cert_config(Spec) of
        {ok, TlsOpts} ->
            case mqtt_quic_nif:ensure() of
                ok -> open(Spec, TlsOpts);
                %% `gen_server:start_link' wraps a `{stop, R}' init failure as
                %% `{error, R}', so the bare atom here surfaces as the
                %% actionable `{error, quicer_not_available}'.
                {error, quicer_not_available} -> {stop, quicer_not_available}
            end;
        {error, Reason} ->
            %% `gen_server:start_link' wraps a `{stop, R}' init failure as
            %% `{error, R}'; passing the reason through unwrapped keeps the
            %% reported refusal exactly `{error, {quic_requires_tls, Key}}'.
            {stop, Reason}
    end.

open(Spec, TlsOpts) ->
    Port = maps:get(port, Spec),
    Alpn = application:get_env(broker_proto_mqtt_quic, alpn, [<<"mqtt">>]),
    %% CN Draft 3 §2.2: one client-initiated bidirectional stream carries the
    %% whole MQTT connection. `max_streams_per_conn' is how many streams a client
    %% may open **in sequence** on one QUIC connection -- i.e. how many times it
    %% may reconnect by opening a new stream after closing the previous one, which
    %% is the whole point of "stream close is not connection close". It is a
    %% credit the connection starts with, not a queue: MsQuic hands the peer its
    %% MAX_STREAMS frame at handshake time and quicer 0.4.12 does not reliably
    %% extend it afterwards (`setopt(Conn, settings, ...)' answers `ok' but the
    %% second stream still never arrives), so the pool is granted up front and
    %% sized for a realistic reconnect count rather than topped up on demand.
    MaxStreams = maps:get(max_streams_per_conn, Spec, max_streams_per_conn()),
    Opts = maps:merge(#{alpn => [binary_to_list(A) || A <- Alpn],
                        peer_bidi_stream_count => MaxStreams,
                        peer_unidi_stream_count => 0,
                        idle_timeout_ms => maps:get(idle_timeout_ms, Spec,
                                                    idle_timeout_ms())},
                      TlsOpts),
    case mqtt_quic_nif:listen(Port, Opts) of
        {ok, Listener} ->
            {ok, RealPort} = listener_port(Listener, Port),
            Cfg = #{handshake_timeout_ms =>
                        maps:get(handshake_timeout_ms, Spec,
                                 handshake_timeout_ms()),
                    idle_timeout_ms => maps:get(idle_timeout_ms, Spec,
                                                idle_timeout_ms()),
                    max_streams_per_conn => MaxStreams},
            St = #st{listener = Listener, port = RealPort, cfg = Cfg,
                     max_connections = maps:get(max_connections, Spec,
                                                ?DEFAULT_MAX_CONNECTIONS)},
            {ok, start_acceptor(St)};
        {error, _} = E ->
            %% A listener that cannot bind is a boot failure for this app, not a
            %% silent absence of the protocol.
            {stop, {quic_listen_failed, Port, E}}
    end.

%% The `cfg' record field is filled from the map built in `open/2'; keeping the
%% two together avoids a second, divergent default table. A QUIC listener is not
%% visible to `inet:i/0', so the bound port has to be read back from the handle.
listener_port(Listener, ReqPort) ->
    case mqtt_quic_nif:sockname(Listener) of
        {ok, {_Ip, Port}} -> {ok, Port};
        {error, _} -> {ok, ReqPort}
    end.

%% QUIC has no cleartext mode. The listener map may carry its own certificate
%% pair; otherwise it takes `broker_security.tls'. If neither has one, this
%% listener cannot start -- and must say so rather than listen on a port that no
%% client can ever complete a handshake with.
cert_config(Spec) ->
    Tls = broker_security_tls:config(),
    Cert = case maps:get(certfile, Spec, undefined) of
               undefined -> maps:get(certfile, Tls);
               C -> C
           end,
    Key = case maps:get(keyfile, Spec, undefined) of
              undefined -> maps:get(keyfile, Tls);
              K -> K
          end,
    case {Cert, Key} of
        {undefined, _} -> {error, {quic_requires_tls, certfile}};
        {_, undefined} -> {error, {quic_requires_tls, keyfile}};
        {CertFile, KeyFile} -> {ok, #{certfile => to_list(CertFile),
                                      keyfile => to_list(KeyFile)}}
    end.

to_list(P) when is_binary(P) -> binary_to_list(P);
to_list(P) -> P.

%%%_* Acceptor ----------------------------------------------------------------

start_acceptor(#st{listener = Listener, cfg = Cfg} = St) ->
    Self = self(),
    Pid = spawn_link(fun() -> accept_loop(Self, Listener, Cfg) end),
    St#st{acceptor = Pid}.

%% Runs in its own process and owns every accepted connection until it hands it
%% to the connection process. `quicer:accept/3' is selective on `new_conn', so a
%% stale message for a handle it no longer owns cannot wake it up early.
accept_loop(ListenerPid, Listener, Cfg) ->
    Timeout = maps:get(handshake_timeout_ms, Cfg),
    case mqtt_quic_nif:accept(Listener, #{}, Timeout) of
        {ok, Conn} ->
            case gen_server:call(ListenerPid, {new_conn, Conn}, 5000) of
                {ok, Pid} ->
                    %% Ownership moves before the connection process is told to
                    %% do anything, so every `{quic, ...}' for this handle lands
                    %% in the process that will keep it.
                    case mqtt_quic_nif:controlling_process(Conn, Pid) of
                        ok ->
                            Pid ! {quic_conn, Conn};
                        {error, Reason} ->
                            logger:warning("quic: handoff failed: ~p", [Reason]),
                            _ = mqtt_quic_nif:close_connection(Conn),
                            Pid ! {quic_conn, closed}
                    end;
                {error, _Reason} ->
                    _ = mqtt_quic_nif:close_connection(Conn)
            end,
            accept_loop(ListenerPid, Listener, Cfg);
        {error, timeout} ->
            accept_loop(ListenerPid, Listener, Cfg);
        {error, closed} ->
            ok;
        {error, Reason} ->
            %% A transient accept error must not kill the listener for everyone;
            %% the next loop iteration retries.
            logger:warning("quic: accept failed: ~p", [Reason]),
            timer:sleep(100),
            accept_loop(ListenerPid, Listener, Cfg)
    end.

%%%_* gen_server --------------------------------------------------------------

handle_call(port, _From, #st{port = Port} = St) ->
    {reply, Port, St};
handle_call({new_conn, _Conn}, _From,
            #st{conns = Conns, max_connections = Max} = St)
  when map_size(Conns) >= Max ->
    {reply, {error, too_many_connections}, St};
handle_call({new_conn, _Conn}, _From, #st{conns = Conns, cfg = Cfg} = St) ->
    case mqtt_quic_conn:start_link(Cfg) of
        {ok, Pid} ->
            Ref = erlang:monitor(process, Pid),
            {reply, {ok, Pid}, St#st{conns = Conns#{Ref => Pid}}};
        {error, Reason} ->
            %% The acceptor closes the connection when this comes back as an
            %% error; the listener itself stays up for the next peer.
            {reply, {error, Reason}, St}
    end;
handle_call(_Req, _From, St) ->
    {reply, {error, unsupported}, St}.

handle_cast(_Msg, St) ->
    {noreply, St}.

handle_info({'DOWN', Ref, process, _Pid, _Reason}, #st{conns = Conns} = St) ->
    {noreply, St#st{conns = maps:remove(Ref, Conns)}};
handle_info({'EXIT', Pid, Reason}, #st{acceptor = Pid} = St) ->
    case Reason of
        normal ->
            {noreply, St#st{acceptor = undefined}};
        _ ->
            %% The acceptor is the only way connections arrive, so its death is
            %% the listener's death; the supervisor restarts the whole child.
            {stop, {acceptor_died, Reason}, St}
    end;
handle_info({'EXIT', _Pid, _Reason}, St) ->
    {noreply, St};
handle_info(_Msg, St) ->
    {noreply, St}.

terminate(_Reason, #st{listener = Listener, acceptor = Acceptor}) ->
    case Acceptor of
        undefined -> ok;
        Pid -> unlink(Pid), exit(Pid, kill)
    end,
    _ = mqtt_quic_nif:close_listener(Listener),
    ok.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%%%_* Config ------------------------------------------------------------------

handshake_timeout_ms() ->
    application:get_env(broker_proto_mqtt_quic, handshake_timeout_ms,
                        ?DEFAULT_HANDSHAKE_TIMEOUT_MS).

idle_timeout_ms() ->
    application:get_env(broker_proto_mqtt_quic, idle_timeout_ms,
                        ?DEFAULT_IDLE_TIMEOUT_MS).

max_streams_per_conn() ->
    application:get_env(broker_proto_mqtt_quic, max_streams_per_conn,
                        ?DEFAULT_MAX_STREAMS_PER_CONN).
