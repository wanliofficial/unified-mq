%% @doc MQTT-SN 1.2 gateway listener (UDP).
%%
%% One socket, one reader. That is the whole design, and it is deliberate:
%% MQTT-SN session state belongs to a *client*, and a client must be served by
%% one process in order. Multiple readers on one UDP port either need
%% `SO_REUSEPORT' (the kernel hashes by 4-tuple, so a client's packets land in
%% different processes) or another hop to re-serialise them -- both buy nothing
%% over one process that does `gen_udp:recv' work in its own mailbox.
%%
%% This process owns the socket and does two things with every datagram:
%%
%%   * gateway-level packets (SEARCHGW, and the periodic ADVERTISE) it answers
%%     itself -- they belong to no session;
%%   * everything else it forwards, as the *decoded packet*, to the session
%%     bound to that {IP, Port}.
%%
%% Sessions reply by calling `gen_udp:send/4' on the same socket. A UDP socket
%% may be written from any process (`gen_udp' does not check
%% `controlling_process' on send), so no hand-off dance and no reply port are
%% needed.
%%
%% The endpoint table, not `client_id', is the routing key, because a MQTT-SN
%% client has no identity until it CONNECTs. Once it has, the session asks this
%% process to bind the id (`{sn_bind, Pid, ClientId}'), which is what makes two
%% things possible: a takeover when the same id connects from a new endpoint,
%% and a PINGREQ that arrives from a *different* endpoint (a sleeping client
%% whose NAT mapping changed) being routed to the session that holds that id.
-module(mqtt_sn_listener).

-behaviour(gen_server).

-export([start_link/1, port/1, stop/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%% Enough in flight that a burst is drained without a syscall per datagram, few
%% enough that the kernel drop counter stays meaningful under overload.
-define(ACTIVE_N, 100).

-record(st, {
    socket :: gen_udp:socket(),
    port :: inet:port_number(),
    conn_sup :: atom() | pid(),
    cfg :: map(),
    %% {IP, Port} -> session pid. The routing table.
    endpoints = #{} :: map(),
    %% ClientId -> session pid, populated by `{sn_bind, ...}'.
    clients = #{} :: map(),
    %% Session pid -> what it owns, so a DOWN can clean up both tables without
    %% guessing.
    sessions = #{} :: map(),
    %% Sessions currently asleep, and the ceiling from config.
    asleep = 0 :: non_neg_integer(),
    max_asleep = 1024 :: non_neg_integer(),
    advertise :: reference() | undefined,
    advertise_interval_ms = 0 :: non_neg_integer(),
    advertise_address = {255, 255, 255, 255} :: inet:ip_address(),
    %% Where ADVERTISE is sent. 1884 is the MQTT-SN well-known port and the
    %% address clients listen on for gateway advertisements; it is NOT this
    %% listener's own port in general (a gateway behind a port mapper, or a
    %% test that binds an ephemeral client socket, must say where to send).
    advertise_port = 1884 :: inet:port_number()
}).

%%%_* API -------------------------------------------------------------------

start_link(Spec) when is_map(Spec) ->
    gen_server:start_link(?MODULE, Spec, []).

%% @doc The port actually bound. Necessary because `port => 0' is how every
%% suite (and any deployment behind a port mapper) asks the kernel to choose.
-spec port(pid()) -> inet:port_number().
port(Pid) ->
    gen_server:call(Pid, port).

stop(Pid) ->
    gen_server:stop(Pid).

%%%_* Lifecycle -------------------------------------------------------------

init(Spec) ->
    process_flag(trap_exit, true),
    %% `predefined/0' validates the configured table; a bad one must fail the
    %% listener, not surface as a wrong topic on the wire later.
    Predefined = mqtt_sn_topics:predefined(),
    MaxTopicIds = env(max_topic_ids_per_client, 1024),
    MaxDuration = env(max_duration_s, 3600),
    GatewayId = env(gateway_id, 1),
    Interval = env(advertise_interval_ms, 0),
    Address = env(advertise_address, {255, 255, 255, 255}),
    AdvertisePort = env(advertise_port, 1884),
    Cfg = #{predefined => Predefined,
            max_topic_ids => MaxTopicIds,
            max_duration_s => MaxDuration,
            gateway_id => GatewayId,
            max_inflight => env(max_inflight, 20)},
    Port = maps:get(port, Spec),
    case gen_udp:open(Port, [binary, {active, ?ACTIVE_N}, {reuseaddr, true},
                             {broadcast, true}]) of
        {ok, Socket} ->
            {ok, RealPort} = inet:port(Socket),
            %% The session supervisor is a sibling started before us, so it is
            %% already registered by name; holding the atom (not a pid) keeps a
            %% restarted supervisor usable without re-registering listeners.
            St = #st{socket = Socket, port = RealPort, conn_sup = mqtt_sn_conn_sup,
                     cfg = Cfg, max_asleep = env(max_asleep_clients, 1024),
                     advertise_interval_ms = Interval,
                     advertise_address = Address,
                     advertise_port = AdvertisePort},
            {ok, arm_advertise(St)};
        {error, Reason} ->
            %% Binding is all-or-nothing, exactly like the TCP listeners: a
            %% gateway that cannot listen must not pretend it started.
            {stop, {udp_open_failed, Port, Reason}}
    end.

terminate(_Reason, #st{socket = Socket}) ->
    _ = gen_udp:close(Socket),
    ok.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%%%_* gen_server ------------------------------------------------------------

handle_call(port, _From, #st{port = Port} = St) ->
    {reply, Port, St};
handle_call(_Req, _From, St) ->
    {reply, {error, unsupported}, St}.

%% A session has finished connecting and owns `ClientId' from now on. A
%% different live session already holding it is a takeover (MQTT §4.3): it is
%% told to close *without* publishing its Will, because the client did not die,
%% it moved.
handle_cast({sn_bind, Pid, ClientId}, St) ->
    case maps:find(ClientId, St#st.clients) of
        {ok, Pid} ->
            {noreply, St};
        {ok, Old} when is_pid(Old) ->
            Old ! {sn_takeover, Pid},
            {noreply, put_client(ClientId, Pid, St)};
        error ->
            {noreply, put_client(ClientId, Pid, St)}
    end;
%% Sleeping-census admission. The answer travels back to the session, which
%% then either sleeps or closes; this process never blocks on it.
handle_cast({sn_sleep, Pid}, #st{asleep = N, max_asleep = Max} = St) ->
    case N < Max of
        true ->
            Pid ! {sn_sleep_granted, self()},
            _ = broker_metrics:gauge_add(mqtt_sn_asleep, 1),
            {noreply, mark_asleep(true, Pid, St#st{asleep = N + 1})};
        false ->
            %% Refusing the *sleep* is not refusing the connection: the spec has
            %% no packet to say "too many sleepers", so the gateway answers the
            %% DISCONNECT and then behaves as if the client had disconnected for
            %% good. Silence (pretending it slept) would lose messages later.
            broker_metrics:inc(mqtt_sn_sleep_denied),
            Pid ! {sn_sleep_denied, self()},
            {noreply, St}
    end;
handle_cast({sn_wake, Pid}, St) ->
    {noreply, release_sleep(Pid, St)};
handle_cast(_Msg, St) ->
    {noreply, St}.

handle_info({udp, Socket, IP, Port, Bin}, #st{socket = Socket} = St) ->
    {noreply, on_datagram(IP, Port, Bin, St)};
handle_info({udp_passive, Socket}, #st{socket = Socket} = St) ->
    _ = inet:setopts(Socket, [{active, ?ACTIVE_N}]),
    {noreply, St};
handle_info({udp_error, Socket, Reason}, #st{socket = Socket} = St) ->
    logger:warning("mqtt_sn listener socket error: ~p", [Reason]),
    {noreply, St};
handle_info({'DOWN', _Ref, process, Pid, _Reason}, St) ->
    {noreply, forget(Pid, St)};
handle_info(advertise_tick, St) ->
    _ = send_advertise(St),
    {noreply, arm_advertise(St)};
handle_info(_Other, St) ->
    {noreply, St}.

%%%_* Datagram dispatch -----------------------------------------------------

on_datagram(IP, Port, Bin, St) ->
    broker_metrics:inc(bytes_in, byte_size(Bin)),
    Endpoint = {IP, Port},
    case mqtt_sn_codec:decode(Bin) of
        {error, _Reason} ->
            %% UDP carries no way to answer "your datagram was malformed", and
            %% inventing a DISCONNECT for it would let any packet kill a
            %% session. Drop and count.
            broker_metrics:inc(frame_decode_errors),
            St;
        {ok, #{type := searchgw, radius := Radius}} ->
            %% Gateway discovery belongs to no session, so it is answered here
            %% even for an endpoint that has one.
            Reply = mqtt_sn_codec:encode(#{type => gwinfo,
                                           gw_id => maps:get(gateway_id,
                                                             St#st.cfg),
                                           gw_add => <<>>}),
            _ = gen_udp:send(St#st.socket, IP, Port, Reply),
            _ = Radius,
            St;
        {ok, #{type := connect} = Packet} ->
            connect_endpoint(Endpoint, Packet, St);
        {ok, #{type := pingreq, client_id := ClientId} = Packet} when ClientId =/= <<>> ->
            %% A sleeping client that lost its NAT mapping sends PINGREQ from a
            %% new endpoint, naming itself. That is the only reason the field
            %% exists, so honour it: rebind the endpoint to the session that
            %% holds the id.
            route_by_client(Endpoint, ClientId, Packet, St);
        {ok, #{type := publish, qos := -1} = Packet} ->
            case maps:find(Endpoint, St#st.endpoints) of
                {ok, Pid} ->
                    Pid ! {sn_packet, Packet},
                    St;
                error ->
                    %% A QoS -1 publish is defined to work without a connection.
                    %% With no session there is no topic-id table, so only the
                    %% stateless topic forms can be resolved (predefined and
                    %% short); a normal id has no meaning outside a session.
                    _ = mqtt_sn_conn:publish_stateless(Packet, St#st.cfg),
                    St
            end;
        {ok, Packet} ->
            route(Endpoint, Packet, St)
    end.

route(Endpoint, Packet, St) ->
    case maps:find(Endpoint, St#st.endpoints) of
        {ok, Pid} ->
            Pid ! {sn_packet, Packet},
            St;
        error ->
            %% Nothing to serve: a client that never CONNECTed, or one whose
            %% session already ended. Count it -- a stream of these is the only
            %% symptom of a client retrying into a gateway that dropped it.
            broker_metrics:inc(protocol_errors),
            St
    end.

%% CONNECT is the one packet that may arrive without a session: it is what
%% creates one. A repeat CONNECT from an endpoint that already has a session is
%% forwarded like any other packet -- the session decides whether the client id
%% is a takeover or a protocol error.
connect_endpoint(Endpoint, Packet, St) ->
    case maps:find(Endpoint, St#st.endpoints) of
        {ok, Pid} ->
            Pid ! {sn_packet, Packet},
            St;
        error ->
            start_session(Endpoint, Packet, St)
    end.

route_by_client(Endpoint, ClientId, Packet, St) ->
    case maps:find(Endpoint, St#st.endpoints) of
        {ok, Pid} ->
            Pid ! {sn_packet, Packet},
            St;
        error ->
            case maps:find(ClientId, St#st.clients) of
                {ok, Pid} ->
                    Pid ! {sn_packet, Packet},
                    rebind(Endpoint, Pid, St);
                error ->
                    %% The gateway does not know this client (it slept past its
                    %% duration, or another gateway took it). MQTT-SN §6.13: say
                    %% so with DISCONNECT rather than leave it pinging.
                    _ = gen_udp:send(St#st.socket, element(1, Endpoint),
                                     element(2, Endpoint),
                                     mqtt_sn_codec:encode(#{type => disconnect,
                                                            duration => 0})),
                    St
            end
    end.

start_session(Endpoint, Packet, St) ->
    case mqtt_sn_conn_sup:start_conn(St#st.conn_sup, self(), St#st.socket,
                                     Endpoint, St#st.cfg) of
        {ok, Pid} ->
            Ref = erlang:monitor(process, Pid),
            Pid ! {sn_packet, Packet},
            St#st{endpoints = maps:put(Endpoint, Pid, St#st.endpoints),
                  sessions = maps:put(Pid, #{endpoint => Endpoint,
                                             client_id => undefined,
                                             ref => Ref,
                                             asleep => false},
                                      St#st.sessions)};
        {error, Reason} ->
            logger:error("mqtt_sn cannot start a session: ~p", [Reason]),
            St
    end.

%%%_* Session bookkeeping ---------------------------------------------------

put_client(ClientId, Pid, St) ->
    case maps:find(Pid, St#st.sessions) of
        {ok, Info} ->
            St#st{clients = maps:put(ClientId, Pid, St#st.clients),
                  sessions = maps:put(Pid, Info#{client_id => ClientId},
                                      St#st.sessions)};
        error ->
            %% The session died between bind and now; nothing to record.
            St
    end.

rebind(Endpoint, Pid, St) ->
    case maps:find(Pid, St#st.sessions) of
        {ok, #{endpoint := Old} = Info} when Old =/= Endpoint ->
            %% The session writes replies to its own endpoint, so it has to be
            %% told where the client moved to; otherwise it would answer the
            %% dead address and the client would keep timing out.
            Pid ! {sn_endpoint, Endpoint},
            St#st{endpoints = maps:put(Endpoint, Pid,
                                       maps:remove(Old, St#st.endpoints)),
                  sessions = maps:put(Pid, Info#{endpoint => Endpoint},
                                      St#st.sessions)};
        _ ->
            St
    end.

mark_asleep(Flag, Pid, St) ->
    case maps:find(Pid, St#st.sessions) of
        {ok, Info} ->
            St#st{sessions = maps:put(Pid, Info#{asleep => Flag}, St#st.sessions)};
        error ->
            St
    end.

release_sleep(Pid, #st{asleep = N} = St) ->
    case maps:find(Pid, St#st.sessions) of
        {ok, #{asleep := true} = Info} ->
            _ = broker_metrics:gauge_add(mqtt_sn_asleep, -1),
            St#st{asleep = max(N - 1, 0),
                  sessions = maps:put(Pid, Info#{asleep => false},
                                      St#st.sessions)};
        _ ->
            St
    end.

forget(Pid, St) ->
    case maps:take(Pid, St#st.sessions) of
        {#{endpoint := Endpoint, client_id := ClientId, asleep := Asleep}, Rest} ->
            Clients = case maps:find(ClientId, St#st.clients) of
                          {ok, Pid} -> maps:remove(ClientId, St#st.clients);
                          _ -> St#st.clients
                      end,
            case Asleep of
                true -> _ = broker_metrics:gauge_add(mqtt_sn_asleep, -1);
                false -> ok
            end,
            St#st{endpoints = maps:remove(Endpoint, St#st.endpoints),
                  sessions = Rest,
                  clients = Clients,
                  asleep = case Asleep of true -> max(St#st.asleep - 1, 0);
                                        false -> St#st.asleep end};
        error ->
            St
    end.

%%%_* ADVERTISE -------------------------------------------------------------

arm_advertise(#st{advertise_interval_ms = 0} = St) ->
    St;
arm_advertise(#st{advertise_interval_ms = Ms} = St) ->
    St#st{advertise = erlang:send_after(Ms, self(), advertise_tick)}.

send_advertise(#st{socket = Socket, advertise_address = Addr,
                   advertise_port = Port} = St) ->
    Packet = mqtt_sn_codec:encode(#{type => advertise,
                                    gw_id => maps:get(gateway_id, St#st.cfg),
                                    duration => advertise_seconds(St)}),
    gen_udp:send(Socket, Addr, Port, Packet).

%% ADVERTISE carries the period in seconds; the config is in milliseconds, so
%% the wire value is the ceiling -- an interval of 1 ms would otherwise be
%% advertised as "0", which means "stop advertising".
advertise_seconds(#st{advertise_interval_ms = Ms}) ->
    max(1, (Ms + 999) div 1000).

%%%_* Misc ------------------------------------------------------------------

env(Key, Default) ->
    case application:get_env(broker_proto_mqtt_sn, Key, Default) of
        V when is_integer(V) -> V;
        _ -> Default
    end.
