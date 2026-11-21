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
%% DTLS (SN-01, 2026-11-17): a listener spec with `transport => dtls' runs the
%% S11-C pattern (PSK-only, serial acceptor, the LISTENER owns every
%% established socket). The routing tables stay keyed by `{IP, Port}' because
%% `ssl:peername/1' reports the same tuple `gen_udp' does, so endpoint
%% routing, takeover and the sleeping-client rebind are transport-blind.
%% Replies converge on one exit: the sessions cast `{sn_send, Endpoint,
%% Data}' and this process writes gen_udp or ssl depending on the endpoint.
%% ADVERTISE broadcast is a UDP-only feature (a DTLS peer is a known,
%% authenticated endpoint; broadcast discovery of an encrypted gateway is
%% meaningless).
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
    transport = udp :: udp | dtls,
    socket :: gen_udp:socket() | undefined,
    port :: inet:port_number(),
    conn_sup :: atom() | pid(),
    cfg :: map(),
    %% DTLS: the listen socket + serial acceptor (S11-C pattern).
    dtls_listen = undefined :: term() | undefined,
    dtls_acceptor = undefined :: pid() | undefined,
    %% Established DTLS socket -> {IP, Port}; the reverse lookup for replies.
    peers = #{} :: map(),
    socks = #{} :: map(),
    %% {IP, Port} -> #{principal, tenant}: the identity the handshake proved.
    idents = #{} :: map(),
    principal_sessions = #{} :: map(),
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
    advertise_port = 1884 :: inet:port_number(),
    %% v1.2 will-delay: ClientID -> {Will, DelayS, AtMs, Tenant}. Held after a
    %% session dies with will_delay>0; published when DelayS elapses unless the
    %% client reconnects first. AtMs is the listener's own monotonic clock.
    deferred_wills = #{} :: map(),
    deferred_will_timer = undefined :: reference() | undefined
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
    Transport = maps:get(transport, Spec, udp),
    case Transport of
        udp -> init_udp(Port, Cfg, Interval, Address, AdvertisePort);
        dtls -> init_dtls(Port, Cfg, Address, AdvertisePort);
        Other -> {stop, {unknown_transport, Other}}
    end.

init_udp(Port, Cfg, Interval, Address, AdvertisePort) ->
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

%% A DTLS listener refuses to start without credentials: no `users' row means
%% no peer can ever complete the PSK handshake, which is a configuration
%% error, not a safe default (the CoAP S11-C posture).
init_dtls(Port, Cfg, Address, AdvertisePort) ->
    {ok, _} = application:ensure_all_started(ssl),
    case has_users() of
        false ->
            {stop, {dtls_no_credentials}};
        true ->
            start_dtls(Port, Cfg, Address, AdvertisePort)
    end.

has_users() ->
    Users = maps:get(users, broker_security:redacted_config(), []),
    Users =/= [].

start_dtls(Port, Cfg, Address, AdvertisePort) ->
    Suites = cert_free_psk_suites(),
    case Suites of
        [] ->
            {stop, {dtls_no_suites}};
        _ ->
            Self = self(),
            DtlsCfg = Cfg#{dtls_transport_accept_timeout_ms =>
                               env(dtls_transport_accept_timeout_ms, 1000),
                           dtls_handshake_timeout_ms =>
                               env(dtls_handshake_timeout_ms, 2000),
                           max_dtls_sessions =>
                               env(max_dtls_sessions, 1024),
                           dtls_max_sessions_per_principal =>
                               env(dtls_max_sessions_per_principal, 16)},
            {ok, Acceptor} = mqtt_sn_dtls_acceptor:start_link(Self, DtlsCfg),
            DecoySecret = crypto:strong_rand_bytes(32),
            ListenOpts = [{protocol, dtls},
                          {ciphers, Suites},
                          {user_lookup_fun,
                           {psk_lookup(Acceptor, DecoySecret), undefined}},
                          {reuseaddr, true},
                          {mode, binary},
                          {active, false}],
            case ssl:listen(Port, ListenOpts) of
                {ok, Listen} ->
                    {ok, {_Addr, ActualPort}} = ssl:sockname(Listen),
                    Acceptor ! {listen_socket, Listen},
                    St = #st{transport = dtls, socket = undefined,
                             port = ActualPort, conn_sup = mqtt_sn_conn_sup,
                             cfg = DtlsCfg, max_asleep = env(max_asleep_clients, 1024),
                             advertise_interval_ms = 0,
                             advertise_address = Address,
                             advertise_port = AdvertisePort,
                             dtls_listen = Listen, dtls_acceptor = Acceptor},
                    {ok, St};
                {error, Reason} ->
                    {stop, {bind_failed, Port, Reason}}
            end
    end.

%% PSK callback (see coap_listener:psk_lookup/2 for the decoy rationale): the
%% identity is a username in `broker_security''s table; the key derives from
%% the stored password under the MQTT-SN label. Unknown identity -> decoy key
%% so username enumeration cannot ride a distinct TLS alert.
psk_lookup(Acceptor, DecoySecret) ->
    fun(psk, Identity, _State) ->
            Acceptor ! {psk_identity, Identity},
            case broker_security:mqttsn_psk_for_identity(Identity) of
                {ok, Psk} -> {ok, Psk};
                {error, _} -> {ok, decoy_psk(Identity, DecoySecret)}
            end
    end.

decoy_psk(Identity, Secret) ->
    crypto:hash(sha256, [<<"unifiedmq-mqttsn-psk-decoy">>, 0, Secret, 0, Identity]).

cert_free_psk_suites() ->
    [S || S <- ssl:cipher_suites(anonymous, 'dtlsv1.2'),
          lists:member(maps:get(key_exchange, S, none), [psk, dhe_psk, ecdhe_psk]),
          lists:member(maps:get(cipher, S, none),
                       [aes_128_gcm, aes_256_gcm, aes_128_ccm, aes_256_ccm,
                        aes_128_ccm_8, aes_256_ccm_8])].

terminate(_Reason, #st{transport = udp, socket = Socket}) ->
    _ = gen_udp:close(Socket),
    ok;
terminate(_Reason, #st{transport = dtls, dtls_listen = Listen, peers = Peers}) ->
    _ = ssl:close(Listen),
    lists:foreach(fun(Sock) -> _ = ssl:close(Sock) end, maps:keys(Peers)),
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
%% The unified reply exit for sessions (mqtt_sn_conn:reply/2). One process
%% owns every socket in either transport, so one cast is the whole protocol.
handle_cast({sn_send, Endpoint, Data}, St) ->
    {noreply, send_to(Endpoint, Data, St)};
handle_cast({sn_deferred_will, ClientID, Will, DelayS, Tenant},
            #st{deferred_wills = DW} = St) ->
    AtMs = erlang:monotonic_time(millisecond),
    DW1 = maps:put(ClientID, {Will, DelayS, AtMs, Tenant}, DW),
    {noreply, arm_deferred_will_timer(St#st{deferred_wills = DW1})};
handle_cast({sn_cancel_deferred_will, ClientID},
            #st{deferred_wills = DW} = St) ->
    case maps:take(ClientID, DW) of
        {_, DW1} ->
            %% Drop the cancelled entry, cancel any armed timer, then re-arm to
            %% the new nearest deadline (or none if the table is now empty).
            St1 = cancel_deferred_will_timer(St#st{deferred_wills = DW1}),
            {noreply, arm_deferred_will_timer(St1)};
        error ->
            {noreply, St}
    end;
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
%% --- DTLS (SN-01) ----------------------------------------------------------

handle_info({dtls_handshake_ok, From, Socket, Peer, Identity}, St) ->
    case admit_session(Identity, Peer, Socket, St) of
        {ok, St1} ->
            From ! {dtls_admit, Socket, ok},
            {noreply, St1};
        {refuse, Why} ->
            logger:notice("mqtt_sn dtls session refused (~p) peer=~p identity=~p",
                          [Why, Peer, Identity]),
            From ! {dtls_admit, Socket, refuse},
            {noreply, St}
    end;
handle_info({dtls_owned, Socket}, #st{peers = Peers} = St) ->
    case maps:is_key(Socket, Peers) of
        true -> _ = ssl:setopts(Socket, [{active, ?ACTIVE_N}]);
        false -> ok
    end,
    {noreply, St};
handle_info({dtls_abandoned, Socket}, St) ->
    {noreply, drop_session(Socket, St)};
handle_info({ssl, Socket, Bin}, #st{peers = Peers} = St) ->
    case maps:find(Socket, Peers) of
        {ok, {IP, Port}} ->
            {noreply, on_datagram(IP, Port, Bin, St)};
        error ->
            {noreply, St}
    end;
handle_info({ssl_closed, Socket}, St) ->
    {noreply, drop_session(Socket, St)};
handle_info({ssl_error, Socket, Reason}, St) ->
    logger:debug("mqtt_sn dtls socket error: ~p", [Reason]),
    {noreply, drop_session(Socket, St)};
handle_info({ssl_passive, Socket}, #st{peers = Peers} = St) ->
    case maps:is_key(Socket, Peers) of
        true -> _ = ssl:setopts(Socket, [{active, ?ACTIVE_N}]);
        false -> ok
    end,
    {noreply, St};
handle_info({'EXIT', Acceptor, Reason}, #st{dtls_acceptor = Acceptor} = St) ->
    {stop, {dtls_acceptor_died, Reason}, St};
handle_info({'DOWN', _Ref, process, Pid, _Reason}, St) ->
    {noreply, forget(Pid, St)};
handle_info({timeout, Ref, deferred_will_due},
            #st{deferred_will_timer = Ref} = St) ->
    {noreply, scan_deferred_wills(St)};
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
            _ = send_to(Endpoint, Reply, St),
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
                    _ = send_to(Endpoint,
                                mqtt_sn_codec:encode(#{type => disconnect,
                                                       duration => 0}),
                                St),
                    St
            end
    end.

start_session(Endpoint, Packet, St) ->
    %% DTLS: the handshake-proven identity rides the session config, so
    %% CONNECT cannot claim a principal the socket did not prove (MQTTSN-003).
    Cfg = case St#st.transport of
              dtls ->
                  Id = maps:get(Endpoint, St#st.idents, #{}),
                  Base = St#st.cfg,
                  Base#{principal => maps:get(principal, Id, undefined),
                        tenant_id => maps:get(tenant, Id,
                                              broker_security:default_tenant()),
                        transport => dtls};
              udp ->
                  Base = St#st.cfg,
                  Base#{principal => undefined,
                        tenant_id => broker_security:default_tenant(),
                        transport => udp}
          end,
    case mqtt_sn_conn_sup:start_conn(St#st.conn_sup, self(), St#st.socket,
                                     Endpoint, Cfg) of
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

%%%_* Deferred Will (v1.2 will-delay) ---------------------------------------

%% Fire the Will for every entry whose delay has elapsed, cancel those whose
%% client has a live session (reconnected), and re-arm for the rest. A live
%% session is keyed in `clients'; the conn's CONNECT-time cancel cast is the
%% primary cancel path, this scan is the expiry path.
scan_deferred_wills(#st{deferred_wills = DW} = St) ->
    Now = erlang:monotonic_time(millisecond),
    St1 = maps:fold(
            fun(ClientID, {Will, DelayS, AtMs, Tenant}, Acc) ->
                    case (Now - AtMs) >= DelayS * 1000 of
                        false ->
                            Acc;  %% not due yet
                        true ->
                            case maps:is_key(ClientID, Acc#st.clients) of
                                true ->
                                    %% Reconnected within the window: cancel.
                                    Acc#st{deferred_wills =
                                           maps:remove(ClientID,
                                                       Acc#st.deferred_wills)};
                                false ->
                                    ok = mqtt_sn_conn:publish_will_now(Will, Tenant),
                                    Acc#st{deferred_wills =
                                           maps:remove(ClientID,
                                                       Acc#st.deferred_wills)}
                            end
                    end
            end, St, DW),
    %% The timer that fired is now stale; reset the ref before re-arming or the
    %% "already armed" guard below would refuse to arm the next deadline.
    arm_deferred_will_timer(St1#st{deferred_will_timer = undefined}).

%% Arm a single timer to the nearest deadline. No-op if the table is empty or a
%% timer is already armed (the earliest deadline triggers the scan and re-arms).
arm_deferred_will_timer(#st{deferred_wills = DW,
                            deferred_will_timer = undefined} = St) ->
    case nearest_due_ms(DW, erlang:monotonic_time(millisecond)) of
        undefined ->
            St;
        DueMs ->
            St#st{deferred_will_timer =
                          erlang:start_timer(DueMs, self(), deferred_will_due)}
    end;
arm_deferred_will_timer(St) ->
    St.

cancel_deferred_will_timer(#st{deferred_will_timer = undefined} = St) ->
    St;
cancel_deferred_will_timer(#st{deferred_will_timer = TRef} = St) ->
    _ = erlang:cancel_timer(TRef),
    St#st{deferred_will_timer = undefined}.

%% Milliseconds until the earliest deadline in the table.
nearest_due_ms(DW, Now) ->
    Due = maps:fold(
            fun(_ClientID, {_Will, DelayS, AtMs, _Tenant}, Acc) ->
                    Deadline = AtMs + DelayS * 1000,
                    min(Deadline, Acc)
            end, infinity, DW),
    case Due of
        infinity -> undefined;
        _ when Due =< Now -> 0;
        _ -> Due - Now
    end.

%%%_* ADVERTISE -------------------------------------------------------------

arm_advertise(#st{advertise_interval_ms = 0} = St) ->
    St;
arm_advertise(#st{advertise_interval_ms = Ms} = St) ->
    St#st{advertise = erlang:send_after(Ms, self(), advertise_tick)}.

send_advertise(#st{transport = dtls}) ->
    %% Broadcast discovery of an authenticated, encrypted gateway is
    %% meaningless; DTLS listeners do not advertise (documented limit).
    ok;
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

%%%_* DTLS session admission / reply exit -----------------------------------

%% The caps and tables live here, so this is the single place that admits.
admit_session(Identity, Peer, Socket, #st{peers = Peers, cfg = Cfg} = St) ->
    Max = maps:get(max_dtls_sessions, Cfg, 1024),
    case maps:size(Peers) >= Max of
        true ->
            {refuse, session_cap};
        false ->
            case Identity of
                undefined ->
                    {refuse, no_identity};
                _ ->
                    Principal = Identity,
                    PerCap = maps:get(dtls_max_sessions_per_principal,
                                      Cfg, 16),
                    case maps:get(Principal, St#st.principal_sessions, 0) >= PerCap of
                        true -> {refuse, principal_cap};
                        false -> {ok, record_session(Peer, Socket, Principal, St)}
                    end
            end
    end.

record_session({IP, Port} = Peer, Socket, Principal,
               #st{peers = Peers, socks = Socks, idents = Idents,
                   principal_sessions = Counts} = St) ->
    St#st{peers = maps:put(Socket, Peer, Peers),
          socks = maps:put({IP, Port}, Socket, Socks),
          idents = maps:put({IP, Port},
                            #{principal => Principal,
                              tenant => broker_security:tenant_id(Principal)},
                            Idents),
          principal_sessions =
              maps:update_with(Principal, fun(N) -> N + 1 end, 1, Counts)}.

drop_session(Socket, #st{peers = Peers} = St) ->
    case maps:take(Socket, Peers) of
        {Peer, Peers1} ->
            St1 = forget_endpoint(Peer, St),
            %% A live session at this endpoint just lost its transport: to the
            %% gateway the client vanished, which is an abnormal ending (the
            %% Will fires). Without this the session would linger until its
            %% keepalive gave up.
            case maps:find(Peer, St1#st.endpoints) of
                {ok, Pid} -> Pid ! {sn_socket_gone};
                error -> ok
            end,
            St1#st{peers = Peers1};
        error ->
            St
    end.

forget_endpoint(Peer, #st{socks = Socks, idents = Idents,
                          principal_sessions = Counts} = St) ->
    Principal = case maps:find(Peer, Idents) of
                    {ok, #{principal := P}} -> P;
                    error -> undefined
                end,
    Counts1 = case Principal of
                  undefined -> Counts;
                  _ -> maps:update_with(Principal, fun(N) -> max(N - 1, 0) end,
                                        0, Counts)
              end,
    St#st{socks = maps:remove(Peer, Socks),
          idents = maps:remove(Peer, Idents),
          principal_sessions = Counts1}.

%% One write exit for both transports. UDP: any process may send on the
%% listener's socket. DTLS: only this process owns the peer socket, so replies
%% MUST come through here (a session writing `ssl:send' on a socket it does
%% not own is an error).
send_to(Endpoint, Data, #st{transport = udp, socket = Socket} = St) ->
    _ = gen_udp:send(Socket, element(1, Endpoint), element(2, Endpoint), Data),
    broker_metrics:inc(bytes_out, byte_size(Data)),
    St;
send_to(Endpoint, Data, #st{transport = dtls, socks = Socks} = St) ->
    case maps:find(Endpoint, Socks) of
        {ok, Socket} ->
            _ = ssl:send(Socket, Data),
            broker_metrics:inc(bytes_out, byte_size(Data));
        error ->
            %% The peer is gone (drop_session raced a reply): nothing to send.
            ok
    end,
    St.

%%%_* Misc ------------------------------------------------------------------

env(Key, Default) ->
    case application:get_env(broker_proto_mqtt_sn, Key, Default) of
        V when is_integer(V) -> V;
        _ -> Default
    end.
