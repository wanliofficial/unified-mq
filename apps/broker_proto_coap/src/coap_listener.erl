%% @doc The CoAP UDP listener: one datagram in, one datagram out.
%%
%% The shape is MQTT-SN's (ADR-022), with the session layer removed, because
%% CoAP has no session: a datagram carries its own Message ID, token and
%% options, so the listener is a stateless-per-datagram router into
%% `coap_resource' plus three pieces of *bounded* bookkeeping that live here
%% and nowhere else:
%%
%%   * CON dedup -- RFC 7252 §4.8.2: a duplicate CON within EXCHANGE_LIFETIME
%%     must be answered with the original ACK, not re-executed. Keyed by
%%     {IP, Port, Id} (ADR-031 §D2: never `{Port, IP}' as a *session* key --
%%     here it is only a transport routing tuple, per-datagram).
%%   * per-endpoint token bucket -- UDP has no handshake, so the source address
%%     is unverifiable and a flood is only observable as a rate (R-X3).
%%   * Block1 reassembly -- RFC 7959 draft §2.1: a POST/PUT body larger than one
%%     datagram arrives as a sequence, and the assembled buffer is bounded.
%%
%% The listener owns the socket and is the only writer to it, so there is no
%% hand-off and no second process to race the dedup table. Replies go out through
%% `send_to/4' -- `gen_udp:send/4' for plain UDP (any process may call it), or
%% `ssl:send/2' on the peer's established socket for DTLS.
%%
%% Reliability (ADR-031 §D3) is per-datagram and is decided here, before the
%% resource layer runs: a CON request is answered with a piggybacked ACK, a NON
%% request gets no piggybacked ACK at all (its reply, if the resource has one,
%% is a separate NON response). Neither is "reliable delivery" in the MQTT QoS 1
%% sense.
%%
%% Every table here is capped and swept on the one-second tick, so a flood of
%% new source addresses or a client that abandons a Block1 stream cannot grow
%% memory without bound.
%%
%% ## Two transports, one state machine (S11-C)
%%
%% A listener spec may say `#{transport => dtls}', which makes this process own
%% an `ssl' listen socket plus one established DTLS socket per peer. The
%% datagram path is unchanged: `on_packet/4' is the single entry for both
%% transports, and `send_to/4' is the single exit, so the codec, the resource
%% layer and Observe never learn which transport carried a message. The five
%% bounded tables keep the same `{IP, Port}' keys, because `ssl:peername/1'
%% reports the same tuple `gen_udp' does.
%%
%% DTLS is PSK-only and needs no certificate (S11-C): the PSK identity IS a
%% username in `broker_security''s `users' table, so an established session has
%% a real `principal' and a real tenant, and the ACL can finally deny a request.
%% Handshaking is delegated to `coap_dtls_acceptor' (one peer at a time) because
%% the identity is only observable inside the PSK lookup callback.
-module(coap_listener).

-behaviour(gen_server).

-include_lib("broker_core/include/broker_message.hrl").

-export([start_link/1, port/1, stop/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_continue/2,
         handle_info/2, terminate/2, code_change/3]).

-record(ep, {tokens :: non_neg_integer(),
             refill :: integer()}).

%% How many datagrams may be queued in the mailbox before the socket goes
%% passive; same budget as `mqtt_sn_listener' (ADR-022).
-define(ACTIVE_N, 100).

%% RFC 9175 §3.1: `Request-Tag' (option 292) names a block-wise request
%% independently of the token, which a client is free to change between blocks.
-define(REQUEST_TAG, 292).

-record(st, {
    %% The plain-UDP socket, or `undefined' when this listener speaks DTLS (the
    %% listen socket then lives in `dtls_listen'). Keeping them separate avoids
    %% lying about the type of a field that is only ever one of the two.
    socket :: gen_udp:socket() | undefined,
    port :: inet:port_number(),
    cfg :: map(),
    transport = udp :: udp | dtls,
    %% {IP, Port, Id} -> {ReplyBytes, AtMs} for a CON we already answered.
    dedup = #{} :: map(),
    %% {IP, Port} -> token-bucket state.
    endpoints = #{} :: map(),
    %% {IP, Port, Tag, Destination} -> {Assembled, Total, AtMs} for a Block1
    %% stream. `Tag' is the Request-Tag when the client sends one, else the
    %% token (RFC 9175 §3.1 / RFC 7959 §2.1); the Message ID changes from block
    %% to block and must not be the key.
    block1 = #{} :: map(),
    %% RFC 7641 Observe relationships: {IP,Port,Token} -> relationship()
    %% (a coap_observe:relationship()). Bounded by `max_observes'.
    observes = #{} :: map(),
    %% Un-acked CON notifications awaiting an empty ACK/RST: {IP,Port,MsgId} ->
    %% {Token, Bytes, RetransLeft, TimerRef}. Correlated by Message ID (a
    %% conformant empty ACK/RST carries no token -- RFC 7252 §4.1 / RFC 7641
    %% §4.5). Bytes are the encoded datagram, so a retransmit resends the same
    %% Message ID.
    pending = #{} :: map(),
    %% --- DTLS only (S11-C) ---
    %% The ssl:listen socket peers are accepted on.
    dtls_listen :: term() | undefined,
    %% The process that handshakes peers and transfers them here.
    dtls_acceptor :: pid() | undefined,
    %% Established DTLS socket -> its peer endpoint (inbound demux).
    peers = #{} :: map(),
    %% Peer endpoint -> its established DTLS socket (outbound demux). The two
    %% maps are inverses; both are needed because `{ssl,Sock,Bin}' arrives keyed
    %% by socket while every table and every send is keyed by endpoint.
    socks = #{} :: map(),
    %% Peer endpoint -> #{principal, tenant}: the identity the handshake proved,
    %% threaded into the resource layer's Ctx so ACL and tenant isolation apply.
    idents = #{} :: map(),
    %% Principal -> number of live sessions, for `dtls_max_sessions_per_principal'.
    principal_sessions = #{} :: map(),
    %% S12-B (ADR-032 D7): established DTLS socket -> its node-level admission
    %% lease. Kept beside `peers' rather than inside it because `peers' is read
    %% on the datagram path, where the lease is of no interest.
    admissions = #{} :: map()
}).

start_link(Spec) ->
    gen_server:start_link(?MODULE, Spec, []).

port(Pid) ->
    gen_server:call(Pid, port).

stop(Pid) ->
    gen_server:stop(Pid).

init(Spec) when is_map(Spec) ->
    process_flag(trap_exit, true),
    %% App-wide settings come from the application environment; the listener
    %% spec may override any of them (a second listener with its own limits).
    Cfg = config(Spec),
    case maps:get(transport, Cfg, udp) of
        udp -> init_udp(Cfg);
        dtls -> init_dtls(Cfg);
        Other -> {stop, {unknown_transport, Other}}
    end.

init_udp(Cfg) ->
    Port = maps:get(port, Cfg),
    Opts = [binary, {active, ?ACTIVE_N}, {reuseaddr, true},
            {recbuf, maps:get(recbuf_bytes, Cfg, 65536)}],
    case gen_udp:open(Port, Opts) of
        {ok, Socket} ->
            {ok, ActualPort} = inet:port(Socket),
            {ok, #st{socket = Socket, port = ActualPort, cfg = Cfg},
             {continue, refill}};
        {error, Reason} ->
            %% A listener that cannot bind is a boot failure (ADR-031 §D6,
            %% mirroring `broker_proto_mqtt_sn').
            {stop, {bind_failed, Port, Reason}}
    end.

%% A DTLS listener refuses to start without credentials: an empty `users' table
%% means no peer can ever complete a handshake, which is a configuration error,
%% not a safe default (design §4.3).
init_dtls(Cfg) ->
    {ok, _} = application:ensure_all_started(ssl),
    case has_users() of
        false ->
            {stop, {dtls_no_credentials}};
        true ->
            start_dtls(Cfg)
    end.

%% Read through `redacted_config/0' rather than the listener poking at
%% `broker_security''s internals: it is the public view of the identity table and
%% it keeps usernames (only the passwords are stripped).
has_users() ->
    Users = maps:get(users, broker_security:redacted_config(), []),
    Users =/= [].

start_dtls(Cfg) ->
    Port = maps:get(port, Cfg),
    Suites = cert_free_psk_suites(),
    case Suites of
        [] ->
            {stop, {dtls_no_suites}};
        _ ->
            %% Order matters: the listen options need the acceptor's pid in the
            %% PSK lookup state, so the acceptor must exist first; it blocks
            %% waiting for the socket this process is about to create.
            Self = self(),
            {ok, Acceptor} = coap_dtls_acceptor:start_link(Self, Cfg),
            %% Per-listener randomness for the decoy key (see `psk_lookup/2'):
            %% without it a decoy could be guessed offline from the public label.
            DecoySecret = crypto:strong_rand_bytes(32),
            ListenOpts = [{protocol, dtls},
                          {ciphers, Suites},
                          {user_lookup_fun,
                           {psk_lookup(Acceptor, DecoySecret), undefined}},
                          {reuseaddr, true},
                          %% `binary' matters: without it every `{ssl,Sock,Data}'
                          %% delivers an iolist and the codec -- which measures
                          %% and matches binaries -- would crash on the first
                          %% request. The UDP listener gets the same guarantee
                          %% from `gen_udp''s `binary' option.
                          {mode, binary},
                          {active, false}],
            case ssl:listen(Port, ListenOpts) of
                {ok, Listen} ->
                    %% `ssl:sockname/1' reports the whole address, unlike
                    %% `inet:port/1' for a UDP socket -- and with `port => 0' the
                    %% kernel-chosen port is a different number than requested,
                    %% so it must be read back rather than assumed.
                    {ok, {_Addr, ActualPort}} = ssl:sockname(Listen),
                    Acceptor ! {listen_socket, Listen},
                    {ok, #st{socket = undefined, port = ActualPort, cfg = Cfg,
                             transport = dtls, dtls_listen = Listen,
                             dtls_acceptor = Acceptor},
                     {continue, refill}};
                {error, Reason} ->
                    {stop, {bind_failed, Port, Reason}}
            end
    end.

%% The PSK callback. The identity is a username in `broker_security''s table;
%% resolving it IS the admission decision, and the key is derived from the stored
%% password so nothing secret is configured twice.
%%
%% An unknown identity gets a DECOY key, not an alert, and that is a security
%% requirement rather than a quirk: an alert fails the handshake before any key
%% exchange is verified, so the client sees a fast, distinct error, while a known
%% identity with a wrong password fails later and looks like a timeout. Measured
%% on OTP 28.3.1: alert in ~20-200 ms versus a 3 s timeout -- enough to enumerate
%% every username in the table. With a decoy both cases die at the Finished
%% check and are indistinguishable from outside. The decoy is salted with a
%% per-listener random secret so it can never coincide with a derived key and
%% cannot be confirmed offline.
%%
%% The callback runs inside `ssl:handshake/2' in the acceptor, so it also posts
%% the identity back to that process -- the only place `psk_identity' is
%% observable on the server side.
psk_lookup(Acceptor, DecoySecret) ->
    fun(psk, Identity, _State) ->
            Acceptor ! {psk_identity, Identity},
            case broker_security:psk_for_identity(Identity) of
                {ok, Psk} -> {ok, Psk};
                {error, _} -> {ok, decoy_psk(Identity, DecoySecret)}
            end
    end.

decoy_psk(Identity, Secret) ->
    crypto:hash(sha256, [<<"unifiedmq-coaps-psk-decoy">>, 0, Secret, 0, Identity]).

%% The cert-free PSK suites. OTP's `all' category only surfaces `rsa_psk', which
%% would drag in certificate handling; the suites that need no server
%% certificate at all live in the `anonymous' category, and AEAD ciphers are the
%% only ones worth negotiating here.
cert_free_psk_suites() ->
    [S || S <- ssl:cipher_suites(anonymous, 'dtlsv1.2'),
          lists:member(maps:get(key_exchange, S, none), [psk, dhe_psk, ecdhe_psk]),
          lists:member(maps:get(cipher, S, none),
                       [aes_128_gcm, aes_256_gcm, aes_128_ccm, aes_256_ccm,
                        aes_128_ccm_8, aes_256_ccm_8])].

handle_continue(refill, St) ->
    _ = erlang:start_timer(1000, self(), refill),
    {noreply, St}.

%% The app-wide defaults, with the listener spec able to override any of them.
config(Spec) ->
    Defaults = #{max_message_bytes => env(max_message_bytes, 65535),
                 dedup_window_ms => env(dedup_window_ms, 247000),
                 max_dedup_entries => env(max_dedup_entries, 4096),
                 max_requests_per_second => env(max_requests_per_second, 100),
                 max_endpoints => env(max_endpoints, 4096),
                 block_size => env(block_size, 1024),
                 max_block1_bytes => env(max_block1_bytes, 262144),
                 resources => env(resources, []),
                 recbuf_bytes => env(recbuf_bytes, 65536),
                 max_observes => env(max_observes, 4096),
                 observe_ack_timeout_ms => env(observe_ack_timeout_ms, 2000),
                 observe_max_retrans => env(observe_max_retrans, 3),
                 observe_max_age_s => env(observe_max_age_s, 30),
                 dtls_transport_accept_timeout_ms =>
                     env(dtls_transport_accept_timeout_ms, 1000),
                 dtls_handshake_timeout_ms => env(dtls_handshake_timeout_ms, 2000),
                 max_dtls_sessions => env(max_dtls_sessions, 1024),
                 dtls_max_sessions_per_principal =>
                     env(dtls_max_sessions_per_principal, 16)},
    maps:merge(Defaults, Spec).

env(Key, Default) ->
    application:get_env(broker_proto_coap, Key, Default).

handle_call(port, _From, #st{port = P} = St) ->
    {reply, {ok, P}, St};
handle_call(_Req, _From, St) ->
    {reply, {error, unknown_call}, St}.

handle_cast(_Msg, St) ->
    {noreply, St}.

%% The kernel delivered a message to an attached subscription (ADR-031 §D2:
%% the listener is the notify pid). Turn it into Observe notifications.
handle_info({broker_deliver, <<"coap">>, _MaxQoS, _Matched, {_P, _O, Msg}},
            #st{} = St) ->
    {noreply, dispatch_notifications(Msg, St)};

%% An ACK for a CON notification cancels its retransmit. (The seq was already
%% advanced when the notification was sent, so nothing to do but cancel.) The
%% A datagram is decoded once here, guarded, so a malformed packet cannot turn
%% into a double `on_datagram/4' via the catch. A conformant empty ACK (RFC 7252
%% §4.1) or empty RST (RFC 7641 §4.5) carries NO token -- it is correlated to a
%% pending CON notification by the echoed Message ID, which is why `pending' is
%% keyed by `{IP,Port,Id}'. An ACK cancels the retransmit timer and keeps the
%% relationship; an RST ends it. Anything else (including a non-empty-code RST)
%% falls through to `on_datagram/4'.
handle_info({udp, _Socket, IP, Port, Bin}, St) ->
    {noreply, on_packet(IP, Port, Bin, St)};

%% --- DTLS (S11-C) ---------------------------------------------------------
%%
%% A peer finished its handshake in the acceptor and asked whether it fits. The
%% caps and the tables live here, so this is the single place that decides.
handle_info({dtls_handshake_ok, From, Socket, Peer, Identity}, St) ->
    case admit_session(Identity, Peer, Socket, St) of
        {ok, St1} ->
            From ! {dtls_admit, Socket, ok},
            {noreply, St1};
        {refuse, Why} ->
            logger:notice("coap dtls session refused (~p) peer=~p identity=~p",
                          [Why, Peer, Identity]),
            From ! {dtls_admit, Socket, refuse},
            {noreply, St}
    end;

%% The acceptor transferred ownership: the socket is ours now, so arming the
%% active budget here is both legal and correct. Doing it before the transfer
%% returns `ok' and yet delivers datagrams to the old owner (measured: the first
%% budget's worth of traffic vanished with the acceptor).
handle_info({dtls_owned, Socket}, #st{peers = Peers} = St) ->
    case maps:is_key(Socket, Peers) of
        true -> _ = ssl:setopts(Socket, [{active, ?ACTIVE_N}]);
        false -> ok
    end,
    {noreply, St};

%% The acceptor admitted this session, then failed to hand the socket over (or
%% the transfer timed out). The record was written when we answered `ok', so it
%% must be released here -- the socket was never ours, so no `{ssl_closed,_}'
%% will ever arrive to trigger `drop_session/2' (spec review B1).
handle_info({dtls_abandoned, Socket}, St) ->
    {noreply, drop_session(Socket, St)};

%% Decrypted datagram from an established peer. Mapped back to the endpoint so
%% the rest of the pipeline is transport-blind.
handle_info({ssl, Socket, Bin}, #st{peers = Peers} = St) ->
    case maps:find(Socket, Peers) of
        {ok, {IP, Port}} ->
            {noreply, on_packet(IP, Port, Bin, St)};
        error ->
            %% Closed in between: the peer is gone, nothing to answer.
            {noreply, St}
    end;

handle_info({ssl_closed, Socket}, St) ->
    {noreply, drop_session(Socket, St)};

handle_info({ssl_error, Socket, Reason}, St) ->
    logger:debug("coap dtls socket error: ~p", [Reason]),
    {noreply, drop_session(Socket, St)};

%% The active budget ran out: re-arm it, or the listener silently stops reading.
handle_info({ssl_passive, Socket}, #st{peers = Peers} = St) ->
    case maps:is_key(Socket, Peers) of
        true -> _ = ssl:setopts(Socket, [{active, ?ACTIVE_N}]);
        false -> ok
    end,
    {noreply, St};

%% The acceptor is the only process that can admit new peers; if it dies the
%% listener can never accept again, which is a restart, not a degraded state.
handle_info({'EXIT', Acceptor, Reason}, #st{dtls_acceptor = Acceptor} = St) ->
    {stop, {dtls_acceptor_died, Reason}, St};

%% A CON notification went un-acked: retransmit exactly that one (the timer
%% message carries its `pending' key) or give up after `observe_max_retrans'
%% tries. Aiming at the fired entry by key -- rather than folding the whole
%% `pending' table -- is what stops one relationship's timer from resending (and
%% spending the retransmit budget of) every other relationship's notification.
handle_info({timeout, Ref, {observe_ack, IdKey}}, #st{} = St) ->
    {noreply, retransmit_one(IdKey, Ref, St)};

%% The 1 s refill tick also sweeps expired Observe relationships.
handle_info({timeout, _Ref, refill}, St) ->
    {noreply, sweep_observes(prune(St)), {continue, refill}};

%% `{active, N}' is a budget, not a mode: when it runs out the socket goes
%% passive and must be re-armed, or the listener silently stops reading. The
%% budget (not `{active, true}') is what keeps a flood from filling the mailbox.
handle_info({udp_passive, Socket}, #st{socket = Socket} = St) ->
    _ = inet:setopts(Socket, [{active, ?ACTIVE_N}]),
    {noreply, St};

handle_info({udp_error, Socket, Reason}, #st{socket = Socket} = St) ->
    logger:warning("coap listener socket error: ~p", [Reason]),
    {noreply, St};

handle_info(_Info, St) ->
    {noreply, St}.

terminate(_Reason, #st{transport = dtls} = St) ->
    %% Close the listen socket first so the acceptor stops accepting, then every
    %% established peer. Left to the runtime, a restarted listener could find the
    %% port still held. Each session closes here rather than through
    %% `drop_session/2', so the gauge and the admission slot are settled in the
    %% same place -- the listener going down must not leave leases behind.
    _ = try ssl:close(St#st.dtls_listen) catch _:_ -> ok end,
    [begin
         _ = try ssl:close(S) catch _:_ -> ok end,
         _ = broker_metrics:connection_closed(true),
         ok = broker_admission:release(maps:get(S, St#st.admissions, undefined))
     end || S <- maps:keys(St#st.peers)],
    ok;
terminate(_Reason, _St) ->
    ok.

code_change(_Old, St, _Extra) ->
    {ok, St}.

%% One datagram, from either transport. The ACK/RST correlation runs first
%% because a conformant empty ACK/RST carries no token -- it is matched to a
%% pending CON notification by the echoed Message ID (RFC 7252 §4.1 / RFC 7641
%% §4.5). Anything else is a request and takes the normal path.
on_packet(IP, Port, Bin, #st{pending = Pend} = St0) ->
    Decoded =
        try coap_codec:decode(Bin) of
            {ok, #{type := ack, code := empty, id := A}} -> {ack, A};
            {ok, #{type := rst, code := empty, id := R}} -> {rst, R};
            {ok, _Msg} -> other;
            {error, _} -> other
        catch
            _:_ -> other
        end,
    case Decoded of
        {ack, Aid} ->
            case maps:find({IP, Port, Aid}, Pend) of
                {ok, {_Token, _Bytes, _Left, Ref}} ->
                    _ = erlang:cancel_timer(Ref),
                    St0#st{pending = maps:remove({IP, Port, Aid}, Pend)};
                error ->
                    on_datagram(IP, Port, Bin, St0)
            end;
        {rst, Rid} ->
            %% A CON notification's retransmit is keyed by its Message ID in
            %% `pending'; if its timer is still armed, cancel it and drop the
            %% entry. If not (already ACKed, superseded by NSTART=1, or not yet
            %% recorded because the notification is dispatched asynchronously), no
            %% timer to cancel.
            St1 =
                case maps:take({IP, Port, Rid}, Pend) of
                    {{_Token, _Bytes, _Left, Ref}, Rest} ->
                        _ = erlang:cancel_timer(Ref),
                        St0#st{pending = Rest};
                    error ->
                        St0
                end,
            %% End the relationship whose CURRENT notification carries this ID. A
            %% conformant empty RST has no token, so the relationship is found by
            %% scanning `observes' for a matching notification id (RFC 7641
            %% §4.5) -- this works whether or not the notification is still in
            %% `pending', so a client may RST immediately.
            case observe_by_notify_id(IP, Port, Rid, St1#st.observes) of
                undefined ->
                    %% No such notification on this endpoint: a stray/late RST. Not
                    %% a request, no response owed (RFC 7252 §4.3).
                    St1;
                {_, _, _} = Key ->
                    deregister_observe(element(1, Key), element(2, Key),
                                       element(3, Key), St1)
            end;
        other ->
            on_datagram(IP, Port, Bin, St0)
    end.

%%%_* Datagram path --------------------------------------------------------

on_datagram(IP, Port, Bin, #st{cfg = Cfg} = St) ->
    broker_metrics:inc(bytes_in, byte_size(Bin)),
    case byte_size(Bin) > maps:get(max_message_bytes, Cfg, 65535) of
        true ->
            broker_metrics:inc(frame_decode_errors),
            St;
        false ->
            case allowed(IP, Port, Cfg, St) of
                {false, St1} ->
                    broker_metrics:inc(protocol_errors),
                    St1;
                {true, St1} ->
                    dispatch(IP, Port, Bin, Cfg, St1)
            end
    end.

%% Token bucket, `max_requests_per_second' tokens refilled at that rate. The
%% bucket is consumed here, before decoding: work is what a flood must not buy.
%% An endpoint the table has never seen is refused once the table is full
%% (`max_endpoints'), so a spoofed-source flood cannot evict live buckets.
allowed(IP, Port, Cfg, #st{endpoints = Eps} = St) ->
    Max = maps:get(max_requests_per_second, Cfg, 100),
    Cap = maps:get(max_endpoints, Cfg, 4096),
    Key = {IP, Port},
    Now = erlang:monotonic_time(millisecond),
    case maps:find(Key, Eps) of
        error when map_size(Eps) >= Cap ->
            {false, St};
        Found ->
            Ep = case Found of
                     {ok, E} -> E;
                     error -> #ep{tokens = Max, refill = Now}
                 end,
            #ep{tokens = Tokens} = Ep1 = refill(Ep, Now, Max),
            case Tokens > 0 of
                true ->
                    {true, St#st{endpoints = maps:put(
                                            Key, Ep1#ep{tokens = Tokens - 1}, Eps)}};
                false ->
                    {false, St#st{endpoints = maps:put(Key, Ep1, Eps)}}
            end
    end.

refill(#ep{tokens = T, refill = Last} = Ep, Now, Max) ->
    case Now - Last >= 1000 of
        true -> Ep#ep{tokens = min(Max, T + (Now - Last) div 1000), refill = Now};
        false -> Ep
    end.

dispatch(IP, Port, Bin, Cfg, St) ->
    case coap_codec:decode(Bin) of
        {error, _} ->
            broker_metrics:inc(frame_decode_errors),
            St;
        {ok, Msg} ->
            handle_message(IP, Port, Msg, Cfg, St)
    end.

handle_message(IP, Port, Msg, Cfg, #st{dedup = Dedup} = St) ->
    case coap_codec:type(Msg) of
        ack ->
            %% Cancels a pending CON notification; the udp clause already matched
            %% it against `pending'. Not a request: no response owed.
            St;
        rst ->
            %% An empty RST is correlated and handled in the udp clause (by
            %% Message ID). A non-empty-code RST reaching here is malformed
            %% (RFC 7252 §4.1): drop it rather than treat it as a command.
            St;
        con ->
            %% Already answered this exact CON? Re-send the cached bytes without
            %% re-executing (RFC 7252 §4.8.2).
            case maps:find({IP, Port, coap_codec:id(Msg)}, Dedup) of
                {ok, {Cached, _At}} ->
                    send_to(IP, Port, Cached, St),
                    St;
                error ->
                    {Reply, St1} = respond(IP, Port, Msg, Cfg, St),
                    send_to(IP, Port, Reply, St1),
                    remember(IP, Port, coap_codec:id(Msg), Reply, Cfg, St1)
            end;
        _Non ->
            %% A NON request still gets the resource's answer, as a separate NON.
            {Reply, St1} = respond(IP, Port, Msg, Cfg, St),
            send_to(IP, Port, Reply, St1),
            St1
    end.

%% Run the datagram through Block1 reassembly, then through the resource layer,
%% and return the bytes to send. A mid-stream block is answered with 2.31
%% Continue; an oversized or incomplete stream with 4.13 / 4.08.
respond(IP, Port, Msg, Cfg, St) ->
    case block1_stream(IP, Port, Msg, Cfg, St) of
        {single, Req, St1} ->
            reply_observe(IP, Port, Req, Cfg, St1);
        {done, Req, St1} ->
            reply_observe(IP, Port, Req, Cfg, St1);
        {streaming, Continue, St1} ->
            {coap_codec:encode(Continue), St1};
        {refused, Refusal, St1} ->
            {coap_codec:encode(Refusal), St1}
    end.

%% GET with Observe=0 registers a relationship and answers the initial 2.05 with
%% `Observe: 0' (RFC 7641 §3.4); Observe=1 deregisters it and answers the GET as
%% usual -- the current representation with NO Observe option (§4.1). Everything
%% else ignores Observe entirely. The registration lives here, not in
%% `coap_resource', because the listener owns the socket, the relationship table
%% and the kernel attachment.
reply_observe(IP, Port, Req, Cfg, St) ->
    case observe_action(Req) of
        none ->
            {resource_reply(Req, Cfg, {IP, Port}, St), St};
        register ->
            {Code, Options, Payload} = run_resource(Req, Cfg, {IP, Port}, St),
            case Code of
                {response, 2, 5} ->
                    case register_observe(IP, Port, coap_codec:token(Req), Req,
                                          Cfg, St) of
                        {St2, undefined} ->
                            %% Bounded table full (or the kernel refused the
                            %% subscription): refuse the observation.
                            {encode_reply(Req, reply_type(Req), {response, 4, 3},
                                          [], <<>>), St2};
                        {St2, Rel} ->
                            Opts = [{observe, coap_observe:seq(Rel)} | Options],
                            {block2_slice(Req, reply_type(Req), Code, Opts,
                                          Payload), St2}
                    end;
                _ ->
                    %% 4.04 / 5.xx: nothing to observe, answer as-is, no
                    %% registration.
                    {encode_reply(Req, reply_type(Req), Code, Options, Payload),
                     St}
            end;
        deregister ->
            St2 = deregister_observe(IP, Port, coap_codec:token(Req), St),
            {resource_reply(Req, Cfg, {IP, Port}, St2), St2}
    end.

%% Observe only applies to GET (RFC 7641 §3.1): 0 registers, 1 deregisters.
observe_action(Req) ->
    case {coap_codec:code(Req), coap_codec:option(observe, Req)} of
        {{method, get}, 0} -> register;
        {{method, get}, 1} -> deregister;
        _ -> none
    end.

%% The single write path for both transports. A DTLS peer that vanished between
%% the decision to send and the send itself is not an error: UDP has always been
%% "send is not delivery", and a closed socket must not turn that into a crash.
send_to(IP, Port, Bin, #st{transport = udp, socket = Socket}) ->
    _ = gen_udp:send(Socket, IP, Port, Bin),
    ok;
send_to(IP, Port, Bin, #st{socks = Socks}) ->
    case maps:find({IP, Port}, Socks) of
        {ok, Socket} ->
            _ = ssl:send(Socket, Bin),
            ok;
        error ->
            ok
    end.

resource_reply(Req, Cfg, Endpoint, St) ->
    {ReplyCode, Options, Payload} = run_resource(Req, Cfg, Endpoint, St),
    block2_slice(Req, reply_type(Req), ReplyCode, Options, Payload).

%%%_* Block2 (server-side chunking, RFC 7959 §3) --------------------------------
%%
%% A GET carrying a `block2' option is answered with the requested slice of the
%% resource's full representation; a GET without one is answered whole (the
%% pre-Block2 behaviour, unchanged). Only a 2.xx content response is sliced: an
%% error code (4.04 / 4.05 / 5.xx) has no body to divide, so it passes through.
%% The block size is the client's requested `ReqSize' (Block2 lets the downloader
%% pick it); there is no server-side cache -- each block is a fresh projection of
%% the current retained state, so no per-transfer table is needed.
block2_slice(Req, Type, Code, Options, Payload) ->
    case {is_content_code(Code), coap_codec:block(block2, Req)} of
        {true, {Num, _More, ReqSize}} ->
            {Slice, ActualSize} = block2_payload(Payload, Num, ReqSize),
            More = block2_more(Payload, Num, ReqSize),
            BlockOpts = lists:keystore(block2, 1, Options,
                                       {block2, coap_codec:block_value(Num, More, ActualSize)}),
            encode_reply(Req, Type, Code, BlockOpts, Slice);
        _ ->
            encode_reply(Req, Type, Code, Options, Payload)
    end.

%% True when the code is a 2.xx success carrying a representational body.
is_content_code({response, Class, _Detail}) when Class >= 2, Class =< 2 ->
    true;
is_content_code(_) ->
    false.

%% The Num-th `ReqSize'-byte slice of `Payload', and the size actually used
%% (== ReqSize except when the final block is short). An out-of-range Num yields
%% an empty slice -- RFC 7959 §3.5 lets the server close with an empty last
%% block (More=false), which the client recognises and stops on.
block2_payload(Payload, Num, ReqSize) ->
    Offset = Num * ReqSize,
    Total = byte_size(Payload),
    case Offset >= Total of
        true ->
            {<<>>, ReqSize};
        false ->
            Len = min(ReqSize, Total - Offset),
            Slice = binary:part(Payload, Offset, Len),
            {Slice, ReqSize}
    end.

%% More=true when at least one byte follows this block.
block2_more(Payload, Num, ReqSize) ->
    ((Num + 1) * ReqSize) < byte_size(Payload).

%% RFC 7252 §5.2.1: a reply to a CON rides in the ACK; a reply to a NON is a
%% separate NON response. Both echo the request's token.
reply_type(Req) ->
    case coap_codec:type(Req) of
        con -> ack;
        Other -> Other
    end.

encode_reply(Req, Type, ReplyCode, Options, Payload) ->
    Base = coap_codec:response(Type, ReplyCode, Payload),
    WithId = Base#{id => coap_codec:id(Req), token => coap_codec:token(Req)},
    WithOptions = lists:foldl(fun({Name, Val}, Acc) ->
                                     coap_codec:put_option(Name, Val, Acc)
                             end, WithId, Options),
    coap_codec:encode(WithOptions).

%%%_* Block1 reassembly ----------------------------------------------------

block1_stream(IP, Port, Msg, Cfg, #st{block1 = Streams} = St) ->
    case coap_codec:block(block1, Msg) of
        undefined ->
            {single, Msg, St};
        {Num, More, Size} ->
            %% RFC 7959 §2.1 says the token identifies a block-wise transfer,
            %% but RFC 9175 §3.1 adds `Request-Tag' precisely because a client
            %% may let the token change between blocks (libcoap does: block 0
            %% carries an empty token, block 1 an 8-byte one). libcoap sends a
            %% stable Request-Tag, so that is the identity when present; the
            %% token stays the fallback for clients that keep it fixed.
            Key = {IP, Port, block1_tag(Msg), coap_codec:destination(Msg)},
            Chunk = coap_codec:payload(Msg),
            Ceiling = maps:get(max_block1_bytes, Cfg, 262144),
            Now = erlang:monotonic_time(millisecond),
            case maps:find(Key, Streams) of
                {ok, {Acc, Total, _At}} ->
                    assemble(Msg, Num, More, Size, <<Acc/binary, Chunk/binary>>,
                             Total, Ceiling, Streams, Key, Now, St);
                error when Num =:= 0 ->
                    assemble(Msg, Num, More, Size, Chunk, 0, Ceiling,
                             Streams, Key, Now, St);
                error ->
                    %% A block after the first with no stream to append to: the
                    %% earlier blocks are gone (expired or never seen), so the
                    %% body cannot be completed -- 4.08, and do not dispatch a
                    %% truncated body as if it were whole.
                    {refused, refusal(Msg, {response, 4, 8}),
                     St#st{block1 = maps:remove(Key, Streams)}}
            end
    end.

%% The stable identity of a Block1 transfer (RFC 9175 §3.1): `Request-Tag'
%% when the client sends it, otherwise the token (RFC 7959 §2.1).
block1_tag(Msg) ->
    case coap_codec:option(?REQUEST_TAG, Msg) of
        undefined -> {token, coap_codec:token(Msg)};
        Tag       -> {request_tag, Tag}
    end.

assemble(Msg, Num, More, Size, Assembled, PrevTotal, Ceiling, Streams, Key, Now,
         St) ->
    Total = PrevTotal + byte_size(coap_codec:payload(Msg)),
    case Total > Ceiling of
        true ->
            %% RFC 7959 draft §2.2: a recipient MAY reject an oversized body.
            {refused, refusal(Msg, {response, 4, 13}),
             St#st{block1 = maps:remove(Key, Streams)}};
        false ->
            case More of
                true ->
                    Continue = continue_reply(Msg, Size, Num),
                    {streaming, Continue,
                     St#st{block1 = maps:put(Key, {Assembled, Total, Now}, Streams)}};
                false ->
                    {done, Msg#{payload => Assembled},
                     St#st{block1 = maps:remove(Key, Streams)}}
            end
    end.

%% The 2.31 Continue echoes the request's type/id/token and the *received*
%% block number with M=1, as RFC 7959 §3.2 Figure 7 shows: a request carrying
%% `1:0/1/128' is answered `1:0/1/128', not `1:1/1/128'. NUM acknowledges the
%% block just stored; the client infers the next number (NUM+1) itself.
continue_reply(Msg, Size, Num) ->
    Resp = refusal_base(Msg, {response, 2, 31}),
    coap_codec:put_option(block1,
                         coap_codec:block_value(Num, true, Size), Resp).

refusal(Msg, Code) ->
    refusal_base(Msg, Code).

refusal_base(Msg, Code) ->
    Resp = coap_codec:response(reply_type(Msg), Code, <<>>),
    Resp#{id => coap_codec:id(Msg), token => coap_codec:token(Msg)}.

%%%_* Resource call ---------------------------------------------------------

%% The context the resource layer sees. On plain UDP there is no credential, so
%% `principal' stays `undefined' and `broker_security' treats the request as the
%% default tenant -- an honest statement of what the wire carries. On DTLS the
%% handshake proved an identity, so a real `principal' and its tenant go in, and
%% the ACL and tenant isolation finally apply (S11-C).
run_resource(Req, Cfg, Endpoint, St) ->
    {Tenant, Principal} = endpoint_identity(Endpoint, St),
    Ctx = #{endpoint => Endpoint,
            tenant => Tenant,
            principal => Principal},
    try coap_resource:handle(Req, Cfg, Ctx) of
        {reply, Code, Options, Payload} ->
            {Code, Options, Payload}
    catch
        _:_ -> {{response, 5, 0}, [], <<>>}
    end.

endpoint_identity(Endpoint, #st{idents = Idents}) ->
    case maps:find(Endpoint, Idents) of
        {ok, #{principal := P, tenant := T}} ->
            {T, P};
        error ->
            {broker_security:default_tenant(), undefined}
    end.

%%%_* DTLS sessions (S11-C) -------------------------------------------------

%% Decide whether an established peer fits, and if so record it. The caps are
%% checked here (not in the acceptor) because the tables live here: a second copy
%% of the counters would be a second truth.
admit_session(Identity, Peer, Socket, #st{peers = Peers, cfg = Cfg} = St) ->
    Max = maps:get(max_dtls_sessions, Cfg, 1024),
    case maps:size(Peers) >= Max of
        true ->
            {refuse, session_cap};
        false ->
            %% An identity that did not arrive is a handshake that must not be
            %% admitted: the whole point of DTLS here is that `principal' is
            %% proven. (It cannot happen on the PSK path -- the lookup either
            %% supplies the key or fails the handshake -- but the guard keeps
            %% the invariant local.)
            case Identity of
                undefined ->
                    {refuse, no_identity};
                _ ->
                    Principal = Identity,
                    PerCap = maps:get(dtls_max_sessions_per_principal, Cfg, 16),
                    case maps:get(Principal, St#st.principal_sessions, 0) >= PerCap of
                        true ->
                            {refuse, principal_cap};
                        false ->
                            %% S12-B (ADR-032 D7): the node-level gate, last of
                            %% the three because it is the only one whose refusal
                            %% has nothing to do with this peer in particular.
                            %% A session that is refused here was never recorded,
                            %% so `drop_session/2' stays the only release point.
                            case broker_admission:try_acquire() of
                                {rejected, _Why} ->
                                    {refuse, node_cap};
                                Lease ->
                                    {ok, record_session(Peer, Socket, Principal,
                                                        Lease, St)}
                            end
                    end
            end
    end.

%% S12-B (ADR-032 D7): a DTLS session is the only thing in this front end that
%% is a connection -- plain UDP is stateless per datagram and has no lifecycle to
%% count, which the capability matrix states rather than papers over. The count
%% goes up here, at the one place a session is recorded, and down in
%% `drop_session/2', its only removal path, so the two cannot drift.
record_session({IP, Port} = Peer, Socket, Principal, Lease,
               #st{peers = Peers, socks = Socks, idents = Idents,
                   principal_sessions = Counts, admissions = Admissions} = St) ->
    _ = broker_metrics:connection_opened(),
    St#st{peers = maps:put(Socket, Peer, Peers),
          socks = maps:put({IP, Port}, Socket, Socks),
          idents = maps:put({IP, Port}, #{principal => Principal,
                                          tenant => broker_security:tenant_id(Principal)},
                            Idents),
          principal_sessions = maps:update_with(Principal, fun(N) -> N + 1 end, 1,
                                                Counts),
          admissions = maps:put(Socket, Lease, Admissions)}.

%% A DTLS peer disappeared. Unlike plain UDP -- where an endpoint just goes
%% silent and its entries age out -- here the close is a positive fact, so every
%% table gets cleaned at once. Skipping this would let dead endpoints hold
%% buckets, dedup slots and observations until the timers eventually caught up.
drop_session(Socket, #st{peers = Peers, admissions = Admissions} = St) ->
    case maps:take(Socket, Peers) of
        error ->
            %% Never recorded, so never counted: `admit_session/4' answers
            %% `refuse' without touching the gauge or the gate.
            St;
        {Peer, Peers1} ->
            _ = broker_metrics:connection_closed(true),
            %% S12-B (ADR-032 D7): the admission slot goes back on the same path
            %% the recorded session does, so a refused session never releases
            %% somebody else's.
            ok = broker_admission:release(maps:get(Socket, Admissions, undefined)),
            St1 = St#st{peers = Peers1,
                        admissions = maps:remove(Socket, Admissions)},
            forget_endpoint(Peer, remove_session(Peer, St1))
    end.

remove_session(Peer, #st{socks = Socks, idents = Idents,
                         principal_sessions = Counts} = St) ->
    St1 = St#st{socks = maps:remove(Peer, Socks),
                idents = maps:remove(Peer, Idents)},
    case maps:find(Peer, Idents) of
        {ok, #{principal := Principal}} ->
            St1#st{principal_sessions = decrement(Principal, Counts)};
        error ->
            St1
    end.

decrement(Key, Map) ->
    case maps:get(Key, Map, 0) of
        N when N =< 1 -> maps:remove(Key, Map);
        N -> maps:put(Key, N - 1, Map)
    end.

%% Every per-endpoint entry this listener holds, dropped. Observe relationships
%% go through the normal teardown so the kernel subscription and the retransmit
%% timers stay consistent with the relationship table.
forget_endpoint({IP, Port}, St0) ->
    Keys = [K || {{KIP, KPort, _} = K, _Rel} <- maps:to_list(St0#st.observes),
                 KIP =:= IP, KPort =:= Port],
    St1 = lists:foldl(fun({_, _, Token}, Acc) ->
                              deregister_observe(IP, Port, Token, Acc)
                      end, St0, Keys),
    Pend = maps:filter(fun({PIP, PPort, _}, _) -> not (PIP =:= IP andalso PPort =:= Port) end,
                       St1#st.pending),
    Dedup = maps:filter(fun({DIP, DPort, _}, _) ->
                                not (DIP =:= IP andalso DPort =:= Port)
                        end, St1#st.dedup),
    Block1 = maps:filter(fun({BIP, BPort, _, _}, _) ->
                                 not (BIP =:= IP andalso BPort =:= Port)
                         end, St1#st.block1),
    St1#st{pending = Pend, dedup = Dedup, block1 = Block1,
           endpoints = maps:remove({IP, Port}, St1#st.endpoints)}.

%%%_* Observe (RFC 7641) ----------------------------------------------------

-define(COAP_CLIENT, <<"coap">>).

%% Called by respond/5 when a GET with Observe=0 has been answered 2.05. It
%% mints the kernel subscription (ADR-031 §D2) and records the relationship.
%% Returns {St, Rel} on success, or {St, undefined} when the bounded table is at
%% capacity -- the caller then answers 4.03 instead of the 2.05.
%%
%% A same-token re-GET is a refresh, not a new observation (RFC 7641 §3.3.1): the
%% sequence is preserved so notifications never go backwards (§4.4), and any
%% inherited un-acked CON notification for the old relationship is dropped.
register_observe(IP, Port, Token, Req, Cfg, #st{observes = Obs} = St) ->
    Dest = coap_codec:destination(Req),
    %% The subscription lives in the kernel under the peer's real tenant, so a
    %% DTLS device observes inside its own tenant and the kernel's dispatch
    %% respects that boundary. Plain UDP has no principal and keeps the default.
    Tenant = element(1, endpoint_identity({IP, Port}, St)),
    MaxAge = coap_codec:option(max_age, Req, 30),
    Key = {IP, Port, Token},
    %% Only a *new* relationship consumes a table slot; a refresh of an existing
    %% one is never refused for capacity.
    case not maps:is_key(Key, Obs) andalso maps:size(Obs) >=
             maps:get(max_observes, Cfg, 4096) of
        true ->
            {St, undefined};
        false ->
            Prev = maps:get(Key, Obs, undefined),
            case broker_subscription:subscribe(?COAP_CLIENT,
                                               coap_observe:filter(Dest), 0,
                                               Tenant) of
                {ok, SubRef} ->
                    %% A single anonymous listener taking over the `<<"coap">>'
                    %% attachment is intended; the previous pid is irrelevant.
                    {ok, _Prev} = broker_subscription:attach(?COAP_CLIENT, self(),
                                                             false),
                    Rel0 = coap_observe:register(Dest, Tenant, SubRef, MaxAge,
                                                 now_ms()),
                    Rel = case Prev of
                              undefined ->
                                  Rel0;
                              #{seq := Seq} ->
                                  %% Continue the sequence across a refresh.
                                  Rel0#{seq => Seq}
                          end,
                    %% A GET is never an ACK/RST; narrow so the relationship's
                    %% req_type stays in `con | non' (what notify_message sends).
                    ReqType = case coap_codec:type(Req) of
                                  con -> con;
                                  non -> non;
                                  _ -> con
                              end,
                    Rel1 = Rel#{req_type => ReqType},
                    St1 = St#st{observes = maps:put(Key, Rel1, Obs)},
                    {cancel_pending_for(Key, St1), Rel1};
                {error, _} ->
                    {St, undefined}
            end
    end.

%% Find the observe relationship, ON THE SENDER'S ENDPOINT, whose MOST RECENT
%% notification carries Message ID `Id'. A conformant empty RST has no token, so
%% this scan of `observes' is how a client's RST is matched to a relationship
%% (RFC 7641 §4.5). It is scoped to `{IP,Port,_}' so a RST from one client can
%% never tear down another endpoint's relationship. Returns the relationship key,
%% or `undefined' when no relationship on that endpoint has such a notification.
observe_by_notify_id(IP, Port, Id, Observes) ->
    lists:foldl(
      fun({{SIP, SPort, _} = Key, Rel}, Found) when SIP =:= IP,
                                                   SPort =:= Port ->
              case maps:get(req_id, Rel, undefined) of
                  Id -> Key;
                  _ -> Found
              end;
         ({_OtherKey, _Rel}, Found) ->
              Found
      end, undefined, maps:to_list(Observes)).

%% Called by respond/5 for a GET with Observe=1, and by the udp clause for an
%% RST correlated by Message ID. Drops the relationship and unsubscribes that one
%% subscription from the kernel. Always succeeds (RFC 7641 §3.5: deregistering a
%% relationship that is not there is not an error).
%%
%% NOTE granularity: `broker_subscription:unsubscribe/2' removes exactly one
%% subscription, but `detach/2' tears down the WHOLE client session (every
%% `<<"coap">>' subscription). Because all observations share one anonymous
%% ClientId, detach is only safe once the LAST relationship is gone -- see
%% `maybe_detach/1'. Likewise the shared subscription must survive while ANY other
%% relationship on the same destination is still live -- see `maybe_unsubscribe/2'.
deregister_observe(IP, Port, Token, St) ->
    Key = {IP, Port, Token},
    case coap_observe:deregister(Key, St#st.observes) of
        {undefined, Obs1} ->
            cancel_pending_for(Key, St#st{observes = Obs1});
        {Rel, Obs1} ->
            St1 = maybe_unsubscribe(maps:get(dest, Rel),
                                    St#st{observes = Obs1}),
            St2 = cancel_pending_for(Key, St1),
            maybe_detach(St2)
    end.

%% The kernel delivered a message to an attached subscription (ADR-031 §D2:
%% the listener is the notify pid). Turn it into Observe notifications.
dispatch_notifications(#broker_message{} = Msg,
                       #st{observes = Obs, cfg = Cfg} = St) ->
    lists:foldl(
      fun({Key, Rel}, Acc) ->
              case maps:get(dest, Rel) =:= Msg#broker_message.destination of
                  true -> notify_one(Key, Rel, Msg, Cfg, Acc);
                  false -> Acc
              end
      end, St, maps:to_list(Obs)).

%% Send one notification. The relationship's `req_type' decides CON (reliable:
%% retransmit until ACKed, tracked in `pending') vs NON (at-most-once: fire once).
%%
%% `pending' is keyed by `{IP,Port,MessageId}': a conformant empty ACK/RST carries
%% no token, only the echoed Message ID, so that is the only key an incoming ACK
%% can match. Each entry ALSO stores the relationship key `{IP,Port,Token}`, so an
%% ACK/RST (and teardown) can find the relationship it belongs to. NSTART=1
%% (RFC 7641 §4.5.1): a new CON notification for a relationship replaces any still
%% outstanding one instead of queueing behind it, which keeps `pending' bounded by
%% the number of relationships and keeps each relationship to a single budget.
notify_one({IP, Port, Token} = ObsKey, Rel, Msg, Cfg,
           #st{pending = Pend, observes = Obs} = St) ->
    Type = maps:get(req_type, Rel, con),
    Now = now_ms(),
    {Seq, Rel1} = coap_observe:next_seq(Rel, Now),
    Note = coap_observe:notify_message(Type, Seq, Token, Msg),
    Id = next_id(),
    Bytes = coap_codec:encode(Note#{id => Id}),
    _ = send_to(IP, Port, Bytes, St),
    case Type of
        con ->
            Timeout = maps:get(observe_ack_timeout_ms, Cfg, 2000),
            Left = maps:get(observe_max_retrans, Cfg, 3),
            IdKey = {IP, Port, Id},
            %% Drop any still-outstanding notification for THIS relationship first
            %% (NSTART=1), so one slow ACK cannot stack up notifications.
            Pend1 = cancel_pending_map(ObsKey, Pend),
            Ref = arm_retransmit(IdKey, Timeout),
            Rel2 = Rel1#{req_id => Id},
            St#st{observes = maps:put(ObsKey, Rel2, Obs),
                  pending = maps:put(IdKey, {ObsKey, Bytes, Left, Ref}, Pend1)};
        non ->
            %% At-most-once: no retransmit state at all (RFC 7641 §4.5).
            St#st{observes = maps:put(ObsKey, Rel1#{req_id => Id}, Obs)}
    end.

%% Arm the retransmit timer for one pending notification. The timer message carries
%% its `pending' key so a firing timer acts on exactly that entry, never on the
%% whole table (one relationship's timer must not spend another's budget).
arm_retransmit(IdKey, Timeout) ->
    erlang:start_timer(Timeout, self(), {observe_ack, IdKey}).

%% Drop (and cancel the timers of) every pending notification belonging to one
%% relationship. Scoped by the full `{IP,Port,Token}' key, so a teardown on one
%% endpoint never cancels another endpoint's notification that happens to share a
%% token (the empty token is the common case: most clients use none).
cancel_pending_map(ObsKey, Pend) ->
    maps:fold(
      fun(_IdKey, {ObsKey0, _Bytes, _Left, Ref}, Acc) when ObsKey0 =:= ObsKey ->
              _ = erlang:cancel_timer(Ref),
              Acc;
         (IdKey, Val, Acc) ->
              maps:put(IdKey, Val, Acc)
      end, #{}, Pend).

cancel_pending_for(ObsKey, #st{pending = Pend} = St) ->
    St#st{pending = cancel_pending_map(ObsKey, Pend)}.

%% The 1 s refill tick also drops relationships silent longer than their own
%% Max-Age window. Side-effecting (it unsubscribes), so it cannot live in the
%% pure prune/1.
sweep_observes(#st{observes = Obs, cfg = Cfg} = St) ->
    Cap = maps:get(observe_max_age_s, Cfg, 30),
    Now = now_ms(),
    lists:foldl(
      fun({Key, Rel}, Acc) ->
              case coap_observe:expired(Rel, Now, Cap) of
                  true ->
                      Obs1 = maps:remove(Key, Acc#st.observes),
                      St1 = maybe_unsubscribe(maps:get(dest, Rel),
                                              Acc#st{observes = Obs1}),
                      St2 = cancel_pending_for(Key, St1),
                      maybe_detach(St2);
                  false ->
                      Acc
              end
      end, St, maps:to_list(Obs)).

%% A CON notification went un-acked. Act on EXACTLY the entry whose timer fired
%% (matched by key AND timer ref, so a stale timer for a superseded notification
%% is ignored): resend it once, counting the retransmit down; at zero give up on
%% the relationship.
retransmit_one(IdKey, Ref, #st{pending = Pend} = St) ->
    case maps:find(IdKey, Pend) of
        {ok, {ObsKey, Bytes, Left, Ref}} ->
            IP = element(1, IdKey),
            Port = element(2, IdKey),
            _ = send_to(IP, Port, Bytes, St),
            Pend1 = maps:remove(IdKey, Pend),
            case Left - 1 of
                0 ->
                    give_up_relationship(ObsKey, St#st{pending = Pend1});
                Left1 ->
                    Timeout = maps:get(observe_ack_timeout_ms, St#st.cfg, 2000),
                    Ref1 = arm_retransmit(IdKey, Timeout),
                    St#st{pending = maps:put(IdKey, {ObsKey, Bytes, Left1, Ref1},
                                             Pend1)}
            end;
        _ ->
            %% Already ACKed, superseded, or torn down: a stale timer, ignore it.
            St
    end.

%% Give up on one relationship after its retransmit budget is spent: cancel its
%% pending notification, unsubscribe its filter (unless another live relationship
%% still needs it), and detach the shared session once the table is empty.
give_up_relationship(ObsKey, #st{observes = Obs} = St) ->
    St1 = cancel_pending_for(ObsKey, St),
    case maps:take(ObsKey, Obs) of
        {Rel, Obs1} ->
            St2 = maybe_unsubscribe(maps:get(dest, Rel), St1#st{observes = Obs1}),
            maybe_detach(St2);
        error ->
            %% Already gone (e.g. deregistered by an RST in the meantime).
            St1
    end.

%% Unsubscribe a destination's shared filter, but only when no OTHER live
%% relationship on that destination still needs it. Every CoAP observation shares
%% one exclusive `(<<"coap">>, Filter)' subscription, so unsubscribing on one
%% relationship's teardown would silently starve the others (two clients observing
%% the same destination is the common case). Returns the state unchanged (aside
%% from the unsubscribe side effect) when another relationship remains.
maybe_unsubscribe(Dest, #st{observes = Obs} = St) ->
    StillObserved =
        lists:any(fun({_K, Rel}) -> maps:get(dest, Rel) =:= Dest end,
                  maps:to_list(Obs)),
    case StillObserved of
        true ->
            St;
        false ->
            _ = try broker_subscription:unsubscribe(?COAP_CLIENT, Dest) catch _:_ -> ok end,
            St
    end.

%% Detach the shared `<<"coap">>' session only when the relationship table is
%% empty: `detach/2' would otherwise tear down every remaining observation.
maybe_detach(#st{observes = Obs} = St) ->
    case maps:size(Obs) of
        0 ->
            _ = try broker_subscription:detach(?COAP_CLIENT, self()) catch _:_ -> ok end,
            St;
        _ ->
            St
    end.

%% Monotonic ms. The clock is read here (the listener is the only process that
%% needs it) and passed into coap_observe, which is pure.
now_ms() -> erlang:monotonic_time(millisecond).

next_id() -> erlang:unique_integer([positive]) rem 65536.

%%%_* Bounded tables -------------------------------------------------------

%% Cache the reply for a CON. The table is capped by `max_dedup_entries' and
%% swept by `prune/1' after `dedup_window_ms' (EXCHANGE_LIFETIME), so the cache
%% is a window, not a growing log.
remember(IP, Port, Id, Reply, Cfg, #st{dedup = Dedup0} = St) ->
    Cap = maps:get(max_dedup_entries, Cfg, 4096),
    Now = erlang:monotonic_time(millisecond),
    Dedup1 = maps:put({IP, Port, Id}, {Reply, Now}, Dedup0),
    Dedup2 = cap(Dedup1, Cap),
    St#st{dedup = Dedup2}.

cap(Map, Cap) when map_size(Map) =< Cap -> Map;
cap(Map, _Cap) ->
    evict(map_size(Map) div 3, Map).

evict(0, Map) -> Map;
evict(N, Map) ->
    [K | _] = maps:keys(Map),
    evict(N - 1, maps:remove(K, Map)).

%% Called on the one-second tick: forget expired CON replies, abandoned Block1
%% streams, and idle endpoints (a full bucket is indistinguishable from the
%% default a new endpoint gets, so dropping it loses nothing).
prune(#st{dedup = Dedup, endpoints = Eps, block1 = Streams, cfg = Cfg} = St) ->
    Window = maps:get(dedup_window_ms, Cfg, 247000),
    Max = maps:get(max_requests_per_second, Cfg, 100),
    Cutoff = erlang:monotonic_time(millisecond) - Window,
    Dedup1 = maps:filter(fun(_K, {_Reply, At}) -> At >= Cutoff end, Dedup),
    Block1 = maps:filter(fun(_K, {_Acc, _Total, At}) -> At >= Cutoff end, Streams),
    Eps1 = maps:filter(fun(_K, #ep{tokens = T, refill = At}) ->
                               not (T >= Max andalso At < Cutoff)
                       end, Eps),
    St#st{dedup = Dedup1, endpoints = Eps1, block1 = Block1}.
