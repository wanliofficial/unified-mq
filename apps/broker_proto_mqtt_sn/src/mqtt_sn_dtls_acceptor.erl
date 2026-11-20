%% @doc The DTLS accepting half of an MQTT-SN listener (SN-01, 2026-11-17).
%%
%% A structural clone of `coap_dtls_acceptor' (S11-C): one acceptor per DTLS
%% listener, handshaking exactly one peer at a time. The serialization is
%% forced by how the PSK identity is observable -- OTP surfaces it only through
%% the `user_lookup_fun' callback running synchronously inside
%% `ssl:handshake/2', posting to the process that owns the callback state.
%% Concurrent handshakes could not tell which connection an identity belongs
%% to. `ssl:connection_information/1' does not expose `psk_identity'.
%%
%% The consequence is a bounded handshake rate (1 / handshake time), the same
%% documented trade the CoAP listener made.
%%
%% Handshake outcome is reported to the listener as
%% `{dtls_handshake_ok, Acceptor, Socket, Peer, Identity}'; admission and the
%% `controlling_process' transfer follow the same `{dtls_admit, ...}' /
%% `{dtls_owned, Socket}' / `{dtls_abandoned, Socket}' protocol as CoAP.
-module(mqtt_sn_dtls_acceptor).

-export([start_link/2]).

-spec start_link(pid(), map()) -> {ok, pid()}.
start_link(ListenerPid, Opts) when is_pid(ListenerPid), is_map(Opts) ->
    Pid = spawn_link(fun() -> await_listen_socket(ListenerPid, Opts) end),
    {ok, Pid}.

await_listen_socket(ListenerPid, Opts) ->
    receive
        {listen_socket, Listen} ->
            loop(ListenerPid, Listen, Opts)
    after 5000 ->
            ok
    end.

loop(ListenerPid, Listen, Opts) ->
    Timeout = maps:get(dtls_transport_accept_timeout_ms, Opts, 1000),
    try ssl:transport_accept(Listen, Timeout) of
        {ok, Socket0} ->
            handshake(ListenerPid, Socket0, Opts),
            loop(ListenerPid, Listen, Opts);
        {error, timeout} ->
            loop(ListenerPid, Listen, Opts);
        {error, closed} ->
            ok;
        {error, Reason} ->
            logger:warning("mqtt_sn dtls transport_accept failed: ~p", [Reason]),
            loop(ListenerPid, Listen, Opts)
    catch
        Class:Reason ->
            logger:warning("mqtt_sn dtls transport_accept crashed ~p:~p",
                           [Class, Reason]),
            loop(ListenerPid, Listen, Opts)
    end.

%% The identity drain runs on BOTH outcomes: a rejected peer's identity
%% message arrives in this mailbox just like an accepted one's, and an
%% undrained rejection would hand the next connection the wrong principal.
handshake(ListenerPid, Socket0, Opts) ->
    Timeout = maps:get(dtls_handshake_timeout_ms, Opts, 2000),
    _ = flush_identities(),
    try ssl:handshake(Socket0, Timeout) of
        {ok, Socket} ->
            try ssl:peername(Socket) of
                {ok, Peer} ->
                    Identity = flush_identities(),
                    offer(ListenerPid, Socket, Peer, Identity);
                _Other ->
                    _ = flush_identities(),
                    _ = ssl:close(Socket)
            catch
                _:_ ->
                    _ = flush_identities(),
                    _ = ssl:close(Socket)
            end;
        {error, Reason} ->
            _ = flush_identities(),
            logger:debug("mqtt_sn dtls handshake refused: ~p", [Reason]),
            ok
    catch
        Class:Reason ->
            _ = flush_identities(),
            logger:debug("mqtt_sn dtls handshake crashed ~p:~p", [Class, Reason]),
            ok
    end.

offer(ListenerPid, Socket, Peer, Identity) ->
    ListenerPid ! {dtls_handshake_ok, self(), Socket, Peer, Identity},
    receive
        {dtls_admit, Socket, ok} ->
            case ssl:controlling_process(Socket, ListenerPid) of
                ok ->
                    ListenerPid ! {dtls_owned, Socket},
                    ok;
                {error, Reason} ->
                    logger:warning("mqtt_sn dtls controlling_process failed: ~p",
                                   [Reason]),
                    ListenerPid ! {dtls_abandoned, Socket},
                    _ = ssl:close(Socket)
            end;
        {dtls_admit, Socket, refuse} ->
            _ = ssl:close(Socket)
    after 5000 ->
            ListenerPid ! {dtls_abandoned, Socket},
            _ = ssl:close(Socket)
    end.

flush_identities() ->
    receive
        {psk_identity, Identity} -> flush_identities(Identity)
    after 0 ->
            undefined
    end.

flush_identities(Last) ->
    receive
        {psk_identity, Identity} -> flush_identities(Identity)
    after 0 ->
            Last
    end.
