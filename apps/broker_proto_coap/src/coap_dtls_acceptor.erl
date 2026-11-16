%% @doc The DTLS accepting half of a CoAP listener (S11-C).
%%
%% One of these runs per DTLS listener, and it handshakes exactly one peer at a
%% time. That serialization is not a shortcut, it is forced by how the PSK
%% identity is observable: OTP exposes the peer's identity only through the
%% `user_lookup_fun' callback, which runs synchronously inside
%% `ssl:handshake/2' and posts to the process that owns the callback state --
%% this one. With concurrent handshakes there is no way to tell which
%% connection an identity message belongs to (measured: a rejected identity
%% leaked into the next connection's principal). `ssl:connection_information/1'
%% does not expose `psk_identity', so there is no race-free alternative.
%%
%% The consequence is a bounded handshake rate (1 / handshake time), which is
%% documented in `docs/design/s11c-coap-dtls.md' §3.2 and measured in the S11-C
%% capacity report. The trade is deliberate: correctness of identity over
%% handshake parallelizm.
%%
%% It is a plain process, not a gen_server: it holds no state beyond the listen
%% socket and has no call/cast protocol. It exits when the listen socket does.
-module(coap_dtls_acceptor).

-export([start_link/2]).

%% @doc Start the acceptor. `Opts' carries the resolved config (`cfg') and the
%% identity of the listener to hand established sockets to. The caller must send
%% `{listen_socket, Socket}' once `ssl:listen/2' has returned, because the listen
%% options need this pid in their lookup state.
-spec start_link(pid(), map()) -> {ok, pid()}.
start_link(ListenerPid, Opts) when is_pid(ListenerPid), is_map(Opts) ->
    Pid = spawn_link(fun() -> await_listen_socket(ListenerPid, Opts) end),
    {ok, Pid}.

await_listen_socket(ListenerPid, Opts) ->
    receive
        {listen_socket, Listen} ->
            loop(ListenerPid, Listen, Opts)
    after 5000 ->
            %% The listener died between spawn and ssl:listen; nothing to accept.
            ok
    end.

loop(ListenerPid, Listen, Opts) ->
    Timeout = maps:get(dtls_transport_accept_timeout_ms, Opts, 1000),
    case catch ssl:transport_accept(Listen, Timeout) of
        {ok, Socket0} ->
            handshake(ListenerPid, Socket0, Opts),
            loop(ListenerPid, Listen, Opts);
        {error, timeout} ->
            %% No peer this tick: not an error, just an idle listener.
            loop(ListenerPid, Listen, Opts);
        {error, closed} ->
            %% The listener closed the listen socket: shut down quietly.
            ok;
        {error, Reason} ->
            logger:warning("coap dtls transport_accept failed: ~p", [Reason]),
            loop(ListenerPid, Listen, Opts);
        {'EXIT', Reason} ->
            logger:warning("coap dtls transport_accept crashed: ~p", [Reason]),
            loop(ListenerPid, Listen, Opts)
    end.

%% Handshake, capture the identity, ask the listener whether the session fits,
%% and only then transfer ownership.
%%
%% The drain runs on BOTH outcomes. A rejected peer's identity message arrives
%% in this mailbox just like an accepted one's; leaving it there would make the
%% NEXT connection inherit the wrong principal. That bug was measured, not
%% imagined.
handshake(ListenerPid, Socket0, Opts) ->
    Timeout = maps:get(dtls_handshake_timeout_ms, Opts, 2000),
    %% Nothing from an earlier round may be mistaken for this peer's identity.
    _ = flush_identities(),
    case catch ssl:handshake(Socket0, Timeout) of
        {ok, Socket} ->
            case catch ssl:peername(Socket) of
                {ok, Peer} ->
                    Identity = flush_identities(),
                    offer(ListenerPid, Socket, Peer, Identity);
                _Other ->
                    _ = flush_identities(),
                    _ = ssl:close(Socket)
            end;
        {error, Reason} ->
            _ = flush_identities(),
            logger:debug("coap dtls handshake refused: ~p", [Reason]),
            ok;
        {'EXIT', Reason} ->
            _ = flush_identities(),
            logger:debug("coap dtls handshake crashed: ~p", [Reason]),
            ok
    end.

%% Ask the listener to admit this session (it owns the caps and the tables),
%% then move the socket. The socket stays `{active, false}' throughout: setting
%% an active budget from the non-owner *returns ok and yet delivers datagrams to
%% whoever owns the socket* -- measured as silently dropped traffic. Only the
%% listener arms it, after the transfer.
offer(ListenerPid, Socket, Peer, Identity) ->
    ListenerPid ! {dtls_handshake_ok, self(), Socket, Peer, Identity},
    receive
        {dtls_admit, Socket, ok} ->
            case ssl:controlling_process(Socket, ListenerPid) of
                ok ->
                    %% Only now, with the listener as owner, may the active
                    %% budget be armed -- and only the listener may do it.
                    ListenerPid ! {dtls_owned, Socket},
                    ok;
                {error, Reason} ->
                    logger:warning("coap dtls controlling_process failed: ~p",
                                   [Reason]),
                    %% The listener already recorded this session when it
                    %% answered `ok' -- it counted the slot before the transfer,
                    %% because the caps live there. Without this notice the
                    %% record would outlive the socket for ever (only
                    %% `{ssl_closed,_}' would clean it, and that socket was
                    %% never handed over). So release it explicitly.
                    ListenerPid ! {dtls_abandoned, Socket},
                    _ = ssl:close(Socket)
            end;
        {dtls_admit, Socket, refuse} ->
            _ = ssl:close(Socket)
    after 5000 ->
            %% The listener is gone or wedged; do not leak the socket. It may
            %% also have recorded the session, so release that too.
            ListenerPid ! {dtls_abandoned, Socket},
            _ = ssl:close(Socket)
    end.

%% The lookup callback posts the presented identity here, once per handshake.
%% Draining unconditionally on every path (success, failure, crash) is what
%% keeps an identity from one connection out of the next one's principal: the
%% callback runs inside `ssl:handshake/2' in THIS process, and a rejected peer's
%% message arrives just like an accepted one's. Measured, not hypothetical --
%% an undrained rejection handed a bad client's name to the next good client.
%%
%% A handshake presents exactly one identity, so taking the last one is exactly
%% one: there is nothing to choose between.
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
