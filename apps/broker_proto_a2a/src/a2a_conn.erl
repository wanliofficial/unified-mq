%% @doc Per-connection loop: read one request, answer it, repeat while the
%% client keeps the connection alive.
%%
%% Every failure mode is translated here into either a bare HTTP status or a
%% closed connection. An exception in the dispatcher is caught per request so a
%% bug in one method cannot turn into a silently dead socket (the client would
%% otherwise see a timeout instead of a 500).
-module(a2a_conn).

-export([start/3]).

%% A single connection may serve many requests, but not forever: a client that
%% keeps one socket open for an hour must not hold a ranch slot indefinitely.
-define(MAX_REQUESTS, 1000).

-spec start(ranch:ref(), module(), map()) -> ok.
%% S12-B (ADR-032 D7): count the connection for the whole time it owns the
%% socket. This is a plain loop rather than a gen_server, so `try ... after'
%% is what makes the decrement unconditional -- an exception in the dispatcher
%% must not leak a row in `connections_current'. The boolean records whether the
%% gauge was actually written; `connection_closed/1' only subtracts when it was.
start(Ref, Transport, _Opts) ->
    case ranch:handshake(Ref) of
        {ok, Socket} ->
            %% S12-B (ADR-032 D7): the node-level gate, asked after the ranch
            %% handshake (so the socket is ours) and before the first request is
            %% read. A refusal is a real HTTP response, which is what separates
            %% this from ranch's own silent parking at its per-listener cap.
            case broker_admission:try_acquire() of
                {rejected, Why} ->
                    logger:warning("a2a: connection refused by admission gate: ~p",
                                   [Why]),
                    _ = Transport:send(Socket, busy_response()),
                    _ = Transport:close(Socket),
                    ok;
                Lease ->
                    Counted = broker_metrics:connection_opened(),
                    try
                        loop(Socket, Transport, 0)
                    after
                        broker_metrics:connection_closed(Counted),
                        ok = broker_admission:release(Lease)
                    end
            end;
        {continue, _} ->
            %% `ranch_tcp' and `ranch_ssl' never return `{continue, _}' (only the
            %% PROXY-protocol handshake does), but the type allows it. Cancel
            %% rather than park the connection waiting for a continuation this
            %% front end will never send.
            _ = ranch:handshake_cancel(Ref),
            ok
    end.

%% S12-B: 503 is what an HTTP client expects from a server that is up but not
%% taking work; `Retry-After' keeps it from spinning. Built through the shared
%% response writer so the framing (connection, length) is the same as every other
%% response this front end emits.
busy_response() ->
    agent_rpc_http:response(
      503,
      [{<<"content-type">>, <<"application/json">>},
       {<<"retry-after">>, <<"5">>}],
      <<"{\"error\":\"server_busy\"}">>,
      #{keep_alive => false}).

loop(Socket, Transport, Count) when Count < ?MAX_REQUESTS ->
    Limits = #{body_bytes => a2a_rpc:max_body_bytes(),
               request_timeout_ms => a2a_rpc:request_timeout_ms(),
               idle_timeout_ms => a2a_rpc:idle_timeout_ms()},
    case agent_rpc_http:read_request(Transport, Socket, Limits) of
        {ok, Req} ->
            case safe_handle(Req) of
                {stream, Plan} ->
                    %% SSE owns the socket until the task ends: a stream is a
                    %% long-lived response, so keep-alive ends here.
                    _ = a2a_stream:run(Transport, Socket, Plan),
                    ok;
                {Status, Headers, Body, KeepAlive} ->
                    _ = Transport:send(
                          Socket,
                          agent_rpc_http:response(Status, Headers, Body,
                                                  #{keep_alive => KeepAlive})),
                    case KeepAlive of
                        true -> loop(Socket, Transport, Count + 1);
                        false -> ok
                    end
            end;
        {error, closed} ->
            ok;
        {error, timeout} ->
            ok;
        {error, Reason} ->
            send_error(Transport, Socket, Reason)
    end;
loop(_Socket, _Transport, _Count) ->
    ok.

safe_handle(Req) ->
    try
        a2a_rpc:handle(Req)
    catch
        Class:Reason:Stack ->
            logger:warning("a2a handler crashed ~p:~p ~p", [Class, Reason, Stack]),
            {500, [{<<"content-type">>, <<"application/json">>}],
             <<"{\"error\":\"internal_error\"}">>, false}
    end.

send_error(Transport, Socket, Reason) ->
    Status = case Reason of
                 payload_too_large -> 413;
                 not_http -> 400;
                 invalid_content_length -> 400;
                 protocol_error -> 400;
                 invalid_chunk -> 400;
                 _ -> 400
             end,
    _ = Transport:send(Socket, agent_rpc_http:response(Status, [], <<>>)),
    ok.
