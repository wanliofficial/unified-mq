%% @doc Per-connection loop: read one POST, answer it, repeat while the client
%% keeps the connection alive.
%%
%% Three response shapes come back from `mcp_rpc:handle/1':
%%   * `{json, Status, Headers, Body, Keep}' -- a single JSON response;
%%   * `{no_content, Keep}'                  -- 202 for an accepted notification;
%%   * `{stream, Plan}'                      -- an SSE response owns the socket.
%%
%% An exception in the dispatcher is caught per request so a bug in one method
%% cannot turn into a silently dead socket.
-module(mcp_conn).

-export([start/3]).

%% A client may keep one connection for many requests, but not forever: an idle
%% socket must not hold a ranch slot indefinitely.
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
                    logger:warning("mcp: connection refused by admission gate: ~p",
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
            _ = ranch:handshake_cancel(Ref),
            ok
    end.

%% S12-B: 503 with `Retry-After', built through the shared response writer.
busy_response() ->
    agent_rpc_http:response(
      503,
      [{<<"content-type">>, <<"application/json">>},
       {<<"retry-after">>, <<"5">>}],
      <<"{\"error\":\"server_busy\"}">>,
      #{keep_alive => false}).

loop(Socket, Transport, Count) when Count < ?MAX_REQUESTS ->
    Limits = #{body_bytes => mcp_rpc:max_body_bytes(),
               request_timeout_ms => mcp_rpc:request_timeout_ms()},
    case agent_rpc_http:read_request(Transport, Socket, Limits) of
        {ok, Req} ->
            case safe_handle(Req) of
                {stream, Plan} ->
                    %% SSE owns the socket until the request or subscription
                    %% ends: keep-alive ends here.
                    _ = mcp_stream:run(Transport, Socket, Plan),
                    ok;
                {no_content, Keep} ->
                    _ = Transport:send(Socket,
                                       agent_rpc_http:response(
                                         202, [], <<>>, #{keep_alive => Keep})),
                    next(Socket, Transport, Count, Keep);
                {json, Status, Headers, Body, Keep} ->
                    _ = Transport:send(Socket,
                                       agent_rpc_http:response(
                                         Status, Headers, Body,
                                         #{keep_alive => Keep})),
                    next(Socket, Transport, Count, Keep)
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

next(Socket, Transport, Count, true) ->
    loop(Socket, Transport, Count + 1);
next(_Socket, _Transport, _Count, false) ->
    ok.

safe_handle(Req) ->
    try
        mcp_rpc:handle(Req)
    catch
        Class:Reason:Stack ->
            logger:warning("mcp handler crashed ~p:~p ~p", [Class, Reason, Stack]),
            {json, 500, [{<<"content-type">>, <<"application/json">>}],
             <<"{\"error\":\"internal_error\"}">>, false}
    end.

send_error(Transport, Socket, Reason) ->
    Status = case Reason of
                 payload_too_large -> 413;
                 _ -> 400
             end,
    _ = Transport:send(Socket, agent_rpc_http:response(Status, [], <<>>)),
    ok.
