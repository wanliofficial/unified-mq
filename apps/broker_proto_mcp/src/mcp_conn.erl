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
start(Ref, Transport, _Opts) ->
    case ranch:handshake(Ref) of
        {ok, Socket} ->
            loop(Socket, Transport, 0);
        {continue, _} ->
            _ = ranch:handshake_cancel(Ref),
            ok
    end.

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
