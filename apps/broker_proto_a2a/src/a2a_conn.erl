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
start(Ref, Transport, _Opts) ->
    case ranch:handshake(Ref) of
        {ok, Socket} ->
            loop(Socket, Transport, 0);
        {continue, _} ->
            %% `ranch_tcp' and `ranch_ssl' never return `{continue, _}' (only the
            %% PROXY-protocol handshake does), but the type allows it. Cancel
            %% rather than park the connection waiting for a continuation this
            %% front end will never send.
            _ = ranch:handshake_cancel(Ref),
            ok
    end.

loop(Socket, Transport, Count) when Count < ?MAX_REQUESTS ->
    Limits = #{body_bytes => a2a_rpc:max_body_bytes(),
               request_timeout_ms => a2a_rpc:request_timeout_ms()},
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
