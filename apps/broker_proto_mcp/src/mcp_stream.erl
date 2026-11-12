%% @doc MCP streams: pass-through calls and `subscriptions/listen' (ADR-027
%% §D2/§D5/§D9).
%%
%% Two shapes share one window on the log:
%%
%%   * `call' -- one pass-through primitive call. The request has already been
%%     published to `$mcp/<method>'; this process follows the unique reply
%%     destination, forwards `notifications/progress' and
%%     `notifications/<name>' frames while the backend works, and terminates on
%%     the first `rpcResult' frame (or on the request timeout, with `-32603').
%%   * `listen' -- `subscriptions/listen'. It acknowledges first (spec: the
%%     acknowledgment MUST precede every notification), then follows the
%%     partitions behind the agreed notification destinations until the client
%%     disconnects, the stream cap elapses (graceful closure) or the socket
%%     breaks.
%%
%% Both are **transient readers** (承 ADR-025 §D7): `broker_queue:subscribe/2'
%% plus `read_from/3', no SubRef, no cursor, no retention pin. A slow or dead
%% client only costs its own connection.
%%
%% The same loops serve both transports through a small sink: an SSE stream
%% (HTTP, `run/3') and a newline-delimited stdout writer (stdio,
%% `run_stdio/2'). Framing is the only difference; the protocol logic is shared,
%% which is exactly the property that keeps the two front ends from drifting.
-module(mcp_stream).

-include_lib("broker_core/include/broker_message.hrl").

-export([run/3, run_stdio/2, max_event_bytes/0, heartbeat_ms/0, max_duration_ms/0]).

-define(PAGE, 200).
-define(SUBSCRIPTION_ID, <<"io.modelcontextprotocol/subscriptionId">>).
-define(UNIFIED_MQ, <<"unified-mq">>).

-type sink() :: {sse, module(), term()} | {stdio, fun((iodata()) -> ok | {error, term()})}.
-export_type([sink/0]).

%% @doc Hand an HTTP socket to an SSE response and stream until it ends.
-spec run(module(), term(), map()) -> ok.
run(Transport, Socket, Plan) ->
    run_sink({sse, Transport, Socket}, Plan).

%% @doc Run the same plan over a newline-delimited writer (stdio).
-spec run_stdio(sink(), map()) -> ok.
run_stdio(Sink, Plan) ->
    run_sink(Sink, Plan).

run_sink(Sink, #{kind := call} = Plan) ->
    run_call(Sink, Plan);
run_sink(Sink, #{kind := listen} = Plan) ->
    run_listen(Sink, Plan).

%%%_* Pass-through call -----------------------------------------------------

run_call(Sink, Plan) ->
    Partition = maps:get(partition, Plan),
    case broker_queue:subscribe(Partition, self()) of
        ok ->
            try
                sink_open(Sink),
                Deadline = now_ms() + maps:get(timeout_ms, Plan,
                                                request_timeout_ms()),
                call_loop(Sink, Plan, maps:get(from, Plan, 0), Deadline)
            after
                _ = broker_queue:unsubscribe(Partition, self())
            end;
        {error, Reason} ->
            logger:warning("mcp stream cannot follow partition ~p: ~0p",
                           [Partition, Reason]),
            sink_open(Sink),
            _ = emit(Sink, error_frame(maps:get(id, Plan), internal_error, undefined)),
            sink_close(Sink)
    end.

call_loop(Sink, Plan, From, Deadline) ->
    case drain(Sink, Plan, From, fun reply_frame/2) of
        {final, _Next} ->
            sink_close(Sink);
        {continue, Offset} ->
            %% `Offset' is the cursor, not a new wake-up token: the already
            %% bound variable is why the match below is `_'.
            case wait_next(Sink, Deadline) of
                {ok, _} ->
                    call_loop(Sink, Plan, Offset, Deadline);
                timeout ->
                    %% The backend never answered: a local error, reported with
                    %% the JSON-RPC standard code so it cannot be confused with
                    %% a protocol-defined one (`basic/index#error-codes').
                    _ = emit(Sink, error_frame(maps:get(id, Plan), internal_error,
                                               #{<<"reason">> => <<"timeout">>})),
                    sink_close(Sink);
                closed ->
                    ok
            end;
        closed ->
            ok;
        error ->
            sink_close(Sink)
    end.

%% Turn one reply message into a frame. The backend says what the message means
%% through `_meta.unified-mq.kind' (ADR-027 §D2); the default is a final result.
%% `{final, Frame}' terminates the stream, `{continue, Frame}' does not, and
%% `skip' drops a message this request does not own.
reply_frame(#broker_message{destination = Destination, payload = Payload},
            #{reply_to := ReplyTo} = Plan) when Destination =:= ReplyTo ->
    case agent_rpc_json:decode(iolist_to_binary(Payload)) of
        {ok, Object} when is_map(Object) ->
            case kind(Object) of
                <<"progress">> ->
                    case progress_frame(Object, Plan) of
                        skip -> skip;
                        Frame -> {continue, Frame}
                    end;
                <<"notification">> ->
                    {continue, passthrough_notification(Object, Plan)};
                _ ->
                    {final, result_frame(Object, Plan)}
            end;
        _ ->
            %% Undecodable reply: skip it rather than kill a stream the client
            %% can still use for a later frame.
            skip
    end;
reply_frame(_Message, _Plan) ->
    skip.

kind(Object) ->
    case maps:get(<<"_meta">>, Object, #{}) of
        #{?UNIFIED_MQ := #{<<"kind">> := Kind}} when is_binary(Kind) -> Kind;
        _ -> <<"rpcResult">>
    end.

%% `{"result": ...}' or `{"error": {...}}' from the backend, with the client's
%% own request id stamped on top: the backend never has to know it.
result_frame(Object, #{id := Id}) ->
    case maps:find(<<"error">>, Object) of
        {ok, Error} when is_map(Error) ->
            #{<<"jsonrpc">> => <<"2.0">>, <<"id">> => Id, <<"error">> => Error};
        _ ->
            Result = maps:get(<<"result">>, Object, Object),
            agent_rpc_json:response_ok(Id, with_server_info(Result))
    end.

%% Per-response protocol field (`basic/index#meta'): servers SHOULD identify
%% themselves on every result. Only a map-shaped result can carry it, and a
%% backend-provided value wins.
with_server_info(Result) when is_map(Result) ->
    Meta = case maps:get(<<"_meta">>, Result, #{}) of
               M when is_map(M) -> M;
               _ -> #{}
           end,
    Result#{<<"_meta">> =>
                maps:merge(#{<<"io.modelcontextprotocol/serverInfo">> =>
                                 mcp_discover:server_info()},
                           Meta)};
with_server_info(Result) ->
    Result.

%% A backend progress report is `{"progress":N,"total":M,"message":"..."}'; the
%% gateway knows the token because the client put it in the request `_meta'.
%% Without a token the client did not opt in, so the report is dropped.
progress_frame(_Object, #{progress_token := undefined}) ->
    skip;
progress_frame(Object, #{progress_token := Token} = Plan) ->
    Params0 = maps:with([<<"progress">>, <<"total">>, <<"message">>], Object),
    Params = Params0#{<<"progressToken">> => Token},
    notification(<<"notifications/progress">>, Params, maps:get(subscription_id, Plan, undefined)).

%% A backend-published notification names its own method.
passthrough_notification(Object, Plan) ->
    Method = case maps:get(<<"method">>, Object, undefined) of
                 M when is_binary(M) -> M;
                 _ -> <<"notifications/message">>
             end,
    Params = case maps:get(<<"params">>, Object, #{}) of
                 P when is_map(P) -> P;
                 _ -> #{}
             end,
    notification(Method, Params, maps:get(subscription_id, Plan, undefined)).

%%%_* subscriptions/listen --------------------------------------------------

run_listen(Sink, Plan) ->
    Id = maps:get(id, Plan),
    Entries = maps:get(entries, Plan, []),
    %% Anchor each cursor **before** installing the hook: an append that lands
    %% in between is then still read (a slightly early notification is
    %% harmless), while nothing published before the subscription is replayed.
    Planned = [{Entry, Partition, tail(Partition)}
               || {Entry, Partition} <- Entries],
    Subscriptions = subscribe_all(Planned),
    try
        sink_open(Sink),
        Acked = [Entry || {Entry, _Partition, _Tail} <- Subscriptions],
        _ = emit(Sink, acknowledged_frame(Id, mcp_sub:acknowledged(Acked))),
        listen_loop(Sink, Id, Subscriptions, cursors(Subscriptions),
                    deadline(Sink))
    after
        unsubscribe_all(Subscriptions)
    end.

listen_loop(Sink, Id, Subscriptions, Cursors, Deadline) ->
    case drain_all(Sink, Id, Subscriptions, Cursors) of
        {ok, Cursors2} ->
            case expired(Deadline) of
                true ->
                    close_listen(Sink, Id);
                false ->
                    case wait_next(Sink, Deadline) of
                        {ok, _} ->
                            listen_loop(Sink, Id, Subscriptions, Cursors2, Deadline);
                        %% The heartbeat tick crossed the deadline: the stream
                        %% cap, not an error (`patterns/subscriptions#graceful-closure').
                        timeout ->
                            close_listen(Sink, Id);
                        closed ->
                            ok
                    end
            end;
        closed ->
            ok
    end.

%% Graceful closure: answer the long-lived request before closing the channel.
close_listen(Sink, Id) ->
    _ = emit(Sink, closure_frame(Id)),
    sink_close(Sink).

subscribe_all(Entries) ->
    lists:filtermap(
      fun({_Entry, Partition, _Tail} = Planned) ->
              case broker_queue:subscribe(Partition, self()) of
                  ok ->
                      {true, Planned};
                  {error, Reason} ->
                      logger:warning("mcp subscription cannot follow ~p: ~0p",
                                     [Partition, Reason]),
                      false
              end
      end, Entries).

unsubscribe_all(Subscriptions) ->
    lists:foreach(fun({_Entry, Partition, _Tail}) ->
                          _ = broker_queue:unsubscribe(Partition, self())
                  end, Subscriptions).

cursors(Subscriptions) ->
    lists:foldl(fun({_Entry, Partition, Tail}, Acc) ->
                        case maps:is_key(Partition, Acc) of
                            true -> Acc;
                            false -> Acc#{Partition => Tail}
                        end
                end, #{}, Subscriptions).

%% Read every partition once, forwarding the notifications the client asked for.
drain_all(Sink, Id, Subscriptions, Cursors) ->
    maps:fold(
      fun(Partition, From, {ok, Acc}) ->
              case drain_partition(Sink, Id, Subscriptions, Partition, From) of
                  {ok, Next} -> {ok, Acc#{Partition => Next}};
                  closed -> closed
              end;
         (_Partition, _From, closed) ->
              closed
      end, {ok, Cursors}, Cursors).

drain_partition(Sink, Id, Subscriptions, Partition, From) ->
    case broker_queue:read_from(Partition, From, ?PAGE) of
        {ok, [], Next} ->
            {ok, Next};
        {ok, Page, Next} ->
            case forward_page(Sink, Id, Subscriptions, Page) of
                closed ->
                    closed;
                ok ->
                    case Next > From of
                        true -> drain_partition(Sink, Id, Subscriptions,
                                                Partition, Next);
                        false -> {ok, Next}
                    end
            end;
        {error, Reason} ->
            logger:warning("mcp subscription read failed on ~p: ~0p",
                           [Partition, Reason]),
            {ok, From}
    end.

forward_page(_Sink, _Id, _Subscriptions, []) ->
    ok;
forward_page(Sink, Id, Subscriptions, [{_Offset, Message} | Rest]) ->
    case notification_frame(Message, Subscriptions, Id) of
        skip ->
            forward_page(Sink, Id, Subscriptions, Rest);
        Frame ->
            case emit(Sink, Frame) of
                ok -> forward_page(Sink, Id, Subscriptions, Rest);
                closed -> closed
            end
    end.

%% A partition carries traffic for many destinations; a notification is ours
%% only when its destination is one the client subscribed to.
notification_frame(#broker_message{destination = Destination, payload = Payload},
                   Subscriptions, Id) ->
    case [Entry || {Entry, _Partition, _Tail} <- Subscriptions,
                   maps:get(dest, Entry) =:= Destination] of
        [] ->
            skip;
        [Entry | _] ->
            notification(maps:get(method, Entry), decode_params(Payload, Entry, Id),
                         Id)
    end;
notification_frame(_Message, _Subscriptions, _Id) ->
    skip.

decode_params(Payload, Entry, Id) ->
    Decoded = case agent_rpc_json:decode(iolist_to_binary(Payload)) of
                  {ok, Map} when is_map(Map) -> Map;
                  _ -> #{}
              end,
    WithUri = case maps:get(uri, Entry, undefined) of
                  undefined -> Decoded;
                  Uri -> maps:merge(Decoded, #{<<"uri">> => Uri})
              end,
    Meta = case maps:get(<<"_meta">>, WithUri, #{}) of
               M when is_map(M) -> M;
               _ -> #{}
           end,
    WithUri#{<<"_meta">> => Meta#{?SUBSCRIPTION_ID => Id}}.

%%%_* Frames ----------------------------------------------------------------

%% A notification frame always carries the subscription id when one exists, so
%% the same builder serves the request stream and the listen stream.
notification(Method, Params, undefined) ->
    #{<<"jsonrpc">> => <<"2.0">>, <<"method">> => Method,
      <<"params">> => Params};
notification(Method, Params, SubscriptionId) ->
    Meta = case maps:get(<<"_meta">>, Params, #{}) of
               M when is_map(M) -> M;
               _ -> #{}
           end,
    #{<<"jsonrpc">> => <<"2.0">>, <<"method">> => Method,
      <<"params">> => Params#{<<"_meta">> => Meta#{?SUBSCRIPTION_ID => SubscriptionId}}}.

acknowledged_frame(Id, Acknowledged) ->
    #{<<"jsonrpc">> => <<"2.0">>,
      <<"method">> => <<"notifications/subscriptions/acknowledged">>,
      <<"params">> => #{<<"_meta">> => #{?SUBSCRIPTION_ID => Id},
                        <<"notifications">> => Acknowledged}}.

closure_frame(Id) ->
    #{<<"jsonrpc">> => <<"2.0">>, <<"id">> => Id,
      <<"result">> => #{<<"resultType">> => <<"complete">>,
                        <<"_meta">> => #{?SUBSCRIPTION_ID => Id}}}.

error_frame(Id, Reason, Data) ->
    agent_rpc_json:response_error(Id, Reason, Data).

%%%_* Framing loop ----------------------------------------------------------

%% Send every stored message for `Plan' from `From'. `FrameFun' maps a message
%% to `{final, Frame}' / `{continue, Frame}' / `skip'.
drain(Sink, Plan, From, FrameFun) ->
    Partition = maps:get(partition, Plan),
    case broker_queue:read_from(Partition, From, ?PAGE) of
        {ok, [], Next} ->
            {continue, Next};
        {ok, Page, Next} ->
            case forward_all(Sink, Plan, Page, FrameFun) of
                {final, _} ->
                    {final, Next};
                continue ->
                    case Next > From of
                        true -> drain(Sink, Plan, Next, FrameFun);
                        false -> {continue, Next}
                    end;
                closed ->
                    closed
            end;
        {error, _Reason} ->
            error
    end.

forward_all(_Sink, _Plan, [], _FrameFun) ->
    continue;
forward_all(Sink, Plan, [{_Offset, Message} | Rest], FrameFun) ->
    case FrameFun(Message, Plan) of
        skip ->
            forward_all(Sink, Plan, Rest, FrameFun);
        {final, Frame} ->
            case emit(Sink, Frame) of
                ok -> {final, Frame};
                closed -> closed
            end;
        {continue, Frame} ->
            case emit(Sink, Frame) of
                ok -> forward_all(Sink, Plan, Rest, FrameFun);
                closed -> closed
            end
    end.

%% One frame bounded by the configured cap: a frame larger than the advertised
%% limit is a contract break, so the stream fails instead of sending it
%% (ADR-025 §D8's posture, reused).
emit(Sink, Frame) ->
    Payload = agent_rpc_json:encode(Frame),
    Size = iolist_size(Payload),
    case Size > max_event_bytes() of
        true ->
            logger:warning("mcp frame exceeds sse_max_event_bytes (~p)", [Size]),
            Oversized = agent_rpc_json:encode(
                          error_frame(null, internal_error,
                                      #{<<"reason">> => <<"frame too large">>})),
            _ = sink_send(Sink, Oversized),
            closed;
        false ->
            case sink_send(Sink, Payload) of
                ok -> ok;
                {error, _} -> closed
            end
    end.

%%%_* Sinks ----------------------------------------------------------------

sink_open({sse, Transport, Socket}) ->
    _ = Transport:send(Socket,
                       agent_rpc_http:response(
                         200, agent_rpc_sse:headers(), <<>>,
                         #{keep_alive => true, chunked => true})),
    ok;
sink_open({stdio, _Write}) ->
    ok.

sink_send({sse, Transport, Socket}, Payload) ->
    Transport:send(Socket, agent_rpc_http:chunk(agent_rpc_sse:event(Payload)));
sink_send({stdio, Write}, Payload) ->
    try
        case Write([Payload, <<"\n">>]) of
            ok -> ok;
            {error, _} = Error -> Error;
            _ -> ok
        end
    catch
        _:_ -> {error, closed}
    end.

sink_close({sse, Transport, Socket}) ->
    _ = Transport:send(Socket, agent_rpc_http:last_chunk()),
    ok;
sink_close({stdio, _Write}) ->
    ok.

sink_heartbeat({sse, Transport, Socket}) ->
    case Transport:send(Socket, agent_rpc_http:chunk(agent_rpc_sse:heartbeat())) of
        ok -> ok;
        {error, _} -> closed
    end;
sink_heartbeat({stdio, _Write}) ->
    %% No intermediary to keep awake on a pipe.
    ok.

%%%_* Deadlines ------------------------------------------------------------

%% HTTP streams are capped (`sse_max_duration_ms'); a stdio subscription lives
%% until the client cancels it or the process exits.
deadline({sse, _Transport, _Socket}) -> now_ms() + max_duration_ms();
deadline({stdio, _Write}) -> infinity.

expired(infinity) -> false;
expired(Deadline) -> now_ms() >= Deadline.

%% Wait for the next append, emitting a heartbeat comment on each idle tick.
%% Reaching the deadline is reported to the caller, which decides between
%% `-32603' (call) and graceful closure (listen).
wait_next(_Sink, infinity) ->
    receive
        {new_messages, _Partition, _From, _To} -> {ok, undefined};
        mcp_cancel -> closed
    end;
wait_next(Sink, Deadline) ->
    case Deadline - now_ms() of
        Remaining when Remaining =< 0 ->
            timeout;
        Remaining ->
            receive
                {new_messages, _Partition, _From, _To} ->
                    {ok, undefined};
                mcp_cancel ->
                    closed
            after min(Remaining, heartbeat_ms()) ->
                    case expired(Deadline) of
                        true ->
                            timeout;
                        false ->
                            case sink_heartbeat(Sink) of
                                ok -> wait_next(Sink, Deadline);
                                closed -> closed
                            end
                    end
            end
    end.

%%%_* Config ---------------------------------------------------------------

-spec max_event_bytes() -> pos_integer().
max_event_bytes() ->
    application:get_env(broker_proto_mcp, sse_max_event_bytes, 1048576).

-spec heartbeat_ms() -> pos_integer().
heartbeat_ms() ->
    application:get_env(broker_proto_mcp, sse_heartbeat_ms, 15000).

-spec max_duration_ms() -> pos_integer().
max_duration_ms() ->
    application:get_env(broker_proto_mcp, sse_max_duration_ms, 600000).

request_timeout_ms() ->
    application:get_env(broker_proto_mcp, request_timeout_ms, 30000).

tail(Partition) ->
    case broker_queue:watermarks(Partition) of
        {ok, _Committed, High} -> High;
        {error, _Reason} -> 0
    end.

now_ms() ->
    erlang:monotonic_time(millisecond).
