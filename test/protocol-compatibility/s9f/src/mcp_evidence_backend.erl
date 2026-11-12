%% Evidence-only fake MCP backend: reads $mcp/<method> requests from every
%% partition and answers on the reply destination the gateway injected.
-module(mcp_evidence_backend).
-export([start/0, stop/0]).

-include_lib("broker_core/include/broker_message.hrl").
-include_lib("broker_router/include/broker_router.hrl").

start() ->
    N = application:get_env(broker_router, partitions, 4),
    spawn(fun() -> init(N) end),
    ok.

stop() ->
    case whereis(mcp_evidence_backend) of
        undefined -> ok;
        Pid -> exit(Pid, kill), ok
    end.

init(N) ->
    register(mcp_evidence_backend, self()),
    Partitions = lists:seq(0, N - 1),
    lists:foreach(fun(P) -> _ = broker_queue:subscribe(P, self()) end, Partitions),
    logger:notice("mcp evidence backend up on ~p partitions", [N]),
    loop(maps:from_list([{P, tail(P)} || P <- Partitions])).

tail(P) ->
    case broker_queue:watermarks(P) of
        {ok, _C, High} -> High;
        _ -> 0
    end.

loop(Cursors) ->
    receive
        {new_messages, _P, _F, _T} -> loop(scan(Cursors));
        scan_now -> loop(scan(Cursors))
    after 200 ->
        loop(scan(Cursors))
    end.

scan(Cursors) ->
    maps:map(fun(P, From) -> drain(P, From) end, Cursors).

drain(P, From) ->
    case broker_queue:read_from(P, From, 200) of
        {ok, [], Next} -> Next;
        {ok, Page, Next} ->
            lists:foreach(fun handle/1, Page),
            case Next > From of
                true -> drain(P, Next);
                false -> Next
            end;
        {error, _} -> From
    end.

handle({_Offset, #broker_message{destination = Dest, payload = Payload,
                                 tenant_id = Tenant}}) ->
    case Dest of
        <<"$mcp/reply/", _/binary>> -> ok;
        <<"$mcp/", _/binary>> -> answer(Payload, Tenant, Dest);
        _ -> ok
    end.

answer(Payload, Tenant, Dest) ->
    case agent_rpc_json:decode(iolist_to_binary(Payload)) of
        {ok, #{<<"method">> := Method} = Obj} ->
            Params = maps:get(<<"params">>, Obj, #{}),
            Meta = maps:get(<<"_meta">>, Params, #{}),
            UM = maps:get(<<"unified-mq">>, Meta, #{}),
            case maps:get(<<"replyTo">>, UM, undefined) of
                undefined -> ok;
                ReplyTo ->
                    progress(Meta, ReplyTo, Tenant, Method, Dest),
                    maybe_notify(Method, Params),
                    publish(ReplyTo, Tenant, result_for(Method, Params))
            end;
        _ ->
            ok
    end.

%% Shape the result per method, published to the gateway's replyTo so the SDK
%% client's call resolves instead of hitting the request timeout. Tool calls
%% echo; list/read methods return their declared collections.
result_for(<<"tools/call">>, _Params) ->
    #{<<"result">> =>
          #{<<"resultType">> => <<"complete">>,
            <<"cacheScope">> => <<"public">>,
            <<"ttlMs">> => 3600000,
            <<"content">> =>
                [#{<<"type">> => <<"text">>,
                   <<"text">> => <<"backend echo: tools/call">>}],
            <<"isError">> => false}};
result_for(<<"tools/list">>, _Params) ->
    #{<<"result">> =>
          #{<<"resultType">> => <<"complete">>,
            <<"cacheScope">> => <<"public">>,
            <<"ttlMs">> => 3600000,
            <<"tools">> =>
                [#{<<"name">> => <<"emit-list-change">>,
                   <<"description">> =>
                       <<"Emit a tools list-changed notification">>,
                   <<"inputSchema">> =>
                       #{<<"type">> => <<"object">>,
                         <<"properties">> =>
                             #{<<"topic">> =>
                                 #{<<"type">> => <<"string">>}}}}]}};
result_for(<<"resources/list">>, _Params) ->
    #{<<"result">> =>
          #{<<"resultType">> => <<"complete">>,
            <<"cacheScope">> => <<"public">>,
            <<"ttlMs">> => 3600000,
            <<"resources">> => []}};
result_for(<<"resources/templates/list">>, _Params) ->
    #{<<"result">> =>
          #{<<"resultType">> => <<"complete">>,
            <<"cacheScope">> => <<"public">>,
            <<"ttlMs">> => 3600000,
            <<"resourceTemplates">> => []}};
result_for(<<"resources/read">>, Params) ->
    #{<<"result">> =>
          #{<<"resultType">> => <<"complete">>,
            <<"cacheScope">> => <<"public">>,
            <<"ttlMs">> => 3600000,
            <<"contents">> =>
                [#{<<"uri">> => maps:get(<<"uri">>, Params, <<>>),
                   <<"text">> => <<"backend resource body">>,
                   <<"mimeType">> => <<"text/plain">>}]}};
result_for(<<"prompts/list">>, _Params) ->
    #{<<"result">> =>
          #{<<"resultType">> => <<"complete">>,
            <<"cacheScope">> => <<"public">>,
            <<"ttlMs">> => 3600000,
            <<"prompts">> => []}};
result_for(<<"prompts/get">>, _Params) ->
    #{<<"result">> =>
          #{<<"resultType">> => <<"complete">>,
            <<"cacheScope">> => <<"public">>,
            <<"ttlMs">> => 3600000,
            <<"messages">> =>
                [#{<<"role">> => <<"user">>,
                   <<"content">> =>
                       #{<<"type">> => <<"text">>,
                         <<"text">> => <<"backend prompt message">>}}]}};
result_for(<<"completion/complete">>, _Params) ->
    #{<<"result">> =>
          #{<<"resultType">> => <<"complete">>,
            <<"cacheScope">> => <<"public">>,
            <<"ttlMs">> => 3600000,
            <<"completion">> =>
                #{<<"values">> => [], <<"total">> => 0,
                  <<"hasMore">> => false}}};
result_for(_Method, _Params) ->
    #{<<"result">> =>
          #{<<"resultType">> => <<"complete">>,
            <<"cacheScope">> => <<"public">>,
            <<"ttlMs">> => 3600000,
            <<"content">> => []}}.

progress(Meta, ReplyTo, Tenant, Method, Dest) ->
    case maps:get(<<"progressToken">>, Meta, undefined) of
        undefined -> ok;
        Token ->
            publish(ReplyTo, Tenant,
                    #{<<"progress">> => 1, <<"total">> => 2,
                      <<"message">> => <<"backend working on ", Method/binary>>,
                      <<"_meta">> =>
                          #{<<"unified-mq">> => #{<<"kind">> => <<"progress">>},
                            <<"progressToken">> => Token}}),
            logger:notice("mcp evidence progress for ~s (~s)", [Method, Dest])
    end.

%% One call name doubles as the notification trigger so the listen evidence
%% does not need a second control channel.
maybe_notify(<<"tools/call">>, Params) ->
    case maps:get(<<"name">>, Params, undefined) of
        <<"emit-list-change">> ->
            %% A notification destination's payload *is* the notification
            %% params object (ADR-027 §D5); the gateway names the method.
            publish(<<"$mcp/notify/tools/list_changed">>, <<"t1">>,
                    #{<<"reason">> => <<"tools changed">>});
        _ ->
            ok
    end;
maybe_notify(_Method, _Params) ->
    ok.

publish(Dest, Tenant, Object) ->
    case broker_router:env() of
        {ok, Env} ->
            Ctx = #route_ctx{tenant_id = Tenant, publisher = <<"evidence-backend">>,
                             reliability = fsync},
            case broker_router_hash:route(Dest, Ctx, Env) of
                {ok, [{Partition, _} | _]} ->
                    Body = iolist_to_binary(agent_rpc_json:encode(Object)),
                    Msg = #broker_message{
                             message_id = integer_to_binary(erlang:unique_integer([positive])),
                             tenant_id = Tenant,
                             destination = Dest,
                             partition_id = Partition,
                             payload = Body,
                             payload_encoding = json,
                             headers = #{},
                             proto_meta = #{},
                             persistence = durable,
                             reliability = fsync,
                             created_at = erlang:system_time(millisecond),
                             expires_at = undefined,
                             event = undefined},
                    broker_queue:append(Partition, Msg, fsync);
                _ -> {error, no_partition}
            end;
        {error, _} = E -> E
    end.
