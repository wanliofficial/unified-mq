%% @doc S9-D: the stdio front end (ADR-027 §D8; `basic/transports/stdio').
%%
%% The transport is driven **in-process**: `mcp_stdio:run/2' takes the read and
%% write primitives as arguments, so the suite feeds newline-delimited JSON and
%% collects the frames the server writes to "stdout". That is exactly what the
%% real release does with `io:get_line/1' and `io:put_chars/1', without needing
%% a subprocess, a pipe or a pseudo-terminal.
%%
%% Groups:
%%   * `protocol'     -- framing, the envelope, per-request metadata, dispatch,
%%                       the ACL refusal and EOF shutdown;
%%   * `subscription' -- `subscriptions/listen' over the shared channel and
%%                       `notifications/cancelled'.
-module(mcp_stdio_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("broker_core/include/broker_message.hrl").
-include_lib("broker_router/include/broker_router.hrl").

-compile([export_all, nowarn_export_all]).

-define(PARTITIONS, 4).
-define(VERSION, <<"2026-07-28">>).

all() ->
    [{group, protocol}, {group, subscription}].

groups() ->
    [{protocol, [sequence],
      [discover_is_local,
       unknown_method_is_32601,
       malformed_json_is_32700,
       notification_produces_no_output,
       missing_version_is_32602,
       tools_call_roundtrip,
       denied_call_is_32603,
       eof_shuts_down]},
     {subscription, [sequence],
      [listen_acknowledges_and_forwards,
       cancel_stops_the_subscription]}].

%%%_* Fixtures --------------------------------------------------------------

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(broker_security),
    {ok, _} = application:ensure_all_started(broker_proto_agent_rpc),
    DataDir = filename:join(?config(priv_dir, Config), "mcp-stdio-data"),
    ok = filelib:ensure_dir(filename:join(DataDir, "placeholder")),
    _ = application:load(broker_storage),
    _ = application:load(broker_router),
    _ = application:load(broker_queue),
    _ = application:load(broker_meta),
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok = application:set_env(broker_storage, data_dir, DataDir),
    ok = application:set_env(broker_storage, wal, #{require_durable_fs => false}),
    ok = application:set_env(broker_router, partitions, ?PARTITIONS),
    ok = application:set_env(broker_queue, partitions, ?PARTITIONS),
    ok = application:set_env(broker_meta, table_storage, ram_copies),
    ok = application:set_env(broker_meta, nodes, []),
    broker_meta:reset(),
    {ok, _} = application:ensure_all_started(broker_meta),
    {ok, _} = application:ensure_all_started(broker_storage),
    {ok, _} = application:ensure_all_started(broker_router),
    [ok = broker_queue:create_partition(P) || P <- broker_queue:partition_ids()],
    {ok, _} = application:ensure_all_started(broker_queue),
    %% `mcp_discover:server_info/0' reads the app's vsn, so it must be loaded
    %% even though the stdio front end never starts the application.
    _ = application:load(broker_proto_mcp),
    OldSec = [{K, application:get_env(broker_security, K)} || K <- [acl]],
    ok = application:set_env(broker_security, acl, allow),
    [{data_dir, DataDir}, {old_security, OldSec} | Config].

end_per_suite(Config) ->
    _ = application:stop(broker_queue),
    _ = application:stop(broker_router),
    _ = application:stop(broker_storage),
    _ = application:stop(broker_meta),
    _ = application:stop(mnesia),
    _ = mnesia:delete_schema([node()]),
    lists:foreach(fun restore_security/1, ?config(old_security, Config)),
    ok.


init_per_group(_Group, Config) ->
    reset_env(),
    Config.

end_per_group(_Group, _Config) ->
    reset_env(),
    ok.

init_per_testcase(_Case, Config) ->
    reset_env(),
    Config.

end_per_testcase(_Case, _Config) ->
    _ = case erase(mcp_stdio) of
            undefined -> ok;
            Pid -> exit(Pid, kill)
        end,
    _ = case erase(mcp_backend) of
            undefined -> ok;
            Backend -> exit(Backend, kill)
        end,
    reset_env(),
    ok.

reset_env() ->
    Defaults = [{features, []},
                {request_timeout_ms, 30000},
                {sse_heartbeat_ms, 60},
                {sse_max_duration_ms, 20000},
                {sse_max_event_bytes, 1048576},
                {discover_ttl_ms, 3600000},
                {instructions, undefined},
                {request_reliability, fsync}],
    [ok = application:set_env(broker_proto_mcp, K, V) || {K, V} <- Defaults],
    ok = application:set_env(broker_security, acl, allow),
    ok.

%%%_* Protocol --------------------------------------------------------------

discover_is_local(_Config) ->
    Server = start_stdio(),
    send(Server, request(<<"server/discover">>, #{}, 1)),
    #{<<"id">> := 1, <<"result">> := Result} = next_out(),
    ?assertEqual(<<"complete">>, maps:get(<<"resultType">>, Result)),
    ?assertEqual([?VERSION], maps:get(<<"supportedVersions">>, Result)),
    %% Answered locally: nothing reached the kernel.
    ?assertEqual([], kernel_messages(<<"$mcp/server/discover">>)).

unknown_method_is_32601(_Config) ->
    ok = application:set_env(broker_proto_mcp, features, [tools]),
    Server = start_stdio(),
    send(Server, request(<<"tools/nope">>, #{}, 2)),
    #{<<"id">> := 2, <<"error">> := Error} = next_out(),
    ?assertEqual(-32601, maps:get(<<"code">>, Error)).

malformed_json_is_32700(_Config) ->
    Server = start_stdio(),
    Server ! {line, <<"{not json\n">>},
    #{<<"id">> := null, <<"error">> := Error} = next_out(),
    ?assertEqual(-32700, maps:get(<<"code">>, Error)).

notification_produces_no_output(_Config) ->
    Server = start_stdio(),
    send(Server, #{<<"jsonrpc">> => <<"2.0">>,
                   <<"method">> => <<"notifications/cancelled">>,
                   <<"params">> => #{<<"requestId">> => 1}}),
    no_out(300).

%% No headers exist on stdio, so a missing version reaches the protocol layer
%% and is an invalid-params error rather than a header mismatch.
missing_version_is_32602(_Config) ->
    Server = start_stdio(),
    Params = #{<<"_meta">> =>
                   #{<<"io.modelcontextprotocol/clientCapabilities">> => #{}}},
    send(Server, request(<<"server/discover">>, Params, 3)),
    #{<<"id">> := 3, <<"error">> := Error} = next_out(),
    ?assertEqual(-32602, maps:get(<<"code">>, Error)),
    ?assertEqual(<<"missing _meta.io.modelcontextprotocol/protocolVersion">>,
                 maps:get(<<"reason">>, maps:get(<<"data">>, Error))).

tools_call_roundtrip(_Config) ->
    ok = application:set_env(broker_proto_mcp, features, [tools]),
    _Backend = start_backend(<<"tools/call">>,
                             fun(_Request) ->
                                     [{<<"rpcResult">>,
                                       #{<<"result">> =>
                                             #{<<"resultType">> => <<"complete">>,
                                               <<"content">> =>
                                                   [#{<<"type">> => <<"text">>,
                                                      <<"text">> => <<"ok">>}]}}}]
                             end),
    Server = start_stdio(),
    send(Server, request(<<"tools/call">>,
                         #{<<"name">> => <<"echo">>, <<"arguments">> => #{}}, 4)),
    #{<<"id">> := 4, <<"result">> := Result} = next_out(),
    ?assertEqual([#{<<"type">> => <<"text">>, <<"text">> => <<"ok">>}],
                 maps:get(<<"content">>, Result)).

%% An ACL refusal is a JSON-RPC error on stdio: there is no status code to
%% spend, so the reason has to be visible in the message (ADR-027 §D9).
denied_call_is_32603(_Config) ->
    ok = application:set_env(broker_proto_mcp, features, [tools]),
    ok = application:set_env(broker_security, acl,
                             [#{effect => deny, principal => <<"*">>,
                                action => publish,
                                pattern => <<"$mcp/tools/call">>}]),
    Server = start_stdio(),
    send(Server, request(<<"tools/call">>,
                         #{<<"name">> => <<"echo">>, <<"arguments">> => #{}}, 5)),
    #{<<"id">> := 5, <<"error">> := Error} = next_out(),
    ?assertEqual(-32603, maps:get(<<"code">>, Error)),
    Data = maps:get(<<"data">>, Error),
    ?assertEqual(<<"denied by policy">>, maps:get(<<"reason">>, Data)),
    ?assertEqual(<<"$mcp/tools/call">>, maps:get(<<"resource">>, Data)).

%% EOF is the graceful shutdown signal: the loop returns, no frame is written.
eof_shuts_down(_Config) ->
    Server = start_stdio(),
    Ref = erlang:monitor(process, Server),
    send(Server, request(<<"server/discover">>, #{}, 6)),
    #{<<"id">> := 6} = next_out(),
    Server ! eof,
    receive
        {'DOWN', Ref, process, Server, normal} -> ok
    after 2000 ->
            ct:fail(stdio_did_not_exit)
    end,
    no_out(200).

%%%_* Subscriptions ---------------------------------------------------------

listen_acknowledges_and_forwards(_Config) ->
    Server = start_stdio(),
    send(Server, request(<<"subscriptions/listen">>,
                         #{<<"notifications">> => #{<<"toolsListChanged">> => true}},
                         1)),
    Ack = next_out(),
    ?assertEqual(<<"notifications/subscriptions/acknowledged">>,
                 maps:get(<<"method">>, Ack)),
    ?assertEqual(1, subscription_id(Ack)),
    ok = publish(<<"$mcp/notify/tools/list_changed">>, #{}, <<"t0">>),
    Note = next_out(),
    ?assertEqual(<<"notifications/tools/list_changed">>,
                 maps:get(<<"method">>, Note)),
    ?assertEqual(1, subscription_id(Note)).

cancel_stops_the_subscription(_Config) ->
    Server = start_stdio(),
    send(Server, request(<<"subscriptions/listen">>,
                         #{<<"notifications">> => #{<<"toolsListChanged">> => true}},
                         1)),
    Ack = next_out(),
    ?assertEqual(<<"notifications/subscriptions/acknowledged">>,
                 maps:get(<<"method">>, Ack)),
    ok = publish(<<"$mcp/notify/tools/list_changed">>, #{}, <<"t0">>),
    ?assertEqual(<<"notifications/tools/list_changed">>,
                 maps:get(<<"method">>, next_out())),
    %% Cancellation arrives as a notification naming the listen request id.
    send(Server, #{<<"jsonrpc">> => <<"2.0">>,
                   <<"method">> => <<"notifications/cancelled">>,
                   <<"params">> => #{<<"requestId">> => 1}}),
    drain_output(200),
    ok = publish(<<"$mcp/notify/tools/list_changed">>, #{}, <<"t0">>),
    no_out(400).

%%%_* stdio harness ---------------------------------------------------------

start_stdio() ->
    Parent = self(),
    Read = fun() ->
                   receive
                       {line, Line} -> Line;
                       eof -> eof
                   end
           end,
    Write = fun(Iodata) ->
                    Parent ! {out, iolist_to_binary(Iodata)},
                    ok
            end,
    %% Not linked: `end_per_testcase' stops the server with `exit/2', and a link
    %% would deliver that kill to the test case process itself.
    Pid = spawn(fun() -> mcp_stdio:run(Read, Write) end),
    put(mcp_stdio, Pid),
    Pid.

send(Server, Request) ->
    Server ! {line, [json:encode(Request), <<"\n">>]},
    ok.

next_out() ->
    receive
        {out, Bin} -> json:decode(Bin)
    after 5000 ->
            ct:fail(no_output)
    end.

no_out(Timeout) ->
    receive
        {out, Bin} -> ct:fail({unexpected_output, Bin})
    after Timeout ->
            ok
    end.

drain_output(Timeout) ->
    receive
        {out, _Bin} -> drain_output(100)
    after Timeout ->
            ok
    end.

request(Method, Params0, Id) ->
    Params = case maps:is_key(<<"_meta">>, Params0) of
                 true -> Params0;
                 false -> Params0#{<<"_meta">> => meta()}
             end,
    #{<<"jsonrpc">> => <<"2.0">>, <<"method">> => Method,
      <<"params">> => Params, <<"id">> => Id}.

meta() ->
    #{<<"io.modelcontextprotocol/protocolVersion">> => ?VERSION,
      <<"io.modelcontextprotocol/clientInfo">> =>
          #{<<"name">> => <<"mcp-stdio-suite">>, <<"version">> => <<"1.0.0">>},
      <<"io.modelcontextprotocol/clientCapabilities">> => #{}}.

subscription_id(#{<<"params">> := #{<<"_meta">> := Meta}}) ->
    maps:get(<<"io.modelcontextprotocol/subscriptionId">>, Meta, undefined);
subscription_id(_Frame) ->
    undefined.

%%%_* Fake backend ---------------------------------------------------------

%% Polls every partition for `$mcp/<Method>' requests and answers each at the
%% gateway's `replyTo'. The handler returns `{Kind, Payload}' frames, exactly
%% the contract ADR-027 §D2 gives a backend.
start_backend(Method, Handler) ->
    Parent = self(),
    Pid = spawn(fun() ->
                        Cursors = maps:from_list(
                                    [{P, tail(P)} || P <- broker_queue:partition_ids()]),
                        Parent ! {backend_ready, self()},
                        backend_loop(Method, Handler, Cursors)
                end),
    receive
        {backend_ready, Pid} ->
            put(mcp_backend, Pid),
            Pid
    after 2000 ->
            ct:fail(backend_not_ready)
    end.

backend_loop(Method, Handler, Cursors) ->
    Cursors2 =
        maps:fold(
          fun(Partition, From, Acc) ->
                  case broker_queue:read_from(Partition, From, 1000) of
                      {ok, [], Next} ->
                          Acc#{Partition => Next};
                      {ok, Page, Next} ->
                          [handle_request(Message, Method, Handler)
                           || {_, Message} <- Page],
                          Acc#{Partition => Next};
                      {error, _} ->
                          Acc#{Partition => From}
                  end
          end, Cursors, Cursors),
    timer:sleep(20),
    backend_loop(Method, Handler, Cursors2).

handle_request(#broker_message{destination = Destination, payload = Payload},
               Method, Handler) ->
    case Destination of
        <<"$mcp/", Method/binary>> ->
            Request = json:decode(iolist_to_binary(Payload)),
            ReplyTo = reply_to(Request),
            lists:foreach(fun({Kind, Frame}) -> reply(ReplyTo, Kind, Frame) end,
                          Handler(Request));
        _ ->
            ok
    end.

reply_to(#{<<"params">> := #{<<"_meta">> := Meta}}) ->
    #{<<"unified-mq">> := #{<<"replyTo">> := ReplyTo,
                            <<"tenantId">> := Tenant}} = Meta,
    {ReplyTo, Tenant}.

reply({ReplyTo, Tenant}, Kind, Payload) ->
    Meta = #{<<"unified-mq">> => #{<<"kind">> => Kind}},
    publish(ReplyTo, Payload#{<<"_meta">> => Meta}, Tenant).

publish(Destination, Payload, Tenant) ->
    {ok, Env} = broker_router:env(),
    Ctx = #route_ctx{tenant_id = Tenant,
                     publisher = <<"mcp-stdio-backend">>,
                     reliability = fsync},
    {ok, [{Partition, _} | _]} = broker_router_hash:route(Destination, Ctx, Env),
    Msg = #broker_message{message_id = integer_to_binary(
                                         erlang:unique_integer([positive])),
                          tenant_id = Tenant,
                          destination = Destination,
                          partition_id = Partition,
                          payload = iolist_to_binary(json:encode(Payload)),
                          payload_encoding = json,
                          headers = #{},
                          proto_meta = #{},
                          persistence = durable,
                          reliability = fsync,
                          created_at = erlang:system_time(millisecond),
                          expires_at = undefined,
                          event = undefined},
    {ok, _} = broker_queue:append(Partition, Msg, fsync),
    ok.

tail(Partition) ->
    case broker_queue:watermarks(Partition) of
        {ok, _Committed, High} -> High;
        {error, _} -> 0
    end.

kernel_messages(Destination) ->
    [Map || {_P, #{destination := D} = Map} <- all_messages(), D =:= Destination].

all_messages() ->
    lists:append([partition_messages(P) || P <- broker_queue:partition_ids()]).

partition_messages(Partition) ->
    partition_messages(Partition, 0, []).

partition_messages(Partition, From, Acc) ->
    case broker_queue:read_from(Partition, From, 1000) of
        {ok, [], _Next} -> Acc;
        {ok, Page, Next} ->
            Acc1 = Acc ++ [{Partition, message_map(M)} || {_, M} <- Page],
            case Next > From of
                true -> partition_messages(Partition, Next, Acc1);
                false -> Acc1
            end;
        {error, _} -> Acc
    end.

message_map(#broker_message{} = M) ->
    #{destination => M#broker_message.destination,
      headers => M#broker_message.headers,
      payload => M#broker_message.payload}.

restore_security({K, {ok, V}}) -> application:set_env(broker_security, K, V);
restore_security({K, undefined}) -> application:unset_env(broker_security, K).
