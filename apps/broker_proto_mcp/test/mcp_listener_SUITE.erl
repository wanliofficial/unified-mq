%% @doc S9-D: the MCP Streamable HTTP front end (ADR-027).
%%
%% Groups so a failure names its subject:
%%   * `discover' -- `server/discover' is answered locally and its capability
%%                   set follows `features'; the per-request metadata rules;
%%   * `headers'  -- the transport gate order (Origin, Accept, method,
%%                   Content-Type) and the three mirrored headers;
%%   * `call'     -- pass-through: a fake backend answers a published request,
%%                   progress is forwarded, timeout ends the stream;
%%   * `stream'   -- `subscriptions/listen': ack first, notifications, resource
%%                   updates, graceful closure.
-module(mcp_listener_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("broker_core/include/broker_message.hrl").
-include_lib("broker_router/include/broker_router.hrl").

-compile([export_all, nowarn_export_all]).

-define(TOKEN, <<"mcp-token">>).
-define(PARTITIONS, 4).
-define(VERSION, <<"2026-07-28">>).

all() ->
    [{group, discover}, {group, headers}, {group, call}, {group, stream}].

groups() ->
    [{discover, [sequence],
      [discover_is_local_and_empty_by_default,
       discover_capabilities_follow_features,
       discover_instructions_and_cache_hints,
       missing_protocol_version_meta_is_header_mismatch,
       unsupported_protocol_version_is_32022,
       missing_client_capabilities_is_32602]},
     {headers, [sequence],
      [unauthenticated_is_401,
       nont_post_is_405,
       wrong_path_is_404,
       illegal_origin_is_403,
       loopback_origin_is_allowed,
       accept_without_sse_is_406,
       content_type_not_json_is_415,
       missing_protocol_version_header_is_32020,
       method_header_mismatch_is_32020,
       name_header_is_required_and_checked,
       name_header_accepts_base64_sentinel,
       malformed_json_is_32700,
       notification_is_202_without_body]},
     {call, [sequence],
      [tools_call_roundtrip,
       tools_call_forwards_progress_then_result,
       tools_call_forwards_notification,
       tools_call_error_is_forwarded,
       tools_call_timeout_is_32603,
       tools_call_requires_name_header,
       unknown_method_is_404,
       feature_not_advertised_is_404,
       sampling_is_refused]},
     {stream, [sequence],
      [listen_acknowledges_first,
       listen_forwards_tools_list_changed,
       listen_forwards_resource_updated,
       listen_closes_gracefully_at_the_cap]}].

%%%_* Fixtures --------------------------------------------------------------

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(broker_security),
    {ok, _} = application:ensure_all_started(broker_proto_agent_rpc),
    DataDir = filename:join(?config(priv_dir, Config), "mcp-data"),
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
    %% The router index writer is what `broker_router_hash:current_env/0' needs;
    %% it is not a dependency of broker_storage, so it is started explicitly
    %% rather than inherited from whichever suite ran first.
    {ok, _} = application:ensure_all_started(broker_router),
    [ok = broker_queue:create_partition(P) || P <- broker_queue:partition_ids()],
    %% Start the queue supervisor so the partition registry exists for the fake
    %% backend; the front end would start it as a dependency anyway, but the
    %% backend starts before any listener.
    {ok, _} = application:ensure_all_started(broker_queue),
    _ = application:load(broker_proto_mcp),
    OldSec = [{K, application:get_env(broker_security, K)} || K <- [users, acl]],
    ok = application:set_env(broker_security, users,
                             [{<<"alice">>, #{token => ?TOKEN,
                                              tenant_id => <<"t1">>}}]),
    ok = application:set_env(broker_security, acl, allow),
    [{data_dir, DataDir}, {old_security, OldSec} | Config].

end_per_suite(Config) ->
    _ = application:stop(broker_proto_mcp),
    _ = application:stop(broker_queue),
    _ = application:stop(broker_router),
    _ = application:stop(broker_storage),
    _ = application:stop(broker_meta),
    _ = application:stop(mnesia),
    _ = mnesia:delete_schema([node()]),
    lists:foreach(fun restore_security/1, ?config(old_security, Config)),
    ok.

init_per_group(Group, Config) ->
    reset_env(),
    [{group, Group} | Config].

end_per_group(_Group, _Config) ->
    reset_env(),
    ok.

init_per_testcase(_Case, Config) ->
    reset_env(),
    Config.

end_per_testcase(_Case, _Config) ->
    %% A fake backend outliving its case would answer the next case's requests
    %% too, and whichever reply arrives first ends the stream -- which shows up
    %% as a mysteriously missing frame later on.
    _ = case erase(mcp_backend) of
            undefined -> ok;
            Pid -> exit(Pid, kill)
        end,
    reset_env(),
    ok.

reset_env() ->
    _ = application:stop(broker_proto_mcp),
    Defaults = [{listeners, []},
                {require_auth, true},
                {path, <<"/">>},
                {features, []},
                {discover_ttl_ms, 3600000},
                {instructions, undefined},
                {allowed_origins, []},
                {request_timeout_ms, 30000},
                {sse_heartbeat_ms, 60},
                {sse_max_duration_ms, 20000},
                {sse_max_event_bytes, 1048576},
                {stdio_enabled, false}],
    [ok = application:set_env(broker_proto_mcp, K, V) || {K, V} <- Defaults],
    ok.

%%%_* Discover --------------------------------------------------------------

discover_is_local_and_empty_by_default(_Config) ->
    {Port, _} = start_listener(),
    {200, _Headers, Body} = request(Port, headers(<<"server/discover">>, #{}),
                                    body(<<"server/discover">>, #{}, 1)),
    #{<<"result">> := Result} = decode(Body),
    ?assertEqual(<<"complete">>, maps:get(<<"resultType">>, Result)),
    ?assertEqual([?VERSION], maps:get(<<"supportedVersions">>, Result)),
    ?assertEqual(#{}, maps:get(<<"capabilities">>, Result)),
    ?assertMatch(#{<<"io.modelcontextprotocol/serverInfo">> :=
                       #{<<"name">> := <<"unified-mq">>}},
                 maps:get(<<"_meta">>, Result)),
    %% Nothing was published to the kernel: discovery is answered locally.
    ?assertEqual([], kernel_messages(<<"$mcp/server/discover">>)).

discover_capabilities_follow_features(_Config) ->
    ok = application:set_env(broker_proto_mcp, features, [tools, resources]),
    {Port, _} = start_listener(),
    {200, _, Body} = request(Port, headers(<<"server/discover">>, #{}),
                             body(<<"server/discover">>, #{}, 1)),
    #{<<"result">> := Result} = decode(Body),
    ?assertEqual(#{<<"tools">> => #{}, <<"resources">> => #{}},
                 maps:get(<<"capabilities">>, Result)).

discover_instructions_and_cache_hints(_Config) ->
    ok = application:set_env(broker_proto_mcp, instructions, <<"use tools">>),
    ok = application:set_env(broker_proto_mcp, discover_ttl_ms, 1234),
    {Port, _} = start_listener(),
    {200, _, Body} = request(Port, headers(<<"server/discover">>, #{}),
                             body(<<"server/discover">>, #{}, 1)),
    #{<<"result">> := Result} = decode(Body),
    ?assertEqual(<<"use tools">>, maps:get(<<"instructions">>, Result)),
    ?assertEqual(1234, maps:get(<<"ttlMs">>, Result)),
    ?assertEqual(<<"public">>, maps:get(<<"cacheScope">>, Result)).

%% A request whose `_meta' lost the protocol version cannot satisfy the header
%% -- body comparison, so it is a header mismatch, not a missing parameter.
missing_protocol_version_meta_is_header_mismatch(_Config) ->
    {Port, _} = start_listener(),
    Params = #{<<"_meta">> => #{<<"io.modelcontextprotocol/clientCapabilities">> => #{}}},
    {400, _, Body} = request(Port, headers(<<"server/discover">>, #{}),
                             body(<<"server/discover">>, Params, 1)),
    ?assertMatch(#{<<"error">> := #{<<"code">> := -32020}}, decode(Body)).

unsupported_protocol_version_is_32022(_Config) ->
    {Port, _} = start_listener(),
    Params = #{<<"_meta">> => #{<<"io.modelcontextprotocol/protocolVersion">> =>
                                    <<"1900-01-01">>,
                                <<"io.modelcontextprotocol/clientCapabilities">> =>
                                    #{}}},
    Headers = [{"MCP-Protocol-Version", "1900-01-01"}
               | drop_header("MCP-Protocol-Version",
                             headers(<<"server/discover">>, #{}))],
    {400, _, Body} = request(Port, Headers,
                             body(<<"server/discover">>, Params, 1)),
    #{<<"error">> := Error} = decode(Body),
    ?assertEqual(-32022, maps:get(<<"code">>, Error)),
    ?assertEqual([?VERSION], maps:get(<<"supported">>, maps:get(<<"data">>, Error))).

missing_client_capabilities_is_32602(_Config) ->
    {Port, _} = start_listener(),
    Params = #{<<"_meta">> => #{<<"io.modelcontextprotocol/protocolVersion">> =>
                                    ?VERSION}},
    {400, _, Body} = request(Port, headers(<<"server/discover">>, #{}),
                             body(<<"server/discover">>, Params, 1)),
    ?assertMatch(#{<<"error">> := #{<<"code">> := -32602}}, decode(Body)).

%%%_* Headers ---------------------------------------------------------------

unauthenticated_is_401(_Config) ->
    {Port, _} = start_listener(),
    Headers = drop_header("Authorization", headers(<<"server/discover">>, #{})),
    {401, _, <<>>} = request(Port, Headers,
                             body(<<"server/discover">>, #{}, 1)).

nont_post_is_405(_Config) ->
    {Port, _} = start_listener(),
    {405, Headers, _} = raw_request(Port, "GET", "/", headers(<<"server/discover">>, #{}),
                                    <<>>),
    ?assertEqual(<<"POST">>, header(<<"allow">>, Headers)),
    {405, _, _} = raw_request(Port, "DELETE", "/",
                              headers(<<"server/discover">>, #{}), <<>>).

wrong_path_is_404(_Config) ->
    ok = application:set_env(broker_proto_mcp, path, <<"/mcp">>),
    {Port, _} = start_listener(),
    {404, _, _} = raw_request(Port, "POST", "/", headers(<<"server/discover">>, #{}),
                              body(<<"server/discover">>, #{}, 1)),
    {200, _, _} = raw_request(Port, "POST", "/mcp",
                              headers(<<"server/discover">>, #{}),
                              body(<<"server/discover">>, #{}, 1)).

illegal_origin_is_403(_Config) ->
    {Port, _} = start_listener(),
    Headers = [{"Origin", "http://evil.example"} | headers(<<"server/discover">>, #{})],
    {403, _, <<>>} = request(Port, Headers, body(<<"server/discover">>, #{}, 1)).

loopback_origin_is_allowed(_Config) ->
    {Port, _} = start_listener(),
    Headers = [{"Origin", "http://127.0.0.1:5173"}
               | headers(<<"server/discover">>, #{})],
    {200, _, _} = request(Port, Headers, body(<<"server/discover">>, #{}, 1)),
    ok = application:set_env(broker_proto_mcp, allowed_origins, [<<"localhost">>]),
    Headers2 = [{"Origin", "http://localhost:5173"}
                | headers(<<"server/discover">>, #{})],
    {200, _, _} = request(Port, Headers2, body(<<"server/discover">>, #{}, 1)).

accept_without_sse_is_406(_Config) ->
    {Port, _} = start_listener(),
    Headers = [{"Accept", "application/json"}
               | drop_header("Accept", headers(<<"server/discover">>, #{}))],
    {406, _, _} = request(Port, Headers, body(<<"server/discover">>, #{}, 1)).

content_type_not_json_is_415(_Config) ->
    {Port, _} = start_listener(),
    Headers = [{"Content-Type", "text/plain"}
               | drop_header("Content-Type", headers(<<"server/discover">>, #{}))],
    {415, _, _} = request(Port, Headers, body(<<"server/discover">>, #{}, 1)).

missing_protocol_version_header_is_32020(_Config) ->
    {Port, _} = start_listener(),
    Headers = drop_header("MCP-Protocol-Version",
                          headers(<<"server/discover">>, #{})),
    {400, _, Body} = request(Port, Headers,
                             body(<<"server/discover">>, #{}, 1)),
    ?assertMatch(#{<<"error">> := #{<<"code">> := -32020}}, decode(Body)).

method_header_mismatch_is_32020(_Config) ->
    {Port, _} = start_listener(),
    Headers = [{"Mcp-Method", "tools/list"}
               | drop_header("Mcp-Method", headers(<<"server/discover">>, #{}))],
    {400, _, Body} = request(Port, Headers,
                             body(<<"server/discover">>, #{}, 1)),
    ?assertMatch(#{<<"error">> := #{<<"code">> := -32020}}, decode(Body)).

%% `Mcp-Name' is required for `tools/call' and must match `params.name'.
name_header_is_required_and_checked(_Config) ->
    ok = application:set_env(broker_proto_mcp, features, [tools]),
    {Port, _} = start_listener(),
    Params = tool_call(<<"echo">>, #{}),
    Missing = drop_header("Mcp-Name", headers(<<"tools/call">>, Params)),
    {400, _, Body1} = request(Port, Missing, body(<<"tools/call">>, Params, 1)),
    ?assertMatch(#{<<"error">> := #{<<"code">> := -32020}}, decode(Body1)),
    Wrong = [{"Mcp-Name", "other"}
             | drop_header("Mcp-Name", headers(<<"tools/call">>, Params))],
    {400, _, Body2} = request(Port, Wrong, body(<<"tools/call">>, Params, 2)),
    ?assertMatch(#{<<"error">> := #{<<"code">> := -32020}}, decode(Body2)).

%% A name that cannot be a plain ASCII header travels as `=?base64?...?='; the
%% server MUST decode it before comparing.
name_header_accepts_base64_sentinel(_Config) ->
    ok = application:set_env(broker_proto_mcp, features, [tools]),
    Backend = start_backend(<<"tools/call">>,
                            fun(_Request) ->
                                    [{<<"rpcResult">>,
                                      #{<<"result">> =>
                                            #{<<"resultType">> => <<"complete">>,
                                              <<"content">> => []}}}]
                            end),
    {Port, _} = start_listener(),
    Name = <<"工具"/utf8>>,
    Params = tool_call(Name, #{}),
    Encoded = base64:encode(Name),
    Headers = [{"Mcp-Name", "=?base64?" ++ binary_to_list(Encoded) ++ "?="}
               | drop_header("Mcp-Name", headers(<<"tools/call">>, Params))],
    Frames = collect_stream(Port, Headers, body(<<"tools/call">>, Params, 1), 5000),
    ?assertMatch([#{<<"result">> := _}], Frames),
    %% A backend left running would answer later cases too, which makes them
    %% nondeterministic -- a frame could be superseded by the stale one.
    stop_backend(Backend).

malformed_json_is_32700(_Config) ->
    {Port, _} = start_listener(),
    {400, _, Body} = request(Port, headers(<<"server/discover">>, #{}),
                             <<"{not json">>),
    ?assertMatch(#{<<"error">> := #{<<"code">> := -32700}}, decode(Body)).

notification_is_202_without_body(_Config) ->
    {Port, _} = start_listener(),
    Notification = json:encode(#{<<"jsonrpc">> => <<"2.0">>,
                                 <<"method">> => <<"notifications/cancelled">>,
                                 <<"params">> => #{<<"requestId">> => 1}}),
    {202, _, <<>>} = request(Port, headers(<<"notifications/cancelled">>, #{}),
                             Notification).

%%%_* Pass-through calls ----------------------------------------------------

tools_call_roundtrip(_Config) ->
    ok = application:set_env(broker_proto_mcp, features, [tools]),
    Backend = start_backend(<<"tools/call">>,
                            fun(Request) ->
                                    #{<<"params">> := #{<<"name">> := Name}} = Request,
                                    [{<<"rpcResult">>,
                                      #{<<"result">> =>
                                            #{<<"resultType">> => <<"complete">>,
                                              <<"content">> =>
                                                  [#{<<"type">> => <<"text">>,
                                                     <<"text">> => Name}]}}}]
                            end),
    %% Only the request published by this case is inspected: partitions are
    %% shared by the whole suite and a previous case may have left traffic.
    Before = all_messages(),
    {Port, _} = start_listener(),
    Frames = collect_stream(Port, headers(<<"tools/call">>, tool_call(<<"echo">>, #{})),
                            body(<<"tools/call">>, tool_call(<<"echo">>, #{}), 7),
                            5000),
    ?assertMatch([#{<<"id">> := 7, <<"result">> := #{<<"content">> := [_]}}], Frames),
    stop_backend(Backend),
    %% The request reached the kernel with the gateway's reply handle.
    New = all_messages() -- Before,
    [Message] = [M || {_P, #{destination := <<"$mcp/tools/call">>} = M} <- New],
    ?assertMatch(#{<<"reply_to">> := <<"$mcp/reply/t1/", _/binary>>},
                 maps:get(headers, Message)).

tools_call_forwards_progress_then_result(_Config) ->
    ok = application:set_env(broker_proto_mcp, features, [tools]),
    start_backend(<<"tools/call">>,
                  fun(_Request) ->
                          [{<<"progress">>, #{<<"progress">> => 1,
                                              <<"total">> => 2,
                                              <<"message">> => <<"half">>}},
                           {<<"progress">>, #{<<"progress">> => 2, <<"total">> => 2}},
                           {<<"rpcResult">>, #{<<"result">> =>
                                                   #{<<"resultType">> => <<"complete">>}}}]
                  end),
    {Port, _} = start_listener(),
    Params = with_progress_token(tool_call(<<"echo">>, #{}), <<"tok-1">>),
    Frames = collect_stream(Port, headers(<<"tools/call">>, Params),
                            body(<<"tools/call">>, Params, 3), 5000),
    ?assertMatch([#{<<"method">> := <<"notifications/progress">>,
                    <<"params">> := #{<<"progressToken">> := <<"tok-1">>,
                                      <<"progress">> := 1}},
                  #{<<"method">> := <<"notifications/progress">>,
                    <<"params">> := #{<<"progress">> := 2}},
                  #{<<"id">> := 3, <<"result">> := _}],
                 Frames).

tools_call_forwards_notification(_Config) ->
    ok = application:set_env(broker_proto_mcp, features, [tools]),
    start_backend(<<"tools/call">>,
                  fun(_Request) ->
                          [{<<"notification">>,
                            #{<<"method">> => <<"notifications/message">>,
                              <<"params">> => #{<<"level">> => <<"info">>,
                                                <<"data">> => <<"working">>}}},
                           {<<"rpcResult">>, #{<<"result">> =>
                                                   #{<<"resultType">> => <<"complete">>}}}]
                  end),
    {Port, _} = start_listener(),
    Frames = collect_stream(Port, headers(<<"tools/call">>, tool_call(<<"echo">>, #{})),
                            body(<<"tools/call">>, tool_call(<<"echo">>, #{}), 4),
                            5000),
    ?assertMatch([#{<<"method">> := <<"notifications/message">>,
                    <<"params">> := #{<<"level">> := <<"info">>}},
                  #{<<"id">> := 4, <<"result">> := _}],
                 Frames).

tools_call_error_is_forwarded(_Config) ->
    ok = application:set_env(broker_proto_mcp, features, [tools]),
    start_backend(<<"tools/call">>,
                  fun(_Request) ->
                          [{<<"rpcResult">>,
                            #{<<"error">> => #{<<"code">> => -32602,
                                               <<"message">> => <<"bad tool">>}}}]
                  end),
    {Port, _} = start_listener(),
    Frames = collect_stream(Port, headers(<<"tools/call">>, tool_call(<<"echo">>, #{})),
                            body(<<"tools/call">>, tool_call(<<"echo">>, #{}), 5),
                            5000),
    ?assertMatch([#{<<"id">> := 5,
                    <<"error">> := #{<<"code">> := -32602,
                                     <<"message">> := <<"bad tool">>}}], Frames).

tools_call_timeout_is_32603(_Config) ->
    ok = application:set_env(broker_proto_mcp, features, [tools]),
    ok = application:set_env(broker_proto_mcp, request_timeout_ms, 300),
    {Port, _} = start_listener(),
    Frames = collect_stream(Port, headers(<<"tools/call">>, tool_call(<<"echo">>, #{})),
                            body(<<"tools/call">>, tool_call(<<"echo">>, #{}), 6),
                            5000),
    ?assertMatch([#{<<"id">> := 6,
                    <<"error">> := #{<<"code">> := -32603,
                                     <<"data">> := #{<<"reason">> := <<"timeout">>}}}],
                 Frames).

tools_call_requires_name_header(_Config) ->
    %% Covered twice on purpose: the gate must not depend on feature routing.
    ok = application:set_env(broker_proto_mcp, features, []),
    {Port, _} = start_listener(),
    Params = tool_call(<<"echo">>, #{}),
    Headers = drop_header("Mcp-Name", headers(<<"tools/call">>, Params)),
    {400, _, Body} = request(Port, Headers, body(<<"tools/call">>, Params, 1)),
    ?assertMatch(#{<<"error">> := #{<<"code">> := -32020}}, decode(Body)).

unknown_method_is_404(_Config) ->
    ok = application:set_env(broker_proto_mcp, features, [tools]),
    {Port, _} = start_listener(),
    {404, _, Body} = request(Port, headers(<<"tools/nope">>, #{}),
                             body(<<"tools/nope">>, #{}, 1)),
    ?assertMatch(#{<<"error">> := #{<<"code">> := -32601}}, decode(Body)).

feature_not_advertised_is_404(_Config) ->
    {Port, _} = start_listener(),
    {404, _, Body} = request(Port, headers(<<"tools/list">>, #{}),
                             body(<<"tools/list">>, #{}, 1)),
    #{<<"error">> := Error} = decode(Body),
    ?assertEqual(-32601, maps:get(<<"code">>, Error)),
    ?assertEqual(<<"feature not advertised">>,
                 maps:get(<<"reason">>, maps:get(<<"data">>, Error))).

sampling_is_refused(_Config) ->
    {Port, _} = start_listener(),
    {404, _, Body} = request(Port, headers(<<"sampling/createMessage">>, #{}),
                             body(<<"sampling/createMessage">>, #{}, 1)),
    ?assertMatch(#{<<"error">> := #{<<"code">> := -32601}}, decode(Body)).

%%%_* subscriptions/listen --------------------------------------------------

listen_acknowledges_first(_Config) ->
    ok = application:set_env(broker_proto_mcp, sse_max_duration_ms, 600),
    {Port, _} = start_listener(),
    Params = listen_params(#{<<"toolsListChanged">> => true}),
    Frames = collect_listen_frames(Port, Params, 1, 2000),
    [Ack | _] = Frames,
    ?assertEqual(<<"notifications/subscriptions/acknowledged">>,
                 maps:get(<<"method">>, Ack)),
    ?assertEqual(#{<<"toolsListChanged">> => true},
                 maps:get(<<"notifications">>, maps:get(<<"params">>, Ack))),
    ?assertEqual(1, subscription_id(Ack)).

%% The stream is opened and drained by the test process, so the notification is
%% published from a helper in the background until the stream closes. That
%% removes any need to synchronise on the acknowledgment: a few publishes are
%% guaranteed to land after the subscription's cursor.
listen_forwards_tools_list_changed(_Config) ->
    ok = application:set_env(broker_proto_mcp, sse_max_duration_ms, 900),
    {Port, _} = start_listener(),
    Publisher = start_publisher(<<"$mcp/notify/tools/list_changed">>, #{}),
    Frames = collect_listen_frames(Port, listen_params(
                                            #{<<"toolsListChanged">> => true}),
                                   1, 5000),
    stop_publisher(Publisher),
    ?assertEqual(<<"notifications/subscriptions/acknowledged">>,
                 maps:get(<<"method">>, hd(Frames))),
    Notes = [F || #{<<"method">> := <<"notifications/tools/list_changed">>} = F
                      <- Frames],
    ?assertMatch([_ | _], Notes),
    ?assertEqual(1, subscription_id(hd(Notes))).

listen_forwards_resource_updated(_Config) ->
    ok = application:set_env(broker_proto_mcp, sse_max_duration_ms, 900),
    {Port, _} = start_listener(),
    Uri = <<"file:///project/config.json">>,
    Publisher = start_publisher(<<"$mcp/notify/resources/updated/", Uri/binary>>,
                                #{<<"size">> => 3}),
    Frames = collect_listen_frames(
               Port, listen_params(#{<<"resourceSubscriptions">> => [Uri]}),
               1, 5000),
    stop_publisher(Publisher),
    ?assertEqual(<<"notifications/subscriptions/acknowledged">>,
                 maps:get(<<"method">>, hd(Frames))),
    [Note | _] = [F || #{<<"method">> := <<"notifications/resources/updated">>} = F
                           <- Frames],
    Params = maps:get(<<"params">>, Note),
    ?assertEqual(Uri, maps:get(<<"uri">>, Params)),
    ?assertEqual(3, maps:get(<<"size">>, Params)),
    ?assertEqual(1, subscription_id(Note)).

listen_closes_gracefully_at_the_cap(_Config) ->
    ok = application:set_env(broker_proto_mcp, sse_max_duration_ms, 400),
    {Port, _} = start_listener(),
    Frames = collect_listen_frames(Port, listen_params(#{<<"toolsListChanged">> => true}),
                                   1, 5000),
    Ack = hd(Frames),
    ?assertEqual(<<"notifications/subscriptions/acknowledged">>,
                 maps:get(<<"method">>, Ack)),
    %% Graceful closure: a `subscriptions/listen' response with `complete'.
    ?assertMatch([#{<<"id">> := 1,
                    <<"result">> := #{<<"resultType">> := <<"complete">>}}],
                 [F || #{<<"id">> := 1} = F <- Frames]).

%%%_* HTTP client -----------------------------------------------------------

headers(Method, Params) ->
    Name = case Method of
               <<"tools/call">> -> [{"Mcp-Name", to_list(maps:get(<<"name">>, Params))}];
               <<"resources/read">> -> [{"Mcp-Name", to_list(maps:get(<<"uri">>, Params))}];
               <<"prompts/get">> -> [{"Mcp-Name", to_list(maps:get(<<"name">>, Params))}];
               _ -> []
           end,
    [{"Content-Type", "application/json"},
     {"Accept", "application/json, text/event-stream"},
     {"Authorization", "Bearer " ++ binary_to_list(?TOKEN)},
     {"MCP-Protocol-Version", binary_to_list(?VERSION)},
     {"Mcp-Method", binary_to_list(Method)} | Name].

to_list(Bin) when is_binary(Bin) -> binary_to_list(Bin);
to_list(List) when is_list(List) -> List.

drop_header(Name, Headers) ->
    [H || {K, _} = H <- Headers, K =/= Name].

meta() ->
    #{<<"io.modelcontextprotocol/protocolVersion">> => ?VERSION,
      <<"io.modelcontextprotocol/clientInfo">> =>
          #{<<"name">> => <<"mcp-suite">>, <<"version">> => <<"1.0.0">>},
      <<"io.modelcontextprotocol/clientCapabilities">> => #{}}.

body(Method, Params0, Id) ->
    Params = case maps:is_key(<<"_meta">>, Params0) of
                 true -> Params0;
                 false -> Params0#{<<"_meta">> => meta()}
             end,
    json:encode(#{<<"jsonrpc">> => <<"2.0">>, <<"method">> => Method,
                  <<"params">> => Params, <<"id">> => Id}).

tool_call(Name, Arguments) ->
    #{<<"name">> => Name, <<"arguments">> => Arguments}.

with_progress_token(Params, Token) ->
    Meta = maps:get(<<"_meta">>, Params, meta()),
    Params#{<<"_meta">> => Meta#{<<"progressToken">> => Token}}.

listen_params(Filter) ->
    #{<<"notifications">> => Filter}.

subscription_id(#{<<"params">> := #{<<"_meta">> := Meta}}) ->
    maps:get(<<"io.modelcontextprotocol/subscriptionId">>, Meta);
subscription_id(_Frame) ->
    undefined.

%%%_* Listener -------------------------------------------------------------

start_listener() ->
    ok = application:set_env(broker_proto_mcp, listeners,
                             [#{port => 0, ref => {mcp, ct}, num_acceptors => 2}]),
    case application:ensure_all_started(broker_proto_mcp) of
        {ok, _} -> {ranch:get_port({mcp, ct}), ok};
        {error, Reason} -> ct:fail({listener_start_failed, Reason})
    end.

request(Port, Headers, Body) ->
    raw_request(Port, "POST", "/", Headers, Body).

raw_request(Port, Method, Path, Headers, Body) ->
    {ok, S} = gen_tcp:connect({127, 0, 0, 1}, Port,
                              [binary, {active, false}, {packet, raw}], 2000),
    ok = gen_tcp:send(S, build_request(Method, Path, Headers, Body)),
    {Head, Rest} = read_head(S, <<>>),
    {Status, Hdrs} = parse_head(Head),
    Length = case header(<<"content-length">>, Hdrs) of
                 undefined -> 0;
                 V -> binary_to_integer(V)
             end,
    BodyBin = read_exact(S, Rest, Length),
    gen_tcp:close(S),
    {Status, Hdrs, BodyBin}.

build_request(Method, Path, Headers, Body) ->
    BodyBin = iolist_to_binary(Body),
    AllHeaders = [{"Host", "localhost"},
                  {"Content-Length", integer_to_list(byte_size(BodyBin))} | Headers],
    [Method, " ", Path, " HTTP/1.1\r\n",
     [[K, ": ", V, "\r\n"] || {K, V} <- AllHeaders],
     "\r\n", BodyBin].

%% Collect the whole SSE body of a request stream.
collect_stream(Port, Headers, Body, Timeout) ->
    {ok, S} = gen_tcp:connect({127, 0, 0, 1}, Port,
                              [binary, {active, false}, {packet, raw}], 2000),
    ok = gen_tcp:send(S, build_request("POST", "/", Headers, Body)),
    {Head, Rest} = read_head(S, <<>>),
    {200, RespHeaders} = parse_head(Head),
    ?assertEqual(<<"text/event-stream">>, header(<<"content-type">>, RespHeaders)),
    Chunks = read_chunks(S, Rest, <<>>, Timeout),
    gen_tcp:close(S),
    [decode(Frame) || Frame <- split_sse(Chunks)].

%% Open a listen stream, wait for the acknowledgment, then read until close.
collect_listen_frames(Port, Params, Id, Timeout) ->
    {ok, S} = gen_tcp:connect({127, 0, 0, 1}, Port,
                              [binary, {active, false}, {packet, raw}], 2000),
    ok = gen_tcp:send(S, build_request("POST", "/",
                                       headers(<<"subscriptions/listen">>, Params),
                                       body(<<"subscriptions/listen">>, Params, Id))),
    {Head, Rest} = read_head(S, <<>>),
    {200, _RespHeaders} = parse_head(Head),
    Chunks = read_chunks(S, Rest, <<>>, Timeout),
    gen_tcp:close(S),
    [decode(Frame) || Frame <- split_sse(Chunks)].

%% The listen stream is blocking, so background publishes are used instead of a
%% handshake: whatever lands after the subscription's cursor is forwarded.
start_publisher(Destination, Payload) ->
    spawn_link(fun() -> publish_loop(Destination, Payload) end).

publish_loop(Destination, Payload) ->
    receive
        stop -> ok
    after 80 ->
            _ = publish(Destination, Payload, <<"t1">>),
            publish_loop(Destination, Payload)
    end.

stop_publisher(Pid) ->
    Pid ! stop,
    ok.

read_head(S, Buf) ->
    case binary:split(Buf, <<"\r\n\r\n">>) of
        [Head, Rest] -> {Head, Rest};
        [_] ->
            case gen_tcp:recv(S, 0, 5000) of
                {ok, Data} -> read_head(S, <<Buf/binary, Data/binary>>);
                {error, Reason} -> ct:fail({read_head_failed, Reason})
            end
    end.

read_chunks(S, Buf, Acc, Timeout) ->
    case decode_chunks(Buf) of
        {done, Body} -> <<Acc/binary, Body/binary>>;
        {more, Body, Rest} ->
            case gen_tcp:recv(S, 0, Timeout) of
                {ok, Data} -> read_chunks(S, <<Rest/binary, Data/binary>>,
                                          <<Acc/binary, Body/binary>>, Timeout);
                {error, closed} -> <<Acc/binary, Body/binary>>;
                {error, timeout} -> <<Acc/binary, Body/binary>>;
                {error, Reason} -> ct:fail({read_chunks_failed, Reason})
            end
    end.

decode_chunks(Buf) ->
    decode_chunks(Buf, <<>>).

decode_chunks(Buf, Acc) ->
    case binary:split(Buf, <<"\r\n">>) of
        [SizeLine, Rest] ->
            case catch binary_to_integer(SizeLine, 16) of
                0 -> {done, Acc};
                Size when is_integer(Size) ->
                    case Rest of
                        <<Chunk:Size/binary, "\r\n", Rest2/binary>> ->
                            decode_chunks(Rest2, <<Acc/binary, Chunk/binary>>);
                        _ -> {more, Acc, Buf}
                    end;
                _ -> {more, Acc, Buf}
            end;
        [_] ->
            {more, Acc, Buf}
    end.

split_sse(Bin) ->
    Parts = binary:split(Bin, <<"\n\n">>, [global]),
    [Payload || Part <- Parts, (Payload = data_of(Part)) =/= none].

data_of(Part) ->
    Lines = binary:split(Part, <<"\n">>, [global]),
    Data = [strip_data(L) || L <- Lines, is_data_line(L)],
    case Data of
        [] -> none;
        _ -> iolist_to_binary(lists:join(<<>>, Data))
    end.

is_data_line(<<"data:", _/binary>>) -> true;
is_data_line(_) -> false.

strip_data(<<"data: ", Rest/binary>>) -> Rest;
strip_data(<<"data:", Rest/binary>>) -> Rest.

parse_head(Head) ->
    [StatusLine | HeaderLines] = binary:split(Head, <<"\r\n">>, [global]),
    [_Version, Code | _] = binary:split(StatusLine, <<" ">>, [global]),
    Status = binary_to_integer(Code),
    Headers = [split_header(Line) || Line <- HeaderLines, Line =/= <<>>],
    {Status, Headers}.

split_header(Line) ->
    [Name, Value] = binary:split(Line, <<":">>),
    {lower(trim(Name)), trim(Value)}.

header(Name, Headers) ->
    proplists:get_value(Name, Headers).

read_exact(_S, Buf, 0) -> Buf;
read_exact(_S, Buf, N) when byte_size(Buf) >= N -> binary:part(Buf, 0, N);
read_exact(S, Buf, N) ->
    case gen_tcp:recv(S, N - byte_size(Buf), 5000) of
        {ok, Data} -> read_exact(S, <<Buf/binary, Data/binary>>, N);
        {error, _} -> Buf
    end.

lower(Bin) ->
    << <<(lower_byte(B))/integer>> || <<B/integer>> <= Bin >>.

lower_byte(B) when B >= $A, B =< $Z -> B + 32;
lower_byte(B) -> B.

trim(Bin) ->
    string:trim(Bin, both, " \t").

decode(Bin) ->
    json:decode(Bin).

%%%_* Fake backend ---------------------------------------------------------

%% Polls every partition for `$mcp/<Method>' requests and answers each one at
%% the gateway's `replyTo'. The handler returns a list of `{Kind, Payload}'
%% frames, which is exactly the contract ADR-027 §D2 gives a backend.
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

stop_backend(Pid) ->
    _ = erase(mcp_backend),
    exit(Pid, kill),
    ok.

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
               Method, Handler)
  when Destination =:= <<"$mcp/", Method/binary>> ->
    Request = json:decode(iolist_to_binary(Payload)),
    ReplyTo = reply_to(Request),
    lists:foreach(fun({Kind, Frame}) -> reply(ReplyTo, Kind, Frame) end,
                  Handler(Request));
handle_request(_Message, _Method, _Handler) ->
    ok.

reply_to(#{<<"params">> := #{<<"_meta">> := Meta}}) ->
    #{<<"unified-mq">> := #{<<"replyTo">> := ReplyTo}} = Meta,
    ReplyTo.

reply(ReplyTo, Kind, Payload) ->
    Meta = #{<<"unified-mq">> => #{<<"kind">> => Kind}},
    ok = publish(ReplyTo, Payload#{<<"_meta">> => Meta}, <<"t1">>),
    ok.

publish(Destination, Payload, Tenant) ->
    {ok, Env} = broker_router:env(),
    Ctx = #route_ctx{tenant_id = Tenant, publisher = <<"mcp-test-backend">>,
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
        {ok, _, High} -> High;
        {error, _} -> 0
    end.

%% Every message a destination received, decoded from the kernel.
kernel_messages(Destination) ->
    [Maps || {_P, #{destination := D} = Maps} <- all_messages(), D =:= Destination].

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
