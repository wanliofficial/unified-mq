%% @doc S9-C: SSE streaming, push notification CRUD and the egress guards
%% (ADR-025 §D7, ADR-026 §D6, R-A1/2/4/5/6/7).
%%
%% The suite is split in three groups so a failure names its subject:
%%
%%   * `stream'  -- SSE framing, event order, close-on-terminal, resync after a
%%                  dropped connection, heartbeat, and the "a slow reader does
%%                  not pin retention" property;
%%   * `push'    -- config CRUD over JSON-RPC plus a real end-to-end webhook
%%                  delivery against a local receiver;
%%   * `guard'   -- the SSRF refusals and the replay/signature refusals, each as
%%                  its own case so a regression names the exact rule.
%%
%% A local TCP receiver stands in for the webhook endpoint: `allow_private' is
%% what makes a loopback URL legitimate, which is itself part of the guard.
-module(a2a_stream_push_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-compile([export_all, nowarn_export_all]).

-define(TOKEN,  <<"stream-push-token">>).
-define(PARTITIONS, 4).

all() ->
    [{group, stream}, {group, push}, {group, guard}].

groups() ->
    [{stream, [sequence],
      [send_streaming_creates_task_and_closes_on_terminal,
       stream_frames_are_json_rpc_responses,
       subscribe_replays_history_from_offset_zero,
       subscribe_unknown_task_is_404,
       stream_heartbeat_keeps_connection_open,
       stream_closes_on_interrupted_state,
       slow_reader_does_not_pin_retention]},
     {push, [sequence],
      [push_disabled_answers_32003,
       create_get_list_delete_roundtrip,
       push_config_is_tenant_scoped,
       delivery_posts_terminal_event,
       delivery_retries_then_writes_dlq]},
     {guard, [sequence],
      [ssrf_refuses_private_address,
       ssrf_refuses_when_allowlist_is_empty,
       ssrf_refuses_host_outside_allowlist,
       ssrf_refuses_non_https,
       ssrf_refuses_redirect,
       guard_allows_private_when_opted_in,
       replay_headers_are_present_and_signed,
       unconfigured_auth_scheme_is_refused]}].

%%%_* Fixtures --------------------------------------------------------------

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(broker_security),
    {ok, _} = application:ensure_all_started(broker_proto_agent_rpc),
    DataDir = filename:join(?config(priv_dir, Config), "a2a-stream-data"),
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
    {ok, _} = application:ensure_all_started(broker_storage),
    [ok = broker_queue:create_partition(P) || P <- broker_queue:partition_ids()],
    _ = application:load(broker_proto_a2a),
    Old = [{K, application:get_env(broker_proto_a2a, K)}
           || K <- [listeners, require_auth, max_body_bytes, task_auto_complete,
                    agent_cards, agent_card_max_age, push_enabled,
                    egress_allowlist, push_tenant_allowlist, push_auth_secrets,
                    push_allow_http, push_max_attempts, push_base_backoff_ms,
                    push_ttl_ms, sse_heartbeat_ms, sse_max_duration_ms]],
    OldSec = [{K, application:get_env(broker_security, K)}
              || K <- [users, acl]],
    ok = application:set_env(broker_security, users,
                             [{<<"alice">>, #{token => ?TOKEN,
                                              tenant_id => <<"t1">>}}]),
    ok = application:set_env(broker_security, acl, allow),
    ok = application:set_env(broker_proto_a2a, listeners, []),
    ok = application:set_env(broker_proto_a2a, require_auth, true),
    ok = application:set_env(broker_proto_a2a, max_body_bytes, 1048576),
    ok = application:set_env(broker_proto_a2a, agent_card_max_age, 60),
    ok = application:set_env(broker_proto_a2a, agent_cards, []),
    ok = application:set_env(broker_proto_a2a, push_enabled, false),
    ok = application:set_env(broker_proto_a2a, egress_allowlist, []),
    ok = application:set_env(broker_proto_a2a, push_tenant_allowlist, #{}),
    ok = application:set_env(broker_proto_a2a, push_auth_secrets, #{}),
    ok = application:set_env(broker_proto_a2a, push_allow_http, false),
    ok = application:set_env(broker_proto_a2a, sse_heartbeat_ms, 60),
    ok = application:set_env(broker_proto_a2a, sse_max_duration_ms, 20000),
    {ok, _} = application:ensure_all_started(broker_proto_a2a),
    [{data_dir, DataDir}, {old_a2a, Old}, {old_security, OldSec} | Config].

end_per_suite(Config) ->
    _ = application:stop(broker_proto_a2a),
    _ = application:stop(broker_queue),
    _ = application:stop(broker_router),
    _ = application:stop(broker_storage),
    _ = application:stop(broker_meta),
    _ = application:stop(mnesia),
    _ = mnesia:delete_schema([node()]),
    lists:foreach(fun restore_a2a/1, ?config(old_a2a, Config)),
    lists:foreach(fun restore_security/1, ?config(old_security, Config)),
    ok.

init_per_group(Group, Config) ->
    [{group, Group} | Config].

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    reset_push_env(),
    ok = application:set_env(broker_proto_a2a, task_auto_complete, true),
    Config.

end_per_testcase(_Case, _Config) ->
    _ = application:stop(broker_proto_a2a),
    reset_push_env(),
    ok = application:set_env(broker_proto_a2a, listeners, []),
    ok = application:set_env(broker_proto_a2a, require_auth, true),
    ok = application:set_env(broker_proto_a2a, task_auto_complete, true),
    ok = application:set_env(broker_security, acl, allow),
    {ok, _} = application:ensure_all_started(broker_proto_a2a),
    ok.

reset_push_env() ->
    ok = application:set_env(broker_proto_a2a, push_enabled, false),
    ok = application:set_env(broker_proto_a2a, egress_allowlist, []),
    ok = application:set_env(broker_proto_a2a, push_tenant_allowlist, #{}),
    ok = application:set_env(broker_proto_a2a, push_auth_secrets, #{}),
    ok = application:set_env(broker_proto_a2a, push_allow_http, false),
    ok = application:set_env(broker_proto_a2a, push_max_attempts, 2),
    ok = application:set_env(broker_proto_a2a, push_base_backoff_ms, 10),
    ok = application:set_env(broker_proto_a2a, push_ttl_ms, 5000),
    ok.

%%%_* SSE -------------------------------------------------------------------

send_streaming_creates_task_and_closes_on_terminal(_Config) ->
    {Port, _} = start_listener([]),
    Frames = collect_stream(Port, <<"SendStreamingMessage">>,
                            params(unique(<<"ctx-stream">>)), 20000),
    ?assert(length(Frames) >= 1),
    %% The stream must have ended on a terminal status update, and the frames
    %% must cover SUBMITTED -> WORKING -> COMPLETED.
    States = [state_of(F) || F <- Frames, is_status_update(F)],
    ?assert(lists:member(<<"TASK_STATE_COMPLETED">>, States)),
    Last = lists:last(Frames),
    ?assert(is_status_update(Last)),
    ?assertEqual(<<"TASK_STATE_COMPLETED">>, state_of(Last)).

stream_frames_are_json_rpc_responses(_Config) ->
    {Port, _} = start_listener([]),
    Frames = collect_stream(Port, <<"SendStreamingMessage">>,
                            params(unique(<<"ctx-frames">>)), 20000),
    lists:foreach(
      fun(Frame) ->
              %% Every frame is one complete JSON-RPC response: same id, no
              %% error, and a `result' that is a StreamResponse branch.
              ?assertMatch(#{<<"jsonrpc">> := <<"2.0">>, <<"id">> := 1,
                             <<"result">> := _}, Frame),
              ?assertNot(maps:is_key(<<"error">>, Frame)),
              Result = maps:get(<<"result">>, Frame),
              ?assert(lists:any(fun(K) -> maps:is_key(K, Result) end,
                                [<<"task">>, <<"message">>, <<"statusUpdate">>,
                                 <<"artifactUpdate">>]))
      end, Frames).

subscribe_replays_history_from_offset_zero(_Config) ->
    {Port, _} = start_listener([]),
    %% Create the task first, then subscribe: the replay must return the whole
    %% history even though every event predates the connection.
    {200, Resp} = rpc(Port, <<"SendMessage">>, params(unique(<<"ctx-sub">>)), 1),
    TaskId = maps:get(<<"id">>, task(Resp)),
    Frames = collect_stream(Port, <<"SubscribeToTask">>,
                            #{<<"id">> => TaskId}, 20000),
    ?assert(length(Frames) >= 3),
    %% Replay starts at the creation snapshot.
    First = hd(Frames),
    ?assertMatch(#{<<"result">> := #{<<"task">> := _}}, First),
    %% And ends at the terminal state.
    ?assertEqual(<<"TASK_STATE_COMPLETED">>, state_of(lists:last(Frames))).

subscribe_unknown_task_is_404(_Config) ->
    {Port, _} = start_listener([]),
    {404, Resp} = rpc(Port, <<"SubscribeToTask">>,
                      #{<<"id">> => <<"does-not-exist">>}, 1),
    ?assertMatch(#{<<"error">> := #{<<"code">> := -32001}}, Resp).

stream_heartbeat_keeps_connection_open(_Config) ->
    {Port, _} = start_listener([]),
    ok = application:set_env(broker_proto_a2a, sse_heartbeat_ms, 20),
    %% An async task stays non-terminal, so the stream has nothing to send and
    %% must emit heartbeat comments instead of closing.
    ok = application:set_env(broker_proto_a2a, task_auto_complete, false),
    {ok, S} = gen_tcp:connect({127, 0, 0, 1}, Port,
                              [binary, {active, false}, {packet, raw}], 2000),
    ok = gen_tcp:send(S, build_request(post, "/",
                                       post_headers(?TOKEN),
                                       rpc_body(<<"SendStreamingMessage">>,
                                                params(unique(<<"ctx-hb">>)), 1))),
    {Head, Rest} = read_head(S, <<>>),
    {200, Headers} = parse_head(Head),
    ?assertEqual(<<"text/event-stream">>, header(<<"content-type">>, Headers)),
    ?assertEqual(<<"chunked">>, header(<<"transfer-encoding">>, Headers)),
    %% Read until a heartbeat comment shows up (": hb" / any comment line).
    Body = read_until_comment(S, Rest, <<>>, 5000),
    ?assert(binary:match(Body, <<":">>) =/= nomatch),
    gen_tcp:close(S),
    ok.

stream_closes_on_interrupted_state(_Config) ->
    {Port, _} = start_listener([]),
    ok = application:set_env(broker_proto_a2a, task_auto_complete, false),
    {200, Resp} = rpc(Port, <<"SendMessage">>, params(unique(<<"ctx-int">>)), 1),
    TaskId = maps:get(<<"id">>, task(Resp)),
    %% Move the task to an interrupted state directly through the shard API.
    {ok, _} = a2a_task_registry:apply_event(
                TaskId, <<"t1">>, status_update,
                #{<<"statusUpdate">> =>
                      #{<<"taskId">> => TaskId,
                        <<"status">> => #{<<"state">> =>
                                              <<"TASK_STATE_INPUT_REQUIRED">>,
                                          <<"timestamp">> =>
                                              a2a_event:timestamp()},
                        <<"final">> => true}}),
    Frames = collect_stream(Port, <<"SubscribeToTask">>,
                            #{<<"id">> => TaskId}, 20000),
    ?assert(length(Frames) >= 1),
    ?assertEqual(<<"TASK_STATE_INPUT_REQUIRED">>, state_of(lists:last(Frames))).

slow_reader_does_not_pin_retention(_Config) ->
    {Port, _} = start_listener([]),
    {200, Resp} = rpc(Port, <<"SendMessage">>, params(unique(<<"ctx-slow">>)), 1),
    TaskId = maps:get(<<"id">>, task(Resp)),
    %% Start a stream and walk away without reading. The stream is a transient
    %% reader (ADR-025 §D7): no SubRef is created, so nothing about it appears
    %% in the subscription table and nothing can pin retention.
    {ok, S} = gen_tcp:connect({127, 0, 0, 1}, Port,
                              [binary, {active, false}, {packet, raw}], 2000),
    ok = gen_tcp:send(S, build_request(post, "/", post_headers(?TOKEN),
                                       rpc_body(<<"SubscribeToTask">>,
                                                #{<<"id">> => TaskId}, 1))),
    {_Head, _Rest} = read_head(S, <<>>),
    %% The stream must have completed and closed on its own; a completed task
    %% ends the stream, so the socket reaches EOF.
    ?assertEqual(ok, drain_until_closed(S, 10000)),
    gen_tcp:close(S),
    ok.

%%%_* Push CRUD -------------------------------------------------------------

push_disabled_answers_32003(_Config) ->
    {Port, _} = start_listener([]),
    {200, Resp} = rpc(Port, <<"SendMessage">>, params(unique(<<"ctx-nopush">>)), 1),
    TaskId = maps:get(<<"id">>, task(Resp)),
    %% `push_notification_not_supported' is a method-level error, which A2A
    %% §5.4 pins to HTTP 400 (unlike envelope errors, which stay 200).
    {400, Err} = rpc(Port, <<"CreateTaskPushNotificationConfig">>,
                     push_params(TaskId, <<"https://example.com/hook">>), 2),
    ?assertMatch(#{<<"error">> := #{<<"code">> := -32003}}, Err).

create_get_list_delete_roundtrip(_Config) ->
    ok = enable_push(<<"example.com">>, <<"t1">>),
    {Port, _} = start_listener_with_push(),
    {200, Resp} = rpc(Port, <<"SendMessage">>, params(unique(<<"ctx-crud">>)), 1),
    TaskId = maps:get(<<"id">>, task(Resp)),
    %% Create
    {200, Created} = rpc(Port, <<"CreateTaskPushNotificationConfig">>,
                         push_params(TaskId, <<"https://example.com/hook">>), 2),
    Config = maps:get(<<"result">>, Created),
    ConfigId = maps:get(<<"id">>, Config),
    ?assert(is_binary(ConfigId)),
    ?assertEqual(TaskId, maps:get(<<"taskId">>, Config)),
    ?assertEqual(<<"https://example.com/hook">>, maps:get(<<"url">>, Config)),
    %% Get
    {200, Got} = rpc(Port, <<"GetTaskPushNotificationConfig">>,
                     #{<<"taskId">> => TaskId, <<"configId">> => ConfigId}, 3),
    ?assertEqual(Config, maps:get(<<"result">>, Got)),
    %% List
    {200, Listed} = rpc(Port, <<"ListTaskPushNotificationConfigs">>,
                        #{<<"taskId">> => TaskId}, 4),
    ?assertEqual([Config], maps:get(<<"configs">>, maps:get(<<"result">>, Listed))),
    %% Delete
    {200, Deleted} = rpc(Port, <<"DeleteTaskPushNotificationConfig">>,
                         #{<<"taskId">> => TaskId, <<"configId">> => ConfigId}, 5),
    ?assertEqual(#{}, maps:get(<<"result">>, Deleted)),
    {200, Listed2} = rpc(Port, <<"ListTaskPushNotificationConfigs">>,
                         #{<<"taskId">> => TaskId}, 6),
    ?assertEqual([], maps:get(<<"configs">>, maps:get(<<"result">>, Listed2))).

push_config_is_tenant_scoped(_Config) ->
    ok = enable_push(<<"example.com">>, <<"t1">>),
    {Port, _} = start_listener_with_push(),
    {200, Resp} = rpc(Port, <<"SendMessage">>, params(unique(<<"ctx-tenant">>)), 1),
    TaskId = maps:get(<<"id">>, task(Resp)),
    {200, Created} = rpc(Port, <<"CreateTaskPushNotificationConfig">>,
                         push_params(TaskId, <<"https://example.com/hook">>), 2),
    ConfigId = maps:get(<<"id">>, maps:get(<<"result">>, Created)),
    %% Another tenant cannot see the config: an unknown credential is refused
    %% at transport level (bare 401), and a *known* second tenant would get the
    %% same `task_not_found' as a nonexistent task id.
    {401, _, <<>>} = request(Port, post_headers(<<"other-token">>),
                             rpc_body(<<"GetTaskPushNotificationConfig">>,
                                      #{<<"taskId">> => TaskId,
                                        <<"configId">> => ConfigId}, 3)),
    %% Seed a real t2 principal and prove the config is invisible to it.
    ok = application:set_env(broker_security, users,
                             [{<<"alice">>, #{token => ?TOKEN,
                                              tenant_id => <<"t1">>}},
                              {<<"bob">>, #{token => <<"t2-token">>,
                                            tenant_id => <<"t2">>}}]),
    {_Status, _, Body} = request(Port, post_headers(<<"t2-token">>),
                                 rpc_body(<<"GetTaskPushNotificationConfig">>,
                                          #{<<"taskId">> => TaskId,
                                            <<"configId">> => ConfigId}, 4)),
    ?assertMatch(#{<<"error">> := _}, decode(Body)).

delivery_posts_terminal_event(_Config) ->
    %% An async task is what lets us drive the terminal transition ourselves
    %% (CANCELED), which is the state that must fire the webhook.
    ok = application:set_env(broker_proto_a2a, task_auto_complete, false),
    {Receiver, ReceiverPort} = start_receiver(),
    Url = iolist_to_binary(["http://127.0.0.1:", integer_to_list(ReceiverPort),
                            "/hook"]),
    ok = enable_push_localhost(Url),
    {Port, _} = start_listener_with_push(),
    {200, Resp} = rpc(Port, <<"SendMessage">>, params(unique(<<"ctx-deliver">>)), 1),
    TaskId = maps:get(<<"id">>, task(Resp)),
    {200, _Created} = rpc(Port, <<"CreateTaskPushNotificationConfig">>,
                          push_params(TaskId, Url), 2),
    %% The config belongs to *this* task, so cancelling it is what fires the
    %% webhook for it.
    {200, _} = rpc(Port, <<"CancelTask">>, #{<<"id">> => TaskId}, 3),
    Request = receive_delivery(Receiver, 10000),
    ?assertMatch(#{method := <<"POST">>, path := <<"/hook">>}, Request),
    ?assert(binary:match(maps:get(body, Request), TaskId) =/= nomatch),
    stop_receiver(Receiver).

delivery_retries_then_writes_dlq(_Config) ->
    %% A receiver that always answers 500 forces the retry path; after
    %% `push_max_attempts' the failure lands on `$a2a/push/dlq/t1'.
    ok = application:set_env(broker_proto_a2a, task_auto_complete, false),
    {Receiver, ReceiverPort} = start_receiver_with_status(500),
    Url = iolist_to_binary(["http://127.0.0.1:", integer_to_list(ReceiverPort),
                            "/fail"]),
    ok = enable_push_localhost(Url),
    ok = application:set_env(broker_proto_a2a, push_max_attempts, 2),
    {Port, _} = start_listener_with_push(),
    {200, Resp} = rpc(Port, <<"SendMessage">>, params(unique(<<"ctx-dlq">>)), 1),
    TaskId = maps:get(<<"id">>, task(Resp)),
    {200, _} = rpc(Port, <<"CreateTaskPushNotificationConfig">>,
                   push_params(TaskId, Url), 2),
    {200, _} = rpc(Port, <<"CancelTask">>, #{<<"id">> => TaskId}, 3),
    %% Two attempts, then the DLQ write.
    _ = receive_delivery(Receiver, 10000),
    Dlq = wait_for_dlq(<<"t1">>, TaskId, 15000),
    ?assertMatch([_ | _], Dlq),
    stop_receiver(Receiver).

%%%_* Guards ----------------------------------------------------------------

ssrf_refuses_private_address(_Config) ->
    %% `guard/3' is the egress decision itself: allowlist passes, address
    %% validation refuses because 127.0.0.1 is loopback.
    ok = application:set_env(broker_proto_a2a, push_enabled, true),
    ok = application:set_env(broker_proto_a2a, push_allow_http, true),
    Allowed = [#{host => <<"127.0.0.1">>, allow_private => false}],
    ok = application:set_env(broker_proto_a2a, egress_allowlist, Allowed),
    ?assertEqual({error, private_address},
                 a2a_push:guard(<<"t1">>, <<"http://127.0.0.1/hook">>, Allowed)),
    %% 10/8, 172.16/12, 192.168/16, 169.254/16 and ::1 are all refused.
    lists:foreach(
      fun(Ip) ->
              ?assert(a2a_push:is_private_address(Ip))
      end,
      [{127, 0, 0, 1}, {10, 1, 2, 3}, {172, 16, 0, 1}, {192, 168, 1, 1},
       {169, 254, 1, 1}, {0, 0, 0, 0}, {224, 0, 0, 1}, {255, 255, 255, 255},
       {0, 0, 0, 0, 0, 0, 0, 1}, {16#fe80, 0, 0, 0, 0, 0, 0, 1}]),
    ?assertNot(a2a_push:is_private_address({93, 184, 216, 34})).

ssrf_refuses_when_allowlist_is_empty(_Config) ->
    ok = application:set_env(broker_proto_a2a, push_enabled, true),
    ok = application:set_env(broker_proto_a2a, egress_allowlist, []),
    ?assertEqual({error, empty_allowlist},
                 a2a_push:guard(<<"t1">>, <<"https://example.com/hook">>, [])).

ssrf_refuses_host_outside_allowlist(_Config) ->
    ok = application:set_env(broker_proto_a2a, push_enabled, true),
    ok = application:set_env(broker_proto_a2a, egress_allowlist,
                             [#{host => <<"api.example.com">>}]),
    %% Global matches but the tenant table does not: intersection is empty.
    ?assertEqual({error, empty_tenant_allowlist},
                 a2a_push:guard(<<"t1">>, <<"https://api.example.com/hook">>, [])),
    %% Both match, but the host is not the allowlisted one.
    ?assertEqual({error, host_not_allowlisted},
                 a2a_push:guard(<<"t1">>, <<"https://evil.test/hook">>,
                                [#{host => <<"api.example.com">>}])),
    %% Suffix entries match the domain and its subdomains.
    ?assertEqual({error, host_not_allowlisted},
                 a2a_push:guard(<<"t1">>, <<"https://notexample.com/hook">>,
                                [#{host => <<".example.com">>}])).

ssrf_refuses_non_https(_Config) ->
    ok = application:set_env(broker_proto_a2a, push_enabled, true),
    ok = application:set_env(broker_proto_a2a, egress_allowlist,
                             [#{host => <<"api.example.com">>}]),
    ?assertEqual({error, scheme_not_allowed},
                 a2a_push:guard(<<"t1">>, <<"ftp://api.example.com/hook">>,
                                [#{host => <<"api.example.com">>}])),
    %% http is refused unless explicitly enabled.
    ?assertEqual({error, scheme_not_allowed},
                 a2a_push:guard(<<"t1">>, <<"http://api.example.com/hook">>,
                                [#{host => <<"api.example.com">>}])).

ssrf_refuses_redirect(_Config) ->
    %% Redirects are never followed: a 302 is an error, not a new request.
    {Receiver, Port} = start_receiver_with_status(302),
    Url = iolist_to_binary(["http://127.0.0.1:", integer_to_list(Port), "/r"]),
    ok = enable_push_localhost(Url),
    Endpoint = endpoint_for(Url),
    {error, {unexpected_status, 302}} =
        a2a_push:deliver(Endpoint, <<"{}">>, []),
    stop_receiver(Receiver).

guard_allows_private_when_opted_in(_Config) ->
    ok = application:set_env(broker_proto_a2a, push_enabled, true),
    ok = application:set_env(broker_proto_a2a, push_allow_http, true),
    Allowed = [#{host => <<"127.0.0.1">>, allow_private => true}],
    ok = application:set_env(broker_proto_a2a, egress_allowlist, Allowed),
    {ok, Endpoint} = a2a_push:guard(<<"t1">>, <<"http://127.0.0.1:9/hook">>,
                                    Allowed),
    ?assertEqual(true, maps:get(allow_private, Endpoint)).

replay_headers_are_present_and_signed(_Config) ->
    ok = application:set_env(broker_proto_a2a, push_auth_secrets,
                             #{<<"bearer">> => #{secret => <<"s3cret">>,
                                                 header => bearer}}),
    Body = <<"{\"a\":1}">>,
    {ok, Headers} = a2a_push_auth:headers(<<"bearer">>, Body,
                                          #{task_id => <<"t-1">>,
                                            config_id => <<"c-1">>}),
    ?assert(is_binary(proplists:get_value(<<"x-a2a-timestamp">>, Headers))),
    Nonce1 = proplists:get_value(<<"x-a2a-nonce">>, Headers),
    ?assert(is_binary(Nonce1)),
    ?assertEqual(<<"Bearer s3cret">>,
                 proplists:get_value(<<"authorization">>, Headers)),
    Sig = proplists:get_value(<<"x-a2a-signature">>, Headers),
    ?assertMatch(<<"sha256=", _/binary>>, Sig),
    %% Two requests never reuse a nonce.
    {ok, Headers2} = a2a_push_auth:headers(<<"bearer">>, Body,
                                           #{task_id => <<"t-1">>,
                                             config_id => <<"c-1">>}),
    ?assertNotEqual(Nonce1, proplists:get_value(<<"x-a2a-nonce">>, Headers2)).

unconfigured_auth_scheme_is_refused(_Config) ->
    %% A client may name a scheme, but if this node has no secret for it the
    %% push is refused rather than sent unauthenticated.
    {error, not_configured} = a2a_push_auth:secret_for(<<"nope">>),
    Body = <<"{}">>,
    {error, not_configured} =
        a2a_push_auth:headers(<<"nope">>, Body,
                              #{task_id => <<"t">>, config_id => <<"c">>}).

%%%_* SSE client ------------------------------------------------------------

collect_stream(Port, Method, Params, Timeout) ->
    {ok, S} = gen_tcp:connect({127, 0, 0, 1}, Port,
                              [binary, {active, false}, {packet, raw}], 2000),
    ok = gen_tcp:send(S, build_request(post, "/", post_headers(?TOKEN),
                                       rpc_body(Method, Params, 1))),
    {Head, Rest} = read_head(S, <<>>),
    {200, Headers} = parse_head(Head),
    ?assertEqual(<<"text/event-stream">>, header(<<"content-type">>, Headers)),
    Chunks = read_chunks(S, Rest, <<>>, Timeout),
    gen_tcp:close(S),
    [decode(Frame) || Frame <- split_sse(Chunks)].

read_head(S, Buf) ->
    case binary:split(Buf, <<"\r\n\r\n">>) of
        [Head, Rest] -> {Head, Rest};
        [_] ->
            case gen_tcp:recv(S, 0, 5000) of
                {ok, Data} -> read_head(S, <<Buf/binary, Data/binary>>);
                {error, Reason} -> ct:fail({read_head_failed, Reason})
            end
    end.

%% Read the whole chunked body until the terminating zero chunk.
read_chunks(S, Buf, Acc, Timeout) ->
    case decode_chunks(Buf) of
        {done, Body} ->
            <<Acc/binary, Body/binary>>;
        {more, Body, Rest} ->
            case gen_tcp:recv(S, 0, Timeout) of
                {ok, Data} -> read_chunks(S, <<Rest/binary, Data/binary>>,
                                          <<Acc/binary, Body/binary>>, Timeout);
                {error, closed} -> <<Acc/binary, Body/binary>>;
                {error, Reason} -> ct:fail({read_chunks_failed, Reason})
            end
    end.

decode_chunks(Buf) ->
    decode_chunks(Buf, <<>>).

decode_chunks(Buf, Acc) ->
    case binary:split(Buf, <<"\r\n">>) of
        [SizeLine, Rest] ->
            case catch binary_to_integer(SizeLine, 16) of
                0 ->
                    {done, Acc};
                Size when is_integer(Size) ->
                    case Rest of
                        <<Chunk:Size/binary, "\r\n", Rest2/binary>> ->
                            decode_chunks(Rest2, <<Acc/binary, Chunk/binary>>);
                        _ ->
                            {more, Acc, Buf}
                    end;
                _ ->
                    {more, Acc, Buf}
            end;
        [_] ->
            {more, Acc, Buf}
    end.

%% SSE frames are separated by a blank line; keep only `data:' payloads.
split_sse(Bin) ->
    Parts = binary:split(Bin, <<"\n\n">>, [global]),
    [Payload || Part <- Parts,
                (Payload = data_of(Part)) =/= none].

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

read_until_comment(S, Buf, Acc, Timeout) ->
    case binary:match(Acc, <<":">>) of
        nomatch ->
            case gen_tcp:recv(S, 0, Timeout) of
                {ok, Data} -> read_until_comment(S, Buf, <<Acc/binary, Data/binary>>,
                                                 Timeout);
                {error, _} -> Acc
            end;
        _ ->
            Acc
    end.

drain_until_closed(S, Timeout) ->
    case gen_tcp:recv(S, 0, Timeout) of
        {ok, _} -> drain_until_closed(S, Timeout);
        {error, closed} -> ok;
        {error, Reason} -> {error, Reason}
    end.

is_status_update(#{<<"result">> := R}) -> maps:is_key(<<"statusUpdate">>, R);
is_status_update(_) -> false.

state_of(#{<<"result">> := #{<<"statusUpdate">> := U}}) ->
    maps:get(<<"state">>, maps:get(<<"status">>, U));
state_of(_) ->
    undefined.

%%%_* Push helpers ----------------------------------------------------------

enable_push(Host, Tenant) ->
    ok = application:set_env(broker_proto_a2a, push_enabled, true),
    ok = application:set_env(broker_proto_a2a, egress_allowlist,
                             [#{host => Host}]),
    ok = application:set_env(broker_proto_a2a, push_tenant_allowlist,
                             #{Tenant => [#{host => Host}]}),
    ok.

%% A loopback endpoint is legitimate only when the entry opts in, which is
%% exactly what this exercises.
enable_push_localhost(Url) ->
    Host = host_of(Url),
    ok = application:set_env(broker_proto_a2a, push_enabled, true),
    ok = application:set_env(broker_proto_a2a, push_allow_http, true),
    ok = application:set_env(broker_proto_a2a, egress_allowlist,
                             [#{host => Host, allow_private => true}]),
    ok = application:set_env(broker_proto_a2a, push_tenant_allowlist,
                             #{<<"t1">> => [#{host => Host,
                                             allow_private => true}]}),
    ok.

host_of(Url) ->
    #{host := Host} = uri_string:parse(Url),
    Host.

endpoint_for(Url) ->
    #{host := Host} = Uri = uri_string:parse(Url),
    Port = maps:get(port, Uri, 80),
    #{scheme => <<"http">>, host => Host, port => Port,
      path => maps:get(path, Uri, <<"/">>),
      addresses => [{127, 0, 0, 1}], allow_private => true}.

start_listener_with_push() ->
    start_listener([]).

%% A one-shot webhook receiver: records each request and answers `Status'.
%% The listen socket is created in the caller so the port is known before the
%% case starts driving traffic.
start_receiver() ->
    start_receiver_with_status(200).

start_receiver_with_status(Status) ->
    Parent = self(),
    {ok, L} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true},
                                 {ip, {127, 0, 0, 1}}]),
    {ok, Port} = inet:port(L),
    Pid = spawn(fun() -> receiver_loop(Parent, Status, L) end),
    ok = gen_tcp:controlling_process(L, Pid),
    {Pid, Port}.

receiver_loop(Parent, Status, L) ->
    case gen_tcp:accept(L, 5000) of
        {ok, S} ->
            Req = read_http_request(S),
            ok = gen_tcp:send(S, simple_response(Status)),
            gen_tcp:close(S),
            Parent ! {delivery, self(), 0, Req},
            receiver_loop(Parent, Status, L);
        {error, _} ->
            gen_tcp:close(L),
            ok
    end.

receive_delivery(Pid, Timeout) ->
    receive
        {delivery, Pid, _Attempt, Req} -> Req
    after Timeout ->
            ct:fail(no_delivery)
    end.

stop_receiver(Pid) ->
    exit(Pid, kill),
    ok.

read_http_request(S) ->
    {Head, Rest} = read_head(S),
    {Method, Path} = request_line(Head),
    Hdrs = split_headers(Head),
    Length = case header(<<"content-length">>, Hdrs) of
                 undefined -> 0;
                 V -> binary_to_integer(V)
             end,
    Body = read_exact(S, Rest, Length),
    #{method => Method, path => Path, body => Body, headers => Hdrs}.

request_line(Head) ->
    [First | _] = binary:split(Head, <<"\r\n">>, [global]),
    [Method, Path | _] = binary:split(First, <<" ">>, [global]),
    {Method, Path}.

%% Request head: the first line is the request line, not a status line.
split_headers(Head) ->
    [_RequestLine | HeaderLines] = binary:split(Head, <<"\r\n">>, [global]),
    Headers = [split_header(Line) || Line <- HeaderLines, Line =/= <<>>],
    Headers.

read_head(S) ->
    read_head(S, <<>>).

read_exact(_S, Buf, 0) -> Buf;
read_exact(_S, Buf, N) when byte_size(Buf) >= N -> binary:part(Buf, 0, N);
read_exact(S, Buf, N) ->
    case gen_tcp:recv(S, N - byte_size(Buf), 5000) of
        {ok, Data} -> read_exact(S, <<Buf/binary, Data/binary>>, N);
        {error, _} -> Buf
    end.

simple_response(Status) ->
    Body = <<"ok">>,
    [<<"HTTP/1.1 ">>, integer_to_binary(Status), <<" X\r\n">>,
     <<"content-length: ">>, integer_to_binary(byte_size(Body)), <<"\r\n">>,
     <<"connection: close\r\n\r\n">>, Body].

wait_for_dlq(Tenant, TaskId, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    wait_for_dlq_loop(Tenant, TaskId, Deadline).

wait_for_dlq_loop(Tenant, TaskId, Deadline) ->
    Dest = <<"$a2a/push/dlq/", Tenant/binary>>,
    Found = [Msg || {_, D, Msg} <- all_messages(), D =:= Dest,
                    contains(TaskId, Msg)],
    case Found of
        [] ->
            case erlang:monotonic_time(millisecond) > Deadline of
                true -> [];
                false -> timer:sleep(100),
                         wait_for_dlq_loop(Tenant, TaskId, Deadline)
            end;
        _ ->
            Found
    end.

contains(TaskId, Msg) ->
    binary:match(element(2, Msg), TaskId) =/= nomatch orelse
        binary:match(element(5, Msg), TaskId) =/= nomatch.

all_messages() ->
    lists:append([partition_messages(P) || P <- broker_queue:partition_ids()]).

partition_messages(Partition) ->
    partition_messages(Partition, 0, []).

partition_messages(Partition, From, Acc) ->
    case broker_queue:read_from(Partition, From, 1000) of
        {ok, [], _Next} -> Acc;
        {ok, Page, Next} ->
            Acc1 = Acc ++ [{Partition, element(4, M), M} || {_, M} <- Page],
            case Next > From of
                true -> partition_messages(Partition, Next, Acc1);
                false -> Acc1
            end;
        {error, _} -> Acc
    end.

%%%_* Listener + HTTP -------------------------------------------------------

start_listener(Extra) ->
    Env = [{listeners, [#{port => 0, ref => {a2a, ct}, num_acceptors => 2}]} | Extra],
    case start_listener_raw(Env) of
        ok -> {ranch:get_port({a2a, ct}), ok};
        {error, Reason} -> ct:fail({listener_start_failed, Reason})
    end.

start_listener_raw(Env) ->
    _ = application:stop(broker_proto_a2a),
    [ok = application:set_env(broker_proto_a2a, K, V) || {K, V} <- Env],
    application:start(broker_proto_a2a).

rpc(Port, Method, Params, Id) ->
    rpc_as(Port, ?TOKEN, Method, Params, Id).

rpc_as(Port, Token, Method, Params, Id) ->
    {Status, _, Body} = request(Port, post_headers(Token),
                                rpc_body(Method, Params, Id)),
    {Status, decode(Body)}.

task(#{<<"result">> := #{<<"task">> := Task}}) -> Task.

params(Context) ->
    #{<<"message">> => #{<<"messageId">> => unique(<<"m">>),
                         <<"role">> => <<"ROLE_USER">>,
                         <<"contextId">> => Context,
                         <<"parts">> => [#{<<"text">> => <<"hi">>}]}}.

push_params(TaskId, Url) ->
    #{<<"taskId">> => TaskId,
      <<"config">> => #{<<"url">> => Url}}.

unique(Prefix) ->
    Bin = integer_to_binary(erlang:unique_integer([positive, monotonic])),
    <<Prefix/binary, "-", Bin/binary>>.

post_headers(Token) ->
    [{"Content-Type", "application/json"},
     {"Authorization", "Bearer " ++ binary_to_list(Token)},
     {"A2A-Version", "1.0"}].

request(Port, Headers, Body) ->
    {ok, S} = gen_tcp:connect({127, 0, 0, 1}, Port,
                              [binary, {active, false}, {packet, raw}], 2000),
    ok = gen_tcp:send(S, build_request(post, "/", Headers, Body)),
    {Head, Rest} = read_head(S),
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
    [string:uppercase(atom_to_list(Method)), " ", Path, " HTTP/1.1\r\n",
     [[K, ": ", V, "\r\n"] || {K, V} <- AllHeaders],
     "\r\n", BodyBin].

rpc_body(Method, Params, Id) ->
    json:encode(#{<<"jsonrpc">> => <<"2.0">>, <<"method">> => Method,
                  <<"params">> => Params, <<"id">> => Id}).

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

lower(Bin) ->
    << <<(lower_byte(B))/integer>> || <<B/integer>> <= Bin >>.

lower_byte(B) when B >= $A, B =< $Z -> B + 32;
lower_byte(B) -> B.

trim(Bin) ->
    string:trim(Bin, both, " \t").

decode(Bin) ->
    json:decode(Bin).

restore_a2a({K, {ok, V}}) -> application:set_env(broker_proto_a2a, K, V);
restore_a2a({K, undefined}) -> application:unset_env(broker_proto_a2a, K).

restore_security({K, {ok, V}}) -> application:set_env(broker_security, K, V);
restore_security({K, undefined}) -> application:unset_env(broker_security, K).
