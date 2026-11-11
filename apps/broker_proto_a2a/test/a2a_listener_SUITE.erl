%% @doc A2A front end over a real listener, with the real task log.
%%
%% Every case drives the same path a third-party client will: a raw HTTP/1.1
%% request over `gen_tcp'. The listener binds `port => 0' -- the fixed 8081 is a
%% deployment choice, never a test fixture (and is not ours to occupy).
%%
%% S9-B attaches the task registry, so the fixture also starts the storage stack
%% (WAL in a private temp dir, four partitions) and creates the partitions before
%% `broker_queue' boots. Tasks therefore survive `application:stop/1' and boot
%% replay is exercised by the suite itself.
-module(a2a_listener_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("broker_core/include/broker_message.hrl").

-define(TOKEN, <<"alice-token">>).
-define(TOKEN2, <<"bob-token">>).
-define(PARTITIONS, 4).

all() ->
    [send_message_creates_task,
     get_task_roundtrip,
     get_task_unknown_is_404,
     list_tasks_filters_by_context,
     list_tasks_paginates,
     cancel_non_terminal_task,
     cancel_completed_task_is_not_cancelable,
     cross_tenant_task_is_not_found,
     task_events_are_in_the_shared_log,
     task_survives_restart_replay,
     orphan_delta_becomes_history_lost,
     well_known_anonymous_serves_public_card,
     well_known_authenticated_returns_extended_card,
     well_known_etag_304,
     well_known_unknown_agent_is_404,
     catalog_filters_by_tag_and_paginates,
     catalog_is_tenant_scoped,
     get_extended_agent_card,
     version_missing_is_read_as_0_3,
     version_unsupported,
     no_credential_401,
     bad_credential_401,
     acl_denied_403,
     unknown_method,
     parse_error,
     invalid_params,
     payload_too_large_413,
     unsupported_media_type_415,
     keep_alive_two_requests,
     connection_close_honored,
     health,
     anonymous_listener_on_loopback_is_allowed,
     anonymous_listener_on_wide_bind_is_refused].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(broker_security),
    {ok, _} = application:ensure_all_started(broker_proto_agent_rpc),
    DataDir = filename:join(?config(priv_dir, Config), "a2a-data"),
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
                    agent_cards, agent_card_max_age]],
    OldSec = [{K, application:get_env(broker_security, K)}
              || K <- [users, acl]],
    ok = application:set_env(broker_security, users,
                             [{<<"alice">>, #{token => ?TOKEN,
                                              tenant_id => <<"t1">>}},
                              {<<"bob">>, #{token => ?TOKEN2,
                                            tenant_id => <<"t2">>}}]),
    ok = application:set_env(broker_security, acl, allow),
    ok = application:set_env(broker_proto_a2a, listeners, []),
    ok = application:set_env(broker_proto_a2a, require_auth, true),
    ok = application:set_env(broker_proto_a2a, max_body_bytes, 1048576),
    ok = application:set_env(broker_proto_a2a, agent_card_max_age, 60),
    ok = application:set_env(broker_proto_a2a, agent_cards, agent_card_specs()),
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

end_per_testcase(_Case, _Config) ->
    _ = application:stop(broker_proto_a2a),
    ok = application:set_env(broker_proto_a2a, listeners, []),
    ok = application:set_env(broker_proto_a2a, require_auth, true),
    ok = application:set_env(broker_proto_a2a, max_body_bytes, 1048576),
    ok = application:set_env(broker_proto_a2a, task_auto_complete, true),
    ok = application:set_env(broker_security, acl, allow),
    ok.

%%%_* Task lifecycle --------------------------------------------------------

send_message_creates_task(_Config) ->
    {Port, _} = start_listener([]),
    Context = unique(<<"ctx-send">>),
    {200, #{<<"result">> := #{<<"task">> := Task}}} =
        rpc(Port, <<"SendMessage">>, params(Context), 1),
    ?assertEqual(Context, maps:get(<<"contextId">>, Task)),
    Status = maps:get(<<"status">>, Task),
    ?assertEqual(<<"TASK_STATE_COMPLETED">>, maps:get(<<"state">>, Status)),
    %% The echo agent answered: user message + agent message.
    History = maps:get(<<"history">>, Task),
    ?assertEqual(2, length(History)),
    Roles = [maps:get(<<"role">>, M) || M <- History],
    ?assertEqual([<<"ROLE_USER">>, <<"ROLE_AGENT">>], Roles),
    ?assert(is_binary(maps:get(<<"id">>, Task))).

get_task_roundtrip(_Config) ->
    {Port, _} = start_listener([]),
    Context = unique(<<"ctx-get">>),
    {200, Sent} = rpc(Port, <<"SendMessage">>, params(Context), 1),
    Task = task(Sent),
    TaskId = maps:get(<<"id">>, Task),
    {200, #{<<"result">> := Fetched}} =
        rpc(Port, <<"GetTask">>, #{<<"id">> => TaskId}, 2),
    ?assertEqual(Task, Fetched).

get_task_unknown_is_404(_Config) ->
    {Port, _} = start_listener([]),
    {404, Resp} = rpc(Port, <<"GetTask">>, #{<<"id">> => <<"nope">>}, 1),
    ?assertMatch(#{<<"error">> := #{<<"code">> := -32001}}, Resp).

list_tasks_filters_by_context(_Config) ->
    {Port, _} = start_listener([]),
    ContextA = unique(<<"ctx-a">>),
    ContextB = unique(<<"ctx-b">>),
    [send(Port, ContextA, Id) || Id <- lists:seq(1, 2)],
    [send(Port, ContextB, Id) || Id <- lists:seq(3, 3)],
    {200, #{<<"result">> := #{<<"tasks">> := Tasks}}} =
        rpc(Port, <<"ListTasks">>, #{<<"contextId">> => ContextA}, 1),
    ?assertEqual(2, length(Tasks)),
    ?assert(lists:all(fun(T) ->
                              maps:get(<<"contextId">>, T) =:= ContextA
                      end, Tasks)),
    %% All of them are terminal in this fixture.
    ?assert(lists:all(fun(T) ->
                              maps:get(<<"state">>, maps:get(<<"status">>, T))
                                  =:= <<"TASK_STATE_COMPLETED">>
                      end, Tasks)).

list_tasks_paginates(_Config) ->
    {Port, _} = start_listener([]),
    Context = unique(<<"ctx-page">>),
    [send(Port, Context, Id) || Id <- lists:seq(1, 3)],
    {200, #{<<"result">> := Page1}} =
        rpc(Port, <<"ListTasks">>,
            #{<<"contextId">> => Context, <<"pageSize">> => 2}, 1),
    ?assertEqual(2, length(maps:get(<<"tasks">>, Page1))),
    Token = maps:get(<<"nextPageToken">>, Page1),
    ?assert(is_binary(Token)),
    {200, #{<<"result">> := Page2}} =
        rpc(Port, <<"ListTasks">>,
            #{<<"contextId">> => Context, <<"pageSize">> => 2,
              <<"pageToken">> => Token}, 2),
    ?assertEqual(1, length(maps:get(<<"tasks">>, Page2))),
    ?assertEqual(undefined, maps:get(<<"nextPageToken">>, Page2, undefined)).

cancel_non_terminal_task(_Config) ->
    %% An async executor leaves the task SUBMITTED, which is what makes
    %% cancellation observable.
    ok = application:set_env(broker_proto_a2a, task_auto_complete, false),
    {Port, _} = start_listener([]),
    {200, Sent} = rpc(Port, <<"SendMessage">>, params(unique(<<"ctx-cancel">>)), 1),
    TaskId = maps:get(<<"id">>, task(Sent)),
    {200, #{<<"result">> := Canceled}} =
        rpc(Port, <<"CancelTask">>, #{<<"id">> => TaskId}, 2),
    ?assertEqual(<<"TASK_STATE_CANCELED">>,
                 maps:get(<<"state">>, maps:get(<<"status">>, Canceled))),
    %% A terminal task cannot be canceled again.
    {400, Resp} = rpc(Port, <<"CancelTask">>, #{<<"id">> => TaskId}, 3),
    ?assertMatch(#{<<"error">> := #{<<"code">> := -32002}}, Resp).

cancel_completed_task_is_not_cancelable(_Config) ->
    {Port, _} = start_listener([]),
    {200, Sent} = rpc(Port, <<"SendMessage">>, params(unique(<<"ctx-done">>)), 1),
    TaskId = maps:get(<<"id">>, task(Sent)),
    {400, Resp} = rpc(Port, <<"CancelTask">>, #{<<"id">> => TaskId}, 2),
    ?assertMatch(#{<<"error">> := #{<<"code">> := -32002}}, Resp).

cross_tenant_task_is_not_found(_Config) ->
    {Port, _} = start_listener([]),
    {200, Sent} = rpc(Port, <<"SendMessage">>, params(unique(<<"ctx-tenant">>)), 1),
    TaskId = maps:get(<<"id">>, task(Sent)),
    %% Alice's task does not exist for Bob: same 404 as a made-up id.
    {404, Resp} = rpc_as(Port, ?TOKEN2, <<"GetTask">>, #{<<"id">> => TaskId}, 2),
    ?assertMatch(#{<<"error">> := #{<<"code">> := -32001}}, Resp).

%%%_* Log mapping and recovery ----------------------------------------------

task_events_are_in_the_shared_log(_Config) ->
    {Port, _} = start_listener([]),
    {200, Sent} = rpc(Port, <<"SendMessage">>, params(unique(<<"ctx-log">>)), 1),
    TaskId = maps:get(<<"id">>, task(Sent)),
    Events = task_events(TaskId),
    %% creation, WORKING, agent message, COMPLETED -- in seq order.
    ?assertEqual([1, 2, 3, 4], [maps:get(seq, E) || E <- Events]),
    ?assertEqual([task, status_update, message, status_update],
                 [maps:get(kind, E) || E <- Events]),
    %% Every event is an ordinary durable message on the A2A destination.
    ?assertEqual(<<"$a2a/task/", TaskId/binary>>, destination(TaskId)).

task_survives_restart_replay(_Config) ->
    {Port, _} = start_listener([]),
    Context = unique(<<"ctx-replay">>),
    {200, Sent} = rpc(Port, <<"SendMessage">>, params(Context), 1),
    Task = task(Sent),
    TaskId = maps:get(<<"id">>, Task),
    %% Stop the whole adapter: ETS projections die, the WAL does not.
    ok = application:stop(broker_proto_a2a),
    {Port2, _} = start_listener([]),
    {200, #{<<"result">> := Replayed}} =
        rpc(Port2, <<"GetTask">>, #{<<"id">> => TaskId}, 2),
    ?assertEqual(Task, Replayed).

orphan_delta_becomes_history_lost(_Config) ->
    {Port, _} = start_listener([]),
    TaskId = unique(<<"orphan">>),
    %% A delta with no creation snapshot: exactly what retention GC looks like.
    {ok, _} = a2a_event:append(
                TaskId, <<"t1">>, status_update, 7,
                #{<<"statusUpdate">> =>
                      #{<<"taskId">> => TaskId,
                        <<"status">> =>
                            #{<<"state">> => <<"TASK_STATE_WORKING">>,
                              <<"timestamp">> => a2a_event:timestamp()},
                        <<"final">> => false}}),
    ok = application:stop(broker_proto_a2a),
    {Port2, _} = start_listener([]),
    {200, #{<<"result">> := Task}} =
        rpc(Port2, <<"GetTask">>, #{<<"id">> => TaskId}, 1),
    ?assertEqual(<<"TASK_STATE_FAILED">>,
                 maps:get(<<"state">>, maps:get(<<"status">>, Task))),
    ?assertEqual(#{<<"reason">> => <<"history_lost">>},
                 maps:get(<<"metadata">>, Task)),
    %% The repair is idempotent: a second replay must not append another event.
    Count1 = length(task_events(TaskId)),
    ok = application:stop(broker_proto_a2a),
    {Port3, _} = start_listener([]),
    {200, #{<<"result">> := Again}} =
        rpc(Port3, <<"GetTask">>, #{<<"id">> => TaskId}, 2),
    ?assertEqual(<<"TASK_STATE_FAILED">>,
                 maps:get(<<"state">>, maps:get(<<"status">>, Again))),
    ?assertEqual(Count1, length(task_events(TaskId))),
    _ = Port.

%%%_* Agent Card discovery --------------------------------------------------

well_known_anonymous_serves_public_card(_Config) ->
    {Port, _} = start_listener([]),
    %% Anonymous is `broker_security:default_tenant()' (t0), so the well-known
    %% path is public *within one tenant*, not across all of them.
    {200, Headers, Body} = get(Port, <<"/.well-known/agent-card.json">>, []),
    ?assertEqual(<<"application/json">>, header(<<"content-type">>, Headers)),
    ?assertEqual(<<"max-age=60">>, header(<<"cache-control">>, Headers)),
    ?assert(is_binary(header(<<"etag">>, Headers))),
    Card = decode(Body),
    ?assertEqual(<<"Anonymous Echo">>, maps:get(<<"name">>, Card)),
    %% Anonymous callers never see an extended variant.
    ?assertNot(maps:is_key(<<"_internal">>, Card)),
    %% And a t1 card is invisible: no credential must not mean all tenants.
    {404, _, _} =
        get(Port, <<"/.well-known/agent-card.json?agent=echo-public">>, []).

well_known_authenticated_returns_extended_card(_Config) ->
    {Port, _} = start_listener([]),
    {200, _, Body} =
        get(Port, <<"/.well-known/agent-card.json?agent=echo-public">>,
            auth(?TOKEN)),
    Card = decode(Body),
    ?assertEqual(<<"Echo Agent (extended)">>, maps:get(<<"name">>, Card)),
    ?assertEqual(<<"internal-note">>, maps:get(<<"_internal">>, Card)).

well_known_etag_304(_Config) ->
    {Port, _} = start_listener([]),
    Path = <<"/.well-known/agent-card.json?agent=echo-public">>,
    {200, H1, _} = get(Port, Path, auth(?TOKEN)),
    ETag = header(<<"etag">>, H1),
    ?assert(is_binary(ETag)),
    {304, H2, Body} =
        request(Port, [{"If-None-Match", binary_to_list(ETag)} | auth(?TOKEN)],
                <<>>, get, Path),
    ?assertEqual(ETag, header(<<"etag">>, H2)),
    ?assertEqual(<<>>, Body),
    %% A stale validator re-reads the card body.
    {200, H3, _} = request(Port, [{"If-None-Match", "\"0-stale\""} | auth(?TOKEN)],
                           <<>>, get, Path),
    ?assertEqual(ETag, header(<<"etag">>, H3)).

well_known_unknown_agent_is_404(_Config) ->
    {Port, _} = start_listener([]),
    {404, _, Body} =
        get(Port, <<"/.well-known/agent-card.json?agent=nope">>, auth(?TOKEN)),
    ?assertMatch(#{<<"error">> := <<"agent_card_not_found">>}, decode(Body)).

catalog_filters_by_tag_and_paginates(_Config) ->
    {Port, _} = start_listener([]),
    %% t1 has two agents: echo-public (tag chat) and text-agent (tag nlp).
    {200, _, TagBody} = get(Port, <<"/agents?tag=nlp">>, auth(?TOKEN)),
    #{<<"agents">> := Tagged} = decode(TagBody),
    ?assertEqual([<<"Text Agent">>], [maps:get(<<"name">>, A) || A <- Tagged]),
    {200, _, CapBody} = get(Port, <<"/agents?capability=streaming">>, auth(?TOKEN)),
    #{<<"agents">> := Capable} = decode(CapBody),
    ?assertEqual([<<"Echo Agent">>], [maps:get(<<"name">>, A) || A <- Capable]),
    %% One per page, with a stable token.
    {200, _, Page1Body} = get(Port, <<"/agents?pageSize=1">>, auth(?TOKEN)),
    #{<<"agents">> := Page1, <<"nextPageToken">> := Token} = decode(Page1Body),
    ?assertEqual(1, length(Page1)),
    {200, _, Page2Body} =
        get(Port, <<"/agents?pageSize=1&pageToken=", Token/binary>>, auth(?TOKEN)),
    #{<<"agents">> := Page2} = decode(Page2Body),
    ?assertEqual(1, length(Page2)),
    ?assertNotEqual(maps:get(<<"name">>, hd(Page1)),
                    maps:get(<<"name">>, hd(Page2))).

catalog_is_tenant_scoped(_Config) ->
    {Port, _} = start_listener([]),
    {200, _, AliceBody} = get(Port, <<"/agents">>, auth(?TOKEN)),
    AliceNames = [maps:get(<<"name">>, A) || A <- maps:get(<<"agents">>, decode(AliceBody))],
    ?assert(lists:member(<<"Echo Agent">>, AliceNames)),
    ?assertNot(lists:member(<<"Other Tenant">>, AliceNames)),
    {200, _, BobBody} = get(Port, <<"/agents">>, auth(?TOKEN2)),
    BobNames = [maps:get(<<"name">>, A) || A <- maps:get(<<"agents">>, decode(BobBody))],
    ?assertEqual([<<"Other Tenant">>], BobNames),
    %% No credential means the default tenant, exactly like the well-known path.
    {200, _, AnonBody} = get(Port, <<"/agents">>, []),
    AnonNames = [maps:get(<<"name">>, A) || A <- maps:get(<<"agents">>, decode(AnonBody))],
    ?assertEqual([<<"Anonymous Echo">>], AnonNames).

get_extended_agent_card(_Config) ->
    {Port, _} = start_listener([]),
    {200, #{<<"result">> := Extended}} =
        rpc(Port, <<"GetExtendedAgentCard">>,
            #{<<"agent">> => <<"echo-public">>}, 1),
    ?assertEqual(<<"Echo Agent (extended)">>, maps:get(<<"name">>, Extended)),
    %% An agent without an extended variant answers -32007.
    {400, Resp} =
        rpc(Port, <<"GetExtendedAgentCard">>,
            #{<<"agent">> => <<"text-agent">>}, 2),
    ?assertMatch(#{<<"error">> := #{<<"code">> := -32007}}, Resp).

%%%_* Transport gates -------------------------------------------------------

version_missing_is_read_as_0_3(_Config) ->
    %% The spec reads a missing header as 0.3; this server offers 1.0 only, so
    %% the honest answer is `-32009', not a 1.0-shaped response to a client that
    %% thinks it is speaking 0.3.
    {Port, _} = start_listener([]),
    Headers = [{"Content-Type", "application/json"},
               {"Authorization", "Bearer " ++ binary_to_list(?TOKEN)}],
    {400, _, Body} = request(Port, Headers,
                             rpc_body(<<"SendMessage">>, params(unique(<<"ctx">>)), 1)),
    ?assertMatch(#{<<"error">> := #{<<"code">> := -32009}}, decode(Body)).

version_unsupported(_Config) ->
    {Port, _} = start_listener([]),
    Headers = [{"Content-Type", "application/json"},
               {"Authorization", "Bearer " ++ binary_to_list(?TOKEN)},
               {"A2A-Version", "2.0"}],
    {400, _, Body} = request(Port, Headers,
                             rpc_body(<<"SendMessage">>, params(unique(<<"ctx">>)), 1)),
    Resp = decode(Body),
    ?assertMatch(#{<<"error">> := #{<<"code">> := -32009}}, Resp),
    ?assertEqual(null, maps:get(<<"id">>, Resp)).

no_credential_401(_Config) ->
    {Port, _} = start_listener([]),
    {401, Headers, _} = request(Port, [{"Content-Type", "application/json"}],
                                rpc_body(<<"SendMessage">>, params(unique(<<"ctx">>)), 1)),
    ?assertEqual(<<"Bearer realm=\"a2a\"">>, header(<<"www-authenticate">>, Headers)).

bad_credential_401(_Config) ->
    {Port, _} = start_listener([]),
    {401, Headers, Body} = request(Port, post_headers(<<"wrong">>),
                                   rpc_body(<<"SendMessage">>, params(unique(<<"ctx">>)), 1)),
    ?assertEqual(<<"Bearer realm=\"a2a\"">>, header(<<"www-authenticate">>, Headers)),
    %% The body says nothing about whether the user exists or the secret is
    %% wrong.
    ?assertEqual(<<>>, Body).

acl_denied_403(_Config) ->
    ok = application:set_env(broker_security, acl,
                             [#{effect => deny, principal => <<"*">>,
                                action => <<"*">>, pattern => <<"$a2a/#">>}]),
    {Port, _} = start_listener([]),
    {403, _, <<>>} = request(Port, post_headers(?TOKEN),
                             rpc_body(<<"SendMessage">>, params(unique(<<"ctx">>)), 1)).

unknown_method(_Config) ->
    {Port, _} = start_listener([]),
    {200, _, Body} = request(Port, post_headers(?TOKEN),
                             rpc_body(<<"GetAgentCard">>, #{}, 2)),
    ?assertMatch(#{<<"error">> := #{<<"code">> := -32601}}, decode(Body)).

parse_error(_Config) ->
    {Port, _} = start_listener([]),
    {400, _, Body} = request(Port, post_headers(?TOKEN), <<"not json">>),
    Resp = decode(Body),
    ?assertMatch(#{<<"error">> := #{<<"code">> := -32700}, <<"id">> := null}, Resp).

invalid_params(_Config) ->
    {Port, _} = start_listener([]),
    Params = #{<<"message">> => #{<<"messageId">> => <<"m1">>,
                                  <<"role">> => <<"ROLE_USER">>}},
    {200, _, Body} = request(Port, post_headers(?TOKEN),
                             rpc_body(<<"SendMessage">>, Params, 3)),
    ?assertMatch(#{<<"error">> := #{<<"code">> := -32602}}, decode(Body)).

payload_too_large_413(_Config) ->
    {Port, _} = start_listener([{max_body_bytes, 32}]),
    Big = binary:copy(<<"x">>, 64),
    {413, _, _} = request(Port, post_headers(?TOKEN), Big).

unsupported_media_type_415(_Config) ->
    {Port, _} = start_listener([]),
    {415, _, _} = request(Port,
                          [{"Content-Type", "text/plain"},
                           {"Authorization", "Bearer " ++ binary_to_list(?TOKEN)}],
                          rpc_body(<<"SendMessage">>, params(unique(<<"ctx">>)), 1)).

keep_alive_two_requests(_Config) ->
    {Port, _} = start_listener([]),
    {ok, S} = gen_tcp:connect({127, 0, 0, 1}, Port,
                              [binary, {active, false}, {packet, raw}], 2000),
    Body = rpc_body(<<"SendMessage">>, params(unique(<<"ctx-ka">>)), 1),
    ok = gen_tcp:send(S, build_request(post_headers(?TOKEN), Body)),
    {200, _, _} = read_response(S),
    ok = gen_tcp:send(S, build_request(post_headers(?TOKEN), Body)),
    {200, _, _} = read_response(S),
    gen_tcp:close(S).

connection_close_honored(_Config) ->
    {Port, _} = start_listener([]),
    {ok, S} = gen_tcp:connect({127, 0, 0, 1}, Port,
                              [binary, {active, false}, {packet, raw}], 2000),
    Headers = [{"Connection", "close"} | post_headers(?TOKEN)],
    ok = gen_tcp:send(S, build_request(Headers,
                                       rpc_body(<<"SendMessage">>,
                                                params(unique(<<"ctx-close">>)), 1))),
    {200, RespHeaders, _} = read_response(S),
    ?assertEqual(<<"close">>, header(<<"connection">>, RespHeaders)),
    ?assertEqual({error, closed}, gen_tcp:recv(S, 1, 2000)),
    gen_tcp:close(S).

health(_Config) ->
    {Port, _} = start_listener([]),
    {200, _, Body} = request(Port, [{"Host", "h"}], <<>>, get, "/health"),
    ?assertEqual(#{<<"status">> => <<"ok">>}, decode(Body)).

anonymous_listener_on_loopback_is_allowed(_Config) ->
    {_Port, ok} = start_listener([{require_auth, false}]),
    ok.

anonymous_listener_on_wide_bind_is_refused(_Config) ->
    Env = [{require_auth, false},
           {listeners, [#{port => 0, ref => {a2a, ct}, ip => {0, 0, 0, 0}}]}],
    ?assertMatch({error, {{insecure_listener, _, {0, 0, 0, 0}}, _}},
                 start_listener_raw(Env)).

%%%_* Helpers ---------------------------------------------------------------

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

send(Port, Context, Id) ->
    {200, Resp} = rpc(Port, <<"SendMessage">>, params(Context), Id),
    task(Resp).

task(#{<<"result">> := #{<<"task">> := Task}}) -> Task.

task_events(TaskId) ->
    Dest = <<"$a2a/task/", TaskId/binary>>,
    Messages = lists:append(
                 [partition_messages(P) || P <- broker_queue:partition_ids()]),
    Events = [Decoded
              || {_, Destination, Payload} <- Messages,
                 Destination =:= Dest,
                 {ok, Decoded} <- [a2a_event:decode(Payload)]],
    lists:sort(fun(A, B) -> maps:get(seq, A) =< maps:get(seq, B) end, Events).

partition_messages(Partition) ->
    case broker_queue:read_from(Partition, 0, 10000) of
        {ok, Page, _Next} ->
            [{Offset, Msg#broker_message.destination, Msg#broker_message.payload}
             || {Offset, Msg} <- Page];
        {error, _} ->
            []
    end.

destination(TaskId) ->
    <<"$a2a/task/", TaskId/binary>>.

rpc_body(Method, Params, Id) ->
    json:encode(#{<<"jsonrpc">> => <<"2.0">>,
                  <<"id">> => Id,
                  <<"method">> => Method,
                  <<"params">> => Params}).

params(Context) ->
    #{<<"message">> => #{<<"messageId">> => unique(<<"m">>),
                         <<"role">> => <<"ROLE_USER">>,
                         <<"contextId">> => Context,
                         <<"parts">> => [#{<<"text">> => <<"hi">>}]}}.

unique(Prefix) ->
    Bin = integer_to_binary(erlang:unique_integer([positive, monotonic])),
    <<Prefix/binary, "-", Bin/binary>>.

%% @doc Seed cards for the Agent Card discovery cases.
%%
%% Every card carries the required fields of the convergence report §5 / R-D1:
%% `name', `description', `supportedInterfaces', `version', `capabilities',
%% `defaultInputModes', `defaultOutputModes', `skills'. The four rows are
%% deliberately distinguishable: `echo-public' has an extended variant, and
%% only `echo-legacy' advertises `streaming', so the capability filter has
%% exactly one answer. `text-agent' is the `nlp' skill, and the t2 row proves
%% the catalog is tenant-scoped.
agent_card_specs() ->
    [#{tenant_id => <<"t0">>, agent_id => <<"anon-echo">>, status => active,
       card => card(<<"Anonymous Echo">>, false, <<"chat">>)},
     #{tenant_id => <<"t1">>, agent_id => <<"echo-public">>, status => active,
       card => card(<<"Public Echo">>, false, <<"chat">>),
       extended_card =>
           (card(<<"Echo Agent (extended)">>, false, <<"chat">>))
           #{<<"_internal">> => <<"internal-note">>}},
     #{tenant_id => <<"t1">>, agent_id => <<"echo-legacy">>, status => active,
       card => card(<<"Echo Agent">>, true, <<"chat">>)},
     #{tenant_id => <<"t1">>, agent_id => <<"text-agent">>, status => active,
       card => card(<<"Text Agent">>, false, <<"nlp">>)},
     #{tenant_id => <<"t2">>, agent_id => <<"other">>, status => active,
       card => card(<<"Other Tenant">>, false, <<"chat">>)}].

card(Name, Streaming, Skill) ->
    #{<<"name">> => Name,
      <<"description">> => <<"agent card for tests">>,
      <<"supportedInterfaces">> =>
          [#{<<"url">> => <<"https://example.test/a2a">>,
             <<"protocolBinding">> => <<"JSONRPC">>}],
      <<"version">> => <<"1.0.0">>,
      <<"capabilities">> => #{<<"streaming">> => Streaming},
      <<"defaultInputModes">> => [<<"text">>],
      <<"defaultOutputModes">> => [<<"text">>],
      <<"skills">> => [#{<<"id">> => Skill, <<"name">> => Skill,
                        <<"tags">> => [Skill]}]}.

%%%_* Discovery helpers -----------------------------------------------------

auth(Token) ->
    [{"Authorization", "Bearer " ++ binary_to_list(Token)}].

get(Port, Path, Headers) ->
    request(Port, Headers, <<>>, get, Path).

post_headers(Token) ->
    [{"Content-Type", "application/json"},
     {"Authorization", "Bearer " ++ binary_to_list(Token)},
     {"A2A-Version", "1.0"}].

request(Port, Headers, Body) ->
    request(Port, Headers, Body, post, <<"/">>).

request(Port, Headers, Body, Method, Path) ->
    {ok, S} = gen_tcp:connect({127, 0, 0, 1}, Port,
                              [binary, {active, false}, {packet, raw}], 2000),
    ok = gen_tcp:send(S, build_request(Method, Path, Headers, Body)),
    Result = read_response(S),
    gen_tcp:close(S),
    Result.

build_request(Headers, Body) ->
    build_request(post, "/", Headers, Body).

build_request(Method, Path, Headers, Body) ->
    BodyBin = iolist_to_binary(Body),
    AllHeaders = [{"Host", "localhost"},
                  {"Content-Length", integer_to_list(byte_size(BodyBin))} | Headers],
    [string:uppercase(atom_to_list(Method)), " ", Path, " HTTP/1.1\r\n",
     [[K, ": ", V, "\r\n"] || {K, V} <- AllHeaders],
     "\r\n", BodyBin].

read_response(Sock) ->
    read_response(Sock, <<>>).

read_response(Sock, Buf) ->
    case binary:split(Buf, <<"\r\n\r\n">>) of
        [Head, Rest] ->
            {Status, Headers} = parse_head(Head),
            Length = case header(<<"content-length">>, Headers) of
                         undefined -> 0;
                         Value -> binary_to_integer(Value)
                     end,
            Body = read_body(Sock, Rest, Length),
            {Status, Headers, Body};
        [_] ->
            case gen_tcp:recv(Sock, 0, 5000) of
                {ok, Data} -> read_response(Sock, <<Buf/binary, Data/binary>>);
                {error, Reason} -> {error, Reason}
            end
    end.

read_body(_Sock, Buf, Length) when byte_size(Buf) >= Length ->
    binary:part(Buf, 0, Length);
read_body(Sock, Buf, Length) ->
    case gen_tcp:recv(Sock, Length - byte_size(Buf), 5000) of
        {ok, Data} -> read_body(Sock, <<Buf/binary, Data/binary>>, Length);
        {error, _Reason} -> Buf
    end.

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
