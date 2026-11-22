%% @doc One task shard, the per-task serialization point (ADR-025 §D2/§D3).
%%
%% A shard is a gen_server whose projection lives in its own ETS table; the log
%% (`a2a_event' through `broker_router') is the source of truth. The mutating
%% happy paths -- create's echo drive, a legal cancel, a legal multi-turn delta
%% -- append to that log and so need the router/queue stack; they stay covered
%% end-to-end by `a2a_task_SUITE'.
%%
%% This suite pins the shard's own logic that never touches the bus: the ETS
%% round-trip (restore/get/count), the `tasks/3' filter tree, the duplicate
%% guard, and the pre-append branches of the cancel/apply_event decision tree
%% (missing task, cross-tenant caller, terminal task). Those branches all
%% resolve before `emit/3' would route an event, so they are exact.
-module(a2a_task_shard_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

-define(TENANT, <<"tenant-a">>).
-define(OTHER, <<"tenant-b">>).

all() ->
    [restore_then_get_roundtrips_projection,
     count_reflects_number_of_live_tasks,
     get_unknown_task_is_not_found,
     tasks_lists_only_the_caller_tenants_tasks,
     tasks_filters_by_context_id,
     tasks_filters_by_status_list,
     tasks_treats_empty_status_list_as_no_filter,
     tasks_treats_missing_filters_as_no_filter,
     create_duplicate_task_is_rejected_before_touching_the_log,
     cancel_unknown_task_is_task_not_found,
     cancel_cross_tenant_caller_is_task_not_found,
     cancel_terminal_task_is_task_not_cancelable,
     apply_event_unknown_task_is_task_not_found,
     apply_event_cross_tenant_caller_is_task_not_found,
     apply_event_terminal_task_is_unsupported_operation].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

%% Each case gets a fresh shard so the count and listing assertions start from
%% an empty table. The shard registers under a fixed local name
%% (`a2a_task_shard_1'), so a case cannot run against a second instance while
%% the first is alive -- we kill the previous one and wait for its name to be
%% released before starting the next (exit/2 is async; the registration is
%% dropped only once the old process is actually gone).
init_per_testcase(_Case, Config) ->
    wait_for_name_free(a2a_task_shard:name(1), 50),
    {ok, Pid} = a2a_task_shard:start_link(1),
    %% start_link/1 links the shard to this test process. The shard owns only an
    %% ETS table and is killed explicitly in end_per_testcase, so drop the link:
    %% a shard that dies for any reason would otherwise take the test process
    %% down with it and abort the case.
    unlink(Pid),
    [{shard, Pid} | Config].

end_per_testcase(_Case, Config) ->
    exit(?config(shard, Config), kill),
    ok.

wait_for_name_free(Name, Retries) ->
    case whereis(Name) of
        undefined ->
            ok;
        _ when Retries =< 0 ->
            {error, name_still_registered};
        _ ->
            timer:sleep(20),
            wait_for_name_free(Name, Retries - 1)
    end.

%%%_* ETS round-trip ----------------------------------------------------------

restore_then_get_roundtrips_projection(Config) ->
    Shard = ?config(shard, Config),
    Proj = proj(<<"t-1">>, ?TENANT, <<"ctx-1">>, working),
    ?assertEqual(ok, a2a_task_shard:restore(Shard, Proj)),
    ?assertEqual({ok, Proj}, a2a_task_shard:get(Shard, <<"t-1">>)),
    ok.

count_reflects_number_of_live_tasks(Config) ->
    Shard = ?config(shard, Config),
    ?assertEqual(0, a2a_task_shard:count(Shard)),
    ok = a2a_task_shard:restore(Shard, proj(<<"t-1">>, ?TENANT, <<"ctx-1">>, working)),
    ok = a2a_task_shard:restore(Shard, proj(<<"t-2">>, ?TENANT, <<"ctx-1">>, working)),
    ?assertEqual(2, a2a_task_shard:count(Shard)),
    ok.

get_unknown_task_is_not_found(Config) ->
    Shard = ?config(shard, Config),
    ?assertEqual(not_found, a2a_task_shard:get(Shard, <<"nope">>)),
    ok.

%%%_* tasks/3 filter tree -----------------------------------------------------

tasks_lists_only_the_caller_tenants_tasks(Config) ->
    Shard = ?config(shard, Config),
    ok = a2a_task_shard:restore(Shard, proj(<<"t-1">>, ?TENANT, <<"ctx-1">>, working)),
    ok = a2a_task_shard:restore(Shard, proj(<<"t-2">>, ?TENANT, <<"ctx-2">>, working)),
    ok = a2a_task_shard:restore(Shard, proj(<<"t-3">>, ?OTHER, <<"ctx-3">>, working)),
    Got = ids(a2a_task_shard:tasks(Shard, ?TENANT, #{})),
    ?assertEqual([<<"t-1">>, <<"t-2">>], Got),
    ok.

tasks_filters_by_context_id(Config) ->
    Shard = ?config(shard, Config),
    ok = a2a_task_shard:restore(Shard, proj(<<"t-1">>, ?TENANT, <<"ctx-1">>, working)),
    ok = a2a_task_shard:restore(Shard, proj(<<"t-2">>, ?TENANT, <<"ctx-2">>, working)),
    Got = ids(a2a_task_shard:tasks(Shard, ?TENANT, #{context_id => <<"ctx-2">>})),
    ?assertEqual([<<"t-2">>], Got),
    ok.

tasks_filters_by_status_list(Config) ->
    Shard = ?config(shard, Config),
    ok = a2a_task_shard:restore(Shard, proj(<<"t-1">>, ?TENANT, <<"ctx-1">>, working)),
    ok = a2a_task_shard:restore(Shard, proj(<<"t-2">>, ?TENANT, <<"ctx-2">>, completed)),
    Got = ids(a2a_task_shard:tasks(Shard, ?TENANT, #{status => [completed]})),
    ?assertEqual([<<"t-2">>], Got),
    ok.

tasks_treats_empty_status_list_as_no_filter(Config) ->
    Shard = ?config(shard, Config),
    ok = a2a_task_shard:restore(Shard, proj(<<"t-1">>, ?TENANT, <<"ctx-1">>, working)),
    ok = a2a_task_shard:restore(Shard, proj(<<"t-2">>, ?TENANT, <<"ctx-2">>, completed)),
    %% An empty status list is "any status", not "no statuses".
    Got = ids(a2a_task_shard:tasks(Shard, ?TENANT, #{status => []})),
    ?assertEqual([<<"t-1">>, <<"t-2">>], Got),
    ok.

tasks_treats_missing_filters_as_no_filter(Config) ->
    Shard = ?config(shard, Config),
    ok = a2a_task_shard:restore(Shard, proj(<<"t-1">>, ?TENANT, <<"ctx-1">>, working)),
    ok = a2a_task_shard:restore(Shard, proj(<<"t-2">>, ?TENANT, <<"ctx-2">>, completed)),
    Got = ids(a2a_task_shard:tasks(Shard, ?TENANT, #{})),
    ?assertEqual([<<"t-1">>, <<"t-2">>], Got),
    ok.

%%%_* create/5 duplicate guard ------------------------------------------------

%% create_task looks the task up first and returns duplicate_task before it
%% appends anything to the log, so this guard is reachable without the router.
create_duplicate_task_is_rejected_before_touching_the_log(Config) ->
    Shard = ?config(shard, Config),
    Existing = proj(<<"t-1">>, ?TENANT, <<"ctx-1">>, working),
    ok = a2a_task_shard:restore(Shard, Existing),
    %% The duplicate guard resolves before the log append, so no bus is needed.
    ?assertEqual({error, duplicate_task},
                 a2a_task_shard:create(Shard, <<"t-1">>, ?TENANT,
                                       <<"ctx-1">>, message())),
    %% A duplicate did not add a second projection.
    ?assertEqual(1, a2a_task_shard:count(Shard)),
    ok.

%%%_* cancel/3 pre-append branches --------------------------------------------

cancel_unknown_task_is_task_not_found(Config) ->
    Shard = ?config(shard, Config),
    ?assertEqual({error, task_not_found},
                 a2a_task_shard:cancel(Shard, <<"nope">>, ?TENANT)),
    ok.

%% A task owned by another tenant is answered exactly like a missing one: task
%% ids must not become a cross-tenant existence oracle.
cancel_cross_tenant_caller_is_task_not_found(Config) ->
    Shard = ?config(shard, Config),
    ok = a2a_task_shard:restore(Shard, proj(<<"t-1">>, ?OTHER, <<"ctx-1">>, working)),
    ?assertEqual({error, task_not_found},
                 a2a_task_shard:cancel(Shard, <<"t-1">>, ?TENANT)),
    ok.

cancel_terminal_task_is_task_not_cancelable(Config) ->
    Shard = ?config(shard, Config),
    ok = a2a_task_shard:restore(Shard, proj(<<"t-1">>, ?TENANT, <<"ctx-1">>, completed)),
    ?assertEqual({error, task_not_cancelable},
                 a2a_task_shard:cancel(Shard, <<"t-1">>, ?TENANT)),
    ok.

%%%_* apply_event/4 pre-append branches ---------------------------------------

apply_event_unknown_task_is_task_not_found(Config) ->
    Shard = ?config(shard, Config),
    ?assertEqual({error, task_not_found},
                 a2a_task_shard:apply_event(Shard, <<"nope">>, ?TENANT,
                                            {status_update, #{}})),
    ok.

apply_event_cross_tenant_caller_is_task_not_found(Config) ->
    Shard = ?config(shard, Config),
    ok = a2a_task_shard:restore(Shard, proj(<<"t-1">>, ?OTHER, <<"ctx-1">>, working)),
    ?assertEqual({error, task_not_found},
                 a2a_task_shard:apply_event(Shard, <<"t-1">>, ?TENANT,
                                            {status_update, #{}})),
    ok.

apply_event_terminal_task_is_unsupported_operation(Config) ->
    Shard = ?config(shard, Config),
    ok = a2a_task_shard:restore(Shard, proj(<<"t-1">>, ?TENANT, <<"ctx-1">>, completed)),
    ?assertEqual({error, unsupported_operation},
                 a2a_task_shard:apply_event(Shard, <<"t-1">>, ?TENANT,
                                            {status_update, #{}})),
    ok.

%%%_* Helpers -----------------------------------------------------------------

%% The shard registers under a fixed local name; two cases running against the
%% same named server would share state, so cases pass the started pid. The pid
%% and the registered name are interchangeable for gen_server:call.
proj(TaskId, TenantId, ContextId, State) ->
    Base = a2a_task_projection:new(TaskId, TenantId, 1,
                                   #{<<"contextId">> => ContextId,
                                     <<"status">> => #{<<"state">> => a2a_task:to_wire(State)}}),
    Base#{state => State}.

message() ->
    #{<<"role">> => <<"ROLE_USER">>,
      <<"parts">> => [#{<<"kind">> => <<"text">>, <<"text">> => <<"hi">>}]}.

ids(Projs) -> lists:sort([a2a_task_projection:id(P) || P <- Projs]).
