%% @doc Boot replay (`a2a_task_recovery:recover/0') is the S9-B "correctness
%% baseline": every task projection is rebuilt from the shared log, a task whose
%% creation snapshot is gone is repaired with exactly one terminal FAILED event,
%% and repeated boots are idempotent. None of that had a test -- the module was
%% never referenced by any suite -- so a regression in the fold (wrong event
%% order, a rewritten history, a second repair per boot) would ship silently.
%%
%% These cases drive the log directly through `a2a_event:append/5' (the same
%% entry the live shard uses) and then call `recover/0', which is what the
%% registry's last child runs at init. Task ids are unique per case, so cases
%% never interfere even though the log and the registry accumulate across a run.
-module(a2a_task_recovery_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("broker_core/include/broker_message.hrl").

-compile([export_all, nowarn_export_all]).

-define(TENANT, <<"t1">>).
-define(PARTITIONS, 4).

all() ->
    [replay_rebuilds_the_projection_from_offset_zero,
     corrupt_history_is_logged_not_rewritten,
     history_lost_gets_exactly_one_terminal_failed_event,
     repeated_boots_do_not_append_another_repair].

%%%_* Fixtures --------------------------------------------------------------

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(broker_security),
    {ok, _} = application:ensure_all_started(broker_proto_agent_rpc),
    DataDir = filename:join(?config(priv_dir, Config), "a2a-recovery-data"),
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
                    push_ttl_ms, sse_heartbeat_ms, sse_max_duration_ms,
                    task_shards]],
    ok = application:set_env(broker_proto_a2a, listeners, []),
    ok = application:set_env(broker_proto_a2a, require_auth, true),
    ok = application:set_env(broker_proto_a2a, push_enabled, false),
    %% One shard, so `a2a_task_registry:get/1' always lands on the live shard
    %% regardless of how the task id hashes.
    ok = application:set_env(broker_proto_a2a, task_shards, 1),
    {ok, _} = application:ensure_all_started(broker_proto_a2a),
    [{data_dir, DataDir}, {old_a2a, Old} | Config].

end_per_suite(Config) ->
    _ = application:stop(broker_proto_a2a),
    _ = application:stop(broker_queue),
    _ = application:stop(broker_router),
    _ = application:stop(broker_storage),
    _ = application:stop(broker_meta),
    _ = application:stop(mnesia),
    _ = mnesia:delete_schema([node()]),
    lists:foreach(fun restore_a2a/1, ?config(old_a2a, Config)),
    ok.

restore_a2a({K, undefined}) ->
    _ = application:unset_env(broker_proto_a2a, K),
    ok;
restore_a2a({K, V}) ->
    ok = application:set_env(broker_proto_a2a, K, V).

%%%_* Cases -----------------------------------------------------------------

%% A healthy task is rebuilt from offset 0: the creation snapshot seeds the
%% projection, each delta folds onto it in order, and the fold neither appends
%% an event nor sends a push (replay "touches no subscription"). A second boot
%% changes nothing.
replay_rebuilds_the_projection_from_offset_zero(_Config) ->
    TaskId = unique(<<"task-replay">>),
    ContextId = unique(<<"ctx-replay">>),
    ok = append(TaskId, task, 1, task_snapshot(TaskId, ContextId)),
    ok = append(TaskId, status_update, 2, status_update(TaskId, working, false)),
    ok = append(TaskId, artifact_update, 3,
                artifact_update(<<"art-1">>, <<"a result">>)),
    ok = append(TaskId, message, 4, message_event(<<"m-1">>)),
    ok = a2a_task_recovery:recover(),
    Wire = wire(TaskId),
    ?assertEqual(a2a_task:to_wire(working),
                 maps:get(<<"state">>, maps:get(<<"status">>, Wire))),
    ?assertEqual(ContextId, maps:get(<<"contextId">>, Wire)),
    ?assertMatch([#{<<"artifactId">> := <<"art-1">>}],
                 maps:get(<<"artifacts">>, Wire)),
    ?assertMatch([#{<<"messageId">> := <<"m-1">>}],
                 maps:get(<<"history">>, Wire)),
    %% The log still holds exactly the four events: replay appended nothing.
    ?assertEqual(4, length(task_events(TaskId))),
    ok = a2a_task_recovery:recover(),
    Wire2 = wire(TaskId),
    ?assertEqual(Wire, Wire2),
    ?assertEqual(4, length(task_events(TaskId))).

%% ADR-025 §D3.3: an event that is not a legal transition is logged, not
%% rewritten. Replay must keep the last valid projection (COMPLETED) and leave
%% the log byte-for-byte intact.
corrupt_history_is_logged_not_rewritten(_Config) ->
    TaskId = unique(<<"task-corrupt">>),
    ok = append(TaskId, task, 1, task_snapshot(TaskId, unique(<<"ctx">>))),
    ok = append(TaskId, status_update, 2, status_update(TaskId, completed, true)),
    %% WORKING after COMPLETED is not a legal transition.
    ok = append(TaskId, status_update, 3, status_update(TaskId, working, false)),
    ok = a2a_task_recovery:recover(),
    Wire = wire(TaskId),
    ?assertEqual(a2a_task:to_wire(completed),
                 maps:get(<<"state">>, maps:get(<<"status">>, Wire))),
    %% No repair, no rewrite: still exactly the three original events.
    ?assertEqual(3, length(task_events(TaskId))).

%% A task whose creation snapshot is gone (the first visible event is a delta)
%% is reported as FAILED / history_lost with exactly one server-generated
%% terminal event, so the client stops polling a half task.
history_lost_gets_exactly_one_terminal_failed_event(_Config) ->
    TaskId = unique(<<"task-orphan">>),
    %% Only a delta: the creation snapshot was garbage-collected.
    ok = append(TaskId, status_update, 1, status_update(TaskId, working, false)),
    ok = a2a_task_recovery:recover(),
    Wire = wire(TaskId),
    ?assertEqual(a2a_task:to_wire(failed),
                 maps:get(<<"state">>, maps:get(<<"status">>, Wire))),
    ?assertEqual(<<"history_lost">>,
                 maps:get(<<"reason">>, maps:get(<<"metadata">>, Wire))),
    Events = task_events(TaskId),
    %% The original orphan delta plus exactly one repair.
    ?assertEqual(2, length(Events)),
    ?assertEqual(1, length([E || E <- Events, is_repair(E)])),
    ?assertMatch([#{<<"final">> := true}], [repair_status(E) || E <- Events,
                                                          is_repair(E)]).

%% The repair event is itself detectable, which is what keeps repeated boots
%% from appending one more repair each time: the second boot recognises the task
%% as already repaired and appends nothing.
repeated_boots_do_not_append_another_repair(_Config) ->
    TaskId = unique(<<"task-idem">>),
    ok = append(TaskId, status_update, 1, status_update(TaskId, working, false)),
    ok = a2a_task_recovery:recover(),
    ?assertEqual(2, length(task_events(TaskId))),
    ok = a2a_task_recovery:recover(),
    ok = a2a_task_recovery:recover(),
    %% Still the orphan delta plus the one repair from the first boot.
    ?assertEqual(2, length(task_events(TaskId))),
    Wire = wire(TaskId),
    ?assertEqual(a2a_task:to_wire(failed),
                 maps:get(<<"state">>, maps:get(<<"status">>, Wire))),
    ?assertEqual(1, length([E || E <- task_events(TaskId), is_repair(E)])).

%%%_* Event builders --------------------------------------------------------

append(TaskId, Kind, Seq, Payload) ->
    {ok, _Offset} = a2a_event:append(TaskId, ?TENANT, Kind, Seq, Payload),
    ok.

task_snapshot(TaskId, ContextId) ->
    #{<<"task">> => #{<<"id">> => TaskId,
                      <<"contextId">> => ContextId,
                      <<"status">> => #{<<"state">> => a2a_task:to_wire(submitted),
                                         <<"timestamp">> => ts()},
                      <<"history">> => []}}.

status_update(TaskId, State, Final) ->
    #{<<"statusUpdate">> =>
          #{<<"taskId">> => TaskId,
            <<"status">> => #{<<"state">> => a2a_task:to_wire(State),
                              <<"timestamp">> => ts()},
            <<"final">> => Final}}.

artifact_update(ArtifactId, Name) ->
    #{<<"artifactUpdate">> =>
          #{<<"artifact">> => #{<<"artifactId">> => ArtifactId,
                                <<"name">> => Name}}}.

message_event(MessageId) ->
    #{<<"message">> =>
          #{<<"messageId">> => MessageId,
            <<"role">> => <<"ROLE_AGENT">>,
            <<"parts">> => [#{<<"text">> => <<"hi">>}]}}.

ts() -> a2a_event:timestamp().

unique(Prefix) ->
    Bin = integer_to_binary(erlang:unique_integer([positive, monotonic])),
    <<Prefix/binary, "-", Bin/binary>>.

%%%_* Log inspection --------------------------------------------------------

%% Every stored event for one task, across every partition, in log order.
task_events(TaskId) ->
    lists:append([partition_events(P, TaskId) || P <- broker_queue:partition_ids()]).

partition_events(Partition, TaskId) ->
    {ok, Page, _Next} =
        broker_queue:read_from(Partition, 1, 1000000),
    [M || {_Offset, #broker_message{destination = D} = M} <- Page,
          D =:= destination(TaskId)].

%% The registry stores the internal projection; the wire shape is what a client
%% would see, and the shape the repair's metadata lands in.
wire(TaskId) ->
    {ok, Proj} = a2a_task_registry:get(TaskId),
    a2a_task_projection:to_wire(Proj).

destination(TaskId) ->
    <<"$a2a/task/", TaskId/binary>>.

is_repair(#broker_message{payload = Payload}) ->
    case a2a_event:decode(Payload) of
        {ok, #{body := Body}} -> is_repair_body(Body);
        _ -> false
    end.

is_repair_body(Body) ->
    Update = maps:get(<<"statusUpdate">>, Body, #{}),
    Metadata = maps:get(<<"metadata">>, Update, #{}),
    maps:get(<<"reason">>, Metadata, undefined) =:= <<"history_lost">>.

repair_status(#broker_message{payload = Payload}) ->
    {ok, #{body := Body}} = a2a_event:decode(Payload),
    maps:get(<<"statusUpdate">>, Body).
