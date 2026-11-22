%% @doc MQ-17 (会话复制 Plan 1 Task 2): the receive-side replica entry
%% `broker_subscription_worker:apply_replica/1'. A replica node applies a
%% session Op straight into the local ETS tables -- no ownership guard, no
%% subscriber notification, no Router registration -- and the apply is
%% idempotent (cross-node rpc may deliver the same Op twice).
%%
%% These cases pin the pure-write contract: after `apply_replica/1' the entity,
%% member, inflight and cursor rows are present and a second apply changes
%% nothing (overwrite, not duplicate). The Op protocol is `session_op()' in the
%% worker module.
-module(broker_subscription_worker_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

init_per_suite(Config) ->
    %% The worker owns the five ETS tables, so it must be up before any case
    %% can write to them. Booting it needs its dependencies (storage) and at
    %% least one real partition -- the worker's boot step queries watermarks,
    %% which an unpartitioned node refuses -- so mirror the production boot
    %% order rather than a bare `ensure_all_started'.
    DataDir = filename:join(["/tmp/opencode", "subs_worker_repl_data"]),
    %% A leftover data dir from a previous run still holds partition 0, which
    %% `create_partition/1' then refuses as `already_exists'; start from a clean
    %% slate so the case assertions are not polluted by a prior run's session.
    _ = os:cmd("rm -rf " ++ DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "sessions")),
    %% Preload the apps before set_env: an app's `.app' env is read at load
    %% time, so a set_env issued before `ensure_all_started' loads the app
    %% would be clobbered by the defaults (partitions => 4). Loading first
    %% pins the app, and the set_env below then sticks. Same order the storage
    %% suite uses.
    _ = application:load(broker_storage),
    _ = application:load(broker_queue),
    _ = application:load(broker_router),
    _ = application:load(broker_subscription),
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok = application:set_env(broker_storage, data_dir, DataDir),
    %% /tmp is a volatile mount; the WAL backend's durability guard is about real
    %% data dirs, so switch it off the same way the other storage suites do.
    ok = application:set_env(broker_storage, wal, #{require_durable_fs => false}),
    ok = application:set_env(broker_subscription, data_dir, DataDir),
    ok = application:set_env(broker_queue, partitions, 1),
    ok = application:set_env(broker_router, partitions, 1),
    %% Freeze the worker's flush/retry ticks far out so no 100 ms tick races a
    %% case: apply_replica is a pure write and must not depend on the scheduler.
    ok = application:set_env(broker_subscription, cursor_flush_interval_ms, 60000),
    ok = application:set_env(broker_subscription, inflight_flush_interval_ms, 60000),
    ok = application:set_env(broker_subscription, retry_tick_ms, 60000),
    {ok, _} = application:ensure_all_started(broker_storage),
    [ok = broker_queue:create_partition(P) || P <- broker_queue:partition_ids()],
    {ok, _} = application:ensure_all_started(broker_subscription),
    Config.

end_per_suite(_) ->
    _ = application:stop(broker_subscription),
    _ = application:stop(broker_queue),
    _ = application:stop(broker_router),
    _ = application:stop(broker_storage),
    ok.

init_per_testcase(_, Config) -> Config.
end_per_testcase(_, _) -> ok.

all() -> [apply_replica_subscribe_idempotent,
          apply_replica_unsubscribe_clears,
          apply_replica_inflight,
          apply_replica_cursor].

apply_replica_subscribe_idempotent(_Config) ->
    SubRef = make_ref(),
    Info = #{kind => exclusive, key => <<"t/1">>, filter => <<"t/1">>,
             qos => 1, consumers => [<<"c1">>, <<"c2">>]},
    Op = {subscribe, <<"c1">>, SubRef, Info},
    ok = broker_subscription_worker:apply_replica(Op),
    ok = broker_subscription_worker:apply_replica(Op),
    ?assertMatch([{SubRef, _}], ets:lookup(broker_subscription_entities, SubRef)),
    ?assertMatch([{{<<"c1">>, SubRef}}], ets:lookup(broker_subscription_members, {<<"c1">>, SubRef})),
    ?assertMatch([{{<<"c2">>, SubRef}}], ets:lookup(broker_subscription_members, {<<"c2">>, SubRef})).

apply_replica_inflight(_Config) ->
    Op = {inflight, <<"c1">>, #{42 => ack}},
    ok = broker_subscription_worker:apply_replica(Op),
    ok = broker_subscription_worker:apply_replica(Op),
    ?assertMatch([{<<"c1">>, #{42 := ack}}],
                 ets:lookup(broker_subscription_inflight, <<"c1">>)).

apply_replica_cursor(_Config) ->
    SubRef = make_ref(),
    Row = #{low => 5, high => 10},
    Op = {cursor, SubRef, 3, Row},
    ok = broker_subscription_worker:apply_replica(Op),
    ok = broker_subscription_worker:apply_replica(Op),
    ?assertMatch([{{SubRef, 3}, _}], ets:lookup(broker_subscription_cursors, {SubRef, 3})).

%% MQ-17 Plan 1 Task 2 review: the `unsubscribe' Op cleanup must actually wipe every
%% derived row. Seed the full state first -- a `{subscribe,...}' Op (writes ENTITIES
%% + MEMBERS) and a `{cursor, SubRef, P, Row}' Op (seeds a cursor row) -- then apply
%% `{unsubscribe, ClientId, SubRef}'. The Op names only the ClientId and SubRef, so
%% the cleanup sequence (delete entity, scan-and-delete cursor rows by SubRef,
%% scan-and-delete member pointers by SubRef) is what leaves all three tables empty.
%% This also documents the assumption the cleanup relies on (see the worker's
%% `unsubscribe' clause): a shared partial leave must never reach this Op.
apply_replica_unsubscribe_clears(_Config) ->
    SubRef = make_ref(),
    Info = #{kind => exclusive, key => <<"t/1">>, filter => <<"t/1">>,
             qos => 1, consumers => [<<"c1">>]},
    ok = broker_subscription_worker:apply_replica({subscribe, <<"c1">>, SubRef, Info}),
    Row = #{low => 5, high => 10},
    ok = broker_subscription_worker:apply_replica({cursor, SubRef, 3, Row}),
    ok = broker_subscription_worker:apply_replica({unsubscribe, <<"c1">>, SubRef}),
    ?assertEqual([], ets:lookup(broker_subscription_entities, SubRef)),
    ?assertEqual([], ets:lookup(broker_subscription_members, {<<"c1">>, SubRef})),
    ?assertEqual([], ets:match_object(broker_subscription_cursors, {{SubRef, '_'}, '_'})).
