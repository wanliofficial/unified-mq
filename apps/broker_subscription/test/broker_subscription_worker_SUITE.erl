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

init_per_testcase(_, Config) ->
    %% The hook is process-global state that outlives a case (persistent_term
    %% survives app restarts); start every case from the absent-hook default so
    %% the I2 assertions are meaningful.
    erase_hook(),
    Config.

%% `persistent_term:erase/1' raises `badarg' on a missing key, so guard it.
end_per_testcase(_, _) ->
    erase_hook(),
    ok.

erase_hook() ->
    case persistent_term:get({broker_subscription_worker, session_replicator}, undefined) of
        undefined -> ok;
        _ -> persistent_term:erase({broker_subscription_worker, session_replicator}), ok
    end.

all() -> [subscribe_without_hook_unchanged,
          subscribe_syncs_with_hook,
          subscribe_fails_when_hook_errors,
          inflight_syncs_with_hook,
          unsubscribe_replicates_without_failing,
          unsubscribe_still_ok_when_hook_errors,
          cursor_flush_replicates_async_with_hook,
          apply_replica_subscribe_idempotent,
          apply_replica_unsubscribe_clears,
          apply_replica_inflight,
          apply_replica_cursor].

%% MQ-17 Plan 1 Task 4: the initiator side consults the `session_replicator'
%% hook in the CALLER's process (the conn_fsm), never inside the worker's
%% gen_server. Absent hook (the default) must leave the single-node path
%% byte-identical (不变量 I2); a present hook makes subscribe/inflight/unsubscribe
%% replicate synchronously (§3.4 多数派 ack 后才回 ACK) and the cursor flush tick
%% replicate asynchronously.

%% The key lives under the *worker's* module atom, not this suite's.
-define(HOOK, {broker_subscription_worker, session_replicator}).

%% I2: with the hook erased, a subscribe behaves exactly as before Task 4 --
%% the hook is not even consulted, no ETS read, no spawn.
subscribe_without_hook_unchanged(_Config) ->
    erase_hook(),
    ?assertEqual(undefined, persistent_term:get(?HOOK, undefined)),
    {ok, _SubRef} = broker_subscription_worker:subscribe(<<"cx">>, <<"t/x">>, 1),
    ?assertMatch([_], broker_subscription:subscriptions(<<"cx">>)),
    ok = broker_subscription_worker:unsubscribe(<<"cx">>, <<"t/x">>),
    ?assertEqual([], broker_subscription:subscriptions(<<"cx">>)).

%% A capturing hook: the Fun runs in the caller's process, so `self()' is the
%% test process and the Op arrives in the mailbox.
subscribe_syncs_with_hook(_Config) ->
    erase_hook(),
    Self = self(),
    persistent_term:put(?HOOK, fun(Op) -> Self ! {replicated, Op}, ok end),
    try
        {ok, SubRef} = broker_subscription_worker:subscribe(<<"cy">>, <<"t/y">>, 1),
        receive
            {replicated, {subscribe, <<"cy">>, SubRef, Info}} ->
                ?assertMatch(#{filter := <<"t/y">>, consumers := [<<"cy">>]}, Info),
                ok
        after 1000 ->
                exit(no_subscribe_replication)
        end
    after
        erase_hook()
    end.

%% §3.4: a hook refusing the Op makes the caller's subscribe fail -- the upstream
%% SUBACK turns into a failure. NOTE (Plan 1): the local subscription is already
%% committed (ETS + fsync .subs) and is NOT torn down on replication error; the
%% caller/conn_fsm decides what to do with the error. The divergence is asserted
%% here so a later task that adds the rollback does not silently pass.
subscribe_fails_when_hook_errors(_Config) ->
    erase_hook(),
    persistent_term:put(?HOOK, fun(_Op) -> {error, boom} end),
    try
        {error, _} = broker_subscription_worker:subscribe(<<"cz">>, <<"t/z">>, 1),
        %% ...but the local sub is still there (see the NOTE above).
        ?assertMatch([_], broker_subscription:subscriptions(<<"cz">>)),
        ok
    after
        erase_hook(),
        ok = broker_subscription_worker:unsubscribe(<<"cz">>, <<"t/z">>)
    end.

%% inflight replicates synchronously too.
inflight_syncs_with_hook(_Config) ->
    erase_hook(),
    Self = self(),
    persistent_term:put(?HOOK, fun(Op) -> Self ! {replicated, Op}, ok end),
    try
        ok = broker_subscription_worker:inflight(<<"cw">>, #{42 => ack}),
        receive
            {replicated, {inflight, <<"cw">>, #{42 := ack}}} -> ok
        after 1000 ->
                exit(no_inflight_replication)
        end
    after
        erase_hook()
    end.

%% unsubscribe: the local removal already happened and cannot be rolled back, so
%% a hook error only warns and the caller still gets `ok'.
unsubscribe_replicates_without_failing(_Config) ->
    erase_hook(),
    Self = self(),
    persistent_term:put(?HOOK, fun(Op) -> Self ! {replicated, Op}, ok end),
    try
        {ok, _SubRef} = broker_subscription_worker:subscribe(<<"cv">>, <<"t/v">>, 1),
        ok = broker_subscription_worker:unsubscribe(<<"cv">>, <<"t/v">>),
        receive
            {replicated, {unsubscribe, <<"cv">>, {exclusive, <<"cv">>, <<"t/v">>}}} -> ok
        after 1000 ->
                exit(no_unsubscribe_replication)
        end
    after
        erase_hook()
    end.

%% With an erroring hook the unsubscribe still answers `ok' (removal is not
%% rolled back; the hook error is logged only).
unsubscribe_still_ok_when_hook_errors(_Config) ->
    erase_hook(),
    Self = self(),
    %% The hook errors ONLY on the unsubscribe Op: a subscribe still has to
    %% succeed so the case can drive a real unsubscribe through it.
    persistent_term:put(?HOOK, fun({unsubscribe, _, _}) -> {error, boom};
                                 (Op) -> Self ! {replicated, Op}, ok end),
    try
        {ok, _SubRef} = broker_subscription_worker:subscribe(<<"cu">>, <<"t/u">>, 1),
        ok = broker_subscription_worker:unsubscribe(<<"cu">>, <<"t/u">>),
        ?assertEqual([], broker_subscription:subscriptions(<<"cu">>))
    after
        erase_hook()
    end.

%% The ONE site that runs inside the worker: the cursor flush tick. It replicates
%% per-row ASYNC. The suite freezes the ticks at 60000 ms, so this case
%% restarts the app with the zero-regress tier (`cursor_flush_interval_ms => 0'),
%% which flushes on every ack -- making the flush (and therefore the spawn) fire
%% without waiting on a tick. The hook sends the Op to the test process from the
%% spawned pid; a plain `receive ... after' absorbs the async.
cursor_flush_replicates_async_with_hook(_Config) ->
    erase_hook(),
    Self = self(),
    persistent_term:put(?HOOK, fun(Op) -> Self ! {replicated, Op}, ok end),
    ok = bounce_worker_with_cursor_tier(0),
    try
        {ok, SubRef} = broker_subscription_worker:subscribe(<<"cr">>, <<"t/r">>, 1),
        ok = broker_subscription:ack(SubRef, 0, 1, <<"cr">>),
        receive
            {replicated, {cursor, SubRef, 0, Row}} ->
                ?assertMatch(#{kind := exclusive, offset := 2}, Row)
        after 2000 ->
                exit(no_cursor_replication)
        end
    after
        erase_hook(),
        ok = bounce_worker_with_cursor_tier(60000)
    end.

%% The suite's init_per_suite boots the worker once with the ticks frozen at
%% 60000 ms; `cursor_flush_interval_ms' is only read at worker init, so a
%% per-case override needs the app bounced (same shape as the facade suite's
%% init_per_testcase re-set pattern).
bounce_worker_with_cursor_tier(Ms) ->
    ok = application:stop(broker_subscription),
    ok = application:set_env(broker_subscription, cursor_flush_interval_ms, Ms),
    {ok, _} = application:ensure_all_started(broker_subscription),
    await_worker_ready(),
    ok.

await_worker_ready() ->
    case whereis(broker_subscription_worker) of
        undefined ->
            timer:sleep(25),
            await_worker_ready();
        _Pid ->
            ok
    end.

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
