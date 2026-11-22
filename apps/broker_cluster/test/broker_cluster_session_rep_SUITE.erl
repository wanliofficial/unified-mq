%% @doc MQ-17 (会话复制 Plan 1 Task 3): the replication initiator `broker_cluster_session_rep'.
%%
%% These cases pin the pure-function core without booting the cluster metadata
%% group (which needs `ra' and a real Raft election): `replicate_session/3' with
%% an empty replica set or a single-node replica set is the trivial majority case
%% and must be `ok'. `remote_apply/1` is the receive-side entry: it delegates to
%% `broker_subscription_worker:apply_replica/1', so the subscription worker (and
%% its ETS tables) must be up -- the boot order mirrors
%% `broker_subscription_worker_SUITE:init_per_suite/1'.
-module(broker_cluster_session_rep_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

init_per_suite(Config) ->
    DataDir = filename:join(["/tmp/opencode", "sess_rep_repl_data"]),
    _ = os:cmd("rm -rf " ++ DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "sessions")),
    _ = application:load(broker_storage),
    _ = application:load(broker_queue),
    _ = application:load(broker_router),
    _ = application:load(broker_subscription),
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok = application:set_env(broker_storage, data_dir, DataDir),
    ok = application:set_env(broker_storage, wal,
                             #{require_durable_fs => false}),
    ok = application:set_env(broker_subscription, data_dir, DataDir),
    ok = application:set_env(broker_queue, partitions, 1),
    ok = application:set_env(broker_router, partitions, 1),
    ok = application:set_env(broker_subscription, cursor_flush_interval_ms,
                             60000),
    ok = application:set_env(broker_subscription, inflight_flush_interval_ms,
                             60000),
    ok = application:set_env(broker_subscription, retry_tick_ms, 60000),
    {ok, _} = application:ensure_all_started(broker_storage),
    [ok = broker_queue:create_partition(P)
     || P <- broker_queue:partition_ids()],
    {ok, _} = application:ensure_all_started(broker_subscription),
    Config.

end_per_suite(_Config) ->
    _ = application:stop(broker_subscription),
    _ = application:stop(broker_queue),
    _ = application:stop(broker_router),
    _ = application:stop(broker_storage),
    ok.

init_per_testcase(_, Config) -> Config.
end_per_testcase(_, _) -> ok.

all() -> [single_node_is_ok, no_replicas_is_ok, remote_apply_ok].

%% Hermetic: the initiator's trivial majority cases, no cluster metadata needed.
single_node_is_ok(_Config) ->
    ?assertEqual(ok, broker_cluster_session_rep:replicate_session(7, {cursor, r, 1, #{}}, [node()])).

no_replicas_is_ok(_Config) ->
    ?assertEqual(ok, broker_cluster_session_rep:replicate_session(7, {cursor, r, 1, #{}}, [])).

remote_apply_ok(_Config) ->
    ?assertEqual(ok, broker_cluster_session_rep:remote_apply({inflight, <<"c1">>, #{1 => x}})).
