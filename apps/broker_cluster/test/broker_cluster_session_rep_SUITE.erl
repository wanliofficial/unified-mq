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

all() -> [single_node_is_ok, no_replicas_is_ok, remote_apply_ok, majority_failure_counts_acks].

%% Hermetic: the initiator's trivial majority cases, no cluster metadata needed.
single_node_is_ok(_Config) ->
    ?assertEqual(ok, broker_cluster_session_rep:replicate_session(7, {cursor, r, 1, #{}}, [node()])).

no_replicas_is_ok(_Config) ->
    ?assertEqual(ok, broker_cluster_session_rep:replicate_session(7, {cursor, r, 1, #{}}, [])).

remote_apply_ok(_Config) ->
    ?assertEqual(ok, broker_cluster_session_rep:remote_apply({inflight, <<"c1">>, #{1 => x}})).

%% Hermetic majority-counting / failure path: replica set = self + two UNREACHABLE
%% nodes, so no Raft cluster is needed. `rpc:call' to a nonexistent node returns
%% `{badrpc, nodedown}' quickly, and the spawned collector counts each as a non-ack.
%% Needed = 2, local counts as 1 ack -> 1 < 2 -> replication_failed with the
%% documented {error, {replication_failed, Acks, Needed}} shape. Exercises
%% `parallel_apply/3' + `collect/2' real counting, not just the short-circuit cases.
majority_failure_counts_acks(_Config) ->
    %% 3-arity get_env returns the raw value (or the default), not {ok, V}:
    %% a suite earlier in the full run may leave replicate_timeout_ms set, so
    %% the saved value must restore that exact integer rather than crash on
    %% {ok, N} in the `case' below.
    SavedTimeout = application:get_env(broker_cluster, replicate_timeout_ms, undefined),
    ok = application:set_env(broker_cluster, replicate_timeout_ms, 500),
    try
        Fake = ['nonexistent-a@nohost', 'nonexistent-b@nohost'],
        Result = broker_cluster_session_rep:replicate_session(
                   7, {inflight, <<"c">>, #{}}, [node() | Fake]),
        ?assertMatch({error, {replication_failed, 1, 2}}, Result)
    after
        case SavedTimeout of
            undefined -> application:unset_env(broker_cluster, replicate_timeout_ms);
            T when is_integer(T) ->
                application:set_env(broker_cluster, replicate_timeout_ms, T)
        end
    end.
