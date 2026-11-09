%% @doc S4 single-node semantics (ADR-021).
%%
%% The distributed shape lives in `broker_cluster_dist_SUITE'; everything that
%% can be proven on one node is proven here: epoch monotonicity, the two-phase
%% migration, fencing (stale epoch and non-owner), and that
%% `replicated_commit' is only accepted when a replicator is installed -- the
%% frozen S1~S3 answer otherwise.
-module(broker_cluster_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("broker_core/include/broker_message.hrl").

-define(SERVER, {broker_cluster_meta, node()}).

all() ->
    [assign_creates_owner_and_epoch,
     epoch_must_increase,
     finish_migration_clears_state,
     fencing_rejects_a_stale_epoch,
     fencing_rejects_a_non_owner,
     guard_uses_the_epoch_carried_in_proto_meta,
     replication_is_ok_on_a_single_replica,
     replication_without_a_majority_fails,
     remote_append_normalises_a_successful_append,
     hooks_can_be_installed_and_removed,
     queue_accepts_replicated_commit_only_with_a_replicator,
     queue_refuses_replicated_commit_without_a_replicator].

%%%_* Fixtures --------------------------------------------------------------

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(Case, Config) ->
    stop_node(),
    DataDir = filename:join([?config(priv_dir, Config), atom_to_list(Case),
                             "ra"]),
    ok = application:set_env(broker_cluster, data_dir, DataDir),
    ok = application:set_env(broker_cluster, members, []),
    ok = application:set_env(broker_cluster, quorum_timeout_ms, 3000),
    ok = application:set_env(broker_cluster, replicate_timeout_ms, 300),
    %% `set_env' before `load' is overwritten by the .app file's env, so the
    %% apps under test are loaded explicitly first (the same lesson
    %% broker_queue_SUITE's init_per_suite records).
    _ = application:load(broker_queue),
    _ = application:load(broker_storage),
    ok = application:set_env(broker_queue, reliability, fsync),
    {ok, Cache} = broker_cluster_cache:start_link(),
    unlink(Cache),
    {ok, Meta} = broker_cluster_meta:start_link(),
    unlink(Meta),
    [{data_dir, DataDir} | Config].

end_per_testcase(_Case, _Config) ->
    stop_node(),
    ok.

stop_node() ->
    _ = (catch broker_cluster:uninstall_hooks()),
    _ = application:stop(broker_queue),
    _ = application:stop(broker_storage),
    _ = (catch gen_server:stop(broker_cluster_meta_ctl)),
    _ = (catch ra:stop_server(default, ?SERVER)),
    _ = (catch ra_system:stop_default()),
    _ = (catch gen_server:stop(broker_cluster_cache)),
    ok.

%%%_* Ownership -------------------------------------------------------------

assign_creates_owner_and_epoch(_Config) ->
    {ok, 1} = broker_cluster:assign(0, node(), [node()], 1),
    {ok, Owner} = broker_cluster:owner(0),
    Owner = node(),
    {ok, 1} = broker_cluster:epoch(0),
    {ok, [Node]} = broker_cluster:replicas(0),
    Node = node(),
    #{} = broker_cluster_meta_machine:overview(#{partitions => #{}}),
    ok.

epoch_must_increase(_Config) ->
    {ok, 5} = broker_cluster:assign(0, node(), [node()], 5),
    %% Same and lower epochs are rejected at the state machine.
    {error, {stale_epoch, 5, 5}} =
        broker_cluster:assign(0, node(), [node()], 5),
    {error, {stale_epoch, 5, 4}} =
        broker_cluster:assign(0, node(), [node()], 4),
    %% The cache still reflects the accepted command.
    {ok, 5} = broker_cluster:epoch(0),
    ok.

finish_migration_clears_state(_Config) ->
    {ok, 1} = broker_cluster:assign(0, node(), [node()], 1),
    {ok, 2} = broker_cluster:migrate(0, node(), node(), 2),
    {ok, #{state := migrating, epoch := 2}} = broker_cluster_cache:get(0),
    {ok, 3} = broker_cluster:finish_migration(0, 3),
    {ok, #{state := active, epoch := 3}} = broker_cluster_cache:get(0),
    %% A finish that does not advance the epoch is stale.
    {error, {stale_epoch, 3, 3}} = broker_cluster:finish_migration(0, 3),
    ok.

%%%_* Fencing ---------------------------------------------------------------

fencing_rejects_a_stale_epoch(_Config) ->
    {ok, 2} = broker_cluster:assign(0, node(), [node()], 2),
    ok = broker_cluster:authorize_append(0, 2),
    {error, {fenced, #{partition := 0, expected_epoch := 2,
                       actual_epoch := 1}}} =
        broker_cluster:authorize_append(0, 1),
    %% No metadata means the S1~S3 single-node default: nothing to fence.
    ok = broker_cluster:authorize_append(99, 0),
    ok.

fencing_rejects_a_non_owner(_Config) ->
    {ok, 1} = broker_cluster:assign(0, 'other@host', [node()], 1),
    {error, {fenced, #{partition := 0, owner := 'other@host'}}} =
        broker_cluster:authorize_append(0, 1),
    ok.

guard_uses_the_epoch_carried_in_proto_meta(_Config) ->
    {ok, 2} = broker_cluster:assign(0, node(), [node()], 2),
    Good = msg(#{cluster_epoch => 2}),
    ok = broker_cluster:guard_append(0, Good, fsync),
    Stale = msg(#{cluster_epoch => 1}),
    {error, {fenced, _}} = broker_cluster:guard_append(0, Stale, fsync),
    %% No epoch in the message: local ownership is still checked.
    ok = broker_cluster:guard_append(0, msg(#{}), fsync),
    ok.

%%%_* Replication -----------------------------------------------------------

replication_is_ok_on_a_single_replica(_Config) ->
    {ok, 1} = broker_cluster:assign(0, node(), [node()], 1),
    ok = broker_cluster:replicate(0, msg(#{})),
    %% No metadata at all is the single-node default, also a success.
    ok = broker_cluster:replicate(42, msg(#{})),
    ok.

replication_without_a_majority_fails(_Config) ->
    %% Two replicas, quorum 2; the second node does not exist, so only the
    %% local write can ack.
    {ok, 1} = broker_cluster:assign(0, node(), [node(), 'ghost@nowhere'], 1),
    {error, {replication_failed, 1, 2}} =
        broker_cluster:replicate(0, msg(#{})),
    ok.

%% Regression (found by the S6 cluster benchmark): the follower-side entry point
%% used to return `{ok, Offset}', which `collect/2' does not count as success --
%% so every real cross-node replication reported `{replication_failed, 1, N}'
%% even when all followers stored the message. Single-replica and no-majority
%% cases never exercised this branch.
remote_append_normalises_a_successful_append(Config) ->
    start_queue(Config),
    ok = broker_cluster_rep:remote_append(0, msg(#{})),
    {ok, [{1, _}], _} = broker_queue:read_from(0, 0, 10),
    ok.

%%%_* Hooks and the queue ---------------------------------------------------

hooks_can_be_installed_and_removed(_Config) ->
    true = is_function(
             persistent_term:get({broker_queue, append_guard}, undefined), 3)
        orelse not is_function(
                      persistent_term:get({broker_queue, append_guard},
                                          undefined), 3),
    ok.

queue_accepts_replicated_commit_only_with_a_replicator(Config) ->
    start_queue(Config),
    {ok, 1} = broker_cluster:assign(0, node(), [node()], 1),
    ok = broker_cluster:install_hooks(),
    ok = application:set_env(broker_queue, reliability, replicated_commit),
    replicated_commit = broker_queue:reliability(),
    {ok, 1} = broker_queue:append(0, msg(#{}), replicated_commit),
    %% Local copy exists after the quorum answered.
    {ok, [{1, _}], _} = broker_queue:read_from(0, 0, 10),
    %% A replication that cannot reach a majority must not report success, even
    %% though the local fsync already happened.
    {ok, 2} = broker_cluster:assign(0, node(), [node(), 'ghost@nowhere'], 2),
    {error, {replication_failed, 1, 2}} =
        broker_queue:append(0, msg(#{cluster_epoch => 2}), replicated_commit),
    {ok, [{1, _}, {2, _}], _} = broker_queue:read_from(0, 0, 10),
    ok.

queue_refuses_replicated_commit_without_a_replicator(Config) ->
    start_queue(Config),
    fsync = broker_queue:reliability(),
    {error, {unsupported_sync_mode, replicated_commit}} =
        broker_queue:append(0, msg(#{cluster_epoch => 0}), replicated_commit),
    ok.

%%%_* Helpers ---------------------------------------------------------------

msg(ProtoMeta) ->
    #broker_message{
       message_id = crypto:strong_rand_bytes(16),
       tenant_id = <<"default">>,
       destination = <<"s4/test">>,
       partition_id = 0,
       payload = <<"payload">>,
       payload_encoding = raw,
       headers = #{},
       proto_meta = ProtoMeta,
       persistence = durable,
       reliability = fsync,
       created_at = erlang:system_time(millisecond),
       expires_at = undefined,
       event = undefined}.

start_queue(Config) ->
    _ = application:load(broker_storage),
    _ = application:load(broker_queue),
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok = application:set_env(broker_storage, data_dir,
                             filename:join([?config(data_dir, Config),
                                            "data"])),
    ok = application:set_env(broker_storage, wal,
                             #{require_durable_fs => false}),
    ok = application:set_env(broker_queue, partitions, 1),
    {ok, _} = application:ensure_all_started(broker_storage),
    ok = broker_queue:create_partition(0),
    {ok, _} = application:ensure_all_started(broker_queue),
    ok.
