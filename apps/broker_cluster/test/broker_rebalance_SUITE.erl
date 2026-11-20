%% @doc S12-C's rebalance trigger, single-node (ADR-032 D4, R-RB1/R-RB2).
%%
%% The distributed half -- real peers, real cross-node ownership moves, fencing
%% and no-quorum -- lives in `broker_rebalance_dist_SUITE'. This suite proves the
%% *decision* logic without forming a real group: `evaluate/1' and `step/1' take
%% the membership explicitly (production passes `broker_cluster_meta:members()'),
%% so an imbalanced view can be described with plain node atoms while a real
%% single-member ra group still commits the resulting `migrate'/'assign'.
%%
%% Everything here is the frozen D4 contract:
%%   * default closed -- `evaluate/0' and `step/0' answer `{disabled}' until an
%%     operator turns it on;
%%   * target = partition-count balance;
%%   * hysteresis + cooldown both must hold before a move;
%%   * exactly one move per `step/0'.
-module(broker_rebalance_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(PT_LAST, {broker_rebalance, last_migration_ms}).
-define(MEMBERS, ['n1@host', 'n2@host']).

all() ->
    [disabled_by_default,
     balanced_view_is_stable,
     imbalance_beyond_hysteresis_proposes_a_move,
     step_moves_one_partition_toward_balance,
     hysteresis_gate_holds_within_threshold,
     cooldown_gate_blocks_then_expires,
     one_move_per_round,
     orphan_partition_is_reassigned_to_a_member,
     finish_pending_clears_migrating_state,
     manual_assign_is_not_fought].

%%%_* Fixtures --------------------------------------------------------------

init_per_suite(Config) -> Config.

end_per_suite(_Config) -> ok.

init_per_testcase(Case, Config) ->
    stop_node(),
    DataDir = filename:join([?config(priv_dir, Config), atom_to_list(Case), "ra"]),
    ok = application:set_env(broker_cluster, data_dir, DataDir),
    ok = application:set_env(broker_cluster, members, []),
    ok = application:set_env(broker_cluster, quorum_timeout_ms, 3000),
    ok = application:set_env(broker_cluster, replicate_timeout_ms, 100),
    _ = application:load(broker_queue),
    _ = application:load(broker_storage),
    ok = filelib:ensure_dir(filename:join([DataDir, "x"])),
    {ok, _} = application:ensure_all_started(broker_storage),
    {ok, Cache} = broker_cluster_cache:start_link(),
    unlink(Cache),
    {ok, _} = broker_cluster_meta:start_link(),
    %% Clear the cooldown clock (persistent_term) and the test clock (process
    %% dictionary) so a move recorded by one case cannot gate the next.
    catch persistent_term:erase(?PT_LAST),
    erase(rebalance_clock),
    rebalance_env(#{enabled => false}),
    [{data_dir, DataDir} | Config].

end_per_testcase(_Case, _Config) ->
    stop_node(),
    ok.

stop_node() ->
    _ = try broker_cluster:uninstall_hooks() catch _:_ -> ok end,
    _ = application:stop(broker_queue),
    _ = application:stop(broker_storage),
    _ = try gen_server:stop(broker_cluster_meta_ctl) catch _:_ -> ok end,
    _ = try ra:stop_server(default, {broker_cluster_meta, node()}) catch _:_ -> ok end,
    _ = try ra_system:stop_default() catch _:_ -> ok end,
    _ = try gen_server:stop(broker_cluster_cache) catch _:_ -> ok end,
    ok.

rebalance_env(Overrides) ->
    Cfg = maps:merge(#{enabled => true, hysteresis => 1, cooldown_ms => 0,
                       now => fun clock/0},
                     Overrides),
    ok = application:set_env(broker_cluster, rebalance, Cfg),
    ok.

clock() ->
    case get(rebalance_clock) of
        undefined -> 0;
        V -> V
    end.

advance_clock(By) ->
    put(rebalance_clock, clock() + By),
    ok.

%%%_* Cases ----------------------------------------------------------------

disabled_by_default(_Config) ->
    %% app.src ships enabled=false; a fresh config answers `{disabled}'.
    ok = application:set_env(broker_cluster, rebalance, #{enabled => false}),
    ?assertMatch({disabled}, broker_rebalance:evaluate()),
    ?assertMatch({disabled}, broker_rebalance:step()),
    ok.

balanced_view_is_stable(_Config) ->
    rebalance_env(#{}),
    ok = assign(0, 'n1@host', 1),
    ok = assign(1, 'n2@host', 1),
    ?assertMatch({stable, _}, broker_rebalance:evaluate(?MEMBERS)),
    ?assertMatch({stable, _}, broker_rebalance:step(?MEMBERS)),
    ok.

imbalance_beyond_hysteresis_proposes_a_move(_Config) ->
    rebalance_env(#{hysteresis => 1}),
    ok = assign(0, 'n1@host', 1),
    ok = assign(1, 'n1@host', 1),
    {rebalance, Plan} = broker_rebalance:evaluate(?MEMBERS),
    ?assertEqual(migrate, maps:get(action, Plan)),
    ?assertEqual('n1@host', maps:get(from, Plan)),
    ?assertEqual('n2@host', maps:get(to, Plan)),
    %% One partition, at the current epoch + 1, and the exact command step runs.
    ?assertEqual(2, maps:get(epoch, Plan)),
    ?assertMatch({migrate, 0, 'n1@host', 'n2@host', 2},
                 maps:get(command, Plan)),
    ok.

step_moves_one_partition_toward_balance(_Config) ->
    rebalance_env(#{hysteresis => 1, cooldown_ms => 0}),
    ok = assign(0, 'n1@host', 1),
    ok = assign(1, 'n1@host', 1),
    %% One step moves exactly one partition; the spread drops by two (one leaves
    %% the source, one arrives at the target) and the view is balanced.
    {migrated, P, 'n1@host', 'n2@host'} = broker_rebalance:step(?MEMBERS),
    ?assert(lists:member(P, [0, 1])),
    {stable, After} = broker_rebalance:evaluate(?MEMBERS),
    ?assertEqual(0, maps:get(spread, After)),
    ?assertEqual(#{'n1@host' => 1, 'n2@host' => 1}, maps:get(counts, After)),
    ok.

hysteresis_gate_holds_within_threshold(_Config) ->
    rebalance_env(#{hysteresis => 3}),
    ok = assign(0, 'n1@host', 1),
    ok = assign(1, 'n1@host', 1),
    %% Spread is 2, within the hysteresis of 3: no move, and not a cooldown.
    ?assertMatch({stable, _}, broker_rebalance:evaluate(?MEMBERS)),
    ?assertMatch({stable, _}, broker_rebalance:step(?MEMBERS)),
    ok.

cooldown_gate_blocks_then_expires(_Config) ->
    rebalance_env(#{hysteresis => 1, cooldown_ms => 100}),
    put(rebalance_clock, 1000),
    %% Four partitions on one node: after one move the spread is still 2, so a
    %% second move is genuinely warranted and the cooldown is what stops it.
    [ok = assign(P, 'n1@host', 1) || P <- lists:seq(0, 3)],
    %% First step moves one and records the clock (1000).
    ?assertMatch({migrated, _, 'n1@host', 'n2@host'},
                 broker_rebalance:step(?MEMBERS)),
    %% Immediately after, the spread is still over hysteresis but the cooldown
    %% holds: the answer is `{cooldown}', not a move and not `{stable}'.
    ?assertMatch({cooldown, _}, broker_rebalance:evaluate(?MEMBERS)),
    ?assertMatch({cooldown, _}, broker_rebalance:step(?MEMBERS)),
    %% Past the cooldown, the next move proceeds.
    advance_clock(1000),
    ?assertMatch({migrated, _, 'n1@host', 'n2@host'},
                 broker_rebalance:step(?MEMBERS)),
    ok.

one_move_per_round(_Config) ->
    rebalance_env(#{hysteresis => 1, cooldown_ms => 0}),
    [ok = assign(P, 'n1@host', 1) || P <- lists:seq(0, 3)],
    %% 4 vs 0: one round moves one partition -> 3 vs 1 (spread 2, still hot).
    ?assertMatch({migrated, _, 'n1@host', 'n2@host'},
                 broker_rebalance:step(?MEMBERS)),
    {rebalance, Plan} = broker_rebalance:evaluate(?MEMBERS),
    ?assertEqual(#{'n1@host' => 3, 'n2@host' => 1}, maps:get(counts, Plan)),
    %% And it would only move one more: the spread is 2, still over hysteresis.
    Counts = maps:get(counts, Plan),
    ?assertEqual(2, maps:get('n1@host', Counts) - maps:get('n2@host', Counts)),
    ok.

orphan_partition_is_reassigned_to_a_member(_Config) ->
    %% The membership is just this node; a partition owned by a departed node is
    %% orphaned and must come home, regardless of the cooldown (recovery, not
    %% tuning).
    rebalance_env(#{hysteresis => 1, cooldown_ms => 100000}),
    put(rebalance_clock, 5000),
    ok = assign(0, 'ghost@nowhere', 1),
    {rebalance, Plan} = broker_rebalance:evaluate([node()]),
    ?assertEqual(reassign, maps:get(action, Plan)),
    ?assertEqual(undefined, maps:get(from, Plan)),
    ?assertEqual(node(), maps:get(to, Plan)),
    Self = node(),
    {reassigned, 0, Self} = broker_rebalance:step([node()]),
    {ok, Self} = broker_cluster:owner(0),
    ok.

finish_pending_clears_migrating_state(_Config) ->
    rebalance_env(#{hysteresis => 1, cooldown_ms => 0}),
    ok = assign(0, 'n1@host', 1),
    ok = assign(1, 'n1@host', 1),
    {migrated, _P, 'n1@host', 'n2@host'} = broker_rebalance:step(?MEMBERS),
    %% ADR-021 D3: after migrate the partition is `migrating', not `active'.
    {ok, Migrating} = broker_cluster_cache:get(0),
    ?assertEqual(migrating, maps:get(state, Migrating)),
    {finished, [0]} = broker_rebalance:finish_pending(),
    ?assertMatch({ok, #{state := active}}, broker_cluster_cache:get(0)),
    %% Finishing an already-active partition is a no-op.
    ?assertEqual({finished, []}, broker_rebalance:finish_pending()),
    ok.

%% D4: manual override wins because the trigger only moves on explicit
%% invocation. Here the "manual" state is simply that an operator placed a
%% partition; a balanced view means the trigger has nothing to say, so it never
%% fights the placement.
manual_assign_is_not_fought(_Config) ->
    rebalance_env(#{hysteresis => 1}),
    ok = assign(0, 'n1@host', 1),
    ok = assign(1, 'n2@host', 1),
    ?assertMatch({stable, _}, broker_rebalance:evaluate(?MEMBERS)),
    ok.

%%%_* Helpers --------------------------------------------------------------

assign(Partition, Owner, Epoch) ->
    {ok, Epoch} = broker_cluster:assign(Partition, Owner, [Owner], Epoch),
    ok.
