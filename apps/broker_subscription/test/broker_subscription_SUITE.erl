%% @doc T7: the subscription layer -- per-(SubRef, PartitionId) cursors (H11),
%% §5.3 three-tier session persistence, boot steps 2+3, QoS0 dispatch with the
%% H7 client-level merge, and the committed-watermark aggregation (K6).
%%
%% Four things are pinned here, each with its own case:
%%
%%  * R1b is executable: the subscription set (with its init anchors, K5) is on
%%    `protected' media before `subscribe/3' answers, and boot rebuilds entities
%%    + Router index all-or-nothing -- a corrupt `.subs' fails the application
%%    instead of serving a silently smaller session set (ADR-011 §11.2 one
%%    layer up, same stance as T6's partition recovery).
%%  * The watermark math is exactly K6: committed = min(live cursors) - 1, and
%%    it never crosses the point the cursor *files* can vouch for
%%    (`flushed_cursors_back_the_watermark_it_proves' checks the file the
%%    watermark claims to be derived from -- the ordering the storage guard
%%    ADR-011 §11.3 needs in the *other* direction than S0 could test it).
%%  * The data path does not pass through the worker: `cursor/2',
%%    `subscriptions/1' and `dispatch/2' read the protected tables in the
%%    caller's process -- `sys:suspend' on the worker proves it structurally,
%%    the same argument T6 made for `append/3'.
%%  * I15's twin for this layer: a killed worker restarts and reloads
%%    everything from disk; `already_registered' backfills are the K8 crash
%%    window's residue, not a boot error.
-module(broker_subscription_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("broker_core/include/broker_message.hrl").

-define(N, 2).
-define(LONG_FLUSH_MS, 3600000).

all() ->
    [boot_with_an_empty_sessions_dir_starts_clean,
     boot_fails_loudly_on_a_corrupt_subs_file,
     subscribe_mints_a_subref_the_route_can_see,
     resubscribe_updates_qos_without_touching_the_route,
     a_new_subscription_anchors_at_the_live_tail,
     ack_moves_the_cursor_over_the_contiguous_prefix_only,
     an_ack_behind_the_cursor_is_a_noop,
     acking_a_dead_subscription_is_idempotent_ok,
     reads_survive_a_suspended_worker,
     committed_is_min_cursor_minus_one_and_a_laggard_pins_it,
     flushed_cursors_back_the_watermark_it_proves,
     the_zero_regress_tier_flushes_every_ack,
     the_default_tier_rewinds_to_the_last_flush_never_loser_acks,
     a_lost_cursors_file_falls_back_to_the_subs_origin_not_committed,
     an_offline_persistent_subscriber_pins_and_releases_the_watermark,
     inflight_is_an_opaque_blob_that_may_only_staleness_lag,
     persisted_inflight_blob_decodes_without_the_mqtt_apps_atoms,
     overlapping_filters_of_one_client_merge_into_one_delivery,
     a_dead_attached_process_detaches_and_clean_session_tears_down,
     detach_is_pid_guarded_against_takeover,
     router_store_is_supervised_and_env_reaches_it,
     the_memory_backend_works_through_the_same_api,
     %% ADR-019 §19.1/§19.2: the pure row algebra, no application booted.
     shared_claim_is_disjoint_across_members,
     claim_does_not_move_low,
     ack_out_of_order_does_not_promote_low,
     filling_the_hole_promotes_low_once,
     acked_below_low_is_idempotent,
     shared_state_max_refuses_more_claims,
     release_returns_offsets_to_the_pool,
     rollback_only_returns_what_is_still_held,
     claim_stops_before_the_watermark,
     %% ADR-019 §19.1/§19.2/§19.4: worker-level state (claim, ack ownership,
     %% group membership, failover election).
     join_adds_a_consumer_in_order,
     leave_promotes_the_next_member,
     leave_of_the_last_member_removes_the_subref,
     the_worker_never_claims_past_the_watermark,
     a_dead_consumer_releases_its_whole_page,
     a_non_holder_ack_counts_and_changes_nothing,
     %% ADR-019 §19.3: the v2 file layout and the v1 migration.
     inflight_stays_client_keyed,
     migration_rewrites_v1_subs_and_cursors,
     migration_is_idempotent_after_a_partial_write,
     migration_refuses_to_boot_on_a_corrupt_v1_file,
     orphan_cursors_files_are_removed_at_boot,
     %% ADR-019 §19.2 (plan §2.5): dispatch recipients by kind.
     a_qos0_publish_reaches_exactly_one_shared_member,
     a_durable_wake_reaches_every_shared_member,
     %% S2-E (ADR-015): retry algebra, backoff, DLQ and TTL-adjacent rules.
     retry_algebra_schedules_and_ack_clears,
     backoff_sequence_is_exponential_and_capped,
     shared_retry_waits_for_the_backoff_before_reclaim,
     max_attempts_dead_letters_and_advances,
     a_dead_letter_is_an_ordinary_subscribable_destination,
     dlq_template_validation_and_rendering,
     a_bad_dlq_template_refuses_boot,
     a_failed_dead_letter_keeps_the_cursor,
     retry_progress_survives_a_restart].

%%%_* Fixtures ------------------------------------------------------------

init_per_suite(Config) ->
    %% `_ =' on purpose: CT runs every suite in one node, and an earlier suite
    %% (router or queue) may have loaded the app already (J10's lesson).
    _ = application:load(broker_queue),
    _ = application:load(broker_router),
    _ = application:load(broker_subscription),
    ok = application:set_env(broker_queue, partitions, ?N),
    ok = application:set_env(broker_router, partitions, ?N),
    Config.

end_per_suite(_Config) ->
    stop_all(),
    ok.

init_per_testcase(Case, Config) ->
    stop_all(),
    DataDir = filename:join([?config(priv_dir, Config), atom_to_list(Case), "data"]),
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok = application:set_env(broker_storage, data_dir, DataDir),
    ok = application:set_env(broker_storage, wal, #{require_durable_fs => false}),
    ok = application:set_env(broker_subscription, data_dir, DataDir),
    %% Default flush windows for the suite; individual cases re-set *before*
    %% `boot/0' (which is the last set_env consumer) to pick another tier.
    ok = application:set_env(broker_subscription, cursor_max_regress, 64),
    ok = application:set_env(broker_subscription, cursor_flush_interval_ms, 100),
    ok = application:set_env(broker_subscription, inflight_flush_interval_ms, 100),
    %% S2-E knobs: every case starts from the production defaults, and a case
    %% that needs another posture re-sets them before boot/0 (same J10 rule).
    ok = application:set_env(broker_subscription, retry_tick_ms, 1000),
    ok = application:set_env(broker_subscription, retry_base_ms, 1000),
    ok = application:set_env(broker_subscription, retry_max_delay_ms, 60000),
    ok = application:set_env(broker_subscription, max_attempts, 10),
    ok = application:set_env(broker_subscription, dlq_name_pattern,
                             <<"$dlq/{tenant}/{original}">>),
    {ok, _Started} = application:ensure_all_started(broker_storage),
    [{data_dir, DataDir} | Config].

end_per_testcase(_Case, _Config) ->
    stop_all(),
    ok.

%% The full consumer chain: partitions exist out-of-band (H8), then one
%% `ensure_all_started' boots storage (up) -> router -> queue -> subscription,
%% which is exactly the §6 order the app dependency lists encode.
boot() ->
    [ok = broker_queue:create_partition(P) || P <- broker_queue:partition_ids()],
    {ok, _Started} = application:ensure_all_started(broker_subscription),
    ok.

%% A subscription-app-only bounce: the Router store *keeps* its index while the
%% worker boots again, so the step-3 backfill runs against `already_registered'
%% for every SubRef -- the K8 residue path, and the real shape of a crash
%% restart under supervision.
bounce_subscription_app() ->
    ok = application:stop(broker_subscription),
    {ok, _} = application:ensure_all_started(broker_subscription),
    ok.

stop_all() ->
    _ = application:stop(broker_subscription),
    _ = application:stop(broker_queue),
    _ = application:stop(broker_router),
    _ = application:stop(broker_storage),
    cleanup_writers(),
    ok.

cleanup_writers() ->
    case whereis(broker_storage_wal_sup) of
        undefined ->
            ok;
        _Sup ->
            lists:foreach(
              fun({Id, _Child, _Type, _Modules}) ->
                      _ = supervisor:terminate_child(broker_storage_wal_sup, Id),
                      ok = supervisor:delete_child(broker_storage_wal_sup, Id)
              end,
              supervisor:which_children(broker_storage_wal_sup))
    end.

%%%_* Boot --------------------------------------------------------------------

boot_with_an_empty_sessions_dir_starts_clean(Config) ->
    ok = boot(),
    Worker = whereis(broker_subscription_worker),
    true = is_process_alive(Worker),
    %% Nothing was invented: the sessions dir only appears with the first
    %% subscribe (boot reads, never creates -- the H8 stance).
    false = filelib:is_dir(filename:join([?config(data_dir, Config), "sessions"])),
    {error, not_found} = broker_subscription:session(<<"nobody">>),
    ok.

boot_fails_loudly_on_a_corrupt_subs_file(_Config) ->
    ok = boot(),
    {ok, _SubRef} = broker_subscription:subscribe(<<"c1">>, <<"a/b">>, 1),
    ok = bounce_subscription_app(),
    Path = broker_subscription_files:subs_path({exclusive, <<"c1">>, <<"a/b">>}),
    ok = file:write_file(Path, <<"not a term at all">>),
    %% The running worker never re-reads the file (§5.3: `.subs' is consulted
    %% at boot); the refusal has to come from the next boot.
    ok = application:stop(broker_subscription),
    {error, {{shutdown, {failed_to_start_child, broker_subscription_worker,
                         {subscription_boot_failed,
                          {session_scan, {corrupt_session, Path}}}}}, _}} =
        application:start(broker_subscription),
    %% All-or-nothing: a refused boot must leave no half-loaded state to serve.
    undefined = whereis(broker_subscription_worker),
    %% And the refusal is about *this file*, not the directory: put back a
    %% valid envelope and the very next start succeeds. The settle dance is
    %% the controller's, not ours: a start that just failed can still be in
    %% `starting' state for a moment, where `start' answers
    %% `{error, {already_started, _}}'.
    ok = broker_subscription_files:write_subs(
           {exclusive, <<"c1">>, <<"a/b">>},
           #{kind => exclusive, key => <<"c1">>, filter => <<"a/b">>, qos => 1,
             consumers => [<<"c1">>], init_cursors => [{0, 1}, {1, 1}],
             created_at => 0}),
    ok = start_after_settle(),
    [_] = broker_subscription:subscriptions(<<"c1">>),
    ok.

start_after_settle() ->
    case application:start(broker_subscription) of
        ok ->
            ok;
        {error, {already_started, broker_subscription}} ->
            %% Either the failed one still settles (stop clears the state and
            %% the next start faces a settled controller) or a racing start
            %% won -- in that case the boot succeeded over the repaired file.
            case whereis(broker_subscription_worker) of
                undefined ->
                    timer:sleep(50),
                    _ = application:stop(broker_subscription),
                    start_after_settle();
                Worker ->
                    true = is_process_alive(Worker),
                    ok
            end
    end.

%%%_* subscribe / route -------------------------------------------------------

subscribe_mints_a_subref_the_route_can_see(_Config) ->
    ok = boot(),
    {ok, SubRef} = broker_subscription:subscribe(<<"c1">>, <<"sport/#">>, 1),
    {exclusive, <<"c1">>, <<"sport/#">>} = SubRef,
    [SubRef] = route_refs(<<"sport/tennis/a">>),
    [S] = broker_subscription:subscriptions(<<"c1">>),
    #{qos := 1, filter := <<"sport/#">>} = S,
    ok.

resubscribe_updates_qos_without_touching_the_route(_Config) ->
    ok = boot(),
    {ok, SubRef} = broker_subscription:subscribe(<<"c1">>, <<"sport/#">>, 0),
    {ok, SubRef} = broker_subscription:subscribe(<<"c1">>, <<"sport/#">>, 1),
    %% K2: one route entry (a re-register would have paid for an unregister
    %% window and a `{error, {already_registered, _}}' refusal), updated QoS,
    %% and the *cursor kept* -- a re-subscribe continues, it does not replay.
    1 = length(route_refs(<<"sport/tennis/a">>)),
    [SubRef] = route_refs(<<"sport/tennis/a">>),
    [#{qos := 1}] = broker_subscription:subscriptions(<<"c1">>),
    ok.

a_new_subscription_anchors_at_the_live_tail(_Config) ->
    ok = boot(),
    {ok, _} = append(0, <<"sport/a">>),
    {ok, _} = append(1, <<"sport/a">>),
    {ok, _} = append(0, <<"sport/a">>),
    {ok, SubRef} = broker_subscription:subscribe(<<"c1">>, <<"sport/#">>, 1),
    %% H11/§4.6: every partition's cursor starts at its *live* High -- not 0,
    %% not the historical tail cached at boot. 1-based offsets: two appends in
    %% P0 put it at 3, one append in P1 puts it at 2.
    {ok, 3} = broker_subscription:cursor(SubRef, 0),
    {ok, 2} = broker_subscription:cursor(SubRef, 1),
    ok.

%%%_* ack / cursor ------------------------------------------------------------

ack_moves_the_cursor_over_the_contiguous_prefix_only(_Config) ->
    ok = boot(),
    {ok, SubRef} = broker_subscription:subscribe(<<"c1">>, <<"sport/#">>, 1),
    {ok, 1} = broker_subscription:cursor(SubRef, 0),
    ok = broker_subscription:ack(SubRef, 0, 2, <<"c1">>),
    ok = broker_subscription:ack(SubRef, 0, 3, <<"c1">>),
    {ok, 1} = broker_subscription:cursor(SubRef, 0),
    ok = broker_subscription:ack(SubRef, 0, 1, <<"c1">>),
    {ok, 4} = broker_subscription:cursor(SubRef, 0),
    ok.

an_ack_behind_the_cursor_is_a_noop(_Config) ->
    ok = boot(),
    {ok, SubRef} = broker_subscription:subscribe(<<"c1">>, <<"sport/#">>, 1),
    ok = broker_subscription:ack(SubRef, 0, 1, <<"c1">>),
    {ok, 2} = broker_subscription:cursor(SubRef, 0),
    ok = broker_subscription:ack(SubRef, 0, 1, <<"c1">>),
    {ok, 2} = broker_subscription:cursor(SubRef, 0),
    ok.

acking_a_dead_subscription_is_idempotent_ok(_Config) ->
    ok = boot(),
    {ok, SubRef} = broker_subscription:subscribe(<<"c1">>, <<"gone">>, 1),
    ok = broker_subscription:unsubscribe(<<"c1">>, <<"gone">>),
    %% A conn_fsm racing its own UNSUBSCRIBE must not get an exit-shaped
    %% protocol error for one in-flight ack.
    ok = broker_subscription:ack(SubRef, 0, 1, <<"c1">>),
    {error, {unknown_subscription, SubRef}} = broker_subscription:cursor(SubRef, 0),
    ok.

reads_survive_a_suspended_worker(_Config) ->
    ok = boot(),
    {ok, SubRef} = broker_subscription:subscribe(<<"c1">>, <<"sport/#">>, 1),
    {ok, undefined} = broker_subscription:attach(<<"c1">>, self(), true),
    Worker = whereis(broker_subscription_worker),
    ok = sys:suspend(Worker),
    {ok, 1} = broker_subscription:cursor(SubRef, 0),
    [_] = broker_subscription:subscriptions(<<"c1">>),
    ok = broker_subscription:dispatch([SubRef], {0, 1, msg(<<"x">>)}),
    receive {broker_deliver, <<"c1">>, 1, [SubRef], {0, 1, _}} -> ok
    after 1000 -> exit(suspended_worker_blocked_reads) end,
    ok = sys:resume(Worker),
    ok.

%%%_* Watermark math (K6) ------------------------------------------------------

committed_is_min_cursor_minus_one_and_a_laggard_pins_it(_Config) ->
    %% Freeze the tick: the only advances in this case come from the
    %% max_regress threshold, so each assertion's cause is exactly known.
    ok = application:set_env(broker_subscription, cursor_flush_interval_ms, ?LONG_FLUSH_MS),
    ok = application:set_env(broker_subscription, cursor_max_regress, 1),
    ok = boot(),
    {ok, A} = broker_subscription:subscribe(<<"c1">>, <<"fast/#">>, 1),
    {ok, B} = broker_subscription:subscribe(<<"c2">>, <<"slow/#">>, 1),
    %% Three real records in partition 0 (offsets 1..3): the storage guard
    %% refuses committed > High-1, and these cases must not *measure* that
    %% refusal when they mean to measure cursor aggregation.
    [_, _, _] = [element(2, append(0, <<"fast/x">>)) || _ <- [1, 2, 3]],
    ok = broker_subscription:ack(A, 0, 1, <<"c1">>),
    %% B still sits at 1: min is 1, committed must not move past 0.
    {ok, Committed, _High} = broker_queue:watermarks(0),
    0 = Committed,
    ok = broker_subscription:ack(B, 0, 1, <<"c2">>),
    {ok, 1, _} = broker_queue:watermarks(0),
    ok = broker_subscription:ack(A, 0, 2, <<"c1">>),
    ok = broker_subscription:ack(B, 0, 2, <<"c2">>),
    {ok, 2, _} = broker_queue:watermarks(0),
    ok.

flushed_cursors_back_the_watermark_it_proves(_Config) ->
    ok = application:set_env(broker_subscription, cursor_flush_interval_ms, ?LONG_FLUSH_MS),
    ok = application:set_env(broker_subscription, cursor_max_regress, 4),
    ok = boot(),
    {ok, SubRef} = broker_subscription:subscribe(<<"c1">>, <<"sport/#">>, 1),
    [_, _, _, _] = [element(2, append(0, <<"sport/x">>)) || _ <- [1, 2, 3, 4]],
    [ok = broker_subscription:ack(SubRef, 0, O, <<"c1">>) || O <- [1, 2, 3]],
    %% Not yet flushed (3 < 4, no tick): the watermark must not have crossed
    %% what the *disk* can vouch for -- the K6 ordering, seen from the side
    %% where breaking it would make S2's truncation delete un-acked data.
    {ok, Committed, _} = broker_queue:watermarks(0),
    0 = Committed,
    ok = broker_subscription:ack(SubRef, 0, 4, <<"c1">>),
    {ok, CommittedNow, _} = broker_queue:watermarks(0),
    4 = CommittedNow,
    {ok, #{kind := exclusive, rows := Rows}} =
        broker_subscription_files:read_cursors(SubRef),
    #{0 := #{offset := 5}} = Rows,
    ok.

the_zero_regress_tier_flushes_every_ack(_Config) ->
    ok = application:set_env(broker_subscription, cursor_flush_interval_ms, 0),
    ok = boot(),
    {ok, SubRef} = broker_subscription:subscribe(<<"c1">>, <<"sport/#">>, 1),
    ok = broker_subscription:ack(SubRef, 0, 1, <<"c1">>),
    {ok, _} = broker_subscription_files:read_cursors(SubRef),
    %% R3's zero-window tier: kill right after an ack and the cursor comes
    %% back from disk at the acked point -- no rewind to re-deliver.
    exit(whereis(broker_subscription_worker), kill),
    await_worker_restart(),
    timer:sleep(50),
    {ok, 2} = broker_subscription:cursor(SubRef, 0),
    ok.

the_default_tier_rewinds_to_the_last_flush_never_loser_acks(_Config) ->
    ok = application:set_env(broker_subscription, cursor_flush_interval_ms, ?LONG_FLUSH_MS),
    ok = application:set_env(broker_subscription, cursor_max_regress, 10000),
    ok = boot(),
    {ok, SubRef} = broker_subscription:subscribe(<<"c1">>, <<"sport/#">>, 1),
    ok = broker_subscription:ack(SubRef, 0, 1, <<"c1">>),
    ok = broker_subscription:ack(SubRef, 0, 2, <<"c1">>),
    exit(whereis(broker_subscription_worker), kill),
    await_worker_restart(),
    timer:sleep(50),
    %% Gate S1 R3: the cursor may regress to the last flush (origin 1 here),
    %% which means already-acked offsets may be delivered *again* (duplicates
    %% are legal, §4.1) -- but never further back than the subscription's
    %% origin, and the storage side lost nothing because the watermark could
    %% not advance past unflushed cursors (K6, previous case).
    {ok, 1} = broker_subscription:cursor(SubRef, 0),
    {ok, 0, _} = broker_queue:watermarks(0),
    ok.

a_lost_cursors_file_falls_back_to_the_subs_origin_not_committed(_Config) ->
    ok = boot(),
    {ok, SubRef} = broker_subscription:subscribe(<<"c1">>, <<"sport/#">>, 1),
    [_, _, _] = [element(2, append(0, <<"sport/x">>)) || _ <- [1, 2, 3]],
    ok = broker_subscription:ack(SubRef, 0, 1, <<"c1">>),
    ok = broker_subscription:ack(SubRef, 0, 2, <<"c1">>),
    timer:sleep(300),
    {ok, 2, _} = broker_queue:watermarks(0),
    %% Now *lose* the cursor file (simulate the worst restore: `.cursors' gone,
    %% `.subs' survived). The K5 rule: fall back to the init anchor embedded in
    %% `.subs' (here 1), never to `Committed' (2 now -- taking it would *skip*
    %% offsets 1..2 the client may still need, and taking 0 would rewind behind
    %% the subscription). In S1 nothing is truncated yet, so replaying from the
    %% origin costs duplicates only -- which §4.1 allows and R7 expects.
    ok = file:delete(broker_subscription_files:cursors_path(SubRef)),
    bounce_subscription_app(),
    {ok, 1} = broker_subscription:cursor(SubRef, 0),
    {ok, 1} = broker_subscription:cursor(SubRef, 1),
    {ok, 2, _} = broker_queue:watermarks(0),
    ok.

an_offline_persistent_subscriber_pins_and_releases_the_watermark(_Config) ->
    ok = application:set_env(broker_subscription, cursor_flush_interval_ms, ?LONG_FLUSH_MS),
    ok = application:set_env(broker_subscription, cursor_max_regress, 1),
    ok = boot(),
    {ok, Online} = broker_subscription:subscribe(<<"fast">>, <<"fast/#">>, 1),
    {ok, Offline} = broker_subscription:subscribe(<<"slow">>, <<"slow/#">>, 1),
    {ok, _} = append(0, <<"slow/x">>),
    Pid = spawn(fun() -> timer:sleep(60000) end),
    {ok, undefined} = broker_subscription:attach(<<"slow">>, Pid, true),
    ok = broker_subscription:detach(<<"slow">>, Pid),
    %% The subscriber is gone but its session (and cursor 1) is kept: it pins.
    ok = broker_subscription:ack(Online, 0, 1, <<"fast">>),
    {ok, Pinned, _} = broker_queue:watermarks(0),
    0 = Pinned,
    %% Catching up *online* or offline is the same ack path (T9's conn_fsm
    %% would replay from the cursor after a real reconnect).
    ok = broker_subscription:ack(Offline, 0, 1, <<"slow">>),
    {ok, 1, _} = broker_queue:watermarks(0),
    ok.

%%%_* inflight (tier 3, R7) ----------------------------------------------------

inflight_is_an_opaque_blob_that_may_only_staleness_lag(_Config) ->
    ok = application:set_env(broker_subscription, inflight_flush_interval_ms, 10),
    ok = boot(),
    {ok, SubRef} = broker_subscription:subscribe(<<"c1">>, <<"sport/#">>, 1),
    _ = SubRef,
    ok = broker_subscription:inflight(<<"c1">>, #{v => 1}),
    timer:sleep(100),
    {ok, #{inflight := #{v := 1}}} = broker_subscription:session(<<"c1">>),
    ok = broker_subscription:inflight(<<"c1">>, #{v => 2}),
    timer:sleep(100),
    exit(whereis(broker_subscription_worker), kill),
    await_worker_restart(),
    timer:sleep(50),
    %% R7: the recovered blob is whichever flush last landed -- never
    %% invented, never empty-with-a-sub-session.
    {ok, #{inflight := #{v := V}}} = broker_subscription:session(<<"c1">>),
    true = (V =:= 1) orelse (V =:= 2),
    ok.

%% 【M11】The blob is opaque and belongs to the conn FSM, so it carries that
%% app's atoms (`next_pid'/`pending'). Boot step 2 reads it *before*
%% broker_proto_mqtt starts, so decoding with `[safe]' turned a persisted
%% session into a boot blocker: `broker_subscription_worker:init/1' exited with
%% `{inflight_file,{corrupt_session,Path}}'. A peer node with only this app on
%% its code path reproduces that boot order (this CT node has every module
%% loaded, which is why the bug hid here).
persisted_inflight_blob_decodes_without_the_mqtt_apps_atoms(Config) ->
    DataDir = ?config(data_dir, Config),
    ok = broker_subscription_files:write_inflight(<<"peer-blob">>,
                                                  #{next_pid => 2, pending => []}),
    Ebin = filename:dirname(code:which(broker_subscription_files)),
    {ok, Peer, _Node} =
        peer:start_link(#{name => subscription_blob_peer,
                          args => ["-pa", Ebin],
                          connection => standard_io}),
    try
        %% Guard the guard: `[safe]' can only fail on an atom the peer does not
        %% know, so a peer that already had it would prove nothing.
        false = peer_has_atom(Peer, "next_pid"),
        ok = peer:call(Peer, application, set_env,
                       [broker_subscription, data_dir, DataDir]),
        {ok, #{next_pid := 2, pending := []}} =
            peer:call(Peer, broker_subscription_files, read_inflight, [<<"peer-blob">>])
    after
        peer:stop(Peer)
    end.

peer_has_atom(Peer, Name) ->
    case catch peer:call(Peer, erlang, list_to_existing_atom, [Name]) of
        Atom when is_atom(Atom) -> true;
        _ -> false
    end.

%%%_* dispatch (H7, §5.1(f)/K9) ------------------------------------------------

overlapping_filters_of_one_client_merge_into_one_delivery(_Config) ->
    ok = boot(),
    {ok, Wide} = broker_subscription:subscribe(<<"c1">>, <<"sport/#">>, 0),
    {ok, Narrow} = broker_subscription:subscribe(<<"c1">>, <<"sport/tennis/+">>, 1),
    {ok, undefined} = broker_subscription:attach(<<"c1">>, self(), true),
    Refs = route_refs(<<"sport/tennis/a">>),
    true = lists:member(Wide, Refs) andalso lists:member(Narrow, Refs),
    ok = broker_subscription:dispatch(Refs, {0, 5, msg(<<"x">>)}),
    receive
        {broker_deliver, <<"c1">>, 1, Matched, {0, 5, _}} ->
            2 = length(Matched),
            true = lists:member(Wide, Matched) andalso lists:member(Narrow, Matched),
            no_more = receive {broker_deliver, _, _, _, _} -> more
                      after 50 -> no_more end
    after 1000 -> exit(h7_merge_not_delivered) end,
    ok.

a_dead_attached_process_detaches_and_clean_session_tears_down(_Config) ->
    ok = boot(),
    {ok, _SubRef} = broker_subscription:subscribe(<<"c1">>, <<"sport/#">>, 1),
    {ok, undefined} = broker_subscription:attach(<<"c1">>, self(), false),
    %% Persist=false (§4.2 clean session): the detach itself must remove the
    %% entities, the Router entries and the files.
    ok = broker_subscription:detach(<<"c1">>, self()),
    [] = broker_subscription:subscriptions(<<"c1">>),
    [] = route_refs(<<"sport/#">>),
    none = read_subs_or_none({exclusive, <<"c1">>, <<"sport/#">>}),
    {error, not_found} = broker_subscription:session(<<"c1">>),
    ok.

detach_is_pid_guarded_against_takeover(_Config) ->
    ok = boot(),
    {ok, _} = broker_subscription:subscribe(<<"c1">>, <<"sport/#">>, 1),
    Old = spawn(fun() -> timer:sleep(60000) end),
    New = self(),
    {ok, undefined} = broker_subscription:attach(<<"c1">>, Old, true),
    %% T9's takeover input: attach answers the *previous* pid, the new conn
    %% decides what to do with it (close its socket).
    {ok, Old} = broker_subscription:attach(<<"c1">>, New, true),
    {ok, undefined} = broker_subscription:attach(<<"c1">>, New, true),
    %% K10: the old socket's late DOWN/detach after a takeover must not evict
    %% the survivor.
    ok = broker_subscription:detach(<<"c1">>, Old),
    ok = broker_subscription:dispatch(route_refs(<<"sport/#">>), {0, 1, msg(<<"x">>)}),
    receive {broker_deliver, <<"c1">>, _, _, _} -> ok
    after 1000 -> exit(takeover_evicted_by_old_pid) end,
    ok.

%%%_* Assembly pins (K-series groundwork) --------------------------------------

router_store_is_supervised_and_env_reaches_it(_Config) ->
    ok = boot(),
    [{broker_router_hash_store, StorePid, worker, [broker_router_hash_store]}] =
        supervisor:which_children(broker_router_sup),
    true = is_process_alive(StorePid),
    {ok, Env} = broker_router:env(),
    #{store := StorePid} = Env,
    ok.

the_memory_backend_works_through_the_same_api(_Config) ->
    ok = boot(),
    {ok, WalRef} = broker_subscription:subscribe(<<"c1">>, <<"sport/#">>, 1),
    {ok, _} = append(0, <<"sport/x">>),
    ok = broker_subscription:ack(WalRef, 0, 1, <<"c1">>),
    timer:sleep(300),
    {ok, 1, _} = broker_queue:watermarks(0),
    %% Same queue/subscription API over the other registered backend (K1's
    %% `watermarks/1' must be same-shaped, not WAL-only). Full bounce: the
    %% backend is decided at supervisor init.
    stop_all(),
    ok = application:set_env(broker_storage, backend, broker_storage_memory),
    {ok, _} = application:ensure_all_started(broker_storage),
    boot(),
    {ok, MemRef} = broker_subscription:subscribe(<<"c1">>, <<"sport/#">>, 1),
    MemRef = WalRef,
    {ok, 0, 1} = broker_queue:watermarks(0),
    %% E12 again: `fsync' is not in the memory backend's capability, and the
    %% storage layer's runtime backstop refuses it -- an honest error, not a
    %% silent downgrade (ADR-011 §11.4).
    {ok, _} = append(0, <<"sport/x">>, os_buffer),
    ok = broker_subscription:ack(MemRef, 0, 1, <<"c1">>),
    timer:sleep(300),
    {ok, 1, 2} = broker_queue:watermarks(0),
    {ok, 2} = broker_subscription:cursor(MemRef, 0),
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok.

%%%_* ADR-019: the pure row algebra ----------------------------------------------
%%
%% These cases boot nothing: `broker_cursor' is a pure module (ADR-013 §13.1),
%% which is exactly why the claim/ack rules can be pinned without an ETS table,
%% a worker or a data dir. The worker-level cases below cover the parts that do
%% need state (watermark lookup, dirty ledger, detach); these cover the algebra.

shared_claim_is_disjoint_across_members(_Config) ->
    Row0 = broker_cursor:new_shared(1),
    {ok, A, Row1} = broker_cursor:claim(Row0, 3, 100, <<"a">>, 4096),
    {ok, B, _Row2} = broker_cursor:claim(Row1, 3, 100, <<"b">>, 4096),
    [1, 2, 3] = A,
    [4, 5, 6] = B,
    %% D-4: the two pages do not intersect -- the whole point of claiming in the
    %% worker instead of letting every member read from `min(cursor)'.
    [] = [O || O <- A, lists:member(O, B)],
    ok.

claim_does_not_move_low(_Config) ->
    Row0 = broker_cursor:new_shared(7),
    {ok, [7, 8, 9], Row1} = broker_cursor:claim(Row0, 3, 100, <<"a">>, 4096),
    %% Assignment and completion are different facts: only an ack may move the
    %% watermark (ADR-019 §19.1).
    7 = broker_cursor:next_read(Row1),
    6 = broker_cursor:committed(Row1),
    %% The tail is an empty page, not an error (D-5).
    {ok, [], _} = broker_cursor:claim(Row1, 3, 10, <<"b">>, 4096),
    ok.

claim_stops_before_the_watermark(_Config) ->
    %% D-6b: `High' is `Last + 1' (broker_storage contract), so offset `High'
    %% itself does not exist yet. Claiming it would create an assignment that
    %% can never be acked and pin `low' forever.
    Row0 = broker_cursor:new_shared(1),
    {ok, [1, 2, 3, 4], Row1} = broker_cursor:claim(Row0, 100, 5, <<"a">>, 4096),
    #{assigned := Assigned} = Row1,
    false = maps:is_key(5, Assigned),
    {ok, [], _} = broker_cursor:claim(Row1, 100, 5, <<"b">>, 4096),
    ok.

ack_out_of_order_does_not_promote_low(_Config) ->
    Row0 = broker_cursor:new_shared(1),
    {ok, _, Row1} = broker_cursor:claim(Row0, 3, 100, <<"a">>, 4096),
    {ok, Row2} = broker_cursor:ack_shared(Row1, 3, <<"a">>),
    1 = broker_cursor:next_read(Row2),
    {ok, Row3} = broker_cursor:ack_shared(Row2, 2, <<"a">>),
    %% 2 and 3 are acked, 1 is not: the prefix is one long, so `low' is still 1.
    1 = broker_cursor:next_read(Row3),
    0 = broker_cursor:committed(Row3),
    ok.

filling_the_hole_promotes_low_once(_Config) ->
    Row0 = broker_cursor:new_shared(1),
    {ok, _, Row1} = broker_cursor:claim(Row0, 3, 100, <<"a">>, 4096),
    {ok, Row2} = broker_cursor:ack_shared(Row1, 3, <<"a">>),
    {ok, Row3} = broker_cursor:ack_shared(Row2, 2, <<"a">>),
    {ok, Row4} = broker_cursor:ack_shared(Row3, 1, <<"a">>),
    %% The hole filled, so `low' eats the whole buffered run in one step and
    %% `acked' is left empty (nothing is stranded above the prefix).
    4 = broker_cursor:next_read(Row4),
    #{} = maps:get(acked, Row4),
    #{} = maps:get(assigned, Row4),
    3 = broker_cursor:committed(Row4),
    ok.

acked_below_low_is_idempotent(_Config) ->
    Row0 = broker_cursor:new_shared(1),
    {ok, _, Row1} = broker_cursor:claim(Row0, 2, 100, <<"a">>, 4096),
    {ok, Row2} = broker_cursor:ack_shared(Row1, 1, <<"a">>),
    {ok, Row3} = broker_cursor:ack_shared(Row2, 2, <<"a">>),
    3 = broker_cursor:next_read(Row3),
    %% A duplicate ack behind the prefix -- from a retried PUBACK or a late
    %% packet -- answers `ok' and changes nothing (D-3).
    {ok, Row3} = broker_cursor:ack_shared(Row3, 1, <<"a">>),
    {ok, Row3} = broker_cursor:ack_shared(Row3, 2, <<"b">>),
    ok.

shared_state_max_refuses_more_claims(_Config) ->
    Row0 = broker_cursor:new_shared(1),
    {ok, [1, 2, 3, 4], Row1} = broker_cursor:claim(Row0, 10, 100, <<"a">>, 4),
    %% D-6: the window is full, so the next claim is refused -- bounded
    %% rejection, never eviction of a live assignment.
    {error, overflow} = broker_cursor:claim(Row1, 10, 100, <<"b">>, 4),
    %% A page that would overflow is trimmed to what fits, not refused.
    {ok, [5], Row2} = broker_cursor:claim(Row1, 10, 100, <<"b">>, 5),
    {error, overflow} = broker_cursor:claim(Row2, 10, 100, <<"c">>, 5),
    %% Acking frees room again.
    {ok, Row3} = broker_cursor:ack_shared(Row2, 1, <<"a">>),
    {ok, [6], _} = broker_cursor:claim(Row3, 10, 100, <<"c">>, 5),
    ok.

release_returns_offsets_to_the_pool(_Config) ->
    Row0 = broker_cursor:new_shared(1),
    {ok, [1, 2, 3], Row1} = broker_cursor:claim(Row0, 3, 100, <<"a">>, 4096),
    {ok, [4], Row2} = broker_cursor:claim(Row1, 1, 100, <<"b">>, 4096),
    %% The member died holding 1..3; they go back to the pool and the survivor
    %% gets them on its next claim (D-8).
    Row3 = broker_cursor:release(Row2, <<"a">>),
    {ok, [1, 2, 3], _} = broker_cursor:claim(Row3, 3, 100, <<"b">>, 4096),
    ok.

%% ADR-019 §19.5: a claim can name an offset the storage `High' has reserved but
%% not yet flushed, so the pull loop reads back a page, finds the offset absent
%% and hands it back. That rollback must be ownership-checked: an offset acked
%% while the read was in flight stays acked (ack wins), and another member's
%% offsets are never touched.
rollback_only_returns_what_is_still_held(_Config) ->
    Row0 = broker_cursor:new_shared(1),
    {ok, [1, 2, 3], Row1} = broker_cursor:claim(Row0, 3, 100, <<"a">>, 4096),
    {ok, [4], Row2} = broker_cursor:claim(Row1, 1, 100, <<"b">>, 4096),
    {ok, Row3} = broker_cursor:ack_shared(Row2, 2, <<"a">>),
    Row4 = broker_cursor:rollback(Row3, [1, 2, 3, 4], <<"a">>),
    #{assigned := Assigned} = Row4,
    %% 4 stays with `b'; 2 is acked (not assigned any more); 1 and 3 go back.
    #{4 := <<"b">>} = Assigned,
    1 = map_size(Assigned),
    0 = broker_cursor:committed(Row4),
    %% The next claim sees 1 and 3 free, skips acked 2 and assigned 4.
    {ok, [1, 3], _} = broker_cursor:claim(Row4, 2, 6, <<"b">>, 4096),
    ok.

%%%_* ADR-019 §19.1/§19.2/§19.4: worker-level membership and claims ------------

%% D-13: the member order IS the failover election (ADR-019 §19.4), so it is
%% written where it can be read back -- the `.subs' body -- and nowhere else.
join_adds_a_consumer_in_order(_Config) ->
    ok = boot(),
    {ok, SubRef} = broker_subscription:join(shared, <<"g">>, <<"c1">>, <<"s/#">>, 1),
    {shared, <<"g">>, <<"s/#">>} = SubRef,
    {ok, SubRef} = broker_subscription:join(shared, <<"g">>, <<"c2">>, <<"s/#">>, 1),
    #{consumers := [<<"c1">>, <<"c2">>], kind := shared, qos := 1} =
        read_sub_body(SubRef),
    %% Both consumers see it as *their* subscription -- `?MEMBERS' is the second
    %% index over the same entity, not a second truth.
    [#{sub_ref := SubRef, kind := shared, key := <<"g">>}] =
        broker_subscription:subscriptions(<<"c2">>),
    %% A re-join only changes the QoS; it must not move the member (that order
    %% decides who is active after a death).
    {ok, SubRef} = broker_subscription:join(shared, <<"g">>, <<"c1">>, <<"s/#">>, 0),
    #{consumers := [<<"c1">>, <<"c2">>], qos := 0} = read_sub_body(SubRef),
    %% `join/5' is the group path; an exclusive kind is a caller bug, not a
    %% silent exclusive subscribe.
    {error, wrong_kind} = broker_subscription:join(exclusive, <<"g">>, <<"c3">>,
                                                   <<"s/#">>, 1),
    ok.

%% D-12: the active is the first member with a live attachment, so a death
%% promotes the next one without any membership table to update.
leave_promotes_the_next_member(_Config) ->
    ok = boot(),
    {ok, SubRef} = broker_subscription:join(failover, <<"g">>, <<"c1">>, <<"f/#">>, 1),
    {ok, SubRef} = broker_subscription:join(failover, <<"g">>, <<"c2">>, <<"f/#">>, 1),
    P1 = spawn(fun() -> timer:sleep(60000) end),
    P2 = spawn(fun() -> timer:sleep(60000) end),
    {ok, undefined} = broker_subscription:attach(<<"c1">>, P1, true),
    {ok, undefined} = broker_subscription:attach(<<"c2">>, P2, true),
    active = broker_subscription:role(SubRef, <<"c1">>),
    standby = broker_subscription:role(SubRef, <<"c2">>),
    ok = broker_metrics:reset(),
    %% The active dies. Its session survives (persist=true): only the
    %% *promotion* is the fact under test here.
    exit(P1, kill),
    ok = wait_role(SubRef, <<"c2">>),
    active = broker_subscription:role(SubRef, <<"c2">>),
    %% The counter is written by the worker when it processes the DOWN, which is
    %% one message behind the facade's own (pid-liveness based) answer.
    ok = wait_until(fun() ->
                            maps:get(failover_promotions,
                                     broker_metrics:snapshot()) =:= 1
                    end, {promotion_not_counted, SubRef}),
    %% ...and a live member that was never in the group is not a candidate.
    not_consumer = broker_subscription:role(SubRef, <<"nobody">>),
    exit(P2, kill),
    ok.

%% The last member leaving is the end of the subscription: entity, cursors,
%% Router entry and both files (ADR-019 §19.2). Keeping the SubRef would leave
%% `low' pinned by a phantom member and a dispatch target nobody consumes.
leave_of_the_last_member_removes_the_subref(_Config) ->
    ok = boot(),
    {ok, SubRef} = broker_subscription:join(shared, <<"g">>, <<"c1">>, <<"s/#">>, 1),
    {ok, SubRef} = broker_subscription:join(shared, <<"g">>, <<"c2">>, <<"s/#">>, 1),
    ok = broker_subscription:leave(SubRef, <<"c1">>),
    present = read_subs_or_none(SubRef),
    [] = broker_subscription:subscriptions(<<"c1">>),
    [#{sub_ref := SubRef}] = broker_subscription:subscriptions(<<"c2">>),
    ok = broker_subscription:leave(SubRef, <<"c2">>),
    none = read_subs_or_none(SubRef),
    [] = broker_subscription:subscriptions(<<"c2">>),
    {error, {unknown_subscription, SubRef}} = broker_subscription:cursor(SubRef, 0),
    [] = route_refs(<<"s/x">>),
    ok.

%% D-6b at the worker boundary: the high watermark comes from the storage
%% contract (`High = Last + 1`), so offset `High` itself does not exist and must
%% never be assigned -- an assignment that can never be acked pins `low' forever.
the_worker_never_claims_past_the_watermark(_Config) ->
    ok = boot(),
    {ok, SubRef} = broker_subscription:join(shared, <<"g">>, <<"c1">>, <<"s/#">>, 1),
    {ok, SubRef} = broker_subscription:join(shared, <<"g">>, <<"c2">>, <<"s/#">>, 1),
    {ok, _} = append(0, <<"s/a">>),
    {ok, _} = append(0, <<"s/b">>),
    {ok, _} = append(0, <<"s/c">>),
    {ok, _Committed, High} = broker_queue:watermarks(0),
    4 = High,
    {ok, A} = broker_subscription:claim(SubRef, 0, 100, <<"c1">>),
    [1, 2, 3] = A,
    true = lists:all(fun(O) -> O < High end, A),
    %% The tail answers an empty page, not an offset the log does not have.
    {ok, []} = broker_subscription:claim(SubRef, 0, 100, <<"c2">>),
    ok.

%% D-8: a claim is a lease tied to a live attachment. The moment the member is
%% gone its whole page returns to the pool -- this is why S2-D needs no retry
%% timer, and why a crash is a redelivery, not a lost message.
a_dead_consumer_releases_its_whole_page(_Config) ->
    ok = boot(),
    {ok, SubRef} = broker_subscription:join(shared, <<"g">>, <<"c1">>, <<"s/#">>, 1),
    {ok, SubRef} = broker_subscription:join(shared, <<"g">>, <<"c2">>, <<"s/#">>, 1),
    {ok, _} = append(0, <<"s/a">>),
    {ok, _} = append(0, <<"s/b">>),
    {ok, _} = append(0, <<"s/c">>),
    {ok, [1, 2, 3]} = broker_subscription:claim(SubRef, 0, 100, <<"c1">>),
    %% Nothing is left for c2 while c1 holds the page.
    {ok, []} = broker_subscription:claim(SubRef, 0, 100, <<"c2">>),
    Holder = spawn(fun() -> timer:sleep(60000) end),
    {ok, undefined} = broker_subscription:attach(<<"c1">>, Holder, true),
    exit(Holder, kill),
    %% The release happens on the DOWN, so the survivor's next claim is the
    %% assertion -- poll it rather than sleeping a fixed time.
    [1, 2, 3] = wait_claim(SubRef, 0, <<"c2">>),
    ok.

%% D-7: an ack that names an offset the consumer does not hold answers `ok' and
%% changes nothing. The acker is a conn_fsm whose view may lag a rebalance by one
%% message; failing it would turn a bookkeeping race into an outage.
a_non_holder_ack_counts_and_changes_nothing(_Config) ->
    ok = boot(),
    {ok, SubRef} = broker_subscription:join(shared, <<"g">>, <<"c1">>, <<"s/#">>, 1),
    {ok, SubRef} = broker_subscription:join(shared, <<"g">>, <<"c2">>, <<"s/#">>, 1),
    {ok, _} = append(0, <<"s/a">>),
    {ok, _} = append(0, <<"s/b">>),
    {ok, [1, 2]} = broker_subscription:claim(SubRef, 0, 100, <<"c1">>),
    ok = broker_metrics:reset(),
    ok = broker_subscription:ack(SubRef, 0, 1, <<"c2">>),
    {ok, 1} = broker_subscription:cursor(SubRef, 0),
    #{shared_ack_unowned := 1} = broker_metrics:snapshot(),
    %% The holder's ack is the one that counts -- in order, promoting `low' once
    %% the prefix is whole.
    ok = broker_subscription:ack(SubRef, 0, 1, <<"c1">>),
    {ok, 2} = broker_subscription:cursor(SubRef, 0),
    ok = broker_subscription:ack(SubRef, 0, 2, <<"c1">>),
    {ok, 3} = broker_subscription:cursor(SubRef, 0),
    #{shared_ack_unowned := 1} = broker_metrics:snapshot(),
    ok.

%%%_* ADR-019 §19.3: the v2 file layout and the v1 migration ------------------

%% The in-flight blob is a property of the *connection*, not of a subscription
%% (packet ids are one allocator per connection), so it stays client-keyed even
%% though `.subs'/`.cursors' moved to SubRef. ADR-019 §19.3 (D-3 in the plan).
inflight_stays_client_keyed(_Config) ->
    ok = application:set_env(broker_subscription, inflight_flush_interval_ms, 0),
    ok = boot(),
    {ok, SubRef} = broker_subscription:subscribe(<<"c1">>, <<"s/#">>, 1),
    ok = broker_subscription:inflight(<<"c1">>, #{next_pid => 7, pending => []}),
    Path = broker_subscription_files:inflight_path(<<"c1">>),
    true = filelib:is_regular(Path),
    %% The name is the client's b64 and nothing else: no kind segment, no
    %% filter -- and it is the same file the v1 layout produced.
    Base = filename:basename(Path),
    true = lists:suffix(".inflight", Base),
    Stem = filename:rootname(Base, ".inflight"),
    ClientId = <<"c1">>,
    Stem = binary_to_list(base64:encode(ClientId, #{mode => urlsafe, padding => false})),
    %% The subscription's own files are SubRef-keyed and *not* named after the
    %% client stem.
    SubsPath = broker_subscription_files:subs_path(SubRef),
    false = filename:basename(SubsPath) =:= filename:basename(Path),
    true = filelib:is_regular(SubsPath),
    {ok, #{next_pid := 7}} = broker_subscription_files:read_inflight(<<"c1">>),
    ok.

%% D-15: the S1 layout (`{vsn,1,ClientId,Body}', one file family per client) is
%% rewritten in place at boot -- the subscription *and* its cursor survive, the
%% v1 files are gone, the v2 files are what boot step 2 read.
migration_rewrites_v1_subs_and_cursors(_Config) ->
    write_v1(<<"c1">>,
             [{<<"a/#">>, 1, [{0, 1}, {1, 1}]}],
             [{<<"a/#">>, [{0, 4}]}]),
    ok = boot(),
    [#{sub_ref := SubRef, kind := exclusive, filter := <<"a/#">>, qos := 1}] =
        broker_subscription:subscriptions(<<"c1">>),
    {exclusive, <<"c1">>, <<"a/#">>} = SubRef,
    %% The v1 `.cursors' row wins for P0 (K5/K6 order); P1 falls back to the
    %% init anchor carried in `.subs'.
    {ok, 4} = broker_subscription:cursor(SubRef, 0),
    {ok, 1} = broker_subscription:cursor(SubRef, 1),
    false = filelib:is_regular(v1_subs_path(<<"c1">>)),
    false = filelib:is_regular(v1_cursors_path(<<"c1">>)),
    true = filelib:is_regular(broker_subscription_files:subs_path(SubRef)),
    true = filelib:is_regular(broker_subscription_files:cursors_path(SubRef)),
    %% The migration is complete enough that a second boot finds nothing to do.
    ok = bounce_subscription_app(),
    {ok, 4} = broker_subscription:cursor(SubRef, 0),
    ok.

%% D-16: a crash between "v2 written" and "v1 deleted" leaves both families on
%% disk. The next boot redoes the same writes and the same deletes -- the bytes
%% the second pass produces are identical to the first pass's.
migration_is_idempotent_after_a_partial_write(_Config) ->
    write_v1(<<"c1">>, [{<<"a/#">>, 1, [{0, 1}]}], [{<<"a/#">>, [{0, 2}]}]),
    SubRef = {exclusive, <<"c1">>, <<"a/#">>},
    %% Pass 1 runs *without* booting: exactly the state a crash after the write
    %% leaves behind (both families present, v1 not yet unlinked).
    ok = broker_subscription_files:migrate_v1(),
    First = read_sub_body(SubRef),
    false = filelib:is_regular(v1_subs_path(<<"c1">>)),
    %% Recreate the v1 files: this is the crash residue the next boot sees.
    write_v1(<<"c1">>, [{<<"a/#">>, 1, [{0, 1}]}], [{<<"a/#">>, [{0, 2}]}]),
    ok = broker_subscription_files:migrate_v1(),
    Second = read_sub_body(SubRef),
    %% Rewriting the same subscription produces the same record (`created_at'
    %% is the one field the clock owns, so it is compared out of the body).
    true = maps:remove(created_at, First) =:= maps:remove(created_at, Second),
    false = filelib:is_regular(v1_subs_path(<<"c1">>)),
    false = filelib:is_regular(v1_cursors_path(<<"c1">>)),
    %% And the end state boots and serves the same cursor.
    ok = boot(),
    {ok, 2} = broker_subscription:cursor(SubRef, 0),
    ok.

%% A v1 file that does not decode is a boot refusal, not a skip: booting without
%% a subscription that exists on disk serves `not_found' for a live session
%% (ADR-011 §11.2). The v1 file is *not* deleted -- the operator still has the
%% evidence of what broke.
migration_refuses_to_boot_on_a_corrupt_v1_file(_Config) ->
    %% Boot once cleanly so every dependency is up, then plant the corrupt v1
    %% pair where the next boot's migration will find it.
    ok = boot(),
    ok = application:stop(broker_subscription),
    Path = write_v1(<<"c1">>, not_a_list_of_entries, none),
    {error, {{shutdown,
              {failed_to_start_child, broker_subscription_worker,
               {subscription_boot_failed,
                {session_scan,
                 {migration, Path,
                  {corrupt_v1_subs, Path, <<"c1">>, not_a_list_of_entries}}}}}},
             _}} =
        start_subscription(),
    undefined = whereis(broker_subscription_worker),
    %% The v1 file is *not* deleted: the operator still has the evidence.
    true = filelib:is_regular(Path),
    ok.

%% An orphan `.cursors' is the residue of a teardown that died between two
%% unlinks. Keeping it would let a later subscription that happens to mint the
%% same SubRef resume from a dead one's position.
orphan_cursors_files_are_removed_at_boot(_Config) ->
    OkDir = sessions_dir(),
    ok = filelib:ensure_dir(filename:join(OkDir, "x")),
    Orphan = {shared, <<"g">>, <<"o/#">>},
    ok = broker_subscription_files:write_cursors(
           Orphan, #{kind => shared, rows => #{0 => broker_cursor:new_shared(1)}}),
    true = filelib:is_regular(broker_subscription_files:cursors_path(Orphan)),
    ok = boot(),
    false = filelib:is_regular(broker_subscription_files:cursors_path(Orphan)),
    %% ...and a live pair is left alone: the sweep only removes cursors whose
    %% `.subs' is gone.
    {ok, SubRef} = broker_subscription:subscribe(<<"c1">>, <<"s/#">>, 1),
    ok = broker_subscription:ack(SubRef, 0, 1, <<"c1">>),
    timer:sleep(200),
    ok = bounce_subscription_app(),
    true = filelib:is_regular(broker_subscription_files:cursors_path(SubRef)),
    ok.

%%%_* ADR-019 §19.2/plan §2.5: dispatch recipients by kind --------------------

%% A QoS0 publish never enters storage, so there is no offset to claim: the push
%% IS the delivery, and pushing to every member would deliver N copies.
a_qos0_publish_reaches_exactly_one_shared_member(_Config) ->
    ok = boot(),
    {ok, SubRef} = broker_subscription:join(shared, <<"g">>, <<"c1">>, <<"s/#">>, 0),
    {ok, SubRef} = broker_subscription:join(shared, <<"g">>, <<"c2">>, <<"s/#">>, 0),
    {ok, undefined} = broker_subscription:attach(<<"c1">>, self(), true),
    {ok, undefined} = broker_subscription:attach(<<"c2">>, self(), true),
    Refs = route_refs(<<"s/x">>),
    [SubRef] = Refs,
    ok = broker_subscription:dispatch(Refs, {undefined, undefined, msg(<<"s/x">>)}),
    receive
        {broker_deliver, ClientId, 0, [SubRef], {undefined, undefined, _}} ->
            %% The lowest-ordered attached member, and only that one.
            <<"c1">> = ClientId,
            no_more = receive {broker_deliver, _, _, _, _} -> more
                      after 100 -> no_more end
    after 1000 -> exit(qos0_push_not_delivered) end,
    ok.

%% A durable wake is only a hint: `claim/4' decides who reads the offset, so
%% every attached member is told. Telling one would stall the group whenever that
%% member is slow (ADR-019 §19.2).
a_durable_wake_reaches_every_shared_member(_Config) ->
    ok = boot(),
    {ok, SubRef} = broker_subscription:join(shared, <<"g">>, <<"c1">>, <<"s/#">>, 1),
    {ok, SubRef} = broker_subscription:join(shared, <<"g">>, <<"c2">>, <<"s/#">>, 1),
    {ok, undefined} = broker_subscription:attach(<<"c1">>, self(), true),
    {ok, undefined} = broker_subscription:attach(<<"c2">>, self(), true),
    Refs = route_refs(<<"s/x">>),
    ok = broker_subscription:dispatch(Refs, {0, 7, msg(<<"s/x">>)}),
    Got = collect_deliveries(2, []),
    [<<"c1">>, <<"c2">>] = lists:sort(Got),
    ok.

%%%_* S2-E: retry, DLQ and TTL (ADR-015) -------------------------------------

%% E-1: the retry row is pure algebra. `attempts' counts failures, `due/3' and
%% `blocked/3' partition the retry map by `next_at', and an ack ends the story.
retry_algebra_schedules_and_ack_clears(_Config) ->
    R0 = broker_cursor:new_shared(1),
    {ok, [1], R1} = broker_cursor:claim(R0, 1, 100, <<"a">>, 4096),
    R2 = broker_cursor:retry(broker_cursor:release_offset(R1, 1), 1, 1000, 10,
                             ack_timeout),
    1 = broker_cursor:attempts(R2, 1),
    %% Still in backoff: not due, and a claim must skip it.
    [] = broker_cursor:due(R2, 500, 100),
    #{1 := true} = broker_cursor:blocked(R2, 500, 100),
    {ok, [], _} = broker_cursor:claim(R2, 1, 2, <<"b">>, 4096,
                                      broker_cursor:blocked(R2, 500, 100)),
    %% Due once the clock reaches next_at.
    [1] = broker_cursor:due(R2, 1000, 100),
    #{} = broker_cursor:blocked(R2, 1000, 100),
    R3 = broker_cursor:retry(R2, 1, 2000, 1000, ack_timeout),
    2 = broker_cursor:attempts(R3, 1),
    %% Paused (handed to a connection) is neither due nor blocked.
    R4 = broker_cursor:pause(R3, [1]),
    [] = broker_cursor:due(R4, 5000, 100),
    #{} = broker_cursor:blocked(R4, 5000, 100),
    %% Releasing the holder unpauses it immediately.
    R5 = broker_cursor:release_offset(R4#{assigned := #{1 => <<"c">>}}, 1),
    [1] = broker_cursor:due(R5, 5000, 100),
    %% An ack clears the retry entry.
    R6 = R5#{assigned := #{1 => <<"a">>}},
    {ok, R7} = broker_cursor:ack_shared(R6, 1, <<"a">>),
    0 = broker_cursor:attempts(R7, 1),
    ok.

%% E-3: §15.2's curve, asserted with no clock.
backoff_sequence_is_exponential_and_capped(_Config) ->
    [1000, 2000, 4000, 8000, 16000, 32000, 60000, 60000] =
        [broker_cursor:backoff(N, 1000, 60000) || N <- lists:seq(1, 8)],
    %% A huge attempt count must not overflow the shift.
    60000 = broker_cursor:backoff(1000000, 1000, 60000),
    ok.

%% E-2/E-5: a timed-out shared offset is released, parked for its backoff and
%% counted in the window while it waits.
shared_retry_waits_for_the_backoff_before_reclaim(_Config) ->
    ok = application:set_env(broker_subscription, retry_base_ms, 400),
    ok = application:set_env(broker_subscription, retry_tick_ms, 3600000),
    ok = boot(),
    {ok, SubRef} = broker_subscription:join(shared, <<"g">>, <<"c1">>, <<"s/#">>, 1),
    {ok, SubRef} = broker_subscription:join(shared, <<"g">>, <<"c2">>, <<"s/#">>, 1),
    {ok, Offset} = append(0, <<"s/a">>),
    {ok, [Offset]} = broker_subscription:claim(SubRef, 0, 100, <<"c1">>),
    ok = broker_subscription:retry(SubRef, 0, Offset, <<"c1">>),
    %% Within the backoff the offset is nobody's to claim ...
    {ok, []} = broker_subscription:claim(SubRef, 0, 100, <<"c2">>),
    timer:sleep(500),
    %% ... and once it elapses the survivor gets it, with the attempt count kept
    %% (a later timeout is attempt 2, not 1).
    {ok, [Offset]} = broker_subscription:claim(SubRef, 0, 100, <<"c2">>),
    ok.

%% E-4/E-5: `max_attempts' failures dead-letter the message, advance the cursor
%% (so the partition is not pinned) and append a normal, fsynced, TTL-free copy.
max_attempts_dead_letters_and_advances(_Config) ->
    ok = application:set_env(broker_subscription, max_attempts, 2),
    ok = application:set_env(broker_subscription, retry_base_ms, 60000),
    ok = application:set_env(broker_subscription, retry_tick_ms, 3600000),
    ok = boot(),
    ok = broker_metrics:reset(),
    {ok, SubRef} = broker_subscription:subscribe(<<"c1">>, <<"s/#">>, 1),
    {ok, Offset} = append(0, <<"s/a">>),
    %% Failure 1: schedule a retry, cursor stays.
    ok = broker_subscription:retry(SubRef, 0, Offset, <<"c1">>),
    {ok, 1} = broker_subscription:cursor(SubRef, 0),
    #{dlq_appends := 0} = broker_metrics:snapshot(),
    %% Failure 2 reaches the budget: dead letter + advance.
    ok = broker_subscription:retry(SubRef, 0, Offset, <<"c1">>),
    {ok, 2} = broker_subscription:cursor(SubRef, 0),
    Dest = <<"$dlq/t0/s/a">>,
    Copy = find_message(dlq_partition(Dest), Dest),
    <<"p">> = Copy#broker_message.payload,
    undefined = Copy#broker_message.expires_at,
    fsync = Copy#broker_message.reliability,
    durable = Copy#broker_message.persistence,
    #{<<"x-original-message-id">> := <<"m">>,
      <<"x-original-destination">> := <<"s/a">>,
      <<"x-attempts">> := <<"2">>,
      <<"x-tenant">> := <<"t0">>,
      <<"x-failure-reason">> := <<"max_attempts">>} = Copy#broker_message.headers,
    true = byte_size(Copy#broker_message.message_id) =:= 16,
    #{dlq_appends := 1, dlq_size := 1} = broker_metrics:snapshot(),
    ok.

%% E-7: the DLQ is a destination, not a subsystem: an ordinary `$dlq/#' filter
%% indexes it in the Router exactly like any other topic.
a_dead_letter_is_an_ordinary_subscribable_destination(_Config) ->
    ok = boot(),
    {ok, SubRef} = broker_subscription:subscribe(<<"audit">>, <<"$dlq/#">>, 1),
    Ref = {exclusive, <<"audit">>, <<"$dlq/#">>},
    SubRef = Ref,
    true = lists:member(Ref, route_refs(<<"$dlq/t0/orders/new">>)),
    false = lists:member(Ref, route_refs(<<"orders/new">>)),
    ok.

%% E-6: template validation is boot-time, and `name/2' is literal substitution.
dlq_template_validation_and_rendering(_Config) ->
    Default = <<"$dlq/{tenant}/{original}">>,
    ok = broker_dlq:validate_pattern(Default),
    ok = broker_dlq:validate_pattern(<<"dead/{kind}/{tenant}/{original}/{qos}">>),
    {error, wildcard_in_pattern} = broker_dlq:validate_pattern(<<"$dlq/+/x">>),
    {error, wildcard_in_pattern} = broker_dlq:validate_pattern(<<"$dlq/#">>),
    {error, {unknown_variable, <<"nope">>}} =
        broker_dlq:validate_pattern(<<"$dlq/{nope}/x">>),
    {error, unterminated_variable} = broker_dlq:validate_pattern(<<"$dlq/{tenant">>),
    Bindings = #{tenant => <<"t0">>, original => <<"orders/new">>,
                 qos => 1, kind => shared},
    <<"$dlq/t0/orders/new">> = broker_dlq:name(Default, Bindings),
    <<"dead/shared/t0/orders/new/1">> =
        broker_dlq:name(<<"dead/{kind}/{tenant}/{original}/{qos}">>, Bindings),
    ok.

%% E-6's boot half: a template that could never be published is refused at
%% start, not on the first dead letter.
a_bad_dlq_template_refuses_boot(_Config) ->
    ok = application:set_env(broker_subscription, dlq_name_pattern,
                             <<"$dlq/+/x">>),
    [ok = broker_queue:create_partition(P) || P <- broker_queue:partition_ids()],
    {error, {broker_subscription,
             {{shutdown, {failed_to_start_child, broker_subscription_worker,
                          {invalid_dlq_pattern, wildcard_in_pattern}}},
              {broker_subscription_app, start, [normal, []]}}}} =
        application:ensure_all_started(broker_subscription),
    undefined = whereis(broker_subscription_worker),
    ok.

%% E-8: a dead-letter append that fails must not consume the cursor (§15.5).
%% The original here is an offset the log does not have, which is the honest way
%% to make the copy impossible without breaking the partition.
a_failed_dead_letter_keeps_the_cursor(_Config) ->
    ok = application:set_env(broker_subscription, max_attempts, 1),
    ok = application:set_env(broker_subscription, retry_tick_ms, 3600000),
    ok = boot(),
    ok = broker_metrics:reset(),
    {ok, SubRef} = broker_subscription:subscribe(<<"c1">>, <<"s/#">>, 1),
    ok = broker_subscription:retry(SubRef, 0, 0, <<"c1">>),
    {ok, 1} = broker_subscription:cursor(SubRef, 0),
    #{dlq_append_failed := Failed} = broker_metrics:snapshot(),
    true = Failed >= 1,
    #{dlq_appends := 0} = broker_metrics:snapshot(),
    ok.

%% E-9: retries are in the cursor row, so a restart keeps the attempt budget.
%% If the retry state were connection-local, the second failure below would be
%% attempt 1 again and the message would not be dead-lettered.
retry_progress_survives_a_restart(_Config) ->
    ok = application:set_env(broker_subscription, max_attempts, 2),
    ok = application:set_env(broker_subscription, retry_base_ms, 60000),
    ok = application:set_env(broker_subscription, retry_tick_ms, 3600000),
    ok = application:set_env(broker_subscription, cursor_flush_interval_ms, 20),
    ok = boot(),
    {ok, SubRef} = broker_subscription:subscribe(<<"c1">>, <<"s/#">>, 1),
    {ok, Offset} = append(0, <<"s/a">>),
    ok = broker_subscription:retry(SubRef, 0, Offset, <<"c1">>),
    {ok, 1} = broker_subscription:cursor(SubRef, 0),
    %% Let the cursor tick put the `retries' map on disk.
    timer:sleep(150),
    ok = bounce_subscription_app(),
    ok = broker_subscription:retry(SubRef, 0, Offset, <<"c1">>),
    {ok, 2} = broker_subscription:cursor(SubRef, 0),
    Dest = <<"$dlq/t0/s/a">>,
    _Copy = find_message(dlq_partition(Dest), Dest),
    ok.

%%%_* Helpers -------------------------------------------------------------------

append(Partition, Dest) ->
    append(Partition, Dest, fsync).

append(Partition, Dest, Mode) ->
    broker_queue:append(Partition, msg(Dest), Mode).

msg(Dest) ->
    #broker_message{message_id = <<"m">>, tenant_id = <<"t0">>,
                    destination = Dest, partition_id = 0,
                    payload = <<"p">>, payload_encoding = raw,
                    headers = #{}, proto_meta = #{},
                    persistence = durable, reliability = fsync,
                    created_at = erlang:system_time(millisecond)}.

%% The DLQ destination is routed by the same oracle a publish uses (ADR-010).
dlq_partition(Dest) ->
    {ok, Env} = broker_router:env(),
    {ok, [{P, _Refs} | _]} = broker_router_hash:route(Dest, #{}, Env),
    P.

find_message(P, Dest) ->
    find_message(P, Dest, 0).

find_message(P, Dest, From) ->
    case broker_queue:read_from(P, From, 100) of
        {ok, Page, Next} when is_list(Page), Page =/= [] ->
            case [M || {_O, M} <- Page, M#broker_message.destination =:= Dest] of
                [M | _] -> M;
                [] when Next > From -> find_message(P, Dest, Next);
                _ -> exit({no_message_on_destination, P, Dest})
            end;
        _ ->
            exit({no_message_on_destination, P, Dest})
    end.

route_refs(Dest) ->
    {ok, Env} = broker_router:env(),
    {ok, [{_Partition, Refs}]} = broker_router_hash:route(Dest, #{}, Env),
    Refs.

await_worker_restart() ->
    case whereis(broker_subscription_worker) of
        undefined -> timer:sleep(10), await_worker_restart();
        _Pid -> ok
    end.

sessions_dir() ->
    %% Only used after boot/0 with the fixture's data_dir.
    broker_subscription_files:sessions_dir().

read_subs_or_none(SubRef) ->
    case broker_subscription_files:read_subscription(SubRef) of
        {ok, none} -> none;
        {ok, _} -> present
    end.

read_sub_body(SubRef) ->
    {ok, Body} = broker_subscription_files:read_subscription(SubRef),
    Body.

%% A live attachment is what makes a member a failover candidate; the worker
%% learns about the death from the DOWN, so the assertion polls.
wait_role(SubRef, ClientId) ->
    _ = wait_until(fun() -> broker_subscription:role(SubRef, ClientId) =:= active end,
                   {role_never_became_active, SubRef, ClientId}),
    ok.

wait_claim(SubRef, Partition, Consumer) ->
    wait_until(fun() ->
                       case broker_subscription:claim(SubRef, Partition, 100, Consumer) of
                           {ok, []} -> false;
                           {ok, Offsets} -> Offsets;
                           _ -> false
                       end
               end, {claim_never_succeeded, SubRef, Partition, Consumer}).

wait_until(Fun, Error) ->
    wait_until(Fun, Error, 200).

wait_until(_Fun, Error, 0) ->
    exit(Error);
wait_until(Fun, Error, N) ->
    case Fun() of
        false ->
            timer:sleep(20),
            wait_until(Fun, Error, N - 1);
        true ->
            ok;
        Other ->
            Other
    end.

collect_deliveries(0, Acc) ->
    Acc;
collect_deliveries(N, Acc) ->
    receive
        {broker_deliver, ClientId, _QoS, _Refs, _Delivery} ->
            collect_deliveries(N - 1, [ClientId | Acc])
    after 1000 ->
            exit({missing_deliveries, N, Acc})
    end.

%% The S1 on-disk layout, written by hand: `{vsn, 1, ClientId, Body}' where the
%% body is `[{Filter, QoS, InitCursors}]' and the cursor body is
%% `[{Filter, [{Partition, Offset}]}]'. Boot's `migrate_v1/0' is the only reader.
v1_subs_path(ClientId) ->
    filename:join(sessions_dir(), stem(ClientId) ++ ".subs").

v1_cursors_path(ClientId) ->
    filename:join(sessions_dir(), stem(ClientId) ++ ".cursors").

stem(ClientId) ->
    binary_to_list(base64:encode(ClientId, #{mode => urlsafe, padding => false})).

start_subscription() ->
    case application:start(broker_subscription) of
        ok -> ok;
        {error, _} = Error -> Error
    end.

%% Writes one v1 client family. `Entries' is the `.subs' body (or a deliberately
%% corrupt value); `CursorEntries' is `none' when there is no `.cursors' at all.
write_v1(ClientId, Entries, CursorEntries) ->
    ok = filelib:ensure_dir(filename:join(sessions_dir(), "x")),
    ok = file:write_file(v1_subs_path(ClientId),
                         term_to_binary({vsn, 1, ClientId, Entries})),
    case CursorEntries of
        none -> ok;
        _ -> ok = file:write_file(v1_cursors_path(ClientId),
                                  term_to_binary({vsn, 1, ClientId, CursorEntries}))
    end,
    v1_subs_path(ClientId).

