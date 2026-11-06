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
     the_memory_backend_works_through_the_same_api].

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
    Path = broker_subscription_files:subs_path(<<"c1">>),
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
    ok = broker_subscription_files:write_subs(<<"c1">>,
                                              [{<<"a/b">>, 1, [{0, 1}, {1, 1}]}]),
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
    {<<"c1">>, <<"sport/#">>} = SubRef,
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
    ok = broker_subscription:ack(SubRef, 0, 2),
    ok = broker_subscription:ack(SubRef, 0, 3),
    {ok, 1} = broker_subscription:cursor(SubRef, 0),
    ok = broker_subscription:ack(SubRef, 0, 1),
    {ok, 4} = broker_subscription:cursor(SubRef, 0),
    ok.

an_ack_behind_the_cursor_is_a_noop(_Config) ->
    ok = boot(),
    {ok, SubRef} = broker_subscription:subscribe(<<"c1">>, <<"sport/#">>, 1),
    ok = broker_subscription:ack(SubRef, 0, 1),
    {ok, 2} = broker_subscription:cursor(SubRef, 0),
    ok = broker_subscription:ack(SubRef, 0, 1),
    {ok, 2} = broker_subscription:cursor(SubRef, 0),
    ok.

acking_a_dead_subscription_is_idempotent_ok(_Config) ->
    ok = boot(),
    {ok, SubRef} = broker_subscription:subscribe(<<"c1">>, <<"gone">>, 1),
    ok = broker_subscription:unsubscribe(<<"c1">>, <<"gone">>),
    %% A conn_fsm racing its own UNSUBSCRIBE must not get an exit-shaped
    %% protocol error for one in-flight ack.
    ok = broker_subscription:ack(SubRef, 0, 1),
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
    ok = broker_subscription:ack(A, 0, 1),
    %% B still sits at 1: min is 1, committed must not move past 0.
    {ok, Committed, _High} = broker_queue:watermarks(0),
    0 = Committed,
    ok = broker_subscription:ack(B, 0, 1),
    {ok, 1, _} = broker_queue:watermarks(0),
    ok = broker_subscription:ack(A, 0, 2),
    ok = broker_subscription:ack(B, 0, 2),
    {ok, 2, _} = broker_queue:watermarks(0),
    ok.

flushed_cursors_back_the_watermark_it_proves(_Config) ->
    ok = application:set_env(broker_subscription, cursor_flush_interval_ms, ?LONG_FLUSH_MS),
    ok = application:set_env(broker_subscription, cursor_max_regress, 4),
    ok = boot(),
    {ok, SubRef} = broker_subscription:subscribe(<<"c1">>, <<"sport/#">>, 1),
    [_, _, _, _] = [element(2, append(0, <<"sport/x">>)) || _ <- [1, 2, 3, 4]],
    [ok = broker_subscription:ack(SubRef, 0, O) || O <- [1, 2, 3]],
    %% Not yet flushed (3 < 4, no tick): the watermark must not have crossed
    %% what the *disk* can vouch for -- the K6 ordering, seen from the side
    %% where breaking it would make S2's truncation delete un-acked data.
    {ok, Committed, _} = broker_queue:watermarks(0),
    0 = Committed,
    ok = broker_subscription:ack(SubRef, 0, 4),
    {ok, CommittedNow, _} = broker_queue:watermarks(0),
    4 = CommittedNow,
    {ok, Flushed} = broker_subscription_files:read_cursors(<<"c1">>),
    [{<<"sport/#">>, Rows}] = Flushed,
    {0, 5} = lists:keyfind(0, 1, Rows),
    ok.

the_zero_regress_tier_flushes_every_ack(_Config) ->
    ok = application:set_env(broker_subscription, cursor_flush_interval_ms, 0),
    ok = boot(),
    {ok, SubRef} = broker_subscription:subscribe(<<"c1">>, <<"sport/#">>, 1),
    ok = broker_subscription:ack(SubRef, 0, 1),
    {ok, _} = broker_subscription_files:read_cursors(<<"c1">>),
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
    ok = broker_subscription:ack(SubRef, 0, 1),
    ok = broker_subscription:ack(SubRef, 0, 2),
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
    ok = broker_subscription:ack(SubRef, 0, 1),
    ok = broker_subscription:ack(SubRef, 0, 2),
    timer:sleep(300),
    {ok, 2, _} = broker_queue:watermarks(0),
    %% Now *lose* the cursor file (simulate the worst restore: `.cursors' gone,
    %% `.subs' survived). The K5 rule: fall back to the init anchor embedded in
    %% `.subs' (here 1), never to `Committed' (2 now -- taking it would *skip*
    %% offsets 1..2 the client may still need, and taking 0 would rewind behind
    %% the subscription). In S1 nothing is truncated yet, so replaying from the
    %% origin costs duplicates only -- which §4.1 allows and R7 expects.
    ok = file:delete(broker_subscription_files:cursors_path(<<"c1">>)),
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
    ok = broker_subscription:ack(Online, 0, 1),
    {ok, Pinned, _} = broker_queue:watermarks(0),
    0 = Pinned,
    %% Catching up *online* or offline is the same ack path (T9's conn_fsm
    %% would replay from the cursor after a real reconnect).
    ok = broker_subscription:ack(Offline, 0, 1),
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
    none = read_subs_or_none(<<"c1">>),
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
    ok = broker_subscription:ack(WalRef, 0, 1),
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
    ok = broker_subscription:ack(MemRef, 0, 1),
    timer:sleep(300),
    {ok, 1, 2} = broker_queue:watermarks(0),
    {ok, 2} = broker_subscription:cursor(MemRef, 0),
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
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

read_subs_or_none(ClientId) ->
    case broker_subscription_files:read_subs(ClientId) of
        {ok, none} -> none;
        {ok, _} -> present
    end.
