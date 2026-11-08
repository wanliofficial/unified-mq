%% @doc T5: the per-partition WAL writer -- group commit, fsync-before-reply,
%% rotation and the durability guard (ADR-011 §11.4/§11.5, design §7.2).
%%
%% What is pinned here and nowhere else:
%%
%%  * **the reply is the durability promise.** An fsync caller must not be told
%%    "ok" before the bytes are on the platter, and a batch of them must pay for
%%    ONE fsync rather than one each -- that single fact is the 17x S0.3 §2.2
%%    measured, and it is invisible in the return value, so it is asserted
%%    through the writer's own counters.
%%  * **offsets stay dense and unique under concurrency** even though they are
%%    allocated by one process, because allocation happens before the caller is
%%    made to wait for the flush.
%%  * **rotation anchors the first record of the new segment.** Without it the
%%    sparse index hands `read_from' a file position that belongs to the previous
%%    segment, and the read silently returns another partition's worth of bytes.
%%  * **a volatile filesystem refuses to start.** S0.3 §4 wanted a timing
%%    heuristic for this; a fast NVMe with power-loss protection would fail it,
%%    so the guard reads the mount table instead.
-module(wal_writer_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("broker_core/include/broker_message.hrl").

all() ->
    [append_returns_dense_offsets_and_the_records_reach_the_disk,
     a_batch_that_owes_fsync_pays_for_one_sync,
     every_fsync_caller_sees_the_sync_before_its_reply,
     os_buffer_appends_never_sync,
     a_mixed_batch_syncs_and_answers_everyone,
     offsets_are_unique_and_contiguous_under_concurrency,
     advance_committed_survives_a_killed_writer,
     fsynced_records_survive_a_killed_writer,
     rotation_starts_a_new_segment_at_the_next_offset,
     every_segment_anchors_its_first_record,
     truncate_is_persisted_with_the_committed_watermark,
     close_marks_the_partition_closed_and_refuses_further_writes,
     a_closed_writer_reopens_on_the_same_log,
     a_stopping_writer_answers_its_queued_callers_instead_of_dropping_them,
     append_rejects_a_mode_outside_the_capability_without_consuming_an_offset,
     appending_above_the_pending_limit_is_refused_without_consuming_an_offset,
     the_guard_refuses_a_volatile_mount_and_allows_a_durable_one,
     the_guard_can_be_waived_and_then_says_so,
     classify_fs_matches_the_longest_mount_on_a_path_boundary,
     classify_fs_reports_unknown_when_no_mount_matches].

%% CT gives every testcase in a suite the SAME priv_dir, so each case gets its
%% own subdirectory -- and that subdirectory is the partition under test.
init_per_testcase(Case, Config) ->
    Dir = filename:join(proplists:get_value(priv_dir, Config), atom_to_list(Case)),
    ok = wal_recovery:create(Dir),
    [{wal_dir, Dir} | Config].

end_per_testcase(_Case, _Config) ->
    case get(writer) of
        Pid when is_pid(Pid) -> catch wal_writer:stop(Pid);
        _ -> ok
    end,
    ok.

%%%_* Append and durability ----------------------------------------------

append_returns_dense_offsets_and_the_records_reach_the_disk(Config) ->
    W = start(Config, #{}),
    {ok, [1, 2, 3]} =
        wal_writer:append(W, [msg(<<"a">>), msg(<<"b">>), msg(<<"c">>)], os_buffer),
    #{high := 4, committed := 0, truncate_before := 1} = wal_writer:state(W),
    ok = wal_writer:stop(W),
    {ok, #{high := 4, warnings := []}} = wal_recovery:recover(1, priv(Config), #{}),
    ok.

%% Ten callers, one 100 ms window, one fsync. The delay is exaggerated so the
%% assertion does not depend on the scheduler being fast -- and so that a writer
%% which synced per message would be caught by the counter, not by a timing race.
a_batch_that_owes_fsync_pays_for_one_sync(Config) ->
    W = start(Config, #{batch_delay_ms => #{os_buffer => 0, fsync => 100}}),
    Results = concurrent_appends(W, 10, 1, fsync),
    Expected = lists:seq(1, 10),
    Expected = lists:sort(lists:flatten(Results)),
    #{appends := 10, records := 10, batches := Batches, syncs := Syncs} =
        wal_writer:stats(W),
    %% One window is enough for all ten to queue up. Two covers a straggler that
    %% missed it; ten would mean the batching never happened at all.
    true = Batches =< 2,
    true = Syncs =< 2,
    ok.

%% §11.4's promise, stated as an observation each caller can make for itself:
%% by the time its own append returned, at least one fsync had happened.
every_fsync_caller_sees_the_sync_before_its_reply(Config) ->
    W = start(Config, #{batch_delay_ms => #{os_buffer => 0, fsync => 20}}),
    Parent = self(),
    Pids = [spawn_link(fun() ->
                           {ok, [_]} = wal_writer:append(W, [msg(<<"d">>)], fsync),
                           Parent ! {self(), maps:get(syncs, wal_writer:stats(W))}
                       end)
            || _ <- lists:seq(1, 8)],
    Seen = [receive
                {Pid, Syncs} -> Syncs
            after 10000 -> ct:fail({no_stats_from, Pid})
            end
            || Pid <- Pids],
    8 = length(Seen),
    lists:foreach(fun(Syncs) -> true = Syncs >= 1 end, Seen),
    ok.

os_buffer_appends_never_sync(Config) ->
    W = start(Config, #{}),
    {ok, [1, 2]} = wal_writer:append(W, [msg(<<"a">>), msg(<<"b">>)], os_buffer),
    {ok, [3]} = wal_writer:append(W, [msg(<<"c">>)], os_buffer),
    #{syncs := 0, batches := 2, records := 3, rotations := 0} = wal_writer:stats(W),
    ok.

%% An os_buffer caller that arrives while an fsync deadline is pending rides
%% along in that batch and is answered after the sync -- stronger than it was
%% promised, so the batch is not split to avoid it.
a_mixed_batch_syncs_and_answers_everyone(Config) ->
    W = start(Config, #{batch_delay_ms => #{os_buffer => 0, fsync => 100}}),
    Parent = self(),
    Spawn = fun(Mode, N) ->
                    [spawn_link(fun() ->
                         Parent ! {self(),
                                   wal_writer:append(W, [msg(atom_to_binary(Mode))], Mode)}
                     end)
                     || _ <- lists:seq(1, N)]
            end,
    Pids = Spawn(fsync, 4) ++ Spawn(os_buffer, 4),
    Offsets = [receive
                   {Pid, {ok, Offs}} -> Offs;
                   {Pid, Other} -> ct:fail({append_failed, Pid, Other})
               after 10000 -> ct:fail({no_reply_from, Pid})
               end
               || Pid <- Pids],
    Expected = lists:seq(1, 8),
    Expected = lists:sort(lists:flatten(Offsets)),
    #{syncs := Syncs} = wal_writer:stats(W),
    true = Syncs >= 1,
    ok.

%% README §7.2 rules out a single global bottleneck, not a per-partition writer:
%% allocation is what has to stay gap-free, and it happens before the wait.
offsets_are_unique_and_contiguous_under_concurrency(Config) ->
    W = start(Config, #{}),
    Results = concurrent_appends(W, 50, 3, os_buffer),
    Expected = lists:seq(1, 150),
    Expected = lists:sort(lists:flatten(Results)),
    #{records := 150} = wal_writer:stats(W),
    ok.

%%%_* Crash behaviour ----------------------------------------------------

advance_committed_survives_a_killed_writer(Config) ->
    W = start(Config, #{}),
    {ok, [1, 2]} = wal_writer:append(W, [msg(<<"a">>), msg(<<"b">>)], os_buffer),
    ok = wal_writer:advance_committed(W, 2),
    kill(W),
    {ok, St} = wal_recovery:recover(1, priv(Config), #{}),
    #{high := 3, committed := 2, truncate_before := 1, warnings := []} = St,
    ok.

%% Honest scope: killing the BEAM process cannot prove os_buffer durability,
%% because the OS still holds those pages (S0.3 §3 said the same about kill -9).
%% What it does prove is that an fsync append reached the disk before its caller
%% was answered -- the only direction §11.4 cares about.
fsynced_records_survive_a_killed_writer(Config) ->
    W = start(Config, #{}),
    {ok, [1, 2, 3]} =
        wal_writer:append(W, [msg(<<"a">>), msg(<<"b">>), msg(<<"c">>)], fsync),
    kill(W),
    {ok, #{high := 4, warnings := []}} = wal_recovery:recover(1, priv(Config), #{}),
    ok.

%%%_* Segments and the sparse index --------------------------------------

%% One batch of seven with three records per segment: rotation has to happen in
%% the middle of a flush, not only between them.
rotation_starts_a_new_segment_at_the_next_offset(Config) ->
    W = start(Config, #{segment_max_records => 3}),
    Msgs = [msg(P) || P <- [<<"a">>, <<"b">>, <<"c">>, <<"d">>, <<"e">>, <<"f">>, <<"g">>]],
    {ok, Offsets} = wal_writer:append(W, Msgs, os_buffer),
    Expected = lists:seq(1, 7),
    Expected = Offsets,
    #{rotations := 2} = wal_writer:stats(W),
    ok = wal_writer:stop(W),
    {ok, St} = wal_recovery:recover(1, priv(Config), #{}),
    #{high := 8} = St,
    [{1, _}, {4, _}, {7, _}] = maps:get(segments, St),
    ok.

%% The reason an anchor carries its segment path: without the per-segment first
%% anchor, locate/2 would answer "position 0 of segment 1" for an offset that
%% lives in segment 2.
every_segment_anchors_its_first_record(Config) ->
    W = start(Config, #{segment_max_records => 3, index_anchor_interval => 2}),
    Msgs = [msg(P) || P <- [<<"a">>, <<"b">>, <<"c">>, <<"d">>, <<"e">>, <<"f">>, <<"g">>]],
    {ok, _} = wal_writer:append(W, Msgs, os_buffer),
    #{index := Tid} = wal_writer:state(W),
    Dir = priv(Config),
    Seg1 = wal_segment:path(Dir, 1),
    Seg2 = wal_segment:path(Dir, 4),
    Seg3 = wal_segment:path(Dir, 7),
    {ok, 1, Seg1, 0} = wal_index:locate(Tid, 1),
    {ok, 1, Seg1, 0} = wal_index:locate(Tid, 2),
    {ok, 3, Seg1, Pos3} = wal_index:locate(Tid, 3),
    true = Pos3 > 0,
    {ok, 4, Seg2, 0} = wal_index:locate(Tid, 4),
    {ok, 4, Seg2, 0} = wal_index:locate(Tid, 5),
    {ok, 6, Seg2, _} = wal_index:locate(Tid, 6),
    {ok, 7, Seg3, 0} = wal_index:locate(Tid, 7),
    {ok, 7, Seg3, 0} = wal_index:locate(Tid, 99),
    ok.

truncate_is_persisted_with_the_committed_watermark(Config) ->
    W = start(Config, #{}),
    {ok, _} = wal_writer:append(W, [msg(<<"a">>), msg(<<"b">>), msg(<<"c">>)], os_buffer),
    ok = wal_writer:advance_committed(W, 2),
    ok = wal_writer:truncate(W, 3),
    %% §11.3: the boundary never walks back, and never passes committed + 1.
    {error, _} = wal_writer:truncate(W, 1),
    {error, _} = wal_writer:truncate(W, 4),
    #{truncate_before := 3} = wal_writer:state(W),
    kill(W),
    {ok, St} = wal_recovery:recover(1, priv(Config), #{}),
    #{committed := 2, truncate_before := 3, warnings := []} = St,
    ok.

%%%_* Capability and lifecycle -------------------------------------------

close_marks_the_partition_closed_and_refuses_further_writes(Config) ->
    W = start(Config, #{}),
    {ok, [1]} = wal_writer:append(W, [msg(<<"a">>)], os_buffer),
    #{index := Tid} = wal_writer:state(W),
    ok = wal_writer:close(W),
    %% The flag lives in the public index so that read_from can refuse a closed
    %% partition in the caller's own process, without a call to a dead writer.
    {false, 1} = wal_index:state(Tid),
    {error, {closed, 1}} = wal_writer:append(W, [msg(<<"b">>)], os_buffer),
    {error, {closed, 1}} = wal_writer:advance_committed(W, 1),
    {error, {closed, 1}} = wal_writer:truncate(W, 1),
    ok.

%% A caller blocked in append/3 when the writer is stopped must be answered.
%% The alternative is a bare exit, which cannot distinguish "never written" from
%% "written but you did not hear about it" -- and the publisher's retry decision
%% depends on exactly that distinction.
a_stopping_writer_answers_its_queued_callers_instead_of_dropping_them(Config) ->
    W = start(Config, #{batch_delay_ms => #{os_buffer => 0, fsync => 30000}}),
    Parent = self(),
    Caller = spawn_link(fun() ->
                                Parent ! {self(), wal_writer:append(W, [msg(<<"late">>)], fsync)}
                        end),
    %% The request is queued and its 30 s deadline armed; the writer is still
    %% answering other calls, which is what makes the batching work at all.
    ok = await_appends(W, 1, 500),
    ok = wal_writer:stop(W),
    receive
        {Caller, Result} -> {error, {writer_stopped, normal}} = Result
    after 5000 -> ct:fail({no_reply_from, Caller})
    end,
    ok.

%% ADR-011 §11.2 does not say a partition may be recovered only once per node:
%% `recover_after_close_returns_watermarks' in the frozen contract closes and
%% re-recovers the same partition. The writer keeps its descriptors and its state
%% through a close, so reopening is a flag flip rather than a rescan -- and it
%% must continue the same offset sequence, not restart it.
a_closed_writer_reopens_on_the_same_log(Config) ->
    W = start(Config, #{}),
    {ok, [1, 2]} = wal_writer:append(W, [msg(<<"a">>), msg(<<"b">>)], os_buffer),
    %% open/1 is idempotent on purpose: recover/2 must not have to know whether
    %% the partition it found was closed by a caller or left open by a supervisor
    %% restart that already ran recovery on its own.
    {ok, #{high := 3}} = wal_writer:open(W),
    ok = wal_writer:close(W),
    {error, {closed, 1}} = wal_writer:append(W, [msg(<<"c">>)], os_buffer),
    {ok, #{index := Tid, high := 3, committed := 0}} = wal_writer:open(W),
    {true, 1} = wal_index:state(Tid),
    {ok, [3]} = wal_writer:append(W, [msg(<<"c">>)], os_buffer),
    ok = wal_writer:stop(W),
    {ok, #{high := 4}} = wal_recovery:recover(1, priv(Config), #{}),
    ok.

append_rejects_a_mode_outside_the_capability_without_consuming_an_offset(Config) ->
    W = start(Config, #{}),
    %% ADR-009's third mode is in no S1 backend's capability (ADR-011 §11.4).
    [os_buffer, fsync] = wal_writer:capability(),
    {error, {unsupported_sync_mode, replicated_commit}} =
        wal_writer:append(W, [msg(<<"a">>)], replicated_commit),
    {ok, [1]} = wal_writer:append(W, [msg(<<"b">>)], os_buffer),
    ok.

%% S2-G (design §4.7): the batch queue is bounded. A long fsync window keeps
%% the first two appends pending, so the next ones meet a full queue and must be
%% answered without allocating an offset -- a hole there would make `read_from'
%% wait forever for a record that was never written.
appending_above_the_pending_limit_is_refused_without_consuming_an_offset(Config) ->
    W = start(Config, #{batch_delay_ms => #{os_buffer => 0, fsync => 500},
                        max_pending_appends => 2}),
    Parent = self(),
    Pids = [spawn_link(fun() ->
                              Parent ! {self(), wal_writer:append(W, [msg(<<"p">>)],
                                                                 fsync)}
                      end) || _ <- lists:seq(1, 4)],
    Results = [receive
                   {Pid, Result} -> Result
               after 10000 -> ct:fail({no_reply_from, Pid})
               end || Pid <- Pids],
    {Accepted, Refused} =
        lists:partition(fun({ok, _}) -> true; (_) -> false end, Results),
    2 = length(Accepted),
    2 = length(Refused),
    %% The refusal names the partition, so a caller can tell "this partition is
    %% saturated" from "the writer is gone".
    lists:foreach(fun(R) -> {error, {overloaded, 1}} = R end, Refused),
    %% The queue drains on its own deadline: the two accepted appends are dense
    %% and reach the log, and the refusals left nothing behind.
    #{high := 3, committed := 0} = await_high(W, 3),
    #{pending := 0} = wal_writer:stats(W),
    ok.

await_high(W, High) ->
    case wal_writer:state(W) of
        #{high := High} = State -> State;
        _ -> timer:sleep(20), await_high(W, High)
    end.

%%%_* The volatile-filesystem guard --------------------------------------

%% S0.3 §4 wanted to detect tmpfs by timing an fsync. A fast NVMe with
%% power-loss protection would fail that test and refuse to boot, so the guard
%% reads the mount table instead -- injected here as a file, no mocking.
the_guard_refuses_a_volatile_mount_and_allows_a_durable_one(Config) ->
    Dir = priv(Config),
    Volatile = mounts_file(Config, "volatile", "tmpfs " ++ Dir ++ " tmpfs rw,relatime 0 0\n"),
    {error, {volatile_filesystem, Dir}} =
        wal_writer:start_link(1, Dir, #{mounts_file => Volatile}),
    %% The guard runs before recovery, so a refused start changed nothing on
    %% disk -- not even the torn-tail cut recovery would otherwise perform.
    Durable = mounts_file(Config, "durable", "/dev/nvme0n1p5 " ++ Dir ++ " ext4 rw 0 0\n"),
    {ok, W} = wal_writer:start_link(1, Dir, #{mounts_file => Durable}),
    {ok, [1]} = wal_writer:append(W, [msg(<<"a">>)], os_buffer),
    ok.

the_guard_can_be_waived_and_then_says_so(Config) ->
    Dir = priv(Config),
    Volatile = mounts_file(Config, "waived", "tmpfs " ++ Dir ++ " tmpfs rw 0 0\n"),
    {ok, W} = wal_writer:start_link(1, Dir,
                                    #{mounts_file => Volatile, require_durable_fs => false}),
    %% Waiving the guard is a decision someone made in a config file; it has to
    %% be visible in the state rather than swallowed, or nobody will know that
    %% "durable" on that node meant "until the next reboot".
    #{warnings := Warnings} = wal_writer:state(W),
    true = lists:member({volatile_filesystem, Dir}, Warnings),
    ok.

classify_fs_matches_the_longest_mount_on_a_path_boundary(_Config) ->
    Mounts = <<"/dev/sda1 / ext4 rw,relatime 0 0\n"
               "tmpfs /a tmpfs rw,nosuid 0 0\n"
               "/dev/sdb1 /a/b ext4 rw,relatime 0 0\n">>,
    volatile = wal_writer:classify_fs("/a", Mounts),
    volatile = wal_writer:classify_fs("/a/x/y", Mounts),
    %% Longest prefix wins: /a/b is a real disk inside a tmpfs mount.
    durable = wal_writer:classify_fs("/a/b", Mounts),
    durable = wal_writer:classify_fs("/a/b/c/segment.log", Mounts),
    durable = wal_writer:classify_fs("/", Mounts),
    durable = wal_writer:classify_fs("/var/lib/unifiedmq", Mounts),
    %% /ab is not inside /a: matching on raw string prefixes would say it is.
    unknown = wal_writer:classify_fs("/ab", <<"/dev/sdb1 /a ext4 rw 0 0\n">>),
    ok.

classify_fs_reports_unknown_when_no_mount_matches(_Config) ->
    unknown = wal_writer:classify_fs("/nowhere", <<"tmpfs /a tmpfs rw 0 0\n">>),
    %% No mount table at all (a non-Linux dev box) is also "cannot tell", and
    %% the writer allows it: refusing to boot off an absent /proc/mounts would
    %% make the guard a portability bug rather than a safety net.
    unknown = wal_writer:classify_fs("/a", <<>>),
    ok.

%%%_* Helpers ------------------------------------------------------------

priv(Config) ->
    proplists:get_value(wal_dir, Config).

start(Config, WalConfig) ->
    {ok, Pid} = wal_writer:start_link(1, priv(Config),
                                      WalConfig#{require_durable_fs => false}),
    put(writer, Pid),
    Pid.

%% Unlink first: start_link ties the writer to the test process, and a kill
%% signal travels both ways along that link.
kill(Pid) ->
    Mref = monitor(process, Pid),
    true = unlink(Pid),
    exit(Pid, kill),
    receive
        {'DOWN', Mref, process, Pid, _} -> ok
    after 5000 -> ct:fail({writer_did_not_die, Pid})
    end.

%% Poll rather than sleep: the writer counts an append when it enqueues it, so
%% this returns as soon as the request is really in the queue.
await_appends(_W, _N, 0) ->
    ct:fail(the_append_never_reached_the_writer);
await_appends(W, N, Tries) ->
    case wal_writer:stats(W) of
        #{appends := Appends} when Appends >= N ->
            ok;
        _ ->
            receive after 10 -> ok end,
            await_appends(W, N, Tries - 1)
    end.

concurrent_appends(W, Callers, PerCaller, Mode) ->
    Parent = self(),
    Pids = lists:map(
             fun(I) ->
                     spawn_link(
                       fun() ->
                               Msgs = [msg(list_to_binary(integer_to_list(I)))
                                       || _ <- lists:seq(1, PerCaller)],
                               Parent ! {self(), wal_writer:append(W, Msgs, Mode)}
                       end)
             end,
             lists:seq(1, Callers)),
    lists:foldl(fun(Pid, Acc) ->
                        receive
                            {Pid, {ok, Offsets}} -> [Offsets | Acc];
                            {Pid, Other} -> ct:fail({append_failed, Pid, Other})
                        after 10000 -> ct:fail({no_reply_from, Pid})
                        end
                end, [], Pids).

%% Beside the partition directory, not inside it: a file in the partition would
%% show up as an {unexpected_file, _} recovery warning and change what the case
%% is asserting.
mounts_file(Config, Name, Line) ->
    Path = filename:join(proplists:get_value(priv_dir, Config), Name ++ ".mounts"),
    ok = file:write_file(Path, Line),
    Path.

msg(Payload) ->
    #broker_message{message_id = <<"m">>,
                    tenant_id = <<"t0">>,
                    destination = <<"d">>,
                    partition_id = 1,
                    payload = Payload,
                    payload_encoding = raw,
                    headers = #{},
                    proto_meta = #{},
                    persistence = durable,
                    reliability = os_buffer,
                    created_at = 1,
                    expires_at = undefined,
                    event = undefined}.
