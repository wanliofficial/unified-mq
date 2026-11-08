%% @doc T5: the ADR-011 storage contract run against the WAL backend.
%%
%% The 13 contract cases come from broker_storage_contract.hrl unchanged, which
%% is what makes Gate「WAL 与内存实现可互换」a mechanical result rather than a
%% claim. `backend_cases()' adds the six a disk backend owes on top: the frozen
%% on-disk layout (§7.3), an independent recovery of what the backend wrote, the
%% volatile-filesystem guard, a supervised crash restart, design §9 E5's
%% mixed-sync_mode concurrency -- measured at the `broker_storage' boundary
%% because that is the layer T6 calls -- and a read that spans a segment
%% rotation, which is the only way `broker_storage_wal:read_pages/5' recursion
%% gets exercised.
-module(broker_storage_wal_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").

%% ADR-011 §11.4 capability table, S1 row: the WAL backend is the only one that
%% may claim fsync, and `replicated_commit' stays out until S3 has a raft group.
-define(BACKEND, broker_storage_wal).
-define(CAPABILITY, [os_buffer, fsync]).

-include("broker_storage_contract.hrl").

%%%_* Contract hooks ------------------------------------------------------

init_per_suite(Config) ->
    %% create/2 adds a partition to a supervisor that has to be running first,
    %% and CT loads no sys.config, so the backend is stated here rather than
    %% inherited from whatever the default happens to be.
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    {ok, _Started} = application:ensure_all_started(broker_storage),
    Config.

%% CT calls end_per_suite whenever init_per_suite is exported, so this has to
%% exist; it also undoes what init_per_suite started, since every suite in the
%% run shares one node and a leftover running storage app is exactly the kind of
%% cross-suite state that makes a later failure look like it came from nowhere.
end_per_suite(_Config) ->
    ok = application:stop(broker_storage),
    ok.

%% CT hands every testcase in one suite run the same priv_dir, so each case gets
%% its own data_dir -- otherwise case N appends into case N-1's partition.
backend_setup(Case, Config) ->
    DataDir = filename:join([?config(priv_dir, Config), atom_to_list(Case), "data"]),
    [{data_dir, DataDir} | Config].

%% A writer child outlives its testcase by design (that is what "supervised
%% partition" means), so it is removed here: a leftover spec would make the next
%% case's create/2 fail with already_present instead of doing its job.
backend_cleanup(_Config) ->
    lists:foreach(
      fun({{wal_writer, _} = Id, _Child, _Type, _Modules}) ->
              _ = supervisor:terminate_child(broker_storage_wal_sup, Id),
              ok = supervisor:delete_child(broker_storage_wal_sup, Id);
         (_) ->
              ok
      end,
      supervisor:which_children(broker_storage_wal_sup)).

%% The durability guard is waived because CT's priv_dir sits on tmpfs on some CI
%% hosts, and design §7.2 makes the guard configurable off for exactly that.
%% The guard itself is asserted in create_refuses_a_volatile_data_dir.
backend_config(Config) ->
    #{data_dir => ?config(data_dir, Config),
      wal => #{require_durable_fs => false}}.

backend_cases() ->
    [the_partition_layout_is_the_frozen_path_from_design_7_3,
     records_written_through_the_backend_survive_an_independent_recovery,
     create_refuses_a_volatile_data_dir,
     a_crashed_writer_is_restarted_and_the_log_survives,
     mixed_sync_modes_concurrently_yield_dense_offsets_and_readable_records,
     read_from_spans_a_segment_rotation,
     retention_deletes_only_whole_segments_below_committed,
     a_pinned_committed_offset_deletes_nothing].

%%%_* Backend cases -------------------------------------------------------

%% §7.3 freezes `data/wal/{partition_id}/segment-{StartOffset:020d}.log'. The
%% expected path is rebuilt here from that rule instead of asking the backend
%% for it, so a change of layout breaks this case rather than agreeing with it.
the_partition_layout_is_the_frozen_path_from_design_7_3(Config) ->
    {ok, _Hd} = ?BACKEND:create(3, backend_config(Config)),
    Dir = partition_dir(3, Config),
    Dir = filename:join([?config(data_dir, Config), "wal", "3"]),
    true = filelib:is_dir(Dir),
    %% Offsets start at 1 (design §7.1), so the first segment is named for it,
    %% and an empty partition already has both files: create/2 that leaves them
    %% out would be indistinguishable from a partition that was never created.
    true = filelib:is_file(wal_segment:path(Dir, 1)),
    true = filelib:is_file(wal_meta:path(Dir)),
    ok.

%% Recovery is read straight off the disk, not through the backend: this is what
%% a restarted node sees, so it proves the records and the watermarks are in the
%% files and not merely in the writer's state.
records_written_through_the_backend_survive_an_independent_recovery(Config) ->
    {ok, Hd} = ?BACKEND:create(1, backend_config(Config)),
    {ok, [1, 2, 3]} = ?BACKEND:append(Hd, [msg(1, <<"a">>), msg(1, <<"b">>), msg(1, <<"c">>)],
                                      fsync),
    ok = ?BACKEND:advance_committed(Hd, 1),
    {ok, Recovered} = wal_recovery:recover(1, partition_dir(1, Config), #{}),
    #{high := 4, committed := 1, truncate_before := 1} = Recovered,
    ok.

%% The guard reads the mount table instead of timing an fsync (S0.3 §4's
%% heuristic would refuse a healthy NVMe with power-loss protection), so the test
%% points it at a table that claims the data dir is on tmpfs.
create_refuses_a_volatile_data_dir(Config) ->
    Dir = partition_dir(9, Config),
    %% In priv_dir rather than beside the data dir: the per-case directory only
    %% exists once something has created a partition in it, and this case is
    %% asserting that nothing gets created.
    Mounts = filename:join(?config(priv_dir, Config), "volatile.mounts"),
    ok = file:write_file(Mounts,
                         ["tmpfs ", filename:absname(Dir), " tmpfs rw,relatime 0 0\n"]),
    Volatile = #{data_dir => ?config(data_dir, Config), wal => #{mounts_file => Mounts}},
    {error, {volatile_filesystem, Dir}} = ?BACKEND:create(9, Volatile),
    %% Refused before the layout was written: a directory that looks created but
    %% can never be recovered is worse than no directory at all.
    false = filelib:is_dir(Dir),
    ok.

%% kill -9 semantics: no terminate/2, no final sync, no chance to flush. What a
%% restart finds is exactly what the fsync above put on the platter.
a_crashed_writer_is_restarted_and_the_log_survives(Config) ->
    {ok, Hd} = ?BACKEND:create(1, backend_config(Config)),
    {ok, [1, 2, 3]} = ?BACKEND:append(Hd, [msg(1, <<"a">>), msg(1, <<"b">>), msg(1, <<"c">>)],
                                      fsync),
    Pid = writer_pid(1),
    Mref = monitor(process, Pid),
    true = unlink(Pid),
    exit(Pid, kill),
    receive
        {'DOWN', Mref, process, Pid, killed} -> ok
    after 5000 -> ct:fail({writer_did_not_die, Pid})
    end,
    Pid2 = await_restart(1, Pid, 500),
    %% E11: a partition worker is permanent, so the supervisor put it back
    %% without being asked.
    true = is_pid(Pid2),
    %% The old handle is stale -- its writer and its index table died with it --
    %% so the caller recovers, which §11.2 allows on an open partition.
    {ok, Hd2, 0, 4} = ?BACKEND:recover(1, backend_config(Config)),
    {ok, Page, 4} = ?BACKEND:read_from(Hd2, 1, 10),
    [<<"a">>, <<"b">>, <<"c">>] = [M#broker_message.payload || {_Offset, M} <- Page],
    ok.

%% E5 at the boundary T6 will call: fsync and os_buffer callers interleaved, all
%% of them concurrent. "批次内每个 caller 回复严格在 fsync 之后" is measured in
%% wal_writer_SUITE (every_fsync_caller_sees_the_sync_before_its_reply) against
%% the same code path; what is new here is that no caller's offsets or records
%% leak into another's reply.
mixed_sync_modes_concurrently_yield_dense_offsets_and_readable_records(Config) ->
    {ok, Hd} = ?BACKEND:create(1, backend_config(Config)),
    Callers = 16,
    PerCaller = 2,
    Parent = self(),
    lists:foreach(
      fun(I) ->
              Mode = case I rem 2 of
                         0 -> fsync;
                         _ -> os_buffer
                     end,
              Msgs = [msg(1, payload(I, J)) || J <- lists:seq(1, PerCaller)],
              spawn_link(fun() ->
                                 Parent ! {self(), I, ?BACKEND:append(Hd, Msgs, Mode)}
                         end)
      end,
      lists:seq(1, Callers)),
    Replies = collect_replies(Callers, []),
    Total = Callers * PerCaller,
    %% Offsets are allocated inside the writer before anyone waits for a flush,
    %% so concurrency can neither duplicate one nor leave a hole.
    Dense = lists:seq(1, Total),
    Dense = lists:sort(lists:append([Offsets || {_I, Offsets} <- Replies])),
    {ok, Page, NextOffset} = ?BACKEND:read_from(Hd, 1, Total),
    ExpectedNext = Total + 1,
    ExpectedNext = NextOffset,
    Total = length(Page),
    ByOffset = maps:from_list(Page),
    lists:foreach(
      fun({I, Offsets}) ->
              Expected = [payload(I, J) || J <- lists:seq(1, PerCaller)],
              Expected = [(maps:get(Offset, ByOffset))#broker_message.payload
                          || Offset <- Offsets]
      end,
      Replies),
    ok.

%% The index exists so that a read does not rescan the partition (design §7.2
%% E8), and a rotation is where that matters: offsets are dense across the whole
%% partition while file positions restart at zero in every segment, so a reader
%% that lost track of which file an offset lives in serves the wrong bytes or
%% stops early. Both failures are silent, which is why the assertion is on the
%% payloads and not just on the offset list.
read_from_spans_a_segment_rotation(Config) ->
    Small = #{data_dir => ?config(data_dir, Config),
              wal => #{require_durable_fs => false, segment_max_records => 3}},
    {ok, Hd} = ?BACKEND:create(1, Small),
    {ok, [1, 2, 3, 4, 5, 6, 7]} =
        ?BACKEND:append(Hd, [msg(1, payload(1, N)) || N <- lists:seq(1, 7)], os_buffer),
    Dir = partition_dir(1, Config),
    %% Seven records at three per segment: segments start at 1, 4 and 7.
    [{1, _}, {4, _}, {7, _}] = segments(Dir),
    %% Starts inside the first segment, ends inside the second, dense across the
    %% boundary.
    {ok, Page, 7} = ?BACKEND:read_from(Hd, 2, 5),
    [2, 3, 4, 5, 6] = [Offset || {Offset, _Msg} <- Page],
    ExpectedPayloads = [payload(1, N) || N <- lists:seq(2, 6)],
    ExpectedPayloads = [Msg#broker_message.payload || {_Offset, Msg} <- Page],
    %% Starting exactly on a rotation boundary is what a missing per-segment
    %% anchor would break: the greatest anchor at or below 4 has to name the
    %% second segment, not the first.
    {ok, [{4, M4}, {5, M5}], 6} = ?BACKEND:read_from(Hd, 4, 2),
    FirstOfSegment = payload(1, 4),
    FirstOfSegment = M4#broker_message.payload,
    SecondOfSegment = payload(1, 5),
    SecondOfSegment = M5#broker_message.payload,
    %% A page that runs past the end of the log stops there instead of looping
    %% back into the last segment.
    {ok, Tail, 8} = ?BACKEND:read_from(Hd, 3, 100),
    [3, 4, 5, 6, 7] = [Offset || {Offset, _Msg} <- Tail],
    ok.

%% ADR-014 §14.1/§14.4: a truncate boundary only becomes a physical delete for
%% whole segments whose last offset is below `committed + 1'. The active
%% segment and the segment the boundary points into stay.
retention_deletes_only_whole_segments_below_committed(Config) ->
    Small = #{data_dir => ?config(data_dir, Config),
              wal => #{require_durable_fs => false, segment_max_records => 3}},
    {ok, Hd} = ?BACKEND:create(1, Small),
    {ok, [1, 2, 3, 4, 5, 6, 7, 8, 9]} =
        ?BACKEND:append(Hd, [msg(1, payload(1, N)) || N <- lists:seq(1, 9)],
                        os_buffer),
    Dir = partition_dir(1, Config),
    [{1, _}, {4, _}, {7, _}] = segments(Dir),
    %% Everything through offset 6 is acked, so the boundary may move to 7.
    ok = ?BACKEND:advance_committed(Hd, 6),
    ok = ?BACKEND:truncate(Hd, 7),
    false = filelib:is_file(wal_segment:path(Dir, 1)),
    false = filelib:is_file(wal_segment:path(Dir, 4)),
    true = filelib:is_file(wal_segment:path(Dir, 7)),
    %% The frozen read contract: below the boundary clamps up, never errors.
    {ok, Page, 10} = ?BACKEND:read_from(Hd, 1, 100),
    [7, 8, 9] = [Offset || {Offset, _Msg} <- Page],
    %% Recovery sees the same partition: retired list, boundary, and no hole.
    {ok, Recovered} = wal_recovery:recover(1, Dir, #{}),
    #{high := 10, committed := 6, truncate_before := 7, retired := [1, 4]} =
        Recovered,
    ok.

%% The hard ADR-014 rule: a slow consumer pins `committed`, so the GC must not
%% delete anything -- and the refusal is counted, not silently swallowed.
a_pinned_committed_offset_deletes_nothing(Config) ->
    Small = #{data_dir => ?config(data_dir, Config),
              wal => #{require_durable_fs => false, segment_max_records => 3}},
    {ok, Hd} = ?BACKEND:create(2, Small),
    _ = ?BACKEND:append(Hd, [msg(2, payload(2, N)) || N <- lists:seq(1, 6)],
                        os_buffer),
    Dir = partition_dir(2, Config),
    [{1, _}, {4, _}] = segments(Dir),
    Before = maps:get(retention_blocked_by_committed, broker_metrics:snapshot()),
    %% Nothing acked: any boundary above 1 is past `committed + 1'.
    {error, {invalid_truncate, _, _}} = ?BACKEND:truncate(Hd, 4),
    After = maps:get(retention_blocked_by_committed, broker_metrics:snapshot()),
    After = Before + 1,
    true = filelib:is_file(wal_segment:path(Dir, 1)),
    true = filelib:is_file(wal_segment:path(Dir, 4)),
    %% A legal no-op boundary deletes nothing either: offset 1 is not a whole
    %% segment below the boundary.
    ok = ?BACKEND:truncate(Hd, 1),
    true = filelib:is_file(wal_segment:path(Dir, 1)),
    true = filelib:is_file(wal_segment:path(Dir, 4)),
    ok.

%%%_* Suite helpers -------------------------------------------------------

%% Recomputed from the frozen §7.3 rule rather than asked of the backend.
partition_dir(PartitionId, Config) ->
    filename:join([?config(data_dir, Config), "wal", integer_to_list(PartitionId)]).

segments(Dir) ->
    {ok, #{segments := Segments}} = wal_segment:list(Dir),
    Segments.

writer_pid(PartitionId) ->
    case writer_pids(PartitionId) of
        [Pid] -> Pid;
        Other -> ct:fail({expected_one_writer, PartitionId, Other})
    end.

writer_pids(PartitionId) ->
    [Pid || {{wal_writer, Id}, Pid, worker, _Modules}
                <- supervisor:which_children(broker_storage_wal_sup),
            Id =:= PartitionId,
            is_pid(Pid)].

%% `restarting' shows up in which_children while the supervisor waits to retry,
%% so poll for a pid that is not the one we just killed.
await_restart(_PartitionId, _OldPid, 0) ->
    ct:fail(writer_was_not_restarted);
await_restart(PartitionId, OldPid, Tries) ->
    case [Pid || Pid <- writer_pids(PartitionId), Pid =/= OldPid] of
        [Pid] -> Pid;
        _ ->
            receive after 10 -> ok end,
            await_restart(PartitionId, OldPid, Tries - 1)
    end.

collect_replies(0, Acc) ->
    lists:sort(Acc);
collect_replies(N, Acc) ->
    receive
        {_Pid, I, {ok, Offsets}} ->
            collect_replies(N - 1, [{I, Offsets} | Acc]);
        {_Pid, I, Other} ->
            ct:fail({append_failed, I, Other})
    after 30000 ->
        ct:fail(append_timeout)
    end.

payload(I, J) ->
    list_to_binary(io_lib:format("caller-~B-record-~B", [I, J])).
