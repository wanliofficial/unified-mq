%% @doc T6: the partition worker layer that sits between a protocol adapter and
%% `broker_storage'.
%%
%% Three things are being pinned here, and each has a case of its own:
%%
%%  * H8's answer is executable. `create_partition/1' is the only place that
%%    calls `broker_storage:create/2', it works with the queue app stopped
%%    (that is what "out-of-band management action" means), and boot only ever
%%    recovers -- so a data dir nobody created fails the whole application
%%    instead of quietly coming up empty (ADR-011 §11.2).
%%  * The data path does not pass through the worker. §5.1(d) asks for appends
%%    to a partition to be serialized; the serialization point is the single
%%    `wal_writer', not an extra gen_server hop. A blocking hop here would cap
%%    every partition at one fsync per message, which is the ~1148 msg/s of
%%    S0.3 §2.1 that group commit exists to beat. `sys:suspend' on the worker
%%    proves it structurally, and `eight_concurrent_callers_pay_for_one_fsync'
%%    proves it in the writer's own counters.
%%  * I15: a handle goes stale when its writer dies. The caller must see an
%%    error, not an exit, and the layer must re-attach by itself.
-module(broker_queue_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("broker_core/include/broker_message.hrl").

%% Fewer than sys.config's 4: the count being config driven is itself under
%% test, and two partitions are enough to show one partition's append does not
%% disturb another's.
-define(N, 2).
-define(BAD_ID, 7).

all() ->
    [boot_fails_loudly_when_a_partition_was_never_created,
     create_partition_is_the_only_entry_that_makes_boot_succeed,
     create_partition_refuses_an_id_outside_the_configured_range,
     append_returns_a_dense_offset_and_the_record_is_readable,
     append_to_an_unknown_partition_is_refused,
     the_data_path_does_not_pass_through_the_partition_worker,
     eight_concurrent_callers_pay_for_one_fsync,
     concurrent_appends_through_the_queue_yield_dense_offsets,
     capability_reports_what_the_backend_actually_does,
     subscribers_are_notified_with_the_offset_range_of_an_append,
     unsubscribe_stops_notifications,
     a_dead_subscriber_is_dropped_without_disturbing_appends,
     a_crashed_storage_writer_is_reattached_and_the_log_continues,
     advance_committed_reaches_the_watermark_and_refuses_to_run_ahead,
     an_unsupported_sync_mode_is_refused_without_consuming_an_offset,
     the_memory_backend_works_through_the_same_queue_api].

%%%_* Fixtures ------------------------------------------------------------

init_per_suite(Config) ->
    %% Read the env rather than inheriting it: CT loads no sys.config, and
    %% broker_queue's defaults would otherwise decide the partition count.
    %% `_ =' on purpose: CT runs every suite in one node, and an earlier suite
    %% may have loaded the app already.
    _ = application:load(broker_queue),
    ok = application:set_env(broker_queue, partitions, ?N),
    Config.

end_per_suite(_Config) ->
    stop_all(),
    ok.

init_per_testcase(Case, Config) ->
    stop_all(),
    %% CT hands every testcase in one suite run the same priv_dir, so each case
    %% gets its own data_dir -- otherwise case N appends into case N-1's log.
    DataDir = filename:join([?config(priv_dir, Config), atom_to_list(Case), "data"]),
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok = application:set_env(broker_storage, data_dir, DataDir),
    %% The durability guard is waived because CT's priv_dir sits on tmpfs on
    %% some CI hosts; design §7.2 makes it configurable off for exactly that,
    %% and broker_storage_wal_SUITE asserts the guard itself.
    ok = application:set_env(broker_storage, wal, #{require_durable_fs => false}),
    {ok, _Started} = application:ensure_all_started(broker_storage),
    [{data_dir, DataDir} | Config].

end_per_testcase(_Case, _Config) ->
    stop_all(),
    ok.

%% A writer child outlives its testcase by design, and the memory store is a
%% node singleton, so both are torn down here: a leftover would let the next
%% case recover a partition it never created. broker_router is stopped too: it
%% rides in as a dependency of ensure_all_started(broker_queue), and since T7
%% its supervisor owns the index store child -- a leak here would collide with
%% broker_router_hash_SUITE, which starts a store of the same registered name
%% in its own testcase.
stop_all() ->
    _ = application:stop(broker_queue),
    _ = application:stop(broker_router),
    _ = application:stop(broker_storage),
    cleanup_writers(),
    ok = application:set_env(broker_storage, backend, broker_storage_wal).

cleanup_writers() ->
    case whereis(broker_storage_wal_sup) of
        undefined ->
            ok;
        _Sup ->
            lists:foreach(
              fun({{wal_writer, _} = Id, _Child, _Type, _Modules}) ->
                      _ = supervisor:terminate_child(broker_storage_wal_sup, Id),
                      ok = supervisor:delete_child(broker_storage_wal_sup, Id);
                 (_) ->
                      ok
              end,
              supervisor:which_children(broker_storage_wal_sup))
    end.

%% The normal path: partitions exist because somebody created them, and only
%% then is boot allowed to succeed.
start_with_partitions() ->
    ok = create_partitions(),
    {ok, _Started} = application:ensure_all_started(broker_queue),
    ok.

create_partitions() ->
    lists:foreach(fun(Id) -> ok = broker_queue:create_partition(Id) end, partition_ids()).

partition_ids() ->
    lists:seq(0, ?N - 1).

%%%_* H8: bootstrap is out of band, boot only recovers ---------------------

%% ADR-011 §11.2: a partition nobody created must be a loud startup failure,
%% not an empty partition that reports itself healthy. The assertion on the
%% directory matters as much as the one on the return value -- a boot that
%% failed *after* creating the dir would pass the first and silently reintroduce
%% "recover 顺手创建" through the back door.
boot_fails_loudly_when_a_partition_was_never_created(Config) ->
    {error, Reason} = application:ensure_all_started(broker_queue),
    true = find_atom(partition_not_found, Reason),
    false = is_running(broker_queue),
    false = filelib:is_dir(filename:join([?config(data_dir, Config), "wal", "0"])),
    ok.

%% create/2 must not need the queue application: it is an operator action that
%% happens before the node is allowed to boot.
create_partition_is_the_only_entry_that_makes_boot_succeed(_Config) ->
    false = is_running(broker_queue),
    ok = create_partitions(),
    {ok, _Started} = application:ensure_all_started(broker_queue),
    true = is_running(broker_queue),
    Workers = [worker(Id) || Id <- partition_ids()],
    true = lists:all(fun is_process_alive/1, Workers),
    %% Every partition recovered to the empty-partition watermarks (design §7.1:
    %% offsets start at 1, nothing committed yet).
    lists:foreach(fun(Id) -> {ok, [], 1} = broker_queue:read_from(Id, 1, 10) end,
                  partition_ids()),
    ok.

create_partition_refuses_an_id_outside_the_configured_range(Config) ->
    %% S1 fixes the partition set (§1.2: online repartitioning is S4), so an id
    %% outside it would create a directory no worker ever opens -- a partition
    %% that looks created and serves nothing.
    {error, {unknown_partition, ?BAD_ID}} = broker_queue:create_partition(?BAD_ID),
    false = filelib:is_dir(filename:join([?config(data_dir, Config), "wal",
                                          integer_to_list(?BAD_ID)])),
    ok.

%%%_* Data path -----------------------------------------------------------

append_returns_a_dense_offset_and_the_record_is_readable(_Config) ->
    ok = start_with_partitions(),
    {ok, 1} = broker_queue:append(0, msg(0, <<"p1">>), fsync),
    {ok, 2} = broker_queue:append(0, msg(0, <<"p2">>), fsync),
    {ok, 3} = broker_queue:append(0, msg(0, <<"p3">>), fsync),
    {ok, Page, 4} = broker_queue:read_from(0, 1, 10),
    [{1, M1}, {2, M2}, {3, M3}] = Page,
    [<<"p1">>, <<"p2">>, <<"p3">>] = [M#broker_message.payload || M <- [M1, M2, M3]],
    %% The neighbour partition is untouched: appends are per partition.
    {ok, [], 1} = broker_queue:read_from(1, 1, 10),
    ok.

append_to_an_unknown_partition_is_refused(_Config) ->
    ok = start_with_partitions(),
    {error, {unknown_partition, ?BAD_ID}} =
        broker_queue:append(?BAD_ID, msg(?BAD_ID, <<"x">>), fsync),
    {error, {unknown_partition, ?BAD_ID}} = broker_queue:read_from(?BAD_ID, 1, 10),
    {error, {unknown_partition, ?BAD_ID}} = broker_queue:capability(?BAD_ID),
    {error, {unknown_partition, ?BAD_ID}} = broker_queue:subscribe(?BAD_ID, self()),
    ok.

%% The structural half of the group-commit argument: with the worker suspended,
%% an append still completes. If `broker_queue:append/3' made a blocking call
%% into the worker, this case would hang until the CT timeout instead of
%% failing fast -- which is exactly the point of asserting it this way.
the_data_path_does_not_pass_through_the_partition_worker(_Config) ->
    ok = start_with_partitions(),
    Worker = worker(0),
    ok = broker_queue:subscribe(0, self()),
    sys:suspend(Worker),
    try
        {ok, 1} = broker_queue:append(0, msg(0, <<"a">>), fsync),
        {ok, 2} = broker_queue:append(0, msg(0, <<"b">>), fsync),
        {ok, [{1, _}, {2, _}], 3} = broker_queue:read_from(0, 1, 10)
    after
        sys:resume(Worker)
    end,
    %% The notification is a cast, so it was queued behind the suspension and
    %% only arrives once the worker is running again.
    receive
        {new_messages, 0, 1, 1} -> ok
    after 1000 -> ct:fail(no_notification_after_resume)
    end,
    ok.

%% The measurable half. Eight callers are parked in the writer's mailbox before
%% it is allowed to run, so the batch is a certainty rather than a race: one
%% fsync serves all eight. If the queue layer serialized appends through a
%% blocking hop, each caller would reach the writer alone and this would read
%% `syncs := 8'.
eight_concurrent_callers_pay_for_one_fsync(_Config) ->
    ok = start_with_partitions(),
    Total = 8,
    Writer = writer_pid(0),
    true = is_pid(Writer),
    sys:suspend(Writer),
    Callers = [spawn_append(0, payload(I), fsync) || I <- lists:seq(1, Total)],
    ok = await_mailbox(Writer, Total),
    sys:resume(Writer),
    Offsets = lists:sort(collect_replies(Callers)),
    Expected = lists:seq(1, Total),
    Expected = Offsets,
    #{records := Records, batches := Batches, syncs := Syncs} = wal_writer:stats(Writer),
    Total = Records,
    1 = Batches,
    1 = Syncs,
    ok.

%% E5 at the queue boundary rather than the storage boundary: the offsets a
%% publisher is told about are dense and unique even when every publisher goes
%% through `broker_queue' instead of calling the backend directly.
concurrent_appends_through_the_queue_yield_dense_offsets(_Config) ->
    ok = start_with_partitions(),
    Total = 32,
    Callers = [spawn_append(0, payload(I), fsync) || I <- lists:seq(1, Total)],
    Offsets = lists:sort(collect_replies(Callers)),
    Expected = lists:seq(1, Total),
    Expected = Offsets,
    {ok, Page, NextOffset} = broker_queue:read_from(0, 1, Total + 10),
    ExpectedNext = Total + 1,
    ExpectedNext = NextOffset,
    Expected = [Offset || {Offset, _Msg} <- Page],
    ok.

%%%_* Capability ----------------------------------------------------------

capability_reports_what_the_backend_actually_does(_Config) ->
    ok = start_with_partitions(),
    %% ADR-011 §11.4: instance level, and the honest answer for the WAL backend.
    %% §5.1(a) has conn_fsm cache this at startup to fail early on QoS 1.
    [os_buffer, fsync] = broker_queue:capability(0),
    [os_buffer, fsync] = broker_queue:capability(1),
    ok.

an_unsupported_sync_mode_is_refused_without_consuming_an_offset(_Config) ->
    ok = start_with_partitions(),
    {error, {unsupported_sync_mode, replicated_commit}} =
        broker_queue:append(0, msg(0, <<"x">>), replicated_commit),
    %% §11.4: the rejection costs the caller nothing, so the next append still
    %% gets offset 1 rather than leaving a hole behind a refused write.
    {ok, 1} = broker_queue:append(0, msg(0, <<"a">>), fsync),
    ok.

%%%_* Notifications -------------------------------------------------------

subscribers_are_notified_with_the_offset_range_of_an_append(_Config) ->
    ok = start_with_partitions(),
    ok = broker_queue:subscribe(0, self()),
    {ok, 1} = broker_queue:append(0, msg(0, <<"a">>), fsync),
    {ok, 2} = broker_queue:append(0, msg(0, <<"b">>), fsync),
    %% §5.2's shape. Delivery is async by design (§5.1(e)/(f): the producer's
    %% ack must not wait on dispatch), so the notification trails the reply.
    receive {new_messages, 0, 1, 1} -> ok after 1000 -> ct:fail(no_notification_1) end,
    receive {new_messages, 0, 2, 2} -> ok after 1000 -> ct:fail(no_notification_2) end,
    %% Subscribing to one partition does not sign the caller up for another.
    receive {new_messages, 1, _, _} -> ct:fail(notification_from_other_partition)
    after 100 -> ok
    end,
    ok.

unsubscribe_stops_notifications(_Config) ->
    ok = start_with_partitions(),
    ok = broker_queue:subscribe(0, self()),
    ok = broker_queue:unsubscribe(0, self()),
    {ok, 1} = broker_queue:append(0, msg(0, <<"a">>), fsync),
    receive
        {new_messages, 0, _, _} -> ct:fail(notification_after_unsubscribe)
    after 300 ->
        ok
    end,
    %% Idempotent: an unsubscribe for a pid that is not subscribed is not an
    %% error, because MQTT UNSUBSCRIBE and a racing disconnect both land here.
    ok = broker_queue:unsubscribe(0, self()),
    ok.

%% A conn_fsm that dies must not stay on the notify list: the worker would keep
%% sending to a dead pid forever and, worse, keep its monitor.
a_dead_subscriber_is_dropped_without_disturbing_appends(_Config) ->
    ok = start_with_partitions(),
    Worker = worker(0),
    {Pid, Ref} = spawn_monitor(fun() -> receive stop -> ok end end),
    ok = broker_queue:subscribe(0, Pid),
    [Pid] = subscribers(Worker),
    true = demonitor(Ref, [flush]),
    exit(Pid, kill),
    ok = await_subscribers(Worker, []),
    {ok, 1} = broker_queue:append(0, msg(0, <<"a">>), fsync),
    ok.

%%%_* I15: handle staleness -----------------------------------------------

%% The writer's index table dies with it, so the handle the queue holds is
%% garbage after a crash. What must NOT happen is the caller dying with it: a
%% conn_fsm has no business crashing because storage restarted. What must happen
%% is an error, a re-attach nobody had to ask for, and a log that continues at
%% the right offset.
a_crashed_storage_writer_is_reattached_and_the_log_continues(_Config) ->
    ok = start_with_partitions(),
    {ok, 1} = broker_queue:append(0, msg(0, <<"a">>), fsync),
    {ok, 2} = broker_queue:append(0, msg(0, <<"b">>), fsync),
    {ok, 3} = broker_queue:append(0, msg(0, <<"c">>), fsync),
    Writer1 = writer_pid(0),
    true = is_pid(Writer1),
    exit(Writer1, kill),
    Writer2 = await_new_writer(0, Writer1),
    true = Writer2 =/= Writer1,
    {error, Reason} = broker_queue:append(0, msg(0, <<"d">>), fsync),
    %% The reason must say "storage was gone", not some generic failure: the
    %% caller has to be able to tell a retryable staleness from a real refusal.
    true = find_atom(stale_handle, Reason) orelse find_atom(noproc, Reason),
    %% The failed call re-attached, so the very next one works and continues
    %% from the recovered high watermark: no offset reuse, no hole.
    {ok, 4} = broker_queue:append(0, msg(0, <<"d">>), fsync),
    {ok, Page, 5} = broker_queue:read_from(0, 1, 10),
    [1, 2, 3, 4] = [Offset || {Offset, _Msg} <- Page],
    <<"d">> = lists:last([M#broker_message.payload || {_O, M} <- Page]),
    ok.

%%%_* Watermark -----------------------------------------------------------

advance_committed_reaches_the_watermark_and_refuses_to_run_ahead(_Config) ->
    ok = start_with_partitions(),
    {ok, 1} = broker_queue:append(0, msg(0, <<"a">>), fsync),
    {ok, 2} = broker_queue:append(0, msg(0, <<"b">>), fsync),
    ok = broker_queue:advance_committed(0, 1),
    %% ADR-011 §11.3 / H10: a watermark ahead of what was written would let
    %% truncate discard unacked data. The guard has to survive the trip through
    %% this layer rather than being flattened into a generic error.
    {error, _} = broker_queue:advance_committed(0, 99),
    {error, _} = broker_queue:advance_committed(0, 0),
    {error, {unknown_partition, ?BAD_ID}} = broker_queue:advance_committed(?BAD_ID, 1),
    ok.

%%%_* Backend interchange -------------------------------------------------

%% Gate S1「WAL 与内存实现可互换」above the storage layer: nothing in
%% `broker_queue' may assume a disk backend, a wal_writer, or fsync.
the_memory_backend_works_through_the_same_queue_api(_Config) ->
    _ = application:stop(broker_storage),
    cleanup_writers(),
    ok = application:set_env(broker_storage, backend, broker_storage_memory),
    {ok, _Started} = application:ensure_all_started(broker_storage),
    ok = start_with_partitions(),
    [os_buffer] = broker_queue:capability(0),
    {ok, 1} = broker_queue:append(0, msg(0, <<"a">>), os_buffer),
    {error, {unsupported_sync_mode, fsync}} = broker_queue:append(0, msg(0, <<"b">>), fsync),
    {ok, [{1, M1}], 2} = broker_queue:read_from(0, 1, 10),
    <<"a">> = M1#broker_message.payload,
    ok = broker_queue:subscribe(0, self()),
    {ok, 2} = broker_queue:append(0, msg(0, <<"c">>), os_buffer),
    receive {new_messages, 0, 2, 2} -> ok after 1000 -> ct:fail(no_notification) end,
    ok.

%%%_* Helpers -------------------------------------------------------------

msg(PartitionId, Payload) ->
    #broker_message{message_id = <<"m">>,
                    tenant_id = <<"t">>,
                    destination = <<"d">>,
                    partition_id = PartitionId,
                    payload = Payload,
                    payload_encoding = raw,
                    headers = #{},
                    proto_meta = #{},
                    persistence = durable,
                    reliability = os_buffer,
                    created_at = 1,
                    expires_at = undefined,
                    event = undefined}.

payload(I) ->
    list_to_binary(io_lib:format("caller-~B", [I])).

is_running(App) ->
    lists:keymember(App, 1, application:which_applications()).

%% The worker is a static child (§6/E11), so it is found through the supervisor
%% rather than a registered name: S1 has no per-partition atom to register.
worker(PartitionId) ->
    case [Pid || {{partition, Id}, Pid, worker, _Mods}
                     <- supervisor:which_children(broker_queue_sup),
                 Id =:= PartitionId, is_pid(Pid)] of
        [Pid] -> Pid;
        Other -> ct:fail({no_single_worker, PartitionId, Other})
    end.

%% `undefined' rather than a failure while a supervised child is mid-restart:
%% `which_children' reports `restarting' for it, and the crash case polls through
%% exactly that window.
writer_pid(PartitionId) ->
    case [Pid || {{wal_writer, Id}, Pid, worker, _Mods}
                     <- supervisor:which_children(broker_storage_wal_sup),
                 Id =:= PartitionId, is_pid(Pid)] of
        [Pid] -> Pid;
        [] -> undefined
    end.

subscribers(Worker) ->
    maps:get(subs, sys:get_state(Worker)).

await_subscribers(Worker, Expected) ->
    await(fun() -> Expected =:= subscribers(Worker) end, 500, subscribers_never_settled).

await_new_writer(PartitionId, Old) ->
    await(fun() ->
                  case writer_pid(PartitionId) of
                      Pid when is_pid(Pid), Pid =/= Old -> {true, Pid};
                      _ -> false
                  end
          end, 2000, writer_never_restarted).

await_mailbox(Pid, Len) ->
    await(fun() ->
                  case process_info(Pid, message_queue_len) of
                      {message_queue_len, Len} -> true;
                      {message_queue_len, _} -> false
                  end
          end, 2000, mailbox_never_filled).

%% Polls rather than sleeping a fixed time: a restart is asynchronous and the
%% alternative is a case that passes on a fast machine and fails on a loaded one.
await(Pred, TimeoutMs, OnTimeout) ->
    Deadline = erlang:monotonic_time(millisecond) + TimeoutMs,
    do_await(Pred, Deadline, OnTimeout).

do_await(Pred, Deadline, OnTimeout) ->
    case Pred() of
        true -> ok;
        {true, Value} -> Value;
        false ->
            case erlang:monotonic_time(millisecond) >= Deadline of
                true -> ct:fail(OnTimeout);
                false ->
                    receive after 5 -> ok end,
                    do_await(Pred, Deadline, OnTimeout)
            end
    end.

spawn_append(PartitionId, Payload, Mode) ->
    Parent = self(),
    spawn_monitor(fun() ->
                          Parent ! {self(), broker_queue:append(PartitionId,
                                                                msg(PartitionId, Payload),
                                                                Mode)}
                  end).

%% Appends are `gen_server:call' into the writer, so a caller that exits instead
%% of replying is a real failure and must not be reported as a missing offset.
collect_replies(Callers) ->
    lists:map(
      fun({Pid, MRef}) ->
              receive
                  {Pid, {ok, Offset}} ->
                      true = demonitor(MRef, [flush]),
                      Offset;
                  {Pid, Other} ->
                      ct:fail({append_failed, Other});
                  {'DOWN', MRef, _, _, Why} ->
                      ct:fail({caller_died, Why})
              after 30000 ->
                  ct:fail({caller_timed_out, Pid})
              end
      end,
      Callers).

find_atom(Atom, Term) ->
    case Term of
        Atom -> true;
        [_ | _] -> lists:any(fun(Part) -> find_atom(Atom, Part) end, Term);
        _ when is_tuple(Term) -> find_atom(Atom, tuple_to_list(Term));
        _ -> false
    end.
