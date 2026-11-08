%% @doc S2-F: retention policy (ADR-014) -- the pure decision function plus the
%% process-level view operators get.
%%
%% The deletion itself is exercised through `broker_storage_wal_SUITE' and
%% `wal_writer_SUITE'; here the point is the boundary between "what the policy
%% allows" and "what `committed' allows", because that is the line the ADR makes
%% hard: the GC may only ever delete whole segments below `committed + 1', and a
%% slow consumer pins that number.
-module(wal_gc_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("broker_core/include/broker_message.hrl").

all() ->
    [max_bytes_deletes_oldest_until_under_the_limit,
     max_age_deletes_only_expired_segments,
     both_limits_intersect_and_delete_less,
     the_cut_never_crosses_the_committed_watermark,
     blocked_when_committed_pins_everything,
     no_limits_means_no_policy_deletion,
     run_is_safe_with_no_partitions,
     status_reports_the_oldest_segment_and_deletable_bytes].

init_per_suite(Config) ->
    %% `data_dir' is the suite's persistent data directory; `priv_dir' is a
    %% per-run log directory. The partition layout has to sit under the former
    %% because that is also what the testcases hand to create/2.
    DataDir = ?config(data_dir, Config),
    %% That directory survives between runs. A `wal/77' left by an earlier run
    %% makes create/2 answer already_exists and leaves a second partition for
    %% `status/0' to report, so it is removed before the first case.
    _ = file:del_dir_r(filename:join(DataDir, "wal")),
    %% set_env before the application is loaded is discarded when the `.app'
    %% file installs its own env, and this suite sets a private data_dir that
    %% must win. Load first, then install the test env.
    ok = ensure_loaded(broker_storage),
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok = application:set_env(broker_storage, data_dir, DataDir),
    %% The periodic sweep is part of the process, not of these tests; disabling
    %% it keeps the assertions deterministic (run/0 is called explicitly).
    ok = application:set_env(broker_storage, retention,
                             #{max_bytes => 0, max_age_ms => 0,
                               check_interval_ms => 0}),
    {ok, _Started} = application:ensure_all_started(broker_storage),
    [{data_dir, DataDir} | Config].

end_per_suite(_Config) ->
    ok = application:stop(broker_storage),
    ok.

init_per_testcase(_Case, Config) ->
    cleanup_writers(),
    Config.

end_per_testcase(_Case, Config) ->
    cleanup_writers(),
    Config.

%%%_* Pure policy --------------------------------------------------------

max_bytes_deletes_oldest_until_under_the_limit(_Config) ->
    %% 300 bytes total, limit 250: deleting the oldest 100-byte segment is
    %% enough, so the second one must stay.
    Scan = scan([seg(1, 4, 100), seg(4, 7, 100), seg(7, undefined, 100)]),
    Retention = #{max_bytes => 250, max_age_ms => 0},
    Decision = wal_gc:plan(Scan, Retention, info(6, 1, 7)),
    #{deletes := [1], cut := 4, deletable_bytes := 200,
      blocked_reason := none} = Decision,
    ok.

max_age_deletes_only_expired_segments(_Config) ->
    Now = now_ms(),
    Scan = scan([seg(1, 4, 100, Now - 50000),
                 seg(4, 7, 100, Now - 1000),
                 seg(7, undefined, 100, Now)]),
    Retention = #{max_bytes => 0, max_age_ms => 10000},
    Decision = wal_gc:plan(Scan, Retention, info(6, 1, 7)),
    %% Only the first segment is older than 10 s.
    #{deletes := [1], cut := 4, blocked_reason := none} = Decision,
    ok.

%% ADR-014 §14.2: both limits configured means a segment goes only when both
%% agree. Age would allow [1, 4], bytes only [1]; the intersection is [1].
both_limits_intersect_and_delete_less(_Config) ->
    Now = now_ms(),
    Scan = scan([seg(1, 4, 100, Now - 50000),
                 seg(4, 7, 100, Now - 50000),
                 seg(7, undefined, 100, Now)]),
    Retention = #{max_bytes => 250, max_age_ms => 10000},
    Decision = wal_gc:plan(Scan, Retention, info(6, 1, 7)),
    #{deletes := [1], cut := 4} = Decision,
    ok.

%% The decision cannot advance past the acked prefix, even when the size limit
%% would happily eat everything.
the_cut_never_crosses_the_committed_watermark(_Config) ->
    Scan = scan([seg(1, 4, 100), seg(4, 7, 100), seg(7, undefined, 100)]),
    Retention = #{max_bytes => 1, max_age_ms => 0},
    %% Only segment [1, 4) is entirely at or below committed = 3.
    Decision = wal_gc:plan(Scan, Retention, info(3, 1, 7)),
    #{deletes := [1], cut := 4} = Decision,
    true = 4 =< 3 + 1,
    ok.

%% committed = 0 means the first unacked offset is 1: no whole segment can go,
%% and that must be reported rather than hidden.
blocked_when_committed_pins_everything(_Config) ->
    Scan = scan([seg(1, 4, 100), seg(4, 7, 100), seg(7, undefined, 100)]),
    Retention = #{max_bytes => 1, max_age_ms => 0},
    Decision = wal_gc:plan(Scan, Retention, info(0, 1, 7)),
    #{deletes := [], cut := 1, deletable_bytes := 0,
      blocked_reason := slow_consumer} = Decision,
    ok.

no_limits_means_no_policy_deletion(_Config) ->
    Scan = scan([seg(1, 4, 100), seg(4, 7, 100), seg(7, undefined, 100)]),
    Decision = wal_gc:plan(Scan, #{max_bytes => 0, max_age_ms => 0},
                          info(6, 1, 7)),
    #{deletes := [], cut := 1, blocked_reason := none} = Decision,
    ok.

%%%_* Process / status ---------------------------------------------------

run_is_safe_with_no_partitions(_Config) ->
    ok = wal_gc:run(),
    ok.

status_reports_the_oldest_segment_and_deletable_bytes(Config) ->
    Dir = ?config(data_dir, Config),
    ConfigMap = #{data_dir => Dir,
                  wal => #{require_durable_fs => false, segment_max_records => 3}},
    {ok, Hd} = broker_storage_wal:create(77, ConfigMap),
    {ok, _Offsets} =
        broker_storage_wal:append(Hd, [msg(N) || N <- lists:seq(1, 6)], os_buffer),
    ok = broker_storage_wal:advance_committed(Hd, 3),
    %% Two segments: [1, 4) and the active [4, 7). The first one is deletable
    %% (its last offset 3 is at the committed watermark).
    [{77, 1, DeletableBytes, none}] = wal_gc:status(),
    true = DeletableBytes > 0,
    %% broker_queue is a thin view over the same data; it must not need the
    %% queue application to be running for this read-only answer.
    [{77, 1, DeletableBytes, none}] = broker_queue:retention_status(),
    ok.

%%%_* Helpers ------------------------------------------------------------

%% `application:load/1' reports already_loaded on every run after the first in
%% the same node; both outcomes are fine, the point is only that the `.app'
%% file's env has been installed before the test env is written over it.
ensure_loaded(App) ->
    case application:load(App) of
        ok -> ok;
        {error, {already_loaded, App}} -> ok
    end.

seg(Start, Next, Size) ->
    seg(Start, Next, Size, now_ms()).

seg(Start, Next, Size, MTime) ->
    #{start => Start, next => Next, path => path(Start), size => Size,
      mtime_ms => MTime}.

path(Start) ->
    "segment-" ++ integer_to_list(Start) ++ ".log".

scan(Segments) ->
    #{partition => 1,
      dir => "wal/1",
      total_bytes => lists:sum([maps:get(size, S) || S <- Segments]),
      segments => Segments}.

info(Committed, Trunc, ActiveStart) ->
    #{committed => Committed, truncate_before => Trunc,
      active_segment => path(ActiveStart)}.

now_ms() ->
    erlang:system_time(millisecond).

msg(N) ->
    #broker_message{message_id = <<"m">>,
                    tenant_id = <<"t0">>,
                    destination = <<"d">>,
                    partition_id = 1,
                    payload = integer_to_binary(N),
                    payload_encoding = raw,
                    headers = #{},
                    proto_meta = #{},
                    persistence = durable,
                    reliability = os_buffer,
                    created_at = 1,
                    expires_at = undefined,
                    event = undefined}.

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
