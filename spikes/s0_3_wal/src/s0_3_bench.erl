%% S0.3 - Group commit benchmark driver.
%%
%% Answers the S0.3 questions with numbers instead of assertions:
%%   Q1 what does per-append fsync cost?
%%   Q2 how much does group commit buy us, at what latency cost?
%%   Q3 how long does recovery take, and what is the duplicate/loss window?
-module(s0_3_bench).

-export([main/1, throughput/3, group_commit/3, recovery/2, run_all/1,
         assert_real_disk/0]).

-define(DEFAULT_MSGS, 20000).
-define(DEFAULT_GC_BATCH, 64).
%% A real block device costs >=100us per fsync. tmpfs is ~10x to 100x faster
%% and would make fsync look free.
-define(MIN_REAL_FSYNC_US, 100).

%% main(Args) -> no_return().  Args: ["msgs","20000","out","report.txt"]
main(Args) ->
    Msgs = int_arg("msgs", Args, ?DEFAULT_MSGS),
    Out = str_arg("out", Args, undefined),
    case assert_real_disk() of
        ok -> ok;
        {error, Reason} ->
            io:format("REFUSING TO BENCHMARK: ~p~n"
                      "The data dir is not a real block device (tmpfs or "
                      "page cache). Set SPIKE_DATA_DIR to a mounted disk.~n",
                      [Reason]),
            halt(2)
    end,
    Result = run_all(Msgs),
    Text = format(Result),
    io:format("~s", [Text]),
    case Out of
        undefined -> ok;
        Path ->
            ok = filelib:ensure_dir(Path),
            ok = file:write_file(Path, io_lib:format("~s~n", [Text])),
            io:format("report written: ~s~n", [Path])
    end,
    halt(0).

%% run_all(MsgCount) -> #{summary := map(), ..}
run_all(MsgCount) ->
    Tput = throughput(MsgCount, payload_of(256), os_buffer),
    Fsync = throughput(MsgCount, payload_of(256), fsync),
    GC = group_commit(MsgCount, payload_of(256), ?DEFAULT_GC_BATCH),
    Rec = recovery(MsgCount, payload_of(256)),
    #{throughput => #{os_buffer => Tput, fsync => Fsync},
      group_commit => GC,
      recovery => Rec}.

%% throughput(MsgCount, Payload, SyncMode) -> map()
%%
%% Single writer, sequential appends. This is the pessimistic baseline that
%% group commit has to beat.
throughput(MsgCount, Payload, SyncMode) ->
    Dir = tmp_dir("tp_" ++ atom_to_list(SyncMode)),
    {ok, W0} = s0_3_wal:open(Dir, #{}),
    Start = erlang:monotonic_time(microsecond),
    W = write_loop(W0, Payload, SyncMode, MsgCount),
    Elapsed = erlang:monotonic_time(microsecond) - Start,
    {ok, _} = s0_3_wal:close(W),
    ok = rm_rf(Dir),
    Secs = Elapsed / 1_000_000,
    #{sync_mode => SyncMode,
      msgs => MsgCount,
      payload_bytes => byte_size(Payload),
      elapsed_us => Elapsed,
      msgs_per_sec => round(MsgCount / Secs),
      us_per_msg => round(Elapsed / MsgCount * 10) / 10}.

%% group_commit(MsgCount, Payload, BatchSize) -> map()
%%
%% Many writers, one fsync per batch. BatchSize 1 == every-writer-fsynces,
%% which is exactly the pathological case the production design must avoid.
group_commit(MsgCount, Payload, BatchSize) ->
    Dir = tmp_dir("gc_" ++ integer_to_list(BatchSize)),
    {ok, W0} = s0_3_wal:open(Dir, #{}),
    N = erlang:min(MsgCount, 2000),
    Start = erlang:monotonic_time(microsecond),
    {ok, W} = batched_loop(W0, Payload, N, BatchSize, 0),
    Elapsed = erlang:monotonic_time(microsecond) - Start,
    {ok, _} = s0_3_wal:close(W),
    ok = rm_rf(Dir),
    Secs = Elapsed / 1_000_000,
    #{batch_size => BatchSize,
      msgs => N,
      fsyncs => ceil_div(N, BatchSize),
      elapsed_us => Elapsed,
      msgs_per_sec => round(N / Secs),
      us_per_msg => round(Elapsed / N * 10) / 10}.

%% recovery(MsgCount, Payload) -> map()
%%
%% Writes with fsync, then measures replay time. Loss window is 0 here because
%% every append was synced; the interesting non-zero window is os_buffer,
%% measured separately in the crash harness (s0_3_crash).
recovery(MsgCount, Payload) ->
    Dir = tmp_dir("rec"),
    {ok, W0} = s0_3_wal:open(Dir, #{}),
    W = write_loop(W0, Payload, fsync, MsgCount),
    {ok, _} = s0_3_wal:close(W),
    Start = erlang:monotonic_time(microsecond),
    {ok, Terms} = s0_3_wal:read_all(Dir),
    Elapsed = erlang:monotonic_time(microsecond) - Start,
    ok = rm_rf(Dir),
    #{written => MsgCount,
      replayed => length(Terms),
      lossless => length(Terms) =:= MsgCount,
      replay_us => Elapsed,
      replay_ms => round(Elapsed / 1000 * 10) / 10,
      us_per_record => round(Elapsed / max(1, MsgCount) * 10) / 10}.

%% ---------------------------------------------------------------- internals

write_loop(W, _P, _S, 0) -> W;
write_loop(W, P, S, Left) ->
    {ok, W1} = s0_3_wal:append(W, {msg, Left, P}, S),
    write_loop(W1, P, S, Left - 1).

batched_loop(W, _P, Limit, _B, Done) when Done >= Limit -> {ok, W};
batched_loop(W, P, Limit, B, Done) ->
    Batch = min(B, Limit - Done),
    W1 = append_batch(W, P, Done, Batch),
    %% One fsync per batch: this is the group commit.
    ok = s0_3_wal:sync(W1),
    batched_loop(W1, P, Limit, B, Done + Batch).

append_batch(W, _P, _From, 0) -> W;
append_batch(W, P, I, K) ->
    {ok, W1} = s0_3_wal:append(W, {msg, I, P}, os_buffer),
    append_batch(W1, P, I + 1, K - 1).

payload_of(Bytes) ->
    binary:copy(<<"x">>, Bytes).

%% Data dir MUST live on a real block device. On tmpfs fsync is a no-op, so
%% every number measured there is fiction. Override with SPIKE_DATA_DIR.
tmp_dir(Tag) ->
    Base = case os:getenv("SPIKE_DATA_DIR") of
        false -> "/home/wanli/workspace/unified-mq/spikes/.data";
        D -> D
    end,
    Dir = filename:join(Base, Tag ++ "_" ++ integer_to_list(erlang:unique_integer([positive]))),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    Dir.

%% file:del_dir_r/1 returns elapsed seconds, not ok.
rm_rf(Dir) ->
    _ = file:del_dir_r(Dir),
    ok.

%% Guards against reporting meaningless throughput numbers.
%%
%% Calibrates fsync latency in the data dir. On tmpfs this comes back as a few
%% microseconds; a real SSD/HDD is two to three orders of magnitude slower.
%% Below the threshold we refuse to publish, because the whole point of S0.3
%% is deciding whether self-hosted fsync throughput is viable (README 5.3).
-spec assert_real_disk() -> ok | {error, {tmpfs_or_cache, non_neg_integer()}}.
assert_real_disk() ->
    Dir = tmp_dir("calib"),
    Path = filename:join(Dir, "calib.bin"),
    {ok, Fd} = file:open(Path, [write, raw, binary]),
    Data = binary:copy(<<"x">>, 4096),
    N = 100,
    T0 = erlang:monotonic_time(microsecond),
    lists:foreach(fun(_) ->
        ok = file:write(Fd, Data),
        ok = file:sync(Fd)
    end, lists:seq(1, N)),
    UsPer = (erlang:monotonic_time(microsecond) - T0) / N,
    _ = file:close(Fd),
    ok = rm_rf(Dir),
    case UsPer >= ?MIN_REAL_FSYNC_US of
        true -> ok;
        false -> {error, {tmpfs_or_cache, round(UsPer)}}
    end.

ceil_div(A, B) when B > 0 -> (A + B - 1) div B.

str_arg(Key, Args, Default) ->
    case lists:keyfind(Key, 1, Args) of
        {Key, Value} -> Value;
        false -> Default
    end.

%% Args come as a flat ["key","value",...] list because that is what is
%% practical to type into an erl -eval shell string. A proplist is not.
int_arg(Key, Args, Default) ->
    case flat_lookup(Key, Args) of
        {ok, Value} -> list_to_integer(Value);
        error -> Default
    end.

flat_lookup(Key, [Key, Value | _]) -> {ok, Value};
flat_lookup(Key, [_ | Rest]) -> flat_lookup(Key, Rest);
flat_lookup(_Key, []) -> error.

format(#{throughput := Tp, group_commit := GC, recovery := Rec}) ->
    Lines =
        ["=== S0.3 WAL + group commit ===\n",
         "  sequential append:\n",
         io_lib:format("    os_buffer : ~p msgs/s (~p us/msg)~n",
                       [maps:get(msgs_per_sec, maps:get(os_buffer, Tp)),
                        maps:get(us_per_msg, maps:get(os_buffer, Tp))]),
         io_lib:format("    fsync     : ~p msgs/s (~p us/msg)~n",
                       [maps:get(msgs_per_sec, maps:get(fsync, Tp)),
                        maps:get(us_per_msg, maps:get(fsync, Tp))]),
         "  group commit (fsync per batch):\n",
         io_lib:format("    batch=~p : ~p msgs/s, ~p fsyncs total~n",
                       [maps:get(batch_size, GC), maps:get(msgs_per_sec, GC),
                        maps:get(fsyncs, GC)]),
         "  recovery:\n",
         io_lib:format("    replayed ~p/~p in ~p ms (lossless=~p)~n",
                       [maps:get(replayed, Rec), maps:get(written, Rec),
                        maps:get(replay_ms, Rec), maps:get(lossless, Rec)])],
    lists:flatten(Lines).