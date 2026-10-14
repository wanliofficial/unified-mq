%% S0.3 - crash harness: measures the durability window per ACK grade.
%%
%% Purpose: make README 6.3 concrete. For each grade we SIGKILL a real
%% external writer process mid-stream, then inspect what survived.
%%
%% IMPORTANT MEASUREMENT LIMIT (do not misread the results):
%%
%%   A `kill -9` destroys the PROCESS, but data already handed to write(2)
%%   lives in the OS page cache and survives process death. Only a kernel
%%   panic or power loss discards it. Therefore:
%%
%%     - kill -9 CANNOT demonstrate loss for the os_buffer grade. It will
%%       look lossless, and that result is an artifact, not a guarantee.
%%     - kill -9 CAN validate the fsync grade: fsync must still show
%%       lossless, and it does so for a stronger reason.
%%
%%   The os_buffer loss window can only be measured with a power-failure
%%   simulator (dm-flakey / dm-log-writes / a VM hard reset). Until that
%%   exists, the os_buffer row of the ACK contract table stays marked
%%   "not yet measured" rather than being guessed. See REPORT.md.
-module(s0_3_crash).

-export([writer_main/5, verify/3, run/2, run_all/1, format/1]).

-define(DEFAULT_ACKED, 3000).
-define(KILL_AFTER_MS, 500).
-define(POLL_ATTEMPTS, 400).

%% writer_main(Dir, Grade, Acked, PidFile, ReadyFile) -> no_return()
%%
%% Runs in the spawned external VM. Writes Acked messages at the given grade,
%% publishes its OS pid so the harness can kill it, then writes an unsynced
%% tail until we are SIGKILLed.
%%
%% ReadyFile is written only after the durable prefix is on disk, so the
%% harness never kills us mid-prefix. Killing during the fsync prefix would
%% measure "how far did the writer get", not "what survives".
writer_main(Dir, Grade, Acked, PidFile, ReadyFile) ->
    ok = write_pid_file(PidFile),
    {ok, W0} = s0_3_wal:open(Dir, #{}),
    W1 = write_n(W0, Grade, Acked, 0),
    %% The prefix is durable. Tell the harness it may now kill us.
    ok = file:write_file(ReadyFile, <<"ready">>),
    %% Spin on the tail so there is always unsynced data in flight.
    tail_loop(W1, Acked).

%% verify(Dir, Grade, Acked) -> map()
%%
%% Only messages 0..Acked-1 matter: those are the ones written at the durable
%% grade and the only ones we would ever have acked to a producer. The tail
%% records the writer spins on afterwards are counted separately, because
%% comparing their count against Acked would be meaningless.
verify(Dir, Grade, Acked) ->
    {ok, Terms} = s0_3_wal:read_all(Dir),
    Indices = lists:sort([I || {msg, I, _} <- Terms]),
    AckedIdx = [I || I <- Indices, I < Acked],
    TailIdx = [I || I <- Indices, I >= Acked],
    #{grade => Grade,
      acked => Acked,
      acked_recovered => length(AckedIdx),
      acked_lost => Acked - length(AckedIdx),
      acked_all_recovered => acked_recovered(Indices, Acked),
      tail_recovered => length(TailIdx),
      tail_max_idx => lists:max([-1 | TailIdx]),
      contiguous => is_contiguous(Indices)}.

%% run(Grade, Acked) -> map()
%%
%% Spawns a real external VM, waits for it to report its pid, SIGKILLs it,
%% then verifies. The kill is genuine SIGKILL: no shutdown callbacks, no
%% flush, no close.
run(Grade, Acked) ->
    Dir = tmp_dir(atom_to_list(Grade)),
    PidFile = filename:join(Dir, "writer.pid"),
    ReadyFile = filename:join(Dir, "writer.ready"),
    ok = spawn_writer(Dir, Grade, Acked, PidFile, ReadyFile),
    OsPid = await_file(PidFile, pid),
    %% Wait for the durable prefix to land before killing. Killing during the
    %% fsync prefix would measure "how far did the writer get", not
    %% "what survives".
    _ = await_file(ReadyFile, ready),
    timer:sleep(?KILL_AFTER_MS),
    os:cmd("kill -9 " ++ OsPid),
    timer:sleep(300),
    Result = verify(Dir, Grade, Acked),
    ok = rm_rf(Dir),
    Result.

run_all(Acked) ->
    #{fsync => run(fsync, Acked),
      os_buffer => run(os_buffer, Acked)}.

%% ---------------------------------------------------------------- internals

write_n(W, _Grade, 0, _N) -> W;
write_n(W, Grade, Left, N) ->
    {ok, W1} = s0_3_wal:append(W, {msg, N, pad()}, Grade),
    write_n(W1, Grade, Left - 1, N + 1).

%% Keep unsynced records in flight so the os_buffer grade has something to
%% lose. Never returns; we get SIGKILLed.
tail_loop(W, N) ->
    {ok, W1} = s0_3_wal:append(W, {msg, N, pad()}, os_buffer),
    tail_loop(W1, N + 1).

write_pid_file(Path) ->
    file:write_file(Path, os:getpid()).

%% Spawn a detached VM. Detached so our own SIGKILL does not race us.
spawn_writer(Dir, Grade, Acked, PidFile, ReadyFile) ->
    Script = lists:flatten(
        io_lib:format("exec erl -pa _build/default/lib/s0_3_wal/ebin -noshell "
                      "-eval 's0_3_crash:writer_main(\"~s\", ~p, ~p, \"~s\", \"~s\").'",
                      [Dir, Grade, Acked, PidFile, ReadyFile])),
    _ = open_port({spawn_executable, "/bin/sh"},
                  [{args, ["-c", Script]}, exit_status, hide]),
    ok.

%% Poll until Path has content, then return it trimmed.
await_file(Path, Tag) -> await_file(Path, Tag, ?POLL_ATTEMPTS).

await_file(Path, _Tag, 0) -> error({writer_never_signalled, Path});
await_file(Path, Tag, Attempts) ->
    case file:read_file(Path) of
        {ok, Bin} ->
            Str = string:trim(binary_to_list(Bin)),
            case Str of
                [] -> retry(Path, Tag, Attempts);
                _ -> Str
            end;
        {error, _} ->
            retry(Path, Tag, Attempts)
    end.

retry(Path, Tag, Attempts) ->
    timer:sleep(50),
    await_file(Path, Tag, Attempts - 1).

%% Messages 0..Acked-1 were written at the durable grade.
acked_recovered(_Indices, Acked) when Acked =:= 0 -> true;
acked_recovered(Indices, Acked) ->
    lists:all(fun(I) -> lists:member(I, Indices) end, lists:seq(0, Acked - 1)).

is_contiguous([]) -> true;
is_contiguous(Indices) -> Indices =:= lists:seq(0, length(Indices) - 1).

pad() -> binary:copy(<<"p">>, 256).

tmp_dir(Tag) ->
    Base = case os:getenv("SPIKE_DATA_DIR") of
        false -> "/home/wanli/workspace/unified-mq/spikes/.data";
        D -> D
    end,
    Dir = filename:join(Base, "crash_" ++ Tag ++ "_"
                        ++ integer_to_list(erlang:unique_integer([positive]))),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    Dir.

rm_rf(Dir) ->
    _ = file:del_dir_r(Dir),
    ok.

format(#{grade := Grade, acked := Acked, acked_recovered := Rec,
         acked_lost := Lost, acked_all_recovered := All,
         tail_recovered := Tail, contiguous := Contig}) ->
    io_lib:format("  ~p: acked=~p recovered=~p lost=~p all_recovered=~p "
                  "tail_recovered=~p contiguous=~p~n",
                  [Grade, Acked, Rec, Lost, All, Tail, Contig]).