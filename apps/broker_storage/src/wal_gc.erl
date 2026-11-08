%% @doc Retention policy driver: decides which whole WAL segments may be
%% physically deleted and asks the partition writer to do it (ADR-014).
%%
%% Two things are deliberately separated here:
%%
%%  * `scan/1' + `plan/3' are pure/read-only: they look at the directory and at
%%    a writer's watermarks and produce a decision. Tests can drive the whole
%%    policy without starting a process or a partition.
%%  * `run/0' (and the periodic tick) applies the decision through
%%    `wal_writer:truncate/2', which is the only place a file is ever unlinked.
%%    That keeps the write path's shape (ADR-011 §11.2) and means GC never
%%    races the writer: both operations are serialized inside that process.
%%
%% The one hard rule is ADR-014 §14.1: never delete past `committed'. Because
%% `committed' is `min(live cursor) - 1' (K6), a slow consumer pins it and the
%% GC deletes nothing -- it only counts `retention_blocked_by_slow_consumer'
%% and reports the reason through `status/0'. The storage layer never asks the
%% subscription layer for a cursor; doing so would invert the dependency and
%% make the deletion decision depend on a second source of truth.
-module(wal_gc).

-behaviour(gen_server).

-include_lib("kernel/include/file.hrl").
-include_lib("broker_core/include/broker_message.hrl").

-export([start_link/0, run/0, status/0, scan/1, plan/3, retention_config/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(WAL_SUBDIR, "wal").
-define(DEFAULT_DATA_DIR, "data").
-define(DEFAULT_CHECK_INTERVAL_MS, 60000).
-define(DEFAULT_MAX_BYTES, 0).
-define(DEFAULT_MAX_AGE_MS, 0).

-type segment() :: #{start := offset(),
                     path := file:filename_all(),
                     size := non_neg_integer(),
                     mtime_ms := integer()}.
-type scanned() :: #{partition := partition_id(),
                     dir := file:filename_all(),
                     total_bytes := non_neg_integer(),
                     segments := [segment()]}.
-type decision() :: #{cut := offset(),
                      deletes := [offset()],
                      deletable_bytes := non_neg_integer(),
                      blocked_reason := none | slow_consumer | no_writer}.

-export_type([scanned/0, decision/0]).

%%%_* API ----------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc One sweep across every partition directory. Safe to call when the
%% process is not running (a memory-backend node, or a unit test).
-spec run() -> ok.
run() ->
    case whereis(?MODULE) of
        undefined -> ok;
        Pid -> gen_server:call(Pid, run, 30000)
    end.

%% @doc Per-partition retention view for operators and tests:
%% `[{PartitionId, OldestSegment, DeletableBytes, BlockedReason}]'.
-spec status() -> [{partition_id(), offset() | undefined, non_neg_integer(),
                    atom()}].
status() ->
    Retention = retention_config(),
    [status_for(Scan, Retention) || Scan <- scan(data_dir())].

%% @doc Enumerate `data_dir/wal/<partition>' directories. A missing `wal'
%% directory is an empty result, not an error: a node that never created a
%% partition has nothing to collect.
-spec scan(file:filename_all()) -> [scanned()].
scan(DataDir) ->
    WalDir = filename:join(DataDir, ?WAL_SUBDIR),
    case file:list_dir(WalDir) of
        {ok, Names} ->
            lists:filtermap(fun(Name) -> scan_partition(WalDir, Name) end, Names);
        {error, _} ->
            []
    end.

%% @doc Pure policy: `Scan' and `Info' in, a decision out. No file is touched.
-spec plan(scanned(), map(), map()) -> decision().
plan(#{segments := Segments} = Scan, Retention, Info) ->
    #{committed := Committed, truncate_before := Trunc,
      active_segment := Active} = Info,
    MaxBytes = maps:get(max_bytes, Retention, ?DEFAULT_MAX_BYTES),
    MaxAgeMs = maps:get(max_age_ms, Retention, ?DEFAULT_MAX_AGE_MS),
    Now = now_ms(),
    Candidates = candidates(Segments, Active),
    Eligible = [C || #{next := Next} = C <- Candidates, Next =< Committed + 1],
    ByteDeletes = byte_deletes(Eligible, maps:get(total_bytes, Scan), MaxBytes),
    AgeDeletes = age_deletes(Eligible, Now, MaxAgeMs),
    Deletes = combine(ByteDeletes, AgeDeletes, MaxBytes, MaxAgeMs),
    #{cut => cut(Trunc, Deletes),
      deletes => [maps:get(start, D) || D <- Deletes],
      deletable_bytes => sum_sizes(Eligible),
      blocked_reason => blocked(Scan, Candidates, Eligible, Retention, Now)}.

%% @doc The `retention' map from `broker_storage' env, with defaults. Exported
%% so the app env and the tests agree on one shape.
-spec retention_config() -> map().
retention_config() ->
    case application:get_env(broker_storage, retention, #{}) of
        Map when is_map(Map) -> Map;
        _ -> #{}
    end.

%%%_* gen_server ---------------------------------------------------------

init([]) ->
    State = #{data_dir => data_dir(), retention => retention_config()},
    Interval = maps:get(check_interval_ms, retention_config(),
                        ?DEFAULT_CHECK_INTERVAL_MS),
    {ok, schedule(State, Interval)}.

handle_call(run, _From, State) ->
    ok = sweep(State),
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unsupported}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(tick, State) ->
    ok = sweep(State),
    Interval = maps:get(check_interval_ms, maps:get(retention, State),
                        ?DEFAULT_CHECK_INTERVAL_MS),
    {noreply, schedule(State, Interval)};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%%_* Sweep --------------------------------------------------------------

sweep(#{data_dir := DataDir, retention := Retention}) ->
    lists:foreach(fun(Scan) -> collect(Scan, Retention) end, scan(DataDir)),
    ok.

collect(#{partition := PartitionId} = Scan, Retention) ->
    case writer_pid(PartitionId) of
        undefined ->
            ok;
        Pid ->
            try wal_writer:retention_info(Pid) of
                Info ->
                    Decision = plan(Scan, Retention, Info),
                    maybe_count_blocked(Decision),
                    case maps:get(deletes, Decision) of
                        [] -> ok;
                        _ -> _ = wal_writer:truncate(Pid, maps:get(cut, Decision)), ok
                    end
            catch
                _:_ ->
                    %% The writer is restarting or the partition was closed;
                    %% the next tick will see it again.
                    ok
            end
    end.

maybe_count_blocked(#{blocked_reason := slow_consumer}) ->
    broker_metrics:inc(retention_blocked_by_slow_consumer);
maybe_count_blocked(_) ->
    ok.

schedule(State, Interval) when is_integer(Interval), Interval > 0 ->
    State#{timer => erlang:send_after(Interval, self(), tick)};
schedule(State, _Interval) ->
    %% 0/negative check interval disables the periodic sweep; `run/0' still
    %% works, which is what the tests and an operator-triggered GC use.
    State#{timer => undefined}.

%%%_* Policy -------------------------------------------------------------

%% Every segment but the active one, annotated with the offset that follows
%% it. That next start is what makes "the segment is entirely below X" a single
%% comparison instead of folding the file.
candidates(Segments, Active) ->
    Pairs = lists:zip(Segments, tl(Segments) ++ [undefined]),
    [Seg#{next => maps:get(start, Next)}
     || {Seg, Next} <- Pairs,
        maps:get(path, Seg) =/= Active,
        Next =/= undefined].

%% Oldest-first deletion until the partition is under `max_bytes'. If the
%% eligible prefix runs out, the decision still carries everything it could
%% remove; `blocked/5' explains why that was not enough.
byte_deletes(_Eligible, _Total, MaxBytes) when MaxBytes =< 0 ->
    [];
byte_deletes(_Eligible, Total, MaxBytes) when Total =< MaxBytes ->
    [];
byte_deletes(Eligible, Total, MaxBytes) ->
    take_until(Eligible, Total, MaxBytes, []).

take_until([], _Remaining, _MaxBytes, Acc) ->
    lists:reverse(Acc);
take_until([Seg | Rest], Remaining, MaxBytes, Acc) ->
    Remaining1 = Remaining - maps:get(size, Seg),
    case Remaining1 =< MaxBytes of
        true -> lists:reverse([Seg | Acc]);
        false -> take_until(Rest, Remaining1, MaxBytes, [Seg | Acc])
    end.

age_deletes(_Eligible, _Now, MaxAgeMs) when MaxAgeMs =< 0 ->
    [];
age_deletes(Eligible, Now, MaxAgeMs) ->
    [Seg || #{mtime_ms := MTime} = Seg <- Eligible, MTime < Now - MaxAgeMs].

%% ADR-014 §14.2: "both configured means the stricter of the two", i.e. a
%% segment goes only when both the size and the age limits agree.
combine(Byte, Age, MaxBytes, MaxAgeMs) ->
    case {MaxBytes > 0, MaxAgeMs > 0} of
        {true, true} ->
            AgeStarts = [maps:get(start, S) || S <- Age],
            [S || S <- Byte, lists:member(maps:get(start, S), AgeStarts)];
        {true, false} ->
            Byte;
        {false, true} ->
            Age;
        {false, false} ->
            []
    end.

cut(Trunc, []) ->
    Trunc;
cut(_Trunc, Deletes) ->
    %% Deletes is a prefix, so the last one's next start is the furthest point
    %% this decision may advance the logical boundary to. It is already
    %% `=< committed + 1' by construction of `Eligible'.
    Nexts = [maps:get(next, S) || S <- Deletes],
    lists:max(Nexts).

%% A "blocked" partition is one where the policy wanted to remove bytes but
%% §14.1's committed guard left nothing eligible. That is the slow-consumer
%% case the ADR makes loud rather than silently fixing by deleting unacked data.
blocked(Scan, Candidates, [], Retention, Now) ->
    MaxBytes = maps:get(max_bytes, Retention, ?DEFAULT_MAX_BYTES),
    MaxAgeMs = maps:get(max_age_ms, Retention, ?DEFAULT_MAX_AGE_MS),
    OverBytes = MaxBytes > 0 andalso maps:get(total_bytes, Scan) > MaxBytes,
    Expired = MaxAgeMs > 0 andalso
        lists:any(fun(#{mtime_ms := MTime}) -> MTime < Now - MaxAgeMs end,
                  Candidates),
    case OverBytes orelse Expired of
        true -> slow_consumer;
        false -> none
    end;
blocked(_Scan, _Candidates, _Eligible, _Retention, _Now) ->
    none.

sum_sizes(Segments) ->
    lists:sum([maps:get(size, S) || S <- Segments]).

%%%_* Status -------------------------------------------------------------

status_for(#{partition := PartitionId} = Scan, Retention) ->
    Segments = maps:get(segments, Scan),
    Oldest = case Segments of
                 [#{start := Start} | _] -> Start;
                 [] -> undefined
             end,
    case writer_pid(PartitionId) of
        undefined ->
            {PartitionId, Oldest, 0, no_writer};
        Pid ->
            try wal_writer:retention_info(Pid) of
                Info ->
                    Decision = plan(Scan, Retention, Info),
                    {PartitionId, Oldest, maps:get(deletable_bytes, Decision),
                     maps:get(blocked_reason, Decision)}
            catch
                _:_ ->
                    {PartitionId, Oldest, 0, no_writer}
            end
    end.

%%%_* Filesystem ---------------------------------------------------------

scan_partition(WalDir, Name) ->
    case string:to_integer(Name) of
        {PartitionId, []} when PartitionId >= 0 ->
            Dir = filename:join(WalDir, Name),
            case filelib:is_dir(Dir) of
                true ->
                    case wal_segment:list(Dir) of
                        {ok, #{segments := Segments}} ->
                            {true, #{partition => PartitionId,
                                     dir => Dir,
                                     total_bytes => sum_file_sizes(Segments),
                                     segments => [segment_info(S, P)
                                                  || {S, P} <- Segments]}};
                        {error, _} ->
                            false
                    end;
                false ->
                    false
            end;
        _ ->
            false
    end.

segment_info(Start, Path) ->
    #{start => Start,
      path => Path,
      size => filelib:file_size(Path),
      mtime_ms => mtime_ms(Path)}.

sum_file_sizes(Segments) ->
    lists:sum([filelib:file_size(Path) || {_Start, Path} <- Segments]).

mtime_ms(Path) ->
    case file:read_file_info(Path, [{time, posix}]) of
        {ok, #file_info{mtime = Seconds}} -> Seconds * 1000;
        {error, _} -> 0
    end.

writer_pid(PartitionId) ->
    try supervisor:which_children(broker_storage_wal_sup) of
        Children ->
            case lists:keyfind({wal_writer, PartitionId}, 1, Children) of
                {{wal_writer, PartitionId}, Pid, _Type, _Modules} when is_pid(Pid) ->
                    Pid;
                _ ->
                    undefined
            end
    catch
        _:_ ->
            undefined
    end.

data_dir() ->
    application:get_env(broker_storage, data_dir, ?DEFAULT_DATA_DIR).

now_ms() ->
    erlang:system_time(millisecond).
