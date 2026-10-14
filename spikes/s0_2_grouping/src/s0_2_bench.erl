%% S0.2 - Raft grouping model experiment driver.
%%
%% Single-node experiment. For each group count K in the sweep we:
%%   1. snapshot baseline resource use (after ra is up, before any group),
%%   2. form K independent 1-voter ra groups,
%%   3. snapshot resource use again -> per-group overhead,
%%   4. fire W writes round-robin across the K groups, recording each write's
%%      wall time, then derive aggregate throughput and tail latency,
%%   5. stop the K groups (so the next K starts clean).
%%
%% This isolates per-group MACHINERY overhead. The per-write REPLICATED cost on
%% a real 3-node deploy is S0.1's ~677 us/write; multiply for production.
-module(s0_2_bench).

-export([main/0, measure_point/1, form_groups/1, stop_groups/1,
         resource/0, bench_writes/2]).

-define(W, 2000).              %% total writes per point (fixed across K)
-define(K_POINTS, [1, 4, 8, 16, 32, 64, 128]).
-define(WRITE_TIMEOUT, 8000).

%% ---------------------------------------------------------------------------
%% entry point: run all points, print table, dump a results file
%% ---------------------------------------------------------------------------
main() ->
    {ok, _} = application:ensure_all_started(ra),
    _ = ra_system:start_default(),
    %% give the default system a moment, then baseline.
    timer:sleep(500),
    Baseline = resource(),
    io:format("~n=== S0.2 Raft grouping sweep (single node, ~p writes/point) ===~n", [?W]),
    io:format("baseline after ra up: ~p~n~n", [Baseline]),
    Rows = [begin
                R = measure_point(K),
                print_row(K, R, Baseline),
                R#{k => K}
            end || K <- ?K_POINTS],
    File = filename:join(data_dir(), "RESULTS.txt"),
    ok = filelib:ensure_dir(File),
    ok = file:write_file(File, format_report(Rows, Baseline)),
    io:format("~nresults written to ~s~n", [File]),
    halt(0).

measure_point(K) ->
    T0 = erlang:monotonic_time(microsecond),
    ok = form_groups(K),
    FormUs = erlang:monotonic_time(microsecond) - T0,
    %% wait for every group to have a leader before benchmarking.
    ok = await_leaders(K, 50),
    Post = resource(),
    #{writes := ?W, succeeded := Ok, failed := Fail,
      elapsed_us := WUs, durations := Durs} = bench_writes(K, ?W),
    ok = stop_groups(K),
    #{form_us => FormUs,
      post_procs => maps:get(procs, Post),
      post_ets => maps:get(ets, Post),
      post_mem_mb => round(maps:get(mem, Post) / (1024 * 1024)),
      writes => ?W, succeeded => Ok, failed => Fail,
      write_us => WUs,
      writes_per_sec => round(?W / (WUs / 1000000)),
      p50_us => pct(Durs, 0.50),
      p95_us => pct(Durs, 0.95),
      p99_us => pct(Durs, 0.99)}.

%% ---------------------------------------------------------------------------
%% group lifecycle
%% ---------------------------------------------------------------------------
group_name(I) -> list_to_atom("g" ++ integer_to_list(I)).

form_groups(K) ->
    Machine = {module, s0_2_machine, #{}},
    %% One cluster call per group. start_or_restart_cluster/5 is the only entry
    %% that bootstraps membership AND triggers an election; start_server/2 from
    %% a single node leaves the group leaderless (see S0.1 REPORT 3.1).
    [begin
         _ = ra:start_or_restart_cluster(default, Group, Machine,
                                        [{Group, node()}], 30000)
     end || Group <- [group_name(I) || I <- lists:seq(1, K)]],
    ok.

stop_groups(K) ->
    [catch ra:stop_server({group_name(I), node()})
     || I <- lists:seq(1, K)],
    ok.

await_leaders(K, Tries) when Tries > 0 ->
    case [G || G <- [group_name(I) || I <- lists:seq(1, K)],
               not has_leader(G)] of
        [] -> ok;
        _Pending when Tries > 1 ->
            timer:sleep(100),
            await_leaders(K, Tries - 1);
        Pending ->
            {timeout_waiting_leaders, Pending}
    end;
await_leaders(_K, 0) -> ok.

has_leader(Group) ->
    case ra:members({local, {Group, node()}}, 2000) of
        {ok, _M, Leader} when Leader =/= undefined -> true;
        _ -> false
    end.

%% ---------------------------------------------------------------------------
%% write benchmark: W writes round-robin across K groups
%% ---------------------------------------------------------------------------
bench_writes(K, W) ->
    Leaders = [leader_of(group_name(I)) || I <- lists:seq(1, K)],
    T0 = erlang:monotonic_time(microsecond),
    {Durs, Ok} = lists:foldl(
             fun(I, {Acc, Cnt}) ->
                     G = (I rem K) + 1,
                     Leader = lists:nth(G, Leaders),
                     S = erlang:monotonic_time(microsecond),
                     Res = ra:process_command(Leader, {write, I, {bench, I}},
                                              ?WRITE_TIMEOUT),
                     E = erlang:monotonic_time(microsecond),
                     case Res of
                         {ok, ok, _} ->
                             {[binary:encode_unsigned(round(E - S)) | Acc], Cnt + 1};
                         _ ->
                             {Acc, Cnt}
                     end
             end, {[], 0}, lists:seq(1, W)),
    Elapsed = erlang:monotonic_time(microsecond) - T0,
    #{writes => W, succeeded => Ok, failed => W - Ok,
      elapsed_us => Elapsed,
      durations => lists:sort([binary:decode_unsigned(D) || D <- Durs])}.

leader_of(Group) ->
    {ok, _M, Leader} = ra:members({local, {Group, node()}}, 5000),
    Leader.

%% ---------------------------------------------------------------------------
%% resource snapshot
%% ---------------------------------------------------------------------------
resource() ->
    #{procs => erlang:system_info(process_count),
      ets => length(ets:all()),
      mem => erlang:memory(total)}.

%% ---------------------------------------------------------------------------
%% helpers
%% ---------------------------------------------------------------------------
pct([], _P) -> 0;
pct(Sorted, P) ->
    N = length(Sorted),
    Idx = max(1, min(N, ceil(P * N))),
    lists:nth(Idx, Sorted).

data_dir() ->
    case os:getenv("SPIKE_DATA_DIR") of
        false ->
            filename:join([filename:dirname(code:which(?MODULE)), "..", ".data"]);
        D -> D
    end.

%% ---------------------------------------------------------------------------
%% reporting
%% ---------------------------------------------------------------------------
print_row(K, R, Baseline) ->
    ProcsPer = round((maps:get(post_procs, R) - maps:get(procs, Baseline)) / K),
    MemPer = round((maps:get(post_mem_mb, R) * 1024 * 1024
                    - maps:get(mem, Baseline)) / K / 1024),
    io:format("K=~3.. B: form=~7.w us  procs=~5.w (~w/grp)  "
              "mem=~5.w MB (~w KB/grp)  writes/s=~7.w  "
              "p50=~5.w p95=~6.w p99=~6.w us  fail=~w~n",
              [K, maps:get(form_us, R), maps:get(post_procs, R), ProcsPer,
               maps:get(post_mem_mb, R), MemPer, maps:get(writes_per_sec, R),
               maps:get(p50_us, R), maps:get(p95_us, R), maps:get(p99_us, R),
               maps:get(failed, R)]).

format_report(Rows, Baseline) ->
    L = ["=== S0.2 Raft grouping sweep results ===\n",
         io_lib:format("baseline: ~p\n", [Baseline]),
         "K | form_us | procs | ets | mem_MB | writes/s | p50_us | p95_us | p99_us | failed\n"],
    L ++ [io_lib:format("~p | ~p | ~p | ~p | ~p | ~p | ~p | ~p | ~p | ~p\n",
                        [maps:get(k, R), maps:get(form_us, R),
                         maps:get(post_procs, R), maps:get(post_ets, R),
                         maps:get(post_mem_mb, R), maps:get(writes_per_sec, R),
                         maps:get(p50_us, R), maps:get(p95_us, R),
                         maps:get(p99_us, R), maps:get(failed, R)])
            || R <- Rows].
