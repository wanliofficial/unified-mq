%% @doc S11x router read-path benchmark: what a publish costs at N subscriptions.
%%
%% `docs/design/router-read-path.md' §7.3 asks for three subscription counts
%% (1 / 1k / 10k) under three implementations (old flat scan, env cache only,
%% env cache + trie). The existing harnesses cannot answer that: `s2_bench.py'
%% drives a release over MQTT, so it measures the whole stack and only at one
%% subscription count; `s6_bench_extra' measures storage. This one calls
%% `broker_router_hash:route/3' directly, which is exactly the work a publish
%% pays between "message arrived" and "[{Partition, Refs}]".
%%
%% Three implementations, one VM, one machine, same data:
%%
%%   old   -- the code before change 1: `current_env/0' was a gen_server:call
%%            and `route/3' did `ets:tab2list/1' + a linear match.
%%   env   -- change 1: env read from persistent_term, index still flat.
%%   trie  -- change 2: env read from persistent_term, index is a trie.
%%
%% Usage (from the repo root, after `rebar3 compile'):
%%
%%   erlc -pa _build/default/lib/broker_router/ebin -o /tmp/opencode/ebin \
%%        tools/bench/router_read_bench.erl
%%   erl -pa _build/default/lib/*/ebin -pa /tmp/opencode/ebin -noshell \
%%       -eval 'router_read_bench:run()' -s init stop
%%
%% Output is Erlang terms so the report can quote them verbatim. Nothing here
%% is a correctness test; `broker_router_hash_SUITE' owns that.
-module(router_read_bench).

-export([run/0, run/1, concurrent/1]).

-include_lib("broker_core/include/broker_message.hrl").
-include_lib("broker_router/include/broker_router.hrl").

-define(STORE, broker_router_bench_store).
-define(HASH, broker_router_hash).
%% One tenant, one topic depth, 1 KiB-ish destinations: the shape that makes
%% the old read path's O(total subscriptions) copy visible.
-define(TOPIC_DEPTH, 4).

%%%_* Public ---------------------------------------------------------------

run() ->
    run(#{}).

%% Two shapes, because they answer different questions:
%%
%%   spread  -- N filters over distinct destinations; one publish hits 3
%%              subscribers (one exact + two wildcards). Measures "does the
%%              index cost grow with the number of subscriptions?"
%%   fanout  -- N subscribers all on the same filter; one publish hits all N
%%              (plus the wildcards). Measures "what does a single publish pay
%%              when it must notify many subscribers?"
%%
%% The report needs both: `spread' is where the trie's search advantage shows,
%% `fanout' is where the result-set cost dominates whatever found it.
run(Opts) ->
    Counts = maps:get(counts, Opts, [1, 1000, 10000]),
    Shapes = maps:get(shapes, Opts, [spread]),
    DurationMs = maps:get(duration_ms, Opts, 1000),
    WarmupMs = maps:get(warmup_ms, Opts, 200),
    Results =
        [begin
             Rows = [{N, Impl, measure(Shape, Impl, N, DurationMs, WarmupMs)}
                     || Impl <- [old, env, trie]],
             io:format("~n=== ~p / ~p subscriptions ===~n", [Shape, N]),
             [io:format("~-5s ~p~n", [Impl, R]) || {_, Impl, R} <- Rows],
             [{shape, Shape}, {subscriptions, N} | Rows]
         end
         || Shape <- Shapes, N <- Counts],
    io:format("~nRESULT ~p~n", [Results]),
    Results.

%% `spread': N filters over distinct destinations. One `#' and one `+' filter
%% ride along so both wildcard branches are exercised.
subscriptions(spread, N) ->
    [{I, filter(I)} || I <- lists:seq(1, N)] ++
        [{wildcard_hash, <<"bench/#">>}, {wildcard_plus, <<"bench/+/+/+">>}];
%% `fanout': N subscribers on the *same* filter, the shape of
%% "devices/+/status" with 10k devices.
subscriptions(fanout, N) ->
    [{I, <<"bench/fanout/status">>} || I <- lists:seq(1, N)] ++
        [{wildcard_hash, <<"bench/#">>}, {wildcard_plus, <<"bench/+/+/+">>}].

filter(I) ->
    iolist_to_binary(["bench/", integer_to_list(I rem 100), "/",
                      integer_to_list(I rem 37), "/dev", integer_to_list(I)]).

numeric(Subs) ->
    [S || {R, _} = S <- Subs, is_integer(R)].

%% The publish destination for each shape, derived from the subscription set
%% rather than written by hand: a destination no filter covers measures "no
%% subscriber" work and reports nothing useful. Both shapes are asserted below.
destination(fanout, _Subs) ->
    <<"bench/fanout/status">>;
destination(spread, Subs) ->
    %% SubRef 1007 exists from N = 1007 up; below that use the largest numeric
    %% SubRef (the wildcard filters carry atom SubRefs and are not destinations).
    case lists:keyfind(1007, 1, Subs) of
        {_, D} -> D;
        false -> element(2, lists:max(numeric(Subs)))
    end.

%% How many of the registered subs a correct route must return.
expected_matches(fanout, N) -> N + 1;   % all fanout subs + `bench/#'
expected_matches(spread, _N) -> 3.      % one exact + both wildcards

%%%_* Measurement ----------------------------------------------------------

measure(Shape, Impl, N, DurationMs, WarmupMs) ->
    Subs = subscriptions(Shape, N),
    Env = setup(Impl, Subs),
    ok = register_all(Env, Subs),
    Dest = destination(Shape, Subs),
    Ctx = #route_ctx{tenant_id = <<"default">>, publisher = undefined,
                     reliability = os_buffer},
    {ok, [{_P, Matched}]} = route_one(Impl, Dest, Ctx, Env),
    %% The index must actually match, or every row would measure "no
    %% subscriber" work -- both a wrong destination and a wrong row shape
    %% produced such rows while building this script. And no matched SubRef may
    %% be phantom, or a baseline would look fast for the wrong reason. Both are
    %% errors in the harness, and a benchmark that silently measures nothing is
    %% worse than one that crashes.
    Matched =/= [] orelse erlang:error({no_match, Shape, Impl, N, Dest}),
    true = lists:all(fun(R) -> lists:keymember(R, 1, Subs) end, Matched),
    %% Every destination is written under `bench/', so the `bench/#' filter
    %% always matches and `old'/`env'/`trie' must agree on it. If they ever
    %% disagree the row is measuring a different index than the baseline.
    %% `route/3' yields bare SubRefs (`[1, wildcard_hash]'), not `{Ref, Seq}'
    %% pairs: the seq ordering is applied inside the router. Matching them as
    %% pairs yields an empty list, so this assertion was checking nothing --
    %% which is how a correct `old' row came out failing.
    true = lists:member(wildcard_hash, Matched),
    %% The three implementations must agree on *how many* subscribers a
    %% destination has. Otherwise "faster" can just mean "found less": that is
    %% precisely the failure mode the fan-out shape exists to catch, since there
    %% a cheaper scan that silently drops half the result set would look great.
    Expected = expected_matches(Shape, N),
    length(Matched) =:= Expected
        orelse erlang:error({wrong_match_count, Shape, Impl, N, Expected,
                             length(Matched)}),
    _ = warmup(Impl, Dest, Ctx, Env, WarmupMs),
    {Ops, Us} = drive(Impl, Dest, Ctx, Env, DurationMs),
    %% One route, measured in isolation and repeated: the p50 is what a single
    %% publish actually waits for, and mean throughput hides a path that is
    %% cheap until the table grows.
    Single = single_latency(Impl, Dest, Ctx, Env),
    %% Read every number before teardown: the store owns the table, so a
    %% measurement taken after the stop reads a deleted ets table (that bug
    %% produced index_rows => 0 for every row while building this script).
    Size = table_size(Env),
    teardown(),
    #{ops => Ops,
      ops_per_s => (Ops * 1000000) div max(1, Us),
      route_us_mean => Us / max(1, Ops),
      route_us_p50 => Single,
      matched => length(Matched),
      index_rows => Size}.

%% Throughput: one process, tight loop, no timers. A single caller is the point
%% -- the old path's floor is process serialization, which concurrency would
%% mask, and the new path's floor is the per-message work.
drive(Impl, Dest, Ctx, Env, DurationMs) ->
    Deadline = erlang:monotonic_time(millisecond) + DurationMs,
    T0 = erlang:monotonic_time(microsecond),
    Ops = loop(Impl, Dest, Ctx, Env, Deadline, 0),
    T1 = erlang:monotonic_time(microsecond),
    {Ops, T1 - T0}.

loop(Impl, Dest, Ctx, Env, Deadline, Acc) ->
    case Acc band 255 of
        0 ->
            case erlang:monotonic_time(millisecond) >= Deadline of
                true -> Acc;
                false -> loop1(Impl, Dest, Ctx, Env, Deadline, Acc)
            end;
        _ ->
            loop1(Impl, Dest, Ctx, Env, Deadline, Acc)
    end.

loop1(Impl, Dest, Ctx, Env, Deadline, Acc) ->
    {ok, _} = route_one(Impl, Dest, Ctx, Env),
    loop(Impl, Dest, Ctx, Env, Deadline, Acc + 1).

warmup(_Impl, _Dest, _Ctx, _Env, Ms) when Ms =< 0 ->
    ok;
warmup(Impl, Dest, Ctx, Env, Ms) ->
    Deadline = erlang:monotonic_time(millisecond) + Ms,
    loop(Impl, Dest, Ctx, Env, Deadline, 0).

%% One route, measured in isolation, repeated and sorted: the p50 is what a
%% single publish actually waits for.
single_latency(Impl, Dest, Ctx, Env) ->
    Samples = [begin
                   T0 = erlang:monotonic_time(microsecond),
                   {ok, _} = route_one(Impl, Dest, Ctx, Env),
                   erlang:monotonic_time(microsecond) - T0
               end
               || _ <- lists:seq(1, 201)],
    Sorted = lists:sort(Samples),
    lists:nth(max(1, length(Sorted) div 2), Sorted).

route_one(old, Dest, Ctx, Env) -> old_route(Dest, Ctx, Env);
route_one(env, Dest, Ctx, Env) -> env_route(Dest, Ctx, Env);
route_one(trie, Dest, Ctx, Env) -> trie_route(Dest, Ctx, Env).

%% Aggregate throughput with P concurrent reader processes. This is the axis
%% ADR-010 §10.1 actually claims something about: reads must not go through the
%% writer. In the old path `table/1' is a gen_server:call, so every reader
%% serialises on the store process and adding processes cannot help. In env/trie
%% the env comes from persistent_term and the index is a direct ETS read
%% (`read_concurrency' is on), so throughput should grow with P.
%%
%% Absolute numbers here are load-sensitive -- use the *shape* (does adding a
%% reader add throughput?) rather than the values on a busy machine.
concurrent(Opts) ->
    Shape = maps:get(shape, Opts, spread),
    N = maps:get(counts, Opts, 10000),
    Ps = maps:get(processes, Opts, [1, 2, 4, 8]),
    DurationMs = maps:get(duration_ms, Opts, 1000),
    WarmupMs = maps:get(warmup_ms, Opts, 200),
    io:format("~n%% ~p / ~p subscriptions, ~p ms per cell~n",
              [Shape, N, DurationMs]),
    io:format("~-6s ~-4s ~-12s ~-18s~n", ["impl", "P", "ops/s", "scaling"]),
    Rows =
        [begin
             {PerImpl, _} =
                 lists:mapfoldl(
                   fun(P, Base) ->
                           Ops = run_concurrent(Shape, N, Impl, P, DurationMs, WarmupMs),
                           OpsPerS = (Ops * 1000) div max(1, DurationMs),
                           Base1 = case Base of 0 -> OpsPerS; _ -> Base end,
                           io:format("~-6s ~-4w ~-12w ~s~n",
                                     [Impl, P, OpsPerS, scaling(Base1, P, OpsPerS)]),
                           {{P, OpsPerS}, Base1}
                   end, 0, Ps),
             {Impl, PerImpl}
         end
         || Impl <- [old, env, trie]],
    io:format("~nRESULT ~p~n", [Rows]),
    Rows.

scaling(_Base, 1, _OpsPerS) ->
    "(baseline)";
scaling(Base, P, OpsPerS) ->
    io_lib:format("~.2fx (ideal ~px)", [OpsPerS / max(1, Base), P]).

%% Spawn P workers, release them together, sum their completed routes.
run_concurrent(Shape, N, Impl, P, DurationMs, WarmupMs) ->
    Subs = subscriptions(Shape, N),
    Env = setup(Impl, Subs),
    ok = register_all(Env, Subs),
    Dest = destination(Shape, Subs),
    Ctx = #route_ctx{tenant_id = <<"default">>, publisher = undefined,
                     reliability = os_buffer},
    %% Warm whatever persistent_term / ETS caches the path needs before timing.
    _ = warmup(Impl, Dest, Ctx, Env, WarmupMs),
    Parent = self(),
    Pids =
        [spawn(fun() ->
                       receive go -> ok end,
                       {Ops, _Us} = drive(Impl, Dest, Ctx, Env, DurationMs),
                       Parent ! {done, self(), Ops}
               end)
         || _ <- lists:seq(1, P)],
    [Pid ! go || Pid <- Pids],
    Total = collect(Pids, 0),
    %% Route once more and check it matched, for the same reason the
    %% single-process path does: a row that routes nothing is not a measurement.
    {ok, [{_Part, Matched}]} = route_one(Impl, Dest, Ctx, Env),
    Matched =/= [] orelse erlang:error({no_match, concurrent, Impl, P}),
    true = lists:member(wildcard_hash, Matched),
    teardown(),
    Total.

collect([], Acc) ->
    Acc;
collect(Pids, Acc) ->
    receive
        {done, Pid, Ops} ->
            collect(lists:delete(Pid, Pids), Acc + Ops)
    after 120000 ->
            erlang:error({worker_timeout, length(Pids)})
    end.

%%%_* The three implementations -------------------------------------------

%% Exactly one variable between neighbours:
%%
%%   old  -- flat index, env fetched by gen_server:call (pre-change-1)
%%   env  -- flat index, env read without a call (change 1)
%%   trie -- trie index, env read without a call (change 1 + 2)
%%
%% The first version of this file made `env' and `trie' the same call, so the
%% report's "change 1 alone fixes almost everything" was unsupported -- both
%% rows measured the trie. The flat table below exists so `env' really is
%% change 1 only.

%% Change 1 + 2: the shipping code.
trie_route(Dest, Ctx, Env) -> ?HASH:route(Dest, Ctx, Env).

%% Change 1 only: env is already in hand, but the index is still one row per
%% filter, scanned linearly.
env_route(Dest, Ctx, #{flat := Tab} = Env) ->
    flat_route(Tab, Dest, Ctx, Env).

%% Pre-change-1: the env (and with it the table ref) arrives through a
%% gen_server:call, exactly as `current_env/0' did.
old_route(Dest, Ctx, Env) ->
    Tab = flat_env:table(),
    flat_route(Tab, Dest, Ctx, Env).

flat_route(Tab, Dest, Ctx, Env) ->
    Tenant = ctx_tenant(Ctx),
    TopicLevels = levels(Dest),
    Matches = lists:flatmap(
                fun({{EntryTenant, Filter}, Subs}) ->
                        case tenant_visible(EntryTenant, Tenant) of
                            false -> [];
                            true ->
                                case match(TopicLevels, levels(Filter)) of
                                    true -> Subs;
                                    false -> []
                                end
                        end
                end,
                ets:tab2list(Tab)),
    #{partitions := N} = Env,
    {ok, [{erlang:phash2(Dest, N), flat_ordered_unique(lists:keysort(2, Matches))}]}.

ctx_tenant(#route_ctx{tenant_id = T}) when is_binary(T), T =/= <<>> -> T;
ctx_tenant(_) -> <<"default">>.

levels(T) -> binary:split(T, <<"/">>, [global]).

tenant_visible(<<"*">>, _) -> true;
tenant_visible(T, T) -> true;
tenant_visible(_, _) -> false.

match(_Topic, [<<"#">>]) -> true;
match([], []) -> true;
match([_ | T], [<<"+">> | F]) -> match(T, F);
match([L | T], [L | F]) -> match(T, F);
match(_, _) -> false.

%% The pre-change dedupe, O(N^2) by design: it is what the old code did.
flat_ordered_unique(Pairs) -> flat_ordered_unique(Pairs, [], []).
flat_ordered_unique([], _Seen, Acc) -> lists:reverse(Acc);
flat_ordered_unique([{R, _} | Rest], Seen, Acc) ->
    case lists:member(R, Seen) of
        true -> flat_ordered_unique(Rest, Seen, Acc);
        false -> flat_ordered_unique(Rest, [R | Seen], [R | Acc])
    end.

%%%_* Setup / teardown -----------------------------------------------------

%% Both structures are built and populated from the same subscription list, so
%% `env' and `trie' see identical data and differ only in the index shape.
setup(_Impl, _Subs) ->
    teardown(),
    _ = application:load(broker_router),
    ok = application:set_env(broker_router, partitions, 4),
    ok = application:set_env(broker_router, backend, ?HASH),
    {ok, Env} = ?HASH:init(#{partitions => 4, name => ?STORE}),
    Flat = ets:new(flat_index, [set, protected, {read_concurrency, true}]),
    {ok, _} = flat_env:start(Flat),
    Env#{flat => Flat}.

%% Register into the trie through the real writer and into the flat table
%% directly. The flat table is populated in one pass during setup, not timed.
register_all(Env, Subs) ->
    Flat = maps:get(flat, Env),
    lists:foreach(
      fun({SubRef, Filter}) ->
              ok = ?HASH:register(Env, Filter, SubRef, #{tenant_id => <<"default">>}),
              Key = {<<"default">>, Filter},
              case ets:lookup(Flat, Key) of
                  [] -> ets:insert(Flat, {Key, [{SubRef, SubRef}]});
                  [{_, Existing}] ->
                      ets:insert(Flat, {Key, [{SubRef, SubRef} | Existing]})
              end
      end, Subs),
    ok.

teardown() ->
    catch gen_server:stop(?STORE, normal, 1000),
    catch gen_server:stop(flat_env, normal, 1000),
    catch ets:delete(flat_index),
    timer:sleep(20),
    ok.

table_size(#{tab := Tab}) ->
    case ets:info(Tab, size) of
        undefined -> 0;
        N -> N
    end.
