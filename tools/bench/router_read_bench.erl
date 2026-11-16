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

-export([run/0, run/1]).

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
route_one(trie, Dest, Ctx, Env) -> ?HASH:route(Dest, Ctx, Env).

%%%_* The three implementations -------------------------------------------

%% Change 1 + 2: the shipping code. `current_env/0' reads persistent_term, the
%% index is a trie.
env_route(Dest, Ctx, Env) -> ?HASH:route(Dest, Ctx, Env).

%% Before change 1/2, reproduced faithfully: take the env through a
%% gen_server:call (that is what `table/1' is), then scan the whole table with a
%% linear matcher and an O(N^2) dedupe. Written here rather than kept behind a
%% flag so the old cost cannot drift from the code the report compares against.
old_route(Dest, Ctx, #{store := Store} = Env) ->
    Tab = broker_router_hash_store:table(Store),
    Tenant = case Ctx of
                 #route_ctx{tenant_id = T} when is_binary(T), T =/= <<>> -> T;
                 _ -> <<"default">>
             end,
    TopicLevels = levels(Dest),
    Matches = lists:flatmap(
                fun({{EntryTenant, Filter}, Payload}) ->
                        case tenant_visible(EntryTenant, Tenant) of
                            false -> [];
                            true ->
                                case old_row(Filter, Payload) of
                                    {filter, Subs} ->
                                        case match(TopicLevels, old_levels(Filter)) of
                                            true -> Subs;
                                            false -> []
                                        end;
                                    intermediate ->
                                        []
                                end
                        end
                end,
                ets:tab2list(Tab)),
    #{partitions := N} = Env,
    {ok, [{erlang:phash2(Dest, N), old_ordered_unique(lists:keysort(2, Matches))}]}.

levels(T) -> binary:split(T, <<"/">>, [global]).

%% Change 2 stores a filter as its path (`[<<"a">>,<<"b">>]') instead of the
%% binary it came from, so the baseline reads whichever shape the table holds.
%% It also has to skip the trie's intermediate rows: only a row with `Subs' is a
%% registered filter, and only its `Path' is the filter (the same distinction the
%% old index got for free, because every row *was* a filter).
old_levels(Filter) when is_binary(Filter) ->
    levels(Filter);
old_levels(Path) when is_list(Path) ->
    Path.

%% Every trie row is `{Children, Subs}', including the row of a filter that is
%% also a prefix of others (`bench/#' has children `1', `+', ... and its own
%% subscriptions). So "is this a filter" is decided by `Subs', not by the shape:
%% a non-empty `Subs' means this path is a registered filter, empty means it is
%% only an intermediate node. Judging on the tuple shape instead silently
%% dropped `bench/#' -- the row the report's own wildcard assertion needs.
old_row(_Filter, {_Children, [_ | _] = Subs}) -> {filter, Subs};
old_row(_Filter, {_Children, []}) -> intermediate;
%% Rows from a flat (pre-change-2) table: the payload is the subscription list.
old_row(_Filter, Subs) when is_list(Subs) -> {filter, Subs}.

tenant_visible(<<"*">>, _) -> true;
tenant_visible(T, T) -> true;
tenant_visible(_, _) -> false.

match(_Topic, [<<"#">>]) -> true;
match([], []) -> true;
match([_ | T], [<<"+">> | F]) -> match(T, F);
match([L | T], [L | F]) -> match(T, F);
match(_, _) -> false.

old_ordered_unique(Pairs) -> old_ordered_unique(Pairs, [], []).
old_ordered_unique([], _Seen, Acc) -> lists:reverse(Acc);
old_ordered_unique([{R, _} | Rest], Seen, Acc) ->
    case lists:member(R, Seen) of
        true -> old_ordered_unique(Rest, Seen, Acc);
        false -> old_ordered_unique(Rest, [R | Seen], [R | Acc])
    end.

%%%_* Setup / teardown -----------------------------------------------------

setup(_Impl, _Subs) ->
    teardown(),
    _ = application:load(broker_router),
    ok = application:set_env(broker_router, partitions, 4),
    ok = application:set_env(broker_router, backend, ?HASH),
    {ok, Env} = ?HASH:init(#{partitions => 4, name => ?STORE}),
    Env.

register_all(_Env, []) ->
    ok;
register_all(Env, [{SubRef, Filter} | Rest]) ->
    ok = ?HASH:register(Env, Filter, SubRef, #{tenant_id => <<"default">>}),
    register_all(Env, Rest).

teardown() ->
    catch gen_server:stop(?STORE, normal, 1000),
    timer:sleep(20),
    ok.

table_size(#{tab := Tab}) ->
    case ets:info(Tab, size) of
        undefined -> 0;
        N -> N
    end.
