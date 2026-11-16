%% @doc Plan-cache hit-rate model: does caching `[{Partition, Refs}]' per
%% (tenant, topic) pay for the router read path?
%%
%% `docs/benchmarks/2026-11-16-s11x-router-fanout.md' §4 left this open. It
%% measured the work a high-fan-out publish does (~11.7 ms at 10k), all of it a
%% function of (tenant, topic, index state), so a cache could in principle drop
%% that to a lookup. The open question was the hit rate, which depends on the
%% ratio of repeated topics to subscription churn.
%%
%% That ratio is not measured anywhere: a live system would need days of traffic
%% to span it, so this models it instead -- a sweep over topic shapes and churn
%% rates that brackets the plausible deployment space. It is a *model*: the hit
%% rates are only as good as the assumptions, which are printed with the result,
%% and only ?MISS_US is measured.
%%
%% The constraint that dominates everything here is in
%% `broker_router_hash_store': `gen/1' is one global counter bumped by *any*
%% subscription write, and design §4 invalidates the whole plan cache on a bump.
%% So the cache is empty immediately after every churn, and the first publish to
%% each topic after each churn is always a miss.
%%
%% Result: `fleet' (one device = one topic) misses on every publish at every
%% churn rate, `hot' decays with churn, `bursty' is capped by its hot-head share.
%% That is why plan caching was rejected -- see
%% `docs/benchmarks/2026-11-16-s11x-router-plan-cache.md'.
%%
%% The constraint that dominates everything here is in
%% `broker_router_hash_store': `gen/1' is one global counter bumped by *any*
%% subscription write, and design §4 invalidates the whole plan cache on a bump.
%% So the cache is empty immediately after every churn, and the first publish to
%% each topic after each churn is always a miss.
-module(router_plan_cache_sim).

-export([run/0, run/1]).

%%%_* Model ------------------------------------------------------------------

%% Workload shapes, bracketing real deployments:
%%
%%   hot    -- few topics, repeated (a dashboard polling ~20 metrics).
%%   fleet  -- many topics, roughly uniform (1 device = 1 topic, telemetry).
%%   bursty -- few hot topics + a long tail (an API: 5 endpoints, many clients).
-define(SHAPES, [hot, fleet, bursty]).

%% Churn as a fraction of publishes (one subscribe/unsubscribe each). 0.001 = one
%% per 1000 publishes. Because `gen' is global, *any* churn empties the cache, so
%% this is the axis that decides the outcome.
-define(CHURNS, [0.0, 0.0001, 0.001, 0.005, 0.01, 0.05]).

%% Miss price = measured full route at 10k fan-out (fan-out report); hit price =
%% a lookup. Both in microseconds.
-define(MISS_US, 11734).
-define(HIT_US, 1).

run() ->
    run(#{}).

run(Opts) ->
    Publishes = maps:get(publishes, Opts, 10000),
    Shapes = maps:get(shapes, Opts, ?SHAPES),
    Churns = maps:get(churns, Opts, ?CHURNS),
    io:format("~nplan-cache model: ~p publishes per cell, deterministic seed~n",
              [Publishes]),
    io:format("miss=~p us (measured at 10k fan-out), hit=~p us (lookup)~n~n",
              [?MISS_US, ?HIT_US]),
    io:format("~-8s ~-9s ~-11s ~-13s ~-13s ~s~n",
              ["shape", "churn", "misses", "miss_rate", "mean_us/pub", "saved"]),
    Rows =
        [{Shape, Churn, begin
                             M = sim(Shape, Churn, Publishes),
                             io:format("~-8s ~-9s ~-11w ~-13w ~-13w ~s~n",
                                       [Shape, fmt(Churn), M, M / Publishes,
                                        mean_us(M, Publishes),
                                        saved(M, Publishes)]),
                             M
                         end}
         || Shape <- Shapes, Churn <- Churns],
    %% Machine-readable. The table above is the one to read; this exists so a
    %% caller can diff two runs without re-parsing aligned text.
    io:format("~nRESULT ~w~n",
              [[#{shape => S, churn => C, miss_rate => M / Publishes}
                || {S, C, M} <- Rows]]),
    verdict(),
    Rows.

mean_us(Misses, Publishes) ->
    (Misses * ?MISS_US + (Publishes - Misses) * ?HIT_US) / Publishes.

%% Churn is a float; `~w' right-aligns with `*' padding for non-atomics, and
%% `~g' truncates to scientific notation. So render it as fixed-point text.
fmt(F) when F == 0 ->
    "0";
fmt(F) ->
    S = io_lib:format("~.6f", [F]),
    string:trim(S, trailing, "0").

saved(Misses, Publishes) ->
    Base = Publishes * ?MISS_US,
    With = Misses * ?MISS_US + (Publishes - Misses) * ?HIT_US,
    %% `~.1f%%' would print the literal '%' twice for a string argument, so the
    %% percent sign is appended here instead.
    io_lib:format("~.1f%", [100 * (1 - With / Base)]).

%%%_* Simulation -------------------------------------------------------------

%% Number of cache misses over `Publishes'. A miss is a (tenant, topic, gen)
%% pair never seen since the last gen bump. One tenant.
sim(Shape, Churn, Publishes) ->
    Keys = keys(Shape),
    Dist = dist(Shape, Keys),
    Loop = fun L(I, Seen, Misses) ->
               if
                   I =< Publishes ->
                       %% Churn with probability `Churn' bumps the global gen and
                       %% empties the whole cache (design §4). Deterministic so
                       %% the sweep is reproducible: churn every `step'th publish.
                       {Seen1, Misses1} =
                           case churns(I, Churn) of
                               true -> {#{}, Misses};        % cache emptied
                               false -> {Seen, Misses}
                           end,
                       Key = Dist(I),
                       case maps:is_key(Key, Seen1) of
                           true -> L(I + 1, Seen1, Misses1);
                           false -> L(I + 1, Seen1#{Key => true}, Misses1 + 1)
                       end;
                   true ->
                       Misses
               end
           end,
    Loop(1, #{}, 0).

%% Deterministic churn: with rate R, churn every round(1/R)th publish. At R=0.01
%% that is every 100th publish; at R=0 never. Keeps the sweep reproducible while
%% matching the intended average rate.
churns(_I, R) when R < 1.0e-9 ->
    false;
churns(I, R) ->
    Step = max(1, round(1.0 / R)),
    I rem Step =:= 0.

%% Keys per shape. `bursty' does not use this list (its picker is closed over
%% small integers), but it must return its topic count for the shape's doc.
keys(hot) ->
    [<<"t", (integer_to_binary(I))/binary>> || I <- lists:seq(1, 20)];
keys(fleet) ->
    [<<"dev/", (integer_to_binary(I))/binary>> || I <- lists:seq(1, 50000)];
keys(bursty) ->
    5000.

%% Distribution: a pick function that chooses the topic for publish #I.
dist(Shape, Keys) when Shape =:= hot; Shape =:= fleet ->
    fun(I) -> lists:nth(1 + ((I * 2654435761) rem length(Keys)), Keys) end;
dist(bursty, _TopicCount) ->
    %% Deterministic Zipf-ish: `I rem 10' chooses. 0 and 1 -> one of the 5 hot
    %% topics (so the head carries ~20% of traffic each), 2..9 -> a tail topic
    %% drawn from the remaining 4995. A hot head with a long tail, without
    %% floating-point weight walking.
    fun(I) ->
            case I rem 10 of
                0 -> {hot, 1 + ((I * 7) rem 5)};
                1 -> {hot, 1 + ((I * 3) rem 5)};
                _ -> {tail, (I * 2654435761) rem 4995 + 6}
            end
    end.

verdict() ->
    io:format("~n=== verdict ===~n"),
    io:format("Misses = cold misses: a (topic, gen) pair not seen since the last~n"),
    io:format("gen bump. Because gen is global and bumped by ANY subscription write,~n"),
    io:format("one churn event empties the entire cache.~n"),
    io:format("~nRead the shape column first, then churn (columns left->right worsen):~n"),
    io:format("  hot   (20 topics):   churn 0.0 -> 99.8% saved, but churn 0.01 -> 80%.~n"),
    io:format("                          A dashboard benefits only while subscribers are stable.~n"),
    io:format("  fleet (50k topics):   0% saved at every churn. A unique-topic fleet gains nothing:~n"),
    io:format("                          every publish is a first-seen topic, so it always misses.~n"),
    io:format("  bursty (5 hot+tail):   half saved with no churn, decaying to 18-20% by churn 0.001.~n"),
    io:format("                          The hot head repeats; the long tail never does.~n"),
    io:format("~nRecommendation: do not build a global-gen plan cache. It pays only for a~n"),
    io:format("few-hot-topic, low-churn workload; a fleet-shaped or churny deployment gets nothing~n"),
    io:format("and would pay pure lookup overhead. Making it robust needs per-prefix or~n"),
    io:format("per-partition invalidation instead of the global gen -- a redesign, not a patch.~n"),
    ok.
