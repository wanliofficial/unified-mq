%% @doc S11x router index memory: OQ-2 in `docs/design/router-read-path.md' §8.
%%
%% The trie stores one ETS row per path prefix, so 100k filters can cost more
%% than 100k rows. The design promises three things that decide whether that
%% needs a configurable cap:
%%
%%   * shared prefixes are stored once (not once per filter);
%%   * memory grows with live filters, not with registrations ever made
%%     (prune must reclaim);
%%   * the per-filter cost is bounded and predictable at 100k.
%%
%% This measures all three. It is a benchmark, not a test: the correctness of
%% pruning is `broker_router_hash_SUITE:unregister_prunes_the_trie/1'.
%%
%% Usage (from the repo root, after `rebar3 compile'):
%%
%%   erlc -pa _build/default/lib/broker_router/ebin -o /tmp/opencode/ebin \
%%        tools/bench/router_index_mem.erl
%%   erl -pa _build/default/lib/*/ebin -pa /tmp/opencode/ebin -noshell \
%%       -eval 'router_index_mem:run(), halt().'
-module(router_index_mem).

-export([run/0, run/1, measure/2, words/1]).

-define(STORE, broker_router_mem_store).
-define(HASH, broker_router_hash).

%%%_* Public ---------------------------------------------------------------

run() ->
    run(#{}).

run(Opts) ->
    Counts = maps:get(counts, Opts, [1000, 10000, 100000]),
    Shapes = maps:get(shapes, Opts, [spread, deep, fleet, fanout]),
    DoChurn = maps:get(churn, Opts, true),
    ChurnAt = maps:get(churn_counts, Opts, maps:get(counts, Opts)),
    %% Print every row as it is measured. A 100k run takes minutes (one
    %% gen_server:call per registration), and losing all of it because the last
    %% shape is slow would mean re-measuring from the start.
    io:format("~-8s ~-10s ~-11s ~-8s ~-10s ~-12s~n",
              ["shape", "subs", "index_rows", "rows/sub", "bytes/sub", "words/sub"]),
    Rows = [begin
                R = measure(Shape, N),
                io:format("~-8s ~-10w ~-11w ~-8.3f ~-10.1f ~-12.2f~n",
                          [Shape, N, maps:get(index_rows, R),
                           maps:get(rows_per_filter, R),
                           maps:get(bytes_per_filter, R),
                           maps:get(words_per_filter, R)]),
                R
            end
            || Shape <- Shapes, N <- Counts],
    case DoChurn of
        true ->
            [churn(Shape, N) || Shape <- Shapes, N <- ChurnAt];
        false ->
            ok
    end,
    io:format("~nRESULT ~p~n", [Rows]),
    Rows.

%%%_* Shapes ---------------------------------------------------------------

%% `spread' -- N filters with distinct leaves over shared numeric prefixes.
%% The read benchmark's shape: ~100 leaves per `bench/<n>/<m>' prefix.
%%
%% `deep' -- N filters that share *no* prefix: the worst case for write
%% amplification, ~one row per path level. This is the shape OQ-2 asks about.
%%
%% `fleet' -- 1000 topics with N subscribers spread over them (N/1000 each).
%% Sharing at topic level, which is what a device fleet looks like.
%%
%% `fanout' -- N subscribers on one filter: the best case for sharing.
filters(spread, N) ->
    [{I, spread_filter(I)} || I <- lists:seq(1, N)];
filters(deep, N) ->
    [{I, deep_filter(I)} || I <- lists:seq(1, N)];
filters(fleet, N) ->
    Per = max(1, N div 1000),
    [{I, fleet_filter((I - 1) div Per)} || I <- lists:seq(1, N)];
filters(fanout, N) ->
    [{I, <<"bench/fanout/status">>} || I <- lists:seq(1, N)].

spread_filter(I) ->
    iolist_to_binary(["bench/", integer_to_list(I rem 100), "/",
                      integer_to_list(I rem 37), "/dev", integer_to_list(I)]).

%% Every level is unique to I, so no prefix is shared with any other filter.
deep_filter(I) ->
    iolist_to_binary(["d", integer_to_list(I), "/", integer_to_list(I * 3),
                      "/", integer_to_list(I * 7), "/leaf", integer_to_list(I)]).

fleet_filter(Topic) ->
    iolist_to_binary(["devices/", integer_to_list(Topic rem 100), "/",
                      integer_to_list(Topic), "/status"]).

%%%_* Measurement ----------------------------------------------------------

measure(Shape, N) ->
    teardown(),
    _ = application:load(broker_router),
    ok = application:set_env(broker_router, partitions, 4),
    ok = application:set_env(broker_router, backend, ?HASH),
    {ok, Env} = ?HASH:init(#{partitions => 4, name => ?STORE}),
    Tab = maps:get(tab, Env),
    Filters = filters(Shape, N),
    Empty = words(Tab),
    %% Registration goes through the real writer, one call per subscription.
    [ok = ?HASH:register(Env, F, Ref, #{tenant_id => <<"default">>})
     || {Ref, F} <- Filters],
    Loaded = words(Tab),
    RowCount = ets:info(Tab, size),
    %% `ets:info(Tab, memory)' is in words, not bytes; a word is 8 bytes on a
    %% 64-bit VM. Keeping both so the report cannot confuse them (an earlier
    %% version of this file reported the same number under both labels).
    Words = Loaded - Empty,
    Bytes = Words * 8,
    teardown(),
    #{shape => Shape,
      subscriptions => N,
      index_rows => RowCount,
      %% Reported three ways because "per filter" hides very different costs:
      %% the trie's rows are per *path*, and only some of them carry a sub.
      rows_per_filter => RowCount / N,
      bytes => Bytes,
      bytes_per_filter => Bytes / N,
      words_per_filter => Words / N}.

%% Load, unregister everything, and confirm no row survives. The design promises
%% memory tracks live filters rather than registrations ever made; a leak would
%% be the real OQ-2 risk.
%%
%% `words/1' does *not* return to its starting value here, and that is not a
%% leak: after a large delete ETS keeps a constant amount of free space in the
%% table (~2311 words on this build) which does not grow with N or with the
%% number of churn rounds. Measured across N = 1k/10k/50k: residue is exactly
%% 2311 words every time. So the assertion is "no rows left", plus the residue
%% must not scale with N.
churn(Shape, N) ->
    teardown(),
    _ = application:load(broker_router),
    ok = application:set_env(broker_router, partitions, 4),
    ok = application:set_env(broker_router, backend, ?HASH),
    {ok, Env} = ?HASH:init(#{partitions => 4, name => ?STORE}),
    Tab = maps:get(tab, Env),
    Empty = words(Tab),
    Filters = filters(Shape, N),
    [ok = ?HASH:register(Env, F, Ref, #{tenant_id => <<"default">>})
     || {Ref, F} <- Filters],
    Peak = words(Tab),
    PeakRows = ets:info(Tab, size),
    [ok = ?HASH:unregister(Env, Ref) || {Ref, _F} <- Filters],
    After = words(Tab),
    AfterRows = ets:info(Tab, size),
    teardown(),
    io:format("~n=== churn ~p / ~p ===~n", [Shape, N]),
    io:format("  empty  ~p words, 0 rows~n", [Empty]),
    io:format("  peak   ~p words, ~p rows~n", [Peak, PeakRows]),
    io:format("  after  ~p words, ~p rows (reclaimed ~p of ~p, residue ~p = ~.4f words/filter)~n",
              [After, AfterRows, Peak - After, Peak - Empty, After - Empty,
               (After - Empty) / N]),
    %% Every row must be gone. A residue that scales with N would mean prune left
    %% a node per churned filter, and OQ-2's answer would have to be "cap it".
    AfterRows =:= 0 orelse erlang:error({leaked_rows, Shape, N, AfterRows}),
    #{shape => Shape, subscriptions => N, peak_words => Peak,
      empty_words => Empty, residue_words => After - Empty,
      residue_per_filter => (After - Empty) / N}.

%%%_* Helpers ---------------------------------------------------------------

%% `ets:info(Tab, memory)' counts words, not bytes. Everything above is in words
%% for that reason; on a 64-bit VM a word is 8 bytes.
words(Tab) ->
    case ets:info(Tab, memory) of
        undefined -> erlang:error({table_gone, Tab});
        W -> W
    end.

teardown() ->
    catch gen_server:stop(?STORE, normal, 1000),
    timer:sleep(20),
    ok.
