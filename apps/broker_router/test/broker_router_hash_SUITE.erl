%% @doc T3: contract tests for broker_router_hash.
%% Wildcard cases come from MQTT 3.1.1 §4.7 as frozen in ADR-010 §10.3 and its
%% 验证 section; route_reads_are_served_without_the_writer is the mechanical
%% check of §10.1【F3】(no process serialization on the read path).
-module(broker_router_hash_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("broker_core/include/broker_message.hrl").
-include_lib("broker_router/include/broker_router.hrl").

-define(STORE, broker_router_hash_store).
-define(HASH, broker_router_hash).

all() ->
    [init_yields_env_and_rejects_bad_config,
     exact_filter_matches_only_its_topic,
     single_level_wildcard_matches_one_level,
     multi_level_wildcard_matches_parent_and_below,
     hash_matches_everything,
     plus_matches_a_single_level_topic,
     illegal_filters_are_rejected_at_register,
     partition_is_deterministic_and_in_range,
     route_lists_every_matching_subref_once,
     unregister_removes_the_subscription,
     register_twice_for_one_subref_is_rejected,
     unmatched_topic_returns_empty_sub_list,
     route_reads_are_served_without_the_writer,
     env_is_served_without_the_writer,
     gen_advances_on_successful_writes_only,
     env_rejects_a_dead_store,
     trie_matches_the_reference_matcher,
     tenant_roots_are_independent_and_legacy_is_visible,
     unregister_prunes_the_trie,
     route_sees_stable_subscriptions_while_the_index_churns,
     on_partition_change_stays_unimplemented_in_s1].

init_per_testcase(_Case, Config) ->
    %% current_env/0 takes the partition count from app env (init/1 takes it
    %% from the Config map), so the two paths must agree for the env tests.
    _ = application:load(broker_router),
    ok = application:set_env(broker_router, partitions, 4),
    {ok, Env} = ?HASH:init(#{partitions => 4}),
    [{env, Env} | Config].

end_per_testcase(_Case, _Config) ->
    catch gen_server:stop(?STORE, normal, 1000),
    ok.

%%%_* Tests --------------------------------------------------------------

init_yields_env_and_rejects_bad_config(Config) ->
    Env = env(Config),
    true = is_tuple(Env) orelse is_map(Env),
    %% 0 partitions would make the modulo crash on the first publish, and a
    %% non-integer would be a config typo caught the same way.
    {error, _} = ?HASH:init(#{partitions => 0}),
    {error, _} = ?HASH:init(#{partitions => <<"four">>}),
    %% A rejected init must not have touched the store the suite is using.
    {ok, [{_, []}]} = ?HASH:route(<<"still/alive">>, ctx(), Env),
    ok.

exact_filter_matches_only_its_topic(Config) ->
    Env = env(Config),
    ok = ?HASH:register(Env, <<"sport/tennis/player1">>, 1, #{}),
    {ok, [{_, [1]}]} = ?HASH:route(<<"sport/tennis/player1">>, ctx(), Env),
    {ok, [{_, []}]} = ?HASH:route(<<"sport/tennis/player2">>, ctx(), Env),
    %% A filter does not match a deeper topic than it describes.
    {ok, [{_, []}]} = ?HASH:route(<<"sport/tennis/player1/ranking">>, ctx(), Env),
    ok.

single_level_wildcard_matches_one_level(Config) ->
    Env = env(Config),
    ok = ?HASH:register(Env, <<"sport/tennis/+">>, 1, #{}),
    {ok, [{_, [1]}]} = ?HASH:route(<<"sport/tennis/player1">>, ctx(), Env),
    {ok, [{_, []}]} = ?HASH:route(<<"sport/tennis">>, ctx(), Env),
    {ok, [{_, []}]} = ?HASH:route(<<"sport/tennis/player1/ranking">>, ctx(), Env),
    %% + consumes exactly one level, so it cannot match a partial level name.
    ok = ?HASH:register(Env, <<"sport/+">>, 2, #{}),
    {ok, [{_, [2]}]} = ?HASH:route(<<"sport/tennis">>, ctx(), Env),
    {ok, [{_, []}]} = ?HASH:route(<<"sportx/tennis">>, ctx(), Env),
    ok.

multi_level_wildcard_matches_parent_and_below(Config) ->
    Env = env(Config),
    ok = ?HASH:register(Env, <<"sport/#">>, 1, #{}),
    {ok, [{_, [1]}]} = ?HASH:route(<<"sport">>, ctx(), Env),
    {ok, [{_, [1]}]} = ?HASH:route(<<"sport/tennis">>, ctx(), Env),
    {ok, [{_, [1]}]} = ?HASH:route(<<"sport/tennis/player1/ranking">>, ctx(), Env),
    {ok, [{_, []}]} = ?HASH:route(<<"other/tennis">>, ctx(), Env),
    ok.

hash_matches_everything(Config) ->
    Env = env(Config),
    ok = ?HASH:register(Env, <<"#">>, 1, #{}),
    {ok, [{_, [1]}]} = ?HASH:route(<<"a">>, ctx(), Env),
    {ok, [{_, [1]}]} = ?HASH:route(<<"a/b/c/d/e">>, ctx(), Env),
    ok.

plus_matches_a_single_level_topic(Config) ->
    Env = env(Config),
    ok = ?HASH:register(Env, <<"+/+">>, 1, #{}),
    {ok, [{_, [1]}]} = ?HASH:route(<<"a/b">>, ctx(), Env),
    {ok, [{_, []}]} = ?HASH:route(<<"a">>, ctx(), Env),
    {ok, [{_, []}]} = ?HASH:route(<<"a/b/c">>, ctx(), Env),
    ok = ?HASH:register(Env, <<"+">>, 2, #{}),
    {ok, [{_, [2]}]} = ?HASH:route(<<"a">>, ctx(), Env),
    ok.

%% §4.7: + and # must occupy a whole level, and # must be the last level.
illegal_filters_are_rejected_at_register(Config) ->
    Env = env(Config),
    Illegal = [<<"sport#">>,
               <<"sport/#/ranking">>,
               <<"#/sport">>,
               <<"sport+">>,
               <<"ten+nis/player1">>,
               <<>>],
    lists:foreach(
        fun(F) ->
            {error, {invalid_filter, F}} = ?HASH:register(Env, F, 1, #{})
        end,
        Illegal),
    {ok, [{_, []}]} = ?HASH:route(<<"sport#">>, ctx(), Env),
    ok.

partition_is_deterministic_and_in_range(Config) ->
    Env = env(Config),
    Dest = <<"sport/tennis/player1">>,
    {ok, [{P, []}]} = ?HASH:route(Dest, ctx(), Env),
    {ok, [{P, []}]} = ?HASH:route(Dest, ctx(), Env),
    %% 0-based partition ids, matching Kafka/Pulsar convention; ADR-010 §10.3
    %% only freezes "pure integer", so the base is fixed here once.
    true = P >= 0 andalso P < 4,
    lists:foreach(
        fun(I) ->
            {ok, [{Pi, []}]} = ?HASH:route(topic(I), ctx(), Env),
            true = Pi >= 0 andalso Pi < 4
        end,
        lists:seq(1, 200)),
    %% Routing is a function of the destination only: the tenant in Ctx must
    %% not change the partition, or per-sub cursors would disagree across
    %% publishers on the same topic.
    {ok, [{P2, []}]} = ?HASH:route(Dest, ctx(<<"other-tenant">>), Env),
    P2 = P,
    ok.

%% ADR-010 验证 (T13), completeness half: one message hitting several
%% subscriptions returns all of them, each exactly once, in registration order.
%% Merging overlapping subscriptions that belong to the same client is NOT
%% possible here -- it would require dereferencing SubRef, which §10.2 forbids.
route_lists_every_matching_subref_once(Config) ->
    Env = env(Config),
    ok = ?HASH:register(Env, <<"sport/tennis/player1">>, 7, #{}),
    ok = ?HASH:register(Env, <<"sport/tennis/+">>, 8, #{}),
    ok = ?HASH:register(Env, <<"sport/#">>, 9, #{}),
    {ok, [{_P, Subs}]} = ?HASH:route(<<"sport/tennis/player1">>, ctx(), Env),
    %% Three overlapping filters match this topic; all three subs are reported.
    [7, 8, 9] = Subs,
    Subs = lists:usort(Subs),
    ok.

%% Change 2 (design §7.2): the trie must answer exactly what the linear scan
%% answered. The oracle is the matcher it replaced, evaluated here over the same
%% registrations, so a trie bug shows up as a list mismatch instead of a lucky
%% pass on hand-picked topics.
trie_matches_the_reference_matcher(Config) ->
    Env = env(Config),
    _ = rand:seed(exsss, {11, 22, 33}),
    Registered =
        [begin
             Tenant = random_tenant(),
             Filter = random_filter(),
             ok = ?HASH:register(Env, Filter, I, Tenant),
             {I, Filter, filter_tenant(Tenant)}
         end
         || I <- lists:seq(1, 400)],
    lists:foreach(
        fun(_) ->
            Topic = random_topic(),
            PubTenant = random_publisher_tenant(),
            Expected = [I || {I, Filter, Tenant} <- Registered,
                             tenant_visible(Tenant, PubTenant),
                             ref_match(levels(Topic), levels(Filter))],
            {ok, [{_, Subs}]} = ?HASH:route(Topic, ctx(PubTenant), Env),
            case Subs of
                Expected ->
                    ok;
                _ ->
                    ct:fail({trie_and_reference_disagree, Topic, PubTenant,
                             Expected, Subs})
            end
        end,
        lists:seq(1, 400)),
    ok.

%% S6 tenancy at the read path, now expressed by which root the walk starts
%% from: the same filter under two tenants is two independent leaves, and `*'
%% stays visible to every tenant (legacy sessions predating tenant metadata).
tenant_roots_are_independent_and_legacy_is_visible(Config) ->
    Env = env(Config),
    ok = ?HASH:register(Env, <<"a/b">>, 1, #{tenant_id => <<"t1">>}),
    ok = ?HASH:register(Env, <<"a/b">>, 2, #{tenant_id => <<"t2">>}),
    ok = ?HASH:register(Env, <<"a/#">>, 3, #{}),
    {ok, [{_, [1, 3]}]} = ?HASH:route(<<"a/b">>, ctx(<<"t1">>), Env),
    {ok, [{_, [2, 3]}]} = ?HASH:route(<<"a/b">>, ctx(<<"t2">>), Env),
    {ok, [{_, [3]}]} = ?HASH:route(<<"a/b">>, ctx(<<"other">>), Env),
    ok.

%% Change 2 memory bound (design §3.6): deleting a subscription must remove the
%% nodes it created, or every filter ever registered would leave a skeleton
%% behind. The shared prefix `a/b' must survive while a sibling still uses it.
unregister_prunes_the_trie(Config) ->
    Env = env(Config),
    #{tab := Trie} = Env,
    0 = ets:info(Trie, size),
    %% Rows for a/b/c: [] , [a] , [a,b] (child links) and [a,b,c] (the leaf).
    ok = ?HASH:register(Env, <<"a/b/c">>, 1, #{}),
    4 = ets:info(Trie, size),
    %% a/b/d shares three of them and adds one leaf.
    ok = ?HASH:register(Env, <<"a/b/d">>, 2, #{}),
    5 = ets:info(Trie, size),
    ok = ?HASH:unregister(Env, 2),
    4 = ets:info(Trie, size),
    {ok, [{_, [1]}]} = ?HASH:route(<<"a/b/c">>, ctx(), Env),
    ok = ?HASH:unregister(Env, 1),
    0 = ets:info(Trie, size),
    {ok, [{_, []}]} = ?HASH:route(<<"a/b/c">>, ctx(), Env),
    %% A node that is also an ancestor must survive losing its own subscriptions:
    %% `a/b' is the only row linking `[a]' to `[a,b,c]'.
    ok = ?HASH:register(Env, <<"a/b">>, 3, #{}),
    ok = ?HASH:register(Env, <<"a/b/c">>, 4, #{}),
    4 = ets:info(Trie, size),
    ok = ?HASH:unregister(Env, 3),
    4 = ets:info(Trie, size),
    {ok, [{_, [4]}]} = ?HASH:route(<<"a/b/c">>, ctx(), Env),
    ok = ?HASH:unregister(Env, 4),
    0 = ets:info(Trie, size),
    ok.

unregister_removes_the_subscription(Config) ->
    Env = env(Config),
    ok = ?HASH:register(Env, <<"sport/#">>, 1, #{}),
    ok = ?HASH:register(Env, <<"sport/#">>, 2, #{}),
    {ok, [{_, [1, 2]}]} = ?HASH:route(<<"sport/a">>, ctx(), Env),
    ok = ?HASH:unregister(Env, 1),
    {ok, [{_, [2]}]} = ?HASH:route(<<"sport/a">>, ctx(), Env),
    ok = ?HASH:unregister(Env, 2),
    {ok, [{_, []}]} = ?HASH:route(<<"sport/a">>, ctx(), Env),
    %% Unknown SubRef is an error, not a silent ok: a no-op would hide a
    %% subscription/router lifetime mismatch.
    {error, {not_found, 1}} = ?HASH:unregister(Env, 1),
    ok.

register_twice_for_one_subref_is_rejected(Config) ->
    Env = env(Config),
    ok = ?HASH:register(Env, <<"a/b">>, 1, #{}),
    {error, {already_registered, 1}} = ?HASH:register(Env, <<"a/+">>, 1, #{}),
    {ok, [{_, [1]}]} = ?HASH:route(<<"a/b">>, ctx(), Env),
    ok.

unmatched_topic_returns_empty_sub_list(Config) ->
    Env = env(Config),
    %% The partition is still reported: the publisher appends the message even
    %% with no subscriber (§5.1 (c) -> (d)).
    {ok, [{_P, []}]} = ?HASH:route(<<"nobody/listens/here">>, ctx(), Env),
    ok.

%% §10.1【F3】: route/3 reads concurrently in the caller; only index writes go
%% through one process. Blocking the writer must leave reads working and writes
%% blocked, otherwise Router is the global bottleneck README §7.2 forbids.
route_reads_are_served_without_the_writer(Config) ->
    Env = env(Config),
    ok = ?HASH:register(Env, <<"sport/#">>, 1, #{}),
    Parent = self(),
    sys:suspend(?STORE),
    Helper =
        spawn(fun() ->
                  ok = ?HASH:register(Env, <<"blocked/topic">>, 99, #{}),
                  Parent ! {helper, self(), returned}
              end),
    try
        {ok, [{_P, [1]}]} = ?HASH:route(<<"sport/tennis">>, ctx(), Env),
        false = helper_returned_in(300, Helper),
        true = is_process_alive(Helper),
        ok
    after
        sys:resume(?STORE),
        true = helper_returned_in(2000, Helper)
    end.

%% The gap the read-path reports could not close (docs/benchmarks/
%% 2026-11-16-s11x-router-{read-path,index-memory,concurrency}.md, "没测什么"):
%% every read measurement ran against a frozen index. Production registers and
%% unregisters while publishes are in flight, and change 2 made a route READ a
%% *multi-row* traversal instead of one `ets:tab2list/1' -- one publish now takes
%% several ETS reads that are individually atomic but collectively are not. The
%% writer's unregister deletes rows as it prunes them (`prune/3').
%%
%% The claim under test is an invariant, not a probability: a subscription that
%% is never unregistered must be reported by every route/3 that runs after its
%% register/4 returned. A torn read that hid a live subscriber would break
%% ADR-007 ("SUBACK 之后必须可投递"): the publish is appended, no subscriber is
%% delivered, and nothing errors.
%%
%% The walk is Ex ++ Plus ++ Hash at every level, and each node is its own ETS
%% read, so the loop below keeps all three branches live at once and churns rows
%% the reader is walking:
%%
%%   a/b/c   literal   -- its own path rows are deleted and re-added by churn
%%   a/+/c   plus      -- forces the reader down the `+' branch at [a]
%%   a/#     hash      -- forces the Hash branch at [a], ends the branch early
%%   a/b     ancestor  -- a filter that is also a node on a/b/c's path, so
%%                        prune/3 must NOT delete its row (the defect recorded
%%                        in docs/design/router-read-path.md on 28453c9)
%%
%% The churner also runs against the legacy `<<"*">>' tenant, because
%% `visible_roots/1' makes every route read that root: a torn read there would
%% be visible to all tenants at once.
route_sees_stable_subscriptions_while_the_index_churns(Config) ->
    Env = env(Config),
    Tenant = <<"spec">>,
    %% {Filter, SubRef, Topic it must match}
    Expected = [{<<"a/b/c">>, 1, <<"a/b/c">>},
                {<<"a/+/c">>, 2, <<"a/b/c">>},
                {<<"a/#">>, 3, <<"a/b/c">>},
                {<<"a/b">>, 4, <<"a/b">>}],
    [ok = ?HASH:register(Env, F, Ref, #{tenant_id => Tenant})
     || {F, Ref, _Topic} <- Expected],
    %% Sanity: a quiet index returns exactly the registered refs, so a later
    %% subset check operates against a known-good baseline.
    {ok, [{_, Quiet}]} = ?HASH:route(<<"a/b/c">>, ctx(Tenant), Env),
    [1, 2, 3] = lists:sort(Quiet),
    {ok, [{_, QuietAncestor}]} = ?HASH:route(<<"a/b">>, ctx(Tenant), Env),
    [3, 4] = lists:sort(QuietAncestor),

    Parent = self(),
    Stop = make_ref(),
    %% Two churners: one on the reader's own tenant (rows the reader walks) and
    %% one on the shared legacy root (a root every reader reads).
    Churners = [spawn(fun() -> churn_loop(Env, Stop, T) end)
                || T <- [Tenant, <<"*">>]],
    Readers = 8,
    ReaderPids = [begin
                      {Pid, MRef} = spawn_monitor(
                                      fun() ->
                                              reader_loop(Env, Stop, Tenant, Expected,
                                                          Parent, 0, [])
                                      end),
                      {Pid, MRef}
                  end
                  || _ <- lists:seq(1, Readers)],
    %% Wall-clock budget, not a fixed iteration count: the point is overlap
    %% between readers and the pruner, and iteration counts cannot guarantee it
    %% under a loaded machine. 1.5 s comfortably exceeds one churn cycle.
    timer:sleep(1500),
    [P ! {stop, Stop} || P <- Churners ++ [Pid || {Pid, _} <- ReaderPids]],
    %% Collect one report per reader. `self()' is a call, not a pattern, so the
    %% parent has to arrive bound. Collection is by arrival, not by list order:
    %% a reader that was descheduled may report after another one, and a fixed
    %% order would look like a missing report.
    Reports = collect_reports(Parent, [Pid || {Pid, _} <- ReaderPids], []),
    [receive
         {'DOWN', MRef, process, Pid, normal} -> ok;
         {'DOWN', MRef, process, Pid, Reason} ->
             ct:fail({reader_died, Pid, Reason})
     after 5000 ->
         ct:fail({reader_not_reaped, Pid})
     end
     || {Pid, MRef} <- ReaderPids],
    %% A reader that was starved would report zero routes; an empty report must
    %% not be able to look like a pass.
    true = lists:all(fun({_, N, _}) -> N > 0 end, Reports),
    AllMisses = lists:append([M || {_, _, M} <- Reports]),
    ct:comment("routes=~p misses=~p", [lists:sum([N || {_, N, _} <- Reports]),
                                       length(AllMisses)]),
    case AllMisses of
        [] ->
            ok;
        _ ->
            ct:fail({live_suppressed_by_concurrent_prune, lists:sublist(AllMisses, 5)})
    end.

%% Read until told to stop. `Count' is the number of routes performed; `Misses'
%% accumulates the first few failures, because one failure is enough to fail and
%% a per-iteration list would grow without bound.
reader_loop(Env, Stop, Tenant, Expected, Parent, Count, Misses) ->
    case stopped(Stop) of
        true ->
            Parent ! {reader, self(), Count, Misses};
        false ->
            RouteOne =
                fun({_F, Ref, Topic}) ->
                    {ok, [{_P, Refs}]} = ?HASH:route(Topic, ctx(Tenant), Env),
                    case lists:member(Ref, Refs) of
                        true -> ok;
                        false -> {Topic, Ref, Refs}
                    end
                end,
            Found = [M || E <- Expected, (M = RouteOne(E)) =/= ok],
            Misses1 = case Found of
                          [] -> Misses;
                          _ -> lists:sublist(Misses ++ Found, 5)
                      end,
            reader_loop(Env, Stop, Tenant, Expected, Parent, Count + length(Expected),
                        Misses1)
    end.

%% Register/unregister the four shapes in a cycle, so every row a reader walks is
%% repeatedly created and pruned. Refs are unique per cycle so a stale row would
%% be recognisable in the report; the reader only asserts on the stable refs, so
%% seeing a churn ref is legitimate (it may be registered at that instant).
churn_loop(Env, Stop, Tenant) ->
    case stopped(Stop) of
        true ->
            ok;
        false ->
            Filters = [<<"a/b/c">>, <<"a/b/c/d">>, <<"a/+/c">>, <<"a/#">>, <<"a/b">>],
            [begin
                 Ref = {churn, Tenant, F, make_ref()},
                 _ = ?HASH:register(Env, F, Ref, #{tenant_id => Tenant}),
                 _ = ?HASH:unregister(Env, Ref)
             end
             || F <- Filters],
            churn_loop(Env, Stop, Tenant)
    end.

%% Wait for one `{reader, ...}' per spawned reader, in whatever order they
%% arrive. A reader that never reports is named so the failure is actionable.
collect_reports(_Parent, [], Acc) ->
    Acc;
collect_reports(Parent, Pending, Acc) ->
    receive
        {reader, Pid, Count, Misses} ->
            case lists:member(Pid, Pending) of
                true -> collect_reports(Parent, Pending -- [Pid], [{Pid, Count, Misses} | Acc]);
                false -> collect_reports(Parent, Pending, Acc)
            end
    after 10000 ->
        ct:fail({readers_did_not_report, Pending})
    end.

stopped(Stop) ->
    receive
        {stop, Stop} -> true
    after 0 -> false
    end.

%% §10.5: S1 leaves the partition-change hook unimplemented; the behavior marks
%% it optional so S4 can add it without touching a frozen signature.
on_partition_change_stays_unimplemented_in_s1(_Config) ->
    Cbs = broker_router:behaviour_info(callbacks),
    Opts = broker_router:behaviour_info(optional_callbacks),
    %% behaviour_info(callbacks) lists every callback, optional ones included;
    %% optional_callbacks marks the subset an implementation may omit.
    [{init, 1}, {on_partition_change, 2}, {register, 4}, {route, 3}, {unregister, 2}] =
        lists:sort(Cbs),
    [{on_partition_change, 2}] = Opts,
    false = lists:member({on_partition_change, 2}, ?HASH:module_info(exports)),
    ok.

%% The other half of §10.1【F3】, and the one the case above cannot see: that
%% case holds an env it already had and proves route/3 does not call the writer.
%% Getting the env was itself a call, so a publisher still queued on the index
%% writer once per message (design §1.1). With the writer suspended, acquiring
%% an env must still succeed.
env_is_served_without_the_writer(_Config) ->
    {ok, Env} = ?HASH:current_env(),
    ok = ?HASH:register(Env, <<"sport/#">>, 1, #{}),
    Parent = self(),
    sys:suspend(?STORE),
    Helper =
        spawn(fun() ->
                  ok = ?HASH:register(Env, <<"blocked/topic">>, 99, #{}),
                  Parent ! {helper, self(), returned}
              end),
    try
        %% Bounded wait, so a regression fails the case instead of hanging the
        %% suite until the CT timeout.
        Lookup =
            spawn(fun() -> Parent ! {env_lookup, self(), ?HASH:current_env()} end),
        receive
            {env_lookup, Lookup, Result} ->
                {ok, EnvNow} = Result,
                {ok, [{_, [1]}]} = ?HASH:route(<<"sport/tennis">>, ctx(), EnvNow)
        after 1000 ->
            ct:fail(env_lookup_blocked_on_the_writer)
        end,
        false = helper_returned_in(300, Helper),
        true = is_process_alive(Helper),
        ok
    after
        sys:resume(?STORE),
        true = helper_returned_in(2000, Helper)
    end.

%% The invalidation contract a route-plan cache would rely on (design §2.4):
%% a successful write moves gen, a rejected one must not. A bump on failure
%% would be harmless but would mean the counter tracks attempts, not index
%% versions, and every caller of gen/0 would have to know which.
gen_advances_on_successful_writes_only(Config) ->
    Env = env(Config),
    G0 = ?HASH:gen(),
    true = is_integer(G0),
    ok = ?HASH:register(Env, <<"a/b">>, 1, #{}),
    G1 = ?HASH:gen(),
    true = G1 > G0,
    %% Duplicate register is rejected and the index is unchanged, so gen stays.
    {error, {already_registered, 1}} = ?HASH:register(Env, <<"a/b">>, 1, #{}),
    G2 = ?HASH:gen(),
    G2 = G1,
    %% Unregistering something that is not there is also a no-op.
    {error, {not_found, 2}} = ?HASH:unregister(Env, 2),
    G3 = ?HASH:gen(),
    G3 = G2,
    ok = ?HASH:unregister(Env, 1),
    G4 = ?HASH:gen(),
    true = G4 > G3,
    ok.

%% A stale env must be reported, not handed out: the table it names is gone, so
%% a publisher that got it would crash on the first read. Two shutdown paths,
%% because only one of them reaches terminate/2.
env_rejects_a_dead_store(_Config) ->
    {ok, Env} = ?HASH:current_env(),
    ok = ?HASH:register(Env, <<"x/y">>, 1, #{}),
    %% Graceful stop: terminate/2 erases the published env.
    ok = gen_server:stop(?STORE, normal, 1000),
    {error, router_store_not_started} = ?HASH:current_env(),
    %% Supervisor-style kill: a non-trapping gen_server never runs terminate/2,
    %% so the published env survives the process. The aliveness check is what
    %% makes that harmless.
    {ok, _} = ?HASH:init(#{partitions => 4}),
    {ok, _} = ?HASH:current_env(),
    Pid = whereis(?STORE),
    OldTrap = process_flag(trap_exit, true),
    exit(Pid, kill),
    receive
        {'EXIT', Pid, killed} -> ok
    after 1000 ->
        ct:fail(store_did_not_die)
    end,
    process_flag(trap_exit, OldTrap),
    {error, router_store_not_started} = ?HASH:current_env(),
    ok.

%%%_* Helpers ------------------------------------------------------------

env(Config) ->
    proplists:get_value(env, Config).

ctx() ->
    #route_ctx{tenant_id = <<"t">>, publisher = undefined, reliability = os_buffer}.

ctx(Tenant) ->
    #route_ctx{tenant_id = Tenant, publisher = undefined, reliability = os_buffer}.

topic(I) ->
    iolist_to_binary(["sport/tennis/player", integer_to_list(I)]).

helper_returned_in(Ms, Helper) ->
    receive
        {helper, Helper, returned} -> true
    after Ms -> false
    end.

%%%_* Oracles for the trie equivalence case ---------------------------------

%% The matcher change 2 replaced (broker_router_hash before the trie). Kept only
%% here, as the reference the trie is compared against -- if the two ever
%% disagree, the index is wrong, not the oracle.
ref_match(_Topic, [<<"#">>]) -> true;
ref_match([], []) -> true;
ref_match([_ | Topic], [<<"+">> | Filter]) -> ref_match(Topic, Filter);
ref_match([Level | Topic], [Level | Filter]) -> ref_match(Topic, Filter);
ref_match(_Topic, _Filter) -> false.

levels(Topic) ->
    binary:split(Topic, <<"/">>, [global]).

random_tenant() ->
    case rand:uniform(3) of
        1 -> #{tenant_id => <<"t1">>};
        2 -> #{tenant_id => <<"t2">>};
        3 -> #{}
    end.

filter_tenant(#{tenant_id := T}) -> T;
filter_tenant(_) -> <<"*">>.

random_publisher_tenant() ->
    lists:nth(rand:uniform(3), [<<"t1">>, <<"t2">>, <<"other">>]).

tenant_visible(<<"*">>, _Tenant) -> true;
tenant_visible(Tenant, Tenant) -> true;
tenant_visible(_, _) -> false.

random_filter() ->
    Levels = [random_level() || _ <- lists:seq(1, rand:uniform(4))],
    WithHash = case rand:uniform(5) of
                   1 -> Levels ++ [<<"#">>];
                   _ -> Levels
               end,
    iolist_to_binary(lists:join(<<"/">>, WithHash)).

random_level() ->
    case rand:uniform(4) of
        1 -> <<"+">>;
        _ -> lists:nth(rand:uniform(3), [<<"a">>, <<"b">>, <<"c">>])
    end.

random_topic() ->
    Levels = [lists:nth(rand:uniform(3), [<<"a">>, <<"b">>, <<"c">>])
              || _ <- lists:seq(1, rand:uniform(5))],
    iolist_to_binary(lists:join(<<"/">>, Levels)).
