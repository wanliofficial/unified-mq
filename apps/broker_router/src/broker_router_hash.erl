%% @doc S1 default router (ADR-010 §10.3): destination hash -> partition, plus a
%% MQTT 3.1.1 §4.7 wildcard subscription index.
%%
%% `route/3' is a plain function call that reads the index table inside the
%% calling process; only `register/4' and `unregister/2' hand work to the single
%% writer. That split is the frozen point 【F3】 of §10.1 -- routing must not
%% serialize through one gen_server (README §7.2).
%%
%% The index itself is a trie (docs/design/router-read-path.md §3): matching
%% walks the destination instead of scanning every registered filter.
-module(broker_router_hash).

-behaviour(broker_router).

-include_lib("broker_core/include/broker_message.hrl").
-include("broker_router.hrl").

-export([init/1, route/3, register/4, unregister/2, current_env/0, gen/0]).

-type env() :: #{store := pid(),
                 tab := ets:tid(),
                 partitions := pos_integer(),
                 hash := phash2,
                 gen => atomics:atomics_ref()}.

-export_type([env/0]).

%%%_* broker_router behaviour -------------------------------------------

%% @doc Starts the index writer and returns the opaque env. Config keys:
%% `partitions' (required, positive integer), `name' (writer registration name,
%% default `broker_router_hash_store'), `hash' (only `phash2' in S1 -- the key
%% exists so S4 partition migration can choose another function without an API
%% change, ADR-010 §10.3).
-spec init(map()) -> {ok, env()} | {error, term()}.
init(Config) ->
    case parse_config(Config) of
        {ok, N, Hash} ->
            Name = maps:get(name, Config, broker_router_hash_store),
            case broker_router_hash_store:start_link(Name) of
                {ok, Pid} ->
                    %% Same shape as current_env/0, gen included: a caller that
                    %% got its env from init/1 (tests) and one that got it from
                    %% current_env/0 (publishers) must not see two different
                    %% maps -- the cache rules in design §2.4 depend on it.
                    {ok, Published} = broker_router_hash_store:env(Name),
                    {ok, Published#{store => Pid, partitions => N, hash => Hash}};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Read path. Runs in the publisher process: an ETS read, no process call.
%% Every SubRef whose filter matches is returned once, in registration order.
%% Merging overlapping subscriptions of the same client (MQTT §4.8 max QoS) is
%% NOT done here: it would need to dereference SubRef, which ADR-010 §10.2
%% forbids -- that belongs to broker_subscription (design §13 H7).
-spec route(binary(), route_ctx(), env()) ->
    {ok, [{partition_id(), [broker_router:sub_ref()]}]}.
%% S6: the tenant is part of the index key. A publisher sees only its own
%% tenant's subscriptions plus legacy entries registered as `<<"*">>' (the
%% pre-S6 shape, kept so existing sessions keep working). This is the read-path
%% half of tenant isolation; the write path is `broker_security:authorize/2'.
%%
%% S11x change 2 (docs/design/router-read-path.md §3): the index is a trie, so
%% this walks the destination's levels (two ETS lookups per visited node) instead
%% of copying every registered filter with `ets:tab2list/1'. The old shape made
%% a publish cost O(total subscriptions) regardless of how many matched.
route(Destination, Ctx, #{tab := Tab, partitions := N, hash := Hash}) ->
    Topic = levels(Destination),
    Matches = lists:append([walk(Tab, Root, [], Topic)
                            || Root <- visible_roots(ctx_tenant(Ctx))]),
    {ok, [{partition(Destination, N, Hash), ordered_unique(lists:keysort(2, Matches))}]}.

%% `<<"*">>' subscriptions are visible to every tenant, including the legacy
%% tenant that reads them directly -- querying it twice would double every hit.
visible_roots(<<"*">>) ->
    [<<"*">>];
visible_roots(Tenant) ->
    [Tenant, <<"*">>].

%% Topic exhausted: filters ending at this node are exact matches, and a `#'
%% child also matches the parent itself (MQTT §4.7: "sport/#" matches "sport").
walk(Tab, Tenant, Path, []) ->
    {Children, Subs} = node(Tab, Tenant, Path),
    case maps:is_key(<<"#">>, Children) of
        true -> Subs ++ subs_at(Tab, Tenant, Path ++ [<<"#">>]);
        false -> Subs
    end;
%% Otherwise three branches at most: the literal level, `+', and `#' (which
%% ends that branch because it already covers every depth below).
walk(Tab, Tenant, Path, [Level | Rest]) ->
    {Children, _Subs} = node(Tab, Tenant, Path),
    Exact =
        case maps:is_key(Level, Children) of
            true -> walk(Tab, Tenant, Path ++ [Level], Rest);
            false -> []
        end,
    Plus =
        case maps:is_key(<<"+">>, Children) of
            true -> walk(Tab, Tenant, Path ++ [<<"+">>], Rest);
            false -> []
        end,
    Hash =
        case maps:is_key(<<"#">>, Children) of
            true -> subs_at(Tab, Tenant, Path ++ [<<"#">>]);
            false -> []
        end,
    Exact ++ Plus ++ Hash.

subs_at(Tab, Tenant, Path) ->
    {_Children, Subs} = node(Tab, Tenant, Path),
    Subs.

%% Row shape is owned by `broker_router_hash_store': `{{Tenant, Path},
%% Children, Subs}'. A missing row is an empty node, not an error: the writer
%% prunes rows as soon as they are empty.
node(Tab, Tenant, Path) ->
    case ets:lookup(Tab, {Tenant, Path}) of
        [{{Tenant, Path}, {Children, Subs}}] -> {Children, Subs};
        [] -> {#{}, []}
    end.

ctx_tenant(#route_ctx{tenant_id = T}) when is_binary(T), T =/= <<>> -> T;
ctx_tenant(_) -> <<"default">>.

-spec register(env(), binary(), broker_router:sub_ref(), map()) -> ok | {error, term()}.
register(#{store := Store}, Filter, SubRef, SubOpts) ->
    case valid_filter(Filter) of
        ok -> broker_router_hash_store:register(Store, tenant_of(SubOpts), Filter, SubRef);
        invalid -> {error, {invalid_filter, Filter}}
    end.

%% A subscription without explicit tenant metadata is the legacy wildcard. The
%% MQTT adapter supplies the authenticated tenant; test/backfill paths that
%% never name one keep the pre-S6 visibility.
tenant_of(#{tenant_id := T}) when is_binary(T), T =/= <<>> -> T;
tenant_of(_) -> <<"*">>.

-spec unregister(env(), broker_router:sub_ref()) -> ok | {error, term()}.
unregister(#{store := Store}, SubRef) ->
    broker_router_hash_store:unregister(Store, SubRef).

%% @doc Assemble the env from the *running* store without starting anything.
%% `init/1' starts a store itself, which is right for a test process but wrong
%% for a consumer: boot step 3 (and every publisher) needs the env of the store
%% `broker_router_sup' owns, and calling init/1 there would either collide with
%% the supervisor's registered child or leak an untracked process. Not a
%% behavior callback -- ADR-010 is frozen; this is the assembly-time accessor
%% the frozen signatures never had a story for (design §13 I12-class gap).
%%
%% S11x (docs/design/router-read-path.md §2): this used to end in
%% `broker_router_hash_store:table/1', a `gen_server:call' -- so every publish
%% in the node queued on the index writer to re-learn a table id that never
%% changes. ADR-010 §10.1 only bites the *inside* of `route/3', but the metric
%% that matters is "publish to `[{Partition, Refs}]'", and that path was
%% serialized. The store now publishes the env once; this reads it, plus the
%% partition count from app env (an ETS read, not a call).
%%
%% Error precedence is unchanged: a missing store outranks a bad partition
%% count, because the former is an assembly bug and the latter a typo.
-spec current_env() -> {ok, env()} | {error, term()}.
current_env() ->
    case broker_router_hash_store:env(store_name()) of
        {ok, Env} ->
            Partitions = application:get_env(broker_router, partitions, undefined),
            case partitions_ok(Partitions) of
                %% Same guard as init/1: an out-of-range count would make modulo
                %% arithmetic on the first publish crash, and here it is caught at
                %% boot instead. The hash stays phash2 -- S1 offers no other, and
                %% the app env has no `hash' key (init/1 took it from a Config map
                %% nobody builds at boot; see design §6 'two partition keys').
                true ->
                    {ok, Env#{partitions => Partitions, hash => phash2}};
                false ->
                    {error, {invalid_partitions, Partitions}}
            end;
        {error, _} = Error ->
            Error
    end.

%% @doc Monotonic write counter of the index behind the configured store.
%%
%% Exposed for the route-plan cache described in design §4, which is not
%% implemented: a cache is only correct if it reads this *before* reading the
%% index and tags the plan with the value it saw. Nothing on the current read
%% path calls it, so it costs nothing today.
-spec gen() -> non_neg_integer() | undefined.
gen() ->
    broker_router_hash_store:gen(store_name()).

partitions_ok(N) ->
    is_integer(N) andalso N > 0.

%% The registered name `broker_router_sup' gives the child, and the name init/1
%% defaults to. One source so the supervisor's child and current_env/0 cannot
%% disagree.
-spec store_name() -> atom().
store_name() ->
    broker_router_hash_store.

%%%_* Internal ----------------------------------------------------------

parse_config(#{partitions := N} = Config) when is_integer(N), N > 0 ->
    case maps:get(hash, Config, phash2) of
        phash2 -> {ok, N, phash2};
        Other -> {error, {unsupported_hash, Other}}
    end;
parse_config(Config) ->
    {error, {invalid_partitions, maps:get(partitions, Config, undefined)}}.

%% 0-based partition ids. phash2/2 already yields 0..N-1, so no offset is added.
partition(Destination, N, phash2) ->
    erlang:phash2(Destination, N).

levels(Topic) ->
    binary:split(Topic, <<"/">>, [global]).

%% A malformed filter would silently never match (or match the wrong thing), so
%% it is rejected when it is registered instead of at publish time.
valid_filter(<<>>) ->
    invalid;
valid_filter(Filter) ->
    valid_levels(levels(Filter)).

valid_levels([]) ->
    ok;
valid_levels([<<"#">>]) ->
    ok;
valid_levels([<<"#">> | _]) ->
    invalid;
valid_levels([<<"+">> | Rest]) ->
    valid_levels(Rest);
valid_levels([Level | Rest]) ->
    case has_wildcard(Level) of
        true -> invalid;
        false -> valid_levels(Rest)
    end.

has_wildcard(Level) ->
    binary:match(Level, [<<"+">>, <<"#">>]) =/= nomatch.

%% Input is sorted by registration sequence, so keeping the first occurrence of
%% each SubRef preserves index order and drops the duplicates. The seen-set is a
%% map, not a list: with N subscriptions matching one topic, `lists:member/2'
%% made this O(N^2) (design §0 P3).
ordered_unique(Pairs) ->
    {Acc, _Seen} =
        lists:foldl(fun({SubRef, _Seq}, {Acc, Seen}) ->
                            case maps:is_key(SubRef, Seen) of
                                true -> {Acc, Seen};
                                false -> {[SubRef | Acc], Seen#{SubRef => true}}
                            end
                    end,
                    {[], #{}},
                    Pairs),
    lists:reverse(Acc).
