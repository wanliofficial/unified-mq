%% @doc S1 default router (ADR-010 §10.3): destination hash -> partition, plus a
%% MQTT 3.1.1 §4.7 wildcard subscription index.
%%
%% `route/3' is a plain function call that reads the index table inside the
%% calling process; only `register/4' and `unregister/2' hand work to the single
%% writer. That split is the frozen point 【F3】 of §10.1 -- routing must not
%% serialize through one gen_server (README §7.2).
-module(broker_router_hash).

-behaviour(broker_router).

-include_lib("broker_core/include/broker_message.hrl").
-include("broker_router.hrl").

-export([init/1, route/3, register/4, unregister/2, current_env/0]).

-type env() :: #{store := pid(),
                 tab := ets:tid(),
                 partitions := pos_integer(),
                 hash := phash2}.

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
                    {ok, #{store => Pid,
                           tab => broker_router_hash_store:table(Pid),
                           partitions => N,
                           hash => Hash}};
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
%% S1 does not consult Ctx: this deployment is single-tenant (broker_security is
%% a stub with one user), and SubOpts is not indexed either. Multi-tenant filter
%% isolation has to be added where the tenant becomes part of the index key --
%% see progress.md S1 residual items.
route(Destination, _Ctx, #{tab := Tab, partitions := N, hash := Hash}) ->
    TopicLevels = levels(Destination),
    Matches =
        lists:flatmap(
            fun({Filter, Subs}) ->
                case match(TopicLevels, levels(Filter)) of
                    true -> Subs;
                    false -> []
                end
            end,
            ets:tab2list(Tab)),
    {ok, [{partition(Destination, N, Hash), ordered_unique(lists:keysort(2, Matches))}]}.

-spec register(env(), binary(), broker_router:sub_ref(), map()) -> ok | {error, term()}.
register(#{store := Store}, Filter, SubRef, _SubOpts) ->
    case valid_filter(Filter) of
        ok -> broker_router_hash_store:register(Store, Filter, SubRef);
        invalid -> {error, {invalid_filter, Filter}}
    end.

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
-spec current_env() -> {ok, env()} | {error, term()}.
current_env() ->
    Partitions = application:get_env(broker_router, partitions, undefined),
    case {whereis(store_name()), partitions_ok(Partitions)} of
        {undefined, _} ->
            {error, router_store_not_started};
        {_, false} ->
            {error, {invalid_partitions, Partitions}};
        %% Same guard as init/1: an out-of-range count would make modulo
        %% arithmetic on the first publish crash, and here it is caught at boot
        %% instead. The hash stays phash2 -- S1 offers no other, and the app env
        %% has no `hash' key (init/1 took it from a Config map nobody builds at
        %% boot; see design §6 'two partition keys').
        {Pid, true} ->
            {ok, #{store => Pid,
                   tab => broker_router_hash_store:table(Pid),
                   partitions => Partitions,
                   hash => phash2}}
    end.

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

%% MQTT 3.1.1 §4.7: # stands for the parent level and any number below it, so
%% "sport/#" matches "sport" as well as "sport/a/b".
match(_TopicLevels, [<<"#">>]) ->
    true;
match([], []) ->
    true;
match([_ | Topic], [<<"+">> | Filter]) ->
    match(Topic, Filter);
match([Level | Topic], [Level | Filter]) ->
    match(Topic, Filter);
match(_Topic, _Filter) ->
    false.

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
%% each SubRef preserves index order and drops the duplicates.
ordered_unique(Pairs) ->
    ordered_unique(Pairs, [], []).

ordered_unique([], _Seen, Acc) ->
    lists:reverse(Acc);
ordered_unique([{SubRef, _Seq} | Rest], Seen, Acc) ->
    case lists:member(SubRef, Seen) of
        true ->
            ordered_unique(Rest, Seen, Acc);
        false ->
            ordered_unique(Rest, [SubRef | Seen], [SubRef | Acc])
    end.
