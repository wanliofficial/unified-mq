%% @doc Single writer for the `broker_router_hash' subscription index.
%%
%% The tables are protected, which means only this process can mutate them and
%% every other process can still read them. That is what makes the §10.1【F3】
%% split mechanical: the read path in `broker_router_hash:route/3' needs no
%% process here, and no caller can write the index behind its back.
%%
%% S11x (docs/design/router-read-path.md §2): the env a caller needs in order to
%% reach that table is published here once, at init, together with a monotonic
%% `gen' counter. Readers used to pay a `gen_server:call' per publish just to
%% learn the table id -- a node-wide serialization point on a value that never
%% changes. Now `env/1' is a `persistent_term' read plus an aliveness check, and
%% every successful write bumps `gen' so a future route-plan cache (design §4)
%% can invalidate without a call.
%%
%% Two rules exist to keep that cheap, and breaking either is a regression:
%%
%%   1. `persistent_term:put' happens in `init/1' only. Writes to it trigger a
%%      VM-wide literal scan; doing it on the write path would cost more than
%%      the call it replaced. Invalidation is `atomics', never a put.
%%   2. Readers never `persistent_term:erase'. Erase is a global operation too;
%%      cleanup belongs to `terminate/2', with `is_process_alive/1' covering the
%%      shutdown path where `terminate/2' does not run (a gen_server that does
%%      not trap exits is killed by the supervisor's shutdown signal).
%%
%% S11x change 2 (design §3): the index is a trie, not a `{Filter, Subs}'
%% flat table. One row per filter *node*:
%%
%%     {{Tenant, Path}, Children, Subs}
%%       Path     :: [Level]          levels from the root that name the node
%%       Children :: #{Level => true} the next level of every filter below it
%%       Subs     :: [{SubRef, Seq}]  filters that end exactly here
%%
%% The read path then walks the destination's levels instead of copying every
%% filter in the node (`ets:tab2list/1' cost scaled with total subscriptions,
%% not with matches). `+` and `#` are ordinary keys in `Children`, so they share
%% the literal path's code instead of being special rows.
%%
%% Deletes prune: a node with no children and no subscriptions is removed, and
%% its link is removed from its parent -- otherwise churn over a long uptime
%% would leave the skeleton of every filter ever registered.
-module(broker_router_hash_store).

-behaviour(gen_server).

-export([start_link/1, start_link/2, register/4, unregister/2, table/1, env/1, gen/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(st, {name :: atom(),
             trie :: ets:tid(),
             subs :: ets:tid(),
             gen :: atomics:atomics_ref(),
             seq = 0 :: non_neg_integer()}).

start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [Name], []).

%% Boot path for `broker_router_hash:init/1', which has an explicit partition
%% count and must not be told one by app env. `broker_router_sup' keeps using
%% `start_link/1' and lets the store read the app env, because at boot the sup is
%% the only place that knows it and it must fail here -- not on the first publish
%% -- if the count is unusable.
start_link(Name, Config) ->
    gen_server:start_link({local, Name}, ?MODULE, [Name, Config], []).

%% S6: the tenant is part of the index key, so `route/3' can filter without a
%% second lookup. `<<"*">>' means "legacy subscription, visible to every tenant"
%% (the pre-S6 behaviour preserved for sessions that predate tenant metadata).
register(Store, Tenant, Filter, SubRef) ->
    gen_server:call(Store, {register, Tenant, Filter, SubRef}).

unregister(Store, SubRef) ->
    gen_server:call(Store, {unregister, SubRef}).

%% The env handed to callers carries the table id, taken once at init.
%%
%% Kept so a caller that only has the registered name can still ask the process
%% directly; `env/1' is the path the publish hot path uses, and the one new code
%% should take. Retaining this export is also the rollback route for change 1:
%% reverting means pointing `broker_router_hash:current_env/0' back at this.
table(Store) ->
    gen_server:call(Store, index_table).

%% @doc Serving copy of the env, readable without messaging the writer.
%%
%% Keyed by the registered name, not a global singleton: `broker_router_hash:init/1'
%% lets a test start a store under a custom name, and two stores on one node must
%% not overwrite each other's env.
%%
%% The aliveness check is what makes a stale env harmless. A supervisor shutdown
%% kills a non-trapping gen_server without running `terminate/2', so the
%% persistent_term entry can survive the table it describes; reporting
%% `router_store_not_started' is exactly what the old `whereis/1' check did.
-spec env(atom()) -> {ok, map()} | {error, router_store_not_started}.
env(Name) ->
    case persistent_term:get(key(Name), undefined) of
        #{store := Pid} = Env ->
            case is_process_alive(Pid) of
                true -> {ok, Env};
                false -> {error, router_store_not_started}
            end;
        undefined ->
            {error, router_store_not_started}
    end.

%% @doc Monotonic count of successful index writes.
%%
%% Not an invalidation trigger by itself: a cache is correct only if it reads
%% this *before* it reads the index and tags the built plan with the value it
%% saw (design §2.4). Read after the index and a concurrent write could be
%% missed for one publish.
-spec gen(atom()) -> non_neg_integer() | undefined.
gen(Name) ->
    case persistent_term:get(key(Name), undefined) of
        #{gen := Gen} -> atomics:get(Gen, 1);
        undefined -> undefined
    end.

%%%_* gen_server ---------------------------------------------------------

init([Name]) ->
    init([Name, #{}]);
init([Name, Config]) ->
    %% `partitions' and `hash' are boot config: read once here, published with the
    %% rest of the env. Callers used to re-read the partition count from app env on
    %% every publish (`current_env/0'), which cost a full `application:get_env/3'
    %% -- an `ac_tab' lookup, measured at ~1.1 us -- plus a map rebuild, for a
    %% value that cannot change without a partition migration (S4).
    Partitions = maps:get(partitions, Config,
                          application:get_env(broker_router, partitions, undefined)),
    Hash = maps:get(hash, Config, phash2),
    case is_integer(Partitions) andalso Partitions > 0 of
        false ->
            %% Fail at start, which the supervisor turns into a failed boot. Before
            %% this check moved here, a bad count surfaced as a crash in a publisher
            %% on the first message, or as `{error, {invalid_partitions, _}}' from a
            %% read path nobody checks.
            {stop, {invalid_partitions, Partitions}};
        true ->
            Trie = ets:new(trie, [set, protected, {read_concurrency, true}]),
            Subs = ets:new(subs, [set, protected, {read_concurrency, true}]),
            Gen = atomics:new(1, [{signed, false}]),
            persistent_term:put(key(Name), #{store => self(), tab => Trie, gen => Gen,
                                             partitions => Partitions, hash => Hash}),
            {ok, #st{name = Name, trie = Trie, subs = Subs, gen = Gen}}
    end.

%% A write is: link the leaf into the tree (one row per ancestor), then append
%% to the leaf's own list. Seq is monotonic, so a leaf's list is already in
%% registration order and route/3 can restore a global order with one keysort.
handle_call({register, Tenant, Filter, SubRef}, _From,
            #st{trie = Trie, subs = Subs} = State) ->
    #st{seq = Seq} = State,
    case ets:lookup(Subs, SubRef) of
        [_] ->
            {reply, {error, {already_registered, SubRef}}, State};
        [] ->
            T = normalize_tenant(Tenant),
            Path = levels(Filter),
            ok = link(T, Trie, [], Path),
            {Children, AtLeaf} = node(Trie, T, Path),
            true = ets:insert(Trie, {{T, Path}, {Children, AtLeaf ++ [{SubRef, Seq + 1}]}}),
            true = ets:insert(Subs, {SubRef, {T, Path}}),
            %% Bumped after the index is written and before the caller is told
            %% `ok', so "subscribe returned" implies "gen already moved".
            _ = bump(State),
            {reply, ok, State#st{seq = Seq + 1}}
    end;

handle_call({unregister, SubRef}, _From, #st{trie = Trie, subs = Subs} = State) ->
    case ets:lookup(Subs, SubRef) of
        [{SubRef, {T, Path}}] ->
            {Children, AtLeaf} = node(Trie, T, Path),
            Rest = lists:keydelete(SubRef, 1, AtLeaf),
            %% The row may look deletable but still be an ancestor: with both
            %% `a/b' and `a/b/c' registered, removing `a/b' must leave `[a,b]'
            %% in place, because that row is the only link from `[a]' to
            %% `[a,b,c]'. Delete only a node with neither subs nor children.
            case {Rest, map_size(Children)} of
                {[], 0} ->
                    true = ets:delete(Trie, {T, Path}),
                    ok = prune(T, Trie, Path);
                _ ->
                    true = ets:insert(Trie, {{T, Path}, {Children, Rest}})
            end,
            true = ets:delete(Subs, SubRef),
            _ = bump(State),
            {reply, ok, State};
        [] ->
            {reply, {error, {not_found, SubRef}}, State}
    end;

handle_call(index_table, _From, #st{trie = Trie} = State) ->
    {reply, Trie, State}.

handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_info(_Info, State) ->
    {noreply, State}.

%% Best effort: a supervisor shutdown of a non-trapping gen_server kills this
%% process without reaching here. `env/1' covers that case by checking the pid.
terminate(_Reason, #st{name = Name}) ->
    _ = persistent_term:erase(key(Name)),
    ok.

%%%_* Internal -----------------------------------------------------------

key(Name) ->
    {?MODULE, Name, env}.

bump(#st{gen = Gen}) ->
    atomics:add(Gen, 1, 1).

%% The store owns its own split so a write never needs the filter binary again
%% after the caller validated it: the path *is* the filter.
levels(Filter) ->
    binary:split(Filter, <<"/">>, [global]).

%% Link `Path' into the trie by making sure every ancestor lists the next level.
%% Rows that already list the child are left alone, so re-registering a sibling
%% filter costs one write, not depth writes.
link(_T, _Trie, _Prefix, []) ->
    ok;
link(T, Trie, Prefix, [Level | Rest]) ->
    {Children, Subs} = node(Trie, T, Prefix),
    case maps:is_key(Level, Children) of
        true ->
            ok;
        false ->
            true = ets:insert(Trie, {{T, Prefix}, {Children#{Level => true}, Subs}})
    end,
    link(T, Trie, Prefix ++ [Level], Rest).

%% Remove `Path's link from its parent, and keep walking up while the parent
%% became empty. A node that still has children or subscriptions stays: it is
%% still on some other filter's path.
prune(_T, _Trie, []) ->
    ok;
prune(T, Trie, Path) ->
    Parent = lists:droplast(Path),
    Level = lists:last(Path),
    {Children, Subs} = node(Trie, T, Parent),
    Remaining = maps:remove(Level, Children),
    case {map_size(Remaining), Subs} of
        {0, []} ->
            true = ets:delete(Trie, {T, Parent}),
            prune(T, Trie, Parent);
        _ ->
            true = ets:insert(Trie, {{T, Parent}, {Remaining, Subs}}),
            ok
    end.

-spec node(ets:tid(), binary(), [binary()]) -> {map(), [{term(), pos_integer()}]}.
node(Trie, T, Path) ->
    case ets:lookup(Trie, {T, Path}) of
        [{{T, Path}, {Children, Subs}}] -> {Children, Subs};
        [] -> {#{}, []}
    end.

normalize_tenant(T) when is_binary(T), T =/= <<>> -> T;
normalize_tenant(_) -> <<"*">>.
