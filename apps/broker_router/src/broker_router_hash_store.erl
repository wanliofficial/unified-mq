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
-module(broker_router_hash_store).

-behaviour(gen_server).

-export([start_link/1, register/4, unregister/2, table/1, env/1, gen/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(st, {name :: atom(),
             index :: ets:tid(),
             subs :: ets:tid(),
             gen :: atomics:atomics_ref(),
             seq = 0 :: non_neg_integer()}).

start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [Name], []).

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
    Index = ets:new(index, [set, protected, {read_concurrency, true}]),
    Subs = ets:new(subs, [set, protected, {read_concurrency, true}]),
    Gen = atomics:new(1, [{signed, false}]),
    persistent_term:put(key(Name), #{store => self(), tab => Index, gen => Gen}),
    {ok, #st{name = Name, index = Index, subs = Subs, gen = Gen}}.

%% {{Tenant, Filter}, [{SubRef, Seq}]} -- Seq is monotonic, so each filter's
%% list is already in registration order and route/3 can restore a global order
%% cheaply. The tenant in the key is what makes cross-tenant delivery impossible
%% at the read path: a publisher with tenant T never even sees another tenant's
%% entries.
handle_call({register, Tenant, Filter, SubRef}, _From, #st{index = Index, subs = Subs} = State) ->
    #st{seq = Seq} = State,
    case ets:lookup(Subs, SubRef) of
        [_] ->
            {reply, {error, {already_registered, SubRef}}, State};
        [] ->
            Key = {normalize_tenant(Tenant), Filter},
            New = existing(Key, Index) ++ [{SubRef, Seq + 1}],
            true = ets:insert(Index, {Key, New}),
            true = ets:insert(Subs, {SubRef, Key}),
            %% Bumped after the index is written and before the caller is told
            %% `ok', so "subscribe returned" implies "gen already moved".
            _ = bump(State),
            {reply, ok, State#st{seq = Seq + 1}}
    end;

handle_call({unregister, SubRef}, _From, #st{index = Index, subs = Subs} = State) ->
    case ets:lookup(Subs, SubRef) of
        [{SubRef, Key}] ->
            Rest = lists:filter(fun({S, _Seq}) -> S =/= SubRef end, existing(Key, Index)),
            case Rest of
                [] -> true = ets:delete(Index, Key);
                _ -> true = ets:insert(Index, {Key, Rest})
            end,
            true = ets:delete(Subs, SubRef),
            _ = bump(State),
            {reply, ok, State};
        [] ->
            {reply, {error, {not_found, SubRef}}, State}
    end;

handle_call(index_table, _From, #st{index = Index} = State) ->
    {reply, Index, State}.

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

existing(Key, Index) ->
    case ets:lookup(Index, Key) of
        [{Key, Subs}] -> Subs;
        [] -> []
    end.

normalize_tenant(T) when is_binary(T), T =/= <<>> -> T;
normalize_tenant(_) -> <<"*">>.
