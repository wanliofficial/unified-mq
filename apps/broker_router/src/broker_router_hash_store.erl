%% @doc Single writer for the `broker_router_hash' subscription index.
%%
%% The tables are protected, which means only this process can mutate them and
%% every other process can still read them. That is what makes the §10.1【F3】
%% split mechanical: the read path in `broker_router_hash:route/3' needs no
%% process here, and no caller can write the index behind its back.
-module(broker_router_hash_store).

-behaviour(gen_server).

-export([start_link/1, register/4, unregister/2, table/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(st, {index :: ets:tid(), subs :: ets:tid(), seq = 0 :: non_neg_integer()}).

start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [], []).

%% S6: the tenant is part of the index key, so `route/3' can filter without a
%% second lookup. `<<"*">>' means "legacy subscription, visible to every tenant"
%% (the pre-S6 behaviour preserved for sessions that predate tenant metadata).
register(Store, Tenant, Filter, SubRef) ->
    gen_server:call(Store, {register, Tenant, Filter, SubRef}).

unregister(Store, SubRef) ->
    gen_server:call(Store, {unregister, SubRef}).

%% The env handed to callers carries the table id, taken once at init.
table(Store) ->
    gen_server:call(Store, index_table).

%%%_* gen_server ---------------------------------------------------------

init([]) ->
    Index = ets:new(index, [set, protected, {read_concurrency, true}]),
    Subs = ets:new(subs, [set, protected, {read_concurrency, true}]),
    {ok, #st{index = Index, subs = Subs}}.

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

terminate(_Reason, _State) ->
    ok.

%%%_* Internal -----------------------------------------------------------

existing(Key, Index) ->
    case ets:lookup(Index, Key) of
        [{Key, Subs}] -> Subs;
        [] -> []
    end.

normalize_tenant(T) when is_binary(T), T =/= <<>> -> T;
normalize_tenant(_) -> <<"*">>.
