%% @doc Router behavior: destination -> partition plus the subscription index.
%% Frozen by ADR-010 §10.1 (2026-10-21).
%%
%% Callbacks take an opaque `router_env()' and never thread state: route/3 runs
%% inside the calling publisher process against a concurrently readable index,
%% so a single Router gen_server cannot become the global bottleneck
%% (README §7.2). Only index writes go through one writer process.
-module(broker_router).

-include_lib("broker_core/include/broker_message.hrl").
-include("broker_router.hrl").

-export([env/0, register/3, unregister/1]).

-export_type([router_env/0, sub_ref/0, route_ctx/0]).

-type router_env() :: term().
-type sub_ref()    :: term().

-callback init(Config :: map()) -> {ok, router_env()} | {error, term()}.

%% Read path: executed in the caller's process, no gen_server:call.
-callback route(Destination :: binary(), Ctx :: route_ctx(), router_env()) ->
    {ok, [{partition_id(), [sub_ref()]}]} | {error, term()}.

%% Write path: index an already-minted SubRef under Filter.
-callback register(router_env(), Filter :: binary(), SubRef :: sub_ref(), SubOpts :: map()) ->
    ok | {error, term()}.

-callback unregister(router_env(), SubRef :: sub_ref()) -> ok | {error, term()}.

-callback on_partition_change(router_env(), Event :: term()) -> ok | {error, term()}.
-optional_callbacks [on_partition_change/2].

%% @doc The live router env for the configured backend, assembled from the
%% store process `broker_router_sup' started -- not by calling `init/1', which
%% belongs to whoever starts a store, not to whoever uses one. Consumers are
%% boot step 3 of §6 (the subscription backfill) and publishers on the §5.1(c)
%% path. `{error, router_store_not_started}' at boot means the application is
%% assembled wrong, and the boot that depends on it must fail loudly.
%% Additive to the frozen behavior: a plain function, not a callback, so no
%% implementation must grow a new export (design §13 I12-class gap).
-spec env() -> {ok, router_env()} | {error, term()}.
env() ->
    Backend = application:get_env(broker_router, backend, broker_router_hash),
    Backend:current_env().

%% @doc Write-path facades for consumers that only have a SubRef and a Filter.
%% `register/4' stays the behavior callback it was when ADR-010 froze the shape;
%% these resolve the configured backend so `broker_subscription' -- the SubRef
%% minter, boot step 3 -- does not hardcode `broker_router_hash' or thread an
%% env it would have to re-fetch after every store restart anyway.
-spec register(binary(), sub_ref(), map()) -> ok | {error, term()}.
register(Filter, SubRef, SubOpts) ->
    case {env(), backend()} of
        {{ok, Env}, Backend} -> Backend:register(Env, Filter, SubRef, SubOpts);
        {{error, Reason}, _} -> {error, Reason}
    end.

-spec unregister(sub_ref()) -> ok | {error, term()}.
unregister(SubRef) ->
    case {env(), backend()} of
        {{ok, Env}, Backend} -> Backend:unregister(Env, SubRef);
        {{error, Reason}, _} -> {error, Reason}
    end.

backend() ->
    application:get_env(broker_router, backend, broker_router_hash).
