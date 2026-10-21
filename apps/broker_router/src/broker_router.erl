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
