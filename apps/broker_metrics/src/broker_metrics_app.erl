%% @doc broker_metrics application entry (ADR-016).
%%
%% The storage (counters/atomics + the persistent_term state map) is created
%% *before* the supervisor starts, because every other app may emit a metric
%% the moment it starts and `inc/1' must never be the reason a boot fails.
-module(broker_metrics_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    ok = broker_metrics:init(),
    broker_metrics_sup:start_link().

stop(_State) ->
    ok.
