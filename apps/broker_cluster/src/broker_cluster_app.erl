-module(broker_cluster_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    case broker_cluster_sup:start_link() of
        {ok, Pid} ->
            %% Hooks go in only after the cache and the ra group are up: a
            %% guard that runs before the cache exists would accept writes it
            %% cannot check, and removing them on stop is what restores exact
            %% S1~S3 behaviour for a single-node deployment.
            ok = broker_cluster:install_hooks(),
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok = broker_cluster:uninstall_hooks(),
    ok.
