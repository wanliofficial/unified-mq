-module(broker_cluster_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    %% ADR-035: the distribution TLS declaration is checked before anything
    %% else. This feature's failure mode is "the operator believes inter-node
    %% traffic is encrypted when it is not", and the only place that can be
    %% caught is here -- `-proto_dist' is fixed at boot, so by the time the
    %% supervisor runs, the transport is already decided.
    case broker_cluster_dist_tls:validate() of
        ok -> start_supervised();
        {error, Reason} -> {error, {dist_tls_invalid, Reason}}
    end.

start_supervised() ->
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
