-module(broker_meta_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    %% A broken schema or table set is a boot failure, never a silent empty
    %% control plane: an operator must see it before the node accepts traffic.
    case broker_meta:ensure_schema() of
        ok -> broker_meta_sup:start_link();
        {error, Reason} -> {error, {broker_meta_startup, Reason}}
    end.

stop(_State) ->
    %% Mnesia is shared with the rest of the node; leave it running. Data-plane
    %% apps use it only through broker_meta.
    ok.
