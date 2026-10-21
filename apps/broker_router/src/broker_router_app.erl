-module(broker_router_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    broker_router_sup:start_link().

stop(_State) ->
    ok.
