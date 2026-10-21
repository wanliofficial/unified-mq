-module(broker_security_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    broker_security_sup:start_link().

stop(_State) ->
    ok.
