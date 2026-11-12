-module(s10_0_ws_signal_app).
-behaviour(application).
-export([start/2, stop/1]).
start(_Type, _Args) -> s10_0_ws_signal_sup:start_link().
stop(_State) -> ok.
