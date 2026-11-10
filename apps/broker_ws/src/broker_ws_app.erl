%% @doc `broker_ws' has no processes and no configuration: it is a pure RFC 6455
%% codec library (`ws_frame' + `ws_handshake') shared by the two WebSocket
%% front ends. The application callback exists only so `application:ensure_all_
%% started/1' callers see a normal node; it starts nothing.
-module(broker_ws_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    {ok, self()}.

stop(_State) ->
    ok.
