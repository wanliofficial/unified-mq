%% @doc ranch 2.x protocol adapter for the WebSocket listener.
%%
%% Unlike the MQTT adapter (which starts its FSM under `mqtt_conn_sup' and adds
%% an explicit link), the WebSocket connection process is started directly as a
%% `gen_server' linked to ranch's connection supervisor. Ranch is the only
%% owner of the process, so `application:stop/1' closes the fleet with the
%% listener.
-module(ws_listener).
-behaviour(ranch_protocol).

-export([start_link/3]).

start_link(Ref, Transport, Opts) ->
    ws_conn:start_link(Ref, Transport, Opts).
