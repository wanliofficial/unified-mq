%% @doc ranch 2.x protocol adapter for the MQTT-over-WebSocket listener.
%%
%% One process per connection, and that process is the WebSocket layer only:
%% `mqtt_ws_conn' owns the socket, performs the RFC 6455 handshake and turns
%% frames into bytes. The MQTT state machine is a second process underneath it
%% (`mqtt_conn_fsm', started through `mqtt_conn_sup'), which is what keeps
%% `mqtt_transport_ws' a pure module instead of a frame codec with hidden state.
-module(mqtt_ws_listener).
-behaviour(ranch_protocol).

-export([start_link/3]).

start_link(Ref, Transport, Opts) ->
    mqtt_ws_conn:start_link(Ref, Transport, Opts).
