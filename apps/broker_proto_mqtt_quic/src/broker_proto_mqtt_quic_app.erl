-module(broker_proto_mqtt_quic_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    broker_proto_mqtt_quic_sup:start_link().

stop(_State) ->
    ok.
