-module(broker_proto_kafka_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    broker_proto_kafka_sup:start_link().

stop(_State) ->
    ok.
