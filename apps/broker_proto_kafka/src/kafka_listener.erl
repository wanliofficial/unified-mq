%% @doc ranch 2.x protocol adapter for the Kafka listener.
-module(kafka_listener).
-behaviour(ranch_protocol).

-export([start_link/3]).

start_link(Ref, Transport, Opts) ->
    kafka_conn:start_link(Ref, Transport, Opts).
