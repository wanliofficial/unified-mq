%% @doc ranch 2.x protocol adapter for the AMQP listener.
-module(amqp_listener).
-behaviour(ranch_protocol).

-export([start_link/3]).

start_link(Ref, Transport, Opts) ->
    amqp_conn:start_link(Ref, Transport, Opts).
