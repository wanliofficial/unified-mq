%% @doc ranch 2.x protocol adapter for the Pulsar listener.
-module(pulsar_listener).
-behaviour(ranch_protocol).

-export([start_link/3]).

start_link(Ref, Transport, Opts) ->
    pulsar_conn:start_link(Ref, Transport, Opts).
