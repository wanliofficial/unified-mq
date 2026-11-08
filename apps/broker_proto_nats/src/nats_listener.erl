%% @doc ranch 2.x protocol adapter for the NATS listener.
%%
%% Like the WebSocket adapter, the connection process is started directly as a
%% `gen_server' linked to ranch's connection supervisor: ranch owns the process,
%% so stopping the listener closes the fleet.
-module(nats_listener).
-behaviour(ranch_protocol).

-export([start_link/3]).

start_link(Ref, Transport, Opts) ->
    nats_conn:start_link(Ref, Transport, Opts).
