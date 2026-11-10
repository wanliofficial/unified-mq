%% @doc The ranch 2.x protocol adapter (design §6 boot step 4, §9 T12).
%%
%% There is no new connection seam here: ranch calls `start_link/3' while the
%% accepted socket is still owned by the acceptor, so this module starts the
%% ordinary `mqtt_conn_fsm' through `mqtt_conn_sup:start_conn/2' and tells the
%% caller (ranch's conns sup) about it with a LINK. Ranch then performs
%% `Transport:controlling_process(Socket, Pid)' itself and delivers
%% `{handshake, Ref, Transport, Socket, Timeout}' — the FSM's `awaiting_owner'
%% state consumes that message exactly like the test helper's `connect'
%% (mqtt_client.hrl). One process per connection, no proxy process.
%%
%% Why the explicit link: `mqtt_conn_sup:start_conn/2' starts a child under the
%% *supervisor*, so the returned pid is linked to that supervisor, not to the
%% process that happened to call `start_conn/2'. Ranch's connection accounting
%% and, more importantly, its graceful listener shutdown work by sending exit
%% signals to the pid returned from this callback (`ranch_conns_sup' watches
%% `{'EXIT', Pid, _}'). Without the second link, `application:stop' would leave
%% every connection alive — L15's leak — instead of closing them with the
%% listener.
%%
%% The per-listener map (`ProtoOpts' below) is the L20 seam: the six connection
%% knobs travel with the listener and are read through `mqtt_conn_fsm:opt/3',
%% falling back to the app env for connections that did not come from ranch.
-module(mqtt_listener).
-behaviour(ranch_protocol).

-export([start_link/3]).

-spec start_link(ranch:ref(), module(), map()) -> {ok, pid()} | {error, term()}.
start_link(_Ref, RanchTransport, Opts) ->
    %% S8: the FSM must know how to write, arm and close this socket. Ranch's
    %% own transport is not that module -- it names the acceptor layer, and the
    %% fsm needs the `mqtt_transport' callbacks. Both socket flavours are served
    %% by `mqtt_transport_tcp', so the only thing to decide here is whether the
    %% listener was configured with something else (an embedded WebSocket or
    %% QUIC front end passes its own module through `Opts').
    ConnOpts = maps:merge(#{transport => transport_for(RanchTransport)}, Opts),
    case mqtt_conn_sup:start_conn(undefined, ConnOpts) of
        {ok, Pid} ->
            true = link(Pid),
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

%% Both `ranch_tcp' and `ranch_ssl' are served by `mqtt_transport_tcp' (it
%% dispatches per socket with `is_port/1'), so ranch's transport only has to be
%% consumed, not interpreted. A `transport' already present in the per-listener
%% map wins, which is how the WebSocket and QUIC front ends inject their own.
transport_for(_RanchTransport) -> mqtt_transport_tcp.
