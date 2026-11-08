-module(broker_proto_ws_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% The WebSocket listener is embedded under this supervisor (same posture as
%% broker_proto_mqtt): stopping the app stops accepting, and ranch's conns sup
%% shuts every live connection down with the listener.
init([]) ->
    Children = [listener_spec(S) || S <- listeners()],
    {ok, {#{strategy => one_for_one, intensity => 1, period => 5}, Children}}.

listeners() ->
    application:get_env(broker_proto_ws, listeners, []).

listener_spec(Spec) when is_map(Spec) ->
    Port = maps:get(port, Spec),
    Ref = maps:get(ref, Spec, {ws, Port}),
    %% S6: TLS is a listener-wide decision; `transport/0' reads the
    %% configured PEM paths and returns ranch_tcp when disabled.
    {Transport, TlsOpts} = broker_security_tls:transport(),
    TransOpts = #{socket_opts => [{port, Port} | TlsOpts],
                  num_acceptors => maps:get(num_acceptors, Spec, 10),
                  max_connections => maps:get(max_connections, Spec, 1024)},
    (ranch:child_spec(Ref, Transport, TransOpts, ws_listener, #{}))#{
        restart => transient
    }.
