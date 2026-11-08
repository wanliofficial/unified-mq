-module(broker_proto_nats_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% Same posture as broker_proto_ws: the ranch listener is embedded under this
%% supervisor, so stopping the app stops accepting and ranch's conns sup shuts
%% every live connection down with the listener.
init([]) ->
    Children = [listener_spec(S) || S <- listeners()],
    {ok, {#{strategy => one_for_one, intensity => 1, period => 5}, Children}}.

listeners() ->
    application:get_env(broker_proto_nats, listeners, []).

listener_spec(Spec) when is_map(Spec) ->
    Port = maps:get(port, Spec),
    Ref = maps:get(ref, Spec, {nats, Port}),
    TransOpts = #{socket_opts => [{port, Port}],
                  num_acceptors => maps:get(num_acceptors, Spec, 10),
                  max_connections => maps:get(max_connections, Spec, 1024)},
    (ranch:child_spec(Ref, ranch_tcp, TransOpts, nats_listener, #{}))#{
        restart => transient
    }.
