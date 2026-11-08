-module(broker_proto_amqp_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% amqp_topo owns the transient exchange/queue/binding table; listeners start
%% after it so the first connection cannot see a missing table.
init([]) ->
    Topo = #{id => amqp_topo,
             start => {amqp_topo, start_link, []},
             restart => permanent,
             shutdown => 5000,
             type => worker,
             modules => [amqp_topo]},
    Children = [Topo | [listener_spec(S) || S <- listeners()]],
    {ok, {#{strategy => one_for_one, intensity => 5, period => 10}, Children}}.

listeners() ->
    application:get_env(broker_proto_amqp, listeners, []).

listener_spec(Spec) when is_map(Spec) ->
    Port = maps:get(port, Spec),
    Ref = maps:get(ref, Spec, {amqp, Port}),
    TransOpts = #{socket_opts => [{port, Port}],
                  num_acceptors => maps:get(num_acceptors, Spec, 10),
                  max_connections => maps:get(max_connections, Spec, 1024)},
    (ranch:child_spec(Ref, ranch_tcp, TransOpts, amqp_listener, #{}))#{
        restart => transient
    }.
