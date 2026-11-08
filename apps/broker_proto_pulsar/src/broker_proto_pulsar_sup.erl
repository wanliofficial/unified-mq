-module(broker_proto_pulsar_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% pulsar_subscriptions owns the broker-side subscription state; listeners
%% start after it so the first connection cannot see a missing registry.
init([]) ->
    Subs = #{id => pulsar_subscriptions,
             start => {pulsar_subscriptions, start_link, []},
             restart => permanent,
             shutdown => 5000,
             type => worker,
             modules => [pulsar_subscriptions]},
    Children = [Subs | [listener_spec(S) || S <- listeners()]],
    {ok, {#{strategy => one_for_one, intensity => 5, period => 10}, Children}}.

listeners() ->
    application:get_env(broker_proto_pulsar, listeners, []).

listener_spec(Spec) when is_map(Spec) ->
    Port = maps:get(port, Spec),
    Ref = maps:get(ref, Spec, {pulsar, Port}),
    TransOpts = #{socket_opts => [{port, Port}, {packet, 4}],
                  num_acceptors => maps:get(num_acceptors, Spec, 10),
                  max_connections => maps:get(max_connections, Spec, 1024)},
    (ranch:child_spec(Ref, ranch_tcp, TransOpts, pulsar_listener, #{}))#{
        restart => transient
    }.
