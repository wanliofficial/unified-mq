-module(broker_proto_kafka_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% The group coordinator starts before the listeners: a connection that joins
%% a group must never find the coordinator missing.
init([]) ->
    Group = #{id => kafka_group,
              start => {kafka_group, start_link, []},
              restart => permanent,
              shutdown => 5000,
              type => worker,
              modules => [kafka_group]},
    Children = [Group | [listener_spec(S) || S <- listeners()]],
    {ok, {#{strategy => one_for_one, intensity => 5, period => 10}, Children}}.

listeners() ->
    application:get_env(broker_proto_kafka, listeners, []).

listener_spec(Spec) when is_map(Spec) ->
    Port = maps:get(port, Spec),
    Ref = maps:get(ref, Spec, {kafka, Port}),
    %% S6: TLS is a listener-wide decision; `transport/0' reads the
    %% configured PEM paths and returns ranch_tcp when disabled.
    {Transport, TlsOpts} = broker_security_tls:transport(),
    TransOpts = #{socket_opts => [{port, Port} | TlsOpts],
                  num_acceptors => maps:get(num_acceptors, Spec, 10),
                  max_connections => maps:get(max_connections, Spec, 1024)},
    (ranch:child_spec(Ref, Transport, TransOpts, kafka_listener, #{}))#{
        restart => transient
    }.
