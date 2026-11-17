%% @doc Supervisor for the MQTT-SN gateway.
%%
%% Two kinds of child, in this order: the session supervisor first (the listener
%% starts sessions on it and refers to it by name), then one UDP listener per
%% configured spec. `one_for_all' with a low intensity, like every other
%% protocol app: a listener that cannot bind is a boot failure, and a listener
%% that dies later takes the sessions down with it rather than leaving orphaned
%% state behind a port nobody reads.
-module(broker_proto_mqtt_sn_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Children = [conn_sup_spec()
                | [listener_spec(S, I) || {I, S} <- enumerate(listeners())]],
    {ok, {#{strategy => one_for_all, intensity => 1, period => 5}, Children}}.

%% Child ids must be unique within a supervisor. `port' is NOT a usable id:
%% `port => 0' means "let the system choose", and two such listeners (the
%% plain/DTLS coexistence deployment) collided on {mqtt_sn_listener, 0} and
%% failed the whole boot with duplicate_child_name. The position in the
%% configured list is unique by construction.
enumerate(L) ->
    lists:zip(lists:seq(1, length(L)), L).

conn_sup_spec() ->
    #{id => mqtt_sn_conn_sup,
      start => {mqtt_sn_conn_sup, start_link, []},
      restart => permanent,
      shutdown => infinity,
      type => supervisor,
      modules => [mqtt_sn_conn_sup]}.

listeners() ->
    application:get_env(broker_proto_mqtt_sn, listeners, []).

%% The UDP socket is opened by the listener itself (`gen_udp:open/2'), not by a
%% ranch transport: MQTT-SN has one socket for the whole gateway, and a bad bind
%% must fail `init/1' so the application does not start half-listening.
listener_spec(Spec, Index) when is_map(Spec) ->
    #{id => {mqtt_sn_listener, Index},
      start => {mqtt_sn_listener, start_link, [Spec]},
      restart => transient,
      shutdown => 5000,
      type => worker,
      modules => [mqtt_sn_listener]}.
