-module(broker_router_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ADR-010 §10.6: the Filter -> SubRef index is not persisted. It is refilled by
%% broker_subscription after storage recovery and before the MQTT listener
%% accepts connections, which is why this app must start before broker_proto_mqtt
%% and after broker_storage.
init([]) ->
    {ok, {#{strategy => one_for_all, intensity => 1, period => 5}, []}}.
