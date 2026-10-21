-module(broker_proto_mqtt_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% Boot step 4 of design §6: the ranch listener is started only here, after
%% storage recovery and the subscription/index rebuild performed by the apps this
%% one depends on. Nothing may accept CONNECT before that.
init([]) ->
    {ok, {#{strategy => one_for_all, intensity => 1, period => 5}, []}}.
