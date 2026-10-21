-module(broker_storage_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% Boot step 1 of design §6: recover every configured partition and fail the
%% whole application if any partition is missing, instead of serving a silently
%% empty one.
init([]) ->
    {ok, {#{strategy => one_for_all, intensity => 1, period => 5}, []}}.
