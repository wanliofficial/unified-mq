-module(broker_queue_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% E11: S1 partition count is fixed by config, so workers are static children
%% under one_for_one. simple_one_for_one would give them transient semantics and
%% silently drop a partition when a worker dies.
init([]) ->
    {ok, {#{strategy => one_for_one, intensity => 5, period => 10}, []}}.
