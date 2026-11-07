-module(broker_retain_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% One permanent worker. It owns the whole retain tree in memory and is the
%% only writer of `retain.v1', so `permanent' is enough: everything it knows is
%% rebuilt from that file.
init([]) ->
    {ok, {#{strategy => one_for_one, intensity => 5, period => 10},
          [#{id => broker_retain,
             start => {broker_retain, start_link, []},
             restart => permanent,
             shutdown => 5000,
             type => worker,
             modules => [broker_retain]}]}}.
