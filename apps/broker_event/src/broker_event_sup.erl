-module(broker_event_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Dedup = #{id => broker_event_dedup,
              start => {broker_event_dedup, start_link, []},
              restart => permanent,
              shutdown => 5000,
              type => worker,
              modules => [broker_event_dedup]},
    {ok, {#{strategy => one_for_one, intensity => 5, period => 10}, [Dedup]}}.
