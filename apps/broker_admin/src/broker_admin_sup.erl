-module(broker_admin_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Children =
        case application:get_env(broker_admin, enabled, true) of
            true ->
                [{broker_admin_listener,
                  {broker_admin_listener, start_link, []},
                  permanent, 5000, worker, [broker_admin_listener]}];
            _ ->
                []
        end,
    {ok, {#{strategy => one_for_one, intensity => 5, period => 10}, Children}}.
