%% @doc Supervisor for the periodic reporter (ADR-016 §16.4).
%%
%% The metrics themselves live in counters/atomics, not in a process; the only
%% child is the report loop, and it is absent when `report_interval_ms' is 0.
-module(broker_metrics_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Interval = application:get_env(broker_metrics, report_interval_ms, 60000),
    Children =
        case Interval of
            0 ->
                [];
            Ms when is_integer(Ms), Ms > 0 ->
                [#{id => broker_metrics_server,
                   start => {broker_metrics_server, start_link, [Ms]},
                   restart => permanent,
                   shutdown => 5000,
                   type => worker,
                   modules => [broker_metrics_server]}];
            Other ->
                %% Fail loudly: a typo in the interval must not silently disable
                %% observability on a node the gate report depends on.
                erlang:error({invalid_report_interval_ms, Other})
        end,
    {ok, {#{strategy => one_for_one, intensity => 5, period => 10}, Children}}.
