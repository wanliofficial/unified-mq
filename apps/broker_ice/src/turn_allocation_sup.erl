%% @doc Supervisor for the `temporary' TURN allocation processes (ADR-029 §D4).
%%
%% One child per allocation, started on demand with a per-allocation child
%% spec (`one_for_one' + `supervisor:start_child/2' rather than the deprecated
%% `simple_one_for_one'). Deliberately `temporary' -- an allocation that dies
%% must not be resurrected behind the client's back, because the client's
%% 5-tuple, credential and channel bindings are gone with it. `broker_ice_udp'
%% notices the DOWN and takes it out of its index.
-module(turn_allocation_sup).

-behaviour(supervisor).

-export([start_link/0, start_allocation/1, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_allocation(map()) -> {ok, pid()} | {error, term()}.
start_allocation(Opts) ->
    Spec = #{id => {turn_allocation, erlang:unique_integer([positive])},
             start => {turn_allocation, start_link, [Opts]},
             restart => temporary,
             shutdown => 5000,
             type => worker,
             modules => [turn_allocation]},
    supervisor:start_child(?MODULE, Spec).

init([]) ->
    {ok, {#{strategy => one_for_one, intensity => 1, period => 5}, []}}.
