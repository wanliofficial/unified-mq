%% @doc Supervisor of the ICE helper (S10-C).
%%
%% `one_for_all' on purpose (ADR-029 §D4): the UDP listener owns the allocation
%% index, so a restarted listener would otherwise point at allocations whose
%% client 5-tuples it no longer knows. Restarting both together makes "listener
%% crashed" mean "every allocation is gone, clients re-Allocate", which is the
%% only honest single-node recovery.
%%
%% `enabled => false' yields an empty child list: the app starts, owns no
%% socket and no process, and can be left in every release.
-module(broker_ice_sup).
-behaviour(supervisor).

-export([start_link/1, init/1]).

start_link(Cfg) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, Cfg).

init(#{enabled := false}) ->
    {ok, {#{strategy => one_for_all, intensity => 1, period => 5}, []}};
init(#{enabled := true} = Cfg) ->
    Children = [allocation_sup_spec(Cfg), listener_spec(Cfg)],
    {ok, {#{strategy => one_for_all, intensity => 1, period => 5}, Children}}.

allocation_sup_spec(_Cfg) ->
    #{id => turn_allocation_sup,
      start => {turn_allocation_sup, start_link, []},
      restart => permanent,
      shutdown => infinity,
      type => supervisor,
      modules => [turn_allocation_sup]}.

listener_spec(Cfg) ->
    #{id => broker_ice_udp,
      start => {broker_ice_udp, start_link, [Cfg]},
      restart => permanent,
      shutdown => 5000,
      type => worker,
      modules => [broker_ice_udp]}.
