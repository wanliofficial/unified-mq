%% @doc Queue supervision tree: the registry, then one worker per partition.
%%
%% E11: S1's partition count is fixed by config, so the workers are static
%% children under one_for_one. simple_one_for_one would give them transient
%% semantics and silently drop a partition when a worker dies.
%%
%% The registry comes first because a worker publishes its handle into it during
%% `init/1'. Order is also what makes §6 boot step 1 all-or-nothing: the first
%% partition that cannot be recovered aborts this supervisor, which aborts the
%% application, which is the loud failure ADR-011 §11.2 asks for.
-module(broker_queue_sup).

-behaviour(supervisor).

-include_lib("broker_core/include/broker_message.hrl").

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {#{strategy => one_for_one, intensity => 5, period => 10},
          [store_spec() | [partition_spec(Id) || Id <- broker_queue:partition_ids()]]}}.

%%%_* Internal -----------------------------------------------------------

store_spec() ->
    #{id => broker_queue_store,
      start => {broker_queue_store, start_link, []},
      restart => permanent,
      shutdown => 5000,
      type => worker,
      modules => [broker_queue_store]}.

partition_spec(PartitionId) ->
    #{id => {partition, PartitionId},
      start => {broker_queue_partition, start_link, [PartitionId]},
      restart => permanent,
      shutdown => 5000,
      type => worker,
      modules => [broker_queue_partition]}.
