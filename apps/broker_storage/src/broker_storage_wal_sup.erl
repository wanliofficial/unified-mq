%% @doc Owner of the per-partition `wal_writer' processes (design §6).
%%
%% `one_for_one' with permanent children is what E11 asks for: a crashed partition
%% must come back on its own, and `simple_one_for_one' + transient would silently
%% drop it. The parent `broker_storage_sup' is `one_for_all', so putting the
%% writers here also keeps a single partition's crash from restarting the memory
%% store next to it.
%%
%% Children are added with `start_child/2' rather than listed in `init/1'. The
%% design tree says "静态", which is about restart semantics -- fixed specs,
%% permanent, never `simple_one_for_one' -- and those hold here. The reason the
%% list cannot be built in `init/1' is ADR-011 §11.2: `create/2' is the only way a
%% partition comes into existence and it is an explicit out-of-band action, so at
%% boot there is nothing to enumerate yet (H8: boot step 1 recovers, never
%% creates).
-module(broker_storage_wal_sup).

-behaviour(supervisor).

-include_lib("broker_core/include/broker_message.hrl").

-export([start_link/0, start_partition/3]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_partition(partition_id(), file:filename_all(), map()) ->
    {ok, pid()} | {error, term()}.
start_partition(PartitionId, Dir, WalConfig) ->
    supervisor:start_child(?MODULE, #{id => {wal_writer, PartitionId},
                                      start => {wal_writer, start_link,
                                                [PartitionId, Dir, WalConfig]},
                                      restart => permanent,
                                      shutdown => 5000,
                                      type => worker,
                                      modules => [wal_writer]}).

%% Intensity 4 in 30 s: one restart per partition survives a kill -9 or an OOM,
%% while a crash loop exhausts it and takes the application down. A partition
%% that cannot be recovered must stop the node, not serve an empty log.
init([]) ->
    {ok, {#{strategy => one_for_one, intensity => 4, period => 30}, []}}.
