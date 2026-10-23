-module(broker_storage_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

%% The memory backend's store is a node singleton: create/2 and recover/2 look it
%% up by name because they have no handle yet. Nothing else starts it, so a
%% release configured with `backend = broker_storage_memory' would fail every
%% create/2 with noproc.
-define(MEMORY_STORE, broker_storage_memory_store).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% Boot step 1 of design §6: recover every configured partition and fail the
%% whole application if any partition is missing, instead of serving a silently
%% empty one. Which partitions there are is not this supervisor's business --
%% `broker_queue' owns the partition list ({partitions, N}) and drives create/2
%% and recover/2 through the configured backend (H8: boot recovers, never
%% creates). What lands here is only the process structure each backend needs.
init([]) ->
    Backend = application:get_env(broker_storage, backend, broker_storage_wal),
    {ok, {#{strategy => one_for_all, intensity => 1, period => 5},
          children(Backend)}}.

children(broker_storage_wal) ->
    [#{id => broker_storage_wal_sup,
       start => {broker_storage_wal_sup, start_link, []},
       restart => permanent,
       shutdown => infinity,
       type => supervisor,
       modules => [broker_storage_wal_sup]}];
children(broker_storage_memory) ->
    [#{id => ?MEMORY_STORE,
       start => {?MEMORY_STORE, start_link, []},
       restart => permanent,
       shutdown => 5000,
       type => worker,
       modules => [?MEMORY_STORE]}];
%% An unrecognised backend must not boot a half-configured storage layer: every
%% partition would look healthy while nothing was ever written to it.
children(Backend) ->
    error({unknown_storage_backend, Backend}).
