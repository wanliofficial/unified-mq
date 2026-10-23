-module(broker_router_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

%% ADR-010 §10.6: the Filter -> SubRef index is not persisted. It is refilled by
%% broker_subscription after storage recovery and before the MQTT listener
%% accepts connections, which is why this app must start before broker_proto_mqtt
%% and after broker_storage.

%% The index writer is a node singleton, exactly like the memory storage store
%% (design §13 I12): broker_router_hash:init/1 starts one for tests, but in an
%% assembled release nobody did, so boot step 3 (register/4 backfill) would have
%% no index to write into and every route/3 would read an empty table. The
%% subscription layer reads the store through broker_router:env/0, which only
%% works while this child runs.
-define(HASH_STORE, broker_router_hash_store).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Backend = application:get_env(broker_router, backend, broker_router_hash),
    {ok, {#{strategy => one_for_all, intensity => 1, period => 5},
          children(Backend)}}.

%% The store's registered name is fixed rather than a config key: env/0 and the
%% subscription backfill must agree on where to find it, and S1 has exactly one
%% router backend.
children(broker_router_hash) ->
    [#{id => ?HASH_STORE,
       start => {?HASH_STORE, start_link, [?HASH_STORE]},
       restart => permanent,
       shutdown => 5000,
       type => worker,
       modules => [?HASH_STORE]}];
%% An unrecognised backend must not boot an app whose index nothing writes to.
children(Backend) ->
    error({unknown_router_backend, Backend}).
