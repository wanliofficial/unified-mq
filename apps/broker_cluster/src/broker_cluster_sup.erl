-module(broker_cluster_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% The cache must exist before the ra group applies anything (`apply/3' writes
%% into it on every member), so it starts first and the metadata group is
%% allowed to fail and restart without taking the cache with it.
init([]) ->
    Cache = #{id => broker_cluster_cache,
              start => {broker_cluster_cache, start_link, []},
              restart => permanent,
              shutdown => 5000,
              type => worker,
              modules => [broker_cluster_cache]},
    Meta = #{id => broker_cluster_meta,
             start => {broker_cluster_meta, start_link, []},
             restart => permanent,
             shutdown => 10000,
             type => worker,
             modules => [broker_cluster_meta]},
    {ok, {#{strategy => one_for_one, intensity => 5, period => 10},
          [Cache, Meta]}}.
