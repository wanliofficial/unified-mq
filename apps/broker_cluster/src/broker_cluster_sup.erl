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
    %% Plan 2 T4 补强：崩溃发现监视进程，session_replication 开启时挂成员监视，
    %% 收到 {nodedown,O} 触发迁移。开关关闭时休眠（不监视，I2 退化）。
    Mon = #{id => broker_cluster_nodedown_mon,
            start => {broker_cluster_nodedown_mon, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [broker_cluster_nodedown_mon]},
    {ok, {#{strategy => one_for_one, intensity => 5, period => 10},
          [Cache, Meta, Mon]}}.
