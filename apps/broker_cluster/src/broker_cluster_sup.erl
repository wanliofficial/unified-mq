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
    %% Plan 3 (gap #3): 启动 bootstrap，为 0..C-1 建立会话分区元数据。
    %% 必须排在 Meta 之后（要读 ra 成员、提交命令）；permanent 重启会让它
    %% 在 Meta 恢复后自动重跑一次（幂等，安全）。
    Boot = #{id => broker_cluster_session_bootstrap,
             start => {broker_cluster_session_bootstrap, start_link, []},
             restart => permanent,
             shutdown => 5000,
             type => worker,
             modules => [broker_cluster_session_bootstrap]},
    {ok, {#{strategy => one_for_one, intensity => 5, period => 10},
          [Cache, Meta, Mon, Boot]}}.
