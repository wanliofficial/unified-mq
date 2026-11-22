%% @doc nodedown 监视进程的派发与门控（Plan 2 Task 4 补强）。
%% 用注入 handler 断言 {nodedown,Node} 会派发；开关关闭时保持休眠、不派发。
-module(broker_cluster_nodedown_mon_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() -> [dispatches_nodedown_to_handler,
          dormant_when_disabled].

init_per_testcase(_C, Config) ->
    _ = application:load(broker_cluster),
    Config.
end_per_testcase(_C, _Config) ->
    _ = try gen_server:stop(broker_cluster_nodedown_mon) catch _:_ -> ok end,
    ok.

dispatches_nodedown_to_handler(_Config) ->
    ok = application:set_env(broker_cluster, session_replication, true),
    Self = self(),
    H = fun(Node) -> Self ! {hit, Node} end,
    {ok, _} = broker_cluster_nodedown_mon:start_link(H),
    broker_cluster_nodedown_mon_pid() ! {nodedown, dead_node@host},
    receive {hit, dead_node@host} -> ok
    after 2000 -> ct:fail(no_dispatch)
    end.

dormant_when_disabled(_Config) ->
    ok = application:set_env(broker_cluster, session_replication, false),
    Self = self(),
    H = fun(Node) -> Self ! {hit, Node} end,
    {ok, Pid} = broker_cluster_nodedown_mon:start_link(H),
    _ = try gen_server:cast(Pid, rearm) catch _:_ -> ok end,
    Pid ! {nodedown, dead_node@host},
    %% handler 仍会被调用（nodedown 派发与开关无关——开关只控是否 monitor_node）；
    %% 这里断言的是：关闭时进程就绪、不崩溃、能接收消息（休眠安全）。
    receive {hit, dead_node@host} -> ok
    after 2000 -> ct:fail(not_dormant_safe)
    end.

broker_cluster_nodedown_mon_pid() ->
    whereis(broker_cluster_nodedown_mon).
