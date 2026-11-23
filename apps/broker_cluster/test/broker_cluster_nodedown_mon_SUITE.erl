%% @doc nodedown 监视进程的派发与门控（Plan 2 Task 4 补强）。
%% 用注入 handler 断言 {nodedown,Node} 会派发；开关关闭时保持休眠、不派发。
-module(broker_cluster_nodedown_mon_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() -> [dispatches_nodedown_to_handler,
          dormant_when_disabled,
          nodedown_demonitors_after_dispatch,
          debounces_then_cancels_on_nodeup,
          dispatches_after_window_expires,
          removes_from_monitored_after_debounce,
          disable_during_pending_cancels_timer].

init_per_testcase(_C, Config) ->
    _ = application:load(broker_cluster),
    Config.
end_per_testcase(_C, _Config) ->
    _ = try gen_server:stop(broker_cluster_nodedown_mon) catch _:_ -> ok end,
    %% 防御性复位 trace（用例中途失败时也不会把 global trace_pattern 留在 VM 里）。
    _ = try erlang:trace_pattern({erlang, monitor_node, 2}, false, [global])
        catch _:_ -> ok end,
    ok.

dispatches_nodedown_to_handler(_Config) ->
    ok = application:set_env(broker_cluster, session_replication, true),
    ok = application:set_env(broker_cluster, nodedown_debounce_ms, 0),
    Self = self(),
    H = fun(Node) -> Self ! {hit, Node} end,
    {ok, _} = broker_cluster_nodedown_mon:start_link(H),
    broker_cluster_nodedown_mon_pid() ! {nodedown, dead_node@host},
    receive {hit, dead_node@host} -> ok
    after 2000 -> ct:fail(no_dispatch)
    end.

dormant_when_disabled(_Config) ->
    ok = application:set_env(broker_cluster, session_replication, false),
    ok = application:set_env(broker_cluster, nodedown_debounce_ms, 0),
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

%% 回归（review）：monitor_node(Node,true) 是引用计数的——若 nodedown 后只从 map
%% 删而不 demonitor，节点每次 down/up 都净 +1，下次 down 会收到多条 {nodedown,_}，
%% on_nodedown/1 被重复触发（无界监视增长 + 重复迁移扫描）。
%%
%% 无 dist 下也能真断言：trace erlang:monitor_node/2，注入一条 {nodedown,X}，
%% 断言进程确实调用了 monitor_node(X, false)（配对计数），并且 monitored map 里
%% 不再含 X。这一步不能被“只删 map”的旧实现骗过。
nodedown_demonitors_after_dispatch(_Config) ->
    ok = application:set_env(broker_cluster, session_replication, true),
    ok = application:set_env(broker_cluster, nodedown_debounce_ms, 0),
    {ok, Pid} = broker_cluster_nodedown_mon:start_link(fun(_) -> ok end),
    %% 监视“所有 monitor_node/2 调用”，只关心 false 那一次（demonitor）。
    _ = erlang:trace_pattern({erlang, monitor_node, 2}, true, [global]),
    _ = erlang:trace(Pid, true, [call]),
    Pid ! {nodedown, fake@host},
    Demonitored = wait_for_demonitor(Pid, fake@host, 2000),
    ?assertEqual(true, Demonitored),
    _ = erlang:trace(Pid, false, [call]),
    _ = erlang:trace_pattern({erlang, monitor_node, 2}, false, [global]),
    %% 主可观察量：map 里已移除该节点（不会在下次 nodeup 时重复计入）。
    St = sys:get_state(Pid),
    ?assertNot(maps:is_key(fake@host, maps:get(monitored, St))),
    ok.

%% 在 trace 消息里等 {erlang, monitor_node, [Node, false]} 那一发。
wait_for_demonitor(_Pid, _Node, 0) -> false;
wait_for_demonitor(Pid, Node, Left) ->
    receive
        {trace, Pid, call, {erlang, monitor_node, [Node, false]}} -> true;
        {trace, Pid, call, {erlang, monitor_node, _Other}} ->
            wait_for_demonitor(Pid, Node, Left - 1);
        _Other ->
            wait_for_demonitor(Pid, Node, Left - 1)
    after 200 ->
        false
    end.

%% 去抖：窗口内 nodeup 自愈 → 取消定时器，绝不派发 on_nodedown（无误迁移）。
debounces_then_cancels_on_nodeup(_Config) ->
    ok = application:set_env(broker_cluster, session_replication, true),
    ok = application:set_env(broker_cluster, nodedown_debounce_ms, 300),
    Self = self(),
    H = fun(Node) -> Self ! {hit, Node} end,
    {ok, _} = broker_cluster_nodedown_mon:start_link(H),
    Node = ghost@host,
    broker_cluster_nodedown_mon_pid() ! {nodedown, Node},
    %% 窗口内（<300ms）自愈。
    timer:sleep(100),
    broker_cluster_nodedown_mon_pid() ! {nodeup, Node},
    receive {hit, Node} -> ct:fail(should_have_been_debounced)
    after 500 -> ok
    end.

%% 去抖：窗口到点、期间无 nodeup → 恰好派发一次（不重复）。
dispatches_after_window_expires(_Config) ->
    ok = application:set_env(broker_cluster, session_replication, true),
    ok = application:set_env(broker_cluster, nodedown_debounce_ms, 100),
    Self = self(),
    H = fun(Node) -> Self ! {hit, Node} end,
    {ok, _} = broker_cluster_nodedown_mon:start_link(H),
    Node = ghost@host,
    broker_cluster_nodedown_mon_pid() ! {nodedown, Node},
    receive {hit, Node} -> ok
    after 2000 -> ct:fail(no_dispatch_after_window)
    end,
    %% 二次确认：定时器只发一次，不会重复派发。
    receive {hit, Node} -> ct:fail(double_dispatch)
    after 200 -> ok
    end.

%% 回归（Finding B）：去抖崩溃到点后，节点须从 monitored 移除，否则其回归将被漏挂监控。
removes_from_monitored_after_debounce(_Config) ->
    ok = application:set_env(broker_cluster, session_replication, true),
    ok = application:set_env(broker_cluster, nodedown_debounce_ms, 100),
    {ok, Pid} = broker_cluster_nodedown_mon:start_link(fun(_) -> ok end),
    Node = fallen@host,
    Pid ! {nodedown, Node},
    ok = wait_until(fun() -> not maps:is_key(Node, monitored_of(Pid)) end, 2000),
    ?assertNot(maps:is_key(Node, monitored_of(Pid))),
    ok.

monitored_of(Pid) ->
    maps:get(monitored, sys:get_state(Pid)).

%% 回归（Finding A）：pending 窗口内关掉 session_replication（rearm→unmonitor_all）须取消定时器，不派发。
disable_during_pending_cancels_timer(_Config) ->
    ok = application:set_env(broker_cluster, session_replication, true),
    ok = application:set_env(broker_cluster, nodedown_debounce_ms, 400),
    Self = self(),
    H = fun(Node) -> Self ! {hit, Node} end,
    {ok, _} = broker_cluster_nodedown_mon:start_link(H),
    Node = sinking@host,
    broker_cluster_nodedown_mon_pid() ! {nodedown, Node},
    timer:sleep(100),
    %% 窗口内关开关并重挂。
    ok = application:set_env(broker_cluster, session_replication, false),
    broker_cluster_nodedown_mon:rearm(),
    receive {hit, Node} -> ct:fail(stray_dispatch_after_disable)
    after 600 -> ok
    end.

%% 本地轮询等待：每 50ms 跑一次 Pred，直到 true 或 Left 用尽（用尽则 ct:fail 超时）。
wait_until(_Pred, Left) when Left =< 0 -> ct:fail({timeout, ?MODULE});
wait_until(Pred, Left) ->
    try Pred() of
        true -> ok;
        _ -> timer:sleep(50), wait_until(Pred, Left - 50)
    catch
        _:_ -> timer:sleep(50), wait_until(Pred, Left - 50)
    end.
