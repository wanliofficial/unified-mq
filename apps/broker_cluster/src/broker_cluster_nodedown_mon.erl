%% @doc 崩溃发现的实际监视进程（spec Plan 2 §4 步骤 1）。在 `session_replication'
%% 开启时，`monitor_node/2' 监视当前 ra 成员，收到 `{nodedown, O}' 就调用
%% `broker_cluster_nodedown:on_nodedown/1' 触发迁移；收到 `{nodeup, O}' 重新
%% 拉取成员并补挂监视。开关关闭时保持休眠（不监视，符合不变量 I2：默认关闭
%% 逐字节退化）。
%%
%% 监视“所有成员”（而非仅 owner）是刻意的：任何一个成员死亡都可能让某个分区
%% 失去 owner，`on_nodedown/1' 自己按 `Owner =:= DeadNode' 过滤。
-module(broker_cluster_nodedown_mon).
-behaviour(gen_server).

-export([start_link/0, start_link/1, rearm/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).

%% @doc 生产入口：默认 handler = broker_cluster_nodedown:on_nodedown/1。
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, default, []).

%% @doc 测试入口：注入一个 `fun((node()) -> any())' 以便断言派发（无需 dist/ra）。
start_link(Handler) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Handler, []).

%% @doc 运行时重挂（install_session_hook/0 在开关切换后调用）。无进程时安全 no-op。
rearm() ->
    _ = try gen_server:cast(?SERVER, rearm) catch _:_ -> ok end,
    ok.

init(HandlerArg) ->
    Handler = case HandlerArg of
                  default -> fun broker_cluster_nodedown:on_nodedown/1;
                  F when is_function(F, 1) -> F
              end,
    {ok, arm(#{handler => Handler, monitored => #{}, pending => #{}})}.

handle_call(_Req, _From, St) -> {reply, ok, St}.

handle_cast(rearm, St) -> {noreply, arm(St)};
handle_cast(_Msg, St) -> {noreply, St}.

handle_info({nodedown, Node}, #{handler := H, monitored := Mon, pending := Pend} = St) ->
    %% ERTS 在 nodedown 时已自动撤除该节点的 monitor；同步从 monitored 移除，避免节点
    %% 稍后回归时 monitor_members 因 maps:is_key 跳过重挂、漏判其下次崩溃。
    unmonitor_node(Node),
    Mon1 = maps:remove(Node, Mon),
    case debounce_ms() of
        0 ->
            _ = try H(Node) catch _:_ -> ok end,
            {noreply, St#{monitored := Mon1}};
        Ms when is_integer(Ms), Ms > 0 ->
            case maps:is_key(Node, Pend) of
                true ->
                    %% 同一断连的重复 nodedown：忽略，不重置定时器。
                    {noreply, St#{monitored := Mon1}};
                false ->
                    TRef = erlang:send_after(Ms, self(), {nodedown_expired, Node}),
                    {noreply, St#{monitored := Mon1, pending := maps:put(Node, TRef, Pend)}}
            end
    end;
handle_info({nodeup, Node}, #{pending := Pend} = St) ->
    %% 窗口内自愈：取消该节点的去抖定时器（若有），不迁移；随后重挂监视。
    {noreply, monitor_members(St#{pending := cancel_pending(Node, Pend)})};
handle_info({nodedown_expired, Node}, #{handler := H, pending := Pend} = St) ->
    %% 去抖窗口到点、期间无 nodeup：确认真崩溃，派发迁移。
    _ = try H(Node) catch _:_ -> ok end,
    {noreply, St#{pending := maps:remove(Node, Pend)}};
handle_info(_Other, St) ->
    {noreply, St}.

terminate(_Reason, #{monitored := Mon, pending := Pend}) ->
    _ = [unmonitor_node(N) || N <- maps:keys(Mon)],
    _ = [try erlang:cancel_timer(TRef) catch _:_ -> ok end
         || TRef <- maps:values(Pend)],
    ok.

%%%_* Internal ------------------------------------------------------------

%% 开关开 → 监视当前成员；关 → 撤掉所有监视（I2 退化）。
arm(St) ->
    case application:get_env(broker_cluster, session_replication, false) of
        true -> monitor_members(St);
        _ -> unmonitor_all(St)
    end.

monitor_members(#{monitored := Mon} = St) ->
    Peers = case current_members() of
                Members when is_list(Members) ->
                    [N || N <- Members, N =/= node()];
                _ -> []
            end,
    %% 先补挂新对端，再撤掉已不在集合的旧监视。
    New = lists:foldl(
            fun(N, Acc) ->
                    case maps:is_key(N, Acc) of
                        true -> Acc;
                        false ->
                            _ = try erlang:monitor_node(N, true)
                                catch _:_ -> ok end,
                            maps:put(N, true, Acc)
                    end
            end, Mon, Peers),
    Stale = [N || N <- maps:keys(New), not lists:member(N, Peers)],
    Mon1 = lists:foldl(
             fun(N, Acc) ->
                     unmonitor_node(N),
                     maps:remove(N, Acc)
             end, New, Stale),
    St1 = St#{monitored := Mon1},
    %% Plan 3 (gap #3): 成员收敛后尝试扩缩容会话分区空间（幂等、有冷却）。
    %% 开关关闭 / 无权威 C 时 rescale 是 no-op；失败绝不崩监视进程。
    _ = try broker_cluster_session_bootstrap:rescale() catch _:_ -> ok end,
    St1.

unmonitor_all(#{monitored := Mon, pending := Pend} = St) ->
    _ = [unmonitor_node(N) || N <- maps:keys(Mon)],
    _ = [try erlang:cancel_timer(TRef) catch _:_ -> ok end || TRef <- maps:values(Pend)],
    St#{monitored := #{}, pending := #{}}.

%% monitor_node/2 撤监视的容错包装：任何错误都吞掉，绝不崩监视进程。
unmonitor_node(Node) ->
    try erlang:monitor_node(Node, false)
    catch _:_ -> ok
    end.

%% 去抖窗口（毫秒）：应用环境 nodedown_debounce_ms，缺省 1500；非负整数，非法值回落默认。
debounce_ms() ->
    case application:get_env(broker_cluster, nodedown_debounce_ms, 1500) of
        N when is_integer(N), N >= 0 -> N;
        _ -> 1500
    end.

%% 取消节点 O 的去抖定时器（若有）：cancel_timer + flush 可能已投递的裸
%% {nodedown_expired, O}，返回更新后的 pending。无定时器则原样返回。
cancel_pending(Node, Pend) ->
    case maps:take(Node, Pend) of
        {TRef, Rest} ->
            _ = erlang:cancel_timer(TRef),
            receive {nodedown_expired, Node} -> ok after 0 -> ok end,
            Rest;
        error ->
            Pend
    end.

%% 成员读取容错：ra 未起 / 无 quorum 时退回空列表（休眠），绝不崩。
current_members() ->
    try broker_cluster_meta:members() of
        L when is_list(L) -> L;
        _ -> []
    catch
        _:_ -> []
    end.
