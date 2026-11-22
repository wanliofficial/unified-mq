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
    {ok, arm(#{handler => Handler, monitored => #{}})}.

handle_call(_Req, _From, St) -> {reply, ok, St}.

handle_cast(rearm, St) -> {noreply, arm(St)};
handle_cast(_Msg, St) -> {noreply, St}.

handle_info({nodedown, Node}, #{handler := H, monitored := Mon} = St) ->
    _ = try H(Node) catch _:_ -> ok end,
    %% 配对累积的 monitor_node(true)：计数归零，避免泄漏。monitor_node/2 的 true
    %% 是引用计数的——只从 map 里删而不 demonitor，节点每次 down/up 都会净 +1，
    %% 下次 down 就会收到多条 {nodedown,_}，on_nodedown/1 被重复触发。
    unmonitor_node(Node),
    {noreply, St#{monitored := maps:remove(Node, Mon)}};
handle_info({nodeup, _Node}, St) ->
    %% 节点回来（或成员变化）：重新拉成员、补挂。nodeup 后 ra 通常已可达。
    {noreply, monitor_members(St)};
handle_info(_Other, St) ->
    {noreply, St}.

terminate(_Reason, #{monitored := Mon}) ->
    _ = [unmonitor_node(N) || N <- maps:keys(Mon)],
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
    St#{monitored := Mon1}.

unmonitor_all(#{monitored := Mon} = St) ->
    _ = [unmonitor_node(N) || N <- maps:keys(Mon)],
    St#{monitored := #{}}.

%% monitor_node/2 撤监视的容错包装：任何错误都吞掉，绝不崩监视进程。
unmonitor_node(Node) ->
    try erlang:monitor_node(Node, false)
    catch _:_ -> ok
    end.

%% 成员读取容错：ra 未起 / 无 quorum 时退回空列表（休眠），绝不崩。
current_members() ->
    try broker_cluster_meta:members() of
        L when is_list(L) -> L;
        _ -> []
    catch
        _:_ -> []
    end.
