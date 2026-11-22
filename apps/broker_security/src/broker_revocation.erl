%% @doc 吊销注册表（MQ-17 #10）：按 principal 吊销身份并驱逐其活动连接。
%%
%% 仿 `broker_admission'：两张 `public, set' 表由 `init/0' 在 app master（sup
%% 之前）创建，并把 principal 表的 id 发布到 `persistent_term'。CONNECT 热路径的
%% `revoked/1' 在调用方进程本地直读镜像，无消息往返、无 gen_server 阻塞——这正是
%% `broker_admission' 用 persistent_term + ETS 而非每次 `gen_server:call' 的原因
%% （ADR-016 §16.5 / R-LB5 禁止热路径上的无界阻塞）。
%%
%% == 为什么有 gen_server，又不走它的热路径 ==
%%
%% 表的**读**（`revoked/1'）与高频**写**（`register_session' /
%% `unregister_session'）都在调用方进程本地完成。gen_server 只承担两类低频、可
%% 延迟的活：周期性 TTL 扫描（删到期 principal 行）与死 pid 会话行的回收。驱逐
%% 派发本身在 `revoke/2' 时同步做一次（每条连接自收 `mqtt_evict' 后按正常
%% DISCONNECT 语义自清 Will、自停），gen_server 不参与。这与 `broker_admission'
%% 把 O(n) 留给拒绝时刻、把热路径留在调用方是同一思路。
%%
%% == 表 ==
%%
%%   * `broker_revocation_principals'：`Principal => permanent | ExpiryMs'。CONNECT
%%     时 `revoked/1' 的无锁查表来源。
%%   * `broker_revocation_sessions'：`{Principal, Pid} => true'。活动连接的
%%     principal 索引，供驱逐按 principal 枚举活动连接。
%%
%% 所有 ETS 帮手都容忍表不存在（`catch error:badarg' 降级）：`init/0' 可能在某个
%% 之后死掉的进程里跑（测试用例尤其如此），模块必须答"未启动"而非从连接进程里抛
%% `badarg'——与 `broker_admission' 各 helper 的容错姿态一致。
-module(broker_revocation).

%% `-behaviour' 必须与 `-module' 同处文件头部、在回调定义之前：否则编译器不把
%% 下面的 gen_server 回调认作 behaviour 契约，会把它们当 unused 函数，而本仓
%% `warnings_as_errors' 会因此编译失败。
-behaviour(gen_server).

%% gen_server 回调显式 export（同 `broker_drain'）：`init/1'、handle_*、terminate
%% 只被 behaviour 在运行时分派，静态看不到调用点。
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
-export([init/0, start_link/0, reset/0]).
-export([revoke/2, clear/1, revoked/1]).
-export([register_session/2, unregister_session/2]).

-define(PT, {?MODULE, principals_table}).
-define(TAB_PRINCIPALS, broker_revocation_principals).
-define(TAB_SESSIONS, broker_revocation_sessions).
-define(SWEEP_MS, 1000).

%% @doc 建表。由 `broker_security_app:start/2' 在 sup 之前调用（紧跟
%% `broker_admission:init/0'），使表与 app 同生命周期。
%%
%% 幂等：测试用例在每个 `init_per_testcase' 都调一次 `init/0'，而 `ets:new' 对已
%% 存在的 named table 会 `badarg'，所以先 `ets:whereis' 判存在、不存在才建。这不改
%% 任何对外行为，只是让重复初始化无害。
-spec init() -> ok.
init() ->
    _ = ensure_table(?TAB_PRINCIPALS, ?TAB_PRINCIPALS),
    _ = ensure_table(?TAB_SESSIONS, ?TAB_SESSIONS),
    persistent_term:put(?PT, ?TAB_PRINCIPALS),
    ok.

ensure_table(_, Name) ->
    case ets:whereis(Name) of
        undefined ->
            _ = ets:new(Name, [named_table, public, set,
                               {read_concurrency, true},
                               {write_concurrency, true}]),
            ok;
        _Tid ->
            ok
    end.

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc 测试专用：清空两张表。生产永不 reset——忘记了自己吊销过谁的注册表会
%% 过度拒绝。表不存在时降级为 no-op。
-spec reset() -> ok.
reset() ->
    _ = clear_table(?TAB_PRINCIPALS),
    _ = clear_table(?TAB_SESSIONS),
    ok.

%% @doc 吊销 `Principal'。`TTL' 为 `undefined' 存 `permanent'（直到手动 clear），
%% 否则存到期毫秒（`system_time(millisecond) + TTL_ms'）；随后对该 principal 的
%% 全部活动连接派发驱逐。
-spec revoke(binary(), non_neg_integer() | undefined) -> ok.
revoke(Principal, TTL) when is_binary(Principal) ->
    Expiry = case TTL of
                 undefined -> permanent;
                 Ms when is_integer(Ms), Ms >= 0 ->
                     erlang:system_time(millisecond) + Ms
             end,
    _ = insert_principal(Principal, Expiry),
    ok = evict(Principal),
    ok.

%% @doc 解除吊销。不主动 kill 已驱逐会话——驱逐在 `revoke/2' 时已完成，这里只让
%% 重连恢复允许。
-spec clear(binary()) -> ok.
clear(Principal) when is_binary(Principal) ->
    _ = delete_principal(Principal),
    ok.

%% @doc `Principal' 当前是否被吊销。读时判过期（`Expiry > now'），所以 TTL 到期即
%% 视为未吊销，不依赖扫描循环跑过。表不存在或非 binary 一律 `false'（fail-open）。
-spec revoked(binary()) -> boolean().
revoked(Principal) when is_binary(Principal) ->
    case lookup_principal(Principal) of
        permanent -> true;
        Expiry when is_integer(Expiry) ->
            Expiry > erlang:system_time(millisecond);
        undefined -> false
    end;
revoked(_) ->
    false.

%% @doc 活动连接的 principal 索引。conn FSM 在连接成功时登记自己（幂等，`set' 表
%% 以 `{Principal, Pid}' 为键，重复登记同一 pid 无害）。
-spec register_session(binary(), pid()) -> ok.
register_session(Principal, Pid) when is_binary(Principal), is_pid(Pid) ->
    _ = insert_session(Principal, Pid),
    ok.

-spec unregister_session(binary(), pid()) -> ok.
unregister_session(Principal, Pid) when is_binary(Principal), is_pid(Pid) ->
    _ = delete_session(Principal, Pid),
    ok.

%%%_* gen_server ------------------------------------------------------------

%% 只排期 TTL 扫描；表的读与高频写都不进这里。
init([]) ->
    _ = erlang:send_after(?SWEEP_MS, self(), sweep),
    {ok, #{}}.

handle_call(_Req, _From, St) ->
    {reply, ok, St}.

handle_cast(_Msg, St) ->
    {noreply, St}.

%% 扫描：删到期的 principal 行（`Expiry =< now'），并回收死 pid 的会话行，然后重排。
handle_info(sweep, St) ->
    Now = erlang:system_time(millisecond),
    _ = sweep_expired(Now, all_principals()),
    _ = reap_dead_sessions(),
    _ = erlang:send_after(?SWEEP_MS, self(), sweep),
    {noreply, St};
handle_info(_Other, St) ->
    {noreply, St}.

terminate(_Reason, _St) ->
    ok.

%%%_* 驱逐派发 --------------------------------------------------------------

%% 对该 principal 的所有活动连接发 `mqtt_evict'：每条连接自收消息后按正常
%% DISCONNECT 语义发帧、清本地 Will、stop。死 pid 跳过（`is_process_alive' 守
%% 卫）并顺带回收其行，表不泄漏。
evict(Principal) ->
    lists:foreach(
      fun({_P, Pid}) ->
              case is_process_alive(Pid) of
                  true -> Pid ! mqtt_evict;
                  false -> _ = delete_session(Principal, Pid)
              end
      end, principal_sessions(Principal)),
    ok.

%%%_* ETS 帮手（容忍表不存在）---------------------------------------------

insert_principal(P, E) ->
    try ets:insert(?TAB_PRINCIPALS, {P, E}) of true -> ok
    catch error:badarg -> ok end.

lookup_principal(P) ->
    try ets:lookup(?TAB_PRINCIPALS, P) of
        [{P, E}] -> E;
        [] -> undefined
    catch error:badarg -> undefined end.

delete_principal(P) ->
    try ets:delete(?TAB_PRINCIPALS, P) of true -> ok
    catch error:badarg -> ok end.

insert_session(P, Pid) ->
    try ets:insert(?TAB_SESSIONS, {{P, Pid}}) of true -> ok
    catch error:badarg -> ok end.

delete_session(P, Pid) ->
    try ets:delete(?TAB_SESSIONS, {P, Pid}) of true -> ok
    catch error:badarg -> ok end.

%% 展开某 principal 的全部活动 pid：`{{Principal, '$1'}} 匹配出 `[[Pid], ...]',
%% 展平为 `[{Principal, Pid}, ...]' 供 `evict/1' 消费。
principal_sessions(P) ->
    try ets:match(?TAB_SESSIONS, {{P, '$1'}}) of
        Rows -> [{P, Pid} || [Pid] <- Rows]
    catch error:badarg -> [] end.

all_principals() ->
    try ets:tab2list(?TAB_PRINCIPALS) of
        Rows -> Rows
    catch error:badarg -> [] end.

sweep_expired(Now, [{P, Expiry} | Rest]) when is_integer(Expiry) ->
    case Expiry =< Now of
        true -> _ = delete_principal(P);
        false -> ok
    end,
    sweep_expired(Now, Rest);
sweep_expired(_Now, [_Row | Rest]) ->
    %% `permanent' 行（非整数）跳过。
    sweep_expired(_Now, Rest);
sweep_expired(_Now, []) ->
    ok.

reap_dead_sessions() ->
    try ets:tab2list(?TAB_SESSIONS) of
        Rows ->
            lists:foreach(
              fun({{P, Pid}}) ->
                      case is_process_alive(Pid) of
                          true -> ok;
                          false -> _ = delete_session(P, Pid)
                      end
              end, Rows)
    catch error:badarg -> ok end.

clear_table(Tab) ->
    try ets:delete_all_objects(Tab) of true -> ok
    catch error:badarg -> ok end.
