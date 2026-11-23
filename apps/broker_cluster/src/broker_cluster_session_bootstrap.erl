%% @doc 启动期会话分区 bootstrap（Plan 3 gap #3，设计 §3.3）。
%%
%% 缺口（调研事实 F1）：生产侧没有任何代码为会话分区 `0..C-1' 建立元数据，
%% 于是 `session_owner(P)' 恒 `not_found'，conn_fsm 永不装 monitor、永不 claim
%% —— 缺口 #4 证明的接管链路在生产从未真正生效。本模块补上这一段：
%%
%%   1. 若 cache 里没有权威 C：推导一个（env 覆盖优先），提交
%%      `{set_session_partition_count, C, 1}'；
%%   2. 对 `0..C-1' 每个分区，幂等 `assign(P, Owner, Members, Epoch+1)'，
%%      补齐缺失或 replicas 过期的分区。
%%
%% 全部操作幂等：重复执行不改变已达标的元数据（见 ensure_partition/2 的判据）。
%% 仅当 `session_replication = true' 时生效（I2 退化）。
-module(broker_cluster_session_bootstrap).

-behaviour(gen_server).

-export([ensure/1, ensure/0, rescale/1, rescale/0, reclaim/2, start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(RETRY_MS, 2000).

%%%_* Public API -----------------------------------------------------------

%% @doc 用当前集群成员跑一次 bootstrap。
%%
%% 先判开关：feature 关闭时直接返回，绝不 probe ra（否则 gen_server 每 2s
%% 白跑一次 members() + ra 查询）。
-spec ensure() -> ok | {error, term()}.
ensure() ->
    case application:get_env(broker_cluster, session_replication, false) of
        true ->
            case members() of
                {ok, Members} -> ensure(Members);
                {error, _} = E -> E
            end;
        _ ->
            ok
    end.

%% @doc 用显式成员跑一次 bootstrap（测试用，避免依赖 ra 成员视图）。
-spec ensure([node()]) -> ok | {error, term()}.
ensure(Members) when is_list(Members), Members =/= [] ->
    case application:get_env(broker_cluster, session_replication, false) of
        true -> do_ensure(Members);
        _ -> ok
    end;
ensure(_) ->
    {error, no_members}.

%%%_* Internal (pure, testable) ---------------------------------------------

do_ensure(Members) ->
    case ensure_count(Members) of
        {ok, Count} -> ensure_partitions(Count, Members);
        {error, _} = E -> E
    end.

%% 1) 权威 C 不存在就建；存在就以它为准确保分区空间一致。
%% C 必须先在 ra 里提交成功才继续（I9：C 集群一致）。提交失败（无 quorum /
%% leader 未选出）时把错误往上抛，让 gen_server 的 2s 重试循环再来一遍——
%% 绝不能吞掉错误后假装 ok，否则分区元数据永远建不起来、接管链路静默失效。
ensure_count(Members) ->
    case broker_cluster_cache:session_partition_config() of
        {ok, Count, _Epoch} ->
            {ok, Count};
        not_found ->
            Desired = desired_count(Members),
            case broker_cluster:set_session_partition_count(Desired) of
                {ok, _} -> {ok, Desired};
                {error, _} = E -> E
            end
    end.

%% env 显式配置优先，否则按成员数推导（与 session_owner 的兜底一致）。
%% 用 get_env/3，否则 arity-2 返回 {ok, V} 会让守卫永不匹配。
desired_count(Members) ->
    case application:get_env(broker_cluster, session_partition_count, undefined) of
        N when is_integer(N), N > 0 -> N;
        _ -> broker_cluster_session_partition:derive_count(length(Members))
    end.

%% 2) 对 0..C-1 幂等 assign。
ensure_partitions(Count, Members) ->
    lists:foreach(
      fun(P) -> ensure_partition(P, Members) end, lists:seq(0, Count - 1)),
    ok.

ensure_partition(P, Members) ->
    Sorted = lists:sort(Members),
    Owner = broker_cluster_session_partition:pick_owner(P, Sorted),
    case broker_cluster_cache:get(P) of
        {ok, #{replicas := Rs, owner := O}} when Rs =:= Sorted, O =:= Owner ->
            ok;                                   %% 已达标，幂等 no-op
        _ ->
            Epoch = broker_cluster:epoch_or_zero(P) + 1,
            _ = broker_cluster:assign(P, Owner, Sorted, Epoch),
            ok
    end.

%%%_* Autoscale (design §3.4/§3.5) -----------------------------------------

%% @doc 成员变化驱动扩缩容（设计 §3.4）。相同 C 不动；冷却期内不动；
%% 否则提交新 C，再按新 C 重建/回收分区。默认开启（session_partition_autoscale）。
%% 返回 `{rescaled, OldC, NewC} | unchanged | {error, Reason}`。
-spec rescale() -> {rescaled, pos_integer(), pos_integer()} | unchanged | {error, term()}.
rescale() ->
    case autoscale_enabled() of
        false ->
            unchanged;
        true ->
            case members() of
                {ok, Members} -> rescale(Members);
                {error, _} = E -> E
            end
    end.

-spec rescale([node()]) ->
          {rescaled, pos_integer(), pos_integer()} | unchanged | {error, term()}.
rescale(Members) when is_list(Members), Members =/= [] ->
    case autoscale_enabled() of
        false ->
            unchanged;
        true ->
            case broker_cluster_cache:session_partition_config() of
                {ok, OldC, _Epoch} ->
                    NewC = broker_cluster_session_partition:derive_count(length(Members)),
                    maybe_rescale(OldC, NewC, Members);
                not_found ->
                    %% 还没有权威 C：走 ensure（它会建）。
                    _ = ensure(Members),
                    unchanged
            end
    end;
rescale(_) ->
    {error, no_members}.

maybe_rescale(OldC, OldC, _Members) ->
    unchanged;
maybe_rescale(OldC, NewC, Members) ->
    case within_rescale_cooldown() of
        true ->
            unchanged;
        false ->
            case broker_cluster:set_session_partition_count(NewC) of
                {ok, _Epoch} ->
                    record_rescale_now(),
                    %% 扩容：为 0..NewC-1 建/修元数据。
                    _ = ensure_partitions(NewC, Members),
                    %% 缩容：NewC..OldC-1 这些已不存在的分区回收。
                    _ = reclaim(shrink_range(NewC, OldC), Members),
                    {rescaled, OldC, NewC};
                {error, _} = E ->
                    E
            end
    end.

%% 缩容区间：`NewC..OldC-1`；扩容时（NewC >= OldC）为空列表。
%% 不能直接用 `lists:seq(NewC, OldC - 1)`——Erlang 的 seq/2 在 From > To 时
%% 抛 function_clause，不是返回空（扩容就会崩）。
shrink_range(NewC, OldC) when NewC >= OldC -> [];
shrink_range(NewC, OldC) -> lists:seq(NewC, OldC - 1).

%% @doc 缩容回收（设计 §3.5）：对给定分区列表中"owner 已不在当前成员里"的
%% 提交 remove。仍有存活 owner 的分区不删（等 conn_fsm 迁移）。
%% 复用调用方传入的 Members，避免每个分区再打一次 ra 成员查询。
-spec reclaim([non_neg_integer()], [node()]) -> {reclaimed, [non_neg_integer()]}.
reclaim(Partitions, Members) ->
    Gone = lists:filter(fun(P) -> partition_unused(P, Members) end, Partitions),
    lists:foreach(fun(P) -> _ = broker_cluster:remove(P) end, Gone),
    {reclaimed, Gone}.

%% 判据：没有元数据 → 无需回收；有 owner 但该 owner 已不在当前成员里 → 回收。
partition_unused(P, Members) ->
    case broker_cluster_cache:get(P) of
        not_found -> false;
        {ok, #{owner := O}} -> not lists:member(O, Members)
    end.

%% I2 退化：feature 关闭时扩缩容也必须整体停摆（不能因为曾建过元数据就继续写），
%% 否则关闭开关后节点仍会改 C。两个开关都开才生效。
autoscale_enabled() ->
    case application:get_env(broker_cluster, session_replication, false) of
        true ->
            application:get_env(broker_cluster, session_partition_autoscale, true);
        _ ->
            false
    end.

cooldown_ms() ->
    case application:get_env(broker_cluster, rescale_cooldown_ms, 300000) of
        N when is_integer(N), N >= 0 -> N;
        _ -> 300000
    end.

within_rescale_cooldown() ->
    case persistent_term:get({?MODULE, last_rescale_ms}, undefined) of
        undefined -> false;
        Last -> erlang:monotonic_time(millisecond) - Last < cooldown_ms()
    end.

record_rescale_now() ->
    persistent_term:put({?MODULE, last_rescale_ms},
                        erlang:monotonic_time(millisecond)),
    ok.

members() ->
    try broker_cluster_meta:members() of
        L when is_list(L), L =/= [] -> {ok, L};
        _ -> {error, no_members}
    catch
        _:_ -> {error, not_started}
    end.

%%%_* gen_server shell ------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    %% 不在 init 里阻塞跑 bootstrap：ra 可能还没选出 leader。用 0 超时投递
    %% 一条自消息，让 init 先返回、supervisor 起完兄弟进程后再跑。
    self() ! run_bootstrap,
    {ok, #{}}.

handle_info(run_bootstrap, St) ->
    case ensure() of
        ok -> {noreply, St};
        {error, _} ->
            %% 无成员 / ra 未就绪：稍后重试，不崩（崩溃会让 supervisor 重启循环）。
            _ = erlang:send_after(?RETRY_MS, self(), run_bootstrap),
            {noreply, St}
    end;
handle_info(_Other, St) ->
    {noreply, St}.

handle_call(_Req, _From, St) -> {reply, ok, St}.
handle_cast(_Msg, St) -> {noreply, St}.
terminate(_Reason, _St) -> ok.
code_change(_OldVsn, St, _Extra) -> {ok, St}.
