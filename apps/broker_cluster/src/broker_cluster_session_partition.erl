%% @doc 会话分区数的推导规则与分区 owner 选择（纯函数，不碰 ra / 网络）。
%%
%% 设计见 2026-11-23-session-partition-count-scaling-design.md §3.4。
%% 抽成独立模块的理由：推导规则与 owner 选择是纯决策，必须能脱离集群单测；
%% 把 ra 提交与它分开，决策错误就不会被 ra 的失败形状掩盖。
-module(broker_cluster_session_partition).

-export([derive_count/1, next_power_of_two/1, pick_owner/2]).

-define(DEFAULT_MIN_COUNT, 4).
-define(DEFAULT_PER_NODE, 4).

%% @doc 按成员数推导会话分区数：`max(min, Members * PerNode)` 向上取 2 的幂。
%% 2 的幂让 `phash2/2` 分布更均匀，且扩缩容步长有意义（4→8→16）。
-spec derive_count(non_neg_integer()) -> pos_integer().
derive_count(MemberCount) when is_integer(MemberCount), MemberCount >= 0 ->
    Min = env_int(session_partition_min_count, ?DEFAULT_MIN_COUNT),
    PerNode = env_int(session_partitions_per_node, ?DEFAULT_PER_NODE),
    next_power_of_two(max(Min, MemberCount * PerNode)).

%% @doc 向上取最近的 2 的幂；0 与 1 都返回 1。
-spec next_power_of_two(non_neg_integer()) -> pos_integer().
next_power_of_two(N) when is_integer(N), N =< 1 ->
    1;
next_power_of_two(N) when is_integer(N) ->
    next_power_of_two(N, 1).

next_power_of_two(N, Acc) when Acc >= N -> Acc;
next_power_of_two(N, Acc) -> next_power_of_two(N, Acc * 2).

%% @doc 为分区 P 在 Members 里确定性选一个 owner：按 P 轮转，保证均摊且可复现。
%% 空列表返回 `undefined`（调用方必须处理：无成员就不该建分区）。
-spec pick_owner(non_neg_integer(), [node()]) -> node() | undefined.
pick_owner(_P, []) ->
    undefined;
pick_owner(P, Members) when is_integer(P), P >= 0 ->
    Sorted = lists:sort(Members),
    lists:nth((P rem length(Sorted)) + 1, Sorted).

env_int(Key, Default) ->
    case application:get_env(broker_cluster, Key) of
        N when is_integer(N), N > 0 -> N;
        _ -> Default
    end.
