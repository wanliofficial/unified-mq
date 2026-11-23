%% @doc 会话→分区映射 + owner 视图读（spec 2026-11-22 Plan 2 §3/§5；
%% Plan 3 gap #3 起，会话分区数 C 的权威值来自 ra 元数据组，见设计 §3.2）。
%%
%% 两套独立空间（spec §5 取舍）：会话分区空间由 C 定义稠密 `0..C-1'，用
%% `erlang:phash2(ClientId, C)' 确定性散列；与 `broker_rebalance' 的动态元数据
%% 分区**互不干扰**（后者随 rebalance 变，前者固定），避免会话归属漂移。owner
%% 视图从**本地** `broker_cluster_cache' 读，绝不每 append 走 ra（ADR-010 数据
%% 路径原则）。
-module(broker_cluster_session_owner).

-export([partition_of/2, session_partition_count/0,
         session_partition_epoch/0, session_owner/1]).

-type partition() :: non_neg_integer().

%% @doc 确定性散列 ClientId 到 `[0, Count)' 的会话分区。
-spec partition_of(binary(), pos_integer()) -> partition().
partition_of(ClientId, Count) when is_binary(ClientId), is_integer(Count), Count > 0 ->
    erlang:phash2(ClientId, Count).

%% @doc 会话分区数（Plan 3 gap #3）：权威值在 ra 元数据组，经 cache 镜像读；
%% 尚无权威值（单机 / 集群未 bootstrap / 旧快照）时退回 env，再退回推导。
%% 读路径绝不触发共识写（ADR-008）。
-spec session_partition_count() -> pos_integer().
session_partition_count() ->
    case broker_cluster_cache:session_partition_config() of
        {ok, Count, _Epoch} -> Count;
        not_found -> configured_or_derived_count()
    end.

%% @doc 会话分区配置的版本号（单调递增）。无配置时返回 0。
%% conn_fsm 用它检测"分区空间变了，该重算分区了"。
-spec session_partition_epoch() -> non_neg_integer().
session_partition_epoch() ->
    case broker_cluster_cache:session_partition_config() of
        {ok, _Count, Epoch} -> Epoch;
        not_found -> 0
    end.

%% @doc 本节点上某会话分区的 owner（本地 cache 读，无元数据 → not_found）。
-spec session_owner(partition()) -> {ok, node()} | not_found.
session_owner(Partition) ->
    case broker_cluster_cache:owner(Partition) of
        {ok, Owner} when is_atom(Owner) -> {ok, Owner};
        _ -> not_found
    end.

%%%_* Internal ------------------------------------------------------------

%% 尚无权威 C 时的兜底：env 显式正整数优先（运维覆盖），否则按推导规则算。
%% 用 get_env/3（带默认值），否则 arity-2 返回 {ok, V} 会让守卫永不匹配。
configured_or_derived_count() ->
    case application:get_env(broker_cluster, session_partition_count, undefined) of
        N when is_integer(N), N > 0 ->
            N;
        _ ->
            Members = cluster_member_count(),
            broker_cluster_session_partition:derive_count(Members)
    end.

%% 读当前成员数；拿不到（单机 / ra 未起 / 无 quorum）按 1 算，得到最小值。
cluster_member_count() ->
    try broker_cluster_meta:members() of
        L when is_list(L), L =/= [] -> length(L);
        _ -> 1
    catch
        _:_ -> 1
    end.
