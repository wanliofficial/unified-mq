%% @doc 会话→分区映射 + owner 视图读（spec 2026-11-22 Plan 2 §3/§5）。
%%
%% 两套独立空间（spec §5 取舍）：会话分区空间由 `session_partition_count'
%% 定义稠密 `0..C-1'，用 `erlang:phash2(ClientId, C)' 确定性散列；与
%% `broker_rebalance' 的动态元数据分区**互不干扰**（后者随 rebalance 变，前者
%% 固定），避免会话归属漂移。owner 视图从**本地** `broker_cluster_cache' 读，
%% 绝不每 append 走 ra（同 ADR-010 数据路径原则）。
-module(broker_cluster_session_owner).

-export([partition_of/2, session_partition_count/0, session_owner/1]).

-type partition() :: non_neg_integer().

%% @doc 确定性散列 ClientId 到 `[0, Count)' 的会话分区。
-spec partition_of(binary(), pos_integer()) -> partition().
partition_of(ClientId, Count) when is_binary(ClientId), is_integer(Count), Count > 0 ->
    erlang:phash2(ClientId, Count).

%% @doc 会话分区数（env，默认 4）。变更会使会话重新散列，由 epoch fencing
%% 兜底陈旧写入（spec §5）。
-spec session_partition_count() -> pos_integer().
session_partition_count() ->
    case application:get_env(broker_cluster, session_partition_count) of
        N when is_integer(N), N > 0 -> N;
        _ -> 4
    end.

%% @doc 本节点上某会话分区的 owner（本地 cache 读，无元数据 → not_found）。
-spec session_owner(partition()) -> {ok, node()} | not_found.
session_owner(Partition) ->
    case broker_cluster_cache:owner(Partition) of
        {ok, Owner} when is_atom(Owner) -> {ok, Owner};
        _ -> not_found
    end.
