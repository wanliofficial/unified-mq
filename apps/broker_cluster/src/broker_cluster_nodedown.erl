%% @doc 崩溃发现（spec Plan 2 §4 步骤 1-2）。各节点 monitor 所有 ra 成员；
%% 收到 {nodedown, O} 时，对 O 拥有的每个会话分区，按副本固定序选第一个存活
%% 副本作新 owner，epoch+1，提交既有 {migrate,...} 命令（复用，不新增命令型）。
%% 本模块只算「新 owner + 新 epoch」并提交；owner 写入与 epoch fencing 由
%% meta_machine 串行 apply 兜底（spec §4 唯一性来源）。
%%
%% 与 conn_fsm 的 {claim,...}（spec §4 步骤 3）是两条不同路径：本模块是
%% 「主动选存活副本迁移」，claim 是「持连节点事后抢占」。二者都经 ra 串行
%% apply，故不会双 owner。
-module(broker_cluster_nodedown).

-export([pick_new_owner/2, on_nodedown/1]).

%% @doc 为分区 P 选新 owner：replicas 里第一个非死亡、非本节点的存活副本。
%% 无存活副本 → {error, no_alive_replica}（quorum 缺失，等恢复）。
-spec pick_new_owner(non_neg_integer(), node()) ->
          {ok, node(), non_neg_integer()} | {error, no_alive_replica}.
pick_new_owner(Partition, DeadNode) ->
    case broker_cluster_cache:replicas(Partition) of
        {ok, Rs} when is_list(Rs) ->
            Alive = [N || N <- Rs, N =/= DeadNode, N =/= node()],
            case Alive of
                [NewOwner | _] ->
                    Epoch = broker_cluster:epoch_or_zero(Partition) + 1,
                    {ok, NewOwner, Epoch};
                [] ->
                    {error, no_alive_replica}
            end;
        _ ->
            {error, no_alive_replica}
    end.

%% @doc 收到 {nodedown, O}：遍历本地镜像里 O 拥有的分区，逐个提交迁移。
%% fire-and-forget：迁移提交失败（无 quorum）不崩溃，等下次重试或恢复。
-spec on_nodedown(node()) -> ok.
on_nodedown(DeadNode) ->
    _ = try
        View = broker_cluster:partition_view(),
        maps:foreach(
          fun(Partition, #{owner := Owner}) when Owner =:= DeadNode ->
                  case pick_new_owner(Partition, DeadNode) of
                      {ok, NewOwner, Epoch} ->
                          _ = broker_cluster:migrate(
                                Partition, DeadNode, NewOwner, Epoch),
                          ok;
                      _ ->
                          ok
                  end;
             (_Partition, _Meta) ->
                  ok
          end, View)
    catch
        _:_ -> ok
    end,
    ok.
