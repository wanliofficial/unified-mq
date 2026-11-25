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
%%
%% MQ-15 §4.1(c)：崩溃回池与分区迁移正交。若 `broker_subscription:
%% cluster_shared_rebalance' 开，追加把崩溃节点 O 的全部 shared consumer
%% 占用的 offset 原子回池（`shared_release_node(O)'），存活消费者据此重新 claim，
%% 不丢、不卡 `low'。复用现有去抖（见 broker_cluster_nodedown_mon），仅在
%% 同一 handler 里追加这一步。
-spec on_nodedown(node()) -> ok.
on_nodedown(DeadNode) ->
    _ = try broker_cluster:partition_view() of
            View when is_map(View) ->
                maps:foreach(
                  fun(Partition, Meta) ->
                          _ = try
                                  case Meta of
                                      #{owner := Owner} when Owner =:= DeadNode ->
                                          case pick_new_owner(Partition, DeadNode) of
                                              {ok, NewOwner, Epoch} ->
                                                  _ = broker_cluster:migrate(
                                                        Partition, DeadNode,
                                                        NewOwner, Epoch),
                                                  ok;
                                              _ -> ok
                                          end;
                                      _ -> ok
                                  end
                              catch _:_ -> ok end,
                          ok
                  end, View)
        catch _:_ -> ok end,
    _ = release_shared_consumers(DeadNode),
    ok.

%% 崩溃回池（MQ-15 §4.1(c)）：仅 cluster_shared_rebalance 开时生效。`shared_release_node'
%% 经 ra 原子地把崩溃节点上全部 shared consumer 的 offset 回池。ra 缺失 / 无 quorum 时
%% `command' 返回 {error,_}，吞掉——回池只是优化，等节点恢复或下次 nodedown 仍会补。
release_shared_consumers(DeadNode) ->
    case application:get_env(broker_subscription, cluster_shared_rebalance, false) of
        true ->
            _ = try broker_cluster:shared_release_node(DeadNode)
                catch _:_ -> ok end,
            ok;
        _ ->
            ok
    end.
