%% @doc 会话状态复制发起侧（spec 2026-11-22 §3.2）。
%% 仿 broker_cluster_rep：查副本集 → 并行 rpc:call 到副本 → 多数派 ack。
%% 本地已在单一写入者落表/落盘，故只测副本往返。Op 协议见
%% broker_subscription_worker:session_op/0。
-module(broker_cluster_session_rep).

-export([replicate_session/2, replicate_session/3, remote_apply/1,
         replicate_session_of/1, replicate_session_targeted/1,
         targeted_plan/1]). %% targeted_plan/1 exported for the routing unit test

-define(MAX_ACKS, 64).

-spec replicate_session(non_neg_integer(), broker_subscription_worker:session_op()) ->
          ok | {error, term()}.
replicate_session(Partition, Op) ->
    case broker_cluster:replicas(Partition) of
        {ok, Replicas} -> replicate_session(Partition, Op, Replicas);
        not_found -> ok
    end.

-spec replicate_session(non_neg_integer(), term(), [node()]) -> ok | {error, term()}.
replicate_session(_Partition, _Op, []) -> ok;
replicate_session(_Partition, _Op, [Node]) when Node =:= node() -> ok;
replicate_session(_Partition, Op, Replicas) ->
    Needed = (length(Replicas) div 2) + 1,
    Others = [N || N <- Replicas, N =/= node()],
    Timeout = application:get_env(broker_cluster, replicate_timeout_ms, 2000),
    Started = erlang:monotonic_time(microsecond),
    Acks = 1 + parallel_apply(Op, Others, Timeout),
    _ = try
        broker_metrics:observe(
        cluster_session_replication_latency_us,
        erlang:monotonic_time(microsecond) - Started)
    catch
        _:_ -> ok
    end,
    case Acks >= Needed of
        true -> ok;
        false ->
            _ = try broker_metrics:inc(cluster_session_replication_failed)
                  catch _:_ -> ok end,
            {error, {replication_failed, Acks, Needed}}
    end.

%% @doc 副本节点入口：把 Op 应用到本地会话表（仿 broker_queue:append_replica 契约）。
remote_apply(Op) ->
    try broker_subscription_worker:apply_replica(Op) of
        ok -> ok
    catch
        _:_ -> {error, apply_replica_crashed}
    end.

%% @doc Hook body (spec §3.5). FIRE-AND-FORGET: spawn so the calling process
%% (conn_fsm on subscribe/unsubscribe, or a detached worker process on cursor)
%% never blocks on a cross-node replica round-trip. Plan 1 has no
%% session->partition map, so this best-effort broadcasts to every replica set
%% in the metadata snapshot; receivers dedupe by idempotent per-key overwrites
%% (apply_replica is set-table). Precise routing is Plan 2.
-spec replicate_session_of(term()) -> ok.
replicate_session_of(Op) ->
    _ = try spawn(fun() -> replicate_session_best_effort(Op) end)
    catch _:_ -> ok end,
    ok.

%% @private Plan 2 (spec §5 / 债#1): 按会话分区路由，不再广播所有副本集。用
%% `broker_cluster_session_owner:partition_of/2' 把 Op 的 ClientId 散列到会话
%% 分区 P，再只复制到 P 在元数据镜像里的副本集。无元数据（空镜像）是单机默认，
%% 安全 no-op。非会话类 Op（cursor 无 ClientId）退回广播，保持 Plan 1 行为。
-spec replicate_session_best_effort(term()) -> ok.
replicate_session_best_effort(Op) ->
    _ = try replicate_session_targeted(Op) catch _:_ -> ok end,
    ok.

%% @doc Pure routing decision (横切测试点): 给定一个 Op，若其会话分区有副本集，
%% 返回 {targeted, Partition, Replicas}；否则（cursor / 未知 Op / 无元数据 /
%% 散列出错）返回 broadcast。所有失败都在本函数内捕获并降级为 broadcast，绝不
%% 外抛——调用方是 fire-and-forget 的 spawnee（见 replicate_session_of/1）。
%%
%% 路由决策（Plan 3 缺口 #5 已定稿）：会话分区 P 按与元数据分区 P **同键**读取
%% 副本集——会话副本集复用 ra 分区表键 P 的 `replicas`。会话分区与 `broker_rebalance`
%% 的队列/路由动态分区是**两套号段**，共用同一张元数据表：会话分区占稠密号段
%% `0..C-1`（`broker_cluster_session_bootstrap` 保证建齐），rebalance 管另一号段；
%% 同号段内不重叠（会话 P 恒 `< C`，rebalance 分区号由别处分配）。故
%% `broker_cluster:replicas(P)` 命中的必是会话分区 P 的副本集。两套空间仍各自独立
%% 演化（会话分区固定稠密、rebalance 动态稀疏），只是共享物理表 + 靠号段隔离。
%% 设计定稿见 2026-11-22-session-state-replication-plan2-design.md §5 / §12。
-spec targeted_plan(term()) -> {targeted, non_neg_integer(), [node()]} | broadcast.
targeted_plan({cursor, _SubRef, _PartitionId, _Row}) ->
    broadcast;
targeted_plan(Op) ->
    try
        ClientId = op_client_id(Op),
        Count = broker_cluster_session_owner:session_partition_count(),
        P = broker_cluster_session_owner:partition_of(ClientId, Count),
        case broker_cluster:replicas(P) of
            {ok, Rs} when is_list(Rs), Rs =/= [] -> {targeted, P, Rs};
            _ -> broadcast
        end
    catch
        _:_ -> broadcast
    end.

-spec replicate_session_targeted(term()) -> ok.
replicate_session_targeted(Op) ->
    case targeted_plan(Op) of
        {targeted, P, Rs} ->
            _ = replicate_session(P, Op, Rs),
            ok;
        broadcast ->
            replicate_session_broadcast_all(Op)
    end.

%% Plan-1 fallback: 广播到镜像里所有有副本集的分区。
replicate_session_broadcast_all(Op) ->
    try broker_cluster:partition_view() of
        View when is_map(View) ->
            maps:foreach(
              fun(Partition, Meta) ->
                      case Meta of
                          #{replicas := Rs} when is_list(Rs) ->
                              _ = replicate_session(Partition, Op, Rs),
                              ok;
                          _ -> ok
                      end
              end, View)
    catch
        _:_ -> ok
    end,
    ok.

%% 从会话 Op 取 ClientId（cursor 无，已在上层分流）。
op_client_id({subscribe, ClientId, _SubRef, _Info}) -> ClientId;
op_client_id({unsubscribe, ClientId, _SubRef}) -> ClientId;
op_client_id({inflight, ClientId, _Term}) -> ClientId;
op_client_id(Op) -> Op.

%%%_* Internal ------------------------------------------------------------

parallel_apply(Op, Nodes, Timeout) ->
    %% Mailbox-safety contract: if `collect/2' returns on its `after' deadline while
    %% a spawn is still in flight, that spawn may later deliver `{sess_repl_result, ..}'
    %% to the caller's mailbox unconsumed. This is inherited verbatim from
    %% `broker_cluster_rep' (identical pattern). Callers wiring this in (Task 4/5)
    %% MUST ensure the calling process is not a long-lived, mailbox-sensitive
    %% gen_server, or must otherwise drain/absorb those stray messages.
    Parent = self(),
    _ = [spawn(fun() ->
                       Result = rpc:call(N, ?MODULE, remote_apply, [Op], Timeout),
                       Parent ! {sess_repl_result, self(), Result}
               end) || N <- lists:sublist(Nodes, ?MAX_ACKS)],
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    collect(length(Nodes), Deadline).

collect(0, _Deadline) -> 0;
collect(N, Deadline) ->
    Remaining = max(0, Deadline - erlang:monotonic_time(millisecond)),
    receive
        {sess_repl_result, _Pid, ok} -> 1 + collect(N - 1, Deadline);
        {sess_repl_result, _Pid, _Error} -> collect(N - 1, Deadline)
    after Remaining -> 0 end.
