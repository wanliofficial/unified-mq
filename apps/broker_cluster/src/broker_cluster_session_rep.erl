%% @doc 会话状态复制发起侧（spec 2026-11-22 §3.2）。
%% 仿 broker_cluster_rep：查副本集 → 并行 rpc:call 到副本 → 多数派 ack。
%% 本地已在单一写入者落表/落盘，故只测副本往返。Op 协议见
%% broker_subscription_worker:session_op/0。
-module(broker_cluster_session_rep).

-export([replicate_session/2, replicate_session/3, remote_apply/1]).

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
