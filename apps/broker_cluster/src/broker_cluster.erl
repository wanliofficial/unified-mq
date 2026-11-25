%% @doc Cluster API and data-path guard (ADR-021 §D2~D5).
%%
%% Ownership/epoch is authoritative in the Metadata Raft Group; the data path
%% reads the local mirror (`broker_cluster_cache') and never pays a consensus
%% round per append. The guard is installed as a `persistent_term' hook so
%% `broker_queue' keeps no compile-time dependency on this application: with
%% `broker_cluster' not running, the queue behaves exactly as it did in S1~S3.
-module(broker_cluster).

-include_lib("broker_core/include/broker_message.hrl").

-export([start/0, stop/0]).
-export([assign/4, migrate/4, finish_migration/2, remove/1, claim/3,
         set_session_partition_count/1]).
%% MQ-15: shared assignment ra commands (the cross-node allocation truth).
-export([shared_claim/7, shared_ack/4, shared_release/3, shared_release_node/1,
         shared_reclaim/5, shared_join/3, shared_leave/2, shared_consumers_on/1]).
-export([join/1, join/2, leave/1, leave/2]).
-export([owner/1, epoch/1, epoch_or_zero/1, replicas/1, snapshot/0,
         partition_view/0]).
-export([authorize_append/2, guard_append/3, token/1]).
-export([replicate/2, install_hooks/0, uninstall_hooks/0]).
%% S12-D (ADR-034): installed as `persistent_term' hooks, never called by
%% adapters directly.
-export([route_op/3, partition_owner/1, reinstall_hooks/1, qos0_fanout/1]).

%%%_* Lifecycle -----------------------------------------------------------

start() ->
    {ok, _} = application:ensure_all_started(broker_cluster),
    ok.

stop() ->
    _ = application:stop(broker_cluster),
    ok.

%%%_* Metadata commands ---------------------------------------------------

assign(Partition, Owner, Replicas, Epoch)
  when is_integer(Partition), is_atom(Owner), is_list(Replicas) ->
    case cross_node_target_ok(Partition, Owner, Replicas) of
        ok -> command({assign, Partition, Owner, Replicas, Epoch});
        {error, _Reason} = Error -> Error
    end.

migrate(Partition, From, To, Epoch) ->
    %% ADR-034 D7: a migration is a metadata change only (ADR-032 D5), so a
    %% target that holds no replica would lose the partition's bytes. With
    %% cross-node delivery on, refuse it before it reaches the log.
    case cross_node_target_ok(Partition, To, replicas_or_empty(Partition)) of
        ok -> command({migrate, Partition, From, To, Epoch});
        {error, _Reason} = Error -> Error
    end.

finish_migration(Partition, Epoch) ->
    command({finish_migration, Partition, Epoch}).

remove(Partition) ->
    Epoch = case epoch(Partition) of
                {ok, E} -> E + 1;
                not_found -> 1
            end,
    command({remove, Partition, Epoch}).

%% @doc Plan 2 (spec §4): 持连副本节点认领会话分区所有权。经 ra 串行 apply，
%% 返回新 epoch。守卫（持副本 / epoch 单调）在 meta_machine 内，此处薄包装。
%%
%% 接管结果按**结果分类**计入冻结键（ADR-016 §16.2：禁止高基数标签，故一结果
%% 一键）。不在此 inc `cluster_owner_changes`——`command/1' 已对任何 `{ok, N}'
%% 成功回包计一次，重复计会双算。
-spec claim(non_neg_integer(), node(), non_neg_integer()) ->
          {ok, non_neg_integer()} | {error, term()}.
claim(Partition, Node, Epoch) when is_integer(Partition), is_atom(Node),
                                   is_integer(Epoch) ->
    Result = command({claim, Partition, Node, Epoch}),
    _ = try inc_takeover(Result) catch _:_ -> ok end,
    Result.

%%%_* MQ-15 shared assignment commands --------------------------------------
%%
%% Thin wrappers over `command/1': the mutual-exclusion invariant is enforced
%% inside the ra state machine (single-threaded apply), so these are safe to
%% call from any node. The worker passes `High' (the committed watermark) and
%% `Blocked' precomputed; the state machine never reads the queue.

%% @doc Claim up to `Max' offsets for `ClientId' on `{Sub, Partition}'. Returns
%% `{ok, [Offsets]}', `{ok, quota_hit}' (this consumer is at its own share while
%% the group still has room) or `{error, overflow}' (the whole window is full).
-spec shared_claim(term(), non_neg_integer(), binary(), non_neg_integer(),
                   non_neg_integer(), map(), non_neg_integer()) ->
          {ok, [offset()]} | {ok, quota_hit} | {error, term()}.
shared_claim(Sub, Partition, ClientId, Max, High, Blocked, PerConsumerMax)
  when is_integer(Partition) ->
    command({shared_claim, Sub, Partition, ClientId, Max, High, Blocked,
             PerConsumerMax}).

-spec shared_ack(term(), non_neg_integer(), offset(), binary()) ->
          {ok, non_neg_integer()} | {error, term()}.
shared_ack(Sub, Partition, Offset, ClientId) ->
    command({shared_ack, Sub, Partition, Offset, ClientId}).

-spec shared_release(term(), non_neg_integer(), binary()) -> {ok, non_neg_integer()} | {error, term()}.
shared_release(Sub, Partition, ClientId) ->
    command({shared_release, Sub, Partition, ClientId}).

%% @doc Crash-release: atomically return every offset held by any consumer hosted
%% on `Node' (across all subs/partitions) to the pool and drop that node's
%% consumer entries. The nodedown handler calls this for a confirmed-dead node
%% (MQ-15 §4.1(c)). One ra command => no half-released table.
-spec shared_release_node(node()) -> {ok, non_neg_integer()} | {error, term()}.
shared_release_node(Node) when is_atom(Node) ->
    command({shared_release_node, Node}).

%% @doc Threshold-triggered rebalance: move `Offsets' from `FromId' to `ToId',
%% but only where `FromId' still holds them.
-spec shared_reclaim(term(), non_neg_integer(), binary(), binary(), [offset()]) ->
          {ok, [offset()]} | {error, term()}.
shared_reclaim(Sub, Partition, FromId, ToId, Offsets) ->
    command({shared_reclaim, Sub, Partition, FromId, ToId, Offsets}).

-spec shared_join(term(), binary(), node()) -> {ok, node()} | {error, term()}.
shared_join(Sub, ClientId, Node) ->
    command({shared_join, Sub, ClientId, Node}).

-spec shared_leave(term(), binary()) -> {ok, removed} | {error, term()}.
shared_leave(Sub, ClientId) ->
    command({shared_leave, Sub, ClientId}).

%% @doc The ClientIds hosted on `Node' -- the nodedown handler uses this to
%% release exactly a crashed node's offsets.
-spec shared_consumers_on(node()) -> [binary()] | {error, term()}.
shared_consumers_on(Node) when is_atom(Node) ->
    command({shared_consumers_on, Node}).

%% 把 claim 结果映射到对应冻结键。错误形状以 meta_machine:claim/6 为准：
%% `{error, {not_a_replica, _}}' / `{error, {stale_epoch, _, _}}' /
%% `{error, not_found}'（spec §11 写作 `no_replica'，实现为 `not_a_replica'，
%% 见 plan2-notes 的偏差记录）。
%%
%% 末尾的 catch-all **有意不计任何键**（不新增冻结键）。它接的是传输层失败：
%% `{error, no_quorum}'、`{error, {metadata_unavailable, _}}'、`{error, noproc}'
%% 等，这些都不是「某一次接管」的结局：
%%   * `{error, no_quorum}' 已由 `cluster_no_quorum' 计数——它在
%%     `broker_cluster_meta:no_quorum/0' 里 inc，此处再计会双算；
%%   * 其余形状（`metadata_unavailable' / `noproc' / 应用未起）说明元数据库
%%     本身不可用，是退化状态而非接管特定事件；按结果计键只会把「集群挂了」
%%     伪装成「接管失败」，混淆真正的运维信号。
inc_takeover({ok, _}) ->
    broker_metrics:inc(cluster_session_takeover_ok);
inc_takeover({error, {stale_epoch, _, _}}) ->
    broker_metrics:inc(cluster_session_takeover_stale_epoch);
inc_takeover({error, {not_a_replica, _}}) ->
    broker_metrics:inc(cluster_session_takeover_no_replica);
inc_takeover({error, not_found}) ->
    broker_metrics:inc(cluster_session_takeover_not_found);
inc_takeover(_Other) ->
    ok.

%% @doc Plan 3 (gap #3): 提交权威会话分区数。写 ra，全集群一致。
-spec set_session_partition_count(pos_integer()) ->
          {ok, non_neg_integer()} | {error, term()}.
set_session_partition_count(Count) when is_integer(Count), Count > 0 ->
    Epoch = case broker_cluster_cache:session_partition_config() of
                {ok, _, E} -> E + 1;
                not_found -> 1
            end,
    command({set_session_partition_count, Count, Epoch}).

%%%_* Online membership change (ADR-033) ----------------------------------
%%
%% The admin-driven join/leave the roadmap's S12-D names. Thin delegations to
%% `broker_cluster_meta' so a caller (the 8090 management API, or an operator
%% with `rebar3 rpc') changes the membership without owning the ra call. See
%% `broker_cluster_meta:join/1' for the two-step sequence and the failure
%% semantics.

-spec join(node()) -> broker_cluster_meta:member_result().
join(Node) when is_atom(Node) ->
    broker_cluster_meta:join(Node).

-spec join(node(), timeout()) -> broker_cluster_meta:member_result().
join(Node, Timeout) when is_atom(Node) ->
    broker_cluster_meta:join(Node, Timeout).

-spec leave(node()) -> broker_cluster_meta:member_result().
leave(Node) when is_atom(Node) ->
    broker_cluster_meta:leave(Node).

-spec leave(node(), timeout()) -> broker_cluster_meta:member_result().
leave(Node, Timeout) when is_atom(Node) ->
    forget_committed_on_leave(Node, broker_cluster_meta:leave(Node, Timeout)).

command(Command) ->
    case broker_cluster_meta:command(Command) of
        {ok, Epoch} when is_integer(Epoch) ->
            _ = try broker_metrics:inc(cluster_owner_changes) catch _:_ -> ok end,
            {ok, Epoch};
        Other ->
            Other
    end.

%%%_* Reads ---------------------------------------------------------------

owner(Partition) -> broker_cluster_cache:owner(Partition).
epoch(Partition) -> broker_cluster_cache:epoch(Partition).
replicas(Partition) -> broker_cluster_cache:replicas(Partition).

epoch_or_zero(Partition) ->
    case epoch(Partition) of
        {ok, E} -> E;
        not_found -> 0
    end.

snapshot() -> broker_cluster_cache:all().

%% @doc The ownership view keyed by partition -- what a rebalancer needs, since
%% a move must name its partition. `snapshot/0' stays as-is for the callers that
%% only read the metas.
partition_view() -> broker_cluster_cache:partition_view().

%% @doc The token a producer should carry for this partition. The epoch is only
%% meaningful together with the owner, so both travel.
token(Partition) ->
    {node(), epoch_or_zero(Partition)}.

%%%_* Fencing -------------------------------------------------------------

%% @doc Accept only if this node is the owner *for that exact epoch*. The epoch
%% is supplied by the caller (carried in `proto_meta.cluster_epoch'), which is
%% what makes a stale writer detectable after a migration.
-spec authorize_append(non_neg_integer(), non_neg_integer()) ->
          ok | {error, term()}.
authorize_append(Partition, Epoch) ->
    case broker_cluster_cache:get(Partition) of
        not_found ->
            %% No metadata: single-node/default mode, nothing to fence.
            ok;
        {ok, #{owner := Owner, epoch := Current} = Meta} ->
            case Owner =:= node() andalso Current =:= Epoch of
                true ->
                    ok;
                false ->
                    _ = try broker_metrics:inc(cluster_fencing_rejected) catch _:_ -> ok end,
                    {error, {fenced, #{partition => Partition,
                                       expected_epoch => Current,
                                       actual_epoch => Epoch,
                                       owner => maps:get(owner, Meta)}}}
            end
    end.

%% @doc Queue hook. A message that carries an epoch is checked against it; one
%% that does not is checked for local ownership only, which still stops a node
%% that already observed a handover from writing.
-spec guard_append(non_neg_integer(), term(), term()) -> ok | {error, term()}.
guard_append(Partition, #broker_message{proto_meta = Meta}, _Mode) ->
    case maps:get(cluster_epoch, Meta, undefined) of
        undefined -> owner_check(Partition);
        Epoch when is_integer(Epoch) -> authorize_append(Partition, Epoch)
    end;
guard_append(Partition, _Msg, _Mode) ->
    owner_check(Partition).

owner_check(Partition) ->
    case broker_cluster_cache:get(Partition) of
        not_found ->
            ok;
        {ok, #{owner := Owner, epoch := E}} when Owner =:= node() ->
            case E of
                _ -> ok
            end;
        {ok, #{owner := Owner, epoch := E}} ->
            _ = try broker_metrics:inc(cluster_fencing_rejected) catch _:_ -> ok end,
            {error, {fenced, #{partition => Partition, expected_epoch => E,
                               owner => Owner}}}
    end.

%%%_* Replication ---------------------------------------------------------

replicate(Partition, Msg) ->
    broker_cluster_rep:replicate(Partition, Msg).

%%%_* Hooks ---------------------------------------------------------------

install_hooks() ->
    persistent_term:put({broker_queue, append_guard},
                        fun ?MODULE:guard_append/3),
    persistent_term:put({broker_queue, replicator},
                        fun ?MODULE:replicate/2),
    %% Optional third hook: lets the protocol adapter stamp the epoch it saw
    %% into `proto_meta' without depending on this application.
    persistent_term:put({broker_cluster, epoch}, fun ?MODULE:epoch/1),
    ok = install_cross_node_hooks(),
    ok = install_session_hook(),
    ok.

%% S12-D (ADR-034 D8): default `false'. When it is off none of the routing
%% hooks exist, so every `broker_queue' / `broker_subscription' path keeps its
%% single-node behaviour byte for byte.
install_cross_node_hooks() ->
    case application:get_env(broker_cluster, cross_node_delivery, false) of
        true ->
            persistent_term:put({broker_queue, router}, fun ?MODULE:route_op/3),
            persistent_term:put({broker_queue, forward_timeout_ms},
                                application:get_env(broker_cluster,
                                                   cross_node_timeout_ms, 2000)),
            persistent_term:put({broker_subscription, owner},
                                fun ?MODULE:partition_owner/1),
            persistent_term:put({broker_subscription, qos0_fanout},
                                fun ?MODULE:qos0_fanout/1),
            ok;
        _ ->
            ok
    end.

%% @doc spec §5 / I2: default `false'. Installs the {broker_subscription_worker,
%% session_replicator} persistent_term hook so the single-writer replicates
%% session ops to replicas. OFF by default -> no hook installed, broker
%% subscription paths are byte-for-byte single-node. The hook is fire-and-forget
%% (spawns) so it never blocks the conn_fsm / worker callers.
%%
%% PLAN 1 SYNC-CONTRACT DEGRADATION (semantic debt, do not mistake for a live
%% guarantee): the hook body installed here is
%% `broker_cluster_session_rep:replicate_session_of/1', which is
%% FIRE-AND-FORGET and ALWAYS returns `ok' (it spawns a detached best-effort
%% broadcast and returns immediately, without inspecting replica acks). As a
%% consequence the worker's SYNC majority-ack error branches are effectively
%% INERT in Plan 1:
%%   * `broker_subscription_worker:replicate_sync/1' returning `{error,_}' -- the
%%     SUBACK can never actually fail on account of an unreachable replica, and
%%   * `maybe_replicate_unsubscribe/2's' warning -- never fires, because no error
%%     result ever propagates back.
%% So subscribe/unsubscribe are NOT actually majority-durable yet: the op is
%% best-effort broadcast to every replica set in the metadata mirror, with
%% idempotent per-key overwrites on the receive side. Restoring a real
%% synchronous majority-ack result (so a SUBACK genuinely fails when a replica
%% can't be reached) is a PLAN 2 task that requires the session->partition
%% routing map to target the right replica set and surface the ack verdict to
%% the caller. This comment exists so future readers don't read the dead
%% `{error,_}' branches below the call sites as if they enforced durability.
install_session_hook() ->
    Result = case application:get_env(broker_cluster, session_replication, false) of
                 true ->
                     persistent_term:put(
                       {broker_subscription_worker, session_replicator},
                       fun(Op) -> broker_cluster_session_rep:replicate_session_of(Op) end),
                     ok;
                 _ ->
                     _ = persistent_term:erase({broker_subscription_worker, session_replicator}),
                     ok
             end,
    %% Plan 2: 开关切换后让 nodedown 监视进程重挂（无进程时安全 no-op）。
    _ = broker_cluster_nodedown_mon:rearm(),
    Result.

%% @doc ADR-034 D4: broadcast a QoS0 message to the other members. Each member
%% re-routes it against its own index (`broker_subscription:dispatch_qos0/1'), so
%% no node needs another node's subscriber list. `rpc:cast' is fire-and-forget:
%% an unreachable member is dropped silently, the publisher's path never blocks,
%% and nothing is stored or retried -- ADR-007 L1, exactly what QoS0 promises.
qos0_fanout(Msg) ->
    case cluster_members() of
        {ok, Members} ->
            lists:foreach(
              fun(Node) when Node =/= node() ->
                      _ = try
                          rpc:cast(Node, broker_subscription,
                          dispatch_qos0, [Msg])
                      catch
                          _:_ -> ok
                      end;
                 (_) ->
                      ok
              end, Members),
            ok;
        {error, _} ->
            ok
    end.

%% The committed membership is the truth (ADR-033). When the control process is
%% not running -- a single node, or a test that only set the env -- fall back to
%% the configured list, which defaults to `[node()]' (no fan-out).
cluster_members() ->
    try broker_cluster_meta:members() of
        Members when is_list(Members) -> {ok, Members};
        _ -> {error, unavailable}
    catch
        _:_ ->
            case application:get_env(broker_cluster, members, [node()]) of
                Ms when is_list(Ms) -> {ok, Ms};
                _ -> {error, unavailable}
            end
    end.

%% @private Test/ops helper (ADR-034 D8): tear every hook down and reinstall the
%% guard with the requested cross-node switch. `install_hooks/0' alone never
%% *erases* a previously installed router, so flipping the switch at runtime needs
%% this; the app does the same dance on start/stop.
reinstall_hooks(CrossNode) ->
    uninstall_hooks(),
    ok = application:set_env(broker_cluster, cross_node_delivery, CrossNode),
    install_hooks().

uninstall_hooks() ->
    _ = persistent_term:erase({broker_queue, append_guard}),
    _ = persistent_term:erase({broker_queue, replicator}),
    _ = persistent_term:erase({broker_queue, router}),
    _ = persistent_term:erase({broker_queue, forward_timeout_ms}),
    _ = persistent_term:erase({broker_subscription, owner}),
    _ = persistent_term:erase({broker_subscription, qos0_fanout}),
    _ = persistent_term:erase({broker_cluster, epoch}),
    _ = persistent_term:erase({broker_subscription_worker, session_replicator}),
    ok.

%%%_* Cross-node routing (ADR-034) -----------------------------------------

%% @doc Router hook consulted by `broker_queue' before every data-path call.
%% It reads the local mirror only (`broker_cluster_cache'), never ra, and it
%% runs in the caller's process -- the same split ADR-010 froze for `route/3'.
%%
%% `get('$broker_queue_forwarded')' is set by `broker_queue:forwarded/3': a
%% forwarded call must never be forwarded again, or two nodes whose mirrors
%% disagree would ping-pong. A stale owner answers `{not_owner, Owner}' instead
%% of serving a local log nobody should read.
route_op(Op, Partition, Args) ->
    case broker_cluster_cache:owner(Partition) of
        not_found ->
            local_or_floor(Op, Partition, Args);
        {ok, Owner} when Owner =:= node() ->
            local_or_floor(Op, Partition, Args);
        {ok, Owner} ->
            case get('$broker_queue_forwarded') of
                true ->
                    _ = try broker_metrics:inc(cluster_fencing_rejected) catch _:_ -> ok end,
                    {error, {not_owner, Owner}};
                _ ->
                    {forward, Owner}
            end
    end.

%% @doc Ownership view for `broker_subscription' (D5): a shared group is
%% refused on a node that does not own the partition, because `claim/4' hands
%% out the same offsets independently on every node.
partition_owner(Partition) ->
    broker_cluster_cache:owner(Partition).

local_or_floor(advance_committed, Partition, [Candidate]) ->
    Origin = case get('$broker_queue_origin') of
                 undefined -> node();
                 Node -> Node
             end,
    case broker_cluster_cache:report_committed(Partition, Origin, Candidate) of
        undefined -> local;
        Floor when is_integer(Floor) -> {local, Floor}
    end;
local_or_floor(_Op, _Partition, _Args) ->
    local.

%%%_* Cross-node guards ----------------------------------------------------

cross_node_enabled() ->
    application:get_env(broker_cluster, cross_node_delivery, false) =:= true.

%% ADR-034 D7: with no byte handoff, only a node that already holds a replica
%% can become owner without losing the partition's history.
cross_node_target_ok(Partition, Target, Replicas) ->
    case cross_node_enabled() andalso not lists:member(Target, Replicas) of
        true -> {error, {cross_node_delivery_requires_replica, Partition, Target}};
        false -> ok
    end.

replicas_or_empty(Partition) ->
    case replicas(Partition) of
        {ok, Replicas} -> Replicas;
        not_found -> []
    end.

forget_committed_on_leave(Node, Result) ->
    case Result of
        ok -> _ = broker_cluster_cache:forget_committed(Node), Result;
        {ok, not_member} -> _ = broker_cluster_cache:forget_committed(Node), Result;
        _ -> Result
    end.
