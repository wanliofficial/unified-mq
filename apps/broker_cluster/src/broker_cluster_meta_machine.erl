%% @doc Metadata Raft state machine (ADR-021 §D2/§D3).
%%
%% The state is tiny on purpose: partition ownership, epoch, replica set and
%% migration state. Message bodies never enter it (ADR-002), so snapshots stay
%% small and a metadata replay is cheap.
%%
%% Epoch monotonicity is enforced here, at the state machine, not at the API:
%% a stale command that reaches the log after a newer one must be rejected even
%% if the caller raced. Every applied change is mirrored into the local cache,
%% which is what the data path reads.
-module(broker_cluster_meta_machine).

-behaviour(ra_machine).

-export([init/1, apply/3, snapshot_installed/5, overview/1]).

-type partition() :: non_neg_integer().
-type meta() :: #{owner := node(),
                  epoch := non_neg_integer(),
                  replicas := [node()],
                  state := active | migrating,
                  index := non_neg_integer()}.
-type state() :: #{partitions := #{partition() => meta()},
                   session_partition_count => pos_integer() | undefined,
                   session_partition_epoch => non_neg_integer(),
                   %% MQ-15: `%{{SubRef, PartitionId} => #{Offset => ClientId}}'. The
                   %% shared assignment table, the one fact that must be globally
                   %% consistent (two nodes claiming independently would duplicate).
                   shared_assignments := #{{sub_key(), partition()} => map()},
                   %% MQ-15: `%{{SubRef, ClientId} => Node}' -- which node hosts each
                   %% shared consumer, so a node crash can release exactly its offsets.
                   shared_consumers := #{{sub_key(), binary()} => node()}}.
-type sub_key() :: {shared, binary(), binary()}.

-spec init(map()) -> state().
init(_Config) ->
    #{partitions => #{}, session_partition_count => undefined,
      session_partition_epoch => 0,
      shared_assignments => #{}, shared_consumers => #{}}.

-spec apply(map(), term(), state()) ->
          {state(), term(), list()} | {state(), term()}.
apply(#{index := Index}, Command, #{partitions := Ps} = State) ->
    case Command of
        {assign, P, Owner, Replicas, Epoch} ->
            assign(P, Owner, Replicas, Epoch, active, Index, Ps, State);
        {migrate, P, _From, To, Epoch} ->
            assign(P, To, replicas_of(P, Ps), Epoch, migrating, Index, Ps, State);
        {finish_migration, P, Epoch} ->
            case maps:find(P, Ps) of
                {ok, #{epoch := E} = M} when E < Epoch ->
                    M1 = M#{state := active, epoch := Epoch, index := Index},
                    broker_cluster_cache:put(P, M1),
                    {State#{partitions := Ps#{P => M1}}, {ok, Epoch}, []};
                {ok, #{epoch := E}} ->
                    {State, {error, {stale_epoch, E, Epoch}}, []};
                error ->
                    {State, {error, not_found}, []}
            end;
        {remove, P, Epoch} ->
            case maps:find(P, Ps) of
                {ok, #{epoch := E}} when E < Epoch ->
                    broker_cluster_cache:clear_partition(P),
                    {State#{partitions := maps:remove(P, Ps)}, {ok, Epoch}, []};
                {ok, #{epoch := E}} ->
                    {State, {error, {stale_epoch, E, Epoch}}, []};
                error ->
                    {State, {error, not_found}, []}
            end;
        {claim, P, Node, Epoch} ->
            claim(P, Node, Epoch, Index, Ps, State);
        {set_session_partition_count, Count, Epoch} ->
            set_session_partition_count(Count, Epoch, Ps, State);
        {shared_claim, Sub, P, ClientId, Max, High, Blocked, PerConsumerMax} ->
            shared_claim(Sub, P, ClientId, Max, High, Blocked, PerConsumerMax, State);
        {shared_ack, Sub, P, Offset, ClientId} ->
            shared_ack(Sub, P, Offset, ClientId, State);
        {shared_release, Sub, P, ClientId} ->
            shared_release(Sub, P, ClientId, State);
        {shared_reclaim, Sub, P, FromId, ToId, Offsets} ->
            shared_reclaim(Sub, P, FromId, ToId, Offsets, State);
        {shared_join, Sub, ClientId, Node} ->
            shared_join(Sub, ClientId, Node, State);
        {shared_leave, Sub, ClientId} ->
            shared_leave(Sub, ClientId, State);
        {shared_consumers_on, Node} ->
            {State, shared_consumers_on(Node, State), []};
        {get, P} ->
            case maps:find(P, Ps) of
                {ok, M} -> {State, {ok, M}, []};
                error -> {State, not_found, []}
            end;
        snapshot ->
            {State, {ok, maps:values(Ps)}, []};
        Other ->
            {State, {error, {unknown_command, Other}}, []}
    end.

%% Rebuilding the cache after a snapshot install: apply/3 is not replayed for
%% entries covered by the snapshot, so without this a restarted node would keep
%% an empty cache until the next ownership change.
snapshot_installed(_Meta, _Snapshot, State, _Machine, _Module) ->
    #{partitions := Ps} = State,
    broker_cluster_cache:clear(),
    maps:foreach(fun(P, M) -> broker_cluster_cache:put(P, M) end, Ps),
    case maps:get(session_partition_count, State, undefined) of
        undefined -> ok;
        C -> broker_cluster_cache:put_session_partition_config(
               C, maps:get(session_partition_epoch, State, 0))
    end,
    %% MQ-15: restore the shared mirrors too, or a restarted node would serve an
    %% empty assignment cache until the next claim.
    _ = broker_cluster_cache:put_shared_assignments(
          maps:get(shared_assignments, State, #{})),
    _ = broker_cluster_cache:put_shared_consumers(
          maps:get(shared_consumers, State, #{})),
    ok.

overview(#{partitions := Ps}) ->
    #{partitions => maps:size(Ps)}.

%%%_* Internal ------------------------------------------------------------

assign(P, Owner, Replicas, Epoch, Phase, Index, Ps, State) ->
    case maps:find(P, Ps) of
        {ok, #{epoch := E}} when E >= Epoch ->
            {State, {error, {stale_epoch, E, Epoch}}, []};
        _ when not is_list(Replicas); Replicas =:= [] ->
            {State, {error, {bad_replicas, Replicas}}, []};
        _ ->
            M = #{owner => Owner, epoch => Epoch, replicas => Replicas,
                  state => Phase, index => Index},
            broker_cluster_cache:put(P, M),
            {State#{partitions := Ps#{P => M}}, {ok, Epoch}, []}
    end.

%% Plan 2 (spec §4): 持连副本节点认领分区所有权。两条守卫：
%%   1. epoch 单调——与 assign 的 epoch 单调守卫同一谓词（assign 写成
%%      `E >= Epoch` 的拒绝形式），防乱序/陈旧 claim；并要求 Epoch 为整数，
%%      否则非整数 epoch 会让分区永久卡死（`1 < bogus` 为真、`bogus < N` 为假）；
%%   2. Node 必须已在 replicas 集中（ADR-034 D7 在会话分区落地）——迁移目标必须
%%      已持副本，否则拒绝。ra 串行 apply 保证全局唯一 owner。
claim(P, Node, Epoch, Index, Ps, State) ->
    case maps:find(P, Ps) of
        {ok, #{epoch := E} = M} when is_integer(Epoch), E < Epoch ->
            case lists:member(Node, maps:get(replicas, M, [])) of
                true ->
                    M1 = M#{owner => Node, epoch => Epoch, state => active,
                            index => Index},
                    broker_cluster_cache:put(P, M1),
                    {State#{partitions := Ps#{P => M1}}, {ok, Epoch}, []};
                false ->
                    {State, {error, {not_a_replica, Node}}, []}
            end;
        {ok, #{epoch := E}} ->
            {State, {error, {stale_epoch, E, Epoch}}, []};
        error ->
            {State, {error, not_found}, []}
    end.

%% Plan 3 (gap #3, design §3.1): 会话分区数的权威值。epoch 单调守卫与 claim
%% 同构：拒绝陈旧写；count 必须是正整数，否则分区空间非法。
%% 更新用 `=>`（可选键）以容忍旧快照缺字段。
set_session_partition_count(Count, Epoch, _Ps, State) ->
    Cur = maps:get(session_partition_epoch, State, 0),
    if
        not is_integer(Count); Count =< 0 ->
            {State, {error, {bad_count, Count}}, []};
        not is_integer(Epoch); Epoch =< Cur ->
            {State, {error, {stale_epoch, Cur, Epoch}}, []};
        true ->
            broker_cluster_cache:put_session_partition_config(Count, Epoch),
            {State#{session_partition_count => Count,
                    session_partition_epoch => Epoch}, {ok, Epoch}, []}
    end.

replicas_of(P, Ps) ->
    case maps:find(P, Ps) of
        {ok, #{replicas := R}} -> R;
        error -> [node()]
    end.

%%%_* MQ-15 shared assignment commands ---------------------------------------
%%
%% The shared assignment table is a `%{{SubRef, P} => #{Offset => ClientId}}'
%% map. Unlike partition ownership it carries no epoch: these are monotone set
%% operations (claim adds, ack/release remove) whose convergence does not depend
%% on ordering, and the mutual-exclusion invariant (one offset -> one consumer)
%% is guaranteed by ra's single-threaded apply, not by fencing. The pure set
%% algebra is `broker_cursor', reused verbatim from the worker's local path.

shared_claim(Sub, P, ClientId, Max, High, Blocked, PerConsumerMax, State) ->
    As = maps:get(shared_assignments, State, #{}),
    Key = {Sub, P},
    %% No stored row yet: this is the first claim for this {Sub,P}. Seed an
    %% empty window at low=1 (`broker_cursor' recomputes `low' from the acked
    %% prefix on every ack, so it is never stored -- design §3.2). `claim/7'
    %% scans from `low' up to the `High' watermark, so it never hands out an
    %% offset past the log tail. `High' bounds the scan; it does not seed low.
    Row = maps:get(Key, As, broker_cursor:new_shared(1)),
    Window = application:get_env(broker_subscription, shared_state_max, 4096),
    case broker_cursor:claim(Row, Max, High, ClientId, Window, Blocked, PerConsumerMax) of
        {ok, [], _} ->
            {State, {ok, []}, []};
        {ok, quota_hit, _} ->
            {State, {ok, quota_hit}, []};
        {ok, Offsets, Row1} ->
            As1 = As#{Key => Row1},
            broker_cluster_cache:put_shared_assignments(As1),
            {State#{shared_assignments => As1}, {ok, Offsets}, []};
        {error, overflow} ->
            {State, {error, overflow}, []}
    end.

shared_ack(Sub, P, Offset, ClientId, State) ->
    As = maps:get(shared_assignments, State, #{}),
    Key = {Sub, P},
    case maps:find(Key, As) of
        error ->
            {State, {error, not_found}, []};
        {ok, Row} ->
            case broker_cursor:ack_shared(Row, Offset, ClientId) of
                unowned ->
                    {State, {error, unowned}, []};
                {ok, Row1} ->
                    As1 = As#{Key => Row1},
                    broker_cluster_cache:put_shared_assignments(As1),
                    {State#{shared_assignments => As1},
                     {ok, broker_cursor:next_read(Row1)}, []}
            end
    end.

shared_release(Sub, P, ClientId, State) ->
    As = maps:get(shared_assignments, State, #{}),
    Key = {Sub, P},
    case maps:find(Key, As) of
        error ->
            {State, {ok, 0}, []};
        {ok, Row} ->
            Row1 = broker_cursor:release(Row, ClientId),
            Released = count_held(Row, ClientId),
            As1 = As#{Key => Row1},
            broker_cluster_cache:put_shared_assignments(As1),
            {State#{shared_assignments => As1}, {ok, Released}, []}
    end.

shared_reclaim(Sub, P, FromId, ToId, Offsets, State) ->
    As = maps:get(shared_assignments, State, #{}),
    Key = {Sub, P},
    case maps:find(Key, As) of
        error ->
            {State, {error, not_found}, []};
        {ok, Row} ->
            Row1 = reclaim_offsets(Row, FromId, ToId, Offsets),
            As1 = As#{Key => Row1},
            broker_cluster_cache:put_shared_assignments(As1),
            {State#{shared_assignments => As1}, {ok, Offsets}, []}
    end.

shared_join(Sub, ClientId, Node, State) ->
    Cs = maps:get(shared_consumers, State, #{}),
    Cs1 = Cs#{{Sub, ClientId} => Node},
    broker_cluster_cache:put_shared_consumers(Cs1),
    {State#{shared_consumers => Cs1}, {ok, Node}, []}.

shared_leave(Sub, ClientId, State) ->
    Cs = maps:get(shared_consumers, State, #{}),
    Cs1 = maps:remove({Sub, ClientId}, Cs),
    broker_cluster_cache:put_shared_consumers(Cs1),
    {State#{shared_consumers => Cs1}, {ok, removed}, []}.

shared_consumers_on(Node, State) ->
    Cs = maps:get(shared_consumers, State, #{}),
    [ClientId || {{_Sub, ClientId}, N} <- maps:to_list(Cs), N =:= Node].

%% Move `Offsets' from FromId to ToId, but only where FromId currently holds them.
reclaim_offsets(Row, FromId, ToId, Offsets) ->
    Assigned = maps:get(assigned, Row, #{}),
    Acked = maps:get(acked, Row, #{}),
    Retries = maps:get(retries, Row, #{}),
    Assigned1 = lists:foldl(
                   fun(O, Acc) ->
                           case maps:get(O, Acc, undefined) of
                               FromId -> Acc#{O => ToId};
                               _ -> Acc
                           end
                   end, Assigned, Offsets),
    Row#{assigned := Assigned1, acked := Acked, retries := Retries}.

count_held(Row, ClientId) ->
    Assigned = maps:get(assigned, Row, #{}),
    length([1 || {_O, C} <- maps:to_list(Assigned), C =:= ClientId]).
