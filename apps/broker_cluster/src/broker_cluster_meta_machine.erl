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
                   session_partition_epoch => non_neg_integer()}.

-spec init(map()) -> state().
init(_Config) ->
    #{partitions => #{}, session_partition_count => undefined,
      session_partition_epoch => 0}.

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
