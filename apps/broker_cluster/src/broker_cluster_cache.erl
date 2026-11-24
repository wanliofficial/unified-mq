%% @doc Local cache of the Metadata Raft state (ADR-021 §D3).
%%
%% `ra` applies each committed command on every member; the state machine
%% writes the result here, so a node sees an ownership change as soon as the
%% entry is applied locally. Reads never go to ra: an append path must not pay
%% a consensus round (ADR-008 §8.4).
%%
%% The table is owned by this gen_server, not by whichever ra process happened
%% to apply first, so a ra restart cannot take the cache with it.
-module(broker_cluster_cache).

%% `get/1' is our cache reader, not the process-dictionary BIF.
-compile({no_auto_import, [get/1]}).

-behaviour(gen_server).

-export([start_link/0, put/2, get/1, all/0, partition_view/0, clear/0,
         clear_partition/1, owner/1, epoch/1, replicas/1,
         put_session_partition_config/2, session_partition_config/0,
         %% MQ-15: shared assignment mirrors.
         put_shared_assignments/1, shared_assignment/2,
         put_shared_consumers/1, shared_consumers_on/1]).
%% ADR-034 D6: the committed floor is a minimum over every node's cursor. The
%% reports live here because this server already owns the local mirror, and the
%% table is *not* a second cursor truth: it only answers "how far may GC go".
-export([report_committed/3, forget_committed/1, committed_floor/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(TAB, broker_cluster_cache).
-define(COMMITTED, broker_cluster_committed).

%% 集群级会话分区配置的保留键：一个不可能与 non_neg_integer() 分区键相同的原子。
%% 单独一行存，使 `all/0' / `partition_view/0' 可以显式过滤，绝不把它当分区。
%% 行形状是三元组 {Key, Count, Epoch}，与分区行 {P, Meta} 的 arity 不同。
-define(SESSION_CFG, '$session_partition_config').

%% MQ-15: 同样用「不可能与整数分区键 / ?SESSION_CFG 冲突」的保留键存 shared 两表。
-define(SHARED_ASG, '$shared_assignments').
-define(SHARED_CON, '$shared_consumers').

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Tolerant by design: the state machine may apply before this server has
%% created the table during boot, and a missing cache must degrade to
%% "no metadata" rather than crash the ra apply loop.
put(Partition, Meta) when is_map(Meta) ->
    case ets:info(?TAB, name) of
        undefined -> ok;
        _ -> true = ets:insert(?TAB, {Partition, Meta}), ok
    end.

get(Partition) ->
    try ets:lookup(?TAB, Partition) of
        [{Partition, Meta}] -> {ok, Meta};
        [] -> not_found
    catch
        _:_ -> not_found
    end.

all() ->
    try [Meta || {P, Meta} <- ets:tab2list(?TAB), P =/= ?SESSION_CFG]
    catch _:_ -> []
    end.

%% @doc The cache keyed by partition. `all/0' drops the key (its existing
%% callers only want the metas), but a rebalancer has to name the partition it
%% wants to move, so this keeps the pair.
partition_view() ->
    try maps:from_list([{P, Meta} || {P, Meta} <- ets:tab2list(?TAB),
                                     P =/= ?SESSION_CFG])
    catch _:_ -> #{}
    end.

%% @doc 集群级会话分区配置（Plan 3 gap #3）：权威 C 与它的 epoch。
%% 与分区行同表不同键，单独读取，不进 partition_view。
-spec put_session_partition_config(pos_integer(), non_neg_integer()) -> ok.
put_session_partition_config(Count, Epoch)
  when is_integer(Count), Count > 0, is_integer(Epoch), Epoch >= 0 ->
    %% 权威 C 落地：清掉 session_owner 的推导 memo，避免它继续盖住新值。
    _ = persistent_term:erase({broker_cluster_session_owner, derived_count}),
    case ets:info(?TAB, name) of
        undefined -> ok;
        _ -> true = ets:insert(?TAB, {?SESSION_CFG, Count, Epoch}), ok
    end.

-spec session_partition_config() -> {ok, pos_integer(), non_neg_integer()} | not_found.
session_partition_config() ->
    try ets:lookup(?TAB, ?SESSION_CFG) of
        [{?SESSION_CFG, Count, Epoch}] -> {ok, Count, Epoch};
        [] -> not_found
    catch
        _:_ -> not_found
    end.

%% @doc MQ-15: replace the whole shared-assignment mirror. Called by the ra
%% state machine on every apply and by `snapshot_installed/5'.
-spec put_shared_assignments(map()) -> ok.
put_shared_assignments(As) when is_map(As) ->
    case ets:info(?TAB, name) of
        undefined -> ok;
        _ -> true = ets:insert(?TAB, {?SHARED_ASG, As}), ok
    end.

%% @doc The ra-side assignment row for `{SubRef, PartitionId}', or `not_found'.
-spec shared_assignment(term(), non_neg_integer()) -> {ok, map()} | not_found.
shared_assignment(Sub, P) ->
    try ets:lookup(?TAB, ?SHARED_ASG) of
        [{?SHARED_ASG, As}] ->
            case maps:find({Sub, P}, As) of
                {ok, Row} -> {ok, Row};
                error -> not_found
            end;
        [] -> not_found
    catch
        _:_ -> not_found
    end.

-spec put_shared_consumers(map()) -> ok.
put_shared_consumers(Cs) when is_map(Cs) ->
    case ets:info(?TAB, name) of
        undefined -> ok;
        _ -> true = ets:insert(?TAB, {?SHARED_CON, Cs}), ok
    end.

%% @doc The ClientIds hosted on `Node', for crash-release (nodedown handler).
-spec shared_consumers_on(node()) -> [binary()].
shared_consumers_on(Node) ->
    try ets:lookup(?TAB, ?SHARED_CON) of
        [{?SHARED_CON, Cs}] ->
            [ClientId || {{_Sub, ClientId}, N} <- maps:to_list(Cs), N =:= Node];
        [] -> []
    catch
        _:_ -> []
    end.

clear() ->
    try ets:delete_all_objects(?TAB) catch _:_ -> true end,
    %% ADR-034 D6: a cleared mirror must not leave stale committed candidates
    %% pinning a partition nobody owns any more.
    try ets:delete_all_objects(?COMMITTED) catch _:_ -> true end,
    %% 镜像全清也意味着权威 C 不再可信：一并清掉推导 memo。
    _ = persistent_term:erase({broker_cluster_session_owner, derived_count}),
    ok.

clear_partition(Partition) ->
    try ets:delete(?TAB, Partition) catch _:_ -> true end,
    ok.

owner(Partition) ->
    case get(Partition) of
        {ok, #{owner := Owner}} -> {ok, Owner};
        not_found -> not_found
    end.

epoch(Partition) ->
    case get(Partition) of
        {ok, #{epoch := Epoch}} -> {ok, Epoch};
        not_found -> not_found
    end.

replicas(Partition) ->
    case get(Partition) of
        {ok, #{replicas := Replicas}} -> {ok, Replicas};
        not_found -> not_found
    end.

%% @doc Record one node's candidate and answer the floor this owner may apply
%% (ADR-034 D6). The floor is `min' over every node that has reported for this
%% partition, which is exactly ADR-014's "never delete past `committed'"
%% applied across nodes: one slow consumer anywhere pins the owner's GC.
%%
%% A node that leaves (`forget_committed/1') drops out; a node that is merely
%% unreachable keeps its last value, which can only be conservative (it may pin
%% the floor, it can never raise it). A missing table means the cache is not up,
%% and the caller then falls back to its own candidate -- the single-node rule.
report_committed(Partition, Node, Candidate) when is_integer(Candidate) ->
    try
        true = ets:insert(?COMMITTED, {{Partition, Node}, Candidate}),
        committed_floor(Partition)
    catch
        _:_ -> undefined
    end.

%% @doc ADR-033 `leave' hook: the departed node no longer holds a cursor here.
forget_committed(Node) ->
    try ets:match_delete(?COMMITTED, {{'_', Node}, '_'}), ok
    catch _:_ -> ok
    end.

committed_floor(Partition) ->
    case lists:append(ets:match(?COMMITTED, {{Partition, '_'}, '$1'})) of
        [] -> undefined;
        Candidates -> lists:min(Candidates)
    end.

%%%_* gen_server ----------------------------------------------------------

init([]) ->
    case ets:info(?TAB, name) of
        undefined ->
            _ = ets:new(?TAB, [named_table, public, set,
                               {read_concurrency, true}]);
        _ ->
            ok
    end,
    %% ADR-034 D6: written by whichever process reports a candidate (usually a
    %% subscription worker), read by the owner's routing hook. `protected' would
    %% force every reporter through this server for no benefit -- the rows are
    %% independent keys and a lost update is re-reported on the next tick.
    case ets:info(?COMMITTED, name) of
        undefined ->
            _ = ets:new(?COMMITTED, [named_table, public, set,
                                     {read_concurrency, true}]);
        _ ->
            ok
    end,
    {ok, #{}}.

handle_call(_Req, _From, St) -> {reply, {error, unsupported}, St}.
handle_cast(_Msg, St) -> {noreply, St}.
handle_info(_Info, St) -> {noreply, St}.
terminate(_Reason, _St) -> ok.
code_change(_OldVsn, St, _Extra) -> {ok, St}.
