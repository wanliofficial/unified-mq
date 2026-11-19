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
         clear_partition/1, owner/1, epoch/1, replicas/1]).
%% ADR-034 D6: the committed floor is a minimum over every node's cursor. The
%% reports live here because this server already owns the local mirror, and the
%% table is *not* a second cursor truth: it only answers "how far may GC go".
-export([report_committed/3, forget_committed/1, committed_floor/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(TAB, broker_cluster_cache).
-define(COMMITTED, broker_cluster_committed).

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
    try [Meta || {_P, Meta} <- ets:tab2list(?TAB)]
    catch _:_ -> []
    end.

%% @doc The cache keyed by partition. `all/0' drops the key (its existing
%% callers only want the metas), but a rebalancer has to name the partition it
%% wants to move, so this keeps the pair.
partition_view() ->
    try maps:from_list(ets:tab2list(?TAB))
    catch _:_ -> #{}
    end.

clear() ->
    try ets:delete_all_objects(?TAB) catch _:_ -> true end,
    %% ADR-034 D6: a cleared mirror must not leave stale committed candidates
    %% pinning a partition nobody owns any more.
    try ets:delete_all_objects(?COMMITTED) catch _:_ -> true end,
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
