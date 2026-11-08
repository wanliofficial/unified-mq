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

-export([start_link/0, put/2, get/1, all/0, clear/0, clear_partition/1,
         owner/1, epoch/1, replicas/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(TAB, broker_cluster_cache).

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

clear() ->
    try ets:delete_all_objects(?TAB) catch _:_ -> true end,
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

%%%_* gen_server ----------------------------------------------------------

init([]) ->
    case ets:info(?TAB, name) of
        undefined ->
            _ = ets:new(?TAB, [named_table, public, set,
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
