%% @doc Single writer for the partition registry.
%%
%% The table is protected, which means only this process can mutate it and every
%% other process can still read it. That is the same split ADR-010【F3】 froze
%% for the router index: `broker_queue:append/3' looks the handle up in its own
%% process, and no caller can install a handle behind the registry's back -- a
%% public table would let any process point a partition at somebody else's log.
%%
%% Writes are rare (boot, and a re-attach after a storage crash), so paying a
%% gen_server call for them costs nothing on the data path.
-module(broker_queue_store).

-behaviour(gen_server).

-include_lib("broker_core/include/broker_message.hrl").

-export([start_link/0, put/2, lookup/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(TABLE, broker_queue_registry).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec put(partition_id(), broker_queue:entry()) -> ok.
put(PartitionId, Entry) ->
    gen_server:call(?MODULE, {put, PartitionId, Entry}).

%% Read in the caller's process: no call, no serialization point.
-spec lookup(partition_id()) -> {ok, broker_queue:entry()} | {error, term()}.
lookup(PartitionId) ->
    case ets:lookup(?TABLE, PartitionId) of
        [{PartitionId, Entry}] -> {ok, Entry};
        [] -> {error, {unknown_partition, PartitionId}}
    end.

%%%_* gen_server ---------------------------------------------------------

init([]) ->
    ?TABLE = ets:new(?TABLE, [named_table, set, protected, {read_concurrency, true}]),
    {ok, #{}}.

handle_call({put, PartitionId, Entry}, _From, State) ->
    true = ets:insert(?TABLE, {PartitionId, Entry}),
    {reply, ok, State}.

handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    %% The table dies with this process. Partition workers hold no state that
    %% outlives it, so there is nothing to flush: everything durable is already
    %% in broker_storage.
    ok.
