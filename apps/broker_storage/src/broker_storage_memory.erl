%% @doc In-memory `broker_storage' backend (design §7.1).
%%
%% Hot-path reads and appends run in the calling process against public ETS
%% tables, so this backend cannot become the single-process bottleneck README
%% §7.2 rules out; only the rarely used watermark/admin operations (create,
%% recover, close, advance_committed, truncate) go through the store process.
-module(broker_storage_memory).

-behaviour(broker_storage).

-include_lib("broker_core/include/broker_message.hrl").

-export([create/2, recover/2, close/1,
         append/3, read_from/3,
         advance_committed/2, truncate/2, capability/1]).

-record(hd, {id       :: partition_id(),
             store    :: pid(),
             meta_tab :: ets:tid(),
             log_tab  :: ets:tid()}).

%% ADR-011 §11.4: capability is instance level, but for this backend there is
%% exactly one honest answer. A config knob here would let a typo make memory
%% claim fsync, which is the silent downgrade §11.4 forbids.
-define(CAPABILITY, [os_buffer]).

create(PartitionId, Config) ->
    case broker_storage_memory_store:create(PartitionId, Config) of
        {ok, {Store, MetaTab, LogTab}} ->
            {ok, #hd{id = PartitionId, store = Store, meta_tab = MetaTab, log_tab = LogTab}};
        {error, Reason} ->
            {error, Reason}
    end.

recover(PartitionId, Config) ->
    case broker_storage_memory_store:recover(PartitionId, Config) of
        {ok, {Store, MetaTab, LogTab}, Committed, High} ->
            {ok, #hd{id = PartitionId, store = Store, meta_tab = MetaTab, log_tab = LogTab},
             Committed, High};
        {error, Reason} ->
            {error, Reason}
    end.

close(#hd{id = Id, store = Store}) ->
    broker_storage_memory_store:close(Store, Id).

capability(#hd{}) ->
    ?CAPABILITY.

append(#hd{} = Hd, Msgs, Mode) ->
    %% §11.4 runtime backstop: a sync_mode outside capability/1 is an error,
    %% never a quieter write that still returns an ok.
    case lists:member(Mode, capability(Hd)) of
        false -> {error, {unsupported_sync_mode, Mode}};
        true -> do_append(Hd, Msgs)
    end.

do_append(#hd{id = Id, meta_tab = MetaTab, log_tab = LogTab} = Hd, Msgs) ->
    case partition_state(Hd) of
        {ok, _Last, _Committed, _Trunc} ->
            N = length(Msgs),
            %% Atomic block allocation: concurrent appends get disjoint,
            %% contiguous offsets without a writer process.
            Last = ets:update_counter(MetaTab, Id, {3, N}),
            Offsets = lists:seq(Last - N + 1, Last),
            lists:foreach(fun({Off, Msg}) ->
                              ets:insert(LogTab, {{Id, Off}, Msg})
                          end,
                          lists:zip(Offsets, Msgs)),
            {ok, Offsets};
        {error, Reason} ->
            {error, Reason}
    end.

read_from(#hd{id = Id, log_tab = LogTab} = Hd, From, Limit) ->
    case partition_state(Hd) of
        {ok, _Last, _Committed, Trunc} ->
            %% §11.6: the truncate boundary is logical -- offsets below it are
            %% simply not served, the records themselves stay in the table.
            Start = erlang:max(From, Trunc),
            Page = page(LogTab, Id, Start, Limit, []),
            {ok, Page, next_offset(Start, Page)};
        {error, Reason} ->
            {error, Reason}
    end.

advance_committed(#hd{id = Id, store = Store}, Offset) ->
    broker_storage_memory_store:advance_committed(Store, Id, Offset).

truncate(#hd{id = Id, store = Store}, BeforeOffset) ->
    broker_storage_memory_store:truncate(Store, Id, BeforeOffset).

%%%_* Internal -----------------------------------------------------------

partition_state(#hd{meta_tab = MetaTab, id = Id}) ->
    case ets:lookup(MetaTab, Id) of
        [{Id, true, Last, Committed, Trunc}] -> {ok, Last, Committed, Trunc};
        [{Id, false, _, _, _}] -> {error, {closed, Id}};
        [] -> {error, {not_found, Id}}
    end.

%% Offsets in an append-only log are dense (nothing is ever removed in S1), so
%% a page is a run of exact key lookups and stops at the first miss.
page(_LogTab, _Id, _Off, 0, Acc) ->
    lists:reverse(Acc);
page(LogTab, Id, Off, N, Acc) ->
    case ets:lookup(LogTab, {Id, Off}) of
        [{_, Msg}] -> page(LogTab, Id, Off + 1, N - 1, [{Off, Msg} | Acc]);
        [] -> lists:reverse(Acc)
    end.

next_offset(Start, []) ->
    Start;
next_offset(_Start, Offsets) ->
    {Last, _} = lists:last(Offsets),
    Last + 1.
