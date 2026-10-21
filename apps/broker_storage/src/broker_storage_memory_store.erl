%% @doc Owner of the in-memory backend's tables and the single writer for the
%% partition registry.
%%
%% Only registry/watermark operations are serialized here. Appends and reads are
%% served by `broker_storage_memory' directly in the caller process, which is why
%% the tables are public.
-module(broker_storage_memory_store).

-behaviour(gen_server).

-include_lib("broker_core/include/broker_message.hrl").

-export([start_link/0, stop/0]).

%% Called by broker_storage_memory.
-export([create/2, recover/2, close/2, advance_committed/3, truncate/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, broker_storage_memory_store).

-record(st, {meta :: ets:tid(), msgs :: ets:tid()}).

%% One store per node in S1. It is registered so that create/2 and recover/2,
%% which do not have a handle yet, can find it.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

stop() ->
    gen_server:stop(?SERVER, normal, 1000).

%%%_* Backend API --------------------------------------------------------

create(PartitionId, Config) ->
    call(Config, {create, PartitionId}).

recover(PartitionId, Config) ->
    call(Config, {recover, PartitionId}).

close(Store, PartitionId) ->
    gen_server:call(Store, {close, PartitionId}).

advance_committed(Store, PartitionId, Offset) ->
    gen_server:call(Store, {advance_committed, PartitionId, Offset}).

truncate(Store, PartitionId, BeforeOffset) ->
    gen_server:call(Store, {truncate, PartitionId, BeforeOffset}).

call(Config, Request) when is_map(Config) ->
    gen_server:call(?SERVER, Request).

%%%_* gen_server ---------------------------------------------------------

init([]) ->
    %% public: broker_storage_memory appends and reads from the caller process.
    %% A concurrent reader can therefore observe an offset that was allocated
    %% but not yet inserted and stop the page early; harmless in S1 because
    %% design §5.1 (d) serializes appends per partition through broker_queue.
    %% The table ids travel inside the handle, so no name is needed.
    Meta = ets:new(meta, [set, public, {read_concurrency, true}]),
    Msgs = ets:new(msgs, [ordered_set, public, {read_concurrency, true}]),
    {ok, #st{meta = Meta, msgs = Msgs}}.

handle_call({create, Id}, _From, #st{meta = Meta} = State) ->
    case ets:member(Meta, Id) of
        true ->
            {reply, {error, {already_exists, Id}}, State};
        false ->
            %% Last = 0 means "nothing appended yet", so the first offset is 1
            %% and CommittedOffset = 0 means "nothing acknowledged".
            true = ets:insert(Meta, {Id, true, 0, 0, 1}),
            {reply, {ok, tables(State)}, State}
    end;

%% §11.2: a missing partition is an error, never an implicit create -- an
%% auto-created empty partition would report itself healthy after a typo.
handle_call({recover, Id}, _From, #st{meta = Meta} = State) ->
    case ets:lookup(Meta, Id) of
        [{Id, _Open, Last, Committed, _Trunc}] ->
            true = ets:update_element(Meta, Id, {2, true}),
            {reply, {ok, tables(State), Committed, Last + 1}, State};
        [] ->
            {reply, {error, {not_found, Id}}, State}
    end;

handle_call({close, Id}, _From, #st{meta = Meta} = State) ->
    case ets:member(Meta, Id) of
        true ->
            true = ets:update_element(Meta, Id, {2, false}),
            {reply, ok, State};
        false ->
            {reply, {error, {not_found, Id}}, State}
    end;

%% §11.3: the watermark may run low after a crash (duplicates are in contract),
%% but must never claim an offset that was not written, and never walk back.
handle_call({advance_committed, Id, Offset}, _From, #st{meta = Meta} = State) ->
    case ets:lookup(Meta, Id) of
        [{Id, _Open, Last, Committed, _Trunc}] when Offset =< Last, Offset >= Committed ->
            true = ets:update_element(Meta, Id, {4, Offset}),
            {reply, ok, State};
        [{Id, _Open, _Last, Committed, _Trunc}] ->
            {reply, {error, {invalid_committed_offset, Id, {Committed, Offset}}}, State};
        [] ->
            {reply, {error, {not_found, Id}}, State}
    end;

%% §11.3 + §11.6: discarding records that no subscriber has acknowledged is the
%% one direction the contract makes impossible, so the boundary may not pass the
%% committed watermark.
handle_call({truncate, Id, BeforeOffset}, _From, #st{meta = Meta} = State) ->
    case ets:lookup(Meta, Id) of
        [{Id, _Open, _Last, Committed, Trunc}] when BeforeOffset >= Trunc,
                                                    BeforeOffset =< Committed + 1 ->
            true = ets:update_element(Meta, Id, {5, BeforeOffset}),
            {reply, ok, State};
        [{Id, _Open, _Last, Committed, Trunc}] ->
            {reply, {error, {invalid_truncate, Id, {Trunc, Committed, BeforeOffset}}}, State};
        [] ->
            {reply, {error, {not_found, Id}}, State}
    end.

handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%%_* Internal -----------------------------------------------------------

%% The handle carries the table ids so the data path needs no call here.
tables(#st{meta = Meta, msgs = Msgs}) ->
    {self(), Meta, Msgs}.
