%% @doc One worker per partition: storage handle lifecycle and notify fan-out.
%%
%% This process is **not** on the data path (see `broker_queue'). What it owns:
%%
%%  * boot step 1 of §6. `init/1' calls `broker_storage:recover/2' for its own
%%    partition, so a partition nobody created fails here and takes the whole
%%    application down with it -- which is ADR-011 §11.2's loud failure, and the
%%    reason there is no `create' call anywhere in this module (H8).
%%  * re-attachment. A storage handle embeds its writer's pid and index table, so
%%    it is garbage once that writer dies (I15). A caller that trips over one
%%    asks here for a fresh handle; `permanent' + `one_for_one' means a crash of
%%    this worker heals the registry the same way, through `init/1'.
%%  * the `{new_messages, _, _, _}' fan-out, by cast, so the producer's reply is
%%    never held up by a slow or dead subscriber (§5.1(e)/(f)).
-module(broker_queue_partition).

-behaviour(gen_server).

-include_lib("broker_core/include/broker_message.hrl").

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% `subs' is what broker_queue_SUITE introspects with sys:get_state/1 to prove a
%% dead subscriber is actually dropped rather than merely ignored. Declared
%% rather than left to inference: init/1 builds both maps empty, and an inferred
%% `#{}' type makes every later non-empty match look unreachable.
-type state() :: #{id := partition_id(),
                   backend := module(),
                   subs := [pid()],
                   monitors := #{pid() => reference()}}.

-spec start_link(partition_id()) -> {ok, pid()} | {error, term()}.
start_link(PartitionId) ->
    gen_server:start_link(?MODULE, [PartitionId], []).

%%%_* gen_server ---------------------------------------------------------

-spec init([partition_id()]) -> {ok, state()} | {stop, term()}.
init([PartitionId]) ->
    Backend = application:get_env(broker_storage, backend, broker_storage_wal),
    case recover(PartitionId, Backend) of
        {ok, _Committed, _High} ->
            {ok, #{id => PartitionId,
                   backend => Backend,
                   subs => [],
                   monitors => #{}}};
        {error, {not_found, PartitionId}} ->
            %% Not "create it and carry on": that is precisely the silent empty
            %% partition ADR-011 §11.2 exists to prevent.
            {stop, {partition_not_found, PartitionId}};
        {error, Reason} ->
            {stop, {partition_recover_failed, PartitionId, Reason}}
    end.

%% Re-reads storage and republishes the handle. The old state is kept on
%% failure: a partition whose storage will not recover is still a partition, and
%% stopping here would turn a disk problem into a supervisor restart storm.
handle_call(reattach, _From, #{id := PartitionId, backend := Backend} = State) ->
    case recover(PartitionId, Backend) of
        {ok, _Committed, _High} -> {reply, ok, State};
        {error, Reason} -> {reply, {error, Reason}, State}
    end;

handle_call({subscribe, Pid}, _From, #{subs := Subs, monitors := Monitors} = State) ->
    case maps:is_key(Pid, Monitors) of
        true ->
            {reply, ok, State};
        false ->
            Ref = monitor(process, Pid),
            {reply, ok, State#{subs := Subs ++ [Pid],
                               monitors := maps:put(Pid, Ref, Monitors)}}
    end;

%% Idempotent: MQTT UNSUBSCRIBE and a racing disconnect both arrive here, and
%% neither is an error the caller can do anything about.
handle_call({unsubscribe, Pid}, _From, #{subs := Subs, monitors := Monitors} = State) ->
    case maps:take(Pid, Monitors) of
        {Ref, Monitors1} ->
            true = demonitor(Ref, [flush]),
            {reply, ok, State#{subs := lists:delete(Pid, Subs), monitors := Monitors1}};
        error ->
            {reply, ok, State}
    end.

handle_cast({appended, PartitionId, From, To}, #{subs := Subs} = State) ->
    lists:foreach(fun(Pid) -> Pid ! {new_messages, PartitionId, From, To} end, Subs),
    {noreply, State};

handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_info({'DOWN', Ref, process, Pid, _Reason},
            #{subs := Subs, monitors := Monitors} = State) ->
    case maps:get(Pid, Monitors, undefined) of
        Ref ->
            {noreply, State#{subs := lists:delete(Pid, Subs),
                             monitors := maps:remove(Pid, Monitors)}};
        _ ->
            {noreply, State}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    %% The registry entry is deliberately left in place. Erasing it would make
    %% the next append answer `unknown_partition', which claims the partition
    %% does not exist -- a worse lie than a stale handle that reports itself
    %% stale and gets repaired. Storage is not closed here either: the writers
    %% belong to broker_storage's tree and outlive this application.
    ok.

%%%_* Internal -----------------------------------------------------------

recover(PartitionId, Backend) ->
    case Backend:recover(PartitionId, broker_queue:storage_config()) of
        {ok, Handle, Committed, High} ->
            ok = broker_queue_store:put(PartitionId,
                                        #{backend => Backend,
                                          handle => Handle,
                                          worker => self()}),
            {ok, Committed, High};
        {error, Reason} ->
            {error, Reason}
    end.
