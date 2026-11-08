%% @doc Partition layer between a protocol adapter and `broker_storage'.
%%
%% §5.1(d) asks for appends to a partition to be serialized. The serialization
%% point is the partition's single storage writer, not this module: `append/3'
%% reads the handle out of the registry and calls the backend **in the caller's
%% process**, the same split ADR-010【F3】 froze for `route/3'. A blocking hop
%% through `broker_queue_partition' would leave one append in flight per
%% partition at a time, so every group-commit batch would be size 1 -- the
%% ~1148 msg/s of S0.3 §2.1 that ADR-008 chose a self-written WAL to beat.
%% `broker_queue_SUITE:eight_concurrent_callers_pay_for_one_fsync' measures the
%% difference in the writer's own counters.
%%
%% What the worker does own: the storage handle's lifecycle (recover at boot,
%% re-attach after a writer crash), the capability answer conn_fsm caches, and
%% the `{new_messages, _, _, _}' fan-out. Notifications travel by cast, so they
%% never delay the producer's reply -- §5.1(e) puts PUBACK before dispatch.
%%
%% H8: `create_partition/1' is the only caller of `broker_storage:create/2' and
%% it deliberately works with this application stopped. Boot only ever recovers
%% (ADR-011 §11.2), so a data dir nobody created fails the whole application
%% instead of coming up as an empty partition that reports itself healthy.
-module(broker_queue).

-include_lib("broker_core/include/broker_message.hrl").

-export([partition_ids/0, create_partition/1, storage_config/0]).
-export([append/3, append_replica/3, reliability/0, read_from/3,
         advance_committed/2, capability/1, watermarks/1]).
-export([subscribe/2, unsubscribe/2]).
%% ADR-014 §14.5: operator/test view of what retention could not delete.
-export([retention_status/0]).

-export_type([entry/0]).

%% What a caller needs to reach a partition's log without asking a process.
-type entry() :: #{backend := module(),
                   handle := broker_storage:handle(),
                   worker := pid()}.

%%%_* Bootstrap -----------------------------------------------------------

%% S1 fixes the partition set (§1.2: online repartitioning is S4), so this is
%% config and not a query. Both `create_partition/1' and the supervisor's static
%% child list read it, which is what keeps the two from disagreeing.
-spec partition_ids() -> [partition_id()].
partition_ids() ->
    %% The 3-arity form on purpose: under OTP 28 get_env/2 hands back {ok, Val}
    %% rather than Val, so a bare pattern match on the count never fires.
    case application:get_env(broker_queue, partitions, undefined) of
        N when is_integer(N), N > 0 ->
            lists:seq(0, N - 1);
        Other ->
            %% Loud rather than an empty partition set, which would boot fine and
            %% then refuse every append with unknown_partition.
            error({invalid_partition_count, Other})
    end.

%% The out-of-band management action H8 asks for. Does not need this application
%% to be running: it only talks to `broker_storage', which is the point -- the
%% operator creates partitions, then the node is allowed to boot.
-spec create_partition(partition_id()) -> ok | {error, term()}.
create_partition(PartitionId) ->
    case lists:member(PartitionId, partition_ids()) of
        %% An id outside the configured set would create a directory no worker
        %% ever opens: a partition that looks created and serves nothing.
        false ->
            {error, {unknown_partition, PartitionId}};
        true ->
            Backend = backend(),
            case Backend:create(PartitionId, storage_config()) of
                {ok, _Handle} -> ok;
                {error, _Reason} = Error -> Error
            end
    end.

%% The single place application env becomes a `broker_storage' Config. Both
%% backends read their config as a **map**, so the env has to hold a map too
%% (`wal_group_committer:new/1' is guarded `is_map'); a proplist here would
%% reach `maps:get' and raise badarg on the first append.
-spec storage_config() -> map().
storage_config() ->
    #{data_dir => application:get_env(broker_storage, data_dir, "data"),
      wal => application:get_env(broker_storage, wal, #{})}.

backend() ->
    application:get_env(broker_storage, backend, broker_storage_wal).

%% @doc What retention is holding back, per partition (ADR-014 §14.5). The
%% `wal' backend delegates to `wal_gc:status/0'; a memory backend has no
%% segments and answers `[]'. Read-only and deliberately out of the data path.
-spec retention_status() -> [{partition_id(), offset() | undefined,
                              non_neg_integer(), atom()}].
retention_status() ->
    case backend() of
        broker_storage_wal ->
            try wal_gc:status() catch _:_ -> [] end;
        _ ->
            []
    end.

%%%_* Data path -----------------------------------------------------------

%% Returns the offset this message was given. On a stale handle the caller gets
%% an error and the registry is repaired synchronously, so the *next* append
%% succeeds. Retrying here instead would be wrong: an append that reached the
%% writer already consumed an offset, and a silent retry would put the same
%% message in the log twice without the publisher ever knowing.
%% S4 (ADR-021 §D3/§D5): two optional `persistent_term' hooks keep this module
%% free of a compile-time dependency on `broker_cluster'. With neither hook
%% installed -- a single node, or a cluster app that is not running -- every
%% path below is byte-for-byte the S1~S3 behaviour, which is what lets the whole
%% existing suite run unchanged.
%%
%%   * `append_guard' runs first. Its answer is final: a `{fenced, _}' means the
%%     write is refused before an offset is consumed.
%%   * `replicated_commit' is only accepted when a replicator is installed. The
%%     local append is still a single `fsync' (the WAL has no notion of a
%%     consensus commit); replication is the extra step that turns "my disk has
%%     it" into "a majority has it". Without a replicator the mode stays
%%     unsupported, exactly as ADR-011 froze it.
-spec append(partition_id(), #broker_message{}, sync_mode()) ->
    {ok, offset()} | {error, term()}.
append(PartitionId, Msg, Mode) ->
    case append_guard(PartitionId, Msg, Mode) of
        ok -> append_checked(PartitionId, Msg, Mode);
        {error, _Reason} = Error -> Error
    end.

%% @doc Follower-side append (ADR-021 §D5). No guard -- only the owner decides
%% whether a write may happen -- and no local notification, because a replica
%% serves no readers until ownership moves to it.
-spec append_replica(partition_id(), #broker_message{}, sync_mode()) ->
    {ok, offset()} | {error, term()}.
append_replica(PartitionId, Msg, Mode) ->
    do_append(PartitionId, Msg, Mode, false).

append_checked(PartitionId, Msg, replicated_commit) ->
    case replicator() of
        undefined ->
            %% No cluster app: keep the frozen S1~S3 answer.
            do_append(PartitionId, Msg, replicated_commit, true);
        Replicate ->
            case do_append(PartitionId, Msg, fsync, false) of
                {ok, Offset} ->
                    case Replicate(PartitionId, Msg) of
                        ok ->
                            notify_range(PartitionId, Offset),
                            {ok, Offset};
                        {error, _Reason} = Error ->
                            %% The local record stays (it is already fsynced);
                            %% the caller must NOT ack the publisher.
                            Error
                    end;
                {error, _Reason} = Error ->
                    Error
            end
    end;
append_checked(PartitionId, Msg, Mode) ->
    do_append(PartitionId, Msg, Mode, true).

do_append(PartitionId, Msg, Mode, Notify) ->
    with_partition(PartitionId,
                   fun(#{backend := Backend, handle := Hd, worker := Worker}) ->
                           try Backend:append(Hd, [Msg], Mode) of
                               {ok, [Offset]} ->
                                   case Notify of
                                       true ->
                                           notify(Worker, {appended, PartitionId,
                                                           Offset, Offset});
                                       false ->
                                           ok
                                   end,
                                   {ok, Offset};
                               {ok, Offsets} ->
                                   {error, {unexpected_offsets, Offsets}};
                               {error, _Reason} = Error ->
                                   Error
                           catch
                               Class:Reason ->
                                   {error, stale(PartitionId, Worker, Class, Reason)}
                           end
                   end).

append_guard(PartitionId, Msg, Mode) ->
    case persistent_term:get({?MODULE, append_guard}, undefined) of
        undefined -> ok;
        Guard when is_function(Guard, 3) -> Guard(PartitionId, Msg, Mode)
    end.

replicator() ->
    case persistent_term:get({?MODULE, replicator}, undefined) of
        undefined -> undefined;
        Replicate when is_function(Replicate, 2) -> Replicate
    end.

notify_range(PartitionId, Offset) ->
    case broker_queue_store:lookup(PartitionId) of
        {ok, #{worker := Worker}} ->
            notify(Worker, {appended, PartitionId, Offset, Offset});
        {error, _Reason} ->
            ok
    end.

-spec read_from(partition_id(), offset(), pos_integer()) ->
    {ok, [{offset(), #broker_message{}}], offset()} | {error, term()}.
read_from(PartitionId, From, Limit) ->
    with_partition(PartitionId,
                   fun(#{backend := Backend, handle := Hd, worker := Worker}) ->
                           try Backend:read_from(Hd, From, Limit) of
                               {ok, _Page, _Next} = Ok -> Ok;
                               {error, _Reason} = Error -> Error
                           catch
                               Class:Reason ->
                                   {error, stale(PartitionId, Worker, Class, Reason)}
                           end
                   end).

-spec advance_committed(partition_id(), offset()) -> ok | {error, term()}.
advance_committed(PartitionId, Offset) ->
    with_partition(PartitionId,
                   fun(#{backend := Backend, handle := Hd, worker := Worker}) ->
                           try Backend:advance_committed(Hd, Offset) of
                               ok -> ok;
                               {error, _Reason} = Error -> Error
                           catch
                               Class:Reason ->
                                   {error, stale(PartitionId, Worker, Class, Reason)}
                           end
                   end).

%% Both S1 backends answer this from a constant, so a stale handle still gives
%% the right capability -- which is what §5.1(a) wants, since conn_fsm caches it
%% once at startup rather than per publish.
%% @doc The mode the connection layer should use for durable publishes
%% (ADR-021 §D5). `replicated_commit' is answered only when the cluster app has
%% actually installed a replicator: advertising a mode the data path would then
%% refuse is worse than staying on `fsync'.
-spec reliability() -> sync_mode().
reliability() ->
    case {application:get_env(broker_queue, reliability, fsync), replicator()} of
        {replicated_commit, Replicate} when is_function(Replicate, 2) ->
            replicated_commit;
        _ ->
            fsync
    end.

-spec capability(partition_id()) -> [sync_mode()] | {error, term()}.
capability(PartitionId) ->
    with_partition(PartitionId,
                   fun(#{backend := Backend, handle := Hd}) -> Backend:capability(Hd) end).

%% Live {Committed, High} of a partition (design §13 K1). The only consumer is
%% `broker_subscription', which at SUBSCRIBE time needs the tail to anchor a new
%% subscription's cursor without replaying history -- and needs it **live**,
%% which is why the worker cannot serve this from the numbers its `init/1'
%% recovered: appends bypass the worker (J1), so its copy would freeze at boot.
%% Unlike `capability/1' this one can genuinely observe a stale handle, so it
%% goes through the same re-attach wrapper as the data-path calls: a caller
%% subscribing right after a writer crash gets a repaired registry, not `badarg'.
-spec watermarks(partition_id()) -> {ok, offset(), offset()} | {error, term()}.
watermarks(PartitionId) ->
    with_partition(PartitionId,
                   fun(#{backend := Backend, handle := Hd, worker := Worker}) ->
                           try Backend:watermarks(Hd) of
                               {ok, _Committed, _High} = Ok -> Ok;
                               {error, _Reason} = Error -> Error
                           catch
                               Class:Reason ->
                                   {error, stale(PartitionId, Worker, Class, Reason)}
                           end
                   end).

%%%_* Notifications -------------------------------------------------------

%% §5.2: the worker broadcasts `{new_messages, PartitionId, From, To}' after an
%% append. The notification is a wakeup, not a delivery contract -- concurrent
%% appenders mean ranges can arrive out of order and overlap, so a subscriber
%% reads from its own cursor (§5.3) rather than trusting the range.
-spec subscribe(partition_id(), pid()) -> ok | {error, term()}.
subscribe(PartitionId, Pid) ->
    with_worker(PartitionId, fun(Worker) -> call(Worker, {subscribe, Pid}) end).

-spec unsubscribe(partition_id(), pid()) -> ok | {error, term()}.
unsubscribe(PartitionId, Pid) ->
    with_worker(PartitionId, fun(Worker) -> call(Worker, {unsubscribe, Pid}) end).

%%%_* Internal ------------------------------------------------------------

with_partition(PartitionId, Fun) ->
    case broker_queue_store:lookup(PartitionId) of
        {ok, Entry} -> Fun(Entry);
        {error, _Reason} = Error -> Error
    end.

with_worker(PartitionId, Fun) ->
    case broker_queue_store:lookup(PartitionId) of
        {ok, #{worker := Worker}} -> Fun(Worker);
        {error, _Reason} = Error -> Error
    end.

call(Worker, Request) ->
    try gen_server:call(Worker, Request) of
        Reply -> Reply
    catch
        Class:Reason -> {error, {queue_unavailable, Class, Reason}}
    end.

%% A storage handle embeds the writer's pid and its index table, so it dies with
%% the writer (ADR-011 T5 批注 I15). The registry is repaired before returning,
%% which is what makes the next call work instead of every call failing until
%% somebody notices.
stale(PartitionId, Worker, Class, Reason) ->
    Repair = call(Worker, reattach),
    {stale_handle, PartitionId, Class, Reason, Repair}.

notify(Worker, Notification) ->
    %% A cast, so a busy or restarting worker delays the notification but never
    %% the producer's reply. If the worker dies the notification is lost, and
    %% the subscriber picks the messages up on its next cursor read (§5.2's
    %% catch-up path) rather than never.
    gen_server:cast(Worker, Notification).
