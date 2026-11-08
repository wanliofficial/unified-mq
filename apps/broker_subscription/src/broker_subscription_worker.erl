%% @doc Single writer for the subscription tables, boot steps 2+3 of design §6,
%% and the batched flush tiers of §5.3.
%%
%% The shape mirrors what J4 froze for `broker_queue_store' and 【F3】 froze for
%% the Router index: five `protected' ETS tables owned by this one process --
%% every other process reads them lock-free (that is `cursor/2', `subscriptions/1',
%% `dispatch/2', `role/2' in the facade), and only state *changes* are gen_server
%% calls here. A consumer's cursor read must not serialize behind a flush tick
%% that is fsyncing somebody else's file.
%%
%% ADR-019 changed three things about this process:
%%
%%  * **A subscription is a SubRef, not a client.** `?ENTITIES' is keyed by
%%    `{Kind, Key, Filter}'; a Shared/Failover group is *one* entity with an
%%    ordered `consumers' list, and `?MEMBERS' is the second index
%%    (`{ClientId, SubRef}') that answers "what does this connection consume".
%%    Two indexes, one truth: the entity owns the list, `?MEMBERS' only points.
%%  * **A shared offset is allocated here, serially** (`claim/4'). That is the
%%    whole decision of §19.1: the cursor is shared, so "who reads what" cannot
%%    be answered by each connection reading from `min(cursor)' -- they would all
%%    read the same message. The row's `assigned' map is the answer, and it lives
%%    only in memory (§19.3).
%%  * **An ack names its consumer** (`ack/4'), so a member cannot advance a
%%    position it does not hold (§19.2). A mismatch is `ok' plus a counter, never
%%    a protocol error: the acker is a conn_fsm whose view may lag a rebalance.
%%
%% What the calls decide, in order:
%%
%%  * `init/1' is boot steps 2+3 and all-or-nothing, the same stance T6 froze
%%    for partition recovery: a corrupt `.subs', an unreadable watermark or a
%%    Router that refuses the backfill stops the application loudly, because a
%%    half-loaded session set serves `not_found' for subscriptions that exist on
%%    disk (ADR-011 §11.2's silent-empty lie, one layer up).
%%  * subscribe/join (K2/K5): register in the Router **first** (a rejection fails
%%    while nothing was written anywhere), then fsync `.subs' carrying the init
%%    cursors and the consumer list, then insert the in-memory rows. A crash
%%    between the last two loses nothing: the disk copy is what boot reloads.
%%  * the flush tick (K6): dirty subscriptions' `.cursors' reach the disk
%%    *before* `advance_committed' is called with min(cursor) - 1. Reversing the
%%    order would let a crash rewind a cursor below the committed watermark, and
%%    S2's truncation would then delete messages the subscriber never acked.
-module(broker_subscription_worker).

-behaviour(gen_server).

-include_lib("broker_core/include/broker_message.hrl").

-export([start_link/0,
         subscribe/3, join/5, unsubscribe/2, leave/2,
         ack/4, claim/4, release/4, retry/4,
         inflight/2, attach/3, detach/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(ENTITIES, broker_subscription_entities).
-define(MEMBERS, broker_subscription_members).
-define(CURSORS, broker_subscription_cursors).
-define(ATTACHMENTS, broker_subscription_attachments).
-define(INFLIGHT, broker_subscription_inflight).

%%%_* Client API -----------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

subscribe(ClientId, Filter, QoS) ->
    gen_server:call(?MODULE, {subscribe, ClientId, Filter, QoS}).

join(Kind, Group, ClientId, Filter, QoS) ->
    gen_server:call(?MODULE, {join, Kind, Group, ClientId, Filter, QoS}).

unsubscribe(ClientId, Filter) ->
    gen_server:call(?MODULE, {unsubscribe, ClientId, Filter}).

leave(SubRef, ClientId) ->
    gen_server:call(?MODULE, {leave, SubRef, ClientId}).

ack(SubRef, PartitionId, Offset, Consumer) ->
    gen_server:call(?MODULE, {ack, SubRef, PartitionId, Offset, Consumer}).

claim(SubRef, PartitionId, Max, Consumer) ->
    gen_server:call(?MODULE, {claim, SubRef, PartitionId, Max, Consumer}).

%% ADR-019 §19.5: the caller read back the page and some claimed offsets were not
%% readable yet (the storage `High' watermark is a reservation, not a read
%% bound). Hand exactly those back to the pool so the next claim can re-issue
%% them once the append has landed.
-spec release({exclusive | shared | failover, binary(), binary()},
              partition_id(), [offset()], binary()) -> ok.
release(SubRef, PartitionId, Offsets, Consumer) ->
    gen_server:call(?MODULE, {release, SubRef, PartitionId, Offsets, Consumer}).

%% S2-E (ADR-015 §15.1): one delivery attempt failed (ack timeout). The
%% connection only reports; the backoff clock, attempt budget and dead-letter
%% decision live in this worker, because that state must outlive the connection.
-spec retry({exclusive | shared | failover, binary(), binary()},
            partition_id(), offset(), binary()) -> ok.
retry(SubRef, PartitionId, Offset, Consumer) ->
    gen_server:call(?MODULE, {retry, SubRef, PartitionId, Offset, Consumer}).

inflight(ClientId, Term) ->
    gen_server:call(?MODULE, {inflight, ClientId, Term}).

attach(ClientId, Pid, Persist) ->
    gen_server:call(?MODULE, {attach, ClientId, Pid, Persist}).

detach(ClientId, Pid) ->
    gen_server:call(?MODULE, {detach, ClientId, Pid}).

%%%_* gen_server -----------------------------------------------------------

init([]) ->
    %% A malformed dead-letter template is a configuration error, and the moment
    %% to say so is boot -- not the first message that needed a DLQ (S2-E E-6).
    case broker_dlq:validate_pattern(dlq_pattern()) of
        ok -> init_tables();
        {error, Reason} -> {stop, {invalid_dlq_pattern, Reason}}
    end.

init_tables() ->
    create_tables(),
    Partitions = broker_queue:partition_ids(),
    State = #{partitions => Partitions,
              max_regress => application:get_env(broker_subscription, cursor_max_regress, 64),
              cursor_interval => application:get_env(broker_subscription, cursor_flush_interval_ms, 100),
              inflight_interval => application:get_env(broker_subscription, inflight_flush_interval_ms, 100),
              shared_state_max => application:get_env(broker_subscription, shared_state_max, 4096),
              retry_interval => application:get_env(broker_subscription, retry_tick_ms, 1000),
              retry_base => application:get_env(broker_subscription, retry_base_ms, 1000),
              retry_max => application:get_env(broker_subscription, retry_max_delay_ms, 60000),
              max_attempts => application:get_env(broker_subscription, max_attempts, 10),
              last_committed => #{},
              cursor_dirty => #{},
              inflight_dirty => #{},
              active_of => #{}},
    case boot(Partitions) of
        {ok, LastCommitted} ->
            State1 = State#{last_committed := LastCommitted},
            {ok, rearm(retry_tick,
                       rearm(inflight_tick, rearm(cursor_tick, State1)))};
        {error, Reason} ->
            %% `{stop, Reason}' from init fails start_link, which fails the
            %% application: nobody can serve a subscription set that only partly
            %% loaded.
            {stop, {subscription_boot_failed, Reason}}
    end.

dlq_pattern() ->
    application:get_env(broker_subscription, dlq_name_pattern,
                        <<"$dlq/{tenant}/{original}">>).

handle_call({subscribe, ClientId, Filter, QoS}, _From, State) ->
    SubRef = {exclusive, ClientId, Filter},
    case ets:lookup(?ENTITIES, SubRef) of
        [_] ->
            %% Re-subscribe (MQTT replaces the QoS of a live filter): the
            %% Router entry is keyed by the same opaque tuple under the same
            %% filter, so it is already exact -- touching it would open an
            %% unregister/register window for zero effect (K2). The cursor is
            %% deliberately *not* re-anchored: a re-subscribe continues from
            %% where the subscription left off.
            reply_resubscribe(SubRef, QoS, State);
        [] ->
            reply_new_subscribe(SubRef, #{kind => exclusive, key => ClientId,
                                          filter => Filter, qos => QoS,
                                          consumers => [ClientId]},
                                State)
    end;

handle_call({join, Kind, Group, ClientId, Filter, QoS}, _From, State)
  when Kind =:= shared; Kind =:= failover ->
    SubRef = {Kind, Group, Filter},
    case ets:lookup(?ENTITIES, SubRef) of
        [{_, #{consumers := Consumers, qos := OldQoS} = Info}] ->
            case lists:member(ClientId, Consumers) of
                true ->
                    %% Already a member: a re-SUBSCRIBE may only change the QoS,
                    %% and it must not move the consumer's place in the order
                    %% (that order is the failover active election, ADR-019 §19.4).
                    Info1 = Info#{qos => QoS},
                    case persist_entity(SubRef, Info1) of
                        ok -> {reply, {ok, SubRef}, State};
                        {error, Reason} ->
                            {reply, {error, {persist_subs, Reason}}, State}
                    end;
                false ->
                    _ = OldQoS,
                    %% Appended at the tail: a joining member never displaces the
                    %% active one, and a reconnecting one is appended rather than
                    %% restored to its old slot -- membership order is "who joined
                    %% first among the current members", which is the only rule
                    %% that survives an arbitrary death order.
                    Info1 = Info#{consumers => Consumers ++ [ClientId],
                                  qos => QoS},
                    case persist_entity(SubRef, Info1) of
                        ok ->
                            true = ets:insert(?ENTITIES, {SubRef, Info1}),
                            true = ets:insert(?MEMBERS, {{ClientId, SubRef}}),
                            {reply, {ok, SubRef},
                             refresh_active(SubRef, State)};
                        {error, Reason} ->
                            {reply, {error, {persist_subs, Reason}}, State}
                    end
            end;
        [] ->
            case broker_router:register(Filter, SubRef, #{qos => QoS}) of
                Result when Result =:= ok;
                            Result =:= {error, {already_registered, SubRef}} ->
                    Info = #{kind => Kind, key => Group, filter => Filter,
                             qos => QoS, consumers => [ClientId]},
                    reply_new_subscribe2(SubRef, Info, State);
                {error, Reason} ->
                    {reply, {error, {router_register, Reason}}, State}
            end
    end;

handle_call({join, _Kind, _Group, _ClientId, _Filter, _QoS}, _From, State) ->
    {reply, {error, wrong_kind}, State};

handle_call({unsubscribe, ClientId, Filter}, _From, State) ->
    remove_subscription({exclusive, ClientId, Filter}, State);

handle_call({leave, SubRef, ClientId}, _From, State) ->
    case ets:lookup(?ENTITIES, SubRef) of
        [] ->
            {reply, ok, State};
        [{_, #{kind := exclusive}}] ->
            %% `leave/2' is for group memberships; an exclusive subscription has
            %% exactly one consumer and UNSUBSCRIBE is its removal path.
            {reply, ok, State};
        [{_, #{consumers := Consumers} = Info}] ->
            case lists:member(ClientId, Consumers) of
                false ->
                    {reply, ok, State};
                true ->
                    Rest = lists:delete(ClientId, Consumers),
                    State1 = maybe_release(SubRef, ClientId, State),
                    case Rest of
                        [] ->
                            %% Last member left: the entity, its cursors, its
                            %% Router entry and its files all go (ADR-019 §19.2).
                            _ = broker_router:unregister(SubRef),
                            ok = broker_subscription_files:delete_subscription(SubRef),
                            remove_local(SubRef),
                            true = ets:delete(?MEMBERS, {ClientId, SubRef}),
                            {reply, ok, drop_dirty(SubRef, State1)};
                        [NewActive | _] ->
                            Info1 = Info#{consumers => Rest},
                            case persist_entity(SubRef, Info1) of
                                ok ->
                                    true = ets:insert(?ENTITIES, {SubRef, Info1}),
                                    true = ets:delete(?MEMBERS, {ClientId, SubRef}),
                                    _ = NewActive,
                                    {reply, ok, refresh_active(SubRef, State1)};
                                {error, Reason} ->
                                    {reply, {error, {persist_subs, Reason}}, State1}
                            end
                    end
            end
    end;

handle_call({ack, SubRef, PartitionId, Offset, Consumer}, _From, State) ->
    State1 = case ets:lookup(?CURSORS, {SubRef, PartitionId}) of
                 [] ->
                     %% Unknown sub or unknown partition: idempotent ok. The
                     %% acker is a conn_fsm whose view may lag an UNSUBSCRIBE by
                     %% one message; failing it would turn a race into a
                     %% protocol error.
                     State;
                 [{_, Row}] ->
                     case ack_row(Row, Offset, Consumer) of
                         unowned ->
                             broker_metrics:inc(shared_ack_unowned),
                             State;
                         {ok, Row1} ->
                             true = ets:insert(?CURSORS,
                                               {{SubRef, PartitionId}, Row1}),
                             absorb_advance(SubRef, delta(Row, Row1), State)
                     end
             end,
    {reply, ok, State1};

handle_call({claim, SubRef = {_Kind, _, _}, PartitionId, Max, Consumer}, _From, State) ->
    case ets:lookup(?CURSORS, {SubRef, PartitionId}) of
        [] ->
            {reply, {error, {no_cursor_for_partition, SubRef, PartitionId}}, State};
        [{_, #{kind := Kind}}] when Kind =/= shared ->
            {reply, {error, wrong_kind}, State};
        [{_, Row}] ->
            #{shared_state_max := Limit} = State,
            case broker_queue:watermarks(PartitionId) of
                {ok, _Committed, High} ->
                    Blocked = broker_cursor:blocked(Row, now_ms(), High),
                    case broker_cursor:claim(Row, Max, High, Consumer, Limit,
                                             Blocked) of
                        {ok, [], _Row1} ->
                            %% The tail: not an error, and not a claim either.
                            {reply, {ok, []}, State};
                        {ok, Offsets, Row1} ->
                            true = ets:insert(?CURSORS,
                                              {{SubRef, PartitionId}, Row1}),
                            broker_metrics:inc(shared_claims),
                            {reply, {ok, Offsets}, State};
                        {error, overflow} ->
                            broker_metrics:inc(shared_window_full),
                            {reply, {error, overflow}, State}
                    end;
                {error, Reason} ->
                    %% The pull loop degrades to `idle' this round and retries on
                    %% the poll tick; a watermark read must not crash a
                    %% connection (§19.1).
                    {reply, {error, {watermark, PartitionId, Reason}}, State}
            end
    end;

handle_call({release, SubRef = {_Kind, _, _}, PartitionId, Offsets, Consumer}, _From, State) ->
    case ets:lookup(?CURSORS, {SubRef, PartitionId}) of
        [{_, #{kind := shared} = Row}] ->
            %% Ownership-checked per offset inside the cursor module, so a
            %% rollback cannot resurrect an offset that was acked in the
            %% meantime (§19.5).
            true = ets:insert(?CURSORS,
                              {{SubRef, PartitionId},
                               broker_cursor:rollback(Row, Offsets, Consumer)}),
            {reply, ok, State};
        _ ->
            %% The row is gone (subscription left) or not shared: nothing to
            %% return, and the caller is retiring this ref anyway.
            {reply, ok, State}
    end;

handle_call({retry, SubRef = {_Kind, _, _}, PartitionId, Offset, Consumer}, _From,
            State) ->
    {reply, ok, retry_delivery(SubRef, PartitionId, Offset, Consumer, State)};

handle_call({inflight, ClientId, Term}, _From, State) ->
    true = ets:insert(?INFLIGHT, {ClientId, Term}),
    State1 = mark_inflight_dirty(ClientId, State),
    {reply, ok, maybe_flush_inflight(ClientId, State1)};

handle_call({attach, ClientId, Pid, Persist}, _From, State) ->
    %% `{ok, PrevPid | undefined}': the takeover signal the new conn sends to
    %% Prev itself — this process must not touch the old socket (K10's pid
    %% guard stays the authority on eviction). The same-pid case answers
    %% `undefined' so a re-attach never tells a conn to kill itself, and the
    %% replaced row's monitor is released here (ref leak otherwise).
    Prev = case ets:lookup(?ATTACHMENTS, ClientId) of
               [{_, #{pid := Pid, ref := _StillValid}}] ->
                   undefined;
               [{_, #{pid := OldPid, ref := OldRef}}] ->
                   _ = erlang:demonitor(OldRef, [flush]),
                   OldPid;
               [] ->
                   undefined
           end,
    true = ets:insert(?ATTACHMENTS,
                      {ClientId, #{pid => Pid,
                                   ref => erlang:monitor(process, Pid),
                                   persist => Persist}}),
    {reply, {ok, Prev}, refresh_all_active(subs_of(ClientId), State)};

handle_call({detach, ClientId, Pid}, _From, State) ->
    {ok, State1} = do_detach(ClientId, Pid, State),
    {reply, ok, State1};

handle_call(Request, _From, State) ->
    {stop, {unexpected_call, Request}, State}.

handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

%% A DOWN whose (Pid, Ref) matches no attachment is a late death notice for an
%% already-detached or already-replaced connection: the pid guard in do_detach
%% is what keeps an old takeover'd process's DOWN from evicting the new
%% attachment (K10).
handle_info({'DOWN', Ref, process, Pid, _Reason}, State) ->
    case ets:foldr(fun({ClientId, #{pid := P, ref := R}}, Acc) ->
                           case {P, R} of
                               {Pid, Ref} -> ClientId;
                               _ -> Acc
                           end
                   end, undefined, ?ATTACHMENTS) of
        undefined ->
            {noreply, State};
        ClientId ->
            {ok, State1} = do_detach(ClientId, Pid, State),
            {noreply, State1}
    end;

handle_info(cursor_tick, State) ->
    %% K6, the rigid order: every dirty cursor file first, aggregation second.
    State1 = flush_all_dirty_cursors(State),
    State2 = compute_committed(State1),
    _ = sample_gauges(State2),
    {noreply, rearm(cursor_tick, State2)};

handle_info(retry_tick, State) ->
    {noreply, rearm(retry_tick, sweep_retries(State))};

handle_info(inflight_tick, State) ->
    {noreply, rearm(inflight_tick, flush_all_dirty_inflight(State))};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    %% Best effort only: this runs while the application is being stopped, and
    %% `broker_queue' may already be gone. The §5.3 bound that the batched tiers
    %% promise is against *crash*, not against a clean shutdown that lost a
    %% tick -- and a failed write here is indistinguishable from that bound.
    try
        _ = flush_all_dirty_cursors(State),
        _ = flush_all_dirty_inflight(State)
    catch
        _:_ -> ok
    end,
    ok.

%%%_* Boot: steps 2 + 3 ------------------------------------------------------

boot(Partitions) ->
    %% ADR-019 §19.4: migration first, then the orphan sweep, then the load. A
    %% directory that is half v1 is refused rather than served with a smaller
    %% subscription set.
    case broker_subscription_files:migrate_v1() of
        ok ->
            ok = broker_subscription_files:prune_orphan_cursors(),
            boot_subscriptions(Partitions);
        {error, Reason} ->
            %% Same wrapper the load path uses: a corrupt session file is one
            %% refusal whatever stage noticed it.
            {error, {session_scan, Reason}}
    end.

boot_subscriptions(Partitions) ->
    case {live_watermarks(Partitions), broker_subscription_files:list_subscriptions()} of
        {{ok, Watermarks}, {ok, Subs}} ->
            LastCommitted = maps:from_list([{P, C} || {P, {C, _High}} <- Watermarks]),
            WM = maps:from_list([{P, High} || {P, {_C, High}} <- Watermarks]),
            case load_entities(Subs, Partitions, WM, []) of
                {ok, Refs} ->
                    case backfill(Refs) of
                        ok ->
                            load_all_inflight(),
                            {ok, LastCommitted};
                        {error, Reason} -> {error, Reason}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        {{error, Reason}, _} ->
            {error, Reason};
        {_, {error, Reason}} ->
            {error, {session_scan, Reason}}
    end.

live_watermarks(Partitions) ->
    live_watermarks(Partitions, []).

live_watermarks([P | Rest], Acc) ->
    case broker_queue:watermarks(P) of
        {ok, Committed, High} -> live_watermarks(Rest, [{P, {Committed, High}} | Acc]);
        {error, Reason} -> {error, {watermark, P, Reason}}
    end;
live_watermarks([], Acc) ->
    {ok, lists:reverse(Acc)}.

%% Step 2: entities + members + cursors from disk. The rule per (SubRef,
%% PartitionId): `.cursors' if it carries the key, else the init anchor embedded
%% in `.subs' (K5) -- never `Committed', never 0, never a fresh High for a
%% partition the subscription already had (that would rewind past acks or skip
%% past un-acknowledged backlog). A partition missing from *both* files is one
%% that did not exist when the subscription was made; it anchors at the live
%% tail, which is what subscribing to it today would do.
load_entities([{SubRef, Body} | Rest], Partitions, WM, Refs) ->
    case load_subscription(SubRef, Body, Partitions, WM) of
        {ok, Ref} -> load_entities(Rest, Partitions, WM, Refs ++ [Ref]);
        {error, Reason} -> {error, {SubRef, Reason}}
    end;
load_entities([], _Partitions, _WM, Refs) ->
    {ok, Refs}.

load_subscription(SubRef, Body, Partitions, WM) ->
    case broker_subscription_files:read_cursors(SubRef) of
        {ok, Flushed0} ->
            try
                #{kind := Kind, filter := Filter, consumers := Consumers,
                  init_cursors := InitCursors} = Body,
                true = ets:insert(?ENTITIES, {SubRef, Body}),
                lists:foreach(fun(C) -> true = ets:insert(?MEMBERS, {{C, SubRef}}) end,
                              Consumers),
                Init = maps:from_list(InitCursors),
                Flushed = flushed_rows(SubRef, Flushed0),
                lists:foreach(
                  fun(P) ->
                      Row = restored_row(Kind, P, Flushed, Init, WM),
                      true = ets:insert(?CURSORS, {{SubRef, P}, Row})
                  end, Partitions),
                {ok, {SubRef, Filter}}
            catch
                Class:Reason -> {error, {load_failed, Class, Reason}}
            end;
        {error, Reason} ->
            {error, {cursors_file, Reason}}
    end.

flushed_rows(_SubRef, none) ->
    #{};
flushed_rows(SubRef, #{rows := Rows}) when is_map(Rows) ->
    maps:from_list([{P, Row} || {P, Row} <- maps:to_list(Rows),
                                is_map(Row), maps:get(kind, Row) =:= element(1, SubRef)]).

restored_row(Kind, P, Flushed, Init, WM) ->
    case Flushed of
        #{P := Row} ->
            %% `assigned' never reached the disk (§19.3), so a shared row comes
            %% back with its claim list empty: everything un-acked is owed again.
            broker_cursor:restore(Row);
        _ ->
            %% `.cursors' has no row for this partition: fall back to the init
            %% anchor inside `.subs' (K5), then to the live tail.
            Offset = case Init of
                         #{P := I} -> I;
                         _ -> maps:get(P, WM, 0)
                     end,
            new_row(Kind, Offset)
    end.

new_row(shared, Offset) -> broker_cursor:new_shared(Offset);
new_row(Kind, Offset) -> broker_cursor:new_plain(Kind, Offset).

%% In-flight blobs are client-keyed and independent of the subscription scan
%% (§19.3), so they load once per client after the entities are in place.
load_all_inflight() ->
    %% S2.5 (ADR-017): a v5 session whose Expiry Interval elapsed while the
    %% broker was down must not come back as `session_present=1'. The deadline
    %% is stamped by the connection at disconnect; boot is the only place that
    %% can enforce it before a client asks. Expired sessions are torn down
    %% (disk first, then Router, then memory -- same order as clean_session).
    Clients = all_clients(),
    Expired = [C || C <- Clients, inflight_expired(C)],
    lists:foreach(
      fun(ClientId) ->
              log_expired(ClientId),
              _ = teardown_session(ClientId, #{cursor_dirty => #{},
                                               inflight_dirty => #{}})
      end, Expired),
    lists:foreach(
      fun(ClientId) ->
              case broker_subscription_files:read_inflight(ClientId) of
                  {ok, none} -> ok;
                  {ok, Term} -> true = ets:insert(?INFLIGHT, {ClientId, Term})
              end
      end, Clients -- Expired).

inflight_expired(ClientId) ->
    case broker_subscription_files:read_inflight(ClientId) of
        {ok, Term} when is_map(Term) ->
            case maps:get(session_expires_at, Term, undefined) of
                At when is_integer(At) ->
                    At =< erlang:system_time(millisecond);
                _ ->
                    false
            end;
        _ ->
            false
    end.

log_expired(ClientId) ->
    logger:info("broker_subscription: session expired at boot ~p", [ClientId]).

all_clients() ->
    lists:usort([C || {{C, _SubRef}} <- ets:tab2list(?MEMBERS)]).

%% Step 3: backfill the Router index. `already_registered' is ok (K8): the
%% crash window that orphans a Router entry ends with exactly this file on disk
%% and this SubRef still indexed, and refusing to boot over it would let a
%% bookkeeping gap in one layer take down another.
backfill([{SubRef, Filter} | Rest]) ->
    case broker_router:register(Filter, SubRef, #{}) of
        ok -> backfill(Rest);
        {error, {already_registered, SubRef}} -> backfill(Rest);
        {error, Reason} -> {error, {backfill, SubRef, Reason}}
    end;
backfill([]) ->
    ok.

%%%_* subscribe / join -------------------------------------------------------

reply_resubscribe(SubRef = {_Kind, _Key, _Filter}, QoS, State) ->
    [{SubRef, Info}] = ets:lookup(?ENTITIES, SubRef),
    Info1 = Info#{qos => QoS},
    case persist_entity(SubRef, Info1) of
        ok ->
            true = ets:insert(?ENTITIES, {SubRef, Info1}),
            {reply, {ok, SubRef}, State};
        {error, Reason} ->
            {reply, {error, {persist_subs, Reason}}, State}
    end.

reply_new_subscribe(SubRef, Info, State) ->
    #{filter := Filter} = Info,
    case broker_router:register(Filter, SubRef, #{qos => maps:get(qos, Info)}) of
        Result when Result =:= ok;
                    Result =:= {error, {already_registered, SubRef}} ->
            reply_new_subscribe2(SubRef, Info, State);
        {error, Reason} ->
            {reply, {error, {router_register, Reason}}, State}
    end.

reply_new_subscribe2(SubRef, Info, State) ->
    #{partitions := Partitions} = State,
    case tail_offsets(Partitions, []) of
        {ok, InitCursors} ->
            Full = Info#{init_cursors => InitCursors,
                         created_at => erlang:system_time(millisecond)},
            case persist_entity(SubRef, Full) of
                ok ->
                    true = ets:insert(?ENTITIES, {SubRef, Full}),
                    lists:foreach(
                      fun(C) -> true = ets:insert(?MEMBERS, {{C, SubRef}}) end,
                      maps:get(consumers, Full)),
                    Kind = maps:get(kind, Full),
                    lists:foreach(
                      fun({P, Offset}) ->
                          true = ets:insert(?CURSORS,
                                            {{SubRef, P}, new_row(Kind, Offset)})
                      end, InitCursors),
                    {reply, {ok, SubRef}, refresh_active(SubRef, State)};
                {error, Reason} ->
                    %% Roll the Router back so a rejected subscribe leaves no
                    %% trace anywhere: the K8 orphan tolerance exists for
                    %% crashes, not for refusals we could have prevented.
                    _ = broker_router:unregister(SubRef),
                    {reply, {error, {persist_subs, Reason}}, State}
            end;
        {error, Reason} ->
            _ = broker_router:unregister(SubRef),
            {reply, {error, {watermarks, Reason}}, State}
    end.

tail_offsets([P | Rest], Acc) ->
    case broker_queue:watermarks(P) of
        {ok, _Committed, High} -> tail_offsets(Rest, [{P, High} | Acc]);
        {error, Reason} -> {error, {P, Reason}}
    end;
tail_offsets([], Acc) ->
    {ok, lists:reverse(Acc)}.

persist_entity(SubRef, #{kind := Kind, key := Key, filter := Filter, qos := QoS,
                         consumers := Consumers, init_cursors := Init} = Info) ->
    broker_subscription_files:write_subs(
      SubRef, #{kind => Kind, key => Key, filter => Filter, qos => QoS,
                consumers => Consumers, init_cursors => Init,
                created_at => maps:get(created_at, Info, 0)}).

%%%_* unsubscribe / leave ---------------------------------------------------

remove_subscription(SubRef = {_Kind, _Key, _Filter}, State) ->
    case ets:lookup(?ENTITIES, SubRef) of
        [] ->
            {reply, ok, State};
        [{_, #{consumers := Consumers}}] ->
            %% Disk first, then Router, then memory: a crash between persist
            %% and unregister leaves a Router orphan the dispatch path skips
            %% (K8), whereas unregistering a still-persisted subscription first
            %% would silently stop delivering a sub that reboot brings back. A
            %% failed persist changes nothing anywhere.
            ok = broker_subscription_files:delete_subscription(SubRef),
            _ = broker_router:unregister(SubRef),
            remove_local(SubRef),
            lists:foreach(fun(C) -> true = ets:delete(?MEMBERS, {C, SubRef}) end,
                          Consumers),
            {reply, ok, drop_dirty(SubRef, State)}
    end.

%% A member's claimed-but-unacked offsets go back to the pool. Only shared rows
%% have assignments; the plain rows of a departed exclusive subscriber keep
%% their scalar cursor, which is the session that will resume.
maybe_release(SubRef, ClientId, State) ->
    lists:foreach(
      fun({{S, P}, Row}) when S =:= SubRef ->
              case maps:get(kind, Row, undefined) of
                  shared -> true = ets:insert(?CURSORS, {{S, P}, broker_cursor:release(Row, ClientId)});
                  _ -> ok
              end;
         (_) ->
              ok
      end, ets:match_object(?CURSORS, {{SubRef, '_'}, '_'})),
    State.

%%%_* ack / cursor aggregation ------------------------------------------------

ack_row(#{kind := shared} = Row, Offset, Consumer) ->
    broker_cursor:ack_shared(Row, Offset, Consumer);
ack_row(Row, Offset, _Consumer) ->
    {ok, broker_cursor:advance_plain(Row, Offset)}.

delta(Row, Row1) ->
    broker_cursor:next_read(Row1) - broker_cursor:next_read(Row).

absorb_advance(_SubRef, 0, State) ->
    State;
absorb_advance(_SubRef, Delta, State) when Delta < 0 ->
    _ = Delta,
    State;
absorb_advance(SubRef, Delta, State) ->
    #{cursor_dirty := Dirty, max_regress := MaxRegress, cursor_interval := Interval} = State,
    Count = maps:get(SubRef, Dirty, 0) + Delta,
    State1 = State#{cursor_dirty := Dirty#{SubRef => Count}},
    if
        %% The two §5.3 knobs meet here: the interval arms a timer, the count
        %% bounds how far a busy subscription can run ahead of its file. An
        %% interval of 0 selects the near-zero-regress tier: every advance
        %% flushes.
        Interval =:= 0, Count > 0 -> flush_sub_cursors(SubRef, compute_committed(State1));
        Count >= MaxRegress -> flush_sub_cursors(SubRef, compute_committed(State1));
        true -> State1
    end.

flush_sub_cursors(SubRef, State) ->
    #{cursor_dirty := Dirty} = State,
    case ets:lookup(?ENTITIES, SubRef) of
        [] ->
            %% The subscription was torn down between its cursor was marked
            %% dirty and this flush tick -- a clean-session teardown or a
            %% takeover can race the tick (the WS compat suite hits this with
            %% rapid real-client connect/subscribe/disconnect churn). There is
            %% no entity and no cursor file to write; drop the stale key so the
            %% next tick does not trip over it again.
            State#{cursor_dirty := maps:remove(SubRef, Dirty)};
        [_] ->
            case broker_subscription_files:write_cursors(SubRef, cursors_body(SubRef)) of
                ok ->
                    State#{cursor_dirty := maps:remove(SubRef, Dirty)};
                {error, _Reason} ->
                    %% The in-memory cursor is still correct to read (a restart is the
                    %% only consumer of the file); the tick retries. The regress bound
                    %% degrades to "as good as the next tick", which is what the
                    %% interval knob already promises.
                    State
            end
    end.

cursors_body(SubRef) ->
    [{_, #{kind := Kind}}] = ets:lookup(?ENTITIES, SubRef),
    #{kind => Kind,
      rows => maps:from_list(
                [{P, broker_cursor:persist(Row)}
                 || {{_SubRef, P}, Row} <- ets:match_object(?CURSORS, {{SubRef, '_'}, '_'})])}.

flush_all_dirty_cursors(State) ->
    #{cursor_dirty := Dirty} = State,
    lists:foldl(fun(SubRef, Acc) -> flush_sub_cursors(SubRef, Acc) end,
                State, maps:keys(Dirty)).

%% K6: committed = min(live cursors) - 1, and only for partitions that have at
%% least one cursor (an S1 partition with no subscriber is pinned by nobody --
%% dropping to `none' and back on the first subscribe would regress the
%% watermark, which the storage guard would rightly refuse). `compute_committed'
%% must run after the cursor files: see the module doc.
compute_committed(State) ->
    #{last_committed := Last, partitions := Partitions} = State,
    Mins = ets:foldr(fun({{_SubRef, P}, Row}, Acc) ->
                             Next = broker_cursor:next_read(Row),
                             maps:update_with(P, fun(Min) -> erlang:min(Min, Next) end,
                                              Next, Acc)
                     end, #{}, ?CURSORS),
    NewLast =
        lists:foldl(
          fun(P, Acc) ->
                  case Mins of
                      #{P := Min} ->
                          Candidate = Min - 1,
                          Old = maps:get(P, Acc, none),
                          case Old =:= none orelse Candidate > Old of
                              true ->
                                  case broker_queue:advance_committed(P, Candidate) of
                                      ok -> Acc#{P => Candidate};
                                      %% Stale handle, guard refusal, whatever:
                                      %% the next tick recomputes from cursors
                                      %% that only moved forward.
                                      {error, _Reason} -> Acc
                                  end;
                              false ->
                                  Acc
                          end;
                      #{} ->
                          Acc
                  end
          end, Last, Partitions),
    State#{last_committed := NewLast}.

%% ADR-016's backlog gauges, refreshed on the cursor tick (100 ms by default):
%%
%%   * subscription_backlog = sum over cursor rows of (High - next_read): messages
%%     a subscription has not read yet.
%%   * cursor_lag = sum over partitions of (High - Committed): the window not
%%     yet acknowledged by *every* subscriber, i.e. what retention may not
%%     reclaim.
%%
%% Both are single aggregate values on purpose -- per-subscription keys would be
%% unbounded cardinality (ADR-016 §16.2). A watermark failure (a writer crash
%% between two ticks) simply skips that partition for this sample.
sample_gauges(#{partitions := Partitions}) ->
    Marks = maps:from_list(
              [{P, WM} || P <- Partitions,
                          WM <- [case (catch broker_queue:watermarks(P)) of
                                     {ok, _C, _H} = Ok -> Ok;
                                     _ -> undefined
                                 end],
                          WM =/= undefined]),
    {Backlog, Lag} =
        lists:foldl(
          fun({{_SubRef, P}, Row}, {B, L}) ->
                  case maps:get(P, Marks, undefined) of
                      {ok, _Committed, High} ->
                          {B + max(0, High - broker_cursor:next_read(Row)), L};
                      undefined -> {B, L}
                  end
          end, {0, 0}, ets:tab2list(?CURSORS)),
    {Backlog1, Lag1} =
        maps:fold(
          fun(_P, {ok, Committed, High}, {B, L}) ->
                  {B, L + max(0, High - Committed)};
             (_P, _Other, Acc) ->
                  Acc
          end, {Backlog, Lag}, Marks),
    broker_metrics:gauge(subscription_backlog, Backlog1),
    broker_metrics:gauge(cursor_lag, Lag1),
    ok.

%%%_* Retry scheduling and dead letters (S2-E, ADR-015) -----------------------

%% The failure signal is an ack timeout reported by the connection (E-1). What
%% happens next is decided here so it survives the connection:
%%
%%   * `attempts' counts *failures*; reaching `max_attempts' dead-letters the
%%     message, advances the cursor and frees the window (§15.2);
%%   * otherwise the offset is scheduled at `base * 2^(n-1)' (capped) and the
%%     retry tick wakes whoever can redeliver it then.
%%
%% `owns/3' drops a report from a consumer that no longer holds the offset: that
%% is a rebalance racing a timeout, not a retry.
retry_delivery(SubRef, PartitionId, Offset, Consumer, State) ->
    case ets:lookup(?CURSORS, {SubRef, PartitionId}) of
        [] ->
            State;
        [{_, Row}] ->
            case owns(Row, Offset, Consumer) of
                false ->
                    State;
                true ->
                    Now = now_ms(),
                    Attempts = broker_cursor:attempts(Row, Offset) + 1,
                    case Attempts >= maps:get(max_attempts, State) of
                        true ->
                            dead_letter(SubRef, PartitionId, Offset, Attempts,
                                        Row, State);
                        false ->
                            NextAt = Now + backoff(Attempts, State),
                            Row1 = schedule_retry(Row, Offset, NextAt, Now,
                                                  ack_timeout),
                            store_row(SubRef, PartitionId, Row1, State)
                    end
            end
    end.

owns(#{kind := shared, assigned := Assigned}, Offset, Consumer) ->
    maps:get(Offset, Assigned, undefined) =:= Consumer;
owns(_Row, _Offset, _Consumer) ->
    true.

%% A shared offset that just timed out goes back to the pool *after* its
%% backoff: `release_offset/2' drops the assignment, `retry/5' parks the offset
%% in `retries', and `claim/6' will skip it until `next_at' (E-5). A plain row
%% keeps its scalar cursor and its retry entry; nothing is reassigned.
schedule_retry(#{kind := shared} = Row, Offset, NextAt, Now, Reason) ->
    broker_cursor:retry(broker_cursor:release_offset(Row, Offset), Offset,
                        NextAt, Now, Reason);
schedule_retry(Row, Offset, NextAt, Now, Reason) ->
    broker_cursor:retry(Row, Offset, NextAt, Now, Reason).

backoff(Attempts, State) ->
    broker_cursor:backoff(Attempts, maps:get(retry_base, State),
                          maps:get(retry_max, State)).

%% The retry tick is the only scheduler. Each row with retry state is examined
%% against its partition's watermark (an offset the log does not have yet cannot
%% be redelivered):
%%
%%   * a due offset is paused (`next_at = infinity') and its executor woken --
%%     `infinity' means "handed to a connection, waiting for its next timeout",
%%     which is why one wake produces at most one retry;
%%   * a `dlq_append_failed' entry tries the dead-letter append again, keeping
%%     the cursor where it is (§15.5).
sweep_retries(State) ->
    Now = now_ms(),
    lists:foldl(
      fun({{SubRef, P}, Row}, Acc) ->
              case map_size(broker_cursor:retries(Row)) of
                  0 ->
                      Acc;
                  _ ->
                      case broker_queue:watermarks(P) of
                          {ok, _Committed, High} ->
                              lists:foldl(
                                fun(O, SAcc) ->
                                        sweep_one(SubRef, P, O, High, Now, SAcc)
                                end, Acc, broker_cursor:due(Row, Now, High));
                          {error, _Reason} ->
                              Acc
                      end
              end
      end, State, ets:tab2list(?CURSORS)).

sweep_one(SubRef, P, O, _High, _Now, State) ->
    %% Re-read: an ack (which clears the entry) may have landed between the
    %% snapshot `sweep_retries/1' took and this offset's turn.
    case ets:lookup(?CURSORS, {SubRef, P}) of
        [] ->
            State;
        [{_, Row}] ->
            case maps:get(O, broker_cursor:retries(Row), undefined) of
                undefined ->
                    State;
                #{reason := dlq_append_failed, attempts := Attempts} ->
                    dead_letter(SubRef, P, O, Attempts, Row, State);
                #{next_at := At} when is_integer(At) ->
                    Row1 = broker_cursor:pause(Row, [O]),
                    State1 = store_row(SubRef, P, Row1, State),
                    wake_executor(SubRef, P, O, State1),
                    broker_metrics:inc(redelivery_total),
                    State1;
                _ ->
                    State
            end
    end.

dead_letter(SubRef, P, O, Attempts, Row, State) ->
    Kind = element(1, SubRef),
    QoS = entity_qos(SubRef),
    case broker_dlq:dead_letter(P, O, #{qos => QoS, kind => Kind,
                                        attempts => Attempts}) of
        ok ->
            finish_dead_letter(SubRef, P, O, Row, State);
        {error, already_dead_letter} ->
            %% The original is already a dead letter (it timed out on a `$dlq'
            %% subscription). Nesting `$dlq/.../$dlq/...' adds no information, so
            %% the cursor advances and the existing copy stays the evidence.
            log_info("dead letter timed out again; advancing", [SubRef, P, O]),
            finish_dead_letter(SubRef, P, O, Row, State);
        {error, Reason} ->
            %% §15.5: a failed dead-letter append must not consume the cursor.
            %% Park it and retry the append on the next tick.
            log_warning("dead letter append failed ~p for ~p ~p/~p",
                        [Reason, SubRef, P, O]),
            Now = now_ms(),
            Row1 = schedule_retry(Row, O, Now + maps:get(retry_base, State), Now,
                                  dlq_append_failed),
            store_row(SubRef, P, Row1, State)
    end.

%% The broker consumed the offset: plain rows advance their scalar cursor,
%% shared rows settle the assignment even though no consumer acked it.
finish_dead_letter(SubRef, P, O, Row, State) ->
    Row1 = case maps:get(kind, Row) of
               shared -> broker_cursor:settle(Row, O);
               _ -> broker_cursor:advance_plain(Row, O)
           end,
    State1 = store_row(SubRef, P, Row1, State),
    absorb_advance(SubRef, delta(Row, Row1), State1).

%% Who can redeliver this offset now:
%%   * exclusive -- the client that owns the SubRef;
%%   * failover  -- the currently active member (a standby must not pull, §19.4);
%%   * shared    -- *every* attached member: the wake is a hint, a later claim
%%                  decides who actually reads it (ADR-019 §19.4).
wake_executor(SubRef = {shared, _Key, _Filter}, P, O, _State) ->
    case ets:lookup(?ENTITIES, SubRef) of
        [{_, #{consumers := Consumers}}] ->
            lists:foreach(fun(C) -> wake_client(C, {broker_redeliver, shared,
                                                    SubRef, P, O}) end, Consumers);
        [] ->
            ok
    end;
wake_executor(SubRef = {Kind, _Key, _Filter}, P, O, _State) when Kind =:= exclusive ->
    wake_client(element(2, SubRef), {broker_redeliver, Kind, SubRef, P, O});
wake_executor(SubRef = {failover, _Key, _Filter}, P, O, State) ->
    case owner_of(SubRef, State) of
        undefined -> ok;
        ClientId -> wake_client(ClientId, {broker_redeliver, failover, SubRef, P, O})
    end.

owner_of(SubRef, State) ->
    case maps:get(SubRef, maps:get(active_of, State, #{}), undefined) of
        undefined ->
            case ets:lookup(?ENTITIES, SubRef) of
                [{_, #{consumers := Consumers}}] -> first_attached(Consumers);
                [] -> undefined
            end;
        ClientId ->
            ClientId
    end.

wake_client(undefined, _Msg) ->
    ok;
wake_client(ClientId, Msg) ->
    case ets:lookup(?ATTACHMENTS, ClientId) of
        [{_, #{pid := Pid}}] -> Pid ! Msg, ok;
        [] -> ok
    end.

entity_qos(SubRef) ->
    case ets:lookup(?ENTITIES, SubRef) of
        [{_, #{qos := QoS}}] -> QoS;
        [] -> 1
    end.

store_row(SubRef, P, Row, State) ->
    true = ets:insert(?CURSORS, {{SubRef, P}, Row}),
    mark_cursor_dirty(SubRef, State).

%% Retry state lives in the cursor row, so it reaches `.cursors' through the
%% same flush window as the cursor itself (E-2). A crash may therefore lose the
%% last window of attempts, which only means *more* retries -- the safe
%% direction (ADR-015 §15.6).
mark_cursor_dirty(SubRef, State) ->
    #{cursor_dirty := Dirty} = State,
    State#{cursor_dirty := Dirty#{SubRef => maps:get(SubRef, Dirty, 0)}}.

now_ms() ->
    %% System time, not monotonic: `next_at' is persisted and must still mean
    %% something after a VM restart.
    erlang:system_time(millisecond).

log_info(Fmt, Args) -> logger:log(info, "broker_subscription: " ++ Fmt, Args).
log_warning(Fmt, Args) -> logger:log(warning, "broker_subscription: " ++ Fmt, Args).

%%%_* failover active election ------------------------------------------------

%% The consumer list is the order; the active is the first member with a live
%% attachment (ADR-019 §19.4). No separate membership table, and no `failover'
%% state on disk: a standby that dies is simply skipped, and the next reconnect
%% joins at the tail.
refresh_all_active(SubRefs, State) ->
    lists:foldl(fun(SubRef, Acc) -> refresh_active(SubRef, Acc) end, State, SubRefs).

refresh_active(SubRef, State) ->
    case ets:lookup(?ENTITIES, SubRef) of
        [{_, #{kind := failover, consumers := Consumers}}] ->
            #{active_of := Active} = State,
            Old = maps:get(SubRef, Active, undefined),
            New = first_attached(Consumers),
            case {Old, New} of
                {undefined, _} ->
                    State#{active_of := Active#{SubRef => New}};
                {_, Old} ->
                    State;
                {_, _} ->
                    broker_metrics:inc(failover_promotions),
                    State#{active_of := Active#{SubRef => New}}
            end;
        _ ->
            State
    end.

first_attached([C | Rest]) ->
    case ets:lookup(?ATTACHMENTS, C) of
        [{_, #{pid := Pid}}] ->
            case is_process_alive(Pid) of
                true -> C;
                false -> first_attached(Rest)
            end;
        [] ->
            first_attached(Rest)
    end;
first_attached([]) ->
    undefined.

%%%_* inflight (tier 3) ---------------------------------------------------------

mark_inflight_dirty(ClientId, State) ->
    #{inflight_dirty := Dirty} = State,
    State#{inflight_dirty := Dirty#{ClientId => true}}.

maybe_flush_inflight(ClientId, #{inflight_interval := 0} = State) ->
    flush_client_inflight(ClientId, State);
maybe_flush_inflight(_ClientId, State) ->
    State.

flush_client_inflight(ClientId, State) ->
    #{inflight_dirty := Dirty} = State,
    case ets:lookup(?INFLIGHT, ClientId) of
        [{_, Term}] ->
            case broker_subscription_files:write_inflight(ClientId, Term) of
                ok -> State#{inflight_dirty := maps:remove(ClientId, Dirty)};
                {error, _Reason} -> State
            end;
        [] ->
            State#{inflight_dirty := maps:remove(ClientId, Dirty)}
    end.

flush_all_dirty_inflight(State) ->
    #{inflight_dirty := Dirty} = State,
    lists:foldl(fun(ClientId, Acc) -> flush_client_inflight(ClientId, Acc) end,
                State, maps:keys(Dirty)).

%%%_* attachments ---------------------------------------------------------------

%% Pid-guarded (K10): a takeover replaces the attachment row, and the old
%% connection's later detach/DOWN then matches nothing here -- evicting the new
%% attachment because an *old* socket closed would log the survivor out.
do_detach(ClientId, Pid, State) ->
    case ets:lookup(?ATTACHMENTS, ClientId) of
        [{_, #{pid := Pid, ref := Ref, persist := Persist}}] ->
            _ = erlang:demonitor(Ref, [flush]),
            true = ets:delete(?ATTACHMENTS, ClientId),
            State1 = release_member_claims(ClientId, State),
            State2 = refresh_all_active(subs_of(ClientId), State1),
            case Persist of
                true -> {ok, State2};
                false -> {ok, teardown_session(ClientId, State2)}
            end;
        _ ->
            {ok, State}
    end.

%% The offsets this connection was holding return to the pool the moment it is
%% gone, whether or not its session survives: a claim is a lease tied to a live
%% attachment, and an offset held by a dead reader would pin `low' forever
%% (ADR-019 §19.2 -- this is also why S2-D needs no retry timer).
release_member_claims(ClientId, State) ->
    lists:foreach(
      fun({SubRef, _}) ->
              lists:foreach(
                fun({{S, P}, Row}) when S =:= SubRef ->
                        case maps:get(kind, Row, undefined) of
                            shared ->
                                true = ets:insert(?CURSORS,
                                                  {{S, P}, broker_cursor:release(Row, ClientId)});
                            _ ->
                                ok
                        end;
                   (_) ->
                        ok
                end, ets:match_object(?CURSORS, {{SubRef, '_'}, '_'}))
      end, entities_of(ClientId)),
    State.

%% clean_session teardown (§4.2): disk first, then Router, then memory, so a
%% crash mid-teardown restarts with *less* than it had, never a session whose
%% files the client believes deleted. Exclusive subscriptions are deleted
%% outright; group memberships only lose this consumer (§19.2).
teardown_session(ClientId, State) ->
    Removed = lists:foldl(
                fun({SubRef, Info}, Acc) ->
                        case teardown_one(SubRef, Info, ClientId) of
                            removed -> [SubRef | Acc];
                            kept -> Acc
                        end
                end, [], entities_of(ClientId)),
    true = ets:delete(?INFLIGHT, ClientId),
    ok = broker_subscription_files:delete_inflight(ClientId),
    #{cursor_dirty := CD, inflight_dirty := ID} = State,
    %% `cursor_dirty' is SubRef-keyed (mark_cursor_dirty/4); a torn-down
    %% subscription must drop its key here, otherwise the next cursor_tick
    %% would try to flush a cursor file for an entity that no longer exists.
    %% `inflight_dirty' is ClientId-keyed, so that one removes by ClientId.
    State#{cursor_dirty := lists:foldl(fun(S, Acc) -> maps:remove(S, Acc) end, CD, Removed),
           inflight_dirty := maps:remove(ClientId, ID)}.

%% Returns `removed' when the entity was deleted outright, `kept' when a shared
%% subscription survives with a reduced consumer list (its cursor is still live).
teardown_one(SubRef, #{kind := exclusive}, _ClientId) ->
    ok = broker_subscription_files:delete_subscription(SubRef),
    _ = broker_router:unregister(SubRef),
    remove_local(SubRef),
    removed;
teardown_one(SubRef, #{consumers := Consumers} = Info, ClientId) ->
    case lists:delete(ClientId, Consumers) of
        [] ->
            ok = broker_subscription_files:delete_subscription(SubRef),
            _ = broker_router:unregister(SubRef),
            remove_local(SubRef),
            removed;
        Rest ->
            ok = persist_entity(SubRef, Info#{consumers => Rest}),
            true = ets:insert(?ENTITIES, {SubRef, Info#{consumers => Rest}}),
            kept
    end.

remove_local(SubRef) ->
    true = ets:delete(?ENTITIES, SubRef),
    lists:foreach(fun({{S, P}, _Row}) -> true = ets:delete(?CURSORS, {S, P}) end,
                  ets:match_object(?CURSORS, {{SubRef, '_'}, '_'})).

drop_dirty(SubRef, State) ->
    #{cursor_dirty := CD, inflight_dirty := ID} = State,
    State#{cursor_dirty := maps:remove(SubRef, CD),
           inflight_dirty := ID}.

subs_of(ClientId) ->
    [SubRef || {{C, SubRef}} <- ets:match_object(?MEMBERS, {{ClientId, '_'}}), C =:= ClientId].

entities_of(ClientId) ->
    [{SubRef, Info} || SubRef <- subs_of(ClientId),
                       {_, Info} <- ets:lookup(?ENTITIES, SubRef)].

%%%_* Tables, timers ------------------------------------------------------------

create_tables() ->
    lists:foreach(fun create_protected_table/1,
                  [?ENTITIES, ?MEMBERS, ?CURSORS, ?ATTACHMENTS, ?INFLIGHT]).

create_protected_table(Name) ->
    _ = ets:new(Name, [named_table, set, protected, {read_concurrency, true}]).

rearm(cursor_tick, State = #{cursor_interval := Ms}) when Ms > 0 ->
    _ = erlang:send_after(Ms, self(), cursor_tick),
    State;
rearm(retry_tick, State = #{retry_interval := Ms}) when Ms > 0 ->
    _ = erlang:send_after(Ms, self(), retry_tick),
    State;
rearm(inflight_tick, State = #{inflight_interval := Ms}) when Ms > 0 ->
    _ = erlang:send_after(Ms, self(), inflight_tick),
    State;
rearm(_Tick, State) ->
    State.
