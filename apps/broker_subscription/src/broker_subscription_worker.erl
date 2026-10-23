%% @doc Single writer for the subscription tables, boot steps 2+3 of design §6,
%% and the batched flush tiers of §5.3.
%%
%% The shape mirrors what J4 froze for `broker_queue_store' and 【F3】 froze for
%% the Router index: four `protected' ETS tables owned by this one process --
%% every other process reads them lock-free (that is `cursor/2', `subscriptions/1',
%% `dispatch/2' in the facade), and only state *changes* are gen_server calls
%% here. A consumer's cursor read must not serialize behind a flush tick that is
%% fsyncing somebody else's file.
%%
%% What the calls actually decide, in order:
%%
%%  * `init/1' is boot steps 2+3 and all-or-nothing, the same stance T6 froze
%%    for partition recovery: a corrupt `.subs', an unreadable watermark or a
%%    Router that refuses the backfill stops the application loudly, because a
%%    half-loaded session set serves `not_found' for subscriptions that exist on
%%    disk (ADR-011 §11.2's silent-empty lie, one layer up).
%%  * subscribe (K2/K5): register in the Router **first** (a rejection fails
%%    while nothing was written anywhere), then fsync `.subs' carrying the init
%%    cursors, then insert the in-memory rows. A crash between the last two
%%    loses nothing: the disk copy is what boot reloads.
%%  * the flush tick (K6): dirty clients' `.cursors' reach the disk *before*
%%    `advance_committed' is called with min(cursor) - 1. Reversing the order
%%    would let a crash rewind a cursor below the committed watermark, and S2's
%%    truncation would then delete messages the subscriber never acknowledged.
-module(broker_subscription_worker).

-behaviour(gen_server).

-include_lib("broker_core/include/broker_message.hrl").

-export([start_link/0,
         subscribe/3, unsubscribe/2, ack/3, inflight/2, attach/3, detach/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(ENTITIES, broker_subscription_entities).
-define(CURSORS, broker_subscription_cursors).
-define(ATTACHMENTS, broker_subscription_attachments).
-define(INFLIGHT, broker_subscription_inflight).

%%%_* Client API -----------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

subscribe(ClientId, Filter, QoS) ->
    gen_server:call(?MODULE, {subscribe, ClientId, Filter, QoS}).

unsubscribe(ClientId, Filter) ->
    gen_server:call(?MODULE, {unsubscribe, ClientId, Filter}).

ack(SubRef, PartitionId, Offset) ->
    gen_server:call(?MODULE, {ack, SubRef, PartitionId, Offset}).

inflight(ClientId, Term) ->
    gen_server:call(?MODULE, {inflight, ClientId, Term}).

attach(ClientId, Pid, Persist) ->
    gen_server:call(?MODULE, {attach, ClientId, Pid, Persist}).

detach(ClientId, Pid) ->
    gen_server:call(?MODULE, {detach, ClientId, Pid}).

%%%_* gen_server -----------------------------------------------------------

init([]) ->
    create_tables(),
    Partitions = broker_queue:partition_ids(),
    State = #{partitions => Partitions,
              max_regress => application:get_env(broker_subscription, cursor_max_regress, 64),
              cursor_interval => application:get_env(broker_subscription, cursor_flush_interval_ms, 100),
              inflight_interval => application:get_env(broker_subscription, inflight_flush_interval_ms, 100),
              last_committed => #{},
              cursor_dirty => #{},
              inflight_dirty => #{}},
    case boot(Partitions) of
        {ok, LastCommitted} ->
            State1 = State#{last_committed := LastCommitted},
            {ok, rearm(inflight_tick, rearm(cursor_tick, State1))};
        {error, Reason} ->
            %% `{stop, Reason}' from init fails start_link, which fails the
            %% application: nobody can serve a subscription set that only partly
            %% loaded.
            {stop, {subscription_boot_failed, Reason}}
    end.

handle_call({subscribe, ClientId, Filter, QoS}, _From, State) ->
    SubRef = {ClientId, Filter},
    case ets:lookup(?ENTITIES, SubRef) of
        [_] ->
            %% Re-subscribe (MQTT replaces the QoS of a live filter): the
            %% Router entry is keyed by the same opaque tuple under the same
            %% filter, so it is already exact -- touching it would open an
            %% unregister/register window for zero effect (K2). The cursor is
            %% deliberately *not* re-anchored: a re-subscribe continues from
            %% where the subscription left off.
            reply_resubscribe(ClientId, SubRef, QoS, State);
        [] ->
            reply_new_subscribe(ClientId, Filter, QoS, State)
    end;

handle_call({unsubscribe, ClientId, Filter}, _From, State) ->
    SubRef = {ClientId, Filter},
    case ets:lookup(?ENTITIES, SubRef) of
        [] ->
            {reply, ok, State};
        [_] ->
            %% Disk first, then Router, then memory: a crash between persist
            %% and unregister leaves a Router orphan the dispatch path skips
            %% (K8), whereas unregistering a still-persisted subscription
            %% first would silently stop delivering a sub that reboot brings
            %% back. A failed persist changes nothing anywhere.
            Body = [Entry || Entry = {F, _QoS, _Init} <- subs_body(ClientId), F =/= Filter],
            case broker_subscription_files:write_subs(ClientId, Body) of
                ok ->
                    _ = broker_router:unregister(SubRef),
                    remove_local(SubRef),
                    {reply, ok, State};
                {error, Reason} ->
                    {reply, {error, {persist_subs, Reason}}, State}
            end
    end;

handle_call({ack, SubRef = {ClientId, _Filter}, PartitionId, Offset}, _From, State) ->
    State1 = case ets:lookup(?CURSORS, {SubRef, PartitionId}) of
                 [] ->
                     %% Unknown sub or unknown partition: idempotent ok. The
                     %% acker is a conn_fsm whose view may lag a UNSUBSCRIBE by
                     %% one message; failing it would turn a race into a
                     %% protocol error.
                     State;
                 [{_, Row}] ->
                     New = advance_row(Row, Offset),
                     Delta = maps:get(offset, New) - maps:get(offset, Row),
                     true = ets:insert(?CURSORS, {{SubRef, PartitionId}, New}),
                     absorb_advance(ClientId, Delta, State)
             end,
    {reply, ok, State1};

handle_call({inflight, ClientId, Term}, _From, State) ->
    true = ets:insert(?INFLIGHT, {ClientId, Term}),
    State1 = mark_inflight_dirty(ClientId, State),
    {reply, ok, maybe_flush_inflight(ClientId, State1)};

handle_call({attach, ClientId, Pid, Persist}, _From, State) ->
    true = ets:insert(?ATTACHMENTS,
                      {ClientId, #{pid => Pid,
                                   ref => erlang:monitor(process, Pid),
                                   persist => Persist}}),
    {reply, ok, State};

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
    {noreply, rearm(cursor_tick, compute_committed(State1))};

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
    case {live_watermarks(Partitions), broker_subscription_files:list_sessions()} of
        {{ok, Watermarks}, {ok, Sessions}} ->
            LastCommitted = maps:from_list([{P, C} || {P, {C, _High}} <- Watermarks]),
            WM = maps:from_list([{P, High} || {P, {_C, High}} <- Watermarks]),
            case load_entities(Sessions, Partitions, WM, []) of
                {ok, Refs} ->
                    case backfill(Refs) of
                        ok -> {ok, LastCommitted};
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

%% Step 2: entities + cursors from disk. The rule per (SubRef, PartitionId):
%% `.cursors' if it carries the key, else the init anchor embedded in `.subs'
%% (K5) -- never `Committed', never 0, never a fresh High for a partition the
%% subscription already had (that would rewind past acks or skip past
%% un-acknowledged backlog). A partition missing from *both* files is one that
%% did not exist when the subscription was made; it anchors at the live tail,
%% which is what subscribing to it today would do.
load_entities([{ClientId, Entries} | Rest], Partitions, WM, Refs) ->
    case load_client(ClientId, Entries, Partitions, WM) of
        {ok, ClientRefs} -> load_entities(Rest, Partitions, WM, Refs ++ ClientRefs);
        {error, Reason} -> {error, {ClientId, Reason}}
    end;
load_entities([], _Partitions, _WM, Refs) ->
    {ok, Refs}.

load_client(ClientId, Entries, Partitions, WM) ->
    case {broker_subscription_files:read_cursors(ClientId),
          broker_subscription_files:read_inflight(ClientId)} of
        {{ok, Flushed0}, {ok, Inflight}} ->
            Flushed = cursors_map(ClientId, Flushed0),
            try
                Refs =
                    lists:map(fun({Filter, QoS, InitCursors}) ->
                                SubRef = {ClientId, Filter},
                                Init = maps:from_list(InitCursors),
                                true = ets:insert(?ENTITIES,
                                                  {SubRef, #{client_id => ClientId,
                                                             filter => Filter,
                                                             qos => QoS,
                                                             init_cursors => InitCursors}}),
                                lists:foreach(
                                  fun(P) ->
                                      Offset = cursor_at(SubRef, P, Flushed, Init, WM),
                                      true = ets:insert(?CURSORS,
                                                        {{SubRef, P}, #{offset => Offset,
                                                                        pending => []}})
                                  end, Partitions),
                                {SubRef, Filter}
                              end, Entries),
                _ = case Inflight of
                        none -> ok;
                        Term -> true = ets:insert(?INFLIGHT, {ClientId, Term})
                    end,
                {ok, Refs}
            catch
                Class:Reason -> {error, {load_failed, Class, Reason}}
            end;
        {{error, Reason}, _} ->
            {error, {cursors_file, Reason}};
        {_, {error, Reason}} ->
            {error, {inflight_file, Reason}}
    end.

cursors_map(_ClientId, none) ->
    #{};
cursors_map(ClientId, Body) ->
    maps:from_list([{{{ClientId, Filter}, P}, Offset}
                    || {Filter, Rows} <- Body, {P, Offset} <- Rows]).

cursor_at(SubRef, P, Flushed, Init, WM) ->
    case Flushed of
        #{{SubRef, P} := Offset} -> Offset;
        _ ->
            case Init of
                #{P := Offset} -> Offset;
                _ -> maps:get(P, WM, 0)
            end
    end.

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

%%%_* subscribe / unsubscribe -----------------------------------------------

reply_resubscribe(ClientId, SubRef, QoS, State) ->
    [{SubRef, Info}] = ets:lookup(?ENTITIES, SubRef),
    true = ets:insert(?ENTITIES, {SubRef, Info#{qos => QoS}}),
    case broker_subscription_files:write_subs(ClientId, subs_body(ClientId)) of
        ok ->
            {reply, {ok, SubRef}, State};
        {error, Reason} ->
            true = ets:insert(?ENTITIES, {SubRef, Info}),
            {reply, {error, {persist_subs, Reason}}, State}
    end.

reply_new_subscribe(ClientId, Filter, QoS, State) ->
    #{partitions := Partitions} = State,
    SubRef = {ClientId, Filter},
    %% Fail fast in the Router: a refused register (invalid filter, store
    %% down) ends here with nothing written anywhere, which is the only
    %% rollback that needs no code. `already_registered' means a K8 orphan
    %% of this very {ClientId, Filter} is still indexed -- adopt it.
    case broker_router:register(Filter, SubRef, #{qos => QoS}) of
        Result when Result =:= ok;
                    Result =:= {error, {already_registered, SubRef}} ->
            anchor_and_persist(ClientId, Filter, QoS, SubRef, Partitions, State);
        {error, Reason} ->
            {reply, {error, {router_register, Reason}}, State}
    end.

anchor_and_persist(ClientId, Filter, QoS, SubRef, Partitions, State) ->
    case tail_offsets(Partitions, []) of
        {ok, InitCursors} ->
            Body = subs_body(ClientId) ++ [{Filter, QoS, InitCursors}],
            case broker_subscription_files:write_subs(ClientId, Body) of
                ok ->
                    true = ets:insert(?ENTITIES,
                                      {SubRef, #{client_id => ClientId,
                                                 filter => Filter,
                                                 qos => QoS,
                                                 init_cursors => InitCursors}}),
                    lists:foreach(
                      fun({P, Offset}) ->
                              true = ets:insert(?CURSORS,
                                                {{SubRef, P}, #{offset => Offset,
                                                                pending => []}})
                      end, InitCursors),
                    {reply, {ok, SubRef}, State};
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

subs_body(ClientId) ->
    [{Filter, QoS, InitCursors}
     || {_SubRef, #{filter := Filter, qos := QoS, init_cursors := InitCursors}}
            <- entities_of(ClientId)].

entities_of(ClientId) ->
    ets:match_object(?ENTITIES, {{ClientId, '_'}, '_'}).

%%%_* ack / cursor aggregation ------------------------------------------------

%% Contiguous-prefix advance: an ack at the cursor moves it and cascades over
%% the buffered set; an ack beyond it buffers; an ack behind it is a duplicate
%% and changes nothing (MQTT acks can arrive out of order via different
%% packet_ids resolving in different orders).
advance_row(Row, Offset) ->
    Current = maps:get(offset, Row),
    Pending = maps:get(pending, Row),
    if
        Offset < Current -> Row;
        Offset =:= Current -> cascade(Current + 1, Pending, Row);
        true -> Row#{pending => lists:usort([Offset | Pending])}
    end.

cascade(Next, [Next | Rest], Row) ->
    cascade(Next + 1, Rest, Row);
cascade(Next, Pending, Row) ->
    Row#{offset => Next, pending => Pending}.

absorb_advance(_ClientId, 0, State) ->
    State;
absorb_advance(ClientId, Delta, State) ->
    #{cursor_dirty := Dirty, max_regress := MaxRegress, cursor_interval := Interval} = State,
    Count = maps:get(ClientId, Dirty, 0) + Delta,
    State1 = State#{cursor_dirty := Dirty#{ClientId => Count}},
    if
        %% The two §5.3 knobs meet here: the interval arms a timer, the count
        %% bounds how far a busy client can run ahead of its file. An interval
        %% of 0 selects the near-zero-regress tier: every advance flushes.
        Interval =:= 0, Count > 0 -> flush_client_cursors(ClientId, compute_committed(State1));
        Count >= MaxRegress -> flush_client_cursors(ClientId, compute_committed(State1));
        true -> State1
    end.

flush_client_cursors(ClientId, State) ->
    #{cursor_dirty := Dirty} = State,
    case broker_subscription_files:write_cursors(ClientId, cursors_body(ClientId)) of
        ok ->
            State#{cursor_dirty := maps:remove(ClientId, Dirty)};
        {error, _Reason} ->
            %% The in-memory cursor is still correct to read (a restart is the
            %% only consumer of the file); the tick retries. The regress bound
            %% degrades to "as good as the next tick", which is what the
            %% interval knob already promises.
            State
    end.

cursors_body(ClientId) ->
    lists:map(
      fun({SubRef, #{filter := Filter}}) ->
              {Filter, [{P, maps:get(offset, Row)}
                        || {{_SubRef, P}, Row} <- ets:match_object(?CURSORS,
                                                                   {{SubRef, '_'}, '_'})]}
      end,
      entities_of(ClientId)).

flush_all_dirty_cursors(State) ->
    #{cursor_dirty := Dirty} = State,
    lists:foldl(fun(ClientId, Acc) -> flush_client_cursors(ClientId, Acc) end,
                State, maps:keys(Dirty)).

%% K6: committed = min(live cursors) - 1, and only for partitions that have at
%% least one cursor (an S1 partition with no subscriber is pinned by nobody --
%% dropping to `none' and back on the first subscribe would regress the
%% watermark, which the storage guard would rightly refuse). `compute_committed'
%% must run after the cursor files: see the module doc.
compute_committed(State) ->
    #{last_committed := Last, partitions := Partitions} = State,
    Mins = ets:foldr(fun({{_SubRef, P}, #{offset := Offset}}, Acc) ->
                             maps:update_with(P, fun(Min) -> erlang:min(Min, Offset) end,
                                              Offset, Acc)
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
            case Persist of
                true -> {ok, State};
                false -> {ok, teardown_session(ClientId, State)}
            end;
        _ ->
            {ok, State}
    end.

%% clean_session teardown (§4.2): disk first, then Router, then memory, so a
%% crash mid-teardown restarts with *less* than it had, never a session whose
%% files the client believes deleted.
teardown_session(ClientId, State) ->
    ok = broker_subscription_files:delete_client(ClientId),
    lists:foreach(
      fun({SubRef, _Info}) ->
              _ = broker_router:unregister(SubRef),
              remove_local(SubRef)
      end, entities_of(ClientId)),
    true = ets:delete(?INFLIGHT, ClientId),
    #{cursor_dirty := CD, inflight_dirty := ID} = State,
    State#{cursor_dirty := maps:remove(ClientId, CD),
           inflight_dirty := maps:remove(ClientId, ID)}.

remove_local(SubRef) ->
    true = ets:delete(?ENTITIES, SubRef),
    lists:foreach(fun({{S, P}, _Row}) -> true = ets:delete(?CURSORS, {S, P}) end,
                  ets:match_object(?CURSORS, {{SubRef, '_'}, '_'})).

%%%_* Tables, timers ------------------------------------------------------------

create_tables() ->
    lists:foreach(fun create_protected_table/1,
                  [?ENTITIES, ?CURSORS, ?ATTACHMENTS, ?INFLIGHT]).

create_protected_table(Name) ->
    _ = ets:new(Name, [named_table, set, protected, {read_concurrency, true}]).

rearm(cursor_tick, State = #{cursor_interval := Ms}) when Ms > 0 ->
    _ = erlang:send_after(Ms, self(), cursor_tick),
    State;
rearm(inflight_tick, State = #{inflight_interval := Ms}) when Ms > 0 ->
    _ = erlang:send_after(Ms, self(), inflight_tick),
    State;
rearm(_Tick, State) ->
    State.
