%% @doc Subscriptions, per-subscription cursors, delivery wakeups and the
%% three-tier session persistence of design §5.3.
%%
%% Owns the truth of *where every subscriber is in every partition's log* --
%% which is exactly what `broker_storage' is forbidden to know (ADR-011 §11.3:
%% storage owns the global committed watermark only). The split makes H11
%% mechanical: one subscription of a hash-partitioned broker spans every
%% partition (`sport/#' was measured hitting [0,1,3] at N=4), so its position is
%% a map of cursors, never a scalar.
%%
%% **ADR-019 changed the identity of a subscription.** It is no longer
%% `{ClientId, Filter}' but `{Kind, Key, Filter}' (§2.1 of the execution plan):
%%
%%  * `{exclusive, ClientId, Filter}' -- one consumer, MQTT 3.1.1 SUBSCRIBE.
%%  * `{shared, Group, Filter}'    -- N consumers, each message to exactly one.
%%  * `{failover, Group, Filter}'  -- N consumers, one active, the rest standby.
%%
%% The Key is the ClientId for `exclusive' and the Group name otherwise, so the
%% tuple is still deterministic and still its own reverse index -- boot re-mints
%% exactly the SubRefs the files describe, and `dispatch/2' can go from a SubRef
%% back to "who is attached" without asking anybody. What it can *not* answer is
%% "which shared member gets this offset": that is a serial decision and belongs
%% to the worker's `claim/4' (ADR-019 §19.1).
%%
%% The frozen-role checklist this module sits on:
%%  * §3.2 【F5】: SubRef is minted here and the Router never dereferences it --
%%    `route/3' hands the same opaque tuples back and `dispatch/2' resolves them
%%    against **this** module's tables.
%%  * §6 boot steps 2+3: the subscription entities, `?MEMBERS' and the Router
%%    index are rebuilt from `.subs' inside the worker's `init/1', all-or-nothing,
%%    before any listener can accept a SUBSCRIBE (step 4 is T12's).
%%  * J6: notifications and `dispatch/2' run in the caller's process; nothing on
%%    the publish path takes a gen_server hop (§3.2 【F3】).
%%  * K6: the committed watermark is `min(all live cursors) - 1`, advanced only
%%    after the cursor files that produced it reached the disk.
%%
%% Reads (`cursor/2', `subscriptions/1', `dispatch/2', `role/2') run in the
%% caller's process against `protected' tables -- the same non-serialization
%% point §3.2【F3】 froze for `route/3' and J1 froze for `append/3'. Only state
%% changes go through `broker_subscription_worker'.
-module(broker_subscription).

-include_lib("broker_core/include/broker_message.hrl").

-export([subscribe/3, join/5, unsubscribe/2, leave/2,
         ack/4, claim/4, release/4, retry/4,
         inflight/2, attach/3, detach/2, dispatch/2]).
-export([subscriptions/1, cursor/2, session/1, role/2]).

-export_type([sub_ref/0, sub_kind/0, cursors/0, subscription/0]).

-type sub_kind() :: exclusive | shared | failover.
-type sub_ref()  :: {sub_kind(), Key :: binary(), Filter :: binary()}.
-type cursors()  :: [{partition_id(), offset()}].
-type subscription() :: #{sub_ref := sub_ref(),
                          kind := sub_kind(),
                          key := binary(),
                          filter := binary(),
                          qos := 0..2,
                          cursors := cursors()}.

-define(ENTITIES, broker_subscription_entities).
-define(MEMBERS, broker_subscription_members).
-define(CURSORS, broker_subscription_cursors).
-define(ATTACHMENTS, broker_subscription_attachments).
-define(INFLIGHT, broker_subscription_inflight).

%%%_* Lifecycle -----------------------------------------------------------

%% @doc Register an exclusive subscription: mint the SubRef, fail fast in the
%% Router (a refused register leaves nothing written anywhere), anchor one cursor
%% per configured partition at that partition's live tail (a fresh MQTT
%% subscription must not replay history, §4.6), then fsync `.subs' before the
%% in-memory rows exist (R1b, zero tolerance -- the file carries the init
%% anchors, K5, so the origin is durable with the set).
%% Re-subscribing the same filter updates the QoS and re-persists; the Router
%% entry is already identical (same tuple under the same filter), so it is not
%% touched -- no unregister/register window (K2).
-spec subscribe(binary(), binary(), 0..2) -> {ok, sub_ref()} | {error, term()}.
subscribe(ClientId, Filter, QoS) ->
    broker_subscription_worker:subscribe(ClientId, Filter, QoS).

%% @doc Join a Shared/Failover group (ADR-019 §19.4). The SubRef is
%% `{Kind, Group, Filter}': it exists -> the consumer is appended to the end of
%% the member list; it does not -> it is created with this consumer as the first
%% member. Appending, never inserting: the member order is the failover active
%% election, and a rejoining member must not displace whoever took over.
-spec join(sub_kind(), binary(), binary(), binary(), 0..2) ->
          {ok, sub_ref()} | {error, term()}.
join(Kind, Group, ClientId, Filter, QoS) ->
    broker_subscription_worker:join(Kind, Group, ClientId, Filter, QoS).

%% @doc Drop an **exclusive** subscription: unregister from the Router, delete
%% the entity and its cursors, remove the files. Idempotent (MQTT §4.10:
%% UNSUBSCRIBE for a nonexistent filter is a no-op, and UNSUBSCRIBE races
%% disconnects).
-spec unsubscribe(binary(), binary()) -> ok.
unsubscribe(ClientId, Filter) ->
    broker_subscription_worker:unsubscribe(ClientId, Filter).

%% @doc Remove one consumer from a group subscription. The last one to leave
%% deletes the SubRef entirely -- entity, cursors, Router entry and files
%% (ADR-019 §19.2): a group nobody consumes is not a subscription, and keeping it
%% would leave `min(cursor)' pinned by a phantom member.
-spec leave(sub_ref(), binary()) -> ok.
leave(SubRef, ClientId) ->
    broker_subscription_worker:leave(SubRef, ClientId).

%%%_* Consumption ---------------------------------------------------------

%% @doc `Consumer has finished handling Offset in PartitionId' -- delivered-and-
%% acked or read-and-skipped (non-matching traffic must still advance the
%% cursor, or the committed watermark would pin at the first unrelated message
%% forever).
%%
%% The consumer is named because the answer depends on it (ADR-019 §19.2): for a
%% shared row the ack must match the member currently holding the offset, or the
%% call answers `ok' and changes nothing (it counts `shared_ack_unowned'). A
%% mismatched ack is a rebalance in flight, not a protocol error -- the acker's
%% view may lag by one rebalance, and killing its connection would turn a
%% bookkeeping race into an outage.
%%
%% Durability is the §5.3 batched tier: the in-memory move is immediate, the file
%% follows within the configured window.
-spec ack(sub_ref(), partition_id(), offset(), binary()) -> ok.
ack(SubRef, PartitionId, Offset, Consumer) ->
    broker_subscription_worker:ack(SubRef, PartitionId, Offset, Consumer).

%% @doc Take up to `Max' unassigned offsets of a **shared** subscription
%% (ADR-019 §19.1). The worker is the serializer: it reads the partition's high
%% watermark, asks `broker_cursor:claim/5' for the next unassigned page and
%% records `offset -> Consumer'. Answers:
%%
%%  * `{ok, [Offset]}' -- ascending, possibly non-contiguous (holes are offsets
%%    acked out of order or held by another member). `{ok, []}' is the tail.
%%  * `{error, overflow}' -- `shared_state_max' reached; the caller backs off, it
%%    never evicts a live assignment.
%%  * `{error, {watermark, P, Reason}}' -- the caller degrades to `idle' this
%%    round and retries on the poll tick.
%%  * `{error, wrong_kind}' -- not a shared subscription.
-spec claim(sub_ref(), partition_id(), pos_integer(), binary()) ->
          {ok, [offset()]} | {error, term()}.
claim(SubRef, PartitionId, Max, Consumer) ->
    broker_subscription_worker:claim(SubRef, PartitionId, Max, Consumer).

%% @doc Hand claimed offsets back to the pool (ADR-019 §19.5). The pull loop
%% calls this when the page it read does not contain everything the claim named
%% -- the storage `High' watermark reserves an offset before the append that
%% makes it readable has landed. Ownership-checked, so an ack that raced the
%% rollback still wins.
-spec release(sub_ref(), partition_id(), [offset()], binary()) -> ok.
release(SubRef, PartitionId, Offsets, Consumer) ->
    broker_subscription_worker:release(SubRef, PartitionId, Offsets, Consumer).

%% @doc One delivery attempt timed out (ADR-015 §15.1, S2-E E-1). The connection
%% reports; the worker owns the backoff clock, the attempt budget and the
%% dead-letter decision, because that state must outlive the connection.
-spec retry(sub_ref(), partition_id(), offset(), binary()) -> ok.
retry(SubRef, PartitionId, Offset, Consumer) ->
    broker_subscription_worker:retry(SubRef, PartitionId, Offset, Consumer).

%% @doc Persist the conn_fsm's opaque session blob (packet_id allocator +
%% in-flight set, §5.3 tier 3). This layer never inspects it -- that is R7's
%% point: the recovery answer may be stale, which means duplicates, which MQTT
%% 3.1.1 §4.1 already allows. Still keyed by client (ADR-019 §19.3).
-spec inflight(binary(), term()) -> ok.
inflight(ClientId, Term) ->
    broker_subscription_worker:inflight(ClientId, Term).

%% @doc Bind the live connection process to the client's session. `Persist'
%% mirrors clean_session (§4.2): on detach, false tears the whole session down
%% (entities, Router index, files), true keeps everything for the reconnect
%% that will find the cursors waiting. Answers the previously attached pid
%% (MQTT §4.3 takeover — the new conn tells it to close; `undefined' when the
%% same pid re-attaches, so nobody is ever asked to kill themselves).
-spec attach(binary(), pid(), boolean()) -> {ok, pid() | undefined}.
attach(ClientId, Pid, Persist) ->
    broker_subscription_worker:attach(ClientId, Pid, Persist).

-spec detach(binary(), pid()) -> ok.
detach(ClientId, Pid) ->
    broker_subscription_worker:detach(ClientId, Pid).

%% §5.1(f), reinterpreted per K9: push for QoS0 (which never enters storage, so
%% there is nothing to pull from), and a *redundant* wakeup for QoS1/2 -- the
%% subscriber's real read position is the cursor (J6). Runs in the caller's
%% process: the publish path must not serialize through this module any more
%% than through the Router.
%%
%% Recipients (ADR-019 §19.4, plan §2.5):
%%  * Exclusive: the owning client's attachment.
%%  * Failover: the *active* member -- the standby receives nothing at all.
%%  * Shared, positional (`{P, O, Msg}', a durable message's wake): **every**
%%    attached member. The wake is only a hint; `claim/4' decides who actually
%%    reads the offset, and telling one member would stall the group whenever
%%    that member is slow.
%%  * Shared, non-positional (`{undefined, undefined, Msg}', a QoS0 push): the
%%    single lowest-ordered attached member, because there is no offset to claim
%%    and pushing to everyone would deliver N copies.
%%
%% Recipients are merged per client (H7): a client whose `sport/#' and
%% `sport/tennis/+' both matched gets ONE frame, at the max QoS of the hits. The
%% shape sent is T9's contract:
%% `{broker_deliver, ClientId, MaxQoS, [SubRef], {PartitionId, Offset, Msg}}'.
-spec dispatch([sub_ref()], {partition_id() | undefined,
                             offset() | undefined,
                             #broker_message{}}) -> ok.
dispatch(SubRefs, {PartitionId, _Offset, _Msg} = Delivery) ->
    Positional = PartitionId =/= undefined,
    lists:foreach(
      fun({ClientId, {MaxQoS, Matched}}) ->
              case ets:lookup(?ATTACHMENTS, ClientId) of
                  [{_, #{pid := Pid}}] ->
                      Pid ! {broker_deliver, ClientId, MaxQoS, Matched, Delivery};
                  [] ->
                      ok
              end
      end,
      maps:to_list(group_by_consumer(SubRefs, Positional))),
    ok.

%%%_* Reads (caller process, no process hop) --------------------------------

%% @doc Every subscription this client consumes -- across all three kinds -- with
%% its cursor per partition. The dispatcher's starting point after an attach or a
%% reconnect (§5.2 catch-up).
-spec subscriptions(binary()) -> [subscription()].
subscriptions(ClientId) ->
    [subscription(SubRef, Info)
     || {{C, SubRef}} <- ets:match_object(?MEMBERS, {{ClientId, '_'}}),
        C =:= ClientId,
        (Info = entity(SubRef)) =/= none].

%% @doc The next offset this subscription will read in a partition: `low' for a
%% shared row (the first offset nobody acked), the scalar cursor otherwise.
%% `unknown_subscription' distinguishes "this sub is gone" from "at the tail": a
%% cursor that merely caught up still reads here, an entity dropped by
%% UNSUBSCRIBE or clean-session teardown does not (§5.2 note 3).
-spec cursor(sub_ref(), partition_id()) -> {ok, offset()} | {error, term()}.
cursor(SubRef, PartitionId) ->
    case entity(SubRef) of
        none ->
            {error, {unknown_subscription, SubRef}};
        _ ->
            case ets:lookup(?CURSORS, {SubRef, PartitionId}) of
                [{_, Row}] -> {ok, broker_cursor:next_read(Row)};
                [] -> {error, {no_cursor_for_partition, SubRef, PartitionId}}
            end
    end.

%% @doc This client's role in a subscription (ADR-019 §19.4). Exclusive and
%% shared members are always `active'; a failover member is `active' only while
%% it is the first member with a live attachment. `not_consumer' is "you are not
%% in this group", which the pull loop reads as "skip this SubRef".
-spec role(sub_ref(), binary()) -> active | standby | not_consumer.
role(SubRef, ClientId) ->
    case entity(SubRef) of
        #{kind := Kind, key := Key, consumers := Consumers} ->
            case lists:member(ClientId, Consumers) of
                false -> not_consumer;
                true ->
                    case Kind of
                        failover ->
                            case first_attached(Consumers) of
                                ClientId -> active;
                                _ -> standby
                            end;
                        _ ->
                            _ = Key,
                            active
                    end
            end;
        none ->
            not_consumer
    end.

%% @doc The session view a reconnecting conn_fsm asks for: subscription set,
%% last flushed in-flight blob, attachment status. A client that never
%% subscribed, never flushed an in-flight blob and holds no connection has no
%% session at all -- `not_found', not an empty map that reads as "resumable".
-spec session(binary()) -> {ok, map()} | {error, not_found}.
session(ClientId) ->
    Subs = subscriptions(ClientId),
    Inflight = case ets:lookup(?INFLIGHT, ClientId) of
                   [{_, Term}] -> Term;
                   [] -> undefined
               end,
    Attached = ets:lookup(?ATTACHMENTS, ClientId),
    Online = case Attached of
                 [{_, #{pid := Pid}}] -> is_process_alive(Pid);
                 [] -> false
             end,
    case {Subs, Inflight, Attached} of
        {[], undefined, []} -> {error, not_found};
        _ ->
            {ok, #{client_id => ClientId,
                   subs => Subs,
                   inflight => Inflight,
                   online => Online}}
    end.

%%%_* Internal (read path) --------------------------------------------------

entity(SubRef) ->
    case ets:lookup(?ENTITIES, SubRef) of
        [{_, Info}] -> Info;
        [] -> none
    end.

subscription(SubRef, #{kind := Kind, key := Key, filter := Filter, qos := QoS}) ->
    #{sub_ref => SubRef, kind => Kind, key => Key, filter => Filter, qos => QoS,
      cursors => cursors_for(SubRef)}.

%% Rows are `{{SubRef, PartitionId}, Row}'; match_object pins the SubRef and
%% returns whole rows, because map *contents* cannot be matched -- only the
%% `#{_ := V}' whole-value form can.
cursors_for(SubRef) ->
    [{PartitionId, broker_cursor:next_read(Row)}
     || {{_SubRef, PartitionId}, Row} <- ets:match_object(?CURSORS, {{SubRef, '_'}, '_'})].

%% H7: merge a client's overlapping hits into one delivery at the max QoS. The
%% recipient set depends on the kind, so this is where the SubRef list collapses
%% into client ids.
group_by_consumer(SubRefs, Positional) ->
    lists:foldl(fun(SubRef, Acc) -> group_step(SubRef, Positional, Acc) end, #{}, SubRefs).

group_step(SubRef, Positional, Acc) ->
    case entity(SubRef) of
        #{kind := Kind, qos := QoS, consumers := Consumers} ->
            add_recipients(recipients(Kind, Consumers, Positional), QoS, SubRef, Acc);
        none ->
            %% K8: an orphan Router entry resolves to nothing here, and the
            %% publisher must not pay for a bookkeeping gap in another layer.
            Acc
    end.

%% Positional wake vs. non-positional push, per §2.5 of the plan.
recipients(exclusive, [ClientId | _], _Positional) -> [ClientId];
recipients(failover, Consumers, _Positional) ->
    case first_attached(Consumers) of
        undefined -> [];
        ClientId -> [ClientId]
    end;
recipients(shared, Consumers, true) -> Consumers;
recipients(shared, Consumers, false) ->
    case first_attached(Consumers) of
        undefined -> [];
        ClientId -> [ClientId]
    end.

add_recipients([], _QoS, _SubRef, Acc) ->
    Acc;
add_recipients([ClientId | Rest], QoS, SubRef, Acc) ->
    {Max, Refs} = maps:get(ClientId, Acc, {undefined, []}),
    add_recipients(Rest, QoS, SubRef, Acc#{ClientId => {max_q(QoS, Max), [SubRef | Refs]}}).

max_q(QoS, undefined) -> QoS;
max_q(QoS, Max) -> erlang:max(QoS, Max).

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
