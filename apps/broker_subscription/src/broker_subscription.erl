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
%% The frozen-role checklist this module sits on:
%%  * §3.2 【F5】: SubRef is minted here (`{ClientId, Filter}', K2) and the
%%    Router never dereferences it -- `route/3' hands the same opaque tuples
%%    back and `dispatch/2' resolves them against **this** module's tables.
%%  * §6 boot steps 2+3: the subscription entities and the Router index are
%%    rebuilt from `.subs' inside the worker's `init/1', all-or-nothing, before
%%    any listener can accept a SUBSCRIBE (step 4 is T12's).
%%  * J6: notifications and `dispatch/2' wakes are hints; `cursor/2' is the only
%%    read position. The dispatcher half (T9's conn_fsm) must read from
%%    `cursor/2', never from a wakeup's range.
%%  * K6: the committed watermark is `min(all live cursors) - 1`, advanced only
%%    after the cursor files that produced it reached the disk.
%%
%% Reads (`cursor/2', `subscriptions/1', `dispatch/2') run in the caller's
%% process against `protected' tables -- the same non-serialization point
%% §3.2【F3】 froze for `route/3' and J1 froze for `append/3'. Only state
%% changes go through `broker_subscription_worker'.
-module(broker_subscription).

-include_lib("broker_core/include/broker_message.hrl").

-export([subscribe/3, unsubscribe/2, ack/3,
         inflight/2, attach/3, detach/2, dispatch/2]).
-export([subscriptions/1, cursor/2, session/1]).

-export_type([sub_ref/0, cursors/0, subscription/0]).

%% `{ClientId, Filter}' (K2): deterministic, so boot step 2 re-mints exactly the
%% SubRefs the `.subs' files describe; and the tuple is its own reverse index,
%% which is what lets H7 (merge a client's overlapping filters) happen here,
%% where dereferencing is legal, instead of in the Router, where it is not
%% (ADR-010 §10.2).
-type sub_ref()  :: {ClientId :: binary(), Filter :: binary()}.
-type cursors()  :: [{partition_id(), offset()}].
-type subscription() :: #{sub_ref := sub_ref(),
                          filter := binary(),
                          qos := 0..1,
                          cursors := cursors()}.

%%%_* Lifecycle -----------------------------------------------------------

%% @doc Register a subscription: mint the SubRef, fail fast in the Router
%% (a refused register leaves nothing written anywhere), anchor one cursor per
%% configured partition at that partition's live tail (a fresh MQTT
%% subscription must not replay history, §4.6), then fsync the client's
%% `.subs' before the in-memory rows exist (R1b, zero tolerance -- the file
%% carries the init anchors, K5, so the origin is durable with the set).
%% Re-subscribing the same filter updates the QoS and re-persists; the Router
%% entry is already identical (same tuple under the same filter), so it is not
%% touched -- no unregister/register window (K2).
-spec subscribe(binary(), binary(), 0..1) -> {ok, sub_ref()} | {error, term()}.
subscribe(ClientId, Filter, QoS) ->
    broker_subscription_worker:subscribe(ClientId, Filter, QoS).

%% @doc Drop a subscription: unregister from the Router, delete the entity and
%% its cursors, re-persist `.subs'. Idempotent (MQTT §4.10: UNSUBSCRIBE for a
%% nonexistent filter is a no-op, and UNSUBSCRIBE races disconnects).
-spec unsubscribe(binary(), binary()) -> ok.
unsubscribe(ClientId, Filter) ->
    broker_subscription_worker:unsubscribe(ClientId, Filter).

%%%_* Consumption ---------------------------------------------------------

%% @doc `SubRef has finished handling Offset in PartitionId' -- delivered-and-
%% acked or read-and-skipped (non-matching traffic must still advance the
%% cursor, or the committed watermark would pin at the first unrelated message
%% forever). The cursor moves over the contiguous acked prefix only; out-of-
%% order acks buffer. Durability is the §5.3 batched tier: the in-memory move
%% is immediate, the file follows within the configured window.
-spec ack(sub_ref(), partition_id(), offset()) -> ok.
ack(SubRef, PartitionId, Offset) ->
    broker_subscription_worker:ack(SubRef, PartitionId, Offset).

%% @doc Persist the conn_fsm's opaque session blob (packet_id allocator +
%% in-flight set, §5.3 tier 3). This layer never inspects it -- that is R7's
%% point: the recovery answer may be stale, which means duplicates, which MQTT
%% 3.1.1 §4.1 already allows.
-spec inflight(binary(), term()) -> ok.
inflight(ClientId, Term) ->
    broker_subscription_worker:inflight(ClientId, Term).

%% @doc Bind the live connection process to the client's session. `Persist'
%% mirrors clean_session (§4.2): on detach, false tears the whole session down
%% (entities, Router index, files), true keeps everything for the reconnect
%% that will find the cursors waiting.
-spec attach(binary(), pid(), boolean()) -> ok.
attach(ClientId, Pid, Persist) ->
    broker_subscription_worker:attach(ClientId, Pid, Persist).

-spec detach(binary(), pid()) -> ok.
detach(ClientId, Pid) ->
    broker_subscription_worker:detach(ClientId, Pid).

%% §5.1(f), reinterpreted per K9: push for QoS0 (which never enters storage, so
%% there is nothing to pull from), and a *redundant* wakeup for QoS1 -- the
%% subscriber's real read position is `cursor/2' (J6). Runs in the caller's
%% process: the publish path must not serialize through this module any more
%% than through the Router. Delivery is deduplicated per client (H7): a client
%% whose `sport/#' and `sport/tennis/+' both matched gets ONE message, at the
%% max QoS of the matching subs -- the merge the Router is forbidden to do.
%% The shape sent to the attached pid is T9's protocol-side contract:
%% `{broker_deliver, ClientId, MaxQoS, [SubRef], {PartitionId, Offset, Msg}}'
%% (QoS0 pushes carry `undefined' position slots; a QoS1 wake may be ignored
%% entirely -- the cursor is the truth, J6).
-spec dispatch([sub_ref()], {partition_id() | undefined,
                             offset() | undefined,
                             #broker_message{}}) -> ok.
dispatch(SubRefs, {PartitionId, Offset, Msg}) ->
    lists:foreach(
      fun({ClientId, {MaxQoS, Matched}}) ->
              case ets:lookup(broker_subscription_attachments, ClientId) of
                  [{_, #{pid := Pid}}] ->
                      Pid ! {broker_deliver, ClientId, MaxQoS, Matched,
                             {PartitionId, Offset, Msg}};
                  [] ->
                      ok
              end
      end,
      maps:to_list(group_by_client(SubRefs))),
    ok.

%%%_* Reads (caller process, no process hop) --------------------------------

%% @doc The subscription entities of a client with their live cursors -- the
%% dispatcher's starting point after an attach or a reconnect (§5.2 catch-up).
-spec subscriptions(binary()) -> [subscription()].
subscriptions(ClientId) ->
    lists:flatmap(
      fun({SubRef, #{qos := QoS}}) -> [subscription(SubRef, QoS)] end,
      ets:match_object(broker_subscription_entities, {{ClientId, '_'}, '_'})).

%% @doc The next offset this subscription will read in a partition. `unknown_
%% subscription' distinguishes "this sub is gone" from "at the tail": a cursor
%% that merely caught up still reads here, an entity dropped by UNSUBSCRIBE or
%% clean-session teardown does not (§5.2 note 3).
-spec cursor(sub_ref(), partition_id()) -> {ok, offset()} | {error, term()}.
cursor({ClientId, _Filter} = SubRef, PartitionId) when is_binary(ClientId) ->
    case ets:lookup(broker_subscription_entities, SubRef) of
        [_Entity] ->
            case ets:lookup(broker_subscription_cursors, {SubRef, PartitionId}) of
                [{_, #{offset := Offset}}] -> {ok, Offset};
                [] -> {error, {no_cursor_for_partition, SubRef, PartitionId}}
            end;
        [] ->
            {error, {unknown_subscription, SubRef}}
    end.

%% @doc The session view a reconnecting conn_fsm asks for: subscription set,
%% last flushed in-flight blob, attachment status. A client that never
%% subscribed, never flushed an in-flight blob and holds no connection has no
%% session at all -- `not_found', not an empty map that reads as "resumable".
-spec session(binary()) -> {ok, map()} | {error, not_found}.
session(ClientId) ->
    Subs = subscriptions(ClientId),
    Inflight = case ets:lookup(broker_subscription_inflight, ClientId) of
                   [{_, Term}] -> Term;
                   [] -> undefined
               end,
    Attached = ets:lookup(broker_subscription_attachments, ClientId),
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

subscription({_ClientId, Filter} = SubRef, QoS) ->
    #{sub_ref => SubRef, filter => Filter, qos => QoS,
      cursors => cursors_for(SubRef)}.

%% Rows are `{{SubRef, PartitionId}, #{offset := _}}'; match_object pins the
%% SubRef and returns whole rows, because map *contents* cannot be matched --
%% only the `#{_ := V}' whole-value form can.
cursors_for(SubRef) ->
    [{PartitionId, maps:get(offset, Row)}
     || {{_SubRef, PartitionId}, Row} <-
            ets:match_object(broker_subscription_cursors, {{SubRef, '_'}, '_'})].

group_by_client(SubRefs) ->
    lists:foldl(fun group_step/2, #{}, SubRefs).

group_step(SubRef, Acc) ->
    case ets:lookup(broker_subscription_entities, SubRef) of
        [{_, #{qos := QoS}}] ->
            ClientId = element(1, SubRef),
            {Max, Refs} = maps:get(ClientId, Acc, {undefined, []}),
            Acc#{ClientId => {max_q(QoS, Max), [SubRef | Refs]}};
        [] ->
            %% K8: an orphan Router entry resolves to nothing here, and the
            %% publisher must not pay for a bookkeeping gap in another layer.
            Acc
    end.

max_q(QoS, undefined) -> QoS;
max_q(QoS, Max) -> erlang:max(QoS, Max).
