%% @doc Pure row algebra for the `?CURSORS' table (ADR-013 §13.2, ADR-019
%% §19.1/§19.2).
%%
%% Two shapes, one module:
%%
%%   * plain (`exclusive' / `failover'): `#{kind, offset, pending}' -- S1's row
%%     plus the explicit `kind' field ADR-019 froze, so no caller has to guess
%%     the type from which keys happen to be present.
%%   * shared: `#{kind, low, acked, assigned}'. `low' is the smallest offset not
%%     yet acked, so `committed = low - 1'; `acked' holds out-of-order acks that
%%     the contiguous prefix has not eaten yet; `assigned' remembers *who* holds
%%     an offset, which is the whole reason ADR-019 §19.1 chose a worker-side
%%     claim over a shared read counter.
%%
%% Pure functions only: no ETS, no process, no `application:get_env'. The worker
%% owns the rows and passes every cap in, which is what makes each rule here
%% testable without booting an application -- and keeps the one place that must
%% be serialized (the worker) the only place that mutates anything.
%%
%% `assigned' is deliberately absent from `persist/1': it is a live assignment,
%% not a fact (ADR-019 §19.3). A crash must redeliver every un-acked offset.
-module(broker_cursor).

-export([new_plain/2, new_shared/1,
         advance_plain/2, ack_shared/3, grow/2,
         claim/5, release/2,
         next_read/1, committed/1,
         persist/1, restore/1]).

-export_type([row/0, sub_kind/0, consumer/0]).

-include_lib("broker_core/include/broker_message.hrl").

-type sub_kind() :: exclusive | shared | failover.
-type consumer() :: binary().
-type row() :: map().

%%%_* Construction ---------------------------------------------------------

-spec new_plain(sub_kind(), offset()) -> row().
new_plain(Kind, Offset) when Kind =:= exclusive; Kind =:= failover ->
    #{kind => Kind, offset => Offset, pending => []}.

-spec new_shared(offset()) -> row().
new_shared(Offset) ->
    #{kind => shared, low => Offset, acked => #{}, assigned => #{}}.

%%%_* Reads -----------------------------------------------------------------

%% @doc The next offset this row will read. For a shared row that is `low'
%% (the first offset nobody has acked yet), for a plain row the scalar cursor.
-spec next_read(row()) -> offset().
next_read(#{low := Low}) -> Low;
next_read(#{offset := Offset}) -> Offset.

%% @doc The last offset that may be reported as committed. Shared rows commit
%% by contiguous prefix; plain rows commit everything below their cursor -- the
%% same arithmetic ADR-013 §13.2 froze, expressed once.
-spec committed(row()) -> offset().
committed(Row) ->
    next_read(Row) - 1.

%%%_* Plain advance ---------------------------------------------------------

%% @doc Contiguous-prefix advance for `exclusive'/`failover': an ack at the
%% cursor moves it and cascades over the buffered set; an ack beyond it buffers;
%% an ack behind it is a duplicate and changes nothing (acks arrive out of order
%% because different packet ids resolve in different orders).
-spec advance_plain(row(), offset()) -> row().
advance_plain(Row, Offset) ->
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

%%%_* Shared claim / ack ----------------------------------------------------

%% @doc Take up to `Max' unassigned offsets from `low' upward, never reaching
%% `High' (the storage contract's next free slot, `Last + 1'). The returned list
%% is ascending and **may be non-contiguous**: holes are offsets already acked
%% out of order or currently assigned to another member.
%%
%% `Limit' is `shared_state_max' -- `map_size(acked) + map_size(assigned)' after
%% the claim may not exceed it. A full window answers `{error, overflow}' and the
%% caller counts it; the claim itself stays bounded rather than evicting a live
%% assignment (eviction would turn "who holds this" into a random value).
%%
%% Not advancing `low' is the point: assignment and completion are different
%% facts, and only an ack's contiguous prefix may move the watermark.
-spec claim(row(), non_neg_integer(), offset(), consumer(), pos_integer()) ->
          {ok, [offset()], row()} | {error, overflow}.
claim(Row, Max, _High, _Consumer, _Limit) when Max =< 0 ->
    {ok, [], Row};
claim(Row, _Max, _High, _Consumer, Limit) when Limit =< 0 ->
    {ok, [], Row};
claim(#{kind := shared, low := Low, acked := Acked, assigned := Assigned} = Row,
      Max, High, Consumer, Limit) ->
    Room = Limit - (map_size(Acked) + map_size(Assigned)),
    if
        Room =< 0 ->
            {error, overflow};
        true ->
            Take = erlang:min(Max, Room),
            {Offsets, New} = scan(Low, Take, High, Acked, Assigned, Consumer, []),
            {ok, lists:reverse(Offsets), Row#{assigned := New}}
    end.

scan(_Next, 0, _High, _Acked, Assigned, _Consumer, Acc) ->
    {Acc, Assigned};
scan(Next, _Left, High, _Acked, Assigned, _Consumer, Acc) when Next >= High ->
    %% The tail. Handing out an offset the log does not have yet would create an
    %% assignment that can never be acked, pinning `low' forever (ADR-019 §19.1).
    {Acc, Assigned};
scan(Next, Left, High, Acked, Assigned, Consumer, Acc) ->
    case maps:is_key(Next, Acked) orelse maps:is_key(Next, Assigned) of
        true ->
            scan(Next + 1, Left, High, Acked, Assigned, Consumer, Acc);
        false ->
            scan(Next + 1, Left - 1, High, Acked, Assigned#{Next => Consumer},
                 Consumer, [Next | Acc])
    end.

%% @doc Turn one ack into either a move of the contiguous prefix or a buffered
%% hole. `unowned' is the §19.2 answer for an ack that names an offset this
%% consumer does not hold: the caller counts it and changes nothing.
-spec ack_shared(row(), offset(), consumer()) -> {ok, row()} | unowned.
ack_shared(#{kind := shared, low := Low} = Row, Offset, _Consumer)
  when Offset < Low ->
    %% Already eaten by the prefix: a duplicate ack is idempotent.
    {ok, Row};
ack_shared(#{kind := shared, acked := Acked} = Row, Offset, _Consumer)
  when is_map_key(Offset, Acked) ->
    {ok, Row};
ack_shared(#{kind := shared, acked := Acked, assigned := Assigned} = Row,
           Offset, Consumer) ->
    case maps:get(Offset, Assigned, undefined) of
        Consumer ->
            {ok, promote(Row#{acked := Acked#{Offset => true},
                              assigned := maps:remove(Offset, Assigned)})};
        _ ->
            unowned
    end.

promote(#{kind := shared, low := Low, acked := Acked} = Row) ->
    case maps:is_key(Low, Acked) of
        true -> promote(Row#{low := Low + 1, acked := maps:remove(Low, Acked)});
        false -> Row
    end.

%% @doc Acknowledge an offset without ownership -- for building a state in a
%% test, not for the data path. The ack path is `ack_shared/3', which refuses
%% offsets this consumer does not hold (§19.2); this one says "pretend the
%% prefix already ate it" so a case can start with a hole above `low' without
%% having to claim and ack a whole run first.
-spec grow(row(), offset()) -> row().
grow(#{kind := shared, low := Low} = Row, Offset) when Offset < Low ->
    Row;
grow(#{kind := shared, acked := Acked} = Row, Offset) ->
    Row#{acked := Acked#{Offset => true}}.

%% @doc Drop every offset assigned to `Consumer' (a member died or left).
%% Those offsets become claimable again -- the redelivery path ADR-019 §19.2
%% relies on, and the reason no retry timer exists in S2-D.
-spec release(row(), consumer()) -> row().
release(#{assigned := Assigned} = Row, Consumer) ->
    Row#{assigned := maps:filter(fun(_Offset, C) -> C =/= Consumer end, Assigned)}.

%%%_* Persistence ------------------------------------------------------------

%% @doc The row as it may reach `.cursors'. `assigned' is dropped: it is live
%% state that must not survive a restart (ADR-019 §19.3).
-spec persist(row()) -> row().
persist(#{kind := shared} = Row) ->
    maps:remove(assigned, Row);
persist(Row) ->
    Row.

%% @doc A row read back from `.cursors'. A shared row comes back with no
%% assignments at all: whatever the previous process had claimed is owed again.
-spec restore(row()) -> row().
restore(#{kind := shared} = Row) ->
    Row#{assigned => #{}};
restore(Row) ->
    Row.
