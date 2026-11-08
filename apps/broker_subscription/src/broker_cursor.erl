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
         claim/5, claim/6, release/2, release_offset/2, rollback/3, settle/2,
         retry/5, attempts/2, backoff/3, due/3, blocked/3,
         retries/1, pause/2,
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
    #{kind => Kind, offset => Offset, pending => [], retries => #{}}.

-spec new_shared(offset()) -> row().
new_shared(Offset) ->
    #{kind => shared, low => Offset, acked => #{}, assigned => #{}, retries => #{}}.

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
    Row1 = clear_retry(Row, Offset),
    if
        Offset < Current -> Row1;
        Offset =:= Current -> cascade(Current + 1, Pending, Row1);
        true -> Row1#{pending => lists:usort([Offset | Pending])}
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
claim(Row, Max, High, Consumer, Limit) ->
    claim(Row, Max, High, Consumer, Limit, #{}).

%% @doc `Blocked' is the set of offsets whose redelivery backoff has not elapsed
%% (`broker_cursor:blocked/3'): a claim must not hand them out yet, or the
%% retry schedule would exist only on paper. `Retries' also counts towards
%% `Limit' -- an offset parked in backoff is still a live entry in this
%% partition's window (S2-E E-5).
-spec claim(row(), non_neg_integer(), offset(), consumer(), pos_integer(), map()) ->
          {ok, [offset()], row()} | {error, overflow}.
claim(Row, Max, _High, _Consumer, _Limit, _Blocked) when Max =< 0 ->
    {ok, [], Row};
claim(Row, _Max, _High, _Consumer, Limit, _Blocked) when Limit =< 0 ->
    {ok, [], Row};
claim(#{kind := shared, low := Low, acked := Acked, assigned := Assigned} = Row,
      Max, High, Consumer, Limit, Blocked) ->
    Retries = retries(Row),
    Room = Limit - (map_size(Acked) + map_size(Assigned) + map_size(Retries)),
    if
        Room =< 0 ->
            {error, overflow};
        true ->
            Take = erlang:min(Max, Room),
            {Offsets, New} = scan(Low, Take, High, Acked, Assigned, Blocked,
                                  Consumer, []),
            {ok, lists:reverse(Offsets), Row#{assigned := New}}
    end.

scan(_Next, 0, _High, _Acked, Assigned, _Blocked, _Consumer, Acc) ->
    {Acc, Assigned};
scan(Next, _Left, High, _Acked, Assigned, _Blocked, _Consumer, Acc)
  when Next >= High ->
    %% The tail. Handing out an offset the log does not have yet would create an
    %% assignment that can never be acked, pinning `low' forever (ADR-019 §19.1).
    {Acc, Assigned};
scan(Next, Left, High, Acked, Assigned, Blocked, Consumer, Acc) ->
    case maps:is_key(Next, Acked) orelse maps:is_key(Next, Assigned)
        orelse maps:is_key(Next, Blocked) of
        true ->
            scan(Next + 1, Left, High, Acked, Assigned, Blocked, Consumer, Acc);
        false ->
            scan(Next + 1, Left - 1, High, Acked, Assigned#{Next => Consumer},
                 Blocked, Consumer, [Next | Acc])
    end.

%% @doc Turn one ack into either a move of the contiguous prefix or a buffered
%% hole. `unowned' is the §19.2 answer for an ack that names an offset this
%% consumer does not hold: the caller counts it and changes nothing.
-spec ack_shared(row(), offset(), consumer()) -> {ok, row()} | unowned.
ack_shared(#{kind := shared, low := Low} = Row, Offset, _Consumer)
  when Offset < Low ->
    %% Already eaten by the prefix: a duplicate ack is idempotent.
    {ok, clear_retry(Row, Offset)};
ack_shared(#{kind := shared, acked := Acked} = Row, Offset, _Consumer)
  when is_map_key(Offset, Acked) ->
    {ok, clear_retry(Row, Offset)};
ack_shared(#{kind := shared, acked := Acked, assigned := Assigned} = Row,
           Offset, Consumer) ->
    case maps:get(Offset, Assigned, undefined) of
        Consumer ->
            Cleared = clear_retry(Row, Offset),
            {ok, promote(Cleared#{acked := Acked#{Offset => true},
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
    Gone = [O || {O, C} <- maps:to_list(Assigned), C =:= Consumer],
    unpause(Row#{assigned := maps:without(Gone, Assigned)}, Gone).

%% @doc Hand exactly one offset back, whoever holds it. The caller is the retry
%% scheduler (S2-E): after a shared delivery timed out, the offset must become
%% claimable again *after* its backoff, not stay parked on a consumer that is
%% not answering.
-spec release_offset(row(), offset()) -> row().
release_offset(#{assigned := Assigned} = Row, Offset) ->
    unpause(Row#{assigned := maps:remove(Offset, Assigned)}, [Offset]).

%% @doc Finish an offset the broker itself consumed -- a dead letter, or a
%% message the reader skipped because it had expired. Unlike `ack_shared/3' it
%% does not ask who holds the offset: the message is gone on purpose, so the
%% holder (if any) must not be able to pin `low' by staying silent. Removing the
%% assignment and recording the ack is exactly what an honest consumer would
%% have done.
-spec settle(row(), offset()) -> row().
settle(#{kind := shared, low := Low} = Row, Offset) when Offset < Low ->
    clear_retry(Row, Offset);
settle(#{kind := shared, acked := Acked, assigned := Assigned} = Row, Offset) ->
    Cleared = clear_retry(Row, Offset),
    promote(Cleared#{acked := Acked#{Offset => true},
                     assigned := maps:remove(Offset, Assigned)}).

%%%_* Retry state (S2-E, ADR-015) --------------------------------------------

%% @doc Record one failed delivery attempt for `Offset'. `attempts' counts
%% *failures*: the first timeout makes it 1, and `max_attempts' is compared
%% against that counter, so `max_attempts = 10' means ten timeouts before the
%% message is dead-lettered. `first_at' is kept from the first failure -- it is
%% the age of the problem, not of the last attempt.
-spec retry(row(), offset(), integer(), integer(), atom()) -> row().
retry(Row, Offset, NextAt, Now, Reason) ->
    Retries = retries(Row),
    {Attempts, FirstAt} =
        case maps:get(Offset, Retries, undefined) of
            undefined -> {1, Now};
            #{attempts := A, first_at := F} -> {A + 1, F}
        end,
    Row#{retries := Retries#{Offset => #{attempts => Attempts,
                                         first_at => FirstAt,
                                         next_at => NextAt,
                                         reason => Reason}}}.

-spec attempts(row(), offset()) -> non_neg_integer().
attempts(Row, Offset) ->
    case maps:get(Offset, retries(Row), undefined) of
        undefined -> 0;
        #{attempts := A} -> A
    end.

%% @doc ADR-015 §15.2's schedule: `min(base * 2^(n-1), max)'. Exported because
%% the worker must not be the only place that knows the curve -- a test asserts
%% the sequence with no clock at all.
-spec backoff(pos_integer(), pos_integer(), pos_integer()) -> pos_integer().
backoff(Attempts, Base, Max) ->
    min(Max, Base * (1 bsl min(Attempts - 1, 30))).

-spec retries(row()) -> map().
retries(Row) ->
    maps:get(retries, Row, #{}).

%% @doc Offsets whose backoff has elapsed and that some consumer should retry
%% now. An assigned offset is still being worked on by its holder, so it is not
%% due; a paused entry (`next_at = infinity', see `pause/2') never is.
-spec due(row(), integer(), offset()) -> [offset()].
due(Row, Now, High) ->
    Assigned = maps:get(assigned, Row, #{}),
    lists:sort([O || {O, #{next_at := At}} <- maps:to_list(retries(Row)),
                     is_integer(At), At =< Now, O < High,
                     not maps:is_key(O, Assigned)]).

%% @doc The complement of `due/3' -- offsets a claim must not hand out yet.
%% `infinity' is a pause, not a backoff, so it is neither due nor blocked.
-spec blocked(row(), integer(), offset()) -> map().
blocked(Row, Now, High) ->
    maps:from_list([{O, true}
                    || {O, #{next_at := At}} <- maps:to_list(retries(Row)),
                       is_integer(At), At > Now, O < High]).

%% @doc Freeze an entry whose redelivery has just been handed to a connection.
%% The attempt counter stays (it is the budget), but nothing is due until the
%% connection reports the next timeout -- otherwise the scheduler would re-issue
%% a redelivery that was already put on the wire.
-spec pause(row(), [offset()]) -> row().
pause(Row, Offsets) ->
    lists:foldl(fun(O, Acc) -> put_next_at(Acc, O, infinity) end, Row, Offsets).

%% @doc A released offset must not stay paused: the member that owned it is
%% gone, so "try again now" is the correct schedule (`attempts' is preserved).
unpause(Row, Offsets) ->
    lists:foldl(fun(O, Acc) ->
                        case maps:get(O, retries(Acc), undefined) of
                            #{next_at := infinity} -> put_next_at(Acc, O, 0);
                            _ -> Acc
                        end
                end, Row, Offsets).

put_next_at(Row, Offset, NextAt) ->
    case maps:get(Offset, retries(Row), undefined) of
        undefined -> Row;
        Entry -> Row#{retries := (retries(Row))#{Offset => Entry#{next_at := NextAt}}}
    end.

%% @doc An ack (or a dead-letter copy that landed) ends the retry story for
%% this offset.
-spec clear_retry(row(), offset()) -> row().
clear_retry(Row, Offset) ->
    case maps:is_key(Offset, retries(Row)) of
        true -> Row#{retries := maps:remove(Offset, retries(Row))};
        false -> Row
    end.

%% @doc Return a *set* of offsets to the pool, but only the ones still held by
%% `Consumer' (ADR-019 §19.5). This is the claim-side correction: the storage
%% `High' watermark is a reservation, so a claim can name an offset whose record
%% has not been flushed yet; the caller reads the page, finds the offset absent
%% and hands it back here instead of leaving a lease nobody will ever ack.
%%
%% Acking wins every race: an offset that moved into `acked' in the meantime is
%% simply not in `assigned' any more, so the fold leaves it where it is.
-spec rollback(row(), [offset()], consumer()) -> row().
rollback(#{assigned := Assigned} = Row, Offsets, Consumer) ->
    Row#{assigned := lists:foldl(
                       fun(Offset, Acc) ->
                               case maps:get(Offset, Acc, undefined) of
                                   Consumer -> maps:remove(Offset, Acc);
                                   _ -> Acc
                               end
                       end, Assigned, Offsets)}.

%%%_* Persistence ------------------------------------------------------------

%% @doc The row as it may reach `.cursors'. `assigned' is dropped: it is live
%% state that must not survive a restart (ADR-019 §19.3). `retries' *does*
%% survive (ADR-015 §15.1: the retry plan must outlive the connection), pruned
%% to the offsets that are still ahead of the read position -- a retry entry
%% below the cursor describes a message that was already consumed.
-spec persist(row()) -> row().
persist(#{kind := shared} = Row) ->
    prune_retries(maps:remove(assigned, Row));
persist(Row) ->
    prune_retries(Row).

prune_retries(Row) ->
    Floor = next_read(Row),
    Row#{retries := maps:filter(fun(O, _) -> O >= Floor end, retries(Row))}.

%% @doc A row read back from `.cursors'. A shared row comes back with no
%% assignments at all: whatever the previous process had claimed is owed again.
%% Retry entries stay, so a crash inside a backoff window does not reset the
%% attempt counter to zero (it may only *regress* by the cursor flush window,
%% which means a few extra retries -- the safe direction, ADR-015 §15.6).
-spec restore(row()) -> row().
restore(#{kind := shared} = Row) ->
    Row#{assigned => #{}, retries => maps:get(retries, Row, #{})};
restore(Row) ->
    Row#{retries => maps:get(retries, Row, #{})}.
