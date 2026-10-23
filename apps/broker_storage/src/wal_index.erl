%% @doc Sparse read index for one WAL partition (ADR-011 §11.6, design §7.2).
%%
%% One public `ordered_set' per partition, whose tid travels inside the storage
%% handle so that `read_from' can locate an offset without touching the writer
%% process. Without this, every dispatch would scan a segment from the start and
%% eat back the fsync savings group commit produces.
%%
%% Rows are keyed so that the two kinds sort apart and `ets:prev/2' can answer
%% "greatest anchor at or below this offset" in one step:
%%
%%   {{meta}}            -> {Open, TruncateBefore}   (1-tuple, sorts first)
%%   {{anchor, Offset}}  -> {SegPath, FilePos}       (2-tuples, in offset order)
%%
%% An anchor carries its segment path as well as the position, because offsets
%% are dense across the whole partition while positions restart at zero in every
%% segment: a bare position would silently seek into the wrong file after a
%% rotation. The writer must therefore anchor the first record of every segment,
%% which wal_writer_SUITE pins.
-module(wal_index).

-export([new/0, state/1, set_open/2, set_trunc/2, add_anchor/4, locate/2]).

-define(META_KEY, {meta}).

-type trunc_before() :: non_neg_integer().

%% Offsets start at 1 (design §7.1), so `truncate_before = 1' discards nothing:
%% a fresh partition is open and has no boundary.
-spec new() -> ets:tid().
new() ->
    Tid = ets:new(wal_index, [ordered_set, public, {read_concurrency, true}]),
    true = ets:insert(Tid, {?META_KEY, true, 1}),
    Tid.

-spec state(ets:tid()) -> {Open :: boolean(), trunc_before()}.
state(Tid) ->
    [{?META_KEY, Open, TruncateBefore}] = ets:lookup(Tid, ?META_KEY),
    {Open, TruncateBefore}.

-spec set_open(ets:tid(), boolean()) -> true.
set_open(Tid, Open) ->
    ets:update_element(Tid, ?META_KEY, {2, Open}).

-spec set_trunc(ets:tid(), trunc_before()) -> true.
set_trunc(Tid, TruncateBefore) ->
    ets:update_element(Tid, ?META_KEY, {3, TruncateBefore}).

-spec add_anchor(ets:tid(), non_neg_integer(), file:filename_all(), non_neg_integer()) -> true.
add_anchor(Tid, Offset, SegPath, FilePos) ->
    ets:insert(Tid, {{anchor, Offset}, SegPath, FilePos}).

%% `not_found' means the offset lies below the first anchor, i.e. before the
%% partition's own start; there is nothing to seek to and the caller serves an
%% empty page.
-spec locate(ets:tid(), non_neg_integer()) ->
    {ok, AnchorOffset :: non_neg_integer(), file:filename_all(), non_neg_integer()}
  | not_found.
locate(Tid, Offset) ->
    case ets:lookup(Tid, {anchor, Offset}) of
        [{{anchor, Offset}, SegPath, FilePos}] ->
            {ok, Offset, SegPath, FilePos};
        [] ->
            case ets:prev(Tid, {anchor, Offset}) of
                {anchor, AnchorOffset} = Key ->
                    [{Key, SegPath, FilePos}] = ets:lookup(Tid, Key),
                    {ok, AnchorOffset, SegPath, FilePos};
                %% '$end_of_table', or the meta row that sorts below every anchor.
                _ ->
                    not_found
            end
    end.
