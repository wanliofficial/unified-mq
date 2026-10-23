%% @doc Creating and recovering a WAL partition directory (ADR-011 §11.2,
%% §11.3, §11.5; Validation F1/F2).
%%
%% Creation lives next to recovery because the two define one thing: what a
%% partition directory looks like. `create/1' writes the layout, `recover/3'
%% refuses anything that does not match it -- so a typo in `data_dir' or a
%% partition id becomes a startup failure instead of a silently empty partition.
%%
%% The rules that follow from the frozen contract:
%%
%%  * **offsets are dense** (F1). Every segment must start exactly where the
%%    previous one ended, and the recovered high offset is one past the last
%%    durable record, so a restart neither reuses a number nor leaves a hole.
%%    S1 never deletes segments (§7.3), which is why the first one must start at
%%    offset 1; S2 retention has to relax that.
%%  * **a torn tail is repaired, corruption is not.** Missing bytes are the
%%    expected shape of a crash mid-write, so the last segment is cut back to
%%    the last record boundary and the write continues there. A CRC failure or a
%%    newer record version means the bytes are present and wrong: recovery
%%    refuses to start and leaves them untouched, because truncating would
%%    destroy the only evidence and the records behind it.
%%  * **watermarks come back low, never high** (§11.3). A committed offset past
%%    the durable log -- an ack whose data did not survive -- is clamped to the
%%    last record, and the truncate boundary is clamped to `committed + 1'.
%%    Both clamps are reported in `warnings' rather than logged here, so the
%%    caller decides how loud to be and the behaviour stays testable.
-module(wal_recovery).

-include_lib("broker_core/include/broker_message.hrl").

-export([create/1, recover/3]).

%% design §7.1: offsets start at 1, so an empty partition's high offset is 1.
-define(FIRST_OFFSET, 1).
%% design §7.2: one anchor per 256 records by default.
-define(DEFAULT_ANCHOR_INTERVAL, 256).

-type current() :: #{path := file:filename_all(),
                     start_offset := non_neg_integer(),
                     end_pos := non_neg_integer(),
                     records := non_neg_integer()}.
-type state() :: #{dir := file:filename_all(),
                   index := ets:tid(),
                   high := non_neg_integer(),
                   committed := non_neg_integer(),
                   truncate_before := non_neg_integer(),
                   segments := [{non_neg_integer(), file:filename_all()}],
                   current := current(),
                   warnings := [term()]}.

-export_type([state/0, current/0]).

%%%_* Create -------------------------------------------------------------

-spec create(file:filename_all()) -> ok | {error, term()}.
create(Dir) ->
    case filelib:is_dir(Dir) of
        %% Refused even when the directory is empty: that is what a create which
        %% died halfway leaves behind, and overwriting it would hide the fact.
        true ->
            {error, {already_exists, Dir}};
        false ->
            case filelib:ensure_dir(filename:join(Dir, "placeholder")) of
                ok -> create_layout(Dir);
                {error, Reason} -> {error, {ensure_dir_failed, Dir, Reason}}
            end
    end.

create_layout(Dir) ->
    case wal_meta:open_append(Dir) of
        {ok, MetaFd} ->
            ok = wal_meta:close(MetaFd),
            case wal_segment:open_append(wal_segment:path(Dir, ?FIRST_OFFSET)) of
                {ok, SegFd} ->
                    ok = wal_segment:close(SegFd),
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%%%_* Recover ------------------------------------------------------------

-spec recover(partition_id(), file:filename_all(), map()) -> {ok, state()} | {error, term()}.
recover(PartitionId, Dir, Opts) ->
    Interval = maps:get(index_anchor_interval, Opts, ?DEFAULT_ANCHOR_INTERVAL),
    case wal_segment:list(Dir) of
        %% F2: a directory that does not exist, or one with no segments, is not
        %% a partition. Nothing is created on the way out.
        {error, enoent} ->
            {error, {not_found, PartitionId}};
        {error, Reason} ->
            {error, {list_failed, Dir, Reason}};
        {ok, #{segments := []}} ->
            {error, {not_found, PartitionId}};
        {ok, #{segments := Segments, unexpected := Unexpected}} ->
            case wal_meta:read(Dir) of
                %% Segments without watermarks means create/1 never finished;
                %% assuming "nothing committed" would throw away every ack.
                not_found ->
                    {error, {missing_meta, Dir}};
                {error, Reason} ->
                    {error, {meta_failed, Dir, Reason}};
                {ok, Watermarks, MetaTail} ->
                    assemble(Dir, Segments, Unexpected, Watermarks, MetaTail,
                             PartitionId, Interval)
            end
    end.

%%%_* Internal -----------------------------------------------------------

assemble(Dir, Segments, Unexpected, Watermarks, MetaTail, PartitionId, Interval) ->
    case replay(Segments, PartitionId, Interval) of
        {ok, #{high := High, anchors := Anchors, warnings := ReplayWarnings,
               current := Current}} ->
            {Committed, Trunc, ClampWarnings} = clamp(Watermarks, High),
            Index = publish(Anchors, Trunc),
            {ok, #{dir => Dir,
                   index => Index,
                   high => High,
                   committed => Committed,
                   truncate_before => Trunc,
                   segments => Segments,
                   current => Current,
                   warnings => ReplayWarnings ++ ClampWarnings
                               ++ meta_warnings(MetaTail)
                               ++ unexpected_warnings(Unexpected)}};
        {error, Reason} ->
            {error, Reason}
    end.

replay(Segments, PartitionId, Interval) ->
    Scan0 = #{partition_id => PartitionId,
              interval => Interval,
              last_path => element(2, lists:last(Segments)),
              expect => ?FIRST_OFFSET,
              high => ?FIRST_OFFSET,
              anchors => [],
              warnings => [],
              current => undefined},
    case lists:foldl(fun replay_segment/2, Scan0, Segments) of
        #{error := Reason} -> {error, Reason};
        Scan -> {ok, Scan}
    end.

replay_segment(_Segment, #{error := _} = Scan) ->
    Scan;
replay_segment({StartOffset, _Path}, #{expect := Expect} = Scan) when StartOffset =/= Expect ->
    Scan#{error => {segment_gap, Expect, StartOffset}};
replay_segment({StartOffset, Path}, Scan) ->
    #{partition_id := PartitionId, interval := Interval, last_path := LastPath} = Scan,
    Collector = fun(Offset, _Msg, FilePos, Anchors) ->
                        {cont, maybe_anchor(Offset, FilePos, Path, StartOffset, Interval, Anchors)}
                end,
    case wal_segment:fold(Path, bof, PartitionId, Collector, []) of
        {ok, Summary, Anchors} ->
            accept_tail(maps:get(tail, Summary), Path, Summary, Anchors,
                        Scan, Path =:= LastPath);
        {error, Reason} ->
            Scan#{error => {fold_failed, Path, Reason}}
    end.

accept_tail(clean, Path, Summary, Anchors, Scan, _IsLast) ->
    advance(Path, Summary, Anchors, Scan);
accept_tail({torn, truncated}, Path, Summary, Anchors, Scan, true) ->
    #{end_pos := EndPos, file_size := FileSize} = Summary,
    case cut(Path, EndPos) of
        ok ->
            Cut = {torn_tail_cut, Path, FileSize - EndPos},
            advance(Path, Summary, Anchors,
                    Scan#{warnings => maps:get(warnings, Scan) ++ [Cut]});
        {error, Reason} ->
            Scan#{error => {truncate_failed, Path, Reason}}
    end;
%% Cutting an earlier segment would drop offsets the next segment already
%% counts on, so a torn tail there is a hole rather than a repairable crash.
accept_tail({torn, truncated}, Path, _Summary, _Anchors, Scan, false) ->
    Scan#{error => {torn_earlier_segment, Path}};
%% §11.5: a record from a newer format is refused, never read as version 1.
accept_tail({corrupt, {unsupported_vsn, Vsn}}, Path, _Summary, _Anchors, Scan, _IsLast) ->
    Scan#{error => {unsupported_vsn, Path, Vsn}};
accept_tail({corrupt, Reason}, Path, _Summary, _Anchors, Scan, _IsLast) ->
    Scan#{error => {corrupt_record, Path, Reason}}.

advance(Path, Summary, Anchors, Scan) ->
    #{next_offset := Next, end_pos := EndPos} = Summary,
    #{expect := StartOffset, anchors := SoFar} = Scan,
    Scan#{expect => Next,
          high => Next,
          anchors => lists:reverse(Anchors, SoFar),
          current => #{path => Path,
                       start_offset => StartOffset,
                       %% Offsets are dense, so the count needs no accumulator.
                       records => Next - StartOffset,
                       end_pos => EndPos}}.

cut(Path, EndPos) ->
    case wal_segment:open_append(Path) of
        {ok, Fd} ->
            Result = case wal_segment:truncate_at(Fd, EndPos) of
                         ok -> wal_segment:close(Fd);
                         {error, Reason} -> _ = wal_segment:close(Fd), {error, Reason}
                     end,
            Result;
        {error, Reason} ->
            {error, Reason}
    end.

%% Every segment anchors its own first record (`Offset - StartOffset = 0'),
%% which is what keeps `wal_index:locate/2' from handing back a position that
%% belongs to the previous segment.
maybe_anchor(Offset, FilePos, Path, StartOffset, Interval, Anchors) ->
    case (Offset - StartOffset) rem Interval of
        0 -> [{Offset, Path, FilePos} | Anchors];
        _ -> Anchors
    end.

clamp(#{committed := Committed0, truncate_before := Trunc0}, High) ->
    MaxCommitted = High - 1,
    {Committed, Warnings1} = clamp_to(Committed0, MaxCommitted, committed_above_high, []),
    {Trunc, Warnings2} = clamp_to(Trunc0, Committed + 1, truncate_above_committed, Warnings1),
    {Committed, Trunc, Warnings2}.

clamp_to(Value, Max, Tag, Warnings) when Value > Max ->
    {Max, Warnings ++ [{Tag, Value, Max}]};
clamp_to(Value, _Max, _Tag, Warnings) ->
    {Value, Warnings}.

publish(Anchors, Trunc) ->
    Tid = wal_index:new(),
    lists:foreach(fun({Offset, Path, FilePos}) ->
                          true = wal_index:add_anchor(Tid, Offset, Path, FilePos)
                  end,
                  Anchors),
    true = wal_index:set_trunc(Tid, Trunc),
    Tid.

meta_warnings(clean) -> [];
meta_warnings(Tail) -> [{meta_tail, Tail}].

%% Our own meta.log lives beside the segments, so it is not "unexpected".
%% Anything else is reported but not fatal: refusing to boot over a stray file
%% would cost more than the mount mix-up it is meant to catch.
unexpected_warnings(Names) ->
    [{unexpected_file, Name} || Name <- Names, Name =/= wal_meta:filename()].
