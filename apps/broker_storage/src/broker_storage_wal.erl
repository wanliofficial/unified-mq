%% @doc WAL `broker_storage' backend (design §7.2, ADR-011).
%%
%% The adapter between the frozen behavior and the modules that do the work:
%% `wal_writer' owns the offsets and the disk, `wal_index' owns the read path,
%% `broker_storage_wal_sup' owns the lifetime. This module owns nothing -- a handle
%% is `{partition id, writer pid, index tid}' -- so the data path does not pass
%% through it. `append/3' is one call to the writer (which encodes in the caller's
%% process and groups the commit), and `read_from/3' touches no process at all: it
%% reads the public index and the segment files directly, which is the only way
%% the fsync savings of group commit survive contact with a dispatching reader.
%%
%% Two consequences of holding no state are load-bearing:
%%
%%  * **A handle goes stale when its writer dies.** The index table belongs to the
%%    writer process, so reading through a pre-crash handle raises `badarg' rather
%%    than serving a short page. The supervisor puts the partition back on its own
%%    (E11) and the caller re-`recover/2's, which §11.2 allows on an open
%%    partition. `broker_storage_memory' has the identical property: its tables die
%%    with `broker_storage_memory_store'.
%%  * **`create/2' never starts a child it would have to take down again.** The
%%    filesystem guard and the on-disk layout are both written before
%%    `start_child', so a refused create leaves neither a directory that looks
%%    created nor a spec that would make the next attempt report `already_present'.
-module(broker_storage_wal).

-behaviour(broker_storage).

-include_lib("broker_core/include/broker_message.hrl").

-export([create/2, recover/2, close/1,
         append/3, read_from/3,
         advance_committed/2, truncate/2, capability/1,
         watermarks/1]).

%% design §7.3: `data/wal/{partition_id}/segment-{StartOffset:020d}.log'.
-define(WAL_SUBDIR, "wal").
-define(DEFAULT_DATA_DIR, "data").

-record(hd, {id     :: partition_id(),
             writer :: pid(),
             index  :: ets:tid()}).

%%%_* Lifecycle ----------------------------------------------------------

create(PartitionId, Config) ->
    Dir = partition_dir(PartitionId, Config),
    WalConfig = wal_config(Config),
    %% Guard first: refusing after the layout exists would leave a directory that
    %% recovery reads as a created partition on a filesystem that cannot keep it.
    case wal_writer:check_filesystem(Dir, WalConfig) of
        {error, Reason} ->
            {error, Reason};
        {ok, _FsWarnings} ->
            case wal_recovery:create(Dir) of
                ok ->
                    %% A fresh partition is then recovered like any other, so there
                    %% is exactly one code path that turns a directory into a
                    %% handle. The watermarks of an empty partition are 0 and 1 and
                    %% are not create/2's business.
                    case recover(PartitionId, Config) of
                        {ok, Hd, _Committed, _High} -> {ok, Hd};
                        {error, Reason} -> {error, Reason}
                    end;
                {error, {already_exists, Dir}} ->
                    {error, {already_exists, PartitionId}};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

recover(PartitionId, Config) ->
    Dir = partition_dir(PartitionId, Config),
    case broker_storage_wal_sup:start_partition(PartitionId, Dir, wal_config(Config)) of
        {ok, Writer} ->
            handle(PartitionId, Writer);
        %% §11.2 allows recover/2 on a partition that was closed, and a supervisor
        %% restart leaves it open again. Both are the same answer from here, which
        %% is why wal_writer:open/1 is idempotent.
        {error, {already_started, Writer}} ->
            case wal_writer:open(Writer) of
                {ok, _State} -> handle(PartitionId, Writer);
                {error, Reason} -> {error, Reason}
            end;
        {error, Error} ->
            unwrap(Error)
    end.

%% OTP 28 reports a child whose start function failed as `{Reason, #child{}}',
%% handing the caller supervisor's internal record. The frozen behavior promises
%% `{error, Reason}', and for recover/2 a missing partition is an expected answer
%% rather than a supervisor fault, so the record is dropped here. Pinned by the
%% contract case recover_missing_partition_does_not_create.
unwrap({Reason, {child, _, _, _, _, _, _, _, _}}) ->
    {error, Reason};
unwrap(Error) ->
    {error, Error}.

handle(PartitionId, Writer) ->
    #{index := Index, committed := Committed, high := High} = wal_writer:state(Writer),
    {ok, #hd{id = PartitionId, writer = Writer, index = Index}, Committed, High}.

%% Drains and marks the partition closed but leaves the child running, so a later
%% recover/2 can reopen it without rescanning the log.
close(#hd{writer = Writer}) ->
    wal_writer:close(Writer).

%%%_* Data path ----------------------------------------------------------

%% §11.4's capability check lives in the writer, before it allocates an offset, so
%% a rejected mode costs nothing and the next caller still gets the number it was
%% promised.
append(#hd{writer = Writer}, Msgs, Mode) ->
    wal_writer:append(Writer, Msgs, Mode).

read_from(#hd{id = PartitionId, index = Index}, From, Limit) ->
    case wal_index:state(Index) of
        %% A closed partition serves nothing. An empty page would be the same
        %% answer a caught-up subscriber gets, and the two must not be confused.
        {false, _TruncateBefore} ->
            {error, {closed, PartitionId}};
        {true, TruncateBefore} ->
            %% §11.6: the boundary is logical, so a lower request simply starts
            %% higher; the records below it are still in the segments.
            Start = erlang:max(From, TruncateBefore),
            case read_pages(PartitionId, Index, Start, Limit, []) of
                {ok, Rev} ->
                    Page = lists:reverse(Rev),
                    {ok, Page, next_offset(Start, Page)};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

%% Walks segments without ever holding more than one page in memory. `Rev' is the
%% reversed page, because a fold can only prepend.
read_pages(_PartitionId, _Index, _From, Limit, Rev) when Limit =< 0 ->
    {ok, Rev};
read_pages(PartitionId, Index, From, Limit, Rev) ->
    case wal_index:locate(Index, From) of
        %% Below the first anchor means before the partition's own start: there is
        %% nothing to seek to, so the page ends here.
        not_found ->
            {ok, Rev};
        {ok, AnchorOffset, Path, FilePos} ->
            Collector = collector(From, Limit),
            case wal_segment:fold(Path, {AnchorOffset, FilePos}, PartitionId,
                                  Collector, {0, Rev}) of
                {halt, {_Taken, Page}} ->
                    {ok, Page};
                %% A complete but unreadable record is not the end of the log.
                %% Serving the page as if it were would leave a subscriber waiting
                %% forever for offsets that exist, which is the silent downgrade
                %% §11.4 forbids -- so the read fails instead.
                {ok, #{tail := {corrupt, Reason}}, {_Taken, _Page}} ->
                    {error, {corrupt_record, Path, Reason}};
                %% Segment exhausted mid-page. The next segment anchors its own
                %% first record (wal_writer pins that), so continuing is one index
                %% lookup rather than a rescan from the start of the partition.
                {ok, #{next_offset := Next}, {Taken, Page}} when Next > From ->
                    read_pages(PartitionId, Index, Next, Limit - Taken, Page);
                {ok, _Summary, {_Taken, Page}} ->
                    %% Nothing was consumed, so `From' is at or past the end of the
                    %% readable log. A torn tail reads the same way: a writer
                    %% mid-append is ahead of what this reader may see, which is
                    %% normal rather than damaged.
                    {ok, Page};
                {error, Reason} ->
                    {error, {read_failed, Path, Reason}}
            end
    end.

%% Records below `From' are skipped, not counted: the fold starts at an anchor,
%% which is at or below the requested offset by construction.
collector(From, Limit) ->
    fun(Offset, Msg, _FilePos, {Taken, Page}) ->
            case Offset < From of
                true ->
                    {cont, {Taken, Page}};
                false when Taken + 1 >= Limit ->
                    {halt, {Taken + 1, [{Offset, Msg} | Page]}};
                false ->
                    {cont, {Taken + 1, [{Offset, Msg} | Page]}}
            end
    end.

next_offset(Start, []) ->
    Start;
next_offset(_Start, Page) ->
    {Last, _Msg} = lists:last(Page),
    Last + 1.

%%%_* Watermarks ---------------------------------------------------------

advance_committed(#hd{writer = Writer}, Offset) ->
    wal_writer:advance_committed(Writer, Offset).

truncate(#hd{writer = Writer}, BeforeOffset) ->
    wal_writer:truncate(Writer, BeforeOffset).

%% ADR-011 §11.4: instance level in the behavior, one honest answer here. The
%% writer's capability is what it actually does -- fsync via group commit -- and
%% `replicated_commit' needs the S3 consensus group.
capability(#hd{}) ->
    wal_writer:capability().

%% @doc Live `{Committed, High}' for a subscription that must start at the tail
%% (design §13 K1). Deliberately NOT a behavior callback -- ADR-011 is frozen --
%% but an instance query both backends answer, because the alternatives are
%% worse: `recover/2' has the side effect of reopening a closed partition
%% (`wal_writer:open'), and a High cached at worker boot goes stale the moment
%% anybody appends (the append path does not pass through the worker, J1). One
%% gen_server call per subscribe is affordable: subscribe is a SUBSCRIBE frame,
%% not the data path.
-spec watermarks(broker_storage:handle()) -> {ok, offset(), offset()} | {error, term()}.
watermarks(#hd{id = PartitionId, writer = Writer}) ->
    State = wal_writer:state(Writer),
    case maps:get(closed, State, false) of
        %% Same answer as the other data-path calls, so a caller cannot mistake
        %% a closed partition's frozen numbers for a live tail.
        true -> {error, {closed, PartitionId}};
        false ->
            #{committed := Committed, high := High} = State,
            {ok, Committed, High}
    end.

%%%_* Internal -----------------------------------------------------------

partition_dir(PartitionId, Config) ->
    DataDir = maps:get(data_dir, Config, ?DEFAULT_DATA_DIR),
    filename:join([DataDir, ?WAL_SUBDIR, integer_to_list(PartitionId)]).

wal_config(Config) ->
    maps:get(wal, Config, #{}).
