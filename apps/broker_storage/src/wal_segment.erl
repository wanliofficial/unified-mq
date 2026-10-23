%% @doc WAL segment files: naming, listing, appending and the streaming fold
%% that recovery and `read_from' are both built on (design §7.3).
%%
%% A segment is named for the offset of its first record, so record offsets are
%% implicit: they are never stored, only counted. That makes the filename part
%% of the on-disk protocol, and it is why `fold/5' refuses a path that does not
%% match -- a mis-named file would silently renumber every record in it.
%%
%% The fold distinguishes two shapes of damaged tail: `truncated' (bytes are
%% missing, the normal outcome of a crash mid-write) from a decode error (bytes
%% are present but wrong). Only the former is expected; `wal_recovery' alarms on
%% the latter. Either way the fold stops there and reports the file position to
%% resume from, since nothing past an unreadable record can ever be read again.
%%
%% `open_append/1' deliberately does NOT use O_APPEND: `truncate_at/2' has to be
%% able to cut a torn tail and continue writing at the new end, which O_APPEND
%% makes ambiguous. The price is that a partition must have exactly one writer
%% process, which the supervisor guarantees (one `wal_writer' per partition).
-module(wal_segment).

-include_lib("broker_core/include/broker_message.hrl").

-export([path/2, list/1]).
-export([open_append/1, append/2, sync/1, close/1, truncate_at/2]).
-export([fold/5, fold/6]).

%% 20 digits, design §7.3. Pinned by wal_segment_SUITE.
-define(SEGMENT_RE, "^segment-([0-9]{20})\\.log$").
-define(CHUNK_BYTES, 1 bsl 20).

-type start_offset() :: non_neg_integer().
-type from() :: bof | {Offset :: start_offset(), FilePos :: non_neg_integer()}.
-type fold_fun(Acc) :: fun((start_offset(), #broker_message{}, FilePos :: non_neg_integer(), Acc) ->
                               {cont, Acc} | {halt, Acc}).
%% `tail' describes the end of the readable stream: `clean' means the last byte
%% of the file ended a record; `{torn, truncated}' means a partial record is
%% still on disk; `{corrupt, _}' means a record is complete but unreadable.
-type summary() :: #{next_offset := start_offset(),
                     end_pos := non_neg_integer(),
                     file_size := non_neg_integer(),
                     tail := clean | {torn, truncated} | {corrupt, term()}}.

-export_type([start_offset/0, from/0, summary/0]).

%%%_* Naming and listing -------------------------------------------------

-spec path(file:filename_all(), start_offset()) -> file:filename_all().
path(Dir, StartOffset) when is_integer(StartOffset), StartOffset >= 0 ->
    Name = io_lib:format("segment-~20..0B.log", [StartOffset]),
    filename:join(Dir, lists:flatten(Name)).

%% Anything in the directory that is not a segment is reported rather than
%% skipped. Recognising our own `meta.log' is `wal_recovery's job: this module
%% has no business knowing what else lives next to the segments.
-spec list(file:filename_all()) ->
    {ok, #{segments := [{start_offset(), file:filename_all()}],
           unexpected := [file:filename()]}}
  | {error, term()}.
list(Dir) ->
    case file:list_dir(Dir) of
        {ok, Names} ->
            {Offsets, Unexpected} = lists:foldl(fun classify/2, {[], []}, Names),
            Segments = [{Offset, path(Dir, Offset)} || Offset <- lists:sort(Offsets)],
            {ok, #{segments => Segments, unexpected => lists:sort(Unexpected)}};
        {error, Reason} ->
            {error, Reason}
    end.

%%%_* Write side ---------------------------------------------------------

-spec open_append(file:filename_all()) -> {ok, file:io_device()} | {error, term()}.
open_append(Path) ->
    case file:open(Path, [raw, binary, read, write]) of
        {ok, Fd} ->
            case file:position(Fd, eof) of
                {ok, _Size} ->
                    {ok, Fd};
                {error, Reason} ->
                    _ = file:close(Fd),
                    {error, {position_failed, Path, Reason}}
            end;
        {error, Reason} ->
            {error, {open_failed, Path, Reason}}
    end.

-spec append(file:io_device(), binary()) -> ok | {error, term()}.
append(Fd, Bin) ->
    file:write(Fd, Bin).

-spec sync(file:io_device()) -> ok | {error, term()}.
sync(Fd) ->
    file:sync(Fd).

-spec close(file:io_device()) -> ok | {error, term()}.
close(Fd) ->
    file:close(Fd).

%% Cut the file at FilePos and leave the write position there, so the next
%% append lands exactly on the boundary instead of after unreadable garbage.
-spec truncate_at(file:io_device(), non_neg_integer()) -> ok | {error, term()}.
truncate_at(Fd, FilePos) ->
    case file:position(Fd, {bof, FilePos}) of
        {ok, FilePos} -> file:truncate(Fd);
        {error, Reason} -> {error, Reason}
    end.

%%%_* Read side ----------------------------------------------------------

%% `From' is `bof' or an index anchor `{Offset, FilePos}'. An anchor carries the
%% offset because a bare file position cannot be turned back into one without
%% re-reading the whole segment, which is precisely what the index exists to
%% avoid (ADR-011 §11.6).
-spec fold(file:filename_all(), from(), partition_id(), fold_fun(Acc), Acc) ->
    {ok, summary(), Acc} | {halt, Acc} | {error, term()}.
fold(Path, From, PartitionId, Fun, Acc) ->
    fold(Path, From, PartitionId, Fun, Acc, #{}).

-spec fold(file:filename_all(), from(), partition_id(), fold_fun(Acc), Acc,
           #{chunk_bytes => pos_integer()}) ->
    {ok, summary(), Acc} | {halt, Acc} | {error, term()}.
fold(Path, From, PartitionId, Fun, Acc0, Opts) when is_function(Fun, 4) ->
    ChunkBytes = maps:get(chunk_bytes, Opts, ?CHUNK_BYTES),
    case start_offset(Path) of
        {ok, StartOffset} ->
            case file:open(Path, [raw, binary, read]) of
                {ok, Fd} ->
                    {FilePos, Offset} = resolve(From, StartOffset),
                    try
                        {ok, FilePos} = file:position(Fd, {bof, FilePos}),
                        loop(Fd, FilePos, Offset, PartitionId, Fun, Acc0, <<>>,
                             ChunkBytes, filelib:file_size(Path))
                    after
                        _ = file:close(Fd)
                    end;
                {error, Reason} ->
                    {error, {open_failed, Path, Reason}}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%%%_* Internal -----------------------------------------------------------

loop(Fd, Pos, Offset, PartitionId, Fun, Acc, Buf, ChunkBytes, FileSize) ->
    case wal_codec:decode(Buf, PartitionId) of
        {ok, Msg, Used} ->
            <<_:Used/binary, Rest/binary>> = Buf,
            case Fun(Offset, Msg, Pos, Acc) of
                {cont, Acc1} ->
                    loop(Fd, Pos + Used, Offset + 1, PartitionId, Fun, Acc1, Rest,
                         ChunkBytes, FileSize);
                {halt, Acc1} ->
                    %% No summary: the tail and the end of the segment were never
                    %% reached, and a partial summary would look like a complete one.
                    {halt, Acc1}
            end;
        truncated ->
            case file:read(Fd, ChunkBytes) of
                {ok, Data} ->
                    loop(Fd, Pos, Offset, PartitionId, Fun, Acc,
                         <<Buf/binary, Data/binary>>, ChunkBytes, FileSize);
                eof ->
                    {ok, summary(Pos, Offset, FileSize, tail_of(Buf)), Acc};
                {error, Reason} ->
                    {error, {read_failed, Reason}}
            end;
        {error, Reason} ->
            {ok, summary(Pos, Offset, FileSize, {corrupt, Reason}), Acc}
    end.

summary(EndPos, NextOffset, FileSize, Tail) ->
    #{next_offset => NextOffset,
      end_pos => EndPos,
      file_size => FileSize,
      tail => Tail}.

tail_of(<<>>) -> clean;
tail_of(_Leftover) -> {torn, truncated}.

resolve(bof, StartOffset) -> {0, StartOffset};
resolve({Offset, FilePos}, _StartOffset) -> {FilePos, Offset}.

classify(Name, {Offsets, Unexpected}) ->
    case start_offset(Name) of
        {ok, Offset} -> {[Offset | Offsets], Unexpected};
        {error, _} -> {Offsets, [Name | Unexpected]}
    end.

start_offset(Path) ->
    case re:run(filename:basename(Path), ?SEGMENT_RE, [{capture, all_but_first, list}]) of
        {match, [Digits]} -> {ok, list_to_integer(Digits)};
        nomatch -> {error, {bad_segment_name, Path}}
    end.
