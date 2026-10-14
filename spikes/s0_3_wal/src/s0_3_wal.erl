%% S0.3 - WAL / Segment Log with group commit.
%%
%% Deliberately plain Erlang (file + binaries), no OPAQUE-style trickery:
%% the point of the spike is to measure what a hand-rolled log costs us in
%% fsync throughput and recovery time, per README 5.3 方案 A.
-module(s0_3_wal).

-export([open/2, append/3, sync/1, close/1, read_all/1, stats/1]).
-export([segment_path/2, load_segments/1, record_size/1]).

-record(wal, {
    dir :: file:filename_all(),
    fd :: file:io_device() | undefined,
    seg_no :: pos_integer(),        % current segment index
    seg_max_bytes :: pos_integer(),
    seg_bytes = 0 :: non_neg_integer(),
    count = 0 :: non_neg_integer()  % records appended in this run
}).

-define(DEFAULT_SEG_MAX_BYTES, 64 * 1024 * 1024).

-type wal() :: #wal{}.
-export_type([wal/0]).

%% open(Dir, Opts) -> {ok, wal()} | {error, Reason}
%%
%% Opts: #{seg_max_bytes => pos_integer(), create => boolean()}
%% Reopens an existing log when segment files are present, otherwise creates.
open(Dir, Opts) when is_map(Opts) ->
    SegMax = maps:get(seg_max_bytes, Opts, ?DEFAULT_SEG_MAX_BYTES),
    case filelib:ensure_dir(filename:join(Dir, "placeholder")) of
        ok ->
            {ok, Segs} = load_segments(Dir),
            Seg0 = case Segs of
                [] -> 1;
                _ -> lists:last(Segs)
            end,
            Path = segment_path(Dir, Seg0),
            case file:open(Path, [append, binary, raw, read]) of
                {ok, Fd} ->
                    {ok, Size} = file:position(Fd, eof),
                    {ok, #wal{dir = Dir, fd = Fd, seg_no = Seg0,
                               seg_max_bytes = SegMax, seg_bytes = Size}};
                {error, Reason} ->
                    {error, {open_failed, Path, Reason}}
            end;
        {error, Reason} ->
            {error, {ensure_dir_failed, Dir, Reason}}
    end.

%% append(Wal, Term, SyncMode) -> {ok, wal()} | {error, Reason}
%%
%% SyncMode is the reliability grade under test (README 6.3):
%%   os_buffer -> write() only. Survives process crash, NOT OS crash.
%%   fsync     -> filelib:sync/1 after write. Survives OS crash.
append(#wal{fd = undefined} = W, _Term, _Sync) ->
    {error, {closed, W}};
append(#wal{} = W0, Term, Sync) ->
    Bin = encode(Term),
    Size = byte_size(Bin),
    try
        W1 = case needs_rotate(W0, Size) of
            true -> rotate(W0);
            false -> W0
        end,
        ok = file:write(W1#wal.fd, Bin),
        ok = case Sync of
            fsync -> file:sync(W1#wal.fd);
            _ -> ok
        end,
        {ok, W1#wal{seg_bytes = W1#wal.seg_bytes + Size,
                    count = W1#wal.count + 1}}
    catch
        Class:Reason -> {error, {Class, Reason}}
    end.

%% sync(Wal) -> ok | {error, Reason}
sync(#wal{fd = undefined}) -> {error, closed};
sync(#wal{fd = Fd}) -> file:sync(Fd).

close(#wal{fd = undefined} = W) -> {ok, W};
close(#wal{fd = Fd} = W) ->
    _ = file:sync(Fd),
    _ = file:close(Fd),
    {ok, W#wal{fd = undefined}}.

stats(#wal{} = W) ->
    maps:with([seg_no, seg_bytes, seg_max_bytes, count], W).

%% read_all(WalOrDir) -> {ok, [term()]} | {error, Reason}
%%
%% Replays every segment in order. Stops at the first truncated record, which
%% is exactly the partial-write case README 11.3 asks us to handle.
read_all(#wal{dir = Dir}) -> read_all(Dir);
read_all(Dir) when is_list(Dir) ->
    {ok, Segs} = load_segments(Dir),
    {ok, lists:flatmap(fun(Seg) -> read_segment(Dir, Seg) end, Segs)}.

%% segment_path(Dir, No) -> filename_all()
segment_path(Dir, No) ->
    filename:join(Dir, io_lib:format("segment-~8..0B.log", [No])).

%% load_segments(Dir) -> {ok, [pos_integer()]} sorted ascending
load_segments(Dir) ->
    case file:list_dir(Dir) of
        {ok, Names} ->
            Segs = [No || N <- Names, No <- [segment_no(N)], No =/= false],
            {ok, lists:sort(Segs)};
        {error, Reason} ->
            {error, Reason}
    end.

%% record_size(Term) -> non_neg_integer()  (encoded on-disk size)
record_size(Term) -> byte_size(encode(Term)).

%% ---------------------------------------------------------------- internals

%% segment_no(Name) -> pos_integer() | false
-spec segment_no(file:filename()) -> pos_integer() | false.
segment_no(Name) ->
    case re:run(Name, "^segment-([0-9]{8})\\.log$",
                [{capture, all_but_first, binary}]) of
        {match, [Bin]} ->
            case binary_to_integer(Bin) of
                0 -> false;
                No -> No
            end;
        nomatch -> false
    end.

-spec encode(term()) -> binary().
encode(Term) ->
    Body = term_to_binary(Term, [{compressed, 1}]),
    <<(byte_size(Body)):32/big, Body/binary>>.

%% decode/1 returns the record plus its total on-disk size so the caller can
%% advance without re-encoding.
-spec decode(binary()) -> {ok, term(), non_neg_integer()} | truncated.
decode(<<Len:32/big, Rest/binary>>) when byte_size(Rest) >= Len ->
    <<Body:Len/binary, _Tail/binary>> = Rest,
    try
        {ok, binary_to_term(Body, [safe]), 4 + Len}
    catch _:_ -> truncated
    end;
decode(_) -> truncated.

-spec needs_rotate(wal(), non_neg_integer()) -> boolean().
needs_rotate(#wal{seg_bytes = Bytes, seg_max_bytes = Max}, Size) ->
    Bytes > 0 andalso Bytes + Size > Max.

-spec rotate(wal()) -> wal().
rotate(#wal{fd = Fd, dir = Dir, seg_no = No, seg_max_bytes = Max}) ->
    _ = file:sync(Fd),
    _ = file:close(Fd),
    Next = No + 1,
    {ok, NewFd} = file:open(segment_path(Dir, Next), [append, binary, raw, read]),
    #wal{dir = Dir, fd = NewFd, seg_no = Next, seg_max_bytes = Max, seg_bytes = 0}.

-spec read_segment(file:filename_all(), pos_integer()) -> [term()].
read_segment(Dir, No) ->
    case file:read_file(segment_path(Dir, No)) of
        {error, _} -> [];
        {ok, Bin} -> decode_all(Bin, [])
    end.

-spec decode_all(binary(), [term()]) -> [term()].
decode_all(<<>>, Acc) -> lists:reverse(Acc);
decode_all(Bin, Acc) ->
    case decode(Bin) of
        truncated ->
            %% Partial tail write (crash mid-record): stop here. Everything
            %% after a torn record is unreadable by definition.
            lists:reverse(Acc);
        {ok, Term, Size} ->
            <<_:Size/binary, Tail/binary>> = Bin,
            decode_all(Tail, [Term | Acc])
    end.