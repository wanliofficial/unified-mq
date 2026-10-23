%% @doc Per-partition watermark log (ADR-011 §11.3).
%%
%% `recover/2' must return the committed offset without replaying anything, so
%% the two watermarks (`committed' and `truncate_before') live in their own
%% append-only file beside the segments. Only the last readable entry matters;
%% earlier ones are history.
%%
%% Append-only is what makes a crash safe here: losing the tail entry brings the
%% watermarks back LOW, and §11.3 explicitly allows that (the result is a
%% duplicate delivery, which is in contract) while absolutely forbidding a
%% watermark that claims an offset which was not written. A torn or unreadable
%% tail is therefore not an error -- `read/1' reports it so recovery can log it
%% and move on.
%%
%% The framing matches `wal_codec' (magic, vsn, crc32, len) but the magic, the
%% version and the body are independent: the two logs version separately, and a
%% distinct magic means pointing one reader at the other's file fails loudly
%% instead of producing a plausible-looking watermark.
%%
%% S1 does not compact this file, so it grows with the number of watermark
%% updates. That is the same trade §7.3 already accepts for segments; retention
%% is an S2 item.
-module(wal_meta).

-export([filename/0, path/1, encode/2,
         open_append/1, write/3, close/1, read/1]).

-define(FILENAME, "meta.log").
%% 'M'. The segment magic is 'U'; wal_meta_SUITE pins both directions.
-define(MAGIC, 16#4D).
-define(VSN, 1).
%% Magic:8 + Vsn:8 + CRC32:32 + Len:32 = 80 bits.
-define(HEADER_SIZE, 10).
-define(CHUNK_BYTES, 1 bsl 16).

-type watermarks() :: #{committed := non_neg_integer(),
                        truncate_before := non_neg_integer()}.
-type tail() :: clean | {torn, truncated} | {corrupt, term()}.

-export_type([watermarks/0, tail/0]).

-spec filename() -> file:filename().
filename() -> ?FILENAME.

-spec path(file:filename_all()) -> file:filename_all().
path(Dir) -> filename:join(Dir, ?FILENAME).

-spec encode(non_neg_integer(), non_neg_integer()) -> binary().
encode(Committed, TruncateBefore) ->
    Body = term_to_binary({?VSN, Committed, TruncateBefore}),
    <<?MAGIC:8, ?VSN:8, (erlang:crc32(Body)):32, (byte_size(Body)):32, Body/binary>>.

-spec open_append(file:filename_all()) -> {ok, file:io_device()} | {error, term()}.
open_append(Dir) ->
    Path = path(Dir),
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

%% Append plus fsync, unconditionally. A watermark that is not on disk is worth
%% nothing after a crash, and batching this would only move the fsync into the
%% caller -- which is where the subscription layer already does it for cursors
%% (design §5.3), so `write/3' is called once per cursor flush, not once per
%% message.
-spec write(file:io_device(), non_neg_integer(), non_neg_integer()) -> ok | {error, term()}.
write(Fd, Committed, TruncateBefore) ->
    case file:write(Fd, encode(Committed, TruncateBefore)) of
        ok -> file:sync(Fd);
        {error, Reason} -> {error, Reason}
    end.

-spec close(file:io_device()) -> ok | {error, term()}.
close(Fd) ->
    file:close(Fd).

%% `not_found' means there is no meta.log at all, which is how `wal_recovery'
%% tells "never created" from "created and still at the initial watermarks".
-spec read(file:filename_all()) -> {ok, watermarks(), tail()} | not_found | {error, term()}.
read(Dir) ->
    Path = path(Dir),
    case file:open(Path, [raw, binary, read]) of
        {ok, Fd} ->
            try
                scan(Fd, initial(), <<>>)
            after
                _ = file:close(Fd)
            end;
        {error, enoent} ->
            not_found;
        {error, Reason} ->
            {error, {open_failed, Path, Reason}}
    end.

%%%_* Internal -----------------------------------------------------------

%% Offsets start at 1 (design §7.1): `committed = 0' means nothing was ever
%% acknowledged and `truncate_before = 1' means nothing has been discarded.
-spec initial() -> watermarks().
initial() -> #{committed => 0, truncate_before => 1}.

scan(Fd, State, Buf) ->
    case decode_entry(Buf) of
        {ok, Entry, Used} ->
            <<_:Used/binary, Rest/binary>> = Buf,
            scan(Fd, Entry, Rest);
        truncated ->
            case file:read(Fd, ?CHUNK_BYTES) of
                {ok, Data} ->
                    scan(Fd, State, <<Buf/binary, Data/binary>>);
                eof ->
                    {ok, State, tail_of(Buf)};
                {error, Reason} ->
                    {error, {read_failed, Reason}}
            end;
        %% Nothing past an unreadable entry can be located, so stop here.
        {error, Reason} ->
            {ok, State, {corrupt, Reason}}
    end.

decode_entry(<<Magic:8, _Vsn:8, _Crc:32, _Len:32, _Rest/binary>>) when Magic =/= ?MAGIC ->
    {error, {bad_magic, Magic}};
decode_entry(<<?MAGIC:8, Vsn:8, Crc:32, Len:32, Rest/binary>>) when byte_size(Rest) >= Len ->
    <<Body:Len/binary, _Tail/binary>> = Rest,
    case erlang:crc32(Body) of
        Crc -> decode_body(Vsn, Body, ?HEADER_SIZE + Len);
        Actual -> {error, {crc_mismatch, Crc, Actual}}
    end;
decode_entry(_Incomplete) ->
    truncated.

decode_body(?VSN, Body, Size) ->
    try binary_to_term(Body, [safe]) of
        {?VSN, Committed, TruncateBefore} ->
            {ok, #{committed => Committed, truncate_before => TruncateBefore}, Size};
        Other ->
            {error, {bad_body, Other}}
    catch
        _:Reason ->
            {error, {bad_body, Reason}}
    end;
%% §11.5's rule applies to this log too: a newer format is refused, not guessed.
decode_body(Vsn, _Body, _Size) ->
    {error, {unsupported_vsn, Vsn}}.

tail_of(<<>>) -> clean;
tail_of(_Leftover) -> {torn, truncated}.
