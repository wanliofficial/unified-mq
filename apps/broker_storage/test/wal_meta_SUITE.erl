%% @doc T5: tests for the per-partition watermark log.
%%
%% `recover/2' has to return the committed offset without replaying anything,
%% so the watermarks live in their own append-only file next to the segments.
%% The contract that makes an append-only log safe here is ADR-011 §11.3: a
%% watermark may come back LOW after a crash (that is a duplicate, which is in
%% contract) but must never come back high. Losing the last entry is therefore
%% the correct failure mode, and these tests pin that it is the *observed* one
%% rather than a crash or a fabricated value.
-module(wal_meta_SUITE).

-compile([export_all, nowarn_export_all]).

all() ->
    [path_is_meta_log_inside_the_partition_dir,
     magic_is_not_the_segment_magic,
     read_on_a_missing_file_is_not_found,
     an_empty_log_reads_as_the_initial_watermarks,
     write_then_read_roundtrips_both_watermarks,
     the_last_entry_wins,
     watermarks_survive_a_reopen_without_truncating,
     a_torn_last_entry_is_skipped_and_reported,
     a_corrupt_last_entry_is_skipped_and_reported].

%% CT hands every testcase in a suite the SAME priv_dir, so each case needs its
%% own subdirectory or it reads the previous case's watermarks.
init_per_testcase(Case, Config) ->
    Dir = filename:join(proplists:get_value(priv_dir, Config), atom_to_list(Case)),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    [{wal_dir, Dir} | Config].

end_per_testcase(_Case, _Config) ->
    ok.

%%%_* Tests --------------------------------------------------------------

path_is_meta_log_inside_the_partition_dir(Config) ->
    Dir = priv(Config),
    Expected = Dir ++ "/meta.log",
    Expected = wal_meta:path(Dir),
    "meta.log" = wal_meta:filename(),
    ok.

%% A meta entry fed to the segment decoder must fail loudly. Both logs sit in
%% the same directory, so a shared magic would turn a mis-pointed read into a
%% plausible-looking watermark instead of an error.
magic_is_not_the_segment_magic(Config) ->
    Dir = priv(Config),
    {ok, Fd} = wal_meta:open_append(Dir),
    ok = wal_meta:write(Fd, 5, 3),
    ok = wal_meta:close(Fd),
    {ok, Bytes} = file:read_file(wal_meta:path(Dir)),
    {error, {bad_magic, Magic}} = wal_codec:decode(Bytes, 1),
    %% 'M'; the segment magic is 'U' (wal_codec_SUITE pins that one).
    16#4D = Magic,
    ok.

read_on_a_missing_file_is_not_found(Config) ->
    not_found = wal_meta:read(priv(Config)),
    ok.

an_empty_log_reads_as_the_initial_watermarks(Config) ->
    Dir = priv(Config),
    {ok, Fd} = wal_meta:open_append(Dir),
    ok = wal_meta:close(Fd),
    %% Spelled out rather than compared against wal_meta:initial/0, which would
    %% pass no matter what that function returned.
    {ok, #{committed := 0, truncate_before := 1}, clean} = wal_meta:read(Dir),
    ok.

write_then_read_roundtrips_both_watermarks(Config) ->
    Dir = priv(Config),
    {ok, Fd} = wal_meta:open_append(Dir),
    ok = wal_meta:write(Fd, 4242, 4001),
    ok = wal_meta:close(Fd),
    {ok, #{committed := 4242, truncate_before := 4001}, clean} = wal_meta:read(Dir),
    ok.

the_last_entry_wins(Config) ->
    Dir = priv(Config),
    {ok, Fd} = wal_meta:open_append(Dir),
    ok = wal_meta:write(Fd, 1, 1),
    ok = wal_meta:write(Fd, 2, 1),
    ok = wal_meta:write(Fd, 3, 2),
    ok = wal_meta:close(Fd),
    {ok, #{committed := 3, truncate_before := 2}, clean} = wal_meta:read(Dir),
    ok.

watermarks_survive_a_reopen_without_truncating(Config) ->
    Dir = priv(Config),
    {ok, Fd1} = wal_meta:open_append(Dir),
    ok = wal_meta:write(Fd1, 10, 1),
    ok = wal_meta:close(Fd1),
    {ok, Fd2} = wal_meta:open_append(Dir),
    ok = wal_meta:write(Fd2, 20, 11),
    ok = wal_meta:close(Fd2),
    {ok, #{committed := 20, truncate_before := 11}, clean} = wal_meta:read(Dir),
    ok.

%% The crash shape: half an entry on disk. The previous watermark is what
%% survives, and the tail says the last entry was cut rather than unreadable.
a_torn_last_entry_is_skipped_and_reported(Config) ->
    Dir = priv(Config),
    {ok, Fd} = wal_meta:open_append(Dir),
    ok = wal_meta:write(Fd, 100, 50),
    ok = wal_meta:close(Fd),
    Encoded = wal_meta:encode(200, 150),
    ok = file:write_file(wal_meta:path(Dir),
                         binary:part(Encoded, 0, byte_size(Encoded) div 2),
                         [append]),
    {ok, #{committed := 100, truncate_before := 50}, {torn, truncated}} = wal_meta:read(Dir),
    ok.

a_corrupt_last_entry_is_skipped_and_reported(Config) ->
    Dir = priv(Config),
    {ok, Fd} = wal_meta:open_append(Dir),
    ok = wal_meta:write(Fd, 100, 50),
    ok = wal_meta:write(Fd, 200, 150),
    ok = wal_meta:close(Fd),
    Path = wal_meta:path(Dir),
    %% The last byte of the file is the last byte of the last entry's body, so
    %% flipping it cannot land in the header.
    Size = filelib:file_size(Path),
    corrupt_byte_at(Path, Size - 1),
    {ok, #{committed := 100, truncate_before := 50}, {corrupt, {crc_mismatch, _, _}}} =
        wal_meta:read(Dir),
    ok.

%%%_* Helpers ------------------------------------------------------------

priv(Config) ->
    proplists:get_value(wal_dir, Config).

corrupt_byte_at(Path, Pos) ->
    {ok, Bin} = file:read_file(Path),
    <<Head:Pos/binary, Byte, Rest/binary>> = Bin,
    ok = file:write_file(Path, <<Head:Pos/binary, (Byte bxor 16#FF), Rest/binary>>).
