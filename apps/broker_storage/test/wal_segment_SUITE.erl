%% @doc T5: tests for WAL segment files -- naming, listing, appending and the
%% streaming fold that both recovery and read_from are built on.
%%
%% The fold is the interesting part: it must number records from the segment's
%% start offset, tell a torn tail (bytes missing -- the expected crash shape)
%% apart from a corrupt record (bytes present but wrong), and report the exact
%% file position a writer has to resume from.
-module(wal_segment_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("broker_core/include/broker_message.hrl").

all() ->
    [path_uses_the_frozen_naming,
     list_orders_segments_and_reports_foreign_files,
     list_on_a_missing_dir_is_an_error,
     fold_on_an_empty_segment_reports_no_records,
     append_then_fold_numbers_records_from_the_segment_start,
     fold_from_an_anchor_starts_at_that_record,
     tiny_chunk_fold_still_decodes_every_record,
     halting_fun_stops_the_fold_without_a_summary,
     torn_tail_is_reported_with_the_resume_position,
     corrupt_record_is_distinguished_from_a_torn_tail,
     fold_on_a_file_that_is_not_a_segment_is_refused,
     reopen_appends_without_truncating,
     torn_tail_is_cut_before_new_records_are_appended].

%% CT hands every testcase in a suite the SAME priv_dir, so without a per-case
%% subdirectory each case would append to the previous one's segment.
init_per_testcase(Case, Config) ->
    Dir = filename:join(proplists:get_value(priv_dir, Config), atom_to_list(Case)),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    [{wal_dir, Dir} | Config].

end_per_testcase(_Case, _Config) ->
    ok.

%%%_* Tests --------------------------------------------------------------

%% design §7.3: data/wal/{partition_id}/segment-{StartOffset:020d}.log
path_uses_the_frozen_naming(_Config) ->
    "/var/lib/mq/wal/3/segment-00000000000000000042.log" =
        wal_segment:path("/var/lib/mq/wal/3", 42),
    ok.

list_orders_segments_and_reports_foreign_files(Config) ->
    Dir = priv(Config),
    %% Written out of order on purpose: the listing has to sort by start offset.
    lists:foreach(fun(Off) -> ok = touch(wal_segment:path(Dir, Off)) end, [101, 1, 51]),
    ok = touch(filename:join(Dir, "meta.log")),
    ok = touch(filename:join(Dir, "segment-notanumber.log")),
    {ok, #{segments := Segments, unexpected := Unexpected}} = wal_segment:list(Dir),
    [1, 51, 101] = [Offset || {Offset, _Path} <- Segments],
    Expected = [wal_segment:path(Dir, Off) || Off <- [1, 51, 101]],
    Expected = [Path || {_Offset, Path} <- Segments],
    %% Anything that is not a segment file is reported, not silently skipped.
    %% Recognising our own meta.log is the caller's job (wal_recovery), because
    %% wal_segment has no business knowing what else lives in the directory.
    ["meta.log", "segment-notanumber.log"] = lists:sort(Unexpected),
    ok.

list_on_a_missing_dir_is_an_error(Config) ->
    {error, _} = wal_segment:list(filename:join(priv(Config), "nope")),
    ok.

fold_on_an_empty_segment_reports_no_records(Config) ->
    Path = write_records(priv(Config), 1, []),
    {ok, Summary, []} = wal_segment:fold(Path, bof, 1, fun collect/4, []),
    #{next_offset := 1, end_pos := 0, file_size := 0, tail := clean} = Summary,
    ok.

append_then_fold_numbers_records_from_the_segment_start(Config) ->
    %% The segment is named for its first offset, so a segment starting at 101
    %% numbers its records 101..103 -- offsets are implicit, never stored.
    Path = write_records(priv(Config), 101, [<<"a">>, <<"b">>, <<"c">>]),
    {ok, Summary, Records} = wal_segment:fold(Path, bof, 7, fun collect/4, []),
    [{101, <<"a">>}, {102, <<"b">>}, {103, <<"c">>}] =
        [{Offset, Msg#broker_message.payload} || {Offset, Msg, _Pos} <- Records],
    #{next_offset := 104, tail := clean} = Summary,
    Size = filelib:file_size(Path),
    Size = maps:get(end_pos, Summary),
    Size = maps:get(file_size, Summary),
    %% The partition id on disk comes from the caller, not the record.
    [7, 7, 7] = [Msg#broker_message.partition_id || {_Offset, Msg, _Pos} <- Records],
    ok.

%% read_from resumes through a sparse index anchor, which carries both the
%% offset and the file position -- the fold cannot recover the offset from a
%% bare position, and scanning from bof to find it would defeat the index.
fold_from_an_anchor_starts_at_that_record(Config) ->
    Path = write_records(priv(Config), 1, [<<"a">>, <<"b">>, <<"c">>, <<"d">>]),
    {ok, _Summary, All} = wal_segment:fold(Path, bof, 1, fun collect/4, []),
    {3, _Msg, PosOfThird} = lists:nth(3, All),
    {ok, _Summary2, Tail} = wal_segment:fold(Path, {3, PosOfThird}, 1, fun collect/4, []),
    [{3, <<"c">>}, {4, <<"d">>}] =
        [{Offset, Msg#broker_message.payload} || {Offset, Msg, _Pos} <- Tail],
    ok.

%% The production chunk is far larger than a record, so the boundary path only
%% gets exercised here: a record split across two reads must still decode.
tiny_chunk_fold_still_decodes_every_record(Config) ->
    Path = write_records(priv(Config), 1, [<<"a">>, <<"b">>, <<"c">>]),
    {ok, Summary, Records} =
        wal_segment:fold(Path, bof, 1, fun collect/4, [], #{chunk_bytes => 1}),
    3 = length(Records),
    #{next_offset := 4, tail := clean} = Summary,
    ok.

%% A halted fold has scanned neither the tail nor the end of the segment, so it
%% returns no summary: a partial one would look exactly like a complete one.
halting_fun_stops_the_fold_without_a_summary(Config) ->
    Path = write_records(priv(Config), 1, lists:duplicate(50, <<"x">>)),
    Fun = fun(Offset, Msg, _Pos, Acc) when length(Acc) < 2 ->
                 {cont, [{Offset, Msg} | Acc]};
             (_Offset, _Msg, _Pos, Acc) ->
                 {halt, Acc}
          end,
    {halt, Records} = wal_segment:fold(Path, bof, 1, Fun, []),
    2 = length(Records),
    ok.

%% A crash mid-write leaves a partial record. That is not corruption and not an
%% error: everything before it is durable, and end_pos is where writing resumes.
torn_tail_is_reported_with_the_resume_position(Config) ->
    Path = write_records(priv(Config), 1, [<<"a">>, <<"b">>, <<"c">>]),
    {ok, Clean, _} = wal_segment:fold(Path, bof, 1, fun collect/4, []),
    GoodEnd = maps:get(end_pos, Clean),
    %% Simulate the torn write: a header plus half a body.
    Encoded = wal_codec:encode(msg(<<"d">>)),
    ok = file:write_file(Path, binary:part(Encoded, 0, byte_size(Encoded) div 2), [append]),
    {ok, Torn, Records} = wal_segment:fold(Path, bof, 1, fun collect/4, []),
    3 = length(Records),
    #{next_offset := 4, tail := {torn, truncated}} = Torn,
    GoodEnd = maps:get(end_pos, Torn),
    FileSize = filelib:file_size(Path),
    FileSize = maps:get(file_size, Torn),
    true = FileSize > GoodEnd,
    ok.

corrupt_record_is_distinguished_from_a_torn_tail(Config) ->
    Path = write_records(priv(Config), 1, [<<"a">>, <<"b">>, <<"c">>]),
    {ok, _Clean, Records} = wal_segment:fold(Path, bof, 1, fun collect/4, []),
    {_Offset, _Msg, PosOfSecond} = lists:nth(2, Records),
    corrupt_byte_at(Path, PosOfSecond + 15),
    {ok, Summary, Readable} = wal_segment:fold(Path, bof, 1, fun collect/4, []),
    1 = length(Readable),
    #{next_offset := 2, tail := {corrupt, {crc_mismatch, _, _}}} = Summary,
    ok.

fold_on_a_file_that_is_not_a_segment_is_refused(Config) ->
    Path = filename:join(priv(Config), "whatever.log"),
    ok = touch(Path),
    {error, {bad_segment_name, Path}} = wal_segment:fold(Path, bof, 1, fun collect/4, []),
    ok.

reopen_appends_without_truncating(Config) ->
    Dir = priv(Config),
    Path = write_records(Dir, 1, [<<"a">>, <<"b">>]),
    Path = append_to(Path, [<<"c">>]),
    {ok, Summary, Records} = wal_segment:fold(Path, bof, 1, fun collect/4, []),
    3 = length(Records),
    #{next_offset := 4, tail := clean} = Summary,
    ok.

%% Recovery truncates the torn tail; the next record has to land exactly where
%% the last good one ended, otherwise it would be written after unreadable
%% garbage and be lost forever.
torn_tail_is_cut_before_new_records_are_appended(Config) ->
    Path = write_records(priv(Config), 1, [<<"a">>, <<"b">>, <<"c">>]),
    Encoded = wal_codec:encode(msg(<<"d">>)),
    ok = file:write_file(Path, binary:part(Encoded, 0, 7), [append]),
    {ok, Torn, _} = wal_segment:fold(Path, bof, 1, fun collect/4, []),
    ResumeAt = maps:get(end_pos, Torn),
    {ok, Fd} = wal_segment:open_append(Path),
    ok = wal_segment:truncate_at(Fd, ResumeAt),
    ok = wal_segment:append(Fd, Encoded),
    ok = wal_segment:sync(Fd),
    ok = wal_segment:close(Fd),
    {ok, Summary, Records} = wal_segment:fold(Path, bof, 1, fun collect/4, []),
    [{1, <<"a">>}, {2, <<"b">>}, {3, <<"c">>}, {4, <<"d">>}] =
        [{Offset, Msg#broker_message.payload} || {Offset, Msg, _Pos} <- Records],
    #{next_offset := 5, tail := clean} = Summary,
    ok.

%%%_* Helpers ------------------------------------------------------------

collect(Offset, Msg, Pos, Acc) ->
    {cont, Acc ++ [{Offset, Msg, Pos}]}.

priv(Config) ->
    proplists:get_value(wal_dir, Config).

write_records(Dir, StartOffset, Payloads) ->
    append_to(wal_segment:path(Dir, StartOffset), Payloads).

%% Offsets are implicit in the record stream, so appending a payload is all it
%% takes; the segment's own name says which offset the first one gets.
append_to(Path, Payloads) ->
    {ok, Fd} = wal_segment:open_append(Path),
    lists:foreach(fun(Payload) ->
                      ok = wal_segment:append(Fd, wal_codec:encode(msg(Payload)))
                  end,
                  Payloads),
    ok = wal_segment:sync(Fd),
    ok = wal_segment:close(Fd),
    Path.

msg(Payload) ->
    #broker_message{message_id = <<"m">>,
                    tenant_id = <<"t0">>,
                    destination = <<"d">>,
                    partition_id = 1,
                    payload = Payload,
                    payload_encoding = raw,
                    headers = #{},
                    proto_meta = #{},
                    persistence = durable,
                    reliability = os_buffer,
                    created_at = 1,
                    expires_at = undefined,
                    event = undefined}.

touch(Path) ->
    file:write_file(Path, <<>>).

%% Flip one byte inside a record body (past the 10-byte header) so the CRC no
%% longer matches while the framing still looks valid.
corrupt_byte_at(Path, Pos) ->
    {ok, Bin} = file:read_file(Path),
    <<Head:Pos/binary, Byte, Rest/binary>> = Bin,
    ok = file:write_file(Path, <<Head:Pos/binary, (Byte bxor 16#FF), Rest/binary>>).
