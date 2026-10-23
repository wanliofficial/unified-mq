%% @doc T5: recovery of a WAL partition directory (ADR-011 §11.2/§11.3/§11.5,
%% Validation F1/F2).
%%
%% Three things are pinned here that the rest of the WAL backend leans on:
%%
%%  * F1 -- the recovered high offset is exactly one past the last durable
%%    record, so a restart neither reuses an offset nor leaves a hole;
%%  * F2 -- recovering something that was never created is an error that leaves
%%    the filesystem untouched, instead of an implicit create that would report
%%    an empty partition as healthy after a typo in the config;
%%  * the watermarks come back LOW or exact, never high: a committed offset past
%%    the durable log is clamped, because §11.3 makes "偏高" the one absolute
%%    prohibition.
%%
%% Corruption is deliberately not repaired. A torn tail is the expected shape of
%% a crash and is cut; a CRC failure means bytes are present but wrong, so
%% recovery refuses to start and leaves them on disk for whoever is called.
-module(wal_recovery_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("broker_core/include/broker_message.hrl").

%% The segment record magic, pinned by wal_codec_SUITE.
-define(MAGIC, 16#55).

all() ->
    [create_makes_an_empty_partition_that_recovers_at_offset_one,
     create_refuses_an_existing_partition,
     recover_on_a_missing_dir_reports_not_found_and_creates_nothing,
     recover_on_an_empty_dir_reports_not_found_and_creates_nothing,
     a_dir_with_only_a_meta_log_is_not_a_partition,
     recover_without_a_meta_log_is_refused,
     high_offset_is_one_past_the_last_durable_record,
     restart_does_not_reuse_or_skip_offsets,
     records_spanning_two_segments_recover_in_order,
     a_torn_tail_is_cut_and_reported,
     a_corrupt_record_refuses_to_start_and_preserves_the_bytes,
     a_newer_record_version_refuses_to_start,
     a_gap_between_segments_refuses_to_start,
     a_torn_tail_in_an_earlier_segment_refuses_to_start,
     anchors_are_rebuilt_at_the_configured_interval,
     a_committed_watermark_above_the_durable_high_is_clamped,
     a_truncate_boundary_above_the_committed_offset_is_clamped,
     a_torn_meta_tail_keeps_the_last_readable_watermarks,
     foreign_files_are_reported_but_do_not_block_recovery].

init_per_testcase(Case, Config) ->
    Dir = filename:join(proplists:get_value(priv_dir, Config), atom_to_list(Case)),
    [{wal_dir, Dir} | Config].

end_per_testcase(_Case, _Config) ->
    ok.

%%%_* Create -------------------------------------------------------------

create_makes_an_empty_partition_that_recovers_at_offset_one(Config) ->
    Dir = priv(Config),
    ok = wal_recovery:create(Dir),
    true = filelib:is_file(wal_meta:path(Dir)),
    true = filelib:is_file(wal_segment:path(Dir, 1)),
    {ok, St} = wal_recovery:recover(1, Dir, #{}),
    #{high := 1, committed := 0, truncate_before := 1, warnings := []} = St,
    #{start_offset := 1, end_pos := 0, records := 0} = maps:get(current, St),
    ok.

%% §11.2 / F2: create is the only way a partition comes into existence, so a
%% second one has to be refused -- including for a directory that is merely
%% empty, which is what a half-finished create leaves behind.
create_refuses_an_existing_partition(Config) ->
    Dir = priv(Config),
    ok = wal_recovery:create(Dir),
    {error, {already_exists, Dir}} = wal_recovery:create(Dir),
    Empty = Dir ++ "-empty",
    ok = filelib:ensure_dir(filename:join(Empty, "x")),
    {error, {already_exists, Empty}} = wal_recovery:create(Empty),
    ok.

%%%_* F2: nothing is created by a failed recover -------------------------

recover_on_a_missing_dir_reports_not_found_and_creates_nothing(Config) ->
    Missing = filename:join(priv(Config), "nope"),
    {error, {not_found, 7}} = wal_recovery:recover(7, Missing, #{}),
    false = filelib:is_dir(Missing),
    ok.

recover_on_an_empty_dir_reports_not_found_and_creates_nothing(Config) ->
    Dir = priv(Config),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    {error, {not_found, 7}} = wal_recovery:recover(7, Dir, #{}),
    {ok, []} = file:list_dir(Dir),
    ok.

a_dir_with_only_a_meta_log_is_not_a_partition(Config) ->
    Dir = priv(Config),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    {ok, Fd} = wal_meta:open_append(Dir),
    ok = wal_meta:close(Fd),
    {error, {not_found, 7}} = wal_recovery:recover(7, Dir, #{}),
    ok.

%% Segments without a meta log means create/1 never finished, so the watermarks
%% are unknown. Guessing "initial" would silently discard every ack ever made.
recover_without_a_meta_log_is_refused(Config) ->
    Dir = priv(Config),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    _ = write_records(Dir, 1, [<<"a">>]),
    {error, {missing_meta, Dir}} = wal_recovery:recover(1, Dir, #{}),
    ok.

%%%_* F1: high offset ----------------------------------------------------

high_offset_is_one_past_the_last_durable_record(Config) ->
    Dir = priv(Config),
    ok = wal_recovery:create(Dir),
    Path = write_records(Dir, 1, [<<"a">>, <<"b">>, <<"c">>]),
    {ok, St} = wal_recovery:recover(1, Dir, #{}),
    #{high := 4, warnings := []} = St,
    #{path := Path, end_pos := EndPos, records := 3} = maps:get(current, St),
    Size = filelib:file_size(Path),
    Size = EndPos,
    ok.

%% The point of F1: append after a restart must continue the sequence, so a
%% second recovery sees four dense offsets and no reused number.
restart_does_not_reuse_or_skip_offsets(Config) ->
    Dir = priv(Config),
    ok = wal_recovery:create(Dir),
    _ = write_records(Dir, 1, [<<"a">>, <<"b">>, <<"c">>]),
    {ok, #{high := High, current := #{path := Path}}} = wal_recovery:recover(1, Dir, #{}),
    4 = High,
    _ = append_at(Path, [wal_codec:encode(msg(<<"d">>))]),
    {ok, St} = wal_recovery:recover(1, Dir, #{}),
    #{high := 5} = St,
    {ok, _Summary, Records} =
        wal_segment:fold(Path, bof, 1, fun collect/4, []),
    [{1, <<"a">>}, {2, <<"b">>}, {3, <<"c">>}, {4, <<"d">>}] =
        [{Offset, Msg#broker_message.payload} || {Offset, Msg, _Pos} <- Records],
    ok.

records_spanning_two_segments_recover_in_order(Config) ->
    Dir = priv(Config),
    ok = wal_recovery:create(Dir),
    _ = write_records(Dir, 1, [<<"a">>, <<"b">>, <<"c">>]),
    _ = write_records(Dir, 4, [<<"d">>, <<"e">>]),
    {ok, St} = wal_recovery:recover(1, Dir, #{}),
    #{high := 6} = St,
    [{1, _}, {4, _}] = maps:get(segments, St),
    #{start_offset := 4, records := 2} = maps:get(current, St),
    ok.

%%%_* Damaged tails ------------------------------------------------------

a_torn_tail_is_cut_and_reported(Config) ->
    Dir = priv(Config),
    ok = wal_recovery:create(Dir),
    Path = write_records(Dir, 1, [<<"a">>, <<"b">>, <<"c">>]),
    GoodSize = filelib:file_size(Path),
    Encoded = wal_codec:encode(msg(<<"d">>)),
    ok = file:write_file(Path, binary:part(Encoded, 0, 7), [append]),
    {ok, St} = wal_recovery:recover(1, Dir, #{}),
    #{high := 4, warnings := [{torn_tail_cut, Path, 7}]} = St,
    GoodSize = filelib:file_size(Path),
    ok.

a_corrupt_record_refuses_to_start_and_preserves_the_bytes(Config) ->
    Dir = priv(Config),
    ok = wal_recovery:create(Dir),
    Path = write_records(Dir, 1, [<<"a">>, <<"b">>, <<"c">>]),
    {ok, _Summary, Records} = wal_segment:fold(Path, bof, 1, fun collect/4, []),
    {_Offset, _Msg, PosOfSecond} = lists:nth(2, Records),
    corrupt_byte_at(Path, PosOfSecond + 15),
    SizeBefore = filelib:file_size(Path),
    {error, {corrupt_record, Path, {crc_mismatch, _, _}}} =
        wal_recovery:recover(1, Dir, #{}),
    %% The bytes stay put: truncating on a CRC failure would destroy the only
    %% evidence that something other than a crash went wrong.
    SizeBefore = filelib:file_size(Path),
    ok.

%% §11.5: a record written by a newer format is refused, never guessed at.
a_newer_record_version_refuses_to_start(Config) ->
    Dir = priv(Config),
    ok = wal_recovery:create(Dir),
    Path = write_records(Dir, 1, [<<"a">>]),
    ok = append_raw(Path, bump_vsn(wal_codec:encode(msg(<<"b">>)), 9)),
    {error, {unsupported_vsn, Path, 9}} = wal_recovery:recover(1, Dir, #{}),
    ok.

%%%_* Segment sequence ---------------------------------------------------

a_gap_between_segments_refuses_to_start(Config) ->
    Dir = priv(Config),
    ok = wal_recovery:create(Dir),
    _ = write_records(Dir, 1, [<<"a">>, <<"b">>, <<"c">>]),
    _ = write_records(Dir, 9, [<<"x">>]),
    {error, {segment_gap, 4, 9}} = wal_recovery:recover(1, Dir, #{}),
    ok.

%% Only the last segment may be torn: cutting an earlier one would lose offsets
%% that a later segment already assumes are behind it.
a_torn_tail_in_an_earlier_segment_refuses_to_start(Config) ->
    Dir = priv(Config),
    ok = wal_recovery:create(Dir),
    First = write_records(Dir, 1, [<<"a">>, <<"b">>, <<"c">>]),
    ok = file:write_file(First, binary:part(wal_codec:encode(msg(<<"d">>)), 0, 7), [append]),
    _ = write_records(Dir, 4, [<<"x">>]),
    {error, {torn_earlier_segment, First}} = wal_recovery:recover(1, Dir, #{}),
    ok.

%%%_* Index rebuild ------------------------------------------------------

anchors_are_rebuilt_at_the_configured_interval(Config) ->
    Dir = priv(Config),
    ok = wal_recovery:create(Dir),
    Path = write_records(Dir, 1, [<<"a">>, <<"b">>, <<"c">>, <<"d">>, <<"e">>]),
    {ok, St} = wal_recovery:recover(1, Dir, #{index_anchor_interval => 2}),
    Tid = maps:get(index, St),
    {ok, 1, Path, 0} = wal_index:locate(Tid, 1),
    {ok, 1, Path, 0} = wal_index:locate(Tid, 2),
    {ok, 3, Path, Pos3} = wal_index:locate(Tid, 3),
    {ok, 3, Path, Pos3} = wal_index:locate(Tid, 4),
    {ok, 5, Path, Pos5} = wal_index:locate(Tid, 5),
    {ok, 5, Path, Pos5} = wal_index:locate(Tid, 99),
    ok.

%%%_* Watermark clamping -------------------------------------------------

%% ADR-011 T14's injection at the recovery level: an ack was recorded but the
%% records it covered did not survive. The watermark must come back low.
a_committed_watermark_above_the_durable_high_is_clamped(Config) ->
    Dir = priv(Config),
    ok = wal_recovery:create(Dir),
    _ = write_records(Dir, 1, [<<"a">>, <<"b">>, <<"c">>]),
    ok = write_meta(Dir, 100, 50),
    {ok, St} = wal_recovery:recover(1, Dir, #{}),
    #{high := 4, committed := 3, truncate_before := 4, warnings := Warnings} = St,
    true = lists:member({committed_above_high, 100, 3}, Warnings),
    true = lists:member({truncate_above_committed, 50, 4}, Warnings),
    ok.

a_truncate_boundary_above_the_committed_offset_is_clamped(Config) ->
    Dir = priv(Config),
    ok = wal_recovery:create(Dir),
    _ = write_records(Dir, 1, [<<"a">>, <<"b">>, <<"c">>]),
    ok = write_meta(Dir, 2, 10),
    {ok, St} = wal_recovery:recover(1, Dir, #{}),
    #{committed := 2, truncate_before := 3,
      warnings := [{truncate_above_committed, 10, 3}]} = St,
    ok.

a_torn_meta_tail_keeps_the_last_readable_watermarks(Config) ->
    Dir = priv(Config),
    ok = wal_recovery:create(Dir),
    _ = write_records(Dir, 1, [<<"a">>, <<"b">>, <<"c">>]),
    ok = write_meta(Dir, 2, 1),
    Encoded = wal_meta:encode(3, 2),
    ok = file:write_file(wal_meta:path(Dir),
                         binary:part(Encoded, 0, byte_size(Encoded) div 2),
                         [append]),
    {ok, St} = wal_recovery:recover(1, Dir, #{}),
    #{committed := 2, truncate_before := 1,
      warnings := [{meta_tail, {torn, truncated}}]} = St,
    ok.

foreign_files_are_reported_but_do_not_block_recovery(Config) ->
    Dir = priv(Config),
    ok = wal_recovery:create(Dir),
    _ = write_records(Dir, 1, [<<"a">>]),
    ok = file:write_file(filename:join(Dir, "junk.txt"), <<"x">>),
    {ok, St} = wal_recovery:recover(1, Dir, #{}),
    #{high := 2, warnings := [{unexpected_file, "junk.txt"}]} = St,
    ok.

%%%_* Helpers ------------------------------------------------------------

priv(Config) ->
    proplists:get_value(wal_dir, Config).

collect(Offset, Msg, Pos, Acc) ->
    {cont, Acc ++ [{Offset, Msg, Pos}]}.

write_records(Dir, StartOffset, Payloads) ->
    append_at(wal_segment:path(Dir, StartOffset),
              [wal_codec:encode(msg(Payload)) || Payload <- Payloads]).

append_at(Path, Bins) ->
    {ok, Fd} = wal_segment:open_append(Path),
    ok = append_all(Fd, Bins),
    ok = wal_segment:sync(Fd),
    ok = wal_segment:close(Fd),
    Path.

append_raw(Path, Bin) ->
    {ok, Fd} = wal_segment:open_append(Path),
    ok = wal_segment:append(Fd, Bin),
    ok = wal_segment:sync(Fd),
    wal_segment:close(Fd).

append_all(_Fd, []) -> ok;
append_all(Fd, [Bin | Rest]) ->
    ok = wal_segment:append(Fd, Bin),
    append_all(Fd, Rest).

write_meta(Dir, Committed, TruncateBefore) ->
    {ok, Fd} = wal_meta:open_append(Dir),
    ok = wal_meta:write(Fd, Committed, TruncateBefore),
    wal_meta:close(Fd).

%% Rewrite the version byte of a valid record and re-cover it with a correct
%% CRC, so the only thing wrong is the format version.
bump_vsn(<<?MAGIC:8, _Vsn:8, _Crc:32, Len:32, Body/binary>>, NewVsn) ->
    <<?MAGIC:8, NewVsn:8, (erlang:crc32(Body)):32, Len:32, Body/binary>>.

corrupt_byte_at(Path, Pos) ->
    {ok, Bin} = file:read_file(Path),
    <<Head:Pos/binary, Byte, Rest/binary>> = Bin,
    ok = file:write_file(Path, <<Head:Pos/binary, (Byte bxor 16#FF), Rest/binary>>).

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
