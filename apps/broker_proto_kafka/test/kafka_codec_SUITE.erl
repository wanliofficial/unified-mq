%% @doc S5 Kafka wire codec: primitives, varints, compact values, the v2
%% RecordBatch and CRC32C. Pure functions, no sockets; the connection
%% behaviour lives in `kafka_conn_SUITE' and the third-party-client evidence
%% in `test/protocol-compatibility/kafka/'.
-module(kafka_codec_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [uvarint_roundtrip,
     varint_roundtrip,
     string_bytes_array_roundtrip,
     compact_values_roundtrip,
     crc32c_known_vector,
     record_batch_roundtrip,
     record_batch_multiple_records,
     record_batch_headers_and_timestamps,
     record_batch_crc_detects_corruption,
     record_batch_truncated,
     empty_batch].

suite() -> [{timetrap, {seconds, 20}}].

uvarint_roundtrip(_Config) ->
    lists:foreach(
      fun(V) ->
              Bin = kafka_codec:uvarint(V),
              {V, <<"tail">>} = kafka_codec:get_uvarint(<<Bin/binary, "tail">>)
      end, [0, 1, 2, 127, 128, 129, 300, 16384, 1 bsl 20, 1 bsl 40]),
    ok.

varint_roundtrip(_Config) ->
    lists:foreach(
      fun(V) ->
              Bin = kafka_codec:varint(V),
              {V, <<>>} = kafka_codec:get_varint(Bin)
      end, [-2147483648, -1000000, -300, -2, -1, 0, 1, 2, 127, 300,
            1000000, 2147483647]),
    lists:foreach(
      fun(V) ->
              Bin = kafka_codec:varlong(V),
              {V, <<>>} = kafka_codec:get_varlong(Bin)
      end, [-(1 bsl 40), -1, 0, 1, 1 bsl 40]),
    ok.

string_bytes_array_roundtrip(_Config) ->
    S = kafka_codec:string(<<"alpha">>),
    {<<"alpha">>, <<"rest">>} = kafka_codec:get_string(<<S/binary, "rest">>),
    ?assertEqual(kafka_codec:int16(-1), kafka_codec:nullable_string(undefined)),
    B = kafka_codec:bytes(<<"payload">>),
    {<<"payload">>, <<>>} = kafka_codec:get_bytes(B),
    A = iolist_to_binary(
          kafka_codec:array(fun kafka_codec:string/1, [<<"a">>, <<"b">>])),
    {[<<"a">>, <<"b">>], <<>>} =
        kafka_codec:get_array(fun kafka_codec:get_string/1, A),
    ?assertEqual(kafka_codec:int32(-1), kafka_codec:nullable_array(fun(X) -> X end, undefined)),
    ok.

compact_values_roundtrip(_Config) ->
    C = iolist_to_binary(kafka_codec:compact_string(<<"flex">>)),
    {<<"flex">>, <<>>} = kafka_codec:get_compact_string(C),
    ?assertEqual(kafka_codec:uvarint(0), kafka_codec:compact_string(undefined)),
    A = iolist_to_binary(
          kafka_codec:compact_array(fun kafka_codec:compact_string/1,
                                    [<<"x">>, <<"yz">>])),
    {[<<"x">>, <<"yz">>], <<>>} =
        kafka_codec:get_compact_array(fun kafka_codec:get_compact_string/1, A),
    {#{}, <<>>} = kafka_codec:get_tagged_fields(kafka_codec:tagged_fields(#{})),
    ok.

crc32c_known_vector(_Config) ->
    %% RFC 3720 / Castagnoli check value.
    ?assertEqual(16#E3069283, kafka_codec:crc32c(<<"123456789">>)),
    ?assertEqual(0, kafka_codec:crc32c(<<>>)),
    ok.

record_batch_roundtrip(_Config) ->
    Records = [#{offset => 5, timestamp => 1700000000000,
                 key => <<"k">>, value => <<"v">>, headers => []}],
    Bin = iolist_to_binary(kafka_codec:encode_record_batch(5, Records)),
    {ok, [#{base_offset := 5, records := [R]}], <<>>} =
        kafka_codec:decode_record_batches(Bin),
    ?assertEqual(5, maps:get(offset, R)),
    ?assertEqual(1700000000000, maps:get(timestamp, R)),
    ?assertEqual(<<"k">>, maps:get(key, R)),
    ?assertEqual(<<"v">>, maps:get(value, R)),
    ok.

record_batch_multiple_records(_Config) ->
    Records = [#{offset => N, timestamp => 1000 + N,
                 key => <<"k", (integer_to_binary(N))/binary>>,
                 value => <<"v", (integer_to_binary(N))/binary>>,
                 headers => []} || N <- [10, 11, 12]],
    Bin = iolist_to_binary(kafka_codec:encode_record_batch(10, Records)),
    {ok, [#{base_offset := 10, records := Decoded}], <<>>} =
        kafka_codec:decode_record_batches(Bin),
    ?assertEqual([10, 11, 12], [maps:get(offset, R) || R <- Decoded]),
    ?assertEqual([<<"v10">>, <<"v11">>, <<"v12">>],
                 [maps:get(value, R) || R <- Decoded]),
    ok.

record_batch_headers_and_timestamps(_Config) ->
    Records = [#{offset => 0, timestamp => 1234, key => undefined,
                 value => <<"body">>,
                 headers => [{<<"trace">>, <<"abc">>},
                             {<<"empty">>, <<>>}]}],
    Bin = iolist_to_binary(kafka_codec:encode_record_batch(0, Records)),
    {ok, [#{records := [R]}], <<>>} = kafka_codec:decode_record_batches(Bin),
    ?assertEqual(undefined, maps:get(key, R)),
    ?assertEqual(1234, maps:get(timestamp, R)),
    ?assertEqual([{<<"trace">>, <<"abc">>}, {<<"empty">>, <<>>}],
                 maps:get(headers, R)),
    ok.

record_batch_crc_detects_corruption(_Config) ->
    Records = [#{offset => 0, timestamp => 1, key => undefined,
                 value => <<"corrupt-me">>, headers => []}],
    Bin = iolist_to_binary(kafka_codec:encode_record_batch(0, Records)),
    Last = byte_size(Bin) - 1,
    Bad = binary:replace(Bin, <<(binary:at(Bin, Last))>>, <<16#FF>>,
                         [{scope, {Last, 1}}]),
    ?assertMatch({error, {crc_mismatch, 0}},
                 kafka_codec:decode_record_batches(Bad)),
    ok.

record_batch_truncated(_Config) ->
    Records = [#{offset => 0, timestamp => 1, key => undefined,
                 value => <<"x">>, headers => []}],
    Bin = iolist_to_binary(kafka_codec:encode_record_batch(0, Records)),
    Cut = binary:part(Bin, 0, byte_size(Bin) - 1),
    ?assertEqual({error, truncated_batch}, kafka_codec:decode_record_batches(Cut)),
    ok.

empty_batch(_Config) ->
    ?assertEqual({ok, []}, kafka_codec:decode_record_batches(<<>>)),
    ?assertEqual(<<>>, kafka_codec:encode_record_batch(0, [])),
    ok.
