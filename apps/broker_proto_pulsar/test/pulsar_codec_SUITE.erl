%% @doc S5 Pulsar wire codec: protobuf primitives, commands, message metadata,
%% the checksummed payload section and CRC32C. Pure functions, no sockets; the
%% connection behaviour lives in `pulsar_conn_SUITE' and the third-party-client
%% evidence in `test/protocol-compatibility/pulsar/'.
-module(pulsar_codec_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [crc32c_known_vector,
     frame_has_command_size_prefix,
     connect_decodes,
     producer_decodes,
     subscribe_decodes,
     send_decodes,
     ack_decodes,
     seek_decodes,
     payload_roundtrip,
     payload_checksum_detects_corruption,
     payload_without_magic,
     batch_payload_roundtrip,
     message_metadata_roundtrip,
     simple_metadata_roundtrip,
     connected_frame].

suite() -> [{timetrap, {seconds, 20}}].

%%%_* Cases -----------------------------------------------------------------

crc32c_known_vector(_Config) ->
    %% RFC 3720 / Castagnoli test vector.
    ?assertEqual(16#E3069283, pulsar_codec:crc32c(<<"123456789">>)),
    ?assertEqual(0, pulsar_codec:crc32c(<<>>)),
    ok.

frame_has_command_size_prefix(_Config) ->
    Frame = iolist_to_binary(pulsar_codec:encode_connected(<<"v">>, 15)),
    <<CmdSize:32/signed, Rest/binary>> = Frame,
    ?assertEqual(CmdSize, byte_size(Rest)),
    %% The command itself is `BaseCommand' field 1 = type (3 = CONNECTED).
    {ok, #{type := connected}, <<>>} = pulsar_codec:decode_frame(Frame),
    ok.

connect_decodes(_Config) ->
    Cmd = command(2, 2, [str(1, <<"Pulsar-CPP">>), varint(4, 15)]),
    {ok, Decoded, <<>>} = pulsar_codec:decode_frame(Cmd),
    ?assertEqual(connect, maps:get(type, Decoded)),
    ?assertEqual(<<"Pulsar-CPP">>, maps:get(client_version, Decoded)),
    ?assertEqual(15, maps:get(protocol_version, Decoded)),
    ok.

producer_decodes(_Config) ->
    Cmd = command(5, 5, [str(1, <<"persistent://t0/ns/t">>),
                         varint(2, 7),
                         varint(3, 42),
                         str(4, <<"prod-a">>)]),
    {ok, Decoded, <<>>} = pulsar_codec:decode_frame(Cmd),
    ?assertEqual(producer, maps:get(type, Decoded)),
    ?assertEqual(<<"persistent://t0/ns/t">>, maps:get(topic, Decoded)),
    ?assertEqual(7, maps:get(producer_id, Decoded)),
    ?assertEqual(42, maps:get(request_id, Decoded)),
    ?assertEqual(<<"prod-a">>, maps:get(producer_name, Decoded)),
    ok.

subscribe_decodes(_Config) ->
    Cmd = command(4, 4, [str(1, <<"t">>),
                         str(2, <<"sub-a">>),
                         varint(3, 1),          % Shared
                         varint(4, 9),          % consumer id
                         varint(5, 11),         % request id
                         varint(8, 0),          % not durable
                         varint(13, 1)]),       % Earliest
    {ok, Decoded, <<>>} = pulsar_codec:decode_frame(Cmd),
    ?assertEqual(subscribe, maps:get(type, Decoded)),
    ?assertEqual(shared, maps:get(sub_type, Decoded)),
    ?assertEqual(9, maps:get(consumer_id, Decoded)),
    ?assertEqual(false, maps:get(durable, Decoded)),
    ?assertEqual(earliest, maps:get(initial_position, Decoded)),
    ok.

send_decodes(_Config) ->
    Cmd = command(6, 6, [varint(1, 7), varint(2, 3),
                         varint(3, 1), varint(6, 3)]),
    {ok, Decoded, <<>>} = pulsar_codec:decode_frame(Cmd),
    ?assertEqual(send, maps:get(type, Decoded)),
    ?assertEqual(7, maps:get(producer_id, Decoded)),
    ?assertEqual(3, maps:get(sequence_id, Decoded)),
    ?assertEqual(1, maps:get(num_messages, Decoded)),
    ok.

ack_decodes(_Config) ->
    Id = enc_fields([varint(1, 1), varint(2, 5), varint(3, -1)]),
    Cmd = command(10, 10, [varint(1, 9),
                           varint(2, 1),        % Cumulative
                           msg(3, Id),
                           varint(8, 21)]),
    {ok, Decoded, <<>>} = pulsar_codec:decode_frame(Cmd),
    ?assertEqual(ack, maps:get(type, Decoded)),
    ?assertEqual(cumulative, maps:get(ack_type, Decoded)),
    ?assertEqual([{1, 5}], maps:get(message_ids, Decoded)),
    ?assertEqual(21, maps:get(request_id, Decoded)),
    ok.

seek_decodes(_Config) ->
    Meta = enc_fields([varint(1, 1), varint(2, 4), varint(3, -1)]),
    Cmd = command(28, 28, [varint(1, 9), varint(2, 22), msg(3, Meta)]),
    {ok, Decoded, <<>>} = pulsar_codec:decode_frame(Cmd),
    ?assertEqual(seek, maps:get(type, Decoded)),
    ?assertEqual({1, 4}, maps:get(message_id, Decoded)),
    ok.

payload_roundtrip(_Config) ->
    Meta = #{producer_name => <<"p">>, sequence_id => 3,
             publish_time => 1700000000000, properties => [{<<"k">>, <<"v">>}],
             partition_key => <<"key">>, uncompressed_size => 5},
    Encoded = pulsar_codec:encode_message_metadata(Meta),
    Frame = iolist_to_binary(
              pulsar_codec:encode_payload_frame(
                command(6, 6, enc_fields([varint(1, 1), varint(2, 3)])),
                Encoded, <<"hello">>)),
    <<CmdSize:32/signed, Rest/binary>> = Frame,
    <<_Cmd:CmdSize/binary, Payload/binary>> = Rest,
    {ok, Meta2, <<"hello">>} = pulsar_codec:decode_payload(Payload),
    ?assertEqual(<<"p">>, maps:get(producer_name, Meta2)),
    ?assertEqual(3, maps:get(sequence_id, Meta2)),
    ?assertEqual(1700000000000, maps:get(publish_time, Meta2)),
    ?assertEqual([{<<"k">>, <<"v">>}], maps:get(properties, Meta2)),
    ?assertEqual(<<"key">>, maps:get(partition_key, Meta2)),
    ok.

payload_checksum_detects_corruption(_Config) ->
    Meta = pulsar_codec:encode_message_metadata(#{publish_time => 1}),
    Frame = iolist_to_binary(
              pulsar_codec:encode_payload_frame(
                command(6, 6, enc_fields([varint(1, 1)])), Meta, <<"data">>)),
    <<CmdSize:32/signed, Rest/binary>> = Frame,
    <<_Cmd:CmdSize/binary, Payload/binary>> = Rest,
    {ok, _, <<"data">>} = pulsar_codec:decode_payload(Payload),
    %% Flip the last byte of the payload section and expect a checksum error.
    Size = byte_size(Payload),
    <<Head:(Size - 1)/binary, Last>> = Payload,
    Corrupt = <<Head/binary, (Last bxor 16#FF)>>,
    ?assertEqual({error, checksum_mismatch}, pulsar_codec:decode_payload(Corrupt)),
    ok.

payload_without_magic(_Config) ->
    Meta = pulsar_codec:encode_message_metadata(#{sequence_id => 9}),
    MetaBin = iolist_to_binary(Meta),
    Legacy = <<(byte_size(MetaBin)):32/signed, MetaBin/binary, "raw">>,
    {ok, Decoded, <<"raw">>} = pulsar_codec:decode_payload(Legacy),
    ?assertEqual(9, maps:get(sequence_id, Decoded)),
    ok.

batch_payload_roundtrip(_Config) ->
    %% A batch carries [meta_size][SingleMessageMetadata][payload] per message
    %% after the outer MessageMetadata.
    One = single(<<"a">>, 1, 3),
    Two = single(<<"bb">>, 2, 5),
    Meta = pulsar_codec:encode_message_metadata(
             #{num_messages_in_batch => 2, uncompressed_size => 3}),
    Frame = iolist_to_binary(
              pulsar_codec:encode_payload_frame(
                command(6, 6, [varint(1, 1), varint(3, 2)]),
                Meta, <<One/binary, Two/binary>>)),
    <<CmdSize:32/signed, Rest/binary>> = Frame,
    <<_Cmd:CmdSize/binary, Payload/binary>> = Rest,
    {ok, Outer, Batch} = pulsar_codec:decode_payload(Payload),
    ?assertEqual(2, maps:get(num_messages_in_batch, Outer)),
    <<S1:32/signed, R1/binary>> = Batch,
    <<M1:S1/binary, P1/binary>> = R1,
    Single1 = pulsar_codec:decode_single_metadata(M1),
    PSize1 = maps:get(payload_size, Single1),
    <<Payload1:PSize1/binary, R2/binary>> = P1,
    <<S2:32/signed, R3/binary>> = R2,
    <<M2:S2/binary, Payload2/binary>> = R3,
    Single2 = pulsar_codec:decode_single_metadata(M2),
    ?assertEqual(<<"a">>, Payload1),
    ?assertEqual(<<"bb">>, Payload2),
    ?assertEqual(1, maps:get(payload_size, Single1)),
    ?assertEqual(2, maps:get(payload_size, Single2)),
    ok.

message_metadata_roundtrip(_Config) ->
    Meta = #{producer_name => <<"prod">>, sequence_id => 7,
             publish_time => 1234567890,
             properties => [{<<"a">>, <<"1">>}, {<<"b">>, <<"2">>}],
             partition_key => <<"pk">>, uncompressed_size => 4,
             event_time => 99, num_messages_in_batch => 2,
             ordering_key => <<"ok">>, null_value => true},
    Decoded = pulsar_codec:decode_message_metadata(
                iolist_to_binary(pulsar_codec:encode_message_metadata(Meta))),
    ?assertEqual(<<"prod">>, maps:get(producer_name, Decoded)),
    ?assertEqual(7, maps:get(sequence_id, Decoded)),
    ?assertEqual(1234567890, maps:get(publish_time, Decoded)),
    ?assertEqual([{<<"a">>, <<"1">>}, {<<"b">>, <<"2">>}],
                 maps:get(properties, Decoded)),
    ?assertEqual(<<"pk">>, maps:get(partition_key, Decoded)),
    ?assertEqual(4, maps:get(uncompressed_size, Decoded)),
    ?assertEqual(99, maps:get(event_time, Decoded)),
    ?assertEqual(2, maps:get(num_messages_in_batch, Decoded)),
    ?assertEqual(<<"ok">>, maps:get(ordering_key, Decoded)),
    ?assertEqual(true, maps:get(null_value, Decoded)),
    ok.

simple_metadata_roundtrip(_Config) ->
    Single = pulsar_codec:decode_single_metadata(single_meta(<<"x">>, 8)),
    ?assertEqual(1, maps:get(payload_size, Single)),
    ?assertEqual(8, maps:get(sequence_id, Single)),
    ok.

connected_frame(_Config) ->
    Frame = iolist_to_binary(pulsar_codec:encode_connected(<<"umq">>, 15)),
    {ok, Decoded, <<>>} = pulsar_codec:decode_frame(Frame),
    ?assertEqual(connected, maps:get(type, Decoded)),
    ok.

%%%_* Helpers ---------------------------------------------------------------

%% Build a full command frame the way a client would: [command_size][cmd],
%% where the command is a BaseCommand: field 1 = type, FieldNo = the nested
%% command message.
command(Type, FieldNo, NestedFields) ->
    Base = iolist_to_binary([varint(1, Type), msg(FieldNo, NestedFields)]),
    <<(byte_size(Base)):32/signed, Base/binary>>.

enc_fields(Fields) ->
    iolist_to_binary(Fields).

varint(Field, V) ->
    [key(Field, 0), uvarint(V band 16#FFFFFFFFFFFFFFFF)].

str(Field, Bin) ->
    [key(Field, 2), uvarint(byte_size(Bin)), Bin].

msg(Field, Encoded) ->
    Bin = iolist_to_binary(Encoded),
    [key(Field, 2), uvarint(byte_size(Bin)), Bin].

key(Field, Wire) ->
    uvarint((Field bsl 3) bor Wire).

uvarint(V) when V < 128 -> [V];
uvarint(V) -> [(V band 127) bor 128 | uvarint(V bsr 7)].

single(Payload, SequenceId, _PayloadSize) ->
    Meta = single_meta(Payload, SequenceId),
    <<(byte_size(Meta)):32/signed, Meta/binary, Payload/binary>>.

single_meta(Payload, SequenceId) ->
    iolist_to_binary(
      [key(3, 0), uvarint(byte_size(Payload)),          % payload_size
       key(8, 0), uvarint(SequenceId)]).                 % sequence_id
