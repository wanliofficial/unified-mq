%% @doc T5: unit tests for the WAL disk codec (ADR-011 §11.5).
%%
%% The header layout and the body tuple's slot order are the on-disk protocol:
%% once a segment exists, changing either makes it unreadable. These tests pin
%% both by hand-decoding, so a reordered tuple fails here instead of silently
%% corrupting a recovered partition.
-module(wal_codec_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("broker_core/include/broker_message.hrl").

all() ->
    [encoded_record_has_the_frozen_header_layout,
     body_tuple_matches_the_frozen_slot_order,
     roundtrip_preserves_every_field,
     undefined_slots_roundtrip_without_derivation,
     partition_id_comes_from_the_caller_not_the_disk,
     encoding_is_deterministic,
     equal_maps_encode_identically_whatever_the_insertion_order,
     record_size_equals_encoded_size,
     missing_header_bytes_are_truncated,
     missing_body_bytes_are_truncated,
     corrupted_body_byte_is_a_crc_mismatch,
     corrupt_length_never_decodes_as_ok,
     unknown_vsn_is_refused,
     body_vsn_disagreeing_with_the_header_is_refused,
     bad_magic_is_refused].

%%%_* Tests --------------------------------------------------------------

%% §11.5: <<Magic:8, Vsn:8, CRC32:32, Len:32>> + term_to_binary(BodyTuple).
%% Magic is pinned literally: it is the only thing distinguishing our segment
%% from any other file, and changing it orphans every existing segment.
encoded_record_has_the_frozen_header_layout(_Config) ->
    Bin = wal_codec:encode(msg()),
    <<Magic:8, Vsn:8, Crc:32, Len:32, Body/binary>> = Bin,
    16#55 = Magic,
    1 = Vsn,
    %% 8 + 8 + 32 + 32 bits: the header is 10 bytes, and everything after it is
    %% exactly the body the Len field promises.
    10 = wal_codec:header_size(),
    Len = byte_size(Body),
    Len = byte_size(Bin) - wal_codec:header_size(),
    Crc = erlang:crc32(Body),
    ok.

%% The 13 frozen slots, in order. `partition_id' deliberately has no slot: the
%% partition is the directory the segment lives in, so decode/2 takes it from
%% the caller (see partition_id_comes_from_the_caller_not_the_disk).
body_tuple_matches_the_frozen_slot_order(_Config) ->
    <<_:8, _:8, _:32, _:32, Body/binary>> = wal_codec:encode(msg()),
    {Vsn, MsgId, Tenant, Destination, Payload, PayloadEncoding, Persistence,
     ProtoMeta, Headers, Reliability, CreatedAt, ExpiresAt, EventBlob} =
        binary_to_term(Body, [safe]),
    1 = Vsn,
    <<"m-1">> = MsgId,
    <<"t0">> = Tenant,
    <<"sport/tennis">> = Destination,
    <<"payload">> = Payload,
    raw = PayloadEncoding,
    durable = Persistence,
    #{proto := 1} = ProtoMeta,
    #{<<"h">> := <<"v">>} = Headers,
    os_buffer = Reliability,
    1700000000 = CreatedAt,
    1700000600 = ExpiresAt,
    #{type := <<"x">>} = EventBlob,
    ok.

roundtrip_preserves_every_field(_Config) ->
    Msg = msg(),
    {ok, Msg, Size} = wal_codec:decode(wal_codec:encode(Msg), 3),
    Size = byte_size(wal_codec:encode(Msg)),
    ok.

%% 【F4】: expires_at stays undefined on disk. Deriving a placeholder TTL here
%% would let S2's retention drop already-acked messages.
undefined_slots_roundtrip_without_derivation(_Config) ->
    Msg = msg(),
    NoExpiry = Msg#broker_message{expires_at = undefined, event = undefined},
    {ok, NoExpiry, _} = wal_codec:decode(wal_codec:encode(NoExpiry), 3),
    ok.

%% The record's own partition_id is not written; the partition that owns the
%% segment is authoritative (ADR-009: partition_id is a plain integer with no
%% semantics of its own).
partition_id_comes_from_the_caller_not_the_disk(_Config) ->
    Encoded = wal_codec:encode(msg(3, <<"p">>)),
    {ok, Stamped7, _} = wal_codec:decode(Encoded, 7),
    7 = Stamped7#broker_message.partition_id,
    {ok, Stamped3, _} = wal_codec:decode(Encoded, 3),
    3 = Stamped3#broker_message.partition_id,
    ok.

encoding_is_deterministic(_Config) ->
    Msg = msg(),
    Once = wal_codec:encode(Msg),
    Once = wal_codec:encode(Msg),
    ok.

%% Two producers that happen to insert their headers in a different order write
%% the same log bytes. Without this, a segment's content depends on map
%% construction order, which S4 replication (byte-wise log comparison) and any
%% external WAL reader would see as spurious divergence. 40 keys is above the
%% flat-map threshold, so insertion order can reach the serialized layout.
equal_maps_encode_identically_whatever_the_insertion_order(_Config) ->
    Keys = [integer_to_binary(I) || I <- lists:seq(1, 40)],
    Forward = maps:from_list([{K, K} || K <- Keys]),
    Backward = maps:from_list([{K, K} || K <- lists:reverse(Keys)]),
    Forward = Backward,
    Msg = msg(),
    Encoded = wal_codec:encode(Msg#broker_message{headers = Forward}),
    Encoded = wal_codec:encode(Msg#broker_message{headers = Backward}),
    ok.

record_size_equals_encoded_size(_Config) ->
    Msg = msg(),
    Size = wal_codec:record_size(Msg),
    Size = byte_size(wal_codec:encode(Msg)),
    ok.

%% A torn tail is the expected crash shape: bytes are missing, so the reader
%% must say "truncated" rather than invent a record or report corruption.
missing_header_bytes_are_truncated(_Config) ->
    <<Head:5/binary, _/binary>> = wal_codec:encode(msg()),
    truncated = wal_codec:decode(Head, 1),
    truncated = wal_codec:decode(<<>>, 1),
    ok.

missing_body_bytes_are_truncated(_Config) ->
    Encoded = wal_codec:encode(msg()),
    truncated = wal_codec:decode(binary:part(Encoded, 0, byte_size(Encoded) - 1), 1),
    truncated = wal_codec:decode(binary:part(Encoded, 0, wal_codec:header_size()), 1),
    ok.

%% One flipped byte in the body must be caught. (A single-bit flip cannot be
%% expressed here: a binary tail segment has to be byte aligned.)
corrupted_body_byte_is_a_crc_mismatch(_Config) ->
    Encoded = wal_codec:encode(msg()),
    Size = byte_size(Encoded),
    <<Head:20/binary, Byte, Rest/binary>> = Encoded,
    Corrupt = <<Head:20/binary, (Byte bxor 16#FF), Rest/binary>>,
    Size = byte_size(Corrupt),
    {error, {crc_mismatch, _, _}} = wal_codec:decode(Corrupt, 1),
    ok.

%% Whatever a corrupt Len produces, it must not decode as a valid record: a
%% short Len reads someone else's bytes, a long Len runs off the end.
corrupt_length_never_decodes_as_ok(_Config) ->
    Encoded = wal_codec:encode(msg()),
    lists:foreach(
        fun(BadLen) ->
            <<Magic:8, Vsn:8, Crc:32, _:32, Body/binary>> = Encoded,
            case wal_codec:decode(<<Magic:8, Vsn:8, Crc:32, BadLen:32, Body/binary>>, 1) of
                {ok, _, _} -> ct:fail({decoded_with_corrupt_length, BadLen});
                _ -> ok
            end
        end,
        [0, 1, 7, 16#FFFFFFFF]),
    ok.

%% §11.5: a record from a newer format must be refused, never guessed at.
%% wal_recovery turns this into "refuse to start".
unknown_vsn_is_refused(_Config) ->
    <<_:8, _:8, Crc:32, Len:32, Body/binary>> = wal_codec:encode(msg()),
    {error, {unsupported_vsn, 9}} =
        wal_codec:decode(<<16#55:8, 9:8, Crc:32, Len:32, Body/binary>>, 1),
    {error, {unsupported_vsn, 0}} =
        wal_codec:decode(<<16#55:8, 0:8, Crc:32, Len:32, Body/binary>>, 1),
    ok.

%% The header Vsn and the body tuple's Vsn slot must agree; otherwise a header
%% rewrite (or a misaligned read) would pass as a valid old-format record.
body_vsn_disagreeing_with_the_header_is_refused(_Config) ->
    Body = term_to_binary({9, <<"m">>, <<"t">>, <<"d">>, <<"p">>, raw, durable,
                           #{}, #{}, os_buffer, 1, undefined, undefined},
                          [{compressed, 1}]),
    Bin = <<16#55:8, 1:8, (erlang:crc32(Body)):32, (byte_size(Body)):32, Body/binary>>,
    {error, {vsn_mismatch, 1, 9}} = wal_codec:decode(Bin, 1),
    ok.

bad_magic_is_refused(_Config) ->
    <<_:8, Vsn:8, Crc:32, Len:32, Body/binary>> = wal_codec:encode(msg()),
    {error, {bad_magic, 0}} = wal_codec:decode(<<0:8, Vsn:8, Crc:32, Len:32, Body/binary>>, 1),
    ok.

%%%_* Helpers ------------------------------------------------------------

msg() ->
    msg(3, <<"payload">>).

msg(PartitionId, Payload) ->
    #broker_message{message_id = <<"m-1">>,
                    tenant_id = <<"t0">>,
                    destination = <<"sport/tennis">>,
                    partition_id = PartitionId,
                    payload = Payload,
                    payload_encoding = raw,
                    headers = #{<<"h">> => <<"v">>},
                    proto_meta = #{proto => 1},
                    persistence = durable,
                    reliability = os_buffer,
                    created_at = 1700000000,
                    expires_at = 1700000600,
                    event = #{type => <<"x">>}}.
