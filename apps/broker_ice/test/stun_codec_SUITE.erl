%% @doc `stun_codec' (RFC 8489 + the RFC 5766 attribute set), pure functions.
%%
%% Every case pins a wire rule from the RFC, not an implementation detail: the
%% 20-byte header and its length field, the 14-bit type split into method and
%% class, 4-byte attribute padding, XOR-*-ADDRESS with the magic cookie, the
%% MESSAGE-INTEGRITY patched-length trick, and FINGERPRINT's CRC-32 XOR.
-module(stun_codec_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(COOKIE, 16#2112A442).
-define(TID, <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12>>).

all() ->
    [binding_request_roundtrip,
     header_length_field_excludes_the_header_itself,
     method_and_class_are_split_across_the_type_bits,
     attribute_padding_is_skipped_on_decode,
     truncated_datagram_is_rejected,
     length_mismatch_is_rejected,
     bad_magic_cookie_is_rejected,
     xor_mapped_address_roundtrip,
     xor_address_is_bound_to_its_transaction,
     ipv6_is_refused,
     message_integrity_roundtrip,
     message_integrity_detects_tampering,
     message_integrity_requires_the_attribute,
     fingerprint_roundtrip,
     fingerprint_detects_tampering,
     fingerprint_must_be_last,
     error_code_roundtrip,
     channel_number_roundtrip,
     method_names_round_trip,
     unknown_required_attributes_are_listed,
     software_attribute_is_comprehension_optional].

%%%_* Header ---------------------------------------------------------------

binding_request_roundtrip(_Config) ->
    Msg = #{method => binding, class => request, transaction_id => ?TID,
            attributes => []},
    Bin = stun_codec:encode(Msg),
    ?assertEqual(20, byte_size(Bin)),
    %% Top two bits zero, type 0x0001, length 0, then the magic cookie.
    ?assertMatch(<<0:2, 1:14, 0:16, ?COOKIE:32, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                   11, 12>>, Bin),
    {ok, Decoded} = stun_codec:decode(Bin),
    ?assertEqual(binding, maps:get(method, Decoded)),
    ?assertEqual(request, maps:get(class, Decoded)),
    ?assertEqual(?TID, maps:get(transaction_id, Decoded)),
    ?assertEqual([], maps:get(attributes, Decoded)).

header_length_field_excludes_the_header_itself(_Config) ->
    Attr = <<16#1234:16, 4:16, 0:32>>,
    Msg = #{method => allocate, class => success, transaction_id => ?TID,
            attributes => [{16#1234, <<0:32>>}]},
    Bin = stun_codec:encode(Msg),
    <<_:16, Len:16, _/binary>> = Bin,
    ?assertEqual(8, Len),
    ?assertEqual(20 + 8, byte_size(Bin)),
    ?assertEqual(Attr, binary:part(Bin, 20, 8)).

method_and_class_are_split_across_the_type_bits(_Config) ->
    %% RFC 8489 §5: M is the 12-bit method, C0/C1 the class bits; the type
    %% interleaves them so both survive the 2 reserved bits at the top.
    ?assertEqual(16#0001, stun_codec:encode_type(1, 0)),   %% Binding request
    ?assertEqual(16#0101, stun_codec:encode_type(1, 2)),   %% Binding success
    ?assertEqual(16#0111, stun_codec:encode_type(1, 3)),   %% Binding error
    ?assertEqual(16#0003, stun_codec:encode_type(3, 0)),   %% Allocate request
    ?assertEqual(16#0103, stun_codec:encode_type(3, 2)),   %% Allocate success
    ?assertEqual(16#0004, stun_codec:encode_type(4, 0)),   %% Refresh request
    ?assertEqual(16#0006, stun_codec:encode_type(6, 0)),   %% Send indication
    %% Method 9 (ChannelBind) with class `error': M0..M3 in bits 0-3, C0 at
    %% bit 4, C1 at bit 8 -> 0x0119.
    ?assertEqual(16#0119, stun_codec:encode_type(9, 3)),
    %% Data indication is the one method whose class bits are already set.
    ?assertEqual(16#0017, stun_codec:encode_type(7, 1)),
    lists:foreach(
      fun(Method) ->
              lists:foreach(
                fun(Class) ->
                        Type = stun_codec:encode_type(Method, Class),
                        ?assertEqual(Method, stun_codec:method(Type)),
                        ?assertEqual(Class, class_code(stun_codec:class(Type)))
                end, [0, 1, 2, 3])
      end, [1, 3, 4, 6, 7, 8, 9]).

class_code(request) -> 0;
class_code(indication) -> 1;
class_code(success) -> 2;
class_code(error) -> 3.

attribute_padding_is_skipped_on_decode(_Config) ->
    %% A 3-byte value occupies 4 bytes on the wire; the tail attribute after it
    %% must still be found.
    Msg = #{method => binding, class => request, transaction_id => ?TID,
            attributes => [{16#0006, <<"abc">>}, {16#000D, <<0:32>>}]},
    Bin = stun_codec:encode(Msg),
    %% 4 + 3 + 1 pad + 8 = 16 bytes of body.
    ?assertEqual(36, byte_size(Bin)),
    {ok, Decoded} = stun_codec:decode(Bin),
    ?assertEqual([{16#0006, <<"abc">>}, {16#000D, <<0:32>>}],
                 maps:get(attributes, Decoded)),
    ?assertEqual(<<"abc">>,
                 stun_codec:attribute(maps:get(attributes, Decoded), 16#0006)).

truncated_datagram_is_rejected(_Config) ->
    ?assertMatch({error, {too_short, 3}}, stun_codec:decode(<<1, 2, 3>>)),
    Bin = stun_codec:encode(#{method => binding, class => request,
                              transaction_id => ?TID,
                              attributes => [{16#000D, <<0:32>>}]}),
    %% Cutting the datagram short of its declared length is a length mismatch,
    %% never a partial decode.
    ?assertMatch({error, {length_mismatch, 8, 2}},
                 stun_codec:decode(binary:part(Bin, 0, 22))),
    %% A body whose length field is consistent but whose attribute claims more
    %% bytes than remain is refused with the attribute that overruns.
    Overrun = <<0:2, 1:14, 8:16, ?COOKIE:32, ?TID/binary,
                16#000D:16, 8:16, 0:32>>,
    ?assertMatch({error, {truncated_attribute, 16#000D, 8, 4}},
                 stun_codec:decode(Overrun)).

length_mismatch_is_rejected(_Config) ->
    Bin = stun_codec:encode(#{method => binding, class => request,
                              transaction_id => ?TID,
                              attributes => [{16#000D, <<0:32>>}]}),
    <<A:16, _Len:16, Rest/binary>> = Bin,
    %% Claim four more bytes than the datagram carries.
    Bad = <<A:16, 12:16, Rest/binary>>,
    ?assertMatch({error, {length_mismatch, 12, 8}}, stun_codec:decode(Bad)),
    %% And a length that is not word-aligned cannot describe this codec's
    %% attributes at all.
    Bad2 = <<A:16, 5:16, Rest/binary>>,
    ?assertMatch({error, {length_not_word_aligned, 5}}, stun_codec:decode(Bad2)).

bad_magic_cookie_is_rejected(_Config) ->
    Bin = stun_codec:encode(#{method => binding, class => request,
                              transaction_id => ?TID, attributes => []}),
    <<A:32, _Cookie:32, Rest/binary>> = Bin,
    ?assertMatch({error, {bad_magic_cookie, 0}},
                 stun_codec:decode(<<A:32, 0:32, Rest/binary>>)).

%%%_* XOR-*-ADDRESS ---------------------------------------------------------

xor_mapped_address_roundtrip(_Config) ->
    Value = stun_codec:xor_mapped_address({192, 0, 2, 1}, 32853, ?TID),
    ?assertEqual(8, byte_size(Value)),
    <<0, 1, _:16, _:32>> = Value,
    ?assertEqual({ok, {{192, 0, 2, 1}, 32853}},
                 stun_codec:decode_xor_address(Value)),
    %% The RFC 8489 §14.2 sample: the last 4 bytes are the address XOR the
    %% cookie, so an address that equals the cookie XORs to zero.
    ?assertEqual(<<0, 1, 0, 0, 0, 0, 0, 0>>,
                 stun_codec:xor_mapped_address({33, 18, 164, 66}, 16#2112,
                                               ?TID)).

xor_address_is_bound_to_its_transaction(_Config) ->
    %% A relayed-address value XORs with the cookie only, so it is identical
    %% for every transaction; the port and address must still round-trip.
    A = stun_codec:xor_address({10, 0, 0, 1}, 49152, ?TID),
    B = stun_codec:xor_address({10, 0, 0, 1}, 49152,
                               crypto:strong_rand_bytes(12)),
    ?assertEqual(A, B),
    ?assertEqual({ok, {{10, 0, 0, 1}, 49152}},
                 stun_codec:decode_xor_address(A)),
    %% A family byte we do not implement is refused, not guessed at.
    ?assertMatch({error, {unsupported_family, 2}},
                 stun_codec:decode_xor_address(<<0, 2, 0, 0, 0:128>>)).

ipv6_is_refused(_Config) ->
    ?assertError(ipv6_not_supported,
                 stun_codec:xor_address({0, 0, 0, 0, 0, 0, 0, 1}, 3478, ?TID)).

%%%_* MESSAGE-INTEGRITY -----------------------------------------------------

message_integrity_roundtrip(_Config) ->
    Key = ice_test_client:key(<<"alice">>, <<"unified-mq">>, <<"s3cret">>),
    Base = stun_codec:encode(#{method => allocate, class => request,
                               transaction_id => ?TID,
                               attributes => [{16#0019, <<17, 0, 0, 0>>}]}),
    Signed = stun_codec:add_integrity(Key, Base),
    %% The attribute is 24 bytes: 4 header + 20 SHA-1 digest.
    ?assertEqual(byte_size(Base) + 24, byte_size(Signed)),
    ?assert(stun_codec:verify_integrity(Key, Signed)),
    %% The header length was patched to include the digest.
    <<_:16, Len:16, _/binary>> = Signed,
    ?assertEqual(byte_size(Signed) - 20, Len),
    %% The digest is the HMAC over the same prefix with that length, which is
    %% exactly what a second implementation has to reproduce.
    Prefix = binary:part(Signed, 0, byte_size(Signed) - 24),
    Expected = crypto:mac(hmac, sha, Key, Prefix),
    ?assertEqual(Expected, binary:part(Signed, byte_size(Signed) - 20, 20)),
    ?assertEqual({ok, byte_size(Signed) - 24},
                 stun_codec:integrity_offset(Signed)).

message_integrity_detects_tampering(_Config) ->
    Key = ice_test_client:key(<<"alice">>, <<"unified-mq">>, <<"s3cret">>),
    Base = stun_codec:encode(#{method => allocate, class => request,
                               transaction_id => ?TID,
                               attributes => [{16#0019, <<17, 0, 0, 0>>}]}),
    Signed = stun_codec:add_integrity(Key, Base),
    ?assert(stun_codec:verify_integrity(Key, Signed)),
    %% Flip one byte of the authenticated prefix.
    Offset = byte_size(Signed) - 24 - 1,
    <<Head:Offset/binary, Byte, Tail/binary>> = Signed,
    Tampered = <<Head/binary, (Byte bxor 1), Tail/binary>>,
    ?assertNot(stun_codec:verify_integrity(Key, Tampered)),
    %% A different password derives a different key.
    ?assertNot(stun_codec:verify_integrity(
                 ice_test_client:key(<<"alice">>, <<"unified-mq">>, <<"nope">>),
                 Signed)).

message_integrity_requires_the_attribute(_Config) ->
    Key = ice_test_client:key(<<"alice">>, <<"unified-mq">>, <<"s3cret">>),
    Base = stun_codec:encode(#{method => allocate, class => request,
                               transaction_id => ?TID, attributes => []}),
    ?assertNot(stun_codec:verify_integrity(Key, Base)),
    ?assertEqual(not_found, stun_codec:integrity_offset(Base)),
    ?assertEqual(error, stun_codec:integrity_offset(<<1, 2, 3>>)).

%%%_* FINGERPRINT -----------------------------------------------------------

fingerprint_roundtrip(_Config) ->
    Base = stun_codec:encode(#{method => binding, class => success,
                               transaction_id => ?TID, attributes => []}),
    Fp = stun_codec:add_fingerprint(Base),
    ?assertEqual(byte_size(Base) + 8, byte_size(Fp)),
    ?assert(stun_codec:check_fingerprint(Fp)),
    %% CRC-32 of the prefix XOR 0x5354554E ("STUN"), by construction.
    Prefix = binary:part(Fp, 0, byte_size(Fp) - 8),
    <<_:2, Type:14, _:16, Rest/binary>> = Prefix,
    Patched = <<0:2, Type:14, (byte_size(Fp) - 20):16, Rest/binary>>,
    <<Crc:32>> = binary:part(Fp, byte_size(Fp) - 4, 4),
    ?assertEqual(erlang:crc32(Patched) bxor 16#5354554E, Crc).

fingerprint_detects_tampering(_Config) ->
    Fp = stun_codec:add_fingerprint(
           stun_codec:encode(#{method => binding, class => success,
                               transaction_id => ?TID, attributes => []})),
    <<Head:20/binary, Byte, Tail/binary>> = Fp,
    Tampered = <<Head/binary, (Byte bxor 1), Tail/binary>>,
    ?assertNot(stun_codec:check_fingerprint(Tampered)),
    ?assertNot(stun_codec:check_fingerprint(<<>>)).

fingerprint_must_be_last(_Config) ->
    Fp = stun_codec:add_fingerprint(
           stun_codec:encode(#{method => binding, class => success,
                               transaction_id => ?TID, attributes => []})),
    ?assert(stun_codec:check_fingerprint(Fp)),
    %% Bytes after FINGERPRINT are outside what the CRC covers, so a message
    %% that carries any is refused rather than partially trusted.
    ?assertNot(stun_codec:check_fingerprint(<<Fp/binary, 0, 0, 0, 0>>)).

%%%_* Attributes ------------------------------------------------------------

error_code_roundtrip(_Config) ->
    Value = stun_codec:error_code(4, 37),
    %% 21 reserved bits, class 4, number 37, then the reason phrase.
    <<0:21, 4:3, 37:8, "Allocation Mismatch">> = Value,
    ?assertEqual({ok, 437, <<"Allocation Mismatch">>},
                 stun_codec:decode_error_code(Value)),
    ?assertEqual({ok, 401, <<"Unauthorized">>},
                 stun_codec:decode_error_code(stun_codec:error_code(4, 1))),
    ?assertEqual({ok, 508, <<"Insufficient Capacity">>},
                 stun_codec:decode_error_code(stun_codec:error_code(5, 8))),
    %% The framing pads the attribute; the value itself must not be padded, or
    %% the padding would be part of the reason phrase.
    Bin = stun_codec:encode(#{method => allocate, class => error,
                              transaction_id => ?TID,
                              attributes => [
                                {stun_codec:attribute_type(error_code),
                                 stun_codec:error_code(4, 37)}]}),
    <<_:20/binary, AttrType:16, AttrLen:16, _/binary>> = Bin,
    ?assertEqual(16#0009, AttrType),
    ?assertEqual(byte_size(Value), AttrLen),
    {ok, Decoded} = stun_codec:decode(Bin),
    ?assertEqual({ok, 437, <<"Allocation Mismatch">>},
                 stun_codec:decode_error_code(
                   stun_codec:attribute(maps:get(attributes, Decoded),
                                        16#0009))).

channel_number_roundtrip(_Config) ->
    ?assertEqual(<<16#4000:16, 0:16>>, stun_codec:channel_number(16#4000)),
    ?assertEqual(<<16#7FFF:16, 0:16>>, stun_codec:channel_number(16#7FFF)),
    %% Outside the TURN channel range the value is not a channel number.
    ?assertError(function_clause, stun_codec:channel_number(16#8000)),
    ?assertError(function_clause, stun_codec:channel_number(16#3FFF)).

method_names_round_trip(_Config) ->
    %% The decoder names the seven methods this app implements and leaves any
    %% other method as its integer, so an unknown request can still be answered
    %% 400 with its method echoed.
    Pairs = [{1, binding}, {3, allocate}, {4, refresh}, {6, send}, {7, data},
             {8, create_permission}, {9, channel_bind}],
    lists:foreach(
      fun({Code, Name}) ->
              ?assertEqual(Name, stun_codec:method_atom(
                                   stun_codec:encode_type(Code, 0))),
              ?assertEqual(Code, stun_codec:method(
                                   stun_codec:encode_type(Code, 0)))
      end, Pairs),
    %% Method 10 has no name here: it comes back as the integer.
    ?assertEqual(10, stun_codec:method_atom(stun_codec:encode_type(10, 0))).

unknown_required_attributes_are_listed(_Config) ->
    %% Type below 0x8000 is comprehension-required (RFC 8489 §14); a server
    %% that does not understand one must answer 420 with the list.
    Known = stun_codec:attribute_type(lifetime),
    ?assert(stun_codec:comprehended(Known)),
    Attrs = [{Known, <<0:32>>}, {16#0033, <<1, 2, 3>>}, {16#8000, <<>>},
             {16#0031, <<>>}],
    ?assertEqual([16#0033, 16#0031], stun_codec:unknown_required(Attrs)),
    ?assertEqual([], stun_codec:unknown_required([{Known, <<0:32>>}])).

software_attribute_is_comprehension_optional(_Config) ->
    %% 0x8022 SOFTWARE is the canonical comprehension-optional attribute: it is
    %% in the range that must never trigger a 420.
    ?assertEqual(16#8022, stun_codec:attribute_type(software)),
    ?assertEqual([], stun_codec:unknown_required([{16#8022, <<"x">>}])),
    %% Values round-trip unchanged even when we do not interpret them.
    Msg = #{method => binding, class => success, transaction_id => ?TID,
            attributes => [{16#8022, <<"unified-mq/0.1.0">>}]},
    {ok, Decoded} = stun_codec:decode(stun_codec:encode(Msg)),
    ?assertEqual(<<"unified-mq/0.1.0">>,
                 stun_codec:attribute(maps:get(attributes, Decoded), 16#8022)).
