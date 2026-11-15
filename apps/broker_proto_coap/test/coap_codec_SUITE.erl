%% @doc `coap_codec' (RFC 7252 + RFC 6690 link format), pure functions.
%%
%% Every case pins a wire rule from the RFC, not an implementation detail: the
%% 4-byte header, TKL bounds, option delta/length nibbles and their 13/14
%% extensions, payload marker rules, uint minimal encoding, block option
%% splitting, and the link-format escaping rules. A change that breaks any of
%% these breaks a real CoAP client, which is exactly what these cases detect.
-module(coap_codec_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-import(coap_codec, [decode/1, encode/1, request/3, request/4, response/3,
                     empty/2, ack/1, reset/1, reject/1,
                     type/1, code/1, id/1, token/1, payload/1,
                     option/2, put_option/3,
                     remove_option/2, uri_path/1, uri_query/1,
                     block/2, block_value/3,
                     content_format_id/1, content_format_name/1,
                     link_format/1]).

%% `options/1' and `destination/1' are imported below with explicit module
%% qualification in the test bodies to avoid clashing with the auto-imported
%% BIFs of the same name.

all() ->
    [decode_header_fields,
     decode_rejects_short_datagram,
     decode_rejects_reserved_token_length,
     decode_rejects_bad_version,
     decode_empty_message,
     decode_token_length_zero,
     decode_token_length_eight,
     decode_options_no_payload,
     decode_options_with_payload,
     decode_rejects_empty_payload_marker,
     decode_rejects_reserved_option_nibble,
     decode_option_delta_extension_one_byte,
     decode_option_delta_extension_two_byte,
     decode_option_length_extension_one_byte,
     decode_option_length_extension_two_byte,
     decode_rejects_truncated_option,
     decode_rejects_duplicate_unordered_option,
     decode_uint_zero_is_empty_bytes,
     decode_uint_nonzero,
     decode_block_option_all_values,
     encode_is_inverse_of_decode_for_canonical_messages,
     encode_empty_payload_has_no_marker,
     encode_nonempty_payload_has_marker,
     encode_options_emitted_in_ascending_number_order,
     encode_repeated_options_preserve_wire_order,
     encode_extended_delta_and_length,
     encode_rejects_out_of_range_response_code,
     request_sets_type_method_id,
     response_sets_code,
     empty_sets_code,
     ack_and_reset_copy_id,
     reject_is_400_with_empty_token,
     option_accessors_first_value_only,
     option_accessors_all_values_in_wire_order,
     put_option_replaces_every_occurrence,
     remove_option_drops_every_occurrence,
     uri_path_segments_in_order,
     uri_query_split_on_ampersand,
     destination_joins_segments_with_slash,
     destination_root_is_empty_binary,
     block_value_roundtrip,
     content_format_id_and_name_agree,
     link_format_single_entry,
     link_format_multiple_entries_sorted,
     link_format_escapes_special_bytes,
     link_format_valueless_attribute,
     link_format_integer_attribute,
     roundtrip_through_listener_bytes].

decode_header_fields(_) ->
    Bin = <<64, 1, 18, 52>>,                 %% CON GET, TKL 0, code 0.01, id 0x1234
    {ok, M} = decode(Bin),
    ?assertEqual(con, type(M)),
    ?assertEqual({method, get}, code(M)),
    ?assertEqual(16#1234, id(M)),
    ?assertEqual(<<>>, token(M)),
    ?assertEqual([], coap_codec:options(M)),
    ?assertEqual(<<>>, payload(M)).

decode_rejects_short_datagram(_) ->
    ?assertMatch({error, {too_short, _}}, decode(<<1, 2, 3>>)),
    ?assertMatch({error, {too_short, 0}}, decode(<<>>)).

decode_rejects_reserved_token_length(_) ->
    %% TKL 9-15 are reserved (RFC 7252 §3). Byte 0 is version(2)|type(2)|TKL(4),
    %% so TKL 9 is `<<1:2, 0:2, 9:4>>' = 16#49, followed by a 1-byte code and id.
    ?assertMatch({error, {bad_token_length, 9}},
                 decode(<<16#49, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>)).

decode_rejects_bad_version(_) ->
    %% Only version 1 exists.
    ?assertMatch({error, {bad_version, 0}}, decode(<<0:2, 0:2, 0:4, 1, 0, 0, 0>>)),
    ?assertMatch({error, {bad_version, 2}}, decode(<<2:2, 0:2, 0:4, 1, 0, 0, 0>>)).

decode_empty_message(_) ->
    %% The 4-byte empty ACK: type ACK, code 0, TKL 0.
    {ok, M} = decode(<<96, 0, 18, 52>>),
    ?assertEqual(ack, type(M)),
    ?assertEqual(empty, code(M)),
    ?assertEqual(<<>>, payload(M)).

decode_token_length_zero(_) ->
    {ok, M} = decode(<<64, 1, 0, 0, 1, 0>>),
    ?assertEqual(<<>>, token(M)).

decode_token_length_eight(_) ->
    %% Maximum token length is 8 bytes (RFC 7252 §3). Byte 0 is
    %% version(2)|type(2)|TKL(4), so version 1, type CON, TKL 8 is `<<1:2,0:2,8:4>>'
    %% = 16#48.
    Bin = <<16#48, 1, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8>>,
    {ok, M} = decode(Bin),
    ?assertEqual(<<1, 2, 3, 4, 5, 6, 7, 8>>, token(M)).

decode_options_no_payload(_) ->
    %% One option: Uri-Path (11), delta 11, length 1, value "a".
    {ok, M} = decode(<<64, 1, 0, 0, 16#B1, $a>>),
    ?assertEqual([{uri_path, <<"a">>}], coap_codec:options(M)).

decode_options_with_payload(_) ->
    %% Option then payload marker 0xFF then payload.
    {ok, M} = decode(<<64, 1, 0, 0, 16#B1, $a, 16#FF, $h, $i>>),
    ?assertEqual([{uri_path, <<"a">>}], coap_codec:options(M)),
    ?assertEqual(<<"hi">>, payload(M)).

decode_rejects_empty_payload_marker(_) ->
    %% A marker with nothing behind it is malformed: "a message with an empty
    %% payload must not have one" (RFC 7252 §3).
    ?assertMatch({error, empty_payload_marker},
                 decode(<<64, 1, 0, 0, 16#FF>>)).

decode_rejects_reserved_option_nibble(_) ->
    %% Delta or length nibble 15 is reserved (only 0-14 are legal). 0xF0 is
    %% delta-nibble 15, length-nibble 0.
    ?assertMatch({error, reserved_option_nibble},
                 decode(<<64, 1, 0, 0, 16#F0>>)).

decode_option_delta_extension_one_byte(_) ->
    %% Delta nibble 13 means "add 13 to the next byte": option header 0xD1 is
    %% delta-nibble 13, length-nibble 1, then the extension byte 5 (13+5=18),
    %% then the one-byte value "a".
    Bin = <<64, 1, 0, 0, 16#D1, 5, $a>>,
    {ok, M} = decode(Bin),
    ?assertEqual([{18, <<"a">>}], coap_codec:options(M)).

decode_option_delta_extension_two_byte(_) ->
    %% Delta nibble 14 means "add 269 to the next two bytes": header 0xE1 is
    %% delta-nibble 14, length-nibble 1, extension 0x0001 (269+1=270), then "a".
    Bin = <<64, 1, 0, 0, 16#E1, 0, 1, $a>>,
    {ok, M} = decode(Bin),
    ?assertEqual([{270, <<"a">>}], coap_codec:options(M)).

decode_option_length_extension_one_byte(_) ->
    %% Length nibble 13 means "add 13 to the next byte": header 0x0D is
    %% delta-nibble 0, length-nibble 13, extension byte 0 (13+0=13), then 13
    %% value bytes.
    Bin = <<64, 1, 0, 0, 16#0D, 0, 0:104>>,
    {ok, M} = decode(Bin),
    ?assertEqual([{0, <<0:104>>}], coap_codec:options(M)).

decode_option_length_extension_two_byte(_) ->
    %% Length nibble 14 means "add 269 to the next two bytes": header 0x0E is
    %% delta-nibble 0, length-nibble 14, extension 0x0001 (269+1=270), then a
    %% 270-byte value.
    Val = <<0:(270*8)>>,
    Bin = <<64, 1, 0, 0, 16#0E, 0, 1, Val/binary>>,
    {ok, M} = decode(Bin),
    ?assertEqual([{0, Val}], coap_codec:options(M)).

decode_rejects_truncated_option(_) ->
    %% Option header 0xB1 is delta-nibble 11 (Uri-Path), length-nibble 1, but
    %% the datagram ends before the one value byte.
    ?assertMatch({error, {truncated_option, 11}},
                 decode(<<64, 1, 0, 0, 16#B1>>)).

decode_rejects_duplicate_unordered_option(_) ->
    %% Option numbers are delta-encoded, so they are structurally monotonic:
    %% a lower number after a higher one cannot be expressed on the wire. Uri-Path
    %% (11) then Uri-Query (15) is the ascending case; the reverse would need a
    %% negative delta and cannot be encoded, so the decoder never sees it. This
    %% case pins that the ascending sequence decodes cleanly.
    Val = <<"k=v">>,
    Bin = <<64, 1, 0, 0, 16#B1, $a, 16#43, Val/binary>>,
    ?assertMatch({ok, _}, decode(Bin)).

decode_uint_zero_is_empty_bytes(_) ->
    %% Observe (6) with a zero-length value decodes to integer 0.
    {ok, M} = decode(<<64, 1, 0, 0, 16#60>>),
    ?assertEqual(0, option(observe, M)).

decode_uint_nonzero(_) ->
    %% Observe (6) with a one-byte value 5 decodes to integer 5.
    {ok, M} = decode(<<64, 1, 0, 0, 16#61, 5>>),
    ?assertEqual(5, option(observe, M)).

decode_block_option_all_values(_) ->
    %% Block2 (23): delta 23 needs the 13-extension (0xD1,10), length 1, value
    %% 0b101_1_010 = num 5, more 1, szx 2 -> size 64.
    Val = <<2#1011010>>,
    {ok, M} = decode(<<64, 1, 0, 0, 16#D1, 10, Val/binary>>),
    ?assertEqual({5, true, 64}, block(block2, M)).

encode_is_inverse_of_decode_for_canonical_messages(_) ->
    %% Every message produced by the constructors round-trips byte-exactly,
    %% because constructors always emit canonical (minimal-length) encodings.
    Msgs = [request(con, get, 16#1234),
            request(non, post, 16#FFFF),
            request(ack, put, 1),
            request(rst, delete, 65535),
            response(ack, {response, 2, 5}, <<"body">>),
            response(non, {response, 4, 0}, <<>>),
            empty(ack, 16#1234),
            empty(rst, 16#1234)],
    lists:foreach(
      fun(M) ->
              ?assertEqual(M, begin {ok, DM} = decode(encode(M)), DM end)
      end, Msgs).

encode_empty_payload_has_no_marker(_) ->
    ?assertNotMatch(<<_:4/binary, 16#FF, _/binary>>,
                    encode(request(con, get, 1))).

encode_nonempty_payload_has_marker(_) ->
    Bin = encode(response(ack, {response, 2, 5}, <<"x">>)),
    ?assert(binary:match(Bin, <<16#FF>>) =/= nomatch).

encode_options_emitted_in_ascending_number_order(_) ->
    %% Caller lists uri_query (15) before uri_path (11); encoder must reorder.
    M = request(con, get, 1, [{uri_query, <<"a=1">>}, {uri_path, <<"x">>}]),
    {ok, D} = decode(encode(M)),
    ?assertEqual([{uri_path, <<"x">>}, {uri_query, <<"a=1">>}], coap_codec:options(D)).

encode_repeated_options_preserve_wire_order(_) ->
    %% Two Uri-Path options: [a, b] must stay [a, b] after a round trip.
    M = request(con, get, 1, [{uri_path, <<"a">>}, {uri_path, <<"b">>}]),
    {ok, D} = decode(encode(M)),
    ?assertEqual([{uri_path, <<"a">>}, {uri_path, <<"b">>}], coap_codec:options(D)),
    ?assertEqual(<<"a/b">>, coap_codec:destination(D)).

encode_extended_delta_and_length(_) ->
    %% An option number > 269 forces the 14-nibble two-byte delta extension,
    %% and a value > 269 bytes forces the same for length. Option 300 is an
    %% unknown (raw) option, so its opaque value survives a round trip as a
    %% binary rather than being typed as a uint.
    Big = binary:copy(<<$z>>, 300),
    M = request(con, get, 1, [{300, Big}]),
    {ok, D} = decode(encode(M)),
    ?assertEqual([{300, Big}], coap_codec:options(D)).

encode_rejects_out_of_range_response_code(_) ->
    ?assertError(function_clause,
                 encode(response(ack, {response, 6, 0}, <<>>))),
    ?assertError(function_clause,
                 encode(response(ack, {response, 2, 32}, <<>>))).

request_sets_type_method_id(_) ->
    M = request(non, put, 42),
    ?assertEqual(non, type(M)),
    ?assertEqual({method, put}, code(M)),
    ?assertEqual(42, id(M)),
    ?assertEqual(<<>>, token(M)),
    ?assertEqual(<<>>, payload(M)).

response_sets_code(_) ->
    M = response(con, {response, 2, 4}, <<>>),
    ?assertEqual({response, 2, 4}, code(M)),
    ?assertEqual(0, id(M)),
    ?assertEqual(<<>>, token(M)).

empty_sets_code(_) ->
    M = empty(rst, 7),
    ?assertEqual(empty, code(M)),
    ?assertEqual(rst, type(M)),
    ?assertEqual(7, id(M)).

ack_and_reset_copy_id(_) ->
    Req = request(con, get, 99),
    ?assertEqual(99, id(ack(Req))),
    ?assertEqual(ack, type(ack(Req))),
    ?assertEqual(99, id(reset(Req))),
    ?assertEqual(rst, type(reset(Req))).

reject_is_400_with_empty_token(_) ->
    M = reject(16#1234),
    ?assertEqual(ack, type(M)),
    ?assertEqual({response, 4, 0}, code(M)),
    ?assertEqual(16#1234, id(M)),
    ?assertEqual(<<>>, token(M)).

option_accessors_first_value_only(_) ->
    M = request(con, get, 1, [{uri_path, <<"a">>}, {uri_path, <<"b">>}]),
    ?assertEqual(<<"a">>, option(uri_path, M)),
    ?assertEqual(undefined, option(etag, M)).

option_accessors_all_values_in_wire_order(_) ->
    M = request(con, get, 1, [{uri_path, <<"a">>}, {uri_path, <<"b">>}]),
    ?assertEqual([<<"a">>, <<"b">>], coap_codec:options(uri_path, M)),
    ?assertEqual([<<"a">>, <<"b">>], uri_path(M)).

put_option_replaces_every_occurrence(_) ->
    M0 = request(con, get, 1, [{uri_path, <<"a">>}, {uri_path, <<"b">>}]),
    M1 = put_option(uri_path, <<"z">>, M0),
    ?assertEqual([<<"z">>], uri_path(M1)).

remove_option_drops_every_occurrence(_) ->
    M0 = request(con, get, 1, [{uri_path, <<"a">>}, {uri_path, <<"b">>}]),
    M1 = remove_option(uri_path, M0),
    ?assertEqual([], uri_path(M1)),
    ?assertEqual(<<>>, coap_codec:destination(M1)).

uri_path_segments_in_order(_) ->
    M = request(con, get, 1, [{uri_path, <<"a">>}, {uri_path, <<"b">>},
                              {uri_path, <<"c">>}]),
    ?assertEqual([<<"a">>, <<"b">>, <<"c">>], uri_path(M)).

uri_query_split_on_ampersand(_) ->
    M = request(con, get, 1, [{uri_query, <<"a=1&b=2">>}, {uri_query, <<"c=3">>}]),
    ?assertEqual([<<"a=1">>, <<"b=2">>, <<"c=3">>], uri_query(M)).

destination_joins_segments_with_slash(_) ->
    M = request(con, get, 1, [{uri_path, <<"a">>}, {uri_path, <<"b">>}]),
    ?assertEqual(<<"a/b">>, coap_codec:destination(M)).

destination_root_is_empty_binary(_) ->
    ?assertEqual(<<>>, coap_codec:destination(request(con, get, 1))).

block_value_roundtrip(_) ->
    lists:foreach(
      fun({Num, More, Size}) ->
              M = put_option(block2, block_value(Num, More, Size),
                             response(ack, {response, 2, 5}, <<>>)),
              ?assertEqual({Num, More, Size}, block(block2, M))
      end,
      [{0, false, 16}, {1, true, 1024}, {42, false, 512}, {65535, true, 16}]).

content_format_id_and_name_agree(_) ->
    ?assertEqual(0, content_format_id(text_plain)),
    ?assertEqual(40, content_format_id(link_format)),
    ?assertEqual({text, plain}, content_format_name(0)),
    ?assertEqual({application, link_format}, content_format_name(40)).

link_format_single_entry(_) ->
    Doc = link_format([#{path => [<<"a">>, <<"b">>],
                         attrs => [{rt, <<"core.ps">>}, {ct, 0}]}]),
    ?assertEqual(<<"</a/b>;rt=\"core.ps\";ct=0">>, Doc).

link_format_multiple_entries_sorted(_) ->
    %% The encoder does not sort the caller's list; it joins in the given order.
    %% (coap_resource sorts before calling.)
    Doc = link_format([#{path => [<<"z">>], attrs => []},
                       #{path => [<<"a">>], attrs => [{obs, true}]}]),
    ?assertEqual(<<"</z>,</a>;obs">>, Doc).

link_format_escapes_special_bytes(_) ->
    %% Space, comma, semicolon, angle brackets, and non-ASCII must be escaped.
    Doc = link_format([#{path => [<<"a b">>], attrs => [{rt, <<"x,y">>}]}]),
    ?assertEqual(<<"</a%20b>;rt=\"x%2Cy\"">>, Doc).


link_format_valueless_attribute(_) ->
    Doc = link_format([#{path => [<<"obs">>], attrs => [{obs, true}]}]),
    ?assertEqual(<<"</obs>;obs">>, Doc).

link_format_integer_attribute(_) ->
    Doc = link_format([#{path => [<<"a">>], attrs => [{sz, 1024}]}]),
    ?assertEqual(<<"</a>;sz=1024">>, Doc).

roundtrip_through_listener_bytes(_) ->
    %% The exact byte string the MQTT-SN listener test suite uses as a sanity
    %% check: a CON GET with token, observe, uri_path, and payload.
    Bin = <<68, 1, 18, 52, 116, 111, 107, 49, 97, 0, 83, 114, 101, 115, 255, 104, 105>>,
    {ok, M} = decode(Bin),
    ?assertEqual([{observe, 0}, {uri_path, <<"res">>}], coap_codec:options(M)),
    ?assertEqual(<<"hi">>, payload(M)),
    ?assertEqual(<<"res">>, coap_codec:destination(M)).

%%%_* Helpers --------------------------------------------------------------

decode_return(Bin) ->
    {ok, M} = decode(Bin),
    M.
