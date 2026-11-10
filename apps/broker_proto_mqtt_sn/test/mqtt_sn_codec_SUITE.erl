%% @doc MQTT-SN 1.2 codec suite (S8-C Task 1).
%%
%% The decoder is exercised harder than the encoder, because it is the only
%% part that has to decide what to do with bytes it did not write: every
%% rejection path gets a case, and every packet gets a round-trip through
%% `encode/1' so the two halves cannot drift apart. The length-header switch
%% (1 vs 3 bytes) is checked with a payload right at the 255-byte boundary,
%% because one off-by-one there still produces decodable output.
%%
%% The hand-built reject vectors spell out every byte, header included; the
%% header byte and the body length must agree or the codec reports
%% `length_mismatch' before it ever looks at the type, which would make these
%% cases pass for the wrong reason.
-module(mqtt_sn_codec_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [roundtrip_advertise,
     roundtrip_searchgw,
     roundtrip_gwinfo,
     roundtrip_connect,
     roundtrip_connack,
     roundtrip_will_flow,
     roundtrip_register,
     roundtrip_regack,
     roundtrip_publish,
     roundtrip_qos_minus1,
     qos_minus1_is_wire_bits_three,
     roundtrip_puback,
     roundtrip_pubrec_pubrel_pubcomp,
     roundtrip_subscribe,
     roundtrip_subscribe_predefined,
     roundtrip_subscribe_short,
     roundtrip_suback,
     roundtrip_unsubscribe,
     roundtrip_unsuback,
     roundtrip_pingreq,
     roundtrip_pingresp,
     roundtrip_disconnect,
     roundtrip_will_topic_upd,
     roundtrip_will_msg_upd,
     roundtrip_will_resp,
     one_byte_length_header,
     three_byte_length_header,
     length_mismatch_is_reported,
     invalid_length_header_is_rejected,
     truncated_packets_are_rejected,
     unknown_type_is_rejected,
     publish_reserved_bits_are_rejected,
     connect_reserved_bits_are_rejected,
     will_topic_reserved_bits_are_rejected,
     publish_invalid_topic_id_type_is_rejected,
     subscribe_invalid_topic_id_type_is_rejected,
     subscribe_trailing_bytes_are_rejected,
     subscribe_truncated_is_rejected,
     decode_empty_is_truncated].

%%%_* Round trips -----------------------------------------------------------

roundtrip_advertise(_Config) ->
    P = #{type => advertise, gw_id => 16#AB, duration => 300},
    ?assertEqual({ok, P}, mqtt_sn_codec:decode(mqtt_sn_codec:encode(P))).

roundtrip_searchgw(_Config) ->
    P = #{type => searchgw, radius => 5},
    ?assertEqual({ok, P}, mqtt_sn_codec:decode(mqtt_sn_codec:encode(P))).

roundtrip_gwinfo(_Config) ->
    P = #{type => gwinfo, gw_id => 7, gw_add => <<192, 168, 0, 1>>},
    ?assertEqual({ok, P}, mqtt_sn_codec:decode(mqtt_sn_codec:encode(P))),
    %% An absent gw_add must survive the round trip too.
    ?assertEqual({ok, #{type => gwinfo, gw_id => 7}},
                 mqtt_sn_codec:decode(mqtt_sn_codec:encode(#{type => gwinfo,
                                                             gw_id => 7}))).

roundtrip_connect(_Config) ->
    P = #{type => connect, will => true, clean_session => false,
          protocol_id => 1, duration => 60, client_id => <<"sensor-1">>},
    ?assertEqual({ok, P}, mqtt_sn_codec:decode(mqtt_sn_codec:encode(P))),
    %% A connect with no will and a clean session is the other common shape.
    P2 = #{type => connect, will => false, clean_session => true,
           protocol_id => 1, duration => 30, client_id => <<"c">>},
    ?assertEqual({ok, P2}, mqtt_sn_codec:decode(mqtt_sn_codec:encode(P2))).

roundtrip_connack(_Config) ->
    lists:foreach(
      fun(RC) ->
              P = #{type => connack, return_code => RC},
              ?assertEqual({ok, P}, mqtt_sn_codec:decode(mqtt_sn_codec:encode(P)))
      end, [0, 1, 2, 3]).

roundtrip_will_flow(_Config) ->
    Topic = #{type => willtopic, qos => 1, retain => false, topic => <<"a/b">>},
    Msg = #{type => willmsg, payload => <<"bye">>},
    TReq = #{type => willtopicreq},
    MReq = #{type => willmsgreq},
    ?assertEqual({ok, Topic}, mqtt_sn_codec:decode(mqtt_sn_codec:encode(Topic))),
    ?assertEqual({ok, Msg}, mqtt_sn_codec:decode(mqtt_sn_codec:encode(Msg))),
    ?assertEqual({ok, TReq}, mqtt_sn_codec:decode(mqtt_sn_codec:encode(TReq))),
    ?assertEqual({ok, MReq}, mqtt_sn_codec:decode(mqtt_sn_codec:encode(MReq))).

roundtrip_register(_Config) ->
    P = #{type => register, topic_id => 0, msg_id => 42, topic => <<"t0/x">>},
    ?assertEqual({ok, P}, mqtt_sn_codec:decode(mqtt_sn_codec:encode(P))).

roundtrip_regack(_Config) ->
    P = #{type => regack, topic_id => 5, msg_id => 42, return_code => 0},
    ?assertEqual({ok, P}, mqtt_sn_codec:decode(mqtt_sn_codec:encode(P))).

roundtrip_publish(_Config) ->
    P = #{type => publish, dup => false, qos => 1, retain => false,
          topic_id_type => normal, topic_id => 3, msg_id => 7,
          payload => <<"payload">>},
    ?assertEqual({ok, P}, mqtt_sn_codec:decode(mqtt_sn_codec:encode(P))),
    %% QoS0 is the common case and carries msg_id 0.
    P0 = #{type => publish, dup => false, qos => 0, retain => true,
           topic_id_type => normal, topic_id => 1, msg_id => 0, payload => <<"x">>},
    ?assertEqual({ok, P0}, mqtt_sn_codec:decode(mqtt_sn_codec:encode(P0))),
    %% An empty payload is legal.
    PEmpty = #{type => publish, dup => true, qos => 2, retain => false,
               topic_id_type => normal, topic_id => 9, msg_id => 1, payload => <<>>},
    ?assertEqual({ok, PEmpty}, mqtt_sn_codec:decode(mqtt_sn_codec:encode(PEmpty))),
    %% A short topic travels in the 16-bit id field, so the type is what tells
    %% the two apart after decoding.
    PShort = #{type => publish, dup => false, qos => 0, retain => false,
               topic_id_type => short, topic_id => <<"ab">>, msg_id => 0,
               payload => <<"v">>},
    ?assertEqual({ok, PShort}, mqtt_sn_codec:decode(mqtt_sn_codec:encode(PShort))).

roundtrip_qos_minus1(_Config) ->
    P = #{type => publish, dup => false, qos => -1, retain => false,
          topic_id_type => normal, topic_id => 1, msg_id => 0,
          payload => <<"fire and forget">>},
    ?assertEqual({ok, P}, mqtt_sn_codec:decode(mqtt_sn_codec:encode(P))).

%% `-1' must land on the wire as the two-bit pattern 3, not as a shift by -5.
qos_minus1_is_wire_bits_three(_Config) ->
    Bin = mqtt_sn_codec:encode(#{type => publish, qos => -1,
                                 topic_id_type => normal, topic_id => 1,
                                 msg_id => 0, payload => <<>>}),
    Flags = binary:at(Bin, 2),
    ?assertEqual(3, (Flags bsr 5) band 2#11),
    ?assertMatch({ok, #{type := publish, qos := -1}},
                 mqtt_sn_codec:decode(Bin)),
    ok.

roundtrip_puback(_Config) ->
    P = #{type => puback, topic_id => 1, msg_id => 9, return_code => 0},
    ?assertEqual({ok, P}, mqtt_sn_codec:decode(mqtt_sn_codec:encode(P))).

roundtrip_pubrec_pubrel_pubcomp(_Config) ->
    lists:foreach(
      fun(T) ->
              P = #{type => T, msg_id => 11},
              ?assertEqual({ok, P}, mqtt_sn_codec:decode(mqtt_sn_codec:encode(P)))
      end, [pubrec, pubrel, pubcomp]).

roundtrip_subscribe(_Config) ->
    P = #{type => subscribe, dup => false, qos => 2, topic_id_type => normal,
          msg_id => 1, topic => <<"t0/sensor/temp">>},
    ?assertEqual({ok, P}, mqtt_sn_codec:decode(mqtt_sn_codec:encode(P))).

roundtrip_subscribe_predefined(_Config) ->
    P = #{type => subscribe, dup => false, qos => 0, topic_id_type => predefined,
          msg_id => 2, topic => 16#0005},
    ?assertEqual({ok, P}, mqtt_sn_codec:decode(mqtt_sn_codec:encode(P))).

roundtrip_subscribe_short(_Config) ->
    P = #{type => subscribe, dup => false, qos => 1, topic_id_type => short,
          msg_id => 3, topic => <<"ab">>},
    ?assertEqual({ok, P}, mqtt_sn_codec:decode(mqtt_sn_codec:encode(P))).

roundtrip_suback(_Config) ->
    P = #{type => suback, qos => 2, topic_id => 5, msg_id => 1, return_code => 0},
    ?assertEqual({ok, P}, mqtt_sn_codec:decode(mqtt_sn_codec:encode(P))).

roundtrip_unsubscribe(_Config) ->
    P = #{type => unsubscribe, topic_id_type => normal, msg_id => 8,
          topic => <<"t0/x">>},
    ?assertEqual({ok, P}, mqtt_sn_codec:decode(mqtt_sn_codec:encode(P))),
    PShort = #{type => unsubscribe, topic_id_type => short, msg_id => 9,
               topic => <<"ab">>},
    ?assertEqual({ok, PShort}, mqtt_sn_codec:decode(mqtt_sn_codec:encode(PShort))).

roundtrip_unsuback(_Config) ->
    P = #{type => unsuback, msg_id => 8},
    ?assertEqual({ok, P}, mqtt_sn_codec:decode(mqtt_sn_codec:encode(P))).

roundtrip_pingreq(_Config) ->
    ?assertEqual({ok, #{type => pingreq, client_id => <<>>}},
                 mqtt_sn_codec:decode(mqtt_sn_codec:encode(#{type => pingreq}))),
    ?assertEqual({ok, #{type => pingreq, client_id => <<"c1">>}},
                 mqtt_sn_codec:decode(mqtt_sn_codec:encode(
                                        #{type => pingreq, client_id => <<"c1">>}))).

roundtrip_pingresp(_Config) ->
    ?assertEqual({ok, #{type => pingresp}},
                 mqtt_sn_codec:decode(mqtt_sn_codec:encode(#{type => pingresp}))).

roundtrip_disconnect(_Config) ->
    ?assertEqual({ok, #{type => disconnect, duration => 0}},
                 mqtt_sn_codec:decode(mqtt_sn_codec:encode(#{type => disconnect}))),
    P = #{type => disconnect, duration => 120},
    ?assertEqual({ok, P}, mqtt_sn_codec:decode(mqtt_sn_codec:encode(P))).

roundtrip_will_topic_upd(_Config) ->
    P = #{type => willtopicupd, qos => 1, retain => true, topic => <<"w/t">>},
    ?assertEqual({ok, P}, mqtt_sn_codec:decode(mqtt_sn_codec:encode(P))).

roundtrip_will_msg_upd(_Config) ->
    P = #{type => willmsgupd, payload => <<"new will">>},
    ?assertEqual({ok, P}, mqtt_sn_codec:decode(mqtt_sn_codec:encode(P))).

roundtrip_will_resp(_Config) ->
    lists:foreach(
      fun(T) ->
              P = #{type => T, return_code => 0},
              ?assertEqual({ok, P},
                           mqtt_sn_codec:decode(mqtt_sn_codec:encode(P)))
      end, [willtopicresp, willmsgresp]).

%%%_* Length header ---------------------------------------------------------

%% WILLMSG is 1 (type) + payload; 253 bytes of payload gives 255 total, which
%% is the largest packet a one-byte header can express.
one_byte_length_header(_Config) ->
    Payload = binary:copy(<<"x">>, 253),
    Bin = mqtt_sn_codec:encode(#{type => willmsg, payload => Payload}),
    ?assertEqual(255, byte_size(Bin)),
    ?assertEqual(255, binary:at(Bin, 0)),
    ?assertEqual({ok, #{type => willmsg, payload => Payload}},
                 mqtt_sn_codec:decode(Bin)),
    ok.

%% One byte more forces the three-byte header; the total must still decode, and
%% the stored length must count the header itself.
three_byte_length_header(_Config) ->
    Payload = binary:copy(<<"x">>, 254),
    Bin = mqtt_sn_codec:encode(#{type => willmsg, payload => Payload}),
    ?assertEqual(258, byte_size(Bin)),
    <<1, Total:16, Rest/binary>> = Bin,
    ?assertEqual(258, Total),
    ?assertEqual(255, byte_size(Rest)),
    ?assertEqual({ok, #{type => willmsg, payload => Payload}},
                 mqtt_sn_codec:decode(Bin)),
    ok.

length_mismatch_is_reported(_Config) ->
    %% Header promises 10 bytes, the datagram is 8.
    ?assertMatch({error, {length_mismatch, 10, 8}},
                 mqtt_sn_codec:decode(<<10, 16#05, 0, 1, 2, 3, 4, 5>>)),
    %% The reverse: body longer than the header claims.
    ?assertMatch({error, {length_mismatch, 3, 5}},
                 mqtt_sn_codec:decode(<<3, 16#17, 9, 9, 9>>)),
    %% Same rule with a three-byte header: 100 bytes promised, 9 delivered.
    ?assertMatch({error, {length_mismatch, 100, 9}},
                 mqtt_sn_codec:decode(<<1, 100:16, 16#17, 1, 2, 3, 4, 5>>)),
    ok.

invalid_length_header_is_rejected(_Config) ->
    %% A three-byte header may only be used for packets of 4 bytes or more.
    ?assertEqual({error, invalid_length_header},
                 mqtt_sn_codec:decode(<<1, 0, 3, 16#17>>)),
    %% Length 2 is the smallest legal value, so 0 and 1 are both bogus and are
    %% rejected even when the datagram happens to be long enough.
    ?assertEqual({error, invalid_length_header}, mqtt_sn_codec:decode(<<0, 16#05, 0, 0>>)),
    ?assertEqual({error, invalid_length_header}, mqtt_sn_codec:decode(<<1, 0, 1, 0>>)),
    %% A truncated three-byte header itself.
    ?assertEqual({error, invalid_length_header}, mqtt_sn_codec:decode(<<1, 16#05>>)),
    ?assertEqual({error, invalid_length_header}, mqtt_sn_codec:decode(<<1>>)),
    ok.

truncated_packets_are_rejected(_Config) ->
    %% CONNACK needs a return code.
    ?assertEqual({error, truncated}, mqtt_sn_codec:decode(<<2, 16#05>>)),
    %% CONNECT needs flags, protocol id, duration and a non-empty client id.
    ?assertEqual({error, truncated}, mqtt_sn_codec:decode(<<2, 16#04>>)),
    ?assertEqual({error, truncated}, mqtt_sn_codec:decode(<<6, 16#04, 1, 1, 0, 1>>)),
    %% A publish that stops inside the message id.
    ?assertEqual({error, truncated}, mqtt_sn_codec:decode(<<4, 16#0C, 0, 0>>)),
    %% REGISTER with no topic name is truncated, not "empty topic".
    ?assertEqual({error, truncated}, mqtt_sn_codec:decode(<<6, 16#0A, 0, 0, 0, 1>>)),
    %% WILLTOPIC with a flags byte but no topic.
    ?assertEqual({error, truncated}, mqtt_sn_codec:decode(<<2, 16#07>>)),
    ok.

unknown_type_is_rejected(_Config) ->
    %% 0x03 is a hole between REQUEST and CONNECT.
    ?assertMatch({error, {unknown_type, 16#03}},
                 mqtt_sn_codec:decode(<<2, 16#03>>)),
    %% 0x11 sits between PUBREL and SUBSCRIBE.
    ?assertMatch({error, {unknown_type, 16#11}},
                 mqtt_sn_codec:decode(<<3, 16#11, 0>>)),
    %% 0x19 sits between DISCONNECT and WILLTOPICUPD.
    ?assertMatch({error, {unknown_type, 16#19}},
                 mqtt_sn_codec:decode(<<3, 16#19, 0>>)),
    %% Anything past the table.
    ?assertMatch({error, {unknown_type, 16#FF}},
                 mqtt_sn_codec:decode(<<3, 16#FF, 0>>)),
    ok.

publish_reserved_bits_are_rejected(_Config) ->
    %% PUBLISH: bit 3 must be zero.
    ?assertMatch({error, {reserved_bits, 16#08}},
                 mqtt_sn_codec:decode(<<9, 16#0C, 16#08, 0, 0, 0, 1, 0, 0>>)),
    ok.

connect_reserved_bits_are_rejected(_Config) ->
    %% CONNECT: only bit 7 (will) and bit 6 (clean session) are defined.
    ?assertMatch({error, {reserved_bits, 16#01}},
                 mqtt_sn_codec:decode(<<9, 16#04, 16#01, 1, 0, 10, "c1", 16#40>>)),
    ?assertMatch({error, {reserved_bits, 16#20}},
                 mqtt_sn_codec:decode(<<8, 16#04, 16#20, 1, 0, 10, "c1">>)),
    ok.

will_topic_reserved_bits_are_rejected(_Config) ->
    %% WILLTOPIC / WILLTOPICUPD: bits 6-5 (QoS) and bit 4 (retain) are defined.
    %% 0x04 sets a reserved bit without touching QoS or retain.
    ?assertMatch({error, {reserved_bits, 16#04}},
                 mqtt_sn_codec:decode(<<6, 16#07, 16#04, "a/b">>)),
    ?assertMatch({error, {reserved_bits, 16#04}},
                 mqtt_sn_codec:decode(<<6, 16#1A, 16#04, "a/b">>)),
    ok.

publish_invalid_topic_id_type_is_rejected(_Config) ->
    %% Bits 1-0 == 3 is not a topic id type.
    ?assertMatch({error, {invalid_topic_id_type, 3}},
                 mqtt_sn_codec:decode(<<9, 16#0C, 16#03, 0, 0, 0, 1, 0, 0>>)),
    ok.

subscribe_invalid_topic_id_type_is_rejected(_Config) ->
    ?assertMatch({error, {invalid_topic_id_type, 3}},
                 mqtt_sn_codec:decode(<<5, 16#12, 16#03, 0, 1>>)),
    ok.

%% A predefined id is exactly two bytes and a short topic exactly two
%% characters; anything longer in those positions is a malformed packet, not a
%% normal topic name that happens to start with the same bytes.
subscribe_trailing_bytes_are_rejected(_Config) ->
    ?assertMatch({error, {trailing_bytes, short_topic, <<"ab">>}},
                 mqtt_sn_codec:decode(<<8, 16#12, 16#02, 0, 1, "ab", 0>>)),
    ?assertMatch({error, {trailing_bytes, predefined_topic_id, 5}},
                 mqtt_sn_codec:decode(<<8, 16#12, 16#01, 0, 1, 0, 5, 0>>)),
    ok.

subscribe_truncated_is_rejected(_Config) ->
    %% Message id cut short.
    ?assertEqual({error, truncated}, mqtt_sn_codec:decode(<<4, 16#12, 0, 0>>)),
    %% Normal topic type with an empty topic name.
    ?assertEqual({error, truncated}, mqtt_sn_codec:decode(<<5, 16#12, 0, 0, 1>>)),
    %% Short topic with only one byte.
    ?assertEqual({error, truncated}, mqtt_sn_codec:decode(<<6, 16#12, 16#02, 0, 1, "a">>)),
    ok.

decode_empty_is_truncated(_Config) ->
    ?assertEqual({error, truncated}, mqtt_sn_codec:decode(<<>>)),
    ok.
