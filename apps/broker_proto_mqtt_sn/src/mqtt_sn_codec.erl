%% @doc MQTT-SN 1.2 wire codec (MQTT-SN 1.2 spec §5.2, Table 5.2).
%%
%% Packets are plain maps with a `type' key, because that is what the session
%% process actually wants: `#{type := publish, qos := 1, topic_id := 7, ...}'.
%% There is no record for every one of the 27 message types, and no state: all
%% of this module is a pure function of bytes.
%%
%% The decoder is deliberately strict. MQTT-SN runs on UDP, where a malformed
%% datagram is not "a connection that will close soon" but one packet among
%% many, so the caller needs a definite answer: `{error, Reason}' means drop
%% that datagram and carry on. Every rejection below is a rule from the spec
%% (length header, reserved bits, truncated fixed header, topic-id type), not a
%% defensive guess.
%%
%% Length header: the first byte is the total packet length when it is `< 256'
%% and `> 1'; otherwise the first byte is 1 and the next two are the total
%% length, big endian. The length counts the header itself -- a detail that is
%% easy to get wrong once and then debug for an hour.
-module(mqtt_sn_codec).

-export([decode/1, encode/1]).

%% Message types (MQTT-SN 1.2 Table 5.2). Gaps are reserved by the spec and are
%% not accepted on the wire.
-define(ADVERTISE, 16#00).
-define(SEARCHGW, 16#01).
-define(GWINFO, 16#02).
-define(CONNECT, 16#04).
-define(CONNACK, 16#05).
-define(WILLTOPICREQ, 16#06).
-define(WILLTOPIC, 16#07).
-define(WILLMSGREQ, 16#08).
-define(WILLMSG, 16#09).
-define(REGISTER, 16#0A).
-define(REGACK, 16#0B).
-define(PUBLISH, 16#0C).
-define(PUBACK, 16#0D).
-define(PUBCOMP, 16#0E).
-define(PUBREC, 16#0F).
-define(PUBREL, 16#10).
-define(SUBSCRIBE, 16#12).
-define(SUBACK, 16#13).
-define(UNSUBSCRIBE, 16#14).
-define(UNSUBACK, 16#15).
-define(PINGREQ, 16#16).
-define(PINGRESP, 16#17).
-define(DISCONNECT, 16#18).
-define(WILLTOPICUPD, 16#1A).
-define(WILLTOPICRESP, 16#1B).
-define(WILLMSGUPD, 16#1C).
-define(WILLMSGRESP, 16#1D).

-define(TOPIC_NORMAL, 0).
-define(TOPIC_PREDEFINED, 1).
-define(TOPIC_SHORT, 2).

%% @doc Decode exactly one datagram. Trailing bytes are an error: UDP delivers
%% one packet at a time, so a datagram that holds more than the length header
%% claims is malformed, not "the start of the next one".
-spec decode(binary()) -> {ok, map()} | {error, term()}.
decode(<<>>) ->
    {error, truncated};
%% A leading 1 announces the three-byte header, so anything shorter than that
%% header is malformed rather than merely short.
decode(<<1>>) ->
    {error, invalid_length_header};
decode(<<1, _:8>>) ->
    {error, invalid_length_header};
decode(<<1, Len:16, Rest/binary>>) when Len >= 4 ->
    check_length(Len, 3, Rest);
decode(<<1, _Len:16, _/binary>>) ->
    %% A three-byte header can only be used for packets of 4 bytes or more.
    {error, invalid_length_header};
decode(<<Len:8, Rest/binary>>) when Len >= 2 ->
    check_length(Len, 1, Rest);
decode(<<_Len:8, _/binary>>) ->
    %% Length 0 or 1 has no valid packet behind it.
    {error, invalid_length_header}.

check_length(Total, HeaderSize, Body) ->
    case byte_size(Body) of
        Expected when Expected =:= Total - HeaderSize ->
            decode_body(Body);
        Actual ->
            {error, {length_mismatch, Total, Actual + HeaderSize}}
    end.

decode_body(<<?ADVERTISE, GwId:8, Duration:16>>) ->
    {ok, #{type => advertise, gw_id => GwId, duration => Duration}};
decode_body(<<?SEARCHGW, Radius:8>>) ->
    {ok, #{type => searchgw, radius => Radius}};
decode_body(<<?GWINFO, GwId:8>>) ->
    %% No address field at all is the common case (the client already knows
    %% where the gateway is); keeping it out of the map makes encode/decode
    %% round trips compare equal.
    {ok, #{type => gwinfo, gw_id => GwId}};
decode_body(<<?GWINFO, GwId:8, GwAdd/binary>>) ->
    {ok, #{type => gwinfo, gw_id => GwId, gw_add => GwAdd}};
decode_body(<<?CONNECT, Flags:8, ProtocolId:8, Duration:16, ClientId/binary>>)
  when byte_size(ClientId) > 0 ->
    case reserved(Flags, 2#00111111) of
        true ->
            {error, {reserved_bits, Flags}};
        false ->
            {ok, #{type => connect,
                   will => bit(Flags, 7),
                   clean_session => bit(Flags, 6),
                   protocol_id => ProtocolId,
                   duration => Duration,
                   client_id => ClientId}}
    end;
decode_body(<<?CONNECT, _/binary>>) ->
    {error, truncated};
decode_body(<<?CONNACK, ReturnCode:8>>) ->
    {ok, #{type => connack, return_code => ReturnCode}};
decode_body(<<?CONNACK, _/binary>>) ->
    {error, truncated};
decode_body(<<?WILLTOPICREQ>>) ->
    {ok, #{type => willtopicreq}};
decode_body(<<?WILLMSGREQ>>) ->
    {ok, #{type => willmsgreq}};
decode_body(<<?WILLTOPIC, Flags:8, Topic/binary>>) ->
    will_topic_packet(willtopic, Flags, Topic);
decode_body(<<?WILLTOPIC, _/binary>>) ->
    {error, truncated};
decode_body(<<?WILLMSG, Payload/binary>>) ->
    {ok, #{type => willmsg, payload => Payload}};
decode_body(<<?REGISTER, TopicId:16, MsgId:16, Topic/binary>>)
  when byte_size(Topic) > 0 ->
    {ok, #{type => register, topic_id => TopicId, msg_id => MsgId, topic => Topic}};
decode_body(<<?REGISTER, _/binary>>) ->
    {error, truncated};
decode_body(<<?REGACK, TopicId:16, MsgId:16, ReturnCode:8>>) ->
    {ok, #{type => regack, topic_id => TopicId, msg_id => MsgId,
           return_code => ReturnCode}};
decode_body(<<?REGACK, _/binary>>) ->
    {error, truncated};
decode_body(<<?PUBLISH, Flags:8, TopicId:16, MsgId:16, Payload/binary>>) ->
    case topic_type_of(Flags) of
        {ok, TopicType} ->
            case reserved(Flags, 2#00001100) of
                true ->
                    {error, {reserved_bits, Flags}};
                false ->
                    {ok, #{type => publish,
                           dup => bit(Flags, 7),
                           qos => qos_of(Flags),
                           retain => bit(Flags, 4),
                           topic_id_type => TopicType,
                           topic_id => topic_id_value(TopicType, TopicId),
                           msg_id => MsgId,
                           payload => Payload}}
            end;
        error ->
            {error, {invalid_topic_id_type, Flags band 2#11}}
    end;
decode_body(<<?PUBLISH, _/binary>>) ->
    {error, truncated};
decode_body(<<?PUBACK, TopicId:16, MsgId:16, ReturnCode:8>>) ->
    {ok, #{type => puback, topic_id => TopicId, msg_id => MsgId,
           return_code => ReturnCode}};
decode_body(<<?PUBACK, _/binary>>) ->
    {error, truncated};
decode_body(<<?PUBREC, MsgId:16>>) ->
    {ok, #{type => pubrec, msg_id => MsgId}};
decode_body(<<?PUBREL, MsgId:16>>) ->
    {ok, #{type => pubrel, msg_id => MsgId}};
decode_body(<<?PUBCOMP, MsgId:16>>) ->
    {ok, #{type => pubcomp, msg_id => MsgId}};
decode_body(<<?PUBREC, _/binary>>) ->
    {error, truncated};
decode_body(<<?SUBSCRIBE, Flags:8, MsgId:16, Rest/binary>>) ->
    case topic_ref(Flags, Rest) of
        {ok, TopicType, TopicRef} ->
            case reserved(Flags, 2#00011100) of
                true -> {error, {reserved_bits, Flags}};
                false ->
                    {ok, #{type => subscribe,
                           dup => bit(Flags, 7),
                           qos => qos_of(Flags),
                           topic_id_type => TopicType,
                           msg_id => MsgId,
                           topic => TopicRef}}
            end;
        {error, _} = Err ->
            Err
    end;
decode_body(<<?SUBSCRIBE, _/binary>>) ->
    {error, truncated};
decode_body(<<?SUBACK, Flags:8, TopicId:16, MsgId:16, ReturnCode:8>>) ->
    {ok, #{type => suback, qos => qos_of(Flags), topic_id => TopicId,
           msg_id => MsgId, return_code => ReturnCode}};
decode_body(<<?SUBACK, _/binary>>) ->
    {error, truncated};
decode_body(<<?UNSUBSCRIBE, Flags:8, MsgId:16, Rest/binary>>) ->
    case topic_ref(Flags, Rest) of
        {ok, TopicType, TopicRef} ->
            {ok, #{type => unsubscribe, topic_id_type => TopicType,
                   msg_id => MsgId, topic => TopicRef}};
        {error, _} = Err ->
            Err
    end;
decode_body(<<?UNSUBSCRIBE, _/binary>>) ->
    {error, truncated};
decode_body(<<?UNSUBACK, MsgId:16>>) ->
    {ok, #{type => unsuback, msg_id => MsgId}};
decode_body(<<?UNSUBACK, _/binary>>) ->
    {error, truncated};
decode_body(<<?PINGREQ, ClientId/binary>>) ->
    {ok, #{type => pingreq, client_id => ClientId}};
decode_body(<<?PINGRESP>>) ->
    {ok, #{type => pingresp}};
decode_body(<<?DISCONNECT>>) ->
    {ok, #{type => disconnect, duration => 0}};
decode_body(<<?DISCONNECT, Duration:16>>) ->
    {ok, #{type => disconnect, duration => Duration}};
decode_body(<<?DISCONNECT, _/binary>>) ->
    {error, truncated};
decode_body(<<?WILLTOPICUPD, Flags:8, Topic/binary>>) ->
    will_topic_packet(willtopicupd, Flags, Topic);
decode_body(<<?WILLTOPICUPD, _/binary>>) ->
    {error, truncated};
decode_body(<<?WILLTOPICRESP, ReturnCode:8>>) ->
    {ok, #{type => willtopicresp, return_code => ReturnCode}};
decode_body(<<?WILLTOPICRESP, _/binary>>) ->
    {error, truncated};
decode_body(<<?WILLMSGUPD, Payload/binary>>) ->
    {ok, #{type => willmsgupd, payload => Payload}};
decode_body(<<?WILLMSGRESP, ReturnCode:8>>) ->
    {ok, #{type => willmsgresp, return_code => ReturnCode}};
decode_body(<<?WILLMSGRESP, _/binary>>) ->
    {error, truncated};
decode_body(<<Type:8, _/binary>>) ->
    {error, {unknown_type, Type}}.

%% @doc Encode one packet. The length header is chosen by the resulting size:
%% one byte while the whole packet fits in 255, three bytes above that.
-spec encode(map()) -> binary().
encode(#{type := Type} = Packet) ->
    Body = encode_body(Type, Packet),
    OneByte = byte_size(Body) + 1,
    case OneByte =< 255 of
        true ->
            <<OneByte:8, Body/binary>>;
        false ->
            ThreeByte = byte_size(Body) + 3,
            case ThreeByte =< 65535 of
                true -> <<1, ThreeByte:16, Body/binary>>;
                false -> error({packet_too_large, ThreeByte})
            end
    end.

encode_body(advertise, #{gw_id := GwId, duration := Duration}) ->
    <<?ADVERTISE, GwId:8, Duration:16>>;
encode_body(searchgw, #{radius := Radius}) ->
    <<?SEARCHGW, Radius:8>>;
encode_body(gwinfo, #{gw_id := GwId} = P) ->
    <<?GWINFO, GwId:8, (maps:get(gw_add, P, <<>>))/binary>>;
encode_body(connect, P) ->
    Flags = flag(bit_of(maps:get(will, P, 0)), 7)
        bor flag(bit_of(maps:get(clean_session, P, 0)), 6),
    <<?CONNECT, Flags:8, (maps:get(protocol_id, P, 1)):8,
      (maps:get(duration, P, 0)):16, (maps:get(client_id, P))/binary>>;
encode_body(connack, #{return_code := RC}) ->
    <<?CONNACK, RC:8>>;
encode_body(willtopicreq, _P) ->
    <<?WILLTOPICREQ>>;
encode_body(willmsgreq, _P) ->
    <<?WILLMSGREQ>>;
encode_body(willtopic, P) ->
    <<?WILLTOPIC, (flags(qos_byte(maps:get(qos, P, 0)),
                         bit_of(maps:get(retain, P, 0)))):8,
      (maps:get(topic, P))/binary>>;
encode_body(willmsg, #{payload := Payload}) ->
    <<?WILLMSG, Payload/binary>>;
encode_body(register, #{topic_id := TopicId, msg_id := MsgId, topic := Topic}) ->
    <<?REGISTER, TopicId:16, MsgId:16, Topic/binary>>;
encode_body(regack, #{topic_id := TopicId, msg_id := MsgId, return_code := RC}) ->
    <<?REGACK, TopicId:16, MsgId:16, RC:8>>;
encode_body(publish, P) ->
    Flags = flag(bit_of(maps:get(dup, P, 0)), 7)
        bor (qos_byte(maps:get(qos, P, 0)) bsl 5)
        bor flag(bit_of(maps:get(retain, P, 0)), 4)
        bor topic_type_byte(maps:get(topic_id_type, P, normal)),
    <<?PUBLISH, Flags:8,
      (topic_id_field(maps:get(topic_id_type, P, normal), maps:get(topic_id, P))):16,
      (maps:get(msg_id, P)):16, (maps:get(payload, P))/binary>>;
encode_body(puback, #{topic_id := TopicId, msg_id := MsgId, return_code := RC}) ->
    <<?PUBACK, TopicId:16, MsgId:16, RC:8>>;
encode_body(pubrec, #{msg_id := MsgId}) ->
    <<?PUBREC, MsgId:16>>;
encode_body(pubrel, #{msg_id := MsgId}) ->
    <<?PUBREL, MsgId:16>>;
encode_body(pubcomp, #{msg_id := MsgId}) ->
    <<?PUBCOMP, MsgId:16>>;
encode_body(subscribe, P) ->
    Flags = flag(bit_of(maps:get(dup, P, 0)), 7)
        bor (qos_byte(maps:get(qos, P, 0)) bsl 5)
        bor topic_type_byte(maps:get(topic_id_type, P, normal)),
    <<?SUBSCRIBE, Flags:8, (maps:get(msg_id, P)):16,
      (topic_ref_bytes(maps:get(topic_id_type, P, normal),
                       maps:get(topic, P)))/binary>>;
encode_body(suback, P) ->
    <<?SUBACK, (qos_byte(maps:get(qos, P, 0)) bsl 5):8,
      (maps:get(topic_id, P)):16, (maps:get(msg_id, P)):16,
      (maps:get(return_code, P)):8>>;
encode_body(unsubscribe, P) ->
    Flags = topic_type_byte(maps:get(topic_id_type, P, normal)),
    <<?UNSUBSCRIBE, Flags:8, (maps:get(msg_id, P)):16,
      (topic_ref_bytes(maps:get(topic_id_type, P, normal),
                       maps:get(topic, P)))/binary>>;
encode_body(unsuback, #{msg_id := MsgId}) ->
    <<?UNSUBACK, MsgId:16>>;
encode_body(pingreq, P) ->
    <<?PINGREQ, (maps:get(client_id, P, <<>>))/binary>>;
encode_body(pingresp, _P) ->
    <<?PINGRESP>>;
encode_body(disconnect, P) ->
    case maps:get(duration, P, 0) of
        0 -> <<?DISCONNECT>>;
        Duration -> <<?DISCONNECT, Duration:16>>
    end;
encode_body(willtopicupd, P) ->
    <<?WILLTOPICUPD, (flags(qos_byte(maps:get(qos, P, 0)),
                            bit_of(maps:get(retain, P, 0)))):8,
      (maps:get(topic, P))/binary>>;
encode_body(willtopicresp, #{return_code := RC}) ->
    <<?WILLTOPICRESP, RC:8>>;
encode_body(willmsgupd, #{payload := Payload}) ->
    <<?WILLMSGUPD, Payload/binary>>;
encode_body(willmsgresp, #{return_code := RC}) ->
    <<?WILLMSGRESP, RC:8>>;
encode_body(Type, _P) ->
    error({unsupported_packet, Type}).

%% WILLTOPIC and WILLTOPICUPD share a flag byte: QoS in bits 6-5, Retain in
%% bit 4, everything else reserved.
will_topic_packet(Type, Flags, Topic) ->
    case reserved(Flags, 2#10001111) of
        true ->
            {error, {reserved_bits, Flags}};
        false ->
            {ok, #{type => Type, qos => qos_of(Flags), retain => bit(Flags, 4),
                   topic => Topic}}
    end.

%%%_* Bit helpers -----------------------------------------------------------

%% Decoded flags are `true'/`false', not 1/0: the encoder accepts either, so
%% normalising on the way out means a round trip compares equal and no caller
%% has to remember which one it got.
bit(Byte, Bit) ->
    (Byte bsr Bit) band 1 =:= 1.

bit_of(true) -> 1;
bit_of(false) -> 0;
bit_of(N) when is_integer(N) -> N band 1.

flag(Value, Bit) ->
    (Value band 1) bsl Bit.

flags(QosByte, Retain) ->
    (QosByte bsl 5) bor flag(Retain, 4).

%% MQTT-SN encodes QoS -1 as the two-bit pattern 3, so the wire value is not
%% the module's `-1'. Keeping that conversion in two functions means no caller
%% has to remember it.
qos_of(Flags) ->
    case (Flags bsr 5) band 2#11 of
        3 -> -1;
        Qos -> Qos
    end.

qos_byte(-1) -> 3;
qos_byte(Qos) when Qos >= 0, Qos =< 2 -> Qos;
qos_byte(Qos) -> error({invalid_qos, Qos}).

topic_type_of(Flags) ->
    case Flags band 2#11 of
        0 -> {ok, normal};
        1 -> {ok, predefined};
        2 -> {ok, short};
        _ -> error
    end.

topic_type_byte(normal) -> ?TOPIC_NORMAL;
topic_type_byte(predefined) -> ?TOPIC_PREDEFINED;
topic_type_byte(short) -> ?TOPIC_SHORT.

%% A PUBLISH topic-id field is 16 bits that mean three different things: a
%% normal id, a predefined id, or -- when the type is `short' -- a two-character
%% topic name. Decoding it as a name where it is one keeps the packet shape the
%% same as SUBSCRIBE's variable topic reference, so the session process has one
%% rule to apply instead of two.
topic_id_value(short, Field) ->
    <<Field:16>>;
topic_id_value(_Type, Id) ->
    Id.

topic_id_field(short, <<A, B>>) when is_integer(A), is_integer(B) ->
    (A bsl 8) bor B;
topic_id_field(_Type, Id) when is_integer(Id) ->
    Id.

%% For a predefined topic the field is the numeric id (two bytes); for normal
%% and short topics it is the topic name itself. A short topic is exactly two
%% characters -- the name is the id, which is why nothing is allocated for it.
topic_ref(Flags, Rest) ->
    case topic_type_of(Flags) of
        {ok, predefined} ->
            case Rest of
                <<TopicId:16>> -> {ok, predefined, TopicId};
                <<TopicId:16, _/binary>> ->
                    {error, {trailing_bytes, predefined_topic_id, TopicId}};
                _ ->
                    {error, truncated}
            end;
        {ok, short} ->
            case Rest of
                <<Topic:2/binary>> -> {ok, short, Topic};
                <<Topic:2/binary, _/binary>> ->
                    {error, {trailing_bytes, short_topic, Topic}};
                _ ->
                    {error, truncated}
            end;
        {ok, normal} ->
            case Rest of
                <<>> -> {error, truncated};
                Topic -> {ok, normal, Topic}
            end;
        error ->
            {error, {invalid_topic_id_type, Flags band 2#11}}
    end.

topic_ref_bytes(predefined, TopicId) when is_integer(TopicId) ->
    <<TopicId:16>>;
topic_ref_bytes(short, Topic) when is_binary(Topic) ->
    %% A short topic is exactly two characters; the codec's contract is that the
    %% caller validated it, and `mqtt_sn_topics:short/1' is where that happens.
    Topic;
topic_ref_bytes(normal, Topic) when is_binary(Topic) ->
    Topic.

%% Reserved bits must be zero on the wire; accepting them silently would let a
%% malformed client look valid until some other field is misread.
reserved(Byte, Mask) ->
    Byte band Mask =/= 0.
