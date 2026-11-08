%% @doc MQTT 3.1.1 §2 wire codec: `decode/1' (streaming, strict, never
%% crashes) and `encode/1' (outbound frames; an illegal construction is a
%% programming error and raises — it never reaches the wire).
%%
%% Strictness is deliberate — §4.5's rejection posture leans on it:
%% fixed-header flags, reserved types, PUBLISH QoS 0b11, packet id 0, the
%% varint bound, and Annex A UTF-8 (NUL, surrogates, overlong, >U+10FFFF) all
%% come back as `{error, _}' for the conn to disconnect on without acking.
%%
%% Since S2-C the frame set is the complete packet range the broker needs:
%% PUBREC(5)/PUBREL(6)/PUBCOMP(7) decode with their fixed-header flags checked
%% (PUBREL is 0b0010, the other two 0b0000), and PUBLISH/will may carry QoS 2
%% (ADR-012, ADR-018).
%%
%% Conventions the conn_fsm relies on:
%%  * `{more, Buf}' means "wait for bytes", never "this prefix is bad".
%%  * Absent optional CONNECT fields are the atom `undefined' (an empty
%%    username `<<>>' stays distinguishable from no username); the integer
%%    wire bits (clean_session, dup, retain, session_present, will_flag/
%%    qos/retain) stay 0/1 integers in the records.
%%  * Non-minimal remaining-length varints (<<128,0>> = 0) are accepted:
%%    §2.2.3's own algorithm parses them; minimality is not spec text.
-module(mqtt_frame).

-export([decode/1, decode/2, encode/1, encode/2]).
%% Direct builds for the conn's hot outbound path (binary in one step, no
%% intermediate frame record).
-export([connack/2, puback/2, suback/2, unsuback/1, pingresp/0]).
%% Wire primitives, exported for the test suite only (xref unused-export on
%% these is expected).
-export([encode_remaining_length/1, decode_remaining_length/1]).

-export_type([frame/0]).

%% Single source for the §2 frame records (the conn_fsm and the test suite
%% include the same header; a divergence would be a silent record-layout
%% mismatch — mirrors how broker_router.hrl carries route_ctx).
-include("mqtt_frame.hrl").

-define(MAX_REMAINING_LENGTH, 268435455).

%% NOT opaque, on purpose: the records in mqtt_frame.hrl are this module's
%% published construction interface (the conn builds an outbound #publish{} and
%% hands it to encode/1), so declaring the union opaque would make the conn's
%% own legal frame a type error at the call site. Illegal frames are refused at
%% runtime by encode/1's field guards, which is where the guarantee actually is.
-type frame() :: #connect{} | #connack{} | #publish{} | #puback{}
               | #pubrec{} | #pubrel{} | #pubcomp{}
               | #subscribe{} | #suback{} | #unsubscribe{} | #unsuback{}
               | #disconnect{} | #ping{}.

%%%_* Decoding ---------------------------------------------------------------

%% @doc Decode the one frame at the head of an accumulating buffer; returns
%% the untouched remainder so the caller loops until `{more, _}'.
-spec decode(binary()) -> {ok, frame(), binary()} | {more, binary()}
                        | {error, term()}.
decode(Buf) ->
    %% `auto' keeps `decode/1' usable before the CONNECT told us the version:
    %% the only legal first frame is CONNECT, whose body carries its own level.
    decode(Buf, auto).

%% @doc Decode one frame for a known protocol version. `Vsn = 5' parses the
%% MQTT 5 properties/reason-code shapes (ADR-017); `4' is byte-for-byte the
%% 3.1.1 path, and never looks at a properties length.
-spec decode(binary(), 4 | 5 | auto) -> {ok, frame(), binary()} | {more, binary()}
                                      | {error, term()}.
decode(<<>>, _Vsn) ->
    {more, <<>>};
decode(<<TypeFlags:8, Rest0/binary>>, Vsn) ->
    Type = TypeFlags bsr 4,
    Flags = TypeFlags band 16#0F,
    %% Reserved packet types (§2.1.3) are rejected on the type byte alone:
    %% waiting for a body that will never be legal is `{more}' with no upside.
    case reserved_type(Type) of
        true ->
            {error, {malformed_packet, {reserved_type, Type}}};
        false ->
            decode_after_type(Type, Flags, Rest0, Vsn)
    end.

reserved_type(0) -> true;    %% §2.1.3 reserved
reserved_type(15) -> true;   %% §2.1.3 reserved
reserved_type(_) -> false.

decode_after_type(Type, Flags, Rest0, Vsn) ->
    case rl_decode(Rest0, 1, 0, 0) of
        more ->
            {more, <<(Type bsl 4 bor Flags):8, Rest0/binary>>};
        {error, Reason} ->
            {error, Reason};
        {ok, Remaining, Rest, _N} ->
            %% Not the whole body yet: report `more' before parsing — a
            %% truncated body is incomplete, not malformed, and parse_body's
            %% crash-on-short would be caught and mis-reported as error.
            %% Buffers carry type+varint bytes only while incomplete, so the
            %% >= branch below can never mis-read `more' as completeness.
            case byte_size(Rest) >= Remaining of
                true ->
                    <<Body:Remaining/binary, Rest2/binary>> = Rest,
                    case parse_body(Type, Flags, Body, Vsn) of
                        {ok, Frame} -> {ok, Frame, Rest2};
                        {error, Reason} -> {error, Reason}
                    end;
                false ->
                    {more, <<(Type bsl 4 bor Flags):8, Rest0/binary>>}
            end
    end.

%% @doc Remaining-Length varint: 1..4 bytes, total value <= 268435455.
%% The internal decoder needs no byte counter — the 4-byte bound falls out of
%% the continuation-bit chain ending at the 4th byte.
-spec decode_remaining_length(binary()) ->
    {ok, non_neg_integer(), binary()} | more | {error, term()}.
decode_remaining_length(Buf) ->
    case rl_decode(Buf, 1, 0, 0) of
        {ok, Value, Rest, _N} -> {ok, Value, Rest};
        Other -> Other
    end.

rl_decode(<<0:1, V:7, Rest/binary>>, Mul, Acc, N) ->
    {ok, Acc + V * Mul, Rest, N + 1};
rl_decode(<<1:1, V:7, Rest/binary>>, Mul, Acc, 3) ->
    case Acc + V * Mul of
        TooBig when TooBig > ?MAX_REMAINING_LENGTH ->
            {error, remaining_length_too_large};
        Partial ->
            rl_decode(Rest, Mul * 128, Partial, 4)
    end;
rl_decode(<<1:1, V:7, Rest/binary>>, Mul, Acc, N) ->
    rl_decode(Rest, Mul * 128, Acc + V * Mul, N + 1);
rl_decode(<<>>, _Mul, _Acc, _N) ->
    more;
rl_decode(<<_:8, _/binary>>, _Mul, _Acc, _N) ->
    {error, remaining_length_too_long}.

%% Safety net for fuzz input: the body parsers aim to return errors, but a
%% shape slipping through a gate must never crash the caller — the decode
%% contract is "never crash on any input".
parse_body(Type, Flags, Body, Vsn) ->
    try do_parse_body(Type, Flags, Body, Vsn)
    catch
        error:_ -> {error, {malformed_packet, {Type, Flags}}}
    end.

do_parse_body(1, 0, Body, _Vsn) when byte_size(Body) >= 10 ->
    p_connect(Body);
%% -- MQTT 5 shapes (ADR-017). Every one is selected by the version the
%% CONNECT negotiated; the 3.1.1 clauses below never see a properties length.
do_parse_body(2, 0, <<SP, RC, PropsBin/binary>>, 5) ->
    case read_props(PropsBin) of
        {ok, Props, <<>>} when SP =< 1 ->
            {ok, #connack{session_present = SP, return_code = RC,
                          props = Props}};
        {ok, _, <<>>} -> {error, {malformed_packet, {bad_connack_flags, SP}}};
        {ok, _, _} -> {error, {malformed_packet, connack_trailing_bytes}};
        {error, R} -> {error, {malformed_packet, R}}
    end;
do_parse_body(3, Flags, Body, 5) ->
    parse_publish_v5(Flags, Body);
do_parse_body(Type, 0, <<Pid:16, Rest/binary>>, 5)
  when Type >= 4, Type =< 7, Pid =/= 0 ->
    case ack_v5(Type, Pid, Rest) of
        {ok, Frame} -> {ok, Frame};
        {error, R} -> {error, {malformed_packet, R}}
    end;
do_parse_body(6, 2, Body, 5) ->
    case Body of
        <<Pid:16, Rest/binary>> when Pid =/= 0 ->
            case ack_v5(6, Pid, Rest) of
                {ok, Frame} -> {ok, Frame};
                {error, R} -> {error, {malformed_packet, R}}
            end;
        _ ->
            {error, {malformed_packet, {bad_fixed_header_or_type, 6}}}
    end;
do_parse_body(8, 2, Body, 5) ->
    p_subscribe_v5(Body);
do_parse_body(9, 0, <<Pid:16, Rest/binary>>, 5) when Pid =/= 0 ->
    case read_props(Rest) of
        {ok, Props, Codes} when Codes =/= <<>> ->
            {ok, #suback{packet_id = Pid,
                         return_codes = binary_to_list(Codes),
                         props = Props}};
        {ok, _, <<>>} -> {error, {malformed_packet, suback_empty}};
        {error, R} -> {error, {malformed_packet, R}}
    end;
do_parse_body(10, 2, Body, 5) ->
    p_unsubscribe_v5(Body);
do_parse_body(11, 0, <<Pid:16, Rest/binary>>, 5) when Pid =/= 0 ->
    case read_props(Rest) of
        {ok, Props, Codes} ->
            {ok, #unsuback{packet_id = Pid,
                           reason_codes = binary_to_list(Codes),
                           props = Props}};
        {error, R} -> {error, {malformed_packet, R}}
    end;
do_parse_body(14, 0, Body, 5) ->
    case Body of
        <<>> -> {ok, #disconnect{}};
        <<RC>> -> {ok, #disconnect{reason_code = RC}};
        <<RC, Rest/binary>> ->
            case read_props(Rest) of
                {ok, Props, <<>>} ->
                    {ok, #disconnect{reason_code = RC, props = Props}};
                {ok, _, _} ->
                    {error, {malformed_packet, disconnect_trailing_bytes}};
                {error, R} -> {error, {malformed_packet, R}}
            end
    end;
do_parse_body(2, 0, <<SP, RC>>, _Vsn) ->
    p_connack(SP, RC);
do_parse_body(3, Flags, Body, _Vsn) ->
    %% §2.2.2 fixed-header flags: bit3 DUP, bits2-1 QoS, bit0 Retain.
    Dup = Flags bsr 3,
    QoS = (Flags bsr 1) band 2#11,
    Retain = Flags band 1,
    if
        %% QoS 0b11 reserved (rejection posture). S2-C carries QoS 2 as well
        %% (ADR-012), so only 0b11 stays illegal.
        QoS =< 2 -> p_publish(Dup, QoS, Retain, Body);
        true -> {error, {malformed_packet, {bad_publish_qos, QoS}}}
    end;
do_parse_body(4, 0, <<Pid:16>>, _Vsn) when Pid =/= 0 ->
    {ok, #puback{packet_id = Pid}};
%% QoS 2 handshake: exact (type, flags) pairs, §2.2.2 table. PUBREL's flags
%% are 0b0010 by definition; a peer sending 0b0000 lands in the fallback and
%% is disconnected by the conn, which is the §4.5 rejection posture.
do_parse_body(5, 0, <<Pid:16>>, _Vsn) when Pid =/= 0 ->
    {ok, #pubrec{packet_id = Pid}};
do_parse_body(6, 2, <<Pid:16>>, _Vsn) when Pid =/= 0 ->
    {ok, #pubrel{packet_id = Pid}};
do_parse_body(7, 0, <<Pid:16>>, _Vsn) when Pid =/= 0 ->
    {ok, #pubcomp{packet_id = Pid}};
do_parse_body(8, 2, Body, _Vsn) ->
    p_subscribe(Body);
do_parse_body(9, 0, <<Pid:16, Codes/binary>>, _Vsn) when Pid =/= 0 ->
    {ok, #suback{packet_id = Pid, return_codes = binary_to_list(Codes)}};
do_parse_body(10, 2, Body, _Vsn) ->
    p_unsubscribe(Body);
do_parse_body(11, 0, <<Pid:16>>, _Vsn) when Pid =/= 0 ->
    {ok, #unsuback{packet_id = Pid}};
do_parse_body(12, 0, <<>>, _Vsn) ->
    {ok, #ping{kind = req}};
do_parse_body(13, 0, <<>>, _Vsn) ->
    {ok, #ping{kind = resp}};
do_parse_body(14, 0, <<>>, _Vsn) ->
    {ok, #disconnect{}};
do_parse_body(Type, _Flags, _Body, _Vsn) ->
    {error, {malformed_packet, {bad_fixed_header_or_type, Type}}}.

p_connect(Body) ->
    case read_string(Body) of
        {<<"MQTT">>, <<4, Flags, KeepAlive:16, Rest/binary>>} ->
            %% §3.1.2.3 bit table: 0 reserved, 1 clean session, 2 will flag,
            %% 4-3 will QoS, 5 will retain, 6 password, 7 user name.
            CSession = (Flags band 2#00000010) bsr 1,
            Will = (Flags band 2#00000100) bsr 2,
            WillQoS = (Flags band 2#00011000) bsr 3,
            WillRetain = (Flags band 2#00100000) bsr 5,
            UserPass = Flags band 2#11000000,
            Reserved = Flags band 2#00000001,
            if
                Reserved =/= 0 ->
                    %% §3.1.2.3: the reserved bit must be 0.
                    {error, {malformed_packet, connect_reserved_bit}};
                Will =:= 0 andalso (WillQoS =/= 0 orelse WillRetain =/= 0) ->
                    %% §3.1.2.4: Will QoS/Retain are meaningless (must be 0)
                    %% without the Will Flag.
                    {error, {malformed_packet, will_flags_without_will}};
                WillQoS > 2 ->
                    %% §3.1.2.4: QoS 3 is reserved; S2-C carries 0/1/2.
                    {error, {unsupported_will_qos, WillQoS}};
                true ->
                    connect_tail(Rest, CSession, Will, WillQoS,
                                 WillRetain, UserPass, KeepAlive)
            end;
        {<<"MQTT">>, <<5, Flags, KeepAlive:16, Rest/binary>>} ->
            connect_v5_tail(Rest, Flags, KeepAlive);
        {<<"MQTT">>, <<Other, _/binary>>} ->
            {error, {unsupported_protocol_version, Other}};
        {error, Reason} ->
            {error, Reason};
        too_short ->
            {error, malformed_connect_truncated};
        {Other, _Rest} ->
            {error, {unsupported_protocol, Other}}
    end.

connect_tail(<<Len:16, Id:Len/binary, R0/binary>>, CS, WF, WQ, WR, UserPass,
             KeepAlive) ->
    case validate_client_id(Id) of
        ok ->
            case read_will(R0, WF) of
                {error, Reason} ->
                    {error, Reason};
                {WT, WP, R2} ->
                    case validate_will_topic(WT) of
                        {error, Reason} ->
                            {error, Reason};
                        ok ->
                            case read_creds(R2, UserPass) of
                                {error, Reason} ->
                                    {error, Reason};
                                {U, P, <<>>} ->
                                    {ok, #connect{client_id = Id,
                                                  keep_alive = KeepAlive,
                                                  clean_session = CS,
                                                  will_flag = WF,
                                                  will_qos = WQ,
                                                  will_retain = WR,
                                                  will_topic = WT,
                                                  will_payload = WP,
                                                  username = U,
                                                  password = P}};
                                {_U, _P, _Trailing} ->
                                    {error, {malformed_packet,
                                            connect_trailing_bytes}}
                            end
                    end
            end;
        {error, Reason} ->
            {error, Reason}
    end;
connect_tail(_Short, _CS, _WF, _WQ, _WR, _UserPass, _KeepAlive) ->
    {error, malformed_connect_truncated}.

%% The optional sections are read as complete packages: a present Will Flag
%% consumes topic+payload, present credential flags consume username+password
%% together, and a half-set flag pair is rejected rather than silently
%% dropped (the record cannot represent an asymmetric pair).
read_will(R0, 0) ->
    {undefined, undefined, R0};
read_will(R0, 1) ->
    case read_string(R0) of
        {error, Reason} ->
            {error, Reason};
        too_short ->
            {error, {malformed_packet, will_topic_truncated}};
        {WT, RA} ->
            case read_binary(RA) of
                {WP, RB} ->
                    {WT, WP, RB};
                too_short ->
                    {error, {malformed_packet, will_payload_truncated}}
            end
    end.

read_creds(R2, 0) ->
    {undefined, undefined, R2};
read_creds(R2, 2#11000000) ->
    case read_string(R2) of
        {error, Reason} ->
            {error, Reason};
        too_short ->
            {error, {malformed_packet, username_truncated}};
        {U, R3} ->
            case read_binary(R3) of
                {P, R4} ->
                    {U, P, R4};
                too_short ->
                    {error, {malformed_packet, password_truncated}}
            end
    end;
read_creds(_R2, 2#10000000) ->
    {error, {malformed_packet, username_without_password}};
read_creds(_R2, 2#01000000) ->
    {error, {malformed_packet, password_without_username}}.

%% The 3.1.1 set is 0..5 and the rest is "reserved for future versions", but
%% design §4.5 (MQTT 5.0 CONNECT -> 0x87 Not supported) makes this broker SEND
%% one of those reserved bytes. A decoder that refused it would turn a
%% deliberate, readable refusal into a malformed-packet error on the client side
%% -- the same asymmetric-CONNACK decision as encode/1's outbound guard, taken
%% from the sending half. `<<SP,RC>>' above already bounds both to one byte.
p_connack(SP, _RC) when SP > 1 ->
    {error, {malformed_packet, {bad_connack_flags, SP}}};
p_connack(SP, RC) ->
    {ok, #connack{session_present = SP, return_code = RC}}.

p_publish(Dup, QoS, Retain, Body) ->
    case read_string(Body) of
        {error, Reason} ->
            {error, Reason};
        too_short ->
            {error, {malformed_packet, publish_truncated}};
        {Topic, R1} ->
            case validate_topic_name(Topic) of
                {error, Reason} ->
                    {error, Reason};
                ok ->
                    case if QoS =:= 0 -> {undefined, R1};
                           true -> take_pid(R1)
                       end of
                        {error, Reason} ->
                            {error, Reason};
                        {Pid, R2} ->
                            {ok, #publish{dup = Dup, qos = QoS,
                                         retain = Retain, topic = Topic,
                                         packet_id = Pid, payload = R2}}
                    end
            end
    end.

take_pid(<<0:16, _/binary>>) ->
    {error, zero_packet_id};
take_pid(<<Pid:16, Rest/binary>>) ->
    {Pid, Rest};
take_pid(_Short) ->
    {error, too_short}.

p_subscribe(<<Pid0:16, R0/binary>> = Body) when byte_size(Body) >= 3 ->
    case Pid0 of
        0 ->
            {error, zero_packet_id};
        _ ->
            case sub_topics(R0, []) of
                {ok, Topics, <<>>} ->
                    {ok, #subscribe{packet_id = Pid0, topics = Topics}};
                {error, Reason} ->
                    {error, Reason}
            end
    end;
p_subscribe(_Short) ->
    {error, {malformed_packet, subscribe_truncated}}.

sub_topics(<<>>, Acc) ->
    {ok, lists:reverse(Acc), <<>>};
sub_topics(Bin, Acc) ->
    case read_string(Bin) of
        {error, Reason} ->
            {error, Reason};
        too_short ->
            {error, {malformed_packet, subscribe_truncated}};
        {F, <<Byte, R/binary>>} ->
            case validate_filter_string(F) of
                ok when Byte =< 2 ->
                    sub_topics(R, [{F, Byte} | Acc]);
                ok ->
                    {error, {malformed_packet, {bad_subscribe_qos, Byte}}};
                {error, Reason} ->
                    {error, Reason}
            end;
        {F, <<>>} ->
            {error, {malformed_packet, {missing_qos_byte, F}}}
    end.

p_unsubscribe(<<Pid0:16, R0/binary>> = Body) when byte_size(Body) >= 3 ->
    case Pid0 of
        0 ->
            {error, zero_packet_id};
        _ ->
            case unsub_topics(R0, []) of
                {ok, Filters, <<>>} ->
                    {ok, #unsubscribe{packet_id = Pid0, topics = Filters}};
                {error, Reason} ->
                    {error, Reason}
            end
    end;
p_unsubscribe(_Short) ->
    {error, {malformed_packet, unsubscribe_truncated}}.

unsub_topics(<<>>, Acc) ->
    {ok, lists:reverse(Acc), <<>>};
unsub_topics(Bin, Acc) ->
    case read_string(Bin) of
        {error, Reason} ->
            {error, Reason};
        too_short ->
            {error, {malformed_packet, unsubscribe_truncated}};
        {Filter, Rest} ->
            case validate_filter_string(Filter) of
                ok ->
                    unsub_topics(Rest, [Filter | Acc]);
                {error, Reason} ->
                    {error, Reason}
            end
    end.

%%%_* MQTT 5 (S2.5, ADR-017) ------------------------------------------------

%% CONNECT v5: variable header ends with a properties length; the payload is
%% client id, optional will (properties + topic + payload), optional credentials.
connect_v5_tail(PropsBin, Flags, KeepAlive) ->
    CS = (Flags band 2#00000010) bsr 1,
    Will = (Flags band 2#00000100) bsr 2,
    WillQoS = (Flags band 2#00011000) bsr 3,
    WillRetain = (Flags band 2#00100000) bsr 5,
    UserPass = Flags band 2#11000000,
    Reserved = Flags band 2#00000001,
    if
        Reserved =/= 0 ->
            {error, {malformed_packet, connect_reserved_bit}};
        Will =:= 0 andalso (WillQoS =/= 0 orelse WillRetain =/= 0) ->
            {error, {malformed_packet, will_flags_without_will}};
        WillQoS > 2 ->
            {error, {unsupported_will_qos, WillQoS}};
        true ->
            case read_props(PropsBin) of
                {error, Reason} ->
                    {error, {malformed_packet, Reason}};
                {ok, Props, R0} ->
                    case connect_v5_payload(R0, CS, Will, WillQoS, WillRetain,
                                            UserPass, KeepAlive, Props) of
                        {error, Reason} -> {error, Reason};
                        {ok, Connect} -> {ok, Connect}
                    end
            end
    end.

connect_v5_payload(<<Len:16, Id:Len/binary, R0/binary>>, CS, WF, WQ, WR,
                   UserPass, KeepAlive, Props) ->
    case validate_client_id(Id) of
        {error, Reason} ->
            {error, Reason};
        ok ->
            case read_will_v5(R0, WF) of
                {error, Reason} ->
                    {error, Reason};
                {WT, WP, WProps, R1} ->
                    case validate_will_topic(WT) of
                        {error, Reason} ->
                            {error, Reason};
                        ok ->
                            case read_creds(R1, UserPass) of
                                {U, P, <<>>} ->
                                    {ok, #connect{proto_level = 5,
                                                  client_id = Id,
                                                  keep_alive = KeepAlive,
                                                  clean_session = CS,
                                                  will_flag = WF,
                                                  will_qos = WQ,
                                                  will_retain = WR,
                                                  will_topic = WT,
                                                  will_payload = WP,
                                                  username = U, password = P,
                                                  props = Props,
                                                  will_props = WProps}};
                                {error, Reason} ->
                                    {error, Reason};
                                {_, _, _Trailing} ->
                                    {error, {malformed_packet,
                                             connect_trailing_bytes}}
                            end
                    end
            end
    end;
connect_v5_payload(_Short, _CS, _WF, _WQ, _WR, _UserPass, _KA, _Props) ->
    {error, malformed_connect_truncated}.

read_will_v5(R0, 0) ->
    {undefined, undefined, #{}, R0};
read_will_v5(R0, 1) ->
    case read_props(R0) of
        {error, Reason} ->
            {error, {malformed_packet, Reason}};
        {ok, Props, R1} ->
            case read_string(R1) of
                {error, Reason} ->
                    {error, Reason};
                too_short ->
                    {error, {malformed_packet, will_topic_truncated}};
                {WT, R2} ->
                    case read_binary(R2) of
                        {WP, R3} -> {WT, WP, Props, R3};
                        too_short ->
                            {error, {malformed_packet, will_payload_truncated}}
                    end
            end
    end.

parse_publish_v5(Flags, Body) ->
    Dup = Flags bsr 3,
    QoS = (Flags bsr 1) band 2#11,
    Retain = Flags band 1,
    case QoS =< 2 of
        false ->
            {error, {malformed_packet, {bad_publish_qos, QoS}}};
        true ->
            case read_string(Body) of
                {error, Reason} -> {error, Reason};
                too_short -> {error, {malformed_packet, publish_truncated}};
                {Topic, R1} ->
                    case validate_topic_name(Topic) of
                        {error, Reason} ->
                            {error, Reason};
                        ok ->
                            case if QoS =:= 0 -> {undefined, R1};
                                   true -> take_pid(R1)
                               end of
                                {error, Reason} ->
                                    {error, Reason};
                                {Pid, R2} ->
                                    case read_props(R2) of
                                        {ok, Props, Payload} ->
                                            {ok, #publish{dup = Dup, qos = QoS,
                                                          retain = Retain,
                                                          topic = Topic,
                                                          packet_id = Pid,
                                                          payload = Payload,
                                                          props = Props}};
                                        {error, Reason} ->
                                            {error, {malformed_packet, Reason}}
                                    end
                            end
                    end
            end
    end.

%% PUBACK/PUBREC/PUBREL/PUBCOMP v5: the reason code and property length may be
%% omitted, but a present tail must parse exactly.
ack_v5(Type, Pid, <<>>) ->
    {ok, ack_record(Type, Pid, 0, #{})};
ack_v5(Type, Pid, <<RC>>) ->
    {ok, ack_record(Type, Pid, RC, #{})};
ack_v5(Type, Pid, <<RC, Rest/binary>>) ->
    case read_props(Rest) of
        {ok, Props, <<>>} -> {ok, ack_record(Type, Pid, RC, Props)};
        {ok, _, _} -> {error, ack_trailing_bytes};
        {error, Reason} -> {error, Reason}
    end.

ack_record(4, Pid, RC, Props) -> #puback{packet_id = Pid, reason_code = RC, props = Props};
ack_record(5, Pid, RC, Props) -> #pubrec{packet_id = Pid, reason_code = RC, props = Props};
ack_record(6, Pid, RC, Props) -> #pubrel{packet_id = Pid, reason_code = RC, props = Props};
ack_record(7, Pid, RC, Props) -> #pubcomp{packet_id = Pid, reason_code = RC, props = Props}.

p_subscribe_v5(<<Pid0:16, Rest/binary>> = Body)
  when byte_size(Body) >= 3, Pid0 =/= 0 ->
    case read_props(Rest) of
        {error, Reason} ->
            {error, {malformed_packet, Reason}};
        {ok, Props, TopicsBin} ->
            case sub_topics_v5(TopicsBin, []) of
                {ok, Topics, <<>>} ->
                    {ok, #subscribe{packet_id = Pid0, topics = Topics,
                                    props = Props}};
                {error, Reason} ->
                    {error, Reason}
            end
    end;
p_subscribe_v5(_Short) ->
    {error, {malformed_packet, subscribe_truncated}}.

sub_topics_v5(<<>>, []) ->
    {error, {malformed_packet, subscribe_truncated}};
sub_topics_v5(<<>>, Acc) ->
    {ok, lists:reverse(Acc), <<>>};
sub_topics_v5(Bin, Acc) ->
    case read_string(Bin) of
        {error, Reason} ->
            {error, Reason};
        too_short ->
            {error, {malformed_packet, subscribe_truncated}};
        {F, <<Opts, R/binary>>} ->
            QoS = Opts band 2#11,
            Reserved = Opts band 2#11000000,
            case {validate_filter_string(F), Reserved, QoS} of
                {ok, 0, Q} when Q =< 2 ->
                    sub_topics_v5(R, [{F, Q} | Acc]);
                {ok, _, Q} when Q > 2 ->
                    {error, {malformed_packet, {bad_subscribe_qos, Q}}};
                {ok, _, _} ->
                    {error, {malformed_packet, {bad_subscribe_options, Opts}}};
                {{error, Reason}, _, _} ->
                    {error, Reason}
            end;
        {F, <<>>} ->
            {error, {malformed_packet, {missing_qos_byte, F}}}
    end.

p_unsubscribe_v5(<<Pid0:16, Rest/binary>> = Body)
  when byte_size(Body) >= 3, Pid0 =/= 0 ->
    case read_props(Rest) of
        {error, Reason} ->
            {error, {malformed_packet, Reason}};
        {ok, Props, TopicsBin} ->
            case unsub_topics(TopicsBin, []) of
                {ok, Filters, <<>>} ->
                    {ok, #unsubscribe{packet_id = Pid0, topics = Filters,
                                      props = Props}};
                {error, Reason} ->
                    {error, Reason}
            end
    end;
p_unsubscribe_v5(_Short) ->
    {error, {malformed_packet, unsubscribe_truncated}}.

%% Properties: [length varint][id + value]*. Unknown ids and duplicate
%% single-valued ids are protocol errors, never silently dropped.
read_props(Bin) ->
    case rl_decode(Bin, 1, 0, 0) of
        more ->
            {error, properties_truncated};
        {error, Reason} ->
            {error, Reason};
        {ok, Len, Rest, _N} ->
            case Rest of
                <<PropsBin:Len/binary, Tail/binary>> ->
                    case parse_props(PropsBin, #{}) of
                        {ok, Props} -> {ok, Props, Tail};
                        {error, Reason} -> {error, Reason}
                    end;
                _ ->
                    {error, properties_truncated}
            end
    end.

parse_props(<<>>, Acc) ->
    {ok, Acc};
parse_props(Bin, Acc) ->
    case prop(Bin) of
        {ok, Key, Value, Rest} ->
            case {Key, maps:is_key(Key, Acc)} of
                {user_properties, _} ->
                    Prev = maps:get(user_properties, Acc, []),
                    parse_props(Rest, Acc#{user_properties => Prev ++ [Value]});
                {_, true} ->
                    {error, {duplicate_property, Key}};
                {_, false} ->
                    parse_props(Rest, Acc#{Key => Value})
            end;
        {error, Reason} ->
            {error, Reason}
    end.

prop(<<16#01, V:8, R/binary>>) -> {ok, payload_format_indicator, V, R};
prop(<<16#02, V:32, R/binary>>) -> {ok, message_expiry_interval, V, R};
prop(<<16#03, Rest/binary>>) -> utf8_prop(content_type, Rest);
prop(<<16#08, Rest/binary>>) -> utf8_prop(response_topic, Rest);
prop(<<16#09, Rest/binary>>) -> bin_prop(correlation_data, Rest);
prop(<<16#0B, Rest/binary>>) ->
    case rl_decode(Rest, 1, 0, 0) of
        {ok, V, R, _} -> {ok, subscription_identifier, V, R};
        _ -> {error, bad_subscription_identifier}
    end;
prop(<<16#11, V:32, R/binary>>) -> {ok, session_expiry_interval, V, R};
prop(<<16#12, Rest/binary>>) -> utf8_prop(assigned_client_identifier, Rest);
prop(<<16#13, V:16, R/binary>>) -> {ok, server_keep_alive, V, R};
prop(<<16#15, Rest/binary>>) -> utf8_prop(authentication_method, Rest);
prop(<<16#16, Rest/binary>>) -> bin_prop(authentication_data, Rest);
prop(<<16#17, V:8, R/binary>>) -> {ok, request_problem_information, V, R};
prop(<<16#18, V:32, R/binary>>) -> {ok, will_delay_interval, V, R};
prop(<<16#19, V:8, R/binary>>) -> {ok, request_response_information, V, R};
prop(<<16#1A, Rest/binary>>) -> utf8_prop(response_information, Rest);
prop(<<16#1C, Rest/binary>>) -> utf8_prop(server_reference, Rest);
prop(<<16#1F, Rest/binary>>) -> utf8_prop(reason_string, Rest);
prop(<<16#21, V:16, R/binary>>) -> {ok, receive_maximum, V, R};
prop(<<16#22, V:16, R/binary>>) -> {ok, topic_alias_maximum, V, R};
prop(<<16#23, V:16, R/binary>>) -> {ok, topic_alias, V, R};
prop(<<16#24, V:8, R/binary>>) -> {ok, maximum_qos, V, R};
prop(<<16#25, V:8, R/binary>>) -> {ok, retain_available, V, R};
prop(<<16#26, Rest/binary>>) ->
    case read_string(Rest) of
        {error, Reason} -> {error, Reason};
        too_short -> {error, user_property_truncated};
        {K, R1} ->
            case read_string(R1) of
                {error, Reason} -> {error, Reason};
                too_short -> {error, user_property_truncated};
                {V, R2} -> {ok, user_properties, {K, V}, R2}
            end
    end;
prop(<<16#27, V:32, R/binary>>) -> {ok, maximum_packet_size, V, R};
prop(<<16#28, V:8, R/binary>>) -> {ok, wildcard_subscription_available, V, R};
prop(<<16#29, V:8, R/binary>>) -> {ok, subscription_identifier_available, V, R};
prop(<<16#2A, V:8, R/binary>>) -> {ok, shared_subscription_available, V, R};
prop(<<Id, _/binary>>) -> {error, {unknown_property, Id}};
prop(<<>>) -> {error, properties_truncated}.

utf8_prop(Key, Rest) ->
    case read_string(Rest) of
        {error, Reason} -> {error, Reason};
        too_short -> {error, {bad_property_string, Key}};
        {V, R} -> {ok, Key, V, R}
    end.

bin_prop(Key, Rest) ->
    case read_binary(Rest) of
        {V, R} -> {ok, Key, V, R};
        too_short -> {error, {bad_property_binary, Key}}
    end.

%%%_* Wire readers and string validation (Annex A) ---------------------------

read_string(<<Len:16, S:Len/binary, Rest/binary>>) ->
    case validate_string(S) of
        ok -> {S, Rest};
        {error, Reason} -> {error, Reason}
    end;
read_string(<<_/bitstring>>) ->
    too_short;
read_string(<<>>) ->
    too_short.

read_binary(<<Len:16, B:Len/binary, Rest/binary>>) ->
    {B, Rest};
read_binary(_) ->
    too_short.

validate_string(S) ->
    case byte_size(S) > 65535 of
        true ->
            {error, string_too_long};
        false ->
            %% NUL is checked first: a byte with value 0 fails the UTF-8
            %% scanner too, and `embedded_null' is the more informative
            %% rejection reason (Annex A forbids it separately).
            case binary:match(S, <<0>>) of
                nomatch ->
                    case utf8_ok(S) of
                        true -> ok;
                        false -> {error, {malformed_packet, invalid_utf8}}
                    end;
                _ ->
                    {error, {malformed_packet, embedded_null}}
            end
    end.

%% Topic *filters* carry wildcards (validated to the §4.7 shape); topic
%% *names* must carry none. The two directions are kept apart on purpose.
validate_filter_string(Filter) ->
    case validate_string(Filter) of
        ok ->
            case filter_shape_ok(Filter) of
                true -> ok;
                false -> {error, {malformed_packet, bad_filter_shape}}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

validate_topic_name(<<>>) ->
    {error, {malformed_packet, empty_topic_name}};
validate_topic_name(Topic) ->
    case validate_string(Topic) of
        ok ->
            case binary:match(Topic, [<<"+">>, <<"#">>]) of
                nomatch -> ok;
                _ -> {error, {malformed_packet, wildcard_in_topic}}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

validate_client_id(<<>>) ->
    ok;
validate_client_id(ClientId) ->
    validate_string(ClientId).

validate_will_topic(undefined) ->
    ok;
validate_will_topic(Topic) ->
    validate_topic_name(Topic).

%% §4.7: '#' only as the whole final level, '+' only as a whole level.
filter_shape_ok(Filter) ->
    filter_levels(binary:split(Filter, <<"/">>, [global])).

filter_levels([<<"#">>]) ->
    true;
filter_levels([<<"#">> | _]) ->
    false;
filter_levels([<<"+">> | Rest]) ->
    filter_levels(Rest);
filter_levels([Level | Rest]) ->
    case binary:match(Level, [<<"+">>, <<"#">>]) of
        nomatch -> filter_levels(Rest);
        _ -> false
    end;
filter_levels([]) ->
    true.

%% Valid UTF-8 with every rejection Annex A lists: surrogates (D800..DFFF is
%% the E+ED/A>=A0 edge below), overlong forms (the A=0 continuation bounds),
%% code points beyond U+10FFFF (the F4/10FFFF bound). NUL is checked
%% separately (well-formed UTF-8 that MQTT still forbids in strings).
utf8_ok(<<>>) ->
    true;
utf8_ok(<<0:8, _/binary>>) ->
    false;
utf8_ok(<<0:1, _:7, Rest/binary>>) ->
    utf8_ok(Rest);
utf8_ok(<<2#110:3, A:5, 2#10:2, _:6, Rest/binary>>) when A >= 2#00010 ->
    utf8_ok(Rest);
utf8_ok(<<2#1110:4, A:4, 2#10:2, B:6, 2#10:2, _:6, Rest/binary>>)
  when A =< 2#1110,
       %% E0 overlong floor (>= U+0800).
       A > 0 orelse B >= 2#100000,
       %% ED A0..BF = surrogates.
       A =/= 2#1101 orelse B =< 2#011111 ->
    utf8_ok(Rest);
utf8_ok(<<2#11110:5, A:3, 2#10:2, B:6, 2#10:2, _:6, 2#10:2, _:6, Rest/binary>>)
  when A =< 2#100,
       %% F0 overlong floor (>= U+10000), F4 ceiling (<= U+10FFFF).
       A > 0 orelse B >= 2#010000,
       A < 2#100 orelse B =< 2#001111 ->
    utf8_ok(Rest);
utf8_ok(_) ->
    false.

%%%_* Encoding ---------------------------------------------------------------

%% @doc Encode one frame for the negotiated protocol version. Field validation
%% raises on programming errors — nothing the conn builds should ever hit these
%% arms, and a silent malformed frame on the wire would be worse than a crash in
%% the writer.
-spec encode(frame()) -> binary().
encode(Frame) ->
    encode(Frame, 4).

-spec encode(frame(), 4 | 5) -> binary().
encode(Frame, 4) ->
    encode_v4(Frame);
encode(Frame, 5) ->
    encode_v5(Frame).

encode_v4(#connack{session_present = SP, return_code = RC}) ->
    if
        is_integer(SP), SP band 16#FE =:= 0,
        is_integer(RC), RC >= 0, RC =< 255 ->
            %% 0..255 rather than 3.1.1's defined 0..5: design §4.5 refuses an
            %% MQTT 5.0 CONNECT with 0x87 on a 3.1.1 CONNACK, which is a legal
            %% byte the peer reads as a failure. The INBOUND guard stays strict
            %% (p_connack/2 rejects >5) — a broker never receives a CONNACK.
            fixed_header(2, 0, <<SP, RC>>);
        true ->
            erlang:error({malformed_frame_out, connack})
    end;
encode_v4(#publish{topic = Topic, payload = Payload} = F) ->
    case ok_topic(Topic) andalso is_binary(Payload) of
        true ->
            e_publish(F);
        false ->
            erlang:error({malformed_frame_out, publish})
    end;
encode_v4(#puback{packet_id = Pid}) ->
    if
        is_integer(Pid), Pid >= 1, Pid =< 65535 ->
            fixed_header(4, 0, <<Pid:16>>);
        true ->
            erlang:error({malformed_frame_out, puback})
    end;
encode_v4(#pubrec{packet_id = Pid}) ->
    if
        is_integer(Pid), Pid >= 1, Pid =< 65535 ->
            fixed_header(5, 0, <<Pid:16>>);
        true ->
            erlang:error({malformed_frame_out, pubrec})
    end;
encode_v4(#pubrel{packet_id = Pid}) ->
    if
        is_integer(Pid), Pid >= 1, Pid =< 65535 ->
            %% §3.6.1: PUBREL's fixed header flags are 0b0010, mandatory.
            fixed_header(6, 2, <<Pid:16>>);
        true ->
            erlang:error({malformed_frame_out, pubrel})
    end;
encode_v4(#pubcomp{packet_id = Pid}) ->
    if
        is_integer(Pid), Pid >= 1, Pid =< 65535 ->
            fixed_header(7, 0, <<Pid:16>>);
        true ->
            erlang:error({malformed_frame_out, pubcomp})
    end;
encode_v4(#subscribe{packet_id = Pid, topics = Topics}) ->
    if
        is_integer(Pid), Pid >= 1, Pid =< 65535,
        is_list(Topics), Topics =/= [] ->
            Body = list_to_binary(
                     [<<(len16(T))/binary, T/binary, Byte>>
                      || {T, Byte} <- Topics]),
            fixed_header(8, 2, <<Pid:16, Body/binary>>);
        true ->
            erlang:error({malformed_frame_out, subscribe})
    end;
encode_v4(#suback{packet_id = Pid, return_codes = Codes}) ->
    if
        is_integer(Pid), Pid >= 1, Pid =< 65535,
        is_list(Codes), Codes =/= [] ->
            fixed_header(9, 0, <<Pid:16, (list_to_binary(Codes))/binary>>);
        true ->
            erlang:error({malformed_frame_out, suback})
    end;
encode_v4(#unsubscribe{packet_id = Pid, topics = Topics}) ->
    if
        is_integer(Pid), Pid >= 1, Pid =< 65535,
        is_list(Topics), Topics =/= [] ->
            Body = list_to_binary([<<(len16(T))/binary, T/binary>>
                                   || T <- Topics]),
            fixed_header(10, 2, <<Pid:16, Body/binary>>);
        true ->
            erlang:error({malformed_frame_out, unsubscribe})
    end;
encode_v4(#unsuback{packet_id = Pid}) ->
    if
        is_integer(Pid), Pid >= 1, Pid =< 65535 ->
            fixed_header(11, 0, <<Pid:16>>);
        true ->
            erlang:error({malformed_frame_out, unsuback})
    end;
encode_v4(#disconnect{}) ->
    fixed_header(14, 0, <<>>);
encode_v4(#ping{kind = req}) ->
    fixed_header(12, 0, <<>>);
encode_v4(#ping{kind = resp}) ->
    fixed_header(13, 0, <<>>);
encode_v4(#connect{client_id = ClientId, keep_alive = KA, clean_session = CS,
                will_flag = WF, will_qos = WQ, will_retain = WR,
                will_topic = WTopic, will_payload = WPayload,
                username = User, password = Pass}) ->
    HasCreds = is_binary(User) andalso is_binary(Pass),
    NoCreds = User =:= undefined andalso Pass =:= undefined,
    if
        is_binary(ClientId), byte_size(ClientId) =< 65535,
        is_integer(KA), KA >= 0, KA =< 65535,
        CS band 16#FE =:= 0,
        WF band 16#FE =:= 0, WQ band 16#FC =:= 0, WR band 16#FE =:= 0,
        (WF =:= 0 andalso WTopic =:= undefined andalso WPayload =:= undefined)
            orelse (WF =:= 1 andalso is_binary(WTopic) andalso
                    is_binary(WPayload)),
        HasCreds orelse NoCreds ->
            Flags = (CS bsl 1) bor (WF bsl 2) bor (WQ bsl 3) bor (WR bsl 5)
                    bor (bitval(HasCreds) bsl 6) bor (bitval(HasCreds) bsl 7),
            WillPart = case WF of
                           1 -> <<(len16(WTopic))/binary, WTopic/binary,
                                  (len16(WPayload))/binary, WPayload/binary>>;
                           0 -> <<>>
                       end,
            CredsPart = case HasCreds of
                            true -> <<(len16(User))/binary, User/binary,
                                      (len16(Pass))/binary, Pass/binary>>;
                            false -> <<>>
                        end,
            Body = <<(len16(<<"MQTT">>))/binary, "MQTT", 4, Flags, KA:16,
                    (len16(ClientId))/binary, ClientId/binary,
                    WillPart/binary, CredsPart/binary>>,
            fixed_header(1, 0, Body);
        true ->
            erlang:error({malformed_frame_out, connect})
    end.

%%%_* MQTT 5 encoders (S2.5, ADR-017) ----------------------------------------

encode_v5(#connack{session_present = SP, return_code = RC, props = Props}) ->
    if
        is_integer(SP), SP band 16#FE =:= 0,
        is_integer(RC), RC >= 0, RC =< 255 ->
            P = props_bin(Props),
            fixed_header(2, 0, <<SP, RC, P/binary>>);
        true ->
            erlang:error({malformed_frame_out, connack})
    end;
encode_v5(#publish{topic = Topic, payload = Payload, dup = Dup, qos = QoS,
                   retain = Ret, packet_id = Pid, props = Props}) ->
    case ok_topic(Topic) andalso is_binary(Payload) andalso QoS =< 2 of
        false ->
            erlang:error({malformed_frame_out, publish});
        true ->
            P = props_bin(Props),
            Flags = (Dup bsl 3) bor (QoS bsl 1) bor Ret,
            Body = case QoS of
                       0 -> <<(len16(Topic))/binary, Topic/binary, P/binary,
                              Payload/binary>>;
                       _ when is_integer(Pid), Pid >= 1, Pid =< 65535 ->
                           <<(len16(Topic))/binary, Topic/binary, Pid:16,
                             P/binary, Payload/binary>>;
                       _ -> erlang:error({malformed_frame_out, publish})
                   end,
            fixed_header(3, Flags, Body)
    end;
encode_v5(#puback{packet_id = Pid, reason_code = RC, props = Props}) ->
    ack_v5_out(4, Pid, RC, Props);
encode_v5(#pubrec{packet_id = Pid, reason_code = RC, props = Props}) ->
    ack_v5_out(5, Pid, RC, Props);
encode_v5(#pubrel{packet_id = Pid, reason_code = RC, props = Props}) ->
    ack_v5_out(6, Pid, RC, Props);
encode_v5(#pubcomp{packet_id = Pid, reason_code = RC, props = Props}) ->
    ack_v5_out(7, Pid, RC, Props);
encode_v5(#subscribe{packet_id = Pid, topics = Topics, props = Props}) ->
    if
        is_integer(Pid), Pid >= 1, Pid =< 65535,
        is_list(Topics), Topics =/= [] ->
            Body = list_to_binary(
                     [<<(len16(T))/binary, T/binary, Q>> || {T, Q} <- Topics]),
            fixed_header(8, 2, <<Pid:16, (props_bin(Props))/binary, Body/binary>>);
        true ->
            erlang:error({malformed_frame_out, subscribe})
    end;
encode_v5(#suback{packet_id = Pid, return_codes = Codes, props = Props}) ->
    if
        is_integer(Pid), Pid >= 1, Pid =< 65535,
        is_list(Codes), Codes =/= [] ->
            fixed_header(9, 0, <<Pid:16, (props_bin(Props))/binary,
                                (list_to_binary(Codes))/binary>>);
        true ->
            erlang:error({malformed_frame_out, suback})
    end;
encode_v5(#unsubscribe{packet_id = Pid, topics = Topics, props = Props}) ->
    if
        is_integer(Pid), Pid >= 1, Pid =< 65535,
        is_list(Topics), Topics =/= [] ->
            Body = list_to_binary([<<(len16(T))/binary, T/binary>>
                                   || T <- Topics]),
            fixed_header(10, 2, <<Pid:16, (props_bin(Props))/binary, Body/binary>>);
        true ->
            erlang:error({malformed_frame_out, unsubscribe})
    end;
encode_v5(#unsuback{packet_id = Pid, reason_codes = Codes, props = Props}) ->
    if
        is_integer(Pid), Pid >= 1, Pid =< 65535 ->
            fixed_header(11, 0, <<Pid:16, (props_bin(Props))/binary,
                                 (list_to_binary(Codes))/binary>>);
        true ->
            erlang:error({malformed_frame_out, unsuback})
    end;
encode_v5(#disconnect{reason_code = RC, props = Props}) ->
    fixed_header(14, 0, <<RC, (props_bin(Props))/binary>>);
encode_v5(#ping{kind = req}) ->
    fixed_header(12, 0, <<>>);
encode_v5(#ping{kind = resp}) ->
    fixed_header(13, 0, <<>>);
encode_v5(#connect{client_id = ClientId, keep_alive = KA, clean_session = CS,
                   will_flag = WF, will_qos = WQ, will_retain = WR,
                   will_topic = WTopic, will_payload = WPayload,
                   username = User, password = Pass, props = Props,
                   will_props = WProps}) ->
    HasCreds = is_binary(User) andalso is_binary(Pass),
    NoCreds = User =:= undefined andalso Pass =:= undefined,
    if
        is_binary(ClientId), byte_size(ClientId) =< 65535,
        is_integer(KA), KA >= 0, KA =< 65535,
        CS band 16#FE =:= 0,
        WF band 16#FE =:= 0, WQ band 16#FC =:= 0, WR band 16#FE =:= 0,
        (WF =:= 0 andalso WTopic =:= undefined andalso WPayload =:= undefined)
            orelse (WF =:= 1 andalso is_binary(WTopic) andalso
                    is_binary(WPayload)),
        HasCreds orelse NoCreds ->
            Flags = (CS bsl 1) bor (WF bsl 2) bor (WQ bsl 3) bor (WR bsl 5)
                    bor (bitval(HasCreds) bsl 6) bor (bitval(HasCreds) bsl 7),
            WillPart = case WF of
                           1 -> <<(props_bin(WProps))/binary,
                                  (len16(WTopic))/binary, WTopic/binary,
                                  (len16(WPayload))/binary, WPayload/binary>>;
                           0 -> <<>>
                       end,
            CredsPart = case HasCreds of
                            true -> <<(len16(User))/binary, User/binary,
                                      (len16(Pass))/binary, Pass/binary>>;
                            false -> <<>>
                        end,
            Body = <<(len16(<<"MQTT">>))/binary, "MQTT", 5, Flags, KA:16,
                    (props_bin(Props))/binary,
                    (len16(ClientId))/binary, ClientId/binary,
                    WillPart/binary, CredsPart/binary>>,
            fixed_header(1, 0, Body);
        true ->
            erlang:error({malformed_frame_out, connect})
    end.

ack_v5_out(Type, Pid, RC, Props)
  when is_integer(Pid), Pid >= 1, Pid =< 65535,
       is_integer(RC), RC >= 0, RC =< 255 ->
    Flags = case Type of 6 -> 2; _ -> 0 end,
    fixed_header(Type, Flags, <<Pid:16, RC, (props_bin(Props))/binary>>);
ack_v5_out(_Type, _Pid, _RC, _Props) ->
    erlang:error({malformed_frame_out, ack}).

%% Deterministic order so a re-encode is byte-identical; unknown keys are a
%% programming error, not something to drop silently.
props_bin(Props) when is_map(Props) ->
    Bin = props_body(maps:to_list(Props)),
    <<(encode_remaining_length(byte_size(Bin)))/binary, Bin/binary>>.

props_body(Pairs) ->
    lists:foldl(fun prop_bin/2, <<>>, lists:sort(Pairs)).

prop_bin({user_properties, Pairs}, Acc) when is_list(Pairs) ->
    lists:foldl(
      fun({K, V}, A) ->
              <<A/binary, 16#26, (len16(K))/binary, K/binary,
                (len16(V))/binary, V/binary>>
      end, Acc, Pairs);
prop_bin({payload_format_indicator, V}, Acc) ->
    <<Acc/binary, 16#01, V:8>>;
prop_bin({message_expiry_interval, V}, Acc) ->
    <<Acc/binary, 16#02, V:32>>;
prop_bin({content_type, V}, Acc) ->
    <<Acc/binary, 16#03, (len16(V))/binary, V/binary>>;
prop_bin({response_topic, V}, Acc) ->
    <<Acc/binary, 16#08, (len16(V))/binary, V/binary>>;
prop_bin({correlation_data, V}, Acc) ->
    <<Acc/binary, 16#09, (len16(V))/binary, V/binary>>;
prop_bin({subscription_identifier, V}, Acc) ->
    <<Acc/binary, 16#0B, (encode_remaining_length(V))/binary>>;
prop_bin({session_expiry_interval, V}, Acc) ->
    <<Acc/binary, 16#11, V:32>>;
prop_bin({assigned_client_identifier, V}, Acc) ->
    <<Acc/binary, 16#12, (len16(V))/binary, V/binary>>;
prop_bin({server_keep_alive, V}, Acc) ->
    <<Acc/binary, 16#13, V:16>>;
prop_bin({authentication_method, V}, Acc) ->
    <<Acc/binary, 16#15, (len16(V))/binary, V/binary>>;
prop_bin({authentication_data, V}, Acc) ->
    <<Acc/binary, 16#16, (len16(V))/binary, V/binary>>;
prop_bin({request_problem_information, V}, Acc) ->
    <<Acc/binary, 16#17, V:8>>;
prop_bin({will_delay_interval, V}, Acc) ->
    <<Acc/binary, 16#18, V:32>>;
prop_bin({request_response_information, V}, Acc) ->
    <<Acc/binary, 16#19, V:8>>;
prop_bin({response_information, V}, Acc) ->
    <<Acc/binary, 16#1A, (len16(V))/binary, V/binary>>;
prop_bin({server_reference, V}, Acc) ->
    <<Acc/binary, 16#1C, (len16(V))/binary, V/binary>>;
prop_bin({reason_string, V}, Acc) ->
    <<Acc/binary, 16#1F, (len16(V))/binary, V/binary>>;
prop_bin({receive_maximum, V}, Acc) ->
    <<Acc/binary, 16#21, V:16>>;
prop_bin({topic_alias_maximum, V}, Acc) ->
    <<Acc/binary, 16#22, V:16>>;
prop_bin({topic_alias, V}, Acc) ->
    <<Acc/binary, 16#23, V:16>>;
prop_bin({maximum_qos, V}, Acc) ->
    <<Acc/binary, 16#24, V:8>>;
prop_bin({retain_available, V}, Acc) ->
    <<Acc/binary, 16#25, V:8>>;
prop_bin({maximum_packet_size, V}, Acc) ->
    <<Acc/binary, 16#27, V:32>>;
prop_bin({wildcard_subscription_available, V}, Acc) ->
    <<Acc/binary, 16#28, V:8>>;
prop_bin({subscription_identifier_available, V}, Acc) ->
    <<Acc/binary, 16#29, V:8>>;
prop_bin({shared_subscription_available, V}, Acc) ->
    <<Acc/binary, 16#2A, V:8>>;
prop_bin(Other, _Acc) ->
    erlang:error({malformed_frame_out, {property, Other}}).

%%%_* Direct builds ----------------------------------------------------------

connack(SP, RC) ->
    encode(#connack{session_present = SP, return_code = RC}).

puback(_ClientId, Pid) ->
    encode(#puback{packet_id = Pid}).

suback(Pid, Codes) ->
    encode(#suback{packet_id = Pid, return_codes = Codes}).

unsuback(Pid) ->
    encode(#unsuback{packet_id = Pid}).

pingresp() ->
    encode(#ping{kind = resp}).

%%%_* Internal ---------------------------------------------------------------

e_publish(#publish{dup = Dup, qos = 0, retain = Ret, topic = Topic,
                   payload = Payload}) ->
    Flags = (Dup bsl 3) bor (Ret bsl 0),
    fixed_header(3, Flags, <<(len16(Topic))/binary, Topic/binary,
                             Payload/binary>>);
e_publish(#publish{dup = Dup, qos = QoS, retain = Ret, topic = Topic,
                   packet_id = Pid, payload = Payload})
  when QoS =:= 1; QoS =:= 2 ->
    if
        is_integer(Pid), Pid >= 1, Pid =< 65535 ->
            Flags = (Dup bsl 3) bor (QoS bsl 1) bor (Ret bsl 0),
            fixed_header(3, Flags, <<(len16(Topic))/binary, Topic/binary,
                                     Pid:16, Payload/binary>>);
        true ->
            erlang:error({malformed_frame_out, publish})
    end.

ok_topic(Topic) ->
    is_binary(Topic) andalso Topic =/= <<>> andalso
        byte_size(Topic) =< 65535 andalso
        binary:match(Topic, [<<"+">>, <<"#">>]) =:= nomatch.

fixed_header(Type, Flags, Content) ->
    Size = byte_size(Content),
    if
        Type >= 0, Type =< 15, Flags >= 0, Flags =< 15,
        Size =< ?MAX_REMAINING_LENGTH ->
            ok;
        true ->
            erlang:error({malformed_frame_out, {Type, Flags, Size}})
    end,
    <<((Type bsl 4) bor Flags):8, (encode_remaining_length(Size))/binary,
      Content/binary>>.

%% @doc Remaining-Length varint (§2.2.3), 1..4 bytes for values <= 268435455.
-spec encode_remaining_length(non_neg_integer()) -> binary().
encode_remaining_length(Value) when
      is_integer(Value), Value >= 0, Value =< ?MAX_REMAINING_LENGTH ->
    rl(Value, <<>>).

rl(X, Acc) when X < 128 ->
    <<Acc/binary, X>>;
rl(X, Acc) ->
    rl(X div 128, <<Acc/binary, ((X rem 128) bor 128):8>>).

len16(B) ->
    <<(byte_size(B)):16>>.

bitval(true) -> 1;
bitval(false) -> 0.
