%% @doc MQTT 3.1.1 §2 wire codec: `decode/1' (streaming, strict, never
%% crashes) and `encode/1' (outbound frames; an illegal construction is a
%% programming error and raises — it never reaches the wire).
%%
%% Strictness is deliberate for S1 — §4.5's rejection posture leans on it:
%% fixed-header flags, reserved types, PUBLISH QoS 0b11, packet id 0, the
%% varint bound, and Annex A UTF-8 (NUL, surrogates, overlong, >U+10FFFF) all
%% come back as `{error, _}' for the conn to disconnect on without acking.
%% The S1 frame set carries no QoS 2 handshake: PUBREC/PUBREL/PUBCOMP (types
%% 5-7) have no records and decode to `{error, _}' — which satisfies §4.5's
%% "QoS 2 PUBLISH -> disconnect" just as the conn's own posture check does.
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

-export([decode/1, encode/1]).
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
               | #subscribe{} | #suback{} | #unsubscribe{} | #unsuback{}
               | #disconnect{} | #ping{}.

%%%_* Decoding ---------------------------------------------------------------

%% @doc Decode the one frame at the head of an accumulating buffer; returns
%% the untouched remainder so the caller loops until `{more, _}'.
-spec decode(binary()) -> {ok, frame(), binary()} | {more, binary()}
                        | {error, term()}.
decode(<<>>) ->
    {more, <<>>};
decode(<<TypeFlags:8, Rest0/binary>>) ->
    Type = TypeFlags bsr 4,
    Flags = TypeFlags band 16#0F,
    %% Reserved packet types (§2.1.3) and the QoS 2 handshake (5-7, no S1
    %% frame set) are rejected on the type byte alone: waiting for a body
    %% that will never be legal is `{more}' with no upside.
    case reserved_type(Type) of
        true ->
            {error, {malformed_packet, {reserved_type, Type}}};
        false ->
            decode_after_type(Type, Flags, Rest0)
    end.

reserved_type(0) -> true;    %% §2.1.3 reserved
reserved_type(5) -> true;    %% PUBREC
reserved_type(6) -> true;    %% PUBREL
reserved_type(7) -> true;    %% PUBCOMP
reserved_type(15) -> true;   %% §2.1.3 reserved
reserved_type(_) -> false.

decode_after_type(Type, Flags, Rest0) ->
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
                    case parse_body(Type, Flags, Body) of
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
parse_body(Type, Flags, Body) ->
    try do_parse_body(Type, Flags, Body)
    catch
        error:_ -> {error, {malformed_packet, {Type, Flags}}}
    end.

do_parse_body(1, 0, Body) when byte_size(Body) >= 10 ->
    p_connect(Body);
do_parse_body(2, 0, <<SP, RC>>) ->
    p_connack(SP, RC);
do_parse_body(3, Flags, Body) ->
    %% §2.2.2 fixed-header flags: bit3 DUP, bits2-1 QoS, bit0 Retain.
    Dup = Flags bsr 3,
    QoS = (Flags bsr 1) band 2#11,
    Retain = Flags band 1,
    if
        %% QoS 0b11 reserved (rejection posture); QoS 2 would need a record
        %% the S1 frame set does not have.
        QoS =< 1 -> p_publish(Dup, QoS, Retain, Body);
        true -> {error, {malformed_packet, {bad_publish_qos, QoS}}}
    end;
do_parse_body(4, 0, <<Pid:16>>) when Pid =/= 0 ->
    {ok, #puback{packet_id = Pid}};
do_parse_body(8, 2, Body) ->
    p_subscribe(Body);
do_parse_body(9, 0, <<Pid:16, Codes/binary>>) when Pid =/= 0 ->
    {ok, #suback{packet_id = Pid, return_codes = binary_to_list(Codes)}};
do_parse_body(10, 2, Body) ->
    p_unsubscribe(Body);
do_parse_body(11, 0, <<Pid:16>>) when Pid =/= 0 ->
    {ok, #unsuback{packet_id = Pid}};
do_parse_body(12, 0, <<>>) ->
    {ok, #ping{kind = req}};
do_parse_body(13, 0, <<>>) ->
    {ok, #ping{kind = resp}};
do_parse_body(14, 0, <<>>) ->
    {ok, #disconnect{}};
do_parse_body(Type, _Flags, _Body) ->
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
                WillQoS > 1 ->
                    %% §3.1.2.4: QoS 3 is reserved; S1 has no QoS 2 either.
                    {error, {unsupported_will_qos, WillQoS}};
                true ->
                    connect_tail(Rest, CSession, Will, WillQoS,
                                 WillRetain, UserPass, KeepAlive)
            end;
        {<<"MQTT">>, <<5, _/binary>>} ->
            {error, {unacceptable_protocol_version, 5}};
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

%% @doc Encode one frame. Field validation raises on programming errors —
%% nothing the conn builds should ever hit these arms, and a silent malformed
%% frame on the wire would be worse than a crash in the writer.
-spec encode(frame()) -> binary().
encode(#connack{session_present = SP, return_code = RC}) ->
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
encode(#publish{topic = Topic, payload = Payload} = F) ->
    case ok_topic(Topic) andalso is_binary(Payload) of
        true ->
            e_publish(F);
        false ->
            erlang:error({malformed_frame_out, publish})
    end;
encode(#puback{packet_id = Pid}) ->
    if
        is_integer(Pid), Pid >= 1, Pid =< 65535 ->
            fixed_header(4, 0, <<Pid:16>>);
        true ->
            erlang:error({malformed_frame_out, puback})
    end;
encode(#subscribe{packet_id = Pid, topics = Topics}) ->
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
encode(#suback{packet_id = Pid, return_codes = Codes}) ->
    if
        is_integer(Pid), Pid >= 1, Pid =< 65535,
        is_list(Codes), Codes =/= [] ->
            fixed_header(9, 0, <<Pid:16, (list_to_binary(Codes))/binary>>);
        true ->
            erlang:error({malformed_frame_out, suback})
    end;
encode(#unsubscribe{packet_id = Pid, topics = Topics}) ->
    if
        is_integer(Pid), Pid >= 1, Pid =< 65535,
        is_list(Topics), Topics =/= [] ->
            Body = list_to_binary([<<(len16(T))/binary, T/binary>>
                                   || T <- Topics]),
            fixed_header(10, 2, <<Pid:16, Body/binary>>);
        true ->
            erlang:error({malformed_frame_out, unsubscribe})
    end;
encode(#unsuback{packet_id = Pid}) ->
    if
        is_integer(Pid), Pid >= 1, Pid =< 65535 ->
            fixed_header(11, 0, <<Pid:16>>);
        true ->
            erlang:error({malformed_frame_out, unsuback})
    end;
encode(#disconnect{}) ->
    fixed_header(14, 0, <<>>);
encode(#ping{kind = req}) ->
    fixed_header(12, 0, <<>>);
encode(#ping{kind = resp}) ->
    fixed_header(13, 0, <<>>);
encode(#connect{client_id = ClientId, keep_alive = KA, clean_session = CS,
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
e_publish(#publish{dup = Dup, qos = 1, retain = Ret, topic = Topic,
                   packet_id = Pid, payload = Payload}) ->
    if
        is_integer(Pid), Pid >= 1, Pid =< 65535 ->
            Flags = (Dup bsl 3) bor 2 bor (Ret bsl 0),
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
