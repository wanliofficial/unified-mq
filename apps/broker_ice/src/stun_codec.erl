%% @doc STUN codec (RFC 8489) plus the attribute set TURN needs (RFC 5766).
%%
%% Hand-written on purpose (ADR-029 §D1): the `stun' hex package drags
%% `fast_tls' + `p1_utils' and its value is concentrated in the 5349/TLS path,
%% which this app deliberately does not implement. The surface here is small
%% and every rule below is a spec rule with a test behind it.
%%
%% A decoded message is a map, not a record, because the interesting part is
%% the attribute list and every TURN call site reads a different subset:
%%
%% ```
%% #{method => binding | allocate | refresh | send | data |
%%             create_permission | channel_bind | OtherInteger,
%%   class  => request | indication | success | error,
%%   type   => 16#0001,              %% the 14-bit wire type, kept verbatim
%%   transaction_id => <<_:96>>,
%%   attributes => [{Type :: 16#0000..16#FFFF, Value :: binary()}]}
%% '''
%%
%% Attributes stay raw `{Type, Value}' pairs so an unknown attribute survives a
%% decode/encode round trip instead of being silently dropped. Typed helpers
%% (`xor_address/3', `lifetime/1', ...) build and read the ones we interpret.
%%
%% Message-integrity is two-step by construction, because a short-term/long-term
%% credential HMAC is computed over the message *with the header length already
%% counting the MESSAGE-INTEGRITY attribute* that is not in the buffer yet:
%%
%% ```
%% Wire0 = stun_codec:encode(Msg),            %% no integrity, no fingerprint
%% Wire1 = stun_codec:add_integrity(Key, Wire0),
%% Wire2 = stun_codec:add_fingerprint(Wire1).
%% '''
%%
%% `verify_integrity/2' and `check_fingerprint/1' take the full wire message and
%% re-derive the same patched prefix, so a client that appends anything after
%% FINGERPRINT is not accidentally trusted.
-module(stun_codec).

-export([decode/1, encode/1, method/1, class/1, encode_type/2, method_atom/1,
         attribute/2, attribute/3, attributes/2, attribute_type/1,
         comprehended/1, unknown_required/1,
         xor_address/3, xor_mapped_address/3, decode_xor_address/1,
         requested_transport/1, lifetime/1, error_code/2, decode_error_code/1,
         channel_number/1, add_integrity/2, verify_integrity/2,
         add_fingerprint/1, check_fingerprint/1,
         integrity_offset/1]).

-export_type([message/0, attr_type/0]).

-type attr_type() :: 0..16#FFFF.
%% `type' is *optional*: `decode/1' fills it in with the raw 14-bit wire type,
%% but `encode/1' derives it from `method' + `class', so a caller building a
%% message must not have to invent it.
-type message() :: #{method := atom() | non_neg_integer(),
                     class := request | indication | success | error,
                     type => non_neg_integer(),
                     transaction_id := <<_:96>>,
                     attributes := [{attr_type(), binary()}]}.

-define(MAGIC_COOKIE, 16#2112A442).
-define(HEADER_SIZE, 20).

%% Attribute types (RFC 8489 §14, RFC 5766 §14).
-define(ATTR_MAPPED_ADDRESS, 16#0001).
-define(ATTR_USERNAME, 16#0006).
-define(ATTR_MESSAGE_INTEGRITY, 16#0008).
-define(ATTR_ERROR_CODE, 16#0009).
-define(ATTR_CHANNEL_NUMBER, 16#000C).
-define(ATTR_LIFETIME, 16#000D).
-define(ATTR_XOR_PEER_ADDRESS, 16#0012).
-define(ATTR_DATA, 16#0013).
-define(ATTR_REALM, 16#0014).
-define(ATTR_NONCE, 16#0015).
-define(ATTR_XOR_RELAYED_ADDRESS, 16#0016).
-define(ATTR_REQUESTED_TRANSPORT, 16#0019).
-define(ATTR_XOR_MAPPED_ADDRESS, 16#0020).
-define(ATTR_FINGERPRINT, 16#8028).

-define(METHOD_BINDING, 16#001).
-define(METHOD_ALLOCATE, 16#003).
-define(METHOD_REFRESH, 16#004).
-define(METHOD_SEND, 16#006).
-define(METHOD_DATA, 16#007).
-define(METHOD_CREATE_PERMISSION, 16#008).
-define(METHOD_CHANNEL_BIND, 16#009).

-define(CLASS_REQUEST, 0).
-define(CLASS_INDICATION, 1).
-define(CLASS_SUCCESS, 2).
-define(CLASS_ERROR, 3).

%% @doc Decode exactly one datagram.
%%
%% Strict by design: UDP hands over one packet at a time, so a message whose
%% length field disagrees with the datagram is malformed rather than "the start
%% of the next message". Every rejection is a named reason the caller can drop
%% on -- and, in the TURN case, answer with 400 where the spec asks for it.
-spec decode(binary()) -> {ok, message()} | {error, term()}.
decode(<<_:2, _:14, _:16, _:32, _:96, _/binary>> = Bin) ->
    <<0:2, Type:14, Len:16, Cookie:32, Tid:12/binary, Body/binary>> = Bin,
    case Cookie of
        ?MAGIC_COOKIE -> decode_body(Type, Len, Tid, Body);
        _ -> {error, {bad_magic_cookie, Cookie}}
    end;
decode(Bin) when is_binary(Bin) ->
    {error, {too_short, byte_size(Bin)}}.

decode_body(Type, Len, Tid, Body) ->
    case {Len rem 4, byte_size(Body) - Len} of
        {0, 0} -> decode_attributes(Type, Tid, Body, []);
        {0, _} -> {error, {length_mismatch, Len, byte_size(Body)}};
        _ -> {error, {length_not_word_aligned, Len}}
    end.

decode_attributes(Type, Tid, <<>>, Acc) ->
    {ok, #{method => method_atom(Type), class => class(Type), type => Type,
           transaction_id => Tid, attributes => lists:reverse(Acc)}};
decode_attributes(Type, Tid, <<AttrType:16, Len:16, Rest/binary>>, Acc) ->
    Padded = pad4(Len),
    case Rest of
        <<Value:Len/binary, _Pad:Padded/binary, Tail/binary>> ->
            decode_attributes(Type, Tid, Tail, [{AttrType, Value} | Acc]);
        _ ->
            {error, {truncated_attribute, AttrType, Len, byte_size(Rest)}}
    end;
decode_attributes(_Type, _Tid, Trailing, _Acc) ->
    {error, {truncated_attribute_header, byte_size(Trailing)}}.

%% @doc Encode a message without MESSAGE-INTEGRITY / FINGERPRINT.
-spec encode(message()) -> binary().
encode(#{method := Method, class := Class, transaction_id := Tid,
         attributes := Attrs}) when is_binary(Tid), byte_size(Tid) =:= 12 ->
    Type = encode_type(method_code(Method), class_code(Class)),
    Body = iolist_to_binary([attr_bin(A) || A <- Attrs]),
    <<0:2, Type:14, (byte_size(Body)):16, ?MAGIC_COOKIE:32, Tid/binary,
      Body/binary>>.

attr_bin({Type, Value}) when is_integer(Type), Type >= 0, Type =< 16#FFFF,
                             is_binary(Value) ->
    Pad = binary:copy(<<0>>, pad4(byte_size(Value))),
    <<Type:16, (byte_size(Value)):16, Value/binary, Pad/binary>>;
attr_bin({Type, Value}) ->
    error({bad_attribute, {Type, Value}}).

pad4(N) -> (4 - (N rem 4)) rem 4.

%% @doc Map the 14-bit wire type to a method number.
-spec method(non_neg_integer()) -> non_neg_integer().
method(Type) when is_integer(Type) ->
    (Type band 16#000F)
        bor ((Type band 16#00E0) bsr 1)
        bor ((Type band 16#3E00) bsr 2).

%% @doc Map the 14-bit wire type to a class atom.
-spec class(non_neg_integer()) -> request | indication | success | error.
class(Type) when is_integer(Type) ->
    case ((Type band 16#0010) bsr 4) bor ((Type band 16#0100) bsr 7) of
        ?CLASS_REQUEST -> request;
        ?CLASS_INDICATION -> indication;
        ?CLASS_SUCCESS -> success;
        ?CLASS_ERROR -> error
    end.

%% @doc Build a 14-bit type from a method and a class (RFC 8489 §5).
-spec encode_type(non_neg_integer(), non_neg_integer()) -> non_neg_integer().
encode_type(Method, Class) when is_integer(Method), is_integer(Class) ->
    (Method band 16#000F)
        bor ((Method band 16#0070) bsl 1)
        bor ((Method band 16#0F80) bsl 2)
        bor ((Class band 16#01) bsl 4)
        bor ((Class band 16#02) bsl 7).

%% @doc The method atom for a 14-bit wire type, or the integer when we have no
%% name for it (a request with an unknown method must still be answerable 400).
-spec method_atom(non_neg_integer()) -> atom() | non_neg_integer().
method_atom(Type) ->
    case method(Type) of
        ?METHOD_BINDING -> binding;
        ?METHOD_ALLOCATE -> allocate;
        ?METHOD_REFRESH -> refresh;
        ?METHOD_SEND -> send;
        ?METHOD_DATA -> data;
        ?METHOD_CREATE_PERMISSION -> create_permission;
        ?METHOD_CHANNEL_BIND -> channel_bind;
        Other -> Other
    end.

%% @doc Attribute type number for a name this codec comprehends. Call sites use
%% this instead of hard-coding the numbers a second time.
-spec attribute_type(atom()) -> attr_type().
attribute_type(mapped_address) -> ?ATTR_MAPPED_ADDRESS;
attribute_type(username) -> ?ATTR_USERNAME;
attribute_type(message_integrity) -> ?ATTR_MESSAGE_INTEGRITY;
attribute_type(error_code) -> ?ATTR_ERROR_CODE;
attribute_type(unknown_attributes) -> 16#000A;
attribute_type(channel_number) -> ?ATTR_CHANNEL_NUMBER;
attribute_type(lifetime) -> ?ATTR_LIFETIME;
attribute_type(xor_peer_address) -> ?ATTR_XOR_PEER_ADDRESS;
attribute_type(data) -> ?ATTR_DATA;
attribute_type(realm) -> ?ATTR_REALM;
attribute_type(nonce) -> ?ATTR_NONCE;
attribute_type(xor_relayed_address) -> ?ATTR_XOR_RELAYED_ADDRESS;
attribute_type(xor_mapped_address) -> ?ATTR_XOR_MAPPED_ADDRESS;
attribute_type(requested_transport) -> ?ATTR_REQUESTED_TRANSPORT;
attribute_type(software) -> 16#8022;
attribute_type(fingerprint) -> ?ATTR_FINGERPRINT.

%% @doc The attribute types this codec comprehends (RFC 8489 §14 / RFC 5766
%% §14). A request carrying any other comprehension-required attribute
%% (type < 16#8000) must be refused with 420 and UNKNOWN-ATTRIBUTES.
-spec comprehended(attr_type()) -> boolean().
comprehended(Type) ->
    lists:member(Type, [?ATTR_MAPPED_ADDRESS, ?ATTR_USERNAME,
                        ?ATTR_MESSAGE_INTEGRITY, ?ATTR_ERROR_CODE,
                        16#000A, ?ATTR_CHANNEL_NUMBER, ?ATTR_LIFETIME,
                        ?ATTR_XOR_PEER_ADDRESS, ?ATTR_DATA, ?ATTR_REALM,
                        ?ATTR_NONCE, ?ATTR_XOR_RELAYED_ADDRESS,
                        ?ATTR_XOR_MAPPED_ADDRESS, ?ATTR_REQUESTED_TRANSPORT,
                        16#8022, ?ATTR_FINGERPRINT]).

%% @doc Unknown comprehension-required attributes, in wire order.
-spec unknown_required([{attr_type(), binary()}]) -> [attr_type()].
unknown_required(Attrs) ->
    [T || {T, _} <- Attrs, T < 16#8000, not comprehended(T)].

method_code(binding) -> ?METHOD_BINDING;
method_code(allocate) -> ?METHOD_ALLOCATE;
method_code(refresh) -> ?METHOD_REFRESH;
method_code(send) -> ?METHOD_SEND;
method_code(data) -> ?METHOD_DATA;
method_code(create_permission) -> ?METHOD_CREATE_PERMISSION;
method_code(channel_bind) -> ?METHOD_CHANNEL_BIND;
method_code(Other) when is_integer(Other) -> Other.

class_code(request) -> ?CLASS_REQUEST;
class_code(indication) -> ?CLASS_INDICATION;
class_code(success) -> ?CLASS_SUCCESS;
class_code(error) -> ?CLASS_ERROR.

%% @doc First attribute of a type, or `undefined'.
-spec attribute([{attr_type(), binary()}], attr_type()) -> binary() | undefined.
attribute(Attrs, Type) ->
    case lists:keyfind(Type, 1, Attrs) of
        {_, Value} -> Value;
        false -> undefined
    end.

%% @doc Replace-or-append: STUN attributes are unique per type in every message
%% this app emits, and a duplicate would make the receiver's choice arbitrary.
-spec attribute([{attr_type(), binary()}], attr_type(), binary()) ->
          [{attr_type(), binary()}].
attribute(Attrs, Type, Value) ->
    [{Type, Value} | lists:keydelete(Type, 1, Attrs)].

%% @doc Every value of a type, in wire order (repeatable attributes only --
%% UNKNOWN-ATTRIBUTES is the one STUN attribute that legitimately repeats).
-spec attributes([{attr_type(), binary()}], attr_type()) -> [binary()].
attributes(Attrs, Type) ->
    [V || {T, V} <- Attrs, T =:= Type].

%% @doc XOR-MAPPED-ADDRESS (RFC 8489 §14.2) for the client's own address.
-spec xor_mapped_address(inet:ip_address(), inet:port_number(), <<_:96>>) ->
          binary().
xor_mapped_address(Ip, Port, Tid) ->
    xor_address(Ip, Port, Tid).

%% @doc XOR-*-ADDRESS attribute value: reserved byte, family, XOR'd port, XOR'd
%% address. The XOR key is the magic cookie followed by the transaction id, so
%% an answer is bound to its request rather than being replayable verbatim.
-spec xor_address(inet:ip_address(), inet:port_number(), <<_:96>>) -> binary().
xor_address({A, B, C, D}, Port, <<_Tid:96>>) when is_integer(Port) ->
    Addr = (A bsl 24) bor (B bsl 16) bor (C bsl 8) bor D,
    <<0, 16#01, (Port bxor 16#2112):16, (Addr bxor ?MAGIC_COOKIE):32>>;
xor_address({_, _, _, _, _, _, _, _}, _Port, _Tid) ->
    %% IPv6 is explicitly out of scope (R-I6): refuse rather than emit a value
    %% with the wrong family byte that a client would have to guess about.
    erlang:error(ipv6_not_supported);
xor_address(Ip, _Port, _Tid) ->
    erlang:error({bad_ip, Ip}).

%% @doc Read an XOR-*-ADDRESS attribute value.
-spec decode_xor_address(binary()) ->
          {ok, {inet:ip_address(), inet:port_number()}} | {error, term()}.
decode_xor_address(<<0, 16#01, XPort:16, XAddr:32>>) ->
    Cookie = ?MAGIC_COOKIE,
    Port = XPort bxor 16#2112,
    Addr = XAddr bxor Cookie,
    {ok, {{Addr bsr 24, (Addr bsr 16) band 16#FF,
           (Addr bsr 8) band 16#FF, Addr band 16#FF}, Port}};
decode_xor_address(<<0, Family, _, _/binary>>) ->
    {error, {unsupported_family, Family}};
decode_xor_address(Other) ->
    {error, {bad_xor_address, Other}}.

%% @doc REQUESTED-TRANSPORT value. Only UDP (17) is implemented (ADR-029 §D4);
%% the value is returned verbatim so the caller can answer 442 itself.
-spec requested_transport(binary()) -> non_neg_integer() | undefined.
requested_transport(<<Protocol, _/binary>>) -> Protocol;
requested_transport(_) -> undefined.

%% @doc LIFETIME value (seconds), big endian.
-spec lifetime(binary()) -> non_neg_integer() | undefined.
lifetime(<<Seconds:32>>) -> Seconds;
lifetime(_) -> undefined.

%% @doc ERROR-CODE value: 21 reserved bits, 3-bit class, 8-bit number, reason.
%% The value itself is not NUL-padded -- the attribute framing in `attr_bin/1'
%% supplies the 4-byte alignment padding, and padding it here too would put
%% bytes inside the reason phrase.
-spec error_code(1..6, non_neg_integer()) -> binary().
error_code(Class, Number) when Class >= 3, Class =< 6, Number >= 0, Number =< 255 ->
    Reason = reason_phrase(Class * 100 + Number),
    <<0:21, Class:3, Number:8, Reason/binary>>.

%% @doc Read an ERROR-CODE attribute value.
-spec decode_error_code(binary()) -> {ok, non_neg_integer(), binary()} |
                                    {error, term()}.
decode_error_code(<<0:21, Class:3, Number:8, Reason/binary>>) ->
    Code = Class * 100 + Number,
    {ok, Code, trim_nul(Reason)};
decode_error_code(Other) ->
    {error, {bad_error_code, Other}}.

trim_nul(<<>>) ->
    <<>>;
trim_nul(Bin) ->
    case binary:last(Bin) of
        0 -> trim_nul(binary:part(Bin, 0, byte_size(Bin) - 1));
        _ -> Bin
    end.

reason_phrase(400) -> <<"Bad Request">>;
reason_phrase(401) -> <<"Unauthorized">>;
reason_phrase(403) -> <<"Forbidden">>;
reason_phrase(420) -> <<"Unknown Attribute">>;
reason_phrase(437) -> <<"Allocation Mismatch">>;
reason_phrase(438) -> <<"Stale Nonce">>;
reason_phrase(440) -> <<"Address Family not Supported">>;
reason_phrase(441) -> <<"Wrong Credentials">>;
reason_phrase(442) -> <<"Unsupported Transport Protocol">>;
reason_phrase(443) -> <<"Peer Address Family Mismatch">>;
reason_phrase(486) -> <<"Allocation Quota Reached">>;
reason_phrase(508) -> <<"Insufficient Capacity">>;
reason_phrase(_) -> <<"Error">>.

%% @doc CHANNEL-NUMBER value: 16-bit channel + 16 reserved bits.
-spec channel_number(non_neg_integer()) -> binary().
channel_number(Channel) when is_integer(Channel),
                             Channel >= 16#4000, Channel =< 16#7FFF ->
    <<Channel:16, 0:16>>.

%% @doc Byte offset of the MESSAGE-INTEGRITY attribute's type field, if present.
%%
%% Needed because the HMAC covers the wire prefix with a *patched* length, so
%% verification has to slice at the attribute boundary -- decode/1 deliberately
%% hands back values only and loses offsets.
-spec integrity_offset(binary()) -> {ok, non_neg_integer()} | not_found | error.
integrity_offset(<<_:16, _Len:16, _/binary>> = Bin) ->
    integrity_offset(Bin, ?HEADER_SIZE);
integrity_offset(_) ->
    error.

integrity_offset(Bin, Offset) when Offset + 4 =< byte_size(Bin) ->
    case Bin of
        <<_:Offset/binary, Type:16, Len:16, _/binary>> ->
            case Type of
                ?ATTR_MESSAGE_INTEGRITY ->
                    {ok, Offset};
                _ ->
                    %% Skip the attribute *and* its value, then its 4-byte
                    %% padding: an attribute total is 4 + Len + pad4(Len).
                    integrity_offset(Bin, Offset + 4 + Len + pad4(Len))
            end;
        _ ->
            error
    end;
integrity_offset(_Bin, _Offset) ->
    not_found.

%% @doc Append MESSAGE-INTEGRITY, computing the HMAC over the message with the
%% header length already counting the new attribute (RFC 8489 §14.5).
-spec add_integrity(binary(), binary()) -> binary().
add_integrity(Key, <<_:2, Type:14, _Len:16, Rest/binary>> = Bin)
  when is_binary(Key) ->
    NewLen = (byte_size(Bin) - ?HEADER_SIZE) + 24,
    Patched = <<0:2, Type:14, NewLen:16, Rest/binary>>,
    Digest = crypto:mac(hmac, sha, Key, Patched),
    <<Patched/binary, ?ATTR_MESSAGE_INTEGRITY:16, 20:16, Digest/binary>>.

%% @doc Verify MESSAGE-INTEGRITY against `Key'.
%%
%% Returns `false' -- not an error -- for "no such attribute", so a caller that
%% is about to answer 401 does not have to distinguish the two cases.
-spec verify_integrity(binary(), binary()) -> boolean().
verify_integrity(Key, Wire) when is_binary(Key), is_binary(Wire) ->
    case integrity_offset(Wire) of
        {ok, Offset} ->
            <<_:Offset/binary, _Type:16, Len:16, Value:Len/binary, _/binary>> = Wire,
            Prefix = binary:part(Wire, 0, Offset),
            %% RFC 8489 §14.5: the length covers the message up to and
            %% *including* MESSAGE-INTEGRITY, so it is the prefix length plus
            %% that attribute's own 4-byte header and 20-byte digest.
            PatchLen = (Offset - ?HEADER_SIZE) + 4 + Len,
            <<0:2, Type:14, _OldLen:16, Rest/binary>> = Prefix,
            Patched = <<0:2, Type:14, PatchLen:16, Rest/binary>>,
            Digest = crypto:mac(hmac, sha, Key, Patched),
            constant_time_equal(Digest, Value);
        _ ->
            false
    end.

%% @doc Append FINGERPRINT (CRC-32 XOR 0x5354554E), RFC 8489 §14.7.
-spec add_fingerprint(binary()) -> binary().
add_fingerprint(<<_:2, Type:14, _Len:16, Rest/binary>> = Bin) ->
    NewLen = (byte_size(Bin) - ?HEADER_SIZE) + 8,
    Patched = <<0:2, Type:14, NewLen:16, Rest/binary>>,
    Crc = erlang:crc32(Patched) bxor 16#5354554E,
    <<Patched/binary, ?ATTR_FINGERPRINT:16, 4:16, Crc:32>>.

%% @doc Check FINGERPRINT. Absent is `false', never a crash.
%%
%% FINGERPRINT must be the last attribute (RFC 8489 §14.7: it is computed over
%% everything before it, so anything appended afterwards would be unauthenticated
%% by design). Requiring `Offset + 8 =:= byte_size(Wire)' is what makes that
%% explicit instead of trusting whatever follows.
-spec check_fingerprint(binary()) -> boolean().
check_fingerprint(<<_:2, Type:14, _Len:16, _/binary>> = Wire) ->
    case fingerprint_offset(Wire, ?HEADER_SIZE) of
        {ok, Offset} when Offset + 8 =:= byte_size(Wire) ->
            <<_:Offset/binary, _T:16, 4:16, Crc:32>> = Wire,
            Prefix = binary:part(Wire, 0, Offset),
            PatchLen = (Offset - ?HEADER_SIZE) + 8,
            <<0:2, _:14, _OldLen:16, Rest/binary>> = Prefix,
            Patched = <<0:2, Type:14, PatchLen:16, Rest/binary>>,
            (erlang:crc32(Patched) bxor 16#5354554E) =:= Crc;
        _ ->
            false
    end;
check_fingerprint(_) ->
    false.

fingerprint_offset(Bin, Offset) when Offset + 4 =< byte_size(Bin) ->
    case Bin of
        <<_:Offset/binary, Type:16, Len:16, _/binary>> ->
            case Type of
                ?ATTR_FINGERPRINT ->
                    {ok, Offset};
                _ ->
                    fingerprint_offset(Bin, Offset + 4 + Len + pad4(Len))
            end;
        _ -> error
    end;
fingerprint_offset(_Bin, _Offset) ->
    not_found.

%% Comparing two 20-byte digests: `=:=' is fine here because both sides are
%% fixed-length binaries derived from a secret that the attacker does not know
%% the length of. Kept as a function so the intent is explicit and so the
%% verification path has exactly one comparison to audit.
constant_time_equal(A, B) when byte_size(A) =:= byte_size(B) ->
    crypto:exor(A, B) =:= binary:copy(<<0>>, byte_size(A));
constant_time_equal(_, _) ->
    false.
