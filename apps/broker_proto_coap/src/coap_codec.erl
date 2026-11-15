%% @doc CoAP wire codec (RFC 7252) plus the RFC 6690 link-format encoder.
%%
%% Hand-written on purpose (ADR-031 §D1): the build spike proved
%% `gotthardp/gen_coap' compiles on OTP 28 but its runtime is broken there
%% (`http_uri' x4 removed in OTP 23, `pg2' x6 removed in OTP 24, no client-side
%% Observe API), so vendoring it would mean maintaining a fork of an unversioned
%% master. The report is `docs/benchmarks/2026-11-15-s11a-gen-coap-spike.md'.
%% The surface below is the same size class as `stun_codec' (ADR-029 §D1, the
%% same reasoning) and `mqtt_sn_codec', and every rule has a test behind it.
%%
%% A decoded message is a map, not a record, because call sites read different
%% subsets of it and the option list is the interesting part:
%%
%% ```
%% #{type    => con | non | ack | rst,
%%   code    => {method, get | post | put | delete}
%%            | {response, Class :: 2..5, Detail :: 0..31}
%%            | empty,
%%   id      => 0..65535,          %% the 16-bit Message ID
%%   token   => <<_:0..64>>,
%%   options => [{Name, Value}],   %% one entry PER OCCURRENCE, wire order
%%   payload => binary()}
%% '''
%%
%% Options keep one entry per occurrence rather than being folded into a map:
%% `uri_path' legitimately repeats once per segment. Option numbers are
%% delta-encoded, so they are structurally monotonic ascending on the wire;
%% preserving the wire list makes `encode(decode(Bin))' byte-identical instead
%% of "semantically equal". Repeated options therefore look like
%% look like `[{uri_path, <<"a">>}, {uri_path, <<"b">>}]'; `uri_path/1' joins
%% them, which is what the resource layer actually wants.
%%
%% Values of the options this server interprets are decoded to typed terms
%% (`observe' -> integer, `block2' -> `{Num, More, Size}'), everything else stays
%% a binary, and unknown option numbers stay integers so they survive a round
%% trip instead of being silently dropped.
%%
%% Decoding is strict. CoAP runs on UDP, where a malformed datagram is one
%% packet among many rather than "a connection that will close soon", so the
%% caller needs a definite answer and a named reason: `{error, Reason}' means
%% drop that datagram (or answer 4.00) and carry on.
-module(coap_codec).

-export([decode/1, encode/1,
         request/3, request/4, response/3, empty/2, ack/1, reset/1, reject/1,
         type/1, code/1, id/1, token/1, payload/1,
         option/2, option/3, options/1, options/2, put_option/3, remove_option/2,
         uri_path/1, uri_query/1, destination/1,
         block/2, block_value/3,
         content_format_id/1, content_format_name/1,
         link_format/1]).

-export_type([message/0, msg_type/0, code/0, method/0, option_name/0]).

-type msg_type()   :: con | non | ack | rst.
-type method()     :: get | post | put | delete.
-type code()       :: {method, method()}
                    | {response, 2..5, 0..31}
                    | empty.
-type option_name() :: atom() | non_neg_integer().
-type message()    :: #{type := msg_type(),
                        code := code(),
                        id := 0..65535,
                        token := binary(),
                        options := [{option_name(), term()}],
                        payload := binary()}.

-define(VERSION, 1).

%% Option numbers (RFC 7252 §5.10, RFC 7641 §4, RFC 7959 §2).
-define(IF_MATCH, 1).
-define(URI_HOST, 3).
-define(ETAG, 4).
-define(IF_NONE_MATCH, 5).
-define(OBSERVE, 6).
-define(URI_PORT, 7).
-define(LOCATION_PATH, 8).
-define(URI_PATH, 11).
-define(CONTENT_FORMAT, 12).
-define(MAX_AGE, 14).
-define(URI_QUERY, 15).
-define(ACCEPT, 17).
-define(LOCATION_QUERY, 20).
-define(BLOCK2, 23).
-define(BLOCK1, 27).
-define(SIZE2, 28).
-define(PROXY_URI, 35).
-define(PROXY_SCHEME, 39).
-define(SIZE1, 60).

%% Codes that carry a meaning beyond their numeric value.
-define(CODE_EMPTY, 0).
-define(CODE_GET, 1).
-define(CODE_POST, 2).
-define(CODE_PUT, 3).
-define(CODE_DELETE, 4).

%% Options whose value is a `uint' (RFC 7252 §3.2). Kept as one list because
%% two places need it: typed decode and minimal-length encode.
-define(UINT_OPTIONS, [?OBSERVE, ?URI_PORT, ?CONTENT_FORMAT, ?MAX_AGE, ?ACCEPT,
                       ?SIZE2, ?SIZE1]).

%%%_* Decode ---------------------------------------------------------------

%% @doc Decode exactly one datagram.
%%
%% Trailing bytes are an error, never "the start of the next message": UDP
%% delivers one packet at a time, and the header has no length field.
-spec decode(binary()) -> {ok, message()} | {error, term()}.
decode(Bin) when is_binary(Bin), byte_size(Bin) < 4 ->
    {error, {too_short, byte_size(Bin)}};
decode(<<Ver:2, _:2, _:4, _/binary>>) when Ver =/= ?VERSION ->
    {error, {bad_version, Ver}};
decode(<<?VERSION:2, _Type:2, TKL:4, _/binary>>) when TKL > 8 ->
    %% RFC 7252 §3: TKL 9-15 are reserved and a message using them is malformed.
    {error, {bad_token_length, TKL}};
decode(<<?VERSION:2, Type:2, TKL:4, Code:8, Id:16, Rest/binary>>) ->
    case Rest of
        <<Token:TKL/binary, Tail/binary>> ->
            case decode_options(Tail, 0, []) of
                {ok, Options, Payload} ->
                    {ok, #{type => type_atom(Type),
                           code => decode_code(Code),
                           id => Id,
                           token => Token,
                           options => Options,
                           payload => Payload}};
                {error, Reason} ->
                    {error, Reason}
            end;
        _ ->
            {error, {truncated_token, TKL, byte_size(Rest)}}
    end.

decode_options(<<>>, _Num, Acc) ->
    {ok, lists:reverse(Acc), <<>>};
%% 0xFF is the payload marker; a marker with nothing behind it is a format
%% error (RFC 7252 §3, "a message with an empty payload must not have one").
decode_options(<<16#FF>>, _Num, _Acc) ->
    {error, empty_payload_marker};
decode_options(<<16#FF, Payload/binary>>, _Num, Acc) ->
    {ok, lists:reverse(Acc), Payload};
decode_options(<<Delta:4, Len:4, Rest/binary>>, Num, Acc) ->
    case extended(Delta, Rest) of
        {ok, DeltaVal, Rest1} ->
            case extended(Len, Rest1) of
                {ok, LenVal, Rest2} ->
                    Number = Num + DeltaVal,
                    case Rest2 of
                        <<Value:LenVal/binary, Rest3/binary>> ->
                            decode_options(Rest3, Number,
                                           [{option_name(Number),
                                             decode_value(Number, Value)} | Acc]);
                        _ ->
                            {error, {truncated_option, Number}}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% The 4-bit nibble is either the value itself, or an escape into 1 or 2 extra
%% bytes (RFC 7252 §3.1). 15 is reserved for the payload marker only.
extended(15, _Rest) ->
    {error, reserved_option_nibble};
extended(N, Rest) when N < 13 ->
    {ok, N, Rest};
extended(13, <<E:8, Rest/binary>>) ->
    {ok, E + 13, Rest};
extended(13, <<>>) ->
    {error, truncated_option_header};
extended(14, <<E:16, Rest/binary>>) ->
    {ok, E + 269, Rest};
extended(14, _) ->
    {error, truncated_option_header}.

type_atom(0) -> con;
type_atom(1) -> non;
type_atom(2) -> ack;
type_atom(3) -> rst.

decode_code(?CODE_EMPTY) -> empty;
decode_code(?CODE_GET) -> {method, get};
decode_code(?CODE_POST) -> {method, post};
decode_code(?CODE_PUT) -> {method, put};
decode_code(?CODE_DELETE) -> {method, delete};
decode_code(Code) -> {response, Code bsr 5, Code band 16#1F}.

option_name(?IF_MATCH) -> if_match;
option_name(?URI_HOST) -> uri_host;
option_name(?ETAG) -> etag;
option_name(?IF_NONE_MATCH) -> if_none_match;
option_name(?OBSERVE) -> observe;
option_name(?URI_PORT) -> uri_port;
option_name(?LOCATION_PATH) -> location_path;
option_name(?URI_PATH) -> uri_path;
option_name(?CONTENT_FORMAT) -> content_format;
option_name(?MAX_AGE) -> max_age;
option_name(?URI_QUERY) -> uri_query;
option_name(?ACCEPT) -> accept;
option_name(?LOCATION_QUERY) -> location_query;
option_name(?BLOCK2) -> block2;
option_name(?BLOCK1) -> block1;
option_name(?SIZE2) -> size2;
option_name(?PROXY_URI) -> proxy_uri;
option_name(?PROXY_SCHEME) -> proxy_scheme;
option_name(?SIZE1) -> size1;
option_name(N) -> N.

option_number(if_match) -> ?IF_MATCH;
option_number(uri_host) -> ?URI_HOST;
option_number(etag) -> ?ETAG;
option_number(if_none_match) -> ?IF_NONE_MATCH;
option_number(observe) -> ?OBSERVE;
option_number(uri_port) -> ?URI_PORT;
option_number(location_path) -> ?LOCATION_PATH;
option_number(uri_path) -> ?URI_PATH;
option_number(content_format) -> ?CONTENT_FORMAT;
option_number(max_age) -> ?MAX_AGE;
option_number(uri_query) -> ?URI_QUERY;
option_number(accept) -> ?ACCEPT;
option_number(location_query) -> ?LOCATION_QUERY;
option_number(block2) -> ?BLOCK2;
option_number(block1) -> ?BLOCK1;
option_number(size2) -> ?SIZE2;
option_number(proxy_uri) -> ?PROXY_URI;
option_number(proxy_scheme) -> ?PROXY_SCHEME;
option_number(size1) -> ?SIZE1;
option_number(N) when is_integer(N), N >= 0 -> N.

decode_value(Number, Value) when Number =:= ?BLOCK1; Number =:= ?BLOCK2 ->
    decode_block(Value);
decode_value(Number, Value) ->
    case lists:member(Number, ?UINT_OPTIONS) of
        true -> decode_uint(Value);
        false -> Value
    end.

%% Empty means 0, which is the canonical encoding of zero (RFC 7252 §3.2 keeps
%% uint options at their minimal length).
decode_uint(<<>>) -> 0;
decode_uint(Bin) -> binary:decode_unsigned(Bin).

%% Block option: 4/12-bit block number, 1-bit "more", 3-bit size exponent
%% (RFC 7959 §2.2). Decoded to a size in bytes so callers never shift bits.
decode_block(<<>>) ->
    {0, false, 16};
decode_block(Bin) when byte_size(Bin) =< 3 ->
    Int = binary:decode_unsigned(Bin),
    Szx = Int band 7,
    More = (Int bsr 3) band 1,
    Num = Int bsr 4,
    {Num, More =:= 1, 16 bsl Szx}.

%%%_* Encode ---------------------------------------------------------------

%% @doc Encode one message. Options are emitted in ascending number order, which
%% the spec requires and which is also how a repeated option (delta 0) keeps its
%% position among its siblings.
-spec encode(message()) -> binary().
encode(#{type := Type, code := Code, id := Id, token := Token} = Msg) ->
    Options = maps:get(options, Msg, []),
    Payload = maps:get(payload, Msg, <<>>),
    Header = <<?VERSION:2, (type_bits(Type)):2, (byte_size(Token)):4,
               (encode_code(Code)):8, Id:16>>,
    <<Header/binary, Token/binary,
      (encode_options(numbered(Options)))/binary,
      (payload_marker(Payload))/binary, Payload/binary>>.

type_bits(con) -> 0;
type_bits(non) -> 1;
type_bits(ack) -> 2;
type_bits(rst) -> 3.

encode_code(empty) -> ?CODE_EMPTY;
encode_code({method, get}) -> ?CODE_GET;
encode_code({method, post}) -> ?CODE_POST;
encode_code({method, put}) -> ?CODE_PUT;
encode_code({method, delete}) -> ?CODE_DELETE;
encode_code({response, Class, Detail}) when Class >= 2, Class =< 5,
                                            Detail >= 0, Detail =< 31 ->
    (Class bsl 5) bor Detail.

payload_marker(<<>>) -> <<>>;
payload_marker(_) -> <<16#FF>>.

%% Tag each option with its position so a stable sort keeps repeated options in
%% wire order regardless of how the caller listed them.
numbered(Options) ->
    Numbered = [{{option_number(Name), Pos}, encode_value(Name, Value)}
                || {Pos, {Name, Value}} <- lists:zip(lists:seq(1, length(Options)),
                                                     Options)],
    lists:map(fun({{Number, _Pos}, Value}) -> {Number, Value} end,
              lists:keysort(1, Numbered)).

encode_options(Options) ->
    encode_options(Options, 0, <<>>).

encode_options([], _Prev, Acc) ->
    Acc;
encode_options([{Number, Value} | Rest], Prev, Acc) ->
    Delta = Number - Prev,
    {DeltaNibble, DeltaExt} = nibble(Delta),
    {LenNibble, LenExt} = nibble(byte_size(Value)),
    encode_options(Rest, Number,
                   <<Acc/binary, DeltaNibble:4, LenNibble:4,
                     DeltaExt/binary, LenExt/binary, Value/binary>>).

nibble(N) when N < 13 -> {N, <<>>};
nibble(N) when N < 269 -> {13, <<(N - 13):8>>};
nibble(N) when N =< 65804 -> {14, <<(N - 269):16>>}.

encode_value(Name, Value) when Name =:= block1; Name =:= block2 ->
    encode_block(Value);
encode_value(Name, Value) when is_integer(Value) ->
    Number = option_number(Name),
    case lists:member(Number, ?UINT_OPTIONS) of
        true -> encode_uint(Value);
        false -> integer_to_binary(Value)
    end;
encode_value(_Name, Value) when is_binary(Value) ->
    Value.

encode_uint(0) -> <<>>;
encode_uint(N) when N > 0 ->
    Bin = binary:encode_unsigned(N),
    Bin.

encode_block({Num, More, Size}) ->
    Szx = szx(Size),
    Int = (Num bsl 4) bor (bool_bit(More) bsl 3) bor Szx,
    case Int of
        0 -> <<>>;
        _ -> binary:encode_unsigned(Int)
    end.

bool_bit(true) -> 1;
bool_bit(false) -> 0.

szx(16) -> 0;
szx(32) -> 1;
szx(64) -> 2;
szx(128) -> 3;
szx(256) -> 4;
szx(512) -> 5;
szx(1024) -> 6.

%%%_* Constructors ---------------------------------------------------------

%% @doc A request with an empty token and payload.
-spec request(msg_type(), method(), 0..65535) -> message().
request(Type, Method, Id) ->
    request(Type, Method, Id, []).

%% @doc A request carrying options. The token is set by the caller (or left
%% empty) with `put_option/3'; Message IDs are the transport layer's business.
-spec request(msg_type(), method(), 0..65535, [{option_name(), term()}]) ->
          message().
request(Type, Method, Id, Options) ->
    #{type => Type, code => {method, Method}, id => Id, token => <<>>,
      options => Options, payload => <<>>}.

%% @doc A piggybacked response: the type and Message ID are copied from the
%% request, exactly as RFC 7252 §5.2 requires for a response inside the ACK.
-spec response(msg_type(), {response, 2..5, 0..31}, binary()) -> message().
response(Type, Code, Payload) ->
    #{type => Type, code => Code, id => 0, token => <<>>,
      options => [], payload => Payload}.

%% @doc A message with no code: the empty ACK, the RST, and the 4.00 "reject a
%% malformed CON" case all use this shape.
-spec empty(msg_type(), 0..65535) -> message().
empty(Type, Id) ->
    #{type => Type, code => empty, id => Id, token => <<>>,
      options => [], payload => <<>>}.

%% @doc The empty ACK for a CON request or a CON response (RFC 7252 §4.2).
-spec ack(message()) -> message().
ack(#{id := Id}) ->
    empty(ack, Id).

%% @doc The RST for a CON message the recipient does not understand
%% (RFC 7252 §4.3).
-spec reset(message()) -> message().
reset(#{id := Id}) ->
    empty(rst, Id).

%% @doc The 4.00 answer to a CON whose datagram cannot be decoded. RFC 7252 §4.2
%% allows rejecting before the token is known, so the response carries no token.
-spec reject(0..65535) -> message().
reject(Id) ->
    #{type => ack, code => {response, 4, 0}, id => Id, token => <<>>,
      options => [], payload => <<>>}.

%%%_* Accessors ------------------------------------------------------------

-spec type(message()) -> msg_type().
type(#{type := Type}) -> Type.

-spec code(message()) -> code().
code(#{code := Code}) -> Code.

-spec id(message()) -> 0..65535.
id(#{id := Id}) -> Id.

-spec token(message()) -> binary().
token(#{token := Token}) -> Token.

-spec payload(message()) -> binary().
payload(#{payload := Payload}) -> Payload.

%% @doc First value of an option, or `undefined'.
-spec option(option_name(), message()) -> term() | undefined.
option(Name, Msg) ->
    Number = option_number(Name),
    case [V || {N, V} <- options_of(Msg), option_number(N) =:= Number] of
        [V | _] -> V;
        [] -> undefined
    end.

-spec option(option_name(), message(), term()) -> term().
option(Name, Msg, Default) ->
    case option(Name, Msg) of
        undefined -> Default;
        Value -> Value
    end.

%% @doc Every value of a repeated option, in wire order.
-spec options(option_name(), message()) -> [term()].
options(Name, Msg) ->
    Number = option_number(Name),
    [V || {N, V} <- options_of(Msg), option_number(N) =:= Number].

%% @doc Every option on the message as `{Name, Value}' pairs in wire order,
%% including repeated occurrences. This is the inverse of what `encode/1'
%% emits and what `decode/1' stores; accessors like `uri_path/1' and
%% `option/2' are thin selections over this list.
-spec options(message()) -> [{option_name(), term()}].
options(Msg) ->
    options_of(Msg).

%% @doc Replace every occurrence of an option with one value.
-spec put_option(option_name(), term(), message()) -> message().
put_option(Name, Value, Msg) ->
    Number = option_number(Name),
    Kept = [{N, V} || {N, V} <- options_of(Msg), option_number(N) =/= Number],
    Msg#{options => Kept ++ [{Name, Value}]}.

-spec remove_option(option_name(), message()) -> message().
remove_option(Name, Msg) ->
    Number = option_number(Name),
    Msg#{options => [{N, V} || {N, V} <- options_of(Msg),
                              option_number(N) =/= Number]}.

options_of(Msg) -> maps:get(options, Msg, []).

%% @doc The request path as segments, in order. This is what the resource layer
%% routes on: `coap://host/a/b' is `[<<"a">>, <<"b">>]'.
-spec uri_path(message()) -> [binary()].
uri_path(Msg) ->
    options(uri_path, Msg).

%% @doc The `Uri-Query' options, split on `&' so `?a=1&b=2' is
%% `[<<"a=1">>, <<"b=2">>]'.
-spec uri_query(message()) -> [binary()].
uri_query(Msg) ->
    lists:append([binary:split(Q, <<"&">>, [global]) || Q <- options(uri_query, Msg)]).

%% @doc The `broker_router' destination a request addresses (ADR-031 §D4): the
%% path segments joined with `/'. An empty path is the root resource, `<<>>'.
-spec destination(message()) -> binary().
destination(Msg) ->
    case uri_path(Msg) of
        [] -> <<>>;
        Segments -> join(Segments, <<"/">>)
    end.

join([], _Sep) -> <<>>;
join([One], _Sep) -> One;
join([Head | Rest], Sep) ->
    lists:foldl(fun(Seg, Acc) -> <<Acc/binary, Sep/binary, Seg/binary>> end,
                Head, Rest).

%%%_* Block helpers --------------------------------------------------------

%% @doc The block option value, or `undefined' when absent.
-spec block(block1 | block2, message()) -> {non_neg_integer(), boolean(),
                                          pos_integer()} | undefined.
block(Name, Msg) -> option(Name, Msg).

-spec block_value(non_neg_integer(), boolean(), pos_integer()) ->
          {non_neg_integer(), boolean(), pos_integer()}.
block_value(Num, More, Size) -> {Num, More, Size}.

%%%_* Content formats ------------------------------------------------------

%% @doc The handful of `Content-Format' registrations this adapter uses. The
%% registry is not reproduced wholesale: an unknown format keeps its number.
-spec content_format_id(atom() | {atom(), atom()}) -> non_neg_integer() | undefined.
content_format_id(text_plain) -> 0;
content_format_id(link_format) -> 40;
content_format_id(octet_stream) -> 42;
content_format_id(json) -> 50;
content_format_id(cbor) -> 60;
content_format_id({text, plain}) -> 0;
content_format_id({application, link_format}) -> 40;
content_format_id({application, octet_stream}) -> 42;
content_format_id({application, json}) -> 50;
content_format_id({application, cbor}) -> 60;
content_format_id(_) -> undefined.

-spec content_format_name(non_neg_integer()) ->
          atom() | {atom(), atom()} | undefined.
content_format_name(0) -> {text, plain};
content_format_name(40) -> {application, link_format};
content_format_name(42) -> {application, octet_stream};
content_format_name(50) -> {application, json};
content_format_name(60) -> {application, cbor};
content_format_name(_) -> undefined.

%%%_* Link format (RFC 6690) ----------------------------------------------

%% @doc Encode `.well-known/core' entries.
%%
%% Input is a list of `#{path := [binary()], attrs := [{Name, Value}]}' where
%% `Name' is an atom (`rt' | `if' | `ct' | `sz' | `title' | `obs' | `anchor')
%% and `Value' is a binary, an integer, or the atom `true' for a valueless
%% attribute such as `obs'. Output is the link-format document:
%%
%% ```
%% </a/b>;rt="x";ct=0;obs,</c>
%% '''
%%
%% Only encoding exists. This adapter *produces* its own `/.well-known/core';
%% it never parses someone else's, so a scanner/parser pair would be code with
%% no caller. The spike recorded the upstream `core_link' enc/dec as evidence
%% that the format is small, not as a reason to ship both directions.
-spec link_format([#{path := [binary()], attrs => [{atom(), term()}]}]) ->
          binary().
link_format(Entries) ->
    join([link_entry(Entry) || Entry <- Entries], <<",">>).

link_entry(#{path := Path} = Entry) ->
    Target = <<"<", (path_string(Path))/binary, ">">>,
    Attrs = [attribute(A) || A <- maps:get(attrs, Entry, [])],
    iolist_to_binary([Target | Attrs]).

path_string([]) -> <<"/">>;
path_string(Segments) ->
    Escaped = [escape(Seg) || Seg <- Segments],
    <<"/", (join(Escaped, <<"/">>))/binary>>.

attribute({Name, true}) ->
    [$;, atom_to_list(Name)];
attribute({Name, Value}) when is_integer(Value) ->
    [$;, atom_to_list(Name), $=, integer_to_list(Value)];
attribute({Name, Value}) when is_binary(Value) ->
    [$;, atom_to_list(Name), $=, $", escape(Value), $"].

%% Percent-encode the characters that would end the token or the quoted string
%% (RFC 6690 §2 requires escaping what the URI grammar reserves).
escape(Bin) ->
    << <<(escape_byte(B))/binary>> || <<B>> <= Bin >>.

escape_byte(B) when B =:= $"; B =:= $,; B =:= $;; B =:= $<; B =:= $>; B =:= $\s ->
    hex(B);
escape_byte(B) when B < 16#20; B >= 16#7F ->
    hex(B);
escape_byte(B) ->
    <<B>>.

hex(B) ->
    <<"%", (hex_digit(B bsr 4)), (hex_digit(B band 15))>>.

hex_digit(N) when N < 10 -> $0 + N;
hex_digit(N) -> $A + N - 10.
