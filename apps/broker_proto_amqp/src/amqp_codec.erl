%% @doc AMQP 0-9-1 wire codec (S5).
%%
%% Frames, method headers, the full 0-9-1 field-table type set, and the
%% content-header property block. `decode_value' knows the type of every
%% field it parses, so an entry can never silently misalign the byte stream:
%% an unknown type byte is a decode ERROR, which the conn turns into a
%% connection close (503) -- the same posture RabbitMQ takes for an
%% unparseable frame. Known limitation: a decoded byte array ($x) and a long
%% string ($S) are both Erlang binaries and re-encode as $S; a decoded
%% timestamp ($T) re-encodes as $l. Values survive, exact types may not. A
%% decimal ($D) keeps both its exponent and value (`{decimal, Exp, Val}') and
%% re-encodes as $D, so it is NOT lossy.
-module(amqp_codec).

-export([decode_frame/1, decode_frame/2, encode_frame/3,
         encode_method/3, decode_method/1,
         get_shortstr/1, get_longstr/1, get_table/1, get_bits/2,
         encode_shortstr/1, encode_longstr/1, encode_table/1,
         encode_bits/1, encode_content_header/1, encode_content_header/2,
         decode_content_header/1, decode_properties/1, encode_properties/1]).

-define(FRAME_END, 16#CE).
%% The spec's guaranteed-minimum peer frame size and this broker's ceiling;
%% `decode_frame/1' (used by tests and by callers with no negotiated state)
%% falls back to it. A connection passes its own negotiated value instead.
-define(DEFAULT_FRAME_MAX, 131072).

%%%_* Frames ---------------------------------------------------------------

%% Frames larger than the negotiated `frame_max' are refused BEFORE the
%% payload is buffered. Without this check a client could announce
%% `size=4GB' and make the connection accumulate bytes until the VM dies;
%% `more' is only correct while the size is still plausible.
decode_frame(Bin) ->
    decode_frame(Bin, ?DEFAULT_FRAME_MAX).

decode_frame(<<_Type:8, _Channel:16, Size:32, _Rest/binary>>, Max)
  when Size + 8 > Max ->
    %% Not `more': the frame can never become acceptable, so the caller must
    %% close with a frame error instead of waiting for bytes that may never
    %% stop arriving. `frame_max' bounds the whole frame (7 header octets +
    %% payload + the frame-end octet), so the payload ceiling is Max - 8 --
    %% the same arithmetic `send_body_frames/3' uses outbound.
    {error, {frame_too_large, Size, Max}};
decode_frame(<<Type:8, Channel:16, Size:32, Rest/binary>>, _Max)
  when byte_size(Rest) >= Size + 1 ->
    <<Payload:Size/binary, End:8, Tail/binary>> = Rest,
    case End of
        ?FRAME_END ->
            {ok, #{type => Type, channel => Channel, payload => Payload}, Tail};
        _ ->
            {error, bad_frame_end}
    end;
decode_frame(_, _Max) ->
    more.

encode_frame(Type, Channel, Payload) ->
    <<Type:8, Channel:16, (byte_size(Payload)):32, Payload/binary, ?FRAME_END:8>>.

%%%_* Methods --------------------------------------------------------------

encode_method(Class, Method, Args) ->
    <<Class:16, Method:16, Args/binary>>.

decode_method(<<Class:16, Method:16, Args/binary>>) ->
    {Class, Method, Args}.

%%%_* Field primitives -----------------------------------------------------

%% @doc A shortstr plus the rest. `{error, bad_shortstr}' when the length
%% prefix runs past the buffer -- a truncated frame is a decode error, never
%% a crash or a misaligned read.
get_shortstr(<<Len:8, Str:Len/binary, Rest/binary>>) ->
    {Str, Rest};
get_shortstr(_) ->
    {error, bad_shortstr}.

get_longstr(<<Len:32, Str:Len/binary, Rest/binary>>) ->
    {Str, Rest};
get_longstr(_) ->
    {error, bad_longstr}.

encode_shortstr(Bin) when byte_size(Bin) =< 255 ->
    <<(byte_size(Bin)):8, Bin/binary>>.

encode_longstr(Bin) ->
    <<(byte_size(Bin)):32, Bin/binary>>.

%% Consecutive bit fields are packed LSB-first into one octet.
get_bits(<<Octet:8, Rest/binary>>, N) ->
    {[Octet band (1 bsl I) =/= 0 || I <- lists:seq(0, N - 1)], Rest};
get_bits(_, _) ->
    {error, bad_bits}.

encode_bits(Bits) ->
    Octet = lists:foldl(
              fun({I, B}, Acc) -> Acc bor (bit(B) bsl I) end, 0,
              lists:zip(lists:seq(0, length(Bits) - 1), Bits)),
    <<Octet:8>>.

bit(true) -> 1;
bit(false) -> 0.

%%%_* Field tables ---------------------------------------------------------

%% @doc A field table plus the rest, or `{error, Reason}' for a table that
%% cannot be parsed exactly (truncated entry, unknown type byte).
get_table(<<Len:32, Body:Len/binary, Rest/binary>>) ->
    case decode_entries(Body, #{}) of
        {ok, Table} -> {Table, Rest};
        {error, Reason} -> {error, Reason}
    end;
get_table(_) ->
    {error, bad_table}.

decode_entries(<<>>, Acc) ->
    {ok, Acc};
decode_entries(<<KeyLen:8, Key:KeyLen/binary, Type:8, Rest/binary>>, Acc) ->
    case decode_value(Type, Rest) of
        {ok, Value, Rest1} ->
            decode_entries(Rest1, Acc#{Key => Value});
        {error, Reason} ->
            {error, Reason}
    end;
decode_entries(_, _) ->
    {error, bad_table_entry}.

%% The 0-9-1 field-table types. Each arm consumes exactly the bytes its type
%% describes; anything else is a hard error rather than a guess (RQ-04: an
%% unknown type used to return without consuming the value bytes, corrupting
%% every key that followed).
decode_value($t, <<V:8, Rest/binary>>) ->
    {ok, V =/= 0, Rest};
decode_value($b, <<V:8/signed, Rest/binary>>) ->
    {ok, V, Rest};
decode_value($B, <<V:8, Rest/binary>>) ->
    {ok, V, Rest};
decode_value($s, <<V:16/signed, Rest/binary>>) ->
    {ok, V, Rest};
decode_value($u, <<V:16, Rest/binary>>) ->
    {ok, V, Rest};
decode_value($I, <<V:32/signed, Rest/binary>>) ->
    {ok, V, Rest};
decode_value($i, <<V:32, Rest/binary>>) ->
    {ok, V, Rest};
decode_value($l, <<V:64/signed, Rest/binary>>) ->
    {ok, V, Rest};
decode_value($f, <<V:32/float, Rest/binary>>) ->
    {ok, V, Rest};
decode_value($d, <<V:64/float, Rest/binary>>) ->
    {ok, V, Rest};
decode_value($D, <<Exp:8, Val:32/signed, Rest/binary>>) ->
    {ok, {decimal, Exp, Val}, Rest};
decode_value($S, Bin) ->
    case get_longstr(Bin) of
        {error, Reason} -> {error, Reason};
        {V, Rest} -> {ok, V, Rest}
    end;
decode_value($x, <<Len:32, V:Len/binary, Rest/binary>>) ->
    {ok, V, Rest};
decode_value($A, <<Len:32, Body:Len/binary, Rest/binary>>) ->
    case decode_array(Body, []) of
        {ok, Items} -> {ok, Items, Rest};
        {error, Reason} -> {error, Reason}
    end;
decode_value($T, <<V:64, Rest/binary>>) ->
    {ok, V, Rest};
decode_value($F, Bin) ->
    case get_table(Bin) of
        {error, Reason} -> {error, Reason};
        {Table, Rest} -> {ok, Table, Rest}
    end;
decode_value($V, Rest) ->
    {ok, undefined, Rest};
decode_value(Type, _Rest) ->
    {error, {bad_field_type, Type}}.

decode_array(<<>>, Acc) ->
    {ok, lists:reverse(Acc)};
decode_array(<<Type:8, Rest/binary>>, Acc) ->
    case decode_value(Type, Rest) of
        {ok, Value, Rest1} -> decode_array(Rest1, [Value | Acc]);
        {error, Reason} -> {error, Reason}
    end;
decode_array(_, _) ->
    {error, bad_array_entry}.

encode_table(Map) when is_map(Map) ->
    Body = iolist_to_binary([entry(K, V) || {K, V} <- maps:to_list(Map)]),
    <<(byte_size(Body)):32, Body/binary>>;
encode_table(_) ->
    <<0:32>>.

%% Round-trip the terms `decode_value' produces. Integers encode as $I inside
%% the signed-32 window and as $l outside it (RQ-04: a 64-bit value used to
%% crash the encoder with badarg).
entry(K, V) when is_binary(V) ->
    Key = iolist_to_binary(K),
    [<<(byte_size(Key)):8, Key/binary, $S:8>>, encode_longstr(V)];
entry(K, V) when is_boolean(V) ->
    Key = iolist_to_binary(K),
    [<<(byte_size(Key)):8, Key/binary, $t:8>>, <<(case V of true -> 1; false -> 0 end):8>>];
entry(K, V) when is_integer(V), V >= -16#80000000, V =< 16#7FFFFFFF ->
    Key = iolist_to_binary(K),
    [<<(byte_size(Key)):8, Key/binary, $I:8, V:32/signed>>];
entry(K, V) when is_integer(V) ->
    Key = iolist_to_binary(K),
    [<<(byte_size(Key)):8, Key/binary, $l:8, V:64/signed>>];
entry(K, V) when is_float(V) ->
    Key = iolist_to_binary(K),
    [<<(byte_size(Key)):8, Key/binary, $d:8, V:64/float>>];
entry(K, V) when is_list(V) ->
    Key = iolist_to_binary(K),
    Body = iolist_to_binary([array_entry(I) || I <- V]),
    [<<(byte_size(Key)):8, Key/binary, $A:8>>,
     <<(byte_size(Body)):32>>, Body];
entry(K, V) when is_map(V) ->
    Key = iolist_to_binary(K),
    [<<(byte_size(Key)):8, Key/binary, $F:8>>, encode_table(V)];
entry(K, {decimal, Exp, Val}) when is_integer(Exp), Exp >= 0, Exp =< 255,
                                   is_integer(Val) ->
    %% $D round-trips `decode_value/2's own output. Without this clause the
    %% tuple fell through to the $V arm and the value vanished, which the
    %% amqp_props round trip (an inbound header re-encoded on delivery) would
    %% turn into real data loss.
    Key = iolist_to_binary(K),
    [<<(byte_size(Key)):8, Key/binary, $D:8, Exp:8, Val:32/signed>>];
entry(K, _V) ->
    %% $V: the void arm keeps unknown terms from crashing the writer.
    Key = iolist_to_binary(K),
    <<(byte_size(Key)):8, Key/binary, $V:8>>.

array_entry(I) ->
    {Type, Body} = array_value(I),
    <<Type:8, Body/binary>>.

array_value(V) when is_binary(V) -> {$S, encode_longstr(V)};
array_value(V) when is_boolean(V) -> {$t, <<(case V of true -> 1; false -> 0 end):8>>};
array_value(V) when is_integer(V), V >= -16#80000000, V =< 16#7FFFFFFF ->
    {$I, <<V:32/signed>>};
array_value(V) when is_integer(V) -> {$l, <<V:64/signed>>};
array_value(V) when is_float(V) -> {$d, <<V:64/float>>};
array_value(V) when is_map(V) -> {$F, encode_table(V)};
array_value({decimal, Exp, Val}) when is_integer(Exp), Exp >= 0, Exp =< 255,
                                      is_integer(Val) ->
    {$D, <<Exp:8, Val:32/signed>>};
array_value(_) -> {$V, <<>>}.

%%%_* Content header -------------------------------------------------------

%% class-id, weight, body-size, property-flags, properties...
decode_content_header(<<Class:16, Weight:16, BodySize:64, Rest/binary>>) ->
    {Class, Weight, BodySize, Rest};
decode_content_header(_) ->
    {error, bad_content_header}.

encode_content_header(BodySize) ->
    %% class 60 (basic), weight 0, no property flags.
    <<60:16, 0:16, BodySize:64, 0:16>>.

%% @doc Content header that carries basic properties. `Props' is a map keyed by
%% `content_type' / `headers' / `delivery_mode' / ...; only present keys are
%% flagged, everything absent costs nothing (the protocol's own encoding).
encode_content_header(BodySize, Props) when map_size(Props) =:= 0 ->
    encode_content_header(BodySize);
encode_content_header(BodySize, Props) ->
    PropsBin = encode_properties(Props),
    <<60:16, 0:16, BodySize:64, PropsBin/binary>>.

%% Basic-properties field order and flag bits (AMQP 0-9-1 §4.2.6.1).
properties() ->
    [{15, content_type, shortstr},
     {14, content_encoding, shortstr},
     {13, headers, table},
     {12, delivery_mode, octet},
     {11, priority, octet},
     {10, correlation_id, shortstr},
     {9, reply_to, shortstr},
     {8, expiration, shortstr},
     {7, message_id, shortstr},
     {6, timestamp, longlong},
     {5, type, shortstr},
     {4, user_id, shortstr},
     {3, app_id, shortstr},
     {2, cluster_id, shortstr}].

%% @doc Decode the basic-properties block that follows `BodySize' in a content
%% header frame. Returns a map and the bytes after the block.
-spec decode_properties(binary()) -> {map(), binary()}.
decode_properties(<<Flags:16, Rest0/binary>>) ->
    lists:foldl(
      fun({Bit, Name, Type}, {Acc, Bin}) ->
              case Flags band (1 bsl Bit) of
                  0 -> {Acc, Bin};
                  _ ->
                      {Value, Rest} = decode_prop(Type, Bin),
                      {Acc#{Name => Value}, Rest}
              end
      end, {#{}, Rest0}, properties());
decode_properties(_) ->
    {#{}, <<>>}.

encode_properties(Props) when is_map(Props) ->
    {Flags, Parts} =
        lists:foldl(
          fun({Bit, Name, Type}, {F, Acc}) ->
                  case maps:find(Name, Props) of
                      error -> {F, Acc};
                      {ok, Value} -> {F bor (1 bsl Bit),
                                      [encode_prop(Type, Value) | Acc]}
                  end
          end, {0, []}, properties()),
    <<Flags:16, (iolist_to_binary(lists:reverse(Parts)))/binary>>.

decode_prop(shortstr, Bin) -> get_shortstr(Bin);
%% A property table that cannot be parsed exactly is a frame error, which the
%% conn's frame guard turns into a 503 connection close.
decode_prop(table, Bin) ->
    case get_table(Bin) of
        {error, Reason} -> erlang:error({bad_property_table, Reason});
        {T, Rest} -> {T, Rest}
    end;
decode_prop(octet, <<V:8, Rest/binary>>) -> {V, Rest};
decode_prop(octet, _) -> erlang:error(bad_property_octet);
decode_prop(longlong, <<V:64/signed, Rest/binary>>) -> {V, Rest};
decode_prop(longlong, _) -> erlang:error(bad_property_longlong).

encode_prop(shortstr, V) -> encode_shortstr(V);
encode_prop(table, V) -> encode_table(V);
encode_prop(octet, V) -> <<V:8>>;
encode_prop(longlong, V) -> <<V:64/signed>>.
