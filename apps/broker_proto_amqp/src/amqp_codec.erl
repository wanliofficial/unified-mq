%% @doc AMQP 0-9-1 wire codec (S5).
%%
%% Only the subset the adapter uses is implemented: frames, method
%% headers, the three field-table value types real clients send in
%% `client-properties' (long string, boolean, nested table), and the
%% content-header body size. Property values on received messages are
%% skipped (the body arrives in its own frames), and deliveries are sent
%% with no properties, so a client sees `delivery_mode=0'.
-module(amqp_codec).

-export([decode_frame/1, encode_frame/3,
         encode_method/3, decode_method/1,
         get_shortstr/1, get_longstr/1, get_table/1, get_bits/2,
         encode_shortstr/1, encode_longstr/1, encode_table/1,
         encode_bits/1, encode_content_header/1, decode_content_header/1]).

-define(FRAME_END, 16#CE).

%%%_* Frames ---------------------------------------------------------------

decode_frame(<<Type:8, Channel:16, Size:32, Rest/binary>>)
  when byte_size(Rest) >= Size + 1 ->
    <<Payload:Size/binary, End:8, Tail/binary>> = Rest,
    case End of
        ?FRAME_END ->
            {ok, #{type => Type, channel => Channel, payload => Payload}, Tail};
        _ ->
            {error, bad_frame_end}
    end;
decode_frame(_) ->
    more.

encode_frame(Type, Channel, Payload) ->
    <<Type:8, Channel:16, (byte_size(Payload)):32, Payload/binary, ?FRAME_END:8>>.

%%%_* Methods --------------------------------------------------------------

encode_method(Class, Method, Args) ->
    <<Class:16, Method:16, Args/binary>>.

decode_method(<<Class:16, Method:16, Args/binary>>) ->
    {Class, Method, Args}.

%%%_* Field primitives -----------------------------------------------------

get_shortstr(<<Len:8, Str:Len/binary, Rest/binary>>) ->
    {Str, Rest}.

get_longstr(<<Len:32, Str:Len/binary, Rest/binary>>) ->
    {Str, Rest}.

encode_shortstr(Bin) when byte_size(Bin) =< 255 ->
    <<(byte_size(Bin)):8, Bin/binary>>.

encode_longstr(Bin) ->
    <<(byte_size(Bin)):32, Bin/binary>>.

%% Consecutive bit fields are packed LSB-first into one octet.
get_bits(<<Octet:8, Rest/binary>>, N) ->
    {[Octet band (1 bsl I) =/= 0 || I <- lists:seq(0, N - 1)], Rest}.

encode_bits(Bits) ->
    Octet = lists:foldl(
              fun({I, B}, Acc) -> Acc bor (bit(B) bsl I) end, 0,
              lists:zip(lists:seq(0, length(Bits) - 1), Bits)),
    <<Octet:8>>.

bit(true) -> 1;
bit(false) -> 0.

%%%_* Field tables ---------------------------------------------------------

get_table(<<Len:32, Body:Len/binary, Rest/binary>>) ->
    {decode_entries(Body, #{}), Rest}.

decode_entries(<<>>, Acc) ->
    Acc;
decode_entries(<<KeyLen:8, Key:KeyLen/binary, Type:8, Rest/binary>>, Acc) ->
    {Value, Rest1} = decode_value(Type, Rest),
    decode_entries(Rest1, Acc#{Key => Value}).

decode_value($S, Bin) ->
    get_longstr(Bin);
decode_value($t, <<0:8, Rest/binary>>) ->
    {false, Rest};
decode_value($t, <<_:8, Rest/binary>>) ->
    {true, Rest};
decode_value($I, <<V:32/signed, Rest/binary>>) ->
    {V, Rest};
decode_value($l, <<V:64/signed, Rest/binary>>) ->
    {V, Rest};
decode_value($F, Bin) ->
    get_table(Bin);
decode_value($V, Rest) ->
    {undefined, Rest};
decode_value(_Other, Rest) ->
    {undefined, Rest}.

encode_table(Map) when is_map(Map) ->
    Body = iolist_to_binary([entry(K, V) || {K, V} <- maps:to_list(Map)]),
    <<(byte_size(Body)):32, Body/binary>>;
encode_table(_) ->
    <<0:32>>.

entry(K, V) when is_binary(V) ->
    Key = iolist_to_binary(K),
    [<<(byte_size(Key)):8, Key/binary, $S:8>>, encode_longstr(V)];
entry(K, V) when is_boolean(V) ->
    Key = iolist_to_binary(K),
    [<<(byte_size(Key)):8, Key/binary, $t:8>>, <<(case V of true -> 1; false -> 0 end):8>>];
entry(K, V) when is_integer(V) ->
    Key = iolist_to_binary(K),
    [<<(byte_size(Key)):8, Key/binary, $I:8, V:32/signed>>];
entry(K, V) when is_map(V) ->
    Key = iolist_to_binary(K),
    [<<(byte_size(Key)):8, Key/binary, $F:8>>, encode_table(V)];
entry(K, _V) ->
    Key = iolist_to_binary(K),
    <<(byte_size(Key)):8, Key/binary, $V:8>>.

%%%_* Content header -------------------------------------------------------

%% class-id, weight, body-size, property-flags, properties...
decode_content_header(<<Class:16, Weight:16, BodySize:64, Rest/binary>>) ->
    {Class, Weight, BodySize, Rest}.

encode_content_header(BodySize) ->
    %% class 60 (basic), weight 0, no property flags.
    <<60:16, 0:16, BodySize:64, 0:16>>.
