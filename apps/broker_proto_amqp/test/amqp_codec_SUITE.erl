%% @doc S5 AMQP wire codec: frames, methods, field tables, bits and content
%% headers. Pure functions, no sockets; the connection behaviour lives in
%% `amqp_conn_SUITE' and the third-party-client evidence in
%% `test/protocol-compatibility/amqp/'.
-module(amqp_codec_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [frame_roundtrip,
     frame_needs_more,
     frame_bad_end,
     method_roundtrip,
     shortstr_roundtrip,
     shortstr_too_long,
     longstr_roundtrip,
     bits_lsb_first,
     bits_roundtrip,
     repeated_bits,
     table_roundtrip,
     table_nested,
     empty_table,
     content_header_roundtrip,
     properties_roundtrip_with_ce_headers].

suite() -> [{timetrap, {seconds, 20}}].

frame_roundtrip(_Config) ->
    Frame = amqp_codec:encode_frame(1, 7, <<"payload">>),
    {ok, #{type := 1, channel := 7, payload := <<"payload">>}, <<>>} =
        amqp_codec:decode_frame(Frame),
    ok.

frame_needs_more(_Config) ->
    Frame = amqp_codec:encode_frame(1, 1, <<"abc">>),
    Prefix = binary:part(Frame, 0, byte_size(Frame) - 1),
    ?assertEqual(more, amqp_codec:decode_frame(Prefix)),
    ok.

frame_bad_end(_Config) ->
    Frame = amqp_codec:encode_frame(1, 1, <<"abc">>),
    Bad = binary:replace(Frame, <<16#CE>>, <<16#FF>>, [{scope, {byte_size(Frame) - 1, 1}}]),
    ?assertEqual({error, bad_frame_end}, amqp_codec:decode_frame(Bad)).

method_roundtrip(_Config) ->
    Body = amqp_codec:encode_method(60, 40, <<"rk">>),
    {60, 40, <<"rk">>} = amqp_codec:decode_method(Body),
    ok.

shortstr_roundtrip(_Config) ->
    Bin = amqp_codec:encode_shortstr(<<"hello">>),
    {<<"hello">>, <<"rest">>} = amqp_codec:get_shortstr(<<Bin/binary, "rest">>),
    ok.

shortstr_too_long(_Config) ->
    ?assertError(function_clause, amqp_codec:encode_shortstr(binary:copy(<<"x">>, 256))),
    ok.

longstr_roundtrip(_Config) ->
    Bin = amqp_codec:encode_longstr(<<"a-long-string">>),
    {<<"a-long-string">>, <<>>} = amqp_codec:get_longstr(Bin),
    ok.

bits_lsb_first(_Config) ->
    Bin = amqp_codec:encode_bits([true, false, true]),
    ?assertEqual(<<5>>, Bin),
    {[true, false, true], <<>>} = amqp_codec:get_bits(Bin, 3),
    ok.

bits_roundtrip(_Config) ->
    Bits = [true, true, false, false, true, false, true, false],
    {Bits, <<>>} = amqp_codec:get_bits(amqp_codec:encode_bits(Bits), length(Bits)),
    ok.

repeated_bits(_Config) ->
    %% Position-based packing: repeated values must keep their own bit.
    ?assertEqual(<<3>>, amqp_codec:encode_bits([true, true])),
    ?assertEqual(<<0>>, amqp_codec:encode_bits([false, false])),
    ?assertEqual(<<4>>, amqp_codec:encode_bits([false, false, true])),
    ok.

table_roundtrip(_Config) ->
    Table = #{<<"product">> => <<"UnifiedMQ">>, <<"ok">> => true,
              <<"n">> => 42},
    {Decoded, <<>>} = amqp_codec:get_table(amqp_codec:encode_table(Table)),
    ?assertEqual(<<"UnifiedMQ">>, maps:get(<<"product">>, Decoded)),
    ?assertEqual(true, maps:get(<<"ok">>, Decoded)),
    ?assertEqual(42, maps:get(<<"n">>, Decoded)),
    ok.

table_nested(_Config) ->
    Table = #{<<"capabilities">> => #{<<"consumer_cancel_notify">> => true}},
    {#{<<"capabilities">> := Cap}, <<>>} =
        amqp_codec:get_table(amqp_codec:encode_table(Table)),
    ?assertEqual(#{<<"consumer_cancel_notify">> => true}, Cap),
    ok.

empty_table(_Config) ->
    ?assertEqual(#{}, begin {T, <<>>} = amqp_codec:get_table(<<0:32>>), T end),
    ok.

content_header_roundtrip(_Config) ->
    Bin = amqp_codec:encode_content_header(11),
    {60, 0, 11, <<0:16>>} = amqp_codec:decode_content_header(Bin),
    ok.

%% CloudEvents Binary Mode rides on the basic-properties `headers' table; the
%% adapter must encode and decode it losslessly (project binding §4.10).
properties_roundtrip_with_ce_headers(_Config) ->
    Headers = #{<<"ce-specversion">> => <<"1.0">>,
                <<"ce-id">> => <<"e1">>,
                <<"ce-source">> => <<"/s5">>,
                <<"ce-type">> => <<"demo">>,
                <<"text/plain">> => <<"not-a-ce-key">>},
    Props = #{content_type => <<"text/plain">>, headers => Headers,
              delivery_mode => 2, priority => 0},
    Bin = amqp_codec:encode_content_header(5, Props),
    {60, 0, 5, PropsBin} = amqp_codec:decode_content_header(Bin),
    {Decoded, <<>>} = amqp_codec:decode_properties(PropsBin),
    ?assertEqual(<<"text/plain">>, maps:get(content_type, Decoded)),
    ?assertEqual(Headers, maps:get(headers, Decoded)),
    ?assertEqual(2, maps:get(delivery_mode, Decoded)),
    ?assertEqual(0, maps:get(priority, Decoded)),
    %% No properties at all is still the compact form.
    <<60:16, 0:16, 0:64, 0:16>> = amqp_codec:encode_content_header(0, #{}),
    ok.
