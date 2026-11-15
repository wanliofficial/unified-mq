%% @doc Unit tests for the Observe relationship bookkeeping. Pure functions,
%% no datagrams: subscription policy, sequence wrap, and notification shaping.
-module(coap_observe_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("broker_core/include/broker_message.hrl").

all() ->
    [filter_is_the_destination,
     register_stores_sub_ref_and_clock,
     next_seq_wraps_at_2pow24,
     notify_message_carries_sequence_token_and_code,
     notify_message_mirrors_content_format,
     deregister_returns_the_removed_relationship].

filter_is_the_destination(_) ->
    ?assertEqual(<<"a/b/c">>, coap_observe:filter(<<"a/b/c">>)).

register_stores_sub_ref_and_clock(_) ->
    SubRef = make_ref(),
    %% Negative on purpose: the clock is a caller argument, not a sentinel.
    Now = -100,
    R = coap_observe:register(<<"a/b">>, <<"t0">>, SubRef, 30, Now),
    ?assertEqual(0, coap_observe:seq(R)),
    ?assertEqual(<<"a/b">>, maps:get(dest, R)),
    ?assertEqual(SubRef, maps:get(sub_ref, R)),
    ?assertEqual(-100, maps:get(last_notified, R)).

next_seq_wraps_at_2pow24(_) ->
    R0 = coap_observe:register(<<"d">>, <<"t0">>, make_ref(), 30, 0),
    R1 = R0#{seq => 16#FFFFFF},
    {16#000000, R2} = coap_observe:next_seq(R1),
    ?assertEqual(16#000000, maps:get(seq, R2)).

notify_message_carries_sequence_token_and_code(_) ->
    Token = <<"tok">>,
    Msg = #broker_message{payload = <<"hello">>},
    Non = coap_observe:notify_message(non, 7, Token, Msg),
    ?assertEqual(non, coap_codec:type(Non)),
    ?assertEqual({response, 2, 5}, coap_codec:code(Non)),
    ?assertEqual(Token, coap_codec:token(Non)),
    ?assertEqual(7, coap_codec:option(observe, Non)),
    ?assertEqual(<<"hello">>, coap_codec:payload(Non)),
    ?assertEqual(undefined, coap_codec:option(content_format, Non)),
    Con = coap_observe:notify_message(con, 3, Token, Msg),
    ?assertEqual(con, coap_codec:type(Con)),
    ?assertEqual(3, coap_codec:option(observe, Con)).

notify_message_mirrors_content_format(_) ->
    Msg = #broker_message{payload = <<"{}">>, proto_meta = #{content_format => 50}},
    Note = coap_observe:notify_message(non, 1, <<"tok">>, Msg),
    ?assertEqual(50, coap_codec:option(content_format, Note)),
    ?assertEqual(1, coap_codec:option(observe, Note)).

deregister_returns_the_removed_relationship(_) ->
    Key = {{127,0,0,1}, 5000, <<"tok">>},
    R = coap_observe:register(<<"d">>, <<"t0">>, make_ref(), 30, -100),
    Table = maps:put(Key, R, #{}),
    {Removed, Table2} = coap_observe:deregister(Key, Table),
    ?assertEqual(R, Removed),
    ?assertNot(maps:is_key(Key, Table2)),
    {undefined, Table3} = coap_observe:deregister(Key, Table2),
    ?assertEqual(#{}, Table3).

%%%_* Fixtures (standard shape; this suite needs no kernel app) -----------
init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.
init_per_testcase(_Case, Config) -> Config.
end_per_testcase(_Case, _Config) -> ok.
