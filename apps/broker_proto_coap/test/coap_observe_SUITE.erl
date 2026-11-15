%% @doc Unit tests for the Observe relationship bookkeeping. Pure functions,
%% no datagrams: subscription policy, sequence wrap, and notification shaping.
-module(coap_observe_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("broker_core/include/broker_message.hrl").

all() ->
    [filter_is_the_destination,
     register_seeds_seq_zero,
     next_seq_wraps_at_2pow24,
     notify_message_carries_sequence_and_payload,
     deregister_removes_the_token].

filter_is_the_destination(_) ->
    ?assertEqual(<<"a/b/c">>, coap_observe:filter(<<"a/b/c">>)).

register_seeds_seq_zero(_) ->
    R = coap_observe:register(<<"a/b">>, <<"t0">>, 30),
    ?assertEqual(0, coap_observe:relationship_value(R)),
    ?assertEqual(<<"a/b">>, maps:get(dest, R)).

next_seq_wraps_at_2pow24(_) ->
    R0 = coap_observe:register(<<"d">>, <<"t0">>, 30),
    R1 = R0#{seq => 16#FFFFFF},
    {16#000000, R2} = coap_observe:next_seq(R1),
    ?assertEqual(16#000000, maps:get(seq, R2)).

notify_message_carries_sequence_and_payload(_) ->
    Msg = #broker_message{payload = <<"hello">>},
    Non = coap_observe:notify_message(non, 7, Msg),
    ?assertEqual(non, coap_codec:type(Non)),
    ?assertEqual(7, coap_codec:option(observe, Non)),
    ?assertEqual(<<"hello">>, coap_codec:payload(Non)),
    Con = coap_observe:notify_message(con, 3, Msg),
    ?assertEqual(con, coap_codec:type(Con)),
    ?assertEqual(3, coap_codec:option(observe, Con)).

deregister_removes_the_token(_) ->
    Key = {{127,0,0,1}, 5000, <<"tok">>},
    Table = maps:put(Key, coap_observe:register(<<"d">>, <<"t0">>, 30), #{}),
    ?assert(maps:is_key(Key, Table)),
    Table2 = coap_observe:deregister(Key, Table),
    ?assertNot(maps:is_key(Key, Table2)).

%%%_* Fixtures (standard shape; this suite needs no kernel app) -----------
init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.
init_per_testcase(_Case, Config) -> Config.
end_per_testcase(_Case, _Config) -> ok.
