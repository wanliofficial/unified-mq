%% @doc Unit tests for the Observe relationship bookkeeping. Pure functions,
%% no datagrams: subscription policy, sequence wrap, and notification shaping.
-module(coap_observe_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("broker_core/include/broker_message.hrl").

all() ->
    [filter_is_the_destination,
     register_stores_sub_ref_clock_and_max_age,
     next_seq_wraps_at_2pow24,
     next_seq_stamps_the_notification_clock,
     seq_answers_the_initial_representation,
     sweep_window_is_clamped,
     expired_needs_the_window_to_elapse,
     notify_message_carries_sequence_token_and_code,
     notify_message_mirrors_content_format,
     deregister_returns_the_removed_relationship].

filter_is_the_destination(_) ->
    ?assertEqual(<<"a/b/c">>, coap_observe:filter(<<"a/b/c">>)).

register_stores_sub_ref_clock_and_max_age(_) ->
    SubRef = make_ref(),
    %% Negative on purpose: the clock is a caller argument, not a sentinel.
    Now = -100,
    R = coap_observe:register(<<"a/b">>, <<"t0">>, SubRef, 30, Now),
    ?assertEqual(0, coap_observe:seq(R)),
    ?assertEqual(<<"a/b">>, maps:get(dest, R)),
    ?assertEqual(<<"t0">>, maps:get(tenant, R)),
    ?assertEqual(SubRef, maps:get(sub_ref, R)),
    ?assertEqual(30, maps:get(max_age, R)),
    ?assertEqual(Now, maps:get(last_notified, R)).

next_seq_wraps_at_2pow24(_) ->
    R0 = coap_observe:register(<<"d">>, <<"t0">>, make_ref(), 30, 0),
    R1 = R0#{seq => 16#FFFFFF},
    {16#000000, R2} = coap_observe:next_seq(R1, 0),
    ?assertEqual(16#000000, maps:get(seq, R2)).

%% A quiet-but-live relationship must not look stale just because the caller
%% forgot to refresh the clock, so next_seq/2 stamps it.
next_seq_stamps_the_notification_clock(_) ->
    R = coap_observe:register(<<"d">>, <<"t0">>, make_ref(), 30, -100),
    {1, R1} = coap_observe:next_seq(R, -40),
    ?assertEqual(-40, maps:get(last_notified, R1)),
    ?assertNot(coap_observe:expired(R1, -40, 30)),
    ?assertNot(coap_observe:expired(R1, 29_959, 30)),
    ?assert(coap_observe:expired(R1, 29_960, 30)).

%% The initial piggybacked 2.05 answers with the registration's own sequence.
seq_answers_the_initial_representation(_) ->
    R = coap_observe:register(<<"d">>, <<"t0">>, make_ref(), 30, 0),
    ?assertEqual(0, coap_observe:seq(R)).

%% Max-Age: 0 cannot kill its own observation on the next tick; the configured
%% cap is a ceiling, not a floor.
sweep_window_is_clamped(_) ->
    ?assertEqual(1000, coap_observe:sweep_window_ms(0, 30)),
    ?assertEqual(30_000, coap_observe:sweep_window_ms(30, 30)),
    ?assertEqual(30_000, coap_observe:sweep_window_ms(604800, 30)),
    ?assertEqual(1000, coap_observe:sweep_window_ms(0, 1)),
    ?assertEqual(1000, coap_observe:sweep_window_ms(10, 1)).

expired_needs_the_window_to_elapse(_) ->
    R = coap_observe:register(<<"d">>, <<"t0">>, make_ref(), 30, 1000),
    ?assertNot(coap_observe:expired(R, 1000 + 29_999, 30)),
    ?assert(coap_observe:expired(R, 1000 + 30_000, 30)).

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
    ?assertEqual(3, coap_codec:option(observe, Con)),
    %% It has to survive the wire: the listener encodes this shape as-is.
    {ok, Round} = coap_codec:decode(coap_codec:encode(Con)),
    ?assertEqual(Token, coap_codec:token(Round)),
    ?assertEqual(3, coap_codec:option(observe, Round)).

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
