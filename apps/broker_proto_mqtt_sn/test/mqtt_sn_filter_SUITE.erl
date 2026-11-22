%% @doc Unit tests for `mqtt_sn_filter', the MQTT-SN gateway's topic-filter
%% validation and matching (MQTT 3.1.1 §4.7).
%%
%% The gateway re-implements the wildcard matcher on its own side of the app
%% boundary rather than depend on `broker_proto_mqtt' (design §13 L9), so it is
%% not covered by the mqtt suites that exercise `mqtt_filter'. The subtle
%% corners differ from `mqtt_filter' -- most notably `sport/#' matches the
%% parent `sport', and the error atoms are named per this module
%% (`multi_level_wildcard_not_last', `wildcard_in_level'). This suite pins
%% both.
-module(mqtt_sn_filter_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [validate_multi_level_wildcard_at_end,
     validate_multi_level_wildcard_must_be_last,
     validate_single_level_wildcard,
     validate_wildcard_inside_level_is_rejected,
     validate_empty_filter_is_rejected,
     validate_plain_topic_is_ok,
     validate_filter_too_long_is_rejected,
     topic_with_wildcard_is_rejected,
     topic_empty_is_rejected,
     topic_plain_is_ok,
     match_exact_topic,
     match_single_level_wildcard,
     match_multi_level_wildcard_tail,
     match_multi_level_wildcard_matches_parent,
     match_single_level_does_not_cross_slash,
     match_mismatched_length_is_false,
     match_leading_empty_level].

%%%_* validate_filter/1 ------------------------------------------------------

validate_multi_level_wildcard_at_end(_Config) ->
    ?assertEqual(ok, mqtt_sn_filter:validate_filter(<<"sport/tennis/#">>)),
    ?assertEqual(ok, mqtt_sn_filter:validate_filter(<<"#">>)),
    ?assertEqual(ok, mqtt_sn_filter:validate_filter(<<"/#">>)),
    ok.

validate_multi_level_wildcard_must_be_last(_Config) ->
    ?assertEqual({error, {multi_level_wildcard_not_last, <<"#/a">>}},
                 mqtt_sn_filter:validate_filter(<<"#/a">>)),
    ?assertEqual({error, {multi_level_wildcard_not_last, <<"a/#/b">>}},
                 mqtt_sn_filter:validate_filter(<<"a/#/b">>)),
    ok.

validate_single_level_wildcard(_Config) ->
    ?assertEqual(ok, mqtt_sn_filter:validate_filter(<<"+">>)),
    ?assertEqual(ok, mqtt_sn_filter:validate_filter(<<"a/+/b">>)),
    ?assertEqual(ok, mqtt_sn_filter:validate_filter(<<"+/+/#">>)),
    ok.

validate_wildcard_inside_level_is_rejected(_Config) ->
    ?assertEqual({error, {{wildcard_in_level, <<"a+">>}, <<"a+">>}},
                 mqtt_sn_filter:validate_filter(<<"a+">>)),
    ?assertEqual({error, {{wildcard_in_level, <<"b#">>}, <<"a/b#">>}},
                 mqtt_sn_filter:validate_filter(<<"a/b#">>)),
    ok.

validate_empty_filter_is_rejected(_Config) ->
    ?assertEqual({error, empty_filter}, mqtt_sn_filter:validate_filter(<<>>)),
    ok.

validate_plain_topic_is_ok(_Config) ->
    ?assertEqual(ok, mqtt_sn_filter:validate_filter(<<"sport/tennis/player1">>)),
    ?assertEqual(ok, mqtt_sn_filter:validate_filter(<<"/">>)),
    ok.

validate_filter_too_long_is_rejected(_Config) ->
    Long = binary:copy(<<"a">>, 65536),
    ?assertEqual({error, filter_too_long}, mqtt_sn_filter:validate_filter(Long)),
    ok.

%%%_* validate_topic/1 -------------------------------------------------------

topic_with_wildcard_is_rejected(_Config) ->
    ?assertEqual({error, {wildcard_in_topic, <<"a/#">>}},
                 mqtt_sn_filter:validate_topic(<<"a/#">>)),
    ?assertEqual({error, {wildcard_in_topic, <<"+">>}},
                 mqtt_sn_filter:validate_topic(<<"+">>)),
    ok.

topic_empty_is_rejected(_Config) ->
    ?assertEqual({error, empty_topic}, mqtt_sn_filter:validate_topic(<<>>)),
    ok.

topic_plain_is_ok(_Config) ->
    ?assertEqual(ok, mqtt_sn_filter:validate_topic(<<"sport/tennis">>)),
    ?assertEqual(ok, mqtt_sn_filter:validate_topic(<<"/">>)),
    ok.

%%%_* matches/2 --------------------------------------------------------------

match_exact_topic(_Config) ->
    ?assert(mqtt_sn_filter:matches(<<"a/b/c">>, <<"a/b/c">>)),
    ?assertNot(mqtt_sn_filter:matches(<<"a/b/c">>, <<"a/b/d">>)),
    ok.

match_single_level_wildcard(_Config) ->
    ?assert(mqtt_sn_filter:matches(<<"sport/tennis">>, <<"sport/+">>)),
    ?assert(mqtt_sn_filter:matches(<<"sport">>, <<"+">>)),
    ?assertNot(mqtt_sn_filter:matches(<<"sport/tennis/rank">>, <<"sport/+">>)),
    ok.

match_multi_level_wildcard_tail(_Config) ->
    ?assert(mqtt_sn_filter:matches(<<"sport/tennis/player1/ranking">>,
                                   <<"sport/tennis/player1/#">>)),
    ok.

%% `sport/#' matches the parent topic `sport' itself, not only its children --
%% the MQTT-SN matcher's distinguishing clause (mqtt_filter requires the
%% trailing level).
match_multi_level_wildcard_matches_parent(_Config) ->
    ?assert(mqtt_sn_filter:matches(<<"sport">>, <<"sport/#">>)),
    ?assert(mqtt_sn_filter:matches(<<"sport/tennis">>, <<"sport/#">>)),
    ok.

match_single_level_does_not_cross_slash(_Config) ->
    ?assertNot(mqtt_sn_filter:matches(<<"sport/tennis/player1">>, <<"sport/+">>)),
    ok.

match_mismatched_length_is_false(_Config) ->
    ?assertNot(mqtt_sn_filter:matches(<<"a/b">>, <<"a">>)),
    ?assertNot(mqtt_sn_filter:matches(<<"a">>, <<"a/b">>)),
    ok.

match_leading_empty_level(_Config) ->
    %% A leading `/` is an empty first level and matches `/+`'s `+'.
    ?assert(mqtt_sn_filter:matches(<<"/finance">>, <<"/+">>)),
    ?assert(mqtt_sn_filter:matches(<<"/finance">>, <<"+/+">>)),
    ok.
