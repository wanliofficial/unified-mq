%% @doc MQTT-SN topic-id rules (S8-C Task 1).
%%
%% Two things are worth pinning here. First, the reserved ids: 0, 0xFFFE and
%% 0xFFFF have meanings the codec cannot express, so they must never be handed
%% out or accepted. Second, allocation stability -- a client that publishes to
%% one topic a thousand times must see the same id every time, which is the
%% whole point of the per-client map.
%%
%% The predefined table lives in app env, so its cases install a value and call
%% `predefined/0'. `init_per_testcase' snapshots the real value into Config and
%% `end_per_testcase' restores it, because reading the env at teardown would
%% capture what the case just wrote and leak configuration into every later
%% suite in the node.
-module(mqtt_sn_topics_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [predefined_empty_by_default,
     predefined_is_read_from_env,
     predefined_rejects_reserved_id,
     predefined_rejects_out_of_range_id,
     predefined_rejects_empty_topic,
     predefined_rejects_non_map,
     validate_zero_is_reserved,
     validate_top_two_are_reserved,
     validate_boundaries,
     validate_rejects_non_integers,
     short_accepts_exactly_two_bytes,
     short_rejects_other_lengths,
     assign_first_id_is_one,
     assign_is_stable_for_the_same_topic,
     assign_reuses_lowest_free_id,
     assign_respects_the_cap,
     assign_exhausts_cleanly,
     assign_rejects_empty_topic,
     topic_of_reverse_lookup,
     topic_of_unknown_id,
     max_id_is_fffd].

%% The predefined table is app env, which is node-global: snapshot it here,
%% let the case install whatever it wants, and put the snapshot back even if
%% the case fails. The snapshot has to travel through Config -- reading the env
%% during teardown would capture the value the case just wrote.
init_per_testcase(_Case, Config) ->
    Snapshot = application:get_env(broker_proto_mqtt_sn, predefined_topics),
    [{predefined_snapshot, Snapshot} | Config].

end_per_testcase(_Case, Config) ->
    case ?config(predefined_snapshot, Config) of
        undefined -> application:unset_env(broker_proto_mqtt_sn, predefined_topics);
        {ok, Value} -> application:set_env(broker_proto_mqtt_sn, predefined_topics, Value)
    end,
    ok.

%%%_* predefined/0 ----------------------------------------------------------

predefined_empty_by_default(_Config) ->
    ?assertEqual(#{}, mqtt_sn_topics:predefined()).

predefined_is_read_from_env(_Config) ->
    put_predefined(#{1 => <<"t0/a">>, 2 => <<"t0/b">>}),
    ?assertEqual(#{1 => <<"t0/a">>, 2 => <<"t0/b">>},
                 mqtt_sn_topics:predefined()).

predefined_rejects_reserved_id(_Config) ->
    lists:foreach(
      fun(Id) ->
              put_predefined(#{Id => <<"t">>}),
              ?assertError({invalid_predefined_topics, _},
                           mqtt_sn_topics:predefined())
      end, [0, 16#FFFE, 16#FFFF]).

predefined_rejects_out_of_range_id(_Config) ->
    put_predefined(#{16#10000 => <<"t">>}),
    ?assertError({invalid_predefined_topics, _}, mqtt_sn_topics:predefined()),
    put_predefined(#{-1 => <<"t">>}),
    ?assertError({invalid_predefined_topics, _}, mqtt_sn_topics:predefined()).

predefined_rejects_empty_topic(_Config) ->
    put_predefined(#{1 => <<>>}),
    ?assertError({invalid_predefined_topics, {empty_topic, 1}},
                 mqtt_sn_topics:predefined()).

predefined_rejects_non_map(_Config) ->
    put_predefined([{1, <<"t">>}]),
    ?assertError({invalid_predefined_topics, _}, mqtt_sn_topics:predefined()),
    put_predefined(#{1 => "not-a-binary"}),
    ?assertError({invalid_predefined_topics, _}, mqtt_sn_topics:predefined()).

%%%_* validate_topic_id/1 ---------------------------------------------------

validate_zero_is_reserved(_Config) ->
    ?assertEqual({error, reserved_topic_id}, mqtt_sn_topics:validate_topic_id(0)).

validate_top_two_are_reserved(_Config) ->
    ?assertEqual({error, reserved_topic_id}, mqtt_sn_topics:validate_topic_id(16#FFFE)),
    ?assertEqual({error, reserved_topic_id}, mqtt_sn_topics:validate_topic_id(16#FFFF)).

validate_boundaries(_Config) ->
    ?assertEqual(ok, mqtt_sn_topics:validate_topic_id(1)),
    ?assertEqual(ok, mqtt_sn_topics:validate_topic_id(16#FFFD)),
    ?assertEqual(16#FFFD, mqtt_sn_topics:max_id()).

validate_rejects_non_integers(_Config) ->
    ?assertEqual({error, invalid_topic_id}, mqtt_sn_topics:validate_topic_id(<<"1">>)),
    ?assertEqual({error, invalid_topic_id}, mqtt_sn_topics:validate_topic_id(-1)),
    ?assertEqual({error, invalid_topic_id}, mqtt_sn_topics:validate_topic_id(1.0)).

%%%_* short/1 ---------------------------------------------------------------

short_accepts_exactly_two_bytes(_Config) ->
    ?assertEqual({ok, <<"ab">>}, mqtt_sn_topics:short(<<"ab">>)),
    ?assertEqual({ok, <<0, 255>>}, mqtt_sn_topics:short(<<0, 255>>)).

short_rejects_other_lengths(_Config) ->
    ?assertEqual(error, mqtt_sn_topics:short(<<>>)),
    ?assertEqual(error, mqtt_sn_topics:short(<<"a">>)),
    ?assertEqual(error, mqtt_sn_topics:short(<<"abc">>)),
    ?assertEqual(error, mqtt_sn_topics:short(<<"t0/x">>)).

%%%_* assign/3 --------------------------------------------------------------

assign_first_id_is_one(_Config) ->
    ?assertEqual({ok, 1, #{<<"t0/a">> => 1}}, mqtt_sn_topics:assign(#{}, <<"t0/a">>, 10)).

assign_is_stable_for_the_same_topic(_Config) ->
    {ok, Id, Map} = mqtt_sn_topics:assign(#{}, <<"t0/a">>, 10),
    ?assertEqual({ok, Id, Map}, mqtt_sn_topics:assign(Map, <<"t0/a">>, 10)),
    %% Even when the cap is now "full", an existing topic still resolves.
    ?assertEqual({ok, Id, Map}, mqtt_sn_topics:assign(Map, <<"t0/a">>, 1)).

assign_reuses_lowest_free_id(_Config) ->
    {ok, 1, M1} = mqtt_sn_topics:assign(#{}, <<"a">>, 10),
    {ok, 2, M2} = mqtt_sn_topics:assign(M1, <<"b">>, 10),
    {ok, 3, M3} = mqtt_sn_topics:assign(M2, <<"c">>, 10),
    ?assertEqual(#{<<"a">> => 1, <<"b">> => 2, <<"c">> => 3}, M3),
    %% Removing the middle topic frees 2, and the next allocation takes it
    %% rather than continuing upward.
    M4 = maps:remove(<<"b">>, M3),
    ?assertEqual({ok, 2, #{<<"a">> => 1, <<"c">> => 3, <<"d">> => 2}},
                 mqtt_sn_topics:assign(M4, <<"d">>, 10)).

assign_respects_the_cap(_Config) ->
    ?assertEqual({ok, 1, #{<<"a">> => 1}}, mqtt_sn_topics:assign(#{}, <<"a">>, 1)),
    %% With the cap at 1 and id 1 taken, there is nowhere left to go.
    ?assertEqual({error, exhausted},
                 mqtt_sn_topics:assign(#{<<"a">> => 1}, <<"b">>, 1)).

assign_exhausts_cleanly(_Config) ->
    Full = maps:from_list([{integer_to_binary(Id), Id} || Id <- lists:seq(1, 3)]),
    ?assertEqual({error, exhausted}, mqtt_sn_topics:assign(Full, <<"new">>, 3)),
    %% A hole below the cap is still usable.
    ?assertMatch({ok, 2, _}, mqtt_sn_topics:assign(maps:remove(<<"2">>, Full),
                                                   <<"new">>, 3)).

assign_rejects_empty_topic(_Config) ->
    %% An empty topic has no encoding; assign/3 must not mint an id for it.
    ?assertEqual({error, empty_topic}, mqtt_sn_topics:assign(#{}, <<>>, 10)).

%%%_* topic_of/2 ------------------------------------------------------------

topic_of_reverse_lookup(_Config) ->
    Map = #{<<"a">> => 1, <<"b">> => 2},
    ?assertEqual({ok, <<"a">>}, mqtt_sn_topics:topic_of(Map, 1)),
    ?assertEqual({ok, <<"b">>}, mqtt_sn_topics:topic_of(Map, 2)).

topic_of_unknown_id(_Config) ->
    ?assertEqual(error, mqtt_sn_topics:topic_of(#{}, 1)),
    ?assertEqual(error, mqtt_sn_topics:topic_of(#{<<"a">> => 1}, 2)).

max_id_is_fffd(_Config) ->
    ?assertEqual(16#FFFD, mqtt_sn_topics:max_id()),
    %% The spec's maximum is a legal id, and a cap of 0xFFFD still starts at 1.
    ?assertEqual(ok, mqtt_sn_topics:validate_topic_id(mqtt_sn_topics:max_id())),
    ?assertMatch({ok, 1, _},
                 mqtt_sn_topics:assign(#{}, <<"t">>, mqtt_sn_topics:max_id())),
    ok.

%%%_* Helpers ---------------------------------------------------------------

put_predefined(Value) ->
    application:set_env(broker_proto_mqtt_sn, predefined_topics, Value).
