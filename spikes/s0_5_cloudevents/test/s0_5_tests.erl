%% S0.5 correctness tests for the CloudEvents codec and validator.
%%
%% The benchmark tells us what validation costs. These tests tell us whether it
%% is CORRECT, which matters more: a fast validator that accepts malformed
%% events is worse than no validator at all.
-module(s0_5_tests).

-include_lib("eunit/include/eunit.hrl").

%% --- core attributes (README 3.2) ---

valid_event_test() ->
    Json = s0_5_codec:generate_json(2, 32),
    {ok, Event} = s0_5_codec:decode(Json),
    ?assertEqual(ok, s0_5_codec:validate(Event)).

missing_required_attribute_test() ->
    Event = maps:remove(<<"type">>,
                        s0_5_codec:event(<<"id1">>, <<"t">>, #{})),
    ?assertMatch({error, {invalid, {missing_required, _}}},
                 s0_5_codec:validate(Event)).

non_string_required_attribute_test() ->
    Event = s0_5_codec:event(<<"id1">>, <<"t">>, #{}),
    Broken = Event#{<<"id">> => 12345},
    ?assertMatch({error, {invalid, {non_string_required, _}}},
                 s0_5_codec:validate(Broken)).

%% --- data / data_base64 mutual exclusion (README 3.1) ---

data_and_data_base64_conflict_test() ->
    Event = (s0_5_codec:event(<<"id1">>, <<"t">>, #{}))#{
              <<"data_base64">> => <<"aGVsbG8=">>},
    ?assertEqual({error, {invalid, {data_and_data_base64}}},
                 s0_5_codec:validate(Event)).

%% --- time format ---

good_time_test() ->
    Event = (s0_5_codec:event(<<"id1">>, <<"t">>, #{}))#{
              <<"time">> => <<"2026-10-14T12:00:00Z">>},
    ?assertEqual(ok, s0_5_codec:validate(Event)).

bad_time_test() ->
    Event = (s0_5_codec:event(<<"id1">>, <<"t">>, #{}))#{
              <<"time">> => <<"not-a-timestamp">>},
    ?assertMatch({error, {invalid, {bad_time, _}}},
                 s0_5_codec:validate(Event)).

non_string_time_test() ->
    Event = (s0_5_codec:event(<<"id1">>, <<"t">>, #{}))#{<<"time">> => 12345},
    ?assertMatch({error, {invalid, {bad_time, _}}},
                 s0_5_codec:validate(Event)).

%% --- extension names (CloudEvents 1.0 requires lowercase alnum) ---

uppercase_extension_rejected_test() ->
    Event = (s0_5_codec:event(<<"id1">>, <<"t">>, #{}))#{<<"Ext1">> => <<"v">>},
    ?assertMatch({error, {invalid, {bad_extension_names, _}}},
                 s0_5_codec:validate(Event)).

lowercase_extension_accepted_test() ->
    Event = (s0_5_codec:event(<<"id1">>, <<"t">>, #{}))#{<<"ext1">> => <<"v">>},
    ?assertEqual(ok, s0_5_codec:validate(Event)).

%% --- codec round trip (README 3.3 Structured Mode) ---

structured_roundtrip_test() ->
    Json = s0_5_codec:generate_json(3, 64),
    {ok, Event} = s0_5_codec:decode(Json),
    Reencoded = s0_5_codec:encode_structured(Event),
    {ok, Again} = s0_5_codec:decode(Reencoded),
    ?assertEqual(Event, Again).

invalid_json_test() ->
    ?assertMatch({error, {invalid_json, _}},
                 s0_5_codec:decode(<<"{not json">>)).

non_object_json_test() ->
    ?assertMatch({error, {not_an_object, _}},
                 s0_5_codec:decode(<<"[1,2,3]">>)).