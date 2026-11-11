%% @doc Event construction and codec (ADR-025 §D1).
-module(a2a_event_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("broker_core/include/broker_message.hrl").

all() ->
    [destination_roundtrip,
     event_is_a_durable_ordinary_message,
     event_id_is_stable_across_replay,
     encode_decode_roundtrip,
     decode_rejects_non_a2a_payloads,
     reliability_defaults_to_fsync].

destination_roundtrip(_Config) ->
    ?assertEqual(<<"$a2a/task/t-1">>, a2a_event:destination(<<"t-1">>)),
    ?assertEqual({ok, <<"t-1">>}, a2a_event:task_id(<<"$a2a/task/t-1">>)),
    %% Other destinations are not A2A events, including the bare prefix.
    ?assertEqual(error, a2a_event:task_id(<<"$a2a/task/">>)),
    ?assertEqual(error, a2a_event:task_id(<<"orders/1">>)).

event_is_a_durable_ordinary_message(_Config) ->
    Msg = a2a_event:event(<<"t-1">>, <<"t1">>, status_update, 2,
                          #{<<"statusUpdate">> => #{<<"taskId">> => <<"t-1">>}}),
    ?assertEqual(<<"t1">>, Msg#broker_message.tenant_id),
    ?assertEqual(<<"$a2a/task/t-1">>, Msg#broker_message.destination),
    ?assertEqual(json, Msg#broker_message.payload_encoding),
    ?assertEqual(durable, Msg#broker_message.persistence),
    ?assertEqual(fsync, Msg#broker_message.reliability),
    %% Payload is a binary and carries our additive metadata.
    ?assert(is_binary(Msg#broker_message.payload)),
    {ok, #{kind := status_update, seq := 2, body := Body}} =
        a2a_event:decode(Msg#broker_message.payload),
    ?assertMatch(#{<<"_meta">> := #{<<"unified-mq">> :=
                                        #{<<"kind">> := <<"statusUpdate">>,
                                          <<"seq">> := 2}}}, Body).

event_id_is_stable_across_replay(_Config) ->
    %% The same task/seq pair always mints the same id, so a replayed log and a
    %% live append are indistinguishable to a deduplicating consumer.
    A = a2a_event:event(<<"t-1">>, <<"t1">>, task, 1, #{<<"task">> => #{}}),
    B = a2a_event:event(<<"t-1">>, <<"t1">>, task, 1, #{<<"task">> => #{}}),
    ?assertEqual(A#broker_message.message_id, B#broker_message.message_id),
    C = a2a_event:event(<<"t-1">>, <<"t1">>, task, 2, #{<<"task">> => #{}}),
    ?assertNotEqual(A#broker_message.message_id, C#broker_message.message_id).

encode_decode_roundtrip(_Config) ->
    Payload = #{<<"task">> => #{<<"id">> => <<"t-1">>}},
    Encoded = a2a_event:encode(task, 1, Payload),
    ?assertEqual(<<"task">>, maps:get(<<"kind">>,
                                      maps:get(<<"unified-mq">>,
                                               maps:get(<<"_meta">>, Encoded)))),
    {ok, #{kind := task, seq := 1, body := Body}} =
        a2a_event:decode(iolist_to_binary(json:encode(Encoded))),
    ?assertEqual(<<"t-1">>, maps:get(<<"id">>, maps:get(<<"task">>, Body))).

decode_rejects_non_a2a_payloads(_Config) ->
    ?assertEqual({error, not_an_a2a_event},
                 a2a_event:decode(<<"{\"task\":{}}">>)),
    ?assertEqual({error, invalid_json}, a2a_event:decode(<<"not json">>)),
    ?assertEqual({error, not_json}, a2a_event:decode(#{})),
    %% An A2A payload with an unknown kind is rejected, not coerced.
    Bad = #{<<"_meta">> => #{<<"unified-mq">> =>
                                 #{<<"kind">> => <<"bogus">>, <<"seq">> => 1}}},
    ?assertEqual({error, {unknown_kind, <<"bogus">>}},
                 a2a_event:decode(iolist_to_binary(json:encode(Bad)))).

reliability_defaults_to_fsync(_Config) ->
    ?assertEqual(fsync, a2a_event:reliability()),
    ok = application:set_env(broker_proto_a2a, task_reliability, os_buffer),
    ?assertEqual(os_buffer, a2a_event:reliability()),
    ok = application:unset_env(broker_proto_a2a, task_reliability).
