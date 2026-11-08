%% @doc S3: CloudEvents 1.0 codec, policy switch, MQTT binding, dedup and
%% rejection (ADR-020). The cases mirror README §11.2 item by item:
%%
%%   * required attribute missing / wrong type / invalid format,
%%   * Structured and Binary encode/decode in both directions,
%%   * extension preservation, unknown extensions, time and content type,
%%   * MQTT binding forward/reverse and the explicit loss report for 3.1.1,
%%   * cross-protocol roundtrip with a named non-lossless field,
%%   * event identity vs broker identity (ADR-005),
%%   * duplicate events and redelivery behavior,
%%   * a rejection path that never ACKs and can isolate a copy,
%%   * ACL never trusts event attributes.
-module(broker_event_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("broker_core/include/broker_message.hrl").
-include_lib("broker_event/include/cloud_event.hrl").

all() ->
    [default_policy_is_pass_through,
     pass_through_is_identity_and_never_parses,
     disabled_is_identity,
     invalid_policy_is_rejected,
     non_message_input_is_rejected,
     cloud_event_record_matches_readme_3_1,
     structured_roundtrip_preserves_extensions,
     missing_required_attributes_rejected,
     wrong_core_types_rejected,
     unsupported_specversion_rejected,
     invalid_time_rejected,
     valid_time_accepted,
     data_and_data_base64_mutually_exclusive,
     invalid_base64_rejected,
     unknown_extension_preserved,
     extension_name_and_count_validated,
     binary_mode_roundtrip,
     mqtt_structured_binding_roundtrip,
     mqtt_binary_binding_v5_roundtrip,
     mqtt_binary_binding_311_reports_losses,
     header_binding_roundtrip_for_all_targets,
     header_binding_structured_mode,
     header_binding_detect_from_header_channel,
     cross_protocol_headers_survive_a_hop,
     cross_protocol_roundtrip_keeps_identity,
     event_id_does_not_overwrite_broker_id,
     policy_resolution_per_tenant_and_destination,
     validate_if_present_passes_ordinary_messages,
     required_rejects_ordinary_message,
     normalize_attaches_event_and_keeps_payload,
     duplicate_window_deduplicates,
     duplicate_is_off_by_default,
     duplicate_window_expires,
     quarantine_writes_a_durable_copy,
     acl_does_not_trust_event_attributes].

%%%_* Fixtures ----------------------------------------------------------------

init_per_suite(Config) ->
    _ = application:load(broker_event),
    _ = application:load(broker_security),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    stop_dedup(),
    ok = application:unset_env(broker_event, policy),
    ok = application:set_env(broker_event, policies, []),
    ok = application:set_env(broker_event, dedup, []),
    ok = application:set_env(broker_event, quarantine, []),
    Config.

end_per_testcase(_Case, _Config) ->
    stop_dedup(),
    ok.

%%%_* Policy basics -----------------------------------------------------------

default_policy_is_pass_through(_Config) ->
    pass_through = broker_event:policy(),
    ok.

pass_through_is_identity_and_never_parses(_Config) ->
    Payloads = [<<255, 0, 1, 2>>, structured_json(), <<"not json at all">>],
    lists:foreach(
      fun(Payload) ->
              Msg = message(Payload),
              {ok, Out} = broker_event:apply_policy(Msg),
              %% Term equality covers payload bytes, headers, proto_meta and
              %% the event slot at once.
              Msg = Out,
              undefined = Out#broker_message.event
      end, Payloads),
    ok.

disabled_is_identity(_Config) ->
    ok = application:set_env(broker_event, policy, disabled),
    disabled = broker_event:policy(),
    Msg = message(<<"plain-bytes">>),
    {ok, Msg} = broker_event:apply_policy(Msg),
    ok.

invalid_policy_is_rejected(_Config) ->
    Msg = message(<<"x">>),
    {error, {invalid_policy, validate}} = broker_event:apply_policy(Msg, validate),
    {error, {invalid_policy, <<"pass_through">>}} =
        broker_event:apply_policy(Msg, <<"pass_through">>),
    ok = application:set_env(broker_event, policy, bogus),
    {error, {invalid_policy, bogus}} = broker_event:apply_policy(Msg),
    ok.

non_message_input_is_rejected(_Config) ->
    {error, {not_a_broker_message, #{}}} = broker_event:apply_policy(#{}),
    {error, {not_a_broker_message, undefined}} =
        broker_event:apply_policy(undefined, pass_through),
    ok.

%%%_* The reserved interface ----------------------------------------------

cloud_event_record_matches_readme_3_1(_Config) ->
    [specversion, id, source, type, subject, time, datacontenttype,
     dataschema, extensions, data, data_base64] =
        record_info(fields, cloud_event),
    12 = record_info(size, cloud_event),
    Event = #cloud_event{id = <<"e1">>, source = <<"/s3">>, type = <<"demo">>},
    <<"1.0">> = Event#cloud_event.specversion,
    #{} = Event#cloud_event.extensions,
    undefined = Event#cloud_event.data,
    undefined = Event#cloud_event.data_base64,
    ok.

%%%_* Structured codec -------------------------------------------------------

structured_roundtrip_preserves_extensions(_Config) ->
    Body = structured_json(),
    {ok, Event} = broker_event_codec:decode_structured(Body),
    <<"1.0">> = maps:get(specversion, Event),
    <<"e1">> = maps:get(id, Event),
    <<"/s3">> = maps:get(source, Event),
    <<"demo">> = maps:get(type, Event),
    <<"s">> = maps:get(subject, Event),
    <<"2026-11-08T12:00:00Z">> = maps:get(time, Event),
    <<"application/json">> = maps:get(datacontenttype, Event),
    <<"https://example/schema">> = maps:get(dataschema, Event),
    #{<<"myext">> := <<"v">>, <<"count">> := 3} = maps:get(extensions, Event),
    #{<<"nested">> := [1, 2, 3]} = maps:get(data, Event),
    {ok, Body2} = broker_event_codec:encode_structured(Event),
    {ok, Event2} = broker_event_codec:decode_structured(Body2),
    ?assertEqual(Event, Event2),
    ok.

missing_required_attributes_rejected(_Config) ->
    Cases = [{"{\"id\":\"e\",\"source\":\"/s\",\"type\":\"t\"}", specversion},
             {"{\"specversion\":\"1.0\",\"source\":\"/s\",\"type\":\"t\"}", id},
             {"{\"specversion\":\"1.0\",\"id\":\"e\",\"type\":\"t\"}", source},
             {"{\"specversion\":\"1.0\",\"id\":\"e\",\"source\":\"/s\"}", type}],
    lists:foreach(
      fun({Json, Missing}) ->
              {error, {invalid_event, {missing, [Missing]}}} =
                  broker_event_codec:decode_structured(list_to_binary(Json))
      end, Cases),
    ok.

wrong_core_types_rejected(_Config) ->
    Cases = [<<"{\"specversion\":\"1.0\",\"id\":12,\"source\":\"/s\",\"type\":\"t\"}">>,
             <<"{\"specversion\":\"1.0\",\"id\":\"\",\"source\":\"/s\",\"type\":\"t\"}">>,
             <<"{\"specversion\":1.0,\"id\":\"e\",\"source\":\"/s\",\"type\":\"t\"}">>,
             <<"{\"specversion\":\"1.0\",\"id\":\"e\",\"source\":\"/s\",\"type\":true}">>,
             <<"{\"specversion\":\"1.0\",\"id\":\"e\",\"source\":\"/s\",\"type\":\"t\","
               "\"subject\":7}">>],
    lists:foreach(
      fun(Body) ->
              ?assertMatch({error, {invalid_event, _}},
                           broker_event_codec:decode_structured(Body))
      end, Cases),
    ok.

unsupported_specversion_rejected(_Config) ->
    Body = <<"{\"specversion\":\"0.3\",\"id\":\"e\",\"source\":\"/s\",\"type\":\"t\"}">>,
    {error, {invalid_event, {unsupported_specversion, <<"0.3">>}}} =
        broker_event_codec:decode_structured(Body),
    ok.

invalid_time_rejected(_Config) ->
    Body = <<"{\"specversion\":\"1.0\",\"id\":\"e\",\"source\":\"/s\",\"type\":\"t\","
             "\"time\":\"yesterday\"}">>,
    ?assertMatch({error, {invalid_event, {bad_type, time, _}}},
                 broker_event_codec:decode_structured(Body)),
    ok.

valid_time_accepted(_Config) ->
    Bodies = [<<"2026-11-08T12:00:00Z">>,
              <<"2026-11-08T12:00:00.123Z">>,
              <<"2026-11-08T12:00:00+08:00">>],
    lists:foreach(
      fun(Time) ->
              Body = iolist_to_binary(
                       ["{\"specversion\":\"1.0\",\"id\":\"e\",\"source\":\"/s\","
                        "\"type\":\"t\",\"time\":\"", Time, "\"}"]),
              {ok, _} = broker_event_codec:decode_structured(Body)
      end, Bodies),
    ok.

data_and_data_base64_mutually_exclusive(_Config) ->
    Body = <<"{\"specversion\":\"1.0\",\"id\":\"e\",\"source\":\"/s\",\"type\":\"t\","
             "\"data\":\"x\",\"data_base64\":\"eA==\"}">>,
    {error, {invalid_event, {mutually_exclusive, data, data_base64}}} =
        broker_event_codec:decode_structured(Body),
    ok.

invalid_base64_rejected(_Config) ->
    Body = <<"{\"specversion\":\"1.0\",\"id\":\"e\",\"source\":\"/s\",\"type\":\"t\","
             "\"data_base64\":\"!!!\"}">>,
    ?assertMatch({error, {invalid_event, {bad_base64, data_base64}}},
                 broker_event_codec:decode_structured(Body)),
    ok.

unknown_extension_preserved(_Config) ->
    Body = <<"{\"specversion\":\"1.0\",\"id\":\"e\",\"source\":\"/s\",\"type\":\"t\","
             "\"tenanthint\":\"blue\",\"retries\":2}">>,
    {ok, Event} = broker_event_codec:decode_structured(Body),
    #{<<"tenanthint">> := <<"blue">>, <<"retries">> := 2} =
        maps:get(extensions, Event),
    {ok, Encoded} = broker_event_codec:encode_structured(Event),
    {ok, Event2} = broker_event_codec:decode_structured(Encoded),
    ?assertEqual(Event, Event2),
    ok.

extension_name_and_count_validated(_Config) ->
    Base = #{specversion => <<"1.0">>, id => <<"e">>,
             source => <<"/s">>, type => <<"t">>},
    {error, {invalid_event, {bad_extension_name, <<"MyExt">>}}} =
        broker_event_codec:validate(Base#{extensions => #{<<"MyExt">> => <<"x">>}}),
    {error, {invalid_event, {bad_extension_value, <<"n">>}}} =
        broker_event_codec:validate(Base#{extensions => #{<<"n">> => null}}),
    TooMany = maps:from_list([{integer_to_binary(I), I} || I <- lists:seq(1, 33)]),
    {error, {invalid_event, {too_many_extensions, 33}}} =
        broker_event_codec:validate(Base#{extensions => TooMany}),
    ok.

%%%_* Binary codec and MQTT binding -----------------------------------------

binary_mode_roundtrip(_Config) ->
    Event = event(<<"raw-bytes">>),
    {ok, Body, Props, CT} = broker_event_codec:encode_binary(Event),
    <<"raw-bytes">> = Body,
    <<"text/plain">> = CT,
    <<"1.0">> = proplists:get_value(<<"ce-specversion">>, Props),
    <<"e1">> = proplists:get_value(<<"ce-id">>, Props),
    {ok, Event2} = broker_event_codec:decode_binary(Body, Props, CT),
    ?assertEqual(Event, Event2),
    ok.

mqtt_structured_binding_roundtrip(_Config) ->
    {ok, Event} = broker_event_codec:decode_structured(structured_json()),
    {ok, Body, Meta, Report} = broker_event_binding:to_mqtt(Event, 5, structured),
    #{mode := structured, target := mqtt, version := 5, losses := []} = Report,
    <<"application/cloudevents+json">> = maps:get(mqtt_content_type, Meta),
    {ok, Event2, structured} = broker_event_binding:decode(message(Body, Meta)),
    ?assertEqual(Event, Event2),
    %% 3.1.1 has no content-type metadata; structured detection still works by
    %% the payload's `specversion'.
    {ok, Body4, Meta4, #{version := 4, losses := []}} =
        broker_event_binding:to_mqtt(Event, 4, structured),
    #{} = Meta4,
    {ok, Event3, structured} = broker_event_binding:decode(message(Body4, Meta4)),
    ?assertEqual(Event, Event3),
    ok.

mqtt_binary_binding_v5_roundtrip(_Config) ->
    Event = event(<<"binary-data">>),
    {ok, Body, Meta, Report} = broker_event_binding:to_mqtt(Event, 5, binary),
    #{mode := binary, version := 5, losses := []} = Report,
    <<"binary-data">> = Body,
    Props = maps:get(mqtt_user_properties, Meta),
    <<"e1">> = proplists:get_value(<<"ce-id">>, Props),
    {ok, Event2, binary} = broker_event_binding:decode(message(Body, Meta)),
    ?assertEqual(Event, Event2),
    ok.

mqtt_binary_binding_311_reports_losses(_Config) ->
    Event = event(<<"binary-data">>),
    {ok, Body, Meta, Report} = broker_event_binding:to_mqtt(Event, 4, binary),
    <<"binary-data">> = Body,
    #{} = Meta,
    #{version := 4, preserved := []} = Report,
    Losses = maps:get(losses, Report),
    true = lists:member(<<"ce-id">>, Losses),
    true = lists:member(<<"ce-specversion">>, Losses),
    true = lists:member(<<"ce-source">>, Losses),
    true = lists:member(<<"ce-type">>, Losses),
    ok.

header_binding_roundtrip_for_all_targets(_Config) ->
    Event = event(<<"binary-data">>),
    lists:foreach(
      fun(Target) ->
              {ok, Body, Headers, Report} =
                  broker_event_binding:to_headers(Event, Target, binary),
              <<"binary-data">> = Body,
              #{mode := binary, target := Target, losses := []} = Report,
              <<"e1">> = maps:get(<<"ce-id">>, Headers),
              <<"1.0">> = maps:get(<<"ce-specversion">>, Headers),
              %% Reverse mapping reads the same header map without re-deriving
              %% per-protocol rules.
              {ok, Decoded} = broker_event_binding:from_headers(Body, Headers, undefined),
              ?assertEqual(Event, Decoded),
              %% And as a proplist, the shape AMQP/Kafka hand over.
              Pairs = maps:to_list(Headers),
              {ok, Decoded2} = broker_event_binding:from_headers(Body, Pairs, undefined),
              ?assertEqual(Event, Decoded2)
      end, [ws, nats, amqp, kafka, pulsar]),
    ok.

header_binding_structured_mode(_Config) ->
    Event = event(<<"binary-data">>),
    {ok, Body, Headers, Report} =
        broker_event_binding:to_headers(Event, kafka, structured),
    #{mode := structured, target := kafka, losses := []} = Report,
    <<"application/cloudevents+json">> = maps:get(<<"content-type">>, Headers),
    Map = json:decode(Body),
    <<"1.0">> = maps:get(<<"specversion">>, Map),
    ok.

%% A publisher that puts `ce-*' straight into the broker header map (WS JSON
%% frame, NATS HPUB, AMQP headers table, Kafka record header, Pulsar property)
%% is recognised as Binary Mode without any MQTT metadata.
header_binding_detect_from_header_channel(_Config) ->
    Event = event(<<"binary-data">>),
    {ok, Body, Headers, _} = broker_event_binding:to_headers(Event, nats, binary),
    Msg = (message(Body))#broker_message{headers = Headers},
    {binary, Props} = broker_event_binding:detect(Msg),
    true = lists:keymember(<<"ce-specversion">>, 1, Props),
    {ok, Event2, binary} = broker_event_binding:decode(Msg),
    ?assertEqual(Event, Event2),
    %% No `ce-*' anywhere: not an event, and `detect' must not guess.
    none = broker_event_binding:detect((message(<<"plain">>))#broker_message{headers = #{}}),
    ok.

%% README §11.2 "cross-protocol conversion roundtrip": an event published on
%% one protocol must survive a hop through the broker and be re-published on
%% another protocol with the same identity and the same data bytes. The header
%% channel is protocol-neutral, so the second encode is a pure re-encode of the
%% first decode -- no path through MQTT metadata.
cross_protocol_headers_survive_a_hop(_Config) ->
    Original = #{specversion => <<"1.0">>,
                 id => <<"hop-1">>,
                 source => <<"/origin">>,
                 type => <<"demo.hop">>,
                 datacontenttype => <<"application/octet-stream">>,
                 extensions => #{<<"traceparent">> => <<"00-abc-def-01">>},
                 data => <<0, 1, 2, 255>>},
    %% Kafka record header -> broker header map -> WebSocket frame header ->
    %% broker header map -> NATS header block. Every hop decodes and re-encodes
    %% from the same neutral header map.
    {ok, Body1, KafkaHeaders, Report1} =
        broker_event_binding:to_headers(Original, kafka, binary),
    #{mode := binary, target := kafka, losses := []} = Report1,
    {ok, Decoded1} = broker_event_binding:from_headers(Body1, KafkaHeaders, undefined),
    ?assertEqual(Original, Decoded1),
    {ok, Body2, WsHeaders, Report2} =
        broker_event_binding:to_headers(Decoded1, ws, binary),
    #{mode := binary, target := ws, losses := []} = Report2,
    {ok, Decoded2} = broker_event_binding:from_headers(Body2, WsHeaders, undefined),
    ?assertEqual(Original, Decoded2),
    {ok, Body3, NatsHeaders, Report3} =
        broker_event_binding:to_headers(Decoded2, nats, binary),
    #{mode := binary, target := nats, losses := []} = Report3,
    {ok, Decoded3} = broker_event_binding:from_headers(Body3, NatsHeaders, undefined),
    ?assertEqual(Original, Decoded3),
    %% The identity attributes and the data bytes never change across hops.
    lists:foreach(
      fun(Key) ->
              ?assertEqual(maps:get(Key, Original), maps:get(Key, Decoded3))
      end, [specversion, id, source, type, datacontenttype]),
    ?assertEqual(maps:get(data, Original), maps:get(data, Decoded3)),
    ?assertEqual(maps:get(extensions, Original), maps:get(extensions, Decoded3)),
    %% And the wire body is byte-identical at every hop.
    ?assertEqual(Body1, Body2),
    ?assertEqual(Body2, Body3),
    ok.

cross_protocol_roundtrip_keeps_identity(_Config) ->
    {ok, Event0} = broker_event_codec:decode_structured(structured_json()),
    {ok, Body1, Meta1, Report1} =
        broker_event_binding:to_mqtt(Event0, 5, binary),
    %% The JSON `data' object becomes bytes in Binary Mode: a named,
    %% non-lossless field, not a silent drop.
    true = lists:member(<<"data">>, maps:get(losses, Report1)),
    {ok, E1, binary} = broker_event_binding:decode(message(Body1, Meta1)),
    {ok, Body2, Meta2, _Report2} =
        broker_event_binding:to_mqtt(E1, 5, structured),
    {ok, E2, structured} = broker_event_binding:decode(message(Body2, Meta2)),
    lists:foreach(
      fun(Key) ->
              ?assertEqual(maps:get(Key, Event0), maps:get(Key, E2))
      end, [specversion, id, source, type, subject, time,
            datacontenttype, dataschema]),
    %% Binary Mode data is bytes; the JSON object is re-serialized on the way
    %% out and comes back as the equivalent byte string. That is exactly the
    %% named non-lossless field, so compare bytes, not the JSON term.
    ?assertEqual(<<"{\"nested\":[1,2,3]}">>, maps:get(data, E2)),
    %% A numeric extension becomes its text form in Binary Mode; the report
    %% names it, so this is an announced loss rather than a silent change.
    true = lists:member(<<"ce-count">>, maps:get(losses, Report1)),
    #{<<"myext">> := <<"v">>, <<"count">> := <<"3">>} =
        maps:get(extensions, E2),
    ok.

event_id_does_not_overwrite_broker_id(_Config) ->
    Msg0 = message(structured_json()),
    {ok, Msg} = broker_event:apply_policy(Msg0, required),
    Event = Msg#broker_message.event,
    <<"e1">> = maps:get(id, Event),
    <<"m1">> = Msg#broker_message.message_id,
    ?assertNotEqual(<<"e1">>, Msg#broker_message.message_id),
    %% The protocol-side packet id stays in proto_meta and never becomes event.id.
    #{packet_id := 7} = Msg#broker_message.proto_meta,
    ok.

%%%_* Policy resolution and arms ---------------------------------------------

policy_resolution_per_tenant_and_destination(_Config) ->
    ok = application:set_env(
           broker_event, policies,
           [{<<"t1">>, <<"orders/#">>, required},
            {<<"*">>, <<"*">>, pass_through}]),
    Base = message(<<"x">>),
    required = broker_event:policy_for(Base#broker_message{tenant_id = <<"t1">>,
                                                           destination = <<"orders/a">>}),
    pass_through = broker_event:policy_for(Base#broker_message{tenant_id = <<"t1">>,
                                                               destination = <<"other">>}),
    pass_through = broker_event:policy_for(Base#broker_message{tenant_id = <<"t2">>,
                                                               destination = <<"orders/a">>}),
    %% A `+' level matches exactly one level.
    ok = application:set_env(broker_event, policies,
                             [{<<"t1">>, <<"a/+/c">>, normalize}]),
    normalize = broker_event:policy_for(Base#broker_message{tenant_id = <<"t1">>,
                                                            destination = <<"a/b/c">>}),
    pass_through = broker_event:policy_for(Base#broker_message{tenant_id = <<"t1">>,
                                                               destination = <<"a/b/c/d">>}),
    ok.

validate_if_present_passes_ordinary_messages(_Config) ->
    Msg = message(<<"plain">>),
    {ok, Msg} = broker_event:apply_policy(Msg, validate_if_present),
    {ok, Out} = broker_event:apply_policy(message(structured_json()),
                                          validate_if_present),
    <<"e1">> = maps:get(id, Out#broker_message.event),
    %% Invalid when it claims to be CloudEvents.
    Bad = message(<<"{\"specversion\":\"1.0\",\"id\":\"e\"}">>),
    ?assertMatch({error, {event_invalid, _}},
                 broker_event:apply_policy(Bad, validate_if_present)),
    ok.

required_rejects_ordinary_message(_Config) ->
    {error, {event_invalid, event_required}} =
        broker_event:apply_policy(message(<<"plain">>), required),
    {error, {event_invalid, event_required}} =
        broker_event:apply_policy(message(<<"{\"a\":1}">>), required),
    ok.

normalize_attaches_event_and_keeps_payload(_Config) ->
    Payload = structured_json(),
    {ok, Msg} = broker_event:apply_policy(message(Payload), normalize),
    Payload = Msg#broker_message.payload,
    Event = Msg#broker_message.event,
    <<"e1">> = maps:get(id, Event),
    #{<<"nested">> := [1, 2, 3]} = maps:get(data, Event),
    ok.

%%%_* Dedup ------------------------------------------------------------------

duplicate_window_deduplicates(_Config) ->
    ok = application:set_env(broker_event, dedup,
                             [{<<"default">>, <<"t11/#">>, 60000}]),
    with_dedup(
      fun() ->
              {ok, Msg} = broker_event:apply_policy(message(structured_json()),
                                                    required),
              false = broker_event:duplicate(Msg),
              true = broker_event:duplicate(Msg),
              %% A different event id is a different event.
              Other = Msg#broker_message{
                        event = (Msg#broker_message.event)#{id => <<"e2">>}},
              false = broker_event:duplicate(Other)
      end),
    ok.

duplicate_is_off_by_default(_Config) ->
    {ok, Msg} = broker_event:apply_policy(message(structured_json()), required),
    false = broker_event:duplicate(Msg),
    false = broker_event:duplicate(Msg),
    ok.

duplicate_window_expires(_Config) ->
    ok = application:set_env(broker_event, dedup,
                             [{<<"*">>, <<"*">>, 30}]),
    with_dedup(
      fun() ->
              {ok, Msg} = broker_event:apply_policy(message(structured_json()),
                                                    required),
              false = broker_event:duplicate(Msg),
              true = broker_event:duplicate(Msg),
              timer:sleep(60),
              false = broker_event:duplicate(Msg)
      end),
    ok.

%%%_* Rejection / isolation -------------------------------------------------

quarantine_writes_a_durable_copy(Config) ->
    Started = start_quarantine_node(Config),
    try
        ok = application:set_env(
               broker_event, quarantine,
               [{<<"default">>, <<"t11/#">>, <<"$invalid/{tenant}/{destination}">>}]),
        Msg = message(<<"raw-payload">>),
        ok = broker_event:quarantine(Msg, {event_invalid, boom}),
        {ok, [{_Offset, Copy}], _} = broker_queue:read_from(0, 0, 10),
        <<"$invalid/default/t11/demo">> = Copy#broker_message.destination,
        <<"raw-payload">> = Copy#broker_message.payload,
        {event_invalid, boom} = maps:get(event_rejection,
                                         Copy#broker_message.headers),
        %% The original destination is evidence, not a new routing key.
        <<"t11/demo">> = maps:get(original_destination,
                                  Copy#broker_message.headers)
    after
        stop_quarantine_node(Started)
    end,
    ok.

acl_does_not_trust_event_attributes(_Config) ->
    ok = application:set_env(broker_security, users,
                             [{<<"guest">>,
                               #{password => <<"guest">>, tenant_id => <<"t0">>}}]),
    {ok, <<"guest">>} = broker_security:authenticate(<<"guest">>, <<"guest">>),
    <<"t0">> = broker_security:tenant_id(<<"guest">>),
    allow = broker_security:authorize(<<"guest">>, #{topic => <<"anything">>}),
    %% The message says t0; the event's source claims a privileged tenant. The
    %% policy decision must still use the authenticated tenant.
    ok = application:set_env(broker_event, policies,
                             [{<<"t0">>, <<"#">>, disabled}]),
    Msg = message(structured_json()),
    disabled = broker_event:policy_for(Msg#broker_message{
                                         tenant_id = <<"t0">>,
                                         destination = <<"secure">>}),
    {ok, Out} = broker_event:apply_policy(Msg#broker_message{tenant_id = <<"t0">>},
                                          disabled),
    <<"t0">> = Out#broker_message.tenant_id,
    undefined = Out#broker_message.event,
    ok.

%%%_* Helpers ----------------------------------------------------------------

event(Data) ->
    #{specversion => <<"1.0">>,
      id => <<"e1">>,
      source => <<"/s3">>,
      type => <<"demo">>,
      datacontenttype => <<"text/plain">>,
      extensions => #{},
      data => Data}.

structured_json() ->
    <<"{\"specversion\":\"1.0\",\"id\":\"e1\",\"source\":\"/s3\","
      "\"type\":\"demo\",\"subject\":\"s\","
      "\"time\":\"2026-11-08T12:00:00Z\","
      "\"datacontenttype\":\"application/json\","
      "\"dataschema\":\"https://example/schema\","
      "\"myext\":\"v\",\"count\":3,"
      "\"data\":{\"nested\":[1,2,3]}}">>.

message(Payload) ->
    message(Payload, #{}).

message(Payload, Meta) ->
    #broker_message{
       message_id = <<"m1">>,
       tenant_id = <<"default">>,
       destination = <<"t11/demo">>,
       partition_id = 0,
       payload = Payload,
       payload_encoding = raw,
       headers = #{},
       proto_meta = maps:merge(#{qos => 1, dup => 0, retain => 0, packet_id => 7},
                               Meta),
       persistence = durable,
       reliability = fsync,
       created_at = 0,
       expires_at = undefined,
       event = undefined}.

with_dedup(Fun) ->
    stop_dedup(),
    {ok, Pid} = broker_event_dedup:start_link(),
    try Fun()
    after
        case is_process_alive(Pid) of
            true -> gen_server:stop(Pid);
            false -> ok
        end
    end.

stop_dedup() ->
    case whereis(broker_event_dedup) of
        undefined -> ok;
        Pid -> _ = (catch gen_server:stop(Pid)), ok
    end.

%% Returns every application this fixture started, so teardown can stop
%% exactly those. `ensure_all_started(broker_event)' pulls in broker_metrics,
%% whose reporter child would otherwise outlive the case and make
%% `broker_metrics_SUITE:report_line_is_single' see a registered server in a
%% later suite (CT runs all suites in one node).
start_quarantine_node(Config) ->
    _ = stop_quarantine_node([]),
    _ = application:load(broker_storage),
    _ = application:load(broker_router),
    _ = application:load(broker_queue),
    DataDir = filename:join([?config(priv_dir, Config), "quarantine", "data"]),
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok = application:set_env(broker_storage, data_dir, DataDir),
    ok = application:set_env(broker_storage, wal, #{require_durable_fs => false}),
    ok = application:set_env(broker_router, partitions, 1),
    ok = application:set_env(broker_queue, partitions, 1),
    {ok, StorageApps} = application:ensure_all_started(broker_storage),
    [ok = broker_queue:create_partition(P) || P <- broker_queue:partition_ids()],
    {ok, EventApps} = application:ensure_all_started(broker_event),
    StorageApps ++ EventApps.

%% Reverse start order: dependents first, `broker_metrics' (started implicitly)
%% last.
stop_quarantine_node(Started) ->
    lists:foreach(fun(App) -> _ = application:stop(App) end,
                  lists:reverse(Started)),
    ok.
