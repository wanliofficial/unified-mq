%% @doc T11: the pass_through stub. The suite pins what "does nothing" means,
%% which is the only thing a stub can get wrong:
%%
%%   * the identity is term-for-term (payload bytes, headers, event unchanged),
%%   * no payload is inspected -- not even a valid CloudEvents JSON body,
%%   * the three policies that promise validation fail loudly instead of
%%     silently degrading (OQ-5 option (a)),
%%   * the reserved `#cloud_event{}' shape matches README §3.1, so S3 starts
%%     from the agreed interface.
-module(broker_event_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("broker_core/include/broker_message.hrl").
-include_lib("broker_event/include/cloud_event.hrl").

all() ->
    [default_policy_is_pass_through,
     pass_through_returns_the_message_term_for_term,
     disabled_is_also_an_identity,
     unimplemented_policies_fail_loudly,
     an_unknown_policy_is_rejected,
     non_message_input_is_rejected,
     cloud_event_record_matches_readme_3_1,
     no_payload_is_ever_parsed].

init_per_suite(Config) ->
    Saved = application:get_env(broker_event, policy, undefined),
    %% The default arm is only testable with the key absent.
    ok = application:unset_env(broker_event, policy),
    [{saved_policy, Saved} | Config].

end_per_suite(Config) ->
    %% J10 lesson: if init_per_suite is exported, end_per_suite must exist and
    %% restore, or a rewritten env leaks into later suites.
    case proplists:get_value(saved_policy, Config) of
        undefined -> ok = application:unset_env(broker_event, policy);
        Saved -> ok = application:set_env(broker_event, policy, Saved)
    end,
    ok.

%%%_* Policy resolution ----------------------------------------------------

default_policy_is_pass_through(_Config) ->
    pass_through = broker_event:policy(),
    ok.

%%%_* The identity ---------------------------------------------------------

pass_through_returns_the_message_term_for_term(_Config) ->
    Msg = message(<<"{\"specversion\":\"1.0\",\"id\":\"e1\","
                    "\"source\":\"/t11\",\"type\":\"demo\"}">>),
    {ok, Out} = broker_event:apply_policy(Msg),
    %% Record equality covers payload bytes, headers, proto_meta and the
    %% `event' slot at once; a stubbed-out field would break it.
    Msg = Out,
    undefined = Out#broker_message.event,
    ok.

disabled_is_also_an_identity(_Config) ->
    ok = application:set_env(broker_event, policy, disabled),
    disabled = broker_event:policy(),
    Msg = message(<<"plain-bytes">>),
    {ok, Out} = broker_event:apply_policy(Msg),
    Msg = Out,
    ok.

%%%_* The unimplemented arms -----------------------------------------------

unimplemented_policies_fail_loudly(_Config) ->
    Msg = message(<<"x">>),
    lists:foreach(
      fun(P) ->
              {error, {policy_not_implemented, P, s3}} =
                  broker_event:apply_policy(Msg, P)
      end, [validate_if_present, required, normalize]),
    %% And through the config, which is the path a deployment actually takes.
    ok = application:set_env(broker_event, policy, required),
    required = broker_event:policy(),
    {error, {policy_not_implemented, required, s3}} =
        broker_event:apply_policy(Msg),
    ok.

an_unknown_policy_is_rejected(_Config) ->
    Msg = message(<<"x">>),
    {error, {invalid_policy, validate}} =
        broker_event:apply_policy(Msg, validate),
    {error, {invalid_policy, <<"pass_through">>}} =
        broker_event:apply_policy(Msg, <<"pass_through">>),
    ok = application:set_env(broker_event, policy, bogus),
    bogus = broker_event:policy(),
    {error, {invalid_policy, bogus}} = broker_event:apply_policy(Msg),
    ok.

non_message_input_is_rejected(_Config) ->
    {error, {not_a_broker_message, #{}}} = broker_event:apply_policy(#{}),
    {error, {not_a_broker_message, undefined}} =
        broker_event:apply_policy(undefined, pass_through),
    ok.

%%%_* The reserved interface ----------------------------------------------

cloud_event_record_matches_readme_3_1(_Config) ->
    %% README §3.1 order, and `size' = 11 fields + tag.
    [specversion, id, source, type, subject, time, datacontenttype,
     dataschema, extensions, data, data_base64] =
        record_info(fields, cloud_event),
    12 = record_info(size, cloud_event),
    Event = #cloud_event{id = <<"e1">>, source = <<"/t11">>, type = <<"demo">>},
    <<"1.0">> = Event#cloud_event.specversion,
    #{} = Event#cloud_event.extensions,
    undefined = Event#cloud_event.data,
    undefined = Event#cloud_event.data_base64,
    ok.

%%%_* The absence of parsing -----------------------------------------------

no_payload_is_ever_parsed(_Config) ->
    %% Garbage bytes, a valid CloudEvents JSON body and a nested JSON object
    %% must all produce the same outcome: nothing is decoded, `event' stays
    %% `undefined', and the bytes come back untouched. If S1 ever starts
    %% sniffing, this is the test that fails.
    Payloads = [<<255, 0, 1, 2>>,
                <<"{\"specversion\":\"1.0\",\"id\":\"e1\","
                  "\"source\":\"/t11\",\"type\":\"demo\","
                  "\"data\":{\"nested\":[1,2,3]}}">>,
                <<"not json at all">>],
    lists:foreach(
      fun(Payload) ->
              Msg = message(Payload),
              {ok, Out} = broker_event:apply_policy(Msg, pass_through),
              Payload = Out#broker_message.payload,
              undefined = Out#broker_message.event,
              #{} = Out#broker_message.headers
      end, Payloads),
    ok.

%%%_* Helpers --------------------------------------------------------------

message(Payload) ->
    #broker_message{
       message_id = <<"m1">>,
       tenant_id = <<"default">>,
       destination = <<"t11/demo">>,
       partition_id = 0,
       payload = Payload,
       payload_encoding = raw,
       headers = #{},
       proto_meta = #{qos => 1, dup => 0, retain => 0, packet_id => 1},
       persistence = durable,
       reliability = fsync,
       created_at = 0,
       expires_at = undefined,
       event = undefined}.
