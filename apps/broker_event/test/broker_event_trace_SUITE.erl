%% @doc README §10.2 / W3C Trace Context propagation.
%%
%% Trace headers are client-controlled, so the executable contract has two
%% halves: the values must be validated against the W3C format before they are
%% allowed to travel with a message, and they must never be able to change the
%% identity or tenant that authorisation sees.  Propagation is off by default
%% and can be gated to an explicit list of trusted tenants.
-module(broker_event_trace_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("broker_core/include/broker_message.hrl").

-define(VALID_PARENT,
        <<"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01">>).
-define(UPPER_PARENT,
        <<"00-4BF92F3577B34DA6A3CE929D0E0E4736-00F067AA0BA902B7-01">>).
-define(VALID_STATE, <<"vendor1=abc, vendor2=def">>).

all() ->
    [version_00_traceparent_is_normalised_to_lower_case,
     malformed_traceparent_is_dropped,
     tracestate_is_validated_and_ows_stripped,
     oversized_or_malformed_tracestate_is_dropped,
     off_strips_trace_headers_by_default,
     on_propagates_only_a_validated_context,
     trusted_tenants_limit_propagation,
     propagate_only_rewrites_trace_headers,
     trace_headers_cannot_escalate_the_tenant,
     apply_policy_is_the_shared_propagation_seam].

init_per_suite(Config) ->
    Saved = [{broker_event, policy, application:get_env(broker_event, policy, undefined)},
             {broker_event, trace_context,
              application:get_env(broker_event, trace_context, undefined)},
             {broker_security, users,
              application:get_env(broker_security, users, undefined)},
             {broker_security, acl,
              application:get_env(broker_security, acl, undefined)},
             {broker_security, default_tenant,
              application:get_env(broker_security, default_tenant, undefined)}],
    application:set_env(broker_event, policy, pass_through),
    application:set_env(broker_security, users,
                        [{<<"alice">>, #{password => <<"pw">>, tenant_id => <<"t1">>}}]),
    application:set_env(broker_security, acl, allow),
    [{saved, Saved} | Config].

end_per_suite(Config) ->
    lists:foreach(fun({App, Key, Value}) -> restore(App, Key, Value) end,
                  ?config(saved, Config)),
    ok.

init_per_testcase(_Case, Config) ->
    application:set_env(broker_event, trace_context, off),
    Config.

%%%_* 1. W3C traceparent ---------------------------------------------------

version_00_traceparent_is_normalised_to_lower_case(_Config) ->
    {ok, ?VALID_PARENT} = broker_event_trace:parse_traceparent(?VALID_PARENT),
    %% W3C requires lower-case hex on the wire; an upper-case value is
    %% accepted from an inbound client but normalised before propagation.
    {ok, ?VALID_PARENT} = broker_event_trace:parse_traceparent(?UPPER_PARENT),
    ok.

malformed_traceparent_is_dropped(_Config) ->
    Bad = [undefined, <<>>, <<"00-short">>,
           <<"ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01">>,
           <<"01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01">>,
           <<"00-00000000000000000000000000000000-00f067aa0ba902b7-01">>,
           <<"00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01">>,
           <<"00-4bf92f3577b34da6a3ce929d0e0e473g-00f067aa0ba902b7-01">>,
           <<"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra">>],
    lists:foreach(fun(V) -> ?assertEqual(none, broker_event_trace:parse_traceparent(V)) end,
                  Bad),
    ok.

%%%_* 2. W3C tracestate ----------------------------------------------------

tracestate_is_validated_and_ows_stripped(_Config) ->
    {ok, <<"vendor1=abc,vendor2=def">>} =
        broker_event_trace:parse_tracestate(?VALID_STATE),
    {ok, <<"a@b=x">>} = broker_event_trace:parse_tracestate(<<" a@b=x ">>),
    ok.

oversized_or_malformed_tracestate_is_dropped(_Config) ->
    TooLong = list_to_binary(lists:duplicate(513, $a)),
    Many = iolist_to_binary(lists:join(<<",">>,
                                      [<<"k", (integer_to_binary(I))/binary, "=v">>
                                       || I <- lists:seq(1, 33)])),
    Bad = [undefined, TooLong, Many, <<"novalue">>, <<"=v">>,
           <<"k=">>, <<"UPPER=v">>, <<"k=v,w">>, <<"k=v=w">>],
    lists:foreach(fun(V) -> ?assertEqual(none, broker_event_trace:parse_tracestate(V)) end,
                  Bad),
    ok.

%%%_* 3. The propagation gate ---------------------------------------------

off_strips_trace_headers_by_default(_Config) ->
    false = broker_event_trace:enabled(),
    Msg = msg(#{<<"traceparent">> => ?VALID_PARENT,
                <<"tracestate">> => ?VALID_STATE,
                <<"ce-id">> => <<"1">>}, <<"t1">>),
    Headers = (broker_event_trace:propagate(Msg))#broker_message.headers,
    ?assertEqual(#{<<"ce-id">> => <<"1">>}, Headers),
    ok.

on_propagates_only_a_validated_context(_Config) ->
    application:set_env(broker_event, trace_context, on),
    true = broker_event_trace:enabled(),
    Msg = msg(#{<<"traceparent">> => ?UPPER_PARENT,
                <<"tracestate">> => ?VALID_STATE,
                <<"ce-id">> => <<"1">>}, <<"t1">>),
    Headers = (broker_event_trace:propagate(Msg))#broker_message.headers,
    ?assertEqual(#{<<"traceparent">> => ?VALID_PARENT,
                   <<"tracestate">> => <<"vendor1=abc,vendor2=def">>,
                   <<"ce-id">> => <<"1">>}, Headers),
    %% An invalid parent is not forwarded, and an invalid state is dropped
    %% while the valid parent survives.
    Bad = broker_event_trace:propagate(
            msg(#{<<"traceparent">> => <<"nope">>,
                  <<"tracestate">> => ?VALID_STATE}, <<"t1">>)),
    ?assertEqual(#{}, Bad#broker_message.headers),
    Partial = broker_event_trace:propagate(
                msg(#{<<"traceparent">> => ?VALID_PARENT,
                      <<"tracestate">> => <<"bad">>}, <<"t1">>)),
    ?assertEqual(#{<<"traceparent">> => ?VALID_PARENT},
                 Partial#broker_message.headers),
    ok.

trusted_tenants_limit_propagation(_Config) ->
    application:set_env(broker_event, trace_context,
                        {trusted_tenants, [<<"t1">>]}),
    true = broker_event_trace:enabled(),
    H = #{<<"traceparent">> => ?VALID_PARENT},
    Kept = broker_event_trace:propagate(msg(H, <<"t1">>)),
    ?assertEqual(H, Kept#broker_message.headers),
    Dropped = broker_event_trace:propagate(msg(H, <<"t2">>)),
    ?assertEqual(#{}, Dropped#broker_message.headers),
    ok.

%%%_* 4. Trust boundary ----------------------------------------------------

propagate_only_rewrites_trace_headers(_Config) ->
    application:set_env(broker_event, trace_context, on),
    Msg = msg(#{<<"traceparent">> => ?VALID_PARENT}, <<"t1">>),
    Result = broker_event_trace:propagate(Msg),
    %% Every slot except `headers' is byte-for-byte unchanged: the module
    %% cannot silently reassign identity, destination, payload or reliability.
    ?assertEqual(Msg#broker_message{headers = Result#broker_message.headers},
                 Result),
    ok.

trace_headers_cannot_escalate_the_tenant(_Config) ->
    application:set_env(broker_event, trace_context, on),
    %% A client tries to smuggle `tenant_id' / `principal' through the trace
    %% header channel.  propagate/1 ignores them as identity inputs.
    Msg0 = msg(#{<<"traceparent">> => ?VALID_PARENT,
                 <<"tenant_id">> => <<"t1">>,
                 <<"principal">> => <<"admin">>}, <<"t2">>),
    Msg1 = broker_event_trace:propagate(Msg0),
    ?assertEqual(<<"t2">>, Msg1#broker_message.tenant_id),
    %% authorize/2 reads only action/tenant_id/destination, so adding a trace
    %% header to the request cannot change the decision.
    Req = #{action => publish, tenant_id => <<"t2">>, destination => <<"d">>},
    ?assertEqual(broker_security:authorize(<<"alice">>, Req),
                 broker_security:authorize(
                   <<"alice">>, Req#{<<"traceparent">> => ?VALID_PARENT})),
    ok.

%%%_* 5. The shared seam ---------------------------------------------------

apply_policy_is_the_shared_propagation_seam(_Config) ->
    application:set_env(broker_event, trace_context, on),
    Msg = msg(#{<<"traceparent">> => ?UPPER_PARENT}, <<"t1">>),
    {ok, Accepted} = broker_event:apply_policy(Msg, pass_through),
    ?assertEqual(?VALID_PARENT,
                 maps:get(<<"traceparent">>, Accepted#broker_message.headers)),
    %% With propagation off the same call is the identity, so all six adapters
    %% keep their existing behaviour until an operator opts in.
    application:set_env(broker_event, trace_context, off),
    {ok, Plain} = broker_event:apply_policy(Msg, pass_through),
    ?assertEqual(#{}, Plain#broker_message.headers),
    ok.

%%%_* Helpers --------------------------------------------------------------

msg(Headers, Tenant) ->
    #broker_message{message_id = <<"m1">>,
                    tenant_id = Tenant,
                    destination = <<"d">>,
                    payload = <<"body">>,
                    payload_encoding = raw,
                    headers = Headers}.

restore(App, Key, undefined) ->
    application:unset_env(App, Key);
restore(App, Key, Value) ->
    application:set_env(App, Key, Value).
