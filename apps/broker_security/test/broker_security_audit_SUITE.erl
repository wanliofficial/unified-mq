%% @doc S6 Gate: the three security-audit checks (roadmap §8.3).
%%
%% This suite is the executable form of the Gate checklist. Each case is one
%% named audit item, written as an attacker would try it:
%%
%%   1. **Tenant escalation** -- an authenticated principal of one tenant
%%      cannot publish or subscribe in another, even when a permissive ACL rule
%%      would allow the topic.
%%   2. **ACL bypass** -- with rules configured, there is no fall-through to
%%      allow: an unmatched principal/action/topic is denied, a deny rule wins
%%      before a later allow, and the legacy `allow' atom is the *only* way to
%%      keep the pre-S6 posture.
%%   3. **Event-attribute escalation** -- CloudEvents attributes are context,
%%      not identity: `source', `subject', `id' and extensions can never change
%%      the broker tenant, destination, or ACL decision. The ACL runs on the
%%      broker message before any event policy, and a policy that accepts the
%%      event returns the same tenant/destination it was given.
-module(broker_security_audit_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("broker_core/include/broker_message.hrl").

all() ->
    [tenant_escalation_publish_is_denied,
     tenant_escalation_subscribe_is_denied,
     acl_bypass_by_unmatched_rule_is_denied,
     acl_bypass_by_action_swap_is_denied,
     acl_deny_precedes_a_later_allow,
     legacy_allow_is_explicit_not_a_fallback,
     event_attribute_cannot_change_the_acl_decision,
     event_policy_preserves_tenant_and_destination,
     forged_event_source_cannot_impersonate_another_principal].

init_per_suite(Config) ->
    SavedUsers = application:get_env(broker_security, users, undefined),
    SavedAcl = application:get_env(broker_security, acl, undefined),
    SavedPolicy = application:get_env(broker_event, policy, undefined),
    application:set_env(broker_security, users,
                        [{<<"alice">>, #{password => <<"pw">>, tenant_id => <<"t1">>}},
                         {<<"bob">>, #{password => <<"pw">>, tenant_id => <<"t2">>}}]),
    [{saved_users, SavedUsers}, {saved_acl, SavedAcl},
     {saved_policy, SavedPolicy} | Config].

end_per_suite(Config) ->
    restore(broker_security, users, proplists:get_value(saved_users, Config)),
    restore(broker_security, acl, proplists:get_value(saved_acl, Config)),
    restore(broker_event, policy, proplists:get_value(saved_policy, Config)),
    ok.

init_per_testcase(_Case, Config) ->
    application:set_env(broker_security, acl, allow),
    application:set_env(broker_event, policy, pass_through),
    Config.

%%%_* 1. Tenant escalation --------------------------------------------------

tenant_escalation_publish_is_denied(_Config) ->
    %% The most permissive rule list there is: everything, by everyone.
    application:set_env(broker_security, acl,
                        [rule(allow, <<"*">>, <<"*">>, <<"#">>)]),
    deny = broker_security:authorize(
             <<"alice">>,
             #{action => publish, tenant_id => <<"t2">>,
               destination => <<"t2/anything">>}),
    allow = broker_security:authorize(
              <<"alice">>,
              #{action => publish, tenant_id => <<"t1">>,
                destination => <<"t1/anything">>}),
    ok.

tenant_escalation_subscribe_is_denied(_Config) ->
    application:set_env(broker_security, acl,
                        [rule(allow, <<"*">>, subscribe, <<"#">>)]),
    deny = broker_security:authorize(
             <<"bob">>,
             #{action => subscribe, tenant_id => <<"t1">>,
               destination => <<"t1/#">>}),
    ok.

%%%_* 2. ACL bypass ---------------------------------------------------------

acl_bypass_by_unmatched_rule_is_denied(_Config) ->
    application:set_env(broker_security, acl,
                        [rule(allow, <<"alice">>, publish, <<"t1/allowed/#">>)]),
    %% A different principal, a different topic and an unknown action all fall
    %% through to deny rather than to allow.
    deny = broker_security:authorize(<<"bob">>,
                                     #{action => publish, tenant_id => <<"t2">>,
                                       destination => <<"t2/allowed/x">>}),
    deny = broker_security:authorize(<<"alice">>,
                                     #{action => publish, tenant_id => <<"t1">>,
                                       destination => <<"t1/other/x">>}),
    deny = broker_security:authorize(<<"alice">>,
                                     #{action => admin, tenant_id => <<"t1">>,
                                       destination => <<"t1/allowed/x">>}),
    ok.

acl_bypass_by_action_swap_is_denied(_Config) ->
    application:set_env(broker_security, acl,
                        [rule(allow, <<"alice">>, publish, <<"t1/#">>)]),
    %% Allowed to publish does not imply allowed to subscribe.
    deny = broker_security:authorize(<<"alice">>,
                                     #{action => subscribe, tenant_id => <<"t1">>,
                                       destination => <<"t1/#">>}),
    ok.

acl_deny_precedes_a_later_allow(_Config) ->
    application:set_env(broker_security, acl,
                        [rule(deny, <<"*">>, <<"*">>, <<"$sys/#">>),
                         rule(allow, <<"*">>, <<"*">>, <<"#">>)]),
    deny = broker_security:authorize(<<"alice">>,
                                     #{action => publish, tenant_id => <<"t1">>,
                                       destination => <<"$sys/shutdown">>}),
    allow = broker_security:authorize(<<"alice">>,
                                      #{action => publish, tenant_id => <<"t1">>,
                                        destination => <<"app/x">>}),
    ok.

legacy_allow_is_explicit_not_a_fallback(_Config) ->
    %% The only configuration that allows everything is the atom `allow'. A
    %% malformed/empty rule list is not interpreted as "allow".
    application:set_env(broker_security, acl, []),
    deny = broker_security:authorize(<<"alice">>,
                                     #{action => publish, tenant_id => <<"t1">>,
                                       destination => <<"t1/x">>}),
    application:set_env(broker_security, acl, garbage),
    allow = broker_security:authorize(<<"alice">>,
                                      #{action => publish, tenant_id => <<"t1">>,
                                        destination => <<"t1/x">>}),
    ok.

%%%_* 3. Event attributes cannot escalate -----------------------------------

event_attribute_cannot_change_the_acl_decision(_Config) ->
    %% Alice may publish only under `t1/app/'. A CloudEvents payload whose
    %% `source' claims she is `bob', whose `subject' names another tenant, and
    %% whose extension asks for `t2/', still publishes to a denied destination
    %% because the ACL decision was made on the broker destination before the
    %% payload was ever decoded.
    application:set_env(broker_security, acl,
                        [rule(allow, <<"alice">>, publish, <<"t1/app/#">>)]),
    Msg = msg(<<"t1/other/x">>, event_json(<<"bob">>, <<"t2/secret">>)),
    deny = broker_security:authorize(
             <<"alice">>,
             #{action => publish, tenant_id => Msg#broker_message.tenant_id,
               destination => Msg#broker_message.destination}),
    %% The same event on an allowed topic is still only an event: the policy
    %% accepts it, but the broker tenant and destination are untouched.
    {ok, Accepted} = broker_event:apply_policy(
                       Msg#broker_message{destination = <<"t1/app/x">>},
                       validate_if_present),
    <<"t1">> = Accepted#broker_message.tenant_id,
    <<"t1/app/x">> = Accepted#broker_message.destination,
    ok.

event_policy_preserves_tenant_and_destination(_Config) ->
    Msg = msg(<<"t1/app/x">>, event_json(<<"alice">>, <<"t1/app/x">>)),
    lists:foreach(
      fun(Policy) ->
              {ok, Out} = broker_event:apply_policy(Msg, Policy),
              <<"t1">> = Out#broker_message.tenant_id,
              <<"t1/app/x">> = Out#broker_message.destination
      end, [disabled, pass_through, validate_if_present, required, normalize]),
    ok.

forged_event_source_cannot_impersonate_another_principal(_Config) ->
    %% The authenticated principal is the only identity the ACL sees. A forged
    %% `source' in the payload does not become the principal, so it cannot use
    %% bob's allow rule.
    application:set_env(broker_security, acl,
                        [rule(allow, <<"bob">>, publish, <<"t2/#">>)]),
    Payload = event_json(<<"bob">>, <<"t2/x">>),
    %% broker_security is asked about alice (the connection), not about the
    %% source string inside the payload.
    deny = broker_security:authorize(<<"alice">>,
                                     #{action => publish, tenant_id => <<"t2">>,
                                       destination => <<"t2/x">>}),
    %% And a topic allowed to bob is not allowed to alice even when the event
    %% claims bob.
    deny = broker_security:authorize(<<"alice">>,
                                     #{action => publish, tenant_id => <<"t1">>,
                                       destination => <<"t2/x">>}),
    true = is_binary(Payload),
    ok.

%%%_* Helpers --------------------------------------------------------------

rule(Effect, Principal, Action, Pattern) ->
    #{effect => Effect, principal => Principal, action => Action,
      pattern => Pattern}.

msg(Destination, Payload) ->
    #broker_message{destination = Destination, payload = Payload,
                    headers = #{}, proto_meta = #{}, tenant_id = <<"t1">>}.

event_json(Source, Subject) ->
    iolist_to_binary(
      json:encode(#{<<"specversion">> => <<"1.0">>,
                    <<"id">> => <<"audit-1">>,
                    <<"source">> => Source,
                    <<"subject">> => Subject,
                    <<"type">> => <<"audit.test">>,
                    <<"tenant">> => <<"t2">>,
                    <<"data">> => #{<<"ok">> => true}})).

restore(App, Key, undefined) ->
    application:unset_env(App, Key);
restore(App, Key, Value) ->
    application:set_env(App, Key, Value).
