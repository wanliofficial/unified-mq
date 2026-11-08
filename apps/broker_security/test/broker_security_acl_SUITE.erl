%% @doc S6 (README §10.1): ACL and tenant isolation.
%%
%% The two rules this suite pins are the ones a security review will try first:
%%
%%   * A principal can never act in another tenant, whatever the ACL says.
%%   * With rules configured, no match means deny (fail closed); the legacy
%%     `acl = allow' posture is the only thing that allows by default.
-module(broker_security_acl_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("broker_core/include/broker_message.hrl").
-include_lib("broker_router/include/broker_router.hrl").

all() ->
    [legacy_allow_posture_allows_everything,
     no_rule_match_is_denied,
     first_matching_rule_wins,
     action_must_match,
     pattern_prefix_and_wildcard,
     cross_tenant_request_is_denied_before_any_rule,
     same_tenant_request_is_judged_by_acl,
     authorized_returns_boolean,
     peer_cannot_see_other_tenants_subscriptions,
     legacy_wildcard_subscription_is_visible_to_all].

init_per_suite(Config) ->
    SavedUsers = application:get_env(broker_security, users, undefined),
    SavedAcl = application:get_env(broker_security, acl, undefined),
    application:set_env(broker_security, users,
                        [{<<"alice">>, #{password => <<"pw">>, tenant_id => <<"t1">>}},
                         {<<"bob">>, #{password => <<"pw">>, tenant_id => <<"t2">>}}]),
    [{saved_users, SavedUsers}, {saved_acl, SavedAcl} | Config].

end_per_suite(Config) ->
    restore(users, proplists:get_value(saved_users, Config)),
    restore(acl, proplists:get_value(saved_acl, Config)),
    ok.

init_per_testcase(_Case, Config) ->
    application:set_env(broker_security, acl, allow),
    Config.

%%%_* ACL ------------------------------------------------------------------

legacy_allow_posture_allows_everything(_Config) ->
    application:set_env(broker_security, acl, allow),
    allow = broker_security:authorize(<<"alice">>,
                                      #{action => publish, tenant_id => <<"t1">>,
                                        destination => <<"anything/at/all">>}),
    ok.

no_rule_match_is_denied(_Config) ->
    application:set_env(broker_security, acl,
                        [rule(allow, <<"alice">>, publish, <<"t1/*">>)]),
    allow = broker_security:authorize(<<"alice">>,
                                      #{action => publish, tenant_id => <<"t1">>,
                                        destination => <<"t1/a">>}),
    deny = broker_security:authorize(<<"alice">>,
                                     #{action => publish, tenant_id => <<"t1">>,
                                       destination => <<"other/a">>}),
    %% No rule names bob, so bob is denied rather than falling through to allow.
    deny = broker_security:authorize(<<"bob">>,
                                     #{action => publish, tenant_id => <<"t2">>,
                                       destination => <<"t2/a">>}),
    ok.

first_matching_rule_wins(_Config) ->
    application:set_env(broker_security, acl,
                        [rule(deny, <<"*">>, <<"*">>, <<"$sys/#">>),
                         rule(allow, <<"*">>, <<"*">>, <<"#">>)]),
    deny = broker_security:authorize(<<"alice">>,
                                     #{action => publish, tenant_id => <<"t1">>,
                                       destination => <<"$sys/x">>}),
    allow = broker_security:authorize(<<"alice">>,
                                      #{action => publish, tenant_id => <<"t1">>,
                                        destination => <<"app/x">>}),
    ok.

action_must_match(_Config) ->
    application:set_env(broker_security, acl,
                        [rule(allow, <<"alice">>, publish, <<"t1/#">>)]),
    allow = broker_security:authorize(<<"alice">>,
                                      #{action => publish, tenant_id => <<"t1">>,
                                        destination => <<"t1/a">>}),
    deny = broker_security:authorize(<<"alice">>,
                                     #{action => subscribe, tenant_id => <<"t1">>,
                                       destination => <<"t1/a">>}),
    ok.

pattern_prefix_and_wildcard(_Config) ->
    application:set_env(broker_security, acl,
                        [rule(allow, <<"alice">>, subscribe, <<"t1/prefix/">>)]),
    allow = broker_security:authorize(<<"alice">>,
                                      #{action => subscribe, tenant_id => <<"t1">>,
                                        destination => <<"t1/prefix/x">>}),
    %% `t1/prefix` itself does not have the trailing slash the rule requires.
    deny = broker_security:authorize(<<"alice">>,
                                     #{action => subscribe, tenant_id => <<"t1">>,
                                       destination => <<"t1/prefix">>}),
    ok.

%%%_* Tenant isolation -----------------------------------------------------

cross_tenant_request_is_denied_before_any_rule(_Config) ->
    %% Even a fully permissive rule cannot move alice (tenant t1) into t2: the
    %% tenant check runs before the ACL by construction.
    application:set_env(broker_security, acl,
                        [rule(allow, <<"*">>, <<"*">>, <<"#">>)]),
    deny = broker_security:authorize(<<"alice">>,
                                     #{action => publish, tenant_id => <<"t2">>,
                                       destination => <<"t2/a">>}),
    ok.

same_tenant_request_is_judged_by_acl(_Config) ->
    application:set_env(broker_security, acl,
                        [rule(allow, <<"*">>, <<"*">>, <<"#">>)]),
    allow = broker_security:authorize(<<"alice">>,
                                      #{action => publish, tenant_id => <<"t1">>,
                                        destination => <<"t1/a">>}),
    ok.

authorized_returns_boolean(_Config) ->
    application:set_env(broker_security, acl,
                        [rule(allow, <<"alice">>, publish, <<"t1/#">>)]),
    true = broker_security:authorized(<<"alice">>,
                                      #{action => publish, tenant_id => <<"t1">>,
                                        destination => <<"t1/a">>}),
    false = broker_security:authorized(<<"alice">>,
                                       #{action => publish, tenant_id => <<"t1">>,
                                         destination => <<"bad/a">>}),
    ok.

%%%_* Router tenant filtering ----------------------------------------------

peer_cannot_see_other_tenants_subscriptions(_Config) ->
    {ok, Env} = broker_router_hash:init(#{partitions => 2}),
    try
        %% alice's subscription is registered under tenant t1; a publisher in t2
        %% must not see it even on the identical topic.
        ok = broker_router_hash:register(Env, <<"shared/topic">>, {exclusive, <<"c1">>, <<"shared/topic">>},
                                         #{tenant_id => <<"t1">>}),
        {ok, [{_, Refs1}]} =
            broker_router_hash:route(<<"shared/topic">>, ctx(<<"t1">>), Env),
        [_] = Refs1,
        {ok, [{_, Refs2}]} =
            broker_router_hash:route(<<"shared/topic">>, ctx(<<"t2">>), Env),
        [] = Refs2,
        ok
    after
        catch gen_server:stop(broker_router_hash_store, normal, 1000)
    end.

legacy_wildcard_subscription_is_visible_to_all(_Config) ->
    {ok, Env} = broker_router_hash:init(#{partitions => 2}),
    try
        %% Pre-S6 sessions registered without tenant metadata are `<<"*">>'.
        %% They keep working for every tenant after an upgrade.
        ok = broker_router_hash:register(Env, <<"legacy/topic">>,
                                         {exclusive, <<"old">>, <<"legacy/topic">>},
                                         #{}),
        {ok, [{_, Refs1}]} =
            broker_router_hash:route(<<"legacy/topic">>, ctx(<<"t1">>), Env),
        [_] = Refs1,
        {ok, [{_, Refs2}]} =
            broker_router_hash:route(<<"legacy/topic">>, ctx(<<"t2">>), Env),
        [_] = Refs2,
        ok
    after
        catch gen_server:stop(broker_router_hash_store, normal, 1000)
    end.

%%%_* Helpers -------------------------------------------------------------

rule(Effect, Principal, Action, Pattern) ->
    #{effect => Effect, principal => Principal, action => Action,
      pattern => Pattern}.

ctx(Tenant) ->
    #route_ctx{tenant_id = Tenant, publisher = undefined, reliability = os_buffer}.

restore(Key, undefined) ->
    application:unset_env(broker_security, Key);
restore(Key, Value) ->
    application:set_env(broker_security, Key, Value).
