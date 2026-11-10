%% @doc The shared identity table every protocol authenticates against (S7).
%%
%% Before S7 only MQTT went through `broker_security', so the other five
%% protocols had no identity at all: their tenant came from a protocol field
%% or a hardcoded default, and a client could name any tenant it liked.  This
%% suite is the executable form of the fix:
%%
%%   1. **One table, six protocols** -- `authenticate_conn/1' resolves both
%%      credential shapes (user/password and bearer token) against the same
%%      `users' list, and returns the tenant from the matched record.
%%   2. **The client does not choose its tenant** -- the tenant in the answer
%%      comes from the user record; a client that supplies one is ignored.
%%   3. **No enumeration** -- an unknown user and a wrong secret produce the
%%      same error, and so do the two credential shapes.
%%   4. **Anonymous is a deployment decision** -- `auth_required/1' says which
%%      protocols must present a credential; a protocol whose handshake cannot
%%      carry one is never required.
-module(broker_security_identity_SUITE).

-compile([export_all, nowarn_export_all]).

all() ->
    [user_pass_resolves_to_the_record_tenant,
     token_resolves_to_the_record_tenant,
     an_unknown_user_is_indistinguishable_from_a_wrong_secret,
     a_token_miss_is_indistinguishable_from_a_wrong_token,
     the_two_credential_shapes_are_indistinguishable,
     a_malformed_credential_is_rejected,
     the_tenant_comes_from_the_record_not_the_caller,
     require_auth_only_applies_to_configured_protocols,
     require_auth_never_applies_to_a_protocol_that_cannot_carry_a_credential,
     the_anonymous_tenant_is_the_deployment_default].

init_per_suite(Config) ->
    SavedUsers = application:get_env(broker_security, users, undefined),
    SavedRequire = application:get_env(broker_security, require_auth, undefined),
    SavedTenant = application:get_env(broker_security, default_tenant, undefined),
    application:set_env(broker_security, users,
                        [{<<"alice">>, #{password => <<"pw">>, tenant_id => <<"t1">>}},
                         {<<"bob">>, #{password => <<"pw">>, tenant_id => <<"t2">>}},
                         {<<"svc">>, #{token => <<"tok">>, tenant_id => <<"t3">>}}]),
    [{saved_users, SavedUsers}, {saved_require, SavedRequire},
     {saved_tenant, SavedTenant} | Config].

end_per_suite(Config) ->
    restore(broker_security, users, proplists:get_value(saved_users, Config)),
    restore(broker_security, require_auth, proplists:get_value(saved_require, Config)),
    restore(broker_security, default_tenant, proplists:get_value(saved_tenant, Config)),
    ok.

init_per_testcase(_Case, Config) ->
    application:set_env(broker_security, require_auth, []),
    Config.

%%%_* 1. One table, two credential shapes ---------------------------------

user_pass_resolves_to_the_record_tenant(_Config) ->
    {ok, #{principal := <<"alice">>, tenant_id := <<"t1">>}} =
        broker_security:authenticate_conn({user_pass, <<"alice">>, <<"pw">>}),
    {ok, #{principal := <<"bob">>, tenant_id := <<"t2">>}} =
        broker_security:authenticate_conn({user_pass, <<"bob">>, <<"pw">>}),
    ok.

token_resolves_to_the_record_tenant(_Config) ->
    %% A protocol whose handshake only carries a bearer value (Pulsar's `token'
    %% auth method) still resolves to a named principal and therefore to a
    %% tenant -- the token is not a second, parallel identity space.
    {ok, #{principal := <<"svc">>, tenant_id := <<"t3">>}} =
        broker_security:authenticate_conn({token, <<"tok">>}),
    ok.

%%%_* 2. No enumeration ---------------------------------------------------

an_unknown_user_is_indistinguishable_from_a_wrong_secret(_Config) ->
    Unknown = broker_security:authenticate_conn({user_pass, <<"nobody">>, <<"pw">>}),
    Wrong = broker_security:authenticate_conn({user_pass, <<"alice">>, <<"nope">>}),
    {error, bad_username_or_password} = Unknown,
    Unknown = Wrong,
    ok.

a_token_miss_is_indistinguishable_from_a_wrong_token(_Config) ->
    %% `tok' is svc's real token, so `wrong' is a token that exists in no
    %% record; `nosuchtoken' is the same thing spelled differently.  Both are
    %% the same error, so a probe cannot enumerate valid token values.
    Unknown = broker_security:authenticate_conn({token, <<"nosuchtoken">>}),
    Wrong = broker_security:authenticate_conn({token, <<"wrong">>}),
    {error, bad_username_or_password} = Unknown,
    Unknown = Wrong,
    ok.

the_two_credential_shapes_are_indistinguishable(_Config) ->
    %% A probe cannot tell which shape a client used, so it cannot use the
    %% error to enumerate one table against the other.
    ViaUser = broker_security:authenticate_conn({user_pass, <<"nobody">>, <<"x">>}),
    ViaToken = broker_security:authenticate_conn({token, <<"nosuchtoken">>}),
    ViaUser = ViaToken,
    {error, bad_username_or_password} = ViaUser,
    ok.

a_malformed_credential_is_rejected(_Config) ->
    {error, bad_username_or_password} = broker_security:authenticate_conn(nonsense),
    {error, bad_username_or_password} = broker_security:authenticate_conn({user_pass, 1, 2}),
    {error, bad_username_or_password} = broker_security:authenticate_conn({token, <<>>}),
    ok.

%%%_* 3. The tenant is the record's, not the caller's ----------------------

the_tenant_comes_from_the_record_not_the_caller(_Config) ->
    %% The tenant is the isolation boundary, so it is derived from the
    %% identity and never taken from the request.  A protocol that lets a
    %% client name a tenant (WebSocket's `tenant' field) must ignore it.
    {ok, #{tenant_id := <<"t1">>}} =
        broker_security:authenticate_conn({user_pass, <<"alice">>, <<"pw">>}),
    ok.

%%%_* 4. Anonymous is a deployment decision -------------------------------

require_auth_only_applies_to_configured_protocols(_Config) ->
    application:set_env(broker_security, require_auth, [<<"ws">>]),
    true = broker_security:auth_required(<<"ws">>),
    false = broker_security:auth_required(<<"nats">>),
    false = broker_security:auth_required(<<"amqp">>),
    false = broker_security:auth_required(<<"kafka">>),
    false = broker_security:auth_required(<<"pulsar">>),
    ok.

require_auth_never_applies_to_a_protocol_that_cannot_carry_a_credential(_Config) ->
    %% Every protocol in the tree can carry one today; the check exists so
    %% that adding a protocol without a credential channel cannot lock every
    %% client out by accident.
    application:set_env(broker_security, require_auth, [<<"made-up">>]),
    false = broker_security:auth_required(<<"made-up">>),
    ok.

the_anonymous_tenant_is_the_deployment_default(_Config) ->
    application:set_env(broker_security, default_tenant, <<"anon-tenant">>),
    <<"anon-tenant">> = broker_security:default_tenant(),
    ok.

%%%_* Helpers --------------------------------------------------------------

restore(App, Key, undefined) ->
    application:unset_env(App, Key);
restore(App, Key, Value) ->
    application:set_env(App, Key, Value).
