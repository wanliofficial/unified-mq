%% @doc T10: the §4.3 authentication stub. The suite pins the two properties
%% the stub must never lose: the frozen error reason (identical for unknown
%% user and wrong password, so it cannot enumerate users) and the config-driven
%% table (`users' env, read with the 3-arity get_env per §13 J3).
-module(broker_security_SUITE).

-compile([export_all, nowarn_export_all]).

all() ->
    [known_user_authenticates,
     unknown_user_is_rejected,
     wrong_password_is_rejected,
     rejection_reason_does_not_distinguish_user_from_password,
     non_binary_credentials_are_rejected,
     tenant_id_comes_from_the_table,
     authorize_stub_always_allows].

init_per_suite(Config) ->
    Saved = application:get_env(broker_security, users, undefined),
    application:set_env(broker_security, users,
                        [{<<"guest">>, #{password => <<"guest">>, tenant_id => <<"t0">>}},
                         {<<"alice">>, #{password => <<"s3cret">>, tenant_id => <<"t1">>}}]),
    [{saved_users, Saved} | Config].

end_per_suite(Config) ->
    %% J10 lesson: end_per_suite is exported only because init_per_suite is;
    %% it must exist and restore, or the rewritten table leaks to later suites.
    case proplists:get_value(saved_users, Config) of
        undefined -> ok;
        Saved -> ok = application:set_env(broker_security, users, Saved)
    end,
    ok.

%%%_* Tests ---------------------------------------------------------------

known_user_authenticates(_Config) ->
    {ok, <<"guest">>} = broker_security:authenticate(<<"guest">>, <<"guest">>),
    {ok, <<"alice">>} = broker_security:authenticate(<<"alice">>, <<"s3cret">>),
    ok.

unknown_user_is_rejected(_Config) ->
    {error, bad_username_or_password} =
        broker_security:authenticate(<<"nobody">>, <<"guest">>),
    ok.

wrong_password_is_rejected(_Config) ->
    {error, bad_username_or_password} =
        broker_security:authenticate(<<"alice">>, <<"alice">>),
    ok.

rejection_reason_does_not_distinguish_user_from_password(_Config) ->
    %% The §4.3 contract is one indistinguishable reason for both failures;
    %% an "unknown user" variant here would be a user-enumeration oracle.
    {error, R1} = broker_security:authenticate(<<"nobody">>, <<"x">>),
    {error, R2} = broker_security:authenticate(<<"alice">>, <<"wrong">>),
    R1 = R2,
    ok.

non_binary_credentials_are_rejected(_Config) ->
    %% A CONNECT frame with no password field decodes to undefined; the guard
    %% must reject it instead of crashing the conn process.
    {error, bad_username_or_password} = broker_security:authenticate(<<"alice">>, undefined),
    {error, bad_username_or_password} = broker_security:authenticate(undefined, <<"x">>),
    {error, bad_username_or_password} = broker_security:authenticate([], []),
    ok.

tenant_id_comes_from_the_table(_Config) ->
    <<"t0">> = broker_security:tenant_id(<<"guest">>),
    <<"t1">> = broker_security:tenant_id(<<"alice">>),
    %% Unknown principal (hand-built, e.g. in a test) stays total.
    <<"default">> = broker_security:tenant_id(<<"ghost">>),
    ok.

authorize_stub_always_allows(_Config) ->
    allow = broker_security:authorize(<<"guest">>, #{action => publish, topic => <<"a/b">>}),
    allow = broker_security:authorize(<<"whatever">>, anything),
    ok.
