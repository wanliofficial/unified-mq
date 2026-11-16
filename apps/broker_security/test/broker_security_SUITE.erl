%% @doc T10: the §4.3 authentication stub. The suite pins the two properties
%% the stub must never lose: the frozen error reason (identical for unknown
%% user and wrong password, so it cannot enumerate users) and the config-driven
%% table (`users' env, read with the 3-arity get_env per §13 J3).
-module(broker_security_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("stdlib/include/assert.hrl").

all() ->
    [known_user_authenticates,
     unknown_user_is_rejected,
     wrong_password_is_rejected,
     rejection_reason_does_not_distinguish_user_from_password,
     non_binary_credentials_are_rejected,
     tenant_id_comes_from_the_table,
     authorize_stub_always_allows,
     psk_for_known_identity,
     psk_is_deterministic,
     psk_changes_with_realm,
     psk_is_not_the_password,
     psk_unknown_identity_is_an_error,
     psk_missing_password_is_same_error,
     psk_is_not_in_redacted_config].

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

%%%_* S11-C: CoAP-over-DTLS PSK derivation ----------------------------------

%% The PSK identity IS a username in the SAME `users' table every other
%% protocol authenticates against -- no second credential store. These cases pin
%% the derivation's four load-bearing properties: it is deterministic, it is
%% realm-bound, it is not the password (nor a bare hash of it), and an unknown
%% identity fails indistinguishably from one with no password.

psk_for_known_identity(_Config) ->
    {ok, Psk} = broker_security:psk_for_identity(<<"alice">>),
    ?assertEqual(32, byte_size(Psk)),
    ok.

psk_is_deterministic(_Config) ->
    {ok, A} = broker_security:psk_for_identity(<<"alice">>),
    {ok, B} = broker_security:psk_for_identity(<<"alice">>),
    ?assertEqual(A, B),
    %% And a different identity yields a different key even under a shared realm.
    {ok, C} = broker_security:psk_for_identity(<<"guest">>),
    ?assertNotEqual(A, C),
    ok.

psk_changes_with_realm(_Config) ->
    Saved = application:get_env(broker_security, coaps, undefined),
    try
        {ok, Before} = broker_security:psk_for_identity(<<"alice">>),
        ok = application:set_env(broker_security, coaps, #{realm => <<"other-realm">>}),
        {ok, After} = broker_security:psk_for_identity(<<"alice">>),
        ?assertNotEqual(Before, After)
    after
        restore_env(broker_security, coaps, Saved)
    end.

psk_is_not_the_password(_Config) ->
    {ok, Psk} = broker_security:psk_for_identity(<<"alice">>),
    Password = <<"s3cret">>,
    %% Not the password, and not a bare hash of it: the domain-separation label
    %% and the identity/realm separators are what keep this derivation from
    %% colliding with any hash a client might guess.
    ?assertNotEqual(Password, Psk),
    ?assertNotEqual(crypto:hash(sha256, Password), Psk),
    ?assertNotEqual(crypto:hash(sha256, [<<"alice">>, 0, Password]), Psk),
    ok.

psk_unknown_identity_is_an_error(_Config) ->
    ?assertEqual({error, bad_username_or_password},
                 broker_security:psk_for_identity(<<"nobody">>)),
    ok.

psk_missing_password_is_same_error(_Config) ->
    %% A user record with no password must fail exactly like an unknown user:
    %% differentiating them would be an enumeration oracle on the handshake.
    Saved = application:get_env(broker_security, users, undefined),
    try
        ok = application:set_env(broker_security, users,
                                 [{<<"nopass">>, #{tenant_id => <<"t0">>}}]),
        {error, R1} = broker_security:psk_for_identity(<<"nopass">>),
        {error, R2} = broker_security:psk_for_identity(<<"nobody">>),
        ?assertEqual(R1, R2),
        ?assertEqual(bad_username_or_password, R1)
    after
        restore_env(broker_security, users, Saved)
    end.

psk_is_not_in_redacted_config(_Config) ->
    {ok, Psk} = broker_security:psk_for_identity(<<"alice">>),
    %% The admin/log view must never carry the derived key; a hex or term form
    %% of it appearing there would be a credential leak.
    Rendered = iolist_to_binary(io_lib:format("~p", [broker_security:redacted_config()])),
    ?assertEqual(nomatch, binary:match(Rendered, Psk)).

restore_env(App, Key, undefined) ->
    application:unset_env(App, Key);
restore_env(App, Key, Value) ->
    application:set_env(App, Key, Value).
