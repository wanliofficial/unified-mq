%% @doc TURN long-term credential authentication (`turn_auth').
%%
%% The interesting part of this suite is what it *cannot* see: the password
%% never appears in a log line or in any term `turn_auth' hands back. One case
%% installs a logger handler and asserts that over a full successful and
%% failing authentication, so a future `?LOG_DEBUG("key=~p", [Key])' fails
%% here rather than in a customer's log store.
%%
%% `broker_security:long_term_key/2' is pinned against the RFC 5766 §4 vector
%% (`MD5(user ":" realm ":" password)') because a second implementation of the
%% server has to derive the identical key from the same account.
-module(turn_auth_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("kernel/include/logger.hrl").

-define(REALM, <<"unified-mq">>).
-define(SECRET, <<"0123456789abcdef0123456789abcdef">>).
-define(TID, <<9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9>>).

all() ->
    [key_is_md5_of_user_realm_password,
     long_term_key_matches_the_same_vector,
     long_term_key_of_an_unknown_user_is_an_error,
     a_challenge_and_response_roundtrip,
     a_wrong_password_is_a_401,
     an_unknown_user_is_indistinguishable_from_a_wrong_password,
     a_missing_message_integrity_asks_for_a_challenge,
     a_wrong_realm_is_a_441,
     a_stale_nonce_is_a_438,
     a_forged_nonce_is_a_438,
     a_missing_nonce_with_integrity_is_a_400,
     a_nonce_from_the_future_within_the_skew_is_accepted,
     an_acl_deny_is_a_403,
     the_turn_action_is_what_acl_sees,
     no_password_or_key_reaches_the_log,
     the_disabled_app_needs_no_realm].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(sasl),
    _ = application:load(broker_security),
    _ = application:load(broker_ice),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    Saved = #{users => application:get_env(broker_security, users, undefined),
              acl => application:get_env(broker_security, acl, undefined),
              default_tenant =>
                  application:get_env(broker_security, default_tenant, undefined)},
    ok = application:set_env(broker_security, users, users()),
    ok = application:set_env(broker_security, acl, allow),
    [{saved_env, Saved} | Config].

end_per_testcase(_Case, Config) ->
    restore(?config(saved_env, Config)),
    ok.

users() ->
    [{<<"alice">>, #{password => <<"alicepw">>, tenant_id => <<"t0">>}},
     {<<"bob">>, #{password => <<"bobpw">>, tenant_id => <<"t1">>}}].

restore(Saved) ->
    [case Value of
         undefined -> application:unset_env(App, Key);
         _ -> application:set_env(App, Key, Value)
     end || {Key, Value} <- maps:to_list(Saved), App <- [broker_security]],
    ok.

%%%_* Key derivation --------------------------------------------------------

key_is_md5_of_user_realm_password(_Config) ->
    Expected = crypto:hash(md5, <<"alice:unified-mq:alicepw">>),
    ?assertEqual(Expected, turn_auth:key(<<"alice">>, ?REALM, <<"alicepw">>)),
    ?assertEqual(Expected,
                 ice_test_client:key(<<"alice">>, ?REALM, <<"alicepw">>)),
    %% The realm is part of the key: the same account in another realm is a
    %% different credential, which is the whole point of a long-term credential.
    ?assertNotEqual(Expected,
                    turn_auth:key(<<"alice">>, <<"other">>, <<"alicepw">>)).

long_term_key_matches_the_same_vector(_Config) ->
    ?assertEqual({ok, turn_auth:key(<<"alice">>, ?REALM, <<"alicepw">>)},
                 broker_security:long_term_key(<<"alice">>, ?REALM)),
    %% bob's record is reachable the same way; the tenant is irrelevant here.
    ?assertEqual({ok, turn_auth:key(<<"bob">>, ?REALM, <<"bobpw">>)},
                 broker_security:long_term_key(<<"bob">>, ?REALM)).

long_term_key_of_an_unknown_user_is_an_error(_Config) ->
    %% An unknown user and a record with no password must both answer the same
    %% thing, and it must not be a key.
    ?assertEqual({error, bad_username_or_password},
                 broker_security:long_term_key(<<"nobody">>, ?REALM)),
    ok = application:set_env(broker_security, users,
                             [{<<"nopw">>, #{tenant_id => <<"t0">>}}]),
    ?assertEqual({error, bad_username_or_password},
                 broker_security:long_term_key(<<"nopw">>, ?REALM)),
    ?assertEqual({error, bad_username_or_password},
                 broker_security:long_term_key(<<"nopw">>, 42)).

%%%_* authenticate/3 --------------------------------------------------------

a_challenge_and_response_roundtrip(_Config) ->
    Key = ice_test_client:key(<<"alice">>, ?REALM, <<"alicepw">>),
    Nonce = turn_auth:nonce(?SECRET, unix_now()),
    Wire = signed_request(<<"alice">>, Nonce, Key),
    ?assertMatch({ok, #{principal := <<"alice">>, key := Key}},
                 turn_auth:authenticate(decode(Wire), Wire, cfg())),
    %% The principal's tenant comes from the same identity table.
    {ok, #{tenant_id := Tenant}} =
        turn_auth:authenticate(decode(Wire), Wire, cfg()),
    ?assertEqual(<<"t0">>, Tenant).

a_wrong_password_is_a_401(_Config) ->
    WrongKey = ice_test_client:key(<<"alice">>, ?REALM, <<"nope">>),
    Wire = signed_request(<<"alice">>, turn_auth:nonce(?SECRET, unix_now()), WrongKey),
    ?assertMatch({error, {401, _}},
                 turn_auth:authenticate(decode(Wire), Wire, cfg())).

an_unknown_user_is_indistinguishable_from_a_wrong_password(_Config) ->
    %% Same code, same reason phrase: an attacker learns nothing about which
    %% user names exist.
    WrongKey = turn_auth:key(<<"ghost">>, ?REALM, <<"whatever">>),
    Wire = signed_request(<<"ghost">>, turn_auth:nonce(?SECRET, unix_now()), WrongKey),
    {error, Unknown} = turn_auth:authenticate(decode(Wire), Wire, cfg()),
    BadPw = signed_request(<<"alice">>, turn_auth:nonce(?SECRET, unix_now()),
                           ice_test_client:key(<<"alice">>, ?REALM, <<"x">>)),
    {error, BadPassword} = turn_auth:authenticate(decode(BadPw), BadPw, cfg()),
    ?assertEqual(BadPassword, Unknown).

a_missing_message_integrity_asks_for_a_challenge(_Config) ->
    %% RFC 5766 §5: a request without MESSAGE-INTEGRITY is the first packet of
    %% every TURN conversation, so it earns 401 + REALM + NONCE whether or not
    %% it named a user. Returning 400 here would leave a compliant client with
    %% no way to learn the realm.
    WithUser = #{method => allocate, class => request, transaction_id => ?TID,
                 attributes => [{stun_codec:attribute_type(username),
                                 <<"alice">>}]},
    Wire = stun_codec:add_fingerprint(stun_codec:encode(WithUser)),
    ?assertMatch({error, {401, _}},
                 turn_auth:authenticate(decode(Wire), Wire, cfg())),
    Bare = stun_codec:add_fingerprint(
             stun_codec:encode(#{method => allocate, class => request,
                                 transaction_id => ?TID, attributes => []})),
    ?assertMatch({error, {401, _}},
                 turn_auth:authenticate(decode(Bare), Bare, cfg())),
    %% Claiming integrity without the attributes it protects is malformed.
    Key = ice_test_client:key(<<"alice">>, ?REALM, <<"alicepw">>),
    NoUser = stun_codec:add_fingerprint(
               stun_codec:add_integrity(
                 Key, stun_codec:encode(
                        #{method => allocate, class => request,
                          transaction_id => ?TID,
                          attributes => [{stun_codec:attribute_type(realm),
                                          ?REALM}]}))),
    ?assertMatch({error, {400, _}},
                 turn_auth:authenticate(decode(NoUser), NoUser, cfg())).

a_wrong_realm_is_a_441(_Config) ->
    Key = ice_test_client:key(<<"alice">>, <<"evil">>, <<"alicepw">>),
    Wire = signed_request(<<"alice">>, turn_auth:nonce(?SECRET, unix_now()), Key,
                          <<"evil">>),
    ?assertMatch({error, {441, _}},
                 turn_auth:authenticate(decode(Wire), Wire, cfg())).

a_stale_nonce_is_a_438(_Config) ->
    %% One second past the documented maximum age.
    Old = unix_now() - turn_auth:nonce_lifetime_s() - 1,
    Key = ice_test_client:key(<<"alice">>, ?REALM, <<"alicepw">>),
    Wire = signed_request(<<"alice">>, turn_auth:nonce(?SECRET, Old), Key),
    ?assertMatch({error, {438, _}},
                 turn_auth:authenticate(decode(Wire), Wire, cfg())),
    %% The nonce lifetime is the documented hour, not an accident of the code.
    ?assertEqual(3600, turn_auth:nonce_lifetime_s()).

a_forged_nonce_is_a_438(_Config) ->
    Key = ice_test_client:key(<<"alice">>, ?REALM, <<"alicepw">>),
    %% Signed with a different secret: the timestamp is plausible but the MAC
    %% is not ours.
    Forged = turn_auth:nonce(<<"another-secret-entirely">>, unix_now()),
    Wire = signed_request(<<"alice">>, Forged, Key),
    ?assertMatch({error, {438, _}},
                 turn_auth:authenticate(decode(Wire), Wire, cfg())),
    %% And garbage is refused the same way instead of crashing the listener.
    Garbage = signed_request(<<"alice">>, <<"not-a-nonce">>, Key),
    ?assertMatch({error, {438, _}},
                 turn_auth:authenticate(decode(Garbage), Garbage, cfg())).

a_missing_nonce_with_integrity_is_a_400(_Config) ->
    %% RFC 5766 §5: once a message carries USERNAME and MESSAGE-INTEGRITY, a
    %% missing REALM or NONCE makes it malformed (400), not stale (438) --
    %% there is no nonce to be stale.
    Key = ice_test_client:key(<<"alice">>, ?REALM, <<"alicepw">>),
    Wire = signed_request(<<"alice">>, undefined, Key),
    ?assertMatch({error, {400, _}},
                 turn_auth:authenticate(decode(Wire), Wire, cfg())),
    %% REALM present, NONCE absent: the same 400.
    Tid = ?TID,
    Msg = #{method => allocate, class => request, transaction_id => Tid,
            attributes => [{stun_codec:attribute_type(username), <<"alice">>},
                           {stun_codec:attribute_type(realm), ?REALM}]},
    NoNonce = stun_codec:add_fingerprint(
                stun_codec:add_integrity(Key, stun_codec:encode(Msg))),
    ?assertMatch({error, {400, _}},
                 turn_auth:authenticate(decode(NoNonce), NoNonce, cfg())).

a_nonce_from_the_future_within_the_skew_is_accepted(_Config) ->
    %% A node whose clock stepped back must not lock its clients out.
    Key = ice_test_client:key(<<"alice">>, ?REALM, <<"alicepw">>),
    Wire = signed_request(<<"alice">>, turn_auth:nonce(?SECRET, unix_now() + 30), Key),
    ?assertMatch({ok, _}, turn_auth:authenticate(decode(Wire), Wire, cfg())),
    Far = signed_request(<<"alice">>, turn_auth:nonce(?SECRET, unix_now() + 600), Key),
    ?assertMatch({error, {438, _}},
                 turn_auth:authenticate(decode(Far), Far, cfg())).

%%%_* ACL -------------------------------------------------------------------

an_acl_deny_is_a_403(_Config) ->
    %% A credential can be valid and still not be allowed to allocate.
    ok = application:set_env(broker_security, acl,
                             [#{effect => deny, principal => <<"bob">>,
                                action => <<"*">>, pattern => <<"*">>},
                              #{effect => allow, principal => <<"*">>,
                                action => <<"*">>, pattern => <<"*">>}]),
    Key = ice_test_client:key(<<"bob">>, ?REALM, <<"bobpw">>),
    Wire = signed_request(<<"bob">>, turn_auth:nonce(?SECRET, unix_now()), Key),
    ?assertMatch({error, {403, _}},
                 turn_auth:authenticate(decode(Wire), Wire, cfg())),
    %% alice is unaffected by bob's rule.
    AliceKey = ice_test_client:key(<<"alice">>, ?REALM, <<"alicepw">>),
    Alice = signed_request(<<"alice">>, turn_auth:nonce(?SECRET, unix_now()), AliceKey),
    ?assertMatch({ok, _},
                 turn_auth:authenticate(decode(Alice), Alice, cfg())).

the_turn_action_is_what_acl_sees(_Config) ->
    %% Each TURN method has its own action atom, so a deployment can allow
    %% Allocate and deny ChannelBind without touching the identity table.
    ok = application:set_env(broker_security, acl,
                             [#{effect => deny, principal => <<"*">>,
                                action => <<"turn_channel">>, pattern => <<"*">>},
                              #{effect => allow, principal => <<"*">>,
                                action => <<"*">>, pattern => <<"*">>}]),
    Key = ice_test_client:key(<<"alice">>, ?REALM, <<"alicepw">>),
    Nonce = turn_auth:nonce(?SECRET, unix_now()),
    Allocate = signed_request(<<"alice">>, Nonce, Key,
                              ?REALM, ?TID, allocate),
    ?assertMatch({ok, _},
                 turn_auth:authenticate(decode(Allocate), Allocate, cfg())),
    Bind = signed_request(<<"alice">>, Nonce, Key,
                          ?REALM, ?TID, channel_bind),
    ?assertMatch({error, {403, _}},
                 turn_auth:authenticate(decode(Bind), Bind,
                                        (cfg())#{action := turn_channel})),
    %% The destination every action is judged against is the single TURN
    %% pseudo-topic, so one ACL pattern covers the whole app.
    ?assertEqual(<<"$ice/turn">>, maps:get(destination, cfg())).

%%%_* Secrets must not leak -------------------------------------------------

no_password_or_key_reaches_the_log(_Config) ->
    Key = ice_test_client:key(<<"alice">>, ?REALM, <<"alicepw">>),
    %% The handler must be able to see *everything* the app might emit, so
    %% raise the primary level to debug for the duration. A probe line proves
    %% the capture is live -- without it this case would pass by capturing
    %% nothing and asserting on an empty string.
    Primary = logger:get_primary_config(),
    ok = logger:set_primary_config(level, debug),
    ok = logger:add_handler(turn_auth_capture, ?MODULE,
                            #{config => self(), level => debug}),
    try
        ?LOG_DEBUG("turn_auth_SUITE probe ~p", [probe_marker]),
        %% A success and two failures: the paths that touch the password most.
        Wire = signed_request(<<"alice">>, turn_auth:nonce(?SECRET, unix_now()), Key),
        {ok, _} = turn_auth:authenticate(decode(Wire), Wire, cfg()),
        Bad = signed_request(<<"alice">>, turn_auth:nonce(?SECRET, unix_now()),
                             ice_test_client:key(<<"alice">>, ?REALM, <<"x">>)),
        {error, _} = turn_auth:authenticate(decode(Bad), Bad, cfg()),
        Ghost = signed_request(<<"ghost">>, turn_auth:nonce(?SECRET, unix_now()),
                               turn_auth:key(<<"ghost">>, ?REALM, <<"p">>)),
        {error, _} = turn_auth:authenticate(decode(Ghost), Ghost, cfg()),
        Log = collect([]),
        %% Live capture, or the rest of this case proves nothing.
        ?assertNotEqual(nomatch, search(Log, <<"probe_marker">>)),
        %% The literal passwords and the derived key must all be absent.
        ?assertEqual(nomatch, search(Log, <<"alicepw">>)),
        ?assertEqual(nomatch, search(Log, <<"bobpw">>)),
        ?assertEqual(nomatch, search(Log, <<?SECRET/binary>>)),
        ?assertEqual(nomatch, search(Log, binary_to_list(Key)))
    after
        _ = logger:remove_handler(turn_auth_capture),
        ok = logger:set_primary_config(level, maps:get(level, Primary, notice))
    end.

collect(Acc) ->
    receive
        {log, _Level, Msg} -> collect([format(Msg) | Acc])
    after 0 ->
            iolist_to_binary(lists:reverse(Acc))
    end.

format({Format, Args}) -> unicode:characters_to_binary(io_lib:format(Format, Args));
format(Msg) when is_binary(Msg) -> Msg;
format(Msg) when is_list(Msg) -> unicode:characters_to_binary(Msg);
format(Msg) -> unicode:characters_to_binary(io_lib:format("~p", [Msg])).

search(Haystack, Needle) when is_list(Needle) ->
    search(Haystack, list_to_binary(Needle));
search(Haystack, Needle) ->
    binary:match(Haystack, Needle).

%% A logger handler: every event this node logs lands in the test process's
%% mailbox. `logger' calls `log/2' on the handler module.
log(#{level := Level, msg := Msg}, #{config := Pid}) ->
    Pid ! {log, Level, Msg},
    ok.

%%%_* The disabled default ----------------------------------------------

the_disabled_app_needs_no_realm(_Config) ->
    %% `enabled => false' is the ship default: the app must validate without a
    %% realm, a turn_ip or a relay range, because none of them is used.
    ok = application:set_env(broker_ice, enabled, false),
    ok = application:unset_env(broker_ice, realm),
    ok = application:unset_env(broker_ice, turn_ip),
    ?assertEqual(ok, broker_ice_app:validate()),
    ?assertEqual({ok, #{enabled => false}}, broker_ice_app:config()).

%%%_* Helpers ---------------------------------------------------------------

cfg() ->
    #{realm => ?REALM, secret => ?SECRET, action => turn_allocate,
      destination => <<"$ice/turn">>}.

unix_now() ->
    erlang:system_time(second).

decode(Wire) ->
    {ok, Msg} = stun_codec:decode(Wire),
    Msg.

signed_request(Username, Nonce, Key) ->
    signed_request(Username, Nonce, Key, ?REALM).

signed_request(Username, Nonce, Key, Realm) ->
    signed_request(Username, Nonce, Key, Realm, ?TID, allocate).

signed_request(Username, Nonce, Key, Realm, Tid, Method) ->
    Attrs = [{stun_codec:attribute_type(username), Username},
             {stun_codec:attribute_type(realm), Realm}]
        ++ case Nonce of
               undefined -> [];
               _ -> [{stun_codec:attribute_type(nonce), Nonce}]
           end,
    Msg = #{method => Method, class => request, transaction_id => Tid,
            attributes => Attrs},
    stun_codec:add_fingerprint(stun_codec:add_integrity(Key,
                                                        stun_codec:encode(Msg))).
