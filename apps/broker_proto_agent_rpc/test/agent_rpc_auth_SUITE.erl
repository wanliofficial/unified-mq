%% @doc Credential extraction and `broker_security' wiring.
%%
%% The suite runs against the real `broker_security' functions: the point is
%% that the adapter does not reimplement identity or tenant derivation, so a
%% fake here would test nothing.
-module(agent_rpc_auth_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [bearer_credential,
     bearer_scheme_is_case_insensitive,
     bearer_without_token_is_none,
     api_key_credential,
     both_headers_agree,
     both_headers_disagree,
     no_credential,
     authenticate_valid_token,
     authenticate_unknown_token,
     authorize_allow,
     authorize_denies_foreign_tenant,
     anonymous_uses_default_tenant,
     challenge_names_bearer].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(broker_security),
    Old = [{K, application:get_env(broker_security, K)}
           || K <- [users, acl, default_tenant]],
    ok = application:set_env(broker_security, users,
                             [{<<"alice">>, #{token => <<"alice-token">>,
                                              password => <<"pw">>,
                                              tenant_id => <<"t1">>}},
                              {<<"bob">>, #{token => <<"bob-token">>,
                                            tenant_id => <<"t2">>}}]),
    ok = application:set_env(broker_security, acl, allow),
    ok = application:set_env(broker_security, default_tenant, <<"t0">>),
    [{old_env, Old} | Config].

end_per_suite(Config) ->
    lists:foreach(
      fun({Key, {ok, Value}}) -> application:set_env(broker_security, Key, Value);
         ({Key, undefined}) -> application:unset_env(broker_security, Key)
      end, ?config(old_env, Config)),
    ok.

bearer_credential(_Config) ->
    ?assertEqual({ok, {token, <<"abc">>}},
                 agent_rpc_auth:credential(#{<<"authorization">> => <<"Bearer abc">>})).

bearer_scheme_is_case_insensitive(_Config) ->
    ?assertEqual({ok, {token, <<"abc">>}},
                 agent_rpc_auth:credential(#{<<"authorization">> => <<"bearer abc">>})),
    ?assertEqual({ok, {token, <<"abc">>}},
                 agent_rpc_auth:credential(#{<<"authorization">> => <<"BEARER abc">>})).

bearer_without_token_is_none(_Config) ->
    ?assertEqual(none,
                 agent_rpc_auth:credential(#{<<"authorization">> => <<"Bearer">>})),
    ?assertEqual(none,
                 agent_rpc_auth:credential(#{<<"authorization">> => <<"Basic abc">>})).

api_key_credential(_Config) ->
    ?assertEqual({ok, {token, <<"k1">>}},
                 agent_rpc_auth:credential(#{<<"x-api-key">> => <<"k1">>})).

both_headers_agree(_Config) ->
    ?assertEqual({ok, {token, <<"same">>}},
                 agent_rpc_auth:credential(
                   #{<<"authorization">> => <<"Bearer same">>,
                     <<"x-api-key">> => <<"same">>})).

both_headers_disagree(_Config) ->
    %% Picking one would let a proxy strip the other and silently change the
    %% identity; refuse instead.
    ?assertEqual({error, ambiguous},
                 agent_rpc_auth:credential(
                   #{<<"authorization">> => <<"Bearer a">>,
                     <<"x-api-key">> => <<"b">>})).

no_credential(_Config) ->
    ?assertEqual(none, agent_rpc_auth:credential(#{})).

authenticate_valid_token(_Config) ->
    ?assertEqual({ok, #{principal => <<"alice">>, tenant_id => <<"t1">>}},
                 agent_rpc_auth:authenticate({token, <<"alice-token">>})).

authenticate_unknown_token(_Config) ->
    %% The failure is `unauthorized' with no detail: "no such user" and "wrong
    %% secret" must stay indistinguishable.
    ?assertEqual({error, unauthorized},
                 agent_rpc_auth:authenticate({token, <<"nope">>})).

authorize_allow(_Config) ->
    Identity = #{principal => <<"alice">>, tenant_id => <<"t1">>},
    ?assertEqual(allow,
                 agent_rpc_auth:authorize(Identity, publish, <<"$a2a/task/x">>)).

authorize_denies_foreign_tenant(_Config) ->
    ok = application:set_env(broker_security, acl,
                             [#{effect => allow, principal => <<"*">>,
                                action => <<"*">>, pattern => <<"t1/*">>}]),
    try
        Own = #{principal => <<"alice">>, tenant_id => <<"t1">>},
        ?assertEqual(allow, agent_rpc_auth:authorize(Own, publish, <<"t1/a">>)),
        %% The identity's tenant is fixed by the user record; asking for another
        %% tenant is refused before any rule is consulted.
        ?assertEqual(deny,
                     broker_security:authorize(<<"alice">>,
                                               #{action => publish,
                                                 tenant_id => <<"t2">>,
                                                 destination => <<"t1/a">>}))
    after
        application:set_env(broker_security, acl, allow)
    end.

anonymous_uses_default_tenant(_Config) ->
    ?assertEqual(#{principal => <<"anonymous">>, tenant_id => <<"t0">>},
                 agent_rpc_auth:anonymous()).

challenge_names_bearer(_Config) ->
    ?assertEqual(<<"Bearer realm=\"a2a\"">>, agent_rpc_auth:challenge()).
