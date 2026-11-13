%% @doc Boot-time validation of the signaling front end (S10-A).
%%
%% `broker_proto_webrtc_app:validate/0' is the loud-fail gate: a bad
%% configuration must stop the app rather than start something that silently
%% accepts unauthenticated traffic or refuses every handshake. Each case here
%% sets exactly one knob and asserts the specific refusal atom, so a
%% regression names itself.
%%
%% The auth switch is this app's own env key (ADR-028 §D5), exactly like
%% `broker_proto_a2a' and `broker_proto_mcp'. Two cases below pin that: reading
%% `broker_security.require_auth' instead made the *shipped* release refuse to
%% boot, because that key is a list of protocol names and is `[]' by default.
-module(webrtc_validate_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [defaults_are_sane,
     the_shipped_security_config_does_not_block_boot,
     a_loopback_listener_without_auth_is_allowed,
     a_wide_listener_without_auth_is_refused,
     a_non_boolean_require_auth_is_refused,
     a_negative_auth_timeout_is_refused,
     a_zero_idle_timeout_is_refused,
     an_infinite_idle_timeout_is_allowed,
     a_zero_frame_cap_is_refused,
     an_empty_subprotocol_list_is_refused,
     a_non_binary_subprotocol_is_refused,
     no_listener_is_a_legal_default].

init_per_testcase(_Case, Config) ->
    reset_env(),
    Config.

end_per_testcase(_Case, _Config) ->
    reset_env(),
    ok.

reset_env() ->
    application:unset_env(broker_security, require_auth),
    application:unset_env(broker_security, users),
    application:unset_env(broker_security, acl),
    application:unset_env(broker_proto_webrtc, require_auth),
    application:unset_env(broker_proto_webrtc, listeners),
    application:unset_env(broker_proto_webrtc, subprotocols),
    application:unset_env(broker_proto_webrtc, auth_timeout_ms),
    application:unset_env(broker_proto_webrtc, idle_timeout_ms),
    application:unset_env(broker_proto_webrtc, max_frame_bytes),
    application:unset_env(broker_proto_webrtc, binary_frames),
    ok.

%% The shipped defaults must pass validation: a release with no webrtc config
%% boots, it just does not listen.
defaults_are_sane(_Config) ->
    ?assertEqual(ok, broker_proto_webrtc_app:validate()).

%% `broker_security.require_auth' is the list of credential-capable protocols
%% ("which protocols must authenticate"), not a boolean. Reading it here as a
%% boolean made a default release (`[]') fail to boot, and an operator who
%% wrote `[<<"ws">>]' fail the same way. The app must ignore that key entirely.
the_shipped_security_config_does_not_block_boot(_Config) ->
    ok = application:set_env(broker_security, require_auth, []),
    ?assertEqual(ok, broker_proto_webrtc_app:validate()),
    ok = application:set_env(broker_security, require_auth, [<<"ws">>, <<"mqtt">>]),
    ?assertEqual(ok, broker_proto_webrtc_app:validate()).

a_loopback_listener_without_auth_is_allowed(_Config) ->
    ok = application:set_env(broker_proto_webrtc, require_auth, false),
    ok = application:set_env(broker_proto_webrtc, listeners,
                             [#{port => 0, ip => {127, 0, 0, 1}}]),
    ?assertEqual(ok, broker_proto_webrtc_app:validate()).

a_wide_listener_without_auth_is_refused(_Config) ->
    ok = application:set_env(broker_proto_webrtc, require_auth, false),
    ok = application:set_env(broker_proto_webrtc, listeners,
                             [#{port => 8084, ip => {0, 0, 0, 0}}]),
    ?assertEqual({error, {insecure_listener, 8084, {0, 0, 0, 0}}},
                 broker_proto_webrtc_app:validate()).

%% A list here is the classic copy-paste from `broker_security'. It must be
%% refused by name, not quietly interpreted as `true'.
a_non_boolean_require_auth_is_refused(_Config) ->
    ok = application:set_env(broker_proto_webrtc, require_auth, [<<"ws">>]),
    ?assertEqual({error, {invalid_require_auth, [<<"ws">>]}},
                 broker_proto_webrtc_app:validate()).

a_negative_auth_timeout_is_refused(_Config) ->
    ok = application:set_env(broker_proto_webrtc, auth_timeout_ms, -1),
    ?assertEqual({error, {invalid_auth_timeout_ms, -1}},
                 broker_proto_webrtc_app:validate()).

a_zero_idle_timeout_is_refused(_Config) ->
    ok = application:set_env(broker_proto_webrtc, idle_timeout_ms, 0),
    ?assertEqual({error, {invalid_idle_timeout_ms, 0}},
                 broker_proto_webrtc_app:validate()).

%% The plan documents `infinity' as the way to switch the idle window off.
an_infinite_idle_timeout_is_allowed(_Config) ->
    ok = application:set_env(broker_proto_webrtc, idle_timeout_ms, infinity),
    ?assertEqual(ok, broker_proto_webrtc_app:validate()).

a_zero_frame_cap_is_refused(_Config) ->
    ok = application:set_env(broker_proto_webrtc, max_frame_bytes, 0),
    ?assertEqual({error, {invalid_max_frame_bytes, 0}},
                 broker_proto_webrtc_app:validate()).

an_empty_subprotocol_list_is_refused(_Config) ->
    ok = application:set_env(broker_proto_webrtc, subprotocols, []),
    ?assertEqual({error, {invalid_subprotocols, empty}},
                 broker_proto_webrtc_app:validate()).

a_non_binary_subprotocol_is_refused(_Config) ->
    ok = application:set_env(broker_proto_webrtc, subprotocols, [webrtc_v1]),
    ?assertEqual({error, {invalid_subprotocols, [webrtc_v1]}},
                 broker_proto_webrtc_app:validate()).

%% The shipped default is `listeners => []': no socket, no port. This is the
%% S10-A gate posture (R-T5) -- the app ships, the port stays closed.
no_listener_is_a_legal_default(_Config) ->
    ?assertEqual([], application:get_env(broker_proto_webrtc, listeners, [])),
    ?assertEqual(ok, broker_proto_webrtc_app:validate()).
