%% @doc The STUN face of `broker_ice_udp' over real UDP datagrams.
%%
%% This suite never calls a codec helper to answer a question the server must
%% answer: it opens a real socket, sends real bytes, and decodes what arrives.
%% The coverage is the STUN half of ADR-029 §D2 -- Binding (the one
%% unauthenticated method), the four refusals a malformed request can earn
%% (400, 420, 442 and "silently dropped"), and the property that every response
%% ends with a valid FINGERPRINT.
%%
%% TURN is in `turn_allocation_SUITE'; this one deliberately never allocates,
%% so a failure here cannot be an allocation bug.
-module(stun_server_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(TID, <<7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7>>).
-define(REALM, <<"unified-mq">>).
-define(SECRET, <<"0123456789abcdef0123456789abcdef">>).

all() ->
    [binding_answers_xor_mapped_address,
     binding_is_the_same_for_every_transaction_id,
     every_response_carries_a_valid_fingerprint,
     a_binding_request_with_a_stale_transaction_is_answered_once,
     an_unknown_comprehension_required_attribute_earns_420,
     a_comprehension_optional_attribute_is_ignored,
     an_unknown_method_is_a_400,
     a_non_udp_requested_transport_is_a_442,
     a_malformed_datagram_is_dropped_silently,
     a_bad_magic_cookie_is_dropped_silently,
     a_channel_data_frame_without_an_allocation_is_dropped,
     the_disabled_app_opens_no_socket,
     the_listener_port_is_configurable].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(sasl),
    _ = application:load(broker_ice),
    {ok, _} = application:ensure_all_started(crypto),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(Case, Config) ->
    stop(),
    Saved = snapshot_env(),
    ok = application:set_env(broker_security, users, users()),
    ok = application:set_env(broker_security, acl, allow),
    case Case of
        the_disabled_app_opens_no_socket ->
            ok = application:set_env(broker_ice, enabled, false);
        _ ->
            ok = start_server()
    end,
    [{saved_env, Saved} | Config].

end_per_testcase(_Case, Config) ->
    stop(),
    restore_env(?config(saved_env, Config)),
    ok.

users() ->
    [{<<"alice">>, #{password => <<"alicepw">>, tenant_id => <<"t0">>}}].

start_server() ->
    ok = application:set_env(broker_ice, enabled, true),
    %% Port 0: the kernel picks a free port and `broker_ice_udp:port/0' reports
    %% it. Jamming 3478 would make the suite unusable on a host that already
    %% runs a STUN server, and would collide with the other suites here.
    ok = application:set_env(broker_ice, port, 0),
    ok = application:set_env(broker_ice, turn_ip, {127, 0, 0, 1}),
    ok = application:set_env(broker_ice, realm, ?REALM),
    ok = application:set_env(broker_ice, nonce_secret, ?SECRET),
    %% A two-port range: this suite allocates nothing, but the range is still
    %% validated at boot and a wide default would make a leaked allocation in a
    %% neighbouring suite look normal.
    ok = application:set_env(broker_ice, relay_port_range, {49200, 49201}),
    {ok, _} = application:ensure_all_started(broker_ice),
    ok.

stop() ->
    _ = application:stop(broker_ice),
    case whereis(broker_ice_udp) of
        undefined -> ok;
        Pid -> exit(Pid, kill)
    end,
    ok.

snapshot_env() ->
    Keys = [enabled, port, turn_ip, hostname, realm, nonce_secret,
            relay_port_range, max_allocations, allocation_lifetime_s,
            max_permissions_per_allocation, max_channels_per_allocation,
            recbuf_bytes],
    {ice, [{K, application:get_env(broker_ice, K, undefined)} || K <- Keys],
     [{K, application:get_env(broker_security, K, undefined)}
      || K <- [users, acl, default_tenant]]}.

restore_env({ice, Ice, Security}) ->
    [case V of
         undefined -> application:unset_env(App, K);
         _ -> application:set_env(App, K, V)
     end || {App, List} <- [{broker_ice, Ice}, {broker_security, Security}],
            {K, V} <- List],
    ok.

%%%_* Binding ---------------------------------------------------------------

binding_answers_xor_mapped_address(_Config) ->
    Socket = ice_test_client:open(),
    try
        Port = broker_ice_udp:port(),
        {ok, Msg} = ice_test_client:binding(Socket, Port),
        ?assertEqual(binding, maps:get(method, Msg)),
        ?assertEqual(success, maps:get(class, Msg)),
        Attrs = maps:get(attributes, Msg),
        Value = stun_codec:attribute(
                  Attrs, stun_codec:attribute_type(xor_mapped_address)),
        %% The address the server reports is the one the kernel saw: our own
        %% loopback socket. That is the entire purpose of Binding.
        ?assertEqual({ok, {{127, 0, 0, 1}, ice_test_client:port(Socket)}},
                     stun_codec:decode_xor_address(Value)),
        %% Binding is not authenticated: a bootstrap probe has no credential.
        ?assertEqual(undefined,
                     stun_codec:attribute(
                       Attrs, stun_codec:attribute_type(message_integrity)))
    after
        ice_test_client:close(Socket)
    end.

binding_is_the_same_for_every_transaction_id(_Config) ->
    %% The transaction id is echoed, not cached: two requests from one socket
    %% both answer, with their own ids.
    Socket = ice_test_client:open(),
    try
        Port = broker_ice_udp:port(),
        {ok, A} = ice_test_client:binding(Socket, Port),
        {ok, B} = ice_test_client:binding(Socket, Port),
        ?assertNotEqual(maps:get(transaction_id, A),
                        maps:get(transaction_id, B)),
        ?assertEqual(12, byte_size(maps:get(transaction_id, A)))
    after
        ice_test_client:close(Socket)
    end.

every_response_carries_a_valid_fingerprint(_Config) ->
    %% FINGERPRINT is the last attribute of every response the server sends.
    %% A client that validates it (browsers do) would drop the message
    %% otherwise, and the failure is invisible without this assertion.
    Socket = ice_test_client:open(),
    try
        Port = broker_ice_udp:port(),
        ok = gen_udp:send(Socket, {127, 0, 0, 1}, Port,
                          stun_codec:add_fingerprint(
                            stun_codec:encode(
                              #{method => binding, class => request,
                                transaction_id => ?TID, attributes => []}))),
        {ok, Wire} = ice_test_client:recv(Socket),
        ?assert(stun_codec:check_fingerprint(Wire)),
        %% Appending anything after FINGERPRINT invalidates it, which is the
        %% property that makes the check meaningful.
        ?assertNot(stun_codec:check_fingerprint(<<Wire/binary, 0, 0, 0, 0>>))
    after
        ice_test_client:close(Socket)
    end.

a_binding_request_with_a_stale_transaction_is_answered_once(_Config) ->
    %% One datagram in, one datagram out. A reply loop would show up here as a
    %% second packet.
    Socket = ice_test_client:open(),
    try
        Port = broker_ice_udp:port(),
        Msg = #{method => binding, class => request, transaction_id => ?TID,
                attributes => []},
        ok = gen_udp:send(Socket, {127, 0, 0, 1}, Port,
                          stun_codec:add_fingerprint(stun_codec:encode(Msg))),
        {ok, First} = ice_test_client:recv(Socket),
        {ok, Decoded} = stun_codec:decode(First),
        ?assertEqual(?TID, maps:get(transaction_id, Decoded)),
        ?assertEqual(timeout, ice_test_client:recv(Socket, 300))
    after
        ice_test_client:close(Socket)
    end.

%%%_* Refusals --------------------------------------------------------------

an_unknown_comprehension_required_attribute_earns_420(_Config) ->
    %% RFC 8489 §6.3.1: an attribute below 0x8000 that we do not implement must
    %% be answered 420 with UNKNOWN-ATTRIBUTES naming it -- dropping it would
    %% let a client believe an option took effect when it did not.
    Socket = ice_test_client:open(),
    try
        Port = broker_ice_udp:port(),
        Unknown = 16#0033,
        Msg = #{method => binding, class => request, transaction_id => ?TID,
                attributes => [{Unknown, <<1, 2, 3, 4>>}]},
        ok = gen_udp:send(Socket, {127, 0, 0, 1}, Port,
                          stun_codec:add_fingerprint(stun_codec:encode(Msg))),
        {ok, Bin} = ice_test_client:recv(Socket),
        {ok, #{class := error, method := binding,
               attributes := Attrs} = Resp} = stun_codec:decode(Bin),
        ?assertEqual(binding, maps:get(method, Resp)),
        ?assertEqual({ok, 420, <<"Unknown Attribute">>},
                     stun_codec:decode_error_code(
                       stun_codec:attribute(
                         Attrs, stun_codec:attribute_type(error_code)))),
        ?assertEqual(<<Unknown:16>>,
                     stun_codec:attribute(
                       Attrs, stun_codec:attribute_type(unknown_attributes)))
    after
        ice_test_client:close(Socket)
    end.

a_comprehension_optional_attribute_is_ignored(_Config) ->
    %% SOFTWARE (0x8022) is above the boundary: unknown-but-optional attributes
    %% are skipped, never refused.
    Socket = ice_test_client:open(),
    try
        Port = broker_ice_udp:port(),
        Msg = #{method => binding, class => request, transaction_id => ?TID,
                attributes => [{16#8022, <<"test-client/1.0">>}]},
        ok = gen_udp:send(Socket, {127, 0, 0, 1}, Port,
                          stun_codec:add_fingerprint(stun_codec:encode(Msg))),
        {ok, Bin} = ice_test_client:recv(Socket),
        {ok, #{class := success}} = stun_codec:decode(Bin)
    after
        ice_test_client:close(Socket)
    end.

an_unknown_method_is_a_400(_Config) ->
    %% Method 0x0A has no definition in STUN or TURN. A request for it is
    %% malformed, and the error echoes the method so the client can match it.
    Socket = ice_test_client:open(),
    try
        Port = broker_ice_udp:port(),
        %% encode_type(10, 0) = 0x000A.
        Wire = <<0:2, 16#000A:14, 0:16, 16#2112A442:32, ?TID/binary>>,
        ok = gen_udp:send(Socket, {127, 0, 0, 1}, Port,
                          stun_codec:add_fingerprint(Wire)),
        {ok, Bin} = ice_test_client:recv(Socket),
        {ok, #{class := error, transaction_id := ?TID, attributes := Attrs}} =
            stun_codec:decode(Bin),
        ?assertEqual({ok, 400, <<"Bad Request">>},
                     stun_codec:decode_error_code(
                       stun_codec:attribute(
                         Attrs, stun_codec:attribute_type(error_code))))
    after
        ice_test_client:close(Socket)
    end.

a_non_udp_requested_transport_is_a_442(_Config) ->
    %% RFC 5766 §6.2: REQUESTED-TRANSPORT other than UDP (17) is unsupported,
    %% and this server says so before asking for a credential -- there is no
    %% point challenging for a transport it will never relay.
    Socket = ice_test_client:open(),
    try
        Port = broker_ice_udp:port(),
        lists:foreach(
          fun(Protocol) ->
                  Msg = #{method => allocate, class => request,
                          transaction_id => ?TID,
                          attributes => [
                            {stun_codec:attribute_type(requested_transport),
                             <<Protocol, 0, 0, 0>>}]},
                  ok = gen_udp:send(Socket, {127, 0, 0, 1}, Port,
                                    stun_codec:add_fingerprint(
                                      stun_codec:encode(Msg))),
                  {ok, Bin} = ice_test_client:recv(Socket),
                  {ok, #{class := error, method := allocate,
                         attributes := Attrs}} = stun_codec:decode(Bin),
                  ?assertEqual({ok, 442, <<"Unsupported Transport Protocol">>},
                               stun_codec:decode_error_code(
                                 stun_codec:attribute(
                                   Attrs,
                                   stun_codec:attribute_type(error_code))))
          end, [6, 33]),
        %% No credential attributes were needed: 442 came first.
        ?assertEqual([], broker_ice_udp:allocations())
    after
        ice_test_client:close(Socket)
    end.

a_malformed_datagram_is_dropped_silently(_Config) ->
    %% A datagram that is not STUN at all must not produce a response: RFC 8489
    %% §7.3 says a server that cannot parse a request stays quiet, and a reply
    %% here would let an attacker map the service with junk.
    Socket = ice_test_client:open(),
    try
        Port = broker_ice_udp:port(),
        lists:foreach(
          fun(<<>>) -> ok;
             (Bin) ->
                  ok = gen_udp:send(Socket, {127, 0, 0, 1}, Port, Bin),
                  ?assertEqual(timeout, ice_test_client:recv(Socket, 200))
          end,
          [<<>>, <<1, 2, 3>>, binary:copy(<<0>>, 21),
           <<0:2, 1:14, 4:16, 16#FFFFFFFF:32, ?TID/binary, 0:32>>,
           %% A truncated attribute inside an otherwise valid header.
           <<0:2, 1:14, 8:16, 16#2112A442:32, ?TID/binary, 0, 13, 0, 8, 0:32>>]),
        %% The listener is still alive and answering afterwards.
        {ok, _} = ice_test_client:binding(Socket, Port)
    after
        ice_test_client:close(Socket)
    end.

a_bad_magic_cookie_is_dropped_silently(_Config) ->
    Socket = ice_test_client:open(),
    try
        Port = broker_ice_udp:port(),
        %% A plausible header with the wrong cookie is not STUN.
        Wire = <<0:2, 1:14, 0:16, 16#DEADBEEF:32, ?TID/binary>>,
        ok = gen_udp:send(Socket, {127, 0, 0, 1}, Port, Wire),
        ?assertEqual(timeout, ice_test_client:recv(Socket, 300))
    after
        ice_test_client:close(Socket)
    end.

a_channel_data_frame_without_an_allocation_is_dropped(_Config) ->
    %% A ChannelData frame from a 5-tuple that never allocated has nowhere to
    %% go. Dropping it (rather than answering 437) is what RFC 5766 §11.4
    %% implies: ChannelData has no transaction and therefore no error path.
    Socket = ice_test_client:open(),
    try
        Port = broker_ice_udp:port(),
        ok = gen_udp:send(Socket, {127, 0, 0, 1}, Port,
                          <<16#4000:16, 4:16, 0:32>>),
        ?assertEqual(timeout, ice_test_client:recv(Socket, 300)),
        ?assertEqual([], broker_ice_udp:allocations())
    after
        ice_test_client:close(Socket)
    end.

%%%_* Boot posture ----------------------------------------------------------

the_disabled_app_opens_no_socket(_Config) ->
    %% The ship default (ADR-029 §D2): the app is in every release and owns
    %% nothing. No listener process at all is the strongest form of that.
    ?assertEqual(undefined, whereis(broker_ice_udp)),
    ?assertEqual(undefined, whereis(turn_allocation_sup)),
    %% `validate/0' stays `ok' without a realm or a turn_ip, because a disabled
    %% app uses neither.
    ?assertEqual(ok, broker_ice_app:validate()),
    ?assertEqual({ok, #{enabled => false}}, broker_ice_app:config()),
    %% Enabling a bad config is still a loud failure.
    ok = application:set_env(broker_ice, enabled, true),
    ?assertEqual({error, {missing_config, realm}}, broker_ice_app:validate()),
    ok = application:set_env(broker_ice, realm, ?REALM),
    ?assertEqual({error, {missing_config, turn_ip}}, broker_ice_app:validate()),
    ok = application:set_env(broker_ice, turn_ip, {127, 0, 0, 1}),
    ok = application:set_env(broker_ice, relay_port_range, {1, 2}),
    ?assertMatch({error, {invalid_relay_port_range, {1, 2}}},
                 broker_ice_app:validate()).

the_listener_port_is_configurable(_Config) ->
    %% The suite configures `port => 0', but `port/0' must report the port the
    %% kernel actually bound -- reporting the configured `0' would leave every
    %% client with nowhere to connect.
    ?assertEqual(0, application:get_env(broker_ice, port, undefined)),
    Port = broker_ice_udp:port(),
    ?assert(is_integer(Port)),
    ?assert(Port > 0),
    Socket = ice_test_client:open(),
    try
        ?assertMatch({ok, _}, ice_test_client:binding(Socket, Port))
    after
        ice_test_client:close(Socket)
    end.
