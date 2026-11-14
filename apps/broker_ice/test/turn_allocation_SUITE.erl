%% @doc TURN allocations over real UDP (`broker_ice_udp' + `turn_allocation').
%%
%% The suite walks the RFC 5766 §6-§11 path a browser walks: challenge,
%% Allocate, CreatePermission, Send/Data, ChannelBind, ChannelData, Refresh,
%% and Refresh(0) to tear down. Every assertion is on bytes that crossed a
%% socket (the client's, or an independent peer socket's), so a change that
%% only works "in principle" fails here.
%%
%% Relay ports come from a three-port range (`?RELAY_MIN'..`?RELAY_MAX'), not
%% the production 49152-65535: that keeps the suite from colliding with
%% anything else on the host and makes the exhaustion cases cheap to provoke.
-module(turn_allocation_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% A deliberately tiny relay pool. Three ports is enough to hold two live
%% allocations plus the "one more must fail" case.
-define(RELAY_MIN, 49210).
-define(RELAY_MAX, 49212).
-define(REALM, <<"unified-mq">>).
-define(SECRET, <<"0123456789abcdef0123456789abcdef">>).
-define(USER, <<"alice">>).
-define(PASS, <<"alicepw">>).
-define(CHANNEL, 16#4001).

all() ->
    [allocate_returns_a_relay_address_in_the_pool,
     allocate_echoes_a_lifetime_and_is_signed,
     an_unauthenticated_allocate_is_challenged_with_realm_and_nonce,
     a_second_allocate_on_the_same_five_tuple_is_a_437,
     a_wrong_password_is_a_401,
     a_wrong_realm_is_a_441,
     a_forged_nonce_is_a_438,
     a_refresh_keeps_the_allocation_alive,
     refresh_zero_tears_the_allocation_down,
     a_refresh_without_an_allocation_is_a_437,
     an_expired_allocation_is_reclaimed,
     a_full_relay_pool_is_a_508,
     max_allocations_is_a_486,
     create_permission_allows_send_to_the_peer,
     without_a_permission_a_send_is_dropped,
     channel_bind_without_a_permission_is_a_403,
     channel_data_reaches_the_peer,
     consecutive_channel_data_frames_all_relay,
     peer_data_arrives_as_a_channel_data_frame,
     peer_data_without_a_binding_arrives_as_a_data_indication,
     peer_data_without_a_permission_is_dropped,
     a_channel_number_outside_the_range_is_a_400,
     channel_number_boundaries_are_inclusive,
     a_malformed_lifetime_on_refresh_is_a_400_and_the_allocation_survives,
     the_allocations_snapshot_names_client_and_relay_port].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(sasl),
    _ = application:load(broker_ice),
    {ok, _} = application:ensure_all_started(crypto),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    stop(),
    Saved = snapshot_env(),
    ok = application:set_env(broker_security, users, users()),
    ok = application:set_env(broker_security, acl, allow),
    ok = start_server(),
    [{saved_env, Saved} | Config].

end_per_testcase(_Case, Config) ->
    stop(),
    restore_env(?config(saved_env, Config)),
    ok.

users() ->
    [{?USER, #{password => ?PASS, tenant_id => <<"t0">>}},
     {<<"bob">>, #{password => <<"bobpw">>, tenant_id => <<"t0">>}}].

%% The baseline configuration; a case that needs different limits passes them
%% in `Overrides' and they win, so a case never has to re-set a key the fixture
%% would then overwrite.
start_server() ->
    start_server(#{}).

start_server(Overrides) ->
    Base = #{enabled => true,
             port => 0,
             turn_ip => {127, 0, 0, 1},
             realm => ?REALM,
             nonce_secret => ?SECRET,
             relay_port_range => {?RELAY_MIN, ?RELAY_MAX},
             max_allocations => 8,
             allocation_lifetime_s => 600,
             max_permissions_per_allocation => 4,
             max_channels_per_allocation => 2},
    maps:foreach(fun(K, V) -> ok = application:set_env(broker_ice, K, V) end,
                 maps:merge(Base, Overrides)),
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

%%%_* Allocate --------------------------------------------------------------

allocate_returns_a_relay_address_in_the_pool(_Config) ->
    Socket = ice_test_client:open(),
    try
        Alloc = allocate(Socket),
        ?assertEqual({127, 0, 0, 1}, maps:get(relay_ip, Alloc)),
        RelayPort = maps:get(relay_port, Alloc),
        ?assert(RelayPort >= ?RELAY_MIN),
        ?assert(RelayPort =< ?RELAY_MAX),
        %% The reflexive address is this client's own socket: that is how the
        %% client learns what to put in its ICE candidate.
        ?assertEqual({127, 0, 0, 1}, maps:get(mapped_ip, Alloc)),
        ?assertEqual(ice_test_client:port(Socket), maps:get(mapped_port, Alloc)),
        ?assertEqual(600, maps:get(lifetime, Alloc)),
        ?assertEqual(1, length(broker_ice_udp:allocations()))
    after
        ice_test_client:close(Socket)
    end.

allocate_echoes_a_lifetime_and_is_signed(_Config) ->
    %% An authenticated success carries MESSAGE-INTEGRITY: a client that cannot
    %% verify the response cannot trust the relay address it advertises.
    Socket = ice_test_client:open(),
    try
        Alloc = allocate(Socket),
        %% Refresh is the cheapest signed round trip that stays in the
        %% allocation, so use it to prove the signature is checkable.
        {ok, Msg} = ice_test_client:refresh(
                      Socket, broker_ice_udp:port(), Alloc, 300),
        Attrs = maps:get(attributes, Msg),
        ?assertEqual(300, stun_codec:lifetime(
                            stun_codec:attribute(
                              Attrs, stun_codec:attribute_type(lifetime)))),
        ?assertNotEqual(undefined,
                        stun_codec:attribute(
                          Attrs, stun_codec:attribute_type(message_integrity)))
    after
        ice_test_client:close(Socket)
    end.

an_unauthenticated_allocate_is_challenged_with_realm_and_nonce(_Config) ->
    %% The 401 must carry REALM + NONCE (RFC 5766 §5): without them the client
    %% has no way to compute MESSAGE-INTEGRITY and would retry forever.
    Socket = ice_test_client:open(),
    try
        Msg = #{method => allocate, class => request,
                transaction_id => ice_test_client:transaction_id(),
                attributes => [
                  {stun_codec:attribute_type(requested_transport),
                   <<17, 0, 0, 0>>}]},
        ok = gen_udp:send(Socket, {127, 0, 0, 1}, broker_ice_udp:port(),
                          stun_codec:add_fingerprint(stun_codec:encode(Msg))),
        {ok, Bin} = ice_test_client:recv(Socket),
        {ok, #{class := error, method := allocate, attributes := Attrs}} =
            stun_codec:decode(Bin),
        ?assertEqual({ok, 401, <<"Unauthorized">>},
                     stun_codec:decode_error_code(
                       stun_codec:attribute(
                         Attrs, stun_codec:attribute_type(error_code)))),
        ?assertEqual(?REALM,
                     stun_codec:attribute(
                       Attrs, stun_codec:attribute_type(realm))),
        Nonce = stun_codec:attribute(Attrs, stun_codec:attribute_type(nonce)),
        ?assert(is_binary(Nonce)),
        %% The nonce is verifiable against the node's secret and fresh.
        ?assertEqual(ok, turn_auth:check_nonce(?SECRET, Nonce,
                                               erlang:system_time(second))),
        %% A challenge never allocates anything.
        ?assertEqual([], broker_ice_udp:allocations())
    after
        ice_test_client:close(Socket)
    end.

a_second_allocate_on_the_same_five_tuple_is_a_437(_Config) ->
    %% RFC 5766 §6.2: the client must Refresh, not Allocate again. A second
    %% allocation on one 5-tuple would leak a relay port per retry.
    Socket = ice_test_client:open(),
    try
        _ = allocate(Socket),
        Port = broker_ice_udp:port(),
        %% Re-allocate with the same credential on the same 5-tuple.
        ?assertMatch({error, 437, _},
                     ice_test_client:allocate(
                       Socket, Port,
                       #{username => ?USER, password => ?PASS})),
        ?assertEqual(1, length(broker_ice_udp:allocations()))
    after
        ice_test_client:close(Socket)
    end.

a_wrong_password_is_a_401(_Config) ->
    Socket = ice_test_client:open(),
    try
        Result = ice_test_client:allocate(
                   Socket, broker_ice_udp:port(),
                   #{username => ?USER, password => <<"wrong">>}),
        ?assertMatch({error, 401, _}, Result),
        ?assertEqual([], broker_ice_udp:allocations())
    after
        ice_test_client:close(Socket)
    end.

a_wrong_realm_is_a_441(_Config) ->
    %% A client that answers our 401 with a different realm proves it holds a
    %% credential for somewhere else.
    Socket = ice_test_client:open(),
    try
        Port = broker_ice_udp:port(),
        {error, 401, Challenge} = raw_allocate(Socket, Port, []),
        Attrs = maps:get(attributes, Challenge),
        Nonce = stun_codec:attribute(Attrs, stun_codec:attribute_type(nonce)),
        Key = ice_test_client:key(?USER, <<"elsewhere">>, ?PASS),
        Signed = [{stun_codec:attribute_type(username), ?USER},
                  {stun_codec:attribute_type(realm), <<"elsewhere">>},
                  {stun_codec:attribute_type(nonce), Nonce}],
        ?assertMatch({error, 441, _},
                     raw_allocate(Socket, Port, Signed, Key)),
        ?assertEqual([], broker_ice_udp:allocations())
    after
        ice_test_client:close(Socket)
    end.

a_forged_nonce_is_a_438(_Config) ->
    Socket = ice_test_client:open(),
    try
        Port = broker_ice_udp:port(),
        Key = ice_test_client:key(?USER, ?REALM, ?PASS),
        Forged = turn_auth:nonce(<<"totally-different-secret">>,
                                 erlang:system_time(second)),
        Attrs = [{stun_codec:attribute_type(username), ?USER},
                 {stun_codec:attribute_type(realm), ?REALM},
                 {stun_codec:attribute_type(nonce), Forged}],
        ?assertMatch({error, 438, _}, raw_allocate(Socket, Port, Attrs, Key)),
        ?assertEqual([], broker_ice_udp:allocations())
    after
        ice_test_client:close(Socket)
    end.

%%%_* Refresh ---------------------------------------------------------------

a_refresh_keeps_the_allocation_alive(_Config) ->
    Socket = ice_test_client:open(),
    try
        Alloc = allocate(Socket),
        Port = broker_ice_udp:port(),
        {ok, Msg} = ice_test_client:refresh(Socket, Port, Alloc, 300),
        ?assertEqual(300,
                     stun_codec:lifetime(
                       stun_codec:attribute(
                         maps:get(attributes, Msg),
                         stun_codec:attribute_type(lifetime)))),
        %% A Refresh above the configured ceiling is clamped, never granted.
        {ok, High} = ice_test_client:refresh(Socket, Port, Alloc, 99999),
        ?assertEqual(600,
                     stun_codec:lifetime(
                       stun_codec:attribute(
                         maps:get(attributes, High),
                         stun_codec:attribute_type(lifetime)))),
        ?assertEqual(1, length(broker_ice_udp:allocations()))
    after
        ice_test_client:close(Socket)
    end.

refresh_zero_tears_the_allocation_down(_Config) ->
    Socket = ice_test_client:open(),
    try
        Alloc = allocate(Socket),
        Port = broker_ice_udp:port(),
        {ok, Msg} = ice_test_client:refresh(Socket, Port, Alloc, 0),
        ?assertEqual(0,
                     stun_codec:lifetime(
                       stun_codec:attribute(
                         maps:get(attributes, Msg),
                         stun_codec:attribute_type(lifetime)))),
        %% The success is sent before the process stops, and the index entry is
        %% gone shortly after via the monitor.
        ok = wait_until(fun() -> broker_ice_udp:allocations() =:= [] end),
        %% The relay port is reusable immediately.
        _ = allocate(Socket),
        ?assertEqual(1, length(broker_ice_udp:allocations()))
    after
        ice_test_client:close(Socket)
    end.

a_refresh_without_an_allocation_is_a_437(_Config) ->
    %% A Refresh from a 5-tuple that never allocated cannot be routed, and 437
    %% is the RFC's answer (the client must Allocate first).
    Socket = ice_test_client:open(),
    try
        Nonce = turn_auth:nonce(?SECRET, erlang:system_time(second)),
        Ctx = #{username => ?USER, realm => ?REALM, nonce => Nonce,
                key => ice_test_client:key(?USER, ?REALM, ?PASS)},
        ?assertMatch({error, 437, _},
                     ice_test_client:refresh(Socket, broker_ice_udp:port(),
                                             Ctx, 600))
    after
        ice_test_client:close(Socket)
    end.

an_expired_allocation_is_reclaimed(_Config) ->
    %% A one-second lifetime: the timer must end the allocation without a
    %% Refresh, and its relay port must come back into the pool.
    stop(),
    ok = start_server(#{allocation_lifetime_s => 1}),
    Socket = ice_test_client:open(),
    try
        Alloc = allocate(Socket),
        ?assertEqual(1, length(broker_ice_udp:allocations())),
        ok = wait_until(fun() -> broker_ice_udp:allocations() =:= [] end, 5000),
        ?assertEqual({127, 0, 0, 1}, maps:get(relay_ip, Alloc))
    after
        ice_test_client:close(Socket)
    end.

%%%_* Capacity --------------------------------------------------------------

a_full_relay_pool_is_a_508(_Config) ->
    %% Three relay ports, `max_allocations' 8: the pool runs out first, and the
    %% fourth distinct client must get 508 rather than a port that is in use.
    Sockets = [ice_test_client:open() || _ <- lists:seq(1, 3)],
    try
        Allocs = [allocate(S) || S <- Sockets],
        Ports = [maps:get(relay_port, A) || A <- Allocs],
        ?assertEqual(3, length(lists:usort(Ports))),
        %% Every port in the range is distinct and inside it.
        ?assertEqual(lists:sort([?RELAY_MIN, ?RELAY_MIN + 1, ?RELAY_MAX]),
                     lists:sort(Ports)),
        Extra = ice_test_client:open(),
        try
            ?assertMatch({error, 508, _},
                         ice_test_client:allocate(
                           Extra, broker_ice_udp:port(),
                           #{username => ?USER, password => ?PASS}))
        after
            ice_test_client:close(Extra)
        end,
        ?assertEqual(3, length(broker_ice_udp:allocations()))
    after
        [ice_test_client:close(S) || S <- Sockets]
    end.

max_allocations_is_a_486(_Config) ->
    %% A wide pool with a small quota: the quota is what answers, with 486.
    stop(),
    %% A wide pool with a small quota, so the quota is what answers.
    ok = start_server(#{relay_port_range => {49210, 49230},
                        max_allocations => 2}),
    Sockets = [ice_test_client:open() || _ <- lists:seq(1, 2)],
    Extra = ice_test_client:open(),
    try
        [_ = allocate(S) || S <- Sockets],
        ?assertMatch({error, 486, _},
                     ice_test_client:allocate(
                       Extra, broker_ice_udp:port(),
                       #{username => ?USER, password => ?PASS})),
        ?assertEqual(2, length(broker_ice_udp:allocations()))
    after
        [ice_test_client:close(S) || S <- [Extra | Sockets]]
    end.

%%%_* Permissions and Send --------------------------------------------------

create_permission_allows_send_to_the_peer(_Config) ->
    %% The allocation relays to a peer socket only after a permission exists
    %% for that peer *IP* (RFC 5766 §9), any port.
    Socket = ice_test_client:open(),
    Peer = peer_socket(),
    try
        Alloc = allocate(Socket),
        Port = broker_ice_udp:port(),
        PeerAddr = peer_addr(Peer),
        {ok, _} = ice_test_client:create_permission(Socket, Port, Alloc,
                                                   [PeerAddr]),
        ok = ice_test_client:send_indication(Socket, Port, PeerAddr,
                                             <<"hello peer">>),
        ?assertEqual({ok, <<"hello peer">>}, peer_recv(Peer))
    after
        ice_test_client:close(Socket),
        ice_test_client:close(Peer)
    end.

without_a_permission_a_send_is_dropped(_Config) ->
    %% A Send indication is an indication: there is no error response, so the
    %% only observable outcome is that nothing reaches the peer.
    Socket = ice_test_client:open(),
    Peer = peer_socket(),
    try
        allocate(Socket),
        ok = ice_test_client:send_indication(Socket, broker_ice_udp:port(),
                                             peer_addr(Peer), <<"nope">>),
        ?assertEqual({error, timeout}, gen_udp:recv(Peer, 0, 400))
    after
        ice_test_client:close(Socket),
        ice_test_client:close(Peer)
    end.

channel_bind_without_a_permission_is_a_403(_Config) ->
    Socket = ice_test_client:open(),
    Peer = peer_socket(),
    try
        Alloc = allocate(Socket),
        Result = ice_test_client:channel_bind(
                   Socket, broker_ice_udp:port(), Alloc,
                   ?CHANNEL, peer_addr(Peer)),
        ?assertMatch({error, 403, _}, Result)
    after
        ice_test_client:close(Socket),
        ice_test_client:close(Peer)
    end.

%%%_* ChannelBind and ChannelData -------------------------------------------

channel_data_reaches_the_peer(_Config) ->
    %% After ChannelBind the client may use the 4-byte ChannelData framing,
    %% which is the whole point: it avoids re-sending XOR-PEER-ADDRESS per packet.
    Socket = ice_test_client:open(),
    Peer = peer_socket(),
    try
        Alloc = allocate(Socket),
        Port = broker_ice_udp:port(),
        PeerAddr = peer_addr(Peer),
        {ok, _} = ice_test_client:create_permission(Socket, Port, Alloc,
                                                    [PeerAddr]),
        {ok, _} = ice_test_client:channel_bind(Socket, Port, Alloc, ?CHANNEL,
                                               PeerAddr),
        ok = ice_test_client:channel_data(Socket, Port, ?CHANNEL,
                                          <<"over the channel">>),
        ?assertEqual({ok, <<"over the channel">>}, peer_recv(Peer))
    after
        ice_test_client:close(Socket),
        ice_test_client:close(Peer)
    end.

consecutive_channel_data_frames_all_relay(_Config) ->
    %% A relay only works if the allocation survives *more than one* frame.
    %% `handle_cast' feeds `forward_to_peer''s return value straight into the
    %% gen_server state; if that call returned `ok' (what `gen_udp:send'
    %% yields) the `#st{}' record would be replaced by the atom `ok' and the
    %% *second* ChannelData would crash the allocation with `badrecord'. So the
    %% real assertion is "N frames in, N payloads out", and the allocation is
    %% still listed afterwards.
    Socket = ice_test_client:open(),
    Peer = peer_socket(),
    try
        Alloc = allocate(Socket),
        Port = broker_ice_udp:port(),
        PeerAddr = peer_addr(Peer),
        {ok, _} = ice_test_client:create_permission(Socket, Port, Alloc,
                                                    [PeerAddr]),
        {ok, _} = ice_test_client:channel_bind(Socket, Port, Alloc, ?CHANNEL,
                                               PeerAddr),
        [ice_test_client:channel_data(Socket, Port, ?CHANNEL,
                                      list_to_binary(integer_to_list(N)))
         || N <- lists:seq(1, 5)],
        Received = [peer_recv(Peer) || _ <- lists:seq(1, 5)],
        ?assertEqual([{ok, <<"1">>}, {ok, <<"2">>}, {ok, <<"3">>},
                       {ok, <<"4">>}, {ok, <<"5">>}], Received),
        %% The allocation is still registered: it did not self-destruct after
        %% the first frame.
        ?assertEqual(1, length(broker_ice_udp:allocations()))
    after
        ice_test_client:close(Socket),
        ice_test_client:close(Peer)
    end.

peer_data_arrives_as_a_channel_data_frame(_Config) ->
    %% Peer -> client in the bound direction uses ChannelData too, with the
    %% same channel number.
    Socket = ice_test_client:open(),
    Peer = peer_socket(),
    try
        Alloc = allocate(Socket),
        Port = broker_ice_udp:port(),
        PeerAddr = peer_addr(Peer),
        {ok, _} = ice_test_client:create_permission(Socket, Port, Alloc,
                                                    [PeerAddr]),
        {ok, _} = ice_test_client:channel_bind(Socket, Port, Alloc, ?CHANNEL,
                                               PeerAddr),
        {ok, {RelayIp, RelayPort}} = relay_addr(Alloc),
        _ = gen_udp:send(Peer, RelayIp, RelayPort, <<"from the peer">>),
        ?assertEqual({ok, {channel_data, ?CHANNEL, <<"from the peer">>}},
                     ice_test_client:recv_data(Socket))
    after
        ice_test_client:close(Socket),
        ice_test_client:close(Peer)
    end.

peer_data_without_a_binding_arrives_as_a_data_indication(_Config) ->
    %% A permission alone is enough for peer -> client: the payload arrives as
    %% a Data indication with XOR-PEER-ADDRESS.
    Socket = ice_test_client:open(),
    Peer = peer_socket(),
    try
        Alloc = allocate(Socket),
        Port = broker_ice_udp:port(),
        PeerAddr = peer_addr(Peer),
        {ok, _} = ice_test_client:create_permission(Socket, Port, Alloc,
                                                    [PeerAddr]),
        {ok, {RelayIp, RelayPort}} = relay_addr(Alloc),
        _ = gen_udp:send(Peer, RelayIp, RelayPort, <<"indicated">>),
        ?assertEqual({ok, {data_indication, <<"indicated">>}},
                     ice_test_client:recv_data(Socket))
    after
        ice_test_client:close(Socket),
        ice_test_client:close(Peer)
    end.

peer_data_without_a_permission_is_dropped(_Config) ->
    %% The relay socket is open to the world; without a permission it must be a
    %% black hole, not an open reflector.
    Socket = ice_test_client:open(),
    Peer = peer_socket(),
    try
        Alloc = allocate(Socket),
        {ok, {RelayIp, RelayPort}} = relay_addr(Alloc),
        _ = gen_udp:send(Peer, RelayIp, RelayPort, <<"unsolicited">>),
        ?assertEqual(timeout, ice_test_client:recv(Socket, 400)),
        %% And the client hears nothing back either.
        ?assertEqual({error, timeout}, gen_udp:recv(Peer, 0, 200))
    after
        ice_test_client:close(Socket),
        ice_test_client:close(Peer)
    end.

a_channel_number_outside_the_range_is_a_400(_Config) ->
    %% 0x3FFF is below the RFC 5766 §11 range; 0x8000 is above it.
    Socket = ice_test_client:open(),
    Peer = peer_socket(),
    try
        Alloc = allocate(Socket),
        Port = broker_ice_udp:port(),
        {ok, _} = ice_test_client:create_permission(Socket, Port, Alloc,
                                                    [peer_addr(Peer)]),
        lists:foreach(
          fun(Channel) ->
                  Tid = ice_test_client:transaction_id(),
                  Attrs = [{stun_codec:attribute_type(channel_number),
                            <<Channel:16, 0:16>>},
                           {stun_codec:attribute_type(xor_peer_address),
                            ice_test_client:xaddr(peer_addr(Peer), Tid)}],
                  %% Signed with the real credential, so the 400 is about the
                  %% channel number and not about a missing nonce.
                  Result = ice_test_client:request(Socket, Port, channel_bind,
                                                   Attrs, Alloc),
                  ?assertMatch({error, 400, _}, Result)
          end, [16#3FFF, 16#8000])
    after
        ice_test_client:close(Socket),
        ice_test_client:close(Peer)
    end.

%% The complement of the case above: 0x4000 and 0x7FFF -- the exact inclusive
%% edges of the RFC 5766 §11.2 range -- are *accepted*. The existing case pins
%% the two just-outside values as 400; this pins the two boundary values as ok,
%% so a future off-by-one in `channel_of/1' (e.g. `> ?CHANNEL_MIN' instead of
%% `>=') fails here rather than silently narrowing the usable range. Each edge
%% gets its own peer: a peer may hold only one channel (RFC 5766 §11.2), so
%% binding two channels to one peer would legitimately 400 on the second.
channel_number_boundaries_are_inclusive(_Config) ->
    lists:foreach(
      fun(Channel) ->
              Socket = ice_test_client:open(),
              Peer = peer_socket(),
              try
                  Alloc = allocate(Socket),
                  Port = broker_ice_udp:port(),
                  PeerAddr = peer_addr(Peer),
                  %% Permission first, so the range gate (not the permission
                  %% gate) is what answers: without it both edges are a 403.
                  {ok, _} = ice_test_client:create_permission(
                              Socket, Port, Alloc, [PeerAddr]),
                  Tid = ice_test_client:transaction_id(),
                  Attrs = [{stun_codec:attribute_type(channel_number),
                            <<Channel:16, 0:16>>},
                           {stun_codec:attribute_type(xor_peer_address),
                            ice_test_client:xaddr(PeerAddr, Tid)}],
                  ?assertMatch({ok, _},
                               ice_test_client:request(Socket, Port, channel_bind,
                                                       Attrs, Alloc))
              after
                  ice_test_client:close(Socket),
                  ice_test_client:close(Peer)
              end
      end, [16#4000, 16#7FFF]).

%% A LIFETIME attribute whose value is not the 4 bytes the codec expects (here 3)
%% is refused with 400 and the allocation survives: the listener never crashes
%% and the client can keep using (and Refreshing) the allocation. This pins the
%% robustness half of the refresh path -- a malformed attribute must not tear
%% down the relay or the shared UDP front end. (A non-4-byte LIFETIME cannot be
%% produced by a conformant client, so this is a hostile/garbage-input probe,
%% not a spec branch.)
a_malformed_lifetime_on_refresh_is_a_400_and_the_allocation_survives(_Config) ->
    Socket = ice_test_client:open(),
    Peer = peer_socket(),
    try
        Alloc = allocate(Socket),
        Port = broker_ice_udp:port(),
        Before = length(broker_ice_udp:allocations()),
        ?assertEqual(1, Before),
        %% Permission first so the only odd thing about this Refresh is the
        %% malformed LIFETIME value.
        {ok, _} = ice_test_client:create_permission(Socket, Port, Alloc,
                                                    [peer_addr(Peer)]),
        Attrs = [{stun_codec:attribute_type(lifetime), <<0, 0, 1>>}],
        ?assertMatch({error, 400, _},
                     ice_test_client:request(Socket, Port, refresh, Attrs, Alloc)),
        %% The allocation is still there and the listener still answers.
        ?assertEqual(Before, length(broker_ice_udp:allocations())),
        ?assert(is_pid(whereis(broker_ice_udp))),
        %% A well-formed Refresh on the same allocation still works.
        ?assertMatch({ok, _}, ice_test_client:refresh(Socket, Port, Alloc, 300))
    after
        ice_test_client:close(Socket),
        ice_test_client:close(Peer)
    end.

%%%_* Snapshot --------------------------------------------------------------

the_allocations_snapshot_names_client_and_relay_port(_Config) ->
    %% The snapshot is what an operator (and `broker_admin') sees: it must name
    %% the username and the relay port, and never the key.
    Socket = ice_test_client:open(),
    try
        Alloc = allocate(Socket),
        [Entry] = broker_ice_udp:allocations(),
        ?assertEqual(?USER, maps:get(username, Entry)),
        ?assertEqual(maps:get(relay_port, Alloc), maps:get(relay_port, Entry)),
        ?assertEqual({{127, 0, 0, 1}, ice_test_client:port(Socket), udp},
                     maps:get(client, Entry)),
        %% No key material in the map at all.
        ?assertEqual(nomatch,
                     binary:match(iolist_to_binary(io_lib:format("~p", [Entry])),
                                  maps:get(key, Alloc)))
    after
        ice_test_client:close(Socket)
    end.

%%%_* Helpers ---------------------------------------------------------------

allocate(Socket) ->
    {ok, Alloc} = ice_test_client:allocate(
                    Socket, broker_ice_udp:port(),
                    #{username => ?USER, password => ?PASS}),
    Alloc.

relay_addr(Alloc) ->
    {ok, {maps:get(relay_ip, Alloc), maps:get(relay_port, Alloc)}}.

%% One Allocate attempt with explicit credential attributes.
raw_allocate(Socket, Port, Attrs) ->
    raw_allocate(Socket, Port, Attrs, undefined).

raw_allocate(Socket, Port, Attrs, Key) ->
    Tid = ice_test_client:transaction_id(),
    All = Attrs ++ [{stun_codec:attribute_type(requested_transport),
                     <<17, 0, 0, 0>>}],
    Msg = #{method => allocate, class => request, transaction_id => Tid,
            attributes => All},
    Base = stun_codec:encode(Msg),
    Wire = case Key of
               undefined -> stun_codec:add_fingerprint(Base);
               _ -> stun_codec:add_fingerprint(stun_codec:add_integrity(Key, Base))
           end,
    ok = gen_udp:send(Socket, {127, 0, 0, 1}, Port, Wire),
    await(Socket, Tid).

await(Socket, Tid) ->
    case ice_test_client:recv(Socket) of
        timeout ->
            timeout;
        {ok, Bin} ->
            case stun_codec:decode(Bin) of
                {ok, #{transaction_id := Tid} = Msg} ->
                    case maps:get(class, Msg) of
                        success -> {ok, Msg};
                        error ->
                            {ok, Code, _} = stun_codec:decode_error_code(
                                              stun_codec:attribute(
                                                maps:get(attributes, Msg),
                                                stun_codec:attribute_type(
                                                  error_code))),
                            {error, Code, Msg};
                        Other -> {Other, Msg}
                    end;
                _ ->
                    await(Socket, Tid)
            end
    end.

%% An independent socket, standing in for a media peer.
peer_socket() ->
    {ok, Socket} = gen_udp:open(0, [binary, {active, false}]),
    Socket.

peer_addr(Socket) ->
    {ok, Port} = inet:port(Socket),
    {{127, 0, 0, 1}, Port}.

peer_recv(Socket) ->
    case gen_udp:recv(Socket, 0, 3000) of
        {ok, {_Ip, _Port, Data}} -> {ok, Data};
        {error, timeout} -> timeout
    end.

wait_until(Fun) ->
    wait_until(Fun, 3000).

wait_until(Fun, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    wait_loop(Fun, Deadline).

wait_loop(Fun, Deadline) ->
    case Fun() of
        true ->
            ok;
        false ->
            case erlang:monotonic_time(millisecond) > Deadline of
                true -> {error, timeout};
                false ->
                    timer:sleep(20),
                    wait_loop(Fun, Deadline)
            end
    end.
