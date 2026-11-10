%% @doc `mqtt_transport_tcp': mailbox normalisation and socket operations.
%%
%% This module is the seam the MQTT state machine calls instead of touching
%% `gen_tcp'/`ssl' directly, so what it must get right is small but exact:
%%
%%  * `decode_msg/2' must answer only for the socket it was handed. A node runs
%%    more than one listener; feeding listener A's bytes into listener B's
%%    connection would be silent corruption, not a crash, so every clause matches
%%    the handle rather than ignoring it.
%%  * `other' must survive as `other'. The FSM's mailbox also carries
%%    subscription deliveries, poll ticks and the `connect' handoff; a transport
%%    that claimed them would break the state machine rather than the transport.
%%  * Both socket flavours are served by this one module, which is what makes
%%    MQTT over TLS work (before S8 the FSM wrote to ssl sockets with
%%    `gen_tcp:send/2'). `ssl' sockets are still `inet' sockets, but only `ssl'
%%    exports `setopts/2' -- the two are NOT interchangeable, and this suite pins
%%    that asymmetry so a later "tidy-up" cannot reintroduce it.
-module(mqtt_transport_tcp_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [decode_tcp_data_for_the_matching_socket,
     decode_ssl_data_for_the_matching_socket,
     decode_ignores_another_sockets_data,
     decode_maps_closed_and_error,
     decode_leaves_other_messages_alone,
     send_and_receive_over_a_real_tcp_pair,
     arm_once_delivers_exactly_one_message,
     arm_once_on_an_unarmed_socket_works,
     close_shuts_the_socket,
     tcp_sockets_are_ports_and_ssl_sockets_are_not].

%%%_* decode_msg/2 ----------------------------------------------------------

decode_tcp_data_for_the_matching_socket(_Config) ->
    Socket = make_ref(),
    ?assertEqual({data, <<"abc">>}, mqtt_transport_tcp:decode_msg({tcp, Socket, <<"abc">>}, Socket)).

decode_ssl_data_for_the_matching_socket(_Config) ->
    Socket = make_ref(),
    ?assertEqual({data, <<"abc">>}, mqtt_transport_tcp:decode_msg({ssl, Socket, <<"abc">>}, Socket)).

%% The other listener's bytes are not ours.
decode_ignores_another_sockets_data(_Config) ->
    Ours = make_ref(),
    Theirs = make_ref(),
    ?assertEqual(other, mqtt_transport_tcp:decode_msg({tcp, Theirs, <<"x">>}, Ours)),
    ?assertEqual(other, mqtt_transport_tcp:decode_msg({ssl, Theirs, <<"x">>}, Ours)).

decode_maps_closed_and_error(_Config) ->
    S = make_ref(),
    ?assertEqual(closed, mqtt_transport_tcp:decode_msg({tcp_closed, S}, S)),
    ?assertEqual(closed, mqtt_transport_tcp:decode_msg({ssl_closed, S}, S)),
    ?assertEqual({error, econnreset}, mqtt_transport_tcp:decode_msg({tcp_error, S, econnreset}, S)),
    ?assertEqual({error, closed}, mqtt_transport_tcp:decode_msg({ssl_error, S, closed}, S)).

%% Everything the FSM's mailbox also carries: deliveries, timers, handoff.
decode_leaves_other_messages_alone(_Config) ->
    S = make_ref(),
    Others = [connect,
              {round_t, make_ref()},
              {poll_t, make_ref()},
              {broker_deliver, <<"c1">>, 1, [], {0, 1, x}},
              {handshake, ranch_tcp, ranch_tcp, S, 5000},
              {tcp, S, not_a_binary}],
    [?assertEqual(other, mqtt_transport_tcp:decode_msg(M, S)) || M <- Others].

%%%_* Real sockets ----------------------------------------------------------

send_and_receive_over_a_real_tcp_pair(_Config) ->
    {Client, Server} = pair(),
    ok = mqtt_transport_tcp:send(Server, <<"hello">>),
    {ok, <<"hello">>} = gen_tcp:recv(Client, 0, 1000),
    ok = mqtt_transport_tcp:close(Server),
    ok = gen_tcp:close(Client).

%% `arm_once/1' is the FSM's flow-control primitive: one delivery, then the FSM
%% re-arms. Arming twice or forgetting to re-arm both show up here as a wrong
%% message count.
arm_once_delivers_exactly_one_message(_Config) ->
    {Client, Server} = pair(),
    ok = mqtt_transport_tcp:arm_once(Server),
    ok = gen_tcp:send(Client, <<"one">>),
    receive
        M1 -> ?assertEqual({data, <<"one">>},
                           mqtt_transport_tcp:decode_msg(M1, Server))
    after 2000 -> ?assert(false)
    end,
    %% No re-arm yet: the second write stays in the kernel.
    ok = gen_tcp:send(Client, <<"two">>),
    receive
        M2 -> ?assertEqual(timeout, {unexpected, M2})
    after 200 -> ok
    end,
    ok = mqtt_transport_tcp:arm_once(Server),
    receive
        M3 -> ?assertEqual({data, <<"two">>},
                           mqtt_transport_tcp:decode_msg(M3, Server))
    after 2000 -> ?assert(false)
    end,
    ok = mqtt_transport_tcp:close(Server),
    ok = gen_tcp:close(Client).

%% Before the handoff the FSM holds `undefined' as its socket; arming it must not
%% crash the connection process into a supervisor report.
arm_once_on_an_unarmed_socket_works(_Config) ->
    {Client, Server} = pair(),
    ?assertEqual({error, einval}, mqtt_transport_tcp:arm_once(undefined)),
    ?assertEqual({error, einval}, mqtt_transport_tcp:send(undefined, <<"x">>)),
    ?assertEqual(other, mqtt_transport_tcp:decode_msg(connect, undefined)),
    ok = mqtt_transport_tcp:close(Server),
    ok = gen_tcp:close(Client).

close_shuts_the_socket(_Config) ->
    {Client, Server} = pair(),
    ok = mqtt_transport_tcp:arm_once(Server),
    ok = mqtt_transport_tcp:close(Server),
    {error, closed} = gen_tcp:recv(Client, 0, 1000),
    ok = gen_tcp:close(Client).

%%%_* Transport dispatch ----------------------------------------------------

%% This is the property `impl/1' relies on: a `gen_tcp' socket is a port, an
%% `ssl' socket is a tuple. If that ever changes, `arm_once/1' would silently
%% fall into the ssl branch for plain sockets.
tcp_sockets_are_ports_and_ssl_sockets_are_not(_Config) ->
    {Client, Server} = pair(),
    ?assert(is_port(Client)),
    ?assert(is_port(Server)),
    ok = mqtt_transport_tcp:close(Server),
    ok = gen_tcp:close(Client).

%%%_* Helpers ---------------------------------------------------------------

pair() ->
    {ok, Ls} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, Port} = inet:port(Ls),
    {ok, Client} = gen_tcp:connect({127, 0, 0, 1}, Port, [binary, {active, false}]),
    {ok, Server} = gen_tcp:accept(Ls),
    ok = gen_tcp:close(Ls),
    {Client, Server}.
