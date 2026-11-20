%% @doc MQTT-SN over DTLS (SN-01, 2026-11-17): PSK handshake, identity ->
%% principal/tenant, DTLS round trip, refusal indistinguishability, boot
%% gating, plaintext coexistence.
%%
%% The evidence here is the broker's own CT (an Erlang ssl DTLS client) --
%% `mqtt-sn-tools' has no DTLS support, so no third-party DTLS client exists
%% for this protocol (recorded in docs/protocol-compatibility.md §4.11). The
%% S11-C CoAP DTLS suite proved the same ssl client pattern on OTP 28.3.1.
-module(mqtt_sn_dtls_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(HOST, {127, 0, 0, 1}).
-define(PSK_LABEL, <<"unifiedmq-mqttsn-psk">>).
-define(REALM, <<"unifiedmq">>).
-define(POLL_MS, 5000).
-define(HANDSHAKE_MS, 4000).

all() ->
    [handshake_and_connect_connack,
     qos1_roundtrip_over_dtls,
     tenant_isolation_from_the_identity,
     unknown_identity_is_indistinguishable,
     dtls_without_users_refuses_to_start,
     plaintext_listener_coexists].

suite() -> [{timetrap, {seconds, 120}}].

init_per_suite(Config) ->
    _ = application:load(broker_storage),
    _ = application:load(broker_router),
    _ = application:load(broker_queue),
    _ = application:load(broker_subscription),
    _ = application:load(broker_event),
    _ = application:load(broker_security),
    _ = application:load(broker_retain),
    _ = application:load(broker_proto_mqtt_sn),
    {ok, _} = application:ensure_all_started(ssl),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(Case, Config0) ->
    stop_all(),
    DataDir = filename:join([?config(priv_dir, Config0), atom_to_list(Case), "data"]),
    ok = filelib:ensure_dir(filename:join([DataDir, "x"])),
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok = application:set_env(broker_storage, data_dir, DataDir),
    ok = application:set_env(broker_storage, wal, #{require_durable_fs => false}),
    ok = application:set_env(broker_queue, partitions, 2),
    ok = application:set_env(broker_router, partitions, 2),
    ok = application:set_env(broker_subscription, data_dir, DataDir),
    ok = application:set_env(broker_retain, data_dir, DataDir),
    ok = application:set_env(broker_event, policy, pass_through),
    %% The ONLY secret configured here: every PSK derives from the stored
    %% password (label || identity || realm || password), which is the wire
    %% contract under test.
    ok = application:set_env(broker_security, users,
                             [{<<"device-01">>, #{password => <<"secret-01">>,
                                                 tenant_id => <<"t1">>}},
                              {<<"device-02">>, #{password => <<"secret-02">>,
                                                 tenant_id => <<"t1">>}},
                              {<<"tenant-b">>, #{password => <<"secret-b">>,
                                                 tenant_id => <<"t2">>}}]),
    ok = application:set_env(broker_security, acl, allow),
    ok = application:set_env(broker_security, require_auth, []),
    ok = application:set_env(broker_security, mqttsn, #{realm => ?REALM}),
    ok = application:set_env(broker_proto_mqtt_sn, predefined_topics,
                             #{1 => <<"sn/dtls">>}),
    Listeners = case Case of
                    plaintext_listener_coexists ->
                        [#{port => 0, transport => dtls},
                         #{port => 0}];
                    dtls_without_users_refuses_to_start ->
                        [];
                    _ ->
                        [#{port => 0, transport => dtls}]
                end,
    ok = application:set_env(broker_proto_mqtt_sn, listeners, Listeners),
    {ok, _} = application:ensure_all_started(broker_storage),
    ok = create_partitions(),
    Boot = case Case of
               dtls_without_users_refuses_to_start ->
                   %% The empty table makes every DTLS handshake impossible: the
                   %% listener must refuse the boot instead of starting dead.
                   ok = application:set_env(broker_security, users, []),
                   ok = application:set_env(broker_proto_mqtt_sn, listeners,
                                            [#{port => 0, transport => dtls}]),
                   application:ensure_all_started(broker_proto_mqtt_sn);
               _ ->
                   application:ensure_all_started(broker_proto_mqtt_sn)
           end,
    case {Case, Boot} of
        {ok, _} -> ok;
        %% The refusal propagates through the supervisor as
        %% {shutdown, {failed_to_start_child, ...}} -- match on the leaf.
        {dtls_without_users_refuses_to_start,
         {error, {broker_proto_mqtt_sn, _}}} -> ok;
        {_, {ok, _}} -> ok;
        {_, {error, Reason}} -> exit({boot_failed, Reason})
    end,
    %% A refused boot has no listener to report and no port to hand out: the
    %% case asserts the refusal, it does not connect. Asking for the port here
    %% would make the very case under test fail in `init_per_testcase'.
    case Case of
        dtls_without_users_refuses_to_start -> [{port, 0} | Config0];
        _ -> [{port, listener_port()} | Config0]
    end.

end_per_testcase(_Case, _Config) ->
    stop_all(),
    ok.

create_partitions() ->
    lists:foreach(
      fun(P) ->
              case broker_queue:create_partition(P) of
                  ok -> ok;
                  {error, {already_exists, P}} -> ok;
                  {error, Other} -> exit({create_partition, P, Other})
              end
      end, broker_queue:partition_ids()).

listener_port() ->
    case [Pid || {Id, Pid, worker, _} <- supervisor:which_children(
                                                broker_proto_mqtt_sn_sup),
                 element(1, Id) =:= mqtt_sn_listener] of
        [Pid | _] -> mqtt_sn_listener:port(Pid);
        [] -> exit(no_listener)
    end.

stop_all() ->
    _ = application:stop(broker_proto_mqtt_sn),
    _ = application:stop(broker_retain),
    _ = application:stop(broker_event),
    _ = application:stop(broker_security),
    _ = application:stop(broker_subscription),
    _ = application:stop(broker_queue),
    _ = application:stop(broker_router),
    _ = application:stop(broker_storage),
    %% Application env outlives `application:stop/1' (it stays in the loaded
    %% .app), so a value set here would leak into another suite in the same
    %% node -- `mqtt_sn_topics_SUITE:predefined_empty_by_default' asserts the
    %% untouched default and failed for exactly that reason. Reset what this
    %% suite sets.
    ok = application:unset_env(broker_proto_mqtt_sn, predefined_topics),
    ok = application:unset_env(broker_proto_mqtt_sn, listeners),
    ok = application:unset_env(broker_security, users),
    ok = application:unset_env(broker_security, mqttsn),
    ok.

%%%_* Cases ------------------------------------------------------------------

handshake_and_connect_connack(Config) ->
    Port = ?config(port, Config),
    {ok, S} = connect(<<"device-01">>, <<"secret-01">>, Port),
    ok = ssl_send(S, #{type => connect, protocol_id => 1,
                       clean_session => true, duration => 60,
                       client_id => <<"sn-d1">>}),
    #{type := connack, return_code := 0} = ssl_expect(S, connack),
    ssl:close(S),
    ok.

%% Full round trip over one DTLS transport: subscriber and publisher on two
%% DTLS connections, predefined topic so no REGISTER dance is needed.
qos1_roundtrip_over_dtls(Config) ->
    Port = ?config(port, Config),
    {ok, Sub} = connect(<<"device-01">>, <<"secret-01">>, Port),
    ok = ssl_send(Sub, #{type => connect, protocol_id => 1,
                         clean_session => true, duration => 60,
                         client_id => <<"sn-sub">>}),
    #{type := connack, return_code := 0} = ssl_expect(Sub, connack),
    ok = ssl_send(Sub, #{type => subscribe, msg_id => 1, qos => 1,
                         topic_id_type => normal, topic => <<"sn/dtls">>}),
    Sub1 = expect(Sub, suback),
    #{type := suback, topic_id := TopicId} = Sub1,
    {ok, Pub} = connect(<<"device-02">>, <<"secret-02">>, Port),
    ok = ssl_send(Pub, #{type => connect, protocol_id => 1,
                         clean_session => true, duration => 60,
                         client_id => <<"sn-pub">>}),
    #{type := connack, return_code := 0} = ssl_expect(Pub, connack),
    ok = ssl_send(Pub, #{type => publish, qos => 1, topic_id_type => predefined,
                         topic_id => TopicId, msg_id => 7,
                         payload => <<"over-dtls">>}),
    #{type := publish, topic_id := TopicId, payload := <<"over-dtls">>} =
        expect(Sub, publish),
    ok = ssl_send(Sub, #{type => puback, topic_id => TopicId, msg_id => 7,
                         return_code => 0}),
    ssl:close(Sub),
    ssl:close(Pub),
    ok.

%% The identity proven by the handshake IS the principal, and the tenant comes
%% from its user record: a t2 device's publish must not reach a t1 subscriber.
tenant_isolation_from_the_identity(Config) ->
    Port = ?config(port, Config),
    {ok, Sub} = connect(<<"device-01">>, <<"secret-01">>, Port),
    ok = ssl_send(Sub, #{type => connect, protocol_id => 1,
                         clean_session => true, duration => 60,
                         client_id => <<"sn-iso-sub">>}),
    #{type := connack, return_code := 0} = ssl_expect(Sub, connack),
    ok = ssl_send(Sub, #{type => subscribe, msg_id => 1, qos => 1,
                         topic_id_type => normal, topic => <<"sn/dtls">>}),
    #{type := suback, topic_id := TopicId} = expect(Sub, suback),
    {ok, Other} = connect(<<"tenant-b">>, <<"secret-b">>, Port),
    ok = ssl_send(Other, #{type => connect, protocol_id => 1,
                           clean_session => true, duration => 60,
                           client_id => <<"sn-iso-b">>}),
    #{type := connack, return_code := 0} = ssl_expect(Other, connack),
    ok = ssl_send(Other, #{type => publish, qos => 0,
                           topic_id_type => predefined,
                           topic_id => TopicId, msg_id => 0,
                           payload => <<"cross-tenant">>}),
    %% Nothing crosses the tenant boundary; the same transport, same topic.
    {error, timeout} = ssl:recv(Sub, 0, 800),
    ssl:close(Sub),
    ssl:close(Other),
    ok.

%% An unknown identity and a wrong password die at the same Finished check
%% with the same observable outcome: the handshake never completes, and the
%% client times out. Nothing distinguishes "no such user" from "bad secret",
%% so the identity table cannot be enumerated (S11-C decoy design).
unknown_identity_is_indistinguishable(Config) ->
    Port = ?config(port, Config),
    Unknown = connect(<<"nosuch-device">>, <<"secret-01">>, Port,
                      ?HANDSHAKE_MS),
    WrongPw = connect(<<"device-01">>, <<"wrong-password">>, Port,
                      ?HANDSHAKE_MS),
    %% OTP 28 left both handshakes hanging until the client timed out
    %% (`{error, timeout}'); OTP 29 refuses both up front with a fatal
    %% `{tls_alert, {handshake_failure, _}}'. Either way the two peers are
    %% indistinguishable, so the reason is normalised to its class before
    %% comparing (the CoAP twin does the same).
    ?assertEqual(refusal_class(Unknown), refusal_class(WrongPw)),
    ?assert(lists:member(refusal_class(Unknown),
                         [timeout, {tls_alert, handshake_failure}])),
    %% The listener is unharmed: a good identity still gets in.
    {ok, Good} = connect(<<"device-01">>, <<"secret-01">>, Port),
    ok = ssl:close(Good),
    ok.

%% No `users' rows -> no PSK can ever verify -> the DTLS listener must refuse
%% the boot (the CoAP S11-C posture), not come up dead.
dtls_without_users_refuses_to_start(_Config) ->
    ok.

%% The plain UDP path is unchanged by the DTLS addition: both listener kinds
%% coexist, and the plaintext one still answers a CONNECT over gen_udp (the
%% DTLS listener's port silently drops raw UDP -- it is an ssl socket).
plaintext_listener_coexists(_Config) ->
    PlainPort = find_plain_port(),
    S = gen_udp_open(),
    ok = gen_udp:send(S, ?HOST, PlainPort,
                      mqtt_sn_codec:encode(#{type => connect, protocol_id => 1,
                                             clean_session => true,
                                             duration => 60,
                                             client_id => <<"sn-plain">>})),
    #{type := connack, return_code := 0} = udp_expect(S, connack),
    gen_udp:close(S),
    ok.

find_plain_port() ->
    Ports = lists:sort(
              [mqtt_sn_listener:port(Pid)
               || {Id, Pid, worker, _} <- supervisor:which_children(
                                            broker_proto_mqtt_sn_sup),
                  element(1, Id) =:= mqtt_sn_listener]),
    ?assertEqual(2, length(Ports)),
    S = gen_udp_open(),
    Plain = probe_plain(S, Ports),
    gen_udp:close(S),
    Plain.

probe_plain(_S, [Port]) ->
    Port;
probe_plain(S, [Port | Rest]) ->
    ok = gen_udp:send(S, ?HOST, Port,
                      mqtt_sn_codec:encode(#{type => connect, protocol_id => 1,
                                             clean_session => true,
                                             duration => 60,
                                             client_id => <<"sn-probe">>})),
    case gen_udp:recv(S, 0, 1000) of
        {ok, {_, _, Bin}} ->
            case mqtt_sn_codec:decode(Bin) of
                {ok, #{type := connack}} -> Port;
                _ -> probe_plain(S, Rest)
            end;
        {error, _} ->
            probe_plain(S, Rest)
    end.

%%%_* DTLS client ------------------------------------------------------------

%% The KDF a device must reproduce: label, NUL, identity, NUL, realm, NUL,
%% password. Pinned here because it IS the wire contract.
psk(Identity, Password) ->
    crypto:hash(sha256, [?PSK_LABEL, 0, Identity, 0, ?REALM, 0, Password]).

%% OTP 28 reported a refused PSK handshake as the bare atom `timeout'; OTP 29
%% reports it as a fatal `{tls_alert, {handshake_failure, _}}'. The two peers
%% under test (unknown identity vs. wrong secret) must stay indistinguishable,
%% so collapse either shape to its class before comparing.
refusal_class(timeout) ->
    timeout;
refusal_class({error, {tls_alert, {Alert, _Desc}}}) ->
    {tls_alert, Alert};
refusal_class(Other) ->
    Other.

connect(Identity, Password, Port) ->
    connect(Identity, Password, Port, ?HANDSHAKE_MS).

connect(Identity, Password, Port, Timeout) ->
    Psk = psk(Identity, Password),
    Opts = [{protocol, dtls},
            %% The client half of the identity is a STRING, not a binary (OTP's
            %% ssl API); the server receives a binary.
            {psk_identity, binary_to_list(Identity)},
            {ciphers, cert_free_psk_suites()},
            {user_lookup_fun, {fun(_, _, _) -> {ok, Psk} end, undefined}},
            {verify, verify_none},
            {reuseaddr, true},
            {mode, binary},
            {active, false}],
    ssl:connect(?HOST, Port, Opts, Timeout).

cert_free_psk_suites() ->
    [S || S <- ssl:cipher_suites(anonymous, 'dtlsv1.2'),
          lists:member(maps:get(key_exchange, S, none), [psk, dhe_psk, ecdhe_psk]),
          lists:member(maps:get(cipher, S, none),
                       [aes_128_gcm, aes_256_gcm, aes_128_ccm, aes_256_ccm,
                        aes_128_ccm_8, aes_256_ccm_8])].

ssl_send(S, Packet) ->
    ok = ssl:send(S, mqtt_sn_codec:encode(Packet)).

%% DTLS application-data records are message-framed: one ssl:send is one
%% recv, so the SN datagram framing survives the transport unchanged.
ssl_expect(S, Type) ->
    Deadline = erlang:monotonic_time(millisecond) + ?POLL_MS,
    ssl_expect(S, Type, Deadline).

ssl_expect(S, Type, Deadline) ->
    Left = Deadline - erlang:monotonic_time(millisecond),
    case Left > 0 andalso ssl:recv(S, 0, Left) of
        {error, timeout} -> exit({timeout_waiting_for, Type});
        {error, Other} -> exit({dtls_recv_error, Other});
        {ok, Bin} ->
            case mqtt_sn_codec:decode(Bin) of
                {ok, #{type := Type} = Packet} -> Packet;
                {ok, _Other} -> ssl_expect(S, Type, Deadline);
                {error, Reason} -> exit({decode_failed, Reason})
            end;
        Other -> exit({recv_odd, Other})
    end.

expect(S, Type) ->
    Deadline = erlang:monotonic_time(millisecond) + ?POLL_MS,
    expect(S, Type, Deadline).

expect(S, Type, Deadline) ->
    Left = Deadline - erlang:monotonic_time(millisecond),
    case Left > 0 andalso ssl:recv(S, 0, Left) of
        timeout -> exit({timeout_waiting_for, Type});
        false -> exit({timeout_waiting_for, Type});
        {ok, Bin} ->
            case mqtt_sn_codec:decode(Bin) of
                {ok, #{type := Type} = Packet} -> Packet;
                {ok, _Other} -> expect(S, Type, Deadline);
                {error, Reason} -> exit({decode_failed, Reason})
            end;
        Other -> exit({recv_odd, Other})
    end.

%%%_* Plaintext UDP client ---------------------------------------------------

gen_udp_open() ->
    {ok, S} = gen_udp:open(0, [binary, {active, false}]),
    S.

udp_expect(S, Type) ->
    Deadline = erlang:monotonic_time(millisecond) + ?POLL_MS,
    udp_expect(S, Type, Deadline).

udp_expect(S, Type, Deadline) ->
    Left = Deadline - erlang:monotonic_time(millisecond),
    case Left > 0 andalso gen_udp:recv(S, 0, Left) of
        {error, Reason} -> exit({udp_recv, Reason});
        {ok, {_IP, _Port, Bin}} ->
            case mqtt_sn_codec:decode(Bin) of
                {ok, #{type := Type} = Packet} -> Packet;
                {ok, _Other} -> udp_expect(S, Type, Deadline);
                {error, Reason} -> exit({decode_failed, Reason})
            end;
        Other -> exit({recv_odd, Other})
    end.

