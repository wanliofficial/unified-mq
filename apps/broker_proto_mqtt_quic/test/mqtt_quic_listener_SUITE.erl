%% @doc The QUIC listener: the refusals, the configuration surface, and (with
%% `quicer' present) a real UDP listener that a real QUIC client can handshake
%% with and run MQTT over.
%%
%% The suite splits into two groups on purpose:
%%
%%   * `config' runs in **every** profile. QUIC's two boot refusals -- no
%%     certificate, no NIF -- are configuration facts and are observable without
%%     a socket, so a default build still proves them;
%%   * `quic' exists only when `quicer' can be loaded. It binds an ephemeral UDP
%%     port and speaks QUIC for real, which is the only way to assert that ALPN
%%     is enforced and that a CONNECT gets a CONNACK through the real
%%     `mqtt_conn_fsm'.
%%
%% The third-party evidence (an `emqtt' client, not this repository's code) lives
%% in `test/protocol-compatibility/mqtt_quic/'.
-module(mqtt_quic_listener_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(APP, broker_proto_mqtt_quic).

all() ->
    case mqtt_quic_nif:available() of
        true -> [{group, config}, {group, quic}];
        false -> [{group, config}]
    end.

groups() ->
    [{config, [],
      [no_certificate_refuses_to_start,
       certificate_comes_from_broker_security_tls,
       alpn_and_limits_have_documented_defaults,
       no_listener_is_started_without_config]},
     {quic, [],
      [listener_binds_an_ephemeral_port,
       a_client_with_alpn_mqtt_can_handshake,
       a_client_with_another_alpn_is_refused,
       a_real_connect_gets_a_connack,
       a_pingreq_is_not_a_protocol_error,
       a_new_stream_on_the_same_connection_is_a_new_session]}].

%%%_* Fixtures ----------------------------------------------------------------

init_per_suite(Config) ->
    _ = application:load(?APP),
    {ok, _} = application:ensure_all_started(broker_security),
    Config.

end_per_suite(_Config) ->
    stop_broker(),
    ok.

init_per_testcase(Case, Config0) ->
    stop_broker(),
    SavedTls = saved_tls(),
    restore_tls(undefined),
    Config = [{saved_tls, SavedTls} | Config0],
    case Case of
        _ when Case =:= certificate_comes_from_broker_security_tls;
               Case =:= listener_binds_an_ephemeral_port;
               Case =:= a_client_with_alpn_mqtt_can_handshake;
               Case =:= a_client_with_another_alpn_is_refused;
               Case =:= a_real_connect_gets_a_connack;
               Case =:= a_pingreq_is_not_a_protocol_error;
               Case =:= a_new_stream_on_the_same_connection_is_a_new_session ->
            {Cert, Key} = self_signed(),
            [{certfile, Cert}, {keyfile, Key} | Config];
        _ ->
            Config
    end.

end_per_testcase(_Case, Config) ->
    stop_broker(),
    restore_tls(?config(saved_tls, Config)),
    ok.

%%%_* config group ------------------------------------------------------------

%% QUIC's handshake *is* TLS 1.3, so there is no cleartext mode to fall back to.
%% A listener without a certificate must say so and stop -- not listen on a port
%% that no client can ever complete a handshake with.
no_certificate_refuses_to_start(_Config) ->
    restore_tls(#{enabled => false, certfile => undefined, keyfile => undefined}),
    Reason = start_refusal(#{port => 0}),
    %% `start_link' reports a refused `init/1' as `{error, StopReason}'.
    ?assertMatch({error, {quic_requires_tls, _}}, Reason),
    %% Whichever half is missing is named, so the operator is not told "TLS".
    ?assertEqual({error, {quic_requires_tls, certfile}}, Reason).

%% The listener has no TLS block of its own: with no certificate in its own map
%% it must take `broker_security.tls'. The observable proof is which refusal
%% comes back: a certificate was found, so the next missing thing is the NIF (on
%% a default build) or nothing at all (with quicer, where the listener starts).
certificate_comes_from_broker_security_tls(Config) ->
    restore_tls(#{enabled => true,
                  certfile => ?config(certfile, Config),
                  keyfile => ?config(keyfile, Config)}),
    case mqtt_quic_nif:available() of
        false ->
            ?assertEqual({error, quicer_not_available},
                         start_refusal(#{port => 0}));
        true ->
            {ok, Pid} = mqtt_quic_listener:start_link(#{port => 0}),
            ?assert(is_integer(mqtt_quic_listener:port(Pid))),
            gen_server:stop(Pid)
    end.

%% The values a release inherits when `sys.config' says nothing. Spelled out
%% here because two of them are protocol obligations, not preferences: ALPN
%% `mqtt' (CN Draft 3 §2.1) and bidi-stream credit > 1 (without it a client
%% could never reconnect on the same QUIC connection).
alpn_and_limits_have_documented_defaults(_Config) ->
    ?assertEqual([<<"mqtt">>], app_env(alpn)),
    ?assertEqual(5000, app_env(handshake_timeout_ms)),
    ?assertEqual(30000, app_env(idle_timeout_ms)),
    ?assertEqual(4, app_env(max_streams_per_conn)),
    ?assertEqual([], app_env(listeners)).

%% No `listeners' entry means no listener -- not "one on the default port". The
%% supervisor is started directly so the assertion is about its child list, not
%% about which unrelated applications happen to be running.
no_listener_is_started_without_config(_Config) ->
    ok = application:set_env(?APP, listeners, []),
    {ok, Sup} = broker_proto_mqtt_quic_sup:start_link(),
    ?assertEqual([], supervisor:which_children(Sup)),
    unlink(Sup),
    exit(Sup, shutdown).

%%%_* quic group --------------------------------------------------------------

%% `port => 0' asks the kernel. A QUIC listener is invisible to `inet:i/0', so
%% the handle read-back is the only way to learn the port -- and a suite needs it
%% before it can connect.
listener_binds_an_ephemeral_port(Config) ->
    {ok, Pid} = start_listener(Config),
    Port = mqtt_quic_listener:port(Pid),
    ?assert(is_integer(Port)),
    ?assert(Port > 0),
    gen_server:stop(Pid).

%% ALPN `mqtt' is what a conforming client offers, so it must succeed.
a_client_with_alpn_mqtt_can_handshake(Config) ->
    {ok, Pid} = start_listener(Config),
    Port = mqtt_quic_listener:port(Pid),
    {ok, Conn} = quicer:connect("localhost", Port,
                                #{alpn => ["mqtt"], verify => none}, 5000),
    ?assertNotEqual(undefined, Conn),
    _ = quicer:close_connection(Conn),
    gen_server:stop(Pid).

%% Offering only an ALPN we do not speak must fail the handshake. Accepting it
%% would let the client believe it is talking a protocol we are not.
a_client_with_another_alpn_is_refused(Config) ->
    {ok, Pid} = start_listener(Config),
    Port = mqtt_quic_listener:port(Pid),
    %% quicer reports a failed TLS handshake as `{error, transport_down, Info}'
    %% with `status => alpn_neg_failure' in `Info' -- a 3-tuple, not a 2-tuple.
    ?assertMatch({error, transport_down, #{status := alpn_neg_failure}},
                 quicer:connect("localhost", Port,
                                #{alpn => ["h3"], verify => none}, 3000)),
    gen_server:stop(Pid).

%% The whole stack on one stream: real QUIC, the real `mqtt_conn_fsm', a real
%% MQTT 3.1.1 CONNECT and a real CONNACK. If the handoff, the passive-to-active
%% switch or the stream ownership were wrong, this is where it shows.
a_real_connect_gets_a_connack(Config) ->
    boot_broker(Config),
    {ok, Pid} = start_listener(Config),
    Port = mqtt_quic_listener:port(Pid),
    {ok, Conn} = quicer:connect("localhost", Port,
                                #{alpn => ["mqtt"], verify => none}, 5000),
    {ok, Stream} = quicer:start_stream(Conn, #{active => true}),
    {ok, _} = quicer:send(Stream, connect_bin(<<"quic-suite">>)),
    Connack = recv_mqtt(Stream, <<>>, 5000),
    %% CONNACK: type 0x20, remaining length 2, session-present 0, return code 0.
    ?assertMatch(<<16#20, 2, 0, 0>>, Connack),
    _ = quicer:close_connection(Conn),
    gen_server:stop(Pid).

%% A transport must not look like a protocol violation.
%%
%% quicer reads an **integer** active value as `{active, N}': the stream delivers
%% N messages and then sends `{quic, passive, Stream, undefined}' to its owner.
%% `arm_once/1' originally used `active, 1', which put the stream in exactly that
%% mode -- so after every MQTT packet the FSM received an event it does not know,
%% logged "unexpected event in connected" and bumped `protocol_errors'. Under
%% load that is a log line and a metric per read.
%%
%% The packet still round-trips in the broken version (the data arrives first),
%% so the counter is what this case asserts on, not the PINGRESP alone. `once' is
%% the atom form of the same one-delivery arm, and it emits no follow-up event.
a_pingreq_is_not_a_protocol_error(Config) ->
    boot_broker(Config),
    {ok, Pid} = start_listener(Config),
    Port = mqtt_quic_listener:port(Pid),
    {ok, Conn} = quicer:connect("localhost", Port,
                                #{alpn => ["mqtt"], verify => none}, 5000),
    {ok, Stream} = quicer:start_stream(Conn, #{active => true}),
    {ok, _} = quicer:send(Stream, connect_bin(<<"quic-ping">>)),
    ?assertMatch(<<16#20, 2, 0, 0>>, recv_mqtt(Stream, <<>>, 5000)),
    Before = protocol_errors(),
    {ok, _} = quicer:send(Stream, <<16#C0, 0>>),
    ?assertMatch(<<16#D0, 0>>, recv_mqtt(Stream, <<>>, 5000)),
    %% The passive event, if it were coming, follows the data -- so assert after
    %% the read has definitely been handled, not as soon as the PINGRESP lands.
    timer:sleep(250),
    ?assertEqual(Before, protocol_errors()),
    _ = quicer:close_connection(Conn),
    gen_server:stop(Pid).

%% CN Draft 3 §2.1: closing the stream ends the MQTT Network Connection, **not**
%% the QUIC connection. The second CONNECT below travels on a *new* stream of the
%% *same* connection -- if the connection process stopped when the first stream
%% ended (or if the listener had granted just one stream), this case would have
%% nothing to talk to.
a_new_stream_on_the_same_connection_is_a_new_session(Config) ->
    boot_broker(Config),
    {ok, Pid} = start_listener(Config),
    Port = mqtt_quic_listener:port(Pid),
    {ok, Conn} = quicer:connect("localhost", Port,
                                #{alpn => ["mqtt"], verify => none}, 5000),
    {ok, First} = quicer:start_stream(Conn, #{active => true}),
    {ok, _} = quicer:send(First, connect_bin(<<"quic-first">>)),
    ?assertMatch(<<16#20, 2, 0, 0>>, recv_mqtt(First, <<>>, 5000)),
    %% The client finishes its direction on stream 1. Its FSM has to be gone
    %% before stream 2 CONNECTs, otherwise the same client id would be a
    %% takeover rather than a fresh session -- and the point of this case is the
    %% stream, not the session semantics.
    _ = quicer:shutdown_stream(First, 1000),
    ok = wait_until(fun() -> conn_count() =:= 0 end),
    {ok, Second} = quicer:start_stream(Conn, #{active => true}),
    {ok, _} = quicer:send(Second, connect_bin(<<"quic-second">>)),
    ?assertMatch(<<16#20, 2, 0, 0>>, recv_mqtt(Second, <<>>, 5000)),
    _ = quicer:close_connection(Conn),
    gen_server:stop(Pid).

%%%_* Helpers -----------------------------------------------------------------

start_refusal(Spec) ->
    %% `start_link' links the listener to this process; a listener that refuses
    %% in `init/1' exits, and without trapping that exit would kill the test case
    %% instead of being observed as `{error, Reason}'.
    WasTrapping = process_flag(trap_exit, true),
    Result = mqtt_quic_listener:start_link(Spec),
    process_flag(trap_exit, WasTrapping),
    case Result of
        {ok, Pid} ->
            %% It started: report that as a failure with the reason, so the test
            %% names what happened instead of timing out.
            gen_server:stop(Pid),
            {unexpected_start, Pid};
        {error, _} = Error ->
            Error
    end.

start_listener(Config) ->
    mqtt_quic_listener:start_link(
      #{port => 0,
        certfile => ?config(certfile, Config),
        keyfile => ?config(keyfile, Config)}).

recv_mqtt(Stream, Buf, Timeout) ->
    recv_packet(Stream, Buf, Timeout).

%% Read one complete MQTT packet: the fixed header byte, then the remaining
%% length (1-4 bytes, 7 bits each), then that many bytes. Enough for the packet
%% types this suite reads (CONNACK, PINGRESP) and honest about the two lengths
%% that actually appear in MQTT 3.1.1.
recv_packet(Stream, Buf, Timeout) ->
    case packet_length(Buf) of
        {ok, Len} when byte_size(Buf) >= Len ->
            binary:part(Buf, 0, Len);
        _ ->
            receive
                {quic, Bin, Stream, _} when is_binary(Bin) ->
                    recv_packet(Stream, <<Buf/binary, Bin/binary>>, Timeout);
                {quic, Event, Stream, _} ->
                    exit({stream_event_before_packet, Event})
            after Timeout ->
                    exit({no_packet, Buf})
            end
    end.

packet_length(<<>>) ->
    more;
packet_length(<<_Type, Rest/binary>>) ->
    case remaining_length(Rest, 0, 1, 0) of
        {ok, Len, Used} -> {ok, 1 + Used + Len};
        _ -> more
    end.

remaining_length(<<>>, _Acc, _Mult, _Used) ->
    more;
remaining_length(<<0:1, Value:7, _/binary>>, Acc, Mult, Used) ->
    {ok, Acc + Value * Mult, Used + 1};
remaining_length(<<1:1, Value:7, Rest/binary>>, Acc, Mult, Used) when Used < 3 ->
    remaining_length(Rest, Acc + Value * Mult, Mult * 128, Used + 1);
remaining_length(_Bin, _Acc, _Mult, _Used) ->
    malformed.

%% MQTT 3.1.1 CONNECT, keepalive 60, clean session, one client id. The wire
%% layout is: protocol name (`00 04 MQTT'), level 4, flags 0x02 (clean session),
%% keepalive, then the client id as a length-prefixed field.
connect_bin(ClientId) ->
    Payload = <<(byte_size(ClientId)):16, ClientId/binary>>,
    Variable = <<4:16, "MQTT", 4, 2, 60:16>>,
    Body = <<Variable/binary, Payload/binary>>,
    <<16#10, (byte_size(Body)), Body/binary>>.

app_env(Key) ->
    application:get_env(?APP, Key, undefined).

%% The MQTT engine's own protocol-error counter. Read out of the snapshot map
%% (`0' when the key has not been touched yet); it is the only observable trace
%% of "the transport handed the FSM something it does not understand".
protocol_errors() ->
    maps:get(protocol_errors, broker_metrics:snapshot(), 0).

%% Live MQTT sessions under the engine's own supervisor -- the direct evidence
%% that a stream's FSM has finished (its child is `temporary', so it is removed
%% the moment the process is gone).
conn_count() ->
    case whereis(mqtt_conn_sup) of
        undefined -> 0;
        Sup -> length(supervisor:which_children(Sup))
    end.

wait_until(Fun) -> wait_until(Fun, 100).

wait_until(_Fun, 0) -> exit(wait_until_timeout);
wait_until(Fun, Left) ->
    case Fun() of
        true -> ok;
        false -> timer:sleep(50), wait_until(Fun, Left - 1)
    end.

%%%_* Broker boot -------------------------------------------------------------

%% Only the QUIC group's end-to-end cases need a broker. The configuration is the
%% one the other protocol suites use: a WAL in the CT priv dir, N partitions, no
%% auth, and the MQTT engine's own defaults spelled out.
%%
%% The apps are **loaded before** their `set_env' calls. `set_env' on an app the
%% controller has not loaded yet records a value that `application:load/1' then
%% throws away in favour of the `.app' file's defaults -- so `broker_queue' would
%% boot with its own `partitions' default (4) after this suite had created
%% partitions 0..N-1, and the second partition worker would abort with
%% `partition_not_found'. Loading first makes the order unambiguous.
boot_broker(Config) ->
    lists:foreach(fun(App) -> _ = application:load(App) end,
                  [broker_storage, broker_router, broker_queue,
                   broker_subscription, broker_retain, broker_event,
                   broker_security, broker_proto_mqtt]),
    DataDir = filename:join([?config(priv_dir, Config), "data"]),
    ok = filelib:ensure_dir(filename:join([DataDir, "x"])),
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok = application:set_env(broker_storage, data_dir, DataDir),
    ok = application:set_env(broker_storage, wal, #{require_durable_fs => false}),
    ok = application:set_env(broker_queue, partitions, 2),
    ok = application:set_env(broker_router, partitions, 2),
    ok = application:set_env(broker_subscription, data_dir, DataDir),
    ok = application:set_env(broker_retain, data_dir, DataDir),
    ok = application:set_env(broker_subscription, cursor_flush_interval_ms, 20),
    ok = application:set_env(broker_subscription, inflight_flush_interval_ms, 20),
    ok = application:set_env(broker_security, users, []),
    ok = application:set_env(broker_security, acl, allow),
    ok = application:set_env(broker_security, require_auth, []),
    ok = application:set_env(broker_proto_mqtt, pull_page_limit, 32),
    ok = application:set_env(broker_proto_mqtt, max_inflight, 64),
    ok = application:set_env(broker_proto_mqtt, pull_poll_interval_ms, 100),
    ok = application:set_env(broker_proto_mqtt, max_keepalive_ms, 60000),
    ok = application:set_env(broker_proto_mqtt, mqtt_max_qos, 2),
    ok = application:set_env(broker_proto_mqtt, max_qos2_state_per_session, 100),
    {ok, _} = application:ensure_all_started(broker_storage),
    lists:foreach(
      fun(P) ->
              case broker_queue:create_partition(P) of
                  ok -> ok;
                  {error, {already_exists, P}} -> ok;
                  {error, Other} -> exit({create_partition, P, Other})
              end
      end, broker_queue:partition_ids()),
    {ok, _} = application:ensure_all_started(broker_proto_mqtt),
    ok.

stop_broker() ->
    _ = application:stop(?APP),
    _ = application:stop(broker_proto_mqtt),
    _ = application:stop(broker_retain),
    _ = application:stop(broker_subscription),
    _ = application:stop(broker_queue),
    _ = application:stop(broker_router),
    _ = application:stop(broker_storage),
    ok.

saved_tls() ->
    application:get_env(broker_security, tls, undefined).

restore_tls(undefined) ->
    application:unset_env(broker_security, tls);
restore_tls(Value) ->
    application:set_env(broker_security, tls, Value).

%% Generated per suite run, never checked in. Same openssl shape as the TLS
%% suites, so all three fail together if openssl changes its defaults.
self_signed() ->
    Dir = filename:join(["/tmp/opencode", "mqtt_quic_test"]),
    _ = file:del_dir_r(Dir),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    Cert = filename:join(Dir, "cert.pem"),
    Key = filename:join(Dir, "key.pem"),
    Cmd = io_lib:format(
            "openssl req -x509 -newkey rsa:2048 -nodes -days 1 "
            "-subj /CN=localhost -addext subjectAltName=DNS:localhost,IP:127.0.0.1 "
            "-addext extendedKeyUsage=serverAuth "
            "-keyout ~s -out ~s 2>/dev/null",
            [Key, Cert]),
    _ = os:cmd(lists:flatten(Cmd)),
    case {filelib:is_regular(Cert), filelib:is_regular(Key)} of
        {true, true} -> {Cert, Key};
        _ -> exit({openssl_failed, Cert, Key})
    end.
