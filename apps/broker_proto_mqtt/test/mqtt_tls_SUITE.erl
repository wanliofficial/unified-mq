%% @doc MQTT over TLS: real MQTT frames through a real `ranch_ssl' listener.
%%
%% `broker_security_tls_SUITE' proves the TLS *configuration* is coherent and
%% that `ssl:listen/2' accepts the options it builds. It never carries an MQTT
%% frame, and that gap hid a real defect: `mqtt_conn_fsm' called
%% `gen_tcp:send/2' and `inet:setopts/2' on whatever socket ranch handed it, so
%% a `ranch_ssl' connection accepted the socket, armed it, and then died on the
%% first write. MQTT over TLS was broken while the suite was green.
%%
%% S8 moved those calls behind `mqtt_transport', whose socket implementation
%% dispatches `gen_tcp'/`ssl' per socket. These cases are the regression: the
%% assertions are MQTT-level (a CONNACK, a SUBACK, a delivered PUBLISH over
%% `ssl:send'/`ssl:recv' from a third party's point of view), not "an ssl
%% listener was created".
-module(mqtt_tls_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("mqtt_frame.hrl").

-define(N, 2).
-define(REF, {mqtt, 0}).
-define(LISTENERS, [#{port => 0,
                      num_acceptors => 2,
                      max_keepalive_ms => 60000}]).

all() ->
    [tls_is_on_only_while_enabled,
     connect_over_tls_completes_the_handshake,
     publish_qos1_over_tls_roundtrips,
     stopping_the_app_closes_the_tls_fleet].

init_per_suite(Config) ->
    _ = application:load(broker_storage),
    _ = application:load(broker_router),
    _ = application:load(broker_queue),
    _ = application:load(broker_subscription),
    _ = application:load(broker_retain),
    _ = application:load(broker_event),
    _ = application:load(broker_security),
    _ = application:load(broker_proto_mqtt),
    Config.

end_per_suite(_Config) ->
    stop_all(),
    ok.

init_per_testcase(Case, Config) ->
    stop_all(),
    %% Snapshot the ORIGINAL tls env before any case can change it, and thread it
    %% through Config so `end_per_testcase' restores exactly that. Reading
    %% `saved_tls()' at teardown time instead would capture whatever the case left
    %% behind (`enabled'), leaking TLS into every suite that runs after this one in
    %% the shared CT node -- their plain listeners would then speak TLS.
    SavedTls = saved_tls(),
    restore_tls(SavedTls),
    DataDir = filename:join([?config(priv_dir, Config), atom_to_list(Case), "data"]),
    ok = filelib:ensure_dir(filename:join([DataDir, "x"])),
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok = application:set_env(broker_storage, data_dir, DataDir),
    ok = application:set_env(broker_storage, wal, #{require_durable_fs => false}),
    ok = application:set_env(broker_queue, partitions, ?N),
    ok = application:set_env(broker_router, partitions, ?N),
    ok = application:set_env(broker_subscription, data_dir, DataDir),
    ok = application:set_env(broker_retain, data_dir, DataDir),
    ok = application:set_env(broker_security, users, []),
    ok = application:set_env(broker_security, acl, allow),
    ok = application:set_env(broker_proto_mqtt, listeners, ?LISTENERS),
    ok = application:set_env(broker_proto_mqtt, pull_page_limit, 32),
    ok = application:set_env(broker_proto_mqtt, max_inflight, 64),
    ok = application:set_env(broker_proto_mqtt, pull_poll_interval_ms, 100),
    ok = application:set_env(broker_proto_mqtt, max_keepalive_ms, 60000),
    ok = application:set_env(broker_proto_mqtt, mqtt_max_qos, 1),
    {Cert, Key} = self_signed(?config(priv_dir, Config)),
    %% Partitions must exist before the listeners boot (H8), and
    %% `create_partition' reaches into the WAL sup that only exists once storage
    %% is running. Re-started per case, after `end_per_testcase' stopped it.
    {ok, _Started} = application:ensure_all_started(broker_storage),
    [{certfile, Cert}, {keyfile, Key}, {saved_tls, SavedTls} | Config].

end_per_testcase(_Case, Config) ->
    kill_conns(),
    stop_all(),
    restore_tls(?config(saved_tls, Config)),
    ok.

%%%_* Cases -------------------------------------------------------------------

%% Disabled TLS is not "TLS we skip": `transport/0' must hand ranch the plain
%% TCP pair, which is the posture every non-TLS listener boots in.
tls_is_on_only_while_enabled(_Config) ->
    disable_tls(),
    ?assertEqual({ranch_tcp, []}, broker_security_tls:transport()),
    enable_tls(_Config),
    {ranch_ssl, Opts} = broker_security_tls:transport(),
    ?assert(lists:keymember(certfile, 1, Opts)),
    ?assert(lists:keymember(keyfile, 1, Opts)).

%% The frame the old code could never deliver: CONNECT in, CONNACK out, both
%% through the ssl socket the FSM was given by ranch.
connect_over_tls_completes_the_handshake(Config) ->
    enable_tls(Config),
    Port = boot(),
    Sock = tls_connect(Port),
    #connack{session_present = 0, return_code = 0} =
        tls_request(Sock, #connect{client_id = <<"tls-client">>, clean_session = 1}),
    ok = ssl:close(Sock).

%% A full QoS1 round trip: the write path (`send_frame/2' -> transport -> ssl)
%% and the read path (`{ssl, ...}' -> transport -> frame decode) both have to be
%% right for the PUBACK to come back.
publish_qos1_over_tls_roundtrips(Config) ->
    enable_tls(Config),
    Port = boot(),
    Sub = tls_connect(Port),
    Pub = tls_connect(Port),
    #connack{return_code = 0} =
        tls_request(Sub, #connect{client_id = <<"tls-sub">>, clean_session = 1}),
    #connack{return_code = 0} =
        tls_request(Pub, #connect{client_id = <<"tls-pub">>, clean_session = 1}),
    #suback{return_codes = [1]} =
        tls_request(Sub, #subscribe{packet_id = 1, topics = [{<<"tls/t">>, 1}]}),
    #puback{packet_id = 2} =
        tls_request(Pub, #publish{qos = 1, topic = <<"tls/t">>,
                                  packet_id = 2, payload = <<"over-tls">>}),
    #publish{topic = <<"tls/t">>, payload = <<"over-tls">>, qos = 1,
             packet_id = DeliveryId} = tls_recv(Sub),
    ok = ssl:send(Sub, mqtt_frame:encode(#puback{packet_id = DeliveryId})),
    ok = ssl:close(Sub),
    ok = ssl:close(Pub).

%% Same reaping contract as the plain listener (L15): stopping the app must close
%% the TLS connections too, not leave ssl processes holding the port.
stopping_the_app_closes_the_tls_fleet(Config) ->
    enable_tls(Config),
    Port = boot(),
    Sock = tls_connect(Port),
    #connack{return_code = 0} =
        tls_request(Sock, #connect{client_id = <<"tls-reap">>, clean_session = 1}),
    %% `boot' already killed them; a second one now, then the app stop.
    stop_all(),
    ?assertEqual({error, closed}, ssl:recv(Sock, 0, 2000)).

%%%_* TLS client -------------------------------------------------------------

tls_connect(Port) ->
    {ok, Sock} = ssl:connect("localhost", Port,
                             [binary, {active, false}, {verify, verify_none},
                              {alpn_advertised_protocols, [<<"mqtt">>]}], 5000),
    %% Leftover bytes from the handshake are not possible with {active,false},
    %% but arming explicitly keeps the read path identical in every case.
    ok = ssl:setopts(Sock, [{active, false}]),
    Sock.

tls_request(Sock, Frame) ->
    ok = ssl:send(Sock, mqtt_frame:encode(Frame)),
    tls_recv(Sock).

tls_recv(Sock) ->
    tls_recv(Sock, <<>>).

tls_recv(Sock, Buf) ->
    case mqtt_frame:decode(Buf) of
        {ok, Frame, _Rest} ->
            Frame;
        {more, _} ->
            case ssl:recv(Sock, 0, 5000) of
                {ok, Data} -> tls_recv(Sock, <<Buf/binary, Data/binary>>);
                Other -> exit({tls_recv_failed, Other, Buf})
            end;
        {error, Reason} ->
            exit({tls_decode_failed, Reason, Buf})
    end.

%%%_* TLS configuration ------------------------------------------------------

enable_tls(Config) ->
    application:set_env(broker_security, tls,
                        #{enabled => true,
                          certfile => ?config(certfile, Config),
                          keyfile => ?config(keyfile, Config),
                          verify => verify_none}).

disable_tls() ->
    application:set_env(broker_security, tls, #{enabled => false}).

saved_tls() ->
    application:get_env(broker_security, tls, undefined).

restore_tls(undefined) ->
    application:unset_env(broker_security, tls);
restore_tls(Value) ->
    application:set_env(broker_security, tls, Value).

%% The certificate/key pair is generated per suite run so nothing in the repo is
%% a checked-in secret. SAN/EKU matter to real QUIC/TLS stacks; here `verify =>
%% verify_none' is the client side, but the openssl shape from
%% `broker_security_tls_SUITE' is reused so both suites fail together if openssl
%% changes its defaults.
self_signed(PrivDir) ->
    Dir = filename:join(["/tmp/opencode", "mqtt_tls_test"]),
    _ = file:del_dir_r(Dir),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    _ = PrivDir,
    Cert = filename:join(Dir, "cert.pem"),
    Key = filename:join(Dir, "key.pem"),
    Cmd = io_lib:format(
            "openssl req -x509 -newkey rsa:2048 -nodes -days 1 "
            "-subj /CN=localhost -addext subjectAltName=DNS:localhost "
            "-addext extendedKeyUsage=serverAuth "
            "-keyout ~s -out ~s 2>/dev/null",
            [Key, Cert]),
    _ = os:cmd(lists:flatten(Cmd)),
    case {filelib:is_regular(Cert), filelib:is_regular(Key)} of
        {true, true} -> {Cert, Key};
        _ -> exit({openssl_failed, Cert, Key})
    end.

%%%_* Boot / teardown -------------------------------------------------------

boot() ->
    ensure_partitions(),
    {ok, _Started} = application:ensure_all_started(broker_proto_mqtt),
    case ranch:get_port(?REF) of
        Port when is_integer(Port) -> Port;
        Other -> exit({listener_port_unavailable, Other})
    end.

ensure_partitions() ->
    lists:foreach(
      fun(P) ->
              case broker_queue:create_partition(P) of
                  ok -> ok;
                  {error, {already_exists, P}} -> ok;
                  {error, Other} -> exit({create_partition, P, Other})
              end
      end, broker_queue:partition_ids()).

stop_all() ->
    _ = application:stop(broker_proto_mqtt),
    _ = application:stop(broker_retain),
    _ = application:stop(broker_subscription),
    _ = application:stop(broker_queue),
    _ = application:stop(broker_router),
    _ = application:stop(broker_storage),
    cleanup_writers(),
    ok.

kill_conns() ->
    case whereis(mqtt_conn_sup) of
        undefined ->
            ok;
        Sup ->
            lists:foreach(
              fun({_, Pid, worker, _}) when is_pid(Pid) -> exit(Pid, kill);
                 (_) -> ok
              end, supervisor:which_children(Sup))
    end.

cleanup_writers() ->
    case whereis(broker_storage_wal_sup) of
        undefined ->
            ok;
        _Sup ->
            lists:foreach(
              fun({Id, _Child, _Type, _Modules}) ->
                      _ = supervisor:terminate_child(broker_storage_wal_sup, Id),
                      ok = supervisor:delete_child(broker_storage_wal_sup, Id)
              end,
              supervisor:which_children(broker_storage_wal_sup))
    end.
