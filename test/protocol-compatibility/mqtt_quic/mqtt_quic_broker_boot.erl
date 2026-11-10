%% @doc Boot a UnifiedMQ node whose only listener is the QUIC front end, on an
%% ephemeral UDP port, print the real port on the line `PORT <n>', then block
%% until the process is killed.
%%
%% `run.sh' launches this via `erl -pa <app ebins> -noshell -s', reads `PORT <n>',
%% drives the third-party `emqtt' client against it, then terminates the node.
%%
%% This mirrors `mqtt_quic_listener_SUITE:boot_broker/1' -- same env, same
%% `port => 0' ephemeral binding, same port readback -- so the third-party run
%% exercises the identical boot path the CT suite does. No MQTT (TCP) or
%% MQTT-over-WS listener is started: this node speaks only MQTT over QUIC, so a
%% passing case cannot have accidentally gone over another protocol.
%%
%% QUIC has no cleartext mode (the handshake *is* TLS 1.3), so the certificate
%% pair is passed to the listener explicitly instead of through
%% `broker_security.tls'. `run.sh' generates it once per run with openssl.
-module(mqtt_quic_broker_boot).
-export([run/0]).

run() ->
    DataDir = os:getenv("BROKER_DATA_DIR", default_data_dir()),
    CertFile = os:getenv("BROKER_CERTFILE", filename:join([DataDir, "tls", "cert.pem"])),
    KeyFile = os:getenv("BROKER_KEYFILE", filename:join([DataDir, "tls", "key.pem"])),
    ok = filelib:ensure_dir(filename:join([DataDir, "x"])),
    %% Load first, `set_env' second: `set_env' on an app the controller has not
    %% loaded yet is discarded when the `.app' file's defaults are loaded.
    lists:foreach(fun(App) -> _ = application:load(App) end, apps()),
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok = application:set_env(broker_storage, data_dir, DataDir),
    ok = application:set_env(broker_storage, wal, #{require_durable_fs => false}),
    ok = application:set_env(broker_queue, partitions, 2),
    ok = application:set_env(broker_router, partitions, 2),
    ok = application:set_env(broker_subscription, data_dir, DataDir),
    ok = application:set_env(broker_retain, data_dir, DataDir),
    ok = application:set_env(broker_event, policy, pass_through),
    ok = application:set_env(broker_security, users, []),
    ok = application:set_env(broker_security, acl, allow),
    ok = application:set_env(broker_security, require_auth, []),
    %% The QUIC listener: ephemeral port, ALPN `mqtt' (the only value CN Draft 3
    %% defines), and the certificate pair `run.sh' generated.
    ok = application:set_env(broker_proto_mqtt_quic, listeners,
                             [#{port => 0, certfile => CertFile, keyfile => KeyFile}]),
    ok = application:set_env(broker_proto_mqtt_quic, alpn, [<<"mqtt">>]),
    ok = application:set_env(broker_proto_mqtt_quic, handshake_timeout_ms, 5000),
    ok = application:set_env(broker_proto_mqtt_quic, idle_timeout_ms, 30000),
    ok = application:set_env(broker_proto_mqtt_quic, max_streams_per_conn, 4),
    %% The MQTT engine's own defaults, spelled out like the CT suites do.
    ok = application:set_env(broker_proto_mqtt, pull_page_limit, 32),
    ok = application:set_env(broker_proto_mqtt, max_inflight, 64),
    ok = application:set_env(broker_proto_mqtt, pull_poll_interval_ms, 100),
    ok = application:set_env(broker_proto_mqtt, max_keepalive_ms, 60000),
    ok = application:set_env(broker_proto_mqtt, mqtt_max_qos, 2),
    ok = application:set_env(broker_proto_mqtt, max_qos2_state_per_session, 100),
    %% Same ordering as the CT suite: storage first, partitions created in
    %% storage, THEN the app whose supervisor starts one queue worker per
    %% pre-created partition.
    {ok, _} = application:ensure_all_started(broker_storage),
    lists:foreach(
      fun(P) ->
              case broker_queue:create_partition(P) of
                  ok -> ok;
                  {error, {already_exists, P}} -> ok;
                  {error, Other} -> halt({create_partition, P, Other})
              end
      end, broker_queue:partition_ids()),
    case application:ensure_all_started(broker_proto_mqtt_quic) of
        {ok, _} ->
            case listener_pid() of
                Pid when is_pid(Pid) ->
                    io:format("PORT ~p~n", [mqtt_quic_listener:port(Pid)]),
                    io:format("MQTT-QUIC-BROKER-READY~n"),
                    receive after infinity -> ok end;
                Other ->
                    io:format("START-ERROR listener ~p~n", [Other]),
                    halt(1)
            end;
        {error, {App, Reason}} ->
            io:format("START-ERROR ~p ~p~n", [App, Reason]),
            halt(1)
    end.

apps() ->
    [broker_storage, broker_router, broker_queue, broker_subscription,
     broker_retain, broker_event, broker_security, broker_proto_mqtt,
     broker_proto_mqtt_quic].

listener_pid() ->
    case whereis(broker_proto_mqtt_quic_sup) of
        undefined ->
            undefined;
        _ ->
            case [Pid || {Id, Pid, worker, _} <-
                              supervisor:which_children(broker_proto_mqtt_quic_sup),
                          element(1, Id) =:= mqtt_quic_listener] of
                [Pid | _] -> Pid;
                [] -> undefined
            end
    end.

default_data_dir() ->
    filename:join(["/home/wanli/workspace/unified-mq", "test",
                   "protocol-compatibility", "mqtt_quic", "data"]).
