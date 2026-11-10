%% @doc Boot a UnifiedMQ node whose only proxy listener is MQTT over WebSocket,
%% on an ephemeral port (ranch assigns a free one), print the real port on the
%% line `PORT <n>', then block until the process is killed.
%%
%% `mqtt_ws_compat.py' launches this via `erl -pa <app ebins> -noshell -s', reads
%% `PORT <n>', drives real `paho-mqtt' clients against it, then terminates the
%% node.
%%
%% This mirrors `mqtt_ws_conn_SUITE init_per_testcase' exactly -- same env, same
%% `port => 0' ephemeral binding, same `ranch:get_port' readback -- so the
%% third-party run exercises the identical boot path the CT suite does. The
%% MQTT engine's listener (`broker_proto_mqtt') is deliberately NOT started:
%% this node speaks MQTT only through WebSocket, so a passing case cannot have
%% accidentally gone over plain TCP.
-module(mqtt_ws_broker_boot).
-export([run/0]).

run() ->
    DataDir = os:getenv("BROKER_DATA_DIR", default_data_dir()),
    ok = filelib:ensure_dir(filename:join([DataDir, "x"])),
    _ = application:load(broker_storage),
    _ = application:load(broker_router),
    _ = application:load(broker_queue),
    _ = application:load(broker_subscription),
    _ = application:load(broker_event),
    _ = application:load(broker_security),
    _ = application:load(broker_retain),
    _ = application:load(broker_proto_mqtt),
    _ = application:load(broker_proto_mqtt_ws),
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
    %% The engine's knobs, at the application defaults the release ships.
    ok = application:set_env(broker_proto_mqtt, listeners, []),
    ok = application:set_env(broker_proto_mqtt, pull_page_limit, 32),
    ok = application:set_env(broker_proto_mqtt, max_inflight, 64),
    ok = application:set_env(broker_proto_mqtt, pull_poll_interval_ms, 100),
    ok = application:set_env(broker_proto_mqtt, max_keepalive_ms, 60000),
    ok = application:set_env(broker_proto_mqtt, mqtt_max_qos, 2),
    ok = application:set_env(broker_proto_mqtt, max_qos2_state_per_session, 100),
    ok = application:set_env(broker_proto_mqtt_ws, listeners,
                             [#{port => 0, ref => {mqtt_ws, 0},
                                num_acceptors => 4, max_connections => 128}]),
    ok = application:set_env(broker_proto_mqtt_ws, subprotocols,
                             [<<"mqtt">>, <<"mqttv3.1">>, <<"mqttv3.1.1">>]),
    ok = application:set_env(broker_proto_mqtt_ws, max_frame_bytes, 16777216),
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
    case application:ensure_all_started(broker_proto_mqtt_ws) of
        {ok, _} ->
            case ranch:get_port({mqtt_ws, 0}) of
                Port when is_integer(Port) ->
                    io:format("PORT ~p~n", [Port]),
                    io:format("MQTT-WS-BROKER-READY~n"),
                    receive after infinity -> ok end;
                Other ->
                    io:format("START-ERROR ranch port ~p~n", [Other]),
                    halt(1)
            end;
        {error, {App, Reason}} ->
            io:format("START-ERROR ~p ~p~n", [App, Reason]),
            halt(1)
    end.

default_data_dir() ->
    filename:join(["/home/wanli/workspace/unified-mq", "test",
                   "protocol-compatibility", "mqtt_ws", "data"]).
