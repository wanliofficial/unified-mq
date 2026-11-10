%% @doc Boot a UnifiedMQ node whose only listener is the MQTT-SN UDP gateway, on
%% an ephemeral port, print the real port on the line `PORT <n>', then block
%% until the process is killed.
%%
%% `mqtt_sn_compat.py' launches this via `erl -pa <app ebins> -noshell -s', reads
%% `PORT <n>', drives the real `mqtt-sn-tools' binaries against it, then
%% terminates the node.
%%
%% This mirrors `mqtt_sn_conn_SUITE init_per_testcase' -- same env, same
%% `port => 0' ephemeral binding, same port readback -- so the third-party run
%% exercises the identical boot path the CT suite does. No MQTT (TCP) listener is
%% started: this node speaks only MQTT-SN over UDP, so a passing case cannot have
%% accidentally gone over another protocol.
-module(mqtt_sn_broker_boot).
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
    _ = application:load(broker_proto_mqtt_sn),
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
    ok = application:set_env(broker_proto_mqtt_sn, listeners, [#{port => 0}]),
    %% The predefined table the `sn-predefined-topic' case names as id 1; the
    %% other cases use normal or short topics and never touch it.
    ok = application:set_env(broker_proto_mqtt_sn, predefined_topics,
                             #{1 => <<"sn/pre">>}),
    ok = application:set_env(broker_proto_mqtt_sn, gateway_id, 1),
    ok = application:set_env(broker_proto_mqtt_sn, advertise_interval_ms, 0),
    ok = application:set_env(broker_proto_mqtt_sn, max_inflight, 20),
    ok = application:set_env(broker_proto_mqtt_sn, max_duration_s, 3600),
    ok = application:set_env(broker_proto_mqtt_sn, max_asleep_clients, 1024),
    ok = application:set_env(broker_proto_mqtt_sn, max_topic_ids_per_client, 1024),
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
    case application:ensure_all_started(broker_proto_mqtt_sn) of
        {ok, _} ->
            case listener_pid() of
                Pid when is_pid(Pid) ->
                    Port = mqtt_sn_listener:port(Pid),
                    io:format("PORT ~p~n", [Port]),
                    io:format("MQTT-SN-BROKER-READY~n"),
                    receive after infinity -> ok end;
                Other ->
                    io:format("START-ERROR listener ~p~n", [Other]),
                    halt(1)
            end;
        {error, {App, Reason}} ->
            io:format("START-ERROR ~p ~p~n", [App, Reason]),
            halt(1)
    end.

listener_pid() ->
    case whereis(broker_proto_mqtt_sn_sup) of
        undefined ->
            undefined;
        _ ->
            case [Pid || {Id, Pid, worker, _} <-
                              supervisor:which_children(broker_proto_mqtt_sn_sup),
                          element(1, Id) =:= mqtt_sn_listener] of
                [Pid | _] -> Pid;
                [] -> undefined
            end
    end.

default_data_dir() ->
    filename:join(["/home/wanli/workspace/unified-mq", "test",
                   "protocol-compatibility", "mqtt_sn", "data"]).
