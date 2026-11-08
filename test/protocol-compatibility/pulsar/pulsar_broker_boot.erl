%% @doc Boot a UnifiedMQ node whose only listener is the Pulsar one, on an
%% ephemeral port, print `PORT <n>', then block. Mirrors
%% `pulsar_conn_SUITE init_per_testcase'.
-module(pulsar_broker_boot).
-export([run/0]).

run() ->
    DataDir = os:getenv("BROKER_DATA_DIR",
                        "/home/wanli/workspace/unified-mq/test/protocol-compatibility/pulsar/data"),
    ok = filelib:ensure_dir(filename:join([DataDir, "x"])),
    _ = application:load(broker_storage),
    _ = application:load(broker_router),
    _ = application:load(broker_queue),
    _ = application:load(broker_subscription),
    _ = application:load(broker_event),
    _ = application:load(broker_security),
    _ = application:load(broker_retain),
    _ = application:load(broker_proto_pulsar),
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok = application:set_env(broker_storage, data_dir, DataDir),
    ok = application:set_env(broker_storage, wal, #{require_durable_fs => false}),
    ok = application:set_env(broker_queue, partitions, 1),
    ok = application:set_env(broker_router, partitions, 1),
    ok = application:set_env(broker_subscription, data_dir, DataDir),
    ok = application:set_env(broker_retain, data_dir, DataDir),
    ok = application:set_env(broker_event, policy, pass_through),
    ok = application:set_env(broker_security, users, []),
    ok = application:set_env(broker_security, acl, allow),
    ok = application:set_env(broker_proto_pulsar, advertised_host, <<"127.0.0.1">>),
    ok = application:set_env(broker_proto_pulsar, listeners,
                             [#{port => 0, ref => {pulsar, 0},
                                num_acceptors => 4, max_connections => 128}]),
    {ok, _} = application:ensure_all_started(broker_storage),
    lists:foreach(
      fun(P) ->
              case broker_queue:create_partition(P) of
                  ok -> ok;
                  {error, {already_exists, P}} -> ok;
                  {error, Other} -> halt({create_partition, P, Other})
              end
      end, broker_queue:partition_ids()),
    case application:ensure_all_started(broker_proto_pulsar) of
        {ok, _} ->
            case ranch:get_port({pulsar, 0}) of
                Port when is_integer(Port) ->
                    io:format("PORT ~p~n", [Port]),
                    io:format("PULSAR-BROKER-READY~n"),
                    receive after infinity -> ok end;
                Other ->
                    io:format("START-ERROR ranch port ~p~n", [Other]),
                    halt(1)
            end;
        {error, {App, Reason}} ->
            io:format("START-ERROR ~p ~p~n", [App, Reason]),
            halt(1)
    end.
