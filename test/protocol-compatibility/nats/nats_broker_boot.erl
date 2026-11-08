%% @doc Boot a UnifiedMQ node whose only listener is the NATS one, on an
%% ephemeral port (ranch assigns a free one), print the real port on the line
%% `PORT <n>', then block until the process is killed. The Python driver
%% (`nats_compat.py') launches this via `erl -pa <app ebins> -noshell -s', reads
%% `PORT <n>' from stdout, runs the real Go `nats.go' client against it, then
%% terminates the node.
%%
%% This mirrors `nats_conn_SUITE init_per_testcase' exactly -- same env, same
%% `port => 0' ephemeral binding, same `ranch:get_port' readback -- so the
%% third-party-client run exercises the identical boot path the suite does.
%% The listener requires the token `compat-secret', so the real client also
%% proves CONNECT authentication end to end (the suite's auth case uses a
%% wrong token; this one uses the right one).
-module(nats_broker_boot).
-export([run/0]).

-define(TOKEN, <<"compat-secret">>).

run() ->
    DataDir = os:getenv("BROKER_DATA_DIR",
                        "/home/wanli/workspace/unified-mq/test/protocol-compatibility/nats/data"),
    ok = filelib:ensure_dir(filename:join([DataDir, "x"])),
    _ = application:load(broker_storage),
    _ = application:load(broker_router),
    _ = application:load(broker_queue),
    _ = application:load(broker_subscription),
    _ = application:load(broker_event),
    _ = application:load(broker_security),
    _ = application:load(broker_retain),
    _ = application:load(broker_proto_nats),
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
    ok = application:set_env(broker_proto_nats, listeners,
                             [#{port => 0, ref => {nats, 0},
                                num_acceptors => 4, max_connections => 128}]),
    ok = application:set_env(broker_proto_nats, max_payload, 1048576),
    ok = application:set_env(broker_proto_nats, shared_poll_ms, 50),
    ok = application:set_env(broker_proto_nats, auth_token, ?TOKEN),
    %% Same ordering as nats_conn_SUITE init_per_testcase: start broker_storage,
    %% create the partitions in storage, THEN start the app that cascades
    %% broker_queue (whose supervisor starts one worker per pre-created
    %% partition).
    {ok, _} = application:ensure_all_started(broker_storage),
    lists:foreach(
      fun(P) ->
              case broker_queue:create_partition(P) of
                  ok -> ok;
                  {error, {already_exists, P}} -> ok;
                  {error, Other} -> halt({create_partition, P, Other})
              end
      end, broker_queue:partition_ids()),
    case application:ensure_all_started(broker_proto_nats) of
        {ok, _} ->
            case ranch:get_port({nats, 0}) of
                Port when is_integer(Port) ->
                    io:format("PORT ~p~n", [Port]),
                    io:format("NATS-BROKER-READY~n"),
                    receive after infinity -> ok end;
                Other ->
                    io:format("START-ERROR ranch port ~p~n", [Other]),
                    halt(1)
            end;
        {error, {App, Reason}} ->
            io:format("START-ERROR ~p ~p~n", [App, Reason]),
            halt(1)
    end.
