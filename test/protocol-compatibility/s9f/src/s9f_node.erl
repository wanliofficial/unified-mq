%% S9-F evidence node: boots the full broker kernel with BOTH the MCP
%% (Streamable HTTP, 18096) and A2A (JSON-RPC, 18094) front ends on loopback,
%% plus the fake MCP backend responder. The three official Python SDK
%% client drivers (mcp / a2a-sdk / agent-client-protocol) talk to this node
%% over real HTTP / stdio. A2A needs no external backend: with
%% task_auto_complete=true the broker runs the SUBMITTED->COMPLETED lifecycle
%% itself.
-module(s9f_node).
-export([run/0, dump/1]).

-include_lib("broker_core/include/broker_message.hrl").
-include_lib("broker_router/include/broker_router.hrl").

run() ->
    %% DataDir is outside the repo tree by default (tmpfs for the
    %% evidence runs); override with S9F_DATA for a persistent run.
    DataDir = os:getenv("S9F_DATA", "/tmp/opencode/s9f-evidence/data"),
    _ = os:cmd("rm -rf " ++ DataDir),
    ok = filelib:ensure_dir(filename:join(DataDir, "x")),
    [application:load(A) || A <- [broker_storage, broker_router, broker_queue,
                                  broker_meta, broker_security, broker_event,
                                  broker_subscription, broker_retain,
                                  broker_metrics, broker_proto_agent_rpc]],
    application:set_env(broker_storage, backend, broker_storage_wal),
    application:set_env(broker_storage, data_dir, DataDir),
    application:set_env(broker_storage, wal, #{require_durable_fs => false}),
    application:set_env(broker_router, partitions, 4),
    application:set_env(broker_queue, partitions, 4),
    application:set_env(broker_meta, table_storage, ram_copies),
    application:set_env(broker_meta, nodes, []),
    application:set_env(broker_metrics, enabled, true),
    %% Two credentials: one per front end tenant, both loopback so auth is on.
    ok = application:set_env(broker_security, users,
                             [{<<"mcp-evidence">>,
                               #{token => <<"mcp-evidence-token">>,
                                 tenant_id => <<"t1">>}},
                              {<<"a2a-evidence">>,
                               #{token => <<"a2a-evidence-token">>,
                                 tenant_id => <<"t2">>}}]),
    application:set_env(broker_security, acl, allow),
    application:set_env(broker_security, require_auth, []),
    {ok, _} = application:ensure_all_started(broker_security),
    broker_meta:reset(),
    {ok, _} = application:ensure_all_started(broker_meta),
    {ok, _} = application:ensure_all_started(broker_storage),
    [ok = broker_queue:create_partition(P) || P <- broker_queue:partition_ids()],
    {ok, _} = application:ensure_all_started(broker_router),
    {ok, _} = application:ensure_all_started(broker_queue),
    _ = application:load(broker_proto_mcp),
    application:set_env(broker_proto_mcp, listeners,
                        [#{port => 18096, ref => {mcp, 18096},
                           num_acceptors => 4, max_connections => 64}]),
    application:set_env(broker_proto_mcp, require_auth, true),
    MCP_FEATURES = [tools, resources, prompts, completions],
    application:set_env(broker_proto_mcp, features, MCP_FEATURES),
    io:format("S9F_DEBUG mcp features set to ~p~n", [MCP_FEATURES]),
    application:set_env(broker_proto_mcp, sse_heartbeat_ms, 3000),
    application:set_env(broker_proto_mcp, request_timeout_ms, 5000),
    {ok, _} = application:ensure_all_started(broker_proto_mcp),
    ok = mcp_evidence_backend:start(),
    _ = application:load(broker_proto_a2a),
    application:set_env(broker_proto_a2a, listeners,
                        [#{port => 18094, ref => {a2a, 18094},
                           num_acceptors => 4, max_connections => 64}]),
    application:set_env(broker_proto_a2a, require_auth, true),
    application:set_env(broker_proto_a2a, task_shards, 4),
    application:set_env(broker_proto_a2a, task_reliability, fsync),
    application:set_env(broker_proto_a2a, task_auto_complete, true),
    application:set_env(broker_proto_a2a, sse_heartbeat_ms, 3000),
    application:set_env(broker_proto_a2a, sse_max_duration_ms, 60000),
    application:set_env(broker_proto_a2a, push_enabled, false),
    application:set_env(broker_proto_a2a, agent_cards,
                        [#{tenant_id => <<"t2">>, agent_id => <<"evidence-agent">>,
                           status => active,
                           card => #{<<"name">> => <<"unified-mq-evidence-agent">>,
                                     <<"description">> =>
                                         <<"S9-F evidence A2A agent">>,
                                     <<"version">> => <<"1.0.0">>,
                                     <<"supportedInterfaces">> =>
                                         [#{<<"url">> =>
                                               <<"http://127.0.0.1:18094/">>,
                                           <<"protocolBinding">> =>
                                               <<"JSONRPC">>}],
                                     <<"capabilities">> =>
                                         #{<<"streaming">> => false,
                                           <<"pushNotifications">> => false},
                                     <<"defaultInputModes">> => [<<"text">>],
                                     <<"defaultOutputModes">> => [<<"text">>],
                                     <<"skills">> =>
                                         [#{<<"id">> => <<"echo">>,
                                            <<"name">> => <<"echo">>,
                                            <<"description">> =>
                                                <<"Echo the task lifecycle">>}],
                                     <<"preferredTransport">> => <<"JSONRPC">>}}]),
    {ok, _} = application:ensure_all_started(broker_proto_a2a),
    io:format("S9F_NODE_READY mcp=18096 a2a=18094~n"),
    timer:sleep(1800000),
    halt().

dump(Dest) ->
    lists:flatmap(
      fun(P) ->
              case broker_queue:watermarks(P) of
                  {ok, 0, _} -> [];
                  {ok, High, _} ->
                      case broker_queue:read_from(P, 0, max(High, 1)) of
                          {ok, Msgs, _} ->
                              [{P, O, binary_to_list(M#broker_message.message_id),
                                binary_to_list(M#broker_message.payload)}
                               || {O, M} <- Msgs,
                                  M#broker_message.destination =:= Dest];
                          Other -> [{P, error, Other, ""}]
                      end;
                  Other -> [{P, error, Other, ""}]
              end
      end, broker_queue:partition_ids()).
