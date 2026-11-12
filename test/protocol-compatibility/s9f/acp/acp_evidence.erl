%% S9-F evidence: broker-as-Client drives the OFFICIAL ACP Python SDK agent
%% (agent-client-protocol==1.0.0) through the production `acp_port' transport.
%% Every frame and every kernel projection below is real output.
-module(acp_evidence).

-export([run/0]).

-include_lib("broker_core/include/broker_message.hrl").
-include_lib("broker_router/include/broker_router.hrl").

-define(AGENT, <<"sdk-agent">>).
-define(PARTITIONS, 4).

run() ->
    DataDir = os:getenv("S9F_ACP_DATA",
                         "/tmp/opencode/s9f-evidence/data-acp"),
    Script = "/tmp/opencode/s9f-evidence/acp/acp_sdk_agent_tee.py",
    boot(DataDir),
    ok = application:set_env(broker_proto_acp, agents,
                             [#{name => ?AGENT,
                                command => <<"/usr/bin/python3">>,
                                args => [list_to_binary(Script)],
                                env => [{<<"PYTHONPATH">>,
                                         <<"/tmp/opencode/s9f-libs">>}],
                                transport => acp_port,
                                cwd => <<"/tmp/opencode/s9f-evidence/acp">>,
                                restart => temporary}]),
    ok = application:set_env(broker_proto_acp, permission_policy,
                             #{default => deny,
                               allow => [#{tool => <<"execute">>, title => any}],
                               timeout_ms => 30000,
                               notify_dest => <<"$acp/permission">>}),
    {ok, _} = application:ensure_all_started(broker_proto_acp),
    Pid = agent_pid(),
    Sid = wait_ready(Pid, 300),
    banner("HANDSHAKE (official ACP SDK agent over acp_port)",
           [{"state", maps:get(state, acp_agent:state(Pid))},
            {"session_id", Sid}]),
    plain(Pid, Sid),
    allow(Pid, Sid),
    deny(Pid, Sid),
    banner("FINAL", [{"state", maps:get(state, acp_agent:state(Pid))},
                     {"stop", maps:get(stop, acp_agent:state(Pid))}]),
    timer:sleep(500),
    ok.

%%%_* Scenarios --------------------------------------------------------------

plain(Pid, Sid) ->
    ok = acp_agent:prompt(Pid, Sid, prompt(<<"hello from the broker client">>)),
    timer:sleep(1200),
    dump("plain: session/update + turn", session_dest(Sid)).

allow(Pid, Sid) ->
    ok = acp_agent:prompt(Pid, Sid,
                          prompt(<<"danger: please run the command">>)),
    timer:sleep(1200),
    dump("permission(allow, kind=execute): session/update + turn",
         session_dest(Sid)),
    dump("permission(allow): $acp/permission", <<"$acp/permission">>).

deny(Pid, Sid) ->
    ok = acp_agent:prompt(Pid, Sid,
                          prompt(<<"deny: touch a config file">>)),
    timer:sleep(1200),
    dump("permission(deny, kind=edit): session/update + turn",
         session_dest(Sid)),
    dump("permission(deny): $acp/permission", <<"$acp/permission">>).

prompt(Text) ->
    [#{<<"type">> => <<"text">>, <<"text">> => Text}].

%%%_* Boot -------------------------------------------------------------------

boot(DataDir) ->
    ok = filelib:ensure_dir(filename:join(DataDir, "placeholder")),
    [application:load(A) || A <- [broker_storage, broker_router, broker_queue,
                                  broker_meta, broker_security,
                                  broker_proto_agent_rpc, broker_proto_acp]],
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok = application:set_env(broker_storage, data_dir, DataDir),
    ok = application:set_env(broker_storage, wal, #{require_durable_fs => false}),
    ok = application:set_env(broker_router, partitions, ?PARTITIONS),
    ok = application:set_env(broker_queue, partitions, ?PARTITIONS),
    ok = application:set_env(broker_meta, table_storage, ram_copies),
    ok = application:set_env(broker_meta, nodes, []),
    ok = application:set_env(broker_security, acl, allow),
    {ok, _} = application:ensure_all_started(broker_security),
    {ok, _} = application:ensure_all_started(broker_proto_agent_rpc),
    broker_meta:reset(),
    {ok, _} = application:ensure_all_started(broker_meta),
    {ok, _} = application:ensure_all_started(broker_storage),
    {ok, _} = application:ensure_all_started(broker_router),
    [ok = broker_queue:create_partition(P) || P <- broker_queue:partition_ids()],
    {ok, _} = application:ensure_all_started(broker_queue),
    ok.

agent_pid() ->
    [Pid] = [P || {_Id, P, worker, _} <- supervisor:which_children(broker_proto_acp_sup),
                   is_pid(P)],
    Pid.

wait_ready(_Pid, 0) ->
    error(agent_not_ready);
wait_ready(Pid, N) ->
    case acp_agent:state(Pid) of
        #{state := ready, session_id := Sid} when is_binary(Sid) ->
            Sid;
        _ ->
            timer:sleep(100),
            wait_ready(Pid, N - 1)
    end.

%%%_* Kernel reads -----------------------------------------------------------

session_dest(Sid) -> <<"$acp/session/", Sid/binary>>.

read_dest(Dest) ->
    From = case get({cursor, Dest}) of
               undefined -> 0;
               N -> N
           end,
    case broker_router:env() of
        {ok, Env} ->
            Ctx = #route_ctx{tenant_id = broker_security:default_tenant(),
                             publisher = <<"acp">>, reliability = os_buffer},
            {ok, [{Partition, _} | _]} = broker_router_hash:route(Dest, Ctx, Env),
            {ok, Messages, Next} = broker_queue:read_from(Partition, From, 100000),
            put({cursor, Dest}, Next),
            [M || {_Offset, #broker_message{destination = D} = M} <- Messages,
                  D =:= Dest];
        {error, _} ->
            []
    end.

dump(Label, Dest) ->
    Msgs = read_dest(Dest),
    io:format("~n== ~ts (~p msg) ==~n", [Label, length(Msgs)]),
    lists:foreach(
      fun(M) ->
              case acp_event:decode(M) of
                  {ok, Payload} ->
                      io:format("  ~ts~n",
                                [iolist_to_binary(json:encode(Payload))]);
                  {error, _} ->
                      io:format("  <undecodable>~n")
              end
      end, Msgs),
    ok.

banner(Label, Fields) ->
    io:format("~n== ~ts ==~n", [Label]),
    lists:foreach(fun({K, V}) -> io:format("  ~ts=~p~n", [K, V]) end, Fields),
    ok.
