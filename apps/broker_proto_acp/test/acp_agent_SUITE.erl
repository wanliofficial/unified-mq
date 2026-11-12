%% @doc S9-E: the ACP client state machine over an in-memory transport.
%%
%% The suite drives the real `acp_agent' gen_server with `acp_fake_transport':
%% every frame the client writes lands in the test process's mailbox, and the test
%% answers with `acp_agent:deliver/2'. Nothing about the handshake, correlation,
%% policy or turn fold is stubbed.
%%
%% Grouping:
%%   * `handshake'  -- initialize params, version gate, session/new, stop paths;
%%   * `permission' -- allow / deny / audit record / unadvertised callbacks;
%%   * `turn'       -- prompt, session/update forwarding, stopReason, cancel,
%%                     and the envelope failure paths.
-module(acp_agent_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("broker_core/include/broker_message.hrl").
-include_lib("broker_router/include/broker_router.hrl").

-compile([export_all, nowarn_export_all]).

-define(PARTITIONS, 4).
-define(AGENT, <<"agent-a">>).

all() ->
    [{group, handshake}, {group, permission}, {group, turn}].

groups() ->
    [{handshake, [sequence],
      [initialize_advertises_no_fs_or_terminal,
       initialize_then_session_new,
       version_mismatch_stops_before_session_new,
       session_new_without_id_is_refused]},
     {permission, [sequence],
      [allowlisted_tool_is_granted,
       non_allowlisted_tool_is_denied,
       every_decision_is_audited,
       fs_read_is_refused,
       terminal_create_is_refused,
       unknown_callback_is_refused]},
     {turn, [sequence],
      [prompt_sends_session_prompt,
       session_update_is_forwarded,
       prompt_response_records_stop_reason,
       cancel_sends_session_cancel,
       prompt_from_kernel_destination,
       malformed_frame_is_32700,
       message_without_jsonrpc_is_32600]}].

%%%_* Suite fixture ----------------------------------------------------------

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(broker_security),
    {ok, _} = application:ensure_all_started(broker_proto_agent_rpc),
    DataDir = filename:join(?config(priv_dir, Config), "acp-data"),
    ok = filelib:ensure_dir(filename:join(DataDir, "placeholder")),
    _ = application:load(broker_storage),
    _ = application:load(broker_router),
    _ = application:load(broker_queue),
    _ = application:load(broker_meta),
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok = application:set_env(broker_storage, data_dir, DataDir),
    ok = application:set_env(broker_storage, wal, #{require_durable_fs => false}),
    ok = application:set_env(broker_router, partitions, ?PARTITIONS),
    ok = application:set_env(broker_queue, partitions, ?PARTITIONS),
    ok = application:set_env(broker_meta, table_storage, ram_copies),
    ok = application:set_env(broker_meta, nodes, []),
    broker_meta:reset(),
    {ok, _} = application:ensure_all_started(broker_meta),
    {ok, _} = application:ensure_all_started(broker_storage),
    {ok, _} = application:ensure_all_started(broker_router),
    [ok = broker_queue:create_partition(P) || P <- broker_queue:partition_ids()],
    {ok, _} = application:ensure_all_started(broker_queue),
    _ = application:load(broker_proto_acp),
    OldAcl = application:get_env(broker_security, acl),
    ok = application:set_env(broker_security, acl, allow),
    [{data_dir, DataDir}, {old_acl, OldAcl} | Config].

end_per_suite(Config) ->
    case ?config(old_acl, Config) of
        {ok, V} -> application:set_env(broker_security, acl, V);
        undefined -> ok
    end,
    ok.

init_per_testcase(_Case, Config) ->
    %% A permissive policy by default; individual cases narrow it.
    ok = application:set_env(broker_proto_acp, permission_policy,
                             #{default => deny,
                               allow => [#{tool => <<"edit">>, title => any}],
                               timeout_ms => 30000,
                               notify_dest => <<"$acp/permission">>}),
    ok = application:set_env(broker_proto_acp, supported_protocol_version, 1),
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

%%%_* handshake --------------------------------------------------------------

initialize_advertises_no_fs_or_terminal(_Config) ->
    Pid = start_agent(),
    Frame = expect_sent(),
    ?assertEqual(<<"initialize">>, maps:get(<<"method">>, Frame)),
    Params = maps:get(<<"params">>, Frame),
    ?assertEqual(1, maps:get(<<"protocolVersion">>, Params)),
    Caps = maps:get(<<"clientCapabilities">>, Params),
    %% The absence of fs/terminal is the capability gate, not a comment.
    ?assertNot(maps:is_key(<<"fs">>, Caps)),
    ?assertNot(maps:is_key(<<"terminal">>, Caps)),
    ?assertNot(maps:is_key(<<"elicitation">>, Caps)),
    stop_agent(Pid).

initialize_then_session_new(_Config) ->
    Pid = start_agent(),
    InitId = reply_initialize(Pid),
    _ = InitId,
    Frame = expect_sent(),
    ?assertEqual(<<"session/new">>, maps:get(<<"method">>, Frame)),
    Params = maps:get(<<"params">>, Frame),
    ?assertEqual([], maps:get(<<"mcpServers">>, Params)),
    ?assert(maps:is_key(<<"cwd">>, Params)),
    reply(Pid, maps:get(<<"id">>, Frame), #{<<"sessionId">> => <<"s1">>}),
    ?assertEqual(ready, session_state(Pid)),
    ?assertEqual(<<"s1">>, session_id(Pid)),
    stop_agent(Pid).

version_mismatch_stops_before_session_new(_Config) ->
    Pid = start_agent(),
    InitFrame = expect_sent(),
    reply(Pid, maps:get(<<"id">>, InitFrame),
          #{<<"protocolVersion">> => 2}),
    %% No `session/new' follows: the negotiation failed, and a v2 agent must not
    %% be silently downgraded.
    ?assertEqual(timeout, expect_sent(300)),
    receive
        {'EXIT', Pid, _} -> ok
    after 0 -> ok
    end,
    ?assertEqual(initializing, session_state(Pid)),
    stop_agent(Pid).

session_new_without_id_is_refused(_Config) ->
    Pid = start_agent(),
    _ = reply_initialize(Pid),
    NewFrame = expect_sent(),
    reply(Pid, maps:get(<<"id">>, NewFrame), #{}),
    ?assertEqual(initializing, session_state(Pid)),
    ?assertEqual(undefined, session_id(Pid)),
    stop_agent(Pid).

%%%_* permission -------------------------------------------------------------

allowlisted_tool_is_granted(_Config) ->
    Pid = start_agent(),
    S = handshake(Pid),
    Id = 900,
    request_from_agent(Pid, Id, <<"session/request_permission">>,
                       permission_params(S, <<"edit">>, <<"Write file">>)),
    Reply = expect_reply(Id),
    Result = maps:get(<<"result">>, Reply),
    ?assertEqual(<<"selected">>, maps:get(<<"outcome">>, Result)),
    ?assertEqual(<<"allow">>, maps:get(<<"optionId">>, Result)),
    stop_agent(Pid).

non_allowlisted_tool_is_denied(_Config) ->
    Pid = start_agent(),
    S = handshake(Pid),
    Id = 901,
    request_from_agent(Pid, Id, <<"session/request_permission">>,
                       permission_params(S, <<"execute">>, <<"rm -rf /">>)),
    Result = maps:get(<<"result">>, expect_reply(Id)),
    ?assertEqual(<<"cancelled">>, maps:get(<<"outcome">>, Result)),
    %% A refusal ends the session: the agent cannot continue a turn whose
    %% sensitive call was not approved.
    ?assertEqual(gone, session_state(Pid)),
    ?assertEqual(<<"refusal">>, stop_reason(Pid)),
    stop_agent(Pid).

every_decision_is_audited(_Config) ->
    Pid = start_agent(),
    S = handshake(Pid),
    ToolCallId = unique_tool_call(),
    Params = permission_params(S, <<"edit">>, <<"ok">>, ToolCallId),
    request_from_agent(Pid, 910, <<"session/request_permission">>, Params),
    _ = expect_reply(910),
    Audits = [D || M <- messages(acp_event:permission_destination()),
                   {ok, D} <- [acp_event:decode(M)],
                   maps:get(<<"toolCallId">>,
                            maps:get(<<"toolCall">>, D)) =:= ToolCallId],
    ?assertMatch([_], Audits),
    [Decoded] = Audits,
    ?assertEqual(?AGENT, maps:get(<<"agent">>, Decoded)),
    ?assertEqual(S, maps:get(<<"sessionId">>, Decoded)),
    ?assertEqual(<<"selected">>, maps:get(<<"decision">>, Decoded)),
    stop_agent(Pid).

fs_read_is_refused(_Config) ->
    Pid = start_agent(),
    _ = handshake(Pid),
    request_from_agent(Pid, 920, <<"fs/read_text_file">>,
                       #{<<"path">> => <<"/etc/passwd">>}),
    Reply = expect_reply(920),
    ?assertEqual(-32601, error_code(Reply)),
    ?assertEqual(ready, session_state(Pid)),
    stop_agent(Pid).

terminal_create_is_refused(_Config) ->
    Pid = start_agent(),
    _ = handshake(Pid),
    request_from_agent(Pid, 921, <<"terminal/create">>,
                       #{<<"command">> => <<"sh">>}),
    ?assertEqual(-32601, error_code(expect_reply(921))),
    stop_agent(Pid).

unknown_callback_is_refused(_Config) ->
    Pid = start_agent(),
    _ = handshake(Pid),
    request_from_agent(Pid, 922, <<"agent/invented">>, #{}),
    ?assertEqual(-32601, error_code(expect_reply(922))),
    stop_agent(Pid).

%%%_* turn -------------------------------------------------------------------

prompt_sends_session_prompt(_Config) ->
    Pid = start_agent(),
    S = handshake(Pid),
    ok = acp_agent:prompt(Pid, S, [#{<<"type">> => <<"text">>,
                                     <<"text">> => <<"hi">>}]),
    Frame = expect_sent(),
    ?assertEqual(<<"session/prompt">>, maps:get(<<"method">>, Frame)),
    Params = maps:get(<<"params">>, Frame),
    ?assertEqual(S, maps:get(<<"sessionId">>, Params)),
    ?assertEqual([#{<<"type">> => <<"text">>, <<"text">> => <<"hi">>}],
                 maps:get(<<"prompt">>, Params)),
    ?assertEqual(prompting, session_state(Pid)),
    stop_agent(Pid).

session_update_is_forwarded(_Config) ->
    Pid = start_agent(),
    S = handshake(Pid),
    ok = acp_agent:prompt(Pid, S, [#{<<"type">> => <<"text">>}]),
    _ = expect_sent(),
    notify_from_agent(Pid, <<"session/update">>,
                      #{<<"sessionId">> => S,
                        <<"update">> => #{<<"sessionUpdate">> =>
                                              <<"agent_message_chunk">>,
                                          <<"content">> => <<"hello">>}}),
    ok = sync(Pid),
    ?assertMatch([_], messages(acp_event:session_destination(S))),
    %% And the forwarded record carries the untouched update object.
    [Msg] = messages(acp_event:session_destination(S)),
    {ok, Body} = acp_event:decode(Msg),
    ?assertEqual(<<"session_update">>, maps:get(<<"kind">>, Body)),
    Update = maps:get(<<"update">>, Body),
    ?assertEqual(<<"agent_message_chunk">>,
                 maps:get(<<"sessionUpdate">>, Update)),
    ?assertEqual(<<"hello">>, maps:get(<<"content">>, Update)),
    stop_agent(Pid).

prompt_response_records_stop_reason(_Config) ->
    Pid = start_agent(),
    S = handshake(Pid),
    ok = acp_agent:prompt(Pid, S, [#{<<"type">> => <<"text">>}]),
    PromptFrame = expect_sent(),
    notify_from_agent(Pid, <<"session/update">>,
                      #{<<"update">> => #{<<"sessionUpdate">> =>
                                              <<"agent_message_chunk">>}}),
    reply(Pid, maps:get(<<"id">>, PromptFrame), #{<<"stopReason">> => <<"end_turn">>}),
    ?assertEqual(ready, session_state(Pid)),
    ?assertEqual(<<"end_turn">>, stop_reason(Pid)),
    %% The finished turn is a kernel message, and it carries the stopReason.
    Msgs = messages(acp_event:session_destination(S)),
    Turns = [D || M <- Msgs, {ok, D} <- [acp_event:decode(M)],
                  maps:get(<<"kind">>, D) =:= <<"turn">>],
    ?assertMatch([_], Turns),
    [Decoded] = Turns,
    Turn = maps:get(<<"turn">>, Decoded),
    ?assertEqual(<<"end_turn">>, maps:get(<<"stopReason">>, Turn)),
    ?assertEqual(1, maps:get(<<"agent_message_chunk">>,
                             maps:get(<<"counts">>, Turn))),
    stop_agent(Pid).

cancel_sends_session_cancel(_Config) ->
    Pid = start_agent(),
    S = handshake(Pid),
    ok = acp_agent:prompt(Pid, S, [#{<<"type">> => <<"text">>}]),
    _ = expect_sent(),
    ok = acp_agent:cancel(Pid),
    Frame = expect_sent(),
    ?assertEqual(<<"session/cancel">>, maps:get(<<"method">>, Frame)),
    ?assertNot(maps:is_key(<<"id">>, Frame)),
    ?assertEqual(ready, session_state(Pid)),
    ?assertEqual(<<"cancelled">>, stop_reason(Pid)),
    %% Cancelling again is a no-op, not a second frame.
    ok = acp_agent:cancel(Pid),
    ?assertEqual(timeout, expect_sent(200)),
    stop_agent(Pid).

prompt_from_kernel_destination(_Config) ->
    Pid = start_agent(),
    S = handshake(Pid),
    %% `handshake/1' casts the `session/new' reply, so the subscription is
    %% installed only once that cast is processed. A sync call is the barrier:
    %% appending before it would publish to a partition this agent does not yet
    %% follow, and a transient reader never replays the past.
    ok = sync(Pid),
    %% A prompt arrives on `$acp/in/<agent>' like any other kernel message.
    {ok, _} = acp_event:append(acp_event:in_destination(?AGENT),
                               broker_security:default_tenant(),
                               #{<<"sessionId">> => S,
                                 <<"prompt">> => [#{<<"type">> => <<"text">>,
                                                    <<"text">> => <<"go">>}]}),
    Frame = wait_for_method(<<"session/prompt">>, 20),
    ?assertEqual(S, maps:get(<<"sessionId">>, maps:get(<<"params">>, Frame))),
    stop_agent(Pid).

malformed_frame_is_32700(_Config) ->
    Pid = start_agent(),
    InitFrame = expect_sent(),
    _ = InitFrame,
    acp_agent:deliver(Pid, <<"this is not json\n">>),
    Reply = expect_any_reply(),
    ?assertEqual(-32700, error_code(Reply)),
    ?assertEqual(null, maps:get(<<"id">>, Reply)),
    %% The connection survives a bad frame.
    ?assertEqual(initializing, session_state(Pid)),
    stop_agent(Pid).

message_without_jsonrpc_is_32600(_Config) ->
    Pid = start_agent(),
    _ = expect_sent(),
    acp_agent:deliver(Pid, <<"{\"id\":5,\"method\":\"m\"}\n">>),
    Reply = expect_reply(5),
    ?assertEqual(-32600, error_code(Reply)),
    stop_agent(Pid).

%%%_* Helpers ----------------------------------------------------------------

start_agent() ->
    Spec = #{name => ?AGENT,
             transport => acp_fake_transport,
             fake_owner => self(),
             command => <<"/bin/false">>},
    {ok, Pid} = acp_agent:start_link(Spec),
    unlink(Pid),
    Pid.

stop_agent(Pid) ->
    _ = catch gen_server:stop(Pid),
    flush().

reply_initialize(Pid) ->
    Frame = expect_sent(),
    Id = maps:get(<<"id">>, Frame),
    reply(Pid, Id, #{<<"protocolVersion">> => 1,
                     <<"agentCapabilities">> => #{},
                     <<"authMethods">> => []}),
    Id.

%% Each case negotiates its own session id: the kernel's partitions are shared
%% for the whole suite, so a unique destination is what keeps assertions from
%% seeing an earlier case's traffic.
handshake(Pid) ->
    SessionId = unique_session(),
    _ = reply_initialize(Pid),
    NewFrame = expect_sent(),
    reply(Pid, maps:get(<<"id">>, NewFrame), #{<<"sessionId">> => SessionId}),
    SessionId.

unique_session() ->
    list_to_binary("s-" ++ integer_to_list(erlang:unique_integer([positive]))).

unique_tool_call() ->
    list_to_binary("t-" ++ integer_to_list(erlang:unique_integer([positive]))).

reply(Pid, Id, Result) ->
    Frame = #{<<"jsonrpc">> => <<"2.0">>, <<"id">> => Id,
              <<"result">> => Result},
    acp_agent:deliver(Pid, [agent_rpc_json:encode(Frame), "\n"]).

%% A cast is asynchronous: a following call on the same gen_server is the
%% barrier that guarantees the frame has been folded before the test reads the
%% kernel log.
sync(Pid) ->
    _ = acp_agent:state(Pid),
    ok.

notify_from_agent(Pid, Method, Params) ->
    Frame = #{<<"jsonrpc">> => <<"2.0">>, <<"method">> => Method,
              <<"params">> => Params},
    acp_agent:deliver(Pid, [agent_rpc_json:encode(Frame), "\n"]).

request_from_agent(Pid, Id, Method, Params) ->
    Frame = #{<<"jsonrpc">> => <<"2.0">>, <<"id">> => Id,
              <<"method">> => Method, <<"params">> => Params},
    acp_agent:deliver(Pid, [agent_rpc_json:encode(Frame), "\n"]).

permission_params(SessionId, Kind, Title) ->
    permission_params(SessionId, Kind, Title, unique_tool_call()).

permission_params(SessionId, Kind, Title, ToolCallId) ->
    #{<<"sessionId">> => SessionId,
      <<"toolCall">> => #{<<"toolCallId">> => ToolCallId,
                          <<"kind">> => Kind,
                          <<"title">> => Title},
      <<"options">> => [#{<<"optionId">> => <<"allow">>,
                          <<"kind">> => <<"allow_once">>,
                          <<"name">> => <<"Allow">>},
                        #{<<"optionId">> => <<"reject">>,
                          <<"kind">> => <<"reject_once">>,
                          <<"name">> => <<"Reject">>}]}.

expect_sent() ->
    expect_sent(2000).

expect_sent(Timeout) ->
    receive
        {acp_sent, _Port, Bin} ->
            {ok, Term} = acp_frame:decode(Bin),
            Term
    after Timeout ->
        timeout
    end.

%% Skip frames the handshake or an earlier step already produced.
wait_for_method(Method, Tries) when Tries > 0 ->
    case expect_sent() of
        timeout -> timeout;
        Frame ->
            case maps:get(<<"method">>, Frame, undefined) of
                Method -> Frame;
                _ -> wait_for_method(Method, Tries - 1)
            end
    end.

expect_reply(Id) ->
    receive
        {acp_sent, _Port, Bin} ->
            {ok, Term} = acp_frame:decode(Bin),
            case maps:get(<<"id">>, Term, undefined) of
                Id -> Term;
                _ -> expect_reply(Id)
            end
    after 2000 ->
        ct:fail({no_reply_for_id, Id})
    end.

expect_any_reply() ->
    receive
        {acp_sent, _Port, Bin} ->
            {ok, Term} = acp_frame:decode(Bin),
            Term
    after 2000 ->
        ct:fail(no_reply)
    end.

error_code(#{<<"error">> := Error}) ->
    maps:get(<<"code">>, Error).

session_state(Pid) ->
    maps:get(state, acp_agent:state(Pid)).

session_id(Pid) ->
    maps:get(session_id, acp_agent:state(Pid)).

stop_reason(Pid) ->
    maps:get(stop, acp_agent:state(Pid)).

%% Read every message on a destination through the kernel read path.
messages(Dest) ->
    case broker_router:env() of
        {ok, Env} ->
            Ctx = #route_ctx{tenant_id = broker_security:default_tenant(),
                             publisher = <<"acp">>, reliability = os_buffer},
            {ok, [{Partition, _}|_]} = broker_router_hash:route(Dest, Ctx, Env),
            case broker_queue:read_from(Partition, 0, 1000) of
                {ok, Messages, _Next} ->
                    [M || {_Offset, #broker_message{destination = D} = M}
                              <- Messages, D =:= Dest];
                _ -> []
            end;
        {error, _} ->
            []
    end.

flush() ->
    receive
        {acp_sent, _, _} -> flush();
        {'EXIT', _, _} -> flush()
    after 0 ->
        ok
    end.
