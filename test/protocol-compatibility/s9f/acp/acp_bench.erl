%% S9-F capacity: ACP turn latency/throughput through the PRODUCTION `acp_port'
%% transport, driving the official ACP Python SDK agent (agent-client-protocol
%% 1.0.0). One sample = one `session/prompt' whose turn projection
%% (`$acp/session/<id>', kind=turn) reaches the kernel -- i.e. the full
%% broker-as-client path: stdio write -> SDK agent -> session/update stream ->
%% turn projection -> queue append.
-module(acp_bench).

-export([run/1, run/2]).

-include_lib("broker_core/include/broker_message.hrl").
-include_lib("broker_router/include/broker_router.hrl").

-define(PARTITIONS, 4).
-define(SCRIPT, "/tmp/opencode/s9f-evidence/acp/acp_sdk_agent.py").
%% Bench data dir; override with S9F_ACP_BENCH_DATA.
-define(DATA, (os:getenv("S9F_ACP_BENCH_DATA",
                  "/tmp/opencode/s9f-evidence/data-acp-bench"))).

%% run(SequentialN) / run(SequentialN, Concurrency)
run(SeqN) -> run(SeqN, 4).

run(SeqN, Conc) ->
    boot(),
    Names = start_agents(Conc),
    %% warm-up: one turn per agent.
    [begin
         _ = wait_ready(Pid, 600),
         Sid = session_of(Pid),
         ok = subscribe(Sid),
         _ = time_turn(Pid, Sid)
     end || {_Name, Pid} <- Names],
    io:format("~n== ACP warm-up done (~p agents) ==~n", [length(Names)]),

    %% Sequential: one agent, SeqN turns.
    {_Name, P0} = hd(Names),
    S0 = session_of(P0),
    TS0 = erlang:monotonic_time(microsecond),
    Seq = [element(1, time_turn(P0, S0)) || _ <- lists:seq(1, SeqN)],
    SeqWall = (erlang:monotonic_time(microsecond) - TS0) / 1000000,
    print("sequential", 1, Seq),
    SeqJson = json_block("sequential", 1, Seq, SeqWall),

    %% Concurrent: Conc agents, SeqN div Conc turns each.
    Per = max(1, SeqN div Conc),
    Parent = self(),
    T0 = erlang:monotonic_time(microsecond),
    Pids = [spawn_link(fun() ->
                              Pid = element(2, lists:nth(I, Names)),
                              Sid = session_of(Pid),
                              %% Each worker subscribes in its own process: the
                              %% wake-up token is delivered to subscribers.
                              ok = subscribe(Sid),
                              L = [element(1, time_turn(Pid, Sid))
                                   || _ <- lists:seq(1, Per)],
                              Parent ! {done, self(), L}
                      end) || I <- lists:seq(1, Conc)],
    Lats = lists:append([receive {done, P, L} -> L end || P <- Pids]),
    Wall = (erlang:monotonic_time(microsecond) - T0) / 1000000,
    ConcJson = json_block("concurrent", Conc, Lats, Wall),
    Doc = iolist_to_binary(
            ["{\n",
             "  \"protocol\": \"ACP\",\n",
             "  \"sdk\": \"agent-client-protocol 1.0.0 (Broker as Client, acp_port stdio)\",\n",
             "  \"operation\": \"session/prompt turn projection (full stdio round trip)\",\n",
             SeqJson, ",\n", ConcJson, "\n}\n"]),
    io:format("~n== prompt retries (races between turn publish and ready) ==~n"),
    lists:foreach(
      fun(D) -> io:format("  ~ts retries=~p max_run=~p~n",
                         [D, get({retries, D}), get({max_retry, D})]) end,
      [S || {_N, P} <- Names, S <- [session_of(P)]]),
    Out = case os:getenv("S9F_ACP_OUT") of
              false -> "/tmp/opencode/s9f-evidence/bench/acp_bench.json";
              P -> P
          end,
    ok = file:write_file(Out, Doc),
    io:format("~nwrote ~ts~n", [Out]),
    ok.

%%%_* One turn ---------------------------------------------------------------

time_turn(Pid, Sid) ->
    Dest = <<"$acp/session/", Sid/binary>>,
    T0 = erlang:monotonic_time(microsecond),
    ok = prompt_ok(Pid, Sid, Dest, 0),
    Msgs = await_turn(Dest, erlang:monotonic_time(millisecond) + 30000),
    T1 = erlang:monotonic_time(microsecond),
    {(T1 - T0) / 1000.0, length(Msgs)}.

%% The turn projection is published a few instructions BEFORE the session state
%% machine returns to `ready' (publish_turn/2 precedes acp_session:end_turn/2 in
%% on_prompt_response/3). Waiting on the message alone can therefore wake us
%% while the agent is still `prompting'; the one-turn-in-flight state is a real
%% protocol property, so the harness retries the prompt instead of failing.
prompt_ok(Pid, Sid, _Dest, N) ->
    case acp_agent:prompt(Pid, Sid,
                          [#{<<"type">> => <<"text">>,
                             <<"text">> => <<"bench turn">>}]) of
        ok -> ok;
        {error, {session_not_ready, prompting}} ->
            case get({retries, _Dest}) of
                undefined -> put({retries, _Dest}, 1);
                K -> put({retries, _Dest}, K + 1)
            end,
            case get({max_retry, _Dest}) of
                undefined -> put({max_retry, _Dest}, N);
                M when N > M -> put({max_retry, _Dest}, N);
                _ -> ok
            end,
            timer:sleep(1),
            prompt_ok(Pid, Sid, _Dest, N + 1);
        {error, Other} ->
            error({prompt_refused, Other})
    end.

await_turn(Dest, Deadline) ->
    case read_dest(Dest) of
        [] ->
            await_turn_wait(Dest, Deadline);
        Msgs ->
            %% The turn projection is the LAST message of a turn (updates are
            %% published first); returning before it lands would let us issue the
            %% next prompt while the session is still `prompting'.
            case has_turn(Msgs) of
                true -> Msgs;
                false -> await_turn_wait(Dest, Deadline)
            end
    end.

await_turn_wait(Dest, Deadline) ->
    Now = erlang:monotonic_time(millisecond),
    case Now >= Deadline of
        true -> error(turn_timeout);
        false ->
            receive
                {new_messages, _P, _F, _T} -> await_turn(Dest, Deadline)
            after min(1000, Deadline - Now) ->
                    await_turn(Dest, Deadline)
            end
    end.

has_turn(Msgs) ->
    lists:any(fun(M) ->
                  case acp_event:decode(M) of
                      {ok, Payload} ->
                          maps:get(<<"kind">>, Payload, undefined) =:= <<"turn">>;
                      {error, _} -> false
                  end
              end, Msgs).

%%%_* Stats ------------------------------------------------------------------

print(Label, Conc, Lats) ->
    S = lists:sort(Lats),
    io:format("~n== ~ts: n=~p conc=~p p50=~.1fms p95=~.1fms p99=~.1fms mean=~.1fms ==~n",
              [Label, length(S), Conc, pct(S, 50), pct(S, 95), pct(S, 99),
               lists:sum(S) / length(S)]).

json_block(Label, Conc, Lats, Wall) ->
    S = lists:sort(Lats),
    WallMs = case Wall of
                 undefined -> undefined;
                 _ -> Wall * 1000
             end,
    Tp = case Wall of
             undefined -> undefined;
             _ -> length(S) / Wall
         end,
    iolist_to_binary(
      ["  \"", Label, "\": {\n",
       "    \"samples\": ", integer_to_list(length(S)), ",\n",
       "    \"concurrency\": ", integer_to_list(Conc), ",\n",
       maybe_wall(WallMs),
       maybe_tp(Tp),
       "    \"p50_ms\": ", f(pct(S, 50)), ",\n",
       "    \"p95_ms\": ", f(pct(S, 95)), ",\n",
       "    \"p99_ms\": ", f(pct(S, 99)), ",\n",
       "    \"mean_ms\": ", f(lists:sum(S) / length(S)), "\n",
       "  }"]).

maybe_wall(undefined) -> "";
maybe_wall(Ms) -> ["    \"wall_ms\": ", f(Ms), ",\n"].

maybe_tp(undefined) -> "";
maybe_tp(Tp) -> ["    \"throughput_per_s\": ", f(Tp), ",\n"].

f(X) -> float_to_list(X * 1.0, [{decimals, 3}, compact]).

pct(S, P) ->
    N = length(S),
    K = max(1, min(N, round(P / 100.0 * N))),
    lists:nth(K, S).

%%%_* Kernel reads -----------------------------------------------------------

read_dest(Dest) ->
    From = case get({cursor, Dest}) of
               undefined -> 0;
               N -> N
           end,
    {ok, Env} = broker_router:env(),
    Ctx = #route_ctx{tenant_id = broker_security:default_tenant(),
                     publisher = <<"acp">>, reliability = os_buffer},
    {ok, [{Partition, _} | _]} = broker_router_hash:route(Dest, Ctx, Env),
    {ok, Messages, Next} = broker_queue:read_from(Partition, From, 100000),
    put({cursor, Dest}, Next),
    [M || {_Offset, #broker_message{destination = D} = M} <- Messages,
          D =:= Dest].

subscribe(Sid) ->
    Dest = <<"$acp/session/", Sid/binary>>,
    {ok, Env} = broker_router:env(),
    Ctx = #route_ctx{tenant_id = broker_security:default_tenant(),
                     publisher = <<"acp">>, reliability = os_buffer},
    {ok, [{Partition, _} | _]} = broker_router_hash:route(Dest, Ctx, Env),
    ok = broker_queue:subscribe(Partition, self()),
    ok.

%%%_* Boot -------------------------------------------------------------------

boot() ->
    ok = filelib:ensure_dir(filename:join(?DATA, "placeholder")),
    [application:load(A) || A <- [broker_storage, broker_router, broker_queue,
                                  broker_meta, broker_security,
                                  broker_proto_agent_rpc, broker_proto_acp]],
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok = application:set_env(broker_storage, data_dir, ?DATA),
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

start_agents(N) ->
    Agents = [#{name => list_to_binary("bench-" ++ integer_to_list(I)),
                command => <<"/usr/bin/python3">>,
                args => [list_to_binary(?SCRIPT)],
                env => [{<<"PYTHONPATH">>, <<"/tmp/opencode/s9f-libs">>}],
                transport => acp_port,
                cwd => <<"/tmp/opencode/s9f-evidence/acp">>,
                restart => temporary} || I <- lists:seq(1, N)],
    ok = application:set_env(broker_proto_acp, agents, Agents),
    ok = application:set_env(broker_proto_acp, permission_policy,
                             #{default => deny, allow => [], timeout_ms => 30000,
                               notify_dest => <<"$acp/permission">>}),
    {ok, _} = application:ensure_all_started(broker_proto_acp),
    Children = [C || {_Id, P, worker, _} = C <- supervisor:which_children(
                                                  broker_proto_acp_sup),
                     is_pid(P)],
    [{element(1, C), element(2, C)} || C <- Children].

session_of(Pid) ->
    #{session_id := Sid} = acp_agent:state(Pid),
    Sid.

wait_ready(_Pid, 0) -> error(agent_not_ready);
wait_ready(Pid, N) ->
    case acp_agent:state(Pid) of
        #{state := ready, session_id := Sid} when is_binary(Sid) -> ok;
        _ -> timer:sleep(100), wait_ready(Pid, N - 1)
    end.
