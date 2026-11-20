%% @doc S11-C capacity bench: drive the real CoAP-over-DTLS listener with a real
%% PSK client and record the numbers the design §9 asks for.
%%
%% Why an Erlang client and not Python: Python 3.12's `ssl' has no way to present
%% a PSK identity without a certificate (`ssl.SSLContext.set_psk_client_callback'
%% exists, but the DTLS cert-free PSK suites OTP negotiates are not offered by
%% OpenSSL's default DTLS cipher list in a way `ssl' will accept without a
%% cert). The suite's own client helper IS the contract device vendors must
%% reproduce, so measuring with it measures the real path.
%%
%% This is NOT part of `rebar3 ct': it boots a broker node in-process, runs the
%% matrix, writes raw JSON to `results/', and stops. Nothing binds a fixed port;
%% every listener uses `port => 0'.
%%
%% The KDF here is repeated deliberately rather than imported: a device author
%% reading this file must be able to reimplement it from scratch. It is pinned
%% in `broker_security_SUITE' and `coap_dtls_SUITE' as well.
-module(dtls_bench).

-export([main/0, run/1]).

-define(HOST, {127, 0, 0, 1}).
-define(PSK_LABEL, <<"unifiedmq-coaps-psk">>).
-define(REALM, <<"unifiedmq">>).
-define(CTX, <<"bench-ctx">>).

%%%_* Entry ---------------------------------------------------------------

main() ->
    Opts = #{handshakes => i("BENCH_HANDSHAKES", 200),
             rtt_samples => i("BENCH_RTT", 500),
             concurrency_levels => levels(os:getenv("BENCH_CONC", "1,8,32")),
             requests_per_worker => i("BENCH_REQ_PER_WORKER", 100),
             payload_bytes => i("BENCH_PAYLOAD", 64),
             out_dir => out_dir()},
    run(Opts),
    init:stop().

%% The bench is launched with the repo as CWD, so a bare `results' would land at
%% the repo root. Anchor it to this module's own directory instead.
out_dir() ->
    case os:getenv("BENCH_RESULTS") of
        false -> filename:join(beam_dir(), "results");
        "" -> filename:join(beam_dir(), "results");
        P -> P
    end.

beam_dir() ->
    case code:which(?MODULE) of
        Path when is_list(Path) -> filename:dirname(Path);
        _ -> "."
    end.

run(Opts) ->
    OutDir = maps:get(out_dir, Opts),
    ok = filelib:ensure_dir(filename:join(OutDir, "x")),
    Broker = boot_broker(),
    try
        Port = maps:get(port, Broker),
        io:format("[bench] listener up on 127.0.0.1:~p~n", [Port]),
        Matched = #{
          "generated_at" => iso8601(),
          "otp_release" => list_to_binary(erlang:system_info(otp_release)),
          "transport" => "dtls",
          "suite_set" => suite_names(),
          "handshake" => handshake_bench(Port, maps:get(handshakes, Opts)),
          "rtt" => rtt_bench(Port, maps:get(rtt_samples, Opts),
                             maps:get(payload_bytes, Opts)),
          "throughput" => throughput_bench(Port, Opts),
          "connect_storm" => connect_storm(Port, storm_size(Opts)),
          "plain_udp_control" => plain_udp_bench(Broker, Opts)
        },
        File = filename:join(OutDir, "dtls_capacity.json"),
        ok = file:write_file(File, json(Matched)),
        io:format("[bench] wrote ~s~n", [File]),
        print_summary(Matched)
    after
        stop_broker(Broker)
    end.

%%%_* Broker boot ---------------------------------------------------------

%% Boot the real applications (not a stub) so the numbers include the actual
%% dispatch path: broker_queue/retain/router, not just the listener.
boot_broker() ->
    os:putenv("BENCH_RUN", integer_to_list(erlang:unique_integer([positive]))),
    DataDir = filename:join([tmp_root(), "data-" ++ atom_to_list(node())]),
    _ = file:del_dir_r(DataDir),
    ok = filelib:ensure_dir(filename:join([DataDir, "x"])),
    lists:foreach(fun(App) -> _ = application:load(App) end,
                  [broker_storage, broker_router, broker_queue,
                   broker_subscription, broker_retain, broker_event,
                   broker_security, broker_proto_coap]),
    {ok, _} = application:ensure_all_started(ssl),
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok = application:set_env(broker_storage, data_dir, DataDir),
    ok = application:set_env(broker_storage, wal, #{require_durable_fs => false}),
    ok = application:set_env(broker_queue, partitions, 2),
    ok = application:set_env(broker_router, partitions, 2),
    ok = application:set_env(broker_subscription, data_dir, DataDir),
    ok = application:set_env(broker_retain, data_dir, DataDir),
    ok = application:set_env(broker_security, acl, allow),
    ok = application:set_env(broker_security, require_auth, []),
    ok = application:set_env(broker_security, coaps, #{realm => ?REALM}),
    %% One bench identity. Distinct identities for the concurrency workers are
    %% unnecessary: the caps are raised far above the matrix so admission never
    %% becomes the thing measured.
    Users = [{<<"bench-", (integer_to_binary(I))/binary>>,
              #{password => <<"bench-pw">>, tenant_id => <<"bench">>}}
             || I <- lists:seq(1, 64)],
    ok = application:set_env(broker_security, users, Users),
    ok = application:set_env(broker_proto_coap, enabled, true),
    ok = application:set_env(broker_proto_coap, listeners,
                             [#{port => 0, transport => dtls}]),
    ok = application:set_env(broker_proto_coap, resources, []),
    ok = application:set_env(broker_proto_coap, max_dtls_sessions, 4096),
    ok = application:set_env(broker_proto_coap, dtls_max_sessions_per_principal,
                            4096),
    %% The listener's own token bucket defaults to 100 req/s per endpoint and
    %% DENIES the surplus silently (a flood guard from the S11-B stage). Left at
    %% the default, the bench measures that guard -- failures start at exactly
    %% request 100 and repeat -- not the transport. This report is about DTLS
    %% capacity, so the limiter is raised out of the way and its existence is
    %% documented rather than re-measured here.
    ok = application:set_env(broker_proto_coap, max_requests_per_second,
                             max_request_rate()),
    {ok, _} = application:ensure_all_started(broker_storage),
    ok = create_partitions(),
    {ok, _} = application:ensure_all_started(broker_proto_coap),
    Port = listener_port(),
    #{port => Port, data_dir => DataDir}.

stop_broker(_Broker) ->
    _ = application:stop(broker_proto_coap),
    _ = application:stop(broker_retain),
    _ = application:stop(broker_security),
    _ = application:stop(broker_event),
    _ = application:stop(broker_subscription),
    _ = application:stop(broker_queue),
    _ = application:stop(broker_router),
    _ = application:stop(broker_storage),
    ok.

create_partitions() ->
    lists:foreach(fun(P) ->
                          case broker_queue:create_partition(P) of
                              ok -> ok;
                              {error, {already_exists, P}} -> ok
                          end
                  end, broker_queue:partition_ids()).

listener_port() ->
    [Pid | _] = [P || {Id, P, worker, _} <- supervisor:which_children(
                                              broker_proto_coap_sup),
                      element(1, Id) =:= coap_listener],
    {ok, Port} = coap_listener:port(Pid),
    Port.

%%%_* Benches -------------------------------------------------------------

%% Sequential handshakes: how fast can the broker admit new PSK peers? This is
%% the serialized acceptor's throughput, which is the design's one deliberate
%% bottleneck (§3.2 constraint 1). Each connection is closed immediately: an
%% unfreed session would fill the cap and turn later handshakes into refusals.
handshake_bench(Port, N) ->
    Times = [element(1, timer:tc(fun() -> handshake_once(Port, I) end))
             || I <- lists:seq(1, N)],
    #{"count" => N,
      "mean_us" => mean(Times),
      "p50_us" => percentile(Times, 50),
      "p95_us" => percentile(Times, 95),
      "max_us" => lists:max(Times),
      "per_sec" => rate(N, sum(Times))}.

handshake_once(Port, I) ->
    case connect(identity(I), I, Port) of
        refused -> refused;
        S -> ssl:close(S)
    end.

%% GET round-trip: the steady-state cost of one request/response on an open
%% session, measured over one long-lived connection so handshake cost is excluded.
rtt_bench(Port, Samples, PayloadBytes) ->
    S = connect_or_die(identity(2), 2, Port),
    Payload = binary:copy(<<"x">>, PayloadBytes),
    {ok, _} = coap_put(S, 1, <<"rtt">>, Payload),
    Times = [element(1, timer:tc(fun() -> coap_get(S, Id, <<"rtt">>) end))
             || Id <- lists:seq(2, Samples + 1)],
    _ = ssl:close(S),
    #{"samples" => Samples,
      "payload_bytes" => PayloadBytes,
      "p50_us" => percentile(Times, 50),
      "p95_us" => percentile(Times, 95),
      "max_us" => lists:max(Times),
      "mean_us" => mean(Times)}.

%% Concurrent throughput: N workers each run `Requests' GETs on their own session.
%%
%% The sessions are established SEQUENTIALLY and only THEN do the workers start
%% firing. That split is deliberate: the acceptor handshakes one peer at a time
%% (design §3.2 constraint 1), so a simultaneous connect storm makes peers wait
%% through a retransmit backoff (~1 s, 3 s, 5 s measured) -- that is connect
%% latency, not request throughput, and mixing the two would attribute the
%% acceptor's serialization to the request path. The storm itself is measured by
%% `connect_storm/2' below, where it belongs.
throughput_bench(Port, Opts) ->
    Levels = maps:get(concurrency_levels, Opts),
    Reqs = maps:get(requests_per_worker, Opts),
    Payload = binary:copy(<<"y">>, maps:get(payload_bytes, Opts)),
    maps:from_list(
      [{integer_to_list(N), concurrent_level(Port, N, Reqs, Payload)}
       || N <- Levels]).

concurrent_level(Port, Workers, Reqs, Payload) ->
    %% Seed the destination once so every GET has something to read.
    Seed = connect_or_die(identity(3), 3, Port),
    {ok, _} = coap_put(Seed, 1, <<"tp">>, Payload),
    _ = ssl:close(Seed),
    %% Establish every session BEFORE timing: connect cost is not request cost.
    %% The warmup Message ID (1000+I) and the worker's loop IDs (I*10000+1...)
    %% live in disjoint ranges so no GET can be mistaken for the warmup's
    %% duplicate and answered from the dedup cache.
    Sockets = [connect_or_die(identity(I), 1000 + I, Port)
               || I <- lists:seq(1, Workers)],
    Parent = self(),
    T0 = erlang:monotonic_time(microsecond),
    Pids = [spawn_link(fun() ->
                               R = worker_loop(S, (I * 10000) + 1, Reqs, 0, 0),
                               Parent ! {done, self(), R}
                       end) || {I, S} <- lists:zip(lists:seq(1, Workers), Sockets)],
    Results = [receive {done, P, R} -> R end || P <- Pids],
    Elapsed = erlang:monotonic_time(microsecond) - T0,
    lists:foreach(fun(S) -> _ = try ssl:close(S) catch _:_ -> ok end end, Sockets),
    Ok = lists:sum([O || {O, _E} <- Results]),
    Err = lists:sum([E || {_O, E} <- Results]),
    #{"workers" => Workers,
      "requests" => Workers * Reqs,
      "ok" => Ok,
      "errors" => Err,
      "wall_us" => Elapsed,
      "req_per_sec" => rate(Ok, Elapsed)}.

%% The other half of the concurrency story, measured honestly: how long do N
%% peers take to CONNECT when they all start at once? The acceptor serializes
%% handshakes, so the tail is dominated by DTLS client retransmit backoff, and
%% that is exactly what an operator sizing a fleet of devices needs to know.
%% The handshake and the post-handshake warmup are timed separately, because
%% only the former is the acceptor's; a warmup failure means admission was
%% refused, a handshake timeout means the peer never got a socket.
connect_storm(Port, Workers) ->
    Parent = self(),
    T0 = erlang:monotonic_time(microsecond),
    %% Not spawn_link: a DTLS client whose state machine dies during teardown
    %% (measured: `dtls_client_connection:init' crashing out of `ssl:close/1'
    %% with `{bad_action_from_state_function, ...}') would otherwise take the
    %% whole bench down just for reporting an honest failure. Each peer is
    %% isolated and reports what it saw.
    Workers1 = [{I, spawn_monitor(fun() ->
                                         Start = erlang:monotonic_time(microsecond),
                                         R = storm_connect(identity(I), 7000 + I, Port),
                                         Ms = (erlang:monotonic_time(microsecond) - Start)
                                             div 1000,
                                         Parent ! {storm, self(), {R, Ms}}
                                 end)}
                || I <- lists:seq(1, Workers)],
    Results = [receive
                   {storm, P, R} ->
                       receive {'DOWN', Ref, process, P, _} -> ok
                       after 0 -> ok
                       end,
                       R;
                   {'DOWN', Ref, process, P, Reason} -> {client_crashed, Reason}
               after 60000 ->
                       {client_stalled, 60000}
               end || {_I, {P, Ref}} <- Workers1],
    Elapsed = erlang:monotonic_time(microsecond) - T0,
    Latencies = [Ms || {_Status, Ms} <- Results, is_integer(Ms)],
    Ok = length([ok || {ok, _} <- Results]),
    ByStatus = lists:foldl(fun({St, _}, Acc) -> maps:update_with(St, fun(V) -> V + 1 end, 1, Acc) end,
                           #{}, Results),
    #{"workers" => Workers,
      "connected" => Ok,
      "by_status" => ByStatus,
      "wall_us" => Elapsed,
      "p50_ms" => percentile(Latencies, 50),
      "p95_ms" => percentile(Latencies, 95),
      "max_ms" => max_or_zero(Latencies),
      %% The full distribution, because the interesting fact is the shape:
      %% simultaneous peers land on the client's retransmit backoff steps
      %% (1 s / 3 s / 7 s / 15 s), not on a smooth curve.
      "latencies_ms" => lists:sort(Latencies),
      %% The control that isolates the cause: the SAME peers, started 150 ms
      %% apart. If those all arrive in the tens of milliseconds, the storm's tail
      %% is contention for the accept queue, not the acceptor being slow.
      "staggered" => staggered_storm(Port, Workers, 150)}.

staggered_storm(Port, Workers, GapMs) ->
    Parent = self(),
    T0 = erlang:monotonic_time(microsecond),
    Ps = [begin
              {P, Ref} = spawn_monitor(fun() ->
                                               Start = erlang:monotonic_time(microsecond),
                                               R = storm_connect(identity(I), 7000 + I, Port),
                                               Ms = (erlang:monotonic_time(microsecond) - Start)
                                                   div 1000,
                                               Parent ! {storm, self(), {R, Ms}}
                                       end),
              timer:sleep(GapMs),
              {P, Ref}
          end || I <- lists:seq(1, Workers)],
    Results = [receive
                   {storm, P, R} ->
                       receive {'DOWN', Ref, process, P, _} -> ok
                       after 0 -> ok
                       end,
                       R;
                   {'DOWN', Ref, process, P, Reason} -> {client_crashed, Reason}
               after 60000 ->
                       {client_stalled, 60000}
               end || {P, Ref} <- Ps],
    Latencies = [Ms || {_Status, Ms} <- Results, is_integer(Ms)],
    #{"gap_ms" => GapMs,
      "connected" => length([ok || {ok, _} <- Results]),
      "p50_ms" => percentile(Latencies, 50),
      "p95_ms" => percentile(Latencies, 95),
      "max_ms" => max_or_zero(Latencies),
      "wall_us" => erlang:monotonic_time(microsecond) - T0}.

max_or_zero([]) -> 0;
max_or_zero(Xs) -> lists:max(Xs).

storm_connect(Identity, Id, Port) ->
    Psk = psk(Identity, <<"bench-pw">>),
    Opts = [{protocol, dtls},
            {psk_identity, binary_to_list(Identity)},
            {ciphers, suites()},
            {user_lookup_fun, {fun(_, _, _) -> {ok, Psk} end, undefined}},
            {verify, verify_none},
            {reuseaddr, true},
            {mode, binary},
            {active, false}],
    case try_connect(?HOST, Port, Opts, 20000) of
        {ok, S} ->
            Status = case try_coap_put(S, Id, <<"warm">>, <<>>) of
                         {ok, _} -> ok;
                         _ -> admit_refused
                     end,
            %% Torn-down DTLS clients can crash out of ssl:close/1 (measured
            %% `{bad_action_from_state_function, ...}' from
            %% dtls_client_connection:init). The status is already known; the
            %% teardown must not change the verdict nor kill the bench.
            _ = try ssl:close(S) catch _:_ -> ok end,
            Status;
        {error, timeout} -> handshake_timeout;
        {error, _} -> handshake_error;
        {'EXIT', Reason} -> {client_error, Reason}
    end.

%% The storm size is the largest concurrency level, so the report describes the
%% same fleet the throughput matrix does.
storm_size(Opts) ->
    case maps:get(concurrency_levels, Opts) of
        [] -> 1;
        Levels -> lists:max(Levels)
    end.

%% Rotate through the 64 bench identities so no single one hits the per-principal
%% cap; `I' also seeds distinct Message IDs per worker so a stray cross-talk
%% cannot make two workers look like a duplicate.
identity(I) ->
    N = (I rem 64) + 1,
    <<"bench-", (integer_to_binary(N))/binary>>.

worker_loop(_S, _Id, 0, Ok, Err) ->
    {Ok, Err};
worker_loop(S, Id, N, Ok, Err) ->
    case try_coap_get(S, Id, <<"tp">>) of
        {ok, _} -> worker_loop(S, Id + 1, N - 1, Ok + 1, Err);
        _ -> worker_loop(S, Id + 1, N - 1, Ok, Err + 1)
    end.

%% The control: the same GET matrix over plain UDP, so the report can say what
%% DTLS's crypto + per-session bookkeeping actually costs on this machine.
plain_udp_bench(Broker, Opts) ->
    _Port = maps:get(port, Broker),
    %% A second listener on plain UDP, ephemeral port.
    {ok, UdpPid} = coap_listener:start_link(listen_spec()),
    {ok, UdpPort} = coap_listener:port(UdpPid),
    Payload = binary:copy(<<"z">>, maps:get(payload_bytes, Opts)),
    Reqs = maps:get(requests_per_worker, Opts),
    {ok, S} = gen_udp:open(0, [binary, {active, false}]),
    PutReq = (coap_codec:request(con, put, 1, [{uri_path, <<"uc">>}]))
        #{payload => Payload},
    ok = gen_udp:send(S, ?HOST, UdpPort, coap_codec:encode(PutReq)),
    _ = udp_recv(S),
    Times = [element(1, timer:tc(fun() ->
                                       GetReq = coap_codec:request(
                                                  con, get, Id,
                                                  [{uri_path, <<"uc">>}]),
                                       ok = gen_udp:send(S, ?HOST, UdpPort,
                                                         coap_codec:encode(GetReq)),
                                       udp_recv(S)
                               end)) || Id <- lists:seq(2, Reqs + 1)],
    gen_udp:close(S),
    stop_listener(UdpPid),
    #{"port" => UdpPort,
      "requests" => Reqs,
      "p50_us" => percentile(Times, 50),
      "p95_us" => percentile(Times, 95),
      "max_us" => lists:max(Times)}.

listen_spec() -> #{port => 0}.

stop_listener(Pid) ->
    _ = coap_listener:stop(Pid),
    ok.

%%%_* DTLS client ---------------------------------------------------------

connect(Identity, Id, Port) ->
    Psk = psk(Identity, <<"bench-pw">>),
    Opts = [{protocol, dtls},
            {psk_identity, binary_to_list(Identity)},
            {ciphers, suites()},
            {user_lookup_fun, {fun(_, _, _) -> {ok, Psk} end, undefined}},
            {verify, verify_none},
            {reuseaddr, true},
            {mode, binary},
            {active, false}],
    case ssl:connect(?HOST, Port, Opts, 5000) of
        {ok, S} ->
            %% Force one request/response here: the acceptor's admission reply is
            %% asynchronous, so a peer the listener refused would otherwise be
            %% counted as a successful handshake until the first write failed.
            case coap_put(S, Id, <<"warmup">>, <<>>) of
                {ok, _} -> S;
                _ -> _ = ssl:close(S), refused
            end;
        _ -> refused
    end.

%% Benches that need a live session use this so a refused admission is a loud
%% error (the cap settings must make refusal impossible) rather than a quiet
%% zero-row result.
connect_or_die(Identity, Id, Port) ->
    case connect(Identity, Id, Port) of
        refused -> exit({bench_connect_refused, Identity});
        S -> S
    end.

psk(Identity, Password) ->
    crypto:hash(sha256, [?PSK_LABEL, 0, Identity, 0, ?REALM, 0, Password]).

suites() ->
    [S || S <- ssl:cipher_suites(anonymous, 'dtlsv1.2'),
          lists:member(maps:get(key_exchange, S, none), [psk, dhe_psk, ecdhe_psk]),
          lists:member(maps:get(cipher, S, none),
                       [aes_128_gcm, aes_256_gcm, aes_128_ccm, aes_256_ccm,
                        aes_128_ccm_8, aes_256_ccm_8])].

suite_names() ->
    [unicode:characters_to_binary(
       io_lib:format("~s/~s", [maps:get(cipher, S), maps:get(key_exchange, S)]))
     || S <- suites()].

coap_put(S, Id, Path, Payload) ->
    Req = (coap_codec:request(con, put, Id, [{uri_path, Path}]))#{payload => Payload},
    ok = ssl:send(S, coap_codec:encode(Req)),
    expect(S, ack, {response, 2, 4}).

coap_get(S, Id, Path) ->
    ok = ssl:send(S, coap_codec:encode(
                        coap_codec:request(con, get, Id, [{uri_path, Path}]))),
    expect(S, ack, {response, 2, 5}).

%% The bench treats a transport-level failure as a data point, not a crash, so
%% each call is wrapped. `try' rather than the deprecated `catch Expr' (OTP 29
%% warns; a torn-down DTLS socket can raise out of ssl:send/2 and ssl:recv/3).
try_connect(Host, Port, Opts, Timeout) ->
    try ssl:connect(Host, Port, Opts, Timeout) catch C:R -> {C, R} end.

try_coap_put(S, Id, Path, Payload) ->
    try coap_put(S, Id, Path, Payload) catch C:R -> {C, R} end.

try_coap_get(S, Id, Path) ->
    try coap_get(S, Id, Path) catch C:R -> {C, R} end.

expect(S, Type, Code) ->
    case ssl:recv(S, 0, 5000) of
        {ok, Bin} ->
            case coap_codec:decode(Bin) of
                {ok, #{type := Type, code := Code}} -> {ok, ok};
                {ok, #{type := Type}} -> {error, wrong_code};
                {ok, _} -> {error, wrong_type};
                _ -> {error, undecodable}
            end;
        Other -> {error, Other}
    end.

udp_recv(S) ->
    case gen_udp:recv(S, 0, 5000) of
        {ok, {_Ip, _Port, Bin}} -> coap_codec:decode(Bin);
        Other -> Other
    end.

%%%_* Stats ---------------------------------------------------------------

mean([]) -> 0;
mean(Xs) -> sum(Xs) div length(Xs).

sum(Xs) -> lists:sum(Xs).

rate(_N, 0) -> 0;
rate(N, Us) -> round(N * 1000000 / Us).

percentile(Xs, P) ->
    Sorted = lists:sort(Xs),
    N = length(Sorted),
    case N of
        0 -> 0;
        _ ->
            Rank = max(1, round(N * P / 100)),
            lists:nth(Rank, Sorted)
    end.

%%%_* JSON ----------------------------------------------------------------

%% Minimal emitter: the bench writes only maps, lists, numbers, binaries and
%% atoms, so a full JSON library would be unnecessary weight for one file.
json(M) when is_map(M) ->
    Pairs = [["  ", quote(K), ": ", json(V)]
             || {K, V} <- lists:sort(maps:to_list(M))],
    ["{\n", join(Pairs, ",\n"), "\n}\n"];
%% Every string producer in this module emits a binary (see `suite_names/0',
%% `iso8601/0'), so a list here is always an array -- no charlist guessing.
json(L) when is_list(L) ->
    ["[", join([json(E) || E <- L], ", "), "]"];
json(B) when is_binary(B) -> quote(B);
json(N) when is_integer(N) -> integer_to_list(N);
json(F) when is_float(F) -> float_to_list(F, [{decimals, 3}]);
json(A) when is_atom(A) -> quote(atom_to_binary(A, utf8)).

quote(S) ->
    Bin = to_binary(S),
    Esc = << <<(escape(C))/binary>> || <<C>> <= Bin >>,
    [$", Esc, $"].

to_binary(B) when is_binary(B) -> B;
to_binary(L) when is_list(L) -> list_to_binary(L);
to_binary(A) when is_atom(A) -> atom_to_binary(A, utf8).

escape($") -> <<"\\\"">>;
escape($\\) -> <<"\\\\">>;
escape($\n) -> <<"\\n">>;
escape($\r) -> <<"\\r">>;
escape($\t) -> <<"\\t">>;
escape(C) -> <<C>>.

join([], _Sep) -> "";
join([X], _Sep) -> X;
join([X | Xs], Sep) -> [X, Sep, join(Xs, Sep)].

iso8601() ->
    {{Y, Mo, D}, {H, Mi, Sc}} = calendar:universal_time(),
    unicode:characters_to_binary(
      io_lib:format("~4..0w-~2..0w-~2..0wT~2..0w:~2..0w:~2..0wZ",
                    [Y, Mo, D, H, Mi, Sc])).

print_summary(M) ->
    H = maps:get("handshake", M),
    R = maps:get("rtt", M),
    io:format("[bench] handshake per_sec=~p p50_us=~p p95_us=~p~n",
              [maps:get("per_sec", H), maps:get("p50_us", H),
               maps:get("p95_us", H)]),
    io:format("[bench] rtt p50_us=~p p95_us=~p~n",
              [maps:get("p50_us", R), maps:get("p95_us", R)]),
    [io:format("[bench] conc=~s req_per_sec=~p errors=~p~n",
               [K, maps:get("req_per_sec", V), maps:get("errors", V)])
     || {K, V} <- maps:to_list(maps:get("throughput", M))],
    S = maps:get("connect_storm", M),
    io:format("[bench] connect storm n=~p connected=~p p50_ms=~p p95_ms=~p max_ms=~p~n",
              [maps:get("workers", S), maps:get("connected", S),
               maps:get("p50_ms", S), maps:get("p95_ms", S),
               maps:get("max_ms", S)]),
    G = maps:get("staggered", S),
    io:format("[bench] storm control (staggered ~pms) connected=~p p50_ms=~p p95_ms=~p~n",
              [maps:get("gap_ms", G), maps:get("connected", G),
               maps:get("p50_ms", G), maps:get("p95_ms", G)]).

%%%_* Misc ----------------------------------------------------------------

i(Key, Default) ->
    case os:getenv(Key) of
        false -> Default;
        "" -> Default;
        S -> list_to_integer(S)
    end.

%% High enough that nothing in the matrix is throttled, but not infinite: an
%% unbounded value would let a real limit hide as merely "fast".
max_request_rate() -> 1000000.

levels(false) -> [1, 8, 32];
levels("") -> [1, 8, 32];
levels(S) -> [list_to_integer(X) || X <- string:tokens(S, ",")].

tmp_root() ->
    case os:getenv("BENCH_DATA") of
        false -> "/tmp/opencode/s11c-bench";
        P -> P
    end.
