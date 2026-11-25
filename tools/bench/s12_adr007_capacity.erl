%% @doc S12 / ADR-007 three-tier **capacity** support (Gate S12 last open item:
%% "有状态服务的状态持久化与崩溃恢复有测试证明，并绑定 ADR-007 显式可靠性等级").
%%
%% `s6_cluster_bench.erl` already proved the *latency* ordering of the three
%% reliability tiers on a real 3-node group (os_buffer < fsync < replicated_commit)
%% and the no-majority refusal. What Gate S12 still lacked is the **capacity**
%% half: how many messages/second each tier can actually sustain, and how that
%% ceiling binds to the S12 stateful service -- the ra metadata group
%% (`broker_cluster_meta`) that owns every partition.
%%
%% This driver measures, on the same real 3-peer shape (real `ra`, real WAL,
%% real `broker_cluster_meta`, no mock):
%%
%%   1. Serial per-tier latency (os_buffer / fsync / replicated_commit) -- the
%%      ACK-return cost ADR-007 promises at each tier (p50/p99/max).
%%   2. Saturated per-tier throughput -- the capacity ceiling. The WAL group-
%%      commits (batch_size 32), so concurrent callers batch into one fsync and
%%      the ceiling is a property of the tier, not of a single caller.
%%   3. ra metadata-layer commit capacity -- the control-plane ceiling of the
%%      S12 stateful service. ra serialises every write into one log applied in
%%      order, so this is measured serially; concurrent clients can only queue.
%%   4. The durability boundary under partition loss -- with both followers
%%      down (no majority) L3 `replicated_commit` must refuse every append while
%%      L1 `os_buffer` and L2 `fsync` keep succeeding (they never needed the
%%      group). This is ADR-007's "失去多数派时写入必须被拒绝" at the capacity
%%      edge, and it is what binds the three tiers to the S12 stateful service.
%%
%%   erlc -pa _build/test/lib/broker_core/ebin -o /tmp/opencode/ebin \
%%        tools/bench/s12_adr007_capacity.erl
%%   erl -pa _build/test/lib/*/ebin -pa /tmp/opencode/ebin -noshell \
%%       -eval 's12_adr007_capacity:run(2000), halt().'
%%
%% `_build/test' is used because `peer' and the CT-built apps are there; the
%% suite module is not needed. Data directories live under the repo's ext4
%% `data/' tree (NOT /tmp -- /tmp is tmpfs here and `wal_writer' refuses it) and
%% are removed on start.
-module(s12_adr007_capacity).

%% os_buffer | fsync | replicated_commit -- the three ADR-007 tiers.
-export([run/1, run/2, bench/2, saturate/3, meta_commit/2, stop_peer/2]).
%% Peer lifecycle, exported so the dist suites and re-runs can reuse them.
-export([prepare_peer/1, start_meta/1, start_data/1, install_replicator/0]).

-include_lib("broker_core/include/broker_message.hrl").

-define(PARTITION, 0).
-define(DEFAULT_MS, 3000).
%% Saturated-throughput wall clock per tier. Long enough to average past the
%% group-commit batching transients, short enough to keep the run to minutes.
-define(SAT_MS, 3000).

%% tier => display name / ordering.
-define(TIERS, [os_buffer, fsync, replicated_commit]).

%%%_* Entry ----------------------------------------------------------------

%% @doc Full run: `N' serial samples per tier for latency, `?SAT_MS' per tier for
%% the throughput ceiling, then the ra control-plane and no-majority phases.
%% Returns the result map (also printed and, with `--json', written by the shell).
-spec run(pos_integer()) -> map().
run(N) ->
    run(N, ?SAT_MS).

-spec run(pos_integer(), pos_integer()) -> map().
run(N, SatMs) ->
    ensure_distribution(),
    Peers = start_peers(),
    Nodes = [maps:get(node, P) || P <- Peers],
    [N1 | _] = Nodes,
    try
        [ok = rpc(Nd, start_meta, [Nodes]) || Nd <- Nodes],
        [ok = rpc(Nd, start_data, [data_dir(Nd)]) || Nd <- Nodes],
        {ok, 1} = rpc(N1, broker_cluster, assign, [?PARTITION, N1, Nodes, 1]),
        {ok, Nodes} = rpc(N1, broker_cluster, replicas, [?PARTITION]),
        ok = rpc(N1, install_replicator, []),

        io:format("~n=== S12 / ADR-007 three-tier capacity (3 peer, 1024 B msgs) ===~n"),

        %% 1. Serial per-tier latency (the ACK-return cost per tier).
        Latency = [{Tier, rpc(N1, bench, [Tier, N])} || Tier <- ?TIERS],
        io:format("~n-- serial latency (N=~p) --~n", [N]),
        [io:format("  ~-18s ~s~n", [Tier, fmt_lat(R)])
         || {Tier, R} <- Latency],

        %% 2. Saturated per-tier throughput (the capacity ceiling).
        Saturation = [{Tier, rpc(N1, saturate, [Tier, 8, SatMs])} || Tier <- ?TIERS],
        io:format("~n-- saturated throughput (~p ms, 8 concurrent) --~n", [SatMs]),
        [io:format("  ~-18s ~s~n", [Tier, fmt_sat(R)])
         || {Tier, R} <- Saturation],

        %% 3. ra metadata-layer (S12 stateful service) control-plane commit ceiling.
        Meta = rpc(N1, meta_commit, [N, Nodes]),
        io:format("~n-- ra metadata-layer commit (S12 stateful service, N=~p) --~n", [N]),
        io:format("  ~s~n", [fmt_meta(Meta)]),

        %% 4. Durability boundary under partition loss: L3 must refuse, L1/L2 proceed.
        Boundary = no_majority(Peers, N1, Nodes, N),
        io:format("~n-- no-majority boundary (both followers down) --~n"),
        [io:format("  ~-18s ~s~n", [Tier, fmt_lat(R)])
         || {Tier, R} <- Boundary],

        #{tier_latency_serial => Latency,
          tier_throughput_saturated => Saturation,
          ra_metadata_commit => Meta,
          no_majority_boundary => Boundary,
          nodes => Nodes,
          serial_samples => N,
          saturate_ms => SatMs}
    after
        stop_peers(Peers)
    end.

%%%_* Phase 4: no-majority boundary ---------------------------------------

%% Stop both followers, then re-run each tier on the lone owner. The expected
%% shape is the ADR-007 binding: L3 `replicated_commit' cannot reach a majority
%% so every append is refused (`failed == n', `ok == 0'); L1 `os_buffer' and
%% L2 `fsync' never needed the group, so they keep succeeding. This is what
%% ties the three tiers to the S12 stateful service: the group's availability
%% gates only L3.
no_majority(Peers, N1, Nodes, N) ->
    [_, N2, N3] = Nodes,
    %% Phase 3's `meta_commit' leaves the partition at a high epoch and a
    %% single-node replica set (a valid ownership change); restore the 3-node
    %% replica set with a fresh epoch first, or the "no majority" probe below
    %% would see a 1-node group where the lone owner is trivially a majority and
    %% L3 would wrongly succeed. Epoch must be strictly greater than the current
    %% one (the meta machine rejects a stale `E >= Epoch').
    Cur = rpc(N1, broker_cluster, epoch_or_zero, [?PARTITION]),
    RestoreEpoch = Cur + 1,
    {ok, RestoreEpoch} = rpc(N1, broker_cluster, assign, [?PARTITION, N1, Nodes, RestoreEpoch]),
    {ok, Nodes} = rpc(N1, broker_cluster, replicas, [?PARTITION]),
    stop_peer(Peers, N3),
    timer:sleep(500),
    stop_peer(Peers, N2),
    timer:sleep(500),
    [{Tier, rpc(N1, bench, [Tier, N])} || Tier <- ?TIERS].

%%%_* Peer bootstrap (mirrors s6_cluster_bench; ra is a `_checkouts/' dep) -----

ensure_distribution() ->
    case node() of
        nonode@nohost ->
            {ok, _} = net_kernel:start([s12_adr007_capacity, shortnames]),
            ok;
        _ ->
            ok
    end.

start_peers() ->
    SuiteDir = filename:dirname(code:which(?MODULE)),
    Checkouts = [filename:absname(D)
                 || D <- filelib:wildcard("_build/test/checkouts/*/ebin")],
    CodePaths = lists:usort(
                  [SuiteDir | Checkouts ++
                   [D || D <- code:get_path(),
                         string:find(D, "_build/test") =/= nomatch]]),
    lists:map(
      fun(I) ->
              Name = list_to_atom("s12cap_" ++ integer_to_list(I)),
              {ok, Pid, Node} =
                  peer:start_link(#{name => Name,
                                    args => ["-pa" | CodePaths],
                                    wait_boot => 20000}),
              ok = rpc(Node, prepare_peer, [[data_dir(Node), []]]),
              #{pid => Pid, node => Node}
      end, [1, 2, 3]).

stop_peers(Peers) ->
    lists:foreach(fun(#{pid := Pid}) -> stop_one(Pid) end, Peers),
    ok.

stop_peer(Peers, Node) ->
    case [P || #{node := N} = P <- Peers, N =:= Node] of
        [#{pid := Pid}] -> stop_one(Pid);
        _ -> ok
    end.

stop_one(Pid) ->
    try peer:stop(Pid) catch _:_ -> ok end.

data_dir(Node) ->
    Rel = "s12-adr007-capacity/" ++ atom_to_list(Node) ++ "-" ++ os:getpid(),
    filename:join(filename:absname("data"), Rel).

%% Runs on a peer.
prepare_peer([DataDir, Members]) ->
    _ = application:load(broker_cluster),
    ok = application:set_env(broker_cluster, data_dir, filename:join(DataDir, "ra")),
    ok = application:set_env(broker_cluster, members, Members),
    ok = application:set_env(broker_cluster, quorum_timeout_ms, 3000),
    ok = application:set_env(broker_cluster, replicate_timeout_ms, 1000),
    _ = os:cmd("rm -rf '" ++ DataDir ++ "'"),
    ok = filelib:ensure_dir(filename:join([DataDir, "ra", "x"])),
    {ok, _} = application:ensure_all_started(ra),
    application:set_env(ra, data_dir, filename:absname(filename:join(DataDir, "ra"))),
    case ra_system:fetch(default) of
        undefined ->
            case ra_system:start_default() of
                {ok, _} -> ok;
                {error, {already_started, _}} -> ok
            end;
        _ -> ok
    end,
    {ok, Cache} = broker_cluster_cache:start_link(),
    unlink(Cache),
    ok.

%% Runs on a peer.
start_meta(Members) ->
    ok = application:set_env(broker_cluster, members, Members),
    {ok, Meta} = broker_cluster_meta:start_link(),
    unlink(Meta),
    ok.

%% Runs on a peer: the real storage + queue data plane with one partition.
start_data(DataDir) ->
    application:load(broker_storage),
    application:load(broker_queue),
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok = application:set_env(broker_storage, data_dir, filename:join(DataDir, "storage")),
    ok = application:set_env(broker_storage, wal,
                             #{batch_size => 32, max_batch_size => 256,
                               batch_delay_ms => #{os_buffer => 0, fsync => 1},
                               index_anchor_interval => 256,
                               segment_max_records => 100000,
                               max_pending_appends => 4096}),
    ok = application:set_env(broker_queue, partitions, 1),
    %% `broker_queue' pulls in `broker_router' (its dep), whose hash store reads
    %% a boot-time `partitions' env with no default and crashes without it; align
    %% it with the single partition under test.
    application:load(broker_router),
    ok = application:set_env(broker_router, partitions, 1),
    {ok, _} = application:ensure_all_started(broker_storage),
    ok = broker_queue:create_partition(?PARTITION),
    {ok, _} = application:ensure_all_started(broker_queue),
    ok.

%% Runs on the owner: turn `replicated_commit' from "unsupported" into the real
%% majority path (the hook `broker_queue' documents).
install_replicator() ->
    persistent_term:put({broker_queue, replicator}, fun broker_cluster_rep:replicate/2),
    ok.

%%%_* Phase 1: serial per-tier latency --------------------------------------

%% Runs on the owner.  Serial on purpose: each sample is one message's
%% publisher-visible latency, which is what a synchronous ACK contract promises.
bench(Mode, N) ->
    Warm = broker_queue:append(?PARTITION, msg(0), Mode),
    Samples = [sample(Mode, I) || I <- lists:seq(1, N)],
    Oks = [Us || {ok, Us} <- Samples],
    Errors = [E || {error, E} <- Samples],
    #{mode => Mode,
      n => N,
      warm => Warm,
      ok => length(Oks),
      failed => length(Errors),
      first_error => case Errors of [E | _] -> E; [] -> none end,
      msg_s => msg_s(Oks),
      latency_ms => quantiles(Oks)}.

sample(Mode, Seq) ->
    T0 = erlang:monotonic_time(microsecond),
    Result = broker_queue:append(?PARTITION, msg(Seq), Mode),
    T1 = erlang:monotonic_time(microsecond),
    case Result of
        {ok, _Offset} -> {ok, T1 - T0};
        {error, Reason} -> {error, Reason}
    end.

%%%_* Phase 2: saturated per-tier throughput --------------------------------

%% Runs on the owner.  Eight concurrent callers each append for `DurationMs';
%% the WAL group-commits (batch_size 32), so their calls coalesce into one
%% fsync and the aggregate `msg/s' is the tier's capacity ceiling, not one
%% caller's. Only `{ok, _}' counts toward throughput -- an `{error,_}' (overload
%% or, for L3 without a majority, refusal) is dropped from the numerator but the
%% attempt is still timed out by the caller's own loop.
saturate(Mode, Workers, DurationMs) ->
    Parent = self(),
    Pids = [spawn(fun() -> Parent ! {sat, self(), sat_worker(Mode, DurationMs, 0)} end)
            || _ <- lists:seq(1, Workers)],
    Counts = [receive {sat, P, C} -> C after DurationMs + 30000 -> 0 end || P <- Pids],
    Total = lists:sum(Counts),
    #{mode => Mode,
      workers => Workers,
      duration_ms => DurationMs,
      messages => Total,
      msg_s => round(Total * 1000 / DurationMs)}.

sat_worker(Mode, DurationMs, Acc) ->
    Deadline = erlang:monotonic_time(millisecond) + DurationMs,
    sat_loop(Mode, Deadline, Acc).

sat_loop(Mode, Deadline, Acc) ->
    case erlang:monotonic_time(millisecond) < Deadline of
        true ->
            case broker_queue:append(?PARTITION, msg(Acc), Mode) of
                {ok, _} -> sat_loop(Mode, Deadline, Acc + 1);
                {error, _} -> sat_loop(Mode, Deadline, Acc)
            end;
        false -> Acc
    end.

%%%_* Phase 3: ra metadata-layer (S12 stateful service) commit capacity ------

%% Runs on the owner.  One ra-committed metadata write at a time: ra serialises
%% every command into a single log applied in append order, so this *is* the
%% control-plane ceiling -- adding concurrent clients only queues behind it.
%% Each `assign' carries a strictly increasing epoch (the meta machine rejects a
%% stale `E >= Epoch') and the live 3-node replica set, so the measurement is a
%% real ownership change on the S12 stateful service, not a degenerate one-node
%% group.
meta_commit(N, Nodes) ->
    Base = rpc(node(), broker_cluster, epoch_or_zero, [?PARTITION]),
    Samples = [meta_sample(Base + I, Nodes) || I <- lists:seq(1, N)],
    Oks = [Us || {ok, Us} <- Samples],
    Errors = [E || {error, E} <- Samples],
    #{n => N,
      ok => length(Oks),
      failed => length(Errors),
      msg_s => msg_s(Oks),
      latency_ms => quantiles(Oks)}.

meta_sample(Epoch, Nodes) ->
    T0 = erlang:monotonic_time(microsecond),
    Result = broker_cluster:assign(?PARTITION, node(), Nodes, Epoch),
    T1 = erlang:monotonic_time(microsecond),
    case Result of
        {ok, _} -> {ok, T1 - T0};
        {error, Reason} -> {error, Reason}
    end.

%%%_* Shared helpers --------------------------------------------------------

msg_s([]) -> 0;
msg_s(Oks) -> round(length(Oks) * 1000000 / lists:sum(Oks)).

quantiles([]) ->
    #{p50 => null, p99 => null, max => null};
quantiles(Us) ->
    Sorted = lists:sort(Us),
    #{p50 => round(pct(Sorted, 50) / 1000),
      p99 => round(pct(Sorted, 99) / 1000),
      max => round(lists:last(Sorted) / 1000)}.

pct(Sorted, P) ->
    N = length(Sorted),
    lists:nth(max(1, (N * P + 99) div 100), Sorted).

fmt_lat(#{ok := OK, failed := F, latency_ms := L}) ->
    io_lib:format("ok=~p failed=~p msg/s=~p p50=~pms p99=~pms max=~pms",
                  [OK, F, maps:get(msg_s, L, 0),
                   maps:get(p50, L, null), maps:get(p99, L, null),
                   maps:get(max, L, null)]).

fmt_sat(#{workers := W, messages := M, msg_s := S}) ->
    io_lib:format("workers=~p messages=~p ceiling=~p msg/s", [W, M, S]).

fmt_meta(#{ok := OK, failed := F, msg_s := S, latency_ms := L}) ->
    io_lib:format("ok=~p failed=~p commits/s=~p p50=~pms p99=~pms max=~pms",
                  [OK, F, S, maps:get(p50, L, null), maps:get(p99, L, null),
                   maps:get(max, L, null)]).

msg(Seq) ->
    #broker_message{message_id = integer_to_binary(Seq),
                    destination = <<"bench/s12-capacity">>,
                    payload = payload(1024, Seq),
                    payload_encoding = raw,
                    headers = #{},
                    proto_meta = #{},
                    persistence = durable,
                    reliability = replicated_commit,
                    created_at = erlang:system_time(millisecond),
                    tenant_id = <<"t0">>}.

payload(Size, Seq) ->
    Header = <<Seq:64>>,
    <<Header/binary, (binary:copy(<<"x">>, Size - byte_size(Header)))/binary>>.

rpc(Node, F, A) ->
    rpc(Node, ?MODULE, F, A).

rpc(Node, M, F, A) ->
    case rpc:call(Node, M, F, A, 120000) of
        {badrpc, Reason} -> error({badrpc, Node, M, F, Reason});
        Result -> Result
    end.
