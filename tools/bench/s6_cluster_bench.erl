%% @doc S6 capacity support: the cost of `replicated_commit' on a real 3-node
%% group (README §11.4 bullet 5: Raft/replication, fsync, failover and recovery
%% effect on tail latency).
%%
%% Three `peer' nodes each run the storage + queue data plane and one shared
%% Metadata Raft Group (the same shape `broker_cluster_dist_SUITE' proves
%% correct).  Partition 0 is owned by node 1 with all three nodes as replicas,
%% so a `replicated_commit' append is: local fsync, then rpc to the two
%% followers (each fsyncs), then a majority check.  The benchmark measures that
%% serial per-message latency against a plain local `fsync' append on the same
%% node, then repeats with one follower down (majority intact) and with both
%% followers down (no majority, so the append must fail rather than ack).
%%
%%   erlc -pa _build/test/lib/broker_core/ebin -o /tmp/opencode/ebin \
%%        tools/bench/s6_cluster_bench.erl
%%   erl -pa _build/test/lib/*/ebin -pa /tmp/opencode/ebin -noshell \
%%       -eval 's6_cluster_bench:run(200), halt().'
%%
%% `_build/test' is used because `peer' and the CT-built apps are there; the
%% suite module is not needed.  Data directories live under the repo's ext4
%% `data/' tree and are removed on start.
-module(s6_cluster_bench).

-export([run/1]).
-export([prepare_peer/1, start_meta/1, start_data/1,
         install_replicator/0, bench/2]).

-include_lib("broker_core/include/broker_message.hrl").

-define(PARTITION, 0).

run(N) ->
    ensure_distribution(),
    Peers = start_peers(),
    Nodes = [maps:get(node, P) || P <- Peers],
    [N1, N2, N3] = Nodes,
    try
        [ok = rpc(Nd, start_meta, [Nodes]) || Nd <- Nodes],
        [ok = rpc(Nd, start_data, [data_dir(Nd)]) || Nd <- Nodes],
        {ok, 1} = rpc(N1, broker_cluster, assign, [?PARTITION, N1, Nodes, 1]),
        {ok, Nodes} = rpc(N1, broker_cluster, replicas, [?PARTITION]),
        ok = rpc(N1, install_replicator, []),

        Base = rpc(N1, bench, [fsync, N]),
        Rep3 = rpc(N1, bench, [replicated_commit, N]),

        %% One follower down: 2 of 3 still acknowledge, so writes keep working.
        stop_peer(Peers, N3),
        timer:sleep(500),
        Rep2 = rpc(N1, bench, [replicated_commit, N]),

        %% Both followers down: only 1 of 3 remains, so the append must fail.
        stop_peer(Peers, N2),
        timer:sleep(500),
        Rep1 = rpc(N1, bench, [replicated_commit, N]),

        Result = #{nodes => Nodes,
                   local_fsync => Base,
                   replicated_3_of_3 => Rep3,
                   replicated_2_of_3 => Rep2,
                   replicated_1_of_3_no_majority => Rep1},
        io:format("~n=== replicated_commit vs local fsync (serial, 1024 B) ===~n"),
        io:format("~p~n", [Result]),
        Result
    after
        stop_peers(Peers)
    end.

%%%_* Peer bootstrap -------------------------------------------------------

ensure_distribution() ->
    case node() of
        nonode@nohost ->
            {ok, _} = net_kernel:start([s6_cluster_bench, shortnames]),
            ok;
        _ ->
            ok
    end.

start_peers() ->
    SuiteDir = filename:dirname(code:which(?MODULE)),
    %% `ra' is a vendored checkout (`_checkouts/'), not a `lib/' dependency, so
    %% `code:get_path()' does not contain it unless rebar3's own shell added it.
    Checkouts = [filename:absname(D)
                 || D <- filelib:wildcard("_build/test/checkouts/*/ebin")],
    CodePaths = lists:usort(
                  [SuiteDir | Checkouts ++
                   [D || D <- code:get_path(),
                         string:find(D, "_build/test") =/= nomatch]]),
    lists:map(
      fun(I) ->
              Name = list_to_atom("s6bench_" ++ integer_to_list(I)),
              {ok, Pid, Node} =
                  peer:start_link(#{name => Name,
                                    args => ["-pa" | CodePaths],
                                    wait_boot => 20000}),
              ok = rpc(Node, prepare_peer, [[data_dir(Node), []]]),
              #{pid => Pid, node => Node}
      end, [1, 2, 3]).

stop_peers(Peers) ->
    lists:foreach(fun(#{pid := Pid}) -> catch peer:stop(Pid) end, Peers),
    ok.

stop_peer(Peers, Node) ->
    case [P || #{node := N} = P <- Peers, N =:= Node] of
        [#{pid := Pid}] -> catch peer:stop(Pid), ok;
        _ -> ok
    end.

data_dir(Node) ->
    Rel = "s6-cluster-bench/" ++ atom_to_list(Node) ++ "-" ++ os:getpid(),
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
    %% Load first: `application:load/1' installs the `.app' env and would
    %% overwrite an env value set before it (the same trap the CT suite notes).
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
    {ok, _} = application:ensure_all_started(broker_storage),
    ok = broker_queue:create_partition(?PARTITION),
    {ok, _} = application:ensure_all_started(broker_queue),
    ok.

%% Runs on the owner: turn `replicated_commit' from "unsupported" into the real
%% majority path (the hook `broker_queue' documents).
install_replicator() ->
    persistent_term:put({broker_queue, replicator}, fun broker_cluster_rep:replicate/2),
    ok.

%% Runs on the owner.  Serial on purpose: each sample is one message's
%% publisher-visible latency, which is what a synchronous ACK contract promises.
bench(Mode, N) ->
    %% The warm-up is allowed to fail: the last phase deliberately runs with no
    %% majority, where the *expected* answer is an error for every append.
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

msg_s([]) -> 0;
msg_s(Oks) -> round(length(Oks) * 1000000 / lists:sum(Oks)).

sample(Mode, Seq) ->
    T0 = erlang:monotonic_time(microsecond),
    Result = broker_queue:append(?PARTITION, msg(Seq), Mode),
    T1 = erlang:monotonic_time(microsecond),
    case Result of
        {ok, _Offset} -> {ok, T1 - T0};
        {error, Reason} -> {error, Reason}
    end.

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

msg(Seq) ->
    #broker_message{message_id = integer_to_binary(Seq),
                    destination = <<"bench/cluster">>,
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
    case rpc:call(Node, M, F, A, 60000) of
        {badrpc, Reason} -> error({badrpc, Node, M, F, Reason});
        Result -> Result
    end.
