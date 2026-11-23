%% @doc 会话分区 bootstrap 的真 3 节点 ra CT（Plan 3 gap #3，设计 §3.3）。
%% 断言：启动后 0..C-1 每个分区都有元数据、session_owner 非 not_found、幂等。
-module(broker_cluster_session_bootstrap_dist_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(NODES, 3).
%% 复制的 boot_data_plane/0 引用它（队列/路由分区数）。
-define(PARTITIONS, 3).
%% 显式 C 让断言确定（bootstrap 走 env 覆盖分支）。
-define(C, 8).

all() -> [bootstrap_creates_all_session_partitions,
          bootstrap_is_idempotent].

init_per_suite(Config) ->
    case node() of
        nonode@nohost ->
            case net_kernel:start([unifiedmq_gap3bct, shortnames]) of
                {ok, _} -> [{started_net, true} | Config];
                {error, Reason} ->
                    ct:fail({cannot_start_distribution, check_epmd, Reason})
            end;
        _ ->
            [{started_net, false} | Config]
    end.

end_per_suite(Config) ->
    case ?config(started_net, Config) of
        true -> _ = net_kernel:stop(), ok;
        _ -> ok
    end.

init_per_testcase(Case, Config) ->
    Peers = start_peers(Case, Config),
    Nodes = [maps:get(node, P) || P <- Peers],
    ok = form_cluster_h(Nodes),
    lists:foreach(
      fun(N) ->
              ok = rpc(N, application, set_env, [broker_cluster, members, Nodes]),
              ok = rpc(N, application, set_env, [broker_cluster, session_replication, true]),
              ok = rpc(N, application, set_env, [broker_cluster, session_partition_count, ?C]),
              ok = rpc(N, boot_data_plane, [])
      end, Nodes),
    [{peers, Peers}, {nodes, Nodes} | Config].

end_per_testcase(_Case, Config) ->
    stop_peers(?config(peers, Config)),
    ok.

%% 核心：bootstrap 为 0..C-1 全部分区建了元数据，且 owner ∈ Nodes。
bootstrap_creates_all_session_partitions(Config) ->
    Nodes = peer_nodes(Config),
    [First | _] = Nodes,
    ok = rpc(First, broker_cluster_session_bootstrap, ensure, [Nodes]),
    lists:foreach(
      fun(P) ->
              {ok, O} = rpc(First, broker_cluster_cache, owner, [P]),
              ?assert(lists:member(O, Nodes)),
              {ok, Rs} = rpc(First, broker_cluster_cache, replicas, [P]),
              ?assertEqual(lists:sort(Nodes), lists:sort(Rs))
      end, lists:seq(0, ?C - 1)),
    %% 权威 C 已建立，且与 env 覆盖一致：session_owner 读到的分区空间生效。
    ?assertEqual(?C, rpc(First, broker_cluster_session_owner, session_partition_count, [])),
    lists:foreach(
      fun(P) ->
              ?assertMatch({ok, _},
                           rpc(First, broker_cluster_session_owner, session_owner, [P]))
      end, lists:seq(0, ?C - 1)),
    ok.

%% 幂等：第二次运行不改变已达标分区的 epoch。
bootstrap_is_idempotent(Config) ->
    Nodes = peer_nodes(Config),
    [First | _] = Nodes,
    ok = rpc(First, broker_cluster_session_bootstrap, ensure, [Nodes]),
    Before = [rpc(First, broker_cluster, epoch_or_zero, [P]) || P <- lists:seq(0, ?C - 1)],
    ok = rpc(First, broker_cluster_session_bootstrap, ensure, [Nodes]),
    After = [rpc(First, broker_cluster, epoch_or_zero, [P]) || P <- lists:seq(0, ?C - 1)],
    ?assertEqual(Before, After),
    ok.

%%%_* Scaffolding — 从 broker_session_replica_SUITE 原样复制（勿自行重写）----------
%% start_peers/2, stop_peers/1, stop_peer/2, peer_name/2, is_code_dir/1,
%% sanitise/1, peer_nodes/1, prepare_peer/1, start_meta/1, boot_data_plane/0,
%% form_cluster_h/1, assign_partition/4, wait_until/1, rpc/3, rpc/4
%% —— 全部逐字复制自 broker_session_replica_SUITE.erl，仅把 peer_name 前缀改为 gap3b_。

%%%_* Cluster helpers ------------------------------------------------------
%%
%% `form_cluster_h/1' runs on the TEST node (it rpc's into each peer to hang the
%% metadata server). `assign_partition/4' and `boot_data_plane/0' execute in the
%% peer's process.

%% 每台挂元数据服，组成一个真正的 ra 组。
form_cluster_h(Nodes) ->
    ok = lists:foldl(fun(N, ok) -> rpc(N, start_meta, [Nodes]) end, ok, Nodes),
    ok.

%% 分配会话分区（ra 命令）：真实机器状态。
assign_partition(Partition, Owner, Nodes, Epoch) ->
    broker_cluster:assign(Partition, Owner, Nodes, Epoch).

%% 每节点启动 data plane（storage/queue/router/subscription + hooks）。
boot_data_plane() ->
    {ok, RaDir} = application:get_env(broker_cluster, data_dir),
    DataDir = filename:join(RaDir, "data"),
    ok = filelib:ensure_dir(filename:join([DataDir, "x"])),
    _ = [application:load(A)
         || A <- [broker_metrics, broker_core, broker_storage, broker_router,
                  broker_queue, broker_subscription, broker_cluster]],
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok = application:set_env(broker_storage, data_dir, DataDir),
    ok = application:set_env(broker_storage, wal, #{require_durable_fs => false}),
    ok = application:set_env(broker_queue, partitions, ?PARTITIONS),
    ok = application:set_env(broker_router, partitions, ?PARTITIONS),
    ok = application:set_env(broker_subscription, data_dir, DataDir),
    {ok, _} = application:ensure_all_started(broker_storage),
    [ok = broker_queue:create_partition(P) || P <- broker_queue:partition_ids()],
    {ok, _} = application:ensure_all_started(broker_router),
    {ok, _} = application:ensure_all_started(broker_queue),
    {ok, _} = application:ensure_all_started(broker_subscription),
    ok = application:set_env(broker_cluster, cross_node_delivery, true),
    ok = application:set_env(broker_cluster, cross_node_timeout_ms, 2000),
    ok = broker_cluster:install_hooks(),
    ok.

%%%_* Peers ----------------------------------------------------------------

start_peers(Case, Config) ->
    Names = [peer_name(Case, I) || I <- lists:seq(1, ?NODES)],
    SuiteDir = filename:dirname(code:which(?MODULE)),
    CodePaths = lists:usort([SuiteDir | [D || D <- code:get_path(), is_code_dir(D)]]),
    lists:reverse(
      lists:foldl(
        fun({I, Name}, Acc) ->
              DataDir = filename:join([?config(priv_dir, Config),
                                       atom_to_list(Case),
                                       "n" ++ integer_to_list(I), "ra"]),
              try
                  {ok, Pid, Node} = peer:start_link(
                                      #{name => Name,
                                        args => ["-pa" | CodePaths],
                                        wait_boot => 20000}),
                  ok = rpc(Node, prepare_peer, [DataDir]),
                  [#{pid => Pid, node => Node, data_dir => DataDir} | Acc]
              catch
                  Class:Reason:Stack ->
                      stop_peers(Acc),
                      erlang:raise(Class, Reason, Stack)
              end
        end, [], lists:zip(lists:seq(1, ?NODES), Names))).

stop_peers(Peers) ->
    lists:foreach(fun(#{pid := Pid}) ->
                      _ = try peer:stop(Pid) catch _:_ -> ok end
                  end, Peers),
    ok.

stop_peer(Config, Node) ->
    Peers = ?config(peers, Config),
    case [P || #{node := N} = P <- Peers, N =:= Node] of
        [#{pid := Pid}] -> _ = try peer:stop(Pid) catch _:_ -> ok end, ok;
        _ -> ok
    end.

peer_name(Case, I) ->
    list_to_atom("gap3b_" ++ sanitise(atom_to_list(Case)) ++ "_" ++ integer_to_list(I)).

is_code_dir(Dir) ->
    filename:basename(Dir) =:= "ebin".

sanitise(S) ->
    [case C of _ when C >= $a andalso C =< $z -> C;
                 _ when C >= $A andalso C =< $Z -> C;
                 _ when C >= $0 andalso C =< $9 -> C;
                 _ -> $_ end || C <- S].

peer_nodes(Config) ->
    [Node || #{node := Node} <- ?config(peers, Config)].

%%%_* Peer-side setup (runs on the peer) ----------------------------------

prepare_peer(DataDir) ->
    _ = application:load(broker_cluster),
    ok = application:set_env(broker_cluster, data_dir, DataDir),
    ok = application:set_env(broker_cluster, members, []),
    ok = application:set_env(broker_cluster, quorum_timeout_ms, 3000),
    ok = application:set_env(broker_cluster, replicate_timeout_ms, 1000),
    ok = filelib:ensure_dir(filename:join(DataDir, "x")),
    {ok, _} = application:ensure_all_started(ra),
    application:set_env(ra, data_dir, filename:absname(DataDir)),
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

start_meta(Members) ->
    ok = application:set_env(broker_cluster, members, Members),
    {ok, Meta} = broker_cluster_meta:start_link(),
    unlink(Meta),
    ok.

%%%_* Helpers --------------------------------------------------------------

wait_until(Pred) -> wait_until(Pred, 20000, 100).

wait_until(_Pred, 0, _Step) -> ct:fail({timeout, ?MODULE});
wait_until(Pred, Wait, Step) ->
    try Pred() of
        true -> ok;
        _ -> timer:sleep(Step), wait_until(Pred, Wait - Step, Step)
    catch
        _:_ -> timer:sleep(Step), wait_until(Pred, Wait - Step, Step)
    end.

%%%_* RPC -------------------------------------------------------------------

rpc(Node, F, A) ->
    rpc(Node, ?MODULE, F, A).

rpc(Node, M, F, A) ->
    case rpc:call(Node, M, F, A, 30000) of
        {badrpc, Reason} -> ct:fail({badrpc, Node, M, F, Reason});
        Result -> Result
    end.
