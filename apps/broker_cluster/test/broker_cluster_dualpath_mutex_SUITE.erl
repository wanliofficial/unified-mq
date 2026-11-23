%% @doc Plan 3 缺口 #2：claim 与 nodedown-migrate 双路径互斥的并发 CT。
%%
%% 真实 3 节点 ra 元数据库（`broker_cluster_meta' 状态机），脚手架复用自
%% `broker_session_replica_SUITE'。`claim/3' 与 `migrate/4' 都是穿过
%% `ra:process_command/3' 的真命令，故二者在同分区同 epoch 上的竞争是真 ra 竞争。
%%
%% 互斥硬保证（`broker_cluster_meta_machine')：claim 要 `E < Epoch' 才接受，
%% assign/migrate 要 `E < Epoch' 才接受（即 `E >= Epoch' 拒）；ra 串行 apply 保证
%% 后到者读到已推进的 epoch，故同 epoch 必一赢一 `{stale_epoch, E, E}'。
%%
%% 四用例（本 Task 只落第 1 个）：
%%   1. claim_then_migrate_same_epoch —— claim 先落地，migrate 同 epoch 被拒；
%%
%% 范围：只打命令层 claim-vs-migrate 互斥；`on_nodedown' 的遍历/选主已由
%% `broker_cluster_nodedown_SUITE' 覆盖，本套件不重复，也不引入真 kill。
-module(broker_cluster_dualpath_mutex_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(NODES, 3).
-define(PARTITION, 0).

all() ->
    [claim_then_migrate_same_epoch,
     migrate_then_claim_same_epoch].

%%%_* Fixtures --------------------------------------------------------------

init_per_suite(Config) ->
    case node() of
        nonode@nohost ->
            case net_kernel:start([unifiedmq_s3dpct, shortnames]) of
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
              ok = rpc(N, application, set_env, [broker_cluster, members, Nodes])
      end, Nodes),
    [{peers, Peers}, {nodes, Nodes} | Config].

end_per_testcase(_Case, Config) ->
    stop_peers(?config(peers, Config)),
    ok.

%%%_* Cases ----------------------------------------------------------------

claim_then_migrate_same_epoch(Config) ->
    [N1, N2, N3] = Nodes = peer_nodes(Config),
    P = ?PARTITION,
    {ok, 1} = rpc(N1, ?MODULE, assign_partition, [P, N1, Nodes, 1]),
    {ok, 2} = rpc(N2, broker_cluster, claim, [P, N2, 2]),
    ?assertEqual({error, {stale_epoch, 2, 2}},
                 rpc(N1, broker_cluster, migrate, [P, N1, N3, 2])),
    ok = wait_until_owner(Nodes, P, N2),
    ok.

%% 用例 2（确定性反向）：migrate 先落地（epoch 2，owner -> N3），claim 以同一
%% epoch 2 提交，必被拒（{stale_epoch, 2, 2}）；终态 owner 稳定为 migrate 的目标。
migrate_then_claim_same_epoch(Config) ->
    [N1, N2, N3] = Nodes = peer_nodes(Config),
    P = ?PARTITION,
    {ok, 1} = rpc(N1, ?MODULE, assign_partition, [P, N1, Nodes, 1]),
    %% migrate 先赢：owner -> N3，epoch -> 2。
    {ok, 2} = rpc(N1, broker_cluster, migrate, [P, N1, N3, 2]),
    %% claim 同 epoch 2 提交：必被拒。
    ?assertEqual({error, {stale_epoch, 2, 2}},
                 rpc(N2, broker_cluster, claim, [P, N2, 2])),
    %% 终态：owner 仍是 N3，三节点收敛一致。
    ok = wait_until_owner(Nodes, P, N3),
    ok.

%%%_* Cluster helpers -------------------------------------------------------

form_cluster_h(Nodes) ->
    ok = lists:foldl(fun(N, ok) -> rpc(N, start_meta, [Nodes]) end, ok, Nodes),
    ok.

assign_partition(Partition, Owner, Nodes, Epoch) ->
    broker_cluster:assign(Partition, Owner, Nodes, Epoch).

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

peer_name(Case, I) ->
    list_to_atom("s3dp_" ++ sanitise(atom_to_list(Case)) ++ "_" ++ integer_to_list(I)).

is_code_dir(Dir) ->
    filename:basename(Dir) =:= "ebin".

sanitise(S) ->
    [case C of _ when C >= $a andalso C =< $z -> C;
                 _ when C >= $A andalso C =< $Z -> C;
                 _ when C >= $0 andalso C =< $9 -> C;
                 _ -> $_ end || C <- S].

peer_nodes(Config) ->
    [Node || #{node := Node} <- ?config(peers, Config)].

%%%_* Peer-side setup (runs on the peer) -----------------------------------

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

wait_until_owner(Nodes, P, Expected) ->
    wait_until(
      fun() ->
              lists:all(
                fun(N) ->
                        rpc(N, broker_cluster_cache, owner, [P]) =:= {ok, Expected}
                end, Nodes)
      end).

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
