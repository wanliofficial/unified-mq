%% @doc MQ-17 Plan 2 Task 6：透明故障接管（transparent failover）故障注入证明。
%%
%% 三个真实 peer 节点（OTP `peer:start_link/1'，与其它 dist 套件同机制）组成
%% **真实的 3 节点 ra 元数据库**（`broker_cluster_meta' 状态机）。每个 peer 各自起
%% ra 与 `broker_cluster_cache'；`form_cluster_h/1' 在每台上挂元数据服，于是
%% `broker_cluster:assign/4' / `claim/3' / `authorize_append/2` 都是穿过
%% `ra:process_command/3` 的真 ra 命令，分区必须先存在于机器状态里。
%%
%% 套件证明 spec §9 五用例（会话状态在 owner 崩溃后存活）：
%%
%%   1. `session_survives_owner_crash' —— kill owner 后，连在 Holder 的客户端其
%%      本地会话视图（订阅集 + 游标）保持不变，游标不回退；Holder 提 claim 成为
%%      新 owner 后可写。
%%   2. `subscribe_not_lost_on_crash' —— kill 一个非 owner 副本后，订阅实体行仍是
%%      完整的单行（无半截行）。
%%   3. `inflight_dedup_preserved' —— QoS2 in-flight blob 在 kill owner 后原样恢复。
%%   4. `stale_owner_fenced' —— 所有权被 claim 抢走后，旧 owner（仍在线）以陈旧
%%      epoch 写入被 fencing 拒；其它旧副本亦然；新 owner 接受当前 epoch。
%%   5. `single_node_unchanged' —— 开关关闭（I2）时无 session-replicator 钩子，
%%      逐字节同今日。
%%
%% 范围声明：本套件在 **会话平面身份**（subs 集 + 游标）这一可观测层证明接管，并
%% **不** 触及 MQTT wire 层的「零重连」——那需要真实 broker 客户端连接在 Holder 上
%% 跑完整个 conn_fsm，超出本套件范围。判据是会话平面身份在接管后保持不变。
%%
%% 三处对 Task 草稿（plan 文档）的实测修正，均已在实现中处理并记录：
%%   * 草稿用 `broker_subscription_worker:cursor/4` 直接写游标——该 API 不存在；
%%     这里改用真实的 `broker_subscription:cursor/3` 读、`ack/4` 推进。
%%   * `session/1` 的 `subs` 是订阅 **map** 列表（`#{sub_ref := SubRef, ...}`），
%%     不是裸 SubRef 列表。
%%   * 会话分区空间与 broker_queue 分区空间是两套编号；`session_partition_count`
%%     设为与 queue 分区数相同，保证 `partition_of/2` 落在真实存在的队列分区上。
-module(broker_session_replica_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(NODES, 3).
-define(PARTITIONS, 3).

all() ->
    [session_survives_owner_crash,
     subscribe_not_lost_on_crash,
     inflight_dedup_preserved,
     stale_owner_fenced,
     single_node_unchanged].

%%%_* Fixtures --------------------------------------------------------------

init_per_suite(Config) ->
    %% 同其它 dist 套件：CT 默认以 nonode@nohost 起，带名 peer 经分布式联系。
    %% 见 broker_cluster_dist_SUITE 的完整说明（net_kernel:start/1 后不可再
    %% 重设 cookie，否则 peer 认证失败）。
    case node() of
        nonode@nohost ->
            case net_kernel:start([unifiedmq_s9ct, shortnames]) of
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
    %% 真实 ra 元数据库：每台挂元数据服，assign/claim 才是真 ra 命令。
    ok = form_cluster_h(Nodes),
    [Owner, Holder, Third] = Nodes,
    lists:foreach(
      fun(N) ->
              ok = rpc(N, application, set_env, [broker_cluster, members, Nodes]),
              ok = rpc(N, application, set_env, [broker_cluster, session_replication, true]),
              %% 与 queue 分区数对齐，partition_of/2 落在真实队列分区上。
              ok = rpc(N, application, set_env,
                       [broker_cluster, session_partition_count, ?PARTITIONS]),
              ok = rpc(N, ?MODULE, boot_data_plane, [])
      end, Nodes),
    [{peers, Peers}, {nodes, Nodes}, {owner, Owner}, {holder, Holder},
     {third, Third} | Config].

end_per_testcase(_Case, Config) ->
    stop_peers(?config(peers, Config)),
    ok.

%%%_* Cases ----------------------------------------------------------------

%% 用例 1：会话分区属 Owner；客户端会话状态在 Holder（持连节点）。kill Owner 后
%% Holder 提 claim 成为新 owner；本地会话视图（订阅集 + 游标）保持，游标不回退。
%%
%% 游标语义（实测 broker_cursor:advance_plain/2 + next_read/1）：游标是**连续前缀
%% 水位**（next_read = offset）。对水位之上的 offset 调 ack 只把它塞进 `pending'，
%% **不**前移水位；只有 ack 到当前连续位才级联前移。故这里先读初始水位 C0，再 ack
%% C0 使水位真正前移到 Before（> C0），接管后断言 After >= Before（不回退），
%% 而非草稿假定的「ack(…,5) → 游标 6」。
session_survives_owner_crash(Config) ->
    [Owner, Holder, Third] = peer_nodes(Config),
    Nodes = peer_nodes(Config),
    ClientId = <<"surv">>,
    Count = rpc(Holder, broker_cluster_session_owner, session_partition_count, []),
    P = rpc(Holder, broker_cluster_session_owner, partition_of, [ClientId, Count]),
    {ok, 1} = rpc(Holder, ?MODULE, assign_partition, [P, Owner, Nodes, 1]),
    {ok, SubRef} = rpc(Holder, ?MODULE, do_subscribe, [ClientId, <<"t/surv">>, 1]),
    %% 游标是 **队列分区** 维度（订阅覆盖每个队列分区），与会话分区 P（所有权维度）
    %% 是两套独立编号，故这里取一个真实存在的队列分区 Q 来演示游标，而非会话分区 P。
    [Q | _] = rpc(Holder, broker_queue, partition_ids, []),
    %% 读初始水位，再 ack 该连续位使水位真正前移。
    {ok, C0} = rpc(Holder, ?MODULE, do_cursor, [SubRef, Q]),
    ok = rpc(Holder, ?MODULE, do_ack, [SubRef, Q, C0, ClientId]),
    {ok, Before} = rpc(Holder, ?MODULE, do_cursor, [SubRef, Q]),
    ?assert(Before > C0),
    %% 真 kill owner 节点（多数派仍在：Holder + Third）。
    ok = stop_peer(Config, Owner),
    %% Holder 提 claim（epoch 2）。ra 需先完成 leader 选举，故重试。
    {ok, 2} = retry(fun() -> rpc(Holder, broker_cluster, claim, [P, Holder, 2]) end,
                    30, 500),
    %% 新 owner 可写；会话视图不变，游标不回退。
    ok = rpc(Holder, broker_cluster, authorize_append, [P, 2]),
    {ok, Session} = rpc(Holder, ?MODULE, do_session, [ClientId]),
    ?assertMatch([#{sub_ref := SubRef}], maps:get(subs, Session)),
    {ok, After} = rpc(Holder, ?MODULE, do_cursor, [SubRef, Q]),
    ?assert(After >= Before),
    _ = Third,
    ok.

%% 用例 2：kill 一个非 owner 副本后，订阅行仍是完整的单行（无半截行）。
subscribe_not_lost_on_crash(Config) ->
    [Owner, Holder, Third] = peer_nodes(Config),
    Nodes = peer_nodes(Config),
    ClientId = <<"snl">>,
    Count = rpc(Holder, broker_cluster_session_owner, session_partition_count, []),
    P = rpc(Holder, broker_cluster_session_owner, partition_of, [ClientId, Count]),
    {ok, 1} = rpc(Holder, ?MODULE, assign_partition, [P, Owner, Nodes, 1]),
    {ok, SubRef} = rpc(Holder, ?MODULE, do_subscribe, [ClientId, <<"t/snl">>, 1]),
    ok = stop_peer(Config, Third),
    %% 实体行要么整行在、要么整行不在。
    ?assertMatch([{SubRef, _}],
                 rpc(Holder, ets, lookup, [broker_subscription_entities, SubRef])),
    {ok, Session} = rpc(Holder, ?MODULE, do_session, [ClientId]),
    ?assertMatch([#{sub_ref := SubRef}], maps:get(subs, Session)),
    _ = Owner,
    ok.

%% 用例 3：QoS2 会话 in-flight 建立后 kill owner；接管节点 inflight 原样恢复、无重复行。
inflight_dedup_preserved(Config) ->
    [Owner, Holder, _Third] = peer_nodes(Config),
    Nodes = peer_nodes(Config),
    ClientId = <<"dedup">>,
    Count = rpc(Holder, broker_cluster_session_owner, session_partition_count, []),
    P = rpc(Holder, broker_cluster_session_owner, partition_of, [ClientId, Count]),
    {ok, 1} = rpc(Holder, ?MODULE, assign_partition, [P, Owner, Nodes, 1]),
    {ok, _SubRef} = rpc(Holder, ?MODULE, do_subscribe, [ClientId, <<"t/dedup">>, 2]),
    Blob = #{vsn => 3, pending => [{1, a}], session_expires_at => undefined},
    ok = rpc(Holder, ?MODULE, do_inflight, [ClientId, Blob]),
    ok = stop_peer(Config, Owner),
    {ok, 2} = retry(fun() -> rpc(Holder, broker_cluster, claim, [P, Holder, 2]) end,
                    30, 500),
    {ok, Session} = rpc(Holder, ?MODULE, do_session, [ClientId]),
    ?assertEqual(Blob, maps:get(inflight, Session)),
    ok.

%% 用例 4：所有权被 claim 抢走后，旧 owner 用陈旧 epoch 写入被 fencing 拒。
stale_owner_fenced(Config) ->
    [Owner, Holder, Third] = peer_nodes(Config),
    Nodes = peer_nodes(Config),
    ClientId = <<"fence">>,
    Count = rpc(Holder, broker_cluster_session_owner, session_partition_count, []),
    P = rpc(Holder, broker_cluster_session_owner, partition_of, [ClientId, Count]),
    {ok, 1} = rpc(Holder, ?MODULE, assign_partition, [P, Owner, Nodes, 1]),
    %% sanity：初始 owner 以 epoch 1 可写。
    ok = rpc(Owner, broker_cluster, authorize_append, [P, 1]),
    %% Holder 认领 epoch 2（模拟 owner 崩溃后的接管，但旧 owner 仍在线）。
    {ok, 2} = rpc(Holder, broker_cluster, claim, [P, Holder, 2]),
    %% 旧 owner（仍在）以 epoch 1 写：被拒。等它把 claim 应用进本地 cache（ra
    %% 各成员异步 apply）。
    wait_until(fun() ->
                case rpc(Owner, broker_cluster, authorize_append, [P, 1]) of
                    {error, {fenced, _}} -> true;
                    _ -> false
                end
            end),
    ?assertMatch({error, {fenced, _}},
                 rpc(Owner, broker_cluster, authorize_append, [P, 1])),
    %% 另一个仍存活的旧副本（Third）以当前 epoch 2 写也被拒——它不是 owner
    %%（不论它是否已应用 claim，它从来不是 owner）。
    ?assertMatch({error, {fenced, _}},
                 rpc(Third, broker_cluster, authorize_append, [P, 2])),
    %% 新 owner 接受当前 epoch。
    ok = rpc(Holder, broker_cluster, authorize_append, [P, 2]),
    ok.

%% 用例 5（回归保护 I2）：开关关闭时无 session_replicator 钩子，逐字节同今日。
single_node_unchanged(Config) ->
    [N1 | _] = peer_nodes(Config),
    ok = rpc(N1, application, set_env, [broker_cluster, session_replication, false]),
    ok = rpc(N1, broker_cluster, install_hooks, []),
    ?assertEqual(undefined,
                 rpc(N1, persistent_term, get,
                     [{broker_subscription_worker, session_replicator}, undefined])),
    ok.

%%%_* Cluster / peer-side helpers (run on the test node) ------------------

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

%% 本机 session 操作（在 peer 上跑）。
do_subscribe(ClientId, Filter, QoS) ->
    broker_subscription:subscribe(ClientId, Filter, QoS).
do_ack(SubRef, P, Offset, ClientId) ->
    broker_subscription:ack(SubRef, P, Offset, ClientId).
do_cursor(SubRef, P) ->
    broker_subscription:cursor(SubRef, P).
do_session(ClientId) ->
    broker_subscription:session(ClientId).
do_inflight(ClientId, Term) ->
    broker_subscription:inflight(ClientId, Term).

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
    list_to_atom("s9d_" ++ sanitise(atom_to_list(Case)) ++ "_" ++ integer_to_list(I)).

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

retry(_Fun, 0, _Wait) ->
    {error, exhausted};
retry(Fun, N, Wait) ->
    case Fun() of
        {ok, _} = Ok -> Ok;
        _ -> timer:sleep(Wait), retry(Fun, N - 1, Wait)
    end.

%%%_* RPC -------------------------------------------------------------------

rpc(Node, F, A) ->
    rpc(Node, ?MODULE, F, A).

rpc(Node, M, F, A) ->
    case rpc:call(Node, M, F, A, 30000) of
        {badrpc, Reason} -> ct:fail({badrpc, Node, M, F, Reason});
        Result -> Result
    end.
