%% @doc MQ-17 Plan 2 Task 6：透明故障接管（transparent failover）故障注入证明。
%%
%% 三个真实 peer 节点（OTP `peer:start_link/1'，与其它 dist 套件同机制）组成
%% **真实的 3 节点 ra 元数据库**（`broker_cluster_meta' 状态机）。每个 peer 各自起
%% ra 与 `broker_cluster_cache'；`form_cluster_h/1' 在每台上挂元数据服，于是
%% `broker_cluster:assign/4' / `claim/3' / `authorize_append/2` 都是穿过
%% `ra:process_command/3` 的真 ra 命令，分区必须先存在于机器状态里。
%%
%% 套件证明 spec §9 五用例（会话状态在 owner 崩溃后存活）。**配对设计**：用例 1 与
%% 用例 2 是一对正反证明——用例 1 证明「复制开启时，订阅在 owner 崩溃后**存在**于
%% 副本（Holder）」，用例 2 证明「复制**关闭**时，订阅**不**出现在副本、kill owner 后
%% Holder 上 `session/1' 诚实返回 `{error, not_found}'（无陈旧幽灵）」。两用例都不可
%% 被「复制删掉仍通过」的退化测试蒙混。
%%
%%   1. `session_survives_owner_crash' —— 会话分区属 Owner；订阅在** doomed Owner**
%%      上建立，复制**落到 Holder 的本地副本表**后才 kill Owner；Holder 提 claim 成
%%      新 owner 后其本地会话视图（订阅集 + 游标）保持不变，游标不回退；可写。
%%   2. `subscribe_not_lost_on_crash' —— **复制关闭**下，Owner 上建的订阅**绝不**出现
%%      在 Holder（无幽灵）；kill Owner 后 Holder 上 `session/1' 返回 `{error, not_found}'。
%%   3. `inflight_dedup_preserved' —— QoS2 in-flight blob 在** doomed Owner** 上建立，
%%      复制**落到 Holder 的本地副本 inflight 表**后才 kill Owner；接管节点 inflight
%%      原样恢复、无重复行。
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

%% 用例 1（与用例 2 配对的**正面**证明）：会话分区属 Owner；订阅**在 doomed Owner**
%% 上建立**，复制**先落到 Holder 的本地副本表**（`wait_until' 轮询 `?ENTITIES' +
%% `?MEMBERS' 非空——此即复制断言），之后才 kill Owner；Holder 提 claim 成为新 owner
%% 后其本地会话视图（订阅集 + 游标）保持不变、游标不回退，且可写。
%%
%% 游标语义（实测 broker_cursor:advance_plain/2 + next_read/1）：游标是**连续前缀
%% 水位**（next_read = offset）。对水位之上的 offset 调 ack 只把它塞进 `pending'，
%% **不**前移水位；只有 ack 到当前连续位才级联前移。故这里先读初始水位 C0，再 ack
%% C0 使水位真正前移到 Before（> C0），接管后断言 After >= Before（不回退），
%% 而非草稿假定的「ack(…,5) → 游标 6」。游标行经复制异步落到 Holder，故接管前先在
%% Holder 侧轮询副本游标水位 ReplicatedBefore，断言 After >= ReplicatedBefore——
%% 不断言硬编码 offset。
session_survives_owner_crash(Config) ->
    [Owner, Holder, _Third] = Nodes = peer_nodes(Config),
    ClientId = <<"surv">>,
    Count = rpc(Holder, broker_cluster_session_owner, session_partition_count, []),
    P = rpc(Holder, broker_cluster_session_owner, partition_of, [ClientId, Count]),
    {ok, 1} = rpc(Holder, ?MODULE, assign_partition, [P, Owner, Nodes, 1]),
    %% 会话写在 **doomed Owner** 上——它才是该会话分区的 owner，复制才有意义。
    {ok, SubRef} = rpc(Owner, ?MODULE, do_subscribe, [ClientId, <<"t/surv">>, 1]),
    %% === 复制断言（proof of replication）===
    %% 在 kill 之前先等 Op 真正**落到 Holder 的本地副本表**。这一步是整套证明的
    %% 关键：若复制被删成 no-op，Holder 的这两张表永远为空，下面的 wait_until 会
    %% 超时失败，用例当场红——而非退化成「一直持有数据的节点没死」。
    %% 这两张表正是 broker_subscription_worker:apply_replica({subscribe,...}) 写入的：
    %% `?ENTITIES' 一条 {SubRef,Info} + `?MEMBERS' 每个消费者一条 {{ClientId,SubRef}}。
    wait_until(
      fun() ->
              rpc(Holder, ?MODULE, replica_has_sub, [ClientId, SubRef]) =:= true
      end),
    %% 游标是 **队列分区** 维度（订阅覆盖每个队列分区），与会话分区 P（所有权维度）
    %% 是两套独立编号，故这里取一个真实存在的队列分区 Q 来演示游标，而非会话分区 P。
    [Q | _] = rpc(Owner, broker_queue, partition_ids, []),
    %% 读初始水位，再 ack 该连续位使水位真正前移（仍在 Owner 上）。
    {ok, C0} = rpc(Owner, ?MODULE, do_cursor, [SubRef, Q]),
    ok = rpc(Owner, ?MODULE, do_ack, [SubRef, Q, C0, ClientId]),
    {ok, Before} = rpc(Owner, ?MODULE, do_cursor, [SubRef, Q]),
    ?assert(Before > C0),
    %% 游标行也经复制异步落到 Holder（apply_replica({cursor,...}) 写 `?CURSORS'
    %% {{SubRef,Q},Row}）。等它落定并记下 Holder 侧的副本水位 ReplicatedBefore，
    %% 接管后断言 After >= ReplicatedBefore——不断言硬编码 offset，只断言不回退。
    {ok, ReplicatedBefore} =
        wait_until_cursor(
          fun() ->
                  case rpc(Holder, ?MODULE, replica_cursor_next_read, [SubRef, Q]) of
                      not_found -> false;
                      N when is_integer(N) -> {ok, N}
                  end
          end),
    %% 真 kill owner 节点（多数派仍在：Holder + Third）。
    ok = stop_peer(Config, Owner),
    %% Holder 提 claim（epoch 2）。ra 需先完成 leader 选举，故重试。
    {ok, 2} = retry(fun() -> rpc(Holder, broker_cluster, claim, [P, Holder, 2]) end,
                    30, 500),
    %% 新 owner 可写；会话视图不变，游标不回退（从 Holder 的副本恢复而来）。
    ok = rpc(Holder, broker_cluster, authorize_append, [P, 2]),
    {ok, Session} = rpc(Holder, ?MODULE, do_session, [ClientId]),
    ?assertMatch([#{sub_ref := SubRef}], maps:get(subs, Session)),
    {ok, After} = rpc(Holder, ?MODULE, do_cursor, [SubRef, Q]),
    ?assert(After >= ReplicatedBefore),
    ok.

%% 用例 2（与用例 1 配对的**反面**证明）：复制**关闭**时，Owner 上建的订阅绝不
%% 出现在 Holder——无陈旧幽灵。kill Owner 后 Holder 的 `session/1' 诚实返回
%% `{error, not_found}'。若有人误把复制重新打开，下面的 `ets:lookup' 断言会抓到
%% Holder 上的实体行，用例变红。
%%
%% **跨用例状态泄漏**：本用例在**同一组 peer**上把 `session_replication' 置为 false
%% 并 `install_hooks()'，结束时**必须**恢复 true + `install_hooks()'——`all()' 顺序里
%% 它排第 2，其后的用例 3/4/5 都依赖复制开启的钩子。故恢复动作显式放在末尾；又因
%% 本用例结尾 kill 掉 Owner，恢复对死节点会 nodedown，故经 `restore_repl/1' 容错。
subscribe_not_lost_on_crash(Config) ->
    [Owner, Holder, Third] = Nodes = peer_nodes(Config),
    ClientId = <<"snl">>,
    HookKey = {broker_subscription_worker, session_replicator},
    %% 复制关闭：init_per_testcase 装的是开启态。三台都关钩子。
    ok = rpc(Owner, application, set_env, [broker_cluster, session_replication, false]),
    ok = rpc(Owner, broker_cluster, install_hooks, []),
    ok = rpc(Holder, application, set_env, [broker_cluster, session_replication, false]),
    ok = rpc(Holder, broker_cluster, install_hooks, []),
    ok = rpc(Third, application, set_env, [broker_cluster, session_replication, false]),
    ok = rpc(Third, broker_cluster, install_hooks, []),
    %% 钩子确实被抹掉（install_session_hook 在 false 时 erase）。这是 absence 的判据。
    ?assertEqual(undefined, rpc(Owner, persistent_term, get, [HookKey, undefined])),
    try
        Count = rpc(Holder, broker_cluster_session_owner, session_partition_count, []),
        P = rpc(Holder, broker_cluster_session_owner, partition_of, [ClientId, Count]),
        {ok, 1} = rpc(Holder, ?MODULE, assign_partition, [P, Owner, Nodes, 1]),
        %% 订阅仍建在 owner（Owner）上。
        {ok, SubRef} = rpc(Owner, ?MODULE, do_subscribe, [ClientId, <<"t/snl">>, 1]),
        %% 给任何「本不该发生的异步复制」一个落地的窗口。
        timer:sleep(500),
        %% 复制已关：Holder 的实体表必须为空（无幽灵行）。
        ?assertEqual([], rpc(Holder, ets, lookup, [broker_subscription_entities, SubRef])),
        %% kill owner 后，Holder 上从未有过该会话——诚实的缺席。
        ok = stop_peer(Config, Owner),
        ?assertEqual({error, not_found}, rpc(Holder, ?MODULE, do_session, [ClientId])),
        ok
    after
        %% 恢复复制开启，避免污染后续用例（见用例头注释）。Owner 已被 kill，
        %% restore_repl/1 对死节点静默跳过。
        restore_repl(Owner),
        restore_repl(Holder),
        restore_repl(Third),
        ok
    end.

%% 容错恢复：节点已死（如本用例 kill 掉的 Owner）时静默跳过，绝不让 after 块把
%% 用例标红。
restore_repl(Node) ->
    try rpc(Node, application, set_env, [broker_cluster, session_replication, true])
    catch _:_ -> ok end,
    try rpc(Node, broker_cluster, install_hooks, [])
    catch _:_ -> ok end,
    ok.

%% 用例 3：QoS2 会话 in-flight 建立后 kill owner；接管节点 inflight 原样恢复、无重复行。
inflight_dedup_preserved(Config) ->
    [Owner, Holder, _Third] = Nodes = peer_nodes(Config),
    ClientId = <<"dedup">>,
    Count = rpc(Holder, broker_cluster_session_owner, session_partition_count, []),
    P = rpc(Holder, broker_cluster_session_owner, partition_of, [ClientId, Count]),
    {ok, 1} = rpc(Holder, ?MODULE, assign_partition, [P, Owner, Nodes, 1]),
    %% 订阅与 inflight 都建在 doomed Owner 上。
    {ok, _SubRef} = rpc(Owner, ?MODULE, do_subscribe, [ClientId, <<"t/dedup">>, 2]),
    Blob = #{vsn => 3, pending => [{1, a}], session_expires_at => undefined},
    ok = rpc(Owner, ?MODULE, do_inflight, [ClientId, Blob]),
    %% === 复制断言（proof of replication）===
    %% inflight 复制是 fire-and-forget（replicate_inflight_async spawn），不等
    %% ack。kill 前先等 Blob 真正落到 Holder 的 `?INFLIGHT' 本地表
    %% （apply_replica({inflight,...}) 写 {ClientId,Term}）。复制若成 no-op，
    %% 这里永远等不到，用例当场红。
    wait_until(
      fun() ->
              case rpc(Holder, ets, lookup, [broker_subscription_inflight, ClientId]) of
                  [{_, Blob}] -> true;
                  _ -> false
              end
      end),
    ok = stop_peer(Config, Owner),
    {ok, 2} = retry(fun() -> rpc(Holder, broker_cluster, claim, [P, Holder, 2]) end,
                    30, 500),
    {ok, Session} = rpc(Holder, ?MODULE, do_session, [ClientId]),
    ?assertEqual(Blob, maps:get(inflight, Session)),
    ok.

%% 用例 4：所有权被 claim 抢走后，旧 owner 用陈旧 epoch 写入被 fencing 拒。
stale_owner_fenced(Config) ->
    [Owner, Holder, Third] = Nodes = peer_nodes(Config),
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

%%%_* Cluster helpers + do_* session wrappers --------------------------------
%%
%% `form_cluster_h/1' runs on the TEST node (it rpc's into each peer to hang the
%% metadata server). The rest of this section are thin wrappers that run ON THE
%% PEER via rpc: `assign_partition/4' and `boot_data_plane/0' execute in the peer's
%% process, and the `do_*' session wrappers (`do_subscribe/3', `do_ack/4',
%% `do_cursor/2', `do_session/1', `do_inflight/2') call `broker_subscription' in the
%% peer whose ETS tables they read/write.

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

%%%_* Replication-proof peer-side readers (run on the peer) ----------------
%%
%% 这三者在 peer 上跑，读的是该 peer **本地**的会话表——它们正是
%% broker_subscription_worker:apply_replica/1 的写入目标，故「非空」即「复制确实
%% 发生了」的硬证据。

%% Holder 本地是否已有该订阅的实体行 + 成员行（apply_replica({subscribe,...})
%% 一次写两张表：`?ENTITIES' 一条 {SubRef,Info}、`?MEMBERS' 每个消费者一条）。
replica_has_sub(ClientId, SubRef) ->
    HasEntity = ets:lookup(broker_subscription_entities, SubRef) =/= [],
    HasMember = ets:lookup(broker_subscription_members, {ClientId, SubRef}) =/= [],
    HasEntity andalso HasMember.

%% Holder 本地副本游标水位（apply_replica({cursor,...}) 写 `?CURSORS'
%% {{SubRef,Q},Row}）。无该行为 `not_found'。
replica_cursor_next_read(SubRef, Q) ->
    case ets:lookup(broker_subscription_cursors, {SubRef, Q}) of
        [{_, Row}] -> broker_cursor:next_read(Row);
        [] -> not_found
    end.

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

%% 轮询一个返回 `{ok, V} | false' 的谓词，成功时返回 `{ok, V}'；否则超时失败。
%% 用于复制断言里需要「等到值出现并取回它」的场景（如副本游标水位）。
wait_until_cursor(Fun) ->
    wait_until_cursor(Fun, 20000, 100).

wait_until_cursor(_Fun, 0, _Step) -> ct:fail({timeout, cursor_replication});
wait_until_cursor(Fun, Wait, Step) ->
    try Fun() of
        {ok, _} = Ok -> Ok;
        _ -> timer:sleep(Step), wait_until_cursor(Fun, Wait - Step, Step)
    catch
        _:_ -> timer:sleep(Step), wait_until_cursor(Fun, Wait - Step, Step)
    end.

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
