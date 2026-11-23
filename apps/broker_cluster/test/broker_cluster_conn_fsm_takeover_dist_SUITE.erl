%% @doc 缺口 #4：conn_fsm 接管端到端（MQTT 线级零重连）真集群 CT。
%%
%% 活 MQTT 客户端经真 TCP 连到 replica 节点（holder）的 ranch listener；其会话
%% 分区 P 的 owner 在另一节点（owner）。kill owner 后断言：**同一 socket 不重连**
%% 地继续收订阅消息（线级零重连透明性）+ conn_fsm 内联 claim 端到端接管成功。
%%
%% 脚手架复用 broker_session_replica_SUITE 的真 3 节点 ra 组。关键事实：该 boot
%% 不启动 broker_cluster app，故 nodedown_mon（migrator）未起——conn 的内联 claim
%% 是 kill owner 后唯一的 takeover actor，takeover 确定性由「owner 选非 leader 节点」
%% 保证（避开 conn claim fire-once 撞 no_leader）。
-module(broker_cluster_conn_fsm_takeover_dist_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("broker_proto_mqtt/include/mqtt_frame.hrl").

-define(NODES, 3).
-define(PARTITIONS, 3).
-define(LISTENERS, [#{port => 0, num_acceptors => 4,
                      mqtt_max_qos => 1, max_keepalive_ms => 60000}]).

all() -> [live_connection_zero_reconnect_on_owner_crash,
          no_owner_no_takeover,
          connection_remaps_when_partition_epoch_changes].

%%%_* Fixtures --------------------------------------------------------------

init_per_suite(Config) ->
    case node() of
        nonode@nohost ->
            case net_kernel:start([unifiedmq_gap4ct, shortnames]) of
                {ok, _} -> [{started_net, true} | Config];
                {error, Reason} ->
                    ct:fail({cannot_start_distribution, check_epmd, Reason})
            end;
        _ -> [{started_net, false} | Config]
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
    %% 设 env（含 nodedown_debounce_ms 双保险）+ boot data plane（storage/queue/
    %% router/subscription；**不含** broker_cluster app → nodedown_mon 不起）。
    lists:foreach(
      fun(N) ->
              ok = rpc(N, application, set_env, [broker_cluster, members, Nodes]),
              ok = rpc(N, application, set_env, [broker_cluster, session_replication, true]),
              ok = rpc(N, application, set_env,
                       [broker_cluster, session_partition_count, ?PARTITIONS]),
              ok = rpc(N, application, set_env,
                       [broker_cluster, nodedown_debounce_ms, 60000]),
              ok = rpc(N, ?MODULE, boot_data_plane, [])
      end, Nodes),
    [{peers, Peers}, {nodes, Nodes} | Config].

end_per_testcase(_Case, Config) ->
    stop_peers(?config(peers, Config)),
    ok.

%%%_* Case ------------------------------------------------------------------

%% 缺口 #4 端到端：真 3 节点 ra 组；kill 会话 owner 后断言同 socket 不重连地续期
%% （PINGRESP + PUBLISH 仍在原 socket、conn_fsm pid 存活）+ 内联 claim 接管
%% （holder 的 cache owner 翻转 + metric `cluster_session_takeover_ok' 递增）。
%%
%% 确定性依据（关键）：会话 owner 选非 ra leader 节点（`assign_roles'），避开
%% conn claim fire-once 撞 `no_leader'；且 peer boot 不启动 broker_cluster app
%% → `nodedown_mon' migrator 未起，conn 的内联 `{nodedown,Owner}' handler 是
%% kill owner 后唯一的 takeover actor，吃 raw dist `:DOWN'、不等 60s 去抖，故
%% post-kill 用 ~20s 预算即可。
live_connection_zero_reconnect_on_owner_crash(Config) ->
    Nodes = peer_nodes(Config),
    %% Owner 必 ≠ ra leader（load-bearing，不可折叠）。
    #{owner := Owner, holder := Holder} = assign_roles(ra_leader(Nodes), Nodes),
    %% 选 ClientId 使命中会话分区 P（控 ClientId → 稳定 hash）。
    Count = rpc(Holder, broker_cluster_session_owner, session_partition_count, []),
    ClientId = <<"gap4-sub">>,
    P = rpc(Holder, broker_cluster_session_owner, partition_of, [ClientId, Count]),
    {ok, 1} = rpc(Holder, ?MODULE, assign_partition, [P, Owner, Nodes, 1]),
    %% 等 owner 复制进 Holder 的 cache，conn 才 monitor 得到远端 owner。
    %% `broker_cluster_cache:owner/1' 返回 `{ok, Owner}'（非裸 Owner）。
    wait_until(fun() -> rpc(Holder, broker_cluster_cache, owner, [P]) =:= {ok, Owner} end),
    %% 指标真 init + 四键齐备（否则 snapshot 回落全零，断言假绿/假红）。
    ok = ensure_metrics(Holder),
    %% 在 holder 起真 ranch listener。
    Port = start_listener(Holder),
    %% 崩溃前：建立 wire 连接。conn_fsm 只在 CONNECT 到达时才由 mqtt_conn_sup 起，
    %% 故 pid 检查必须在 open 之后。此刻只有订阅者一个连在 holder，conn_fsm_pid/1
    %% 唯一、可归属到订阅者连接。
    Sock = connect(Port),
    #connack{return_code = 0} = open(Sock, #{client_id => ClientId}),
    ConnPid = conn_fsm_pid(Holder),
    true = is_pid(ConnPid) orelse ct:fail(no_conn_yet),
    %% subscribe t/# QoS1 → SUBACK。
    #suback{} = subscribe(Sock, 1, <<"t/#">>, 1),
    %% 基线投递 + 基线 PINGRESP（崩溃前同 socket 可用）。
    Pub0 = connect(Port),
    #connack{return_code = 0} = open(Pub0, #{client_id => <<"gap4-pub0">>}),
    ok = send_publish(Pub0, 7, <<"t/a">>, <<"pre">>, 1),
    #puback{} = recv(Pub0),
    #publish{topic = <<"t/a">>, payload = <<"pre">>} = recv(Sock),
    #ping{kind = resp} = ping(Sock),
    %% CLOSE 发布者，让 kill 时 holder 上只有订阅者一个 conn（接管指标可归属、不歧义）。
    ok = gen_tcp:close(Pub0),
    %% === kill owner（graceful peer:stop；多数派存活：Holder + Third；leader != owner）===
    ok = stop_peer(Config, Owner),
    %% === 透明性①：同 socket 不闭、PINGRESP 仍 round-trip。不嗅探 socket（recv 0 会
    %% 消费帧/超时）；若 socket 已闭，ping 内 recv 以 {listener_recv,closed,_} exit，
    %% 用例失败——正是要的断言。 ===
    #ping{kind = resp} = ping(Sock),
    %% === 透明性②：conn_fsm pid 全程存活（未因 nodedown 重启/换 pid）。conn_fsm 在
    %% Holder（远端节点），`is_process_alive/1' 对非本节点 pid 抛 badarg，故经 rpc 在
    %% Holder 本地查。 ===
    true = rpc(Holder, erlang, is_process_alive, [ConnPid]),
    %% === 透明性③：续期——崩溃后 publish 仍在**同一 socket** 收到 PUBLISH（无重连）。
    %% 接管后投递靠 poll tick（~1s）兜底，给足超时；只放宽等待，不弱化「同 socket
    %% 收到」的断言。 ===
    Pub1 = connect(Port),
    #connack{return_code = 0} = open(Pub1, #{client_id => <<"gap4-pub1">>}),
    ok = send_publish(Pub1, 8, <<"t/b">>, <<"post">>, 1),
    #puback{} = recv(Pub1),
    #publish{topic = <<"t/b">>, payload = <<"post">>} = recv(Sock, 15000),
    %% === takeover①：Holder 的 cache owner 翻转为 Holder（内联 claim 成功）。 ===
    wait_until(fun() -> rpc(Holder, broker_cluster_cache, owner, [P]) =:= {ok, Holder} end),
    %% === takeover②：metric cluster_session_takeover_ok 递增（≥1）。 ===
    wait_until(fun() -> takeover_ok(Holder) >= 1 end),
    ?assert(takeover_ok(Holder) >= 1),
    ok = gen_tcp:close(Sock),
    ok = gen_tcp:close(Pub1),
    ok.

%% 读 holder 快照里四个接管键中的 cluster_session_takeover_ok（缺键 = 0，但
%% ensure_metrics/1 已先校验四键齐备，故此处缺键即显式失败）。
takeover_ok(Node) ->
    Snap = rpc(Node, broker_metrics, snapshot, []),
    maps:get(cluster_session_takeover_ok, Snap, 0).

%% 负向证明：会话分区无 owner（不 assign）→ conn 不 monitor 远端 →
%% kill 任意非 holder 节点都不触发 claim → takeover 确定不发生（metric 恒 0、
%% cache owner 不翻到 Holder）。与主用例对照：主用例 assign owner=Owner 后 kill →
%% takeover 发生；本用例不 assign → 不发生。这证明 takeover metric 真由 claim 驱动，
%% 而非凭空/本地计数。
no_owner_no_takeover(Config) ->
    Nodes = peer_nodes(Config),
    #{holder := Holder} = assign_roles(ra_leader(Nodes), Nodes),
    Count = rpc(Holder, broker_cluster_session_owner, session_partition_count, []),
    ClientId = <<"gap4-noowner">>,
    P = rpc(Holder, broker_cluster_session_owner, partition_of, [ClientId, Count]),
    %% 故意**不 assign**：分区 P 无 owner，conn 无远端可 monitor、kill 不触发 claim。
    %% 先把计数器归零并证明四键齐备——否则 `takeover_ok==0' 可能只是「metrics 未
    %% init 的全零」假绿（`snapshot/0' 回落 `zeros()'）。
    ok = ensure_metrics(Holder),
    ok = rpc(Holder, broker_metrics, reset, []),
    Port = start_listener(Holder),
    Sock = connect(Port),
    #connack{return_code = 0} = open(Sock, #{client_id => ClientId}),
    #suback{} = subscribe(Sock, 1, <<"t/#">>, 1),
    Before = takeover_ok(Holder),
    0 = Before,                          %% reset 归零后必为 0
    %% kill 一个**非 holder** 节点（无 claim 逻辑，kill 谁都不触发 takeover；holder
    %% 必须留着读 metric/cache）。不用 `hd(Nodes)': ra leader 常是 Nodes 首元素，
    %% 此时 `assign_roles/2' 的 Holder 恰是 `hd(Nodes)'，kill 掉就读不到 holder 了。
    Victim = hd([N || N <- Nodes, N =/= Holder]),
    ok = stop_peer(Config, Victim),
    timer:sleep(2000),
    %% takeover **确定不发生**：delta 仍 0、cache owner 仍非 Holder（owner/1 返回
    %% `{ok,Owner} | not_found'，从不是裸 node）。
    0 = takeover_ok(Holder) - Before,
    ?assertNotEqual({ok, Holder}, rpc(Holder, broker_cluster_cache, owner, [P])),
    %% 透明性仍成立（本地 poll 兜底，与 claim 无关）。
    #ping{kind = resp} = ping(Sock),
    ok = gen_tcp:close(Sock),
    ok.

%% Plan 3 (gap #3): 分区配置 epoch 变化 → conn 在 poll tick 上迁移到新分区
%% （owner_part 改变），但 socket 不重连（仍能 PINGRESP round-trip）。
%%
%% 注意：phash2(Cid, 3) 与 phash2(Cid, 6) 约 55% 的 id 会撞（迁移后分区号不变，
%% wait_until 必然超时）。故用 pick_remapping_client_id/0 现算一个在本用例的
%% 3→6 分区变化下确实会换分区的 id，绝不写死单个字面量。
connection_remaps_when_partition_epoch_changes(Config) ->
    Nodes = peer_nodes(Config),
    #{owner := Owner, holder := Holder} = assign_roles(ra_leader(Nodes), Nodes),
    Count = rpc(Holder, broker_cluster_session_owner, session_partition_count, []),
    ?assertEqual(?PARTITIONS, Count),
    ClientId = pick_remapping_client_id(Count, Count * 2),
    P = rpc(Holder, broker_cluster_session_owner, partition_of, [ClientId, Count]),
    {ok, 1} = rpc(Holder, ?MODULE, assign_partition, [P, Owner, Nodes, 1]),
    wait_until(fun() -> rpc(Holder, broker_cluster_cache, owner, [P]) =:= {ok, Owner} end),
    %% 建权威 C + epoch（conn 的 maybe_monitor_owner 才能记下 owner_epoch）。
    {ok, _} = rpc(Holder, broker_cluster, set_session_partition_count, [Count]),
    Port = start_listener(Holder),
    Sock = connect(Port),
    #connack{return_code = 0} = open(Sock, #{client_id => ClientId}),
    #suback{} = subscribe(Sock, 1, <<"t/#">>, 1),
    BeforePart = wait_owner_part(Holder),
    ?assertEqual(P, BeforePart),
    %% 提交更大的 C（epoch 递增）：conn 应在下一个 poll tick 上迁移。
    Bigger = Count * 2,
    {ok, _} = rpc(Holder, broker_cluster, set_session_partition_count, [Bigger]),
    AfterPart = wait_owner_part_changed(Holder, BeforePart),
    ?assertEqual(rpc(Holder, broker_cluster_session_owner, partition_of,
                     [ClientId, Bigger]),
                 AfterPart),
    %% socket 未重连：PINGRESP 仍在原 socket 上 round-trip。
    #ping{kind = resp} = ping(Sock),
    ok = gen_tcp:close(Sock),
    ok.

%% 找第一个在 OldC→NewC 下确实换分区的 client id（确定性、不走运气）。
pick_remapping_client_id(OldC, NewC) ->
    pick_remapping_client_id(OldC, NewC, 0).

pick_remapping_client_id(OldC, NewC, N) when N < 1000 ->
    Cid = <<"gap3-remap-", (integer_to_binary(N))/binary>>,
    case erlang:phash2(Cid, OldC) =/= erlang:phash2(Cid, NewC) of
        true -> Cid;
        false -> pick_remapping_client_id(OldC, NewC, N + 1)
    end.

%% holder 上唯一 conn 的 owner_part；连接刚建好时 owner_part 可能尚未写入，
%% 故轮询等待其非 undefined。
wait_owner_part(Holder) ->
    wait_until(fun() ->
                       P = owner_part_of(Holder),
                       is_integer(P)
               end),
    owner_part_of(Holder).

wait_owner_part_changed(Holder, Before) ->
    wait_until(fun() ->
                       P = owner_part_of(Holder),
                       is_integer(P) andalso P =/= Before
               end),
    owner_part_of(Holder).

owner_part_of(Holder) ->
    case conn_fsm_pid(Holder) of
        Pid when is_pid(Pid) -> rpc(Holder, mqtt_conn_fsm, owner_part, [Pid]);
        _ -> undefined
    end.

%% MQTT PINGREQ → 同 socket 收 PINGRESP。socket 若已闭，recv exit {listener_recv,...}。
ping(Sock) ->
    ok = gen_tcp:send(Sock, mqtt_frame:encode(#ping{kind = req})),
    recv(Sock).

%%%_* Cluster formation (照搬 broker_session_replica_SUITE) ------------------------

form_cluster_h(Nodes) ->
    ok = lists:foldl(fun(N, ok) -> rpc(N, start_meta, [Nodes]) end, ok, Nodes),
    ok.

start_meta(Members) ->
    ok = application:set_env(broker_cluster, members, Members),
    {ok, Meta} = broker_cluster_meta:start_link(),
    unlink(Meta),
    ok.

assign_partition(Partition, Owner, Nodes, Epoch) ->
    broker_cluster:assign(Partition, Owner, Nodes, Epoch).

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

%% 确证 holder 上 `broker_metrics' 真 init 过：否则 `snapshot/0' 回落全零
%% `zeros()'，让下面的接管断言假绿（`takeover_ok >= 1' 永远读 0、或
%% `takeover_ok =:= 0' 永远成立）。照搬 `broker_session_replica_SUITE:ensure_metrics/1'：
%% 缺状态就显式补 init，并校验四个接管键都在快照里（缺键 = 显式失败，不是静默 0）。
ensure_metrics(Node) ->
    case rpc(Node, persistent_term, get, [{broker_metrics, state}, undefined]) of
        undefined -> ok = rpc(Node, broker_metrics, init, []);
        _ -> ok
    end,
    %% snapshot/0 返回全部冻结键；取四个接管键。缺键 = maps:get 显式 Badkey 失败，
    %% 非静默 0；非整数 = 显式失败。
    Snap = maps:with(takeover_keys(), rpc(Node, broker_metrics, snapshot, [])),
    ?assertEqual(lists:sort(takeover_keys()), lists:sort(maps:keys(Snap))),
    lists:foreach(fun(K) -> ?assert(is_integer(maps:get(K, Snap))) end, takeover_keys()),
    ok.

takeover_keys() ->
    [cluster_session_takeover_ok, cluster_session_takeover_stale_epoch,
     cluster_session_takeover_no_replica, cluster_session_takeover_not_found].

%%%_* Peers (照搬 broker_session_replica_SUITE) ----------------------------------

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
    lists:foreach(
      fun(#{pid := Pid}) ->
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
    list_to_atom("gap4_" ++ sanitise(atom_to_list(Case)) ++ "_" ++ integer_to_list(I)).

is_code_dir(Dir) -> filename:basename(Dir) =:= "ebin".

sanitise(S) ->
    [case C of _ when C >= $a andalso C =< $z -> C;
                 _ when C >= $A andalso C =< $Z -> C;
                 _ when C >= $0 andalso C =< $9 -> C;
                 _ -> $_ end || C <- S].

peer_nodes(Config) -> [Node || #{node := Node} <- ?config(peers, Config)].

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

%%%_* Leader query + role assignment -------------------------------------------

%% 该 ra 组的 leader 节点（系统 id `broker_cluster`，server id `{broker_cluster_meta,N}'）。
%% 本 ra 版本无 `ra:leader/1`（plan 的写法已校准）：用 `ra:members/1`，其返回值是
%% `{ok, Members, {_Name, LeaderNode}}`（`ra_leader_call_ret` 的第三元素即 leader id）。
ra_leader(Nodes) ->
    [N | _] = Nodes,
    wait_until(
      fun() ->
              case rpc(N, ?MODULE, ra_members, [N]) of
                  {ok, L} when is_atom(L) -> {return, L};
                  _ -> false
              end
      end).

%% 在 peer 上跑：`broker_cluster_meta' 的 ra 组 leader 节点，从 members 的三元组取。
ra_members(N) ->
    case ra:members({broker_cluster_meta, N}) of
        {ok, _Members, {_Name, LeaderNode}} -> {ok, LeaderNode};
        Other -> Other
    end.

wait_until({return, V}) -> V;
wait_until(Pred) -> wait_until(Pred, 20000, 100).
wait_until(_Pred, 0, _Step) -> ct:fail({timeout, ?MODULE});
wait_until(Pred, Wait, Step) ->
    try Pred() of
        true -> ok;
        {return, V} -> V;
        _ -> timer:sleep(Step), wait_until(Pred, Wait - Step, Step)
    catch
        _:_ -> timer:sleep(Step), wait_until(Pred, Wait - Step, Step)
    end.

%% 角色分配：Owner 必 ≠ leader（避开 conn claim fire-once 撞 no_leader）。
%% Holder = 跑活 conn 的存活 replica（≠ Owner）；Third = 余下节点。
assign_roles(Leader, Nodes) ->
    Others = Nodes -- [Leader],
    Owner = hd(Others),
    Rest = Nodes -- [Owner],          %% 含 Leader + 另一个
    Holder = hd(Rest),
    Third = hd(Rest -- [Holder]),
    #{owner => Owner, holder => Holder, third => Third}.

%%%_* Listener on peer --------------------------------------------------------

start_listener(Node) ->
    %% 校准点 #3：必须先 `application:load(broker_proto_mqtt)` 再 set_env——`load/1`
    %% 会用 `.app' 里的缺省值（listeners=[]）覆盖此前 set 的 env（实测）。peer 的
    %% `boot_data_plane/0' 并不加载 broker_proto_mqtt，故这里显式先 load；否则
    %% listener 子 spec 永远为空，`ranch:get_port({mqtt,0})' 报 badkey。
    _ = rpc(Node, application, load, [broker_proto_mqtt]),
    ok = rpc(Node, application, set_env, [broker_proto_mqtt, listeners, ?LISTENERS]),
    ok = rpc(Node, application, set_env, [broker_proto_mqtt, pull_poll_interval_ms, 1000]),
    ok = rpc(Node, application, set_env, [broker_proto_mqtt, max_keepalive_ms, 60000]),
    {ok, _} = rpc(Node, application, ensure_all_started, [broker_proto_mqtt]),
    case rpc(Node, ranch, get_port, [{mqtt, 0}]) of
        Port when is_integer(Port) -> Port;
        Other -> ct:fail({listener_port_unavailable, Other})
    end.

%% holder 上当前唯一的 conn_fsm pid（mqtt_conn_sup 的子进程）。
conn_fsm_pid(Node) ->
    case rpc(Node, supervisor, which_children, [mqtt_conn_sup]) of
        Ch when is_list(Ch) ->
            case [Pid || {_Id, Pid, worker, _} <- Ch, is_pid(Pid)] of
                [Pid | _] -> Pid;
                [] -> undefined
            end;
        _ -> undefined
    end.

%%%_* Real-wire MQTT client (照搬 mqtt_listener_SUITE 客户端形态) --------------

connect(Port) ->
    {ok, Sock} = gen_tcp:connect({127, 0, 0, 1}, Port,
                                 [binary, {active, false}, {nodelay, true}]),
    Sock.

open(Sock, Knobs) ->
    ok = gen_tcp:send(Sock, mqtt_frame:encode(
                             #connect{client_id = maps:get(client_id, Knobs),
                                      clean_session = 1,
                                      keep_alive = 0})),
    recv(Sock).

subscribe(Sock, PacketId, Filter, QoS) ->
    ok = gen_tcp:send(Sock, mqtt_frame:encode(
                             #subscribe{packet_id = PacketId,
                                        topics = [{Filter, QoS}]})),
    recv(Sock).

send_publish(Sock, PacketId, Topic, Payload, QoS) ->
    ok = gen_tcp:send(Sock, mqtt_frame:encode(
                          #publish{qos = QoS, topic = Topic,
                                   packet_id = PacketId, payload = Payload})).

recv(Sock) -> recv(Sock, 5000).

recv(Sock, Timeout) ->
    Buf = buf(Sock),
    try mqtt_frame:decode(Buf) of
        {ok, Frame, Rest} -> set_buf(Sock, Rest), Frame;
        {more, _} ->
            case gen_tcp:recv(Sock, 0, Timeout) of
                {ok, Data} -> set_buf(Sock, <<Buf/binary, Data/binary>>), recv(Sock, Timeout);
                {error, R} -> exit({listener_recv, R, Buf})
            end;
        {error, R} -> exit({listener_recv_malformed, R, Buf})
    catch
        Class:Reason -> exit({listener_recv_crashed, {Class, Reason}, Buf})
    end.

buf(Sock) ->
    case erlang:get({gap4_buf, Sock}) of undefined -> <<>>; B -> B end.
set_buf(Sock, Buf) -> erlang:put({gap4_buf, Sock}, Buf), ok.

%%%_* RPC ---------------------------------------------------------------------

%% Runs on the PEER via rpc: used by start_peers/prepare_peer and form_cluster_h.
rpc(Node, F, A) ->
    rpc(Node, ?MODULE, F, A).

rpc(Node, M, F, A) ->
    case rpc:call(Node, M, F, A, 30000) of
        {badrpc, Reason} -> ct:fail({badrpc, Node, M, F, Reason});
        Result -> Result
    end.
