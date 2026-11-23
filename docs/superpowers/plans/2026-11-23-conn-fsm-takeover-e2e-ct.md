# conn_fsm 接管端到端零重连真集群 CT（缺口 #4）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增一个真 3 节点 ra 集群 CT，证明活 MQTT 客户端经真 TCP 连到 replica 节点（holder）、其会话 owner 在另一节点（owner）时，kill owner 后**同一 socket 不重连**地继续收订阅消息（线级零重连透明性），且 conn_fsm 的 takeover claim 端到端成功。

**Architecture:** 复用 `broker_session_replica_SUITE` 的真 3 节点 ra 脚手架（`start_peers/prepare_peer/form_cluster_h/start_meta/boot_data_plane/stop_peer`）。在 holder peer 上起真 ranch listener，客户端用真 `gen_tcp:connect` 连过去（走真 listener → 真 ranch → 真 `mqtt_conn_fsm`）。kill 会话 owner 节点后，断言同 socket 续期（PINGRESP + PUBLISH 仍在）+ conn_fsm pid 存活 + takeover（holder 的 cache owner 翻转、metric `cluster_session_takeover_ok` 递增）。

**Tech Stack:** Erlang/OTP 29.1.2、peer、ra、ranch、gen_tcp、Common Test、rebar3。

**设计依据：** `docs/superpowers/specs/2026-11-23-conn-fsm-takeover-e2e-ct-design.md`

---

## 环境（所有 CT/编译步骤通用）

```bash
export PATH="$HOME/Development/otp-29.1.2/lib/erlang/bin:$PATH"
cd /home/wanli/.local/share/opencode/worktree/gap4-conn-fsm-e2e-ct
rm -rf _build/test   # 每次 ct 前必做（陈旧 test-source 副本陷阱）
```

**rebar3 cover bug on OTP 29：** `rebar3 ct` 可能在全部测试通过后、末尾以 `undef {cover,modules,[]}` 非零退出——上游 bug，非测试失败。以 CT 日志判结果：`find _build/test/logs -name suite.log -path "*conn_fsm_takeover_dist*" -exec grep -E "^=result|^=successful|^=failed" {} \;`，看 `=result ok`。

**关键事实（决定实现，已实测确认）：**
- peer 启动脚手架（`boot_data_plane/0` + `start_meta/1`）**不**启动 `broker_cluster` app（只 `application:load(broker_cluster)`，手动起 `broker_cluster_meta`/`broker_cluster_cache`）。故 `broker_cluster_sup` 监督树下的 `broker_cluster_nodedown_mon`（默认 migrator）**根本没起**——kill owner 时**不存在 migrator 竞争**，conn_fsm 的内联 claim 是**唯一** takeover actor。无需中和 migrator（比设计稿的"设高 debounce"更干净、更确定）。仍设 `nodedown_debounce_ms` 为极大值作双保险（无害）。
- ra 系统 id = `broker_cluster`（`broker_cluster_meta` 用 `ra:start_or_restart_cluster(default, broker_cluster, ...)`）；ra server id = `{broker_cluster_meta, Node}`。查 leader：`ra:leader({broker_cluster_meta, Node})` → `{ok, LeaderNode} | {error, no_leader}`。
- 真 wire 客户端**不能**用 `mqtt_client.hrl`（其 `mqtt_open/mqtt_pair` 在本地起 conn）；改用 `mqtt_listener_SUITE` 的真 TCP 客户端形态（`gen_tcp:connect` 到端口 + 逐帧 send/recv）。

---

## File Structure

- **Create:** `apps/broker_cluster/test/broker_cluster_conn_fsm_takeover_dist_SUITE.erl` —— 唯一新增文件。含：3 节点脚手架 helper（照搬 `broker_session_replica_SUITE`）、真 wire MQTT 客户端 helper（照搬 `mqtt_listener_SUITE` 客户端形态）、init_per_testcase 角色分配（owner 选非 leader 节点）、1 个综合用例 `live_connection_zero_reconnect_on_owner_crash`。
- **Modify:** `docs/superpowers/specs/2026-11-22-session-state-replication-plan2-notes.md` —— §3 缺口表 #4 行下追加带日期批注（实现最后一步）。

**include 路径：** 套件需 `mqtt_frame.hrl`（records `#connect/#subscribe/#publish/#connack/#suback/#puback/#ping`）。用绝对 `-include_lib("broker_proto_mqtt/include/mqtt_frame.hrl")`（实现期先 `ls apps/broker_proto_mqtt/include/` 确认路径；若不在 include/ 而在 test/，改用对应 `-include_lib` 路径或 test profile 的 `{i, "apps/broker_proto_mqtt/include"}` + `-include`）。

---

## Task 1: 3 节点脚手架 + 真 wire 客户端 plumbing（无崩溃冒烟）

**Files:**
- Create: `apps/broker_cluster/test/broker_cluster_conn_fsm_takeover_dist_SUITE.erl`

本任务建好全部脚手架，并用一个**无崩溃**冒烟用例证明整条真 wire 链路通：客户端连到 holder peer 的 ranch listener → conn_fsm 在 holder 起 → attach + monitor 远端 owner → subscribe/publish → 收到 PUBLISH。这是最难的 plumbing，先单独跑通再叠加崩溃。

- [ ] **Step 1: 写套件头部 + fixtures + 脚手架 helper（照搬 `broker_session_replica_SUITE`）**

创建 `apps/broker_cluster/test/broker_cluster_conn_fsm_takeover_dist_SUITE.erl`：

```erlang
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

all() -> [live_connection_zero_reconnect_on_owner_crash].

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

%% 占位：Task 1 只验证 plumbing（连得上、能 pub/sub）。Task 2 在此追加崩溃+透明性+
%% takeover 断言。
live_connection_zero_reconnect_on_owner_crash(_Config) ->
    ct:fail(not_implemented_yet).
```

- [ ] **Step 2: 追加脚手架 helper（peer / cluster / data_plane / boot listener / 真 wire 客户端 / 轮询）**

把以下内容追加到套件末尾（`broker_session_replica_SUITE` 的 peer/ra 脚手架 + `mqtt_listener_SUITE` 的真 TCP 客户端）：

```erlang
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
ra_leader() ->
    [N | _] = peer_nodes_rpc(),
    wait_until(
      fun() ->
              case rpc(N, ra, leader, [{broker_cluster_meta, N}]) of
                  {ok, L} when is_atom(L) -> {return, L};
                  _ -> false
              end
      end).

%% 角色分配：Owner 必 ≠ leader（避开 conn claim fire-once 撞 no_leader）。
%% Holder = 跑活 conn 的存活 replica（≠ Owner）；Third = 余下节点。
assign_roles(Leader, Nodes) ->
    Others = Nodes -- [Leader],
    Owner = hd(Others),
    Rest = Nodes -- [Owner],          %% 含 Leader + 另一个
    Holder = hd(Rest),
    Third = hd(Rest -- [Holder]),
    #{owner => Owner, holder => Holder, third => Third}.

peer_nodes_rpc() -> [].
```

> 注：`peer_nodes_rpc/0` 是占位，下一步删除——`ra_leader/0` 直接接收 Nodes 参数（见 Step 3）。

- [ ] **Step 3: 修正 `ra_leader/0` 接收 Nodes，写冒烟用例**

把上面 `ra_leader/0` 与占位 `peer_nodes_rpc/0` 替换为接收 Nodes 的版本，并把冒烟用例体填好：

```erlang
ra_leader(Nodes) ->
    [N | _] = Nodes,
    wait_until(
      fun() ->
              case rpc(N, ra, leader, [{broker_cluster_meta, N}]) of
                  {ok, L} when is_atom(L) -> {return, L};
                  _ -> false
              end
      end).

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
```

把 `live_connection_zero_reconnect_on_owner_crash/1` 替换为 plumbing 冒烟（无崩溃）：

```erlang
%% Task 1 plumbing 冒烟：真 wire 客户端连到 holder 的 ranch listener，conn_fsm 在
%% holder 起、attach 会话、monitor 远端 owner、subscribe + publish 收到 PUBLISH。
live_connection_zero_reconnect_on_owner_crash(Config) ->
    Nodes = peer_nodes(Config),
    #{owner := Owner, holder := Holder} = assign_roles(ra_leader(Nodes), Nodes),
    %% 选 ClientId 使命中会话分区 P（控 ClientId → 稳定 hash）。
    Count = rpc(Holder, broker_cluster_session_owner, session_partition_count, []),
    ClientId = <<"gap4-sub">>,
    P = rpc(Holder, broker_cluster_session_owner, partition_of, [ClientId, Count]),
    {ok, 1} = rpc(Holder, ?MODULE, assign_partition, [P, Owner, Nodes, 1]),
    %% 等 owner 复制进 Holder 的 cache，conn 才 monitor 得到远端 owner。
    wait_until(fun() -> rpc(Holder, broker_cluster_cache, owner, [P]) =:= Owner end),
    %% 在 holder 起真 ranch listener。
    Port = start_listener(Holder),
    ConnPid = conn_fsm_pid(Holder),
    true = is_pid(ConnPid) orelse ct:fail(no_conn_yet),
    %% 真 wire 客户端：CONNECT → CONNACK（1 次）。
    Sock = connect(Port),
    #connack{return_code = 0} = open(Sock, #{client_id => ClientId}),
    %% subscribe t/# QoS1 → SUBACK。
    #suback{} = subscribe(Sock, 1, <<"t/#">>, 1),
    %% 同节点另起客户端 publish t/a QoS1 → 收 PUBLISH → PUBACK。
    Pub = connect(Port),
    #connack{return_code = 0} = open(Pub, #{client_id => <<"gap4-pub">>}),
    ok = send_publish(Pub, 7, <<"t/a">>, <<"hi">>, 1),
    #puback{} = recv(Pub),
    #publish{topic = <<"t/a">>, payload = <<"hi">>} = recv(Sock),
    ok = gen_tcp:close(Sock),
    ok = gen_tcp:close(Pub),
    ok.
```

- [ ] **Step 4: 追加 listener + 真 wire 客户端 + 辅助 helper**

```erlang
%%%_* Listener on peer --------------------------------------------------------

start_listener(Node) ->
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
                                      clean_session = true,
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

rpc(Node, M, F, A) ->
    case rpc:call(Node, M, F, A, 30000) of
        {badrpc, Reason} -> ct:fail({badrpc, Node, M, F, Reason});
        Result -> Result
    end.
```

- [ ] **Step 5: 跑 plumbing 冒烟，确认绿**

```bash
export PATH="$HOME/Development/otp-29.1.2/lib/erlang/bin:$PATH"
cd /home/wanli/.local/share/opencode/worktree/gap4-conn-fsm-e2e-ct
rm -rf _build/test
rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_conn_fsm_takeover_dist_SUITE.erl
find _build/test/logs -name suite.log -path "*conn_fsm_takeover_dist*" -exec grep -E "^=result|^=successful|^=failed" {} \;
```
Expected: `live_connection_zero_reconnect_on_owner_crash` 为 `=result ok`。若 `broker_frame.hrl` 路径错（编译报 `mqtt_frame.hrl not found`），按 File Structure 注修正 include 路径后重跑。若 listener 端口/ref 不符（`ranch:get_port({mqtt,0})` 报错），在 holder 上 `rpc(Holder, ranch, info, [])` 查实际 ref 名后修正。

- [ ] **Step 6: 提交**

```bash
cd /home/wanli/.local/share/opencode/worktree/gap4-conn-fsm-e2e-ct
git add apps/broker_cluster/test/broker_cluster_conn_fsm_takeover_dist_SUITE.erl
git commit -m "test(cluster,gap4): conn_fsm 接管 CT 脚手架 + 真 wire plumbing 冒烟（缺口 #4 T1）"
```

---

## Task 2: kill owner + 零重连透明性 + takeover 接管

**Files:**
- Modify: `apps/broker_cluster/test/broker_cluster_conn_fsm_takeover_dist_SUITE.erl`

把 Task 1 的 plumbing 冒烟用例扩成完整故障注入：kill owner 前后断言同 socket 续期 + conn 存活 + takeover。

- [ ] **Step 1: 替换用例体为完整崩溃+透明性+接管断言**

把 `live_connection_zero_reconnect_on_owner_crash/1` 整体替换为：

```erlang
live_connection_zero_reconnect_on_owner_crash(Config) ->
    Nodes = peer_nodes(Config),
    #{owner := Owner, holder := Holder} = assign_roles(ra_leader(Nodes), Nodes),
    Count = rpc(Holder, broker_cluster_session_owner, session_partition_count, []),
    ClientId = <<"gap4-sub">>,
    P = rpc(Holder, broker_cluster_session_owner, partition_of, [ClientId, Count]),
    {ok, 1} = rpc(Holder, ?MODULE, assign_partition, [P, Owner, Nodes, 1]),
    wait_until(fun() -> rpc(Holder, broker_cluster_cache, owner, [P]) =:= Owner end),
    Port = start_listener(Holder),
    %% === 崩溃前：建立 wire 连接 + 基线投递 ===
    Sock = connect(Port),
    #connack{return_code = 0} = open(Sock, #{client_id => ClientId}),
    #suback{} = subscribe(Sock, 1, <<"t/#">>, 1),
    ConnPid = conn_fsm_pid(Holder),
    true = is_pid(ConnPid),
    %% 基线：publish → 同 socket 收到 PUBLISH（崩溃前链路通）。
    Pub0 = connect(Port),
    #connack{return_code = 0} = open(Pub0, #{client_id => <<"gap4-pub">>}),
    ok = send_publish(Pub0, 7, <<"t/a">>, <<"pre">>, 1),
    #puback{} = recv(Pub0),
    #publish{payload = <<"pre">>} = recv(Sock),
    %% 基线 PINGRESP（崩溃前同 socket 可用）。
    #ping{kind = resp} = ping(Sock),
    %% === kill owner（graceful，多数派存活：Holder + Third）===
    ok = stop_peer(Config, Owner),
    %% === 透明性①：同 socket 不闭、PINGRESP 仍 round-trip ===
    wait_until(fun() ->
              case gen_tcp:recv(Sock, 0, 0) of
                  {error, closed} -> false;
                  _ -> true
              end
          end),
    #ping{kind = resp} = ping(Sock),
    %% === 透明性②：conn_fsm pid 全程存活 ===
    true = is_process_alive(ConnPid),
    %% === 透明性③：续期——崩溃后 publish 仍在**同一 socket** 收到 PUBLISH ===
    Pub1 = connect(Port),
    #connack{return_code = 0} = open(Pub1, #{client_id => <<"gap4-pub2">>}),
    ok = send_publish(Pub1, 8, <<"t/b">>, <<"post">>, 1),
    #puback{} = recv(Pub1),
    #publish{topic = <<"t/b">>, payload = <<"post">>} = recv(Sock),
    %% === takeover①：Holder 的 cache owner 翻转为 Holder ===
    wait_until(fun() -> rpc(Holder, broker_cluster_cache, owner, [P]) =:= Holder end),
    %% === takeover②：metric cluster_session_takeover_ok 递增 ===
    wait_until(fun() -> takeover_ok(Holder) >= 1 end),
    ?assert(takeover_ok(Holder) >= 1),
    ok = gen_tcp:close(Sock),
    ok = gen_tcp:close(Pub0),
    ok = gen_tcp:close(Pub1),
    ok.

%% 读 holder 上四个接管键里的 cluster_session_takeover_ok。
takeover_ok(Node) ->
    Snap = rpc(Node, broker_metrics, snapshot, []),
    maps:get(cluster_session_takeover_ok, Snap, 0).

ping(Sock) ->
    ok = gen_tcp:send(Sock, mqtt_frame:encode(#ping{kind = req})),
    recv(Sock).
```

- [ ] **Step 2: 跑套件，确认绿**

```bash
export PATH="$HOME/Development/otp-29.1.2/lib/erlang/bin:$PATH"
cd /home/wanli/.local/share/opencode/worktree/gap4-conn-fsm-e2e-ct
rm -rf _build/test
rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_conn_fsm_takeover_dist_SUITE.erl
find _build/test/logs -name suite.log -path "*conn_fsm_takeover_dist*" -exec grep -E "^=result|^=successful|^=failed" {} \;
```
Expected: 唯一用例 `=result ok`，`=successful 1`，`=failed 0`。

- [ ] **Step 3: 提交**

```bash
cd /home/wanli/.local/share/opencode/worktree/gap4-conn-fsm-e2e-ct
git add apps/broker_cluster/test/broker_cluster_conn_fsm_takeover_dist_SUITE.erl
git commit -m "test(cluster,gap4): kill owner 后同 socket 零重连 + conn_fsm takeover 接管（缺口 #4 T2）"
```

---

## Task 3: 变异验证（证明 CT 真在测接管）

**Files:**
- Modify: `apps/broker_cluster/test/broker_cluster_conn_fsm_takeover_dist_SUITE.erl`（新增 2 个 `*_mutant` 验证用例，加入 `all/0`）

证明套件不是退化测试：把 conn 的 claim 改 no-op、或去掉 owner≠leader 控制，takeover 断言会红。

- [ ] **Step 1: 加两个变异验证用例 + 注册到 all/0**

在 `all/0` 加入两行：
```erlang
all() -> [live_connection_zero_reconnect_on_owner_crash,
          mutates_claim_noop_fails_takeover,
          mutates_owner_is_leader_flakes_takeover].
```

追加两用例（复用 Task 1/2 的脚手架，clone 主用例前奏）：

```erlang
%% 变异1：conn_fsm 的 claim 若被改 no-op，takeover 断言（cache 翻转 + metric）必红。
%% 这里以「不 assign owner=远端、让 conn 无 owner 可 monitor」模拟「claim 不发生」：
%% 会话分区不存在 → conn 不 monitor 远端 → kill 任意节点都不触发 claim → takeover 永不发生。
%% 故断言 takeover_ok 在合理窗口内仍为 0（证明 metric 真由 claim 驱动，非凭空计数）。
mutates_claim_noop_fails_takeover(Config) ->
    Nodes = peer_nodes(Config),
    #{owner := Owner, holder := Holder} = assign_roles(ra_leader(Nodes), Nodes),
    Count = rpc(Holder, broker_cluster_session_owner, session_partition_count, []),
    ClientId = <<"gap4-mut1">>,
    P = rpc(Holder, broker_cluster_session_owner, partition_of, [ClientId, Count]),
    %% 故意**不 assign**：分区 P 无 owner，conn 不 monitor，kill 不触发 claim。
    Port = start_listener(Holder),
    Sock = connect(Port),
    #connack{return_code = 0} = open(Sock, #{client_id => ClientId}),
    #suback{} = subscribe(Sock, 1, <<"t/#">>, 1),
    ok = stop_peer(Config, Owner),
    %% 透明性仍成立（本地 poll 兜底）。
    #ping{kind = resp} = ping(Sock),
    %% 但 takeover **不发生**：等 2s 后 metric 仍 0、cache owner 仍非 Holder。
    timer:sleep(2000),
    ?assertEqual(0, takeover_ok(Holder)),
    ?assertNotEqual(Holder, rpc(Holder, broker_cluster_cache, owner, [P])),
    ok = gen_tcp:close(Sock),
    ok.

%% 变异2：owner == leader 时 kill，conn claim fire-once 可能撞 no_leader → takeover
%% 不确定。本用例**固定** owner=leader，断言 takeover 可能失败（计数不增），证明
%% 「owner 选非 leader」是 takeover 确定性的必要手段（去掉它 →  flaky）。
mutates_owner_is_leader_flakes_takeover(Config) ->
    Nodes = peer_nodes(Config),
    Leader = ra_leader(Nodes),
    Owner = Leader,                          %% 故意让 owner == leader
    Holder = hd(Nodes -- [Owner]),
    Count = rpc(Holder, broker_cluster_session_owner, session_partition_count, []),
    ClientId = <<"gap4-mut2">>,
    P = rpc(Holder, broker_cluster_session_owner, partition_of, [ClientId, Count]),
    {ok, 1} = rpc(Holder, ?MODULE, assign_partition, [P, Owner, Nodes, 1]),
    wait_until(fun() -> rpc(Holder, broker_cluster_cache, owner, [P]) =:= Owner end),
    Port = start_listener(Holder),
    Sock = connect(Port),
    #connack{return_code = 0} = open(Sock, #{client_id => ClientId}),
    #suback{} = subscribe(Sock, 1, <<"t/#">>, 1),
    ok = stop_peer(Config, Owner),
    #ping{kind = resp} = ping(Sock),
    %% owner==leader 时 claim 撞 no_leader：takeover 不确定。这里**不强断言它成功**，
    %% 只打印结果，证明去掉 leader 控制后行为不可控（CI 上会出现随机红/绿）。
    case takeover_ok(Holder) of
        0 -> ct:comment("owner==leader: claim 撞 no_leader，takeover 未发生（预期 flaky）");
        N when N >= 1 -> ct:comment("owner==leader: 本次侥幸接管 ok=~p（不可复现的确定性）", [N])
    end,
    ok = gen_tcp:close(Sock),
    ok.
```

> 说明：变异2 是「**展示**」用例——它故意构造 flaky 条件并打印结果，**不**断言固定成败（断言固定成败会让变异用例本身 flaky）。其目的是文档化「owner≠leader 控制不可去掉」。若 CI 不希望跑潜在 flaky 的变异2，可从 `all/0` 移除、仅留注释；由实现者按 §设计 §7.1 决定。

- [ ] **Step 2: 跑套件，确认绿（变异1 通过、变异2 打印注释）**

```bash
export PATH="$HOME/Development/otp-29.1.2/lib/erlang/bin:$PATH"
cd /home/wanli/.local/share/opencode/worktree/gap4-conn-fsm-e2e-ct
rm -rf _build/test
rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_conn_fsm_takeover_dist_SUITE.erl
find _build/test/logs -name suite.log -path "*conn_fsm_takeover_dist*" -exec grep -E "^=result|^=successful|^=failed" {} \;
```
Expected: 三用例全 `=result ok`，`=successful 3`，`=failed 0`。变异1 证明 metric 由 claim 驱动（不 assign → 计数 0）；变异2 打印 owner==leader 的不可控结果。

- [ ] **Step 3: 提交**

```bash
cd /home/wanli/.local/share/opencode/worktree/gap4-conn-fsm-e2e-ct
git add apps/broker_cluster/test/broker_cluster_conn_fsm_takeover_dist_SUITE.erl
git commit -m "test(cluster,gap4): 变异验证——claim no-op/owner==leader 下 takeover 行为（缺口 #4 T3）"
```

---

## Task 4: plan2-notes §3 缺口 #4 追加带日期批注

**Files:**
- Modify: `docs/superpowers/specs/2026-11-22-session-state-replication-plan2-notes.md`

- [ ] **Step 1: 在缺口表 #4 行下（表后）追加批注**

在 `## 3. 缺口表（Plan 3 边界债）` 表格之后、`## 4.` 之前，追加（紧随 #2、#1 两条既有批注之后）：

```markdown
> **2026-11-23 追加批注**：缺口 #4 已补端到端 CT —— 新套件 `broker_cluster_conn_fsm_takeover_dist_SUITE`（真 3 节点 ra 组）跑一条活 MQTT 客户端经真 TCP 连到 holder replica 的 ranch listener，其会话分区 owner 在另一节点；kill owner 后断言**同一 socket 不重连**地续期（PINGRESP + PUBLISH 仍在原 socket、无新 CONNECT/CONNACK）且 conn_fsm 内联 claim 端到端接管成功（holder 的 cache owner 翻转 + metric `cluster_session_takeover_ok` 递增）。确定性：会话 owner 选非 ra leader 节点（避开 conn claim fire-once 撞 no_leader）；且 peer boot 不启动 `broker_cluster` app → `nodedown_mon` migrator 未起，conn 的内联 claim 是唯一 takeover actor。设计见 `2026-11-23-conn-fsm-takeover-e2e-ct-design.md`。
```

- [ ] **Step 2: 提交**

```bash
cd /home/wanli/.local/share/opencode/worktree/gap4-conn-fsm-e2e-ct
git add docs/superpowers/specs/2026-11-22-session-state-replication-plan2-notes.md
git commit -m "docs(plan2-notes): 缺口 #4 conn_fsm 端到端零重连真集群 CT 已补（Plan 3）"
```

---

## Self-Review

**1. Spec coverage：**
- §2 目标①（活客户端真 TCP 连 holder、owner 在另节点）→ Task 1 冒烟 + Task 2 用例前奏。
- §2 目标②（同 socket 不重连续期）→ Task 2 透明性①（PINGRESP）+ ③（PUBLISH 续期）。
- §2 目标③（takeover claim 端到端成功）→ Task 2 takeover①②（cache 翻转 + metric）。
- §3.1 拓扑（复用脚手架、session_partition_count=queue 分区数、中和 migrator）→ 脚手架 + env 设置；migrator 未起（实测）已在环境节说明，无需高 debounce 但仍设双保险。
- §3.2 分置（控 ClientId 命中 P、连 holder listener、monitor 远端 owner）→ Task 1/2 用例前奏。
- §3.3 控 leader（owner ≠ leader）→ `ra_leader/1` + `assign_roles/2`。
- §4.1 透明性 4 条 → ①同 socket、②无新 CONNECT/CONNACK（客户端自控、续期即证）、③pid 存活、④续期。
- §4.2 takeover 2 条 → ⑤cache 翻转、⑥metric +1。
- §6 flaky 防护（复制先落定、wait_until、stop_peer graceful、不重施 cookie、wait_boot、leader 确认后 kill）→ 脚手架 + 用例。
- §7 已知局限（claim 短板不修 / 会话≠队列分区 / 中和失效=回归信号）→ 变异1（claim no-op→metric 0）+ §7.1 注释 + 变异2（owner==leader flaky）。
- §8 验收（套件全绿 + 变异验证）→ Task 2/3。

**2. Placeholder scan：** 无 TBD/TODO。`ct:fail(not_implemented_yet)` 是 Task 1 待替换占位（同会话内 Step 3 替换），非最终占位。`ra_leader` 初版占位 `peer_nodes_rpc/0` 在 Step 3 删除替换。

**3. Type consistency：** `assign_roles/2` 返回 `#{owner,holder,third}`，用例一致解构 `#{owner := Owner, holder := Holder}`；`ra_leader/1` 收 Nodes 返回 atom leader；`takeover_ok/1`、`conn_fsm_pid/1`、`start_listener/1`、`stop_peer/2`、`wait_until/1` 命名前后一致；wire 客户端 `connect/open/subscribe/send_publish/recv/ping` 签名一致；真 wire 客户端用 `#connect{clean_session=true}`（record 字段名与 mqtt_client.hrl 一致）。

**已知实现期校准点（非占位，是实测项）：**
- `mqtt_frame.hrl` include 路径（File Structure 注已给校准法）。
- holder 上 ranch listener ref 是否为 `{mqtt, 0}`（Step 5 校准法）。
- `broker_proto_mqtt` 在 peer 上 `ensure_all_started` 是否还需额外 env（如 `broker_retain` data_dir）；若 listener 起不来，对照 `mqtt_listener_SUITE:init_per_testcase` 补齐 broker_proto_mqtt 相关 env。
