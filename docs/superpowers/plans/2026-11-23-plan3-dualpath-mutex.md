# claim 与 nodedown-migrate 双路径互斥并发 CT — 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 用真 3 节点 ra 组证明 `broker_cluster:claim/3` 与 `broker_cluster:migrate/4` 在同一分区、同一 epoch 上对撞时恰好一个成功、owner 唯一且三节点收敛，竞态后写权限只有一份。

**Architecture:** 新建一个 Common Test 套件 `broker_cluster_dualpath_mutex_SUITE`，复用 `broker_session_replica_SUITE` 的真 3 节点 peer 脚手架（`peer:start_link/1` + `prepare_peer/1` + `form_cluster_h/1`），使 `claim/3` / `migrate/4` 都是穿过 `ra:process_command/3` 的真命令。四个用例：确定性正向、确定性反向、并发循环 20 轮、fencing 可见后果。互斥硬保证来自 `broker_cluster_meta_machine` 的 epoch 单调守卫 + ra 串行 apply。

**Tech Stack:** Erlang/OTP 29.1.2、Common Test、OTP `peer` 模块、ra（Raft）、rebar3。编译/测试用 OTP 29.1.2（`/home/wanli/Development/otp-29.1.2/lib/erlang/bin`）。

**设计依据：** `docs/superpowers/specs/2026-11-23-plan3-dualpath-mutex-design.md`

---

## 文件结构

- **新建**：`apps/broker_cluster/test/broker_cluster_dualpath_mutex_SUITE.erl` — 本 Plan 唯一新增文件。承载 4 个用例 + 从 `broker_session_replica_SUITE` 复制的 peer 脚手架（peer 起停、`prepare_peer/1`、`start_meta/1`、`form_cluster_h/1`、`assign_partition/4`、`wait_until`/`retry`/`rpc` 辅助）。
- **不改动任何生产代码**。这是纯测试补强（缺口 #2）。
- **参考但不修改**：`apps/broker_cluster/test/broker_session_replica_SUITE.erl`（脚手架来源）、`apps/broker_cluster/src/broker_cluster.erl`（`claim/3`/`migrate/4`/`assign/4`/`authorize_append/2` 接口）、`apps/broker_cluster/src/broker_cluster_meta_machine.erl`（epoch 守卫）。

**为什么单文件而非拆文件：** 套件是自包含的 CT 证据，脚手架与用例同生共死；拆成 helper 模块反而增加 CT 加载负担。与 `broker_session_replica_SUITE` 单文件风格一致。

---

## Task 1: 套件骨架 + peer 脚手架 + 第一个确定性用例

**Files:**
- Create: `apps/broker_cluster/test/broker_cluster_dualpath_mutex_SUITE.erl`

本 Task 建出可编译、可跑的最小套件：3 个真 peer + 真 ra 元数据组 + `claim_then_migrate_same_epoch` 一个用例。脚手架完整复制自 `broker_session_replica_SUITE`（改了模块名、peer 名前缀 `s3dp_`、用例列表）。

- [ ] **Step 1: 写套件骨架与脚手架（含第一个用例）**

创建 `apps/broker_cluster/test/broker_cluster_dualpath_mutex_SUITE.erl`，完整内容如下：

```erlang
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
%% 四用例：
%%   1. claim_then_migrate_same_epoch —— claim 先落地，migrate 同 epoch 被拒；
%%   2. migrate_then_claim_same_epoch —— migrate 先落地，claim 同 epoch 被拒；
%%   3. concurrent_claim_vs_migrate_never_double_owner —— 20 轮并发对撞，
%%      每轮恰好一个 {ok}、owner 唯一且三节点收敛、epoch 单调；
%%   4. fencing_after_dualpath_race —— 赛跑后唯一 owner 可写、其余 fencing 拒。
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
    [claim_then_migrate_same_epoch].

%%%_* Fixtures --------------------------------------------------------------

init_per_suite(Config) ->
    %% 同其它 dist 套件：CT 默认以 nonode@nohost 起，带名 peer 经分布式联系。
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
    %% 真实 ra 元数据库：每台挂元数据服，assign/claim/migrate 才是真 ra 命令。
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

%% 用例 1（确定性正向）：claim 先落地（epoch 2），migrate 以同一 epoch 2 提交，
%% 必被 epoch 单调守卫拒（{stale_epoch, 2, 2}）；终态 owner 稳定为 claim 的目标。
claim_then_migrate_same_epoch(Config) ->
    [N1, N2, N3] = Nodes = peer_nodes(Config),
    P = ?PARTITION,
    {ok, 1} = rpc(N1, ?MODULE, assign_partition, [P, N1, Nodes, 1]),
    %% claim 先赢：owner -> N2，epoch -> 2。
    {ok, 2} = rpc(N2, broker_cluster, claim, [P, N2, 2]),
    %% migrate 同 epoch 2 提交：必被拒。
    ?assertEqual({error, {stale_epoch, 2, 2}},
                 rpc(N1, broker_cluster, migrate, [P, N1, N3, 2])),
    %% 终态：owner 仍是 N2，三节点收敛一致。
    ok = wait_until_owner(Nodes, P, N2),
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

%%%% 轮询直到三节点 cache 的 owner(P) 都等于 Expected。
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
```

- [ ] **Step 2: 清构建缓存并编译**

Run:
```bash
export PATH="$HOME/Development/otp-29.1.2/lib/erlang/bin:$PATH"
cd /home/wanli/.local/share/opencode/worktree/10af50/plan3-dualpath
rm -rf _build/test
rebar3 as test compile
```
Expected: 编译通过，无 error（`_checkouts` 的 deprecation warning 属既有噪声，可忽略）。

- [ ] **Step 3: 跑第一个用例**

Run:
```bash
export PATH="$HOME/Development/otp-29.1.2/lib/erlang/bin:$PATH"
cd /home/wanli/.local/share/opencode/worktree/10af50/plan3-dualpath
rm -rf _build/test
rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_dualpath_mutex_SUITE.erl --case=claim_then_migrate_same_epoch
```
Expected: `=result ok`（在 `_build/test/logs/**/suite.log` 里查；rebar3 可能因 OTP 29 cover bug 非零退出，以 suite.log 的 `=result ok` 为准——见 plan2-notes §5.3）。

- [ ] **Step 4: 提交**

```bash
cd /home/wanli/.local/share/opencode/worktree/10af50/plan3-dualpath
git add apps/broker_cluster/test/broker_cluster_dualpath_mutex_SUITE.erl
git commit -m "test(cluster): 双路径互斥 CT 骨架 + claim-then-migrate 确定性用例（缺口 #2 T1）"
```

---

## Task 2: 确定性反向用例 migrate-then-claim

**Files:**
- Modify: `apps/broker_cluster/test/broker_cluster_dualpath_mutex_SUITE.erl`（`all/0` + 新增用例函数）

- [ ] **Step 1: 在 `all/0` 加入新用例**

把 `all/0` 改为：

```erlang
all() ->
    [claim_then_migrate_same_epoch,
     migrate_then_claim_same_epoch].
```

- [ ] **Step 2: 新增用例函数（放在 `claim_then_migrate_same_epoch/1` 之后）**

```erlang
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
```

- [ ] **Step 3: 跑两个用例**

Run:
```bash
export PATH="$HOME/Development/otp-29.1.2/lib/erlang/bin:$PATH"
cd /home/wanli/.local/share/opencode/worktree/10af50/plan3-dualpath
rm -rf _build/test
rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_dualpath_mutex_SUITE.erl
```
Expected: suite.log 里两条 `=result ok`，无 `=result failed`。

- [ ] **Step 4: 提交**

```bash
cd /home/wanli/.local/share/opencode/worktree/10af50/plan3-dualpath
git add apps/broker_cluster/test/broker_cluster_dualpath_mutex_SUITE.erl
git commit -m "test(cluster): 双路径互斥 CT 确定性反向用例 migrate-then-claim（缺口 #2 T2）"
```

---

## Task 3: 并发循环用例 never_double_owner

**Files:**
- Modify: `apps/broker_cluster/test/broker_cluster_dualpath_mutex_SUITE.erl`（`all/0` + 新增用例 + `run_race/4` 辅助）

- [ ] **Step 1: 在 `all/0` 加入新用例**

```erlang
all() ->
    [claim_then_migrate_same_epoch,
     migrate_then_claim_same_epoch,
     concurrent_claim_vs_migrate_never_double_owner].
```

- [ ] **Step 2: 新增用例 + `run_race/5` 辅助（放在用例 2 之后）**

```erlang
%% 用例 3（并发打时序）：20 轮，每轮把分区重置到已知 epoch Base，然后并发让
%% claim(P, N2, Base+1) 与 migrate(P, N1, N3, Base+1) 对撞（同 epoch、不同目标）。
%% 断言每轮恰好一个 {ok, Base+1}、另一个 {error, {stale_epoch, Base+1, Base+1}}，
%% 且三节点 owner 收敛到赢家提议的目标、epoch = Base+1。
concurrent_claim_vs_migrate_never_double_owner(Config) ->
    [N1, N2, N3] = Nodes = peer_nodes(Config),
    P = ?PARTITION,
    Rounds = 20,
    lists:foldl(
      fun(Round, ok) ->
              %% Base 每轮 +2，严格大于上一轮竞态后的 epoch（Base+1），故 assign 恒被接受。
              Base = 2 * Round,
              {ok, Base} = rpc(N1, ?MODULE, assign_partition, [P, N1, Nodes, Base]),
              %% 两个独立异步 RPC 同时提交：claim 目标 N2、migrate 目标 N3。
              {ClaimRes, MigrateRes} = run_race(N2, N1, N3, P, Base),
              Winner = assert_one_winner(ClaimRes, MigrateRes, Base, N2, N3),
              ok = wait_until_owner(Nodes, P, Winner),
              ?assertEqual({ok, Base + 1}, rpc(N1, broker_cluster_cache, epoch, [P])),
              ok
      end, ok, lists:seq(1, Rounds)),
    ok.

%% 在 ClaimNode 发 claim(P, ClaimNode, Base+1)，在 MigrateNode 发
%% migrate(P, MigrateNode, MigrateTarget, Base+1)，两个 rpc:async_call 立即返回、
%% 真正并发提交，再各自 yield 取回结果。返回 {ClaimResult, MigrateResult}。
run_race(ClaimNode, MigrateNode, MigrateTarget, P, Base) ->
    Epoch = Base + 1,
    ClaimTag = rpc:async_call(ClaimNode, broker_cluster, claim, [P, ClaimNode, Epoch]),
    MigrateTag = rpc:async_call(MigrateNode, broker_cluster, migrate,
                                [P, MigrateNode, MigrateTarget, Epoch]),
    ClaimRes = rpc:yield(ClaimTag),
    MigrateRes = rpc:yield(MigrateTag),
    {ClaimRes, MigrateRes}.

%% 断言恰好一个 {ok, Base+1}、另一个 {error, {stale_epoch, Base+1, Base+1}}，
%% 返回赢家提议的目标 owner。两个都 ok 或两个都 error 都 ct:fail。
assert_one_winner({ok, E}, {error, {stale_epoch, E, E}}, _Base, ClaimTarget, _MigrateTarget) ->
    ClaimTarget;
assert_one_winner({error, {stale_epoch, E, E}}, {ok, E}, _Base, _ClaimTarget, MigrateTarget) ->
    MigrateTarget;
assert_one_winner(ClaimRes, MigrateRes, Base, _ClaimTarget, _MigrateTarget) ->
    ct:fail({unexpected_race_result, Base, ClaimRes, MigrateRes}).
```

> 注意（**2026-11-23 修正**）：原计划第三位置参数写成 `E`（即 `Base+1`），但调用点传的是 `Base`，二者永不相等 ⇒ 两个匹配分支恒不可达、所有合法结果都掉进 `ct:fail`（实测第一轮即红）。已改为 `_Base`：仅去掉这个调用方永不可满足的约束，arg1/arg2 的 `E` 相等链接（真正契约，要求两侧 epoch 同为 `Base+1`）保持不变，两个都 ok / 两个都 error 仍 `ct:fail`。Task 3 实现时已据此修正并落地。

> 注意：`migrate` 的 `From` 参数（这里是 `MigrateNode`=N1）只透传进命令、不参与 apply 校验；真正约束是 `To`(N3) 必须 ∈ replicas（`assign` 时 replicas=Nodes，故成立）。`Base = 2*Round` 每轮 +2，恒严格大于上一轮竞态结果后的终态 epoch（上一轮 `Base+1` 即本轮 `Base-1`），故每轮 `assign` 都被接受。

- [ ] **Step 3: 跑三个用例**

Run:
```bash
export PATH="$HOME/Development/otp-29.1.2/lib/erlang/bin:$PATH"
cd /home/wanli/.local/share/opencode/worktree/10af50/plan3-dualpath
rm -rf _build/test
rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_dualpath_mutex_SUITE.erl
```
Expected: suite.log 里三条 `=result ok`。若出现 `unexpected_race_result`，说明两路没有真正并发（都串行了）或守卫异常——检查 `rpc:async_call` 是否真的并行发起。

- [ ] **Step 4: 提交**

```bash
cd /home/wanli/.local/share/opencode/worktree/10af50/plan3-dualpath
git add apps/broker_cluster/test/broker_cluster_dualpath_mutex_SUITE.erl
git commit -m "test(cluster): 双路径互斥 CT 20 轮并发对撞用例（缺口 #2 T3）"
```

---

## Task 4: fencing 可见后果用例

**Files:**
- Modify: `apps/broker_cluster/test/broker_cluster_dualpath_mutex_SUITE.erl`（`all/0` + 新增用例）

- [ ] **Step 1: 在 `all/0` 加入新用例**

```erlang
all() ->
    [claim_then_migrate_same_epoch,
     migrate_then_claim_same_epoch,
     concurrent_claim_vs_migrate_never_double_owner,
     fencing_after_dualpath_race].
```

- [ ] **Step 2: 新增用例（放在用例 3 之后）**

```erlang
%% 用例 4（互斥的可见后果）：跑一轮 claim-vs-migrate 对撞，拿到赢家 owner W 后，
%% 断言写权限只有一份：W 以 Base+1 可写；两个非 owner（含败者）以 Base+1 被
%% fencing 拒；W 以陈旧 epoch Base 也被拒。
fencing_after_dualpath_race(Config) ->
    [N1, N2, N3] = Nodes = peer_nodes(Config),
    P = ?PARTITION,
    Base = 2,
    {ok, Base} = rpc(N1, ?MODULE, assign_partition, [P, N1, Nodes, Base]),
    {ClaimRes, MigrateRes} = run_race(N2, N1, N3, P, Base),
    Winner = assert_one_winner(ClaimRes, MigrateRes, Base, N2, N3),
    ok = wait_until_owner(Nodes, P, Winner),
    Epoch = Base + 1,
    %% 唯一合法 owner 以当前 epoch 可写。
    ok = rpc(Winner, broker_cluster, authorize_append, [P, Epoch]),
    %% 两个非 owner（含败者）以当前 epoch 写：被 fencing 拒。
    Losers = Nodes -- [Winner],
    [?assertMatch({error, {fenced, _}},
                  rpc(L, broker_cluster, authorize_append, [P, Epoch]))
     || L <- Losers],
    %% 赢家以陈旧 epoch 写：也被拒。
    ?assertMatch({error, {fenced, _}},
                 rpc(Winner, broker_cluster, authorize_append, [P, Base])),
    ok.
```

- [ ] **Step 3: 跑全部四个用例**

Run:
```bash
export PATH="$HOME/Development/otp-29.1.2/lib/erlang/bin:$PATH"
cd /home/wanli/.local/share/opencode/worktree/10af50/plan3-dualpath
rm -rf _build/test
rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_dualpath_mutex_SUITE.erl
```
Expected: suite.log 里四条 `=result ok`。

- [ ] **Step 4: 提交**

```bash
cd /home/wanli/.local/share/opencode/worktree/10af50/plan3-dualpath
git add apps/broker_cluster/test/broker_cluster_dualpath_mutex_SUITE.erl
git commit -m "test(cluster): 双路径互斥 CT fencing 可见后果用例（缺口 #2 T4）"
```

---

## Task 5: 反向验证（守卫被改成「接受相等 epoch」时测试必须变红）

这是**验收判据**，证明测试真锁住互斥而非退化通过。临时改守卫 → 跑 → 必须红 → 还原。

**Files:**
- Temporarily modify: `apps/broker_cluster/src/broker_cluster_meta_machine.erl:87`（`E >= Epoch` → `E > Epoch`）与 `:106`（`E < Epoch` → `E =< Epoch`）
- Test: `apps/broker_cluster/test/broker_cluster_dualpath_mutex_SUITE.erl`

- [ ] **Step 1: 备份并临时放宽守卫**

把 `broker_cluster_meta_machine.erl` 的 `assign/8` 守卫 `E >= Epoch`（第 87 行附近）改为 `E > Epoch`，`claim/6` 守卫 `E < Epoch`（第 106 行附近）改为 `E =< Epoch`。

Run:
```bash
cd /home/wanli/.local/share/opencode/worktree/10af50/plan3-dualpath
cp apps/broker_cluster/src/broker_cluster_meta_machine.erl /tmp/opencode/meta_machine.orig
# 手工编辑这两处守卫后继续
```

- [ ] **Step 2: 跑用例，确认变红**

Run:
```bash
export PATH="$HOME/Development/otp-29.1.2/lib/erlang/bin:$PATH"
cd /home/wanli/.local/share/opencode/worktree/10af50/plan3-dualpath
rm -rf _build/test
rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_dualpath_mutex_SUITE.erl
```
Expected: 至少确定性用例 1/2 与并发用例 3 变红（同 epoch 不再被拒，出现双成功或终态 owner 与预期不符）。

- [ ] **Step 3: 还原守卫**

```bash
cd /home/wanli/.local/share/opencode/worktree/10af50/plan3-dualpath
cp /tmp/opencode/meta_machine.orig apps/broker_cluster/src/broker_cluster_meta_machine.erl
git diff --stat apps/broker_cluster/src/broker_cluster_meta_machine.erl
```
Expected: `git diff` 为空（守卫已还原，生产代码零改动）。

- [ ] **Step 4: 还原后重跑，确认全绿**

Run:
```bash
export PATH="$HOME/Development/otp-29.1.2/lib/erlang/bin:$PATH"
cd /home/wanli/.local/share/opencode/worktree/10af50/plan3-dualpath
rm -rf _build/test
rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_dualpath_mutex_SUITE.erl
```
Expected: 四条 `=result ok`。此次不提交任何东西（生产代码未变）。

---

## Task 6: 全量回归 + 文档批注

**Files:**
- Modify: `docs/superpowers/specs/2026-11-22-session-state-replication-plan2-notes.md`（§3 缺口表 #2 追加带日期批注）

- [ ] **Step 1: 跑全量 CT（确认本套件不引入新失败）**

Run:
```bash
export PATH="$HOME/Development/otp-29.1.2/lib/erlang/bin:$PATH"
cd /home/wanli/.local/share/opencode/worktree/10af50/plan3-dualpath
rm -rf _build/test
rebar3 ct
```
Expected: 除既有已知无关项（kafka cover 崩溃，见 plan2-notes §5.3）外，本套件 4 个用例全绿；无新增 `=result failed`。若 rebar3 因 cover bug 非零退出，以 logs 里的逐 suite `=result` 为准。

- [ ] **Step 2: 在 plan2-notes §3 缺口表 #2 行追加批注**

在 `docs/superpowers/specs/2026-11-22-session-state-replication-plan2-notes.md` 的缺口表 #2 行下（表后），追加一段：

```markdown
> **2026-11-23 追加批注**：缺口 #2 已补并发 CT —— 新套件 `broker_cluster_dualpath_mutex_SUITE`（真 3 节点 ra 组）用确定性双向（claim-then-migrate / migrate-then-claim 各一赢一 `stale_epoch`）+ 20 轮并发对撞，证明 claim 与 nodedown-migrate 同分区同 epoch 下恰好一个成功、owner 唯一且三节点收敛、竞态后写权限只有一份。设计见 `2026-11-23-plan3-dualpath-mutex-design.md`。
```

- [ ] **Step 3: 提交**

```bash
cd /home/wanli/.local/share/opencode/worktree/10af50/plan3-dualpath
git add docs/superpowers/specs/2026-11-22-session-state-replication-plan2-notes.md
git commit -m "docs(plan2-notes): 缺口 #2 双路径互斥并发 CT 已补（Plan 3）"
```

---

## 自审清单（写完后已核对）

- **Spec 覆盖**：设计 §4 的 4 个用例 → Task 1（用例1）、Task 2（用例2）、Task 3（用例3）、Task 4（用例4）；§7 验收判据「守卫放宽须变红」→ Task 5；§7「全量不引入新失败」→ Task 6 Step 1；§7 文档 → Task 6 Step 2。
- **类型/签名一致**：`run_race/5`（ClaimNode, MigrateNode, MigrateTarget, P, Base）在 Task 3 定义、Task 4 以 `run_race(N2, N1, N3, P, Base)` 调用，参数个数与顺序一致。`assert_one_winner/5` 两处调用一致。`wait_until_owner/3`（Nodes, P, Expected）一致。
- **无占位符**：所有代码块为完整可粘贴内容；Task 5 的守卫改动是显式行号 + 具体改动（`>=`→`>`、`<`→`=<`）。
- **无未定义符号**：`?PARTITION`、`?NODES` 在 Task 1 定义；`peer_nodes/1`、`form_cluster_h/1`、`assign_partition/4`、`start_meta/1`、`prepare_peer/1` 均在 Task 1 脚手架中出现。
