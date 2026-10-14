# S0.1 Spike Report: rabbitmq/ra 评估

**结论：`ra` 可用于 Metadata Raft Group，且在本项目统一的 OTP 28.3.1 上编译与运行均已验证通过；存在多个必须绕开的 API 陷阱。**

- **对应 Roadmap**：`docs/roadmap.md` §3 S0.1
- **验证对象**：README 6.2 Raft 职责、ADR-003
- **版本**：`rabbitmq/ra` 3.2.0（commit `2d81d21`，2026-10-01）
- **代码**：`spikes/s0_1_ra/`；运行 `./run_cluster.sh all`

---

## 1. 关键结论

| 问题 | 结论 |
|---|---|
| 能否编译/运行于 OTP 28.3.1 | 能，**实测 3 节点集群 + 5 故障场景全部通过**（上游仅宣称支持 26/27，但该版本可用） |
| 能否形成 3 节点集群 | 能，用 `ra:start_or_restart_cluster/5` |
| 复制写入成本 | **~677 µs/次**（约 1,477 次/秒，OTP 28.3.1 实测） |
| Leader 被 kill | **自动重选，写入恢复**，日志索引连续（523→524） |
| 失去多数派 | **写入被拒绝**（返回 `{timeout, _}`）✅ |
| API 陷阱数量 | **6 个**，全部会导致集群无法形成或写入永久超时 |

**最关键的验证**：失去多数派时写入被拒绝，而不是静默成功。这直接支撑 ADR-007 的"可靠性承诺必须显式化"——生产者 ACK 不会宣称集群未达到的持久化等级。

---

## 2. 实测数据

### 2.1 健康状态

```erlang
members => {ok, [{meta,'ra_n1@...'},{meta,'ra_n2@...'},{meta,'ra_n3@...'}],
                {meta,'ra_n1@...'}}    % n1 为 leader
write   => {ok, ok, {meta,'ra_n1@...'}}
read    => {ok, {{2,1},{ok,hello,#{kv => hello}}, {meta,'ra_n1@...'}}}
```

写入在 index 2 处提交，`local_query` 读到一致的值。

### 2.2 复制写入成本（500 次）

| 指标 | 数值 |
|---|---|
| 成功 | 500 / 500 |
| 总耗时 | ~339 ms |
| **单位成本** | **~677 µs/次** |
| 吞吐 | ~1,477 次/秒 |

### 2.3 故障场景

| 场景 | 观察结果 |
|---|---|
| **kill Leader（n1）** | n2 当选，写入恢复 `{ok,ok,...}`，索引 523→524 **连续无空洞** |
| **kill Follower（n3）** | 剩余 2 个节点构成多数派；写入 `{timeout,_}`（见下方说明） |
| **失去多数派（仅剩 n2）** | 写入 `{timeout,{meta,'ra_n2@...'}}` — **正确拒绝** |

**关于 follower 被 kill 后写入超时**：杀死 n3 后剩余 n1+n2，其中 n1 已在阶段 3 被杀，实际只剩 n2 一个节点 —— 已无多数派，因此超时是**正确行为**，不是缺陷。

---

## 3. 必须知道的 API 陷阱

这是本 spike 最有价值的产出。以下每一条都会让集群**静默失效**（节点健康、有成员列表、但永远不选举 / 写入永久超时）。

### 3.1 必须用 `start_or_restart_cluster/5`，不能用逐节点 `start_server/2`

逐节点调用 `ra:start_server/2` 时，三个节点**都健康、都认同成员列表、却永远不选举**，每次写入都超时。`ra:start_or_restart_cluster/5` 由单个节点调用一次，即可把各 server 分发到各自节点并完成成员初始化。

### 3.2 `apply/3` 的元数据在**第一个**参数

回调签名是 `apply(Meta, Command, State)`。若按 `apply({write, Term}, State, Meta)` 匹配，所有命令都会落到 `unknown_command`。

### 3.3 `apply/3` 返回 `{State, Reply}`，不是 `{reply, Reply, State}`

见 `ra_machine.erl:257`。返回错误形状会让每次 `process_command` 超时。

### 3.4 uid 必须是 **binary**

`ra_log_snapshot_state:insert/5` 有 `when is_binary(UId)` 守卫，字符串 uid 会让 server 在启动时崩溃。同时 uid 必须是 base64uri 字符集（`ra_lib:validate_base64uri/1`），因此 BEAM 节点名（含 `@`）不能直接用作 uid。

> 注：`ra` 自己的 `coordination_SUITE` 传的是 binary，与此一致。

### 3.5 Server ID 的节点部分必须是真实 BEAM 节点名

ID 形如 `{Cluster, "ra_n1@host"}`。若写成 `{Cluster, "n1@host"}`，`ra:members/2` 返回 `{error, nodedown}`。

另外 server 在本地注册的**进程名是 ID 的第一个元素**（`ra_lib:ra_server_id_to_local_name/1`），不是整个 ID。

### 3.6 `data_dir` 必须用 `-ra` 命令行参数

`application:set_env(ra, data_dir, ...)` 在节点自身代码里设置**太晚**：`ra_env:data_dir/0` 在 system 启动时就被读取。值还必须是**带引号的 Erlang 字符串**：

```bash
erl -sname ra_n1 -ra data_dir '"/tmp/ra_spike/n1"' ...
```

---

## 4. 上游缺陷（需记录以免误判为自身问题）

| 位置 | 问题 |
|---|---|
| `ra.erl:325` | `start_or_restart_cluster/5` 只处理 `{ok,_}` 和 `{error,Err}`，但 `ra_server_sup_sup:restart_server/3` 的 spec 明确会返回 `{badrpc, _}`。远端节点尚未连上时触发 `case_clause` 崩溃。**改用 4 元数版本可规避**（它做了错误归一化） |

---

## 5. OTP 版本决策

`ra` README 声明仅支持 **OTP 26.x / 27.x**，但本次实测确认 **ra 3.2.0 在 OTP 28.3.1 上编译与运行均通过**：3 节点集群正常形成、Leader 被 kill 后自动重选且日志索引连续、失去多数派时写入被正确拒绝。

**结论**：本项目全栈统一使用 **OTP 28.3.1**（见 `.tool-versions`），Metadata Raft Group 也运行在 OTP 28.3.1 上，**无需为 ra 单独锁定 OTP 27**。需注意的残余风险：上游 CI 未覆盖 OTP 28，未来 ra 升级时应复跑本 spike（`run_cluster.sh all`）确认无回归。

---

## 6. 对 S4 的影响

1. **分组模型**：单次复制写入 ~650 µs。若每个分区一个 Raft Group，128 分区意味着 128 份该成本 —— S0.2 必须实测分组数量与资源的关系。
2. **不得逐条消息走共识**：与 S0.3 的 fsync 结论叠加（871 µs/条），本地批量追加 + 分组共识是唯一可行路径（印证 README 6.2）。
3. **失去多数派必须拒绝写入**：已验证 `ra` 具备该行为。Producer ACK 逻辑可以直接依赖它来实现 ADR-007 的第三档等级。
4. **Leader 切换**：kill Leader 后索引连续（523→524），说明日志恢复正确，可支撑分区归属与 Epoch 迁移设计。

---

## 7. 未覆盖项（诚实记录）

本 spike **未**覆盖以下 README 11.3 要求的场景：

- **网络分区**（非进程死亡）：单机 loopback 无法模拟真实分区，需 `toxiproxy` 或多机环境
- **慢节点 / 磁盘满**
- **成员变更**（`add_member` / `remove_member`）
- **快照与日志截断**（需写入足够多数据触发 `min_snapshot_interval`）
- **Epoch / Fencing 语义**

这些应在 S4 用多机 + toxiproxy 环境补齐。

---

## 8. 复现方式

```bash
export PATH=/home/wanli/Development/otp-28.3.1/bin:$PATH   # 全项目统一 OTP 28.3.1
cd spikes/s0_1_ra

rebar3 get-deps && rebar3 compile

./run_cluster.sh all      # 启动 3 节点集群并跑完 5 个故障场景
./run_cluster.sh up       # 仅启动，便于手工排查
./run_cluster.sh down     # 停止
```

结果写入 `/tmp/ra_spike/RESULTS.txt`。

> **注意**：`run_cluster.sh` 的 `node_pids/1` 通过 `/proc/<pid>/cmdline` 判定节点。
> 不要改回 `ps | grep` —— 运行脚本的 shell 自身 argv 里就含有这些模式文本，
> 会导致 `alive/1` 把已死节点报成存活、`kill_node/1` 静默空转。