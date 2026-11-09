# S4 — 集群与一致性执行计划

- **依据**：[ADR-021](../adr/021-cluster-control-plane-raft-and-fencing.md)、ADR-002/003/007/008、README §6/§11.3、roadmap §6
- **状态**：✅ 完成（2026-11-08，Gate S4 通过）
- **不动的东西**：`#broker_message{}`、MQTT 3.1.1 线上字节、默认单节点行为（钩子未注册时 no-op）

## 1. 交付物

| 交付物 | 模块/应用 | 验收 |
|---|---|---|
| Mnesia 控制面（11 表） | `broker_meta` | 建表/读写/版本/outbox；不含消息正文与 cursor |
| 配置版本 + 通知 + 对账 | `broker_meta_config` | `bump/1` 广播；直接改表后周期对账补发 |
| Outbox | `broker_meta_outbox` | pending → publisher → published；失败不标记 |
| Metadata Raft Group | `broker_cluster_meta` + `broker_cluster_meta_machine` | assign/migrate/finish；epoch 单调；单/多节点 |
| 归属缓存与 Fencing | `broker_cluster_cache` + `broker_cluster` | 旧 epoch 写入 `{error, {fenced, _}}` |
| 失去多数派 | `broker_cluster` | 元数据写 `{error, no_quorum}`；可靠 append 按配置拒绝 |
| 复制与 ACK 等级 | `broker_cluster_rep` + `broker_queue` 钩子 | `replicated_commit` 只在 quorum 后返回 `{ok, Offset}` |
| 指标 | `broker_metrics` | 归属变更、fencing 拒绝、复制失败/延迟、no-quorum |
| 分布式测试 | `broker_cluster_dist_SUITE` | 3 节点 ra、杀 Leader、失去多数派、旧 owner fencing |

## 2. 实现步骤

1. `broker_meta`：schema/11 表/CRUD/用户哈希/ACL/partition_meta；`broker_meta_config`；`broker_meta_outbox`。
2. `broker_cluster_meta_machine`：ra 状态机（assign/migrate/finish/remove + 查询）。
3. `broker_cluster_meta`：ra 启停、命令提交、`no_quorum` 超时。
4. `broker_cluster_cache` + `broker_cluster`：本地 ETS 归属/epoch 缓存、`authorize_append/2`、`token/1`。
5. `broker_cluster_rep`：按 replicas 做复制、quorum 等待、超时/失败返回。
6. `broker_queue`：`persistent_term` 钩子（append guard / replicator），默认 no-op；`replicated_commit` 走复制钩子。
7. `mqtt_conn_fsm`：可靠等级配置为 `replicated_commit` 时使用该等级；拒绝路径沿用 S3（v5 `0x83` / 3.1.1 断开）。
8. 配置默认值：`config/sys.config.src` 的 `broker_meta` / `broker_cluster`；默认单节点、单副本、钩子生效但不改变 fsync 语义。
9. 测试：`broker_meta_SUITE`、`broker_cluster_SUITE`、`broker_cluster_dist_SUITE`（peer 3 节点）；回归全量 CT。

## 3. 测试步骤

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
rm -rf _build/test/extras
rebar3 ct --suite=apps/broker_meta/test/broker_meta_SUITE
rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_SUITE
rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_dist_SUITE
rebar3 ct
rebar3 dialyzer
rebar3 xref
```

## 4. 完成口径

- README §11.3 每一项有具名用例或指向 S1~S3 已有证据的说明。
- 旧 owner 的 Fencing、失去多数派、Leader 切换、复制 ACK 时机有可复跑证据。
- 默认单节点 422 个既有用例保持通过；`dialyzer` 干净；`xref` 不新增未定义调用。
- 文档只写已实现事实；未实现项（如自动再平衡、跨机房复制）明确列为 S4 之外。


---

## 5. 交付记录（2026-11-08）

| 交付物 | 落地位置 | 证据 |
|---|---|---|
| Mnesia 控制面 11 表 | `apps/broker_meta/src/broker_meta.erl` | `broker_meta_SUITE` 10/10：建表/CRUD、用户 sha256+盐、ACL、partition/node、版本、outbox |
| 配置版本 + 通知 + 对账 | `broker_meta_config`、`broker_meta_outbox` | 版本广播与周期对账用例；发布器成功才写 `PublishedAt` |
| Metadata Raft Group | `broker_cluster_meta.erl`、`broker_cluster_meta_machine.erl` | `broker_cluster_SUITE` 单节点 assign/migrate/remove；`broker_cluster_dist_SUITE` 3 节点组形成 |
| 归属缓存与 Fencing | `broker_cluster_cache.erl`、`broker_cluster.erl` | `authorize_append/2` 旧 epoch 返回 `{error, {fenced, _}}`；迁移后旧 owner 被拒 |
| 失去多数派 | `broker_cluster.erl` | 3 节点停两节点后元数据写 `{error, no_quorum}` |
| 复制与 ACK 等级 | `broker_cluster_rep.erl` + `broker_queue` 钩子 | quorum 达成后才返回 `{ok, Offset}`；副本不足返回 `{error, {replication_failed, ..}}` 且不 ACK |
| 指标 | `broker_metrics` 的 `cluster_*` 键 | `broker_metrics_SUITE` 键集合用例 |
| 分布式测试 | `apps/broker_cluster/test/broker_cluster_dist_SUITE.erl` | 4/4：组形成与复制、Leader 故障切换、失去多数派、迁移后 Fencing |

**全量回归**：`rebar3 ct` **447/447**（S3 422 + `broker_meta` 10 + `broker_cluster` 11 + `broker_cluster_dist` 4），零 `FAILED`、零 `Skipped`；`rebar3 dialyzer` 0 warnings；`rebar3 xref` 121 条均为预期 unused-export（含新 app 的公开 API 与测试入口）。

**实现中确认的三个 ra/peer 事实**（避免后续重复踩坑）：

1. `ra:start_or_restart_cluster/5` 的首个 server 在本地直启、其余经 rpc；某成员已在运行时返回 `{error, {already_started, Pid}}`，再次建组会得到 `cluster_not_formed`。因此 `broker_cluster_meta:init/1` 用 `whereis(?SERVER)` 判断「已运行则只 attach」，并把本节点排在成员列表首位。
2. CT 默认 `nonode@nohost`；`peer` 需要分布式 origin。`broker_cluster_dist_SUITE:init_per_suite/1` 自行 `net_kernel:start/1`，且**不得**在启动前读取并回写 cookie（会拿到 `nocookie` 导致 peer 握手超时）。
3. peer 节点必须显式把测试套件 beam 目录加入 `-pa`，否则 `rpc(Node, prepare_peer, ...)` 报 `undef`。

**与 ADR-010 §10.5 的关系**：ADR-010 冻结时设想 S4 通过 `broker_router_hash:on_partition_change/2` 通知 Router 更新 `Destination → Partition`。S4 实际未走该路线，也无需走：`route/3` 的分区是 `phash2(Destination, N)` 纯函数，迁移只改 `(owner, epoch)`，不改分区编号；归属与 fencing 由 `broker_cluster`（ra 元数据 + `broker_cluster_cache` + `broker_queue` 的 `append_guard` / `replicator` 钩子）承担，见 §1 与 ADR-021 §D2~§D5。因此 `on_partition_change/2` 继续作为 optional callback 未实现（`broker_router_hash_SUITE:on_partition_change_stays_unimplemented_in_s1` 锁定），ADR-010 以「冻结后补充」记录该取代关系，冻结文本未改。

**S4 之外（明确不做）**：自动再平衡、跨机房复制、Mnesia 在线 schema 迁移工具、消息级 Raft 共识。
