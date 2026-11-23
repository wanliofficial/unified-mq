# conn_fsm 接管端到端零重连真集群 CT 设计（缺口 #4）

> 状态：设计稿（brainstorming 产出，待用户复核）
> 日期：2026-11-23
> 依据：`docs/superpowers/specs/2026-11-22-session-state-replication-plan2-notes.md` §3 缺口表 #4

## 1. 背景与缺口

Plan 2 在**会话平面**证明了身份保持（订阅集 / inflight / 游标）：`broker_session_replica_SUITE`
用真 3 节点 ra 组证明 kill owner → claim → fencing 后订阅集不变、游标不回退。但 conn_fsm 的
接管路径（`nodedown → claim → 续传`）只由**单元级**套件覆盖，**从未**在真 3 节点 CT 里跑一条
**活 TCP / MQTT 连接**穿过 owner 崩溃 + takeover。这正是缺口 #4：

> conn_fsm 接管路径的**端到端**（MQTT 线级零重连）未在本 Plan 证明。本 Plan 只在**会话平面**
> 证明 identity 保持；conn_fsm 的 nodedown→claim→续传由单元级套件覆盖，未在真 3 节点 CT 里跑活
> TCP 连接。

本设计补这条 CT：**全真 wire**（真 gen_tcp socket + 手写 MQTT 客户端 + 真 ranch listener），
kill owner，断言同一 socket 不重连地继续投递（线级零重连透明性）+ takeover claim 端到端成功。

### 1.1 两个关键事实（决定设计形状）

1. **conn 的内联 claim 与自动启动的 migrator 竞争。** `broker_cluster_nodedown_mon` 在 app 监督树里
   （`broker_cluster_app.erl:17-25` → `broker_cluster_sup.erl:27-32`），默认挂 migrator handler
   `broker_cluster_nodedown:on_nodedown/1`，owner 崩溃后 1500ms（缺口 #1 的 `nodedown_debounce_ms`）
   触发 `migrate`。而 conn_fsm 的 claim 是**即时、fire-once、无重试**（`mqtt_conn_fsm.erl:210-243`）。
   两者经同一 ra 组串行 apply，绝不双 owner，但**谁赢是随机的**。本 CT 用「中和 migrator」消除这个
   竞争，让 conn 的即时 claim 成为唯一 actor。

2. **会话分区 ≠ 队列分区。** conn claim 的是**会话分区** `P = phash2(ClientId, session_partition_count)`
   （`broker_cluster_session_owner` 命名空间）；生产者 fence 盖的是**队列分区** epoch
   （`broker_router:route(Topic)` 命名空间）。两者独立编号。conn_fsm 对所有本地队列分区做消费者
   （`subscribe_partitions/2`，`mqtt_conn_fsm.erl:1054-1061`），交付续期靠本地 poll 兜底
   （`arm_poll/2` + `pull_round/1`，`mqtt_conn_fsm.erl2144-2191`），**与 claim 是否赢无关**。
   推论：**「零重连交付」不依赖 claim 胜出**；claim 是一个独立的所有权断言。本 CT 据此把断言分成
   两组（§4），透明性组不依赖 claim 结果，takeover 组在中和后确定。

## 2. 目标与边界

- **证明（In scope）**：
  1. 活 MQTT 客户端经真 TCP 连到 replica 节点（holder），其会话 owner 在另一节点（owner）。
  2. kill owner 后，**同一 socket 不重连**地继续收订阅匹配消息（线级零重连透明性）。
  3. conn_fsm 的 takeover claim 端到端触发成功（中和 migrator + 控 leader 后确定）。
- **不证明（Out of scope，归 Plan 2 或其他缺口）**：
  - producer-fencing 在**队列分区**的完整故事（`broker_session_replica_SUITE` 已覆盖）。
  - conn claim fire-once 无重试这个产品短板本身——本 CT 用「控 leader≠owner」规避，**不修**它，
    仅在设计 §7 记录为已知局限。

## 3. 拓扑与确定性手段

### 3.1 拓扑

3 节点 `[N1, N2, N3]`，真 ra 组。脚手架**复用** `broker_session_replica_SUITE`
（`apps/broker_cluster/test/broker_session_replica_SUITE.erl`）：`start_peers/2`、`prepare_peer/1`、
`form_cluster_h/1`、`start_meta/1`、`boot_data_plane/0`、`stop_peer/2`、`wait_until/1,2` 等 helper
照搬（或抽公共）。节点命名 `[N1, N2, N3]`，**角色（owner / holder / third）在 leader 选出后再定**（见 §3.3）。

环境设置（per peer，对齐 `broker_session_replica_SUITE:84-100`）：
- `members` = 三节点；`session_replication = true`；
- `session_partition_count = ?PARTITIONS`（**设 = 队列分区数**，保证 `partition_of/2` 命中真分区）；
- `boot_data_plane/0`（load apps、WAL backend、`require_durable_fs=false`、建队列分区、
  `cross_node_delivery=true`、`install_hooks/0`）。
- **中和 migrator**：在各 peer 设 `nodedown_debounce_ms = 60000`（足够大，测试窗口内 migrator 不触发）。
  必须在 `install_session_hook`（→ `rearm`）**之前**设好该 env，使 rearm 后 mon 读到高值。

### 3.2 把客户端与 owner 分置不同节点

- 角色在 leader 选出后定（§3.3）：`L` = ra leader，`Owner` = 会话分区 P 的 owner（**必 ≠ L**），
  `Holder` = 跑活 conn 的 replica（存活、`≠ Owner`），`Third` = 第三 replica 保多数派。
- assign：会话分区 P 的 owner = `Owner`，replicas = `[Owner, Holder, Third]`
  （`broker_cluster:assign(P, Owner, [Owner,Holder,Third], Epoch)`，带 bounded retry）。
- 客户端 `ClientId` 经挑选使 `partition_of(ClientId, ?PARTITIONS) == P`（控 ClientId 命中目标会话分区）。
- 客户端 TCP 连到 **Holder** 的 ranch listener：先 `rpc(Holder, <启动 listener>, ...)`，
  端口经 `rpc(Holder, ranch, get_port, [Ref])` 读回（全节点在 127.0.0.1）。
  → conn_fsm 跑在 Holder 上，`maybe_monitor_owner` 读到 P 的 owner 是远端 `Owner` → `monitor_node(Owner,true)`。

### 3.3 控 leader（确定性来源之二）

conn claim 是 fire-once 无重试；若被 kill 的 `Owner` 恰是 ra leader，claim 撞 `{error,no_leader}`
而永不重试（recon §flaky#1）。确定性手段：**先确定 ra leader，再把会话 owner 选为非 leader 节点**，
使 kill 目标（`Owner`）必 ≠ leader，且 leader（`L`）在 kill 后存活、ra 仍有 leader、conn 的即时
claim 能成功。具体：

1. `form_cluster_h` 后 `wait_until` 轮询确定 ra leader `L`（经 `ra:leader(SysId)` 或
   `broker_cluster_cache` / ra 状态查询，实现期择稳定读法）。
2. 从 `[N1,N2,N3] \ {L}` 中取 `Owner`；`Holder` 取另一存活节点（≠ Owner，可为 L 或第三节点）。
3. assign owner=`Owner`（≠ L）。kill 时 `L` 必存活。

> 这把 recon 的「pre-establish a leader on a survivor」落到具体动作：**owner 选非 leader 节点**，
> 而非试图用客户端写移动 ra leader（ra 客户端写不移动 leader，不可控）。

## 4. 断言集（全部硬断言）

### 4.1 透明性（健壮，不依赖 claim 胜出）

1. **同一 socket 不断**：客户端 `gen_tcp` 在 kill 后不收到 `{error,closed}`；且 kill 后
   PINGREQ→PINGRESP 仍在**同一 socket** round-trip。
2. **无新 CONNECT / 无新 CONNACK**：整个崩溃窗口内客户端只发 **1×CONNECT**、只收 **1×CONNACK**
   （手写客户端按 socket 计数）。
3. **conn_fsm pid 存活**：kill 前后 `is_process_alive(ConnPid)` 为真；或 `sys:get_state` 为
   `{connected, _}`。
4. **续期**：kill owner 后，发布者向匹配 `t/#` 的主题发消息，订阅者在**同一 socket** 收到（无重连）。

### 4.2 takeover（中和 migrator + 控 leader 后确定）

5. **cache owner 翻转**：`rpc(Holder, broker_cluster_cache, owner, [P])` 经 `wait_until` 轮询为 `Holder`。
6. **metric 递增**：`rpc(Holder, broker_metrics, snapshot, [])` 的 `cluster_session_takeover_ok` +1。

> 说明：断言 ⑤⑥ 在中和后由 conn 的 claim 唯一驱动，确定性来源是 §3.2（migrator 不触发）+
> §3.3（Owner 非 leader，claim 不撞 no_leader）。若中和失效让 migrator 赢，⑤⑥ 会红——这正是 CT
> 抓回归的价值，不是要掩盖的行为。

## 5. 复用与新增

- **复用**：`broker_session_replica_SUITE` 的 3 节点真 ra 脚手架（照搬 helper，避免过早抽公共模块）。
- **新增套件**：`apps/broker_cluster/test/broker_cluster_conn_fsm_takeover_dist_SUITE.erl`
  （跨集群 + 活 wire，按惯例所有多节点套件落 `apps/broker_cluster/test/`）。
- **MQTT 客户端**：复用 `apps/broker_proto_mqtt/test/mqtt_client.hrl` 的帧编解码
  （`mqtt_open/subscribe/publish/recv`），但 **`connect` 指向 Holder 的真 ranch port**
  （走真 listener → 真 ranch → 真 `mqtt_conn_fsm`），区别于单节点版经本地 `start_conn` 起 conn。
  - 需 `broker_proto_mqtt` 在 peer 上 load/started（`boot_data_plane` 起其依赖，套件 ensure start）。
  - 需 include 路径 `apps/broker_proto_mqtt/include`（`rebar.config:102-105` 的 test profile 已加
    `{i, "apps/broker_proto_mqtt/include"}`）。
- **观测性**：takeover metric 读法照 `broker_session_replica_SUITE:takeover_counters/1`
  （`broker_metrics:snapshot/0`）；owner 读 `broker_cluster_cache:owner/1`。

## 6. flaky 防护

- **复制是 fire-and-forget**（`broker_cluster.erl:251-269`，恒返回 `ok`）：kill 前必须 `wait_until`
  订阅态已在 Holder 本地副本表落定（照搬 `broker_session_replica_SUITE:128-137`），否则套件退化。
- assign / 控 leader / kill 后一律 `wait_until`（20s / 100ms step，既有预算），**不用固定 sleep**。
- kill 用 `stop_peer/2`（graceful `init:stop`，关 dist 连接保 ra 多数派）。
- `net_kernel:start` guard 照搬既有套件（shortnames；**不要**重施 `nocookie`，否则 peer 读
  `~/.erlang.cookie` 鉴权失败、表现为不透明的 `wait_boot` 超时）。
- `peer:start_link` 的 `wait_boot => 20000`；leader 经 `wait_until` 确认后才 kill。

## 7. 已知局限（写入文档）

1. **claim fire-once 无重试**（`mqtt_conn_fsm.erl:225`）：本 CT 用「控 leader≠owner」规避，**不修**此
   产品短板。若未来要暴露它，是另一个缺口（设计未决点）。
2. **会话分区 ≠ 队列分区**：本 CT 不主张 producer-fencing 由会话 claim 门控；「零重连交付」靠本地
   poll 兜底，独立于 claim。
3. **中和失效 = migrator 赢**：此时 takeover 断言 ⑤⑥ 红，是 CT 抓回归的预期信号，非待掩盖行为。
4. **`#st` 记录私有**：无 `owner_node/owner_part` 导出访问器；takeover 经 cache owner（⑤）+ metric（⑥）
   + 可选日志断言，不依赖读 `#st{}` 字段。

## 8. 验收判据

- 新套件 `broker_cluster_conn_fsm_takeover_dist_SUITE` 在真 3 节点 ra 组上全绿；
  `rebar3 ct --suite=...` 查 suite.log 为 `=result ok`（rebar3×OTP29 cover bug 末尾非零非测试失败）。
- 变异验证（设计期执行、证明 CT 真在测接管而非测「死节点一直持有数据」）：
  - 把 conn 的 claim 改 no-op → 断言 ⑤⑥ 红（owner 不翻转、metric 不增）。
  - 去掉 migrator 中和（`nodedown_debounce_ms=0`）→ migrator 与 conn 抢，takeover 断言出现随机红
    （证明中和是确定性的必要手段）。
- plan2-notes §3 缺口 #4 追加带日期批注（实现最后一步）。

## 9. 非目标（YAGNI）

- 不引入第三方 `emqtt` 客户端（其在 `test/protocol-compatibility` 子工程、不在主 CT build）；
  用手写 `mqtt_client.hrl` 帧编解码即可。
- 不为可观测性新增 `#st` getter（§7.4 已规避）。
- 不修 conn claim 重试短板（§7.1 记录为后续缺口）。
- 不抽公共 dist 脚手架模块（两套件后若再需要再抽，避免过度抽象）。
