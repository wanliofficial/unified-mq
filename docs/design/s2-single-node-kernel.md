# S2 方案设计 — 单机生产内核

- **文档版本**：v0.3（M0 完成；O1~O4 已拍板，D1~D9 结案，ADR-012~017 已签认并冻结）
- **对应 Roadmap**：[`docs/roadmap.md`](../roadmap.md) §4
- **S1 基线**：`4bf145b`（S1 全部任务含 T11 完成；`rebar3 ct` 264/264，零 FAILED / 零 Skipped，`dialyzer` 干净）
- **目标 Gate**：roadmap §4.3「Gate S2」
- **状态**：决策已冻结。**§3 的 D1~D9 是实施依据，改动必须走 ADR 修订**；每个子项目启动时另写一份带测试步骤的详细计划（任务表 + 验收证据 + 复跑入口）。
- **修订记录**：v0.1 规划草案 → v0.2 按用户四项拍板更新——O1 Shared ack=per-message + 连续前缀（Pulsar 风格，见 D2）、O2 DLQ 按租户可配命名 + 独立保留期（见 D4）、O3 MQTT 5 后移出 Gate S2 到 **S2.5**（见 D8/§4）、O4 `os_buffer` 双轨证明（CI 替身常驻 + `dm-flakey` 实机报告，见 D7）。估算从 ≈55 人日降为 **≈50 人日**（Gate S2 范围），另 S2.5 MQTT 5 约 7 人日独立跟踪。
- **与 S3 的关系**：roadmap 允许 S2/S3 并行。本文不覆盖 CloudEvents（S3），但 S2 的 `broker_metrics`（S2-A）与 `event` 字段位要为 S3 留出计数点。

---

## 1. 目标与非目标

### 1.1 目标（把 Gate S2 翻译成可验收断言）

| Gate S2 条目 | 本阶段要给出的可机械核对证据 |
|---|---|
| 三档可靠性逐档有自动化测试证明（尤其 `os_buffer` 的丢失边界） | 每档一份故障注入报告：`fsync`/`replicated_commit`（S4 前为"不可用"）与 `os_buffer` 的**丢失窗口**用可复现实验证明，而不是只测"正常路径能写" |
| 慢消费者 / 热点分区场景不导致 OOM | 有界队列 + 流控的压测：单连接慢消费、单分区热点、连接风暴三组，观测 RSS / mailbox / ETS 上限并给出阈值 |
| 基准报告已出 | `docs/benchmarks/`：单机吞吐、消息大小 / 批量大小 / 分区数 / 订阅数四维影响、延迟分位（p50/p99/p999）、恢复时间 |
| 无单一 GenServer 成为全局瓶颈 | 按 README §7.2 四条逐条检查，并以"挂起某进程仍能推进"的反证测试 + 基准数字支撑 |

### 1.2 非目标（本阶段明确不做）

- **不做复制 / Raft / 分区迁移**：`replicated_commit` 只保留接口位与"不可用"的显式错误，S4 实现（ADR-011 §11.4 已冻结）。
- **不做集群控制面、Mnesia 迁移、TLS/ACL 强化**：分别在 S4/S6。
- **不做 CloudEvents codec / 五档策略**：S3；S2 只保证 `event` 槽与计数点不被破坏。
- **不做第二协议**：S5（WebSocket/NATS/AMQP/Kafka/Pulsar）。
- **不做在线扩缩容**：分区数仍固定，弹性分区在 S4（ADR-008）。
- **不承诺 QoS2 跨崩溃 exactly-once**：见 D1，只承诺线协议语义 + 至少一次（跨崩溃允许重复，MQTT 允许 DUP）。

---

## 2. S1 基线盘点：已有什么，缺什么

| 能力 | S1 现状（可核对） | S2 目标 | 主要落点 |
|---|---|---|---|
| 分区与顺序 | 固定 4 分区；`route/3` 按 destination 哈希；单分区 `wal_writer` 串行 append | **写清并测试有序性边界**：同 destination 单发布者 FIFO；跨分区/跨发布者无全局序 | `broker_router`、`broker_queue`、文档 |
| QoS | 0/1；QoS2 PUBLISH 直接断连；无 PUBREC/PUBREL/PUBCOMP | 完整 QoS2 四步握手（收发双向），跨崩溃至少一次 | `mqtt_frame`、`mqtt_conn_fsm`、`.inflight` |
| Retain / Will | retain 剥离并记日志；Will 忽略并记日志 | retain 树（内存 + 持久化策略）、Will 延迟发布（含异常断连） | 新增 `broker_retain`（或 `mqtt_retain`）、FSM |
| 订阅语义 | 仅 Exclusive；`sub_ref = {ClientId, Filter}`；cursor 按客户端文件持久化 | Exclusive / Shared / Failover；**cursor 归属改为订阅（组）** | `broker_subscription`、`.subs`/`.cursors` v2 |
| 重投递 | 无重试定时器；断线重连按 cursor 重发（R7） | 指数退避、重试上限、DLQ（复用普通 destination）、TTL | `broker_redelivery`（`broker_subscription` 内）、DLQ 配置、`expires_at` |
| retention | `truncate/2` 只是逻辑边界，不删段 | 物理段删除 + 慢消费者守卫 + 保留策略配置 | `wal_recovery`/`wal_segment`/`broker_queue` |
| 背压 | group commit 有界批量（32/256）；无 mailbox 水位、无限流、无 flow permit | 生产者限速、消费者 permit、热点分区隔离 | `broker_queue`、`mqtt_conn_fsm`、配置 |
| 可观测性 | 只有 `wal_writer:stats/1` 与日志；无统一计数 | 连接/吞吐/延迟/积压/cursor lag/DLQ/WAL 延迟最小集 | 新 `broker_metrics` |
| 可靠性证明 | `fsync` 由 T14 R1a/R3 覆盖；`os_buffer` 明确**未证明**丢失边界（T14 只做 `kill -9`） | 三档逐档报告 | `tools/` 新故障注入 + 报告 |
| MQTT 5 | 不支持（CONNECT 按 3.1.1 解析，v5 走拒绝路径） | **后移 S2.5（O3）**；S2 只保留不破坏 3.1.1 的 properties 槽位预留 | `mqtt_frame`、`mqtt_conn_fsm` |
| 帧大小限制 | 无显式上限（只受 TCP/内存约束） | 可配置最大包长与超限姿态 | `mqtt_frame`、listener opts |

**结论**：S1 的内核形状（读路径不进 gen_server、每分区一个写者、cursor 是唯一读位置、通知只是提示）足以承载 S2；S2 的主要风险不在性能，而在**新增状态的所有权**（QoS2 release 状态、共享订阅 cursor、DLQ、retain 树）与**证明方法**（`os_buffer` 丢失边界）。

---

## 3. 必须先定案的决策（D1 ~ D9）

> 每条都要在对应子项目启动前落成 ADR 或本文批注。**未定案不写代码**——S1 的教训是冻结契约后的返工成本远高于评审成本。

**M0 冻结结论（2026-11-07）**：

| 决策 | 结论 | ADR |
|---|---|---|
| D1 QoS2 | release 状态进 `.inflight` v2；线协议四步 + 跨重连不重复、跨崩溃至少一次 | 012 |
| D2 订阅/cursor | `SubRef = {Kind,Key,Filter}`，cursor 归 SubRef；`broker_cursor` 为内部模块 | 013 |
| D3 retention | 只删满足 `max_offset <= 全局 committed` 且 `max_offset < min(live cursor)` 的段；独立 `wal_gc`；behavior 冻结 | 014 |
| D4 重投递/DLQ/TTL | `broker_redelivery` 表 + 单进程定时器；DLQ 按租户可配命名 + 独立保留期；TTL 用 `expires_at` | 015 |
| D5 背压 | 先测后调；生产者限速/断开，消费者 permit；阈值由基准反推 | 本文 §4.7 |
| D6 可观测性 | 不引入外部依赖；`broker_metrics`（counters/atomics + snapshot） | 016 |
| D7 `os_buffer` 证明 | 双轨：CI 替身常驻 + `dm-flakey` 实机报告 | 本文 §4.2 |
| D8 MQTT5 | 后移 S2.5，不阻塞 Gate；S2 只留兼容预留 | 017 |
| D9 会话文件迁移 | v1 读、v2 写、boot 内一次性升级，失败拒绝启动 | 013 |

### D1 QoS2 的语义与持久化范围

- **问题**：QoS2 的"恰好一次"在单机、可崩溃的 broker 上到底承诺到哪一层？receiver 侧 PUBREL 状态是否持久化？
- **选项**：(a) 只做线协议四步，release 状态不持久化，崩溃后按 QoS1 重发（DUP）；(b) release 状态写入 `.inflight`（新建 `vsn` 字段），跨重连不重复投递，跨崩溃仍允许重复；(c) 全量事务日志，追求跨崩溃不重复。
- **已定案（D1 = (b)，M0 采纳；见 ADR-012）**：与 MQTT 3.1.1 §4.3.3 的"至少一次 + DUP"一致；`#broker_message.reliability` 仍由发布者 QoS 映射（QoS2 → `fsync`），不新增等级。持久化形状：`.inflight` blob 从 `#{next_pid, pending}` 升为 `#{vsn := 2, next_pid, pending, released := #{Pid => Offset}, received := #{Pid => true}}`——`released` 是出站待 PUBCOMP，`received` 是入站已 PUBREC 待 PUBREL；`vsn=1` 走迁移分支。
- **影响**：`mqtt_frame`（新增 3 个帧类型）、`mqtt_conn_fsm`（出站两阶段）、`broker_subscription`（blob 版本）、T14 增加 R8/R9。
- **需要 ADR**：是（ADR-012）。

### D2 订阅类型与 cursor 归属

- **问题**：Shared/Failover 的 cursor 应该属于"客户端"还是"订阅组"？`sub_ref` 形状是否要改？
- **选项**：(a) `sub_ref` 从 `{ClientId, Filter}` 泛化为 `{Kind, Key, Filter}`，cursor 仍按 SubRef 持久化（Exclusive 的 Key=ClientId，Shared/Failover 的 Key=Group）；(b) 另建 `broker_cursor` 表按 `{Kind, Group, Partition}`，SubRef 不动。
- **已定案（D2 = (a)，M0 采纳；见 ADR-013）**：**单一真相**——cursor 的 owner 就是 SubRef，避免两张表对账。roadmap §4.1 提到的 `broker_cursor` 落成 `broker_subscription` **内部模块**（同一 app、同一 owner），不新开 app，否则又出现第二个 cursor 真相。Router 只要求 `sub_ref()` 不透明（ADR-010 §10.1），形状改变**不触发 ADR-010 修订**；但 `.subs`/`.cursors` 的 body 与文件名要升 `vsn 2`（现有文件已是 `{vsn,1,ClientId,Body}`，有天然迁移位）。
- **已定案（O1）：Shared 的 ack = per-message ack + 连续前缀**（Pulsar 风格）。组内记录每条消息被哪个 consumer ack（`{SubRef, Partition} -> #{AckedOffsets :: gb_sets, Consumers :: ...}` 或等价 bitmap），`committed` 取**最小连续前缀**（从 `low` 起第一条未 ack 的 offset 的前一条），而不是取组内最慢 consumer 的标量 cursor。好处：一条慢消费者不会把整个组的水位钉住；坏处：状态比标量 cursor 大，需要位图压缩与快照。Exclusive 退化为单 consumer 的连续前缀（与 S1 等价），Failover 只对 active consumer 取前缀、standby 不推进。
- **影响**：`broker_subscription`（实体/游标/绑定三张表 + Shared 的 acked/assigned 行）、`mqtt_conn_fsm`（订阅集合读取）、`broker_subscription_files`、boot step 2+3。
- **需要 ADR**：是（ADR-013）。

### D3 retention / truncate 的物理删除

- **问题**：逻辑 `truncate` 何时变成物理删段？慢消费者把 `min(cursor)` 钉住时怎么办？
- **已定案（M0 采纳；见 ADR-014）**：保留策略 = `max_bytes` / `max_age` 二选一或同时；物理删除条件 = 段内最大 offset `<=` 全局 committed **且** `< min(live cursor)`（无未 ack 数据）；被钉住时只告警 + 计数（`broker_metrics`），**绝不越过慢消费者删数据**。新增 `broker_storage` optional callback `delete_before/2`？——不，保持 behavior 冻结：用 `truncate/2` 的既有语义 + 由内部段 GC（`wal_gc` 模块）实现；若必须加 callback，按 ADR-011 的规则走修订。
- **影响**：`wal_recovery`、`wal_segment`、`broker_queue`、配置、恢复测试。
- **需要 ADR**：是（ADR-014）。

### D4 重投递 / DLQ / TTL 的归属与数据形状

- **问题**：重试状态放 conn 还是 subscription？DLQ 是"特殊 destination"还是独立存储？TTL 用 `created_at` 还是 `expires_at`？
- **已定案（M0 采纳；见 ADR-015）**：
  - 重试状态归 `broker_redelivery`（新表，按 SubRef + offset 记 `attempts/next_at/reason`），conn 只执行定时唤醒；**不在 conn 里做重试定时器**（S1 L8 的教训：连接级定时器会随连接死亡丢失）。
  - DLQ 用普通 destination，复用同一套路由/存储，不引入第二套 I/O。**已定案（O2）：命名按租户可配，保留期独立**——默认形状 `$dlq/{tenant}/{原 destination}`（可通过 `broker_dlq.name_pattern` 配置模板，如 `$dlq/{tenant}/{qos}/{original}`），DLQ 的 `retention` 单独一组配置（不与源 destination 共享）；DLQ 消息带 `headers`（`x-original-destination`、`x-failure-reason`、`x-attempts`、`x-tenant`）。
  - TTL 激活 `#broker_message.expires_at`（ADR-009 已冻结的槽位，S1 恒 `undefined`），比较 `created_at`（broker 接收时间）；过期消息在 `read_from` 与投递前双重过滤，并在 retention 时清除。
- **影响**：`broker_subscription`、`broker_queue`、`mqtt_conn_fsm`、DLQ 路由、配置。
- **需要 ADR**：是（ADR-015）。

### D5 背压信号与限流点

- **问题**：限生产者还是限消费者？用什么信号？
- **已定案（M0 采纳）：先测后调**。S2-G 先只加"可观测 + 有界"：每个连接 mailbox 长度水位、每分区 pending append 数、每订阅积压条数；超阈值时对**生产者**回 0x97（quota exceeded，MQTT 5）或断开（3.1.1 无背压帧），对**消费者**发 `flow_permit` 降速（暂停 pull round），不新增协议帧。背压阈值全部可配，默认值由 S2-I 基准反推。
- **影响**：`mqtt_conn_fsm`、`broker_queue`、`broker_metrics`。
- **需要 ADR**：否（实现细节），但阈值表进本文。

### D6 可观测性的形态

- **问题**：是否引入 HTTP/Prometheus/OpenTelemetry 依赖？
- **已定案（M0 采纳；见 ADR-016）：S2 不引入外部依赖**。新 `broker_metrics` app：`counters`/`atomics` 热路径自增 + `snapshot/0` 返回 map + 周期性日志摘要（`logger`）；命名与 README §10.2 对齐。HTTP 暴露 / Prometheus exporter 留到 S6（运维化），届时只加导出层，不动计数点。
- **影响**：新 app + 各模块埋点（编译期 `-ifdef` 不做，直接调用，热路径只做 `counters:add`）。
- **需要 ADR**：是（ADR-016，记录"最小集 + 无依赖"的取舍）。

### D7 `os_buffer` 丢失边界的证明方法

- **问题**：`kill -9` 不会丢内核页缓存，无法证明 `os_buffer` 的掉电窗口。
- **选项**：(a) `dm-flakey`/`dm-dust` 在 loop 设备上丢写；(b) 测试替身：把 `file:write` 换成"入队即返回、按开关丢弃未 sync 的部分"；(c) 只做模型论证。
- **已定案（O4）：(a)+(b) 双轨**。`dm-flakey` 场景作为"真实掉电"证据（需要在容器/宿主机有 root，T14 已具备 docker 权限）；测试替身作为 **CI 常驻回归**（不依赖特权），断言"未 `fsync` 的尾部在丢弃后不可读、已 `fsync` 的完整"。两者都是 Gate S2 的必交证据，缺一不算通过。
- **影响**：`tools/docker/` 新场景 + `wal_writer` 注入点（仅测试用，不改变生产路径）。
- **需要 ADR**：否，但报告进 `docs/benchmarks/` 或 `docs/storage-and-recovery.md` §13。

### D8 MQTT 5 的范围与时序

- **问题**：S2 内做 5.0 的全部内容（properties、session expiry、reason codes、shared subscription 语法）还是只做兼容层？
- **已定案（O3）：MQTT 5 整体后移到 S2.5**，不属于 Gate S2。理由：Gate S2 §4.3 四项（三档可靠性证明、不 OOM、基准报告、无单一 GenServer 瓶颈）没有一项要求 MQTT 5；把 5.0 从 Gate 路径摘下能让 Gate S2 提前 1~2 周。S2 期间只保留**不阻塞**的准备：`mqtt_frame` 的 record 预留 properties 槽位不得破坏 3.1.1 路径、CONNECT 的 v5 拒绝路径保持（`20 03 00 87 00`，M16）。
- **S2.5 范围（独立跟踪，不影响 Gate S2）**：CONNECT/PUBLISH/SUBSCRIBE 的 properties 解析与透传（User Properties）、reason codes 全集、session expiry、`$share` 语法（依赖 D2 已完成）；retain/Will 语义复用 S2-C 实现；topic alias / request-response 留到 S5。
- **影响**：`mqtt_frame` 的 record 扩展（`mqtt_frame.hrl` 已有 record，可加 properties 字段而不改冻结消息模型）、`mqtt_conn_fsm`。
- **需要 ADR**：是（ADR-017，只冻结 properties 落位与 3.1.1 兼容边界，不承诺 S2.5 的实现细节；ADR-009 §9.2 的 `proto_meta` 分工必须遵守）。

### D9 会话文件 v2 的迁移策略

- **问题**：`.subs`/`.cursors`/`.inflight` 形状改变后，S1 数据目录能否原地升级？
- **已定案（M0 采纳；并入 ADR-013）：保留 `{vsn,1,...}` 读取分支，写入永远 `vsn 2`；boot 时读到 v1 立即重写为 v2（一次性迁移，失败则**拒绝启动**而不是回退到空会话——S1 的 K5 原则）。提供 `tools/migrate_s1_to_s2.escript` 与演练用例。
- **影响**：`broker_subscription_files`、boot step 2、`docs/storage-and-recovery.md`。
- **需要 ADR**：并入 ADR-013。

---

## 4. 子项目分解

> 估算单位：人日（含测试与文档）。每个子项目开工时另写执行计划；本表只锁边界、依赖与验收。

| ID | 子项目 | 交付物 | 依赖 | 估算 | Gate 对应 | 新 ADR |
|---|---|---|---|---|---|---|
| S2-A | 可观测性最小集 | `broker_metrics` app + 埋点 + `snapshot/0` + 周期日志 | — | 3 | 无单一瓶颈的证据基础 | ADR-016 |
| S2-B | 三档可靠性证明 | `os_buffer` 丢失边界（dm-flakey + 替身）、`fsync` 对照、`replicated_commit` 显式拒绝 | S2-A（计数） | 4 | Gate 1 | — |
| S2-C | QoS2 / Retain / Will | 四步握手（双向）、retain 树、Will 延迟发布 | — | 6 | — | ADR-012 |
| S2-D | 订阅语义与 cursor 重构 | `{Kind,Key,Filter}`、Shared/Failover、Shared 的 worker 原子 claim + 最小连续前缀、`.subs/.cursors` v2 + 迁移 | S2-C（同改 FSM） | 10 | Gate 2 的结构前提 | ADR-013（经 ADR-019 修订） |
| S2-E | 重投递 / DLQ / TTL | `broker_redelivery`、可配 `$dlq/*` + 独立保留期、`expires_at` 生效 | S2-D、S2-F | 6 | Gate 2 | ADR-015 |
| S2-F | retention 与慢消费者守卫 | 段 GC、`max_bytes/max_age`、钉住告警 | S2-D | 5 | Gate 2 | ADR-014 |
| S2-G | 背压与限流 | mailbox/pending/积压水位、permit、生产者限制 | S2-A | 4 | Gate 2 | — |
| S2-I | 基准与 Gate 报告 | `docs/benchmarks/`、四维矩阵、延迟分位、恢复时间 | 全部 | 6 | Gate 3、4 | — |
| — | 评审 / 集成 / 回归缓冲 | 设计评审、ADR 冻结、全量回归 | — | 6 | — | — |
| | | | | **≈ 50** | | |

**Gate S2 之外（独立跟踪，不阻塞 Gate）**：

| ID | 子项目 | 交付物 | 依赖 | 估算 | 新 ADR |
|---|---|---|---|---|---|
| S2.5 | MQTT 5.0 子集 | properties、reason codes、session expiry、`$share` 语法 | S2-C、S2-D | 7 | ADR-017 |

### 4.1 S2-A 可观测性最小集

> 执行计划：[`s2-a-metrics-execution-plan.md`](s2-a-metrics-execution-plan.md)（任务 T-A1~T-A7 + 测试清单 + 验收命令；ADR-016 签认后启动）。

- **状态**：**已实现（2026-11-07）**——`apps/broker_metrics/` 与其 12 例套件已合入；`mqtt_conn_fsm`/`wal_writer`/`broker_storage_wal:recover/2`/`broker_subscription_worker` 完成埋点；全量 `rebar3 ct` 278/278、`dialyzer` 干净、`as t14 release` 含该 app。
- **范围**：连接数（当前/峰值）、认证失败、协议错误/帧解析失败、发布/消费吞吐、端到端延迟、ACK 延迟、重投递次数、队列深度、订阅积压、cursor lag、DLQ 规模、WAL 写入/刷盘延迟、段大小、恢复时间。
- **形状**：`counters` + `atomics`（热路径无锁），`broker_metrics:snapshot/0` 聚合；`broker_metrics:inc/1,2`、`observe/2`（直方图用固定桶，避免依赖）、`gauge/2` 与增补的 `gauge_add/2`（活计数原子读改写）。
- **测试**：埋点不改变行为（同一 CT 套件开/关计数结果一致）；snapshot 键集合被一条 `keys_are_stable` 用例钉住；周期日志不刷屏。
- **风险**：热路径原子操作开销；用 S2-I 的前后基准对照，>2% 则改采样。

### 4.2 S2-B 三档可靠性证明

- **范围**：`os_buffer`（页缓存返回）、`fsync`（已由 T14 R1a 覆盖，补齐"未 fsync 的尾部"对照）、`replicated_commit`（S1/S2 均不可用，必须显式 `{error,{unsupported_sync_mode,_}}` 且 Adapter 早拒）。
- **证据**：`tools/fault/os_buffer_loss.py`（替身，CI 常驻）+ `tools/docker/s2_powerloss.py`（dm-flakey，需特权，出报告）；报告进 `docs/benchmarks/reliability/`。
- **断言**：丢弃未 sync 尾部后，已 ACK（PUBACK 过）的消息可读；未 ACK 的尾部不可读但不产生"已提交"假象；恢复后 `high` 回到真实末尾。
- **状态（2026-11-07）**：**替身档已实现并实测通过**（`tail` / `mqtt` / `replicated` 三场景，K=16 / M=64），证据见 [`docs/benchmarks/reliability/s2-b-osbuffer-loss.md`](../benchmarks/reliability/s2-b-osbuffer-loss.md) 与同目录 `report-osbuffer-*.json`。共享 harness 抽取为 `tools/docker/mq_harness.py`，S1 全量矩阵复跑作回归闸（15/16 PASS，唯一失败 `r3/default` 已用未改动的原始脚本复现为既有 flake）。dm-flakey 实机档驱动已交付（`--skip-prereq` 可核对步骤），**实机报告待特权执行**。

### 4.3 S2-C QoS2 / Retain / Will ✅ 已实现（2026-11-07）

> 冻结决策见 [ADR-018](../adr/018-retain-will-and-qos2-amendments.md)；执行计划与用例清单见 [`s2-c-protocol-completion-execution-plan.md`](s2-c-protocol-completion-execution-plan.md)。以下只写**已实现事实**，实现期与计划的偏差在计划文档里逐条记录。

- **QoS2（D1 已定案 (b)）**：`mqtt_frame` 新增 PUBREC(5)/PUBREL(6)/PUBCOMP(7) 双向编解码（固定头 flags：PUBREL `0b0010`，其余 `0b0000`）；PUBLISH 与 Will 的 QoS 放宽到 0..2。FSM 出站用 `pending_qos` 记录级别、`released` 标记"已见 PUBREC 待 PUBCOMP"——**cursor 只在 PUBCOMP 之后推进**；入站用 `received` 去重（Method A：PUBLISH 时即投递）。`.inflight` 形状升至 **v2**：`#{vsn:=2, next_pid, pending, qos, released, received}`，v1 读入按"全 QoS1"解释。重连重发按 MQTT §4.3：未 `PUBREC` ⇒ 同 packet id `DUP=1` 原级别重发；已 `PUBREC` ⇒ 只发 `PUBREL`。三种未知握手帧全部幂等（回 PUBCOMP / 回 PUBREL / 忽略），不断连不推进 cursor。承诺边界写死：**线协议四步 + 跨重连不重复；跨崩溃允许重复（至少一次 + DUP）**，不做 exactly-once。
- **Retain（O6 已签认）**：`broker_retain`（新 app，避免订阅层再长）持有 `Topic -> #broker_message{}` 树，持久化到 `retain/retain.v1`（整树 + `vsn`，损坏即启动失败）；订阅时按 filter 匹配补发（QoS = `min(订阅, 存储)`，retain=1，字典序），转发给在线订阅者的帧 retain=0，retain=1 的 PUBLISH 更新树并**零长度 payload 清除**。**QoS1/2 的 retain=1 PUBLISH 先 fsync 落盘再回 ack**，QoS0 走延迟批量。三个上限（条数/字节数/单条大小）**拒绝写入但正常转发**，计数 `retain_overflow`。
- **Will（O5 已签认）**：CONNECT 保存 will（topic/payload/qos/retain），**仅内存不持久化**——broker 重启不补发（与 mosquitto 默认一致）。TCP 异常关闭 / keepalive 超时 / 协议错误 / 内部失败 / takeover 都发布；**正常 DISCONNECT 与 broker shutdown 不发布**。发布走普通 PUBLISH 路径（QoS>0 先 append fsync），retain 与 will 的组合按规范更新树。
- **测试**：帧级 27 例、连接级 50 例（其中 QoS2/Retain/Will 20 例）、`broker_retain_SUITE` 17 例；T14 新增 **R8**（收 PUBREC 后 `kill -9`，重启幂等回 PUBCOMP，默认档/零回退档都 PASS）；T15 新增 `paho-qos2`/`paho-retain`/`paho-will`/`paho-will-skipped`/`mosq-qos2`，全套 **10/10 PASS**。

### 4.4 S2-D 订阅语义与 cursor 重构

- **状态**：**已实现（S2-D，2026-11-08）**。决策见 [ADR-013](../adr/013-subscription-kinds-and-cursor-ownership.md) 与修订它的 **[ADR-019](../adr/019-shared-assignment-ack-ownership-and-session-layout.md)**；任务与用例见 [`s2-d-subscription-kinds-execution-plan.md`](s2-d-subscription-kinds-execution-plan.md)（§8 交付记录）。下面是 ADR-019 冻结的实现契约。

- **数据模型**：`SubRef = {Kind, Key, Filter}`；`Kind ∈ {exclusive, shared, failover}`；`Key = ClientId | Group`。实体表 key 仍是 SubRef；不建 `CONSUMERS` 表——成员顺序就是 `.subs` body 里的有序 `consumers`，failover 的 active = 列表中第一个有 live attachment 的成员。
- **Shared 的分配**：**worker 原子 `claim/4`**。cursor 行是 `#{kind, low, acked, assigned}`；conn 每轮向 worker 领一整页 offset（`Max = pull_page_limit`），worker 在单个 gen_server 内串行分配并记入 `assigned => Consumer`。pull 只读领到的那些 offset；`low` 只由 ack 的连续前缀推进，分配不移动它。保留「cursor 是唯一读位置」（J6）。代价是每页多一次 `gen_server` call（不是每条一次）。
- **claim 的读验证与退回（ADR-019 §19.5，实现期新增）**：`broker_queue:watermarks/1` 的 `High` 是**预留**指针，不是「已可读」边界（WAL 在 group commit 前就前移；memory 后端先 `update_counter` 再 `insert`）。因此 claim 可能领到「已预留、尚不可读」的 offset：`read_from` 返回空页，而它已记在 `assigned` 里，再没有出口。conn 读回后求差集，用 `release/4` 按 consumer 校验退回（ack 胜出），本轮该 ref 记 `stalled`，下一个 append wake / poll tick 再领。计数 `shared_claim_rollbacks`。`< High` 与读验证叠加才完整：前者保证不产生幻影 offset，后者保证不产生悬挂租约。
- **Shared 的 committed**：`low - 1`；一条慢 consumer 只钉住它没 ack 的那几条，不钉住整组。
- **ack 归属**：`ack/4` 带 consumer，校验 `assigned[Offset] =:= Consumer`；不符者 `ok` + `shared_ack_unowned`，不改状态、不断连。consumer 死亡时 `do_detach` 把它名下未 ack 的 offset 清回未分配，下一轮 claim 自然重投。**S2-D 不加重试定时器**：重投只来自成员死亡与连接重连，重试上限/退避/DLQ 是 ADR-015 的 S2-E。
- **上界**：`shared_state_max`（默认 4096）限制 `acked ∪ assigned`；超限 `claim` 返回 overflow，conn 本轮不投递并记 `shared_window_full`——有界拒绝，不驱逐。
- **dispatch 归属**：位置型 wake 发给 Shared 组内**所有** attached 成员（wake 只是提示）；非位置型（QoS0，推送即投递）只发给 `order` 最小且 attached 的一个。
- **会话文件**：`.subs`/`.cursors` 按 SubRef（`"<kind>.<b64url(Key)>.<b64url(Filter)>.subs"`），内容为 `vsn 2`；**`.inflight` 仍按 client**（它是 conn FSM 的 blob，一条 pending 可同属多个 SubRef）。`assigned` 不落盘。boot 从 `.subs` 内容重建 `ClientId→SubRef` 索引；v1→v2 迁移在 boot step 2 内**先写后删**、幂等，失败拒绝启动。
- **不在 S2-D**：`$share` 线上语法（S2.5）、消费权重、位图的位级表示（S2-I 压测后再定）。
- **测试**：三种类型的投递归属、claim 并发不相交、`low` 在乱序 ack/空洞/成员退出三场景的正确性、Shared 组内不重复、failover 接管、`.subs` v2 与 v1 迁移演练（含中途失败）、Router 回填（boot step 2+3）、T14 R9。**结果（2026-11-08）**：`broker_subscription_SUITE` 44/44、`mqtt_conn_SUITE` 55/55、全量 `rebar3 ct` 349/349、T14 R9 双档 PASS、`dialyzer` 干净；明细见执行计划 §8。

### 4.5 S2-E 重投递 / DLQ / TTL（✅ 已实现 2026-11-08）

- **重投递**：状态**不建独立表**，放在 cursor 行的 `retries` 字段（`#{Offset => #{attempts, first_at, next_at, reason}}`，随 `.cursors` 窗口落盘）；定时器与决策都在 `broker_subscription_worker`（单进程），连接只负责检测 ack 超时并上报。退避 `base * 2^(n-1)`、上限 `retry_max_delay_ms`；`max_attempts` 次失败后转 DLQ。实现见 [S2-E 执行计划](s2-e-redelivery-dlq-ttl-execution-plan.md) §2 / ADR-015 §15.6。
- **DLQ**：普通 destination，命名模板 `broker_subscription.dlq_name_pattern`（默认 `$dlq/{tenant}/{original}`，变量 `{tenant}{original}{qos}{kind}`，含 `+`/`#`/未知变量 ⇒ **启动失败**）；副本固定 `fsync`、`expires_at=undefined`、新 `message_id`，原值进 `x-original-destination`/`x-original-message-id`/`x-failure-reason`/`x-attempts`/`x-tenant`。写失败保留 cursor（`dlq_append_failed`）。**独立保留期**（O2）落在 S2-F 的段级 GC，S2-E 只提供 `dlq_size` 计数。
- **TTL**：`expires_at` 在读者侧（`mqtt_conn_fsm`）过滤：不投递但推进 cursor 与 committed；不改 `broker_queue:read_from/3` 的连续记录契约（ADR-011 冻结）。
- **测试**：退避时序（假时钟）、超限转 DLQ、DLQ 可订阅、坏模板拒绝启动、DLQ 写失败保 cursor、重启保进度、ack 超时 `DUP=1` 重投、TTL 到期不投递且 committed 前进（360/360 中的 11 例，映射见执行计划 §8.2）。

### 4.6 S2-F retention 与慢消费者守卫

- **段 GC**：`wal_gc`（`broker_storage_wal_sup` 下新 worker 或挂到每分区 writer？建议独立进程按分区扫描，避免阻塞写路径）；删除条件见 D3。
- **配置**：`retention.max_bytes`、`retention.max_age_ms`、`retention.check_interval_ms`；`truncate_before` 语义不变。
- **守卫**：慢消费者钉住时 `broker_metrics` 计数 + 告警日志，不删；提供 `broker_queue:retention_status/0`。
- **测试**：段删除边界（不删有未消费数据的段）、删除后 `read_from` 从 `truncate_before` 起、恢复不误读已删段、慢消费者场景不 OOM。

### 4.7 S2-G 背压与限流

- **信号**：`mailbox_len`、`pending_append`、`backlog`（per subscription）、`inflight` 占用；阈值可配。
- **动作**：超阈值时暂停该 conn 的 pull round（consumer permit）、拒绝新 PUBLISH（3.1.1 断开 / 5.0 回 0x97）、对热点分区限制单连接吞吐。
- **测试**：慢消费者（读 1 条/秒）不导致 broker RSS 增长；热点分区下其他分区仍可推进；连接风暴不击穿 accept 池。

### 4.8 S2-H → S2.5：MQTT 5.0 子集（Gate S2 之外）

> O3 定案后本节移出 Gate 路径，**不进入 M1~M4**；S2 期间只保证 3.1.1 路径不被 properties 预留破坏。

- **范围**：CONNECT properties（session expiry、receive maximum、maximum packet size、user properties）、PUBLISH properties（payload format、message expiry、user properties）、SUBSCRIBE properties（subscription identifier 可选）、reason codes（0x00/0x10/0x80/0x87/0x97/0x9C…）、`clean_start` 与 session expiry、`$share/{group}/{filter}`。
- **不做**：request/response、topic alias（留 S5）。
- **测试**：与 paho-mqtt 5 客户端的真实交互（T15 升级为双版本）、properties 往返、非法 properties 断开。

### 4.9 S2-I 基准与 Gate 报告

- **工具**：`tools/bench/`（Erlang 驱动 + paho/mosquitto 对照），复用 S0.3 的 fsync/batch 口径。
- **矩阵**：消息大小 {64B, 1KB, 64KB, 1MB} × batch {1,32,256} × 分区 {1,4,16} × 订阅数 {1,10,100}；吞吐、p50/p99/p999、恢复时间。
- **报告**：`docs/benchmarks/YYYY-MM-DD-s2-baseline.md`，含机器规格、commit、复跑命令。
- **Gate 4 检查**：四条 §7.2 约束逐条给"反证测试 + 数字"。

---

## 5. 里程碑与顺序

| 里程碑 | 内容 | 退出条件 |
|---|---|---|
| M0 决策冻结 | D1~D9 结案；ADR-012~017 起草并评审 | 每份 ADR 状态=已采纳；本文升 v0.2+（**已完成**：2026-11-07 六份 ADR 全部签认，本文 v0.3） |
| M1 观测与证明 | S2-A + S2-B | `os_buffer` 双轨报告可复跑（CI 替身 + dm-flakey）；metrics snapshot 稳定 |
| M2 协议补齐 | S2-C（QoS2 / Retain / Will） | ✅ **已完成（2026-11-07）**：帧级 27 例 + 连接级 20 例 + retain 17 例；T14 R8 默认档/零回退档双档 PASS；T15 新增 5 例、全套 10/10 PASS。**MQTT 5 已移出本里程碑到 S2.5** |
| M3 结构改造 | S2-D → S2-E → S2-F | ✅ **S2-D 已完成（2026-11-08）**：三种订阅类型（worker 原子 claim + 最小连续前缀 + §19.5 读验证）、`.subs` v1→v2 迁移、T14 R9 双档全绿；S2-E（DLQ/TTL）与 S2-F（retention）未开始 |
| M4 收口 | S2-G + S2-I + Gate S2 报告 | Gate 四项证据齐备，全量回归零 FAILED |
| （S2.5） | MQTT 5 子集 | 独立跟踪，不阻塞 Gate S2 |

**建议的首批顺序**：`S2-A → S2-B → S2-C`。理由：A/B 先建立"可观测 + 可证明"的底座（Gate 1 与 Gate 4 都要它）；C 独立于订阅结构改造，能尽早暴露帧/FSM 的设计问题，为 D 的状态机改造探路。D 是最大结构风险，放在 C 之后、由 C 的 FSM 经验支撑。

---

## 6. Gate S2 对齐

| Gate 项 | 证据 | 复跑入口 |
|---|---|---|
| 三档可靠性逐档证明 | `os_buffer` 丢失边界（替身 + dm-flakey 双轨）、`fsync` 对照、`replicated_commit` 显式拒绝 | `tools/fault/`、`tools/docker/s2_powerloss.py` |
| 慢消费者 / 热点分区不 OOM | 三组压测的 RSS / mailbox / 积压曲线与阈值 | `tools/bench/`（S2-I 场景脚本） |
| 基准报告 | 四维矩阵 + 延迟分位 + 恢复时间 | `docs/benchmarks/` |
| 无单一 GenServer 瓶颈 | §7.2 四条逐条反证：路由在调用者、append 只进分区 writer、读路径不进 gen_server、订阅 dispatch 不串行 | `rebar3 ct` 的反证用例 + 基准数字 |

---

## 7. 风险与开放问题

| # | 风险 / 开放问题 | 影响 | 缓解 |
|---|---|---|---|
| R1 | D2 的 cursor 归属改造触碰 R3/R7 契约与 boot 重建 | 高（数据一致性） | ADR-013 冻结形状；先做 v1→v2 迁移演练；R3/R7 用例扩展为三类型 |
| R2 | QoS2 的 release 状态与 `.inflight` 快照竞态 | 中（重复/丢失） | 沿用 K10 pid 守卫与 vsn 迁移；T14 R8 覆盖重启 |
| R3 | `os_buffer` 证明依赖特权（dm-flakey） | 中（证据可复跑性） | 替身进 CI；真实掉电报告单独出，环境与命令写死 |
| R4 | retention 物理删除与恢复的交界 | 中（误删不可恢复） | 只删满足 14.1 两条件的段（`<= committed` 且 `< min(live cursor)`）；先 dry-run + `retention_status/0`；恢复矩阵加"段已删"场景 |
| R5 | metrics 热路径开销 | 低-中（吞吐） | 固定桶直方图 + atomics；>2% 回退到采样 |
| R6 | MQTT5 properties 落位（`proto_meta` vs `headers`） | 中（跨协议污染） | ADR-017 按 ADR-009 §9.2 分工定案；仅在 Adapter 内解析；且 5.0 已移出 Gate 路径 |
| R7 | S2 范围过大导致 Gate 拖延 | 中（进度） | 已消除最大项：MQTT 5 移出 Gate（O3），Gate 范围 ≈50 人日；按里程碑分批验收 |
| R8 | Shared 的 `acked ∪ assigned` 内存随积压线性增长 | 中（内存 / 恢复时间） | `shared_state_max = 4096` 按 `{SubRef, Partition}` 有界拒绝（ADR-019 §19.1）；S2-I 压测把积压上限与 RSS 曲线写进 Gate 2 证据 |

**已定案的产品决策（原 O1~O4）**：

- **O1 → 已拍板**：Shared 的 ack = **per-message ack + 最小连续前缀**（Pulsar 风格），见 D2 / §4.4。
- **O2 → 已拍板**：DLQ destination **按租户可配命名**（默认 `$dlq/{tenant}/{original}`）+ **独立保留期**，见 D4 / §4.5。
- **O3 → 已拍板**：MQTT 5 **后移到 S2.5**，不阻塞 Gate S2，见 D8 / §4.8 / §5。
- **O4 → 已拍板**：`os_buffer` 采 **双轨**（CI 替身常驻 + `dm-flakey` 实机报告），见 D7 / §4.2。

---

## 8. 文档与证据产出

- 新 ADR：012（QoS2）、013（订阅/cursor）、014（retention）、015（重投递/DLQ/TTL）、016（可观测性）、017（MQTT5 properties）。**均已于 2026-11-07 起草并经用户签认，状态=已采纳。**
- `docs/architecture.md`：新增 `broker_metrics`、`broker_retain`、`broker_redelivery`（或模块）的进程边界与数据流；§9 演进表逐项落地。
- `docs/storage-and-recovery.md`：段 GC、DLQ 存储、TTL 过滤、会话文件 v2 与迁移、R8/R9 矩阵。
- `docs/protocol-compatibility.md`：MQTT 行升级（QoS2/Retain/Will/5.0 子集），T15 扩为双版本客户端。
- `docs/benchmarks/`：S2 基准与可靠性报告。
- `progress.md`：阶段状态表拆分 S2/S3，任务表按本文 §4 展开。

---

## 9. 本文不覆盖

- S3 的 CloudEvents codec / 五档策略 / Binding Mapper（仅保证 `event` 槽与计数点）。
- S4 的复制、分区迁移、控制面。
- S6 的 TLS/ACL/运维导出层。

以上分别由各自的阶段设计文档承接。
