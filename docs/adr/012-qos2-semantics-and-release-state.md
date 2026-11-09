# ADR-012：MQTT QoS 2 的语义边界与 release 状态持久化

- **状态**：**已采纳（S2 M0 冻结，2026-11-07）**——用户签认；后续修改须走 ADR 修订流程
- **修订**：§12.2 的 `released` 形状与 §12.4 的未知握手帧姿态由 **ADR-018 §18.3（R1~R4）** 修订；本文其余条款继续有效
- **日期**：2026-11-07
- **关联**：README §6.3/§11.1；roadmap §4.1（MQTT 补齐）；ADR-007（显式可靠性）、ADR-009（`#broker_message{}`）、ADR-011（`broker_storage` 冻结行为）；设计文档 `docs/design/s2-single-node-kernel.md` D1 / §4.3

## 背景（Context）

S1 只实现 QoS 0/1。`mqtt_frame` 明确不携带 PUBREC(5)/PUBREL(6)/PUBCOMP(7)，`mqtt_conn_fsm` 对 QoS 2 的 PUBLISH 直接断连（`mqtt_qos2_unsupported_or_above_max`）。S2 必须补齐 QoS 2，但"QoS 2 = 恰好一次"在单机、可崩溃、无复制的 broker 上是一个**必须限定承诺边界**的说法，否则会造成两类错误：

1. **对用户过度承诺**：把 QoS 2 说成"跨崩溃不重复"。单机 WAL + fsync 只能在"已 fsync 的消息不丢"这一层给承诺；崩溃发生在"消息已落盘、消费者已接收、PUBREL 未落盘"的窗口时，重启后按协议重发必然产生重复。
2. **状态被塞进错误的所有者**：PUBREL release 状态若存在连接进程里，连接一死就丢，重连后无法判断"这条 QoS 2 消息是否已经 release 过"，只能无条件重发——这等于放弃 QoS 2 相对 QoS 1 的全部收益。

因此本 ADR 要冻结三件事：**承诺边界**、**release 状态的持久化位置与形状**、**与 `.inflight` v1 的迁移关系**。

## 决策（Decision）

### 12.1 承诺边界：线协议四步 + 跨重连不重复；跨崩溃至少一次

UnifiedMQ 的 QoS 2 承诺**分两段**，两段都必须写进文档与测试：

| 场景 | 承诺 | 依据 |
|---|---|---|
| 连接存活期间 | 完整四步握手（PUBLISH → PUBREC → PUBREL → PUBCOMP），接收侧不重复投递给应用 | MQTT 3.1.1 §4.3.3 |
| 同一会话重连（连接断、Broker 未重启） | **不重复投递**：已 PUBREC 的消息在重连后用 DUP 重发 PUBLISH 或继续 PUBREL，接收侧按 packet id 去重 | release 状态持久化（12.2） |
| Broker 崩溃 / 断电重启 | **至少一次**：允许重复，接收侧据 PUBLISH 的 DUP 与 packet id 仍可去重，但 Broker 不保证 | 单机无复制、无跨节点事务 |
| 任何情况 | **不做跨崩溃 exactly-once** | ADR-007 的 `replicated_commit` 才谈跨节点；S4 之前它是"不可用"档 |

这条边界与 ADR-007 的口径一致：**可靠性等级描述"消息是否已提交到持久介质"，不描述"应用是否只处理一次"**。QoS 2 在单机上的增量价值是"连接级不重复 + 协议兼容"，不是"分布式恰好一次"。

### 12.2 release 状态归属 `broker_subscription`，形状进 `.inflight` v2

**归属**：release 状态属于**会话**，不属于连接进程。连接进程（`mqtt_conn_fsm`）只执行协议时序，状态真相与 S1 的 `pending`/`next_pid` 一样由 `broker_subscription` 持有，按 `ClientId` 落 `.inflight` 文件。

**形状**：`.inflight` 的 blob 从 S1 的

```erlang
#{next_pid := pos_integer(), pending := [{Pid, {SubRef, {Partition, Offset}}}]}
```

升为

```erlang
#{vsn      := 2,
  next_pid := pos_integer(),
  pending  := [{Pid, {SubRef, {Partition, Offset}}}],  % 出站 QoS1 未 PUBACK
  released := #{Pid => Offset},   % 出站 QoS2：已发 PUBREL、等 PUBCOMP
  received := #{Pid => true}}     % 入站 QoS2：已发 PUBREC、等 PUBREL
```

- **`released`（出站方向，Broker 作为 sender）**：已收到对端 PUBREC 并发出 PUBREL、等待 PUBCOMP 的 packet id。跨重连只需重发 PUBREL，**不重发 PUBLISH**。
- **`received`（入站方向，Broker 作为 receiver）**：已发出 PUBREC、等待 PUBREL 的 packet id。跨重连若对端用 `dup=1` 重发同一 PUBLISH，直接重发 PUBREC，**不重复投递**给订阅者。
- 两者都以 packet id 为键，条目数都受 12.4 的上限约束。

**为什么写进 `.inflight` 而不是新文件**：S1 已经冻结"三层会话持久化"（`.subs` 强一致 / `.cursors` 批量 / `.inflight` 批量）与"先刷文件再推进 cursor"的顺序。新增一个 `.qos2` 文件会引入**第四份需要与前三份对齐的真相**，且崩溃恢复要处理四文件不一致。放进 `.inflight` 的 v2 blob 保持"一份会话快照 = 一次原子 rename"的性质。

### 12.3 帧层与状态机的最小改动

- `mqtt_frame` 新增三个帧类型与编解码：PUBREC(5)、PUBREL(6)、PUBCOMP(7)。3.1.1 固定头 flags 必须按规范写死（PUBREL 的 flags 为 `0b0010`，其余为 `0b0000`；非法 flags 走既有"协议错误断连"姿态）。
- `mqtt_conn_fsm` 的出站 `pending` 增加 stage：`awaiting_puback`（QoS 1）/ `awaiting_pubrec`（QoS 2 第一步）/ `awaiting_pubcomp`（QoS 2 第二步）。stage 是连接内瞬态，**不持久化**；持久化的是 12.2 的 blob。
- 重连重发按 MQTT §4.3：未 PUBREC 的 PUBLISH 用 `dup=1` 重发；已 PUBREC 未 PUBCOMP 的直接重发 PUBREL，不重发 PUBLISH。
- QoS 2 的 `#broker_message.reliability` 映射与 QoS 1 相同（`fsync`），**不新增可靠性等级**；`proto_meta` 继续记录 `#{qos => 2, dup => _, retain => _, packet_id => _}`。

### 12.4 上下界与拒绝姿态

- `released` 与 `received` 的条目数合计不得超过 `broker_proto_mqtt.max_qos2_state_per_session`（默认 `100`），超过时对该连接断连并记 `mqtt_qos2_state_overflow`，**不静默丢弃状态**。
- `next_pid` 回绕与 S1 一致：耗尽 `1..65535` 时拒绝新投递并断连，不重用未完成的 packet id。
- 收到未知 packet id 的 PUBREC/PUBREL/PUBCOMP 时记日志并按协议错误断开（3.1.1 无 reason code）。

### 12.5 `.inflight` v1 → v2 迁移

- **读**：`vsn=1` 的 blob 视为 `pending` 原样、`released = #{}`、`received = #{}`、`next_pid` 不变——语义等价于"没有 QoS 2 状态"，安全。
- **写**：任何写回都升级为 `vsn=2`，一次性完成，不保留 v2→v1 降级路径（与 D9 一致：升级单向，失败拒绝启动）。
- S1 数据目录可直接被 S2 打开；不需要离线工具（`tools/migrate_s1_to_s2.escript` 只用于演练与审计，不进入启动路径）。

**【冻结后补充，2026-11-09】** 该 escript 最终**未交付**；实际交付物是 boot 路径上的 `broker_subscription_files:migrate_v1/0`（写 v2 成功后才删 v1）与三个 CT 演练用例（`migration_rewrites_v1_subs_and_cursors`、`migration_is_idempotent_after_a_partial_write`、`migration_refuses_to_boot_on_a_corrupt_v1_file`）。迁移本来就不依赖离线工具，因此这一缺口不影响「S1 数据目录可直接被 S2 打开」的承诺。

## 后果（Consequences）

- **+** QoS 2 的重复窗口被明确限定在"Broker 崩溃"这一种情况，测试与文档可机械核对。
- **+** release 状态与会话同生共死，重连不重复，符合 MQTT §4.3.3 对 session state 的定义。
- **+** 复用 `.inflight` 的原子替换与批量刷盘路径，不新增第四份会话真相。
- **-** 单机仍然做不到"应用只处理一次"；用户若需要，必须在应用侧按 `source + id` 去重（ADR-005）。
- **-** `released`/`received` 增加会话 blob 大小与上限管理；上限超界会断连，属于**故意**的失败模式。
- **-** 出站 QoS 2 的两阶段使 `pending` 生命周期变长，慢消费者场景下 `.inflight` 条目更多，需要 S2-G 的 permit 配合。
- **-** QoS 2 的完整跨崩溃 exactly-once 仍被推迟到 S4（复制提交）之后；本 ADR 不承诺。

## 验证（Validation）

- **帧级**：PUBREC/PUBREL/PUBCOMP 的字节黄金用例（含 PUBREL flags `0b0010`、非法 flags 断连），并入 `mqtt_frame_SUITE`。
- **状态机**：`mqtt_conn_SUITE` 新增用例——正常四步、重连重发（存活的未 PUBREC / 已 PUBREC 未 PUBCOMP 两种）、入站 DUP 重发不重复投递、未知 packet id 断连、QoS2 状态超上限断连。
- **持久化**：`broker_subscription_SUITE` 断言 `vsn=1` blob 读成 `released=#{}` 且 `received=#{}`、写回为 `vsn=2`；`session` 快照包含两者。
- **崩溃恢复**：T14 新增 R8——在"已 PUBREC、未 PUBCOMP"状态 kill -9，重启后按 §4.3 重发 PUBREL，接收侧可去重；断言**允许重复但不丢消息**。
- **真实客户端**：T15 增加 paho-mqtt 的 QoS 2 端到端用例（发布/订阅双向）。
- **文档**：`docs/protocol-compatibility.md` 的 MQTT 行与 `docs/storage-and-recovery.md` 的 `.inflight` 格式同步更新。
- **Gate S2**：本条不单独构成 Gate 项，但其证据进入 Gate 1（`fsync` 档）与 M2 退出条件。
