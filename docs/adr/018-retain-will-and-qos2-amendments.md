# ADR-018：Retain 与 Will 的语义/持久化边界，及 QoS 2 握手幂等修订

- **状态**：**已采纳（S2 决策冻结，2026-11-07）**——用户签认 O5 / O6 / R1 / R2；后续修改须走 ADR 修订流程
- **日期**：2026-11-07
- **关联**：README §5.2/§6.3；roadmap §4.1（MQTT 补齐）；ADR-007（显式可靠性）、ADR-009（`#broker_message{}`）、ADR-011（`broker_storage` 冻结行为）、**ADR-012（本 ADR §18.3 修订其 §12.2/§12.4）**；设计文档 `docs/design/s2-single-node-kernel.md` §4.3
- **修订**：`amends` ADR-012 §12.2（`.inflight` v2 形状）与 §12.4（未知握手帧姿态）

## 背景（Context）

S2-C 要补齐 QoS 2 / Retain / Will。ADR-012 已冻结 QoS 2 的承诺边界与 release 状态归属，但还有三块语义没有 ADR：

1. **Will 跨重启**：MQTT 3.1.1 §3.1.2.5 说「网络连接异常关闭时必须发布 Will，除非收到 DISCONNECT」。Broker 自己崩溃也等于异常关闭，但绝大多数实现（含 mosquitto 默认）不持久化 Will，重启后丢失。设计 §4.3 只写了「异常断连时由 session 所有者发布」。
2. **Retain 落盘时机**：ADR-007 规定 PUBACK 表示消息已 fsync。若 retain 树只在内存/延迟批量写，则「已 PUBACK 的 retained 消息」在崩溃后会消失，新订阅者拿不到它。
3. **未知握手帧姿态**：ADR-012 §12.4 规定收到未知 packet id 的 PUBREC/PUBREL/PUBCOMP 一律断连。但 `.inflight` 是批量层（`inflight_flush_interval_ms`，默认 100ms），崩溃/进程死亡会丢掉最近的握手状态；此时对端按协议重发握手帧，断连会让客户端陷入**永久重连循环**——一个本可恢复的重复被升级成不可用。

前两项由用户拍板（O5 / O6），第三项按同一崩溃回退论证扩展为「三种握手帧全幂等」（R1 / R2）。

## 决策（Decision）

### 18.1 Will：仅内存，不跨 broker 重启（O5 定案）

**Will 是连接状态的附属物，不是会话真相。** CONNECT 解析出的 `#{topic, payload, qos, retain}` 只保存在 `mqtt_conn_fsm` 的进程状态里；连接进程死亡即消失，broker 重启**不补发**。

发布条件（`terminate/3` 中判定，异常路径才发布）：

| 终止原因 | 发布 Will |
|---|---|
| 正常 `DISCONNECT`（§3.14） | **否**（收到 DISCONNECT 即丢弃 Will） |
| TCP 关闭 / `tcp_error` / 半开连接超时 | 是 |
| keepalive 超时（1.5×） | 是 |
| 协议错误 / 解码失败 / 超限 PUBLISH | 是 |
| `{append_failed, _}` 等内部错误 | 是 |
| 会话接管（同 ClientID 新连接） | 是（§3.1.4 + §3.1.2.5 字面：旧连接未经 DISCONNECT 被关闭） |
| broker 自身崩溃 / 重启 | 不适用（Will 未落盘，进程已死） |

- **Will QoS/Retain**：`will_qos ∈ 0..2`（S2-C 起含 2，帧层同步放宽），`will_retain ∈ 0|1`。发布走与普通 PUBLISH 相同的路径：`broker_router:route/3` → `broker_queue:append/3`（qos>0 用 `fsync`，qos=0 不落 WAL）→ `broker_subscription:dispatch/2`；`will_retain=1` 时同时更新 retain 树（§18.2）。
- **没有 ack**：Will 的发布者是正在死亡的连接，没有可回 PUBACK 的对象；`reliability` 仍按 QoS 映射（qos>0 → `fsync`），失败只记日志。
- **为什么不做持久化**：持久化 Will 需要第四份与 `.subs`/`.cursors`/`.inflight` 对齐的会话真相，并引入「重启即 Will 风暴」的失败模式；mosquitto 默认同样不持久化。MQTT 5 的 Will Delay（S2.5）会重新讨论这个边界，届时用 ADR 修订处理。
- **与 MQTT 5 的关系**：S2.5 引入 `session expiry` 后，「Will 在所有会话结束后才发布」需要新的状态机；本 ADR 只冻结 3.1.1 行为。

### 18.2 Retain：独立 app、独立文件、QoS>0 同步落盘（O6 定案）

**归属**：新增 `broker_retain` app（设计 §4.3 已定），持有 `Topic -> #{msg := #broker_message{}, qos := 0..2}`。它**不**进入 `broker_subscription`（订阅层已 628 行），也不进入 `broker_storage`（WAL 按分区、按 offset 组织，retained 是「每 topic 最后一条」的覆盖语义，形状不同）。

**更新规则**（§3.3.1.3）：

- `retain=1` 且 payload 非空：覆盖该 topic 的条目；现有订阅者照常收到该消息，但转发帧 **retain=0**（规范要求）。
- `retain=1` 且 payload 为零长度：**删除**该 topic 的条目；零长度消息本身仍按普通消息投递给现有订阅者（规范要求），但不进入 retain 树。
- `retain=0`：不触碰 retain 树。

**落盘边界**：

| 入站 QoS | retain 树更新 | 落盘时机 | 承诺 |
|---|---|---|---|
| 0 | 内存立即 | `retain_flush_interval_ms`（默认 100ms）批量 | 无（QoS0 本无持久承诺） |
| 1 / 2 | 内存立即 | **同步 fsync + rename 完成后才回 PUBACK/PUBREC** | PUBACK 过 ⇒ retained 状态可恢复（沿用 ADR-007） |

QoS1/2 的顺序固定为：`broker_queue:append/3`（fsync）→ `broker_retain:store/3`（fsync）→ PUBACK/PUBREC → dispatch。retain 落盘失败时**不回 ack** 并断开连接（与 append 失败同一姿态）；WAL 里那条消息仍在，订阅者会收到它——重复可接受，丢 retained 不可接受。

**文件**：`{data_dir}/retain/retain.v1`，内容 `term_to_binary({vsn, 1, [{Topic, #broker_message{}}]})`，写入用与 `.subs` 相同的 `temp + fsync + rename` 原子序列。启动时整体载入；损坏/未知 vsn **拒绝启动**（错误含路径，与 `.subs` 同一姿态）。

**订阅时补发**：

- SUBACK 之后，按 filter 匹配当前 retain 树，逐 topic（字典序，确定性）发送 `retain=1` 的 PUBLISH；
- 投递 QoS = `min(retained qos, granted qos)`；客户端请求 QoS0 时以 QoS0 补发（该帧无 packet id、不需 ack）；
- 补发走连接自己的 `pending`/`next_pid` 空间，与普通投递共享 `max_inflight` 窗口；窗口满时余下的 retained 由后续 pull round 之外的一次补发队列继续（实现上：补发列表挂在连接状态里，按窗口排空）。
- 一个 filter 命中多个 retained topic 时逐条发送，**不合并**（不同 topic 是不同消息）。

**上限（故意的失败模式）**：`max_retained`（默认 10000 条）与 `max_retained_bytes`（默认 64 MiB）。超限时**不驱逐**已有条目、不静默截断；新 retained 更新被拒绝——记录 `retain_overflow` 计数与 warning，消息仍按普通 publish 投递给现有订阅者，但 retain 树不更新。理由：驱逐会让「最后一条 retained」变成不确定的随机值；拒绝是可观测的、有界的。

**序列化代价**：每次 retained 更新都重写整个快照文件，并且经过 `broker_retain` 单进程。retained 发布通常是低频状态广播（`device/x/status`），S2 接受这个代价；S2-I 基准会量 retained 更新延迟，若成为瓶颈，S3 改为 append-only + 压缩（已在设计 §7 风险表登记）。

### 18.3 QoS 2 修订（amends ADR-012 §12.2 / §12.4）

#### R1：未知 PUBREL → 回 PUBCOMP（用户签认）

收到 packet id 不在 `received` 中的 PUBREL：记 `info` 日志，**回 PUBCOMP**，不改任何状态。依据 MQTT 3.1.1 §3.6.1（PUBCOMP 是 PUBREL 的幂等应答）：崩溃丢掉 `received` 后，客户端重连重发 PUBREL 仍能完成握手，不会陷入重连循环。

#### R2：三种握手帧全幂等（用户签认，同 R1 论证）

| 收到 | 状态里没有该 packet id | 动作 |
|---|---|---|
| PUBREL | `received` 无此 id | 回 PUBCOMP，`info` 日志，不推进 cursor |
| PUBREC | `pending` 无此 id | 回 PUBREL（推进对端状态机），`info` 日志，不推进 cursor |
| PUBCOMP | `pending` 无此 id | 忽略，`info` 日志 |

三者都**不再断连**。未知 id 永远不会推进 cursor（没有 `pending` 条目就没有 `{Refs, Pos}`），因此被对端认为「已完成」的消息会在后续 pull round 里以新 packet id 重投——这正是 ADR-012 §12.1 的「跨崩溃至少一次」允许的重复。断连则把可恢复的重复变成不可用，收益为负。

#### R3：`.inflight` v2 形状（替代 ADR-012 §12.2 的 `released` 定义）

```erlang
#{vsn      := 2,
  next_pid := pos_integer(),
  pending  := [{Pid, {[SubRef], {Partition, Offset}}}],  % 未完成握手的出站消息
  qos      := #{Pid => 1 | 2},                           % pending 的交付等级
  released := #{Pid => true},                            % pending 的子集：已发 PUBREL、等 PUBCOMP
  received := #{Pid => true}}                            % 入站：已发 PUBREC、等 PUBREL
```

- `pending` 同时承载 QoS1（等 PUBACK）与 QoS2（等 PUBREC 或 PUBCOMP）的条目；`released` 只是标记「哪些 pending 已进入 PUBREL 阶段」——重连时据此选择**重发 PUBREL** 而不是 `dup=1` 重发 PUBLISH。
- `qos` 记录每条 pending 的交付等级，重连重发才不会把 QoS2 消息降级成 QoS1 发出去（客户端状态机会因此错位）。它是 `pending` 的伴随映射，与 `pending` 同生共死。
- ADR-012 §12.2 写的 `released := #{Pid => Offset}` 不足以在 PUBCOMP 时找到 `Refs`/`Partition` 去推进 cursor；本修订用「`pending` 保留完整条目 + `qos`/`released` 标记阶段」表达同一语义，信息严格更多，不新增第四份真相。
- **订阅时补发的 retained 消息不进 blob**：它们没有 `{Partition, Offset}`（不来自 WAL），没有可推进的 cursor，重连后应等客户端重新 SUBSCRIBE 而不是重发一个可能已过期的快照。它们照常占用 `pending`/`max_inflight` 窗口，但 `blob/1` 过滤掉。
- **读 v1**：`vsn=1` 的 blob 视为 `pending` 原样、`qos` 全 1、`released = received = #{}`、`next_pid` 不变；**写 v2**：任何写回升级为 `vsn=2`，单向，无降级路径（D9）。
- **刷新窗口**：`released`/`received` 与 `pending` 同在批量层（默认 100ms）。「同一会话重连不重复」在 blob 落盘后成立；崩溃落在窗口内时退化为「跨崩溃至少一次」。连接正常关闭时 `terminate/3` 会同步写入一次 blob（best effort），把窗口缩到「崩溃」而非「断连」。

#### R4：上下界（替代 ADR-012 §12.4 的前两条）

- `released` 与 `received` 的条目数合计 ≤ `max_qos2_state_per_session`（默认 100）；超限时**断连**并记 `mqtt_qos2_state_overflow`，不静默丢状态。
- `next_pid` 回绕：耗尽 `1..65535` 时先回绕并与 `pending` 中仍存活的 id 冲突检测；冲突则拒绝新投递并断连（沿用 ADR-012，实现上以 `pending` 集合为准）。
- 未知 id 不再断连（R1/R2）。

#### R5：入站 QoS 2 用 Method A

入站 QoS2 PUBLISH：append(fsync) → 记录 `received[Pid]`（内存立即、blob 批量）→ 回 PUBREC → dispatch。即**在 PUBLISH 时投递**（MQTT §4.3.3 Method A），`received` 只用于去重；同连接内 `dup=1` 重发同一 packet id 直接回 PUBREC、不再投递。跨重连时若 `received` 已落盘同样不重复；未落盘则允许重复（R3 的窗口）。

### 18.4 新增配置

| key | 默认 | 说明 |
|---|---|---|
| `broker_proto_mqtt.mqtt_max_qos` | `2` | S2-C 起默认允许 QoS2；仍受后端能力约束（E2） |
| `broker_proto_mqtt.max_qos2_state_per_session` | `100` | `released ∪ received` 上限 |
| `broker_retain.data_dir` | 与 `broker_storage.data_dir` 相同 | retain 文件落点（J8：必须同一绝对路径） |
| `broker_retain.retain_flush_interval_ms` | `100` | QoS0 retained 的批量落盘窗口 |
| `broker_retain.max_retained` | `10000` | 条数上限 |
| `broker_retain.max_retained_bytes` | `67108864` | 字节上限 |
| `broker_retain.max_payload_bytes` | `262144` | 单条 retained payload 上限（超限按普通消息投递、不入树） |

### 18.5 新增指标（ADR-016 的增量）

沿用 ADR-016「无外部依赖 + `atom()` 键」的口径，只增计数点，不改既有键：

| 键 | 类型 | 触发 |
|---|---|---|
| `retain_updates` | counter | retained 树成功写入/覆盖 |
| `retain_clears` | counter | 零长度 payload 清除 |
| `retain_overflow` | counter | 条数/字节/单条上限拒绝 |
| `retain_store_errors` | counter | retain 文件写失败（该次发布不回 ack） |
| `will_published` | counter | 异常断连成功发起 Will 发布 |
| `qos2_state_overflow` | counter | `released ∪ received` 超限断连 |

## 后果（Consequences）

- **+** Will 不引入第四份会话真相；异常断连路径统一在 `terminate/3`，可测。
- **+** Retain 的 PUBACK 承诺与 ADR-007 对齐：QoS1 retained 消息的「已确认」同时覆盖 WAL 与 retain 树。
- **+** 未知握手帧幂等消除了崩溃回退造成的重连循环；三条规则都能用「状态被清空后重发帧」的用例机械验证。
- **-** Retain 每次更新重写快照，且经过单进程；高频 retained 发布是已知瓶颈（S2-I 量化，S3 改追加日志）。
- **-** Will 在 broker 重启后丢失，与 mosquitto 默认一致，但与规范字面有差距；MQTT 5 Will Delay（S2.5）再评估。
- **-** QoS1+retain 的发布多一次 fsync（retain 文件）。
- **-** QoS2 的 `received` 批量窗口意味着「跨重连不重复」在崩溃窗口内退化为至少一次；文档必须写清楚，不能宣传 exactly-once。

## 验证（Validation）

- **帧级**：PUBREC/PUBREL/PUBCOMP 黄金字节（PUBREL flags `0b0010`、其余 `0b0000`）；非法 flags → 解码错误；`will_qos=2` 往返。
- **FSM**：正常四步、重连重发（未 PUBREC → DUP PUBLISH；已 PUBREC → PUBREL）、入站 DUP 不重复投递、未知 PUBREL/PUBREC/PUBCOMP 幂等、QoS2 状态超限断连。
- **Retain**：订阅补发（单 topic / 通配符多 topic / QoS=min）、零长度清除、`retain=1` 转发时置 0、QoS1 retained 在 PUBACK 前落盘（掉电后重启仍可补发）、QoS0 窗口、上限拒绝、损坏文件拒绝启动。
- **Will**：异常断连发布（含 QoS/retain 组合）、DISCONNECT 不发布、接管发布、broker 重启不补发。
- **崩溃恢复**：T14 新增 R8（已 PUBREC 未 PUBCOMP 时 `kill -9`，重启后按 §4.3 完成握手，允许重复不丢消息）。
- **真实客户端**：T15 新增 paho QoS2 双向、paho retain 补发/清除、paho will 异常断连；既有 `paho-qos2-refused` / `paho-will-ignored` 用例改为新语义。
- **文档**：`docs/protocol-compatibility.md` 的 MQTT 行、ADR-012 状态行、设计 §4.3 状态。
