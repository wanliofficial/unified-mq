# MQTT 特性对比与差异待办（fss-mqtt-broker / robustmq vs 本仓库）

- **文档版本**：v1.2（2026-11-10 起草；2026-11-10 同步任务状态）
- **状态**：调研对比 + 差异待办（非实现文档）；§4 待办已登记为任务 `#5`–`#28`，状态见 §4 任务状态同步块
- **读者**：协议层 / 架构 / 路线图评审
- **对应**：`docs/protocol-compatibility.md` §4/§4.1/§4.2（本仓库 MQTT 已实现事实）、`docs/security.md`（认证/ACL/TLS/审计现状）、`docs/architecture.md` §1、`docs/roadmap.md` §8/§9
- **数据来源**：两项目 GitHub 主页（2026-11-10 抓取）；本仓库能力以 `docs/protocol-compatibility.md` / `docs/security.md` 已实测事实为准

> 本文聚焦 **MQTT 协议特性**，但两项目把「集群、会话复制、ACL、TLS、共享订阅、桥接、规则引擎」都作为 MQTT Broker 的一等能力，因此这些相邻能力一并纳入对比。与 MQTT 无关的纯项目特性（如 robustmq 的 mq9 AI Agent mailbox）仅在末尾标注，不进差异待办。

---

## 1. fss-mqtt-broker 的 MQTT 特性（Rust，Apache-2.0）

来源：`github.com/mbilling/fss-mqtt-broker`（mqttd）

**协议与核心语义**
- MQTT **3.1.1 与 5.0** 双协议支持。
- **Durable QoS 1 默认复制**：ack 后做 fsync + 另一节点 quorum 副本；2,000,000 msg/s 零丢失（12 节点）。
- 确认的 QoS 1/2 消息**可幸存节点丢失**（SIGKILL 与网络分区 harness 证明）。

**集群与复制**
- **集群原生**：clustering + replicated sessions（默认 2 副本，可配 2–7）。
- **Acked message survives node loss**：经 SIGKILL 与 partition harness 证明。
- **Site bridges**：站点桥接把各站点的 topic 汇入同一个 `$share` 组。
- **Cluster-wide shared subscriptions**：集群级 `$share`，非单节点。

**安全与治理**
- **TLS 1.3 + mTLS 或 OIDC 身份**；deny-by-default ACL；吊销可驱逐**活动会话**；hash-chained **防篡改审计日志**。
- **OIDC（JWKS discovery）** 客户端认证；JWT client auth。
- 每个不安全模式都是 opt-in 且记 `INSECURE:`；映射 EU CRA / IEC 62443 / SOC 2 / ISO 27001。

**运维与交付**
- 单静态二进制（~14MB distroless 镜像）、Helm chart、**Kubernetes operator**、cosign 签名可复现构建 + SBOM + SLSA provenance + VEX。
- **Rule engine**（filter / reshape / re-route，有 sinks）。
- 自带告警的可观测性、`deploy/observability/`。
- FIPS 变体。

**测试**
- `./scripts/interop/run.sh` 跑外部客户端 conformance；仓库内 `crates/mqttd/tests/wire.rs` 手工字节级畸形包测试（静默关闭 / 拒绝 / 接受三类断言）。

**定位声明**：该 broker 「no production users yet」，数据是它自己提供的证据；功能清单完整但部分（如 8→12 节点 join 丢 11 个 durable session，#1218/#1219）仍是未修缺陷。

---

## 2. robustmq 的 MQTT 特性（Rust，Apache-2.0）

来源：`github.com/robustmq/robustmq`

**协议与核心语义**
- **MQTT 3.1 / 3.1.1 / 5.0**：核心稳定并持续成熟（官方标注「MQTT core is stable」，Kafka/NATS/AMQP 仍在开发）。
- 会话持久化与恢复（Session persistence and recovery）✅。
- **Shared subscription** ✅（打破「并发=分区数」限制，消费者可随时弹性扩容）。
- **Authentication and ACL** ✅。

**统一架构（影响 MQTT 的关键）**
- **Unified storage layer**：所有协议共享一个存储引擎，消息**写一次、任意协议消费**（MQTT 发布 → Kafka/NATS/AMQP 消费）。
- **Native multi-tenancy**：跨协议统一，每租户数据隔离 + 独立权限管理。
- **Multi-mode storage**：Memory / RocksDB / File 可选，按 topic 配置，**冷数据自动分层到 S3**。
- **Edge-to-cloud**：单二进制零依赖，离线缓冲 + 自动同步。
- **Built-in Raft** 共识。

**运维**
- **Grafana + Prometheus 监控** ✅（Admin HTTP API + `/metrics`）。
- **Web management console** ✅。
- 超低延迟 NATS 纯内存路由（与 MQTT 共享运行时）。

**MQTT 专属文档**：官方站 `robustmq.com/RobustMQ-MQTT/` 有 MQTT 特性与配置、CLI 命令参考。

> robustmq 的 mq9（AI Agent mailbox）是第五个原生协议，**不是 MQTT 特性**，本文不纳入差异待办；但它印证了「统一消息模型 + 多协议消费」这一架构方向，与本仓库的 CloudEvents 统一层目标一致（见 §4 对照）。

---

## 3. 本仓库（unified-mq）当前 MQTT 能力（已实现事实）

以 `docs/protocol-compatibility.md` §4/§4.1/§4.2 与 `docs/security.md` 为准：

| 维度 | 当前状态 |
|---|---|
| 协议版本 | MQTT 3.1.1 + **5.0 子集**（S2.5/ADR-017） |
| QoS | 0/1/2 全实现；QoS2 **跨崩溃至少一次**（非 exactly-once，ADR-012/018） |
| Retain | 独立 `broker_retain`，持久化 + 订阅补发 + 零长度清除 |
| Will | 异常发布已实现 |
| 会话 | clean/persistent session、per-session 订阅+cursor+in-flight 跨连接持久化；离线追赶（无优先级、无独立队列） |
| 通配符 | `+` / `#`（Annex A 校验） |
| MQTT 5.0 特性 | session expiry、message expiry、reason codes、properties、user properties、`$share/{group}/{filter}` Shared 订阅、Maximum Packet Size 出站丢弃 |
| 认证 | `broker_security` 统一身份表（user_pass / token），六协议共用；匿名可配；空 ClientID 拒绝 |
| ACL | 有序 first-match，**规则列表无匹配即 deny**；默认值为 `allow`（legacy，可配为规则列表实现 deny-by-default） |
| 租户隔离 | 认证身份绑定租户，租户隔离先于 ACL；事件属性永不改变身份/租户/目标 |
| TLS | 所有协议 listener 同一开关，可选 TLS，支持 PEM 轮换（无明确 1.3/mTLS 专项） |
| 审计 | `broker_security_audit_SUITE` 覆盖；**签名/防篡改未实现**（`security.md` §8） |
| 存储后端 | 本地 内存 / WAL（ADR-007 L1/L2）；复制靠 Metadata Raft + `replicated_commit` |
| 跨协议 | CloudEvents 统一消息模型；逐协议 `ce-*` Binding（S7 结案） |
| 测试 | 帧层 CT + 真实 paho/mosquitto 逐帧字节一致（T15）+ 崩溃恢复（T14） |

**已知对外限制**（与两项目对照时即为差距）：无 topic alias、无 AUTH 扩展认证、subscription identifier 不透传、QoS2 非 exactly-once、无背压只有停投、无集群级共享订阅/桥接/规则引擎。

---

## 4. 差异待办列表（本仓库 vs 两项目）

> 标记：**缺** = 当前未实现；**部分** = 已实现但形态不同/需配置/未证明；**同** = 对齐。
> 优先级：**P0** 影响正确性/合规红线；**P1** 影响生产可用性与对标；**P2** 增强项。

> **任务状态同步（2026-11-10）**：下列 24 项已登记为可追踪任务（`#5`–`#28`，按待办出现顺序连续分配），当前状态全部为 **待办（pending）**。任务板另有前期 S7 阶段的 `#1`–`#4`，其对应能力（os_buffer 接 CI、六协议统一认证、S7 立项、trace context 传播）已在代码中结案，状态已同步为 completed，不在本清单内。整体任务板：**4 completed / 0 in_progress / 39 pending（共 43 项）**。其中 `#29`–`#43` 是 2026-11-10 另一次内部评估登记的「ACL/认证 + 集群」缺口（15 项），主题与本竞品对比清单不同，详情见任务板、不并入下表。状态以任务板为准，本表为当日快照；后续进展须同步更新两侧。

**按分组的任务编号与状态**

| 分组 | 任务编号 | 优先级分布 | 状态 |
|---|---|---|---|
| 4.1 集群 / 复制 / 会话高可用 | `#5`–`#8` | P0×1 P1×2 P2×1 | 全部 pending |
| 4.2 认证 / 授权 / 安全治理 | `#9`–`#14` | P0×3 P1×2 P2×1 | 全部 pending |
| 4.3 规则引擎与数据处理 | `#15` | P2×1 | pending |
| 4.4 存储与边缘 | `#16`–`#17` | P1×1 P2×1 | 全部 pending |
| 4.5 可观测性与运维交付 | `#18`–`#21` | P1×1 P2×3 | 全部 pending |
| 4.6 协议语义补齐 | `#22`–`#26` | P0×1 P1×2 P2×2 | 全部 pending |
| 4.7 测试资产 | `#27`–`#28` | P1×2 | 全部 pending |
| **合计** | **`#5`–`#28`（24 项）** | **P0×5 P1×10 P2×9** | **24 pending** |

### 4.1 集群、复制与会话高可用

- [ ] **P0 集群级 durable session 复制（2–7 副本，quorum）**：fss 默认 2 副本且 Acked 消息可幸存节点丢失；本仓库有 `replicated_commit` 但**未证明**跨节点会话/消息在 SIGKILL 下零丢失。待办：定义并测「节点丢失后 durable 消息与订阅集合不丢」的 harness（对标 fss 的 SIGKILL/partition harness）。
- [ ] **P1 跨节点共享订阅（$share cluster-wide）**：当前 `$share` 在单节点语义内；需扩展到集群级消费组分发。
- [ ] **P1 站点桥接（bridge）**：两项目均有；本仓库无 bridge 能力。待办：评估是否纳入 S7 之后路线。
- [ ] **P2 集群扩容零会话丢失**：fss 实测 3→5→8 join 保住会话（其 8→12 有缺陷 #1218，可作反面教材）；本仓库需补 join/leave 的会话与消息不丢证明。

### 4.2 认证、授权与安全治理

- [ ] **P0 OIDC / JWKS 客户端认证**：fss/robustmq 均有；本仓库仅 user_pass/token，无 OIDC。待办：在 `broker_security` 增加 JWKS discovery + JWT 验证形态，纳入统一身份表。
- [ ] **P0 吊销实时驱逐活动会话**：fss 有；本仓库无 revocation 机制。待办：定义 token/user 吊销后已连接会话的驱逐路径。
- [ ] **P1 deny-by-default ACL 默认姿态**：本仓库 ACL 默认 `allow`（legacy）；需明确「新部署默认 deny，无匹配即拒」的推荐配置与文档，避免误配放行。
- [ ] **P1 TLS 1.3 / mTLS 专项**：本仓库有可选 TLS 但未声明 1.3/mTLS 与证书双向校验；补协议级测试与文档。
- [ ] **P0 防篡改审计日志（签名/哈希链）**：`security.md` §8 已列未实现；fss 有 hash-chained audit。待办：审计日志签名/VEX 化。
- [ ] **P2 FIPS 变体**：fss 有；本仓库无，记录为低优先。

### 4.3 规则引擎与数据处理

- [ ] **P2 规则引擎（filter / reshape / re-route + sinks）**：fss 有；本仓库 CloudEvents 策略层是「pass_through 恒等 + 按租户/目的地开启」，无通用规则引擎。待办：评估是否扩展 `broker_event` 为规则引擎。

### 4.4 存储与边缘

- [ ] **P1 多模式存储后端（RocksDB / File / S3 冷分层）**：robustmq 有 Memory/RocksDB/File + S3 冷分层；本仓库仅内存/WAL。待办：评估 pluggable storage 与冷数据分层。
- [ ] **P2 Edge-to-cloud 离线缓冲 + 自动同步**：robustmq 有；本仓库无边缘同步；记录为低优先。

### 4.5 可观测性与运维交付

- [ ] **P1 外部可观测性导出（Prometheus / Grafana / OTel）**：`security.md` §8 / S7 之后已列「OTel 等可观测性导出层未实现」；robustmq 有原生 Prometheus。待办：把 `broker_metrics` 指标导出到 Prometheus/OTel。
- [ ] **P2 Web 管理控制台**：robustmq 有；本仓库无。
- [ ] **P2 可复现构建 + SBOM + SLSA/VEX**：fss 有 cosign 签名 + SBOM；本仓库 rebar3 构建，无 SBOM/签名。待办：补 SBOM 生成与签名（与防篡改审计同源）。
- [ ] **P2 Kubernetes operator / Helm**：fss 有；本仓库无官方 chart/operator。

### 4.6 协议语义补齐（MQTT 自身）

- [ ] **P1 topic alias（MQTT 5.0）**：本仓库未实现；补编解码与映射。
- [ ] **P1 AUTH 扩展认证（MQTT 5.0）**：本仓库未实现；与 §4.2 OIDC 关联。
- [ ] **P2 subscription identifier 透传到投递帧**：当前 `mqtt_conn_fsm` 不透传；补 v5 语义。
- [ ] **P0 QoS2 exactly-once 边界澄清**：本仓库明确「跨崩溃至少一次」（ADR-012）；与 fss 的「acked QoS1/2 survives node loss（集群副本）」语义不同（本仓库是单节点 WAL 保证，非集群副本）。待办：文档红线已就位，需补集群副本下的 exactly-once 承诺界定。
- [ ] **P2 背压（慢消费者隔离）**：本仓库只有「停投」无背压；两项目有流控/隔离。待办：评估 per-subscriber 背压。

### 4.7 测试资产（已在 `protocol-conformance-validation.md` 立项）

- [ ] **P1 畸形包/线格式专项（借鉴 fss `wire.rs`）**：本仓库 T15 已有逐帧字节对比；按 `docs/protocol-conformance-validation.md` §4.3.2 扩展「静默关闭/拒绝/接受」三类断言。
- [ ] **P1 外部客户端 conformance（`scripts/interop/run.sh` 思路）**：按 `docs/protocol-conformance-validation.md` 接入 Paho 互操作 + kafkatest。

---

## 5. 对齐项（本仓库已与两项目一致，记入备查）

- [x] MQTT 3.1.1 + 5.0 双协议（robustmq 还含 3.1，本仓库 3.1.1 已覆盖 3.1 语义）。
- [x] QoS 0/1/2、Retain、Will、会话持久化、通配符、Shared 订阅（`$share`）。
- [x] 统一身份表 + ACL（first-match、deny 无匹配）+ 租户隔离先于 ACL。
- [x] 多协议统一消息模型（本仓库 CloudEvents 层 / robustmq unified storage 同方向）。
- [x] 可选 TLS、审计套件、Raft 共识（S4）。

---

## 6. 范围外（不纳入 MQTT 差异待办）

- robustmq 的 **mq9 AI Agent mailbox**（第五协议，非 MQTT）。
- fss 的商业对比数据（HiveMQ/EMQX 基准）——仅作竞品参考，不转化为本仓库待办。
