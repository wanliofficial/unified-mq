# UnifiedMQ todo —— 剩余功能缺口清单

> 生成：2026-11-24　|　状态口径：`[ ]` 未开始　`[~]` 进行中　`[x]` 已完成
> 依据：`docs/compatibility/unifiedmq-mqtt-family-gap.md`、`docs/mqtt-feature-comparison.md`、`docs/roadmap.md`、`docs/security.md`、`docs/superpowers/specs/2026-11-2*.md`
> 前提：**S0–S12 阶段已全部收口**（`docs/roadmap.md` §14、`progress.md:4`）。本文件只列阶段之外的剩余项，不重复已交付的阶段能力。
> 测试数字一律以当次 `rebar3 ct` 为准；最近可查记录为 **1945/1945（2026-11-25，MQ-15 全量实跑，0 failed / 0 skipped）**，此前为 1705/1705（`docs/superpowers/specs/2026-11-22-session-state-replication-plan2-notes.md:169`）。

---

## P0 文档一致性（零代码风险，先做）—— ✅ 已完成 2026-11-24

这一组的共同特征：**实现已经完成或语义已经定死，但文档仍在说"没做"**。它们会让后续排期把已完成的工作重新立项，必须先清掉。

> **2026-11-24 全部执行完毕**，改动落在 `README.md`、`progress.md`、`docs/roadmap.md`、`docs/security.md`、`docs/compatibility/unifiedmq-mqtt-family-gap.md`、`docs/mqtt-feature-comparison.md` 共 6 个文件。原则：**历史结论不改写，用带日期的批注标注其已过期**；只对当前生效的结论直接改正文。

- [x] **1. MQ-09 发布侧重试：三处描述过时**
  - 现状：`unifiedmq-mqtt-family-gap.md:60` 与 `:166`、`mqtt-feature-comparison.md:164` 均写「不挂重传定时器 / 重发只在重连」。
  - 事实：连接内 ack 超时重试**已完整实现且默认启用**（`ack_timeout_ms` 默认 30000ms、`pull_round` 每 1000ms 扫描 `sent_at`、cursor `retries` 指数退避、同 packet id + DUP=1、`max_attempts`=10 后进 DLQ）；shared 成员超时走 release 回池重 claim。
  - 依据：`docs/superpowers/specs/2026-11-24-mq09-publish-retry-timer-reconcile-design.md:21-83`（逐环 file:line）与 §3「本稿要做的改动」。
  - 覆盖测试：`mqtt_conn_SUITE:a_timed_out_delivery_is_redelivered_with_dup_and_the_same_packet_id`、`an_unacked_message_is_resent_with_dup_and_the_same_packet_id`。
  - 注意：提交 `11346ac` 只新增了核实稿，**doc 改动本身尚未执行**。
  - **已完成**：代码侧参数已逐项复核（`ack_timeout_ms:41`、`pull_poll_interval_ms:31`、`retry_tick_ms:18`、`max_attempts:21`）；缺口表 MQ-09 行改为「已支持（2026-11-24 核实）」并补完整机制 + 两种重试形态；§8 结论段与 `mqtt-feature-comparison.md` §4.6 同步改写。

- [x] **2. 缺口表 §8 结论段把已交付项写成"还没做"**
  - `unifiedmq-mqtt-family-gap.md:175`：「明确列在范围外、还没做的：MQTT 3.1、SN 的 DTLS…」。
  - 事实：MQTT 3.1 已于 2026-11-22 交付（同表 MQ-16）；SN DTLS 已于 2026-11-17 落地（同文 SN-01）。
  - 处置：本句只保留真正未做的 `Forwarder Encapsulation`、QUIC 0-RTT/多流、WS `permessage-deflate`，另加一句交代两者移出的依据。
  - **已完成**：本句已收敛为三项，并追加 2026-11-24 更正批注说明两项移出的依据。

- [x] **3. `progress.md` 状态行落后于实际**
  - `progress.md:4` 仍为「2026-11-21，1579/1579、95 套件」；11-22 之后的会话复制 Plan 2/3、MQ-09/11/15 设计稿均未反映。
  - 处置：更新状态行（日期 + 套件数），并补一行「11-22~24 收口：会话复制边界债 #1–#5 全关闭」。
  - **已完成**：状态行更新为 2026-11-24，补测试基线说明（1705/1705 全绿 + 1729/1 failed 的 `error_in_suite` 构建缓存陷阱）与 `docs/todo.md` 指针。

- [x] **4. `security.md` 自相矛盾（Block2）**
  - `docs/security.md:371` 写「2026-11-20 已交付服务端 Block2」；`:372` 同章第 4 条仍写「**Block2 仍未实现**，这是真限制」。
  - 处置：`:372` 改为「Block2 已交付，仍不做跨块缓存/重传与 Observe 通知分块」。
  - **已完成**：原句加删除线 + 2026-11-24 更正批注，保留「仍不做」的两项边界。

- [x] **5. `roadmap.md` S2.5 段落的历史陈述易误读**
  - `docs/roadmap.md:195`：「topic alias / request-response / AUTH **仍未实现**（ADR-017 §17.1 冻结子集，S5 未纳入）」。
  - 事实：三项已分别由 MQ-04/MQ-06/MQ-05 于 2026-11-21 落地。
  - 处置：**不改写历史结论**，在句末追加带日期批注指向 MQ-04/05/06 的交付记录。
  - **已完成**：原文保留不动，追加 2026-11-24 批注 + 三份设计稿指针，并注明「MQTT 5 子集已无未交付项」。

- [x] **6. `mqtt-feature-comparison.md` §4 任务表与正文冲突**
  - `:105` 与 `:118` 仍是 2026-11-10 快照（「24 pending」「P0×5 P1×10 P2×9」），而 §4.6 已有 MQ-01~07、09~16 大量 `[x]`。
  - 处置：按 §4.1–§4.6 的勾选状态逐条重算，或明确标注「本表为 11-10 快照，状态以正文为准」。**不要臆造新数字**。
  - **已完成**：选择标注快照的路线（不重算数字，避免臆造）——状态块标题加「2026-11-10 快照」，追加醒目警告指向 §4.1–§4.6 与缺口表为唯一准绳，并点明 `#10` 已于 2026-11-22 结案。

- [x] **7. `README.md` 状态行停在 11-20**
  - `README.md:5` 最后一项进展是 ADR-035（跨节点 dist TLS）。11-21~24 的 MQTT 5 收尾、MQ-16、会话复制 Plan 1–3、吊销踢人均未进状态行。
  - **已完成**：追加 2026-11-21~24 段（MQTT 5 子集无未交付项、MQ-16、MQ-09 核实、会话复制边界债 #1–#5 全关闭、吊销踢人、1705/1705 基线）+ `docs/todo.md` 指针。

---

## P1 活跃功能缺口（已有设计稿，待 review → plan → 实现）

> **2026-11-25 状态：本节已全部收口**——MQ-11（`b95a32a`）、MQ-15（`53946f5`）已实现并合入；MQ-09 为纯文档项，其「两种重试形态」说明已随 P0-1 落笔。本节不再有 `[ ]`。

- [x] **MQ-11 per-subscriber 背压（慢消费者隔离）** ✅ 已实现并合入（2026-11-24，`b95a32a`）
  - 缺口定性：L1 mailbox 断连与 L2 per-conn in-flight 停投**均已存在且不是缺口**；真缺口只在 **L3 shared 窗口**——`shared_state_max=4096` 无 per-consumer 上限，一个慢消费者把 offset 钉在 `assigned` 即可拖停全组（`broker_subscription_worker.erl:552`、`:567`）。
  - 方案：fair-share 动态配额（固定小上限会浪费 97% 窗口，已被设计稿否决）+ retention 钉住可观测；skip 策略明确 out-of-scope。
  - 设计：`docs/superpowers/specs/2026-11-24-mq11-per-subscriber-backpressure-design.md`（§1.3 逐层定性、§2.2 配额算法）。
  - 下一步：review 定稿 → writing-plans → 实现 + `broker_subscription_SUITE` 新例（含「快消费者不被慢的拖住」正控与「无配额时全组 overflow」负控）。
  - **落地记录（2026-11-24）**：`broker_cursor:claim/7`（`PerConsumerMax`）+ `{ok, quota_hit, Row}` 区分「本消费者到配额」vs「组满 overflow」；worker 按 `shared_per_consumer_max>0` 固定上限、否则 `max(1, Limit div N)` 动态均分；`claim/5`、`claim/6` 等价保留。retention 观测：`wal_gc:lag_bytes/2` + `retention_lag/0`，`sweep/1` 写 `retention_lag_bytes` gauge。指标闭集：`shared_consumer_quota_hit`、`retention_lag_bytes`（ADR-016 禁高基数，per-consumer `shared_consumer_assigned` gauge 未做，见设计 §10）。测试 +9；全量 ct 1930/1930，dialyzer 0 warning。

- [x] **MQ-15 共享订阅集群再均衡** ✅ 已实现并合入（2026-11-25，`53946f5`）
  - 缺口定性（已闭合）：`cross_node_subscribe_ok/2` 曾显式拒绝跨节点 shared 组；根因是两节点独立 claim 会把同一 offset 派给两个消费者＝静默重复。消息体跨节点读取路径 S12-D 已存在，缺的只是**分配决策的跨节点串行化**。
  - 方案：分配决策进 ra 串行化 + 混合再均衡（懒式 + 阈值触发）。无 epoch 围栏（单调集合运算，§9 决策 1）；快照不压缩（§9 决策 2）；reclaim 复用 poll tick（§9 决策 3）；worker `handle_call({claim|ack|release,...})` flag 双路径（§9 决策 4）。
  - 设计：`docs/superpowers/specs/2026-11-24-mq15-shared-subscription-cluster-rebalance-design.md`；计划：`docs/superpowers/plans/2026-11-24-mq15-shared-subscription-cluster-rebalance.md`（7 task）。
  - **落地记录（2026-11-25）**：
    - **ra 元数据组**：新增 `shared_assignments` + `shared_consumers` 两表与 7 命令（`shared_claim/7`、`shared_ack/4`、`shared_release/3`、`shared_release_node/1`、`shared_reclaim/5`、`shared_join/3`、`shared_leave/2`），分配互斥由单线程 apply 保证；`snapshot_installed/5` 恢复两表；apply 返回干净 `{ok, Offsets}`（不回传内部表）。
    - **worker 双路径**：`broker_subscription:cluster_shared_rebalance` 默认 `false`＝零回归；开时 claim/ack/release 经 ra 并把 committed row 从 cache mirror 回灌本地 `?CURSORS`。cross_node 放宽：flag 关保持原跨 owner 拒绝；开时放行（ra 串行保证不重复），但 ra 元数据组未运行则拒绝（`shared_rebalance_unavailable`）。
    - **崩溃回池（§4.1(c)）**：`shared_release_node(Node)` 原子回池崩溃节点上全部 shared consumer 的 offset（跨所有 sub/partition）并清其 consumer 条目；`broker_cluster_nodedown:on_nodedown/1` 在分区迁移后追加该调用（仅 flag 开），复用既有去抖；`broker_cluster_nodedown_mon` 在 session_replication 或 cluster_shared_rebalance 任一开时即监视成员。
    - **阈值再均衡（§4.1(b)）**：worker claim 得 `quota_hit` 后 `maybe_rebalance` 读镜像算每 consumer in-flight，当 `max-min > shared_rebalance_hysteresis` 且过 `shared_rebalance_cooldown_ms` 时经 `shared_reclaim` 把过载者超出均值的 offset 改派给最空闲者，inc `shared_reclaim`。复用 poll tick，无新进程。
    - **指标闭集（ADR-016）**：新增 `shared_reclaim`、`shared_rebalance_no_quorum`。
    - **测试**：`broker_cluster_meta_SUITE` +1（`shared_release_node` 跨两组释放 + survivor 保留 + consumer 表清理）；`broker_subscription_shared_dist_SUITE` 新建 3 例（跨节点并发 claim 不重复、崩溃 offset 回池可回收、阈值 reclaim 改派单 offset）；worker_SUITE +3（flag off 拒绝 / on 放宽 / non-shared 恒 ok）；broker_cluster_SUITE +2（经真实 ra claim/ack/release + consumers 表）；metrics_SUITE 同步。**全量 `rebar3 ct` 1945/1945、0 failed / 0 skipped，dialyzer 0 warning，xref 仅既有 unused-export。**
  - 范围限制（与 ADR-034 S12-D 一致）：shared 组跨节点仅交付分配串行化与不重复投递；owner 读热点、shared 的 exactly-once 不追求、默认关闭。

- [x] **MQ-09 语义已闭环后的产品化取舍（非代码）** ✅ 已随 P0-1 一并落笔（2026-11-24）
  - 实现已完整（见 P0-1），剩下的是与 fss/robustmq 对读时说明「exclusive/failover 原地 resend vs shared 重 claim」两种重试形态的差异。属文档，工作量极小，与 P0-1 合并执行即可。
  - **已完成**：`docs/compatibility/unifiedmq-mqtt-family-gap.md:60`（MQ-09 行改为「已支持（2026-11-24 核实）」+ 完整链路与测试指针）、`:166`（重试路径逐环描述），`docs/mqtt-feature-comparison.md:166`（§4.6 改写为「连接内 ack 超时重试已实现且默认启用」，并显式写出**两种重试形态**：exclusive/failover 单读者原地 `resend`；shared 超时 `release` 回池由任意成员**重新 claim**，新 packet id、DUP=0 —— 这正是与 fss/robustmq 对读时需要的取舍说明）。核实稿：`docs/superpowers/specs/2026-11-24-mq09-publish-retry-timer-reconcile-design.md`（§3 第 3 条「可选」即本次落笔内容）。实现侧零改动。

---

## P2 产品化对标缺口（未立项，需产品决策）

来源 `docs/mqtt-feature-comparison.md` §4，任务编号 `#5`–`#28` 与 `#29`–`#43`。**这些不是协议缺陷，是对标生产级 broker 的差距**；是否立项取决于目标定位。

- [ ] **P0 级（正确性/合规红线）**
  - [x] durable session 复制的 **SIGKILL / 网络分区故障注入证明** —— **✅ 已交付（2026-11-25）**：管线已有（Plan 1，默认关闭），缺「节点丢失后 durable 消息与订阅集合不丢」的 harness。新增 `broker_session_replica_SUITE` 两个硬杀用例——`subscription_set_survives_sigkill`（订阅集 + 游标在 owner 被 OS `kill -9` 后由副本恢复、游标不回退）、`inflight_blob_survives_sigkill`（QoS2 in-flight blob 硬杀后原样恢复、无重复行）；均「先轮询副本表证明复制到达、再硬杀」，并经源码级变异（复制改 no-op → 4 例超时变红）验证非退化。网络分区 / 失多数派 / 无双 owner 分裂脑由 `broker_cluster_dist_SUITE:online_join_is_rejected_without_a_majority` + `broker_rebalance_dist_SUITE`（`no_quorum`）+ 本套件 `stale_owner_fenced` 覆盖。**边界**：模块级会话平面身份证明（非 MQTT 线级零重连——后者由 `broker_cluster_conn_fsm_takeover_dist_SUITE` 覆盖）；真网络分区因 Erlang `global` 自愈无法稳定构造，未引入脆弱用例。
  - **OIDC / JWKS 客户端认证**：现仅 user_pass / token（`broker_security` 统一身份表）。
  - **防篡改审计日志（哈希链/签名）**：`docs/security.md` §8 明列未实现。
- [ ] **P1 级（生产可用性）**
  - deny-by-default ACL 的默认姿态与推荐配置（当前默认 `allow` 为 legacy 兼容）。
  - 客户端 listener 的 TLS 1.3 / mTLS **端到端证据**（2026-11-24 核实）。能力已在：`broker_security_tls.erl:43` 默认 `versions => ['tlsv1.2','tlsv1.3']`，mTLS 选项装配有单测 `broker_security_tls_SUITE:verify_peer_adds_the_client_ca`；缺的是 MQTT/WS listener 真握手断言（现有 `mqtt_tls_SUITE` 只跑 `verify_none`）。注意：**distribution 的 mTLS 已由 ADR-035 交付**，不要混为一谈。
  - 多模式存储后端（RocksDB / File / S3 冷分层）：现仅内存 + WAL。
  - ~~Prometheus / Grafana / OTel 导出：`broker_metrics` 有指标，无导出层。~~ **Prometheus 导出已交付（2026-11-24 核实）**：`GET /metrics` 返回 `200 text/plain; version=0.0.4`（`broker_admin_http.erl:68-69` 路由、`:305-325` 渲染，测试 `broker_admin_SUITE:metrics_endpoint_lists_the_closed_key_set`）。**仅剩** Grafana dashboard 与 OTel 导出未做。
  - 畸形包三类断言（静默关闭/拒绝/接受，借鉴 fss `wire.rs`）：MQTT **已覆盖主要异常路径**（`mqtt_conn_SUITE:1844` 非法 filter 断连无 SUBACK、`:1956` reserved QoS 位断连、`:2344` 超限 PUBLISH 断连；`mqtt_frame_SUITE:427` 截断包永不 OK、`:447` 固定头 flag 拒绝），缺 `test/conformance/mqtt/wire/` 下「三类逐条对账」独立套件（`test/conformance/` 目录不存在）。
  - 外部 conformance 接入：L3 外部套件未建，当前只有设计稿 `docs/protocol-conformance-validation.md`（`test/conformance/` 目录不存在）——这是**真缺口**，与上面的畸形包已覆盖部分分开看。
- [ ] **P2 级（增强）**
  - 规则引擎（filter / reshape / re-route + sinks）、Web 管理控制台、SBOM/SLSA/VEX 可复现构建、K8s operator/Helm、FIPS 变体、Edge-to-cloud 离线缓冲。
- [x] **P0 吊销实时驱逐活动会话** —— 2026-11-22 已完成（`broker_revocation` + admin `/api/v1/revoke`）。**范围限制**：只在 MQTT 连接路径生效，其余协议认证入口不查吊销表；状态节点本地、粒度仅 principal。

---

## P3 明确不做（ADR/设计已冻结，不得转成排期）

盘点时容易误判为缺口，实际是显式边界：

- **CloudEvents 官方 Binding 逐字实现** —— S7 裁定**不立项**（MQTT 5 要求去前缀、Kafka 用 `ce_`、NATS 需 percent-encoding、WS 需子协议、AMQP 仅 1.0、Pulsar 无规范），见 `docs/roadmap.md:351-353`。
- **CoAP** —— 证书/mTLS、OSCORE、over TCP/WebSocket、Observe 跨节点与崩溃恢复、Observe 通知分块（服务端 Block2 响应已于 11-20 交付）。
- **WebRTC / ICE** —— 媒体面、TURN over TCP/TLS/DTLS/5349、ICE-lite、Trickle、多人房间、速率限制闸门（`docs/security.md` §12.2/§12.3）。
- **A2A / MCP / ACP** —— 18 条（SSE pending-bytes 闸门、ACP v2、JWT/JWKS/KMS 等，`docs/security.md` §12.1）。
- **传输层** —— MQTT-SN Forwarder Encapsulation / 串口桥；QUIC 0-RTT / 多流 / 连接迁移；WS `permessage-deflate`。
- **S12-D 跨节点投递** —— owner 读热点、默认关闭、不追求 exactly-once。~~shared 组跨节点不交付~~ **（2026-11-25 批注：shared 组跨节点分配串行化已由 MQ-15 交付（`53946f5`），flag `cluster_shared_rebalance` 默认仍关；上列其余边界不变。）**
- **MQTT 语义取舍项** —— MQ-08（QoS2 跨崩溃至少一次）、MQ-10（100ms 刷盘窗口）、MQ-12（超大出站丢弃）、MQ-13（Will 不落盘）、MQ-14（离线追赶无优先级）：已选语义 + 测试覆盖，**不是半成品**。

---

## P4 Gate 项（2026-11-25 已全部勾选）

- [x] **`docs/roadmap.md:537`（Gate S12 最后一项，已于 2026-11-25 勾选）**：有状态服务的
  状态持久化与崩溃恢复**绑定 ADR-007 三档的专项压测**已补齐。
  - 已具备：崩溃节点重入并追平 epoch 由 `broker_cluster_dist_SUITE:a_crashed_node_rejoins_and_catches_up` 证明。
  - 已具备（2026-11-24 核实）：三档**丢失边界**已实测并落盘——`docs/benchmarks/reliability/s2-b-osbuffer-loss.md` 逐档给出 `fsync` 前缀完整可读、`os_buffer` 尾部可丢且 `high` 回真实末尾、`replicated_commit` 分配 offset 前显式拒绝，含替身档与 dm-flakey 实机档。
  - **已补齐（2026-11-25）：三档容量专项 + 绑定 S12 有状态服务（ra 元数据层）**——`tools/bench/s12_adr007_capacity.erl`（真实 3 peer + 真实 ra + 真实 WAL）实测并落盘 `docs/benchmarks/reliability/s12-adr007-tier-capacity.md`：串行 ACK 延迟 L1≈0 < L2≈3 ms < L3≈7–8 ms；饱和吞吐天花板 ≈ 14.7k : 1.9k : 0.85k msg/s（L1:L2:L3）；ra 元数据层控制面 ~450 提交/s（单次 ~2 ms）；失多数派时 L3 逐条拒绝（ok=0/failed=2000）而 L1/L2 照常成功（各 ok=2000）——证明 ra 组的可用性只门控 `replicated_commit`。Gate S12 全部勾选项证据齐备。

---

## 建议执行顺序

1. ~~**P0-1 ~ P0-7**（纯文档，一次性提交；消除"已实现却记为缺口"的误报）~~ —— **已完成 2026-11-24**
2. ~~**MQ-11 → MQ-15**（唯一在推进的功能缺口；两者都涉及 `assigned` 窗口与 ra 串行化，MQ-11 先做可为 MQ-15 铺垫）~~ —— **均已交付：MQ-11 `b95a32a`（11-24）、MQ-15 `53946f5`（11-25）。P1 活跃功能缺口清空。**
3. **P2 的 P0 三项**（是否立项需产品确认目标定位）—— **SIGKILL harness 已交付（2026-11-25）；OIDC/JWKS 与防篡改审计仍需产品决策**
4. ~~**P4** 与容量报告合并做~~ —— **已交付 2026-11-25**：Gate S12 最后一项已勾选，三档容量专项 + ra 元数据层绑定报告落盘 `docs/benchmarks/reliability/s12-adr007-tier-capacity.md`
5. ~~**MQ-09 产品化取舍**（纯文档，与 P0-1 合并即可，无排期压力）~~ —— **已落笔 2026-11-24**

> 当前状态（2026-11-25）：**P1 已无 `[ ]` 项；P4 容量专项已交付（Gate S12 全部勾选项证据齐备）。**
> 剩余待办集中在 P2（原 9 项）：**SIGKILL/网络分区 harness 已于 2026-11-25 交付**，其余 8 项仍需产品决策定位。

---

## 核实记录（2026-11-24）

> 对上方每一条 `[ ]` 做了一次「代码/产物 vs 文档口径」核对，判定分四类。**结论（2026-11-24 快照）：P1 全部为真缺口；P2 中 Prometheus 导出为误报（已就地改），TLS1.3/mTLS、畸形包、ADR-007 三档为部分误报（已就地改并标注「能力已在、缺专项证据」）；其余 P0/P1/P2/P4 为真缺口。**
>
> **2026-11-25 更新**：上句的「P1 全部为真缺口」已过期——**MQ-11（`b95a32a`）与 MQ-15（`53946f5`）均已交付**，MQ-09 的文档取舍也已随 P0-1 落笔，**P1 已无 `[ ]` 项**。表中 P1 三行已就地更新判定，其余行仍为 2026-11-24 判定。
>
> **2026-11-25 再更新**：上句「其余……P4 为真缺口」也已过期——**P4（ADR-007 三档容量专项 + 绑定 S12 有状态服务）已交付**，`docs/roadmap.md` Gate S12 最后一项勾选，表中 P4 行已改为「✅ 已交付」。至此 `[ ]` 仅剩 P2 九项（需产品决策定位）。

| 项 | 判定 | 证据（file:line / 产物） |
|---|---|---|
| P1 MQ-11 per-consumer 配额 | 真缺口 → ✅ 已交付（`b95a32a`） | `broker_cursor:claim/7`（`PerConsumerMax`）+ worker `shared_per_consumer_quota/2`；retention `wal_gc:lag_bytes/2` + `retention_lag_bytes` gauge |
| P1 MQ-15 跨节点 shared | 真缺口 → ✅ 已交付（`53946f5`） | 跨节点 shared 分配已进 ra 串行化（`broker_cluster_meta_machine` 的 `shared_assignments`/`shared_consumers` 两表 + 7 命令）；`cross_node_subscribe_ok/2` 在 `broker_subscription:cluster_shared_rebalance=true` 时放行，ra 组未运行则拒绝 `{shared_rebalance_unavailable, no_meta}`。默认关闭、零回归。 |
| P1 MQ-09 产品化取舍 | 真缺口（纯文档）→ ✅ 已落笔（2026-11-24，随 P0-1） | `unifiedmq-mqtt-family-gap.md:60,166` + `mqtt-feature-comparison.md:166`（含 exclusive/failover 原地 resend vs shared 重 claim 两种形态对比）；核实稿 `2026-11-24-mq09-publish-retry-timer-reconcile-design.md` |
| P2-P0 SIGKILL/网络分区 harness | ✅ 已交付（2026-11-25） | `broker_session_replica_SUITE` 新增 `subscription_set_survives_sigkill` + `inflight_blob_survives_sigkill`（OS `kill -9` 后 durable 订阅集/游标/in-flight blob 由副本恢复），源码级变异验证非退化；网络分区/`no_quorum` 由既有 dist 套件覆盖 |
| P2-P0 OIDC/JWKS | 真缺口 | 全仓无 `oidc`/`jwks` |
| P2-P0 防篡改审计 | 真缺口 | `security.md:24,255` 明列未实现 |
| P2-P1 deny-by-default ACL | 真缺口 | 默认 `allow`（`security.md:100`） |
| P2-P1 TLS1.3/mTLS 专项 | **部分误报** | 能力已在 `broker_security_tls.erl:43` + `broker_security_tls_SUITE:88`；缺 listener 端到端断言（已就地改） |
| P2-P1 多模式存储后端 | 真缺口 | 仅 memory + WAL，无 rocksdb/s3/file |
| P2-P1 Prometheus 导出 | **误报（已交付）** | `broker_admin_http.erl:68-69,305-325` + `broker_admin_SUITE:152`（已就地改） |
| P2-P1 畸形包三类断言 | **部分误报** | MQTT 已覆盖 `mqtt_conn_SUITE:1844/1956/2344`、`mqtt_frame_SUITE:427/447`；缺 conformance 三类对账套件（已就地改） |
| P2-P1 外部 conformance | 真缺口 | `test/conformance/` 不存在，仅设计稿 |
| P2-P2 规则引擎/Web/SBOM/SLSA/VEX/K8s/FIPS/Edge | 真缺口 | 全仓均无（CI 无 sbom/slsa/vex/fips，无 Chart.yaml/k8s，无 offline buffer） |
| P4 ADR-007 三档压测 | ✅ 已交付（2026-11-25） | 丢失边界 `docs/benchmarks/reliability/s2-b-osbuffer-loss.md` + 容量专项 `docs/benchmarks/reliability/s12-adr007-tier-capacity.md`（`tools/bench/s12_adr007_capacity.erl`，真实 3 peer：三档容量天花板 + ra 元数据层绑定 + 失多数派拒绝） |

> 注：本报告只核对「是否已在代码/产物里实现」，不改变每条的立项必要性；多数 P2 项仍是「要不要做」的产品决策，而非「做没做」的遗漏。
