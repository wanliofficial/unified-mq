# ADR-030：S11 协议候选池筛选——仅 CoAP 进入实现评审，SIP 并入 S10 信令面评估，链路本地发现保留为延迟候选

- **状态**：已采纳（S11-0 评审签认，2026-11-15）
- **日期**：2026-11-15
- **关联**：ADR-001（协议 Adapter 与消息内核分离）、ADR-007（显式可靠性三档）、ADR-009/013（`broker_message` / 订阅游标）、ADR-016（可观测最小集、无外部依赖）、ADR-021（集群控制面 Metadata Raft / Membership / Fencing——发现面不得替代）、ADR-027（前端依赖账本）、ADR-028（WebRTC 信令面边界——SIP 并入其评估）、ADR-029（ICE 组件——`processone/stun` 复用其结论）
- **关联文档**：[`docs/design/s11-protocol-candidates.md`](../design/s11-protocol-candidates.md)（总览 §1 全表 / §3 优先级 / §4.4 OQ-1~OQ-9）、14 份协议立项稿（§5 文档产出物索引）、[`docs/roadmap.md`](../roadmap.md) §13（S11）
- **事实来源**：14 个 GitHub 仓库（`master`/`main`）README、`rebar.config`/`Makefile`、关键源码逐字登记见各立项稿 §2；RFC 7252/7641/6690/7959/8323（CoAP）、RFC 5389/5766（STUN/TURN）、RFC 6763（DNS-SD）、SIP RFC 3261（部分）等只在仓库逐字引用时记录；README 自述性能/规模数字一律不引用（须来自 S11 实测报告）

## 背景（Context）

S11 是**候选池 / 筛选阶段**，不是「14 个协议一起实现」。14 个 Erlang/OTP 网络协议仓库已完成三段式立项调研（9 份深挖 + 5 份索引级），总览 [`s11-protocol-candidates.md`](../design/s11-protocol-candidates.md) 给出四档分类建议与 9 个未决问题（OQ-1~OQ-9）。S11-0 的职责是把这些开放选项**收敛成冻结裁决**：哪些进入实现、哪些归并既有阶段、哪些仅作参考、哪些明确不做，并回答全部 OQ。

必须一起定下来的问题：①进入实现评审的协议范围；②发现面（DHT / mDNS / DNS-SD / DNS）是否需要、以什么形态存在；③SIP 是否与 S10 信令面合并评估；④归并既有阶段的三个候选（LoRaWAN / stun / APNs）如何收窄；⑤五个索引级稿的去向；⑥S11 与 S9 / S10 的先后与装配边界。

本轮是**纯文档收敛**：不新增 app、不新增端口、不新增代码路径。经 Gate 通过的候选在后续子阶段（S11-A/B/C）才有代码与实测证据。

## 决策

### D1. 仅 CoAP 进入实现评审（S11-A/B/C），其余候选不进入本轮实现

- **唯一进入实现评审的是 `gotthardp/gen_coap`**（`broker_proto_coap` adapter），子阶段 S11-A（前端与编解码）/ S11-B（Observe 与内核映射）/ S11-C（安全与一致性收口）按总览 §4.2 与 [`s11-coap.md`](../design/s11-coap.md) §4.2 推进。
- 冻结依据（[s11-coap.md](../design/s11-coap.md) §1/§3）：Observe（RFC 7641）天然「订阅→持续推送」，与 Broker pub/sub 同构；IoT/受限设备定位契合；纯 Erlang、**零外部依赖**、可嵌入（`coap_resource` behaviour）；已有 `rabbitmq-coap-pubsub` 桥接先例。
- **不引入第二套消息内核**：CoAP 资源路径→`broker_router` destination（ADR-010），Observe→订阅/游标（ADR-013），载荷→`broker_message`（ADR-009）+ 可选 CloudEvents `ce-*`（S3），认证→六协议统一 `broker_security`（S7），UDP 前端复用 S8-C 单 socket / 多会话经验。adapter 可配置门控、默认关闭（参照 S8-D `quicer`）。
- 其余 13 个候选**本轮均不进入实现**（归并 / 参考 / 延迟候选 / 不做，见 D2~D5）。

### D2. 归并既有阶段：三个候选收窄到既有阶段，不产生 S11 子阶段

| 候选 | 冻结结论 | 落点 |
|---|---|---|
| `gotthardp/lorawan-server` | **桥接**，不重写 NS | 优先让 Broker 作后端：复用其 Connector 已实现的 MQTT 3.1.1 / AMQP 0-9-1 / WebSocket（本项目已实现）。新增网关 UDP 前端是**后续独立决策**（见 OQ-4），本轮不立。归并路线与 S5/S8/S9/S10 同构 |
| `processone/stun` | **复用 S10 ICE**，不重复立项 | 其唯一交付是「依赖级复核结论」，**回灌 S10-0 OQ-3/5/6**（锁定版本、`add_listener/3` vs `/4` 签名、依赖审计）。S11 不另出结论（ADR-029 的 `broker_ice` 与手写编解码已吸收此面） |
| `inaka/apns4erl` | **归并 S9 出站 webhook** | APNs over HTTP/2 是出站推送、非接入协议；若做出站按 S9 Push Notification / 出站 webhook 路线（含 SSRF / 签名 / 防重放 / 重试-DLQ-TTL），不另立前端 |

### D3. SIP 一组并入 S10 信令面（ADR-028）评估，不单独立项

- SIP 三仓（`poroh/ersip` / `erlnets/nksip` / `fredrikt/yxa`）**本轮不做出「是否作为消息接入协议」的独立裁决**；该问题**并入 ADR-028 信令面边界**一起评估——SIP 与 WebRTC 信令同属「会话信令前端」，是否在既有 `broker_proto_webrtc` 之外另立 SIP 前端，取决于 ADR-028 信令边界是否需要扩展。
- **若** ADR-028 评估后决定做 SIP：入口候选为 **`poroh/ersip`**（纯函数库、零依赖、无 server/ETS/supervisor，最契合 ADR-001 的嵌入形态），届时**升级为深挖稿**（OQ-8）。`nksip`（重框架）/ `yxa`（历史基线）保持参考 / 不做。
- `mtproto_proxy`（端到端代理，非消息协议）归 D5 明确不做。

### D4. 发现面一组保留为**延迟候选**（不关闭、不实现）

- **用户裁决：链路本地发现保留为候选**，不采纳总览 §3.1「倾向只作参考」的收口口径。发现面一组（`shortishly/mdns` / `benoitc/dnssd_erlang` / `refuge/rbeacon` / `dnsimple/erldns` + DHT 两仓 `jlouis/dht` / `kevinlynx/dhtcrawler`）**状态 = 延迟候选（deferred candidate）**，不是「明确不做」。
- **延迟而非关闭的理由**：S4 已有 Metadata Raft / Membership / Fencing（ADR-021）解决**集群成员**问题，但「链路本地零配置发现 / 服务广告」与「集群成员」不是同一问题；当前没有证据证明 Broker 有 S4 解不了的发现需求，故**不在本轮实现**，也**不把它关闭**——保留到出现具体真实需求时再裁决。
- **升级的硬前置**（满足才从延迟候选转实现候选）：① 一个 S4 Membership 解不了的**具体真实问题**（在生产中观察到，非假设）；② 对候选仓的**新一次源码级深挖**（现有深挖稿覆盖的是协议事实，「是否需要」仍是开放问题）；③ 不违反 D6 的硬边界。
- **硬边界（即便将来升级也冻结）**：默认禁用 `mdns` 的 `MDNS_CAN_MESH`（自动组网会绕过共识层与 ACL，与 ADR-021 正面冲突）；**不得**用 DHT / mDNS / DNS-SD / DNS 提供「找到节点」的能力**替代 S4 成员真相**；不引入 `avahi-daemon` 等外部守护进程（ADR-016 无外部依赖）；不引入 `gproc` 作 Broker pub/sub（已有 `broker_subscription`）。

### D5. 明确不做（候选池与能力矩阵逐行留痕）

| 候选 | 冻结结论 |
|---|---|
| `seriyps/mtproto_proxy` | 端到端 TCP 代理（`dd` / Fake-TLS / 1443），非消息协议，**明确不做** |
| `kevinlynx/dhtcrawler` | BitTorrent DHT **爬虫 / 索引应用**（协议在外部 `kdht`），不含协议栈，**明确不做** |
| `fredrikt/yxa` | 历史 SIP 服务器套件（Mnesia + 分布式 Erlang + OTP R13B 基线），与 ADR-002/021 冲突，**明确不做**（历史参考） |
| 为任何候选写第二套消息内核 / 照搬完整服务成 `broker_proto_*` | **明确不做**（ADR-001） |

### D6. 跨候选的硬边界（冻结）

- **不写第二套消息内核**（ADR-001）；14 个候选无一需要新的内核抽象。
- **不用发现类 / DHT 候选替代 S4 Membership / Fencing**（ADR-021）。
- **可靠性显式分档，不得混淆**（ADR-007）：CoAP CON/ACK（datagram 级确认）、LoRaWAN FCnt（链路帧去重）、MQTT-SN QoS（会话级）是**不同层级**，禁止含糊表述为「可靠」或互相等价。
- **不引入外部守护进程**（`avahi-daemon` 等，ADR-016）、**不引入 `gproc` 作 Broker pub/sub**。
- **官方 CloudEvents Binding 逐字实现不立项**（S7 已结案「不立项」）。
- **不预先承诺任何性能 / 容量 / 规模数字**——所有数字须来自 S11 起落盘的实测报告。
- **装配不得破坏既有基线**：CoAP adapter 按 [`docs/architecture.md`](../architecture.md) §3.1 boot step 顺序接入，默认关闭 / profile 门控，既有全量 CT 零回归（本轮 S11-0 无代码，基线即 S10-D 收口的 1251/1251、79 套件）。

### D7. S11-0 的证据口径与状态同步

- **本轮只新增文档**：ADR-030 + 总览回填 + roadmap §13.1 S11-0 状态。不新增 app / 端口 / 代码路径。
- **状态行不同步**（刻意）：README / `progress.md` / `architecture.md` 的「S0–S10 已实现」状态行与 `docs/security.md` §12 清单**本轮保持现状**——S11-0 没有代码交付，待 S11-A 有首个代码交付时统一同步，避免把规划读成现状（与总览 §4.5 口径一致）。
- **Gate S11 全未勾选**：本 ADR 只冻结筛选结论，S11-A/B/C 的真实第三方客户端证据、容量报告、安全专项用例**尚未产出**。

## 后果（Consequences）

- **正向**：候选池收敛为「1 个实现（CoAP）+ 3 个归并 + 1 组延迟候选 + 3 个不做」，范围清晰、资源聚焦；发现面不被过早关闭也不被仓促实现；SIP 与信令面边界统一评估，避免两套信令前端各立门户。
- **代价**：发现面延迟候选在升级前**没有**产出——若生产真出现 S4 解不了的发现需求，需临时补一次源码级深挖与 ADR 评估（D4 硬前置已写明，属可控返工）。SIP 并入 ADR-028 意味着 SIP 的「做/不做」要等信令面边界下次评审才落定，本轮不拿到 SIP 的独立结论。
- **与既有 ADR 的关系**：不 amend 任何一条；D1 复用 ADR-001/009/010/013，D2 复用 ADR-029/027，D3 挂接 ADR-028，D4 复用 ADR-021/016，D6 复用 ADR-007/016。

## OQ 收敛表（S11-0 必须收敛的 9 问，逐条落定）

| 编号 | 问题 | S11-0 收敛结论 |
|---|---|---|
| OQ-1 | S11 只立 CoAP 还是同时推 LoRaWAN 桥接 | **只立 CoAP**（D1）。LoRaWAN 桥接归并、收窄为「优先后端 Connector」，网关 UDP 前端是后续独立决策，本轮不立 |
| OQ-2 | 是否需要发现面 | **保留为延迟候选**（D4）：不关闭、不实现；升级须满足 D4 硬前置（真实问题 + 新深挖 + 不违硬边界） |
| OQ-3 | 是否做 SIP | **并入 S10 信令面（ADR-028）评估**（D3），本轮不单独裁决；若做则入口为 `ersip` 并升级深挖稿 |
| OQ-4 | LoRaWAN 范围：后端 Connector 还是网关 UDP 前端 | **延迟**：先走后端 Connector（复用既有三协议）；网关 UDP 前端作为后续独立前端决策，本轮不立 |
| OQ-5 | `processone/stun` 锁定版本 / `add_listener` / 依赖审计回灌 | **回灌 S10-0 OQ-3/5/6**（D2），S11 不另出结论；与 ADR-029 的 `broker_ice` 共用一套结论 |
| OQ-6 | 14 仓依赖 / OTP 下限 / 构建体系与 OTP 28.3.1 兼容 | 分候选登记（各深挖稿 §2）；**进入构建 spike 范围的只有 CoAP**（[`s11-coap.md`](../design/s11-coap.md) OQ-3），其余随其裁决 deferred |
| OQ-7 | 各组候选与身份表 / ACL / 端口的装配顺序 | CoAP adapter 按 [`docs/architecture.md`](../architecture.md) §3.1 boot step、默认关闭 / profile 门控接入，不破坏既有基线（D6） |
| OQ-8 | 5 份索引级稿的深挖排期 | `ersip` **条件升级**（仅当 ADR-028 评估后做 SIP，D3）；`nksip`/`yxa`/`mtproto_proxy`/`apns4erl` 保持索引级 / 归并 / 不做 |
| OQ-9 | S11 与 S9 / S10 的先后 | S10 已于 2026-11-14 全部交付（S10-D 收口），S9 主体已交付；**S11 排在 S10 之后**，无硬阻塞，CoAP 复用既有身份表与前端经验 |

## 落地偏差记录

（S11-A 起逐条追加，格式：计划 → 实际 → 为什么 → 留痕位置。冻结文本不改。本轮 S11-0 无实现偏差。）

### S11-0（2026-11-15）

> 本轮无代码、无端口、无 app，只有文档收敛，因此无「实现 vs 计划」偏差。以下为相对总览规划稿的**裁决差异留痕**（不是实现偏差）：

1. **发现面从「倾向只作参考」改为「延迟候选」**
   - 计划（总览 §3.1 C 档）：发现面 4 仓 + DHT 2 仓「倾向只作参考」，§3.2 明列「不做」的部分边界。
   - 实际（D4）：保留为**延迟候选**——不关闭、不实现，附 D4 硬前置作为升级条件。
   - 为什么：S4 Membership 解决的是集群成员，链路本地服务发现是相邻但不同的问题；无证据证明有 S4 解不了的真实需求，过早关闭会丢失一个未来可能必要的选项，仓促实现又违反 ADR-021。延迟候选是既不关也不做的中间态。
   - 留痕：本 ADR D4、[`docs/roadmap.md`](../roadmap.md) §13.1 S11-0 行、[`s11-protocol-candidates.md`](../design/s11-protocol-candidates.md) §3 注记。

2. **SIP 从「S11-0 一组裁决」改为「并入 ADR-028 评估」**
   - 计划（总览 OQ-3 / §3.1 C 档）：SIP 一组在 S11-0 收敛，倾向不做、`ersip` 为入口。
   - 实际（D3）：SIP 的「做/不做」**并入 ADR-028 信令面边界**评估，本轮不单独立结论；`ersip` 条件升级深挖稿。
   - 为什么：SIP 与 WebRTC 信令同属会话信令前端，若各立门户会形成两套信令边界，违背 ADR-001「一个 adapter 一个边界」的收敛取向；统一在 ADR-028 评估更自洽。
   - 留痕：本 ADR D3、[`s11-ersip.md`](../design/s11-ersip.md) §1.2 注记。

- **结论**：ADR-030 全部决策在 S11-0 评审签认时冻结；14 份立项稿的 §2 协议事实保持不变，本 ADR 只冻结 UnifiedMQ 侧的筛选结论与 OQ 收敛。S11-A/B/C 是否开工、以何范围开工，待 CoAP 实现评审与（如需）ADR-028 信令面扩展评估后另行排期。
