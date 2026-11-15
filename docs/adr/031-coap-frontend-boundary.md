# ADR-031：CoAP 前端边界——自写编解码、Observe 归订阅层、CON/ACK 为 datagram 级确认

- **状态**：已采纳（S11-A 实现评审签认，2026-11-15）
- **日期**：2026-11-15
- **关联**：ADR-001（协议 Adapter 与消息内核分离）、ADR-007（显式可靠性三档）、ADR-009（`broker_message`）、ADR-010（`broker_router` destination）、ADR-013（订阅类型与 cursor 归属）、ADR-016（可观测最小集、资源上限、无外部依赖）、ADR-021（Metadata Raft / Membership / Fencing）、ADR-022（MQTT-SN 单 socket / 多会话——UDP 前端的直接对位）、ADR-029（ICE 组件——手写编解码的先例）、ADR-030（S11 候选池：仅 CoAP 进入实现评审）
- **关联文档**：[`docs/design/s11-coap.md`](../design/s11-coap.md)（§1 对位 / §3 R-* 需求 / §4.3 OQ-1~OQ-6）、[`docs/benchmarks/2026-11-15-s11a-gen-coap-spike.md`](../benchmarks/2026-11-15-s11a-gen-coap-spike.md)（OQ-3 构建 spike 实测）、[`docs/roadmap.md`](../roadmap.md) §13.1（S11-A/B/C）
- **事实来源**：`gotthardp/gen_coap` `master` @ `245e220`（README、`src/coap_resource.erl` 回调签名、`include/coap.hrl` 记录形状、`rebar.config`）；RFC 7252（CoAP）、RFC 7641（Observe）、RFC 6690（CoRE Link Format）、`draft-ietf-core-block-18`（仓库实际对齐的草案，非 RFC 7959）；OTP 28.3.1 实测（`pg2`/`http_uri` 均已移除）

## 背景（Context）

ADR-030 §D1 把 CoAP 定为 S11 唯一进入实现评审的协议。`s11-coap.md` §4.3 留了 6 个必须在写代码前冻结的问题（OQ-1~OQ-6）。其中 OQ-3 是经验性的——`gen_coap` 用旧式 rebar、DTLS 代码自 OTP 19.2 起、README 自述「API may change」，与 OTP 28.3.1 / rebar3 3.27 是否兼容必须实测，不能靠读 README 下结论。

本 ADR 只冻结 **adapter 的边界与形状**，不冻结 RFC 细节。经本 ADR 冻结后，S11-A 才写第一行代码。

## 决策

### D1. 自写 CoAP 报文编解码，不 vendor `gen_coap`（回答 OQ-3）

- **不原样 vendor `gen_coap`**：构建 spike 证明它在 OTP 28.3.1 上**编译通过但运行时核心不可用**——`http_uri`（4 处，OTP 23 移除）与 `pg2`（6 处，OTP 24 移除）均 `undef`，且 `coap_client` **没有 Observe 客户端 API**。详见 [`2026-11-15-s11a-gen-coap-spike.md`](../benchmarks/2026-11-15-s11a-gen-coap-spike.md)。
- **自写 `coap_codec`**（4 字节固定头 / Token / Options / payload；`observe` / `block1` / `block2` / `uri_path` / `uri_query` 等 option 编号照 RFC 7252 / 7641）。**借鉴** `coap.hrl` 的记录形状与 option 编号，**不复制**其源码。
- 规模判定：与既有两个自写编解码同量级——`stun_codec`（S10-C，ADR-029 D1 同样的理由）与 `mqtt_sn_codec`（S8-C）。**净新增运行时依赖保持 0**。
- 不引入 fork、不引入外部守护进程、不锁「无版本 master」。上游将来修好 OTP 28 也不回头 vendor（避免双份编解码）。

### D2. Observe 归订阅层：`Observe` → `broker_subscription` + cursor，不另立扇出机制（回答 OQ-1）

- **Observe 订阅不是新机制**：CoAP `Observe=0` 在 adapter 内映射为一次 `broker_subscription` 订阅 + cursor（ADR-013），`Observe=1` 取消订阅；通知来自 cursor 推进后的投递，**不由 adapter 自己维护观察者集合**。
- **禁止**照搬上游的 `pg2`/`pg` 观察者扇出：那是「进程组广播」，与 `broker_subscription` 的 cursor/retention/重投递语义是两套东西；并存会形成第二个订阅真相。
- **订阅状态归属**：订阅与 cursor 的所有权在订阅层（ADR-013 冻结口径），adapter 只持「CoAP 会话 ↔ SubRef 的对应」。**adapter 进程崩了订阅可重建**，不把 `{Port, IP}` 当持久会话键（UDP 无连接，S8-C 同一结论）。
- **通知的可达性**：UDP 无连接，客户端消失无 FIN。adapter 以「通知发送失败 + 客户端 `Observe=1` 或超时」结束订阅并释放 cursor；**不引入 keepalive 心跳协议**，靠 CoAP 自身的 CON 通知重传耗尽判定（见 D3）。
- CoAP 的 Observe 语义（`Observe` option 序号、`Max-Age`、通知的 `Observe` 递增）由 `coap_codec` 承载，**不写进内核**。

### D3. CON/ACK 是 datagram 级确认，与 Broker 业务 ACK 分档（回答 OQ-2）

- **两级确认必须分开表述**（ADR-007 显式分档）：
  - **CoAP CON/ACK**：**datagram 级**确认，证明「一个 UDP 报文到达了对端」，**不代表**消息已被 Broker 持久化、**不等于** MQTT QoS 1 的 PUBACK。
  - **Broker 业务 ACK**：`broker_message` 落盘后的提交确认（ADR-007 的 R1/R2/R3 档），由内核给出。
- **分档口径（冻结）**：
  - CoAP **NON** 消息 → 可靠性档 = **至多一次（at-most-once）**，不落盘承诺，不重传。
  - CoAP **CON** 消息 → adapter 负责 **datagram 重传**（RFC 7252 §4.2 的 ACK_TIMEOUT / MAX_RETRANSMIT），重传**只保证报文到达 adapter**；adapter 收下后按既有投递路径处理，**业务可靠性仍是内核那一档**（不因 CoAP 是 CON 就升格为持久化）。
  - **禁止**把 CoAP CON 等价成 QoS 1 的持久化 ACK，也**禁止**在能力矩阵里含混写「可靠传输」。
- CON 重传状态属 **adapter 的瞬时状态**，不落盘、不进 cursor；adapter 重启即丢（与「信令面不落盘」同族，参见 ADR-028 D1 的边界口径，但这里是 CoAP adapter 自己的 mask，不是信令）。

### D4. 资源路径直接映射 `broker_router` destination，不新开命名空间（回答 OQ-5）

- `coap://host/a/b/c` 的 path → destination `<<"a/b/c">>`（`/` 分隔，与既有 topic 过滤器的层级语义一致），**复用 `broker_router` 的 destination → partition / 订阅索引**（ADR-010）。
- **不新开 `$coap/...` 顶层命名空间**：CoAP 资源路径本身就是层级命名，与 topic 同构；另立 `$coap/` 会与 `$webrtc/` / `$ice/` 这类「非路由主体」混淆（那两个是 ACL 模式主体，不是 destination，见 ADR-028 D5 / ADR-029 D3）。
- **冲突消解**：与既有六协议的 destination 若撞名（例如 MQTT 也用 `a/b/c`），按既有 destination 命名空间规则处理——**同一个 destination 就是同一条路由**，这正是统一消息内核的价值；不因「来自 CoAP」而隔离。需要隔离的部署用 ACL 或不同 destination 前缀表达，adapter 不硬编码前缀。
- `/.well-known/core` 是 **adapter 本地实现**（CoRE Link Format 由 `coap_codec` 的 link-format 部分生成），不落内核、不占 destination。

### D5. DTLS 凭据接 `broker_security` 统一身份表；默认不自签（回答 OQ-6）

- `coaps://`（DTLS）证书与凭据来自 `broker_security` 的既有 TLS 材料（与 MQTT/WSS 同一份），**禁止**默认自签（上游示例用 `openssl req` 自签，那是 demo 不是部署）。
- DTLS 的 PSK 模式下，PSK identity ↔ `broker_security` 的 `users` 表映射；证书模式下走既有 CA 链校验。**不引入 KMS、不新建凭据存储**。
- **默认关闭**：`broker_proto_coap` 出厂 `enabled => false`、零 socket（照 `broker_ice` / `broker_proto_webrtc` 的出厂姿态），监听配置由 release config 给出（照 `broker_proto_mqtt_sn` 的 `listeners => []`）。

### D6. 默认关闭、profile 门控、按 boot step 接入（回答 OQ-7 的一部分）

- 新 app `broker_proto_coap`：`applications` 只依赖 `kernel, stdlib, sasl, broker_core, broker_security, broker_metrics, broker_router, broker_queue, broker_subscription, broker_event`（与 `broker_proto_mqtt_sn` 同族），**不依赖** `broker_proto_webrtc` / `broker_ice` / `ranch`。
- 出厂 `listeners => []`（零 socket）、`enabled => false`；接入顺序照 [`docs/architecture.md`](../architecture.md) §3.1 boot step，**不改既有 boot 顺序**。
- `broker_metrics` 复用既有键（`connections_current` 等），**不扩冻结键集**（ADR-016）；CoAP 专属失败路径只 `logger`，姿态与 S9-F / S10-D 一致。

### D7. Block-wise 如实标注对齐草案（回答 OQ-4 的口径部分）

- 首期按 **`draft-ietf-core-block-18`** 实现（与上游一致），**能力矩阵必须写明是草案、不是 RFC 7959**，不得声称「完整 RFC 7959」。
- 互操作（与 libcoap 的分块往返）由 S11-A 的**真实第三方客户端证据**证明，不由本 ADR 假定。若实测发现草案与 libcoap 现行实现不互通，则在 S11-A 落地偏差记录里如实标注，并决定「降级为不分块」还是「对齐 RFC 7959」。

## 后果（Consequences）

- **正向**：不引入 fork、不引入新依赖、不引入外部守护进程；Observe 复用既有订阅层，不形成第二套订阅真相；CON/ACK 与业务 ACK 的分档从一开始就写清，避免能力矩阵含混。
- **代价**：要自己写 CoAP 编解码（4 字节头 + 十几种 option）。这是可控成本，且有两个先例（`stun_codec` / `mqtt_sn_codec`）；但 Block-wise 与 Observe 通知的边界情况需要测试逐条钉死，工作量压在 S11-A/B。
- **与既有 ADR 的关系**：不 amend 任何一条；D5 复用 ADR-021 的 TLS/身份表口径，D2 严格服膺 ADR-013 的 cursor 归属，D3 严格服膺 ADR-007。

## 落地偏差记录

（S11-A 起逐条追加，格式：计划 → 实际 → 为什么 → 留痕位置。冻结文本不改。）

### S11-A 前置（2026-11-15）

> 本 ADR 在 S11-A 写代码前冻结。以下为相对 `s11-coap.md` 规划稿的**裁决差异留痕**（不是实现偏差，因为还没有实现）：

1. **OQ-3：从「是否 vendor」改为「不做 vendor，自写编解码」**
   - 计划（`s11-coap.md` OQ-3）：「构建 spike + 依赖审计」，隐含「先试 vendor、必要时打补丁」。
   - 实际（D1）：spike 实测证明 `gen_coap` 在 OTP 28.3.1 **运行时核心不可用**（`http_uri` 4 处 + `pg2` 6 处 + 无客户端 Observe API），因此**不 vendor**，自写 `coap_codec`。
   - 为什么：vendor 一个「无版本 master + 6 处移除 API + 无客户端 Observe」的 fork，成本高于自写同量级编解码；且 Observe 必须按 ADR-013 的订阅模型写，上游的 `pg2` 扇出不能复用。与 ADR-029 D1「手写 STUN/TURN 编解码」同一判断路径。
   - 留痕：[`docs/benchmarks/2026-11-15-s11a-gen-coap-spike.md`](../benchmarks/2026-11-15-s11a-gen-coap-spike.md) §1/§3/§4、本 ADR D1。

2. **Observe 状态归属：从「进程 / ETS / 控制面」三选一，定为「订阅层 + cursor」**
   - 计划（`s11-coap.md` OQ-1）：在「进程 / ETS / 控制面」之间选。
   - 实际（D2）：都不是——Observe 直接归 `broker_subscription` + cursor（ADR-013），adapter 只持会话↔SubRef 映射。
   - 为什么：把 Observe 当「新订阅机制」会形成第二个订阅真相；ADR-013 已经冻结 cursor 的所有权，「进程 / ETS / 控制面」三选一本身就问错了层。
   - 留痕：本 ADR D2、`docs/design/s11-coap.md` §3.2 R-O2/R-O3。

- **结论**：ADR-031 的 D1~D7 在 S11-A 开工前冻结；S11-A 的首个代码交付须逐条对照本 ADR，任何偏离记入本节。
