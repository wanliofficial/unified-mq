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

### S11-A 首个代码交付（2026-11-15）

> 新 app `broker_proto_coap` 落地（`coap_codec` / `coap_resource` / `coap_listener` / `broker_proto_coap_app`），CT 73 例、全量 `rebar3 ct` 1324/1324 零回归、dialyzer 0 warning。**D1~D7 逐条成立**，没有需要推翻的边界；以下是 D 条文没写死、实现时必须选择的地方，以及本期**没有**落地的 D 条目。

1. **D7 的 Block1 重组键：`{IP, Port, token, destination}`，不是 Message ID**
   - 计划（D7）：只说「按 `draft-ietf-core-block-18` 实现分块重组」，没有指定流的键。
   - 实际：键是**请求 token + 目的地**（再加端点四元组）。Message ID 每个分块都变，用它当键会让每个分块各开一条流，重组永远只剩最后一块。
   - 为什么：草案 §2.1 用 token 标识一次 block-wise 请求；这也是 `coap_listener_SUITE` 的分块用例特意让两块用**不同 Message ID、同一 token** 的原因（自编对端用同一 ID 会把这个错误藏起来）。
   - 留痕：`apps/broker_proto_coap/src/coap_listener.erl` 的 `block1_stream/5`、`docs/protocol-compatibility.md` §4.24.3。

2. **D6 的「有界簿记」具体化为三张表 + 1 s tick**
   - 计划（D6）：只要求「出厂零 socket、metrics 不扩容、同 MQTT-SN 族的 app 依赖」；ADR 正文提到 dedup/Block1 是 listener 内的有界状态，但没有给出界的形式。
   - 实际：CON dedup（`max_dedup_entries` + `dedup_window_ms`）、端点令牌桶（`max_requests_per_second` + `max_endpoints`，表满时**拒绝新来源**而不是驱逐已有桶）、Block1 缓冲（`max_block1_bytes`）；三张表都在 1 s tick 上按 `dedup_window_ms` 清扫。
   - 为什么：UDP 源地址可伪造，任何以源地址为键的表都必须有硬上限，否则一个伪造源地址的洪泛就能把 listener 的堆吃干净（与 §3.11 的 `{active, N}` 同一判断）。
   - 留痕：`coap_listener.erl` 的 `allowed/4`、`remember/6`、`prune/1`；`docs/architecture.md` §3.18。

3. **listener 不注册进程名（原计划是 `coap_listener_<Port>`）**
   - 计划：按 MQTT-SN/TCP listener 的习惯给子进程注册一个以端口命名的原子。
   - 实际：**不注册名字**，`start_link/1` 直接 `gen_server:start_link(?MODULE, Spec, [])`；测试从 `supervisor:which_children/1` 取 pid 后调 `coap_listener:port/1` 读回实际端口。
   - 为什么：`port => 0` 时每个 listener 都想要 `coap_listener_0`，第二个就 `already_started`；而端口在 `gen_udp:open/2` 之后才知道，名字里没法写真实端口。**运维面没有因此丢信息**——`port/1` 一直存在。
   - 留痕：`coap_listener.erl` 的 `start_link/1`；`coap_listener_SUITE.erl` 的 `listener_port/0`。

4. **分块流的拒绝码：超限 `4.13`，中间块无流可续 `4.08`**
   - 计划（D7）：草案允许「拒绝过大的 body」，但没指定「收到 Num>0 却没有流」时怎么办。
   - 实际：超 `max_block1_bytes` → `4.13 Request Entity Too Large` 且丢弃该流；Num>0 且找不到流 → `4.08 Request Entity Incomplete`，**不**把这一块当成完整 body 去发布。
   - 为什么：把半截 body 当完整消息发布，比拒绝更糟——客户端以为成功，订阅者收到截断数据。两种拒绝都在草案的错误码集合内。
   - 留痕：`coap_listener.erl` 的 `block1_stream/5`、`assemble/11`。

5. ~~D5（DTLS 接 `broker_security`、默认不自签）在 S11-A 未落地~~ —— **不是偏差，是分期**
   - 计划（D5）：DTLS 接 `broker_security`，默认不自签。
   - 实际（S11-A）：只有明文 UDP，`principal` 恒 `undefined`，ACL 只在 `acl => allow` 下有意义。
   - 为什么：ADR 把 DTLS 放在 S11-C 的收口范围；本期的验收面是「编解码 + 资源映射 + UDP listener」，把 DTLS 一起做会让首个交付无法在一天内形成可核对的证据。**能力矩阵已如实写「无 DTLS」**，没有把计划读成现状。
   - 留痕：`docs/protocol-compatibility.md` §4.24.4、[`docs/roadmap.md`](../roadmap.md) §13.1 S11-C 行。

- **对照结论**：D1（自写编解码）、D2（Observe 归订阅层，本期未实现通知）、D3（CON/ACK 是 datagram 级、NON 不产生 piggybacked ACK）、D4（资源路径直映射 destination，不新开 `$coap/`）、D6（零 socket 出厂、依赖同族、metrics 不扩容）在首个交付里成立；D5 与 D7 的 Observe/互操作两半分别留 S11-C 与 S11-B/第三方证据。**冻结文本未改**，本节只追加落地事实。

### S11-B 首个代码交付（2026-11-15）

> Observe（RFC 7641）落地：`coap_observe`（纯函数：关系表 / 序列 / `Max-Age` 窗口 / 通知报文形状）+ `coap_listener` 的 notify 侧（注册 / 通知 / 重传 / 注销 / 清扫）。CT **96 例**（CoAP 五套）、全量 `rebar3 ct` **1347/1347、84 套件**零回归、`broker_subscription_SUITE` 53/53、dialyzer 0 warning、xref **302**（零新增）、`rebar3 release` 装配成功。**D1~D4/D6 逐条仍成立**；以下是 D2/D3 条文没写死、实现时必须选择的地方。

1. **D2 的「adapter 只持会话↔SubRef 映射」具体化为「listener 自己就是 notify pid」**
   - 计划（D2）：Observe 归 `broker_subscription` + cursor，adapter 只持映射；没有指定谁当 consumer 进程。
   - 实际：**不新开进程、不建 `simple_one_for_one`**——`coap_listener`（S11-A 的唯一 UDP socket owner）直接 `attach/3` 为 consumer（`persist=false`），`handle_info({broker_deliver,…})` 里对 destination 匹配的每条关系各发一条通知。CON 重传的 timer 状态用 `pending` map 存在 listener state 里。
   - 为什么：通知发送只是一次 `gen_udp:send`，为它单开监督树是纯开销；`{active,100}` 的**收包**预算也不受影响（通知是发送不是接收）。代价是 UDP 编码在 listener 串行——通知速率受对端窗口与 `max_requests_per_second` 约束，受限设备场景不构成瓶颈。与 S11-A「单 socket owner 持有全部有界状态」的姿态一致。
   - 留痕：`coap_listener.erl` 的 `register_observe/6`、`dispatch_notifications/2`、`notify_one/5`；`docs/architecture.md` §3.18；`docs/design/s11b-coap-observe.md` §2。

2. **ClientId 固定 `<<"coap">>`：所有观察共用一个匿名内核会话，filter 才是区分粒度**
   - 计划（D2/D4）：只说「归 `broker_subscription`」「资源路径即 destination」（OQ-5），没指定 ClientId。
   - 实际：`?COAP_CLIENT = <<"coap">>`；`subscribe(<<"coap">>, Destination, 0, Tenant)`，`coap_observe:filter/1` 就是恒等（destination 本身）。
   - 为什么：UDP 无连接、无会话，为每个端点铸一个 ClientId 会凭空造出「伪会话」并让 `detach`/超时语义与真实身份脱钩；一个匿名 ClientId + 每关系一个 filter 已经够表达「谁在看哪个目的地」。代价——`detach/2` 会拆掉该 ClientId 的**全部**订阅，因此必须 `maybe_detach/1` 只在关系表空时调用；同理 `unsubscribe` 必须 `maybe_unsubscribe/2` 只在无人再观察同一 destination 时调用。这两级粒度是首版缺陷（见本条 3/4 与 §4.24.7）。
   - 留痕：`coap_listener.erl` 的 `?COAP_CLIENT`、`maybe_unsubscribe/2`、`maybe_detach/1`；`docs/protocol-compatibility.md` §4.24.6。

3. **D3 的「可确认通知靠通知自己的重传」具体化为 `pending` 键 `{IP,Port,MessageId}`（不是 token），且 CON 通知**总是**进入 `pending`**
   - 计划（D3）：CON 通知靠重传、空 ACK 确认；没有指定关联键。
   - 实际：`pending[{IP,Port,MsgId}] = {关系键, 已发字节, 剩余次数, timer}`。符合 RFC 的空 ACK（§4.1）与空 RST（§4.5）都是 **TKL=0、不带 token**，只有回显的 Message ID 能把它们配到某条通知上；值里再存关系键，RST 才能注销对应关系。**CON 通知一律进 `pending`**（不是「仅在超时后才记」），否则通知发出到 ACK 到达之间的空 ACK 无处可配。
   - 为什么：首版按 token 关联，真实客户端的空 ACK **永不命中**，每条观察在 `1 + MAX_RETX` 帧后被自己拆掉——D3 的可靠性目标整体失效（§4.24.7 第 1 条）。NON 通知**不进** `pending`：至多一次，不重传、不超时。
   - 留痕：`coap_listener.erl` 的 `notify_one/5`、`retransmit_one/3`、udp 子句的空 ack/rst 分支；`docs/protocol-compatibility.md` §4.24.6。

4. **NSTART=1（§4.5.1）是 `pending` 有界的唯一手段**
   - 计划（D3）：未提并发通知的窗口。
   - 实际：同一关系的新 CON 通知**先取消**仍未确认的旧通知（`cancel_pending_map/2` 按完整 `{IP,Port,token}` 作用域），再登记新的。`pending` 因此恒 ≤ 关系数，且每条关系只有一份重传预算。
   - 为什么：不做的话每次 publish 都给慢客户端新增一条未确认项，表涨到与 publish 速率成正比（这正是首版第二个缺陷）；同时「一个 timer 触发 fold 全表重发并扣减所有关系预算」也是错的——timer 消息带 `{observe_ack, IdKey}`，`retransmit_one/3` 按 **key + timer ref 双重匹配**精确定位，陈旧 timer 直接忽略。
   - 留痕：`coap_listener.erl` 的 `notify_one/5` con 分支、`arm_retransmit/2`、`retransmit_one/3`；§4.24.6 的 NSTART 行。

5. **RST 关联必须限定在发送者端点 `{IP,Port,_}`**
   - 计划（D3/§4.5）：只说「RST 取消关系」。
   - 实际：`observe_by_notify_id/4` 在 `observes` 里按 `req_id` 匹配**且** `when SIP =:= IP, SPort =:= Port`。另外该函数用 `maps:get(req_id, Rel, undefined)` 而非硬匹配——刚注册、尚未收到第一条通知的关系（GET-Observe 到下次 publish 之间的**稳态**）没有 `req_id` 键。
   - 为什么：Message ID 是 **16-bit**，可枚举也可观测；不限定端点就等于允许任一客户端猜到/看到一个 ID 就拆掉别人的观察（跨客户端 DoS）。而硬匹配会让一个**未认证**来源的空 RST 在稳态下打死 listener（`function_clause` → one_for_all 1/5s 重启）。`relationship()` 类型补 `req_id => non_neg_integer()` 让 dialyzer 能捕获此类回归；`observe_rst_is_endpoint_scoped` 是这两个守卫的回归护栏（移除 `when` 即失败）。
   - 留痕：`coap_listener.erl` 的 `observe_by_notify_id/4`、`deregister_observe/4`；`coap_observe.erl` 的 `relationship/0`；§4.24.7 第 4/5 条。

6. **`Max-Age` 兜底窗口：每关系取客户端 `Max-Age`，钳到 `[1 s, observe_max_age_s]`**
   - 计划（§3.2/§3.3）：`Max-Age` 作为非确认兜底超时，默认 30 s。
   - 实际：`coap_observe:sweep_window_ms(ClientAgeS, CapS) = min(max(ClientAgeS,1), max(CapS,1)) * 1000`；`observe_max_age_s`（默认 30）是**上界**，1 s 是**下界**；在 1 s refill tick 上由 `sweep_observes/1` 清扫（有副作用——要 `unsubscribe`，所以不能放进纯的 `prune/1`）。
   - 为什么：没有下界，客户端一个 `Max-Age: 0` 就能在下一 tick 杀掉自己的观察；没有上界，一个 `Max-Age: 604800` 就能白占一个表槽一周。**时间戳由 listener 传入**（`register/5` 的 `NowMs` / `next_seq/2`），因为本 VM 上 `erlang:monotonic_time(millisecond)` 是负数，用 0 当哨兵会让「从未通知过」的关系永不过期。
   - 留痕：`coap_observe.erl` 的 `sweep_window_ms/2`、`expired/3`；`coap_listener.erl` 的 `sweep_observes/1`；`coap_observe_SUITE` 的 `sweep_window_is_clamped` / `expired_needs_the_window_to_elapse`；§4.24.4、`docs/security.md` §12.5 第 6 条。

7. **`observe_acl_denied` 降级为 `observe_plain_udp_principal_is_allowed`——4.03 只剩容量路径**
   - 计划（§4.24.2 的 ACL 行 + 设计稿 §7 的用例名）：ACL 拒绝时回 `4.03`。
   - 实际：plain UDP 下 `principal = undefined`，`broker_security:authorize/2` 在 `acl => allow` 时直接放行，**构造不出** ACL 导致的 4.03；用例改名为 `observe_plain_udp_principal_is_allowed`，如实钉住「ACL 受限部署下仍然可观察」。S11-B 唯一的拒绝路径是**容量**（`max_observes` 满 → `4.03`，`observe_bounded_table_refuses`）。把 `coap`/`coaps` 列进 `require_auth` 会让 app **拒绝启动**（`coap_app_SUITE` 钉住），所以连「启动即收紧」的路都不通。
   - 为什么：这不是实现偏差，是**分期**——ACL 需要一个可识别的主体，而主体要等 S11-C 的 DTLS 客户端身份（D5）。把用例名和文档口径改成现状，好过留一条永远绿的假用例。
   - 留痕：`coap_listener_SUITE` 的 `observe_plain_udp_principal_is_allowed`、`observe_bounded_table_refuses`；`coap_app_SUITE` 的 `require_auth_for_coap_is_refused` / `require_auth_for_coaps_is_refused`；§4.24.4、§4.24.7、`docs/security.md` §12.5 第 5 条。

- **对照结论**：D2（Observe 归订阅层、adapter 不写第二套内核）、D3（CON 通知是 datagram 级重传、NON 至多一次、**都不提供恰好一次**）、D4（filter 即 destination）在 S11-B 成立；D6（metrics 不扩容、有界簿记）从三张表扩到五张表但姿态不变。**D5（DTLS）与跨节点/崩溃恢复仍不在本期**，D7 的 Block2 也未做。**冻结文本未改**，本节只追加落地事实。

### S11-C 首个安全交付（2026-11-16）

> DTLS 1.2（PSK）落地：`coap_listener` 新增 `transport => dtls` 分支 + 新模块 `coap_dtls_acceptor`，身份 → `principal`/`tenant`、ACL 生效、会话/每主体双上限、六表关闭清理。CT 112 例（CoAP 六套，新增 `coap_dtls_SUITE` 14 例）、全量 `rebar3 ct` 1378/1378〔**注**：该数取自含 S11x router 测试的树，main 上是 1372/1372；见本节末「互操作证据补充」〕、dialyzer 0、容量报告落盘。**D1~D7 仍然成立**；以下是 D 条文没写死、实现时必须选择的地方，以及本期**没有**落地的 D 条目。

1. **D5 的「DTLS」具体化为 PSK-only、cert-free，且 realm 放在 `broker_security.coaps` 而不是 `broker_security_tls`**
   - 计划（D5）：只说「接 `broker_security`、默认不自签」，隐含「DTLS 可能带证书」。
   - 实际：`ssl:listen` **不加载任何证书**，只启用 PSK 套件 + `verify => verify_none`；PSK 由 `broker_security:psk_for_identity/1`（新增导出）从 `users` 表派生。realm 单独放 `broker_security.coaps = #{realm => binary()}`，缺省 `<<"unifiedmq">>`。
   - 为什么：S11-C 的范围明确是「DTLS 凭据仅 PSK」（证书/mTLS 是已决非目标）。把 realm 塞进 `broker_security_tls` 会污染那个**证书导向的封闭 map**、并向 dialyzer 撒谎（类型是封闭的），所以单开 `coaps` 语义自洽。口令不过线（服务端派生）与 TURN `long_term_key/2` 同一先例。
   - 留痕：`broker_security.erl` 的 `psk_for_identity/1`、`coaps_realm/0`；`coap_listener.erl` 的 DTLS 分支；`docs/protocol-compatibility.md` §4.24.8、`docs/security.md` §12.6。

2. **D5 的「接 `broker_security`」具体化为「串行握手 + 身份在 `user_lookup_fun` 里归属」**
   - 计划（D5）：只说「接 `broker_security`」，没有说握手能不能并发。
   - 实际：一个 `coap_dtls_acceptor` **串行**跑 `transport_accept → handshake → drain identity → offer → controlling_process`。PSK identity 只能由 `user_lookup_fun` 观察到，该回调**同步**跑在 `ssl:handshake/2` 内、把消息投给持有回调状态的进程；并发握手时无法判断一条 identity 消息属于哪条连接。
   - 为什么：实测并发握手会让被拒绝的身份**串进下一条连接**——这是一个身份/租户串号的正确性缺陷，不是性能问题。因此串行化是**身份归属的决定性约束**，代价是握手速率约 41 次/秒、同时上线设备落入 DTLS 客户端重传退避（容量报告 §3.5）。OTP 目前不提供「回调能拿到连接标识」的接口，所以没有并发的正解。
   - 留痕：`coap_dtls_acceptor.erl` 的模块 doc 与 `accept_loop/4`；`docs/benchmarks/2026-11-16-s11c-dtls-capacity.md` §3.1/§3.5。

3. **`coaps` 进入 `broker_security:credential_capable/0`，`require_auth` 从「一刀切拒启」改为分协议门控**
   - 计划（D5 + S11-B 遗留）：`require_auth` 含 `coap`/`coaps` 时 app 拒绝启动（S11-B 的 `auth_unsupported/0`）；D5 没有说 `coaps` 是否算「能带凭据」。
   - 实际：`credential_capable/0` 追加 `<<"coaps">>`（与 `mqtt`/`mqtts`/`wss`/`turn` 同姿态）；`broker_proto_coap_app:guard/0` 分协议判定——`coap` 仍 `{error, {require_auth_unsupported, coap}}`（明文无法承载凭据），`coaps` 则要求**所有** listener 都是 DTLS，否则 `{error, {require_auth_needs_dtls, coaps}}`。
   - 为什么：S11-B 的一刀切在 DTLS 落地后是错的——`coaps` 恰恰**能**承载凭据。门控必须区分「明文不可能有凭据」与「要求凭据但配了明文 listener」两件不同的事，否则用户得到一个无法诊断的拒启。
   - 留痕：`broker_security.erl` 的 `credential_capable/0`；`broker_proto_coap_app.erl` 的 `guard/0`；`coap_app_SUITE` 的 `require_auth_for_coaps_is_allowed_when_dtls` / `coaps_requires_dtls_listeners` / `dtls_without_users_is_refused`。

4. **未知身份回诱饵密钥，不发 TLS alert（对 D5「默认不自签」的安全姿态补充）**
   - 计划（D5）：只讲「不加载证书」，没讲失败形态。
   - 实际：未知身份 → 返回 `SHA-256("unifiedmq-coaps-psk-decoy" ‖0‖ 每 listener `crypto:strong_rand_bytes(32)` ‖0‖ Identity)`，让握手走到 Finished 校验失败；**不**发 alert。
   - 为什么：实测未知身份 alert 在 20–200 ms 返回，「用户存在+口令错」走 Finished 校验失败 → 客户端 3 s 超时，两者**可区分** ⇒ 用户名枚举 oracle。诱饵密钥把两条路径的错误形态拉平（`unknown_identity_is_refused` 断言错误项完全相等）。这是一条**安全修复**，不是新增范围。
   - 留痕：`coap_listener.erl` 的 `decoy_psk/2`；`coap_dtls_SUITE` 的 `unknown_identity_is_refused`；`docs/design/s11c-coap-dtls.md` §4.4。

5. **`max_dtls_sessions` / `dtls_max_sessions_per_principal` 在 listener 侧判定，acceptor 不做容量判定**
   - 计划（D6）：要求「有界簿记」，没有说上限归谁。
   - 实际：两个上限都在 listener 的 `admit_session/4` 判定（它持有五张表的真相）；acceptor 只负责握手与移交。移交失败/超时补发 `{dtls_abandoned, Sock}`，listener 用 `drop_session/2` 释放——否则记录的唯一清理触发 `{ssl_closed,_}`（需要 listener 持有 socket）永不到来。
   - 为什么：容量是「表」的属性，acceptor 看不到表；把判定放在 acceptor 会需要跨进程同步，而 `{dtls_abandoned}` 这条补丁正是「只记不释放」的必然结果（规格复审 B1）。表从 S11-B 的五张扩到六张（+`principal_sessions`），姿态不变。
   - 留痕：`coap_listener.erl` 的 `admit_session/4`、`drop_session/2`、`dtls_abandoned` 分支；`coap_dtls_SUITE` 的 `dtls_session_cap` / `dtls_per_principal_cap` / `dtls_abandoned_releases_slot` / `dtls_close_cleans_up`。

- **对照结论**：D2（Observe 归订阅层）、D3（CON 通知是 datagram 级重传、不是 QoS 1）、D4（filter 即 destination）、D6（metrics 不扩容、有界簿记）在 S11-C 继续成立；D5（DTLS）**按 PSK-only 交付**，证书/mTLS 是已决非目标；D7（Block1、无 Block2）不变。**D5 的「DTLS」不再是「未做」**；libcoap 真实客户端证据当时单列推迟，**已由下一节补齐**。**冻结文本未改**，本节只追加落地事实。

### S11-C 互操作证据补充（2026-11-16）

> `test/protocol-compatibility/s11c/libcoap/` 落盘 **libcoap v4.3.5 `coap-client`** 对真实 DTLS-PSK 监听器的证据（8/8 PASS），**据其修正两条 Block1 实现缺陷**。CT **113 例**（CoAP 六套）、全量 `rebar3 ct` **1373/1373、84 套件**零 FAILED。D1~D6 不变；**D7 的「草案对齐」一半被实测推翻**——决定互通的是 RFC 7959 的报文语义，不是草案编号。

1. **D7 的 Block1 应答块号：从「回 `Num + 1`」改为「回收到的 `Num`」**
   - 计划（D7）：只说按 `draft-ietf-core-block-18` 实现分块重组，没写 `2.31 Continue` 的块号语义；实现时读成「已收到，请给下一块」，回 `Num + 1`。
   - 实际：**回收到的 `Num`**。RFC 7959 §3.2 图 7 中请求 `1:0/1/128` 的应答是 `1:0/1/128`——`NUM` 确认的是**刚存下**的那一块。
   - 为什么：自编对端按 `Num + 1` 期待，所以一直绿；libcoap 严格比对，`Num + 1` 被当成协议错位。`coap_listener_SUITE` 的 `block1_reassembles_a_multidatagram_body` 现断言 `{0,true,16}`，把修复 revert 即红。
   - 留痕：`coap_listener.erl` 的 `continue_reply/3`、`block1_stream/5`；`docs/protocol-compatibility.md` §4.24.9.1。

2. **D7 的流键：从「token」改为「`Request-Tag` 优先、缺省 token」**
   - 计划（S11-A 落地时）：键为「请求 token + 目的地 + 端点四元组」，依据是草案 §2.1「token 标识一次 block-wise 请求」。
   - 实际：**libcoap 每块换 token**（block 0 空 token、block 1 八字节），跨块稳定的只有 `Request-Tag`（RFC 9175 §3.1，option 292）。键改为「有 `Request-Tag` 用它，否则退回 token」。
   - 为什么：只删 token 会让**同端点并发**的两条分块流串在一起；`Request-Tag` 正是 RFC 9175 为此设的标识。`coap_codec` 对未知 option 保留整数键与原始 binary，292 可直接透传，无需改编解码。护栏 `block1_keyed_by_request_tag_across_changing_tokens` 把「换 token、同 `Request-Tag` 必须重组」钉死。
   - 留痕：`coap_listener.erl` 的 `block1_tag/1`、`?REQUEST_TAG`；`coap_listener_SUITE` 同名用例；§4.24.9.1。

3. **`block_size` 从未被读取，「Block1/2 的 SZX 单元」是空声明**
   - 计划（D7 + `docs/operations.md` §1.8 的配置示例）：`block_size` 是分块的 SZX 单元。
   - 实际：**没有任何代码读它**；Block1 用的是客户端给的 SZX。配置注释已改为「预留（Block2 未实现，当前无效果）」。
   - 为什么：一条不起作用但看起来会起作用的配置，比没有配置更坏——运维改它、行为不变、就开始怀疑别的东西。
   - 留痕：`docs/operations.md` §1.8。

4. **Block2 仍不做，且「响应上限 `max_message_bytes`（65535）」是错的**
   - 计划（D7）：不做服务端 Block2，未给端到端可用上限。
   - 实际：响应不分块、整条一个数据报。libcoap 默认接收 PDU 1500 B，**超过约 1.1 KB 的响应被客户端丢弃**（`coap_pdu_resize: pdu too big`）；实测 1100 B 可读、1150 B 起失败。
   - 为什么：把**服务端**配置当成端到端上限，会让读者以为 2 KB payload 能读回。事实是写得进去、读不回来。
   - 留痕：§4.24.4；`docs/operations.md` §1.8；`test/protocol-compatibility/s11c/libcoap/README.md`。

5. **负向证据的形态：静默丢弃，不是 alert**
   - 计划（D5 第 4 条）：未知身份回诱饵密钥，让握手走到 Finished 校验失败。
   - 实际：真实 libcoap 上「错 PSK」与「未知身份」都表现为**无响应**（服务端先发 identity hint，之后不再回应），客户端超时；两者不可区分。
   - 为什么：这正是 D5 第 4 条想要的形态，现在由本仓库不控制的实现对**外部**确认了一遍——诱饵密钥不只对自编对端成立。
   - 留痕：`coap_dtls_SUITE` 的 `unknown_identity_is_refused`；`libcoap_compat.py` 的 `coaps-wrong-psk` / `coaps-unknown-identity`。

- **对照结论（补充）**：D1（自写编解码）被证据反向确认——`coap_codec` 从未充当客户端，全部互通结论来自本仓库控制的实现之外。D5 的 PSK-only 姿态在真实客户端上成立。**D7 的两条 Block1 语义按 RFC 7959 校正，Block2 仍未做**，且已作为限制写进能力矩阵。「libcoap 证据单列推迟」这一状态**结束**。**冻结文本未改**，本节只追加落地事实。
