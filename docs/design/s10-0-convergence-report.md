# S10-0 收敛报告 — WebRTC 信令与 ICE 接入

- **对应**：[`docs/design/s10-webrtc-signaling.md`](s10-webrtc-signaling.md) §4.4（OQ-1~OQ-10）、§3（候选需求）、§1.3（非目标）；[`docs/roadmap.md`](../roadmap.md) §12。
- **性质**：**收敛报告草案**。所有结论基于仓库内可核对的代码/配置与上游包元数据，未写实现代码、未开端口、未新增依赖。
- **基线**：S9-F 已交付，`rebar3 ct` **1063/1063、68 套件**，`dialyzer` 0 warning（2026-11-12）。
- **配套**：[`docs/design/s10-execution-plan.md`](s10-execution-plan.md)——S10-0~S10-D 的任务拆分、测试矩阵、证据计划、端口·配置·依赖表、风险回退与待填门禁表。
- **签认状态**：本文 §3 的两份 ADR（028/029）已于 2026-11-13 签认并落盘 `docs/adr/`；§5 入口条件第 1 条已满足，S10-A 可开工（下一个动作是 §5 第 2 条的 spike）。

---

## 0. 结论速览

| OQ | 问题 | 结论 | 决定性依据 |
|---|---|---|---|
| OQ-1 | 信令是否走统一消息内核 | **不走**。数据面（SDP/candidate）与五类服务器事件都是纯内存扇出，**不产生 `#broker_message{}`**、不进 `broker_router`/`broker_queue` | `broker_queue:append/3` 无 transient 旁路（`append_checked` → `do_append` → `Backend:append`），WAL 编码器原样保留 `persistence` 字段 → 「transient + os_buffer」仍落盘，与 roadmap「瞬时转发、默认不落盘」冲突 |
| OQ-2 | 是否落盘 / TTL / 可靠性档 | **不落盘、无 TTL、无 retention、无 SubRef cursor**；由 OQ-1 推出，并用测试证明（WAL offset 不增长、`.cursors` 无新增） | 同上 + SDP 含内网地址/指纹（R-X6/OQ-9） |
| OQ-3 | 前端选型与依赖 | `ranch 2.3.0` + `broker_ws`（`ws_handshake`/`ws_frame`）+ OTP 28 内置 `json`；**不引入** cowboy / syn / jsx / stun | cowboy 2.0.0 的 hex 元数据要求 `ranch ~> 1.4.0`（与本仓 2.3.0 冲突）；`stun 1.0.20` 额外拖 `fast_tls` + `p1_utils` |
| OQ-4 | 房间成员 / peer 注册表 | 新 `webrtc_room_registry`：`phash2(Room, N)` 分片 gen_server + 每分片 ETS；**单节点**、无 Mnesia、无集群一致 | 与 `a2a_task_registry`（S9-B）同构；ADR-002「Mnesia 只做控制面」 |
| OQ-5 | ICE 组件归属 | 独立 app **`broker_ice`**（UDP 有状态），与 `broker_proto_webrtc` 互不依赖；两者都默认关闭 | UDP 状态机与 ranch TCP listener 是两条路径；参照 S8-D 的可选依赖姿态 |
| OQ-6 | 端口矩阵 | 信令 **8084/TCP**（wss，默认不开）、STUN/TURN **3478/UDP**、relay **49152-65535/UDP**；**不做** 5349、TURN/TCP、DTLS | 既有端口清单（1883/8080/8083/1884/14567/4223/5673/9093/6650/8090 + S9 示例 8081/8082）无冲突 |
| OQ-7 | 浏览器证据方式 | 主选 Python **`aiortc`**（仓库外安装，像 S9-F 的官方 SDK）；备选 headless Chromium | WebRTC 无官方服务器一致性套件；aiortc 支持 TURN 且可强制 `relay` |
| OQ-8 | 多人房间 | 首期 **1:1**；房间允许 >2 连接但不提供成员快照/选路，列 P2 | 上游仓库 1:1 假设 |
| OQ-9 | 日志/指标最小化 | **永不记录 `data` 原文**；只记 `event`/`room`/`peer_id`/`bytes`/trace；房间号不进指标标签 | `docs/security.md` §7/§9 |
| OQ-10 | 与 S9 的先后 | S9 已收口（2026-11-12），S10 可开工；端口与依赖零交集 | S9-F 交付记录 |

**一句话**：S10 是一个**不碰消息日志**的接入前端 + 一个**独立**的 ICE 辅助服务；它复用 `broker_ws`/`broker_security`/`broker_metrics`/`broker_event_trace`/`broker_limits`，但**不**复用 `broker_router`/`broker_queue`。

---

## 1. 事实底座（仓库内可核对）

### 1.1 可复用的门面

| 模块 | 导出（节选） | S10 用法 |
|---|---|---|
| `broker_ws` / `ws_frame` | `decode/2`、`encode/2,3`、`close_payload/2`、`close_code/1` | 唯一帧编解码；不再写第二份 RFC 6455 |
| `broker_ws` / `ws_handshake` | `accept/1,2`、`max_handshake/0`（16 KiB） | 握手 + 子协议协商（`webrtc.v1`） |
| `broker_security` | `authenticate_conn/1`（`{user_pass,N,S}` / `{token,S}`）、`authorize/2`、`tenant_id/1` | 信令认证与 ACL；与 A2A/MCP 同一条适配器自有 `require_auth` 路线 |
| `broker_limits` | `check_message/1`、`max_message_bytes/0`、`json_depth/1` | 信令 JSON 大小/深度上限（`max_json_depth` 默认 16） |
| `broker_event_trace` | `propagate/1`、`parse_traceparent/1`、`enabled/0` | `ce-*`/trace 门控，不新造 |
| `broker_metrics` | `inc/1,2`、`observe/2`、`gauge/2`、`gauge_add/2` | 低基数计数；键集冻结（ADR-016），新增键需显式决策 |

### 1.2 不可绕过的事实（决定 OQ-1/OQ-2）

```erlang
%% broker_queue.erl (S1 起冻结的唯一写路径)
append(PartitionId, Msg, Mode) ->
    case broker_limits:check_message(Msg) of
        ok -> case append_guard(...) of
                  ok -> append_checked(PartitionId, Msg, Mode); ...
append_checked(PartitionId, Msg, Mode) -> do_append(PartitionId, Msg, Mode, true).

do_append(PartitionId, Msg, Mode, Notify) ->
    ... Backend:append(Hd, [Msg], Mode) ...
```

- `persistence = transient` 只是消息字段：`wal_codec` 把 `persistence` 原样编进 WAL 记录，**存储层不按它分流**。
- 因此「ACP 式 transient + os_buffer」= **照样写盘**（S9-E 的 `$acp/*` 就是这样，且 S9-E 的文档从未声称不落盘）。
- 这与 roadmap §12 对 S10 的硬要求「信令为**瞬时转发、默认不落盘**（持久化边界由 S10-0 以 ADR 冻结）」直接冲突。

**推论**：只要信令帧走 `broker_queue:append/3`，就必然落盘、占 offset、进 retention，且 boot 重放会读到过期的 SDP。要么改内核（新增「无日志发布」能力），要么信令完全不进内核。S10 选后者。

### 1.3 依赖账本（`rebar.config`）

当前只有两个运行时依赖：`ranch 2.3.0`、`ra 3.2.0`（`vendor/` 内置 + `_checkouts/`）。上游仓库的四个候选依赖逐个核对（hex API 元数据，2026-11-12）：

| 包 | 版本 | 元数据事实 | 结论 |
|---|---|---|---|
| `cowboy` | 2.0.0 | `requirements: {cowlib: "~> 2.0.0", ranch: "~> 1.4.0"}`；且 2.0.0 落在两个 2026 年公告的受影响区间（`< 2.15.0`、`< 2.18.0`） | **不引入**：与 ranch 2.3.0 冲突，且版本线本身有已知漏洞 |
| `syn` | 1.6.1 | 无依赖（2017 年发布） | **不需要**：OQ-4 用 ETS 分片，不需要全局进程注册表 |
| `jsx` | 2.8.2 | 无依赖 | **不需要**：OTP 28 自带 `json`，S9 全线已在用 |
| `stun` | 1.0.20 | 依赖 `fast_tls 1.0.20` + `p1_utils 1.0.10`（NIF/OpenSSL 编译面） | **不引入**：TURN-over-TLS（5349）是首期非目标，为一个不做的能力拖两个老包不值 |

### 1.4 端口账本（`docs/operations.md` §1）

已占用：1883、8080、8083、1884/**UDP**、14567/**UDP**、4223、5673、9093、6650、8090；S9 示例端口 8081/8082（非默认监听）。

S10 拟用：**8084/TCP**（信令，wss）、**3478/UDP**（STUN/TURN）、**49152-65535/UDP**（relay）。三者与上表零交集，且 3478 是 IANA 注册的标准 STUN/TURN 端口。

### 1.5 先例

| 先例 | 与 S10 的关系 |
|---|---|
| `broker_proto_ws`（S5） | 自有 JSON 消息 API 的 WS 前端：ranch + `ws_handshake` + `ws_frame` + 原生 ping/pong（opcode 9/10）；S10 的信令前端是它的「换业务层」版本 |
| `broker_proto_mqtt_ws`（S8-B） | 复用 `broker_ws` 载标准协议、子协议协商、wss 经 `ranch` 的 ssl transport；S10-A 直接照此装配 |
| `broker_proto_acp`（S9-E） | 「瞬时数据面」的写法参照；但它的 `$acp/*` **确实落盘**，S10 不能照抄这一点 |
| `a2a_task_registry`（S9-B） | 分片 gen_server + ETS + `phash2` 的注册表形态；S10 的房间注册表同构（不含 boot replay） |

---

## 2. OQ 逐条收敛

### OQ-1 信令是否走统一消息内核

**选项**

| 选项 | 行为 | 问题 |
|---|---|---|
| A 全量内核（每帧一条 `#broker_message{}`，`$webrtc/room/<room>`） | 统一可观测、可订阅 | 落盘 + retention + offset；SDP 进 WAL（隐私）；每帧多一次 `broker_limits` + 路由 + 后端写 |
| B 内核「无日志发布」新能力 | 复用 destination 命名与路由索引，但不写后端 | **新增内核抽象**（违反设计 §4.1「不引入新的内核抽象」），要改 `broker_queue`/`broker_router` 并重跑 1063 用例 |
| C ACP 式（transient + os_buffer） | 表面上是「瞬时」 | 事实是**仍落盘**（§1.2）；等于把信令写进 WAL 并接受 boot 重放过期 SDP |
| **D 双平面 + 完全旁路内核**（选定） | 数据面进程直投；控制面纯内存扇出；不产生内核消息 | 房间不是 destination；能力矩阵要显式写「不进消息内核」 |

**结论**：D。

- **数据面**（客户端 `{"event":<任意>,"to":<peer_id>,"data":{...}}`）：`webrtc_room_registry` 查 `to` 的连接进程 → 直接 `Pid ! {text, Frame}`。转发帧 = 原报文 + `from`（保留 `to`），`event`/`data` **不解析**。
- **控制面**（`authenticated`/`unauthorized`/`invalid_message`/`joined`/`left`）：同样是房间成员进程表的纯内存扇出；可观测走 `logger` + 既有 metrics 键（`docs/security.md` §12.1 第 17 条的同一姿态：不加新指标键）。
- **为什么不是「写第二套消息内核」**：这里没有日志、没有 offset、没有 cursor、没有 retention、没有跨协议路由、没有订阅过滤器——只有一张 `Room → [Pid]` 的连接表。它不承载任何「消息」，承载的是「谁在线」。
- **代价（必须写进能力矩阵）**：`$webrtc/*` 不存在，MQTT/WS/A2A 的订阅者看不到信令；房间不能跨节点；审计只有日志，不能回放。

### OQ-2 持久化 / TTL / 可靠性

**结论**：不落盘；`persistence`/`reliability` 概念不适用（根本没有消息记录）；无 TTL、无 retention、无 `SubRef`、不写 `.cursors`、不参与 `min(live cursor)`。

**证明方式（S10-D 必做）**：跑一条完整 1:1 信令后断言：
1. `broker_queue` 的各分区 `committed` offset 不变（对比前后 `retention_status/0`）；
2. 数据目录下无 `$webrtc` 相关段增长（按 bytes 对比）；
3. `.cursors` 文件 mtime/内容不变。

**反证点**：如果 S10-A 的 spike 发现必须复用 `broker_queue` 才能满足某项既有约束（例如慢消费者守卫只在队列路径上有），则回到选项 B 并先立 ADR 改内核——**不允许**偷偷用 C。

### OQ-3 前端选型与依赖

**结论**：`ranch 2.3.0`（已有）+ `broker_ws`（已有）+ OTP 28 `json`（已有）。**净新增依赖 = 0**。

- TLS/wss：沿用 `broker_proto_mqtt_ws` 的 ranch ssl transport 装配（`RanchTransport:handshake/2`，`{handshake, Ref, Transport, Socket, Timeout}` 消息），证书来源 `broker_security.tls`。
- 子协议：`Sec-WebSocket-Protocol: webrtc.v1`（R-T6），缺失/未知 → 400（与 MQTT-WS 同一姿态，不静默降级）。
- 帧策略（R-T3）：只有 text 帧承载 JSON；binary → close 1003；控制帧（8/9/10）按 RFC 处理（原生 ping 回 pong，与上游的纯文本 `ping`/`pong` 并存）。

### OQ-4 注册表归属

**结论**：新增 `webrtc_room_registry`（`broker_proto_webrtc` 内部模块，不是新 app）：

- `N = erlang:system_info(schedulers_online)`（可通过 `room_shards` 覆盖），`phash2(Room, N)` 定位分片；
- 每分片一个 `gen_server`（串行化成员变更）+ 一张 protected ETS（读路径无锁）；
- 表项：`{{Room, PeerId}, #{pid, username, tenant, joined_at}}` 与 `{PeerId, Room}`；
- 成员变更与 `joined`/`left` 扇出在同一进程内完成 → 广播集合可测（R-S3）；
- **单节点**：跨节点房间列未实现项；重启丢成员（连接必然已断，广播 `left` 无意义）。

**为什么不是 Mnesia / syn / `broker_meta` 新表**：房间成员是**连接态**，随进程生死；Mnesia 是控制面持久化（ADR-002），`syn` 是全局进程组（引入依赖且要处理集群一致性）。

### OQ-5 ICE 组件归属

**结论**：独立 app `broker_ice`。

- `applications: [kernel, stdlib, sasl, crypto, broker_security, broker_metrics]`；不依赖 `ranch`、不依赖 `broker_queue`。
- 顶层 supervisor：`broker_ice_sup`（`one_for_all`）→ `broker_ice_udp`（socket owner + accept/recv loop）→ 每 allocation 一个进程（`temporary`）。
- 默认 `enabled => false`；`turn_ip` 未配置或 relay 端口段非法 → **启动期显式失败**（不静默降级）。
- 与 `broker_proto_webrtc` **互不依赖**：只开信令不起 ICE，反之亦然。

**为什么不是塞进 `broker_proto_webrtc`**：UDP 无连接、有分配状态与 relay 端口池，和 ranch TCP listener 的监督语义不同；混在一个 app 里会让 `one_for_all` 把信令一起拖死。

### OQ-6 端口矩阵

| 端口 | 协议 | 默认 | 说明 |
|---|---|---|---|
| 8084/TCP | wss（可明文，仅回环） | **关**（`listeners => []`） | 信令前端；生产必须 TLS |
| 3478/UDP | STUN + TURN | **关**（`enabled => false`） | 标准 STUN/TURN 端口 |
| 49152-65535/UDP | TURN relay | 关（随 3478 一起开） | 可配窄区间（如 `49200-49300`），部署需放行 |
| ~~5349~~ | TURN over TLS | **不做** | 首期非目标（R-I6），能力矩阵留痕 |
| ~~3478/TCP~~ | TURN over TCP | **不做** | 同上 |

信令**不需要**额外端口；ICE 的 relay 段与既有 UDP 端口（1884、14567）不重叠。

### OQ-7 真实客户端证据

**结论**：主选 Python `aiortc`（pip 安装到仓库外，方式同 S9-F 的 `mcp`/`a2a-sdk`/`agent-client-protocol`）：

- `RTCPeerConnection` 完整 offer/answer/candidate 时序，两个 peer 都连 Broker 的 8084；
- TURN 证据：`RTCIceServer(urls=["turn:127.0.0.1:3478"], username, credential)` + `iceTransportPolicy="relay"`，断言选中的 candidate pair 类型为 `relay`；
- STUN 证据：`stun:127.0.0.1:3478` 得到 `srflx` candidate，加一份裸 UDP Binding 请求/响应原文；
- 证据落 `test/protocol-compatibility/s10/`，含 `README.md`、脚本与输出原文。

**备选**：环境里已有 headless Chromium 时，加一份浏览器侧同样的时序（更强的「真实浏览器」证据）；没有就如实标注「以 aiortc 为第三方实现，非浏览器」。

**为什么不是 Pion（Go）**：aiortc 能同时覆盖信令与 TURN-required 路径，且与既有 Python 证据链一致；Pion 留作备选。

### OQ-8 多人房间

**结论**：首期 1:1。房间不限制人数（最多 `max_peers_per_room`，默认 2，可配），但：
- 新加入者**不**收到成员快照（与上游一致，R-S4 P1 的 `peers/1` 服务端 API 仍提供）；
- >2 时的选路（谁给谁发 offer）是客户端职责，Broker 只按 `to` 转发；
- 多人选路、成员快照推送、SFU 边界 → P2，能力矩阵留行。

### OQ-9 SDP / candidate 的日志与指标最小化

**结论**：

- **从不**记录 `data`（SDP/candidate 原文），也不进 audit body；只记 `event`、`room`、`peer_id`、`from`/`to`、字节数、`traceparent`（若开启）。
- 调试开关 `log_signaling_payloads => false`（默认）：显式开启才在 `debug` 级别记录前 64 字节 + SHA-256 前缀。
- 指标**优先复用 `broker_metrics` 既有的通用键**（`connections_current` / `connections_peak` / `auth_failures` / `authz_failures` / `protocol_errors` / `frame_decode_errors` / `bytes_in` / `bytes_out`）——这些键已经在六协议里这样用（见 `mqtt_sn_conn` / `mqtt_conn_fsm` 的调用），语义与协议无关。**`broker_metrics` 的键集是闭集**（`key_specs/0`，`inc/1` 对未注册键返回 `{error,{unknown_metric,K}}`），所以「房间数 / ICE 分配数 / relay 字节数」这类**新键默认不加**，与 S9-F 的同一结论（`docs/security.md` §12.1 第 17 条）；若 S10-D 评审认为必须，单独走 ADR-016 的键集变更，不夹带在 S10 里。**房间号/peer_id 永不进标签**（`docs/security.md` §7）。
- 拒绝事件的 `logger` 里，`to` 只记「未知 peer」而非原值（避免它变成探测预言机）。

### OQ-10 与 S9 的先后

**结论**：S9 已收口（S9-F 2026-11-12），S10 无阻塞。端口（8084 / 3478）与依赖（净新增 0）均与 S9 零交集。

---

## 3. 待落的 ADR（草案，未签认）

### ADR-028（已签认 2026-11-13）信令面与消息内核的边界

要冻结的决策：

1. 信令**不进** `broker_queue`/`broker_router`，**不产生** `#broker_message{}`；
2. 数据面进程直投；控制面内存扇出；`room` 不是 destination；
3. 「瞬时、不落盘、无 cursor、无 retention」为**契约**，附 §2/OQ-2 的三条测试证明；
4. 明确不新增内核抽象；若未来需要「无日志发布」，另立 ADR 并给出内核变更范围；
5. 五类服务器事件的线格式与广播集合以测试钉死（R-S1/R-S3）；
6. 明确「不构成第二套消息内核」的论证（无日志/无 offset/无 cursor/无跨协议路由）；
7. **边界口径要写准**：契约是「**不存在对 `broker_queue` / `broker_router` 的调用、不产生 `#broker_message{}`**」，**不是**「依赖闭包里没有它们」——`broker_event`（S7 trace 所在 app）本身依赖 `broker_router` + `broker_queue`，一旦信令要用 `broker_event_trace` 就会把两者带进闭包。守护方式：**一条源码级守卫测试**（`webrtc_boundary_SUITE`：grep `apps/broker_proto_webrtc/src/*.erl` 与 `apps/broker_ice/src/*.erl`，断言无 `broker_queue:` / `broker_router:` 调用、无 `#broker_message{` 构造，`broker_core` 的 include 也不得出现）+ 代码评审 + S10-D 的「不落盘」实测三断言。**注意 `rebar3 xref` 在本仓的既有口径只覆盖 undefined/deprecated/recursive 调用与 unused-export，不是调用图查询，不能用来证明「没有这条边」**。
8. **ACL 的 `destination` 键不是路由目的地**：`broker_security:authorize/2` 只认 `action` / `tenant_id` / `destination` 三个键，其中 `destination` 是 ACL pattern 的匹配主体。信令需要 ACL 时传 `destination => <<"$webrtc/room/", Room/binary>>`，**这个字符串只用于模式匹配**，不代表房间是内核 destination（不进 `broker_router`、不建订阅索引）。这一条要写进 ADR-028，否则实现期容易被误读。

### ADR-029（已签认 2026-11-13）ICE 组件、端口矩阵与部署边界

要冻结的决策：

1. ICE 独立 app `broker_ice`，与信令 app 解耦，默认关闭；
2. 端口矩阵：8084/TCP、3478/UDP、49152-65535/UDP；**不做** 5349 / TURN over TCP / DTLS；
3. TURN 长期凭据接 `broker_security` 身份表（`authenticate_conn/1` + `authorize/2`），口令不进日志；
4. `turn_ip` 缺失、relay 端口段非法、证书缺失 → 启动期显式失败；
5. 单节点语义与跨节点限制；
6. 首期范围：STUN Binding + TURN Allocate/Refresh/CreatePermission/ChannelBind/ChannelData/Send/Data（UDP），不含 TURN-TLS/TCP。

> 两份 ADR 都**尚未写入** `docs/adr/`；等评审确认上面每条后再落盘，避免把草案读成冻结文本。

---

## 4. 风险与反证点（写下来，便于以后证伪）

| 风险 | 触发信号 | 应对 |
|---|---|---|
| 纯内存扇出撑不住连接数 | S10-D 压测下 1 万连接 / 1 千房间时调度饱和、`left` 广播抖动 | 房间注册表分片数上调；控制面改批量扇出；**不**回退到内核写路径 |
| 信令不进内核导致运维可见性不足 | 运维无法订阅「谁加入了房间」 | 先靠 `logger` 特征表；房间/成员只读视图**当前按「未排期」**（不夹在 S10-D 里承诺），需要时在 `broker_admin` 既有只读范式上单独立项 |
| aiortc 不能代表浏览器行为 | TURN 路径在 aiortc 通过、浏览器失败 | 加 Chromium 证据；或把「浏览器兼容性」写成限制 |
| in-tree STUN/TURN 工作量被低估 | S10-C 首个 spike 超过预算 | 先交付 STUN（S10-C1）+ TURN Allocate/Refresh（C2），Channel/relay（C3）单独 gate；**不**为此引入 `stun` 包 |
| 集群需求提前 | 需要跨节点房间 | 单独立项（注册表跨节点涉及 ADR-021 归属/epoch），S10 不做 |
| 端口/防火墙 | 部署环境不放行 49152-65535/UDP | 提供窄区间配置 + `docs/operations.md` 防火墙表 |

**最可能错的一条**：OQ-1 的「控制面也不进内核」。如果 S10-D 的审计要求需要「可回放加入/离开」，那就必须为控制面单独设计（或引入内核无日志发布）。当前结论基于「审计只需日志 + 只读视图」这一假设，写进 ADR-028 时要显式声明。

---

## 5. S10-A 入口条件

1. ADR-028 / ADR-029 评审签认并落盘（本文 §3 的条目为底稿）。
2. 一个 **spike**（`spikes/s10_0_ws_signal/`，独立 rebar3 项目，不进 umbrella）：
   - 用 `ranch 2.3.0` + `ws_handshake`/`ws_frame` 在临时端口起一个 wss listener；
   - 用 Python `websockets` 客户端完成：握手 + `authenticate` + 纯文本 `ping`/`pong` + 一条 `to` 转发；
   - 记录：握手字节数、帧类型策略、原生 ping 与文本 ping 的并存行为。
3. 基线确认：`rebar3 ct` 1063/1063（S10-A 不得先降后升）；`dialyzer` 0 warning。
4. `docs/operations.md` 端口表新增 8084/3478 的**计划**标注（明确「未实现」），避免与现状混淆。
5. `docs/design/s10-execution-plan.md` 与本次签认结果对齐（DoD / 测试矩阵 / 依赖表如有改动，回写到该文件而不是只留在评审记录里）。
