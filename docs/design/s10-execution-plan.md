# S10 执行计划 — WebRTC 信令面 + ICE 面（媒体面不做）

- **对应设计**：[`docs/design/s10-webrtc-signaling.md`](s10-webrtc-signaling.md) §3（候选需求 R-T/R-S/R-M/R-I/R-X/R-C/R-O/R-E）、§4.2（子阶段）、§4.3（Gate S10）、§4.4（OQ-1~OQ-10）；[`docs/design/s10-0-convergence-report.md`](s10-0-convergence-report.md)（OQ 收敛结论与 ADR-028/029 底稿）；[`docs/roadmap.md`](../roadmap.md) §12.1 / §12.2。
- **基线（开工前）**：`rebar3 ct` **1063/1063、68 套件**；`dialyzer` 0 warning；`rebar3 release` 装配成功（S9-F 收尾，`fcbf83e`）。
- **性质**：**计划稿**。本文件不含任何实现代码、不开任何端口、不新增任何依赖；§DoD 与 Gate S10 一律**未勾选**。实测数字只在阶段交付后回填（§门禁实测表）。
- **范围**：一个不碰消息日志的 **信令接入前端** + 一个与内核解耦的 **ICE 辅助服务**。**媒体面（RTP/RTCP/DataChannel/SFU/录制/转码）明确不做**。
- **执行约束**（仓库既定规则，全程适用）：编译/测试前 `export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"`；`rebar3 ct` 前 `rm -rf _build/test/extras`；Python 一律 `/usr/bin/python3`（3.12）；用例数由各套件 `all/0`（含 `groups/0`）展开后逐个数出；CT 一律 `port => 0`，不动既有 8080/8083/1883/8081/8082；不杀用户进程；ADR 冻结文本不动、偏差处加批注；设计稿原文不改。

## 0. 三条硬边界（先写死，避免实现期回头改架构）

1. **信令不进消息内核**：不产生 `#broker_message{}`、不调 `broker_queue:append/3`、不调 `broker_router`、无 offset / cursor / retention / SubRef / `.cursors`。依据：`broker_queue:append/3 → append_checked → do_append → Backend:append` 无 transient 旁路，`wal_codec` 原样编码 `persistence` 字段，**ACP 式 `transient + os_buffer` 照样落盘**。详见收敛报告 §1.2 / OQ-1。若 S10-A 的 spike 证伪，则退回「新增内核无日志发布能力」并**先改 ADR、再改内核**，不允许偷偷走 ACP 式 transient。
2. **净新增运行时依赖 = 0**：只用 `ranch 2.3.0`（已有）+ `broker_ws`（`ws_handshake` / `ws_frame`）+ OTP 28 内置 `json` + `crypto`。不引入 `cowboy`（2.0.0 的 hex 元数据要求 `ranch ~> 1.4.0`，与 2.3.0 冲突且版本线在 2026 年两条公告的受影响区间）、`syn`（不需要全局进程组）、`jsx`（OTP 28 自带 `json`）、`stun`（拖 `fast_tls` + `p1_utils`，而 5349 是首期非目标）。
3. **默认全关**：`broker_proto_webrtc.listeners => []`、`broker_ice.enabled => false`。与 S9-A/S9-D 同姿态——app 随节点装配但不占端口。两个新 app 在 `rebar.config` 的**两份** relx 清单里都要加（默认清单与 `quic` profile 清单；profile 的 `relx` 是**替换**而非继承，ADR-024 §D5 已踩过这个坑）。

## 1. 任务拆分

### S10-0 不确定性收敛（无实现代码；先决条件）

| # | 任务 | 产出 | 验收 |
|---|---|---|---|
| 0.1 | **ADR-028 落盘**：信令面与消息内核的边界（不进内核 / 进程直投 / `room` 不是 destination / 不构成第二套消息内核的论证 / 边界口径写准） | `docs/adr/028-webrtc-signaling-kernel-boundary.md` | 收敛报告 §3 ADR-028 的 8 条逐条成文 + 反证点写成「触发信号 → 应对」 |
| 0.2 | **ADR-029 落盘**：ICE 组件归属、端口矩阵、部署边界、TURN 凭据接身份表 | `docs/adr/029-ice-component-and-port-matrix.md` | 同上 |
| 0.3 | **spike `spikes/s10_0_ws_signal/`**（独立 rebar3 项目，**不进 umbrella**，照 `spikes/s0_*` 惯例：自带 `rebar.config` + `rebar.lock` + `REPORT.md`） | **已交付（2026-11-13）**。`{deps, [{ranch,"2.3.0"}, {broker_ws, {path, "../../apps/broker_ws"}}]}`——**用 path dep 而不是 `_checkouts` 符号链接**（后者在 `_build/test` 下不生效，eunit 会 `undef`；path dep 在 default/test 两个 profile 都稳）。`ws` 与 `wss` 两份脚本都在 `port => 0` 起 listener，Python `websockets 17.4` 跑完：握手 → `authenticate` → 文本 `ping`/`pong` → 原生 opcode 9/10 → 一条 `to` 直投 → binary 1003 | **[`spikes/s10_0_ws_signal/REPORT.md`](../../spikes/s10_0_ws_signal/REPORT.md) + 10 条 eunit 断言全绿**。实测：101 响应 **164 字节**（ws 与 wss 相同）；子协议 `webrtc.v1` 协商成功，无共同子协议 → **400**；会话拿到 **TLSv1.3**（`ranch_ssl` 与 `ranch_tcp` **同一份连接模块**，一行没改）；文本 `ping`→文本 `pong` 与 opcode 9→10 并存；binary → **close 1003**；`to` 直投对端收到 `from`+`to`+原 `data`（SDP 不动）；`peer_id` 服务端 `base64(strong_rand_bytes(9))` 铸造、客户端不可指定 |
| 0.4 | **OQ-2 证据脚本化**：把「不落盘」的断言写成可复跑脚本 | 记录 `retention_status/0` 各分区 `committed` offset、数据目录字节数、`.cursors` mtime/内容的前后对比 | 脚本退出码 0；S10-D 直接复用 |
| 0.5 | 评审签认 + roadmap §12.1 的 S10-0 行改状态；`progress.md` 下一步指向 S10-A | 文档 | 两份 ADR 已被签认（否则 S10-A 不得开工） |

**S10-0 不做**：任何 app 骨架、任何端口、任何 `rebar.config` 改动。

### S10-A 信令前端底座（`broker_proto_webrtc` 首个代码）

新 app `apps/broker_proto_webrtc/`：

| 模块 | 职责 | 关键点 |
|---|---|---|
| `broker_proto_webrtc.app.src` | 依赖 `kernel, stdlib, sasl, ssl, ranch, crypto, broker_core, broker_security, broker_metrics`（`broker_limits` 在 `broker_security` app 内，不用单列） | **契约不是「依赖闭包里没有 `broker_queue`/`broker_router`」，而是「没有对它们的调用、不产生 `#broker_message{}`」**：`broker_event`（S7 trace 所在 app）本身就依赖这两个，一旦用 `broker_event_trace` 就会进闭包。守这条的是一份**源码级守卫测试** `webrtc_boundary_SUITE`（grep 两个新 app 的 `src/*.erl`，断言无 `broker_queue:` / `broker_router:` / `#broker_message{`）；`rebar3 xref` 在本仓只查 undefined/deprecated/recursive + unused-export，证明不了「没有这条边」。S10-A 不做 trace，S10-D 再定 |
| `broker_proto_webrtc_app` | boot 校验（子协议非空、`auth_timeout_ms` > 0、帧上限 > 0、`max_peers_per_room` ≥ 2），失败即启动失败 | 不静默降级 |
| `broker_proto_webrtc_sup` | `one_for_one`：`webrtc_room_registry`（S10-B）在前、各 listener 在最后（照 `broker_proto_a2a_sup` 的次序） | listener 用 `ranch_embedded_sup` |
| `webrtc_listener` | `ranch_protocol` 适配器，`start_link/3` | 照 `mqtt_ws_listener` 形态 |
| `webrtc_conn` | 每连接一个 `gen_server`：sock 归属 → `RanchTransport:handshake/2` → RFC 6455 握手 → 认证窗口 → 已认证态 → 帧循环 → 清理 | 复用 `ws_handshake:accept/2`（16 KiB 上限）与 `ws_frame:decode/2, encode/2,3`；**不写第二份 RFC 6455** |
| `webrtc_auth` | 认证：JSON `{"event":"authenticate","data":{username,password}}` → `broker_security:authenticate_conn/1` | 口令只在本模块内存里出现，不进日志/指标 |

要点：

- **子协议** `webrtc.v1`（R-T6）：缺失或未知 → 握手 400（与 `broker_proto_mqtt_ws` 同一姿态，不静默降级）。
- **帧策略**（R-T3）：text 帧承载 JSON；**binary → close 1003**（不是上游那种只 warning 忽略）；控制帧 8/9/10 按 RFC 处理。
- **保活**（R-T4）：客户端发纯文本帧 `ping` → 回纯文本帧 `pong`（上游语义，与 JSON 事件区分）；同时原生 opcode 9/10 也照 RFC 回 pong。两者并存，测试都要钉死。
- **认证窗口**（R-C1）：`auth_timeout_ms` 默认 **5000**（上游 `ws_auth_delay` 300 ms 太短，浏览器 `getUserMedia` 后才连、且 README 未记载；这里显式化并写进 `docs/operations.md`）。超时未认证 → close 1008。
- **空闲超时**：`idle_timeout_ms` 默认 60000，`infinity` 可关。
- **门控**：默认 `listeners => []`；示例配置给 8084（非默认项，与 S9 的 8081/8082 同处理）。
- **本阶段不做**：房间成员、`to` 转发、`joined`/`left`（全部 S10-B）。

### S10-B 房间与信令内核

| 模块 | 职责 | 关键点 |
|---|---|---|
| `webrtc_room_registry` | `phash2(Room, N)` 分片 `gen_server` + 每分片一张 protected ETS；`join/4`、`leave/2`、`lookup/2`、`peers/1`、`rooms/0` | 与 `a2a_task_registry` 同构；**单节点**、无 Mnesia、**无 boot replay**（成员是连接态，重启后连接必然已断） |
| `webrtc_signal` | 五类服务器事件的构造（`text_event/1,2` 等价物；`authenticated` 带 `peer_id`、`unauthorized`/`invalid_message` **无 `data`**） | 线格式严格按设计 §2.3；`event`/`data` 对服务器不透明 |
| `webrtc_dispatch` | 已认证消息 → 校验 `to` → 房间归属 → `Pid ! {text, Frame}`（原报文 + `from`，**保留 `to`**） | 未知 / 跨房间 `to` → 明确错误事件，**handler 不崩溃**（上游 F-1 的正确处置） |
| `webrtc_server` | 服务端 API：`peers/1`、`publish/2,3`、`send/2,3` | 未知 peer 返回 `{error, unknown_peer}`，**不抛 `badarg`**（上游 F-2） |
| ACL | 加入房间 / 向 `to` 发信令 / 服务端 API 都过 `broker_security:authorize/2` | `authorize/2` 只读三个键：`action`（atom 或 binary）、`tenant_id`、`destination`（作为 ACL pattern 的匹配主体）。所以 resource 形状是 `#{action => signal, tenant_id => T, destination => <<"$webrtc/room/", Room/binary>>}`——**这个 `destination` 字符串只当 ACL 模式主体用，不是路由目的地**（不进 `broker_router`，ADR-028 里要显式写这一条，否则会误读成"信令有 destination"）。`credential_capable/0` **不改**（照 A2A/MCP 的「适配器自有 require_auth」路线）；`authorize(undefined, _)` 返回 `allow`，所以 `require_auth => true` 必须由适配器自己兜住，不能指望 ACL |

要点：

- **`peer_id`** 由服务端 `crypto:strong_rand_bytes/1` 铸造并 base64 编码，**不可由客户端指定**（R-S7）。
- **广播集合**（R-S3，测试钉死）：`joined` 只发**加入前已有**成员；`left` 发**剩余全体**（已离开的连接不再收）；`authenticated`/`unauthorized`/`invalid_message` 只回请求者。
- **房间隔离**（R-X4）：`to` 只能指向同房间 peer；跨房间 / 伪造 `peer_id` / 未知 peer 一律拒绝，日志里 `to` 只记「未知 peer」而非原值（避免变成探测预言机）。
- **人数上限**：`max_peers_per_room` 默认 **2**（首期 1:1）；超额 → 明确拒绝事件 + close。
- **断线清理**（R-C2）：`terminate/2` → registry `leave` → `left` 扇出；进程被 kill 时靠 ETS 表项的 `monitor` 兜底。
- **多人房间（>2）与成员快照推送** = P2，能力矩阵留行（R-S4）。

### S10-C ICE / STUN / TURN（`broker_ice`，与信令互不依赖）

新 app `apps/broker_ice/`：

| 模块 | 职责 |
|---|---|
| `broker_ice.app.src` | 依赖 `kernel, stdlib, sasl, crypto, broker_security, broker_metrics`；**不依赖** `ranch` / `broker_queue` / `broker_proto_webrtc` |
| `broker_ice_app` / `_sup` | `one_for_all`；`enabled => false` 时干净启动零 socket；`turn_ip` 缺失 / relay 段非法 / realm 缺失 → **启动期显式失败** |
| `broker_ice_udp` | socket owner + recv loop；`inet_udp`，`recbuf`/`active` 口径写明 |
| `stun_codec` | 手写 STUN 编解码：20 字节头（type/length/magic cookie/transaction id）、`XOR-MAPPED-ADDRESS`、`MESSAGE-INTEGRITY`（SHA-1，长期凭据）、`ERROR-CODE`、`FINGERPRINT` |
| `turn_allocation` | 每 allocation 一个进程（`temporary`）：`Allocate` / `Refresh` / `CreatePermission` / `ChannelBind` / `ChannelData` / `Send` / `Data`（**仅 UDP**） |
| `turn_auth` | TURN 长期凭据 → `broker_security:authenticate_conn/1` + `authorize/2`；输出 `key = MD5(user:realm:password)`，**口令不进日志** |

要点：

- **端口**：`3478/UDP`（STUN + TURN）、`49152-65535/UDP`（relay，可配窄区间如 `49200-49300`）。**不做** 5349、TURN over TCP、DTLS、ICE-lite、IPv6 候选（R-I6，能力矩阵留行）。
- **凭据**（R-I3）：与信令凭据**同账号**（同一张 `users` 表），但 TURN 用 `realm` 参与密钥派生；轮换靠改用户记录 + `Refresh` 重认证，**不引入 KMS**。
- **上限**（R-X5）：`max_allocations`（默认 1024）、`allocation_lifetime_s`（默认 600）、`max_permission_per_allocation`。
- 与 S10-B **并行**：S10-A 完成后 C 即可开工，不阻塞 B。

### S10-D 安全与一致性收口

| # | 任务 | 产出 |
|---|---|---|
| D.1 | 真实第三方客户端证据 | `test/protocol-compatibility/s10/`（见 §3） |
| D.2 | 「不落盘」证明 | 复用 S10-0 的脚本，跑完整 1:1 信令后断言 offset / 字节 / `.cursors` 三不变 |
| D.3 | 安全专项 | `unauthorized` / ACL 越权 / 跨房间 `to` / 未知 `peer_id` / 伪造 `peer_id` / 超大帧 / 认证窗口超时，逐条用例且**不崩溃**；`docs/security.md` 新增 §12.2（本阶段明确未实现项） |
| D.4 | 可观测 | ① 日志字段（`room`/`peer_id`/`bytes`/traceparent，**永不记 `data`**）；② 失败事件只 `logger`——**`broker_metrics` 冻结键集默认不扩容**，照 S9-F 的同一结论（`docs/security.md` §12.1 第 17 条），要动就单独走 ADR，不夹在 S10-D 里静默加；③ 房间/成员只读视图：**先按「未排期」处理**，若立项则在 `broker_admin` 的既有只读接口范式上加（读注册表，不进日志），不在本阶段承诺 |
| D.5 | 能力矩阵 + 限制清单 | `docs/protocol-compatibility.md` §2 新增 WebRTC / STUN-TURN 两行 + §4.20 小节；`docs/security.md` §12.2；`docs/operations.md` 端口表 + 防火墙表 |
| D.6 | 压测 / 容量 | `docs/benchmarks/<date>-s10-capacity.md` + 原始 JSON：连接数 × 房间数 × 信令速率（顺序 + 并发）；ICE 的 allocation 建立速率与 relay 吞吐 |
| D.7 | 文档同步 + ADR 定稿说明 | roadmap §12.1 各行 + §12.2 Gate + README 状态行 + progress + architecture（§3.1 app 表 + env 表 + 延期表）+ operations；ADR-028/029 加「定稿说明」批注（冻结文本不改） |
| D.8 | 非目标逐行留痕 | §1.3 非目标 + R-I6 裁剪项，每条在能力矩阵有一行 |

## 2. 测试矩阵（计划）

| 套件 | 归属 | 覆盖 | 计划例数 |
|---|---|---|---|
| `webrtc_handshake_SUITE`（或并入 `webrtc_conn_SUITE`） | S10-A | 握手成功/缺 `Sec-WebSocket-Key`/超 16 KiB/子协议缺失/子协议未知 → 400 | ~8 |
| `webrtc_conn_SUITE` | S10-A + S10-B | 认证成功（`authenticated` + `peer_id`）/ 错口令（`unauthorized` 无 `data`）/ 非法 JSON / binary → close 1003 / 文本 `ping`→`pong` / 原生 opcode 9→10 / `auth_timeout_ms` 超时 close 1008 / 空闲超时 / 未认证就发信令 | ~14 |
| `webrtc_boundary_SUITE` | S10-A（守 ADR-028） | 源码级守卫：两个新 app 的 `src/*.erl` 里无 `broker_queue:` / `broker_router:` 调用、无 `#broker_message{` 构造；`rebar.config` 的两份 relx 清单都含两个新 app | ~4 |
| `webrtc_room_SUITE` | S10-B | `join`/`leave` 幂等、`phash2` 分片归属、进程死亡自动清理、`max_peers_per_room` 上限、`peers/1`、`rooms/0` | ~10 |
| `webrtc_signal_SUITE` | S10-B | 五类事件线格式（含 `text_event/1` 无 `data`）；**广播集合矩阵**：`joined` 只发已有成员 / `left` 发剩余全体 / 其余只回请求者 | ~12 |
| `webrtc_route_SUITE` | S10-B | `to` 直投（原报文 + `from`、保留 `to`）、未知 peer、跨房间、缺 `to`、伪造 `peer_id`、`event`/`data` 不被改写（SDP 字节透传比对） | ~10 |
| `webrtc_acl_SUITE` | S10-B | 加入房间 / 发信令 / 服务端 API 三条路径的 allow/deny；ACL 不能被 `data` 属性绕过 | ~8 |
| `stun_codec_SUITE` | S10-C | 头编解码、`XOR-MAPPED-ADDRESS`（IPv4）、`MESSAGE-INTEGRITY`、`ERROR-CODE`、`FINGERPRINT`、截断/超长报文 | ~12 |
| `stun_server_SUITE` | S10-C | 裸 UDP Binding 请求/响应字节、未知 method、非法 cookie、认证失败 | ~8 |
| `turn_allocation_SUITE` | S10-C | `Allocate`/`Refresh`/`CreatePermission`/`ChannelBind`/`ChannelData`/`Send`/`Data`、生命周期过期回收、端口池耗尽、`max_allocations` | ~14 |
| `turn_auth_SUITE` | S10-C | 长期凭据接身份表、错凭据、realm 不匹配、口令不进日志断言 | ~8 |

**约束**：全部 `port => 0`；不依赖外部网络；不新增 Umbrella 外进程（真实客户端证据另立 `test/protocol-compatibility/s10/`）。

## 3. 证据计划（真实第三方客户端）

- **主选：Python `aiortc`**（pip 安装到 `/tmp/opencode/s10-libs`，方式同 S9-F 的官方 SDK；`PYTHONPATH` 注入，**不进提交**）。
  - **信令**：两个 `RTCPeerConnection` 都连 Broker 的 `wss://127.0.0.1:<port>/websocket/<room>`，走完 `offer → answer → candidate`（含 trickle），断言媒体协商完成（不需要真正收发音视频帧，`aiortc` 的 `connectionState` 到位即可）。
  - **STUN**：`stun:127.0.0.1:3478` 得到 `srflx` candidate；并加一份**裸 UDP Binding 请求/响应原文**（十六进制 + 解码），不依赖任何库。
  - **TURN**：`iceTransportPolicy="relay"` + `turn:` 凭据，断言选中的 candidate pair 类型为 `relay`，且 relay 端口落在配置段内。
- **备选**：环境里有 headless Chromium 时，加一份浏览器侧同样时序（更强的证据）；没有就**如实标注**「以 aiortc 为第三方实现，非浏览器」。
- **落盘**：`test/protocol-compatibility/s10/README.md`（复跑命令 + 前置约束）+ 脚本 + `*.out` / `*.jsonl` 原文；`.gitignore` 加 s10 scratch 段（`__pycache__/`、`data*/`、`node.out`/`node.err`、`*.beam`）。
- **诚实边界**（写进报告）：证据节点是 `127.0.0.1` 回环，**不声称**公网 NAT 穿透、真实运营商网络、TLS 证书链、浏览器兼容矩阵。

## 4. 端口 / 配置 / 依赖表（计划值，全部未实现）

### 4.1 端口

| 端口 | 传输 | 默认 | 说明 |
|---|---|---|---|
| 8084 | TCP（wss；明文仅回环） | **关**（`listeners => []`） | 信令前端。与既有 8080/8083/1883/8081/8082 零冲突 |
| 3478 | UDP | **关**（`enabled => false`） | STUN + TURN（IANA 注册的标准端口） |
| 49152-65535 | UDP | 关（随 3478 一起开） | TURN relay；可配窄区间，部署需放行 |
| ~~5349~~ | — | **不做** | TURN over TLS，首期非目标 |
| ~~3478/TCP~~ | — | **不做** | TURN over TCP，首期非目标 |

**端口占用复核（2026-11-12，写计划时实跑 `ss -ltnup`）**：`8080`、`8083` 正在监听（用户进程，勿动）；`8084`、`3478`、`49152` 段当时无占用。这只是当时的快照——S10-A 起 listener 前要再核一次，且 CT 里一律 `port => 0`。

### 4.2 配置（`broker_proto_webrtc` / `broker_ice`）

| env | 默认 | 说明 |
|---|---|---|
| `broker_proto_webrtc.listeners` | `[]` | 出厂零监听；示例配置给 `#{port => 8084}` |
| `broker_proto_webrtc.require_auth` | `true` | `false` **只在所有 listener 都绑回环时**才被接受；非回环 + `false` → 启动失败 `{insecure_listener, Port, Ip}`（照 `broker_proto_a2a_app:check_auth/0` 的 `check_loopback/1` 实现） |
| `broker_proto_webrtc.subprotocols` | `[<<"webrtc.v1">>]` | 缺失/未知 → 400 |
| `broker_proto_webrtc.auth_timeout_ms` | 5000 | 上游 `ws_auth_delay` 是 300 ms 且未记载；这里显式化 |
| `broker_proto_webrtc.idle_timeout_ms` | 60000 | `infinity` 可关 |
| `broker_proto_webrtc.max_frame_bytes` | 262144 | 单帧上限；超限 close 1009 |
| `broker_proto_webrtc.max_peers_per_room` | 2 | 首期 1:1 |
| `broker_proto_webrtc.room_shards` | `undefined` → `schedulers_online` | 改值必须重启（成员是连接态） |
| `broker_proto_webrtc.binary_frame_policy` | `close_1003` | 上游是「warning 后忽略」，这里明确拒绝 |
| `broker_proto_webrtc.log_signaling_payloads` | `false` | 显式开启才在 `debug` 记前 64 字节 + SHA-256 前缀 |
| `broker_ice.enabled` | `false` | 出厂零 socket |
| `broker_ice.stun_port` | 3478 | UDP |
| `broker_ice.turn_ip` | `undefined` | 必填（`enabled => true` 时缺失 → 启动失败）；**不**按 hostname A 记录静默兜底 |
| `broker_ice.realm` | `undefined` | 必填；参与长期凭据密钥派生 |
| `broker_ice.relay_min_port` / `relay_max_port` | 49152 / 65535 | 区间非法 → 启动失败 |
| `broker_ice.max_allocations` | 1024 | 端口池上限 |
| `broker_ice.allocation_lifetime_s` | 600 | `Refresh` 可续 |

### 4.3 依赖账本

| 项 | 变更 | 依据 |
|---|---|---|
| `ranch 2.3.0` | 复用（已有） | `ranch_protocol` + `ranch_embedded_sup` + ssl transport |
| `broker_ws` | 复用（已有） | `ws_handshake` / `ws_frame`，不再写第二份 RFC 6455 |
| OTP `json` / `crypto` | 复用（已有） | S9 全线已在用 |
| `cowboy` | **不引入** | hex 元数据 2.0.0 要求 `ranch ~> 1.4.0`，与本仓 2.3.0 冲突；且该版本线在两条 2026 年公告的受影响区间 |
| `syn` | **不引入** | 房间注册表用分片 ETS，不需要全局进程组 |
| `jsx` | **不引入** | OTP 28 自带 `json` |
| `stun` | **不引入** | 拖 `fast_tls 1.0.20` + `p1_utils 1.0.10`，只为 5349（首期不做） |

**净新增运行时依赖：0。**

## 5. DoD（对照 Gate S10，**全部未勾选**）

- [ ] 一份**真实第三方客户端**证据落盘 `test/protocol-compatibility/s10/`：`RTCPeerConnection` 完成 offer/answer/candidate 全时序 + STUN/TURN 连通性；一行能力矩阵 + 逐项限制清单
- [ ] 信令「瞬时转发、不落盘」由 ADR-028 冻结并有测试证明（分区 `committed` offset 不变、数据目录无增长、`.cursors` 不写）
- [ ] 五类服务器事件（`authenticated`/`unauthorized`/`invalid_message`/`joined`/`left`）的**广播集合与线格式**由 `webrtc_signal_SUITE` 钉死
- [ ] 认证接六协议统一身份表；`unauthorized` / ACL 越权 / 跨房间 `to` / 未知 `peer_id` / 伪造 `peer_id` 专项用例通过且 **handler 不崩溃**
- [ ] STUN/TURN 认证接身份表、凭据轮换、日志脱敏有专项用例；端口矩阵与防火墙要求写入 `docs/operations.md`
- [ ] 既有 `rebar3 ct` 零回归：**基线 1063/1063、68 套件**（设计稿 §4.3 写的 823/823 是 2026-11-11 写稿时的快照，冻结原文不改）；`dialyzer` / `xref` 干净；`rebar3 release` 装配成功（**两个 profile 都要**）
- [ ] 压测 / 容量报告落盘，数字全部来自实测
- [ ] ADR-028/029 定稿说明；roadmap / architecture / operations / security / README / progress / protocol-compatibility 同步
- [ ] §1.3 非目标与 §3.9 裁剪项在能力矩阵中逐行留痕

## 6. 风险与回退

| 风险 | 触发信号 | 应对 |
|---|---|---|
| 纯内存扇出撑不住连接数 | S10-D 压测下万级连接 / 千级房间调度饱和、`left` 抖动 | 上调 `room_shards`；控制面批量扇出；**不**回退到内核写路径 |
| 运维可见性不足（信令不进内核） | 运维无法订阅「谁加入/离开」 | 日志特征表先兜；房间/成员只读视图**当前按「未排期」**（D.4 ③），需要时在 `broker_admin` 既有只读范式上单独立项，不夹带 |
| `aiortc` 不能代表浏览器行为 | TURN 路径 aiortc 通过、浏览器失败 | 加 Chromium 证据；否则把「浏览器兼容性」写成限制 |
| in-tree STUN/TURN 工作量低估 | S10-C 首个 spike 超预算 | 拆 C1（STUN Binding）→ C2（TURN Allocate/Refresh）→ C3（Permission/Channel/relay），各自单独 gate；**不**为此引入 `stun` 包 |
| 集群需求提前 | 需要跨节点房间 | 单独立项（涉及 ADR-021 归属/epoch），S10 不做 |
| 防火墙不放行 49152-65535/UDP | 部署环境拒绝大段 UDP | 提供窄区间配置 + `docs/operations.md` 防火墙表 |
| **OQ-1 判断错**（控制面也不该留在内存） | S10-D 审计要求「可回放加入/离开」 | 回到 ADR-028 的「无日志发布」备选并**先改内核、先改 ADR**；不在实现里偷偷混用 |

**回退姿势**：S10-A/B 全在 `broker_proto_webrtc` 内且默认关闭——删 app + 两行 relx 清单即完全回退。S10-C 是独立 app，单独回退。都不触碰既有六协议代码路径。

## 7. 门禁实测表（**待填**，逐阶段交付后回填）

| 门禁 | 命令 | 结果 |
|---|---|---|
| 全量 CT | `rm -rf _build/test/extras && rebar3 ct` | 待填（基线 1063/1063、68 套件） |
| 静态分析 | `rebar3 dialyzer` | 待填（基线 0 warning） |
| xref | `rebar3 xref` | 待填 |
| Release（默认） | `rebar3 release` | 待填 |
| Release（quic profile） | `rebar3 as quic release` | 待填 |
| 信令证据 | `PYTHONPATH=/tmp/opencode/s10-libs /usr/bin/python3 test/protocol-compatibility/s10/webrtc_peer.py` | 待填 |
| STUN/TURN 证据 | 同上（`turn_allocation.py` + 裸 UDP `stun_raw.py`） | 待填 |
| 「不落盘」证明 | S10-0 的 `assert_no_persistence` 脚本 | 待填 |
| 压测 | 见 `docs/benchmarks/<date>-s10-capacity.md` §1 | 待填 |

## 8. 落地偏差记录（**待填**，实现期逐条追加）

> 格式照 `docs/design/s9-f-execution-plan.md` §落地偏差记录：每条写「计划 → 实际 → 为什么 → 留痕位置」。设计稿原文不改，偏差只在这里与 ADR 批注里记。

1. （待填）
