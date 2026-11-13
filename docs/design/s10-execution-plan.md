# S10 执行计划 — WebRTC 信令面 + ICE 面（媒体面不做）

- **对应设计**：[`docs/design/s10-webrtc-signaling.md`](s10-webrtc-signaling.md) §3（候选需求 R-T/R-S/R-M/R-I/R-X/R-C/R-O/R-E）、§4.2（子阶段）、§4.3（Gate S10）、§4.4（OQ-1~OQ-10）；[`docs/design/s10-0-convergence-report.md`](s10-0-convergence-report.md)（OQ 收敛结论与 ADR-028/029 底稿）；[`docs/roadmap.md`](../roadmap.md) §12.1 / §12.2。
- **基线（开工前）**：`rebar3 ct` **1063/1063、68 套件**；`dialyzer` 0 warning；`rebar3 release` 装配成功（S9-F 收尾，`fcbf83e`）。
- **性质**：**计划稿 + S10-A/S10-B 交付记录**。S10-A 的实现事实（含 45 例实测与 13 条落地偏差）已回填 §1/§2.1/§4/§5/§7/§8，S10-B 的实现事实（含 61 例实测与 12 条落地偏差）已回填 §1/§2.2/§4.2/§5/§7/§8；S10-C/D 仍是计划值，未勾选。本文件不在 S10-A/B 之外开任何端口、不新增任何依赖。
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

### S10-A 信令前端底座（`broker_proto_webrtc` 首个代码）— **已交付（2026-11-13）**

新 app `apps/broker_proto_webrtc/`（5 个 src 文件 + 3 个测试套件）：

| 模块 | 实际职责 | 关键点 |
|---|---|---|
| `broker_proto_webrtc.app.src` | 依赖 `kernel, stdlib, sasl, ssl, ranch, crypto, broker_ws, broker_security, broker_metrics`；**不含 `broker_core`** | **契约不是「依赖闭包里没有 `broker_queue`/`broker_router`」，而是「没有对它们的调用、不产生 `#broker_message{}`」**：`broker_event`（S7 trace 所在 app）本身就依赖这两个，一旦用 `broker_event_trace` 就会进闭包。守这条的是源码级守卫 `webrtc_boundary_SUITE`（剥离注释后扫描 `src/*.erl`，断言无 `broker_queue:` / `broker_router:` / `#broker_message{`）；`rebar3 xref` 在本仓只查 undefined/deprecated/recursive + unused-export，证明不了「没有这条边」。`broker_core` 只经 `broker_security` 传递进来，本 app 不列、不 include（ADR-028 §D8）。S10-A 不做 trace，S10-D 再定 |
| `broker_proto_webrtc_app` | `validate/0` boot 校验：`insecure_listener`（非回环 + `require_auth => false`）、`invalid_require_auth`、`invalid_subprotocols`（空/非 binary）、`invalid_auth_timeout_ms`、`invalid_idle_timeout_ms`、`invalid_max_frame_bytes` | 不静默降级。`require_auth` 是**本 app 自己的 env 键**（ADR-028 §D5，照 A2A/MCP），**不读 `broker_security.require_auth`**——那个键是「必须认证的协议名列表」（出厂 `[]`），当布尔读会让出厂 release 启动失败（落地时实测到并已修，见 §8 第 1 条） |
| `broker_proto_webrtc_sup` | `one_for_one`，只有 listener；无 registry 子进程（S10-B 才加） | listener 走 `ranch:child_spec/5` + `restart => transient` |
| `webrtc_listener` | `ranch_protocol` 适配器，`start_link/3` **直接**返回 `webrtc_conn:start_link/3` 的 gen_server pid | 不能 `spawn_link` 包一层：ranch 拿到立刻退出的 pid 会撕掉 listener（见 §8 第 2 条） |
| `webrtc_conn` | 每连接一个 `gen_server`：sock 归属 → `RanchTransport:handshake/2` → RFC 6455 握手 → 认证窗口 → 已认证态 → 帧循环 → 清理 | 复用 `ws_handshake:accept/2`（16 KiB 上限）与 `ws_frame:decode/2, encode/2,3`；**不写第二份 RFC 6455** |
| 认证 | 在 `webrtc_conn:try_authenticate/2` 内（未单开 `webrtc_auth` 模块，见 §8 第 6 条）：JSON `{"event":"authenticate","data":{username,password}}` → `broker_security:authenticate_conn/1` | 口令只在该函数栈上出现，不进日志/指标 |

要点（全部已实现并有用例）：

- **子协议** `webrtc.v1`（R-T6）：缺失或未知 → 握手 400（与 `broker_proto_mqtt_ws` 同一姿态，不静默降级）；客户端把 `webrtc.v1` 排在第二位时仍选中它（服务端优先）。
- **帧策略**（R-T3）：text 帧承载 JSON；binary → **close 1003**（不是上游那种只 warning 忽略），`binary_frames => ignore` 为显式退出口。
- **保活**（R-T4）：客户端发纯文本帧 `ping` → 回纯文本帧 `pong`（非 JSON）；原生 opcode 9 → 10 同时可用。两者并存，用例钉死。
- **认证窗口**（R-C1）：`auth_timeout_ms` 默认 **5000**（上游 `ws_auth_delay` 300 ms 太短、README 未记载）；超时 → **close 1008**。
- **空闲超时**：`idle_timeout_ms` 默认 60000，`infinity` 可关（两个方向都有用例）。
- **门控**：默认 `listeners => []`（出厂零端口，`webrtc_validate_SUITE` 断言）。
- **S10-A 不做**：房间成员、`to` 转发、`joined`/`left`、ACL。已认证消息带 `to` 时回 `invalid_message`（没有房间注册表就诚实说不能路由，不静默丢弃）。

### S10-B 房间与信令内核 — **已交付（2026-11-13）**

新增 5 个 src 模块 + 改造 `webrtc_conn` / `_sup` / `_app`（现有 11 个 src 文件），新增 4 个测试套件：

| 模块 | 实际职责 | 关键点 |
|---|---|---|
| `webrtc_room_registry` | 分片 supervisor（`one_for_all`）+ 公共 API：`join/4`、`leave/2`、`lookup/2`、`member/2`、`peers/1`、`rooms/0`、`shard_for/1`、`table_name/1`、`shard_count/0`、`shard_names/0` | 与 `a2a_task_registry` 同构：`erlang:phash2(Room, N) + 1`（**分片编号从 1 起**），`room_shards` 未设时取 `schedulers_online`；每分片一张 `protected` named table `webrtc_room_shard_<N>_members`，行 `{{Room, PeerId}, Pid, Username}`，读者免 call；**单节点**、无 Mnesia、**无 boot replay**（成员是连接态，重启后连接必然已断） |
| `webrtc_room_shard` | 每个分片一个 `gen_server`：`do_join/6`、`do_leave/3`、`remove/3`、30 s `sweep`、`erlang:monitor/2` | 「数成员 → 判容量 → 插入 → 告诉其他人」在同一把锁里完成；**广播在分片内扇出**（ADR-028 §D3）：`joined` 发给加入前已有成员、`left` 发给剩余成员；`maps:take` + `demonitor(Ref, [flush])` 保证恰一次 |
| `webrtc_signal` | 五类服务器事件的构造：`authenticated/1`、`unauthorized/0`、`invalid_message/0`、`joined/2`、`left/2` | 线格式严格按设计 §2.3；`unauthorized`/`invalid_message` **无 `data`**；私有 `text_event/1,2`；`json:encode/1` 在 OTP 28 返回 iodata，统一 `iolist_to_binary` |
| `webrtc_dispatch` | `deliver/3`（校验 `to`/`event` → `relay/3`）、`relay/3`（`lookup/2` + `Pid ! {text, Bin}`）、私有 `with_from/2` | 原报文 + **覆盖式** `from`（客户端自带的 `from` 被丢弃，R-S7）、**保留 `to`**；未知 / 跨房间 `to` 都是 `{error, unknown_peer}`，连接层翻成 `invalid_message`，**handler 不崩溃**（上游 F-1 的正确处置） |
| `webrtc_server` | 服务端 API：`peers/1`、`publish/2,3`、`send/3,4` | 未知 peer 返回 `{error, unknown_peer}`，**不抛 `badarg`**（上游 F-2）；`publish` 返回写入条数 |
| ACL（无独立模块） | 加入房间、向 `to` 发信令、服务端 API 三条路径都过 `broker_security:authorize/2`（`webrtc_conn:acl/2,3` 与 `webrtc_server`） | `authorize/2` 只读三个键：`action`（atom）、`tenant_id`、`destination`（ACL pattern 的匹配主体）。resource 形状 `#{action => signal, tenant_id => T, destination => <<"$webrtc/room/", Room/binary>>}`——**这个 `destination` 只当 ACL 模式主体用，不是路由目的地**（不进 `broker_router`）。`credential_capable/0` **不改**；`authorize(undefined, _)` 返回 `allow`，所以 `require_auth => true` 由适配器自己兜住 |

要点（全部已实现并有用例）：

- **房间键**由 WSS 路径绑定（R-T1）：只接受 `GET /websocket/<room>` 两段，其它形状 → 400。
- **`peer_id`** 由服务端 `crypto:strong_rand_bytes(10)` + base64 铸造（16 字符），**不可由客户端指定**（R-S7）。
- **广播集合**（R-S3，`webrtc_signal_SUITE` + `webrtc_room_SUITE` 钉死）：`joined` 只发**加入前已有**成员；`left` 发**剩余全体**（已离开的连接不再收）；`authenticated`/`unauthorized`/`invalid_message` 只回请求者。
- **房间隔离**（R-X4）：`to` 只能指向同房间 peer；跨房间 / 未知 / 空 `to` / 缺 `event` 一律回同一条 `invalid_message`（两种失败在线上不可区分，避免变成 peer_id 存在性预言机），连接保持可用。
- **人数上限**：`max_peers_per_room` 默认 **2**（首期 1:1）；超额 → `invalid_message` + close 1008；`peer_id` 冲突同路径（`peer_id_taken`）。
- **ACL 的两条拒绝姿态**：join 被拒 → `unauthorized` + close 1008（客户端拿不到用不了的 `peer_id`）；relay 被拒 → `invalid_message`（冻结集合里没有第六类事件，R-S1）。**join 与 relay 共用同一主体**（同一房间 = 同一 `$webrtc/room/<room>`），所以 ACL 无法「允许 join 但拒绝 relay」——这是计划 §1 的有意简化。
- **断线清理**（R-C2）：`terminate/2` → `leave/2` → `left` 扇出；被 `exit(Pid, kill)` 杀掉的连接靠分片的 `monitor` 兜底（恰一次 `left`，无幽灵成员）；分片崩溃重启后由 30 s `sweep` 清残留。
- **多人房间（>2）与成员快照推送** = P2，能力矩阵留行（R-S4）。`max_peers_per_room` 调到 3 时有「三人房间逐个寻址」用例，但出厂仍按 1:1 配置。

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

## 2. 测试矩阵

### 2.1 已交付（S10-A 实测 45 例，全部 `port => 0`；S10-B 的扩展见 §2.2）

| 套件 | 覆盖 | 实测例数 |
|---|---|---|
| `webrtc_conn_SUITE` | 5 个 group：**handshake** 4（子协议协商 / 缺失 → 400 / 未知 → 400 / 服务端优先）；**auth** 8（`authenticated` + 16 字符 `peer_id` / 两条连接 `peer_id` 不同 / 错口令 / 未知用户 / 缺字段 / 未认证发信令 / 未认证非 JSON / `require_auth => false` 握手即放行）；**frames** 10（文本 `ping`→`pong` / 原生 9→10 / 两者并存 / binary→1003 / `binary_frames => ignore` / 缺 `to`→`invalid_message` / 带 `to`→S10-A 仍 `invalid_message` / 客户端 close / 超限→1009 / RSV→1002）；**timeouts** 4（认证窗口→1008 / 空闲→1001 / 文本 `ping` 续命 / `infinity` 不关）；**tls** 1（`wss` 走同一份连接模块） | **27** |
| `webrtc_boundary_SUITE` | ADR-028 §D7 源码级守卫：无 `broker_queue:` / `broker_router:` / `#broker_message{` / `broker_core` include / `broker_subscription:` / `.cursors`（**扫描前剥离注释**，否则 ADR 引文会误报）；`.app` 的 `applications` 不含 `broker_queue`/`broker_router`/`broker_subscription`/`broker_event`，且含 `broker_security`/`broker_ws`/`ranch` | **6** |
| `webrtc_validate_SUITE` | 启动门控：默认值合法 / 出厂 `broker_security.require_auth = []` 不阻塞启动 / 回环 + 无认证放行 / 非回环 + 无认证 → `insecure_listener` / 非布尔 `require_auth` → `invalid_require_auth` / 负 `auth_timeout_ms` / 零 `idle_timeout_ms` / `infinity` 合法 / 零 `max_frame_bytes` / 空子协议 / 非 binary 子协议 / 默认零 listener | **12** |

### 2.2 已交付（S10-B 实测 54 例 + `webrtc_conn_SUITE` 扩 4 例 + 边界套件扩 3 例，全部 `port => 0`）

| 套件 | 覆盖 | 实测例数 |
|---|---|---|
| `webrtc_signal_SUITE` | **constructors** 6（`authenticated` 只带 `peer_id` / `unauthorized` 无 `data` / `invalid_message` 无 `data` / `joined` 带 `username`+`peer_id` / `left` 带 `username`+`peer_id` / 五类都是 text 帧）；**broadcast** 7（`joined` 只发已有成员、加入者自己不收、`left` 只发剩余成员、离开者不收、`authenticated`/`unauthorized`/`invalid_message` 只回请求者） | **13** |
| `webrtc_room_SUITE` | **registry** 4（`join`+`lookup` / `phash2` 分片归属与表名 / `rooms/0` 只列非空房间 / `peers/1` 全员）；**membership** 4（同 pid 重复 `join` 幂等 / `peer_id` 冲突被拒 / `leave` 幂等 / 空房间 `leave` 不抛）；**capacity** 3（默认 2 拒第三人且拒者不在房间 / 显式 `max_peers` 生效 / 未知房间空列表）；**lifecycle** 3（正常断开 → 剩余成员收 `left` / `exit(Pid,kill)` 后**恰一次** `left` 且无后续 / 无幽灵成员，房间从 `rooms/0` 消失） | **14** |
| `webrtc_route_SUITE` | **delivery** 3（`to` 直投 + `from` 追加 / 转发的帧保留 `to` / 三人房间逐个寻址且他人收不到）；**transparency** 3（SDP 逐字节透传 / ICE candidate 原文透传 / 未知 `event` 名原样透传）；**refusals** 5（缺 `to` / 未知 `to` / 空 `to` / 缺 `event` 各回 `invalid_message`，以及拒绝后连接仍可用）；**isolation** 3（跨房间不泄漏 / 跨房间与未知响应**完全相同** / 客户端伪造的 `from` 被服务端覆盖） | **14** |
| `webrtc_acl_SUITE` | **join** 4（允许的房间可加入 / 拒绝发生在 `authenticated` 之前且不进注册表 / 尾部 `/` 前缀规则覆盖所有房间 / 拒绝后 close 1008）；**relay** 3（允许则正常转发 / 被拒房间整个不可达 / 逐房间规则互不影响）；**server_api** 3（`send/4` 送达 / 未知 peer → `{error, unknown_peer}` / `peers/1`）；**not_bypassable** 3（`data` 属性不放大 ACL / `event` 名不放大 ACL / 消息体里的 `tenant_id` 无效） | **13** |
| `webrtc_conn_SUITE`（扩展） | 原 27 例 + **4 例**：分片 `authenticate`（opcode 1 FIN=0 + opcode 0 FIN=1 重组）成功、无起始帧的续帧 → close 1002、客户端 close 帧回显同 code、保留码（1005）→ 回 1000；并把旧名 `a_message_with_to_is_invalid_message_in_s10a` 改为 `a_message_with_an_unknown_to_is_invalid_message` | **31** |
| `webrtc_boundary_SUITE`（扩展） | 原 6 例 + **3 例**：无 `mnesia:`/`broker_meta:`（成员不是控制面元数据）、无 `syn:`（peer 查找是分片内 ETS，不是全局进程组）、**S10-B 六个模块都在扫描集合里**（守卫的覆盖率本身被断言） | **9** |

### 2.3 后续阶段（计划值，未实现）

| 套件 | 归属 | 覆盖 | 计划例数 |
|---|---|---|---|
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

## 4. 端口 / 配置 / 依赖表

`broker_proto_webrtc` 部分**已实现**（S10-A + S10-B）；`broker_ice` 部分仍是计划值。

### 4.1 端口

| 端口 | 传输 | 默认 | 状态 |
|---|---|---|---|
| 8084 | TCP（wss；明文仅回环） | **关**（`listeners => []`） | **S10-A 已实现**：app 随 release 装配，出厂不占端口。与既有 8080/8083/1883/8081/8082 零冲突 |
| 3478 | UDP | **关**（`enabled => false`） | 未实现（S10-C）。STUN + TURN（IANA 注册的标准端口） |
| 49152-65535 | UDP | 关（随 3478 一起开） | 未实现（S10-C）。TURN relay；可配窄区间，部署需放行 |
| ~~5349~~ | — | **不做** | TURN over TLS，首期非目标 |
| ~~3478/TCP~~ | — | **不做** | TURN over TCP，首期非目标 |

**端口占用复核（2026-11-12，写计划时实跑 `ss -ltnup`）**：`8080`、`8083` 正在监听（用户进程，勿动）；`8084`、`3478`、`49152` 段当时无占用。S10-A 交付时全量 CT 与两个 release 装配均在 `port => 0` 下跑完，未占用任何固定端口。

### 4.2 配置（`broker_proto_webrtc` / `broker_ice`）

**S10-A 已实现（`broker_proto_webrtc`）**：

| env | 默认 | 说明 |
|---|---|---|
| `listeners` | `[]` | 出厂零监听；示例配置给 `#{port => 8084}` |
| `require_auth` | `true` | **本 app 自己的键**（ADR-028 §D5，照 `broker_proto_a2a`/`broker_proto_mcp`）。`false` **只在所有 listener 都绑回环时**才被接受；非回环 + `false` → 启动失败 `{insecure_listener, Port, Ip}`。**不读 `broker_security.require_auth`**——那是「必须认证的协议名列表」 |
| `subprotocols` | `[<<"webrtc.v1">>]` | 缺失/未知 → 400；空或非 binary → 启动失败 |
| `auth_timeout_ms` | 5000 | 上游 `ws_auth_delay` 是 300 ms 且未记载；这里显式化。超时 → close 1008 |
| `idle_timeout_ms` | 60000 | `infinity` 可关（启动校验接受该值） |
| `max_frame_bytes` | 262144 | 单帧上限，直接交给 `ws_frame:decode/2`；超限 close 1009 |
| `binary_frames` | `reject` | `reject` → close 1003；`ignore` → 丢帧保持连接（上游行为，显式退出；不写日志） |
| `max_connections` | 1024 | 每个 listener 的连接上限（**代码兜底，`.app.src` 未声明**；listener spec 的 `max_connections` 逐条覆盖） |

**S10-B 新增（`broker_proto_webrtc`）**：

| env | 默认 | 说明 |
|---|---|---|
| `max_peers_per_room` | 2 | 首期 1:1。超额 → `invalid_message` + close 1008。`join/4` 的 `Opts` 可带 `max_peers` 逐次覆盖（用例用它构造三人房间）；非正整数 → 启动失败 `{invalid_max_peers_per_room, V}`。**代码兜底，`.app.src` 未声明** |
| `room_shards` | `undefined` → `schedulers_online` | 分片数（`phash2(Room, N) + 1`）。**改值必须重启**：分片进程不重建。非正整数 → 启动失败 `{invalid_room_shards, V}`。**代码兜底，`.app.src` 未声明** |

**后续阶段（未实现，计划值）**：

| env | 默认 | 说明 |
|---|---|---|
| `broker_proto_webrtc.log_signaling_payloads` | `false` | 显式开启才在 `debug` 记前 64 字节 + SHA-256 前缀（S10-D） |
| `broker_ice.enabled` | `false` | 出厂零 socket（S10-C） |
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

**净新增运行时依赖：0（S10-A 实测确认）。** `broker_proto_webrtc` 的 `applications` 只有 `kernel, stdlib, sasl, ssl, ranch, crypto, broker_ws, broker_security, broker_metrics`；`rebar.config` 的 `deps` 一行未改，只在两份 relx 清单各加一行 app 名。

## 5. DoD（对照 Gate S10）

**S10-A 已勾选**：

- [x] 子协议强制、binary → 1003、双 ping/pong、认证窗口 → 1008、空闲超时 → 1001、`infinity` 可关、默认零 listener，全部有真实 socket 用例（45 例 / 3 套件）
- [x] 认证接 `broker_security:authenticate_conn/1`；`peer_id` 服务端铸造（16 字符）；口令不进日志
- [x] ADR-028 边界由源码级守卫守护（无 `broker_queue:` / `broker_router:` / `#broker_message{` / `broker_core` include）
- [x] 既有 `rebar3 ct` 零回归：**1108/1108、71 套件**（基线 1063/68；设计稿 §4.3 写的 823/823 是 2026-11-11 写稿时的快照，冻结原文不改）；`dialyzer` 0 warning；`xref` 新 app 只剩 1 条**预期**告警（`broker_proto_webrtc_app:validate/0` 仅供 `webrtc_validate_SUITE` 调用，与 `broker_proto_acp_app:validate_agents/1` 同性质）；`rebar3 release` 与 `rebar3 as quic release` 均装配成功

**S10-B 已勾选**：

- [x] 五类服务器事件（`authenticated`/`unauthorized`/`invalid_message`/`joined`/`left`）的**广播集合与线格式**由 `webrtc_signal_SUITE`（13 例）+ `webrtc_room_SUITE`（14 例）钉死，`unauthorized`/`invalid_message` 确认**无 `data` 字段**
- [x] ACL 越权 / 跨房间 `to` / 未知 `peer_id` / 伪造 `peer_id` 专项用例通过且 **handler 不崩溃**（`webrtc_acl_SUITE` 13 + `webrtc_route_SUITE` 14）
- [x] 断线清理两条路径：`terminate/2` 与 `exit(Pid, kill)`（后者由分片 `monitor` 兜底，`left` 恰一次、无幽灵成员）
- [x] ADR-028 §D3 的「广播在分片内」由实现位置保证（`webrtc_room_shard` 持锁期间扇出），边界套件扩到 **9 例**（新增无 `mnesia`/`broker_meta`/`syn`，以及「六个新模块都在扫描集合里」）
- [x] 既有 `rebar3 ct` 零回归：**1169/1169、75 套件**（S10-A 基线 1108/71 + S10-B 54 例 / 4 套件 + `webrtc_conn_SUITE` 扩 4 例 + 边界 3 例）；`dialyzer` 0 warning（过程中删掉 3 处不可达的 `{binary, _}` 分支）；`xref` 全仓 265 条全部是 unused-export；两份 release 均装配成功

**Gate S10 剩余（未勾选）**：

- [ ] 一份**真实第三方客户端**证据落盘 `test/protocol-compatibility/s10/`：`RTCPeerConnection` 完成 offer/answer/candidate 全时序 + STUN/TURN 连通性；一行能力矩阵 + 逐项限制清单
- [ ] 信令「瞬时转发、不落盘」由 ADR-028 冻结并有测试证明（分区 `committed` offset 不变、数据目录无增长、`.cursors` 不写）
- [ ] STUN/TURN 认证接身份表、凭据轮换、日志脱敏有专项用例；端口矩阵与防火墙要求写入 `docs/operations.md`
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

## 7. 门禁实测表（逐阶段交付后回填）

**S10-B 实测（2026-11-13）**：

| 门禁 | 命令 | 结果 |
|---|---|---|
| 全量 CT | `rm -rf _build/test/extras && rebar3 ct` | **1169/1169 全绿、75 套件**（S10-A 基线 1108/71 + `webrtc_signal_SUITE` 13 + `webrtc_room_SUITE` 14 + `webrtc_route_SUITE` 14 + `webrtc_acl_SUITE` 13 + `webrtc_conn_SUITE` 27→31 + `webrtc_boundary_SUITE` 6→9），零 FAILED、零 Skipped |
| 静态分析 | `rebar3 dialyzer` | **0 warning**（初版 2 条：`send_event/2` 与 `webrtc_room_shard:send_frame/2` 的 `{binary, _}` 分支不可达——五类事件全是 text 帧，已删；同源的 `handle_info({binary, _})` 一并删） |
| xref | `rebar3 xref` | 全仓 **265 条**，全部 unused-export（S10-A 时 252 条；新增 13 条均为 `webrtc_room_registry`/`webrtc_room_shard`/`webrtc_server` 的对外 API 与 `start_link/0,1`，与既有 app 的测试入口同性质），零非预期告警 |
| Release（默认） | `rebar3 release` | 装配成功，`_build/default/rel/unified_mq/lib/` 含 `broker_proto_webrtc-0.1.0` |
| Release（quic profile） | `rebar3 as quic release` | 装配成功，`_build/quic/rel/unified_mq/lib/` 含 `broker_proto_webrtc-0.1.0` |
| 出厂 release 启动 | 装配出的 `sys.config`（无 `broker_proto_webrtc` 段）+ `-pa` 加载后 | `validate/0 → ok`；`ensure_all_started` 后监督树子进程只有 `webrtc_room_registry`，**零 listener**；`webrtc_room_registry:rooms() → []`；`shard_count() → 16`（= `schedulers_online`） |
| 信令证据 | `webrtc_peer.py`（aiortc） | **未做**（S10-D D.1；S10-B 只有 Broker 侧的 socket 级用例） |
| 「不落盘」证明 | S10-0 的 `assert_no_persistence` 脚本 | **未做**（S10-D D.2） |
| 压测 | 见 `docs/benchmarks/<date>-s10-capacity.md` §1 | **未做**（S10-D D.6） |

**S10-A 实测（2026-11-13）**：

| 门禁 | 命令 | 结果 |
|---|---|---|
| 全量 CT | `rm -rf _build/test/extras && rebar3 ct` | **1108/1108 全绿、71 套件**（基线 1063/68 + 本阶段 45 例 / 3 套件），零 FAILED、零 Skipped |
| 静态分析 | `rebar3 dialyzer` | **0 warning**（过程中修掉 2 条：`auth_timer` 收到 `cancel_timer/1` 的返回值而非 `undefined`；`close_code/1` 的兜底分支被判不可达 → 收成 `payload_too_large → 1009` + catch-all `1002`） |
| xref | `rebar3 xref` | 新 app 只剩 **1 条**预期告警：`broker_proto_webrtc_app:validate/0`（导出只服务 `webrtc_validate_SUITE`，与 `broker_proto_acp_app:validate_agents/1` 同一姿态）。初版另有 6 条已删（`webrtc_conn` 4 个测试导出 + `broker_proto_webrtc_sup` 2 个）。全仓 **252 条**，全部是 unused-export，其中 251 条属既有 app |
| Release（默认） | `rebar3 release` | 装配成功，`_build/default/rel/unified_mq/lib/` 含 `broker_proto_webrtc-0.1.0` |
| Release（quic profile） | `rebar3 as quic release` | 装配成功，`_build/quic/rel/unified_mq/lib/` 含 `broker_proto_webrtc-0.1.0` |
| 出厂 release 启动 | 以装配出的 `sys.config` 加载后 `broker_proto_webrtc_app:validate/0` | `ok`；`listeners => []`；`ensure_all_started(broker_proto_webrtc)` → `{ok, [...broker_proto_webrtc]}`，监督树子进程为空（零端口） |
| 信令证据 | `PYTHONPATH=/tmp/opencode/s10-libs /usr/bin/python3 test/protocol-compatibility/s10/webrtc_peer.py` | **未做**（S10-D；S10-A 无 `to` 转发，1:1 信令走不通） |
| STUN/TURN 证据 | 同上（`turn_allocation.py` + 裸 UDP `stun_raw.py`） | **未做**（S10-C） |
| 「不落盘」证明 | S10-0 的 `assert_no_persistence` 脚本 | **未做**（S10-D D.2） |
| 压测 | 见 `docs/benchmarks/<date>-s10-capacity.md` §1 | **未做**（S10-D D.6） |

## 8. 落地偏差记录

> 格式照 `docs/design/s9-f-execution-plan.md` §落地偏差记录：每条写「计划 → 实际 → 为什么 → 留痕位置」。设计稿原文不改，偏差只在这里与 ADR 批注里记。

### S10-B（2026-11-13）

1. **`webrtc_server` 的实际接口是 `send/3,4`，不是 `send/2,3`**
   - 计划：§1 S10-B 表格写 `peers/1`、`publish/2,3`、`send/2,3`。
   - 实际：`send(Room, PeerId, Event)` + `send(Room, PeerId, Event, Data)`——地址是 `(Room, PeerId)` 两段（与 `to` 一样，peer id 只在房间内有意义），再往后才是事件与数据。`send/2` 会是一个只有 `(Room, Event)` 的畸形签名。
   - 留痕：`webrtc_server.erl` 的模块注释与 `-export`。

2. **`webrtc_dispatch` 的切分是 `deliver/3` + `relay/3`**
   - 计划：只描述职责（「校验 `to` → 房间归属 → `Pid ! {text, Frame}`」），未定签名。
   - 实际：`deliver/3`（客户端消息，含 `to` 校验与 `event` 校验）与 `relay/3`（已寻址帧，`webrtc_server` 与测试复用寻址那一半）分开，避免服务端 API 被迫伪造一个客户端消息。
   - 留痕：§1 表格该行、`webrtc_acl_SUITE:server_api` 组。

3. **`max_peers_per_room` / `room_shards` 只做代码兜底，`.app.src` 不声明**
   - 计划：§4.2 只列键与默认值。
   - 实际：两个键都不在 `.app.src` 的 `env` 里，默认值写在 `broker_proto_webrtc_app:max_peers_per_room/0`、`room_shards/0`，`validate/0` 只校验「若设置则必须是正整数」（`{invalid_max_peers_per_room, V}` / `{invalid_room_shards, V}`）。理由与 `max_connections` 一致：出厂不需要它们，声明反而让 `sys.config` 看起来必须配。
   - 留痕：§4.2 表格的「代码兜底，`.app.src` 未声明」标注、`broker_proto_webrtc_app:check_rooms/0`。

4. **ACL 的两条拒绝姿态不同（join 用 `unauthorized` + 1008，relay 用 `invalid_message`）**
   - 计划：只说「三条路径都过 `authorize/2`」。
   - 实际：join 被拒 → `unauthorized`（无 `data`）+ close 1008，且**先判 ACL 再 `join`**，客户端永远拿不到一个用不了的 `peer_id`；relay 被拒 → `invalid_message`（无 `data`），连接保持可用。冻结集合只有五类事件（R-S1），拒绝信令没有第六类可用，`invalid_message` 是唯一诚实的选择。
   - 留痕：`webrtc_conn:go_active/2`、`webrtc_conn:relay/3` 的注释、`webrtc_acl_SUITE` 的 join/relay 两组。

5. **join 与 relay 共用同一个 ACL 主体，因此 ACL 无法「允许加入、拒绝转发」**
   - 计划：resource 形状冻结为 `#{action => signal, tenant_id, destination => "$webrtc/room/<room>"}`（§1），同一房间的 join 与 relay 自然同址。
   - 实际：照此实现，并把这个后果写进用例：逐房间规则只能「整个房间允许/拒绝」，不能拆 join 与 relay。若将来需要拆分，要新增一个 `action`（例如 `join_room`）并改 ADR-028 §D5 的冻结文本——不在实现里偷偷加第二个主体。
   - 留痕：`webrtc_acl_SUITE:a_denied_room_cannot_be_reached_at_all/1` 的注释。

6. **匿名身份必须是 `principal => <<"anonymous">>, tenant_id => undefined`**
   - 计划：未写（S10-A 的 `require_auth => false` 分支没有 ACL）。
   - 实际：S10-B 引入 ACL 后，匿名连接若填 `broker_security:default_tenant()`，`authorize/2` 会因为租户不匹配把**所有**信令拒掉（包括默认 `acl => allow` 的场景之外的一切）。改为 `tenant_id => undefined`——「没有凭据就没有租户可隔离」，与 `mcp_dispatch` 的匿名姿态一致。
   - 留痕：`webrtc_conn:anonymous/0` 的注释。

7. **`{error, unknown_peer}` 不区分「不在这个房间」与「不存在」**
   - 计划：R-X4 只说跨房间要拒。
   - 实际：两种失败在线上完全同形（都是 `invalid_message`），用例直接断言两次响应**相等**。留一个可区分的错误码会让 `to` 变成 peer id 存在性预言机。
   - 留痕：`webrtc_dispatch.erl` 模块注释、`webrtc_route_SUITE:a_peer_id_from_another_room_is_indistinguishable_from_unknown/1`。

8. **`peer_id_taken` 与 `room_full` 走同一条拒绝路径**
   - 计划：只写 `max_peers_per_room`。
   - 实际：`join/4` 的两个错误（容量满、peer id 已被活着的成员占用）在连接层都翻成 `invalid_message` + close 1008。`peer_id` 由服务端 `strong_rand_bytes(10)` 铸造，冲突在概率上不可达，但它必须有确定行为而不是覆盖掉一个活着的 peer。
   - 留痕：`webrtc_room_registry:join/4` 的 `-spec`、`webrtc_room_SUITE:a_taken_peer_id_is_refused/1`。

9. **补上 S10-A 记的两处传输层缺口（§S10-A 第 13 条）**
   - 计划：S10-A 如实记录「未做分片重组、close 帧不回显」，并写明「应与 S10-B 的转发一起做并一起测」。
   - 实际：`webrtc_conn` 增加 `frag = {Op, Acc, Size}` 状态，续帧（`opcode 0`）按首帧 opcode 分发；非法分片序列 → close 1002。收到客户端 close 帧时**回显**其 code（1005/1006/1015 与越界值统一回 1000），再关 socket。`webrtc_conn_SUITE` 相应补 4 例：分片 `authenticate` 重组成功、无起始帧的续帧 → 1002、close 帧回显、保留码 → 1000（`ws_frame:encode/3` 的第三参是 mask 位、且恒置 FIN=1，所以分片帧由测试侧手写 `frag_frame/3`）。
   - 留痕：`webrtc_conn:on_frame/2`、`webrtc_conn_SUITE` 的 frames 组（27→31）、`docs/security.md` §12.2 第 11 条已划掉。

10. **`room_shards` 的用例用运行时实际分片数断言，不热改 env**
    - 计划：「`phash2` 分片归属」有用例。
    - 实际：初版在用例里 `set_env(room_shards, 4)` 再断言 4 个分片——但分片进程在 app 启动时就建好了，改 env 不会重建；那样断言会在「实现正确、断言错」和「实现错、断言也过」之间摇摆。改为读 `shard_count/0` 与 `shard_names/0`，断言 `table_name(Room)` 与 `webrtc_room_shard:name(phash2(Room, N) + 1)` 一致（**编号从 1 起**，偏移是初版写错的地方）。
    - 留痕：`webrtc_room_SUITE:placement_is_phash2_sharded/1` 的注释。

11. **`webrtc_conn_SUITE` 要显式抬高 `max_peers_per_room`，握手响应必须逐字节读**
    - 计划：未写（S10-A 时没有房间概念）。
    - 实际：两处连带修改。①该套件所有用例共用 `/websocket/room1`，`a_second_connection_gets_a_different_peer_id` 累计开 4 条连接，S10-B 的默认容量 2 会让第 3 条收到 `invalid_message`——容量是 `webrtc_room_SUITE` 的主题，这里把上限抬到 32。②`raw_handshake_on/3` 原来用 `recv(S, 0)` 循环直到 `\r\n\r\n`，`require_auth => false` 时服务端会在 101 之后立刻推 `authenticated` 帧，两个写合并到同一个 segment 时该帧会被当成「握手响应的一部分」吞掉（`string:lowercase/1` 在 0x81 上报 `badarg`）。改为逐字节读到终止符，不再越过响应边界。
    - 留痕：`webrtc_conn_SUITE:init_per_testcase/2` 与 `recv_header/3` 的注释。

12. **删掉三处不可达的 `{binary, _}` 分支**
    - 计划：无（S10-A 留下的形状）。
    - 实际：`webrtc_conn:send_event/2`、`webrtc_room_shard:send_frame/2` 各有一个 binary 分支，`webrtc_conn:handle_info({binary, _})` 有一个入站分支。五类事件全是 text 帧（设计 §2.3），转发路径也只发 `{text, Frame}`，dialyzer 报前两处「永不匹配」。三处全删——留着会让人以为信令支持二进制帧。
    - 留痕：本节 §7 dialyzer 行。

### S10-A（2026-11-13）

1. **`require_auth` 的读取位置**
   - 计划：`broker_proto_webrtc.require_auth`，照 `broker_proto_a2a_app:check_auth/0`。
   - 实际：初始实现从 `broker_security.require_auth` 读（当成布尔）。**这是启动缺陷**：该键是「必须认证的协议名列表」，出厂值为 `[]`，于是装配成功的出厂 release 一启动就 `{error, {invalid_require_auth, []}}`；运维写 `[<<"ws">>]` 同样启动失败。已改为读本 app 自己的 `broker_proto_webrtc.require_auth`，并把「出厂 `broker_security` 配置不阻塞启动」与「非布尔值被具名拒绝」写成两条用例。
   - 为什么：ADR-028 §D5 明写「适配器自有 `require_auth`，不改 `broker_security` 语义」；我把「适配器自有」读成了「从 `broker_security` 取」，方向正好相反。
   - 留痕：`broker_proto_webrtc_app.erl` 的 `auth_env/0` 注释、`webrtc_validate_SUITE:the_shipped_security_config_does_not_block_boot/1`、ADR-028「落地偏差记录」§S10-A 第 1 条。

2. **`webrtc_listener:start_link/3` 不能包一层进程**
   - 计划：「照 `mqtt_ws_listener` 形态」（`a2a_listener` 用 `spawn_link` 包 `a2a_conn:start/3`）。
   - 实际：照 A2A 写 `spawn_link(fun() -> webrtc_conn:start(...) end)`，ranch 拿到一个立刻退出的 pid，把 listener 直接撕掉——所有握手用例 `{error, closed}`。改为直接返回 `webrtc_conn:start_link/3` 的 gen_server pid。
   - 为什么：`a2a_conn:start/3` 是**长期驻留**的入口（自己进 receive 循环），`webrtc_conn` 是 gen_server（`start_link` 返回即已驻留）；包一层后外层函数立刻返回，内层 pid 与 ranch 的关系就断了。
   - 留痕：`webrtc_listener.erl` 模块注释。

3. **计时器必须用 `erlang:send_after/3`**
   - 计划：「认证窗口 / 空闲窗口用计时器」。
   - 实际：先写 `erlang:start_timer/3`，它投递的是 `{timeout, Ref, Msg}`，而 `handle_info` 匹配的是裸 `auth_timeout` / `idle_timeout`，两个窗口**静默不生效**（用例表现为「等 2 s 也没关连接」）。改 `send_after/3`。同时 `idle_timeout_ms => infinity` 走「不起计时器」而不是「起一个永不触发的计时器」。
   - 为什么：`start_timer/3` 与 `send_after/3` 的投递形状不同，看名字容易混。
   - 留痕：`webrtc_conn:start/2` 的注释、`an_infinite_idle_timeout_never_closes_the_connection/1`。

4. **`{active, once}` 必须每次重新 arm**
   - 计划：未单列（模板细节）。
   - 实际：只 arm 一次时连接只收第一个 chunk；改为每次 `{tcp|ssl, _, Data}` 处理完重新 `setopts`。
   - 留痕：`webrtc_conn:handle_info({T, _S, Data}, ...)` 注释。

5. **认证窗口超时与空闲超时改为「发关帧再关」**
   - 计划：R-C1 写「超时未认证 → close 1008」；空闲超时未写码。
   - 实际：初版直接 `{stop, ...}`（裸 TCP 关闭，客户端区分不出「太慢」与「网络断」）。改为先发 `close 1008`（认证窗口）/ `close 1001`（空闲），再关 socket；两条各有用例断言关闭码。
   - 留痕：`webrtc_conn` 的两个 `handle_info`、`timeouts` group。

6. **未单开 `webrtc_auth` 模块**
   - 计划：独立模块 `webrtc_auth` 负责认证。
   - 实际：认证逻辑只有 `try_authenticate/2` 一个函数（读 JSON 两个字段 → 调 `broker_security:authenticate_conn/1` → 回事件），放在 `webrtc_conn` 内；模块化会为一个 15 行的函数增加一层间接。
   - 为什么：不为形式拆模块；口令仍然只在该函数栈上出现，不进日志/指标（有边界用例扫源码）。
   - 留痕：§1 S10-A 表格的「认证」行。

7. **`require_auth => false` 时服务端主动下发 `authenticated`**
   - 计划：设计稿 §2.3 把 `authenticated` 定义为 `authenticate` 的**回复**。
   - 实际：关闭认证时没有请求可回复，但客户端仍需要 `peer_id`（R-S7 规定只能服务端铸造），所以握手完成即主动推一条 `authenticated`，随后连接直接进 `active`。
   - 为什么：不给客户端 `peer_id` 会让它在 S10-B 里无法被 `to` 寻址；这是关闭认证时才出现的分支，默认路径不受影响。
   - 留痕：`auth_disabled_goes_free_at_handshake_time/1` 的断言与注释。

8. **`binary_frames` 取代 `binary_frame_policy`**
   - 计划：§4.2 写 `binary_frame_policy => close_1003`。
   - 实际：实现为 `binary_frames => reject | ignore`（`reject` 即 1003）。原计划名暗示可配多种关闭码，但设计稿只要求 1003 一种，多留一个枚举值是无用配置。
   - 留痕：§4.2 已改为实际键名。

9. **`broker_core` 不进 `.app` 的 `applications`**
   - 计划：§1 S10-A 表格原列 `broker_core`。
   - 实际：不列。本 app 不调用、不 include，`broker_core` 只经 `broker_security` 传递进来；列上会让依赖账本看起来像「需要内核」，与 ADR-028 §D8 相悖。守卫用例断言 `broker_queue`/`broker_router`/`broker_subscription`/`broker_event` 都不在列表里。
   - 留痕：`broker_proto_webrtc.app.src` 注释、`webrtc_boundary_SUITE:app_dependency_ledger_is_clean/1`。

10. **守卫测试必须剥离注释**
    - 计划：「grep `src/*.erl`」。
    - 实际：直接 grep 会把注释里引用的 `broker_queue:append/3`（ADR 原文引用）当成调用，用例自己先红。改为扫描前剥离 `%` 到行尾（跳过字符串字面量）。
    - 为什么：守卫测试若因文档措辞而失败，一周内就会被关掉。
    - 留痕：`webrtc_boundary_SUITE:strip_comments/1`。

11. **`webrtc_boundary_SUITE` 的源码定位**
    - 计划：未写。
    - 实际：`_build/test/lib/<app>/src` 是符号链接，`filelib:wildcard/1` **不跟随符号链接目录**，会数出 0 个文件。改为先 `file:read_link_all/1` 解析真实路径。
    - 留痕：`webrtc_boundary_SUITE:real_path/1`。

12. **`webrtc_conn` 删掉未使用的测试导出**
    - 计划：未写。
    - 实际：初版导出了 `event_json/1,2`、`close_code/1`、`mint_peer_id/0` 供测试使用，但测试最终都走真实 socket 断言，无一处调用——xref 报 4 条 unused export。已删。
    - 留痕：本节 §7 xref 行。

13. **未做 WebSocket 分片重组，客户端 close 帧不回关帧（两处如实记录的传输层缺口）**
    - 计划：R-T2 只要求「复用 `broker_ws`，不写第二份 RFC 6455」；R-T3 只规定帧类型策略。
    - 实际：①`ws_frame:decode/2` 把 `fin` 如实返回（分片由连接进程处理），但 `webrtc_conn:on_frame/2` 不看 `fin`——首片直接进 `handle_text/2`，`opcode 0` 续帧落兜底子句被忽略。对比 `ws_conn` 与 `mqtt_transport_ws` 都做了 `frag = {Op, Acc}` 重组。②收到客户端 close 帧时 `stop_now/1` 直接关 socket，**不回 close 帧**（RFC 6455 §5.5.1 是 SHOULD，不是 MUST）。
    - 为什么如实记录而不在本阶段修：本阶段没有 `to` 转发，分片导致的后果只是「分片 `authenticate` 被判 `unauthorized`」——不误路由、不崩溃；浏览器信令客户端默认不分片。补重组属于「半条消息当完整消息路由」的风险面，应与 S10-B 的转发一起做并一起测。
    - 留痕：`docs/security.md` §12.2 第 11 条、`docs/protocol-compatibility.md` §4.20.3 表与 §4.20.5。
