# ADR-029：ICE 组件、端口矩阵与部署边界——`broker_ice` 独立 app、UDP 3478 + relay 段、默认关闭

- **状态**：已采纳（S10-0 评审签认，2026-11-13）
- **日期**：2026-11-13
- **关联**：ADR-007（显式可靠性——TURN allocation 的生命周期即一种可靠性承诺）、ADR-016（可观测——指标默认不加新键）、ADR-021（集群控制面——单节点语义的边界）、ADR-027（前端依赖账本与 relx 双清单口径）、ADR-028（信令面边界——ICE 面与信令面互不依赖）
- **关联文档**：[`docs/design/s10-webrtc-signaling.md`](../design/s10-webrtc-signaling.md) §2.2（端口表）、§3.2（R-I1~R-I6）、[`docs/design/s10-0-convergence-report.md`](../design/s10-0-convergence-report.md) §2（OQ-3~OQ-6 收敛）、[`docs/design/s10-execution-plan.md`](../design/s10-execution-plan.md) §4.1/§4.3
- **事实来源**：IANA 端口注册表（3478 stun/turn、49152-65535 动态/私有段）；`lambdaclass/webrtc-server`（`master`）README 的防火墙清单与 `stun_listener:add_listener(3478, udp, Opts)` 调用；hex.pm 上 `cowboy 2.0.0`（要求 `ranch ~> 1.4.0`）、`stun 1.0.20`（依赖 `fast_tls 1.0.20` + `p1_utils 1.0.10`）的元数据；RFC 5766（TURN UDP）、RFC 5389/8489（STUN）

## 背景（Context）

S10 的第二个组件是为浏览器 `RTCPeerConnection` 提供 STUN/TURN 辅助（媒体面仍不做，RTP/RTCP 不经服务端）。设计稿 §2.2 给了上游的端口选择（3478 UDP、5349 tcp/udp、49152-65535 udp），§3.2 的 R-I1~R-I6 划了范围。上游 README 的防火墙清单与源码只 `add_listener(3478, udp, ...)` 之间存在差（README 记 5349，源码未建），本 ADR 必须钉死**我们**实现哪些、不做哪些，以及为什么。

必须一起定下来的问题：①ICE 功能放哪个 app、依赖什么；②端口矩阵与默认状态；③TURN 认证如何接既有身份表；④哪些 TURN 能力首期裁剪；⑤单节点语义与集群边界；⑥运维/防火墙要求落点。

## 决策

### D1. 独立 app `broker_ice`，与信令面、与消息内核全部解耦

- 新增 `broker_ice`：`applications` 只含 `kernel, stdlib, sasl, crypto, broker_security, broker_metrics`。**不依赖** `ranch`、`broker_ws`、`broker_proto_webrtc`、`broker_queue`、`broker_router`。
- 与信令面互不依赖：S10-A 完成后 S10-C 即可开工（执行计划 §1）。
- **不引入 `stun` hex 包**（`1.0.20` 拖 `fast_tls 1.0.20` + `p1_utils 1.0.10`；其附加价值集中在 5349/TLS，而 5349 首期不做——见 D2）。STUN/TURN 编解码在 tree 内手写（20 字节头 + 少量属性，规模可控且有测试钉死）。
- `broker_metrics` 复用既有键（`connections_current` 等），**新键默认不加**（ADR-016 冻结键集；与 S9-F §12.1 第 17 条同一姿态）。

### D2. 端口矩阵（冻结）与裁剪

| 端口 | 传输 | 默认 | 状态 |
|---|---|---|---|
| 3478 | UDP | 关（`enabled => false`） | **做**：STUN Binding + TURN（IANA 注册端口） |
| 49152-65535 | UDP | 关 | **做**：TURN relay；部署侧可配窄区间（如 49200-49300），防火墙表写入 `docs/operations.md` |
| 5349 | TCP+UDP | — | **不做**：TURN over TLS |
| 3478 | TCP | — | **不做**：TURN over TCP |
| — | DTLS / IPv6 候选 / ICE-lite | — | **不做**（R-I6） |

- 上游 README 与源码的 5349 差异按「README 记载为准、源码未建」处理；我们首期两端口都不建。
- **8084/TCP 是信令前端（`broker_proto_webrtc`）的端口，不属本 ADR**；本 ADR 只管 UDP 面。两者默认全关、互不影响。

### D3. TURN 长期凭据接既有身份表，口令不进日志

- 认证走 `broker_security:authenticate_conn/1 + authorize/2`（`action => turn_allocate` 等），与信令**同账号**（同一张 `users` 表），不引入 KMS。
- TURN 密钥派生 RFC 5766 §4：`key = MD5(username ":" realm ":" password)`；`realm` 为必填 env，缺失或 `turn_ip` 缺失或 relay 段非法 → **启动期显式失败**（照 `broker_proto_a2a_app:check_auth/0` 的 loud-fail 姿态）。
- 凭据轮换 = 改用户记录 + 客户端 `Refresh` 重认证；不建凭据缓存层。
- 日志/审计只记 username、realm、allocation 元数据，**从不记 password 与 key**。

### D4. TURN 协议范围（首期）：仅 UDP

- 实现：`Allocate` / `Refresh` / `CreatePermission` / `ChannelBind` / `ChannelData` / `Send` / `Data`（RFC 5766 的 UDP 面）+ STUN Binding（RFC 8489）与 `XOR-MAPPED-ADDRESS`、`MESSAGE-INTEGRITY`、`ERROR-CODE`、`FINGERPRINT`。
- 每 allocation 一个 `temporary` 进程；生命周期（默认 600 s，`Refresh` 可续）由进程 + 注册表双侧管理，端口池耗尽即拒绝（`max_allocations` 默认 1024）。
- 不做：TURN-TCP/TURN-TLS、DTLS、权限的 TCP 形式。

### D5. 单节点语义与集群边界

- **首期单节点**：`broker_ice` 无 ra、无 Mnesia；allocation 与 relay 端口都是节点内资源。
- 集群需求（跨节点 TURN 一致性、relay 端口在节点间如何分片）→ 另立 ADR（涉及 ADR-021），S10 不做。
- 与 ADR-028 D4 的房间注册表同属「单节点控制面」家族，但两者**互不依赖**，可独立回退。

### D6. 运维与文档落点

- `docs/operations.md`：端口表（3478/UDP + relay 段 + 可选窄区间）、防火墙放行要求、TURN 客户端 `iceServers` 配置示例（`turn:user:pass@host:3478`、`iceTransportPolicy` 说明）。
- `docs/security.md`：§12.2 记录 TURN 相关明确未实现项（TCP/TLS 形态、IPv6、集群语义）。
- `docs/protocol-compatibility.md`：能力矩阵给 STUN/TURN 一行 + 限制清单（首期为 S10-D 的 D.5 产出，本 ADR 只冻结口径）。

## 后果（Consequences）

- 正向：ICE 面零内核依赖、默认零 socket；删 app 即完全回退；手写编解码避免拖入 `fast_tls`/`p1_utils` 两个新依赖，净新增运行时依赖保持 0。
- 代价：无 TURN over TLS → 明文 UDP 上传输长期凭据的派生密钥（MESSAGE-INTEGRITY 完整性有保护，但链路无 TLS 加密）；跨公网部署需自行评估。这是把 5349 排到 P2 的直接后果，D6 要求写进安全文档，不允许实现期悄悄补 TLS 而不改 ADR。
- 与既有 ADR 的关系：不 amend 任何一条；D3 复用既有身份/ACL 面。

## 落地偏差记录

（S10-C 起逐条追加，格式：计划 → 实际 → 为什么 → 留痕位置。冻结文本不改。）
