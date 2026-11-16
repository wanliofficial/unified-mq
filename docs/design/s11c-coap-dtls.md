# S11-C 设计：CoAP DTLS（仅 PSK）+ 安全与一致性收口

**阶段**：S11-C（`docs/roadmap.md` §13.1，ADR-031 §D5 的落点 / OQ-6 的收敛）
**前置**：S11-A（`broker_proto_coap`：编解码 / 资源映射 / UDP listener）、S11-B（Observe/RFC 7641）均已交付并合并（全量 CT 1347/1347、84 套件）
**冻结合同**：ADR-031 §D5（DTLS 接线、默认不自签）、§D2/§D3（不变）；R-T2 / F-5（`docs/design/s11-coap.md` §3）
**实测依据**：[`docs/benchmarks/2026-11-16-s11c-dtls-psk-spike.md`](../benchmarks/2026-11-16-s11c-dtls-psk-spike.md)（OTP 28.3.1 / ssl 11.5.1 上 DTLS+PSK 全部实测通过）

## 1. 目标与边界

把 CoAP 的**明文 UDP 前端**升级为**可选的 DTLS 前端**，并把客户端身份接进 `broker_security` 的统一身份表，使 `principal` 不再是 `undefined`、ACL 真正能拒绝一条请求（4.03）。这是 S11-B 明确留下的唯一安全缺口（`docs/security.md` §12.5：plain UDP 下 `principal = undefined` 故 ACL 无法拒绝 datagram）。

**本阶段做**：

1. `coap_listener` 支持 `{transport, dtls}`：**仅 PSK**、**默认不自签证书**（cert-free PSK 套件，spike 已证）。
2. PSK identity ↔ `broker_security` `users` 表：新增派生导出，**不新建凭据存储**、不引 KMS。
3. `run_resource` 的 `Ctx` 带真实 `principal`/`tenant`，ACL/租户隔离对 DTLS 生效。
4. 限流加固：复用既有令牌桶 + 新增**握手级/会话级**上限与**连接关闭清理**。
5. 能力矩阵、`docs/security.md`、`docs/roadmap.md` 同步；ADR-031 定稿（落地偏差记录追加）。
6. **容量/压测报告**落盘。

**不做（本阶段明确非目标）**：

- **证书 / mTLS**：DTLS 只走 PSK（本会话已定）。`broker_security_tls` 的证书路径是 **TCP TLS（ranch_ssl）**，与本阶段无关，不改。
- **libcoap 真实客户端证据**：单列、推迟（本会话已定），不作为 S11-C 的 Gate 证据。
- **OSCORE**、**CoAP over TCP/WebSocket（RFC 8323）**、**服务端 Block2**：已冻结非目标（`s11-coap.md` §1.3）。
- **跨节点 Observe 恢复**：超单节点 UDP 前端范围（`docs/architecture.md` §3.18 已声明单节点）。

## 2. 范围决策（本会话已确认，写死）

| 决策 | 取值 | 理由 |
|---|---|---|
| DTLS 凭据模式 | **仅 PSK** | spike 证明 cert-free PSK 可握手，「默认不自签」可满足；证书会引入 PKI 运维面，ADR-031 §D5 未要求 |
| libcoap 证据 | **推迟，单列** | 需真实 C 客户端与工具链，独立于本阶段代码正确性 |
| 容量/压测 | **纳入本阶段** | Gate S11 要求「压测 / 容量报告落盘」（roadmap §13.1） |

## 3. 进程模型：acceptor + listener 持多个 DTLS socket

### 3.1 形状（spike 已证）

UDP 的模型是「**一个进程 = 一个 socket owner + 全部有界状态**」（ADR-022 的形，套在请求/响应协议上）。DTLS 是连接导向的：每个对端一个 `sslsocket`。**不引入每会话一进程**，而是：

```
coap_listener (gen_server, 唯一状态持有者)
  ├─ udp 模式:  #st.socket = gen_udp socket
  └─ dtls 模式: #st.socket = ssl:listen socket（listen 用）
                接收 {ssl, SslSock, Bin}（每个已建立的对端 socket 一条）
    └─ coap_dtls_acceptor (spawn_link, 非 gen_server，串行循环)
         循环: ssl:transport_accept → ssl:handshake（内联）
                → drain identity（成功/失败两路径都 drain）
                → ssl:peername
                → {dtls_handshake_ok, self(), Ref, S, Peer, Principal} 问 listener 要不要
                → listener 回 {dtls_admit, Ref, ok|refuse}
                → ok: ssl:controlling_process(S, ListenerPid) ；refuse: ssl:close(S)
```

**为什么准入要一次往返**：`controlling_process` 必须由当前 owner（acceptor）执行，而「这个会话是否超过上限」只有 listener 知道（表在它那儿）。把计数复制一份到 acceptor 就是两份真相。一次往返换来单点计数。

**active 模式的唯一正确时序**（spike11/12 实测）：握手与移交期间 socket 保持 `{active, false}`；**移交之后由 listener（新 owner）** 才设 `{active, ?ACTIVE_N}`。若 acceptor 先 `setopts({active,N})` 再移交，`setopts` 返回 `ok` 却把前 N 条 datagram 投进 acceptor 的邮箱（随后随进程退出静默丢失）。`{ssl_passive, Sock}` 会正常发出，UDP 的重挂子句可原样复用。

spike8 实测：一个普通进程可同时持有 2+ 个 active `sslsocket`，收 `{ssl, Sock, Bin}`，用 `Sock -> {IP,Port}` 映射 demux，并回 `ssl:send`。**因此现有「单进程持全部有界表」的姿态原样保留**，五张表的键仍是 `{IP,Port}`（DTLS 的 `ssl:peername` 给的就是对端 `{IP,Port}`，与 UDP 同形）。

### 3.2 硬约束（写进实现，不可偏离）

1. **握手必须串行**：身份只能在 `user_lookup_fun` 里捕获（spike9：`ssl:connection_information` **不暴露** `psk_identity`，返回 `{ok,[]}`）。lookup fun 的 `State` 在 `ssl:listen` 时固定，所以 identity 消息只有**一个**接收者；并发握手会让「A 的身份」串到「B 的连接」（spike8 首版实测到的竞态）。**一个 DTLS listener 一个 acceptor，串行 `transport_accept → handshake → 移交`。**
2. **成功与失败两条路径都要 drain identity**：被拒连接（未知身份）的 identity 消息会留在 acceptor 邮箱，不 drain 就会串给下一个连接（spike8 修正点）。
3. **listener 不碰握手**：它只接收已建立的 socket。acceptor 死亡 → listener 停 → supervisor `one_for_all` 重启（与 bind 失败同类），不出现「端口有人 listen 但没人读」。
4. **串行握手是已接受的上限**：握手阶段吞吐 = 1 / (单次握手耗时)，由 `dtls_transport_accept_timeout_ms` 与 `dtls_handshake_timeout_ms` 双重限时。容量报告给出实测握手速率，这一点在能力矩阵里如实声明（不是缺陷，是选择：并行握手需要暴露身份竞态，代价比收益大）。
5. **握手期 socket 必须 passive**：`{active, false}` 从 `transport_accept` 保持到 `controlling_process` 完成；active 只由移交后的 listener 设。`setopts` 在非 owner 上返回 `ok` 但投递到错误的邮箱——静默丢包，不留痕迹（spike11 实测）。

### 3.3 为什么 acceptor 是普通进程而不是 gen_server

它没有 call/cast、没有状态表（握手成功后立刻移交），只有「循环 + 限时 + 把错误记进日志」。做成 gen_server 只会多一层 mailbox 协议而没有状态可管。它与 listener `spawn_link`，listener 已 `trap_exit`，收到 `{'EXIT', Acceptor, Reason}` 即 `{stop, {dtls_acceptor_died, Reason}}`。

## 4. 身份与 PSK：OQ-6 的落地形状

### 4.1 映射

> **DTLS 的 PSK identity 逐字就是 `broker_security` `users` 表里的用户名；PSK 由该用户的 `password` 派生（域分隔 KDF）；未命中即拒绝握手。**

- `Fun(psk, Identity, State)` 拿到的 `Identity`（binary，如 `<<"device-01">>`）→ `broker_security:psk_for_identity/1`。
  - `{ok, Psk}` → 握手继续；acceptor 把 `Principal = Identity` 交给 listener。
  - `{error, _}` → 返回 `#alert{?FATAL, ?UNKNOWN_PSK_IDENTITY}` → 客户端在 ~224 ms 内收到 `{tls_alert, {handshake_failure, _}}`（spike10 实测，干净拒绝，非挂死）。
- **principal = Identity 本身**（与 S6/S1 的 `principal` 定义一致：principal 是认证身份）。
- **tenant = `broker_security:tenant_id(Identity)`**（身份记录里带的租户，客户端不能自选）。

### 4.2 密钥派生（`broker_security:psk_for_identity/1`，新增导出）

```
PSK = SHA-256( "unifiedmq-coaps-psk" || 0x00 || Identity || 0x00 || Realm || 0x00 || Password )
```

- **口令不过线**：服务端自己派生，客户端用同一 KDF 从自己的 password 算出 PSK（与 TURN 的 `broker_security:long_term_key/2`（`MD5(user ":" realm ":" pass)`）**同一先例**：复用同一张 `users` 表、同一个「服务端派生」姿态）。
- **域分隔**：标签 `"unifiedmq-coaps-psk"` + 三道 `0x00` 保证 `(a,b,c)` 三元组无歧义，且与 TURN 的派生结果**不可能相撞**（不同 hash、不同标签）。
- **Realm**：`broker_security` 新 env `coaps` = `#{realm => binary()}`，缺省 `<<"unifiedmq">>`。**不塞进 `broker_security_tls`**——那是证书导向的 TCP TLS 配置，类型是封闭 map，塞 DTLS 的 realm 会污染其语义并向 dialyzer 撒谎；`coaps` 单独一块，语义自洽（「CoAP 安全前端」而非「TLS 证书」）。
- **不改既有语义**：`authenticate/2`、`authenticate_conn/1`、`authorize/2`、`long_term_key/2` 一字不动；`psk_for_identity/1` 是**新增**只读导出；密码与 PSK 均不落日志（`redacted_config/0` 不暴露）。
- **失败原因不泄露**：未知用户与「用户存在但无 password」返回同一个 `{error, bad_username_or_password}`（与 `authenticate/2` 的口径一致）。

### 4.3 `credential_capable/0` 与启动 guard

- `broker_security:credential_capable/0` **追加 `<<"coaps">>`**（DTLS 握手能携带凭据）。**不加 `coap`**：明文 UDP 仍无凭据字段。
- `broker_proto_coap_app:auth_unsupported/0` 改为**分协议判定**：
  - `require_auth` 含 `<<"coap">>` → **拒绝启动**（明文前端无法索要凭据；提示改用 `coaps`）；
  - `require_auth` 含 `<<"coaps">>` → **允许**，但**要求所有启用的 listener spec 都是 `transport => dtls`**；只要有一个是明文 UDP 就拒绝启动（否则「要求认证」的部署被悄悄降级成匿名前端——这正是 §D5 要防的静默降级）。
  - 其他协议在列 → 不干扰（既有「外科式」负控保持）。
- **DTLS listener 的额外启动硬约束**：`users` 表为空时拒绝启动 `{dtls_no_credentials}`。空表意味着任何对端都不可能握手成功，那是配置错误，不是「安全默认」。

## 5. 传输抽象：一个 send/recv 分派点，五张表不动

`#st{}` 新增字段：

| 字段 | 用途 | 模式 |
|---|---|---|
| `transport :: udp \| dtls` | 分派开关（缺省 `udp`，DTLS 必须显式配置） | 两者 |
| `socket :: gen_udp:socket() \| undefined` | udp 分支的 socket；dtls 下为 `undefined`（不谎报类型） | 两者 |
| `dtls_listen :: ssl:sslsocket() \| undefined` | `ssl:listen` 返回的监听 socket | dtls |
| `peers = #{}` :: `SslSock => {IP,Port}` | 入向 demux：`{ssl, Sock, Bin}` → 端点 | dtls |
| `socks = #{}` :: `{IP,Port} => SslSock` | 出向 demux：一次 ACK/通知 → 该端点的 socket | dtls |
| `idents = #{}` :: `{IP,Port} => #{principal, tenant}` | `Ctx` 的 principal/tenant 来源 | dtls（udp 恒 `undefined`/default） |
| `dtls_acceptor :: pid() \| undefined` | 串行握手进程 | dtls |
| `principal_sessions = #{}` :: `Principal => Count` | 每身份会话计数（上限用） | dtls |

**入向**：新增 `handle_info({ssl, Sock, Bin}, St)`（以及 `{ssl_closed, Sock}` / `{ssl_error, Sock, Reason}` / `{ssl_passive, Sock}`），把既有 `{udp, ...}` 子句的 ACK/RST 关联 + `on_datagram/4` **原样复用**（把那段逻辑抽成 `on_packet(IP, Port, Bin, St)`，两种传输共用一个入口）。**这是本阶段唯一的结构性重构，且只是把代码搬家，不改编解码/资源/观察语义。**

**出向**：把 `gen_udp:send(St#st.socket, IP, Port, Bytes)` 的三处调用点（`send/4`、`notify_one/5`、`retransmit_one/3`）统一收进 `send_to(IP, Port, Bytes, St)`：
- `udp` → `gen_udp:send`；
- `dtls` → 查 `socks`，`ssl:send(SslSock, Bytes)`；端点已消失（`ssl_closed` 竞态）→ 丢弃并返回 `ok`（UDP 语义：发送不等于送达，不允许把「对端已走」变成崩溃）。

**五张有界表（dedup / endpoints / block1 / observes / pending）键与语义完全不变**——这是本阶段要保住的不变量（S11-A/B 的 96 例必须零改动通过）。

## 6. 限流与有界（加固点）

| 层 | 机制 | 新/旧 |
|---|---|---|
| 握手 | acceptor 串行 + `dtls_transport_accept_timeout_ms`（1000）+ `dtls_handshake_timeout_ms`（2000） | **新** |
| 握手 | 未知身份 → alert 拒绝（不建 socket、不进表） | **新**（复用 identity 表） |
| 会话 | `max_dtls_sessions`（1024）：`peers`/`socks`/`idents` 表上限；满则 acceptor 关掉新 socket（不挤掉已有会话） | **新** |
| 会话 | `dtls_max_sessions_per_principal`（16）：同一 principal 的并发会话数上限，超则拒 | **新** |
| 连接关闭 | `{ssl_closed, Sock}` → 清 `peers`/`socks`/`idents`、`principal_sessions` 计数、该端点的 `dedup`/`block1`/`endpoints` 条目、注销该端点的全部 Observe 关系并取消 `pending` timer（DTLS 关闭 = 端点消失，比 UDP 的「静默消失」更确定，所以必须真清理——否则表会被死端点占满） | **新** |
| 数据面 | 既有每端点令牌桶 `max_requests_per_second` + `max_endpoints` + `max_message_bytes` | 旧（不变） |
| 观察 | 既有 `max_observes` + `Max-Age` 清扫 | 旧（不变） |

**为什么 DTLS 的令牌桶仍有意义**：握手证明了对端身份，但不证明它不会洪泛——一个已认证设备照样能刷包。DTLS 只是把「源地址可伪造」变成「源地址可信 + 哈希代价」；限流层不变。

## 7. 组件改动清单

| 文件 | 改动 |
|---|---|
| `apps/broker_security/src/broker_security.erl` | 新增导出 `psk_for_identity/1`（§4.2）；`credential_capable/0` 追加 `<<"coaps">>` |
| `apps/broker_proto_coap/src/coap_listener.erl` | `transport` 决议 + DTLS 分支（`init` 建 `ssl:listen` 并 `spawn_link` acceptor）；`{ssl,...}` 系列 `handle_info`；`send_to/4` 统一出向；`Ctx` 带 principal/tenant；`dtls` 表与上限；close 清理 |
| `apps/broker_proto_coap/src/coap_dtls_acceptor.erl` | **新模块**：串行 `transport_accept → handshake → drain identity → controlling_process → 移交`；未知身份 alert 拒绝；上限检查（会话数/每身份） |
| `apps/broker_proto_coap/src/coap_resource.erl` | 无逻辑改动（`authorized/3` 已读 `Ctx.principal`）；仅在 `Ctx` 说明处补 DTLS 来源 |
| `apps/broker_proto_coap/src/broker_proto_coap_app.erl` | `auth_unsupported/0` 改分协议判定（§4.3） |
| `apps/broker_proto_coap/src/broker_proto_coap.app.src` | `applications` 追加 `ssl`；env 追加 `coaps` 相关键（见下） |
| `apps/broker_proto_coap/test/coap_dtls_SUITE.erl` | **新套件**：真实 DTLS PSK 客户端（§8） |
| `apps/broker_proto_coap/test/coap_app_SUITE.erl` | `require_auth` 两用例改口径（`coap` 拒 / `coaps` 允许）+ 2 例（dtls 无凭据拒启、coaps 要求但存在明文 listener 拒启） |
| `apps/broker_proto_coap/test/coap_listener_SUITE.erl` | 不加/不改用例（明文回归证明零改动通过） |
| `apps/broker_security/test/broker_security_SUITE.erl`（或 identity 套件） | `psk_for_identity/1` 的纯函数用例（确定性 / realm 敏感 / 未知即 err / 不泄露） |

**env 键**（`broker_proto_coap.app.src`，全部有缺省、出厂 `enabled => false` 时不读）：`transport`（每 listener spec 内，缺省 `udp`）、`dtls_transport_accept_timeout_ms`(1000)、`dtls_handshake_timeout_ms`(2000)、`max_dtls_sessions`(1024)、`dtls_max_sessions_per_principal`(16)。`broker_security` 新增 env `coaps` = `#{realm => <<"unifiedmq">>}`。

## 8. 测试计划

**新套件 `coap_dtls_SUITE`（真实 DTLS，`port => 0`，客户端是 spike 的 PSK 路径）**：

1. `dtls_psk_handshake_and_get`：合法身份握手 → `GET` retained → 2.05（证「无证书 PSK 可用」在产品路径上也成立）。
2. `dtls_psk_write_and_read`：`PUT` → 2.04，再 `GET` → 2.05 同 payload（写读闭环）。
3. `unknown_identity_is_refused`：未知身份 → 客户端 `{tls_alert, _}`，listener 表零增长。
4. `acl_denies_on_dtls`：ACL 对 `device-01` 拒某 destination → **4.03**（S11-B 欠的这条，本阶段真正可达）。
5. `acl_allows_on_dtls`：allow 规则 → 2.05（负控的正向对位，防「一律拒绝」当通过）。
6. `tenant_isolation_on_dtls`：principal 属 t1，写 t2 destination → 拒（`authorize_principal` 的租户检查在 DTLS 路径上真跑）。
7. `dtls_con_dedup`：DTLS 上重发同一 CON（同 `{IP,Port,Id}`）→ 回原 ACK、不二次执行（证五张表的键在 DTLS 下仍正确）。
8. `dtls_observe_registers_and_notifies`：DTLS 上 `Observe=0` → 发布 → 收到通知（B 期能力跨传输成立）。
9. `dtls_session_cap`：`max_dtls_sessions = 1`，第二个客户端握手被拒、第一个不受影响。
10. `dtls_close_cleans_up`：客户端 `ssl:close` → listener 的 `peers`/`socks`/`idents`/`observes` 归零（用心跳式断言：再 `sys:get_state` 或经 `port/1` 后重连证明无泄漏）。
11. `dtls_per_principal_cap`：同身份第二条会话超限被拒（不同身份不受影响）。
12. `plain_udp_unaffected`：`transport => udp` 的 listener 在 `users` 表非空时行为不变（`principal = undefined`，ACL 放行）——明文的如实降级不被 DTLS 改动波及。

**`coap_app_SUITE` 改/增**：`require_auth_for_coap_is_refused`（保持拒绝，理由文案更新）、`require_auth_for_coaps_is_allowed`（改成允许）、`coaps_requires_dtls_listeners`（coaps 在列但有明文 listener → 拒）、`dtls_without_users_is_refused`。

**基线目标**：CoAP 五套件 96 例零改动全过 + 新 DTLS 套件（12 例）+ app 套件净增 2 例；全量 CT 从 1347 起、零回归；dialyzer 0；xref 只多 `psk_for_identity/1` 的导出（若被调用则不算 unused）。

## 9. 容量报告（纳入本阶段）

落盘 `docs/benchmarks/2026-11-1X-s11c-dtls-capacity.md`，harness 与原始数据在 `test/protocol-compatibility/s11c/bench/`（沿用 S10-D `run_bench.sh` + JSON 结果的形状）。测量口径：

- 握手速率（串行 acceptor 的实际上限，§3.2 第 4 条的实测值）；
- 单客户端 GET 往返延迟（p50 / p95 / max）与吞吐；
- N 并发会话（1 / 8 / 32）下的聚合吞吐与错误数；
- 与**明文 UDP** 的同机对照（隔离 DTLS 的 CPU 代价，不写成「DTLS 更慢」的泛泛结论）；
- 前置约束与机器负载如实记录（不计入 Gate 判定，除非出现数量级异常）。

## 10. 文档同步与 ADR 定稿

- `docs/protocol-compatibility.md` §4.24：能力矩阵 CoAP 行追加 DTLS（`coaps`）；新增 §4.24.8（DTLS：PSK-only、cert-free、身份=principal、握手串行与上限、关闭清理）；§4.24.4 删去「无 DTLS」并改为「无证书 / 无 mTLS / 无 OSCORE / 无 libcoap 证据」；证据计数改新数；「最近一次追加」改 2026-11-1X S11-C。
- `docs/security.md`：§12.5 末尾收口 + 新增 §12.6「S11-C DTLS（coaps）面」——PSK 派生式、realm、principal/tenant 来源、串行握手与上限、关闭清理、仍无的东西（证书/mTLS/OSCORE/重放窗口外的会话恢复）。
- `docs/roadmap.md` §13.1：S11-C 行改「🟡 已交付」+ 证据；§13.2 Gate S11 勾选项更新；状态口径改 S11-A/B/C 全交付。
- `docs/adr/031-coap-frontend-boundary.md`：**只在「落地偏差记录」追加 S11-C 段**（冻结文本不改）——记录：DTLS 只做 PSK（证书非目标）、realm 放在 `broker_security.coaps` 而非 `tls`、串行握手（身份竞态的决定性约束）、`coaps` 进入 `credential_capable/0`。
- `progress.md` / `README.md`：应用数、CT 计数、协议清单（CoAP 由「UDP」改「UDP + DTLS 可选」）。

## 11. 风险与缓解

| 风险 | 缓解 |
|---|---|
| `{active, N}` 在 DTLS 上的行为 | **已 spike 证**：`{ssl_passive, Sock}` 正常发出，UDP 重挂模式可直接复用（spike12）。风险已消除 |
| `setopts` 在非 owner 上静默错投 | **已 spike 证**并写进设计 §3.2 第 5 条：握手期 `{active,false}`，只有移交后的 listener 设 active |
| 串行握手成为 DoS 面 | 双重限时（1000 + 2000 ms）+ 会话上限；容量报告给实测握手速率，能力矩阵如实声明 |
| `ssl` 未启动导致 `ssl:listen` `noproc` | `app.src` `applications` 追加 `ssl`（OTP 内置，非新增运行时依赖）；DTLS 分支首行再 `ensure_all_started(ssl)` 兜底 |
| 关闭清理漏项导致表泄漏 | §6 的 close 清理逐表列出；`dtls_close_cleans_up` 用例断言四张表归零 |
| 身份竞态回归 | acceptor 串行 + 双路径 drain 写进模块 doc 与用例（并发好/坏客户端各一，断言身份不串） |
| 客户端 PSK 身份类型（list vs binary） | 产品代码只处理**服务端**方向（identity 是 binary）；suite 的客户端 helper 用 `binary_to_list/1`，spike8 踩过这个坑 |
