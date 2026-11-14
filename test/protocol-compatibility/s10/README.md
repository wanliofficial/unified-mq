# S10-D 真实第三方客户端证据 harness（WebRTC 信令 + STUN/TURN + 不落盘）

这一目录是 Gate S10 两项门禁的落地：

- **D.1「一份真实第三方客户端证据」**（三段脚本，见下 §1–§3）；
- **D.2「不落盘运行时证明」**（§4，`run_no_persist.sh`）。

它**不是** `rebar3 ct` 的一部分：CT 套件覆盖协议状态机与门控顺序（见
`apps/broker_ice` 与 `apps/broker_proto_webrtc` 的各套件），这里覆盖的是
「**用真实的、独立实现的 WebRTC 客户端能不能真的跑通**」与「**跑通的同时内核
一个字节都没写**」这两层。

客户端是 Python [`aiortc`](https://aiortc.org/) 1.13.0（含 `aioice` 0.13.0），一个
完整、独立的 WebRTC 实现。它连**本仓库生产代码**起起的证据节点（`s10_node`）走完整
`offer → answer → candidate` 时序、真实 STUN Binding、真实 TURN Allocate。

> 以 aiortc 为第三方实现，**非浏览器**。诚实边界见文末。

## 三段证据（原始输出见 `*.jsonl`，是最后一次干净跑的原文）

### 1. 裸 STUN Binding（`stun_evidence.py`，无第三方库）

自己拼一个最小 RFC 8489 STUN Binding 请求（type `0x0001`、magic cookie、12 字节
transaction id、无属性），用裸 UDP 发给我们，把请求/响应**逐字节打印为十六进制**并
手工解码。断言：响应是 Binding Success（`0x0101`）、transaction id 原样回来、
XOR-MAPPED-ADDRESS 解码后等于服务器看到的源地址。这一步不依赖任何 STUN 库，是
aiortc `srflx` 结果成立的地面真相。

```bash
PYTHONPATH=/tmp/opencode/s10-libs /usr/bin/python3 stun_evidence.py \
    --ice-host 192.168.10.26 --stun-port <stun_port> --out stun_evidence.jsonl
```

### 2. aiortc 信令 + STUN srflx（`webrtc_evidence.py`）

两个 `aiortc.RTCPeerConnection` 各自连证据节点的 `wss://127.0.0.1:<port>/websocket/e2e`，
`authenticate` 后通过 Broker 的房间中继完成真实 offer/answer（aiortc 走全 ICE，候选
内联进 SDP，无 trickle，所以交换 SDP 即驱动 ICE）。断言：

- 两端都到 `connectionState == "connected"`（ICE + DTLS + SCTP 全程经我们的中继与
  ICE 服务器起来）；
- 一端开的 `RTCDataChannel` 在另一端收到——证明协商出的媒体面真实携带了有效载荷，
  而不只是信令握手；
- 收集到的 SDP 含 `typ srflx` 候选，其反射地址由**我们的** STUN 服务器签发。

```bash
PYTHONPATH=/tmp/opencode/s10-libs /usr/bin/python3 webrtc_evidence.py \
    --host 127.0.0.1 --stun-host 192.168.10.26 \
    --wss-port <wss_port> --stun-port <stun_port> --out webrtc_signaling.jsonl
```

### 3. aiortc TURN Allocate（`turn_evidence.py`）

把 aioice 强制成 relay-only（见下「诚实偏差」），用 `alice` 长期凭据连 `turn:` 服务器。
断言：

- **正确凭据**：peer 成功 Allocate，其 SDP 带 `typ relay` 候选，地址是节点通告的
  ICE IP，端口落在配置段 `49500-49519` 内；
- **错误凭据**：拿到 0 个 relay 候选——我们的长期凭据校验对真实客户端同样生效。

```bash
PYTHONPATH=/tmp/opencode/s10-libs /usr/bin/python3 turn_evidence.py \
    --ice-host 192.168.10.26 --turn-port <stun_port> --out turn_evidence.jsonl
```

### 4. 不落盘运行时证明（`run_no_persist.sh`，D.2）

`webrtc_boundary_SUITE` 是**源码级**证据：剥掉注释后扫描
`apps/broker_proto_webrtc/src/*.erl`，断言其中不出现 `broker_queue:` /
`broker_router:` / `#broker_message{` / `.cursors`。那证明的是「没有调用点」，
证明不了「没有写」——写可以从依赖里绕进来。这一段补的是**运行时**证据。

一个 Erlang VM 里同时起来：WAL 存储 + `broker_subscription`（**其 100 ms 的
cursor tick 是活的**）+ 生产 WSS 信令监听 + 生产 STUN/TURN。为什么必须起
subscription：不起它，「没有 `.cursors` 文件」就是「没人会写」的同义反复，断言没有
意义。

时序：

```
seed 一条普通消息 → snapshot(before) → 完整 1:1 aiortc 通话 → snapshot(after)
→ diff(before, after) 必须 unchanged
→ 负控 A：再 append 一条 → diff 必须 changed（offsets + wal_bytes 都动）
→ 负控 B：真订一个订阅并 ack 一次 → 必须出现一个 `.cursors` 文件
```

三不变的对象（`src/s10_no_persist.erl`）：

| Fact | 读法 | 为什么是它 |
|---|---|---|
| `offsets` | `broker_queue:watermarks/1`，逐分区 `{Committed, High}` | append 一动 `high` 就变，ack 一动 `committed` 就变 |
| `wal_bytes` | `data_dir/wal` 下所有普通文件的字节和 | 就算水位被回滚，只要落过盘，字节数就变 |
| `cursors` | `broker_subscription_files:sessions_dir()` 下 `*.cursors` 的 **basename 集合** | 第三块数据面（ADR-028 §D1 明确禁止），必须始终为空 |

`session_files`（`.subs`/`.inflight`）也录进证据，但**不参与判定**：D.2 的契约是上面
三条，守卫一旦偏离自己的契约就会被关掉。

**负控为什么必须有**：一个「永远返回 unchanged」的快照也能让「不落盘」通过。两条
负控把守卫钉成可证伪的——A 证明 offset/字节检测器能看见写，B 证明 cursor 检测器能
看见文件。

**实测（2026-11-14，最后一次干净跑，原文见 `no_persistence.jsonl`）**：

| 项 | 值 |
|---|---|
| 通话 | 两 peer `connected`、DataChannel 往返 `s10-d-evidence-ping` 一致、18 个 srflx 候选 |
| `diff(before, after)` | **`unchanged`**（`moved: []`） |
| baseline | `offsets` = `[{P1, 0, 1}, {P0, 0, 2}]`（seed 后非空）、`wal_bytes` = **117**、`cursors` = `[]` |
| after | 同上三条**逐字节相同** |
| 负控 A | `offsets` 变到 `{P0, 0, 3}`、`wal_bytes` 117 → **228** |
| 负控 B | `.cursors` 从 `[]` 变成 `["exclusive.czEwLWQyLWNvbnRyb2w.czEwL2QyL2NvbnRyb2w.cursors"]` |
| 判定 | `S10-NOPERSIST-VERDICT PASS` |

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
S10_DATA=/tmp/opencode/s10-d2-data ./run_no_persist.sh
```

`S10_DATA` 是节点的 WAL/会话卷（启动时 `rm -rf`），`S10_EVIDENCE` 是 JSONL 落点
（不动），两者默认不同目录，免得证据跟着卷一起被擦。

## 一键复跑

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
./run_all.sh          # D.1 三段 + D.2，并把刷新后的 JSONL 拷回本目录
```

单独跑：

```bash
./run_evidence.sh     # 只跑 D.1
./run_no_persist.sh   # 只跑 D.2
```

`run_evidence.sh` 自己编译证据节点、起节点、等 `S10-NODE-READY`、跑三段、拆节点，
原始 JSONL 落到 `/tmp/opencode/s10-evidence/`（脚本内 `S10_DATA`）。节点全用临时端口
（ranch `port => 0`、`broker_ice` `port => 0`、relay 段收窄到 `49500-49519`），
**不占用**也不改动 8080/8083/1883/8081/8082/8084 或任何 CT relay 段。

## 前置条件

1. **必须用 `/usr/bin/python3`（3.12）**，且 `PYTHONPATH` 指向 aiortc 安装目录：
   ```bash
   printf 'av==14.2.0\n' > /tmp/opencode/s10-constraints.txt
   /usr/bin/python3 -m pip install --target /tmp/opencode/s10-libs \
       -c /tmp/opencode/s10-constraints.txt aiortc==1.13.0
   ```
   **`av` 必须锁 14.2.0**：aiortc 约束 `av>=14,<15`，而 `av` 19 会触发源码编译
   （需要 ffmpeg dev 头文件）而失败。其余依赖（aioice/cffi/cryptography/
   pylibsrtp/pyopenssl/pyee 等）由 pip 自动装好。
2. Broker 代码在仓库里已编译：`rebar3 compile`（`run_evidence.sh` 依赖
   `_build/default/lib/*/ebin`）。
3. aiortc 把 ICE UDP socket 绑到主机真实网卡 IP，再往 STUN/TURN 地址发包。证据
   节点因此把 ICE 通告地址设为**主机非回环 IPv4**（`S10_ICE_IP` 可覆盖），否则
   回环目的地址会被沙箱 `EPERM` 拦掉。WSS 信令仍走 `127.0.0.1` 回环 TCP（沙箱允许）。

## 文件

| 文件 | 作用 |
|---|---|
| `src/s10_node.erl` | 证据节点：`boot/0` 起内核（含**活的** `broker_subscription`）+ WSS 临时端口 + STUN/TURN 临时端口（`enabled`，窄 relay 段），返回 `#{wss_port, stun_port, ice_ip, data_dir}` |
| `src/s10_run.erl` | `erl -s s10_run main` 入口（D.1）：`s10_node:boot/0` 后打印 `S10-NODE-READY` 并阻塞 |
| `src/s10_no_persist.erl` | D.2 快照：`snapshot/0`（offsets / wal_bytes / cursors / session_files）、`diff/2`（只比前三条）、`to_json/1` |
| `src/s10_no_persist_run.erl` | D.2 驱动：boot → seed → 快照 → 跑 aiortc 通话 → 快照 → diff → 两条负控 → 落 JSONL + 判定 |
| `webrtc_signaling.py` | 共享底座：`SignalingClient`（连 WSS、`authenticate`、中继 opaque 信号）、候选/SDP 解析、`force_relay_policy` |
| `stun_evidence.py` | 第 1 段：裸 UDP Binding，hex + 手工解码 |
| `webrtc_evidence.py` | 第 2 段：两 aiortc peer 信令 + DataChannel + STUN srflx（D.2 也复用它当「完整 1:1 通话」） |
| `turn_evidence.py` | 第 3 段：aiortc TURN Allocate + 凭据校验 |
| `run_evidence.sh` | D.1 一键编译/起节点/跑三段/拆节点 |
| `run_no_persist.sh` | D.2 一键编译/起节点/跑证明/拆节点 |
| `run_all.sh` | 跑全部并把 JSONL 拷回本目录 |

## 诚实偏差（写在这里，不藏）

- **relay-only 是「越过公开 API」达成的**。计划（设计 §3）要
  `iceTransportPolicy="relay"`，但 aiortc 1.13.0 的公开 `RTCConfiguration` 只有
  `iceServers` 和 `bundlePolicy`，**没有** `iceTransportPolicy`。为拿到 relay-only，
  `webrtc_signaling.force_relay_policy` 直接设底层 `aioice.Connection` 的
  `transport_policy = RELAY`——这正是 RFC 9429 §3.5.3 relay-only 模式，即浏览器
  `iceTransportPolicy:"relay"` 的等价物；因为公开 API 表达不了，才伸手进私有属性，
  此处明说。
- **TURN 媒体面（relay-to-relay 媒体吞吐）未被 aioice 完成，记为发现而非静默通过**。
  aioice 的 TURN 客户端只用 ChannelBind 发数据、**从不发 CreatePermission**；而本
  仓库 `broker_ice` 严格按 RFC 5766 §9 实现：没有 CreatePermission 的 ChannelBind
  一律 403。这正是 CT 套件 `turn_allocation_SUITE:
  channel_bind_without_a_permission_is_a_403` 钉死的冻结契约。真实浏览器会先发
  CreatePermission，故能与严格服务器互通；aioice 省掉这步，故无法在严格服务器上跑通
  relay-to-relay 媒体。这是 **aioice 客户端的局限，不是 `broker_ice` 的缺陷**：
  permission → channel → ChannelData → peer 的完整媒体路径已由 CT 侧的合规客户端
  `ice_test_client` 证明。我们**刻意没有**为让 aioice 通过而放宽服务器的 403——那会
  破坏冻结的 CT 契约、削弱这条 403 本要保护的安全属性。本段证明的是 **Allocate /
  凭据 / relay 地址** 这一属于服务器职责的第三方客户端互操作面。
- **非浏览器、单机、回环 + 一个可路由内网 IP、自签证书、一个房间两个 peer**。不声称
  公网 NAT 穿透、真实运营商网络、TLS 证书链校验、浏览器兼容矩阵，也不声称 TURN over
  TCP / DTLS-SRTP 媒体解密 / IPv6 relay。
- **D.2 的「不落盘」只覆盖信令面，且只在那一次运行里成立**。它证明的是：在这个节点、
  这次通话（含 ICE/SCTP/DTLS 全流程）之后，WAL 水位、WAL 字节数、`.cursors` 集合三者
  逐字节没动，并且两条负控证明检测器确实看得见写。它不证明「代码里永远没有第二处写」
  （那是 `webrtc_boundary_SUITE` 的静态扫描负责的范围），也不覆盖其他 app 的写路径。

## 已知环境约束

- 若 aiortc 只绑回环或沙箱禁止「非回环源 → 回环目的」的 UDP，STUN/TURN 会拿不到
  候选；证据节点用主机真实 IPv4 规避（见前置条件 3）。
- 证据机网络与负载会让绝对吞吐不可复现，故本目录只给**正确性**结论（连通、协商、
  分配、拒绝），不给容量数字；容量由 D.6 单独压测报告给出。
