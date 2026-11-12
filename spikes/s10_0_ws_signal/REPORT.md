# S10-0 spike — WSS 信令前端可行性（ranch 2.3.0 + `broker_ws`，无 cowboy）

- **对应**：[ADR-028](../../docs/adr/028-webrtc-signaling-kernel-boundary.md)（信令面边界、进程直投、不落盘）、[ADR-029](../../docs/adr/029-ice-component-and-port-matrix.md)（端口矩阵）；[`docs/design/s10-execution-plan.md`](../../docs/design/s10-execution-plan.md) Task 0.3、§4.3 依赖账本
- **日期**：2026-11-13
- **环境**：OTP 28（ERTS 16.2）、`ranch 2.3.0`（hex）、`broker_ws 0.1.0`（**仓内源码**，经 `{path, "../../apps/broker_ws"}` 引用）、Python 3.12 + `websockets 17.4`、OpenSSL 3.2.4
- **性质**：**一次性可行性验证**。独立 rebar3 项目，不进 umbrella（同 `spikes/s0_*`）。**不新增任何依赖到主仓**、不占用任何固定端口。

## 0. 结论速览

| 待验证问题 | 结论 | 关键数字/证据 |
|---|---|---|
| 只有 `ranch 2.3.0` + `broker_ws`，能否跑起一个可用的 WSS 信令前端？ | **能**，且 **`ws` 与 `wss` 是同一份代码路径** | 两种传输的 101 响应都是 **164 字节**、同一子协议 `webrtc.v1`；`wss` 协商到 **TLSv1.3** |
| 子协议是否强制？ | **强制**：无共同子协议 → `400 Bad Request`，不静默降级 | 见 §1 与 `spike_ws_tests:no_common_subprotocol_is_400_test/0` |
| 文本 `ping`/`pong` 与 RFC 原生 opcode 9/10 能否并存？ | **能，且必须分两套处理** | 文本 `ping` → 文本 `pong`；opcode 9 `ping` → opcode 10 `pong`，两者在同一条连接上先后都成立 |
| binary 帧怎么处理？ | 服务端必须**明确拒绝**（close 1003），不是忽略 | 客户端实测收到 `close_code = 1003` |
| 「进程直投」够不够？（ADR-028 D3） | **够**：一条 `to` 消息从一个连接进程直达另一个连接进程，无 router/queue/消息记录 | `{delivered, PXF1…, [PgwK…]}`；对端收到 `from`+`to`+原 `data` 逐字节不变 |
| `peer_id` 能不能由服务端铸造？（R-S7） | **能**，客户端无法自选 | `base64(strong_rand_bytes(9))`，两个连接各自拿到不同 id |
| 需要 `cowboy` 吗？ | **不需要** | 全部行为由 `ranch` + `ws_handshake` + `ws_frame` 提供，见 §4 |

**对 S10-A 的直接影响**：`broker_proto_webrtc` 不需要第二套 TLS 实现，也不需要引入 cowboy；连接模块只写一份，`ranch` 的 transport 抽象已经吸收了这个差异。

## 1. 握手（`ws_handshake:accept/2`）

服务端发回的完整 101 响应（`port => 0` 实测，逐字节）：

```
HTTP/1.1 101 Switching Protocols\r\n
Upgrade: websocket\r\n
Connection: Upgrade\r\n
Sec-WebSocket-Accept: <base64(SHA1(key ++ GUID))>\r\n
Sec-WebSocket-Protocol: webrtc.v1\r\n
\r\n
```

- **字节数：164**（`"Sec-WebSocket-Accept: "` 之后是 28 字节摘要）。`ws` 与 `wss` 都是 164——TLS 不改变应用层字节。
- 摘要与 RFC 6455 §1.3 的样例 key 一致（`s3pPLMBiTxaQ9kYGzzhZRbK+xOo=`），已固化为 eunit 断言。
- 返回形态是 **`{ok, IoData, Subprotocol}`**（三元素），不是 `{ok, IoData}`。S10-A 的连接模块必须按三元素解构；`accept/1` 是兼容旧的二元素包装。
- **无共同子协议 → `{error, <<"HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n">>}`**。这满足设计稿 R-T6（不足就拒，不静默降级），与 `broker_proto_mqtt_ws` 的姿态一致。
- 未提供 `supported_subprotocols`（或空列表）时，响应**不带** `Sec-WebSocket-Protocol` 头，`Subprotocol = undefined`——服务端不主动提供一个客户端没要的子协议。

## 2. 帧策略（`ws_frame:decode/2` + `encode/2,3`）

`decode/2` 的第二参数是 **`non_neg_integer()` 上限**，不是 options map。传 map 会 `function_clause`。这是 S10-A 最容易踩的一个坑，已写进断言。

| 输入 | 结果 |
|---|---|
| `<<16#81,3,"abc">>`（FIN+text） | `{ok, #{fin := true, opcode := 1, payload := <<"abc">>}, <<>>}` |
| `<<16#82,2,"hi">>`（FIN+binary） | `{ok, #{opcode := 2, ...}, <<>>}` —— **codec 不拒绝 binary，是连接层决定** |
| `<<16#89,0>>`（FIN+ping） | `{ok, #{opcode := 9, ...}, <<>>}` |
| `<<16#81>>`（截断） | `{more, 1}` |
| `<<16#81,126,1000:16>>`，上限 16 | `{error, {payload_too_large, 1000, 16}}` |
| `<<16#C1,0>>`（RSV 位置位） | `{error, {reserved_bits, 1}}` |

- `encode/1,2` 的 opcode 是**整数** `0..15`（`-type opcode() :: 0..15`），**不是**原子 `text`/`close`。传原子会 `badarg` 崩在 binary 构造里。关闭帧用 `encode(8, ws_frame:close_payload(Code, Reason))`。
- 因此 binary 的处置（R-T3）归连接层：`decode` 返回 `opcode := 2` → 连接层回 `close 1003` 并终止。

## 3. 保活：两套 ping 必须共存

- **文本 `ping`**（上游 `webrtc-server` 的约定：一条内容恰为 `"ping"` 的文本帧）→ 回一条文本 `pong`。实测：客户端 `send("ping")` 收到 `opcode 1 payload "pong"`。
- **原生 opcode 9**（RFC 6455 §5.5.2）→ 回 opcode 10，payload 原样回显。实测：`websockets.ping(b"spike")` 的 `pong_waiter` 在 5 s 内完成。
- 两者在同一条连接上**先后都成功**（event trace 里 `{text_ping_answered, P}` 紧跟 `{native_ping_answered, P}`）。
- S10-A 必须两条都实现：只做文本 ping 会让标准客户端（浏览器）的发现在失效检测变差；只做 opcode 9/10 会破坏上游客户端的约定。

## 4. 依赖与「不用 cowboy」的证据

把「不引入 cowboy」从判断变成事实，逐条核对：

| 断言 | 证据 |
|---|---|
| 握手不需要 HTTP 服务器 | `ws_handshake:accept/2` 输入是**原始请求头字节**，输出是**原始响应字节**；中间不需要任何 HTTP 栈 |
| 分帧不需要 cowboy | `ws_frame:decode/2` / `encode/2,3` 是纯函数，已由 10 条 eunit 断言覆盖 |
| 连接接受不需要 cowboy | `ranch:start_listener(Ref, ranch_tcp \| ranch_ssl, TransOpts, Protocol, Args)` + `ranch:handshake(Ref)` 即可；本 spike 两种 transport 都跑通 |
| OTP 28 没有内置替代 | 探测 `websocket` / `ws` / `gun` / `hackney` 全部 `nofile`；只有 `inets` 的 `httpd` 存在（那是 HTTP 服务器，不含 RFC 6455） |

`cowboy` 的问题不在「能不能用」，而在**版本约束冲突**：hex 上 `cowboy 2.0.0` 的元数据要求 `ranch ~> 1.4.0`，与本仓 `ranch 2.3.0` 不兼容（执行计划 §4.3 已记）。本 spike 证明不需要它，冲突就不必解。

## 5. 进程直投（ADR-028 D3 的最小实现）

两个连接、一个房间，A 发 `{"to": <B 的 peer_id>, "event": "offer", "data": {"sdp": "v=0 fake"}}`：

- **B 收到**：`{"from": "PXF1…", "to": "PgwK…", "event": "offer", "data": {"sdp": "v=0 fake"}}`
  - `event` 与 `data` 逐字节不变（服务端不解析 SDP）；
  - **保留 `to`**、注入 `from`（R-S2）；
  - key 顺序变了（`json:encode` 的 map 顺序），所以断言只能比语义，不能比字节序。
- **投递路径**：`ets:tab2list(spike_room)` 拿到 `{PeerId, Pid, Socket, Transport}` → `Transport:send(Socket, ws_frame:encode(1, Frame))`。
  - 全程没有 `#broker_message{}`、没有 `broker_queue:append/3`、没有 `broker_router`、没有订阅索引、没有 `.cursors`。ADR-028 D1 的契约在真实连接上成立。
- **成员清理**：连接进程结束（正常 close 或 binary 触发 close 1003）后 `left` 事件到达采集进程，ETS 表项消失。真实实现的这一步换成 `webrtc_room_registry` 的 monitor（ADR-028 D4）——机制相同，只是把「谁在房间」从 ETS 提到分片 gen_server 后面。

`peer_id` 由服务端 `base64:encode(crypto:strong_rand_bytes(9))` 铸造，两个连接拿到不同值。客户端**无法**指定自己的 id——它必须先从 `authenticated` 事件里读到对方的 id 才能发 `to`。这直接满足 R-S7。

## 6. `wss` 路径（ranch ssl transport）

`./run_wss.sh` 用自签证书（`openssl req -x509 … -days 1`，生成在本 spike 目录内，不装系统）在 `port => 0` 起 `ranch_ssl`：

- 客户端：`wss_status = 101`、`wss_subprotocol = webrtc.v1`、**`wss_tls_version = TLSv1.3`**、认证与文本 `pong` 全通。
- **`spike_ws` 模块一行没改**就同时服务了 `ranch_tcp` 和 `ranch_ssl`。`Transport` 是参数，`Transport:recv/3`、`Transport:send/2`、`Transport:setopts/2` 在两种 transport 上签名相同。
- 这是 S10-A「不写第二套 TLS 实现」的实证依据。

## 7. 对 S10-A 的落地约束（从 spike 反推，不是设想）

1. 连接模块按 **`{ok, IoData, Subprotocol}`** 解构 `ws_handshake:accept/2`；子协议缺失/未知一律 400 + close，不升级。
2. `ws_frame:decode(Buffer, MaxBytes)` 的第二参数是**整数上限**；`encode(Opcode :: 0..15, Payload)` 的 opcode 是**整数**。
3. binary 帧：`opcode := 2` → `close 1003`（不是 warning 忽略）。
4. 文本 `ping` 与 opcode 9 **都要实现**，且必须能区分（前者是业务保活，后者是协议保活）。
5. `peer_id` 服务端铸造，客户端不可指定；`to` 只能指向**同房间**的已注册 peer。
6. 投递走 `Pid`/`Transport:send`，**不构造 `#broker_message{}`**；边界由 `webrtc_boundary_SUITE` 的源码级 grep 守（ADR-028 D7）。
7. `ranch_tcp` 与 `ranch_ssl` 共用一份连接模块，TLS 由 transport 吸收。

## 8. 诚实边界（这份 spike 没证明什么）

- **不是产品实现**：无认证窗口超时、无 ACL、无 `joined`/`left` 广播集合语义、无人数上限、无空闲超时、无 traceparent。这些都是 S10-A/B 的活。
- **不是浏览器证据**：客户端是 Python `websockets`，不是 `RTCPeerConnection`。SDP/candidate 的真实时序、`getUserMedia` 前置、浏览器 TLS 证书链都在 Gate S10 的第三方客户端证据里（`test/protocol-compatibility/s10/`，S10-D）。
- **不声称性能**：本文没有任何吞吐/延迟数字。容量结论要等 S10-D 的压测报告。
- **不声称 NAT 穿越**：ICE 面（3478/UDP、relay 段）本 spike 完全没碰，那是 S10-C。
- 回环地址 `127.0.0.1`，自签证书，`CERT_NONE`。公网部署的证书/防火墙/端口暴露都不在结论范围内。

## 9. 复现方式

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
cd spikes/s10_0_ws_signal

python3 -m pip install --target /tmp/opencode/s10-libs websockets   # 客户端

rebar3 eunit        # 10 条 wire 断言（无需监听端口）
rebar3 compile

./run_spike.sh      # ws：握手 -> 认证 -> 文本 ping/pong -> 原生 ping -> to 直投 -> binary 1003
./run_wss.sh        # wss：同上（自签证书，TLSv1.3）
```

两个脚本都用 `port => 0`，结束时 kill 自己的 BEAM，不触碰 8080/8083/1883/8081/8082。

## 10. 留痕文件

| 文件 | 内容 |
|---|---|
| `src/spike_ws.erl` | 连接进程：握手 → 认证 → 帧循环 → 进程直投 → close；`ranch_protocol` 实现 |
| `src/spike_run.erl` / `src/spike_run_tls.erl` | 在 `port => 0` 起 `ranch_tcp` / `ranch_ssl` listener，采集事件 |
| `spike_client.py` / `spike_wss_client.py` | Python `websockets` 客户端（第三方实现，不是本仓代码） |
| `test/spike_ws_tests.erl` | 10 条 eunit 断言（握手摘要、子协议拒绝、帧解码、超限、RSV、binary/opcode9） |
| `run_spike.sh` / `run_wss.sh` | 一键复跑 |
| `.out` 文件（git 忽略） | 每次运行的 `port.txt` / `events.out` / `client.out` / `node.out` |

> `spike.out`、`*.out`、`_build/`、`certs/`、`__pycache__/` 均由 `.gitignore` 排除（见仓库 `.gitignore` 的 spikes 段）。
