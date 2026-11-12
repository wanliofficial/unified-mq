# 运维手册（S6）

- **状态**：S6 交付事实文档 v1.0（2026-11-09）；S8-B/S8-C/S8-D 追加 §1 的 8083、UDP 1884 与 UDP 14567
- **对应**：`docs/roadmap.md` §8.2；Gate S6 第 4 项（运维手册与告警规则）
- **实现**：`apps/broker_admin/`（`broker_admin_http`、`broker_admin_listener`、`broker_backup`、`broker_config_audit`、`broker_version`）
- **告警规则**：[`config/alerts.yml`](../config/alerts.yml)
- **相关**：`docs/storage-and-recovery.md`（WAL/会话恢复）、`docs/security.md`（TLS/ACL）

本手册只描述已实现的管理面。命令示例以默认配置（admin `127.0.0.1:8090`，空 token）为准；生产环境必须设置 token 并限制 bind。

---

## 1. 组件与端口

| 组件 | 默认端口 | 协议 | 说明 |
|---|---|---|---|
| MQTT | 1883 | 明文（可 TLS） | `broker_proto_mqtt` |
| WebSocket | 8080 | 明文（可 TLS） | `broker_proto_ws`（Broker 自有 JSON/binary 消息 API） |
| MQTT over WebSocket | 8083 | 明文（可 TLS，wss） | `broker_proto_mqtt_ws`（S8-B；MQTT 3.1.1 §6 / 5.0 §6） |
| MQTT-SN | 1884/**UDP** | 明文（无 TLS） | `broker_proto_mqtt_sn`（S8-C；MQTT-SN 1.2，无 DTLS） |
| MQTT over QUIC | 14567/**UDP** | **强制 TLS 1.3**（无明文模式） | `broker_proto_mqtt_quic`（S8-D；CN Draft 3 单流模式，`rebar3 as quic` 才构建） |
| NATS | 4223 | 明文（可 TLS） | `broker_proto_nats` |
| AMQP | 5673 | 明文（可 TLS） | `broker_proto_amqp` |
| Kafka | 9093 | 明文（可 TLS） | `broker_proto_kafka` |
| Pulsar | 6650 | 明文（可 TLS） | `broker_proto_pulsar` |
| A2A (JSON-RPC) | **无（默认不监听）** | HTTP/1.1 明文（可 TLS） | `broker_proto_a2a`（S9-A/B/C）。示例部署端口 8081，`listeners => []` 是出厂值；出站 Push webhook 单独由 `push_enabled` 门控（默认关） |
| MCP (Streamable HTTP) | **无（默认不监听）** | HTTP/1.1 明文（可 TLS） | `broker_proto_mcp`（S9-D）。示例部署端口 8082，`listeners => []` 是出厂值；stdio 前端另由 `stdio_enabled` 门控（默认关） |
| 管理 API | 8090 | HTTP | `broker_admin`，默认只绑 `127.0.0.1` |

TLS 由 `broker_security.tls.enabled` 一个开关统一控制，所有协议 listener（目前九个 app、七个受 TLS 控制）同时切换。见 `docs/security.md` §9。两个例外是 MQTT-SN（UDP 无 TLS，见下）与 MQTT over QUIC（只读证书，开关不是开关）。

**例外是 MQTT-SN**：它的 payload 走 **UDP**，TLS 在这里不适用（需要 DTLS）。`broker_security.tls.enabled` **不影响 1884**，1884 永远是明文 UDP；这是当前唯一**明文且没有 TLS 路径**的协议端口，DTLS 列为后续待办（`docs/security.md` §12 第 8 条）。

**QUIC 是另一个方向的例外**：它**没有明文模式**，握手本身就是 TLS 1.3，所以 `broker_security.tls.enabled` 对它不是「开关」而是「证书来源」——关掉这个开关不会让 14567 变明文，只会让 listener 找不到证书而**拒绝启动**（`{quic_requires_tls, Key}`，只影响这一个 app）。

`broker_ws`（S8-A 起）是**纯库 app**：无进程、无监听端口、无运行时配置，只提供 `ws_frame`（RFC 6455 帧编解码）与 `ws_handshake`（握手），供 `broker_proto_ws` 与 `broker_proto_mqtt_ws` 共用。运维上无需任何配置项，也不出现在上表。

`broker_proto_agent_rpc`（S9-A 起）同样是**纯库 app**：无进程、无端口、无自有 env，只提供 JSON-RPC 2.0 信封、HTTP/1.1 帧、SSE 帧、`A2A-Version` 协商与凭据 helper，供将来的 A2A/MCP/ACP 三个前端共用。运维上无需配置，也不出现在上表。

**两个 WebSocket 端口不是一件事**。`8080` 是 Broker 自己的 JSON/binary 消息 API（S5），`8083` 是标准 MQTT over WebSocket（S8-B）：同一个 `mqtt_conn_fsm` 跑在 RFC 6455 binary 帧里，客户端是普通 MQTT 库（如 paho-mqtt 的 `transport="websockets"`）。端口内容互不兼容，**不要**把 MQTT 客户端指向 8080。

`broker_proto_mqtt_ws` 的配置（`config/sys.config.src`）：

```erlang
{broker_proto_mqtt_ws, [
  {listeners, [#{port => 8083,
                 ref => {mqtt_ws, 8083},
                 num_acceptors => 10,
                 max_connections => 1024}]},
  {subprotocols, [<<"mqtt">>, <<"mqttv3.1">>, <<"mqttv3.1.1">>]},
  {max_frame_bytes, 16777216}
]}.
```

- **任一 URL path 都接受**（`/`、`/mqtt`、`/ws/...`）：MQTT over WebSocket 规定的是子协议，不是路径。客户端库里的 path 只需与服务端部署约定一致即可。
- **子协议是硬门槛**：握手没带 `Sec-WebSocket-Protocol`，或带的不是 `subprotocols` 里的任一项，服务端回 **HTTP 400** 而不是升级。这是有意的——浏览器随手连的 WebSocket 不该被当成 MQTT 连接。
- `max_frame_bytes` 限制**单个 WebSocket 帧**；MQTT 报文级上限仍是 `broker_proto_mqtt.mqtt_max_packet_size`。帧超大 → close `1002`；text 帧 → close `1003`。
- TLS 与其他 listener 一样由 `broker_security.tls.enabled` 一个开关控制（wss）；没有单独的 WS TLS 配置。

### 1.1 MQTT-SN（UDP 1884）

`broker_proto_mqtt_sn` 的配置（`config/sys.config.src`）：

```erlang
{broker_proto_mqtt_sn, [
  {listeners, [#{port => 1884}]},
  {predefined_topics, #{}},
  {gateway_id, 1},
  {advertise_interval_ms, 0},
  {advertise_address, {255, 255, 255, 255}},
  {advertise_port, 1884},
  {max_asleep_clients, 1024},
  {max_topic_ids_per_client, 1024},
  {max_inflight, 20},
  {max_duration_s, 3600}
]}.
```

- **一个 socket 服务整个网关**：UDP 没有连接，会话按 `{IP, Port}` 区分、`client_id` 在 CONNECT 时绑定。因此 `listeners` 里只有一条，且**没有** `num_acceptors` / `max_connections` —— 那两个旋钮属于 TCP。`port => 0` 时由内核分配，`mqtt_sn_listener:port/1` 可读回真实端口（CT 与兼容性套件都这么用）。
- **`predefined_topics`**：`#{1 => <<"t0/sensor/temp">>}` 形状的表，**全局共享、只读**，客户端用 TopicIdType=predefined 直接引用，不需要 REGISTER。id `0`、`0xFFFE`、`0xFFFF` 是保留值，写进去会在**启动时**失败（不静默丢弃）。改这张表要重启。
- **`advertise_address` / `advertise_port` 不是本 listener 的地址**：ADVERTISE 是广播给**客户端监听端口**的（默认 `{255,255,255,255}`:1884，即 MQTT-SN 惯用端口）。单机部署把地址指到 `127.0.0.1`；`advertise_interval_ms = 0`（默认）表示永不主动广播，但仍会应答任何 SEARCHGW。
- **`max_inflight`**（默认 20）：每会话未 ACK 的下行 QoS 1/2 条数。窗口满时消息**留在分区里、不落会话缓冲**，下一次 PINGREQ 从 cursor 重试；不会因为客户端慢而丢消息，也不会无限吃内存。
- **`max_asleep_clients`**（默认 1024）：休眠会话登记上限。超限时拒绝进入休眠并按**正常断开**处理（计数 `mqtt_sn_sleep_denied`），不假装睡着后丢掉期间的所有消息。
- **`max_duration_s`**（默认 3600）：客户端请求的 keepalive/sleep 时长的上限，防止一个荒谬值把会话钉一星期。keepalive 实际是 1.5× 探测 / 2× 判死。
- **`gateway_id`**：GWINFO 里回的网关 id。
- **DTLS 不支持**：`broker_security.tls.enabled` 对 1884 不生效（见 §1 的例外说明）。请不要把 MQTT-SN 暴露到不可信网络。

**指标**：`mqtt_sn_asleep`（当前休眠会话数，gauge）、`mqtt_sn_sleep_denied`（被拒的休眠请求，counter）、`mqtt_sn_keepalive_timeout`（2× 静默判死，counter）。三个 key 都在 `broker_metrics` 的封闭集里，`/metrics` 直接可见。

**排查**：
- 客户端连不上时先确认是 **UDP** 且端口对得上（`ss -lunp | grep 1884`）；拿 `1883`（TCP MQTT）去连必然超时。
- 客户端反复 REGISTER 却收不到 REGACK：检查 `predefined_topics` 启动是否成功（id 0 会让 app 拒绝启动）。
- 会话莫名消失：看 `mqtt_sn_keepalive_timeout` 是否在涨——MQTT-SN 客户端必须在自己声明的 keepalive 内发 PINGREQ。

### 1.2 MQTT over QUIC（UDP 14567，默认关闭）

`broker_proto_mqtt_quic` 实现 OASIS MQTT TC《MQTT over QUIC — Single Stream Mode》CN Draft 3：MQTT 报文零改动，整条连接走**一条客户端发起的双向流**，ALPN 固定 `mqtt`。

**构建前提**：`quicer`（msquic + quictls 的 NIF）只在 `quic` profile 里，且第一次构建要编 C 依赖（`cmake`/`ninja`/`cc`，约 10–15 分钟）：

```sh
rebar3 as quic compile      # 或 rebar3 as quic ct / release
```

默认 profile（`rebar3 compile/ct/release`）**完全不拉 quicer**，`broker_proto_mqtt_quic` 照常编译并启动（它出现在两个 release 的 app 列表里，`optional_applications => [quicer]`），只是列表里没有 listener；手工开 listener 会得到 `{error, quicer_not_available}`。

**配置**（`config/sys.config.src` 里默认 `listeners => []`）：

```erlang
{broker_proto_mqtt_quic, [
  {listeners, [#{port => 14567,         % 默认 []，见下
                 max_connections => 1024}]},
  {alpn, [<<"mqtt">>]},
  {handshake_timeout_ms, 5000},
  {idle_timeout_ms, 30000},
  {max_streams_per_conn, 4}
]}.
```

**listener map 的 key**（`mqtt_quic_listener` 的 `init/1` 读；`port` 必需，其余可省）：

| key | 缺省 | 说明 |
|---|---|---|
| `port` | — | **必填**。UDP 端口，`0` 由内核分配。 |
| `max_connections` | 1024 | 该 listener 的连接上限，超限的新连接被拒。 |
| `certfile` / `keyfile` | 取自 `broker_security.tls` | 写在 map 里就**只**对这个 listener 生效，且不要求 `tls.enabled => true`。两者必须同时给，缺一个仍以 `{quic_requires_tls, Key}` 拒绝启动。 |
| `max_streams_per_conn` | app 级同名的值（4） | 只覆盖这一个 listener 的流信用。 |
| `handshake_timeout_ms` / `idle_timeout_ms` | app 级同名的值（5000 / 30000） | 同上，按 listener 覆盖。 |

**为什么默认不监听**：QUIC 没有明文模式，没有证书时 listener 会以 `{quic_requires_tls, certfile}` 拒绝启动，而**拒绝启动是该 app 的启动失败**——即装出来的 release 起不来。所以默认 `[]`，启用顺序是：

1. 给 `broker_security.tls` 配 `certfile`/`keyfile` 并 `enabled => true`（见 `docs/security.md` §9）；
2. 把 `listeners` 改成 `[#{port => 14567}]` 再重启。

证书也可以直接写在 listener map 里（`#{port => 14567, certfile => "...", keyfile => "..."}`），那样不依赖 `broker_security.tls`——但 QUIC 仍然是 TLS 1.3，这一点不会变。14567 是 EMQX 的 QUIC 惯用端口，便于与 EMQX 对照测试。

- **`alpn`**：CN Draft 3 §2.1 只定义 `mqtt` 一个值，客户端不带它就**握手失败**（`{error, transport_down, #{status := alpn_neg_failure}}`），不会静默降级。
- **`max_streams_per_conn`**：每条 QUIC 连接在建连时一次性拿到的双向流信用，语义是「**同一连接上最多重连几次**」（流关闭 ≠ 连接关闭，客户端可以开新流重新 CONNECT），不是并发流数——单流模式同一时刻只有一条活动 MQTT 流。quicer 0.4.12 不支持运行期补充信用，所以它是一次给足的配额。
- **`idle_timeout_ms`** 是 QUIC 连接级的 idle timeout；MQTT 自己的 keepalive 仍在 CONNECT 里协商，两者互不替代。
- **没有 0-RTT、没有多流（EMQX 私有扩展）**：本实现只做规范的单流模式。

**指标**：QUIC 连接与 MQTT 会话走同一套引擎指标（`connections_current`、`publish_total`、`protocol_errors` 等）；`protocol_errors` 在这一层尤其值得看——它涨说明传输层把事件当成了协议数据（S8-D 收尾就修过一处：`active, 1` 被 quicer 当作 multi 模式，每包多一个 `{quic, passive, ...}` 事件）。

**排查**：
- 先确认是 **UDP**：`ss -lunp | grep 14567`。QUIC listener 不出现在 `inet:i/0` 里，TCP 客户端连不上是正常的。
- 握手瞬间失败但 UDP 端口在听：多半是 **ALPN 不是 `mqtt`**，或客户端不信任证书（自签证书需客户端显式 `verify => none` 或导入 CA）。
- `rebar3 as quic release` 出来的 release 起不来且日志里有 `{quic_requires_tls, _}`：证书没配，见上面的启用顺序。
- 客户端在同一 QUIC 连接上重连超过 `max_streams_per_conn` 次后新流不再到达：日志里会有 `quic: stream credit exhausted`，让客户端新建 QUIC 连接（或调大该值）。
- 真实客户端对照：`test/protocol-compatibility/mqtt_quic/` 用 EMQX 的 `emqtt` 1.16.1 跑 8 个用例，见 `docs/protocol-compatibility.md` §4.12。

### 1.3 A2A JSON-RPC 前端（默认关闭，S9-A / S9-B / S9-C）

`broker_proto_a2a` 是 A2A 1.0.0 的 JSON-RPC 传输绑定前端。**S9-C 起**它有一整套：**Task 生命周期**（`SendMessage` 建 Task、`GetTask`/`ListTasks`/`CancelTask` 读投影）、**Agent Card 发现面**（`/.well-known/agent-card.json`、`/agents`、`GetExtendedAgentCard`）、**SSE 流式**（`SendStreamingMessage`/`SubscribeToTask` 回 `text/event-stream`）、**出站 Push webhook**（Push 四方法 + 终态投递/重试/DLQ，默认关闭）。它**默认不监听**（ADR-026 §D4）：app 装进 release、随节点启动，但 `listeners => []` 时不开任何端口。

**配置**（`config/sys.config.src`，出厂值）：

```erlang
{broker_proto_a2a, [
  {listeners, []},                       % 默认零 listener
  {require_auth, true},                  % 新面没有历史包袱，失败关闭
  {max_body_bytes, 1048576},             % 与 max_message_bytes 取 min
  {max_connections, 1024},
  {request_timeout_ms, 30000},
  {supported_versions, [<<"1.0">>]},
  {task_reliability, fsync},             % Task 事件的可靠性档（ADR-025 §D4）
  {task_shards, undefined},              % undefined => schedulers_online
  {task_timeout_ms, 30000},
  {task_auto_complete, true},            % 内建 echo agent 同步跑完；外部执行器设 false
  {agent_cards, []},                     % boot 种子卡（写 broker_meta.agent_card）
  {agent_card_max_age, 60},              % Cache-Control: max-age
  {sse_max_event_bytes, 1048576},        % 单帧上限（S9-C 已接到方法）
  {sse_heartbeat_ms, 15000},             % 空闲心跳（SSE 注释行）
  {sse_max_duration_ms, 1800000},        % 一条流的硬上限（模块默认 600000）
  {push_enabled, false},                 % 出厂关闭：worker 不进监督树，Push 四方法回 -32003
  {egress_allowlist, []},                % 全局出站白名单（空 = 全部拒绝）
  {push_tenant_allowlist, #{}},          % 租户级白名单；与全局取交
  {push_auth_secrets, #{}},              % 按 scheme 名放 HMAC 密钥（不在 broker_meta 里）
  {push_allow_http, false},              % 明文 webhook 默认拒绝（只给本地测试开）
  {push_max_attempts, 5},                % 重试上限
  {push_base_backoff_ms, 500},           % 指数退避基数
  {push_max_backoff_ms, 30000},          % 退避封顶
  {push_ttl_ms, 900000},                 % 一条投递的总时限（模块默认 300000）
  {push_timeouts, #{connect_ms => 5000, recv_ms => 10000}},
  {push_max_response_bytes, 65536},      % 只读状态行，但仍给响应体一个上限
  {push_inet_family, inet}
]}.
```

**启用顺序**：

1. 在 `broker_security.users` 里给调用方建一条带 `token` 的用户记录（`{<<"agent">>, #{token => <<"...">>, tenant_id => <<"t1">>}}`）——A2A 只接受 `broker_user` 里的**不透明 bearer token**，不验 JWT、不取 JWKS。**租户决定可见范围**：t1 的调用方看不到 t2 的 Task 与 Agent Card。
2. 如需按主体限制，配 `broker_security.acl` 的 `publish`（`SendMessage`/`CancelTask`）与 `subscribe`（`GetTask`/`ListTasks`/`GetExtendedAgentCard`）规则——resource 是真实 destination `$a2a/task/<id>`（`SendMessage` 用 `$a2a/task/new`）；ACL 拒绝回裸 `403`。
3. 要暴露 Agent Card，配 `agent_cards` 种子（或先让 `broker_meta.table_storage = disc_copies` 再用管理面/直接写表）。种子在 app 启动时幂等写入；内容不变不会 bump `version`（ETag 稳定）。
4. **要开 Push**（默认关）：`push_enabled => true` + 非空 `egress_allowlist`（否则所有出站都被拒），需要私网/loopback 目标时在该条目上显式 `allow_private => true`；按需在 `push_tenant_allowlist` 里对租户再收窄。客户端声明的 `authentication.scheme` **必须在 `push_auth_secrets` 里有同名条目**，否则该条投递被跳过（`auth_not_configured`）——不会降级为明文投递。
5. 把 `listeners` 改成 `[#{port => 8081, ref => {a2a, 8081}, num_acceptors => 10, max_connections => 1024}]`（`ip` 默认 `{127,0,0,1}`）再重启。
6. 探活：`curl -sS http://127.0.0.1:8081/health` → `{"status":"ok"}`，不需要凭据。

**`require_auth => false` 只在回环绑定下允许**：boot 时校验所有 listener 的 `ip`，非回环直接 `{error, {insecure_listener, Port, Ip}}`，整个 app 起不来。这是刻意的——与其起一个无认证的任务 API，不如让节点起不来。

**客户端调用形态**：

```sh
curl -sS -X POST http://127.0.0.1:8081/ \
  -H 'Content-Type: application/json' \
  -H 'A2A-Version: 1.0' \
  -H 'Authorization: Bearer <token>' \
  -d '{"jsonrpc":"2.0","id":1,"method":"SendMessage","params":{"message":{
        "messageId":"m1","role":"ROLE_USER","contextId":"ctx-1",
        "parts":[{"text":"hi"}]}}}'
```

返回 `result.task`（含 `id`/`contextId`/`status.state`/`history`）。`task_auto_complete=true` 时 `status.state` 直接是 `TASK_STATE_COMPLETED`。

- `A2A-Version` **必须带**：缺失时按 `0.3` 读入，而服务端只声明支持 `1.0`，所以缺失会回 `-32009`（HTTP 400）。这是 spec 要求客户端带版本头的直接结果。
- `Content-Type` 必须是 `application/json` 或 `application/a2a+json`，否则 `415`。
- 请求体上限 = `min(max_body_bytes, broker_security.limits.max_message_bytes)`；`Content-Length` 超限时**不读体**直接 `413` + `Connection: close`。
- **Agent Card 发现端点不需要 `A2A-Version`**：`GET /.well-known/agent-card.json` 与 `GET /agents` 走独立的 GET 路由（浏览器/缓存不会带版本头）。它们也不需要凭据，但**匿名身份是默认租户**（`broker_security.default_tenant`，默认 `t0`）——「匿名可读」不等于"跨租户可读"。

**排查**：

- 端口没起来：先看 `listeners` 是不是还是 `[]`；再看日志里有没有 `insecure_listener`（`require_auth=false` + 非回环）。
- 全部请求 `401`：`Authorization: Bearer` 的 token 与 `broker_security.users` 里的 `token` 不一致，或用户表为空（用户表为空时任何凭据都失败）。
- 全部请求 `-32009`：客户端没带 `A2A-Version: 1.0`（或带了 `2.0`）。
- `SendMessage` 回 `-32602`：`params.message` 缺 `messageId`/`role`/`parts`，或某个 Part 不是「恰好一个内容键」（`text`/`raw`/`url`/`data` 选一）。
- `GetTask` 回 `404` + `-32001`：Task id 不存在，**或属于别的租户**（两者故意同码，不暴露存在性）。
- `CancelTask` 回 `400` + `-32002`：Task 已进入终态（`COMPLETED`/`FAILED`/`CANCELED`/`REJECTED`），终态不可变。
- `GetExtendedAgentCard` 回 `400` + `-32007`：该卡没有 `extended_card`。
- `GET /.well-known/agent-card.json` 回 `404 {"error":"agent_card_not_found"}`：当前租户下没有 active 卡。**匿名请求看的是 `default_tenant`**，所以「配了卡但匿名拿不到」通常是卡配在 t1 而匿名是 t0。
- Agent Card 改了但客户端还拿旧的：`If-None-Match` 命中会回 `304`；ETag 由 `version` + 内容哈希组成，所以只有**真改**才会变。`ram_copies` 下重启会让 `version` 从头开始。
- 重启后任务"没了"：先确认 `broker_queue` 的 WAL 目录是同一个（`broker_storage.data_dir`），再看日志里 `a2a_recovery_failed`。A2A 的投影完全来自日志，日志换了目录就等于换了一个节点。
- 重启后任务变 `FAILED` 且带 `history_lost`：创建事件已被 retention 回收。调大 `broker_storage.retention` 或按 §5.1 的估算预留容量。
- `-32601`：方法名不在 A2A 1.0 的方法集里（S9-C 之后 `SendStreamingMessage`/`SubscribeToTask`/Push 四方法都已实现，不再回 `-32601`）。
- **SSE 流没有 `data:` 帧**：`SubscribeToTask` 传的 id 存在但事件都已被 retention 回收（`history_lost` 会先发一帧 `status=failed` 再关流）；或 Task 从未产生过新事件，只看到 `: ping` 心跳。
- **webhook 没收到 POST**：按顺序看——`push_enabled` 是否 `true`；`egress_allowlist` 是否非空且 host 后缀匹配（日志 `{guard_refused, host_not_allowlisted}` / `empty_tenant_allowlist`）；`push_allow_http` 是否允许明文（`https` 不需要）；客户端声明的 `authentication.scheme` 是否在 `push_auth_secrets` 里有同名条目（日志 `{auth_not_configured, not_configured}`——**这是跳过，不是失败投递**）。
- **webhook 收到多次 POST**：按 `push_max_attempts` 的指数退避重试，正常现象；端点幂等靠 `X-A2A-Task-Id` + `X-A2A-Config-Id` + `X-A2A-Nonce` 去重。
- **投递耗尽后消息去哪了**：写进 `$a2a/push/dlq/<tenant>`（租户名替换）。它是普通消息——用任一前端订阅该 destination 即可审计；payload 里有 `taskId`/`configId`/`url`/`attempts`/`reason`。
- 真实请求/响应原文见 `docs/protocol-compatibility.md` §4.14（S9-A）、§4.15（S9-B）与 §4.16（S9-C）。

### 1.4 MCP 前端（默认关闭，S9-D）

`broker_proto_mcp` 是 MCP `2026-07-28` 的前端，形态是**网关**：`server/discover` 与 `subscriptions/listen` 由 Broker 本地回答，其余原语（`tools/*`、`resources/*`、`prompts/*`、`completion/complete`）原样发布到 `$mcp/<method>`，由后端消费并回复。**Broker 不执行工具语义**；`sampling`/`elicitation` 不支持。默认不监听，stdio 也默认关闭。

**配置**（`config/sys.config.src`，出厂值）：

```erlang
{broker_proto_mcp, [
  {listeners, []},                    % 默认零 listener
  {require_auth, true},               % 新面没有历史包袱，失败关闭
  {path, <<"/">>},                    % 单端点（2026-07-28 只接受 POST）
  {max_body_bytes, 1048576},          % 与 max_message_bytes 取 min
  {max_connections, 1024},
  {request_timeout_ms, 30000},        % 一次透传调用的等待上限（超时 -> -32603）
  {features, []},                     % 诚实空：没挂后端就没有能力
  {discover_ttl_ms, 3600000},         % server/discover 的 ttlMs
  {instructions, undefined},          % 可选的自然语言说明
  {allowed_origins, []},              % Origin 白名单（loopback 总是允许）
  {stdio_enabled, false},             % 不抢占 release 的 stdin
  {sse_max_event_bytes, 1048576},     % 单帧上限
  {sse_heartbeat_ms, 15000},          % 空闲 ": ping"
  {sse_max_duration_ms, 600000},      % 一条流的硬上限
  {request_reliability, fsync}        % 请求消息的可靠性档（ADR-007）
]}.
```

**启用顺序**：

1. 在 `broker_security.users` 里给调用方建一条带 `token` 的用户记录；MCP HTTP 只接受 `Authorization: Bearer <token>`（`broker_security` 的不透明 token），`X-API-Key` 也识别。租户由用户记录决定，客户端不能自选。
2. 配 `features`：`[tools, resources, prompts, completions]` 的子集。**没配的 family 一律 `404 + -32601`**（`data.reason="feature not advertised"`）；空列表是诚实的出厂值（`server/discover` 的 `capabilities` 也为空）。
3. 让后端消费 `$mcp/<method>`：消息是普通 `#broker_message{}`（`payload_encoding=json`），payload 是客户端 JSON-RPC 原文，`_meta.unified-mq` 里带 `replyTo`/`requestToken`/`tenantId`；`headers` 里另有 `reply_to`/`tenant_id`/`request_token`。后端把 `{"result":…}` 或 `{"error":{…}}` 发到 `replyTo`；要发进度就在 `_meta.unified-mq.kind` 写 `progress`，要发通知就写 `notification`。
4. 如需按主体限制，配 `broker_security.acl` 的 `publish`（原语调用，resource 是 `$mcp/<method>`）与 `subscribe`（`subscriptions/listen`，resource 是 `$mcp/notify`）规则。**MCP 的 ACL 检查不做租户比较**（`$mcp/*` 是网关自己的命名空间，租户由网关写入），HTTP ACL 拒绝回裸 `403`。
5. 把 `listeners` 改成 `[#{port => 8082, ref => {mcp, 8082}, num_acceptors => 10, max_connections => 1024}]`（`ip` 默认 `{127,0,0,1}`）再重启。`require_auth => false` 只在回环绑定下允许（同 A2A 的 boot 校验）。

**客户端调用形态**（HTTP；三个镜像头都必填）：

```sh
curl -sS -N -X POST http://127.0.0.1:8082/ \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -H 'MCP-Protocol-Version: 2026-07-28' \
  -H 'Mcp-Method: tools/call' \
  -H 'Mcp-Name: my-tool' \
  -H 'Authorization: Bearer <token>' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{
        "name":"my-tool","arguments":{},
        "_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28",
                 "io.modelcontextprotocol/clientCapabilities":{},
                 "progressToken":"p1"}}}'
```

- `server/discover` 回**单帧** `application/json`；所有透传方法回 `text/event-stream`（进度帧在前，终止帧是带客户端 `id` 的 Response；空闲 `: ping`）。
- `subscriptions/listen` 第一帧固定是 `notifications/subscriptions/acknowledged`；到达 `sse_max_duration_ms` 时先回 `resultType: complete` 再关流。
- **通知 POST**（`notifications/*`）回 `202` 无 body；服务端不回复。

**stdio 部署**：`stdio_enabled => true` 且 `listeners => []`（或保留 HTTP 也行，但同机通常二选一）。协议帧写 `stdout`，**部署必须把 logger 的 `default` handler 指向 `stderr`**，否则日志会污染协议流：

```erlang
%% vm.args / -eval 里（OTP 不允许原地改 type）
logger:remove_handler(default),
ok = logger:add_handler(default, logger_std_h,
                        #{config => #{type => standard_error}}).
```

一行一条 JSON（不得内嵌换行）；EOF 即优雅退出。stdio **没有认证**：身份固定是匿名（默认租户），信任边界是进程访问权。`notifications/cancelled` 按 listen 请求 id 取消订阅子进程。

**排查**：

- 端口没起来：先看 `listeners` 是否为 `[]`，再看日志里有没有 `insecure_listener`。
- 全部 `401`：token 与 `broker_security.users` 不一致，或用户表为空。401 带 `WWW-Authenticate: Bearer realm="mcp"`、无 body。
- 全部 `400 + -32020`：`MCP-Protocol-Version`/`Mcp-Method`/`Mcp-Name` 缺失或与 body 不一致（`Mcp-Name` 只要求在 `tools/call`/`resources/read`/`prompts/get` 上出现）。三个头都**必填**，不是可选。
- `400 + -32602`：body 缺 `_meta.io.modelcontextprotocol/protocolVersion` 或 `clientCapabilities`。
- `400 + -32022`：`supportedVersions` 只声明 `2026-07-28`；`data.supported/requested` 会给出对照。
- `404 + -32601`：方法不存在、`initialize`（无握手）、`sampling`/`elicitation`（明确拒绝），或该方法所属 feature 没配（`data.reason="feature not advertised"`）。
- 客户端拿不到结果、只看到 `: ping`：确认后端消费了 `$mcp/<method>` 并发布到 `replyTo`；`request_timeout_ms` 到点会收到 `-32603` + `data.reason="timeout"`。
- `406`：`Accept` 必须**同时**含 `application/json` 与 `text/event-stream`（或 `*/*`）。`415`：`Content-Type` 必须是 `application/json`。`403`：`Origin` 存在但不在 `allowed_origins` 且不是 loopback。
- stdio 的 stdout 混进日志：按上面的 logger 配置把 `default` handler 换到 `standard_error`；本 app 不会自动改。
- 真实请求/响应原文见 `docs/protocol-compatibility.md` §4.17。**官方 MCP SDK / Inspector 的客户端矩阵留 S9-F。**



---

## 2. 管理 API

Base URL：`http://127.0.0.1:8090`。设置 token 后，除健康检查外都需要：

```
Authorization: Bearer <token>
```

（也接受 `X-Admin-Token: <token>`。）

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/healthz` | 存活：进程在即 200，永远不鉴权 |
| GET | `/readyz` | 就绪：必需 app 未全起时 503，列缺失 app |
| GET | `/metrics` | Prometheus 文本（封闭 key 集合） |
| GET | `/api/v1/status` | 节点、版本、分区、retention、集群、指标快照 |
| GET | `/api/v1/config` | 脱敏配置快照 + SHA-256 |
| GET | `/api/v1/version` | release/OTP/数据格式/配置版本 |
| GET | `/api/v1/backups` | 备份清单（名称、字节、mtime） |
| POST | `/api/v1/backup` | 创建一次热备份 |
| POST | `/api/v1/tls/reload` | 清 `ssl` PEM 缓存，轮换证书 |

`/readyz` 检查的 app：`broker_storage`、`broker_router`、`broker_queue`、`broker_subscription`、`broker_event`、`broker_security`。`broker_metrics` 与 `broker_admin` 故意不在其中：指标/管理面故障不应把 broker 摘出负载均衡。

示例：

```bash
curl -fsS http://127.0.0.1:8090/healthz
curl -fsS http://127.0.0.1:8090/readyz
curl -fsS -H "Authorization: Bearer $ADMIN_TOKEN" \
     http://127.0.0.1:8090/api/v1/status | jq .
curl -fsS -H "Authorization: Bearer $ADMIN_TOKEN" \
     -X POST http://127.0.0.1:8090/api/v1/backup
```

---

## 3. 日常巡检

### 3.1 健康与就绪

- `/healthz` 200 且 `/readyz` 200：正常。
- `/readyz` 503：响应体会列出缺失 app，先查该 app 的启动日志（`data/log/s1.log`）。
- 进程在但 `/readyz` 一直 503：检查数据目录是否可写、分区是否已带外创建（见 §5）。

### 3.2 关键指标

`/metrics` 的 key 是封闭集合（ADR-016）。巡检优先级：

| 指标 | 含义 | 关注点 |
|---|---|---|
| `connections_current` / `connections_peak` | 当前/峰值连接 | 突增或触顶 `max_connections` |
| `auth_failures` / `authz_failures` | 认证失败 / 授权失败 | 前者账号问题，后者 ACL/租户问题 |
| `publish_total` / `deliver_total` / `ack_total` | 发布/投递/确认计数 | 三者比例失衡说明消费滞后 |
| `publish_latency_us` / `deliver_latency_us` / `ack_latency_us` | 直方图 | p99 抬升先看 WAL |
| `queue_depth` / `subscription_backlog` / `cursor_lag` | 积压 | 持续增长 → 慢消费者或容量不足 |
| `redelivery_total` / `dlq_size` | 重投递 / 死信 | DLQ 增长说明消费端反复失败 |
| `wal_sync_latency_us` / `wal_pending_append` | 刷盘延迟/待刷队列 | 磁盘或组提交异常 |
| `segment_bytes` / `recovery_time_us` | 段大小 / 恢复耗时 | 恢复时间随日志增长 |
| `retention_deleted_bytes` / `retention_blocked_by_slow_consumer` | 段回收 | 被慢消费者钉住说明有订阅不推进 |
| `cluster_owner_changes` / `cluster_fencing_rejected` / `cluster_no_quorum` | 集群 | 频繁切换或失去多数派 |
| `cluster_replication_failed` / `cluster_replication_latency_us` | 复制失败计数 / 复制往返延迟 | 失败上升或 p99 逼近 `replicate_timeout_ms` 说明副本跟不上 |
| `backpressure_pauses` / `backpressure_disconnects` | 背压 | 与 `conn_mailbox_len` 一起看 |

### 3.3 版本与配置审计

```bash
curl -fsS -H "Authorization: Bearer $ADMIN_TOKEN" \
     http://127.0.0.1:8090/api/v1/version | jq .
curl -fsS -H "Authorization: Bearer $ADMIN_TOKEN" \
     http://127.0.0.1:8090/api/v1/config | jq -r .hash
```

`data/config-audit.log` 每次 boot 追加一行（脱敏快照 + hash）。升级前后对比 hash，可确认节点实际加载的配置没有漂移。

---

## 4. 备份与恢复

### 4.1 备份

备份是 `<data_dir>` **整棵树**（递归）的 gzip tar，热备份（读取时 broker 仍在运行）。当前包含：

- `wal/`：各分区的追加日志、索引与 meta；
- `sessions/`：`.subs` / `.cursors` / `.inflight`；
- `retain/retain.v1`：保留消息快照；
- `mnesia/`、`ra/`：S4 控制面（Mnesia 表 + Metadata Raft）；
- `config-audit.log`：脱敏配置审计。

热备份对 WAL 是安全的：正在追加的段按原样复制，恢复时走 `wal_recovery` 的撕裂尾规则；会话与 retain 文件用 temp+rename，读者只会看到完整旧文件或完整新文件。**`mnesia/` 与 `ra/` 是活的控制面状态，没有撕裂尾规则**；要求控制面逐字节一致时，先静止写入或停节点再备份。需要逐字节一致的业务数据同样应先静止写入。

```bash
# 通过 API
curl -fsS -H "Authorization: Bearer $ADMIN_TOKEN" \
     -X POST http://127.0.0.1:8090/api/v1/backup | jq .

# 直接调用（运维 shell / 远程 console）
erl -pa _build/default/lib/*/ebin -noshell -eval \
  'io:format("~p~n", [broker_backup:create()]), halt().'
```

- 输出目录由 `broker_admin.backup_dir` 决定，必须是 release 树外的绝对路径（容器 harness 渲染配置时会把它映射到数据卷的 `backups/`）。
- 备份期间正在追加的 WAL 段按原样复制；恢复时按 `wal_recovery` 的撕裂尾规则处理，因此**业务数据的热备份是安全的**。`mnesia/` 与 `ra/` 为控制面，热备份不保证逐字节一致；需要一致时先静止写入或停节点。
- `GET /api/v1/backups` 返回清单，用于核对备份是否落在预期位置。

### 4.2 恢复

恢复是显式的：`broker_backup:restore(Archive, TargetDir)` 只解压到调用者指定的**空目录或不存在目录**，从不覆盖已有数据。

```bash
erl -pa _build/default/lib/*/ebin -noshell -eval \
  'io:format("~p~n", [broker_backup:restore("backups/unified-mq-20261109-120000.tar.gz", "/srv/unified-mq-restore")]), halt().'
```

步骤：

1. 停目标节点（滚动升级窗口内一次只动一个）。
2. 解压到 release 树外的新目录（上例 `/srv/unified-mq-restore`）。
3. 把**五个**数据目录配置指向恢复树：`broker_storage.data_dir` / `broker_subscription.data_dir` / `broker_retain.data_dir` 指向恢复根；`broker_meta.data_dir` 指向 `<恢复根>/mnesia`，`broker_cluster.data_dir` 指向 `<恢复根>/ra`。
4. 启动节点，确认 `/readyz` 200 且 `/api/v1/status` 的分区与预期一致。
5. 校验：发布一条 QoS1 并确认订阅端收到，再检查 `recovery_time_us`。

**禁止**直接解压覆盖在线数据目录——`restore` 本身也会拒绝非空目标。

---

## 5. 分区与数据目录

- 分区创建是**带外**动作：干净的 `data_dir` 启动会失败（ADR-011 §11.2），必须先 `broker_queue:create_partition/1`。
- 数据目录必须在 release 树外、ext4（tmpfs/ramfs 被存储层拒绝）。
- 目录布局与恢复算法见 `docs/storage-and-recovery.md`。

**控制面持久性（必须显式配置）**：`broker_meta` 的 12 张控制面表（用户、ACL、schema 版本、outbox、Agent Card）默认建为 **`ram_copies`**——`sys.config.src` 没有覆盖 `table_storage`，代码默认即 `ram_copies`，此时节点重启**全部丢失**（`maybe_set_dir` 也只在 `disc_copies` 时设置 `mnesia.dir`）。ADR-021 §D1 要求生产配置：

```erlang
{broker_meta, [{data_dir, "<abs>/data/mnesia"}, {table_storage, disc_copies}]}
```

未设置时请按「重启后用户/ACL/Agent Card 需重新建立」理解现状。若代理卡来自 `broker_proto_a2a.agent_cards` 种子，则重启后会被重新写回（但 `version` 会从 1 重新开始，客户端缓存的 ETag 因此失效一次）；管理面写入的卡在 `ram_copies` 下会丢失——这正是 `disc_copies` 要解决的问题。

---

## 5.1 A2A Task 数据的量与保留（S9-B）

**每个 Task 写 4 条事件**（`task` 创建快照 / `status_update` WORKING / 一条 `message` / `status_update` 终态）。`task_auto_complete=false` 时创建只有 1 条（`task`），取消再加 1 条（`status_update` CANCELED）。事件是**普通消息**，落在 `$a2a/task/<TaskId>`，因此：

| 关注点 | 事实 |
|---|---|
| 存储位置 | 与 MQTT 消息同一批分区、同一套 WAL 段；没有单独的 A2A 数据目录 |
| retention | 完全由 `broker_storage.retention`（`max_bytes` / `max_age_ms`）决定，**没有 A2A 专用保留策略**。按 §9 的同一公式估算 |
| 容量估算 | `每任务字节 ≈ 4 × (payload + ~200 B 消息头)`；一个 1 KiB 回包的任务约 5 KiB。100 万任务 ≈ 5 GB，需按 `max_bytes` 预留 |
| 投影 | 在 shard 的 ETS 里（Task 数 × 约 1–2 KiB，含 history）。随 retention 一起被约束——创建事件被 GC 后，下次 boot replay 会把该 Task 判为 `history_lost` 并补 `FAILED` |
| 重启成本 | boot 全量重放扫全部分区（S9-B 的正确性基线）。节点上有 N GB 日志时启动时间是分钟级，**不是秒级**；`task_checkpoint_dir` 相关 env 已声明但当前不读（S9-F 演进位） |
| 删除 | 没有单独的"删任务"接口：靠 retention 覆盖。删除 `$a2a/` 前缀数据只能走运维侧的 retention 配置 |

### 5.2 A2A 出站 Push 的运维面（S9-C）

**这是唯一由 Broker 主动发起的出站连接。** 数据面/控制面其余部分都是被动接受连接；Push 是例外，因此它的边界要单独看：

| 关注点 | 事实 / 处置 |
|---|---|
| 默认状态 | `push_enabled => false`：worker 不在监督树，Push 四方法回 `400` + `-32003`。**不配不生效**，也不会偷偷往外发 |
| 两层白名单 | `egress_allowlist`（全局）× `push_tenant_allowlist[Tenant]`，host 后缀匹配；任一为空则全拒。私网/loopback 目标必须在该条目上显式 `allow_private => true` |
| 出站目标 | 只连**已解析并校验过的 IP**（pin），带 `Host` 头；`https` 为默认，明文要 `push_allow_http => true` |
| 重定向 | 3xx 一律当失败（不跟随），杜绝"白名单 host 把我们 302 到内网" |
| 响应处理 | 只读状态行判成败，不解析响应体；响应体上限 `push_max_response_bytes`（64 KiB） |
| 重试语义 | `push_max_attempts` 次指数退避（`push_base_backoff_ms × 2^attempt`，封顶 `push_max_backoff_ms`）；超过 `push_ttl_ms` 放弃。端点的幂等性由它自己按 `X-A2A-Task-Id`/`X-A2A-Config-Id`/`X-A2A-Nonce` 保证 |
| 签名 | 客户端声明 `authentication.scheme`，服务端用 `push_auth_secrets[scheme]` 的密钥签 `X-A2A-Signature: sha256=HMAC(timestamp "." nonce "." body)`；声明了未配置的 scheme → **跳过投递**（不降级明文） |
| DLQ | 耗尽/过期写 `$a2a/push/dlq/<tenant>`（一条普通消息，`message_id` = taskId）。**没有自动重投**：谁订阅谁负责。它的 retention 与 §5.1 同一套 |
| 容量 | DLQ 每条约几百字节；失败率高的部署要盯它，别让它悄悄占满分区 |
| 阻塞性 | 投递**从不阻塞任务终态**：分片只 `cast` 一次。worker 崩溃丢的是在途通知，不是任务状态 |
| 告警占位（S9-F 落地） | 失败路径计数器（`a2a_push_dlq` / `a2a_stream_*`）推迟到 S9-F 统一动冻结键集；当前盯日志 `a2a push skipped` / `a2a push dlq` |

**告警占位（S9-F 落地）**：S9-B/C 没有新增 `broker_metrics` 键——`a2a_state_violation` / `a2a_task_history_lost` / `a2a_checkpoint_corrupt`（S9-B）与 Push/Stream 失败计数器（S9-C）都推迟到 S9-F 统一动冻结键集时一并加。当前这些事件只进日志，运维侧按下面的日志特征盯：

| 现象 | 日志特征 | 含义 / 处置 |
|---|---|---|
| Task 被补成 `FAILED` | `history_lost`（`a2a_task_projection` / `a2a_task_recovery`） | 创建事件被 retention GC 掉了。要么调大 `max_bytes`/`max_age_ms`，要么接受"客户端应尽早读终态" |
| 投影与日志不一致的嫌疑 | `a2a_task_recovery` 的重放耗时异常增长 | 日志变大而 checkpoint 未做（S9-F）；短期靠增加 `task_shards` 分摊，长期等 checkpoint |
| 状态机拒绝 | `a2a_state_violation` 语义的日志行（`illegal_transition`） | 客户端在终态上重发/重取消；属客户端错误，`-32002`/`-32004` 是正常答复，不是故障 |

---

## 6. 滚动升级与版本兼容

`/api/v1/version` 返回：

- `release`：`broker_core` 的 vsn（当前 `0.1.0`）。
- `otp`：ERTS 版本（本项目固定 OTP 28.3.1 / ERTS 16.2）。
- `apps`：所有 `broker_*` app 的 vsn。
- `data_version`：新写入的 WAL 记录格式版本。
- `compatible_data_versions`：本节点能读取的版本窗口（当前 `[1]`）。
- `config_version`：`broker_meta` 的控制面版本（Mnesia/ra 可用时）。

升级流程：

1. 记录每台旧节点的 `data_version` 与 `/api/v1/config` 的 hash。
2. 确认新版本的 `compatible_data_versions` 覆盖旧 `data_version`；不覆盖则**不能**直接滚动升级，需先做数据迁移。
3. 一次升级一个节点，等 `/readyz` 200 且 `cluster_no_quorum = 0` 再动下一个。
4. 升级后对比 hash 与 `data_version`。

**本版本边界**：WAL 记录格式只有 v1，没有跨版本迁移工具；真正的「WAL v1→v2 迁移」是 S6 之后的待办。这里交付的是可查询的兼容窗口与升级顺序，不是迁移器。（会话文件的 `.subs`/`.cursors` v1→v2 是另一回事：boot 时由 `broker_subscription_files:migrate_v1/0` 自动完成，见 ADR-012 §12.5 / ADR-019。）

---

## 7. 故障演练（GameDay）

驱动：`tools/bench/s6_gameday.py`（`crash` / `slow_consumer` / `disk_readonly` / `partition`）。报告见 `docs/benchmarks/2026-11-09-s6-gameday.md`。

| 场景 | 注入 | 期望 | 观测 |
|---|---|---|---|
| crash | `docker kill -s KILL` | 已 ACK 的 QoS1 重启后必达；订阅集合不丢 | 到首个 CONNACK 的时间 = 恢复时间 |
| slow_consumer | 订阅端永不 ack，2 万条 QoS1 | 内存有界、broker 仍接新发布 | cgroup `memory.current` 增长 |
| disk_readonly | 数据卷 `:ro`（真 EROFS） | 拒绝启动或对不可持久写不 ACK；恢复可写后重新服务且旧数据在 | 恢复时间、旧消息是否仍在 |
| partition | 3 节点 Raft 杀 Leader / 失去多数派 | 多数派继续提交；少数派 `{error, no_quorum}` | CT 套件结果 |

演练前先备份；演练后核对 `/api/v1/status` 与 `data/config-audit.log`。

---

## 8. 告警规则

[`config/alerts.yml`](../config/alerts.yml) 是 Prometheus 规则（告警 + 记录），直接读取 `/metrics`。规则只使用封闭 key 集合里的指标，不含用户可控标签。部署时把它挂到 Prometheus 的 `rule_files`。

规则覆盖：就绪失败、认证/授权失败激增、积压与 cursor 滞后、DLQ 增长、WAL 刷盘延迟、恢复时间、慢消费者钉住回收、背压、失去多数派、磁盘（由 node_exporter 的 `node_filesystem_avail_bytes` 提供，可选）。

---

## 9. 容量规划与压测

- 容量基准：`docs/benchmarks/2026-11-09-s6-capacity.md`（四维矩阵 + 恢复时间）。
- 复跑：`python3 tools/bench/s2_bench.py --skip-build`。
- 快速结论（本机 16 逻辑核 / ext4 / OTP 28）：1 KiB 8 连接约 900 msg/s（fsync 档），吞吐随消息大小下降，1 MiB 约 11 msg/s；`kill -9` 恢复到首个 CONNACK 约 3.4–3.6 s。
- 容量估算以「持久化 msg/s × 消息大小 ÷ 副本因子 × 安全系数」为起点，必须用目标硬件复测，不要直接外推本机数字。

---

## 10. 故障排查速查

| 现象 | 先看 | 常见原因 |
|---|---|---|
| 启动即退出，日志 `partition_not_found` | data_dir 是否为空 | 未带外建分区 |
| 启动失败 `tmpfs`/`ramfs` | `mount` | 数据目录在内存文件系统 |
| `/readyz` 503 | 缺失 app | 某 app 启动失败，查 `data/log/s1.log` |
| 发布无 PUBACK | `wal_sync_latency_us`、磁盘 | 写盘失败/只读/满 |
| 积压不降 | `cursor_lag`、`retention_blocked_by_slow_consumer` | 慢消费者不 ack |
| `authz_failures` 激增 | `config` 的 `acl` | ACL 规则过严或租户不符 |
| 集群写失败 `no_quorum` | `cluster_*` | 存活节点不足多数派 |
| 管理 API 401 | `broker_admin.token` | 未带 Bearer 或 token 不符 |
