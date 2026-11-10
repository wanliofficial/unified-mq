# S8-C 执行计划 — MQTT-SN 1.2 网关（`broker_proto_mqtt_sn`）

- **前置**：S8-A（传输底座）与 S8-B（MQTT over WebSocket）已结案；设计见
  [`s8-mqtt-sn-mqtt-over-quic-and-mqtt-over-ws.md`](s8-mqtt-sn-mqtt-over-quic-and-mqtt-over-ws.md) §5。
- **执行方式**：inline 逐 Task，每个 Task 一次提交（与 S8-A / S8-B 相同）。
- **用例数**：所有"N 例"以 `all/0` 实数回填，不预先承诺。

---

## 关键架构决定（写代码前必须先定）

### 决定 1：一个 UDP socket、一个 reader 进程，不做 `num_workers`

设计草案里写了 `num_workers => 4`，实现不做：UDP 上多 reader 要么依赖 `SO_REUSEPORT`
（内核按四元组分流，同一客户端的不同包可能落到不同进程），要么在进程间再转发一次。
MQTT-SN 的会话状态是**按客户端**的，一个客户端必须固定由同一个进程串行处理它的报文。
所以：`mqtt_sn_listener`（gen_server）独占 socket，`{active, N}` 收包，按 `{IP, Port}`
把**原始字节**转发给会话进程；会话进程直接用同一个 socket 句柄 `gen_udp:send/4` 回包
（UDP port 命令允许非 controlling process 发送）。

配置里不保留 `num_workers`——**不留死配置**（S8-A 的教训）。

### 决定 2：会话按端点索引，`client_id` 在 CONNECT 时绑定

`listener` 维护 `#{{IP,Port} => SessionPid}`。CONNECT 之前没有 client_id，所以端点就是身份；
CONNECT 之后会话把 `{ClientId => Pid}` 登记到 listener，用于「同一 client_id 从新端点接管」。

- 同一端点重新 CONNECT 一个**不同** client_id：旧会话正常终止（detach、不发布 Will），新会话建立。
- 同一 client_id 从**新端点** CONNECT：视为 takeover，旧会话被要求关闭且**不发布 Will**（MQTT `takeover` 同义）。

### 决定 3：会话键与 MQTT 同域，复用 `broker_subscription` / `broker_queue` / `broker_router` / `broker_retain`

`ClientId` 直接用作 `broker_subscription` 的会话键，`attach/3` / `detach/2` / `subscribe/4` /
`ack/4` / `cursor/2` 与 MQTT/WS 完全同一套。投递入口是既有契约
`{broker_deliver, ClientId, MaxQoS, Refs, {P, O, Msg}}`；QoS0 的 push 与 QoS1/2 的 wake 都由它承载，
QoS1/2 的真实读位置仍是 cursor（与 `mqtt_conn_fsm` 的 K9/J6 一致）。

**不复用 `mqtt_conn_fsm`**：MQTT-SN 的报文层、topic id、休眠语义与 MQTT 差异太大，
硬塞进同一个 FSM 会让「MQTT 语义只有一份」变成「两套语义互相污染」。本 app 的会话进程
只复用**存储/订阅/路由**这层内核（设计 §3 的依赖方向即如此）。

### 决定 4：topic id 三张表，预定义全局只读、动态按客户端

| 表 | 归属 | 说明 |
|---|---|---|
| 预定义（predefined） | app env `predefined_topics`，启动时校验 | `1..0xFFFD`，`0x0000`/`0xFFFE`/`0xFFFF` 非法；只读，不随会话消失 |
| 动态（normal） | 每会话内存 map，双向 | 网关收到 PUBLISH/SUBSCRIBE 的 topic name 后分配 id 并回 REGISTER/SUBACK；下行未知 topic 先发 REGISTER 再发 PUBLISH |
| 短 topic（short） | 每会话内存 map，**不分配 id** | 2 字符 topic name 直接进 PUBLISH/SUBSCRIBE 的 TopicId 字段 |

上限 `max_topic_ids_per_client`（默认 1024）。超限：上行 `PUBACK/REGACK` 用
`0x03 not supported`；下行是网关自己的分配，超限即断开会话（不可能为它再编码出合法报文）。

### 决定 5：QoS -1 不进会话状态

QoS -1 的 PUBLISH 只做一次路由/投递，不分配 packet id、不重传、不进 in-flight；
订阅端收到 QoS -1 投递时也不回任何 ACK。它**不写会话持久状态**，因此崩溃后不会重放。

### 决定 6：休眠不新建缓冲

`DISCONNECT(duration > 0)` → 会话进入 `asleep`：**detach**（停止接收 `broker_deliver`）、
保留订阅与 cursor、记 `asleep_until`/`duration`。消息留在分区里，唤醒后按 cursor 追赶。

- QoS0 在休眠期间丢弃（QoS0 本就不进存储，语义一致，写进能力矩阵）。
- 唤醒：同端点发 `PINGREQ` → `PINGRESP` + `attach` + 从 cursor 拉取。
- `duration = 0` 的 DISCONNECT 是正常断开（清 clean session 的会话状态，发布 Will 抑制）。
- 休眠登记数超 `max_asleep_clients`：进入休眠的 DISCONNECT 仍回 `DISCONNECT`（协议无拒绝报文），
  但会话**不进入休眠**、直接按正常断开处理并计数——不静默假装睡着。

### 决定 7：认证边界显式失败

MQTT-SN CONNECT 没有凭据字段。`broker_security.require_auth` 里出现 `mqtt_sn`（或 `<<"mqtt-sn">>`）
时，`broker_proto_mqtt_sn` 的 supervisor **拒绝启动监听器**，日志与返回值都是
`{require_auth_unsupported, mqtt_sn}`；配置了却不支持时必须响亮失败，不能静默忽略。
SN 会话一律按 `broker_security:default_tenant/0` 的匿名 principal 运行，ACL 照常生效。

### 决定 8：定时器只保留两个

每会话只有两个 timer：`keepalive`（`1.5 × duration` 无流量 → 发 PINGREQ；`2 ×` 未响应 → 判死）
与 `sleep`（`asleep` 到期 → 直接 detach 并回收会话）。重传（QoS1/2）**不建定时器**：
重传由客户端发起（DUP），网关只在收到重复 packet id 时幂等应答，这与 MQTT 侧
「broker 不主动重传」的既有取舍一致。

---

## Task 1 — `mqtt_sn_codec` + `mqtt_sn_topics`（纯函数 + 两个套件）

**1a. `mqtt_sn_codec`**

报文用 map 表示（`#{type := atom(), ...}`），`decode/1` 返回 `{ok, Packet}` 或
`{error, Reason}`；`encode/1` 返回 iodata。**严格解码**：

- 长度头：首字节 `0x01` → 3 字节长度（大端 16 位），否则 1 字节长度；长度 `< 2` 或
  `> 65535` 拒绝；报文实际字节数与长度字段不符 → `{error, {length_mismatch, Len, Actual}}`。
- 未知 type → `{error, {unknown_type, T}}`；保留位非零 → `{error, {reserved_bits, _}}`；
  字段不足 → `{error, truncated}`；负载超长（如 REGISTER topic 名）→ `{error, too_long}`。
- 覆盖：ADVERTISE(0x00)、SEARCHGW(0x01)、GWINFO(0x02)、CONNECT(0x04)、CONNACK(0x05)、
  WILLTOPICREQ(0x06)、WILLTOPIC(0x07)、WILLMSGREQ(0x08)、WILLMSG(0x09)、REGISTER(0x0A)、
  REGACK(0x0B)、PUBLISH(0x0C)、PUBACK(0x0D)、PUBCOMP(0x0E)、PUBREC(0x0F)、PUBREL(0x10)、
  SUBSCRIBE(0x12)、SUBACK(0x13)、UNSUBSCRIBE(0x14)、UNSUBACK(0x15)、PINGREQ(0x16)、
  PINGRESP(0x17)、DISCONNECT(0x18)、WILLTOPICUPD(0x1A)、WILLTOPICRESP(0x1B)、
  WILLMSGUPD(0x1C)、WILLMSGRESP(0x1D)。

**1b. `mqtt_sn_topics`**

纯函数 + 两个 ETS 表（预定义只读表在 app 启动时建；动态表按会话进程私有，用 map 即可——
因此本模块只提供**校验与分配逻辑**，不持有全局 ETS）：

- `predefined/0`：app env 的 map，启动校验 id 范围与 topic 名非空。
- `validate_topic_id/1`：`0x0000`/`0xFFFE`/`0xFFFF` 非法。
- `assign/3`（`Map`, `Topic`, `Max`）：已存在则返回原 id；否则取最小可用 id（`1..Max`）。
- `short/1`：topic 名长度 `1..2` 时返回 `{ok, Bin}`，否则 `error`。

**1c. 套件**

`apps/broker_proto_mqtt_sn/test/mqtt_sn_codec_SUITE.erl`、`mqtt_sn_topics_SUITE.erl`：
全报文集 round-trip（encode→decode 恒等）、两种长度头、每个拒绝路径、边界 id、分配上限、
短 topic 边界。

**1d. 验证**

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
rm -rf _build/test/extras
rebar3 ct --suite=apps/broker_proto_mqtt_sn/test/mqtt_sn_codec_SUITE.erl
rebar3 ct --suite=apps/broker_proto_mqtt_sn/test/mqtt_sn_topics_SUITE.erl
```

**1e. 提交**：`feat(s8-c): MQTT-SN 1.2 codec and topic-id table`

---

## Task 2 — app 骨架 + `mqtt_sn_listener` + `mqtt_sn_conn`（协议主体）

**2a. 新建 app**

```
apps/broker_proto_mqtt_sn/
├── src/broker_proto_mqtt_sn.app.src
├── src/broker_proto_mqtt_sn_app.erl
├── src/broker_proto_mqtt_sn_sup.erl     # listener + simple_one_for_one 会话
├── src/mqtt_sn_listener.erl             # gen_udp、端点表、SEARCHGW/GWINFO/ADVERTISE
├── src/mqtt_sn_conn.erl                 # 每客户端会话（连接态、QoS、will、休眠、keepalive）
├── src/mqtt_sn_codec.erl
├── src/mqtt_sn_topics.erl
└── test/mqtt_sn_conn_SUITE.erl
```

`broker_proto_mqtt_sn.app.src` 依赖：`kernel, stdlib, sasl, broker_core, broker_metrics,
broker_storage, broker_router, broker_queue, broker_subscription, broker_event,
broker_security, broker_retain`（**不含** `broker_proto_mqtt`、`ranch`）。

**2b. `mqtt_sn_listener`**

- `gen_udp:open(Port, [binary, {active, N}, {reuseaddr, true}])`；端口 `0` 时回读真实端口。
- 收到 `{udp, S, IP, Port, Bin}`：解码失败 → `metrics:inc(mqtt_sn_bad_packets)` 并丢弃
  （UDP 无连接，不能回错误帧）；`SEARCHGW` → 直接回 `GWINFO`；其余按端点查会话，
  无会话且报文不是 CONNECT/WILLTOPIC/WILLMSG → 丢弃并计数。
- ADVERTISE：`advertise_interval_ms > 0` 时周期广播；`0` 只应答 SEARCHGW。
- 会话退出时清理端点表与 client_id 表（`monitor`）。

**2c. `mqtt_sn_conn`**

状态：`client_id`、`endpoint`、`socket`、`tenant`、`subs`、`topics`（动态 id map）、
`pending`（下行 QoS1/2）、`qos2`（上下行 PUBREL 状态）、`will`、`duration`、`asleep`、
`last_seen`。

报文处理（全部走 `mqtt_sn_codec`）：

| 报文 | 行为 |
|---|---|
| CONNECT | 无 Will → 直接 CONNACK；`will_flag=1` → WILLTOPICREQ → WILLTOPIC → WILLMSGREQ → WILLMSG → CONNACK；`attach/3` + takeover |
| PUBLISH（QoS0/1/2/-1） | `broker_event:apply_policy/1` → `broker_router:route/3` → 逐分区 `broker_queue:append/3`（`fsync`）→ `dispatch/2` → 按 QoS 回 PUBACK/PUBREC；重复 packet id 幂等 |
| PUBREL / PUBCOMP | 推进 QoS2 状态、回 PUBCOMP / 清理 |
| SUBSCRIBE | 预定义/短/normal（可能带 REGISTER）；`broker_subscription:subscribe/4` → SUBACK（含分配的 topic id） |
| UNSUBSCRIBE | `unsubscribe/2` → UNSUBACK |
| REGISTER | 客户端上行注册 topic name → 分配/复用 id → REGACK |
| PINGREQ | 刷新 `last_seen`；asleep → 唤醒（attach + 追赶）后 PINGRESP |
| DISCONNECT | `duration=0` → 正常断开（抑制 Will）；`>0` → 休眠（决定 6） |
| WILLTOPICUPD / WILLMSGUPD | 更新 Will 并回 `*RESP` |
| ADVERTISE / SEARCHGW / GWINFO | 客户端不应发送；收到即丢弃并计数（网关角色） |

下行：`{broker_deliver, ...}` → QoS = `min(MaxQoS, granted)` → topic id 不存在则先 `REGISTER` →
`PUBLISH`；QoS1/2 记 `pending`，收到 PUBACK/PUBCOMP 后 `broker_subscription:ack/4` 并推进 cursor；
QoS0 立即 ack。

**2d. 套件 `mqtt_sn_conn_SUITE`（真实 UDP）**

`gen_udp` 客户端按字节收发。用例：CONNECT/CONNACK、clean session、QoS0/1/2 上行、
QoS0/1/2 下行、UNSUBSCRIBE、REGISTER 双向、预定义 topic id、短 topic、retain、
Will（异常端点消失）、账号 takeover、休眠 + 唤醒追赶、QoS -1、keepalive 判死、
`require_auth` 拒绝启动、SEARCHGW→GWINFO、非法报文丢弃。

**2e. 提交**：`feat(s8-c): MQTT-SN 1.2 gateway (broker_proto_mqtt_sn)`

---

## Task 3 — 真实客户端 Gate 证据（`mqtt-sn-tools` 0.0.7）

`test/protocol-compatibility/mqtt_sn/`（boot 助手 + 驱动 + README，`run/`/`data/` gitignore）：

| 用例 | 客户端 | 说明 |
|---|---|---|
| `sn-connect` | `mqtt-sn-pub`/`mqtt-sn-sub` | UDP CONNECT/CONNACK |
| `sn-qos0` / `sn-qos1` | 同上 | 上下行 QoS0/1 |
| `sn-predefined-topic` | `-t` 预定义 id | 服务端 `predefined_topics` 与客户端 id 对齐 |
| `sn-short-topic` | 2 字符 topic | 短 topic 路径 |
| `sn-register` | 客户端 REGISTER | 动态注册后发布 |
| `sn-retain` | `-r` | retained 投递 |
| `sn-will` | `-w` | 异常退出后 Will |
| `sn-sleep` | `-e` | 休眠 + 唤醒 |
| `sn-qos-minus1` | QoS -1 | 只发一次、无 ACK |

**明确标注**：`mqtt-sn-tools` **不支持 QoS 2**，QoS2 只有仓内套件覆盖（Gate 文本如实写）。
另两项同样只有仓内覆盖：工具**无 Will 选项**（不发 WILLTOPIC/WILLMSG），且它的 `PINGREQ` 是
不带 client id 的两字节报文，**无法按名唤醒**休眠会话——所以 Task 3 只能覆盖休眠**请求**这一半。
实际用例集合与理由见「交付记录」表。
QoS -1 是否受支持以工具实际行为为准，不支持则该行从第三方矩阵移出、仅列仓内证据。

---

## Task 4 — 全量验证与文档同步

`rebar3 ct`（基线 722 + 84 = 806）、`dialyzer`、`xref`、`rebar3 as prod release`；
同步 `progress.md`、`docs/architecture.md`（app 列表/表格/§3.11）、`docs/operations.md`
（UDP 1884 + 配置块 + `predefined_topics`）、`docs/protocol-compatibility.md` §4.11、
`docs/security.md` §2.2（`mqtt_sn` 的 require_auth 行为）。

---

## 交付记录（2026-11-10 结案）

提交：`bf44d1f`（Task 1）、`41c8dea`（Task 2）、`e45969c`（Task 3）、文档同步提交紧随其后。

与计划的偏差，逐条记录：

| 计划处 | 实际 | 原因 |
|---|---|---|
| Task 2a 模块清单只列 6 个 src | 多出 `mqtt_sn_conn_sup.erl`、`mqtt_sn_filter.erl` | 前者：`simple_one_for_one` 会话监督器（listener 按名引用，绝不持有 pid）；后者：`broker_proto_mqtt_sn` 不得依赖 `broker_proto_mqtt`，因此 filter 校验（SUBSCRIBE 通配符合法性、retain 重放匹配）在本 app 内重写 20 行 |
| Task 2b 只写 `advertise_interval_ms` | 另加 `advertise_address`（默认 `{255,255,255,255}`）与 `advertise_port`（默认 1884） | ADVERTISE 的目的地是**客户端监听端口**，不是本 listener 端口；单机部署/CT 必须能把它指到 `127.0.0.1` |
| Task 2b 写 `metrics:inc(mqtt_sn_bad_packets)` | 未新增 bad-packet key（`broker_metrics` 的 key 集是封闭集），坏数据报直接丢弃；新增的是 `mqtt_sn_sleep_denied`、`mqtt_sn_keepalive_timeout`、`mqtt_sn_asleep` | 不为了一个诊断计数扩大封闭集；三项都是运维真正会看的（见 `docs/operations.md` §1.1） |
| Task 2c SUBSCRIBE 只写「可能带 REGISTER」 | 额外约束：short topic 必须**恰好 2 字节**，否则拒绝而非存成客户端永远叫不出名字的 filter | 1.2 §5.4.16 语义 |
| Task 2c 未写 QoS −1 无会话时的处理 | 无会话的 QoS −1 PUBLISH 在 **listener 进程**里就地 live push（仅 predefined/short topic 可用，不落盘、不建会话） | 1.2 明确 QoS −1 可用于「未连接」的传感器；拒绝它就等于漏实现一个必选能力 |
| Task 3 用例表列 `sn-will`、`sn-sleep` | 实际没有这两条；改为 `sn-register-downlink`、`sn-sleep-disconnect`，最终 **10 例** | `mqtt-sn-tools` 0.0.7 **无 Will 选项**；其 `PINGREQ` 不带 client id，无法按名唤醒休眠会话，所以只能覆盖休眠**请求**侧。网关下行 REGISTER 是只有网关能驱动的一半，必须单独覆盖 |
| Task 3 写「QoS −1 是否受支持以工具实际行为为准」 | 支持（`-q -1`），已列入第三方矩阵 | 实测通过 |

## 完成判据（Definition of Done）

- [x] `broker_proto_mqtt_sn` 监听 UDP 1884（CT 用 `port => 0`），一个 socket 一个 reader
- [x] `mqtt_sn_codec` 覆盖 1.2 全报文（27 种），两种长度头，拒绝路径明确（40 例）
- [x] `mqtt_sn_conn` 覆盖 CONNECT/Will 流程、QoS -1/0/1/2、retain、REGISTER/短/预定义 topic、休眠与唤醒、keepalive 判死（23 例；topic id 表另有 21 例）
- [x] `require_auth` 配 `mqtt_sn` 时监听器拒绝启动（`{require_auth_unsupported, mqtt_sn}`，`require_auth_refuses_to_start`）
- [x] `mqtt-sn-tools` 真实客户端 10/10 全绿；QoS2 / Will / 唤醒三项无第三方覆盖一事在 `test/protocol-compatibility/mqtt_sn/README.md` 与 `docs/protocol-compatibility.md` §4.11 单列
- [x] 全量 `rebar3 ct` 零 FAILED（**806/806**，50 套件），用例数按 `all/0` 回填（40/21/23）
- [x] dialyzer 干净 / xref 173 条仅基线同类 unused-export / `rebar3 as prod release` 含 `broker_proto_mqtt_sn-0.1.0`
- [x] 四份文档同步（`progress.md`、`docs/architecture.md` §3.11、`docs/operations.md` §1/§1.1、`docs/protocol-compatibility.md` §4.11、`docs/security.md` §2.2/§12），`docs/roadmap.md`/ADR 统一留到 B/C/D 全部结案

## 不在本阶段（明确留给后续）

| 项 | 阶段 |
|---|---|
| MQTT-SN over DTLS | 后续待办（设计 §1.2） |
| MQTT-SN 串口桥 / 转发到外部 Broker | 不做（设计 §1.2） |
| `broker_proto_mqtt_quic` | **S8-D** |
