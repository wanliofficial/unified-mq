# 吊销实时驱逐活动会话（按 principal）设计

| 项 | 内容 |
|---|---|
| 任务 | MQ-17 产品级清单 `#10`「吊销可驱逐活动会话」（P0） |
| 关联 | [MQ-17 缺口](../compatibility/unifiedmq-mqtt-family-gap.md)（MQ-17 行）；[feature-comparison §4.2 #10](../mqtt-feature-comparison.md)；ADR-016（metrics 冻结键集）、ADR-018 §18.1（DISCONNECT 抑制 Will）、ADR-032 D7（admission 模板） |
| 日期 | 2026-11-22 |
| 状态 | 已批准 |

## 1. 目标

让运维（或自动策略）能**吊销一个 principal（用户名）**：该用户当前所有活动连接被立即驱逐，且其后的重连/新连接被拒绝，直到吊销被显式清除或（可选）TTL 到期。

- **粒度**：按 principal（username binary）。一个用户可能有多设备 / 多 `client_id` 连接，全部命中。
- **TTL**：可选，默认**永久**直到手动清除；带 TTL 时到点自动解除，重连恢复允许。
- **驱逐机制**：broker 主动发 **DISCONNECT**（v5 带未授权原因码；3.1.1 / 3.1 发普通 DISCONNECT）再关闭连接。DISCONNECT 是正常关闭路径，**抑制 Will 发布**（ADR-018 §18.1），客户端也能明确感知「被拒」而非网络抖动。

## 2. 现状（已核实，非猜测）

- `broker_security` 当前**无运行时可变状态**：`authenticate/2`、`authenticate_conn/1`、`authorize/2`、`tenant_id/1` 全部同步、从 app env 读配置（`broker_security.erl:60-91`）。principal = 认证解析出的 username binary（`:34`）；匿名 CONNECT 时 principal 退化为 `client_id`（`mqtt_conn_fsm.erl:726-729`）。
- **已有模板** `broker_admission`（`broker_security`）：ETS-backed 的**纯模块**（无 gen_server），表由 `broker_security_app:start/2` 在 sup **之前** `init/0` 创建（`broker_security_app.erl:17`）。文档明确为何用 persistent_term + ETS 而非每次 `gen_server:call`：R-LB5 禁止无界阻塞，CONNECT 热路径不能同步阻塞到单一进程（`broker_admission.erl:25-29`）。吊销注册表照此模式。
- **CONNECT 认证路径**：`mqtt_conn_fsm:authenticate/2`（`mqtt_conn_fsm.erl:722`）解析 `Principal` 后、调 `authorize/2` 前，是插入「已吊销则拒绝」检查的天然位置；返回 `{error, 4, D}` 经 `refuse/2`（`:2914`）发 CONNACK rc 4（v5 映射 0x86 Not authorized，见 `v5_refuse/1` `:2941`）。
- **连接成功挂钩点**：`connect_success/2`（`mqtt_conn_fsm.erl:623`）是 principal 已定、会话已建立的尾部，注册 `principal → self()` 在此。
- **驱逐复用现成机制**：`event(#disconnect{}, connected, D)`（`mqtt_conn_fsm.erl:354-359`）确立了正常关闭如何**抑制 Will**（置 `will = undefined` + `mqtt_will_server` 撤 parked Will，`publish_will(mqtt_disconnect, _)` 返回 `ok`）。`protocol_error/3`（`:2922`）是「发 `#disconnect{reason_code=RC}` 再 stop」的形状但**不**清本地 Will——驱逐不能照搬它（见 §3.3）。
- **admin HTTP**：`broker_admin_http:handle/4`（`:37`）→ `authorize/2` 门控（`:39-43`）→ `route/4`（`:41`）显式 `case Method/Path` 子句返回 `json(Status, Map)`。加 `POST /api/v1/revoke` + 清除端点直接。
- 连接进程按 `client_id` 注册在 `broker_subscription:attach/3`（takeover 用它找旧 pid，`mqtt_conn_fsm.erl:867`），**无按 principal 枚举连接的注册表**——需新增 `principal → [pid]` 索引。

## 3. 设计

采用 **Approach A：persistent_term 镜像 + 双 ETS 表**（经设计评审选定，详见评审结论：仿 `broker_admission`，CONNECT 热路径无消息往返）。

### 3.1 `broker_revocation`（新模块，`broker_security`）— 注册表

ETS-backed，仿 `broker_admission`。两张 `public, set, {read_concurrency, true}` 表，由 `init/0` 在 sup 前创建并发布其中 principal 表的 persistent_term 镜像：

- `broker_revocation_principals`：`Principal => ExpiryPosIntMs | permanent` — CONNECT 时 `revoked/1` 的**无锁**查表来源。
- `broker_revocation_sessions`：`{Principal, Pid} => true` — 活动连接的 principal 索引，供驱逐查表。

API：

- `init() -> ok` — 建两张表 + 发布 principal 表 id 到 persistent_term；由 `broker_security_app:start/2` 在 sup 前调用（紧跟 `broker_admission:init/0`）。
- `revoke(Principal, TTL) -> ok` — TTL 为 `undefined` 写 `permanent`，否则写到期毫秒（`erlang:system_time(millisecond) + TTL_ms`）；随后对该 principal 的所有活动 pid 执行驱逐。
- `clear(Principal) -> ok` — 删 principal 行（不主动 kill 已驱逐会话；驱逐在 revoke 时已完成）。
- `revoked(Principal) -> boolean()` — **调用方本地、无消息往返**读 ETS 镜像（容忍表不存在 → `false`，仿 `broker_admission` 的 `state/0` 容错）。
- `register_session(Principal, Pid) -> ok` / `unregister_session(Principal, Pid) -> ok` — conn FSM 在连接成功 / 退出时维护 sessions 表（幂等，`ets:take` 语义）。
- 内部 gen_server 循环：TTL 扫描（删到期 principal 行）+ 驱逐派发（对每个活动 pid 发 DISCONNECT + stop）。

> **gen_server 职责边界**：表的**读**（`revoked/1`、`register_session`/`unregister_session` 的高频写）在调用方进程本地直读 ETS，不进 gen_server；gen_server 只承担「周期性 TTL 扫描」与「revoke 触发的一次性驱逐遍历」这类低频、可延迟的活——这正是 `broker_admission` 把 O(n) 留给拒绝时刻、把热路径留在调用方的同一思路。

### 3.2 `broker_security` — 安全语汇门面

新增薄封装，把吊销纳入安全模块的对外语汇（不改既有纯函数语义）：

- `revoke(Principal, TTL) -> ok`（转发 `broker_revocation:revoke/2`）
- `clear_revocation(Principal) -> ok`
- `revoked(Principal) -> boolean()`（转发 `broker_revocation:revoked/1`）

CONNECT 热路径**直接**调 `broker_security:revoked/1`（或在 conn FSM 内调 `broker_revocation:revoked/1`，二选一，实现时定；倾向经 `broker_security` 以保持调用点只认安全模块）。

### 3.3 `mqtt_conn_fsm`（`broker_proto_mqtt`）— 三个触点

- **CONNECT 拒绝**（`authenticate/2`，`mqtt_conn_fsm.erl:736` 附近）：`Principal` 解析后、`authorize/2` 前，若 `broker_security:revoked(Principal)` → `{error, 4, D}`。返回码 4 与「坏用户名/密码」**不可区分**（`refuse/2` → v5 0x86），不泄露「该用户被吊销」vs「密码错」。匿名 CONNECT（principal = client_id）除非被显式注册，否则不会被吊销。
- **注册活动会话**（`connect_success/2`，`mqtt_conn_fsm.erl:623` 尾部）：`broker_revocation:register_session(Principal, self())`。
- **注销**（`terminate/2`，`mqtt_conn_fsm.erl:2976`）：`broker_revocation:unregister_session(...)`（principal 从 state 取）。
- **驱逐（新增内部 `evict/2`）**：复用 `event(#disconnect{}, connected, D)`（`:354-359`）确立的**正常关闭**语义，而非 `protocol_error/3` 的形状——因为 Will 抑制的关键在**本地 Will 状态清除**，不在发出的 DISCONNECT 帧：`evict/2` 依次
  1. `mqtt_will_server:cancel(ClientId)` —— 撤掉已 parked 的延迟 Will（`mqtt_will_server:cancel/1`，`:81`）；
  2. `send_frame(enc(#disconnect{reason_code = RC}), D)` —— 通知客户端（RC 同下）；
  3. `{stop, normal, D#st{will = undefined}}` —— **清掉本地立即发布 Will**（`DelayS = 0` 的那支走 `publish_will(normal, D)` 会发 Will，必须像 `:359` 一样置 `will = undefined` 才抑制）。

  RC 取 v5 `0x87`（Not authorized）/ 3.1.1+ 普通 DISCONNECT（无原因码）。**关键**：照 `:354-359` 走正常 DISCONNECT 路径才能让 `terminate(normal, ...)` 不误发 Will（ADR-018 §18.1），直接套 `protocol_error/3` 会漏掉第 1、3 步、把敏感遗嘱广播出去。

> WS / QUIC 因共用 `mqtt_conn_fsm` 自动继承本特性（同 MQTT 5 子集的继承逻辑）。

### 3.4 `broker_admin_http`（`broker_admin`）— 运维端点

经既有 `authorize/2` 门控（`:313-315`）：

- `POST /api/v1/revoke` — body `{principal, ttl_ms?}` → `broker_security:revoke/2` → 返回 `{status, evicted}`（`evicted` = 被驱逐的活动连接数）。
  - **（2026-11-22 实现记录）** 实际返回 `#{status => revoked, principal => P}`，**无**驱逐计数：`broker_revocation:evict/1` 逐 pid 发 `mqtt_evict` 后返回 `ok`，且驱逐是异步的（连接自收消息后自行关闭），此刻的计数既不可得也不代表已完成驱逐。按计划（Task 6）`status => revoked` 实现，此处以计划为准。
- `DELETE /api/v1/revoke/:principal`（或 `POST /api/v1/revoke/clear`，实现时取既有风格一致者）→ `broker_security:clear_revocation/1`。
  - **（2026-11-22 实现记录）** 取 `DELETE /api/v1/revoke/:principal`；路径段先经 `uri_string:percent_decode/1` 解码再查表（admin listener 交出的是原始请求目标，未解码会把 `user%40example.com` 当成另一个 principal）。
- 更新端点文档头（`:9-21` 区块）补两行。

## 4. 数据流

- **吊销（运维触发）**：`POST /api/v1/revoke` → `broker_security:revoke/2` → 写 principal 行（到期 / 永久）→ 查 `broker_revocation_sessions` 取该 principal 的全部活动 pid → 逐个发 DISCONNECT（抑制 Will）+ stop。此后该 principal 的新 CONNECT 被 `authenticate/2` 拒绝。
- **TTL 到期**：扫描循环删 principal 行 → 重连恢复允许。**注意**：TTL 只治理「到点后可否重连」，不对已驱逐会话做二次驱逐（它们在 revoke 时已断开）。
- **吊销期内的重连**：`authenticate/2` → `revoked/1` 为真 → CONNACK rc 4 / v5 0x86。

## 5. 错误处理与边界

- **吊销无活动会话的 principal**：principal 行照写；重连仍被拒；`evicted => 0`。
- **并发 takeover**（同 client_id 新连接驱逐旧）：旧 pid `terminate/2` 调 `unregister_session`；移除用 `ets:take` 式幂等，重复清理无害。
- **sessions 表中的死 pid**：驱逐跳过死 pid（`is_process_alive` 守卫）；扫描回收死 owner 行，表不泄漏（同 `broker_admission:prune/2` 思路）。
- **竞态 — 吊销恰好落在 CONNECT 中途**：若 CONNECT 尚未注册会话，重连拒绝（`authenticate/2`）兜住；若已注册于吊销查询之后，驱逐遍历会找到刚注册的 pid 并断开。两种排序都收敛到「最终不在线」。
- **表不存在（应用未启动 / 测试重初始化）**：`revoked/1` 等读返回 `false`（容忍 `badarg`，仿 `broker_admission` 各 helper）——读 fail-open，符合「无门控即无拒绝」的既有姿态。
- **metrics**：默认**只 log、不新增 metrics 键**——`broker_metrics` 键集是冻结的（ADR-016，由 `keys_are_stable` 强制）。除非实现期确认加键合法，否则记日志，不动冻结键集。

## 6. 测试

- **单元（`broker_revocation_SUITE`）**：吊销使 CONNECT 被拒；TTL 到期恢复；clear 恢复；驱逐只命中目标 principal 的 pid；死 pid 行被回收；表不存在时 `revoked/1` fail-open。
- **连接级（`mqtt_conn_SUITE` 新增用例）**：被吊销 principal 的 CONNECT → rc 4 / v5 0x86；活动会话被驱逐且**收到 DISCONNECT 且不发 Will**（断言订阅其 Will 主题的客户端在驱逐后收不到任何消息——覆盖 parked 延迟 Will 与本地立即 Will 两种形态）；吊销期内重连被拒；3.1 / 3.1.1 / v5 三种线格式各自正确。
- **admin（`broker_admin_http` SUITE，存在则加用例，否则新建）**：`POST /api/v1/revoke` 返回 `{status, evicted}`；未授权调用方得 401。
- **零回归**：全量 `rebar3 ct` 基线 **1628/1628**（OTP 29.1.2，CT `port => 0`，`rm -rf _build/test/extras` 后跑）；dialyzer 干净。

## 7. 文档收尾

- `docs/mqtt-feature-comparison.md` §4.2（`#10`）与任务板同步。
- `docs/compatibility/unifiedmq-mqtt-family-gap.md` MQ-17 `#10` 子项标 done。
- `app.src` 描述 / 各新模块 docstring；admin 端点头补两行。
- 历史实测记录正文不改，只追加带日期批注。

## 8. 范围外（显式）

- **集群范围吊销的扇出**（`#5`/`#6` 拥有集群复制）：本特性为**节点本地**，与 `broker_security` 其余运行时状态（admission、drain）一致。集群扇出列为后续。
- **MQTT-SN 会话吊销**：`mqtt_sn_conn` 不共用 `mqtt_conn_fsm`，列为后续单独一刀。
- **按租户吊销 / 按 client_id 吊销**：本期仅 principal 粒度。
- **非 MQTT 协议的吊销执行**（2026-11-22 实现时明确）：吊销的**拒绝重连**检查只接在 `mqtt_conn_fsm` 的两处（`authenticate/2` 的 CONNECT 拒绝、`connect_success/2` 的登记后复查）。WS / NATS / AMQP / Kafka / Pulsar / WebRTC / A2A / MCP 走 `broker_security:authenticate/2`、`authenticate_conn/1`，二者**都不查**吊销表，因此同一 principal 在这些协议上的活动连接不会被驱逐、重连也不会被拒。要让吊销全协议生效，须把检查下沉到统一认证入口（`authenticate/2` / `authenticate_conn/1`）——但那会让 `broker_security` 的认证热路径依赖注册表，属后续独立评估。
