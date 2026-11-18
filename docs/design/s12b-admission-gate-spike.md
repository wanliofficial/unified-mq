# S12-B spike：节点级准入闸门落在哪里

**日期**：2026-11-18
**状态**：spike 结论（未实现）
**上游约束**：ADR-032 D7 —— 「TCP 侧需在 ranch per-listener `max_connections` 之上补一个**节点级计数闸门**，落地位置在 S12-B spike 定」

这份稿子只回答一件事：D7 要的闸门该在哪一层、用什么计数、超限时各协议怎么显式拒绝。**结论先写在 §1，证据在 §2–§6。**所有事实都是在本仓库实测的，不是从 ranch 文档推断的。

---

## 1. 结论

### 1.1 落地位置：**conn/FSM 层（每协议「连接已建立、协议握手尚未完成」的那一刻），不是 listener 层**

理由不是偏好，而是 ranch 的 `max_connections` 在**结构上无法承载**这个闸门：

- **它在超限时不拒绝，只是让 acceptor 睡下。**`ranch_conns_sup.erl:263-276` 的 `handshake/8` 在 `CurConns2 >= MaxConns` 时把 acceptor 放进 `Sleepers` 而**仍然完成了 `controlling_process` 与 `{handshake, ...}` 投递**。实测（§2）用 `max_connections => 2` 连 4 个 TCP 连接，**4 个全部 accept 成功**，`ranch:info` 报 `active_connections => 2`。第 3/4 个连接在内核 backlog 里挂着，客户端看到的是「连上了但服务端永远不说话」。
- **listener 层拿不到跨 listener 的预算。**闸门是**节点级**的（D7 原话），而每个 `listener_spec/1` 是各 app 各自的函数、各自读 `maps:get(max_connections, Spec, 1024)`（`broker_proto_mqtt_sup.erl:41-51`、`broker_proto_ws_sup.erl` 同形态）。在那一层做「节点级」，等于让每个 listener 启动时去查一个还不知道自己会不会被启动的全局预算，且**协议握手尚未解析，无法按协议给出正确的拒绝帧**。
- **conn/FSM 层已经有「显式拒绝」的全部材料。**MQTT 的 `refuse/2`（`mqtt_conn_fsm.erl:2218`）已在用，且已经处理了 v5 与 3.1.1 的 CONNACK 差异（`refuse_code/2`、`v5_refuse/1`）；CoAP 的 `admit_session/4` 已在做 `session_cap` / `principal_cap` 两级拒绝（`coap_listener.erl:766-790`），且注释明确写着「caps are checked here (not in the acceptor) because the tables live here: a second copy of the counters would be a second truth」——这正是对「别把桩打在 listener 层」的既有裁决。
- **QUIC 的既有示范也在 listener 层之上的连接点**：`mqtt_quic_listener` 在 `{new_conn, _}` 时回 `{error, too_many_connections}`（`mqtt_quic_listener.erl:210-213`），拒绝发生在**已经有连接对象、但尚未开始服务**的时刻。TCP 侧要对齐的就是这个时刻。

**所以闸门的形状是**：在一个连接「传输层已建立、协议层尚未承诺服务」的点上做一次 `admit/0`；成功则继续握手，失败则**按协议发拒绝帧后关闭**。这对 TCP 前端是 `ranch:handshake/1` 成功后、第一个协议包处理前；对 CoAP 是 `admit_session/4`；对 QUIC 是既有的 `{new_conn, _}`。

### 1.2 计数：**不使用 `connections_current`**，需要一对独立的原子计数

这是本 spike 最重要的发现，也是**不能**直接拿已修好的 gauge 当闸门的原因。三个理由，每个都实测过：

1. **`connections_current` 的口径与「占用资源」不一致（MQTT-SN）。**`mqtt_sn_conn:enter_sleep/1`（`mqtt_sn_conn.erl:675-683`）会调 `connection_closed/1` 把计数减掉，但**进程仍然存活**（保留 `sleep_timer` 与订阅），唤醒时（`wake_and_answer/1`，`:642-646`）再加回来。所以 sleeping 会话在 gauge 里是 0，在内存里是一个活进程。拿它当预算，**会漏算所有 sleeping 会话**。
2. **`connections_current` 的计数点在协议握手完成之后，不是连接建立时（MQTT）。**`mqtt_conn_fsm:handle_connect/2` 里 `count_connection/1` 在 `:419`，即在 CONNECT 解码、认证、`attach_session`、`subscribe_partitions` **全部成功之后**才调。用一个「还没算上自己」的计数器做准入，在同一个瞬间涌入的连接会**同时看到同一个旧值**，闸门形同虚设。
3. **它是可关闭的。**`broker_metrics` 的 `enabled => false` 时 `gauge_add/2` 返回 `0`（`broker_metrics.erl:120-136`），gauge 不涨。准入**不能依赖一个可选的观测功能**——metrics 关掉时节点仍然必须限流。

**结论：准入需要自己的原子计数器，与观测用的 gauge 并存、各自记账。**这不是「两套真相」，因为两者回答不同问题：gauge 是「服务中的连接」（可被 metrics 关闭、可因 sleep 归零），准入计数是「已占用的准入名额」（必须始终精确，且必须包含 sleeping 会话）。§6 会说明为何这个区分是必要的而不是重复。

**承载位置：`broker_security` 中的一个新模块（建议 `broker_admission`）**，形态照抄同目录的 `broker_limits.erl`：

- 纯函数 + `atomics`，**无进程、无 supervisor 子项**。`broker_limits` 的先例已证明这条路径成立（`broker_limits.erl:17-19`：「the function is pure so `broker_queue` and `broker_event` can call it from the caller's process without a new process or a new dependency cycle」）。
- 计数器存在 `persistent_term`（一次性写入，不运行时改——避免 ADR-016 §16.5 的全局 GC 暂停），值是一个 `atomics` 数组。
- **预算配置进 `broker_security.limits`**（`max_connections`），与既有六个 limit 同处一张 map；`broker_limits:limits/0` 的 merge-default 形态直接复用。
- **13 个接受连接的前端全部已依赖 `broker_security`**（实测逐个核对过），所以这不是新增运行时依赖。

用 `atomics:compare_exchange/3` 做「读-判-占」的原子步骤是可行的（OTP 28 实测 `compare_exchange` 与 `add_get` 均导出），**不需要 `gen_server:call`，也就天然满足 R-LB5「禁止无限阻塞」**。

### 1.3 超限时各协议的显式拒绝

原则：**用协议自己的「服务不可用」语义**，不是无差别关 socket（那和今天的静默挂起一样不可观测）。

| 前端 | 拒绝方式 | 依据 |
|---|---|---|
| MQTT 3.1.1 | CONNACK `rc=3`（Server unavailable） | 既有 `refuse/2` + `v5_refuse/1` 已实现两套码表 |
| MQTT 5 | CONNACK reason `0x89`（Server busy） | 同上；`refuse_code/2` 按 `proto_level` 分流 |
| MQTT-SN | CONNACK `return_code = 3`（congestion）后关闭 | MQTT-SN 1.2 §5.4.4 的既有码值 |
| WS / WebRTC / MQTT-over-WS | HTTP `503 Service Unavailable` + `Retry-After`，**在 upgrade 阶段**（不发 101） | 尚未切协议，此时还是 HTTP |
| NATS / AMQP / Kafka / Pulsar | 协议自身的「server unavailable / broker not available」错误帧，随后关闭；**做不到就 close + 服务端日志** | 见下注 |
| A2A / MCP | HTTP `503`（JSON-RPC 语义上是 `-32603` 或 transport-level 503） | 它们本身就是 HTTP |
| CoAP DTLS | DTLS alert 后不建立会话（既有 `{refuse, session_cap}` 路径） | `admit_session/4` 已有 |
| CoAP 明文 UDP | **不适用**：无连接语义，无名额可占 | D7 的「CoAP 明文不计」同理 |
| MQTT over QUIC | 既有 `{error, too_many_connections}` | `mqtt_quic_listener.erl:210-213` |

**注（NATS/AMQP/Kafka/Pulsar）**：这四个协议在「连接刚建立、还没收第一个包」时，协议层尚未协商出版本，报错帧的合法性取决于客户端发来的第一包。诚实做法：**在收到第一个协议帧、且版本已确定之后立刻拒绝**，各用其错误帧；如果客户端在版本确定前就停了，就是 `close` + 一条服务端 warning。这一点必须写进能力矩阵，不能假装四个协议都能给出同等的可观测性。

### 1.4 MQTT-SN sleeping 会话**计入**预算

理由：sleeping 会话持有 `sleep_timer`、订阅关系和进程内存，它占的是**节点资源**，不是「一条 TCP 连接」。D7 要防的是节点被压垮，不是 TCP fd 被耗尽。

代价要说清楚：一个故意 sleep 大量会话的部署会先撞到闸门。这正确的解法是给 MQTT-SN 配一个**独立的 session 上限**（今天没有，实测无 `max_session*`），而不是让节点级闸门假装 sleeping 不存在。**但 S12-B 不引入这个 session 上限**——它属于协议容量策略，且一次只做一件可验证的事。

### 1.5 与 drain 的交互：**draining 时直接拒绝**

draining 的语义是「不再接受新连接」（`broker_drain.erl:8-10`）。今天它靠 `ranch:suspend_listener/1` 实现，因此：

- **ranch 前端**：drain 之后新连接在 TCP 层就被拒（listener 挂起），闸门根本不会被调用。**无需额外代码**。
- **QUIC / CoAP**：**这是本次发现的既有缺口**。它们不是 ranch listener（`gen_udp:open`/`ssl:listen`/`quicer:listen`），而 `broker_drain:ranch_refs/0`（`:160-168`）只枚举 `ranch:info()`，所以 **drain 时 QUIC 与 CoAP 仍在接受新连接**。闸门应当承担这个职责：admission 时读 `broker_drain:status()`，`draining | down` 一律拒绝。这同时把 drain 的语义补全到非 ranch 前端——**但要注意依赖方向**：`broker_security` 不能反向依赖 `broker_admin`。
  - 解法：admission 模块不调用 `broker_drain`，而是读同一个 `persistent_term` 镜像。`broker_drain` 的 key 是 `{broker_drain, state}`，admission 读它、`undefined` 视为 `up`。**这不新增依赖，因为 `persistent_term` 是 `erts` 内建的。**drain 状态成了节点级的公共事实，而不是某个 app 的私有状态。

**这条不能含糊**：如果闸门不检查 drain，那么非 ranch 前端的 drain 缺口就永远补不上；如果检查，就必须用 persistent_term 而不是函数调用，否则 app 之间会成环。

### 1.6 不做的事（边界）

- **不做排队**（D7 明令）：超限就是拒绝，等待队列长度恒为 0。admission 是 `compare_exchange` 一次尝试，失败即拒绝，**没有 retry、没有 sleep**。
- **不新增 metrics 键**：被拒连接数不落 `broker_metrics`（R-LB9 标「S12 不交付」，且键集被 `keys_are_stable` 钉死）。被拒的事实进 **log（warning）**，这是今天唯一不扩容键集的观测手段。
- **不改 `broker_security` 的认证/授权语义**：admission 发生在认证**之前**还是**之后**，是每个前端各自的决定（见 §3 的接入清单）。闸门只回答「这个节点还有名额吗」，不回答「你是谁」。
- **新建模块但不新建 app、不新增端口、不新增运行时依赖。**

---

## 2. 证据一：ranch 的 `max_connections` 不拒绝

### 2.1 裸 ranch listener 实测

`max_connections => 2`、`num_acceptors => 1`，连 4 个 TCP 连接：

```
port=35625 max=2
  conn 1: TCP ACCEPTED, reply=no_reply
  conn 2: TCP ACCEPTED, reply=no_reply
  conn 3: TCP ACCEPTED, reply=no_reply
  conn 4: TCP ACCEPTED, reply=no_reply
ranch info: #{... max_connections => 2, active_connections => 2,
              all_connections => 2,
              metrics => #{{conns_sup,1,accept} => 2,
                           {conns_sup,1,terminate} => 0},
              ...}
```

**4 个全 accept，ranch 只记 2 个。**第 3/4 个在内核 backlog 里，`{conns_sup,1,accept}` 只有 2 次。源码对应 `ranch_conns_sup.erl:258-276`：`CurConns2 >= MaxConns` 时 `Sleepers2 = [To|Sleepers]`，acceptor 只是不再被唤醒，**连接本身没有被拒**。

### 2.2 真实 MQTT 前端实测

`broker_proto_mqtt`，listener `#{port => 0, max_connections => 2, num_acceptors => 1}`：

```
mqtt ref={mqtt,0} port=34875 max_connections=2
  client 1: CONNACK rc=0
  client 2: CONNACK rc=0
  client 3: TCP ACCEPTED, no CONNACK (silent)
  client 4: TCP ACCEPTED, no CONNACK (silent)
connections_current = 2
ranch active_connections = 2
```

**第 3、4 个客户端在 TCP 层被接受，但永远收不到 CONNACK。**客户端体验是**静默挂起**（等到自己的超时才失败），运维侧在 `/metrics` 上看不到任何「拒绝」信号——`connections_current` 停在 2，看上去完全健康。

这就是 D7 要求补闸门的直接理由：**今天超限既不给客户端可观测的拒绝，也不给运维可观测的信号。**

### 2.3 除 QUIC 外没有前端做容量检查

```
grep -rn "too_many_connections" apps/*/src/*.erl
  → 仅 apps/broker_proto_mqtt_quic/src/mqtt_quic_listener.erl:210-213
```

其余前端只是把 `max_connections` 下传给 ranch（`broker_proto_*_sup.erl:listener_spec/1`），即落进 §2.1 的静默行为。

---

## 3. 证据二：谁在哪一刻知道「连接已建立」

闸门必须打在一个**已经有连接、但还没承诺服务**的点上。逐前端核对：

| 前端 | 接入点 | 已有拒绝材料 |
|---|---|---|
| `mqtt_conn_fsm` | `awaiting_connect` 收到 CONNECT 后、`handle_connect/2` 早段 | `refuse/2`、`v5_refuse/1` |
| `mqtt_sn_conn` | CONNECT 处理前 | 码表已有 `congestion` |
| `ws_conn` | `{handshake, ...}` 处理内、RFC 6455 upgrade **之前** | 尚未发 101，可回 503 |
| `webrtc_conn` | 同上（复用 `ws_conn` 的握手） | 同上 |
| `nats_conn` / `amqp_conn` / `kafka_conn` / `pulsar_conn` | 首帧版本确定后 | 各自错误帧（见 §1.3 注） |
| `a2a_conn` / `mcp_conn` | `ranch:handshake/1` 成功后、读第一个请求前 | 纯 HTTP，503 |
| `coap_listener` | `admit_session/4` | 已有 `{refuse, session_cap}` 两级 |
| `mqtt_quic_listener` | `handle_call({new_conn, _})` | 已有 `{error, too_many_connections}` |
| `mqtt_ws_conn` | 不单独接（其下是 `mqtt_conn_fsm`） | 已在 FSM 覆盖 |

**`mqtt_quic_listener` 既有的 `{error, too_many_connections}` 是 per-listener 的**（`map_size(Conns) >= Max`，`mqtt_quic_listener.erl:210-213`），不是节点级；节点级闸门要在它之前再问一次 `broker_admission`，两者并存不冲突。**`mqtt_ws_conn` 不重复接入**，与 S12-B 第一项（连接计数）的结论一致：它的下层就是 `mqtt_conn_fsm`。

---

## 4. 证据三：为什么 `connections_current` 不能当闸门

### 4.1 MQTT-SN sleep 会减计数但进程存活

```erlang
%% mqtt_sn_conn.erl:675-683
enter_sleep(St) ->
    _ = broker_subscription:detach(St#st.client_id, self()),
    ok = broker_metrics:connection_closed(St#st.counted),
    TRef = erlang:start_timer(St#st.duration * 1000, self(), sleep_expired),
    cancel_keepalive(St#st{asleep = true, counted = false, sleep_timer = TRef}).
```

gauge 减 1，**进程活着**，`sleep_timer` 还在走。唤醒时（`:642-646`）再加回来。用 gauge 做预算 = 每个 sleeping 会话白送一个名额。

### 4.2 MQTT 的计数在握手完成之后

```erlang
%% mqtt_conn_fsm.erl:405-419
case subscribe_partitions(D3) of
    {ok, D4} ->
        D5 = count_connection(D4),   %% ← 计数在这里
```

在它之前的 CONNECT 解码、认证、session attach、分区订阅都可能很慢（`subscribe_partitions` 走 `broker_queue`）。**闸门若读这个数，在并发涌入时所有连接都会看到同一个旧值**——这不是理论问题：`max_connections` 恰恰是在「大量连接同时涌入」时才有意义的，而那个时刻正是它读数最不准的时刻。

### 4.3 metrics 可关闭

```erlang
%% broker_metrics.erl:120-136
gauge_add(Key, Delta) ->
    case persistent_term:get(?PT, undefined) of
        undefined -> 0;
        #{enabled := false} -> 0;
        ...
```

`enabled => false` 时一切不涨。准入是**正确性**要求，不能依赖一个可选的观测开关。

---

## 5. 证据四：drain 对非 ranch 前端是空操作

```erlang
%% broker_drain.erl:160-168
ranch_refs() ->
    case code:ensure_loaded(ranch) of
        {module, ranch} ->
            try maps:keys(ranch:info())
            catch _:_ -> []
            end;
        _ -> []
    end.
```

`ranch:info()` 只含 ranch listener。实测确认：

- **QUIC**：`mqtt_quic_listener` 用 `quicer:listen/2`（`:111`），不是 ranch。
- **CoAP**：`coap_listener` 用 `gen_udp:open/2`（`:138`）与 `ssl:listen/2`（`:195`），不是 ranch。

因此 **drain 之后 QUIC 与 CoAP 仍然接受新连接**。这是 S12-A 引入 drain 时未覆盖的部分（S12-A 的验证只用了 ranch listener 探针），spike 在此如实记录为**既有缺口**，并给出闸门承担的方案（§1.5）。

---

## 6. 为什么不把准入计数并进 `broker_metrics`

一个自然的质疑：既然 `broker_metrics` 已有原子 gauge，为什么不直接用它，非要再写一个计数器？

因为**两者的问题不同，且必须能独立回答**：

| | 观测 gauge | 准入计数 |
|---|---|---|
| 回答的问题 | 现在服务着多少连接（给人和告警看） | 还有没有名额（给代码做决定） |
| metrics 关闭时 | 无值（`0`） | **必须有值** |
| MQTT-SN sleep | 减 1（它不再服务流量） | **不减**（进程与订阅还占着） |
| MQTT 计数时机 | 握手成功后（反映真实服务） | **连接建立时**（防止并发涌入穿透） |
| 峰值 | 有（`connections_peak`） | 不需要 |

把两者合成一个数，必然要在这四行里选一边牺牲：要么 gauge 在 sleep 时虚高（运维读数失真），要么准入在 metrics 关闭时失效（节点可被压垮），要么准入在并发涌入时穿透（闸门无意义）。**ADR-032 D7 的「不扩容键集」约束的是 `broker_metrics` 的键**，不是「节点上只允许存在一个计数器」。准入计数不落 metrics，正是为了同时满足 D7 的键集约束与正确性要求。

如果将来要观测「被拒了多少」，那是 ADR-016 的键集修订，独立走流程——D7 已把它标为 S12 不交付。

---

## 7. 落地清单（实现时照此执行，不在本 spike 内）

1. **新增 `apps/broker_security/src/broker_admission.erl`**：`persistent_term` 持有 `atomics`，导出 `try_acquire/0`（`ok | {error, over_capacity | draining}`）、`release/0`、`snapshot/0`（给测试与 `/api/v1/node` 读）。无进程、无 supervisor 子项。
2. **配置**：`broker_security.limits` 增加 `max_connections`（默认沿用 1024，与现有 per-listener 默认一致），由 `broker_limits:limits/0` 的 merge 机制提供；默认 `infinity` 时闸门恒放行（保持既有部署行为不变）。
3. **12 个前端逐点接入**（§3 的表）。13 个接受连接的 app 里，`mqtt_ws` 不单独接（其下就是 `mqtt_conn_fsm`）；`mqtt_quic` 要接 —— 它既有的 `{new_conn, _}` 检查是 **per-listener** 的（`map_size(Conns) >= Max`），不是节点级，两者并存不冲突。CoAP 明文 UDP 不接。
4. **drain 联动**：`broker_admission` 读 `{broker_drain, state}` 的 `persistent_term` 镜像，`draining | down` 拒绝。
5. **测试**：每个前端一条负控（把预算压到 1，第二条连接必须拿到**可观测的拒绝帧**，而不是静默挂起）+ 一条正控（释放后能再进）+ `broker_admission_SUITE` 单独钉「并发 `try_acquire` 不超发」与「release 幂等」。每条都做反证（去掉闸门调用，对应用例必须挂）。
6. **能力矩阵**：逐协议写明拒绝方式，以及 NATS/AMQP/Kafka/Pulsar 的「版本确定前无法给错误帧」这一真实限制。
7. **文档**：ADR-032 追加「S12-B 第二项」落地偏差节；`docs/operations.md` 写明 `max_connections` 的节点级语义与被拒只进日志。

---

## 8. 一句话总结

**闸门放在 conn/FSM 层（协议握手尚未承诺服务的那一刻），用一个独立于 `broker_metrics` 的原子计数（因为它必须在 metrics 关闭、sleep 会话、并发涌入三种情况下都准确），超限时按各协议自己的「服务不可用」语义显式拒绝，并顺带补上 drain 对非 ranch 前端（QUIC / CoAP）的空操作缺口。**
