# MQTT-SN Will Delay Interval 实现

- **对应**：`docs/compatibility/unifiedmq-mqtt-family-gap.md` MQ-02（"Will Delay 未做完"）、`docs/protocol-compatibility.md` §4.11、MQTT-SN v1.2 §5.4.4（CONNECT）/ §5.6.4（Will Delay 语义）
- **性质**：**代码交付**，把 MQTT-SN 遗嘱（Will）的延迟语义补齐：断连后等待 Will Delay 秒再发，期间重连则取消
- **范围决策（本会话已确认）**：延迟 timer 挂在 **listener** 上（conn 异常断开时上交 will + deadline，listener 在已有 1s tick 扫描到期者发布、被新 session 覆盖者取消）；不新增进程类型；Will Delay Interval 字段解析**仅在 Will 标志置位时存在**（与现有真实客户端 wire 形状一致）

---

## 1. 现状（已核实，不是猜测）

| 层 | 现状 | 证据 |
|---|---|---|
| codec 解析 | CONNECT 的 `decode_body` 解析 Flags/ProtocolId/Duration/ClientId，**不含 Will Delay Interval 字段** | `mqtt_sn_codec.erl:99-116` |
| codec 测试向量 | 现有真实 `mqtt-sn-tools` wire 形状：`<<Len,16#04,Flags,ProtoId, 0,Duration, ClientId>>` —— Duration 后**直接是 ClientId，无 Will Delay 字段**（因这些用例 Will=false） | `mqtt_sn_codec_SUITE.erl:318-324` |
| conn 发布 | `publish_will/1` 在 `terminate/2` 里**同步立即发**遗嘱（`abnormal(Reason)` 为真即发） | `mqtt_sn_conn.erl:144-148, 417-427` |
| will 状态 | `#st.will = {Topic,Payload,QoS,Retain}`（或 undefined），进程死即丢 | `mqtt_sn_conn.erl:419` |
| listener | 已持有 asleep/sleep 表、有 1s `refill` tick、outlives conn 进程；`sn_sleep`/`sn_sleep_granted` 已是 conn↔listener 异步上交的先例 | `mqtt_sn_listener.erl:256-273, 386-387` |

**关键 wire-compat 事实**：MQTT-SN v1.2 §5.4.4 —— Will Delay Interval（2 字节，秒）**仅当 Will 标志置位时**才出现在 Duration 与 ClientId 之间；Will=false 时不存在该字段（= 现有测试向量的形状）。Will Delay Interval=0 表示无延迟。**解析必须按 Will 标志条件化**，否则破坏每个 Will=false 客户端（正是上次 MQTT-SN CONNECT 解码踩过的坑）。

**核心缺口**：`publish_will` 在进程将死的 `terminate/2` 里同步发，无法 arm timer。延迟 Will 需要一个 outlive conn 的家 —— 已定 = listener。

---

## 2. 设计

### 2.1 codec：条件解析 Will Delay Interval

`mqtt_sn_codec:decode_body` 的 CONNECT 子句改为：先解 Flags/ProtocolId/Duration；再**按 `will` 位**决定是否吃 2 字节 Will Delay Interval，然后 ClientId。

```erlang
decode_body(<<?CONNECT, Flags:8, ProtocolId:8, Duration:16, ClientId/binary>>)
  when byte_size(ClientId) > 0, bit(Flags, 3) =:= false ->
    %% Will=false: no Will Delay Interval field (Duration directly precedes
    %% ClientId) -- the shape every current mqtt-sn-tools client sends. The
    %% `bit(Flags,3)=:= false' guard is REQUIRED: without it this clause would
    %% also match a Will=true frame and swallow the first 2 ClientId bytes.
    case reserved(Flags, 2#00000011) of
        true -> {error, {reserved_bits, Flags}};
        false ->
            {ok, #{type => connect, will => false,
                   clean_session => bit(Flags,2),
                   protocol_id => ProtocolId, duration => Duration,
                   will_delay => 0, client_id => ClientId}}
    end;
decode_body(<<?CONNECT, Flags:8, ProtocolId:8, Duration:16, WillDelay:16,
            ClientId/binary>>)
  when byte_size(ClientId) > 0, bit(Flags, 3) =:= true ->
    %% Will=true (v1.2 §5.4.4): the Will Delay Interval (seconds) sits between
    %% Duration and ClientId. 0 means no delay.
    case reserved(Flags, 2#00000011) of
        true -> {error, {reserved_bits, Flags}};
        false ->
            {ok, #{type => connect, will => true,
                   clean_session => bit(Flags,2),
                   protocol_id => ProtocolId, duration => Duration,
                   will_delay => WillDelay, client_id => ClientId}}
    end.
```

- `will => false` 的 map 现多一个 `will_delay => 0`（缺省），保持向后兼容（消费者用 `maps:get(will_delay, M, 0)`）。
- encode 对称：`encode_body(connect, ...)` 当 `will=true` 且 `will_delay>0` 时插入该 2 字节；否则维持现形状（Will=false 或 delay=0 都不插）：

```erlang
encode_body(connect, #{client_id := ClientId} = P) ->
    Flags = flag(bit_of(maps:get(will, P, 0)), 3)
            bor flag(bit_of(maps:get(clean_session, P, 0)), 2),
    Delay = maps:get(will_delay, P, 0),
    Body = case {maps:get(will, P, 0), Delay > 0} of
               {true, true} -> <<Delay:16>>;   %% Will=true + delay>0: insert field
               _            -> <<>>             %% else: no field (current shape)
           end,
    <<?CONNECT, Flags:8, (maps:get(protocol_id, P, 1)):8,
      (maps:get(duration, P, 0)):16, Body/binary, ClientId/binary>>;
```

encode 改动必须与 decode 对称，否则 round-trip 断；测试覆盖（验收项 4）。

> 注意：两个 `decode_body` 子句**靠 `bit(Flags,3)` 守卫区分**——第一个只匹配 Will=false（无字段），第二个只匹配 Will=true（带 2 字节 WillDelay）。顺序无关紧要，因为守卫互斥。encode 与 round-trip 测试据此固定。

### 2.2 conn：异常断开时把 will 上交 listener（而非立即发）

`publish_will/1` 的调用点 `terminate/2`：当 `will_delay > 0` 且 will 存在时，**不立即发**，改为 `gen_server:cast(listener, {sn_deferred_will, self(), Will, DeadlineMs})` 后让进程正常结束（detach）。`will_delay = 0`（或无 will）维持现状立即发。

- `DeadlineMs` = `erlang:monotonic_time(millisecond) + will_delay*1000`。但 monotonic 在 listener 进程才有意义 —— 改由 listener 收到消息时以**自己的**时钟记 `ReceivedAtMs`，deadline 存相对秒数 `will_delay`，避免跨进程时钟混用。
- conn 的 `#st{}` 需带 `will_delay`（从 CONNECT 解析，存 connect 时一并写入，见 §2.4）。

### 2.3 listener：延迟 will 表 + tick 扫描 + 重连取消

- 新表 `#st.deferred_wills = #{}`：`ClientID -> {Will, DelayS, AtMs}`（AtMs = listener 记的到达时刻）。**按 ClientID 键** —— 重连即同 ClientID，天然是取消键。
- `handle_cast({sn_deferred_will, Pid, Will, DelayS}, St)`：登记 `ClientID`（需从 Pid 取 client_id —— 改 conn 上交时直接带 ClientID，见 §2.2 修正：`{sn_deferred_will, ClientID, Will, DelayS}`），`AtMs = monotonic`。若该 ClientID 已有 pending will，覆盖（最新遗嘱为准，符合"重连后新遗嘱取代"）。
- 在已有 1s `refill` tick 里加一步 `scan_deferred_wills/1`：对每个 `ClientID -> {Will, DelayS, AtMs}`，若 `monotonic - AtMs >= DelayS*1000`：
  - 若 `ClientID` 现在有一个**活着的 session**（在 asleep/active 表里）—— 说明已重连，**取消**（不发，删表项）。
  - 否则 —— 到期且未重连，**发布** will（走与 `publish_will` 同一条 `store_publish` 路径）并删表项。
- **重连取消的另一触发点**：CONNECT 成功建立 session 时（conn 上线），`gen_server:cast(listener, {sn_cancelled_deferred_will, ClientID})`（或 conn 在 connect 里直接删 listener 表项）。放在 conn 的 CONNECT 处理里删表项最直接（重连即到）。

> 选用"conn 在 CONNECT 时删 deferred 表项"作为取消主路径（确定性、即时），tick 扫描只负责到期的"未重连者发布"。两者都按 ClientID 键，逻辑单一。

### 2.4 conn 状态带 will_delay

- `#st{}` 加字段 `will_delay = 0 :: non_neg_integer()`。
- CONNECT 处理（`on_connect`/`finish_connect` 一带，`mqtt_sn_conn.erl:275, 315, 384` 等）从解码 map `will_delay` 写入 `St#st{will_delay = ...}`。
- 重连时若新 CONNECT 无 will 或 will_delay=0，conn 在建立 session 时即向 listener 发取消（清掉旧的 pending deferred will）。

### 2.5 publish 路径复用

listener 发布到期 will 时，复用现有 `store_publish(Topic,Payload,QoS,Retain, ...)`（`publish_will` 现在调的就是它，`mqtt_sn_conn.erl:420`）。listener 不直接持有该函数的私有性 —— 提取一个 `publish_will_now/1` 或在 listener 里调用 `broker_router` 的发布路径。**确认**：`store_publish/5` 是 `mqtt_sn_conn` 内部函数。方案：把"发一个 will 三元组/四元组"提取为 `mqtt_sn_conn` 的导出 `publish_will_now({Topic,Payload,QoS,Retain})`，listener 调用它；或 listener 直接走 `broker_router:destination` 发布。**取前者**（复用 conn 已验证的发布语义、指标 `will_published`），让 listener 在到期分支调用 `mqtt_sn_conn:publish_will_now(Will)`。

---

## 3. 不做（明确边界）

| 项 | 理由 |
|---|---|
| 把 will 延迟到独立进程 | 已选 listener 持有（A），不新增进程类型 |
| Will=false 客户端的 wire 形状改动 | 维持 Duration→ClientId 直接相连（现有向量），零回归 |
| 跨节点延迟 will | MQTT-SN 前端单节点（与其它 MQTT-SN 限制一致），延迟 will 不跨节点 |
| Will 更新（WILLMSGUPD）对已登记 deferred will 的影响 | 后续增量；本设计只覆盖 CONNECT 时登记的 will |
| 精确到亚秒 | Will Delay Interval 单位是秒，tick 1s 粒度足够 |

---

## 4. 留痕（本设计不解决）

- 延迟 will 的扫描粒度是 listener 的 1s tick，故实际发布时刻在 `[deadline, deadline+1s)` 区间 —— 秒级语义内可接受，文档说明。
- 若 listener 进程重启，pending deferred will 表清空（与 asleep 表同，重启即空）—— 文档如实声明。

---

## 5. 验收

- codec 单测（`mqtt_sn_codec_SUITE`）：
  1. Will=false 的 CONNECT（现有向量）解出 `will_delay => 0`，**且 ClientId 不被误切**（零回归）；
  2. Will=true 的 CONNECT 带 WillDelay=30 → 解出 `will_delay => 30`；
  3. Will=true 且 WillDelay=0 → `will_delay => 0`；
  4. encode/decode round-trip：Will=true+delay>0 的 connect map 编码后解码相等；Will=false 编码形状不变。
- conn 单测（`mqtt_sn_conn_SUITE`）：
  5. will_delay=0 时断连仍立即发 will（现状不回归）；
  6. will_delay>0 时异常断连**不立即发** will（向 listener 上交）。
- listener/集成（`mqtt_sn_conn_SUITE` 或 `mqtt_sn_dtls_SUITE` 走真实 datagram）：
  7. will_delay=2 的异常断连，约 2s 后 will 被发布（订阅者收到）；
  8. will_delay=3 的异常断连后在窗口内**重连**同一 ClientID → will **不发布**（被取消）；
  9. 全程 dialyzer 0 warning、compile 0 warning、全量 ct 不回归（当前基线 1573）。

---

## 6. 风险

- **wire-compat（高影响）**：codec 双子句必须加守卫 `bit(Flags,3)`，否则 Will=false 帧被第二个子句/WillDelay 误吞。验收项 1、4 守护。
- **跨进程时钟**：deadline 用 listener 自己的 monotonic，不用 conn 的（§2.3）。验收项 7/8 守护。
- **取消时机**：以 conn CONNECT 时删 listener deferred 表项为主路径（确定性），避免依赖 tick 恰好扫到重连。验收项 8 守护。
- **发布路径复用**：提取 `publish_will_now/1` 为导出，listener 复用 conn 已验证的发布 + 指标，避免在 listener 重造发布语义。
