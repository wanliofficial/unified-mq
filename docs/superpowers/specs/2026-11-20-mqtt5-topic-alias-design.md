# MQTT 5.0 Topic Alias 设计与实现计划

| 项 | 内容 |
|---|---|
| ID | MQ-04 |
| 协议 | MQTT 5.0（明文 1883 / WS / QUIC 共用 `mqtt_conn_fsm`） |
| 关联 | [ADR-017](../adr/017-mqtt5-subset.md)（MQTT 5 子集）；[MQ-04 缺口](../compatibility/unifiedmq-mqtt-family-gap.md#L55)；MQTT 5.0 §3.3.2.1.2 / §3.3.2.3.2 / §3.3.2.3.4 |
| 日期 | 2026-11-20 |
| 状态 | 待批准 |

## 1. 目标

让合规的 MQTT 5.0 客户端能用 Topic Alias 收发 PUBLISH：服务端在 CONNACK 里宣布
`Topic Alias Maximum`，入站 PUBLISH 携带 `Topic Alias`（无 topic 名或空 topic 名）时按
本连接维护的别名表还原真实 topic，出站 PUBLISH 按规则为频繁 topic 分配别名并写进属性。
目标是消除重复 topic 名的线上字节，不改变路由 / 订阅 / retain 语义。

## 2. 现状（已核实，非猜测）

- `mqtt_frame` 已能编解码 `topic_alias`（入站属性 `prop(<<16#23, V:16, ...>>)`，出站
  `prop_bin({topic_alias, V})`）和 `topic_alias_maximum`（`0x22`）。**codec 层已就绪**。
- `mqtt_conn_fsm:handle_publish/2`（v5 路径，`:740-757`）目前对**任何**携带 `topic_alias`
  的入站 PUBLISH 直接 `protocol_error(16#94, "topic_alias on a connection without
  topic_alias_maximum")`——因为 broker 从不宣布 maximum，所以判定「客户端不该用别名」。
- `connack_bin/3`（`:2423`）构造 CONNACK 时 properties 恒为 `#{}``，**从不发**
  `topic_alias_maximum`，也不发 Server Keep Alive / Assigned Client ID / Maximum QoS /
  Retain·Wildcard·Shared·Subscription Identifier Available（后四者属 MQ-07，本 spec 不涉及）。
- 发布路径 `handle_publish_allowed/7`（`:816`）以真实 `Topic` 调
  `broker_router_hash:route/3`；别名只存在于 v5 PUBLISH 的属性字节里，`#broker_message`
  的 `destination` 永远是真实 topic。**别名解析必须在连接层完成、不污染内核**。
- 会话 blob（`blob/1` `:2466`，`restore_blob/1` `:678`）持久化 `subs`/`sub_ids`/`sub_flags`，
  **尚无别名表**——而 §3.3.2.3.2 要求别名映射随会话跨重连保留。

## 3. 设计

### 3.1 配置与宣布

新增 env `mqtt_topic_alias_maximum`（默认 **0** = 关闭，保持当前行为）。CONNACK（仅 v5）
当 `max > 0` 时带 `topic_alias_maximum => Max`；`max = 0` 时维持现状（不发该属性，入站
别名继续被 0x94 拒绝）。默认值选 0 是因为 Topic Alias 是**可选**特性，且默认关闭可保证
零回归、不破坏现有第三方客户端证据（paho/mosquitto/emqtt 都不依赖别名）。

### 3.2 连接状态

`#st{}` 新增两个字段：

```erlang
topic_alias_max = 0 :: non_neg_integer(),      %% 本连接宣布的上限（CONNACK 用的值）
alias_in  = #{} :: #{pos_integer() => binary()},        %% 入站：alias -> topic（客户端定义）
alias_out = #{} :: #{binary() => pos_integer()},        %% 出站：topic -> alias（broker 分配）
next_alias_out = 1 :: 1..65535                          %% 出站别名分配游标（持久化）
```

别名值域 1..65535（0 非法，§3.3.2.3.2）。`alias_in` 由客户端定义、`alias_out` 由 broker 分配，
两者方向相反、互不复用编号空间。

### 3.3 入站解析（§3.3.2.3.2）

`handle_publish/2` 的 v5 分支改为：

1. 读 `Alias = maps:get(topic_alias, Props, undefined)`。
2. 若 `topic_alias_max =:= 0` 且 `Alias =/= undefined` → 维持现状 `protocol_error(16#94)`。
3. 否则进入解析：
   - **topic 名非空**（`byte_size(Topic) > 0`）且 `Alias =/= undefined`：
     客户端同时给了 topic 名和别名 → 用别名登记/覆盖 `alias_in[Alias] = Topic`
     （§3.3.2.3.2：服务器必须用收到的别名更新映射），用真实 `Topic` 派发。
   - **topic 名为空**（`<<>>`）且 `Alias =/= undefined`：
     查 `alias_in[Alias]`；命中则用其 topic 派发，未命中 → `protocol_error(16#94, ...)`
     （客户端引用了服务器从未登记的别名）。
   - **topic 名非空**且 `Alias =:= undefined`：普通发布，用真实 `Topic` 派发（现状）。
   - **topic 名为空**且 `Alias =:= undefined`：`protocol_error(16#91, ...)`（Packet payload
     invalid——空 topic 无别名无法还原；§3.3.2.2 要求非空，这是既有的非法形状，给出明确码）。

解析出的真实 `Topic` 继续走 `handle_publish_in/7` 既有管线（背压 / QoS2 去重 / ACL / 路由 /
存储 / dispatch），别名不进 `#broker_message`。

### 3.4 出站生成（§3.3.3.1）

出站 PUBLISH（`publish_bin/6` `:2304` 与 will 路径）在 v5 且 `topic_alias_max > 0` 时：

- 查 `alias_out[Topic]`：命中 → props 加 `topic_alias => Alias`，**topic 名仍照常写入**
  （MQTT 5 允许 topic 名与别名并存；§3.3.4 说服务器「可以」发别名，与 mosquitto/EMQX 行为一致）。
- 未命中且 `next_alias_out =< topic_alias_max` → 分配 `Alias = next_alias_out`，
  `next_alias_out+1`，登记 `alias_out[Topic] = Alias`，props 加 `topic_alias => Alias`。
- 未命中且已达上限 → 不发别名，仅写 topic 名（§3.3.3.1：别名用尽时只发 topic）。

出站别名一经分配**不因后续订阅变化而回收**（与 mosquitto 默认一致，简单且有界：上限即
`topic_alias_max`）。空 topic（retained 重放的 wildcard 展开不会发生，订阅侧 topic 恒具体）
不分配别名。

### 3.5 持久化

`blob/1` 增 `aliases => {alias_in_list, alias_out_list, next_alias_out}`；
`restore_blob/1`（vsn 升至 4，旧 blob 读为 `#{}` 即空表 + `next_alias_out=1`）。
仅 v5 且 `topic_alias_max > 0` 的连接才写入，3.1.1 / v5 关闭态 blob 形状不变（与
`subs_blob` 同样的「vsn 门控」）。

### 3.6 will / retain 不受影响

- Will 的 topic 是 broker 内部存储的真实 topic，不经别名层（别名是「本连接PUBLISH」的
  线上优化，will topic 不出现在该连接的 PUBLISH 属性里）。
- Retain 重放走 `deliver`/`publish_bin`，会经过出站别名生成（§3.4），符合「出站 PUBLISH
  可带别名」——但 retained 消息的 topic 通常只出现一次，多半不命中 `alias_out`，仅发
  topic 名，无副作用。

## 4. 验收（测试先行，TDD）

在 `apps/broker_proto_mqtt/test/mqtt_conn_SUITE.erl` 增（全部 `port => 0`）：

1. `v5_connack_announces_topic_alias_maximum`：设 `topic_alias_maximum=10`，CONNACK 带
   `topic_alias_maximum => 10`；未配置时 CONNACK 不含该属性。
2. `v5_publish_with_topic_name_and_alias_registers_it`：入站带 topic 名 + `topic_alias=1`，
   订阅方用真实 topic 名收到消息（别名被登记、不影响投递）。
3. `v5_publish_with_empty_topic_and_known_alias`：先登记 alias 1，再发空 topic +
   `topic_alias=1`，订阅方用真实 topic 收到。
4. `v5_publish_with_empty_topic_and_unknown_alias_is_protocol_error`：发空 topic +
   `topic_alias=99`（未登记）→ 连接以 0x94 断开。
5. `v5_publish_with_empty_topic_and_no_alias_is_protocol_error`：空 topic 无别名 → 0x91 断开。
6. `v5_publish_alias_survives_reconnect`：登记 alias→topic，断开、clean=0 重连，再发空
   topic + 该 alias 仍能还原（验证 blob 持久化 `alias_in`）。
7. `v5_outbound_publish_uses_allocated_alias`：broker 给某 topic 分配别名，订阅方收到的
   PUBLISH 带 `topic_alias`（验证 `alias_out` 生效；用 paho 风格的裸帧或直读字节确认属性）。
8. `v5_topic_alias_zero_still_rejects`：`topic_alias_maximum=0`（默认）时入站带别名仍 0x94
   （零回归，证明默认关闭）。

`mqtt_frame_SUITE` 已有 topic_alias / topic_alias_maximum 往返，无需新增（codec 层就绪）。

## 5. 非目标

- 不实现别名回收 / LRU 回收（别名用尽即停发，简单有界）。
- 不动 MQ-07 的 CONNACK 能力回填（Server Keep Alive / Assigned Client ID / Maximum QoS /
  Retain·Wildcard·Shared·SubId Available）——那是 CONNACK 的另一组属性，单独一个特性。
- 不改 3.1.1（无别名概念，属 MQ-16 关注点）。
- 不为别名引入新 ADR（属 ADR-017 MQTT 5 子集内的能力补齐，沿用 ADR-017）。

## 6. 影响面

- `mqtt_conn_fsm.erl`：`#st{}` 加 4 字段、`handle_publish/2` 改解析、`publish_bin/6` 改生成、
  `connack_bin/3` 接 maximum、`blob/1`/`restore_blob/1` 持久化、`handle_connect` 把
  `topic_alias_maximum` 写进 state。
- `config/sys.config.src`：新增 `mqtt_topic_alias_maximum` 默认 0。
- `docs/protocol-compatibility.md` §4.11 MQTT 5 行：Topic Alias 由「明确未做」改「已支持」。
- `docs/compatibility/unifiedmq-mqtt-family-gap.md` MQ-04 行：改「已支持」。
- 无新 app、无新端口、无新依赖、无新指标键（`broker_metrics` 键集不变）。

## 7. 验证基线

- compile 0 warning、dialyzer 0 warning、xref 无新增。
- 全量 `rebar3 ct` 在 1579 基线上 +8（上述 8 例）→ 1587，零回归。
- 合到 main 后复跑同基线。
