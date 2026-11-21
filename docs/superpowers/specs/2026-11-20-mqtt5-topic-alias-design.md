# MQTT 5.0 Topic Alias 设计与实现计划

| 项 | 内容 |
|---|---|
| ID | MQ-04 |
| 协议 | MQTT 5.0（明文 1883 / WS / QUIC 共用 `mqtt_conn_fsm`） |
| 关联 | [ADR-017](../adr/017-mqtt5-subset.md)（MQTT 5 子集）；[MQ-04 缺口](../compatibility/unifiedmq-mqtt-family-gap.md#L55)；MQTT 5.0 §3.3.2.1.2 / §3.3.2.3.2 / §3.3.2.3.4 |
| 日期 | 2026-11-20 |
| 状态 | 已实现（2026-11-21，按文末「修订记录」做了规范性修正） |

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
  **尚无别名表**。**注意：§3.3.2.3.4 MQTT-3.3.2-7 明确禁止别名映射跨 Network Connection 携带**（"Topic Alias mappings exist only within a Network Connection and last only for the lifetime of that Network Connection"），所以别名表**不该**进会话 blob——这一点原批准稿写反了，见文末「修订记录」。

## 3. 设计

### 3.1 配置与宣布

新增 env `mqtt_topic_alias_maximum`（默认 **0** = 关闭，保持当前行为）。CONNACK（仅 v5）
当 `max > 0` 时带 `topic_alias_maximum => Max`；`max = 0` 时维持现状（不发该属性，入站
别名继续被 0x94 拒绝）。默认值选 0 是因为 Topic Alias 是**可选**特性，且默认关闭可保证
零回归、不破坏现有第三方客户端证据（paho/mosquitto/emqtt 都不依赖别名）。

### 3.2 连接状态

`#st{}` 新增四个字段：

```erlang
topic_alias_max = 0 :: non_neg_integer(),      %% 本连接宣布给客户端的上限（CONNACK 用的值），
                                               %% 也是入站 alias 的合法上界（MQTT-3.3.2-9）
peer_alias_max = 0 :: non_neg_integer(),       %% 客户端自己在 CONNECT 里宣布的上限，是出站
                                               %% alias 的合法上界（MQTT-3.3.2-11）；0/缺省 = 不出站别名
alias_in  = #{} :: #{pos_integer() => binary()},        %% 入站：alias -> topic（客户端定义）
alias_out = #{} :: #{binary() => pos_integer()},        %% 出站：topic -> alias（broker 分配）
next_alias_out = 1 :: 1..65535                          %% 出站别名分配游标（仅本连接生命周期）
```

别名值域 1..65535（0 非法，MQTT-3.3.2-8）。`alias_in` 由客户端定义、`alias_out` 由 broker 分配，
两者方向相反、互不复用编号空间。**四个字段都只存活于本次网络连接**：`alias_in`/`alias_out`/
`next_alias_out` 是 §3.3.2.3.4 明确禁止跨连接携带的状态（MQTT-3.3.2-7），`peer_alias_max` 更是
每次连接都要重新读 CONNECT 属性，因此都不能进会话 blob。

### 3.3 入站解析（§3.3.2.3.4）

`handle_publish/2` 的 v5 分支改为（顺序很重要：先验属性本身是否合法，再看映射）：

1. 读 `Alias = maps:get(topic_alias, Props, undefined)`、`Max = topic_alias_max`。
2. 若 `Idx`（携带 `subscription_identifier`）→ 维持现状 `protocol_error(16#82)`。
3. **值域校验先于映射查找**：
   - `Alias =/= undefined` 且 `not (Alias >= 1 andalso Alias =< Max)`（含 `Max =:= 0`
     即功能关闭、以及 `Alias =:= 0`）→ `protocol_error(16#94)`
     （MQTT-3.3.2-8/-9：0 非法、超出发给客户端的上限也非法）。
4. 值域合法后再看 topic 名：
   - **topic 名非空**：若 `Alias =/= undefined`，登记/覆盖 `alias_in[Alias] = Topic`
     （§3.3.2.3.4：服务器必须用收到的别名更新映射）；用真实 `Topic` 派发。
   - **topic 名为空**（`<<>>`）且 `Alias =/= undefined`：查 `alias_in[Alias]`；命中则用其
     topic 派发，未命中 → `protocol_error(16#94, ...)`（客户端引用了本次连接内从未登记的别名）。
   - **topic 名非空**且 `Alias =:= undefined`：普通发布，用真实 `Topic` 派发（现状）。
   - **topic 名为空**且 `Alias =:= undefined`：`protocol_error(16#91, ...)`（Packet payload
     invalid——空 topic 无别名无法还原；§3.3.2.2 要求非空，这是既有的非法形状，给出明确码）。

解析出的真实 `Topic` 继续走 `handle_publish_in/7` 既有管线（背压 / QoS2 去重 / ACL / 路由 /
存储 / dispatch），别名不进 `#broker_message`。

### 3.4 出站生成（§3.3.2.3.4，MQTT-3.3.2-11）

出站 PUBLISH（`publish_bin/6`/``/7` 与 will、retained 重放路径）在 v5 且
**`peer_alias_max > 0`** 时（**不是** `topic_alias_max`——那是发给客户端的，不是客户端发来的）：

- 查 `alias_out[Topic]`：命中 → props 加 `topic_alias => Alias`，**topic 名仍照常写入**
  （MQTT 5 允许 topic 名与别名并存；§3.3.2.3.4 说服务器「可以」发别名）。
- 未命中且 `next_alias_out =< peer_alias_max` → 分配 `Alias = next_alias_out`，
  `next_alias_out+1`，登记 `alias_out[Topic] = Alias`，props 加 `topic_alias => Alias`。
- 未命中且已达 `peer_alias_max` → 不发别名，仅写 topic 名。
- `peer_alias_max =:= 0`（客户端没在 CONNECT 里给 `topic_alias_maximum`，或给了 0）→ 完全
  不出站别名，与 `topic_alias_max` 无关。

出站别名一经分配**不因后续订阅变化而回收**（简单且有界：上限即 `peer_alias_max`）。

### 3.5 持久化（原稿要求，已撤销）

**原批准稿要求**把 `alias_in`/`alias_out`/`next_alias_out` 写进 `.inflight` blob（vsn 升到 4）。
**已撤销**：§3.3.2.3.4 MQTT-3.3.2-7 明确「Topic Alias mappings exist only within a Network
Connection and last only for the lifetime of that Network Connection. A receiver MUST NOT carry
forward any Topic Alias mappings from one Network Connection to another」，跨连接携带映射是
MUST NOT 违规。因此 `blob/1` **不增任何别名键**，`restore_blob/1` 也不恢复（重连后
`alias_in`/`alias_out` 恒为空、`next_alias_out` 恒为 1，即记录默认值），会话 blob **仍是 vsn
3**。

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
   `topic_alias=5`（**在范围内**、但本次连接从未登记）→ 连接以 0x94 断开。
5. `v5_publish_with_empty_topic_and_no_alias_is_protocol_error`：空 topic 无别名 → 0x91 断开。
6. `v5_publish_with_alias_zero_is_protocol_error`：非空 topic + `topic_alias=0`
   （MQTT-3.3.2-8）→ 0x94 断开。
7. `v5_publish_with_alias_above_maximum_is_protocol_error`：非空 topic + `topic_alias=11`，
   而 CONNACK 只宣布了 10（MQTT-3.3.2-9）→ 0x94 断开。
8. `v5_publish_alias_does_not_survive_reconnect`：登记 alias 1→topic，异常断开、clean=0
   重连，再发空 topic + alias 1 → **0x94 断开**（MQTT-3.3.2-7：重连后别名表清空，不得
   跨连接携带）。订阅方收不到重连后那条消息。
9. `v5_outbound_publish_uses_allocated_alias`：订阅方在 CONNECT 里带
   `topic_alias_maximum=5`（`peer_alias_max`），broker 给某 topic 分配别名，订阅方收到的
   PUBLISH 带 `topic_alias=1`（验证 `alias_out` 生效）。
10. `v5_outbound_no_alias_when_client_offers_none`：订阅方 CONNECT 不带
    `topic_alias_maximum`，即便 broker 自己的 `topic_alias_maximum` env 非零，订阅方收到的
    PUBLISH **不带** `topic_alias`（MQTT-3.3.2-11）。
11. `v5_outbound_alias_respects_client_maximum`：订阅方 CONNECT 带 `topic_alias_maximum=1`，
    发布到两个不同 topic：第一个收到的 PUBLISH 带 `topic_alias=1`，第二个**不带**别名
    （预算用完即停发）。
12. `v5_topic_alias_zero_still_rejects`：`topic_alias_maximum=0`（默认）时入站带别名仍 0x94
    （零回归，证明默认关闭）。

`mqtt_frame_SUITE` 已有 topic_alias / topic_alias_maximum 往返，无需新增（codec 层就绪）。

## 5. 非目标

- 不实现别名回收 / LRU 回收（别名用尽即停发，简单有界）。
- 不动 MQ-07 的 CONNACK 能力回填（Server Keep Alive / Assigned Client ID / Maximum QoS /
  Retain·Wildcard·Shared·SubId Available）——那是 CONNACK 的另一组属性，单独一个特性。
- 不改 3.1.1（无别名概念，属 MQ-16 关注点）。
- 不为别名引入新 ADR（属 ADR-017 MQTT 5 子集内的能力补齐，沿用 ADR-017）。

## 6. 影响面

- `mqtt_conn_fsm.erl`：`#st{}` 加 4 字段（`topic_alias_max`/`peer_alias_max`/`alias_in`/
  `alias_out`/`next_alias_out`）、`handle_publish/2` 改解析（含值域校验）、`publish_bin/6` 改生成、
  `connack_bin/3` 接 maximum、`handle_connect` 把 env 与客户端 CONNECT 的 `topic_alias_maximum`
  分别写进 state。**`blob/1`/`restore_blob/1` 不改**（vsn 保持 3，别名表不进会话状态）。
- `config/sys.config.src`：新增 `mqtt_topic_alias_maximum` 默认 0。
- `docs/protocol-compatibility.md` §4.2 MQTT 5 段 + §4.25 整改记录：Topic Alias 由「明确未做」改「已支持（默认关闭）」。
- `docs/compatibility/unifiedmq-mqtt-family-gap.md` MQ-04 行：改「已支持」，并注明出站上限取自客户端 CONNECT。
- 无新 app、无新端口、无新依赖、无新指标键（`broker_metrics` 键集不变）。

## 7. 验证基线

- compile 0 warning、dialyzer 0 warning、xref 无新增。
- 全量 `rebar3 ct` 在 1579 基线上 +12（上述 §4 的 12 例）→ 1591，零回归。
- 合到 main 后复跑同基线。

## 8. 修订记录

### 2026-11-21：按 MQTT-5.0 规范原文修正（MQTT-3.3.2-7 / -8 / -9 / -11）

原批准稿（2026-11-20）对 §3.3.2.3.4「Topic Alias」有两处与规范原文相悖的写
法，实现阶段对照 OASIS 正式文本核对后由用户批准修正：

1. **别名表不得跨 Network Connection 保留（原稿要求持久化，撤销）**。
   原稿 §2/§3.5 写「§3.3.2.3.2 要求别名映射随会话跨重连保留」并据
   此把 `alias_in`/`alias_out`/`next_alias_out` 写进 `.inflight` blob（vsn 4）。
   规范原文（§3.3.2.3.4，MQTT-3.3.2-7）实际是：
   > "Topic Alias mappings exist only within a Network Connection and last only
   > for the lifetime of that Network Connection. A receiver MUST NOT carry
   > forward any Topic Alias mappings from one Network Connection to another."
   即**禁止**跨连接携带映射，是 MUST NOT 而非要求。据此撤销 blob 持久化，
   vsn 保持 3，测试 `v5_publish_alias_does_not_survive_reconnect` 断言重连后
   旧别名失效（0x94）。

2. **出站别名上限取自客户端 CONNECT，而非本端宣布值（原稿用错字段，修正）**。
   原稿 §3.4 以本端 `topic_alias_max` 作为出站分配上限。规范原文
   （MQTT-3.3.2-11）：
   > "A Server MUST NOT send a PUBLISH packet with a Topic Alias greater than
   > the Topic Alias Maximum value sent by the Client in the CONNECT packet."
   即出站上限是**客户端**在 CONNECT 里发的 `topic_alias_maximum`（原稿未解析
   该字段）。据此新增 `peer_alias_max`，`maybe_alias/2` 改用它；客户端未给
   或给 0 时完全不出站别名，与本端 env 无关。测试
   `v5_outbound_no_alias_when_client_offers_none` /
   `v5_outbound_alias_respects_client_maximum` 覆盖。

3. **入站值域校验（原稿遗漏，补齐）**。MQTT-3.3.2-8（alias 0 非法）与
   MQTT-3.3.2-9（alias 不得超过本端宣布值）原稿未显式校验，仅依赖
   「未登记别名」间接覆盖。现改为先验值域、再查映射，测试
   `v5_publish_with_alias_zero_is_protocol_error` /
   `v5_publish_with_alias_above_maximum_is_protocol_error` 覆盖。

其余（入站登记/还原、空 topic 无 alias = 0x91、用尽即停发不回收、CONNACK
宣布 `topic_alias_maximum`、env 默认 0 零回归）与原批准稿一致，未改动。
