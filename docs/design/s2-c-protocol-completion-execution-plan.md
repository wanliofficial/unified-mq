# S2-C 执行计划：QoS 2 / Retain / Will

- **状态**：**已完成（2026-11-07）**；计划冻结于同日，决策见 ADR-012（经 ADR-018 §18.3 修订）与 ADR-018 §18.1/§18.2/§18.4/§18.5。实现事实与验收证据见 §7「交付记录」。
- **对应**：`docs/design/s2-single-node-kernel.md` §4.3 / §5（M2）；`docs/adr/012-qos2-semantics-and-release-state.md`；`docs/adr/018-retain-will-and-qos2-amendments.md`；roadmap §4.3 Gate S2（M2 退出条件）
- **估算**：6 人日（T-C0 ~ T-C7）
- **退出条件**：QoS2 四步握手双向通过（帧级 + 真实客户端）；重连重发与未知握手帧幂等有确定性用例；Retain 补发/清除/持久化/上限全绿；Will 异常发布矩阵全绿；T14 R8 通过；全量回归零 FAILED。

---

## 1. 目标与非目标

**目标**：把 MQTT 3.1.1 的 QoS2、Retain、Will 从「解析后丢弃/断连」补齐为可用语义，并保持 S1 已冻结的三条硬契约：

1. **PUBACK/PUBREC 只在持久化返回后发出**（ADR-007；retain=1 时还要等 retain 文件 fsync，ADR-018 §18.2）。
2. **cursor 只由消费者 ack 推进**（J6）；QoS2 出站只在 **PUBCOMP** 后推进。
3. **不存在第四份会话真相**（ADR-012 §12.2 / ADR-018 §18.3 R3）：QoS2 状态进 `.inflight` v2 blob，Will 只在内存。

**非目标**：

- 跨崩溃 exactly-once（ADR-012 §12.1 明确不做；T14 R8 只断言「允许重复、不丢消息」）。
- Will 持久化 / Will Delay（O5；MQTT 5 的 Will Delay 留 S2.5 修订）。
- retain 的追加日志与压缩（S2 用整文件快照；瓶颈由 S2-I 量化，S3 再改）。
- `$share` 语法（S2-D）、MQTT 5 properties（S2.5）。

**关键语义（实现据此）**：

| 项 | 冻结值 | 依据 |
|---|---|---|
| 入站 QoS2 投递时机 | PUBLISH 时投递（Method A），`received` 只做去重 | ADR-012 §12.3；ADR-018 §18.3 R5 |
| 出站 QoS2 cursor 推进 | 收到 **PUBCOMP** 才 ack cursor | 本节 §3.2 |
| 未知 PUBREL | 回 PUBCOMP，不断连 | ADR-018 §18.3 R1 |
| 未知 PUBREC | 回 PUBREL，不断连 | ADR-018 §18.3 R2 |
| 未知 PUBCOMP | 忽略，不断连 | ADR-018 §18.3 R2 |
| Retain 补发 QoS | `min(retained qos, granted qos)` | MQTT §4.8；ADR-018 §18.2 |
| Retain 转发标志 | 给现有订阅者 `retain=0`；补发 `retain=1` | MQTT §3.3.1.3 |
| Will 触发 | 除正常 DISCONNECT 外的所有终止（含接管） | ADR-018 §18.1 |
| Will 跨重启 | 不补发（仅内存） | O5 |

---

## 2. 交付物

| 文件 | 动作 | 说明 |
|---|---|---|
| `apps/broker_proto_mqtt/include/mqtt_frame.hrl` | 修改 | 新增 `#pubrec{}`/`#pubrel{}`/`#pubcomp{}`；`will_qos :: 0..2`；`publish.qos :: 0..2` |
| `apps/broker_proto_mqtt/src/mqtt_frame.erl` | 修改 | 类型 5/6/7 解码与编码（PUBREL flags 固定 `0b0010`）；`will_qos=2` 放行；QoS2 PUBLISH 放行 |
| `apps/broker_proto_mqtt/src/mqtt_conn_fsm.erl` | 修改 | 出站 QoS2 状态、入站 `received`、retain 整合、Will 状态与 terminate、`.inflight` v2 blob |
| `apps/broker_proto_mqtt/src/mqtt_will.erl` | 新建 | Will 发布（路由 → append → dispatch → retain），与 conn 解耦以便单测 |
| `apps/broker_proto_mqtt/src/broker_proto_mqtt.app.src` | 修改 | `applications` 增 `broker_retain`；env 增 `max_qos2_state_per_session`，`mqtt_max_qos` 默认 2 |
| `apps/broker_retain/src/*.erl`、`*.app.src` | 新建 | `broker_retain_app` / `_sup` / `broker_retain`（gen_server）/ `broker_retain_files`（原子快照） |
| `apps/broker_retain/test/broker_retain_SUITE.erl` | 新建 | 落盘、上限、损坏拒绝、并发更新 |
| `apps/broker_metrics/src/broker_metrics.erl` | 修改 | `key_specs/0` 增 ADR-018 §18.5 的 6 个计数键 |
| `config/sys.config.src` | 修改 | `mqtt_max_qos=2`（全局与 listener 两处）；`broker_retain` 配置块 |
| `rebar.config` | 修改 | release app 列表增 `broker_retain` |
| `apps/broker_proto_mqtt/test/mqtt_frame_SUITE.erl` | 修改 | 帧级黄金用例与拒绝姿态 |
| `apps/broker_proto_mqtt/test/mqtt_conn_SUITE.erl` | 修改 | FSM 四步、重连、幂等、retain、will |
| `test/integration/s1_e2e_SUITE.erl` | 修改 | 端到端 QoS2 双向 + retain 补发（若现套件形状适合；不适合则放 conn_SUITE） |
| `tools/docker/mq_harness.py` + `s1_recovery.py` | 修改 | `Client.publish` 支持 qos2 等待；新增 raw QoS2 客户端；新增 R8 场景 |
| `test/protocol-compatibility/mqtt/t15_compat.py` + `README.md` | 修改 | `paho-qos2`、`paho-retain`、`paho-will`、`mosq-qos2`；报告入库 |
| `docs/design/s2-single-node-kernel.md` / `docs/roadmap.md` / `progress.md` | 修改 | §4.3 标「已实现」；M2 退出条件勾选；S2-C 行 |

---

## 3. 冻结的接口与断言

### 3.1 帧层（T-C1）

| 帧 | 固定头 | Body | 黄金字节（pid=10） |
|---|---|---|---|
| PUBREC | `0x50`（type 5, flags 0） | `<<Pid:16>>` | `50 02 00 0A` |
| PUBREL | `0x62`（type 6, flags 2） | `<<Pid:16>>` | `62 02 00 0A` |
| PUBCOMP | `0x70`（type 7, flags 0） | `<<Pid:16>>` | `70 02 00 0A` |

拒绝姿态（沿用 S1「协议错误断连」）：

- `pid = 0`：`{error, zero_packet_id}`；
- PUBREL flags ≠ 2（如 `0x60`）、PUBREC/PUBCOMP flags ≠ 0：`{error, {malformed_packet, {bad_fixed_header_or_type, T}}}`；
- 截断/多余字节：现有 `parse_body` 尾部规则（body 长度必须精确等于 remaining length）；
- `will_qos = 2` 必须往返成功（S1 的 `unsupported_will_qos` 只在 `>2` 时返回）。

### 3.2 `.inflight` 内层 v2（T-C2）

形状见 ADR-018 §18.3 R3：`#{vsn => 2, next_pid, pending, qos, released, received}`。

- **写**：`blob/1` 始终写 `vsn => 2`；`qos` 由连接的 `pending_qos` 映射给出；`released` 为 pending 的子集。
- **读**：`restore_blob/1` 同时接受 v1（无 `vsn`、无 `qos`/`released`/`received`）与 v2；v1 的 pending 全部按 QoS1 处理。
- **订阅补发的 retained pending 不写 blob**（ADR-018 §18.3 R3）。
- **推进规则**：`pending` 里 `qos=1` 的条目收到 PUBACK 才 ack cursor；`qos=2` 的条目收到 PUBCOMP 才 ack cursor；PUBREC 只改变阶段。
- **重连重发**：CONNACK 之后、`schedule_round/1` 之前——先对 `released` 中的 pid 逐条发 PUBREL，其余 pending 由首轮 pull 以 `dup=1` 重发（QoS2 保持 qos=2、原 pid）。

### 3.3 状态上限（T-C3）

`maps:size(released) + maps:size(received) >= max_qos2_state_per_session` 时**在加入新条目前**断连：`broker_metrics:inc(qos2_state_overflow)` + `log(warning, "mqtt_qos2_state_overflow", [])`；不写状态、不回 PUBREC/PUBCOMP。

### 3.4 Retain（T-C4/T-C5）

- 存储单元：`Topic -> #{msg := #broker_message{}, qos := 0..2}`；`msg` 里保留原始 `proto_meta`（含 retain=1），供补发时构造帧。
- 更新顺序（QoS>0）：`append(fsync)` → `broker_retain:store/1`（同步 fsync+rename）→ **PUBACK/PUBREC** → `dispatch`。store 返回真正的写盘错误 `{error, _}`（非 `overflow`）时**直接断连且不回 ack**（无重试；`overflow` 已在 `retain_store/3` 吞成 `ok`）。
- 更新顺序（QoS0）：`broker_retain:store/1`（内存 + 标记脏）→ `dispatch`；不 append。
- 零长度 payload：从树中删除；消息仍 dispatch；`retain_clears` +1。
- 补发：SUBACK 之后从连接状态里的 `retained_queue` 排空；每条占用 `pending` 窗口（`max_inflight`），PUBACK/PUBCOMP 释放后继续；不推进任何 cursor。
- 上限：`max_retained` / `max_retained_bytes` / `max_payload_bytes` 任一超限 → 不入树、`retain_overflow` +1、warning；消息仍正常 dispatch、仍照常 ack。

### 3.5 Will（T-C6）

- 状态：`#st.will = undefined | #{topic, payload, qos, retain}`；CONNECT 时写入，正常 DISCONNECT 时置 `undefined`。
- `terminate/3` 中：`will =/= undefined` 且 reason ≠ `{shutdown, disconnect}`（正常断开用专用 reason）→ `mqtt_will:publish/3`；用 `try ... catch` 包裹，失败只记日志。
- `mqtt_will:publish/3` 与普通 publish 同路径：route → append(fsync if qos>0) → retain store（若 will_retain=1）→ dispatch；`will_published` +1。
- 发布发生在 `detach` **之前**：订阅者看到 Will 时，该会话仍在线（避免「Will 比 detach 先到」的竞态被误读为在线）。

### 3.6 投递 QoS（T-C3）

`DeliverQoS = min(message proto_meta.qos, max(QoS of the filters that HIT))`。

- 存储消息的 `proto_meta.qos` 为 0/1/2；S1 的「只存 QoS1」假设作废。
- `DeliverQoS=0`：无 pid、不占窗口、立即 ack_skip 所有命中的 cursor。
- `DeliverQoS=1`：现有路径。
- `DeliverQoS=2`：与 QoS1 相同地分配 pid/占窗口，但 PUBLISH 的 qos 位为 2；PUBACK 无效（记 warning，不动 cursor）。

---

## 4. 任务拆解

### T-C0 前置核对（0.25 人日）

1. 确认 ADR-012/018 状态为「已采纳」，`docs/adr/README.md` 索引含 018。
2. 记录基线：`rebar3 ct` 278/278、`xref` 38 条、`dialyzer` 干净。
3. 确认 `broker_retain` 的启动顺序：`broker_metrics` → `broker_storage` → `broker_subscription` → `broker_retain` → `broker_proto_mqtt`（`.app.src` 的 `applications` 列表即顺序）。

### T-C1 帧层（0.75 人日）

1. `mqtt_frame.hrl` 增三条记录；`publish.qos`/`will_qos` 类型放宽到 `0..2`。
2. `reserved_type/1` 删除 5/6/7；`do_parse_body/3` 增 `(5,0)`、`(6,2)`、`(7,0)` 三条精确匹配（非法 flags 走既有 fallback）。
3. `encode/1` 增三条（pid 1..65535，越界 raise）。
4. `p_connect/1` 的 `WillQoS > 1` 改为 `> 2`；`p_publish` 的 QoS 守卫改为 `=< 2`。
5. `mqtt_frame_SUITE` 增用例：`pubrec_pubrel_pubcomp_golden`、`qos2_handshake_packet_ids`、`pubrel_flags_must_be_two`、`qos2_publish_roundtrip`、`will_qos_two_roundtrip`、`pubrel_pid_zero_rejected`；更新 `connect_version_rejections` 里对 `will_qos=3` 的断言。

### T-C2 `.inflight` v2 与重连重发（1 人日）

1. `#st` 增 `pending_qos = #{}`、`released = #{}`、`received = #{}`、`retained_queue = []`。
2. `restore_blob/1` 解析 v1/v2；按 `qos` 重建 `sent` 空集语义（恢复的条目一律视为「本连接尚未发送」）。
3. `blob/1` 写 v2 并过滤非位置型 pending。
4. `handle_connect/2`：CONNACK 之后先发 `released` 的 PUBREL，再 `schedule_round/1`。
5. `deliver_qos1/6` 泛化为 `deliver/7`：`pending_rev` 命中且 `released` 命中 → 不发 PUBLISH（等 PUBREL/PUBCOMP）；其余按 `dup=1` 重发，QoS 取 `pending_qos`。
6. 用例：`inflight_v1_blob_is_read_as_qos1`、`inflight_v2_roundtrips_released_and_received`、`reconnect_before_pubrec_resends_dup_publish`、`reconnect_after_pubrec_resends_pubrel_only`。

### T-C3 出站/入站 QoS 2（1.5 人日）

1. 出站：`deliver/7` 的 `QoS=2` 分支；`take_pid` 冲突检测（`maps:is_key(Pid, pending)` 时跳过，最多 65535 次，仍冲突则断连）。
2. `handle_pubrec/2`：命中且 `qos=2` 且未 released → 记 `released`、发 PUBREL、置脏；未命中 → R2（发 PUBREL + info）；命中但 `qos=1` → warning，按未知处理（不回 PUBREL，避免把 QoS1 消息推进到 QoS2 状态机）。
3. `handle_pubcomp/2`：命中且 released → 从 pending/qos/released 删除、ack cursor、调度 snapshot；未命中 → info 忽略。
4. `handle_puback/2`：只接受 `qos=1`；`qos=2` → warning + 忽略（不动 cursor）。
5. 入站：`handle_publish` 的 `QoS=2` 分支——append(fsync) → `received` 记入（先查重：已在 `received` 则只回 PUBREC）→ PUBREC → dispatch；`handle_pubrel/2` 命中 → 删除 `received` → PUBCOMP；未命中 → R2。
6. 入站去重与 `dup` 无关：同连接内只看 packet id。
7. 用例：`qos2_inbound_four_steps_delivers_once`、`qos2_inbound_dup_publish_is_not_redelivered`、`qos2_outbound_advances_cursor_only_on_pubcomp`、`unknown_pubrel_gets_pubcomp`、`unknown_pubrec_gets_pubrel`、`unknown_pubcomp_is_ignored`、`qos2_state_overflow_disconnects`、`puback_for_qos2_does_not_advance_cursor`。

### T-C4 `broker_retain` app（1.25 人日）

1. `broker_retain_files`：路径 `{data_dir}/retain/retain.v1`；`write/1` 用 temp+fsync+rename；`read/0` 返回 `{ok, Entries} | {error, {retain_file, Path, Reason}}`（未知 vsn/损坏均拒绝）。
2. `broker_retain` gen_server：`store/1`（同步 fsync）、`lookup/1`、`match/1`（按 filter 匹配，字典序）、`snapshot/0`、`size/0`；QoS0 更新走 `retain_flush_interval_ms` 定时器批量写，`store_sync/1` 立即写。
3. 上限检查在**变更前**：条数、字节、单条 payload；拒绝时返回 `{error, overflow}`，不改变树。
4. 启动：载入文件；损坏 → `{stop, {retain_file, Path, Reason}}`。
5. `broker_retain_SUITE`：`store_and_reload`、`zero_length_clears`、`wildcard_match_is_deterministic`、`overflow_rejects_without_dropping`、`corrupt_file_refuses_to_start`、`qos0_flush_window`、`max_bytes`。

### T-C5 发布路径与订阅补发整合（0.75 人日）

1. 删除 `event(#publish{retain = 1})` 的「strip + log」分支，改为 `handle_publish/2` 内处理 retain。
2. `build_message/6` 记录真实 `retain` 与 `qos` 到 `proto_meta`；`persistence`/`reliability` 按 QoS 0/1/2 映射（2 → durable/fsync）。
3. `store_and_ack/6` 增 QoS2 分支；QoS>0 且 retain=1 时插入 retain store 调用（§3.4 顺序）。
4. `handle_subscribe/2`：SUBACK 后为每个 granted filter 取 `broker_retain:match/1`，构造 `retained_queue`，调用 `drain_retained/1`。
5. `drain_retained/1`：窗口有空位就发送，`QoS = min(retained, granted)`；`QoS>0` 记 `pending`（`{retained, Topic}`），`QoS=0` 直接发。
6. `handle_puback`/`handle_pubcomp` 识别 `{retained, Topic}`：只释放窗口，不 ack cursor。
7. 用例：`subscribe_receives_retained_after_suback`、`retained_qos_is_capped_by_grant`、`retained_wildcard_delivers_each_topic`、`retain_flag_is_zero_for_live_subscribers`、`zero_length_retain_clears_for_new_subscribers`、`retained_qos1_is_durable_before_puback`（重启后仍补发）、`retain_overflow_still_delivers_live`、`retained_pending_is_not_persisted`。

### T-C6 Will（0.75 人日）

1. `#st.will` 与 `handle_connect` 写入；`#disconnect{}` 分支置 `undefined` 后以 `{shutdown, disconnect}` 停止。
2. `mqtt_will:publish/3`：独立模块，参数 `{Will, TenantId, Principal}`；返回 `ok | {error, _}`，全部 catch。
3. `terminate/3` 在 detach 之前调用（仅异常 reason）。
4. `will_qos=2` 走 QoS2 存储映射（reliability=fsync）；`will_retain=1` 同步更新 retain 树。
5. 用例：`will_is_published_on_tcp_close`、`will_is_not_published_on_disconnect`、`will_is_published_on_keepalive_timeout`、`will_is_published_on_takeover`、`will_retain_updates_the_tree`、`will_qos_two_uses_the_qos2_path`、`will_is_not_republished_after_restart`（重启后新订阅者收不到）。

### T-C7 崩溃恢复、真实客户端与文档（0.75 人日）

1. `tools/docker/mq_harness.py`：`Client.publish` 支持 qos2 等待 `on_publish`；新增 `RawClient`（手写四步帧，用于在 PUBREC 后精确 `kill -9`）。
2. `tools/docker/s1_recovery.py` 新增 `r8`：离线订阅者 + QoS2 发布；收到 PUBREC 后 `kill -9`；重启后重连，客户端重发 PUBREL；断言**恰好完成握手、消息至少投递一次、cursor 最终推进**。
3. `t15_compat.py`：`paho-qos2`（双向四步、字节级 roundtrip）、`paho-retain`（补发/清除/转发 retain=0）、`paho-will`（异常断连发布）、`mosq-qos2`；删除 `paho-qos2-refused`/`paho-will-ignored` 的旧报告并重新生成。
4. 文档：设计 §4.3 标「已实现」+ 用例/报告链接；`docs/protocol-compatibility.md` MQTT 行；`docs/storage-and-recovery.md` `.inflight` v2；roadmap M2 与 Gate S2 的 M2 行；`progress.md`。
5. 全量回归 + `rebar3 as t14 release` + 一次 T15 全跑。

---

## 5. 场景 / 断言清单

| # | 层 | 场景 | 断言 |
|---|---|---|---|
| C-1 | 帧 | PUBREC/PUBREL/PUBCOMP 黄金字节 | `50 02 00 0A` / `62 02 00 0A` / `70 02 00 0A`；非法 flags、pid 0、截断全拒绝 |
| C-2 | 帧 | `will_qos=2`、QoS2 PUBLISH | 往返成功；`will_qos=3` 仍拒绝 |
| C-3 | FSM | 入站 QoS2 四步 | 恰好 1 次投递；PUBREL 后 PUBCOMP |
| C-4 | FSM | 入站 DUP 重发 | 只回 PUBREC，不重复投递 |
| C-5 | FSM | 出站 QoS2 | PUBLISH qos2 → 收 PUBREC 只回 PUBREL；收 PUBCOMP 后 cursor 推进 |
| C-6 | FSM | 重连（未 PUBREC） | `dup=1`、原 pid、qos2 重发 |
| C-7 | FSM | 重连（已 PUBREC） | 只发 PUBREL，不重发 PUBLISH |
| C-8 | FSM | 未知 PUBREL/PUBREC/PUBCOMP | PUBCOMP / PUBREL / 忽略；连接不断 |
| C-9 | FSM | QoS2 状态超限 | 断连 + `qos2_state_overflow` |
| C-10 | Retain | 订阅补发 | SUBACK 后逐条 `retain=1`；QoS=min；通配符多 topic 字典序 |
| C-11 | Retain | 零长度清除 | 新订阅者收不到；现有订阅者仍收到空 payload |
| C-12 | Retain | 现有订阅者 | 转发帧 `retain=0` |
| C-13 | Retain | QoS1 retained 持久化 | PUBACK 后重启仍可补发 |
| C-14 | Retain | 上限 | 不入树、仍投递、`retain_overflow` +1 |
| C-15 | Retain | 损坏文件 | 启动失败，错误含路径 |
| C-16 | Will | TCP 关闭 / keepalive / 接管 | 发布；QoS/retain 组合正确 |
| C-17 | Will | 正常 DISCONNECT | 不发布 |
| C-18 | Will | broker 重启 | 不补发 |
| C-19 | 持久化 | `.inflight` v1→v2 | v1 读为 qos1；写回 v2；`released/received` 往返 |
| C-20 | 崩溃 | T14 R8 | 已 PUBREC 未 PUBCOMP 时 `kill -9`；重启完成握手；不丢消息（允许重复） |
| C-21 | 真实客户端 | paho / mosquitto | QoS2 双向、retain 补发/清除、will 发布；帧字节 roundtrip |

---

## 6. 验收命令

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"

rm -rf _build/test/extras
rebar3 ct                       # 期望：278 + 新增用例，零 FAILED / 零 Skipped
rebar3 dialyzer
rebar3 xref                     # 预期 unused-export 基线（新增模块同步核对）
rebar3 as t14 release

# 真实客户端（需容器 + paho；/usr/bin/python3 才有 paho-mqtt 2.1.0）
/usr/bin/python3 test/protocol-compatibility/mqtt/t15_compat.py

# 崩溃恢复（含新增 R8；两档 default / zero）
/usr/bin/python3 tools/docker/s1_recovery.py --scenario r8
/usr/bin/python3 tools/docker/s1_recovery.py --keep-going
```

**判定口径**：以 `rebar3 ct` 的 `FAILED`/`Skipped` 明细为准；`TEST COMPLETE` 的用例数必须等于 `all()` 之和。T15 报告写入 `test/protocol-compatibility/mqtt/run/`，失败时保留 `data/` 与容器日志。

---

## 7. 风险

| 风险 | 缓解 |
|---|---|
| QoS2 状态机把 QoS1 路径改坏（S1 的 15 个测试用例依赖它） | T-C3 先加用例再改 `deliver`；`deliver_qos1` 的 DUP/窗口语义用现有用例做回归闸 |
| `.inflight` v2 写坏 v1 数据 | `restore_blob` 双读；v1→v2 单向；加「v1 文件 → 启动 → 写回 v2 → 再启动」用例 |
| Retain 快照重写在 retained 多时成为瓶颈 | 上限 `max_retained=10000`；S2-I 量更新延迟；S3 改追加日志 + 压缩 |
| Retain 同步落盘让 QoS1 发布多一次 fsync | 只对 retain=1 生效；基准对比 retain=0/1 的 p99 |
| Will 在 `terminate` 中发布失败（queue 已停） | `catch` 包住；失败只记日志；R8/T14 不依赖 Will |
| Will 在接管时发布导致「重连即离线」抖动 | 规范字面要求；测试固定该行为；S2.5 用 Will Delay 处理 |
| 未知握手帧幂等掩盖真实协议错误 | 只对「未知 pid」幂等；pid 已知但方向/阶段不符仍记 warning 并按 R2 处理；`protocol_errors` 只统计解码/状态错误，不把幂等应答算进去 |
| T14 R8 的 kill 时序不稳定 | 用 raw 客户端精确在 PUBREC 后触发；断言放宽为「至少一次 + 最终 cursor 推进」，与 ADR-012 §12.1 一致 |
| 新增 app 打乱 release 启动顺序 | `.app.src` 的 `applications` 列表驱动顺序；`rebar3 as t14 release` 后跑一次 `--scenario r1a` 冒烟 |

---

## 8. 交付记录（2026-11-07）

### 8.1 验收结果

| 项 | 结果 |
|---|---|
| `rebar3 ct` | **322/322 通过**，零 `FAILED`、零 `Skipped` |
| `rebar3 dialyzer` | 干净 |
| `rebar3 xref` | 46 条 unused-export（基线 38 + `broker_retain` 新增 8，均为仅由套件/FSM 调用的公开 API） |
| `mqtt_frame_SUITE` | 27/27（含 PUBREC/PUBREL/PUBCOMP 双向、QoS2 PUBLISH/Will、reserved flags） |
| `mqtt_conn_SUITE` | **50/50**（其中 S2-C 新增 20 例，覆盖 C-3~C-18） |
| `broker_retain_SUITE` | 17/17 |
| T14 | **18/18**（R1a~R8 × default/zero）；R8 为新场景 |
| T15 | **10/10**（新增 `paho-qos2`/`paho-retain`/`paho-will`/`paho-will-skipped`/`mosq-qos2`；旧 `paho-qos2-refused`/`paho-will-ignored` 删除） |

### 8.2 用例 → 计划编号对照

| 计划 | 用例 |
|---|---|
| C-1/C-2 (帧) | `mqtt_frame_SUITE:qos2_handshake_golden`、`qos2_handshake_rejections`、`qos2_publish_roundtrip`、`connect_will_qos_two_roundtrip`（另 `connect_will_qos_above_two_rejected`、`fixed_header_flag_rejections` 覆盖 0b11 与三种 fixed-header flags） |
| C-3 入站四步 | `mqtt_conn_SUITE:qos2_inbound_four_steps_delivers_once` |
| C-4 入站 DUP | `qos2_inbound_dup_publish_is_not_redelivered` |
| C-5 出站 cursor | `qos2_outbound_advances_cursor_only_on_pubcomp`（另有 `puback_for_a_qos2_delivery_does_not_advance_the_cursor` 钉 peer-bug 一半） |
| C-6 重连未 PUBREC | `reconnect_before_pubrec_resends_a_dup_publish` |
| C-7 重连已 PUBREC | `reconnect_after_pubrec_resends_pubrel_only` |
| C-8 未知握手帧 | `unknown_pubrel_gets_pubcomp`、`unknown_pubrec_gets_pubrel`、`unknown_pubcomp_is_ignored` |
| C-9 状态超限 | `qos2_state_overflow_disconnects` |
| C-10 retain 补发 | `subscribe_receives_retained_after_suback`、`retained_qos_is_capped_by_the_grant`、`retained_wildcard_delivers_each_topic_in_topic_order` |
| C-11 零长度清除 | `zero_length_retain_clears_for_new_subscribers`、`zero_length_retain_still_reaches_live_subscribers` |
| C-12 转发 retain=0 | `retain_flag_is_zero_for_live_subscribers` |
| C-13 持久化 | `retained_survives_a_retain_app_restart` |
| C-14 上限 | `retain_overflow_still_delivers_live`（含 `retain_overflow` 计数） |
| C-15 损坏文件 | `broker_retain_SUITE:corrupt_snapshot_refuses_to_boot`、`unsupported_vsn_refuses_to_boot`、`absent_snapshot_boots_empty`、`parse_rejects_garbage` |
| C-16 Will 三触发 | `will_is_published_on_tcp_close`、`will_is_published_on_a_keepalive_timeout`、`will_is_published_on_takeover`、`will_retain_updates_the_tree` |
| C-17 正常 DISCONNECT | `will_is_not_published_on_disconnect` |
| C-18 重启不补发 | `will_is_not_republished_after_a_broker_restart` |
| C-19 blob v2 | `inflight_blob_is_v2_and_skips_retained_entries` |
| C-20 崩溃 | T14 R8（`tools/docker/s1_recovery.py --scenario r8`） |
| C-21 真实客户端 | T15 的五个新用例：`paho-qos2`、`paho-retain`、`paho-will`、`paho-will-skipped`、`mosq-qos2` |

### 8.3 实现期偏差（计划外、但不改冻结决策）

1. **`will_qos=2` 的 `will_qos_two_uses_the_qos2_path` 未单独立例**：`mqtt_will:route/3` 对 QoS>0 走同一条 append+fsync 路径，QoS1 与 QoS2 在该层无分支差异，所以用例只钉住 QoS1 的发布与 `will_published` 计数；QoS2 的存储路径由 `qos2_inbound_*` 覆盖。
2. **retain 匹配不在 `broker_retain` 里**：`entries/0` 返回全树，协议层用 `mqtt_filter:matches/2` 逐条匹配。避免在 retain app 里复制一份 MQTT filter 语义（那会成为第二份真相）。
3. **`retain_store/3` 把 `{error, overflow}` 吞成 `ok`**：超过上限是**有界拒绝**（树不变、消息仍转发、publisher 仍拿到 ack），只有真正的写盘错误才断连。
4. **`retain_flush_interval_ms = 0`** 表示 async 也同步落盘（测试用）；默认 100 ms。
5. **`broker_retain_SUITE` 每个用例独立 `data_dir`**：首轮曾因共享仓库内 `data/retain` 让上一个用例的快照被下一个读到，改为逐例隔离。
6. **`killed` 不是 `mqtt_disconnect`**：DISCONNECT 现在让 FSM `{stop, normal, ...}` 并清 `will`，`terminate/3` 靠 `will =:= undefined` 判断"干净告别"。原先用 `mqtt_disconnect` 作为 stop reason 会让正常的客户端告别被记成异常终止。
7. **T14/T15 的 `data_dir` 期望数从 2 改为 3**：新增 `broker_retain` 也持有 `data_dir`；两个 harness 的渲染断言同步更新，防止某个 app 静默指向开发机路径。
8. **retain 写盘错误无重试**：§3.4 原文写「不断连地重试一次，仍失败则断连」，实现里 `retain_store/3` 只对真正的写盘错误 `{error, _}`（非 `overflow`）直接 `{stop, {retain_store_failed, _}, D}` 断连且不回 ack，没有重试层。
