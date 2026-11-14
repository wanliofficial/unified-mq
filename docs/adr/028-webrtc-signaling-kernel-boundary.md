# ADR-028：WebRTC 信令面与消息内核的边界——进程直投、瞬时转发、不落盘

- **状态**：已采纳（S10-0 评审签认，2026-11-13）
- **日期**：2026-11-13
- **关联**：ADR-001（协议适配与内核分离）、ADR-002（Mnesia 只承担控制面元数据）、ADR-007（显式可靠性）、ADR-009（`broker_message`）、ADR-010（router）、ADR-013（订阅/游标归属）、ADR-015（重投递/DLQ/TTL）、ADR-016（可观测）、ADR-021（集群控制面）、ADR-025（A2A 瞬态读者范式）
- **关联文档**：[`docs/design/s10-webrtc-signaling.md`](../design/s10-webrtc-signaling.md) §3.3（R-M1/R-M2）、§4.4（OQ-1/OQ-2）、[`docs/design/s10-0-convergence-report.md`](../design/s10-0-convergence-report.md) §1/§2（OQ-1/OQ-2 收敛）、[`docs/design/s10-execution-plan.md`](../design/s10-execution-plan.md) §0（三条硬边界）
- **事实来源**：`lambdaclass/webrtc-server`（`master`）§2.3/§2.4 报文与房间语义；本仓 `broker_queue:append/3` 源码（无 transient 旁路）、`wal_codec` 编码行为；`broker_security:authorize/2` 键集合；`rebar3 xref` 本仓既有口径

## 背景（Context）

S10 在既有统一消息内核之上新增 WebRTC **信令接入前端**（WSS 房间中继）与 STUN/TURN **ICE 辅助服务**（媒体面不做）。设计稿 §3.3 的 R-M1 要求「信令事件到 `broker_message` / `broker_router` 的映射必须先在 S10-0 以 ADR 冻结，再写实现；不得写第二套消息内核」；R-M2 要求给出「信令是瞬时转发、默认不持久化」的显式口径。

必须一起定下来的问题：①信令数据面是否进内核、以什么形态进；②「不落盘」是契约还是实现细节，如何防止 ACP 式 transient 悄悄落盘；③房间注册表与成员生命周期放哪里；④ACL 如何表达而不把房间误读成内核 destination；⑤守护这条边界的工程手段（xref 证明不了调用边，见 D7）；⑥若未来出现「可回放加入/离开」的审计需求，怎么办。

关键事实（均可在本仓源码核对）：

1. `broker_queue:append/3 → append_checked → do_append → Backend:append` 路径上**没有任何基于 `persistence` 的旁路**——字段写进 `wal_codec` 的编码单元，transient 消息同样进 WAL。
2. S9 的 ACP 前端用 `transient` 做「os_buffer 不落盘」，在本仓该语义**只在 ACP 进程内实现**，存储层并不知情。
3. `broker_security:authorize/2` 只读 `action`（atom 或 binary）、`tenant_id`、`destination` 三个键，`destination` 用于 ACL pattern 匹配。
4. `rebar3 xref` 在本仓的口径只覆盖 undefined/deprecated/recursive 调用与 unused-export，**不是调用图查询**，不能证明「不存在某条调用边」。

## 决策

### D1. 契约：信令面不产生任何内核消息——无 `#broker_message{}`、无 append、无 router、无订阅/游标/offset/retention/SubRef/`.cursors`

- WebRTC 信令数据面（SDP offer/answer、ICE candidate、五类服务器事件 `authenticated`/`unauthorized`/`invalid_message`/`joined`/`left`）是**纯内存进程间消息**：`Pid ! {text, Frame}`。
- **明确声明**：这不是「性能选择」而是**正确性契约**——房间成员是连接态连接，无订阅、无重放语义；任何落盘都会制造「消息日志之外的第三条数据面」，与 ADR-009/010/013 的单一消息面模型冲突。
- 判定标准（测试钉死）：S10-D 的完整 1:1 信令会话后，各分区 `committed` offset 不变、数据目录无增长、`.cursors` 无新增条目（`webrtc_no_persistence_SUITE` 或收敛报告 §0.4 脚本的正式版）。

### D2. 「瞬时、不落盘」是显式契约，并写明 ACP 式 transient 不落盘的前提不成立

- 本 ADR 冻结「瞬时、不落盘」为**对外承诺**；实现上不调用任何存储路径即满足，不需要新增档位。
- 明确**不**复用 ACP 的 `transient` 语义作为依据：D1 已确认存储层不认识 `persistence`，S9-F 落地修正后的 ACP transient 由 ACP 前端自己承担。**任何「把 signaling 当 transient 发进 `broker_queue`」的方案都视为违反 D1，即使字段名相同。**

### D3. 数据面进程直投；控制面内存扇出；`room` 不是 destination

- 信令消息的投递路径：连接进程 →（`webrtc_dispatch` 校验）→ 目标连接进程 `Pid ! {text, Frame}`（原报文 + `from`，**保留 `to`**）。
- 广播路径：`webrtc_room_registry` 持有的成员 pid 列表直接遍历投递；无索引、无订阅表。
- `room` 只出现在：①WSS 路径（`/websocket/<room>` 的路径绑定）；②ACL 匹配主体（D5）；③注册表分片键。**不出现在** `broker_router` 路由、`broker_subscription` 订阅索引、`broker_queue` 任何 destination 参数中。`broker_queue:append/3` 的 destination 概念对应的是「有日志的队列」，房间不属于此类。

### D4. 房间注册表：独立 app 内组件，分片 gen_server + ETS，单节点，无 Mnesia、无 boot replay

- 形态照 `a2a_task_registry`：`phash2(Room, N)` 分片的 `gen_server`，每分片一张 protected ETS 表。首期 `room_shards` 默认 `schedulers_online`。
- **单节点语义**：房间成员是连接态，节点重启后连接必然已断，注册表为空是**正确**状态，因此不落 Mnesia、不做 boot replay（与 ADR-002 不冲突——Mnesia 只存控制面元数据，这里连元数据都不需要）。
- 进程死亡兜底：注册表对成员进程 `monitor`，DOWN 时自动清理并扇出 `left`。
- 集群化（跨节点房间）不在本 ADR 范围，届时涉及 ADR-021 的归属/epoch，另立 ADR。

### D5. ACL：`destination => <<"$webrtc/room/", Room/binary>>` 仅作模式匹配主体，不是路由目的地

- 加入房间、向 `to` 发信令、服务端 API 三类操作都过 `broker_security:authorize/2`，resource 形状 `#{action => signal, tenant_id => T, destination => <<"$webrtc/room/", Room/binary>>}`。
- **显式声明**：该 `destination` 字符串只参与 ACL rule 的 pattern 匹配（`rule_matches/3` 的 `pattern_matches/2`），**不代表房间是内核 destination**——不建订阅、不进路由表、不进日志正文。
- `credential_capable/0` **不修改**：照 ADR-025/026/027 的「适配器自有 `require_auth`」路线。`authorize(undefined, _)` 返回 `allow`（既有语义），因此 `require_auth => true` 必须由适配器在 `broker_security:authenticate_conn/1` 处兜住，不得依赖 ACL 兜底。

### D6. 显式不新增内核抽象

- S10 不为信令引入任何新的内核 behavior、存储档位或路由设施。若未来出现真实需求（如「可回放加入/离开」审计、跨节点房间、需要信令的持久订阅者），**另立 ADR 并给出内核变更范围**，不在实现里夹带。

### D7. 守护手段：源码级守卫测试 + 代码评审 + 不落盘实测（xref 不用）

- **`webrtc_boundary_SUITE`（源码级守卫）**：grep `apps/broker_proto_webrtc/src/*.erl` 与 `apps/broker_ice/src/*.erl`，断言无 `broker_queue:` / `broker_router:` 调用、无 `#broker_message{}` 构造、不含 `broker_core` 的 include。
- **代码评审**：新 app 的 `.app.src` `applications` 清单评审（`broker_limits` 在 `broker_security` app 内可单列；`broker_event` 因依赖 router+queue **不**在本 ADR 的默认路径内——见 D8）。
- **S10-D 不落盘实测**：D1 的三条断言。
- **不用 `rebar3 xref` 证明边界**：本仓 xref 口径不覆盖调用边存在性（见背景事实 4）。文档中若引用 xref，只允许引用其「无 undefined/deprecated」的既有用途。

### D8. 边界口径（防误读，冻结）：契约是「无调用、无消息」，不是「依赖闭包里没有」

- `broker_event`（S7 trace helper `broker_event_trace` 所在 app）本身依赖 `broker_router` + `broker_queue`。一旦信令想用 `broker_event_trace:propagate/1`，两者就进入依赖闭包。
- 本 ADR 冻结的口径是：**默认路径下不存在对 `broker_queue` / `broker_router` 的调用、不产生 `#broker_message{}`**；`broker_event` 不在 S10-A/B 的依赖内。
- 若 S10-D 评审决定引入 trace：**先回到本 ADR 增补 D8 一条豁免说明并声明闭包变化**，不允许静默依赖。

## 后果（Consequences）

- 正向：信令路径零 WAL、零 fsync、零订阅索引；S10-A~D 可全部默认关闭发布（`listeners => []`）而不影响内核存储基线；回退姿势是删 app + 两行 relx 清单。
- 代价：信令不可审计回放（只有 `logger` 特征字段）、不可跨节点、无持久化订阅者。收敛报告 §4 已把「需要回放」列为**最可能证伪 OQ-1 的信号**，届时按 D6 另立 ADR。
- 与既有 ADR 的关系：不 amend 任何一条；D4 与 ADR-002 一致（这里连元数据都不存）；D5 复用 ADR-006/007 的身份与 ACL 面，未改其语义。

## 落地偏差记录

（S10-A 起逐条追加，格式：计划 → 实际 → 为什么 → 留痕位置。冻结文本不改。）

### S10-A（2026-11-13）

1. **D5 的「适配器自有 `require_auth`」实现方向**
   - 计划：D5 冻结「照 ADR-025/026/027 的路线，适配器自有 `require_auth`」。
   - 实际：初版把该键当成 `broker_security.require_auth` 读——但那是一个**协议名列表**（出厂 `[]`），当布尔读会让装配成功的出厂 release 一启动就 `{error, {invalid_require_auth, []}}`。已改为读本 app 自己的 `broker_proto_webrtc.require_auth`，与 `broker_proto_a2a` / `broker_proto_mcp` 一致，且有意**不**触碰 `broker_security` 语义。
   - 为什么：D5 想表达的是「开关由适配器拥有」，不是「开关的值从 `broker_security` 取」。`broker_security.require_auth` 说的是「哪些协议必须认证」，与「本适配器要不要认证」是两回事。
   - 留痕：`broker_proto_webrtc_app:auth_env/0` 注释、`webrtc_validate_SUITE:the_shipped_security_config_does_not_block_boot/1` 与 `a_non_boolean_require_auth_is_refused/1`、`docs/design/s10-execution-plan.md` §8 第 1 条。
   - **冻结文本无需变更**：D5 原文「适配器自有 `require_auth`」与修正后的实现一致，本条只记录实现曾一度读错对象。

2. **D7 的守卫测试在实现期的两处细化（不改契约，只补扫描手法）**
   - 计划：D7 写「grep `src/*.erl`，断言无 `broker_queue:` / `broker_router:` / `#broker_message{}` / 不含 `broker_core` include」。
   - 实际：①直接 grep 会把**注释里**引用的 `broker_queue:append/3`（本 ADR 原文被抄进模块注释）当成调用，用例自己先红 → 扫描前剥离注释；②`_build/test/lib/<app>/src` 是符号链接，`filelib:wildcard/1` 不跟随符号链接目录，会数出 0 个文件 → 先用 `file:read_link_all/1` 解析真实路径。
   - 为什么：守卫测试若因文档措辞或构建布局而失败，一周内就会被关掉，等于没有守卫。
   - 留痕：`webrtc_boundary_SUITE:strip_comments/1` 与 `real_path/1`、执行计划 §8 第 10/11 条。
   - **D7 冻结文本不改**：断言集合一字未少。

3. **D8 的依赖清单口径确认**
   - 计划：D8 冻结「契约是『无调用、无消息』，不是『依赖闭包里没有』」。
   - 实际：`broker_proto_webrtc.app.src` 的 `applications` **不列 `broker_core`**（不调用、不 include，只经 `broker_security` 传递进来），也不列 `broker_queue` / `broker_router` / `broker_subscription` / `broker_event`。`webrtc_boundary_SUITE` 对这四项各有一条断言。
   - 留痕：`broker_proto_webrtc.app.src` 注释、`webrtc_boundary_SUITE:app_dependency_ledger_is_clean/1`、执行计划 §8 第 9 条。

4. **D4「不落 Mnesia、不做 boot replay」的现状确认**
   - S10-A 无房间注册表（D4 描述的是 S10-B 的 `webrtc_room_registry`），因此本阶段**连注册表都没有**：监督树下默认零子进程，`listeners => []` 时零 socket。D4 的语义在 S10-B 落地时再核对（见下节 S10-B 第 1 条）。

### S10-B（2026-11-13）

1. **D4 落地核对：分片 gen_server + protected ETS，单节点，无 Mnesia、无 boot replay**
   - 计划：D4 冻结「分片 gen_server + ETS，单节点，无 Mnesia、无 boot replay」；D3 冻结「广播在持锁期内扇出」。
   - 实际：`webrtc_room_registry` 是 `one_for_all` 分片 supervisor，`erlang:phash2(Room, N) + 1` 选分片（`room_shards` 未设取 `schedulers_online`），每分片一张 `protected` named ETS `webrtc_room_shard_<N>_members`（行 `{{Room, PeerId}, Pid, Username}`，读者免 call）。`joined`/`left` 在 `webrtc_room_shard` 的同一个 `handle_call` 里、插入/删除之后立即扇出，成员集合不可能与插入竞争。`erlang:monitor/2` + `maps:take` + `demonitor(Ref, [flush])` 保证 `exit(Pid, kill)` 也恰一次 `left`；30 s `sweep` 作兜底。
   - 为什么（与 D4 一致）：成员是**连接态**，重启后连接必然已断，空注册表是正确状态；因此不建 Mnesia 表、不做 boot replay、不需要恢复语义。`one_for_all` 而非 `one_for_one` 是 D4「分片必须一致」的机械后果：单个分片崩溃时其余分片的 `monitor`/成员行会陈旧，只有整组重建 + `sweep` 才能让「注册表 = 活着的连接」重新成立。
   - 留痕：`webrtc_room_registry.erl` / `webrtc_room_shard.erl` 模块注释、`webrtc_room_SUITE:placement_is_phash2_sharded/1` 与 lifecycle 组、`webrtc_boundary_SUITE:no_mnesia_or_boot_replay/1`、`docs/architecture.md` §3.16、执行计划 §8 第 10 条。
   - **冻结文本无需变更**。

2. **D3 落地核对：`room` 只出现在三处**
   - 计划：D3 冻结「`room` 只出现在 ①WSS 路径 ②ACL 匹配主体 ③注册表分片键；不出现在 router/subscription/queue 的任何 destination 参数中」。
   - 实际：`webrtc_conn:room_from_head/1` 从 `GET /websocket/<room>` 取房间（①）；`acl/2,3` 把它拼成 `<<"$webrtc/room/", Room/binary>>` 作 ACL pattern 主体（②）；`webrtc_room_registry:shard_for/1` 用 `phash2(Room, N)` 选分片、`table_name/1` 用它作 ETS 键的一部分（③）。`webrtc_dispatch` 只在 `lookup(Room, PeerId)` 里用房间，转发目标是 `Pid`；**没有任何一处**把房间传给 `broker_router` / `broker_subscription` / `broker_queue`——这三个模块在本 app 里根本不存在（`webrtc_boundary_SUITE` 的源码扫描断言无 `broker_queue:` / `broker_router:` 调用）。
   - 留痕：`webrtc_conn:room_from_head/1` / `acl/2,3`、`webrtc_room_registry:shard_for/1`、`webrtc_boundary_SUITE`、`docs/protocol-compatibility.md` §4.21.1/§4.21.5。
   - **冻结文本无需变更**。

3. **D5 落地的一条有意简化：join 与 relay 共用同一 ACL 主体**
   - 计划：D5 冻结 `destination => <<"$webrtc/room/", Room/binary>>`，join 与 relay 自然同址。
   - 实际：照此实现，后果是 ACL **无法「允许 join、拒绝 relay」**——逐房间规则只能对整个房间允许/拒绝。两条拒绝姿态也不同：join 拒 → `unauthorized` + close 1008（且先判 ACL 再 `join`，客户端拿不到用不了的 `peer_id`），relay 拒 → `invalid_message`（冻结五类事件没有第六类，连接保持可用）。
   - 为什么：不在实现里偷偷加第二个 `action` 主体。若将来需要拆分，应当新增一个 `action`（例如 `join_room`）并改 D5 的冻结文本，而不是让 ACL 语义随实现漂移。
   - 留痕：`webrtc_acl_SUITE:a_denied_room_cannot_be_reached_at_all/1` 的注释、`docs/protocol-compatibility.md` §4.21.5、执行计划 §8 第 4/5 条。
   - **冻结文本无需变更**。

### S10-D（2026-11-14）

1. **D7「不落盘实测」由 S10-D D.2 新写证据脚本落地（不是 CT 用例，也不是复用 S10-0 的脚本）**
   - 计划：D7 写「S10-D 不落盘实测：D1 的三条断言」，判定标准是完整 1:1 会话后 offset 不变、数据目录无增长、`.cursors` 无新增。执行计划 §3 还写「复用 S10-0 的 `assert_no_persistence` 脚本」。
   - 实际：`spikes/s10_0_ws_signal/` 里**没有**这个脚本（S10-0 的 §0.4 从未交付该项），所以 D.2 是**新写**：`test/protocol-compatibility/s10/run_no_persist.sh` + `src/s10_no_persist{,_run}.erl`。它起一个活的 WAL 与**活的** `broker_subscription`（否则「没有 `.cursors`」是同义反复），seed 一条消息后跑完整 1:1 aiortc 通话，前后 `offsets`/`wal_bytes`/`.cursors` 逐字段不变，并带两条负控（append 必须动 offsets+字节；真订阅+ack 必须让 `.cursors` 出现）证明检测器可证伪。
   - 为什么：D1 的判定标准需要一个可证伪的守卫，否则「三不变」是空洞断言；同时这一层验证的是「运行时没有写」，与 `webrtc_boundary_SUITE` 的「源码级没有调用点」互补，两者都不能替代对方。它是证据脚本而不是 CT 套件，因此不进 `rebar3 ct` 计数。
   - 留痕：`test/protocol-compatibility/s10/run_no_persist.sh`、`docs/design/s10-execution-plan.md` §1 D.2 行、§2.4、§7「S10-D 部分实测」第 2 行、§8 第 1~3 条。
   - **冻结文本无需变更**。

### S10-D 定稿说明（2026-11-14）

- **定稿范围**：ADR-028 的 D1~D8 全部冻结文本**保持不变**，本说明只记录 S10-D 收尾时的最终核对结论与一处实现期缺陷修复，不改任何一条决策。
- **D1 边界的最终证据**：S10-D D.2 的 `run_no_persist.sh` 给出「信令 + ICE 全路径通话后 WAL/subscription 三不变」的运行时实测；`webrtc_boundary_SUITE` 的源码级守卫（无 `broker_queue:`/`broker_router:` 调用、无 `#broker_message{}`、无 `broker_core` include）作为 D7 的常驻守护。D.6 压测（[`docs/benchmarks/2026-11-14-s10-capacity.md`](../benchmarks/2026-11-14-s10-capacity.md)）在不落盘 tmpfs 证据节点上跑，信令路径零 WAL/零订阅索引的结论与压测数据互不矛盾。
- **D7 守护的有效性反证**：S10-D D.6 压测戳穿了一个 D1~D8 文本未及、但属于 D3「数据面进程直投」实现范围的真实缺陷——`broker_ice` 的 `turn_allocation:forward_to_peer/4` 把 `gen_udp:send` 的 `ok` 当成 gen_server 新 state 返回，导致 relay 每次分配只中继一帧就 `badrecord` 崩溃（CT 只发一帧、cast 异步，从未暴露）。修复（返回 `St`）+ 回归用例 `consecutive_channel_data_frames_all_relay`（`turn_allocation_SUITE` 25/25）落地。**该缺陷在 ADR-028 的范围外**（属 ADR-029 的 ICE relay 数据面），此处仅交叉留痕，冻结文本不动。
- **结论**：ADR-028 全部决策在 S10-D 收尾时与实现一致，无待办修订；如未来出现「可回放加入/离开审计 / 跨节点房间 / 持久化订阅者」需求，按 D6 另立 ADR。
