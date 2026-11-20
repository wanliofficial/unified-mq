# 推进边界内下一批：RQ-01 消费锚点 + MQTT-SN DTLS + JetStream MVP

## Context

`docs/compatibility/` 各文档把"边界内未完成"分三类立项。本次按依赖顺序推进三项：**M1 RQ-01**（AMQP 队列积压可读，P0，且解除 RQ-09 basic.get 的阻塞）、**M2 SN-01 DTLS**（复用 S11-C CoAP DTLS-PSK 的成熟模式）、**M3 JetStream MVP**（NATS-JS-001~003 的最小切片，依赖 M1 的 join 选项）。每项完成后跑通各自 CT + 真实客户端套件并更新文档，再进入下一项。

探索结论（已核实代码）：
- 锚点唯一咽喉：`broker_subscription_worker.erl` `reply_new_subscribe2/3`（:632-661）+ `tail_offsets/2`（:663-669），新鲜实体（exclusive 和 shared）一律锚 `High`；`broker_cursor:new_shared(Offset)` 可锚任意值；`restored_row/5` 启动恢复已支持任意 init anchor；`read_from(P,0,N)` 可读全部积压（默认无 GC）；0 锚会钉住分区 committed（与离线订阅者钉住同一机制，已有测试范式）。API 无 opts map，需新增。
- SN：`mqtt_sn_listener` 单 UDP socket 按 `{IP,Port}` 路由到 `mqtt_sn_conn`，conn 的 `reply/2`（mqtt_sn_conn.erl:1191-1196）是唯一发送口；CoAP 的 S11-C 模式（`coap_dtls_acceptor.erl` + listener 持有全部 DTLS socket + `{ssl,...}` 消息复用同一数据路径）可整体克隆，且 SN 的 per-client 路由表（endpoints/clients/sessions）无需改动；`principal` 字段已预留未接线；`psk_for_identity/1` 身份=users 表用户名，tenant 可查。
- NATS：`$JS.API.*` 主题今天就能解析/路由/匹配（`$` 合法、`.`→`/`、`>`→`#`），无任何 `$` 过滤；reply subject 经 proto_meta `<<"reply">>` 原样回显；nats.go v1.48.0 的 JS 合约已从 module cache 摸清（PubAck 最小 `{"stream","seq"}`；错误 `{"error":{code,err_code,description}}`；pull = `CONSUMER.MSG.NEXT.<s>.<c>` 请求 + `_INBOX` 收 MSG、MSG 带 `$JS.ACK.<s>.<c>.<delivery>.<sseq>.<cseq>.<ts>.<pending>` reply；ack = 客户端向该 reply 发布 `+ACK/-NAK/+TERM/+WPI`，服务端无需应答）。`broker_cursor` 的 shared 行（low/acked/assigned/retries+backoff）≈ ack floor/MaxAckPending/AckWait/MaxDeliver。

---

## M1：RQ-01 消费锚点（earliest anchor for AMQP queues）

**语义**：AMQP 队列是 durable 状态——消费者加入前发布的消息必须可投递。MQTT `$share`、NATS queue group、exclusive subscribe 保持 tail 锚（各自测试已冻结该语义），通过新增选项实现，默认不变。

改动：
1. `apps/broker_subscription/src/broker_subscription.erl`：新增 `join(Kind, Group, ClientId, Filter, QoS, Tenant, Opts)`（7 参，`Opts = #{anchor => earliest}`，默认 `tail`）。
2. `broker_subscription_worker.erl`：
   - 新增 `handle_call({join, Kind, Group, ClientId, Filter, QoS, Tenant, Opts}, ...)`；现有 7 参子句委托（Opts=`#{}`）。
   - `Info` 增加 `anchor` 字段（随 `persist_entity` 写入 `.subs`，启动恢复天然支持——`restored_row/5` 的 init-anchor 分支已读该值）。
   - `reply_new_subscribe2/3`：`anchor =:= earliest` 时 init_cursors = `[{P, 0}]`（不经 `tail_offsets/2`），否则现状不变。
   - 已有实体的 join（成员追加/QoS 更新）不碰游标——锚只在实体诞生时生效（"first consumer" 语义，与 Pulsar `new_sub/4` 的 InitialPosition 同型）。
3. `apps/broker_proto_amqp/src/amqp_conn.erl`：
   - `basic.consume` 的 join 改带 `#{anchor => earliest}`（tenant 用现有 `St#st.tenant`，从 5 参升 7 参）。
   - **恢复 basic.get 实现**（上次因锚点问题回退的那版结构）：transient join（earliest）→ 逐分区 claim(1) → 读+过滤 `amqp/<queue>` → get-ok/get-empty；`#ch.gets` 跟踪在途 get 投递，membership 保持到 ack/release/recover/连接清理才 `leave`（`leave/2` 会释放未 ack claim，不能提前走）；`message-count` 填 0。
4. 测试：
   - `broker_subscription_SUITE`：新增 `a_fresh_shared_group_with_anchor_earliest_reads_the_backlog`（publish→join→claim 拿到全部；`.subs` 重启恢复后仍从 0 语义恢复）。
   - `amqp_conn_SUITE`：恢复并修正 `basic_get_delivers_then_answers_empty`（现在能真读到积压）；新增 `backlog_published_before_consume_is_delivered`（100 条先发后收 + 双竞争消费者不重不丢，对应 RQ-01 验收 1/2）。
   - 真实客户端：`test/protocol-compatibility/amqp/main.go` 新增 `amqp-backlog-before-consume` 用例（文档 §6 点名的用例名），重跑 `amqp_compat.py --all`。
   - 回归底线：NATS `new_subscription_does_not_replay_history`、MQTT `$share`/offline-catchup、`broker_subscription_SUITE` 全量必须保持绿。
5. 文档：`docs/protocol-compatibility.md` §4.7 已知限制 5（不回放）改写为已支持 + matrix 行 + basic.get 行；§4.25 追加 M1 小节。

## M2：MQTT-SN over DTLS（SN-01，PSK，克隆 S11-C 模式）

**架构决策**：沿用 CoAP 的"listener 持有全部 DTLS socket"而非"conn 持有"——SN 的 endpoints/clients/sessions 路由表、takeover、rebind 全部保留 `{IP,Port}` 键不动，只把发送口改走 listener。

改动：
1. 新增 `apps/broker_proto_mqtt_sn/src/mqtt_sn_dtls_acceptor.erl`（克隆 `coap_dtls_acceptor.erl` 150 行：串行 handshake、identity drain、`{active,false}` 直到 `controlling_process` 完成、`{dtls_abandoned}` 泄漏防护）。
2. `mqtt_sn_listener.erl`：
   - spec 新增 `transport => dtls`（默认 `udp`，明文路径零改动）。DTLS init：`users` 表为空拒绝启动（同 CoAP）；`ssl:listen(Port, [{protocol,dtls},{ciphers,PSK套件},{user_lookup_fun,...},{mode,binary},{active,false}])`；新表 `peers/socks/idents`（克隆 coap_listener :91-106）。
   - 握手准入：`{dtls_handshake_ok,...}` → identity 校验（未知身份与失败不可区分，防枚举，同 CoAP decoy）→ `broker_security` 查 principal/tenant → admit（容量上限 `max_dtls_sessions`/`dtls_max_sessions_per_principal`）→ `{dtls_admit, ok|refuse}`；`{ssl, Sock, Bin}` 复用现有 decode+路由（键 `ssl:peername/1` 的 `{IP,Port}`）。
   - **发送口统一**：新增 listener `send_to/3`（udp 分支 = `gen_udp:send`，dtls 分支 = socks 查表 `ssl:send`）；conn 的 `reply/2`、listener 自答（SEARCHGW/GWINFO、未知客户端 DISCONNECT）改走 `{sn_send, Endpoint, Data}` cast。ADVERTISE 广播在 dtls listener 上不做（记录限制）。
   - 每端点身份随路由带给 conn：`start_session/3` 的 Cfg 注入 `principal/tenant`（明文仍 undefined + 默认租户）。
3. `mqtt_sn_conn.erl`：`#st.principal` 接线（`on_connect` 用 Cfg 值——修掉永远 undefined 的死字段）；`acl_publish/acl_subscribe`（:1151-1159）由此真正生效。
4. `broker_security.erl`：新增 `mqttsn_psk_for_identity/1`（KDF 标签 `"unifiedmq-mqttsn-psk"`，realm env `broker_security.mqttsn #{realm => <<"unifiedmq">>}`，与 `psk_for_identity/1`（coaps 专用）并列）。
5. `broker_proto_mqtt_sn.app.src`：`applications` 加 `ssl`。`require_auth` 现有拒绝行为不变（明文无身份）；DTLS 身份即认证（DTLS listener 无 users 拒绝启动），文档写清。
6. 测试：新增 `apps/broker_proto_mqtt_sn/test/mqtt_sn_dtls_SUITE.erl`（克隆 `coap_dtls_SUITE` 夹具，OTP 28 ssl DTLS 客户端已验证可用）：握手+CONNECT/CONNACK、QoS1 双向往返、未知身份 = 同样不可区分超时、ACL 拒绝（principal 生效）、无 users 拒绝启动、明文 listener 共存。现有 SN 三套件（84 例）必须全绿。
7. 文档：§4.11 DTLS 行 + `docs/security.md` §12 + `unifiedmq-mqtt-family-gap.md` SN-01 状态；注明**第三方 DTLS 证据不可得**（mqtt-sn-tools 无 DTLS），证据为仓内 DTLS CT。

## M3：JetStream MVP（NATS-JS-001~003 最小切片）

**范围**（含/不含）：含 Stream CREATE/INFO/DELETE/NAMES、JS 托管发布 + PubAck、Durable Pull Consumer CREATE/INFO/DELETE、`CONSUMER.MSG.NEXT` 批量拉取、显式 ACK（`+ACK`）/NAK（`-NAK` 重投）、DeliverPolicy `all|new`、AckPolicy `explicit|none`、流/消费者配置持久化与重启恢复。**不含**（写进文档）：push 消费者、Dedup（Nats-Msg-Id）、`STREAM.MSG.GET/DIRECT`、last_per_subject/by_start_time、镜像/源、KV/Object Store、每流独立分区。

改动：
1. 新增 `apps/broker_proto_nats/src/nats_jetstream.erl`（gen_server，sup 中在 listener 之前启动）：
   - 内部 `broker_subscription:subscribe(<<"$JS/API/#">>)` 收 API 请求（现有路由零改动）；订阅 `$JS/ACK/#` 收 ack。
   - 元数据：ETS + 持久化 `<js_data_dir>/streams.term`、`consumers.term`（写时落盘、boot 恢复）；每流计数器（first/last seq）随元数据持久化。
   - 流存储映射：destination `js/<stream>`（哈希到固定分区，offset 连续段即消息）；原 subject 存 `proto_meta.<<"nats">>.<<"subject">>`。
   - 回复生产者：向请求 reply-inbox 目的地 append+dispatch（复用 `route_message` 模式），信封不带 `type`，错误 `{"error":{code,err_code,description}}`（nats.go 只查 error 与 PubAck.stream 非空）。
   - 消费者 = 单成员 shared 组 `join(shared, <<"js-c-",S,"-",C>>, VClient, js/<stream>, 1, <<"*">>, Opts)`：DeliverPolicy all → `#{anchor => earliest}`（**依赖 M1**）；new → tail。AckPolicy none → 投递即自 ack。
   - Fetch：claim batch → `read_from` → filter_subject 在原 subject 上匹配（不匹配 ack-skip，同 AMQP 模式）→ 逐条向请求 inbox 投递，MSG 的 reply 字段填 `$JS.ACK.<s>.<c>.<d>.<sseq>.<cseq>.<ts>.<pending>`（v1 9 段格式，`Msg.Metadata()` 可解析）；`expires` 内持续供给、到期静默收尾（客户端自行返回空）。
   - Ack：`+ACK`→`ack_shared`、`-NAK`→release 重投、`+TERM`→settle、`+WPI` 忽略；AckWait/MaxDeliver 映射 broker_cursor 既有 retry/backoff（MVP 记录默认值）。
2. `nats_conn.erl`：`publish/5` 前置拦截——subject 命中托管流（查 JS ETS）→ 交 `nats_jetstream:publish_managed/4`（append 到 `js/<stream>` + PubAck 回信），不走通用路由；`$JS.API` 请求保持通用路径零改动。CONNECT/INFO 无需声明 JS 能力（`nc.JetStream()` 不做握手探测）。
3. sup/env：`broker_proto_nats_sup` 增加 `nats_jetstream` child；`{broker_proto_nats, js_data_dir}`（套件/真实客户端 boot 各自指向私有 data 目录）。
4. 测试：
   - 新增 `apps/broker_proto_nats/test/nats_jetstream_SUITE.erl`（复用 nats_conn_SUITE 的字节级客户端）：CREATE/INFO/DELETE 往返、托管发布 PubAck 序列、pull fetch 批量 + ack floor、NAK 重投、filter_subject、重启恢复（流+消费者+游标）。
   - 真实客户端：`test/protocol-compatibility/nats/main.go` 新增 3 例：`nats-js-stream-publish`（AddStream→Publish 无错）、`nats-js-pull-ack`（PullSubscribe→Fetch→Ack→再 Fetch）、`nats-js-redelivery`（不 ack→再 fetch 得同一条）；重跑 `nats_compat.py --all`（11 旧 + 3 新）。
5. 文档：`docs/protocol-compatibility.md` §4.6 JetStream 小节 + matrix 行（"无 JetStream"改为"MVP 已支持、列出明确不含项"）；`nats-kafka-pulsar-compatibility-requirements.md` 不改（需求文档）。

## 实施顺序与验证

1. **M1** → `rebar3 ct`（subscription + amqp + mqtt + nats 回归）→ `amqp_compat.py --all`（14 例含新 backlog 用例）→ 文档。
2. **M2** → SN 全部 CT（含新 dtls 套件）→ `mqtt_sn_compat.py --all` 回归（10 例明文）→ 文档。
3. **M3** → nats CT 全量 → `nats_compat.py --all`（11+3 例）→ 文档。
4. 收尾：全量 `rebar3 compile`（OTP 28）+ 受影响套件总回归；每里程碑独立提交粒度（不 commit，留给你审）。

关键复用：`broker_cursor` shared 行代数（claim/ack/release/retry 全套）、`restored_row/5` 的 init-anchor 恢复、CoAP 的 `coap_dtls_acceptor/listener` DTLS 模式与测试夹具、`psk_for_identity` 的"身份=用户名"映射、NATS 的 reply-subject 回显与 ack-skip 模式（amqp_conn.erl:781-788 同款）。

风险与对策：JetStream 的 nats.go 行为差异（Fetch 空/到期语义、ack 解析）以真实 Go 客户端迭代验收为准；JS 发布每条持久化计数器是 MVP 已知性能边界（写进文档）；SN DTLS 明文/DTLS 双 listener 共存需测。