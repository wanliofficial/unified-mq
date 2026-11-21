# MQTT 5.0 Request Response Information (MQ-06) 实现计划

> **Spec**：`docs/superpowers/specs/2026-11-21-mqtt-request-response-design.md`（已批准）
> **执行方式**：inline（当前会话逐 task 写码、跑测试、提交）。

## 概览

补齐 MQTT 5 子集最后一个「明确未做」协议特性 MQ-06。codec 已就绪（`request_response_information`(0x19) 在 CONNECT、`response_information`(0x1A) 在 CONNACK、`response_topic`/`correlation_data` 在 PUBLISH 透传），**只差 FSM 接线**：读 CONNECT 的标志、在成功 CONNACK 回填一个可配置的 Response Information 前缀。请求/响应「路由」由既有 topic-match + props 透传承担（`mqtt_conn_SUITE.erl:775` 已证明），**不写新路由代码**。

**环境（必读）**：
- worktree：`/home/wanli/.local/share/opencode/worktree/mq-request-response`，分支 `mq-request-response`（基于 main `0b01915`），clean。
- 编译/测试用 OTP 29.1.2：`export PATH="/home/wanli/Development/otp-29.1.2/bin:$PATH"`；rebar3 在 `$HOME/.local/bin`：`export PATH="$HOME/.local/bin:$PATH"`。
- CT 前 `rm -rf _build/test/extras`。
- **不提交 `rebar.lock`**（compile 副作用改它）：`git checkout rebar.lock`。
- **CT suite 路径形式**：`--suite=apps/broker_proto_mqtt/test/mqtt_conn_SUITE.erl`（`--suite=app:test/mod` 形式被拒）。`--case` 只接受单值。
- 全量 CT 基线：1601 passed / 95 suites / 0 failed（OTP 29.1.2，2026-11-21，含已合入的 MQ-05/07 + Topic Alias）。本特性预期 +5 例，目标 0 failed。
- 提交只用显式路径（禁 `git add -A`）。不杀用户进程。

**文件**：
- 改：`apps/broker_proto_mqtt/src/mqtt_conn_fsm.erl`（`#st{}` record、`handle_connect/2`、`connack_bin/3`）
- 改：`apps/broker_proto_mqtt/src/broker_proto_mqtt.app.src`、`config/sys.config.src`（配置键）
- 改：`apps/broker_proto_mqtt/test/mqtt_conn_SUITE.erl`（5 个新用例 + `all()` 注册 + `init_per_testcase` set_env）
- 改：`docs/compatibility/unifiedmq-mqtt-family-gap.md`（MQ-06 → 已支持）

---

### Task 1 — CONNACK 回填 + CONNECT 标志读取（MQ-06 回填行为）

**目标**：v5 CONNECT 带 `request_response_information=1` 且 broker 配了 `mqtt_response_information` 时，成功 CONNACK 回填 `response_information`；否则不发（零回归）。

- [ ] **Step 1：写失败测试 + 注册 + set_env**

`mqtt_conn_SUITE.erl` 的 `all()`（`:46`）加 5 个名（紧跟 `v5_connack_capabilities_are_exact` 之后）：
`v5_request_response_info_is_returned_when_requested`、`v5_request_response_info_absent_when_not_requested`、`v5_request_response_info_off_by_default`、`v5_request_response_roundtrip`、`v5_request_response_info_absent_on_refusal`。

`init_per_testcase`（`:215` 那组 set_env 区，`mqtt_topic_alias_maximum` 那行附近）加：
```erlang
    ok = application:set_env(broker_proto_mqtt, mqtt_response_information, undefined),
```

加 5 个用例（Step 4 给全文）。先加前 3 个回填用例 + 第 5 个拒绝用例（第 4 个端到端在 Task 2 加也行，但建议一次性都加）。`v5_request_response_roundtrip` 在 Task 1 先加占位（可断言能收到响应），Task 2 验证。

- [ ] **Step 2：跑测试确认失败**

`rm -rf _build/test/extras && rebar3 ct --suite=apps/broker_proto_mqtt/test/mqtt_conn_SUITE.erl --case=v5_request_response_info_is_returned_when_requested`
Expected: FAIL — 现 FSM 从不读 `request_response_information`、CONNACK 无 `response_information`，断言 `maps:get(response_information, P)` 得到 `{badkey, response_information}`。

- [ ] **Step 3：加 `#st{}` 字段 + CONNECT 读取 + CONNACK 回填**

`mqtt_conn_fsm.erl` `#st{}` record 末尾（`auth_connect = undefined :: ...` 之后，`}).` 之前）加：
```erlang
             %% MQ-06 (spec §3.1): the client asked for request/response info
             %% (CONNECT flag) and the broker has a configured Response
             %% Information to hand back. Both must hold for the CONNACK to
             %% carry response_information. `undefined' = not requested or not
             %% configured (no property emitted, zero regression).
             response_info_requested = false :: boolean(),
             response_information = undefined :: undefined | binary()
```

`handle_connect/2` 的 `#st{}` 初始化（`:452-457`，`Props = C#connect.props` 之后那组）加两字段：
```erlang
                 response_info_requested =
                     maps:get(request_response_information, Props, 0) =:= 1,
                 response_information = opt(mqtt_response_information, undefined, D0)
```

`connack_bin/3`（`:2670`）加第三个 props 来源：
```erlang
    ResponseInfoProps =
        case vsn(D) of
            5 when D#st.response_info_requested,
                  D#st.response_information =/= undefined ->
                #{response_information => D#st.response_information};
            _ ->
                #{}
        end,
    Props = maps:merge(maps:merge(connack_capability_props(RC, D),
                                  AliasProps),
                        ResponseInfoProps),
```
（把原来 `Props = maps:merge(connack_capability_props(RC, D), AliasProps),` 换成上面三行。）

- [ ] **Step 4：测试全文**

```erlang
%% MQ-06: CONNECT requests request/response info + broker configured -> CONNACK
%% carries the assigned Response Information prefix.
v5_request_response_info_is_returned_when_requested(_Config) ->
    ok = application:set_env(broker_proto_mqtt, mqtt_response_information,
                             <<"mqtt-response/">>),
    ok = boot(),
    {Conn, #connack{return_code = 0, props = P}} =
        mqtt_open_v5(#{client_id => <<"rrq">>,
                       props => #{request_response_information => 1}}),
    ?assertEqual(<<"mqtt-response/">>, maps:get(response_information, P)),
    {ok, normal} = mqtt_close(Conn),
    ok.

%% MQ-06: client did not request it -> no response_information (zero regression).
v5_request_response_info_absent_when_not_requested(_Config) ->
    ok = application:set_env(broker_proto_mqtt, mqtt_response_information,
                             <<"mqtt-response/">>),
    ok = boot(),
    {Conn, #connack{return_code = 0, props = P}} =
        mqtt_open_v5(#{client_id => <<"rrno">>, props => #{}}),
    ?assertNot(maps:is_key(response_information, P)),
    {ok, normal} = mqtt_close(Conn),
    ok.

%% MQ-06: broker not configured (default undefined) -> nothing returned even if
%% the client asked.
v5_request_response_info_off_by_default(_Config) ->
    ok = boot(),
    {Conn, #connack{return_code = 0, props = P}} =
        mqtt_open_v5(#{client_id => <<"rrodef">>,
                       props => #{request_response_information => 1}}),
    ?assertNot(maps:is_key(response_information, P)),
    {ok, normal} = mqtt_close(Conn),
    ok.

%% MQ-06: a refused CONNECT (bad password) carries no response_information -- the
%% property rides only on a successful (RC=0) CONNACK.
v5_request_response_info_absent_on_refusal(_Config) ->
    ok = application:set_env(broker_proto_mqtt, mqtt_response_information,
                             <<"mqtt-response/">>),
    ok = boot(),
    {Conn, #connack{return_code = 16#86}} =
        mqtt_open_v5(#{client_id => <<"rrbad">>, username => <<"alice">>,
                       password => <<"wrong">>,
                       props => #{request_response_information => 1}}),
    {ok, normal} = mqtt_close(Conn),
    ok.
```
> `mqtt_open_v5` 会 `mqtt_recv_v5` 收第一帧——对带 `request_response_information` 的 CONNECT 收到的是 CONNACK（不是 AUTH；MQ-06 不引入 AUTH）。拒绝情形收 `#connack{return_code=16#86}`。

- [ ] **Step 5：跑测试确认通过 + 回归**

逐个跑（`--case` 单值）：`v5_request_response_info_is_returned_when_requested`、`..._absent_when_not_requested`、`..._off_by_default`、`..._absent_on_refusal`。Expected: 4 PASS。
再跑全量 conn suite 确认零回归：`rm -rf _build/test/extras && rebar3 ct --suite=apps/broker_proto_mqtt/test/mqtt_conn_SUITE.erl`（基线 97 → +4 = 101）。
再 `rebar3 compile`（warnings-as-errors，clean）。

- [ ] **Step 6：提交（三文件；不提交 rebar.lock）**

```bash
git add apps/broker_proto_mqtt/src/mqtt_conn_fsm.erl apps/broker_proto_mqtt/src/broker_proto_mqtt.app.src config/sys.config.src apps/broker_proto_mqtt/test/mqtt_conn_SUITE.erl
git commit --no-verify -m "feat(mqtt): MQ-06 CONNACK Request Response Information回填

A v5 CONNECT with request_response_information=1, when the broker has a
configured mqtt_response_information prefix, gets that value back as the
response_information CONNACK property on a successful connect. Absent the flag
or the config, nothing is emitted (zero regression). The request/response route
itself needs no new code: response_topic/correlation_data already pass through
to subscribers (proven by v5_publish_properties_reach_the_subscriber)."
```
（app.src / sys.config 的 config 键在 Task 3 加也行；若本步想先让测试过，config 键可后置——`opt/3` 默认值 `undefined` 已保证未配时不回填。但 Step 1 的 set_env 已能驱动回填用例。建议本步先只改 `mqtt_conn_fsm.erl` + 测试，config 键统一在 Task 3 加，避免半成品。）

---

### Task 2 — 端到端请求/响应闭环（证明路由成立）

**目标**：证明 requester → responder 的请求/响应经既有路由闭环。

- [ ] **Step 1：写端到端测试**

加 `v5_request_response_roundtrip`：
```erlang
%% MQ-06 end-to-end: requester asks for response info, gets the prefix from
%% CONNACK, publishes a request whose response_topic = RI ++ correlation; the
%% responder subscribed to that topic gets the request (with response_topic +
%% correlation_data) and replies on the same topic; the requester gets the reply.
%% Proves the request/response loop closes over the existing topic routing.
v5_request_response_roundtrip(_Config) ->
    RI = <<"mqtt-response/">>,
    ok = application:set_env(broker_proto_mqtt, mqtt_response_information, RI),
    ok = boot(),
    {Requester, #connack{props = ConnProps}} =
        mqtt_open_v5(#{client_id => <<"rrReq">>,
                       props => #{request_response_information => 1}}),
    ?assertEqual(RI, maps:get(response_information, ConnProps)),
    Correlation = <<1, 2, 3>>,
    RespTopic = <<RI/binary, "c-", Correlation/binary>>,
    {Responder, _} = mqtt_open_v5(#{client_id => <<"rrResp">>, props => #{}}),
    #suback{return_codes = [1]} =
        mqtt_subscribe_v5(Responder, 1, [{RespTopic, 1}], #{}),
    mqtt_publish_v5(Requester, 3, RespTopic, <<"request">>, 1,
                    #{response_topic => RespTopic,
                      correlation_data => Correlation}),
    #puback{packet_id = 3} = mqtt_recv_v5(csock(Requester)),
    %% responder gets the request with its response_topic + correlation_data
    #publish{topic = RespTopic, payload = <<"request">>, props = ReqProps} =
        mqtt_recv_v5(csock(Responder)),
    ?assertEqual(RespTopic, maps:get(response_topic, ReqProps)),
    ?assertEqual(Correlation, maps:get(correlation_data, ReqProps)),
    %% responder replies on the same topic; requester (subscribed) gets it
    ok = mqtt_subscribe_v5(Requester, 1, [{RespTopic, 1}], #{}),
    mqtt_publish_v5(Responder, 4, RespTopic, <<"response">>, 1, #{}),
    #puback{} = mqtt_recv_v5(csock(Responder)),
    #publish{payload = <<"response">>} = mqtt_recv_v5(csock(Requester)),
    {ok, normal} = mqtt_close(Requester),
    {ok, normal} = mqtt_close(Responder),
    ok.
```
> 确认 `mqtt_subscribe_v5/4`、`mqtt_publish_v5/6`、`mqtt_recv_v5/1`、`csock/1` 的签名（看 `v5_publish_properties_reach_the_subscriber` 与 `v5_shared_subscription_delivers_to_one_member` 的用法）。`mqtt_publish_v5(Conn, Pid, Topic, Payload, QoS, Props)` 的 `Pid` 是 packet id。

- [ ] **Step 2：跑测试确认通过**

`rm -rf _build/test/extras && rebar3 ct --suite=apps/broker_proto_mqtt/test/mqtt_conn_SUITE.erl --case=v5_request_response_roundtrip`
Expected: PASS。若失败，先看是订阅时序（requester 的二次 subscribe 与 responder 的回复竞争）还是断言问题——**不要**靠放宽断言蒙混；必要时让 responder 的回复在 requester subscribe 之后发（用例里已先 subscribe requester 再 publish response，顺序已对）。

- [ ] **Step 3：提交**

```bash
git add apps/broker_proto_mqtt/test/mqtt_conn_SUITE.erl
git commit --no-verify -m "test(mqtt): MQ-06 end-to-end request/response roundtrip

Requester uses the assigned response_information to build response_topic, the
responder subscribed to it receives the request (response_topic +
correlation_data intact), replies, and the requester gets the reply -- the loop
closes over the existing topic routing."
```

---

### Task 3 — 配置键 + 缺口文档 + 全量验证 + 合入 main

- [ ] **Step 1：配置键（app.env）**

`broker_proto_mqtt.app.src` 的 `env`（`{pull_page_limit, 32}` 之前）加：
```erlang
            %% MQ-06: prefix handed back as response_information when a v5 CONNECT
            %% sets request_response_information. undefined = feature off (no
            %% property emitted). Must be a UTF-8 binary string.
            {mqtt_response_information, undefined},
```

- [ ] **Step 2：配置键（sys.config.src）**

`config/sys.config.src` 的 `{broker_proto_mqtt, [...]}` 块（`{pull_page_limit, 32}` 之前）加：
```erlang
    {mqtt_response_information, undefined},
```

- [ ] **Step 3：编译 + dialyzer**

`rebar3 compile`（exit 0）+ `rebar3 dialyzer`。本特性不引入新模块、不加 `binary_to_term`（不碰 auth），dialyzer 应仍 0 警告。若有新警告报 `mqtt_conn_fsm.erl`，修字段类型（`response_information :: undefined | binary()` 已对），不要消音。

- [ ] **Step 4：全量 CT（1601 基线 + 本特性 +5）**

`rm -rf _build/test/extras && rebar3 ct`。Expected: 1606 / 0 failed。关键 0 failed，无既有 v5 CONNACK 断言回归。

- [ ] **Step 5：更新缺口文档**

`docs/compatibility/unifiedmq-mqtt-family-gap.md`：
- §3 表 MQ-06 行（`:57`）：级别「明确未做」→「已支持」；「现在」列改为「CONNECT 的 `request_response_information=1` 且 broker 配了 `mqtt_response_information`（默认 undefined 关）时，成功 CONNACK 回填该值作为响应主题前缀；客户端据此拼 `Response Topic = RI ++ Correlation Data`。请求/响应经既有 topic 路由透传 `response_topic`/`correlation_data`，无特殊路由」。追加日期批注 `（2026-11-21 落地于 mq-request-response）`。历史实测正文不改。
- §7 状态表（`:148` 那行 `Topic Alias / AUTH` 区）：若有独立 MQ-06/Request Response 行，标「完成（MQ-06）」。
- §8 结论（`:167`）：把 Request Response Information（MQ-06）从「范围外、还没做」移到「已支持」列表。
- §70-72 的「MQ-05..MQ-07 是范围外」段：更新为「MQ-05/06/07 已全部落地」。

- [ ] **Step 6：提交**

```bash
git add apps/broker_proto_mqtt/src/broker_proto_mqtt.app.src config/sys.config.src docs/compatibility/unifiedmq-mqtt-family-gap.md
git commit --no-verify -m "feat(mqtt): wire MQ-06 mqtt_response_information config + docs

app.env + sys.config carry mqtt_response_information (default undefined). Gap
doc: MQ-06 moved to delivered; the MQTT-5 family gap table now has only MQTT 3.1
and product-level items outstanding."
```

- [ ] **Step 7：合入 main + 复跑基线**

```bash
cd /home/wanli/.local/share/opencode/worktree/mq-request-response
export PATH="/home/wanli/Development/otp-29.1.2/bin:$PATH"; export PATH="$HOME/.local/bin:$PATH"
git fetch origin
git merge --no-ff origin/main -m "merge main into mq-request-response"  # 先合 main，解冲突
git checkout main && git pull --ff-only origin main
git merge --no-ff mq-request-response -m "merge mq-request-response: MQTT-5 Request Response Information (MQ-06)"
git push origin main
```
> 冲突预期：`mqtt_conn_fsm.erl`（本特性改 `connack_bin/3` + `#st{}` + `handle_connect`；并发会话可能改别处——按行解，AUTH/Topic-Alias 段与 MQ-06 段不重叠）；`mqtt_conn_SUITE.erl`（`all()` 列表 + 用例块，union 合并）；缺口文档（双方都改 §3/§7/§8，union）。**不丢并发改动**。
> push 若因非 fast-forward 被拒：`git pull --no-rebase` 后重试，报结果。

合并后在 main 复跑全量：`rm -rf _build/test/extras && rebar3 ct`，Expected 0 failed（main 可能含别会话新用例，总数更高，0 failed 即可）。

---

## 自检（writing-plans 要求）

- **Spec 覆盖**：回填（Task 1）、CONNECT 标志读取（Task 1）、config（Task 3）、端到端闭环 + 路由（Task 2）、文档（Task 3）、merge（Task 3）。
- **零回归**：默认 `mqtt_response_information=undefined` ⇒ 永不回填；只在 RC=0 回填 ⇒ 拒绝 CONNACK 不变；3.1.1 永不回填；不碰 codec/路由 ⇒ 既有透传测试不受影响。
- **无新依赖 / 无新模块**：只改 `mqtt_conn_fsm.erl` + app.src + sys.config + 测试 + 文档。
- **类型一致**：`response_information :: undefined | binary()`，`response_info_requested :: boolean()`；CONNACK props key `response_information`（codec 已用此 atom）。
- **已知风险**：`mqtt_response_information` 须为 UTF-8 二进制（codec `utf8_prop` 会校验，非法值在编码期报错，不出错帧）。
