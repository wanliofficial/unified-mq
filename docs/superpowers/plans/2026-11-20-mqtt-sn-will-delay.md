# MQTT-SN Will Delay Interval 实现 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 补齐 MQTT-SN 遗嘱（Will）的延迟语义（v1.2 §5.4.4 / §5.6.4，gap 文档 MQ-02）：客户端带 `will_delay_interval` 异常断连后，broker 先等该秒数再发 Will；窗口内重连则取消。

**Architecture:** codec 按 Will 标志条件解析 Will Delay Interval（2 字节秒，仅 Will=true 时存在，否则破坏现有客户端 wire 兼容）。conn 异常断开且 `will_delay>0` 时不立即发，而是把 `{ClientID, Will, DelayS, Tenant}` cast 上交 listener；`will_delay=0` 维持现状立即发。listener 持 `deferred_wills` 表，自臂一个 `erlang:start_timer`（镜像 conn keepalive/sleep 惯用法）到期扫描：未重连者发布、重连者取消。重连取消主路径 = conn CONNECT 时 cast `{sn_cancel_deferred_will, ClientID}`。发布复用从 `store_publish/5` 提取的导出函数 `mqtt_sn_conn:publish_will_now(Will, Tenant)`。

**Tech Stack:** Erlang/OTP 29，rebar3，Common Test，`mqtt_sn_codec`，`mqtt_sn_conn`，`mqtt_listener`（mqtt_sn_listener）。

设计稿：`docs/superpowers/specs/2026-11-20-mqtt-sn-will-delay-design.md`

---

## 命令前缀（每个 rebar3 命令都要）

`PATH=/home/wanli/Development/otp-29.1.2/bin:$PATH /home/wanli/Development/otp-29.1.2/bin/escript $(command -v rebar3)`

下文记作 `$REBAR3 <cmd>`，执行时替换为该完整前缀（OTP 29.1.2）。

- 增量 compile 不可靠：改源码后先 `clean` 再 `compile` 才能暴露全部告警。
- ct 跑前每次 `rm -rf _build/test/extras`（只读 cookie 会让第二次运行失败）。
- 全量 ct 基线：1573 passed（CoAP Block2 已并入）。

---

## File Structure

- **Modify** `apps/broker_proto_mqtt_sn/src/mqtt_sn_codec.erl`
  - `decode_body/1`：CONNECT 双子句按 `bit(Flags,3)` 守卫条件解析 Will Delay Interval；Will=false map 增 `will_delay => 0`。
  - `encode_body(connect, ...)`：Will=true 且 `will_delay>0` 时对称插入 2 字节；否则维持现形状。
- **Modify** `apps/broker_proto_mqtt_sn/src/mqtt_sn_conn.erl`
  - `#st{}` record：加 `will_delay = 0 :: non_neg_integer()`。
  - `on_connect/2`：解构 pattern 加 `will_delay`，传入 `on_connect_sn`。
  - `on_connect_sn/5`：写 `St#st{will_delay=...}`；`sn_bind` 之后 cast `{sn_cancel_deferred_will, ClientId}`。
  - `publish_will/1`：`will_delay>0` 时改 cast 上交 listener；`store_publish/5` 改为 `store_publish(Topic,Payload,QoS,Retain,Tenant)`。
  - 新增导出 `publish_will_now/2`（will 四元组 + tenant），供 listener 复用。
- **Modify** `apps/broker_proto_mqtt_sn/src/mqtt_sn_listener.erl`
  - `#st{}` record：加 `deferred_wills = #{} :: map()`、`deferred_will_timer = undefined :: reference() | undefined`。
  - `handle_cast`：新增 `{sn_deferred_will, ClientID, Will, DelayS, Tenant}`、`{sn_cancel_deferred_will, ClientID}`。
  - `handle_info`：新增 `{timeout, Ref, deferred_will_due}`（以 `deferred_will_timer = Ref` 守卫）。
  - 新增私有 `scan_deferred_wills/1`、`arm_deferred_will_timer/1`、`cancel_deferred_will_timer/1`。
- **Modify** `apps/broker_proto_mqtt_sn/test/mqtt_sn_codec_SUITE.erl`：加 4 个 codec 验收用例（§5 1-4）。
- **Modify** `apps/broker_proto_mqtt_sn/test/mqtt_sn_conn_SUITE.erl`：加 conn 立即发不回归用例（§5 5）+ listener 集成用例（§5 6,7）。
- **Modify** `docs/compatibility/unifiedmq-mqtt-family-gap.md`、`docs/protocol-compatibility.md`：把 MQ-02 标为已交付。

---

## Task 1: codec 条件解析 Will Delay Interval（解码）

**Files:**
- Modify: `apps/broker_proto_mqtt_sn/src/mqtt_sn_codec.erl`（`decode_body/1`，`:99-117`）
- Test: `apps/broker_proto_mqtt_sn/test/mqtt_sn_codec_SUITE.erl`

- [ ] **Step 1: 写失败测试 —— Will=false 仍解出 `will_delay=>0` 且 ClientId 不被误切（零回归）**

在 `mqtt_sn_codec_SUITE.erl` 的 `all()` 列表里加入 `connect_parses_will_delay`，并把以下两个用例加进文件（紧邻现有 `connect_decodes_the_documented_layout/1`，`:318-330` 一带）。新增 helper `connect_will/5` 复用现有 encode/decode。

```erlang
connect_parses_will_delay(_Config) ->
    ClientId = <<"mqtt-sn-tools-1">>,
    %% Will=false: existing vector shape -- Duration directly precedes ClientId,
    %% no Will Delay field. Must decode will_delay=>0 AND keep ClientId intact.
    Len = 6 + byte_size(ClientId),
    ?assertEqual({ok, #{type => connect, will => false, clean_session => true,
                        protocol_id => 1, duration => 10, will_delay => 0,
                        client_id => ClientId}},
                 mqtt_sn_codec:decode(<<Len, 16#04, 16#04, 16#01, 0, 10,
                                        ClientId/binary>>)),
    ok.
```

> 注：`all()` 改动 + 用例本体都在本步一起加（下面 Step 2 再补 Will=true 两例与 round-trip）。

- [ ] **Step 2: 写失败测试 —— Will=true 带 delay 与 Will=true delay=0（含 round-trip）**

紧接上一用例追加：

```erlang
connect_true_will_delay_decodes(_Config) ->
    %% Will=true (0x08), WillDelay=30 between Duration and ClientId (v1.2 5.4.4).
    ?assertEqual({ok, #{type => connect, will => true, clean_session => false,
                        protocol_id => 1, duration => 5, will_delay => 30,
                        client_id => <<"c1">>}},
                 mqtt_sn_codec:decode(<<13, 16#04, 16#08, 16#01, 0, 5,
                                        0, 30, "c1">>)),
    %% Will=true with WillDelay=0: field present but zero.
    ?assertEqual({ok, #{type => connect, will => true, clean_session => false,
                        protocol_id => 1, duration => 5, will_delay => 0,
                        client_id => <<"c1">>}},
                 mqtt_sn_codec:decode(<<11, 16#04, 16#08, 16#01, 0, 5,
                                        0, 0, "c1">>)),
    ok.

connect_will_delay_round_trips(_Config) ->
    %% encode(Wire=true+delay>0) then decode must equal the original map.
    Map = #{type => connect, will => true, clean_session => true,
            protocol_id => 1, duration => 5, will_delay => 30,
            client_id => <<"c1">>},
    ?assertEqual({ok, Map}, mqtt_sn_codec:decode(mqtt_sn_codec:encode(Map))),
    %% Will=false encode keeps the current shape (no delay field).
    Map0 = #{type => connect, will => false, clean_session => true,
             protocol_id => 1, duration => 10, client_id => <<"c0">>},
    ?assertEqual({ok, Map0#{will_delay => 0}},
                 mqtt_sn_codec:decode(mqtt_sn_codec:encode(Map0))),
    ok.
```

把三个新用例名加入 `all()`（在现有 `connect_*` 用例附近）。

- [ ] **Step 3: 跑测试确认失败**

`rm -rf _build/test/extras && $REBAR3 ct --suite=apps/broker_proto_mqtt_sn/test/mqtt_sn_codec_SUITE.erl`

预期：`connect_true_will_delay_decodes` 与 `connect_will_delay_round_trips` 失败（当前 codec 不解析/不编码 Will Delay，且 Will=false map 无 `will_delay` 键）。`connect_parses_will_delay` 失败（缺 `will_delay => 0`）。

- [ ] **Step 4: 实现 codec 解码（双子句 + bit(Flags,3) 守卫）**

把 `mqtt_sn_codec.erl:99-117` 的单一 CONNECT 子句替换为两个互斥子句：

```erlang
decode_body(<<?CONNECT, Flags:8, ProtocolId:8, Duration:16, ClientId/binary>>)
  when byte_size(ClientId) > 0, bit(Flags, 3) =:= false ->
    %% Will=false: no Will Delay Interval field (Duration directly precedes
    %% ClientId) -- the shape every current mqtt-sn-tools client sends. The
    %% `bit(Flags,3)=:=false' guard is REQUIRED: without it this clause would
    %% also match a Will=true frame and swallow the first 2 ClientId bytes.
    case reserved(Flags, 2#00000011) of
        true ->
            {error, {reserved_bits, Flags}};
        false ->
            {ok, #{type => connect, will => false,
                   clean_session => bit(Flags, 2),
                   protocol_id => ProtocolId, duration => Duration,
                   will_delay => 0, client_id => ClientId}}
    end;
decode_body(<<?CONNECT, Flags:8, ProtocolId:8, Duration:16, WillDelay:16,
            ClientId/binary>>)
  when byte_size(ClientId) > 0, bit(Flags, 3) =:= true ->
    %% Will=true (v1.2 5.4.4): the Will Delay Interval (seconds) sits between
    %% Duration and ClientId. 0 means no delay.
    case reserved(Flags, 2#00000011) of
        true ->
            {error, {reserved_bits, Flags}};
        false ->
            {ok, #{type => connect, will => true,
                   clean_session => bit(Flags, 2),
                   protocol_id => ProtocolId, duration => Duration,
                   will_delay => WillDelay, client_id => ClientId}}
    end;
```

> `bit/2`（`:358`）、`reserved/2`（`:452`）已存在，无需新增。

- [ ] **Step 5: 跑解码测试确认通过（编码测试仍失败）**

`rm -rf _build/test/extras && $REBAR3 ct --suite=apps/broker_proto_mqtt_sn/test/mqtt_sn_codec_SUITE.erl`

预期：`connect_parses_will_delay`、`connect_true_will_delay_decodes` 通过；`connect_will_delay_round_trips` 仍失败（encode 未对称）。

- [ ] **Step 6: 提交（解码部分）**

```bash
git add apps/broker_proto_mqtt_sn/src/mqtt_sn_codec.erl apps/broker_proto_mqtt_sn/test/mqtt_sn_codec_SUITE.erl
git commit -m "feat(mqtt_sn): decode Will Delay Interval (Will flag gated)"
```

---

## Task 2: codec 对称编码 Will Delay Interval

**Files:**
- Modify: `apps/broker_proto_mqtt_sn/src/mqtt_sn_codec.erl`（`encode_body(connect, ...)`，`:265-270`）
- Test: `apps/broker_proto_mqtt_sn/test/mqtt_sn_codec_SUITE.erl`（Task 1 已加 round-trip 用例）

- [ ] **Step 1: 写失败测试（已在 Task 1 Step 2 加）**

`connect_will_delay_round_trips/1` 已覆盖：Will=true+delay>0 编码后解码相等；Will=false 编码形状不变。

- [ ] **Step 2: 跑测试确认失败**

`rm -rf _build/test/extras && $REBAR3 ct --suite=apps/broker_proto_mqtt_sn/test/mqtt_sn_codec_SUITE.erl`

预期：`connect_will_delay_round_trips` 失败（当前 encode 不插 WillDelay 字节，Will=true 帧解码回 `will_delay => 0` 而非 30）。

- [ ] **Step 3: 实现 encode 对称**

把 `encode_body(connect, P)`（`:265-270`）替换为：

```erlang
encode_body(connect, #{client_id := ClientId} = P) ->
    Flags = flag(bit_of(maps:get(will, P, 0)), 3)
            bor flag(bit_of(maps:get(clean_session, P, 0)), 2),
    %% v1.2 5.4.4: insert the Will Delay Interval (2 bytes) only when Will is set
    %% and the value is >0; otherwise keep the current Duration->ClientId shape.
    Body = case {maps:get(will, P, 0), maps:get(will_delay, P, 0) > 0} of
               {true, true} -> <<(maps:get(will_delay, P, 0)):16>>;
               _            -> <<>>
           end,
    <<?CONNECT, Flags:8, (maps:get(protocol_id, P, 1)):8,
      (maps:get(duration, P, 0)):16, Body/binary, ClientId/binary>>;
```

> `flag/2`（`:365`）、`bit_of/1`（`:361`）已存在。

- [ ] **Step 4: 跑全套 codec 测试确认通过**

`rm -rf _build/test/extras && $REBAR3 ct --suite=apps/broker_proto_mqtt_sn/test/mqtt_sn_codec_SUITE.erl`

预期：全部通过（含 4 个新用例与既有 CONNECT 向量）。

- [ ] **Step 5: 提交**

```bash
git add apps/broker_proto_mqtt_sn/src/mqtt_sn_codec.erl
git commit -m "feat(mqtt_sn): encode Will Delay Interval symmetric to decode"
```

---

## Task 3: conn 携带 will_delay 并在重连时取消 deferred will

**Files:**
- Modify: `apps/broker_proto_mqtt_sn/src/mqtt_sn_conn.erl`（`#st{}` `:69`、`on_connect/2` `:274`、`on_connect_sn/5` `:295,315,323`）
- Test: `apps/broker_proto_mqtt_sn/test/mqtt_sn_conn_SUITE.erl`（重连取消用例见 Task 6）

- [ ] **Step 1: 给 `#st{}` 加 `will_delay` 字段**

在 `mqtt_sn_conn.erl` 的 `#st{}` record（`:69` 的 `will ::` 之后）加一行：

```erlang
    will :: undefined | {binary(), binary(), 0..2, boolean()},
    %% v1.2 5.4.4: Will Delay Interval in seconds from CONNECT; 0 = no delay.
    will_delay = 0 :: non_neg_integer(),
    will_stage = none :: none | want_topic | want_msg,
```

- [ ] **Step 2: `on_connect/2` 解构 pattern 加 `will_delay` 并传入 `on_connect_sn`**

把 `on_connect/2`（`:274-275`）的 head 改为：

```erlang
on_connect(#{client_id := ClientId, protocol_id := ProtocolId,
             clean_session := Clean, duration := Duration, will := Will,
             will_delay := WillDelay}, St) ->
```

并把其中的调用 `on_connect_sn(ClientId, Clean, Duration, Will, St0)`（`:280`）改为：

```erlang
                    on_connect_sn(ClientId, Clean, Duration, Will,
                                  WillDelay, St0);
```

- [ ] **Step 3: `on_connect_sn/5` → `on_connect_sn/6`，写 `will_delay`，重连取消**

把 `on_connect_sn/5`（`:295`）改为 `on_connect_sn/6`：

```erlang
on_connect_sn(ClientId, Clean, Duration, Will, WillDelay, St) ->
```

把 `St1 = St#st{client_id = ClientId, clean = Clean, duration = Dur,`（`:315-316`）改为含 `will_delay`：

```erlang
    St1 = St#st{client_id = ClientId, clean = Clean, duration = Dur,
                will_delay = WillDelay,
                subs = Subs,
```

在 `gen_server:cast(St#st.listener, {sn_bind, self(), ClientId}),`（`:323`）之后追加取消 cast：

```erlang
    gen_server:cast(St#st.listener, {sn_bind, self(), ClientId}),
    %% v1.2 will-delay: a reconnect cancels any pending deferred Will for this
    %% client id (deterministic cancel path; see spec 2.3). Sent regardless of
    %% whether this CONNECT carries a Will -- reconnecting is the cancel signal.
    gen_server:cast(St#st.listener, {sn_cancel_deferred_will, ClientId}),
```

> 其余 `on_connect_sn/5` 子句（如 `:295` 之外无其他子句）——本函数仅此一定义。`finish_connect`、`on_will_msg` 等不改。

- [ ] **Step 4: 编译确认 0 warning**

`$REBAR3 clean && $REBAR3 compile`

预期：无 `mqtt_sn_conn.erl` 告警。若有"`on_connect_sn/5` undefined"或"unused" 即漏改某处，全仓 `on_connect_sn` 引用应只剩 `:280` 一处调用。

- [ ] **Step 5: 提交**

```bash
git add apps/broker_proto_mqtt_sn/src/mqtt_sn_conn.erl
git commit -m "feat(mqtt_sn): carry will_delay and cancel deferred will on reconnect"
```

---

## Task 4: conn 异常断连上交 deferred will（替代立即发）

**Files:**
- Modify: `apps/broker_proto_mqtt_sn/src/mqtt_sn_conn.erl`（`publish_will/1` `:417-427`、`store_publish/5` `:1066`、新增导出 `publish_will_now/2`、API export 行）
- Test: `apps/broker_proto_mqtt_sn/test/mqtt_sn_conn_SUITE`（will_delay=0 立即发不回归用例见 Task 6）

- [ ] **Step 1: 把 `store_publish/5` 改为接收 tenant 参数**

把 `store_publish(Topic, Payload, QoS, Retain, St)`（`:1066`）改为：

```erlang
store_publish(Topic, Payload, QoS, Retain, Tenant) ->
    Msg = new_message(Tenant, Topic, Payload, QoS, Retain),
    publish_routed(Msg, QoS, Retain).
```

更新两处既有调用：`:420`（`publish_will` 内）改为 `store_publish(Topic, Payload, QoS, Retain, St#st.tenant)`；`:474`、`:486`（PUBLISH 上行）改为 `store_publish(Topic, Payload, QoS, Retain, St#st.tenant)`。

> 用 grep 确认 conn 内 `store_publish(` 全部三处调用都已加第 5 实参。

- [ ] **Step 2: 改 `publish_will/1` 分流（will_delay>0 上交 listener）**

把 `publish_will/1`（`:417-427`）替换为：

```erlang
publish_will(#st{will = undefined}) ->
    ok;
publish_will(#st{will_delay = 0} = St) ->
    %% No delay: fire now, exactly as before.
    do_publish_will(St);
publish_will(#st{will = {Topic, Payload, QoS, Retain} = Will,
                will_delay = DelayS, client_id = ClientId,
                tenant = Tenant} = St) when DelayS > 0 ->
    %% v1.2 will-delay: hand off to the listener, which holds the will for
    %% DelayS seconds and cancels it if the client reconnects. The conn process
    %% is about to die, so it cannot arm the timer itself.
    gen_server:cast(St#st.listener,
                    {sn_deferred_will, ClientId, Will, DelayS, Tenant}),
    ok.

%% Fire a Will immediately (no delay). Shared by publish_will/1 and the will
%% update path; the metric and the store_publish call live in one place.
do_publish_will(#st{will = {Topic, Payload, QoS, Retain}} = St) ->
    case store_publish(Topic, Payload, QoS, Retain, St#st.tenant) of
        ok ->
            broker_metrics:inc(will_published);
        {error, Reason} ->
            logger:warning("mqtt_sn could not publish will for ~p: ~p",
                           [St#st.client_id, Reason])
    end,
    ok.
```

> `do_publish_will/1` 取代原 `publish_will/1` 立即发分支体。`store_publish/5` 已是新签名（Task 4 Step 1）。

- [ ] **Step 3: 导出 `publish_will_now/2` 供 listener 复用**

在 `mqtt_sn_conn.erl` 的 `-export` 区（API `-export([start_link/1, ...])` 那一行，或新建一个 listener 可见的导出）加入：

```erlang
-export([publish_will_now/2]).
```

在 `do_publish_will/1` 附近新增导出实现：

```erlang
%% @doc Publish a Will four-tuple under an explicit tenant. Used by the MQTT-SN
%% listener to fire a deferred Will whose delay elapsed (spec 2.5): same path
%% and metric as publish_will/1, but the listener is not a #st{} holder.
-spec publish_will_now({binary(), binary(), 0..2, boolean()}, binary()) -> ok.
publish_will_now({Topic, Payload, QoS, Retain}, Tenant) ->
    case store_publish(Topic, Payload, QoS, Retain, Tenant) of
        ok ->
            broker_metrics:inc(will_published);
        {error, Reason} ->
            logger:warning("mqtt_sn could not publish deferred will: ~p",
                           [Reason])
    end,
    ok.
```

> 确认 `-export([publish_will_now/2])` 不与现有导出块冲突（放 API export 块内或单列均可，dialyzer 要 spec 与实现一致）。

- [ ] **Step 4: 编译确认 0 warning**

`$REBAR3 clean && $REBAR3 compile`

预期：无告警。特别看 `do_publish_will/1`、`publish_will_now/2`、`store_publish/5` 的 arity 一致。

- [ ] **Step 5: 提交**

```bash
git add apps/broker_proto_mqtt_sn/src/mqtt_sn_conn.erl
git commit -m "feat(mqtt_sn): hand deferred will to listener on abnormal disconnect"
```

---

## Task 5: listener 持有 deferred_wills 表 + 自臂 timer + 扫描发布/取消

**Files:**
- Modify: `apps/broker_proto_mqtt_sn/src/mqtt_sn_listener.erl`（`#st{}` `:52-85`、`handle_cast` `:261-283`、`handle_info` `:334-340`、新增私有函数）
- Test: `apps/broker_proto_mqtt_sn/test/mqtt_sn_conn_SUITE`（集成用例见 Task 6）

- [ ] **Step 1: 给 `#st{}` 加 deferred_wills 表与 timer 引用**

在 `mqtt_sn_listener.erl` 的 `#st{}` record（`:84` `advertise_port` 之后）加：

```erlang
    %% v1.2 will-delay: ClientID -> {Will, DelayS, AtMs, Tenant}. Held after a
    %% session dies with will_delay>0; published when DelayS elapses unless the
    %% client reconnects first. AtMs is the listener's own monotonic clock.
    deferred_wills = #{} :: map(),
    deferred_will_timer = undefined :: reference() | undefined,
```

- [ ] **Step 2: `handle_cast` 加两个消息**

在 `handle_cast(_Msg, St)`（`:282`）之前插入：

```erlang
handle_cast({sn_deferred_will, ClientID, Will, DelayS, Tenant},
            #st{deferred_wills = DW} = St) ->
    AtMs = erlang:monotonic_time(millisecond),
    DW1 = maps:put(ClientID, {Will, DelayS, AtMs, Tenant}, DW),
    {noreply, arm_deferred_will_timer(St#st{deferred_wills = DW1})};
handle_cast({sn_cancel_deferred_will, ClientID},
            #st{deferred_wills = DW} = St) ->
    case maps:take(ClientID, DW) of
        {_, DW1} ->
            %% Drop the cancelled entry, cancel any armed timer, then re-arm to
            %% the new nearest deadline (or none if the table is now empty).
            St1 = cancel_deferred_will_timer(St#st{deferred_wills = DW1}),
            {noreply, arm_deferred_will_timer(St1)};
        error ->
            {noreply, St}
    end;
```

> 先 `cancel_deferred_will_timer/1` 再 `arm_deferred_will_timer/1`：cancel 后若表非空立即重臂到下一个最近 deadline；若表空，timer 已清、`arm_...` 见空表不臂。这样 cancel 路径不会留下 stale timer。

- [ ] **Step 3: `handle_info` 加到期扫描**

在 `handle_info(advertise_tick, St)`（`:336`）之前插入：

```erlang
handle_info({timeout, Ref, deferred_will_due},
            #st{deferred_will_timer = Ref} = St) ->
    {noreply, scan_deferred_wills(St)};
```

> `#st{deferred_will_timer = Ref}` 守卫确保只认当前 timer（stale timer 落到 `:339` 的 `_Other`）。

- [ ] **Step 4: 实现扫描 + 自臂 timer 私有函数**

在 `forget/2`（`:535`）之后、`%%%_* ADVERTISE`（`:537`）之前插入：

```erlang
%%%_* Deferred Will (v1.2 will-delay) ---------------------------------------

%% Fire the Will for every entry whose delay has elapsed, cancel those whose
%% client has a live session (reconnected), and re-arm for the rest. A live
%% session is keyed in `clients'; the conn's CONNECT-time cancel cast is the
%% primary cancel path, this scan is the expiry path.
scan_deferred_wills(#st{deferred_wills = DW} = St) ->
    Now = erlang:monotonic_time(millisecond),
    St1 = maps:fold(
            fun(ClientID, {Will, DelayS, AtMs, Tenant}, Acc) ->
                    case (Now - AtMs) >= DelayS * 1000 of
                        false ->
                            Acc;  %% not due yet
                        true ->
                            case maps:is_key(ClientID, Acc#st.clients) of
                                true ->
                                    %% Reconnected within the window: cancel.
                                    Acc#st{deferred_wills =
                                           maps:remove(ClientID,
                                                       Acc#st.deferred_wills)};
                                false ->
                                    ok = mqtt_sn_conn:publish_will_now(Will, Tenant),
                                    Acc#st{deferred_wills =
                                           maps:remove(ClientID,
                                                       Acc#st.deferred_wills)}
                            end
                    end
            end, St, DW),
    arm_deferred_will_timer(St1#st{deferred_will_timer = undefined}).

%% Arm a single timer to the nearest deadline. No-op if the table is empty.
arm_deferred_will_timer(#st{deferred_wills = DW,
                            deferred_will_timer = undefined} = St) ->
    case nearest_due_ms(DW, erlang:monotonic_time(millisecond)) of
        undefined ->
            St;
        DueMs ->
            St#st{deferred_will_timer =
                          erlang:start_timer(DueMs, self(), deferred_will_due)}
    end;
arm_deferred_will_timer(St) ->
    %% A timer is already armed; the earliest deadline will trigger the scan and
    %% re-arm if anything remains. (Cancelling + re-arming on every cancel would
    %% churn; the scan handles correctness.)
    St.

cancel_deferred_will_timer(#st{deferred_will_timer = undefined} = St) ->
    St;
cancel_deferred_will_timer(#st{deferred_will_timer = TRef} = St) ->
    _ = erlang:cancel_timer(TRef),
    St#st{deferred_will_timer = undefined}.

%% Milliseconds until the earliest deadline in the table.
nearest_due_ms(DW, Now) ->
    Due = maps:fold(
            fun(_ClientID, {_Will, DelayS, AtMs, _Tenant}, Acc) ->
                    Deadline = AtMs + DelayS * 1000,
                    min(Deadline, Acc)
            end, infinity, DW),
    case Due of
        infinity -> undefined;
        _ when Due =< Now -> 0;
        _ -> Due - Now
    end.
```

> **关键**：扫描结束时**必须**先把 `deferred_will_timer` 置 `undefined` 再 `arm_deferred_will_timer/1`。因为触发本次扫描的 timer 已 fired（其 ref 仍留在 `#st.deferred_will_timer`），不重置的话 `arm_deferred_will_timer/1` 的"已 armed"守卫会拒绝重臂，到期表项就再不会被触发。上方代码已用 `St1#st{deferred_will_timer = undefined}` 处理。
>
> `sn_cancel_deferred_will`（Step 2）同理：先 `cancel_deferred_will_timer/1` 清旧 ref，再 `arm_deferred_will_timer/1` 才能为新 deadline 重臂。

- [ ] **Step 5: 编译确认 0 warning**

`$REBAR3 clean && $REBAR3 compile`

预期：无告警。看 `scan_deferred_wills/1`、`arm_deferred_will_timer/1`、`nearest_due_ms/2`、`cancel_deferred_will_timer/1` 的 dialyzer 类型（`infinity` 分支）。

- [ ] **Step 6: 提交**

```bash
git add apps/broker_proto_mqtt_sn/src/mqtt_sn_listener.erl
git commit -m "feat(mqtt_sn): listener holds deferred wills, fires on expiry"
```

---

## Task 6: 集成测试（立即发不回归 + 延迟发布 + 重连取消）

**Files:**
- Modify: `apps/broker_proto_mqtt_sn/test/mqtt_sn_conn_SUITE.erl`（`all()` `:32`、helper `connect_with_will/5` 旁加带 delay 变体、用例本体）

- [ ] **Step 1: 加带 will_delay 的连接 helper**

在 `connect_with_will/5`（`:283`）之后加：

```erlang
%% Same as connect_with_will/5 but with a Will Delay Interval (seconds).
connect_with_will_delay(S, Port, ClientId, Topic, Payload, DelayS) ->
    send(S, Port, #{type => connect, will => true, clean_session => true,
                    protocol_id => 1, duration => 60, will_delay => DelayS,
                    client_id => ClientId}),
    #{type := willtopicreq} = expect(S, willtopicreq),
    send(S, Port, #{type => willtopic, qos => 0, retain => false, topic => Topic}),
    #{type := willmsgreq} = expect(S, willmsgreq),
    send(S, Port, #{type := willmsg, payload := Payload}),
    #{type := connack, return_code := 0} = expect(S, connack),
    ok.
```

> 注：`send/3` 的参数是构造 map，用 `=>`；`expect/2` 返回的 `#{type := ...}` 是模式匹配，用 `:=`。

- [ ] **Step 2: 写用例 A —— will_delay=0 立即发不回归（§5 5）**

`all()` 加入 `will_delay_zero_publishes_immediately`。用例本体（紧邻 `will_is_published_when_the_session_dies/1`，`:524`）：

```erlang
will_delay_zero_publishes_immediately(Config) ->
    Port = ?config(port, Config),
    Sub = open(),
    connect(Sub, Port, <<"sn-d0-sub">>),
    _ = subscribe(Sub, Port, <<"sn/d0/t">>, 1),
    Before = conn_pids(),
    W = open(),
    connect_with_will_delay(W, Port, <<"sn-d0-client">>, <<"sn/d0/t">>,
                            <<"now">>, 0),
    [ConnPid] = conn_pids() -- Before,
    ok = gen_server:stop(ConnPid, boom, 1000),
    %% delay=0: the Will fires at once, same as the no-delay path.
    #{type := publish, payload := <<"now">>} = expect(Sub, publish),
    close(Sub),
    close(W).
```

- [ ] **Step 3: 写用例 B —— will_delay>0 异常断连后在窗口内不立即发（§5 6，前半）**

`all()` 加入 `will_delay_defers_the_publish`。用例本体：

```erlang
will_delay_defers_the_publish(Config) ->
    Port = ?config(port, Config),
    Sub = open(),
    connect(Sub, Port, <<"sn-dd-sub">>),
    _ = subscribe(Sub, Port, <<"sn/dd/t">>, 1),
    Before = conn_pids(),
    W = open(),
    connect_with_will_delay(W, Port, <<"sn-dd-client">>, <<"sn/dd/t">>,
                            <<"later">>, 5),
    [ConnPid] = conn_pids() -- Before,
    ok = gen_server:stop(ConnPid, boom, 1000),
    %% The Will is deferred: NOTHING arrives within a window shorter than 5s.
    refute(Sub, publish, 1000),
    %% Now let it elapse; the Will arrives.
    #{type := publish, payload := <<"later">>} = expect(Sub, publish),
    close(Sub),
    close(W).
```

- [ ] **Step 4: 写用例 C —— 窗口内重连取消（§5 7）**

`all()` 加入 `will_delay_cancelled_by_reconnect`。用例本体：

```erlang
will_delay_cancelled_by_reconnect(Config) ->
    Port = ?config(port, Config),
    Sub = open(),
    connect(Sub, Port, <<"sn-dc-sub">>),
    _ = subscribe(Sub, Port, <<"sn/dc/t">>, 1),
    Before = conn_pids(),
    W = open(),
    connect_with_will_delay(W, Port, <<"sn-dc-client">>, <<"sn/dc/t">>,
                            <<"gone">>, 8),
    [ConnPid] = conn_pids() -- Before,
    %% Drop the will-owner abnormally; the deferred will is now registered.
    ok = gen_server:stop(ConnPid, boom, 1000),
    refute(Sub, publish, 500),
    %% Reconnect the SAME client id within the window: cancels the deferred Will.
    Again = open(),
    connect(Again, Port, <<"sn-dc-client">>),
    %% The window elapses; the cancelled Will never arrives.
    timer:sleep(9200),
    refute(Sub, publish, 500),
    close(Sub),
    close(Again),
    close(W).
```

> 用例 C 约 10s，是该套件最长的用例；可接受（参照 `keepalive_timeout_ends_the_session` 等已有真实等待用例）。

- [ ] **Step 5: 跑 conn 套件确认通过**

`rm -rf _build/test/extras && $REBAR3 ct --suite=apps/broker_proto_mqtt_sn/test/mqtt_sn_conn_SUITE.erl`

预期：新增 3 用例 + 既有全部通过。若用例 B/C 因时序抖动偶发，调 `refute` 窗口或 `DelayS`。

- [ ] **Step 6: 提交**

```bash
git add apps/broker_proto_mqtt_sn/test/mqtt_sn_conn_SUITE.erl
git commit -m "test(mqtt_sn): will-delay defer, immediate-fire, reconnect-cancel"
```

---

## Task 7: 全量验证 + 文档 + 合并

**Files:**
- Modify: `docs/compatibility/unifiedmq-mqtt-family-gap.md`（MQ-02 行 `:53`、说明 `:159`）、`docs/protocol-compatibility.md`（§4.11 MQTT-SN Will Delay 段）
- 复用：`apps/broker_proto_mqtt_sn/CHANGES.md`

- [ ] **Step 1: 全量 compile + dialyzer 0 warning**

```bash
$REBAR3 clean && $REBAR3 compile
$REBAR3 dialyzer
```

预期：compile 0 warning、dialyzer 0 warning。重点看 `publish_will_now/2` spec、`scan_deferred_wills/1` 的 `clients`/`deferred_wills` 类型、`nearest_due_ms/2` 的 `infinity` 分支。

- [ ] **Step 2: 全量 ct 不回归（基线 1573 + 本次新增 7 例 = 1580）**

```bash
rm -rf _build/test/extras && $REBAR3 ct
```

预期：`All 1580 tests passed.`（1573 基线 + codec 4 + conn 3）。若任一失败，按 ct 日志 HTML 定位（`_build/test/logs/ct_run.*/.../*.html`），不靠改断言掩盖。

- [ ] **Step 3: 更新缺口文档 —— MQ-02 标为已交付**

把 `docs/compatibility/unifiedmq-mqtt-family-gap.md:53` 的 MQ-02 行 `未做完` 改为 `已交付`，并把"现状"列更新为：

```
| MQ-02 | 已交付 | Will Delay：codec 按 Will 标志条件解析 will_delay_interval（仅 Will=true 时存在），断连后 listener 持有延迟 will、到期发布、窗口内重连取消 |
```

把 `:159` 附近的 "Will Delay 解了帧却立刻发遗嘱。" 一句改为说明已交付（或移入"已解决"清单）。

- [ ] **Step 4: 更新 `docs/protocol-compatibility.md` §4.11**

在该节把 Will Delay 从"未做/半做"更新为"已交付"，并写明：延迟精度 = listener timer（毫秒驱动、秒级语义）；listener 重启则 pending 延迟 will 清空。

- [ ] **Step 5: 追加 `apps/broker_proto_mqtt_sn/CHANGES.md`**

```markdown
## Will Delay Interval (v1.2 §5.4.4, gap MQ-02)

- codec 按 Will 标志条件解析/编码 `will_delay_interval`（2 字节秒，仅 Will=true 时存在；Will=false 维持 Duration→ClientId 直接相连，零回归）。
- 异常断连且 `will_delay>0` 时，conn 把 will 上交 listener 而非立即发；listener 持有延迟 will，到期未重连则发布、窗口内重连（同 ClientID）取消。
- `will_delay=0` 维持立即发。延迟精度秒级（listener 自臂 timer，毫秒驱动）。
```

- [ ] **Step 6: 提交**

```bash
git add docs/compatibility/unifiedmq-mqtt-family-gap.md docs/protocol-compatibility.md apps/broker_proto_mqtt_sn/CHANGES.md
git commit -m "docs(mqtt_sn): mark Will Delay Interval (MQ-02) delivered"
```

- [ ] **Step 7: 合并回 main（--no-ff）并清理 worktree**

> 仅当本计划在隔离 worktree/feature 分支执行（如 `mqtt-sn-will-delay`）。若直接在 main 上做则跳过。

```bash
# 在主 checkout（main 分支）：
git checkout main
git fetch origin
git merge --no-ff mqtt-sn-will-delay -m "Merge mqtt-sn-will-delay: Will Delay Interval (MQ-02)"
# 验证合并结果：
rm -rf _build/test/extras && $REBAR3 ct   # 期望 All 1580 tests passed.
```

用平台 `worktree_remove` 工具清理 worktree（harness-owned，路径在 `~/.local/share/opencode/worktree/`），随后 `git branch -d mqtt-sn-will-delay`。**推送 origin 等用户确认。**

---

## Self-Review（计划自检）

- **spec 覆盖**：spec §5 验收 1-4 → Task 1/2（codec）；§5 5-6 → Task 4/6 用例 A；§5 7-8 → Task 6 用例 B/C；§5 9 → Task 7 Step 1-2。无遗漏。
- **占位符**：无 TBD/TODO；每个改动步骤给了完整代码。
- **类型一致**：`publish_will_now/2`（Will 四元组 + Tenant）在 Task 4 定义、Task 5 调用一致；`store_publish/5` 在 Task 4 改签名、conn 三处调用同步更新；`sn_deferred_will`/`sn_cancel_deferred_will` cast 元数在 Task 3（conn 发）与 Task 5（listener 收）一致。
- **wire-compat**：codec 双子句 `bit(Flags,3)` 守卫 + Task 1 用例 A（零回归）+ round-trip 守护。
- **跨进程时钟**：deadline 用 listener 自己的 monotonic（Task 5），不混用 conn 时钟。
