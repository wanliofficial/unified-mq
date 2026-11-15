# S11-B: CoAP Observe (RFC 7641) + 内核映射 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 `broker_proto_coap` 里实现 RFC 7641 Observe：`Observe=0` 建订阅、`broker_subscription` 驱动通知、`Observe=1`/RST/超时注销，通知可靠性为 NON 至多一次 / CON datagram 级重传。

**Architecture:** 复用既有消息内核（`broker_subscription:subscribe/4` + `attach/3` + `dispatch/2`），不写第二套内核（ADR-031 §D2）。`coap_listener` 直接当 notify pid（唯一 UDP socket owner，用户已定）：内核 dispatch 到它 → 它编码通知并用既有 socket 发出，CON 通知起 `start_timer` 重传。会话是 `{IP,Port,Token}` 进程内表（有界），非持久会话（R-O3）。

**Tech Stack:** Erlang/OTP 28.3.1、rebar3、Common Test（真实 UDP socket，`port => 0`）。

**设计稿：** `docs/design/s11b-coap-observe.md`（已 commit `51b6647`）。

**环境（每个 shell 都要）：** `export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"`；跑全量 CT 前 `rm -rf _build/test/extras`；Python 用 `/usr/bin/python3`；CT 用 `port => 0`；不占用/改动 8080/8083/1883/8081/8082/8084/3478。

**提交规则：** 显式路径 `git add <files>`，禁止 `git add -A`；不 push；scratch（probe/escript/日志）不进提交。

---

## 关键既有事实（实现时对照）

- `broker_subscription:subscribe(ClientId, Filter, QoS, Tenant) -> {ok, SubRef} | {error, term()}`；`attach(ClientId, Pid, Persist) -> {ok, PrevPid}`；`unsubscribe(ClientId, Filter) -> ok`；`detach(ClientId, Pid) -> ok`；`subscriptions(ClientId) -> [subscription()]`（`subscription/0` 是带 `filter` 字段的 map）。dispatch 扇出 `dispatch/2` 给 `attach` 的 pid 发 `{broker_deliver, ClientId, MaxQoS, Matched, {P,O,Msg}}`。**未 attach 的订阅收不到 live push。**
- `coap_codec` 已支持 `observe` option（`option(observe, Msg)`）与 `put_option/3`；`response/3` 造空壳（id=0），用 `put_option(observe, N, Resp)` 加通知序号。
- listener state 在 `#st{}`；`send(IP,Port,Bin,#st{socket})` 是唯一 UDP 写入口；1 s tick 走 `handle_info({timeout,_,refill}, St) -> {noreply, prune(St), {continue, refill}}`；`config(Spec)` 合并 app env 默认 + spec 覆盖。
- 现有 piggybacked ACK 路径 `respond/resource_reply/encode_reply` 已回显 id/token（S11-A），保持不变。

## 文件结构

| 文件 | 责任 | 本阶段动作 |
|---|---|---|
| `apps/broker_proto_coap/src/coap_observe.erl` | **新建**：订阅生命周期纯函数（建/注销/通知消息构造/表上限/序号） | Create |
| `apps/broker_proto_coap/src/coap_listener.erl` | state 加 `observes`/`pending`；`handle_info({broker_deliver,...})`、ACK 匹配重传、tick 清扫 | Modify |
| `apps/broker_proto_coap/src/coap_resource.erl` | `get/3` 分流 `Observe=0/1`；非 GET 忽略 Observe | Modify |
| `apps/broker_proto_coap/src/broker_proto_coap.app.src` | env 加 4 个 observe 键 | Modify |
| `apps/broker_proto_coap/test/coap_observe_SUITE.erl` | 纯函数单测（建/注销/序号/上限/通知构造） | Create |
| `apps/broker_proto_coap/test/coap_resource_SUITE.erl` | observe 注册/注销（mock 注入） | Modify |
| `apps/broker_proto_coap/test/coap_listener_SUITE.erl` | 7 条真实 UDP Observe 用例 | Modify |

**ClientId 固定 `<<"coap">>`**（单 listener、anonymous、单节点，与 `coap_resource` 的 `proto_meta.client => <<"coap">>` 一致）。`Filter` 用 destination 二进制（OQ-5：路径直映射）。

---

## Task 1: env 键 + `coap_observe` 骨架（订阅生命周期纯函数）

**Files:**
- Modify: `apps/broker_proto_coap/src/broker_proto_coap.app.src`
- Create: `apps/broker_proto_coap/src/coap_observe.erl`
- Test: `apps/broker_proto_coap/test/coap_observe_SUITE.erl`

- [ ] **Step 1: 加 env 键**

在 `broker_proto_coap.app.src` 的 env 里，`{recbuf_bytes, 65536}` 之后、`]}` 之前插入：

```erlang
           %% RFC 7641 Observe (S11-B). Bounded relationship table + the
           %% datagram-level CON retransmit timers (ADR-031 §D2/§D3).
           {max_observes, 4096},
           {observe_ack_timeout_ms, 2000},
           {observe_max_retrans, 3},
           {observe_max_age_s, 30}
```

- [ ] **Step 2: 写 `coap_observe.erl`（纯函数，无进程）**

Create `apps/broker_proto_coap/src/coap_observe.erl`:

```erlang
%% @doc RFC 7641 Observe relationship bookkeeping for the CoAP listener.
%%
%% Pure functions only -- no process, no socket. The listener owns the maps
%% (`observes' keyed by {IP,Port,Token}, `pending' keyed by {IP,Port,Token})
%% and calls into here, so the whole subscription/notification policy is
%% unit-testable without a datagram in flight. ADR-031 §D2: the subscription
%% itself lives in `broker_subscription'; this module only mints the filter,
%% holds the per-relationship metadata, and builds the notification message.
-module(coap_observe).

-export([filter/1, register/3, relationship_value/1, next_seq/1,
         notify_message/3, deregister/2]).

-type relationship() :: #{dest := binary(),
                         tenant := binary(),
                         sub_ref := term(),
                         max_age := pos_integer(),
                         seq := non_neg_integer(),
                         last_notified := integer()}.

%% The CoAP-Observe filter: the resource path IS the broker destination
%% (ADR-031 §D4 / OQ-5), so the subscription filter is the destination itself.
-spec filter(binary()) -> binary().
filter(Destination) ->
    Destination.

%% Build a relationship entry from the successful subscribe result. `Seq' starts
%% at 0 (RFC 7641 §3.3 initial value); `LastNotified' is the monotonic ms of the
%% registration (the Max-Age backstop clock).
-spec register(binary(), binary(), pos_integer()) -> relationship().
register(Dest, Tenant, MaxAge) ->
    #{dest => Dest, tenant => Tenant, sub_ref => undefined,
      max_age => MaxAge, seq => 0, last_notified => 0}.

-spec relationship_value(relationship()) -> non_neg_integer().
relationship_value(#{seq := Seq}) ->
    Seq.

%% Advance the Observe sequence, wrapping at 2^24 (RFC 7641 §3.2). Returns
%% {NextSeq, Relationship'}.
-spec next_seq(relationship()) -> {non_neg_integer(), relationship()}.
next_seq(#{seq := Seq} = R) ->
    Next = (Seq + 1) band 16#FFFFFF,
    {Next, R#{seq => Next}}.

%% The notification to send for a dispatched message: a CON or NON response
%% carrying the incremented Observe sequence and the message payload, echoing the
%% request token. `Type' is con | non (never ack -- a notification is a separate
%% message, RFC 7641 §4.1).
-spec notify_message(con | non, non_neg_integer(), #broker_message{}) ->
          coap_codec:message().
notify_message(Type, Seq, #broker_message{payload = Payload}) ->
    coap_codec:put_option(observe, Seq,
                          coap_codec:response(Type, {response, 2, 5}, Payload)).

%% Drop the relationship for a token (Observe=1 / RST). Returns the map without
%% it; the caller unsubscribes with the stored SubRef.
-spec deregister({inet:ip_address(), inet:port_number(), binary()},
                 map()) -> map().
deregister(Key, Observes) ->
    maps:remove(Key, Observes).
```

> 注意：`notify_message/3` 引用 `#broker_message{}`，需在文件顶部加
> `-include_lib("broker_core/include/broker_message.hrl").`

- [ ] **Step 3: 写 `coap_observe_SUITE.erl`（先让纯函数用例失败→实现→通过）**

Create `apps/broker_proto_coap/test/coap_observe_SUITE.erl`:

```erlang
%% @doc Unit tests for the Observe relationship bookkeeping. Pure functions,
%% no datagrams: subscription policy, sequence wrap, and notification shaping.
-module(coap_observe_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("broker_core/include/broker_message.hrl").

all() ->
    [filter_is_the_destination,
     register_seeds_seq_zero,
     next_seq_wraps_at_2pow24,
     notify_message_carries_sequence_and_payload,
     deregister_removes_the_token].

filter_is_the_destination(_) ->
    ?assertEqual(<<"a/b/c">>, coap_observe:filter(<<"a/b/c">>)).

register_seeds_seq_zero(_) ->
    R = coap_observe:register(<<"a/b">>, <<"t0">>, 30),
    ?assertEqual(0, coap_observe:relationship_value(R)),
    ?assertEqual(<<"a/b">>, maps:get(dest, R)).

next_seq_wraps_at_2pow24(_) ->
    R0 = coap_observe:register(<<"d">>, <<"t0">>, 30),
    R1 = R0#{seq => 16#FFFFFF},
    {16#000000, R2} = coap_observe:next_seq(R1),
    ?assertEqual(16#000000, maps:get(seq, R2)).

notify_message_carries_sequence_and_payload(_) ->
    Msg = #broker_message{payload = <<"hello">>},
    Non = coap_observe:notify_message(non, 7, Msg),
    ?assertEqual(non, coap_codec:type(Non)),
    ?assertEqual(7, coap_codec:option(observe, Non)),
    ?assertEqual(<<"hello">>, coap_codec:payload(Non)),
    Con = coap_observe:notify_message(con, 3, Msg),
    ?assertEqual(con, coap_codec:type(Con)),
    ?assertEqual(3, coap_codec:option(observe, Con)).

deregister_removes_the_token(_) ->
    Key = {{127,0,0,1}, 5000, <<"tok">>},
    Table = maps:put(Key, coap_observe:register(<<"d">>, <<"t0">>, 30), #{}),
    ?assert(maps:is_key(Key, Table)),
    Table2 = coap_observe:deregister(Key, Table),
    ?assertNot(maps:is_key(Key, Table2)).

%%%_* Fixtures (standard shape; this suite needs no kernel app) -----------
init_per_suite(Config) -> Config.
end_per_suite(_Config) -> ok.
init_per_testcase(_Case, Config) -> Config.
end_per_testcase(_Case, _Config) -> ok.
```

- [ ] **Step 4: 跑新套件，确认通过**

Run: `export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"; cd /home/wanli/workspace/unified-mq && rm -rf _build/test/extras && rebar3 ct --suite=apps/broker_proto_coap/test/coap_observe_SUITE.erl 2>&1 | tail -4`
Expected: `All 5 tests passed.`

- [ ] **Step 5: 提交**

```bash
cd /home/wanli/workspace/unified-mq
git add apps/broker_proto_coap/src/broker_proto_coap.app.src apps/broker_proto_coap/src/coap_observe.erl apps/broker_proto_coap/test/coap_observe_SUITE.erl
git commit -q -m "feat(broker_proto_coap): S11-B coap_observe 订阅生命周期纯函数 + env 键"
```

---

## Task 2: listener state + 内核 attach/subscribe + ACK 取消重传

**Files:**
- Modify: `apps/broker_proto_coap/src/coap_listener.erl`
- Test: `apps/broker_proto_coap/test/coap_listener_SUITE.erl`（本任务加 1 条 smoke）

**契约（本任务落地，后续任务在其上扩展）：**
- `register_observe(IP,Port,Token,Req,Cfg,St) -> St | {error, full, St}`：在 `coap_resource:get/3` 里 `Observe=0` 且资源存在（2.05）时调用；先 `broker_subscription:subscribe(<<"coap">>, Dest, 0, Tenant)` → `attach(<<"coap">>, self(), false)`，再写 `observes` 表。表满（`max_observes`）返回 `{error, full}`（listener 回 4.03）。
- `handle_info({broker_deliver, <<"coap">>, _, _, {_,_,Msg}}, St)`：遍历 `observes`，凡 `dest =:= Msg.destination` 的关系 → 发通知；请求是 CON 则起 `observe_ack` 重传 timer 存 `pending`，NON 只发一次。
- `handle_info({udp,_,IP,Port,Msg}, ...)`：解码后若 `type=ack` 且 `{IP,Port,token}` 命中 `pending` → 取消 timer、从 `pending` 移除、推进该关系 seq。
- `handle_info({timeout,_,observe_ack}, St)`：重传该 pending 通知并计数；超过 `observe_max_retrans` → 放弃，`unsubscribe`/`detach` 并清 `observes`/`pending`。
- `prune/1` 增：`observes` 按 `max_age`（`last_notified` 超 `observe_max_age_s*1000`）清扫；超 `max_observes` 上限时拒绝新注册（在 register 处判）。

- [ ] **Step 1: 加 state 字段与 config 键**

`coap_listener.erl` 的 `-record(st, {...})` 末尾 `block1 = #{} :: map()` 之后加：

```erlang
    %% RFC 7641 Observe relationships: {IP,Port,Token} -> relationship()
    %% (a coap_observe:relationship()). Bounded by `max_observes'.
    observes = #{} :: map(),
    %% Un-acked CON notifications awaiting an ACK: {IP,Port,Token} ->
    %% {Msg, RetransLeft, TimerRef}. Reused by the dedup/Block1-free path.
    pending = #{} :: map()
```

`config(Spec)` 的 `Defaults` map 里 `recbuf_bytes => ...` 之后加：

```erlang
                 max_observes => env(max_observes, 4096),
                 observe_ack_timeout_ms => env(observe_ack_timeout_ms, 2000),
                 observe_max_retrans => env(observe_max_retrans, 3),
                 observe_max_age_s => env(observe_max_age_s, 30)
```

- [ ] **Step 2: 加 register/notify/deregister 函数（供 coap_resource 与 handle_info 调用）**

在 `%%%_* Bounded tables` 段之前插入：

```erlang
%%%_* Observe (RFC 7641) ----------------------------------------------------

%% Called by coap_resource:get/3 when it has a 2.05 and an Observe=0 request.
%% Registers the subscription in the kernel and records the relationship here.
%% Returns St, or {error, full, St} when the bounded table is at capacity.
register_observe(IP, Port, Token, Req, Cfg,
                 #st{observes = Obs} = St) ->
    Dest = coap_codec:destination(Req),
    Tenant = broker_security:default_tenant(),
    MaxAge = coap_codec:option(max_age, Req, 30),
    Cap = maps:get(max_observes, Cfg, 4096),
    case maps:size(Obs) >= Cap of
        true ->
            {error, full, St};
        false ->
            case broker_subscription:subscribe(<<"coap">>,
                                               coap_observe:filter(Dest), 0,
                                               Tenant) of
                {ok, SubRef} ->
                    ok = broker_subscription:attach(<<"coap">>, self(), false),
                    Rel = coap_observe:register(Dest, Tenant, MaxAge),
                    Key = {IP, Port, Token},
                    St#st{observes = maps:put(Key, Rel#{sub_ref => SubRef}, Obs)};
                {error, _} ->
                    St
            end
    end.

%% Kernel pushed a message that matches an Observe relationship. Build and send
%% the notification(s); CON requests get a retransmit timer, NON ones fire once.
dispatch_notifications(Msg, #st{observes = Obs, cfg = Cfg} = St) ->
    lists:foldl(
      fun({Key, Rel}, Acc) ->
              case maps:get(dest, Rel) =:= Msg#broker_message.destination of
                  true -> notify_one(Key, Rel, Msg, Cfg, Acc);
                  false -> Acc
              end
      end, St, maps:to_list(Obs)).

notify_one({IP, Port, _Token} = Key, Rel, Msg, Cfg,
           #st{pending = Pend} = St) ->
    {Seq, Rel1} = coap_observe:next_seq(Rel),
    Token = element(3, Key),
    Notify = coap_observe:notify_message(con, Seq, Msg),
    Bytes = coap_codec:encode(Notify#{token => Token, id => next_id()}),
    _ = gen_udp:send(St#st.socket, IP, Port, Bytes),
    Timeout = maps:get(observe_ack_timeout_ms, Cfg, 2000),
    Ref = erlang:start_timer(Timeout, self(), observe_ack),
    St#st{observes = maps:put(Key, Rel1, St#st.observes),
          pending = maps:put(Key, {Bytes, maps:get(observe_max_retrans, Cfg, 3), Ref},
                             Pend)}.

next_id() -> erlang:unique_integer([positive]) rem 65536.
```

> 说明：本任务的 `notify_one` 一律发 **CON**（先做 CON 路径，NON 在 Task 3 按请求类型分流）。`dispatch_notifications/2` 由 Task 3 的 `handle_info({broker_deliver,...})` 调用；本任务先把函数写好，Task 3 接 handle_info。

- [ ] **Step 3: 加 handle_info 子句（broker_deliver + ACK 取消 + observe_ack 重传）**

在 `handle_info({udp_error, ...})` 之前插入：

```erlang
%% The kernel delivered a message to an attached subscription (ADR-031 §D2:
%% the listener is the notify pid). Turn it into Observe notifications.
handle_info({broker_deliver, <<"coap">>, _MaxQoS, _Matched, {_P, _O, Msg}},
            #st{} = St) ->
    {noreply, dispatch_notifications(Msg, St)};

%% An ACK for a CON notification cancels its retransmit and advances the seq.
handle_info({udp, _Socket, IP, Port, Bin}, #st{pending = Pend} = St0) ->
    try coap_codec:decode(Bin) of
        {ok, #{type := ack, token := Token} = _Msg} ->
            Key = {IP, Port, Token},
            case maps:find(Key, Pend) of
                {ok, {_Bytes, _Left, Ref}} ->
                    _ = erlang:cancel_timer(Ref),
                    St = St0#st{pending = maps:remove(Key, Pend)},
                    {noreply, St};
                error ->
                    St1 = on_datagram(IP, Port, Bin, St0),
                    {noreply, St1}
            end;
        {ok, _Msg} ->
            {noreply, on_datagram(IP, Port, Bin, St0)};
        {error, _} ->
            {noreply, on_datagram(IP, Port, Bin, St0)}
    catch
        _:_ -> {noreply, on_datagram(IP, Port, Bin, St0)}
    end;
```

> 关键：把原来的 `handle_info({udp, _Socket, IP, Port, Bin}, #st{} = St0) -> on_datagram` 子句**替换**为上面这个（它先判 ACK-命中-pending，未命中再走 `on_datagram`）。`on_datagram` 收到的 datagram 若本身是 ACK，会在 `dispatch` 里因 `coap_codec:type` 非 con 而走 respond（产生一个 NON 响应）——这不对。**修正**：`on_datagram` 顶部对 `type=ack/rst` 直接原样丢弃（它们是来取消通知/重置的，不是请求）。在 Task 3 Step 1 一并处理（见下）。

- [ ] **Step 4: `on_datagram` 忽略 ack/rst（它们不是请求）**

`dispatch/5` 里 `handle_message` 的 `case coap_codec:type(Msg) of con -> ...; _Non -> respond...` 会把 ack/rst 也当请求回一个响应。改为在 `handle_message` 顶部：

```erlang
handle_message(_IP, _Port, Msg, _Cfg, St) ->
    case coap_codec:type(Msg) of
        T when T =:= ack; T =:= rst ->
            %% An ACK cancels a pending CON notification (handled in the pending
            %% branch before we get here); an RST ends the relationship. Neither
            %% is a request, so no response is owed.
            St;
        con ->
            ... (unchanged)
```

- [ ] **Step 5: `prune/1` 清扫 observes（max_age 兜底）**

`prune(#st{dedup=..., block1=..., cfg=Cfg} = St)` 加 `observes` 清扫：超过 `observe_max_age_s*1000` 未更新的关系连同其 `sub_ref` 一起 `unsubscribe`。因 `prune/1` 是纯函数（不能 side-effect unsubscribe），改为：先收集要过期的 Key，在 `handle_info({timeout,_,refill}, St)` 里做 side-effect 清理再调 `prune`。具体：新增 `sweep_observes(St) -> St`（可副作用），在 refill tick 调用：

```erlang
handle_info({timeout, _Ref, refill}, St) ->
    {noreply, sweep_observes(prune(St)), {continue, refill}};

%% Drop Observe relationships silent longer than Max-Age: the client should
%% re-Observe. Side-effecting (unsubscribes) so it runs on the tick, not in the
%% pure prune/1.
sweep_observes(#st{observes = Obs, cfg = Cfg} = St) ->
    Cutoff = erlang:monotonic_time(millisecond)
             - maps:get(observe_max_age_s, Cfg, 30) * 1000,
    lists:foldl(
      fun({Key, #{sub_ref := Ref, last_notified := At}}, Acc) when At < Cutoff ->
              _ = (catch broker_subscription:unsubscribe(<<"coap">>, coap_observe:filter(maps:get(dest, ...)))),
              ... ;
```

> 实现提示：`last_notified` 目前 register 时置 0。为让 max_age 兜底有实际语义，在 `notify_one` 里把 `last_notified` 更新为当前 monotonic ms。`unsubscribe` 需要 Filter——从 relationship 的 `dest` 经 `coap_observe:filter/1` 得到。**完整 `sweep_observes/1` 由实现者按此补全**，测试用例 `observe_max_age_expires_relationship`（Task 3）钉住行为。

- [ ] **Step 6: 写 CON 通知 smoke 用例并跑**

在 `coap_listener_SUITE.erl` 的 `all/0` 加 `observe_con_notifies_after_put`，实现见 Task 3 Step 2（本任务先加最小版：PUT 一条 → CON Observe=0 → 收到 2.05 piggyback → 再 PUT 同 dest → 收到 CON 通知带递增 observe）。跑全套确认无回归。

Run: `export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"; cd /home/wanli/workspace/unified-mq && rm -rf _build/test/extras && rebar3 ct --suite=apps/broker_proto_coap/test/coap_listener_SUITE.erl 2>&1 | tail -5`
Expected: `All <N> tests passed.`（原 7 + 新增）

- [ ] **Step 7: 提交**

```bash
cd /home/wanli/workspace/unified-mq
git add apps/broker_proto_coap/src/coap_listener.erl apps/broker_proto_coap/test/coap_listener_SUITE.erl
git commit -q -m "feat(broker_proto_coap): S11-B listener 兼 notify pid——订阅/attach/CON 重传/ACK 取消"
```

---

## Task 3: NON/Observe=1/RST 分支 + observe 套件全量用例

**Files:**
- Modify: `apps/broker_proto_coap/src/coap_listener.erl`（`notify_one` 按请求类型发 CON/NON）
- Modify: `apps/broker_proto_coap/src/coap_resource.erl`（`get/3` 分流 Observe=0/1）
- Test: `apps/broker_proto_coap/test/coap_listener_SUITE.erl`（+6 用例）
- Test: `apps/broker_proto_coap/test/coap_resource_SUITE.erl`（+2 用例，mock 注入）

- [ ] **Step 1: `notify_one` 按关系的请求类型发 CON/NON**

relationship 里需记住请求的 `type`（con/non）以决定通知类型。在 `register_observe` 写 relationship 时加 `req_type => coap_codec:type(Req)`；`notify_one` 用 `maps:get(req_type, Rel, con)` 选 `coap_observe:notify_message(Type, Seq, Msg)`，且仅 `Type=:= con` 时起 `observe_ack` timer 与 `pending`；NON 直接发不入 `pending`。

- [ ] **Step 2: `coap_resource:get/3` 分流 Observe=0/1；非 GET 忽略 Observe**

`get/3` 改为：

```erlang
get(Req, Cfg, Ctx) ->
    case coap_codec:destination(Req) of
        <<".well-known/core">> ->
            Body = well_known(Cfg),
            {reply, {response, 2, 5},
             [{content_format, coap_codec:content_format_id(link_format)}], Body};
        Destination ->
            case coap_codec:option(observe, Req) of
                undefined ->
                    read_resource(Destination, Ctx);
                0 ->
                    %% Register the subscription and answer with the initial
                    %% representation. The listener owns the socket and the
                    %% relationship table, so registration happens there (it can
                    %% answer 4.03 if the table is full); here we just read.
                    read_resource(Destination, Ctx);
                1 ->
                    %% Deregister: the listener drops the relationship. Answer
                    %% 2.01 "Changed" (RFC 7641 §3.5 - deregister is benign).
                    {reply, {response, 2, 1}, [], <<>>};
                _ObsVal ->
                    read_resource(Destination, Ctx)
            end
    end.
```

> 注册/注销的**副作用**（subscribe/attach/unsubscribe）放在 listener，因为 `coap_resource` 是纯函数。落地方式：listener 在 `respond/5` 之前，若 `code=method,get` 且带 `Observe=0/1`，先调 `register_observe`/`deregister_observe`。在 `respond/5` 的 `{single, Req, St1}` / `{done, Req, St1}` 分支里，resource 返回 2.05 时调 `register_observe(IP,Port,Token,Req,Cfg,St1)`；返回其他码时不注册。`Observe=1` 时 listener 调 `deregister_observe` 并仍回 2.01。
>
> **本步只改 `coap_resource:get/3` 的返回码语义**（Observe=1 → 2.01），注册副作用在 Step 3 由 listener 承担。非 GET 的 `handle/3` 不改（Observe 只作用在 GET）。

- [ ] **Step 3: listener 承担注册/注销副作用**

在 `respond/5` 里，拿到 `resource_reply` 之前，若请求是 GET 且带 Observe：

```erlang
respond(IP, Port, Msg, Cfg, St) ->
    case coap_codec:observe_action(Msg) of   %% 新增 codec 辅助或直接内联判断
        register ->
            case block1_stream(IP, Port, Msg, Cfg, St) of
                {single, Req, St1} ->
                    {Reply, St2} = resource_reply_observe(Req, Cfg, {IP,Port}, St1, register),
                    {Reply, St2};
                {done, Req, St1} ->
                    {Reply, St2} = resource_reply_observe(Req, Cfg, {IP,Port}, St1, register),
                    {Reply, St2};
                Other -> ...
            end;
        deregister ->
            St1 = deregister_observe(IP, Port, coap_codec:token(Msg), St),
            {Reply, St2} = resource_reply(coap_codec:remove_option(observe, Msg), Cfg, {IP,Port}),
            {coap_codec:encode(Reply), St2};
        none ->
            ... (existing block1_stream + resource_reply)
    end.
```

`resource_reply_observe/5` 在 `register_observe` 返回 `{error, full, St)` 时把回复改成 `4.03` 且不加 observe；否则正常 `resource_reply`（piggybacked 2.05 已带初始表示，observe 序号由通知路径从 0 起）。`deregister_observe/4` = `unsubscribe(<<"coap">>, filter(dest))` + `detach` + `observes` 移除该 Key。

> `coap_codec:observe_action/1` 若不想加，直接内联：`case {coap_codec:type(Msg), coap_codec:option(observe, Msg)} of {_, 0} -> register; {_, 1} -> deregister; _ -> none end`（且仅对 `code =:= {method,get}`）。

- [ ] **Step 4: 加 6 条 listener 用例**

在 `coap_listener_SUITE.erl` `all/0` 追加：

```erlang
     observe_con_notifies_and_acks,
     observe_con_retries_then_gives_up,
     observe_non_is_at_most_once,
     observe_deregister_with_observe_1,
     observe_rst_deregisters,
     observe_bounded_table_refuses,
     observe_acl_denied,
     observe_max_age_expires_relationship
```

用例实现（真实 UDP；token 显式；用 `expect`/`refute` 既有助手）：

```erlang
%% CON Observe=0 → piggybacked 2.05 (observe:0) + 初始表示；再 publish →
%% CON 通知带 observe:1；回一个同 token ACK → 无重传；再 publish → 第二个通知。
observe_con_notifies_and_acks(Config) ->
    Port = ?config(port, Config),
    S = open(),
    Token = <<"ob1">>,
    Req = (request(con, get, 1, [{uri_path, <<"o">>}], <<"init">>))#{token => Token},
    send(S, Port, Req),
    #{type := ack, code := {response,2,5}, token := Token} = expect(S, ack, {response,2,5}),
    %% Publish a new value.
    send(S, Port, request(con, put, 2, [{uri_path, <<"o">>}], <<"v1">>)),
    _ = expect(S, ack, {response,2,4}),
    %% A CON notification with the next observe sequence arrives.
    Note = expect(S, non, {response,2,5}),
    ?assertEqual(Token, coap_codec:token(Note)),
    ?assertEqual(1, coap_codec:option(observe, Note)),
    %% ACK it: no retransmit.
    send(S, Port, coap_codec:ack(Note)),
    refute(S, non, {response,2,5}, 400),
    close(S).

%% CON notification not ACKed → retransmitted after the timeout, then given up.
observe_con_retries_then_gives_up(Config) ->
    Port = ?config(port, Config),
    S = open(),
    send(S, Port, request(con, get, 1, [{uri_path, <<"r">>}])),
    #{code := {response,2,5}} = expect(S, ack, {response,2,5}),
    send(S, Port, request(con, put, 2, [{uri_path, <<"r">>}], <<"x">>)),
    _ = expect(S, ack, {response,2,4}),
    N1 = expect(S, non, {response,2,5}),
    %% Do NOT ack. With observe_ack_timeout_ms small (pinned below) it retransmits.
    N2 = expect(S, non, {response,2,5}),
    ?assertEqual(coap_codec:id(N1), coap_codec:id(N2)),
    close(S).

%% NON Observe=0 → at most one notification, no retransmit.
observe_non_is_at_most_once(Config) ->
    Port = ?config(port, Config),
    S = open(),
    send(S, Port, request(non, get, 1, [{uri_path, <<"n">>}])),
    #{type := non, code := {response,2,5}} = expect(S, non, {response,2,5}),
    send(S, Port, request(non, put, 2, [{uri_path, <<"n">>}], <<"y">>)),
    Note = expect(S, non, {response,2,5}),
    ?assertEqual(1, coap_codec:option(observe, Note)),
    refute(S, non, {response,2,5}, 400),
    close(S).

observe_deregister_with_observe_1(Config) ->
    Port = ?config(port, Config),
    S = open(),
    send(S, Port, request(con, get, 1, [{uri_path, <<"d1">>}])),
    #{code := {response,2,5}} = expect(S, ack, {response,2,5}),
    send(S, Port, request(con, put, 2, [{uri_path, <<"d1">>}], <<"a">>)),
    _ = expect(S, ack, {response,2,4}),
    _ = expect(S, non, {response,2,5}),
    %% Deregister.
    send(S, Port, (request(con, get, 3, [{uri_path, <<"d1">>}, {observe, 1}])) ),
    #{code := {response,2,1}} = expect(S, ack, {response,2,1}),
    send(S, Port, request(con, put, 4, [{uri_path, <<"d1">>}], <<"b">>)),
    _ = expect(S, ack, {response,2,4}),
    refute(S, non, {response,2,5}, 400),
    close(S).

observe_rst_deregisters(Config) ->
    Port = ?config(port, Config),
    S = open(),
    send(S, Port, request(con, get, 1, [{uri_path, <<"rst1">>}])),
    #{code := {response,2,5}} = expect(S, ack, {response,2,5}),
    send(S, Port, request(con, put, 2, [{uri_path, <<"rst1">>}], <<"a">>)),
    _ = expect(S, ack, {response,2,4}),
    _ = expect(S, non, {response,2,5}),
    %% RST with the same token ends the relationship.
    send(S, Port, (coap_codec:request(rst, empty, 3))#{token => <<"rsttok">>}),
    ok,
    send(S, Port, request(con, put, 4, [{uri_path, <<"rst1">>}], <<"b">>)),
    _ = expect(S, ack, {response,2,4}),
    refute(S, non, {response,2,5}, 400),
    close(S).

observe_bounded_table_refuses(Config) ->
    Port = ?config(port, Config),
    S = open(),
    ok = application:set_env(broker_proto_coap, max_observes, 1),
    T1 = <<"t1">>, T2 = <<"t2">>,
    send(S, Port, (request(con, get, 1, [{uri_path, <<"b1">>}]))#{token => T1}),
    #{code := {response,2,5}} = expect(S, ack, {response,2,5}),
    send(S, Port, (request(con, get, 2, [{uri_path, <<"b2">>}]))#{token => T2}),
    #{code := {response,4,3}} = expect(S, ack, {response,4,3}),
    close(S).

observe_acl_denied(Config) ->
    Port = ?config(port, Config),
    S = open(),
    ok = application:set_env(broker_security, acl, all),
    ok = application:set_env(broker_security, require_auth, [<<"coap">>]),
    send(S, Port, request(con, get, 1, [{uri_path, <<"x">>}])),
    #{code := {response,4,3}} = expect(S, ack, {response,4,3}),
    close(S).

observe_max_age_expires_relationship(Config) ->
    Port = ?config(port, Config),
    S = open(),
    ok = application:set_env(broker_proto_coap, observe_max_age_s, 1),
    send(S, Port, request(con, get, 1, [{uri_path, <<"mx">>}])),
    #{code := {response,2,5}} = expect(S, ack, {response,2,5}),
    send(S, Port, request(con, put, 2, [{uri_path, <<"mx">>}], <<"a">>)),
    _ = expect(S, ack, {response,2,4}),
    _ = expect(S, non, {response,2,5}),
    %% After max_age the relationship is swept (unsubscribe); a later publish
    %% yields no notification.
    timer:sleep(1300),
    send(S, Port, request(con, put, 3, [{uri_path, <<"mx">>}], <<"b">>)),
    _ = expect(S, ack, {response,2,4}),
    refute(S, non, {response,2,5}, 400),
    close(S).
```

> `observe_con_retries_then_gives_up` 需在 `case_env/2` 里为它把 `observe_ack_timeout_ms` 设为 300、`observe_max_retrans` 设为 2（加一条 `case_env(observe_con_retries_then_gives_up, _) -> set both`）。`observe_bounded_table_refuses` 在 `case_env` 里设 `max_observes=1`；`end_per_testcase` 的 `restore_env` 需保存/恢复这 3 个键（扩展 `Saved` map）。

- [ ] **Step 5: 跑全套 listener**

Run: `export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"; cd /home/wanli/workspace/unified-mq && rm -rf _build/test/extras && rebar3 ct --suite=apps/broker_proto_coap/test/coap_listener_SUITE.erl 2>&1 | tail -5`
Expected: `All 15 tests passed.`（原 7 + 8 新）

- [ ] **Step 6: 加 resource 层 2 条 mock 注入用例**

在 `coap_resource_SUITE.erl` 加（用既有 mock 注入路径，参考该文件现有 `put`/`get` 测试的 fixture）：

```erlang
observe_1_on_get_answers_2_01(_) ->
    Req = request(con, get, 9, [{uri_path, <<"z">>}, {observe, 1}]),
    {reply, {response, 2, 1}, _, _} = coap_resource:handle(Req, ?CFG, ?CTX).

observe_0_on_get_still_reads_retained(_) ->
    put(<<"z">>, <<"v">>),
    Req = request(con, get, 10, [{uri_path, <<"z">>}, {observe, 0}]),
    {reply, {response, 2, 5}, _, <<"v">>} = coap_resource:handle(Req, ?CFG, ?CTX).
```

Run: `... rebar3 ct --suite=apps/broker_proto_coap/test/coap_resource_SUITE.erl 2>&1 | tail -4`
Expected: `All 15 tests passed.`（原 13 + 2）

- [ ] **Step 7: 提交**

```bash
cd /home/wanli/workspace/unified-mq
git add apps/broker_proto_coap/src/coap_listener.erl apps/broker_proto_coap/src/coap_resource.erl apps/broker_proto_coap/test/coap_listener_SUITE.erl apps/broker_proto_coap/test/coap_resource_SUITE.erl
git commit -q -m "feat(broker_proto_coap): S11-B Observe=0/1 分流 + NON 至多一次 + RST/注销/max_age/有界表/ACL 用例"
```

---

## Task 4: 全量验证 + 文档同步 + 提交

**Files:** 无代码改动（除非 dialyzer/xref 指出问题）

- [ ] **Step 1: 编译 + CoAP 四套件**

Run: `export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"; cd /home/wanli/workspace/unified-mq && rm -rf _build/test/extras && rebar3 ct --suite=apps/broker_proto_coap/test/coap_observe_SUITE.erl,apps/broker_proto_coap/test/coap_codec_SUITE.erl,apps/broker_proto_coap/test/coap_resource_SUITE.erl,apps/broker_proto_coap/test/coap_listener_SUITE.erl 2>&1 | tail -4`
Expected: `All 94 tests passed.`（48 + 15 + 15 + 5 = 83；若 resource/listener 数与计划略有出入以实跑为准）

- [ ] **Step 2: 全量 CT**

Run: `export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"; cd /home/wanli/workspace/unified-mq && rm -rf _build/test/extras && timeout 3000 rebar3 ct > /tmp/opencode/ct_s11b.log 2>&1; echo exit=$?; tail -3 /tmp/opencode/ct_s11b.log`
Expected: `All <N> tests passed.`，N = 1324 + 新增（约 83），零 FAILED。

- [ ] **Step 3: dialyzer + xref**

Run: `export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"; cd /home/wanli/workspace/unified-mq && timeout 900 rebar3 dialyzer 2>&1 | tail -3; echo "=== xref ==="; timeout 600 rebar3 xref 2>&1 | grep -c Warning`
Expected: dialyzer 0 warning；xref 计数 = 302 + `coap_observe` 导出未用项（若有新增 unused-export，如实写入文档）。

- [ ] **Step 4: release 装配**

Run: `export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"; cd /home/wanli/workspace/unified-mq && timeout 600 rebar3 release 2>&1 | tail -3`
Expected: `Release successfully assembled`。

- [ ] **Step 5: 文档同步**

按 `docs/design/s11b-coap-observe.md` §9 与仓库既有口径，更新：
- `docs/protocol-compatibility.md` §4.24：能力矩阵 CoAP 行追加 Observe（已支持：注册/通知/取消 + CON datagram 级重传 + NON 至多一次 + max_age）；§4.24.4 加「无跨节点崩溃恢复、通知非 QoS1、无 Block2、无 OSCORE」；§4.24 证据计数改新数；「本套件抓到的真实缺陷」追加 Observe 相关（如 notify 需入 observes 表、ACK 匹配 pending、max_age 兜底）；头部「最近一次追加」改 2026-11-15 S11-B。
- `docs/architecture.md` §3.18：observes/pending 两张表 + listener 兼 notify pid 的说明；§4.1 启动实测不变。
- `docs/roadmap.md` §13.1 S11-B 行改「🟡 已交付（2026-11-15）」+ 证据；§13.2 Gate S11 勾选项对应更新；状态口径改 S11-B 已交付、S11-C 未开工。
- `docs/security.md`：新增 §12.5 S11-B 未实现项（跨节点恢复、Block2、OSCORE、通知非 QoS1、observe_max_age 兜底）。
- `docs/adr/031-coap-frontend-boundary.md`「落地偏差记录」追加 S11-B 段（listener 兼 notify pid、ClientId=<<"coap">>、CON 通知一律入 pending、max_age 兜底）——冻结文本不改。
- `README.md` / `progress.md`：计数改新数，追加 S11-B 交付段。
- 提交：`git add docs/protocol-compatibility.md docs/architecture.md docs/roadmap.md docs/security.md docs/adr/031-coap-frontend-boundary.md README.md progress.md && git commit -q -m "docs(s11b): S11-B Observe 交付——能力矩阵/架构/roadmap/security/ADR 同步"`

- [ ] **Step 6: 最终提交确认**

Run: `cd /home/wanli/workspace/unified-mq && git log --oneline -6 && git status --short`
Expected：S11-B 相关提交齐全，工作树干净（`docs/compatibility/` 若存在仍不提交）。

---

## Self-Review 记录

- **Spec 覆盖**：设计稿 §2 进程模型→Task2/3；§3 会话归属→Task2（observes 表 + max_age sweep）；§4 可靠性分档→Task2（CON 重传）/Task3（NON 至多一次）；§5 数据流→Task2/3 各分支；§6 组件清单→Task1/2/3；§7 测试→Task1/3 逐条；§8 ADR 偏差→Task4 Step5；§9 收口→Task4。
- **类型一致**：`relationship()` 字段（dest/tenant/sub_ref/max_age/seq/last_notified + req_type）在 coap_observe 与 listener 一致；ClientId 恒 `<<"coap">>`；`coap_observe:filter/1` 在 register 与 sweep/deregister 复用。
- **已知坑**：`sweep_observes/1` 因需 side-effect（unsubscribe）放在 tick 而非纯 `prune/1`——已在 Task2 Step5 标注由实现者补全，并有 `observe_max_age_expires_relationship` 钉住。`notify pid = listener` 意味着 `attach(<<"coap">>, self(), false)` 的 self 是 listener 进程。
