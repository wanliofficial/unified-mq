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

## ⚠️ Task 1 已交付，本文档 Task 1 的代码片段是历史快照

Task 1 已实现并提交（`d1dbc7e` → `40aa366` → `58bd7dc`），规格复审 + 代码质量复审通过。质量复审提出 4 项重要问题并已全部落地，因此 **Task 2/3 请以下面这份最终 API 为准**，Task 1 小节里的旧代码片段（`register/3`、`relationship_value/1`、`next_seq/1`、`notify_message/3`、`deregister/2 -> map()`）**不要照抄**。

### `coap_observe` 最终 API（8 个导出）

```erlang
-export([filter/1, register/5, seq/1, next_seq/2,
         sweep_window_ms/2, expired/3, notify_message/4, deregister/2]).
-export_type([relationship/0, key/0, observes/0]).

-type key()      :: {inet:ip_address(), inet:port_number(), binary()}.
-type relationship() :: #{dest := binary(),
                         tenant := binary(),
                         sub_ref := broker_subscription:sub_ref(),
                         max_age := non_neg_integer(),   %% 客户端 Max-Age（秒），0 合法
                         seq := non_neg_integer(),
                         last_notified := integer()}.
-type observes() :: #{key() => relationship()}.
```

| 函数 | 语义 |
|---|---|
| `filter(Dest) -> Dest` | 订阅 filter 就是 destination 本身（ADR-031 §D4 / OQ-5）。 |
| `register(Dest, Tenant, SubRef, MaxAge, NowMs) -> relationship()` | `seq => 0`，`last_notified => NowMs`。时钟是**入参**（本 VM 上 `erlang:monotonic_time(millisecond)` 为负，哨兵值 0 会让从未通知过的关系永远躲过 max_age 清扫）。 |
| `seq(Rel) -> non_neg_integer()` | 关系的当前序号。**初始 piggybacked 2.05 用它**（RFC 7641 §3.3 初始值为 0，Task 3 断言初始通知带 `observe:0`）。 |
| `next_seq(Rel, NowMs) -> {NextSeq, Rel'}` | 2^24 回绕；**同时**把 `last_notified` 刷成 `NowMs`（由构造保证「静默但存活」的关系不会看起来过期）。 |
| `sweep_window_ms(ClientAgeS, CapS) -> Millis` | `min(max(ClientAgeS,1), max(CapS,1)) * 1000`。下限 1 s（客户端 `Max-Age: 0` 不能让它下一个 tick 就被清掉），上限取 `observe_max_age_s` 配置（防止 `Max-Age: 604800` 钉住表槽一周）。 |
| `expired(Rel, NowMs, CapS) -> boolean()` | `(NowMs - last_notified) >= sweep_window_ms(...)`。`NowMs` 与 `last_notified` 同源，负纪元不会反转比较。 |
| `notify_message(Type, Seq, Token, #broker_message{}) -> message()` | `{response,2,5}`，type = `con\|non`，`observe = Seq`，`token = Token`，payload = 消息 payload；`proto_meta.content_format` 非 `undefined` 时镜像成 `{content_format, F}` 选项（与 `coap_resource:content_format_options/1` 同规则）；**不设 `id`**（listener 发送时自己铸 Message ID）。 |
| `deregister(Key, Observes) -> {Rel \| undefined, Observes'}` | `maps:take` 语义：调用方拿回关系（及其 `sub_ref`）。`broker_subscription` 是按 `(ClientId, Filter)` 寻址的，`sub_ref` 只用于断言与 `broker_subscription:subscriptions/1`。 |

### 状态与测试现状

- `coap_observe_SUITE`：**10 例全绿**（filter / register（dest·tenant·sub_ref·max_age·seq 0·时钟）/ next_seq 2^24 回绕 / next_seq 刷时钟 / seq 取值 / sweep 窗口钳制 / expired 边界 / notify（type·code·token·seq·payload·无 content_format）/ notify 镜像 content_format / deregister 返回被删关系）。
- 4 个 env 键已就位：`max_observes 4096`、`observe_ack_timeout_ms 2000`、`observe_max_retrans 3`、`observe_max_age_s 30`。
- `rebar3 dialyzer` 0 warning；`rebar3 xref` 310（302 基线 + 8 个尚未接线的 coap_observe 导出，属分阶段任务的正常预期）。
- **max_age 兜底语义已定稿**（设计稿 §3 + ADR-031 落地偏差记录同步）：每个关系用自己的 Max-Age，钳制在 `[1s, observe_max_age_s]`；清扫在 1 s tick 里做（有 unsubscribe 副作用，不进纯 `prune/1`）。`observe_max_age_s` 因此是**上限**而非默认值。

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
- `register_observe(IP,Port,Token,Req,Cfg,St) -> {St, Rel} | {St, undefined}`：在 `respond/5` 里 GET 且 `Observe=0` 且资源存在（2.05）时调用；先 `broker_subscription:subscribe(<<"coap">>, coap_observe:filter(Dest), 0, Tenant)` → `attach(<<"coap">>, self(), false)`，再写 `observes` 表（关系带 `req_type`）。表满（`max_observes`）或 subscribe 失败 → 返回 `{St, undefined}`（respond 回 4.03）。
- `deregister_observe(IP,Port,Token,St) -> St`：`Observe=1` 或 RST 时调用；`unsubscribe` + `detach` + 从 `observes`/`pending` 移除。始终成功。
- `handle_info({broker_deliver, <<"coap">>, _, _, {_,_,Msg}}, St)`：调 `dispatch_notifications/2`；关系 `dest =:= Msg.destination` 的按 `req_type` 发 CON（入 `pending` 重传）/ NON（只发一次）。
- `handle_info({udp,_,IP,Port,Bin}, ...)`：解码后若 `type=ack` 且 `{IP,Port,token}` 命中 `pending` → 取消 timer、从 `pending` 移除（seq 已由 `notify_one` 在推进时推进）；未命中则走既有 `on_datagram`。
- `handle_info({timeout,_,observe_ack}, St)`：重传该 pending 通知并计数；超过 `observe_max_retrans` → 放弃，`unsubscribe`/`detach` 并清 `observes`/`pending`。
- `handle_info({timeout,_,refill}, St)`：在既有 `prune/1` 之外再调 `sweep_observes/1`（有 unsubscribe 副作用，不能进纯 `prune/1`），按 `coap_observe:expired/3` 清扫。

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

-define(COAP_CLIENT, <<"coap">).

%% Called by respond/5 when a GET with Observe=0 has been answered 2.05. It
%% mints the kernel subscription (ADR-031 §D2) and records the relationship.
%% Returns {St, Rel} on success, or {St, undefined} when the bounded table is at
%% capacity -- the caller then answers 4.03 instead of the 2.05.
register_observe(IP, Port, Token, Req, Cfg, #st{observes = Obs} = St) ->
    Dest = coap_codec:destination(Req),
    Tenant = broker_security:default_tenant(),
    MaxAge = coap_codec:option(max_age, Req, 30),
    Cap = maps:get(max_observes, Cfg, 4096),
    case maps:size(Obs) >= Cap of
        true ->
            {St, undefined};
        false ->
            case broker_subscription:subscribe(?COAP_CLIENT,
                                               coap_observe:filter(Dest), 0,
                                               Tenant) of
                {ok, SubRef} ->
                    ok = broker_subscription:attach(?COAP_CLIENT, self(), false),
                    Rel = coap_observe:register(Dest, Tenant, SubRef, MaxAge,
                                                now_ms()),
                    Rel1 = Rel#{req_type => coap_codec:type(Req)},
                    Key = {IP, Port, Token},
                    {St#st{observes = maps:put(Key, Rel1, Obs)}, Rel};
                {error, _} ->
                    {St, undefined}
            end
    end.

%% Called by respond/5 for a GET with Observe=1. Drops the relationship and
%% unsubscribes from the kernel. Always succeeds (RFC 7641 §3.5: deregistering a
%% relationship that is not there is not an error).
deregister_observe(IP, Port, Token, #st{observes = Obs} = St) ->
    Key = {IP, Port, Token},
    case coap_observe:deregister(Key, Obs) of
        {undefined, Obs1} ->
            St#st{observes = Obs1, pending = maps:remove(Key, St#st.pending)};
        {Rel, Obs1} ->
            _ = (catch broker_subscription:unsubscribe(
                   ?COAP_CLIENT, coap_observe:filter(maps:get(dest, Rel)))),
            _ = (catch broker_subscription:detach(?COAP_CLIENT, self())),
            St#st{observes = Obs1, pending = maps:remove(Key, St#st.pending)}
    end.

%% The kernel delivered a message to an attached subscription (ADR-031 §D2:
%% the listener is the notify pid). Turn it into Observe notifications.
dispatch_notifications(#broker_message{} = Msg,
                       #st{observes = Obs, cfg = Cfg} = St) ->
    lists:foldl(
      fun({Key, Rel}, Acc) ->
              case maps:get(dest, Rel) =:= Msg#broker_message.destination of
                  true -> notify_one(Key, Rel, Msg, Cfg, Acc);
                  false -> Acc
              end
      end, St, maps:to_list(Obs)).

%% Send one notification. The relationship's `req_type' decides CON (reliable:
%% retransmit until ACKed, tracked in `pending') vs NON (at-most-once: fire once).
notify_one(Key, Rel, Msg, Cfg, #st{pending = Pend} = St) ->
    Type = maps:get(req_type, Rel, con),
    Token = element(3, Key),
    Now = now_ms(),
    {Seq, Rel1} = coap_observe:next_seq(Rel, Now),
    Note = coap_observe:notify_message(Type, Seq, Token, Msg),
    Bytes = coap_codec:encode(Note#{id => next_id()}),
    IP = element(1, Key),
    Port = element(2, Key),
    _ = gen_udp:send(St#st.socket, IP, Port, Bytes),
    case Type of
        con ->
            Timeout = maps:get(observe_ack_timeout_ms, Cfg, 2000),
            Ref = erlang:start_timer(Timeout, self(), observe_ack),
            Left = maps:get(observe_max_retrans, Cfg, 3),
            St#st{observes = maps:put(Key, Rel1, St#st.observes),
                  pending = maps:put(Key, {Bytes, Left, Ref}, Pend)};
        non ->
            St#st{observes = maps:put(Key, Rel1, St#st.observes)}
    end.

%% The 1 s refill tick also drops relationships silent longer than their own
%% Max-Age window. Side-effecting (it unsubscribes), so it cannot live in the
%% pure prune/1.
sweep_observes(#st{observes = Obs, cfg = Cfg} = St) ->
    Cap = maps:get(observe_max_age_s, Cfg, 30),
    Now = now_ms(),
    lists:foldl(
      fun({Key, Rel}, Acc) ->
              case coap_observe:expired(Rel, Now, Cap) of
                  true ->
                      _ = (catch broker_subscription:unsubscribe(
                             ?COAP_CLIENT,
                             coap_observe:filter(maps:get(dest, Rel)))),
                      _ = (catch broker_subscription:detach(?COAP_CLIENT, self())),
                      Acc#st{observes = maps:remove(Key, Acc#st.observes),
                             pending = maps:remove(Key, Acc#st.pending)};
                  false ->
                      Acc
              end
      end, St, maps:to_list(Obs)).

%% Monotonic ms. The clock is read here (the listener is the only process that
%% needs it) and passed into coap_observe, which is pure.
now_ms() -> erlang:monotonic_time(millisecond).

next_id() -> erlang:unique_integer([positive]) rem 65536.
```

> 说明：注册/注销的**副作用**（subscribe/attach/unsubscribe/detach）都在 listener 里做，`coap_resource` 保持纯函数。`notify_one` 按关系的 `req_type` 发 CON/NON：CON 入 `pending` 起重传 timer，NON 只发一次（至多一次，ADR-031 §D3）。`now_ms/0` 是本 VM 上唯一的时钟读取点，负纪元对 `expired/3` 的比较无影响（`Next - At` 仍为正）。`sweep_observes/1` 由 Task 2 Step 3 的 tick 子句调用。

- [ ] **Step 3: 加 handle_info 子句（broker_deliver + ACK 取消 + observe_ack 重传）**

在 `handle_info({udp_error, ...})` 之前插入：

```erlang
%% The kernel delivered a message to an attached subscription (ADR-031 §D2:
%% the listener is the notify pid). Turn it into Observe notifications.
handle_info({broker_deliver, <<"coap">>, _MaxQoS, _Matched, {_P, _O, Msg}},
            #st{} = St) ->
    {noreply, dispatch_notifications(Msg, St)};

%% An ACK for a CON notification cancels its retransmit. (The seq was already
%% advanced when the notification was sent, so nothing to do but cancel.)
handle_info({udp, _Socket, IP, Port, Bin}, #st{pending = Pend} = St0) ->
    try coap_codec:decode(Bin) of
        {ok, #{type := ack, token := Token}} ->
            Key = {IP, Port, Token},
            case maps:find(Key, Pend) of
                {ok, {_Bytes, _Left, Ref}} ->
                    _ = erlang:cancel_timer(Ref),
                    {noreply, St0#st{pending = maps:remove(Key, Pend)}};
                error ->
                    {noreply, on_datagram(IP, Port, Bin, St0)}
            end;
        {ok, _Msg} ->
            {noreply, on_datagram(IP, Port, Bin, St0)};
        {error, _} ->
            {noreply, on_datagram(IP, Port, Bin, St0)}
    catch
        _:_ -> {noreply, on_datagram(IP, Port, Bin, St0)}
    end;

%% A CON notification went un-acked: retransmit it, or give up after
%% `observe_max_retrans' tries and tear the relationship down.
handle_info({timeout, _Ref, observe_ack}, #st{pending = Pend} = St) ->
    {noreply, retransmit_observes(St)};

%% The 1 s refill tick also sweeps expired Observe relationships.
handle_info({timeout, _Ref, refill}, St) ->
    {noreply, sweep_observes(prune(St)), {continue, refill}};
```

`retransmit_observes/1` (add next to `sweep_observes/1`) resends each pending CON once and counts the retransmit down; at zero it unsubscribes and drops the relationship:

```erlang
retransmit_observes(#st{pending = Pend} = St) ->
    lists:foldl(
      fun({Key, {Bytes, Left, _Ref}}, Acc) ->
              IP = element(1, Key),
              Port = element(2, Key),
              _ = gen_udp:send(Acc#st.socket, IP, Port, Bytes),
              case Left - 1 of
                  0 ->
                      drop_relationship(Key, Acc);
                  Left1 ->
                      Timeout = maps:get(observe_ack_timeout_ms, Acc#st.cfg, 2000),
                      Ref = erlang:start_timer(Timeout, self(), observe_ack),
                      Acc#st{pending = maps:put(Key, {Bytes, Left1, Ref},
                                                 Acc#st.pending)}
              end
      end, St, maps:to_list(Pend)).

%% Give up on a relationship: unsubscribe, detach, forget it.
drop_relationship(Key, #st{observes = Obs, pending = Pend} = St) ->
    case maps:find(Key, Obs) of
        {ok, Rel} ->
            _ = (catch broker_subscription:unsubscribe(
                   ?COAP_CLIENT,
                   coap_observe:filter(maps:get(dest, Rel)))),
            _ = (catch broker_subscription:detach(?COAP_CLIENT, self()));
        error ->
            ok
    end,
    St#st{observes = maps:remove(Key, Obs), pending = maps:remove(Key, Pend)}.
```

> 关键：把原来的 `handle_info({udp, _Socket, IP, Port, Bin}, #st{} = St0) -> on_datagram` 子句**替换**为上面那个（它先判 ACK-命中-pending，未命中再走 `on_datagram`）。`on_datagram` 收到的 datagram 若本身是 ACK，会在 `dispatch` 里因 `coap_codec:type` 非 con 而走 respond（产生一个 NON 响应）——这不对。**修正**：`handle_message` 顶部对 `type=ack/rst` 直接原样丢弃（它们是来取消通知/重置的，不是请求）。在 Step 4 处理（见下）。

- [ ] **Step 4: `handle_message` 忽略 ack/rst（它们不是请求）**

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

- [ ] **Step 5: refill tick 挂上 `sweep_observes/1`（max_age 兜底）**

`sweep_observes/1` 已在 Step 2 给出完整实现（用 `coap_observe:expired/3`，每关系用自己的 Max-Age 窗口，钳制在 `[1s, observe_max_age_s]`）。本步只需确认 `handle_info({timeout,_,refill}, St)` 子句（Step 3 已写）在 `prune/1` 之后调用它，且**没有**在纯 `prune/1` 里做 unsubscribe（`prune/1` 保持纯函数，只清 dedup/block1）。

> 不要改 `prune/1` 去碰 `observes`——它会被 `prune/1` 的纯函数性质约束，而清扫需要 unsubscribe 的副作用。关系清扫只走 tick 里的 `sweep_observes/1`。

- [ ] **Step 6: 加 1 条 plumbing 用例并跑**

Task 2 只落地管道（observes/pending 表 + handle_info 子句 + sweep），**通知语义的完整覆盖在 Task 3 的 8 条用例里**（它们建立在本步之上）。本步加一条钉住「ack/rst 不是请求、不得回响应」的小用例，顺带证明管道没破坏既有行为。

在 `coap_listener_SUITE.erl` 的 `all/0` 加 `observe_ack_and_rst_are_not_requests`：

```erlang
%% A bare ACK and a bare RST are transport control messages, not requests: the
%% listener must not answer either (no 2.05/4.04 in flight).
observe_ack_and_rst_are_not_requests(Config) ->
    Port = ?config(port, Config),
    S = open(),
    send(S, Port, ack_of(#{id => 4242, token => <<"nobody">>})),
    send(S, Port, rst_of(<<"nobody">>, 4243)),
    refute(S, ack, {response, 2, 5}, 300),
    refute(S, ack, {response, 4, 4}, 300),
    close(S).
```

`ack_of/1`、`rst_of/2` 两个助手在 Task 3 Step 5 定义；**本步就要加**（Task 3 复用）。把它们的定义放在 `request/4` 那组旁边。

跑全套确认无回归：

Run: `export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"; cd /home/wanli/workspace/unified-mq && rm -rf _build/test/extras && rebar3 ct --suite=apps/broker_proto_coap/test/coap_listener_SUITE.erl 2>&1 | tail -5`
Expected: `All 8 tests passed.`（原 7 + 1）

- [ ] **Step 7: 提交**

```bash
cd /home/wanli/workspace/unified-mq
git add apps/broker_proto_coap/src/coap_listener.erl apps/broker_proto_coap/test/coap_listener_SUITE.erl
git commit -q -m "feat(broker_proto_coap): S11-B listener 兼 notify pid——订阅/attach/CON 重传/ACK 取消/sweep"
```

---

## Task 3: Observe=0/1 分流 + RST/注销/max_age/有界表/ACL 用例

**Files:**
- Modify: `apps/broker_proto_coap/src/coap_listener.erl`（`respond/5` 承担注册/注销副作用）
- Modify: `apps/broker_proto_coap/src/coap_resource.erl`（`get/3` 分流 Observe=0/1）
- Test: `apps/broker_proto_coap/test/coap_listener_SUITE.erl`（+8 用例）
- Test: `apps/broker_proto_coap/test/coap_resource_SUITE.erl`（+2 用例，mock 注入）

**关键事实（已核对，实现时照抄）**
- `coap_codec:ack/1`、`coap_codec:reset/1` 都产出 `token => <<>>`（它们只取 id）。**测试里要回 ACK / 发 RST 就必须自己带上 token**，否则匹配不上 `{IP,Port,Token}`。新增测试助手见 Step 4。
- 通知的 type = 关系的 `req_type`：CON Observe=0 → **通知是 CON**（要 ACK、要重传）；NON Observe=0 → 通知是 NON（至多一次）。用例里 `expect(S, con, {response,2,5})` vs `expect(S, non, {response,2,5})` 别搞反。
- `read_resource/2` 对**没有 retained 状态**的 destination 回 4.04。所以 Observe=0 用例必须先 PUT 再 Observe，否则连 2.05 都拿不到、关系也不会注册。
- `run_resource/3` 返回 `{Code, Options, Payload}`；`respond/5` 目前把 `single/done` 都走 `resource_reply/3`（内部已 encode）。加 Observe 后需要**在 encode 之前**插入 `{observe, N}` 选项，所以 Task 3 要把 `single/done` 分支改成先拿三元组再 encode（见 Step 3）。
- `handle_message/5` 目前对 `con` 查 dedup、对 `_Non` 一律 `respond`。Observe 的 ack/rst 不是请求：ack 直接丢弃（取消 pending 的分支在 udp 子句里已处理），rst 走 `deregister_observe/4`。

- [ ] **Step 1: `coap_resource:get/3` 分流 Observe=0/1；非 GET 忽略 Observe**

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
                    %% The subscription is minted by the listener (it owns the socket
                    %% and the table). Here we just read the current representation;
                    %% a 4.04 means there is nothing to observe yet.
                    read_resource(Destination, Ctx);
                1 ->
                    %% Deregister: the listener drops the relationship. Answer 2.01
                    %% "Changed" (RFC 7641 §3.5 - deregistering is benign).
                    {reply, {response, 2, 1}, [], <<>>};
                _ObsVal ->
                    %% Any other Observe value is malformed; treat as a plain read.
                    read_resource(Destination, Ctx)
            end
    end.
```

> **本步只改 `coap_resource:get/3` 的返回码语义**（Observe=1 → 2.01）。注册/注销的**副作用**由 listener 在 `respond/5` 里承担（Step 3），`coap_resource` 保持纯函数。非 GET 的 `handle/3` 不改（Observe 只作用在 GET）；`.well-known/core` 上带 Observe 也只读不注册（Step 3 的 `observe_action/1` 用 `code =:= {method,get}` 卡掉）。

- [ ] **Step 3: listener 承担注册/注销副作用（`respond/5` + `reply_observe/5` + `observe_action/1`）**

把 `respond/5` 的 `single/done` 两个分支从 `resource_reply/3`（内部直接 encode）改成走新的 `reply_observe/5`（先拿 `{Code, Options, Payload}`，按 Observe 加选项或改状态码，再 encode）；`streaming/refused` 分支不动。`handle_message/5` 的 ack/rst 分流见 Step 4。

```erlang
respond(IP, Port, Msg, Cfg, St) ->
    case block1_stream(IP, Port, Msg, Cfg, St) of
        {single, Req, St1} -> reply_observe(IP, Port, Req, Cfg, St1);
        {done, Req, St1}   -> reply_observe(IP, Port, Req, Cfg, St1);
        {streaming, Continue, St1} -> {coap_codec:encode(Continue), St1};
        {refused, Refusal, St1}    -> {coap_codec:encode(Refusal), St1}
    end.

%% GET with Observe=0 registers a relationship and answers the initial 2.05 with
%% Observe: 0 (RFC 7641 §3.4); Observe=1 deregisters and answers 2.01 (§3.5).
%% Everything else ignores Observe entirely.
reply_observe(IP, Port, Req, Cfg, St) ->
    case observe_action(Req) of
        none ->
            {resource_reply(Req, Cfg, {IP, Port}), St};
        register ->
            {Code, Options, Payload} = run_resource(Req, Cfg, {IP, Port}),
            case Code of
                {response, 2, 5} ->
                    case register_observe(IP, Port, coap_codec:token(Req), Req,
                                          Cfg, St) of
                        {St2, Rel} ->
                            Opts = [{observe, coap_observe:seq(Rel)} | Options],
                            {encode_reply(Req, reply_type(Req), Code, Opts,
                                          Payload), St2};
                        {St2, undefined} ->
                            %% Bounded table full: refuse the observation.
                            {encode_reply(Req, reply_type(Req), {response, 4, 3},
                                          [], <<>>), St2}
                    end;
                _ ->
                    %% 4.04 / 5.xx: nothing to observe, answer as-is, no registration.
                    {encode_reply(Req, reply_type(Req), Code, Options, Payload), St}
            end;
        deregister ->
            St2 = deregister_observe(IP, Port, coap_codec:token(Req), St),
            {resource_reply(Req, Cfg, {IP, Port}), St2}
    end.

%% Observe only applies to GET (RFC 7641 §3.1): 0 registers, 1 deregisters.
observe_action(Req) ->
    case {coap_codec:code(Req), coap_codec:option(observe, Req)} of
        {{method, get}, 0} -> register;
        {{method, get}, 1} -> deregister;
        _ -> none
    end.
```

> `resource_reply/3` 保留（`none` 分支与既有调用点仍在用它）。`run_resource/3` 已是私有函数、返回三元组，`encode_reply/5` 也是既有的——本步没有新增 codec 依赖。
>
> 注意 piggyback 的初始 2.05 会**同时**带资源自身的 `content_format`（来自 `run_resource` → `coap_resource:content_format_options/1`）和 `{observe, 0}`；后续通知经 `coap_observe:notify_message/4` 镜像同一 `content_format`，因此首帧与通知帧形状一致。

- [ ] **Step 4: `handle_message/5` 忽略 ack、rst 触发注销**

```erlang
handle_message(IP, Port, Msg, Cfg, #st{dedup = Dedup} = St) ->
    case coap_codec:type(Msg) of
        ack ->
            %% Cancels a pending CON notification; the udp clause already matched
            %% it against `pending'. Not a request: no response owed.
            St;
        rst ->
            %% Ends the observation relationship (RFC 7641 §3.6). Not a request:
            %% no response owed.
            deregister_observe(IP, Port, coap_codec:token(Msg), St);
        con ->
            %% Already answered this exact CON? Re-send the cached bytes without
            %% re-executing (RFC 7252 §4.8.2).
            case maps:find({IP, Port, coap_codec:id(Msg)}, Dedup) of
                {ok, {Cached, _At}} ->
                    send(IP, Port, Cached, St),
                    St;
                error ->
                    {Reply, St1} = respond(IP, Port, Msg, Cfg, St),
                    send(IP, Port, Reply, St1),
                    remember(IP, Port, coap_codec:id(Msg), Reply, Cfg, St1)
            end;
        _Non ->
            %% A NON request still gets the resource's answer, as a separate NON.
            {Reply, St1} = respond(IP, Port, Msg, Cfg, St),
            send(IP, Port, Reply, St1),
            St1
    end.
```

> 把原 `handle_message/5` **整体替换**。注意原代码用 `Id = coap_codec:id(Msg)`，这里内联成 `coap_codec:id(Msg)`；dedup 键是 `{IP,Port,Id}`，别和 `observes`/`pending` 的 `{IP,Port,Token}` 搞混。

- [ ] **Step 5: 加 8 条 listener 用例 + `case_env`/`restore_env` 扩展**

在 `coap_listener_SUITE.erl` 的 `all/0` 追加这 8 个名字（放在 `malformed_datagram_is_dropped` 之后）：

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

`init_per_testcase/2` 的 `Saved` map 要**多存 3 个键**（`max_observes`、`observe_ack_timeout_ms`、`observe_max_age_s`），`restore_env/1` 要**多恢复这 3 个键**（否则 `observe_bounded_table_refuses` 设的 `max_observes=1` 会泄漏到后面的用例）：

```erlang
    Saved = #{acl => env(broker_security, acl),
              require_auth => env(broker_security, require_auth),
              users => env(broker_security, users),
              max_rps => env(broker_proto_coap, max_requests_per_second),
              max_msg => env(broker_proto_coap, max_message_bytes),
              max_observes => env(broker_proto_coap, max_observes),
              ack_timeout => env(broker_proto_coap, observe_ack_timeout_ms),
              max_age => env(broker_proto_coap, observe_max_age_s)},
```

```erlang
restore_env(#{acl := Acl, require_auth := Auth, users := Users,
               max_rps := Rps, max_msg := MaxMsg,
               max_observes := MaxObs, ack_timeout := AckTO, max_age := MaxAge}) ->
    set_or_unset(broker_security, acl, Acl),
    set_or_unset(broker_security, require_auth, Auth),
    set_or_unset(broker_security, users, Users),
    set_or_unset(broker_proto_coap, max_requests_per_second, Rps),
    set_or_unset(broker_proto_coap, max_message_bytes, MaxMsg),
    set_or_unset(broker_proto_coap, max_observes, MaxObs),
    set_or_unset(broker_proto_coap, observe_ack_timeout_ms, AckTO),
    set_or_unset(broker_proto_coap, observe_max_age_s, MaxAge),
    ok.
```

`case_env/2` 追加三条（其余用例用默认）：

```erlang
%% The retry case pins a short retransmit timeout so it does not sit on 2 s.
case_env(observe_con_retries_then_gives_up, _Config) ->
    ok = application:set_env(broker_proto_coap, observe_ack_timeout_ms, 300),
    ok = application:set_env(broker_proto_coap, observe_max_retrans, 2);
%% The bounded-table case shrinks the relationship table to a single slot.
case_env(observe_bounded_table_refuses, _Config) ->
    ok = application:set_env(broker_proto_coap, max_observes, 1);
%% The max-age case sweeps after one second instead of thirty.
case_env(observe_max_age_expires_relationship, _Config) ->
    ok = application:set_env(broker_proto_coap, observe_max_age_s, 1);
```

再加两个测试助手（放在 `request/4` 那组旁边）。**必须带 token**，因为 `coap_codec:ack/1` 和 `coap_codec:reset/1` 会把 token 清空：

```erlang
%% An empty ACK that echoes a notification's token and Message ID. The codec's
%% own `ack/1' drops the token, which would miss the `pending' match.
ack_of(Note) ->
    #{type => ack, code => empty, id => coap_codec:id(Note),
      token => coap_codec:token(Note), options => [], payload => <<>>}.

%% An RST carrying the token of the observation being ended (RFC 7641 §3.6).
rst_of(Token, Id) ->
    #{type => rst, code => empty, id => Id, token => Token,
      options => [], payload => <<>>}.
```

8 条用例（真实 UDP；token 显式；用既有 `expect/3`、`refute/4`）：

```erlang
%% CON Observe=0 -> piggybacked 2.05 with Observe: 0; a later PUT -> a CON
%% notification with Observe: 1; ACK it -> no retransmit; a further PUT -> the
%% second notification.
observe_con_notifies_and_acks(Config) ->
    Port = ?config(port, Config),
    S = open(),
    Token = <<"ob1">>,
    send(S, Port, request(con, put, 1, [{uri_path, <<"o">>}], <<"init">>)),
    _ = expect(S, ack, {response, 2, 4}),
    send(S, Port, (request(con, get, 2, [{uri_path, <<"o">>}]))#{token => Token}),
    First = expect(S, ack, {response, 2, 5}),
    ?assertEqual(Token, coap_codec:token(First)),
    ?assertEqual(0, coap_codec:option(observe, First)),
    send(S, Port, request(con, put, 3, [{uri_path, <<"o">>}], <<"v1">>)),
    _ = expect(S, ack, {response, 2, 4}),
    Note = expect(S, con, {response, 2, 5}),
    ?assertEqual(Token, coap_codec:token(Note)),
    ?assertEqual(1, coap_codec:option(observe, Note)),
    send(S, Port, ack_of(Note)),
    refute(S, con, {response, 2, 5}, 400),
    send(S, Port, request(con, put, 4, [{uri_path, <<"o">>}], <<"v2">>)),
    _ = expect(S, ack, {response, 2, 4}),
    Note2 = expect(S, con, {response, 2, 5}),
    ?assertEqual(2, coap_codec:option(observe, Note2)),
    close(S).

%% A CON notification that is never ACKed is retransmitted (same Message ID) and
%% finally given up: the relationship is torn down and no further notification
%% arrives.
observe_con_retries_then_gives_up(Config) ->
    Port = ?config(port, Config),
    S = open(),
    send(S, Port, request(con, get, 1, [{uri_path, <<"r">>}])),
    _ = expect(S, ack, {response, 4, 4}),
    send(S, Port, request(con, put, 2, [{uri_path, <<"r">>}], <<"x">>)),
    _ = expect(S, ack, {response, 2, 4}),
    %% The Observe registration itself needs a 2.05 first.
    send(S, Port, request(con, get, 3, [{uri_path, <<"r">>}])),
    _ = expect(S, ack, {response, 2, 5}),
    send(S, Port, request(con, put, 4, [{uri_path, <<"r">>}], <<"y">>)),
    _ = expect(S, ack, {response, 2, 4}),
    N1 = expect(S, con, {response, 2, 5}),
    N2 = expect(S, con, {response, 2, 5}),
    ?assertEqual(coap_codec:id(N1), coap_codec:id(N2)),
    N3 = expect(S, con, {response, 2, 5}),
    ?assertEqual(coap_codec:id(N1), coap_codec:id(N3)),
    send(S, Port, request(con, put, 5, [{uri_path, <<"r">>}], <<"z">>)),
    _ = expect(S, ack, {response, 2, 4}),
    refute(S, con, {response, 2, 5}, 600),
    close(S).

%% A NON Observe=0 gets at most one notification: no retransmit timer, no retry.
observe_non_is_at_most_once(Config) ->
    Port = ?config(port, Config),
    S = open(),
    send(S, Port, request(non, put, 1, [{uri_path, <<"n">>}], <<"init">>)),
    _ = expect(S, non, {response, 2, 4}),
    send(S, Port, request(non, get, 2, [{uri_path, <<"n">>}, {observe, 0}])),
    First = expect(S, non, {response, 2, 5}),
    ?assertEqual(0, coap_codec:option(observe, First)),
    send(S, Port, request(non, put, 3, [{uri_path, <<"n">>}], <<"y">>)),
    Note = expect(S, non, {response, 2, 5}),
    ?assertEqual(1, coap_codec:option(observe, Note)),
    refute(S, non, {response, 2, 5}, 400),
    close(S).

%% Observe=1 deregisters: a later PUT notifies nobody.
observe_deregister_with_observe_1(Config) ->
    Port = ?config(port, Config),
    S = open(),
    send(S, Port, request(con, put, 1, [{uri_path, <<"d1">>}], <<"a">>)),
    _ = expect(S, ack, {response, 2, 4}),
    send(S, Port, request(con, get, 2, [{uri_path, <<"d1">>}])),
    _ = expect(S, ack, {response, 2, 5}),
    send(S, Port, request(con, put, 3, [{uri_path, <<"d1">>}], <<"b">>)),
    _ = expect(S, ack, {response, 2, 4}),
    _ = expect(S, con, {response, 2, 5}),
    send(S, Port, request(con, get, 4, [{uri_path, <<"d1">>}, {observe, 1}])),
    ?assertEqual({response, 2, 1},
                 coap_codec:code(expect(S, ack, {response, 2, 1}))),
    send(S, Port, request(con, put, 5, [{uri_path, <<"d1">>}], <<"c">>)),
    _ = expect(S, ack, {response, 2, 4}),
    refute(S, con, {response, 2, 5}, 400),
    close(S).

%% An RST carrying the observation's token ends it (RFC 7641 §3.6).
observe_rst_deregisters(Config) ->
    Port = ?config(port, Config),
    S = open(),
    Token = <<"rsttok">>,
    send(S, Port, request(con, put, 1, [{uri_path, <<"rst1">>}], <<"a">>)),
    _ = expect(S, ack, {response, 2, 4}),
    send(S, Port, (request(con, get, 2, [{uri_path, <<"rst1">>}]))#{token => Token}),
    _ = expect(S, ack, {response, 2, 5}),
    send(S, Port, request(con, put, 3, [{uri_path, <<"rst1">>}], <<"b">>)),
    _ = expect(S, ack, {response, 2, 4}),
    _ = expect(S, con, {response, 2, 5}),
    send(S, Port, rst_of(Token, 9)),
    send(S, Port, request(con, put, 4, [{uri_path, <<"rst1">>}], <<"c">>)),
    _ = expect(S, ack, {response, 2, 4}),
    refute(S, con, {response, 2, 5}, 400),
    close(S).

%% A full relationship table refuses the observation with 4.03.
observe_bounded_table_refuses(Config) ->
    Port = ?config(port, Config),
    S = open(),
    send(S, Port, request(con, put, 1, [{uri_path, <<"b1">>}, {observe, 0}])),
    _ = expect(S, ack, {response, 2, 4}),
    send(S, Port, request(con, put, 2, [{uri_path, <<"b2">>}])),
    _ = expect(S, ack, {response, 2, 4}),
    send(S, Port, request(con, get, 3, [{uri_path, <<"b1">>}, {observe, 0}])),
    _ = expect(S, ack, {response, 2, 5}),
    send(S, Port, request(con, get, 4, [{uri_path, <<"b2">>}, {observe, 0}])),
    ?assertEqual({response, 4, 3},
                 coap_codec:code(expect(S, ack, {response, 4, 3}))),
    close(S).

%% An unauthenticated principal is refused: the observation never registers.
observe_acl_denied(Config) ->
    Port = ?config(port, Config),
    S = open(),
    ok = application:set_env(broker_security, acl, all),
    ok = application:set_env(broker_security, require_auth, [<<"coap">>]),
    send(S, Port, request(con, get, 1, [{uri_path, <<"x">>}, {observe, 0}])),
    ?assertEqual({response, 4, 3},
                 coap_codec:code(expect(S, ack, {response, 4, 3}))),
    close(S).

%% A relationship silent longer than Max-Age is swept on the tick: a later PUT
%% notifies nobody (the client is expected to re-Observe).
observe_max_age_expires_relationship(Config) ->
    Port = ?config(port, Config),
    S = open(),
    send(S, Port, request(con, put, 1, [{uri_path, <<"mx">>}])),
    _ = expect(S, ack, {response, 2, 4}),
    send(S, Port, request(con, get, 2, [{uri_path, <<"mx">>}, {observe, 0}])),
    _ = expect(S, ack, {response, 2, 5}),
    send(S, Port, request(con, put, 3, [{uri_path, <<"mx">>}], <<"a">>)),
    _ = expect(S, ack, {response, 2, 4}),
    _ = expect(S, con, {response, 2, 5}),
    timer:sleep(1300),
    send(S, Port, request(con, put, 4, [{uri_path, <<"mx">>}], <<"b">>)),
    _ = expect(S, ack, {response, 2, 4}),
    refute(S, con, {response, 2, 5}, 400),
    close(S).
```

> `observe_con_retries_then_gives_up` 的时序：`observe_ack_timeout_ms=300`、`observe_max_retrans=2`，所以未 ACK 的通知在 0 / 300 / 600 ms 各发一次共 3 帧（首发 + 2 次重传），第 3 帧后放弃。`refute(..., 600)` 覆盖了「放弃 + 拆关系」的窗口；若机器卡顿导致 600 ms 内没跑完 3 帧，把窗口调大到 900。
>
> `observe_non_is_at_most_once` 里 NON PUT 的应答是 **NON**（不是 ack），因为 `reply_type/1` 对 non 返回 non。

- [ ] **Step 6: 跑全套 listener**

Run: `export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"; cd /home/wanli/workspace/unified-mq && rm -rf _build/test/extras && rebar3 ct --suite=apps/broker_proto_coap/test/coap_listener_SUITE.erl 2>&1 | tail -5`
Expected: `All 15 tests passed.`（原 7 + 8 新）

- [ ] **Step 7: 加 resource 层 2 条 mock 注入用例**

在 `coap_resource_SUITE.erl` 的 `all/0` 追加 `observe_1_on_get_answers_2_01`、`observe_0_on_get_still_reads_retained`，并按该文件现有 fixture 风格实现：

```erlang
%% Observe=1 deregisters; the resource layer itself just answers 2.01.
observe_1_on_get_answers_2_01(_Config) ->
    Req = request(con, get, 9, [{uri_path, <<"z">>}, {observe, 1}]),
    {reply, {response, 2, 1}, [], <<>>} = coap_resource:handle(Req, ?CFG, ?CTX).

%% Observe=0 does not change what the resource reads: still the retained rep.
observe_0_on_get_still_reads_retained(_Config) ->
    ok = broker_retain:put(<<"z">>, #broker_message{payload = <<"v">>}),
    Req = request(con, get, 10, [{uri_path, <<"z">>}, {observe, 0}]),
    {reply, {response, 2, 5}, _Options, <<"v">>} =
        coap_resource:handle(Req, ?CFG, ?CTX).
```

> 用该文件**既有**的 mock/fixture 写法（`?CFG`/`?CTX` 或等价的 `handle(Req, Cfg, Ctx)` 调用形式）。先读一遍该文件现有的 `get`/`put` 用例再照抄风格；如果它用的是别的断言宏，就用那个。若 `broker_retain:put/2` 签名不同，用该文件已经用过的方式写 retained 状态。

Run: `... rebar3 ct --suite=apps/broker_proto_coap/test/coap_resource_SUITE.erl 2>&1 | tail -4`
Expected: `All 15 tests passed.`（原 13 + 2）

- [ ] **Step 8: 提交**

```bash
cd /home/wanli/workspace/unified-mq
git add apps/broker_proto_coap/src/coap_listener.erl apps/broker_proto_coap/src/coap_resource.erl apps/broker_proto_coap/test/coap_listener_SUITE.erl apps/broker_proto_coap/test/coap_resource_SUITE.erl
git commit -q -m "feat(broker_proto_coap): S11-B Observe=0/1 分流 + RST/注销/max_age/有界表/ACL 用例"
```

---

## Task 4: 全量验证 + 文档同步 + 提交

**Files:** 无代码改动（除非 dialyzer/xref 指出问题）

- [ ] **Step 1: 编译 + CoAP 四套件**

Run: `export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"; cd /home/wanli/workspace/unified-mq && rm -rf _build/test/extras && rebar3 ct --suite=apps/broker_proto_coap/test/coap_observe_SUITE.erl,apps/broker_proto_coap/test/coap_codec_SUITE.erl,apps/broker_proto_coap/test/coap_resource_SUITE.erl,apps/broker_proto_coap/test/coap_listener_SUITE.erl 2>&1 | tail -4`
Expected: `All 93 tests passed.`（codec 48 + resource 15 + listener 15 + observe 10 + app 5；若资源/listener 用例数与计划略有出入，以实跑为准并在文档里写实际数）

- [ ] **Step 2: 全量 CT**

Run: `export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"; cd /home/wanli/workspace/unified-mq && rm -rf _build/test/extras && timeout 3000 rebar3 ct > /tmp/opencode/ct_s11b.log 2>&1; echo exit=$?; tail -3 /tmp/opencode/ct_s11b.log`
Expected: `All <N> tests passed.`，N = 1324 + 本阶段新增（1 + 8 + 2 + 10 = 21，全量约 1345），零 FAILED。新增数 = listener 1（Task 2）+ listener 8（Task 3）+ resource 2（Task 3）+ observe 10（Task 1）。以实际跑出的数为准。

- [ ] **Step 3: dialyzer + xref**

Run: `export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"; cd /home/wanli/workspace/unified-mq && timeout 900 rebar3 dialyzer 2>&1 | tail -3; echo "=== xref ==="; timeout 600 rebar3 xref 2>&1 | grep -c Warning`
Expected: dialyzer 0 warning；xref 计数 = 302 基线 + 本阶段新增（coap_observe 8 个导出在 Task 1 后未接线 = +8 → 310；Task 2/3 接线后回落到 302 + coap_listener/coap_resource 自身的新增未用导出，以实跑为准）。

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
- **类型一致**：`coap_observe:relationship()` 的字段是 dest/tenant/sub_ref/max_age/seq/last_notified（**注意 `req_type` 不在其中**——它由 listener 在 `register_observe` 里以 `Rel#{req_type => ...}` 附加，是 listener 侧的表项元数据，不进纯模块的类型）；listener 侧 `observes` 表项 = `relationship() + req_type`，`pending` 表项 = `{Bytes, RetransLeft, TimerRef}`；ClientId 恒 `<<"coap">>`；`coap_observe:filter/1` 在 register 与 sweep/deregister 复用。
- **已知坑**：`sweep_observes/1` 因需 side-effect（unsubscribe）放在 tick 而非纯 `prune/1`（Task2 Step5 已给完整实现），`observe_max_age_expires_relationship` 钉住。`notify pid = listener` 意味着 `attach(<<"coap">>, self(), false)` 的 self 是 listener 进程。`coap_codec:ack/1`、`reset/1` 丢 token，测试必须自带 token（Task3 Step5 的 `ack_of/1`/`rst_of/2`）。
