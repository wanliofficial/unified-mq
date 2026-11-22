# 吊销实时驱逐活动会话（按 principal）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让运维能按 principal（用户名）吊销身份——立即驱逐该用户所有活动连接并拒绝其重连，直到显式清除或可选 TTL 到期。

**Architecture:** 在 `broker_security` 新增 ETS-backed 的 `broker_revocation` 注册表（仿 `broker_admission`：表由 app master 在 sup 前 `init/0` 建，热路径读表无消息往返），gen_server 只管 TTL 扫描与驱逐派发。`mqtt_conn_fsm` 在认证处拒已吊销 principal、在连接成功/退出时维护 `principal → pid` 索引，并在收到驱逐消息时按正常 DISCONNECT 语义发帧、清本地 Will 后 stop。admin HTTP 暴露 `POST /api/v1/revoke` 与清除端点。

**Tech Stack:** Erlang/OTP（gen_server、gen_statem、ETS、persistent_term）、Common Test、rebar3、dialyzer。

**设计依据:** `docs/superpowers/specs/2026-11-22-mqtt-session-revocation-design.md`（已批准）。所有任务以该 spec 为准；冲突时以 spec 为准并回改 spec。

**环境（每个测试步骤都以此为准）:**
```bash
export PATH="/home/wanli/Development/otp-29.1.2/bin:$PATH"
export PATH="$HOME/.local/bin:$PATH"   # rebar3
cd /home/wanli/workspace/unified-mq
```

**全量基线:** MQ-16 落地后 `rebar3 ct` 为 **1628/1628**（OTP 29.1.2）。每步回归都必须零 failed。（`broker_queue_SUITE:eight_concurrent_callers_pay_for_one_fsync` 在 load >~5 时 flaky，判 0 failed 时注意当前 load。）

**提交纪律:** 只用显式路径 `git add <path>`，禁 `git add -A`；不提交 `rebar.lock`、scratch、`.zode/`；每步提交前 `git checkout rebar.lock 2>/dev/null`。

---

## File Structure

**Create:**
- `apps/broker_security/src/broker_revocation.erl` — 吊销注册表 + 驱逐派发的 gen_server（表由 `init/0` 建）。
- `apps/broker_security/test/broker_revocation_SUITE.erl` — 注册表单元测试。

**Modify:**
- `apps/broker_security/src/broker_security_app.erl` — 在 sup 前 `broker_revocation:init()`；把 gen_server 作为 sup child 启动。
- `apps/broker_security/src/broker_security_sup.erl` — 加 `broker_revocation` child spec。
- `apps/broker_security/src/broker_security.erl` — 加 `revoke/2`、`clear_revocation/1`、`revoked/1` 门面。
- `apps/broker_proto_mqtt/src/mqtt_conn_fsm.erl` — CONNECT 拒已吊销；成功注册/退出注销；处理驱逐消息。
- `apps/broker_admin/src/broker_admin_http.erl` — `POST /api/v1/revoke` + 清除端点 + 文档头。
- `apps/broker_proto_mqtt/test/mqtt_conn_SUITE.erl` — 端到端驱逐/重连/Will 抑制用例。
- `apps/broker_admin/test/broker_admin_SUITE.erl` — 端点用例。
- `docs/compatibility/unifiedmq-mqtt-family-gap.md` — MQ-17 行标注 #10 已完成。
- `docs/mqtt-feature-comparison.md` — §4.2 #10 标 `[x]`。

---

## Task 1: `broker_revocation` 注册表骨架（revoke/clear/revoked）

**Files:**
- Create: `apps/broker_security/src/broker_revocation.erl`
- Test: `apps/broker_security/test/broker_revocation_SUITE.erl`

- [ ] **Step 1: 写失败测试**

```erlang
%% apps/broker_security/test/broker_revocation_SUITE.erl
-module(broker_revocation_SUITE).
-compile([export_all, nowarn_export_all]).

all() ->
    [revoke_makes_principal_revoked,
     clear_lifts_the_revocation,
     revoke_with_ttl_expires_and_lifts,
     revoke_without_ttl_is_permanent,
     unknown_principal_is_not_revoked].

init_per_testcase(_Case, Config) ->
    ok = broker_revocation:init(),
    ok = broker_revocation:reset(),
    Config.

end_per_testcase(_Case, _Config) ->
    ok = broker_revocation:reset(),
    ok.

revoke_makes_principal_revoked(_Config) ->
    false = broker_revocation:revoked(<<"alice">>),
    ok = broker_revocation:revoke(<<"alice">>, undefined),
    true = broker_revocation:revoked(<<"alice">>),
    false = broker_revocation:revoked(<<"bob">>).

clear_lifts_the_revocation(_Config) ->
    ok = broker_revocation:revoke(<<"alice">>, undefined),
    true = broker_revocation:revoked(<<"alice">>),
    ok = broker_revocation:clear(<<"alice">>),
    false = broker_revocation:revoked(<<"alice">>).

revoke_with_ttl_expires_and_lifts(_Config) ->
    ok = broker_revocation:revoke(<<"alice">>, 50),
    true = broker_revocation:revoked(<<"alice">>),
    timer:sleep(120),
    %% 读时也会判到期，不依赖扫描循环的时延。
    false = broker_revocation:revoked(<<"alice">>).

revoke_without_ttl_is_permanent(_Config) ->
    ok = broker_revocation:revoke(<<"alice">>, undefined),
    timer:sleep(120),
    true = broker_revocation:revoked(<<"alice">>).

unknown_principal_is_not_revoked(_Config) ->
    false = broker_revocation:revoked(<<"nobody">>).
```

- [ ] **Step 2: 运行确认失败**

Run: `rebar3 ct --suite=apps/broker_security/test/broker_revocation_SUITE`
Expected: FAIL（`broker_revocation` 未定义 / `undef`）。

- [ ] **Step 3: 写最小实现**

```erlang
%% apps/broker_security/src/broker_revocation.erl
%% @doc 吊销注册表（MQ-17 #10）：按 principal 吊销身份并驱逐其活动连接。
%%
%% 仿 `broker_admission'：两张表由 `init/0' 在 app master（sup 之前）创建，
%% 热路径（CONNECT 的 `revoked/1'）在调用方进程本地直读，无消息往返；
%% `gen_server' 只承担 TTL 扫描与驱逐派发这类低频可延迟的活。
-module(broker_revocation).

-export([init/0, start_link/0, reset/0]).
-export([revoke/2, clear/1, revoked/1]).
-export([register_session/2, unregister_session/2]).

-define(PT, {?MODULE, principals_table}).
-define(TAB_PRINCIPALS, broker_revocation_principals).
-define(TAB_SESSIONS, broker_revocation_sessions).
-define(SWEEP_MS, 1000).

%% @doc 建表。由 `broker_security_app:start/2' 在 sup 之前调用。
-spec init() -> ok.
init() ->
    _ = ets:new(?TAB_PRINCIPALS,
                [named_table, public, set, {read_concurrency, true},
                 {write_concurrency, true}]),
    _ = ets:new(?TAB_SESSIONS,
                [named_table, public, set, {read_concurrency, true},
                 {write_concurrency, true}]),
    persistent_term:put(?PT, ?TAB_PRINCIPALS),
    ok.

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc 测试专用：清空两张表。生产不调。
-spec reset() -> ok.
reset() ->
    _ = clear_table(?TAB_PRINCIPALS),
    _ = clear_table(?TAB_SESSIONS),
    ok.

%% @doc 吊销 `Principal'。`TTL' 为 `undefined' 表示永久，否则毫秒后自动解除。
-spec revoke(binary(), non_neg_integer() | undefined) -> ok.
revoke(Principal, TTL) when is_binary(Principal) ->
    Expiry = case TTL of
                 undefined -> permanent;
                 Ms when is_integer(Ms), Ms >= 0 ->
                     erlang:system_time(millisecond) + Ms
             end,
    _ = insert_principal(Principal, Expiry),
    ok = evict(Principal),
    ok.

%% @doc 解除吊销（不主动 kill 已驱逐会话；驱逐在 revoke 时已完成）。
-spec clear(binary()) -> ok.
clear(Principal) when is_binary(Principal) ->
    _ = delete_principal(Principal),
    ok.

%% @doc `Principal' 当前是否被吊销。到期即视为未吊销（读时判，不依赖扫描）。
-spec revoked(binary()) -> boolean().
revoked(Principal) when is_binary(Principal) ->
    case lookup_principal(Principal) of
        permanent -> true;
        Expiry when is_integer(Expiry) ->
            Expiry > erlang:system_time(millisecond);
        undefined -> false
    end;
revoked(_) ->
    false.

%% @doc 活动连接的 principal 索引。conn FSM 在连接成功时调用。
-spec register_session(binary(), pid()) -> ok.
register_session(Principal, Pid) when is_binary(Principal), is_pid(Pid) ->
    _ = insert_session(Principal, Pid),
    ok.

-spec unregister_session(binary(), pid()) -> ok.
unregister_session(Principal, Pid) when is_binary(Principal), is_pid(Pid) ->
    _ = delete_session(Principal, Pid),
    ok.

%%%_* gen_server ------------------------------------------------------------

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

init([]) ->
    _ = erlang:send_after(?SWEEP_MS, self(), sweep),
    {ok, #{}}.

handle_call(_Req, _From, St) -> {reply, ok, St}.
handle_cast(_Msg, St) -> {noreply, St}.

handle_info(sweep, St) ->
    Now = erlang:system_time(millisecond),
    _ = sweep_expired(Now, all_principals()),
    _ = reap_dead_sessions(),
    _ = erlang:send_after(?SWEEP_MS, self(), sweep),
    {noreply, St};
handle_info(_Other, St) ->
    {noreply, St}.

terminate(_Reason, _St) -> ok.

%%%_* 驱逐派发 --------------------------------------------------------------

%% 对该 principal 的所有活动连接发驱逐消息（每条连接自己按正常 DISCONNECT 语义
%% 发帧、清 Will、stop）。死 pid 跳过；顺带回收其行。
evict(Principal) ->
    lists:foreach(
      fun({_P, Pid}) ->
              case is_process_alive(Pid) of
                  true -> Pid ! mqtt_evict;
                  false -> _ = delete_session(Principal, Pid)
              end
      end, principal_sessions(Principal)),
    ok.

%%%_* ETS 帮手（容忍表不存在）---------------------------------------------

insert_principal(P, E) ->
    try ets:insert(?TAB_PRINCIPALS, {P, E}) of true -> ok
    catch error:badarg -> ok end.

lookup_principal(P) ->
    try ets:lookup(?TAB_PRINCIPALS, P) of
        [{P, E}] -> E;
        [] -> undefined
    catch error:badarg -> undefined end.

delete_principal(P) ->
    try ets:delete(?TAB_PRINCIPALS, P) of true -> ok
    catch error:badarg -> ok end.

insert_session(P, Pid) ->
    try ets:insert(?TAB_SESSIONS, {{P, Pid}}) of true -> ok
    catch error:badarg -> ok end.

delete_session(P, Pid) ->
    try ets:delete(?TAB_SESSIONS, {P, Pid}) of true -> ok
    catch error:badarg -> ok end.

principal_sessions(P) ->
    try ets:match(?TAB_SESSIONS, {{P, '$1'}}) of
        Rows -> [{P, Pid} || [Pid] <- Rows]
    catch error:badarg -> [] end.

all_principals() ->
    try ets:tab2list(?TAB_PRINCIPALS) of
        Rows -> Rows
    catch error:badarg -> [] end.

sweep_expired(Now, [{P, Expiry} | Rest]) when is_integer(Expiry) ->
    case Expiry =< Now of
        true -> _ = delete_principal(P);
        false -> ok
    end,
    sweep_expired(Now, Rest);
sweep_expired(Now, [_ | Rest]) ->
    sweep_expired(Now, Rest);
sweep_expired(_Now, []) ->
    ok.

reap_dead_sessions() ->
    try ets:tab2list(?TAB_SESSIONS) of
        Rows ->
            lists:foreach(
              fun({{P, Pid}}) ->
                      case is_process_alive(Pid) of
                          true -> ok;
                          false -> _ = delete_session(P, Pid)
                      end
              end, Rows)
    catch error:badarg -> ok end.

clear_table(Tab) ->
    try ets:delete_all_objects(Tab) of true -> ok
    catch error:badarg -> ok end.
```

- [ ] **Step 4: 运行确认通过**

Run: `rebar3 ct --suite=apps/broker_security/test/broker_revocation_SUITE`
Expected: PASS，5/5。

- [ ] **Step 5: 编译 + dialyzer**

Run: `rebar3 compile && rebar3 dialyzer`
Expected: 无 warning、无 error（新模块单独干净）。

- [ ] **Step 6: 提交**

```bash
git checkout rebar.lock 2>/dev/null
git add apps/broker_security/src/broker_revocation.erl apps/broker_security/test/broker_revocation_SUITE.erl
git commit -m "feat(security): 吊销注册表 broker_revocation（revoke/clear/revoked）"
```

---

## Task 2: 接入应用启动与 supervisor

**Files:**
- Modify: `apps/broker_security/src/broker_security_app.erl`
- Modify: `apps/broker_security/src/broker_security_sup.erl`
- Test: `apps/broker_security/test/broker_revocation_SUITE.erl`（加一个 app-up 用例）

- [ ] **Step 1: 写失败测试（验证 app 启动后注册表可用）**

在 `broker_revocation_SUITE` 的 `all/0` 里加 `registry_is_live_after_app_start`：

```erlang
registry_is_live_after_app_start(_Config) ->
    %% 表必须已存在（app master 在 sup 前 init），且 registry 进程在跑。
    true = is_pid(whereis(broker_revocation)),
    ok = broker_revocation:revoke(<<"carol">>, undefined),
    true = broker_revocation:revoked(<<"carol">>),
    ok = broker_revocation:clear(<<"carol">>).
```

> 注意：`init_per_testcase` 里的 `broker_revocation:init()` 是幂等的（`ets:new` 对已存在的 named table 会 `badarg`，见 Step 3 的 `init/0` 容错）。若与 app 启动冲突，改为 `reset/0` 即可——见 Step 3 的幂等处理。

- [ ] **Step 2: 运行确认失败**

Run: `rebar3 ct --suite=apps/broker_security/test/broker_revocation_SUITE --case=registry_is_live_after_app_start`
Expected: FAIL（`whereis(broker_revocation)` 为 `undefined`）。

- [ ] **Step 3: 实现**

`broker_revocation:init/0` 改为幂等（表已存在则复用，不抛 `badarg`）：

```erlang
init() ->
    _ = ensure_table(?TAB_PRINCIPALS, ?TAB_PRINCIPALS),
    _ = ensure_table(?TAB_SESSIONS, ?TAB_SESSIONS),
    persistent_term:put(?PT, ?TAB_PRINCIPALS),
    ok.

ensure_table(_, Name) ->
    case ets:whereis(Name) of
        undefined ->
            _ = ets:new(Name, [named_table, public, set,
                               {read_concurrency, true},
                               {write_concurrency, true}]),
            ok;
        _Tid -> ok
    end.
```

`broker_security_app:start/2`（在 `broker_admission:init()` 之后、sup 之前加一行）：

```erlang
ok = broker_admission:init(),
ok = broker_revocation:init(),
broker_security_sup:start_link();
```

`broker_security_sup:init/1` 加 child：

```erlang
init([]) ->
    {ok, {#{strategy => one_for_all, intensity => 1, period => 5},
          [#{id => broker_revocation,
             start => {broker_revocation, start_link, []},
             restart => permanent,
             shutdown => 5000,
             type => worker,
             modules => [broker_revocation]}]}}.
```

- [ ] **Step 4: 运行确认通过**

Run: `rebar3 ct --suite=apps/broker_security/test/broker_revocation_SUITE`
Expected: PASS，6/6。

- [ ] **Step 5: 回归 `broker_security` 全 app 测试**

Run: `rebar3 ct --dir=apps/broker_security/test`
Expected: 全 PASS（0 failed）。

- [ ] **Step 6: 提交**

```bash
git checkout rebar.lock 2>/dev/null
git add apps/broker_security/src/broker_revocation.erl apps/broker_security/src/broker_security_app.erl apps/broker_security/src/broker_security_sup.erl apps/broker_security/test/broker_revocation_SUITE.erl
git commit -m "feat(security): 启动 broker_revocation 注册表进程（sup child + app init）"
```

---

## Task 3: `broker_security` 门面

**Files:**
- Modify: `apps/broker_security/src/broker_security.erl`
- Test: `apps/broker_security/test/broker_revocation_SUITE.erl`（加一个门面用例）

- [ ] **Step 1: 写失败测试**

加用例 `security_facade_delegates`：

```erlang
security_facade_delegates(_Config) ->
    false = broker_security:revoked(<<"dave">>),
    ok = broker_security:revoke(<<"dave">>, undefined),
    true = broker_security:revoked(<<"dave">>),
    ok = broker_security:clear_revocation(<<"dave">>),
    false = broker_security:revoked(<<"dave">>).
```

- [ ] **Step 2: 运行确认失败**

Run: `rebar3 ct --suite=apps/broker_security/test/broker_revocation_SUITE --case=security_facade_delegates`
Expected: FAIL（`broker_security:revoke/2` undef）。

- [ ] **Step 3: 实现**

在 `broker_security` 导出列表加 `revoke/2, clear_revocation/1, revoked/1`，并加：

```erlang
%% @doc MQ-17 #10: 吊销 `Principal' 并驱逐其活动连接。TTL 为 `undefined'
%% 表示永久。转发 `broker_revocation'，保持安全语汇集中在本模块。
-spec revoke(principal(), non_neg_integer() | undefined) -> ok.
revoke(Principal, TTL) ->
    broker_revocation:revoke(Principal, TTL).

-spec clear_revocation(principal()) -> ok.
clear_revocation(Principal) ->
    broker_revocation:clear(Principal).

-spec revoked(principal()) -> boolean().
revoked(Principal) ->
    broker_revocation:revoked(Principal).
```

- [ ] **Step 4: 运行确认通过**

Run: `rebar3 ct --suite=apps/broker_security/test/broker_revocation_SUITE`
Expected: PASS，7/7。

- [ ] **Step 5: 提交**

```bash
git checkout rebar.lock 2>/dev/null
git add apps/broker_security/src/broker_security.erl apps/broker_security/test/broker_revocation_SUITE.erl
git commit -m "feat(security): broker_security 暴露 revoke/clear_revocation/revoked 门面"
```

---

## Task 4: CONNECT 拒绝已吊销 principal（3.1/3.1.1/5）

**Files:**
- Modify: `apps/broker_proto_mqtt/src/mqtt_conn_fsm.erl:736-756`（`authenticate/2` 的 `Pr` 分支）
- Test: `apps/broker_proto_mqtt/test/mqtt_conn_SUITE.erl`

- [ ] **Step 1: 写失败测试**

在 `mqtt_conn_SUITE:all/0` 加 `connect_for_a_revoked_principal_is_refused_4`，并写用例：

```erlang
connect_for_a_revoked_principal_is_refused_4(_Config) ->
    ok = boot(),
    ok = broker_security:clear_revocation(<<"alice">>),
    ok = broker_security:revoke(<<"alice">>, undefined),
    {Conn, Connack} = mqtt_open(#{client_id => <<"c1">>,
                                  username => <<"alice">>,
                                  password => <<"pw">>}),
    #connack{return_code = 4} = Connack,
    #connack{} = Connack,
    {ok, _} = mqtt_close(Conn),
    ok = broker_security:clear_revocation(<<"alice">>).
```

> 说明：`mqtt_open/1` 用一个 TCP pair 起一个真实 FSM；`#connack{return_code = 4}` 断言拒绝码。

- [ ] **Step 2: 运行确认失败**

```bash
rm -rf _build/test/extras
rebar3 ct --suite=apps/broker_proto_mqtt/test/mqtt_conn_SUITE --case=connect_for_a_revoked_principal_is_refused_4
```
Expected: FAIL（return_code = 0，因为尚未检查吊销）。

- [ ] **Step 3: 实现**

在 `mqtt_conn_fsm:authenticate/2` 的 `Pr` 分支、`authorize/2` 之前插入吊销检查：

```erlang
        Pr ->
            case broker_security:revoked(Pr) of
                true ->
                    %% MQ-17 #10: 已吊销的 principal 与「坏用户名/密码」不可区分
                    %% （同为 rc 4 / v5 0x86），不泄露吊销状态。
                    broker_metrics:inc(auth_failures),
                    log(warning, "connect refused: principal revoked client_id=~p",
                        [D#st.client_id]),
                    {error, 4, D};
                false ->
                    case broker_security:authorize(Pr, {connect, D#st.client_id}) of
                        allow ->
                            {ok, D#st{max_qos = MaxQos,
                                      keepalive_ms = clamp_keepalive(KA, D),
                                      principal = Pr,
                                      tenant_id = broker_security:tenant_id(Pr)}};
                        deny ->
                            broker_metrics:inc(auth_failures),
                            log(warning, "connect not authorized for client_id=~p",
                                [D#st.client_id]),
                            {error, 5, D}
                    end
            end
    end.
```

- [ ] **Step 4: 运行确认通过**

Run: `rebar3 ct --suite=apps/broker_proto_mqtt/test/mqtt_conn_SUITE --case=connect_for_a_revoked_principal_is_refused_4`
Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git checkout rebar.lock 2>/dev/null
git add apps/broker_proto_mqtt/src/mqtt_conn_fsm.erl apps/broker_proto_mqtt/test/mqtt_conn_SUITE.erl
git commit -m "feat(mqtt): CONNECT 拒绝已吊销 principal（rc 4 / v5 0x86）"
```

---

## Task 5: 驱逐活动会话（发 DISCONNECT 抑制 Will）

**Files:**
- Modify: `apps/broker_proto_mqtt/src/mqtt_conn_fsm.erl`
  - `connect_success/2`（~`:623`）加注册
  - `terminate/2`（~`:2976`）加注销
  - 新增 `event(mqtt_evict, connected, D)` 子句（放在 `event(#disconnect{}, connected, D)` `:354` 之后）
  - 新增内部 `evict/1`
- Test: `apps/broker_proto_mqtt/test/mqtt_conn_SUITE.erl`

- [ ] **Step 1: 写失败测试**

加 `a_revoked_principal_live_session_is_evicted_without_will`：

```erlang
a_revoked_principal_live_session_is_evicted_without_will(_Config) ->
    ok = boot(),
    ok = broker_security:clear_revocation(<<"alice">>),
    %% 见证者订阅 alice 的 Will 主题。
    {W, _} = mqtt_open(#{client_id => <<"watcher">>,
                         username => <<"guest">>, password => <<"guest">>}),
    #suback{} = mqtt_subscribe(W, 1, [{<<"will/alice">>, 0}]),
    %% alice 带着 Will 连接。
    {A, _} = mqtt_open(#{client_id => <<"alice-conn">>,
                         username => <<"alice">>, password => <<"pw">>,
                         will_topic => <<"will/alice">>,
                         will_payload => <<"bye">>}),
    %% 吊销 alice：她的会话应收到 DISCONNECT 并被关闭。
    ok = broker_security:revoke(<<"alice">>, undefined),
    #disconnect{} = mqtt_recv(csock(A)),
    {ok, _} = mqtt_close(A),
    %% 见证者在合理窗口内收不到 Will。
    true = mqtt_silence(csock(W), 300),
    {ok, _} = mqtt_close(W),
    ok = broker_security:clear_revocation(<<"alice">>).
```

> 说明：`csock/1` 是 suite 里既有的 helper（`mqtt_conn_SUITE.erl:2788`）。`#disconnect{}`/`#suback{}` 来自 `mqtt_frame.hrl`（suite 已 include）。**每条吊销用例开头先 `ok = broker_security:clear_revocation(<<"alice">>)`** 清理上一用例可能残留的吊销（`stop_all/0` 不重启 `broker_security`，状态跨用例存活）。

- [ ] **Step 2: 运行确认失败**

```bash
rm -rf _build/test/extras
rebar3 ct --suite=apps/broker_proto_mqtt/test/mqtt_conn_SUITE --case=a_revoked_principal_live_session_is_evicted_without_will
```
Expected: FAIL（alice 未被驱逐，收不到 DISCONNECT）。

- [ ] **Step 3: 实现**

(a) `connect_success/2` 在 `D5 = count_connection(D4)` 之后、`schedule_round(...)` 之前加注册：

```erlang
            D5 = count_connection(D4),
            %% MQ-17 #10: 为该 principal 建活动连接索引，吊销时据此驱逐。
            ok = broker_revocation:register_session(D5#st.principal, self()),
```

(b) `terminate/2` 开头（`report_gauge` 之前或之后任一稳定点）加注销：

```erlang
terminate(Reason, _StateName, #st{partitions = Ps} = D) ->
    _ = broker_revocation:unregister_session(D#st.principal, self()),
    _ = report_gauge(conn_mailbox_len, -D#st.reported_mailbox),
    ...
```

> `unregister_session/2` 对 `undefined` principal 与死表都幂等（Task 1 的 ETS 帮手 clamp），不需要额外守卫。

(c) 新增 `event(mqtt_evict, connected, D)` 子句，紧跟 `event(#disconnect{}, connected, D)` 之后：

```erlang
event(mqtt_evict, connected, D) ->
    %% MQ-17 #10: 管理吊销驱逐。按「正常关闭」语义抑制 Will：
    %% 先撤 parked 的延迟 Will，再发 DISCONNECT，最后清本地 Will 再 stop。
    %% 不能照搬 protocol_error/3——它只发帧不清本地 Will，立即 Will 会在
    %% terminate(normal, ...) 时被 publish_will/2 发出去。
    _ = mqtt_will_server:cancel(D#st.client_id),
    RC = case vsn(D) of 5 -> 16#87; _ -> 0 end,   %% 0x87 Not authorized；311 无原因码
    log(info, "evicted by revocation (client_id=~p)", [D#st.client_id]),
    {stop, normal, send_frame(enc(#disconnect{reason_code = RC}, D),
                              D#st{will = undefined})};
```

> `enc(#disconnect{reason_code = 0}, D)` 对 vsn 4/3 编码为 `E0 00`（`encode_v4(#disconnect{})` 忽略 reason_code），符合 3.1.1/3.1 无原因码。

- [ ] **Step 4: 运行确认通过**

Run: `rebar3 ct --suite=apps/broker_proto_mqtt/test/mqtt_conn_SUITE --case=a_revoked_principal_live_session_is_evicted_without_will`
Expected: PASS。

- [ ] **Step 5: 加 3.1/v5 驱逐帧型用例**

加 `a_revoked_principal_v5_session_gets_disconnect_0x87` 与 `a_revoked_principal_mqtt31_session_is_evicted`：

```erlang
a_revoked_principal_v5_session_gets_disconnect_0x87(_Config) ->
    ok = boot(),
    ok = broker_security:clear_revocation(<<"alice">>),
    {A, #connack{return_code = 0}} =
        mqtt_open_v5(#{client_id => <<"alice-v5">>,
                       username => <<"alice">>, password => <<"pw">>}),
    ok = broker_security:revoke(<<"alice">>, undefined),
    #disconnect{reason_code = 16#87} = mqtt_recv_v5(csock(A)),
    {ok, _} = mqtt_close(A),
    ok = broker_security:clear_revocation(<<"alice">>).

a_revoked_principal_mqtt31_session_is_evicted(_Config) ->
    ok = boot(),
    ok = broker_security:clear_revocation(<<"alice">>),
    {A, #connack{return_code = 0}} =
        mqtt_open_v31(#{client_id => <<"a31">>,
                        username => <<"alice">>, password => <<"pw">>}),
    ok = broker_security:revoke(<<"alice">>, undefined),
    #disconnect{} = mqtt_recv(csock(A)),
    {ok, _} = mqtt_close(A),
    ok = broker_security:clear_revocation(<<"alice">>).
```

- [ ] **Step 6: 运行确认通过**

Run: `rebar3 ct --suite=apps/broker_proto_mqtt/test/mqtt_conn_SUITE --case=a_revoked_principal_v5_session_gets_disconnect_0x87,a_revoked_principal_mqtt31_session_is_evicted`
Expected: PASS，2/2。

- [ ] **Step 7: 跑整个 conn 套件（零回归）**

```bash
rm -rf _build/test/extras
rebar3 ct --suite=apps/broker_proto_mqtt/test/mqtt_conn_SUITE
```
Expected: 全 PASS（含 126 + 新增 4 个），0 failed。

- [ ] **Step 8: 提交**

```bash
git checkout rebar.lock 2>/dev/null
git add apps/broker_proto_mqtt/src/mqtt_conn_fsm.erl apps/broker_proto_mqtt/test/mqtt_conn_SUITE.erl
git commit -m "feat(mqtt): 吊销时驱逐活动会话（DISCONNECT 抑制 Will，3.1/3.1.1/5）"
```

---

## Task 6: admin HTTP `POST /api/v1/revoke` + 清除端点

**Files:**
- Modify: `apps/broker_admin/src/broker_admin_http.erl`（route 子句 + handler + 文档头）
- Test: `apps/broker_admin/test/broker_admin_SUITE.erl`

- [ ] **Step 1: 写失败测试**

在 `broker_admin_SUITE:all/0` 加 `revoke_endpoint_revokes_and_clears`：

```erlang
revoke_endpoint_revokes_and_clears(Config) ->
    {200, #{<<"status">> := <<"revoked">>}} =
        request_json(post, "/api/v1/revoke",
                     [{<<"authorization">>, <<"Bearer ", ?TOKEN/binary>>}],
                     #{<<"principal">> => <<"alice">>}, Config),
    true = broker_security:revoked(<<"alice">>),
    {200, #{<<"status">> := <<"cleared">>}} =
        request(delete, "/api/v1/revoke/alice",
                [{<<"authorization">>, <<"Bearer ", ?TOKEN/binary>>}], Config),
    false = broker_security:revoked(<<"alice">>).
```

- [ ] **Step 2: 运行确认失败**

Run: `rebar3 ct --suite=apps/broker_admin/test/broker_admin_SUITE --case=revoke_endpoint_revokes_and_clears`
Expected: FAIL（404 not_found）。

- [ ] **Step 3: 实现**

在 `route/4` 子句区（`route(post, <<"/api/v1/cluster/members">>, ...)` 之后）加两条：

```erlang
route(post, <<"/api/v1/revoke">>, _H, Body) ->
    revoke_principal(Body);
route(delete, <<"/api/v1/revoke/", Principal/binary>>, _H, _B) ->
    ok = broker_security:clear_revocation(Principal),
    json(200, #{status => cleared, principal => Principal});
```

加 handler：

```erlang
%%%_* 吊销（MQ-17 #10）-------------------------------------------------------

revoke_principal(Body) ->
    case parse_revoke_body(Body) of
        {ok, Principal, TTL} ->
            ok = broker_security:revoke(Principal, TTL),
            json(200, #{status => revoked, principal => Principal});
        {error, Error} ->
            json(400, Error)
    end.

parse_revoke_body(<<>>) ->
    {error, #{error => body_required}};
parse_revoke_body(Body) ->
    try json:decode(Body) of
        #{<<"principal">> := P} = M ->
            case {is_binary(P), maps:get(<<"ttl_ms">>, M, undefined)} of
                {true, undefined} -> {ok, P, undefined};
                {true, T} when is_integer(T), T >= 0 -> {ok, P, T};
                {true, _} -> {error, #{error => invalid_ttl_ms}};
                {false, _} -> {error, #{error => principal_must_be_a_string}}
            end;
        _ ->
            {error, #{error => body_must_have_principal}}
    catch
        _:_ -> {error, #{error => invalid_json}}
    end.
```

更新文档头（`:9-21`）加两行：

```erlang
%%   POST /api/v1/revoke       吊销 principal（body: principal, ttl_ms?）
%%   DELETE /api/v1/revoke/:p  解除吊销
```

- [ ] **Step 4: 运行确认通过**

Run: `rebar3 ct --suite=apps/broker_admin/test/broker_admin_SUITE --case=revoke_endpoint_revokes_and_clears`
Expected: PASS。

- [ ] **Step 5: 跑 admin 套件（零回归）**

Run: `rebar3 ct --suite=apps/broker_admin/test/broker_admin_SUITE`
Expected: 全 PASS，0 failed。

- [ ] **Step 6: 提交**

```bash
git checkout rebar.lock 2>/dev/null
git add apps/broker_admin/src/broker_admin_http.erl apps/broker_admin/test/broker_admin_SUITE.erl
git commit -m "feat(admin): POST /api/v1/revoke 与 DELETE /api/v1/revoke/:principal"
```

---

## Task 7: 文档收尾 + 全量回归

**Files:**
- Modify: `docs/compatibility/unifiedmq-mqtt-family-gap.md`
- Modify: `docs/mqtt-feature-comparison.md`

- [ ] **Step 1: 改缺口表 MQ-17 行**

把 `MQ-17` 行（`unifiedmq-mqtt-family-gap.md:68`）中「吊销踢人」一项标注为已完成——在该行末尾追加批注（不改正文其它词）：

```markdown
| MQ-17 | 产品，不是报文缺口 | 桥接、规则引擎、集群会话副本的故障注入证明、~~吊销踢人~~（2026-11-22 已完成：按 principal 吊销 + 驱逐活动会话，见 `docs/superpowers/specs/2026-11-22-mqtt-session-revocation-design.md`）、Prometheus/OTel | ... |
```

（保留该行其余文字与 `#5`–`#28` 的 pending 说明不动；只标 #10 子项。）

- [ ] **Step 2: 改 feature-comparison §4.2 #10**

`docs/mqtt-feature-comparison.md:130` 的 `- [ ]` 改 `- [x]`，并追加实现说明：

```markdown
- [x] **P0 吊销实时驱逐活动会话**（2026-11-22 已完成）：按 principal 吊销身份，broker 立即发 DISCONNECT 驱逐该用户全部活动连接（抑制 Will），并在吊销期内拒绝其重连；支持可选 TTL，默认永久直到显式清除。实现见 `broker_revocation` + admin `POST /api/v1/revoke`。
```

- [ ] **Step 3: 全量 CT（权威计数）**

```bash
export PATH="/home/wanli/Development/otp-29.1.2/bin:$PATH"
export PATH="$HOME/.local/bin:$PATH"
rm -rf _build/test/extras
rebar3 ct
```
Expected: **0 failed**；总数应为 1628 + 新增用例（约 1628 + 9）。记录实际 pass 数。

- [ ] **Step 4: dialyzer**

Run: `rebar3 dialyzer`
Expected: 无 warning、无 error。

- [ ] **Step 5: 提交**

```bash
git checkout rebar.lock 2>/dev/null
git add docs/compatibility/unifiedmq-mqtt-family-gap.md docs/mqtt-feature-comparison.md
git commit -m "docs: MQ-17 #10 吊销实时驱逐活动会话标记完成"
```

- [ ] **Step 6: 推送**

```bash
git push origin main
```

---

## Self-Review

**1. Spec coverage:**
- §1 目标（principal 粒度 / 可选 TTL 默认永久 / DISCONNECT 抑制 Will）→ Task 1（revoke/clear/revoked + TTL 读写判定）、Task 5（DISCONNECT + 清 Will）、Task 4（重连拒绝）。✓
- §3.1 `broker_revocation` 双表 + gen_server 责任边界 → Task 1。✓
- §3.2 `broker_security` 门面 → Task 3。✓
- §3.3 `mqtt_conn_fsm` 三触点 + `evict/2` 正确抑制 Will → Task 4（拒绝）+ Task 5（注册/注销/驱逐）。✓ 含 spec 修复的「不能照搬 `protocol_error/3`」关键点。✓
- §3.4 admin 端点 → Task 6。✓
- §4 数据流 / §5 边界（无会话、死 pid、竞态、表缺失 fail-open）→ Task 1 的 `evict/1` 跳死 pid、`revoked/1` 容错、`reap_dead_sessions/0`。✓
- §6 测试（单元/连接级/admin/零回归）→ Task 1、5、6、7。✓
- §7 文档 → Task 7。✓
- §8 范围外（集群/MQTT-SN/租户粒度）→ 明确不在任何任务内。✓

**2. Placeholder scan:** 无 TBD/TODO；所有代码步骤含完整代码；所有测试步骤含真实测试体与预期输出。✓

**3. Type consistency:** `revoke/2`、`clear/1`、`revoked/1`、`register_session/2`、`unregister_session/2`、`start_link/0`、`init/0`、`reset/0` 在 Task 1 定义，Task 2–6 一致引用；`broker_security:clear_revocation/1`（Task 3）与 admin handler（Task 6）一致；驱逐消息 `mqtt_evict` 在 Task 5 的生产与消费两侧一致；`{status, revoked|cleared}` 在 handler 与测试一致。✓

**已核实的两点（无剩余未知）：**
1. `mqtt_conn_SUITE` 有 `csock/1` helper（`mqtt_conn_SUITE.erl:2788`：`csock(#{sock := Sock}) -> Sock.`），测试可直接用 `csock(Conn)`；`mqtt_silence/2`（`mqtt_client.hrl:206-210`）返回 `true` 或 exit。
2. `broker_admin_SUITE` 的 `?TOKEN`（`:12`：`<<"s3cret-token">>`）与 `request_json/5`、`request/4`（`:617-624`）签名已核实存在。
