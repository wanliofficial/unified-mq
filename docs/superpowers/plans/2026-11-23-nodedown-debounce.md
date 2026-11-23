# nodedown 去抖窗口 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 给 `broker_cluster_nodedown_mon` 的崩溃发现加定时器去抖窗口：收到 `{nodedown,O}` 后等待可配窗口，窗口内 `{nodeup,O}` 取消则不迁移，到点仍 down 才派发 `on_nodedown`。

**Architecture:** 去抖只加在唯一接收 `{nodedown}`/`{nodeup}` 的 gen_server `broker_cluster_nodedown_mon`；`on_nodedown/1`、`migrate`、meta_machine 全不动。State 新增 `pending :: #{node()=>timer:tref()}` 与每节点定时器；用 `erlang:send_after` 投递裸 `{nodedown_expired,Node}` 消息。`nodedown_debounce_ms=0` 退回旧的即时派发（向后兼容）。

**Tech Stack:** Erlang/OTP 29.1.2、gen_server、Common Test、rebar3。

**设计依据：** `docs/superpowers/specs/2026-11-23-nodedown-debounce-design.md`

---

## File Structure

- **Modify:** `apps/broker_cluster/src/broker_cluster_nodedown_mon.erl` —— 去抖状态机（`init` 加 `pending`；`{nodedown}` 起定时器或即时派发；`{nodeup}` 取消 pending 定时器；新增 `{nodedown_expired,Node}` 派发子句；`terminate` 取消定时器；新增 `debounce_ms/0`、`cancel_pending/2` 私有函数）。
- **Modify:** `apps/broker_cluster/test/broker_cluster_nodedown_mon_SUITE.erl` —— 现有 3 测试设 `nodedown_debounce_ms=0` 保持即时；新增 2 个去抖单测（窗口内 nodeup 取消、过窗口派发恰好一次）。
- **Modify:** `docs/superpowers/specs/2026-11-22-session-state-replication-plan2-notes.md` —— §3 缺口表 #1 追加带日期批注。

**环境（所有 CT/编译步骤通用）：**
```bash
export PATH="$HOME/Development/otp-29.1.2/lib/erlang/bin:$PATH"
cd /home/wanli/.local/share/opencode/worktree/plan3-nodedown-debounce
rm -rf _build/test   # 每次 ct 前必做（陈旧 test-source 副本陷阱）
```
**rebar3 cover bug on OTP 29：** `rebar3 ct` 可能在全部测试通过后、末尾以 `undef {cover,modules,[]}` 非零退出——上游 bug，非测试失败。以 CT 日志判结果：`find _build/test/logs -name suite.log -path "*nodedown_mon*" -exec grep -E "^=result|^=successful|^=failed" {} \;`，看 `=result ok`。

---

## Task 1: mon 去抖状态机 + 单测

**Files:**
- Modify: `apps/broker_cluster/src/broker_cluster_nodedown_mon.erl`
- Modify: `apps/broker_cluster/test/broker_cluster_nodedown_mon_SUITE.erl`

- [ ] **Step 1: 改现有三个测试为即时路径（设 `nodedown_debounce_ms=0`），并加两个新用例到 `all/0`**

修改 `apps/broker_cluster/test/broker_cluster_nodedown_mon_SUITE.erl`。

把 `all/0` 改为（追加两个去抖用例）：
```erlang
all() -> [dispatches_nodedown_to_handler,
          dormant_when_disabled,
          nodedown_demonitors_after_dispatch,
          debounces_then_cancels_on_nodeup,
          dispatches_after_window_expires].
```

在 `dispatches_nodedown_to_handler/1` 里，紧接 `session_replication` 那行后加一行（设为即时派发，保持测试快）：
```erlang
    ok = application:set_env(broker_cluster, nodedown_debounce_ms, 0),
```
（即该函数开头两行 `set_env` 之后加这一行。）

在 `dormant_when_disabled/1` 里，同样在 `session_replication` 那行后加：
```erlang
    ok = application:set_env(broker_cluster, nodedown_debounce_ms, 0),
```

在 `nodedown_demonitors_after_dispatch/1` 里，同样在 `session_replication` 那行后加：
```erlang
    ok = application:set_env(broker_cluster, nodedown_debounce_ms, 0),
```

在文件末尾（`wait_for_demonitor/3` 之后）追加两个新用例与辅助：
```erlang
%% 去抖：窗口内 nodeup 自愈 → 取消定时器，绝不派发 on_nodedown（无误迁移）。
debounces_then_cancels_on_nodeup(_Config) ->
    ok = application:set_env(broker_cluster, session_replication, true),
    ok = application:set_env(broker_cluster, nodedown_debounce_ms, 300),
    Self = self(),
    H = fun(Node) -> Self ! {hit, Node} end,
    {ok, _} = broker_cluster_nodedown_mon:start_link(H),
    Node = ghost@host,
    broker_cluster_nodedown_mon_pid() ! {nodedown, Node},
    %% 窗口内（<300ms）自愈。
    timer:sleep(100),
    broker_cluster_nodedown_mon_pid() ! {nodeup, Node},
    receive {hit, Node} -> ct:fail(should_have_been_debounced)
    after 500 -> ok
    end.

%% 去抖：窗口到点、期间无 nodeup → 恰好派发一次（不重复）。
dispatches_after_window_expires(_Config) ->
    ok = application:set_env(broker_cluster, session_replication, true),
    ok = application:set_env(broker_cluster, nodedown_debounce_ms, 100),
    Self = self(),
    H = fun(Node) -> Self ! {hit, Node} end,
    {ok, _} = broker_cluster_nodedown_mon:start_link(H),
    Node = ghost@host,
    broker_cluster_nodedown_mon_pid() ! {nodedown, Node},
    receive {hit, Node} -> ok
    after 2000 -> ct:fail(no_dispatch_after_window)
    end,
    %% 二次确认：定时器只发一次，不会重复派发。
    receive {hit, Node} -> ct:fail(double_dispatch)
    after 200 -> ok
    end.
```

- [ ] **Step 2: 运行套件，确认新用例失败（红）**

```bash
export PATH="$HOME/Development/otp-29.1.2/lib/erlang/bin:$PATH"
cd /home/wanli/.local/share/opencode/worktree/plan3-nodedown-debounce
rm -rf _build/test
rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_nodedown_mon_SUITE.erl
find _build/test/logs -name suite.log -path "*nodedown_mon*" -exec grep -E "^=result" {} \;
```
Expected: `debounces_then_cancels_on_nodeup` 与 `dispatches_after_window_expires` 为 `=result failed`（当前代码收到 nodedown 立即派发 → 自愈用例的 `hit` 先到）。其余三个 `=result ok`（已设 0）。

- [ ] **Step 3: 实现去抖状态机**

修改 `apps/broker_cluster/src/broker_cluster_nodedown_mon.erl`。

3a. `init/1` 的 State 加 `pending => #{}`：
```erlang
    {ok, arm(#{handler => Handler, monitored => #{}, pending => #{}})}.
```

3b. 替换 `{nodedown, Node}`、`{nodeup, _Node}` 两个 `handle_info` 子句，并新增 `{nodedown_expired, Node}` 子句：
```erlang
handle_info({nodedown, Node}, #{handler := H, monitored := Mon, pending := Pend} = St) ->
    case debounce_ms() of
        0 ->
            %% 向后兼容：即时派发（旧的即时行为）。
            _ = try H(Node) catch _:_ -> ok end,
            unmonitor_node(Node),
            {noreply, St#{monitored := maps:remove(Node, Mon)}};
        Ms when is_integer(Ms), Ms > 0 ->
            case maps:is_key(Node, Pend) of
                true ->
                    %% 同一断连的重复 nodedown：忽略，不重置定时器。
                    {noreply, St};
                false ->
                    TRef = erlang:send_after(Ms, self(), {nodedown_expired, Node}),
                    {noreply, St#{pending := maps:put(Node, TRef, Pend)}}
            end
    end;
handle_info({nodeup, Node}, #{pending := Pend} = St) ->
    %% 窗口内自愈：取消该节点的去抖定时器（若有），不迁移；随后重挂监视。
    {noreply, monitor_members(St#{pending := cancel_pending(Node, Pend)})};
handle_info({nodedown_expired, Node}, #{handler := H, pending := Pend} = St) ->
    %% 去抖窗口到点、期间无 nodeup：确认真崩溃，派发迁移。
    _ = try H(Node) catch _:_ -> ok end,
    {noreply, St#{pending := maps:remove(Node, Pend)}};
handle_info(_Other, St) ->
    {noreply, St}.
```

3c. `terminate/2` 取消 pending 定时器（保留原 demonitor 卫生）：
```erlang
terminate(_Reason, #{monitored := Mon, pending := Pend}) ->
    _ = [unmonitor_node(N) || N <- maps:keys(Mon)],
    _ = [catch erlang:cancel_timer(TRef) || TRef <- maps:values(Pend)],
    ok.
```

3d. 在 `%%%_* Internal` 区新增两个私有辅助（放在 `unmonitor_node/1` 附近）：
```erlang
%% 去抖窗口（毫秒）：应用环境 nodedown_debounce_ms，缺省 1500；非负整数，非法值回落默认。
debounce_ms() ->
    case application:get_env(broker_cluster, nodedown_debounce_ms) of
        N when is_integer(N), N >= 0 -> N;
        _ -> 1500
    end.

%% 取消节点 O 的去抖定时器（若有）：cancel_timer + flush 可能已投递的裸
%% {nodedown_expired, O}，返回更新后的 pending。无定时器则原样返回。
cancel_pending(Node, Pend) ->
    case maps:take(Node, Pend) of
        {TRef, Rest} ->
            _ = erlang:cancel_timer(TRef),
            receive {nodedown_expired, Node} -> ok after 0 -> ok end,
            Rest;
        error ->
            Pend
    end.
```

- [ ] **Step 4: 运行套件，确认全绿**

```bash
export PATH="$HOME/Development/otp-29.1.2/lib/erlang/bin:$PATH"
cd /home/wanli/.local/share/opencode/worktree/plan3-nodedown-debounce
rm -rf _build/test
rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_nodedown_mon_SUITE.erl
find _build/test/logs -name suite.log -path "*nodedown_mon*" -exec grep -E "^=result|^=successful|^=failed" {} \;
```
Expected: 五个用例全 `=result ok`；`=successful 5`、`=failed 0`。

- [ ] **Step 5: 提交**

```bash
cd /home/wanli/.local/share/opencode/worktree/plan3-nodedown-debounce
git add apps/broker_cluster/src/broker_cluster_nodedown_mon.erl apps/broker_cluster/test/broker_cluster_nodedown_mon_SUITE.erl
git commit -m "feat(cluster): nodedown 监视加去抖窗口——窗口内 nodeup 取消、过窗口才派发（缺口 #1 T1）"
```

---

## Task 2:（可选，可 defer 到 #4）真 dist 瞬时抖动 vs 崩溃 CT

> **本任务可选。** 设计稿 §4.2 将其标为可选、与缺口 #4（conn_fsm 端到端零重连真集群 CT）在真 dist 环境重叠。若本阶段不想引入真 dist 时序测试（潜在 flaky），**跳过本任务**，直接做 Task 3。若做，按下列步骤。

**Files:**
- Create: `apps/broker_cluster/test/broker_cluster_nodedown_debounce_dist_SUITE.erl`

- [ ] **Step 1: 写真 dist 双用例套件（瞬时自愈不迁移 / 真崩溃迁移）**

脚手架复用 `apps/broker_cluster/test/broker_cluster_nodedown_mon_SUITE.erl` 的 peer 模式（3 真节点 ra 组）+ `broker_session_replica_SUITE.erl` 的 `prepare_peer`/`start_meta`。核心两用例：
- `blip_does_not_migrate`：起集群 → 设 `nodedown_debounce_ms=2000` → kill 某 owner peer 进程并在窗口内重启（`net_kernel` 重连触发 `{nodeup}`）→ 断言该分区 owner/epoch 未变（未迁移）。
- `crash_triggers_migration`：起集群 → 设 `nodedown_debounce_ms=300` → kill owner peer 且保持 down → 等过窗口 → 断言 owner 迁到存活副本、epoch+1。

> 注：精确"kill 进程 + 窗口内重启使其触发 `{nodeup}`"在真 dist 上有时序敏感性；实现时若某用例不稳定，按设计 §4.2 把它 defer 到 #4，不要在 #1 阶段放宽断言或硬凑时序。

- [ ] **Step 2: 运行**
```bash
export PATH="$HOME/Development/otp-29.1.2/lib/erlang/bin:$PATH"
cd /home/wanli/.local/share/opencode/worktree/plan3-nodedown-debounce
rm -rf _build/test
rebar3 ct --suite=apps/broker_cluster/test/broker_cluster_nodedown_debounce_dist_SUITE.erl
find _build/test/logs -name suite.log -path "*nodedown_debounce_dist*" -exec grep -E "^=result" {} \;
```
Expected: 两用例 `=result ok`；若不稳定，按 Step 1 注 defer 并报告。

- [ ] **Step 3: 提交（若做了）**
```bash
git add apps/broker_cluster/test/broker_cluster_nodedown_debounce_dist_SUITE.erl
git commit -m "test(cluster): 真 dist nodedown 去抖 瞬时自愈 vs 崩溃 双用例（缺口 #1 T2，可选）"
```

---

## Task 3: plan2-notes §3 缺口表 #1 追加带日期批注

**Files:**
- Modify: `docs/superpowers/specs/2026-11-22-session-state-replication-plan2-notes.md`

- [ ] **Step 1: 在缺口表 #1 行下（表后）追加批注**

在 `## 3. 缺口表（Plan 3 边界债）` 表格之后、`## 4.` 之前，追加：
```markdown
> **2026-11-23 追加批注**：缺口 #1 已补去抖窗口 —— `broker_cluster_nodedown_mon` 收到 `{nodedown,O}` 后起定时器（应用环境 `nodedown_debounce_ms`，默认 1500ms），窗口内 `{nodeup,O}` 取消则不迁移，到点仍 down 才派发既有 `on_nodedown`；`=0` 退回即时派发（向后兼容）。去抖只改"何时派发"，不改"派发什么"：唯一 owner 仍由 epoch 单调 + ra 串行 apply 保证。设计见 `2026-11-23-nodedown-debounce-design.md`。
```

- [ ] **Step 2: 提交**
```bash
cd /home/wanli/.local/share/opencode/worktree/plan3-nodedown-debounce
git add docs/superpowers/specs/2026-11-22-session-state-replication-plan2-notes.md
git commit -m "docs(plan2-notes): 缺口 #1 nodedown 去抖窗口已补（Plan 3）"
```

---

## Self-Review

**1. Spec coverage：**
- §2.1 目标（窗口内 nodeup 取消 / 到点派发 / 默认 1500 / 可配 / `=0` 退回即时）→ Task 1 Step 3（debounce_ms/0 分支、send_after 定时器、nodeup 取消）+ 单测覆盖。
- §3.2 状态机（nodedown 起定时器、nodeup 取消、nodedown_expired 派发、terminate 取消）→ Task 1 Step 3a-3d。
- §3.4 边界（重复 nodedown 不重置、窗口内已被 claim 迁走仍安全 no-op、开关关不触发）→ "重复 nodedown 忽略"在 Task 1 Step 3b；"已被迁走 no-op"由 epoch fencing 保证（#2 已证，无需新代码）；"开关关"由 I2 保持（mon 休眠）。
- §4.1 主单测（窗口内 nodeup 不派发 / 过窗口恰好一次）→ Task 1 Step 1 两新用例。
- §4.2 可选真 dist → Task 2（可选）。
- §6 验收判据 → Task 1 单测 + Task 3 批注。

**2. Placeholder scan：** 无 TBD/TODO；所有代码步骤含完整代码；Task 2 明确标可选并给出具体做法与 defer 条件。

**3. Type consistency：** State 新增键 `pending :: #{node()=>tref()}` 在 init/handle_info/terminate 一致；消息 `{nodedown_expired, Node}` 在 send_after、handle_info、cancel_pending flush 三处同形；`debounce_ms/0`、`cancel_pending/2` 命名前后一致；测试注入 `start_link(H)` 入口不变。
