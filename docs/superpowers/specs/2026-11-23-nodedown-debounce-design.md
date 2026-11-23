# nodedown 去抖窗口 —— 设计（Plan 3 缺口 #1）

## 1. 背景与缺口

### 1.1 现状触发链路

`session_replication` 开启时，崩溃发现由 `broker_cluster_nodedown_mon`
（gen_server）完成：用 `erlang:monitor_node/2` 监视当前 ra 的**所有成员**，
收到 `{nodedown, O}` **立即**调用 `broker_cluster_nodedown:on_nodedown/1`——
后者遍历本地镜像里 O 拥有的每个会话分区，按副本固定序选第一个存活副本作
新 owner，`epoch+1`，提交既有 `{migrate,...}` 命令（fire-and-forget）。

- 监视进程：`broker_cluster_nodedown_mon`（`broker_cluster_sup` 子进程）。
- 派发入口：`on_nodedown/1`。
- owner 写入与 epoch fencing 由 `broker_cluster_meta_machine` 串行 apply
  兜底（唯一性来源）。

### 1.2 缺口

**触发本身没有去抖。** `monitor_node` 在 dist 连接一断就投递 `{nodedown, O}`，
它无法区分"节点真的崩溃"与"网络短暂抖动 / GC 停顿 / 瞬时分区导致 dist 连接
短暂中断又很快自愈"。当前实现对后者也会立刻迁移：误判崩溃 → 误迁移 +
不必要的 epoch 抖动（破坏性，但不会双 owner——epoch 单调 + ra 串行 apply 仍
保证唯一）。

缺口表（`docs/superpowers/specs/2026-11-22-session-state-replication-plan2-notes.md`
§3 #1）：**nodedown 触发无去抖**，影响"误迁移 + epoch 抖动"，后续"加去抖窗口（Plan 3）"。

### 1.3 与缺口 #2 的关系

缺口 #2（已交付，见 `2026-11-23-plan3-dualpath-mutex-design.md`）证明了
`claim` 与 `nodedown-migrate` 两路径在同分区同 epoch 下的互斥（恰好一赢、
owner 唯一、三节点收敛、竞态后写权限单一份）。#2 证明的是"派发什么"的正确性；
本缺口 #1 加固的是"何时派发"——避免派发本身被抖动误触发。二者正交、互补。

## 2. 目标与非目标

### 2.1 目标

- 在 `{nodedown}` 与 `on_nodedown` 派发之间插入**定时器去抖窗口**：收到
  `{nodedown, O}` 后等待一个可配窗口，窗口内若 `{nodeup, O}` 到达则取消、
  不迁移；窗口到点仍无 `{nodeup, O}` 才视为真崩溃并派发迁移。
- 默认窗口 1500 ms，可经应用环境变量 `nodedown_debounce_ms` 配置。
- 向后兼容：`nodedown_debounce_ms =:= 0` 时退回旧的"收到 nodedown 立即派发"
  行为。

### 2.2 非目标（YAGNI）

- 不做主动多探点探测、不做 quorum/ra 内部确认（设计备选 B/C，后续按需叠加）。
- 不改 `on_nodedown/1` 的选主逻辑、不改 `migrate`/meta_machine、不碰
  `conn_fsm`（那是缺口 #4）。
- 不引入新命令型。

## 3. 设计

### 3.1 改动点：只在 `broker_cluster_nodedown_mon`

去抖加在唯一接收 `{nodedown}`/`{nodeup}` 的 gen_server 里。`on_nodedown/1`
及下游全部不动。本质是把"收到 nodedown 立即派发"改成"收到 nodedown 起定时器，
到点仍无 nodeup 才派发"。

### 3.2 状态机

`init/1` 构造的 State 新增：

- `pending :: #{node() => reference()}` —— 每个正在去抖的节点的定时器引用。
- `debounce_ms :: non_neg_integer()` —— 从应用环境 `nodedown_debounce_ms`
  读取，缺省 1500。

三条消息处理（其余子句不变）：

1. `{nodedown, Node}`：
   - 若 `maps:is_key(Node, pending)`（同一断连的重复投递）→ 忽略。
   - 否则 `TRef = erlang:send_after(debounce_ms, self(), {nodedown_expired, Node})`，
     记 `pending := maps:put(Node, TRef, pending)`（`send_after` 投递的正是裸消息
     `{nodedown_expired, Node}`，便于 `{nodeup}` 侧按同形 flush）。
   - **不**立即调 handler、**不** demonitor（monitor 已被 ERTS 在 nodedown
     时自动清）。`debounce_ms =:= 0` 时直接走旧路径：立即 `handler(Node)`。

2. `{nodeup, Node}`：
   - 若 `Node` 在 `pending`：`erlang:cancel_timer/1`（拿 TRef）+ 从 mailbox
     `flush` 掉可能已投递的裸 `{nodedown_expired, Node}`（`receive {nodedown_expired, Node} -> ok after 0 -> ok end`），
     从 `pending` 删除 Node（窗口内自愈 → 不迁移）。
   - 再走现有 `monitor_members/1`（重挂/补挂监视）。

3. `{nodedown_expired, Node}`（自定义定时器消息，带 tag，不与系统消息混淆）：
   - 从 `pending` 删除 Node，调 `handler(Node)`（即 `on_nodedown/1`，迁移
     Node 拥有的全部分区）。
   - monitor 保持关闭；Node 若回来，后续 `{nodeup, Node}` 会重挂。

`terminate/2` 额外取消 `pending` 里所有定时器（卫生）。

### 3.3 数据流

```
monitor_node 断 → {nodedown,O} → 起定时器(debounce_ms)
   ├─(自愈) {nodeup,O} → 取消定时器 → 重挂 → 不迁移
   └─(到点) {nodedown_expired,O} → on_nodedown(O) → 选主+epoch+1+migrate
```

epoch 单调 + ra 串行 apply 仍是唯一 owner 来源；去抖只改"何时派发"，不改
"派发什么"。

### 3.4 错误处理与边界

- **flapping**（nodedown↔nodeup 反复）：nodeup 取消定时器；同一断连的重复
  nodedown 不重置已存在定时器（`pending` 命中即忽略）。最终只有"持续 down
  超过窗口"才迁移。
- **窗口内分区已被 claim 路径迁走**：到点 `on_nodedown` 仍按 epoch fencing
  安全 no-op（#2 已证互斥），不会双 owner、不会回滚已迁移结果。
- **`on_nodedown` 派发失败**（无 quorum）：与现状一致，fire-and-forget 不崩，
  等恢复或后续重挂再触发。
- **开关关闭**（`session_replication` 缺省 false）：mon 休眠、不监视（I2 不变量
  保持），去抖自然不触发。
- **默认关闭不变**：去抖只在既有 `session_replication` 开关开启时才武装，不改
  默认行为面。

### 3.5 配置

- `application:set_env(broker_cluster, nodedown_debounce_ms, N)`。
  - `N =:= 0` → 退回即时派发（向后兼容逃生口）。
  - 未设置 → 默认 1500。
  - 在 `arm/1` 武装时读取一次（运行期改动需 `rearm/0` 重读，与现有开关
    重挂机制一致）。

### 3.6 可测性

`start_link/1`（生产，handler = `on_nodedown/1`）与 `start_link/Handler)`
（测试，注入 `fun((node()) -> any())`）入口保持不变。测试通过设置
`nodedown_debounce_ms` 为小值（如 100 ms）驱动确定性去抖。

## 4. 测试策略

### 4.1 主：mon 单元测试（确定性，复用注入入口）

- nodedown 后窗口内收到 nodeup → handler **未**被调用。
- nodedown 后等过窗口 → handler **恰好一次**。
- 窗口内自愈 → 无迁移（owner/epoch 不变，间接：handler 未被调用即代表未派发
  `on_nodedown`）。
- `debounce_ms =:= 0` → nodedown 立即派发（向后兼容路径）。

### 4.2 可选（标注为后续，非本缺口验收项）

真 dist CT：瞬时 kill 并在窗口内重启 ⇒ 无迁移；kill 后保持 down ⇒ 迁移发生。
此项与缺口 #4（conn_fsm 端到端零重连真集群 CT）在真 dist 环境上重叠，
#1 阶段以 mon 单元测试为主，真 dist 区分留待 #4 或单独补充。

## 5. 影响面与不变量

- **改动文件**：`broker_cluster_nodedown_mon.erl`（新增 pending/debounce 状态
  与三条消息处理）。`broker_cluster_nodedown.erl`、`broker_cluster.erl`、
  `broker_cluster_meta_machine.erl` 不改。
- **保持的不变量**：
  - I2（默认关闭逐字节退化）：`session_replication` 关时 mon 休眠、不监视。
  - 唯一 owner：仍由 epoch 单调 + ra 串行 apply 保证，去抖不触碰该来源。
  - 命令面不变：复用既有 `{migrate,...}`，无新命令型。
- **行为面**：默认仍关闭（须显式开 `session_replication`）；开启后由
  即时派发变为去抖派发，默认窗口 1500 ms、可配、可 `0` 退回即时。

## 6. 验收判据

- mon 单元测试：窗口内 nodeup → 不派发；过窗口 → 恰好一次派发；`0` → 即时派发。
- 默认关闭下行为不变（不监视、不触发）。
- 生产代码零改动于 `on_nodedown`/migrate/meta_machine。
