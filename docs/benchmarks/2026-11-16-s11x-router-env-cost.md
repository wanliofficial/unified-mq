# 每条 PUBLISH 取路由 env 的真实成本（2026-11-16）

- **对应**：[`docs/design/router-read-path.md`](../design/router-read-path.md) §2.3 / §7.3 第 1 条；[`2026-11-16-s11x-router-read-path.md`](2026-11-16-s11x-router-read-path.md)
- **代码版本**：`61b3aab` + 本次改动（`s11x-router-trie-index`）
- **复跑**：

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
rebar3 compile
erlc -pa _build/default/lib/broker_router/ebin -pa _build/default/lib/broker_core/ebin \
     -o /tmp/opencode/bench-ebin tools/bench/env_cost.erl
erl -pa _build/default/lib/*/ebin -pa /tmp/opencode/bench-ebin -noshell \
  -eval 'env_cost:run(#{iters => 500000}), halt().'
```

> **本文修正读路径报告的口径，不是推翻它。** 那份报告的 `env` 行测的是 `route/3` 本身，env 是 setup 阶段构造好的。但 `mqtt_conn_fsm:handle_publish_allowed/7` 在**每条 PUBLISH** 上都会调 `broker_router:env()`，这段成本不在那 9 个点里。所以「改动 1 省 21 → 13 µs」是**路由**的差值，真实的发布路径还要再加取 env 的成本——而那段成本原先比想象的大。

---

## 0. 结论

`broker_router:env/0` 在发布路径上的净成本：

| 版本 | ns/op（均值，减 baseline） | µs/op |
|---|---|---|
| 改动 1 之后、本次修正之前 | **1,694 / 1,672 / 1,771** | **1.67–1.77** |
| 本次修正之后 | **371 / 389 / 391** | **0.37–0.39** |

两行是**同一台机器、相邻时段、同一份 `env_cost.erl`** 的 A/B：回退 `apps/broker_router/src/` 到 `61b3aab` 跑三次，恢复后再跑三次。**约 4.4x**。

改动前（`gen_server:call` 取 env）没有在本机直接测——`env_cost.erl` 是为当前代码写的，回退到改动 1 之前需要另一个模块。读路径报告的 21 → 13 µs 差值给出约 8 µs 的量级，但那是**不同基准的不同口径**（那个基准里 env 是预先构造的，差值来自一次 call 对一次 map 查找），**不要把它与下表的绝对值相减**。

**改动 1 并没有把取 env 变成「一次 `persistent_term` 读」。** 它把一次 `gen_server:call` 换成了一次 `persistent_term` 读 **加两次 `application:get_env/3` 加一次 map 重建**。后三者合计约 1.4 µs——比它取代的那次 `persistent_term` 读（0.11 µs）贵一个数量级，约占整个 env 获取成本的 85%。

修正后 env 获取降到 **0.37–0.39 µs**，是修正前的 **1/4.4**，也已最接近「一次 `persistent_term` 读 + 一次存活检查」的 0.19–0.21 µs 下界（余下部分是两个函数调用的开销）。

---

## 1. 成本拆解（修正前）

| 步骤 | ns/op | 说明 |
|---|---|---|
| `baseline`（空函数调用） | 24–37 | 减掉 |
| `application:get_env(partitions)` | **573–611** | `ac_tab` 查找。**每次 publish 一次** |
| `persistent_term:get` | 101–124 | 改动 1 引入的那次读 |
| `store_env/1`（persistent_term + `is_process_alive`） | 185–211 | |
| `current_env/0`（+ app env + map 重建） | 1,031–1,107 | |
| `broker_router:env/0`（+ `application:get_env(backend)`） | 1,672–1,771 | **发布路径实际调用** |

（表中数字取自 A/B 的「修正前」一侧，三次运行的范围。）`application:get_env/3` 的一次查找略低于 0.6 µs，但它在发布路径上出现**两次**，合计约 1.2 µs，占 1.72 µs 的七成；其余是 persistent_term 读 0.1 + 存活检查 0.1 + map 重建。

**关键点：`partitions` 和 `backend` 都是启动期配置**，在 S4 分区迁移之前不可能变。每次 publish 去 `ac_tab` 里查一个常数值，是这次测量的全部意义。

---

## 2. 修正内容

1. **`broker_router_hash_store:init/1` 发布完整 env**：`#{store, tab, gen, partitions, hash}`。`partitions` 由 `broker_router:init/1` 显式传入（测试路径），或由 supervisor 启动时从 app env 读一次（生产路径）。
2. **坏配置在启动时失败**：`partitions <= 0` 或非整数现在让 store 的 `init/1` 返回 `{stop, {invalid_partitions, N}}`，即 supervisor 启动失败。此前它表现为「第一次 publish 时 publisher 崩溃」，或 `current_env/0` 返回一个没人检查的错误元组。
3. **`broker_router_hash:current_env/0` 变回纯读**：一次 `persistent_term:get` + 存活检查，不再合并 map。
4. **`broker_router_sup:init/1` 把 backend 发布到 `persistent_term`**，`broker_router:backend/0` 读它，不再每次 publish 查 app env。保留 `application:get_env` 回退，让没启动 app 的单元测试仍可用。

### 2.1 一个顺带修掉的不一致

改动 1 的注释声称 `init/1` 与 `current_env/0` 返回「同一形状的 map」，实际上两者是**分别**把 `partitions`/`hash` 合并进去的：`init/1` 用的是 Config 里的值，`current_env/0` 用的是 app env 里的值。两者一致纯属约定。现在两者都从 store 发布的那一份 map 来，**结构上**不可能不同。

---

## 3. 这份数据没证明什么

1. **没有端到端数字。** 这是微基准，不含 MQTT 解码、WAL append、投递。省下的 1.2 µs 在 1 KiB QoS1（基线 p50 20.4 ms）里占比可忽略——**不要用它推算吞吐提升**。
2. **测量机器有外部负载**（load 22–24 / 16 核，见 §4）。绝对数字偏低，但五行在同一轮内测，相对关系有效。
3. **没测 S4 分区迁移之后的行为。** 现在 `partitions` 在启动时冻结并发进 env。若 S4 要在运行期改分区数，必须重新发布 env（`persistent_term:put`），那是一次 VM 全局扫描——这是 S4 的约束，本改动把它显式化了而不是掩盖。
4. **没测 `persistent_term` 写入代价**。启动时一次 put 的全局扫描成本未计入，因为它不发生在热路径上。
5. **三次运行之间的波动**（修正后 371/389/391 ns，修正前 1672/1694/1771 ns）来自机器上的其他负载，不是测量方法问题；三次都落在同一侧，A/B 的方向不受影响。

---

## 4. 机器

| 项 | 值 |
|---|---|
| CPU | AMD Ryzen 7 4800U（16 逻辑核） |
| OTP / ERTS | 28.3.1 / 16.2 |
| 计量方式 | `erlang:monotonic_time()`（native 单位换算为 ns），500,000 次迭代取均值 |
| 同时负载 | load average 22–24（16 核），`node`、`zcode`、`java` 等外部进程 |

**为什么不用端到端压测来测这个**：这台机器上端到端数字会由资源争抢决定。`tools/bench/s2_bench.py` 的 docker 装置可用（镜像、release、paho 都在位），但在这个负载下跑出来的 A/B 差异不能当证据。**需要空闲机器。**

---

## 5. 对决策的影响

- **改动 1 的价值表述要再次收窄**：并发报告里说它「移除并发天花板」；这里补充——它当时并没有把 env 获取做到底，还留了约 1.4 µs 的每次调用常数值查询。两者不矛盾（那天花板确实移除了），但「env 获取已经是免调用」这句话在本次修正前是不完整的。
- **不改变任何已交付结论**：改动 2 仍是大表收益的全部来源（118x / 1,236x），计划缓存仍否决，OQ-2 结论不变。
- **新增的启动期强校验**是一个行为变化：坏 `partitions` 从「运行期崩溃」变成「启动失败」。这是改善，但升级时若有人配了非法值，会看到 broker 起不来而不是能起来再崩。
