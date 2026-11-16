# 路由读路径压测 · 高扇出（2026-11-16）

- **对应**：[`docs/design/router-read-path.md`](../design/router-read-path.md) §7.3；[`2026-11-16-s11x-router-read-path.md`](2026-11-16-s11x-router-read-path.md) §3 第 1 条（当时明确未测）
- **代码版本**：`53cefd3`（`s11x-router-trie-index`）
- **复跑命令**：

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
rebar3 compile
erlc -pa _build/default/lib/broker_router/ebin -pa _build/default/lib/broker_core/ebin \
     -o /tmp/opencode/ebin tools/bench/router_read_bench.erl
erl -pa _build/default/lib/*/ebin -pa /tmp/opencode/ebin -noshell \
  -eval 'router_read_bench:run(#{shapes => [fanout], counts => [10, 100, 1000, 10000], duration_ms => 2000, warmup_ms => 300}), halt().'
```

---

## 0. 结论

上一份报告只测了 **spread**（N 个 filter 覆盖不同 destination，一条消息命中 3 个订阅）。这补上 **fanout**：N 个订阅者挂在**同一个** filter 上，一条消息命中全部 N 个——即「`devices/+/status`，1 万台设备」的形状。

| 扇出 | old | env | trie | 每订阅成本 |
|---|---|---|---|---|
| 10 | 43,136 ops/s / p50 25 µs | 71,973 / 17 µs | 65,603 / 19 µs | 2.5 µs |
| 100 | 18,889 / 57 µs | 15,873 / 59 µs | 17,610 / 61 µs | 0.6 µs |
| 1,000 | 568 / 1,767 µs | 1,183 / 829 µs | 1,231 / 747 µs | 0.75-1.8 µs |
| 10,000 | **6 / 154,553 µs** | 81 / 12,210 µs | **89 / 13,313 µs** | 1.2-1.3 µs |

三个结论：

1. **扇出下三种实现几乎一样慢，且都随 N 线性恶化**。1 万扇出时 `env` 与 `trie` 都是 ~90 ops/s，p50 12-13 ms。trie 的索引查找优势**完全消失**——索引只有 8 行，找得再快也没用。
2. **这条曲线的成本在结果集，不在索引**。§2 把成本拆开量过：1 万个匹配元素，`keysort` ~0.8 ms、去重 ~7 ms，其余是遍历与列表构造。`ordered_unique/1` 是主要项。
3. **改动 3（计划缓存）的收益上限就此确定**：计划缓存省的是「查找」这一步，而扇出下查找本就只占约 0.2%（8 行 ETS 读）。**在扇出场景里做计划缓存不会改善任何东西**。它唯一可能的收益仍在 spread + 重复 topic，而那正是 §3 第 2 条「多进程并发」尚未测量的部分。

---

## 1. 口径与机器状态

与上一份报告相同：裸 VM、OTP 28.3.1、单进程紧循环、`broker_router_hash:route/3` 直测。

**重要**：本轮测量时机器有**其它负载**（load average 6.3→8.0 / 16 核，约 10 GiB 内存已被占用）。因此**绝对吞吐**比上一份报告略低，但**同一轮内三种实现的相对关系**有效——它们在同一负载下交替跑。两次独立完整复跑的结论一致（`env`/`trie` 在 1 万扇出下分别为 81/89 与 82/91 ops/s）。

### 1.1 新增的断言

fan-out 形状最容易掩盖的一类错误是「快是因为少找了」：一个更便宜的扫描若悄悄丢掉一半结果，会跑出漂亮的数字。所以 `measure/5` 新增：

```erlang
Expected = expected_matches(Shape, N),
length(Matched) =:= Expected orelse erlang:error({wrong_match_count, ...})
```

`fanout` 期望 `N + 1`（全部扇出订阅 + `bench/#`），`spread` 期望 3。三种实现必须报出**相同数量**的订阅，否则直接失败。本报告所有行的 `matched` 都满足该断言。

---

## 2. 成本拆解：为什么扇出下 trie 没有优势

用编译模块单独测量（`-eval` 里测毫秒级会因 shell 环境保活全部绑定而虚高 10 倍以上——这个坑踩过一次）：

| 操作（10k 元素） | p50 |
|---|---|
| `lists:keysort/2` | 0.8 ms |
| `ordered_unique/1`（现有实现） | **7.3 ms** |
| 纯 `cons` 遍历（基线） | 0.4 ms |
| 只 fold map、不组元组累加器 | 7.1 ms |
| 元组累加器、不碰 map | 0.8 ms |

成本几乎全在 **map 的成员判定与插入**（7.1 ms），不在累加器的元组重建（0.8 ms）。

**结论：不改 `ordered_unique/1`。** 我试过用 `gb_sets` 替换，逐点对比：

| N | 现有 map | `gb_sets` 逐插 | 比值 |
|---|---|---|---|
| 10 | 4 µs | 5 µs | 0.80x |
| 100 | 43 µs | 72 µs | 0.60x |
| 1,000 | 406 µs | 1,314 µs | 0.31x |
| 10,000 | 7,293 µs | 23,616 µs | 0.31x |

`gb_sets` 在每个规模上都更慢（`gb_sets:from_list/1` 批量构建更快，但这里必须逐条去重）。**map 已经是正确选择**，这条路径没有可回收的空间。记录在此，是为了避免后续有人「顺手优化」成更慢的版本。

---

## 3. 这份数据没证明什么

1. **没测多进程并发下的扇出**。高扇出在生产里往往伴随「一条消息分发给 N 个进程」，那部分的成本在 `broker_router` 之后的投递环节，不在 `route/3`。
2. **没测 1 万以上的扇出**，也没有跨租户/跨节点的扇出。
3. **绝对数字受机器负载影响**（§1）。要引用吞吐，请在空闲机器上复跑。
4. **没测内存**（OQ-2 仍未解决）。

---

## 4. 对改动 3 的最终建议

**不做。** 理由从「没有数据」升级为「有反向数据」：

- 扇出下查找只占约 0.2%，计划缓存无可改善；
- spread 下 `env`/`trie` 在 1 万订阅已是平线（上一份报告）；
- 剩下唯一可能受益的形状是「高并发 + 相同 topic 反复发布」，而这块尚无数据。

若将来要做，前置条件是先有 §3 第 1 条的并发扇出数据。在此之前实现计划缓存，等于为一个尚未观测到的瓶颈增加一层缓存失效逻辑（并与 ADR-007 的「SUBACK 之后必须可投递」约束正面互动）。
