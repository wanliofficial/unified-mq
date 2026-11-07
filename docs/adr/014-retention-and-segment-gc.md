# ADR-014：retention、物理段删除与慢消费者守卫

- **状态**：拟议（S2 M0 起草，2026-11-07，待评审签认）
- **日期**：2026-11-07
- **关联**：README §5.3/§7.2；roadmap §4.1（retention）；ADR-011（`broker_storage` 8 回调冻结）；设计文档 `docs/design/s2-single-node-kernel.md` D3 / §4.6

## 背景（Context）

S1 的 `truncate/2` 是**逻辑**边界：它让 `read_from` 把 `BeforeOffset` 之前的数据视为不可读，但**从不删除文件**（`broker_storage.erl` 的 callback 注释明写 "never deletes files in S1"）。S2 必须把逻辑边界变成**物理删除**，否则磁盘只增不减。

但物理删除是不可逆的，必须回答三个问题：

1. **什么时候可以删**：只看 `committed` 够不够？慢消费者把水位钉住时怎么办？
2. **谁来删**：写路径（`wal_writer`）里删会阻塞发布；独立进程删又可能和恢复竞争。
3. **删了之后恢复怎么读**：`meta.log` 里的段清单、锚点索引、`read_from` 的起点都要自洽。

## 决策（Decision）

### 14.1 删除条件：`max_offset(segment) < min(live committed)` 且 `< 全局 committed`

一个段只有在**同时满足**以下条件时才可物理删除：

1. 段内**最大 offset** `<=` `broker_storage` 持有的全局 `committed`；
2. 段内最大 offset `< min(所有 live SubRef 的 cursor)`（即没有未 ack 的数据）；
3. 该段不是 `meta.log` 记录的**当前活跃写段**。

说明：S1 的 `committed = min(live cursor) - 1`，所以条件 1 与 2 不总是等价（storage 水位可能因崩溃回退）。两者都写是为了让"storage 不能越过订阅层删数据"与"订阅层不能越过 storage 删数据"双向成立，任一不满足都不删。

`min(live cursor)` 的 "live" 定义见 ADR-013 §13.6。**绝不为了回收磁盘越过慢消费者删数据**——这是本 ADR 的硬约束。

### 14.2 保留策略：`max_bytes` / `max_age` 双维，且二者只能收紧不能放宽删除条件

配置形状：

```erlang
{broker_storage, [
  {retention, #{
      max_bytes        => 1073741824,  % 1 GiB，0 = 不限
      max_age_ms       => 604800000,   % 7 天，0 = 不限
      check_interval_ms => 60000
  }}
]}
```

- `max_bytes`：分区总字节超过阈值时，从**最老**的可删段开始删，直到低于阈值。若没有可删段（都被慢消费者钉住），**只告警不删**。
- `max_age_ms`：段内最新记录的 `created_at` 超过阈值时可删（仍须满足 14.1）。
- 两者同时配置时取**更严格**者（即"两个条件都允许"才删）。
- `max_bytes = 0` 且 `max_age_ms = 0` 表示**只受 14.1 约束**（有界保留仍生效，不会无限删除）。

### 14.3 执行者：独立 `broker_storage_wal_gc` 进程，按分区扫描

- GC 是 `broker_storage_wal_sup` 下的**独立 worker**（每分区一个或全局一个按分区串行，实现细节由 S2-F 定），**不进入 `wal_writer` 的写路径**。理由：写路径的唯一职责是 append + group commit，加入删除会让发布延迟受磁盘扫描影响，违反 ADR-011 §11.2 的写路径形状。
- GC 通过 `broker_storage` **既有** `truncate/2` 推进逻辑边界，并在 storage 实现内部完成"删除已逻辑截断且满足 14.1 的段文件"。
- **不新增 behavior callback**：ADR-011 §11.1 的 8 个回调保持冻结。段 GC 是 storage 实现的内部行为，由 `truncate/2` 触发或由 GC 进程调用 `truncate/2` 触发；`capability/1` 不变。
- GC 与恢复的互斥：GC 在 `recover/2` 完成、`broker_queue` 就绪后才启动；启动顺序由监督树保证（T12 的 boot step 1 之后）。

### 14.4 删除的原子性与 `meta.log`

- 删除顺序固定：**先更新 `meta.log` 的段清单（标记段为 retired）并 fsync → 再 unlink 段文件 → 最后清理锚点索引**。
- 崩溃在任一步之间：恢复时以 `meta.log` 为准；已 retired 但文件仍在的段被忽略（并可在恢复期补删）；未 retired 但文件缺失视为**损坏**，按 ADR-011 的恢复策略报错，不静默跳过。
- 锚点索引（每 `index_anchor_interval` 条一个）只清理被删段范围内的锚点；跨段锚点保留。

### 14.5 可观测与守卫

- `broker_metrics` 计数：`retention_deleted_bytes`、`retention_deleted_segments`、`retention_blocked_by_slow_consumer`、`retention_blocked_by_committed`。
- `broker_queue:retention_status/0` 返回每分区的 `{oldest_segment, deletable_bytes, blocked_reason}`，供运维与测试断言。
- `retention_blocked_by_slow_consumer > 0` 时按 `warning` 级别告警（限频），提示具体 SubRef。

### 14.6 与 TTL 的分工

- **retention** 负责"段级"回收：整段满足条件才删。
- **TTL**（ADR-015）负责"消息级"过期：个别消息 `expires_at` 到期后不可投递，但所在段可能因其他未过期消息而保留。
- 两者不互相替代；TTL 到期消息在 retention 时随段一起消失。

## 后果（Consequences）

- **+** 磁盘有界，且删除条件可机械核对（两个不等式 + 活跃段排除）。
- **+** 写路径不变，发布延迟不受 GC 影响；ADR-011 的 8 回调保持冻结。
- **+** 慢消费者场景"只告警不删"，与 Gate 2（不 OOM、不误删）一致。
- **-** 慢消费者可能长期钉住磁盘；这是**故意**的选择，运维需通过告警与扩容/清理会话处理。
- **-** `meta.log` 多了 retired 状态，恢复逻辑复杂度上升；需要 T14 新增"段已删"恢复场景。
- **-** GC 扫描本身消耗 IO；`check_interval_ms` 需要按负载调，默认 60 s。
- **-** `max_age` 依赖 `created_at` 的时钟；时钟回拨会导致"看似很老"的段，恢复期需以 `meta.log` 的写入时间做二次校验（实现细节，S2-F 固化）。

## 验证（Validation）

- **单元**：`broker_storage_wal_SUITE` 新增——构造多段、推进 committed、断言只删除满足 14.1 的段；慢消费者钉住时**零删除**；`max_bytes`/`max_age` 各自与组合的阈值行为。
- **恢复**：T14 新增 R9——删除进行中 kill -9（在 `meta.log` retired 之后、unlink 之前注入），重启后不误读已删段、不把缺失段当损坏。
- **读路径**：删除后 `read_from(truncate_before - 1)` 仍返回从 `truncate_before` 起的数据（S1 已冻结的"clamp 到边界"语义，见 `broker_storage_wal.erl` / `broker_storage_memory.erl` 的 `Start = max(From, TruncateBefore)`），**不新增** `below_truncate` 错误；`read_from(truncate_before)` 正常。
- **不变量**：测试断言任何时刻 `truncate_before <= min(live committed) + 1`。
- **可观测**：`retention_status/0` 在慢消费者场景返回 `blocked_reason = slow_consumer`，且 metrics 计数增长。
- **Gate S2**：本条为 Gate 2 的证据之一；报告进 `docs/storage-and-recovery.md` 与 `docs/benchmarks/`。
