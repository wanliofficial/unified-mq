# S2-F 执行计划：retention、段 GC 与慢消费者守卫

- **状态**：**进行中**。决策见 [ADR-014](../adr/014-retention-and-segment-gc.md)（S2 M0 冻结）与本计划 §2 的实现期解释（完成后写入 ADR-014 §14.7）。
- **对应**：`docs/design/s2-single-node-kernel.md` §4.6 / §5（M4）；roadmap §4.1（消息生命周期 / retention）
- **估算**：5 人日（T-F0 ~ T-F6）
- **退出条件**：段删除边界可机械断言（只删整段、活跃段不删、慢消费者钉住时零删除、`committed` 不足时零删除）；删除后 `read_from(truncate_before - 1)` 从 `truncate_before` 起返回；恢复不把已删段当损坏、也不把缺失段当已删；`retention_status/0` 与 4 个指标可观测；全量回归零 FAILED。

---

## 1. 目标与非目标

**目标**：把 S1 的「逻辑 `truncate_before`」变成**物理段回收**，同时保持 ADR-011 的 8 个存储回调冻结。

1. **删除资格以 storage 的 `committed` 为准**（ADR-014 §14.1）。`committed = min(live cursor) - 1`（K6），所以慢消费者会自然钉住删除；存储层**不反向调用订阅层**。
2. **只删整段**：段内最大 offset `< committed + 1`，且不是当前活跃写段。
3. **先写后删**：`meta.log` 先记录 retired 段与新的 `truncate_before` 并 fsync，再 `unlink`，最后清锚点（ADR-014 §14.4）。任何一步崩溃都可由恢复收敛。
4. **策略只收紧不放松**：`max_bytes` / `max_age_ms`，0 = 不限；两者同时配置时取交集（删除更少）。
5. **慢消费者只告警不删**（ADR-014 硬约束），并提供 `broker_queue:retention_status/0` 与指标。
6. **读路径不变**：`read_from/3` 仍按 `max(From, truncate_before)` 起点，删段后不新增错误码。

**非目标**

- `meta.log` 自身压缩（只追加；S2-F 只让它的 body 承载 retired 列表）。
- 消息级过期（S2-E 的 TTL 已实现，两者分工见 ADR-014 §14.6）。
- 跨分区全局容量配额、运维重放工具（S6）。

---

## 2. 实现决策（ADR-014 冻结范围内的解释）

> ADR-014 已冻结；下面八项是它对实现的留白，在此冻结并写入 ADR-014 §14.7。**均不改变 §14.1~§14.6 的对外契约。**

| # | 决策 | 依据 | 代价 |
|---|---|---|---|
| **F-1** | **删除资格直接用 `wal_writer` 的 `committed`**，不查询订阅层。`committed` 已经由 `broker_subscription_worker` 按 K6 推进为 `min(live cursor) - 1`，慢消费者自然把它钉住。存储层因此保持对订阅层零依赖。 | ADR-014 §14.1；ADR-011 §11.3 | 如果订阅层的水位推进出错，GC 的“安全”与水位同源；这是既有契约，不在存储层重复校验 |
| **F-2** | **`meta.log` 升 vsn 2**：body 为 `{2, Committed, TruncateBefore, Retired}`，`Retired` 是已物理删除（或计划删除）的段起始 offset 列表。v1 body 仍可读，`retired = []`。保留 `encode/2`、`write/3`（转发到新形状），新增 `encode/3`、`write/4`（计划原写 `encode/4`，实现期按参数形状落成 `encode/3`，见 §8）。 | ADR-014 §14.4 | 旧代码遇到 vsn 2 拒绝启动（§11.5 的有意行为） |
| **F-3** | **物理删除在 `wal_writer:truncate/2` 内完成**：列出分区目录 → 选出可删整段 → 写 `retired + truncate_before` 并 fsync → `unlink` → `wal_index:prune_below/2`。`truncate/2` 仍是 GC 唯一入口，不新增 behavior callback。 | ADR-014 §14.3/§14.4 | 一次 truncate 需要一次目录扫描；GC 默认 60 s 一次，可接受 |
| **F-4** | **独立 `wal_gc` 进程**挂在 `broker_storage_wal_sup` 下，全局一个，每 `check_interval_ms` 扫描 `data_dir/wal/*`，按分区串行。纯决策函数 `wal_gc:plan/3` 与文件扫描 `wal_gc:scan/1` 分离，测试不启动进程也能断言。 | ADR-014 §14.3 | 全局进程比每分区一个少一层监督树；删除本身在分区 writer 内串行 |
| **F-5** | **策略组合**：`max_bytes` 超阈值时从最老的可删段开始删到低于阈值；`max_age_ms` 用段文件的 **mtime**（ADR-014 §14.2 的 `created_at` 在读取前必须解析每条记录，与“GC 不做写路径工作”冲突；mtime 在段轮转后只增不减）。两者都配时取**交集**。两者都为 0 时不做策略性删除。 | ADR-014 §14.2 / 后果段 | 时钟回拨会让段“看起来更年轻”，方向安全（少删） |
| **F-6** | **删除上限 `committed + 1`**：GC 计算出的 cut 永远不超过 storage 水位；`truncate/2` 原有校验保留，拒绝时计 `retention_blocked_by_committed`。 | ADR-014 §14.1 | 无 |
| **F-7** | **`retention_status/0` 委托 `wal_gc:status/0`**；memory 后端返回 `[]`。每分区返回 `{oldest_segment, deletable_bytes, blocked_reason}`。 | ADR-014 §14.5 | 查询会做一次目录扫描；这是运维/测试入口，不在数据路径 |
| **F-8** | **指标**：`retention_deleted_bytes` / `retention_deleted_segments` 在 `wal_writer` 真正 `unlink` 成功时累加；`retention_blocked_by_committed` 在 `truncate/2` 拒绝非法边界时累加；`retention_blocked_by_slow_consumer` 在 `wal_gc` 有删除意图但 `committed` 钉住时按分区每次扫描累加。键已由 S2-A 冻结，不新增。 | ADR-014 §14.5；ADR-016 | 指标是累计值，不区分分区 |

### 2.1 恢复规则（写入 ADR-014 §14.7）

`wal_recovery:recover/3` 读到 `meta.log` 的 `retired` 后：

1. **retired 且文件仍在**（崩溃在 fsync 之后、unlink 之前）：删除文件，记 `{retired_segment_removed, Path}` 警告；不参与 replay。
2. **retired 且文件已不在**：正常，忽略。
3. **未 retired 但文件缺失**：按 §11.2 当作损坏（`segment_gap`），拒绝启动，不静默跳过。
4. **retired 列表包含最后一个（活跃）段**：视为损坏（`retired_active_segment`），拒绝启动——活跃段永远不删。
5. replay 的 `expect` 从**第一个现存段的起始 offset** 开始，不再硬编码 1。
6. 第一个现存段起点 `> truncate_before` 视为 gap（`segment_below_truncate_missing`）；正常不变量是 `first_start =< truncate_before`。
7. `committed` / `truncate_before` 的 clamp 规则不变（只夹低）。

---

## 3. 数据形状

### 3.1 `meta.log` v2

```erlang
{2, Committed, TruncateBefore, Retired}
%% Retired :: [StartOffset]  （升序、去重；物理删除的段）
```

- 每次 `write/4` 整条追加并 fsync；读最后一条可读记录。
- v1 body `{1, Committed, TruncateBefore}` 读作 `#{committed, truncate_before, retired => []}`。
- 未知 vsn 仍是 `{unsupported_vsn, Vsn}`。

### 3.2 `wal_gc:plan/3` 的输入输出

```erlang
Scan = #{partition := partition_id(),
         dir := file:filename_all(),
         total_bytes := non_neg_integer(),
         segments := [#{start := offset(), path := file:filename_all(),
                        size := non_neg_integer(), mtime_ms := integer()}]}
Info = #{committed := offset(), truncate_before := offset(),
         active_segment := file:filename_all()}
Decision = #{cut := offset(),                 % 新的 truncate_before
             deletes := [offset()],            % 要删的段起始 offset
             deletable_bytes := non_neg_integer(),
             blocked_reason := none | slow_consumer}
```

- `eligible(Segments, Committed, Active) = [{Start,Next,Size} || 非活跃段, Next =< Committed + 1]`
- `max_bytes`：`Total > MaxBytes` 时从最老 eligible 起删，直到 `Total - Deleted =< MaxBytes`。
- `max_age_ms`：`MTimeMs < Now - MaxAge` 的 eligible 段。
- 两者都配：交集。
- `cut = max(TruncateBefore, last deleted segment 的 NextStart)`；`deletes = []` 时 `cut = TruncateBefore`。
- 有删除意图（超 bytes 或有过期段）但 `eligible = []` ⇒ `blocked_reason = slow_consumer`。

---

## 4. 任务

| # | 任务 | 交付 | 依赖 |
|---|---|---|---|
| T-F0 | 本计划 + ADR-014 §14.7 实现注记 | 决策冻结 | — |
| T-F1 | `wal_meta` v2（`retired`、v1 兼容） | `encode/3`、`write/4`；`read/1` 返回 `retired` | — |
| T-F2 | `wal_index:prune_below/2` | 删除 `< Below` 的锚点，返回删除数 | — |
| T-F3 | `wal_writer` 物理删除 | state `retired`；`truncate/2` 写 meta → unlink → 清锚点 + 指标；`retention_info/1` | T-F1/T-F2 |
| T-F4 | `wal_recovery` retired / 已删段 | 上述 §2.1 七条；`expect` 从首个现存段起 | T-F1 |
| T-F5 | `wal_gc` 进程 + 纯决策 | `scan/1`、`plan/3`、`run/0`、`status/0`、定时 tick；挂到 `broker_storage_wal_sup` | T-F3 |
| T-F6 | 门面与配置 | `broker_queue:retention_status/0`；`sys.config.src` + `.app.src` 的 `retention` map | T-F5 |
| T-F7 | 测试 + 文档 | §5 清单；`docs/storage-and-recovery.md`、S2 设计 §4.6、`progress.md`、`docs/roadmap.md` | 全部 |

---

## 5. 测试清单

| 场景 | 用例（套件） |
|---|---|
| F-1 只删整段，活跃段保留 | `broker_storage_wal_SUITE:retention_deletes_only_whole_segments_below_committed` |
| F-2 慢消费者钉住时零删除 + `blocked_by_slow_consumer` | `broker_storage_wal_SUITE:a_pinned_committed_offset_deletes_nothing` |
| F-3 `max_bytes` 阈值 | `wal_gc_SUITE:max_bytes_deletes_oldest_until_under_the_limit` |
| F-4 `max_age_ms` 阈值 | `wal_gc_SUITE:max_age_deletes_only_expired_segments` |
| F-5 组合取交集 | `wal_gc_SUITE:both_limits_intersect_and_delete_less` |
| F-6 cut 不超过 `committed + 1` | `wal_gc_SUITE:the_cut_never_crosses_the_committed_watermark` |
| F-7 删除后 `read_from` 从 `truncate_before` 起 | `broker_storage_wal_SUITE:retention_deletes_only_whole_segments_below_committed`（该用例末尾断言 `read_from(Hd, 1, 100)` 返回 `[7,8,9]`） |
| F-8 恢复忽略并补删 retired 文件 | `wal_recovery_SUITE:retired_segments_are_ignored_and_removed` |
| F-9 恢复拒绝缺失段 / 活跃段被 retired | `wal_recovery_SUITE:a_missing_segment_is_still_corruption`、`wal_recovery_SUITE:retiring_the_active_segment_refuses_to_start` |
| F-10 meta v2 往返 + v1 兼容 | `wal_meta_SUITE:write_then_read_roundtrips_retired_segments`、`wal_meta_SUITE:a_v1_entry_reads_with_no_retired_segments` |
| F-11 锚点清理 | `wal_index_SUITE:prune_below_removes_only_older_anchors` |
| F-12 `retention_status/0` | `wal_gc_SUITE:status_reports_the_oldest_segment_and_deletable_bytes` |
| F-13 指标 | `broker_metrics_SUITE:keys_are_stable`（4 键已在 S2-A 冻结）+ `wal_gc_SUITE` 断言 counters |
| F-14 回归 | 全量 `rebar3 ct`、`dialyzer`、`xref` |

---

## 6. 验收命令

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
rm -rf _build/test/extras
rebar3 ct                       # 期望：360 + 新增用例，0 FAILED / 0 Skipped
rebar3 dialyzer
rebar3 xref                     # 与 53 条基线比对
```

---

## 7. 风险

1. **删除与恢复竞争**：删除只在 `truncate/2`（writer 进程内）执行，恢复只在 writer 启动时执行，二者不会并发。GC 进程只发 `truncate` 调用。
2. **`meta.log` 无限增长**：S2-F 只升 vsn，不压缩；每次 truncate 追加一条。按 60 s 一次、每天最多 1440 条，S2-I 的基准再评估是否需要压缩。
3. **mtime 语义**：复制/备份工具可能改写 mtime。方向是“看起来更年轻 ⇒ 少删”，不会误删未消费数据（仍有 committed 守卫）。
4. **慢消费者长期钉盘**：这是 ADR-014 的故意选择；`retention_status/0` 与 warning 日志是运维出口。

---

## 8. 交付记录（2026-11-08）

**状态：已实现并验证。** 决策 F-1~F-8 与 §2.1 恢复规则全部落地；对外契约（ADR-011 的 8 个 behavior 回调、`read_from/3`、`truncate/2` 签名）未改。

| 任务 | 落地 | 证据 |
|---|---|---|
| T-F0 | 本计划 + ADR-014 §14.7 | `docs/adr/014-retention-and-segment-gc.md` |
| T-F1 | `wal_meta` vsn 2：`{2, Committed, TruncateBefore, Retired}`，v1 读作 `[]`；`encode/3`、`write/4`，`encode/2`/`write/3` 转发 | `wal_meta_SUITE` 2 例 |
| T-F2 | `wal_index:prune_below/2`（保留 `{{meta}}` 行） | `wal_index_SUITE:prune_below_removes_only_older_anchors` |
| T-F3 | `wal_writer` state `retired`；`truncate/2` 写 meta → unlink → prune；`retention_info/1`；4 个指标 | `broker_storage_wal_SUITE` 2 例 + `broker_metrics_SUITE:keys_are_stable` |
| T-F4 | `wal_recovery` retired 七条规则；`expect` 从首个现存段起 | `wal_recovery_SUITE` 3 例 |
| T-F5 | `wal_gc`（`scan/1`、`plan/3`、`run/0`、`status/0`、定时 tick）挂 `broker_storage_wal_sup` | `wal_gc_SUITE` 8 例 |
| T-F6 | `broker_queue:retention_status/0`；`sys.config.src` / `.app.src` 的 `retention` map | `wal_gc_SUITE` status 用例 |
| T-F7 | 本文档、ADR-014 §14.7、S2 设计 §4.6、`storage-and-recovery.md`、`progress.md`、`roadmap.md` | 本节 |

**实现偏差（3 条）**

1. `wal_meta` 新增的是 `encode/3` 而不是计划里的 `encode/4`（参数是 `Committed, TruncateBefore, Retired`，没有第四个“版本”参数）。
2. `max_age_ms` 用段文件 mtime，而不是逐条解析 `created_at`（ADR-014 §14.7 第 5 条；方向安全，少删）。
3. `wal_gc` 是**全局一个**进程，不是每分区一个（ADR-014 §14.3 允许；实际删除在分区 writer 内串行）。

**验证结果**

| 命令 | 结果 |
|---|---|
| `rebar3 ct`（`rm -rf _build/test/extras` 后） | **376/376 passed**（基线 360 + 16 新增），0 FAILED / 0 Skipped |
| `rebar3 dialyzer` | 干净（0 warnings） |
| `rebar3 xref` | **61** 条（基线 53；新增 8 条全部记录在 ADR-014 §14.7 第 9 条） |

**测试用例映射（计划 §5 → 实际）**

- F-1/F-2：`broker_storage_wal_SUITE:retention_deletes_only_whole_segments_below_committed`、`a_pinned_committed_offset_deletes_nothing`
- F-3/F-4/F-5：`wal_gc_SUITE:max_bytes_deletes_oldest_until_under_the_limit`、`max_age_deletes_only_expired_segments`、`both_limits_intersect_and_delete_less`
- F-6：`wal_gc_SUITE:the_cut_never_crosses_the_committed_watermark`、`blocked_when_committed_pins_everything`
- F-7：`broker_storage_wal_SUITE` 的删除用例已断言 `read_from(1)` 从 7 起；计划的独立用例并入该断言
- F-8/F-9：`wal_recovery_SUITE:retired_segments_are_ignored_and_removed`、`retiring_the_active_segment_refuses_to_start`、`a_missing_segment_is_still_corruption`
- F-10：`wal_meta_SUITE:write_then_read_roundtrips_retired_segments`、`a_v1_entry_reads_with_no_retired_segments`
- F-11：`wal_index_SUITE:prune_below_removes_only_older_anchors`
- F-12：`wal_gc_SUITE:status_reports_the_oldest_segment_and_deletable_bytes`
- F-13：`broker_metrics_SUITE:keys_are_stable`（4 键 S2-A 已冻结）
- F-14：上表三条命令
