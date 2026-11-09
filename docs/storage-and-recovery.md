# 存储与恢复

- **文档版本**：v1.0（S6，2026-11-09：补 §2 的 Mnesia/ra 目录、更新证据口径）
- **状态**：已实现并实测的磁盘格式、刷盘策略与恢复语义；S6 之后明确未实现的方向单列于 §14
- **对应**：ADR-007（可靠性等级）、ADR-011（`broker_storage` 冻结契约）、ADR-018（retain 树与 `.inflight` v2）、ADR-019（会话文件按 SubRef 与 v1→v2 迁移）、ADR-021（Metadata Raft 与复制提交）、设计文档 §5.3/§7/§8
- **代码**：`apps/broker_storage/src/`（WAL 七个模块 + 两后端）、`apps/broker_subscription/src/broker_subscription_files.erl`、`apps/broker_retain/src/broker_retain_files.erl`、`apps/broker_meta/src/broker_meta.erl`、`apps/broker_cluster/src/broker_cluster_meta.erl`
- **证据**：`rebar3 ct` **631/631**；T14 真实容器 `kill -9` 矩阵（R1a~R9 × 双档）；S2-B 掉电/OS 缓冲注入（`tools/fault/`、`tools/docker/s2_powerloss.py`）；T15 真实客户端 10/10

> 本文回答三个问题：**盘上有什么、什么时候真正落盘、崩溃后哪些字节会被信任**。架构分层与进程归属见 [`architecture.md`](architecture.md)。

---

## 1. 不变量（先看这五条）

1. **offset 稠密且从 1 开始。** 每分区一条连续日志；恢复后的 `high` = 最后一条持久记录 + 1。retention（S2-F）只删**段**，不删记录中间，所以现存第一条记录的 offset 等于 `truncate_before`。
2. **一条记录要么完整，要么不存在。** 记录头带长度与 CRC-32；撕裂尾在恢复时被截断到最后一个完整记录的边界，绝不"修补"半条记录。
3. **水位只能回退，不能超前。** `committed` 在崩溃后允许变低（代价是重复投递，契约允许），绝不允许高于日志里真实存在的记录（那会让 retention 删掉未确认数据）。
4. **订阅集合强一致，位置/在途允许有界回退。** `.subs` 每次变化即 fsync；`.cursors`/`.inflight` 按窗口批量落盘。前者是 Gate 的 R1b，后者是"至少一次"。
5. **ACK 只代表它声明的等级。** `fsync` 档的 PUBACK 发生在 `file:sync` 返回之后；`os_buffer` 档只承诺页缓存，掉电窗口由 S2-B 的注入测试覆盖（`tools/fault/os_buffer_loss.py`）。

---

## 2. 数据目录布局

```
<data_dir>/                          # 绝对路径，release 树外（J8），非 tmpfs/ramfs（J11）
├── wal/
│   ├── 0/
│   │   ├── meta.log                 # 每分区一个，追加式水位日志
│   │   ├── segment-00000000000000000001.log
│   │   └── segment-00000000000000001235.log
│   ├── 1/ …
│   └── 3/ …                         # 默认 4 分区（`broker_queue.partitions`）
├── sessions/
│   ├── exclusive.Z3Vlc3Q.dC8j.subs    # <kind>.<b64url(Key)>.<b64url(Filter)> + 扩展名
│   ├── exclusive.Z3Vlc3Q.dC8j.cursors
│   ├── shared.Z3JvdXA.dC8j.subs       # 一组一文件，consumers 在 body 里（ADR-019 §19.3）
│   ├── shared.Z3JvdXA.dC8j.cursors
│   └── Z3Vlc3Q.inflight               # 仍按 client：连接的不透明 blob
├── retain/
│   └── retain.v1                    # 整棵 retained 树，S2-C 新增（ADR-018 §18.2）
├── mnesia/                          # broker_meta.data_dir（S4 控制面）
│   └── Mnesia.<node>/               # Mnesia 自行追加节点名
├── ra/                              # broker_cluster.data_dir（S4 Metadata Raft）
│   └── <node>/                      # ra 自行追加节点名
└── config-audit.log                 # broker_admin 的脱敏配置审计（逐行 JSON）
```

- 五个 `data_dir` 分两类。**业务数据同根**：`broker_storage.data_dir`、`broker_subscription.data_dir`、`broker_retain.data_dir` 必须指向**同一个**绝对路径；否则 `.subs`/`retain.v1` 与 WAL 分居两地，节点重启后互不相认。**控制面在子目录**：`broker_meta.data_dir`（`<根>/mnesia`）与 `broker_cluster.data_dir`（`<根>/ra`）各自独占子目录，避免 Mnesia schema 文件与 ra 段混进 WAL 根目录。`config-audit.log` 也落在 storage 根下。
- 部署时**五个键都要显式配置为绝对路径**。只改业务三键时，`broker_meta`/`broker_cluster` 会退回相对默认值（`data`、`data/ra`），在只读挂载的 release 容器里会在启动阶段因无法建 Mnesia/ra 目录而中止。
- retained 树是一棵树、不是一份日志：`retain.v1` 每次变化整文件重写（temp + `file:sync` + `rename`），没有增量记录，也就没有"撕裂的树"。它有 `vsn`，未知版本与解析失败都是**启动失败**并带路径——用空树起来等于告诉每个新订阅者"从来没有过 retained 消息"。
- 目录名是分区 id（`0..N-1`），不是哈希值；`wal_recovery` 用目录里的段文件重建 offset，不读任何分区清单。
- `sessions/` 的文件名只是地址：真实身份（kind/key/filter）存在文件内容里（key 与 filter 可含 `/`，不能直接做文件名）。`.inflight` 是唯一仍按 client 命名的文件（ADR-019 §19.3：一条 pending 可同属多个 SubRef）。

---

## 3. WAL 记录格式

### 3.1 段记录（`wal_codec`）

```
┌────────┬────────┬──────────────┬──────────────┬─────────────────────────┐
│ Magic  │  Vsn   │    CRC-32    │     Len      │  Body (Len 字节)         │
│ 1 B    │ 1 B    │    4 B BE    │    4 B BE    │  term_to_binary(...)     │
│ 0x55   │  0x01  │ erlang:crc32 │              │  [{compressed,1}]        │
└────────┴────────┴──────────────┴──────────────┴─────────────────────────┘
        固定 10 字节头
```

Body 是一个**独立于 `#broker_message{}` 的元组**（磁盘协议 ≠ 内存模型）：

```erlang
{Vsn, MsgId, Tenant, Destination, Payload, PayloadEncoding,
 Persistence, ProtoMeta, Headers, Reliability, CreatedAt, ExpiresAt, EventBlob}
```

| 决定 | 原因 |
|---|---|
| `partition_id` 不进记录 | 目录已经权威；`decode/2` 由调用者传入 id 并盖上 |
| 不做 `[safe]` 解码（M10） | `proto_meta` 含协议私有原子（`qos`/`packet_id`/`retain`），`[safe]` 会让恢复依赖"哪些模块先被加载"；完整性由 CRC + Vsn 保证，且 WAL 是本地可信状态 |
| `compressed,1` | 消息体（JSON/文本）通常可压；这是每个记录独立压缩，不做跨记录压缩 |
| Body 元组带 Vsn | 加字段 = 增 Vsn + 增加解码分支，旧段仍可读；遇到**更高** Vsn 直接拒绝启动（不猜） |
| 不使用 O_APPEND | 恢复要能 `truncate_at/2` 切掉撕裂尾再从新末尾写；O_APPEND 会让"末尾"含义模糊。代价是每分区必须只有一个写者（由 supervisor 保证） |

### 3.2 水位记录（`wal_meta`）

```
Magic=0x4D, Vsn=0x02, CRC32, Len, term_to_binary({2, Committed, TruncateBefore, Retired})
```

- 追加写 + **每次 `file:sync`**。段文件只增不减；retention（S2-F）整段删除并把段起点记入 `Retired`，不做记录级压缩。最后一条可读记录就是当前水位。
- `Retired` 是已物理删除（或已决定删除、尚未 unlink）的段起始 offset 列表（升序去重，S2-F）。v1 body `{1, Committed, TruncateBefore}` 仍可读，解释为 `retired = []`。
- 尾部撕裂/损坏**不是错误**：`read/1` 返回 `{ok, LastGood, {torn|{corrupt,_}}}`，恢复采用最后一条完整记录并记一条 warning。
- Magic 与段记录不同（`0x4D` vs `0x55`）：把两种文件指错会**响亮失败**，而不是读出一个看似合理的水位。
- 初始值：`committed = 0`（没有任何确认）、`truncate_before = 1`（没有丢弃任何记录）、`retired = []`。

---

## 4. 段文件、轮转与读路径

### 4.1 命名

```
segment-<StartOffset:20 位零填充>.log
```

20 位零填充是**正确性**要求：`wal_segment:list/1` 按文件名字典序排序，只有等宽填充才能让字典序等于 offset 序（否则 `segment-10` 会排在 `segment-2` 前面）。`wal_segment:fold/5` 拒绝名字不符的路径——一个错名的文件会静默给整段记录重新编号。

### 4.2 轮转

- 阈值：`segment_max_records`（默认 100 000 条）。
- 轮转发生在 `wal_writer` 内部，**允许发生在一个批次的中间**：先对旧段 `sync`（若该批欠一次 fsync），关旧段，以 `Offset` 为新 `StartOffset` 开新段。
- 新段的第一条记录必然成为锚点（`(Offset - Start) rem Interval =:= 0` 在 `Offset = Start` 时天然成立），所以跨段续读只需一次 `wal_index:locate/2`。
- S2-F 起，整段可以被物理删除（条件见 §4.5）；`truncate/2` 仍先移动**逻辑**边界，只有边界覆盖的整段才 unlink。

### 4.3 稀疏索引（`wal_index`）

每分区一张 `public` `ordered_set` ETS，键两种：

```erlang
{{meta}}           => {Open :: boolean(), TruncateBefore}
{{anchor, Offset}} => {SegPath, FilePos}      % 每 index_anchor_interval（默认 256）条一个
```

- 锚点自带段路径：offset 跨段连续，但文件位置每段从 0 重新开始。
- 读路径在**调用者进程**内 `locate/2`（≤Offset 的最近锚点）→ `wal_segment:fold/5` 流式扫描（1 MiB 读取块）→ 边扫边筛掉 `< From` 的记录。消息体不驻留索引。
- `high` 刻意**不进**索引：读者看到的是磁盘上真实存在的字节，`read_from/3` 读到 torn tail 与读到日志末尾不可区分，也不需要区分。
- 索引表随 `wal_writer` 进程死亡而消失；旧句柄在此后读会 `badarg`，调用方必须 `recover/2` 换新句柄（`broker_queue` 负责）。

### 4.4 `read_from/3` 的语义

- `From = max(请求 offset, truncate_before)`；逻辑边界以下直接跳过。边界以下的段可能已被物理删除（S2-F），因此不能再假设"记录还在盘上"。
- 返回 `{ok, [{Offset, Msg}], NextOffset}`；`NextOffset = 最后一条的 offset + 1`；空页时等于请求值。
- 遇到**完整但不可读**的记录返回 `{error, {corrupt_record, Path, Reason}}`，**不**把损坏当成"日志到此为止"——否则订阅者会永远等一个真实存在的 offset。
- 关闭的分区返回 `{error, {closed, Id}}`，不与"已追平"混淆。

### 4.5 段回收（`wal_gc` + `wal_writer:truncate/2`，S2-F）

- **执行者**：`broker_storage_wal_sup` 下的静态 `wal_gc` worker，每 `retention.check_interval_ms`（默认 60 s）扫描 `data_dir/wal/<partition>`；实际删除在对应 `wal_writer` 进程内串行完成，写路径形状不变。
- **资格**：段内最大 offset `<= committed`，且该段不是当前活跃写段。`committed` 由订阅层按 K6 推进为 `min(live cursor) - 1`，所以慢消费者会钉住删除；存储层不反向查询订阅层。
- **策略**：`max_bytes`（超阈值时从最老段删到阈值以下）、`max_age_ms`（段文件 mtime 早于阈值）；0 = 不限；两者同时配置时取交集。没有可删段时只计 `retention_blocked_by_slow_consumer` / `retention_blocked_by_committed`，不删。
- **顺序**：写 `meta.log`（`retired` + 新 `truncate_before`）并 fsync → unlink 段文件 → `wal_index:prune_below/2` 清锚点。任一中间崩溃由恢复收敛（§7）。
- **指标**：`retention_deleted_bytes` / `retention_deleted_segments`；`broker_queue:retention_status/0` 返回每分区 `{PartitionId, OldestSegment, DeletableBytes, BlockedReason}`，`BlockedReason ∈ none | slow_consumer | no_writer`。
- **`read_from` 不变**：删除后 `From < truncate_before` 仍 clamp 到 `truncate_before`，不新增错误码。

---

## 5. Group commit（`wal_writer` + `wal_group_committer`）

### 5.1 为什么只有一个写者

每分区一个 `wal_writer` 进程，是"一次 fsync 服务多个 caller"的唯一可行做法：S0.3 实测逐条 fsync ≈ 1 148 msg/s，批 32 ≈ 17 155 msg/s（17×）。写者只做**队列、文件描述符、offset 分配、轮转、水位**；编码在**调用者进程**完成（`wal_codec:encode/1`），否则 CPU 密集的 `term_to_binary` 会成为批越大越严重的瓶颈。

### 5.2 决策规则

| 参数 | 默认 | 规则 |
|---|---|---|
| `batch_size` | 32 | 待处理请求数 ≥ 32 立即 flush |
| `max_batch_size` | 256 | 一次 flush 最多 256 条，给新 caller 让路 |
| `batch_delay_ms.os_buffer` | 0 | 立刻写（只承诺页缓存，不必等） |
| `batch_delay_ms.fsync` | 1 ms | 给同批 caller 一个合并窗口 |
| 欠 fsync 判定 | — | 批内**任一**请求要求非 `os_buffer`，整批做一次 `file:sync`（对 `os_buffer` caller 是更强的承诺，无需拆分） |
| 空队列 deadline | `infinity` | 原子序在整数之上，`Now >= infinity` 恒假，`min/2` 保留真实 deadline |

### 5.3 一次 append 的时序

```
caller:  encode(Msg) ──► gen_server:call({append, Bins, Mode})
writer:  ① capability 检查（失败：不分配 offset，立即 {error,{unsupported_sync_mode,_}}）
         ② 分配 offsets = High .. High+N-1，High += N     ← 先分配
         ③ 入 pending，计算 deadline
         ④ service/1：满批或到期 → flush
              take(max_batch_size) → 逐条写段（必要时轮转）
              → 若欠 sync：file:sync
              → 逐个 gen_server:reply({ok, Offsets})
caller:  收到 {ok, Offsets} 后才发 PUBACK
```

**两条致命规则**：

- **先分配 offset，后落盘**；因此一次写/同步失败绝不能继续服务——那会把已承诺的 offset 变成永久空洞。失败时：批内 caller 收到 `{error, Reason}`，writer `{stop, Reason}`，supervisor 重启后从磁盘重建，后续 offset 从真实末尾继续。
- **`terminate/2` 不丢弃等待者**：pending 批逐个回复 `{error, {writer_stopped, Reason}}`，再尝试最后一次 sync/close。调用者能区分"没写"与"写了但没答复"。

### 5.4 可靠性等级

| 等级 | 含义 | 谁支持 |
|---|---|---|
| `os_buffer` | 写入内核页缓存即返回；掉电可能丢 | 两个后端 |
| `fsync` | `file:sync` 返回后才返回 | 仅 WAL |
| `replicated_commit` | 本地 `fsync` + 复制到多数派才算成功（S4 已实现） | `broker_queue` 层，**不是后端能力值** |

`capability/1` 是**实例级**答案，只列**后端**能做的档：WAL 返回 `[os_buffer, fsync]`，内存返回 `[os_buffer]`。`replicated_commit` 不在任何后端的列表里——它由 `broker_queue:append/3` 在 `broker_cluster` 装好复制器时接受（先本地 `fsync`，再 `broker_cluster_rep:replicate/2` 求多数派），没有复制器时按 ADR-011 冻结的答案返回 `{error, {unsupported_sync_mode, replicated_commit}}`。`broker_queue:append/3` 与 writer 各做一次校验；`mqtt_conn_fsm` 在 CONNECT 时按后端推导 `mqtt_max_qos`（内存档为 0；此时 QoS1 的 SUBSCRIBE 在 SUBACK 回 `0x80`，MQTT 5 回 `0x87`）。

---

## 6. 水位与提交规则

| 水位 | 含义 | 维护者 | 持久化 |
|---|---|---|---|
| `high` | 下一条 append 的位置；最后一条持久记录 + 1 | `wal_writer` | 由段文件本身推导 |
| `committed` | 全局"最后已确认"的连续前缀 | 存储（由订阅层驱动） | `meta.log` |
| `truncate_before` | 逻辑不可读边界；S2-F 起也是整段删除的下界 | 存储 | `meta.log` |
| per-sub `cursor` | "下一条要读" | `broker_subscription` | `sessions/*.cursors` |

### 6.1 守卫

- `advance_committed/2`：仅接受 `Committed =< Offset =< High - 1`，否则 `{error,{invalid_committed_offset,...}}`。
- `truncate/2`：仅接受 `Trunc =< Before =< Committed + 1`，否则 `{error,{invalid_truncate,...}}` 并计 `retention_blocked_by_committed`。合法边界会物理删除所有「非活跃且最后 offset `< Before`」的整段（S2-F）。
- 两条都先更新内存、再 `meta.log` 追加 + fsync；fsync 失败则 writer 停止（水位不能"只在内存里成立"）。

### 6.2 订阅层的聚合与次序（K6）

```
committed = min(该分区所有活 cursor) - 1        % 没有活 cursor 的分区跳过
```

**必须先把 `.cursors` 刷盘，再调 `advance_committed`。** 反过来的话，崩溃会让 cursor 回退到 committed 之下，S2 的 retention 就会删掉订阅者从未 ack 的数据。这条次序是 `broker_subscription_worker` 的 flush tick 的硬约束。

### 6.3 为什么水位允许回退

崩溃后 `meta.log` 的尾记录可能丢。恢复把 `committed` 夹到 `high - 1`，把 `truncate_before` 夹到 `committed + 1`，并产生 warning（`committed_above_high` / `truncate_above_committed`）。方向永远偏低：代价是重复投递，而不是丢数据。

---

## 7. 恢复算法（`wal_recovery:recover/3`）

### 7.1 步骤

```
recover(P, Dir):
 1. wal_segment:list(Dir)
      → 目录不存在 / 无段 → {error, {not_found, P}}     （绝不自动 create）
      → 段按文件名排序；非段文件记入 unexpected
 2. wal_meta:read(Dir)
      → not_found → {error, {missing_meta, Dir}}          （create 没写完，不能假装什么都没确认）
      → {ok, Watermarks, MetaTail}                        （Watermarks 含 retired 列表）
 2b. 处理 retired 段（S2-F，ADR-014 §14.4）：
       retired 且文件仍在 → 补删 + warning {retired_segment_removed, Path}
       retired 且文件已不在 → 正常，忽略
       retired 列表含最后一个（活跃）段 → {error, {retired_active_segment, Path}}，拒绝启动
       未 retired 但文件缺失 → 仍是 {segment_gap, ...}，不当成已删
 3. 逐段 replay（顺序，不允许跳段）：
      起始 offset 必须等于 expect（第一个现存段的起点，而不是硬编码 1；
      若首段起点 > truncate_before 则 {segment_below_truncate_missing, ...}）
      fold 每条记录：校验 magic/vsn/CRC → 解码 → 记锚点（每 interval 条 + 每段首条）
 4. 处理最后一段的尾部（见 7.2）
 5. clamp 水位：committed ≤ high-1；truncate_before ≤ committed+1
 6. 把锚点/边界发布成新的 wal_index ETS
 7. 打开最后一段（append）与 meta.log（append）
 8. warnings 随 writer state 暴露（state/1），不主动写日志
```

### 7.2 尾部判定表

| 情形 | 判定 | 动作 |
|---|---|---|
| 最后一段 `clean` | 正常 | 继续 |
| 最后一段 `{torn, truncated}` | 崩溃中断写（预期） | `truncate_at(EndPos)` 切到最后一个完整记录；warning `{torn_tail_cut, Path, Bytes}` |
| **非最后**一段 `{torn, truncated}` | 中间段缺字节 = 真实空洞 | `{error, {torn_earlier_segment, Path}}`，拒绝启动 |
| `{corrupt, {unsupported_vsn, V}}` | 来自更新版本 | `{error, {unsupported_vsn, Path, V}}`，保留现场 |
| 其它 `{corrupt, _}`（CRC/解码头错） | 字节在但内容错 | `{error, {corrupt_record, Path, Reason}}`，不截断（截断会毁掉唯一证据和其后的记录） |
| 段起始 offset 不连续 | 段被删/错名 | `{error, {segment_gap, Expect, Actual}}` |
| 段在 `retired` 里且文件仍在 | 崩溃在 meta fsync 与 unlink 之间 | 补删文件，warning `{retired_segment_removed, Path}`；不 replay |
| 段在 `retired` 里且文件已不在 | 删除已完成 | 正常，忽略 |
| `retired` 含最后一个（活跃）段 | 违反"活跃段永不删" | `{error, {retired_active_segment, Path}}`，拒绝启动 |
| 首个现存段起点 `> truncate_before` | 边界以下出现未声明的空洞 | `{error, {segment_below_truncate_missing, ...}}` |
| 目录/段缺失 | 分区未创建或挂载错 | `{error, {not_found, P}}` → application 启动失败 |
| `meta.log` 缺失 | create 未完成 | `{error, {missing_meta, Dir}}` |
| `meta.log` 尾撕裂/损坏 | 水位更新被打断 | 用最后一条完整记录 + warning |

### 7.3 启动期与运行期的分野

- **boot 只 recover，从不 create**（ADR-011 §11.2）。干净目录启动必失败；运维先带外 `broker_queue:create_partition/1`。
- `create/2` 的顺序是：文件系统守卫 → 写布局（`meta.log` + `segment-1`）→ 立刻按 `recover/2` 打开。任一步失败不留"看起来已创建"的目录。
- writer 崩溃后，`broker_queue` 的调用者会拿到 `{error,{stale_handle,...}}`；分区 worker `reattach` 重新 `recover/2`。**不自动重试 append**：已抵达 writer 的请求可能消耗了 offset，静默重试会制造发布者不知情的重复。

---

## 8. 内存后端（`broker_storage_memory`）

- 两张 `public` ETS：`meta`（分区 → 最后 offset / committed / truncate）与 `msgs`（`{{Partition, Offset}, Msg}`）。
- `append/3` 在**调用者进程**内用 `ets:update_counter/3` **成块原子分配** offset，然后逐条 insert；`read_from/3` 也直读表。只有 create/recover/close/advance_committed/truncate 走 `broker_storage_memory_store`。
- `capability/1 = [os_buffer]`；`append` 传 `fsync` 返回 `{error,{unsupported_sync_mode,fsync}}`。
- `recover/2`：分区不存在 → `{error, {not_found, P}}`；存在但为空 → `Committed=0, High=1`。
- **已知观察窗口**：并发读者可能看见"offset 已分配、消息尚未 insert"的瞬时空洞，从而提前结束一页。S1 的真实链路是每分区单写者（`wal_writer`），内存后端只用于测试与 QoS0 场景，因此不触发；这条限制写在设计文档 §7.1。
- 进程死亡即数据全失（这是后端语义，不是缺陷）；`broker_storage_sup` 用 `one_for_all` 连同它一起重启。

---

## 9. 会话持久化（`broker_subscription_files`）

### 9.1 三个文件（v2：按 SubRef）

| 文件 | 内容 | 何时写 | fsync |
|---|---|---|---|
| `<kind>.<b64(Key)>.<b64(Filter)>.subs` | 一个 SubRef 的订阅集：`#{kind, key, filter, qos, consumers, init_cursors, created_at}` | join / leave / SUBSCRIBE / UNSUBSCRIBE | **每次变化** |
| `<kind>.<b64(Key)>.<b64(Filter)>.cursors` | 该 SubRef **全部**分区的 cursor 行（`#{kind, rows := #{P => Row}}`） | 窗口触发或 tick | 每次写 |
| `<b64(ClientId)>.inflight` | 不透明 blob，**形状 v2**（见下） | 每条 QoS1/QoS2 投递、ack、握手推进（受窗口约束） | 每次写 |

- **`.subs`/`.cursors` 按 SubRef，`.inflight` 仍按 client**（ADR-019 §19.3）。Shared/Failover 组是「一个订阅、N 个 consumer」：按 client 切的 cursor 会把同一份读位置复制 N 次（ADR-013 §13.1 禁止的第二份真相）；而 in-flight 是连接的不透明状态，一条 pending 可同属多个 SubRef，按 SubRef 切无处安放。
- 文件名 = `<kind>.<base64url(Key)>.<base64url(Filter)>`（无填充）+ 扩展名；内容 = `term_to_binary({vsn,2,SubRef,Body})`。名字只是地址，`kind/key/filter` 在 body 里也有一份；`subref_from_name/2` 可逆解析，boot 与孤儿清理只靠名字就能还原身份。
- 写入序列：**临时文件 → `file:sync` → `rename` 覆盖**。rename 在 ext4/xfs 上原子，读者只会看到完整旧文件或完整新文件；**不做目录级 fsync**（掉电语义见 §11）。
- 解码：`.subs`/`.cursors` 用 `[safe]`（body 只有 binary/integer/atom）；`.inflight` **不用** `[safe]`（含 `mqtt_conn_fsm` 的原子，M11）。
- `list_subscriptions/0` 只枚举 `*.subs`：集合是权威；没有对应 `.subs` 的 `.cursors` 是孤儿，boot 时删除（`prune_orphan_cursors/0`）。
- **`assigned` 不落盘**（ADR-019 §19.3）：它是活的分配，崩溃后必须全部重投。落盘的是 `low`（plain 行是 `offset`）、`acked`，以及 S2-E 的重试状态 `retries`。

`.cursors` 的两种行（`broker_cursor`）：

```
plain（exclusive/failover）：#{kind, offset := offset(), pending := [offset()],
                               retries := #{offset() => retry()}}
shared                      ：#{kind, low := offset(), acked := #{offset() => true},
                               assigned := #{offset() => ClientId},   % 不落盘
                               retries := #{offset() => retry()}}

retry() :: #{attempts := pos_integer(), first_at := integer(),
             next_at := integer(), reason := ack_timeout | dlq_append_failed}
```

- **`retries` 落盘（S2-E / ADR-015 §15.6）**：退避进度不能只活在内存里，否则重启会把 `max_attempts` 的预算清零。崩溃回退窗口内 `attempts` 可能回退，表现是**多重试几次**（方向安全，不会少投）；`restore/1` 保留 `retries`、清 `assigned`，`persist/1` 原样保留。
- **`next_at` 的两种取值**：整数 = 退避到期时刻（epoch ms）；`infinity` = 已下发给连接、等下一次 ack 超时。成员死亡/`release` 会把 `infinity` 解冻回 `0`（立即可重投），`attempts` 不丢。

`.inflight` 的 Body（ADR-018 §18.3 R3）：

```
#{vsn        := 2,
  next_pid   := pos_integer(),        % 出站 packet id 分配器的下一值
  pending    := #{Pid => {Refs, {Partition, Offset}}},
  qos        := #{Pid => 0 | 1 | 2},  % 与 pending 同键的级别
  released   := #{Pid => true},       % pending 子集：已见 PUBREC，待 PUBCOMP
  received   := #{Pid => true}}       % 入站：已回 PUBREC，待 PUBREL
```

- `released` 只描述 `pending` 里的条目（`restore/1` 会 `maps:filter` 掉孤儿），语义是"重连时发 PUBREL，不要再发一遍 PUBLISH"。
- retained 补发的条目**不写 blob**：它们没有 `{Partition, Offset}`，重连后客户端重新订阅就能再拿到，写进去只会永久占住窗口槽位。
- 读到 **v1** blob（`#{next_pid, pending}`）时按"全部是 QoS1、无 released/received"解释——S1 写下的文件在 S2 仍能恢复，代价是那次握手从 QoS1 语义重来（允许重复，符合 §4.1）。

### 9.2 三个刷盘档

| 层 | 触发 | 默认 | 零回退档 | 崩溃后回退窗口 |
|---|---|---|---|---|
| 订阅集合 | 每次 join/leave/SUB/UNSUB | 同步 fsync | 无（同一路径） | **0** |
| cursor | `cursor_max_regress` 条 **或** `cursor_flush_interval_ms` | 64 / 100 ms | `1` / `0`（每条 ack 即刷） | ≤ 64 条 / 100 ms |
| in-flight | `inflight_flush_interval_ms` | 100 ms | `0`（每次写入即刷） | ≤ 100 ms |

- cursor 的判定在 `absorb_advance/3`：`interval =:= 0` 时每次推进都刷；否则累计到 `max_regress` 才刷，tick 负责兜底。
- Shared 行的「推进」是 `low` 的前移（连续 acked 前缀），不是每条 ack 都移动文件里的位置；`acked` 与 `assigned` 的窗口也计入同一个刷盘判定。`assigned` 不进文件，回退窗口只作用于 `low`/`acked`。
- in-flight 精确性**不是** MQTT 3.1.1 的契约：session 恢复允许 broker 重发未确认消息（DUP=1）。因此默认档选择批量，R7 只断言"不丢、可多"。
- 写失败不阻塞内存状态：cursor 仍可读（重启才消费文件），下一个 tick 重试；这使回退窗口退化为"下一次 tick"，与 interval 参数的承诺一致。

### 9.3 boot step 2 的合并规则（K5）

boot 先跑 `broker_subscription_files:migrate_v1/0`（§9.4），再扫 `.subs`。对每个 `(SubRef, Partition)`：

```
1. 该 SubRef 的 .cursors 里有该分区     → 用它（plain 用 offset，shared 用 low/acked）
2. 否则 .subs 内嵌的 init_cursors 原点  → 用它
3. 两者都没有（订阅时该分区还不存在）   → 用该分区当前 live High（等价于"今天才订阅"）
```

**绝不**用 `Committed`、`0` 或"新的 High"作为兜底来替代第 1/2 条：回退到订阅起点之前会重放已确认数据（违 §4.2 语义），跳到 High 会跳过未 ack 的积压（丢消息）。

shared 行恢复时 `assigned` 一律清空（`broker_cursor:restore/1`）：崩溃后所有未 ack 的 offset 都该重投，谁在崩溃前领过它们不再有任何意义。`.subs` 的 `consumers` 还原为 `?MEMBERS` 的成员索引；**不建 group→members 的独立索引**，成员顺序就是 body 里的有序列表（failover 的 active = 第一个有 live attachment 的成员）。

boot step 3 再逐条 `broker_router:register(Filter, SubRef, #{})` 回填索引；`{error,{already_registered,SubRef}}` 视为成功（崩溃窗口内 Router 可能残留同一 SubRef）。

### 9.4 v1 → v2 迁移（ADR-019 §19.4）

S1 的布局是「一个 client 一个文件」：`{b64(ClientId)}.subs` 的 body 是 `[{Filter, QoS, [{Partition, InitOffset}]}]`，`.cursors` 的 body 是 `[{Filter, [{Partition, Offset}]}]`。boot step 2 在最前面调用 `migrate_v1/0`：

1. 读目录里每个 **v1** `.subs`（`{vsn,1,ClientId,Body}` 信封），为每个 `{Filter,...}` 生成 `SubRef = {exclusive, ClientId, Filter}`。
2. 读同名 v1 `.cursors`，把 per-filter 的行搬进新文件的 `rows`（plain 行 `#{kind => exclusive, offset, pending => []}`）。
3. 逐个 `temp + fsync + rename` 写 v2 `.subs` 与 `.cursors`。
4. **全部成功后**才 `unlink` 对应的 v1 `.subs` 与 v1 `.cursors`。

顺序不可颠倒：先写后删，崩溃的两种结果分别是「多做一次」和「丢了一个订阅」，只有前者可接受。

- **幂等**：重跑会重写相同内容的 v2 文件（同名同 body），再删 v1；v2 文件存在时不重复迁移。
- **失败即拒绝启动**：worker 把错误包成 `{session_scan, {migration, Path, Reason}}`，`broker_subscription` 应用启动失败。半迁移目录不能被当作「少了一些订阅」来服务。
- 迁移只产生 `exclusive` 订阅：S1 没有 Shared/Failover，`{ClientId, Filter}` 的唯一语义就是独占。

### 9.5 retained 树（`retain/retain.v1`，S2-C）

| 项 | 值 |
|---|---|
| 文件 | `<data_dir>/retain/retain.v1` |
| 内容 | `term_to_binary({vsn, 1, [{Topic, #broker_message{}}]})` |
| 写入 | 整树重写：临时文件 → `file:sync` → `rename` |
| 解码 | 不用 `[safe]`（`#broker_message.proto_meta` 带协议私有原子，与 M10/M11 同因） |
| 损坏 / 未知 `vsn` | **启动失败**，错误里带路径 |

刷盘时机（ADR-018 §18.2，O6）：

- **QoS1 / QoS2 的 `retain=1` PUBLISH**：先落盘，再回 PUBACK / PUBREC。也就是说，客户端拿到的 ack 一定覆盖了"这条 retained 值已经能被重启后的节点读到"。
- **QoS0 的 `retain=1` PUBLISH**：走窗口批量（`retain_flush_interval_ms`，默认 100 ms，`0` 表示每次写穿）。QoS0 本身不承诺持久化，所以 retained 值也可能丢——这与 QoS0 的语义一致，不是例外。
- 上限拒绝（`max_retained` / `max_retained_bytes` / `max_payload_bytes`）发生在**写盘之前**：树不变、仍向在线订阅者转发、publisher 仍拿到正常 ack，计数 `retain_overflow` 加一。**拒绝是有界的显式行为，不是丢消息**。
- retain 树与 `.inflight`/`.cursors` 之间没有跨文件一致性要求：树只在 PUBLISH 时变，会话文件只在投递/ack 时变，两者可以各自回退到上一个完整版本，最坏结果是"重复补发一条 retained 消息"。

### 9.6 DLQ 与 TTL 的存储事实（S2-E）

| 项 | 事实 |
|---|---|
| DLQ 位置 | 普通 WAL destination（默认 `$dlq/{tenant}/{original}`），由 `broker_router_hash` 定分区；不是独立目录/独立 app |
| 写入等级 | 固定 `fsync`（死信不能比原件更弱）；`expires_at = undefined`（否则过期消息进 DLQ 后立刻不可消费） |
| 原值 | `x-original-destination` / `x-original-message-id` / `x-failure-reason` / `x-attempts` / `x-tenant`；`message_id` 重新生成 |
| 嵌套 | 已在 DLQ 的消息再次失败不再嵌套死信（`already_dead_letter`），只推进 cursor |
| 失败 | `append` 失败时 cursor 不越过原件，`dlq_append_failed` +1；下一轮重试仍从原件再试 |
| TTL | `expires_at` 由 `build_message/7` 从 `message_ttl_ms` 填入；读者跳过过期记录并推进 cursor/committed，不改 `read_from/3` 契约 |
| 恢复 | 不需要新文件：`retries` 在 `.cursors` 里；DLQ 副本就是普通 WAL 记录，按常规恢复 |

---

## 10. 恢复矩阵（Gate S1 硬项，S2 后扩展）

### 10.1 断言定义与实测结果

驱动：`python3 tools/docker/s1_recovery.py --tier all`（真实 release + `erlang:28.3.1.0` 容器 + 宿主 ext4 数据卷 + `docker kill -s KILL`）。**两档各 10/10 PASS，共 20/20，0 FAIL。**

| # | 断言（简） | 默认档（K=64 / 100 ms） | 零回退档（K=1 / 0 ms） |
|---|---|---|---|
| **R1a** | 收到 PUBACK 的消息必须能读回 | PASS：`high=2`（恰一条），payload 原样 | PASS：`high=2`，同上 |
| **R1b** | 非 clean 会话的订阅集合必须免重发恢复 | PASS：`session_present=1`、`.subs` 90 B、未重发 SUBSCRIBE 仍能收到新消息 | PASS：`.subs` 87 B，同上 |
| **R2** | 未收到 PUBACK 的并发发布：全丢或完整前缀 | PASS：200 条恢复 31 条，`msg-0000..msg-0030` 无空洞 | PASS：恢复 29 条，无空洞、无撕裂 |
| **R3** | 已 ack 的 cursor 只允许 `≤K` 回退；之后的必须可再投递；不得丢 | PASS：`flushed_cursor=33`（两次独立运行 33/34，均在窗口内），重放 18/17 条，丢失 0、重复 0 | PASS：`flushed_cursor=51=N+1`，重放 0、重复 0（逐 ack 已刷盘） |
| **R4** | 撕裂尾必须被截断，之前记录完整 | PASS：3196 → 手工写 `55 01` 后 3198 → 恢复截回 **3196**；20 条全回 | PASS：3198 → 3200 → 3198；20 条全回 |
| **R5** | QoS0 允许丢失 | PASS：10 条 → WAL 存储 0、重启后投递 0 | PASS：同上 |
| **R6** | 20 k 条恢复 < 500 ms | PASS：**208.836 ms**（4 分区 high 4892/5055/4966/5091） | PASS：**204.706 ms** |
| **R7** | 已投递未 ack 的消息重连后必须重发且不丢 | PASS：重发（`dup=false`，按 cursor 追赶等价） | PASS：同上 |
| **R8** | QoS2 交握中崩溃：不丢、可重、PUBREL 幂等完成 | PASS（S2-C）：已存 1 条、重放 PUBREL 回 PUBCOMP、重连至少一次 + cursor 推进 | PASS：同上 |
| **R9** | 两成员 Shared：已 ack 不重投、未 ack 重投、`low` 最终推进 | PASS：6 条按 claim 分成 5/1；ack `[0,1,5]`、kill 后重投 `[2,3,4]`、已 ack 重投 0 条、`low=6` | PASS：分成 4/2；ack `[0,1,2]`、重投 `[3,4,5]`、已 ack 重投 0 条、`low=6` |

### 10.2 怎么读这张表

- **R3 的双档差异是参数语义的证据**：默认档停在 33/34（随 100 ms tick 落点变化），零回退档确定地停在 51 = N+1，且重放集为空。没有这个对照，`cursor_max_regress` 就只是纸面参数。
- **R4 的断言是"回到撕裂前大小"，不是固定数字**：记录编码长度随消息内容变化，两档的 3196/3198 只是同一次运行的事实。
- **R6 只计 `broker_storage_wal:recover/2`**（一次性 VM 内对 4 分区计时），不含容器/VM 启动——否则测到的是 Docker 抖动（M14）。
- **协议看不到的量**（WAL high、刷盘 cursor、重放耗时）由一次性 Erlang VM 以同一份 `sys.config` 只读打开数据卷读出，不修改状态。

---

## 11. 保证与不保证

### 11.1 当前保证

| 等级 / 层 | 保证 |
|---|---|
| `fsync` append | 返回即已 `file:sync`；`kill -9` 不丢 |
| `os_buffer` append | 返回即已写入页缓存；`kill -9` 不丢，掉电可能丢 |
| 订阅集合 | 每次变化 fsync + 原子 rename；`kill -9` 后完整 |
| cursor / in-flight | 有界回退（窗口见 §9.2）；方向只偏低；`retries` 随 cursor 窗口回退（多试几次，不会少试） |
| 恢复 | 撕裂尾截断；损坏/跨版本/缺段拒绝启动；水位只夹低；retired 段补删不 replay |
| 分区创建 | 只由带外动作产生；boot 不静默创建 |
| 段回收（S2-F） | 只删非活跃整段且最大 offset `<= committed`；写 meta 先于 unlink；慢消费者钉住时零删除 |

### 11.2 明确不保证

- **掉电（power loss）**：`kill -9` 不覆盖 `os_buffer` 窗口；`os_buffer` 档的掉电丢页缓存已由 S2-B 的 `tools/fault/os_buffer_loss.py` / `tools/docker/s2_powerloss.py` 注入验证。**dm-flakey 实机报告需要 root，尚未补**（`docs/roadmap.md` Gate S2 第 1 项保持未勾选）；目录级 fsync 也未实现。
- **不重复**：默认档 cursor/in-flight 有回退窗口，重复是契约内行为；只有零回退档能逼近"零重复"。
- **retention / 空间回收**：S2-F 起整段可删，但边界只到 `committed`；`meta.log` 自身仍只增不减（不压缩）。被慢消费者钉住的段不会被回收。
- **TTL 不删数据**：`expires_at` 只在读者侧过滤（S2-E）；过期记录仍占段空间，直到 S2-F 的段级 GC 回收。
- **DLQ 条数**：`dlq_size` 是 gauge，S2-E 只增不减（段删除在 S2-F 才发生），因此它是「累计死信量」的上界，不是「当前可消费条数」。
- **跨版本写**：旧代码遇到更高 Vsn 拒绝启动（这是有意的安全行为，不是兼容承诺）。
- **多写者并发**：每分区单写者是硬前提；绕过 `wal_writer` 直接写段文件会破坏 offset 稠密性。

---

## 12. 文件系统守卫与运维

### 12.1 守卫（`wal_writer:check_filesystem/2`）

- 读 `/proc/mounts`，按**挂载点**找到 `data_dir` 所属文件系统，按 fstype 判定。
- 只有 `tmpfs` / `ramfs` 被拒绝（易失的**定义**）；ext4/xfs/btrfs/zfs/overlay 以及无法判定的挂载一律放行——误拒会让健康节点起不来。
- 在 `start_link/3` 里执行（不是 `init/1`）：配置错误应作为 `{error, Reason}` 返回，而不是先起一个注定崩溃的子进程再拆掉。
- `require_durable_fs => false` 可关闭（CI 用），关闭时返回 warning 而非错误。
- 实测背景：本机 `/tmp` 是 tmpfs，`data_dir` 指过去会直接拒绝建分区（J11）；T14/T15 的数据卷因此落在宿主 ext4。

### 12.2 首次启动

```sh
# 1) 带外创建分区（broker_queue 只需 load，不必 start；重复创建是幂等的）
#    代码路径来自 release 的 lib/，容器里由 ERL_LIBS=/opt/unified_mq/lib 提供。
erl -noshell -boot no_dot_erlang -config <rel>/releases/0.1.0/sys.config \
    -eval 'application:load(broker_queue),
           {ok, _} = application:ensure_all_started(broker_storage),
           lists:foreach(
             fun(P) ->
                 case broker_queue:create_partition(P) of
                     ok -> io:format("created partition ~p~n", [P]);
                     {error, {already_exists, P}} -> ok;
                     {error, Reason} -> io:format(standard_error, "~p: ~p~n", [P, Reason]),
                                        halt(1)
                 end
             end, broker_queue:partition_ids()),
           halt(0).'

# 2) 启动节点
<rel>/bin/unified_mq foreground
```

`tools/docker/entrypoint.sh` 就是这段逻辑的可执行版本（T14 每次容器启动都跑它，对已恢复目录走 `already_exists` 分支）。

- 该 `erl` 的 CWD 必须与节点一致（relx 的启动脚本会 `cd $ROOTDIR`），否则相对 `data_dir` 会指向不同目录（J8）。生产配置用绝对路径即可规避。
- `create_partition/1` 只接受 `partition_ids/0` 里的 id（防止孤儿目录），重复创建返回 `{error,{already_exists,P}}`（幂等脚本可忽略）。

### 12.3 容量与观测

- 段数 ≈ 总记录数 / `segment_max_records`；`wal_gc` 按 retention 策略清理已过期的整段（S2-F）。
- `wal_writer:stats/1`（`appends/records/batches/syncs/rotations`）是观察 group commit 是否生效的唯一入口：批 8 条应看到 `records=8, batches=1, syncs=1`。
- `wal_writer:state/1` 的 `warnings` 携带恢复期的截断/夹水位/意外文件信息。

---

## 13. 与冻结契约的对应

| ADR-011 条款 | 实现 |
|---|---|
| §11.1 behavior 签名 | `broker_storage.erl` 的 8 个 callback，两个后端完整实现 |
| §11.2 create/recover 分离、boot 只 recover | §7.3、§12.2 |
| §11.3 水位语义、只许回退 | §6、§7.2 |
| §11.4 capability 显式、禁止静默降级 | §5.4、两个后端的 `capability/1` 与 append 校验 |
| §11.5 记录格式与版本 | §3 |
| §11.6 读路径索引 | §4.3 |

Gate「WAL 与内存实现可互换」的机械检查：`apps/broker_storage/test/broker_storage_contract.hrl` 的同一组 13 个用例在两个后端各跑一遍（内存 + WAL），头文件里不出现任何后端名/表名/目录名。

---

## 14. 演进（S6 之后）

| 方向 | 状态 | 会改什么 / 必须保持 |
|---|---|---|
| retention / compaction | **部分实现** | ✅ S2-F 已实现段删除（`wal_gc` + `meta.log` v2 retired）；`meta.log` 压缩仍待做。保持记录头格式；水位只低不高 |
| 掉电验证 | **部分实现** | ✅ S2-B 的 OS 缓冲注入；dm-flakey 实机报告待 root。保持 `fsync` 档的 ACK 时机 |
| 复制（S4） | **已实现** | ✅ 复制后端 `replicated_commit`，提交点在共识返回后。保持同一 behavior、ACK 与 capability 一一对应 |
| 慢消费者/背压（S2-G） | **已实现** | ✅ cursor 回收、离线 QoS0 skip、per-consumer 配额。保持 `committed = min(活 cursor)-1` 的方向性 |
| 会话迁入 Mnesia 控制面 | **未实现** | 把 `.subs`/`.cursors`/`.inflight` 迁入 `subscription_meta`/`cursor_meta`/`session_meta`。保持 `broker_subscription` 对外 API |
| retained 树扩容 | **未实现** | 增量日志或按 filter 分片，取代整树重写。保持 `vsn` 字段；损坏即启动失败 |

---

## 15. 证据索引

| 内容 | 位置 |
|---|---|
| WAL 内部单测（codec/segment/index/meta/recovery/writer/group commit） | `apps/broker_storage/test/wal_*_SUITE.erl` |
| 两后端契约（13 例 × 2） | `apps/broker_storage/test/broker_storage_contract.hrl` |
| 会话文件与订阅层 | `apps/broker_subscription/test/broker_subscription_SUITE.erl` |
| 端到端（wal/memory 双组 12 例） | `test/integration/s1_e2e_SUITE.erl` |
| 崩溃矩阵（R1a~R9，双档） | `tools/docker/s1_recovery.py` + `tools/docker/README.md`；报告 `tools/docker/run/report-*.json`（gitignored） |
| 真实客户端字节级 | `test/protocol-compatibility/mqtt/` |
