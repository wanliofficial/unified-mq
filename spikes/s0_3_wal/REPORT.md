# S0.3 Spike Report: WAL + Group Commit

**结论：方案 A（Erlang 原生 WAL/Segment + Raft 复制）在单机可行，但 fsync 档吞吐只有 ~1.1k msg/s，必须依赖组提交。**

- **对应 Roadmap**：`docs/roadmap.md` §4 S0.3
- **验证对象**：README 5.3 方案 A、README 6.3 可靠性等级
- **环境**：OTP 28 (erts-16.2)，16 核，数据目录 `/home/wanli/workspace/unified-mq`（ext4 on nvme0n1p5）
- **代码**：`spikes/s0_3_wal/`

---

## 1. 关键结论

| 问题 | 结论 |
|---|---|
| 自研 WAL 是否可行 | **可行**。崩溃恢复无损，撕裂记录被正确截断 |
| 单条 fsync 能否撑住生产 | **不能**。871 µs/条 = ~1.1k msg/s，比预期差一个数量级 |
| 组提交收益 | **17x**。batch=64 时 ~19k msg/s |
| 推荐组提交窗口 | **batch=32~64**（吞吐拐点），batch≥256 收益趋平 |
| 恢复速度 | 20k 记录重放 **143 ms**，无索引场景下可接受 |
| fsync 档崩溃后是否丢消息 | **未丢失**（3000/3000 全恢复） |

---

## 2. 实测数据

### 2.1 单写顺序追加（20,000 条，256 B payload）

| 等级 | 吞吐 | 单位耗时 |
|---|---|---|
| `os_buffer`（仅 write） | 33,818 msg/s | 29.6 µs/msg |
| `fsync`（每条刷盘） | **1,148 msg/s** | **871.2 µs/msg** |

**这是本 spike 最重要的发现**：每条消息单独 fsync 时，吞吐受磁盘限制而非 CPU 限制。`os_buffer` 的 33k msg/s 说明编码和写入路径本身不是瓶颈，慢的是刷盘。

### 2.2 组提交窗口扫描（2,000 条）

| batch | 吞吐 | 单位耗时 | 每 fsync 消息数 |
|---|---|---|---|
| 1 | 1,177 msg/s | 849.6 µs | 1 |
| 8 | 7,239 msg/s | 138.1 µs | 8 |
| 16 | 11,317 msg/s | 88.4 µs | 16 |
| **32** | **17,155 msg/s** | 58.3 µs | 31 |
| **64** | **19,944 msg/s** | 50.1 µs | 62 |
| 128 | 22,792 msg/s | 43.9 µs | 125 |
| 256 | 26,517 msg/s | 37.7 µs | 250 |
| 512 | 27,211 msg/s | 36.8 µs | 500 |

吞吐在 batch=32~64 出现明显拐点，之后增益递减。
**建议：组提交窗口取 32~64 条，或按 1 ms 延迟上限触发。**

### 2.3 崩溃恢复（外部 VM + `kill -9`）

| 等级 | acked | 恢复 | 丢失 | 尾部额外恢复 |
|---|---|---|---|---|
| `fsync` | 3000 | 3000 | **0** | 12,958 条 |
| `os_buffer` | 3000 | 3000 | 0 | 10,100 条 |

**两档都未丢失，但这不是 fsync 档独有的保证。** 见 §3 的测量限制说明。

---

## 3. 测量限制（重要，不要误读）

**`kill -9` 无法证明 `os_buffer` 档的可靠性。**

`kill -9` 只销毁进程。已通过 `write(2)` 交给内核的数据保存在 **OS page cache** 中，进程死亡不会丢弃它。只有内核 panic 或掉电才会丢失。

因此：

- 本次两档都显示"丢失 = 0"是**预期结果**，不能据此声称 os_buffer 档安全
- `fsync` 档"丢失 = 0"是**真实结论**，因为它主动落盘，不依赖 page cache
- 要真正测量 os_buffer 档的丢失窗口，需要**掉电模拟器**（dm-flakey / dm-log-writes / 虚拟机硬复位），本 spike 未覆盖

### 对 ACK 契约表的影响

README 6.3 要求的可靠性等级表，目前状态：

| 等级 | 承诺 | 实测状态 |
|---|---|---|
| `os_buffer` | 进程崩溃不丢，掉电可能丢 | **未测**，需掉电模拟 |
| `fsync` | 掉电不丢已刷盘消息 | ✅ 已验证（进程崩溃场景） |
| `replicated_commit` | 多数派持久化 | S4 验证 |

**在补上掉电测试前，`os_buffer` 一栏不得写入具体数字。**

---

## 4. 测试覆盖

`spikes/s0_3_wal/test/s0_3_wal_tests.erl`，6 个 EUnit 用例全部通过：

| 用例 | 验证内容 |
|---|---|
| `basic_append_read_test` | 基本写读往返 |
| `reopen_appends_test` | 重开日志不截断已有数据 |
| `torn_tail_recovery_test` | **撕裂记录**：崩溃在写入中途，恢复时干净截断于撕裂点，不报错、不虚构记录 |
| `segment_rotation_test` | 段轮转后跨段读取顺序正确 |
| `restart_with_segments_test` | 多段日志重启后不新建段 |
| `real_disk_guard_test` | tmpfs 防护生效 |

撕裂记录处理对应 README 11.3「WAL 部分写入」，是本 spike 最重要的正确性断言。

---

## 5. 过程中修正的错误（记录以免重犯）

| 错误 | 现象 | 根因 |
|---|---|---|
| `/tmp` 上测量 | fsync 显示 43 µs，实为无效 | `/tmp` 是 tmpfs，fsync 空操作 |
| `filelib:sync/1` | undef | 正确 API 是 `file:sync/1` |
| 变量名 `sec` | badmatch | `sec` 是自动导入 BIF `erlang:sec/0`，`sec = ...` 变成模式匹配 |
| `is_segment` 返回布尔 | `io_lib:format` 收到 `false` | 返回值语义与 `load_segments` 需求不符 |
| kill 时机错误 | fsync 档只恢复 462/3000 | 在写前缀过程中就 kill，测的是进度不是耐久性 |
| `Batch1` 变量名 | 语法错误 | `Batch1` 被解析为原子 `'Batch1'` |
| 嵌套列表推导缺括号 | `syntax error before '.'` | `[[f(X) || X <- L], ...]` 内层未闭合 |

其中两条对生产代码有直接影响：

1. **`/tmp` 陷阱**：`s0_3_bench:assert_real_disk/0` 现在会在数据目录 fsync < 100 µs 时拒绝出报告，防止再次把 tmpfs 当磁盘。
2. **kill 时机**：harness 增加 ready 文件握手，等 fsync 前缀落盘后才 kill，否则测的是"写到哪了"而不是"什么能存活"。

---

## 6. 对后续阶段的影响

### 对 S2（单机内核）

- 组提交窗口应作为**可配置参数**，默认 32~64，而不是硬编码
- 存储层需要区分 `os_buffer` / `fsync` 两种写入路径，但**接口保持一致**（`append/3` 的 SyncMode）
- 撕裂记录恢复逻辑已验证，可直接作为 `broker_storage` 恢复路径的基线

### 对 S4（集群与复制）

- `fsync` 单机 1.1k msg/s 意味着**不能把 fsync 放在每条消息的 Raft 提交路径上**，否则复制开销会再叠加一层 871 µs
- 必须沿用 README 6.2 的原则：本地批量追加 + 分组共识，不逐条走共识

### 待办

- [ ] 掉电模拟器（dm-flakey / VM 硬复位）测 `os_buffer` 真实丢失窗口
- [ ] 大消息体（1 MB / 10 MB）对 fsync 成本的影响
- [ ] 多写者并发场景下组提交的实现与验证（当前 bench 为单写者 + 批处理模拟）

---

## 7. 复现方式

```bash
export PATH=/home/wanli/Development/otp-28.3.1/bin:$PATH
cd spikes/s0_3_wal

rebar3 eunit                                    # 正确性测试
rebar3 compile

# 基础基准（需真实磁盘，tmpfs 会自动拒绝）
erl -pa _build/default/lib/s0_3_wal/ebin -noshell \
    -eval 's0_3_bench:main(["msgs","20000"]), halt(0).'

# 组提交窗口扫描
erl -pa _build/default/lib/s0_3_wal/ebin -noshell \
    -eval 's0_3_sweep:main(["msgs","2000"]), halt(0).'

# 崩溃恢复
erl -pa _build/default/lib/s0_3_wal/ebin -noshell \
    -eval 'R = s0_3_crash:run_all(3000),
           io:format("~s",[s0_3_crash:format(maps:get(fsync,R))]),
           halt(0).'
```

可用环境变量 `SPIKE_DATA_DIR` 指定数据目录（必须为真实块设备）。