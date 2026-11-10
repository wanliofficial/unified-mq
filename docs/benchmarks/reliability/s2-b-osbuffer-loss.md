# S2-B 可靠性证据：`os_buffer` 丢失边界

- **状态**：替身档（CI 常驻）与 **dm-flakey 实机档均已实测通过**（2026-11-09）。
- **对应**：`docs/design/s2-single-node-kernel.md` §4.2；`docs/adr/007-explicit-reliability.md`；`docs/roadmap.md` §4.3 Gate S2 第 1 项
- **执行计划**：`docs/design/s2-b-reliability-execution-plan.md`
- **工具**：`tools/fault/os_buffer_loss.py`（替身，无需 root）、`tools/docker/s2_powerloss.py`（dm-flakey，需 root 或特权容器）
- **原始报告**：
  - 替身：[`report-osbuffer-mqtt-replicated-tail-20261107-131941.json`](report-osbuffer-mqtt-replicated-tail-20261107-131941.json)
  - 实机：[`s2-powerloss-20261109-215930.json`](s2-powerloss-20261109-215930.json)

---

## 1. 证明了什么

ADR-007 的三档可靠性在 S1 只被 `kill -9` 覆盖过：`docker kill -s KILL` **保留页缓存**，内核随后会把脏页回写，所以 `kill -9` 丢不掉 `os_buffer` 的未同步尾部——它证明不了这一档的丢失边界。

本证据补上三件事：

| 档位 | 结论 | 证据形态 |
|---|---|---|
| `fsync` | 已 fsync 的前缀在「掉电」后完整可读、无撕裂、不重复 | 替身：K 条 fsync 记录在截断后原样读回；实机：dm-flakey 真掉电后同断言（§5.1）。S1 T14 R1a 已用 `kill -9` 覆盖 |
| `os_buffer` | 未 fsync 的尾部**可以丢**，且丢掉后**不产生「已提交」假象**、`high` 回到真实末尾 | 替身 S-B1a / S-B1b；实机 dm-flakey 真掉电（§5.1，64 条尾部全丢、`high` 回到 17） |
| `replicated_commit` | 在分配 offset 之前被**显式拒绝**，能力集不广告该档 | 替身 S-B1c + 既有 CT 三套 |

---

## 2. 替身模型（确定性、无需 root）

**屏障**：`fsync` 档 append 在返回前完成一次 group-commit `fsync`，返回瞬间段文件 `[0, B)` 已落盘，`B` 即真实末尾屏障。随后 `os_buffer` append 只把字节写进内核页缓存（返回 offset = 弱 ACK），`[B, EOF)` 是已知未同步区间。

**掉电**：把段文件按记录边界截断回 `B`，等价于「未同步尾部随掉电丢失」。这比 `kill -9` 更严格：`kill -9` 的尾部会被回写，截断才真正丢掉它。

**关键实现约束**：注入 probe **不调用 `wal_writer:close/1`**。`close/1` 会 `fsync`，把尾部刷盘，正好毁掉待测窗口；probe 打印证据后 `halt/0`，fd 关闭但不 fsync，尾部保持脏页。单写者（ADR-011 §11.3）保证 broker 停机后 probe 独占该分区，主机侧截断时无并发写者。

**为何 `os_buffer` 只能由 probe 触达**：S1 的 MQTT 边 QoS0 → `os_buffer` 但**不入 WAL**（只做未定位 push，R5 已证）；QoS1 → `fsync`。因此 MQTT 客户端写不出「入 WAL 的 os_buffer 记录」，必须走存储 API。这不是替身的取巧，而是 S1 语义事实。

---

## 3. 实测结果（2026-11-07，`--scenario all`，K=16 / M=64，tier=default）

```
PASS mqtt        (20.0s)  delivered=16  partition=2  barrier=2692  size_before=10240
                          wal={0:{0,1}, 1:{0,1}, 2:{committed:16, high:17}, 3:{0,1}}
PASS replicated  ( 3.3s)  refusal={error,{unsupported_sync_mode,replicated_commit}}
                          capability=[os_buffer,fsync]  next_offset=1
PASS tail        ( 7.3s)  k=16  m=64  barrier=1785  size_before=9329  read=16
                          wal={0:{committed:16, high:17}, 1:{0,1}, 2:{0,1}, 3:{0,1}}
SUMMARY passed=3 failed=0
```

（`wal` 值写作 `{committed, high}`；空分区 `{0,1}`，记录数 `= high − 1`。）

### 3.1 S-B1a `tail`：存储层丢失边界

- 写入前：空分区，4 个分区各一个空段。
- probe：16 条 `fsync`（offset 1–16，屏障 `B=1785`）→ `advance_committed(16)` → 64 条 `os_buffer`（offset 17–80，文件涨到 9329 字节，仅页缓存）。
- 掉电：`os.truncate(segment, 1785)`。
- 恢复断言（全部通过）：
  - `high[0] == 17`（= K+1），**不是** `81` —— `high` 回到真实末尾；
  - `committed[0] == 16` —— 水位没有越过 fsync 前缀，无「已提交」假象；
  - `read_from(1, …)` 恰 16 条，payload 精确等于 `fsync-1 … fsync-16`，无 `osbuf-*`、无撕裂、无 corrupt 告警；
  - 分区 1–3 仍为 `{committed:0, high:1}` —— 尾部没有污染其他分区。

### 3.2 S-B1b `mqtt`：抬到 PUBACK 边

- 离线订阅者（`clean=false`）订阅 `t/s2b/osbuffer/#`，cursor 已持久化。
- 单 topic `t/s2b/osbuffer/0` 上发 **16 条真 QoS1 PUBLISH**，逐条等 PUBACK（PUBACK 紧跟 `broker_queue:append(P, Msg, fsync)` 返回，即 fsync 前缀）。
- `kill -9` 后 probe 独占该分区：读当前段大小作屏障（2692），追加 64 条 `os_buffer`，不 close，退出；主机截断回屏障。
- 重启后订阅者重连（`session_present=1`）**恰收 16 条** `pub-0000 … pub-0015`，无尾部、无重复、无 corrupt；停机后再验 `high == 17`、`committed == 16`、读回 16 条且 payload 与发布逐一相等。

意义：同一结论在 **PUBACK 边界**成立——PUBACK 过的消息可读，只入 `os_buffer` 的尾部丢失，且「已 ACK」的承诺没有被尾部丢失污染。

### 3.3 S-B1c `replicated`：显式拒绝

- `Backend:append(Hd, [Msg], replicated_commit)` → `{error,{unsupported_sync_mode,replicated_commit}}`；
- `capability/1` → `[os_buffer, fsync]`，不广告 `replicated_commit`；
- 紧接着的 `fsync` append 拿到 offset **1** —— 拒绝**不消耗 offset**（检查发生在计数器前移之前）。

该档无 I/O、无掉电维度，持久证据还包括既有 CT：`broker_queue_SUITE`（`capability_reports_what_the_backend_actually_does`、`an_unsupported_sync_mode_is_refused_without_consuming_an_offset`）、`wal_writer_SUITE`、`broker_storage_wal_SUITE`。

---

## 4. 断言 ↔ 设计要求

| 设计要求（§4.2 / ADR-007） | 断言 | 场景 |
|---|---|---|
| 丢弃未 sync 尾部后，已 ACK（PUBACK 过）的消息可读 | `read_from` 返回恰 K 条、payload 逐条相等 | tail / mqtt |
| 未 ACK 的尾部不可读 | offset K+1..K+M 不在恢复结果里、无 `osbuf-*` | tail / mqtt |
| 不产生「已提交」假象 | `committed == K`（未越过 fsync 前缀） | tail |
| 恢复后 `high` 回到真实末尾 | `high == K+1` | tail / mqtt |
| 缺口不是「损坏」，是「缺失」 | 无 `{corrupt_record,_}` / `{unsupported_vsn,_}`；截断在记录边界 | tail |
| `replicated_commit` 早拒且不耗 offset | refusal + `[os_buffer,fsync]` + next offset 1 | replicated |
| 空分区不被污染 | 分区 1–3 恒 `{0,1}` | tail |

---

## 5. dm-flakey 实机档（已实测通过）

`tools/docker/s2_powerloss.py` 在同一逻辑场景上把「掉电」交给真实设备丢弃：

1. `truncate` 造 ext4 镜像（默认 256 MiB，放仓库内 ext4，**不放 `/tmp`**——本机 `/tmp` 是 tmpfs）→ `losetup` → `dmsetup create … flakey <loop> 0 3600 3600` → `mkfs.ext4` → `mount` 为 broker 数据目录；
2. 注入与替身同一 probe（K 条 fsync + M 条 os_buffer，不 close）；
3. 设备进入 `drop_writes` 状态后 `sync`：脏尾部被报告「写成功」后静默丢弃——设备撒谎，即掉电；
4. 脏挂载状态下 `umount`，把 flakey 层换成 `linear`，重新 `mount`（必要时 ext4 journal replay）；
5. 断言与替身一致（`high == K+1`、`committed == K`、K 条 fsync 可读、无 `osbuf-*`），并额外**真启动一次 broker**（挂载点、journal、release 全参与）。

同时满足 `fsync` 档的实机对照：真掉电后 fsync 前缀存活。

### 5.1 实测结果（2026-11-09，`--scenario powerloss`，K=16 / M=64，tier=default）

```json
{
  "barrier": 1783, "k": 16, "m": 64, "read": 16, "size_before": 9332,
  "broker_booted": true,
  "wal": {"0": {"committed": 16, "high": 17},
          "1": {"committed": 0, "high": 1},
          "2": {"committed": 0, "high": 1},
          "3": {"committed": 0, "high": 1}}
}
```

- `high == 17 == K+1`：尾部 64 条 `osbuf-*` **全部丢失**，恢复后的水位回到真实末尾。
- `committed == 16 == K`：没有越过 fsync 前缀，未产生「已提交」假象。
- `read == 16`：K 条 fsync 记录逐条可读、payload 精确，无撕裂、无 `osbuf-*`。
- `broker_booted == true`：真 broker 在恢复后的文件系统上启动成功（挂载点、ext4 journal、release 全参与）。
- 分区 1–3 恒 `{0,1}`：尾部没有污染其他分区。

### 5.2 让实机档真正测到掉电的三个技术要点

这三处都是实测踩出来的，不是理论推演；改掉任何一处，测试都会「通过」但什么都没证明。

**① `dmsetup suspend` 会强制刷盘，把脏尾部写给还健康的设备。**
`dmsetup suspend` 冻结设备的 I/O 队列，冻结会触发文件系统的强制 sync，于是未 fsync 的尾部在切盘**之前**就被写进了设备——尾部随后自然存活。实测：镜像上的 `osbuf-` 计数在 `suspend` 那一步从 0 跳到 64。`reload` 与 `resume`（热切换表）同样如此：任何表变更都伴随队列冻结。

**因此 `drop_writes` 表不能用手工切换装上去**，只能用 dm-flakey 自带的**定时 up/down 窗口**：表建好后设备先健康 `UP_SECONDS` 秒，之后自己转入 `drop_writes` 状态，整个过程没有任何 ioctl，因此没有强制 sync，尾部一直留在 page cache 里，直到掉电那一刻才被设备丢弃。

**② ext4 默认的 ordered 模式会把尾部提前刷出去。**
WAL 段文件每次追加都改 `i_size`，ordered 模式下 ext4 在下一次 journal commit 时会把该文件的脏数据页一起刷出；默认 commit 间隔 5 秒，远早于切盘。改用 `mount -o data=writeback,commit=600`：尾部留在 page cache，直到掉电。`fsync` 档不受影响——`file:sync(2)` 无论挂载模式都会把文件的数据与元数据刷出去，所以 K 条 fsync 记录仍然在设备上。

**③ 注入必须落在 up 窗口内。**
K 条 fsync 记录必须写给一个还健康的设备，否则连前缀的结论都不成立。驱动在注入后断言「注入耗时 < up 窗口」，否则明确报错要求调大 `S2B_DM_UP_SECONDS`。

### 5.3 复跑

实机档需要 root。有 sudo 密码时直接跑：

```bash
sudo python3 tools/docker/s2_powerloss.py --scenario powerloss
```

没有 sudo 密码但有 Docker 时，用特权容器加入宿主 mount namespace（`nsenter -t 1 -m`），这条路径已实测跑通并产出上面的报告：

```bash
sh tools/docker/s2_powerloss_priv.sh --scenario powerloss
```

- `tools/docker/s2_powerloss_priv.sh`：在宿主上跑驱动，只把特权命令（losetup/dmsetup/mkfs.ext4/mount/sync）经 `tools/docker/host_priv.sh` 送进宿主 mount namespace；容器还需 `--pid=host` 才能让 `nsenter -t 1` 命中宿主 init。
- `tools/docker/host_priv.sh`：执行**一条**特权命令的最小封装（`docker run --privileged --pid=host … nsenter -t 1 -m -- <cmd>`）。mount 必须落在宿主命名空间，否则 Docker 守护进程解析 bind-mount 源路径时看不到它，broker 会写错目录——测试就会「因为错误的原因通过」。
- `--noudevsync`：特权容器里没有 udev 守护进程，`dmsetup` 默认会等一个永不到来的 udev 事件；设备节点由内核创建，跳过 udev 通知即可。
- up 窗口默认 20 秒，实测需要覆盖「容器启动 + Erlang boot + 探针」，因此复跑时用 `S2B_DM_UP_SECONDS=120`。

### 5.4 Gate 口径

CI 常驻的替身（本文件 §3）承载「有自动化测试证明」；实机档（§5.1）提供真实设备上的对照。两档都已通过，Gate S2 第 1 项满足。

---

## 6. 共享 harness 抽取的回归闸

S2-B 的两个驱动复用 T14 的容器/probe/客户端 harness（`tools/docker/mq_harness.py`，从 `s1_recovery.py` 1–513 行逐字抽出；`s1_recovery.py` 改为 `from mq_harness import *` 只保留场景与 CLI）。

- **场景代码逐字一致**：`git show HEAD:tools/docker/s1_recovery.py | sed -n '515,889p'` 与抽取后的场景段 `diff` 只差节头注释与 `__main__` guard。
- **S1 全量复跑**：`--keep-going` 16 跑 **15 PASS / 1 FAIL**，唯一失败是 `r3 tier=default`（`acked offsets … lost: [32..41]`）。
- **该失败是既有 flake，非本次回归**：用**未改动的原始** `s1_recovery.py` 单跑 `r3 --tier default`，同一错误、同一 offset 区间；随后单独重跑 r3 双档均 PASS（default replayed 13/13，zero 0 dup）。r3 的时序敏感性不在 S2-B 范围，不在此修，仅记录。

---

## 7. 边界与非目标

- 不证明真正的 Raft 多数派提交（`replicated_commit` 到 S3/S4 才有实体）；本阶段只证明**显式拒绝**。
- 不重测 `kill -9` 语义（T14 R1a/R3 已覆盖）；替身只补「未 fsync 尾部」这一维度。
- 替身的「掉电」是**按记录边界截断**，不模拟撕裂写（撕裂尾由 T14 R4 覆盖，恢复会 cut 回记录边界）。二者正交。
- `K+M ≪ segment_max_records(100000)`，保证屏障与尾部同段落，避免旋转路径的 sync-before-close 干扰屏障语义；这也是 §2「不 close」成立的前提。

---

## 8. 复跑

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"

# 替身（CI 常驻，无需 root；会重写 docs/benchmarks/reliability/report-*.json）
/usr/bin/python3 tools/fault/os_buffer_loss.py --scenario all                # K=16, M=64
/usr/bin/python3 tools/fault/os_buffer_loss.py --scenario tail --fsync 32 --osbuffer 128

# dm-flakey 实机（需 root 或特权容器；--skip-prereq 可先看步骤）
python3 tools/docker/s2_powerloss.py --skip-prereq
sudo python3 tools/docker/s2_powerloss.py --scenario powerloss

# 无 sudo 密码但有 Docker：特权容器加入宿主 mount namespace
S2B_DM_UP_SECONDS=120 sh tools/docker/s2_powerloss_priv.sh --scenario powerloss

# Erlang 侧不受影响（S2-B 未改 broker_ 代码）
rm -rf _build/test/extras && rebar3 ct && rebar3 dialyzer && rebar3 xref
```

`/usr/bin/python3` 是必需的：`paho-mqtt 2.1.0` 装在系统 `python3.12` 的用户站点，miniconda 的 `python3` 看不到它。
