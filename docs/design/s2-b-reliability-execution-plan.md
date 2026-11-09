# S2-B 执行计划：三档可靠性证明（`os_buffer` 丢失边界）

- **状态**：**替身档已完成（2026-11-07）**；T-B1/T-B2/T-B3/T-B4 已落地并实测（三场景 3/3 PASS），**T-B5 dm-flakey 实机报告待特权执行**；T-B6 文档已更新。证据见 `docs/benchmarks/reliability/s2-b-osbuffer-loss.md`。
- **对应**：`docs/design/s2-single-node-kernel.md` §4.2；`docs/adr/007-explicit-reliability.md`；`docs/roadmap.md` §4.3 Gate S2 第 1 项
- **估算**：4 人日
- **退出条件**：`os_buffer` 丢失边界有 CI 常驻的确定性证明（替身）；`fsync` 档有实机掉电对照（dm-flakey 报告）；`replicated_commit` 在 driver 层被显式拒绝；Gate S2「三档可靠性等级逐档有自动化证明」可勾选。

---

## 1. 目标与非目标

**目标**：把 ADR-007 的三档可靠性从「代码里能选」推进到「故障场景下逐档证明」。S1 已用 `kill -9` 证明 `fsync` 档（R1a），但 `kill -9` 丢不掉页缓存，**证明不了 `os_buffer` 的丢失窗口**。S2-B 补齐两件事：

1. **`os_buffer` 档丢失边界**：已 PUBACK（=fsync 前缀）的消息在掉电后可读；只入过 `os_buffer`（未 fsync）的尾部可丢失，且丢失后**不产生「已提交」假象**、恢复后 `high` 回到真实末尾。
2. **`replicated_commit` 显式拒绝**：S1/S2 无共识组，该档必须被 Adapter/Writer 早拒并返回 `{error,{unsupported_sync_mode,replicated_commit}}`，不得静默成功、不得消耗 offset。

**非目标（S3+ 再做）**：真正的 Raft 多数派提交（`replicated_commit` 到 S3 ADR-008 才有实体）；`kill -9` 语义的复测（R1a 已覆盖，本计划只补对照）；跨节点故障注入。

**证据双轨（ADR-007 三档 + 设计 §4.2）**：

| 档位 | 证明工具 | 运行条件 | 证据落点 |
|---|---|---|---|
| `fsync`（对照） | T14 R1a（已有）+ dm-flakey 实机 | CI / 特权 | 既有报告 + 本报告 |
| `os_buffer`（核心） | `tools/fault/os_buffer_loss.py`（替身） | **CI 常驻**（无需 root） | `docs/benchmarks/reliability/` |
| `os_buffer`（实机对照） | `tools/docker/s2_powerloss.py`（dm-flakey） | 特权，操作者跑 | `docs/benchmarks/reliability/` 报告 |
| `replicated_commit` | 既有 CT 套件 + driver 层断言 | CI | 既有 + 本计划补 |

---

## 2. 交付物

| 文件 | 动作 | 说明 |
|---|---|---|
| `tools/docker/mq_harness.py` | 新建 | 从 `s1_recovery.py` 抽出的共享 harness（`docker_args`/`render_config`/`erl_probe`/`probe_recover`/`Client`/`assert_true` 等），`s1_recovery.py` 与两个 S2-B 工具共用 |
| `tools/fault/os_buffer_loss.py` | 新建 | `os_buffer` 丢失替身（CI 常驻）；详见 §4。实测偏差：不需要 `__init__.py`，`sys.path` 引入即可 |
| `tools/docker/s2_powerloss.py` | 新建 | dm-flakey 实机掉电驱动（特权）；详见 §5 |
| `tools/docker/s1_recovery.py` | 修改 | 改为 import `mq_harness`；**S1 全量矩阵复跑为回归闸**（T-B1） |
| `docs/benchmarks/reliability/` | 新建目录 | 报告落点（`report-*.json` + `*.md` 说明） |
| `docs/design/s2-single-node-kernel.md` | 修改 | §4.2 标「已实现」并附报告链接 |
| `docs/roadmap.md` / `progress.md` | 修改 | Gate S2 第 1 项勾选；S2-B 完成行 |

> **路径决策**：设计 §4.2 已把替身钉在 `tools/fault/os_buffer_loss.py`、dm-flakey 钉在 `tools/docker/s2_powerloss.py`。二者都仍需 broker 容器与一次性 probe（`os_buffer` 路径 S1 只走存储 API，见 §3.3），故共享 harness 放 `tools/docker/mq_harness.py`，`tools/fault/os_buffer_loss.py` 用 `sys.path` 引入。

---

## 3. 机制与断言（冻结）

### 3.1 已冻结的语义事实（实现据此，不得改动）

- **档位↔sync_mode 映射**（`mqtt_conn_fsm:build_message/7`、`store_and_ack/6`）：QoS0 → `os_buffer`（**且 QoS0 不入 WAL**，只走未定位 push，R5 已证）；QoS1 → `fsync`（PUBACK 紧跟 `broker_queue:append(P,Msg,fsync)` 返回）。⇒ **S1 的 MQTT 边只会 fsync**，`os_buffer` 只能经存储 API 直连触达，故替身必须用 Erlang probe 注入。
- **offset / 水位约定**：`wal_recovery` `?FIRST_OFFSET=1`；空分区 `{committed=0, high=1}`；**记录数 = `high − 1`**；`committed ∈ [0, high−1]`（`wal_writer:advance_committed/2` 守卫 `Offset =< High−1`）。
- **能力集**：WAL `?CAPABILITY = [os_buffer, fsync]`；`replicated_commit` 在 `wal_writer:handle_call({append,…})` **分配 offset 之前**被拒（`broker_queue_SUITE:258`、`wal_writer_SUITE`、`broker_storage_wal_SUITE` 已钉）。
- **恢复语义**（`wal_recovery:accept_tail/6`）：**缺字节 = 可修复的撕裂尾**，按记录边界 cut 回并发 `{torn_tail_cut,…}` warning；**CRC/版本不符 = 损坏，拒绝启动**；`committed` 被夹到 `high−1`、`truncate_before` 夹到 `committed+1`（ADR-011 §11.3「水位可以回来低」）。
- **单写者**（ADR-011 §11.3）：一个分区同一时刻只有一个 `wal_writer`。⇒ **broker 运行时 probe 不能对同一分区 append**；注入必须在 broker 停机、probe 独占写该分区时完成。

### 3.2 `os_buffer` 丢失替身模型（§4 断言来源）

**屏障定义**：`fsync` 档 append 在 PUBACK 前完成一次 group-commit `fsync`——其返回的瞬间，段文件 `[0, B)` 已落盘，`B` 即**真实末尾屏障**。随后 `os_buffer` append 只入页缓存（返回 offset = 弱 ACK），`[B, EOF)` 属**已知未同步区间**。

**掉电模型**：把段文件截断到 `B`（按记录边界），等价于「`[B, EOF)` 的未同步尾部随页缓存丢失」。这是设计 §4.2「显式 fsync 屏障 + 在已知未同步区间内按记录边界截断」的直译，确定性、可复现、无需 root。

> 为何不是 `kill -9`：`docker kill` 保留页缓存，内核仍会把脏页回写，`os_buffer` 尾部不会丢——这正是 T14 README「M:kill -9 无法模拟 os_buffer 掉电」的记录。替身用**显式截断**制造真实丢失边界。

**断言**（落到 probe 返回值与 `probe_recover`）：

1. 恢复后 `high[分区] == K+1`（K 条 fsync 记录），**不是** `K+M+1`——「`high` 回到真实末尾」。
2. `read_from(Hd, 1, K)` 恰好返回 K 条、无撕裂、无 `corrupt_record`——「已 ACK（fsync 前缀）可读」。
3. offset `K+1..K+M`（`os_buffer` 尾部）读不到；`committed == K`（未越过 fsync 前缀）——「未同步尾部不可读，且**不产生『已提交』假象**」。
4. 恢复 warnings 不含 `{corrupt_record,_}` / `{unsupported_vsn,_}`；截断在记录边界 ⇒ 尾部为 `clean`（非 `torn`），无 `committed_above_high` 夹取告警。

### 3.3 dm-flakey 实机档（§5）

同一逻辑场景，但掉电由 **dm-flakey `drop_writes`** 在真实 ext4 loopback 上制造：写进掉电窗口的 `os_buffer` 尾部不落盘，`fsync` 前缀（窗口前已回写）保留。解除特权/密码限制后由操作者执行，产出 `docs/benchmarks/reliability/` 报告；其 `fsync` 前缀在真掉电后存活，即 `fsync` 档的实机对照。若主机内核 dm-flakey 不可靠，回退到 VM 拉电源（`vmops`-style），报告标注手段。

---

## 4. 任务拆解

> 共享 harness 抽取（T-B1）先行，S1 全量矩阵复跑通过前不写 S2-B 场景——S2 的纪律是**绝不回退 S1 Gate 证据**。

### T-B1 共享 harness 抽取（0.75 人日）

1. 把 `s1_recovery.py` 的 `docker_args`/`render_config`/`clear_data`/`ensure_release`/`wait_port`/`kill_broker`/`stop_broker`/`start_broker`/`fresh_start`/`broker_logs`/`erl_probe`/`probe_recover`/`probe_cursors`/`Client`/`offline_subscriber`/`reconnect`/`assert_true`/`payload_indices`/`b64url`/`subs_path` 及常量迁入 `tools/docker/mq_harness.py`；`s1_recovery.py` 改为 `from mq_harness import *`（保留其场景函数）。
2. `os_buffer_loss.py` / `s2_powerloss.py` 用 `sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "docker"))` 引 `mq_harness`。
3. **回归闸**：`python3 tools/docker/s1_recovery.py`（R1a–R7 × default/zero）全绿，报告 bit 级一致，方算抽取无损。

### T-B2 `os_buffer` 丢失替身 `tools/fault/os_buffer_loss.py`（1.5 人日）

1. **注入 probe**（一次 `erl -eval`，broker 未启动，probe 独占写分区 0）：
   - `application:load(broker_queue)` + `ensure_all_started(broker_storage)`；`create_partition(P) || P <- [0,1,2,3]`（幂等，对齐 4 分区 boot）。
   - `Hd = Backend:recover(0, Config)`；`{ok,_} = Backend:append(Hd, [fsync_msg(I) || I <- 1..K], fsync)`。
   - 读屏障 `B = file:read_file_info(SegPath0).size`（`SegPath0 = data_dir/wal/0/segment-00000000000000000001.log`）。
   - `Backend:advance_committed(Hd, K)`（fsync 前缀视为已持久确认）。
   - `{ok, OffsTail} = Backend:append(Hd, [osbuffer_msg(I) || I <- 1..M], os_buffer)`（弱 ACK，未 fsync）。
   - `Backend:close(Hd)`：`wal_writer:close/1` drain 待批处理、`sync_fd_measured/1` 落盘、置 `closed=true`——**此后写者不再写入**（段 fd 仍开但静默，probe VM 随即 `halt`，无后续写、无稀疏孔）。随后 `file:truncate(SegPath0, B)`（另开 fd）把段裁回屏障 B，丢弃尾部；close 的 fsync 即便刷过尾部也被这次截断覆盖，最终盘态 = 纯 K 条。
   - 打印 `BARRIER\t<B>`、`TAIL_OFFS\t<offs>`；halt。
2. **host 侧**：`start_broker`（entrypoint `create_partition` 幂等 → recover）；`probe_recover` 断言 §3.2 四点；`read_from` 校验 K 条可读。
3. **CLI**：`--fsync K`（默认 16）、`--osbuffer M`（默认 64）、`--keep-data`；报告写 `docs/benchmarks/reliability/report-osbuffer-tail-*.json`。

### T-B3 端到端对照 `osbuffer-mqtt`（0.75 人日）

1. broker 起；offline 订阅者（clean=false）订 `t/s2b/osbuffer/#`（哈希落分区 0）。
2. **真 QoS1 PUBLISH ×K** → 等 PUBACK（fsync 前缀，broker 为写者）→ `stop_broker`（优雅停机即 fsync）。
3. probe 独占分区 0：`recover(0)` 读当前段大小 = 屏障 `B0`；`append(…, os_buffer) ×M`；`close`；`file:truncate(SegPath0, B0)`。
4. `start_broker`；订阅者重连收**恰好 K 条前缀**、无尾部、无重复；`probe_recover` 断言 `high[0]==K+1`。
5. 意义：把 §3.2 的存储级断言抬到 **PUBACK 边**——「PUBACK 过 → 可读；os_buffer 尾部 → 丢失」。

### T-B4 `replicated_commit` driver 层断言（0.25 人日）

1. probe：`{error,{unsupported_sync_mode,replicated_commit}} = Backend:append(Hd, [msg], replicated_commit)`；`capability(Hd)` 不含 `replicated_commit`；再 `append(fsync)` 的 offset **不因被拒而前移**（拒绝不消耗 offset）。
2. 报告标注：driver 层确认 + 既有 CT 三套（`broker_queue_SUITE`/`wal_writer_SUITE`/`broker_storage_wal_SUITE`）为**持久证据**——该档无 I/O、无掉电维度，docker harness 不新增证明价值。

### T-B5 dm-flakey 实机 `tools/docker/s2_powerloss.py`（1 人日，特权/操作者）

1. `dd` 造空镜像 → `losetup -f --show` → `mkfs.ext4` → `dmsetup create … --table "0 <sectors> flaky <loop> 0 <up> <down>"`（或 message 切 `drop_writes`）→ mount 到 `data_dir`。
2. 复用 T-B2 的注入序列：`fsync` 前缀（回写）→ `os_buffer` 尾部（ACK，页缓存）→ 触发掉电窗口（drop）→ 让尾部写入丢失 → 卸载 → 去掉 flaky 层 → 重新 mount。
3. `start_broker` → recover → 同 §3.2 断言；`fsync` 前缀存活即 `fsync` 档实机对照。
4. 报告 `docs/benchmarks/reliability/s2-powerloss-<date>.md`（手段、表参数、K/M、恢复断言、dmesg）；`--skip-prereq` 时打印所需 root 步骤并退出 0（不污染 CI）。

### T-B6 文档（0.25 人日）

1. `docs/design/s2-single-node-kernel.md` §4.2 标「已实现」+ 报告链接；§4.2 的 dm-flakey 若本会话未跑，标「证据待特权执行」。
2. `docs/roadmap.md` Gate S2 第 1 项勾选；`progress.md` S2-B 行。

---

## 5. 场景 / 断言清单

| # | 工具·场景 | 注入 | 断言 | 特权 |
|---|---|---|---|---|
| S-B1a | `os_buffer_loss.py --scenario tail` | probe：K fsync + M os_buffer，截断到屏障 B | `high[0]=K+1`、`high[1..3]=1`；`read_from(1,K)=K` 条无撕裂；offset K+1..K+M 不存在；`committed=K`；无 corrupt/vsn 告警 | 否（CI） |
| S-B1b | `os_buffer_loss.py --scenario mqtt` | 真 QoS1×K（PUBACK）+ probe os_buffer×M 截断 | 订阅者恰收 K 条前缀、无尾部、无重复；`high[0]=K+1` | 否（CI） |
| S-B1c | `os_buffer_loss.py --scenario replicated` | probe append `replicated_commit` | 返回 `{error,{unsupported_sync_mode,replicated_commit}}`；能力集不含该档；拒绝不消耗 offset | 否（CI） |
| S-B2 | `s2_powerloss.py --scenario powerloss` | dm-flakey drop_writes 真掉电 | 同 S-B1a 的存储断言；`fsync` 前缀实机存活（对照） | **是**（操作者） |

**参数约束**：`K+M ≪ segment_max_records(100000)`，保证单段、屏障与尾部落同一 `segment-00000000000000000001.log`，避免旋转路径（`ensure_room` 的 sync-before-close）干扰屏障语义。

---

## 6. 验收命令

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"

# T-B1 回归闸：S1 全量必须 bit 级全绿（抽取无损）
python3 tools/docker/s1_recovery.py                       # R1a..R7 × default/zero

# S2-B 替身（CI 常驻，无需 root）
python3 tools/fault/os_buffer_loss.py --scenario tail
python3 tools/fault/os_buffer_loss.py --scenario mqtt
python3 tools/fault/os_buffer_loss.py --scenario replicated

# S2-B 实机掉电（特权；操作者执行）
sudo python3 tools/docker/s2_powerloss.py --scenario powerloss

# Erlang 侧回归（替身不改 broker_ 代码，基线不变）
rm -rf _build/test/extras
rebar3 ct                                                # 278/278，零 FAILED / 零 Skipped
rebar3 dialyzer
rebar3 xref                                              # 仍为预期 unused-export（38 条基线）
rebar3 as t14 release
```

**Gate 证据**：`docs/benchmarks/reliability/report-osbuffer-tail-*.json`（S-B1a/b/c 计数与断言）；`s2-powerloss-*.md`（S-B2 实机掉电 + `fsync` 对照）；既有 CT 三套截图/日志（`replicated_commit`）。

> **落地注记（2026-11-09）**：实际交付的 S-B1 证据文件名是 [`report-osbuffer-mqtt-replicated-tail-20261107-131941.json`](../benchmarks/reliability/report-osbuffer-mqtt-replicated-tail-20261107-131941.json)，结论与断言写在 [`s2-b-osbuffer-loss.md`](../benchmarks/reliability/s2-b-osbuffer-loss.md)。S-B2 的 dm-flakey **实机**报告至今未产生（需 root，本环境 `sudo` 需密码），这是 `docs/roadmap.md` Gate S2 唯一未勾选项；`s2_powerloss.py --skip-prereq` 可打印完整步骤并退出 0，因此它不阻塞 CI。

---

## 7. 风险

| 风险 | 缓解 |
|---|---|
| 抽取 `mq_harness.py` 引入 S1 回归 | T-B1 以 S1 全量矩阵复跑为硬闸；`--skip-build` 对比报告；不一致立即回退为「替身自包含」 |
| probe 与 broker 抢同一分区写（单写者 ADR-011） | 注入序列限定 broker 停机、probe 独占；`close` 后置 `closed=true` 使写者静默，截断后 probe VM 即 halt，无并发写 |
| `close/1` 的 fsync 把 os_buffer 尾部刷脏页，削弱「未同步」语义 | close 后立即 `file:truncate` 到屏障 B，尾部被丢弃，最终盘态 = 纯 K 条；模型等价于掉电丢失 |
| 屏障 B 记录边界读偏（读到尾部已扩展的 size） | B 在 os_buffer append **之前**采集，且单写者无并发；采集后文件只增不减直至截断 |
| dm-flakey 需 root，当前会话 `sudo` 需密码 | S-B2 标记特权、CI 不阻塞；`--skip-prereq` 打印步骤退出 0；替身（S-B1）独立承载 Gate |
| `read_from` 遇损坏改为「失败而非空页」被误判 | 断言区分 `{error,{corrupt_record,_,_}}` 与空页；S-B1a 显式断言无 corrupt |
| 分区数/段名假设漂移 | 复用 `mq_harness` 的 `render_config`（4 分区）与 `segment-%020d.log` 命名；参数变化走 `--fsync/--osbuffer` |
