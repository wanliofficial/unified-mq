# 运维手册（S6）

- **状态**：S6 交付事实文档 v1.0（2026-11-09）
- **对应**：`docs/roadmap.md` §8.2；Gate S6 第 4 项（运维手册与告警规则）
- **实现**：`apps/broker_admin/`（`broker_admin_http`、`broker_admin_listener`、`broker_backup`、`broker_config_audit`、`broker_version`）
- **告警规则**：[`config/alerts.yml`](../config/alerts.yml)
- **相关**：`docs/storage-and-recovery.md`（WAL/会话恢复）、`docs/security.md`（TLS/ACL）

本手册只描述已实现的管理面。命令示例以默认配置（admin `127.0.0.1:8090`，空 token）为准；生产环境必须设置 token 并限制 bind。

---

## 1. 组件与端口

| 组件 | 默认端口 | 协议 | 说明 |
|---|---|---|---|
| MQTT | 1883 | 明文（可 TLS） | `broker_proto_mqtt` |
| WebSocket | 8080 | 明文（可 TLS） | `broker_proto_ws`（Broker 自有 JSON/binary 消息 API） |
| MQTT over WebSocket | 8083 | 明文（可 TLS，wss） | `broker_proto_mqtt_ws`（S8-B；MQTT 3.1.1 §6 / 5.0 §6） |
| NATS | 4223 | 明文（可 TLS） | `broker_proto_nats` |
| AMQP | 5673 | 明文（可 TLS） | `broker_proto_amqp` |
| Kafka | 9093 | 明文（可 TLS） | `broker_proto_kafka` |
| Pulsar | 6650 | 明文（可 TLS） | `broker_proto_pulsar` |
| 管理 API | 8090 | HTTP | `broker_admin`，默认只绑 `127.0.0.1` |

TLS 由 `broker_security.tls.enabled` 一个开关统一控制，所有协议 listener（目前七个）同时切换。见 `docs/security.md` §9。

`broker_ws`（S8-A 起）是**纯库 app**：无进程、无监听端口、无运行时配置，只提供 `ws_frame`（RFC 6455 帧编解码）与 `ws_handshake`（握手），供 `broker_proto_ws` 与 `broker_proto_mqtt_ws` 共用。运维上无需任何配置项，也不出现在上表。

**两个 WebSocket 端口不是一件事**。`8080` 是 Broker 自己的 JSON/binary 消息 API（S5），`8083` 是标准 MQTT over WebSocket（S8-B）：同一个 `mqtt_conn_fsm` 跑在 RFC 6455 binary 帧里，客户端是普通 MQTT 库（如 paho-mqtt 的 `transport="websockets"`）。端口内容互不兼容，**不要**把 MQTT 客户端指向 8080。

`broker_proto_mqtt_ws` 的配置（`config/sys.config.src`）：

```erlang
{broker_proto_mqtt_ws, [
  {listeners, [#{port => 8083,
                 ref => {mqtt_ws, 8083},
                 num_acceptors => 10,
                 max_connections => 1024}]},
  {subprotocols, [<<"mqtt">>, <<"mqttv3.1">>, <<"mqttv3.1.1">>]},
  {max_frame_bytes, 16777216}
]}.
```

- **任一 URL path 都接受**（`/`、`/mqtt`、`/ws/...`）：MQTT over WebSocket 规定的是子协议，不是路径。客户端库里的 path 只需与服务端部署约定一致即可。
- **子协议是硬门槛**：握手没带 `Sec-WebSocket-Protocol`，或带的不是 `subprotocols` 里的任一项，服务端回 **HTTP 400** 而不是升级。这是有意的——浏览器随手连的 WebSocket 不该被当成 MQTT 连接。
- `max_frame_bytes` 限制**单个 WebSocket 帧**；MQTT 报文级上限仍是 `broker_proto_mqtt.mqtt_max_packet_size`。帧超大 → close `1002`；text 帧 → close `1003`。
- TLS 与其他 listener 一样由 `broker_security.tls.enabled` 一个开关控制（wss）；没有单独的 WS TLS 配置。

---

## 2. 管理 API

Base URL：`http://127.0.0.1:8090`。设置 token 后，除健康检查外都需要：

```
Authorization: Bearer <token>
```

（也接受 `X-Admin-Token: <token>`。）

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/healthz` | 存活：进程在即 200，永远不鉴权 |
| GET | `/readyz` | 就绪：必需 app 未全起时 503，列缺失 app |
| GET | `/metrics` | Prometheus 文本（封闭 key 集合） |
| GET | `/api/v1/status` | 节点、版本、分区、retention、集群、指标快照 |
| GET | `/api/v1/config` | 脱敏配置快照 + SHA-256 |
| GET | `/api/v1/version` | release/OTP/数据格式/配置版本 |
| GET | `/api/v1/backups` | 备份清单（名称、字节、mtime） |
| POST | `/api/v1/backup` | 创建一次热备份 |
| POST | `/api/v1/tls/reload` | 清 `ssl` PEM 缓存，轮换证书 |

`/readyz` 检查的 app：`broker_storage`、`broker_router`、`broker_queue`、`broker_subscription`、`broker_event`、`broker_security`。`broker_metrics` 与 `broker_admin` 故意不在其中：指标/管理面故障不应把 broker 摘出负载均衡。

示例：

```bash
curl -fsS http://127.0.0.1:8090/healthz
curl -fsS http://127.0.0.1:8090/readyz
curl -fsS -H "Authorization: Bearer $ADMIN_TOKEN" \
     http://127.0.0.1:8090/api/v1/status | jq .
curl -fsS -H "Authorization: Bearer $ADMIN_TOKEN" \
     -X POST http://127.0.0.1:8090/api/v1/backup
```

---

## 3. 日常巡检

### 3.1 健康与就绪

- `/healthz` 200 且 `/readyz` 200：正常。
- `/readyz` 503：响应体会列出缺失 app，先查该 app 的启动日志（`data/log/s1.log`）。
- 进程在但 `/readyz` 一直 503：检查数据目录是否可写、分区是否已带外创建（见 §5）。

### 3.2 关键指标

`/metrics` 的 key 是封闭集合（ADR-016）。巡检优先级：

| 指标 | 含义 | 关注点 |
|---|---|---|
| `connections_current` / `connections_peak` | 当前/峰值连接 | 突增或触顶 `max_connections` |
| `auth_failures` / `authz_failures` | 认证失败 / 授权失败 | 前者账号问题，后者 ACL/租户问题 |
| `publish_total` / `deliver_total` / `ack_total` | 发布/投递/确认计数 | 三者比例失衡说明消费滞后 |
| `publish_latency_us` / `deliver_latency_us` / `ack_latency_us` | 直方图 | p99 抬升先看 WAL |
| `queue_depth` / `subscription_backlog` / `cursor_lag` | 积压 | 持续增长 → 慢消费者或容量不足 |
| `redelivery_total` / `dlq_size` | 重投递 / 死信 | DLQ 增长说明消费端反复失败 |
| `wal_sync_latency_us` / `wal_pending_append` | 刷盘延迟/待刷队列 | 磁盘或组提交异常 |
| `segment_bytes` / `recovery_time_us` | 段大小 / 恢复耗时 | 恢复时间随日志增长 |
| `retention_deleted_bytes` / `retention_blocked_by_slow_consumer` | 段回收 | 被慢消费者钉住说明有订阅不推进 |
| `cluster_owner_changes` / `cluster_fencing_rejected` / `cluster_no_quorum` | 集群 | 频繁切换或失去多数派 |
| `cluster_replication_failed` / `cluster_replication_latency_us` | 复制失败计数 / 复制往返延迟 | 失败上升或 p99 逼近 `replicate_timeout_ms` 说明副本跟不上 |
| `backpressure_pauses` / `backpressure_disconnects` | 背压 | 与 `conn_mailbox_len` 一起看 |

### 3.3 版本与配置审计

```bash
curl -fsS -H "Authorization: Bearer $ADMIN_TOKEN" \
     http://127.0.0.1:8090/api/v1/version | jq .
curl -fsS -H "Authorization: Bearer $ADMIN_TOKEN" \
     http://127.0.0.1:8090/api/v1/config | jq -r .hash
```

`data/config-audit.log` 每次 boot 追加一行（脱敏快照 + hash）。升级前后对比 hash，可确认节点实际加载的配置没有漂移。

---

## 4. 备份与恢复

### 4.1 备份

备份是 `<data_dir>` **整棵树**（递归）的 gzip tar，热备份（读取时 broker 仍在运行）。当前包含：

- `wal/`：各分区的追加日志、索引与 meta；
- `sessions/`：`.subs` / `.cursors` / `.inflight`；
- `retain/retain.v1`：保留消息快照；
- `mnesia/`、`ra/`：S4 控制面（Mnesia 表 + Metadata Raft）；
- `config-audit.log`：脱敏配置审计。

热备份对 WAL 是安全的：正在追加的段按原样复制，恢复时走 `wal_recovery` 的撕裂尾规则；会话与 retain 文件用 temp+rename，读者只会看到完整旧文件或完整新文件。**`mnesia/` 与 `ra/` 是活的控制面状态，没有撕裂尾规则**；要求控制面逐字节一致时，先静止写入或停节点再备份。需要逐字节一致的业务数据同样应先静止写入。

```bash
# 通过 API
curl -fsS -H "Authorization: Bearer $ADMIN_TOKEN" \
     -X POST http://127.0.0.1:8090/api/v1/backup | jq .

# 直接调用（运维 shell / 远程 console）
erl -pa _build/default/lib/*/ebin -noshell -eval \
  'io:format("~p~n", [broker_backup:create()]), halt().'
```

- 输出目录由 `broker_admin.backup_dir` 决定，必须是 release 树外的绝对路径（容器 harness 渲染配置时会把它映射到数据卷的 `backups/`）。
- 备份期间正在追加的 WAL 段按原样复制；恢复时按 `wal_recovery` 的撕裂尾规则处理，因此**业务数据的热备份是安全的**。`mnesia/` 与 `ra/` 为控制面，热备份不保证逐字节一致；需要一致时先静止写入或停节点。
- `GET /api/v1/backups` 返回清单，用于核对备份是否落在预期位置。

### 4.2 恢复

恢复是显式的：`broker_backup:restore(Archive, TargetDir)` 只解压到调用者指定的**空目录或不存在目录**，从不覆盖已有数据。

```bash
erl -pa _build/default/lib/*/ebin -noshell -eval \
  'io:format("~p~n", [broker_backup:restore("backups/unified-mq-20261109-120000.tar.gz", "/srv/unified-mq-restore")]), halt().'
```

步骤：

1. 停目标节点（滚动升级窗口内一次只动一个）。
2. 解压到 release 树外的新目录（上例 `/srv/unified-mq-restore`）。
3. 把**五个**数据目录配置指向恢复树：`broker_storage.data_dir` / `broker_subscription.data_dir` / `broker_retain.data_dir` 指向恢复根；`broker_meta.data_dir` 指向 `<恢复根>/mnesia`，`broker_cluster.data_dir` 指向 `<恢复根>/ra`。
4. 启动节点，确认 `/readyz` 200 且 `/api/v1/status` 的分区与预期一致。
5. 校验：发布一条 QoS1 并确认订阅端收到，再检查 `recovery_time_us`。

**禁止**直接解压覆盖在线数据目录——`restore` 本身也会拒绝非空目标。

---

## 5. 分区与数据目录

- 分区创建是**带外**动作：干净的 `data_dir` 启动会失败（ADR-011 §11.2），必须先 `broker_queue:create_partition/1`。
- 数据目录必须在 release 树外、ext4（tmpfs/ramfs 被存储层拒绝）。
- 目录布局与恢复算法见 `docs/storage-and-recovery.md`。

**控制面持久性（必须显式配置）**：`broker_meta` 的 11 张控制面表（用户、ACL、schema 版本、outbox）默认建为 **`ram_copies`**——`sys.config.src` 没有覆盖 `table_storage`，代码默认即 `ram_copies`，此时节点重启**全部丢失**（`maybe_set_dir` 也只在 `disc_copies` 时设置 `mnesia.dir`）。ADR-021 §D1 要求生产配置：

```erlang
{broker_meta, [{data_dir, "<abs>/data/mnesia"}, {table_storage, disc_copies}]}
```

未设置时请按「重启后用户/ACL 需重新建立」理解现状。

---

## 6. 滚动升级与版本兼容

`/api/v1/version` 返回：

- `release`：`broker_core` 的 vsn（当前 `0.1.0`）。
- `otp`：ERTS 版本（本项目固定 OTP 28.3.1 / ERTS 16.2）。
- `apps`：所有 `broker_*` app 的 vsn。
- `data_version`：新写入的 WAL 记录格式版本。
- `compatible_data_versions`：本节点能读取的版本窗口（当前 `[1]`）。
- `config_version`：`broker_meta` 的控制面版本（Mnesia/ra 可用时）。

升级流程：

1. 记录每台旧节点的 `data_version` 与 `/api/v1/config` 的 hash。
2. 确认新版本的 `compatible_data_versions` 覆盖旧 `data_version`；不覆盖则**不能**直接滚动升级，需先做数据迁移。
3. 一次升级一个节点，等 `/readyz` 200 且 `cluster_no_quorum = 0` 再动下一个。
4. 升级后对比 hash 与 `data_version`。

**本版本边界**：WAL 记录格式只有 v1，没有跨版本迁移工具；真正的「WAL v1→v2 迁移」是 S6 之后的待办。这里交付的是可查询的兼容窗口与升级顺序，不是迁移器。（会话文件的 `.subs`/`.cursors` v1→v2 是另一回事：boot 时由 `broker_subscription_files:migrate_v1/0` 自动完成，见 ADR-012 §12.5 / ADR-019。）

---

## 7. 故障演练（GameDay）

驱动：`tools/bench/s6_gameday.py`（`crash` / `slow_consumer` / `disk_readonly` / `partition`）。报告见 `docs/benchmarks/2026-11-09-s6-gameday.md`。

| 场景 | 注入 | 期望 | 观测 |
|---|---|---|---|
| crash | `docker kill -s KILL` | 已 ACK 的 QoS1 重启后必达；订阅集合不丢 | 到首个 CONNACK 的时间 = 恢复时间 |
| slow_consumer | 订阅端永不 ack，2 万条 QoS1 | 内存有界、broker 仍接新发布 | cgroup `memory.current` 增长 |
| disk_readonly | 数据卷 `:ro`（真 EROFS） | 拒绝启动或对不可持久写不 ACK；恢复可写后重新服务且旧数据在 | 恢复时间、旧消息是否仍在 |
| partition | 3 节点 Raft 杀 Leader / 失去多数派 | 多数派继续提交；少数派 `{error, no_quorum}` | CT 套件结果 |

演练前先备份；演练后核对 `/api/v1/status` 与 `data/config-audit.log`。

---

## 8. 告警规则

[`config/alerts.yml`](../config/alerts.yml) 是 Prometheus 规则（告警 + 记录），直接读取 `/metrics`。规则只使用封闭 key 集合里的指标，不含用户可控标签。部署时把它挂到 Prometheus 的 `rule_files`。

规则覆盖：就绪失败、认证/授权失败激增、积压与 cursor 滞后、DLQ 增长、WAL 刷盘延迟、恢复时间、慢消费者钉住回收、背压、失去多数派、磁盘（由 node_exporter 的 `node_filesystem_avail_bytes` 提供，可选）。

---

## 9. 容量规划与压测

- 容量基准：`docs/benchmarks/2026-11-09-s6-capacity.md`（四维矩阵 + 恢复时间）。
- 复跑：`python3 tools/bench/s2_bench.py --skip-build`。
- 快速结论（本机 16 逻辑核 / ext4 / OTP 28）：1 KiB 8 连接约 900 msg/s（fsync 档），吞吐随消息大小下降，1 MiB 约 11 msg/s；`kill -9` 恢复到首个 CONNACK 约 3.4–3.6 s。
- 容量估算以「持久化 msg/s × 消息大小 ÷ 副本因子 × 安全系数」为起点，必须用目标硬件复测，不要直接外推本机数字。

---

## 10. 故障排查速查

| 现象 | 先看 | 常见原因 |
|---|---|---|
| 启动即退出，日志 `partition_not_found` | data_dir 是否为空 | 未带外建分区 |
| 启动失败 `tmpfs`/`ramfs` | `mount` | 数据目录在内存文件系统 |
| `/readyz` 503 | 缺失 app | 某 app 启动失败，查 `data/log/s1.log` |
| 发布无 PUBACK | `wal_sync_latency_us`、磁盘 | 写盘失败/只读/满 |
| 积压不降 | `cursor_lag`、`retention_blocked_by_slow_consumer` | 慢消费者不 ack |
| `authz_failures` 激增 | `config` 的 `acl` | ACL 规则过严或租户不符 |
| 集群写失败 `no_quorum` | `cluster_*` | 存活节点不足多数派 |
| 管理 API 401 | `broker_admin.token` | 未带 Bearer 或 token 不符 |
