# S6 — 生产化执行计划与交付记录

- **依据**：README §10.1 / §10.2 / §11.3 / §11.4；`docs/roadmap.md` §8
- **状态**：✅ 完成（2026-11-09）
- **不动的东西**：`#broker_message{}`、各协议线上字节、默认单节点行为（未配置 ACL 规则时保持 S1 放行姿态、未注册复制钩子时 no-op）

## 1. 交付物

| 交付物 | 模块/应用 | 验收 |
|---|---|---|
| 租户隔离 | `broker_security` + `broker_router_hash_store` + `broker_subscription` | 索引键 `{Tenant, Filter}`；跨租户读写拒绝，先于 ACL |
| 认证 | `broker_security:authenticate/2` | MQTT CONNECT；失败恒 `bad_username_or_password` |
| ACL | `broker_security:authorize/2` | 有序 first-match，无匹配 deny；通配与前缀 |
| 资源上限 | `broker_limits` + `broker_queue:append/3` + `broker_event_codec:validate/1` | 消息体/属性数/属性长度/扩展/JSON 深度 |
| 指标防高基数 | `broker_metrics` `key_specs/0` + `key_types/0` | key 为封闭集合，无用户可控标签 |
| TLS 与轮换 | `broker_security_tls` + 六协议 `*_sup` | `transport/0` 单开关覆盖所有 listener；`reload/0` |
| 管理 API | `broker_admin_http` + `broker_admin_listener` | `/healthz` `/readyz` `/metrics` `/api/v1/*` |
| 配置审计 | `broker_config_audit` | 脱敏快照 + SHA-256，落 `config-audit.log` |
| 备份恢复 | `broker_backup` | gzip tar 整个 `<data_dir>` 树（wal/sessions/retain/mnesia/ra）；列表/恢复 |
| 版本兼容窗口 | `broker_version` | `data_version` / `compatible/1` |
| 告警规则 | `config/alerts.yml` | Prometheus 告警 + 记录规则，仅用封闭指标 |
| 文档 | `docs/security.md`、`docs/operations.md`、三份基准报告 | 只写已实现事实 |

## 2. 实现步骤

1. `broker_security`：新增 `broker_limits`、`broker_security_tls`；`authorize/2` 租户先于 ACL；`redacted_config/0`；`authorized/2`。
2. `broker_queue:append/3`：在写前调用 `broker_limits:check_message/1`，超限 `{error, {limit_exceeded, ...}}`。
3. `broker_event_codec:validate/1`：事件级上限（属性数/扩展长度/JSON 深度），超限返回 `{invalid_event, {limit_exceeded, ...}}`。
4. `broker_router_hash_store` / `broker_router_hash` / `broker_subscription*`：索引与路由按 `{Tenant, Filter}` 隔离；`<<"*">>` 保留 legacy 通配。
5. `mqtt_conn_fsm`：CONNECT 认证 → 取租户 → 发布/订阅经 `broker_security:authorize/2`；失败计 `authz_failures`。
6. 六协议 `*_sup`：listener 改用 `broker_security_tls:transport/0`，启用且缺 PEM 时启动失败。
7. `broker_admin`：`gen_tcp` + `{packet, http_bin}` HTTP/1.1；默认 `127.0.0.1:8090`、空 token；常量时间 token 比较；备份/审计/版本模块。
8. 配置：`config/sys.config.src` 增 `broker_security`（users/acl/limits/tls）与 `broker_admin`；`rebar.config` 加入新 app 与 `public_key` PLT。
9. 测试：`broker_limits_SUITE`、`broker_security_acl_SUITE`、`broker_security_tls_SUITE`、`broker_admin_SUITE`、`broker_security_audit_SUITE`；回归全量 CT。
10. 基准与演练：`tools/bench/s6_bench_extra.erl`、`tools/bench/s6_cluster_bench.erl`、`tools/bench/s6_gameday.py`；产出三份报告。

## 3. 测试与验收

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
rm -rf _build/test/extras
rebar3 ct --suite=apps/broker_security/test/broker_limits_SUITE,apps/broker_security/test/broker_security_acl_SUITE,apps/broker_security/test/broker_security_tls_SUITE,apps/broker_security/test/broker_security_audit_SUITE,apps/broker_admin/test/broker_admin_SUITE
rebar3 ct            # 全量
rebar3 dialyzer
rebar3 xref
```

## 4. 实现期发现与修正

| # | 现象 | 处理 |
|---|---|---|
| S6-1 | 3 节点复制基准第一次跑通时，`replicated_commit` 全部判失败 | `broker_cluster_rep:remote_append/2` 原返回 `{ok, Offset}`，`collect/2` 只认裸 `ok`。归一化为 `ok \| {error,_}`，补回归用例 `remote_append_normalises_a_successful_append`（S4 单副本/无多数派用例从未覆盖成功分支） |
| S6-2 | 磁盘 GameDay 用 `chmod` 模拟只读无效 | 已打开的 WAL fd 忽略权限位；改用真实只读 bind mount（EROFS） |
| S6-3 | 3 节点基准首次启动 `ra.app` 不在 peer 代码路径 | 把 `_build/test/checkouts/*/ebin`（vendored ra）加入 peer `-pa` |
| S6-4 | 基准里 `application:set_env` 被 `.app` 默认值覆盖 | 先 `application:load/1` 再 `set_env/3` |

## 5. 明确未实现（S6 之后；S7 承接）

本节是 S6 交付时点的清单，保留原始口径；后续进展见 `docs/roadmap.md` §9 与 `docs/security.md` §2.1 / §12。

1. **除 MQTT 外五种协议统一走 `broker_security` 身份表 —— 2026-11-09 已完成**：WS/NATS/AMQP/Kafka/Pulsar 都经 `broker_security:authenticate_conn/1` 解析凭据，租户来自用户记录（`docs/security.md` §2.1）。
2. 跨节点/复制通道 TLS。
3. Schema Registry 内容校验。
4. 审计日志签名/防篡改、外部 KMS。
5. 未知扩展属性「按租户可配拒绝」。
6. 除 MQTT 外五种协议的**标准** CloudEvents Binding 逐字实现 —— **已立项为 S7**（`docs/roadmap.md` §9）：交付形态是逐协议给出逐字实现或「无官方 Binding」的明确结论；当前仍是统一 `ce-*` 头通道的等价映射，非无损点见 `docs/protocol-compatibility.md` §4.10。
