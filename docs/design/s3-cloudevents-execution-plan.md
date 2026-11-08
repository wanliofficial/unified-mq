# S3 — CloudEvents 执行计划

- **依据**：[ADR-020](../adr/020-cloudevents-codec-policy-binding.md)（已采纳）、README §3/§4/§11.2
- **状态**：已完成（2026-11-08，Gate S3 通过）
- **不动的东西**：MQTT 3.1.1 线上字节、ADR-009 的 `#broker_message{}` 字段、`pass_through` 默认行为

## 1. 交付物

| 交付物 | 模块 | 验收 |
|---|---|---|
| JSON Codec（Structured） | `broker_event_codec` | 必需/可选/扩展校验、`data`/`data_base64` 互斥、时间格式 |
| Binary Mode Codec | `broker_event_codec` | `ce-*` 元数据 ↔ 事件 map |
| MQTT Binding Mapper | `broker_event_binding` | 3.1.1/5.0 正反向映射 + 损失报告 |
| 策略开关 | `broker_event` | 五档策略、按租户+目的地解析 |
| 去重窗口 | `broker_event_dedup` | 可配窗口、`source+id` 键、默认关闭 |
| 拒绝/隔离 | `broker_event` + `mqtt_conn_fsm` | 不 ACK、可选隔离副本、v5 reason code |
| 指标 | `broker_metrics` | `cloudevents_validate_ok/failed`、`cloudevents_duplicates`、`cloudevents_quarantined` |

## 2. 实现步骤

1. `broker_event_codec`：JSON 1.0 解码/编码、`validate/1`、binary 元数据编解码。
2. `broker_event_binding`：检测 Structured/Binary、`decode/1`、`to_mqtt/3`、损失报告。
3. `broker_event_dedup`：受监督 gen_server + ETS；`check/2`、`reset/0`。
4. `broker_event`：策略解析与五档实现；`apply_policy/1,2`；`duplicate/1`；`quarantine/2`。
5. `mqtt_conn_fsm`：发布路径调用 `apply_policy/1`；拒绝时 v5 回 `0x83`、3.1.1 断开；去重命中只 ACK 不存储。
6. 配置默认值：`broker_event.app.src` / `config/sys.config.src`（`policies`、`dedup`、`quarantine` 默认空）。
7. 测试：`broker_event_SUITE` 覆盖 README §11.2；`mqtt_conn_SUITE` 增加拒绝路径用例。

## 3. 测试步骤

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
rm -rf _build/test/extras
rebar3 ct --suite=apps/broker_event/test/broker_event_SUITE
rebar3 ct --suite=apps/broker_proto_mqtt/test/mqtt_conn_SUITE
rebar3 ct
rebar3 dialyzer
rebar3 xref
```

## 4. 完成口径

- README §11.2 的每一项在 `broker_event_SUITE` 有具名用例。
- Structured / Binary 正反向、跨协议往返与损失清单通过。
- 重复事件与重投递行为有测试；拒绝路径有 v5/3.1.1 端到端证据。
- ACL 回归：伪造事件属性不改变租户/授权。
- 全量 CT、`dialyzer`、`xref` 通过；文档只写已实现事实。

## 5. 交付记录（2026-11-08）

| 交付物 | 实现 | 证据 |
|---|---|---|
| CloudEvents 1.0 JSON codec | `broker_event_codec`：`decode_structured/1`、`encode_structured/1`、`decode_binary/3`、`encode_binary/1`、`validate/1` | `structured_roundtrip_preserves_extensions`、`missing_required_attributes_rejected`、`wrong_core_types_rejected`、`unsupported_specversion_rejected`、`invalid_time_rejected`、`valid_time_accepted`、`data_and_data_base64_mutually_exclusive`、`invalid_base64_rejected`、`extension_name_and_count_validated`、`unknown_extension_preserved` |
| MQTT Binding + 损失报告 | `broker_event_binding`：`detect/1`、`decode/1`、`to_mqtt/3`、`report/4` | `mqtt_structured_binding_roundtrip`、`mqtt_binary_binding_v5_roundtrip`、`mqtt_binary_binding_311_reports_losses`、`cross_protocol_roundtrip_keeps_identity` |
| 五档策略 + 租户/目的地解析 | `broker_event`：`policy/0`、`policy_for/1`、`apply_policy/1,2` | `default_policy_is_pass_through`、`pass_through_is_identity_and_never_parses`、`disabled_is_identity`、`invalid_policy_is_rejected`、`policy_resolution_per_tenant_and_destination`、`validate_if_present_passes_ordinary_messages`、`required_rejects_ordinary_message`、`normalize_attaches_event_and_keeps_payload` |
| 去重窗口 | `broker_event_dedup`（受监督 gen_server + ETS） | `duplicate_window_deduplicates`、`duplicate_is_off_by_default`、`duplicate_window_expires` |
| 拒绝 / 隔离 | `mqtt_conn_fsm:reject_event/4`、`broker_event:quarantine/2` | `quarantine_writes_a_durable_copy`、`mqtt_conn_SUITE:event_policy_rejection_is_a_v5_reason_code`、`event_policy_rejection_closes_311_without_ack` |
| 事件 ID 与 Broker ID 分离 | Binding 只写 `proto_meta`；`event.id` 独立 | `event_id_does_not_overwrite_broker_id` |
| ACL 边界 | 授权只用认证主体 | `acl_does_not_trust_event_attributes` |
| 指标 | `cloudevents_validate_ok/failed`、`cloudevents_duplicates`、`cloudevents_quarantined` | `broker_metrics_SUITE` 键表；`cloudevents_*` 在策略/去重/隔离路径埋点 |
| 配置默认值 | `broker_event.app.src` / `config/sys.config.src`：`policies=[]`、`dedup=[]`、`quarantine=[]` | 全量 CT 中默认 `pass_through` 用例 |

**自测结果**：`rebar3 ct` **422/422 passed**；`rebar3 dialyzer` 0 warnings；`rebar3 xref` 76 条均为预期 unused-export，无未定义函数调用。

**实现期修正**：

1. 测试隔离：`quarantine_writes_a_durable_copy` 会通过 `ensure_all_started(broker_event)` 隐式启动 `broker_metrics`，原先只停部分应用，导致后续 `broker_metrics_SUITE:report_line_is_single` 看到残留的 `broker_metrics_server`。改为记录并逆序停止本次实际启动的全部应用。
2. Dialyzer：`broker_router_hash:route/3` 按 ADR-010 至少返回一个分区，删除不可达的 `{ok, []}` / `{error, _}` 分支（隔离失败仍由外层 try 记为 `quarantine_crashed`）；`detect_by_payload/1` 的兜底分支不可达，删除。
3. 跨协议往返明确两项非无损：Structured `data` JSON term → Binary 字节；integer 扩展 → 文本。测试按**字节等价 + 报告列出损失**断言，而不是假装无损。
