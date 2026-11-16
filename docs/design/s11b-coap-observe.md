# S11-B 设计：CoAP Observe（RFC 7641）+ 内核映射

**阶段**：S11-B（`docs/roadmap.md` §13.1，ADR-031 §D2 的落点）
**前置**：S11-A 首个代码交付已落地（`broker_proto_coap`，CT 73 例、全量 1324/1324）
**冻结合同**：ADR-031 §D2/§D3、R-O1~R-O4（`docs/design/s11-coap.md` §3.2）

## 1. 目标与边界

在 `broker_proto_coap` 里实现 RFC 7641 Observe：`Observe=0` 建立订阅、服务端事件生成通知、`Observe=1`/RST 取消。订阅与游标**完全复用** `broker_subscription` + ADR-013 cursor，adapter 不写第二套消息内核（R-O2）、不引入 `pg2` 扇出（ADR-031 §D2）。

**不做**（留 S11-C 或非目标）：DTLS（§D5）；Observe 的跨节点/崩溃恢复语义超出单节点 UDP 前端（`docs/architecture.md` §3.18 已声明单节点）；Block2 服务端分块。

## 2. 进程模型：listener 直接当 notify pid（用户已定）

S11-A 的 `coap_listener` 是**唯一 UDP socket owner + 唯一状态持有者**，且用 `{active,100}` 收 datagram。S11-B 让同一个 listener 进程兼任 Observe 的 notify 目标：

- `coap_resource` 对 GET 带 `Observe=0` 时调用 `broker_subscription:subscribe/4` 建订阅，`broker_subscription:attach/3` 把 **listener pid** 登记为 consumer（persist=false）。
- 内核发布 → `broker_subscription:dispatch/2` 给 listener 发 `{broker_deliver, ClientId, MaxQoS, Matched, {P,O,Msg}}`（`broker_subscription_worker:dispatch/2` 的既有扇出）。
- listener `handle_info({broker_deliver,...})` → 对每个活跃 Observe 会话生成并发送通知，CON 通知启重传 timer。

**为什么 listener 自己当 notify 而不是每会话一进程**：通知发送只是一次 `gen_udp:send`（成本极低），CON 重传的 timer 状态用 map 存在 listener state 即可，不需要为它单开监督树。这与 S11-A「单 socket owner 持有全部有界状态」的姿态一致，也避免 N 个进程 + `simple_one_for_one` 的额外复杂度（首期 YAGNI）。代价——所有通知的 UDP 编码在 listener 串行——在受限设备场景下不构成瓶颈（通知速率受对端窗口与 `max_requests_per_second` 约束）。

**listener 因此从纯 gen_server 变成「UDP 收包 + 主动通知发送」的混合体**，`handle_info` 新增 `{broker_deliver,...}` 子句。`{active,100}` 预算不变：通知是**发送**不是接收，不占收包预算。

## 3. 会话与状态归属（R-O3）

UDP 无连接，没有持久会话。**在途 CON 请求**与**活跃 Observe 会话**都是进程内状态，不是持久会话：

- **在途 CON**：`{IP,Port,Token}` → 临时请求进程（用于把异步 Piggybacked ACK 配到请求；S11-A 现有 piggybacked 路径已覆盖同步情形，B 期不改）。
- **活跃 Observe 会话表**（listener state 新字段 `observes = #{{IP,Port,Token} => #{sub_ref, dest, tenant, principal, max_age, seq}}`）：
  - 键 `{IP,Port,Token}` 是**一次 Observe 关系**的标识，不是「会话」——对端不发 RST 就一直在，这与 RFC 7641 一致（观察关系由 `Observe` 注册/注销或超时终止）。
  - 生命周期：注册（收到 `Observe=0`）→ 活跃 → 注销（收到 `Observe=1`/RST，或客户端 CON 进程 DOWN，或 `Max-Age`/非确认超时）。
  - **清理口径**：listener 是 notify pid，`broker_subscription:attach/3` 已 `monitor` listener；**反过来** listener 需要感知对端消失——CoAP UDP 无连接，无法直接感知对端进程，因此用**非确认超时**：CON 通知在 `ACK_TIMEOUT` 内无匹配 ACK（按 Message ID 关联）重传 `MAX_RETX` 次后，放弃该关系的活跃状态并 `unsubscribe`（仅当 observes 表无其它关系观察同一 dest 时才退订、表清空时才 `detach`，见 §5）；NON 通知只发一次不超时（至多一次，D3）。

**不变量**：`observes` 表与 `dedup`/`endpoints`/`block1` 一样**有界**（`max_observes`，默认 4096；满时拒绝新的 `Observe=0` 注册并回 `4.03 Service Unavailable`），在 1 s tick 上按 `Max-Age`/非确认超时清扫。与 §3.18 的三张表同一姿态。

## 4. 可靠性分档（R-O4 / §D3，用户已定）

通知**没有**回传给服务端的 ACK 通道（CoAP 的 ACK 只确认 piggybacked 请求响应，不确认独立通知；RFC 7641 §3.6 的「可确认通知」靠的是**通知自己的重传**，由对端 ACK）。因此：

| 通知类型 | 语义 | 实现 |
|---|---|---|
| 请求是 **NON** | 至多一次 | 发一个 NON 通知，不重传、不超时。丢失是协议固有。 |
| 请求是 **CON** | datagram 级重传（**非 QoS 1**） | 发 CON 通知（新 Message ID，token 复用），启 `start_timer(ACK_TIMEOUT)`；收到**按 Message ID 关联**的空 ACK（TKL=0，不带 token）→ 取消 timer 并推进序列；超时 → 按 timer 精确定位重传并计数，`MAX_RETX` 次后放弃该关系（NSTART=1：同关系新通知替换未确认旧通知）。 |

**显式承诺（写进能力矩阵 §4.24.4）**：CON 通知是 **datagram 级**可靠性——尽力送达、有重传上限，**不落盘、不在内核重投、不等于 MQTT QoS 1**。NON 通知至多一次。二者都不提供「恰好一次」。这是 §D3 的冻结口径。

**序列与 Max-Age（RFC 7641 §3.2/§3.3）**：每个活跃关系的通知带 `Observe` 选项 = 单调递增序列（0..2^24-1 回绕，`(Seq+1) band 16#FFFFFF`）；`Max-Age`（来自资源的 `max-age`，默认 30 s）作为**非确认兜底超时**：超过 `Max-Age` 无更新则关系自然消亡（对端应重新 `Observe=0`）。

## 5. 数据流

```
注册：GET(Observe=0, token=T)
  → coap_resource: 鉴权(subscribe ACL) → broker_subscription:subscribe/4 → SubRef
  → broker_subscription:attach/3(listener, persist=false)
  → listener.observes[{IP,Port,T}] := #{sub_ref, dest, tenant, principal, max_age, seq=0}
  → Piggybacked ACK + Observe:0 + 当前表示（resource GET 的 retained）

通知：内核 publish 命中 dest
  → broker_subscription:dispatch/2 → listener ! {broker_deliver, ClientId, _, _, {P,O,Msg}}
  → listener 对每个 observes 关系（新通知替换该关系未确认旧通知，NSTART=1）：
       编码通知(Observe=seq, payload=Msg.payload)
       请求 CON → 发 + start_timer(ACK_TIMEOUT) 存 pending[{IP,Port,MsgId}]；请求 NON → 发
  → 收到 {udp,_,IP,Port,ACK}：按回显的 Message ID 匹配 pending（空 ACK 无 token）→ 取消 timer，seq 推进

注销：GET(Observe=1, token=T) 或 收到 RST(回显某通知的 Message ID)
  → broker_subscription:unsubscribe/2（只退订该 dest 的 filter，且仅当无其它关系仍观察同 dest）
  → 取消该关系（完整 {IP,Port,Token}）的 pending timer，observes 移除该键
  → **仅当 observes 表清空时**才 broker_subscription:detach/2
    （detach/2 拆的是整个 <<"coap">> 会话 = 所有订阅，而所有观察共用一个匿名 ClientId，
      故按 token 逐个 detach 会把同会话的其它观察一起误删 —— 见 ADR-031 落地偏差记录 S11-B-⑥）
```

## 6. 组件改动清单

| 文件 | 改动 |
|---|---|
| `coap_codec.erl` | 已支持 `observe` option 解析/编码（S11-A）；无需改。 |
| `coap_resource.erl` | `get/3` 识别 `Observe` option：`0` → 建订阅 + attach + piggybacked 初始表示；`1` → 注销。新增 `authorize` 的 `observe` action（复用 subscribe ACL）。非 GET 带 `Observe` → 忽略（RFC 7641 §3.1）。 |
| `coap_listener.erl` | state 加 `observes`、`pending`（未确认 CON 通知 timer，键 `{IP,Port,MsgId}`，NSTART=1）；`handle_info({broker_deliver,...})` 生成/发送通知 + 启 timer；`handle_info({udp,...})` 按空 ACK/RST 的 **Message ID** 匹配 pending 取消 timer、RST 按端点作用域注销关系；1 s tick 的 `prune/1` 增 `max_observes` 上限检查 + 非确认超时清扫。 |
| `broker_proto_coap.app.src` | env 增 `max_observes`(4096)、`observe_ack_timeout_ms`(2000)、`observe_max_retrans`(3)、`observe_max_age_s`(30)。 |
| `coap_resource_SUITE` / `coap_listener_SUITE` | 新增 Observe 用例（见 §7）。 |

## 7. 测试（真实 UDP，`port => 0`）

- `observe_registers_and_sends_initial`：`Observe=0` → piggybacked ACK 带 `Observe:0` + retained 表示；随后 publish 一条 → 收到 NON 通知带递增 `Observe` 序列。
- `observe_con_notifies_and_acks`：CON `Observe=0` → publish → 收到 CON 通知；回一个按 Message ID 关联的空 ACK（TKL=0，无 token）→ 无重传；再 publish → 第二个通知。
- `observe_con_retries_on_missing_ack`：CON 通知发出后不回 ACK → 在 `ACK_TIMEOUT` 后重传，`MAX_RETX` 次后放弃（关系被清，`unsubscribe`）。
- `observe_non_is_at_most_once`：NON `Observe=0` → publish → 只收到一个通知，不回 ACK 也不重发。
- `observe_deregister`：`Observe=0` → publish（有通知）→ `Observe=1` → 再 publish → 无通知。
- `observe_rst_deregisters`：注册后发**回显某通知 Message ID 的空 RST**（无 token）→ 再 publish → 无通知。
- `observe_plain_udp_principal_is_allowed`：ACL 受限部署下 plain UDP 的 principal 为 `undefined`，`authorize/2` 直接放行 → 仍可观察（S11-B 的 4.03 只来自容量路径；4.03 的接入鉴权待 S11-C DTLS 客户端身份）。
- `observe_bounded_table`：`max_observes=1`，注册两个不同 token 的第二个 → `4.03`（首个关系不受影响）。
- `observe_stray_rst_does_not_crash` / `observe_rst_is_endpoint_scoped`：未知 id 的空 RST 不崩且关系存活；跨端点的空 RST 不能拆别的观察者（端点作用域）。
- `observe_down_cleans_up`：注册后停 listener 对端 → 关系超时清除（`broker_subscription:subscriptions/1` 不再含该 SubRef）。

CT 例数：预计 `coap_listener_SUITE` +9、`coap_resource_SUITE` +1~2（纯函数层的 option 路由）。全量基线 1324/1324、83 套件零回归；`dialyzer` 0 warning；`xref` 302 unused-export。

## 8. ADR-031 落地偏差（预期）

本阶段是 §D2 的实现落点，预期**无边界级偏差**。实现中若出现「listener 兼 notify pid 导致某处需偏离 §D2 的 adapter 只持映射」的细节，记入 ADR-031「落地偏差记录」，冻结文本不改。

## 9. 收口

全量 CT 零回归 + dialyzer/xref 干净 + `rebar3 release` 装配成功后，同步：能力矩阵 §4.24 追加 Observe 行与「已支持/限制」、`docs/architecture.md` §3.18 增 Observe 状态、`docs/roadmap.md` §13.1 S11-B 行、`docs/security.md` §12 追加、ADR-031 落地偏差、`progress.md`/`README` 计数。
