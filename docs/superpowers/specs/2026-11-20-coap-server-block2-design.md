# CoAP 服务端 Block2 分块发送（RFC 7959 §3）

- **对应**：`docs/design/s11-coap.md` §1.3 非目标边界（"完整 RFC 7959 Block-wise" 部分落地）、`docs/protocol-compatibility.md` §4.24.4（当前 "服务端不做 Block2 分块响应"）、ADR-031 §D4（CoAP 资源映射冻结）、ADR-016（指标键集冻结）
- **性质**：**代码交付**，把 S11-C 当前"写进得去、>1 KB 读不出来"的功能缺口补齐服务端下载方向
- **范围决策（本会话已确认）**：**纯服务端分块**——只做服务端按客户端请求的块大小切片回送；**不做**跨块响应缓存/重传（UDP 无状态，与现有 Block1 重组对称）、**不做** 客户端 Block2 请求方向（那是上传，已由 Block1 承担语义）、**不改** RFC 7959 draft 与最终 RFC 的偏差留痕

---

## 1. 现状（已核实，不是猜测）

| 层 | 现状 | 证据 |
|---|---|---|
| `coap_codec` | **Block1/Block2 编解码均已实现**：`decode_block/1`（4/12-bit 块号、1-bit more、3-bit szx → 字节数 16..1024）、`encode_block/1`、`block/2`、`block_value/3`、`szx/1` | `coap_codec.erl:84-85,204-205,225-226,246-255,315-337,486-496` |
| Block1（上传） | **已完整实现**：`block1_stream/5` 按 `{IP,Port,Request-Tag,destination}` 重组、`max_block1_bytes` 上限（256 KiB）、2.31 Continue / 4.13 / 4.08 拒绝 | `coap_listener.erl:662-738`；CT `coap_resource_SUITE` 覆盖 Block1 |
| Block2（下载） | **缺失**：`respond/5` 跑 resource 后经 `resource_reply/4`+`encode_reply/5` **一次性回送整个 payload**，从不读取请求的 `block2` 选项 | `coap_listener.erl:570-580,642-660` |
| 后果 | 端到端响应实际上限约 1 KB（`block_size` 默认 1024，且真正受 UDP 单报文约束）；>1 KB 的 retained 资源**写进得去、读不回来** | `docs/protocol-compatibility.md` §4.24.4 |
| Observe | 已与回复路径交织（`reply_observe/5`），Block2 必须与之组合 | `coap_listener.erl:582-617` |

**结论**：缺的纯是 `coap_listener` 的一个服务端 Block2 切片层，镜像现有 Block1 机制，codec 无需改动。

---

## 2. 设计

### 2.1 接入点

在 `respond/5` 的 `{single, Req, St1}` 分支（Block1 重组完成后的"单体请求"）与 `reply_observe/5` 之间插入一个 **Block2 出口**。GET 走到 resource 拿到完整 `{Code, Options, Payload}` 后，若请求带 `block2` 选项则切片回送，否则走原路（零回归）。

```
respond/5
  └─ block1_stream/5           %% 不变：上传方向
       └─ {single, Req, St1} / {done, Req, St1}
            └─ reply_observe/5 %% 不变：Observe 注册/注销 + 读 resource
                 └─ resource_reply/4  ←—— 改为经 block2_slice/5 出口
                      └─ run_resource/4 → {Code, Options, FullPayload}
                      └─ block2_slice/5(Req, Code, Options, FullPayload)
                           ├─ 请求无 block2 选项 → 原样 encode_reply（现状）
                           └─ 请求有 block2 {Num,_,ReqSize} → 切片 + 注入 block2 选项
```

### 2.2 切片规则（RFC 7959 §3）

- 客户端在请求里带 `block2: {Num, _M, ReqSize}`，**`ReqSize` 是客户端想要的字节数**（Block2 由下载方/客户端指定块大小，与服务端 Block1 的 `max_block1_bytes` 上限语义不同）。
- 服务端把完整 `Payload` 按 `ReqSize` 切片：`Block N = binary:part(Payload, N*ReqSize, min(ReqSize, Size - N*ReqSize))`。
- 回送块带 `block2` 选项 `{Num, More, ActualSize}`：
  - `More = (Num+1)*ReqSize < TotalSize`（还有后续块）。
  - `ActualSize` = 服务端**实际**使用的块大小。RFC 7959 允许服务端把块裁到 ≤ 请求值（对齐到 16 的幂）；本设计**直接采用客户端的 `ReqSize`**（已是 `szx` 合法的 16..1024），即 `ActualSize = ReqSize`，最后一块可不足。这样最简单且不损失正确性。
- **末块**（`More=false`）：照常回该块 payload + `block2:{Num,false,ReqSize}`。
- **无 block2 选项或 payload ≤ ReqSize 且 Num=0**：不加 block2 选项，原样回整个 payload（= 现状，纯 2.05）。这保证非分块客户端零感知。

### 2.3 与 Observe 的组合

- `Observe: 0` 注册时的**首个 2.05**：若该请求同时带 `block2`（RFC 7641 §4.5 允许 Observe 初值用 Block2 协商块大小），首个响应走切片回送，并**保留 `Observe: seq` 选项 + 注入 `block2` 选项**。后续通知（`coap_observe:notify_message/4`）的块大小协商本设计**不处理**（通知是服务端主动推送，块大小由后续客户端 GET+block2 拉取），留痕在 §4。
- 注销（`Observe: 1`）/ 普通 GET：与 Observe 无关，纯按 §2.2 切片。

### 2.4 错误与边界

- 4.04（无此资源）/ 4.03 / 5.05 等**非 2.xx 响应不带 payload，也就不切片**（空 body 无块可分），按原样回。
- `Num*ReqSize ≥ TotalSize` 且 `Num>0`：RFC 7959 §3.5 —— 服务端 MAY 回 5.00（空响应，More=false）。本设计**直接回该越界块的空 payload + `block2:{Num,false,ReqSize}`**，让客户端据 More=false 收尾，不引入新错误码。
- `ReqSize` 已由 codec 解码为 16..1024 的合法值（非法值在 `decode_block` 已被钳到 szx 语义），listener 不再校验位。

### 2.5 配置

- 复用现有 `block_size`（默认 1024）作为**默认块大小**：仅当客户端请求带 block2 但 szx 解析异常（理论不会）时的兜底。正常路径块大小**完全由客户端 `ReqSize` 决定**。
- 不新增 `max_block2_bytes`：下载方向没有"重组累积"内存风险（每块独立、不缓存全payload 之外的状态），完整 payload 来自单次 `run_resource`，内存特征与现状一致。

### 2.6 指标

- **不新增指标键**（ADR-016 冻结键集）。现有 CoAP 只增 `bytes_in`（`coap_listener.erl:480`，入站字节）；出站无对应计数键，Block2 也**不引入**出站键。本设计零指标变更。

---

## 3. 不做（明确边界）

| 项 | 理由 |
|---|---|
| 跨块响应缓存 / 同一块重传命中缓存 | 用户已选"纯服务端分块"；UDP 无状态，与 Block1 不对称正是 Block1 需要重组缓存而 Block2 不需要（每块是完整 resource 读的自包含投影） |
| 服务端主动把块大小裁小到非请求值再协商 | 直接采用客户端 ReqSize，最简单且 RFC 合法 |
| Observe 后续通知的 Block2 推送 | 通知是主动推送，块协商留给后续客户端 GET；本设计只保证首个响应可与 block2 共存 |
| 客户端 Block2 上传方向 | 上传语义已由 Block1 承担，不重复 |
| 改 codec / draft→RFC 偏差 | codec 已完备；draft 18 vs RFC 7959 留痕维持 `s11-coap.md` §1.3 现状 |

---

## 4. 留痕（本设计不解决、如实声明）

- Observe 通知（`coap_observe:notify_message/4`）当前不带 block2，>1 KB 的通知体仍受单报文约束。补齐需改 `coap_observe`，属独立增量，本设计不动。
- `block_size` 配置当前在 listener 未被实际消费（仅默认值存在）；本设计让它成为兜底，正常路径不依赖它。

---

## 5. 验收

- `rebar3 clean compile`：exit 0，零 warning。
- `rebar3 dialyzer`：exit 0，零新增 warning。
- `rebar3 ct`：`coap_resource_SUITE` / `coap_listener_SUITE` / `coap_observe_SUITE` 全绿。
- **新增 CT**：`coap_block2_SUITE`（或并入 `coap_resource_SUITE`）——
  1. retained 资源 payload = 3 KB，客户端 `block2:0/0/1024` GET → 收块 0（1024 B，`block2:{0,true,1024}`）；
  2. 续 `block2:1/0/1024` → 块 1（1024 B，`{1,true,1024}`）；
  3. 续 `block2:2/0/1024` → 末块（976 B，`{2,false,1024}`），三块拼回 = 原始 3 KB；
  4. 不带 block2 的普通 GET → 原样 2.05 完整 payload（零回归）；
  5. payload 恰好 = ReqSize（1024 B）+ block2:0/0 → 单块 `{0,false,1024}`；
  6. Block2 + Observe:0 首个响应同时带 `observe` 与 `block2` 选项；
  7. 4.04 资源带 block2 → 仍 4.04，无块、无 payload。
- 全量 `rebar3 ct` 不回归（当前基线 1568 passed）。
- 文档：`protocol-compatibility.md` §4.24.4 与 `s11-coap.md` §1.3 把"服务端不做 Block2"更新为"服务端支持 Block2 分块发送（不含跨块缓存/通知分块）"。

---

## 6. 风险

- **低**。改动集中在 `coap_listener` 的回复出口，新增一个纯函数 `block2_slice/5`，codec 与 resource 层不动；非分块路径保持原样，回归面小。
- 唯一需小心的是 Observe 首响应与 block2 选项的共存（选项顺序、seq 选项不被覆盖），由验收项 6 守护。
