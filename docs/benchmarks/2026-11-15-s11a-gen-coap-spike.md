# S11-A 构建与兼容性 spike（`gen_coap` × OTP 28.3.1 / rebar3 3.27）

- **对应**：[`docs/design/s11-coap.md`](../design/s11-coap.md) §4.3 **OQ-3**（旧式 rebar / OTP 19.2 代码与当前 OTP 28.3.1 / rebar3 3.27 的兼容性；是否 vendor）；ADR-030 §D1（仅 CoAP 进入实现评审）
- **执行日期**：2026-11-15
- **环境**：OTP 28 / ERTS 16.2（`/home/wanli/Development/otp-28.3.1`）、rebar3 3.27.0
- **被测版本**：`gotthardp/gen_coap` `master` @ `245e220`（"More Erlangs to test against"）
- **性质**：**spike，不是产品代码**。结论决定 S11-A 采用「vendor + 打补丁」还是「手写编解码」。**本轮不新增 `apps/` 下的任何代码**。

---

## 1. 结论先行

| 项 | 事实 |
|---|---|
| 构建（rebar3 3.27 / OTP 28） | ✅ **编译通过**，24 个 `.beam`，零依赖（`rebar.config` 无 `{deps,...}`） |
| 纯编解码（`coap_message_parser` / `coap_message` / `core_link`） | ✅ **可用**，字节级往返一致（见 §2） |
| 运行时 —— `http_uri`（4 处） | ❌ **`undef`**：`http_uri` 在 OTP 23 起移除，改 `uri_string` |
| 运行时 —— `pg2`（6 处） | ❌ **`undef`**：`pg2` 在 OTP 24 移除，改 `pg` |
| 客户端 Observe API | ❌ **不存在**：`coap_client` 只有 `request/2,3,4,5`，无订阅/通知接口 |
| Observe 服务端路径 | ❌ **必崩**：`coap_responder:handle_observe/4` 首行即 `pg2:create` |
| **净结论** | **不原样 vendor 可用**。要么打 2 类补丁（`http_uri`→`uri_string`、`pg2`→`pg`）并补客户端 Observe，要么只借鉴其编解码思路自写。**推荐「自己写编解码 + 不 vendor」**，理由见 §4 |

---

## 2. 已证实可用：纯编解码

`#coap_message{}` 构造 → `coap_message_parser:encode/1` → `decode/1` → 再 encode，**字节级完全一致**：

```
encode -> <<68,1,18,52,116,111,107,49,97,0,83,114,101,115,255,104,105>>
decode -> {coap_message,con,get,4660,<<"tok1">>,
                        [{uri_path,[<<"res">>]},{observe,0}], <<"hi">>}
BYTE-EXACT: true
```

`core_link`（CoRE Link Format，`.well-known/core`）encode/decode 也正常：

```
encode -> "</res>;rt=\"core.ps\""
decode -> [{absolute,[<<"res">>],[{rt,<<"core.ps">>}]}]
```

**范围**：4 字节固定头、Token、Options（含 `observe` / `block1` / `block2` / `uri_path` / `content_format` 等）、payload。这部分是「RFC 7252 报文层的可靠资产」。

---

## 3. 已证实不可用：OTP 移除的 API

### 3.1 `http_uri`（4 处，OTP 23 起移除）

| 文件:行 | 调用 |
|---|---|
| `src/coap_client.erl:105` | `http_uri:parse(Uri, [{scheme_defaults, ...}])` |
| `src/coap_client.erl:126` | `http_uri:decode(Seg)` |
| `src/core_link.erl:52` | `http_uri:encode(binary_to_list(Seg))` |
| `src/core_link.erl:54` | `http_uri:encode(...) ++ "/" ++ join_uri(Uri)` |

实测：`coap_client:request(get, "coap://127.0.0.1:56833/res")` → `{'EXIT',{undef,[{http_uri,parse,...}]}}`。**任何走 `coap_client` 的调用都会立即崩**。

替代已验证可行：`uri_string:parse/1`（返回 map，非 `http_uri` 的 6 元组——**签名不兼容，必须改代码**）、`uri_string:percent_decode/1`、`uri_string:quote/1`。

### 3.2 `pg2`（6 处，OTP 24 起移除，全在 `coap_responder.erl`）

| 行 | 调用 | 所属路径 |
|---|---|---|
| 30 | `pg2:get_members/1` | 通知扇出 |
| 205 | `pg2:create/1` | **`handle_observe/4` 第一行** |
| 206 | `pg2:join/2` | `handle_observe/4` |
| 230 | `pg2:leave/2` | `cancel_observer/2`（unobserve） |
| 232 | `pg2:get_members/1` | `cancel_observer/2` |
| 233 | `pg2:delete/1` | `cancel_observer/2` |

实测：`pg2:create({coap_observer, [<<"res">>]})` → `{'EXIT',{undef,[{pg2,create,...}]}}`（`pg2.beam` 仍在 `kernel-10.5/ebin` 但只是残留，调用即 `undef`）。

**含义**：即便修好 `http_uri`，**Observe 是 CoAP 立项的核心价值，而它在 OTP 28 上的实现路径第一行就崩**。`pg2`→`pg` 不是机械替换（`pg` 的 scope 模型、`join/2` 返回值、`get_members/1` 语义都不同）。

### 3.3 `coap_client` 无 Observe API

`coap_client` 导出只有 `request/2,3,4,5` 与 `ack/2`，**没有** `observe` / `unobserve` 入口。Observe 只在 **server 侧**（`coap_resource:coap_observe/4`）有回调。因此「Observe 端到端」要么自写客户端订阅逻辑，要么用 `gen_udp` 裸发（本 spike 用它绕过崩溃点，见 §5）。

---

## 4. 决策建议：不 vendor，自写编解码

| 方案 | 代价 | 判断 |
|---|---|---|
| A. vendor `gen_coap` + 打 2 类补丁 + 补客户端 Observe | 改 10 处（含语义不兼容的 `uri_string` map / `pg` scope），且上游 `master` 无版本、README 自述「API may change」 | ❌ 引入不受控 fork |
| B. 只 vendor 编解码三模块（`coap_message_parser` / `coap_message` / `core_link`），传输与 Observe 自写 | 仍需修 `core_link` 的 2 处 `http_uri`；模块间有 `-include("coap.hrl")` 耦合 | ⚠️ 可行，但仍带 fork 负担 |
| C. **自写编解码（照 `coap.hrl` 记录形状借鉴）+ 自写 UDP 前端 + 自写 Observe** | 4 字节头 + 十几种 option，规模与 S8-C `mqtt_sn_codec` / S10-C `stun_codec` 同量级，**已有两次同类成功先例** | ✅ **推荐** |

**推荐 C 的理由**（与项目既有姿态一致）：

1. **ADR-029 先例**：S10-C 面对 `processone/stun`（拖 `fast_tls` + 用不上 5349）时选择**手写 STUN/TURN 编解码**，理由正是「20 字节头 + 少量属性，规模可控且有测试钉死」。CoAP 的报文层是同一量级。
2. **S8-C 先例**：MQTT-SN 网关就是自己写 `mqtt_sn_codec` + 单 socket 多会话，没有 vendor `emqttsn`。
3. **净新增依赖保持 0**：vendor 会把上游的「无版本 master」变成我们 fork 的一部分，`rebar3 release` 里多一个不受控组件。
4. **Observe 必须按我们的订阅/游标模型写**（ADR-013），上游的 `pg2` 扇出模型与 `broker_subscription` 是两套语义——借它的编解码可以，借它的 Observe 反而是负担。

---

## 5. spike 命令留痕（可复跑）

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
cd /tmp/opencode/s11-coap-spike
git clone --depth 1 https://github.com/gotthardp/gen_coap && cd gen_coap
git log --oneline -1                 # 245e220 More Erlangs to test against
rebar3 compile                       # exit 0，24 beams，6 条 pg2 警告
erl -noshell -eval 'pg2:create(x)'   # {'EXIT',{undef,...}}
```

补丁验证（`gen_coap_patched`，仅 spike 用，不进仓库）：`coap_client.erl` 的 `http_uri:parse/decode` → `uri_string:parse/percent_decode`（并适配 map 返回值）后，`coap_client:request/2` 可发起 UDP 请求；服务端 `coap_responder` 的 GET 路径在回调形状正确时可返回内容。**Observe 仍因 `pg2` 不可用。**

## 6. 对 OQ-3 的回答

> **`gen_coap` 在 OTP 28.3.1 + rebar3 3.27 上「编译通过、运行时核心功能不可用」**（`http_uri` 4 处 + `pg2` 6 处 + 无客户端 Observe API）。**不原样 vendor**。S11-A 采取**自写 CoAP 报文编解码**（借鉴 `coap.hrl` 的记录形状与 option 编号，规模对齐 `stun_codec` / `mqtt_sn_codec`），传输与 Observe 按本项目模型自建。**净新增运行时依赖保持 0**，不引入 fork、不引入外部守护进程。
