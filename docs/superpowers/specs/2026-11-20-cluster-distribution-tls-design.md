# 跨节点 / 复制通道 TLS — 设计（S12 之后的安全收口，2026-11-20）

> 状态：**设计已评审通过；实现前的 OTP 行为 spike 已完成，结论已并入正文（2026-11-20）**。
> 触发：`docs/security.md` §12 第 1 条「跨节点 / 复制通道的 TLS 未实现」，以及 §12.7
> 对 S12-D 新扩大的分布信任面的留痕。本设计收口该缺口。

## 1. 问题与硬约束

S12-D 把节点间 RPC（QoS0 广播、转发读/写）与 ra 元数据复制都放到同一条 Erlang
distribution 通道上，而这条通道今天只有 **magic cookie** 这一层弱鉴权、且是明文。
`security.md` §12.7 已如实写明：开启 `cross_node_delivery` 会扩大这条既有信任面，
跨节点 / 复制通道 TLS 仍是未实现项。本设计补上它。

**硬约束（决定全部形态）**：OTP 在 **boot 期**用 `-proto_dist` 选定分布协议，
**早于任何 application 加载**。因此：

- `broker_security_tls` 的「运行时切换」模式**不适用**——节点间 TLS 握手的选项由
  `inet_tls_dist` 在 app 启动前消费，应用层无法注入。
- 本特性**不是运行时可切换的开关**，是部署时（vm.args + env）的显式选择。
- 它加密**全部分布流量**（ra 复制 + 跨节点 RPC），无法只加密 ra——ra 就走节点分布。

### 1.1 实现前 spike 的实测结论（2026-11-20，OTP 28.3.1）

在写计划前用一个独立 escript 把「文档说的」逐条验掉。四条结论改变了初稿：

| # | 实测发现 | 对设计的影响 |
|---|---|---|
| 1 | **`-ssl_dist_opt server certfile X` 这种写法是错的**。`inet_tls_dist:ssl_options/4` 只认下划线拼接的键名（`server_certfile`、`client_keyfile`），写成 `server` + `certfile` 两段会在连**出**时 `function_clause` 崩溃（`inet_tls_dist.erl:867`）。 | 初稿的 vm.args 块**不可用**。改用 **`-ssl_dist_optfile <File>`**：文件是普通 Erlang term（`[{server,[...]},{client,[...]}]`），由 `ssl_dist_sup:consult/1` 用 `erl_scan` 解析，无键名拼接问题、可写 `%` 注释。 |
| 2 | `-ssl_dist_optfile` 路径写错 → **节点直接起不来**（`net_sup` 的 `start_error`，`{bad_ssl_dist_optfile, Path}`）。 | 少了「配了 TLS 却静默明文」的失败模式；渲染器仍应输出绝对路径（相对路径按节点 CWD 解析）。 |
| 3 | `-proto_dist inet_tls` **可被应用回读**：`init:get_argument(proto_dist)` 返回 `{ok,[["inet_tls"]]}`；进程 `ssl_dist_sup` 存在 ⟺ 本次 boot 真的用了 TLS 分布；用 `-ssl_dist_optfile` 时还能从 `ssl_dist_opts` ETS 表读到实际生效的 server/client 选项（内联 `-ssl_dist_opt` 则**该表不存在** ⇒ 选项无从回读）。 | 初稿「vm.args 无法回读、只能靠 fail-loud 兜住」**过于悲观**。`validate/0` 可以做**双向一致性校验**：env 声明 vs 实际 boot 协议 vs 实际选项。前提是**强制用 optfile**——因此 validate 对「用了内联 `-ssl_dist_opt`」直接报错并指向 renderer，而不是容忍一个无法校验的载体。 |
| 4 | 默认**不做**主机名 / SAN 校验：CA 签发但 SAN 写成 `IP:10.99.99.99` 的证书，连 `127.0.0.1` 的节点**照样握手成功**；完全没有 SAN 的证书也一样。SAN 只有在两个条件**同时**满足时才被校验：① 选项里带 `{verify_fun,{fun inet_tls_dist:verify_client/3,_}}` 这个 marker；② `net_kernel` 的 allowed 列表**非空**（实测必须用 `net_kernel:allow/1` 在连接前设，`-kernel net_kernel allowed [...]` 这个启动标志**不生效**，`net_kernel:allowed/0` 仍是 `[]`）。满足后 `allowed_nodes/4` 按 peer IP 与 allowed host 校验 SAN，错配报 `{bad_cert, cert_no_hostname_nor_ip_match}` 并拒绝。 | **CA 信任就是准入闸门，SAN 默认不参与判定**。这必须写进安全文档而不是假装有主机名校验；可选加固的具体前置条件也一并写明（否则运维以为配上就生效）。 |

另两条已验证的正向事实：mTLS（`verify_peer` + `fail_if_no_peer_cert`）在 dist 上按预期
工作；跨 CA 的节点握手失败为 `Fatal - Unknown CA`、`connect_node` 返回 `false`、
`nodes/0` 为空（失败是**干净且可观测**的）。两个节点都用 `inet_tls` 但都不带证书时
互相 `pang`（fail-closed，不会退化成明文）。

还有一条对测试写法的约束：`verify_fun` 里写 `{inet_tls_dist, verify_client, []}` 会被
`inet_tls_dist` 判成 `Invalid TLS option` 而拒握手，必须写成
`{fun inet_tls_dist:verify_client/3, []}`（同一个 marker 靠**函数值**识别）。

## 2. 决策摘要

| 维度 | 裁决 |
|---|---|
| 实现机制 | OTP 原生 `-proto_dist inet_tls`（`inet_tls_dist`），不自造分布模块 |
| 选项载体 | **`-ssl_dist_optfile <File>`**（唯一支持的载体）。内联 `-ssl_dist_opt` 下划线键名虽能被 OTP 解析，但**不创建 `ssl_dist_opts` ETS 表**，实际选项无从回读 ⇒ 无法校验，`validate/0` 直接拒绝（见 §1.1 第 1、3 条） |
| 鉴权 | **mTLS**：`verify_peer` + `fail_if_no_peer_cert`，节点互验证书；**CA 信任是成员准入闸门** |
| 主机名校验 | **默认不做**（OTP 默认行为），文档如实写明；需要时 = `verify_fun` marker **且** 运行期 `net_kernel:allow/1` 非空（`-kernel allowed` 启动标志实测不生效），列为可选加固，不在本期实现 |
| 证书来源 | 独立 `broker_cluster.dist_tls` 块，与面向客户端的 `broker_security.tls` 分开 |
| 默认 | **`enabled => false`**：默认仍是明文 `inet_tcp` + cookie，与现状逐字节相同 |
| Cookie | **保留** mTLS 之上的 cookie 作为第二因子（OTP 默认仍检查） |
| 漂移防护 | `validate/0` **双向**校验 env 声明 vs 实际 boot 协议与实际生效选项，不一致即拒启 |
| 运行时开关 | 不做（OTP 约束）；证书轮换 = 换 PEM + 重启节点 |
| 新增 app / 端口 / 指标键 | 无**第三方**依赖、无新 app、无新端口（分布复用既有 dist listen 端口，epmd 注册）；`broker_metrics` 键集不变。`broker_cluster.app.src` 的 `applications` 追加 OTP 自带的 **`public_key`**（用于 PEM 校验，见 §4 第 3 条） |

方案取舍（B「自造 `-proto_dist` 读 app env」被否：分布先于 app 加载，读不到 env；
C「relx overlay 构建期生成」可行但只能构建期决定、需重建 release，不如 A 直接），
最终选 **A：optfile + vm.args 承载实际选项，env 作声明与校验，renderer 从 env 生成
optfile 避免手抄漂移**。

## 3. 组件

### 3.1 `broker_cluster_dist_tls`（新模块，`broker_cluster` 内）

纯函数模块，**无进程、无监督树子项**（照 `broker_rebalance` / `broker_cluster_rep` 形态）。
对外：

- `enabled/0 -> boolean()` —— 读 `broker_cluster.dist_tls.enabled`。
- `config/0 -> map()` —— 取 env 并合并出厂默认。
- `validate/0 -> ok | {error, term()}` —— boot 期调用；内部等价于 `validate(effective())`。
- `validate/1` —— 纯函数变体，入参是 `effective/0` 的形状；测试用它注入「没开 TLS 的
  boot 状态」等场景，不必真的改节点 `init` 参数。
- `effective/0 -> map()` —— 实际 boot 状态：`proto_dist`（`init:get_argument/1`）、
  `tls_booted`（`whereis(ssl_dist_sup) =/= undefined`）、`optfile`（`ssl_dist_optfile`）、
  `inline_opt`（`ssl_dist_opt` 是否存在）、以及从 `ssl_dist_opts` ETS 读到的
  `#{server => [...], client => [...]}`（不存在时为 `undefined`）。
  供 `validate/0` 与节点视图复用。
- `ssl_dist_opts/0 -> #{server => [proplists], client => [proplists]}` —— 由 `config/0`
  构造两侧选项；**同时**被 renderer 与测试消费，保证只有一处定义形状。
- `render_optfile/0 -> iodata()` —— 输出可直接写成 optfile 的 Erlang term，
  **路径一律 `filename:absname/1` 规范化**。
- `vm_args_lines/0 -> iodata()` —— 输出要追加进 vm.args 的两行：
  `-proto_dist inet_tls` 与 `-ssl_dist_optfile <abs path>`。
- `redacted_config/0 -> map()` —— 节点视图/日志用；**不含密钥材料**，只给
  `enabled`/`verify`/`versions`/certfile 的 basename。

### 3.2 `broker_cluster.dist_tls` env 块

```
{broker_cluster, [{dist_tls, #{enabled              => false,
                                certfile             => undefined,
                                keyfile              => undefined,
                                cacertfile           => undefined,
                                verify               => verify_peer,
                                fail_if_no_peer_cert => true,
                                versions             => ['tlsv1.2', 'tlsv1.3'],
                                optfile              => undefined}}]}
```

`.app.src` 出厂默认 `enabled => false`。`optfile` 是 optfile 的路径：`enabled => true`
时**必须显式给出**（`validate/0` 不接受 `undefined`——不替运维猜路径，也不偷偷写进
`data_dir`），`vm_args_lines/0` 与 `validate/0` 都从它取值。

### 3.3 boot 钩子

`broker_cluster_app:start/2` 在起监督树**之前**调 `broker_cluster_dist_tls:validate/0`：

- 校验通过 → 继续启动。
- 校验失败 → `start/2` 回 `{error, {dist_tls_invalid, Reason}}`，**app 拒绝启动**
  （fail loud，照 `broker_ice:validate/0`、`broker_proto_coap:validate/0`）。

默认（disabled + 明文分布）必须零成本通过，否则会破坏既有 1472 例基线。

### 3.4 节点视图 additive 字段

`broker_admin_http` 的节点视图 JSON 增加 `dist_tls` 字段（`redacted_config/0` 的形状，
外加 `effective/0` 的 `proto_dist`）。纯附加，无契约破坏；ADR-016 冻结的
`broker_metrics` 键集不含此字段（它不是指标）。

取值方式照 `broker_admin_http` 既有的**容错调用**惯例（它已经这样调 `broker_cluster:owner/1`
而**没有**在 `broker_admin.app.src` 里声明 `broker_cluster`）：`try ... catch _:_ -> #{enabled => false} end`，
所以在 admin-only 启动或 cluster 应用未起时节点视图照常返回，只是 `dist_tls.enabled = false`。

## 4. 错误处理（`validate/0` 的判据）

按顺序判：

1. **声明与 boot 一致**（§1.1 第 3 条）：
   - `enabled = true` 但 `proto_dist ≠ inet_tls` → `{error, {dist_tls_enabled_but_booted_plain, Actual}}`
   - `enabled = true` 但用了内联 `-ssl_dist_opt`（`ssl_dist_optfile` 缺失而 `ssl_dist_opt` 存在）
     → `{error, {dist_tls_requires_optfile, see_renderer}}`
   - `enabled = false` 但 `proto_dist = inet_tls`（或 `ssl_dist_sup` 活着）
     → `{error, {dist_tls_booted_with_tls_but_disabled, Actual}}`
   - `enabled = false` 且明文 → `ok`（**不做任何文件检查**，保持默认零成本）
2. **文件可得**：`certfile`/`keyfile`/`cacertfile`/`optfile` 非 `undefined` 且文件存在，
   否则 `{error, {missing_file, Which, Path}}`。
3. **PEM 可解析**：用 `public_key:pem_decode/1` 验 cert/key/CA（**不需要 `ssl` 应用已启动**，
   实测可用），失败 → `{error, {bad_pem, Which, Path}}`。因此 `broker_cluster.app.src` 的
   `applications` 需加 **`public_key`**（OTP 自带应用，非新依赖；不加则 dialyzer/xref
   会把这次调用算作对未声明应用的调用）。
4. **实际选项与 env 一致**：从 `ssl_dist_opts` ETS 取 server/client 两行，比对
   `certfile`/`keyfile`/`cacertfile`/`verify`/`fail_if_no_peer_cert`/`versions`
   （路径按 `filename:absname/1` 归一），不一致 → `{error, {dist_tls_optfile_drift, Field, Declared, Actual}}`。
5. 其余 → `ok`。

**跨节点握手层错误不归 app 处理**：对端证书不被受信 CA 签发时，TLS 握手在分布层失败
（`Fatal - Unknown CA`），`net_kernel` 报连接错、该节点进不了集群，app 层无需也无法补救。
由负向测试断言其效果。

## 5. vm.args / release 接线

- `config/vm.args.src` **不变**（默认关闭）。
- 开启时运维只需两行，且**由 `vm_args_lines/0` 生成**：
  ```
  -proto_dist inet_tls
  -ssl_dist_optfile /abs/path/dist_tls.optfile
  ```
  optfile 内容由 `render_optfile/0` 生成（示例见 §5.1）。
- **不要**手写 `-ssl_dist_opt server certfile ...`（§1.1 第 1 条：连出时会崩）。

### 5.1 optfile 形状

```erlang
%% generated by broker_cluster_dist_tls:render_optfile/0
[{server, [{certfile,             "/abs/node.crt"},
           {keyfile,              "/abs/node.key"},
           {cacertfile,           "/abs/ca.crt"},
           {verify,               verify_peer},
           {fail_if_no_peer_cert, true},
           {versions,             ['tlsv1.2','tlsv1.3']}]},
 {client, [{certfile,   "/abs/node.crt"},
           {keyfile,    "/abs/node.key"},
           {cacertfile, "/abs/ca.crt"},
           {verify,     verify_peer},
           {versions,   ['tlsv1.2','tlsv1.3']}]}].
```

（mTLS 要求 client 侧也出示证书与 CA。`erl_dist => true` 由 `inet_tls_dist` 自己加，不必写。）

## 6. 测试

### 6.1 新套件 `broker_cluster_dist_tls_SUITE`

**测试拓扑受一条实测约束支配**：CT 宿主节点是 rebar3 ct 启动的，跑的是**明文**
`inet_tcp`；`-proto_dist` 只能在 boot 期设定，**套件无法把 CT 宿主改成 TLS**。而 TLS peer
与明文宿主之间**不能**用 `rpc:call`（实测 `{badrpc, nodedown}`，宿主侧还会打一条
`Fatal - Unexpected Message / {unsupported_record_type,0}` —— 这是**预期噪音**，不是失败）。
因此：

- peer 一律 `peer:start_link(#{connection => standard_io, args => [...]})`，
  宿主只用 **`peer:call/3`**（走 stdio，不经分布），**不用** `rpc:call`；
- 所有跨节点断言都在 **peer 内部**执行：`peer:call(A, rpc, call, [B, M, F, A])`
  （实测可行，TLS 握手发生在 A 与 B 之间）；
- 既有 `broker_cluster_dist_SUITE` 的 `rpc(Node, ...)` 写法**不能照抄**——它的 peer 与宿主
  同协议才成立。这是本套件最主要的写法差异，实现时必须显式处理。

epmd 仍需运行。**证书在 suite 内用 `openssl` 现生成**（与 `broker_security_tls_SUITE`
同一先例；仓库不提交任何 PEM）；需生成：CA、2 张受信节点证书（SAN 含 `IP:127.0.0.1`，
虽然默认不校验，但保留以免将来加校验时证书全废）、1 张**另一 CA**签发的恶意证书。

- **happy path**：2 peer 组 2 节点 ra 集群（`peer:call` 驱动），断言：组形成、
  `broker_cluster:assign` 提交、两节点缓存一致（即把 S12-D 的跨节点 happy path 跑在
  TLS 分布上）。
- **负向（跨 CA）**：第 3 peer 用另一 CA 的证书 → 被拒，集群保持 2，从集群侧对其
  `rpc:call` 失败（`{badrpc, nodedown}`）。
- **负向（fail-closed）**：两个 `inet_tls` 节点都不带证书 → 互 `pang`（不退化成明文）。
- **单测 `broker_cluster_dist_tls`**（无分布）：`validate/0` 的 disabled 通过、缺文件、
  坏 PEM、`versions` 空、**env enabled 但 boot 明文**、**用了内联 `-ssl_dist_opt`**、
  **选项漂移**；`render_optfile/0` 形状（server+client 两行、路径绝对化）；
  `vm_args_lines/0` 两行；`redacted_config/0` 不含 key 字节。
  （`effective/0` 读的是进程级全局状态，单测通过 `validate/1` 注入「未开 TLS 的 boot 状态」
  等场景来覆盖，不真的改节点的 `init` 参数。）

### 6.2 回归

默认关闭时全量 `rebar3 ct` **必须零回归**（1472/1472 不变）——这是「默认逐字节相同」
的守门。新增套件后基线相应 +N。

## 7. 文档与 ADR

- **新 ADR-035「集群跨节点 / 复制通道 TLS」**：决策 OTP inet_tls + mTLS + **optfile 载体**
  + 默认关闭 + 双向漂移校验；**把「boot 期约束 ⇒ 非运行时可切换」与「`-ssl_dist_opt`
  内联写法不可用（§1.1）」写进裁决理由**；记录保留 cookie 的理由；记录 **CA 即闸门、
  SAN 默认不校验**这一诚实边界；冻结正文不回填历史（照既有 ADR 追加式）。
- 更新：`operations.md`（开启步骤 / optfile / 证书供给 / 轮换）、
  `architecture.md`（env 表 + `broker_cluster` 行）、`security.md`（§12 第 1 条标已实现 +
  §12.7 更新「分布现在可 TLS」，并写明 SAN 不校验、cookie 仍在）、`roadmap`、`progress.md`。
- `protocol-compatibility.md` 无需改：分布在所有协议之下，无新线上帧。

## 8. 范围边界（明确不做）

- 不做运行时开关（OTP 约束）。
- 不做「只加密 ra」——全分布。
- 不改 `broker_security.tls`（面向客户端）的语义。
- 不移除 cookie。
- 不加 `broker_metrics` 键、不加管理端口、不新增第三方依赖 / 应用（唯一新增的
  `applications` 项是 OTP 自带的 `public_key`，用于本地 PEM 校验）。
- 不默认做主机名/SAN 校验（OTP 默认行为）；需要 SAN 校验时的两个前置条件（`verify_fun`
  marker + 运行期 `net_kernel:allow/1`）只作为文档化的可选加固，不在本期实现。
- 不做证书自动签发 / ACME / 轮转编排（轮换仍走「换 PEM + 重启节点」，文档说明）。

## 9. 验收

1. 默认关闭：全量 `rebar3 ct` 零回归（1472/1472），`rebar3 release` 装配成功。
2. 开启（测试 CA）：2 节点 ra 集群在 TLS 分布上形成、写入提交、缓存一致。
3. 负向：非受信 CA 节点被拒，集群不纳入它；无证书的两个 TLS 节点互不可达。
   另外确认：明文宿主对 TLS peer 的 `rpc:call` 失败是**预期的**，套件不因此判失败，
   但也不能拿它当「TLS 生效」的证据（证据是 peer 之间的握手与集群形成）。
4. fail-loud：enabled 但缺证书 / env 与 boot 协议不符 / optfile 漂移 → `broker_cluster`
   拒绝启动，错误信息可定位。
5. `redacted_config/0` 与节点视图不漏 key。
6. dialyzer 0 warning；xref 仅既有 unused-export。
