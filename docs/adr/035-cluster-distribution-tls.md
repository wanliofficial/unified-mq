# ADR-035：集群跨节点 / 复制通道 TLS（OTP `inet_tls` + mTLS，默认关闭）

- **状态**：已接受（2026-11-20）
- **日期**：2026-11-20
- **关联**：`docs/security.md` §12 第 1 条（本 ADR 收口该未实现项）、§12.7（S12-D 扩大的分布信任面）、ADR-021（集群控制面 / Metadata Raft——**本 ADR 不修订其正文**，只改变底层传输的机密性与对端鉴权）、ADR-034（S12-D 跨节点投递——它把 distribution 变成数据面，本 ADR 给这条通道加上加密与证书准入）、ADR-016（`broker_metrics` 冻结键集——**不扩容**）、ADR-032（S12 边界——本 ADR 不改 S12 任何裁决）
- **设计文档**：[`docs/superpowers/specs/2026-11-20-cluster-distribution-tls-design.md`](../superpowers/specs/2026-11-20-cluster-distribution-tls-design.md)

## 背景

S12-D（ADR-034）之后，节点间流量同时承载 ra 元数据复制与跨节点 RPC（QoS0 广播、转发读/写），但这条通道只有 Erlang magic cookie 这一层弱鉴权，且完全明文。`docs/security.md` §12.7 已如实写明这是既有信任面被扩大，而跨节点 / 复制通道 TLS 是 §12 第 1 条明示未实现项。

## 决策

### D1. 用 OTP 原生 `-proto_dist inet_tls`，不自造分布模块

`-proto_dist` 在 **boot 期**选定分布协议，早于任何 application 加载。因此没有「应用层注入 TLS」的位置，自造 `-proto_dist` 模块去读 app env 也读不到（app 还没起）。OTP 自带 `inet_tls_dist`，用它。

### D2. mTLS：CA 信任就是成员准入闸门

`verify_peer` + `fail_if_no_peer_cert`，节点互验证书。只有持有受信 CA 签发证书的节点能建立分布会话——这比 cookie 强（cookie 是共享秘密，泄露即可冒充），且可与面向客户端证书的生命周期分开管理。

**诚实边界（2026-11-20 实测，双向不对称）**：

- **拨入（server）方向默认不校验对端 SAN。**一个持有受信 CA 签发证书、但 SAN 写错的节点**仍可拨进来**。要收紧需同时满足 `verify_fun` marker（`{fun inet_tls_dist:verify_client/3, []}`）与运行期 `net_kernel:allow/1` 非空；本期**不做**，只在本 ADR 与安全文档中写明。
- **拨出（client）方向默认校验对端 SAN**——`inet_tls_dist` 在 client 选项里恒注入 `{server_name_indication, Host}`，`ssl` 据此做标准 hostname 校验。所以**节点证书的 SAN 必须匹配对端拨它的地址**（`@IP` → `IP:` SAN；`@hostname` → `DNS:` SAN），否则握手 `{bad_cert,{hostname_check_failed,...}}` 失败。这是硬要求，运维文档必须写明。

即：CA 是双向准入闸门，SAN 绑定只在拨出方向生效（fail-closed），拨入方向不绑定客户端身份。

### D3. 独立配置块，默认关闭

`broker_cluster.dist_tls`，出厂 `enabled => false`。关闭时 `validate/0` 立即 `ok`、不读任何文件、不改任何分布行为——已有单节点与集群部署逐字节不变。证书与面向客户端的 `broker_security.tls` 分开：节点身份与客户端身份的生命周期、轮换、ACL 都不同。

### D4. 选项只能走 `-ssl_dist_optfile`，且 boot 期双向校验

两条实测事实决定了这一条：

1. 内联 `-ssl_dist_opt server certfile X` 的**两段式写法是不可用的**——`inet_tls_dist` 只认下划线拼接的键名，两段式在连出时 `function_clause` 崩溃。
2. 即使写成正确的下划线形式，内联写法**不创建 `ssl_dist_opts` ETS 表**，实际生效的选项无从回读。

因此强制 optfile，并在 `broker_cluster` 启动前做**双向**校验：声明为开但实际 boot 明文 → 拒启；实际 boot 了 TLS 但声明为关 → 也拒启（否则管理面与审计说的是假话）。这是本 ADR 敢默认关闭的前提——漂移不会静默。

### D5. 保留 cookie

mTLS 之上的第二因子。OTP 默认仍检查 cookie，去掉它需要额外配置且没有收益。

### D6. 不是运行时开关

证书轮换 = 换 PEM + 重启节点。不做热加载、不做 ACME、不做轮转编排：`-proto_dist` 的 boot 期约束使「运行时开 TLS」不存在，而证书轮换的热加载需要替换 `ssl_dist_sup` 的选项表，属于另一件事。

## 后果

- **正面**：§12 第 1 条关闭；跨节点流量（含 ra 复制）可选加密 + 证书准入；默认路径零变化。
- **负面 / 边界**：
  - 开启后**不能**灰度混跑：一个 TLS 节点与一个明文节点无法建立分布会话，所以切换是全组重启，窗口内跨节点调用失败。文档必须写明。
  - 拨入方向不校验客户端 SAN（见 D2）；拨出方向按 SAN 绑定对端身份，所以证书 SAN 必须匹配对端拨入地址。
  - 证书轮换需要重启。
  - 运维多管一个 optfile（由模块渲染，不必手抄）。

## 落地验证记录

见 `docs/superpowers/plans/2026-11-20-cluster-distribution-tls.md` 的 Task 6~9 与全量 CT 记录。

### 定稿说明（2026-11-20）：OTP 29.1.2 复验

`main` 在合并本 ADR 的实现后随即升到 **OTP 29.1.2**（`ca04f24`、`d26bb00`）。原本的 29 例只在 OTP 28.3.1 上跑过，因此在新版本上重跑了一遍，**裁决与边界结论一字未改**：

- **结果**：`broker_cluster_dist_tls_SUITE` **29/29 全绿**，0 failed、0 skipped（`=== TEST COMPLETE, 29 ok, 0 failed of 29 test cases`）。
- **运行时证据**（取自 CT 的 `suite.log` 头部，不是推断）：`=otp_release 29`、`=emulator_vsn 17.1.1`；树上 `broker_cluster_dist_tls.beam` 的 `compile_info` 编译器版本 `10.0.7`（OTP 29 的编译器）。
- **真实分布式用例确实跑了**（耗时说明没有被短路）：`a_two_node_ra_group_forms_over_tls_distribution` 0.200 s、`a_write_commits_and_the_remote_node_sees_the_owner` 0.232 s；两个负向用例 `a_node_signed_by_another_ca_cannot_join` 1.825 s、`tls_nodes_without_certificates_cannot_reach_each_other` 1.412 s（等握手失败的自然超时）。
- **因此 D2 的两条实测边界在 29 上原样成立**：`-proto_dist inet_tls` 仍在 boot 期生效、`ssl_dist_opts` 仍可回读、mTLS 双向验证、跨 CA 拒绝、无证书 fail-closed 全部一致；**拨出方向按 SAN 绑定对端、拨入方向不校验客户端 SAN** 的不对称也没有回退。
- **未复验的部分**：本 ADR 的验证只覆盖分布通道本身。OTP 29 迁移的其余影响（`catch Expr` 退役等）由 `ca04f24`/`d26bb00` 单独承担，不在本 ADR 的记录范围内。
