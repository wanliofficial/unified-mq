# S11-C 设计 spike：DTLS（PSK-only）与 `broker_security` 身份表接线

- **对应**：ADR-031 §D5 / OQ-6（DTLS 凭据与统一身份表的绑定方式）、`docs/design/s11-coap.md` R-T2 / F-5、`docs/roadmap.md` §13.1 S11-C
- **执行日期**：2026-11-16
- **环境**：OTP 28 / ERTS 16.2（`/home/wanli/Development/otp-28.3.1`）、`ssl` 11.5.1、DTLS 1.2
- **性质**：**spike，不是产品代码**。本文件记录 DTLS+PSK 在 OTP 28.3.1 上的**实测事实**，用来把 ADR-031 OQ-6 的开放问题收敛为一个可实现的形状，再据此写设计稿与冻结计划。**本轮不新增 `apps/` 下的任何代码。**
- **范围决策（本会话已确认）**：DTLS 凭据模式 = **仅 PSK**（不引证书/mTLS）；libcoap 真实客户端证据 **推迟、单列**（不纳入 S11-C）；容量/压测报告 **纳入 S11-C**。

---

## 1. 结论先行

| 项 | 事实 |
|---|---|
| DTLS 可用性 | ✅ `ssl:versions()` → `supported_dtls:['dtlsv1.2']`、`available_dtls:['dtlsv1.2',dtlsv1]` |
| 选 DTLS 传输 | `{protocol, dtls}`（**不是** `{dtls, true}`——后者 transport 仍是 `inet_tcp`，`ssl:listen` 直接 `badarg`） |
| 启动前置 | 必须先 `application:ensure_all_started(ssl)`，否则 `ssl:listen` 报 `{noproc,{gen_server,call,[dtls_listener_sup,...]}}` |
| 服务端 PSK 回调 | ✅ `{user_lookup_fun, {Fun, State}}`，`Fun(psk, PSKIdentity, State) -> {ok, Key} \| #alert{} \| _` |
| 客户端 PSK | `{psk_identity, String}`（是 **list（string）**，不是 binary）+ `{user_lookup_fun, ...}` 供密钥；**没有** `{psk_key, ...}` 这个选项 |
| PSK 身份抵达服务端 | ✅ 客户端的 identity **逐字**到达服务端 lookup（`<<"device-01">>`）——这正是映射到 `broker_security` principal 的钩子 |
| 无证书可行 | ✅ 用 **cert-free PSK 套件**（`psk`/`dhe_psk`/`ecdhe_psk` 的 AEAD），服务端**不配 certfile/keyfile** 也能完成握手 → ADR-031 D5「默认不自签」可满足 |
| 单进程多 DTLS socket | ✅ 一个进程可同时持有多个 active DTLS socket，收 `{ssl, Sock, Data}`，按 `Sock -> {Peer, Identity}` 映射 demux |
| 未知身份拒绝 | ⚠️ **不要用 `#alert{}`**（实现期实测修正）。返回 alert → 客户端 ~20–200 ms 拿到 `{tls_alert,{handshake_failure,_}}`；而「用户存在但口令错」走的是**后期 Finished 校验失败** → 客户端表现为 **3 s 超时**。两者时延/错误形态截然不同 ⇒ **用户名枚举 oracle**。正确做法：未知身份返回**诱饵密钥**（`SHA-256("...-decoy" ‖ 0 ‖ 每 listener 随机 secret ‖ 0 ‖ Identity)`），让两种情况都死在同一个 Finished 检查上，对外不可区分。 |
| 身份竞态 | ⚠️ 身份只能在 lookup fun 里捕获；`ssl:connection_information` **不暴露** `psk_identity`。**并发握手时**必须在成功与失败**两条路径都 drain**，否则被拒连接的 identity 会串给下一连接 |
| 二进制模式 | ⚠️ `ssl:listen` / `ssl:connect` **必须显式 `{mode, binary}`**，否则 `{ssl,Sock,Data}` 与 `ssl:recv` 给的是 **iolist/整数列表**，`coap_codec` 的 `byte_size/1` 直接 `badarg` 崩溃 |
| `port/1` 语义 | ⚠️ `ssl:sockname/1` 返回 `{ok, {Addr, Port}}`，而 `gen_udp` 路径用 `inet:port/1` 返回裸 port。DTLS 分支必须解开元组，否则 `port/1` 把端口暴露成元组 |
| `{active, N}` 预算 | ✅ `{ssl_passive, Sock}` **会**发出（spike12：收 5 条后到达）→ UDP 的重挂模式原样可用 |
| socket 所有权 | ⚠️ **`ssl:setopts({active,_})` 由非 owner 调用返回 `ok` 却把数据投给当时的 owner**（spike11）。握手期必须 `{active,false}`，**只有移交后的 listener** 才设 active |

**净结论**：OTP 28.3.1 的 DTLS+PSK **完全可用**，且能做到「无证书、身份即 principal」。S11-C 的 DTLS 接线是**工程落地**问题，不是可行性问题。

---

## 2. 已证实的关键链路（最小证据）

### 2.1 握手 + 身份 + 解密（无证书）

`/tmp/opencode/s11c_final/s11c_spike.erl`（spike v7 定稿）实测输出：

```
OTP 28
cert-free PSK AEAD suites: [{dhe_psk,aes_256_gcm}, {psk,aes_256_gcm}, ...]   % 17 套
  [srv] listen ok on 34455 (NO certfile)
  [srv] lookup identity=<<"device-01">>          % 客户端 identity 逐字抵达
  [srv] handshake OK peer={ok,{{127,0,0,1},57197}}
  [cli] connected, sending
  [cli] send ok
  [srv] decrypted "hello dtls psk"               % 解密后的明文收到
  [cli] srv saw identity=<<"device-01">>
```

这证明四件事同时成立：**无证书握手、PSK 身份抵达服务端、身份可回传给上层、解密后明文可达**。

### 2.2 单进程持有多个 DTLS socket（架构问题）

`/tmp/opencode/s11c_spike8.erl`（spike v8）把「acceptor 握手后 `controlling_process` 移交给 listener」与「listener 同时持有多个 active DTLS socket」都跑通：

```
listener: DTLS listen on 60481
listener: peer ready peer={{127,0,0,1},55265} identity=<<"device-01">>
listener: decrypted from {{127,0,0,1},55265} (identity <<"device-01">>): "hello from 01"
listener: peer ready peer={{127,0,0,1},33720} identity=<<"device-02">>
listener: decrypted from {{127,0,0,1},33720} (identity <<"device-02">>): "hello from 02"
bad_client: refused as expected (timeout)          % 未知身份被拒
```

关键：listener 是一个**普通进程**，`ssl:setopts(S, [{active, true}])` 后收 `{ssl, S, Bin}`，用 `maps:get(S, Peers)` 找回 `{Peer, Identity}`。这与 S11-A/B 的「单进程持有全部有界状态」模型**同构**——DTLS 的连接导向不需要「每 peer 一进程」的会话层。

### 2.3 未知身份干净拒绝

`/tmp/opencode/s11c_spike10.erl`（spike v10）实测：lookup 对未知 identity 返回 `#alert{}`，客户端 **224 ms** 内得到：

```
client with UNKNOWN identity: {error, {tls_alert, {handshake_failure, "..."}}} (after 224 ms)
```

> 返回的 alert 是 `handshake_failure` 而非 `unknown_psk_identity`（OTP 内部把 PSK lookup 的 alert 统一成 handshake_failure）；对客户端而言是**可动作的致命告警**，符合预期。

### 2.4 身份竞态（必须记住的坑）

spike8 **首版**在并发的「好客户端 + 坏客户端」下把 `device-99`（坏）的 identity 串给了 `device-02`（好）。原因：`user_lookup_fun` 在 `ssl:handshake` 内部同步执行，把 `{psk_identity, Id}` 投递到 acceptor 邮箱；**被拒连接的握手失败后，它的 identity 消息还留在邮箱**，下一次 `drain_identity` 就取到了错误的。修正：**失败路径也要 drain**（`{error,_}` / `{'EXIT',_}` 分支各 drain 一次）。修正后四连接身份全部正确。

**设计含义**：身份捕获是「每个 acceptor 顺序化握手 + 同步 drain」的**副作用**，不能跨握手共享邮箱、不能并发握手。这是设计稿里必须钉死的约束（见 §4）。

### 2.5 活跃模式与所有权（**最容易写错的一处**）

spike12 / spike11 实测两条：

1. ✅ **`{ssl_passive, Sock}` 会发出**：owner 设 `{active, 5}`，收 5 条 `{ssl, Sock, Bin}` 后收到 `{ssl_passive, Sock}`，重挂即继续。**UDP 的 `{udp_passive, ...}` 重挂模式可以原样复用**，不需要退回 `{active, once}`。
   ```
   owner got socket, re-arming active,5
     PASSIVE after 5 datagrams -> re-arming
     PASSIVE after 10 datagrams -> re-arming
     PASSIVE after 15 datagrams -> re-arming
     PASSIVE after 20 datagrams -> re-arming
   RESULT: datagrams=20 passives=4
   ```
2. ❌ **`ssl:setopts(S, [{active,N}])` 由非 owner 调用返回 `ok`，但数据被投递给当时的 owner**。spike11 在 acceptor 里先 `setopts({active,5})` 再 `controlling_process`，结果前 5 条 datagram 进了 acceptor 的邮箱（ acceptor 随后退出，数据静默丢失），owner 只看到后 3 条。**这不是报错，是静默错投**。

**因此设计钉死**：握手期间 socket 保持 `{active, false}`；`controlling_process` 移交**之后**，**只有 listener（新 owner）**设 `{active, ?ACTIVE_N}`。acceptor 除了 `transport_accept` / `handshake` / `peername` / `controlling_process` / `close` 之外**不碰**这个 socket。


---

## 3. OTP 28.3.1 `ssl` DTLS API 权威点（实现时对照）

| 主题 | 结论 | 来源 |
|---|---|---|
| 传输选择 | `{protocol, dtls}`；`{dtls, true}` 无效 | `ssl_config.erl:614` `opt_version` 读 `protocol`，默认 `tls` |
| DTLS 的 socket 回调 | `default_cb_info(dtls) = {gen_udp, udp, udp_closed, udp_error, udp_passive}` | `ssl_config.erl:2121` |
| 服务端 PSK 回调 | `{user_lookup_fun, {Fun, State}}`，`Fun/3` | `ssl_config.erl:1418-1427` |
| PSK 回调签名 | `Fun(psk, PSKIdentity, State)`；`psk_secret/2` 返回 `{ok, <<Key>>}` 才继续 | `ssl_handshake.erl:3471/3494` |
| 客户端 PSK 身份 | `{psk_identity, PskID::string()}`（list） | `ssl.erl:1652` |
| 客户端 PSK 密钥 | 也用 `user_lookup_fun`（**无 `psk_key` 选项**） | `ssl.erl:1652-1655` |
| cert-free PSK 套件 | 仅在 `ssl:cipher_suites(anonymous, 'dtlsv1.2')`；`all` 只给 `rsa_psk`（需证书） | 实测 spike6/7 |
| 取对端 | 服务端 `ssl:peername(Socket)`（`{ok,{IP,Port}}`）；对 sslsocket 用 `inet:peername` 会 `function_clause` | 实测 spike4 |
| 取连接信息 | `ssl:connection_information(S, [selected_cipher_suite, protocol])` 可用；**`psk_identity` 不在可查询键里**（返回 `{ok,[]}`） | 实测 spike9 |
| 启动前置 | `application:ensure_all_started(ssl)`；`ssl_sup → ssl_connection_sup → dtls_sup → dtls_server_sup → dtls_listener_sup` | `ssl_sup.erl` |
| 版本 atom | `'dtlsv1.2'`（atom，带点必须单引号）；`ssl:cipher_suites/2` 的 Version 参数是 atom 不是字符串 | `ssl.erl:2883-2891` |

---

## 4. 对 OQ-6 的回答（可落地的形状）

> **DTLS 客户端身份 = PSK identity，逐字解析为 `broker_security` 的 `users` 表用户名；PSK 密钥由该用户记录的 `password` 派生（域分隔 KDF），不新建凭据存储、不引证书、默认不自签。**

- **身份映射**：`Fun(psk, Identity, State)` 拿到的 `Identity`（binary）**就是** `broker_security` `users` 表里的用户名。命中 → `principal = Identity`、`tenant = broker_security:tenant_id(Identity)`；未命中 → 返回 alert 拒绝握手。
- **密钥派生**：不把明文 password 当 PSK 直接用，而是 `crypto:hash(sha256, [<<"unifiedmq-coaps-psk">>, 0, Identity, 0, Realm, 0, Password])`——与 TURN 的 `broker_security:long_term_key/2`（`MD5(username ":" realm ":" password)`）**同一先例**：口令不过线，服务端自己派生。派生**新增**一个 `broker_security` 导出（不动 `authenticate/2`/`authorize/2` 语义）。Realm 取部署配置（`broker_security.tls.realm`，缺省 `<<"unifiedmq">>`）。
- **principal 传递**：acceptor 在握手成功后把 `{ssl,Sock, Peer, Principal}` 交给 listener；listener 维护 `Sock -> #{peer, principal, tenant}`，`run_resource` 的 `Ctx` 用真实 `principal`/`tenant` 而不是 `undefined`/`default_tenant()`。**这是 S11-B 留下的「ACL 无法拒绝 datagram」被真正接上的点。**
- **`credential_capable/0`**：加 `<<"coaps">>`（DTLS 握手能携带凭据）。**不改** `coap`（明文 UDP 仍无凭据），所以 `require_auth` 列 `coap` 仍拒启动、列 `coaps` 才可强制。`broker_proto_coap_app` 的 `auth_unsupported/0` 从「`coap` **或** `coaps` 都拒」改为「`coap` 拒、`coaps` 允许」。

**并发握手约束（设计稿必须钉死）**：每个 DTLS listener 一个 **acceptor 进程**串行 `transport_accept → handshake → controlling_process → 移交`，lookup fun 的 identity 在**成功与失败两条路径都 drain**。listener 只收已建立的 socket，不碰握手。

---

## 5. spike 命令留痕（可复跑）

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"

# 2.1 无证书握手 + 身份 + 解密
erlc -o /tmp/opencode/s11c_final /tmp/opencode/s11c_final/s11c_spike.erl
erl -noshell -pa /tmp/opencode/s11c_final -run s11c_spike main

# 2.2 单进程多 DTLS socket + 未知身份拒绝
erlc -o /tmp/opencode/s11c_out8 /tmp/opencode/s11c_out8/s11c_spike8.erl
erl -noshell -pa /tmp/opencode/s11c_out8 -run s11c_spike8 main

# 2.3 未知身份的干净拒绝（需 -I 到 ssl 源码 include 取 ssl_alert.hrl）
erlc -I /home/wanli/Development/otp-28.3.1/lib/erlang/lib/ssl-11.5.1 \
     -o /tmp/opencode/s11c_out10 /tmp/opencode/s11c_out10/s11c_spike10.erl
erl -noshell -pa /tmp/opencode/s11c_out10 -run s11c_spike10 main
```

> 探针全部是 throwaway，**不进仓库**。仓库只新增本报告 + 设计稿 + 冻结计划 + 实现 + 测试。

## 6. 已知环境坑（复跑时）

1. shell 单引号 `-eval` 会被 atom 里的点（`tlsv1.2`）与引号破坏——用 `.erl` 文件 + `erlc` + `erl -run`。
2. atom 带点必须单引号：`'dtlsv1.2'`；`ssl:cipher_suites/2` 的 Version 是 atom。
3. `ssl:versions(all)` 在 OTP 28 不存在；`inet:versions/0` 不存在。
4. `ssl:listen` 无证书**可以**，只要 ciphers 全是 cert-free PSK；混入 `rsa_psk` 会走 `get_cert_params` 崩。
5. 客户端 DTLS 默认 `verify_peer`，无 `cacerts` 会 `{options,incompatible,...}`——PSK 下需 `{verify, verify_none}`。
6. sslsocket 的对端用 `ssl:peername/1`，不是 `inet:peername/1`。
7. `ssl:setopts(S, [{active,N}])` 由**非 owner** 调用会返回 `ok` 但把数据投给当时的 owner——握手期必须保持 `{active,false}`，只用移交后的新 owner 设 active。

---

## 7. 对 S11-C 设计稿的输入

1. **DTLS 传输内建在 `coap_listener`**（`{transport, dtls}` 的 listener spec）：同进程持有 UDP socket 或 DTLS acceptor + 多个 DTLS socket，不改 S11-A/B 的 `{IP,Port}` 键与五张有界表。
2. **身份 → principal → ACL**：`run_resource` 的 `Ctx` 带真实 principal/tenant；S11-B 降级的 `observe_plain_udp_principal_is_allowed` 在 DTLS listener 上新增**基于 ACL 的 4.03** 专项用例（DTLS `principal` 不再是 `undefined`）。
3. **PSK 派生新增 `broker_security:psk_for_identity/1`**（或等价导出），复用 `users` 表与 `long_term_key` 同款先例，不改既有语义。
4. **限流加固**：DTLS 下 `{IP,Port}` 来自已认证握手（源地址不可伪造），令牌桶仍按端点；补充「握手级」上限（并发未完成握手数 / 每身份连接数）以防握手洪泛。
5. **容量报告**：DTLS listener 的吞吐 / 延迟 / 送达，规格对齐 S10-D D.6。
6. **能力矩阵 + 文档 + ADR-031 定稿**：§4.24 追加 DTLS 行、`docs/security.md` §12.5 更新、ADR-031「落地偏差记录」追加 S11-C 定稿。
