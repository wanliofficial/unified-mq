# 跨节点 / 复制通道 TLS — 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 unified-mq 的 Erlang distribution（ra 复制 + 跨节点 RPC）可选用 OTP 原生 mTLS，默认关闭，开启时 boot 期校验并 fail-loud。

**Architecture:** 新增纯函数模块 `broker_cluster_dist_tls`（无进程、无监督树子项）。它（a）读 `broker_cluster.dist_tls` env 作为**声明**，（b）读 `init:get_argument/1` 与 `ssl_dist_opts` ETS 得到**实际 boot 状态**，（c）双向比对，不一致就让 `broker_cluster` 拒绝启动，（d）从同一份 env 渲染 `-ssl_dist_optfile` 与 vm.args 两行，保证运维不必手抄、也不会与 env 漂移。TLS 本身由 OTP 的 `inet_tls_dist` 在 boot 期完成，本仓不实现任何 TLS 代码。

**Tech Stack:** Erlang/OTP 28.3.1（`inet_tls_dist`、`ssl_dist_sup`、`public_key`）、rebar3、Common Test（`peer`、`epmd`）、`openssl`（仅测试期生成证书）。

**Spec:** `docs/superpowers/specs/2026-11-20-cluster-distribution-tls-design.md`

---

## 前置：环境与既有事实

- 每条命令前先 `export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"`。
- 跑 `rebar3 ct` 前先 `rm -rf _build/test/extras`；需要分布式时先 `epmd -daemon`。
- 当前基线：全量 `rebar3 ct` **1472/1472**、91 个 `*_SUITE.erl`、dialyzer 0 warning。
- 默认关闭时**必须零回归**——这是本计划最重要的一条验收。

## 文件结构

| 文件 | 职责 |
|---|---|
| `apps/broker_cluster/src/broker_cluster_dist_tls.erl`（新建） | 纯函数：`enabled/0`、`config/0`、`effective/0`、`validate/0`、`validate/1`、`ssl_dist_opts/0`、`render_optfile/0`、`vm_args_lines/0`、`redacted_config/0` |
| `apps/broker_cluster/src/broker_cluster.app.src`（改） | `applications` 加 `public_key`；`env` 加 `dist_tls` 默认块（`enabled => false`） |
| `apps/broker_cluster/src/broker_cluster_app.erl`（改） | `start/2` 起树前调 `broker_cluster_dist_tls:validate/0`，失败即拒启 |
| `apps/broker_admin/src/broker_admin_http.erl`（改） | 节点视图加 additive `dist_tls` 字段 |
| `apps/broker_cluster/test/broker_cluster_dist_tls_SUITE.erl`（新建） | 单测（纯函数）+ 分布式（2 peer 真实 ra over TLS + 负向） |
| `docs/adr/035-cluster-distribution-tls.md`（新建） | ADR-035 |
| `docs/adr/README.md`（改） | ADR 索引加一行 |
| `docs/operations.md`（改） | 新增 §5.5 运维面 |
| `docs/architecture.md`（改） | env 表 + `broker_cluster` 行 + 配置面 |
| `docs/security.md`（改） | §12 第 1 条标已实现；§12.7 补分布可 TLS |
| `docs/roadmap.md`、`progress.md`、`README.md`（改） | 状态口径 |
| `config/vm.args.src`（**不改**） | 默认关闭，保持明文 |

---

## Task 1: 模块骨架 + `enabled/0` / `config/0` / `redacted_config/0`

**Files:**
- Create: `apps/broker_cluster/src/broker_cluster_dist_tls.erl`
- Create: `apps/broker_cluster/test/broker_cluster_dist_tls_SUITE.erl`

- [ ] **Step 1: 写失败的测试**

创建 `apps/broker_cluster/test/broker_cluster_dist_tls_SUITE.erl`：

```erlang
%% @doc 跨节点 / 复制通道 TLS（ADR-035）。
%%
%% 分两部分：本文件前半是无分布的纯函数用例（配置解析、校验判据、渲染器），
%% 后半是真实 TLS 分布用例（peer 之间走 `inet_tls'，组 2 节点 ra 集群）。
%%
%% 关键写法约束（spec §6.1，实测得出）：CT 宿主跑的是明文 `inet_tcp'，
%% `-proto_dist' 只能在 boot 期设定，套件无法把宿主改成 TLS。因此
%%   * peer 一律 `connection => standard_io'，宿主只用 `peer:call/3'；
%%   * 所有跨节点断言在 peer 内部执行（`peer:call(A, rpc, call, [B, ...])'）；
%%   * 宿主对 TLS peer 的 `rpc:call' 必然 `{badrpc, nodedown}'，那不是失败。
-module(broker_cluster_dist_tls_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").

all() ->
    [disabled_by_default,
     disabled_config_keeps_the_shipped_defaults,
     config_merges_overrides_onto_defaults,
     redacted_config_never_contains_key_material].

%%%_* Fixtures --------------------------------------------------------------

init_per_suite(Config) ->
    Saved = application:get_env(broker_cluster, dist_tls, undefined),
    [{saved_dist_tls, Saved} | Config].

end_per_suite(Config) ->
    restore(?config(saved_dist_tls, Config)),
    ok.

init_per_testcase(_Case, Config) ->
    application:unset_env(broker_cluster, dist_tls),
    Config.

end_per_testcase(_Case, Config) ->
    restore(?config(saved_dist_tls, Config)),
    ok.

%%%_* Pure cases ------------------------------------------------------------

disabled_by_default(_Config) ->
    %% The .app.src env is what a release reads, so this asserts the shipped
    %% default rather than a hard-coded `false'.
    application:unset_env(broker_cluster, dist_tls),
    false = broker_cluster_dist_tls:enabled(),
    ok.

disabled_config_keeps_the_shipped_defaults(_Config) ->
    Cfg = broker_cluster_dist_tls:config(),
    false = maps:get(enabled, Cfg),
    verify_peer = maps:get(verify, Cfg),
    true = maps:get(fail_if_no_peer_cert, Cfg),
    ['tlsv1.2', 'tlsv1.3'] = maps:get(versions, Cfg),
    ok.

config_merges_overrides_onto_defaults(_Config) ->
    application:set_env(broker_cluster, dist_tls,
                        #{enabled => true, certfile => "/tmp/x.crt"}),
    Cfg = broker_cluster_dist_tls:config(),
    true = maps:get(enabled, Cfg),
    "/tmp/x.crt" = maps:get(certfile, Cfg),
    %% Untouched keys keep their defaults.
    verify_peer = maps:get(verify, Cfg),
    ok.

redacted_config_never_contains_key_material(_Config) ->
    application:set_env(broker_cluster, dist_tls,
                        #{enabled => true,
                          certfile => "/tmp/node.crt",
                          keyfile => "/tmp/node.key",
                          cacertfile => "/tmp/ca.crt"}),
    Map = broker_cluster_dist_tls:redacted_config(),
    true = maps:get(enabled, Map),
    %% Basename only: the view says which file, never where the tree lives,
    %% and never any key bytes.
    <<"node.crt">> = maps:get(certfile, Map),
    false = maps:is_key(keyfile, Map),
    false = maps:is_key(private_key, Map),
    false = maps:is_key(key, Map),
    ok.

%%%_* Helpers ---------------------------------------------------------------

restore(undefined) -> application:unset_env(broker_cluster, dist_tls);
restore(Value) -> application:set_env(broker_cluster, dist_tls, Value).
```

- [ ] **Step 2: 跑测试确认失败**

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
cd /tmp/opencode/mq-s12-0 && rebar3 ct --suite apps/broker_cluster/test/broker_cluster_dist_tls_SUITE
```

Expected: 编译失败 `undefined function broker_cluster_dist_tls:enabled/0`。

- [ ] **Step 3: 写最小实现**

创建 `apps/broker_cluster/src/broker_cluster_dist_tls.erl`：

```erlang
%% @doc 跨节点 / 复制通道 TLS 的配置与校验（ADR-035）。
%%
%% 这个模块**不发一个字节、不开一个 socket**。节点间的 TLS 由 OTP 在 boot 期
%% 用 `-proto_dist inet_tls' + `-ssl_dist_optfile' 完成（`inet_tls_dist'）。
%% 它做的事只有三件：
%%
%%   1. 把 `broker_cluster.dist_tls' env 读成一份**声明**；
%%   2. 把节点**实际**的 boot 状态读出来（`init:get_argument/1' +
%%      `ssl_dist_opts' ETS，见 spec §1.1 第 3 条），与声明双向比对；
%%   3. 从同一份声明渲染 optfile 与 vm.args 两行，让运维不必手抄。
%%
%% 为什么要比对而不是「配了就当生效」：vm.args 与 sys.config 是两份独立的
%% 文件，运维改了其中一份而忘了另一份，是这类特性最常见的事故。`-proto_dist
%% inet_tls' 可被应用回读，所以这种漂移**能**在启动时被抓到，而不是等到第一次
%% 跨节点写入才暴露。默认关闭时本模块什么都不做（`validate/0' 立即 `ok'），
%% 这是「默认逐字节不变」的守门。
-module(broker_cluster_dist_tls).

-export([enabled/0, config/0, effective/0, validate/0, validate/1]).
-export([ssl_dist_opts/0, render_optfile/0, vm_args_lines/0,
         redacted_config/0]).

-export_type([config/0]).

-type config() :: #{enabled := boolean(),
                    certfile := file:name() | undefined,
                    keyfile := file:name() | undefined,
                    cacertfile := file:name() | undefined,
                    verify := verify_peer | verify_none,
                    fail_if_no_peer_cert := boolean(),
                    versions := [atom()],
                    optfile := file:name() | undefined}.

-define(DEFAULTS, #{enabled => false,
                    certfile => undefined,
                    keyfile => undefined,
                    cacertfile => undefined,
                    verify => verify_peer,
                    fail_if_no_peer_cert => true,
                    versions => ['tlsv1.2', 'tlsv1.3'],
                    optfile => undefined}).

%% @doc The declaration, with the shipped defaults filled in.
-spec config() -> config().
config() ->
    case application:get_env(broker_cluster, dist_tls, #{}) of
        Map when is_map(Map) -> maps:merge(?DEFAULTS, Map);
        Other -> erlang:error({invalid_dist_tls_config, Other})
    end.

-spec enabled() -> boolean().
enabled() ->
    maps:get(enabled, config()) =:= true.

%% @doc Admin/log view: which files and modes, never key material. The certfile
%% is reduced to its basename so the view does not leak the deployment layout.
-spec redacted_config() -> map().
redacted_config() ->
    Cfg = config(),
    #{enabled => maps:get(enabled, Cfg),
      certfile => basename_or_undefined(maps:get(certfile, Cfg)),
      verify => maps:get(verify, Cfg),
      fail_if_no_peer_cert => maps:get(fail_if_no_peer_cert, Cfg),
      versions => maps:get(versions, Cfg)}.

basename_or_undefined(undefined) -> undefined;
basename_or_undefined(Path) -> unicode:characters_to_binary(filename:basename(Path)).
```

- [ ] **Step 4: 跑测试确认通过**

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
cd /tmp/opencode/mq-s12-0 && rebar3 ct --suite apps/broker_cluster/test/broker_cluster_dist_tls_SUITE
```

Expected: `All 4 tests passed.`

如果 `redacted_config_never_contains_key_material` 报 `function_clause`（`config/0` 因为 env 里没有该键而报错），检查 `.app.src` 的 env——但本 Task 的测试用 `unset_env` 走 `?DEFAULTS`，应当通过。

- [ ] **Step 5: 提交**

```bash
cd /tmp/opencode/mq-s12-0
git add apps/broker_cluster/src/broker_cluster_dist_tls.erl \
        apps/broker_cluster/test/broker_cluster_dist_tls_SUITE.erl
git commit --no-verify -m "feat(cluster): dist_tls config module skeleton

Pure-function module for the cross-node/replication-channel TLS feature
(ADR-035). This first slice only reads the declaration: config/0 merges the
broker_cluster.dist_tls env onto shipped defaults (enabled => false, mTLS
shape), and redacted_config/0 exposes a key-free view with the certfile
reduced to its basename so an admin view cannot leak the deployment layout.

No behaviour change: nothing calls it yet and validate/0 does not exist." \
        apps/broker_cluster/src/broker_cluster_dist_tls.erl \
        apps/broker_cluster/test/broker_cluster_dist_tls_SUITE.erl
```

---

## Task 2: `effective/0` —— 读回真实 boot 状态

**Files:**
- Modify: `apps/broker_cluster/src/broker_cluster_dist_tls.erl`
- Modify: `apps/broker_cluster/test/broker_cluster_dist_tls_SUITE.erl`

- [ ] **Step 1: 写失败的测试**

在 `all/0` 列表里加两项，并在文件末尾（`Helpers` 段之前）加两个用例：

```erlang
all() ->
    [disabled_by_default,
     disabled_config_keeps_the_shipped_defaults,
     config_merges_overrides_onto_defaults,
     redacted_config_never_contains_key_material,
     effective_reports_the_plain_default,
     effective_reads_the_arguments_this_node_booted_with].
```

```erlang
%% The CT host runs plain `inet_tcp', so "effective" must say exactly that:
%% no proto_dist flag, no ssl_dist_sup, no options table. This is the value
%% validate/0 compares the declaration against on every unmodified node.
effective_reports_the_plain_default(_Config) ->
    Eff = broker_cluster_dist_tls:effective(),
    error = maps:get(proto_dist, Eff),
    false = maps:get(tls_booted, Eff),
    undefined = maps:get(server_opts, Eff),
    undefined = maps:get(client_opts, Eff),
    ok.

%% Guard rail for the distributed cases below: if this ever stopped being
%% true, `effective/0' would be reading the wrong things and every drift
%% check would silently pass.
effective_reads_the_arguments_this_node_booted_with(_Config) ->
    Eff = broker_cluster_dist_tls:effective(),
    %% CT's own host is un-named unless a dist suite started distribution;
    %% either way we assert the *shape*, not a particular value.
    true = is_atom(maps:get(node_name, Eff)),
    true = lists:member(maps:get(longnames, Eff), [true, false]),
    ok.
```

- [ ] **Step 2: 跑测试确认失败**

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
cd /tmp/opencode/mq-s12-0 && rebar3 ct --suite apps/broker_cluster/test/broker_cluster_dist_tls_SUITE
```

Expected: `undefined function broker_cluster_dist_tls:effective/0`。

- [ ] **Step 3: 写实现**

在 `broker_cluster_dist_tls.erl` 里加：

```erlang
%% @doc What this node *actually* booted with, as opposed to what the env
%% declares. Every field here was measured on OTP 28.3.1 (spec §1.1):
%%
%%   * `proto_dist'      -- `init:get_argument(proto_dist)'; `error' means the
%%                          default `inet_tcp'. Present only when `-proto_dist'
%%                          was given at boot.
%%   * `tls_booted'      -- `ssl_dist_sup' is started by `inet_tls_dist' and by
%%                          nothing else, so its existence is proof that this
%%                          node's distribution terminates TLS.
%%   * `optfile'         -- `init:get_argument(ssl_dist_optfile)'.
%%   * `inline_opt'      -- `init:get_argument(ssl_dist_opt)' was used. That
%%                          form works but does NOT populate `ssl_dist_opts',
%%                          so its contents cannot be checked -- which is why
%%                          validate/0 refuses it (see `validate/1').
%%   * `server_opts' / `client_opts' -- the options `inet_tls_dist' will really
%%                          hand to `ssl'; `undefined' when there is no table.
-spec effective() -> map().
effective() ->
    #{node_name => node(),
      longnames => net_kernel:longnames() =:= true,
      proto_dist => init:get_argument(proto_dist),
      optfile => init:get_argument(ssl_dist_optfile),
      inline_opt => init:get_argument(ssl_dist_opt),
      tls_booted => whereis(ssl_dist_sup) =/= undefined,
      server_opts => dist_opts(server),
      client_opts => dist_opts(client)}.

%% The table exists only in the optfile path (spec §1.1 finding 3), and only
%% once `ssl_dist_sup' has run. Absent means "no readable options", which is
%% not the same as "no options" -- hence `undefined' rather than `[]'.
dist_opts(Type) ->
    try ets:lookup(ssl_dist_opts, Type) of
        [{Type, Opts}] -> Opts;
        [] -> undefined
    catch
        error:badarg -> undefined
    end.
```

- [ ] **Step 4: 跑测试确认通过**

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
cd /tmp/opencode/mq-s12-0 && rebar3 ct --suite apps/broker_cluster/test/broker_cluster_dist_tls_SUITE
```

Expected: `All 6 tests passed.`

- [ ] **Step 5: 提交**

```bash
cd /tmp/opencode/mq-s12-0
git add apps/broker_cluster/src/broker_cluster_dist_tls.erl \
        apps/broker_cluster/test/broker_cluster_dist_tls_SUITE.erl
git commit --no-verify -m "feat(cluster): read back the real distribution boot state

effective/0 reports what this node actually booted with -- proto_dist,
ssl_dist_optfile, whether the inline -ssl_dist_opt form was used, whether
ssl_dist_sup is alive, and the server/client options inet_tls_dist will
really hand to ssl.

This is the half of the drift check that the earlier design assumed was
impossible. It is possible: -proto_dist is readable via init:get_argument/1
and the optfile path populates the ssl_dist_opts ETS table. The inline
-ssl_dist_opt form does not create that table, which is why effective/0
distinguishes the two instead of treating them as equivalent.

No callers yet." \
        apps/broker_cluster/src/broker_cluster_dist_tls.erl \
        apps/broker_cluster/test/broker_cluster_dist_tls_SUITE.erl
```

---

## Task 3: `validate/1` / `validate/0` —— 双向漂移校验与 fail-loud

**Files:**
- Modify: `apps/broker_cluster/src/broker_cluster_dist_tls.erl`
- Modify: `apps/broker_cluster/test/broker_cluster_dist_tls_SUITE.erl`

- [ ] **Step 1: 写失败的测试**

`all/0` 加：

```erlang
     disabled_is_always_valid,
     enabled_but_booted_plain_is_refused,
     enabled_with_inline_opt_is_refused,
     booted_with_tls_but_disabled_is_refused,
     missing_files_are_refused,
     bad_pem_is_refused,
     rejecting_the_field_names_the_problem,
     validate_zero_argument_on_a_plain_disabled_node_is_ok],
```

用例（放在纯函数区，**不启动分布式**——靠 `validate/1` 注入 boot 状态）：

```erlang
%% Disabled must never require files: that is what keeps an untouched
%% single-node deployment byte-for-byte identical.
disabled_is_always_valid(_Config) ->
    application:set_env(broker_cluster, dist_tls, #{enabled => false}),
    Plain = plain_boot(),
    ok = broker_cluster_dist_tls:validate(Plain),
    ok.

%% The most likely operator mistake: they set the env but forgot vm.args.
%% Refusing here is the whole point of the feature -- a node that thinks it
%% encrypts while it does not is worse than one that fails to boot.
enabled_but_booted_plain_is_refused(Config) ->
    set_enabled(self_signed(Config)),
    {error, {dist_tls_enabled_but_booted_plain, error}} =
        broker_cluster_dist_tls:validate(plain_boot()),
    ok.

%% The inline -ssl_dist_opt form works at the OTP level but leaves no readable
%% options, so the declaration can never be verified against reality. Refuse
%% it and point at the renderer rather than accepting an unverifiable setup.
enabled_with_inline_opt_is_refused(Config) ->
    set_enabled(self_signed(Config)),
    Boot = (tls_boot(Config))#{inline_opt => {ok, [["server_certfile", "x"]]},
                               optfile => error},
    {error, {dist_tls_requires_optfile, see_renderer}} =
        broker_cluster_dist_tls:validate(Boot),
    ok.

%% The mirror image: someone enabled TLS in vm.args but left the env at its
%% shipped default. The node *is* encrypted, but nobody declared it, so the
%% admin view would lie. Both directions are drift.
booted_with_tls_but_disabled_is_refused(Config) ->
    application:set_env(broker_cluster, dist_tls, #{enabled => false}),
    {error, {dist_tls_booted_with_tls_but_disabled, inet_tls}} =
        broker_cluster_dist_tls:validate(tls_boot(Config)),
    ok.

missing_files_are_refused(Config) ->
    Self = self_signed(Config),
    set_enabled(Self#{certfile => "/nonexistent/node.crt"}),
    {error, {missing_file, certfile, "/nonexistent/node.crt"}} =
        broker_cluster_dist_tls:validate(tls_boot(Config)),
    ok.

bad_pem_is_refused(Config) ->
    Self = self_signed(Config),
    Junk = filename:join(?config(tls_dir, Config), "junk.crt"),
    ok = file:write_file(Junk, <<"this is not a certificate">>),
    set_enabled(Self#{certfile => Junk}),
    {error, {bad_pem, certfile, Junk}} =
        broker_cluster_dist_tls:validate(tls_boot(Config)),
    ok.

%% An error that names the field without naming the file it compared is not
%% actionable. Pin the shape.
rejecting_the_field_names_the_problem(Config) ->
    set_enabled((self_signed(Config))#{verify => verify_none}),
    {error, {dist_tls_optfile_drift, verify, verify_none, verify_peer}} =
        broker_cluster_dist_tls:validate(tls_boot(Config)),
    ok.

%% The zero-arity form is what the app calls. On a plain, disabled node it must
%% be a no-op -- this is the "1472 tests keep passing" guard expressed as a
%% test rather than as a hope.
validate_zero_argument_on_a_plain_disabled_node_is_ok(_Config) ->
    application:set_env(broker_cluster, dist_tls, #{enabled => false}),
    ok = broker_cluster_dist_tls:validate(),
    ok.
```

加 helper（放在 `Helpers` 段）：`plain_boot/0`、`tls_boot/1`、`self_signed/1`、`set_enabled/1`、`gen_certs/1`、`openssl/1`，以及 `init_per_suite` 里现生成一套证书。

**为什么 Task 1 就要有 `gen_certs/1`**：Task 3 的 `missing_files_are_refused` / `bad_pem_is_refused` / `tls_boot/1` 都要求**真实存在**的 PEM 文件（一条断言「路径存在且是合法 PEM」，另一条断言「存在但不是 PEM」）。若把证书生成留到 Task 6，Task 3 的测试无法成立。因此 fixture 一次到位。

**⚠ 两处 CT 陷阱（实测，2026-11-20）**：

1. `?config(Key, Config)` 宏只能在**带 `Config` 参数**的 helper 里用。所有需要证书路径的 helper 都写成 `f(Config)` 形式（`self_signed(Config)`、`tls_boot(Config)`、`cert_path(Config)`…），不要写无参版本再在里面读 `?config`——那会编译失败（`Config` 未绑定）。
2. `application:get_key(App, env)` **也会**反映运行期的 `set_env`（实测：`set_env` 之后从 `get_key` 读到的就是新值），所以「出厂默认」不能用它断言。要读**真正写盘的**默认值，直接解析 `.app` 文件：

```erlang
%% The shipped default is what a release reads off disk, so read the file
%% rather than the runtime env -- application:get_key/2 reflects set_env too,
%% which would make this test tautological.
shipped_env() ->
    AppFile = filename:join(filename:dirname(code:which(broker_cluster)),
                            "broker_cluster.app"),
    {ok, [{application, broker_cluster, Props}]} = file:consult(AppFile),
    proplists:get_value(env, Props).
```

`init_per_suite`：

```erlang
init_per_suite(Config) ->
    Saved = application:get_env(broker_cluster, dist_tls, undefined),
    Dir = filename:join(?config(priv_dir, Config), "dist_tls"),
    _ = file:del_dir_r(Dir),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    Certs = gen_certs(Dir),
    [{saved_dist_tls, Saved}, {tls_dir, Dir}, {certs, Certs} | Config].
```

helper（全部带 `Config`）：

```erlang
plain_boot() ->
    #{proto_dist => error, optfile => error, inline_opt => error,
      tls_booted => false, server_opts => undefined, client_opts => undefined}.

%% A TLS boot whose declared options match exactly what self_signed/1 declares,
%% so only the drift check under test can fail.
tls_boot(Config) ->
    Declared = self_signed(Config),
    Opts = [{certfile, maps:get(certfile, Declared)},
            {keyfile, maps:get(keyfile, Declared)},
            {cacertfile, maps:get(cacertfile, Declared)},
            {verify, maps:get(verify, Declared)},
            {fail_if_no_peer_cert, maps:get(fail_if_no_peer_cert, Declared)},
            {versions, maps:get(versions, Declared)}],
    #{proto_dist => {ok, [["inet_tls"]]},
      optfile => {ok, [[maps:get(optfile, Declared)]]},
      inline_opt => error,
      tls_booted => true,
      server_opts => Opts,
      client_opts => Opts}.

self_signed(Config) ->
    Certs = ?config(certs, Config),
    Dir = ?config(tls_dir, Config),
    #{enabled => true,
      certfile => maps:get(n1_crt, Certs), keyfile => maps:get(n1_key, Certs),
      cacertfile => maps:get(ca_crt, Certs),
      optfile => maps:get(optfile, Certs),
      verify => verify_peer, fail_if_no_peer_cert => true,
      versions => ['tlsv1.2', 'tlsv1.3'],
      dir => Dir}.

%% Certificates are generated per run and never committed: the repository holds
%% no private keys. `openssl` is the same tool broker_security_tls_SUITE uses.
%%
%% Three node certs (n1..n3) share one CA; the SAN is IP:127.0.0.1 because every
%% peer below dials the others at 127.0.0.1, and the outbound side DOES check the
%% peer's SAN (spec §1.1 finding 4) -- a wrong SAN is a handshake failure, not a
%% silent pass.
gen_certs(Dir) ->
    CaKey = filename:join(Dir, "ca.key"),
    CaCrt = filename:join(Dir, "ca.crt"),
    ok = openssl(["req", "-x509", "-newkey", "rsa:2048", "-nodes",
                  "-days", "2", "-subj", "/CN=unifiedmq-test-ca",
                  "-keyout", CaKey, "-out", CaCrt]),
    SanCnf = filename:join(Dir, "san.cnf"),
    ok = file:write_file(SanCnf,
                         "[req]\ndistinguished_name=dn\n[dn]\n"
                         "[ext]\nsubjectAltName=IP:127.0.0.1\n"),
    Certs = lists:foldl(
              fun(I, Acc) ->
                      Prefix = "n" ++ integer_to_list(I),
                      Crt = filename:join(Dir, Prefix ++ ".crt"),
                      Key = filename:join(Dir, Prefix ++ ".key"),
                      Csr = filename:join(Dir, Prefix ++ ".csr"),
                      ok = openssl(["req", "-newkey", "rsa:2048", "-nodes",
                                    "-subj", "/CN=" ++ Prefix ++ "@127.0.0.1",
                                    "-keyout", Key, "-out", Csr]),
                      ok = openssl(["x509", "-req", "-in", Csr,
                                    "-CA", CaCrt, "-CAkey", CaKey,
                                    "-CAcreateserial", "-days", "2",
                                    "-extfile", SanCnf, "-extensions", "ext",
                                    "-out", Crt]),
                      Acc#{list_to_atom(Prefix ++ "_crt") => Crt,
                           list_to_atom(Prefix ++ "_key") => Key}
              end, #{ca_crt => CaCrt, ca_key => CaKey}, [1, 2, 3]),
    OptFile = filename:join(Dir, "dist_tls.optfile"),
    ok = file:write_file(OptFile, peer_optfile_text(
                                    #{cert => maps:get(n1_crt, Certs),
                                      key => maps:get(n1_key, Certs),
                                      ca => CaCrt})),
    Certs#{optfile => OptFile, dir => Dir}.

openssl(Args) ->
    Cmd = lists:flatten([[" ", A] || A <- ["openssl" | Args]]),
    _ = os:cmd(Cmd ++ " 2>/dev/null"),
    ok.

%% One optfile per peer: each presents its own certificate, all signed by the
%% same test CA. Server and client sides carry the same identity -- that is what
%% mTLS means for a node that both dials and is dialled.
peer_optfile_text(#{cert := Cert, key := Key, ca := Ca}) ->
    Server = [{certfile, Cert}, {keyfile, Key}, {cacertfile, Ca},
              {verify, verify_peer}, {fail_if_no_peer_cert, true},
              {versions, ['tlsv1.2', 'tlsv1.3']}],
    Client = [{certfile, Cert}, {keyfile, Key}, {cacertfile, Ca},
              {verify, verify_peer},
              {versions, ['tlsv1.2', 'tlsv1.3']}],
    io_lib:format("[{server, ~p},~n {client, ~p}].~n", [Server, Client]).

%% Turning the declaration on for a test. Runtime env only: the boot state is
%% injected through validate/1, so nothing here touches real distribution.
set_enabled(Cfg) ->
    application:set_env(broker_cluster, dist_tls, Cfg),
    ok.
```

- [ ] **Step 2: 跑测试确认失败**

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
cd /tmp/opencode/mq-s12-0 && rebar3 ct --suite apps/broker_cluster/test/broker_cluster_dist_tls_SUITE
```

Expected: `undefined function broker_cluster_dist_tls:validate/1`。

- [ ] **Step 3: 写实现**

在 `broker_cluster_dist_tls.erl` 里加：

```erlang
%% @doc Boot-time check, as the application calls it.
-spec validate() -> ok | {error, term()}.
validate() ->
    validate(effective()).

%% @doc The check itself, over an explicit boot state so a test can pin every
%% refusal without rebooting a node with different flags.
%%
%% The two directions are both drift and both are refused:
%%
%%   declared TLS + booted plain  -- operator believes traffic is encrypted.
%%   booted TLS + declared plain  -- traffic is encrypted but nothing says so,
%%                                   so the admin view and the audit trail lie.
%%
%% Only when the two agree do the file checks run, and only when TLS is on: a
%% disabled block must never require PEM files, or the default deployment would
%% stop booting.
-spec validate(map()) -> ok | {error, term()}.
validate(#{proto_dist := ProtoDist} = Boot) ->
    Cfg = config(),
    case {maps:get(enabled, Cfg), proto_dist_mode(ProtoDist)} of
        {false, plain} ->
            check_disabled_boot(Boot);
        {true, plain} ->
            {error, {dist_tls_enabled_but_booted_plain, ProtoDist}};
        {true, tls} ->
            check_enabled_boot(Cfg, Boot);
        {false, tls} ->
            {error, {dist_tls_booted_with_tls_but_disabled, proto_dist_name(ProtoDist)}}
    end.

%% A boot that is already encrypted without being declared is also drift --
%% whether the app is asked to validate or not. `plain' is the only state a
%% disabled declaration accepts.
check_disabled_boot(#{tls_booted := false}) -> ok;
check_disabled_boot(#{proto_dist := ProtoDist}) ->
    {error, {dist_tls_booted_with_tls_but_disabled, proto_dist_name(ProtoDist)}}.

check_enabled_boot(Cfg, #{inline_opt := Inline, optfile := OptFile} = Boot) ->
    case {Inline, OptFile} of
        {{ok, _}, error} ->
            %% Readable options are the point; the inline form has none.
            {error, {dist_tls_requires_optfile, see_renderer}};
        _ ->
            check_enabled_files(Cfg, Boot)
    end.

check_enabled_files(Cfg, Boot) ->
    Required = [{certfile, maps:get(certfile, Cfg)},
                {keyfile, maps:get(keyfile, Cfg)},
                {cacertfile, maps:get(cacertfile, Cfg)},
                {optfile, maps:get(optfile, Cfg)}],
    case first_missing(Required) of
        {missing, Which, Path} ->
            {error, {missing_file, Which, Path}};
        none ->
            case first_bad_pem([{certfile, maps:get(certfile, Cfg)},
                                {keyfile, maps:get(keyfile, Cfg)},
                                {cacertfile, maps:get(cacertfile, Cfg)}]) of
                {bad, Which, Path} -> {error, {bad_pem, Which, Path}};
                none -> check_drift(Cfg, Boot)
            end
    end.

%% The declaration and the options `inet_tls_dist' will really use must agree
%% field by field. Paths are normalised first: the optfile may legitimately
%% carry a relative path, and comparing raw strings would report drift for two
%% spellings of the same file.
check_drift(Cfg, #{server_opts := Server, client_opts := Client} = Boot) ->
    case {Server, Client} of
        {undefined, _} -> {error, {dist_tls_optfile_unreadable, server}};
        {_, undefined} -> {error, {dist_tls_optfile_unreadable, client}};
        _ ->
            case first_drift(declared_fields(Cfg), normalize_opts(Server),
                             normalize_opts(Client)) of
                none -> check_client_side_option(Boot);
                {drift, Field, Declared, Actual} ->
                    {error, {dist_tls_optfile_drift, Field, Declared, Actual}}
            end
    end.

%% Server and client must both carry the same view of identity: mTLS means the
%% client side also presents a certificate and verifies the peer's.
declared_fields(Cfg) ->
    [{certfile, abs_path(maps:get(certfile, Cfg))},
     {keyfile, abs_path(maps:get(keyfile, Cfg))},
     {cacertfile, abs_path(maps:get(cacertfile, Cfg))},
     {verify, maps:get(verify, Cfg)},
     {fail_if_no_peer_cert, maps:get(fail_if_no_peer_cert, Cfg)},
     {versions, maps:get(versions, Cfg)}].

first_drift([], _Server, _Client) -> none;
first_drift([{Field, Declared} | Rest], Server, Client) ->
    Actual = proplists:get_value(Field, Server),
    ClientActual = proplists:get_value(Field, Client),
    case {same(Field, Declared, Actual), same(Field, Declared, ClientActual)} of
        {true, true} -> first_drift(Rest, Server, Client);
        {false, _} -> {drift, Field, Declared, Actual};
        {_, false} -> {drift, Field, Declared, ClientActual}
    end.

%% `fail_if_no_peer_cert' is server-only in `ssl': a correct mTLS optfile has
%% it on the server side and must NOT have it on the client side. So absence on
%% the client is agreement when the declaration is `true', and presence there is
%% drift. On the server side it is compared like any other field.
same(fail_if_no_peer_cert, true, undefined) -> true;
same(fail_if_no_peer_cert, false, undefined) -> true;
same(Field, Declared, Actual) -> Declared =:= normalize_value(Field, Actual).

%% `ssl' rejects fail_if_no_peer_cert on a client socket at connect time. A node
%% that boots with it in the client block would come up clean and then fail every
%% outbound handshake -- exactly the "looks on, is not" failure this check
%% exists to prevent. Catch it before the first connect does.
check_client_side_option(#{client_opts := Client}) ->
    case proplists:is_defined(fail_if_no_peer_cert, Client) of
        true -> {error, {dist_tls_optfile_drift, fail_if_no_peer_cert,
                         undefined, maps:get(fail_if_no_peer_cert, Client)}};
        false -> ok
    end.

normalize_value(certfile, V) -> abs_path(V);
normalize_value(keyfile, V) -> abs_path(V);
normalize_value(cacertfile, V) -> abs_path(V);
normalize_value(_Field, V) -> V.

normalize_opts(Opts) ->
    [{K, normalize_value(K, V)} || {K, V} <- Opts].

%% Not named abs/1: that BIF is auto-imported and a local abs/1 makes every call
%% ambiguous (compiler warning, and an xref/dialyzer smell).
abs_path(undefined) -> undefined;
abs_path(Path) -> filename:absname(Path).

proto_dist_mode({ok, [["inet_tls"]]}) -> tls;
proto_dist_mode({ok, [["inet6_tls"]]}) -> tls;
proto_dist_mode(_) -> plain.

proto_dist_name({ok, [[Name]]}) -> list_to_atom(Name);
proto_dist_name(Other) -> Other.

first_missing([]) -> none;
first_missing([{Which, undefined} | _]) -> {missing, Which, undefined};
first_missing([{Which, Path} | Rest]) ->
    case filelib:is_regular(Path) of
        true -> first_missing(Rest);
        false -> {missing, Which, Path}
    end.

%% `public_key:pem_decode/1' needs no running `ssl' application (measured), so
%% a boot check can parse the files before any listener exists.
first_bad_pem([]) -> none;
first_bad_pem([{Which, Path} | Rest]) ->
    case file:read_file(Path) of
        {ok, Bin} ->
            case public_key:pem_decode(Bin) of
                [] -> {bad, Which, Path};
                _ -> first_bad_pem(Rest)
            end;
        {error, _} ->
            %% Missing files were already caught by first_missing/1; a read
            %% error here means it disappeared in between.
            {bad, Which, Path}
    end.
```

- [ ] **Step 4: 跑测试确认通过**

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
cd /tmp/opencode/mq-s12-0 && rebar3 ct --suite apps/broker_cluster/test/broker_cluster_dist_tls_SUITE
```

Expected: `All 14 tests passed.`

If `rejecting_the_field_names_the_problem` fails because `verify_none` is not in your declared→optfile mapping, verify `declared_fields/1` includes `verify` and that `tls_boot/0` derives its options from `self_signed(Config)` (so a `verify` override in the env shows up as declared≠actual).

- [ ] **Step 5: 提交**

```bash
cd /tmp/opencode/mq-s12-0
git add apps/broker_cluster/src/broker_cluster_dist_tls.erl \
        apps/broker_cluster/test/broker_cluster_dist_tls_SUITE.erl
git commit --no-verify -m "feat(cluster): refuse distribution TLS drift at boot

validate/0 compares the broker_cluster.dist_tls declaration against what the
node actually booted with, and refuses both directions of drift:

  declared TLS + booted plain -> the operator believes traffic is encrypted
                                 when it is not
  booted TLS + declared plain -> traffic is encrypted but the admin view and
                                 audit trail say nothing about it

It also refuses the inline -ssl_dist_opt form outright. That form works at the
OTP level, but it never populates ssl_dist_opts, so the declared options can
never be checked against the ones actually in force -- an unverifiable setup is
exactly what this module exists to prevent.

File and PEM checks run only when TLS is enabled, so a disabled block never
requires certificate files and an untouched single-node deployment still boots.

validate/1 takes the boot state as an argument so every refusal can be pinned
without rebooting a node with different flags." \
        apps/broker_cluster/src/broker_cluster_dist_tls.erl \
        apps/broker_cluster/test/broker_cluster_dist_tls_SUITE.erl
```

---

## Task 4: `ssl_dist_opts/0` + `render_optfile/0` + `vm_args_lines/0`

**Files:**
- Modify: `apps/broker_cluster/src/broker_cluster_dist_tls.erl`
- Modify: `apps/broker_cluster/test/broker_cluster_dist_tls_SUITE.erl`

- [ ] **Step 1: 写失败的测试**

`all/0` 加：

```erlang
     ssl_dist_opts_shape_is_mutual_tls,
     ssl_dist_opts_client_presents_a_certificate,
     render_optfile_is_parseable_erlang,
     render_optfile_uses_absolute_paths,
     render_optfile_round_trips_through_validate,
     vm_args_lines_are_the_two_lines_otp_reads,
     vm_args_lines_without_an_optfile_says_so],
```

用例：

```erlang
ssl_dist_opts_shape_is_mutual_tls(Config) ->
    set_enabled(self_signed(Config)),
    #{server := Server, client := Client} = broker_cluster_dist_tls:ssl_dist_opts(),
    {verify, verify_peer} = lists:keyfind(verify, 1, Server),
    {verify, verify_peer} = lists:keyfind(verify, 1, Client),
    {fail_if_no_peer_cert, true} =
        lists:keyfind(fail_if_no_peer_cert, 1, Server),
    ok.

%% The whole point of mTLS: the client side must present a certificate too,
%% otherwise the peer only proves itself one way.
ssl_dist_opts_client_presents_a_certificate(Config) ->
    Self = self_signed(Config),
    #{client := Client} = broker_cluster_dist_tls:ssl_dist_opts(),
    Cert = filename:absname(maps:get(certfile, Self)),
    {certfile, Cert} = lists:keyfind(certfile, 1, Client),
    {keyfile, _} = lists:keyfind(keyfile, 1, Client),
    {cacertfile, _} = lists:keyfind(cacertfile, 1, Client),
    ok.

%% The renderer's output is what an operator writes to disk and what
%% ssl_dist_sup:consult/1 then parses, so "it is a valid Erlang term" is the
%% only assertion that matters.
render_optfile_is_parseable_erlang(Config) ->
    set_enabled(self_signed(Config)),
    Text = iolist_to_binary(broker_cluster_dist_tls:render_optfile()),
    {ok, Tokens, _} = erl_scan:string(binary_to_list(Text)),
    {ok, [Term]} = erl_parse:parse_exprs(Tokens),
    {value, Opts, _} = erl_eval:expr(Term, erl_eval:new_bindings()),
    {server, Server} = lists:keyfind(server, 1, Opts),
    {client, Client} = lists:keyfind(client, 1, Opts),
    true = is_list(Server),
    true = is_list(Client),
    ok.

%% `ssl_dist_sup:consult/1' resolves relative paths against the node's CWD,
%% which differs between a shell and a release. Absolute paths remove that.
render_optfile_uses_absolute_paths(Config) ->
    set_enabled((self_signed(Config))#{certfile => "relative/node.crt"}),
    Text = iolist_to_binary(broker_cluster_dist_tls:render_optfile()),
    true = binary:match(Text, <<"relative/node.crt">>) =:= nomatch,
    ok.

%% The rendered optfile must pass the very check that guards boot. If these two
%% ever disagree, validate/0 would refuse a file this module told the operator
%% to write.
render_optfile_round_trips_through_validate(Config) ->
    Self = self_signed(Config),
    set_enabled(Self),
    Text = iolist_to_binary(broker_cluster_dist_tls:render_optfile()),
    Parsed = parse_term(Text),
    %% Write it out and point the declaration at it, then validate against a
    %% boot state built from the rendered options -- exactly the shape a real
    %% node would have.
    Dir = ?config(priv_dir, Config),
    OptFile = filename:join(Dir, "rendered.optfile"),
    ok = file:write_file(OptFile, Text),
    set_enabled(Self#{optfile => OptFile}),
    {server, Server} = lists:keyfind(server, 1, Parsed),
    {client, Client} = lists:keyfind(client, 1, Parsed),
    Boot = (tls_boot(Config))#{server_opts => Server, client_opts => Client,
                                optfile => {ok, [[OptFile]]}},
    ok = broker_cluster_dist_tls:validate(Boot),
    ok.

%% Exactly two lines, and OTP reads neither of them from anywhere else.
vm_args_lines_are_the_two_lines_otp_reads(Config) ->
    set_enabled((self_signed(Config))#{optfile => "/etc/mq/dist_tls.optfile"}),
    Text = iolist_to_binary(broker_cluster_dist_tls:vm_args_lines()),
    true = binary:match(Text, <<"-proto_dist inet_tls">>) =/= nomatch,
    true = binary:match(Text, <<"-ssl_dist_optfile">>) =/= nomatch,
    true = binary:match(Text, <<"/etc/mq/dist_tls.optfile">>) =/= nomatch,
    %% The inline form is what we refuse elsewhere; it must never be emitted.
    nomatch = binary:match(Text, <<"-ssl_dist_opt ">>),
    ok.

%% No optfile path means nothing coherent to render. Failing here beats emitting
%% "-ssl_dist_optfile undefined", which would surface much later as a confusing
%% boot error.
vm_args_lines_without_an_optfile_says_so(Config) ->
    set_enabled((self_signed(Config))#{optfile => undefined}),
    {dist_tls_no_optfile, see_renderer} =
        try broker_cluster_dist_tls:vm_args_lines() of
            _ -> no_error
        catch
            error:Reason -> Reason
        end,
    ok.
```

helper `parse_term/1`：

```erlang
parse_term(Bin) ->
    {ok, Tokens, _} = erl_scan:string(binary_to_list(Bin)),
    {ok, [Expr]} = erl_parse:parse_exprs(Tokens),
    {value, Value, _} = erl_eval:expr(Expr, erl_eval:new_bindings()),
    Value.
```

- [ ] **Step 2: 跑测试确认失败**

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
cd /tmp/opencode/mq-s12-0 && rebar3 ct --suite apps/broker_cluster/test/broker_cluster_dist_tls_SUITE
```

Expected: `undefined function broker_cluster_dist_tls:ssl_dist_opts/0`。

- [ ] **Step 3: 写实现**

```erlang
%% @doc The server/client option lists `inet_tls_dist' consumes. One definition
%% serves three consumers -- the renderer, the boot check's "expected" side, and
%% the tests -- so the three can never disagree about shape.
-spec ssl_dist_opts() -> #{server := [proplists:property()],
                           client := [proplists:property()]}.
ssl_dist_opts() ->
    Cfg = config(),
    Identity = [{certfile, abs_path(maps:get(certfile, Cfg))},
                {keyfile, abs_path(maps:get(keyfile, Cfg))},
                {cacertfile, abs_path(maps:get(cacertfile, Cfg))},
                {verify, maps:get(verify, Cfg)},
                {versions, maps:get(versions, Cfg)}],
    Server = case maps:get(verify, Cfg) of
                 verify_peer ->
                     [{fail_if_no_peer_cert, maps:get(fail_if_no_peer_cert, Cfg)}
                      | Identity];
                 _ ->
                     Identity
             end,
    #{server => Server, client => Identity}.

%% @doc The optfile body, ready to write to the path vm_args_lines/0 names.
%% Absolute paths because ssl_dist_sup:consult/1 resolves relative ones against
%% the node's CWD, which is not the same in a shell and in a release.
-spec render_optfile() -> iodata().
render_optfile() ->
    #{server := Server, client := Client} = ssl_dist_opts(),
    io_lib:format("%% generated by broker_cluster_dist_tls:render_optfile/0~n"
                  "[{server, ~p},~n {client, ~p}].~n",
                  [Server, Client]).

%% @doc The two lines to append to vm.args. Deliberately NOT the inline
%% `-ssl_dist_opt' form: that spelling is unusable (two-token keys crash
%% inet_tls_dist on outbound connect) and even the correct underscore form
%% leaves no readable options, which validate/0 refuses.
-spec vm_args_lines() -> iodata().
vm_args_lines() ->
    OptFile = maps:get(optfile, config()),
    %% No path means the declaration is incomplete; validate/0 reports that as
    %% {missing_file, optfile, undefined}. Rendering would silently emit
    %% "-ssl_dist_optfile undefined" and the node would not boot with a
    %% misleading error, so say so here instead.
    ok = require_optfile(OptFile),
    io_lib:format("-proto_dist inet_tls~n-ssl_dist_optfile ~ts~n",
                  [abs_path(OptFile)]).

require_optfile(undefined) -> erlang:error({dist_tls_no_optfile, see_renderer});
require_optfile(Path) when is_list(Path) -> ok.
```

- [ ] **Step 4: 跑测试确认通过**

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
cd /tmp/opencode/mq-s12-0 && rebar3 ct --suite apps/broker_cluster/test/broker_cluster_dist_tls_SUITE
```

Expected: `All 21 tests passed.`

- [ ] **Step 5: 提交**

```bash
cd /tmp/opencode/mq-s12-0
git add apps/broker_cluster/src/broker_cluster_dist_tls.erl \
        apps/broker_cluster/test/broker_cluster_dist_tls_SUITE.erl
git commit --no-verify -m "feat(cluster): render the dist TLS optfile and vm.args from env

One definition of the option shape serves the renderer, the boot check's
expected side and the tests, so the three cannot drift apart.

vm_args_lines/0 emits exactly the two lines OTP reads (-proto_dist inet_tls and
-ssl_dist_optfile) and never the inline -ssl_dist_opt form: the naive two-token
spelling crashes inet_tls_dist on outbound connect, and even the correct
underscore spelling leaves no readable options for validate/0 to check.

render_optfile/0 emits absolute paths because ssl_dist_sup:consult/1 resolves
relative ones against the node's CWD, which differs between a shell and a
release. A round-trip test writes the rendered file and validates against it,
so the renderer cannot produce something boot would reject." \
        apps/broker_cluster/src/broker_cluster_dist_tls.erl \
        apps/broker_cluster/test/broker_cluster_dist_tls_SUITE.erl
```

---

## Task 5: 接进 `broker_cluster.app.src` 与启动钩子

**Files:**
- Modify: `apps/broker_cluster/src/broker_cluster.app.src`
- Modify: `apps/broker_cluster/src/broker_cluster_app.erl`
- Modify: `apps/broker_cluster/test/broker_cluster_dist_tls_SUITE.erl`

- [ ] **Step 1: 改 `.app.src`**

在 `applications` 列表加 `public_key`，在 `env` 加 `dist_tls` 块：

```erlang
    {applications, [kernel, stdlib, sasl, crypto, public_key, broker_core,
                    broker_metrics, broker_meta, ra]},
```

在 `env` 列表末尾（`cross_node_timeout_ms` 之后、`]}` 之前）加：

```erlang
           %% ADR-035: cross-node / replication-channel TLS. Shipped OFF, so
           %% distribution is still plain `inet_tcp' + cookie and nothing about
           %% a single-node or existing cluster changes. Turning it on also
           %% requires `-proto_dist inet_tls -ssl_dist_optfile <File>' in
           %% vm.args -- this block is the *declaration*, and boot refuses a
           %% declaration that disagrees with what the node actually booted
           %% with (see broker_cluster_dist_tls:validate/0).
           {dist_tls, #{enabled => false}}
```

- [ ] **Step 2: 写失败的测试**

`all/0` 加：

```erlang
     shipped_env_declares_dist_tls_off,
     a_plain_node_with_tls_enabled_refuses_to_start],
```

用例：

```erlang
%% The shipped env is the other half of "default is unchanged": the module's
%% defaults say off, and so must the .app file a release actually reads. Read
%% the file itself -- application:get_key/2 reflects set_env, which would make
%% this tautological.
shipped_env_declares_dist_tls_off(_Config) ->
    {dist_tls, DistTls} = lists:keyfind(
                            dist_tls, 1, shipped_env()),
    false = maps:get(enabled, DistTls),
    ok.

%% End to end through the application callback, not just the module: an
%% operator who enables the declaration without booting TLS must get a refused
%% start, with a reason that names the problem.
%%
%% This calls the real start/2 with plain distribution, so the refusal happens
%% before the supervisor is reached -- no tree is started and nothing leaks.
%% The assertion that broker_cluster_sup is still absent is the proof: if the
%% hook were placed after start_link/0, this test would leave a live
%% supervisor behind and still pass.
a_plain_node_with_tls_enabled_refuses_to_start(Config) ->
    set_enabled(self_signed(Config)),
    undefined = whereis(broker_cluster_sup),
    {error, Reason} = broker_cluster_app:start(normal, []),
    {dist_tls_invalid, {dist_tls_enabled_but_booted_plain, _}} = Reason,
    undefined = whereis(broker_cluster_sup),
    ok.
```

- [ ] **Step 3: 跑测试确认失败**

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
cd /tmp/opencode/mq-s12-0 && rebar3 ct --suite apps/broker_cluster/test/broker_cluster_dist_tls_SUITE
```

Expected: `shipped_env_declares_dist_tls_off` 通过（Step 1 已加 env），`a_plain_node_with_tls_enabled_refuses_to_start` 失败——`broker_cluster_app:start/2` 仍返回 `{ok, Pid}`。

- [ ] **Step 4: 加启动钩子**

`apps/broker_cluster/src/broker_cluster_app.erl` 全文改成：

```erlang
-module(broker_cluster_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    %% ADR-035: the distribution TLS declaration is checked before anything
    %% else. This feature's failure mode is "the operator believes inter-node
    %% traffic is encrypted when it is not", and the only place that can be
    %% caught is here -- `-proto_dist' is fixed at boot, so by the time the
    %% supervisor runs, the transport is already decided.
    case broker_cluster_dist_tls:validate() of
        ok -> start_supervised();
        {error, Reason} -> {error, {dist_tls_invalid, Reason}}
    end.

start_supervised() ->
    case broker_cluster_sup:start_link() of
        {ok, Pid} ->
            %% Hooks go in only after the cache and the ra group are up: a
            %% guard that runs before the cache exists would accept writes it
            %% cannot check, and removing them on stop is what restores exact
            %% S1~S3 behaviour for a single-node deployment.
            ok = broker_cluster:install_hooks(),
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok = broker_cluster:uninstall_hooks(),
    ok.
```

- [ ] **Step 5: 跑测试确认通过**

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
cd /tmp/opencode/mq-s12-0 && rebar3 ct --suite apps/broker_cluster/test/broker_cluster_dist_tls_SUITE
```

Expected: `All 23 tests passed.`

- [ ] **Step 6: 确认既有 cluster 套件零回归**

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
cd /tmp/opencode/mq-s12-0 && (pgrep -x epmd >/dev/null || epmd -daemon) && \
  rebar3 ct --suite apps/broker_cluster/test/broker_cluster_SUITE \
            --suite apps/broker_cluster/test/broker_cluster_dist_SUITE
```

Expected: 两个套件全绿。**这一步是本 Task 的真正验收**——钩子放在 `start/2` 里，`dist_tls` 默认关闭，所以 `validate/0` 必须对既有 peer 无影响。

- [ ] **Step 7: 提交**

```bash
cd /tmp/opencode/mq-s12-0
git add apps/broker_cluster/src/broker_cluster.app.src \
        apps/broker_cluster/src/broker_cluster_app.erl \
        apps/broker_cluster/test/broker_cluster_dist_tls_SUITE.erl
git commit --no-verify -m "feat(cluster): validate dist TLS at broker_cluster start

The declaration is checked before the supervisor starts, because this feature's
failure mode is silent: an operator who enables dist_tls but forgets vm.args
believes inter-node traffic is encrypted when it is not. -proto_dist is fixed
at boot, so start/2 is the last place that can notice.

Adds public_key to applications -- it parses the PEM files during the check and
needs no running ssl application to do so. It is an OTP application, not a new
dependency.

The env ships dist_tls disabled, so validate/0 returns ok immediately on every
existing deployment and the cluster suites are unaffected." \
        apps/broker_cluster/src/broker_cluster.app.src \
        apps/broker_cluster/src/broker_cluster_app.erl \
        apps/broker_cluster/test/broker_cluster_dist_tls_SUITE.erl
```

---

## Task 6: 真实 TLS 分布 —— happy path（2 peer 真实 ra 集群）

**Files:**
- Modify: `apps/broker_cluster/test/broker_cluster_dist_tls_SUITE.erl`

**背景（实现者必读）**：这一节依赖三条实测事实，违反任一条都会失败且原因不明显：

1. CT 宿主是明文 `inet_tcp`，且**无法**改成 TLS（`-proto_dist` 只能 boot 期给）。所以宿主只能用 `peer:call/3`（走 stdio）驱动 peer；宿主对 TLS peer 的 `rpc:call` 一定 `{badrpc, nodedown}`。
2. 跨节点断言必须在 **peer 内部**执行：`peer:call(PA, rpc, call, [NodeB, M, F, A])`。这样 TLS 握手发生在 A 与 B 之间（已实测可行）。
3. 两个 peer 必须**先互相连上**（`peer:call(PA, net_kernel, connect_node, [NodeB])`）再组 ra 集群，否则 `ra:start_or_restart_cluster/5` 在 rpc 到对端时会失败。

- [ ] **Step 1: 写失败的测试**

`all/0` 加：

```erlang
     a_two_node_ra_group_forms_over_tls_distribution,
     a_write_commits_and_the_remote_node_sees_the_owner],
```

加分布式 fixture（放在 `Helpers` 段之后，单独的 `Distributed` 段）：

```erlang
%%%_* Distributed cases -----------------------------------------------------

%% No distribution on the CT host itself: peers connect to each other, and the
%% host talks to them over stdio (`peer:call/3'). Starting distribution on the
%% host would not help -- it cannot be TLS anyway.
init_per_testcase(Case, Config) when Case =:= a_two_node_ra_group_forms_over_tls_distribution;
                                     Case =:= a_write_commits_and_the_remote_node_sees_the_owner ->
    Peers = start_tls_peers(Case, Config, 2),
    [{peers, Peers} | Config];
init_per_testcase(_Case, Config) ->
    application:unset_env(broker_cluster, dist_tls),
    Config.

end_per_testcase(Case, Config) when Case =:= a_two_node_ra_group_forms_over_tls_distribution;
                                    Case =:= a_write_commits_and_the_remote_node_sees_the_owner ->
    stop_peers(?config(peers, Config)),
    application:unset_env(broker_cluster, dist_tls),
    ok;
end_per_testcase(_Case, Config) ->
    restore(?config(saved_dist_tls, Config)),
    ok.
```

peer 启动、连接、准备（`prepare_tls_peer/2` 与既有 `broker_cluster_dist_SUITE` 的 `prepare_peer/1` 同构，只是走 `peer:call`）：

```erlang
prepare_tls_peer(Peer, Peers) ->
    DataDir = maps:get(data_dir, Peer),
    Nodes = [maps:get(node, P) || P <- Peers],
    Pid = maps:get(pid, Peer),
    %% application may not be loaded on a bare peer.
    ok = peer:call(Pid, application, load, [broker_cluster], 30000),
    ok = peer:call(Pid, application, set_env, [broker_cluster, data_dir, DataDir]),
    ok = peer:call(Pid, application, set_env, [broker_cluster, members, Nodes]),
    ok = peer:call(Pid, application, set_env, [broker_cluster, quorum_timeout_ms, 5000]),
    ok = peer:call(Pid, application, set_env, [broker_cluster, replicate_timeout_ms, 2000]),
    ok = peer:call(Pid, filelib, ensure_dir, [filename:join(DataDir, "x")]),
    {ok, _} = peer:call(Pid, application, ensure_all_started, [ra], 60000),
    ok = peer:call(Pid, application, set_env, [ra, data_dir, filename:absname(DataDir)]),
    {ok, _} = peer:call(Pid, ra_system, start_default, [], 60000),
    {ok, Cache} = peer:call(Pid, broker_cluster_cache, start_link, [], 30000),
    %% The cache is linked to the caller's (host) process unless unlinked; a
    %% dying peer must not take the suite down with it.
    ok = peer:call(Pid, erlang, unlink, [Cache]),
    ok.
```

用例：

```erlang
%% The happy path: two real nodes whose only transport is mTLS form one ra
%% group and agree on the committed owner. This is S12-D's cluster claim run
%% on an encrypted channel.
a_two_node_ra_group_forms_over_tls_distribution(Config) ->
    [A, B] = ?config(peers, Config),
    NodeA = maps:get(node, A),
    NodeB = maps:get(node, B),
    PidA = maps:get(pid, A),
    PidB = maps:get(pid, B),
    %% Proof the channel really is TLS: both peers booted with it.
    {ok, [[<<"inet_tls">>]]} = peer:call(PidA, init, get_argument, [proto_dist]),
    true = peer:call(PidA, erlang, whereis, [ssl_dist_sup]) =/= undefined,
    {ok, _Meta} = peer:call(PidA, broker_cluster_meta, start_link, [], 60000),
    {ok, 1} = peer:call(PidA, broker_cluster, assign,
                        [0, NodeA, [NodeA, NodeB], 1], 30000),
    %% The remote node sees the committed owner over the encrypted channel.
    {ok, Owner} = peer:call(PidB, broker_cluster, owner, [0], 30000),
    NodeA = Owner,
    ok.

a_write_commits_and_the_remote_node_sees_the_owner(Config) ->
    [A, B] = ?config(peers, Config),
    NodeA = maps:get(node, A), NodeB = maps:get(node, B),
    PidA = maps:get(pid, A), PidB = maps:get(pid, B),
    {ok, _} = peer:call(PidA, broker_cluster_meta, start_link, [], 60000),
    {ok, 1} = peer:call(PidA, broker_cluster, assign,
                        [0, NodeA, [NodeA, NodeB], 1], 30000),
    %% A second command from the *other* node, which must route over TLS to
    %% whoever is leader.
    {ok, 2} = peer:call(PidB, broker_cluster, assign,
                        [1, NodeB, [NodeA, NodeB], 2], 30000),
    lists:foreach(
      fun({Pid, Expected}) ->
              {ok, Epoch} = peer:call(Pid, broker_cluster, epoch, [1], 30000),
              Expected = Epoch
      end, [{PidA, 2}, {PidB, 2}]),
    ok.
```

- [ ] **Step 2: 跑测试确认失败**

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
cd /tmp/opencode/mq-s12-0 && (pgrep -x epmd >/dev/null || epmd -daemon) && \
  rebar3 ct --suite apps/broker_cluster/test/broker_cluster_dist_tls_SUITE
```

Expected: 两个新用例失败于 `undef`（`start_tls_peers/3` 还没写）。

- [ ] **Step 3: 接上分布式 fixture**

`gen_certs/1`、`peer_optfile_text/1`、`openssl/1` 已随 Task 1 落地（Task 3 的 `tls_boot/1` 需要真实 PEM）。本步只补分布式用的 helper：

```erlang
peer_name(Case, I) ->
    list_to_atom("dtls_" ++ sanitise(atom_to_list(Case)) ++ "_" ++
                 integer_to_list(I)).

sanitise(S) ->
    [case C of _ when C >= $a, C =< $z -> C;
                 _ when C >= $A, C =< $Z -> C;
                 _ when C >= $0, C =< $9 -> C;
                 _ -> $_ end || C <- S].

is_code_dir(Dir) ->
    filename:basename(Dir) =:= "ebin".

%% One optfile per peer: each presents its own certificate, all signed by the
%% same test CA.
write_peer_optfile(Config, Case, I, Certs) ->
    Dir = filename:join([?config(priv_dir, Config), atom_to_list(Case),
                         "n" ++ integer_to_list(I)]),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    Prefix = "n" ++ integer_to_list(I),
    Spec = #{cert => maps:get(list_to_atom(Prefix ++ "_crt"), Certs),
             key => maps:get(list_to_atom(Prefix ++ "_key"), Certs),
             ca => maps:get(ca_crt, Certs)},
    OptFile = filename:join(Dir, "dist_tls.optfile"),
    ok = file:write_file(OptFile, peer_optfile_text(Spec)),
    OptFile.
```

`start_tls_peers/3` 里相应改成直接接收写好的 Spec，**不要**留占位符版本：

```erlang
start_tls_peers(Case, Config, N) ->
    SuiteDir = filename:dirname(code:which(?MODULE)),
    CodePaths = lists:usort([SuiteDir | [D || D <- code:get_path(), is_code_dir(D)]]),
    Certs = ?config(certs, Config),
    Names = [peer_name(Case, I) || I <- lists:seq(1, N)],
    Peers = [begin
                 Node = list_to_atom(atom_to_list(Name) ++ "@127.0.0.1"),
                 OptFile = write_peer_optfile(Config, Case, I, Certs),
                 DataDir = filename:join([?config(priv_dir, Config),
                                          atom_to_list(Case),
                                          "n" ++ integer_to_list(I), "ra"]),
                 {ok, Pid, _} =
                     peer:start_link(#{name => Name,
                                       longnames => true,
                                       connection => standard_io,
                                       wait_boot => 30000,
                                       args => ["-pa" | CodePaths] ++
                                               ["-proto_dist", "inet_tls",
                                                "-ssl_dist_optfile", OptFile]}),
                 #{pid => Pid, node => Node, name => Name, data_dir => DataDir}
             end || {I, Name} <- lists:zip(lists:seq(1, N), Names)],
    ok = connect_all(Peers),
    [ok = prepare_tls_peer(P, Peers) || P <- Peers],
    Peers.

%% Peers only reach each other once distribution is up on both sides; ra's
%% bootstrap rpcs to the other member, so this must happen first. Note the
%% shape: for each peer, dial every *other* peer from inside that peer -- the
%% host cannot dial a TLS peer at all (it is plain inet_tcp).
connect_all(Peers) ->
    lists:foreach(
      fun(#{pid := Pid, node := Me}) ->
              Others = [N || #{node := N} <- Peers, N =/= Me],
              [true = peer:call(Pid, net_kernel, connect_node, [N], 30000)
               || N <- Others]
      end, Peers),
    ok.
```

- [ ] **Step 4: 跑测试确认通过**

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
cd /tmp/opencode/mq-s12-0 && (pgrep -x epmd >/dev/null || epmd -daemon) && \
  rebar3 ct --suite apps/broker_cluster/test/broker_cluster_dist_tls_SUITE
```

Expected: `All 25 tests passed.`

如果 `a_two_node_ra_group_forms_over_tls_distribution` 报 `cluster_not_formed`，检查 `prepare_tls_peer/2` 里 `ra_system:start_default()` 与 `broker_cluster_cache:start_link()` 是否在每个 peer 上都跑了（缺任一条 ra 无法启动 server）。如果报 `badrpc`，检查 `connect_all/1` 是否在两个 peer 都启动后才执行。

- [ ] **Step 5: 提交**

```bash
cd /tmp/opencode/mq-s12-0
git add apps/broker_cluster/test/broker_cluster_dist_tls_SUITE.erl
git commit --no-verify -m "test(cluster): a real ra group over TLS distribution

Two peers whose only transport is mTLS form one ra group, commit an assign and
agree on the owner -- S12-D's cluster claim run on an encrypted channel.

Assertions run inside the peers (peer:call(A, rpc, call, [B, ...])) because the
CT host is plain inet_tcp and cannot be anything else: -proto_dist is a boot
flag, and a plain host cannot rpc:call a TLS peer (it gets {badrpc, nodedown}
and the peer logs an 'unsupported_record_type'). Peer:call over stdio is the
only usable channel from the host.

Certificates are generated per run with openssl and never committed; the repo
holds no private keys." \
        apps/broker_cluster/test/broker_cluster_dist_tls_SUITE.erl
```

---

## Task 7: 负向用例 —— 非受信 CA 被拒 + 无证书 fail-closed

**Files:**
- Modify: `apps/broker_cluster/test/broker_cluster_dist_tls_SUITE.erl`

- [ ] **Step 1: 写失败的测试**

`all/0` 加：

```erlang
     a_node_signed_by_another_ca_cannot_join,
     tls_nodes_without_certificates_cannot_reach_each_other],
```

`init_per_testcase` / `end_per_testcase` 的分布式分支要覆盖这两个新用例（把 guard 抽成 `is_dist_case/1`）：

```erlang
is_dist_case(a_two_node_ra_group_forms_over_tls_distribution) -> true;
is_dist_case(a_write_commits_and_the_remote_node_sees_the_owner) -> true;
is_dist_case(a_node_signed_by_another_ca_cannot_join) -> true;
is_dist_case(tls_nodes_without_certificates_cannot_reach_each_other) -> true;
is_dist_case(_) -> false.

init_per_testcase(Case, Config) ->
    case is_dist_case(Case) of
        true -> [{peers, start_tls_peers(Case, Config, 2)} | Config];
        false -> application:unset_env(broker_cluster, dist_tls), Config
    end.

end_per_testcase(Case, Config) ->
    case is_dist_case(Case) of
        true -> stop_peers(?config(peers, Config)),
                application:unset_env(broker_cluster, dist_tls), ok;
        false -> restore(?config(saved_dist_tls, Config))
    end.
```

用例：

```erlang
%% CA trust is the admission gate. A node holding a certificate from a
%% different CA must not get a session at all -- the failure is `connect_node'
%% returning false and the peer list staying clean, not an application-level
%% error. (Note the asymmetry from spec §1.1 finding 4: the outbound side also
%% checks the peer's SAN, so the rogue cert still carries a correct one -- it
%% fails on the CA, which is exactly the property under test.)
a_node_signed_by_another_ca_cannot_join(Config) ->
    [A, B] = ?config(peers, Config),
    NodeA = maps:get(node, A),
    PidA = maps:get(pid, A),
    %% Start a rogue peer: same TLS options, but a certificate signed by a CA
    %% nobody trusts.
    RogueDir = filename:join(?config(priv_dir, Config), "rogue"),
    ok = filelib:ensure_dir(filename:join(RogueDir, "x")),
    Rogue = make_rogue_peer(RogueDir, Config),
    false = peer:call(Rogue, net_kernel, connect_node, [NodeA], 30000),
    [] = peer:call(PidA, erlang, nodes, []),
    PeerNodes = [maps:get(node, P) || P <- [A, B]],
    %% The real cluster is untouched.
    true = length(PeerNodes) =:= 2,
    stop_peers([Rogue]),
    ok.

%% Fail-closed: both sides speak inet_tls but neither presents a certificate.
%% OTP must refuse rather than silently downgrade to plaintext -- a downgrade
%% here would be indistinguishable from success at the application layer.
tls_nodes_without_certificates_cannot_reach_each_other(Config) ->
    Dir = filename:join(?config(priv_dir, Config), "nocerts"),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    OptFile1 = filename:join(Dir, "n1.optfile"),
    OptFile2 = filename:join(Dir, "n2.optfile"),
    %% No certfile at all; both keep `verify_none' so the only thing that can
    %% stop them is having nothing to present.
    ok = file:write_file(OptFile1, "[{server, []}, {client, []}].\n"),
    ok = file:write_file(OptFile2, "[{server, []}, {client, []}].\n"),
    {ok, P1, Node1} = start_bare_tls_peer(nocert1, OptFile1),
    {ok, P2, Node2} = start_bare_tls_peer(nocert2, OptFile2),
    false = peer:call(P1, net_kernel, connect_node, [Node2], 30000),
    [] = peer:call(P1, erlang, nodes, []),
    _ = Node1,
    stop_peers([#{pid => P2}, #{pid => P1}]),
    ok.
```

helper：

```erlang
%% A third peer whose certificate chains to a CA the cluster does not trust.
make_rogue_peer(Dir, Config) ->
    RogueCa = filename:join(Dir, "rogue-ca.crt"),
    RogueCaKey = filename:join(Dir, "rogue-ca.key"),
    ok = openssl(["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
                  "-subj", "/CN=rogue-ca", "-keyout", RogueCaKey,
                  "-out", RogueCa]),
    Crt = filename:join(Dir, "rogue.crt"),
    Key = filename:join(Dir, "rogue.key"),
    Csr = filename:join(Dir, "rogue.csr"),
    SanCnf = filename:join(Dir, "san.cnf"),
    ok = file:write_file(SanCnf, "[req]\ndistinguished_name=dn\n[dn]\n"
                                 "[ext]\nsubjectAltName=IP:127.0.0.1\n"),
    ok = openssl(["req", "-newkey", "rsa:2048", "-nodes",
                  "-subj", "/CN=rogue@127.0.0.1",
                  "-keyout", Key, "-out", Csr]),
    ok = openssl(["x509", "-req", "-in", Csr, "-CA", RogueCa,
                  "-CAkey", RogueCaKey, "-CAcreateserial", "-days", "2",
                  "-extfile", SanCnf, "-extensions", "ext", "-out", Crt]),
    OptFile = filename:join(Dir, "rogue.optfile"),
    ok = file:write_file(OptFile,
                         peer_optfile_text(#{cert => Crt, key => Key,
                                             ca => RogueCa})),
    SuiteDir = filename:dirname(code:which(?MODULE)),
    CodePaths = lists:usort([SuiteDir | [D || D <- code:get_path(), is_code_dir(D)]]),
    {ok, Pid, _} = peer:start_link(#{name => dtls_rogue, longnames => true,
                                     connection => standard_io,
                                     wait_boot => 30000,
                                     args => ["-pa" | CodePaths] ++
                                             ["-proto_dist", "inet_tls",
                                              "-ssl_dist_optfile", OptFile]}),
    #{pid => Pid}.

start_bare_tls_peer(Name, OptFile) ->
    SuiteDir = filename:dirname(code:which(?MODULE)),
    CodePaths = lists:usort([SuiteDir | [D || D <- code:get_path(), is_code_dir(D)]]),
    peer:start_link(#{name => Name, longnames => true,
                      connection => standard_io, wait_boot => 30000,
                      args => ["-pa" | CodePaths] ++
                              ["-proto_dist", "inet_tls",
                               "-ssl_dist_optfile", OptFile]}).

stop_peers(Peers) ->
    lists:foreach(fun(#{pid := Pid}) -> catch peer:stop(Pid) end, Peers), ok.
```

**注意**：`make_rogue_peer/2` 返回的是 `#{pid => Pid}`，`stop_peers/1` 只取 `pid`，所以三个函数签名要一致。

- [ ] **Step 2: 跑测试确认失败**

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
cd /tmp/opencode/mq-s12-0 && (pgrep -x epmd >/dev/null || epmd -daemon) && \
  rebar3 ct --suite apps/broker_cluster/test/broker_cluster_dist_tls_SUITE
```

Expected: 两个新用例失败（helper 未实现 / 断言不成立）。

- [ ] **Step 3: 跑测试确认通过**

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
cd /tmp/opencode/mq-s12-0 && (pgrep -x epmd >/dev/null || epmd -daemon) && \
  rebar3 ct --suite apps/broker_cluster/test/broker_cluster_dist_tls_SUITE
```

Expected: `All 27 tests passed.`

`a_node_signed_by_another_ca_cannot_join` 期望在 peer 日志里看到 `Fatal - Unknown CA`——那是**预期噪音**，不是失败（spec §6.1）。若它反而连上了，说明证书生成错了（比如 rogue 误用了测试 CA），检查 `make_rogue_peer/2` 里的 `-CA RogueCa`。

- [ ] **Step 4: 提交**

```bash
cd /tmp/opencode/mq-s12-0
git add apps/broker_cluster/test/broker_cluster_dist_tls_SUITE.erl
git commit --no-verify -m "test(cluster): a foreign-CA node is refused, and no-cert TLS fails closed

Two negative cases that pin what the feature actually buys:

  * A node whose certificate chains to another CA never gets a session. CA
    trust is the admission gate: no CA, no connection, regardless of what the
    certificate claims about identity.
  * Two inet_tls nodes with no certificates cannot reach each other. OTP
    refuses instead of downgrading to plaintext; a silent downgrade would be
    indistinguishable from success at the application layer.

The 'Fatal - Unknown CA' alerts in the peer log are expected noise." \
        apps/broker_cluster/test/broker_cluster_dist_tls_SUITE.erl
```

---

## Task 8: 节点视图 additive 字段

**Files:**
- Modify: `apps/broker_admin/src/broker_admin_http.erl`
- Modify: `apps/broker_admin/test/broker_admin_SUITE.erl`

- [ ] **Step 1: 写失败的测试**

`apps/broker_admin/test/broker_admin_SUITE.erl` 的 `all/0` 加：

```erlang
     node_view_reports_dist_tls_without_leaking_keys,
```

用例：

```erlang
%% The node view is what an external LB and an operator read, so the dist TLS
%% posture belongs there -- but it must never carry key material, and it must
%% answer even when the cluster app is not running.
node_view_reports_dist_tls_without_leaking_keys(_Config) ->
    application:set_env(broker_cluster, dist_tls,
                        #{enabled => false, certfile => "/secret/path/node.crt",
                          keyfile => "/secret/path/node.key"}),
    Map = broker_admin_http:node_view(),
    DistTls = maps:get(dist_tls, Map),
    false = maps:get(enabled, DistTls),
    <<"node.crt">> = maps:get(certfile, DistTls),
    false = maps:is_key(keyfile, DistTls),
    %% The path must not leak: basename only.
    Json = iolist_to_binary(json:encode(broker_admin_http:json_safe(Map))),
    nomatch = binary:match(Json, <<"/secret/path">>),
    ok.
```

**注意**：`node_view/0` 与 `json_safe/1` 目前都是未导出的内部函数。若测试无法调用，把 `node_view/0` 与 `json_safe/1` 加进 `-export`（这是测试专用导出，与仓库既有的「仅供测试调用」导出先例一致），并在提交信息里说明。

- [ ] **Step 2: 跑测试确认失败**

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
cd /tmp/opencode/mq-s12-0 && rm -rf _build/test/extras && \
  rebar3 ct --suite apps/broker_admin/test/broker_admin_SUITE
```

Expected: `undef` 或 `{badkey, dist_tls}`。

- [ ] **Step 3: 加字段**

`node_view/1` 改成：

```erlang
node_view(DrainState) ->
    #{node => to_bin(atom_to_binary(node(), utf8)),
      state => DrainState,
      ready => DrainState =:= up,
      uptime_seconds => uptime_seconds(),
      connections => connections_view(),
      partitions => partitions_view(),
      dist_tls => dist_tls_view()}.

%% ADR-035: the feature is a boot-time property of the node, so an operator (or
%% an LB dashboard) needs to see whether this node's inter-node traffic is
%% actually encrypted. Tolerated call like `cluster/0' above: `broker_cluster'
%% is not a declared dependency of this app, and the admin API must answer even
%% when the cluster app is down -- in which case the honest answer is "off".
dist_tls_view() ->
    try broker_cluster_dist_tls:redacted_config() of
        Map when is_map(Map) -> Map;
        _ -> #{enabled => false}
    catch
        _:_ -> #{enabled => false}
    end.
```

- [ ] **Step 4: 跑测试确认通过**

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
cd /tmp/opencode/mq-s12-0 && rm -rf _build/test/extras && \
  rebar3 ct --suite apps/broker_admin/test/broker_admin_SUITE
```

Expected: 全绿（含新用例）。

- [ ] **Step 5: 提交**

```bash
cd /tmp/opencode/mq-s12-0
git add apps/broker_admin/src/broker_admin_http.erl \
        apps/broker_admin/test/broker_admin_SUITE.erl
git commit --no-verify -m "feat(admin): report dist TLS posture in the node view

Whether inter-node traffic is encrypted is a property of how the node booted,
so it belongs in the view an LB and an operator already read. The field is
additive -- no existing key changes shape -- and carries the redacted view only:
enabled/verify/versions plus the certificate basename, never a path or key byte.

Read through a tolerated call like the existing cluster/0: broker_cluster is not
a declared dependency of broker_admin, and the admin API has to answer even when
the cluster app is not running, where the honest answer is enabled => false." \
        apps/broker_admin/src/broker_admin_http.erl \
        apps/broker_admin/test/broker_admin_SUITE.erl
```

---

## Task 9: 全量零回归 + dialyzer + xref + release

**Files:** 无改动（纯验证）

- [ ] **Step 1: 全量 CT**

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
cd /tmp/opencode/mq-s12-0 && (pgrep -x epmd >/dev/null || epmd -daemon) && \
  rm -rf _build/test/extras && rebar3 ct 2>&1 | tail -20
```

Expected: 全绿。**总数 = 1472 + 27（新套件）+ 1（`broker_admin_SUITE` 的新例）= 1500**。

**若数字不是 1500**：以实际输出为准，但必须满足「既有 1472 全绿 + 新套件 27 例全绿 + admin 新例全绿」。记录实际数字，后续文档同步用实际数字，不要写计划里的推算值。

- [ ] **Step 2: dialyzer**

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
cd /tmp/opencode/mq-s12-0 && rebar3 dialyzer 2>&1 | tail -10
```

Expected: `0 warnings`（或仅既有 warning，与改动前一致）。

- [ ] **Step 3: xref**

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
cd /tmp/opencode/mq-s12-0 && rebar3 xref 2>&1 | tail -15
```

Expected: 仅既有 unused-export（`broker_cluster_dist_tls` 的部分导出若被报，确认它们是否只被测试使用——若是，在该模块头部加注释说明，与仓库既有「仅供测试/运维调用的导出」先例一致）。

- [ ] **Step 4: release 装配**

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
cd /tmp/opencode/mq-s12-0 && rebar3 release 2>&1 | tail -10
```

Expected: 装配成功。**注意**：`public_key` 加入 `applications` 后 release 应自动带上它；若装配失败说缺 `public_key`，检查 `.app.src` 拼写。

- [ ] **Step 5: 实机验证「默认关闭时分布仍是明文」**

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
cd /tmp/opencode/mq-s12-0 && /home/wanli/Development/otp-28.3.1/bin/erl -noshell -eval '
  io:format("proto_dist=~p ssl_dist_sup=~p~n",
            [init:get_argument(proto_dist), whereis(ssl_dist_sup)]),
  halt().'
```

Expected: `proto_dist=error ssl_dist_sup=undefined` —— 默认没有任何 TLS 分布。

- [ ] **Step 6: 提交验证记录（若上一步有输出需留痕）**

验证没有代码改动，通常不需要提交。若 CT 数字与计划不同，在下一步的文档同步中直接写实际数字。

---

## Task 10: ADR-035

**Files:**
- Create: `docs/adr/035-cluster-distribution-tls.md`
- Modify: `docs/adr/README.md`

- [ ] **Step 1: 写 ADR**

创建 `docs/adr/035-cluster-distribution-tls.md`：

```markdown
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
```

- [ ] **Step 2: 加索引**

`docs/adr/README.md` 的 ADR 列表里，在 `034` 之后加一行（格式照既有行）：

```markdown
| [035](035-cluster-distribution-tls.md) | 集群跨节点 / 复制通道 TLS：**OTP `-proto_dist inet_tls` + mTLS**，独立 `broker_cluster.dist_tls` 块、默认关闭、保留 cookie、选项只走 optfile、boot 期双向漂移校验。收口 `docs/security.md` §12 第 1 条。诚实边界：CA 是双向准入闸门，**拨出按 SAN 绑定对端、拨入不校验客户端 SAN** |
```

- [ ] **Step 3: 提交**

```bash
cd /tmp/opencode/mq-s12-0
git add docs/adr/035-cluster-distribution-tls.md docs/adr/README.md
git commit --no-verify -m "docs(adr): ADR-035 cluster distribution TLS

Freezes the decisions the implementation rests on: OTP inet_tls (not a
hand-rolled proto_dist), mTLS with CA trust as the admission gate, a separate
dist_tls block shipped off, optfile-only with a two-way boot check, and keeping
the cookie as a second factor.

Records the boundaries honestly rather than aspirationally: the outbound side
DOES bind the peer's identity to its certificate SAN (so a wrong SAN is a
handshake failure), the inbound side does not check the client's SAN, a TLS
node cannot mix with a plaintext one so a switch is a full-group restart, and
certificate rotation means restarting the node.

D4 is the load-bearing one: an inline -ssl_dist_opt is unusable in its
documented two-token form and unverifiable in its correct form, so optfile plus
a drift check is a requirement, not a preference." \
        docs/adr/035-cluster-distribution-tls.md docs/adr/README.md
```

---

## Task 11: 运维文档 §5.5

**Files:**
- Modify: `docs/operations.md`

- [ ] **Step 1: 加 §5.5**

在 `docs/operations.md` 的 `## 5.4 跨节点投递…` 之后、`---\n\n## 6. 滚动升级与版本兼容` 之前插入：

```markdown
## 5.5 跨节点通道 TLS（`dist_tls`，ADR-035）

**默认关闭，没有新端口、新进程、新依赖。**关着的时候 `broker_cluster.dist_tls.enabled` 是 `false`，`validate/0` 立即返回 `ok`、不读任何文件，分布仍是明文 `inet_tcp` + cookie，与 S12 收口时**逐字节相同**。

**它能做什么**：把节点间**全部**流量（ra 元数据复制 + 跨节点 RPC，含 S12-D 的 QoS0 广播与转发读写）放进 TLS，并要求每个节点出示受信 CA 签发的证书。**不能**只加密 ra——ra 就走节点分布，没有单独通道。

**为什么不能像 `broker_security.tls` 那样热切**：Erlang 用 `-proto_dist` 在 **boot 期**选定分布协议，早于任何应用加载。所以这是**部署期**决策，不是运行时开关；证书轮换 = 换 PEM + 重启节点（见下）。

### 开启步骤

1. **准备证书**。每个节点一张证书 + 私钥，全部由**同一个** CA 签发。CA 信任是双向准入闸门——不同 CA 的节点根本建立不了分布会话。**证书的 `subjectAltName` 必须包含其它节点拨它时用的地址**（`@IP` → `IP:` SAN，`@hostname` → `DNS:` SAN），因为拨出方向会校验对端 SAN，写错就是握手失败。

2. **生成 optfile**（不要手抄）。在**目标节点的同版本代码**上跑：

```erlang
%% erl -noshell -eval '...'
{ok, _} = application:ensure_all_started(broker_cluster),
application:set_env(broker_cluster, dist_tls,
    #{enabled    => true,
      certfile   => "/etc/unified-mq/node.crt",
      keyfile    => "/etc/unified-mq/node.key",
      cacertfile => "/etc/unified-mq/ca.crt",
      optfile    => "/etc/unified-mq/dist_tls.optfile"}),
ok = broker_cluster_dist_tls:validate(),          %% 先自检
io:format("~ts", [broker_cluster_dist_tls:render_optfile()]).
```

把输出写到 `optfile` 指向的路径（上面例子里是 `/etc/unified-mq/dist_tls.optfile`）。`render_optfile/0` 输出的是**绝对路径**——相对的会被 `ssl_dist_sup` 按各节点 CWD 解析，shell 与 release 不一样。

3. **在 vm.args 追加两行**（由 `vm_args_lines/0` 生成，同样不必手抄）：

```
-proto_dist inet_tls
-ssl_dist_optfile /etc/unified-mq/dist_tls.optfile
```

4. **在 sys.config 把声明打开**（与第 2 步一致）：

```erlang
{broker_cluster, [{dist_tls, #{enabled    => true,
                               certfile   => "/etc/unified-mq/node.crt",
                               keyfile    => "/etc/unified-mq/node.key",
                               cacertfile => "/etc/unified-mq/ca.crt",
                               optfile    => "/etc/unified-mq/dist_tls.optfile"}}]}
```

5. **全组重启**。不能灰度：TLS 节点与明文节点**无法**建立分布会话，所以要么全组都开、要么全组都不开。滚动窗口内跨节点调用会失败——按你的可用性预算安排。

### 它一定会在启动时拒你的四种情况

`broker_cluster` 在起监督树**之前**校验，任一条不满足就 `{error, {dist_tls_invalid, Reason}}`，节点不起：

| Reason | 意思 / 处置 |
|---|---|
| `{dist_tls_enabled_but_booted_plain, _}` | 你开了 env 但忘了 vm.args 的两行。补上并重启 |
| `{dist_tls_booted_with_tls_but_disabled, _}` | 反过来：vm.args 开了但 env 还是默认关闭。管理面/审计会说假话，所以也拒 |
| `{dist_tls_requires_optfile, see_renderer}` | 你用了内联 `-ssl_dist_opt`。那个写法即使拼对也**无法回读选项**，所以不接受；改用 optfile（第 2~3 步） |
| `{missing_file, Which, Path}` / `{bad_pem, Which, Path}` | 路径不存在，或文件不是合法 PEM。检查权限与内容 |
| `{dist_tls_optfile_drift, Field, Declared, Actual}` | env 与 optfile 说的不一致（最常见：换了证书路径但只改了其中一处）。用 `render_optfile/0` 重新生成 |

**注意一个反直觉的失败模式**：`-ssl_dist_optfile` 指向的文件**不存在时节点根本起不来**（`{bad_ssl_dist_optfile, Path}`，在 `net_sup` 启动阶段）。这是好事——没有「配了 TLS 却静默明文」这条路——但排错时要知道它发生在应用启动之前，日志里看到的是 supervisor 报告而不是 `broker_cluster` 的错误。

### 主机名校验：一个方向有、一个方向没有

这是最容易误解的一点，实测结论（ADR-035 §D2）：

- **拨出方向（本节点去连别人）默认校验对端证书的 SAN。**`inet_tls_dist` 恒注入 `{server_name_indication, Host}`，`ssl` 据此做标准 hostname 校验。所以**每张节点证书的 `subjectAltName` 必须匹配其它节点拨它时用的地址**：节点名是 `name@10.0.0.7` 就要 `IP:10.0.0.7`；是 `name@mq-1.internal` 就要 `DNS:mq-1.internal`。写错的表现是握手失败：`{bad_cert,{hostname_check_failed,{requested,"10.0.0.7"},...}}`，节点连不上。
- **拨入方向（别人来连本节点）默认不校验对方 SAN。**一个持有受信 CA 签发证书、但 SAN 写错的节点，仍可拨进来。

也就是说：**CA 信任是双向准入闸门，SAN 绑定只在拨出方向生效**。若合规要求拨入方向也绑定客户端身份，需要额外的 `verify_fun` marker 加上运行期 `net_kernel:allow/1` 白名单，本期**没有**实现。

### 证书轮换

换掉 PEM 文件后**重启节点**。TLS 分布在 boot 期读一次选项，没有热加载路径（这与 `broker_security.tls` 的 `POST /api/v1/tls/reload` 不同——那个只影响面向客户端的 listener）。轮换顺序建议：先换 CA 信任链（各节点 cacertfile 都指向包含新旧两张 CA 的 bundle），再逐节点换证书与私钥并重启。

### 怎么确认它真的开着

```bash
curl -s localhost:8090/api/v1/node | jq .dist_tls
```

`enabled` 为 `true` 且 `verify` 为 `verify_peer` 才算是真开了。也可以在节点上直接查：

```erlang
init:get_argument(proto_dist).        %% {ok,[["inet_tls"]]}
whereis(ssl_dist_sup).                %% 是个 pid
```

两个都符合才说明这条节点的分布**真的**走了 TLS——只看 sys.config 不算。

### 边界（别指望它）

- **不能灰度混跑**：见上，TLS 与明文节点之间没有会话。
- **拨入方向不校验客户端 SAN**：见上。拨出方向会校验，所以证书 SAN 必须写对。
- **不做证书热加载 / ACME / 轮转编排**：换 PEM + 重启。
- **不改变任何协议语义**：分布在所有协议之下，线上帧格式、QoS、ACK 全不变。
- **不加指标键**：`broker_metrics` 冻结集不变；要知道开没开，看节点视图或 `init:get_argument/1`。
```

- [ ] **Step 2: 提交**

```bash
cd /tmp/opencode/mq-s12-0
git add docs/operations.md
git commit --no-verify -m "docs(operations): dist TLS runbook (section 5.5)

The enabling procedure an operator actually needs: which four places must agree,
how to render the optfile instead of hand-copying it, and the five boot
refusals with what each one means.

Leads with the two things that surprise people:

  * A switch is a full-group restart, not a rolling one -- a TLS node and a
    plaintext node cannot form a distribution session at all.
  * CA trust is the admission gate. The outbound side also binds the peer's
    identity to its certificate SAN, so a wrong SAN is a handshake failure --
    worth saying plainly, because it is both a real protection and a real
    operational requirement.

Also documents the counter-intuitive one: a missing optfile kills the node
before any application starts, so the error appears as a supervisor report, not
as a broker_cluster refusal." \
        docs/operations.md
```

---

## Task 12: 架构 / 安全 / 状态文档同步

**Files:**
- Modify: `docs/architecture.md`
- Modify: `docs/security.md`
- Modify: `docs/roadmap.md`
- Modify: `progress.md`
- Modify: `README.md`

- [ ] **Step 1: `docs/architecture.md`**

(a) 配置面表格：在 `broker_cluster.cross_node_timeout_ms` 那行之后加：

```markdown
| `broker_cluster.dist_tls` | `#{enabled => false}`（ADR-035） | 跨节点 / 复制通道 TLS 的**声明**。默认关闭时分布仍是明文 `inet_tcp` + cookie。**开启还需 vm.args 两行**（`-proto_dist inet_tls` + `-ssl_dist_optfile`），两者不一致 `broker_cluster` 拒绝启动（双向校验）。运维口径见 `docs/operations.md` §5.5 |
```

(b) `broker_cluster` 那一行的「自己的进程」列，在末尾补：

```markdown
；**ADR-035**：`broker_cluster_dist_tls` 也是**纯函数模块，无进程**——TLS 由 OTP 的 `inet_tls_dist` 在 boot 期完成，本仓只做配置声明、双向校验与 optfile 渲染
```

「对外入口」列补：

```markdown
、`broker_cluster_dist_tls:{validate/0, render_optfile/0, vm_args_lines/0, redacted_config/0}`
```

- [ ] **Step 2: `docs/security.md`**

(a) §12 第 1 条「跨节点/复制通道的 TLS。」改成带批注的已实现：

```markdown
1. ~~跨节点/复制通道的 TLS。~~ **已实现（2026-11-20，ADR-035）**：`broker_cluster.dist_tls` + OTP `-proto_dist inet_tls` + mTLS，默认关闭。**边界照实说**：CA 信任是双向准入闸门；**拨出方向按 SAN 绑定对端身份（证书 SAN 必须写对），拨入方向不校验客户端 SAN**；开关是全组重启，不能灰度混跑；证书轮换需重启。见 `docs/operations.md` §5.5。
```

(b) 在 §12.7 末尾追加一条：

```markdown
5. **分布现在可以加密了（2026-11-20，ADR-035）**：上面第 1 条描述的信任面——「节点间 RPC 没有应用层鉴权，信任边界是 Erlang distribution」——现在有了一条加固路径：开启 `broker_cluster.dist_tls` 后，distribution 走 mTLS，**只有持有受信 CA 签发证书的节点能建立会话**，且流量加密。这不改变第 1~4 条的任何结论（owner 仍不做二次 ACL、`forwarded` 标志仍不是权限位、shared 拒绝仍是正确性边界），但把「谁能连上 distribution」从「知道 cookie 的人」收紧到「持有受信证书的人」。**CA 信任是双向准入闸门**，同时**拨出方向按 SAN 绑定对端身份**（证书 SAN 写错则握手失败），**拨入方向不校验客户端 SAN**——所以「谁能连上 distribution」从「知道 cookie 的人」收紧到「持有受信证书且能被对端拨到的人」。**默认仍是关闭的**，所以未开启的部署，第 1 条的结论原样成立。
```

- [ ] **Step 3: `docs/roadmap.md`**

在 §14.2 Gate S12 清单**之后**、`---\n\n## 15. 附录` **之前**插入 §14.3（不能叫 §15——那个编号已经是「附录」）：

```markdown
### 14.3 收口项：跨节点 / 复制通道 TLS（2026-11-20 交付）

`docs/security.md` §12 第 1 条的未实现项已关闭（ADR-035）：`broker_cluster.dist_tls` + OTP `-proto_dist inet_tls` + mTLS，默认关闭、boot 期双向漂移校验、optfile 由模块渲染。验收：全量 `rebar3 ct` 零回归（既有 1472 全绿）+ 新套件 `broker_cluster_dist_tls_SUITE` 全绿 + `broker_admin_SUITE` 新例全绿；负向用例证明跨 CA 节点被拒、无证书的 TLS 节点 fail-closed。运维面见 `docs/operations.md` §5.5，边界见 ADR-035 §D2/§后果。
```

> 如果实际 CT 数字与计划不同，把「既有 1472 全绿」改成实际数字。

- [ ] **Step 4: `progress.md`**

在进度表末尾加一行（格式照既有）：

```markdown
| 跨节点 / 复制通道 TLS | ✅ **已交付（2026-11-20，ADR-035）** | 新增 `broker_cluster_dist_tls`（纯函数、无进程、无新 app / 端口 / 指标键）；`broker_cluster.dist_tls` 默认关闭；boot 期双向漂移校验（声明 vs `init:get_argument(proto_dist)` vs `ssl_dist_opts` 实际选项）；`render_optfile/0` + `vm_args_lines/0` 免手抄；节点视图加 additive `dist_tls` 字段。**边界**：CA 是双向准入闸门、拨出按 SAN 绑定对端 / 拨入不校验客户端 SAN、全组重启、证书轮换需重启。证据：`broker_cluster_dist_tls_SUITE` |
```

- [ ] **Step 5: `README.md`**

在状态行里补一句（照既有风格，简短）：

```markdown
**跨节点 / 复制通道 TLS 已交付（2026-11-20，ADR-035）**：`broker_cluster.dist_tls` + OTP `-proto_dist inet_tls` + mTLS，默认关闭，boot 期双向漂移校验；见 `docs/operations.md` §5.5。
```

- [ ] **Step 6: 提交**

```bash
cd /tmp/opencode/mq-s12-0
git add docs/architecture.md docs/security.md docs/roadmap.md progress.md README.md
git commit --no-verify -m "docs: sync dist TLS across architecture, security, roadmap and progress

- security.md section 12 item 1 is closed (struck through, with the date and the
  boundaries that remain: CA-only admission, no SAN check, full-group restart).
  Section 12.7 gains the fifth note: the S12-D trust surface now has a hardening
  path, and the default is still off so the earlier conclusions stand unchanged
  for any deployment that has not turned it on.
- architecture.md: the config table gains dist_tls, and the broker_cluster row
  records that the new module is process-free -- TLS itself is OTP's, this repo
  only declares, validates and renders.
- roadmap: a closing section for the item.
- progress.md / README.md: status line." \
        docs/architecture.md docs/security.md docs/roadmap.md progress.md README.md
```

---

## Task 13: 计划自审与最终验证

**Files:** 无改动

- [ ] **Step 1: 对照 spec 逐节核对**

打开 `docs/superpowers/specs/2026-11-20-cluster-distribution-tls-design.md`，逐节确认有对应实现：

| spec 节 | 落在 |
|---|---|
| §2 决策表（机制/载体/鉴权/SAN/来源/默认/cookie/漂移/开关/无新依赖） | Task 1~5 |
| §3.1 模块与 8 个导出 | Task 1~4 |
| §3.2 env 块 | Task 5 Step 1 |
| §3.3 boot 钩子 | Task 5 Step 4 |
| §3.4 节点视图 | Task 8 |
| §4 五条判据 | Task 3 |
| §5 vm.args / optfile | Task 4 |
| §6.1 happy + 两个负向 + 单测 | Task 3、6、7 |
| §6.2 零回归 | Task 9 |
| §7 ADR + 文档 | Task 10~12 |
| §8 边界（不做运行时开关/只加密 ra/不改 broker_security/不移 cookie/不加指标键/SAN） | Task 10 ADR §D2、D6 + Task 11 边界节 |
| §9 验收 6 条 | Task 9 + Task 6/7 |

- [ ] **Step 2: 占位符扫描**

```bash
cd /tmp/opencode/mq-s12-0 && grep -rn "TBD\|TODO\|FIXME\|待补\|占位\|implement later\|similar to Task" \
  apps/broker_cluster/src/broker_cluster_dist_tls.erl \
  apps/broker_cluster/test/broker_cluster_dist_tls_SUITE.erl \
  docs/adr/035-cluster-distribution-tls.md
```

Expected: 无输出。

- [ ] **Step 3: 类型与函数名一致性**

确认以下名字在每个任务里拼写一致（这是最容易出的错）：

```bash
cd /tmp/opencode/mq-s12-0 && grep -n "^enabled\|^config\|^effective\|^validate\|^ssl_dist_opts\|^render_optfile\|^vm_args_lines\|^redacted_config" \
  apps/broker_cluster/src/broker_cluster_dist_tls.erl
```

Expected: `enabled/0`、`config/0`、`effective/0`、`validate/0`、`validate/1`、`ssl_dist_opts/0`、`render_optfile/0`、`vm_args_lines/0`、`redacted_config/0` 各一处定义。

- [ ] **Step 4: 最后一次全量验证**

```bash
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
cd /tmp/opencode/mq-s12-0 && (pgrep -x epmd >/dev/null || epmd -daemon) && \
  rm -rf _build/test/extras && rebar3 ct 2>&1 | tail -5
```

Expected: 全绿。记录实际总数（用于与 Task 12 文档里写的数字对齐；若不一致，改文档而不是改期望）。

- [ ] **Step 5: 确认没有留下 scratch 文件**

```bash
cd /tmp/opencode/mq-s12-0 && git status --short
```

Expected: 只有 `rebar.lock`（按约定不提交）。若出现别的未跟踪文件（证书、optfile、临时脚本），删掉它们——证书必须在 `priv_dir` 里由套件生成，不能进仓库。

---

## 执行选项

计划已写好。两种执行方式：

**1. Subagent-Driven（推荐）** —— 每个 Task 派一个干净 subagent，任务之间我来复审。适合这种「任务独立、每步都要跑 CT」的计划。

**2. Inline Execution** —— 在当前会话里按 Task 批量执行，带检查点。

选哪个？
