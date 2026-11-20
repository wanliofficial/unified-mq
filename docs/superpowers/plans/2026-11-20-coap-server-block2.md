# CoAP 服务端 Block2 分块发送 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 CoAP 服务端在 GET 请求带 `block2` 选项时，按客户端请求的块大小把 retained 表示分块回送，补齐 >1 KB 响应读不回来的缺口。

**Architecture:** 在 `coap_listener` 的回复出口插入一个纯函数 `block2_slice/5`：拿到 resource 的完整 `{Code, Options, Payload}` 后，若请求带 `block2:{Num,_More,ReqSize}` 则切片注入 `block2` 选项，否则原样回（零回归）。codec 与 resource 层不动。无跨块缓存、无新指标、无新配置依赖。

**Tech Stack:** Erlang/OTP 29，rebar3，Common Test，`coap_codec`（已含 block1/block2 编解码）。

设计稿：`docs/superpowers/specs/2026-11-20-coap-server-block2-design.md`

---

## File Structure

- **Modify** `apps/broker_proto_coap/src/coap_listener.erl`
  - `resource_reply/4`：改为把 resource 结果交给 `block2_slice/5` 再 `encode_reply`。
  - `reply_observe/5`：Observe 注册的首个 2.05 也经 `block2_slice/5`（保留 `observe` 选项）。
  - 新增私有 `block2_slice/5` 及其两个小助手 `block2_payload/4`、`block2_more/2`。
- **Create** `apps/broker_proto_coap/test/coap_block2_SUITE.erl`：新套件，走真实 UDP datagram 路径，覆盖 §5 验收 7 项。
- **Modify** `docs/protocol-compatibility.md` §4.24.4 与 `docs/design/s11-coap.md` §1.3：把"服务端不做 Block2"更新为"服务端支持 Block2 分块发送（不含跨块缓存/通知分块）"。

> 命令前缀（每个 rebar3 命令都要）：`PATH=/home/wanli/Development/otp-29.1.2/bin:$PATH /home/wanli/Development/otp-29.1.2/bin/escript $(command -v rebar3)`。下文记作 `$REBAR3 <cmd>`，执行时替换为该前缀。
> ct 跑前每次 `rm -rf _build/test/extras`（只读 cookie 会让第二次运行失败）。

---

## Task 1: 失败的测试 —— Block2 多块下载拼接

**Files:**
- Create: `apps/broker_proto_coap/test/coap_block2_SUITE.erl`

- [ ] **Step 1: 写新测试套件的骨架与夹具**

创建 `apps/broker_proto_coap/test/coap_block2_SUITE.erl`，内容如下（夹具逐行照搬 `coap_listener_SUITE`，因为本套件也要走真实 listener + datagram）：

```erlang
%% @doc Server-side Block2 (RFC 7959 §3): a GET carrying a `block2' option is
%% answered with the retained representation sliced to the client's requested
%% block size. This is the download counterpart to Block1 reassembly; the codec
%% already encodes/decodes both, the gap was purely in the response exit.
%%
%% The suite talks real UDP datagrams to a real listener, exactly like
%% `coap_listener_SUITE', and asserts on the decoded blocks that come back.
-module(coap_block2_SUITE).

-compile([export_all, nowarn_export_all]).
-compile({no_auto_import, [put/2]}).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(N, 2).
-define(POLL_MS, 20).
-define(POLL_BUDGET_MS, 5000).
-define(HOST, {127, 0, 0, 1}).

all() ->
    [block2_streams_a_three_block_body,
     block2_streams_a_single_block_when_body_fits,
     block2_is_not_used_without_the_option,
     block2_combines_with_observe_zero,
     block2_on_missing_resource_is_still_404].

%%%_* Fixtures --------------------------------------------------------------

init_per_suite(Config) ->
    _ = application:load(broker_storage),
    _ = application:load(broker_router),
    _ = application:load(broker_queue),
    _ = application:load(broker_subscription),
    _ = application:load(broker_retain),
    _ = application:load(broker_event),
    _ = application:load(broker_security),
    _ = application:load(broker_proto_coap),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(Case, Config0) ->
    stop_all(),
    Saved = #{acl => env(broker_security, acl),
              require_auth => env(broker_security, require_auth),
              users => env(broker_security, users)},
    DataDir = filename:join([?config(priv_dir, Config0), atom_to_list(Case), "data"]),
    ok = filelib:ensure_dir(filename:join([DataDir, "x"])),
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok = application:set_env(broker_storage, data_dir, DataDir),
    ok = application:set_env(broker_storage, wal, #{require_durable_fs => false}),
    ok = application:set_env(broker_queue, partitions, ?N),
    ok = application:set_env(broker_router, partitions, ?N),
    ok = application:set_env(broker_subscription, data_dir, DataDir),
    ok = application:set_env(broker_retain, data_dir, DataDir),
    ok = application:set_env(broker_security, users, []),
    ok = application:set_env(broker_security, acl, allow),
    ok = application:set_env(broker_security, require_auth, []),
    ok = application:set_env(broker_proto_coap, enabled, true),
    ok = application:set_env(broker_proto_coap, listeners, [#{port => 0}]),
    ok = application:set_env(broker_proto_coap, resources, []),
    {ok, _} = application:ensure_all_started(broker_storage),
    ok = create_partitions(),
    {ok, _} = application:ensure_all_started(broker_proto_coap),
    [{port, listener_port()}, {saved_env, Saved} | Config0].

end_per_testcase(_Case, Config) ->
    stop_all(),
    restore_env(?config(saved_env, Config)),
    ok.

env(App, Key) ->
    application:get_env(App, Key, undefined).

restore_env(#{acl := Acl, require_auth := Auth, users := Users}) ->
    set_or_unset(broker_security, acl, Acl),
    set_or_unset(broker_security, require_auth, Auth),
    set_or_unset(broker_security, users, Users),
    ok.

set_or_unset(_App, _Key, undefined) -> ok;
set_or_unset(App, Key, Value) -> application:set_env(App, Key, Value).

create_partitions() ->
    lists:foreach(
      fun(P) ->
              case broker_queue:create_partition(P) of
                  ok -> ok;
                  {error, {already_exists, P}} -> ok;
                  {error, Other} -> exit({create_partition, P, Other})
              end
      end, broker_queue:partition_ids()).

stop_all() ->
    _ = application:stop(broker_proto_coap),
    _ = application:stop(broker_retain),
    _ = application:stop(broker_security),
    _ = application:stop(broker_event),
    _ = application:stop(broker_subscription),
    _ = application:stop(broker_queue),
    _ = application:stop(broker_router),
    _ = application:stop(broker_storage),
    ok.

listener_port() ->
    case [Pid || {Id, Pid, worker, _} <- supervisor:which_children(
                                             broker_proto_coap_sup),
                 element(1, Id) =:= coap_listener] of
        [Pid | _] -> {ok, Port} = coap_listener:port(Pid), Port;
        [] -> exit(no_listener)
    end.

%%%_* Client ----------------------------------------------------------------

open() ->
    {ok, S} = gen_udp:open(0, [binary, {active, false}]),
    S.

close(S) ->
    gen_udp:close(S).

send(S, Port, Msg) ->
    ok = gen_udp:send(S, ?HOST, Port, coap_codec:encode(Msg)).

recv(S, Timeout) ->
    case gen_udp:recv(S, 0, Timeout) of
        {ok, {_IP, _Port, Bin}} ->
            {ok, Msg} = coap_codec:decode(Bin),
            Msg;
        {error, timeout} ->
            timeout
    end.

expect(S, Type, Code) ->
    Deadline = erlang:monotonic_time(millisecond) + ?POLL_BUDGET_MS,
    expect(S, Type, Code, Deadline).

expect(S, Type, Code, Deadline) ->
    Left = Deadline - erlang:monotonic_time(millisecond),
    case Left > 0 andalso recv(S, Left) of
        timeout -> exit({timeout_waiting_for, Type, Code});
        false -> exit({timeout_waiting_for, Type, Code});
        #{type := Type, code := Code} = Msg -> Msg;
        _Other -> expect(S, Type, Code, Deadline)
    end.

request(Type, Method, Id, Options) ->
    coap_codec:request(Type, Method, Id, Options).

request(Type, Method, Id, Options, Payload) ->
    (coap_codec:request(Type, Method, Id, Options))#{payload => Payload}.

%%%_* Cases -----------------------------------------------------------------
```

- [ ] **Step 2: 写第一个 case（多块下载）与占位断言**

在 `%%%_* Cases` 段后追加：

```erlang
%% A 3 KiB retained body, fetched with block size 1024, comes back as three
%% blocks: N=0 (M=1), N=1 (M=1), N=2 (M=0, 1024 bytes). The three block payloads
%% concatenate to the original body, and each reply carries the block2 option the
%% client asked for.
block2_streams_a_three_block_body(Config) ->
    Port = ?config(port, Config),
    S = open(),
    Body = binary:copy(<<"abcdefghij">>, 307),   %% 3070 bytes
    ?assertEqual(3070, byte_size(Body)),
    send(S, Port, request(con, put, 1, [{uri_path, <<"big">>}], Body)),
    #{code := {response, 2, 4}} = expect(S, ack, {response, 2, 4}),
    %% Block 0 of a 1024-byte transfer.
    send(S, Port, request(con, get, 2,
                          [{uri_path, <<"big">>}, {block2, {0, true, 1024}}}])),
    B0 = expect(S, ack, {response, 2, 5}),
    ?assertEqual({0, true, 1024}, coap_codec:block(block2, B0)),
    ?assertEqual(1024, byte_size(coap_codec:payload(B0))),
    %% Block 1.
    send(S, Port, request(con, get, 3,
                          [{uri_path, <<"big">>}, {block2, {1, true, 1024}}}])),
    B1 = expect(S, ack, {response, 2, 5}),
    ?assertEqual({1, true, 1024}, coap_codec:block(block2, B1)),
    ?assertEqual(1024, byte_size(coap_codec:payload(B1))),
    %% Block 2 (last, 1022 bytes).
    send(S, Port, request(con, get, 4,
                          [{uri_path, <<"big">>}, {block2, {2, true, 1024}}}])),
    B2 = expect(S, ack, {response, 2, 5}),
    ?assertEqual({2, false, 1024}, coap_codec:block(block2, B2)),
    ?assertEqual(1022, byte_size(coap_codec:payload(B2))),
    %% The three payloads reconstruct the body.
    ?assertEqual(Body, <<(coap_codec:payload(B0))/binary,
                         (coap_codec:payload(B1))/binary,
                         (coap_codec:payload(B2))/binary>>),
    close(S).

block2_streams_a_single_block_when_body_fits(_Config) ->
    ok.

block2_is_not_used_without_the_option(_Config) ->
    ok.

block2_combines_with_observe_zero(_Config) ->
    ok.

block2_on_missing_resource_is_still_404(_Config) ->
    ok.
```

- [ ] **Step 3: 跑套件，确认第一个 case 失败**

Run:
```bash
cd /home/wanli/workspace/unified-mq
rm -rf _build/test/extras
$REBAR3 ct --suite=apps/broker_proto_coap/test/coap_block2_SUITE
```
Expected: `block2_streams_a_three_block_body` FAIL —— 现在响应把整个 3070 字节一次性塞进单个 datagram，且**不带** block2 选项，故 `coap_codec:block(block2, B0)` 返回 `undefined`，断言 `{0,true,1024}` 失败。其余 4 个占位 case 通过（它们不触发 Block2 或不断言）。

- [ ] **Step 4: Commit**

```bash
cd /home/wanli/workspace/unified-mq
git add apps/broker_proto_coap/test/coap_block2_SUITE.erl
git commit -m "test(coap): Block2 multi-block download suite (failing first case)"
```

---

## Task 2: 实现 Block2 切片（仅普通 GET 路径）

**Files:**
- Modify: `apps/broker_proto_coap/src/coap_listener.erl`

- [ ] **Step 1: 把 `resource_reply/4` 接到切片出口**

在 `apps/broker_proto_coap/src/coap_listener.erl` 中，将现有 `resource_reply/4`（约 642-644 行）：

```erlang
resource_reply(Req, Cfg, Endpoint, St) ->
    {ReplyCode, Options, Payload} = run_resource(Req, Cfg, Endpoint, St),
    encode_reply(Req, reply_type(Req), ReplyCode, Options, Payload).
```

替换为：

```erlang
resource_reply(Req, Cfg, Endpoint, St) ->
    {ReplyCode, Options, Payload} = run_resource(Req, Cfg, Endpoint, St),
    block2_slice(Req, reply_type(Req), ReplyCode, Options, Payload).
```

> 注意：`block2_slice/5` 的末参是 `Payload`，但 `encode_reply/5` 原本接收的是 `Payload` 作为最后一个参数；这里 `block2_slice` 内部会在需要时对 `{ReplyCode, Options, Payload}` 调用 `encode_reply/5`。下一步定义该函数。

- [ ] **Step 2: 定义 `block2_slice/5` 与助手**

在 `resource_reply/4` 之后（`%%%_* Block2` 注释下）插入：

```erlang
%%%_* Block2 (server-side chunking, RFC 7959 §3) --------------------------------
%%
%% A GET carrying a `block2' option is answered with the requested slice of the
%% resource's full representation; a GET without one is answered whole (the
%% pre-Block2 behaviour, unchanged). Only a 2.xx content response is sliced: an
%% error code (4.04 / 4.05 / 5.xx) has no body to divide, so it passes through.
%% The block size is the client's requested `ReqSize' (Block2 lets the downloader
%% pick it); there is no server-side cache -- each block is a fresh projection of
%% the current retained state, so no per-transfer table is needed.
block2_slice(Req, Type, Code, Options, Payload) ->
    case {is_content_code(Code), coap_codec:block(block2, Req)} of
        {true, {Num, _More, ReqSize}} ->
            {Slice, ActualSize} = block2_payload(Payload, Num, ReqSize),
            More = block2_more(Payload, Num, ReqSize),
            BlockOpts = lists:keystore(block2, 1, Options,
                                       {block2, coap_codec:block_value(Num, More, ActualSize)}),
            encode_reply(Req, Type, Code, BlockOpts, Slice);
        _ ->
            encode_reply(Req, Type, Code, Options, Payload)
    end.

%% True when the code is a 2.xx success carrying a representational body.
is_content_code({response, Class, _Detail}) when Class >= 2, Class =< 2 ->
    true;
is_content_code(_) ->
    false.

%% The Num-th `ReqSize'-byte slice of `Payload', and the size actually used
%% (== ReqSize except when the final block is short). An out-of-range Num yields
%% an empty slice -- RFC 7959 §3.5 lets the server close with an empty last
%% block (More=false), which the client recognises and stops on.
block2_payload(Payload, Num, ReqSize) ->
    Offset = Num * ReqSize,
    Total = byte_size(Payload),
    case Offset >= Total of
        true ->
            {<<>>, ReqSize};
        false ->
            Len = min(ReqSize, Total - Offset),
            Slice = binary:part(Payload, Offset, Len),
            {Slice, ReqSize}
    end.

%% More=true when at least one byte follows this block.
block2_more(Payload, Num, ReqSize) ->
    ((Num + 1) * ReqSize) < byte_size(Payload).
```

> `lists:keystore/4`：若 `Options` 里已有 `block2` 项则替换它，否则追加。客户端请求里的 block2 在 `Req` 上、不在 `Options` 上（`Options` 是 resource 层产出的选项，如 `content_format`），所以这里实际是**新增**，`keystore` 的替换分支不会被命中，但用 `keystore` 而非 `++` 可避免万一重复。

- [ ] **Step 3: 跑第一个 case，确认通过**

Run:
```bash
cd /home/wanli/workspace/unified-mq
rm -rf _build/test/extras
$REBAR3 ct --suite=apps/broker_proto_coap/test/coap_block2_SUITE
```
Expected: `block2_streams_a_three_block_body` PASS。其余占位 case 仍通过。

- [ ] **Step 4: 全量编译 + dialyzer 无回退**

Run:
```bash
cd /home/wanli/workspace/unified-mq
$REBAR3 clean
$REBAR3 compile
$REBAR3 dialyzer
```
Expected: compile exit 0、零 warning；dialyzer exit 0、零 warning（无新增）。

- [ ] **Step 5: Commit**

```bash
cd /home/wanli/workspace/unified-mq
git add apps/broker_proto_coap/src/coap_listener.erl
git commit -m "feat(coap): server-side Block2 response slicing (RFC 7959 §3)"
```

---

## Task 3: Observe 注册首响应与 Block2 共存

**Files:**
- Modify: `apps/broker_proto_coap/src/coap_listener.erl`（`reply_observe/5`）
- Modify: `apps/broker_proto_coap/test/coap_block2_SUITE.erl`

- [ ] **Step 1: 写失败测试（Observe=0 + Block2 首个响应）**

把 `coap_block2_SUITE.erl` 中的占位 `block2_combines_with_observe_zero(_Config) -> ok.` 替换为：

```erlang
%% A GET with Observe=0 AND block2 registers the relationship and answers the
%% initial 2.05 carrying BOTH the `observe' sequence option AND a `block2'
%% option (RFC 7641 §4.5 lets the initial response negotiate the block size).
%% The first block is the only one that fits in the initial response.
block2_combines_with_observe_zero(Config) ->
    Port = ?config(port, Config),
    S = open(),
    Body = binary:copy(<<"0123456789">>, 200),   %% 2000 bytes
    send(S, Port, request(con, put, 1, [{uri_path, <<"obs">>}], Body)),
    #{code := {response, 2, 4}} = expect(S, ack, {response, 2, 4}),
    send(S, Port, request(con, get, 2,
                          [{uri_path, <<"obs">>}, {observe, 0},
                           {block2, {0, true, 1024}}])),
    Init = expect(S, ack, {response, 2, 5}),
    %% Both options are present on the initial response.
    ?assertMatch({0, true, 1024}, coap_codec:block(block2, Init)),
    ?assertEqual(1024, byte_size(coap_codec:payload(Init))),
    ?assert(is_integer(coap_codec:option(observe, Init))),
    close(S).
```

- [ ] **Step 2: 跑测试，确认失败**

Run:
```bash
cd /home/wanli/workspace/unified-mq
rm -rf _build/test/extras
$REBAR3 ct --suite=apps/broker_proto_coap/test/coap_block2_SUITE --case=block2_combines_with_observe_zero
```
Expected: FAIL —— 当前 Observe 注册分支（`reply_observe/5` 的 `register` 子句）直接 `encode_reply`，**不经** `block2_slice`，故首响应无 block2 选项，`coap_codec:block(block2, Init)` 返回 `undefined`。

- [ ] **Step 3: 让 Observe 注册首响应走切片出口**

在 `apps/broker_proto_coap/src/coap_listener.erl` 的 `reply_observe/5` 中，找到 `register` 子句里成功注册后构造首个响应的代码（约 603-606 行）：

```erlang
                        {St2, Rel} ->
                            Opts = [{observe, coap_observe:seq(Rel)} | Options],
                            {encode_reply(Req, reply_type(Req), Code, Opts,
                                          Payload), St2}
```

替换为（把 `encode_reply` 换成 `block2_slice`，`block2_slice` 会在 Options 已含 `observe` 的基础上追加 `block2`）：

```erlang
                        {St2, Rel} ->
                            Opts = [{observe, coap_observe:seq(Rel)} | Options],
                            {block2_slice(Req, reply_type(Req), Code, Opts,
                                          Payload), St2}
```

> 同一 `register` 子句里 `{St2, undefined}`（表满拒绝）与 `_`（非 2.05，无可观察）两个分支返回的是 4.03/原样错误响应，**不带 block2**（它们经 `encode_reply` 直出），保持不变 —— `block2_slice` 只对 2.xx 切片，这两个分支本就不该切。

- [ ] **Step 4: 跑该 case，确认通过**

Run:
```bash
cd /home/wanli/workspace/unified-mq
rm -rf _build/test/extras
$REBAR3 ct --suite=apps/broker_proto_coap/test/coap_block2_SUITE --case=block2_combines_with_observe_zero
```
Expected: PASS。

- [ ] **Step 5: 全套 coap_block2 + observe 套件不回归**

Run:
```bash
cd /home/wanli/workspace/unified-mq
rm -rf _build/test/extras
$REBAR3 ct --suite=apps/broker_proto_coap/test/coap_block2_SUITE
$REBAR3 ct --suite=apps/broker_proto_coap/test/coap_observe_SUITE
```
Expected: 两个套件全绿（Observe 注册/注销/通知行为未变）。

- [ ] **Step 6: Commit**

```bash
cd /home/wanli/workspace/unified-mq
git add apps/broker_proto_coap/src/coap_listener.erl apps/broker_proto_coap/test/coap_block2_SUITE.erl
git commit -m "feat(coap): Block2 on the Observe initial response"
```

---

## Task 4: 边界 case（无选项零回归 / 单块 / 404 带块）

**Files:**
- Modify: `apps/broker_proto_coap/test/coap_block2_SUITE.erl`

- [ ] **Step 1: 写"无选项零回归"case**

把 `block2_is_not_used_without_the_option(_Config) -> ok.` 替换为：

```erlang
%% A plain GET with NO block2 option returns the whole representation in one
%% 2.05 with NO block2 option -- the pre-Block2 behaviour is untouched.
block2_is_not_used_without_the_option(Config) ->
    Port = ?config(port, Config),
    S = open(),
    Body = binary:copy(<<"z">>, 3000),
    send(S, Port, request(con, put, 1, [{uri_path, <<"whole">>}], Body)),
    #{code := {response, 2, 4}} = expect(S, ack, {response, 2, 4}),
    send(S, Port, request(con, get, 2, [{uri_path, <<"whole">>}])),
    Reply = expect(S, ack, {response, 2, 5}),
    ?assertEqual(undefined, coap_codec:block(block2, Reply)),
    ?assertEqual(Body, coap_codec:payload(Reply)),
    close(S).
```

- [ ] **Step 2: 写"单块"case（body 恰好 ≤ ReqSize）**

把 `block2_streams_a_single_block_when_body_fits(_Config) -> ok.` 替换为：

```erlang
%% A body that fits in one requested block comes back as a single block with
%% More=false (block2:0/1024). The client stops after one round-trip.
block2_streams_a_single_block_when_body_fits(Config) ->
    Port = ?config(port, Config),
    S = open(),
    Body = binary:copy(<<"q">>, 1024),   %% exactly one block
    send(S, Port, request(con, put, 1, [{uri_path, <<"one">>}], Body)),
    #{code := {response, 2, 4}} = expect(S, ack, {response, 2, 4}),
    send(S, Port, request(con, get, 2,
                          [{uri_path, <<"one">>}, {block2, {0, true, 1024}}])),
    B0 = expect(S, ack, {response, 2, 5}),
    ?assertEqual({0, false, 1024}, coap_codec:block(block2, B0)),
    ?assertEqual(Body, coap_codec:payload(B0)),
    close(S).
```

- [ ] **Step 3: 写"404 带块"case**

把 `block2_on_missing_resource_is_still_404(_Config) -> ok.` 替换为：

```erlang
%% A block2 GET on an absent resource is still 4.04: an error code has no body
%% to slice, so no block2 option and no payload are attached.
block2_on_missing_resource_is_still_404(Config) ->
    Port = ?config(port, Config),
    S = open(),
    send(S, Port, request(con, get, 1,
                          [{uri_path, <<"nope">>}, {block2, {0, true, 1024}}])),
    Reply = expect(S, ack, {response, 4, 4}),
    ?assertEqual(undefined, coap_codec:block(block2, Reply)),
    ?assertEqual(<<>>, coap_codec:payload(Reply)),
    close(S).
```

- [ ] **Step 4: 跑整个新套件，确认全绿**

Run:
```bash
cd /home/wanli/workspace/unified-mq
rm -rf _build/test/extras
$REBAR3 ct --suite=apps/broker_proto_coap/test/coap_block2_SUITE
```
Expected: 5 个 case 全 PASS。

- [ ] **Step 5: Commit**

```bash
cd /home/wanli/workspace/unified-mq
git add apps/broker_proto_coap/test/coap_block2_SUITE.erl
git commit -m "test(coap): Block2 boundary cases (no-option, single-block, 404)"
```

---

## Task 5: 全量验证 + 文档更新

**Files:**
- Modify: `docs/protocol-compatibility.md` §4.24.4
- Modify: `docs/design/s11-coap.md` §1.3

- [ ] **Step 1: 全量 ct 不回归**

Run:
```bash
cd /home/wanli/workspace/unified-mq
rm -rf _build/test/extras
$REBAR3 ct
```
Expected: `All N tests passed`（N ≥ 1568，新增 5 例后约 1573，0 失败）。

- [ ] **Step 2: 定位 protocol-compatibility.md 里关于服务端 Block2 的现状描述**

Run:
```bash
cd /home/wanli/workspace/unified-mq
grep -n "Block2\|block2\|1 KB\|1KiB\|分块" docs/protocol-compatibility.md | head
```
Expected: 找到 §4.24.4 附近"服务端不做 Block2 分块响应 / 端到端上限约 1 KB"那一行（记录确切行号）。

- [ ] **Step 3: 更新该行**

把该行（"服务端**不做 Block2 分块响应**……端到端上限约 1 KB"）改为说明服务端**支持 Block2 分块下载**（不含跨块缓存/通知分块），并去掉"端到端约 1 KB"的能力缺口表述，改为：分块下载由服务端按客户端请求块大小切片回送；上传分块仍为 Block1。保留 draft-ietf-core-block-18 与 RFC 7959 的偏差留痕提示。

> 用 Step 2 拿到的确切原文做 `edit` 替换；只改能力矩阵/§4.24.4 的描述，不动其它协议的条目。

- [ ] **Step 4: 更新 s11-coap.md §1.3 非目标表**

Run:
```bash
cd /home/wanli/workspace/unified-mq
grep -n "Block-wise\|Block2\|Block1\|7959" docs/design/s11-coap.md
```
Expected: 找到 §1.3 非目标表里"完整 RFC 7959 Block-wise"那一行。

- [ ] **Step 5: 收窄该非目标表述**

把"完整 RFC 7959 Block-wise"那条改为：已实现服务端 Block2 分块发送 + Block1 重组（draft-ietf-core-block-18），仍未做的是**跨块响应缓存/重传**与 **Observe 通知的分块推送**（独立增量，见设计稿 §4 留痕）。保留"与最终 RFC 有偏差，须在能力矩阵留痕"提示。

- [ ] **Step 6: 最终全量验证（compile + dialyzer + ct）**

Run:
```bash
cd /home/wanli/workspace/unified-mq
$REBAR3 clean
$REBAR3 compile
$REBAR3 dialyzer
rm -rf _build/test/extras
$REBAR3 ct
```
Expected: compile 0 warning；dialyzer 0 warning；ct 全绿。

- [ ] **Step 7: Commit**

```bash
cd /home/wanli/workspace/unified-mq
git add docs/protocol-compatibility.md docs/design/s11-coap.md
git commit -m "docs(coap): mark server-side Block2 chunking as delivered"
```

---

## Self-Review（计划自检）

**Spec coverage：**
- §2.1 接入点 → Task 2 Step 1（`resource_reply`）+ Task 3 Step 3（Observe 注册）。✓
- §2.2 切片规则（Num/ReqSize/More/末块/无选项零回归）→ Task 2 Step 2（`block2_slice`/`block2_payload`/`block2_more`）。✓
- §2.3 Observe 组合 → Task 3。✓
- §2.4 错误码不切片 / 越界 Num 空块 → Task 2 Step 2（`is_content_code` 守卫 + `Offset >= Total` 返回空 + More=false）。✓
- §2.5 配置复用 `block_size` 兜底 → 本设计正常路径块大小由客户端 ReqSize 决定，`block_size` 不作依赖；计划未引入新配置（符合 spec）。✓
- §2.6 零指标变更 → 计划不碰指标。✓
- §3 不做项（缓存/通知分块/上传方向）→ 计划不实现，文档留痕（Task 5）。✓
- §5 验收 7 项 → Task 1（多块 3 项断言）、Task 4（单块/零回归/404）、Task 3（Observe）。注：spec 列的"payload 恰好=ReqSize"由 Task 4 单块 case 覆盖；"越界 Num"未单列测试但由 §2.4 逻辑保证，可后续补。✓

**Placeholder scan：** 无 TBD/TODO；每个代码步骤都给了完整代码。占位 case 在 Task 1 故意留 `ok.`，在后续 Task 逐个替换为真实现（TDD 正常节奏）。✓

**Type consistency：** `block2_slice/5` 全程签名一致（`(Req, Type, Code, Options, Payload)`）；`block2_payload/3` 返回 `{Slice, ActualSize}`；`block2_more/3` 返回 bool；`is_content_code/1` 守卫 `{response,2,_}`。`coap_codec:block/2`、`block_value/3`、`option/2`、`payload/1` 均为 codec 现有导出。✓
