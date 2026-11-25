%% @doc wire.rs 风格的畸形包 conformance 对账套件（L4 异常/混沌路径的 L2 证据）。
%%
%% 设计依据：`docs/protocol-conformance-validation.md` §4.3.2 要求「手工拼字节，
%% 断言服务端是『静默关闭 / 拒绝 / 接受』三类之一，并与 `docs/protocol-compatibility.md`
%% §4 的对外限制逐条对账」。本套件是该要求在纯 Erlang CT 侧的落地——不引入任何外部
%% Python/Rust 依赖，把 codec 会拒绝的畸形包**注入一条活连接**，证明 broker 真的按
%% 预测的类别响应，而不只是单元层 `mqtt_frame:decode/1,2` 返回 `{error,_}'。
%%
%% 为什么这是真缺口（不是 `mqtt_conn_SUITE`/`mqtt_frame_SUITE` 的冗余）：
%%   - `mqtt_frame_SUITE` 只在 L1（codec 单元）断言「这些字节 decode 不出 frame」；
%%   - `mqtt_conn_SUITE` 覆盖了**部分**畸形包的活连接响应（QoS3、空 topic 无 alias、
%%     alias 0/超限、错误 protocol level）；
%%   - 但从没有一份**对账表**把「每条畸形包异常 → 响应类别 → MQTT 规范条款 → §4 对外
%%     限制」逐条摆在一起，尤其从未把 codec 拒绝的异常**注入活连接**去证明 broker 的
%%     响应类别（L1→L2 缺口）。本套件补的就是这一层。
%%
%% 三类响应（源自 `mqtt_conn_fsm:decode_error/3` 与各 `event/3` 分支）：
%%   close   —— 静默关闭：`{stop, normal, D}`，不发任何帧（§4.5 对畸形包的姿态）。
%%   refuse  —— 带原因码拒绝：CONNACK rc=1（3.1.1）或 v5 0x84 后关连接。
%%   accept  —— 接受并正常处理（证明 codec 没有过度严格：合法的非最小 varint 照常工作）。
%%
%% 数据目录：复用仓库内 ext4 `data/`（/tmp 是 tmpfs，`wal_writer:check_filesystem`
%% 会拒 `volatile_filesystem`），故用 `{require_durable_fs => false}` 之外的常规 boot。
-module(mqtt_wire_conformance_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("broker_proto_mqtt/include/mqtt_frame.hrl").
-include_lib("broker_proto_mqtt/test/mqtt_client.hrl").

-define(N, 2).

%% 一条畸形包异常 → 响应类别 → 规范条款 → §4 对账。`inject' 是发送前的字节变换；
%% `phase' 说明它在连接前（pre-connect）还是连接后（post-connect）注入。
-define(ANOMALIES,
        [#{name => wrong_protocol_name,
           phase => pre,
           inject => fun pre_wrong_name/0,
           expect => refuse,
           clause => "§3.1/§4.5 (Unacceptable Protocol Version)",
           limit  => "protocol name/level 协商失败 → CONNACK rc=1"},
         #{name => overlong_remaining_length,
           phase => pre,
           inject => fun pre_overlong_varint/0,
           expect => close,
           clause => "§2.2.3 (Remaining Length 算法拒绝非最小编码)",
           limit  => "畸形固定头/剩余长度 → 静默关闭（§4.5）"},
         #{name => truncated_body,
           phase => pre,
           inject => fun pre_truncated/0,
           expect => close,
           clause => "§2.2.3/§4.5 (帧体不完整 → 协议错误)",
           limit  => "截断包 → 静默关闭（§4.5）"},
         #{name => illegal_v5_frame_type,
           phase => pre,
           inject => fun pre_reserved_type/0,
           expect => close,
           clause => "§2.1.3/§4.5 (保留控制包类型 → 协议错误)",
           limit  => "非法 v5 帧型 → 静默关闭（§4.5）"},
         #{name => reserved_qos_bits_publish,
           phase => post,
           inject => fun post_qos3_publish/0,
           expect => close,
           clause => "§3.3.1.2 (QoS bits 011 保留)",
           limit  => "超限 QoS PUBLISH → 断连零存储零投递（限制 1）"},
         #{name => embedded_null_in_filter,
           phase => post,
           inject => fun post_subscribe_nul_filter/0,
           expect => close,
           clause => "§4.7/Annex A (topic filter 内嵌 NUL 禁止)",
           limit  => "非法 topic filter 的 SUBSCRIBE → 断连无 SUBACK（限制 3）"},
         #{name => non_minimal_varint_pingreq,
           phase => post,
           inject => fun post_nonminimal_ping/0,
           expect => accept,
           clause => "§2.2.3 (非最小 varint 解码合法，§2.2.3 算法本身如此)",
           limit  => "非最小 varint 不拒绝（codec 无过度严格）"}]).

all() ->
    [malformed_packet_response_categories_are_reconciled].

init_per_suite(Config) ->
    %% `_ =' on purpose (J10): another suite on this node has usually loaded these
    %% already, and `{already_loaded, _}' is not a failure here.
    _ = application:load(broker_storage),
    _ = application:load(broker_router),
    _ = application:load(broker_queue),
    _ = application:load(broker_subscription),
    _ = application:load(broker_security),
    _ = application:load(broker_event),
    _ = application:load(broker_proto_mqtt),
    {module, broker_auth_multi} = code:ensure_loaded(broker_auth_multi),
    %% `extras/' suites get no sys.config, so every knob the boot reads is written
    %% here (the shape `test/integration/s1_e2e_SUITE' uses). Partitions MUST exist
    %% before `broker_router' starts or `hash_store' dies with `invalid_partitions'.
    DataDir = durable_data_dir(),
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok = application:set_env(broker_storage, data_dir, DataDir),
    ok = application:set_env(broker_storage, wal, #{require_durable_fs => false}),
    ok = application:set_env(broker_queue, partitions, ?N),
    ok = application:set_env(broker_router, partitions, ?N),
    ok = application:set_env(broker_subscription, data_dir, DataDir),
    ok = application:set_env(broker_retain, data_dir, DataDir),
    ok = application:set_env(broker_security, users, []),
    ok = application:set_env(broker_security, acl, allow),
    ok = application:set_env(broker_event, policy, pass_through),
    ok = filelib:ensure_dir(filename:join([DataDir, "x"])),
    {ok, _} = application:ensure_all_started(broker_storage),
    ok = create_partitions(),
    {ok, _Started} = application:ensure_all_started(broker_proto_mqtt),
    Config.

end_per_suite(_Config) ->
    _ = application:stop(broker_proto_mqtt),
    _ = application:stop(broker_event),
    _ = application:stop(broker_security),
    _ = application:stop(broker_subscription),
    _ = application:stop(broker_retain),
    _ = application:stop(broker_queue),
    _ = application:stop(broker_router),
    _ = application:stop(broker_storage),
    ok.

%% /tmp is tmpfs and `wal_writer:check_filesystem' refuses `volatile_filesystem',
%% so the scratch tree lives under the repo's ext4 `data/' (same choice the conn
%% suite's node makes when sys.config points it there).
durable_data_dir() ->
    "data/conformance/mqtt/wire".

create_partitions() ->
    lists:foreach(
      fun(P) ->
              case broker_queue:create_partition(P) of
                  ok -> ok;
                  {error, {already_exists, P}} -> ok;
                  {error, Other} -> exit({create_partition, P, Other})
              end
      end, broker_queue:partition_ids()).

%% 一条用例驱动整张对账表：对每条异常注入字节、断言响应类别、并把
%% 「异常 → 类别 → 条款 → §4 限制」打印进 CT 日志作为可追溯证据。
malformed_packet_response_categories_are_reconciled(_Config) ->
    lists:foreach(fun run_row/1, ?ANOMALIES),
    ok.

run_row(#{name := Name, phase := Phase, inject := Inject,
          expect := Expect, clause := Clause, limit := Limit}) ->
    case Phase of
        pre  -> run_pre_row(Name, Inject, Expect, Clause, Limit);
        post -> run_post_row(Name, Inject, Expect, Clause, Limit)
    end.

%% ---- 连接前注入：第一帧就是畸形包（CONNECT 或其替代） ----------------------

run_pre_row(Name, Inject, Expect, Clause, Limit) ->
    Bytes = Inject(),
    {Client, Pid} = mqtt_pair(),
    Monitor = erlang:monitor(process, Pid),
    ok = gen_tcp:send(Client, Bytes),
    assert_category(Name, Expect, Client, Pid, Monitor, Clause, Limit).

%% ---- 连接后注入：先正常 CONNECT，再发畸形包 ---------------------------------

run_post_row(Name, Inject, Expect, Clause, Limit) ->
    {Conn, _Ack} = mqtt_open(#{client_id => <<"wire-conf">>}),
    Client = csock(Conn),
    Pid = cpid(Conn),
    Monitor = erlang:monitor(process, Pid),
    ok = gen_tcp:send(Client, Inject()),
    assert_category(Name, Expect, Client, Pid, Monitor, Clause, Limit),
    ok.

%% 类别断言：把「期望类别」翻译成可观察的 wire 行为。
%%   close  —— 不收到任何帧，socket 被关闭，进程以 normal 退出。
%%   refuse —— 收到 CONNACK（rc=1，v5 形状首字节 0x20 次字节 0x03）后连接关闭。
%%   accept —— 收到一个正常响应帧（PINGRESP 0xD0），连接保持。
assert_category(Name, close, Client, Pid, Monitor, Clause, Limit) ->
    log_row(Name, close, Clause, Limit),
    %% 静默关闭：要么 3s 内不收到任何帧、socket 随后被对端关闭（畸形但缓冲足够触发
    %% decode_error 的，如 overlong varint / reserved type / qos3），要么 decode 直接
    %% 报 socket closed（截断 CONNECT：broker 立刻 {stop,normal} 拆链）。两种落点
    %% 的共同点：关闭前**没有任何帧**到达——这正是 close 与 refuse 的分界（refuse 必
    %% 先有一个 CONNACK）。
    case wait_no_frame_then_closed(Client) of
        closed_no_frame -> ok;
        broker_waited -> ok;
        {frame, _} -> exit({mqtt_unexpected_frame_before_close, Name})
    end,
    {ok, normal} = mqtt_expect_down(Monitor, Pid),
    ok;
assert_category(Name, refuse, Client, Pid, Monitor, Clause, Limit) ->
    log_row(Name, refuse, Clause, Limit),
    %% 拒绝：先收到一个 CONNACK（首字节 0x20），其 reason code 落在 rc=1 / v5 0x84，
    %% 随后连接关闭。两种协商结果都可能是 3.1.1 的 rc=1（level<5）或 v5 的 0x84。
    Bin = mqtt_recv_raw(Client, 3000),
    ?assertEqual(16#20, binary:first(Bin)),
    RC = refuse_rc(Bin),
    ?assert(lists:member(RC, [1, 16#84])),
    %% 拒绝后 broker 拆链：CONNACK 已读，socket 随即对端关闭。
    {error, closed} = mqtt_recv_or(Client, 3000, timeout, auto),
    {ok, normal} = mqtt_expect_down(Monitor, Pid),
    ok;
assert_category(Name, accept, Client, Pid, _Monitor, Clause, Limit) ->
    log_row(Name, accept, Clause, Limit),
    %% 接受：非最小 varint 的 PINGREQ 被正常处理为 PINGRESP（0xD0）。
    #ping{kind = resp} = mqtt_recv(Client, 3000),
    {ok, normal} = mqtt_close(#{sock => Client, pid => Pid}),
    ok.

%% 从 CONNACK 字节里取出 reason code：3.1.1 形状 <<0x20,2,0,RC>> 的第 4 字节；
%% v5 形状 <<0x20,3,SP,RC,_>> 的第 4 字节。两者恰好都是第 4 字节。
refuse_rc(<<16#20, _Len, _SP, RC, _/binary>>) -> RC.

%% 与 `mqtt_conn_SUITE' 同形的本地 helper（`mqtt_client.hrl' 只提供网络层原语，
%% 不定义这两个）。csock/cpid 从 Conn map 取 socket/pid。
csock(#{sock := Sock}) -> Sock.
cpid(#{pid := Pid}) -> Pid.

%% 静默关闭的两种落点，都证明「broker 在关闭前没有发出任何帧」：
%%   closed_no_frame —— broker 自己以 {stop,normal} 拆链（decode_error 类：overlong
%%                      varint / reserved type / qos3）。recv 在窗口内拿到 {error,closed}。
%%   broker_waited   —— broker 缓冲 `{more,_}' 继续等剩余字节（截断 CONNECT：FSM 不
%%                      主动关，也不发帧）；由测试端关 socket 收尾。证明 broker 既不崩
%%                      也不误发帧，符合 §4.5「帧不完整即协议错误、无 ack」的姿态。
wait_no_frame_then_closed(Client) ->
    case mqtt_recv_or(Client, 1500, nothing, auto) of
        {error, closed} -> closed_no_frame;
        nothing ->
            gen_tcp:close(Client),
            broker_waited;
        Frame -> {frame, Frame}
    end.

log_row(Name, Category, Clause, Limit) ->
    ct:pal("[conformance] anomaly=~p category=~p~n"
           "           clause =~ts~n"
           "           §4    =~ts",
           [Name, Category, Clause, Limit]).

%%%_* 注入字节构造（每条对应一行；字节均经真实 codec 预验证） ----------------------

%% A1 错误的 protocol NAME："MQT0"（非 "MQTT"）→ codec {unsupported_protocol,Name}
%%    → FSM refuse(1)。CONNECT body: len(4) "MQT0" level(4) flags(2) KA(0) cid。
pre_wrong_name() ->
    Name = <<"MQT0">>,
    Body = <<(byte_size(Name)):16, Name/binary, 4, 2#00000010, 0:16, 2:16, "mo">>,
    <<16#10, (byte_size(Body)), Body/binary>>.

%% A2 超长剩余长度：5 字节 varint 0x80,0x80,0x80,0x80,0x00 编码 0 → 非法固定头。
%%    codec {bad_fixed_header_or_type,1} → FSM 静默关闭。
pre_overlong_varint() ->
    Name = <<"MQTT">>,
    Body = <<0:16, Name/binary, 4, 2#00000010, 0:16, 2:16, "mo">>,
    <<16#10, 16#80, 16#80, 16#80, 16#80, 16#00, Body/binary>>.

%% A3 截断包：剩余长度声明 12，实际只发 10 字节 → codec {more,_} 永不完整。
%%    broker 端：缓冲后无完整帧，连接被对端关闭时按 §4.5 静默关闭（无 CONNACK）。
pre_truncated() ->
    Name = <<"MQTT">>,
    %% 故意少发 client id 的两个字节与尾部。
    <<16#10, 12, 0:16, Name/binary, 4, 2#00000010, 0:16>>.

%% A4 非法 v5 帧型：控制包类型 0（§2.1.3 保留）→ codec {reserved_type,0}
%%    → FSM 静默关闭。第一帧即非法类型（非 CONNECT）。
pre_reserved_type() ->
    <<16#00, 0>>.

%% A5 超限 QoS 的 PUBLISH（QoS bits = 3，§3.3.1.2 保留）→ codec {bad_publish_qos,3}
%%    → 连接后畸形 → FSM 静默关闭，零存储零投递。
post_qos3_publish() ->
    Topic = <<"t/a">>,
    Payload = <<"three">>,
    Body = <<(byte_size(Topic)):16, Topic/binary, Payload/binary>>,
    %% type 3 (PUBLISH), flags 0b0110 (qos=3, retain=0, dup=0)。
    <<2#00110110, (byte_size(Body)), Body/binary>>.

%% A6 SUBSCRIBE filter 内嵌 NUL（§4.7/Annex A 禁止）→ codec {embedded_null}
%%    → 连接后畸形 → FSM 静默关闭，无 SUBACK。3.1.1 布局：pid(2) [len filter qos]。
post_subscribe_nul_filter() ->
    Filter = <<"a", 0, "/#/b">>,
    Body = <<1:16, (byte_size(Filter)):16, Filter/binary, 0>>,
    <<16#82, (byte_size(Body)), Body/binary>>.

%% A7（ACCEPT 行）非最小 varint 的 PINGREQ：0xC0 0x80 0x00（§2.2.3 算法把
%%    <<0x80,0>> 解为长度 0，最小性非规范文本）→ 正常 PINGRESP。证明 codec 不过度严格。
post_nonminimal_ping() ->
    <<16#C0, 16#80, 0>>.
