%% @doc 吊销注册表（MQ-17 #10）单元用例：revoke / clear / revoked 三个动词，
%% 以及 TTL 到期自动解除与永久两种生命周期。
%%
%% 注册表是 `broker_admission' 那套 ETS 模式的同构体：热路径 `revoked/1' 在调用方
%% 进程本地按表名直读，不进 gen_server。用例只断言对外语义——吊销使 principal 命中、
%% clear 解除、TTL 到期自动抬起、无 TTL 永久、未注册者默认不吊销——不关心内部表
%% 结构。`init_per_testcase' 调 `init/0'（**复用**已存在的表，不重建）后紧跟
%% `reset/0' 清空两张表，所以一个用例的吊销不会泄漏到下一个。
-module(broker_revocation_SUITE).

-compile([export_all, nowarn_export_all]).

all() ->
    [revoke_makes_principal_revoked,
     clear_lifts_the_revocation,
     revoke_with_ttl_expires_and_lifts,
     revoke_without_ttl_is_permanent,
     unknown_principal_is_not_revoked,
     register_and_unregister_are_total].

init_per_testcase(_Case, Config) ->
    ok = broker_revocation:init(),
    ok = broker_revocation:reset(),
    Config.

end_per_testcase(_Case, _Config) ->
    ok = broker_revocation:reset(),
    ok.

%% 吊销一个 principal 后，它命中、别人不命中。
revoke_makes_principal_revoked(_Config) ->
    false = broker_revocation:revoked(<<"alice">>),
    ok = broker_revocation:revoke(<<"alice">>, undefined),
    true = broker_revocation:revoked(<<"alice">>),
    false = broker_revocation:revoked(<<"bob">>).

%% clear 显式解除吊销。
clear_lifts_the_revocation(_Config) ->
    ok = broker_revocation:revoke(<<"alice">>, undefined),
    true = broker_revocation:revoked(<<"alice">>),
    ok = broker_revocation:clear(<<"alice">>),
    false = broker_revocation:revoked(<<"alice">>).

%% 带 TTL 的吊销到点自动解除（读时判过期，不依赖扫描循环跑过）。
revoke_with_ttl_expires_and_lifts(_Config) ->
    ok = broker_revocation:revoke(<<"alice">>, 50),
    true = broker_revocation:revoked(<<"alice">>),
    timer:sleep(120),
    false = broker_revocation:revoked(<<"alice">>).

%% 无 TTL 的吊销永久有效，睡一会仍在。
revoke_without_ttl_is_permanent(_Config) ->
    ok = broker_revocation:revoke(<<"alice">>, undefined),
    timer:sleep(120),
    true = broker_revocation:revoked(<<"alice">>).

%% 从未被吊销的 principal 默认不命中（fail-open）。
unknown_principal_is_not_revoked(_Config) ->
    false = broker_revocation:revoked(<<"nobody">>).

%% 登记/注销是全函数：`#st.principal' 在认证前的拒答路径上是 `undefined'，而
%% `terminate/2' 会无条件注销；契约外输入（非 pid）也必须返回 `ok' 而非
%% `function_clause'。
register_and_unregister_are_total(_Config) ->
    ok = broker_revocation:register_session(undefined, self()),
    ok = broker_revocation:unregister_session(undefined, self()),
    ok = broker_revocation:register_session(<<"alice">>, not_a_pid),
    ok = broker_revocation:unregister_session(<<"alice">>, not_a_pid).
