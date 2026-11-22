%% @doc 吊销注册表（MQ-17 #10）单元用例：revoke / clear / revoked 三个动词，
%% 以及 TTL 到期自动解除与永久两种生命周期。
%%
%% 注册表沿用 `broker_admission' 的 ETS 路子（仅建表时机与读写位置相同，
%% admission 是纯模块，此模块另有 gen_server）：热路径 `revoked/1' 在调用方
%% 进程本地按表名直读，不进 gen_server。用例只断言对外语义——吊销使 principal 命中、
%% clear 解除、TTL 到期自动抬起、无 TTL 永久、未注册者默认不吊销——不关心内部表
%% 结构。`init_per_testcase' 调 `init/0'（**复用**已存在的表，不重建）后紧跟
%% `reset/0' 清空两张表，所以一个用例的吊销不会泄漏到下一个。
-module(broker_revocation_SUITE).

-compile([export_all, nowarn_export_all]).

all() ->
    [registry_is_live_after_app_start,
     revoke_makes_principal_revoked,
     clear_lifts_the_revocation,
     revoke_with_ttl_expires_and_lifts,
     revoke_without_ttl_is_permanent,
     unknown_principal_is_not_revoked,
     register_and_unregister_are_total,
     security_facade_delegates].

%% `registry_is_live_after_app_start' 断言 registry 随应用一起起来：app master 在
%% sup 之前建表、gen_server 作为 sup child 运行。其余用例只关心对外动词语义，不
%% 依赖应用是否启动（每个 `init_per_testcase' 都幂等建表），所以这一条单独验证
%% 生命周期接线，其余六条与 `broker_admission_SUITE' 的用例姿态一致。
%%
%% 启停按**所有权**平衡：只停自己起来的 app。`broker_security' 被约 15 个应用
%% 依赖，若在别的套件已经拉起它（或有依赖它的 app 在跑）时无条件 `stop'，会把节点
%% 留在 `{rejected, not_started}' 状态污染后续套件；且重复停会 badmatch。捕获
%% "进来时是否已在跑"，只有 `false -> true' 的那次才归我们停。
init_per_suite(Config) ->
    %% 这一条用例要 `whereis(broker_revocation)' 是 pid，而 gen_server 只由
    %% supervisor 启动、supervisor 只由应用启动，所以整个 suite 需要应用在跑；
    %% 其余用例不依赖它，但无害（建表幂等）。
    WasRunning = lists:keymember(broker_security, 1, application:which_applications()),
    {ok, _} = application:ensure_all_started(broker_security),
    [{stop_on_exit, not WasRunning} | Config].

end_per_suite(Config) ->
    case proplists:get_value(stop_on_exit, Config) of
        true -> ok = application:stop(broker_security);
        false -> ok
    end.

init_per_testcase(_Case, Config) ->
    ok = broker_revocation:init(),
    ok = broker_revocation:reset(),
    Config.

end_per_testcase(_Case, _Config) ->
    ok = broker_revocation:reset(),
    ok.

%% 注册表随应用一起起来：表已存在（app master 在 sup 前 init），且 registry
%% 进程在跑。这条单独验证生命周期接线，独立于其余六个动词语义用例。
registry_is_live_after_app_start(_Config) ->
    %% 表必须已存在（app master 在 sup 前 init），且 registry 进程在跑。
    true = is_pid(whereis(broker_revocation)),
    ok = broker_revocation:revoke(<<"carol">>, undefined),
    true = broker_revocation:revoked(<<"carol">>),
    ok = broker_revocation:clear(<<"carol">>).

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

%% `broker_security' 的门面转发：安全语汇（认证/授权/吊销）集中在同一模块，
%% 调用方不必知道 `broker_revocation' 的存在。这里只断言转发后的对外语义与
%% 直接调用一致，不重复测 TTL 等细节。
security_facade_delegates(_Config) ->
    false = broker_security:revoked(<<"dave">>),
    ok = broker_security:revoke(<<"dave">>, undefined),
    true = broker_security:revoked(<<"dave">>),
    ok = broker_security:clear_revocation(<<"dave">>),
    false = broker_security:revoked(<<"dave">>).
