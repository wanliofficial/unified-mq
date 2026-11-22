-module(broker_security_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    %% S6: a TLS block that is enabled but missing its PEM files must fail the
    %% application at boot, not the first handshake. Disabled TLS is always
    %% valid (the pre-S6 deployment keeps working unchanged).
    %%
    %% S12-B (ADR-032 D7): the admission counters are created here, before the
    %% supervisor, so no front end can be up before the gate has a number to
    %% enforce. `init/0' reads `broker_security.limits', which is validated
    %% below by `broker_security_tls:validate/0''s neighbour `broker_limits'.
    case broker_security_tls:validate() of
        ok ->
            ok = broker_admission:init(),
            %% S12-B: revocation 注册表与 admission 同构——两张 named 表在 sup
            %% 之前由 app master 建好，使任何前端（连接进程）可能在注册表存在前
            %% 就读到 `revoked/1'。gen_server 自身只跑 TTL 扫描与死 pid 回收，
            %% 作为 sup child 与应用同生命周期启停。
            ok = broker_revocation:init(),
            broker_security_sup:start_link();
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.
