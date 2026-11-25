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
            %% S12-B: revocation 注册表仿 admission 的**建表时机**（两者只在
            %% 这一点同构：admission 是纯模块，revocation 另有 gen_server）——
            %% 两张 named 表在 sup 之前由 app master 建好，使任何前端（连接
            %% 进程）在注册表进程存在之前也能读到 `revoked/1'，不会 fail-open。
            %% gen_server 自身只跑 TTL 扫描与死 pid 回收，作为 sup child 与
            %% 应用同生命周期启停。
            ok = broker_revocation:init(),
            %% Tamper-evident audit (S13): the ring buffer is created here too,
            %% before the supervisor, so an authentication event during boot has
            %% a buffer to land in. The writer gen_server is a supervisor child
            %% (`broker_security_sup'), started with the rest of the app; it only
            %% drains the buffer, so it being briefly absent costs dropped events,
            %% never blocked logins.
            ok = broker_audit:init(),
            broker_security_sup:start_link();
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.
