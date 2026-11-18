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
            broker_security_sup:start_link();
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.
