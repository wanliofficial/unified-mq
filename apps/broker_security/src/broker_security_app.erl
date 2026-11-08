-module(broker_security_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    %% S6: a TLS block that is enabled but missing its PEM files must fail the
    %% application at boot, not the first handshake. Disabled TLS is always
    %% valid (the pre-S6 deployment keeps working unchanged).
    case broker_security_tls:validate() of
        ok -> broker_security_sup:start_link();
        {error, Reason} -> {error, Reason}
    end.

stop(_State) ->
    ok.
