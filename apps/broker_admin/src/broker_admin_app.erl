-module(broker_admin_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    %% S6 config audit: one immutable snapshot + hash at boot, so an operator
    %% can prove later which configuration a run started with. Failures never
    %% block boot (the audit is evidence, not a gate).
    _ = broker_config_audit:log(),
    broker_admin_sup:start_link().

stop(_State) ->
    ok.
