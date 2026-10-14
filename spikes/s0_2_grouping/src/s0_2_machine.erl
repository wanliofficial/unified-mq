%% S0.2 - Raft state machine shared by every group in the experiment.
%%
%% A minimal counter is enough: we only care about the cost of appending and
%% replicating a command, not its semantics. Mirrors S0.1's machine contract:
%%   - init/1 returns the initial state
%%   - apply/3 is called as apply(Meta, Command, State) -- metadata FIRST
%%   - apply/3 returns {State, Reply} (NOT {reply, Reply, State})
-module(s0_2_machine).

-export([init/1, apply/3]).

init(Args) -> Args.

apply(_Meta, {write, _Key, _Term}, State) ->
    {maps:update_with(counter, fun(C) -> C + 1 end, 1, State), ok};
apply(_Meta, _Command, State) ->
    {State, {error, unknown_command}}.
