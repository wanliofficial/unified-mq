%% @doc The production transport: the agent is a subprocess the client spawns
%% (convergence report §7.1, "who spawns is the Client").
%%
%% The command is declared by the operator and never by a remote request. We
%% spawn the executable **directly** (`spawn_executable') with no shell, so a
%% configured path with spaces or metacharacters cannot become a command
%% injection. `stderr_to_stdout' is deliberately **not** set: the agent's stderr
%% must never be mistaken for protocol frames (same rule as `mcp_stdio''s
%% stdout-only discipline); it is inherited from the broker, not merged.
%%
%% `open_port/2' requires the executable, argv and environment entries as
%% strings, while the config surface speaks binaries (the same binaries appear
%% on the wire). `to_str/1' is the single conversion point.
-module(acp_port).

-behaviour(acp_transport).

-export([open/1, send/2, close/1]).

open(#{command := Command} = Spec) ->
    Args = [to_str(A) || A <- maps:get(args, Spec, [])],
    Env = [{to_str(K), to_str(V)} || {K, V} <- maps:get(env, Spec, [])],
    %% `{line, N}' emits `{data, {eol, Line}}' per newline and
    %% `{data, {noeol, Chunk}}' for a line longer than N; the client's buffer
    %% joins both shapes, so a long frame degrades to chunked reads instead of
    %% failing. N must be positive (`{line, 0}' is a badarg).
    Opts = [binary, exit_status, use_stdio, {line, 1048576},
            {args, Args}, {env, Env}],
    try
        Port = open_port({spawn_executable, to_str(Command)}, Opts),
        {ok, Port}
    catch
        error:Reason -> {error, {spawn_failed, Reason}}
    end.

send(Port, Frame) ->
    try
        true = port_command(Port, Frame),
        ok
    catch
        error:Reason -> {error, {send_failed, Reason}}
    end.

close(Port) ->
    catch port_close(Port),
    ok.

to_str(Value) when is_binary(Value) -> binary_to_list(Value);
to_str(Value) when is_list(Value) -> Value.
