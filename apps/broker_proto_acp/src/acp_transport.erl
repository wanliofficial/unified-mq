%% @doc The seam between the ACP client and a live agent process (S9-E).
%%
%% Exactly one thing varies between production and tests: who owns the pipe. In
%% production `acp_port' wraps `open_port({spawn_executable, ...})' over the
%% agent's stdin/stdout. In the suite `acp_fake_transport' answers in memory, so
%% the protocol state machine is exercised without a subprocess, a pipe or a
%% pseudo-terminal (the same posture `mcp_stdio' took for MCP).
%%
%% Contract: `open/1' returns `{Mod, Inner}' where `Mod' implements the callbacks
%% below; `send/2' writes one frame; inbound data arrives in the owner's mailbox
%% as
%%   `{acp_transport, Handle, {data, Binary}}'   -- bytes, possibly partial
%%   `{acp_transport, Handle, closed}'            -- EOF or abnormal exit
%% Frames are newline-delimited, so the client buffers partial reads.
-module(acp_transport).

-export([open/1, send/2, close/1]).

-export_type([handle/0]).

-type handle() :: {module(), term()}.

-callback open(Spec :: map()) -> {ok, term()} | {error, term()}.
-callback send(Inner :: term(), Frame :: iodata()) -> ok | {error, term()}.
-callback close(Inner :: term()) -> ok.

%% @doc Open the transport named by the agent spec. `transport' defaults to
%% `acp_port' (a real subprocess); a test supplies its own module.
-spec open(map()) -> {ok, handle()} | {error, term()}.
open(Spec) ->
    Mod = maps:get(transport, Spec, acp_port),
    case Mod:open(Spec) of
        {ok, Inner} -> {ok, {Mod, Inner}};
        {error, _} = Error -> Error
    end.

-spec send(handle(), iodata()) -> ok | {error, term()}.
send({Mod, Inner}, Frame) ->
    Mod:send(Inner, Frame).

-spec close(handle()) -> ok.
close({Mod, Inner}) ->
    Mod:close(Inner).
