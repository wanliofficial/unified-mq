%% @doc ranch 2.x protocol adapter for the MCP endpoint (ADR-027 §D1/§D7).
%%
%% One process per connection, exactly like the A2A front end (ADR-026 §D2):
%% the process takes ownership of the socket with `ranch:handshake/1' and then
%% loops in `mcp_conn', so keep-alive costs no extra process and a dropped
%% client only takes its own connection down.
-module(mcp_listener).
-behaviour(ranch_protocol).

-export([start_link/3]).

-spec start_link(ranch:ref(), module(), map()) -> {ok, pid()} | {error, term()}.
start_link(Ref, Transport, Opts) ->
    Pid = spawn_link(fun() -> mcp_conn:start(Ref, Transport, Opts) end),
    {ok, Pid}.
