%% @doc ranch 2.x protocol adapter for A2A.
%%
%% One process per connection (ADR-026 §D2). The process takes ownership of the
%% socket with `ranch:handshake/1' and then loops in `a2a_conn', so keep-alive
%% costs no extra process and a dropped client only takes its own connection
%% down.
-module(a2a_listener).
-behaviour(ranch_protocol).

-export([start_link/3]).

-spec start_link(ranch:ref(), module(), map()) -> {ok, pid()} | {error, term()}.
start_link(Ref, Transport, Opts) ->
    Pid = spawn_link(fun() -> a2a_conn:start(Ref, Transport, Opts) end),
    {ok, Pid}.
