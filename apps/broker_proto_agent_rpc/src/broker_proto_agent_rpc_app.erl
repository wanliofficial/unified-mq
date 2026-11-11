%% @doc `broker_proto_agent_rpc' is a pure library: JSON-RPC 2.0 envelopes,
%% HTTP/1.1 request/response framing, SSE writers, version negotiation and
%% credential helpers shared by the three agent adapters (A2A / MCP / ACP).
%%
%% It has no supervisor, no listener and no configuration of its own -- the same
%% posture as `broker_ws' (ADR-026 §D1). The dependency direction is fixed at
%% `adapter -> base -> kernel', so this module starts nothing and owns nothing.
-module(broker_proto_agent_rpc_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    {ok, self()}.

stop(_State) ->
    ok.
