%% @doc In-memory transport for the ACP suite (S9-E).
%%
%% It implements the two-callback `acp_transport' contract without a subprocess:
%% every frame the client writes is sent to the test process, which answers by
%% calling `acp_agent:deliver/2' with the agent's bytes. That keeps the real state
%% machine (handshake, correlation, turn fold) under test while removing the pipe,
%% the executable and the platform's line-buffering behaviour.
-module(acp_fake_transport).

-behaviour(acp_transport).

-export([open/1, send/2, close/1]).

open(Spec) ->
    {ok, #{owner => maps:get(fake_owner, Spec)}}.

send(#{owner := Owner}, Frame) ->
    Owner ! {acp_sent, self(), iolist_to_binary(Frame)},
    ok.

close(_Inner) ->
    ok.
