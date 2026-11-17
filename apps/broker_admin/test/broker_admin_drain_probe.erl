%% @doc Minimal ranch protocol used by `broker_admin_SUITE' to prove what
%% `broker_drain' actually does to a live listener.
%%
%% It echoes bytes and holds the connection open until the peer closes. That is
%% enough to answer the two questions D2 hinges on:
%%
%%   * after a drain, can a *new* connection still be accepted? (must not be)
%%   * after a drain, does an *existing* connection keep working? (must be)
%%
%% A stub listener is used deliberately: the drain mechanism is
%% `ranch:suspend_listener/1', which is protocol-agnostic, so testing it through
%% the MQTT stack would only add unrelated machinery to the failure path.
-module(broker_admin_drain_probe).
-behaviour(ranch_protocol).

-export([start_link/3, init/2]).

start_link(Ref, Transport, _ProtoOpts) ->
    Pid = proc_lib:spawn_link(?MODULE, init, [Ref, Transport]),
    {ok, Pid}.

init(Ref, Transport) ->
    {ok, Socket} = ranch:handshake(Ref),
    loop(Transport, Socket).

loop(Transport, Socket) ->
    case Transport:recv(Socket, 0, infinity) of
        {ok, Data} ->
            _ = Transport:send(Socket, Data),
            loop(Transport, Socket);
        {error, _Reason} ->
            ok
    end.
