%% @doc The ranch 2.x protocol adapter (design §6 boot step 4, §9 T12).
%%
%% There is no new connection seam here: ranch calls `start_link/3' while the
%% accepted socket is still owned by the acceptor, so this module starts the
%% ordinary `mqtt_conn_fsm' through `mqtt_conn_sup:start_conn/2' and tells the
%% caller (ranch's conns sup) about it with a LINK. Ranch then performs
%% `Transport:controlling_process(Socket, Pid)' itself and delivers
%% `{handshake, Ref, Transport, Socket, Timeout}' — the FSM's `awaiting_owner'
%% state consumes that message exactly like the test helper's `connect'
%% (mqtt_client.hrl). One process per connection, no proxy process.
%%
%% Why the explicit link: `mqtt_conn_sup:start_conn/2' starts a child under the
%% *supervisor*, so the returned pid is linked to that supervisor, not to the
%% process that happened to call `start_conn/2'. Ranch's connection accounting
%% and, more importantly, its graceful listener shutdown work by sending exit
%% signals to the pid returned from this callback (`ranch_conns_sup' watches
%% `{'EXIT', Pid, _}'). Without the second link, `application:stop' would leave
%% every connection alive — L15's leak — instead of closing them with the
%% listener.
%%
%% The per-listener map (`ProtoOpts' below) is the L20 seam: the six connection
%% knobs travel with the listener and are read through `mqtt_conn_fsm:opt/3',
%% falling back to the app env for connections that did not come from ranch.
-module(mqtt_listener).
-behaviour(ranch_protocol).

-export([start_link/3]).

-spec start_link(ranch:ref(), module(), map()) -> {ok, pid()} | {error, term()}.
start_link(_Ref, _Transport, Opts) ->
    case mqtt_conn_sup:start_conn(undefined, Opts) of
        {ok, Pid} ->
            true = link(Pid),
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.
