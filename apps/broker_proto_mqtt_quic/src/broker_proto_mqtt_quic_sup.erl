%% @doc Supervisor for the MQTT over QUIC front end.
%%
%% One child kind: the listener. `one_for_all' with a low intensity, like every
%% other protocol app -- a listener that cannot bind (or cannot find a
%% certificate, or has no `quicer') is a boot failure for *this app only*, and a
%% listener that dies later takes its connections down rather than leaving
%% orphaned streams on a UDP port nobody reads.
%%
%% The QUIC connections themselves are not supervised children: quicer owns the
%% connection and stream handles, and the per-connection `mqtt_conn_fsm' is a
%% child of `broker_proto_mqtt''s existing `mqtt_conn_sup' (S8-A made that
%% possible on purpose). This app supervises exactly one thing, which is why the
%% failure of one connection can never look like a failure of the listener.
-module(broker_proto_mqtt_quic_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Children = [listener_spec(S) || S <- listeners()],
    {ok, {#{strategy => one_for_all, intensity => 1, period => 5}, Children}}.

listeners() ->
    application:get_env(broker_proto_mqtt_quic, listeners, []).

listener_spec(Spec) when is_map(Spec) ->
    Port = maps:get(port, Spec),
    #{id => {mqtt_quic_listener, Port},
      start => {mqtt_quic_listener, start_link, [Spec]},
      restart => transient,
      shutdown => 5000,
      type => worker,
      modules => [mqtt_quic_listener]}.
