%% @doc Supervisor of the WebRTC signaling front end (S10-A).
%%
%% One-for-one, listeners only. Each listener is a ranch-managed acceptor pool
%% (ADR-028: the signaling face is connection state, nothing else -- there is
%% no registry child here yet; the room registry lands in S10-B behind this
%% same supervisor, one child per shard).
-module(broker_proto_webrtc_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Children = [listener_spec(S) || S <- listeners()],
    {ok, {#{strategy => one_for_one, intensity => 1, period => 5}, Children}}.

listeners() ->
    application:get_env(broker_proto_webrtc, listeners, []).

%% `broker_security_tls:transport/0' is the whole TLS story: the signaling
%% listener speaks `wss' exactly when the deployment enabled TLS for the node,
%% with no webrtc-specific switch to keep in sync (same posture as
%% broker_proto_mqtt_ws and broker_proto_a2a).
listener_spec(Spec) when is_map(Spec) ->
    Port = maps:get(port, Spec),
    Ref = maps:get(ref, Spec, {webrtc, Port}),
    {Transport, TlsOpts} = broker_security_tls:transport(),
    Ip = maps:get(ip, Spec, {127, 0, 0, 1}),
    TransOpts = #{socket_opts => [{port, Port}, {ip, Ip} | TlsOpts],
                  num_acceptors => maps:get(num_acceptors, Spec, 10),
                  max_connections => maps:get(max_connections, Spec,
                                              max_connections())},
    (ranch:child_spec(Ref, Transport, TransOpts, webrtc_listener, #{}))#{
        restart => transient
    }.

max_connections() ->
    application:get_env(broker_proto_webrtc, max_connections, 1024).
