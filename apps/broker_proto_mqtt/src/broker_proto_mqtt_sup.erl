-module(broker_proto_mqtt_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

%% The knobs a listener carries down to each connection (L20's seam). Everything
%% else in a listener map is listener-level: `port`, `ref`, `num_acceptors`,
%% `max_connections`.
-define(CONN_KEYS, [mqtt_max_qos, max_keepalive_ms, pull_page_limit,
                    max_inflight, pull_poll_interval_ms, inflight_snapshot_ms,
                    pull_mailbox_limit, publish_mailbox_limit]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% Boot step 4 of design §6: the ranch listener is started only here, after
%% storage recovery and the subscription/index rebuild performed by the apps this
%% one depends on. Nothing may accept CONNECT before that. A listener spec that
%% cannot bind fails `init/1', and therefore the application — the same
%% all-or-nothing posture as boot step 1.
init([]) ->
    Children = [conn_sup_spec() | [listener_spec(S) || S <- listeners()]],
    {ok, {#{strategy => one_for_all, intensity => 1, period => 5}, Children}}.

conn_sup_spec() ->
    {mqtt_conn_sup,
     {mqtt_conn_sup, start_link, []},
     permanent, infinity, supervisor, [mqtt_conn_sup]}.

listeners() ->
    application:get_env(broker_proto_mqtt, listeners, []).

%% `ranch:child_spec/5' embeds the listener under THIS supervisor (via
%% `ranch_embedded_sup') instead of ranch's global `ranch_sup', so stopping
%% broker_proto_mqtt also stops accepting — no listener outlives its app.
%%
%% `restart => transient' matches design §6: a listener that fails on our own
%% watch is restarted, while a normal shutdown (application stop, upgrade) is not
%% resurrected. When it stops, ranch's conns sup sends every connection process a
%% shutdown exit — that is what makes `application:stop' reap the fleet (L15).
listener_spec(Spec) when is_map(Spec) ->
    Port = maps:get(port, Spec),
    Ref = maps:get(ref, Spec, {mqtt, Port}),
    %% S6: TLS is a listener-wide decision; `transport/0' reads the
    %% configured PEM paths and returns ranch_tcp when disabled.
    {Transport, TlsOpts} = broker_security_tls:transport(),
    TransOpts = #{socket_opts => [{port, Port} | TlsOpts],
                  num_acceptors => maps:get(num_acceptors, Spec, 10),
                  max_connections => maps:get(max_connections, Spec, 1024)},
    ProtoOpts = maps:with(?CONN_KEYS, Spec),
    (ranch:child_spec(Ref, Transport, TransOpts, mqtt_listener, ProtoOpts))#{
        restart => transient
    }.
