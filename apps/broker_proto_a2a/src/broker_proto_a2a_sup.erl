%% @doc Supervisor for the task registry and the A2A listeners.
%%
%% Child order is the boot contract (ADR-026 §D4): the task-shard registry
%% (which starts boot replay as its last child) exists before any listener can
%% accept a request, so `SendMessage' never races the projection rebuild.
%% `listeners' defaults to `[]': an operator release ships the app but opens no
%% port until a listener map is configured. The listener embeds in *our*
%% supervisor (ranch 2.3 `ranch_embedded_sup'), so stopping the app stops
%% accepting and closes live connections -- the same posture as
%% `broker_proto_ws_sup' / `broker_proto_mqtt_sup'.
-module(broker_proto_a2a_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Children = [registry_spec() | [listener_spec(Spec) || Spec <- listeners()]],
    {ok, {#{strategy => one_for_one, intensity => 1, period => 5}, Children}}.

registry_spec() ->
    #{id => a2a_task_registry,
      start => {a2a_task_registry, start_link, []},
      restart => permanent,
      shutdown => 10000,
      type => supervisor,
      modules => [a2a_task_registry]}.

listeners() ->
    application:get_env(broker_proto_a2a, listeners, []).

listener_spec(Spec) when is_map(Spec) ->
    Port = maps:get(port, Spec),
    Ref = maps:get(ref, Spec, {a2a, Port}),
    %% S6: TLS is a listener-wide decision; `transport/0' reads the configured
    %% PEM paths and returns ranch_tcp when disabled.
    {Transport, TlsOpts} = broker_security_tls:transport(),
    Ip = maps:get(ip, Spec, {127, 0, 0, 1}),
    %% Only the address and port belong in `socket_opts': ranch validates that
    %% list strictly and warns on anything it does not recognize. The per-request
    %% `{packet, http_bin}' / `{active, false}' pair is set by
    %% `agent_rpc_http:read_request/3' itself, which is where the http framing
    %% contract lives (and where it is switched back to `raw' for the body).
    TransOpts = #{socket_opts => [{port, Port}, {ip, Ip} | TlsOpts],
                  num_acceptors => maps:get(num_acceptors, Spec, 10),
                  max_connections => maps:get(max_connections, Spec,
                                              max_connections())},
    (ranch:child_spec(Ref, Transport, TransOpts, a2a_listener, #{}))#{
        restart => transient
    }.

max_connections() ->
    application:get_env(broker_proto_a2a, max_connections, 1024).
