%% @doc Supervisor for the MCP front end (ADR-027 §D1).
%%
%% `listeners' defaults to `[]': an operator release ships the app but opens no
%% port until a listener map is configured. The listener embeds in *our*
%% supervisor (ranch 2.3 `ranch_embedded_sup'), so stopping the app stops
%% accepting and closes live connections -- the same posture as
%% `broker_proto_a2a_sup'.
%%
%% The stdio front end, when enabled, is a worker under the same supervisor: it
%% is the process that owns the release's stdin/stdout channel.
-module(broker_proto_mcp_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Children = [listener_spec(Spec) || Spec <- listeners()] ++ stdio_specs(),
    {ok, {#{strategy => one_for_one, intensity => 1, period => 5}, Children}}.

listeners() ->
    application:get_env(broker_proto_mcp, listeners, []).

listener_spec(Spec) when is_map(Spec) ->
    Port = maps:get(port, Spec),
    Ref = maps:get(ref, Spec, {mcp, Port}),
    %% TLS is a listener-wide decision (S6); `transport/0' reads the configured
    %% PEM paths and returns ranch_tcp when disabled.
    {Transport, TlsOpts} = broker_security_tls:transport(),
    Ip = maps:get(ip, Spec, {127, 0, 0, 1}),
    TransOpts = #{socket_opts => [{port, Port}, {ip, Ip} | TlsOpts],
                  num_acceptors => maps:get(num_acceptors, Spec, 10),
                  max_connections => maps:get(max_connections, Spec,
                                              max_connections())},
    (ranch:child_spec(Ref, Transport, TransOpts, mcp_listener, #{}))#{
        restart => transient
    }.

max_connections() ->
    application:get_env(broker_proto_mcp, max_connections, 1024).

%% stdio is disabled by default: a running release must not steal stdin.
stdio_specs() ->
    case application:get_env(broker_proto_mcp, stdio_enabled, false) of
        true ->
            [#{id => mcp_stdio,
               start => {mcp_stdio, start_link, []},
               restart => transient,
               shutdown => 5000,
               type => worker,
               modules => [mcp_stdio]}];
        false ->
            []
    end.
