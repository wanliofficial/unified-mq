%% @doc Supervisor for the CoAP front-end.
%%
%% One listener child per configured spec, and nothing else: unlike MQTT-SN
%% there is no per-session process to supervise. CoAP has no session -- a
%% datagram carries its own Message ID, token and options, and the adapter's
%% per-endpoint state (dedup window, Block1 reassembly) is bounded bookkeeping
%% inside the listener, which is also the only UDP socket owner
%% (ADR-022's shape, applied to a request/response protocol).
%%
%% `one_for_all' at 1/5 s like every other protocol app: a listener that cannot
%% bind is a boot failure, and a listener that dies later takes the rest down
%% with it rather than leaving a port nobody reads.
-module(broker_proto_coap_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Children = case enabled() of
                   false -> [];
                   true -> [listener_spec(S) || S <- listeners()]
               end,
    {ok, {#{strategy => one_for_all, intensity => 1, period => 5}, Children}}.

enabled() ->
    application:get_env(broker_proto_coap, enabled, false) =:= true.

listeners() ->
    application:get_env(broker_proto_coap, listeners, []).

%% The UDP socket is opened by the listener itself rather than by a ranch
%% transport: CoAP has one socket for the whole front-end, and a bad bind must
%% fail `init/1' so the app never starts half-listening.
listener_spec(Spec) when is_map(Spec) ->
    Port = maps:get(port, Spec),
    #{id => {coap_listener, Port},
      start => {coap_listener, start_link, [Spec]},
      restart => transient,
      shutdown => 5000,
      type => worker,
      modules => [coap_listener]}.
