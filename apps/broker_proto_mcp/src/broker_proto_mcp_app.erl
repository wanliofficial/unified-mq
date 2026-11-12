%% @doc MCP 2026-07-28 front end: the broker as an MCP gateway (S9-D).
%%
%% Boot refusals, both loud (the same two the A2A front end makes):
%%   * `require_auth => false' on a non-loopback bind would expose an
%%     unauthenticated endpoint;
%%   * `stdio_enabled => true' together with a non-loopback HTTP listener is
%%     allowed, but `features' naming an unknown feature is refused -- an
%%     advertised capability that routes nothing is a silent lie.
-module(broker_proto_mcp_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    case validate() of
        ok -> broker_proto_mcp_sup:start_link();
        {error, Reason} -> {error, Reason}
    end.

stop(_State) ->
    ok.

validate() ->
    case valid_features(features()) of
        ok -> check_auth();
        {error, _} = Error -> Error
    end.

valid_features([]) ->
    ok;
valid_features([Feature | Rest]) ->
    case is_known_feature(Feature) of
        true -> valid_features(Rest);
        false -> {error, {unknown_feature, Feature}}
    end;
valid_features(Other) ->
    {error, {invalid_features, Other}}.

is_known_feature(tools) -> true;
is_known_feature(resources) -> true;
is_known_feature(prompts) -> true;
is_known_feature(completions) -> true;
is_known_feature(_) -> false.

check_auth() ->
    case require_auth() of
        true -> ok;
        false -> check_loopback(listeners())
    end.

check_loopback([]) ->
    ok;
check_loopback([Spec | Rest]) when is_map(Spec) ->
    Ip = maps:get(ip, Spec, {127, 0, 0, 1}),
    case is_loopback(Ip) of
        true -> check_loopback(Rest);
        false -> {error, {insecure_listener, maps:get(port, Spec, undefined), Ip}}
    end;
check_loopback([_Other | Rest]) ->
    check_loopback(Rest).

is_loopback({127, _, _, _}) -> true;
is_loopback({0, 0, 0, 0, 0, 0, 0, 1}) -> true;
is_loopback(<<"localhost">>) -> true;
is_loopback(_) -> false.

require_auth() ->
    application:get_env(broker_proto_mcp, require_auth, true).

listeners() ->
    application:get_env(broker_proto_mcp, listeners, []).

features() ->
    application:get_env(broker_proto_mcp, features, []).
