%% @doc A2A 1.0.0 front end (S9-A).
%%
%% Boot order matters only in that the listener is the last child of the
%% supervisor: S9-B adds the task-shard registry in front of it, and the
%% listener must not accept a request before the pieces it dispatches to exist
%% (ADR-026 §D4).
-module(broker_proto_a2a_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    case validate() of
        ok -> broker_proto_a2a_sup:start_link();
        {error, Reason} -> {error, Reason}
    end.

stop(_State) ->
    ok.

%% `require_auth => false' is a loopback-only posture: the whole point of the
%% switch is "try it on this machine". Binding it wide would silently expose an
%% unauthenticated task API, so the node refuses to boot instead.
validate() ->
    case require_auth() of
        true ->
            ok;
        false ->
            check_loopback(listeners())
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
    application:get_env(broker_proto_a2a, require_auth, true).

listeners() ->
    application:get_env(broker_proto_a2a, listeners, []).
