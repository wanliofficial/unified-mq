%% @doc A2A 1.0.0 front end (S9-A transport, S9-B task lifecycle).
%%
%% The listener is the last child of the supervisor: the task-shard registry
%% (and its boot replay) must exist before a request can dispatch to it
%% (ADR-026 §D4).
-module(broker_proto_a2a_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    case validate() of
        ok ->
            case a2a_card:seed() of
                ok -> broker_proto_a2a_sup:start_link();
                {error, Reason} -> {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.

%% Two boot-time refusals, both loud:
%%   * `require_auth => false' on a wide bind would expose an unauthenticated
%%     task API;
%%   * `task_reliability = replicated_commit' without a replicator would silently
%%     degrade a durable promise to `fsync' (ADR-025 §D4).
validate() ->
    case a2a_event:validate_reliability() of
        ok -> check_auth();
        {error, Reason} -> {error, Reason}
    end.

check_auth() ->
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
