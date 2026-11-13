%% @doc WebRTC signaling front end (S10-A: connection base only).
%%
%% ADR-028 boundary in one line: this app produces no `#broker_message{}',
%% never calls `broker_queue:append/3' or `broker_router', and holds no
%% cursor/retention/SubRef state. A signaling message is a process-direct
%% frame delivered to the owning connection process (`webrtc_conn'), nothing
%% more. `broker_core' is a dependency only because `broker_metrics' and
%% `broker_security' pull it in; no broker_core header is included here and
%% no broker_core record is constructed.
-module(broker_proto_webrtc_app).
-behaviour(application).

-export([start/2, stop/1, validate/0]).

start(_Type, _Args) ->
    case validate() of
        ok -> broker_proto_webrtc_sup:start_link();
        {error, Reason} -> {error, Reason}
    end.

stop(_State) ->
    ok.

%% Loud-fail boot validation, same posture as broker_proto_a2a_app (ADR-025
%% §D4): a misconfigured listener must stop the node, not silently degrade.
%%
%% Four refusals, all S10-A scope:
%%   * `require_auth => false' on a wide bind would expose an unauthenticated
%%     signaling relay;
%%   * a non-positive timeout or frame limit is a configuration error, not a
%%     "disabled" feature;
%%   * `subprotocols' must be a non-empty list of binaries -- an empty list
%%     would make every handshake a 400, and a non-binary entry would never
%%     match a client offer.
validate() ->
    case auth_env() of
        ok ->
            case check_subprotocols() of
                ok -> check_timeouts();
                {error, Reason} -> {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% ADR-028 §D5: `require_auth' is this app's own env key, exactly like
%% broker_proto_a2a and broker_proto_mcp. It is deliberately NOT read from
%% `broker_security': that key is a list of protocol names for the six
%% credential-capable protocols, and reading it here made the shipped release
%% (where it is `[]') refuse to boot.
auth_env() ->
    case application:get_env(broker_proto_webrtc, require_auth, true) of
        true ->
            ok;
        false ->
            %% Same shape as a2a: only a loopback bind may run without auth.
            check_loopback(listeners());
        Other -> {error, {invalid_require_auth, Other}}
    end.

check_timeouts() ->
    case auth_timeout_ms() > 0 of
        true -> check_idle();
        false -> {error, {invalid_auth_timeout_ms, auth_timeout_ms()}}
    end.

check_idle() ->
    %% `infinity' is a documented value meaning "no idle window"; anything else
    %% must be a positive millisecond count.
    case idle_timeout_ms() of
        infinity -> check_frame_bytes();
        Ms when is_integer(Ms), Ms > 0 -> check_frame_bytes();
        _ -> {error, {invalid_idle_timeout_ms, idle_timeout_ms()}}
    end.

check_frame_bytes() ->
    case max_frame_bytes() > 0 of
        true -> ok;
        false -> {error, {invalid_max_frame_bytes, max_frame_bytes()}}
    end.

check_subprotocols() ->
    case subprotocols() of
        [] -> {error, {invalid_subprotocols, empty}};
        List when is_list(List) ->
            case lists:all(fun is_binary/1, List) of
                true -> ok;
                false -> {error, {invalid_subprotocols, List}}
            end;
        Other -> {error, {invalid_subprotocols, Other}}
    end.

%% No listener is a legal state (default: `listeners => []'), so there is
%% nothing to gate.
check_loopback([]) ->
    ok;
check_loopback([Spec | Rest]) ->
    case is_loopback(maps:get(ip, Spec, {127, 0, 0, 1})) of
        true -> check_loopback(Rest);
        false -> {error, {insecure_listener, maps:get(port, Spec, undefined),
                          maps:get(ip, Spec, undefined)}}
    end.

is_loopback({127, 0, 0, 1}) -> true;
is_loopback({127, 0, 1, 1}) -> true;
is_loopback({0, 0, 0, 0, 0, 0, 0, 1}) -> true;
is_loopback({0, 0, 0, 0, 0, 0, 0, 0}) -> false;
is_loopback(Ip) when is_tuple(Ip) ->
    %% Anything not literally loopback is treated as wide; IPv6 ::1 is the
    %% only non-IPv4 loopback we enumerate rather than resolve.
    false;
is_loopback(_) -> false.

listeners() ->
    application:get_env(broker_proto_webrtc, listeners, []).

auth_timeout_ms() ->
    application:get_env(broker_proto_webrtc, auth_timeout_ms, 5000).

idle_timeout_ms() ->
    application:get_env(broker_proto_webrtc, idle_timeout_ms, 60000).

max_frame_bytes() ->
    application:get_env(broker_proto_webrtc, max_frame_bytes, 262144).

subprotocols() ->
    case application:get_env(broker_proto_webrtc, subprotocols,
                             [<<"webrtc.v1">>]) of
        List when is_list(List) -> List;
        _ -> []
    end.
