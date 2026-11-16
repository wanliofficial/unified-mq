%% @doc CoAP (RFC 7252) front-end application (S11-A, DTLS in S11-C).
%%
%% Two postures, both explicit (ADR-031 §D5/§D6):
%%
%%   * `enabled => false' (the ship default) starts the supervisor with no
%%     children at all -- zero sockets, nothing to firewall by accident.
%%   * `enabled => true' starts one listener per `listeners' entry, either plain
%%     UDP (the default) or DTLS when the spec says `transport => dtls'.
%%
%% ## The boot-time credential guard
%%
%% `broker_security''s `require_auth' is a promise that a protocol will not run
%% unauthenticated. Whether this app can keep that promise depends on the
%% transport, so the guard is per protocol name rather than "is the list
%% non-empty":
%%
%%   * `<<"coap">>' (plain UDP) is REFUSED. A UDP datagram has no credential
%%     field, so the promise cannot be kept and the deployment would silently get
%%     an anonymous front-end. The refusal names `coaps' so the operator knows the
%%     fix.
%%   * `<<"coaps">>' (DTLS) is ACCEPTED, but only when EVERY enabled listener is
%%     actually DTLS. One plain listener alongside a DTLS one would be the same
%%     silent downgrade -- requests would arrive on the plain socket with no
%%     principal -- so a mixed configuration is refused too.
%%   * any other protocol in the list leaves this app alone: a guard that refuses
%%     everything is an outage, not a guard.
%%
%% The guard reads the raw `require_auth' list rather than
%% `broker_security:auth_required/1', which answers `false' for anything absent
%% from `credential_capable/0' and so cannot tell "not required" from "cannot be
%% provided".
-module(broker_proto_coap_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    case enabled() of
        false ->
            broker_proto_coap_sup:start_link();
        true ->
            case guard() of
                ok -> broker_proto_coap_sup:start_link();
                {error, Reason} -> {error, Reason}
            end
    end.

stop(_State) ->
    ok.

enabled() ->
    application:get_env(broker_proto_coap, enabled, false) =:= true.

guard() ->
    Required = require_auth_list(),
    case lists:member(<<"coap">>, Required) of
        true ->
            %% Plain UDP can never carry a credential, so `require_auth' for it
            %% is unsatisfiable -- the honest answer is to refuse the boot.
            {error, {require_auth_unsupported, coap}};
        false ->
            case lists:member(<<"coaps">>, Required) of
                false ->
                    ok;
                true ->
                    case lists:all(fun is_dtls/1, listeners()) of
                        true -> ok;
                        false -> {error, {require_auth_needs_dtls, coaps}}
                    end
            end
    end.

is_dtls(#{transport := dtls}) -> true;
is_dtls(_Spec) -> false.

listeners() ->
    case application:get_env(broker_proto_coap, listeners, []) of
        List when is_list(List) -> List;
        _ -> []
    end.

require_auth_list() ->
    case application:get_env(broker_security, require_auth, []) of
        List when is_list(List) -> [P || P <- List, is_binary(P)];
        _ -> []
    end.
