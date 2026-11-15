%% @doc CoAP (RFC 7252) front-end application (S11-A).
%%
%% Two postures, both explicit (ADR-031 §D5/§D6):
%%
%%   * `enabled => false' (the ship default) starts the supervisor with no
%%     children at all -- zero sockets, nothing to firewall by accident.
%%   * `enabled => true' starts one UDP listener per `listeners' entry.
%%
%% The one boot-time refusal is the mirror of `broker_proto_mqtt_sn_app''s: this
%% stage implements CoAP over plain UDP only, which has no credential field, so
%% a deployment that lists `coap' in `broker_security''s `require_auth' would be
%% silently given an anonymous front-end. Refusing is the honest answer. S11-C
%% (DTLS) is what lifts this, because `coaps://' does carry a credential; the
%% guard reads the raw list rather than `broker_security:auth_required/1',
%% which answers `false' for every protocol absent from `credential_capable/0'
%% and therefore cannot tell "not required" from "cannot be provided".
-module(broker_proto_coap_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    case {enabled(), auth_unsupported()} of
        {false, _} ->
            broker_proto_coap_sup:start_link();
        {true, true} ->
            {error, {require_auth_unsupported, coap}};
        {true, false} ->
            broker_proto_coap_sup:start_link()
    end.

stop(_State) ->
    ok.

enabled() ->
    application:get_env(broker_proto_coap, enabled, false) =:= true.

%% `coap' is the wire name; `coaps' is what an operator writing "DTLS CoAP" is
%% likely to type. Both name this app.
auth_unsupported() ->
    case application:get_env(broker_security, require_auth, []) of
        List when is_list(List) ->
            lists:any(fun(P) -> P =:= <<"coap">> orelse P =:= <<"coaps">> end, List);
        _ ->
            false
    end.
