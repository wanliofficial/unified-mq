%% @doc MQTT-SN 1.2 gateway application.
%%
%% One guard lives here rather than in the listener: MQTT-SN's CONNECT packet
%% has no credential field at all (§5.4.4 carries flags, protocol id, duration
%% and client id -- nothing else), so `require_auth' cannot be honoured for this
%% protocol. `broker_security:auth_required/1' would answer `false' for
%% `<<"mqtt_sn">>' because the protocol is absent from `credential_capable/0',
%% which is the right answer for authentication but the wrong one for
%% configuration: a deployment that asked for mandatory credentials on every
%% listener must be told this listener cannot provide them, not silently given
%% an anonymous gateway. So the check reads the raw `require_auth' list and
%% refuses to start.
-module(broker_proto_mqtt_sn_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    case auth_unsupported() of
        true ->
            {error, {require_auth_unsupported, mqtt_sn}};
        false ->
            broker_proto_mqtt_sn_sup:start_link()
    end.

stop(_State) ->
    ok.

%% `require_auth' naming this protocol is a configuration error, whichever
%% spelling the operator used: the wire name is `mqtt_sn', the app name is the
%% same, and `mqtt-sn' is the name the protocol goes by in prose and in most
%% client tooling.
auth_unsupported() ->
    case application:get_env(broker_security, require_auth, []) of
        List when is_list(List) ->
            lists:any(fun(P) -> P =:= <<"mqtt_sn">> orelse P =:= <<"mqtt-sn">> end,
                      List);
        _ ->
            false
    end.
