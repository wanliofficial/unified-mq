%%% @doc Pluggable MQTT 5 enhanced-auth method framework (MQ-05, spec §3.2/3.3).
%%%
%%% A CONNECT that names an `authentication_method' is routed here instead of the
%%% synchronous username/password path. Each method is a `broker_auth_<name>'
%%% module implementing the `broker_auth' behaviour; this module is the
%%% registry that dispatches by method name and reads the enabled list from
%%% config. A CONNECT without an `authentication_method' never reaches here.
-module(broker_auth).

%% This module DEFINES the behaviour (the -callback clauses below); it is the
%% registry/dispatcher, not an implementer, so it does NOT carry
%% -behaviour(broker_auth). The implementers (broker_auth_plain) do.
%%
%% NOTE: `begin' is an Erlang reserved word, so the callback, the export and
%% every call site quote it: broker_auth:'begin'(Method, Data, Ctx). That is
%% the entry point the FSM's `awaiting_auth' step will use.
-export(['begin'/3, continue/3, reauth/3, enabled_methods/0]).

-type method()  :: binary().
-type result() :: {ok, term(), map()}
                | {continue, binary(), term()}
                | {error, term()}.

-export_type([method/0, result/0]).

-callback 'begin'(Method :: method(), InitialData :: binary() | undefined,
                  Ctx :: map()) -> result().
-callback continue(Method :: method(), AuthState :: term(),
                   ResponseData :: binary() | undefined) -> result().
-callback reauth(Method :: method(), Ctx :: map(),
                 Data :: binary() | undefined) -> {ok, map()} | {error, term()}.

-spec enabled_methods() -> [method()].
enabled_methods() ->
    application:get_env(broker_proto_mqtt, mqtt_auth_methods, [<<"plain">>]).

-spec 'begin'(method(), binary() | undefined, map()) -> result().
'begin'(Method, Data, Ctx) ->
    case module_for(Method) of
        {ok, Mod} -> Mod:'begin'(Method, Data, Ctx);
        error -> {error, unknown_auth_method}
    end.

-spec continue(method(), term(), binary() | undefined) -> result().
continue(Method, AuthState, Data) ->
    case module_for(Method) of
        {ok, Mod} -> Mod:continue(Method, AuthState, Data);
        error -> {error, unknown_auth_method}
    end.

-spec reauth(method(), map(), binary() | undefined) -> {ok, map()} | {error, term()}.
reauth(Method, Ctx, Data) ->
    case module_for(Method) of
        {ok, Mod} -> Mod:reauth(Method, Ctx, Data);
        error -> {error, unknown_auth_method}
    end.

%% `mqtt_auth_methods' is a list of binaries naming enabled methods; the module
%% is `broker_auth_<name>'. A method not in the list is refused.
module_for(Method) ->
    case lists:member(Method, enabled_methods()) of
        true ->
            Name = binary_to_list(Method),
            {ok, list_to_atom("broker_auth_" ++ Name)};
        false ->
            error
    end.
