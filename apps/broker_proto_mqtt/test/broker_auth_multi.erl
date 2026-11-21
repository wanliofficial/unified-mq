%%% @doc Test-only enhanced-auth method used by mqtt_conn_SUITE to exercise
%%% the multi-round challenge branch of the AUTH state machine.
%%%
%%% `broker_auth_plain' resolves in one continue, so `handle_auth_response's'
%%% second `{continue, ...}' arm (which re-sends AUTH 0x18 instead of the
%%% CONNACK attach tail) is never reached by any shipped method. This method
%%% challenges exactly twice before succeeding: initiate -> challenge 1,
%%% continue(round 1) -> challenge 2, continue(round 2) -> success. The
%%% response bytes are not validated (any authentication_data advances a
%%% round); the point is the round count, not credential checking.
-module(broker_auth_multi).
-behaviour(broker_auth).

-export([initiate/3, continue/3, reauth/3]).

initiate(_Method, _InitialData, _Ctx) ->
    {continue, <<1>>, 1}.

continue(_Method, 1, _ResponseData) ->
    {continue, <<2>>, 2};
continue(_Method, 2, _ResponseData) ->
    {ok, <<"alice">>, #{}};
continue(_Method, _Round, _ResponseData) ->
    {error, unauthorized}.

reauth(_Method, Ctx, _Data) ->
    {ok, Ctx}.
