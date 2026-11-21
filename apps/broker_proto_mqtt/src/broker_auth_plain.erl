%%% @doc First enhanced-auth method: a nonce-guarded username/password
%%% exchange (MQ-05, spec §3.3). Deliberately NOT SCRAM — no new crypto
%%% dependency. The point is that the AUTH packet + multi-round state machine
%%% is real and end-to-end testable; a crypto-challenge method can plug into the
%%% same framework later.
%%%
%%% Wire shape:
%%%   begin    -> challenge data is a fresh Nonce binary.
%%%   continue -> response data is term_to_binary({User, Nonce, Pass}). The
%%%               Nonce must equal the one begin issued for this handshake, and
%%%               broker_security:authenticate(User, Pass) must pass.
%%%   reauth   -> data is term_to_binary({User, Pass}).
-module(broker_auth_plain).
-behaviour(broker_auth).
-export(['begin'/3, continue/3, reauth/3]).

%% A short, unique, non-secret nonce. Not crypto-grade on purpose: it binds a
%% response to its handshake, it does not resist an active attacker (TLS does).
nonce() ->
    term_to_binary({unique_integer, erlang:monotonic_time()}).

'begin'(_Method, _InitialData, Ctx) when is_map(Ctx) ->
    Nonce = nonce(),
    {continue, Nonce, {Nonce, Ctx}}.

continue(_Method, {Nonce, Ctx}, ResponseData) ->
    try binary_to_term(ResponseData) of
        {User, Nonce, Pass}
          when is_binary(User), is_binary(Pass) ->
            case broker_security:authenticate(User, Pass) of
                {ok, Principal} -> {ok, Principal, Ctx};
                {error, _} -> {error, unauthorized}
            end;
        _ ->
            {error, unauthorized}
    catch
        _:_ -> {error, unauthorized}
    end;
continue(_Method, _AuthState, _ResponseData) ->
    {error, unauthorized}.

reauth(_Method, Ctx, Data) ->
    try binary_to_term(Data) of
        {User, Pass} when is_binary(User), is_binary(Pass) ->
            case broker_security:authenticate(User, Pass) of
                {ok, _Principal} -> {ok, Ctx};
                {error, _} -> {error, unauthorized}
            end;
        _ -> {error, unauthorized}
    catch
        _:_ -> {error, unauthorized}
    end.
