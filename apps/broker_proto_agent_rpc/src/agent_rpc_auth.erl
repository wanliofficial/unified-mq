%% @doc Credential extraction and `broker_security' wiring for the agent front
%% ends.
%%
%% The only credential shape accepted is an **opaque bearer token** resolved
%% against `broker_user': no JWT verification, no JWKS fetch, no external
%% introspection (ADR-026 §D7). `Authorization: Bearer <token>' and the
%% `X-API-Key' convenience header both normalize to `{token, Secret}', which is
%% exactly the shape `broker_security:authenticate_conn/1' already serves for
%% Pulsar.
%%
%% Two headers carrying different secrets is refused instead of picked: silently
%% preferring one would let a proxy strip the other and change the identity.
-module(agent_rpc_auth).

-export([credential/1, authenticate/1, authorize/3, anonymous/0, challenge/0]).
-export_type([identity/0]).

-type identity() :: #{principal := binary(), tenant_id := binary()}.
-type credential() :: {token, binary()}.

%% @doc `none' when no credential was presented; `{error, ambiguous}' when both
%% headers disagree.
-spec credential(map()) -> {ok, credential()} | none | {error, ambiguous}.
credential(Headers) when is_map(Headers) ->
    Bearer = bearer(maps:get(<<"authorization">>, Headers, undefined)),
    ApiKey = api_key(maps:get(<<"x-api-key">>, Headers, undefined)),
    case {Bearer, ApiKey} of
        {none, none} -> none;
        {{ok, Token}, none} -> {ok, {token, Token}};
        {none, {ok, Token}} -> {ok, {token, Token}};
        {{ok, Token}, {ok, Token}} -> {ok, {token, Token}};
        {{ok, _}, {ok, _}} -> {error, ambiguous}
    end.

%% @doc Resolve the credential to an identity. Any failure is reported as
%% `unauthorized'; `broker_security' already collapses "unknown user" and "wrong
%% secret" into one reason, and the HTTP layer must not reintroduce the
%% distinction.
-spec authenticate(credential()) -> {ok, identity()} | {error, unauthorized}.
authenticate({token, _} = Credential) ->
    case broker_security:authenticate_conn(Credential) of
        {ok, Identity} -> {ok, Identity};
        {error, _Reason} -> {error, unauthorized}
    end;
authenticate(_Other) ->
    {error, unauthorized}.

%% @doc ACL check with the identity's own tenant. A principal can never widen
%% its tenant: `broker_security' applies isolation before any rule.
-spec authorize(identity(), publish | subscribe, binary()) -> allow | deny.
authorize(#{principal := Principal, tenant_id := Tenant}, Action, Destination)
  when Action =:= publish; Action =:= subscribe ->
    broker_security:authorize(Principal, #{action => Action,
                                           tenant_id => Tenant,
                                           destination => Destination}).

%% @doc The identity an anonymous connection runs as, only used when an adapter
%% explicitly sets `require_auth => false' (and then only on a loopback bind).
-spec anonymous() -> identity().
anonymous() ->
    #{principal => <<"anonymous">>,
      tenant_id => broker_security:default_tenant()}.

-spec challenge() -> binary().
challenge() ->
    <<"Bearer realm=\"a2a\"">>.

%%%_* Internal --------------------------------------------------------------

bearer(undefined) ->
    none;
bearer(Value) when is_binary(Value) ->
    case binary:split(Value, <<" ">>) of
        [Scheme, Token] when Token =/= <<>> ->
            case lower(Scheme) of
                <<"bearer">> -> {ok, Token};
                _ -> none
            end;
        _ ->
            none
    end;
bearer(_Other) ->
    none.

api_key(undefined) -> none;
api_key(<<>>) -> none;
api_key(Value) when is_binary(Value) -> {ok, Value};
api_key(_Other) -> none.

lower(Bin) when is_binary(Bin) ->
    << <<(lower_byte(B))/integer>> || <<B/integer>> <= Bin >>.

lower_byte(B) when B >= $A, B =< $Z -> B + 32;
lower_byte(B) -> B.
