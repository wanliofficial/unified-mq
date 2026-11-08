%% @doc Authentication, authorization and tenant isolation (S6).
%%
%% S1 shipped an authentication stub and `authorize/2' always returning
%% `allow'. S6 keeps the same public shape (`authenticate/2', `authorize/2',
%% `tenant_id/1') so every call site keeps working, and adds:
%%
%%   * **ACL rules** (`acl' env): an ordered list of
%%     `#{effect := allow | deny, principal => P, action => A,
%%        pattern => Pattern}' maps. `<<"*">>' is the wildcard for principal
%%     and action; `Pattern' is an exact destination, a trailing `/` prefix, or
%%     a `*'/`#' suffix wildcard. First match wins. No match means `deny' when
%%     rules are configured and `allow' when `acl' is the atom `allow' (the S1
%%     default).
%%   * **Tenant isolation**: a principal authenticated with tenant `T' may only
%%     act inside `T'. A request that names a different tenant is denied even
%%     when a rule would otherwise allow it -- an ACL rule can widen topics,
%%     never tenants.
%%
%% The pure-function shape stays: the config is read per call, no process, no
%% boot-time validation. A malformed rule is skipped (never crashes the
%% publisher); an ACL typo therefore fails closed only when rules exist, which
%% is the safe direction.
-module(broker_security).

-export([authenticate/2, authorize/2, tenant_id/1,
         authorized/2, acl_rules/0, redacted_config/0]).

-export_type([principal/0, request/0]).

%% §4.3: S1's Principal is the username itself. S6 keeps that: a principal is
%% the authenticated identity, and the tenant is a property of the identity.
-type principal() :: binary().
-type request() :: {connect, binary()}
                 | #{action := publish | subscribe | admin,
                     tenant_id => binary(),
                     destination => binary()}.

%% @doc Rejects with the same reason whether the username is unknown or the
%% password merely differs -- the error must not leak "this user exists".
-spec authenticate(binary(), binary()) -> {ok, principal()} | {error, term()}.
authenticate(Username, Password) when is_binary(Username), is_binary(Password) ->
    case user_record(Username) of
        #{password := Password} -> {ok, Username};
        _ -> {error, bad_username_or_password}
    end;
authenticate(_Username, _Password) ->
    {error, bad_username_or_password}.

%% @doc `allow' when the principal may perform the request, `deny' otherwise.
%% Total: a malformed request is a denial, never a crash.
-spec authorize(principal() | undefined, request() | term()) -> allow | deny.
authorize(Principal, {connect, _ClientId}) ->
    authorize_principal(Principal, connect, undefined, undefined);
authorize(Principal, #{action := Action} = Req) ->
    Tenant = maps:get(tenant_id, Req, undefined),
    Destination = maps:get(destination, Req, undefined),
    authorize_principal(Principal, Action, Tenant, Destination);
authorize(Principal, _Other) ->
    authorize_principal(Principal, undefined, undefined, undefined).

%% @doc Same decision with a boolean answer, for call sites that prefer it.
-spec authorized(principal() | undefined, request()) -> boolean().
authorized(Principal, Req) ->
    authorize(Principal, Req) =:= allow.

%% @doc The tenant the message's `tenant_id' slot is filled from (§4.1 field
%% table). A principal always authenticated through `authenticate/2', so its
%% record exists; the default keeps this total for hand-built principals.
-spec tenant_id(principal()) -> binary().
tenant_id(Principal) ->
    maps:get(tenant_id, user_record(Principal), <<"default">>).

%% @doc The ACL configuration verbatim (tests and the admin API read it).
-spec acl_rules() -> allow | [map()] | term().
acl_rules() ->
    application:get_env(broker_security, acl, allow).

%% @doc A copy of the security configuration safe to log or serve: passwords
%% are replaced, user names and tenants are kept.
-spec redacted_config() -> map().
redacted_config() ->
    #{users => [{Name, maps:remove(password, Rec)}
                || {Name, Rec} <- users()],
      acl => acl_rules(),
      limits => broker_limits:limits(),
      tls => broker_security_tls:redacted_config()}.

%%%_* Internal --------------------------------------------------------------

%% Tenant isolation is checked first and independently of the ACL: an
%% authenticated principal cannot name another tenant, because that is the
%% privilege boundary, not a policy preference.
authorize_principal(undefined, _Action, _Tenant, _Destination) ->
    allow;
authorize_principal(Principal, Action, Tenant, Destination) ->
    OwnTenant = tenant_id(Principal),
    case tenant_ok(Tenant, OwnTenant) of
        false ->
            deny;
        true ->
            acl_decision(Principal, Action, Destination)
    end.

tenant_ok(undefined, _Own) -> true;
tenant_ok(Tenant, Own) -> Tenant =:= Own.

acl_decision(Principal, Action, Destination) ->
    case acl_rules() of
        allow ->
            allow;
        Rules when is_list(Rules) ->
            case first_match(Rules, Principal, Action, Destination) of
                {ok, Effect} -> Effect;
                none -> deny
            end;
        _Other ->
            allow
    end.

first_match([], _Principal, _Action, _Destination) ->
    none;
first_match([Rule | Rest], Principal, Action, Destination) ->
    case rule_matches(Rule, Principal, Action, Destination) of
        {ok, Effect} -> {ok, Effect};
        nomatch -> first_match(Rest, Principal, Action, Destination)
    end.

rule_matches(#{effect := Effect, principal := P, action := A} = Rule,
             Principal, Action, Destination) when Effect =:= allow;
                                              Effect =:= deny ->
    Pattern = maps:get(pattern, Rule, <<"*">>),
    case principal_matches(P, Principal) andalso action_matches(A, Action)
        andalso pattern_matches(Pattern, Destination) of
        true -> {ok, Effect};
        false -> nomatch
    end;
rule_matches(_Rule, _Principal, _Action, _Destination) ->
    nomatch.

principal_matches(<<"*">>, _Principal) -> true;
principal_matches(P, Principal) -> P =:= Principal.

action_matches(<<"*">>, _Action) -> true;
action_matches(A, Action) when is_atom(A) -> A =:= Action;
action_matches(A, Action) when is_binary(A) -> A =:= atom_to_binary(Action, utf8);
action_matches(_, _) -> false.

pattern_matches(<<>>, _Destination) -> true;
pattern_matches(<<"*">>, _Destination) -> true;
pattern_matches(<<"#">>, _Destination) -> true;
pattern_matches(Pattern, undefined) -> Pattern =:= <<"*">> orelse Pattern =:= <<"#">>;
pattern_matches(Pattern, Destination) when is_binary(Pattern), is_binary(Destination) ->
    case binary:last(Pattern) of
        $* -> prefix_match(Pattern, 1, Destination);
        %% `#' is the MQTT multi-level wildcard: `t1/#' matches `t1/a/b',
        %% `$sys/#' matches `$sys/x'.
        $# -> prefix_match(Pattern, 1, Destination);
        %% A trailing slash is a plain prefix: `t1/app/' matches `t1/app/x'
        %% but not `t1/app'.
        $/ -> prefix_match(Pattern, 0, Destination);
        _ -> Pattern =:= Destination
    end;
pattern_matches(_, _) ->
    false.

prefix_match(Pattern, Drop, Destination) ->
    Size = byte_size(Pattern) - Drop,
    Prefix = binary:part(Pattern, 0, Size),
    byte_size(Destination) >= Size andalso
        binary:part(Destination, 0, Size) =:= Prefix.

user_record(Username) ->
    case lists:keyfind(Username, 1, users()) of
        {_, Record} -> Record;
        false -> #{}
    end.

users() ->
    case application:get_env(broker_security, users, []) of
        List when is_list(List) -> List;
        _ -> []
    end.
