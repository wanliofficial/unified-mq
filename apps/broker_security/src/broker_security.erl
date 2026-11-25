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
%%     a `*'/`#' suffix wildcard. First match wins. No match means `deny' when rules
%%     are configured. An unauthenticated datagram (anonymous principal) carries no
%%     principal to match against, so under `allow' and under a rule list it is
%%     allowed by default -- only the explicit `deny' posture refuses it.
%%   * **Tenant isolation**: a principal authenticated with tenant `T' may only
%%     act inside `T'. A request that names a different tenant is denied even
%%     when a rule would otherwise allow it -- an ACL rule can widen topics,
%%     never tenants.
%%
%% The pure-function shape stays: the config is read per call, no process, no
%% boot-time validation. A malformed rule is skipped (never crashes the
%% publisher); an ACL typo therefore fails closed whenever a rule list or the
%% `deny' posture is configured, which is the safe direction.
-module(broker_security).

-export([authenticate/2, authorize/2, tenant_id/1, mqttsn_psk_for_identity/1,
         authorized/2, acl_rules/0, redacted_config/0,
         authenticate_conn/1, auth_required/1, default_tenant/0,
         long_term_key/2, psk_for_identity/1,
         revoke/2, clear_revocation/1, revoked/1]).

-export_type([principal/0, request/0, credential/0, conn_identity/0]).

%% §4.3: S1's Principal is the username itself. S6 keeps that: a principal is
%% the authenticated identity, and the tenant is a property of the identity.
-type principal() :: binary().
-type request() :: {connect, binary()}
                 | #{action := publish | subscribe | admin
                              | turn_allocate | turn_refresh | turn_permission
                              | turn_channel | turn_send,
                     tenant_id => binary(),
                     destination => binary()}.

%% @doc How a protocol carries a credential, normalized.
%%
%% Every protocol has its own handshake shape, but they all reduce to one of
%% two forms: a name plus a secret (`{user_pass, Name, Secret}'), or a single
%% shared secret with no name (`{token, Secret}' -- Pulsar's `token' auth
%% method, and any future protocol that only carries a bearer value).
%%
%% Both forms resolve against the SAME `users' table, so one identity list
%% serves all six protocols.
-type credential() :: {user_pass, binary(), binary()} | {token, binary()}.

%% @doc The identity a connection ends up with: who it is and which tenant it
%% belongs to. The tenant comes from the matched user record, never from the
%% client -- a client that names its own tenant gets its own tenant ignored.
-type conn_identity() :: #{principal := principal(), tenant_id := binary()}.

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
-spec acl_rules() -> allow | deny | [map()] | term().
acl_rules() ->
    application:get_env(broker_security, acl, allow).

%%%_* Revocation facade -----------------------------------------------------

%% @doc MQ-17 #10: 吊销 `Principal' 并驱逐其活动连接。TTL 为 `undefined'
%% 表示永久。转发 `broker_revocation'，保持安全语汇集中在本模块。
-spec revoke(principal(), non_neg_integer() | undefined) -> ok.
revoke(Principal, TTL) ->
    broker_revocation:revoke(Principal, TTL).

%% @doc 解除 `Principal' 的吊销。转发 `broker_revocation'。
-spec clear_revocation(principal()) -> ok.
clear_revocation(Principal) ->
    broker_revocation:clear(Principal).

%% @doc `Principal' 当前是否被吊销。转发 `broker_revocation'。
-spec revoked(principal()) -> boolean().
revoked(Principal) ->
    broker_revocation:revoked(Principal).

%%%_* Protocol connections --------------------------------------------------

%% @doc Authenticate a protocol connection's credential and resolve the
%% identity it runs as.
%%
%% This is the single entry point every protocol adapter uses, so all six
%% protocols share one identity table and one tenant derivation:
%%
%%   * `{user_pass, Name, Secret}' matches the `users' table by name and
%%     compares the secret against that record's `password'.
%%   * `{token, Secret}' matches the `users' table by the record's `token'
%%     field -- a protocol that carries only a bearer value (Pulsar's `token'
%%     auth method) still resolves to a named principal and therefore to a
%%     tenant.
%%
%% The error is always `bad_username_or_password' so a probe cannot tell
%% "no such user" from "wrong secret" -- and a token lookup reports the same
%% thing, so the two credential shapes are not distinguishable either.
-spec authenticate_conn(credential()) -> {ok, conn_identity()} | {error, term()}.
authenticate_conn({user_pass, Name, Secret})
  when is_binary(Name), is_binary(Secret) ->
    case user_record(Name) of
        #{password := Secret} -> {ok, identity(Name)};
        _ -> {error, bad_username_or_password}
    end;
authenticate_conn({token, Secret}) when is_binary(Secret) ->
    case user_by_token(Secret) of
        {ok, Name} -> {ok, identity(Name)};
        error -> {error, bad_username_or_password}
    end;
authenticate_conn(_Other) ->
    {error, bad_username_or_password}.

%% @doc Long-term credential key for TURN (RFC 8489 §9.2.2, RFC 5766 §4):
%% `MD5(username ":" realm ":" password)', using the password stored in the
%% same `users' table every protocol authenticates against.
%%
%% Exists because TURN never sees the password: the client proves knowledge of
%% it with MESSAGE-INTEGRITY, so the server has to derive the key itself. This
%% is an *addition* to the S6 surface -- `authenticate/2',
%% `authenticate_conn/1' and `authorize/2' keep their exact semantics. The
%% password and the key are never logged; a caller that stores the key must
%% treat it as a credential.
-spec long_term_key(binary(), binary()) -> {ok, <<_:128>>} | {error, term()}.
long_term_key(Username, Realm) when is_binary(Username), is_binary(Realm) ->
    case user_record(Username) of
        #{password := Password} when is_binary(Password) ->
            {ok, crypto:hash(md5, [Username, <<":">>, Realm, <<":">>, Password])};
        _ ->
            {error, bad_username_or_password}
    end;
long_term_key(_Username, _Realm) ->
    {error, bad_username_or_password}.

%% @doc The PSK for a CoAP-over-DTLS peer (S11-C), derived from the SAME
%% `users' table every other protocol authenticates against.
%%
%% The DTLS PSK identity IS the username: a device presents its own name during
%% the handshake and the broker derives the key from the password it already
%% stores. No second credential store, no certificate, no key material on the
%% wire -- same precedent as `long_term_key/2' (TURN): the server derives, the
%% client derives, the password never crosses.
%%
%% Domain separation is what makes this safe to co-exist with the TURN key: a
%% distinct label plus NUL separators cannot collide with TURN's
%% `MD5(user ":" realm ":" pass)'.
%%
%% The separator encoding is only injective when the identity and the realm are
%% NUL-free, so both are rejected outright if they contain one (the password may
%% contain anything: with two NUL-free leading fields, the first two NULs are the
%% separators and the remainder is the password, unambiguously).
%%
%% A missing user and a user without a password fail identically, so the handshake
%% never reveals which names exist.
-spec psk_for_identity(binary()) -> {ok, <<_:256>>} | {error, term()}.
psk_for_identity(Identity) when is_binary(Identity) ->
    Realm = coaps_realm(),
    case separator_safe(Identity) andalso separator_safe(Realm) of
        false ->
            {error, bad_username_or_password};
        true ->
            case user_record(Identity) of
                #{password := Password} when is_binary(Password) ->
                    {ok, crypto:hash(sha256, [<<"unifiedmq-coaps-psk">>, 0,
                                              Identity, 0, Realm, 0, Password])};
                _ ->
                    {error, bad_username_or_password}
            end
    end;
psk_for_identity(_Other) ->
    {error, bad_username_or_password}.

%% A field that must act as a separator-delimited prefix cannot contain the
%% separator, or the encoding stops being injective.
separator_safe(Field) ->
    binary:match(Field, <<0>>) =:= nomatch.

%% The realm the derivation is bound to. Separate from `broker_security.tls'
%% on purpose: that block describes certificate-based TCP TLS, and DTLS/PSK has
%% no certificate at all. A deployment that changes the realm invalidates every
%% derived PSK, which is the point -- realm is part of the key.
coaps_realm() ->
    case application:get_env(broker_security, coaps, #{}) of
        #{realm := R} when is_binary(R), byte_size(R) > 0 -> R;
        _ -> <<"unifiedmq">>
    end.

%% MQTT-SN over DTLS (SN-01, 2026-11-17): the same identity-is-the-username
%% contract as `psk_for_identity/1', with its own derivation label and realm
%% (`broker_security.mqttsn #{realm => ...}') so an MQTT-SN PSK can never equal
%% a CoAPS PSK for the same user -- a captured key on one transport must not be
%% replayable on the other.
-spec mqttsn_psk_for_identity(binary()) -> {ok, <<_:256>>} | {error, term()}.
mqttsn_psk_for_identity(Identity) when is_binary(Identity) ->
    Realm = mqttsn_realm(),
    case separator_safe(Identity) andalso separator_safe(Realm) of
        false ->
            {error, bad_username_or_password};
        true ->
            case user_record(Identity) of
                #{password := Password} when is_binary(Password) ->
                    {ok, crypto:hash(sha256, [<<"unifiedmq-mqttsn-psk">>, 0,
                                              Identity, 0, Realm, 0, Password])};
                _ ->
                    {error, bad_username_or_password}
            end
    end;
mqttsn_psk_for_identity(_Other) ->
    {error, bad_username_or_password}.

mqttsn_realm() ->
    case application:get_env(broker_security, mqttsn, #{}) of
        #{realm := R} when is_binary(R), byte_size(R) > 0 -> R;
        _ -> <<"unifiedmq">>
    end.

%% @doc Whether a protocol must present a credential at all.
%%
%% A protocol that is not required accepts anonymous connections and runs them
%% as the default tenant, which is the posture every adapter had before the
%% identity table was shared.  Requiring a credential is what turns the
%% shared table from "extra info" into the actual gate.
%%
%% A protocol whose handshake cannot carry a credential is never required:
%% demanding one would lock every client out rather than secure anything.
-spec auth_required(binary()) -> boolean().
auth_required(Protocol) when is_binary(Protocol) ->
    lists:member(Protocol, required_protocols())
        andalso lists:member(Protocol, credential_capable());
auth_required(_Other) ->
    false.

%% @doc The tenant an anonymous (or not-required) connection runs as.
%%
%% Fixed per deployment: an unauthenticated client cannot pick its own
%% tenant, because the tenant is the isolation boundary.
-spec default_tenant() -> binary().
default_tenant() ->
    case application:get_env(broker_security, default_tenant, <<"t0">>) of
        Tenant when is_binary(Tenant) -> Tenant;
        _ -> <<"t0">>
    end.

%%%_* Internal: identity ----------------------------------------------------

identity(Name) ->
    #{principal => Name, tenant_id => tenant_id(Name)}.

%% The protocols whose handshakes can carry a credential at all. `coaps' is
%% CoAP over DTLS (S11-C): the PSK identity IS the username, so the shared
%% table is the actual gate. Plain `coap' is absent on purpose -- a UDP datagram
%% has no credential field, and claiming otherwise would let `require_auth'
%% silently do nothing.
credential_capable() ->
    [<<"mqtt">>, <<"ws">>, <<"nats">>, <<"amqp">>, <<"kafka">>, <<"pulsar">>,
     <<"coaps">>].

required_protocols() ->
    case application:get_env(broker_security, require_auth, []) of
        List when is_list(List) -> [P || P <- List, is_binary(P)];
        _ -> []
    end.

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
    %% Anonymous principal: an unauthenticated datagram (e.g. plaintext CoAP).
    %% Such a channel has no principal to match rules against, so it stays out of
    %% the ACL by design -- under the permissive `allow' posture and under a rule
    %% list it is allowed by default. The explicit `deny' posture is the one lever
    %% that refuses it: deny-by-default closes the unauthenticated door too.
    case acl_rules() of
        deny -> deny;
        _ -> allow
    end;
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
        deny ->
            %% Fail-closed: the bare `deny' posture carries no allow rules, so every
            %% request -- named principal or anonymous -- is refused. Operators who
            %% want selective allows configure a rule list (no-match is then deny).
            deny;
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

%% A user record may carry a `token' instead of (or besides) a `password'.
%% That is the credential shape for protocols whose handshake only carries a
%% bearer value -- Pulsar's `token' auth method is the one in-tree example.
%%
%% A token miss reports the same error as a password miss, so the two
%% credential shapes are not distinguishable by the failure either.
user_by_token(Token) ->
    case lists:dropwhile(fun({_, #{token := T}}) -> T =/= Token;
                             (_) -> true
                          end, users()) of
        [{Name, _} | _] -> {ok, Name};
        [] -> error
    end.

users() ->
    case application:get_env(broker_security, users, []) of
        List when is_list(List) -> List;
        _ -> []
    end.
