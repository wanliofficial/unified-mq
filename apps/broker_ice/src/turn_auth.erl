%% @doc TURN long-term credential authentication (RFC 5766 §4, RFC 8489 §9.2).
%%
%% The whole module is a pure function of (request bytes, configuration):
%% nothing is cached, nothing is logged, and the password never leaves
%% `broker_security'. A TURN client proves knowledge of the password with
%% MESSAGE-INTEGRITY, so the server has to derive the same key:
%%
%%     key = MD5(username ":" realm ":" password)
%%
%% `broker_security:long_term_key/2' performs that lookup against the same
%% `users' table the six protocol adapters authenticate against (R-I3: TURN and
%% signalling share one account, TURN additionally mixes in the realm), and
%% `broker_security:authorize/2' applies the ACL with a TURN-specific action
%% (`turn_allocate', `turn_refresh', ...). Neither the password nor the key is
%% ever put in a log line or an audit record -- the only fields this module
%% ever exposes are username, realm and the allocation metadata the caller
%% already knows.
%%
%% Nonces are stateless: `nonce/2' signs the issue timestamp with a per-node
%% secret, `check_nonce/3' re-derives it. A restart invalidates every nonce
%% (the secret is regenerated), which is exactly the right outcome: the
%% allocation that owned them is gone too.
-module(turn_auth).

-export([key/3, authenticate/3, nonce/2, check_nonce/3, nonce_lifetime_s/0]).

-export_type([result/0, config/0]).

-type config() :: #{realm := binary(),
                    secret := binary(),
                    action := atom(),
                    destination := binary()}.
-type result() :: {ok, #{principal := binary(),
                         tenant_id := binary() | undefined,
                         key := <<_:128>>}}
                | {error, {non_neg_integer(), binary()}}.

%% One hour: long enough that a client does not have to re-challenge on every
%% Refresh, short enough that a leaked nonce is not useful for a day.
-define(NONCE_MAX_AGE_S, 3600).
%% Tolerate a small clock skew into the future; a nonce "from the future" is
%% still a valid signature, and rejecting it would break a node whose clock
%% stepped back during the allocation.
-define(NONCE_SKEW_S, 60).

%% @doc RFC 5766 §4 key derivation. Exported so tests can pin the vector and so
%% a deployment tool can pre-compute a credential without speaking the wire.
-spec key(binary(), binary(), binary()) -> <<_:128>>.
key(Username, Realm, Password) ->
    crypto:hash(md5, [Username, <<":">>, Realm, <<":">>, Password]).

%% @doc Verify one authenticated TURN request.
%%
%% Returns the key the allocation must keep (it re-verifies every later
%% Refresh/CreatePermission/ChannelBind with it) and the identity ACL was
%% evaluated against. Errors carry the TURN error code the caller puts in the
%% ERROR-CODE attribute; 401 and 438 responses must also carry REALM + NONCE,
%% which is the caller's job because only it owns the configured realm.
-spec authenticate(stun_codec:message(), binary(), config()) -> result().
authenticate(#{attributes := Attrs}, Wire, #{realm := Realm} = Cfg) ->
    Username = stun_codec:attribute(Attrs, stun_codec:attribute_type(username)),
    RealmAttr = stun_codec:attribute(Attrs, stun_codec:attribute_type(realm)),
    Nonce = stun_codec:attribute(Attrs, stun_codec:attribute_type(nonce)),
    Integrity = stun_codec:attribute(Attrs, stun_codec:attribute_type(message_integrity)),
    case Integrity of
        undefined ->
            %% No MESSAGE-INTEGRITY at all: the client has not been challenged
            %% yet, so 401 is the answer that tells it which realm and nonce
            %% to use. This is the first packet of every TURN conversation and
            %% it carries no USERNAME (RFC 5766 §5).
            {error, {401, <<"Unauthorized">>}};
        _ ->
            %% From here on the client claims a credential, so the request has
            %% to be well-formed before it can be judged.
            case {Username, RealmAttr, Nonce} of
                {undefined, _, _} -> {error, {400, <<"Bad Request">>}};
                {_, undefined, _} -> {error, {400, <<"Bad Request">>}};
                {_, _, undefined} -> {error, {400, <<"Bad Request">>}};
                _ -> authenticate_1(Username, RealmAttr, Nonce, Wire, Cfg, Realm)
            end
    end.

authenticate_1(Username, RealmAttr, Nonce, Wire, Cfg, Realm) ->
    case RealmAttr of
        Realm ->
            case check_nonce(maps:get(secret, Cfg), Nonce,
                             erlang:system_time(second)) of
                ok -> authenticate_2(Username, Realm, Wire, Cfg);
                stale -> {error, {438, <<"Stale Nonce">>}};
                invalid -> {error, {438, <<"Stale Nonce">>}}
            end;
        _ ->
            {error, {441, <<"Wrong Credentials">>}}
    end.

authenticate_2(Username, Realm, Wire, Cfg) ->
    case broker_security:long_term_key(Username, Realm) of
        {ok, Key} ->
            case stun_codec:verify_integrity(Key, Wire) of
                true -> authorize(Username, Key, Cfg);
                false -> {error, {401, <<"Unauthorized">>}}
            end;
        {error, _} ->
            %% Unknown user and wrong password are indistinguishable, exactly
            %% like `broker_security:authenticate_conn/1'.
            {error, {401, <<"Unauthorized">>}}
    end.

authorize(Principal, Key, #{action := Action, destination := Destination}) ->
    Request = #{action => Action, destination => Destination},
    case broker_security:authorize(Principal, Request) of
        allow ->
            {ok, #{principal => Principal,
                   tenant_id => tenant_of(Principal),
                   key => Key}};
        deny ->
            {error, {403, <<"Forbidden">>}}
    end.

%% `tenant_id/1' answers `<<"default">>' for an unknown principal, which is the
%% right value for a user that just authenticated; kept behind a helper so the
%% auth module never constructs an identity map itself.
tenant_of(Principal) ->
    case broker_security:tenant_id(Principal) of
        <<"default">> -> undefined;
        Tenant -> Tenant
    end.

%% @doc A statistically unforgeable nonce bound to issue time and node secret.
-spec nonce(binary(), integer()) -> binary().
nonce(Secret, Now) when is_binary(Secret), is_integer(Now) ->
    Ts = <<Now:64>>,
    Mac = crypto:mac(hmac, sha256, Secret, Ts),
    base64:encode(<<Ts/binary, (binary:part(Mac, 0, 16))/binary>>).

%% @doc `ok' for a fresh, correctly signed nonce; `stale' for a valid signature
%% that is too old; `invalid' for anything else (including a non-binary).
-spec check_nonce(binary(), binary(), integer()) -> ok | stale | invalid.
check_nonce(Secret, Nonce, Now) when is_binary(Secret), is_binary(Nonce) ->
    try base64:decode(Nonce) of
        <<Ts:64, Mac:16/binary>> ->
            Expected = crypto:mac(hmac, sha256, Secret, <<Ts:64>>),
            case constant_time_equal(Mac, binary:part(Expected, 0, 16)) of
                false -> invalid;
                true -> nonce_age(Ts, Now)
            end;
        _ ->
            invalid
    catch
        _:_ -> invalid
    end;
check_nonce(_Secret, _Nonce, _Now) ->
    invalid.

nonce_age(Ts, Now) ->
    Age = Now - Ts,
    case Age =< ?NONCE_MAX_AGE_S andalso Age >= -?NONCE_SKEW_S of
        true -> ok;
        false -> stale
    end.

nonce_lifetime_s() ->
    ?NONCE_MAX_AGE_S.

constant_time_equal(A, B) when byte_size(A) =:= byte_size(B) ->
    crypto:exor(A, B) =:= binary:copy(<<0>>, byte_size(A));
constant_time_equal(_, _) ->
    false.
