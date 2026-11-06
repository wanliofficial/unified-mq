%% @doc Authentication stub (design §4.3): a config-driven plaintext user
%% table and nothing else. Real ACL / TLS belongs to S6.
%%
%% Pure functions reading `application' env -- there is no process here, the
%% same "reads never take a hop" shape §3.2【F3】 froze for `route/3'. The
%% users table is `{Username, #{password, tenant_id}}' tuples as written in
%% `config/sys.config.src'; every env read in this project uses the 3-arity
%% form because OTP 28 `get_env/2' returns `{ok, Val}' (§13 J3).
-module(broker_security).

-export([authenticate/2, authorize/2, tenant_id/1]).

-export_type([principal/0]).

%% §4.3: S1's Principal is the username itself.
-type principal() :: binary().

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

%% @doc ACL stub: §4.3 froze "一律返回 allow". Kept as an exported seam so the
%% conn_fsm call site is already in its S6 shape.
-spec authorize(principal(), term()) -> allow | deny.
authorize(_Principal, _Request) ->
    allow.

%% @doc The tenant the message's `tenant_id' slot is filled from (§4.1 field
%% table). A principal always authenticated through `authenticate/2', so its
%% record exists; the default keeps this total for hand-built principals.
-spec tenant_id(principal()) -> binary().
tenant_id(Principal) ->
    maps:get(tenant_id, user_record(Principal), <<"default">>).

%%%_* Internal --------------------------------------------------------------

user_record(Username) ->
    case lists:keyfind(Username, 1, users()) of
        {_, Record} -> Record;
        false -> #{}
    end.

users() ->
    application:get_env(broker_security, users, []).
