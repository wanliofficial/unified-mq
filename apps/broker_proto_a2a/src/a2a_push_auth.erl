%% @doc Replay-proof outbound headers for push notifications (ADR-026 §D6.6).
%%
%% Two rules:
%%
%%   * **Secrets never come from the client.** `TaskPushNotificationConfig'
%%     carries an `authentication.scheme' *name* only; the material is looked up
%%     in this node's configuration (`push_auth_secrets'). A client cannot make
%%     the Broker sign with a secret it does not already hold.
%%   * **Every request self-identifies and cannot be replayed.** `X-A2A-Timestamp'
%%     (unix milliseconds) and `X-A2A-Nonce' are *project-private headers, not
%%     A2A spec fields* (they are listed as such in the capability matrix), and
%%     the optional `X-A2A-Signature' is
%%     `sha256=<hex hmac(timestamp "." nonce "." body)>' keyed by the configured
%%     secret. The receiver compares the timestamp against its own clock and
%%     refuses a nonce it has already seen; the Broker documents the window
%%     (`push_replay_window_ms') rather than silently choosing one.
-module(a2a_push_auth).

-export([headers/3, secret_for/1, signature/2, schemes/0]).

%% @doc Headers for one delivery. `Scheme' is `undefined' (no authentication,
%% timestamp + nonce only) or a configured scheme name.
-spec headers(binary() | undefined, iodata(), map()) ->
          {ok, [{binary(), binary()}]} | {error, term()}.
headers(Scheme, Body, Config) ->
    Timestamp = integer_to_binary(erlang:system_time(millisecond)),
    Nonce = nonce(),
    Base = [{<<"x-a2a-timestamp">>, Timestamp},
            {<<"x-a2a-nonce">>, Nonce},
            {<<"x-a2a-task-id">>, maps:get(task_id, Config, <<>>)},
            {<<"x-a2a-config-id">>, maps:get(config_id, Config, <<>>)}],
    case Scheme of
        undefined ->
            {ok, Base};
        _ ->
            case secret_for(Scheme) of
                {ok, #{secret := Secret} = Auth} ->
                    Sig = signature(Secret, <<Timestamp/binary, ".",
                                              Nonce/binary, ".",
                                              (iolist_to_binary(Body))/binary>>),
                    Extra = case maps:get(header, Auth, bearer) of
                                bearer ->
                                    [{<<"authorization">>,
                                      <<"Bearer ", Secret/binary>>}];
                                api_key ->
                                    [{<<"x-api-key">>, Secret}];
                                none ->
                                    []
                            end,
                    {ok, Base ++ [{<<"x-a2a-signature">>, <<"sha256=", Sig/binary>>}
                                  | Extra]};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

%% @doc The configured secret for one scheme, from `push_auth_secrets':
%%       `#{<<"bearer">> => #{secret => <<"...">>, header => bearer}}'.
-spec secret_for(binary()) -> {ok, map()} | {error, not_configured}.
secret_for(Scheme) when is_binary(Scheme) ->
    Configured = application:get_env(broker_proto_a2a, push_auth_secrets, #{}),
    case maps:get(Scheme, Configured, undefined) of
        #{secret := _} = Spec -> {ok, Spec};
        _ -> {error, not_configured}
    end;
secret_for(_Other) ->
    {error, not_configured}.

-spec signature(binary(), binary()) -> binary().
signature(Secret, Data) ->
    Hex = crypto:mac(hmac, sha256, Secret, Data),
    binary:encode_hex(Hex, lowercase).

%% @doc Scheme names an operator may configure; used to reject a client naming
%% something we cannot sign with.
-spec schemes() -> [binary()].
schemes() ->
    maps:keys(application:get_env(broker_proto_a2a, push_auth_secrets, #{})).

nonce() ->
    base64:encode(crypto:strong_rand_bytes(16)).
