%% @doc Configuration audit (S6, roadmap §8.2).
%%
%% At boot the broker writes one immutable snapshot of the effective
%% configuration -- passwords and key material removed -- plus a SHA-256 of the
%% canonical form. The snapshot is what the admin API serves at
%% `/api/v1/config' and what `docs/operations.md' tells an operator to capture
%% before an upgrade; the hash makes "did this node run the config I think it
%% did" a one-line comparison.
%%
%% Two properties are load-bearing:
%%
%%   * **Redaction happens at the source.** Each module exposes its own
%%     `redacted_config/0'; this module never receives a plaintext password.
%%   * **The audit never blocks boot.** A failure to compute or write the
%%     snapshot is logged and swallowed -- an audit record is evidence, not a
%%     gate, and a broken audit must not turn a healthy broker into a down one.
-module(broker_config_audit).

-export([snapshot/0, hash/0, log/0, write/1]).

%% @doc The redacted, effective configuration as a nested map.
-spec snapshot() -> map().
snapshot() ->
    #{security => safe(fun broker_security:redacted_config/0),
      limits => safe(fun broker_limits:limits/0),
      tls => safe(fun broker_security_tls:redacted_config/0),
      storage => #{backend => env(broker_storage, backend, broker_storage_wal),
                   data_dir => env(broker_storage, data_dir, "data")},
      subscription => #{data_dir => env(broker_subscription, data_dir, "data"),
                        max_attempts => env(broker_subscription, max_attempts, 10)},
      event => #{policy => env(broker_event, policy, pass_through)},
      metrics => #{enabled => env(broker_metrics, enabled, true)},
      admin => #{enabled => env(broker_admin, enabled, true),
                 bind => env(broker_admin, bind, {127, 0, 0, 1}),
                 port => env(broker_admin, port, 8090)},
      version => safe(fun broker_version:info/0)}.

%% @doc SHA-256 of the canonical (key-sorted, term-to-binary) snapshot, as a
%% lowercase hex binary.
-spec hash() -> binary().
hash() ->
    Canonical = canonical(snapshot()),
    hex(crypto:hash(sha256, term_to_binary(Canonical, [{minor_version, 2}]))).

%% @doc Log the snapshot + hash once at boot. Always returns `ok'.
-spec log() -> ok.
log() ->
    try
        Snapshot = snapshot(),
        H = hash(),
        logger:notice("broker_config_audit hash=~s snapshot=~0p", [H, Snapshot]),
        _ = write(#{hash => H, snapshot => Snapshot,
                    at => erlang:system_time(second)}),
        ok
    catch
        Class:Reason:Stack ->
            logger:warning("broker_config_audit failed ~p:~p ~p",
                           [Class, Reason, Stack]),
            ok
    end.

%% @doc Append one JSON line to `<data_dir>/config-audit.log'. Best-effort.
-spec write(map()) -> ok | {error, term()}.
write(Entry) ->
    DataDir = env(broker_storage, data_dir, "data"),
    Path = filename:join(DataDir, "config-audit.log"),
    try
        _ = filelib:ensure_dir(Path),
        Line = [json:encode(Entry), $\n],
        case file:open(Path, [append, raw, binary]) of
            {ok, Fd} ->
                Result = file:write(Fd, Line),
                _ = file:close(Fd),
                Result;
            {error, _} = Error ->
                Error
        end
    catch
        _:_ -> {error, audit_write_failed}
    end.

%%%_* Internal --------------------------------------------------------------

%% Recursively sort map keys and drop non-JSON values (pids, refs, functions)
%% so two runs with the same config hash identically and the audit file stays
%% parseable.
canonical(Term) when is_map(Term) ->
    [{K, canonical(V)} || {K, V} <- lists:sort(maps:to_list(Term)),
                          json_safe(V)];
canonical(Term) when is_list(Term) ->
    [canonical(V) || V <- Term];
canonical(Term) when is_tuple(Term) ->
    {tuple, canonical(tuple_to_list(Term))};
canonical(Term) when is_binary(Term); is_number(Term); is_atom(Term) ->
    Term;
canonical(_) ->
    null.

json_safe(V) ->
    not (is_pid(V) orelse is_reference(V) orelse is_function(V) orelse
         is_port(V)).

hex(Bin) ->
    << <<(hex_digit(N))>> || <<N:4>> <= Bin >>.

hex_digit(N) when N < 10 -> $0 + N;
hex_digit(N) -> $a + (N - 10).

safe(Fun) ->
    try Fun() catch _:_ -> unavailable end.

env(App, Key, Default) ->
    case application:get_env(App, Key, Default) of
        undefined -> Default;
        V -> V
    end.
