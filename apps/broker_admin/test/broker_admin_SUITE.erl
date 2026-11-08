%% @doc S6 (roadmap §8.2): the admin API, config audit, backup and version.
%%
%% The suite exercises the HTTP surface through the real listener (ephemeral
%% port) plus the supporting modules directly. It pins the properties an
%% operator depends on: health/readiness, auth enforcement, the closed metric
%% set appearing in the exposition, a backup that can be restored, and a config
%% snapshot whose hash is stable and whose passwords are gone.
-module(broker_admin_SUITE).

-compile([export_all, nowarn_export_all]).

-define(TOKEN, <<"s3cret-token">>).

all() ->
    [healthy_and_ready,
     metrics_endpoint_lists_the_closed_key_set,
     status_reports_partitions,
     config_audit_hash_is_stable_and_redacts,
     version_reports_data_format,
     auth_is_required_when_a_token_is_configured,
     health_endpoints_skip_auth,
     unknown_endpoint_is_404,
     method_mismatch_is_405,
     backup_round_trip,
     restore_refuses_a_non_empty_target].

init_per_suite(Config) ->
    SavedToken = application:get_env(broker_admin, token, undefined),
    SavedEnabled = application:get_env(broker_admin, enabled, undefined),
    SavedPort = application:get_env(broker_admin, port, undefined),
    SavedBackupDir = application:get_env(broker_admin, backup_dir, undefined),

    %% Load first: `set_env' before the app is loaded is overwritten by the
    %% `.app' file's env default (here `port 8090'), which is exactly the
    %% collision this suite must avoid. Load, then override, then start.
    _ = application:load(broker_admin),
    application:set_env(broker_admin, token, <<>>),
    application:set_env(broker_admin, port, 0),
    Dir = filename:join(["/tmp/opencode", "admin_suite"]),
    _ = file:del_dir_r(Dir),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    application:set_env(broker_admin, backup_dir, Dir),

    {ok, _} = application:ensure_all_started(broker_metrics),
    {ok, _} = application:ensure_all_started(broker_admin),
    Port = broker_admin_listener:port(),
    [{port, Port}, {backup_dir, Dir},
     {saved_token, SavedToken}, {saved_enabled, SavedEnabled},
     {saved_port, SavedPort}, {saved_backup_dir, SavedBackupDir} | Config].

end_per_suite(Config) ->
    _ = application:stop(broker_admin),
    restore(token, proplists:get_value(saved_token, Config)),
    restore(enabled, proplists:get_value(saved_enabled, Config)),
    restore(port, proplists:get_value(saved_port, Config)),
    restore(backup_dir, proplists:get_value(saved_backup_dir, Config)),
    _ = file:del_dir_r(proplists:get_value(backup_dir, Config)),
    ok.

init_per_testcase(_Case, Config) ->
    application:set_env(broker_admin, token, <<>>),
    Config.

%%%_* HTTP ----------------------------------------------------------------

healthy_and_ready(Config) ->
    {200, Health} = request(get, "/healthz", [], Config),
    #{<<"status">> := <<"ok">>} = Health,
    %% The admin suite runs without the data plane, so readiness is allowed to
    %% be 503 -- but it must answer structurally, naming what is missing.
    {ReadyStatus, Ready} = request(get, "/readyz", [], Config),
    true = ReadyStatus =:= 200 orelse ReadyStatus =:= 503,
    case Ready of
        #{<<"status">> := <<"ready">>} -> ok;
        #{<<"status">> := <<"not_ready">>, <<"missing">> := Missing} ->
            true = is_list(Missing)
    end,
    ok.

metrics_endpoint_lists_the_closed_key_set(Config) ->
    {200, Body} = raw_request(get, "/metrics", [], Config),
    Bin = iolist_to_binary(Body),
    %% Metric *names* are the closed key set. Labels are only the histogram
    %% `le' boundary (a fixed bucket), never a `source'/`subject'/`id' value.
    true = binary:match(Bin, <<"publish_total">>) =/= nomatch,
    true = binary:match(Bin, <<"authz_failures">>) =/= nomatch,
    nomatch = binary:match(Bin, <<"source=">>),
    nomatch = binary:match(Bin, <<"subject=">>),
    nomatch = binary:match(Bin, <<"id=">>),
    ok.

status_reports_partitions(_Config) ->
    {200, Status} = request(get, "/api/v1/status", []),
    true = maps:is_key(<<"partitions">>, Status),
    true = maps:is_key(<<"version">>, Status),
    ok.
config_audit_hash_is_stable_and_redacts(_Config) ->
    H1 = broker_config_audit:hash(),
    H2 = broker_config_audit:hash(),
    H1 = H2,
    true = byte_size(H1) =:= 64,
    Snapshot = broker_config_audit:snapshot(),
    #{security := #{users := Users}} = Snapshot,
    %% No password field survives redaction.
    lists:foreach(
      fun({_Name, Rec}) -> false = maps:is_key(password, Rec) end, Users),
    {200, Body} = request(get, "/api/v1/config", []),
    true = maps:is_key(<<"hash">>, Body),
    ok.

version_reports_data_format(_Config) ->
    {200, Version} = request(get, "/api/v1/version", []),
    #{<<"data_version">> := 1, <<"release">> := _} = Version,
    true = broker_version:compatible(1),
    false = broker_version:compatible(999),
    ok.

auth_is_required_when_a_token_is_configured(Config) ->
    application:set_env(broker_admin, token, ?TOKEN),
    {401, #{<<"error">> := <<"unauthorized">>}} =
        request(get, "/api/v1/status", [], Config),
    {401, _} = request(get, "/api/v1/status",
                       [{<<"authorization">>, <<"Bearer wrong">>}], Config),
    {200, _} = request(get, "/api/v1/status",
                       [{<<"authorization">>, <<"Bearer ", ?TOKEN/binary>>}], Config),
    {200, _} = request(get, "/api/v1/status",
                       [{<<"x-admin-token">>, ?TOKEN}], Config),
    ok.

health_endpoints_skip_auth(Config) ->
    application:set_env(broker_admin, token, ?TOKEN),
    {200, _} = request(get, "/healthz", [], Config),
    %% /readyz skips auth too; its status may be 200 or 503 depending on whether
    %% the data plane runs in this suite, but never 401.
    {ReadyStatus, _} = request(get, "/readyz", [], Config),
    true = ReadyStatus =/= 401,
    ok.

unknown_endpoint_is_404(Config) ->
    {404, _} = request(get, "/nope", [], Config),
    ok.

method_mismatch_is_405(Config) ->
    {405, _} = request(post, "/healthz", [], Config),
    ok.

%%%_* Backup / restore -----------------------------------------------------

backup_round_trip(Config) ->
    Dir = proplists:get_value(backup_dir, Config),
    %% Seed a nice little tree so the archive has something to carry. The backup
    %% reads `broker_storage.data_dir', so point it at our temp dir.
    SavedDataDir = application:get_env(broker_storage, data_dir, undefined),
    try
        application:set_env(broker_storage, data_dir, Dir),
        ok = filelib:ensure_dir(filename:join([Dir, "wal", "0", "x"])),
        ok = file:write_file(filename:join([Dir, "wal", "0", "segment-1.log"]),
                             <<"payload">>),
        ok = filelib:ensure_dir(filename:join([Dir, "sessions", "x"])),
        ok = file:write_file(filename:join([Dir, "sessions", "s.subs"]), <<"{}">>),

        {ok, Archive} = broker_backup:create(<<"roundtrip">>),
        true = filelib:is_regular(Archive),
        [_] = broker_backup:list(),

        Target = filename:join([Dir, "restored"]),
        {ok, Target} = broker_backup:restore(Archive, Target),
        true = filelib:is_regular(filename:join([Target, "wal", "0",
                                                 "segment-1.log"])),
        <<"payload">> = element(2, file:read_file(
                                    filename:join([Target, "wal", "0",
                                                   "segment-1.log"]))),
        true = filelib:is_regular(filename:join([Target, "sessions", "s.subs"])),
        ok
    after
        case SavedDataDir of
            undefined -> application:unset_env(broker_storage, data_dir);
            V -> application:set_env(broker_storage, data_dir, V)
        end
    end.

restore_refuses_a_non_empty_target(Config) ->
    Dir = proplists:get_value(backup_dir, Config),
    {ok, Archive} = broker_backup:create(<<"nonempty">>),
    Target = filename:join([Dir, "occupied"]),
    ok = filelib:ensure_dir(filename:join(Target, "x")),
    ok = file:write_file(filename:join(Target, "existing"), <<"x">>),
    {error, {target_not_empty, _}} = broker_backup:restore(Archive, Target),
    ok.

%%%_* HTTP client ---------------------------------------------------------

request(Method, Path, Headers) ->
    request(Method, Path, Headers, [{port, broker_admin_listener:port()}]).

request(Method, Path, Headers, Config) ->
    {Status, Body} = raw_request(Method, Path, Headers, Config),
    try json:decode(iolist_to_binary(Body)) of
        Decoded -> {Status, Decoded}
    catch
        _:_ -> {Status, Body}
    end.

raw_request(Method, Path) ->
    raw_request(Method, Path, [], []).

raw_request(Method, Path, Headers) ->
    raw_request(Method, Path, Headers, []).

raw_request(Method, Path, Headers, Config) ->
    Port = proplists:get_value(port, Config),
    {ok, Sock} = gen_tcp:connect({127, 0, 0, 1}, Port,
                                 [binary, {active, false}], 5000),
    HeaderLines =
        [[H, ": ", V, "\r\n"] || {H, V} <- Headers],
    Req = [method_bin(Method), " ", Path, " HTTP/1.1\r\n",
           "Host: localhost\r\n", HeaderLines, "Content-Length: 0\r\n\r\n"],
    ok = gen_tcp:send(Sock, Req),
    Resp = recv_all(Sock, <<>>),
    _ = gen_tcp:close(Sock),
    parse_response(Resp).

method_bin(get) -> "GET";
method_bin(post) -> "POST";
method_bin(put) -> "PUT";
method_bin(delete) -> "DELETE".

recv_all(Sock, Acc) ->
    case gen_tcp:recv(Sock, 0, 5000) of
        {ok, Data} -> recv_all(Sock, <<Acc/binary, Data/binary>>);
        {error, closed} -> Acc;
        {error, _} -> Acc
    end.

parse_response(Resp) ->
    [Head, Body] = binary:split(Resp, <<"\r\n\r\n">>),
    [StatusLine | _] = binary:split(Head, <<"\r\n">>, [global]),
    [_, Code | _] = binary:split(StatusLine, <<" ">>, [global]),
    {binary_to_integer(Code), Body}.

restore(Key, undefined) ->
    application:unset_env(broker_admin, Key);
restore(Key, Value) ->
    application:set_env(broker_admin, Key, Value).
