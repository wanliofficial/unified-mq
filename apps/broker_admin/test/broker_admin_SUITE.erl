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
     started_predicate_ignores_loaded_not_started_apps,
     status_reports_partitions,
     config_audit_hash_is_stable_and_redacts,
     version_reports_data_format,
     auth_is_required_when_a_token_is_configured,
     health_endpoints_skip_auth,
     unknown_endpoint_is_404,
     method_mismatch_is_405,
     backup_round_trip,
     restore_refuses_a_non_empty_target,
     drain_makes_readyz_503_and_suspends_a_real_listener,
     drain_keeps_in_use_connections_and_blocks_new_ones,
     undrain_resumes_accepting_and_readyz_recovers,
     drain_timeout_advances_state_to_down,
     redrain_replaces_the_earlier_grace_period,
     drain_rejects_a_malformed_body,
     cluster_members_rejects_wrong_method,
     cluster_members_validates_the_request,
     cluster_members_without_cluster_app_returns_503,
     node_view_reports_state_and_connections,
     node_view_reports_dist_tls_without_leaking_keys,
     insecure_bind_with_empty_token_refuses_to_start].

init_per_suite(Config) ->
    SavedToken = application:get_env(broker_admin, token, undefined),
    SavedEnabled = application:get_env(broker_admin, enabled, undefined),
    SavedPort = application:get_env(broker_admin, port, undefined),
    SavedBackupDir = application:get_env(broker_admin, backup_dir, undefined),
    SavedBind = application:get_env(broker_admin, bind, undefined),

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
    %% ranch is optional for the admin app (it reads it lazily), but the drain
    %% cases need a real ranch listener to suspend.
    {ok, _} = application:ensure_all_started(ranch),
    {ok, _} = application:ensure_all_started(broker_admin),
    Port = broker_admin_listener:port(),
    [{port, Port}, {backup_dir, Dir}, {probe_ref, {broker_admin_drain_probe, ct}},
     {saved_token, SavedToken}, {saved_enabled, SavedEnabled},
     {saved_port, SavedPort}, {saved_backup_dir, SavedBackupDir},
     {saved_bind, SavedBind} | Config].

end_per_suite(Config) ->
    _ = stop_probe(Config),
    _ = application:stop(broker_admin),
    restore(token, proplists:get_value(saved_token, Config)),
    restore(enabled, proplists:get_value(saved_enabled, Config)),
    restore(bind, proplists:get_value(saved_bind, Config)),
    restore(port, proplists:get_value(saved_port, Config)),
    restore(backup_dir, proplists:get_value(saved_backup_dir, Config)),
    _ = file:del_dir_r(proplists:get_value(backup_dir, Config)),
    ok.

init_per_testcase(_Case, Config) ->
    application:set_env(broker_admin, token, <<>>),
    Config.

%% A drain case always leaves the node in a non-serving state. Restore `up' so
%% the following case sees the same starting point as this one did, otherwise a
%% failed drain would silently poison every later case.
restore_state() ->
    _ = broker_drain:undrain(),
    ok.

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

%% A required app that is loaded but not started must not count as ready.
%% This is the S12-A / ADR-032 D2 regression: the old `started/1' keyed on
%% `application:get_application/1', which answers `{ok, App}' the moment the
%% app is *loaded* -- so a crashed-but-not-unloaded app kept `/readyz' at 200.
started_predicate_ignores_loaded_not_started_apps(Config) ->
    %% Drive the real endpoint, not a private predicate. Loading a required app
    %% does *not* start it, and the old implementation ("loaded counts as
    %% ready") answered 200 for exactly that state -- the bug ADR-032 D2 names.
    Required = [broker_storage, broker_router, broker_queue,
                broker_subscription, broker_event, broker_security],
    ok = ensure_loaded(Required),
    %% Expected set is derived at runtime: some required apps are started as a
    %% side effect of this suite (`broker_security' comes in with broker_admin),
    %% and other suites may leave some running. Deriving it keeps the assertion a
    %% property rather than a dependency on suite ordering.
    NotStarted = [A || A <- Required, not is_started(A)],
    %% Without at least one loaded-but-not-started app there is nothing to
    %% distinguish: old and new logic would agree.
    true = NotStarted =/= [] orelse ct:fail(no_loaded_not_started_app),
    {Status, Ready} = request(get, "/readyz", [], Config),
    #{<<"status">> := <<"not_ready">>, <<"missing">> := Missing0} = Ready,
    503 = Status,
    Missing = lists:sort([binary_to_atom(M, utf8) || M <- Missing0]),
    case Missing =:= lists:sort(NotStarted) of
        true -> ok;
        false -> ct:fail({missing_apps_mismatch, Missing, lists:sort(NotStarted)})
    end,
    %% Positive control: an app that really is started is never reported missing.
    [false = lists:member(A, Missing) || A <- Required, is_started(A)],
    ok.

is_started(App) -> lists:keymember(App, 1, application:which_applications()).

ensure_loaded(Apps) ->
    lists:foreach(
      fun(A) ->
              case application:load(A) of
                  ok -> ok;
                  {error, {already_loaded, A}} -> ok
              end
      end, Apps),
    ok.

metrics_endpoint_lists_the_closed_key_set(Config) ->
    {200, Body} = raw_request(get, "/metrics", [], <<>>, Config),
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

%%%_* Drain (S12-A, ADR-032 D2) -------------------------------------------

%% A real ranch listener, started under the node's ranch supervisor. This is the
%% same machinery the data plane uses, so `broker_drain' has to work against it:
%% if drain only worked against a mock, the claim "stop accepting but do not
%% disconnect in-use connections" would be untested.
%%
%% The port is captured once, while the listener is running: `ranch:get_port/1'
%% reports `undefined' after a suspend (ranch sets the listener address to
%% `{undefined, undefined}'), so looking it up again would make "a new connection
%% is refused" indistinguishable from "the test cannot find the port".
start_probe(Config) ->
    Ref = proplists:get_value(probe_ref, Config),
    _ = stop_probe(Config),
    {ok, _} = ranch:start_listener(Ref, ranch_tcp,
                                   #{socket_opts => [{port, 0}],
                                     num_acceptors => 1},
                                   broker_admin_drain_probe, #{}),
    Port = case ranch:get_port(Ref) of
               P when is_integer(P) -> P;
               Other -> exit({probe_port_unavailable, Other})
           end,
    persistent_term:put({?MODULE, probe_port}, Port),
    Port.

stop_probe(Config) ->
    Ref = proplists:get_value(probe_ref, Config),
    %% ranch may not be running: the bind-guard case stops the admin app, and
    %% that app is the only reason ranch is up here.
    _ = try ranch:stop_listener(Ref) catch _:_ -> ok end,
    _ = persistent_term:erase({?MODULE, probe_port}),
    ok.

probe_port(Config) ->
    Ref = proplists:get_value(probe_ref, Config),
    %% A suspended listener reports `undefined' (`ranch' sets its address to
    %% `{undefined, undefined}'), while an `undrain' of a `port => 0' listener
    %% picks a *new* ephemeral port. So: prefer the live address, fall back to the
    %% one captured at start.
    try ranch:get_port(Ref) of
        Port when is_integer(Port) -> Port;
        _ -> persistent_term:get({?MODULE, probe_port})
    catch
        _:_ -> persistent_term:get({?MODULE, probe_port})
    end.

%% `ok' on success, `{error, _}' when the acceptor is suspended.
connect_probe(Config) ->
    gen_tcp:connect({127, 0, 0, 1}, probe_port(Config),
                    [binary, {active, false}, {packet, raw}], 2000).

probe_status(Config) ->
    #{status := Status} = ranch:info(proplists:get_value(probe_ref, Config)),
    Status.

drain(Config) ->
    request(post, "/api/v1/drain", [], Config).

drain(Config, Body) ->
    request_json(post, "/api/v1/drain", [], Body, Config).

undrain(Config) ->
    request(post, "/api/v1/undrain", [], Config).

%% The drain must be visible to the LB-facing contract and to `/readyz': 503 with
%% `status => draining', while `/healthz' stays 200 (D2: a draining node is still
%% healthy, it is just not accepting traffic).
drain_makes_readyz_503_and_suspends_a_real_listener(Config) ->
    start_probe(Config),
    try
        {ok, Sock} = connect_probe(Config),
        ok = gen_tcp:send(Sock, <<"ping">>),
        {ok, <<"ping">>} = gen_tcp:recv(Sock, 0, 3000),
        running = probe_status(Config),

        {200, Drained} = drain(Config),
        #{<<"node">> := Node, <<"state">> := <<"draining">>,
          <<"ready">> := false} = Drained,
        true = byte_size(Node) > 0,
        %% The listener really is suspended, as ranch reports it.
        suspended = probe_status(Config),

        {503, #{<<"status">> := <<"draining">>}} = request(get, "/readyz", [], Config),
        %% Liveness is untouched by drain.
        {200, #{<<"status">> := <<"ok">>}} = request(get, "/healthz", [], Config),
        {200, #{<<"state">> := <<"draining">>, <<"ready">> := false}} =
            request(get, "/api/v1/node", [], Config),
        ok = gen_tcp:close(Sock)
    after
        restore_state()
    end.

%% The two halves of D2, proven separately: an in-use connection survives the
%% drain, and a new one cannot be established afterwards.
drain_keeps_in_use_connections_and_blocks_new_ones(Config) ->
    start_probe(Config),
    try
        {ok, Old} = connect_probe(Config),
        ok = gen_tcp:send(Old, <<"hello">>),
        {ok, <<"hello">>} = gen_tcp:recv(Old, 0, 3000),

        {200, _} = drain(Config),

        %% In-use connection: still alive, still echoes.
        ok = gen_tcp:send(Old, <<"again">>),
        {ok, <<"again">>} = gen_tcp:recv(Old, 0, 3000),

        %% New connection: refused, because the acceptors are gone.
        {error, _} = connect_probe(Config),
        suspended = probe_status(Config),
        ok = gen_tcp:close(Old)
    after
        restore_state()
    end.

%% After an `undrain' the listener accepts again and `/readyz' stops reporting
%% the drain. Readiness itself may still be 503 (the data plane is not booted in
%% this suite); the property under test is that the drain status is gone.
undrain_resumes_accepting_and_readyz_recovers(Config) ->
    start_probe(Config),
    try
        {200, _} = drain(Config),
        {error, _} = connect_probe(Config),
        suspended = probe_status(Config),

        {200, _} = undrain(Config),
        up = broker_drain:status(),
        running = probe_status(Config),

        {ok, Sock} = connect_probe(Config),
        ok = gen_tcp:send(Sock, <<"x">>),
        {ok, <<"x">>} = gen_tcp:recv(Sock, 0, 3000),

        {Status, Ready} = request(get, "/readyz", [], Config),
        true = Status =:= 200 orelse Status =:= 503,
        case Ready of
            #{<<"status">> := <<"draining">>} -> ct:fail(still_draining);
            #{<<"status">> := <<"down">>} -> ct:fail(still_down);
            _ -> ok
        end,
        ok = gen_tcp:close(Sock)
    after
        restore_state()
    end.

%% The grace period has a real consumer (R-E6): after it the node advances to
%% `down' on its own, without an explicit `undrain'. It must still not disturb
%% the in-use connection.
drain_timeout_advances_state_to_down(Config) ->
    start_probe(Config),
    try
        {ok, Old} = connect_probe(Config),
        ok = gen_tcp:send(Old, <<"hold">>),
        {ok, <<"hold">>} = gen_tcp:recv(Old, 0, 3000),

        %% 0ms grace period: drain now, advance to `down' immediately.
        {200, _} = drain(Config, #{timeout_ms => 0}),

        %% The advance is asynchronous (`send_after'), so wait for it instead of
        %% assuming the reply implies it already happened.
        down = wait_state(down, 50),

        {503, #{<<"status">> := <<"down">>}} = request(get, "/readyz", [], Config),
        %% The connection is still up: `down' is a signal, not a disconnect.
        ok = gen_tcp:send(Old, <<"still">>),
        {ok, <<"still">>} = gen_tcp:recv(Old, 0, 3000),
        ok = gen_tcp:close(Old)
    after
        restore_state()
    end.

%% A second drain must *replace* the first one's deadline rather than run beside
%% it. An operator who re-drains with a longer grace period is extending the
%% window; honouring the older, shorter timer would take the node down early.
%%
%% `cancel_timer/1' alone cannot guarantee this: a message already delivered to
%% the mailbox cannot be recalled, so the fix tags each timer with a generation
%% and disowns stale ones.
redrain_replaces_the_earlier_grace_period(Config) ->
    try
        %% Short deadline, then a much longer one.
        {200, _} = drain(Config, #{timeout_ms => 150}),
        {200, _} = drain(Config, #{timeout_ms => 30000}),
        %% Well past the first deadline, nowhere near the second.
        timer:sleep(600),
        %% The old 150ms timer must not have advanced us.
        draining = broker_drain:status()
    after
        restore_state()
    end.

wait_state(Expected, 0) ->
    ct:fail({state_did_not_become, Expected, broker_drain:status()});
wait_state(Expected, N) ->
    case broker_drain:status() of
        Expected -> Expected;
        _ ->
            timer:sleep(20),
            wait_state(Expected, N - 1)
    end.

%% R-LB8: the body is validated strictly. A typo'd field is a 400, not a
%% silently ignored drain -- and a rejected request must not drain anything.
drain_rejects_a_malformed_body(Config) ->
    try
        {400, #{<<"error">> := <<"invalid_json">>}} =
            request_raw(post, "/api/v1/drain", [], <<"{bad json">>, Config),
        {400, #{<<"error">> := <<"unknown_field">>,
                <<"field">> := <<"bogus">>}} =
            request_raw(post, "/api/v1/drain", [], <<"{\"bogus\":1}">>, Config),
        {400, #{<<"error">> := <<"invalid_timeout_ms">>}} =
            request_raw(post, "/api/v1/drain", [],
                        <<"{\"timeout_ms\":\"soon\"}">>, Config),
        {400, #{<<"error">> := <<"invalid_timeout_ms">>}} =
            request_raw(post, "/api/v1/drain", [],
                        <<"{\"timeout_ms\":-1}">>, Config),
        %% Accepted shapes still work: `{}' means "use the configured timeout".
        up = broker_drain:status(),
        {200, _} = drain(Config, #{}),
        draining = broker_drain:status()
    after
        restore_state()
    end.

%%%_* Online cluster membership change (ADR-033 D2) ----------------------

%% The endpoint is token-protected like every other `/api/' route, so a GET is
%% a method mismatch (405), not a bypass.
cluster_members_rejects_wrong_method(Config) ->
    {405, #{<<"error">> := <<"method_not_allowed">>}} =
        request_raw(get, "/api/v1/cluster/members", [], <<>>, Config),
    ok.

%% Malformed requests are rejected before any ra call: bad JSON, missing fields,
%% an unknown action, and an unknown node each get a precise 400. The node check
%% uses `binary_to_existing_atom', so a name this node has never heard of cannot
%% mint a new atom.
cluster_members_validates_the_request(Config) ->
    {400, #{<<"error">> := <<"invalid_json">>}} =
        request_raw(post, "/api/v1/cluster/members", [], <<"{bad json">>, Config),
    {400, #{<<"error">> := <<"body_required">>}} =
        request_raw(post, "/api/v1/cluster/members", [], <<>>, Config),
    {400, #{<<"error">> := <<"body_must_have_action_and_node">>}} =
        request_raw(post, "/api/v1/cluster/members", [],
                    <<"{\"action\":\"join\"}">>, Config),
    {400, #{<<"error">> := <<"invalid_action">>}} =
        request_raw(post, "/api/v1/cluster/members", [],
                    <<"{\"action\":\"explode\",\"node\":\"n@h\"}">>, Config),
    {400, #{<<"error">> := <<"invalid_node">>}} =
        request_raw(post, "/api/v1/cluster/members", [],
                    <<"{\"action\":\"join\",\"node\":42}">>, Config),
    %% A well-formed node name this VM does not know is a 400, never a new atom.
    {400, #{<<"error">> := <<"unknown_node">>}} =
        request_raw(post, "/api/v1/cluster/members", [],
                    <<"{\"action\":\"join\",\"node\":\"no_such_node@nowhere\"}">>,
                    Config),
    ok.

%% With a well-formed request but no cluster running, the endpoint answers 503
%% `cluster_not_running' rather than crashing the listener. The success path
%% (a real join/leave against a live group) is exercised in `broker_cluster_dist_SUITE';
%% here only the guard and the dispatch are pinned.
cluster_members_without_cluster_app_returns_503(Config) ->
    false = lists:keymember(broker_cluster, 1,
                            application:which_applications()),
    Self = atom_to_binary(node(), utf8),
    {503, #{<<"error">> := <<"cluster_not_running">>,
            <<"action">> := <<"join">>}} =
        request_json(post, "/api/v1/cluster/members", [],
                     #{<<"action">> => <<"join">>, <<"node">> => Self}, Config),
    ok.

node_view_reports_state_and_connections(Config) ->
    start_probe(Config),
    try
        {ok, Sock} = connect_probe(Config),
        ok = gen_tcp:send(Sock, <<"x">>),
        {ok, <<"x">>} = gen_tcp:recv(Sock, 0, 3000),

        {200, Up} = request(get, "/api/v1/node", [], Config),
        #{<<"state">> := <<"up">>, <<"ready">> := true,
          <<"connections">> := #{<<"active">> := 1,
                                 <<"total">> := 1}} = Up,

        {200, _} = drain(Config),
        {200, Draining} = request(get, "/api/v1/node", [], Config),
        %% A suspended listener keeps its connections: the view must not claim
        %% the node is idle just because it stopped accepting.
        #{<<"state">> := <<"draining">>, <<"ready">> := false,
          <<"connections">> := #{<<"active">> := 1}} = Draining,
        ok = gen_tcp:close(Sock)
    after
        restore_state()
    end.

%% The node view is what an external LB and an operator read, so the dist TLS
%% posture belongs there -- but it must never carry key material, and it must
%% answer even when the cluster app is not running.
node_view_reports_dist_tls_without_leaking_keys(_Config) ->
    application:set_env(broker_cluster, dist_tls,
                        #{enabled => false, certfile => "/secret/path/node.crt",
                          keyfile => "/secret/path/node.key"}),
    Map = broker_admin_http:node_view(),
    DistTls = maps:get(dist_tls, Map),
    false = maps:get(enabled, DistTls),
    <<"node.crt">> = maps:get(certfile, DistTls),
    false = maps:is_key(keyfile, DistTls),
    %% The path must not leak: basename only.
    Json = iolist_to_binary(json:encode(broker_admin_http:json_safe(Map))),
    nomatch = binary:match(Json, <<"/secret/path">>),
    ok.

%% ADR-032 D2's last clause: a non-loopback bind with an empty token must refuse
%% to start, rather than serving an unauthenticated management API on every
%% interface. That invariant used to live only in a comment.
insecure_bind_with_empty_token_refuses_to_start(Config) ->
    SavedBind = proplists:get_value(saved_bind, Config),
    try
        _ = stop_probe(Config),
        _ = application:stop(broker_admin),
        application:set_env(broker_admin, bind, {0, 0, 0, 0}),
        application:set_env(broker_admin, token, <<>>),
        application:set_env(broker_admin, port, 0),
        {error, StartError} = application:ensure_all_started(broker_admin),
        %% The supervisor refuses at `init/1'; OTP wraps that in `bad_return'.
        true = contains(StartError, {insecure_bind, {0, 0, 0, 0}, empty_token})
            orelse ct:fail({wrong_start_error, StartError}),
        undefined = broker_admin_listener:port(),

        %% With a token it may listen widely: the guard is about the
        %% unauthenticated combination, not about the bind address.
        application:set_env(broker_admin, token, ?TOKEN),
        {ok, _} = application:ensure_all_started(broker_admin),
        true = is_integer(broker_admin_listener:port())
    after
        _ = application:stop(broker_admin),
        application:set_env(broker_admin, bind, {127, 0, 0, 1}),
        application:set_env(broker_admin, token, <<>>),
        application:set_env(broker_admin, port, 0),
        {ok, _} = application:ensure_all_started(broker_admin),
        restore(bind, SavedBind)
    end.

%% Whether a (possibly nested) start error mentions `Needle'.
contains(Term, Needle) when Term =:= Needle -> true;
contains(Term, Needle) when is_tuple(Term) ->
    contains(tuple_to_list(Term), Needle);
contains(Term, Needle) when is_list(Term) ->
    lists:any(fun(E) -> contains(E, Needle) end, Term);
contains(_, _) -> false.

%%%_* HTTP client ---------------------------------------------------------

%% A POST with a JSON body, for the drain input-validation cases (R-LB8).
%% `Body' is a map; encoding happens here so each case states the request, not
%% the wire format.
request_json(Method, Path, Headers, Body, Config) ->
    request_raw(Method, Path, Headers, json:encode(Body), Config).

request(Method, Path, Headers) ->
    request(Method, Path, Headers, [{port, broker_admin_listener:port()}]).

request(Method, Path, Headers, Config) ->
    request_raw(Method, Path, Headers, <<>>, Config).

request_raw(Method, Path, Headers, Body, Config) ->
    {Status, RespBody} = raw_request(Method, Path, Headers, Body, Config),
    try json:decode(iolist_to_binary(RespBody)) of
        Decoded -> {Status, Decoded}
    catch
        _:_ -> {Status, RespBody}
    end.

raw_request(Method, Path) ->
    raw_request(Method, Path, [], <<>>, []).

raw_request(Method, Path, Headers) ->
    raw_request(Method, Path, Headers, <<>>, []).

raw_request(Method, Path, Headers, Body0, Config) ->
    Body = iolist_to_binary(Body0),
    Port = proplists:get_value(port, Config),
    {ok, Sock} = gen_tcp:connect({127, 0, 0, 1}, Port,
                                 [binary, {active, false}], 5000),
    HeaderLines =
        [[H, ": ", V, "\r\n"] || {H, V} <- Headers],
    Req = [method_bin(Method), " ", Path, " HTTP/1.1\r\n",
           "Host: localhost\r\n", HeaderLines,
           "Content-Type: application/json\r\n",
           "Content-Length: ", integer_to_binary(byte_size(Body)), "\r\n\r\n",
           Body],
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
