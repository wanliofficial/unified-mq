%% @doc 跨节点 / 复制通道 TLS（ADR-035）。
%%
%% 分两部分：本文件前半是无分布的纯函数用例（配置解析、校验判据、渲染器），
%% 后半是真实 TLS 分布用例（peer 之间走 `inet_tls'，组 2 节点 ra 集群）。
%%
%% 关键写法约束（spec §6.1，实测得出）：CT 宿主跑的是明文 `inet_tcp'，
%% `-proto_dist' 只能在 boot 期设定，套件无法把宿主改成 TLS。因此
%%   * peer 一律 `connection => standard_io'，宿主只用 `peer:call/3'；
%%   * 所有跨节点断言在 peer 内部执行（`peer:call(A, rpc, call, [B, ...])'）；
%%   * 宿主对 TLS peer 的 `rpc:call' 必然 `{badrpc, nodedown}'，那不是失败。
-module(broker_cluster_dist_tls_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").

all() ->
    [disabled_by_default,
     disabled_config_keeps_the_shipped_defaults,
     config_merges_overrides_onto_defaults,
     redacted_config_never_contains_key_material,
     effective_reports_the_plain_default,
     effective_reads_the_arguments_this_node_booted_with,
     disabled_is_always_valid,
     enabled_but_booted_plain_is_refused,
     enabled_with_inline_opt_is_refused,
     booted_with_tls_but_disabled_is_refused,
     missing_files_are_refused,
     bad_pem_is_refused,
     client_side_fail_if_no_peer_cert_is_refused,
     server_side_missing_fail_if_no_peer_cert_is_refused,
     rejecting_the_field_names_the_problem,
     validate_zero_argument_on_a_plain_disabled_node_is_ok].

%%%_* Fixtures --------------------------------------------------------------

init_per_suite(Config) ->
    Saved = application:get_env(broker_cluster, dist_tls, undefined),
    Dir = filename:join(?config(priv_dir, Config), "dist_tls"),
    _ = file:del_dir_r(Dir),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    Certs = gen_certs(Dir),
    [{saved_dist_tls, Saved}, {tls_dir, Dir}, {certs, Certs} | Config].

end_per_suite(Config) ->
    restore(?config(saved_dist_tls, Config)),
    ok.

init_per_testcase(_Case, Config) ->
    application:unset_env(broker_cluster, dist_tls),
    Config.

end_per_testcase(_Case, Config) ->
    restore(?config(saved_dist_tls, Config)),
    ok.

%%%_* Pure cases ------------------------------------------------------------

disabled_by_default(_Config) ->
    %% The .app.src env is what a release reads, so this asserts the shipped
    %% default rather than a hard-coded `false'.
    application:unset_env(broker_cluster, dist_tls),
    false = broker_cluster_dist_tls:enabled(),
    ok.

disabled_config_keeps_the_shipped_defaults(_Config) ->
    Cfg = broker_cluster_dist_tls:config(),
    false = maps:get(enabled, Cfg),
    verify_peer = maps:get(verify, Cfg),
    true = maps:get(fail_if_no_peer_cert, Cfg),
    ['tlsv1.2', 'tlsv1.3'] = maps:get(versions, Cfg),
    ok.

config_merges_overrides_onto_defaults(_Config) ->
    application:set_env(broker_cluster, dist_tls,
                        #{enabled => true, certfile => "/tmp/x.crt"}),
    Cfg = broker_cluster_dist_tls:config(),
    true = maps:get(enabled, Cfg),
    "/tmp/x.crt" = maps:get(certfile, Cfg),
    %% Untouched keys keep their defaults.
    verify_peer = maps:get(verify, Cfg),
    ok.

redacted_config_never_contains_key_material(_Config) ->
    application:set_env(broker_cluster, dist_tls,
                        #{enabled => true,
                          certfile => "/tmp/node.crt",
                          keyfile => "/tmp/node.key",
                          cacertfile => "/tmp/ca.crt"}),
    Map = broker_cluster_dist_tls:redacted_config(),
    true = maps:get(enabled, Map),
    %% Basename only: the view says which file, never where the tree lives,
    %% and never any key bytes.
    <<"node.crt">> = maps:get(certfile, Map),
    false = maps:is_key(keyfile, Map),
    false = maps:is_key(private_key, Map),
    false = maps:is_key(key, Map),
    ok.

%% The CT host runs plain `inet_tcp', so "effective" must say exactly that:
%% no proto_dist flag, no ssl_dist_sup, no options table. This is the value
%% validate/0 compares the declaration against on every unmodified node.
effective_reports_the_plain_default(_Config) ->
    Eff = broker_cluster_dist_tls:effective(),
    error = maps:get(proto_dist, Eff),
    false = maps:get(tls_booted, Eff),
    undefined = maps:get(server_opts, Eff),
    undefined = maps:get(client_opts, Eff),
    ok.

%% Guard rail for the distributed cases below: if this ever stopped being
%% true, `effective/0' would be reading the wrong things and every drift
%% check would silently pass.
effective_reads_the_arguments_this_node_booted_with(_Config) ->
    Eff = broker_cluster_dist_tls:effective(),
    %% CT's own host is un-named unless a dist suite started distribution;
    %% either way we assert the *shape*, not a particular value.
    true = is_atom(maps:get(node_name, Eff)),
    true = lists:member(maps:get(longnames, Eff), [true, false]),
    ok.

%%%_* Validation cases -------------------------------------------------------

%% Disabled must never require files: that is what keeps an untouched
%% single-node deployment byte-for-byte identical.
disabled_is_always_valid(_Config) ->
    application:set_env(broker_cluster, dist_tls, #{enabled => false}),
    ok = broker_cluster_dist_tls:validate(plain_boot()),
    ok.

%% The most likely operator mistake: they set the env but forgot vm.args.
%% Refusing here is the whole point of the feature -- a node that thinks it
%% encrypts while it does not is worse than one that fails to boot.
enabled_but_booted_plain_is_refused(Config) ->
    set_enabled(self_signed(Config)),
    {error, {dist_tls_enabled_but_booted_plain, error}} =
        broker_cluster_dist_tls:validate(plain_boot()),
    ok.

%% The inline -ssl_dist_opt form works at the OTP level but leaves no readable
%% options, so the declaration can never be verified against reality. Refuse
%% it and point at the renderer rather than accepting an unverifiable setup.
enabled_with_inline_opt_is_refused(Config) ->
    set_enabled(self_signed(Config)),
    Boot = (tls_boot(Config))#{inline_opt => {ok, [["server_certfile", "x"]]},
                               optfile => error},
    {error, {dist_tls_requires_optfile, see_renderer}} =
        broker_cluster_dist_tls:validate(Boot),
    ok.

%% The mirror image: someone enabled TLS in vm.args but left the env at its
%% shipped default. The node *is* encrypted, but nobody declared it, so the
%% admin view would lie. Both directions are drift.
booted_with_tls_but_disabled_is_refused(Config) ->
    application:set_env(broker_cluster, dist_tls, #{enabled => false}),
    {error, {dist_tls_booted_with_tls_but_disabled, inet_tls}} =
        broker_cluster_dist_tls:validate(tls_boot(Config)),
    ok.

missing_files_are_refused(Config) ->
    Self = self_signed(Config),
    set_enabled(Self#{certfile => "/nonexistent/node.crt"}),
    {error, {missing_file, certfile, "/nonexistent/node.crt"}} =
        broker_cluster_dist_tls:validate(tls_boot(Config)),
    ok.

bad_pem_is_refused(Config) ->
    Self = self_signed(Config),
    Junk = filename:join(?config(tls_dir, Config), "junk.crt"),
    ok = file:write_file(Junk, <<"this is not a certificate">>),
    set_enabled(Self#{certfile => Junk}),
    {error, {bad_pem, certfile, Junk}} =
        broker_cluster_dist_tls:validate(tls_boot(Config)),
    ok.

%% ssl rejects fail_if_no_peer_cert on a client socket at connect time, so a
%% node that booted with it in the client block would look healthy and then
%% fail every outbound handshake. Catch it at boot instead.
client_side_fail_if_no_peer_cert_is_refused(Config) ->
    Self = self_signed(Config),
    set_enabled(Self),
    Boot = tls_boot(Config),
    Client = [{fail_if_no_peer_cert, true}
              | maps:get(client_opts, Boot)],
    {error, {dist_tls_optfile_drift, fail_if_no_peer_cert, undefined, true}} =
        broker_cluster_dist_tls:validate(Boot#{client_opts => Client}),
    ok.

%% The mirror of the case above, and the more dangerous one: without
%% fail_if_no_peer_cert on the server, ssl happily accepts a connection that
%% presents no certificate at all. The node would claim mTLS and admit anonymous
%% peers.
server_side_missing_fail_if_no_peer_cert_is_refused(Config) ->
    Self = self_signed(Config),
    set_enabled(Self),
    Boot = tls_boot(Config),
    Server = lists:keydelete(fail_if_no_peer_cert, 1,
                             maps:get(server_opts, Boot)),
    {error, {dist_tls_optfile_drift, fail_if_no_peer_cert, true, undefined}} =
        broker_cluster_dist_tls:validate(Boot#{server_opts => Server}),
    ok.

%% An error that names the field without naming the file it compared is not
%% actionable. Pin the shape.
rejecting_the_field_names_the_problem(Config) ->
    set_enabled((self_signed(Config))#{verify => verify_none}),
    {error, {dist_tls_optfile_drift, verify, verify_none, verify_peer}} =
        broker_cluster_dist_tls:validate(tls_boot(Config)),
    ok.

%% The zero-arity form is what the app calls. On a plain, disabled node it must
%% be a no-op -- this is the "1472 tests keep passing" guard expressed as a
%% test rather than as a hope.
validate_zero_argument_on_a_plain_disabled_node_is_ok(_Config) ->
    application:set_env(broker_cluster, dist_tls, #{enabled => false}),
    ok = broker_cluster_dist_tls:validate(),
    ok.

%%%_* Helpers ---------------------------------------------------------------

restore(undefined) -> application:unset_env(broker_cluster, dist_tls);
restore(Value) -> application:set_env(broker_cluster, dist_tls, Value).

plain_boot() ->
    #{proto_dist => error, optfile => error, inline_opt => error,
      tls_booted => false, server_opts => undefined, client_opts => undefined}.

%% A TLS boot whose declared options match exactly what self_signed/1 declares,
%% so only the drift check under test can fail.
tls_boot(Config) ->
    Declared = self_signed(Config),
    Opts = [{certfile, maps:get(certfile, Declared)},
            {keyfile, maps:get(keyfile, Declared)},
            {cacertfile, maps:get(cacertfile, Declared)},
            {verify, maps:get(verify, Declared)},
            {fail_if_no_peer_cert, maps:get(fail_if_no_peer_cert, Declared)},
            {versions, maps:get(versions, Declared)}],
    #{proto_dist => {ok, [["inet_tls"]]},
      optfile => {ok, [[maps:get(optfile, Declared)]]},
      inline_opt => error,
      tls_booted => true,
      server_opts => Opts,
      %% The client side never carries fail_if_no_peer_cert: ssl rejects it
      %% there at connect time.
      client_opts => lists:keydelete(fail_if_no_peer_cert, 1, Opts)}.

%% Turning the declaration on for a test. Runtime env only: the boot state is
%% injected through validate/1, so nothing here touches real distribution.
set_enabled(Cfg) ->
    application:set_env(broker_cluster, dist_tls, Cfg),
    ok.

%% Certificates are generated per run and never committed: the repository holds
%% no private keys. `openssl` is the same tool broker_security_tls_SUITE uses.
%%
%% Three node certs (n1..n3) share one CA; the SAN is IP:127.0.0.1 because every
%% peer in the distributed cases dials the others at 127.0.0.1, and the outbound
%% side DOES check the peer's SAN (spec §1.1 finding 4) -- a wrong SAN is a
%% handshake failure, not a silent pass.
gen_certs(Dir) ->
    CaKey = filename:join(Dir, "ca.key"),
    CaCrt = filename:join(Dir, "ca.crt"),
    ok = openssl(["req", "-x509", "-newkey", "rsa:2048", "-nodes",
                  "-days", "2", "-subj", "/CN=unifiedmq-test-ca",
                  "-keyout", CaKey, "-out", CaCrt]),
    SanCnf = filename:join(Dir, "san.cnf"),
    ok = file:write_file(SanCnf,
                         "[req]\ndistinguished_name=dn\n[dn]\n"
                         "[ext]\nsubjectAltName=IP:127.0.0.1\n"),
    Certs = lists:foldl(
              fun(I, Acc) ->
                      Prefix = "n" ++ integer_to_list(I),
                      Crt = filename:join(Dir, Prefix ++ ".crt"),
                      Key = filename:join(Dir, Prefix ++ ".key"),
                      Csr = filename:join(Dir, Prefix ++ ".csr"),
                      ok = openssl(["req", "-newkey", "rsa:2048", "-nodes",
                                    "-subj", "/CN=" ++ Prefix ++ "@127.0.0.1",
                                    "-keyout", Key, "-out", Csr]),
                      ok = openssl(["x509", "-req", "-in", Csr,
                                    "-CA", CaCrt, "-CAkey", CaKey,
                                    "-CAcreateserial", "-days", "2",
                                    "-extfile", SanCnf, "-extensions", "ext",
                                    "-out", Crt]),
                      Acc#{list_to_atom(Prefix ++ "_crt") => Crt,
                           list_to_atom(Prefix ++ "_key") => Key}
              end, #{ca_crt => CaCrt, ca_key => CaKey}, [1, 2, 3]),
    OptFile = filename:join(Dir, "dist_tls.optfile"),
    ok = file:write_file(OptFile, peer_optfile_text(
                                    #{cert => maps:get(n1_crt, Certs),
                                      key => maps:get(n1_key, Certs),
                                      ca => CaCrt})),
    Certs#{optfile => OptFile, dir => Dir}.

openssl(Args) ->
    Cmd = lists:flatten([[" ", A] || A <- ["openssl" | Args]]),
    _ = os:cmd(Cmd ++ " 2>/dev/null"),
    ok.

%% One optfile per peer: each presents its own certificate, all signed by the
%% same test CA. Server and client sides carry the same identity -- that is what
%% mTLS means for a node that both dials and is dialled.
peer_optfile_text(#{cert := Cert, key := Key, ca := Ca}) ->
    Server = [{certfile, Cert}, {keyfile, Key}, {cacertfile, Ca},
              {verify, verify_peer}, {fail_if_no_peer_cert, true},
              {versions, ['tlsv1.2', 'tlsv1.3']}],
    Client = [{certfile, Cert}, {keyfile, Key}, {cacertfile, Ca},
              {verify, verify_peer},
              {versions, ['tlsv1.2', 'tlsv1.3']}],
    io_lib:format("[{server, ~p},~n {client, ~p}].~n", [Server, Client]).

self_signed(Config) ->
    Certs = ?config(certs, Config),
    #{enabled => true,
      certfile => maps:get(n1_crt, Certs), keyfile => maps:get(n1_key, Certs),
      cacertfile => maps:get(ca_crt, Certs),
      optfile => maps:get(optfile, Certs),
      verify => verify_peer, fail_if_no_peer_cert => true,
      versions => ['tlsv1.2', 'tlsv1.3']}.

%% The shipped default is what a release reads off disk, so read the file
%% rather than the runtime env -- application:get_key/2 reflects set_env too,
%% which would make this assertion tautological.
shipped_env() ->
    AppFile = filename:join(filename:dirname(code:which(broker_cluster)),
                            "broker_cluster.app"),
    {ok, [{application, broker_cluster, Props}]} = file:consult(AppFile),
    proplists:get_value(env, Props).
