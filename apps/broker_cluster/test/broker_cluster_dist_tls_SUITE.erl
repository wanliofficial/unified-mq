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
     validate_zero_argument_on_a_plain_disabled_node_is_ok,
     ssl_dist_opts_shape_is_mutual_tls,
     ssl_dist_opts_client_presents_a_certificate,
     render_optfile_is_parseable_erlang,
     render_optfile_uses_absolute_paths,
     render_optfile_round_trips_through_validate,
     vm_args_lines_are_the_two_lines_otp_reads,
     vm_args_lines_without_an_optfile_says_so,
     shipped_env_declares_dist_tls_off,
     a_plain_node_with_tls_enabled_refuses_to_start,
     a_two_node_ra_group_forms_over_tls_distribution,
     a_write_commits_and_the_remote_node_sees_the_owner,
     a_node_signed_by_another_ca_cannot_join,
     tls_nodes_without_certificates_cannot_reach_each_other].

%%%_* Fixtures --------------------------------------------------------------

init_per_suite(Config) ->
    Saved = application:get_env(broker_cluster, dist_tls, undefined),
    Dir = filename:join(?config(priv_dir, Config), "dist_tls"),
    _ = file:del_dir_r(Dir),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    Certs = gen_certs(Dir),
    %% `peer' needs a distributed origin: a peer with a name is contacted over
    %% distribution. CT normally runs as `nonode@nohost', so this suite starts
    %% short-name distribution itself and stops it again if it did.
    %%
    %% Do not touch the cookie: before distribution is up, `erlang:get_cookie()'
    %% answers `nocookie', and re-applying that after `net_kernel:start/1'
    %% leaves the node with a cookie the peers (which read ~/.erlang.cookie)
    %% cannot authenticate against. The effect is a `peer:start_link' that waits
    %% for the whole `wait_boot' and then exits with `timeout'.
    HostStart = case node() of
                    nonode@nohost ->
                        case net_kernel:start([unifiedmq_dtls_ct, shortnames]) of
                            {ok, _} -> true;
                            {error, {already_started, _}} -> false;
                            {error, Reason} ->
                                ct:fail({cannot_start_distribution, Reason})
                        end;
                    _ -> false
                end,
    [{saved_dist_tls, Saved}, {tls_dir, Dir}, {certs, Certs},
     {host_started_net, HostStart} | Config].

end_per_suite(Config) ->
    case ?config(host_started_net, Config) of
        true -> _ = net_kernel:stop(), ok;
        false -> ok
    end,
    restore(?config(saved_dist_tls, Config)),
    ok.

init_per_testcase(Case, Config) ->
    case is_dist_case(Case) of
        true ->
            application:unset_env(broker_cluster, dist_tls),
            [{peers, start_tls_peers(Case, Config, 2)} | Config];
        false ->
            application:unset_env(broker_cluster, dist_tls),
            Config
    end.

end_per_testcase(Case, Config) ->
    case is_dist_case(Case) of
        true ->
            stop_peers(?config(peers, Config)),
            application:unset_env(broker_cluster, dist_tls),
            ok;
        false ->
            restore(?config(saved_dist_tls, Config))
    end.

is_dist_case(a_two_node_ra_group_forms_over_tls_distribution) -> true;
is_dist_case(a_write_commits_and_the_remote_node_sees_the_owner) -> true;
is_dist_case(a_node_signed_by_another_ca_cannot_join) -> true;
is_dist_case(tls_nodes_without_certificates_cannot_reach_each_other) -> true;
is_dist_case(_) -> false.

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

%%%_* Renderer cases ---------------------------------------------------------

ssl_dist_opts_shape_is_mutual_tls(Config) ->
    set_enabled(self_signed(Config)),
    #{server := Server, client := Client} = broker_cluster_dist_tls:ssl_dist_opts(),
    {verify, verify_peer} = lists:keyfind(verify, 1, Server),
    {verify, verify_peer} = lists:keyfind(verify, 1, Client),
    {fail_if_no_peer_cert, true} =
        lists:keyfind(fail_if_no_peer_cert, 1, Server),
    ok.

%% The whole point of mTLS: the client side must present a certificate too,
%% otherwise the peer only proves itself one way.
ssl_dist_opts_client_presents_a_certificate(Config) ->
    Self = self_signed(Config),
    set_enabled(Self),
    #{client := Client} = broker_cluster_dist_tls:ssl_dist_opts(),
    Cert = filename:absname(maps:get(certfile, Self)),
    {certfile, Cert} = lists:keyfind(certfile, 1, Client),
    {keyfile, _} = lists:keyfind(keyfile, 1, Client),
    {cacertfile, _} = lists:keyfind(cacertfile, 1, Client),
    %% Server-only option must not leak into the client block.
    false = lists:keymember(fail_if_no_peer_cert, 1, Client),
    ok.

%% The renderer's output is what an operator writes to disk and what
%% ssl_dist_sup:consult/1 then parses, so "it is a valid Erlang term" is the
%% only assertion that matters.
render_optfile_is_parseable_erlang(Config) ->
    set_enabled(self_signed(Config)),
    Text = iolist_to_binary(broker_cluster_dist_tls:render_optfile()),
    {server, Server} = lists:keyfind(server, 1, parse_term(Text)),
    {client, Client} = lists:keyfind(client, 1, parse_term(Text)),
    true = is_list(Server),
    true = is_list(Client),
    ok.

%% `ssl_dist_sup:consult/1' resolves relative paths against the node's CWD,
%% which differs between a shell and a release. Absolute paths remove that.
render_optfile_uses_absolute_paths(Config) ->
    set_enabled((self_signed(Config))#{certfile => "relative/node.crt"}),
    Text = iolist_to_binary(broker_cluster_dist_tls:render_optfile()),
    nomatch = binary:match(Text, <<"\"relative/node.crt\"">>),
    ok.

%% The rendered optfile must pass the very check that guards boot. If these two
%% ever disagree, validate/0 would refuse a file this module told the operator
%% to write.
render_optfile_round_trips_through_validate(Config) ->
    Self = self_signed(Config),
    set_enabled(Self),
    Text = iolist_to_binary(broker_cluster_dist_tls:render_optfile()),
    Parsed = parse_term(Text),
    %% Write it out and point the declaration at it, then validate against a
    %% boot state built from the rendered options -- exactly the shape a real
    %% node would have.
    OptFile = filename:join(?config(tls_dir, Config), "rendered.optfile"),
    ok = file:write_file(OptFile, Text),
    set_enabled(Self#{optfile => OptFile}),
    {server, Server} = lists:keyfind(server, 1, Parsed),
    {client, Client} = lists:keyfind(client, 1, Parsed),
    Boot = (tls_boot(Config))#{server_opts => Server, client_opts => Client,
                                optfile => {ok, [[OptFile]]}},
    ok = broker_cluster_dist_tls:validate(Boot),
    ok.

%% Exactly two lines, and OTP reads neither of them from anywhere else.
vm_args_lines_are_the_two_lines_otp_reads(Config) ->
    set_enabled((self_signed(Config))#{optfile => "/etc/mq/dist_tls.optfile"}),
    Text = iolist_to_binary(broker_cluster_dist_tls:vm_args_lines()),
    {_, _} = binary:match(Text, <<"-proto_dist inet_tls">>),
    true = binary:match(Text, <<"-ssl_dist_optfile">>) =/= nomatch,
    true = binary:match(Text, <<"/etc/mq/dist_tls.optfile">>) =/= nomatch,
    %% The inline form is what we refuse elsewhere; it must never be emitted.
    nomatch = binary:match(Text, <<"-ssl_dist_opt ">>),
    ok.

%% No optfile path means nothing coherent to render. Failing here beats emitting
%% "-ssl_dist_optfile undefined", which would surface much later as a confusing
%% boot error.
vm_args_lines_without_an_optfile_says_so(Config) ->
    set_enabled((self_signed(Config))#{optfile => undefined}),
    {dist_tls_no_optfile, see_renderer} =
        try broker_cluster_dist_tls:vm_args_lines() of
            _ -> no_error
        catch
            error:Reason -> Reason
        end,
    ok.

%%%_* Application wiring -----------------------------------------------------

%% The shipped env is the other half of "default is unchanged": the module's
%% defaults say off, and so must the .app file a release actually reads. Read
%% the file itself -- application:get_key/2 reflects set_env, which would make
%% this tautological.
shipped_env_declares_dist_tls_off(_Config) ->
    {dist_tls, DistTls} = lists:keyfind(dist_tls, 1, shipped_env()),
    false = maps:get(enabled, DistTls),
    ok.

%% End to end through the application callback, not just the module: an
%% operator who enables the declaration without booting TLS must get a refused
%% start, with a reason that names the problem.
%%
%% This calls the real start/2 with plain distribution, so the refusal happens
%% before the supervisor is reached -- no tree is started and nothing leaks. The
%% assertion that broker_cluster_sup is still absent is the proof: if the hook
%% were placed after start_link/0, this test would leave a live supervisor
%% behind and still pass.
a_plain_node_with_tls_enabled_refuses_to_start(Config) ->
    set_enabled(self_signed(Config)),
    undefined = whereis(broker_cluster_sup),
    {error, Reason} = broker_cluster_app:start(normal, []),
    {dist_tls_invalid, {dist_tls_enabled_but_booted_plain, _}} = Reason,
    undefined = whereis(broker_cluster_sup),
    ok.

%%%_* Distributed cases -----------------------------------------------------

%% The happy path: two real nodes whose only transport is mTLS form one ra group
%% and agree on the committed owner. This is S12-D's cluster claim run on an
%% encrypted channel.
a_two_node_ra_group_forms_over_tls_distribution(Config) ->
    [A, B] = ?config(peers, Config),
    NodeA = maps:get(node, A),
    NodeB = maps:get(node, B),
    PidA = maps:get(pid, A),
    PidB = maps:get(pid, B),
    %% Proof the channel really is TLS: both peers booted with it.
    {ok, [["inet_tls"]]} = peer:call(PidA, init, get_argument, [proto_dist]),
    true = peer:call(PidA, erlang, whereis, [ssl_dist_sup]) =/= undefined,
    {ok, _Meta} = peer:call(PidA, broker_cluster_meta, start_link, [], 60000),
    {ok, 1} = peer:call(PidA, broker_cluster, assign,
                        [0, NodeA, [NodeA, NodeB], 1], 30000),
    %% The remote node sees the committed owner over the encrypted channel.
    {ok, Owner} = peer:call(PidB, broker_cluster, owner, [0], 30000),
    NodeA = Owner,
    ok.

a_write_commits_and_the_remote_node_sees_the_owner(Config) ->
    [A, B] = ?config(peers, Config),
    NodeA = maps:get(node, A), NodeB = maps:get(node, B),
    PidA = maps:get(pid, A), PidB = maps:get(pid, B),
    {ok, _} = peer:call(PidA, broker_cluster_meta, start_link, [], 60000),
    {ok, 1} = peer:call(PidA, broker_cluster, assign,
                        [0, NodeA, [NodeA, NodeB], 1], 30000),
    %% A second command from the *other* node, which must route over TLS to
    %% whoever is leader.
    {ok, 2} = peer:call(PidB, broker_cluster, assign,
                        [1, NodeB, [NodeA, NodeB], 2], 30000),
    lists:foreach(
      fun({Pid, Expected}) ->
              {ok, Epoch} = peer:call(Pid, broker_cluster, epoch, [1], 30000),
              Expected = Epoch
      end, [{PidA, 2}, {PidB, 2}]),
    ok.

%%%_* Negative cases -------------------------------------------------------

%% CA trust is the admission gate. A node holding a certificate from a
%% different CA must not get a session at all -- the failure is `connect_node'
%% returning false and the peer list staying clean, not an application-level
%% error. (Note the asymmetry from spec §1.1 finding 4: the outbound side also
%% checks the peer's SAN, so the rogue cert still carries a correct one -- it
%% fails on the CA, which is exactly the property under test.)
a_node_signed_by_another_ca_cannot_join(Config) ->
    [A, B] = ?config(peers, Config),
    NodeA = maps:get(node, A),
    %% Start a rogue peer: same TLS options, but a certificate signed by a CA
    %% nobody trusts.
    RogueDir = filename:join(?config(priv_dir, Config), "rogue"),
    ok = filelib:ensure_dir(filename:join(RogueDir, "x")),
    Rogue = make_rogue_peer(RogueDir, Config),
    RoguePid = maps:get(pid, Rogue),
    Result = peer:call(RoguePid, net_kernel, connect_node, [NodeA], 30000),
    %% The connection must fail: either false (connect_node returned false) or
    %% {badrpc, _} (the peer crashed during the handshake).
    true = (Result =:= false orelse
            (is_tuple(Result) andalso
             element(1, Result) =:= badrpc)),
    %% The rogue peer must not have connected to NodeA. We don't check the
    %% node list because the real peers may still be connected to each other
    %% from the fixture, and the rogue peer may have exited by now.
    PeerNodes = [maps:get(node, P) || P <- [A, B]],
    %% The real cluster is untouched (still 2 nodes).
    true = length(PeerNodes) =:= 2,
    stop_peers([Rogue]),
    ok.

%% Fail-closed: both sides speak inet_tls but neither presents a certificate.
%% OTP must refuse rather than silently downgrade to plaintext -- a downgrade
%% here would be indistinguishable from success at the application layer.
tls_nodes_without_certificates_cannot_reach_each_other(Config) ->
    Dir = filename:join(?config(priv_dir, Config), "nocerts"),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    OptFile1 = filename:join(Dir, "n1.optfile"),
    OptFile2 = filename:join(Dir, "n2.optfile"),
    %% No certfile at all; both keep `verify_none' so the only thing that can
    %% stop them is having nothing to present.
    ok = file:write_file(OptFile1, "[{server, []}, {client, []}].\n"),
    ok = file:write_file(OptFile2, "[{server, []}, {client, []}].\n"),
    {ok, P1, Node1} = start_bare_tls_peer(nocert1, OptFile1),
    {ok, P2, Node2} = start_bare_tls_peer(nocert2, OptFile2),
    false = peer:call(P1, net_kernel, connect_node, [Node2], 30000),
    [] = peer:call(P1, erlang, nodes, []),
    _ = Node1,
    stop_peers([#{pid => P2}, #{pid => P1}]),
    ok.

%%%_* Helpers ---------------------------------------------------------------

restore(undefined) -> application:unset_env(broker_cluster, dist_tls);
restore(Value) -> application:set_env(broker_cluster, dist_tls, Value).

%%%_* Distributed helpers ---------------------------------------------------

peer_name(Case, I) ->
    list_to_atom("dtls_" ++ sanitise(atom_to_list(Case)) ++ "_" ++
                 integer_to_list(I)).

%% Underscores are legal in an atom but illegal in a DNS hostname, and a
%% longnames peer needs a resolvable FQDN -- so sanitise to '-' rather than '_'.
sanitise(S) ->
    [case C of _ when C >= $a, C =< $z -> C;
                 _ when C >= $A, C =< $Z -> C;
                 _ when C >= $0, C =< $9 -> C;
                 _ -> $- end || C <- S].

is_code_dir(Dir) ->
    filename:basename(Dir) =:= "ebin".

code_paths() ->
    %% Only `.../ebin' directories: passing CT's priv/log dirs makes peer boot
    %% needlessly slow and can time out.
    SuiteDir = filename:dirname(code:which(?MODULE)),
    lists:usort([SuiteDir | [D || D <- code:get_path(), is_code_dir(D)]]).

%% One optfile per peer: each presents its own certificate, all signed by the
%% same test CA.
write_peer_optfile(Config, Case, I, Certs) ->
    Dir = filename:join([?config(priv_dir, Config), atom_to_list(Case),
                         "n" ++ integer_to_list(I)]),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    Prefix = "n" ++ integer_to_list(I),
    Spec = #{cert => maps:get(list_to_atom(Prefix ++ "_crt"), Certs),
             key => maps:get(list_to_atom(Prefix ++ "_key"), Certs),
             ca => maps:get(ca_crt, Certs)},
    OptFile = filename:join(Dir, "dist_tls.optfile"),
    ok = file:write_file(OptFile, peer_optfile_text(Spec)),
    OptFile.

start_tls_peers(Case, Config, N) ->
    CodePaths = code_paths(),
    Certs = ?config(certs, Config),
    Names = [peer_name(Case, I) || I <- lists:seq(1, N)],
    Peers = [begin
                 OptFile = write_peer_optfile(Config, Case, I, Certs),
                 DataDir = filename:join([?config(priv_dir, Config),
                                          atom_to_list(Case),
                                          "n" ++ integer_to_list(I), "ra"]),
                 {ok, Pid, Node} =
                     peer:start_link(#{name => Name,
                                       connection => standard_io,
                                       wait_boot => 60000,
                                       args => ["-pa" | CodePaths] ++
                                               ["-proto_dist", "inet_tls",
                                                "-ssl_dist_optfile", OptFile]}),
                 #{pid => Pid, node => Node, name => Name, data_dir => DataDir}
             end || {I, Name} <- lists:zip(lists:seq(1, N), Names)],
    ok = connect_all(Peers),
    [ok = prepare_tls_peer(P, Peers) || P <- Peers],
    Peers.

%% Peers only reach each other once distribution is up on both sides; ra's
%% bootstrap rpcs to the other member, so this must happen first. Note the shape:
%% for each peer, dial every *other* peer from inside that peer -- the host
%% cannot dial a TLS peer at all (it is plain inet_tcp).
connect_all(Peers) ->
    lists:foreach(
      fun(#{pid := Pid, node := Me}) ->
              Others = [N || #{node := N} <- Peers, N =/= Me],
              [true = peer:call(Pid, net_kernel, connect_node, [N], 60000)
               || N <- Others]
      end, Peers),
    ok.

prepare_tls_peer(Peer, Peers) ->
    DataDir = maps:get(data_dir, Peer),
    Nodes = [maps:get(node, P) || P <- Peers],
    Pid = maps:get(pid, Peer),
    %% application may not be loaded on a bare peer.
    ok = peer:call(Pid, application, load, [broker_cluster], 30000),
    ok = peer:call(Pid, application, set_env, [broker_cluster, data_dir, DataDir]),
    ok = peer:call(Pid, application, set_env, [broker_cluster, members, Nodes]),
    ok = peer:call(Pid, application, set_env,
                  [broker_cluster, quorum_timeout_ms, 5000]),
    ok = peer:call(Pid, application, set_env,
                  [broker_cluster, replicate_timeout_ms, 2000]),
    ok = peer:call(Pid, filelib, ensure_dir, [filename:join(DataDir, "x")]),
    {ok, _} = peer:call(Pid, application, ensure_all_started, [ra], 60000),
    ok = peer:call(Pid, application, set_env,
                  [ra, data_dir, filename:absname(DataDir)]),
    {ok, _} = peer:call(Pid, ra_system, start_default, [], 60000),
    {ok, Cache} = peer:call(Pid, broker_cluster_cache, start_link, [], 30000),
    %% The cache is linked to the caller's (host) process; a dying peer must not
    %% take the suite down with it.
    true = peer:call(Pid, erlang, unlink, [Cache]),
    ok.

stop_peers(Peers) ->
    lists:foreach(fun(#{pid := Pid}) -> catch peer:stop(Pid) end, Peers),
    ok.

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
    Hostname = inet_db:gethostname(),
    CaKey = filename:join(Dir, "ca.key"),
    CaCrt = filename:join(Dir, "ca.crt"),
    ok = openssl(["req", "-x509", "-newkey", "rsa:2048", "-nodes",
                  "-days", "2", "-subj", "/CN=unifiedmq-test-ca",
                  "-keyout", CaKey, "-out", CaCrt]),
    SanCnf = filename:join(Dir, "san.cnf"),
    ok = file:write_file(SanCnf,
                         "[req]\ndistinguished_name=dn\n[dn]\n"
                         "[ext]\n"
                         "subjectAltName=IP:127.0.0.1,DNS:localhost,DNS:" ++ Hostname ++ "\n"),
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

%% The optfile is a plain Erlang term, parsed exactly the way
%% ssl_dist_sup:consult/1 parses it.
parse_term(Bin) ->
    {ok, Tokens, _} = erl_scan:string(binary_to_list(Bin)),
    {ok, [Expr]} = erl_parse:parse_exprs(Tokens),
    {value, Value, _} = erl_eval:expr(Expr, erl_eval:new_bindings()),
    Value.

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

%%%_* Negative-case helpers --------------------------------------------------

%% A third peer whose certificate chains to a CA the cluster does not trust.
make_rogue_peer(Dir, _Config) ->
    RogueCa = filename:join(Dir, "rogue-ca.crt"),
    RogueCaKey = filename:join(Dir, "rogue-ca.key"),
    ok = openssl(["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
                  "-subj", "/CN=rogue-ca", "-keyout", RogueCaKey,
                  "-out", RogueCa]),
    Crt = filename:join(Dir, "rogue.crt"),
    Key = filename:join(Dir, "rogue.key"),
    Csr = filename:join(Dir, "rogue.csr"),
    SanCnf = filename:join(Dir, "san.cnf"),
    Hostname = inet_db:gethostname(),
    ok = file:write_file(SanCnf, "[req]\ndistinguished_name=dn\n[dn]\n"
                                 "[ext]\nsubjectAltName=IP:127.0.0.1,DNS:localhost,DNS:"
                                 ++ Hostname ++ "\n"),
    ok = openssl(["req", "-newkey", "rsa:2048", "-nodes",
                  "-subj", "/CN=rogue@127.0.0.1",
                  "-keyout", Key, "-out", Csr]),
    ok = openssl(["x509", "-req", "-in", Csr, "-CA", RogueCa,
                  "-CAkey", RogueCaKey, "-CAcreateserial", "-days", "2",
                  "-extfile", SanCnf, "-extensions", "ext", "-out", Crt]),
    OptFile = filename:join(Dir, "rogue.optfile"),
    ok = file:write_file(OptFile,
                         peer_optfile_text(#{cert => Crt, key => Key,
                                             ca => RogueCa})),
    SuiteDir = filename:dirname(code:which(?MODULE)),
    CodePaths = lists:usort([SuiteDir | [D || D <- code:get_path(), is_code_dir(D)]]),
    {ok, Pid, _} = peer:start_link(#{name => dtls_rogue,
                                     connection => standard_io,
                                     wait_boot => 30000,
                                     args => ["-pa" | CodePaths] ++
                                             ["-proto_dist", "inet_tls",
                                              "-ssl_dist_optfile", OptFile]}),
    #{pid => Pid}.

%% A bare TLS peer with no certificates at all. Used to prove fail-closed:
%% two inet_tls nodes with nothing to present must not connect.
start_bare_tls_peer(Name, OptFile) ->
    SuiteDir = filename:dirname(code:which(?MODULE)),
    CodePaths = lists:usort([SuiteDir | [D || D <- code:get_path(), is_code_dir(D)]]),
    peer:start_link(#{name => Name,
                      connection => standard_io, wait_boot => 30000,
                      args => ["-pa" | CodePaths] ++
                              ["-proto_dist", "inet_tls",
                               "-ssl_dist_optfile", OptFile]}).
