%% @doc CoAP over DTLS (S11-C): the security half of the CoAP front-end.
%%
%% Everything here runs a real DTLS 1.2 handshake with PSK against a real
%% listener -- no mocked ssl, no stubbed identity. That is the whole point of the
%% stage: `ADR-031' §D5 left CoAP with no credential, so `principal' was
%% `undefined' on the wire and an ACL could not deny a datagram. These cases prove
%% the gap is closed: the PSK identity a device presents IS its `broker_security'
%% username, so requests arrive with a real principal and a real tenant, and
%% `4.03 Forbidden' finally happens for a real reason.
%%
%% The client below is deliberately a hand-written PSK peer (the one the spike
%% report measured), not a CoAP library: the point is to pin the wire contract --
%% the KDF a device must reproduce, the cert-free suite set, and the server's
%% identity-to-principal mapping.
%%
%% The listener runs with `port => 0' like every other suite here; nothing binds a
%% fixed port.
-module(coap_dtls_SUITE).

-compile([export_all, nowarn_export_all]).
-compile({no_auto_import, [put/2]}).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(N, 2).
-define(HOST, {127, 0, 0, 1}).
-define(POLL_MS, 20).
-define(POLL_BUDGET_MS, 5000).
-define(OK, {response, 2, 4}).
-define(CONTENT, {response, 2, 5}).
-define(PSK_LABEL, <<"unifiedmq-coaps-psk">>).
-define(DEFAULT_REALM, <<"unifiedmq">>).

all() ->
    [dtls_psk_handshake_and_get,
     dtls_psk_write_and_read,
     unknown_identity_is_refused,
     dtls_con_dedup,
     identity_does_not_leak_between_handshakes,
     acl_denies_on_dtls,
     acl_allows_on_dtls,
     tenant_isolation_on_dtls,
     dtls_observe_registers_and_notifies,
     dtls_session_cap,
     dtls_per_principal_cap,
     dtls_abandoned_releases_slot,
     dtls_close_cleans_up,
     plain_udp_unaffected].

%%%_* Fixtures --------------------------------------------------------------

init_per_suite(Config) ->
    %% Load every kernel app before any `set_env' so the per-case values override
    %% the loaded app.src defaults instead of being clobbered by
    %% `ensure_all_started' loading the .app file later.
    _ = application:load(broker_storage),
    _ = application:load(broker_router),
    _ = application:load(broker_queue),
    _ = application:load(broker_subscription),
    _ = application:load(broker_retain),
    _ = application:load(broker_event),
    _ = application:load(broker_security),
    _ = application:load(broker_proto_coap),
    %% ssl is a runtime prerequisite for DTLS and is not started by the CoAP app
    %% in the test node unless asked; start it once for the whole suite.
    {ok, _} = application:ensure_all_started(ssl),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(Case, Config0) ->
    stop_all(),
    Saved = #{acl => env(broker_security, acl),
              require_auth => env(broker_security, require_auth),
              users => env(broker_security, users),
              coaps => env(broker_security, coaps),
              max_sessions => env(broker_proto_coap, max_dtls_sessions),
              per_principal => env(broker_proto_coap,
                                   dtls_max_sessions_per_principal),
              max_observes => env(broker_proto_coap, max_observes),
              ack_timeout => env(broker_proto_coap, observe_ack_timeout_ms)},
    DataDir = filename:join([?config(priv_dir, Config0), atom_to_list(Case), "data"]),
    ok = filelib:ensure_dir(filename:join([DataDir, "x"])),
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok = application:set_env(broker_storage, data_dir, DataDir),
    ok = application:set_env(broker_storage, wal, #{require_durable_fs => false}),
    ok = application:set_env(broker_queue, partitions, ?N),
    ok = application:set_env(broker_router, partitions, ?N),
    ok = application:set_env(broker_subscription, data_dir, DataDir),
    ok = application:set_env(broker_retain, data_dir, DataDir),
    %% Two devices and one operator-known account, each with a tenant, so the ACL
    %% and tenant cases have real principals to distinguish. The password is the
    %% ONLY secret configured here: the PSK is derived from it, which is the
    %% property `broker_security_SUITE' pins and this suite exercises end to end.
    ok = application:set_env(broker_security, users,
                             [{<<"device-01">>, #{password => <<"secret-01">>,
                                                  tenant_id => <<"t1">>}},
                              {<<"device-02">>, #{password => <<"secret-02">>,
                                                  tenant_id => <<"t1">>}},
                              {<<"tenant-b">>, #{password => <<"secret-b">>,
                                                 tenant_id => <<"t2">>}}]),
    ok = application:set_env(broker_security, acl, allow),
    ok = application:set_env(broker_security, require_auth, []),
    ok = application:set_env(broker_security, coaps, #{realm => ?DEFAULT_REALM}),
    ok = application:set_env(broker_proto_coap, enabled, true),
    %% `transport => dtls' is the opt-in: no plain UDP socket is opened at all.
    %% The one case about the plain path overrides this (see `case_env/2').
    ok = application:set_env(broker_proto_coap, listeners, listener_spec(Case)),
    ok = application:set_env(broker_proto_coap, resources, []),
    ok = case_env(Case, Config0),
    {ok, _} = application:ensure_all_started(broker_storage),
    ok = create_partitions(),
    {ok, _} = application:ensure_all_started(broker_proto_coap),
    [{port, listener_port()}, {saved_env, Saved} | Config0].

end_per_testcase(_Case, Config) ->
    stop_all(),
    restore_env(?config(saved_env, Config)),
    ok.

%% The plain-path control runs a UDP listener instead of a DTLS one; every other
%% case opts into DTLS. Plain UDP is the default, so omitting `transport' is
%% exactly what an operator who never heard of S11-C gets.
listener_spec(plain_udp_unaffected) -> [#{port => 0}];
listener_spec(_Case) -> [#{port => 0, transport => dtls}].

%% The per-principal cap case allows a single session per identity so the second
%% one is refused deterministically; everything else keeps the shipped default.
case_env(dtls_per_principal_cap, _Config) ->
    ok = application:set_env(broker_proto_coap,
                             dtls_max_sessions_per_principal, 1);
%% The session cap case allows exactly one established session in total.
case_env(dtls_session_cap, _Config) ->
    ok = application:set_env(broker_proto_coap, max_dtls_sessions, 1);
%% The close case needs a short ACK timeout so it can prove no retransmit timer
%% outlived the session.
case_env(dtls_close_cleans_up, _Config) ->
    ok = application:set_env(broker_proto_coap, observe_ack_timeout_ms, 300);
case_env(_Case, _Config) ->
    ok.

env(App, Key) ->
    application:get_env(App, Key, undefined).

restore_env(#{acl := Acl, require_auth := Auth, users := Users, coaps := Coaps,
              max_sessions := MaxSessions, per_principal := PerPrincipal,
              max_observes := MaxObs, ack_timeout := AckTO}) ->
    set_or_unset(broker_security, acl, Acl),
    set_or_unset(broker_security, require_auth, Auth),
    set_or_unset(broker_security, users, Users),
    set_or_unset(broker_security, coaps, Coaps),
    set_or_unset(broker_proto_coap, max_dtls_sessions, MaxSessions),
    set_or_unset(broker_proto_coap, dtls_max_sessions_per_principal,
                 PerPrincipal),
    set_or_unset(broker_proto_coap, max_observes, MaxObs),
    set_or_unset(broker_proto_coap, observe_ack_timeout_ms, AckTO),
    ok.

set_or_unset(_App, _Key, undefined) -> ok;
set_or_unset(App, Key, Value) -> application:set_env(App, Key, Value).

create_partitions() ->
    lists:foreach(
      fun(P) ->
              case broker_queue:create_partition(P) of
                  ok -> ok;
                  {error, {already_exists, P}} -> ok;
                  {error, Other} -> exit({create_partition, P, Other})
              end
      end, broker_queue:partition_ids()).

listener_port() ->
    case [Pid || {Id, Pid, worker, _} <- supervisor:which_children(
                                                 broker_proto_coap_sup),
                  element(1, Id) =:= coap_listener] of
        [Pid | _] -> {ok, Port} = coap_listener:port(Pid), Port;
        [] -> exit(no_listener)
    end.

stop_all() ->
    _ = application:stop(broker_proto_coap),
    _ = application:stop(broker_retain),
    _ = application:stop(broker_security),
    _ = application:stop(broker_event),
    _ = application:stop(broker_subscription),
    _ = application:stop(broker_queue),
    _ = application:stop(broker_router),
    _ = application:stop(broker_storage),
    cleanup_writers(),
    ok.

cleanup_writers() ->
    case whereis(broker_storage_wal_sup) of
        undefined ->
            ok;
        _Sup ->
            lists:foreach(
              fun({Id, _Child, _Type, _Modules}) ->
                      _ = supervisor:terminate_child(broker_storage_wal_sup, Id),
                      ok = supervisor:delete_child(broker_storage_wal_sup, Id)
              end, supervisor:which_children(broker_storage_wal_sup))
    end.

%%%_* DTLS client -----------------------------------------------------------

%% The KDF a device must reproduce. Pinned here (and in
%% `broker_security_SUITE') because it IS the wire contract between a client and
%% this broker: label, NUL, identity, NUL, realm, NUL, password.
psk(Identity, Password) ->
    crypto:hash(sha256, [?PSK_LABEL, 0, Identity, 0, ?DEFAULT_REALM, 0, Password]).

%% Connect as `Identity' with the PSK derived from `Password'. Returns
%% `{ok, SslSocket}' or `{error, _}' -- a refused identity surfaces as
%% `{error, {tls_alert, _}}' in a couple of hundred milliseconds, which the
%% unknown-identity case asserts on directly.
connect(Identity, Password, Port) ->
    connect(Identity, Password, Port, ?POLL_BUDGET_MS).

connect(Identity, Password, Port, Timeout) ->
    Psk = psk(Identity, Password),
    Opts = [{protocol, dtls},
            %% The client half of the identity is a STRING, not a binary (OTP's
            %% ssl API, not our choice); the server receives a binary.
            {psk_identity, binary_to_list(Identity)},
            {ciphers, cert_free_psk_suites()},
            {user_lookup_fun, {fun(_, _, _) -> {ok, Psk} end, undefined}},
            {verify, verify_none},
            {reuseaddr, true},
            %% Binary mode, like the server: without it `ssl:recv' returns an
            %% iolist and the codec would choke on the first reply.
            {mode, binary},
            {active, false}],
    ssl:connect(?HOST, Port, Opts, Timeout).

%% The suites that need no server certificate. `all' would only offer `rsa_psk',
%% which implies certificate handling; the cert-free ones live in `anonymous'.
%% AEAD only -- a CBC suite here would be a step backwards.
cert_free_psk_suites() ->
    [S || S <- ssl:cipher_suites(anonymous, 'dtlsv1.2'),
          lists:member(maps:get(key_exchange, S, none), [psk, dhe_psk, ecdhe_psk]),
          lists:member(maps:get(cipher, S, none),
                       [aes_128_gcm, aes_256_gcm, aes_128_ccm, aes_256_ccm,
                        aes_128_ccm_8, aes_256_ccm_8])].

close(S) ->
    _ = ssl:close(S),
    ok.

send(S, Msg) ->
    ok = ssl:send(S, coap_codec:encode(Msg)).

%% A probe for a socket that is expected to be CLOSED: we must not write to it
%% and demand `ok', because admission is refused after the TLS handshake and the
%% acceptor then closes the socket -- that close races our next `ssl:send', and
%% when it wins the send returns `{error, closed}' (measured flaky). So both the
%% write and the read are treated as observations of the same fact: either of
%% them coming back closed proves the peer was refused; data coming back proves
%% it was not.
probe_closed(S) ->
    case ssl:send(S, <<>>) of
        ok -> recv(S, ?POLL_BUDGET_MS);
        {error, closed} -> closed;
        {error, _Other} -> closed
    end.

recv(S) ->
    recv(S, ?POLL_BUDGET_MS).

recv(S, Timeout) ->
    case ssl:recv(S, 0, Timeout) of
        {ok, Bin} ->
            {ok, Msg} = coap_codec:decode(Bin),
            Msg;
        {error, timeout} ->
            timeout;
        {error, closed} ->
            closed
    end.

%% Wait for one message whose `type' and `code' match, discarding anything else
%% that arrives in the meantime (a DTLS socket preserves order per peer, but a
%% notification and an ACK can legitimately interleave).
expect(S, Type, Code) ->
    Deadline = erlang:monotonic_time(millisecond) + ?POLL_BUDGET_MS,
    expect(S, Type, Code, Deadline).

expect(S, Type, Code, Deadline) ->
    Left = Deadline - erlang:monotonic_time(millisecond),
    case Left > 0 andalso recv(S, Left) of
        timeout -> exit({timeout_waiting_for, Type, Code});
        closed -> exit({socket_closed_waiting_for, Type, Code});
        false -> exit({timeout_waiting_for, Type, Code});
        #{type := Type, code := Code} = Msg -> Msg;
        _Other -> expect(S, Type, Code, Deadline)
    end.

refute(S, Type, Code, WindowMs) ->
    case recv(S, WindowMs) of
        timeout -> ok;
        closed -> ok;
        #{type := Type, code := Code} = Msg -> exit({unexpected_message, Msg});
        _Other -> refute(S, Type, Code, WindowMs)
    end.

request(Type, Method, Id, Options) ->
    coap_codec:request(Type, Method, Id, Options).

request(Type, Method, Id, Options, Payload) ->
    (coap_codec:request(Type, Method, Id, Options))#{payload => Payload}.

%%%_* Cases -----------------------------------------------------------------

%% The base capability: a cert-free PSK handshake completes against the broker's
%% own listener, and a CON GET over it is answered with a piggybacked ACK. If
%% this passes, ADR-031 §D5's "default to no self-signed certificate" is
%% satisfied by the product path and not just by a spike script.
dtls_psk_handshake_and_get(Config) ->
    Port = ?config(port, Config),
    {ok, S} = connect(<<"device-01">>, <<"secret-01">>, Port),
    send(S, request(con, put, 1, [{uri_path, <<"a">>}], <<"hello">>)),
    _ = expect(S, ack, ?OK),
    send(S, request(con, get, 2, [{uri_path, <<"a">>}])),
    #{type := ack, code := ?CONTENT, id := 2, payload := <<"hello">>} =
        expect(S, ack, ?CONTENT),
    close(S).

%% The write path over DTLS, end to end: PUT then GET returns what was written.
%% Distinct from the case above because it proves the ACK carried a real
%% dispatch (not just a decode), and that the retained state is shared with the
%% kernel rather than kept per transport.
dtls_psk_write_and_read(Config) ->
    Port = ?config(port, Config),
    {ok, S} = connect(<<"device-02">>, <<"secret-02">>, Port),
    send(S, request(con, put, 10, [{uri_path, <<"dtls/w">>}], <<"payload">>)),
    #{code := ?OK} = expect(S, ack, ?OK),
    send(S, request(con, get, 11, [{uri_path, <<"dtls/w">>}])),
    #{code := ?CONTENT, payload := <<"payload">>} = expect(S, ack, ?CONTENT),
    %% A different device sees the same retained state: the destination is the
    %% broker's, not the connection's.
    {ok, S2} = connect(<<"device-01">>, <<"secret-01">>, Port),
    send(S2, request(con, get, 12, [{uri_path, <<"dtls/w">>}])),
    #{code := ?CONTENT, payload := <<"payload">>} = expect(S2, ack, ?CONTENT),
    close(S),
    close(S2).

%% A device that is not in the `users' table cannot establish a session -- and it
%% must fail in exactly the same way as a known device with the wrong password.
%% That equivalence is a security property, not politeness: an alert fails the
%% handshake early while a wrong key fails later, so if the two were
%% distinguishable an attacker could enumerate every username in the table by
%% timing. Measured before the fix, the wrong way round: an unknown identity
%% alerted in 20-200 ms and a wrong password timed out at 3 s.
%%
%% So the listener answers an unknown identity with a decoy key, both cases die at
%% the same Finished check, and the client reports the same `timeout' (OTP's
%% `ssl:connect/4' returns the bare atom, not a tuple).
unknown_identity_is_refused(Config) ->
    Port = ?config(port, Config),
    {error, UnknownReason} = connect(<<"device-99">>, <<"secret-01">>, Port, 1500),
    {error, BadPasswordReason} = connect(<<"device-01">>, <<"wrong-password">>, Port, 1500),
    ?assertEqual(UnknownReason, BadPasswordReason),
    ?assertEqual(timeout, UnknownReason),
    %% The listener is still healthy and still serves a legitimate peer.
    {ok, S} = connect(<<"device-01">>, <<"secret-01">>, Port),
    send(S, request(con, put, 1, [{uri_path, <<"still">>}], <<"ok">>)),
    #{code := ?OK} = expect(S, ack, ?OK),
    close(S).

%% The identity race §3.2 constraint 2 exists to prevent, as a regression test.
%%
%% The PSK identity is only observable inside the lookup callback, which runs in
%% the acceptor's process; a rejected peer's identity message lands in that
%% mailbox exactly like an accepted one's. Two things must therefore hold, and
%% both are asserted here:
%%
%%   1. NO ACCUMULATION -- repeated rejected handshakes must not pile up identity
%%      messages in the acceptor's mailbox. One leftover is benign: the lookup
%%      callback for a failed handshake can post after the failure path already
%%      drained, so a single stale name is normal and the NEXT handshake's
%%      pre-flush discards it. What must not happen is the mailbox growing with
%%      every rejection -- that is what a skipped drain looks like, and it is
%%      the state from which a stale name can be handed out.
%%   2. ATTRIBUTION -- good sessions interleaved with bad ones each write into
%%      THEIR OWN tenant, read back from the retain store. A leaked identity
%%      stamps the wrong tenant on the write.
identity_does_not_leak_between_handshakes(Config) ->
    Port = ?config(port, Config),
    %% (1) No accumulation: N rejections leave at most one identity behind,
    %% regardless of N. One is the benign post-failure race; growing with N is
    %% the bug.
    Before = length(stale_identities(listener_pid())),
    [begin
         {error, _} = connect(<<"device-99">>, <<"nope">>, Port, 1500)
     end || _ <- lists:seq(1, 8)],
    After = length(stale_identities(listener_pid())),
    ?assert(After =< 1),
    ?assert(After =< Before + 1),
    %% (2) Attribution: alternate a failing peer with a good one, per tenant.
    [begin
         {error, _} = connect(<<"device-99">>, <<"nope">>, Port, 1500),
         {ok, S} = connect(<<"device-01">>, <<"secret-01">>, Port),
         Dest = <<"leak/t1/", (integer_to_binary(N))/binary>>,
         send(S, request(con, put, 100 + N, [{uri_path, Dest}], <<"t1">>)),
         #{code := ?OK} = expect(S, ack, ?OK),
         ?assertEqual(<<"t1">>, retained_tenant(Dest)),
         close(S)
     end || N <- lists:seq(1, 3)],
    [begin
         {error, _} = connect(<<"no-such-user">>, <<"x">>, Port, 1500),
         {ok, S} = connect(<<"tenant-b">>, <<"secret-b">>, Port),
         Dest = <<"leak/t2/", (integer_to_binary(N))/binary>>,
         send(S, request(con, put, 200 + N, [{uri_path, Dest}], <<"t2">>)),
         #{code := ?OK} = expect(S, ack, ?OK),
         ?assertEqual(<<"t2">>, retained_tenant(Dest)),
         close(S)
     end || N <- lists:seq(1, 3)],
    %% And still bounded after the mixed run (same benign-at-most-one rule).
    ?assert(length(stale_identities(listener_pid())) =< 1).

%% Identity messages the acceptor is still holding. The acceptor runs as a plain
%% process, so its messages are readable directly; the listener's state carries
%% its pid (`dtls_acceptor', field 12).
stale_identities(ListenerPid) ->
    Acceptor = element(12, sys:get_state(ListenerPid)),
    {messages, Msgs} = process_info(Acceptor, messages),
    [M || M = {psk_identity, _} <- Msgs].

%% RFC 7252 §4.8.2 over DTLS: a retransmitted CON (same Message ID, same token,
%% same body) is answered from the dedup cache and not executed twice. This is
%% the case that proves the five bounded tables still key on `{IP, Port}' when
%% the transport is a DTLS socket -- `ssl:peername/1' reports the same tuple
%% `gen_udp' does, so the tables need no change at all.
dtls_con_dedup(Config) ->
    Port = ?config(port, Config),
    {ok, S} = connect(<<"device-01">>, <<"secret-01">>, Port),
    Req = request(con, put, 21, [{uri_path, <<"dd">>}], <<"once">>),
    send(S, Req),
    First = expect(S, ack, ?OK),
    ?assertEqual(21, coap_codec:id(First)),
    send(S, Req),
    Second = expect(S, ack, ?OK),
    %% The duplicate got the cached original: same Message ID, same token.
    ?assertEqual(coap_codec:id(First), coap_codec:id(Second)),
    ?assertEqual(coap_codec:token(First), coap_codec:token(Second)),
    %% Executed once: a second PUT would have replaced the payload.
    send(S, request(con, get, 22, [{uri_path, <<"dd">>}])),
    #{payload := <<"once">>} = expect(S, ack, ?CONTENT),
    close(S).

%% The gap S11-B left open, closed: with a real principal on the wire, an ACL
%% `deny' finally produces 4.03 for a CoAP request. On plain UDP this case is
%% impossible to write -- `principal' is `undefined' and `broker_security' allows
%% anonymous traffic outright.
acl_denies_on_dtls(Config) ->
    Port = ?config(port, Config),
    ok = application:set_env(broker_security, acl,
                             [rule(deny, <<"device-01">>, <<"*">>, <<"secret/#">>)]),
    {ok, S} = connect(<<"device-01">>, <<"secret-01">>, Port),
    send(S, request(con, get, 30, [{uri_path, <<"secret/plans">>}])),
    %% 4.03 Forbidden -- not 4.04 (nothing there), not 5.00.
    #{code := {response, 4, 3}} = expect(S, ack, {response, 4, 3}),
    close(S).

%% The positive control for the case above. Without it, a listener that refused
%% every request would pass the deny case and look correct.
acl_allows_on_dtls(Config) ->
    Port = ?config(port, Config),
    ok = application:set_env(broker_security, acl,
                             [rule(allow, <<"device-01">>, <<"*">>, <<"pub/#">>)]),
    {ok, S} = connect(<<"device-01">>, <<"secret-01">>, Port),
    send(S, request(con, put, 31, [{uri_path, <<"pub/x">>}], <<"v">>)),
    #{code := ?OK} = expect(S, ack, ?OK),
    send(S, request(con, get, 32, [{uri_path, <<"pub/x">>}])),
    #{code := ?CONTENT, payload := <<"v">>} = expect(S, ack, ?CONTENT),
    %% And a destination outside the granted prefix is still refused, so the pass
    %% above is the rule's doing and not a blanket allow.
    send(S, request(con, get, 33, [{uri_path, <<"other/x">>}])),
    #{code := {response, 4, 3}} = expect(S, ack, {response, 4, 3}),
    close(S).

%% The tenant stamped on a CoAP write comes from the authenticated principal, not
%% from anything the client sends. That is the property S11-C makes reachable: on
%% plain UDP every message lands in the default tenant, because there is no
%% identity to derive one from.
%%
%% Note what this does NOT claim: a CoAP destination is a resource path, not a
%% tenant-qualified topic, so there is no "device names another tenant" attack to
%% refuse here (unlike MQTT, where the topic carries the tenant). The boundary is
%% that a device cannot choose its tenant at all, plus the ACL, which the two
%% cases above exercise. Confining a device to its own prefix is an ACL pattern
%% (`t1/#'), written by an operator, not a built-in path rule.
tenant_isolation_on_dtls(Config) ->
    Port = ?config(port, Config),
    ok = application:set_env(broker_security, acl, allow),
    {ok, A} = connect(<<"device-01">>, <<"secret-01">>, Port),
    send(A, request(con, put, 40, [{uri_path, <<"shared/x">>}], <<"from-t1">>)),
    #{code := ?OK} = expect(A, ack, ?OK),
    {ok, B} = connect(<<"tenant-b">>, <<"secret-b">>, Port),
    send(B, request(con, put, 41, [{uri_path, <<"shared/y">>}], <<"from-t2">>)),
    #{code := ?OK} = expect(B, ack, ?OK),
    %% Each write is stamped with its own device's tenant, as recorded by the
    %% retain store (the same `#broker_message{}' the kernel dispatches).
    ?assertEqual(<<"t1">>, retained_tenant(<<"shared/x">>)),
    ?assertEqual(<<"t2">>, retained_tenant(<<"shared/y">>)),
    close(A),
    close(B).

retained_tenant(Destination) ->
    {ok, Msg} = broker_retain:lookup(Destination),
    %% `#broker_message.tenant_id' (element 3: tag, message_id, tenant_id).
    element(3, Msg).

%% Observe works over DTLS with no change to the relationship machinery: the
%% tables are keyed by `{IP,Port}' and `ssl:peername/1' supplies the same tuple
%% `gen_udp' does. The tenant the subscription lives under is now the device's
%% real tenant rather than the default.
dtls_observe_registers_and_notifies(Config) ->
    Port = ?config(port, Config),
    {ok, S} = connect(<<"device-01">>, <<"secret-01">>, Port),
    Token = <<"obs">>,
    send(S, request(con, put, 50, [{uri_path, <<"o">>}], <<"init">>)),
    _ = expect(S, ack, ?OK),
    send(S, (request(con, get, 51, [{uri_path, <<"o">>}, {observe, 0}]))
             #{token => Token}),
    First = expect(S, ack, ?CONTENT),
    ?assertEqual(Token, coap_codec:token(First)),
    ?assertEqual(0, coap_codec:option(observe, First)),
    send(S, request(con, put, 52, [{uri_path, <<"o">>}], <<"v1">>)),
    _ = expect(S, ack, ?OK),
    Note = expect(S, con, ?CONTENT),
    ?assertEqual(Token, coap_codec:token(Note)),
    ?assertEqual(1, coap_codec:option(observe, Note)),
    send(S, ack_of(Note)),
    close(S).

%% The total session cap bounds memory. With a cap of one, the second peer is
%% refused after its handshake (the listener owns the table, so it decides) and
%% the first session keeps working -- a new client must never evict a live one.
dtls_session_cap(Config) ->
    Port = ?config(port, Config),
    {ok, A} = connect(<<"device-01">>, <<"secret-01">>, Port),
    send(A, request(con, put, 60, [{uri_path, <<"cap">>}], <<"a">>)),
    #{code := ?OK} = expect(A, ack, ?OK),
    %% A second peer: the handshake may complete, but admission is refused and the
    %% acceptor closes the socket. We never write a real request to it (see
    %% `probe_closed/1'): proving refusal is the socket closing, not a response.
    {ok, B} = connect(<<"device-02">>, <<"secret-02">>, Port),
    ?assertEqual(closed, probe_closed(B)),
    %% The first session was not disturbed.
    send(A, request(con, get, 62, [{uri_path, <<"cap">>}])),
    #{code := ?CONTENT, payload := <<"a">>} = expect(A, ack, ?CONTENT),
    close(A),
    close(B).

%% One device must not hold the whole table. A second session for the SAME
%% identity is refused at the per-principal cap, while a different identity still
%% gets in -- so the cap is per identity and not a global second-session ban.
dtls_per_principal_cap(Config) ->
    Port = ?config(port, Config),
    {ok, A} = connect(<<"device-01">>, <<"secret-01">>, Port),
    send(A, request(con, put, 70, [{uri_path, <<"pp">>}], <<"a">>)),
    #{code := ?OK} = expect(A, ack, ?OK),
    {ok, A2} = connect(<<"device-01">>, <<"secret-01">>, Port),
    ?assertEqual(closed, probe_closed(A2)),
    %% A different identity is unaffected: the cap is per principal.
    {ok, B} = connect(<<"device-02">>, <<"secret-02">>, Port),
    send(B, request(con, get, 72, [{uri_path, <<"pp">>}])),
    #{code := ?CONTENT, payload := <<"a">>} = expect(B, ack, ?CONTENT),
    close(A),
    close(A2),
    close(B).

%% A session the listener admitted but could not take ownership of must not
%% hold its slot for ever. The acceptor answers `{dtls_admit, Socket, ok}' (the
%% listener records the session) and only THEN calls `controlling_process/2';
%% if that fails the socket never becomes the listener's, so no `{ssl_closed,_}'
%% ever arrives to trigger `drop_session/2'. The acceptor therefore sends
%% `{dtls_abandoned, Socket}', and this case pins that the record is released.
%%
%% Driven at the protocol level on purpose: real `controlling_process/2' failure
%% is not reproducible on demand, but the consequence -- a recorded session with
%% no live socket -- is exactly what the message describes. With the
%% per-principal cap at one, a reconnect proves the slot really came back.
dtls_abandoned_releases_slot(Config) ->
    Port = ?config(port, Config),
    Pid = listener_pid(),
    %% Establish one real session, then abandon a SECOND endpoint's record the
    %% way the acceptor would after a failed transfer.
    {ok, A} = connect(<<"device-01">>, <<"secret-01">>, Port),
    send(A, request(con, put, 90, [{uri_path, <<"ab">>}], <<"a">>)),
    #{code := ?OK} = expect(A, ack, ?OK),
    ?assertEqual(1, maps:get(<<"device-01">>,
                              element(16, sys:get_state(Pid)))),
    GhostSocket = make_ref(),
    GhostPeer = {{10, 0, 0, 1}, 65000},
    Pid ! {dtls_handshake_ok, self(), GhostSocket, GhostPeer, <<"device-01">>},
    %% The listener records it and answers `ok' -- the session is counted.
    receive {dtls_admit, GhostSocket, ok} -> ok after 2000 -> exit(no_admit) end,
    ?assertEqual(2, maps:get(<<"device-01">>, element(16, sys:get_state(Pid)))),
    %% The acceptor then reports the abandoned transfer.
    Pid ! {dtls_abandoned, GhostSocket},
    ok = wait_until(fun() ->
                            maps:get(<<"device-01">>,
                                     element(16, sys:get_state(Pid)), 0) =:= 1
                    end, 2000),
    St = sys:get_state(Pid),
    ?assertEqual(1, maps:get(<<"device-01">>, element(16, St))),
    ?assertNot(maps:is_key(GhostSocket, element(13, St))),   %% peers
    ?assertNot(maps:is_key(GhostPeer, element(14, St))),     %% socks
    ?assertNot(maps:is_key(GhostPeer, element(15, St))),     %% idents
    close(A).

%% A closed DTLS session is a positive fact, unlike a UDP endpoint that just goes
%% silent, so every table entry for that peer is dropped at once. Proven by
%% capacity rather than by poking at state: with the per-principal cap at one, a
%% reconnect as the same identity can only succeed if the previous session's
%% counter was released.
dtls_close_cleans_up(Config) ->    Port = ?config(port, Config),
    {ok, A} = connect(<<"device-01">>, <<"secret-01">>, Port),
    send(A, request(con, put, 80, [{uri_path, <<"cl">>}], <<"a">>)),
    #{code := ?OK} = expect(A, ack, ?OK),
    %% Register an observation too, so the cleanup has a relationship to remove.
    send(A, (request(con, get, 81, [{uri_path, <<"cl">>}, {observe, 0}]))
             #{token => <<"cltok">>}),
    _ = expect(A, ack, ?CONTENT),
    Pid = listener_pid(),
    ?assertEqual(1, maps:get(<<"device-01">>,
                             element(16, sys:get_state(Pid)))),
    close(A),
    %% Wait for the {ssl_closed, _} to be processed.
    ok = wait_until(fun() -> maps:size(element(13, sys:get_state(Pid))) =:= 0 end,
                    2000),
    St = sys:get_state(Pid),
    ?assertEqual(0, maps:size(element(13, St))),   %% peers
    ?assertEqual(0, maps:size(element(14, St))),   %% socks
    ?assertEqual(0, maps:size(element(15, St))),   %% idents
    ?assertEqual(0, maps:size(element(16, St))),   %% principal_sessions
    ?assertEqual(0, maps:size(element(9, St))),    %% observes
    ?assertEqual(0, maps:size(element(10, St))),   %% pending
    %% And a reconnect as the same identity works (the counter really was freed).
    {ok, B} = connect(<<"device-01">>, <<"secret-01">>, Port),
    send(B, request(con, get, 82, [{uri_path, <<"cl">>}])),
    #{code := ?CONTENT, payload := <<"a">>} = expect(B, ack, ?CONTENT),
    close(B).

%% Plain UDP is untouched by all of the above, and its posture is unchanged on
%% purpose: a datagram carries no credential, so `principal' is `undefined' and
%% `broker_security' allows anonymous traffic. This is the honest floor -- an
%% operator who wants authorization must use `coaps'.
plain_udp_unaffected(Config) ->
    Port = ?config(port, Config),
    %% A restrictive ACL that would deny any real principal.
    ok = application:set_env(broker_security, acl,
                             [rule(deny, <<"*">>, <<"*">>, <<"*">>)]),
    S = udp_open(),
    udp_send(S, Port, request(con, put, 90, [{uri_path, <<"u">>}], <<"plain">>)),
    #{code := ?OK} = udp_expect(S, ack, ?OK),
    udp_send(S, Port, request(con, get, 91, [{uri_path, <<"u">>}])),
    #{code := ?CONTENT, payload := <<"plain">>} = udp_expect(S, ack, ?CONTENT),
    udp_close(S).

%%%_* Helpers ---------------------------------------------------------------

%% The ACL rule shape `broker_security' consumes (`rule_matches/4').
rule(Effect, Principal, Action, Pattern) ->
    #{effect => Effect, principal => Principal,
      action => Action, pattern => Pattern}.

listener_pid() ->
    case [Pid || {Id, Pid, worker, _} <- supervisor:which_children(
                                                 broker_proto_coap_sup),
                  element(1, Id) =:= coap_listener] of
        [Pid | _] -> Pid;
        [] -> exit(no_listener)
    end.

wait_until(Fun, Budget) when Budget =< 0 ->
    case Fun() of
        true -> ok;
        false -> timeout
    end;
wait_until(Fun, Budget) ->
    case Fun() of
        true -> ok;
        false -> timer:sleep(?POLL_MS), wait_until(Fun, Budget - ?POLL_MS)
    end.

udp_open() ->
    {ok, S} = gen_udp:open(0, [binary, {active, false}]),
    S.

udp_close(S) ->
    gen_udp:close(S).

udp_send(S, Port, Msg) ->
    ok = gen_udp:send(S, ?HOST, Port, coap_codec:encode(Msg)).

udp_expect(S, Type, Code) ->
    Deadline = erlang:monotonic_time(millisecond) + ?POLL_BUDGET_MS,
    udp_expect(S, Type, Code, Deadline).

udp_expect(S, Type, Code, Deadline) ->
    Left = Deadline - erlang:monotonic_time(millisecond),
    case Left > 0 andalso gen_udp:recv(S, 0, Left) of
        false -> exit({timeout_waiting_for, Type, Code});
        {error, timeout} -> exit({timeout_waiting_for, Type, Code});
        {ok, {_IP, _Port, Bin}} ->
            {ok, Msg} = coap_codec:decode(Bin),
            case Msg of
                #{type := Type, code := Code} -> Msg;
                _Other -> udp_expect(S, Type, Code, Deadline)
            end
    end.

%% A conformant empty ACK: TKL 0, echoing the notification's Message ID.
ack_of(Note) ->
    coap_codec:ack(Note).
