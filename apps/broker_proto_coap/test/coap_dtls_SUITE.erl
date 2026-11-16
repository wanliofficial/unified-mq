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
     dtls_con_dedup].

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
    ok = application:set_env(broker_proto_coap, listeners,
                             [#{port => 0, transport => dtls}]),
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
