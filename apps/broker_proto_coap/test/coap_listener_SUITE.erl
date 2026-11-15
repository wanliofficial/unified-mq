%% @doc The CoAP UDP listener: real datagrams through the real listener.
%%
%% The suite talks to the listener the way a device would -- one UDP socket, one
%% encoded datagram at a time -- and asserts on the decoded messages that come
%% back, not on process bookkeeping. That matters here more than usual: CoAP has
%% no connection, so "the resource was written" is only observable through the
%% ACK, the retained state a later GET returns, and the dedup window that makes a
%% retransmitted CON harmless.
%%
%% Coverage is the ADR-031 acceptance surface: CON gets a piggybacked ACK, a
%% duplicate CON within EXCHANGE_LIFETIME is answered with the original ACK and
%% NOT re-executed (§D2), a NON write is dispatched live and is not answered (§D3,
%% at-most-once), a body larger than one datagram is reassembled with Block1
%% (draft-ietf-core-block-18 per §D7), a flood is bounded by the per-endpoint
%% token bucket (R-X3), and malformed or oversized datagrams are dropped.
%%
%% The suite's own client is byte-level on purpose: every helper below encodes
%% with `coap_codec' and decodes what arrives, so a codec change that breaks the
%% listener breaks these cases too.
-module(coap_listener_SUITE).

-compile([export_all, nowarn_export_all]).
-compile({no_auto_import, [put/2]}).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(N, 2).
-define(POLL_MS, 20).
-define(POLL_BUDGET_MS, 5000).
-define(HOST, {127, 0, 0, 1}).

all() ->
    [con_get_returns_piggybacked_content,
     duplicate_con_is_answered_with_the_original_ack,
     non_put_dispatches_without_a_response,
     block1_reassembles_a_multidatagram_body,
     token_bucket_bounds_a_flood,
     oversized_datagram_is_dropped,
     malformed_datagram_is_dropped,
     observe_ack_and_rst_are_not_requests,
     observe_con_notifies_and_acks,
     observe_con_retries_then_gives_up,
     observe_non_is_at_most_once,
     observe_deregister_with_observe_1,
     observe_rst_deregisters,
     observe_bounded_table_refuses,
     observe_plain_udp_principal_is_allowed,
     observe_max_age_expires_relationship,
     observe_two_observers_same_dest_both_notified].

%%%_* Fixtures --------------------------------------------------------------

init_per_suite(Config) ->
    %% Load every kernel app up front so the per-case `set_env' calls below
    %% override the loaded (app.src default) environment instead of being
    %% overwritten when `ensure_all_started' later loads the .app file. This is
    %% the ordering `mqtt_sn_conn_SUITE' relies on.
    _ = application:load(broker_storage),
    _ = application:load(broker_router),
    _ = application:load(broker_queue),
    _ = application:load(broker_subscription),
    _ = application:load(broker_retain),
    _ = application:load(broker_event),
    _ = application:load(broker_security),
    _ = application:load(broker_proto_coap),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(Case, Config0) ->
    stop_all(),
    Saved = #{acl => env(broker_security, acl),
              require_auth => env(broker_security, require_auth),
              users => env(broker_security, users),
              max_rps => env(broker_proto_coap, max_requests_per_second),
              max_msg => env(broker_proto_coap, max_message_bytes),
              max_observes => env(broker_proto_coap, max_observes),
              ack_timeout => env(broker_proto_coap, observe_ack_timeout_ms),
              max_retrans => env(broker_proto_coap, observe_max_retrans),
              max_age => env(broker_proto_coap, observe_max_age_s)},
    DataDir = filename:join([?config(priv_dir, Config0), atom_to_list(Case), "data"]),
    ok = filelib:ensure_dir(filename:join([DataDir, "x"])),
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok = application:set_env(broker_storage, data_dir, DataDir),
    ok = application:set_env(broker_storage, wal, #{require_durable_fs => false}),
    ok = application:set_env(broker_queue, partitions, ?N),
    ok = application:set_env(broker_router, partitions, ?N),
    ok = application:set_env(broker_subscription, data_dir, DataDir),
    ok = application:set_env(broker_retain, data_dir, DataDir),
    ok = application:set_env(broker_security, users, []),
    ok = application:set_env(broker_security, acl, allow),
    ok = application:set_env(broker_security, require_auth, []),
    %% The app under test. `token_bucket_bounds_a_flood' pins `max_requests_per_second'
    %% to 2 and `oversized_datagram_is_dropped' shrinks `max_message_bytes' below
    %% the shipped default, both before the listener starts.
    ok = application:set_env(broker_proto_coap, enabled, true),
    ok = application:set_env(broker_proto_coap, listeners, [#{port => 0}]),
    ok = application:set_env(broker_proto_coap, resources, []),
    {ok, _} = application:ensure_all_started(broker_storage),
    ok = create_partitions(),
    ok = case_env(Case, Config0),
    {ok, _} = application:ensure_all_started(broker_proto_coap),
    [{port, listener_port()}, {saved_env, Saved} | Config0].

end_per_testcase(_Case, Config) ->
    stop_all(),
    restore_env(?config(saved_env, Config)),
    ok.

%% The token-bucket case pins `max_requests_per_second' to 2 so a small burst
%% crosses the limit deterministically; every other case leaves the default.
case_env(token_bucket_bounds_a_flood, _Config) ->
    ok = application:set_env(broker_proto_coap, max_requests_per_second, 2);
%% The oversized-datagram case shrinks `max_message_bytes' so a modest body
%% crosses it without building a 64 KiB datagram.
case_env(oversized_datagram_is_dropped, _Config) ->
    ok = application:set_env(broker_proto_coap, max_message_bytes, 16);
%% The retry case pins a short retransmit timeout so it does not sit on 2 s.
case_env(observe_con_retries_then_gives_up, _Config) ->
    ok = application:set_env(broker_proto_coap, observe_ack_timeout_ms, 300),
    ok = application:set_env(broker_proto_coap, observe_max_retrans, 2);
case_env(observe_con_notifies_and_acks, _Config) ->
    %% Pin a short ACK timeout so the post-ACK silence window can actually catch a
    %% broken retransmit-cancel (the default 2000 ms would mask it).
    ok = application:set_env(broker_proto_coap, observe_ack_timeout_ms, 300);
%% The bounded-table case shrinks the relationship table to a single slot.
case_env(observe_bounded_table_refuses, _Config) ->
    ok = application:set_env(broker_proto_coap, max_observes, 1);
%% The max-age case sweeps after one second instead of thirty.
case_env(observe_max_age_expires_relationship, _Config) ->
    ok = application:set_env(broker_proto_coap, observe_max_age_s, 1);
case_env(_Case, _Config) ->
    ok.

env(App, Key) ->
    application:get_env(App, Key, undefined).

restore_env(#{acl := Acl, require_auth := Auth, users := Users,
              max_rps := Rps, max_msg := MaxMsg,
              max_observes := MaxObs, ack_timeout := AckTO,
              max_retrans := MaxRetrans, max_age := MaxAge}) ->
    set_or_unset(broker_security, acl, Acl),
    set_or_unset(broker_security, require_auth, Auth),
    set_or_unset(broker_security, users, Users),
    set_or_unset(broker_proto_coap, max_requests_per_second, Rps),
    set_or_unset(broker_proto_coap, max_message_bytes, MaxMsg),
    set_or_unset(broker_proto_coap, max_observes, MaxObs),
    set_or_unset(broker_proto_coap, observe_ack_timeout_ms, AckTO),
    set_or_unset(broker_proto_coap, observe_max_retrans, MaxRetrans),
    set_or_unset(broker_proto_coap, observe_max_age_s, MaxAge),
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

%% The one listener this suite runs: `enabled=true' + `port => 0' means the
%% kernel binds an ephemeral port and the listener child appears in the sup.
%%
%% The name is registered with a unique suffix (the test node, the pid and a
%% counter) rather than by port: with `port => 0' every listener would ask for
%% the same name and collide, and a CT run shares one node with other suites.
%% The suffix also keeps a restarted listener usable without re-registering.
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

%%%_* Client ----------------------------------------------------------------

open() ->
    {ok, S} = gen_udp:open(0, [binary, {active, false}]),
    S.

close(S) ->
    gen_udp:close(S).

send(S, Port, Msg) ->
    ok = gen_udp:send(S, ?HOST, Port, coap_codec:encode(Msg)).

recv(S) ->
    recv(S, ?POLL_BUDGET_MS).

recv(S, Timeout) ->
    case gen_udp:recv(S, 0, Timeout) of
        {ok, {_IP, _Port, Bin}} ->
            {ok, Msg} = coap_codec:decode(Bin),
            Msg;
        {error, timeout} ->
            timeout
    end.

%% Wait for one message whose `type' and `code' match, discarding anything else
%% that arrives in the meantime. UDP gives no ordering guarantee across replies,
%% so a strict next-message assertion would be flaky rather than strict.
expect(S, Type, Code) ->
    Deadline = erlang:monotonic_time(millisecond) + ?POLL_BUDGET_MS,
    expect(S, Type, Code, Deadline).

expect(S, Type, Code, Deadline) ->
    Left = Deadline - erlang:monotonic_time(millisecond),
    case Left > 0 andalso recv(S, Left) of
        timeout -> exit({timeout_waiting_for, Type, Code});
        false -> exit({timeout_waiting_for, Type, Code});
        #{type := Type, code := Code} = Msg -> Msg;
        _Other -> expect(S, Type, Code, Deadline)
    end.

%% Nothing matching may arrive within the window.
refute(S, Type, Code, WindowMs) ->
    case recv(S, WindowMs) of
        timeout -> ok;
        #{type := Type, code := Code} = Msg -> exit({unexpected_message, Msg});
        _Other -> refute(S, Type, Code, WindowMs)
    end.

%% Build a request message; payload set on the `payload' field directly.
request(Type, Method, Id, Options) ->
    coap_codec:request(Type, Method, Id, Options).

request(Type, Method, Id, Options, Payload) ->
    (coap_codec:request(Type, Method, Id, Options))#{payload => Payload}.

%% A conformant empty ACK: TKL 0, echoing the notification's Message ID (RFC 7252
%% §4.1 / RFC 7641 §4.5). The listener correlates a CON notification's ACK by ID,
%% not token, so this is exactly what a real client sends.
ack_of(Note) ->
    coap_codec:ack(Note).

%% A conformant empty RST: TKL 0, echoing the rejected notification's Message ID.
rst_of(Id) ->
    coap_codec:reset(#{id => Id}).

%%%_* Cases -----------------------------------------------------------------

%% A CON GET is answered with a piggybacked ACK that echoes the request's type,
%% Message ID and token (RFC 7252 §5.2.1). The ACK carries the retained
%% representation when the destination has one.
con_get_returns_piggybacked_content(Config) ->
    Port = ?config(port, Config),
    S = open(),
    %% Write the representation first.
    send(S, Port, request(con, put, 1, [{uri_path, <<"a">>}], <<"hello">>)),
    #{type := ack, code := {response, 2, 4}} = expect(S, ack, {response, 2, 4}),
    %% Now GET it: the ACK must echo id 1 and carry the payload.
    send(S, Port, request(con, get, 2, [{uri_path, <<"a">>}])),
    #{type := ack, code := {response, 2, 5}, id := 2, payload := <<"hello">>} =
        expect(S, ack, {response, 2, 5}),
    close(S).

%% RFC 7252 §4.8.2: a retransmitted CON within EXCHANGE_LIFETIME is answered
%% with the ORIGINAL ACK, not re-executed. The listener caches the reply keyed
%% by {IP, Port, Id} and resends the cached bytes on a duplicate.
duplicate_con_is_answered_with_the_original_ack(Config) ->
    Port = ?config(port, Config),
    S = open(),
    Req = request(con, put, 7, [{uri_path, <<"d">>}], <<"once">>),
    send(S, Port, Req),
    First = expect(S, ack, {response, 2, 4}),
    ?assertEqual(7, coap_codec:id(First)),
    %% Retransmit the exact same datagram (same id, same token, same body).
    send(S, Port, Req),
    Second = expect(S, ack, {response, 2, 4}),
    %% The second ACK is the cached original: identical Message ID and token.
    ?assertEqual(coap_codec:id(First), coap_codec:id(Second)),
    ?assertEqual(coap_codec:token(First), coap_codec:token(Second)),
    %% The resource was executed exactly once: a second PUT would have replaced
    %% the payload, so a GET still returns the first write.
    send(S, Port, request(con, get, 8, [{uri_path, <<"d">>}])),
    #{payload := <<"once">>} = expect(S, ack, {response, 2, 5}),
    close(S).

%% ADR-031 §D3: a NON request is at-most-once. The listener dispatches it live
%% (the retained state is updated, so a later GET sees it) but owes no response,
%% so nothing comes back for the NON write itself.
non_put_dispatches_without_a_response(Config) ->
    Port = ?config(port, Config),
    W = open(),
    send(W, Port, request(non, put, 1, [{uri_path, <<"live">>}], <<"body">>)),
    %% No piggybacked ACK is owed for a NON request.
    refute(W, ack, {response, 2, 4}, 300),
    %% But the write landed: a later CON GET reads it back.
    G = open(),
    send(G, Port, request(con, get, 2, [{uri_path, <<"live">>}])),
    #{payload := <<"body">>} = expect(G, ack, {response, 2, 5}),
    close(W),
    close(G).

%% RFC 7959 draft §2.1 (ADR-031 §D7): a body larger than one datagram arrives
%% as a sequence of Block1 messages. The first (M=1) is answered with a 2.31
%% Continue echoing the next block number; the last (M=0) completes the stream
%% and is dispatched to the resource layer.
%%
%% The blocks carry the SAME token but DIFFERENT Message IDs, as a real client
%% sends them: a transfer is one request, so reassembly must be keyed by the
%% token, not by the per-datagram Message ID.
block1_reassembles_a_multidatagram_body(Config) ->
    Port = ?config(port, Config),
    S = open(),
    %% Two 3-byte blocks of a 6-byte body, block size 16 (so no SZX extension).
    Chunk1 = <<1, 2, 3>>,
    Chunk2 = <<4, 5, 6>>,
    Token = <<"blk1">>,
    %% Block1 value encodes {Num, More, Size}.
    Block1First = coap_codec:block_value(0, true, 16),
    Block1Last = coap_codec:block_value(1, false, 16),
    Req1 = (request(con, put, 1, [{uri_path, <<"big">>}, {block1, Block1First}],
                    Chunk1))#{token => Token},
    send(S, Port, Req1),
    %% The listener answers 2.31 Continue with the next block number (1).
    Continue = expect(S, ack, {response, 2, 31}),
    ?assertEqual(Token, coap_codec:token(Continue)),
    ?assertEqual({1, true, 16}, coap_codec:block(block1, Continue)),
    %% Send the last block with a new Message ID and the same token.
    Req2 = (request(con, put, 2, [{uri_path, <<"big">>}, {block1, Block1Last}],
                    Chunk2))#{token => Token},
    send(S, Port, Req2),
    #{type := ack, code := {response, 2, 4}} = expect(S, ack, {response, 2, 4}),
    %% The assembled body is what was retained: a GET returns both chunks.
    send(S, Port, request(con, get, 3, [{uri_path, <<"big">>}])),
    #{payload := <<1, 2, 3, 4, 5, 6>>} = expect(S, ack, {response, 2, 5}),
    close(S).

%% R-X3: the per-endpoint token bucket bounds a flood. With the limit pinned to
%% 2 requests/second, the first two datagrams are answered and the rest are
%% dropped silently within the same second (no ACK, no crash).
token_bucket_bounds_a_flood(Config) ->
    Port = ?config(port, Config),
    S = open(),
    %% Three CON PUTs to distinct destinations so dedup does not mask the rate
    %% limit; all within the same millisecond so the bucket cannot refill.
    lists:foreach(
      fun(N) ->
              send(S, Port, request(con, put, N,
                                    [{uri_path, list_to_binary(integer_to_list(N))}],
                                    <<"x">>))
      end, [1, 2, 3]),
    %% Exactly two ACKs come back (the bucket's two tokens); the third is dropped.
    _ = expect(S, ack, {response, 2, 4}),
    _ = expect(S, ack, {response, 2, 4}),
    refute(S, ack, {response, 2, 4}, 300),
    close(S).

%% A datagram larger than `max_message_bytes' is dropped before decoding: no ACK,
%% and the listener keeps serving well-formed datagrams afterwards.
oversized_datagram_is_dropped(Config) ->
    Port = ?config(port, Config),
    S = open(),
    %% The cap is pinned to 16 bytes for this case (see `case_env/2'), so a GET
    %% whose raw datagram is larger is dropped before the resource layer runs.
    Big = binary:copy(<<$z>>, 32),
    send(S, Port, request(con, get, 1, [{uri_path, <<"a">>}], Big)),
    refute(S, ack, {response, 4, 4}, 300),
    %% The listener is still alive: a well-formed small datagram is answered.
    send(S, Port, request(con, get, 2, [{uri_path, <<"a">>}])),
    #{code := {response, 4, 4}} = expect(S, ack, {response, 4, 4}),
    close(S).

%% A malformed datagram (a decode error) is dropped silently: no ACK, no crash,
%% and a well-formed datagram from the same socket still works.
malformed_datagram_is_dropped(Config) ->
    Port = ?config(port, Config),
    S = open(),
    %% A datagram the codec rejects: a header whose token-length nibble (9) is
    %% reserved, so `coap_codec:decode' returns {error, _} and the listener
    %% drops it without replying. (TKL is the low 4 bits of byte 0, so TKL 9 is
    %% 16#49.)
    ok = gen_udp:send(S, ?HOST, Port, <<16#49, 1, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8>>),
    %% No ACK for the garbage.
    timer:sleep(100),
    refute(S, ack, {response, 2, 4}, 200),
    %% A well-formed datagram from the same socket is still answered.
    send(S, Port, request(con, put, 1, [{uri_path, <<"ok">>}], <<"v">>)),
    #{type := ack, code := {response, 2, 4}} = expect(S, ack, {response, 2, 4}),
    close(S).

%% A bare ACK and a bare RST are transport control messages, not requests: the
%% listener must not answer either (no 2.05/4.04 in flight).
observe_ack_and_rst_are_not_requests(Config) ->
    Port = ?config(port, Config),
    S = open(),
    send(S, Port, ack_of(#{id => 4242})),
    send(S, Port, rst_of(4243)),
    refute(S, ack, {response, 2, 5}, 300),
    refute(S, ack, {response, 4, 4}, 300),
    close(S).

%% CON Observe=0 -> piggybacked 2.05 with Observe: 0; a later PUT -> a CON
%% notification with Observe: 1; ACK it -> no retransmit; a further PUT -> the
%% second notification.
observe_con_notifies_and_acks(Config) ->
    Port = ?config(port, Config),
    S = open(),
    Token = <<"ob1">>,
    send(S, Port, request(con, put, 1, [{uri_path, <<"o">>}], <<"init">>)),
    _ = expect(S, ack, {response, 2, 4}),
    send(S, Port, (request(con, get, 2, [{uri_path, <<"o">>}, {observe, 0}]))#{token => Token}),
    First = expect(S, ack, {response, 2, 5}),
    ?assertEqual(Token, coap_codec:token(First)),
    ?assertEqual(0, coap_codec:option(observe, First)),
    send(S, Port, request(con, put, 3, [{uri_path, <<"o">>}], <<"v1">>)),
    _ = expect(S, ack, {response, 2, 4}),
    Note = expect(S, con, {response, 2, 5}),
    ?assertEqual(Token, coap_codec:token(Note)),
    ?assertEqual(1, coap_codec:option(observe, Note)),
    send(S, Port, ack_of(Note)),
    %% ACK timeout is pinned to 300 ms (see case_env); a broken cancel would
    %% retransmit at 300 ms, so the silence window must exceed that to catch it.
    refute(S, con, {response, 2, 5}, 700),
    send(S, Port, request(con, put, 4, [{uri_path, <<"o">>}], <<"v2">>)),
    _ = expect(S, ack, {response, 2, 4}),
    Note2 = expect(S, con, {response, 2, 5}),
    ?assertEqual(2, coap_codec:option(observe, Note2)),
    close(S).

%% A CON notification that is never ACKed is retransmitted (same Message ID) and
%% finally given up: the relationship is torn down and no further notification
%% arrives.
observe_con_retries_then_gives_up(Config) ->
    Port = ?config(port, Config),
    S = open(),
    send(S, Port, request(con, get, 1, [{uri_path, <<"r">>}])),
    _ = expect(S, ack, {response, 4, 4}),
    send(S, Port, request(con, put, 2, [{uri_path, <<"r">>}], <<"x">>)),
    _ = expect(S, ack, {response, 2, 4}),
    %% The Observe registration itself needs a 2.05 first.
    send(S, Port, request(con, get, 3, [{uri_path, <<"r">>}, {observe, 0}])),
    _ = expect(S, ack, {response, 2, 5}),
    send(S, Port, request(con, put, 4, [{uri_path, <<"r">>}], <<"y">>)),
    _ = expect(S, ack, {response, 2, 4}),
    N1 = expect(S, con, {response, 2, 5}),
    N2 = expect(S, con, {response, 2, 5}),
    ?assertEqual(coap_codec:id(N1), coap_codec:id(N2)),
    N3 = expect(S, con, {response, 2, 5}),
    ?assertEqual(coap_codec:id(N1), coap_codec:id(N3)),
    send(S, Port, request(con, put, 5, [{uri_path, <<"r">>}], <<"z">>)),
    _ = expect(S, ack, {response, 2, 4}),
    refute(S, con, {response, 2, 5}, 600),
    close(S).

%% A NON Observe=0 gets at most one notification: no retransmit timer, no retry.
observe_non_is_at_most_once(Config) ->
    Port = ?config(port, Config),
    S = open(),
    send(S, Port, request(non, put, 1, [{uri_path, <<"n">>}], <<"init">>)),
    _ = expect(S, non, {response, 2, 4}),
    send(S, Port, request(non, get, 2, [{uri_path, <<"n">>}, {observe, 0}])),
    First = expect(S, non, {response, 2, 5}),
    ?assertEqual(0, coap_codec:option(observe, First)),
    send(S, Port, request(non, put, 3, [{uri_path, <<"n">>}], <<"y">>)),
    Note = expect(S, non, {response, 2, 5}),
    ?assertEqual(1, coap_codec:option(observe, Note)),
    refute(S, non, {response, 2, 5}, 400),
    close(S).

%% Observe=1 deregisters: a later PUT notifies nobody.
observe_deregister_with_observe_1(Config) ->
    Port = ?config(port, Config),
    S = open(),
    send(S, Port, request(con, put, 1, [{uri_path, <<"d1">>}], <<"a">>)),
    _ = expect(S, ack, {response, 2, 4}),
    send(S, Port, request(con, get, 2, [{uri_path, <<"d1">>}, {observe, 0}])),
    _ = expect(S, ack, {response, 2, 5}),
    send(S, Port, request(con, put, 3, [{uri_path, <<"d1">>}], <<"b">>)),
    _ = expect(S, ack, {response, 2, 4}),
    _ = expect(S, con, {response, 2, 5}),
    send(S, Port, request(con, get, 4, [{uri_path, <<"d1">>}, {observe, 1}])),
    %% Deregister processes the GET as usual: the current representation, no
    %% Observe option (RFC 7641 §4.1) -- 2.05 here, since the dest has state.
    ?assertEqual({response, 2, 5},
                 coap_codec:code(expect(S, ack, {response, 2, 5}))),
    send(S, Port, request(con, put, 5, [{uri_path, <<"d1">>}], <<"c">>)),
    _ = expect(S, ack, {response, 2, 4}),
    refute(S, con, {response, 2, 5}, 400),
    close(S).

%% An RST carrying the observation's token ends it (RFC 7641 §3.6).
observe_rst_deregisters(Config) ->
    Port = ?config(port, Config),
    S = open(),
    %% CON notifications retransmit on the ACK-timeout; pin it short so the RST
    %% below lands well before a retransmit (which would arrive as a second
    %% notification the test does not expect).
    ok = application:set_env(broker_proto_coap, observe_ack_timeout_ms, 300),
    Token = <<"rsttok">>,
    send(S, Port, request(con, put, 1, [{uri_path, <<"rst1">>}], <<"a">>)),
    _ = expect(S, ack, {response, 2, 4}),
    send(S, Port, (request(con, get, 2, [{uri_path, <<"rst1">>}, {observe, 0}]))#{token => Token}),
    _ = expect(S, ack, {response, 2, 5}),
    send(S, Port, request(con, put, 3, [{uri_path, <<"rst1">>}], <<"b">>)),
    _ = expect(S, ack, {response, 2, 4}),
    Note = expect(S, con, {response, 2, 5}),
    %% A conformant empty RST echoes the rejected notification's Message ID. It
    %% must arrive before any ACK (an ACK would cancel the retransmit and leave
    %% nothing for the RST to act on); the RST cancels the timer AND ends the
    %% relationship in one step.
    send(S, Port, rst_of(coap_codec:id(Note))),
    send(S, Port, request(con, put, 4, [{uri_path, <<"rst1">>}], <<"c">>)),
    _ = expect(S, ack, {response, 2, 4}),
    refute(S, con, {response, 2, 5}, 400),
    close(S).

%% A full relationship table refuses the observation with 4.03.
observe_bounded_table_refuses(Config) ->
    Port = ?config(port, Config),
    S = open(),
    %% Both destinations need retained state: Observe=0 only registers once the
    %% resource answers 2.05, so the PUTs must carry a (non-empty) payload.
    send(S, Port, request(con, put, 1, [{uri_path, <<"b1">>}], <<"x">>)),
    _ = expect(S, ack, {response, 2, 4}),
    send(S, Port, request(con, put, 2, [{uri_path, <<"b2">>}], <<"y">>)),
    _ = expect(S, ack, {response, 2, 4}),
    %% Distinct tokens => distinct relationships, so the second is a NEW key that
    %% the full table must refuse (a shared empty token would be the same key and
    %% read as a refresh).
    T1 = <<"b1tok">>,
    T2 = <<"b2tok">>,
    send(S, Port, (request(con, get, 3, [{uri_path, <<"b1">>}, {observe, 0}]))
                   #{token => T1}),
    _ = expect(S, ack, {response, 2, 5}),
    send(S, Port, (request(con, get, 4, [{uri_path, <<"b2">>}, {observe, 0}]))
                   #{token => T2}),
    ?assertEqual({response, 4, 3},
                 coap_codec:code(expect(S, ack, {response, 4, 3}))),
    %% The refused registration did not disturb the first observation.
    send(S, Port, request(con, put, 5, [{uri_path, <<"b1">>}], <<"z">>)),
    _ = expect(S, ack, {response, 2, 4}),
    ?assertEqual(T1, coap_codec:token(expect(S, con, {response, 2, 5}))),
    close(S).

%% ADR-031 §D5: plain UDP carries no credential, so the CoAP principal is
%% `undefined' and `broker_security:authorize/2' allows it outright -- an `acl'
%% rule set and `require_auth' cannot refuse a *datagram* until S11-C's DTLS
%% client identity exists (the front-end even refuses to boot with
%% `require_auth' listing coap: see `coap_app_SUITE'). The refusal this stage
%% can produce is capacity, not authorization (`observe_bounded_table_refuses').
%%
%% Pinned here so the posture is a decision and not an accident: an
%% ACL-restricted deployment still observes (the principal is `undefined', not
%% refused), and the relationship is live (a later PUT notifies).
observe_plain_udp_principal_is_allowed(Config) ->
    Port = ?config(port, Config),
    S = open(),
    ok = application:set_env(broker_security, acl, all),
    ok = application:set_env(broker_security, require_auth, [<<"coap">>]),
    send(S, Port, request(con, put, 1, [{uri_path, <<"x">>}], <<"v">>)),
    _ = expect(S, ack, {response, 2, 4}),
    send(S, Port, request(con, get, 2, [{uri_path, <<"x">>}, {observe, 0}])),
    First = expect(S, ack, {response, 2, 5}),
    ?assertEqual(0, coap_codec:option(observe, First)),
    send(S, Port, request(con, put, 3, [{uri_path, <<"x">>}], <<"w">>)),
    _ = expect(S, ack, {response, 2, 4}),
    ?assertEqual(1, coap_codec:option(observe, expect(S, con, {response, 2, 5}))),
    close(S).

%% A relationship silent longer than Max-Age is swept on the tick: a later PUT
%% notifies nobody (the client is expected to re-Observe).
observe_max_age_expires_relationship(Config) ->
    Port = ?config(port, Config),
    S = open(),
    send(S, Port, request(con, put, 1, [{uri_path, <<"mx">>}], <<"init">>)),
    _ = expect(S, ack, {response, 2, 4}),
    send(S, Port, request(con, get, 2, [{uri_path, <<"mx">>}, {observe, 0}])),
    _ = expect(S, ack, {response, 2, 5}),
    send(S, Port, request(con, put, 3, [{uri_path, <<"mx">>}], <<"a">>)),
    _ = expect(S, ack, {response, 2, 4}),
    _ = expect(S, con, {response, 2, 5}),
    %% The sweep runs on the 1 s refill tick, whose phase is set when the
    %% listener boots -- not when this relationship was last notified. With a
    %% 1 s Max-Age window the tick at +1 s can land a few milliseconds short of
    %% the window (it fires at boot+N s, the notification happened just after
    %% boot), so the sweep that actually matches is the one at +2 s. Sleep past
    %% that second tick rather than trusting the first. Sleep past it with margin
    %% so a sub-300 ms scheduling stall of the listener cannot flake the case.
    timer:sleep(3200),
    send(S, Port, request(con, put, 4, [{uri_path, <<"mx">>}], <<"b">>)),
    _ = expect(S, ack, {response, 2, 4}),
    refute(S, con, {response, 2, 5}, 400),
    close(S).

%% Two observers on the SAME destination (distinct tokens, one socket) share one
%% exclusive kernel subscription. Deregistering one must NOT starve the other:
%% the shared filter survives while any relationship on the destination is live
%% (the `maybe_unsubscribe/2' guard -- regression for the per-relationship
%% unsubscribe that would otherwise tear the subscription out from under the
%% second observer). NON observations keep this timer-free and deterministic.
observe_two_observers_same_dest_both_notified(Config) ->
    Port = ?config(port, Config),
    S = open(),
    send(S, Port, request(non, put, 1, [{uri_path, <<"two">>}], <<"init">>)),
    _ = expect(S, non, {response, 2, 4}),
    T1 = <<"ta">>,
    T2 = <<"tb">>,
    send(S, Port, (request(non, get, 2, [{uri_path, <<"two">>}, {observe, 0}]))
                   #{token => T1}),
    First = expect(S, non, {response, 2, 5}),
    ?assertEqual(T1, coap_codec:token(First)),
    ?assertEqual(0, coap_codec:option(observe, First)),
    send(S, Port, (request(non, get, 3, [{uri_path, <<"two">>}, {observe, 0}]))
                   #{token => T2}),
    Second = expect(S, non, {response, 2, 5}),
    ?assertEqual(T2, coap_codec:token(Second)),
    %% One publish fans out to BOTH observers.
    send(S, Port, request(non, put, 4, [{uri_path, <<"two">>}], <<"v1">>)),
    N1 = expect(S, non, {response, 2, 5}),
    ?assertEqual(T1, coap_codec:token(N1)),
    N2 = expect(S, non, {response, 2, 5}),
    ?assertEqual(T2, coap_codec:token(N2)),
    %% T1 deregisters (Observe=1): T2 must keep receiving.
    send(S, Port, (request(non, get, 5, [{uri_path, <<"two">>}, {observe, 1}]))
                   #{token => T1}),
    _ = expect(S, non, {response, 2, 5}),
    send(S, Port, request(non, put, 6, [{uri_path, <<"two">>}], <<"v2">>)),
    N3 = expect(S, non, {response, 2, 5}),
    ?assertEqual(T2, coap_codec:token(N3)),
    ?assertEqual(2, coap_codec:option(observe, N3)),
    close(S).
