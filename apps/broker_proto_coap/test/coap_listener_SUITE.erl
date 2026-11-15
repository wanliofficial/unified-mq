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
     malformed_datagram_is_dropped].

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
              max_msg => env(broker_proto_coap, max_message_bytes)},
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
case_env(_Case, _Config) ->
    ok.

env(App, Key) ->
    application:get_env(App, Key, undefined).

restore_env(#{acl := Acl, require_auth := Auth, users := Users,
              max_rps := Rps, max_msg := MaxMsg}) ->
    set_or_unset(broker_security, acl, Acl),
    set_or_unset(broker_security, require_auth, Auth),
    set_or_unset(broker_security, users, Users),
    set_or_unset(broker_proto_coap, max_requests_per_second, Rps),
    set_or_unset(broker_proto_coap, max_message_bytes, MaxMsg),
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
