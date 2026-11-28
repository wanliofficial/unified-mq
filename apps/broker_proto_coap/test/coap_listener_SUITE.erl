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
     non_policy_rejection_returns_rst,
     block1_reassembles_a_multidatagram_body,
     block1_keyed_by_request_tag_across_changing_tokens,
     token_bucket_bounds_a_flood,
     oversized_datagram_is_dropped,
     malformed_datagram_is_dropped,
     observe_ack_and_rst_are_not_requests,
     observe_con_notifies_and_acks,
     observe_con_retries_then_gives_up,
     observe_give_up_of_one_keeps_the_other,
     observe_non_is_at_most_once,
     observe_deregister_with_observe_1,
     observe_rst_deregisters,
     observe_bounded_table_refuses,
     observe_stray_rst_does_not_crash,
     observe_rst_is_endpoint_scoped,
     observe_plain_udp_principal_is_allowed,
     observe_max_age_expires_relationship,
     observe_max_age_renews_on_notification,
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
              max_age => env(broker_proto_coap, observe_max_age_s),
              policy => env(broker_event, policy),
              policies => env(broker_event, policies),
              quarantine => env(broker_event, quarantine),
              trace_context => env(broker_event, trace_context)},
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
    ok = application:set_env(broker_event, policy, pass_through),
    ok = application:set_env(broker_event, policies, []),
    ok = application:set_env(broker_event, quarantine, []),
    ok = application:set_env(broker_event, trace_context, off),
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
case_env(observe_give_up_of_one_keeps_the_other, _Config) ->
    %% Same short budget as the single-observer give-up case: A must exhaust its
    %% retransmit budget within the window, but the assertion is about B.
    ok = application:set_env(broker_proto_coap, observe_ack_timeout_ms, 300),
    ok = application:set_env(broker_proto_coap, observe_max_retrans, 2);
case_env(observe_con_notifies_and_acks, _Config) ->
    %% Pin a short ACK timeout so the post-ACK silence window can actually catch a
    %% broken retransmit-cancel (the default 2000 ms would mask it).
    ok = application:set_env(broker_proto_coap, observe_ack_timeout_ms, 300);
%% The RST case pins a short ACK timeout so the RST reliably lands before a
%% retransmit (set_env here runs before the listener starts, so it takes effect).
case_env(observe_rst_deregisters, _Config) ->
    ok = application:set_env(broker_proto_coap, observe_ack_timeout_ms, 300);
%% The bounded-table case shrinks the relationship table to a single slot.
case_env(observe_bounded_table_refuses, _Config) ->
    ok = application:set_env(broker_proto_coap, max_observes, 1);
%% The max-age case sweeps after one second instead of thirty.
case_env(observe_max_age_expires_relationship, _Config) ->
    ok = application:set_env(broker_proto_coap, observe_max_age_s, 1);
case_env(observe_max_age_renews_on_notification, _Config) ->
    ok = application:set_env(broker_proto_coap, observe_max_age_s, 3);
case_env(_Case, _Config) ->
    ok.

env(App, Key) ->
    application:get_env(App, Key, undefined).

restore_env(#{acl := Acl, require_auth := Auth, users := Users,
              max_rps := Rps, max_msg := MaxMsg,
              max_observes := MaxObs, ack_timeout := AckTO,
              max_retrans := MaxRetrans, max_age := MaxAge,
              policy := Policy, policies := Policies, quarantine := Quarantine,
              trace_context := Trace}) ->
    set_or_unset(broker_security, acl, Acl),
    set_or_unset(broker_security, require_auth, Auth),
    set_or_unset(broker_security, users, Users),
    set_or_unset(broker_proto_coap, max_requests_per_second, Rps),
    set_or_unset(broker_proto_coap, max_message_bytes, MaxMsg),
    set_or_unset(broker_proto_coap, max_observes, MaxObs),
    set_or_unset(broker_proto_coap, observe_ack_timeout_ms, AckTO),
    set_or_unset(broker_proto_coap, observe_max_retrans, MaxRetrans),
    set_or_unset(broker_proto_coap, observe_max_age_s, MaxAge),
    set_or_unset(broker_event, policy, Policy),
    set_or_unset(broker_event, policies, Policies),
    set_or_unset(broker_event, quarantine, Quarantine),
    set_or_unset(broker_event, trace_context, Trace),
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

non_policy_rejection_returns_rst(Config) ->
    Port = ?config(port, Config),
    ok = application:set_env(broker_event, policy, required),
    S = open(),
    send(S, Port, request(non, put, 3, [{uri_path, <<"reject">>},
                                       {uri_path, <<"wire">>}], <<"ordinary">>)),
    #{type := rst, code := {response, 5, 0}, id := 3} =
        expect(S, rst, {response, 5, 0}),
    ?assertEqual(error, broker_retain:lookup(<<"reject/wire">>)),
    close(S).

%% RFC 7959 draft §2.1 (ADR-031 §D7): a body larger than one datagram arrives
%% as a sequence of Block1 messages. The first (M=1) is answered with a 2.31
%% Continue echoing the next block number; the last (M=0) completes the stream
%% and is dispatched to the resource layer.
%%
%% The blocks carry the SAME token but DIFFERENT Message IDs, as a real client
%% sends them: a transfer is one request, so reassembly must not be keyed by the
%% per-datagram Message ID.
%%
%% The 2.31 Continue echoes the block number it RECEIVED (RFC 7959 §3.2
%% Figure 7: request `1:0/1/128' is answered `1:0/1/128'), not the next one.
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
    %% The listener answers 2.31 Continue acknowledging block 0.
    Continue = expect(S, ack, {response, 2, 31}),
    ?assertEqual(Token, coap_codec:token(Continue)),
    ?assertEqual({0, true, 16}, coap_codec:block(block1, Continue)),
    %% Send the last block with a new Message ID and the same token.
    Req2 = (request(con, put, 2, [{uri_path, <<"big">>}, {block1, Block1Last}],
                    Chunk2))#{token => Token},
    send(S, Port, Req2),
    #{type := ack, code := {response, 2, 4}} = expect(S, ack, {response, 2, 4}),
    %% The assembled body is what was retained: a GET returns both chunks.
    send(S, Port, request(con, get, 3, [{uri_path, <<"big">>}])),
    #{payload := <<1, 2, 3, 4, 5, 6>>} = expect(S, ack, {response, 2, 5}),
    close(S).

%% A real client is free to change the token between blocks -- libcoap v4.3.5
%% sends block 0 with an empty token and block 1 with an eight-byte one, and ties
%% the transfer together with `Request-Tag' (RFC 9175 §3.1, option 292). Keying
%% reassembly on the token alone silently dropped every such body: the PUT was
%% answered 2.04 while nothing was retained. This case is that regression.
block1_keyed_by_request_tag_across_changing_tokens(Config) ->
    Port = ?config(port, Config),
    S = open(),
    Chunk1 = <<"first-half">>,
    Chunk2 = <<"second-half">>,
    Tag = <<"reqtag01">>,
    %% Distinct tokens per block, exactly as libcoap emits them.
    Req1 = (request(con, put, 11,
                    [{uri_path, <<"tagged">>}, {block1, {0, true, 16}},
                     {292, Tag}], Chunk1))#{token => <<>>},
    send(S, Port, Req1),
    Continue = expect(S, ack, {response, 2, 31}),
    ?assertEqual({0, true, 16}, coap_codec:block(block1, Continue)),
    Req2 = (request(con, put, 12,
                    [{uri_path, <<"tagged">>}, {block1, {1, false, 16}},
                     {292, Tag}], Chunk2))#{token => <<2, 0, 0, 0, 0, 0, 2>>},
    send(S, Port, Req2),
    #{type := ack, code := {response, 2, 4}} = expect(S, ack, {response, 2, 4}),
    send(S, Port, request(con, get, 13, [{uri_path, <<"tagged">>}])),
    #{payload := <<"first-halfsecond-half">>} = expect(S, ack, {response, 2, 5}),
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

%% Two CON observers on the SAME destination. One (A) never ACKs and exhausts its
%% retransmit budget; the other (B) ACKs every notification and stays healthy.
%% A's give-up must NOT disturb B: every CoAP observation of a destination shares
%% one exclusive subscription, so tearing down A's relationship must not
%% unsubscribe the filter or detach the shared session while B still needs it --
%% otherwise B's stream silently starves. The retransmit/pending maps are keyed by
%% {IP,Port,Id}, so A's budget must not be spent by B's traffic and vice versa.
%% (The single-observer give-up case proves A is torn down; this proves B survives
%% it. Neither cancels a shared resource the other still needs.)
observe_give_up_of_one_keeps_the_other(Config) ->
    Port = ?config(port, Config),
    A = open(),
    B = open(),
    TA = <<"isoa">>,
    TB = <<"isob">>,
    %% Both observers register a CON observation on the same destination.
    send(A, Port, request(con, put, 1, [{uri_path, <<"iso">>}], <<"init">>)),
    _ = expect(A, ack, {response, 2, 4}),
    send(A, Port, (request(con, get, 2, [{uri_path, <<"iso">>}, {observe, 0}]))
                   #{token => TA}),
    _ = expect(A, ack, {response, 2, 5}),
    send(B, Port, request(con, put, 3, [{uri_path, <<"iso">>}], <<"init">>)),
    _ = expect(B, ack, {response, 2, 4}),
    send(B, Port, (request(con, get, 4, [{uri_path, <<"iso">>}, {observe, 0}]))
                   #{token => TB}),
    _ = expect(B, ack, {response, 2, 5}),
    %% First publish: both are notified. A ignores it; B ACKs and stays healthy.
    send(A, Port, request(con, put, 5, [{uri_path, <<"iso">>}], <<"v1">>)),
    _ = expect(A, ack, {response, 2, 4}),
    _NA1 = expect(A, con, {response, 2, 5}),
    send(B, Port, request(con, put, 6, [{uri_path, <<"iso">>}], <<"v1">>)),
    _ = expect(B, ack, {response, 2, 4}),
    NB1 = expect(B, con, {response, 2, 5}),
    ?assertEqual(TB, coap_codec:token(NB1)),
    send(B, Port, ack_of(NB1)),
    %% Second publish: same split. A still ignores; B ACKs again.
    send(A, Port, request(con, put, 7, [{uri_path, <<"iso">>}], <<"v2">>)),
    _ = expect(A, ack, {response, 2, 4}),
    _NA2 = expect(A, con, {response, 2, 5}),
    send(B, Port, request(con, put, 8, [{uri_path, <<"iso">>}], <<"v2">>)),
    _ = expect(B, ack, {response, 2, 4}),
    NB2 = expect(B, con, {response, 2, 5}),
    send(B, Port, ack_of(NB2)),
    %% A has now missed its notifications. Drain its own retransmits (the un-acked
    %% v2 note repeats at 300ms and 600ms) for longer than the give-up budget
    %% (~900ms) so the relationship is actually torn down before the decisive
    %% assertion. A is the only one ignoring, so every con on A's socket is its own
    %% retransmit -- discarding them is exactly "let A give up".
    drain_con(A, 1200),
    %% The decisive assertion: A is gone, but B -- on the SAME shared
    %% subscription -- still receives a fresh notification.
    send(B, Port, request(con, put, 9, [{uri_path, <<"iso">>}], <<"v3">>)),
    _ = expect(B, ack, {response, 2, 4}),
    NB3 = expect(B, con, {response, 2, 5}),
    ?assertEqual(TB, coap_codec:token(NB3)),
    %% A later observe sequence than NB2 proves this is a fresh notification,
    %% generated after A's relationship was already torn down.
    ?assert(coap_codec:option(observe, NB3) > coap_codec:option(observe, NB2)),
    send(B, Port, ack_of(NB3)),
    close(A),
    close(B).

%% Consume any CON notification frames (A's own un-acked retransmits) for
%% `WindowMs', so a misbehaving peer's budget can fully exhaust. Unlike `refute',
%% this does not fail when a frame arrives -- it expects them.
drain_con(S, WindowMs) ->
    Deadline = erlang:monotonic_time(millisecond) + WindowMs,
    drain_con_loop(S, Deadline).

drain_con_loop(S, Deadline) ->
    Left = Deadline - erlang:monotonic_time(millisecond),
    case Left > 0 of
        false -> ok;
        true ->
            case recv(S, Left) of
                timeout -> ok;
                #{type := con, code := {response, 2, 5}} -> drain_con_loop(S, Deadline);
                _Other -> drain_con_loop(S, Deadline)
            end
    end.

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

%% An empty RST (RFC 7641 §4.5) echoing a notification's Message ID ends the
%% relationship -- the RST carries no token, so it is correlated by Message ID,
%% not token.
observe_rst_deregisters(Config) ->
    Port = ?config(port, Config),
    S = open(),
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

%% A stray empty RST (unknown Message ID) from a client that has a live, but
%% never-yet-notified, relationship must NOT crash the listener: the fold over
%% `observes' uses maps:get(req_id,...,undefined), so a relationship without a
%% req_id is simply skipped. The relationship survives and still notifies.
%% (Regression for a function_clause that took the whole CoAP app down.)
observe_stray_rst_does_not_crash(Config) ->
    Port = ?config(port, Config),
    S = open(),
    send(S, Port, request(non, put, 1, [{uri_path, <<"stray">>}], <<"init">>)),
    _ = expect(S, non, {response, 2, 4}),
    send(S, Port, (request(non, get, 2, [{uri_path, <<"stray">>}, {observe, 0}]))
                   #{token => <<"stok">>}),
    _ = expect(S, non, {response, 2, 5}),
    %% An RST echoing an id no notification ever used.
    send(S, Port, rst_of(60000)),
    %% The relationship is untouched: a later publish still notifies.
    send(S, Port, request(non, put, 3, [{uri_path, <<"stray">>}, {observe, 0}])),
    Note = expect(S, non, {response, 2, 5}),
    ?assertEqual(<<"stok">>, coap_codec:token(Note)),
    close(S).

%% A RST is only honoured for the endpoint that sent it: a RST from client A
%% echoing a Message ID that belongs to client B's notification must NOT end B's
%% relationship (RFC 7641 §4.5 correlates an empty RST by Message ID, but only on
%% the sender's endpoint -- the pending/observes scan is scoped to {IP,Port,_}).
%% Regression for a cross-endpoint kill: Message IDs are a 16-bit space, so any
%% client could otherwise guess one and tear down another's observation.
observe_rst_is_endpoint_scoped(Config) ->
    Port = ?config(port, Config),
    A = open(),
    B = open(),
    send(A, Port, request(non, put, 1, [{uri_path, <<"epA">>}], <<"a">>)),
    _ = expect(A, non, {response, 2, 4}),
    send(B, Port, request(non, put, 2, [{uri_path, <<"epB">>}], <<"b">>)),
    _ = expect(B, non, {response, 2, 4}),
    send(A, Port, (request(non, get, 3, [{uri_path, <<"epA">>}, {observe, 0}]))
                  #{token => <<"tA">>}),
    _ = expect(A, non, {response, 2, 5}),
    send(B, Port, (request(non, get, 4, [{uri_path, <<"epB">>}, {observe, 0}]))
                  #{token => <<"tB">>}),
    _ = expect(B, non, {response, 2, 5}),
    %% Drive B's relationship to a notification so it has a req_id to target.
    send(A, Port, request(non, put, 5, [{uri_path, <<"epB">>}], <<"b1">>)),
    _ = expect(A, non, {response, 2, 4}),
    BNote = expect(B, non, {response, 2, 5}),
    ?assertEqual(<<"tB">>, coap_codec:token(BNote)),
    %% A sends a RST echoing B's notification id -- from A's endpoint. B must
    %% survive; if the scan were not endpoint-scoped this would kill B.
    send(A, Port, rst_of(coap_codec:id(BNote))),
    send(A, Port, request(non, put, 6, [{uri_path, <<"epB">>}], <<"b2">>)),
    _ = expect(A, non, {response, 2, 4}),
    BNote2 = expect(B, non, {response, 2, 5}),
    ?assertEqual(<<"tB">>, coap_codec:token(BNote2)),
    close(A),
    close(B).

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

%% The Max-Age window is "time since the LAST notification", not since
%% registration: next_seq/3 stamps last_notified on every notification, so each
%% one renews the window. The expiry case above proves a silent relationship
%% dies; this proves an active one survives. With a 3 s window: notify at ~0 s
%% (stamps last_notified), wait 2 s, notify again at ~2 s (renews to expire at
%% ~5 s), wait 2 s more (~4 s total, only ~2 s since the last notification --
%% inside the window). A PUT then must still reach the observer. Without the
%% renewal (last_notified frozen at registration), ~4 s would already exceed
%% the 3 s window and the relationship would be gone.
observe_max_age_renews_on_notification(Config) ->
    Port = ?config(port, Config),
    S = open(),
    send(S, Port, request(con, put, 1, [{uri_path, <<"mr">>}], <<"init">>)),
    _ = expect(S, ack, {response, 2, 4}),
    send(S, Port, request(con, get, 2, [{uri_path, <<"mr">>}, {observe, 0}])),
    _ = expect(S, ack, {response, 2, 5}),
    %% First notification stamps last_notified ~0.
    send(S, Port, request(con, put, 3, [{uri_path, <<"mr">>}], <<"v1">>)),
    _ = expect(S, ack, {response, 2, 4}),
    _ = expect(S, con, {response, 2, 5}),
    %% Halfway into the 3 s window: a second notification renews last_notified.
    timer:sleep(2000),
    send(S, Port, request(con, put, 4, [{uri_path, <<"mr">>}], <<"v2">>)),
    _ = expect(S, ack, {response, 2, 4}),
    _ = expect(S, con, {response, 2, 5}),
    %% ~2 s since the renewal (well inside 3 s) but ~4 s since registration --
    %% past the window if last_notified had not been refreshed. The relationship
    %% must still be live.
    timer:sleep(2000),
    send(S, Port, request(con, put, 5, [{uri_path, <<"mr">>}], <<"v3">>)),
    _ = expect(S, ack, {response, 2, 4}),
    N3 = expect(S, con, {response, 2, 5}),
    ?assertEqual(<<"v3">>, coap_codec:payload(N3)),
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
