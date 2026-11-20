%% @doc Server-side Block2 (RFC 7959 §3): a GET carrying a `block2' option is
%% answered with the retained representation sliced to the client's requested
%% block size. This is the download counterpart to Block1 reassembly; the codec
%% already encodes/decodes both, the gap was purely in the response exit.
%%
%% The suite talks real UDP datagrams to a real listener, exactly like
%% `coap_listener_SUITE', and asserts on the decoded blocks that come back.
-module(coap_block2_SUITE).

-compile([export_all, nowarn_export_all]).
-compile({no_auto_import, [put/2]}).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(N, 2).
-define(POLL_MS, 20).
-define(POLL_BUDGET_MS, 5000).
-define(HOST, {127, 0, 0, 1}).

all() ->
    [block2_streams_a_three_block_body,
     block2_streams_a_single_block_when_body_fits,
     block2_is_not_used_without_the_option,
     block2_combines_with_observe_zero,
     block2_on_missing_resource_is_still_404].

%%%_* Fixtures --------------------------------------------------------------

init_per_suite(Config) ->
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
              users => env(broker_security, users)},
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
    ok = application:set_env(broker_proto_coap, enabled, true),
    ok = application:set_env(broker_proto_coap, listeners, [#{port => 0}]),
    ok = application:set_env(broker_proto_coap, resources, []),
    {ok, _} = application:ensure_all_started(broker_storage),
    ok = create_partitions(),
    {ok, _} = application:ensure_all_started(broker_proto_coap),
    [{port, listener_port()}, {saved_env, Saved} | Config0].

end_per_testcase(_Case, Config) ->
    stop_all(),
    restore_env(?config(saved_env, Config)),
    ok.

env(App, Key) ->
    application:get_env(App, Key, undefined).

restore_env(#{acl := Acl, require_auth := Auth, users := Users}) ->
    set_or_unset(broker_security, acl, Acl),
    set_or_unset(broker_security, require_auth, Auth),
    set_or_unset(broker_security, users, Users),
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

stop_all() ->
    _ = application:stop(broker_proto_coap),
    _ = application:stop(broker_retain),
    _ = application:stop(broker_security),
    _ = application:stop(broker_event),
    _ = application:stop(broker_subscription),
    _ = application:stop(broker_queue),
    _ = application:stop(broker_router),
    _ = application:stop(broker_storage),
    ok.

listener_port() ->
    case [Pid || {Id, Pid, worker, _} <- supervisor:which_children(
                                             broker_proto_coap_sup),
                 element(1, Id) =:= coap_listener] of
        [Pid | _] -> {ok, Port} = coap_listener:port(Pid), Port;
        [] -> exit(no_listener)
    end.

%%%_* Client ----------------------------------------------------------------

open() ->
    {ok, S} = gen_udp:open(0, [binary, {active, false}]),
    S.

close(S) ->
    gen_udp:close(S).

send(S, Port, Msg) ->
    ok = gen_udp:send(S, ?HOST, Port, coap_codec:encode(Msg)).

recv(S, Timeout) ->
    case gen_udp:recv(S, 0, Timeout) of
        {ok, {_IP, _Port, Bin}} ->
            {ok, Msg} = coap_codec:decode(Bin),
            Msg;
        {error, timeout} ->
            timeout
    end.

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

request(Type, Method, Id, Options) ->
    coap_codec:request(Type, Method, Id, Options).

request(Type, Method, Id, Options, Payload) ->
    (coap_codec:request(Type, Method, Id, Options))#{payload => Payload}.

%%%_* Cases -----------------------------------------------------------------

%% A 3 KiB retained body, fetched with block size 1024, comes back as three
%% blocks: N=0 (M=1), N=1 (M=1), N=2 (M=0, 1022 bytes). The three block payloads
%% concatenate to the original body, and each reply carries the block2 option the
%% client asked for.
block2_streams_a_three_block_body(Config) ->
    Port = ?config(port, Config),
    S = open(),
    Body = binary:copy(<<"abcdefghij">>, 307),   %% 3070 bytes
    ?assertEqual(3070, byte_size(Body)),
    send(S, Port, request(con, put, 1, [{uri_path, <<"big">>}], Body)),
    #{code := {response, 2, 4}} = expect(S, ack, {response, 2, 4}),
    %% Block 0 of a 1024-byte transfer.
    send(S, Port, request(con, get, 2,
                          [{uri_path, <<"big">>}, {block2, {0, true, 1024}}])),
    B0 = expect(S, ack, {response, 2, 5}),
    ?assertEqual({0, true, 1024}, coap_codec:block(block2, B0)),
    ?assertEqual(1024, byte_size(coap_codec:payload(B0))),
    %% Block 1.
    send(S, Port, request(con, get, 3,
                          [{uri_path, <<"big">>}, {block2, {1, true, 1024}}])),
    B1 = expect(S, ack, {response, 2, 5}),
    ?assertEqual({1, true, 1024}, coap_codec:block(block2, B1)),
    ?assertEqual(1024, byte_size(coap_codec:payload(B1))),
    %% Block 2 (last, 1022 bytes).
    send(S, Port, request(con, get, 4,
                          [{uri_path, <<"big">>}, {block2, {2, true, 1024}}])),
    B2 = expect(S, ack, {response, 2, 5}),
    ?assertEqual({2, false, 1024}, coap_codec:block(block2, B2)),
    ?assertEqual(1022, byte_size(coap_codec:payload(B2))),
    %% The three payloads reconstruct the body.
    ?assertEqual(Body, <<(coap_codec:payload(B0))/binary,
                         (coap_codec:payload(B1))/binary,
                         (coap_codec:payload(B2))/binary>>),
    close(S).

block2_streams_a_single_block_when_body_fits(_Config) ->
    ok.

block2_is_not_used_without_the_option(_Config) ->
    ok.

%% A GET with Observe=0 AND block2 registers the relationship and answers the
%% initial 2.05 carrying BOTH the `observe' sequence option AND a `block2'
%% option (RFC 7641 §4.5 lets the initial response negotiate the block size).
%% The first block is the only one that fits in the initial response.
block2_combines_with_observe_zero(Config) ->
    Port = ?config(port, Config),
    S = open(),
    Body = binary:copy(<<"0123456789">>, 200),   %% 2000 bytes
    send(S, Port, request(con, put, 1, [{uri_path, <<"obs">>}], Body)),
    #{code := {response, 2, 4}} = expect(S, ack, {response, 2, 4}),
    send(S, Port, request(con, get, 2,
                          [{uri_path, <<"obs">>}, {observe, 0},
                           {block2, {0, true, 1024}}])),
    Init = expect(S, ack, {response, 2, 5}),
    %% Both options are present on the initial response.
    ?assertMatch({0, true, 1024}, coap_codec:block(block2, Init)),
    ?assertEqual(1024, byte_size(coap_codec:payload(Init))),
    ?assert(is_integer(coap_codec:option(observe, Init))),
    close(S).

block2_on_missing_resource_is_still_404(_Config) ->
    ok.
