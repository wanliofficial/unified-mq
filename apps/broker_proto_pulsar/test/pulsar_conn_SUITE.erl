%% @doc S5 Pulsar adapter over real sockets: connect, partitioned-metadata,
%% lookup, producer, send (single + batch), subscribe/flow, ack, redelivery,
%% seek, ping and the last-message-id query.
%%
%% The suite's client is byte-level: it speaks the documented Pulsar subset
%% directly. The Gate S5 evidence for a third-party client comes from
%% `test/protocol-compatibility/pulsar/' (official Apache Pulsar Java client).
-module(pulsar_conn_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("broker_core/include/broker_message.hrl").

-define(N, 1).
-define(REF, {pulsar, 0}).
-define(PARTITION, 0).

-record(c, {s :: gen_tcp:socket(), port :: inet:port_number()}).

all() ->
    [connect_returns_connected,
     partitioned_metadata_zero,
     lookup_returns_broker_url,
     producer_success,
     send_single_then_receipt,
     send_batch_appends_all,
     subscribe_flow_delivers_in_order,
     consume_then_ack,
     ack_response_echoes_request_id,
     redelivery_after_consumer_close,
     seek_replays_from_entry,
     ping_returns_pong,
     get_last_message_id,
     close_producer_success,
     unsubscribe_success,
     send_without_producer_errors].

suite() -> [{timetrap, {seconds, 90}}].

init_per_suite(Config) ->
    _ = application:load(broker_storage),
    _ = application:load(broker_router),
    _ = application:load(broker_queue),
    _ = application:load(broker_subscription),
    _ = application:load(broker_event),
    _ = application:load(broker_security),
    _ = application:load(broker_retain),
    _ = application:load(broker_proto_pulsar),
    Config.

end_per_suite(_Config) ->
    stop_all(),
    ok.

init_per_testcase(Case, Config) ->
    stop_all(),
    DataDir = filename:join([?config(priv_dir, Config), atom_to_list(Case), "data"]),
    ok = filelib:ensure_dir(filename:join([DataDir, "x"])),
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok = application:set_env(broker_storage, data_dir, DataDir),
    ok = application:set_env(broker_storage, wal, #{require_durable_fs => false}),
    ok = application:set_env(broker_queue, partitions, ?N),
    ok = application:set_env(broker_router, partitions, ?N),
    ok = application:set_env(broker_subscription, data_dir, DataDir),
    ok = application:set_env(broker_retain, data_dir, DataDir),
    ok = application:set_env(broker_event, policy, pass_through),
    ok = application:set_env(broker_security, users, []),
    ok = application:set_env(broker_security, acl, allow),
    ok = application:set_env(broker_proto_pulsar, advertised_host, <<"127.0.0.1">>),
    ok = application:set_env(broker_proto_pulsar, listeners,
                             [#{port => 0, ref => ?REF,
                                num_acceptors => 4, max_connections => 128}]),
    {ok, _} = application:ensure_all_started(broker_storage),
    ok = create_partitions(),
    {ok, _} = application:ensure_all_started(broker_proto_pulsar),
    [{port, listener_port()} | Config].

end_per_testcase(_Case, _Config) ->
    stop_all(),
    ok.

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
    case ranch:get_port(?REF) of
        Port when is_integer(Port) -> Port;
        Other -> exit({listener_port_unavailable, Other})
    end.

stop_all() ->
    _ = application:stop(broker_proto_pulsar),
    _ = application:stop(broker_retain),
    _ = application:stop(broker_event),
    _ = application:stop(broker_security),
    _ = application:stop(broker_subscription),
    _ = application:stop(broker_queue),
    _ = application:stop(broker_router),
    _ = application:stop(broker_storage),
    ok.

%%%_* Cases -----------------------------------------------------------------

connect_returns_connected(Config) ->
    C = connect(Config),
    send_simple(C, connect_fields()),
    #{type := connected, server_version := Version,
      protocol_version := Proto, max_message_size := Max} = recv(C),
    ?assertEqual(15, Proto),
    ?assert(Max > 0),
    ?assertNotEqual(<<>>, Version),
    ok.

partitioned_metadata_zero(Config) ->
    C = connect(Config),
    connect_ok(C),
    send_simple(C, command(21, 21, [str(1, <<"persistent://t0/ns/t">>),
                                    varint(2, 5)])),
    #{type := partitioned_metadata_response, partitions := Partitions,
      request_id := Req, error := 0} = recv(C),
    ?assertEqual(0, Partitions),
    ?assertEqual(5, Req),
    ok.

lookup_returns_broker_url(Config) ->
    C = connect(Config),
    connect_ok(C),
    send_simple(C, command(23, 23, [str(1, <<"persistent://t0/ns/t">>),
                                    varint(2, 6), varint(3, 0)])),
    #{type := lookup_response, broker_url := Url, request_id := 6,
      authoritative := true} = recv(C),
    Expected = iolist_to_binary([<<"pulsar://127.0.0.1:">>,
                                 integer_to_binary(?config(port, Config))]),
    ?assertEqual(Expected, Url),
    ok.

producer_success(Config) ->
    C = connect(Config),
    connect_ok(C),
    #{type := producer_success, request_id := 1, producer_name := Name} =
        producer(C, <<"persistent://t0/ns/t">>, 1, 1, <<"my-producer">>),
    ?assertEqual(<<"my-producer">>, Name),
    ok.

send_single_then_receipt(Config) ->
    C = connect(Config),
    connect_ok(C),
    producer(C, <<"t">>, 1, 1, <<"p">>),
    #{type := send_receipt, producer_id := 1, sequence_id := 0,
      message_id := EntryId} = send_one(C, 1, 0, <<"hello">>),
    ?assertEqual(0, EntryId),
    [Msg] = read_topic(<<"t">>),
    ?assertEqual(<<"hello">>, Msg#broker_message.payload),
    ok.

send_batch_appends_all(Config) ->
    C = connect(Config),
    connect_ok(C),
    producer(C, <<"t">>, 1, 1, <<"p">>),
    Batch = [single(<<"a">>, 0), single(<<"b">>, 1), single(<<"c">>, 2)],
    #{type := send_receipt, message_id := EntryId} =
        send_batch(C, 1, 0, 3, Batch),
    ?assertEqual(0, EntryId),
    Payloads = [M#broker_message.payload || M <- read_topic(<<"t">>)],
    ?assertEqual([<<"a">>, <<"b">>, <<"c">>], Payloads),
    ok.

subscribe_flow_delivers_in_order(Config) ->
    C = connect(Config),
    connect_ok(C),
    producer(C, <<"t">>, 1, 1, <<"p">>),
    send_one(C, 1, 0, <<"m0">>),
    send_one(C, 1, 1, <<"m1">>),
    subscribe(C, <<"t">>, <<"s">>, 10, 1, earliest),
    flow(C, 10, 10),
    {Id0, P0} = recv_message(C),
    {Id1, P1} = recv_message(C),
    ?assertEqual({0, <<"m0">>}, {Id0, P0}),
    ?assertEqual({1, <<"m1">>}, {Id1, P1}),
    ok.

consume_then_ack(Config) ->
    C = connect(Config),
    connect_ok(C),
    producer(C, <<"t">>, 1, 1, <<"p">>),
    send_one(C, 1, 0, <<"only">>),
    subscribe(C, <<"t">>, <<"s">>, 10, 1, earliest),
    flow(C, 10, 10),
    {EntryId, _} = recv_message(C),
    send_simple(C, command(10, 10, [varint(1, 10), varint(2, 0),
                                    msg(3, message_id(EntryId))])),
    %% The ACK had request_id 0 so there is no reply; poll the registry until
    %% the entry is gone from the unacked set.
    ?assertEqual(ok, wait_unacked(<<"t">>, <<"s">>)),
    ok.

ack_response_echoes_request_id(Config) ->
    C = connect(Config),
    connect_ok(C),
    producer(C, <<"t">>, 1, 1, <<"p">>),
    send_one(C, 1, 0, <<"x">>),
    subscribe(C, <<"t">>, <<"s">>, 10, 1, earliest),
    flow(C, 10, 10),
    {EntryId, _} = recv_message(C),
    send_simple(C, command(10, 10, [varint(1, 10), varint(2, 0),
                                    msg(3, message_id(EntryId)),
                                    varint(8, 77)])),
    #{type := ack_response, consumer_id := 10, request_id := 77} = recv(C),
    ok.

redelivery_after_consumer_close(Config) ->
    C = connect(Config),
    connect_ok(C),
    producer(C, <<"t">>, 1, 1, <<"p">>),
    send_one(C, 1, 0, <<"redeliver-me">>),
    %% Durable (field 8 defaults to 1) so the subscription survives the close.
    subscribe(C, <<"t">>, <<"s">>, 10, 1, earliest),
    flow(C, 10, 10),
    {_EntryId, Payload} = recv_message(C),
    ?assertEqual(<<"redeliver-me">>, Payload),
    %% Close without acking; the entry moves to the redelivery queue.
    send_simple(C, command(16, 16, [varint(1, 10), varint(2, 20)])),
    #{type := success, request_id := 20} = recv(C),
    subscribe(C, <<"t">>, <<"s">>, 11, 2, earliest),
    flow(C, 11, 10),
    {EntryId2, Payload2} = recv_message(C),
    ?assertEqual(<<"redeliver-me">>, Payload2),
    ?assertEqual(0, EntryId2),
    ok.

seek_replays_from_entry(Config) ->
    C = connect(Config),
    connect_ok(C),
    producer(C, <<"t">>, 1, 1, <<"p">>),
    [send_one(C, 1, I, <<"m", (integer_to_binary(I))/binary>>)
     || I <- [0, 1, 2]],
    subscribe(C, <<"t">>, <<"s">>, 10, 1, earliest),
    flow(C, 10, 10),
    First = [recv_message(C) || _ <- [1, 2, 3]],
    ?assertEqual([0, 1, 2], [Id || {Id, _} <- First]),
    [send_simple(C, command(10, 10, [varint(1, 10), varint(2, 0),
                                     msg(3, message_id(Id))]))
     || {Id, _} <- First],
    send_simple(C, command(28, 28, [varint(1, 10), varint(2, 30),
                                    msg(3, message_id(0))])),
    #{type := success, request_id := 30} = recv(C),
    flow(C, 10, 10),
    Again = [recv_message(C) || _ <- [1, 2, 3]],
    ?assertEqual([0, 1, 2], [Id || {Id, _} <- Again]),
    ok.

ping_returns_pong(Config) ->
    C = connect(Config),
    send_simple(C, command(18, 18, [])),
    #{type := pong} = recv(C),
    ok.

get_last_message_id(Config) ->
    C = connect(Config),
    connect_ok(C),
    producer(C, <<"t">>, 1, 1, <<"p">>),
    send_one(C, 1, 0, <<"a">>),
    send_one(C, 1, 1, <<"b">>),
    subscribe(C, <<"t">>, <<"s">>, 10, 1, earliest),
    send_simple(C, command(29, 29, [varint(1, 10), varint(2, 40)])),
    #{type := last_message_id_response, request_id := 40,
      message_id := EntryId} = recv(C),
    ?assertEqual(1, EntryId),
    ok.

close_producer_success(Config) ->
    C = connect(Config),
    connect_ok(C),
    producer(C, <<"t">>, 1, 1, <<"p">>),
    send_simple(C, command(15, 15, [varint(1, 1), varint(2, 50)])),
    #{type := success, request_id := 50} = recv(C),
    ok.

unsubscribe_success(Config) ->
    C = connect(Config),
    connect_ok(C),
    subscribe(C, <<"t">>, <<"s">>, 10, 1, earliest),
    send_simple(C, command(12, 12, [varint(1, 10), varint(2, 60)])),
    #{type := success, request_id := 60} = recv(C),
    ok.

send_without_producer_errors(Config) ->
    C = connect(Config),
    connect_ok(C),
    Meta = pulsar_codec:encode_message_metadata(#{publish_time => 1,
                                                  uncompressed_size => 1}),
    Frame = pulsar_codec:encode_payload_frame(
              base_command(6, 6, [varint(1, 99), varint(2, 0), varint(3, 1)]),
              Meta, <<"z">>),
    ok = gen_tcp:send(C#c.s, Frame),
    #{type := error, error := Code} = recv(C),
    ?assertEqual(0, Code),
    ok.

%%%_* Byte-level client -----------------------------------------------------

connect(Config) ->
    Port = ?config(port, Config),
    {ok, S} = gen_tcp:connect({127, 0, 0, 1}, Port,
                              [binary, {active, false}, {packet, 4}], 5000),
    #c{s = S, port = Port}.

connect_ok(C) ->
    send_simple(C, connect_fields()),
    #{type := connected} = recv(C),
    ok.

recv(#c{s = S}) ->
    {ok, Frame} = gen_tcp:recv(S, 0, 10000),
    {ok, Decoded, _Tail} = pulsar_codec:decode_frame(Frame),
    Decoded.

recv_message(C) ->
    Frame = recv_raw(C),
    {ok, #{type := message} = Decoded, Tail} = pulsar_codec:decode_frame(Frame),
    {ok, _Meta, Payload} = pulsar_codec:decode_payload(Tail),
    {maps:get(message_id, Decoded), Payload}.

recv_raw(#c{s = S}) ->
    {ok, Frame} = gen_tcp:recv(S, 0, 10000),
    Frame.

send_simple(C, Frame) ->
    ok = gen_tcp:send(C#c.s, Frame).

producer(C, Topic, ProducerId, RequestId, Name) ->
    send_simple(C, command(5, 5, [str(1, Topic), varint(2, ProducerId),
                                  varint(3, RequestId), str(4, Name)])),
    recv(C).

subscribe(C, Topic, Sub, ConsumerId, RequestId, InitialPosition) ->
    Pos = case InitialPosition of earliest -> 1; latest -> 0 end,
    send_simple(C, command(4, 4, [str(1, Topic), str(2, Sub),
                                  varint(3, 0),            % Exclusive
                                  varint(4, ConsumerId),
                                  varint(5, RequestId),
                                  str(6, <<"consumer">>),
                                  varint(13, Pos)])),
    #{type := success, request_id := RequestId} = recv(C),
    ok.

flow(C, ConsumerId, Permits) ->
    send_simple(C, command(11, 11, [varint(1, ConsumerId), varint(2, Permits)])),
    ok.

send_one(C, ProducerId, Seq, Payload) ->
    Meta = pulsar_codec:encode_message_metadata(
             #{producer_name => <<"p">>, sequence_id => Seq,
               publish_time => 1700000000000 + Seq,
               uncompressed_size => byte_size(Payload)}),
    Frame = pulsar_codec:encode_payload_frame(
              base_command(6, 6, [varint(1, ProducerId), varint(2, Seq),
                                  varint(3, 1)]),
              Meta, Payload),
    ok = gen_tcp:send(C#c.s, Frame),
    recv(C).

send_batch(C, ProducerId, Seq, Count, Records) ->
    Payload = iolist_to_binary(Records),
    Meta = pulsar_codec:encode_message_metadata(
             #{producer_name => <<"p">>, sequence_id => Seq,
               publish_time => 1700000000000,
               num_messages_in_batch => Count,
               uncompressed_size => byte_size(Payload)}),
    Frame = pulsar_codec:encode_payload_frame(
              base_command(6, 6, [varint(1, ProducerId), varint(2, Seq),
                                  varint(3, Count)]),
              Meta, Payload),
    ok = gen_tcp:send(C#c.s, Frame),
    recv(C).

connect_fields() ->
    command(2, 2, [str(1, <<"Pulsar-CT">>), varint(4, 15)]).

%%%_* Topic helpers ---------------------------------------------------------

read_topic(Topic) ->
    Destination = <<"pulsar/", Topic/binary>>,
    {ok, Page, _Next} = broker_queue:read_from(?PARTITION, 0, 256),
    [M || {_Off, #broker_message{destination = D} = M} <- Page, D =:= Destination].

wait_unacked(Topic, Sub) ->
    wait_unacked(Topic, Sub, 100).

wait_unacked(Topic, Sub, N) ->
    case [Unacked || {T, S, _C, _Next, _Scan, Unacked, _R} <-
                         pulsar_subscriptions:stats(),
                     T =:= Topic, S =:= Sub] of
        [0] ->
            ok;
        _ when N =:= 0 ->
            {still_unacked, pulsar_subscriptions:stats()};
        _ ->
            timer:sleep(20),
            wait_unacked(Topic, Sub, N - 1)
    end.

%%%_* Frame builders -------------------------------------------------------

%% A BaseCommand with no size prefix (used inside encode_payload_frame/3).
base_command(Type, FieldNo, Fields) ->
    iolist_to_binary([varint(1, Type), msg(FieldNo, Fields)]).

%% A complete simple command frame: [command_size][BaseCommand].
command(Type, FieldNo, Fields) ->
    Base = base_command(Type, FieldNo, Fields),
    <<(byte_size(Base)):32/signed, Base/binary>>.

varint(Field, V) ->
    [key(Field, 0), uvarint(V band 16#FFFFFFFFFFFFFFFF)].

str(Field, Bin) ->
    [key(Field, 2), uvarint(byte_size(Bin)), Bin].

msg(Field, Fields) ->
    Bin = iolist_to_binary(Fields),
    [key(Field, 2), uvarint(byte_size(Bin)), Bin].

key(Field, Wire) ->
    uvarint((Field bsl 3) bor Wire).

uvarint(V) when V < 128 -> [V];
uvarint(V) -> [(V band 127) bor 128 | uvarint(V bsr 7)].

message_id(EntryId) ->
    iolist_to_binary([varint(1, 1), varint(2, EntryId), varint(3, -1)]).

%% One batched record: [meta_size][SingleMessageMetadata][payload].
single(Payload, Seq) ->
    Meta = iolist_to_binary([key(3, 0), uvarint(byte_size(Payload)),
                             key(8, 0), uvarint(Seq)]),
    <<(byte_size(Meta)):32/signed, Meta/binary, Payload/binary>>.
