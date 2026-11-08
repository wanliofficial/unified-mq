%% @doc Pulsar binary protocol codec (S5, ADR-006).
%%
%% Pulsar speaks protobuf-lite inside a length-prefixed frame:
%%
%%   [total_size:32][command_size:32][BaseCommand][payload?]
%%
%% and a payload is `[16#0e01:16][crc32c:32][metadata_size:32][MessageMetadata][payload]'.
%% This module implements exactly the messages the official Java client needs
%% for connect / lookup / produce / consume / ack / flow; unknown fields are
%% skipped on decode, and everything this adapter emits is a subset of
%% `PulsarApi.proto'. The scope and its limits are documented in
%% `docs/protocol-compatibility.md' §4.9.
-module(pulsar_codec).

-export([decode_frame/1, decode_payload/1, encode_payload_frame/3,
         encode_connected/2, encode_partitioned_metadata_response/2,
         encode_lookup_response/3, encode_producer_success/3,
         encode_send_receipt/4, encode_success/1, encode_error/3,
         encode_message/3, encode_pong/0,
         encode_last_message_id_response/2, encode_ack_response/2,
         encode_message_metadata/1, decode_message_metadata/1,
         decode_single_metadata/1, crc32c/1, message_id/1]).

%% BaseCommand.Type values.
-define(CONNECT, 2).
-define(CONNECTED, 3).
-define(SUBSCRIBE, 4).
-define(PRODUCER, 5).
-define(SEND, 6).
-define(SEND_RECEIPT, 7).
-define(MESSAGE, 9).
-define(ACK, 10).
-define(FLOW, 11).
-define(UNSUBSCRIBE, 12).
-define(SUCCESS, 13).
-define(ERROR, 14).
-define(CLOSE_PRODUCER, 15).
-define(CLOSE_CONSUMER, 16).
-define(PRODUCER_SUCCESS, 17).
-define(PING, 18).
-define(PONG, 19).
-define(PARTITIONED_METADATA, 21).
-define(PARTITIONED_METADATA_RESPONSE, 22).
-define(LOOKUP, 23).
-define(LOOKUP_RESPONSE, 24).
-define(SEEK, 28).
-define(GET_LAST_MESSAGE_ID, 29).
-define(GET_LAST_MESSAGE_ID_RESPONSE, 30).
-define(ACK_RESPONSE, 38).

-define(MAGIC, 16#0e01).
-define(MAX_MESSAGE_SIZE, 5 * 1024 * 1024).

%%%_* Frames ---------------------------------------------------------------

%% @doc Split one frame body (the transport already removed the outer
%% `total_size'). Returns the decoded command plus the payload section, which
%% is empty for every command except `SEND'.
-spec decode_frame(binary()) -> {ok, map(), binary()} | more.
decode_frame(<<CmdSize:32/signed, Rest/binary>>) when byte_size(Rest) >= CmdSize ->
    <<Cmd:CmdSize/binary, Tail/binary>> = Rest,
    {ok, command(decode_fields(Cmd)), Tail};
decode_frame(_) ->
    more.

command(Fields) ->
    case get_int(1, Fields, 0) of
        ?CONNECT -> connect(decode_fields(get_bin(2, Fields, <<>>)));
        ?CONNECTED -> connected(decode_fields(get_bin(3, Fields, <<>>)));
        ?SUBSCRIBE -> subscribe(decode_fields(get_bin(4, Fields, <<>>)));
        ?PRODUCER -> producer(decode_fields(get_bin(5, Fields, <<>>)));
        ?SEND -> send(decode_fields(get_bin(6, Fields, <<>>)));
        ?SEND_RECEIPT -> send_receipt(decode_fields(get_bin(7, Fields, <<>>)));
        ?MESSAGE -> message(decode_fields(get_bin(9, Fields, <<>>)));
        ?ACK -> ack(decode_fields(get_bin(10, Fields, <<>>)));
        ?FLOW -> flow(decode_fields(get_bin(11, Fields, <<>>)));
        ?UNSUBSCRIBE -> unsubscribe(decode_fields(get_bin(12, Fields, <<>>)));
        ?SUCCESS -> success(decode_fields(get_bin(13, Fields, <<>>)));
        ?ERROR -> error_reply(decode_fields(get_bin(14, Fields, <<>>)));
        ?CLOSE_PRODUCER -> close_producer(decode_fields(get_bin(15, Fields, <<>>)));
        ?CLOSE_CONSUMER -> close_consumer(decode_fields(get_bin(16, Fields, <<>>)));
        ?PRODUCER_SUCCESS -> producer_success(decode_fields(get_bin(17, Fields, <<>>)));
        ?PING -> #{type => ping};
        ?PONG -> #{type => pong};
        ?PARTITIONED_METADATA ->
            partition_metadata(decode_fields(get_bin(21, Fields, <<>>)));
        ?PARTITIONED_METADATA_RESPONSE ->
            partitioned_metadata_response(decode_fields(get_bin(22, Fields, <<>>)));
        ?LOOKUP -> lookup(decode_fields(get_bin(23, Fields, <<>>)));
        ?LOOKUP_RESPONSE ->
            lookup_response(decode_fields(get_bin(24, Fields, <<>>)));
        ?SEEK -> seek(decode_fields(get_bin(28, Fields, <<>>)));
        ?GET_LAST_MESSAGE_ID ->
            get_last_message_id(decode_fields(get_bin(29, Fields, <<>>)));
        ?GET_LAST_MESSAGE_ID_RESPONSE ->
            last_message_id_response(decode_fields(get_bin(30, Fields, <<>>)));
        ?ACK_RESPONSE -> ack_response(decode_fields(get_bin(38, Fields, <<>>)));
        Other -> #{type => {unknown, Other}}
    end.

%%%_* Server-side decoders (client direction, used by the CT client) --------

connected(F) ->
    #{type => connected,
      server_version => get_str(1, F, <<>>),
      protocol_version => get_int(2, F, 0),
      max_message_size => get_int(3, F, 0)}.

%% SendReceipt.message_id is field 3 (field 2 is the sequence id).
send_receipt(F) ->
    #{type => send_receipt,
      producer_id => get_int(1, F, 0),
      sequence_id => get_int(2, F, 0),
      message_id => message_id_at(3, F),
      highest_sequence_id => get_int(4, F, 0)}.

%% MessageIdData sits directly in field 2 of both Message and
%% GetLastMessageIdResponse.
message(F) ->
    #{type => message,
      consumer_id => get_int(1, F, 0),
      message_id => message_id_at(2, F),
      redelivery_count => get_int(3, F, 0)}.

success(F) ->
    #{type => success, request_id => get_int(1, F, 0)}.

error_reply(F) ->
    #{type => error,
      request_id => get_int(1, F, 0),
      error => get_int(2, F, 0),
      message => get_str(3, F, <<>>)}.

producer_success(F) ->
    #{type => producer_success,
      request_id => get_int(1, F, 0),
      producer_name => get_str(2, F, <<>>),
      last_sequence_id => get_int(3, F, -1)}.

partitioned_metadata_response(F) ->
    #{type => partitioned_metadata_response,
      partitions => get_int(1, F, 0),
      request_id => get_int(2, F, 0),
      error => get_int(3, F, 0)}.

lookup_response(F) ->
    #{type => lookup_response,
      broker_url => get_str(1, F, <<>>),
      response => get_int(3, F, 0),
      request_id => get_int(4, F, 0),
      authoritative => get_int(5, F, 0) =/= 0,
      proxy_through_service_url => get_int(8, F, 0) =/= 0}.

last_message_id_response(F) ->
    #{type => last_message_id_response,
      request_id => get_int(1, F, 0),
      message_id => message_id_at(2, F)}.

ack_response(F) ->
    #{type => ack_response,
      consumer_id => get_int(1, F, 0),
      request_id => get_int(6, F, 0)}.

%% Read a nested MessageIdData found under `Field' and return its entry id.
message_id_at(Field, F) ->
    case get_bin(Field, F, <<>>) of
        Bin when is_binary(Bin), Bin =/= <<>> ->
            {_LedgerId, EntryId} = message_id_fields(Bin),
            EntryId;
        _ ->
            -1
    end.

connect(F) ->
    #{type => connect,
      client_version => get_str(1, F, <<>>),
      protocol_version => get_int(4, F, 0),
      auth_method_name => get_str(5, F, <<>>),
      auth_data => get_bin(3, F, <<>>)}.

subscribe(F) ->
    #{type => subscribe,
      topic => get_str(1, F, <<>>),
      subscription => get_str(2, F, <<>>),
      sub_type => sub_type(get_int(3, F, 0)),
      consumer_id => get_int(4, F, 0),
      request_id => get_int(5, F, 0),
      consumer_name => get_str(6, F, <<>>),
      durable => get_int(8, F, 1) =/= 0,
      initial_position => case get_int(13, F, 0) of 1 -> earliest; _ -> latest end}.

sub_type(0) -> exclusive;
sub_type(1) -> shared;
sub_type(2) -> failover;
sub_type(3) -> key_shared;
sub_type(_) -> exclusive.

producer(F) ->
    #{type => producer,
      topic => get_str(1, F, <<>>),
      producer_id => get_int(2, F, 0),
      request_id => get_int(3, F, 0),
      producer_name => get_str(4, F, <<>>)}.

send(F) ->
    #{type => send,
      producer_id => get_int(1, F, 0),
      sequence_id => get_int(2, F, 0),
      num_messages => get_int(3, F, 1),
      highest_sequence_id => get_int(6, F, 0)}.

ack(F) ->
    #{type => ack,
      consumer_id => get_int(1, F, 0),
      ack_type => case get_int(2, F, 0) of 1 -> cumulative; _ -> individual end,
      message_ids => [message_id_fields(B) || B <- repeated(3, F)],
      request_id => get_int(8, F, 0)}.

flow(F) ->
    #{type => flow,
      consumer_id => get_int(1, F, 0),
      permits => get_int(2, F, 0)}.

unsubscribe(F) ->
    #{type => unsubscribe,
      consumer_id => get_int(1, F, 0),
      request_id => get_int(2, F, 0)}.

close_producer(F) ->
    #{type => close_producer,
      producer_id => get_int(1, F, 0),
      request_id => get_int(2, F, 0)}.

close_consumer(F) ->
    #{type => close_consumer,
      consumer_id => get_int(1, F, 0),
      request_id => get_int(2, F, 0)}.

partition_metadata(F) ->
    #{type => partitioned_metadata,
      topic => get_str(1, F, <<>>),
      request_id => get_int(2, F, 0)}.

lookup(F) ->
    #{type => lookup,
      topic => get_str(1, F, <<>>),
      request_id => get_int(2, F, 0),
      authoritative => get_int(3, F, 0) =/= 0}.

seek(F) ->
    case get_bin(3, F, <<>>) of
        <<>> ->
            #{type => seek, consumer_id => get_int(1, F, 0),
              request_id => get_int(2, F, 0), message_id => {-1, -1}};
        Bin ->
            {LedgerId, EntryId} = message_id_fields(Bin),
            #{type => seek, consumer_id => get_int(1, F, 0),
              request_id => get_int(2, F, 0), message_id => {LedgerId, EntryId}}
    end.

get_last_message_id(F) ->
    #{type => get_last_message_id,
      consumer_id => get_int(1, F, 0),
      request_id => get_int(2, F, 0)}.

message_id_fields(Bin) ->
    F = decode_fields(Bin),
    {signed64(get_int(1, F, 0)), signed64(get_int(2, F, 0))}.

%% Protobuf encodes negative int64 as a 10-byte two's-complement varint.
%% `MessageId.earliest'/`latest' use ledgerId = entryId = -1, so reinterpret
%% the top half of the uint64 range as negative.
signed64(V) when V >= 16#8000000000000000 -> V - 16#10000000000000000;
signed64(V) -> V.

%% @doc Decode the payload section of a SEND/MESSAGE frame.
-spec decode_payload(binary()) -> {ok, map(), binary()} | {error, term()}.
decode_payload(<<>>) ->
    {ok, #{}, <<>>};
decode_payload(<<?MAGIC:16/unsigned, Crc:32/unsigned, Rest/binary>>) ->
    case Rest of
        <<MetaSize:32/signed, Tail/binary>> when byte_size(Tail) >= MetaSize ->
            <<Meta:MetaSize/binary, Payload/binary>> = Tail,
            case crc32c(<<MetaSize:32/signed, Meta/binary, Payload/binary>>) of
                Crc -> {ok, decode_message_metadata(Meta), Payload};
                _ -> {error, checksum_mismatch}
            end;
        _ ->
            {error, truncated_metadata}
    end;
decode_payload(<<MetaSize:32/signed, Tail/binary>>) when byte_size(Tail) >= MetaSize ->
    %% No magic: a pre-v6 client sends metadata directly, without a checksum.
    <<Meta:MetaSize/binary, Payload/binary>> = Tail,
    {ok, decode_message_metadata(Meta), Payload};
decode_payload(_) ->
    {error, unsupported_payload}.

%% @doc Build a command plus its checksummed payload section.
-spec encode_payload_frame(iodata(), iodata(), binary()) -> binary().
encode_payload_frame(Command, Metadata, Payload) ->
    Cmd = iolist_to_binary(Command),
    Meta = iolist_to_binary(Metadata),
    Checked = <<(byte_size(Meta)):32/signed, Meta/binary, Payload/binary>>,
    iolist_to_binary([<<(byte_size(Cmd)):32/signed>>, Cmd,
                      <<?MAGIC:16/unsigned, (crc32c(Checked)):32/unsigned>>,
                      Checked]).

%%%_* Server commands ------------------------------------------------------

%% The transport socket uses `{packet, 4}', so it writes the outer
%% `total_size' prefix itself; a simple command's body still has to start
%% with its own `command_size'. `encode_payload_frame/3' adds that prefix for
%% SEND/MESSAGE, and `frame/2' adds it for every other command.
frame(Type, Fields) ->
    Cmd = base(Type, Fields),
    [<<(byte_size(Cmd)):32/signed>>, Cmd].

encode_connected(ServerVersion, ProtocolVersion) ->
    frame(?CONNECTED,
          enc_msg(3, [enc_str(1, ServerVersion),
                      enc_varint(2, ProtocolVersion),
                      enc_varint(3, ?MAX_MESSAGE_SIZE)])).

encode_partitioned_metadata_response(RequestId, Partitions) ->
    frame(?PARTITIONED_METADATA_RESPONSE,
          enc_msg(22, [enc_varint(1, Partitions),
                       enc_varint(2, RequestId),
                       enc_varint(3, 0)])).             % Success

encode_lookup_response(RequestId, Url, Authoritative) ->
    frame(?LOOKUP_RESPONSE,
          enc_msg(24, [enc_str(1, Url),
                       enc_varint(3, 1),                 % Connect
                       enc_varint(4, RequestId),
                       enc_bool(5, Authoritative),
                       enc_bool(8, true)])).             % proxy through service url

encode_producer_success(RequestId, Name, LastSequenceId) ->
    %% pulsar-client 3.3.1 treats `schema_version` (field 4) as required on
    %% decode, so always emit it (empty = "no schema").
    frame(?PRODUCER_SUCCESS,
          enc_msg(17, [enc_varint(1, RequestId),
                       enc_str(2, Name),
                       enc_varint(3, LastSequenceId),
                       enc_str(4, <<>>)])).

encode_send_receipt(ProducerId, SequenceId, EntryId, HighestSequenceId) ->
    frame(?SEND_RECEIPT,
          enc_msg(7, [enc_varint(1, ProducerId),
                      enc_varint(2, SequenceId),
                      enc_msg(3, message_id(EntryId)),
                      enc_varint(4, HighestSequenceId)])).

encode_success(RequestId) ->
    frame(?SUCCESS, enc_msg(13, [enc_varint(1, RequestId)])).

encode_error(RequestId, Code, Message) ->
    frame(?ERROR, enc_msg(14, [enc_varint(1, RequestId),
                               enc_varint(2, Code),
                               enc_str(3, Message)])).

encode_message(ConsumerId, EntryId, Payload) ->
    B = base(?MESSAGE,
             enc_msg(9, [enc_varint(1, ConsumerId),
                         enc_msg(2, message_id(EntryId)),
                         enc_varint(3, 0)])),           % redelivery_count
    {B, Payload}.

encode_pong() ->
    frame(?PONG, enc_msg(19, [])).

encode_last_message_id_response(RequestId, EntryId) ->
    frame(?GET_LAST_MESSAGE_ID_RESPONSE,
          enc_msg(30, [enc_varint(1, RequestId),
                       enc_msg(2, message_id(EntryId))])).

encode_ack_response(ConsumerId, RequestId) ->
    frame(?ACK_RESPONSE,
          enc_msg(38, [enc_varint(1, ConsumerId),
                       enc_varint(6, RequestId)])).

message_id(EntryId) ->
    [enc_varint(1, 1),                              % ledgerId
     enc_varint(2, EntryId),
     enc_varint(3, -1)].                            % partition

%%%_* Message metadata -----------------------------------------------------

encode_message_metadata(Meta) ->
    Properties = maps:get(properties, Meta, []),
    [enc_str(1, maps:get(producer_name, Meta, <<"unified-mq">>)),
     enc_varint(2, maps:get(sequence_id, Meta, 0)),
     enc_varint(3, maps:get(publish_time, Meta, 0)),
     [enc_msg(4, key_value(K, V)) || {K, V} <- Properties],
     enc_opt_str(6, maps:get(partition_key, Meta, undefined)),
     enc_varint(8, 0),                              % compression NONE
     enc_varint(9, maps:get(uncompressed_size, Meta, 0)),
     enc_opt_int(11, maps:get(num_messages_in_batch, Meta, undefined)),
     enc_opt_int(12, maps:get(event_time, Meta, undefined)),
     enc_opt_bin(18, maps:get(ordering_key, Meta, undefined)),
     enc_opt_bool(25, maps:get(null_value, Meta, undefined))].

key_value(K, V) ->
    [enc_str(1, K), enc_str(2, V)].

decode_message_metadata(Bin) ->
    F = decode_fields(Bin),
    #{producer_name => get_str(1, F, <<>>),
      sequence_id => get_int(2, F, 0),
      publish_time => get_int(3, F, 0),
      properties => [key_value_decode(B) || B <- repeated(4, F)],
      partition_key => opt_str(6, F),
      compression => get_int(8, F, 0),
      uncompressed_size => get_int(9, F, 0),
      num_messages_in_batch => opt_int(11, F),
      event_time => get_int(12, F, 0),
      partition_key_b64_encoded => get_int(17, F, 0) =/= 0,
      ordering_key => opt_bin(18, F),
      null_value => get_int(25, F, 0) =/= 0}.

key_value_decode(Bin) ->
    F = decode_fields(Bin),
    {get_str(1, F, <<>>), get_str(2, F, <<>>)}.

decode_single_metadata(Bin) ->
    F = decode_fields(Bin),
    #{properties => [key_value_decode(B) || B <- repeated(1, F)],
      partition_key => opt_str(2, F),
      payload_size => get_int(3, F, 0),
      event_time => get_int(5, F, 0),
      sequence_id => get_int(8, F, 0),
      null_value => get_int(9, F, 0) =/= 0}.

%%%_* Protobuf primitives --------------------------------------------------

base(Type, Fields) ->
    iolist_to_binary([enc_varint(1, Type), Fields]).

key(Field, Wire) ->
    uvarint((Field bsl 3) bor Wire).

enc_varint(Field, V) ->
    [key(Field, 0), uvarint(V band 16#FFFFFFFFFFFFFFFF)].

enc_bool(_Field, false) -> [];
enc_bool(Field, true) -> enc_varint(Field, 1);
enc_bool(_Field, undefined) -> [].

enc_str(Field, Bin) ->
    [key(Field, 2), uvarint(byte_size(Bin)), Bin].

enc_opt_str(_Field, undefined) -> [];
enc_opt_str(Field, Bin) -> enc_str(Field, Bin).

enc_opt_bin(_Field, undefined) -> [];
enc_opt_bin(Field, Bin) -> enc_str(Field, Bin).

enc_opt_int(_Field, undefined) -> [];
enc_opt_int(Field, V) -> enc_varint(Field, V).

enc_opt_bool(_Field, undefined) -> [];
enc_opt_bool(Field, V) -> enc_bool(Field, V).

enc_msg(Field, Encoded) ->
    Bin = iolist_to_binary(Encoded),
    [key(Field, 2), uvarint(byte_size(Bin)), Bin].

uvarint(V) when V < 128 -> [V];
uvarint(V) -> [(V band 127) bor 128 | uvarint(V bsr 7)].

decode_fields(Bin) when is_binary(Bin) ->
    df(Bin, #{}).

df(<<>>, Acc) ->
    Acc;
df(Bin, Acc) ->
    {Key, R1} = get_uvarint(Bin),
    Field = Key bsr 3,
    case Key band 7 of
        0 ->
            {V, R2} = get_uvarint(R1),
            df(R2, add(Field, V, Acc));
        1 ->
            <<V:64/little-unsigned, R2/binary>> = R1,
            df(R2, add(Field, V, Acc));
        2 ->
            {Len, R2} = get_uvarint(R1),
            <<V:Len/binary, R3/binary>> = R2,
            df(R3, add(Field, V, Acc));
        5 ->
            <<V:32/little-unsigned, R2/binary>> = R1,
            df(R2, add(Field, V, Acc));
        Wire ->
            error({unsupported_wire_type, Wire})
    end.

add(Field, Value, Acc) ->
    maps:update_with(Field, fun(L) -> [Value | L] end, [Value], Acc).

repeated(Field, Fields) ->
    lists:reverse(maps:get(Field, Fields, [])).

get_int(Field, Fields, Default) ->
    case maps:get(Field, Fields, []) of
        [V | _] -> V;
        [] -> Default
    end.

get_bin(Field, Fields, Default) ->
    case maps:get(Field, Fields, []) of
        [V | _] when is_binary(V) -> V;
        _ -> Default
    end.

get_str(Field, Fields, Default) ->
    get_bin(Field, Fields, Default).

opt_int(Field, Fields) ->
    case maps:get(Field, Fields, []) of
        [V | _] -> V;
        [] -> undefined
    end.

opt_bin(Field, Fields) ->
    case maps:get(Field, Fields, []) of
        [V | _] -> V;
        [] -> undefined
    end.

opt_str(Field, Fields) ->
    opt_bin(Field, Fields).

get_uvarint(Bin) ->
    get_uvarint(Bin, 0, 0).

get_uvarint(<<>>, _Shift, _Acc) ->
    error(truncated_varint);
get_uvarint(<<B, Rest/binary>>, Shift, Acc) ->
    Acc1 = Acc bor ((B band 127) bsl Shift),
    case B band 128 of
        0 -> {Acc1, Rest};
        _ -> get_uvarint(Rest, Shift + 7, Acc1)
    end.

%%%_* CRC32C (Castagnoli) --------------------------------------------------

%% @doc CRC32C of a binary, the checksum Pulsar puts in front of every payload.
-spec crc32c(binary()) -> non_neg_integer().
crc32c(Bin) ->
    crc(Bin, 16#FFFFFFFF, table()) bxor 16#FFFFFFFF.

crc(<<>>, Crc, _Table) ->
    Crc;
crc(<<B, Rest/binary>>, Crc, Table) ->
    Index = (Crc bxor B) band 16#FF,
    crc(Rest, (Crc bsr 8) bxor element(Index + 1, Table), Table).

table() ->
    case persistent_term:get({?MODULE, crc_table}, undefined) of
        undefined ->
            Table = list_to_tuple([entry(N) || N <- lists:seq(0, 255)]),
            persistent_term:put({?MODULE, crc_table}, Table),
            Table;
        Table ->
            Table
    end.

entry(N) ->
    entry(N, 8).

entry(C, 0) ->
    C;
entry(C, N) ->
    C1 = case C band 1 of
             1 -> (C bsr 1) bxor 16#82F63B78;
             0 -> C bsr 1
         end,
    entry(C1, N - 1).
