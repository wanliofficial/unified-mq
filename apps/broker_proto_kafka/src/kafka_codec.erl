%% @doc Kafka wire codec (S5, README §11.1).
%%
%% Pure functions: Kafka primitive types, the `ApiVersions' flexible encoding
%% helpers, and the v2 RecordBatch (the format kafka-clients 3.7.1 produces and
%% consumes). No sockets here; `kafka_conn_SUITE' exercises it through real
%% connections and `test/protocol-compatibility/kafka/' through the official
%% Java client.
%%
%% Version posture: `kafka_conn' advertises one exact version per API (the last
%% non-flexible version), so the decoders below only implement those shapes.
%% The single flexible shape is `ApiVersions' v3, which clients send before
%% any version negotiation exists.
-module(kafka_codec).

-export([%% decoders
         get_int8/1, get_int16/1, get_int32/1, get_int64/1, get_bool/1,
         get_string/1, get_nullable_string/1,
         get_bytes/1, get_nullable_bytes/1,
         get_array/2, get_nullable_array/2,
         get_uvarint/1, get_varint/1, get_varlong/1,
         get_compact_string/1, get_compact_array/2, get_tagged_fields/1,
         %% encoders
         int8/1, int16/1, int32/1, int64/1, bool/1,
         string/1, nullable_string/1,
         bytes/1, nullable_bytes/1,
         array/2, nullable_array/2,
         uvarint/1, varint/1, varlong/1,
         compact_string/1, compact_array/2, tagged_fields/1,
         %% record batch
         encode_record_batch/2, decode_record_batches/1, crc32c/1]).

%%%_* Scalars -------------------------------------------------------------

int8(V)  -> <<V:8/signed>>.
int16(V) -> <<V:16/signed>>.
int32(V) -> <<V:32/signed>>.
int64(V) -> <<V:64/signed>>.
bool(true) -> <<1>>;
bool(false) -> <<0>>.

%%%_* Length-prefixed values ----------------------------------------------

string(Bin) when is_binary(Bin) ->
    <<(byte_size(Bin)):16/signed, Bin/binary>>.

nullable_string(undefined) -> int16(-1);
nullable_string(Bin)       -> string(Bin).

bytes(Bin) when is_binary(Bin) ->
    <<(byte_size(Bin)):32/signed, Bin/binary>>.

nullable_bytes(undefined) -> int32(-1);
nullable_bytes(Bin)       -> bytes(Bin).

array(Fun, Items) when is_list(Items) ->
    Encoded = [Fun(I) || I <- Items],
    [int32(length(Items)) | Encoded].

nullable_array(_Fun, undefined) -> int32(-1);
nullable_array(Fun, Items)      -> array(Fun, Items).

%%%_* Varints -------------------------------------------------------------

%% Unsigned LEB128, used by compact strings/arrays and tagged fields.
uvarint(V) when V >= 0 ->
    uvarint(V, []).

uvarint(V, Acc) when V < 128 ->
    list_to_binary(lists:reverse([V | Acc]));
uvarint(V, Acc) ->
    uvarint(V bsr 7, [(V band 127) bor 128 | Acc]).

%% Zig-zag signed varint/varlong, used by RecordBatch record fields.
varint(V) ->
    uvarint(((V bsl 1) bxor (V bsr 63)) band 16#FFFFFFFFFFFFFFFF).

varlong(V) ->
    varint(V).

%%%_* Compact (flexible) values -------------------------------------------

compact_string(undefined) -> uvarint(0);
compact_string(Bin)       -> [uvarint(byte_size(Bin) + 1), Bin].

compact_array(_Fun, undefined) -> uvarint(0);
compact_array(Fun, Items) ->
    [uvarint(length(Items) + 1) | [Fun(I) || I <- Items]].

tagged_fields(_Fields) -> uvarint(0).

%%%_* Decoders ------------------------------------------------------------

get_int8(<<V:8/signed, R/binary>>) -> {V, R}.
get_int16(<<V:16/signed, R/binary>>) -> {V, R}.
get_int32(<<V:32/signed, R/binary>>) -> {V, R}.
get_int64(<<V:64/signed, R/binary>>) -> {V, R}.
get_bool(<<0, R/binary>>) -> {false, R};
get_bool(<<1, R/binary>>) -> {true, R}.

get_string(Bin) ->
    {Len, R} = get_int16(Bin),
    case Len of
        -1 -> {undefined, R};
        _  -> <<S:Len/binary, R2/binary>> = R, {S, R2}
    end.

get_nullable_string(Bin) -> get_string(Bin).

get_bytes(Bin) ->
    {Len, R} = get_int32(Bin),
    case Len of
        -1 -> {undefined, R};
        _  -> <<B:Len/binary, R2/binary>> = R, {B, R2}
    end.

get_nullable_bytes(Bin) -> get_bytes(Bin).

get_array(Fun, Bin) ->
    {N, R} = get_int32(Bin),
    get_n(Fun, N, R, []).

get_nullable_array(Fun, Bin) ->
    case get_int32(Bin) of
        {-1, R} -> {undefined, R};
        {N, R}  -> get_n(Fun, N, R, [])
    end.

get_n(_Fun, N, R, Acc) when N =< 0 ->
    {lists:reverse(Acc), R};
get_n(Fun, N, R, Acc) ->
    {Item, R2} = Fun(R),
    get_n(Fun, N - 1, R2, [Item | Acc]).

%% Unsigned LEB128.
get_uvarint(Bin) ->
    get_uvarint(Bin, 0, 0).

get_uvarint(<<1:1, V:7, R/binary>>, Shift, Acc) ->
    get_uvarint(R, Shift + 7, Acc bor (V bsl Shift));
get_uvarint(<<0:1, V:7, R/binary>>, Shift, Acc) ->
    {Acc bor (V bsl Shift), R}.

get_varint(Bin) ->
    {U, R} = get_uvarint(Bin),
    {unzigzag(U), R}.

get_varlong(Bin) -> get_varint(Bin).

unzigzag(U) ->
    (U bsr 1) bxor -(U band 1).

get_compact_string(Bin) ->
    {N, R} = get_uvarint(Bin),
    case N of
        0 -> {undefined, R};
        _ -> Len = N - 1, <<S:Len/binary, R2/binary>> = R, {S, R2}
    end.

%% Flexible arrays carry their length + 1 as a uvarint. The caller supplies
%% the item decoder.
get_compact_array(Fun, Bin) ->
    {N, R} = get_uvarint(Bin),
    get_n(Fun, N - 1, R, []).

get_tagged_fields(Bin) ->
    {N, R} = get_uvarint(Bin),
    skip_tagged(N, R).

skip_tagged(0, R) ->
    {#{}, R};
skip_tagged(N, R) ->
    {_Tag, R1} = get_uvarint(R),
    {Size, R2} = get_uvarint(R1),
    <<_:Size/binary, R3/binary>> = R2,
    skip_tagged(N - 1, R3).

%%%_* RecordBatch v2 ------------------------------------------------------

%% @doc Encode a v2 RecordBatch. `Records' are maps:
%% `#{offset := non_neg_integer(), timestamp := integer(),
%%    key := binary() | undefined, value := binary() | undefined,
%%    headers := [{binary(), binary() | undefined}]}'.
encode_record_batch(_BaseOffset, []) ->
    <<>>;
encode_record_batch(BaseOffset, Records) ->
    Last = lists:last(Records),
    BaseTs = lists:min([maps:get(timestamp, R) || R <- Records]),
    MaxTs = lists:max([maps:get(timestamp, R) || R <- Records]),
    LastDelta = maps:get(offset, Last) - BaseOffset,
    Encoded = [encode_record(BaseOffset, BaseTs, R) || R <- Records],
    %% The CRC covers everything after the CRC field: attributes onwards.
    %% partitionLeaderEpoch and magic sit before it and are not covered.
    Checked = <<0:16,                         % attributes: no compression
                LastDelta:32/signed,
                BaseTs:64/signed,
                MaxTs:64/signed,
                -1:64/signed,                 % producer id: unknown
                -1:16/signed,                 % producer epoch
                -1:32/signed,                 % base sequence
                (length(Records)):32/signed,
                (iolist_to_binary(Encoded))/binary>>,
    Crc = crc32c(Checked),
    AfterLength = <<-1:32/signed, 2:8, Crc:32/unsigned, Checked/binary>>,
    BatchLength = byte_size(AfterLength),
    <<BaseOffset:64/signed, BatchLength:32/signed, AfterLength/binary>>.

encode_record(BaseOffset, BaseTs, #{offset := Offset, timestamp := Ts} = R) ->
    Key = maps:get(key, R, undefined),
    Value = maps:get(value, R, undefined),
    Headers = maps:get(headers, R, []),
    Body = iolist_to_binary(
             [int8(0),
              varlong(Ts - BaseTs),
              varint(Offset - BaseOffset),
              svarint_bytes(Key),
              svarint_bytes(Value),
              varint(length(Headers)),
              [[svarint_bytes(HKey), svarint_bytes(HVal)] || {HKey, HVal} <- Headers]]),
    [varint(byte_size(Body)), Body].

svarint_bytes(undefined) -> varint(-1);
svarint_bytes(Bin)       -> [varint(byte_size(Bin)), Bin].

%% @doc Decode one or more v2 batches. Returns
%% `{ok, [#{base_offset := integer(), records := [record()]}]}' or
%% `{error, Reason}'. `records' carry absolute offsets and absolute timestamps.
decode_record_batches(<<>>) ->
    {ok, []};
decode_record_batches(undefined) ->
    {ok, []};
decode_record_batches(<<BaseOffset:64/signed, BatchLength:32/signed, Rest/binary>>)
  when byte_size(Rest) >= BatchLength ->
    <<Batch:BatchLength/binary, Tail/binary>> = Rest,
    <<_Epoch:32/signed, Magic:8, Crc:32/unsigned, Checked/binary>> = Batch,
    case crc32c(Checked) of
        Crc -> decode_batch_body(BaseOffset, Magic, Checked, Tail);
        _   -> {error, {crc_mismatch, BaseOffset}}
    end;
decode_record_batches(_) ->
    {error, truncated_batch}.

decode_batch_body(BaseOffset, 2, <<Attributes:16/signed,
                                _LastDelta:32/signed, BaseTs:64/signed,
                                _MaxTs:64/signed, _Pid:64/signed, _Pepoch:16/signed,
                                _Seq:32/signed, Count:32/signed, RecordsBin/binary>>,
                   Tail) when Attributes band 7 =:= 0 ->
    case decode_records(BaseOffset, BaseTs, Count, RecordsBin, Tail, []) of
        {ok, Records, Tail2} ->
            {ok, [#{base_offset => BaseOffset, records => Records}], Tail2};
        {error, _} = Error ->
            Error
    end;
decode_batch_body(_BaseOffset, Magic, _Checked, _Tail) when Magic =/= 2 ->
    {error, {unsupported_magic, Magic}};
decode_batch_body(_BaseOffset, _Magic, _Checked, _Tail) ->
    {error, unsupported_compression}.

decode_records(_BaseOffset, _BaseTs, 0, _Bin, Tail, Acc) ->
    {ok, lists:reverse(Acc), Tail};
decode_records(BaseOffset, BaseTs, N, Bin, Tail, Acc) ->
    case decode_record(BaseOffset, BaseTs, Bin) of
        {ok, Record, Rest} ->
            decode_records(BaseOffset, BaseTs, N - 1, Rest, Tail, [Record | Acc]);
        {error, _} = Error ->
            Error
    end.

decode_record(BaseOffset, BaseTs, Bin) ->
    try
        {Len, R} = get_varint(Bin),
        <<Body:Len/binary, Rest/binary>> = R,
        {_Attrs, R1} = get_int8(Body),
        {TsDelta, R2} = get_varlong(R1),
        {OffDelta, R3} = get_varint(R2),
        {Key, R4} = dvarint_bytes(R3),
        {Value, R5} = dvarint_bytes(R4),
        {HCount, R6} = get_varint(R5),
        {Headers, _R7} = decode_headers(HCount, R6, []),
        {ok, #{offset => BaseOffset + OffDelta,
               timestamp => BaseTs + TsDelta,
               key => Key, value => Value, headers => Headers}, Rest}
    catch
        _:_ -> {error, malformed_record}
    end.

dvarint_bytes(Bin) ->
    case get_varint(Bin) of
        {-1, R} -> {undefined, R};
        {Len, R} when Len >= 0 ->
            <<B:Len/binary, R2/binary>> = R,
            {B, R2}
    end.

decode_headers(0, Bin, Acc) ->
    {lists:reverse(Acc), Bin};
decode_headers(N, Bin, Acc) ->
    {K, R1} = dvarint_bytes(Bin),
    {V, R2} = dvarint_bytes(R1),
    decode_headers(N - 1, R2, [{K, V} | Acc]).

%%%_* CRC32C (Castagnoli) ------------------------------------------------

%% Reflected polynomial 0x82F63B78; the table is built once and cached.
crc32c(Data) ->
    Table = table(),
    crc32c(Data, 16#FFFFFFFF, Table).

crc32c(<<>>, Crc, _Table) ->
    Crc bxor 16#FFFFFFFF;
crc32c(<<Byte, Rest/binary>>, Crc, Table) ->
    Index = (Crc bxor Byte) band 16#FF,
    Next = (Crc bsr 8) bxor element(Index + 1, Table),
    crc32c(Rest, Next, Table).

table() ->
    case persistent_term:get({?MODULE, crc32c_table}, undefined) of
        undefined ->
            Table = list_to_tuple([crc_entry(I) || I <- lists:seq(0, 255)]),
            persistent_term:put({?MODULE, crc32c_table}, Table),
            Table;
        Table ->
            Table
    end.

crc_entry(I) ->
    lists:foldl(fun(_, C) ->
                        case C band 1 of
                            1 -> (C bsr 1) bxor 16#82F63B78;
                            0 -> C bsr 1
                        end
                end, I, lists:seq(1, 8)).
