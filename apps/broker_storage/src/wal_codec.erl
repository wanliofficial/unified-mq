%% @doc WAL record codec: the on-disk protocol of ADR-011 §11.5.
%%
%% The record layout is `<<Magic:8, Vsn:8, CRC32:32, Len:32>>' followed by
%% `term_to_binary(BodyTuple)'. The body tuple is deliberately NOT
%% `#broker_message{}': adding a field to the in-memory record must stay a Vsn
%% bump with a decode branch, never a broken segment.
%%
%% `partition_id' has no slot. A segment lives in `data/wal/{PartitionId}/', so
%% the directory is authoritative and `decode/2' stamps the id the caller passes.
%%
%% CRC-32 uses the native `erlang:crc32/1' BIF (0.55 µs for a 256 B body,
%% measured on OTP 28.3.1). The BIF is deprecated but present; a table-driven
%% Erlang implementation yields byte-identical values at ~11x the cost, so
%% replacing it later is format compatible rather than a migration.
%%
%% `term_to_binary/2' is called without the `deterministic' option: it costs
%% record size (no atom cache) and the option's guarantee is not needed here,
%% since a Raft leader's bytes are what replicas store. What must not regress is
%% that equal maps serialize identically whatever their insertion order, so
%% wal_codec_SUITE pins exactly that and would catch an OTP change.
-module(wal_codec).

-include_lib("broker_core/include/broker_message.hrl").

-export([encode/1, decode/2, record_size/1, header_size/0, current_vsn/0]).

%% 'U' for UnifiedMQ. wal_codec_SUITE pins this value: it is the only marker
%% distinguishing a segment from any other file in the data dir.
-define(MAGIC, 16#55).
-define(VSN, 1).
%% Magic:8 + Vsn:8 + CRC32:32 + Len:32 = 80 bits.
-define(HEADER_SIZE, 10).

-type decoded() :: {ok, #broker_message{}, Size :: non_neg_integer()}.

%% `truncated' means "bytes are missing", which is the expected shape of a crash
%% mid-write. The error tuples mean the bytes are present but wrong; callers
%% (wal_recovery) treat the two differently.
-type decode_error() :: truncated
                      | {error, {bad_magic, byte()}}
                      | {error, {unsupported_vsn, byte()}}
                      | {error, {vsn_mismatch, byte(), term()}}
                      | {error, {crc_mismatch, non_neg_integer(), non_neg_integer()}}
                      | {error, {bad_body, term()}}.

-export_type([decoded/0, decode_error/0]).

-spec current_vsn() -> byte().
current_vsn() -> ?VSN.

-spec header_size() -> non_neg_integer().
header_size() -> ?HEADER_SIZE.

-spec encode(#broker_message{}) -> binary().
encode(#broker_message{} = Msg) ->
    Body = term_to_binary(body_tuple(?VSN, Msg), [{compressed, 1}]),
    <<?MAGIC:8, ?VSN:8, (erlang:crc32(Body)):32, (byte_size(Body)):32, Body/binary>>.

-spec record_size(#broker_message{}) -> non_neg_integer().
record_size(#broker_message{} = Msg) ->
    byte_size(encode(Msg)).

-spec decode(binary(), partition_id()) -> decoded() | decode_error().
decode(<<Magic:8, _Vsn:8, _Crc:32, _Len:32, _Rest/binary>>, _PartitionId)
        when Magic =/= ?MAGIC ->
    {error, {bad_magic, Magic}};
decode(<<?MAGIC:8, Vsn:8, Crc:32, Len:32, Rest/binary>>, PartitionId)
        when byte_size(Rest) >= Len ->
    <<Body:Len/binary, _Tail/binary>> = Rest,
    case erlang:crc32(Body) of
        Crc -> decode_body(Vsn, Body, PartitionId, ?HEADER_SIZE + Len);
        Actual -> {error, {crc_mismatch, Crc, Actual}}
    end;
%% Either the header itself is incomplete or Len runs past the end of what was
%% written: both are a torn tail.
decode(_Incomplete, _PartitionId) ->
    truncated.

%%%_* Internal -----------------------------------------------------------

-spec body_tuple(byte(), #broker_message{}) -> tuple().
body_tuple(Vsn, #broker_message{message_id = MsgId,
                                tenant_id = Tenant,
                                destination = Destination,
                                payload = Payload,
                                payload_encoding = PayloadEncoding,
                                persistence = Persistence,
                                proto_meta = ProtoMeta,
                                headers = Headers,
                                reliability = Reliability,
                                created_at = CreatedAt,
                                expires_at = ExpiresAt,
                                event = Event}) ->
    {Vsn, MsgId, Tenant, Destination, Payload, PayloadEncoding, Persistence,
     ProtoMeta, Headers, Reliability, CreatedAt, ExpiresAt, Event}.

decode_body(Vsn, Body, PartitionId, Size) when Vsn =:= ?VSN ->
    try binary_to_term(Body, [safe]) of
        {?VSN, MsgId, Tenant, Destination, Payload, PayloadEncoding, Persistence,
         ProtoMeta, Headers, Reliability, CreatedAt, ExpiresAt, EventBlob} ->
            {ok, #broker_message{message_id = MsgId,
                                 tenant_id = Tenant,
                                 destination = Destination,
                                 partition_id = PartitionId,
                                 payload = Payload,
                                 payload_encoding = PayloadEncoding,
                                 headers = Headers,
                                 proto_meta = ProtoMeta,
                                 persistence = Persistence,
                                 reliability = Reliability,
                                 created_at = CreatedAt,
                                 expires_at = ExpiresAt,
                                 event = EventBlob}, Size};
        {BodyVsn, _, _, _, _, _, _, _, _, _, _, _, _} ->
            {error, {vsn_mismatch, Vsn, BodyVsn}};
        Other ->
            {error, {bad_body, Other}}
    catch
        _:Reason ->
            {error, {bad_body, Reason}}
    end;
%% §11.5: a record written by a newer format is refused, never guessed at.
%% wal_recovery turns this into "refuse to start and alarm".
decode_body(Vsn, _Body, _PartitionId, _Size) ->
    {error, {unsupported_vsn, Vsn}}.
