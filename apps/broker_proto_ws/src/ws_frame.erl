%% @doc RFC 6455 frame codec (server side).
%%
%% The adapter owns its framer instead of pulling a WebSocket dependency: the
%% only transport feature used is RFC 6455 itself, and the S5 contract requires
%% a documented, testable byte-level surface. Control frames (close/ping/pong)
%% are validated here; fragmentation is surfaced to the connection process,
%% which reassembles it (a data frame with FIN=0 followed by continuation
%% frames).
-module(ws_frame).

-export([decode/2, encode/2, encode/3, close_payload/2, close_code/1]).

-export_type([frame/0]).

-type opcode() :: 0..15.
-type frame() :: #{fin := boolean(),
                   opcode := opcode(),
                   masked := boolean(),
                   payload := binary()}.

-define(DEFAULT_MAX, 16777216).

%% @doc Decode one frame from `Buffer'.
%%
%% `{more, Need}' means the buffer is a strict prefix of a frame and at least
%% `Need' more bytes are required. `{error, Reason}' is a protocol violation the
%% connection must answer with a close frame (1002) -- never a silent skip.
-spec decode(binary(), non_neg_integer()) ->
          {ok, frame(), binary()} | {more, non_neg_integer()} | {error, term()}.
decode(Buffer, Max) when is_binary(Buffer), is_integer(Max), Max > 0 ->
    case Buffer of
        <<Fin:1, Rsv:3, Op:4, Mask:1, Len0:7, Rest/binary>> ->
            case Rsv of
                0 -> decode1(Fin, Op, Mask, Len0, Rest, Max);
                _ -> {error, {reserved_bits, Rsv}}
            end;
        _ ->
            {more, 2 - byte_size(Buffer)}
    end.

decode1(Fin, Op, Mask, Len0, Rest, Max) ->
    case Op of
        _ when Op =:= 0; Op =:= 1; Op =:= 2; Op =:= 8; Op =:= 9; Op =:= 10 ->
            decode2(Fin, Op, Mask, Len0, Rest, Max);
        _ ->
            {error, {unknown_opcode, Op}}
    end.

decode2(Fin, Op, Mask, Len0, Rest, Max) ->
    Control = Op >= 8,
    case Control andalso (Fin =/= 1 orelse Len0 > 125) of
        true ->
            {error, {invalid_control_frame, #{fin => Fin, len => Len0}}};
        false ->
            case payload_length(Len0, Rest) of
                {more, Need} ->
                    {more, Need};
                {ok, Len, _Rest1, _HeaderBytes} when Len > Max ->
                    {error, {payload_too_large, Len, Max}};
                {ok, Len, Rest1, _HeaderBytes} ->
                    decode_payload(Fin, Op, Mask, Len, Rest1)
            end
    end.

payload_length(Len0, Rest) when Len0 < 126 ->
    {ok, Len0, Rest, 0};
payload_length(126, Rest) ->
    case Rest of
        <<Len:16, Rest1/binary>> -> {ok, Len, Rest1, 2};
        _ -> {more, 2 - byte_size(Rest)}
    end;
payload_length(127, Rest) ->
    case Rest of
        <<Len:64, Rest1/binary>> when Len =< 16#7FFFFFFFFFFFFFFF -> {ok, Len, Rest1, 8};
        <<_:64, _/binary>> -> {error, {invalid_length, 64}};
        _ -> {more, 8 - byte_size(Rest)}
    end.

decode_payload(Fin, Op, 0, Len, Rest) ->
    case Rest of
        <<Payload:Len/binary, Tail/binary>> ->
            {ok, #{fin => Fin =:= 1, opcode => Op, masked => false,
                   payload => Payload}, Tail};
        _ ->
            {more, Len - byte_size(Rest)}
    end;
decode_payload(Fin, Op, 1, Len, Rest) ->
    case Rest of
        <<Key:4/binary, Payload:Len/binary, Tail/binary>> ->
            {ok, #{fin => Fin =:= 1, opcode => Op, masked => true,
                   payload => unmask(Key, Payload)}, Tail};
        _ ->
            {more, 4 + Len - byte_size(Rest)}
    end.

unmask(Key, Payload) ->
    Size = byte_size(Payload),
    Stream = binary:copy(Key, (Size div 4) + 1),
    crypto:exor(Payload, binary:part(Stream, 0, Size)).

%% @doc Server to client frame, unmasked.
-spec encode(opcode(), iodata()) -> iodata().
encode(Op, Payload) ->
    encode(Op, Payload, false).

%% @doc `Mask = true' is only for tests and client-side helpers; a server must
%% send unmasked frames (RFC 6455 §5.1).
-spec encode(opcode(), iodata(), boolean()) -> iodata().
encode(Op, Payload0, Mask) ->
    Payload = iolist_to_binary(Payload0),
    Len = byte_size(Payload),
    {LenBits, Ext} =
        if
            Len < 126 -> {Len, <<>>};
            Len < 65536 -> {126, <<Len:16>>};
            true -> {127, <<Len:64>>}
        end,
    MaskBit = case Mask of true -> 1; false -> 0 end,
    {Key, Body} =
        case Mask of
            true ->
                K = crypto:strong_rand_bytes(4),
                {K, unmask(K, Payload)};
            false ->
                {<<>>, Payload}
        end,
    [<<1:1, 0:3, Op:4, MaskBit:1, LenBits:7>>, Ext, Key, Body].

%% @doc Close frame payload: 2-byte code + UTF-8 reason.
-spec close_payload(non_neg_integer(), binary()) -> binary().
close_payload(Code, Reason) when is_integer(Code), Code >= 1000, Code =< 4999 ->
    Reason1 = binary:part(Reason, 0, min(byte_size(Reason), 123)),
    <<Code:16, Reason1/binary>>.

%% @doc Extract the close code from a peer's close payload; absent payload means
%% 1005 (no status received), which RFC 6455 forbids on the wire but clients do
%% send an empty payload.
-spec close_code(binary()) -> non_neg_integer().
close_code(<<>>) -> 1005;
close_code(<<Code:16, _/binary>>) -> Code;
close_code(_) -> 1002.
