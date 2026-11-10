%% @doc RFC 6455 frame codec (S5 WebSocket adapter).
-module(ws_frame_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [encode_decode_roundtrip,
     short_and_long_lengths,
     masked_client_frame_is_unmasked,
     partial_buffer_asks_for_more,
     control_frames_are_validated,
     oversized_payload_is_rejected,
     reserved_bits_are_rejected,
     unknown_opcode_is_rejected,
     close_payload_roundtrip].

encode_decode_roundtrip(_Config) ->
    P = <<"hello">>,
    Bin = iolist_to_binary(ws_frame:encode(1, P)),
    {ok, #{fin := true, opcode := 1, masked := false, payload := P}, <<>>} =
        ws_frame:decode(Bin, 1024),
    ok.

short_and_long_lengths(_Config) ->
    lists:foreach(
      fun(Size) ->
              P = binary:copy(<<"x">>, Size),
              Bin = iolist_to_binary(ws_frame:encode(2, P)),
              {ok, #{payload := P, opcode := 2}, <<>>} = ws_frame:decode(Bin, 200000)
      end, [0, 1, 125, 126, 127, 65535, 65536, 70000]),
    ok.

masked_client_frame_is_unmasked(_Config) ->
    P = <<"masked payload">>,
    Bin = iolist_to_binary(ws_frame:encode(1, P, true)),
    {ok, #{masked := true, payload := P}, <<>>} = ws_frame:decode(Bin, 1024),
    ok.

partial_buffer_asks_for_more(_Config) ->
    P = <<"abcdefghij">>,
    Bin = iolist_to_binary(ws_frame:encode(1, P)),
    lists:foreach(
      fun(N) ->
              Prefix = binary:part(Bin, 0, N),
              {more, _} = ws_frame:decode(Prefix, 1024)
      end, lists:seq(0, byte_size(Bin) - 1)),
    ok.

control_frames_are_validated(_Config) ->
    %% A fragmented ping or a ping longer than 125 bytes is a protocol error.
    BadFin = <<0:1, 0:3, 9:4, 0:1, 0:7>>,
    {error, {invalid_control_frame, _}} = ws_frame:decode(BadFin, 1024),
    BadLen = <<1:1, 0:3, 9:4, 0:1, 126:7, 200:16>>,
    {error, {invalid_control_frame, _}} = ws_frame:decode(BadLen, 1024),
    Pong = iolist_to_binary(ws_frame:encode(10, <<"p">>)),
    {ok, #{opcode := 10}, <<>>} = ws_frame:decode(Pong, 1024),
    ok.

oversized_payload_is_rejected(_Config) ->
    Bin = iolist_to_binary(ws_frame:encode(2, binary:copy(<<"x">>, 2000))),
    {error, {payload_too_large, 2000, 1024}} = ws_frame:decode(Bin, 1024),
    ok.

reserved_bits_are_rejected(_Config) ->
    {error, {reserved_bits, 1}} = ws_frame:decode(<<1:1, 1:3, 1:4, 0:1, 0:7>>, 1024),
    ok.

unknown_opcode_is_rejected(_Config) ->
    {error, {unknown_opcode, 3}} = ws_frame:decode(<<1:1, 0:3, 3:4, 0:1, 0:7>>, 1024),
    ok.

close_payload_roundtrip(_Config) ->
    P = ws_frame:close_payload(1000, <<"bye">>),
    <<1000:16, "bye">> = P,
    1000 = ws_frame:close_code(P),
    1005 = ws_frame:close_code(<<>>),
    1002 = ws_frame:close_code(<<1>>),
    ok.
