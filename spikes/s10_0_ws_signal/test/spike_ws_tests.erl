%% S10-0 spike: assertions on the wire codec, runnable without a listener.
%%
%% Everything here is a claim REPORT.md makes about `broker_ws', turned into a
%% test. If `broker_ws' changes shape in a later phase, this file fails and
%% S10-A knows before writing a connection module against a moved target.
-module(spike_ws_tests).
-include_lib("eunit/include/eunit.hrl").

-define(HEAD, <<"GET /websocket/room1 HTTP/1.1\r\n"
                "Host: 127.0.0.1\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                "Sec-WebSocket-Version: 13\r\n"
                "Sec-WebSocket-Protocol: webrtc.v1\r\n"
                "\r\n">>).

%% %%%_* Handshake -----------------------------------------------------------

subprotocol_is_echoed_test() ->
    {ok, Resp, Sub} = ws_handshake:accept(?HEAD, #{supported_subprotocols => [<<"webrtc.v1">>]}),
    ?assertEqual(<<"webrtc.v1">>, Sub),
    Bin = iolist_to_binary(Resp),
    ?assertMatch({_, _}, binary:match(Bin, <<"Sec-WebSocket-Protocol: webrtc.v1">>)).

%% The accept digest is a fixed function of the key; pinning it means a
%% regression in the crypto call is caught here, not by a browser.
accept_digest_matches_rfc6455_example_test() ->
    {ok, Resp, _} = ws_handshake:accept(?HEAD, #{supported_subprotocols => [<<"webrtc.v1">>]}),
    Bin = iolist_to_binary(Resp),
    ?assertNotEqual(nomatch, binary:match(Bin, <<"s3pPLMBiTxaQ9kYGzzhZRbK+xOo=">>)).

%% No common subprotocol must be refused, not silently upgraded (R-T6).
no_common_subprotocol_is_400_test() ->
    {error, Resp} = ws_handshake:accept(?HEAD, #{supported_subprotocols => [<<"chat">>]}),
    Bin = iolist_to_binary(Resp),
    ?assertNotEqual(nomatch, binary:match(Bin, <<"400 Bad Request">>)).

%% A server must not offer a protocol the client never asked for.
absent_option_omits_header_test() ->
    {ok, Resp, Sub} = ws_handshake:accept(?HEAD, #{}),
    ?assertEqual(undefined, Sub),
    Bin = iolist_to_binary(Resp),
    ?assertEqual(nomatch, binary:match(Bin, <<"Sec-WebSocket-Protocol">>)).

%% %%%_* Frames -------------------------------------------------------------

text_frame_decodes_test() ->
    Frame = <<16#81, 3, "abc">>,
    {ok, #{fin := true, opcode := 1, payload := <<"abc">>}, <<>>} =
        ws_frame:decode(Frame, 262144).

%% Binary is a protocol decision, not a codec limitation: the codec reports it
%% and the connection must answer 1003 (R-T3).
binary_frame_decodes_with_opcode_2_test() ->
    {ok, #{opcode := 2, payload := <<"hi">>}, <<>>} = ws_frame:decode(<<16#82, 2, "hi">>, 262144).

native_ping_decodes_with_opcode_9_test() ->
    {ok, #{opcode := 9}, <<>>} = ws_frame:decode(<<16#89, 0>>, 262144).

partial_frame_reports_needed_bytes_test() ->
    ?assertMatch({more, _}, ws_frame:decode(<<16#81>>, 262144)).

oversize_frame_is_error_test() ->
    ?assertMatch({error, {payload_too_large, _, _}},
                 ws_frame:decode(<<16#81, 126, 1000:16>>, 16)).

%% Reserved bits are a protocol violation, never silently dropped.
reserved_bits_are_error_test() ->
    ?assertMatch({error, {reserved_bits, _}}, ws_frame:decode(<<16#C1, 0>>, 262144)).
