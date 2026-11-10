%% @doc The QUIC transport layer's pure contract: what `decode_msg/2' answers for
%% each shape quicer can put in a connection's mailbox, and what the three
%% socket callbacks do when the NIF is not there.
%%
%% These cases need **no** quicer, no certificate and no network: `decode_msg/2'
%% is a pure function of `(Msg, Handle)', and the callbacks' no-NIF path is
%% exactly what a default-profile deployment sees. That is deliberate -- the
%% interesting part of this transport is a single discrimination (binary vs atom
%% in the *same* message shape), and it can be pinned without a UDP socket. The
%% real-wire proof lives in `test/protocol-compatibility/mqtt_quic/'.
-module(mqtt_quic_transport_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [a_binary_from_this_stream_is_data,
     an_empty_binary_is_still_data,
     a_closed_event_closes,
     a_peer_send_shutdown_closes,
     a_stream_closed_event_closes,
     a_send_shutdown_complete_is_not_data,
     an_unknown_event_is_other,
     another_streams_data_is_other,
     another_streams_event_is_other,
     a_non_quic_message_is_other,
     send_reports_a_missing_nif,
     arm_once_reports_a_missing_nif,
     close_tolerates_a_missing_nif].

%%%_* decode_msg/2 ------------------------------------------------------------

%% The MQTT packet itself. The props map is per-delivery metadata (offset,
%% flags) and must not influence the decision.
a_binary_from_this_stream_is_data(_Config) ->
    Stream = make_ref(),
    Props = #{flags => 0, len => 5, absolute_offset => 0},
    ?assertEqual({data, <<"hello">>},
                 mqtt_transport_quic:decode_msg(
                   {quic, <<"hello">>, Stream, Props}, Stream)).

%% An empty binary is a legal delivery (a zero-length send is a no-op, but a
%% reassembly edge can hand one over); it is data, not an event.
an_empty_binary_is_still_data(_Config) ->
    Stream = make_ref(),
    ?assertEqual({data, <<>>},
                 mqtt_transport_quic:decode_msg(
                   {quic, <<>>, Stream, #{}}, Stream)).

%% `closed' is the whole connection going away: no new stream can be opened on
%% it, so the MQTT session on this stream is over.
a_closed_event_closes(_Config) ->
    Stream = make_ref(),
    ?assertEqual(closed,
                 mqtt_transport_quic:decode_msg(
                   {quic, closed, Stream, #{}}, Stream)).

%% The peer finished *its* direction. In Single Stream Mode that is the end of
%% this MQTT Network Connection -- and, importantly, NOT the end of the QUIC
%% connection.
a_peer_send_shutdown_closes(_Config) ->
    Stream = make_ref(),
    ?assertEqual(closed,
                 mqtt_transport_quic:decode_msg(
                   {quic, peer_send_shutdown, Stream, undefined}, Stream)).

a_stream_closed_event_closes(_Config) ->
    Stream = make_ref(),
    ?assertEqual(closed,
                 mqtt_transport_quic:decode_msg(
                   {quic, stream_closed, Stream, false}, Stream)).

%% The one that looks like data and is not: `send_shutdown_complete' arrives
%% after *our* close and carries no bytes. Feeding it to `mqtt_frame' is the bug
%% this clause exists to prevent.
a_send_shutdown_complete_is_not_data(_Config) ->
    Stream = make_ref(),
    ?assertEqual(other,
                 mqtt_transport_quic:decode_msg(
                   {quic, send_shutdown_complete, Stream, false}, Stream)).

an_unknown_event_is_other(_Config) ->
    Stream = make_ref(),
    Events = [passive, start_completed, send_complete, peer_accepted,
              peer_send_aborted, peer_receive_aborted, streams_available,
              peer_needs_streams, dgram_state_changed, local_address_changed],
    lists:foreach(
      fun(Event) ->
              ?assertEqual(other,
                           mqtt_transport_quic:decode_msg(
                             {quic, Event, Stream, undefined}, Stream))
      end, Events).

%% One node runs many connections. A stream's bytes must never reach another
%% stream's state machine, so the handle match is part of the contract.
another_streams_data_is_other(_Config) ->
    Mine = make_ref(),
    Theirs = make_ref(),
    ?assertEqual(other,
                 mqtt_transport_quic:decode_msg(
                   {quic, <<"not mine">>, Theirs, #{}}, Mine)).

another_streams_event_is_other(_Config) ->
    Mine = make_ref(),
    Theirs = make_ref(),
    ?assertEqual(other,
                 mqtt_transport_quic:decode_msg(
                   {quic, closed, Theirs, #{}}, Mine)).

%% Everything else in the FSM's mailbox -- `broker_deliver', a poll tick, the
%% `connect' handoff -- is not the transport's business and must fall through.
a_non_quic_message_is_other(_Config) ->
    Stream = make_ref(),
    Messages = [connect,
                {broker_deliver, {p, 1}},
                {poll_t, make_ref()},
                {tcp, Stream, <<"not quic">>},
                {quic, <<"only three elements">>, Stream}],
    lists:foreach(
      fun(Msg) ->
              ?assertEqual(other, mqtt_transport_quic:decode_msg(Msg, Stream))
      end, Messages).

%%%_* Callbacks without the NIF -----------------------------------------------

%% In the default profile `quicer' is not a dependency at all, so these three
%% callbacks are the ones a misplaced QUIC listener would hit. The requirement is
%% an actionable error, not an `undef' crash inside a supervisor.
send_reports_a_missing_nif(_Config) ->
    case mqtt_quic_nif:available() of
        true ->
            {skip, "quicer is available in this profile"};
        false ->
            ?assertEqual({error, quicer_not_available},
                         mqtt_transport_quic:send(make_ref(), <<"x">>))
    end.

arm_once_reports_a_missing_nif(_Config) ->
    case mqtt_quic_nif:available() of
        true ->
            {skip, "quicer is available in this profile"};
        false ->
            ?assertEqual({error, quicer_not_available},
                         mqtt_transport_quic:arm_once(make_ref()))
    end.

%% `close/1' is documented to swallow a close failure -- asking to close a
%% stream that is already gone is asking for the state it is already in.
close_tolerates_a_missing_nif(_Config) ->
    ?assertEqual(ok, mqtt_transport_quic:close(make_ref())).
