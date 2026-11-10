%% @doc `mqtt_transport' over QUIC: the MQTT engine's view of one bidi stream.
%%
%% OASIS MQTT over QUIC (Single Stream Mode, CN Draft 3) says the MQTT packet
%% format does not change at all: one MQTT Network Connection travels on **one
%% client-initiated bidirectional stream**, and closing that stream is not
%% closing the connection. So the engine is the same `mqtt_conn_fsm' that serves
%% 1883 and 8083; only the four transport callbacks differ.
%%
%% The handle is the **stream handle**, not the connection: every read/write
%% belongs to one stream, and a client that reconnects opens a new one on the
%% same connection.
%%
%% `decode_msg/2' is where this transport differs from TCP/WS, and the whole
%% difference is one discrimination: quicer delivers payload and events into the
%% **same mailbox**, both shaped `{quic, X, Stream, Props}'. Only the type of
%% `X' separates them --
%%
%%   * `X' a binary  -> payload for this stream;
%%   * `X' an atom   -> an event (`closed', `peer_send_shutdown',
%%                      `stream_closed', `send_shutdown_complete', ...).
%%
%% Feeding `{quic, send_shutdown_complete, Stream, _}' to `mqtt_frame' would be
%% "half of a packet is a protocol violation", which is exactly the kind of bug
%% that looks like fragmentation under load.
%%
%% It also matches on the stream handle, like the TCP transport matches on the
%% socket: one node runs many connections and one stream's bytes must never
%% reach another's state machine.
-module(mqtt_transport_quic).
-behaviour(mqtt_transport).

-export([send/2, arm_once/1, close/1, decode_msg/2]).

%% @doc MQTT bytes -> one QUIC stream write.
-spec send(term(), iodata()) -> ok | {error, term()}.
send(Stream, IoData) ->
    mqtt_quic_nif:send(Stream, iolist_to_binary(IoData)).

%% @doc `active, once' on the stream, so the FSM's own mailbox stays bounded.
%%
%% The atom matters: quicer reads an **integer** here as `{active, N}' (multi
%% mode), so `active, 1' would arm a counter that drops to zero after one
%% delivery and *then* sends `{quic, passive, Stream, undefined}' to the owner.
%% The MQTT engine has no idea what that message is, so every read would log
%% "unexpected event" and bump `protocol_errors' -- a transport detail leaking
%% into the protocol layer. `once' arms the same one-delivery arm with no
%% follow-up event (`quicer_stream.c', ACCEPTOR_RECV_MODE_ONCE vs ..._MULTI).
-spec arm_once(term()) -> ok | {error, term()}.
arm_once(Stream) ->
    mqtt_quic_nif:setopt(Stream, active, once).

%% @doc Close our sending side. A failure means the stream is already gone,
%% which is what `close/1' was asking for anyway.
-spec close(term()) -> ok.
close(Stream) ->
    _ = mqtt_quic_nif:close_stream(Stream),
    ok.

%% @doc Normalise quicer's messages. Payload vs event is decided by type, and
%% only this stream's messages are claimed.
-spec decode_msg(term(), term()) -> mqtt_transport:decision().
decode_msg({quic, Bin, Stream, _Props}, Stream) when is_binary(Bin) ->
    {data, Bin};
decode_msg({quic, Event, Stream, _Props}, Stream) when is_atom(Event) ->
    quic_event(Event);
decode_msg(_Msg, _Handle) ->
    other.

%% `closed' and `peer_send_shutdown' mean the peer will send no more: a normal
%% end of that MQTT connection. `stream_closed' is the same thing arriving after
%% the shutdown, and `shutdown_complete' is about *our* close being done -- none
%% of them is an error, and none of them closes the QUIC connection either
%% (CN Draft 3: the client may open a new stream and CONNECT again).
quic_event(closed) -> closed;
quic_event(peer_send_shutdown) -> closed;
quic_event(stream_closed) -> closed;
quic_event(send_shutdown_complete) -> other;
quic_event(_Other) -> other.
