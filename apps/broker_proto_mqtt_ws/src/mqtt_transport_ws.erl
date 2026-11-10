%% @doc `mqtt_transport' over WebSocket: the MQTT engine's view of a WS session.
%%
%% The handle is the **pid of `mqtt_ws_conn'**, not a socket. That process owns
%% the socket, the RFC 6455 handshake and the frame codec; this module only
%% translates the engine's four callback calls into messages to it, and its
%% mailbox messages back into the four answers `mqtt_transport' defines.
%%
%% Two consequences worth stating, because both are deliberate:
%%
%%   * `arm_once/1' is a **no-op**. `mqtt_ws_conn' re-arms its socket by itself
%%     after every delivery, so there is no per-frame backpressure on this
%%     transport; the connection's `max_inflight' / `receive_maximum' windows
%%     remain the only flow control. The callback still exists because the FSM
%%     calls it on every path -- returning `ok' keeps that path uniform.
%%   * `send/2' is asynchronous (a message to the WS process, which writes the
%%     frame). An `{error, _}' therefore only reports a dead WS process, which
%%     the engine treats the same way it treats a dead socket: log, keep going,
%%     and let the closed notice arrive.
%%
%% `decode_msg/2' matches on the WS pid, exactly like `mqtt_transport_tcp' does
%% on the socket: a node runs many connections and one connection's bytes must
%% never reach another's state machine.
-module(mqtt_transport_ws).
-behaviour(mqtt_transport).

-export([send/2, arm_once/1, close/1, decode_msg/2]).

%% @doc MQTT bytes -> one binary WebSocket frame.
-spec send(pid(), iodata()) -> ok | {error, term()}.
send(WsPid, IoData) when is_pid(WsPid) ->
    WsPid ! {mqtt_ws_send, iolist_to_binary(IoData)},
    ok;
send(_Handle, _IoData) ->
    {error, einval}.

%% @doc Nothing to arm: `mqtt_ws_conn' drives its own read loop.
-spec arm_once(pid()) -> ok | {error, term()}.
arm_once(WsPid) when is_pid(WsPid) ->
    WsPid ! {mqtt_ws_arm, self()},
    ok;
arm_once(_Handle) ->
    {error, einval}.

%% @doc Ask the WS layer to send a normal close and shut the socket.
-spec close(pid()) -> ok.
close(WsPid) when is_pid(WsPid) ->
    WsPid ! mqtt_ws_close,
    ok;
close(_Handle) ->
    ok.

%% @doc Normalise the WS layer's messages. Only our own carrier is claimed.
-spec decode_msg(term(), term()) -> mqtt_transport:decision().
decode_msg({mqtt_ws_data, WsPid, Bin}, WsPid) when is_binary(Bin) ->
    {data, Bin};
decode_msg({mqtt_ws_closed, WsPid}, WsPid) ->
    closed;
decode_msg({mqtt_ws_error, WsPid, Reason}, WsPid) ->
    {error, Reason};
decode_msg(_Msg, _Handle) ->
    other.
