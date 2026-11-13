%% @doc ranch 2.x protocol adapter for the WebRTC signaling listener.
%%
%% One process per connection. `webrtc_conn' owns the socket, performs the
%% RFC 6455 opening handshake, and turns frames into signaling events --
%% which, in S10-A, are nothing but replies to the same connection (the
%% `to' face lands in S10-B behind this exact module).
%%
%% The returned pid IS the connection process (a gen_server): ranch monitors
%% it and sends it the `{handshake, Ref, Transport, Socket, Timeout}' message.
%% Wrapping it in another `spawn_link' would hand ranch a pid that exits
%% immediately, and ranch would then tear the listener down.
-module(webrtc_listener).
-behaviour(ranch_protocol).

-export([start_link/3]).

-spec start_link(ranch:ref(), module(), map()) -> {ok, pid()} | {error, term()}.
start_link(Ref, Transport, Opts) ->
    webrtc_conn:start_link(Ref, Transport, Opts).
