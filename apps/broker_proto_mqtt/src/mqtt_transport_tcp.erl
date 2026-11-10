%% @doc `mqtt_transport' over a socket: plain (`ranch_tcp') or TLS (`ranch_ssl').
%%
%% Both are covered by one module on purpose. They differ only in which module
%% writes and which message shapes arrive in the mailbox, and the MQTT engine
%% above has no reason to know the difference. The dispatch is `is_port/1': a
%% `gen_tcp' socket is a port and an `ssl:sslsocket()' is a tuple. Note that the
%% two modules do NOT mirror each other -- `ssl' exports `setopts/2' while
%% `gen_tcp' only has `inet:setopts/2' (TLS sockets are still `inet' sockets) --
%% so arming is spelled per transport instead of through a shared call.
%%
%% This is also the fix for MQTT over TLS. Before S8 the FSM called
%% `gen_tcp:send/2' and `inet:setopts/2' on whatever socket ranch handed it, so
%% an ssl socket made every write fail and the connection simply died -- the
%% existing `broker_security_tls_SUITE' only ever proved that an `ssl' listener
%% could be created, never that an MQTT frame crossed it.
-module(mqtt_transport_tcp).
-behaviour(mqtt_transport).

-export([send/2, arm_once/1, close/1, decode_msg/2]).

-spec send(port() | ssl:sslsocket() | undefined, iodata()) -> ok | {error, term()}.
send(undefined, _IoData) ->
    {error, einval};
send(Socket, IoData) ->
    (impl(Socket)):send(Socket, IoData).

%% `undefined' is the state the FSM holds until the socket handoff has completed
%% (and forever, for a process that never receives one). Answering `einval' here
%% keeps that a refusal the FSM can log, not a `function_clause' crash that
%% buries the real cause in a supervisor report.
-spec arm_once(port() | ssl:sslsocket() | undefined) -> ok | {error, term()}.
arm_once(undefined) ->
    {error, einval};
arm_once(Socket) when is_port(Socket) ->
    inet:setopts(Socket, [{active, once}]);
arm_once(Socket) ->
    ssl:setopts(Socket, [{active, once}]).

-spec close(port() | ssl:sslsocket() | undefined) -> ok.
close(undefined) ->
    ok;
close(Socket) ->
    _ = (impl(Socket)):close(Socket),
    ok.

%% The socket in the message is matched against OUR handle rather than ignored:
%% a node running two listeners must never feed one connection's bytes into
%% another's state machine.
-spec decode_msg(term(), port() | ssl:sslsocket()) -> mqtt_transport:decision().
decode_msg({tcp, Socket, Data}, Socket) when is_binary(Data) ->
    {data, Data};
decode_msg({ssl, Socket, Data}, Socket) when is_binary(Data) ->
    {data, Data};
decode_msg({tcp_closed, Socket}, Socket) ->
    closed;
decode_msg({ssl_closed, Socket}, Socket) ->
    closed;
decode_msg({tcp_error, Socket, Reason}, Socket) ->
    {error, Reason};
decode_msg({ssl_error, Socket, Reason}, Socket) ->
    {error, Reason};
decode_msg(_Other, _Socket) ->
    other.

impl(Socket) when is_port(Socket) -> gen_tcp;
impl(_Socket) -> ssl.
