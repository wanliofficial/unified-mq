%% @doc The one seam between `mqtt_conn_fsm' and whatever carries its bytes.
%%
%% MQTT itself says nothing about the transport: 3.1.1 §6 and 5.0 §6 allow TCP,
%% TLS, WebSocket and (per the OASIS MQTT over QUIC single-stream note) QUIC,
%% and the packet format does not change. Until S8 the FSM hard-coded
%% `gen_tcp:send/2' and `inet:setopts/2', which had two consequences: the
%% ranch-provided transport was ignored (so MQTT over TLS could not work at all
%% -- every send went to `gen_tcp' with an ssl socket), and a second transport
%% would have meant a second copy of the state machine.
%%
%% So the FSM holds a module name plus an opaque handle and calls four
%% callbacks. Implementations live with the app that owns the dependency:
%% `mqtt_transport_tcp' here (it covers both ranch_tcp and ranch_ssl, which is
%% what fixes plain MQTT-over-TLS), while the WebSocket and QUIC ones belong to
%% their own apps so this one never depends on them.
%%
%% Only `decode_msg/2' has real semantics to get right: it normalises whatever
%% shape the transport puts in the process mailbox into four answers. Everything
%% else is a thin wrapper, and that is the point -- the FSM must not know
%% whether `{ssl, S, Data}' or `{quic, Bin, Stream, Props}' arrived.
-module(mqtt_transport).

-export_type([handle/0, decision/0, t/0]).

-type handle() :: term().
-type t() :: module().

%% `{data, Bin}': new bytes for the stream buffer. `closed': the peer is gone
%% and the connection ends normally (no error, no Will bypass -- the FSM decides
%% what a normal close means). `{error, Reason}': also a disconnect, logged.
%% `other': not addressed to this transport; the FSM falls through to its own
%% event matrix. That last case matters because the FSM's mailbox also carries
%% subscription deliveries and timers.
-type decision() :: {data, binary()} | closed | {error, term()} | other().
-type other() :: other.

-callback send(handle(), iodata()) -> ok | {error, term()}.
-callback arm_once(handle()) -> ok | {error, term()}.
-callback close(handle()) -> ok.
-callback decode_msg(term(), handle()) -> decision().
