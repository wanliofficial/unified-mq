%% @doc Targeted signaling dispatch (S10-B): deliver a client signal to one peer
%% by `to'.
%%
%% The forwarded frame is the client's own message with a `from' field added --
%% `event' and `data' are never interpreted, never rewritten (R-S2), and the
%% `to' field is kept, exactly as the upstream handler does (§2.3, F-12). The
%% server only reads `to' to pick a pid.
%%
%% Addressing is process-direct: resolve `to' *inside the sender's room* via
%% the room registry and `Pid ! {text, Frame}' (ADR-028 D1/D3). No
%% `#broker_message{}', no `broker_queue', no `broker_router'.
%%
%% Room scoping is the whole isolation story (R-X4). `lookup/2' is per-room, so
%% a `to' naming a peer in another room is *indistinguishable* from a `to'
%% naming nobody: both are `{error, unknown_peer}'. That is deliberate -- a
%% distinct `cross_room' answer would let a client probe which peer ids exist
%% elsewhere.
-module(webrtc_dispatch).

-export([deliver/3, relay/3]).

%% @doc Relay `Message' (a decoded client message with `to') to that peer in
%% `Room'. `From' is the sender's server-minted peer_id.
%%
%% Errors are values, never exceptions: the connection stays up and turns them
%% into an `invalid_message' reply (R-S5, upstream F-1's correct handling).
-spec deliver(binary(), map(), binary()) -> ok | {error, term()}.
deliver(Room, Message, From) ->
    case maps:get(<<"to">>, Message, undefined) of
        To when is_binary(To), To =/= <<>> ->
            case maps:get(<<"event">>, Message, undefined) of
                Event when is_binary(Event), Event =/= <<>> ->
                    relay(Room, To, with_from(Message, From));
                _ ->
                    {error, missing_event}
            end;
        _ ->
            {error, missing_to}
    end.

%% @doc Send an already-addressed frame. Split out so `webrtc_server' and the
%% tests can exercise the addressing half without building a client message.
-spec relay(binary(), binary(), map()) -> ok | {error, unknown_peer}.
relay(Room, To, Message) when is_binary(To) ->
    case webrtc_room_registry:lookup(Room, To) of
        undefined -> {error, unknown_peer};
        Pid when is_pid(Pid) ->
            %% `{text, Frame}' is the shape ADR-028 D1 froze: a plain process
            %% message, indistinguishable from any other frame the connection
            %% writes out.
            Pid ! {text, encode(Message)},
            ok
    end.

%% ---- internals ----------------------------------------------------------

with_from(Message, From) when is_binary(From) ->
    Message#{from => From}.

%% The relayed frame is a re-encoding of the decoded JSON object. Values --
%% including the SDP text and the ICE candidate strings -- are carried through
%% unchanged; only JSON formatting is normalised, which is not part of the
%% contract (a client may send any whitespace and key order).
encode(Message) ->
    iolist_to_binary(json:encode(Message)).
