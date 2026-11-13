%% @doc Server-side signaling API (S10-B, R-S6).
%%
%% Host applications (and, in S10-C, the ICE side) can address a room or a single
%% peer without a client having to ask first. Three functions, each with a
%% defined answer for "that peer is not here":
%%
%%   * `peers/1' -- `[member()]', `[]' for an unknown room.
%%   * `publish/2,3' -- broadcast to a room; count of peers written to.
%%   * `send/2,3' -- to one peer; `{error, unknown_peer}', never `badarg'.
%%
%% Everything goes through the same in-memory path as a client signal: a plain
%% `Pid ! {text, Frame}' (ADR-028 D1/D3). No `#broker_message{}', no queue, no
%% router -- so a server-published event is as ephemeral as a client one, which
%% is exactly the contract.
-module(webrtc_server).

-export([peers/1, publish/2, publish/3, send/3, send/4]).

%% @doc Pids and identities currently in `Room'.
-spec peers(binary()) -> [webrtc_room_registry:member()].
peers(Room) ->
    webrtc_room_registry:peers(Room).

%% @doc Broadcast `{"event":Event}' to everyone in `Room'. Returns how many
%% connections the frame was handed to (0 for an empty or unknown room).
-spec publish(binary(), atom()) -> non_neg_integer().
publish(Room, Event) ->
    publish(Room, Event, undefined).

%% @doc Broadcast `{"event":Event,"data":Data}' (`Data => undefined' means no
%% `data' field, matching `text_event/1').
-spec publish(binary(), atom(), map() | undefined) -> non_neg_integer().
publish(Room, Event, Data) ->
    Frame = event_frame(Event, Data),
    Members = webrtc_room_registry:peers(Room),
    _ = [deliver(Member, Frame) || Member <- Members],
    length(Members).

%% @doc Send one event to one peer, addressed by `peer_id' *within `Room'*.
%%
%% The room is required on purpose: `peer_id' alone would let a caller (or a
%% misconfigured host app) reach into another room, which is the isolation
%% violation R-X4 forbids. `{error, unknown_peer}' is the one answer for "no
%% such peer here", so the return value is not an existence oracle either.
-spec send(binary(), binary(), atom()) -> ok | {error, unknown_peer}.
send(Room, PeerId, Event) ->
    send(Room, PeerId, Event, undefined).

-spec send(binary(), binary(), atom(), map() | undefined) ->
          ok | {error, unknown_peer}.
send(Room, PeerId, Event, Data) ->
    case webrtc_room_registry:member(Room, PeerId) of
        undefined ->
            {error, unknown_peer};
        Member ->
            deliver(Member, event_frame(Event, Data))
    end.

%% ---- internals ----------------------------------------------------------

%% `webrtc_signal' builds the five events the *server itself* emits, each with a
%% frozen payload. A host-published event is an arbitrary sixth one, so its two
%% shapes are built here rather than widening `webrtc_signal' with an untyped
%% constructor that would blur which events carry a server-defined payload.
event_frame(Event, undefined) ->
    {text, iolist_to_binary(json:encode(#{event => Event}))};
event_frame(Event, Data) when is_map(Data) ->
    {text, iolist_to_binary(json:encode(#{event => Event, data => Data}))}.

deliver(#{pid := Pid}, Frame) ->
    Pid ! Frame,
    ok.
