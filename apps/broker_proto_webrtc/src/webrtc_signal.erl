%% @doc Wire formats for the five signaling events (S10-B).
%%
%% The exact JSON of `docs/design/s10-webrtc-signaling.md' §2.3, built with the
%% `text_event/1,2' shape the plan mandates:
%%
%%   text_event(Event)        -> `{"event":<atom>}'                 (no data)
%%   text_event(Event, Data)  -> `{"event":<atom>,"data":<map>}'
%%
%% Server events are always WebSocket **text** frames. `unauthorized' and
%% `invalid_message' carry **no** `data' field; `authenticated', `joined' and
%% `left' carry exactly the three maps below -- no extra keys, because the
%% compatibility matrix quotes these bytes.
%%
%% Note what is *not* here: the relayed client signal. It is not a server
%% event, it is the client's own message re-encoded with a `from' added
%% (`webrtc_dispatch'), so its `event' name and `data' payload stay opaque.
-module(webrtc_signal).

-export([authenticated/1, unauthorized/0, invalid_message/0,
         joined/2, left/2]).

%% @doc `{"event":"authenticated","data":{"peer_id":<id>}}' -- reply to the
%% requester only. `peer_id' is server-minted (R-S7), never client-chosen.
-spec authenticated(binary()) -> {text, binary()}.
authenticated(PeerId) ->
    text_event(authenticated, #{peer_id => PeerId}).

%% @doc `{"event":"unauthorized"}' -- no data (bad credential / bad JSON /
%% bad frame / room refused by ACL).
-spec unauthorized() -> {text, binary()}.
unauthorized() ->
    text_event(unauthorized).

%% @doc `{"event":"invalid_message"}' -- no data. Covers every authenticated
%% message the server will not relay: missing `to', unknown `to', a `to' in
%% another room, a room that is full, and an ACL refusal on one signal.
-spec invalid_message() -> {text, binary()}.
invalid_message() ->
    text_event(invalid_message).

%% @doc `{"event":"joined","data":{"username":<name>,"peer_id":<id>}}' -- to
%% the members that were already in the room (R-S3). The joiner itself gets
%% nothing: it learns its peers from the `from' field of the first signal it
%% receives, which is exactly the upstream browser flow (§2.4).
-spec joined(binary(), binary()) -> {text, binary()}.
joined(Username, PeerId) ->
    text_event(joined, #{username => Username, peer_id => PeerId}).

%% @doc `{"event":"left","data":{"username":<name>,"peer_id":<id>}}' -- to the
%% members that remain (R-S3: the leaving connection is not a recipient).
-spec left(binary(), binary()) -> {text, binary()}.
left(Username, PeerId) ->
    text_event(left, #{username => Username, peer_id => PeerId}).

%% `json:encode/1' returns iodata in OTP 28, and these frames travel to other
%% processes (`Pid ! {text, Bin}'); making them binaries here means every
%% consumer can match on a binary and no one has to know the codec's return
%% shape.
text_event(Event) when is_atom(Event) ->
    {text, iolist_to_binary(json:encode(#{event => Event}))}.

text_event(Event, Data) when is_atom(Event), is_map(Data) ->
    {text, iolist_to_binary(json:encode(#{event => Event, data => Data}))}.
