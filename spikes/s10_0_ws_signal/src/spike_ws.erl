%% S10-0 spike: a WebSocket signaling listener built ONLY from what the repo
%% already has (ranch 2.3.0 + broker_ws's ws_handshake / ws_frame).
%%
%% Purpose is measurement, not product shape. Every interesting event is
%% reported to the probe process as {ev, Term} so REPORT.md can quote real
%% numbers instead of guesses:
%%   * exact 101 response bytes and their length for a `webrtc.v1' handshake
%%   * subprotocol refusal (no common protocol -> 400, never a silent upgrade)
%%   * text `ping' -> text `pong' AND native opcode 9 -> 10, coexisting
%%   * binary frame -> close 1003
%%   * `to' dispatch with plain process sends (ADR-028 D3), no router/queue
-module(spike_ws).
-behaviour(ranch_protocol).

-export([start_link/3, ranch_handles/1, init/4]).

-define(SUBPROTOCOLS, [<<"webrtc.v1">>]).
-define(MAX_FRAME, 262144).
-define(ROOM, <<"room1">>).

start_link(Ref, Transport, TransportOpts) ->
    Owner = probe(),
    Pid = proc_lib:spawn_link(?MODULE, init, [Ref, Transport, TransportOpts, Owner]),
    {ok, Pid}.

ranch_handles(_Info) -> [conn_info].

probe() ->
    case whereis(spike_probe_srv) of
        undefined -> self();
        P -> P
    end.

init(Ref, Transport, _Opts, Owner) ->
    {ok, Socket} = ranch:handshake(Ref),
    ok = Transport:setopts(Socket, [{active, false}, {packet, raw}, binary]),
    case handshake(Socket, Transport, <<>>, Owner) of
        {ok, Subprotocol} ->
            PeerId = mint_peer_id(),
            ok = join(Socket, Transport, PeerId),
            Owner ! {ev, {connected, PeerId, Subprotocol}},
            try
                loop(Socket, Transport, Owner,
                     #{peer_id => PeerId, authenticated => false})
            after
                %% R-C2: membership is connection state -- a dying connection
                %% leaves the room. A real registry also monitors and fans out
                %% `left' (S10-B); the spike only needs the entry gone.
                leave(PeerId),
                Owner ! {ev, {left, PeerId}}
            end;
        {error, Why} ->
            Owner ! {ev, {handshake_refused, Why}},
            Transport:close(Socket)
    end.

%% R-S7: the server mints the id; a client can never choose or forge it.
mint_peer_id() ->
    <<"P", (base64:encode(crypto:strong_rand_bytes(9)))/binary>>.

%% Room membership lives in a public ETS table (the spike's stand-in for
%% `webrtc_room_registry', ADR-028 D4). Values are {Pid, Socket, Transport}.
join(Socket, Transport, PeerId) ->
    true = ets:insert(spike_room, {PeerId, self(), Socket, Transport}),
    ok.

leave(PeerId) ->
    _ = ets:delete(spike_room, PeerId),
    ok.

peers() ->
    [{Id, P, S, T} || {Id, P, S, T} <- ets:tab2list(spike_room)].

handshake(Socket, Transport, Acc, _Owner) when byte_size(Acc) > 16384 ->
    _ = Transport:send(Socket, <<"HTTP/1.1 431 Request Header Fields Too Large\r\n"
                                 "Connection: close\r\n\r\n">>),
    Transport:close(Socket),
    {error, head_too_large};
handshake(Socket, Transport, Acc, Owner) ->
    case binary:match(Acc, <<"\r\n\r\n">>) of
        nomatch ->
            case Transport:recv(Socket, 0, 5000) of
                {ok, More} -> handshake(Socket, Transport, <<Acc/binary, More/binary>>, Owner);
                {error, Why} -> {error, {recv, Why}}
            end;
        _ ->
            case ws_handshake:accept(Acc, #{supported_subprotocols => ?SUBPROTOCOLS}) of
                {ok, Response, Sub} ->
                    RespBin = iolist_to_binary(Response),
                    ok = Transport:send(Socket, RespBin),
                    Owner ! {ev, {handshake, byte_size(RespBin), RespBin, Sub}},
                    {ok, Sub};
                {error, Response} ->
                    RespBin = iolist_to_binary(Response),
                    _ = Transport:send(Socket, RespBin),
                    {error, {refused, RespBin}}
            end
    end.

loop(Socket, Transport, Owner, State) ->
    case Transport:recv(Socket, 0, 30000) of
        {ok, Data} ->
            case consume(Socket, Transport, Owner, State, Data) of
                {ok, State1} -> loop(Socket, Transport, Owner, State1);
                {stop, _} -> Transport:close(Socket)
            end;
        {error, closed} -> Owner ! {ev, {closed, maps:get(peer_id, State)}};
        {error, Why} -> Owner ! {ev, {recv_error, Why}}
    end.

consume(Socket, Transport, Owner, State, Data) ->
    case ws_frame:decode(Data, ?MAX_FRAME) of
        {more, _Need} ->
            {ok, State};
        {error, Reason} ->
            Owner ! {ev, {frame_error, Reason}},
            _ = Transport:send(Socket, ws_frame:encode(8, ws_frame:close_payload(1002, <<>>))),
            {stop, State};
        {ok, Frame, Rest} ->
            {Action, State1} = on_frame(Frame, Socket, Transport, Owner, State),
            case {Action, Rest} of
                {continue, <<>>} -> {ok, State1};
                {continue, _} -> consume(Socket, Transport, Owner, State1, Rest);
                {stop, _} -> {stop, State1}
            end
    end.

%% Text "ping" is the upstream library's keepalive convention and is distinct
%% from the RFC's own opcode 9. Both must work; they must not be conflated.
on_frame(#{opcode := 1, payload := <<"ping">>}, Socket, Transport, Owner, State) ->
    ok = Transport:send(Socket, ws_frame:encode(1, <<"pong">>)),
    Owner ! {ev, {text_ping_answered, maps:get(peer_id, State)}},
    {continue, State};
on_frame(#{opcode := 9, payload := P}, Socket, Transport, Owner, State) ->
    ok = Transport:send(Socket, ws_frame:encode(10, P)),
    Owner ! {ev, {native_ping_answered, maps:get(peer_id, State)}},
    {continue, State};
on_frame(#{opcode := 2} = F, Socket, Transport, Owner, State) ->
    Owner ! {ev, {binary_frame, maps:get(peer_id, State), maps:get(payload, F)}},
    %% R-T3: refuse, do not silently ignore (upstream only warns).
    _ = Transport:send(Socket, ws_frame:encode(8, ws_frame:close_payload(1003, <<>>))),
    Owner ! {ev, {binary_close_1003, maps:get(peer_id, State)}},
    {stop, State};
on_frame(#{opcode := 1, payload := P}, Socket, Transport, Owner, State) ->
    Owner ! {ev, {text_frame, maps:get(peer_id, State), P}},
    case decode_json(P) of
        #{<<"event">> := <<"authenticate">>} ->
            PeerId = maps:get(peer_id, State),
            Reply = json_msg(#{<<"event">> => <<"authenticated">>,
                               <<"data">> => #{<<"peer_id">> => PeerId}}),
            ok = Transport:send(Socket, ws_frame:encode(1, Reply)),
            {continue, State#{authenticated => true}};
        #{<<"to">> := To} = Msg ->
            Delivered = fanout(To, Msg, State),
            Owner ! {ev, {delivered, maps:get(peer_id, State), Delivered}},
            {continue, State};
        _ ->
            ok = Transport:send(Socket, ws_frame:encode(1,
                   json_msg(#{<<"event">> => <<"invalid_message">>}))),
            {continue, State}
    end;
on_frame(#{opcode := 8}, _Socket, _Transport, Owner, State) ->
    Owner ! {ev, {client_close, maps:get(peer_id, State)}},
    {stop, State};
on_frame(#{opcode := Op}, _S, _T, Owner, State) ->
    Owner ! {ev, {other_frame, Op}},
    {continue, State}.

%% ADR-028 D3: the whole delivery path is `RoomRegistry -> pid ! frame'.
%% No broker_message record, no append, no router, no subscription index.
%% The forwarded frame is the original message plus `from', keeping `to'
%% (R-S2) -- event/data stay opaque byte-for-byte.
fanout(To, Msg, State) ->
    From = maps:get(peer_id, State),
    Targets = [{Id, S, T} || {Id, P, S, T} <- peers(),
                             P =/= self(), Id =:= To],
    Forward = json_msg(Msg#{<<"from">> => From}),
    [ok = T:send(S, ws_frame:encode(1, Forward)) || {_Id, S, T} <- Targets],
    [Id || {Id, _S, _T} <- Targets].

decode_json(Bin) ->
    try json:decode(Bin) of
        M when is_map(M) -> M;
        _ -> #{}
    catch _:_ -> #{}
    end.

%% OTP 28's json module encodes maps to JSON objects directly.
json_msg(Map) ->
    json:encode(Map).
