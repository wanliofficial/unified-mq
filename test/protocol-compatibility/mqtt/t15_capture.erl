%% @doc T15 capture analyzer: decode a real client's byte stream with the
%% production codec and prove the frame round-trips byte-for-byte.
%%
%% The T15 harness runs a transparent TCP tee between a real MQTT client
%% (paho-mqtt 2.x / mosquitto 2.1.2) and the release broker, so every file this
%% module reads is bytes a real client actually put on the wire.  For each
%% decoded frame the analyzer:
%%
%%   1. prints the exact consumed bytes as hex,
%%   2. re-encodes the decoded record and compares it to those bytes,
%%   3. prints the semantic fields the T15 driver asserts on.
%%
%% A `mismatch' therefore means our encoder cannot reproduce a real client's
%% encoding of a frame we just accepted -- the same class of gap T8's
%% hand-written golden vectors cannot see by construction (design §13 M3).
%%
%% Output is tab-separated so the Python driver can parse it without an Erlang
%% JSON dependency:
%%
%%   FRAME  <index>  <type>  roundtrip=ok|mismatch  <hex>  <k=v;...>
%%   ERROR  <reason term>  <remaining hex>
%%   PARTIAL  <remaining hex>
%%   DONE  <frame count>
-module(t15_capture).

-export([main/1]).

-include("mqtt_frame.hrl").

main([Path]) ->
    {ok, Bin} = file:read_file(Path),
    loop(Bin, 0).

loop(<<>>, N) ->
    io:format("DONE\t~p~n", [N]);
loop(Bin, N) ->
    case mqtt_frame:decode(Bin) of
        {ok, Frame, Rest} ->
            Used = byte_size(Bin) - byte_size(Rest),
            <<Raw:Used/binary, _/binary>> = Bin,
            case reencode(Frame) of
                {ok, Raw} ->
                    emit("FRAME", [N, type(Frame), "roundtrip=ok",
                                   hex(Raw), info(Frame)]);
                {ok, Enc} ->
                    emit("FRAME", [N, type(Frame), "roundtrip=mismatch",
                                   hex(Raw), info(Frame), "enc=" ++ hex(Enc)]);
                {error, Class, Reason} ->
                    emit("FRAME", [N, type(Frame), "roundtrip=error:" ++ atom(Class),
                                   hex(Raw), info(Frame), "err=" ++ term(Reason)])
            end,
            loop(Rest, N + 1);
        {more, Rest} ->
            emit("PARTIAL", [hex(Rest)]);
        {error, Reason} ->
            emit("ERROR", [term(Reason), hex(Bin)])
    end.

reencode(Frame) ->
    try {ok, mqtt_frame:encode(Frame)}
    catch Class:Reason -> {error, Class, Reason}
    end.

type(Frame) -> atom(element(1, Frame)).

%%%_* Frame fields ------------------------------------------------------------

info(#connect{proto_level = L, clean_session = CS, keep_alive = KA,
              client_id = Id, will_flag = WF, will_qos = WQ,
              will_retain = WR, username = U, password = P}) ->
    pairs([{proto_level, L}, {clean, CS}, {keepalive, KA}, {client, b64(Id)},
           {will, WF}, {will_qos, WQ}, {will_retain, WR},
           {user, defined(U)}, {pass, defined(P)}]);
info(#connack{session_present = SP, return_code = RC}) ->
    pairs([{session_present, SP}, {rc, RC}]);
info(#publish{dup = D, qos = Q, retain = R, topic = T, packet_id = Pid,
              payload = Payload}) ->
    pairs([{dup, D}, {qos, Q}, {retain, R}, {topic, b64(T)},
           {packet_id, opt(Pid)}, {payload, b64(Payload)},
           {payload_bytes, byte_size(Payload)}]);
info(#puback{packet_id = Pid}) ->
    pairs([{packet_id, Pid}]);
info(#subscribe{packet_id = Pid, topics = Topics}) ->
    pairs([{packet_id, Pid},
           {topics, join([b64(T) ++ ":" ++ integer_to_list(Q) || {T, Q} <- Topics])}]);
info(#suback{packet_id = Pid, return_codes = Codes}) ->
    pairs([{packet_id, Pid}, {codes, join([integer_to_list(C) || C <- Codes])}]);
info(#unsubscribe{packet_id = Pid, topics = Topics}) ->
    pairs([{packet_id, Pid}, {topics, join([b64(T) || T <- Topics])}]);
info(#unsuback{packet_id = Pid}) ->
    pairs([{packet_id, Pid}]);
info(#ping{kind = Kind}) ->
    pairs([{kind, Kind}]);
info(#disconnect{}) ->
    pairs([]);
info(_Other) ->
    pairs([]).

pairs(KVs) ->
    join_sep(";", [atom_to_list(K) ++ "=" ++ value(V) || {K, V} <- KVs]).

value(V) when is_integer(V) -> integer_to_list(V);
value(V) when is_atom(V) -> atom_to_list(V);
value(V) when is_binary(V) -> binary_to_list(V);
value(V) when is_list(V) -> V.

opt(undefined) -> "none";
opt(V) -> integer_to_list(V).

defined(undefined) -> 0;
defined(_) -> 1.

join([]) -> "";
join([H | T]) ->
    lists:foldl(fun(E, Acc) -> Acc ++ "," ++ E end, H, T).

join_sep(_Sep, []) -> "";
join_sep(Sep, [H | T]) ->
    lists:foldl(fun(E, Acc) -> Acc ++ Sep ++ E end, H, T).

b64(Bin) when is_binary(Bin) ->
    binary_to_list(base64:encode(Bin)).

%%%_* Output helpers ----------------------------------------------------------

emit(Tag, Fields) ->
    io:format("~s\t~s~n", [Tag, join_sep("\t", [flat(F) || F <- Fields])]).

flat(F) when is_binary(F) -> binary_to_list(F);
flat(F) when is_list(F) -> F;
flat(F) when is_atom(F) -> atom_to_list(F);
flat(F) when is_integer(F) -> integer_to_list(F).

atom(A) -> atom_to_list(A).

term(T) ->
    lists:flatten(io_lib:format("~0p", [T])).

hex(Bin) ->
    << <<(digit(B bsr 4)), (digit(B band 16#0F))>> || <<B>> <= Bin >>.

digit(N) when N < 10 -> $0 + N;
digit(N) -> $a + (N - 10).
