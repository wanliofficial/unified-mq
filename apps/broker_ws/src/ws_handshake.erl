%% @doc RFC 6455 opening handshake, server side.
%%
%% This module owns exactly the HTTP part of RFC 6455: parse the client's
%% opening handshake, validate it, and produce the `101 Switching Protocols'
%% response (including the `Sec-WebSocket-Accept' digest). It does no I/O, holds
%% no state and knows nothing about the broker, so every WebSocket front end
%% (`broker_proto_ws', the broker's own JSON/binary API,
%% `broker_proto_mqtt_ws', the MQTT-over-WebSocket transport, and
%% `broker_proto_webrtc', the S10-A signaling front end) shares it.
%%
%% Two entry points:
%%
%%   * `accept/1' is the historical, subprotocol-free form. Its return value is
%%     byte-for-byte what `broker_proto_ws' sent before this module existed --
%%     that is a hard requirement, because the S5 compatibility evidence pins
%%     those bytes with a third-party client.
%%   * `accept/2' additionally negotiates `Sec-WebSocket-Protocol'. MQTT over
%%     WebSocket (MQTT 3.1.1 §6 / MQTT 5.0 §6) requires the `mqtt' subprotocol,
%%     so a client that sends no subprotocol, or one the listener does not
%%     offer, must be refused rather than upgraded.
%%
%% Errors are returned as ready-to-send HTTP responses, not atoms: the caller
%% writes them verbatim and closes, which is the same posture `ws_conn' already
%% had. A refused handshake is a normal outcome, never a crash.
-module(ws_handshake).

-export([accept/1, accept/2]).
-export([max_handshake/0]).

-export_type([opts/0, response/0]).

-type opts() :: #{supported_subprotocols => [binary()]}.
-type response() :: iodata().

-define(GUID, <<"258EAFA5-E914-47DA-95CA-C5AB0DC85B11">>).
-define(MAX_HANDSHAKE, 16384).

%% @doc Largest opening handshake (request line + headers) the callers accept.
%% The check itself lives in the connection processes, which are the ones
%% holding the read buffer; it is exported so the rule has one definition.
-spec max_handshake() -> pos_integer().
max_handshake() -> ?MAX_HANDSHAKE.

%% @doc Subprotocol-free handshake: `{ok, Response}' or `{error, Response}'.
-spec accept(binary()) -> {ok, response()} | {error, response()}.
accept(Head) ->
    case accept(Head, #{}) of
        {ok, Response, _Subprotocol} -> {ok, Response};
        {error, Response} -> {error, Response}
    end.

%% @doc Handshake with optional subprotocol negotiation.
%%
%% `supported_subprotocols' lists what this listener offers, in the server's
%% own preference order. The selection walks that list and takes the first entry
%% the client also asked for, so the choice does not depend on the order the
%% client happened to send. When the option is absent (or empty) the response
%% carries no `Sec-WebSocket-Protocol' header at all -- a server must not offer
%% a subprotocol the client never requested.
-spec accept(binary(), opts()) ->
          {ok, response(), binary() | undefined} | {error, response()}.
accept(Head, Opts) when is_binary(Head), is_map(Opts) ->
    case binary:split(Head, <<"\r\n">>, [global]) of
        [RequestLine | HeaderLines] ->
            Headers = headers(HeaderLines),
            case request_line(RequestLine) of
                ok ->
                    case validate_upgrade(Headers) of
                        ok -> upgrade(Headers, Opts);
                        {error, Response} -> {error, Response}
                    end;
                error ->
                    {error, bad_request()}
            end;
        _ ->
            {error, bad_request()}
    end.

upgrade(Headers, Opts) ->
    Key = maps:get(<<"sec-websocket-key">>, Headers),
    Accept = base64:encode(crypto:hash(sha, <<Key/binary, ?GUID/binary>>)),
    case select_subprotocol(Headers, Opts) of
        {ok, undefined} ->
            {ok, [<<"HTTP/1.1 101 Switching Protocols\r\n"
                   "Upgrade: websocket\r\n"
                   "Connection: Upgrade\r\n"
                   "Sec-WebSocket-Accept: ">>, Accept, <<"\r\n\r\n">>],
             undefined};
        {ok, Subprotocol} ->
            {ok, [<<"HTTP/1.1 101 Switching Protocols\r\n"
                   "Upgrade: websocket\r\n"
                   "Connection: Upgrade\r\n"
                   "Sec-WebSocket-Accept: ">>, Accept,
                  <<"\r\nSec-WebSocket-Protocol: ">>, Subprotocol,
                  <<"\r\n\r\n">>],
             Subprotocol};
        {error, Response} ->
            {error, Response}
    end.

%% No configured list means "this endpoint does not speak subprotocols"; the
%% response must then look exactly like the pre-S8 one.
select_subprotocol(Headers, Opts) ->
    case maps:get(supported_subprotocols, Opts, []) of
        [] ->
            {ok, undefined};
        Supported ->
            Requested = requested_subprotocols(Headers),
            case first_shared(Supported, Requested) of
                undefined -> {error, bad_request()};
                Chosen -> {ok, Chosen}
            end
    end.

requested_subprotocols(Headers) ->
    case maps:get(<<"sec-websocket-protocol">>, Headers, <<>>) of
        <<>> ->
            [];
        Value ->
            [trim(P) || P <- binary:split(Value, <<",">>, [global]), trim(P) =/= <<>>]
    end.

%% Server preference wins: walk what we offer, not what the client listed.
first_shared([], _Requested) ->
    undefined;
first_shared([Proto | Rest], Requested) ->
    case lists:member(Proto, Requested) of
        true -> Proto;
        false -> first_shared(Rest, Requested)
    end.

request_line(<<"GET ", _/binary>>) -> ok;
request_line(_) -> error.

validate_upgrade(Headers) ->
    Upgrade = lower(maps:get(<<"upgrade">>, Headers, <<>>)),
    Connection = lower(maps:get(<<"connection">>, Headers, <<>>)),
    Version = maps:get(<<"sec-websocket-version">>, Headers, <<>>),
    Key = maps:get(<<"sec-websocket-key">>, Headers, <<>>),
    case {binary:match(Upgrade, <<"websocket">>), binary:match(Connection, <<"upgrade">>),
          Version, Key} of
        {nomatch, _, _, _} -> {error, upgrade_required()};
        {_, nomatch, _, _} -> {error, upgrade_required()};
        {_, _, <<"13">>, <<>>} -> {error, bad_request()};
        {_, _, <<"13">>, _} -> ok;
        {_, _, _, _} -> {error, version_required()}
    end.

headers(Lines) ->
    lists:foldl(
      fun(Line, Acc) ->
              case binary:split(Line, <<":">>) of
                  [K, V] ->
                      Acc#{lower(trim(K)) => trim(V)};
                  _ ->
                      Acc
              end
      end, #{}, Lines).

trim(B) ->
    iolist_to_binary(string:trim(B, both, " \t")).

lower(B) ->
    << <<(if C >= $A, C =< $Z -> C + 32; true -> C end)>> || <<C>> <= B >>.

bad_request() ->
    <<"HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n">>.

upgrade_required() ->
    <<"HTTP/1.1 400 Bad Request\r\nConnection: close\r\n"
      "Upgrade: websocket\r\n\r\n">>.

version_required() ->
    <<"HTTP/1.1 426 Upgrade Required\r\n"
      "Sec-WebSocket-Version: 13\r\n"
      "Connection: close\r\n\r\n">>.
