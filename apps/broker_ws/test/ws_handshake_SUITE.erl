%% @doc RFC 6455 opening handshake (`ws_handshake').
%%
%% Two things are pinned here that are easy to break later:
%%
%%  * `accept/1' must produce the exact bytes the WebSocket front end has always
%%    sent. The S5 compatibility evidence is a third-party client talking to
%%    those bytes, so a stray header or a reordered line is a regression, not a
%%    cosmetic change. The RFC 6455 §1.3 example key/digest pair makes the
%%    expectation objective instead of "whatever the code prints today".
%%  * `accept/2' must offer a subprotocol only when one was both configured and
%%    requested. MQTT over WebSocket depends on this: MQTT 3.1.1 §6 and MQTT 5.0
%%    §6 require the `mqtt' subprotocol, so a client that asks for none gets a
%%    refusal rather than an upgrade it cannot use.
-module(ws_handshake_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(KEY, <<"dGhlIHNhbXBsZSBub25jZQ==">>).
-define(ACCEPT, <<"s3pPLMBiTxaQ9kYGzzhZRbK+xOo=">>).

all() ->
    [accept_computes_the_rfc_example_digest,
     accept_1_bytes_are_the_historical_response,
     accept_rejects_non_get,
     accept_rejects_missing_upgrade,
     accept_rejects_missing_connection_upgrade,
     accept_rejects_missing_key,
     accept_rejects_wrong_version,
     header_names_are_case_insensitive,
     connection_header_accepts_multiple_tokens,
     accept_without_subprotocols_sends_no_subprotocol_header,
     accept_selects_the_servers_first_shared_subprotocol,
     accept_trims_the_client_subprotocol_list,
     accept_rejects_a_client_that_asked_for_no_subprotocol,
     accept_rejects_when_no_subprotocol_matches,
     max_handshake_is_the_documented_limit].

%%%_* A successful handshake ------------------------------------------------

accept_computes_the_rfc_example_digest(_Config) ->
    {ok, Response, undefined} = ws_handshake:accept(request(), #{}),
    Bin = iolist_to_binary(Response),
    <<"HTTP/1.1 101 Switching Protocols\r\n", _/binary>> = Bin,
    ?assert(binary:match(Bin, <<"Sec-WebSocket-Accept: ", ?ACCEPT/binary>>) =/= nomatch).

%% The historical `ws_conn:accept/1' response, written out literally. If this
%% fails, a real client's handshake expectation moved with it.
accept_1_bytes_are_the_historical_response(_Config) ->
    Expected =
        <<"HTTP/1.1 101 Switching Protocols\r\n"
          "Upgrade: websocket\r\n"
          "Connection: Upgrade\r\n"
          "Sec-WebSocket-Accept: ", ?ACCEPT/binary, "\r\n\r\n">>,
    {ok, Response} = ws_handshake:accept(request()),
    ?assertEqual(Expected, iolist_to_binary(Response)).

%%%_* Refusals --------------------------------------------------------------

accept_rejects_non_get(_Config) ->
    {error, Response} =
        ws_handshake:accept(<<"POST / HTTP/1.1\r\nHost: x\r\n\r\n">>),
    ?assertMatch(<<"HTTP/1.1 400 ", _/binary>>, Response).

accept_rejects_missing_upgrade(_Config) ->
    {error, Response} =
        ws_handshake:accept(request(#{<<"upgrade">> => undefined})),
    ?assertMatch(<<"HTTP/1.1 400 ", _/binary>>, Response),
    ?assert(binary:match(Response, <<"Upgrade: websocket">>) =/= nomatch).

accept_rejects_missing_connection_upgrade(_Config) ->
    {error, Response} =
        ws_handshake:accept(request(#{<<"connection">> => <<"keep-alive">>})),
    ?assertMatch(<<"HTTP/1.1 400 ", _/binary>>, Response).

accept_rejects_missing_key(_Config) ->
    {error, Response} =
        ws_handshake:accept(request(#{<<"sec-websocket-key">> => <<>>})),
    ?assertMatch(<<"HTTP/1.1 400 ", _/binary>>, Response).

%% A version this server does not speak is 426 with the version it does speak,
%% never a silent upgrade.
accept_rejects_wrong_version(_Config) ->
    {error, Response} =
        ws_handshake:accept(request(#{<<"sec-websocket-version">> => <<"8">>})),
    ?assertMatch(<<"HTTP/1.1 426 ", _/binary>>, Response),
    ?assert(binary:match(Response, <<"Sec-WebSocket-Version: 13">>) =/= nomatch).

%%%_* Header parsing --------------------------------------------------------

header_names_are_case_insensitive(_Config) ->
    Raw = <<"GET /mqtt HTTP/1.1\r\n"
            "Host: localhost\r\n"
            "UPGRADE: WebSocket\r\n"
            "connection: Upgrade\r\n"
            "SEC-WEBSOCKET-KEY: ", ?KEY/binary, "\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n">>,
    {ok, Response} = ws_handshake:accept(Raw),
    ?assert(binary:match(iolist_to_binary(Response), ?ACCEPT) =/= nomatch).

connection_header_accepts_multiple_tokens(_Config) ->
    Raw = request(#{<<"connection">> => <<"keep-alive, Upgrade">>}),
    {ok, _Response} = ws_handshake:accept(Raw).

%%%_* Subprotocol negotiation ----------------------------------------------

%% The pre-S8 response, byte for byte: no `Sec-WebSocket-Protocol' line at all.
accept_without_subprotocols_sends_no_subprotocol_header(_Config) ->
    Raw = request(#{<<"sec-websocket-protocol">> => <<"mqtt">>}),
    {ok, Response, undefined} = ws_handshake:accept(Raw, #{}),
    Bin = iolist_to_binary(Response),
    ?assertEqual(nomatch, binary:match(Bin, <<"Sec-WebSocket-Protocol">>)),
    %% ... and the empty list says the same thing as the missing key.
    {ok, Response2, undefined} = ws_handshake:accept(Raw, #{supported_subprotocols => []}),
    ?assertEqual(Bin, iolist_to_binary(Response2)).

%% Server preference wins, not the order the client listed.
accept_selects_the_servers_first_shared_subprotocol(_Config) ->
    Raw = request(#{<<"sec-websocket-protocol">> => <<"mqttv3.1.1, mqtt">>}),
    {ok, Response, <<"mqtt">>} =
        ws_handshake:accept(Raw, #{supported_subprotocols => [<<"mqtt">>, <<"mqttv3.1.1">>]}),
    Bin = iolist_to_binary(Response),
    ?assert(binary:match(Bin, <<"Sec-WebSocket-Protocol: mqtt\r\n">>) =/= nomatch).

accept_trims_the_client_subprotocol_list(_Config) ->
    Raw = request(#{<<"sec-websocket-protocol">> => <<"  chat , mqtt ,, ">>}),
    {ok, Response, <<"mqtt">>} =
        ws_handshake:accept(Raw, #{supported_subprotocols => [<<"mqtt">>]}),
    ?assert(binary:match(iolist_to_binary(Response),
                         <<"Sec-WebSocket-Protocol: mqtt\r\n">>) =/= nomatch).

%% MQTT over WebSocket's case: the listener offers `mqtt', the client asks for
%% nothing. Upgrading here would create a connection whose framing the client
%% did not agree to.
accept_rejects_a_client_that_asked_for_no_subprotocol(_Config) ->
    {error, Response} =
        ws_handshake:accept(request(), #{supported_subprotocols => [<<"mqtt">>]}),
    ?assertMatch(<<"HTTP/1.1 400 ", _/binary>>, Response).

accept_rejects_when_no_subprotocol_matches(_Config) ->
    Raw = request(#{<<"sec-websocket-protocol">> => <<"chat">>}),
    {error, Response} =
        ws_handshake:accept(Raw, #{supported_subprotocols => [<<"mqtt">>]}),
    ?assertMatch(<<"HTTP/1.1 400 ", _/binary>>, Response).

%%%_* Handshake size --------------------------------------------------------

%% The connection processes reject anything larger before parsing, so the limit
%% they compare against has one definition.
max_handshake_is_the_documented_limit(_Config) ->
    ?assertEqual(16384, ws_handshake:max_handshake()).

%%%_* Helpers ---------------------------------------------------------------

request() ->
    request(#{}).

%% `undefined' as an override drops that header entirely (the "missing header"
%% case); any other value replaces the suite default.
request(Overrides) ->
    Defaults = [{<<"host">>, <<"Host">>, <<"localhost">>},
                {<<"upgrade">>, <<"Upgrade">>, <<"websocket">>},
                {<<"connection">>, <<"Connection">>, <<"Upgrade">>},
                {<<"sec-websocket-key">>, <<"Sec-WebSocket-Key">>, ?KEY},
                {<<"sec-websocket-version">>, <<"Sec-WebSocket-Version">>, <<"13">>}],
    Known = [Key || {Key, _Name, _Default} <- Defaults],
    Extra = [{Key, canonical_name(Key), Value}
             || {Key, Value} <- maps:to_list(Overrides),
                not lists:member(Key, Known)],
    Lines = [header(Key, Name, maps:get(Key, Overrides, Default))
             || {Key, Name, Default} <- Defaults ++ Extra],
    iolist_to_binary([<<"GET /mqtt HTTP/1.1\r\n">>, Lines, <<"\r\n">>]).

%% Only the subprotocol header currently arrives as an extra; spelling it the
%% way a client would keeps the fixture readable.
canonical_name(<<"sec-websocket-protocol">>) -> <<"Sec-WebSocket-Protocol">>;
canonical_name(Other) -> Other.

header(_Key, _Name, undefined) ->
    [];
header(_Key, Name, Value) ->
    [Name, <<": ">>, Value, <<"\r\n">>].
