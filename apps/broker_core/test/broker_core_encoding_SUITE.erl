%% @doc Unit tests for `broker_core_encoding:base64url/1' (RFC 4648 §5).
%%
%% The encoded form is a security-sensitive wire ID: A2A task ids, MCP
%% correlation ids and the A2A card ETag all carry it. It is a one-function,
%% stateless helper, but the exact alphabet (`-' and `_' instead of `+' and
%% `/', no `=' padding) is the kind of thing a copy-paste drift breaks silently,
%% so it is pinned directly here.
-module(broker_core_encoding_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [empty_binary_encodes_to_empty,
     standard_alphabet_unchanged,
     plus_becomes_minus,
     slash_becomes_underscore,
     padding_is_stripped,
     known_rfc4648_vectors,
     no_plus_or_slash_in_output,
     round_trips_via_base64_decode].

empty_binary_encodes_to_empty(_Config) ->
    ?assertEqual(<<>>, broker_core_encoding:base64url(<<>>)),
    ok.

%% Bytes whose base64 has neither `+' nor `/' pass through unchanged (minus
%% padding).
standard_alphabet_unchanged(_Config) ->
    %% "foo" -> "Zm9v"
    ?assertEqual(<<"Zm9v">>, broker_core_encoding:base64url(<<"foo">>)),
    ok.

%% 0xFB 0xFF base64-encodes to "+/8=" (both special chars, with padding).
plus_becomes_minus(_Config) ->
    Encoded = broker_core_encoding:base64url(<<251, 255>>),
    ?assertNotMatch({_, _}, binary:match(Encoded, <<"+">>)),
    ok.

slash_becomes_underscore(_Config) ->
    Encoded = broker_core_encoding:base64url(<<251, 255>>),
    ?assertNotMatch({_, _}, binary:match(Encoded, <<"/">>)),
    ?assertEqual(<<"-_8">>, Encoded),
    ok.

padding_is_stripped(_Config) ->
    %% "f" -> "Zg==" -> "Zg"; "fo" -> "Zm8=" -> "Zm8".
    ?assertEqual(<<"Zg">>, broker_core_encoding:base64url(<<"f">>)),
    ?assertEqual(<<"Zm8">>, broker_core_encoding:base64url(<<"fo">>)),
    ok.

%% The canonical RFC 4648 §10 test vectors, adjusted to base64url.
known_rfc4648_vectors(_Config) ->
    ?assertEqual(<<"">>, broker_core_encoding:base64url(<<"">>)),
    ?assertEqual(<<"Zg">>, broker_core_encoding:base64url(<<"f">>)),
    ?assertEqual(<<"Zm8">>, broker_core_encoding:base64url(<<"fo">>)),
    ?assertEqual(<<"Zm9v">>, broker_core_encoding:base64url(<<"foo">>)),
    ?assertEqual(<<"Zm9vYg">>, broker_core_encoding:base64url(<<"foob">>)),
    ?assertEqual(<<"Zm9vYmE">>, broker_core_encoding:base64url(<<"fooba">>)),
    ?assertEqual(<<"Zm9vYmFy">>, broker_core_encoding:base64url(<<"foobar">>)),
    ok.

no_plus_or_slash_in_output(_Config) ->
    %% Exercise a byte range that produces both `+' and `/' in standard base64.
    Bin = list_to_binary(lists:seq(0, 255)),
    Encoded = broker_core_encoding:base64url(Bin),
    ?assertEqual(nomatch, binary:match(Encoded, <<"+">>)),
    ?assertEqual(nomatch, binary:match(Encoded, <<"/">>)),
    ?assertEqual(nomatch, binary:match(Encoded, <<"=">>)),
    ok.

%% base64url without padding round-trips through standard base64 decode once the
%% alphabet is restored and padding re-added (the contract the decoders rely on).
round_trips_via_base64_decode(_Config) ->
    Bin = crypto:strong_rand_bytes(31),
    Encoded = broker_core_encoding:base64url(Bin),
    Standard = iolist_to_binary(
                 [case C of
                      $- -> $+;
                      $_ -> $/;
                      _ -> C
                  end || <<C>> <= Encoded]),
    Padded = pad(Standard),
    ?assertEqual(Bin, base64:decode(Padded)),
    ok.

pad(Bin) ->
    case byte_size(Bin) rem 4 of
        0 -> Bin;
        N -> <<Bin/binary, (binary:copy(<<"=">>, 4 - N))/binary>>
    end.
