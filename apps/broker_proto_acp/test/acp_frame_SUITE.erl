%% @doc S9-E: newline framing and the JSON-RPC envelope builders.
%%
%% Grouping:
%%   * `split'  -- partial reads, several frames per read, CRLF, empty input;
%%   * `decode' -- malformed lines, batch/array rejection via the shared model;
%%   * `build'  -- request/notification/response shapes and key classification.
-module(acp_frame_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-compile([export_all, nowarn_export_all]).

all() ->
    [{group, split}, {group, decode}, {group, build}].

groups() ->
    [{split, [sequence],
      [one_complete_line,
       two_lines_in_one_read,
       partial_line_is_buffered,
       crlf_is_stripped,
       empty_chunk_keeps_buffer]},
     {decode, [sequence],
      [valid_object_decodes,
       garbage_is_parse_error,
       batch_array_is_never_a_message]},
     {build, [sequence],
      [request_has_id_method_params,
       notification_has_no_id,
       response_shapes,
       kind_classification]}].

%%%_* split ------------------------------------------------------------------

one_complete_line(_Config) ->
    {[<<"{\"a\":1}">>], <<>>} = acp_frame:split(<<>>, <<"{\"a\":1}\n">>).

two_lines_in_one_read(_Config) ->
    {[<<"a">>, <<"b">>], <<>>} = acp_frame:split(<<>>, <<"a\nb\n">>).

partial_line_is_buffered(_Config) ->
    {[], <<"ab">>} = acp_frame:split(<<>>, <<"ab">>),
    {[<<"abc">>], <<>>} = acp_frame:split(<<"ab">>, <<"c\n">>).

crlf_is_stripped(_Config) ->
    {[<<"x">>], <<>>} = acp_frame:split(<<>>, <<"x\r\n">>).

empty_chunk_keeps_buffer(_Config) ->
    {[], <<"keep">>} = acp_frame:split(<<"keep">>, <<>>).

%%%_* decode -----------------------------------------------------------------

valid_object_decodes(_Config) ->
    {ok, Term} = acp_frame:decode(<<"{\"jsonrpc\":\"2.0\"}">>),
    ?assertEqual(<<"2.0">>, maps:get(<<"jsonrpc">>, Term)).

garbage_is_parse_error(_Config) ->
    ?assertEqual({error, parse_error}, acp_frame:decode(<<"not json">>)),
    ?assertEqual({error, parse_error}, acp_frame:decode(<<>>)).

batch_array_is_never_a_message(_Config) ->
    %% ACP forbids batch. Decoding is JSON-level and tolerates an array, but
    %% classification must not: an array is neither request, notification nor
    %% response, so the client answers `invalid_request' and never dispatches it.
    ?assertMatch({ok, _}, acp_frame:decode(<<"[{}]">>)),
    {ok, Term} = acp_frame:decode(<<"[{}]">>),
    ?assertEqual(invalid, acp_frame:kind(Term)),
    {ok, List} = acp_frame:decode(
                   <<"[{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"m\"}]">>),
    ?assertEqual(invalid, acp_frame:kind(List)).

%%%_* build ------------------------------------------------------------------

request_has_id_method_params(_Config) ->
    Frame = acp_frame:request(7, <<"session/prompt">>, #{<<"x">> => 1}),
    ?assertEqual(<<"2.0">>, maps:get(<<"jsonrpc">>, Frame)),
    ?assertEqual(7, maps:get(<<"id">>, Frame)),
    ?assertEqual(<<"session/prompt">>, maps:get(<<"method">>, Frame)),
    ?assertEqual(#{<<"x">> => 1}, maps:get(<<"params">>, Frame)).

notification_has_no_id(_Config) ->
    Frame = acp_frame:notification(<<"session/cancel">>, #{}),
    ?assertNot(maps:is_key(<<"id">>, Frame)),
    ?assertEqual(<<"session/cancel">>, maps:get(<<"method">>, Frame)).

response_shapes(_Config) ->
    Ok = acp_frame:response_ok(1, #{<<"sessionId">> => <<"s1">>}),
    ?assertEqual(1, maps:get(<<"id">>, Ok)),
    ?assertEqual(<<"s1">>, maps:get(<<"sessionId">>, maps:get(<<"result">>, Ok))),
    Err = acp_frame:response_error(2, method_not_found, #{<<"m">> => <<"x">>}),
    ?assertEqual(-32601, maps:get(<<"code">>, maps:get(<<"error">>, Err))).

kind_classification(_Config) ->
    ?assertEqual(request, acp_frame:kind(#{<<"jsonrpc">> => <<"2.0">>,
                                           <<"id">> => 1,
                                           <<"method">> => <<"m">>})),
    ?assertEqual(notification, acp_frame:kind(#{<<"jsonrpc">> => <<"2.0">>,
                                                <<"method">> => <<"m">>})),
    ?assertEqual(response, acp_frame:kind(#{<<"jsonrpc">> => <<"2.0">>,
                                            <<"id">> => 1,
                                            <<"result">> => #{}})),
    ?assertEqual(invalid, acp_frame:kind(#{<<"id">> => 1})),
    ?assertEqual(invalid, acp_frame:kind(<<"just a string">>)).
