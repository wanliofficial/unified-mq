%% @doc JSON-RPC 2.0 envelope validation (A2A §9.3).
-module(agent_rpc_json_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [decode_object,
     decode_invalid_json,
     request_valid,
     request_missing_jsonrpc,
     request_wrong_jsonrpc,
     request_missing_id,
     request_null_id,
     request_non_string_method,
     request_empty_method,
     request_params_default,
     request_params_array,
     request_params_scalar,
     request_batch_rejected,
     response_ok_shape,
     response_error_omits_missing_data,
     response_error_includes_data].

decode_object(_Config) ->
    ?assertEqual({ok, #{<<"a">> => <<"b">>}},
                 agent_rpc_json:decode(<<"{\"a\":\"b\"}">>)).

decode_invalid_json(_Config) ->
    ?assertEqual({error, parse_error}, agent_rpc_json:decode(<<"not json">>)),
    ?assertEqual({error, parse_error}, agent_rpc_json:decode(<<"{">>)).

request_valid(_Config) ->
    Term = #{<<"jsonrpc">> => <<"2.0">>, <<"id">> => 1,
             <<"method">> => <<"SendMessage">>,
             <<"params">> => #{<<"message">> => #{}}},
    ?assertEqual({ok, #{id => 1, method => <<"SendMessage">>,
                        params => #{<<"message">> => #{}}}},
                 agent_rpc_json:request(Term)).

request_missing_jsonrpc(_Config) ->
    ?assertEqual({error, invalid_request},
                 agent_rpc_json:request(#{<<"id">> => 1,
                                          <<"method">> => <<"SendMessage">>})).

request_wrong_jsonrpc(_Config) ->
    ?assertEqual({error, invalid_request},
                 agent_rpc_json:request(#{<<"jsonrpc">> => <<"1.0">>,
                                          <<"id">> => 1,
                                          <<"method">> => <<"SendMessage">>})).

request_missing_id(_Config) ->
    ?assertEqual({error, invalid_request},
                 agent_rpc_json:request(#{<<"jsonrpc">> => <<"2.0">>,
                                          <<"method">> => <<"SendMessage">>})).

request_null_id(_Config) ->
    ?assertMatch({ok, #{id := null}},
                 agent_rpc_json:request(#{<<"jsonrpc">> => <<"2.0">>,
                                          <<"id">> => null,
                                          <<"method">> => <<"SendMessage">>})).

request_non_string_method(_Config) ->
    ?assertEqual({error, invalid_request},
                 agent_rpc_json:request(#{<<"jsonrpc">> => <<"2.0">>,
                                          <<"id">> => 1,
                                          <<"method">> => 42})).

request_empty_method(_Config) ->
    ?assertEqual({error, invalid_request},
                 agent_rpc_json:request(#{<<"jsonrpc">> => <<"2.0">>,
                                          <<"id">> => 1,
                                          <<"method">> => <<>>})).

request_params_default(_Config) ->
    ?assertMatch({ok, #{params := #{}}},
                 agent_rpc_json:request(#{<<"jsonrpc">> => <<"2.0">>,
                                          <<"id">> => <<"x">>,
                                          <<"method">> => <<"GetTask">>})).

request_params_array(_Config) ->
    ?assertMatch({ok, #{params := [1, 2]}},
                 agent_rpc_json:request(#{<<"jsonrpc">> => <<"2.0">>,
                                          <<"id">> => <<"x">>,
                                          <<"method">> => <<"GetTask">>,
                                          <<"params">> => [1, 2]})).

request_params_scalar(_Config) ->
    ?assertEqual({error, invalid_request},
                 agent_rpc_json:request(#{<<"jsonrpc">> => <<"2.0">>,
                                          <<"id">> => 1,
                                          <<"method">> => <<"GetTask">>,
                                          <<"params">> => 7})).

request_batch_rejected(_Config) ->
    %% A top-level array is a JSON-RPC batch; A2A's binding never uses one, and
    %% "some of the batch succeeded" is not a semantics we can honour.
    ?assertEqual({error, invalid_request},
                 agent_rpc_json:request([#{<<"jsonrpc">> => <<"2.0">>,
                                           <<"id">> => 1,
                                           <<"method">> => <<"SendMessage">>}])).

response_ok_shape(_Config) ->
    ?assertEqual(#{<<"jsonrpc">> => <<"2.0">>, <<"id">> => 1,
                   <<"result">> => #{<<"ok">> => true}},
                 agent_rpc_json:response_ok(1, #{<<"ok">> => true})).

response_error_omits_missing_data(_Config) ->
    Map = agent_rpc_json:response_error(7, method_not_found, undefined),
    ?assertEqual(#{<<"jsonrpc">> => <<"2.0">>, <<"id">> => 7,
                   <<"error">> => #{<<"code">> => -32601,
                                    <<"message">> => <<"Method not found">>}},
                 Map).

response_error_includes_data(_Config) ->
    Map = agent_rpc_json:response_error(7, version_not_supported,
                                        #{<<"supported">> => [<<"1.0">>]}),
    Error = maps:get(<<"error">>, Map),
    ?assertEqual(-32009, maps:get(<<"code">>, Error)),
    ?assertEqual(#{<<"supported">> => [<<"1.0">>]}, maps:get(<<"data">>, Error)).
