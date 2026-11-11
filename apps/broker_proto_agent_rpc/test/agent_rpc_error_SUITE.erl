%% @doc Error table: JSON-RPC code, HTTP status and message must agree with
%% A2A §5.4 and JSON-RPC 2.0. One test per row so a typo in one cell fails a
%% named case instead of a giant map comparison.
-module(agent_rpc_error_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [jsonrpc_codes,
     a2a_codes,
     http_statuses,
     transport_failures_have_no_jsonrpc_code,
     messages_are_non_empty,
     unknown_reason_falls_back,
     jsonrpc_error_shape,
     jsonrpc_error_with_data].

jsonrpc_codes(_Config) ->
    ?assertEqual(-32700, agent_rpc_error:code(parse_error)),
    ?assertEqual(-32600, agent_rpc_error:code(invalid_request)),
    ?assertEqual(-32601, agent_rpc_error:code(method_not_found)),
    ?assertEqual(-32602, agent_rpc_error:code(invalid_params)),
    ?assertEqual(-32603, agent_rpc_error:code(internal_error)).

a2a_codes(_Config) ->
    Table = [{task_not_found, -32001},
             {task_not_cancelable, -32002},
             {push_notification_not_supported, -32003},
             {unsupported_operation, -32004},
             {content_type_not_supported, -32005},
             {invalid_agent_response, -32006},
             {extended_agent_card_not_configured, -32007},
             {extension_support_required, -32008},
             {version_not_supported, -32009}],
    [?assertEqual(Code, agent_rpc_error:code(Reason)) || {Reason, Code} <- Table].

http_statuses(_Config) ->
    ?assertEqual(404, agent_rpc_error:http_status(task_not_found)),
    ?assertEqual(500, agent_rpc_error:http_status(invalid_agent_response)),
    ?assertEqual(400, agent_rpc_error:http_status(task_not_cancelable)),
    ?assertEqual(400, agent_rpc_error:http_status(version_not_supported)),
    ?assertEqual(200, agent_rpc_error:http_status(method_not_found)),
    ?assertEqual(200, agent_rpc_error:http_status(invalid_params)),
    ?assertEqual(400, agent_rpc_error:http_status(parse_error)),
    ?assertEqual(500, agent_rpc_error:http_status(internal_error)).

transport_failures_have_no_jsonrpc_code(_Config) ->
    [?assertEqual(undefined, agent_rpc_error:code(R))
     || R <- [unauthorized, forbidden, payload_too_large, unsupported_media_type]],
    ?assertEqual(401, agent_rpc_error:http_status(unauthorized)),
    ?assertEqual(403, agent_rpc_error:http_status(forbidden)),
    ?assertEqual(413, agent_rpc_error:http_status(payload_too_large)),
    ?assertEqual(415, agent_rpc_error:http_status(unsupported_media_type)).

messages_are_non_empty(_Config) ->
    Reasons = [parse_error, invalid_request, method_not_found, invalid_params,
               internal_error, task_not_found, task_not_cancelable,
               push_notification_not_supported, unsupported_operation,
               content_type_not_supported, invalid_agent_response,
               extended_agent_card_not_configured, extension_support_required,
               version_not_supported, unauthorized, forbidden,
               payload_too_large, unsupported_media_type],
    [?assert(byte_size(agent_rpc_error:message(R)) > 0) || R <- Reasons].

unknown_reason_falls_back(_Config) ->
    ?assertEqual(-32603, agent_rpc_error:code(something_new)),
    ?assertEqual(500, agent_rpc_error:http_status(something_new)),
    ?assertEqual(<<"Internal error">>, agent_rpc_error:message(something_new)),
    %% An atom outside both tables is a programming error, not a client error:
    %% it must never be reported as a 4xx.
    ?assertEqual(500, agent_rpc_error:http_status(task_already_exists)).

jsonrpc_error_shape(_Config) ->
    ?assertEqual(#{<<"code">> => -32001, <<"message">> => <<"Task not found">>},
                 agent_rpc_error:jsonrpc_error(task_not_found, undefined)).

jsonrpc_error_with_data(_Config) ->
    Map = agent_rpc_error:jsonrpc_error(version_not_supported,
                                        #{<<"supported">> => [<<"1.0">>]}),
    ?assertEqual(-32009, maps:get(<<"code">>, Map)),
    ?assertEqual(#{<<"supported">> => [<<"1.0">>]}, maps:get(<<"data">>, Map)).
