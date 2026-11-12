%% @doc JSON-RPC 2.0 standard errors plus the A2A 1.0.0 error family.
%%
%% Two tables live here on purpose: the JSON-RPC `code' (what the client parses)
%% and the HTTP status (what an intermediary sees). A2A §5.4 pins the A2A family
%% (`-32001'..`-32009'); the JSON-RPC standard codes are RFC 8259 / JSON-RPC 2.0.
%%
%% Method-level errors (`method_not_found', `invalid_params') keep HTTP 200: the
%% request was accepted and the failure is in the envelope. Transport-level
%% failures (auth, size) never become JSON-RPC bodies -- they are returned as
%% bare HTTP statuses by the adapter (ADR-026 §D2, convergence report §2.3).
-module(agent_rpc_error).

-export([code/1, http_status/1, message/1, jsonrpc_error/2, jsonrpc_error/3]).
-export_type([reason/0]).

-type reason() :: parse_error
                | invalid_request
                | method_not_found
                | invalid_params
                | internal_error
                | header_mismatch
                | unsupported_protocol_version
                | task_not_found
                | task_not_cancelable
                | push_notification_not_supported
                | unsupported_operation
                | content_type_not_supported
                | invalid_agent_response
                | extended_agent_card_not_configured
                | extension_support_required
                | version_not_supported
                | unauthorized
                | forbidden
                | payload_too_large
                | unsupported_media_type.

%% JSON-RPC 2.0 standard codes.
code(parse_error)        -> -32700;
code(invalid_request)    -> -32600;
code(method_not_found)   -> -32601;
code(invalid_params)     -> -32602;
code(internal_error)     -> -32603;
%% A2A 1.0.0 (§5.4). The proto has no error enum; these numbers are normative
%% in the spec's mapping table, not derived from the .proto file.
code(task_not_found)                 -> -32001;
code(task_not_cancelable)            -> -32002;
code(push_notification_not_supported) -> -32003;
code(unsupported_operation)          -> -32004;
code(content_type_not_supported)     -> -32005;
code(invalid_agent_response)         -> -32006;
code(extended_agent_card_not_configured) -> -32007;
code(extension_support_required)     -> -32008;
code(version_not_supported)          -> -32009;
%% MCP 2026-07-28 (`basic/index' error table). `-32020' is a transport safety
%% error: the request headers disagree with the JSON-RPC body, which is exactly
%% the class the Streamable HTTP binding answers before touching the envelope.
code(header_mismatch)                -> -32020;
code(unsupported_protocol_version)   -> -32022;
%% Transport-level failures have no JSON-RPC code. Answering with
%% `internal_error' would be a lie (the request never became an envelope), so
%% the adapter asks `http_status/1' and sends a bare HTTP response instead.
code(unauthorized)          -> undefined;
code(forbidden)             -> undefined;
code(payload_too_large)     -> undefined;
code(unsupported_media_type) -> undefined;
code(_Other)                -> -32603.

-spec http_status(reason() | atom()) -> 200 | 400 | 401 | 403 | 404 | 413 | 415 | 500.
http_status(task_not_found) -> 404;
http_status(invalid_agent_response) -> 500;
http_status(method_not_found) -> 200;
http_status(invalid_params) -> 200;
http_status(unauthorized) -> 401;
http_status(forbidden) -> 403;
http_status(payload_too_large) -> 413;
http_status(unsupported_media_type) -> 415;
http_status(parse_error) -> 400;
http_status(invalid_request) -> 400;
http_status(header_mismatch) -> 400;
http_status(unsupported_protocol_version) -> 400;
http_status(internal_error) -> 500;
http_status(Reason) ->
    %% Every other A2A error maps to 400 (§5.4). An atom outside the table is a
    %% programming error, not a client error, so it stays a 500.
    case lists:member(Reason, a2a_errors()) of
        true -> 400;
        false -> 500
    end.

a2a_errors() ->
    [task_not_cancelable, push_notification_not_supported, unsupported_operation,
     content_type_not_supported, extended_agent_card_not_configured,
     extension_support_required, version_not_supported].

%% Stable English text. Clients must switch on `code', never on this string.
-spec message(reason() | atom()) -> binary().
message(parse_error) -> <<"Parse error">>;
message(invalid_request) -> <<"Invalid Request">>;
message(method_not_found) -> <<"Method not found">>;
message(invalid_params) -> <<"Invalid params">>;
message(internal_error) -> <<"Internal error">>;
message(task_not_found) -> <<"Task not found">>;
message(task_not_cancelable) -> <<"Task cannot be canceled">>;
message(push_notification_not_supported) -> <<"Push notifications are not supported">>;
message(unsupported_operation) -> <<"Unsupported operation">>;
message(content_type_not_supported) -> <<"Content type not supported">>;
message(invalid_agent_response) -> <<"Invalid agent response">>;
message(extended_agent_card_not_configured) -> <<"Extended agent card is not configured">>;
message(extension_support_required) -> <<"A required extension is not supported">>;
message(version_not_supported) -> <<"Protocol version not supported">>;
message(header_mismatch) -> <<"Request headers do not match the request body">>;
message(unsupported_protocol_version) -> <<"Unsupported protocol version">>;
message(unauthorized) -> <<"Unauthorized">>;
message(forbidden) -> <<"Forbidden">>;
message(payload_too_large) -> <<"Payload too large">>;
message(unsupported_media_type) -> <<"Unsupported media type">>;
message(_Other) -> <<"Internal error">>.

%% @doc The `error' member of a JSON-RPC response object.
-spec jsonrpc_error(reason(), term()) -> map().
jsonrpc_error(Reason, Data) ->
    jsonrpc_error(Reason, message(Reason), Data).

-spec jsonrpc_error(reason(), binary(), term()) -> map().
jsonrpc_error(Reason, Msg, undefined) ->
    #{<<"code">> => code(Reason), <<"message">> => Msg};
jsonrpc_error(Reason, Msg, Data) ->
    #{<<"code">> => code(Reason), <<"message">> => Msg, <<"data">> => Data}.
