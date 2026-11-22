%% @doc Unit tests for `mcp_dispatch', the transport-neutral MCP protocol
%% layer (ADR-027 D2-D6).
%%
%% `mcp_listener_SUITE' and `mcp_stdio_SUITE' drive this module end-to-end
%% through a live front end and a router. This suite exercises the layer
%% directly, which is possible because most paths never touch the bus:
%%   * `shape/1' is pure -- it decides request vs notification vs invalid;
%%   * `handle/2' answers envelope/per-request failures, `server/discover',
%%     and the routing refusals (`feature_not_advertised', `refused',
%%     `unknown') without publishing anything.
%%
%% Only the passthrough (`tools/*', ...) and stream (`subscriptions/listen')
%% paths need `broker_security', `broker_router' and `broker_queue'; those are
%% covered by the listener suites, so they are not duplicated here. The tests
%% below set the `features' and `supported_versions' env and call the module
%% directly -- no application or listener has to start.
-module(mcp_dispatch_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([shape_accepts_request_with_binary_id/1,
         shape_accepts_request_with_integer_id/1,
         shape_rejects_null_id/1,
         shape_rejects_empty_binary_id/1,
         shape_accepts_notification_without_id/1,
         shape_rejects_missing_jsonrpc/1,
         shape_rejects_wrong_jsonrpc_version/1,
         shape_rejects_non_binary_method/1,
         shape_rejects_empty_method/1,
         shape_rejects_batch/1,
         notification_produces_no_reply/1,
         invalid_envelope_is_invalid_request_400/1,
         missing_protocol_version_is_invalid_params/1,
         unsupported_protocol_version_is_rejected/1,
         missing_client_capabilities_is_invalid_params/1,
         params_must_be_an_object/1,
         discover_is_answered_locally/1,
         unknown_method_is_method_not_found_404/1,
         feature_not_advertised_is_method_not_found_404/1,
         refused_family_is_method_not_found_404/1,
         http_acl_denial_is_bare_403/1]).

all() ->
    [shape_accepts_request_with_binary_id,
     shape_accepts_request_with_integer_id,
     shape_rejects_null_id,
     shape_rejects_empty_binary_id,
     shape_accepts_notification_without_id,
     shape_rejects_missing_jsonrpc,
     shape_rejects_wrong_jsonrpc_version,
     shape_rejects_non_binary_method,
     shape_rejects_empty_method,
     shape_rejects_batch,
     notification_produces_no_reply,
     invalid_envelope_is_invalid_request_400,
     missing_protocol_version_is_invalid_params,
     unsupported_protocol_version_is_rejected,
     missing_client_capabilities_is_invalid_params,
     params_must_be_an_object,
     discover_is_answered_locally,
     unknown_method_is_method_not_found_404,
     feature_not_advertised_is_method_not_found_404,
     refused_family_is_method_not_found_404,
     http_acl_denial_is_bare_403].

-define(CTX, #{transport => stdio, identity => #{tenant_id => <<"t1">>,
                                                  principal => <<"alice">>}}).
-define(VERSION, <<"2026-07-28">>).

init_per_testcase(_Case, Config) ->
    %% `broker_security' (and its `broker_core' dep) must run for the ACL
    %% refusal path. `broker_proto_mcp' itself is only read for `features' /
    %% `supported_versions' env, so it is loaded (not started) -- starting it
    %% would drag in `broker_router' and require partition configuration.
    ok = ensure_app(broker_security),
    _ = application:load(broker_proto_mcp),
    %% Snapshot the ACL so a case that tightens it cannot leak into the next
    %% suite sharing this CT node (broker_security is a dependency of most
    %% protocol apps, so the decision table is node state, not case state).
    PrevAcl = case application:get_env(broker_security, acl) of
                  {ok, A} -> A;
                  undefined -> undefined
              end,
    ok = set_env(broker_proto_mcp, supported_versions, [?VERSION]),
    ok = set_env(broker_proto_mcp, features, []),
    [{prev_acl, PrevAcl} | Config].

end_per_testcase(_Case, Config) ->
    case lists:keytake(prev_acl, 1, Config) of
        {value, {prev_acl, undefined}, _Rest} ->
            _ = application:unset_env(broker_security, acl),
            ok;
        {value, {prev_acl, Prev}, _Rest} ->
            ok = application:set_env(broker_security, acl, Prev),
            ok
    end,
    ok.

%% A request with a string id and a non-empty method is a request.
shape_accepts_request_with_binary_id(_Config) ->
    Term = #{<<"jsonrpc">> => <<"2.0">>, <<"method">> => <<"ping">>,
             <<"id">> => <<"req-1">>},
    ?assertEqual({ok, request, Term}, mcp_dispatch:shape(Term)).

%% An integer id is also a valid request id.
shape_accepts_request_with_integer_id(_Config) ->
    Term = #{<<"jsonrpc">> => <<"2.0">>, <<"method">> => <<"ping">>,
             <<"id">> => 42},
    ?assertEqual({ok, request, Term}, mcp_dispatch:shape(Term)).

%% `null' id is explicitly forbidden by MCP `basic/index'.
shape_rejects_null_id(_Config) ->
    Term = #{<<"jsonrpc">> => <<"2.0">>, <<"method">> => <<"ping">>,
             <<"id">> => null},
    ?assertEqual({error, invalid_request}, mcp_dispatch:shape(Term)).

%% An empty binary id is not a valid id.
shape_rejects_empty_binary_id(_Config) ->
    Term = #{<<"jsonrpc">> => <<"2.0">>, <<"method">> => <<"ping">>,
             <<"id">> => <<>>},
    ?assertEqual({error, invalid_request}, mcp_dispatch:shape(Term)).

%% A message with no `id' is a notification.
shape_accepts_notification_without_id(_Config) ->
    Term = #{<<"jsonrpc">> => <<"2.0">>, <<"method">> => <<"notifications/x">>},
    ?assertEqual({ok, notification, Term}, mcp_dispatch:shape(Term)).

%% Missing `jsonrpc' is invalid.
shape_rejects_missing_jsonrpc(_Config) ->
    Term = #{<<"method">> => <<"ping">>, <<"id">> => <<"1">>},
    ?assertEqual({error, invalid_request}, mcp_dispatch:shape(Term)).

%% Wrong `jsonrpc' version is invalid.
shape_rejects_wrong_jsonrpc_version(_Config) ->
    Term = #{<<"jsonrpc">> => <<"1.0">>, <<"method">> => <<"ping">>,
             <<"id">> => <<"1">>},
    ?assertEqual({error, invalid_request}, mcp_dispatch:shape(Term)).

%% A non-binary method is invalid.
shape_rejects_non_binary_method(_Config) ->
    Term = #{<<"jsonrpc">> => <<"2.0">>, <<"method">> => 5,
             <<"id">> => <<"1">>},
    ?assertEqual({error, invalid_request}, mcp_dispatch:shape(Term)).

%% An empty method is invalid.
shape_rejects_empty_method(_Config) ->
    Term = #{<<"jsonrpc">> => <<"2.0">>, <<"method">> => <<>>,
             <<"id">> => <<"1">>},
    ?assertEqual({error, invalid_request}, mcp_dispatch:shape(Term)).

%% MCP has no batch: a top-level array is invalid.
shape_rejects_batch(_Config) ->
    ?assertEqual({error, invalid_request},
                 mcp_dispatch:shape([#{<<"jsonrpc">> => <<"2.0">>}])).

%% A notification is answered with no reply (HTTP 202, stdio nothing).
notification_produces_no_reply(_Config) ->
    Term = #{<<"jsonrpc">> => <<"2.0">>, <<"method">> => <<"notifications/x">>},
    ?assertEqual(no_reply, mcp_dispatch:handle(Term, ?CTX)).

%% An invalid envelope is invalid_request with HTTP 400. The id is null
%% because an invalid envelope cannot be relied on to carry a usable id.
invalid_envelope_is_invalid_request_400(_Config) ->
    Term = #{<<"method">> => <<"ping">>, <<"id">> => <<"1">>},
    {json, 400, Resp} = mcp_dispatch:handle(Term, ?CTX),
    ?assertEqual(null, maps:get(<<"id">>, Resp)),
    ?assertEqual(<<"2.0">>, maps:get(<<"jsonrpc">>, Resp)),
    Error = maps:get(<<"error">>, Resp),
    ?assertEqual(-32600, maps:get(<<"code">>, Error)).

%% A request missing the protocol version is invalid_params (the per-request
%% `basic/index' fields are required).
missing_protocol_version_is_invalid_params(_Config) ->
    Term = valid_request(<<"server/discover">>, #{<<"clientCapabilities">> => #{}}),
    {json, 400, Resp} = mcp_dispatch:handle(Term, ?CTX),
    ?assertEqual(-32602, maps:get(<<"code">>, maps:get(<<"error">>, Resp))),
    Reason = maps:get(<<"reason">>, maps:get(<<"data">>, maps:get(<<"error">>, Resp))),
    ?assertEqual(<<"missing _meta.io.modelcontextprotocol/protocolVersion">>, Reason).

%% A request with an unsupported protocol version is rejected.
unsupported_protocol_version_is_rejected(_Config) ->
    Meta = #{<<"io.modelcontextprotocol/protocolVersion">> => <<"1999-01-01">>,
             <<"io.modelcontextprotocol/clientCapabilities">> => #{}},
    Term = valid_request(<<"server/discover">>, #{<<"_meta">> => Meta}),
    {json, Status, Resp} = mcp_dispatch:handle(Term, ?CTX),
    ?assertEqual(400, Status),
    Error = maps:get(<<"error">>, Resp),
    ?assertEqual(-32022, maps:get(<<"code">>, Error)),
    Data = maps:get(<<"data">>, Error),
    ?assertEqual([?VERSION], maps:get(<<"supported">>, Data)),
    ?assertEqual(<<"1999-01-01">>, maps:get(<<"requested">>, Data)).

%% A request missing clientCapabilities is invalid_params.
missing_client_capabilities_is_invalid_params(_Config) ->
    Meta = #{<<"io.modelcontextprotocol/protocolVersion">> => ?VERSION},
    Term = valid_request(<<"server/discover">>, #{<<"_meta">> => Meta}),
    {json, 400, Resp} = mcp_dispatch:handle(Term, ?CTX),
    ?assertEqual(-32602, maps:get(<<"code">>, maps:get(<<"error">>, Resp))),
    Reason = maps:get(<<"reason">>, maps:get(<<"data">>, maps:get(<<"error">>, Resp))),
    ?assertEqual(<<"missing _meta.io.modelcontextprotocol/clientCapabilities">>, Reason).

%% `params' that is not an object is invalid_params.
params_must_be_an_object(_Config) ->
    Term = #{<<"jsonrpc">> => <<"2.0">>, <<"id">> => <<"1">>,
             <<"method">> => <<"server/discover">>,
             <<"params">> => <<"not-an-object">>},
    {json, 400, Resp} = mcp_dispatch:handle(Term, ?CTX),
    ?assertEqual(-32602, maps:get(<<"code">>, maps:get(<<"error">>, Resp))).

%% `server/discover' is answered locally from configuration, never forwarded.
discover_is_answered_locally(_Config) ->
    ok = set_env(broker_proto_mcp, features, [tools]),
    Term = valid_request(<<"server/discover">>, #{<<"_meta">> => full_meta()}),
    {json, 200, Resp} = mcp_dispatch:handle(Term, ?CTX),
    ?assertEqual(<<"1">>, maps:get(<<"id">>, Resp)),
    Result = maps:get(<<"result">>, Resp),
    ?assertEqual(<<"complete">>, maps:get(<<"resultType">>, Result)),
    ?assertEqual([?VERSION], maps:get(<<"supportedVersions">>, Result)).

%% An unknown method is method_not_found with HTTP 404.
unknown_method_is_method_not_found_404(_Config) ->
    Term = valid_request(<<"does/not-exist">>, #{<<"_meta">> => full_meta()}),
    {json, 404, Resp} = mcp_dispatch:handle(Term, ?CTX),
    ?assertEqual(<<"1">>, maps:get(<<"id">>, Resp)),
    Error = maps:get(<<"error">>, Resp),
    ?assertEqual(-32601, maps:get(<<"code">>, Error)),
    ?assertEqual(undefined, maps:get(<<"data">>, Error, undefined)).

%% A known method whose feature is off is method_not_found on 404 (not
%% advertised).
feature_not_advertised_is_method_not_found_404(_Config) ->
    ok = set_env(broker_proto_mcp, features, []),
    Term = valid_request(<<"tools/list">>, #{<<"_meta">> => full_meta()}),
    {json, 404, Resp} = mcp_dispatch:handle(Term, ?CTX),
    Error = maps:get(<<"error">>, Resp),
    ?assertEqual(-32601, maps:get(<<"code">>, Error)),
    Data = maps:get(<<"data">>, Error),
    ?assertEqual(<<"feature not advertised">>, maps:get(<<"reason">>, Data)),
    ?assertEqual(<<"tools">>, maps:get(<<"feature">>, Data)).

%% A refused family (sampling/*) is method_not_found on 404.
refused_family_is_method_not_found_404(_Config) ->
    Term = valid_request(<<"sampling/createMessage">>, #{<<"_meta">> => full_meta()}),
    {json, 404, Resp} = mcp_dispatch:handle(Term, ?CTX),
    Error = maps:get(<<"error">>, Resp),
    ?assertEqual(-32601, maps:get(<<"code">>, Error)),
    Data = maps:get(<<"data">>, Error),
    ?assertEqual(<<"sampling is not supported">>, maps:get(<<"reason">>, Data)).

%% An HTTP ACL refusal is a bare transport decision (403), not a JSON-RPC
%% error. Uses a passthrough method (tools/*) which goes through authorize.
http_acl_denial_is_bare_403(_Config) ->
    ok = set_env(broker_proto_mcp, features, [tools]),
    ok = set_env(broker_security, acl,
                 [#{effect => deny, principal => <<"*">>, action => publish}]),
    Ctx = maps:put(transport, http, ?CTX),
    Term = valid_request(<<"tools/list">>, #{<<"_meta">> => full_meta()}),
    ?assertEqual({bare, 403}, mcp_dispatch:handle(Term, Ctx)).

%%%_* Helpers ---------------------------------------------------------------

%% A well-formed request term: jsonrpc, method, id, params._meta.
valid_request(Method, Params) ->
    #{<<"jsonrpc">> => <<"2.0">>, <<"id">> => <<"1">>, <<"method">> => Method,
      <<"params">> => Params}.

full_meta() ->
    #{<<"io.modelcontextprotocol/protocolVersion">> => ?VERSION,
      <<"io.modelcontextprotocol/clientCapabilities">> => #{}}.

ensure_app(App) ->
    case application:ensure_all_started(App) of
        {ok, _} -> ok;
        {error, {already_started, App}} -> ok
    end.

set_env(App, Key, Value) ->
    case application:get_env(App, Key) of
        {ok, Value} -> ok;
        _ -> application:set_env(App, Key, Value)
    end.
