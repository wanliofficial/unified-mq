%% @doc Unit tests for `a2a_rpc', the S9-A/B JSON-RPC entry point.
%%
%% `a2a_listener_SUITE' drives the four task lifecycle methods end-to-end
%% through a live socket, an ACL and a registry. This suite calls `handle/1'
%% directly on a synthetic `agent_rpc_http:request()' map, which is possible for
%% everything that resolves before the bus is touched:
%%   * routing -- `/health' is 200, an unroutable GET and PUT/DELETE are 405, a
%%     POST off the RPC path is 404;
%%   * the transport gates -- a non-JSON content type is 415 and a missing
%%     credential under `require_auth' is a 401 Bearer challenge;
%%   * the `A2A-Version' negotiation and the JSON-RPC envelope gates (parse
%%     error, invalid request) -- all 400;
%%   * the pre-authorization refusals -- an unknown method is `method_not_found'
%%     and the task-id/message parameter checks fail as `invalid_params' before
%%     any ACL lookup (A2A §5.4 keeps these on HTTP 200, not 4xx).
%%
%% The gates that reach `with_authorization' run with `require_auth' off, so the
%% reply is the verdict rather than a 401; `default_tenant/0' still reads
%% `broker_security', so that app is loaded. The authenticated dispatch (which
%% needs `broker_router'/`broker_meta') stays covered by `a2a_listener_SUITE'.
-module(a2a_rpc_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [health_is_200_ok,
     get_unknown_path_is_405,
     put_is_405,
     delete_is_405,
     post_off_rpc_path_is_404,
     post_rpc_root_proceeds_to_gates,
     content_type_not_json_is_415,
     content_type_a2a_json_is_accepted,
     require_auth_without_credential_is_401,
     unparseable_version_is_400,
     unsupported_version_is_400,
     parse_error_is_400,
     invalid_request_is_400,
     unknown_method_is_method_not_found_200,
     send_message_missing_message_is_invalid_params,
     get_task_missing_id_is_invalid_params,
     cancel_task_missing_id_is_invalid_params,
     subscribe_task_missing_id_is_invalid_params].

init_per_suite(Config) ->
    %% `agent_rpc_auth:anonymous/0' (reached by the gates that run with
    %% `require_auth' off) reads `broker_security:default_tenant/0'. It is a pure
    %% env read, so loading -- not starting -- the app is enough, and the ACL
    %% decision table is never consulted on these paths.
    _ = application:load(broker_security),
    Config.

end_per_suite(_Config) ->
    ok.

%% The gates read `require_auth' and `supported_versions' from env. Snapshot them
%% so a tightened setting cannot leak into a later suite sharing this node.
init_per_testcase(_Case, Config) ->
    PrevAuth = application:get_env(broker_proto_a2a, require_auth),
    PrevVersions = application:get_env(broker_proto_a2a, supported_versions),
    ok = application:set_env(broker_proto_a2a, require_auth, false),
    ok = application:set_env(broker_proto_a2a, supported_versions, [<<"1.0">>]),
    [{prev_require_auth, PrevAuth}, {prev_supported_versions, PrevVersions} | Config].

end_per_testcase(_Case, Config) ->
    restore(broker_proto_a2a, require_auth, ?config(prev_require_auth, Config)),
    restore(broker_proto_a2a, supported_versions,
            ?config(prev_supported_versions, Config)),
    ok.

restore(App, Key, undefined) ->
    _ = application:unset_env(App, Key),
    ok;
restore(App, Key, Value) ->
    ok = application:set_env(App, Key, Value).

%%%_* Routing -----------------------------------------------------------------

health_is_200_ok(_Config) ->
    {200, _Headers, Body, Keep} = a2a_rpc:handle(http_get(<<"/health">>)),
    ?assertEqual(<<"{\"status\":\"ok\"}">>, iolist_to_binary(Body)),
    ?assertEqual(false, Keep),
    ok.

get_unknown_path_is_405(_Config) ->
    {405, Headers, Body, Keep} = a2a_rpc:handle(http_get(<<"/nope">>)),
    ?assertEqual(<<"GET, POST">>, proplists:get_value(<<"allow">>, Headers)),
    ?assertEqual(<<"method_not_allowed">>, error_value(Body)),
    ?assertEqual(false, Keep),
    ok.

put_is_405(_Config) ->
    ?assertMatch({405, _, _, _}, a2a_rpc:handle(http_put(<<"/">>))),
    ok.

delete_is_405(_Config) ->
    ?assertMatch({405, _, _, _}, a2a_rpc:handle(http_delete(<<"/">>))),
    ok.

%% POST is only routed to JSON-RPC on `/' and `/rpc'; anything else is 404.
post_off_rpc_path_is_404(_Config) ->
    {404, _Headers, Body, Keep} = a2a_rpc:handle(http_post(<<"/other">>, json_headers())),
    ?assertEqual(<<"not_found">>, error_value(Body)),
    ?assertEqual(false, Keep),
    ok.

%% `/' is an RPC path, so the request proceeds into the transport gates and is
%% refused there (415 here), never by the router.
post_rpc_root_proceeds_to_gates(_Config) ->
    Resp = a2a_rpc:handle(http_post(<<"/">>, #{<<"content-type">> => <<"text/plain">>})),
    ?assertMatch({415, _, <<>>, _}, Resp),
    ok.

%%%_* Transport gates ---------------------------------------------------------

content_type_not_json_is_415(_Config) ->
    ?assertMatch({415, _, <<>>, _},
                 a2a_rpc:handle(http_post(<<"/">>,
                                          #{<<"content-type">> => <<"text/plain">>}))),
    ok.

%% `application/a2a+json' is an accepted alias, so it clears the 415 gate (the
%% request then fails later, on the envelope, not on content type).
content_type_a2a_json_is_accepted(_Config) ->
    Req = http_post(<<"/">>, #{<<"content-type">> => <<"application/a2a+json">>},
               <<"not json">>),
    ?assertNotMatch({415, _, _, _}, a2a_rpc:handle(Req)),
    ok.

require_auth_without_credential_is_401(_Config) ->
    ok = application:set_env(broker_proto_a2a, require_auth, true),
    {401, Headers, <<>>, Keep} =
        a2a_rpc:handle(http_post(<<"/">>, json_headers())),
    ?assertEqual(<<"Bearer realm=\"a2a\"">>,
                 proplists:get_value(<<"www-authenticate">>, Headers)),
    ?assertEqual(false, Keep),
    ok.

%%%_* Version and envelope gates (run with require_auth off) ------------------

unparseable_version_is_400(_Config) ->
    Req = http_post(<<"/">>, json_headers(<<"not-a-version">>), bad_body()),
    {400, _Headers, Body, _Keep} = a2a_rpc:handle(Req),
    ?assertEqual(<<"Protocol version not supported">>, error_message(Body)),
    ok.

%% A well-formed but unsupported version (the client is on 2.0, we serve 1.0).
unsupported_version_is_400(_Config) ->
    Req = http_post(<<"/">>, json_headers(<<"2.0">>), bad_body()),
    {400, _Headers, Body, _Keep} = a2a_rpc:handle(Req),
    ?assertEqual(<<"Protocol version not supported">>, error_message(Body)),
    ok.

parse_error_is_400(_Config) ->
    Req = http_post(<<"/">>, json_headers(<<"1.0">>), <<"{ not json">>),
    {400, _Headers, Body, _Keep} = a2a_rpc:handle(Req),
    ?assertEqual(<<"Parse error">>, error_message(Body)),
    ok.

invalid_request_is_400(_Config) ->
    %% A JSON object with no `jsonrpc': request/1 rejects it as invalid_request.
    Body = js_encode(#{<<"method">> => <<"SendMessage">>, <<"id">> => <<"1">>}),
    Req = http_post(<<"/">>, json_headers(<<"1.0">>), Body),
    {400, _Headers, RespBody, _Keep} = a2a_rpc:handle(Req),
    ?assertEqual(<<"Invalid Request">>, error_message(RespBody)),
    ok.

%%%_* Pre-authorization refusals (HTTP 200, not 4xx) -------------------------

%% An unknown method with a valid envelope is refused before any ACL lookup.
unknown_method_is_method_not_found_200(_Config) ->
    Body = js_encode(#{<<"jsonrpc">> => <<"2.0">>, <<"id">> => <<"1">>,
                       <<"method">> => <<"Bogus">>}),
    Req = http_post(<<"/">>, json_headers(<<"1.0">>), Body),
    {200, _Headers, RespBody, _Keep} = a2a_rpc:handle(Req),
    ?assertEqual(<<"Method not found">>, error_message(RespBody)),
    ok.

%% SendMessage validates its `message' parameter before the authorization step,
%% so a missing message is invalid_params without consulting the ACL.
send_message_missing_message_is_invalid_params(_Config) ->
    Body = js_encode(#{<<"jsonrpc">> => <<"2.0">>, <<"id">> => <<"1">>,
                       <<"method">> => <<"SendMessage">>, <<"params">> => #{}}),
    Req = http_post(<<"/">>, json_headers(<<"1.0">>), Body),
    {200, _Headers, RespBody, _Keep} = a2a_rpc:handle(Req),
    ?assertEqual(<<"Invalid params">>, error_message(RespBody)),
    ok.

get_task_missing_id_is_invalid_params(_Config) ->
    ?assertEqual(200, status_of(a2a_rpc:handle(rpc(<<"GetTask">>, #{}, <<"1">>)))),
    ok.

cancel_task_missing_id_is_invalid_params(_Config) ->
    ?assertEqual(200, status_of(a2a_rpc:handle(rpc(<<"CancelTask">>, #{}, <<"1">>)))),
    ok.

%% SubscribeToTask takes a task id and validates it before authorizing.
subscribe_task_missing_id_is_invalid_params(_Config) ->
    ?assertEqual(200, status_of(a2a_rpc:handle(rpc(<<"SubscribeToTask">>, #{}, <<"1">>)))),
    ok.

%%%_* Helpers ---------------------------------------------------------------

json_headers() -> #{<<"content-type">> => <<"application/json">>}.

json_headers(Version) ->
    #{<<"content-type">> => <<"application/json">>,
      <<"a2a-version">> => Version}.

bad_body() -> <<"{ not json">>.

http_get(Path) -> #{method => get, path => Path, headers => #{}}.

http_put(Path) -> #{method => put, path => Path, headers => #{}}.

http_delete(Path) -> #{method => delete, path => Path, headers => #{}}.

http_post(Path, Headers) -> http_post(Path, Headers, <<"{}">>).

http_post(Path, Headers, Body0) when is_map(Body0) ->
    http_post(Path, Headers, iolist_to_binary(js_encode(Body0)));
http_post(Path, Headers, Body) when is_binary(Body) ->
    #{method => post, path => Path,
      version => {1, 1}, headers => Headers, body => Body, keep_alive => false}.

%% Build a JSON-RPC request for `Method' with `Params', at version 1.0.
rpc(Method, Params, Id) ->
    Term = #{<<"jsonrpc">> => <<"2.0">>, <<"id">> => Id,
             <<"method">> => Method, <<"params">> => Params},
    http_post(<<"/">>, json_headers(<<"1.0">>), Term).

js_encode(Term) -> iolist_to_binary(agent_rpc_json:encode(Term)).

%% HTTP status of an error response, decoded from the body.
status_of({Status, _Headers, _Body, _Keep}) -> Status.

%% The JSON-RPC error object's `message' member (the body is a JSON iolist).
error_message(Body) when is_list(Body) ->
    error_message(iolist_to_binary(Body));
error_message(Body) when is_binary(Body) ->
    {ok, Map} = agent_rpc_json:decode(Body),
    maps:get(<<"message">>, maps:get(<<"error">>, Map)).

%% The bare `{"error": Reason}' body of a routing refusal.
error_value(Body) when is_list(Body) ->
    error_value(iolist_to_binary(Body));
error_value(Body) when is_binary(Body) ->
    {ok, Map} = agent_rpc_json:decode(Body),
    maps:get(<<"error">>, Map).
