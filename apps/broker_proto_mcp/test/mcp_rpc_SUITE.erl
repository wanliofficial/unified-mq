%% @doc Unit tests for `mcp_rpc', the Streamable HTTP transport for MCP
%% 2026-07-28 (ADR-027 §D7).
%%
%% `mcp_listener_SUITE' drives this module end-to-end through a real socket and
%% a router. This suite calls `handle/1' directly on a synthetic
%% `agent_rpc_http:request()' map, which is possible because every transport
%% gate resolves before the request ever reaches the bus:
%%   * routing -- wrong path is 404, non-POST is 405 with `Allow: POST';
%%   * the DNS-rebinding defence -- a present, non-loopback Origin is 403;
%%   * content negotiation -- a non-covering Accept is 406, a non-JSON content
%%     type is 415;
%%   * the mirrored headers -- MCP-Protocol-Version / Mcp-Method / Mcp-Name must
%%     agree with the body, each failure a -32020 on HTTP 400;
%%   * the body gates -- an unparseable body (-32700) and an invalid envelope
%%     (-32600) are 400.
%%
%% The header/body gates are checked before authentication, so they need no
%% running dependency. The body gates that reach `with_identity' (parse error,
%% invalid envelope, notification) run with `require_auth' off, so the reply is
%% the body verdict rather than a 401; `default_tenant' still reads
%% `broker_security', so that app is loaded. The full authenticated dispatch
%% and the SSE stream, which need `broker_router'/'broker_queue', stay covered
%% by `mcp_listener_SUITE'.
-module(mcp_rpc_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(VERSION, <<"2026-07-28">>).

all() ->
    [wrong_path_is_404,
     get_method_is_405_with_allow_post,
     delete_method_is_405,
     query_string_is_stripped_before_path_match,
     non_loopback_origin_is_403_bodiless,
     missing_origin_is_allowed,
     disallowed_origin_via_config_is_403,
     accept_missing_is_not_rejected,
     accept_without_json_is_406,
     accept_without_sse_is_406,
     accept_with_both_types_passes_negotation,
     wildcard_accept_is_accepted,
     content_type_not_json_is_415,
     content_type_with_charset_passes_content_type_gate,
     missing_protocol_version_header_is_header_mismatch_400,
     protocol_version_header_mismatch_is_400,
     missing_method_header_is_header_mismatch_400,
     method_header_mismatch_is_400,
     name_header_required_for_tools_call_is_400,
     name_header_required_for_prompts_get_is_400,
     name_header_required_for_resources_read_is_400,
     name_header_mismatch_is_400,
     unparseable_body_is_parse_error_400,
     invalid_envelope_is_invalid_request_400,
     notification_is_accepted_as_no_content,
     require_auth_on_without_credential_is_401,
     path_configurable_via_env,
     max_body_bytes_capped_by_broker_limits].

init_per_suite(Config) ->
    %% `broker_security' backs `agent_rpc_auth:default_tenant/0', reached by the
    %% body gates that run with `require_auth' off. It is loaded, not started:
    %% default_tenant/0 is a pure env read and the ACL decision table is never
    %% consulted on these paths, so there is nothing to leak across suites.
    _ = application:load(broker_security),
    Config.

end_per_suite(_Config) ->
    ok.

%% The transport gates that precede authentication read `require_auth',
%% `allowed_origins' and `path' from env. Snapshot the two this suite changes so
%% a tightened setting cannot leak into a later suite sharing this CT node.
init_per_testcase(_Case, Config) ->
    PrevAuth = application:get_env(broker_proto_mcp, require_auth),
    PrevOrigins = application:get_env(broker_proto_mcp, allowed_origins),
    ok = application:set_env(broker_proto_mcp, require_auth, false),
    ok = application:set_env(broker_proto_mcp, allowed_origins, []),
    [{prev_require_auth, PrevAuth}, {prev_allowed_origins, PrevOrigins} | Config].

end_per_testcase(_Case, Config) ->
    restore(broker_proto_mcp, require_auth, ?config(prev_require_auth, Config)),
    restore(broker_proto_mcp, allowed_origins,
            ?config(prev_allowed_origins, Config)),
    ok.

%% `Prev' is whatever `application:get_env/2' returned: `{ok, V}' when the key was
%% set, `undefined' when it was not. The old `Value' clause stored the `{ok, V}'
%% tuple verbatim, so every test case rewrapped `require_auth' / `allowed_origins'
%% one level deeper ({ok,{ok,...}}). A wrapped `require_auth' then reached
%% `mcp_rpc:require_auth/0' consumers as a non-boolean and crashed `function_clause'
%% -> a 500 in a later suite sharing this node. Unwrap the single `{ok, _}' layer
%% `get_env' added; a value that is itself `{ok, _}' still restores exactly, since
%% `get_env' only ever wraps once. Mirrors a2a_rpc_SUITE's restore/3.
restore(App, Key, {ok, Value}) ->
    ok = application:set_env(App, Key, Value);
restore(App, Key, undefined) ->
    _ = application:unset_env(App, Key),
    ok;
restore(App, Key, Value) ->
    ok = application:set_env(App, Key, Value).

%%%_* Routing ----------------------------------------------------------------

wrong_path_is_404(_Config) ->
    Resp = mcp_rpc:handle(req(get, <<"/mcp">>, #{})),
    ?assertMatch({json, 404, _, <<>>, _}, Resp),
    ok.

get_method_is_405_with_allow_post(_Config) ->
    {json, 405, Headers, <<>>, _Keep} = mcp_rpc:handle(req(get, <<"/">>, #{})),
    ?assertEqual(<<"POST">>, proplists:get_value(<<"allow">>, Headers)),
    ok.

delete_method_is_405(_Config) ->
    ?assertMatch({json, 405, _, <<>>, _},
                 mcp_rpc:handle(req(delete, <<"/">>, #{}))),
    ok.

%% The path match ignores the query string.
query_string_is_stripped_before_path_match(_Config) ->
    Headers = good_post_headers(),
    ?assertMatch({json, 404, _, <<>>, _},
                 mcp_rpc:handle(req(post, <<"/other?x=1">>, Headers))),
    ok.

%%%_* Origin gate (DNS-rebinding defence) ------------------------------------

non_loopback_origin_is_403_bodiless(_Config) ->
    Headers = maps:merge(good_post_headers(),
                         #{<<"origin">> => <<"https://evil.example.com">>}),
    ?assertEqual({json, 403, [], <<>>, false},
                 mcp_rpc:handle(req(post, <<"/">>, Headers))),
    ok.

%% No Origin header at all is allowed (same-origin / non-browser client). It
%% passes the origin gate and is then refused downstream, never by Origin.
missing_origin_is_allowed(_Config) ->
    Headers = good_post_headers(),
    ?assertNotMatch({json, 403, _, _, _},
                    mcp_rpc:handle(req(post, <<"/">>, Headers))),
    ok.

disallowed_origin_via_config_is_403(_Config) ->
    ok = application:set_env(broker_proto_mcp, allowed_origins,
                             [<<"https://good.example.com">>]),
    Headers = maps:merge(good_post_headers(),
                         #{<<"origin">> => <<"https://evil.example.com">>}),
    ?assertEqual({json, 403, [], <<>>, false},
                 mcp_rpc:handle(req(post, <<"/">>, Headers))),
    ok.

%%%_* Accept gate ------------------------------------------------------------

%% A missing Accept header is treated as acceptable (the gate only refuses an
%% explicit, non-covering list).
accept_missing_is_not_rejected(_Config) ->
    Headers = maps:remove(<<"accept">>, good_post_headers()),
    ?assertNotMatch({json, 406, _, _, _},
                    mcp_rpc:handle(req(post, <<"/">>, Headers))),
    ok.

accept_without_json_is_406(_Config) ->
    Headers = maps:merge(good_post_headers(),
                         #{<<"accept">> => <<"text/event-stream">>}),
    ?assertMatch({json, 406, _, <<>>, _},
                 mcp_rpc:handle(req(post, <<"/">>, Headers))),
    ok.

accept_without_sse_is_406(_Config) ->
    Headers = maps:merge(good_post_headers(),
                         #{<<"accept">> => <<"application/json">>}),
    ?assertMatch({json, 406, _, <<>>, _},
                 mcp_rpc:handle(req(post, <<"/">>, Headers))),
    ok.

%% Both required media types present: the request clears content negotiation
%% and is then refused by the (absent) mirrored headers, not by Accept.
accept_with_both_types_passes_negotation(_Config) ->
    Resp = mcp_rpc:handle(req(post, <<"/">>, good_post_headers())),
    ?assertNotMatch({json, 406, _, _, _}, Resp),
    ok.

wildcard_accept_is_accepted(_Config) ->
    Headers = maps:merge(good_post_headers(),
                         #{<<"accept">> => <<"*/*">>}),
    ?assertNotMatch({json, 406, _, _, _},
                    mcp_rpc:handle(req(post, <<"/">>, Headers))),
    ok.

%%%_* Content-Type gate ------------------------------------------------------

content_type_not_json_is_415(_Config) ->
    Headers = maps:put(<<"content-type">>, <<"text/plain">>,
                       good_post_headers()),
    ?assertMatch({json, 415, _, <<>>, _},
                 mcp_rpc:handle(req(post, <<"/">>, Headers))),
    ok.

%% `agent_rpc_http:content_type/1' strips parameters, so a charset suffix on
%% `application/json' still passes the content-type gate (it is refused later,
%% by the mirrored headers, not by a 415).
content_type_with_charset_passes_content_type_gate(_Config) ->
    Headers = maps:merge(good_post_headers(),
                         #{<<"content-type">> => <<"application/json; charset=utf-8">>}),
    ?assertNotMatch({json, 415, _, _, _},
                    mcp_rpc:handle(req(post, <<"/">>, Headers))),
    ok.

%%%_* Mirrored headers (gates 5-7), all -32020 on HTTP 400 ------------------

missing_protocol_version_header_is_header_mismatch_400(_Config) ->
    Headers = maps:remove(<<"mcp-protocol-version">>, good_post_headers()),
    {json, 400, _Hdrs, Body, _Keep} =
        mcp_rpc:handle(req(post, <<"/">>, Headers)),
    ?assertEqual(-32020, code_of(Body)),
    ?assertEqual(<<"MCP-Protocol-Version header is required">>, message_of(Body)),
    ok.

protocol_version_header_mismatch_is_400(_Config) ->
    Headers = maps:put(<<"mcp-protocol-version">>, <<"1999-01-01">>,
                       good_post_headers()),
    {json, 400, _Hdrs, Body, _Keep} =
        mcp_rpc:handle(req(post, <<"/">>, Headers)),
    ?assertEqual(-32020, code_of(Body)),
    ok.

missing_method_header_is_header_mismatch_400(_Config) ->
    Headers = maps:remove(<<"mcp-method">>, good_post_headers()),
    {json, 400, _Hdrs, Body, _Keep} =
        mcp_rpc:handle(req(post, <<"/">>, Headers)),
    ?assertEqual(-32020, code_of(Body)),
    ?assertEqual(<<"Mcp-Method header is required">>, message_of(Body)),
    ok.

method_header_mismatch_is_400(_Config) ->
    Headers = maps:put(<<"mcp-method">>, <<"prompts/get">>, good_post_headers()),
    {json, 400, _Hdrs, Body, _Keep} =
        mcp_rpc:handle(req(post, <<"/">>, Headers)),
    ?assertEqual(-32020, code_of(Body)),
    ?assertEqual(<<"Mcp-Method header does not match the request body">>,
                 message_of(Body)),
    ok.

%% `Mcp-Name' is required for the three methods that carry one. The body method
%% must match the `Mcp-Method' header, so these build a matching body.
name_header_required_for_tools_call_is_400(_Config) ->
    Headers = maps:remove(<<"mcp-name">>, tools_call_headers()),
    {json, 400, _Hdrs, Body, _Keep} =
        mcp_rpc:handle(named_req(<<"tools/call">>, <<"name">>, <<"weather">>,
                                 Headers)),
    ?assertEqual(-32020, code_of(Body)),
    ?assertEqual(<<"Mcp-Name header is required">>, message_of(Body)),
    ok.

name_header_required_for_prompts_get_is_400(_Config) ->
    Headers = maps:remove(<<"mcp-name">>, prompts_get_headers()),
    {json, 400, _Hdrs, Body, _Keep} =
        mcp_rpc:handle(named_req(<<"prompts/get">>, <<"name">>, <<"greet">>,
                                 Headers)),
    ?assertEqual(<<"Mcp-Name header is required">>, message_of(Body)),
    ok.

name_header_required_for_resources_read_is_400(_Config) ->
    Headers = maps:remove(<<"mcp-name">>, resources_read_headers()),
    {json, 400, _Hdrs, Body, _Keep} =
        mcp_rpc:handle(named_req(<<"resources/read">>, <<"uri">>,
                                 <<"file:///x">>, Headers)),
    ?assertEqual(<<"Mcp-Name header is required">>, message_of(Body)),
    ok.

name_header_mismatch_is_400(_Config) ->
    Headers = maps:put(<<"mcp-name">>, <<"wrong-name">>, tools_call_headers()),
    {json, 400, _Hdrs, Body, _Keep} =
        mcp_rpc:handle(named_req(<<"tools/call">>, <<"name">>, <<"weather">>,
                                 Headers)),
    ?assertEqual(-32020, code_of(Body)),
    ?assertEqual(<<"Mcp-Name header does not match the request body">>,
                 message_of(Body)),
    ok.

%%%_* Body gates (run with require_auth off) ---------------------------------

unparseable_body_is_parse_error_400(_Config) ->
    Headers = good_post_headers(),
    Req = req(post, <<"/">>, Headers, <<"not json">>),
    {json, 400, _Hdrs, Body, _Keep} = mcp_rpc:handle(Req),
    ?assertEqual(-32700, code_of(Body)),
    ?assertEqual(<<"Parse error">>, message_of(Body)),
    ok.

invalid_envelope_is_invalid_request_400(_Config) ->
    Headers = good_post_headers(),
    %% No `jsonrpc': shape/1 rejects it as invalid_request.
    Body0 = js_encode(#{<<"method">> => <<"ping">>, <<"id">> => <<"1">>}),
    Req = req(post, <<"/">>, Headers, Body0),
    {json, 400, _Hdrs, Body, _Keep} = mcp_rpc:handle(Req),
    ?assertEqual(-32600, code_of(Body)),
    ?assertEqual(<<"Invalid Request">>, message_of(Body)),
    ok.

notification_is_accepted_as_no_content(_Config) ->
    Headers = good_post_headers(),
    Term = #{<<"jsonrpc">> => <<"2.0">>,
             <<"method">> => <<"notifications/initialized">>},
    Req = req(post, <<"/">>, Headers, js_encode(Term)),
    %% A notification is accepted with 202; the keep flag echoes the request's.
    ?assertEqual({no_content, true}, mcp_rpc:handle(Req)),
    ok.

%%%_* Authentication gate ----------------------------------------------------

%% With require_auth on and no credential, the body verdict is never reached:
%% the request is refused with a Bearer challenge first.
require_auth_on_without_credential_is_401(_Config) ->
    ok = application:set_env(broker_proto_mcp, require_auth, true),
    Headers = good_post_headers(),
    {json, 401, RespHeaders, <<>>, _Keep} =
        mcp_rpc:handle(req(post, <<"/">>, Headers)),
    ?assertEqual(<<"Bearer realm=\"mcp\"">>,
                 proplists:get_value(<<"www-authenticate">>, RespHeaders)),
    ok.

%%%_* Config accessors -------------------------------------------------------

path_configurable_via_env(_Config) ->
    ok = application:set_env(broker_proto_mcp, path, <<"/mcp">>),
    ?assertEqual(<<"/mcp">>, mcp_rpc:path()),
    ok.

%% max_body_bytes is the smaller of the configured cap and the broker-wide
%% message limit; with the default config it is the broker limit that binds.
max_body_bytes_capped_by_broker_limits(_Config) ->
    ?assertEqual(broker_limits:max_message_bytes(), mcp_rpc:max_body_bytes()),
    ok.

%%%_* Helpers ---------------------------------------------------------------

%% A POST header set that clears every gate: path/method/origin/accept/content-type
%% AND the mirrored MCP-Protocol-Version / Mcp-Method headers matching the
%% default `server/discover' body. Cases that exercise one gate remove or corrupt
%% just that header.
good_post_headers() ->
    #{<<"content-type">> => <<"application/json">>,
      <<"accept">> => <<"application/json, text/event-stream">>,
      <<"mcp-protocol-version">> => ?VERSION,
      <<"mcp-method">> => <<"server/discover">>}.

tools_call_headers() ->
    maps:put(<<"mcp-method">>, <<"tools/call">>, good_post_headers()).

prompts_get_headers() ->
    maps:put(<<"mcp-method">>, <<"prompts/get">>, good_post_headers()).

resources_read_headers() ->
    maps:put(<<"mcp-method">>, <<"resources/read">>, good_post_headers()).

%% Build a request map whose body method matches the `Mcp-Method' header, so
%% the gate proceeds past the method check to the `Mcp-Name' check. The body
%% carries the protocol version in `_meta' so the version gate agrees with the
%% header.
named_req(Method, NameKey, NameValue, Headers) ->
    Term = #{<<"jsonrpc">> => <<"2.0">>, <<"id">> => <<"1">>,
             <<"method">> => Method,
             <<"params">> => #{NameKey => NameValue,
                               <<"_meta">> =>
                                   #{<<"io.modelcontextprotocol/protocolVersion">>
                                         => ?VERSION}}},
    req(post, <<"/">>, Headers, Term).

%% Build a request map. `Body0' defaults to a well-formed `server/discover'
%% request whose `_meta' protocol version matches the default headers' intent.
req(Method, Path, Headers) ->
    req(Method, Path, Headers, discover_body()).

req(Method, Path, Headers, Body0) when is_map(Body0) ->
    req(Method, Path, Headers, iolist_to_binary(agent_rpc_json:encode(Body0)));
req(Method, Path, Headers, Body) when is_binary(Body) ->
    #{method => Method, path => Path, version => {1, 1},
      headers => Headers, body => Body, keep_alive => true}.

%% A discover request: method + id + a valid `_meta' version.
discover_body() ->
    #{<<"jsonrpc">> => <<"2.0">>, <<"id">> => <<"1">>,
      <<"method">> => <<"server/discover">>,
      <<"params">> => #{<<"_meta">> =>
                            #{<<"io.modelcontextprotocol/protocolVersion">> =>
                                  ?VERSION}}}.

js_encode(Term) ->
    iolist_to_binary(agent_rpc_json:encode(Term)).

code_of(Body) when is_binary(Body) ->
    {ok, Map} = agent_rpc_json:decode(Body),
    code_of(Map);
code_of(Body) when is_list(Body) ->
    {ok, Map} = agent_rpc_json:decode(iolist_to_binary(Body)),
    code_of(Map);
code_of(Body) when is_map(Body) ->
    maps:get(<<"code">>, maps:get(<<"error">>, Body)).

%% `header_error/3' builds the error with a custom message and no `data'; the
%% body gates (`error_body/4') use the standard message for the reason atom.
%% Both land the human-readable string in `error.message'.
message_of(Body) when is_binary(Body) ->
    {ok, Map} = agent_rpc_json:decode(Body),
    message_of(Map);
message_of(Body) when is_list(Body) ->
    {ok, Map} = agent_rpc_json:decode(iolist_to_binary(Body)),
    message_of(Map);
message_of(Body) when is_map(Body) ->
    maps:get(<<"message">>, maps:get(<<"error">>, Body)).
