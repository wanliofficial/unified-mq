%% @doc HTTP/1.1 framing over a real socket.
%%
%% `agent_rpc_http' is handed `ranch_tcp' and a plain `gen_tcp' socket here:
%% that is exactly the call the A2A listener makes, so the suite exercises the
%% code path rather than a mock. Each case starts a fresh listener on `port 0'
%% (never 8081; the fixed port belongs to a deployment, not to CT).
-module(agent_rpc_http_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(LIMITS, #{body_bytes => 32, request_timeout_ms => 5000}).

all() ->
    [content_length_body,
     chunked_body,
     expect_100_continue,
     too_large_rejected_without_reading_body,
     chunked_too_large_rejected,
     connection_close_is_honoured,
     duplicate_headers_are_comma_joined,
     path_and_method_are_normalized,
     response_has_content_length_and_close,
     response_keep_alive_header,
     keep_alive_rules,
     accepts_json,
     chunked_response_omits_content_length].

content_length_body(_Config) ->
    R = request(<<"POST /rpc HTTP/1.1\r\nHost: h\r\n"
                 "Content-Type: application/json\r\n"
                 "Content-Length: 5\r\n\r\nhello">>),
    ?assertMatch({ok, #{method := post, path := <<"/rpc">>, body := <<"hello">>,
                        keep_alive := true}},
                 R),
    {ok, Req} = R,
    ?assertEqual(<<"application/json">>,
                 agent_rpc_http:content_type(maps:get(headers, Req))).

chunked_body(_Config) ->
    R = request(<<"POST /rpc HTTP/1.1\r\nHost: h\r\n"
                 "Transfer-Encoding: chunked\r\n\r\n"
                 "5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n">>),
    ?assertMatch({ok, #{body := <<"hello world">>}}, R).

expect_100_continue(_Config) ->
    {LSock, Port} = listen(),
    Parent = self(),
    spawn(fun() ->
                  {ok, S} = gen_tcp:accept(LSock),
                  Result = agent_rpc_http:read_request(ranch_tcp, S, ?LIMITS),
                  Parent ! {request_result, Result},
                  gen_tcp:close(S)
          end),
    {ok, C} = connect(Port),
    ok = gen_tcp:send(C, <<"POST /rpc HTTP/1.1\r\nHost: h\r\n"
                          "Content-Length: 2\r\n"
                          "Expect: 100-continue\r\n\r\n">>),
    {ok, Interim} = gen_tcp:recv(C, 0, 2000),
    ?assertEqual(<<"HTTP/1.1 100 Continue\r\n\r\n">>, Interim),
    ok = gen_tcp:send(C, <<"hi">>),
    receive
        {request_result, Result} ->
            ?assertMatch({ok, #{body := <<"hi">>}}, Result)
    after 5000 ->
            ct:fail(no_request_result)
    end,
    gen_tcp:close(C),
    gen_tcp:close(LSock).

too_large_rejected_without_reading_body(_Config) ->
    %% The cap is checked before any body byte is read: a lying Content-Length
    %% must not be able to make the server allocate what it claims.
    R = request(<<"POST /rpc HTTP/1.1\r\nHost: h\r\n"
                 "Content-Length: 64\r\n\r\n">>),
    ?assertEqual({error, payload_too_large}, R).

chunked_too_large_rejected(_Config) ->
    R = request(<<"POST /rpc HTTP/1.1\r\nHost: h\r\n"
                 "Transfer-Encoding: chunked\r\n\r\n"
                 "28\r\n0123456789012345678901234567890123456789\r\n0\r\n\r\n">>),
    ?assertEqual({error, payload_too_large}, R).

connection_close_is_honoured(_Config) ->
    R = request(<<"POST /rpc HTTP/1.1\r\nHost: h\r\nConnection: close\r\n"
                 "Content-Length: 0\r\n\r\n">>),
    ?assertMatch({ok, #{keep_alive := false}}, R).

duplicate_headers_are_comma_joined(_Config) ->
    R = request(<<"POST /rpc HTTP/1.1\r\nHost: h\r\n"
                 "A2A-Extensions: https://a/1\r\n"
                 "A2A-Extensions: https://b/2\r\n"
                 "Content-Length: 0\r\n\r\n">>),
    {ok, Req} = R,
    ?assertEqual(<<"https://a/1,https://b/2">>,
                 agent_rpc_http:header(<<"a2a-extensions">>,
                                       maps:get(headers, Req))).

path_and_method_are_normalized(_Config) ->
    R = request(<<"GET /health HTTP/1.1\r\nHost: h\r\n\r\n">>),
    ?assertMatch({ok, #{method := get, path := <<"/health">>, body := <<>>}}, R).

response_has_content_length_and_close(_Config) ->
    Bytes = iolist_to_binary(
              agent_rpc_http:response(200, [{<<"content-type">>, <<"application/json">>}],
                                      <<"{}">>)),
    ?assertMatch({match, _}, re:run(Bytes, <<"HTTP/1.1 200 OK\r\n">>)),
    ?assertMatch({match, _}, re:run(Bytes, <<"content-length: 2\r\n">>)),
    ?assertMatch({match, _}, re:run(Bytes, <<"connection: close\r\n">>)),
    ?assertMatch({match, _}, re:run(Bytes, <<"\r\n\r\n\\{\\}">>)).

response_keep_alive_header(_Config) ->
    Bytes = iolist_to_binary(
              agent_rpc_http:response(200, [], <<>>, #{keep_alive => true})),
    ?assertMatch({match, _}, re:run(Bytes, <<"connection: keep-alive\r\n">>)).

keep_alive_rules(_Config) ->
    ?assertEqual(true, agent_rpc_http:keep_alive(#{})),
    ?assertEqual(true, agent_rpc_http:keep_alive(#{<<"connection">> => <<"keep-alive">>})),
    ?assertEqual(false, agent_rpc_http:keep_alive(#{<<"connection">> => <<"close">>})),
    ?assertEqual(false, agent_rpc_http:keep_alive({1, 0}, #{})),
    ?assertEqual(true, agent_rpc_http:keep_alive({1, 0}, #{<<"connection">> => <<"Keep-Alive">>})).

accepts_json(_Config) ->
    ?assert(agent_rpc_http:accepts_json(#{<<"content-type">> => <<"application/json">>})),
    ?assert(agent_rpc_http:accepts_json(
              #{<<"content-type">> => <<"application/json; charset=utf-8">>})),
    ?assert(agent_rpc_http:accepts_json(
              #{<<"content-type">> => <<"application/a2a+json">>})),
    ?assertNot(agent_rpc_http:accepts_json(#{<<"content-type">> => <<"text/plain">>})),
    ?assertNot(agent_rpc_http:accepts_json(#{})).

chunked_response_omits_content_length(_Config) ->
    Bytes = iolist_to_binary(
              agent_rpc_http:response(200, agent_rpc_sse:headers(), <<>>,
                                      #{chunked => true, keep_alive => true})),
    ?assertEqual(nomatch, re:run(Bytes, <<"content-length">>)),
    ?assertMatch({match, _}, re:run(Bytes, <<"transfer-encoding: chunked">>)).

%%%_* Helpers ---------------------------------------------------------------

listen() ->
    {ok, LSock} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true},
                                     {ip, {127, 0, 0, 1}}]),
    {ok, Port} = inet:port(LSock),
    {LSock, Port}.

connect(Port) ->
    gen_tcp:connect({127, 0, 0, 1}, Port, [binary, {active, false},
                                           {packet, raw}], 2000).

request(Bytes) ->
    {LSock, Port} = listen(),
    Parent = self(),
    spawn(fun() ->
                  {ok, S} = gen_tcp:accept(LSock),
                  Result = agent_rpc_http:read_request(ranch_tcp, S, ?LIMITS),
                  Parent ! {request_result, Result},
                  gen_tcp:close(S)
          end),
    {ok, C} = connect(Port),
    ok = gen_tcp:send(C, Bytes),
    Result = receive
                 {request_result, R} -> R
             after 5000 ->
                     ct:fail(no_request_result)
             end,
    gen_tcp:close(C),
    gen_tcp:close(LSock),
    Result.
