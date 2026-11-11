%% @doc A2A JSON-RPC front end over a real listener.
%%
%% Every case drives the same path a third-party client will: a raw HTTP/1.1
%% request over `gen_tcp'. The listener binds `port => 0' -- the fixed 8081 is
%% a deployment choice, never a test fixture (and is not ours to occupy).
-module(a2a_listener_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(TOKEN, <<"alice-token">>).

all() ->
    [echo_roundtrip,
     version_missing_is_read_as_0_3,
     version_unsupported,
     no_credential_401,
     bad_credential_401,
     acl_denied_403,
     unknown_method,
     parse_error,
     invalid_params,
     payload_too_large_413,
     unsupported_media_type_415,
     keep_alive_two_requests,
     connection_close_honored,
     health,
     anonymous_listener_on_loopback_is_allowed,
     anonymous_listener_on_wide_bind_is_refused].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(broker_security),
    {ok, _} = application:ensure_all_started(broker_proto_agent_rpc),
    _ = application:load(broker_proto_a2a),
    Old = [{K, application:get_env(broker_proto_a2a, K)}
           || K <- [listeners, require_auth, max_body_bytes]],
    OldSec = [{K, application:get_env(broker_security, K)}
              || K <- [users, acl]],
    ok = application:set_env(broker_security, users,
                             [{<<"alice">>, #{token => ?TOKEN,
                                              tenant_id => <<"t1">>}}]),
    ok = application:set_env(broker_security, acl, allow),
    ok = application:set_env(broker_proto_a2a, listeners, []),
    ok = application:set_env(broker_proto_a2a, require_auth, true),
    ok = application:set_env(broker_proto_a2a, max_body_bytes, 1048576),
    {ok, _} = application:ensure_all_started(broker_proto_a2a),
    [{old_a2a, Old}, {old_security, OldSec} | Config].

end_per_suite(Config) ->
    _ = application:stop(broker_proto_a2a),
    lists:foreach(fun restore_a2a/1, ?config(old_a2a, Config)),
    lists:foreach(fun restore_security/1, ?config(old_security, Config)),
    ok.

end_per_testcase(_Case, _Config) ->
    _ = application:stop(broker_proto_a2a),
    ok = application:set_env(broker_proto_a2a, listeners, []),
    ok = application:set_env(broker_proto_a2a, require_auth, true),
    ok = application:set_env(broker_proto_a2a, max_body_bytes, 1048576),
    ok = application:set_env(broker_security, acl, allow),
    ok.

echo_roundtrip(_Config) ->
    {Port, _} = start_listener([]),
    Body = rpc_body(<<"SendMessage">>, valid_params(), 1),
    {200, Headers, RespBody} = request(Port, post_headers(?TOKEN), Body),
    ?assertEqual(<<"application/json">>, header(<<"content-type">>, Headers)),
    Resp = decode(RespBody),
    ?assertEqual(1, maps:get(<<"id">>, Resp)),
    Message = maps:get(<<"message">>, maps:get(<<"result">>, Resp)),
    ?assertEqual(<<"ROLE_AGENT">>, maps:get(<<"role">>, Message)),
    ?assertEqual([#{<<"text">> => <<"hi">>}], maps:get(<<"parts">>, Message)),
    ?assertNotEqual(<<"m1">>, maps:get(<<"messageId">>, Message)),
    ?assertEqual(<<"ctx-1">>, maps:get(<<"contextId">>, Message)).

version_missing_is_read_as_0_3(_Config) ->
    %% The spec reads a missing header as 0.3; this server offers 1.0 only, so
    %% the honest answer is `-32009', not a 1.0-shaped response to a client that
    %% thinks it is speaking 0.3.
    {Port, _} = start_listener([]),
    Headers = [{"Content-Type", "application/json"},
               {"Authorization", "Bearer " ++ binary_to_list(?TOKEN)}],
    {400, _, Body} = request(Port, Headers,
                             rpc_body(<<"SendMessage">>, valid_params(), 1)),
    ?assertMatch(#{<<"error">> := #{<<"code">> := -32009}}, decode(Body)).

version_unsupported(_Config) ->
    {Port, _} = start_listener([]),
    Headers = [{"Content-Type", "application/json"},
               {"Authorization", "Bearer " ++ binary_to_list(?TOKEN)},
               {"A2A-Version", "2.0"}],
    {400, _, Body} = request(Port, Headers,
                             rpc_body(<<"SendMessage">>, valid_params(), 1)),
    Resp = decode(Body),
    ?assertMatch(#{<<"error">> := #{<<"code">> := -32009}}, Resp),
    ?assertEqual(null, maps:get(<<"id">>, Resp)).

no_credential_401(_Config) ->
    {Port, _} = start_listener([]),
    {401, Headers, _} = request(Port, [{"Content-Type", "application/json"}],
                                rpc_body(<<"SendMessage">>, valid_params(), 1)),
    ?assertEqual(<<"Bearer realm=\"a2a\"">>, header(<<"www-authenticate">>, Headers)).

bad_credential_401(_Config) ->
    {Port, _} = start_listener([]),
    {401, Headers, Body} = request(Port, post_headers(<<"wrong">>),
                                   rpc_body(<<"SendMessage">>, valid_params(), 1)),
    ?assertEqual(<<"Bearer realm=\"a2a\"">>, header(<<"www-authenticate">>, Headers)),
    %% The body says nothing about whether the user exists or the secret is
    %% wrong.
    ?assertEqual(<<>>, Body).

acl_denied_403(_Config) ->
    ok = application:set_env(broker_security, acl,
                             [#{effect => deny, principal => <<"*">>,
                                action => <<"*">>, pattern => <<"$a2a/#">>}]),
    {Port, _} = start_listener([]),
    {403, _, <<>>} = request(Port, post_headers(?TOKEN),
                             rpc_body(<<"SendMessage">>, valid_params(), 1)).

unknown_method(_Config) ->
    {Port, _} = start_listener([]),
    {200, _, Body} = request(Port, post_headers(?TOKEN),
                             rpc_body(<<"GetTask">>, #{}, 2)),
    ?assertMatch(#{<<"error">> := #{<<"code">> := -32601}}, decode(Body)).

parse_error(_Config) ->
    {Port, _} = start_listener([]),
    {400, _, Body} = request(Port, post_headers(?TOKEN), <<"not json">>),
    Resp = decode(Body),
    ?assertMatch(#{<<"error">> := #{<<"code">> := -32700}, <<"id">> := null}, Resp).

invalid_params(_Config) ->
    {Port, _} = start_listener([]),
    Params = #{<<"message">> => #{<<"messageId">> => <<"m1">>,
                                  <<"role">> => <<"ROLE_USER">>}},
    {200, _, Body} = request(Port, post_headers(?TOKEN),
                             rpc_body(<<"SendMessage">>, Params, 3)),
    ?assertMatch(#{<<"error">> := #{<<"code">> := -32602}}, decode(Body)).

payload_too_large_413(_Config) ->
    {Port, _} = start_listener([{max_body_bytes, 32}]),
    Big = binary:copy(<<"x">>, 64),
    {413, _, _} = request(Port, post_headers(?TOKEN), Big).

unsupported_media_type_415(_Config) ->
    {Port, _} = start_listener([]),
    {415, _, _} = request(Port,
                          [{"Content-Type", "text/plain"},
                           {"Authorization", "Bearer " ++ binary_to_list(?TOKEN)}],
                          rpc_body(<<"SendMessage">>, valid_params(), 1)).

keep_alive_two_requests(_Config) ->
    {Port, _} = start_listener([]),
    {ok, S} = gen_tcp:connect({127, 0, 0, 1}, Port,
                              [binary, {active, false}, {packet, raw}], 2000),
    Body = rpc_body(<<"SendMessage">>, valid_params(), 1),
    ok = gen_tcp:send(S, build_request(post_headers(?TOKEN), Body)),
    {200, _, _} = read_response(S),
    ok = gen_tcp:send(S, build_request(post_headers(?TOKEN), Body)),
    {200, _, _} = read_response(S),
    gen_tcp:close(S).

connection_close_honored(_Config) ->
    {Port, _} = start_listener([]),
    {ok, S} = gen_tcp:connect({127, 0, 0, 1}, Port,
                              [binary, {active, false}, {packet, raw}], 2000),
    Headers = [{"Connection", "close"} | post_headers(?TOKEN)],
    ok = gen_tcp:send(S, build_request(Headers, rpc_body(<<"SendMessage">>, valid_params(), 1))),
    {200, RespHeaders, _} = read_response(S),
    ?assertEqual(<<"close">>, header(<<"connection">>, RespHeaders)),
    ?assertEqual({error, closed}, gen_tcp:recv(S, 1, 2000)),
    gen_tcp:close(S).

health(_Config) ->
    {Port, _} = start_listener([]),
    {200, _, Body} = request(Port, [{"Host", "h"}], <<>>, get, "/health"),
    ?assertEqual(#{<<"status">> => <<"ok">>}, decode(Body)).

anonymous_listener_on_loopback_is_allowed(_Config) ->
    {_Port, ok} = start_listener([{require_auth, false}]),
    ok.

anonymous_listener_on_wide_bind_is_refused(_Config) ->
    Env = [{require_auth, false},
           {listeners, [#{port => 0, ref => {a2a, ct}, ip => {0, 0, 0, 0}}]}],
    ?assertMatch({error, {{insecure_listener, _, {0, 0, 0, 0}}, _}},
                 start_listener_raw(Env)).

%%%_* Helpers ---------------------------------------------------------------

start_listener(Extra) ->
    Env = [{listeners, [#{port => 0, ref => {a2a, ct}, num_acceptors => 2}]} | Extra],
    case start_listener_raw(Env) of
        ok -> {ranch:get_port({a2a, ct}), ok};
        {error, Reason} -> ct:fail({listener_start_failed, Reason})
    end.

start_listener_raw(Env) ->
    _ = application:stop(broker_proto_a2a),
    [ok = application:set_env(broker_proto_a2a, K, V) || {K, V} <- Env],
    application:start(broker_proto_a2a).

rpc_body(Method, Params, Id) ->
    json:encode(#{<<"jsonrpc">> => <<"2.0">>,
                  <<"id">> => Id,
                  <<"method">> => Method,
                  <<"params">> => Params}).

valid_params() ->
    #{<<"message">> => #{<<"messageId">> => <<"m1">>,
                         <<"role">> => <<"ROLE_USER">>,
                         <<"contextId">> => <<"ctx-1">>,
                         <<"parts">> => [#{<<"text">> => <<"hi">>}]}}.

post_headers(Token) ->
    [{"Content-Type", "application/json"},
     {"Authorization", "Bearer " ++ binary_to_list(Token)},
     {"A2A-Version", "1.0"}].

request(Port, Headers, Body) ->
    request(Port, Headers, Body, post, "/").

request(Port, Headers, Body, Method, Path) ->
    {ok, S} = gen_tcp:connect({127, 0, 0, 1}, Port,
                              [binary, {active, false}, {packet, raw}], 2000),
    ok = gen_tcp:send(S, build_request(Method, Path, Headers, Body)),
    Result = read_response(S),
    gen_tcp:close(S),
    Result.

build_request(Headers, Body) ->
    build_request(post, "/", Headers, Body).

build_request(Method, Path, Headers, Body) ->
    BodyBin = iolist_to_binary(Body),
    AllHeaders = [{"Host", "localhost"},
                  {"Content-Length", integer_to_list(byte_size(BodyBin))} | Headers],
    [string:uppercase(atom_to_list(Method)), " ", Path, " HTTP/1.1\r\n",
     [[K, ": ", V, "\r\n"] || {K, V} <- AllHeaders],
     "\r\n", BodyBin].

read_response(Sock) ->
    read_response(Sock, <<>>).

read_response(Sock, Buf) ->
    case binary:split(Buf, <<"\r\n\r\n">>) of
        [Head, Rest] ->
            {Status, Headers} = parse_head(Head),
            Length = case header(<<"content-length">>, Headers) of
                         undefined -> 0;
                         Value -> binary_to_integer(Value)
                     end,
            Body = read_body(Sock, Rest, Length),
            {Status, Headers, Body};
        [_] ->
            case gen_tcp:recv(Sock, 0, 5000) of
                {ok, Data} -> read_response(Sock, <<Buf/binary, Data/binary>>);
                {error, Reason} -> {error, Reason}
            end
    end.

read_body(_Sock, Buf, Length) when byte_size(Buf) >= Length ->
    binary:part(Buf, 0, Length);
read_body(Sock, Buf, Length) ->
    case gen_tcp:recv(Sock, Length - byte_size(Buf), 5000) of
        {ok, Data} -> read_body(Sock, <<Buf/binary, Data/binary>>, Length);
        {error, _Reason} -> Buf
    end.

parse_head(Head) ->
    [StatusLine | HeaderLines] = binary:split(Head, <<"\r\n">>, [global]),
    [_Version, Code | _] = binary:split(StatusLine, <<" ">>, [global]),
    Status = binary_to_integer(Code),
    Headers = [split_header(Line) || Line <- HeaderLines, Line =/= <<>>],
    {Status, Headers}.

split_header(Line) ->
    [Name, Value] = binary:split(Line, <<":">>),
    {lower(trim(Name)), trim(Value)}.

header(Name, Headers) ->
    proplists:get_value(Name, Headers).

lower(Bin) ->
    << <<(lower_byte(B))/integer>> || <<B/integer>> <= Bin >>.

lower_byte(B) when B >= $A, B =< $Z -> B + 32;
lower_byte(B) -> B.

trim(Bin) ->
    string:trim(Bin, both, " \t").

decode(Bin) ->
    json:decode(Bin).

restore_a2a({K, {ok, V}}) -> application:set_env(broker_proto_a2a, K, V);
restore_a2a({K, undefined}) -> application:unset_env(broker_proto_a2a, K).

restore_security({K, {ok, V}}) -> application:set_env(broker_security, K, V);
restore_security({K, undefined}) -> application:unset_env(broker_security, K).
