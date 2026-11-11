%% @doc SSE framing bytes (A2A §9.4.2).
-module(agent_rpc_sse_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [data_frame,
     named_frame,
     comment_frame,
     heartbeat_is_a_comment,
     headers_are_event_stream,
     chunk_wrap,
     last_chunk].

data_frame(_Config) ->
    ?assertEqual(<<"data: {\"a\":1}\n\n">>,
                 iolist_to_binary(agent_rpc_sse:event(<<"{\"a\":1}">>))).

named_frame(_Config) ->
    ?assertEqual(<<"event: message\ndata: {}\n\n">>,
                 iolist_to_binary(agent_rpc_sse:event(<<"message">>, <<"{}">>))).

comment_frame(_Config) ->
    ?assertEqual(<<": ping\n\n">>,
                 iolist_to_binary(agent_rpc_sse:comment(<<"ping">>))).

heartbeat_is_a_comment(_Config) ->
    ?assertEqual(<<": ping\n\n">>,
                 iolist_to_binary(agent_rpc_sse:heartbeat())).

headers_are_event_stream(_Config) ->
    Headers = agent_rpc_sse:headers(),
    ?assertEqual(<<"text/event-stream">>,
                 proplists:get_value(<<"content-type">>, Headers)),
    ?assertEqual(<<"no-cache">>,
                 proplists:get_value(<<"cache-control">>, Headers)).

chunk_wrap(_Config) ->
    ?assertEqual(<<"5\r\nhello\r\n">>,
                 iolist_to_binary(agent_rpc_http:chunk(<<"hello">>))).

last_chunk(_Config) ->
    ?assertEqual(<<"0\r\n\r\n">>, agent_rpc_http:last_chunk()).
