%% @doc Server-Sent Events framing.
%%
%% Pure byte construction: the module never touches a socket and never decides
%% when a stream ends. The listener wraps these frames in `Transfer-Encoding:
%% chunked' with `agent_rpc_http:chunk/1' (A2A §9.4.2; ADR-026 §D2).
%%
%% The single hard rule here is that a frame's `data:' payload is one complete
%% JSON-RPC response, so the JSON encoder -- not this module -- is responsible
%% for escaping newlines. `event/2' adds the optional SSE event name for clients
%% that discriminate on it.
-module(agent_rpc_sse).

-export([event/1, event/2, comment/1, heartbeat/0, headers/0]).

-spec event(iodata()) -> iolist().
event(Json) ->
    [<<"data: ">>, Json, <<"\n\n">>].

-spec event(binary(), iodata()) -> iolist().
event(Name, Json) when is_binary(Name) ->
    [<<"event: ">>, Name, <<"\n">>, <<"data: ">>, Json, <<"\n\n">>].

-spec comment(binary()) -> iolist().
comment(Text) when is_binary(Text) ->
    [<<": ">>, Text, <<"\n\n">>].

%% A comment line every `sse_heartbeat_ms' keeps intermediaries from closing an
%% idle stream. It is deliberately not a JSON-RPC event: clients ignore `:'.
-spec heartbeat() -> iolist().
heartbeat() ->
    comment(<<"ping">>).

-spec headers() -> [{binary(), binary()}].
headers() ->
    [{<<"content-type">>, <<"text/event-stream">>},
     {<<"cache-control">>, <<"no-cache">>},
     {<<"x-accel-buffering">>, <<"no">>}].
