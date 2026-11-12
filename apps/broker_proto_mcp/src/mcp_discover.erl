%% @doc `server/discover' result assembly (ADR-027 §D3).
%%
%% `server/discover' is the one method the broker answers itself: it asks "what
%% does this Server support", and the broker *is* the server here. The result is
%% built from the configured `features' so the advertised capabilities always
%% agree with what `mcp_route' will actually route (ADR-027 §D6).
%%
%% Shape per `server/discover#discoverresult': `resultType', `supportedVersions',
%% `capabilities', `_meta.io.modelcontextprotocol/serverInfo', and the two
%% cache hints inherited from `CacheableResult' (`ttlMs', `cacheScope').
-module(mcp_discover).

-export([result/0, result/1, capabilities/1, server_info/0,
         protocol_version/0, supported_versions/0]).

-define(MCP_VERSION, <<"2026-07-28">>).

-spec protocol_version() -> binary().
protocol_version() ->
    ?MCP_VERSION.

%% @doc Versions this front end speaks. One entry today: the whole implementation
%% is written against the 2026-07-28 per-request model, and advertising a version
%% we do not implement would be a lie the client only discovers later.
-spec supported_versions() -> [binary()].
supported_versions() ->
    application:get_env(broker_proto_mcp, supported_versions, [?MCP_VERSION]).

-spec result() -> map().
result() ->
    result(application:get_env(broker_proto_mcp, features, [])).

-spec result([mcp_route:feature()]) -> map().
result(Features) ->
    Base = #{<<"resultType">> => <<"complete">>,
             <<"supportedVersions">> => supported_versions(),
             <<"capabilities">> => capabilities(Features),
             <<"_meta">> =>
                 #{<<"io.modelcontextprotocol/serverInfo">> => server_info()},
             <<"ttlMs">> => ttl_ms(),
             %% The answer is configuration, not per-caller data, so any cache
             %% (client or intermediary) may share it.
             <<"cacheScope">> => <<"public">>},
    case application:get_env(broker_proto_mcp, instructions, undefined) of
        undefined -> Base;
        Instructions when is_binary(Instructions) ->
            Base#{<<"instructions">> => Instructions}
    end.

%% Capabilities derived from `features': each advertised feature contributes its
%% standard capability object. An empty object is the minimum declaration; this
%% front end is a gateway and does not promise list-change notifications of its
%% own (`ServerCapabilities.tools.listChanged' stays absent).
-spec capabilities([mcp_route:feature()]) -> map().
capabilities(Features) ->
    lists:foldl(
      fun(tools, Acc) -> Acc#{<<"tools">> => #{}};
         (resources, Acc) -> Acc#{<<"resources">> => #{}};
         (prompts, Acc) -> Acc#{<<"prompts">> => #{}};
         (completions, Acc) -> Acc#{<<"completions">> => #{}};
         (_Other, Acc) -> Acc
      end, #{}, Features).

%% @doc Self-reported identity (`_meta.io.modelcontextprotocol/serverInfo'). The
%% spec is explicit that this is not verified and MUST NOT drive behaviour.
%% `version' is a JSON string, so the app's vsn (an Erlang string) is converted.
-spec server_info() -> map().
server_info() ->
    {ok, Vsn} = application:get_key(broker_proto_mcp, vsn),
    #{<<"name">> => <<"unified-mq">>, <<"version">> => to_binary(Vsn)}.

to_binary(Bin) when is_binary(Bin) -> Bin;
to_binary(List) when is_list(List) -> unicode:characters_to_binary(List).

ttl_ms() ->
    application:get_env(broker_proto_mcp, discover_ttl_ms, 3600000).
