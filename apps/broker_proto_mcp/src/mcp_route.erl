%% @doc MCP method routing and feature gating (ADR-027 §D5/§D6).
%%
%% A pure module: no socket, no bus. It answers three questions for `mcp_rpc':
%%   * is this method one this front end knows, and which server feature does it
%%     belong to;
%%   * is that feature advertised in `features' (ADR-027 §D6) -- if not, the
%%     answer is `feature_not_advertised' and the caller answers -32601 with a
%%     200 (the spec's capability-gated case, distinct from a genuinely unknown
%%     method which is -32601 on 404);
%%   * is the method handled locally (`server/discover', `subscriptions/listen')
%%     or must it be passed through to the kernel.
%%
%% Two families are refused outright regardless of `features' (ADR-027 §D10,
%% R-B2): `sampling/*' (deprecated in 2026-07-28 and never implemented by a
%% broker) and `elicitation/*' (server -> client only). A client sending either
%% is answered -32601 rather than silently routed into the void.
-module(mcp_route).

-export([classify/2, method_features/0, local_methods/0]).

-type method() :: binary().
-type feature() :: tools | resources | prompts | completions.
-type class() ::
        {local, discover | subscriptions}
      | {passthrough, feature()}
      | {refused, sampling | elicitation}
      | {unknown, method()}
      | {feature_not_advertised, feature()}.

-export_type([class/0, feature/0]).

%% @doc Classify a method against the advertised feature set.
-spec classify(method(), [feature()]) -> class().
classify(<<"server/discover">>, _Features) ->
    {local, discover};
classify(<<"subscriptions/listen">>, _Features) ->
    {local, subscriptions};
classify(Method, Features) ->
    case method_feature(Method) of
        {ok, Feature} ->
            case lists:member(Feature, Features) of
                true -> {passthrough, Feature};
                false -> {feature_not_advertised, Feature}
            end;
        none ->
            case refused_family(Method) of
                {ok, Why} -> {refused, Why};
                none -> {unknown, Method}
            end
    end.

%% The server -> client families that a broker never implements.
refused_family(<<"sampling/", _/binary>>) -> {ok, sampling};
refused_family(<<"elicitation/", _/binary>>) -> {ok, elicitation};
refused_family(_) -> none.

%% Which server feature a method belongs to. `undefined' for methods outside
%% the four feature families (discover/subscriptions handled before this, and
%% sampling/elicitation refused separately).
-spec method_feature(method()) -> {ok, feature()} | none.
method_feature(Method) ->
    case method_feature_(Method) of
        {ok, _} = Ok -> Ok;
        _ -> none
    end.

method_feature_(<<"tools/list">>) -> {ok, tools};
method_feature_(<<"tools/call">>) -> {ok, tools};
method_feature_(<<"resources/list">>) -> {ok, resources};
method_feature_(<<"resources/templates/list">>) -> {ok, resources};
method_feature_(<<"resources/read">>) -> {ok, resources};
method_feature_(<<"prompts/list">>) -> {ok, prompts};
method_feature_(<<"prompts/get">>) -> {ok, prompts};
method_feature_(<<"completion/complete">>) -> {ok, completions};
method_feature_(_) -> none.

%% Every method this front end treats specially (local), for tests and docs.
-spec local_methods() -> [method()].
local_methods() -> [<<"server/discover">>, <<"subscriptions/listen">>].

-spec method_features() -> [{method(), feature()}].
method_features() ->
    [{M, F} || M <- [<<"tools/list">>, <<"tools/call">>,
                     <<"resources/list">>, <<"resources/templates/list">>,
                     <<"resources/read">>, <<"prompts/list">>, <<"prompts/get">>,
                     <<"completion/complete">>],
               {ok, F} <- [method_feature(M)]].
