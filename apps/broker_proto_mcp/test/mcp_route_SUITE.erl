%% @doc Unit tests for `mcp_route', the pure MCP method classifier
%% (ADR-027 §D5/§D6).
%%
%% `mcp_route' is a pure module: no socket, no bus, no application
%% dependencies. The whole suite is direct calls against `classify/2',
%% covering the five outcomes:
%%   * `{local, discover}' / `{local, subscriptions}' -- handled by this
%%     front end regardless of the advertised feature set;
%%   * `{passthrough, Feature}' -- a known method whose feature is advertised;
%%   * `{feature_not_advertised, Feature}' -- a known method whose feature is
%%     not advertised (capability-gated -32601 on 200);
%%   * `{refused, sampling | elicitation}' -- server->client families a broker
%%     never implements (ADR-027 §D10, R-B2);
%%   * `{unknown, Method}' -- genuinely unknown (-32601 on 404).
-module(mcp_route_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0]).
-export([discover_is_local_regardless_of_features/1,
         subscriptions_is_local_regardless_of_features/1,
         tools_methods_pass_through_when_advertised/1,
         resources_methods_pass_through_when_advertised/1,
         prompts_methods_pass_through_when_advertised/1,
         completions_method_passes_through_when_advertised/1,
         known_method_without_feature_is_not_advertised/1,
         feature_gating_is_independent_per_feature/1,
         sampling_family_is_refused/1,
         elicitation_family_is_refused/1,
         refused_family_takes_precedence_over_unknown/1,
         unknown_method_is_unknown/1,
         local_methods_listing/1,
         method_features_listing/1]).

all() ->
    [discover_is_local_regardless_of_features,
     subscriptions_is_local_regardless_of_features,
     tools_methods_pass_through_when_advertised,
     resources_methods_pass_through_when_advertised,
     prompts_methods_pass_through_when_advertised,
     completions_method_passes_through_when_advertised,
     known_method_without_feature_is_not_advertised,
     feature_gating_is_independent_per_feature,
     sampling_family_is_refused,
     elicitation_family_is_refused,
     refused_family_takes_precedence_over_unknown,
     unknown_method_is_unknown,
     local_methods_listing,
     method_features_listing].

%% `server/discover' is local whether or not any feature is advertised.
discover_is_local_regardless_of_features(_Config) ->
    ?assertEqual({local, discover}, mcp_route:classify(<<"server/discover">>, [])),
    ?assertEqual({local, discover},
                 mcp_route:classify(<<"server/discover">>,
                                    [tools, resources, prompts, completions])).

%% `subscriptions/listen' is local whether or not any feature is advertised.
subscriptions_is_local_regardless_of_features(_Config) ->
    ?assertEqual({local, subscriptions},
                 mcp_route:classify(<<"subscriptions/listen">>, [])),
    ?assertEqual({local, subscriptions},
                 mcp_route:classify(<<"subscriptions/listen">>, [tools])).

%% `tools/*' pass through when `tools' is advertised.
tools_methods_pass_through_when_advertised(_Config) ->
    ?assertEqual({passthrough, tools},
                 mcp_route:classify(<<"tools/list">>, [tools])),
    ?assertEqual({passthrough, tools},
                 mcp_route:classify(<<"tools/call">>, [tools, resources])).

%% `resources/*' pass through when `resources' is advertised.
resources_methods_pass_through_when_advertised(_Config) ->
    ?assertEqual({passthrough, resources},
                 mcp_route:classify(<<"resources/list">>, [resources])),
    ?assertEqual({passthrough, resources},
                 mcp_route:classify(<<"resources/templates/list">>, [resources])),
    ?assertEqual({passthrough, resources},
                 mcp_route:classify(<<"resources/read">>, [resources])).

%% `prompts/*' pass through when `prompts' is advertised.
prompts_methods_pass_through_when_advertised(_Config) ->
    ?assertEqual({passthrough, prompts},
                 mcp_route:classify(<<"prompts/list">>, [prompts])),
    ?assertEqual({passthrough, prompts},
                 mcp_route:classify(<<"prompts/get">>, [prompts])).

%% `completion/complete' passes through when `completions' is advertised.
completions_method_passes_through_when_advertised(_Config) ->
    ?assertEqual({passthrough, completions},
                 mcp_route:classify(<<"completion/complete">>, [completions])).

%% A known method whose feature is not advertised is capability-gated, not
%% unknown: the front end answers -32601 with a 200.
known_method_without_feature_is_not_advertised(_Config) ->
    ?assertEqual({feature_not_advertised, tools},
                 mcp_route:classify(<<"tools/list">>, [])),
    ?assertEqual({feature_not_advertised, resources},
                 mcp_route:classify(<<"resources/read">>, [tools])).

%% Gating is per feature: advertising one family does not open another.
feature_gating_is_independent_per_feature(_Config) ->
    Features = [tools],
    ?assertEqual({passthrough, tools},
                 mcp_route:classify(<<"tools/call">>, Features)),
    ?assertEqual({feature_not_advertised, resources},
                 mcp_route:classify(<<"resources/list">>, Features)),
    ?assertEqual({feature_not_advertised, prompts},
                 mcp_route:classify(<<"prompts/get">>, Features)),
    ?assertEqual({feature_not_advertised, completions},
                 mcp_route:classify(<<"completion/complete">>, Features)).

%% `sampling/*' is refused outright, even if every feature is advertised.
sampling_family_is_refused(_Config) ->
    ?assertEqual({refused, sampling},
                 mcp_route:classify(<<"sampling/createMessage">>, [])),
    ?assertEqual({refused, sampling},
                 mcp_route:classify(<<"sampling/createMessage">>,
                                    [tools, resources, prompts, completions])).

%% `elicitation/*' is refused outright.
elicitation_family_is_refused(_Config) ->
    ?assertEqual({refused, elicitation},
                 mcp_route:classify(<<"elicitation/create">>, [])),
    ?assertEqual({refused, elicitation},
                 mcp_route:classify(<<"elicitation/create">>, [tools])).

%% An unknown method in a refused family is still refused, not unknown: the
%% family check is by prefix and runs before the unknown fall-through.
refused_family_takes_precedence_over_unknown(_Config) ->
    ?assertEqual({refused, sampling},
                 mcp_route:classify(<<"sampling/anything">>, [])),
    ?assertEqual({refused, elicitation},
                 mcp_route:classify(<<"elicitation/anything">>, [])).

%% A genuinely unknown method (no feature, no refused family) is unknown.
unknown_method_is_unknown(_Config) ->
    ?assertEqual({unknown, <<"does/not-exist">>},
                 mcp_route:classify(<<"does/not-exist">>, [])),
    ?assertEqual({unknown, <<"does/not-exist">>},
                 mcp_route:classify(<<"does/not-exist">>,
                                    [tools, resources, prompts, completions])),
    %% A bare word with no slash is unknown too.
    ?assertEqual({unknown, <<"ping">>}, mcp_route:classify(<<"ping">>, [])).

%% `local_methods/0' lists exactly the two locally handled methods.
local_methods_listing(_Config) ->
    ?assertEqual(lists:sort([<<"server/discover">>, <<"subscriptions/listen">>]),
                 lists:sort(mcp_route:local_methods())).

%% `method_features/0' lists every method that maps to a feature, with the
%% right feature, and the list round-trips through `classify/2'.
method_features_listing(_Config) ->
    Pairs = mcp_route:method_features(),
    ?assertEqual(8, length(Pairs)),
    %% Every listed method classifies as passthrough when its feature is on.
    lists:foreach(
      fun({Method, Feature}) ->
              ?assertEqual({passthrough, Feature},
                           mcp_route:classify(Method, [Feature]))
      end, Pairs),
    %% The expected mapping (and no duplicates).
    Expected = lists:sort(
                 [{<<"tools/list">>, tools},
                  {<<"tools/call">>, tools},
                  {<<"resources/list">>, resources},
                  {<<"resources/templates/list">>, resources},
                  {<<"resources/read">>, resources},
                  {<<"prompts/list">>, prompts},
                  {<<"prompts/get">>, prompts},
                  {<<"completion/complete">>, completions}]),
    ?assertEqual(Expected, lists:sort(Pairs)).
