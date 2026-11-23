%% @doc S9: `subscriptions/listen' filter mapping (mcp_sub, pure).
%%
%% `mcp_sub' turns a client's `params.notifications' filter into the set of
%% kernel destinations the gateway subscribes to, and back into the
%% acknowledgment subset it echoes. No process, no socket: the two maps are the
%% whole contract, and both are easy to get subtly wrong (a false flag that
%% still subscribes, an empty or NUL-bearing URI that lands in a destination,
%% an acknowledgment that echoes a field the plan never honoured). These cases
%% pin the contract the dispatcher and stream rely on.
%%
%% Grouping:
%%   * `plan'   -- flags, resourceSubscriptions, invalid URIs, non-map input;
%%   * `ack'    -- the acknowledged subset (reflect only what was agreed);
%%   * `round'  -- plan -> acknowledged is self-consistent.
-module(mcp_sub_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-compile([export_all, nowarn_export_all]).

all() ->
    [{group, plan}, {group, ack}, {group, round}].

groups() ->
    [{plan, [sequence],
      [empty_filter_plans_nothing,
       false_flag_plans_nothing,
       tools_flag_plans_one_destination,
       all_flags_plan_in_documented_order,
       resource_subscriptions_plan_per_uri,
       empty_and_nul_uris_are_dropped,
       non_map_filter_plans_nothing]},
     {ack, [sequence],
      [empty_plan_acknowledges_nothing,
       acknowledged_echoes_only_agreed_flags,
       acknowledged_lists_only_accepted_uris]},
     {round, [sequence],
      [plan_and_acknowledged_agree]}].

%% plan --------------------------------------------------------------------

empty_filter_plans_nothing(_Config) ->
    ?assertEqual([], mcp_sub:plan(#{})).

false_flag_plans_nothing(_Config) ->
    ?assertEqual([], mcp_sub:plan(#{<<"toolsListChanged">> => false})).

tools_flag_plans_one_destination(_Config) ->
    ?assertEqual([#{dest => <<"$mcp/notify/tools/list_changed">>,
                    method => <<"notifications/tools/list_changed">>,
                    field => <<"toolsListChanged">>}],
                 mcp_sub:plan(#{<<"toolsListChanged">> => true})).

all_flags_plan_in_documented_order(_Config) ->
    Plan = mcp_sub:plan(#{<<"toolsListChanged">> => true,
                          <<"promptsListChanged">> => true,
                          <<"resourcesListChanged">> => true}),
    ?assertEqual([<<"$mcp/notify/tools/list_changed">>,
                  <<"$mcp/notify/prompts/list_changed">>,
                  <<"$mcp/notify/resources/list_changed">>],
                 [maps:get(dest, E) || E <- Plan]),
    ?assertEqual([<<"toolsListChanged">>, <<"promptsListChanged">>,
                  <<"resourcesListChanged">>],
                 [maps:get(field, E) || E <- Plan]).

resource_subscriptions_plan_per_uri(_Config) ->
    Plan = mcp_sub:plan(#{<<"resourceSubscriptions">> => [<<"file:///a">>, <<"file:///b">>]}),
    ?assertEqual([#{dest => <<"$mcp/notify/resources/updated/file:///a">>,
                    method => <<"notifications/resources/updated">>,
                    uri => <<"file:///a">>,
                    field => <<"resourceSubscriptions">>},
                  #{dest => <<"$mcp/notify/resources/updated/file:///b">>,
                    method => <<"notifications/resources/updated">>,
                    uri => <<"file:///b">>,
                    field => <<"resourceSubscriptions">>}],
                 Plan).

empty_and_nul_uris_are_dropped(_Config) ->
    %% An empty binary or one containing NUL must never become a destination.
    ?assertEqual([], mcp_sub:plan(#{<<"resourceSubscriptions">> => [<<>>]})),
    ?assertEqual([], mcp_sub:plan(#{<<"resourceSubscriptions">> => [<<"a\0b">>]})),
    %% A mix keeps only the valid ones.
    ?assertEqual([<<"file:///ok">>],
                 [maps:get(uri, E)
                  || E <- mcp_sub:plan(#{<<"resourceSubscriptions">> =>
                                             [<<>>, <<"file:///ok">>, <<"x\0y">>]})]).

non_map_filter_plans_nothing(_Config) ->
    ?assertEqual([], mcp_sub:plan(undefined)),
    ?assertEqual([], mcp_sub:plan(<<"not a map">>)),
    ?assertEqual([], mcp_sub:plan([])).

%% ack ---------------------------------------------------------------------

empty_plan_acknowledges_nothing(_Config) ->
    ?assertEqual(#{}, mcp_sub:acknowledged([])).

acknowledged_echoes_only_agreed_flags(_Config) ->
    %% A field absent from the plan must be absent from the acknowledgment,
    %% not echoed as false.
    Plan = mcp_sub:plan(#{<<"toolsListChanged">> => true}),
    ?assertEqual(#{<<"toolsListChanged">> => true}, mcp_sub:acknowledged(Plan)),
    ?assertNot(maps:is_key(<<"promptsListChanged">>,
                           mcp_sub:acknowledged(Plan))).

acknowledged_lists_only_accepted_uris(_Config) ->
    Plan = mcp_sub:plan(#{<<"resourceSubscriptions">> => [<<"file:///a">>]}),
    ?assertEqual(#{<<"resourceSubscriptions">> => [<<"file:///a">>]},
                 mcp_sub:acknowledged(Plan)).

%% round -------------------------------------------------------------------

plan_and_acknowledged_agree(_Config) ->
    Filter = #{<<"toolsListChanged">> => true,
               <<"resourcesListChanged">> => true,
               <<"resourceSubscriptions">> => [<<"file:///x">>, <<>>]},
    Plan = mcp_sub:plan(Filter),
    Ack = mcp_sub:acknowledged(Plan),
    %% Every acknowledged field has a plan entry backing it.
    [?assert(lists:any(fun(#{field := F}) -> F =:= Key end, Plan))
     || Key <- [<<"toolsListChanged">>, <<"resourcesListChanged">>]],
    %% The dropped empty URI is not echoed.
    ?assertEqual([<<"file:///x">>], maps:get(<<"resourceSubscriptions">>, Ack)),
    %% And a flag the filter did not ask for is not in the ack.
    ?assertNot(maps:is_key(<<"promptsListChanged">>, Ack)).
