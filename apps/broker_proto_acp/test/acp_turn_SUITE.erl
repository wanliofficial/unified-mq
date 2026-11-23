%% @doc S9-E: the per-turn fold of `session/update' notifications (pure).
%%
%% ACP's `session/prompt' is a request whose *response* carries the terminal
%% stop reason, while the work streams as `session/update' notifications in
%% between. `acp_turn' is the client's memory of one such turn: the ordered
%% updates (replayable for a subscriber) plus a count per discriminator. These
%% cases pin the fold's contract directly, without a bus, a socket or a turn
%% in flight:
%%
%%   * `new'    -- what an empty turn reports;
%%   * `fold'   -- arrival order, per-kind counts, the two accepted shapes;
%%   * `unknown'-- unknown discriminators survive as `other', never dropped;
%%   * `finish' -- the stop reason and the closed turn;
%%   * `project'-- the wire projection a subscriber receives.
-module(acp_turn_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-compile([export_all, nowarn_export_all]).

all() ->
    [{group, new}, {group, fold}, {group, unknown}, {group, finish},
     {group, project}].

groups() ->
    [{new, [sequence],
      [empty_turn_projects_open,
       kinds_are_the_documented_set]},
     {fold, [sequence],
      [updates_keep_arrival_order,
       counts_are_per_kind,
       nested_update_shape_is_unwrapped,
       bare_update_shape_is_accepted]},
     {unknown, [sequence],
      [unknown_kind_is_kept_verbatim,
       nested_unknown_kind_is_kept,
       object_without_discriminator_is_unknown]},
     {finish, [sequence],
      [finish_records_stop_reason,
       done_turn_ignores_further_updates,
       finish_is_idempotent]},
     {project, [sequence],
      [project_reverses_to_arrival_order,
       project_reports_done_status,
       project_reports_undefined_stop_reason]}].

%% Fixtures ----------------------------------------------------------------

turn() -> acp_turn:new(<<"s1">>, [#{<<"type">> => <<"text">>,
                                    <<"text">> => <<"hi">>}]).

nested(Kind) ->
    #{<<"sessionId">> => <<"s1">>, <<"update">> => #{<<"sessionUpdate">> => Kind}}.

bare(Kind) ->
    #{<<"sessionUpdate">> => Kind}.

updates(Turn) ->
    maps:get(<<"updates">>, acp_turn:project(Turn)).

counts(Turn) ->
    maps:get(<<"counts">>, acp_turn:project(Turn)).

%% new ---------------------------------------------------------------------

empty_turn_projects_open(_Config) ->
    P = acp_turn:project(turn()),
    ?assertEqual(<<"s1">>, maps:get(<<"sessionId">>, P)),
    ?assertEqual([], maps:get(<<"updates">>, P)),
    ?assertEqual(#{}, maps:get(<<"counts">>, P)),
    ?assertEqual(<<"open">>, maps:get(<<"status">>, P)),
    ?assertEqual(undefined, maps:get(<<"stopReason">>, P)),
    %% The prompt is carried through verbatim so a replay shows what was asked.
    ?assertMatch([#{<<"text">> := <<"hi">>}], maps:get(<<"prompt">>, P)).

kinds_are_the_documented_set(_Config) ->
    Kinds = acp_turn:kinds(),
    ?assertEqual(9, length(Kinds)),
    [?assert(lists:member(K, Kinds))
     || K <- [<<"user_message_chunk">>, <<"agent_message_chunk">>,
              <<"agent_thought_chunk">>, <<"tool_call">>, <<"tool_call_update">>,
              <<"plan">>, <<"available_commands_update">>,
              <<"current_mode_update">>, <<"config_option_update">>]],
    %% The documentation claims unknown values are not dropped; that only holds
    %% if the known set does not accidentally include `unknown' or `other'.
    ?assertNot(lists:member(<<"unknown">>, Kinds)).

%% fold --------------------------------------------------------------------

updates_keep_arrival_order(_Config) ->
    T0 = turn(),
    T1 = acp_turn:update(T0, nested(<<"agent_message_chunk">>)),
    T2 = acp_turn:update(T1, nested(<<"agent_thought_chunk">>)),
    T3 = acp_turn:update(T2, nested(<<"agent_message_chunk">>)),
    Kinds = [maps:get(<<"sessionUpdate">>, O) || O <- updates(T3)],
    ?assertEqual([<<"agent_message_chunk">>, <<"agent_thought_chunk">>,
                  <<"agent_message_chunk">>], Kinds).

counts_are_per_kind(_Config) ->
    T0 = turn(),
    T1 = acp_turn:update(T0, nested(<<"agent_message_chunk">>)),
    T2 = acp_turn:update(T1, nested(<<"agent_message_chunk">>)),
    T3 = acp_turn:update(T2, nested(<<"tool_call">>)),
    ?assertEqual(#{<<"agent_message_chunk">> => 2, <<"tool_call">> => 1},
                 counts(T3)).

nested_update_shape_is_unwrapped(_Config) ->
    %% The wire shape `session/update' sends: { sessionId, update: { sessionUpdate: ... } }
    T = acp_turn:update(turn(), nested(<<"plan">>)),
    ?assertEqual([#{<<"sessionUpdate">> => <<"plan">>}], updates(T)),
    ?assertEqual(#{<<"plan">> => 1}, counts(T)).

bare_update_shape_is_accepted(_Config) ->
    %% A caller may hand in the bare update object; the fold must not then
    %% misclassify it as the (missing) wrapper.
    T = acp_turn:update(turn(), bare(<<"tool_call_update">>)),
    ?assertEqual([#{<<"sessionUpdate">> => <<"tool_call_update">>}], updates(T)),
    ?assertEqual(#{<<"tool_call_update">> => 1}, counts(T)).

%% unknown -----------------------------------------------------------------

unknown_kind_is_kept_verbatim(_Config) ->
    %% A newer agent's discriminator must be forwarded, never silently lost.
    Params = bare(<<"future_update">>),
    T = acp_turn:update(turn(), Params),
    ?assertEqual([Params], updates(T)),
    ?assertEqual(#{<<"future_update">> => 1}, counts(T)).

nested_unknown_kind_is_kept(_Config) ->
    Params = nested(<<"future_update">>),
    T = acp_turn:update(turn(), Params),
    ?assertEqual([#{<<"sessionUpdate">> => <<"future_update">>}], updates(T)),
    ?assertEqual(#{<<"future_update">> => 1}, counts(T)).

object_without_discriminator_is_unknown(_Config) ->
    %% Neither { sessionUpdate } nor { update: { sessionUpdate } }: the fold
    %% classifies it as `unknown' but still forwards the body.
    Params = #{<<"some">> => <<"payload">>},
    T = acp_turn:update(turn(), Params),
    ?assertEqual([Params], updates(T)),
    ?assertEqual(#{<<"unknown">> => 1}, counts(T)).

%% finish ------------------------------------------------------------------

finish_records_stop_reason(_Config) ->
    T = acp_turn:finish(turn(), <<"end_turn">>),
    P = acp_turn:project(T),
    ?assertEqual(<<"end_turn">>, maps:get(<<"stopReason">>, P)),
    ?assertEqual(<<"done">>, maps:get(<<"status">>, P)).

done_turn_ignores_further_updates(_Config) ->
    %% An update arriving after the response must not mutate the finished
    %% turn -- the subscriber has already seen its final projection.
    Done = acp_turn:finish(turn(), <<"end_turn">>),
    After = acp_turn:update(Done, nested(<<"agent_message_chunk">>)),
    ?assertEqual(updates(Done), updates(After)),
    ?assertEqual(counts(Done), counts(After)),
    ?assertEqual(acp_turn:project(Done), acp_turn:project(After)).

finish_is_idempotent(_Config) ->
    Once = acp_turn:finish(turn(), <<"max_tokens">>),
    Twice = acp_turn:finish(Once, <<"max_tokens">>),
    ?assertEqual(acp_turn:project(Once), acp_turn:project(Twice)).

%% project -----------------------------------------------------------------

project_reverses_to_arrival_order(_Config) ->
    T = lists:foldl(fun(K, Acc) -> acp_turn:update(Acc, nested(K)) end,
                    turn(),
                    [<<"plan">>, <<"tool_call">>, <<"tool_call_update">>]),
    ?assertEqual([<<"plan">>, <<"tool_call">>, <<"tool_call_update">>],
                 [maps:get(<<"sessionUpdate">>, O) || O <- updates(T)]).

project_reports_done_status(_Config) ->
    T = acp_turn:finish(acp_turn:update(turn(), bare(<<"agent_message_chunk">>)),
                        <<"refusal">>),
    P = acp_turn:project(T),
    ?assertEqual(<<"done">>, maps:get(<<"status">>, P)),
    ?assertEqual(<<"refusal">>, maps:get(<<"stopReason">>, P)),
    ?assertEqual(#{<<"agent_message_chunk">> => 1}, maps:get(<<"counts">>, P)).

project_reports_undefined_stop_reason(_Config) ->
    ?assertEqual(undefined, maps:get(<<"stopReason">>,
                                     acp_turn:project(turn()))).
