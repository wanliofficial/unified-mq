%% @doc Pure projection folding (ADR-025 §D2/§D3/§D6).
-module(a2a_task_projection_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    [folds_creation_to_completed,
     tracks_history_and_artifacts,
     illegal_transition_is_rejected_live,
     replay_logs_violation_and_keeps_last_valid_state,
     first_event_must_be_a_creation_snapshot,
     history_lost_repair_is_idempotent,
     to_wire_shape_roundtrips,
     mark_history_lost_is_terminal_failed].

folds_creation_to_completed(_Config) ->
    Events = [{10, event(task, 1, #{<<"task">> => task_json(submitted)})},
              {11, status_event(2, working, false)},
              {12, event(message, 3,
                         #{<<"message">> =>
                               #{<<"messageId">> => <<"m2">>,
                                 <<"role">> => <<"ROLE_AGENT">>,
                                 <<"parts">> => [#{<<"text">> => <<"hi">>}]}})},
              {13, status_event(4, completed, true)}],
    {ok, Proj} = a2a_task_projection:fold_events(<<"t1">>, <<"tenant">>, Events),
    ?assertEqual(completed, a2a_task_projection:state(Proj)),
    ?assertEqual(4, a2a_task_projection:seq(Proj)),
    ?assertEqual(13, maps:get(last_offset, Proj)),
    ?assertEqual(2, length(maps:get(history, Proj))),
    ?assert(a2a_task_projection:is_terminal(Proj)),
    Wire = a2a_task_projection:to_wire(Proj),
    ?assertEqual(<<"t1">>, maps:get(<<"id">>, Wire)),
    ?assertEqual(<<"c1">>, maps:get(<<"contextId">>, Wire)),
    ?assertEqual(<<"TASK_STATE_COMPLETED">>,
                 maps:get(<<"state">>, maps:get(<<"status">>, Wire))),
    ?assertEqual(2, length(maps:get(<<"history">>, Wire))).

tracks_history_and_artifacts(_Config) ->
    Events = [{1, event(task, 1, #{<<"task">> => task_json(submitted)})},
              {2, event(artifact_update, 2,
                        #{<<"artifactUpdate">> =>
                              #{<<"artifact">> =>
                                    #{<<"artifactId">> => <<"a1">>,
                                      <<"parts">> => [#{<<"text">> => <<"v1">>}]}}})},
              %% Same artifact id: replaced, not appended twice.
              {3, event(artifact_update, 3,
                        #{<<"artifactUpdate">> =>
                              #{<<"artifact">> =>
                                    #{<<"artifactId">> => <<"a1">>,
                                      <<"parts">> => [#{<<"text">> => <<"v2">>}]}}})},
              {4, event(artifact_update, 4,
                        #{<<"artifactUpdate">> =>
                              #{<<"artifact">> =>
                                    #{<<"artifactId">> => <<"a2">>,
                                      <<"parts">> => []}}})}],
    {ok, Proj} = a2a_task_projection:fold_events(<<"t1">>, <<"tenant">>, Events),
    Artifacts = maps:get(artifacts, Proj),
    ?assertEqual(2, length(Artifacts)),
    A1 = [A || A <- Artifacts, maps:get(<<"artifactId">>, A) =:= <<"a1">>],
    ?assertMatch([#{<<"parts">> := [#{<<"text">> := <<"v2">>}]}], A1).

illegal_transition_is_rejected_live(_Config) ->
    {ok, Proj} = a2a_task_projection:fold_events(
                   <<"t1">>, <<"tenant">>,
                   [{1, event(task, 1, #{<<"task">> => task_json(completed)})}]),
    ?assertEqual({error, {illegal_transition, completed, working}},
                 a2a_task_projection:apply_event(
                   Proj, status_update,
                   maps:get(body, status_event(2, working, false)), 2)).

replay_logs_violation_and_keeps_last_valid_state(_Config) ->
    %% A terminal state followed by more events is corrupt history: replay must
    %% not rewrite it, but must not adopt the illegal successor either.
    Events = [{1, event(task, 1, #{<<"task">> => task_json(completed)})},
              {2, status_event(2, working, false)}],
    {ok, Proj} = a2a_task_projection:fold_events(<<"t1">>, <<"tenant">>, Events),
    ?assertEqual(completed, a2a_task_projection:state(Proj)).

first_event_must_be_a_creation_snapshot(_Config) ->
    %% Creation GC'd; a delta is all replay sees.
    Events = [{5, status_event(2, working, false)}],
    ?assertEqual({history_lost, 5},
                 a2a_task_projection:fold_events(<<"t1">>, <<"tenant">>, Events)),
    Other = [{5, event(message, 1,
                       #{<<"message">> => #{<<"messageId">> => <<"m">>}}) }],
    ?assertEqual({history_lost, 5},
                 a2a_task_projection:fold_events(<<"t1">>, <<"tenant">>, Other)),
    ?assertEqual({error, no_events},
                 a2a_task_projection:fold_events(<<"t1">>, <<"tenant">>, [])).

history_lost_repair_is_idempotent(_Config) ->
    %% Once the repair event is in the log, replay adopts it as the terminal
    %% state instead of appending another repair on every boot.
    Repair = repair_event(9, <<"t1">>),
    Events = [{5, status_event(2, working, false)}, {9, Repair}],
    {ok, Proj} = a2a_task_projection:fold_events(<<"t1">>, <<"tenant">>, Events),
    ?assertEqual(failed, a2a_task_projection:state(Proj)),
    ?assert(a2a_task_projection:history_lost(Proj)),
    ?assertEqual(9, maps:get(last_offset, Proj)).

to_wire_shape_roundtrips(_Config) ->
    {ok, Proj} = a2a_task_projection:fold_events(
                   <<"t1">>, <<"tenant">>,
                   [{1, event(task, 1, #{<<"task">> => task_json(submitted)})}]),
    Wire = a2a_task_projection:to_wire(Proj),
    ?assertEqual(<<"TASK_STATE_SUBMITTED">>,
                 maps:get(<<"state">>, maps:get(<<"status">>, Wire))),
    ?assertEqual(<<"c1">>, maps:get(<<"contextId">>, Wire)),
    %% No artifacts yet: the key is omitted rather than sent as `[]'.
    ?assertNot(maps:is_key(<<"artifacts">>, Wire)).

mark_history_lost_is_terminal_failed(_Config) ->
    Proj = a2a_task_projection:mark_history_lost(<<"t1">>, <<"tenant">>, 42),
    ?assertEqual(failed, a2a_task_projection:state(Proj)),
    ?assert(a2a_task_projection:is_terminal(Proj)),
    ?assert(a2a_task_projection:history_lost(Proj)),
    Wire = a2a_task_projection:to_wire(Proj),
    ?assertEqual(#{<<"reason">> => <<"history_lost">>},
                 maps:get(<<"metadata">>, Wire)).

%%%_* Helpers ---------------------------------------------------------------

event(Kind, Seq, Payload) ->
    #{kind => Kind, seq => Seq, body => Payload}.

status_event(Seq, State, Final) ->
    event(status_update, Seq,
          #{<<"statusUpdate">> =>
                #{<<"taskId">> => <<"t1">>,
                  <<"contextId">> => <<"c1">>,
                  <<"status">> =>
                      #{<<"state">> => a2a_task:to_wire(State),
                        <<"timestamp">> => <<"2026-11-12T00:00:00.000Z">>},
                  <<"final">> => Final}}).

repair_event(Seq, TaskId) ->
    event(status_update, Seq,
          #{<<"statusUpdate">> =>
                #{<<"taskId">> => TaskId,
                  <<"status">> =>
                      #{<<"state">> => a2a_task:to_wire(failed),
                        <<"timestamp">> => <<"2026-11-12T00:00:00.000Z">>},
                  <<"final">> => true,
                  <<"metadata">> => #{<<"reason">> => <<"history_lost">>}}}).

task_json(State) ->
    #{<<"id">> => <<"t1">>,
      <<"contextId">> => <<"c1">>,
      <<"status">> =>
          #{<<"state">> => a2a_task:to_wire(State),
            <<"timestamp">> => <<"2026-11-12T00:00:00.000Z">>},
      <<"history">> =>
          [#{<<"messageId">> => <<"m1">>,
             <<"role">> => <<"ROLE_USER">>,
             <<"parts">> => [#{<<"text">> => <<"hi">>}]}]}.
