%% @doc A2A Task state machine (ADR-025 §D3).
%%
%% The state machine has no I/O, so these cases pin the transition table
%% exhaustively rather than sampling it: an edit that widens or narrows the ADR
%% has to change an assertion here.
-module(a2a_task_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    [states_are_the_proto_enum,
     terminal_and_interrupted_states_are_classified,
     legal_transition_table_matches_adr,
     illegal_transition_is_rejected_everywhere,
     terminal_states_have_no_outgoing_edges,
     working_may_report_progress_to_itself,
     interrupted_states_resume_to_working,
     wire_names_are_the_proto_enum_names,
     from_wire_accepts_short_names,
     from_wire_rejects_unknown_states,
     event_kinds_roundtrip].

states_are_the_proto_enum(_Config) ->
    ?assertEqual([unspecified, submitted, working, input_required,
                  auth_required, completed, failed, canceled, rejected],
                 a2a_task:states()),
    ?assertEqual(submitted, a2a_task:initial()),
    %% Every state has a wire name.
    [?assert(is_binary(a2a_task:to_wire(S))) || S <- a2a_task:states()],
    ok.

terminal_and_interrupted_states_are_classified(_Config) ->
    ?assertEqual([completed, failed, canceled, rejected],
                 a2a_task:terminal_states()),
    ?assertEqual([input_required, auth_required],
                 a2a_task:interrupted_states()),
    [?assertNot(a2a_task:is_terminal(S))
     || S <- [unspecified, submitted, working, input_required, auth_required]],
    [?assert(a2a_task:is_terminal(S))
     || S <- [completed, failed, canceled, rejected]],
    [?assert(a2a_task:is_interrupted(S)) || S <- [input_required, auth_required]],
    [?assertNot(a2a_task:is_interrupted(S)) || S <- [submitted, working]],
    ok.

%% ADR-025 §D3 verbatim, as a table. Both directions are checked below, so
%% this is the single source the illegal-transition case derives from.
legal_transition_table_matches_adr(_Config) ->
    Expected =
        #{unspecified => [submitted],
          submitted => [working, completed, failed, canceled, rejected,
                        input_required, auth_required],
          working => [working, completed, failed, canceled, rejected,
                      input_required, auth_required],
          input_required => [working, canceled, failed, rejected],
          auth_required => [working, canceled, failed, rejected],
          completed => [],
          failed => [],
          canceled => [],
          rejected => []},
    lists:foreach(
      fun(From) ->
              Want = maps:get(From, Expected),
              Got = lists:sort(a2a_task:transitions(From)),
              ?assertEqual(lists:sort(Want), Got),
              lists:foreach(
                fun(To) ->
                        ?assertEqual(lists:member(To, Want),
                                     a2a_task:legal_transition(From, To))
                end, a2a_task:states())
      end, a2a_task:states()),
    ok.

%% Exhaustive: for all 81 pairs, `transition/2' succeeds exactly where
%% `legal_transition/2' is true and otherwise returns the typed error.
illegal_transition_is_rejected_everywhere(_Config) ->
    lists:foreach(
      fun(From) ->
              lists:foreach(
                fun(To) ->
                        case a2a_task:legal_transition(From, To) of
                            true -> ?assertEqual(ok, a2a_task:transition(From, To));
                            false -> ?assertEqual({error, illegal_transition},
                                                  a2a_task:transition(From, To))
                        end
                end, a2a_task:states())
      end, a2a_task:states()),
    ok.

terminal_states_have_no_outgoing_edges(_Config) ->
    lists:foreach(
      fun(From) ->
              ?assertEqual([], a2a_task:transitions(From)),
              lists:foreach(
                fun(To) ->
                        ?assertEqual({error, illegal_transition},
                                     a2a_task:transition(From, To))
                end, a2a_task:states())
      end, a2a_task:terminal_states()),
    ok.

working_may_report_progress_to_itself(_Config) ->
    %% A long-running task emits several WORKING status updates; that is a
    %% legal self-loop, unlike SUBMITTED -> SUBMITTED.
    ?assert(a2a_task:legal_transition(working, working)),
    ?assertNot(a2a_task:legal_transition(submitted, submitted)),
    ok.

interrupted_states_resume_to_working(_Config) ->
    [?assert(a2a_task:legal_transition(S, working))
     || S <- [input_required, auth_required]],
    %% But an interrupted task cannot jump straight back to another interrupt.
    ?assertNot(a2a_task:legal_transition(input_required, auth_required)),
    ?assertNot(a2a_task:legal_transition(auth_required, input_required)),
    %% Nor can it be "re-submitted".
    ?assertNot(a2a_task:legal_transition(input_required, submitted)),
    ok.

wire_names_are_the_proto_enum_names(_Config) ->
    ?assertEqual(<<"TASK_STATE_UNSPECIFIED">>, a2a_task:to_wire(unspecified)),
    ?assertEqual(<<"TASK_STATE_SUBMITTED">>, a2a_task:to_wire(submitted)),
    ?assertEqual(<<"TASK_STATE_WORKING">>, a2a_task:to_wire(working)),
    ?assertEqual(<<"TASK_STATE_INPUT_REQUIRED">>, a2a_task:to_wire(input_required)),
    ?assertEqual(<<"TASK_STATE_AUTH_REQUIRED">>, a2a_task:to_wire(auth_required)),
    ?assertEqual(<<"TASK_STATE_COMPLETED">>, a2a_task:to_wire(completed)),
    ?assertEqual(<<"TASK_STATE_FAILED">>, a2a_task:to_wire(failed)),
    ?assertEqual(<<"TASK_STATE_CANCELED">>, a2a_task:to_wire(canceled)),
    ?assertEqual(<<"TASK_STATE_REJECTED">>, a2a_task:to_wire(rejected)),
    lists:foreach(
      fun(S) -> ?assertEqual({ok, S}, a2a_task:from_wire(a2a_task:to_wire(S))) end,
      a2a_task:states()),
    ok.

from_wire_accepts_short_names(_Config) ->
    %% A 0.3-era or hand-written client may send the short form.
    ?assertEqual({ok, working}, a2a_task:from_wire(<<"working">>)),
    ?assertEqual({ok, input_required}, a2a_task:from_wire(<<"input-required">>)),
    ?assertEqual({ok, input_required}, a2a_task:from_wire(<<"input_required">>)),
    ?assertEqual({ok, canceled}, a2a_task:from_wire(<<"cancelled">>)),
    ok.

from_wire_rejects_unknown_states(_Config) ->
    %% Not mapped to `unspecified': losing the client's intent would be worse
    %% than refusing the request.
    ?assertEqual({error, unknown_state}, a2a_task:from_wire(<<"bogus">>)),
    ?assertEqual({error, unknown_state}, a2a_task:from_wire(<<"TASK_STATE_BOGUS">>)),
    ok.

event_kinds_roundtrip(_Config) ->
    ?assertEqual([task, status_update, artifact_update, message],
                 a2a_task:kinds()),
    ?assertEqual(<<"statusUpdate">>, a2a_task:kind_to_wire(status_update)),
    lists:foreach(
      fun(K) ->
              ?assertEqual({ok, K}, a2a_task:kind_from_wire(a2a_task:kind_to_wire(K)))
      end, a2a_task:kinds()),
    ?assertEqual({error, unknown_kind}, a2a_task:kind_from_wire(<<"nope">>)),
    ok.
