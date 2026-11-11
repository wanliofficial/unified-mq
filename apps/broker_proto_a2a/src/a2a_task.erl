%% @doc A2A Task state machine (ADR-025 §D3) -- pure, no I/O.
%%
%% Both the task shard (which validates before appending an event) and the
%% projection (which re-validates while replaying the log) use this module, so
%% "is this transition legal?" has exactly one answer in the codebase. The log
%% is the source of truth; this module only decides whether a new event may be
%% appended on top of a given state.
%%
%% Internal atoms are snake_case (`input_required') because Erlang atoms with
%% hyphens need quoting; the wire vocabulary is the proto enum name
%% (`<<"TASK_STATE_INPUT_REQUIRED">>', convergence report §2.2 requires the
%% full SCREAMING_SNAKE_CASE name even in JSON).
-module(a2a_task).

-export([states/0, terminal_states/0, interrupted_states/0,
         is_terminal/1, is_interrupted/1,
         transitions/1, legal_transition/2, transition/2,
         to_wire/1, from_wire/1,
         kinds/0, kind_to_wire/1, kind_from_wire/1,
         initial/0]).

-type state() :: unspecified | submitted | working | input_required
               | auth_required | completed | failed | canceled | rejected.
-type kind() :: task | status_update | artifact_update | message.
-export_type([state/0, kind/0]).

%% @doc Every state the proto defines, in enum order.
-spec states() -> [state()].
states() ->
    [unspecified, submitted, working, input_required, auth_required,
     completed, failed, canceled, rejected].

%% @doc ADR-025 §D3: a terminal task cannot be restarted, cannot receive
%% another message and cannot be canceled again.
-spec terminal_states() -> [state()].
terminal_states() ->
    [completed, failed, canceled, rejected].

%% @doc Interrupted states wait for the client (`input-required') or for an
%% out-of-band auth step (`auth-required'); they can still move to WORKING.
-spec interrupted_states() -> [state()].
interrupted_states() ->
    [input_required, auth_required].

-spec is_terminal(state()) -> boolean().
is_terminal(State) ->
    lists:member(State, terminal_states()).

-spec is_interrupted(state()) -> boolean().
is_interrupted(State) ->
    lists:member(State, interrupted_states()).

%% @doc State a fresh task starts in once the creating event is appended.
-spec initial() -> state().
initial() ->
    submitted.

%% @doc All states reachable from `From' in one event, per ADR-025 §D3.
%%
%% The table is spelled out rather than derived (e.g. "anything but a terminal
%% state can fail") because the ADR is the contract and a future edit should be
%% forced to touch this list consciously.
-spec transitions(state()) -> [state()].
transitions(unspecified) ->
    [submitted];
transitions(submitted) ->
    [working, completed, failed, canceled, rejected,
     input_required, auth_required];
transitions(working) ->
    [working, completed, failed, canceled, rejected,
     input_required, auth_required];
transitions(input_required) ->
    [working, canceled, failed, rejected];
transitions(auth_required) ->
    [working, canceled, failed, rejected];
transitions(State) ->
    case is_terminal(State) of
        true -> [];
        false -> []
    end.

-spec legal_transition(state(), state()) -> boolean().
legal_transition(From, To) ->
    lists:member(To, transitions(From)).

%% @doc The form the shard calls: `{error, illegal_transition}' is a decision,
%% not a crash, so it can be translated into `-32002'/`-32004' at the method
%% layer (ADR-025 §D3.2).
-spec transition(state(), state()) -> ok | {error, illegal_transition}.
transition(From, To) ->
    case legal_transition(From, To) of
        true -> ok;
        false -> {error, illegal_transition}
    end.

%% @doc Wire name for a state (proto JSON enum value).
-spec to_wire(state()) -> binary().
to_wire(unspecified) -> <<"TASK_STATE_UNSPECIFIED">>;
to_wire(submitted) -> <<"TASK_STATE_SUBMITTED">>;
to_wire(working) -> <<"TASK_STATE_WORKING">>;
to_wire(input_required) -> <<"TASK_STATE_INPUT_REQUIRED">>;
to_wire(auth_required) -> <<"TASK_STATE_AUTH_REQUIRED">>;
to_wire(completed) -> <<"TASK_STATE_COMPLETED">>;
to_wire(failed) -> <<"TASK_STATE_FAILED">>;
to_wire(canceled) -> <<"TASK_STATE_CANCELED">>;
to_wire(rejected) -> <<"TASK_STATE_REJECTED">>.

%% @doc Parse a wire state. Accepts the proto full name as well as the short
%% name an older client may send (`"working"'); anything unknown is
%% `{error, unknown_state}' rather than `unspecified', because silently
%% degrading an unknown state to UNSPECIFIED would lose the client's intent.
-spec from_wire(binary()) -> {ok, state()} | {error, unknown_state}.
from_wire(<<"TASK_STATE_UNSPECIFIED">>) -> {ok, unspecified};
from_wire(<<"TASK_STATE_SUBMITTED">>) -> {ok, submitted};
from_wire(<<"TASK_STATE_WORKING">>) -> {ok, working};
from_wire(<<"TASK_STATE_INPUT_REQUIRED">>) -> {ok, input_required};
from_wire(<<"TASK_STATE_AUTH_REQUIRED">>) -> {ok, auth_required};
from_wire(<<"TASK_STATE_COMPLETED">>) -> {ok, completed};
from_wire(<<"TASK_STATE_FAILED">>) -> {ok, failed};
from_wire(<<"TASK_STATE_CANCELED">>) -> {ok, canceled};
from_wire(<<"TASK_STATE_REJECTED">>) -> {ok, rejected};
from_wire(<<"unspecified">>) -> {ok, unspecified};
from_wire(<<"submitted">>) -> {ok, submitted};
from_wire(<<"working">>) -> {ok, working};
from_wire(<<"input-required">>) -> {ok, input_required};
from_wire(<<"input_required">>) -> {ok, input_required};
from_wire(<<"auth-required">>) -> {ok, auth_required};
from_wire(<<"auth_required">>) -> {ok, auth_required};
from_wire(<<"completed">>) -> {ok, completed};
from_wire(<<"failed">>) -> {ok, failed};
from_wire(<<"canceled">>) -> {ok, canceled};
from_wire(<<"cancelled">>) -> {ok, canceled};
from_wire(<<"rejected">>) -> {ok, rejected};
from_wire(_Other) -> {error, unknown_state}.

%% @doc Event kinds: the four `StreamResponse' oneof branches (ADR-025 §D1).
%% The kind is advisory metadata (`_meta.unified-mq.kind'); the payload itself
%% is always the A2A wire JSON, never a parallel model.
-spec kinds() -> [kind()].
kinds() ->
    [task, status_update, artifact_update, message].

-spec kind_to_wire(kind()) -> binary().
kind_to_wire(task) -> <<"task">>;
kind_to_wire(status_update) -> <<"statusUpdate">>;
kind_to_wire(artifact_update) -> <<"artifactUpdate">>;
kind_to_wire(message) -> <<"message">>.

-spec kind_from_wire(binary()) -> {ok, kind()} | {error, unknown_kind}.
kind_from_wire(<<"task">>) -> {ok, task};
kind_from_wire(<<"statusUpdate">>) -> {ok, status_update};
kind_from_wire(<<"status_update">>) -> {ok, status_update};
kind_from_wire(<<"artifactUpdate">>) -> {ok, artifact_update};
kind_from_wire(<<"artifact_update">>) -> {ok, artifact_update};
kind_from_wire(<<"message">>) -> {ok, message};
kind_from_wire(_Other) -> {error, unknown_kind}.
