%% @doc Fold A2A task events into a `Task' projection (ADR-025 §D2/§D3).
%%
%% Pure functions, no I/O and no clock reads except the history-lost timestamp:
%% the same code folds a live event and a replayed one, which is what makes
%% "the projection is the log" checkable instead of aspirational. The projection
%% stays in the owning shard's ETS; Mnesia is never involved (ADR-002).
%%
%% A task is the ordered fold of its `$a2a/task/<id>' events. The first event
%% must be the creation snapshot (`kind = task'); if replay starts with a delta
%% the creation was garbage-collected and the task is reported as
%% `history_lost' rather than half-rebuilt (ADR-025 §D6).
-module(a2a_task_projection).

-include_lib("broker_core/include/broker_message.hrl").

-export([new/4, apply_event/4, fold_events/3, mark_history_lost/3,
         to_wire/1, is_terminal/1,
         id/1, tenant_id/1, context_id/1, state/1, seq/1, created_offset/1,
         history_lost/1]).

%% @doc Projection of a freshly created task from its creation snapshot.
-spec new(binary(), binary(), offset(), map()) -> map().
new(TaskId, TenantId, Offset, TaskJson) ->
    Status = maps:get(<<"status">>, TaskJson, #{}),
    #{id => TaskId,
      tenant_id => TenantId,
      context_id => maps:get(<<"contextId">>, TaskJson, undefined),
      state => parse_state(maps:get(<<"state">>, Status, undefined)),
      status_message => maps:get(<<"message">>, Status, undefined),
      status_timestamp => maps:get(<<"timestamp">>, Status, undefined),
      artifacts => maps:get(<<"artifacts">>, TaskJson, []),
      history => maps:get(<<"history">>, TaskJson, []),
      created_offset => Offset,
      last_offset => Offset,
      seq => 1,
      history_lost => false}.

%% @doc Apply one decoded event to a projection.
%%
%% `{error, {illegal_transition, From, To}}' is returned to the live shard (it
%% turns it into `-32002'/`-32004'). Replay deliberately does not propagate it:
%% ADR-025 §D3.3 says corrupt history is logged, not rewritten, so replay keeps
%% the last valid projection (see `fold_events/3').
-spec apply_event(map(), a2a_task:kind(), map(), offset()) ->
    {ok, map()} | {error, term()}.
apply_event(_Proj, task, _Body, _Offset) ->
    %% A second creation snapshot for the same id is corrupt history, not a
    %% legitimate re-open: terminal tasks are immutable and non-terminal ones
    %% continue with deltas.
    {error, duplicate_creation};
apply_event(Proj, status_update, Body, Offset) ->
    Update = maps:get(<<"statusUpdate">>, Body, #{}),
    Status = maps:get(<<"status">>, Update, #{}),
    From = maps:get(state, Proj),
    To = parse_state(maps:get(<<"state">>, Status, undefined)),
    case a2a_task:transition(From, To) of
        ok ->
            {ok, Proj#{state := To,
                       status_message := maps:get(<<"message">>, Status, undefined),
                       status_timestamp := maps:get(<<"timestamp">>, Status,
                                                    undefined),
                       last_offset := Offset,
                       seq := maps:get(seq, Proj) + 1}};
        {error, illegal_transition} ->
            {error, {illegal_transition, From, To}}
    end;
apply_event(Proj, artifact_update, Body, Offset) ->
    Update = maps:get(<<"artifactUpdate">>, Body, #{}),
    Artifact = maps:get(<<"artifact">>, Update, #{}),
    Artifacts = upsert_artifact(Artifact, maps:get(artifacts, Proj)),
    {ok, Proj#{artifacts := Artifacts,
               last_offset := Offset,
               seq := maps:get(seq, Proj) + 1}};
apply_event(Proj, message, Body, Offset) ->
    Message = maps:get(<<"message">>, Body, #{}),
    {ok, Proj#{history := maps:get(history, Proj) ++ [Message],
               last_offset := Offset,
               seq := maps:get(seq, Proj) + 1}}.

%% @doc Rebuild one task from its ordered events (offset ascending).
%%
%% Returns `history_lost' when the first visible event is not a creation
%% snapshot; the caller (recovery) then appends a server-generated FAILED event
%% so the client learns the truth instead of polling a half task forever.
-spec fold_events(binary(), binary(), [{offset(), map()}]) ->
    {ok, map()} | {history_lost, offset()} | {error, term()}.
fold_events(_TaskId, _TenantId, []) ->
    {error, no_events};
fold_events(TaskId, TenantId, [{FirstOffset, #{kind := task, body := Body}} | Rest]) ->
    TaskJson = maps:get(<<"task">>, Body, #{}),
    Proj = new(TaskId, TenantId, FirstOffset, TaskJson),
    fold_rest(Proj, Rest);
fold_events(TaskId, TenantId, [{_Offset, #{kind := status_update}} | _] = Events) ->
    %% Either an orphan delta, or an orphan delta already carrying our
    %% history-lost repair. The repair event is itself detectable, which keeps
    %% repeated boots from appending one more repair each time.
    case lists:any(fun({_, #{body := Body}}) -> is_history_lost_repair(Body) end,
                   Events) of
        true -> {ok, mark_history_lost(TaskId, TenantId, last_offset(Events))};
        false -> {history_lost, last_offset(Events)}
    end;
fold_events(_TaskId, _TenantId, Events) ->
    {history_lost, last_offset(Events)}.

%% @doc Projection for a task whose creation snapshot is unavailable. It is
%% terminal (`FAILED') by construction: an interrupted task the broker cannot
%% describe must not look resumable.
-spec mark_history_lost(binary(), binary(), offset()) -> map().
mark_history_lost(TaskId, TenantId, LastOffset) ->
    #{id => TaskId,
      tenant_id => TenantId,
      context_id => undefined,
      state => failed,
      status_message => undefined,
      status_timestamp => a2a_event:timestamp(),
      artifacts => [],
      history => [],
      created_offset => undefined,
      last_offset => LastOffset,
      seq => 1,
      history_lost => true}.

%% @doc The A2A `Task' JSON. Empty `history'/'artifacts' are omitted, which is
%% what the proto's `optional' fields expect; `status' is always present.
-spec to_wire(map()) -> map().
to_wire(Proj) ->
    Status0 = #{<<"state">> => a2a_task:to_wire(maps:get(state, Proj))},
    Status1 = maybe_put(<<"message">>, maps:get(status_message, Proj), Status0),
    Timestamp = case maps:get(status_timestamp, Proj) of
                    undefined -> a2a_event:timestamp();
                    Value -> Value
                end,
    Status = Status1#{<<"timestamp">> => Timestamp},
    Base0 = #{<<"id">> => maps:get(id, Proj), <<"status">> => Status},
    Base1 = maybe_put(<<"contextId">>, maps:get(context_id, Proj), Base0),
    Base2 = maybe_put_nonempty(<<"history">>, maps:get(history, Proj), Base1),
    Base3 = maybe_put_nonempty(<<"artifacts">>, maps:get(artifacts, Proj), Base2),
    case maps:get(history_lost, Proj) of
        true -> Base3#{<<"metadata">> => #{<<"reason">> => <<"history_lost">>}};
        false -> Base3
    end.

-spec is_terminal(map()) -> boolean().
is_terminal(Proj) ->
    a2a_task:is_terminal(maps:get(state, Proj)).

id(Proj) -> maps:get(id, Proj).
tenant_id(Proj) -> maps:get(tenant_id, Proj).
context_id(Proj) -> maps:get(context_id, Proj).
state(Proj) -> maps:get(state, Proj).
seq(Proj) -> maps:get(seq, Proj).
created_offset(Proj) -> maps:get(created_offset, Proj).
history_lost(Proj) -> maps:get(history_lost, Proj).

%%%_* Internal --------------------------------------------------------------

fold_rest(Proj, []) ->
    {ok, Proj};
fold_rest(Proj, [{Offset, #{kind := Kind, body := Body}} | Rest]) ->
    case apply_event(Proj, Kind, Body, Offset) of
        {ok, Next} ->
            fold_rest(Next, Rest);
        {error, {illegal_transition, From, To}} ->
            %% ADR-025 §D3.3: log, do not silently rewrite. The last valid
            %% projection is kept; a metric is a S9-F addition.
            logger:warning("a2a_state_violation task=~s from=~p to=~p offset=~p",
                           [maps:get(id, Proj), From, To, Offset]),
            {ok, Proj};
        {error, Reason} ->
            {error, Reason}
    end.

last_offset(Events) ->
    {Offset, _} = lists:last(Events),
    Offset.

is_history_lost_repair(Body) ->
    Update = maps:get(<<"statusUpdate">>, Body, #{}),
    Metadata = maps:get(<<"metadata">>, Update, #{}),
    maps:get(<<"reason">>, Metadata, undefined) =:= <<"history_lost">>.

parse_state(Bin) when is_binary(Bin) ->
    case a2a_task:from_wire(Bin) of
        {ok, State} -> State;
        {error, unknown_state} -> unspecified
    end;
parse_state(_Other) ->
    unspecified.

upsert_artifact(Artifact, Artifacts) ->
    Id = maps:get(<<"artifactId">>, Artifact, undefined),
    case Id of
        undefined ->
            Artifacts ++ [Artifact];
        _ ->
            Rest = [A || A <- Artifacts, maps:get(<<"artifactId">>, A, undefined) =/= Id],
            Rest ++ [Artifact]
    end.

maybe_put(_Key, undefined, Map) -> Map;
maybe_put(Key, Value, Map) -> Map#{Key => Value}.

maybe_put_nonempty(_Key, [], Map) -> Map;
maybe_put_nonempty(_Key, undefined, Map) -> Map;
maybe_put_nonempty(Key, Value, Map) -> Map#{Key => Value}.
