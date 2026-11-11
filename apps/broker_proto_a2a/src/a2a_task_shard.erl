%% @doc One task shard: the serialization point for a subset of task ids
%% (ADR-025 §D2/§D3).
%%
%% Every state-changing request for a task goes through exactly one gen_server,
%% so "check the current state, append the event, update the projection" is
%% atomic with respect to other requests for the same task without a lock.
%% Different tasks on different shards never wait for each other. The projection
%% lives in this process's ETS table; the log is the source of truth and the
%% table is a cache that boot replay rebuilds (see `a2a_task_recovery').
-module(a2a_task_shard).

-behaviour(gen_server).

-export([start_link/1, name/1]).
-export([create/5, get/2, tasks/3, cancel/3, apply_event/4, restore/2, count/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(PAGE_DEFAULT, 50).
-define(PAGE_MAX, 100).

-record(state, {index :: pos_integer(),
                table :: ets:tid()}).

-spec start_link(pos_integer()) -> {ok, pid()} | {error, term()}.
start_link(Index) ->
    gen_server:start_link({local, name(Index)}, ?MODULE, Index, []).

-spec name(pos_integer()) -> atom().
name(Index) ->
    list_to_atom("a2a_task_shard_" ++ integer_to_list(Index)).

%% @doc Create a task and drive it to completion. S9-B has no real agent
%% executor, so the built-in echo agent runs SYNCHRONOUSLY: SUBMITTED ->
%% WORKING -> (agent message) -> COMPLETED, all appended before we reply. That
%% is exactly the "return_immediately = false" promise of ADR-025 §D4.
-spec create(atom(), binary(), binary(), binary(), map()) ->
    {ok, map()} | {error, term()}.
create(Server, TaskId, TenantId, ContextId, Message) ->
    gen_server:call(Server, {create, TaskId, TenantId, ContextId, Message},
                    timeout()).

-spec get(atom(), binary()) -> {ok, map()} | not_found.
get(Server, TaskId) ->
    gen_server:call(Server, {get, TaskId}, timeout()).

-spec tasks(atom(), binary(), map()) -> [map()].
tasks(Server, TenantId, Filters) ->
    gen_server:call(Server, {tasks, TenantId, Filters}, timeout()).

-spec cancel(atom(), binary(), binary()) -> {ok, map()} | {error, term()}.
cancel(Server, TaskId, TenantId) ->
    gen_server:call(Server, {cancel, TaskId, TenantId}, timeout()).

%% @doc Append an event on top of an existing task (multi-turn input, artifacts).
%% S9-C uses it; S9-B exercises it through create/5 only.
-spec apply_event(atom(), binary(), binary(), {a2a_task:kind(), map()}) ->
    {ok, map()} | {error, term()}.
apply_event(Server, TaskId, TenantId, Event) ->
    gen_server:call(Server, {apply_event, TaskId, TenantId, Event}, timeout()).

-spec restore(atom(), map()) -> ok.
restore(Server, Proj) ->
    gen_server:call(Server, {restore, Proj}, timeout()).

-spec count(atom()) -> non_neg_integer().
count(Server) ->
    gen_server:call(Server, count, timeout()).

%%%_* gen_server ------------------------------------------------------------

init(Index) ->
    Table = ets:new(name(Index), [set, protected, {read_concurrency, true}]),
    {ok, #state{index = Index, table = Table}}.

handle_call({create, TaskId, TenantId, ContextId, Message}, _From, State) ->
    {Reply, Next} = create_task(TaskId, TenantId, ContextId, Message, State),
    {reply, Reply, Next};
handle_call({get, TaskId}, _From, State) ->
    {reply, lookup(TaskId, State), State};
handle_call({tasks, TenantId, Filters}, _From, State) ->
    {reply, matching(TenantId, Filters, State), State};
handle_call({cancel, TaskId, TenantId}, _From, State) ->
    {Reply, Next} = cancel_task(TaskId, TenantId, State),
    {reply, Reply, Next};
handle_call({apply_event, TaskId, TenantId, {Kind, Payload}}, _From, State) ->
    {Reply, Next} = apply_to_task(TaskId, TenantId, Kind, Payload, State),
    {reply, Reply, Next};
handle_call({restore, Proj}, _From, #state{table = Table} = State) ->
    true = ets:insert(Table, {a2a_task_projection:id(Proj), Proj}),
    {reply, ok, State};
handle_call(count, _From, #state{table = Table} = State) ->
    {reply, ets:info(Table, size), State};
handle_call(_Other, _From, State) ->
    {reply, {error, unsupported_operation}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%%_* Operations ------------------------------------------------------------

create_task(TaskId, TenantId, ContextId, Message, State) ->
    case lookup(TaskId, State) of
        {ok, _Existing} ->
            {{error, duplicate_task}, State};
        not_found ->
            Creation = creation_snapshot(TaskId, ContextId, Message, TenantId),
            case a2a_event:append(TaskId, TenantId, task, 1,
                                  #{<<"task">> => Creation}) of
                {ok, Offset} ->
                    Proj0 = a2a_task_projection:new(TaskId, TenantId, Offset,
                                                    Creation),
                    State1 = store(Proj0, State),
                    case auto_complete() of
                        true -> drive_echo(Proj0, Message, State1);
                        false -> {{ok, Proj0}, State1}
                    end;
                {error, Reason} ->
                    {{error, Reason}, State}
            end
    end.

%% The built-in echo agent: report WORKING, answer the user's parts, complete.
drive_echo(Proj0, Message, State) ->
    case emit(Proj0, status_update,
              #{<<"statusUpdate">> => status_event(Proj0, working, false)}) of
        {ok, Proj1} ->
            State1 = store(Proj1, State),
            case emit(Proj1, message,
                      #{<<"message">> => agent_message(Proj1, Message)}) of
                {ok, Proj2} ->
                    State2 = store(Proj2, State1),
                    case emit(Proj2, status_update,
                              #{<<"statusUpdate">> =>
                                    status_event(Proj2, completed, true)}) of
                        {ok, Proj3} ->
                            {{ok, Proj3}, store(Proj3, State2)};
                        {error, Reason} ->
                            {{error, Reason}, State2}
                    end;
                {error, Reason} ->
                    {{error, Reason}, State1}
            end;
        {error, Reason} ->
            {{error, Reason}, State}
    end.

cancel_task(TaskId, TenantId, State) ->
    case lookup(TaskId, State) of
        not_found ->
            {{error, task_not_found}, State};
        {ok, Proj} ->
            case a2a_task_projection:tenant_id(Proj) =:= TenantId of
                false ->
                    %% Same answer as "does not exist": task ids must not become
                    %% a cross-tenant existence oracle.
                    {{error, task_not_found}, State};
                true ->
                    case a2a_task_projection:is_terminal(Proj) of
                        true ->
                            {{error, task_not_cancelable}, State};
                        false ->
                            case emit(Proj, status_update,
                                      #{<<"statusUpdate">> =>
                                            status_event(Proj, canceled, true)}) of
                                {ok, Next} -> {{ok, Next}, store(Next, State)};
                                {error, Reason} -> {{error, Reason}, State}
                            end
                    end
            end
    end.

apply_to_task(TaskId, TenantId, Kind, Payload, State) ->
    case lookup(TaskId, State) of
        not_found ->
            {{error, task_not_found}, State};
        {ok, Proj} ->
            case a2a_task_projection:tenant_id(Proj) =:= TenantId of
                false ->
                    {{error, task_not_found}, State};
                true ->
                    case a2a_task_projection:is_terminal(Proj) of
                        true ->
                            {{error, unsupported_operation}, State};
                        false ->
                            case emit(Proj, Kind, Payload) of
                                {ok, Next} -> {{ok, Next}, store(Next, State)};
                                {error, Reason} -> {{error, Reason}, State}
                            end
                    end
            end
    end.

%% Append exactly one event and fold it into the projection. The append must
%% succeed before the in-memory state moves: a projection that ran ahead of the
%% log would vanish on restart.
emit(Proj, Kind, Payload) ->
    TaskId = a2a_task_projection:id(Proj),
    TenantId = a2a_task_projection:tenant_id(Proj),
    Seq = a2a_task_projection:seq(Proj) + 1,
    case a2a_event:append(TaskId, TenantId, Kind, Seq, Payload) of
        {ok, Offset} -> a2a_task_projection:apply_event(Proj, Kind, Payload, Offset);
        {error, Reason} -> {error, Reason}
    end.

%%%_* Queries ---------------------------------------------------------------

lookup(TaskId, #state{table = Table}) ->
    case ets:lookup(Table, TaskId) of
        [{_, Proj}] -> {ok, Proj};
        [] -> not_found
    end.

matching(TenantId, Filters, #state{table = Table}) ->
    All = [Proj || {_, Proj} <- ets:tab2list(Table),
                   a2a_task_projection:tenant_id(Proj) =:= TenantId],
    lists:filter(fun(Proj) -> matches(Proj, Filters) end, All).

matches(Proj, Filters) ->
    matches_context(Proj, maps:get(context_id, Filters, undefined)) andalso
        matches_status(Proj, maps:get(status, Filters, undefined)).

matches_context(_Proj, undefined) -> true;
matches_context(Proj, ContextId) ->
    a2a_task_projection:context_id(Proj) =:= ContextId.

matches_status(_Proj, undefined) -> true;
matches_status(_Proj, []) -> true;
matches_status(Proj, States) when is_list(States) ->
    lists:member(a2a_task_projection:state(Proj), States).

store(Proj, #state{table = Table} = State) ->
    true = ets:insert(Table, {a2a_task_projection:id(Proj), Proj}),
    State.

%%%_* Wire construction -----------------------------------------------------

creation_snapshot(TaskId, ContextId, Message, _TenantId) ->
    #{<<"id">> => TaskId,
      <<"contextId">> => ContextId,
      <<"status">> => #{<<"state">> => a2a_task:to_wire(submitted),
                        <<"timestamp">> => a2a_event:timestamp()},
      <<"history">> => [with_task(Message, TaskId, ContextId)]}.

status_event(Proj, State, Final) ->
    #{<<"taskId">> => a2a_task_projection:id(Proj),
      <<"contextId">> => a2a_task_projection:context_id(Proj),
      <<"status">> => #{<<"state">> => a2a_task:to_wire(State),
                        <<"timestamp">> => a2a_event:timestamp()},
      <<"final">> => Final}.

agent_message(Proj, Message) ->
    #{<<"messageId">> => new_id(),
      <<"role">> => <<"ROLE_AGENT">>,
      <<"parts">> => maps:get(<<"parts">>, Message, []),
      <<"contextId">> => a2a_task_projection:context_id(Proj),
      <<"taskId">> => a2a_task_projection:id(Proj)}.

with_task(Message, TaskId, ContextId) when is_map(Message) ->
    Message#{<<"taskId">> => TaskId, <<"contextId">> => ContextId};
with_task(_Other, _TaskId, _ContextId) ->
    #{}.

new_id() ->
    url_base64(crypto:strong_rand_bytes(16)).

url_base64(Bin) ->
    Encoded = base64:encode(Bin),
    iolist_to_binary([url_char(B) || <<B/integer>> <= Encoded, B =/= $=]).

url_char($+) -> $-;
url_char($/) -> $_;
url_char(B) -> B.

timeout() ->
    application:get_env(broker_proto_a2a, task_timeout_ms, 30000).

%% S9-B ships a synchronous echo agent; an async executor (S9-C+) turns this
%% off so a task stays non-terminal until the agent reports progress.
auto_complete() ->
    application:get_env(broker_proto_a2a, task_auto_complete, true).
