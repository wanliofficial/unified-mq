%% @doc Boot replay: rebuild every task projection from the shared log
%% (ADR-025 §D5/§D6).
%%
%% This is the correctness baseline, not an optimisation: scan every partition
%% from offset 0, keep `$a2a/task/' messages, group by task and fold. It runs
%% once per boot, does not produce events (except the explicit history-lost
%% repair below), sends no push and touches no subscription.
%%
%% A task whose creation snapshot is gone is reported as `FAILED /
%% history_lost' with one server-generated terminal event, so the client stops
%% polling a half task. The repair event is itself detectable on the next
%% replay, which keeps repeated boots idempotent.
-module(a2a_task_recovery).

-behaviour(gen_server).

-include_lib("broker_core/include/broker_message.hrl").

-export([start_link/0, recover/0]).
-export([init/1, handle_call/3, handle_cast/2]).

-define(PAGE, 1000).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link(?MODULE, [], []).

%% @doc Runs in the child's init so the supervisor does not report the registry
%% ready until projections exist. A read failure stops the registry (and the
%% application) rather than booting an empty, silently lossy view.
-spec recover() -> ok | {error, term()}.
recover() ->
    try broker_queue:partition_ids() of
        Partitions ->
            case read_all(Partitions, []) of
                {ok, Messages} ->
                    restore_tasks(group(Messages)),
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end
    catch
        Class:Reason ->
            {error, {recovery_crashed, Class, Reason}}
    end.

init([]) ->
    case recover() of
        ok -> {ok, #{}};
        {error, Reason} -> {stop, {a2a_recovery_failed, Reason}}
    end.

handle_call(_Request, _From, State) ->
    {reply, {error, unsupported_operation}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%%%_* Reading ---------------------------------------------------------------

read_all([], Acc) ->
    {ok, Acc};
read_all([Partition | Rest], Acc) ->
    case read_partition(Partition, 0, []) of
        {ok, Messages} -> read_all(Rest, Acc ++ Messages);
        {error, Reason} -> {error, {partition_read_failed, Partition, Reason}}
    end.

read_partition(Partition, From, Acc) ->
    case broker_queue:read_from(Partition, From, ?PAGE) of
        {ok, [], _Next} ->
            {ok, Acc};
        {ok, Page, Next} ->
            Acc1 = Acc ++ [{partition_offset(Partition, Offset), Msg}
                           || {Offset, Msg} <- Page],
            case Next > From of
                true -> read_partition(Partition, Next, Acc1);
                %% No progress: stop rather than loop forever if a backend ever
                %% returns the same page twice.
                false -> {ok, Acc1}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% Offset alone is not globally unique across partitions; the pair is what makes
%% the per-task order deterministic when a task somehow appears in two logs
%% (it cannot through `route/3', but replay should not rely on that).
partition_offset(Partition, Offset) ->
    {Partition, Offset}.

%%%_* Grouping and folding --------------------------------------------------

group(Messages) ->
    Folded =
        lists:foldl(
          fun({{_Partition, Offset}, #broker_message{destination = Destination,
                                                     tenant_id = Tenant} = Msg},
              Acc) ->
                  case a2a_event:task_id(Destination) of
                      {ok, TaskId} ->
                          case a2a_event:decode(Msg#broker_message.payload) of
                              {ok, Event} ->
                                  Entry = {Offset, Event},
                                  maps:update_with(
                                    TaskId,
                                    fun({T, L}) -> {T, L ++ [Entry]} end,
                                    {Tenant, [Entry]}, Acc);
                              {error, Reason} ->
                                  logger:warning(
                                    "a2a_event_undecodable task=~s offset=~p reason=~p",
                                    [TaskId, Offset, Reason]),
                                  Acc
                          end;
                      error ->
                          Acc
                  end
          end, #{}, Messages),
    [{TaskId, Tenant, sort_events(Events)}
     || {TaskId, {Tenant, Events}} <- maps:to_list(Folded)].

sort_events(Events) ->
    lists:sort(fun({A, _}, {B, _}) -> A =< B end, Events).

restore_tasks(Tasks) ->
    lists:foreach(fun restore_task/1, Tasks).

restore_task({TaskId, Tenant, Events}) ->
    case a2a_task_projection:fold_events(TaskId, Tenant, Events) of
        {ok, Proj} ->
            ok = a2a_task_registry:restore(Proj);
        {history_lost, LastOffset} ->
            repair_history_lost(TaskId, Tenant, LastOffset, Events);
        {error, Reason} ->
            logger:warning("a2a_task_replay_failed task=~s reason=~p",
                           [TaskId, Reason])
    end.

%% ADR-025 §D6: append one terminal FAILED event, then project it. Not silent,
%% not a rewrite of history: the earlier deltas stay exactly where they are.
repair_history_lost(TaskId, Tenant, LastOffset, Events) ->
    logger:warning("a2a_task_history_lost task=~s offset=~p", [TaskId, LastOffset]),
    Seq = next_seq(Events),
    Payload =
        #{<<"statusUpdate">> =>
              #{<<"taskId">> => TaskId,
                <<"status">> => #{<<"state">> => a2a_task:to_wire(failed),
                                  <<"timestamp">> => a2a_event:timestamp()},
                <<"final">> => true,
                <<"metadata">> => #{<<"reason">> => <<"history_lost">>}}},
    RepairedOffset =
        case a2a_event:append(TaskId, Tenant, status_update, Seq, Payload) of
            {ok, Offset} -> Offset;
            {error, Reason} ->
                logger:error("a2a_history_lost_repair_failed task=~s reason=~p",
                             [TaskId, Reason]),
                LastOffset
        end,
    Proj = a2a_task_projection:mark_history_lost(TaskId, Tenant, RepairedOffset),
    ok = a2a_task_registry:restore(Proj).

next_seq(Events) ->
    lists:max([Seq || {_Offset, #{seq := Seq}} <- Events] ++ [0]) + 1.
