%% @doc A2A Task events as ordinary `#broker_message{}' (ADR-025 §D1).
%%
%% There is no second log and no parallel data model: every state change is one
%% immutable message whose destination is `$a2a/task/<TaskId>' and whose payload
%% is the A2A wire JSON of a `StreamResponse' branch. Partitioning goes through
%% `broker_router_hash:route/3' (the only partition oracle, ADR-010), appending
%% through `broker_queue:append/3' (the only write path, so `replicated_commit'
%% and the size/attribute guards apply unchanged).
%%
%% The project-private `_meta.unified-mq' block carries `seq' and `kind'. It is
%% additive: a stock A2A client ignores it, and recovery uses it to tell a
%% creation snapshot from a delta without guessing from JSON shape.
-module(a2a_event).

-include_lib("broker_core/include/broker_message.hrl").
-include_lib("broker_router/include/broker_router.hrl").

-export([destination/1, task_id/1, event/5, append/5,
         encode/3, decode/1, reliability/0, validate_reliability/0,
         timestamp/0]).

-define(PREFIX, <<"$a2a/task/">>).

-spec destination(binary()) -> binary().
destination(TaskId) when is_binary(TaskId) ->
    <<?PREFIX/binary, TaskId/binary>>.

%% @doc Inverse of `destination/1'; `error' for every other destination, which
%% is how replay filters the shared partition logs.
-spec task_id(binary()) -> {ok, binary()} | error.
task_id(<<"$a2a/task/", TaskId/binary>>) when TaskId =/= <<>> ->
    {ok, TaskId};
task_id(_Other) ->
    error.

%% @doc Build the durable message without writing it. Pure, so tests can assert
%% the exact frozen record without a storage node.
-spec event(binary(), binary(), a2a_task:kind(), pos_integer(), map()) ->
    #broker_message{}.
event(TaskId, TenantId, Kind, Seq, Payload) ->
    #broker_message{
       message_id = event_id(TaskId, Seq),
       tenant_id = TenantId,
       destination = destination(TaskId),
       partition_id = 0,                    % filled in by append/5 after routing
       payload = iolist_to_binary(agent_rpc_json:encode(encode(Kind, Seq, Payload))),
       payload_encoding = json,
       headers = #{},
       proto_meta = #{},
       persistence = durable,
       reliability = reliability(),
       created_at = erlang:system_time(millisecond),
       expires_at = undefined,
       event = undefined}.

%% @doc Append one event. Routes exactly like a publisher: the task id decides
%% the partition, so all events of one task land in one log and replay sees
%% them in order.
%%
%% ADR-010 makes `route/3' answer at least the destination's own partition, so a
%% non-matching return is a contract break, not a case to handle: the `try'
%% turns it into `{error, {route_crashed, _}}' instead of a silent `ok'.
-spec append(binary(), binary(), a2a_task:kind(), pos_integer(), map()) ->
    {ok, offset()} | {error, term()}.
append(TaskId, TenantId, Kind, Seq, Payload) ->
    Msg = event(TaskId, TenantId, Kind, Seq, Payload),
    case broker_router:env() of
        {ok, Env} ->
            Ctx = #route_ctx{tenant_id = TenantId, publisher = <<"a2a">>,
                             reliability = Msg#broker_message.reliability},
            try
                {ok, [{Partition, _Refs} | _]} =
                    broker_router_hash:route(Msg#broker_message.destination,
                                             Ctx, Env),
                Partitioned = Msg#broker_message{partition_id = Partition},
                broker_queue:append(Partition, Partitioned,
                                    Msg#broker_message.reliability)
            catch
                Class:Reason ->
                    {error, {route_crashed, Class, Reason}}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc The on-the-wire event object: the A2A branch plus our additive `_meta'.
-spec encode(a2a_task:kind(), pos_integer(), map()) -> map().
encode(Kind, Seq, Payload) ->
    Meta = #{<<"_meta">> =>
                 #{<<"unified-mq">> =>
                       #{<<"seq">> => Seq,
                         <<"kind">> => a2a_task:kind_to_wire(Kind)}}},
    maps:merge(Payload, Meta).

%% @doc Parse a stored payload back into `{Kind, Seq, Body}'. `Body' is the full
%% decoded object (including `_meta'), because a projection needs the A2A branch
%% (`task'/'statusUpdate'/'artifactUpdate'/'message') and nothing else should
%% have to know our metadata layout.
-spec decode(binary()) ->
    {ok, #{kind := a2a_task:kind(), seq := pos_integer(), body := map()}}
    | {error, term()}.
decode(Payload) when is_binary(Payload) ->
    try json:decode(Payload) of
        #{<<"_meta">> := #{<<"unified-mq">> :=
                               #{<<"kind">> := KindBin, <<"seq">> := Seq}}} = Body
          when is_integer(Seq), Seq > 0 ->
            case a2a_task:kind_from_wire(KindBin) of
                {ok, Kind} -> {ok, #{kind => Kind, seq => Seq, body => Body}};
                {error, _} -> {error, {unknown_kind, KindBin}}
            end;
        _Other ->
            {error, not_an_a2a_event}
    catch
        _:_ -> {error, invalid_json}
    end;
decode(_Other) ->
    {error, not_json}.

%% @doc Reliability for task events (ADR-025 §D4). One setting for both the
%% creation and the terminal event: a task that was created durably must not
%% complete into a buffer.
-spec reliability() -> sync_mode().
reliability() ->
    application:get_env(broker_proto_a2a, task_reliability, fsync).

%% @doc ADR-025 §D4: `replicated_commit' without a replicator is refused at
%% boot rather than silently degraded to `fsync'.
-spec validate_reliability() -> ok | {error, term()}.
validate_reliability() ->
    case reliability() of
        replicated_commit ->
            case broker_queue:reliability() of
                replicated_commit -> ok;
                _ -> {error, {task_reliability_requires_replicator,
                              replicated_commit}}
            end;
        _Other ->
            ok
    end.

%% @doc RFC 3339 with milliseconds, which is what the A2A `timestamp' fields
%% carry. Kept here so events and projections format time identically.
-spec timestamp() -> binary().
timestamp() ->
    list_to_binary(
      calendar:system_time_to_rfc3339(erlang:system_time(millisecond),
                                     [{unit, millisecond}, {offset, "Z"}])).

%% Stable across replay: the same task's n-th event always has the same id.
event_id(TaskId, Seq) ->
    <<TaskId/binary, ":", (integer_to_binary(Seq))/binary>>.
