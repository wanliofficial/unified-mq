%% @doc Replication for `replicated_commit' appends (ADR-021 §D5).
%%
%% The local append already fsynced before this module is called. Replication is
%% therefore "copy the same message to the replica set and wait for a majority",
%% not a second consensus round per message: the metadata group decides who owns
%% a partition, the data path only copies bytes.
%%
%% A follower writes with `broker_queue:append_replica/3', which deliberately
%% does not notify local subscribers or run the ownership guard. Followers serve
%% no reads until ownership moves to them.
-module(broker_cluster_rep).

-export([replicate/2, replicate/3, remote_append/2]).

-define(MAX_ACKS, 64).

%% @doc Copy one message to this partition's replica set and report whether a
%% majority acknowledged. `no metadata' means "single-node default" and is a
%% success; the caller only asked for replication when metadata exists.
-spec replicate(non_neg_integer(), term()) -> ok | {error, term()}.
replicate(Partition, Msg) ->
    case broker_cluster:replicas(Partition) of
        {ok, Replicas} -> replicate(Partition, Msg, Replicas);
        not_found -> ok
    end.

-spec replicate(non_neg_integer(), term(), [node()]) -> ok | {error, term()}.
replicate(_Partition, _Msg, []) ->
    ok;
replicate(_Partition, _Msg, [Node]) when Node =:= node() ->
    ok;
replicate(Partition, Msg, Replicas) ->
    Needed = (length(Replicas) div 2) + 1,
    Others = [N || N <- Replicas, N =/= node()],
    Timeout = application:get_env(broker_cluster, replicate_timeout_ms, 2000),
    Started = erlang:monotonic_time(microsecond),
    Acks = 1 + parallel_append(Partition, Msg, Others, Timeout),
    %% The local fsync already happened, so this measures the replica round
    %% only: majority wait on success, timeout when the majority never forms.
    _ = (catch broker_metrics:observe(
                   cluster_replication_latency_us,
                   erlang:monotonic_time(microsecond) - Started)),
    case Acks >= Needed of
        true ->
            ok;
        false ->
            _ = (catch broker_metrics:inc(cluster_replication_failed)),
            {error, {replication_failed, Acks, Needed}}
    end.

%% Executed on a replica node. `fsync' is the follower's durability level: a
%% follower that acknowledged must survive its own crash too.
%%
%% The contract with `parallel_append/4' is a bare `ok' or `{error, _}': the
%% local append answers `{ok, Offset}', and returning that verbatim made every
%% real cross-node replica look like a failure (the offset tuple fell into the
%% `_Error' branch). Normalising here is what keeps a majority of successful
%% followers counted as success.
remote_append(Partition, Msg) ->
    case broker_queue:append_replica(Partition, Msg, fsync) of
        {ok, _Offset} -> ok;
        {error, _Reason} = Error -> Error
    end.

%%%_* Internal ------------------------------------------------------------

parallel_append(Partition, Msg, Nodes, Timeout) ->
    Parent = self(),
    _ = [spawn(fun() ->
                       Result = rpc:call(N, ?MODULE, remote_append,
                                         [Partition, Msg], Timeout),
                       Parent ! {replicate_result, self(), Result}
               end) || N <- lists:sublist(Nodes, ?MAX_ACKS)],
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    collect(length(Nodes), Deadline).

collect(0, _Deadline) ->
    0;
collect(N, Deadline) ->
    Remaining = max(0, Deadline - erlang:monotonic_time(millisecond)),
    receive
        {replicate_result, _Pid, ok} ->
            1 + collect(N - 1, Deadline);
        {replicate_result, _Pid, _Error} ->
            collect(N - 1, Deadline)
    after Remaining ->
        0
    end.
