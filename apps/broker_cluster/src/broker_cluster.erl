%% @doc Cluster API and data-path guard (ADR-021 §D2~D5).
%%
%% Ownership/epoch is authoritative in the Metadata Raft Group; the data path
%% reads the local mirror (`broker_cluster_cache') and never pays a consensus
%% round per append. The guard is installed as a `persistent_term' hook so
%% `broker_queue' keeps no compile-time dependency on this application: with
%% `broker_cluster' not running, the queue behaves exactly as it did in S1~S3.
-module(broker_cluster).

-include_lib("broker_core/include/broker_message.hrl").

-export([start/0, stop/0]).
-export([assign/4, migrate/4, finish_migration/2, remove/1]).
-export([owner/1, epoch/1, epoch_or_zero/1, replicas/1, snapshot/0]).
-export([authorize_append/2, guard_append/3, token/1]).
-export([replicate/2, install_hooks/0, uninstall_hooks/0]).

%%%_* Lifecycle -----------------------------------------------------------

start() ->
    {ok, _} = application:ensure_all_started(broker_cluster),
    ok.

stop() ->
    _ = application:stop(broker_cluster),
    ok.

%%%_* Metadata commands ---------------------------------------------------

assign(Partition, Owner, Replicas, Epoch)
  when is_integer(Partition), is_atom(Owner), is_list(Replicas) ->
    command({assign, Partition, Owner, Replicas, Epoch}).

migrate(Partition, From, To, Epoch) ->
    command({migrate, Partition, From, To, Epoch}).

finish_migration(Partition, Epoch) ->
    command({finish_migration, Partition, Epoch}).

remove(Partition) ->
    Epoch = case epoch(Partition) of
                {ok, E} -> E + 1;
                not_found -> 1
            end,
    command({remove, Partition, Epoch}).

command(Command) ->
    case broker_cluster_meta:command(Command) of
        {ok, Epoch} when is_integer(Epoch) ->
            _ = (catch broker_metrics:inc(cluster_owner_changes)),
            {ok, Epoch};
        Other ->
            Other
    end.

%%%_* Reads ---------------------------------------------------------------

owner(Partition) -> broker_cluster_cache:owner(Partition).
epoch(Partition) -> broker_cluster_cache:epoch(Partition).
replicas(Partition) -> broker_cluster_cache:replicas(Partition).

epoch_or_zero(Partition) ->
    case epoch(Partition) of
        {ok, E} -> E;
        not_found -> 0
    end.

snapshot() -> broker_cluster_cache:all().

%% @doc The token a producer should carry for this partition. The epoch is only
%% meaningful together with the owner, so both travel.
token(Partition) ->
    {node(), epoch_or_zero(Partition)}.

%%%_* Fencing -------------------------------------------------------------

%% @doc Accept only if this node is the owner *for that exact epoch*. The epoch
%% is supplied by the caller (carried in `proto_meta.cluster_epoch'), which is
%% what makes a stale writer detectable after a migration.
-spec authorize_append(non_neg_integer(), non_neg_integer()) ->
          ok | {error, term()}.
authorize_append(Partition, Epoch) ->
    case broker_cluster_cache:get(Partition) of
        not_found ->
            %% No metadata: single-node/default mode, nothing to fence.
            ok;
        {ok, #{owner := Owner, epoch := Current} = Meta} ->
            case Owner =:= node() andalso Current =:= Epoch of
                true ->
                    ok;
                false ->
                    _ = (catch broker_metrics:inc(cluster_fencing_rejected)),
                    {error, {fenced, #{partition => Partition,
                                       expected_epoch => Current,
                                       actual_epoch => Epoch,
                                       owner => maps:get(owner, Meta)}}}
            end
    end.

%% @doc Queue hook. A message that carries an epoch is checked against it; one
%% that does not is checked for local ownership only, which still stops a node
%% that already observed a handover from writing.
-spec guard_append(non_neg_integer(), term(), term()) -> ok | {error, term()}.
guard_append(Partition, #broker_message{proto_meta = Meta}, _Mode) ->
    case maps:get(cluster_epoch, Meta, undefined) of
        undefined -> owner_check(Partition);
        Epoch when is_integer(Epoch) -> authorize_append(Partition, Epoch)
    end;
guard_append(Partition, _Msg, _Mode) ->
    owner_check(Partition).

owner_check(Partition) ->
    case broker_cluster_cache:get(Partition) of
        not_found ->
            ok;
        {ok, #{owner := Owner, epoch := E}} when Owner =:= node() ->
            case E of
                _ -> ok
            end;
        {ok, #{owner := Owner, epoch := E}} ->
            _ = (catch broker_metrics:inc(cluster_fencing_rejected)),
            {error, {fenced, #{partition => Partition, expected_epoch => E,
                               owner => Owner}}}
    end.

%%%_* Replication ---------------------------------------------------------

replicate(Partition, Msg) ->
    broker_cluster_rep:replicate(Partition, Msg).

%%%_* Hooks ---------------------------------------------------------------

install_hooks() ->
    persistent_term:put({broker_queue, append_guard},
                        fun ?MODULE:guard_append/3),
    persistent_term:put({broker_queue, replicator},
                        fun ?MODULE:replicate/2),
    %% Optional third hook: lets the protocol adapter stamp the epoch it saw
    %% into `proto_meta' without depending on this application.
    persistent_term:put({broker_cluster, epoch}, fun ?MODULE:epoch/1),
    ok.

uninstall_hooks() ->
    _ = persistent_term:erase({broker_queue, append_guard}),
    _ = persistent_term:erase({broker_queue, replicator}),
    _ = persistent_term:erase({broker_cluster, epoch}),
    ok.
