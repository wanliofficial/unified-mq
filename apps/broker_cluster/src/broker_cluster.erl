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
-export([join/1, join/2, leave/1, leave/2]).
-export([owner/1, epoch/1, epoch_or_zero/1, replicas/1, snapshot/0,
         partition_view/0]).
-export([authorize_append/2, guard_append/3, token/1]).
-export([replicate/2, install_hooks/0, uninstall_hooks/0]).
%% S12-D (ADR-034): installed as `persistent_term' hooks, never called by
%% adapters directly.
-export([route_op/3, partition_owner/1, reinstall_hooks/1]).

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
    case cross_node_target_ok(Partition, Owner, Replicas) of
        ok -> command({assign, Partition, Owner, Replicas, Epoch});
        {error, _Reason} = Error -> Error
    end.

migrate(Partition, From, To, Epoch) ->
    %% ADR-034 D7: a migration is a metadata change only (ADR-032 D5), so a
    %% target that holds no replica would lose the partition's bytes. With
    %% cross-node delivery on, refuse it before it reaches the log.
    case cross_node_target_ok(Partition, To, replicas_or_empty(Partition)) of
        ok -> command({migrate, Partition, From, To, Epoch});
        {error, _Reason} = Error -> Error
    end.

finish_migration(Partition, Epoch) ->
    command({finish_migration, Partition, Epoch}).

remove(Partition) ->
    Epoch = case epoch(Partition) of
                {ok, E} -> E + 1;
                not_found -> 1
            end,
    command({remove, Partition, Epoch}).

%%%_* Online membership change (ADR-033) ----------------------------------
%%
%% The admin-driven join/leave the roadmap's S12-D names. Thin delegations to
%% `broker_cluster_meta' so a caller (the 8090 management API, or an operator
%% with `rebar3 rpc') changes the membership without owning the ra call. See
%% `broker_cluster_meta:join/1' for the two-step sequence and the failure
%% semantics.

-spec join(node()) -> broker_cluster_meta:member_result().
join(Node) when is_atom(Node) ->
    broker_cluster_meta:join(Node).

-spec join(node(), timeout()) -> broker_cluster_meta:member_result().
join(Node, Timeout) when is_atom(Node) ->
    broker_cluster_meta:join(Node, Timeout).

-spec leave(node()) -> broker_cluster_meta:member_result().
leave(Node) when is_atom(Node) ->
    broker_cluster_meta:leave(Node).

-spec leave(node(), timeout()) -> broker_cluster_meta:member_result().
leave(Node, Timeout) when is_atom(Node) ->
    forget_committed_on_leave(Node, broker_cluster_meta:leave(Node, Timeout)).

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

%% @doc The ownership view keyed by partition -- what a rebalancer needs, since
%% a move must name its partition. `snapshot/0' stays as-is for the callers that
%% only read the metas.
partition_view() -> broker_cluster_cache:partition_view().

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
    ok = install_cross_node_hooks(),
    ok.

%% S12-D (ADR-034 D8): default `false'. When it is off none of the routing
%% hooks exist, so every `broker_queue' / `broker_subscription' path keeps its
%% single-node behaviour byte for byte.
install_cross_node_hooks() ->
    case application:get_env(broker_cluster, cross_node_delivery, false) of
        true ->
            persistent_term:put({broker_queue, router}, fun ?MODULE:route_op/3),
            persistent_term:put({broker_queue, forward_timeout_ms},
                                application:get_env(broker_cluster,
                                                   cross_node_timeout_ms, 2000)),
            persistent_term:put({broker_subscription, owner},
                                fun ?MODULE:partition_owner/1),
            ok;
        _ ->
            ok
    end.

%% @private Test/ops helper (ADR-034 D8): tear every hook down and reinstall the
%% guard with the requested cross-node switch. `install_hooks/0' alone never
%% *erases* a previously installed router, so flipping the switch at runtime needs
%% this; the app does the same dance on start/stop.
reinstall_hooks(CrossNode) ->
    uninstall_hooks(),
    ok = application:set_env(broker_cluster, cross_node_delivery, CrossNode),
    install_hooks().

uninstall_hooks() ->
    _ = persistent_term:erase({broker_queue, append_guard}),
    _ = persistent_term:erase({broker_queue, replicator}),
    _ = persistent_term:erase({broker_queue, router}),
    _ = persistent_term:erase({broker_queue, forward_timeout_ms}),
    _ = persistent_term:erase({broker_subscription, owner}),
    _ = persistent_term:erase({broker_cluster, epoch}),
    ok.

%%%_* Cross-node routing (ADR-034) -----------------------------------------

%% @doc Router hook consulted by `broker_queue' before every data-path call.
%% It reads the local mirror only (`broker_cluster_cache'), never ra, and it
%% runs in the caller's process -- the same split ADR-010 froze for `route/3'.
%%
%% `get('$broker_queue_forwarded')' is set by `broker_queue:forwarded/3': a
%% forwarded call must never be forwarded again, or two nodes whose mirrors
%% disagree would ping-pong. A stale owner answers `{not_owner, Owner}' instead
%% of serving a local log nobody should read.
route_op(Op, Partition, Args) ->
    case broker_cluster_cache:owner(Partition) of
        not_found ->
            local_or_floor(Op, Partition, Args);
        {ok, Owner} when Owner =:= node() ->
            local_or_floor(Op, Partition, Args);
        {ok, Owner} ->
            case get('$broker_queue_forwarded') of
                true ->
                    _ = (catch broker_metrics:inc(cluster_fencing_rejected)),
                    {error, {not_owner, Owner}};
                _ ->
                    {forward, Owner}
            end
    end.

%% @doc Ownership view for `broker_subscription' (D5): a shared group is
%% refused on a node that does not own the partition, because `claim/4' hands
%% out the same offsets independently on every node.
partition_owner(Partition) ->
    broker_cluster_cache:owner(Partition).

local_or_floor(advance_committed, Partition, [Candidate]) ->
    Origin = case get('$broker_queue_origin') of
                 undefined -> node();
                 Node -> Node
             end,
    case broker_cluster_cache:report_committed(Partition, Origin, Candidate) of
        undefined -> local;
        Floor when is_integer(Floor) -> {local, Floor}
    end;
local_or_floor(_Op, _Partition, _Args) ->
    local.

%%%_* Cross-node guards ----------------------------------------------------

cross_node_enabled() ->
    application:get_env(broker_cluster, cross_node_delivery, false) =:= true.

%% ADR-034 D7: with no byte handoff, only a node that already holds a replica
%% can become owner without losing the partition's history.
cross_node_target_ok(Partition, Target, Replicas) ->
    case cross_node_enabled() andalso not lists:member(Target, Replicas) of
        true -> {error, {cross_node_delivery_requires_replica, Partition, Target}};
        false -> ok
    end.

replicas_or_empty(Partition) ->
    case replicas(Partition) of
        {ok, Replicas} -> Replicas;
        not_found -> []
    end.

forget_committed_on_leave(Node, Result) ->
    case Result of
        ok -> _ = broker_cluster_cache:forget_committed(Node), Result;
        {ok, not_member} -> _ = broker_cluster_cache:forget_committed(Node), Result;
        _ -> Result
    end.
