%% @doc Metadata Raft state machine (ADR-021 §D2/§D3).
%%
%% The state is tiny on purpose: partition ownership, epoch, replica set and
%% migration state. Message bodies never enter it (ADR-002), so snapshots stay
%% small and a metadata replay is cheap.
%%
%% Epoch monotonicity is enforced here, at the state machine, not at the API:
%% a stale command that reaches the log after a newer one must be rejected even
%% if the caller raced. Every applied change is mirrored into the local cache,
%% which is what the data path reads.
-module(broker_cluster_meta_machine).

-behaviour(ra_machine).

-export([init/1, apply/3, snapshot_installed/5, overview/1]).

-type partition() :: non_neg_integer().
-type meta() :: #{owner := node(),
                  epoch := non_neg_integer(),
                  replicas := [node()],
                  state := active | migrating,
                  index := non_neg_integer()}.
-type state() :: #{partitions := #{partition() => meta()}}.

-spec init(map()) -> state().
init(_Config) ->
    #{partitions => #{}}.

-spec apply(map(), term(), state()) ->
          {state(), term(), list()} | {state(), term()}.
apply(#{index := Index}, Command, #{partitions := Ps} = State) ->
    case Command of
        {assign, P, Owner, Replicas, Epoch} ->
            assign(P, Owner, Replicas, Epoch, active, Index, Ps, State);
        {migrate, P, _From, To, Epoch} ->
            assign(P, To, replicas_of(P, Ps), Epoch, migrating, Index, Ps, State);
        {finish_migration, P, Epoch} ->
            case maps:find(P, Ps) of
                {ok, #{epoch := E} = M} when E < Epoch ->
                    M1 = M#{state := active, epoch := Epoch, index := Index},
                    broker_cluster_cache:put(P, M1),
                    {State#{partitions := Ps#{P => M1}}, {ok, Epoch}, []};
                {ok, #{epoch := E}} ->
                    {State, {error, {stale_epoch, E, Epoch}}, []};
                error ->
                    {State, {error, not_found}, []}
            end;
        {remove, P, Epoch} ->
            case maps:find(P, Ps) of
                {ok, #{epoch := E}} when E < Epoch ->
                    broker_cluster_cache:clear_partition(P),
                    {State#{partitions := maps:remove(P, Ps)}, {ok, Epoch}, []};
                {ok, #{epoch := E}} ->
                    {State, {error, {stale_epoch, E, Epoch}}, []};
                error ->
                    {State, {error, not_found}, []}
            end;
        {get, P} ->
            case maps:find(P, Ps) of
                {ok, M} -> {State, {ok, M}, []};
                error -> {State, not_found, []}
            end;
        snapshot ->
            {State, {ok, maps:values(Ps)}, []};
        Other ->
            {State, {error, {unknown_command, Other}}, []}
    end.

%% Rebuilding the cache after a snapshot install: apply/3 is not replayed for
%% entries covered by the snapshot, so without this a restarted node would keep
%% an empty cache until the next ownership change.
snapshot_installed(_Meta, _Snapshot, _State, _Machine, _Module) ->
    #{partitions := Ps} = _State,
    broker_cluster_cache:clear(),
    maps:foreach(fun(P, M) -> broker_cluster_cache:put(P, M) end, Ps),
    ok.

overview(#{partitions := Ps}) ->
    #{partitions => maps:size(Ps)}.

%%%_* Internal ------------------------------------------------------------

assign(P, Owner, Replicas, Epoch, Phase, Index, Ps, State) ->
    case maps:find(P, Ps) of
        {ok, #{epoch := E}} when E >= Epoch ->
            {State, {error, {stale_epoch, E, Epoch}}, []};
        _ when not is_list(Replicas); Replicas =:= [] ->
            {State, {error, {bad_replicas, Replicas}}, []};
        _ ->
            M = #{owner => Owner, epoch => Epoch, replicas => Replicas,
                  state => Phase, index => Index},
            broker_cluster_cache:put(P, M),
            {State#{partitions := Ps#{P => M}}, {ok, Epoch}, []}
    end.

replicas_of(P, Ps) ->
    case maps:find(P, Ps) of
        {ok, #{replicas := R}} -> R;
        error -> [node()]
    end.
