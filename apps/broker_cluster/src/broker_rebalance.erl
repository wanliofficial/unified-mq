%% @doc The rebalance trigger (ADR-032 D4, docs/design/s12-infrastructure-ha-and-lb.md
%% R-RB1/R-RB2). A new *caller* of the four metadata commands
%% (`assign/4`, `migrate/4`, `finish_migration/2`, `remove/1`) -- not a new write
%% path, not a new consensus, not a new app, and deliberately no process of its
%% own. It reads the local ownership view and decides, then hands the decision to
%% the existing commands; epoch monotonicity and fencing stay in
%% `broker_cluster_meta_machine'.
%%
%% What is frozen here (D4):
%%
%%   * **Default closed.** `enabled => false'. A node joining or leaving never
%%     auto-triggers a rebalance; an operator runs it by hand. That is the
%%     continuation of ADR-021's "S4 does not auto-rebalance": the rebalancer is
%%     an optional tool, never an automatic behaviour. Because it only moves
%%     ownership on explicit invocation, a manual `assign' always wins -- there is
%%     no background loop to fight it (this is how D4's "manual override has
%%     priority" is honoured without adding pin state to the ra metadata).
%%   * **Target function = partition-count balance.** `count(partitions owned by
%%     Node)`. Not connection count (D7: `connections_current` only covers the MQTT
%%     family, so weighting by it today would weight by fake data), not memory/CPU
%%     (neither is in the frozen 62-key metric set).
%%   * **Stability = a double gate.** Do not move unless *both*: the spread
%%     (max count - min count) exceeds `hysteresis` (default 1), and at least
%%     `cooldown_ms` (default 300000) has passed since the last move. Both gates
%%     must hold to act; the spread gate alone is checked first so a stable view
%%     never reports "cooldown".
%%   * **One move per round.** `step/0` issues exactly one command. Balancing a
%%     cluster means calling `step/0` repeatedly until it answers `{stable, _}'.
%%   * **Granularity = one partition.** The smallest unit of an owner switch is a
%%     single partition; cross-group balancing is an operator config change
%%     (ADR-008 :15-19), never automatic.
%%
%% The two-phase move is ADR-021 D3 / ADR-032 D5: `migrate' marks the partition
%% `migrating' with epoch+1 and makes the target the owner while the source is
%% still readable; `finish_pending/0' later clears it to `active'. A partition
%% whose owner has left the membership is *orphaned* and is brought home with
%% `assign' (there is no live source to `migrate' from), which is still one of
%% the four frozen commands.
%%
%% This module moves ownership metadata only. It does not migrate any live
%% process or queue data (D5): consumers reconnect to the new owner and rebuild
%% (ADR-019), which is the documented, deliberate limitation -- not a gap.
-module(broker_rebalance).

%% The pure decision (no ra round-trip): what *should* happen given the view.
%% `evaluate/1' takes the membership explicitly so the decision is a pure function
%% of (view, membership, config) and can be tested without forming a real group.
%% Production always calls `evaluate/0', which passes the ra membership -- the
%% trigger never keeps a member set of its own (R-LB3).
-export([evaluate/0, evaluate/1]).
%% The imperative step: apply one move.
-export([step/0, step/1]).
%% Complete every partition still in `migrating' state.
-export([finish_pending/0]).
%% Current configuration and balance summary, for ops.
-export([status/0]).

-define(PT_LAST, {?MODULE, last_migration_ms}).

%%%_* Pure decision --------------------------------------------------------

%% @doc Decide from the current view without touching ra. Returns:
%%
%%   {disabled}                         -- rebalancing is switched off
%%   {stable, Summary}                  -- within hysteresis, nothing to do
%%   {cooldown, Summary}                -- would move, but too soon
%%   {rebalance, Plan}                  -- one move is warranted
%%
%% `Plan' carries the exact command `step/0' will run plus the numbers behind the
%% decision, so a caller can log or preview it.
-spec evaluate() -> {disabled}
                 | {stable, map()}
                 | {cooldown, map()}
                 | {rebalance, map()}.
evaluate() ->
    evaluate(broker_cluster_meta:members()).

%% @doc The decision against an explicit membership. `Members' is the ra member
%% list (what production passes); nothing here consults a second source.
-spec evaluate([node()]) -> {disabled}
                           | {stable, map()}
                           | {cooldown, map()}
                           | {rebalance, map()}.
evaluate(Members) when is_list(Members) ->
    Cfg = config(),
    case maps:get(enabled, Cfg) of
        false ->
            {disabled};
        true ->
            View = broker_cluster:partition_view(),
            decide(View, Members, Cfg)
    end.

%%%_* Imperative step ------------------------------------------------------

%% @doc Apply at most one rebalance move. Same gate order as `evaluate/0'; on a
%% warranted move it issues the single command, records the time (for the
%% cooldown), and returns what it did. A `{error, no_quorum}' means the metadata
%% group could not commit and the view is unchanged -- the honest answer, never a
%% silent degrade (ADR-021 D4).
-spec step() -> {disabled}
              | {stable, map()}
              | {cooldown, map()}
              | {migrated, non_neg_integer(), node(), node()}
              | {reassigned, non_neg_integer(), node()}
              | {error, term()}.
step() ->
    step(broker_cluster_meta:members()).

%% @doc Apply at most one move against an explicit membership (see `evaluate/1').
-spec step([node()]) -> {disabled}
                        | {stable, map()}
                        | {cooldown, map()}
                        | {migrated, non_neg_integer(), node(), node()}
                        | {reassigned, non_neg_integer(), node()}
                        | {error, term()}.
step(Members) when is_list(Members) ->
    Cfg = config(),
    case maps:get(enabled, Cfg) of
        false ->
            {disabled};
        true ->
            View = broker_cluster:partition_view(),
            case decide(View, Members, Cfg) of
                {rebalance, #{command := {migrate, P, From, To, Epoch}}} ->
                    run_migrate(P, From, To, Epoch);
                {rebalance, #{command := {assign, P, To, Replicas, Epoch}}} ->
                    run_assign(P, To, Replicas, Epoch);
                {stable, _} = Stable ->
                    Stable;
                {cooldown, _} = Cooldown ->
                    Cooldown
            end
    end.

run_migrate(P, From, To, Epoch) ->
    case broker_cluster:migrate(P, From, To, Epoch) of
        {ok, Epoch} ->
            record_now(),
            {migrated, P, From, To};
        {error, _} = Error ->
            Error
    end.

run_assign(P, To, Replicas, Epoch) ->
    case broker_cluster:assign(P, To, Replicas, Epoch) of
        {ok, Epoch} ->
            record_now(),
            {reassigned, P, To};
        {error, _} = Error ->
            Error
    end.

%% @doc Finish every partition currently in the `migrating' state, oldest epoch
%% first. This is the second half of ADR-021 D3: `migrate' alone leaves the view
%% showing `migrating'; finishing returns it to `active' at epoch+1.
-spec finish_pending() -> {finished, [non_neg_integer()]}.
finish_pending() ->
    View = broker_cluster:partition_view(),
    Pending = lists:sort(
                 [P || {P, #{state := migrating}} <- maps:to_list(View)]),
    Finished = lists:filtermap(
                  fun(P) ->
                          case broker_cluster:epoch(P) of
                              {ok, E} ->
                                  case broker_cluster:finish_migration(P, E + 1) of
                                      {ok, _} -> {true, P};
                                      {error, _} -> false
                                  end;
                              not_found ->
                                  false
                          end
                  end, Pending),
    {finished, Finished}.

%%%_* Status ---------------------------------------------------------------

%% @doc Current configuration plus a live balance summary, for `rebar3 shell'
%% inspection and the capacity/migration report. Never issues a write.
-spec status() -> map().
status() ->
    Cfg = config(),
    View = broker_cluster:partition_view(),
    Members = broker_cluster_meta:members(),    Counts = count_by_owner(View, Members),
    #{enabled => maps:get(enabled, Cfg),
      hysteresis => maps:get(hysteresis, Cfg),
      cooldown_ms => maps:get(cooldown_ms, Cfg),
      members => Members,
      counts => Counts,
      spread => spread(Members, Counts),
      orphaned => orphaned(View, Members),
      last_migration_ms => last_ms()}.

%%%_* Internal -------------------------------------------------------------

decide(View, Members, Cfg) ->
    Hyst = maps:get(hysteresis, Cfg),
    Counts = count_by_owner(View, Members),
    Spread = spread(Members, Counts),
    Orphans = orphaned(View, Members),
    Summary = #{counts => Counts, spread => Spread, orphaned => Orphans,
                members => Members},
    case pick_target(View, Members, Counts, Spread, Orphans, Hyst) of
        stable ->
            {stable, Summary};
        cooldown ->
            {cooldown, Summary};
        {migrate, P, From, To, Epoch} ->
            {rebalance, #{action => migrate, partition => P, from => From,
                          to => To, epoch => Epoch, counts => Counts,
                          command => {migrate, P, From, To, Epoch}}};
        {reassign, P, To, Replicas, Epoch} ->
            {rebalance, #{action => reassign, partition => P, from => undefined,
                          to => To, epoch => Epoch, counts => Counts,
                          command => {assign, P, To, Replicas, Epoch}}}
    end.

%% Choose what, if anything, to move. Priority: bring an orphaned partition home
%% first (a departed node's work must not stay stranded), then balance live
%% ownership -- the latter gated by the hysteresis/cooldown double threshold.
pick_target(View, Members, Counts, Spread, Orphans, Hyst) ->
    case Orphans of
        [P | _] ->
            %% Someone left; give their partition to the least-loaded member. Not
            %% subject to the cooldown: this is recovery, not tuning.
            case least_loaded(Members, Counts) of
                {ok, To} -> {reassign, P, To, Members, epoch_of(View, P) + 1};
                error -> stable
            end;
        [] ->
            case {most_loaded(Members, Counts), least_loaded(Members, Counts)} of
                {{ok, From}, {ok, To}} when From =/= To ->
                    case Spread > Hyst of
                        false ->
                            stable;
                        true ->
                            case within_cooldown() of
                                true ->
                                    cooldown;
                                false ->
                                    case pick_movable(View, From) of
                                        {ok, P} ->
                                            {migrate, P, From, To, epoch_of(View, P) + 1};
                                        none ->
                                            stable
                                    end
                            end
                    end;
                _ ->
                    stable
            end
    end.

%% A partition is movable if it is `active' and owned by `From`. One per round.
pick_movable(View, From) ->
    Candidates = [P || {P, #{owner := O, state := active}} <- maps:to_list(View),
                         O =:= From],
    case lists:sort(Candidates) of
        [] -> none;
        [P | _] -> {ok, P}
    end.

%% Active partition count per member, read straight from the view so the target
%% function never depends on a second source of truth. Counted by *owner*, not by
%% phase: ADR-021 D3 makes the target the owner the moment `migrate' is applied,
%% so a partition mid-migration already belongs to its target and is counted there.
%% That is what makes one `step/0' reduce the spread by two (one leaves the
%% source, one arrives at the target) rather than by one.
count_by_owner(View, Members) ->
    Owned = [M || {_P, #{owner := M}} <- maps:to_list(View)],
    Base = [{M, 0} || M <- Members],
    lists:foldl(
      fun(M, Acc) ->
              maps:update_with(M, fun(N) -> N + 1 end, 1, Acc)
      end, maps:from_list(Base), Owned).

%% max(count) - min(count) over the membership; 0 for empty or single-member.
spread([], _Counts) -> 0;
spread([_], _Counts) -> 0;
spread(Members, Counts) ->
    Ns = [maps:get(M, Counts, 0) || M <- Members],
    lists:max(Ns) - lists:min(Ns).

%% Partitions still marked active but owned by a node no longer in the membership:
%% the signature of a node that left without an operator draining it first.
orphaned(View, Members) ->
    MemberSet = lists:usort(Members),
    lists:sort([P || {P, #{owner := O, state := active}} <- maps:to_list(View),
                     not lists:member(O, MemberSet)]).

most_loaded(Members, Counts) ->
    arg_extreme(Members, Counts, fun(N, Acc) -> N > Acc end).

least_loaded(Members, Counts) ->
    arg_extreme(Members, Counts, fun(N, Acc) -> N < Acc end).

%% Deterministic extreme: the first member (in membership order) that attains the
%% max/min count. Ties broken by membership order keep decisions reproducible.
arg_extreme([], _Counts, _Better) ->
    error;
arg_extreme([M | Rest], Counts, Better) ->
    Seed = {M, maps:get(M, Counts, 0)},
    {Winner, _} = lists:foldl(
                     fun(Cand, {AccM, AccN}) ->
                             N = maps:get(Cand, Counts, 0),
                             case Better(N, AccN) of
                                 true -> {Cand, N};
                                 false -> {AccM, AccN}
                             end
                     end, Seed, Rest),
    {ok, Winner}.

epoch_of(View, P) ->
    case maps:find(P, View) of
        {ok, #{epoch := E}} -> E;
        error -> 0
    end.

%%%_* Config, clock, cooldown ----------------------------------------------

config() ->
    Defaults = #{enabled => false, hysteresis => 1, cooldown_ms => 300000,
                 now => fun() -> erlang:monotonic_time(millisecond) end},
    maps:merge(Defaults,
               application:get_env(broker_cluster, rebalance, #{})).

now_ms() -> (maps:get(now, config()))().

last_ms() -> persistent_term:get(?PT_LAST, undefined).

within_cooldown() ->
    case last_ms() of
        undefined -> false;
        Last -> now_ms() - Last < maps:get(cooldown_ms, config())
    end.

record_now() ->
    persistent_term:put(?PT_LAST, now_ms()),
    ok.
