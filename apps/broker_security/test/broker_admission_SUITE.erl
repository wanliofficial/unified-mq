%% @doc S12-B (ADR-032 D7): the node-level admission gate.
%%
%% The gate's whole job is to be right under two kinds of pressure that a
%% single-threaded test cannot show: many connections arriving at once, and a
%% limit that can be flipped while leases are outstanding. So the cases here are
%% mostly about *interleavings* -- concurrent acquires must never exceed the
%% limit, release must be safe to run twice, and a gate turned off/on must not
%% invent or lose slots.
%%
%% The `persistent_term' that holds the counter is global, so every case resets
%% both the config and the counter in `init_per_testcase'; a leaked count would
%% make the next case fail for the wrong reason.
-module(broker_admission_SUITE).

-compile([export_all, nowarn_export_all]).

all() ->
    [gate_disabled_by_default,
     acquires_up_to_the_limit_and_refuses_the_next,
     concurrent_acquires_never_exceed_the_limit,
     release_returns_the_slot,
     double_release_does_not_go_negative,
     rejected_lease_releases_nothing,
     unlimited_lease_releases_nothing,
     limit_is_read_per_call_not_cached,
     draining_refuses_even_with_room,
     not_started_refuses,
     snapshot_reports_what_is_enforced,
     %% The lease is only as good as its release point, and `terminate/2' is not
     %% one on MQTT: conns are `temporary, brutal_kill' children, so a killed
     %% connection runs no teardown at all.
     a_killed_owner_does_not_leak_its_slot,
     a_live_owner_is_not_reclaimed,
     adopt_moves_the_slot_to_the_owning_process].

init_per_suite(Config) ->
    Saved = application:get_env(broker_security, limits, undefined),
    [{saved_limits, Saved} | Config].

end_per_suite(Config) ->
    case proplists:get_value(saved_limits, Config) of
        undefined -> ok;
        Saved -> application:set_env(broker_security, limits, Saved)
    end,
    ok.

init_per_testcase(_Case, Config) ->
    application:set_env(broker_security, limits, #{}),
    ok = broker_admission:init(),
    ok = broker_admission:reset(),
    Config.

end_per_testcase(_Case, _Config) ->
    persistent_term:erase({broker_drain, state}),
    application:set_env(broker_security, limits, #{}),
    ok = broker_admission:reset(),
    ok.

%%%_* Defaults -------------------------------------------------------------

%% The default has to leave existing deployments alone: before S12-B the node had
%% no node-wide cap (ranch's 1024 is per listener), so switching one on during an
%% upgrade would reject connections that used to work. An operator turns it on.
gate_disabled_by_default(_Config) ->
    infinity = broker_admission:limit(),
    unlimited = broker_admission:try_acquire(),
    0 = broker_admission:in_use(),
    ok.

%%%_* The limit ------------------------------------------------------------

acquires_up_to_the_limit_and_refuses_the_next(_Config) ->
    set_limit(2),
    {leased, _} = broker_admission:try_acquire(),
    {leased, _} = broker_admission:try_acquire(),
    %% The third is refused, and refused *without* queueing: it answers at once,
    %% because D7 says an over-capacity connection is rejected, not parked.
    {rejected, over_capacity} = broker_admission:try_acquire(),
    2 = broker_admission:in_use(),
    ok.

release_returns_the_slot(_Config) ->
    set_limit(1),
    L1 = broker_admission:try_acquire(),
    {rejected, over_capacity} = broker_admission:try_acquire(),
    ok = broker_admission:release(L1),
    0 = broker_admission:in_use(),
    %% And the freed slot is usable again -- release that only decremented a
    %% number without the gate re-reading it would strand capacity.
    {leased, _} = broker_admission:try_acquire(),
    1 = broker_admission:in_use(),
    ok.

%% The case the compare_exchange is for. A read-then-write gate lets several
%% processes see the same free slot and all take it; the assertion is on the
%% *count of successes*, not on any single answer, so a broken gate fails here
%% rather than intermittently.
%%
%% The winners must stay alive for the duration, and that is not a detail: a
%% lease whose owner has died is not really held, so the gate reclaims it on the
%% next refusal. Children that exit right after acquiring would therefore let
%% more than `Limit' of them "succeed" -- correctly, but it would measure the
%% sweep instead of the race. A real connection also outlives its admission.
concurrent_acquires_never_exceed_the_limit(_Config) ->
    Limit = 8,
    Waiters = 60,
    set_limit(Limit),
    Parent = self(),
    Go = make_ref(),
    Pids = [spawn(fun() ->
                          receive Go -> ok end,
                          Lease = broker_admission:try_acquire(),
                          Parent ! {lease, self(), Lease},
                          receive stop -> ok end
                  end) || _ <- lists:seq(1, Waiters)],
    %% Release them together so they genuinely contend instead of trickling in.
    [Pid ! Go || Pid <- Pids],
    Leases = [receive {lease, Pid, L} -> L end || Pid <- Pids],
    Leased = [L || {leased, _} = L <- Leases],
    Rejected = [L || {rejected, over_capacity} = L <- Leases],
    Limit = length(Leased),
    true = length(Rejected) =:= Waiters - Limit,
    Limit = broker_admission:in_use(),
    %% Distinct lease keys: two winners must not be handed the same key, or the
    %% ledger would have one row for two holders and a release would free both.
    Limit = length(lists:usort([N || {leased, N} <- Leased])),
    [Pid ! stop || Pid <- Pids],
    ok.

%% A front end's teardown can run twice (an explicit release plus a `terminate').
%% That must not corrupt the counter for everyone else. Idempotency comes from
%% `ets:take/2' on the ledger row: whichever of release and sweep arrives first
%% decrements, and the second finds nothing to do.
double_release_does_not_go_negative(_Config) ->
    set_limit(4),
    {leased, _} = L = broker_admission:try_acquire(),
    ok = broker_admission:release(L),
    ok = broker_admission:release(L),
    0 = broker_admission:in_use(),
    %% The next acquire still succeeds, i.e. the counter is coherent.
    {leased, _} = broker_admission:try_acquire(),
    ok.

%% A refused connection has no slot, so a front end that runs its normal teardown
%% on the refusal path must not take a slot away from somebody else.
rejected_lease_releases_nothing(_Config) ->
    set_limit(1),
    {leased, _} = Keep = broker_admission:try_acquire(),
    Refused = broker_admission:try_acquire(),
    {rejected, over_capacity} = Refused,
    ok = broker_admission:release(Refused),
    1 = broker_admission:in_use(),
    ok = broker_admission:release(Keep),
    0 = broker_admission:in_use(),
    ok.

%% The gate-disabled lease is the same story in disguise: it never incremented,
%% so releasing it must not decrement. Collapsing `unlimited' into `{leased, 0}'
%% would let a single connection under a disabled gate free a slot another
%% connection holds.
unlimited_lease_releases_nothing(_Config) ->
    infinity = broker_admission:limit(),
    Lease = broker_admission:try_acquire(),
    unlimited = Lease,
    0 = broker_admission:in_use(),
    ok = broker_admission:release(Lease),
    0 = broker_admission:in_use(),
    %% Now switch the gate on: the count must still start from zero.
    set_limit(1),
    {leased, _} = broker_admission:try_acquire(),
    ok.

%% `broker_limits' promises that what is reported is what is enforced. Caching
%% the limit at boot would make a config change disagree with `snapshot/0' until a
%% restart, which is exactly the "config says X, code does Y" the limits module
%% documents against.
limit_is_read_per_call_not_cached(_Config) ->
    set_limit(1),
    1 = broker_admission:limit(),
    set_limit(5),
    5 = broker_admission:limit(),
    5 = maps:get(limit, broker_admission:snapshot()),
    %% And the new bound is actually applied, not just reported.
    {leased, _} = broker_admission:try_acquire(),
    {leased, _} = broker_admission:try_acquire(),
    2 = broker_admission:in_use(),
    ok.

%% Drain wins over capacity: a node being removed must not accept new work even
%% when it has room, because the connections it admits would be invisible to the
%% drain that is already in flight.
draining_refuses_even_with_room(_Config) ->
    set_limit(100),
    persistent_term:put({broker_drain, state}, draining),
    {rejected, draining} = broker_admission:try_acquire(),
    0 = broker_admission:in_use(),
    persistent_term:put({broker_drain, state}, down),
    {rejected, draining} = broker_admission:try_acquire(),
    persistent_term:put({broker_drain, state}, up),
    {leased, _} = broker_admission:try_acquire(),
    ok.

%% `init/0' not having run is a refusal, not a pass: a gate that cannot read its
%% own counter cannot know it has room, and admitting on that basis would be a
%% silent unlimited node.
not_started_refuses(Config) ->
    persistent_term:erase({broker_admission, state}),
    set_limit(4),
    {rejected, not_started} = broker_admission:try_acquire(),
    0 = broker_admission:in_use(),
    not_started = maps:get(state, broker_admission:snapshot()),
    ok = broker_admission:init(),
    _ = Config,
    ok.

snapshot_reports_what_is_enforced(_Config) ->
    #{limit := infinity, in_use := 0, state := up} = broker_admission:snapshot(),
    set_limit(3),
    _ = broker_admission:try_acquire(),
    #{limit := 3, in_use := 1, state := up} = broker_admission:snapshot(),
    ok.

%%%_* Reclaiming what a dead owner cannot give back -------------------------
%%
%% This is the defect the first version of this module had. `mqtt_conn_sup' runs
%% its children `temporary, brutal_kill', so a connection killed by its
%% supervisor (or by the OOM killer) never executes `terminate/3'. If the slot
%% only came back from an explicit release, one such kill would shrink the node
%% permanently, and enough of them would make a healthy node refuse everybody
%% while claiming to be at capacity -- worse than having no gate.

%% The exact shape: a process takes a slot and dies without releasing. The next
%% caller must still get in, because the slot is not really held.
a_killed_owner_does_not_leak_its_slot(_Config) ->
    set_limit(1),
    {Lease, Pid} = acquire_in_child(),
    {leased, _} = Lease,
    1 = broker_admission:in_use(),
    %% `exit(kill)' on purpose: it is the one exit that cannot be trapped, so it
    %% is the closest thing to the brutal_kill a real connection suffers.
    MRef = erlang:monitor(process, Pid),
    exit(Pid, kill),
    receive {'DOWN', MRef, process, Pid, killed} -> ok
    after 5000 -> exit({child_did_not_die, Pid})
    end,
    %% The counter is still high -- nothing has swept yet, and nothing should
    %% have: a sweep on the fast path would cost every admission an ETS scan.
    1 = broker_admission:in_use(),
    %% Refusing is the moment the question becomes load-bearing, so the answer
    %% must be "there is room, the previous owner is gone".
    {leased, _} = broker_admission:try_acquire(),
    1 = broker_admission:in_use(),
    ok.

%% The other half of the same rule: a sweep must never reclaim a slot from a
%% *live* owner. A sleeping MQTT-SN session is exactly that -- alive, holding a
%% slot, and silent for minutes -- so a gate that reclaimed quiet connections
%% would over-admit under the workload it was built for.
a_live_owner_is_not_reclaimed(_Config) ->
    set_limit(1),
    Parent = self(),
    Owner = spawn(fun() ->
                          Lease = broker_admission:try_acquire(),
                          Parent ! {acquired, Lease},
                          receive stop -> broker_admission:release(Lease)
                          end
                  end),
    receive {acquired, {leased, _}} -> ok
    after 5000 -> exit(no_lease)
    end,
    1 = broker_admission:in_use(),
    %% Refused, and the refusal must not have "found room" by clearing a row
    %% whose owner is alive and merely idle.
    {rejected, over_capacity} = broker_admission:try_acquire(),
    1 = broker_admission:in_use(),
    Owner ! stop,
    MRef = erlang:monitor(process, Owner),
    receive {'DOWN', MRef, process, Owner, _} -> ok
    after 5000 -> exit({owner_did_not_stop, Owner})
    end,
    0 = broker_admission:in_use(),
    ok.

%% The QUIC front end takes its lease in the listener -- the only place a refusal
%% can still be answered -- while the connection lives in a process created
%% afterwards. Without `adopt/1' the ledger would name the listener as the owner,
%% so killing the connection (and not the listener) would leak the slot.
adopt_moves_the_slot_to_the_owning_process(_Config) ->
    set_limit(1),
    {Lease, Taker} = acquire_in_child(),
    {leased, _} = Lease,
    Parent = self(),
    %% A second process adopts the lease, exactly as `mqtt_quic_conn:init/1' does
    %% with what the listener handed it.
    Owner = spawn(fun() ->
                          Adopted = broker_admission:adopt(Lease),
                          Parent ! {adopted, Adopted},
                          receive stop -> ok end
                  end),
    receive {adopted, Adopted} -> Lease = Adopted
    after 5000 -> exit(no_adopt)
    end,
    %% The taker is gone, but the slot now belongs to the live owner, so it must
    %% NOT be reclaimed yet: this is what distinguishes `adopt/1' from a re-take.
    stop_child(Taker),
    {rejected, over_capacity} = broker_admission:try_acquire(),
    %% Kill the real owner -- now the slot must come back through the sweep even
    %% though no `release/1' runs.
    stop_child(Owner),
    {leased, _} = broker_admission:try_acquire(),
    1 = broker_admission:in_use(),
    ok.

%%%_* Helpers -------------------------------------------------------------

%% Take a slot in a separate process, so the ledger row names that process and
%% killing it is what the gate has to cope with.
acquire_in_child() ->
    Parent = self(),
    Pid = spawn(fun() ->
                       Parent ! {acquired, broker_admission:try_acquire()},
                       receive stop -> ok end
               end),
    receive {acquired, Lease} -> {Lease, Pid}
    after 5000 -> exit({no_lease_from_child, Pid})
    end.

stop_child(Pid) ->
    MRef = erlang:monitor(process, Pid),
    exit(Pid, kill),
    receive {'DOWN', MRef, process, Pid, _} -> ok
    after 5000 -> exit({child_survived, Pid})
    end.

set_limit(Map) when is_map(Map) ->
    application:set_env(broker_security, limits, Map);
set_limit(Max) ->
    application:set_env(broker_security, limits, #{max_connections => Max}).
