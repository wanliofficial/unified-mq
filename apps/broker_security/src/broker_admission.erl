%% @doc Node-level admission gate (S12-B, ADR-032 D7).
%%
%% D7 asks for a counter *above* ranch's per-listener `max_connections', because
%% ranch does not reject at the limit: `ranch_conns_sup' parks the acceptor in
%% `Sleepers' and the connection is still established, so an over-capacity client
%% sits on a socket that never answers. The spike
%% (`docs/design/s12b-admission-gate-spike.md') settled two things this module
%% implements:
%%
%%   * the gate is asked at the conn/FSM seam -- the moment a connection exists
%%     but has not been promised service -- so each front end can refuse in its
%%     own protocol's words instead of hanging up silently;
%%   * the count cannot be `broker_metrics:connections_current'. That gauge is
%%     decremented by MQTT-SN while a sleeping session's process stays alive,
%%     incremented by MQTT only *after* the handshake completes (the least
%%     accurate instant for a connect storm), and frozen at 0 when metrics are
%%     disabled. Admission has to be right in all three of those cases, so it
%%     keeps its own count.
%%
%% What that count means: a *slot*. One slot is held from the moment a
%% connection is admitted until the process that owns it dies or releases it --
%% including while an MQTT-SN session sleeps, because the timer, the
%% subscriptions and the heap are still this node's resources.
%%
%% Why a pure module and no process: `broker_limits' in this same app is the
%% precedent -- a `persistent_term' answers from the caller's process, with no
%% supervisor child and no message round-trip. That also means there is no
%% `gen_server:call', so R-LB5's ban on unbounded blocking cannot be violated
%% here by construction.
%%
%% == Why there are two data structures and not one ==
%%
%% An atomics counter alone is not enough, and the reason is a bug this module
%% had before it was tested: `terminate/2' is not a reliable release point. MQTT
%% connections are `temporary, brutal_kill' supervisor children, so a connection
%% killed by its supervisor (or by the OOM killer) never runs `terminate' at all
%% -- its slot would be lost for the lifetime of the node, and a gate that leaks
%% slots is worse than no gate, because the node ratchets down to zero capacity
%% and looks "at capacity" forever.
%%
%% So the atomics counter is the *authority* (it is what makes concurrent
%% admission exact) and a public ETS set is the *ledger*: one row per live lease,
%% keyed by a unique integer and carrying the owning pid. When a reservation is
%% refused, the ledger is swept for owners that are no longer alive and their
%% slots are reclaimed. That keeps the fast path (a single `compare_exchange')
%% free of any bookkeeping, and confines the O(n) work to the case where
%% accuracy is actually in doubt. A slot whose owner is alive is never touched,
%% so a sweep cannot steal capacity from a connection that is merely quiet.
%%
%% A rejection is not a metric: `broker_metrics' keys are a frozen set (ADR-016,
%% policed by `keys_are_stable'), and D7 marks rejected-connection counts a S12
%% non-deliverable. Each front end logs its own refusals.
-module(broker_admission).

-export([init/0, try_acquire/0, release/1, adopt/1, snapshot/0, reset/0]).
-export([limit/0, in_use/0]).

-export_type([lease/0, rejection/0]).

%% The atomics reference and the ledger's table id. Written only by `init/0' --
%% never at runtime, because rewriting a persistent_term triggers a global GC
%% pause (ADR-016 §16.5). The limit is deliberately NOT cached here; see
%% `limit/0'.
-define(PT, {?MODULE, state}).

%% Slot 1 is the in-use count. One slot is enough: admission is a single
%% compare_exchange on that number, so there is no per-protocol breakdown to keep
%% in sync (and none is asked for).
-define(IN_USE, 1).

-type rejection() :: over_capacity | draining | not_started.
-type limit() :: non_neg_integer() | infinity.

%% The shapes a caller must hand back to `release/1' unchanged:
%%   * `{leased, Key}' holds a slot, ledger row `Key';
%%   * `unlimited' means the gate is configured off and nothing was taken;
%%   * `{rejected, Why}' took nothing;
%%   * `undefined' means this front end never asked -- so its single teardown
%%     path can release unconditionally.
-type lease() :: {leased, pos_integer()} | unlimited | {rejected, rejection()}
               | undefined.

%% @doc Create the counter and the ledger. Called by
%% `broker_security_app:start/2' -- which runs in the application master, so the
%% ledger lives exactly as long as the application and dies with it.
-spec init() -> ok.
init() ->
    Ref = atomics:new(1, [{signed, true}]),
    Tab = ets:new(broker_admission_leases,
                  [public, set, {read_concurrency, true},
                   {write_concurrency, true}]),
    persistent_term:put(?PT, #{ref => Ref, table => Tab}),
    ok.

%% @doc Ask for one slot.
%%
%% The check is a single atomic compare_exchange, so N processes arriving at once
%% cannot all see the same stale value and slip through together -- the failure
%% mode of a read-then-write gate, and precisely the situation `max_connections'
%% is meant for. There is no sleep and no hand-off: D7 says an over-capacity
%% connection is refused, not queued, so a rejection is final.
%%
%% `draining' is checked *before* the slot, because a node being removed must not
%% accept work even when it has room. It is read from `broker_drain''s
%% persistent_term mirror rather than by calling `broker_drain': this module lives
%% in `broker_security', and depending on `broker_admin' would point the
%% dependency graph back on itself. A missing key means no drain has ever run,
%% which is `up'.
-spec try_acquire() -> lease().
try_acquire() ->
    case drain_state() of
        up -> acquire(limit());
        _Draining -> {rejected, draining}
    end.

%% @doc Give one slot back, by lease. Safe to call twice, and safe to call from a
%% different process than the one that acquired it (a listener that owns a session
%% may tear it down from its own process).
%%
%% Idempotency comes from the ledger, not from a clamp: the row is removed
%% atomically by `ets:take/2', so whichever of "release" and "sweep" gets there
%% first decrements, and the second one finds nothing and does nothing. That is
%% what makes a double release harmless instead of double-counting.
-spec release(lease()) -> ok.
release({leased, Key}) ->
    case state() of
        #{ref := R, table := Tab} ->
            case take_slot(Tab, Key) of
                {ok, _Owner} -> _ = atomics:sub(R, ?IN_USE, 1), ok;
                gone -> ok
            end;
        undefined ->
            ok
    end;
release(unlimited) ->
    %% The gate was disabled when this connection was admitted; it took no slot,
    %% so releasing one here would steal somebody else's.
    ok;
release(undefined) ->
    %% This front end never asked. Releasing nothing keeps its teardown
    %% unconditional, which is what makes a single `after`/`terminate` correct.
    ok;
release({rejected, _}) ->
    ok.

%% @doc Point a lease at the calling process.
%%
%% Only the QUIC front end needs this: the lease is taken by the listener (the
%% only place a refusal can still be answered) and the connection lives in a
%% separate process created afterwards. Re-homing the row to the process that
%% actually owns the resource is what makes the sweep able to reclaim it if that
%% process is killed.
-spec adopt(lease()) -> lease().
adopt({leased, Key} = Lease) ->
    case state() of
        #{table := Tab} -> _ = rehome_slot(Tab, Key, self()), Lease;
        undefined -> Lease
    end;
adopt(Lease) ->
    Lease.

%% @doc What the gate currently sees, for the admin API and for tests.
-spec snapshot() -> #{limit := limit(), in_use := non_neg_integer(),
                      state := up | draining | down | not_started}.
snapshot() ->
    State = case state() of
                undefined -> not_started;
                #{ref := _} -> drain_state()
            end,
    #{limit => limit(), in_use => in_use(), state => State}.

%% @doc Drop the counter and the ledger back to empty. Test-only: production
%% never resets, because a gate that forgets what it admitted would over-admit.
-spec reset() -> ok.
reset() ->
    case state() of
        #{ref := R, table := Tab} ->
            _ = atomics:put(R, ?IN_USE, 0),
            _ = clear_slots(Tab),
            ok;
        undefined ->
            ok
    end.

%% @doc The configured slot limit, read fresh on every call.
%%
%% Deliberately not cached in the `persistent_term' at boot: `broker_limits'
%% reads its bounds per call, and its docstring promises that the admin API
%% reports exactly what is enforced. A cached copy would make a `sys.config' edit
%% silently disagree with `snapshot/0' until the next restart.
%%
%% `infinity' is the default and disables the gate -- turning a node that had no
%% node-wide cap into one that does is an operator's decision, not an upgrade's
%% side effect. See the spike doc §7.
-spec limit() -> limit().
limit() ->
    case maps:get(max_connections, broker_limits:limits(), infinity) of
        infinity ->
            infinity;
        N when is_integer(N), N >= 0 ->
            N;
        Other ->
            erlang:error({invalid_limit, max_connections, Other})
    end.

%% @doc Slots currently taken, as the counter sees them.
%%
%% This does not sweep -- it answers "what is being enforced right now", and a
%% sweep is reserved for the moment a refusal makes the question load-bearing.
-spec in_use() -> non_neg_integer().
in_use() ->
    case state() of
        #{ref := R} -> max(0, atomics:get(R, ?IN_USE));
        undefined -> 0
    end.

%%%_* Internal --------------------------------------------------------------

%% `infinity' short-circuits to a counter-free lease so a disabled gate has no
%% hot-path atomic at all, and so `in_use/0' stays honest about what is enforced
%% (0 taken slots, because nothing is being capped).
acquire(infinity) ->
    case state() of
        undefined -> {rejected, not_started};
        #{ref := _} -> unlimited
    end;
acquire(Limit) ->
    case state() of
        undefined ->
            {rejected, not_started};
        #{ref := R, table := Tab} ->
            case reserve(R, Tab, Limit) of
                {rejected, over_capacity} = Refused ->
                    %% Only now is it worth paying for a sweep: the node is
                    %% about to refuse a peer, and the common cause of a full
                    %% counter that should not be full is a killed owner.
                    case prune(R, Tab) of
                        0 -> Refused;
                        _Reclaimed -> reserve(R, Tab, Limit)
                    end;
                Lease ->
                    Lease
            end
    end.

%% Take one slot: reserve the counter, then record the owner. The order matters
%% -- the counter is what other processes race against, and the ledger row must
%% never exist without the count behind it (a row without a count would make the
%% sweep decrement a slot somebody else holds).
reserve(R, Tab, Limit) ->
    case take_counter(R, Limit) of
        {ok, Slot} ->
            Key = erlang:unique_integer([positive, monotonic]),
            case put_slot(Tab, Key, Slot) of
                ok ->
                    {leased, Key};
                gone ->
                    %% The ledger disappeared between `state/0' and here (an
                    %% application restart, or a test that re-initialised). Give
                    %% the counter back rather than leak it.
                    _ = atomics:sub(R, ?IN_USE, 1),
                    {rejected, not_started}
            end;
        {rejected, _} = Refused ->
            Refused
    end.

%% The compare_exchange loop. The only reason it repeats is another connection
%% winning the same slot, and each repetition is one atomic op; the limit is
%% re-tested on every pass, which is what keeps `in_use =< limit' true under any
%% interleaving.
take_counter(R, Limit) ->
    Current = atomics:get(R, ?IN_USE),
    case Current >= Limit of
        true ->
            {rejected, over_capacity};
        false ->
            case atomics:compare_exchange(R, ?IN_USE, Current, Current + 1) of
                ok -> {ok, Current + 1};
                _Moved -> take_counter(R, Limit)
            end
    end.

%% Sweep the ledger for owners that no longer exist. Returns how many slots were
%% reclaimed, so the caller only retries when the sweep actually changed
%% something (otherwise a genuine "full node" would cost two passes).
%%
%% Correctness rests on two facts about Erlang pids: a live node never reuses a
%% local pid, so a dead owner cannot be confused with a new one; and `ets:take/2'
%% is atomic, so two sweeps racing over the same dead row decrement once between
%% them.
prune(R, Tab) ->
    prune(R, Tab, all_slots(Tab), 0).

prune(_R, _Tab, [], Reclaimed) ->
    Reclaimed;
prune(R, Tab, [{Key, {Pid, _Slot}} | Rest], Reclaimed) ->
    case owner_alive(Pid) of
        true ->
            prune(R, Tab, Rest, Reclaimed);
        false ->
            case take_slot(Tab, Key) of
                {ok, _} ->
                    _ = atomics:sub(R, ?IN_USE, 1),
                    prune(R, Tab, Rest, Reclaimed + 1);
                gone ->
                    prune(R, Tab, Rest, Reclaimed)
            end
    end.

owner_alive(Pid) when is_pid(Pid) ->
    node(Pid) =:= node() andalso erlang:is_process_alive(Pid);
owner_alive(_Other) ->
    false.

%%%_* ETS access helpers ------------------------------------------------
%%
%% Every one of these tolerates the table being gone. That is not defensive
%% padding: `init/0' may run in a process that later dies (a test case, in
%% particular), taking its anonymous table with it, and the module must answer
%% "not started" rather than raise `badarg' out of a connection process.

state() ->
    case persistent_term:get(?PT, undefined) of
        #{ref := _, table := _} = State -> State;
        _ -> undefined
    end.

put_slot(Tab, Key, Slot) ->
    try ets:insert(Tab, {Key, {self(), Slot}}) of
        true -> ok
    catch
        error:badarg -> gone
    end.

take_slot(Tab, Key) ->
    try ets:take(Tab, Key) of
        [{Key, Owner}] -> {ok, Owner};
        [] -> gone
    catch
        error:badarg -> gone
    end.

rehome_slot(Tab, Key, Pid) ->
    try ets:lookup(Tab, Key) of
        [{Key, {_Old, Slot}}] ->
            _ = ets:insert(Tab, {Key, {Pid, Slot}}),
            ok;
        [] ->
            gone
    catch
        error:badarg -> gone
    end.

all_slots(Tab) ->
    try ets:tab2list(Tab) of
        Rows -> Rows
    catch
        error:badarg -> []
    end.

clear_slots(Tab) ->
    try ets:delete_all_objects(Tab) of
        true -> ok
    catch
        error:badarg -> ok
    end.

drain_state() ->
    case persistent_term:get({broker_drain, state}, up) of
        up -> up;
        draining -> draining;
        down -> down;
        _Other -> up
    end.
