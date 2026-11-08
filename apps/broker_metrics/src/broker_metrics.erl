%% @doc In-process metrics (ADR-016): counters, gauges and fixed-bucket
%% histograms, with **no external dependency** and no process on the hot path.
%%
%% Design choices worth knowing before adding a key:
%%
%%   * Keys are a closed set (`keys/0'). `inc/1' on an unregistered key answers
%%     `{error, {unknown_metric, Key}}' and logs once per process -- it never
%%     silently creates a metric. This is what keeps cardinality finite and
%%     catches typos at the call site instead of in a dashboard.
%%   * Writes are directly `counters:add/3' / `atomics:add/3'. `snapshot/0' is a
%%     pure read in the caller's process, so neither direction takes a hop.
%%   * `enabled = false' (or the app not being started) makes every write an
%%     `ok' no-op. That is deliberate: metrics must never be able to fail a
%%     publish, and the S1/S2 test suites are expected to run with metrics off
%%     and get identical behaviour.
%%   * Histograms use fixed buckets (microseconds by default). `observe/2'
%%     stores count, sum and one slot per bucket including +Inf; the report
%%     loop turns that into an approximate p99 and nothing else.
%%
%% The state is one `persistent_term' map, written only by `init/0' and
%% `reset/0' (test-only). Writing it at runtime would trigger a global GC pause
%% (ADR-016 §16.5), so no code path does that.
-module(broker_metrics).

-export([init/0, inc/1, inc/2, observe/2, gauge/2, gauge_add/2,
         snapshot/0, reset/0, keys/0, p99/1]).

-export_type([key/0, histogram/0]).

-define(PT, {?MODULE, state}).
-define(DEFAULT_BUCKETS, [100, 500, 1000, 5000, 10000, 50000,
                          100000, 500000, 1000000]).

-type key() :: atom().
-type histogram() :: #{count := non_neg_integer(),
                       sum := non_neg_integer(),
                       buckets := [{non_neg_integer() | infinity, non_neg_integer()}]}.

%%%_* Lifecycle ------------------------------------------------------------

%% @doc Create the storage and register the key set. Called by
%% `broker_metrics_app:start/2' before the supervisor, and idempotent.
-spec init() -> ok.
init() ->
    Enabled = application:get_env(broker_metrics, enabled, true),
    Buckets = application:get_env(broker_metrics, histogram_buckets,
                                  ?DEFAULT_BUCKETS),
    ok = validate_buckets(Buckets),
    Specs = key_specs(),
    Counters = [K || {K, counter} <- Specs],
    Gauges = [K || {K, gauge} <- Specs],
    Hists = [K || {K, histogram} <- Specs],
    NC = length(Counters),
    NG = length(Gauges),
    %% count + sum + one slot per bucket (+Inf)
    Width = 2 + length(Buckets) + 1,
    CRef = counters:new(max(NC, 1), [write_concurrency]),
    GRef = atomics:new(max(NG + length(Hists) * Width, 1), [{signed, true}]),
    %% counters and atomics are 1-based; a histogram occupies `Width' slots
    %% starting at its base (count, sum, one per finite bucket, +Inf last).
    KeyMap = maps:from_list(
               [{K, {counter, CRef, I}}
                || {K, I} <- lists:zip(Counters, lists:seq(1, NC))] ++
               [{K, {gauge, GRef, I}}
                || {K, I} <- lists:zip(Gauges, lists:seq(1, NG))] ++
               [{K, {histogram, GRef, NG + 1 + J * Width, Buckets}}
                || {K, J} <- lists:zip(Hists, lists:seq(0, length(Hists) - 1))]),
    persistent_term:put(?PT, #{enabled => Enabled =:= true,
                               keys => KeyMap,
                               order => [K || {K, _} <- Specs],
                               buckets => Buckets,
                               width => Width,
                               counters => CRef,
                               gauges => GRef}),
    ok.

validate_buckets(Buckets) when is_list(Buckets), Buckets =/= [] ->
    case lists:all(fun(B) -> is_integer(B) andalso B > 0 end, Buckets) of
        true -> ok;
        false -> erlang:error({invalid_histogram_buckets, Buckets})
    end;
validate_buckets(Buckets) ->
    erlang:error({invalid_histogram_buckets, Buckets}).

%%%_* Write path -----------------------------------------------------------

-spec inc(key()) -> ok | {error, term()}.
inc(Key) ->
    inc(Key, 1).

-spec inc(key(), non_neg_integer()) -> ok | {error, term()}.
inc(Key, N) when is_integer(N), N >= 0 ->
    with_key(Key,
             fun({counter, Ref, Idx}) ->
                     counters:add(Ref, Idx, N),
                     ok;
                (Spec) ->
                     wrong_kind(Key, Spec)
             end);
inc(_Key, N) ->
    {error, {invalid_increment, N}}.

-spec gauge(key(), integer()) -> ok | {error, term()}.
gauge(Key, Value) when is_integer(Value) ->
    with_key(Key,
             fun({gauge, Ref, Idx}) ->
                     atomics:put(Ref, Idx, Value),
                     ok;
                (Spec) ->
                     wrong_kind(Key, Spec)
             end);
gauge(_Key, Value) ->
    {error, {invalid_gauge_value, Value}}.

%% @doc Add `Delta' to a gauge and answer the new value. Gauges that track a
%% live count (connections_current) need the atomic read-modify-write this
%% provides; `gauge/2' remains the "set an absolute value" operation.
-spec gauge_add(key(), integer()) -> integer() | {error, term()}.
gauge_add(Key, Delta) when is_integer(Delta) ->
    %% Not `with_key/2': the caller wants the new value, so the disabled and
    %% not-started cases answer 0 instead of `ok'.
    case persistent_term:get(?PT, undefined) of
        undefined ->
            0;
        #{enabled := false} ->
            0;
        #{keys := Keys} ->
            case maps:get(Key, Keys, undefined) of
                undefined -> unknown(Key);
                {gauge, Ref, Idx} -> atomics:add_get(Ref, Idx, Delta);
                Spec -> wrong_kind(Key, Spec)
            end
    end;
gauge_add(_Key, Delta) ->
    {error, {invalid_gauge_delta, Delta}}.

-spec observe(key(), non_neg_integer()) -> ok | {error, term()}.
observe(Key, Us) when is_integer(Us), Us >= 0 ->
    with_key(Key,
             fun({histogram, Ref, Base, Buckets}) ->
                     %% count
                     atomics:add(Ref, Base, 1),
                     %% sum
                     atomics:add(Ref, Base + 1, Us),
                     %% first bucket whose boundary >= Us (last slot is +Inf)
                     Bucket = bucket_index(Buckets, Us),
                     atomics:add(Ref, Base + 2 + Bucket, 1),
                     ok;
                (Spec) ->
                     wrong_kind(Key, Spec)
             end);
observe(_Key, Us) ->
    {error, {invalid_observation, Us}}.

bucket_index(Buckets, Us) ->
    bucket_index(Buckets, Us, 0).

bucket_index([], _Us, Acc) ->
    Acc;                                  % +Inf slot
bucket_index([B | Rest], Us, Acc) when Us > B ->
    bucket_index(Rest, Us, Acc + 1);
bucket_index(_Buckets, _Us, Acc) ->
    Acc.

wrong_kind(Key, {Kind, _, _}) ->
    {error, {wrong_kind, Key, Kind}};
wrong_kind(Key, {histogram, _, _, _}) ->
    {error, {wrong_kind, Key, histogram}}.

%%%_* Read path ------------------------------------------------------------

-spec snapshot() -> #{key() => integer() | histogram()}.
snapshot() ->
    case persistent_term:get(?PT, undefined) of
        undefined ->
            zeros();
        #{keys := Keys, order := Order} ->
            maps:from_list([{K, read(maps:get(K, Keys))} || K <- Order])
    end.

read({counter, Ref, Idx}) ->
    counters:get(Ref, Idx);
read({gauge, Ref, Idx}) ->
    atomics:get(Ref, Idx);
read({histogram, Ref, Base, Buckets}) ->
    Count = atomics:get(Ref, Base),
    Sum = atomics:get(Ref, Base + 1),
    BucketsOut =
        [{B, atomics:get(Ref, Base + 2 + I)}
         || {I, B} <- lists:zip(lists:seq(0, length(Buckets) - 1), Buckets)] ++
        [{infinity, atomics:get(Ref, Base + 2 + length(Buckets))}],
    #{count => Count, sum => Sum, buckets => BucketsOut}.

zeros() ->
    Buckets = application:get_env(broker_metrics, histogram_buckets,
                                  ?DEFAULT_BUCKETS),
    maps:from_list(
      [{K, case Kind of
               histogram -> #{count => 0, sum => 0,
                              buckets => [{B, 0} || B <- Buckets] ++
                                         [{infinity, 0}]};
               _ -> 0
           end}
       || {K, Kind} <- key_specs()]).

%% @doc Approximate 99th percentile from a histogram, in the same unit as
%% `observe/2' (microseconds by default). 0 when nothing was observed; when the
%% target falls in the +Inf slot it answers the last finite boundary, which is
%% the most the buckets can honestly say.
-spec p99(histogram()) -> non_neg_integer().
p99(#{count := 0}) ->
    0;
p99(#{count := N, buckets := Buckets}) ->
    Target = max(1, (N * 99 + 99) div 100),
    pick(Buckets, Target, 0, 0).

pick([{infinity, _} | _], _Target, _Acc, Last) ->
    Last;
pick([{Boundary, Count} | Rest], Target, Acc, _Last) ->
    case Acc + Count >= Target of
        true -> Boundary;
        false -> pick(Rest, Target, Acc + Count, Boundary)
    end;
pick([], _Target, _Acc, Last) ->
    Last.

-spec reset() -> ok.
reset() ->
    case persistent_term:get(?PT, undefined) of
        undefined ->
            ok;
        #{keys := Keys, width := Width} ->
            maps:foreach(
              fun(_K, {counter, Ref, Idx}) ->
                      counters:put(Ref, Idx, 0);
                 (_K, {gauge, Ref, Idx}) ->
                      atomics:put(Ref, Idx, 0);
                 (_K, {histogram, Ref, Base, _Buckets}) ->
                      lists:foreach(
                        fun(Off) -> atomics:put(Ref, Base + Off, 0) end,
                        lists:seq(0, Width - 1))
              end, Keys),
            ok
    end.

-spec keys() -> [key()].
keys() ->
    [K || {K, _} <- key_specs()].

%%%_* Internals ------------------------------------------------------------

with_key(Key, Fun) ->
    case persistent_term:get(?PT, undefined) of
        undefined ->
            ok;                            % not started: metrics are optional
        #{enabled := false} ->
            ok;                            % explicitly disabled
        #{keys := Keys} ->
            case maps:get(Key, Keys, undefined) of
                undefined -> unknown(Key);
                Spec -> Fun(Spec)
            end
    end.

unknown(Key) ->
    case get({?MODULE, warned, Key}) of
        true ->
            ok;
        _ ->
            put({?MODULE, warned, Key}, true),
            logger:warning("broker_metrics: unknown metric ~p", [Key])
    end,
    {error, {unknown_metric, Key}}.

%% @doc The closed key set (ADR-016 §16.2). Changing this list requires
%% changing `keys_are_stable' in broker_metrics_SUITE and the architecture
%% document, which is the point.
key_specs() ->
    [{connections_current, gauge},
     {connections_peak, gauge},
     {auth_failures, counter},
     {protocol_errors, counter},
     {frame_decode_errors, counter},
     {publish_total, counter},
     {deliver_total, counter},
     {ack_total, counter},
     {bytes_in, counter},
     {bytes_out, counter},
     {publish_latency_us, histogram},
     {deliver_latency_us, histogram},
     {ack_latency_us, histogram},
     {wal_sync_latency_us, histogram},
     {queue_depth, gauge},
     {subscription_backlog, gauge},
     {cursor_lag, gauge},
     {redelivery_total, counter},
     {dlq_size, gauge},
     {wal_append_total, counter},
     {wal_sync_total, counter},
     {segment_bytes, gauge},
     {recovery_time_us, histogram},
     {retention_deleted_bytes, counter},
     {retention_deleted_segments, counter},
     {retention_blocked_by_slow_consumer, counter},
     {retention_blocked_by_committed, counter},
     {cloudevents_validate_ok, counter},
     {cloudevents_validate_failed, counter},
     %% S3 (ADR-020 §D7/§D8): duplicate events dropped inside a configured
     %% window, and rejected events written to an isolation destination.
     {cloudevents_duplicates, counter},
     {cloudevents_quarantined, counter},
     %% S2-C (ADR-018 §18.5): retained state, Will publication and QoS 2
     %% state-cap violations. Same rule as every other key -- a closed atom
     %% set, no labels, no per-topic suffixes.
     {retain_updates, counter},
     {retain_clears, counter},
     {retain_overflow, counter},
     {retain_store_errors, counter},
     {will_published, counter},
     {qos2_state_overflow, counter},
     %% S2-D (ADR-019 §19.1/§19.2): shared-page claims, acks that named an
     %% offset the consumer does not hold, refusals when a partition's
     %% `acked ∪ assigned' window is full, and failover actives promoted by a
     %% death. Same closed-set rule as every other key.
     {shared_claims, counter},
     {shared_ack_unowned, counter},
     {shared_window_full, counter},
     {shared_claim_rollbacks, counter},
     {failover_promotions, counter},
     %% S2-E (ADR-015 §15.2/§15.3/§15.4): dead-letter appends (and the ones that
     %% failed and must be retried), plus messages skipped because `expires_at'
     %% had passed. `redelivery_total' and `dlq_size' already existed; the
     %% former counts every retry the worker scheduled, the latter the copies
     %% currently retained (S2-E only grows it -- S2-F decrements on GC).
     {dlq_appends, counter},
     {dlq_append_failed, counter},
     {ttl_expired_total, counter},
     %% S2-G (design §4.7, D5): the backpressure signals and the two actions.
     %% The three gauges are maintained as cross-process SUMS (each process adds
     %% only the delta it caused), because a bare `gauge/2' would be overwritten
     %% by whichever process wrote last and say nothing. They are diagnostic
     %% watermarks, not billing-grade: a `kill'ed process cannot report its
     %% `terminate/2' decrement (execution plan §2).
     {conn_mailbox_len, gauge},
     {conn_inflight, gauge},
     %% S2.5 (ADR-017 §17.3): an inbound packet over the configured cap, and an
     %% outbound PUBLISH the peer's Maximum Packet Size forbids. Both are drops,
     %% not protocol errors: the first disconnects, the second leaves the
     %% delivery to the redelivery path.
     {mqtt_packet_too_large, counter},
     {mqtt_publish_dropped_oversize, counter},
     {wal_pending_append, gauge},
     {backpressure_pauses, counter},
     {backpressure_disconnects, counter},
     %% S4 (ADR-021 §D3~§D5): ownership changes committed by the Metadata Raft
     %% group, appends refused by epoch fencing, replication attempts that
     %% failed to reach a majority, and metadata writes rejected because the
     %% group had no quorum.
     {cluster_owner_changes, counter},
     {cluster_fencing_rejected, counter},
     {cluster_replication_failed, counter},
     {cluster_no_quorum, counter}].
