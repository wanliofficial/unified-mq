%% @doc S2-A (ADR-016): the metrics surface must be boring and stable.
%%
%% The suite pins the things a metrics layer can get wrong without anyone
%% noticing: the key set drifting, buckets being off by one, a "disabled"
%% deployment still paying for the write, unknown keys silently creating new
%% metrics, and the report loop logging more than one line per tick.
-module(broker_metrics_SUITE).

-compile([export_all, nowarn_export_all]).

-behaviour(logger_handler).
-export([log/2]).

all() ->
    [keys_are_stable,
     inc_accumulates,
     gauge_overwrites,
     gauge_add_accumulates,
     observe_buckets,
     snapshot_is_pure_read,
     unknown_key_is_rejected,
     wrong_kind_is_rejected,
     disabled_is_noop,
     reset_clears,
     no_high_cardinality_keys,
     report_line_is_single].

-define(KEYS,
        [connections_current, connections_peak, auth_failures,
         protocol_errors, frame_decode_errors, publish_total, deliver_total,
         ack_total, bytes_in, bytes_out, publish_latency_us,
         deliver_latency_us, ack_latency_us, wal_sync_latency_us,
         queue_depth, subscription_backlog, cursor_lag, redelivery_total,
         dlq_size, wal_append_total, wal_sync_total, segment_bytes,
         recovery_time_us, retention_deleted_bytes,
         retention_deleted_segments, retention_blocked_by_slow_consumer,
         retention_blocked_by_committed, cloudevents_validate_ok,
         cloudevents_validate_failed, retain_updates, retain_clears,
         retain_overflow, retain_store_errors, will_published,
         qos2_state_overflow, shared_claims, shared_ack_unowned,
         shared_window_full, shared_claim_rollbacks, failover_promotions,
         dlq_appends, dlq_append_failed, ttl_expired_total,
         conn_mailbox_len, conn_inflight, wal_pending_append,
         backpressure_pauses, backpressure_disconnects]).

init_per_suite(Config) ->
    %% No report loop during the suite: the single-line test starts its own.
    Saved = application:get_env(broker_metrics, report_interval_ms, undefined),
    ok = application:set_env(broker_metrics, report_interval_ms, 0),
    {ok, _} = application:ensure_all_started(broker_metrics),
    ok = broker_metrics:reset(),
    [{saved_interval, Saved} | Config].

end_per_suite(Config) ->
    %% J10 lesson: restore env if init_per_suite exported it.
    case proplists:get_value(saved_interval, Config) of
        undefined -> ok;
        Saved -> ok = application:set_env(broker_metrics, report_interval_ms, Saved)
    end,
    _ = application:stop(broker_metrics),
    ok.

%%%_* The key set ----------------------------------------------------------

keys_are_stable(_Config) ->
    ?KEYS = broker_metrics:keys(),
    %% And the snapshot always carries exactly these keys, even at zero load.
    Snapshot = broker_metrics:snapshot(),
    Sorted = lists:sort(?KEYS),
    Sorted = lists:sort(maps:keys(Snapshot)),
    ok.

no_high_cardinality_keys(_Config) ->
    lists:foreach(
      fun(K) ->
              true = is_atom(K),
              %% No binaries, no tuples, no dynamic suffixes: the ADR's
              %% high-cardinality ban is enforced at the type level.
              false = is_binary(K)
      end, broker_metrics:keys()),
    ok.

%%%_* Write path -----------------------------------------------------------

inc_accumulates(_Config) ->
    ok = broker_metrics:reset(),
    ok = broker_metrics:inc(publish_total),
    ok = broker_metrics:inc(publish_total, 3),
    4 = maps:get(publish_total, broker_metrics:snapshot()),
    ok.

gauge_overwrites(_Config) ->
    ok = broker_metrics:reset(),
    ok = broker_metrics:gauge(queue_depth, 5),
    5 = maps:get(queue_depth, broker_metrics:snapshot()),
    ok = broker_metrics:gauge(queue_depth, 2),
    2 = maps:get(queue_depth, broker_metrics:snapshot()),
    ok.

gauge_add_accumulates(_Config) ->
    ok = broker_metrics:reset(),
    1 = broker_metrics:gauge_add(connections_current, 1),
    2 = broker_metrics:gauge_add(connections_current, 1),
    1 = broker_metrics:gauge_add(connections_current, -1),
    1 = maps:get(connections_current, broker_metrics:snapshot()),
    0 = broker_metrics:gauge_add(connections_current, -1),
    {error, {wrong_kind, publish_total, counter}} =
        broker_metrics:gauge_add(publish_total, 1),
    {error, {unknown_metric, bogus}} = broker_metrics:gauge_add(bogus, 1),
    ok.

observe_buckets(_Config) ->
    ok = broker_metrics:reset(),
    %% Boundaries: 100, 500, 1000, 5000, ... 1000000, then +Inf.
    ok = broker_metrics:observe(publish_latency_us, 0),      % bucket 0
    ok = broker_metrics:observe(publish_latency_us, 100),    % == boundary, bucket 0
    ok = broker_metrics:observe(publish_latency_us, 101),    % bucket 1
    ok = broker_metrics:observe(publish_latency_us, 1000001),% +Inf
    #{count := 4, sum := 1000202, buckets := Buckets} =
        maps:get(publish_latency_us, broker_metrics:snapshot()),
    {100, 2} = lists:keyfind(100, 1, Buckets),
    {500, 1} = lists:keyfind(500, 1, Buckets),
    {infinity, 1} = lists:keyfind(infinity, 1, Buckets),
    %% p99 of four samples lands in the +Inf bucket, i.e. it is reported as the
    %% last finite boundary (1 s) rather than as an impossible value.
    1000000 = broker_metrics:p99(#{count => 4, sum => 1000202,
                                   buckets => Buckets}),
    ok.

snapshot_is_pure_read(_Config) ->
    ok = broker_metrics:reset(),
    ok = broker_metrics:inc(ack_total, 7),
    S1 = broker_metrics:snapshot(),
    S2 = broker_metrics:snapshot(),
    S1 = S2,
    7 = maps:get(ack_total, S2),
    ok.

unknown_key_is_rejected(_Config) ->
    {error, {unknown_metric, bogus_metric}} =
        broker_metrics:inc(bogus_metric),
    {error, {unknown_metric, bogus_metric}} =
        broker_metrics:gauge(bogus_metric, 1),
    {error, {unknown_metric, bogus_metric}} =
        broker_metrics:observe(bogus_metric, 1),
    %% A rejected key must not appear in the snapshot afterwards.
    false = maps:is_key(bogus_metric, broker_metrics:snapshot()),
    ok.

wrong_kind_is_rejected(_Config) ->
    {error, {wrong_kind, queue_depth, gauge}} =
        broker_metrics:inc(queue_depth),
    {error, {wrong_kind, publish_total, counter}} =
        broker_metrics:gauge(publish_total, 1),
    {error, {wrong_kind, publish_total, counter}} =
        broker_metrics:observe(publish_total, 1),
    {error, {invalid_increment, -1}} = broker_metrics:inc(publish_total, -1),
    ok.

disabled_is_noop(_Config) ->
    ok = application:set_env(broker_metrics, enabled, false),
    ok = broker_metrics:init(),
    ok = broker_metrics:inc(publish_total, 100),
    ok = broker_metrics:gauge(queue_depth, 9),
    ok = broker_metrics:observe(publish_latency_us, 1234),
    0 = broker_metrics:gauge_add(connections_current, 1),
    0 = maps:get(publish_total, broker_metrics:snapshot()),
    0 = maps:get(queue_depth, broker_metrics:snapshot()),
    #{count := 0} = maps:get(publish_latency_us, broker_metrics:snapshot()),
    ok = application:set_env(broker_metrics, enabled, true),
    ok = broker_metrics:init(),
    ok.

reset_clears(_Config) ->
    ok = broker_metrics:inc(publish_total, 5),
    ok = broker_metrics:gauge(queue_depth, 5),
    ok = broker_metrics:observe(publish_latency_us, 5),
    ok = broker_metrics:reset(),
    S = broker_metrics:snapshot(),
    0 = maps:get(publish_total, S),
    0 = maps:get(queue_depth, S),
    #{count := 0, sum := 0} = maps:get(publish_latency_us, S),
    ok.

%%%_* The report loop ------------------------------------------------------

report_line_is_single(_Config) ->
    %% init_per_suite pinned report_interval_ms to 0, so the supervisor has no
    %% reporter child at all -- the "0 means no output" half of the contract.
    undefined = whereis(broker_metrics_server),
    %% The default primary level is `notice', which would drop our info report
    %% before any handler sees it; raise it for the duration of the case.
    Primary = logger:get_primary_config(),
    ok = logger:set_primary_config(level, info),
    ok = logger:add_handler(ct_metrics_capture, ?MODULE,
                            #{config => self(), level => info}),
    try
        {ok, Pid} = broker_metrics_server:start_link(60000),
        unlink(Pid),
        Pid ! report,
        %% Exactly one line per tick...
        receive
            {metrics_log, info, Msg} ->
                Line = format_msg(Msg),
                true = string:find(Line, "broker_metrics:") =/= nomatch
        after 2000 ->
            ct:fail(no_report_line)
        end,
        %% ...and nothing else before the next tick 60 s later.
        receive
            {metrics_log, _Level, _Message} ->
                ct:fail(more_than_one_line_per_tick)
        after 50 ->
            ok
        end,
        exit(Pid, kill)
    after
        _ = logger:remove_handler(ct_metrics_capture),
        _ = logger:set_primary_config(level, maps:get(level, Primary))
    end,
    ok.

%% logger handler callback: forward the formatted report to the test process.
log(#{level := Level, msg := Msg}, #{config := Pid}) ->
    Pid ! {metrics_log, Level, Msg},
    ok.

%% `logger:info(Format, Args)' arrives as msg = {Format, Args}; flatten it so
%% the assertion talks about the line a human would see, not the event shape.
format_msg({Format, Args}) when is_list(Format), is_list(Args) ->
    lists:flatten(io_lib:format(Format, Args));
format_msg(Msg) ->
    lists:flatten(io_lib:format("~p", [Msg])).
