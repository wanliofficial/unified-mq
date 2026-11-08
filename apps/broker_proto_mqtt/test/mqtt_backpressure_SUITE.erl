%% @doc S2-G: the pure backpressure decisions (design §4.7).
%%
%% These cases are the boundary the connection FSM and the WAL writer share:
%% "pause this pull round" and "refuse this publish" are decided from numbers
%% alone, so they can be pinned without a socket, a writer or a metrics app.
-module(mqtt_backpressure_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").

all() ->
    [consumer_is_ok_below_both_limits,
     consumer_checks_the_mailbox_first,
     consumer_pauses_on_a_full_inflight_window,
     a_zero_mailbox_limit_disables_only_the_mailbox_check,
     a_zero_inflight_limit_delivers_nothing,
     producer_is_ok_below_the_limit,
     producer_rejects_at_the_limit,
     a_zero_producer_limit_is_disabled].

consumer_is_ok_below_both_limits(_Config) ->
    ok = mqtt_backpressure:consumer(0, 0, #{mailbox => 10, inflight => 10}),
    ok = mqtt_backpressure:consumer(9, 9, #{mailbox => 10, inflight => 10}).

consumer_checks_the_mailbox_first(_Config) ->
    %% Both over: the mailbox wins. The reason is logged, so the order is part
    %% of the contract, not an implementation detail.
    {pause, mailbox} =
        mqtt_backpressure:consumer(10, 10, #{mailbox => 10, inflight => 10}).

consumer_pauses_on_a_full_inflight_window(_Config) ->
    {pause, inflight} =
        mqtt_backpressure:consumer(0, 64, #{mailbox => 1000, inflight => 64}).

a_zero_mailbox_limit_disables_only_the_mailbox_check(_Config) ->
    %% `0' is "no mailbox bound", but the in-flight window still applies: a
    %% disabled mailbox must not turn into "deliver without bound".
    ok = mqtt_backpressure:consumer(100000, 0, #{mailbox => 0, inflight => 64}),
    {pause, inflight} =
        mqtt_backpressure:consumer(100000, 64, #{mailbox => 0, inflight => 64}).

a_zero_inflight_limit_delivers_nothing(_Config) ->
    %% The window is a size, not a switch: 0 has always meant "no deliveries"
    %% in the FSM, and the decision function must agree or the two disagree at
    %% runtime.
    {pause, inflight} =
        mqtt_backpressure:consumer(0, 0, #{mailbox => 0, inflight => 0}).

producer_is_ok_below_the_limit(_Config) ->
    ok = mqtt_backpressure:producer(0, 10),
    ok = mqtt_backpressure:producer(9, 10).

producer_rejects_at_the_limit(_Config) ->
    {reject, mailbox} = mqtt_backpressure:producer(10, 10),
    {reject, mailbox} = mqtt_backpressure:producer(11, 10).

a_zero_producer_limit_is_disabled(_Config) ->
    ok = mqtt_backpressure:producer(100000, 0).
