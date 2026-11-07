%% @doc One-line periodic metrics summary (ADR-016 §16.4).
%%
%% Started only when `report_interval_ms > 0'. It reads `snapshot/0' (pure,
%% no hop) and logs a single line; it never mutates metrics and never owns
%% state another module needs.
-module(broker_metrics_server).
-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(st, {interval_ms :: pos_integer(),
             timer :: reference() | undefined}).

start_link(IntervalMs) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, IntervalMs, []).

init(IntervalMs) ->
    Timer = erlang:send_after(IntervalMs, self(), report),
    {ok, #st{interval_ms = IntervalMs, timer = Timer}}.

handle_call(_Req, _From, St) ->
    {reply, {error, unsupported}, St}.

handle_cast(_Msg, St) ->
    {noreply, St}.

handle_info(report, #st{interval_ms = Ms} = St) ->
    log_line(broker_metrics:snapshot()),
    Timer = erlang:send_after(Ms, self(), report),
    {noreply, St#st{timer = Timer}};
handle_info(_Other, St) ->
    {noreply, St}.

terminate(_Reason, #st{timer = undefined}) ->
    ok;
terminate(_Reason, #st{timer = Timer}) ->
    _ = erlang:cancel_timer(Timer),
    ok.

%%%_* Formatting -----------------------------------------------------------

log_line(S) ->
    P99 = broker_metrics:p99(maps:get(publish_latency_us, S)),
    logger:info(
      "broker_metrics: connections=~p peak=~p publish=~p deliver=~p "
      "ack=~p auth_failures=~p protocol_errors=~p wal_sync=~p "
      "queue_depth=~p publish_p99_us=~p",
      [maps:get(connections_current, S),
       maps:get(connections_peak, S),
       maps:get(publish_total, S),
       maps:get(deliver_total, S),
       maps:get(ack_total, S),
       maps:get(auth_failures, S),
       maps:get(protocol_errors, S),
       maps:get(wal_sync_total, S),
       maps:get(queue_depth, S),
       P99]).
