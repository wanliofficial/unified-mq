%% Unit tests for the will_server generation guard (§3.1.3.2.2 over a reconnect
%% race). The integration-level cases in mqtt_conn_SUITE cover "a reconnect
%% cancels a parked Will"; these cover the specific race the generation token
%% exists for -- a dying connection whose Will is scheduled on a generation that a
%% takeover has already superseded, so `will_due' must drop it instead of firing.
-module(mqtt_will_server_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([adopt_bumps_the_generation/1,
         schedule_parks_under_the_current_generation/1,
         a_superseded_will_is_dropped_at_due/1,
         a_current_will_fires_at_due/1,
         cancel_drops_a_parked_will/1,
         a_schedule_from_a_stale_generation_is_refused/1]).

-define(WILL, #{topic => <<"wd/gen">>, payload => <<"bye">>,
               qos => 1, retain => 0}).

all() ->
    [adopt_bumps_the_generation,
     schedule_parks_under_the_current_generation,
     a_superseded_will_is_dropped_at_due,
     a_current_will_fires_at_due,
     cancel_drops_a_parked_will,
     a_schedule_from_a_stale_generation_is_refused].

init_per_suite(Config) ->
    _ = application:load(broker_storage),
    _ = application:load(broker_router),
    _ = application:load(broker_queue),
    _ = application:load(broker_subscription),
    _ = application:load(broker_retain),
    _ = application:load(broker_proto_mqtt),
    Dir = filename:join([?config(priv_dir, Config), "will_gen", "data"]),
    _ = filelib:ensure_dir(filename:join([Dir, "x"])),
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok = application:set_env(broker_storage, data_dir, Dir),
    ok = application:set_env(broker_storage, wal, #{require_durable_fs => false}),
    ok = application:set_env(broker_queue, partitions, 2),
    ok = application:set_env(broker_router, partitions, 2),
    ok = application:set_env(broker_subscription, data_dir, Dir),
    ok = application:set_env(broker_retain, data_dir, Dir),
    ok = application:set_env(broker_subscription, cursor_max_regress, 64),
    ok = application:set_env(broker_subscription, cursor_flush_interval_ms, 20),
    ok = application:set_env(broker_subscription, inflight_flush_interval_ms, 20),
    ok = application:set_env(broker_event, policy, pass_through),
    {ok, _} = application:ensure_all_started(broker_storage),
    [begin
         case broker_queue:create_partition(P) of
             ok -> ok;
             {error, {already_exists, _}} -> ok;
             E -> exit({create_partition, P, E})
         end
     end || P <- broker_queue:partition_ids()],
    {ok, _} = application:ensure_all_started(broker_proto_mqtt),
    Config.

end_per_suite(_Config) ->
    _ = application:stop(broker_proto_mqtt),
    _ = application:stop(broker_retain),
    _ = application:stop(broker_subscription),
    _ = application:stop(broker_queue),
    _ = application:stop(broker_router),
    _ = application:stop(broker_storage),
    ok.

%% Record the current will_published count as this case's baseline, so a
%% concurrent suite's Will traffic cannot make the guard look like it leaked.
mark_will_baseline() ->
    put(will_baseline, maps:get(will_published, broker_metrics:snapshot(), 0)),
    ok.

will_fired_since_baseline() ->
    Now = maps:get(will_published, broker_metrics:snapshot(), 0),
    Now > get(will_baseline).

%% adopt/1 returns a fresh, increasing generation for the ClientID and clears any
%% parked Will the previous holder left.
adopt_bumps_the_generation(_Config) ->
    Id = unique_id(),
    1 = mqtt_will_server:adopt(Id),
    2 = mqtt_will_server:adopt(Id),
    3 = mqtt_will_server:adopt(Id),
    ok.

%% A Will scheduled under the current generation parks and fires when due.
a_current_will_fires_at_due(_Config) ->
    Id = unique_id(),
    Gen = mqtt_will_server:adopt(Id),
    ok = mark_will_baseline(),
    ok = mqtt_will_server:schedule(Id, ?WILL, 50, <<"t0">>, undefined, Gen),
    wait_until(fun() -> will_fired_since_baseline() end, 2000),
    ok.

%% The generation guard: schedule a Will on generation G, then adopt again (G+1),
%% then let the original timer come due. The Will must NOT fire -- it belongs to a
%% generation the ClientID has moved past.
a_superseded_will_is_dropped_at_due(_Config) ->
    Id = unique_id(),
    ok = mark_will_baseline(),
    %% Simulate the takeover race: the new connection adopts first (bumping the
    %% generation to 1), then the old connection's delayed schedule arrives with
    %% the stale generation it captured before the takeover.
    Gen = mqtt_will_server:adopt(Id),
    ?assertEqual(1, Gen),
    %% A reconnect outruns the dying connection: bump the generation again.
    Gen1 = mqtt_will_server:adopt(Id),
    ?assertEqual(2, Gen1),
    %% Now the old connection's schedule arrives carrying the stale generation.
    %% It must be rejected on sight.
    ok = mqtt_will_server:schedule(Id, ?WILL, 80, <<"t0">>, undefined, Gen),
    ?assertNot(will_fired_since_baseline()),
    timer:sleep(300),
    ?assertNot(will_fired_since_baseline()),
    ok.

%% A schedule carrying a stale generation is dropped on sight, never parked.
a_schedule_from_a_stale_generation_is_refused(_Config) ->
    Id = unique_id(),
    ok = mark_will_baseline(),
    Gen = mqtt_will_server:adopt(Id),
    %% The holder moves on; a late schedule from the old generation arrives now.
    _NewGen = mqtt_will_server:adopt(Id),
    ok = mqtt_will_server:schedule(Id, ?WILL, 100, <<"t0">>, undefined, Gen),
    ?assertNot(will_fired_since_baseline()),
    timer:sleep(300),
    ?assertNot(will_fired_since_baseline()),
    ok.

%% cancel/1 clears a parked Will whatever its generation.
cancel_drops_a_parked_will(_Config) ->
    Id = unique_id(),
    ok = mark_will_baseline(),
    Gen = mqtt_will_server:adopt(Id),
    ok = mqtt_will_server:schedule(Id, ?WILL, 100, <<"t0">>, undefined, Gen),
    ok = mqtt_will_server:cancel(Id),
    ?assertNot(will_fired_since_baseline()),
    timer:sleep(300),
    ?assertNot(will_fired_since_baseline()),
    ok.

%% schedule_parks_under_the_current_generation: a sanity counterpart proving the
%% happy path is generation-aware -- the parked Will fires (so the drop tests
%% above are about generation, not about a broken timer).
schedule_parks_under_the_current_generation(_Config) ->
    Id = unique_id(),
    Gen = mqtt_will_server:adopt(Id),
    ok = mark_will_baseline(),
    ok = mqtt_will_server:schedule(Id, ?WILL, 50, <<"t0">>, undefined, Gen),
    wait_until(fun() -> will_fired_since_baseline() end, 2000),
    ok.

%%%_* Helpers -----------------------------------------------------------------

unique_id() ->
    list_to_binary(["wd-", integer_to_binary(erlang:unique_integer([positive]))]).

%% Observe the Will through the broker's will_published metric: it only moves when
%% mqtt_will:publish/3 actually runs, which is exactly the event the guard gates.
%% `will_fired_since_baseline/0' (above) compares against this case's marked
%% baseline so concurrent suites' Will traffic cannot leak into the verdict.

wait_until(_Pred, Budget) when Budget =< 0 ->
    error(timeout);
wait_until(Pred, Budget) ->
    case Pred() of
        true -> ok;
        false -> timer:sleep(25), wait_until(Pred, Budget - 25)
    end.
