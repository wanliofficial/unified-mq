%% @doc T3: contract tests for broker_router_hash.
%% Wildcard cases come from MQTT 3.1.1 §4.7 as frozen in ADR-010 §10.3 and its
%% 验证 section; route_reads_are_served_without_the_writer is the mechanical
%% check of §10.1【F3】(no process serialization on the read path).
-module(broker_router_hash_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("broker_core/include/broker_message.hrl").
-include_lib("broker_router/include/broker_router.hrl").

-define(STORE, broker_router_hash_store).
-define(HASH, broker_router_hash).

all() ->
    [init_yields_env_and_rejects_bad_config,
     exact_filter_matches_only_its_topic,
     single_level_wildcard_matches_one_level,
     multi_level_wildcard_matches_parent_and_below,
     hash_matches_everything,
     plus_matches_a_single_level_topic,
     illegal_filters_are_rejected_at_register,
     partition_is_deterministic_and_in_range,
     route_lists_every_matching_subref_once,
     unregister_removes_the_subscription,
     register_twice_for_one_subref_is_rejected,
     unmatched_topic_returns_empty_sub_list,
     route_reads_are_served_without_the_writer,
     on_partition_change_stays_unimplemented_in_s1].

init_per_testcase(_Case, Config) ->
    {ok, Env} = ?HASH:init(#{partitions => 4}),
    [{env, Env} | Config].

end_per_testcase(_Case, _Config) ->
    catch gen_server:stop(?STORE, normal, 1000),
    ok.

%%%_* Tests --------------------------------------------------------------

init_yields_env_and_rejects_bad_config(Config) ->
    Env = env(Config),
    true = is_tuple(Env) orelse is_map(Env),
    %% 0 partitions would make the modulo crash on the first publish, and a
    %% non-integer would be a config typo caught the same way.
    {error, _} = ?HASH:init(#{partitions => 0}),
    {error, _} = ?HASH:init(#{partitions => <<"four">>}),
    %% A rejected init must not have touched the store the suite is using.
    {ok, [{_, []}]} = ?HASH:route(<<"still/alive">>, ctx(), Env),
    ok.

exact_filter_matches_only_its_topic(Config) ->
    Env = env(Config),
    ok = ?HASH:register(Env, <<"sport/tennis/player1">>, 1, #{}),
    {ok, [{_, [1]}]} = ?HASH:route(<<"sport/tennis/player1">>, ctx(), Env),
    {ok, [{_, []}]} = ?HASH:route(<<"sport/tennis/player2">>, ctx(), Env),
    %% A filter does not match a deeper topic than it describes.
    {ok, [{_, []}]} = ?HASH:route(<<"sport/tennis/player1/ranking">>, ctx(), Env),
    ok.

single_level_wildcard_matches_one_level(Config) ->
    Env = env(Config),
    ok = ?HASH:register(Env, <<"sport/tennis/+">>, 1, #{}),
    {ok, [{_, [1]}]} = ?HASH:route(<<"sport/tennis/player1">>, ctx(), Env),
    {ok, [{_, []}]} = ?HASH:route(<<"sport/tennis">>, ctx(), Env),
    {ok, [{_, []}]} = ?HASH:route(<<"sport/tennis/player1/ranking">>, ctx(), Env),
    %% + consumes exactly one level, so it cannot match a partial level name.
    ok = ?HASH:register(Env, <<"sport/+">>, 2, #{}),
    {ok, [{_, [2]}]} = ?HASH:route(<<"sport/tennis">>, ctx(), Env),
    {ok, [{_, []}]} = ?HASH:route(<<"sportx/tennis">>, ctx(), Env),
    ok.

multi_level_wildcard_matches_parent_and_below(Config) ->
    Env = env(Config),
    ok = ?HASH:register(Env, <<"sport/#">>, 1, #{}),
    {ok, [{_, [1]}]} = ?HASH:route(<<"sport">>, ctx(), Env),
    {ok, [{_, [1]}]} = ?HASH:route(<<"sport/tennis">>, ctx(), Env),
    {ok, [{_, [1]}]} = ?HASH:route(<<"sport/tennis/player1/ranking">>, ctx(), Env),
    {ok, [{_, []}]} = ?HASH:route(<<"other/tennis">>, ctx(), Env),
    ok.

hash_matches_everything(Config) ->
    Env = env(Config),
    ok = ?HASH:register(Env, <<"#">>, 1, #{}),
    {ok, [{_, [1]}]} = ?HASH:route(<<"a">>, ctx(), Env),
    {ok, [{_, [1]}]} = ?HASH:route(<<"a/b/c/d/e">>, ctx(), Env),
    ok.

plus_matches_a_single_level_topic(Config) ->
    Env = env(Config),
    ok = ?HASH:register(Env, <<"+/+">>, 1, #{}),
    {ok, [{_, [1]}]} = ?HASH:route(<<"a/b">>, ctx(), Env),
    {ok, [{_, []}]} = ?HASH:route(<<"a">>, ctx(), Env),
    {ok, [{_, []}]} = ?HASH:route(<<"a/b/c">>, ctx(), Env),
    ok = ?HASH:register(Env, <<"+">>, 2, #{}),
    {ok, [{_, [2]}]} = ?HASH:route(<<"a">>, ctx(), Env),
    ok.

%% §4.7: + and # must occupy a whole level, and # must be the last level.
illegal_filters_are_rejected_at_register(Config) ->
    Env = env(Config),
    Illegal = [<<"sport#">>,
               <<"sport/#/ranking">>,
               <<"#/sport">>,
               <<"sport+">>,
               <<"ten+nis/player1">>,
               <<>>],
    lists:foreach(
        fun(F) ->
            {error, {invalid_filter, F}} = ?HASH:register(Env, F, 1, #{})
        end,
        Illegal),
    {ok, [{_, []}]} = ?HASH:route(<<"sport#">>, ctx(), Env),
    ok.

partition_is_deterministic_and_in_range(Config) ->
    Env = env(Config),
    Dest = <<"sport/tennis/player1">>,
    {ok, [{P, []}]} = ?HASH:route(Dest, ctx(), Env),
    {ok, [{P, []}]} = ?HASH:route(Dest, ctx(), Env),
    %% 0-based partition ids, matching Kafka/Pulsar convention; ADR-010 §10.3
    %% only freezes "pure integer", so the base is fixed here once.
    true = P >= 0 andalso P < 4,
    lists:foreach(
        fun(I) ->
            {ok, [{Pi, []}]} = ?HASH:route(topic(I), ctx(), Env),
            true = Pi >= 0 andalso Pi < 4
        end,
        lists:seq(1, 200)),
    %% Routing is a function of the destination only: the tenant in Ctx must
    %% not change the partition, or per-sub cursors would disagree across
    %% publishers on the same topic.
    {ok, [{P2, []}]} = ?HASH:route(Dest, ctx(<<"other-tenant">>), Env),
    P2 = P,
    ok.

%% ADR-010 验证 (T13), completeness half: one message hitting several
%% subscriptions returns all of them, each exactly once, in registration order.
%% Merging overlapping subscriptions that belong to the same client is NOT
%% possible here -- it would require dereferencing SubRef, which §10.2 forbids.
route_lists_every_matching_subref_once(Config) ->
    Env = env(Config),
    ok = ?HASH:register(Env, <<"sport/tennis/player1">>, 7, #{}),
    ok = ?HASH:register(Env, <<"sport/tennis/+">>, 8, #{}),
    ok = ?HASH:register(Env, <<"sport/#">>, 9, #{}),
    {ok, [{_P, Subs}]} = ?HASH:route(<<"sport/tennis/player1">>, ctx(), Env),
    %% Three overlapping filters match this topic; all three subs are reported.
    [7, 8, 9] = Subs,
    Subs = lists:usort(Subs),
    ok.

unregister_removes_the_subscription(Config) ->
    Env = env(Config),
    ok = ?HASH:register(Env, <<"sport/#">>, 1, #{}),
    ok = ?HASH:register(Env, <<"sport/#">>, 2, #{}),
    {ok, [{_, [1, 2]}]} = ?HASH:route(<<"sport/a">>, ctx(), Env),
    ok = ?HASH:unregister(Env, 1),
    {ok, [{_, [2]}]} = ?HASH:route(<<"sport/a">>, ctx(), Env),
    ok = ?HASH:unregister(Env, 2),
    {ok, [{_, []}]} = ?HASH:route(<<"sport/a">>, ctx(), Env),
    %% Unknown SubRef is an error, not a silent ok: a no-op would hide a
    %% subscription/router lifetime mismatch.
    {error, {not_found, 1}} = ?HASH:unregister(Env, 1),
    ok.

register_twice_for_one_subref_is_rejected(Config) ->
    Env = env(Config),
    ok = ?HASH:register(Env, <<"a/b">>, 1, #{}),
    {error, {already_registered, 1}} = ?HASH:register(Env, <<"a/+">>, 1, #{}),
    {ok, [{_, [1]}]} = ?HASH:route(<<"a/b">>, ctx(), Env),
    ok.

unmatched_topic_returns_empty_sub_list(Config) ->
    Env = env(Config),
    %% The partition is still reported: the publisher appends the message even
    %% with no subscriber (§5.1 (c) -> (d)).
    {ok, [{_P, []}]} = ?HASH:route(<<"nobody/listens/here">>, ctx(), Env),
    ok.

%% §10.1【F3】: route/3 reads concurrently in the caller; only index writes go
%% through one process. Blocking the writer must leave reads working and writes
%% blocked, otherwise Router is the global bottleneck README §7.2 forbids.
route_reads_are_served_without_the_writer(Config) ->
    Env = env(Config),
    ok = ?HASH:register(Env, <<"sport/#">>, 1, #{}),
    Parent = self(),
    sys:suspend(?STORE),
    Helper =
        spawn(fun() ->
                  ok = ?HASH:register(Env, <<"blocked/topic">>, 99, #{}),
                  Parent ! {helper, self(), returned}
              end),
    try
        {ok, [{_P, [1]}]} = ?HASH:route(<<"sport/tennis">>, ctx(), Env),
        false = helper_returned_in(300, Helper),
        true = is_process_alive(Helper),
        ok
    after
        sys:resume(?STORE),
        true = helper_returned_in(2000, Helper)
    end.

%% §10.5: S1 leaves the partition-change hook unimplemented; the behavior marks
%% it optional so S4 can add it without touching a frozen signature.
on_partition_change_stays_unimplemented_in_s1(_Config) ->
    Cbs = broker_router:behaviour_info(callbacks),
    Opts = broker_router:behaviour_info(optional_callbacks),
    %% behaviour_info(callbacks) lists every callback, optional ones included;
    %% optional_callbacks marks the subset an implementation may omit.
    [{init, 1}, {on_partition_change, 2}, {register, 4}, {route, 3}, {unregister, 2}] =
        lists:sort(Cbs),
    [{on_partition_change, 2}] = Opts,
    false = lists:member({on_partition_change, 2}, ?HASH:module_info(exports)),
    ok.

%%%_* Helpers ------------------------------------------------------------

env(Config) ->
    proplists:get_value(env, Config).

ctx() ->
    #route_ctx{tenant_id = <<"t">>, publisher = undefined, reliability = os_buffer}.

ctx(Tenant) ->
    #route_ctx{tenant_id = Tenant, publisher = undefined, reliability = os_buffer}.

topic(I) ->
    iolist_to_binary(["sport/tennis/player", integer_to_list(I)]).

helper_returned_in(Ms, Helper) ->
    receive
        {helper, Helper, returned} -> true
    after Ms -> false
    end.
