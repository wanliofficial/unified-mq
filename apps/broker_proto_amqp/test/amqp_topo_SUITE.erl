%% @doc Unit tests for `amqp_topo': exchange/queue declaration, bindings,
%% and the topic matching logic.
%%
%% The module is pure ETS state (no processes, no sockets), so these tests
%% start/stop the gen_server around each test case to get a clean ETS table.
-module(amqp_topo_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [default_exchange_routes_by_queue_name,
     declare_exchange_is_idempotent,
     declare_exchange_type_mismatch_fails,
     declare_queue_auto_generates_name,
     declare_queue_durable_mismatch_fails,
     bind_requires_existing_queue_and_exchange,
     unbind_removes_routing,
     direct_exchange_exact_match_only,
     fanout_exchange_ignores_routing_key,
     topic_exchange_star_matches_one_word,
     topic_exchange_hash_matches_zero_or_more,
     delete_exchange_cascades_bindings,
     delete_queue_cascades_bindings,
     exchange_type_query,
     queue_exists_query].

suite() -> [{timetrap, {seconds, 10}}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    %% `amqp_topo' is pure ETS state. It only needs `broker_core' for
    %% `broker_core_encoding:base64url/1' in the auto-generated name path.
    %% No need for the full broker stack.
    ok = ensure_started(broker_core),
    {ok, Pid} = amqp_topo:start_link(),
    unlink(Pid),
    [{topo_pid, Pid} | Config].

end_per_testcase(_TestCase, _Config) ->
    case whereis(amqp_topo) of
        undefined -> ok;
        Pid -> exit(Pid, shutdown)
    end,
    ok.

ensure_started(App) ->
    case application:ensure_all_started(App) of
        {ok, _} -> ok;
        {error, {already_started, App}} -> ok;
        {error, Reason} -> error({cannot_start, App, Reason})
    end.

%% Default exchange (empty name) routes by queue name
default_exchange_routes_by_queue_name(_Config) ->
    {ok, _} = amqp_topo:declare_queue(<<"test.q">>, true, false),
    ?assertEqual([<<"test.q">>], amqp_topo:queues_for(<<>>, <<"test.q">>)),
    ?assertEqual([], amqp_topo:queues_for(<<>>, <<"other">>)).

%% Declaring the same exchange twice with the same type is idempotent
declare_exchange_is_idempotent(_Config) ->
    ok = amqp_topo:declare_exchange(<<"myex">>, <<"topic">>, true),
    ?assertMatch({ok, topic}, amqp_topo:declare_exchange(<<"myex">>, <<"topic">>, true)).

%% Declaring with a different type fails
declare_exchange_type_mismatch_fails(_Config) ->
    ok = amqp_topo:declare_exchange(<<"myex">>, <<"topic">>, true),
    ?assertMatch({error, {type_mismatch, topic, direct}},
                 amqp_topo:declare_exchange(<<"myex">>, <<"direct">>, true)).

%% Auto-generated queue names
declare_queue_auto_generates_name(_Config) ->
    {ok, Name1} = amqp_topo:declare_queue(<<>>, true, false),
    {ok, Name2} = amqp_topo:declare_queue(<<>>, true, false),
    ?assertMatch({0, 8}, binary:match(Name1, <<"amq.gen-">>)),
    ?assertMatch({0, 8}, binary:match(Name2, <<"amq.gen-">>)),
    ?assertNotEqual(Name1, Name2).

%% Declaring with a different durable flag fails
declare_queue_durable_mismatch_fails(_Config) ->
    {ok, _} = amqp_topo:declare_queue(<<"myq">>, true, false),
    ?assertMatch({error, {precondition_failed, durable_mismatch}},
                 amqp_topo:declare_queue(<<"myq">>, false, false)).

%% Binding requires both queue and exchange to exist
bind_requires_existing_queue_and_exchange(_Config) ->
    ?assertMatch({error, no_queue},
                 amqp_topo:bind(<<"ex">>, <<"noqueue">>, <<"key">>, #{})),
    ok = amqp_topo:declare_exchange(<<"ex">>, <<"direct">>, true),
    {ok, _} = amqp_topo:declare_queue(<<"q1">>, true, false),
    ?assertMatch({error, no_exchange},
                 amqp_topo:bind(<<"noex">>, <<"q1">>, <<"key">>, #{})),
    ?assertEqual(ok, amqp_topo:bind(<<"ex">>, <<"q1">>, <<"key">>, #{})).

%% Unbind removes routing
unbind_removes_routing(_Config) ->
    ok = amqp_topo:declare_exchange(<<"ex">>, <<"direct">>, true),
    {ok, _} = amqp_topo:declare_queue(<<"q1">>, true, false),
    ok = amqp_topo:bind(<<"ex">>, <<"q1">>, <<"key">>, #{}),
    ?assertEqual([<<"q1">>], amqp_topo:queues_for(<<"ex">>, <<"key">>)),
    ok = amqp_topo:unbind(<<"ex">>, <<"q1">>, <<"key">>, #{}),
    ?assertEqual([], amqp_topo:queues_for(<<"ex">>, <<"key">>)).

%% Direct exchange: exact routing key match only
direct_exchange_exact_match_only(_Config) ->
    ok = amqp_topo:declare_exchange(<<"dex">>, <<"direct">>, true),
    {ok, _} = amqp_topo:declare_queue(<<"q1">>, true, false),
    ok = amqp_topo:bind(<<"dex">>, <<"q1">>, <<"exact.key">>, #{}),
    ?assertEqual([<<"q1">>], amqp_topo:queues_for(<<"dex">>, <<"exact.key">>)),
    ?assertEqual([], amqp_topo:queues_for(<<"dex">>, <<"different">>)).

%% Fanout exchange: all bound queues get the message regardless of routing key
fanout_exchange_ignores_routing_key(_Config) ->
    ok = amqp_topo:declare_exchange(<<"fex">>, <<"fanout">>, true),
    {ok, _} = amqp_topo:declare_queue(<<"q1">>, true, false),
    {ok, _} = amqp_topo:declare_queue(<<"q2">>, true, false),
    ok = amqp_topo:bind(<<"fex">>, <<"q1">>, <<"anykey">>, #{}),
    ok = amqp_topo:bind(<<"fex">>, <<"q2">>, <<"anotherkey">>, #{}),
    ?assertEqual([<<"q1">>, <<"q2">>],
                 amqp_topo:queues_for(<<"fex">>, <<"anykey">>)),
    ?assertEqual([<<"q1">>, <<"q2">>],
                 amqp_topo:queues_for(<<"fex">>, <<"completely.different">>)).

%% Topic exchange: * matches exactly one word
topic_exchange_star_matches_one_word(_Config) ->
    ok = amqp_topo:declare_exchange(<<"tex">>, <<"topic">>, true),
    {ok, _} = amqp_topo:declare_queue(<<"q1">>, true, false),
    ok = amqp_topo:bind(<<"tex">>, <<"q1">>, <<"logs.*.error">>, #{}),
    ?assertEqual([<<"q1">>], amqp_topo:queues_for(<<"tex">>, <<"logs.api.error">>)),
    ?assertEqual([<<"q1">>], amqp_topo:queues_for(<<"tex">>, <<"logs.db.error">>)),
    ?assertEqual([], amqp_topo:queues_for(<<"tex">>, <<"logs.api.warning">>)).

%% Topic exchange: # matches zero or more words
topic_exchange_hash_matches_zero_or_more(_Config) ->
    ok = amqp_topo:declare_exchange(<<"tex">>, <<"topic">>, true),
    {ok, _} = amqp_topo:declare_queue(<<"q1">>, true, false),
    ok = amqp_topo:bind(<<"tex">>, <<"q1">>, <<"logs.#">>, #{}),
    ?assertEqual([<<"q1">>], amqp_topo:queues_for(<<"tex">>, <<"logs">>)),
    ?assertEqual([<<"q1">>], amqp_topo:queues_for(<<"tex">>, <<"logs.api">>)),
    ?assertEqual([<<"q1">>], amqp_topo:queues_for(<<"tex">>, <<"logs.api.error">>)).

%% Deleting an exchange also deletes its bindings
delete_exchange_cascades_bindings(_Config) ->
    ok = amqp_topo:declare_exchange(<<"ex">>, <<"direct">>, true),
    {ok, _} = amqp_topo:declare_queue(<<"q1">>, true, false),
    ok = amqp_topo:bind(<<"ex">>, <<"q1">>, <<"key">>, #{}),
    ?assertEqual([<<"q1">>], amqp_topo:queues_for(<<"ex">>, <<"key">>)),
    ok = amqp_topo:delete_exchange(<<"ex">>),
    ?assertEqual([], amqp_topo:queues_for(<<"ex">>, <<"key">>)).

%% Deleting a queue also deletes its bindings
delete_queue_cascades_bindings(_Config) ->
    ok = amqp_topo:declare_exchange(<<"ex">>, <<"direct">>, true),
    {ok, _} = amqp_topo:declare_queue(<<"q1">>, true, false),
    ok = amqp_topo:bind(<<"ex">>, <<"q1">>, <<"key">>, #{}),
    ?assertEqual([<<"q1">>], amqp_topo:queues_for(<<"ex">>, <<"key">>)),
    ok = amqp_topo:delete_queue(<<"q1">>),
    ?assertEqual([], amqp_topo:queues_for(<<"ex">>, <<"key">>)).

%% Query exchange type
exchange_type_query(_Config) ->
    ok = amqp_topo:declare_exchange(<<"tex">>, <<"topic">>, true),
    ?assertEqual({ok, topic}, amqp_topo:exchange_type(<<"tex">>)),
    ?assertEqual({error, no_exchange}, amqp_topo:exchange_type(<<"noex">>)).

%% Query queue existence
queue_exists_query(_Config) ->
    {ok, _} = amqp_topo:declare_queue(<<"exists">>, true, false),
    ?assert(amqp_topo:queue_exists(<<"exists">>)),
    ?assertNot(amqp_topo:queue_exists(<<"missing">>)).
