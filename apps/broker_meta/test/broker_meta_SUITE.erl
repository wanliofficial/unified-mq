%% @doc S4 control plane (ADR-021 §D1): the metadata tables, password
%% hashing, config version/notification/reconciliation and the outbox.
%%
%% The suite also pins the ADR-002 boundary mechanically: the table list is
%% exactly the frozen set and no message/cursor table exists here. ADR-026 §D3
%% amended that frozen set with exactly one table, `agent_card'; the assertion
%% below is the place that notices if a second one appears.
-module(broker_meta_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    [schema_has_the_frozen_tables,
     tenant_crud,
     user_password_is_hashed_and_authenticates,
     authenticate_is_not_a_user_oracle,
     acl_rules_are_read_per_tenant,
     partition_and_node_metadata,
     agent_card_crud_bumps_version,
     agent_card_registration_is_serialized,
     version_bump_broadcasts,
     reconcile_notices_a_direct_table_write,
     outbox_stays_pending_without_a_publisher,
     outbox_publishes_and_marks_rows].

%%%_* Fixtures --------------------------------------------------------------

init_per_suite(Config) ->
    _ = application:load(broker_meta),
    ok = application:set_env(broker_meta, table_storage, ram_copies),
    ok = application:set_env(broker_meta, nodes, []),
    ok = application:set_env(broker_meta, reconcile_ms, 50),
    ok = application:set_env(broker_meta, outbox_interval_ms, 0),
    ok = application:set_env(broker_meta, outbox_publisher, none),
    broker_meta:reset(),
    {ok, _} = application:ensure_all_started(broker_meta),
    Config.

end_per_suite(_Config) ->
    _ = application:stop(broker_meta),
    _ = application:stop(mnesia),
    _ = mnesia:delete_schema([node()]),
    ok.

init_per_testcase(_Case, Config) ->
    %% Restart the app so the config server re-reads a known version (0) and
    %% the outbox timer starts from this case's environment.
    _ = application:stop(broker_meta),
    [ok = broker_meta:clear(T) || {T, _} <- broker_meta:tables()],
    ok = broker_meta:put(config_version, {config_version, global, 0, 0}),
    ok = application:set_env(broker_meta, outbox_publisher, none),
    {ok, _} = application:ensure_all_started(broker_meta),
    Config.

end_per_testcase(_Case, _Config) ->
    _ = persistent_term:erase({?MODULE, sink}),
    ok.

%%%_* Schema ----------------------------------------------------------------

schema_has_the_frozen_tables(_Config) ->
    Names = [T || {T, _} <- broker_meta:tables()],
    ?assertEqual([tenant, broker_user, acl_rule, topic_meta, queue_meta,
                  binding_meta, subscription_meta, cluster_node,
                  partition_meta, config_version, config_outbox,
                  agent_card], Names),
    %% ADR-002 boundary: no message body / ack / cursor table.
    lists:foreach(
      fun(Forbidden) ->
              ?assertNot(lists:member(Forbidden, Names))
      end, [message, cursor, ack, inflight, payload]),
    %% Every table really exists after boot.
    lists:foreach(
      fun(T) -> ?assertEqual(T, mnesia:table_info(T, name)) end, Names),
    ok.

%%%_* Rows ------------------------------------------------------------------

tenant_crud(_Config) ->
    ok = broker_meta:put_tenant(<<"t1">>),
    {ok, {tenant, <<"t1">>, <<"t1">>, active, _}} =
        broker_meta:get_tenant(<<"t1">>),
    not_found = broker_meta:get_tenant(<<"missing">>),
    ok = broker_meta:delete(tenant, <<"t1">>),
    not_found = broker_meta:get_tenant(<<"t1">>),
    ok.

user_password_is_hashed_and_authenticates(_Config) ->
    ok = broker_meta:put_user(<<"alice">>, <<"t1">>, <<"s3cret">>),
    {ok, {broker_user, <<"alice">>, <<"t1">>, Hash, Salt, []}} =
        broker_meta:get_user(<<"alice">>),
    %% Not plaintext, not trivially reversible, and salted.
    ?assertNotEqual(<<"s3cret">>, Hash),
    ?assertEqual(32, byte_size(Hash)),
    ?assertEqual(16, byte_size(Salt)),
    {ok, <<"t1">>} = broker_meta:authenticate(<<"alice">>, <<"s3cret">>),
    {error, bad_username_or_password} =
        broker_meta:authenticate(<<"alice">>, <<"wrong">>),
    ok.

authenticate_is_not_a_user_oracle(_Config) ->
    ok = broker_meta:put_user(<<"bob">>, <<"t2">>, <<"pw">>),
    %% Unknown user and wrong password are indistinguishable.
    {error, bad_username_or_password} =
        broker_meta:authenticate(<<"nobody">>, <<"pw">>),
    {error, bad_username_or_password} =
        broker_meta:authenticate(<<"bob">>, <<"nope">>),
    ok.

acl_rules_are_read_per_tenant(_Config) ->
    ok = broker_meta:put_acl(#{id => a1, tenant_id => <<"t1">>,
                               principal => <<"alice">>,
                               action => publish, resource => <<"orders/#">>,
                               effect => allow}),
    ok = broker_meta:put_acl(#{id => a2, tenant_id => <<"t2">>,
                               principal => <<"bob">>,
                               action => subscribe, resource => <<"#">>,
                               effect => deny}),
    [R1] = broker_meta:acls(<<"t1">>),
    {acl_rule, a1, <<"t1">>, <<"alice">>, publish, <<"orders/#">>, allow} = R1,
    [R2] = broker_meta:acls(<<"t2">>),
    {acl_rule, a2, <<"t2">>, <<"bob">>, subscribe, <<"#">>, deny} = R2,
    ok.

partition_and_node_metadata(_Config) ->
    ok = broker_meta:put_partition(
           #{partition_id => 3, owner => 'a@host', epoch => 7,
             replicas => ['a@host', 'b@host', 'c@host'], state => active}),
    {ok, {partition_meta, 3, 'a@host', 7, ['a@host', 'b@host', 'c@host'],
          active}} = broker_meta:get_partition(3),
    ok = broker_meta:put_node('a@host', voter),
    [Node] = broker_meta:nodes(),
    {cluster_node, 'a@host', voter, up, _} = Node,
    ok.

%% Agent Cards (ADR-026 §D3): a monotonic per-agent version drives the ETag,
%% so re-registering the same payload still changes the version. The row is
%% tenant-keyed and tenant-filterable.
agent_card_crud_bumps_version(_Config) ->
    Card = #{name => <<"echo">>, url => <<"https://example.test/a2a">>},
    {ok, 1} = broker_meta:put_agent_card(
                #{tenant_id => <<"t1">>, agent_id => <<"echo">>, card => Card}),
    {ok, {agent_card, {<<"t1">>, <<"echo">>}, <<"t1">>, 1, active, Card,
          undefined, _}} = broker_meta:get_agent_card(<<"t1">>, <<"echo">>),
    %% Same payload, new version: the ETag must move even on an idempotent upsert.
    {ok, 2} = broker_meta:put_agent_card(
                #{tenant_id => <<"t1">>, agent_id => <<"echo">>, card => Card}),
    {ok, {agent_card, _, _, 2, active, _, _, _}} =
        broker_meta:get_agent_card(<<"t1">>, <<"echo">>),
    %% A disabled card keeps its version lineage but flips status.
    {ok, 3} = broker_meta:put_agent_card(
                #{tenant_id => <<"t1">>, agent_id => <<"echo">>, card => Card,
                  status => disabled}),
    {ok, {agent_card, _, _, 3, disabled, _, _, _}} =
        broker_meta:get_agent_card(<<"t1">>, <<"echo">>),
    %% A different tenant's identically named agent is a different row.
    {ok, 1} = broker_meta:put_agent_card(
                #{tenant_id => <<"t2">>, agent_id => <<"echo">>, card => Card}),
    [Only] = broker_meta:agent_cards(<<"t1">>),
    {agent_card, {<<"t1">>, <<"echo">>}, <<"t1">>, 3, _, _, _, _} = Only,
    ok = broker_meta:delete_agent_card(<<"t2">>, <<"echo">>),
    not_found = broker_meta:get_agent_card(<<"t2">>, <<"echo">>),
    %% Malformed input is rejected, not stored.
    {error, {invalid_agent_card, _}} = broker_meta:put_agent_card(#{foo => bar}),
    ok.

%% 10 concurrent registrations must produce versions 1..10 with no gaps and no
%% duplicates: the write lock inside the transaction is what makes this hold.
agent_card_registration_is_serialized(_Config) ->
    Self = self(),
    N = 10,
    [spawn_link(fun() ->
                        {ok, V} = broker_meta:put_agent_card(
                                    #{tenant_id => <<"t1">>, agent_id => <<"race">>,
                                      card => #{}}),
                        Self ! {version, V}
                end) || _ <- lists:seq(1, N)],
    Versions = [receive {version, V} -> V after 5000 -> ct:fail(timeout) end
                || _ <- lists:seq(1, N)],
    ?assertEqual(lists:seq(1, N), lists:sort(Versions)),
    {ok, {agent_card, _, _, N, _, _, _, _}} =
        broker_meta:get_agent_card(<<"t1">>, <<"race">>),
    ok.

%%%_* Version, notification, reconciliation --------------------------------

version_bump_broadcasts(_Config) ->
    {ok, Ref} = broker_meta_config:subscribe(),
    {ok, 1} = broker_meta:bump_version(tenant_added),
    receive
        {broker_config, 1, tenant_added} -> ok
    after 1000 ->
        ct:fail(no_notification)
    end,
    ?assertEqual(1, broker_meta:version()),
    ?assertEqual(1, broker_meta_config:cached_version()),
    ok = broker_meta_config:unsubscribe(Ref),
    {ok, 2} = broker_meta:bump_version(other),
    receive
        {broker_config, 2, other} -> ct:fail(unsubscribed_process_still_notified)
    after 100 ->
        ok
    end.

reconcile_notices_a_direct_table_write(_Config) ->
    {ok, Ref} = broker_meta_config:subscribe(),
    %% An operator (or another node) writing the table directly bypasses bump/1.
    ok = broker_meta:put(config_version, {config_version, global, 42, 0}),
    ok = broker_meta_config:reconcile_now(),
    receive
        {broker_config, 42, reconcile} -> ok
    after 1000 ->
        ct:fail(no_reconcile_notification)
    end,
    ?assertEqual(42, broker_meta_config:cached_version()),
    ok = broker_meta_config:unsubscribe(Ref).

%%%_* Outbox ---------------------------------------------------------------

outbox_stays_pending_without_a_publisher(_Config) ->
    ok = broker_meta:enqueue(#{what => tenant_added}, 1),
    [Row] = broker_meta:pending_outbox(),
    {config_outbox, _, 1, #{what := tenant_added}, _, undefined} = Row,
    {ok, #{published := 0, pending := 1, reason := no_publisher}} =
        broker_meta_outbox:drain(),
    %% Still pending: "no publisher" is not "delivered".
    [_] = broker_meta:pending_outbox(),
    ok.

outbox_publishes_and_marks_rows(_Config) ->
    persistent_term:put({?MODULE, sink}, self()),
    ok = application:set_env(broker_meta, outbox_publisher,
                             {?MODULE, publish}),
    %% Restart so the outbox reads the publisher from env.
    _ = application:stop(broker_meta),
    {ok, _} = application:ensure_all_started(broker_meta),
    ok = broker_meta:enqueue(#{what => first}, 1),
    ok = broker_meta:enqueue(#{what => second}, 2),
    {ok, #{published := 2, pending := 0}} = broker_meta_outbox:drain(),
    receive {published, #{what := first}} -> ok after 1000 -> ct:fail(no_first) end,
    receive {published, #{what := second}} -> ok after 1000 -> ct:fail(no_second) end,
    [] = broker_meta:pending_outbox(),
    %% Rows stay for audit, with a published timestamp.
    All = broker_meta:all(config_outbox),
    2 = length(All),
    true = lists:all(fun(R) -> element(6, R) =/= undefined end, All),
    ok.

publish(Event) ->
    case persistent_term:get({?MODULE, sink}, undefined) of
        undefined -> {error, no_sink};
        Pid -> Pid ! {published, Event}, ok
    end.
