%% @doc T-C4: the retained-message store (ADR-018 §18.2).
%%
%% The suite is about the two things the conn FSM relies on and cannot test in
%% isolation:
%%   * the snapshot is on disk before a `sync' put answers (the PUBACK contract);
%%   * the caps refuse the mutation instead of evicting something older.
%%
%% Every case gets its own `data_dir', so a snapshot from one case can never be
%% read by the next -- the trap the first S2-C run fell into with the repo's
%% shared `data/retain'.
-module(broker_retain_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("broker_core/include/broker_message.hrl").

all() ->
    [put_is_visible_and_survives_a_restart,
     overwrite_keeps_one_entry_per_topic,
     delete_removes_the_entry,
     entries_are_sorted_by_topic,
     zero_length_payload_is_stored_by_the_retain_app,
     sync_put_is_on_disk_before_it_answers,
     async_put_lands_after_the_flush_window,
     flush_writes_a_dirty_tree,
     overflow_by_count_refuses_the_new_topic,
     overflow_by_bytes_refuses_the_update,
     overflow_by_payload_refuses_the_update,
     overwriting_an_existing_topic_is_not_an_overflow,
     corrupt_snapshot_refuses_to_boot,
     unsupported_vsn_refuses_to_boot,
     absent_snapshot_boots_empty,
     boot_loads_the_previous_tree,
     parse_rejects_garbage].

%%%_* Fixtures ----------------------------------------------------------------

init_per_suite(Config) ->
    _ = application:load(broker_metrics),
    _ = application:load(broker_retain),
    {ok, _} = application:ensure_all_started(broker_metrics),
    Config.

end_per_suite(_Config) ->
    _ = application:stop(broker_retain),
    _ = application:stop(broker_metrics),
    ok.

init_per_testcase(Case, Config) ->
    stop(),
    DataDir = filename:join([?config(priv_dir, Config), atom_to_list(Case), "data"]),
    ok = filelib:ensure_dir(filename:join([DataDir, "x"])),
    ok = application:set_env(broker_retain, data_dir, DataDir),
    ok = application:set_env(broker_retain, retain_flush_interval_ms, 100),
    ok = application:set_env(broker_retain, max_retained, 10000),
    ok = application:set_env(broker_retain, max_retained_bytes, 67108864),
    ok = application:set_env(broker_retain, max_payload_bytes, 262144),
    boot(),
    [{data_dir, DataDir} | Config].

end_per_testcase(_Case, _Config) ->
    stop(),
    ok.

boot() ->
    {ok, _} = application:ensure_all_started(broker_retain),
    ok.

stop() ->
    _ = application:stop(broker_retain),
    ok.

%%%_* Cases -------------------------------------------------------------------

put_is_visible_and_survives_a_restart(_Config) ->
    ok = broker_retain:put(msg(<<"a/b">>, <<"v1">>), sync),
    {ok, #broker_message{payload = <<"v1">>}} = broker_retain:lookup(<<"a/b">>),
    #{count := 1} = broker_retain:stats(),
    %% A real restart of the app: the tree must come back from the file alone.
    stop(),
    boot(),
    {ok, #broker_message{destination = <<"a/b">>, payload = <<"v1">>}} =
        broker_retain:lookup(<<"a/b">>),
    #{count := 1, dirty := false} = broker_retain:stats(),
    ok.

overwrite_keeps_one_entry_per_topic(_Config) ->
    ok = broker_retain:put(msg(<<"a/b">>, <<"v1">>), sync),
    ok = broker_retain:put(msg(<<"a/b">>, <<"v2-and-longer">>), sync),
    {ok, #broker_message{payload = <<"v2-and-longer">>}} =
        broker_retain:lookup(<<"a/b">>),
    #{count := 1} = broker_retain:stats(),
    %% The byte gauge tracks the replacement, not the sum of both.
    #{bytes := Bytes} = broker_retain:stats(),
    ?assertEqual(byte_size(<<"a/b">>) + byte_size(<<"v2-and-longer">>), Bytes),
    ok.

delete_removes_the_entry(_Config) ->
    ok = broker_retain:put(msg(<<"a/b">>, <<"v1">>), sync),
    ok = broker_retain:delete(<<"a/b">>, sync),
    error = broker_retain:lookup(<<"a/b">>),
    #{count := 0, bytes := 0} = broker_retain:stats(),
    %% Deleting an absent topic is a no-op, not an error: a zero-length retained
    %% publish for a topic nobody retained is legal (§3.3.1.3).
    ok = broker_retain:delete(<<"never/was">>, sync),
    stop(),
    boot(),
    error = broker_retain:lookup(<<"a/b">>),
    ok.

entries_are_sorted_by_topic(_Config) ->
    Topics = [<<"b/2">>, <<"a/1">>, <<"c/3">>, <<"a/0">>],
    [ok = broker_retain:put(msg(T, <<"x">>), sync) || T <- Topics],
    Got = [T || {T, _} <- broker_retain:entries()],
    ?assertEqual(lists:sort(Topics), Got),
    ok.

zero_length_payload_is_stored_by_the_retain_app(_Config) ->
    %% The app is semantics-free about payload length: clearing a topic is one
    %% `delete/2' and the conn is what decides which call to make (§3.3.1.3).
    %% `put/2' with an empty payload must simply not be the conn's path, so
    %% storing it here is asserted as the app's own behaviour, not as a promise.
    ok = broker_retain:put(msg(<<"a/b">>, <<>>), sync),
    {ok, #broker_message{payload = <<>>}} = broker_retain:lookup(<<"a/b">>),
    ok = broker_retain:delete(<<"a/b">>, sync),
    error = broker_retain:lookup(<<"a/b">>),
    ok.

%% The whole reason `put/2' has a mode: a QoS 1 retained publish is acked only
%% after the snapshot is durable (ADR-007 + ADR-018 §18.2). Reading the file
%% synchronously in the same process proves the ack was not sent early.
sync_put_is_on_disk_before_it_answers(_Config) ->
    ok = broker_retain:put(msg(<<"a/b">>, <<"durable">>), sync),
    {ok, Entries} = broker_retain_files:read(),
    ?assertEqual([{<<"a/b">>, <<"durable">>}],
                 [{T, M#broker_message.payload} || {T, M} <- Entries]),
    ok.

async_put_lands_after_the_flush_window(_Config) ->
    %% Kill the batch window's timing assumption: with a 0ms interval the app
    %% writes before answering, which is the suite-facing "durable now" mode.
    ok = application:set_env(broker_retain, retain_flush_interval_ms, 0),
    ok = broker_retain:put(msg(<<"a/b">>, <<"soon">>), async),
    {ok, Entries} = broker_retain_files:read(),
    ?assertEqual([{<<"a/b">>, <<"soon">>}],
                 [{T, M#broker_message.payload} || {T, M} <- Entries]),
    ok.

flush_writes_a_dirty_tree(_Config) ->
    ok = application:set_env(broker_retain, retain_flush_interval_ms, 60000),
    ok = broker_retain:put(msg(<<"a/b">>, <<"pending">>), async),
    %% Not on disk yet (a 60s window), and the app says so.
    {ok, []} = broker_retain_files:read(),
    #{dirty := true} = broker_retain:stats(),
    ok = broker_retain:flush(),
    {ok, [{<<"a/b">>, _}]} = broker_retain_files:read(),
    #{dirty := false} = broker_retain:stats(),
    ok.

overflow_by_count_refuses_the_new_topic(_Config) ->
    ok = application:set_env(broker_retain, max_retained, 2),
    ok = broker_retain:put(msg(<<"a/1">>, <<"x">>), sync),
    ok = broker_retain:put(msg(<<"a/2">>, <<"x">>), sync),
    {error, overflow} = broker_retain:put(msg(<<"a/3">>, <<"x">>), sync),
    %% Refusal is total: the tree did not change, and the older entries are
    %% still there. Eviction would make "the last retained value" random.
    error = broker_retain:lookup(<<"a/3">>),
    {ok, _} = broker_retain:lookup(<<"a/1">>),
    {ok, _} = broker_retain:lookup(<<"a/2">>),
    #{count := 2} = broker_retain:stats(),
    ok.

overflow_by_bytes_refuses_the_update(_Config) ->
    %% `size_of/1' is topic + payload, so `<<"t">>' contributes one byte and the
    %% replacement has to exceed 19 bytes to trip a 20-byte cap (the old 18 was
    %% under it, and the case passed for the wrong reason).
    ok = application:set_env(broker_retain, max_retained_bytes, 20),
    ok = broker_retain:put(msg(<<"t">>, <<"1234567890">>), sync),   % 1 + 10 = 11
    {error, overflow} = broker_retain:put(msg(<<"t">>, <<"12345678901234567890">>),
                                          sync),                    % 1 + 20 = 21
    {ok, #broker_message{payload = <<"1234567890">>}} =
        broker_retain:lookup(<<"t">>),
    ok.

overflow_by_payload_refuses_the_update(_Config) ->
    ok = application:set_env(broker_retain, max_payload_bytes, 4),
    {error, overflow} = broker_retain:put(msg(<<"t">>, <<"12345">>), sync),
    error = broker_retain:lookup(<<"t">>),
    ok = broker_retain:put(msg(<<"t">>, <<"1234">>), sync),
    {ok, _} = broker_retain:lookup(<<"t">>),
    ok.

overwriting_an_existing_topic_is_not_an_overflow(_Config) ->
    %% The count cap is about distinct topics: replacing the last allowed one
    %% must still work, or the tree would freeze at its limit.
    ok = application:set_env(broker_retain, max_retained, 1),
    ok = broker_retain:put(msg(<<"only">>, <<"v1">>), sync),
    ok = broker_retain:put(msg(<<"only">>, <<"v2">>), sync),
    {ok, #broker_message{payload = <<"v2">>}} = broker_retain:lookup(<<"only">>),
    ok.

corrupt_snapshot_refuses_to_boot(Config) ->
    DataDir = ?config(data_dir, Config),
    stop(),
    Path = filename:join([DataDir, "retain", "retain.v1"]),
    ok = filelib:ensure_dir(Path),
    ok = file:write_file(Path, <<"not a term at all">>),
    %% A boot that silently started empty would tell every new subscriber
    %% "nothing was ever retained" -- a lie `.subs' already refuses to tell.
    {error, Reason} = application:ensure_all_started(broker_retain),
    ?assertMatch({broker_retain, _}, Reason),
    ok.

unsupported_vsn_refuses_to_boot(Config) ->
    DataDir = ?config(data_dir, Config),
    stop(),
    Path = filename:join([DataDir, "retain", "retain.v1"]),
    ok = filelib:ensure_dir(Path),
    ok = file:write_file(Path, term_to_binary({vsn, 99, []})),
    {error, Reason} = application:ensure_all_started(broker_retain),
    ?assertMatch({broker_retain, _}, Reason),
    ok.

absent_snapshot_boots_empty(_Config) ->
    %% No file at all is not a fault: this broker simply never retained.
    #{count := 0, bytes := 0} = broker_retain:stats(),
    [] = broker_retain:entries(),
    ok.

boot_loads_the_previous_tree(_Config) ->
    ok = broker_retain:put(msg(<<"x/1">>, <<"one">>), sync),
    ok = broker_retain:put(msg(<<"x/2">>, <<"two">>), sync),
    stop(),
    boot(),
    ?assertEqual([<<"x/1">>, <<"x/2">>], [T || {T, _} <- broker_retain:entries()]),
    #{count := 2} = broker_retain:stats(),
    ok.

parse_rejects_garbage(_Config) ->
    {error, _} = broker_retain_files:parse(<<"garbage">>),
    {error, {unsupported_vsn, 7, _}} = broker_retain_files:parse(
                                          term_to_binary({vsn, 7, []})),
    {ok, []} = broker_retain_files:parse(term_to_binary({vsn, 1, []})),
    ok.

%%%_* Helpers -----------------------------------------------------------------

msg(Topic, Payload) ->
    #broker_message{message_id = crypto:strong_rand_bytes(16),
                    tenant_id = <<"default">>,
                    destination = Topic,
                    partition_id = 0,
                    payload = Payload,
                    payload_encoding = raw,
                    headers = #{},
                    proto_meta = #{qos => 1, dup => 0, retain => 1},
                    persistence = durable,
                    reliability = fsync,
                    created_at = erlang:system_time(millisecond),
                    expires_at = undefined,
                    event = undefined}.
