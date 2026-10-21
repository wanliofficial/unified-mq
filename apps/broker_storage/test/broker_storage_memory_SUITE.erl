%% @doc T4: behavior-contract tests for the in-memory storage backend.
%% Every assertion traces to ADR-011 §11.2~§11.6; nothing here invents semantics
%% that the frozen ADR does not require.
-module(broker_storage_memory_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("broker_core/include/broker_message.hrl").

-define(BACKEND, broker_storage_memory).

all() ->
    [create_returns_handle_with_os_buffer_capability,
     recover_missing_partition_does_not_create,
     create_twice_reports_already_exists,
     append_rejects_unsupported_sync_mode_without_writing,
     append_then_read_roundtrip,
     read_from_honours_from_offset_and_limit,
     recover_after_close_returns_watermarks,
     advance_committed_survives_close,
     advance_committed_rejects_unacked_offset,
     truncate_moves_logical_boundary_not_data,
     truncate_rejects_beyond_committed,
     ops_on_closed_handle_are_rejected,
     concurrent_append_yields_unique_offsets].

init_per_testcase(_Case, Config) ->
    {ok, _Pid} = broker_storage_memory_store:start_link(),
    Config.

end_per_testcase(_Case, _Config) ->
    ok = broker_storage_memory_store:stop(),
    ok.

%%%_* Tests --------------------------------------------------------------

%% §11.4: S1 memory backend capability is exactly [os_buffer].
create_returns_handle_with_os_buffer_capability(_Config) ->
    {ok, Hd} = ?BACKEND:create(1, #{}),
    [os_buffer] = ?BACKEND:capability(Hd),
    ok.

%% §11.2: recover/2 must never auto-create. A mistyped partition id or a lost
%% mount has to fail loudly; if this test's create/2 succeeded before the
%% recover, the assertion below would still pass, so the order matters.
recover_missing_partition_does_not_create(_Config) ->
    {error, {not_found, 7}} = ?BACKEND:recover(7, #{}),
    %% The failed recover left nothing behind: this is a genuine first create.
    {ok, _Hd} = ?BACKEND:create(7, #{}),
    ok.

create_twice_reports_already_exists(_Config) ->
    {ok, _} = ?BACKEND:create(1, #{}),
    {error, {already_exists, 1}} = ?BACKEND:create(1, #{}),
    ok.

%% §11.4 硬约束：超出 capability 的 sync_mode 必须报错，禁止静默降级。
%% A degraded write that still consumed an offset would be a silent promise break.
append_rejects_unsupported_sync_mode_without_writing(_Config) ->
    {ok, Hd} = ?BACKEND:create(1, #{}),
    {error, {unsupported_sync_mode, fsync}} = ?BACKEND:append(Hd, [msg(<<"a">>)], fsync),
    {error, {unsupported_sync_mode, replicated_commit}} =
        ?BACKEND:append(Hd, [msg(<<"b">>)], replicated_commit),
    {ok, [1]} = ?BACKEND:append(Hd, [msg(<<"c">>)], os_buffer),
    ok.

append_then_read_roundtrip(_Config) ->
    {ok, Hd} = ?BACKEND:create(1, #{}),
    {ok, [1, 2, 3]} = ?BACKEND:append(Hd, [msg(<<"p1">>), msg(<<"p2">>), msg(<<"p3">>)],
                                      os_buffer),
    {ok, Read, NextOffset} = ?BACKEND:read_from(Hd, 1, 10),
    [{1, M1}, {2, M2}, {3, M3}] = Read,
    <<"p1">> = M1#broker_message.payload,
    <<"p2">> = M2#broker_message.payload,
    <<"p3">> = M3#broker_message.payload,
    4 = NextOffset,
    ok.

read_from_honours_from_offset_and_limit(_Config) ->
    {ok, Hd} = ?BACKEND:create(1, #{}),
    {ok, _} = ?BACKEND:append(Hd, [msg(<<"p1">>), msg(<<"p2">>), msg(<<"p3">>)], os_buffer),
    {ok, [{2, M2}], 3} = ?BACKEND:read_from(Hd, 2, 1),
    <<"p2">> = M2#broker_message.payload,
    %% Past the end of the log: empty page, NextOffset stays at the request.
    {ok, [], 10} = ?BACKEND:read_from(Hd, 10, 5),
    ok.

%% §11.2 + §11.3: recover reports both watermarks; a fresh partition has
%% nothing committed (0) and the next append goes to offset 1.
recover_after_close_returns_watermarks(_Config) ->
    {ok, Hd} = ?BACKEND:create(2, #{}),
    ok = ?BACKEND:close(Hd),
    {ok, Hd2, 0, 1} = ?BACKEND:recover(2, #{}),
    {ok, [1]} = ?BACKEND:append(Hd2, [msg(<<"x">>)], os_buffer),
    ok = ?BACKEND:close(Hd2),
    {ok, _Hd3, 0, 2} = ?BACKEND:recover(2, #{}),
    ok.

advance_committed_survives_close(_Config) ->
    {ok, Hd} = ?BACKEND:create(1, #{}),
    {ok, _} = ?BACKEND:append(Hd, [msg(<<"a">>), msg(<<"b">>)], os_buffer),
    ok = ?BACKEND:advance_committed(Hd, 2),
    ok = ?BACKEND:close(Hd),
    {ok, _Hd2, 2, 3} = ?BACKEND:recover(1, #{}),
    ok.

%% §11.3: 偏高绝对禁止。Claiming an offset that was never appended would let
%% truncate/2 discard data that does not exist / is not acked.
advance_committed_rejects_unacked_offset(_Config) ->
    {ok, Hd} = ?BACKEND:create(1, #{}),
    {ok, _} = ?BACKEND:append(Hd, [msg(<<"a">>)], os_buffer),
    {error, _} = ?BACKEND:advance_committed(Hd, 2),
    ok = ?BACKEND:advance_committed(Hd, 1),
    %% A second, lower advance must not silently walk the watermark back.
    {error, _} = ?BACKEND:advance_committed(Hd, 0),
    ok.

%% §11.6: truncate only sets the logical unreadable boundary; records at and
%% above the boundary stay readable.
truncate_moves_logical_boundary_not_data(_Config) ->
    {ok, Hd} = ?BACKEND:create(1, #{}),
    {ok, _} = ?BACKEND:append(Hd, [msg(<<"a">>), msg(<<"b">>), msg(<<"c">>)], os_buffer),
    ok = ?BACKEND:advance_committed(Hd, 2),
    ok = ?BACKEND:truncate(Hd, 3),
    {ok, [{3, M3}], 4} = ?BACKEND:read_from(Hd, 1, 10),
    <<"c">> = M3#broker_message.payload,
    ok.

truncate_rejects_beyond_committed(_Config) ->
    {ok, Hd} = ?BACKEND:create(1, #{}),
    {ok, _} = ?BACKEND:append(Hd, [msg(<<"a">>), msg(<<"b">>)], os_buffer),
    ok = ?BACKEND:advance_committed(Hd, 1),
    %% Offset 2 is not acked yet, so discarding it would be data loss.
    {error, _} = ?BACKEND:truncate(Hd, 3),
    ok = ?BACKEND:truncate(Hd, 2),
    ok.

ops_on_closed_handle_are_rejected(_Config) ->
    {ok, Hd} = ?BACKEND:create(1, #{}),
    ok = ?BACKEND:close(Hd),
    {error, _} = ?BACKEND:append(Hd, [msg(<<"a">>)], os_buffer),
    {error, _} = ?BACKEND:read_from(Hd, 1, 10),
    %% close/1 does not destroy the partition: it still has to be recovered.
    {error, {already_exists, 1}} = ?BACKEND:create(1, #{}),
    ok.

%% README §7.2: append does not serialize through one process, so concurrent
%% writers must still get unique, gap-free offsets.
concurrent_append_yields_unique_offsets(_Config) ->
    {ok, Hd} = ?BACKEND:create(1, #{}),
    Parent = self(),
    Readers =
        lists:map(
            fun(I) ->
                spawn_link(fun() ->
                    {ok, Offsets} = ?BACKEND:append(Hd,
                                                    [msg(list_to_binary(integer_to_list(I)))],
                                                    os_buffer),
                    Parent ! {self(), Offsets}
                end)
            end,
            lists:seq(1, 20)),
    All = collect(Readers, []),
    Flat = lists:flatten(All),
    20 = length(Flat),
    20 = length(lists:usort(Flat)),
    {ok, Read, 21} = ?BACKEND:read_from(Hd, 1, 100),
    20 = length(Read),
    ok.

%%%_* Helpers ------------------------------------------------------------

collect([], Acc) ->
    lists:reverse(Acc);
collect([Pid | Rest], Acc) ->
    receive
        {Pid, Offsets} -> collect(Rest, [Offsets | Acc])
    after 5000 -> ct:fail({no_append_result_from, Pid})
    end.

msg(Payload) ->
    #broker_message{message_id = <<"m">>,
                    tenant_id = <<"t">>,
                    destination = <<"d">>,
                    partition_id = 1,
                    payload = Payload,
                    payload_encoding = raw,
                    headers = #{},
                    proto_meta = #{},
                    persistence = durable,
                    reliability = os_buffer,
                    created_at = 1,
                    expires_at = undefined,
                    event = undefined}.
