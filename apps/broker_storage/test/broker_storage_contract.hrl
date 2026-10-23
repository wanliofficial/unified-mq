%% @doc The `broker_storage' behavior contract as a reusable Common Test case set.
%%
%% This is ADR-011 §11.2~§11.6 turned into executable assertions. Every backend
%% has to satisfy all of them: the case names are the contract, so roadmap
%% Gate「WAL 与内存实现可互换」becomes a mechanical check -- include this file in
%% the other backend's suite and run the same 13 cases -- rather than a claim
%% about design intent.
%%
%% A backend suite supplies two macros and three local functions and then
%% includes this file:
%%   -define(BACKEND, Module).        the backend module atom
%%   -define(CAPABILITY, [sync_mode()])  its ADR-011 §11.4 capability value
%%   backend_setup(Case, Config)      per-testcase cold start, returns Config
%%   backend_cleanup(Config)          per-testcase teardown
%%   backend_config(Config)           the map handed to create/2 and recover/2
%%   backend_cases()                  extra cases this backend owes beyond the
%%                                    contract ([] for most)
%% A missing hook is an undefined-function warning, and the umbrella
%% `warnings_as_errors' turns that into a build failure, so a suite cannot
%% forget one.
%%
%% `backend_setup' takes the case name because CT hands every testcase in one
%% suite run the SAME priv_dir: a disk backend has to build a per-case
%% subdirectory or each case appends to the previous one's partition.
%%
%% Nothing below may name a backend, a table or a directory. If a case needs one
%% of those, the hook layer is too thin and has to be widened instead.
%%
%% Writes in these cases always use `os_buffer': §11.4 puts it in every
%% backend's capability, so it is the only mode the contract may assume. Modes
%% outside ?CAPABILITY are exercised only through the rejection case.

-include_lib("broker_core/include/broker_message.hrl").

-ifndef(BACKEND).
-error("broker_storage_contract.hrl requires -define(BACKEND, Module).").
-endif.
-ifndef(CAPABILITY).
-error("broker_storage_contract.hrl requires -define(CAPABILITY, [sync_mode()]).").
-endif.

all() ->
    contract_cases() ++ backend_cases().

%% The contract proper. A backend suite cannot add to this list, only to
%% backend_cases/0, so "13/13, nothing skipped" stays a property of the header
%% rather than of each suite's all/0.
contract_cases() ->
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

%% Setup must leave the partition set empty -- the cases never delete anything,
%% so a disk backend uses a fresh tmp dir per testcase and a process-backed
%% backend uses a freshly started store.
init_per_testcase(Case, Config) ->
    backend_setup(Case, Config).

end_per_testcase(_Case, Config) ->
    backend_cleanup(Config),
    ok.

%%%_* Contract cases -----------------------------------------------------

%% §11.4: capability is what the backend honestly supports. The suite declares
%% the expected set, so a backend cannot pass by reporting whatever it likes --
%% and a volatile backend can never quietly claim fsync.
create_returns_handle_with_os_buffer_capability(Config) ->
    {ok, Hd} = ?BACKEND:create(1, backend_config(Config)),
    Declared = lists:usort(?CAPABILITY),
    Declared = lists:usort(?BACKEND:capability(Hd)),
    true = lists:member(os_buffer, Declared),
    %% Tripwire: a backend that declares a mode outside the frozen sync_mode()
    %% set fails here, which is the signal to widen all_sync_modes/0 -- otherwise
    %% the new mode would escape the rejection case below unnoticed.
    lists:foreach(fun(Mode) ->
                     true = lists:member(Mode, all_sync_modes())
                  end,
                  Declared),
    ok.

%% §11.2: recover/2 must never auto-create. A mistyped partition id or a lost
%% mount has to fail loudly; if this test's create/2 succeeded before the
%% recover, the assertion below would still pass, so the order matters.
recover_missing_partition_does_not_create(Config) ->
    {error, {not_found, 7}} = ?BACKEND:recover(7, backend_config(Config)),
    %% The failed recover left nothing behind: this is a genuine first create.
    {ok, _Hd} = ?BACKEND:create(7, backend_config(Config)),
    ok.

%% §11.2: create/2 is the only bootstrap path and it refuses to overwrite.
create_twice_reports_already_exists(Config) ->
    {ok, _} = ?BACKEND:create(1, backend_config(Config)),
    {error, {already_exists, 1}} = ?BACKEND:create(1, backend_config(Config)),
    ok.

%% §11.4 硬约束：超出 capability 的 sync_mode 必须报错，禁止静默降级。
%% A degraded write that still consumed an offset would be a silent promise break.
append_rejects_unsupported_sync_mode_without_writing(Config) ->
    {ok, Hd} = ?BACKEND:create(1, backend_config(Config)),
    lists:foreach(
        fun(Mode) ->
            {error, {unsupported_sync_mode, Mode}} =
                ?BACKEND:append(Hd, [msg(<<"x">>)], Mode)
        end,
        unsupported_modes()),
    %% None of the rejected attempts consumed an offset.
    {ok, [1]} = ?BACKEND:append(Hd, [msg(<<"c">>)], os_buffer),
    ok.

append_then_read_roundtrip(Config) ->
    {ok, Hd} = ?BACKEND:create(1, backend_config(Config)),
    {ok, [1, 2, 3]} = ?BACKEND:append(Hd, [msg(<<"p1">>), msg(<<"p2">>), msg(<<"p3">>)],
                                      os_buffer),
    {ok, Read, NextOffset} = ?BACKEND:read_from(Hd, 1, 10),
    [{1, M1}, {2, M2}, {3, M3}] = Read,
    <<"p1">> = M1#broker_message.payload,
    <<"p2">> = M2#broker_message.payload,
    <<"p3">> = M3#broker_message.payload,
    4 = NextOffset,
    ok.

read_from_honours_from_offset_and_limit(Config) ->
    {ok, Hd} = ?BACKEND:create(1, backend_config(Config)),
    {ok, _} = ?BACKEND:append(Hd, [msg(<<"p1">>), msg(<<"p2">>), msg(<<"p3">>)], os_buffer),
    {ok, [{2, M2}], 3} = ?BACKEND:read_from(Hd, 2, 1),
    <<"p2">> = M2#broker_message.payload,
    %% Past the end of the log: empty page, NextOffset stays at the request.
    {ok, [], 10} = ?BACKEND:read_from(Hd, 10, 5),
    ok.

%% §11.2 + §11.3: recover reports both watermarks; a fresh partition has
%% nothing committed (0) and the next append goes to offset 1.
recover_after_close_returns_watermarks(Config) ->
    {ok, Hd} = ?BACKEND:create(2, backend_config(Config)),
    ok = ?BACKEND:close(Hd),
    {ok, Hd2, 0, 1} = ?BACKEND:recover(2, backend_config(Config)),
    {ok, [1]} = ?BACKEND:append(Hd2, [msg(2, <<"x">>)], os_buffer),
    ok = ?BACKEND:close(Hd2),
    {ok, _Hd3, 0, 2} = ?BACKEND:recover(2, backend_config(Config)),
    ok.

advance_committed_survives_close(Config) ->
    {ok, Hd} = ?BACKEND:create(1, backend_config(Config)),
    {ok, _} = ?BACKEND:append(Hd, [msg(<<"a">>), msg(<<"b">>)], os_buffer),
    ok = ?BACKEND:advance_committed(Hd, 2),
    ok = ?BACKEND:close(Hd),
    {ok, _Hd2, 2, 3} = ?BACKEND:recover(1, backend_config(Config)),
    ok.

%% §11.3: 偏高绝对禁止。Claiming an offset that was never appended would let
%% truncate/2 discard data that does not exist / is not acked.
advance_committed_rejects_unacked_offset(Config) ->
    {ok, Hd} = ?BACKEND:create(1, backend_config(Config)),
    {ok, _} = ?BACKEND:append(Hd, [msg(<<"a">>)], os_buffer),
    {error, _} = ?BACKEND:advance_committed(Hd, 2),
    ok = ?BACKEND:advance_committed(Hd, 1),
    %% A second, lower advance must not silently walk the watermark back.
    {error, _} = ?BACKEND:advance_committed(Hd, 0),
    ok.

%% §11.6: truncate only sets the logical unreadable boundary; records at and
%% above the boundary stay readable.
truncate_moves_logical_boundary_not_data(Config) ->
    {ok, Hd} = ?BACKEND:create(1, backend_config(Config)),
    {ok, _} = ?BACKEND:append(Hd, [msg(<<"a">>), msg(<<"b">>), msg(<<"c">>)], os_buffer),
    ok = ?BACKEND:advance_committed(Hd, 2),
    ok = ?BACKEND:truncate(Hd, 3),
    {ok, [{3, M3}], 4} = ?BACKEND:read_from(Hd, 1, 10),
    <<"c">> = M3#broker_message.payload,
    ok.

truncate_rejects_beyond_committed(Config) ->
    {ok, Hd} = ?BACKEND:create(1, backend_config(Config)),
    {ok, _} = ?BACKEND:append(Hd, [msg(<<"a">>), msg(<<"b">>)], os_buffer),
    ok = ?BACKEND:advance_committed(Hd, 1),
    %% Offset 2 is not acked yet, so discarding it would be data loss.
    {error, _} = ?BACKEND:truncate(Hd, 3),
    ok = ?BACKEND:truncate(Hd, 2),
    ok.

ops_on_closed_handle_are_rejected(Config) ->
    {ok, Hd} = ?BACKEND:create(1, backend_config(Config)),
    ok = ?BACKEND:close(Hd),
    {error, _} = ?BACKEND:append(Hd, [msg(<<"a">>)], os_buffer),
    {error, _} = ?BACKEND:read_from(Hd, 1, 10),
    %% close/1 does not destroy the partition: it still has to be recovered.
    {error, {already_exists, 1}} = ?BACKEND:create(1, backend_config(Config)),
    ok.

%% README §7.2: append does not serialize through one process, so concurrent
%% writers must still get unique, gap-free offsets.
concurrent_append_yields_unique_offsets(Config) ->
    {ok, Hd} = ?BACKEND:create(1, backend_config(Config)),
    Parent = self(),
    Writers =
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
    All = collect(Writers, []),
    Flat = lists:flatten(All),
    20 = length(Flat),
    20 = length(lists:usort(Flat)),
    {ok, Read, 21} = ?BACKEND:read_from(Hd, 1, 100),
    20 = length(Read),
    ok.

%%%_* Contract helpers ---------------------------------------------------

%% The frozen `sync_mode()' set of ADR-009, spelled out rather than read from the
%% type: a new mode then has to be added here deliberately, and both the tripwire
%% above and the rejection case below keep covering the whole set.
all_sync_modes() ->
    [os_buffer, fsync, replicated_commit].

%% Every sync_mode() in the frozen ADR-009 type, minus what this backend
%% declares it can honour.
unsupported_modes() ->
    [Mode || Mode <- all_sync_modes(), not lists:member(Mode, ?CAPABILITY)].

collect([], Acc) ->
    lists:reverse(Acc);
collect([Pid | Rest], Acc) ->
    receive
        {Pid, Offsets} -> collect(Rest, [Offsets | Acc])
    after 5000 -> ct:fail({no_append_result_from, Pid})
    end.

%% ADR-011 defines no validation of the message's own partition_id field -- the
%% partition is chosen by the caller and carried by the handle. It is still
%% filled with the partition under test so that a backend which does check it
%% passes the contract.
msg(Payload) ->
    msg(1, Payload).

msg(PartitionId, Payload) ->
    #broker_message{message_id = <<"m">>,
                    tenant_id = <<"t">>,
                    destination = <<"d">>,
                    partition_id = PartitionId,
                    payload = Payload,
                    payload_encoding = raw,
                    headers = #{},
                    proto_meta = #{},
                    persistence = durable,
                    reliability = os_buffer,
                    created_at = 1,
                    expires_at = undefined,
                    event = undefined}.
