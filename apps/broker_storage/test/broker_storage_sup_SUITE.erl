%% @doc T5: what each storage backend needs from the supervision tree at boot.
%%
%% A backend that is configured but not supervised does not fail at boot -- it
%% fails at the first `create/2', in the caller, with a bare `noproc'. Both
%% branches are asserted here because `backend' is an application env value: the
%% release ships one, the tests need the other, and nothing else in the codebase
%% would notice if either branch rotted.
-module(broker_storage_sup_SUITE).

-compile([export_all, nowarn_export_all]).

all() ->
    [each_backend_boots_the_process_its_create_needs].

%% CT runs every suite in one node, so the application is stopped again
%% afterwards: leaving it up with one backend would decide what the next suite
%% sees, and `set_env' does not reconfigure an application that is running.
each_backend_boots_the_process_its_create_needs(_Config) ->
    ok = with_backend(broker_storage_wal, fun assert_wal_sup/0),
    ok = with_backend(broker_storage_memory, fun assert_memory_store/0),
    ok.

assert_wal_sup() ->
    %% create/2 adds partitions here, so the supervisor has to exist before any
    %% handle can be made. It starts with exactly the retention driver (S2-F /
    %% ADR-014) and no partition: boot step 1 recovers partitions and never
    %% creates them (ADR-011 §11.2, H8).
    [{broker_storage_wal_sup, Pid, supervisor, [broker_storage_wal_sup]}] =
        supervisor:which_children(broker_storage_sup),
    true = is_process_alive(Pid),
    [{wal_gc, GcPid, worker, [wal_gc]}] =
        supervisor:which_children(broker_storage_wal_sup),
    true = is_process_alive(GcPid),
    ok.

assert_memory_store() ->
    %% The store is a node singleton that create/2 and recover/2 find by name
    %% because they have no handle yet, and nothing else starts it.
    [{broker_storage_memory_store, Pid, worker, [broker_storage_memory_store]}] =
        supervisor:which_children(broker_storage_sup),
    Pid = whereis(broker_storage_memory_store),
    %% Not merely started: usable. This is the call that died with noproc.
    {error, {not_found, 1}} = broker_storage_memory:recover(1, #{}),
    ok.

%%%_* Helpers ------------------------------------------------------------

with_backend(Backend, Assert) ->
    _ = application:stop(broker_storage),
    ok = application:set_env(broker_storage, backend, Backend),
    {ok, _Started} = application:ensure_all_started(broker_storage),
    try
        Assert()
    after
        ok = application:stop(broker_storage)
    end.
