%% @doc T4: the ADR-011 storage contract run against the in-memory backend.
%% The 13 cases and every assertion live in broker_storage_contract.hrl, so the
%% WAL backend is held to the identical contract; nothing in this file is
%% memory-specific except the two macros and three hooks that parameterize it.
-module(broker_storage_memory_SUITE).

-compile([export_all, nowarn_export_all]).

%% ADR-011 §11.4 capability table, S1 row: memory never claims more than
%% os_buffer, and the contract asserts exactly the declared set.
-define(BACKEND, broker_storage_memory).
-define(CAPABILITY, [os_buffer]).

-include("broker_storage_contract.hrl").

%%%_* Contract hooks ------------------------------------------------------

%% The store process owns the two ETS tables; one fresh store per testcase is
%% this backend's equivalent of an empty data directory.
backend_setup(_Case, Config) ->
    {ok, _StorePid} = broker_storage_memory_store:start_link(),
    Config.

backend_cleanup(_Config) ->
    ok = broker_storage_memory_store:stop().

%% Memory partitions are ETS keys: no path, no per-partition options.
backend_config(_Config) ->
    #{}.

backend_cases() ->
    [].
