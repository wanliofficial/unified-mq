%% S0.1 - API surface probe for rabbitmq/ra.
%%
%% Guards against silent breakage across version bumps: every function listed
%% here is one the broker's consensus layer would depend on. If one vanishes
%% or changes arity, ADR-003 has to be revisited before the upgrade lands.
%%
%% Verified against ra 3.2.0 (commit 2d81d21).
-module(s0_1_ra).

-export([probe_api/0, required/0, missing/0]).

%% Functions we depend on, with the arity we call them at.
required() ->
    [{start_or_restart_cluster, 5},
     {members, 2},
     {process_command, 3},
     {local_query, 3},
     {leader_query, 3},
     {consistent_query, 3},
     {overview, 1},
     {member_overview, 2},
     {key_metrics, 2},
     {ping, 2},
     {add_member, 3},
     {remove_member, 3},
     {transfer_leadership, 3},
     {leave_and_delete_server, 4}].

%% missing() -> [{Function, Arity}] the current ra does not export.
missing() ->
    Exports = ra:module_info(exports),
    [{F, A} || {F, A} <- required(), not lists:member({F, A}, Exports)].

%% probe_api() -> map()
probe_api() ->
    #{version => application:get_key(ra, vsn),
      required_count => length(required()),
      missing => missing()}.