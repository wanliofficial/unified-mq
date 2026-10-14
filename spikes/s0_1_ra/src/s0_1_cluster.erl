%% S0.1 - 3-node ra cluster harness.
%%
%% Three real Raft groups as three separate OS processes on loopback, so we can
%% kill them individually and observe leadership and majority behaviour
%% (README 11.3: node crash, partition, loss of majority, leader switch).
%%
%% API notes measured against ra 3.2.0:
%%   - machine is {module, Module, InitArgs}
%%   - server id is {ClusterName, "nX@host"}: a BARE atom never forms a group
%%   - uid must be a base64uri binary, not an atom
%%   - every call takes the cluster/server id as first argument
-module(s0_1_cluster).

-export([node_main/1, init/1, apply/3, stop/1, start_local/1,
         members/1, bench/2, write/2, read/1, conf/2]).

-define(PORT_BASE, 9300).

%% node_main([DataDir, Name]) -> no_return()
%%
%% Entry point for each node process. Starts ra and parks.
node_main([DataDirArg, NameArg]) ->
    %% ra_env:data_dir/0 only accepts a list (a binary is ignored) and
    %% appends the node name itself, so pass the parent dir.
    DataDir = DataDirArg,
    Name = list_to_atom(NameArg),
    ok = filelib:ensure_dir(filename:join(DataDir, "x")),
    application:set_env(ra, data_dir, DataDir),
    {ok, _} = application:ensure_all_started(ra),
    %% ra_server_sup_sup:restart_server/3 returns {error, system_not_started}
    %% unless the named system exists on that node first. The default system
    %% is named 'default' (ra_system:default_config/0), NOT ?MODULE.
    _ = ra_system:start_default(),
    io:format("NODE_UP ~p data_dir=~s~n", [Name, DataDir]),
    ok = file:write_file(filename:join(DataDir, "up"),
                         atom_to_binary(Name, utf8)),
    timer:sleep(infinity).

%% start_local(Cluster) -> ok | {error, Reason}
%%
%% Boots the three-node group. Assumes all three nodes are already running
%% and have their host ports registered below.
%% ra:start_cluster/2 starts every server on the LOCAL node, so it cannot
%% form a cluster that spans three OS processes.
%% ra:start_or_restart_cluster/5 would be the right entry point, but on
%% ra 3.2.0 it crashes with a case_clause when a remote node is not yet
%% reachable: ra.erl:325 matches only {ok,_} and {error,Err} while
%% ra_server_sup_sup:restart_server/3 is documented to also return
%% {badrpc, term()}. See REPORT.md "upstream bug".
%%
%% Workaround: each node starts its own server locally.
%%
%% ra:restart_server/3 is the WRONG entry point for a first boot: it calls
%% recover_config/2, which returns {error, name_not_registered} for a name
%% that has never been registered (ra_server_sup_sup.erl:238). Only
%% ra:start_server/2 reaches the fresh-start path, and it must be given the
%% full config including id and uid.
%% A Raft server id's node part MUST be the real BEAM node name. The harness
%% runs nodes as ra_n1@host, so the id must be {Cluster, ra_n1@host}; using
%% {Cluster, n1@host} makes ra:members/2 answer {error, nodedown} because that
%% node does not exist.
start_local(Cluster) ->
    %% Use ra's documented multi-node entry point, as its own
    %% coordination_SUITE does: ONE call from any node dispatches the whole
    %% group, starting each server on its own node and performing the
    %% membership bootstrap that leads to an election.
    %%
    %% Starting each server with ra:start_server/2 from its own node does NOT
    %% work: all three come up healthy, agree on membership and never elect a
    %% leader, so every write times out.
    %%
    %% (ra:start_or_restart_cluster/5 has a case_clause bug when a remote node
    %% is not yet reachable -- ra.erl:325 handles only {ok,_} and {error,Err}
    %% while ra_server_sup_sup:restart_server/3 is documented to also return
    %% {badrpc,_}. The 4-arity form normalises that and is safe.)
    ra:start_or_restart_cluster(default, Cluster, {module, ?MODULE, #{}},
                                [{Cluster, peer(N)} || N <- [n1, n2, n3]],
                                20000).

%% The three member node names of the harness.
peer(Bare) ->
    list_to_atom("ra_" ++ atom_to_list(Bare) ++ "@" ++ host()).

host() ->
    case inet:gethostname() of
        {ok, H} -> H;
        _ -> "localhost"
    end.

conf(Cluster, Name) ->
    %% One uid per server, shared by the config and the WAL init args; ra
    %% keys its snapshot state table on it.
    UId = uid_of(Cluster),
    #{id => {Cluster, Name},
      uid => UId,
      cluster_name => Cluster,
      initial_members => [{Cluster, peer(N)} || N <- [n1, n2, n3]],
      log_init_args => #{uid => UId,
                         min_checkpoint_interval => 3,
                         min_snapshot_interval => 5},
      machine => {module, ?MODULE, #{}},
      friendly_name => node_tag(Name),
      metrics_key => Cluster}.

%% uid constraints, both learned the hard way:
%%   - ra_log_snapshot_state:insert/5 guards on `when is_binary(UId)' and
%%     CRASHES the server at boot for anything else, so uid MUST be binary;
%%   - ra_lib:validate_base64uri/1 rejects '@', so the BEAM node name cannot
%%     be used verbatim.
%% ra builds uids with its own ra:new_uid/1 (a random suffix on a sanitised
%% cluster-name prefix); reuse it rather than inventing a format.
uid_of(Cluster) ->
    ra:new_uid(list_to_binary(atom_to_list(Cluster))).

%% The node tag is only used for the human-readable friendly_name.
node_tag(Name) ->
    [Bare | _] = string:split(atom_to_list(Name), "@", all),
    Bare.

%% init/1 is the machine's Raft entry point.
init(#{name := Name} = Args) -> Args#{name => Name}.

%% apply/3 is the state machine. The return contract is
%% {State, Reply} or {State, Reply, Effects} (ra_machine.erl:257) -- NOT
%% {reply, Reply, State}, which is what RabbitMQ's own ra_machine_intern
%% style examples sometimes show. Returning the wrong shape makes every
%% process_command time out.
%%
%% A single KV entry is enough to measure consensus cost, and is exactly what
%% a broker metadata group would do.
%% ra calls apply/3 as apply(Meta, Command, State) -- the METADATA comes
%% first. Matching on {write, Term} in the first position therefore never
%% matches and every command falls through to unknown_command.
apply(_Meta, {write, Term}, State) ->
    {State#{kv => Term}, ok};
apply(_Meta, _Command, State) ->
    {State, {error, unknown_command}}.

stop(Cluster) ->
    catch ra:stop_server({Cluster, peer(n1)}),
    ok.

%% members(Cluster) -> {ok, [Member], Leader} | {error, _}
%%
%% NOTE the {local, Id} form. A bare ServerId routes through
%% ra_server_proc:gen_statem_safe_call/3, which does an rpc to the id's node
%% and then calls the process registered under ra_server_id_to_local_name(Id)
%% -- that is the id's FIRST element (ra_lib.erl:225). Since all three nodes
%% register their server locally under the cluster name "meta", the
%% networked form only works if the target node is genuinely reachable and
%% agrees on that name. {local, Id} skips the rpc entirely, which is what a
%% node-local admin query wants anyway.
members(Cluster) ->
    ra:members({local, {Cluster, node()}}, 5000).

%% read(Cluster) -> {ok, Value, _Leader} | {error, _}
read(Cluster) ->
    ra:local_query({Cluster, node()},
                   fun(S) -> {ok, maps:get(kv, S, null), S} end,
                   5000).

%% bench(Cluster, N) -> map()
%%
%% Cost of one replicated write. S0.2 multiplies this by Raft group count, so
%% it is the number that decides the grouping strategy.
bench(Cluster, N) ->
    %% Warm up: JIT and first-log-entry costs would skew early samples.
    _ = [write(Cluster, {warm, I}) || I <- lists:seq(1, 20)],
    T0 = erlang:monotonic_time(microsecond),
    Results = [write(Cluster, {bench, I}) || I <- lists:seq(1, N)],
    Elapsed = erlang:monotonic_time(microsecond) - T0,
    Ok = length([1 || R <- Results, is_ok(R)]),
    #{writes => N,
      succeeded => Ok,
      failed => N - Ok,
      elapsed_us => Elapsed,
      us_per_write => round(Elapsed / N * 10) / 10,
      writes_per_sec => round(N / (Elapsed / 1000000))}.

write(Cluster, Term) ->
    %% process_command/3 must reach the leader. Asking the leader's own id
    %% avoids depending on ra's follower->leader redirect, which needs the
    %% directory rpc to resolve and times out in this harness.
    case ra:members({local, {Cluster, node()}}, 5000) of
        {ok, _Members, Leader} ->
            ra:process_command(Leader, {write, Term}, 8000);
        Other ->
            {error, {no_leader, Other}}
    end.

is_ok({ok, ok, _}) -> true;
is_ok(_) -> false.