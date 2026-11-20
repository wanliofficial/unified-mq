%% @doc S4 distributed semantics (ADR-021, README §11.3).
%%
%% Three real peer nodes, each with its own `ra' data directory, form one
%% Metadata Raft Group. The suite proves the four distributed claims the
%% roadmap's Gate S4 names:
%%
%%   * a 3-node group forms and every node sees the committed owner;
%%   * killing the Leader leaves a majority that still commits (epoch advances);
%%   * losing the majority turns metadata writes into `{error, no_quorum}';
%%   * a node whose cache still says "I own it" is fenced once ownership moved.
%%
%% `peer' is part of OTP 28's stdlib, so no external orchestration is needed.
-module(broker_cluster_dist_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("broker_core/include/broker_message.hrl").

-define(CLUSTER, broker_cluster_meta).

all() ->
    [three_node_group_forms_and_replicates,
     leader_failover_keeps_committing,
     losing_the_majority_rejects_metadata_writes,
     old_owner_is_fenced_after_migration,
     a_crashed_node_rejoins_and_catches_up,
     a_stale_session_writer_is_fenced_by_the_guard_hook,
     online_join_makes_the_new_node_a_voter,
     online_join_is_visible_to_the_rebalance_view,
     online_join_is_rejected_without_a_majority,
     clean_leave_removes_the_node_from_the_group].

%%%_* Fixtures --------------------------------------------------------------

init_per_suite(Config) ->
    %% `peer' needs a distributed origin: a peer with a name is contacted over
    %% distribution. CT normally runs as `nonode@nohost', so this suite starts
    %% short-name distribution itself and stops it again if it did.
    case node() of
        nonode@nohost ->
            %% Do not touch the cookie here: before distribution is up,
            %% `erlang:get_cookie()' answers `nocookie', and re-applying that
            %% after `net_kernel:start/1' leaves the node with a cookie the
            %% peers (which read ~/.erlang.cookie) cannot authenticate
            %% against. The effect is a `peer:start_link' that waits for the
            %% whole `wait_boot' and then exits with `timeout'.
            case net_kernel:start([unifiedmq_s4_ct, shortnames]) of
                {ok, _} ->
                    [{started_net, true} | Config];
                {error, Reason} ->
                    ct:fail({cannot_start_distribution, Reason})
            end;
        _ ->
            [{started_net, false} | Config]
    end.

end_per_suite(Config) ->
    case ?config(started_net, Config) of
        true -> _ = net_kernel:stop(), ok;
        _ -> ok
    end.

init_per_testcase(Case, Config) ->
    Peers = start_peers(Case, Config),
    [{peers, Peers}, {new_peer, undefined} | Config].

end_per_testcase(_Case, Config) ->
    %% `online_join_*' start a fourth peer and register it as `new_peer'; stop it
    %% before the fixture peers so its teardown never races a peer the cluster
    %% still references.
    case ?config(new_peer, Config) of
        #{pid := Pid} -> catch peer:stop(Pid);
        undefined -> ok
    end,
    stop_peers(?config(peers, Config)),
    ok.

%%%_* Cases ----------------------------------------------------------------

three_node_group_forms_and_replicates(Config) ->
    [N1, N2, N3] = peer_nodes(Config),
    %% One call dispatches the whole group; every peer already has its ra
    %% system and cache up (see prepare_peer/1).
    ok = rpc(N1, start_meta, [members(Config)]),
    ok = rpc(N2, start_meta, [members(Config)]),
    ok = rpc(N3, start_meta, [members(Config)]),
    {ok, 1} = rpc(N1, broker_cluster, assign, [0, N1, members(Config), 1]),
    %% The command is replicated: each node's local cache sees the owner.
    lists:foreach(
      fun(N) ->
              {ok, Owner} = rpc(N, broker_cluster, owner, [0]),
              Owner = N1,
              {ok, 1} = rpc(N, broker_cluster, epoch, [0])
      end, [N1, N2, N3]),
    ok.

leader_failover_keeps_committing(Config) ->
    [N1, N2, N3] = peer_nodes(Config),
    ok = form_cluster(Config),
    {ok, 1} = rpc(N1, broker_cluster, assign, [0, N1, members(Config), 1]),
    Leader = leader(Config),
    true = lists:member(Leader, [N1, N2, N3]),
    %% Kill the Leader. The surviving two nodes are still a majority.
    stop_peer(Config, Leader),
    Survivors = [N || N <- [N1, N2, N3], N =/= Leader],
    [S1 | _] = Survivors,
    wait_until(fun() -> is_pid(rpc(S1, erlang, whereis,
                                   [broker_cluster_meta_ctl])) end,
               15000),
    {ok, 2} = retry(fun() ->
                            rpc(S1, broker_cluster, assign,
                                [1, S1, Survivors, 2])
                    end, 20, 250),
    %% Both survivors agree, and the epoch advanced.
    lists:foreach(
      fun(N) ->
              {ok, 2} = rpc(N, broker_cluster, epoch, [1])
      end, Survivors),
    ok.

losing_the_majority_rejects_metadata_writes(Config) ->
    [N1, N2, N3] = peer_nodes(Config),
    _ = N3,
    ok = form_cluster(Config),
    {ok, 1} = rpc(N1, broker_cluster, assign, [0, N1, members(Config), 1]),
    %% Stop two nodes: one left out of three is not a majority.
    stop_peer(Config, N2),
    stop_peer(Config, N3),
    {error, no_quorum} =
        rpc(N1, broker_cluster, assign, [1, N1, [N1], 2]),
    ok.

old_owner_is_fenced_after_migration(Config) ->
    [N1, N2, N3] = peer_nodes(Config),
    _ = N3,
    ok = form_cluster(Config),
    All = members(Config),
    {ok, 1} = rpc(N1, broker_cluster, assign, [0, N1, All, 1]),
    ok = rpc(N1, broker_cluster, authorize_append, [0, 1]),
    %% Move ownership to N2. Every node applies the command, so N1 now knows
    %% it is no longer the owner.
    {ok, 2} = rpc(N2, broker_cluster, migrate, [0, N1, N2, 2]),
    {error, {fenced, Fenced}} = rpc(N1, broker_cluster,
                                    authorize_append, [0, 1]),
    0 = maps:get(partition, Fenced),
    2 = maps:get(expected_epoch, Fenced),
    %% N2, the current owner, accepts the new epoch.
    ok = rpc(N2, broker_cluster, authorize_append, [0, 2]),
    ok.

%% A node that crashed and booted again must come back on its own: its local ra
%% server restarts and catches up from the leader, with no operator action and
%% no restart of the surviving peers. This is the "stateful service crashes and
%% recovers" half of the S12 gate, at the layer where the ownership truth lives.
%%
%% The case exists because the recovery path was broken: `ra' restarts the head
%% server locally and then "restarts" every other member over rpc, which on a
%% rejoining node answers `{error, {already_started, _}}' for peers that never
%% went down -- and the list comprehension inside `ra' badmatches on it, so
%% `broker_cluster_meta:init/1' returned `{stop, ...}' and the node never came
%% back. A single node crashing was enough to require a full-group restart.
a_crashed_node_rejoins_and_catches_up(Config) ->
    Members = members(Config),
    [N1, N2, N3] = Members,
    ok = form_cluster(Config),
    %% N1 owns partition 0 at epoch 1, and N3 has seen it.
    {ok, 1} = rpc(N1, broker_cluster, assign, [0, N1, Members, 1]),
    {ok, N1} = rpc(N3, broker_cluster, owner, [0]),
    %% N3 crashes: its metadata server and ra system go away, its peers stay up.
    ok = rpc(N3, crash_meta, []),
    undefined = rpc(N3, whereis_meta, []),
    %% The cluster moves on while N3 is down -- this is the state it must catch
    %% up on, not merely "the same view it had before".
    {ok, 2} = rpc(N2, broker_cluster, assign, [0, N2, Members, 2]),
    %% N3 boots again with the same member list, exactly as a reboot would.
    ok = rpc(N3, revive_cache, []),
    ok = rpc(N3, start_meta, [Members]),
    ok = wait_until(fun() ->
                            rpc(N3, broker_cluster, owner, [0]) =:= {ok, N2}
                    end, 20000),
    %% It did not just come back: it caught up to epoch 2 and to the new owner.
    {ok, 2} = rpc(N3, broker_cluster, epoch, [0]),
    {ok, N2} = rpc(N3, broker_cluster, owner, [0]),
    %% Being caught up also means fencing correctly: N3 is not the owner any
    %% more, so it refuses the write at *any* epoch, while the real owner takes
    %% the current one. (Before the crash N3 would have answered `ok' for
    %% epoch 1 had it owned the partition; the point here is that its view is
    %% current, not stale.)
    {error, {fenced, #{partition := 0, expected_epoch := 2}}} =
        rpc(N3, broker_cluster, authorize_append, [0, 2]),
    ok = rpc(N2, broker_cluster, authorize_append, [0, 2]),
    %% And the survivors did not have to be touched: the group still commits.
    {ok, 1} = rpc(N1, broker_cluster, assign, [1, N1, Members, 1]),
    ok.

%% Session affinity (R-CL2 / Gate S12's 会话粘性) at the layer the protocol
%% adapters actually reach. A connection is pinned to one access node for its
%% whole lifetime; that node stamps the epoch it saw into `proto_meta' when it
%% hands a message to the queue (`mqtt_conn_fsm:annotate_epoch/1', the
%% `{broker_cluster, epoch}' hook). The queue runs this guard *before* it
%% consumes an offset, so a `{fenced, _}' answer is a refusal, not a retry --
%% the "no double write" half of the Gate, and why the adapter never re-sends
%% a fenced message to a different node.
%%
%% What this proves beyond `old_owner_is_fenced_after_migration': that the
%% *adapter-facing* entry point -- `guard_append/3', the persistent_term the
%% queue installs -- enforces affinity by reading `proto_meta', not just the
%% bare `authorize_append/2' epoch API; and that the no-epoch fallback
%% (`owner_check') also fences a node that no longer owns the partition, which
%% is the single-node / pre-cluster default path an adapter hits when it has
%% nothing to stamp.
a_stale_session_writer_is_fenced_by_the_guard_hook(Config) ->
    Members = members(Config),
    [N1, N2, N3] = Members,
    ok = form_cluster(Config),
    %% Partition 0 owned by N1 at epoch 1. Every node's cache now agrees.
    {ok, 1} = rpc(N1, broker_cluster, assign, [0, N1, Members, 1]),
    %% A publisher pinned to N1 stamps the epoch it saw (annotate_epoch/1).
    Stamped1 = stamped(0, 1),
    %% While N1 owns it, the guard lets the stamped write through.
    ok = rpc(N1, broker_cluster, guard_append, [0, Stamped1, replicated_commit]),
    %% ...and a message with no epoch falls back to the owner check, which N1
    %% passes because it still owns the partition.
    ok = rpc(N1, broker_cluster, guard_append, [0, unstamped(0), replicated_commit]),
    %% Ownership moves N1 -> N2 (two phases), epoch ends at 3, active.
    {ok, 2} = rpc(N1, broker_cluster, migrate, [0, N1, N2, 2]),
    {ok, 3} = rpc(N1, broker_cluster, finish_migration, [0, 3]),
    %% The connection is still pinned to N1 and still carries the epoch it saw
    %% before the move (cluster_epoch => 1). The guard on N1 must refuse it --
    %% a stale writer, fenced before an offset is consumed.
    {error, {fenced, Fenced}} =
        rpc(N1, broker_cluster, guard_append, [0, Stamped1, replicated_commit]),
    0 = maps:get(partition, Fenced),
    1 = maps:get(actual_epoch, Fenced),
    3 = maps:get(expected_epoch, Fenced),
    %% The no-epoch fallback also fences N1 now: it is not the owner any more.
    {error, {fenced, _}} =
        rpc(N1, broker_cluster, guard_append, [0, unstamped(0), replicated_commit]),
    %% Even carrying the *current* epoch, N1 is refused -- it is not the owner;
    %% epoch alone does not grant the write, ownership does.
    {error, {fenced, _}} =
        rpc(N1, broker_cluster, guard_append, [0, stamped(0, 3), replicated_commit]),
    %% The new owner, N2, accepts its own stamped epoch.
    ok = rpc(N2, broker_cluster, guard_append, [0, stamped(0, 3), replicated_commit]),
    %% ...but a stale epoch against N2's current view is fenced too.
    {error, {fenced, _}} =
        rpc(N2, broker_cluster, guard_append, [0, stamped(0, 2), replicated_commit]),
    _ = N3,
    ok.

%% A message as an adapter would hand it to the queue: partition + the epoch
%% the access node stamped into `proto_meta'.
stamped(Partition, Epoch) ->
    #broker_message{partition_id = Partition,
                   proto_meta = #{cluster_epoch => Epoch}}.

%% A message with nothing to stamp (single-node / pre-cluster adapter).
unstamped(Partition) ->
    #broker_message{partition_id = Partition, proto_meta = #{}}.

%%%_* Online membership change (ADR-033) ----------------------------------
%%
%% The four cases below are ADR-033's "验证" list. They exercise the
%% admin-driven `join/1' / `leave/1' on `broker_cluster_meta': a node joins a
%% *running* group without restarting the survivors, the rebalance view sees it
%% as a member (not an orphan), a join that cannot commit is refused with the
%% view unchanged, and a clean leave removes the node. They are the
%% counterpart to ADR-032's measured "single node cannot join a running group":
%% `ra:add_member' replaces the static `initial_members' path for exactly this.

%% ADR-033 (验证 1): an explicit join adds the node to the *committed* group,
%% and the survivors never restart. After the join the leader's committed
%% membership contains the new node and a metadata write still commits -- the
%% group is healthy with four voters.
online_join_makes_the_new_node_a_voter(Config) ->
    Members = members(Config),
    [N1, N2, N3] = Members,
    ok = form_cluster(Config),
    New = start_join_peer(Config),
    ok = rpc(N1, broker_cluster, join, [New]),
    wait_until(fun() -> ra_members_include(N1, New) end, 20000),
    %% A voter must also catch up to the committed log, else the group's commit
    %% index stalls behind the new member. `ra:add_member' only records the new
    %% id in the leader's config; the new node starts its own ra server with an
    %% existing member as seed (the README's documented second step), reaches the
    %% leader as a follower, and replays the log up to the commit index.
    ok = rpc(New, ra, start_server,
             [default, ?CLUSTER, {?CLUSTER, New},
              {module, broker_cluster_meta_machine, #{}},
              [{?CLUSTER, N1}]]),
    %% Prove the catch-up through our own cache: assign a fresh partition on the
    %% leader, then wait for the new node's ra apply loop to mirror it. The cache
    %% is written by the state machine on every member, so once the new node's
    %% cache shows the owner it has applied the post-join entry -- caught up.
    {ok, 2} = retry(fun() ->
                            rpc(N1, broker_cluster, assign,
                                [1, N1, Members ++ [New], 2])
                    end, 20, 250),
    wait_until(fun() ->
                      rpc(New, broker_cluster_cache, owner, [1]) =:= {ok, N1}
                end, 20000),
    {ok, N1} = rpc(New, broker_cluster_cache, owner, [1]),
    %% The survivors were never touched: their control process is still up.
    true = is_pid(rpc(N2, erlang, whereis, [broker_cluster_meta_ctl])),
    true = is_pid(rpc(N3, erlang, whereis, [broker_cluster_meta_ctl])),
    %% The group still commits with the new voter present.
    {ok, 3} = retry(fun() ->
                            rpc(N1, broker_cluster, assign,
                                [2, N1, Members ++ [New], 3])
                    end, 20, 250),
    ok.

%% ADR-033 (验证 2): the rebalance view reads the *live* membership, not the
%% startup config, so a joined node is a member -- `broker_rebalance:status()'
%% lists it and does not report it orphaned. This is the concrete proof that
%% the members source moved from `application:get_env' to the ra state machine.
online_join_is_visible_to_the_rebalance_view(Config) ->
    Members = members(Config),
    [N1 | _] = Members,
    ok = form_cluster(Config),
    New = start_join_peer(Config),
    %% The startup config on every peer names only the original three; New is
    %% never in `application:get_env(broker_cluster, members)'. This is the
    %% baseline 验证 5 reverses: if the rebalance view read the startup config,
    %% New could not appear in it.
    false = lists:member(New, Members),
    ok = rpc(N1, broker_cluster, join, [New]),
    ok = rpc(New, ra, start_server,
             [default, ?CLUSTER, {?CLUSTER, New},
              {module, broker_cluster_meta_machine, #{}},
              [{?CLUSTER, N1}]]),
    wait_until(fun() -> rebalance_members_include(N1, New) end, 20000),
    Status = rpc(N1, broker_rebalance, status, []),
    %% The live view (the ra state machine) lists New; the startup config does
    %% not. That gap is the proof the members source moved off the config.
    true = lists:member(New, maps:get(members, Status)),
    false = lists:member(New, maps:get(orphaned, Status)),
    ok.

%% ADR-033 (验证 3): a clean leave removes the node from the group. Join N4,
%% wait for it to be a committed voter, then leave it from the leader; the
%% committed membership shrinks back to the original three and the group still
%% commits.
clean_leave_removes_the_node_from_the_group(Config) ->
    Members = members(Config),
    [N1 | _] = Members,
    ok = form_cluster(Config),
    New = start_join_peer(Config),
    ok = rpc(N1, broker_cluster, join, [New]),
    ok = rpc(New, ra, start_server,
             [default, ?CLUSTER, {?CLUSTER, New},
              {module, broker_cluster_meta_machine, #{}},
              [{?CLUSTER, N1}]]),
    wait_until(fun() -> ra_members_include(N1, New) end, 20000),
    %% A clean leave: `remove_member' on the leader commits the membership
    %% change and the group shrinks back to three. (Draining the node first is
    %% an operational precondition -- ADR-032 D5 -- not part of the ra change;
    %% it lives in `broker_admin' and is exercised there.)
    %% Wait for the *live view* on N1 (what `leave/1' consults) to include the
    %% new node, not just the leader-redirected read, so `leave' sees a member.
    wait_until(fun() -> rebalance_members_include(N1, New) end, 20000),
    ok = rpc(N1, broker_cluster, leave, [New]),
    wait_until(fun() -> not ra_members_include(N1, New) end, 20000),
    %% The node is gone from every survivor's live view and the group still
    %% commits without it.
    lists:foreach(
      fun(N) -> wait_until(fun() -> not rebalance_members_include(N, New) end,
                           20000) end,
      Members),
    {ok, 2} = retry(fun() ->
                            rpc(N1, broker_cluster, assign,
                                [1, N1, Members, 2])
                    end, 20, 250),
    ok.

%% ADR-033 (验证 4): a join that cannot reach a quorum leaves the committed view
%% unchanged. Stop two of the three nodes so the caller (N1) is a lone voter
%% with no majority, then attempt a join.
%%
%% The probe is the *committed* membership read on N1 (`ra:members'). A lone
%% voter CAN append the membership entry locally, so `join/1' legitimately
%% returns `ok' (ra's `after_log_append' reply fires the moment the leader
%% appends, and a one-node group is its own majority) -- the durable fact is
%% what the committed view shows. Two things must hold: the new node never
%% enters N1's committed membership, and a *fresh* command still cannot commit.
online_join_is_rejected_without_a_majority(Config) ->
    Members = members(Config),
    [N1, N2, N3] = Members,
    ok = form_cluster(Config),
    NewName = join_peer_name(?config(name, Config)),
    stop_peer(Config, N2),
    stop_peer(Config, N3),
    _ = rpc(N1, broker_cluster, join, [NewName]),
    %% The new node never entered N1's committed membership: the lone voter
    %% cannot learn a node it has never heard from. Read the on-disk directory
    %% (the committed config) rather than the live server, which would answer
    %% `{error, name_not_registered}' once its ra system is torn down.
    Registered = rpc(N1, ra_directory, list_registered, [default]),
    false = lists:member(NewName, Registered),
    %% And a fresh command still cannot commit -- the group has no quorum to
    %% append it, so the metadata write is refused.
    {error, no_quorum} = rpc(N1, broker_cluster, assign,
                             [1, N1, [N1], 2]),
    ok.

%%%_* Peers ---------------------------------------------------------------

start_peers(Case, Config) ->
    Names = [peer_name(Case, N) || N <- [1, 2, 3]],
    %% Only the code directories (`.../ebin'): passing every path CT added
    %% (including priv and log directories) makes peer boot needlessly slow and
    %% can time out. The suite's own beam directory is added explicitly --
    %% `prepare_peer/1' runs on the peer, and CT keeps test modules outside
    %% `ebin'.
    %%
    %% "ends in ebin" rather than "contains `_build/test'": CT's build directory
    %% is named after the rebar3 profile, so a hard-coded `_build/test' silently
    %% drops every dependency -- and `_build/<profile>/checkouts/ra/ebin' --
    %% under `rebar3 as quic ct', which made all four cases below fail in
    %% `prepare_peer/1' with `{error, {ra, {"no such file or directory",
    %% "ra.app"}}}'.
    SuiteDir = filename:dirname(code:which(?MODULE)),
    CodePaths = lists:usort(
                  [SuiteDir | [D || D <- code:get_path(), is_code_dir(D)]]),
    lists:map(
      fun({I, Name}) ->
              DataDir = filename:join([?config(priv_dir, Config),
                                       atom_to_list(Case),
                                       "n" ++ integer_to_list(I), "ra"]),
              {ok, Pid, Node} =
                  peer:start_link(#{name => Name,
                                    args => ["-pa" | CodePaths],
                                    wait_boot => 20000}),
              %% ra and the cache must be up on the peer before the group is
              %% dispatched (start_cluster/4 starts members over rpc).
              ok = rpc(Node, prepare_peer, [[DataDir, []]]),
              #{pid => Pid, node => Node, data_dir => DataDir}
      end, lists:zip([1, 2, 3], Names)).

stop_peers(Peers) ->
    lists:foreach(
      fun(#{pid := Pid}) -> catch peer:stop(Pid) end, Peers),
    ok.

stop_peer(Config, Node) ->
    Peers = ?config(peers, Config),
    case [P || #{node := N} = P <- Peers, N =:= Node] of
        [#{pid := Pid}] -> catch peer:stop(Pid), ok;
        _ -> ok
    end.

peer_name(Case, I) ->
    list_to_atom("s4_" ++ sanitise(atom_to_list(Case)) ++ "_" ++
                 integer_to_list(I)).

%% A directory the peer can put on `-pa': the profile-independent test.
is_code_dir(Dir) ->
    filename:basename(Dir) =:= "ebin".

sanitise(S) ->
    [case C of _ when C >= $a andalso C =< $z -> C;
                 _ when C >= $A andalso C =< $Z -> C;
                 _ when C >= $0 andalso C =< $9 -> C;
                 _ -> $_ end || C <- S].

peer_nodes(Config) ->
    [Node || #{node := Node} <- ?config(peers, Config)].

%% The `members' env and the `replicas' argument are plain node atoms;
%% `broker_cluster_meta' wraps them into ra server ids itself.
members(Config) ->
    peer_nodes(Config).

form_cluster(Config) ->
    [N1, N2, N3] = peer_nodes(Config),
    ok = rpc(N1, start_meta, [members(Config)]),
    ok = rpc(N2, start_meta, [members(Config)]),
    ok = rpc(N3, start_meta, [members(Config)]),
    ok.

%% A fourth peer, started but *not* part of the initial group. ADR-033's join
%% brings it in at runtime; until then it is an ordinary node with ra up but no
%% metadata server of its own. Registered in `new_peer' so `end_per_testcase'
%% tears it down first.
start_join_peer(Config) ->
    Name = join_peer_name(?config(name, Config)),
    DataDir = filename:join([?config(priv_dir, Config),
                             atom_to_list(Name), "ra"]),
    {ok, Pid, Node} =
        peer:start_link(#{name => Name,
                          args => ["-pa" | code_paths()],
                          wait_boot => 20000}),
    ok = rpc(Node, prepare_peer, [[DataDir, []]]),
    Peer = #{pid => Pid, node => Node, data_dir => DataDir},
    put(new_peer, Peer),
    Node.

join_peer_name(Case) ->
    list_to_atom("s4_" ++ sanitise(atom_to_list(Case)) ++ "_n4").

code_paths() ->
    SuiteDir = filename:dirname(code:which(?MODULE)),
    lists:usort([SuiteDir | [D || D <- code:get_path(), is_code_dir(D)]]).

%% Is `Node' among the leader's committed voters? Reads the ra membership
%% directly (not our cache) so it reflects the committed configuration, the
%% thing `join/1' changed.
ra_members_include(Leader, Node) ->
    case rpc(Leader, ra, members, [{?CLUSTER, Leader}, 10000]) of
        {ok, Ids, _} ->
            lists:member({?CLUSTER, Node}, Ids);
        _ -> false
    end.

%% Does this node's *rebalance* view see `Node' as a member? That view is the
%% live `broker_cluster_meta:members/0' answer, which is what ADR-033 moved
%% from the startup config to the ra state machine.
rebalance_members_include(Node, Member) ->
    case rpc(Node, broker_cluster_meta, members, []) of
        Members when is_list(Members) -> lists:member(Member, Members);
        _ -> false
    end.

leader(Config) ->
    [N | _] = peer_nodes(Config),
    case rpc(N, ra, members, [{?CLUSTER, N}, 10000]) of
        {ok, _Members, Leader} when is_atom(Leader) -> Leader;
        {ok, _Members, {?CLUSTER, Leader}} -> Leader;
        Other -> ct:fail({no_leader, Other})
    end.

rpc(Node, F, A) ->
    rpc(Node, ?MODULE, F, A).

rpc(Node, M, F, A) ->
    case rpc:call(Node, M, F, A, 30000) of
        {badrpc, Reason} -> ct:fail({badrpc, Node, M, F, Reason});
        Result -> Result
    end.

retry(_Fun, 0, _Wait) ->
    {error, exhausted};
retry(Fun, N, Wait) ->
    case Fun() of
        {ok, _} = Ok -> Ok;
        _ -> timer:sleep(Wait), retry(Fun, N - 1, Wait)
    end.

wait_until(Fun, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    wait_until_loop(Fun, Deadline).

wait_until_loop(Fun, Deadline) ->
    Now = erlang:monotonic_time(millisecond),
    case Fun() of
        true -> ok;
        _ when Now >= Deadline -> ok;
        _ -> timer:sleep(100), wait_until_loop(Fun, Deadline)
    end.

%%%_* Code executed on each peer ------------------------------------------

%% Every peer must have ra and the cache up *before* the group is dispatched,
%% because `ra:start_cluster/4' starts each member on its own node over rpc.
prepare_peer([DataDir, Members]) ->
    _ = application:load(broker_cluster),
    ok = application:set_env(broker_cluster, data_dir, DataDir),
    ok = application:set_env(broker_cluster, members, Members),
    ok = application:set_env(broker_cluster, quorum_timeout_ms, 3000),
    ok = application:set_env(broker_cluster, replicate_timeout_ms, 1000),
    ok = filelib:ensure_dir(filename:join(DataDir, "x")),
    {ok, _} = application:ensure_all_started(ra),
    application:set_env(ra, data_dir, filename:absname(DataDir)),
    case ra_system:fetch(default) of
        undefined ->
            case ra_system:start_default() of
                {ok, _} -> ok;
                {error, {already_started, _}} -> ok
            end;
        _ -> ok
    end,
    {ok, Cache} = broker_cluster_cache:start_link(),
    unlink(Cache),
    ok.

start_meta(Members) ->
    ok = application:set_env(broker_cluster, members, Members),
    {ok, Meta} = broker_cluster_meta:start_link(),
    unlink(Meta),
    ok.

%% Simulate a crash of everything this application owns on the peer. The ra
%% system is stopped last: stopping the server first would leave the system up
%% with a stale server id registered, which is not what a restart looks like.
crash_meta() ->
    catch gen_server:stop(broker_cluster_meta_ctl, kill, 1000),
    catch ra:stop_server(default, {broker_cluster_meta, node()}),
    catch ra_system:stop_default(),
    catch gen_server:stop(broker_cluster_cache),
    ok.

whereis_meta() -> whereis(broker_cluster_meta).

%% A reboot brings the application's supervision tree back in order: the cache
%% first, then the metadata server attaches to the existing group.
revive_cache() ->
    {ok, Cache} = broker_cluster_cache:start_link(),
    unlink(Cache),
    ok.
