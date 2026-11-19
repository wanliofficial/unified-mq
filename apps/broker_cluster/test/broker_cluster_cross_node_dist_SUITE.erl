%% @doc S12-D (ADR-034) end-to-end semantics: `owner 权威日志 + 跨节点读写路由'.
%%
%% Two real peer nodes run the data-plane apps (storage / router / queue /
%% subscription) and share an ownership mirror (`broker_cluster_cache') that both
%% agree on. The routing hook itself is the one `broker_cluster' installs
%% (`install_hooks/0' with `cross_node_delivery = true'), so these cases fail if
%% `broker_queue' ever stops asking the hook -- which is the whole point of
%% exercising it through the real facade rather than through a private stub.
%%
%% Six claims, one case each, matching ADR-034 §"验证":
%%
%%   1. a publish from a non-owner writes only on the owner (D2: no local copy);
%%   2. a forwarded call that lands on a node that is not the owner is refused,
%%      and the refusal is counted as fencing (D2 depth);
%%   3. a subscribe from a non-owner is answered by the owner's worker, a wake
%%      reaches the remote subscriber, and a dead remote subscriber is dropped
%%      from the owner's `subs' (D3: no new wake mechanism, the monitor does it);
%%   4. a shared group joining on a non-owner is refused while an exclusive
%%      subscription is not (D5: no silent duplicate delivery);
%%   5. `committed' is the minimum over the nodes' candidates and a node leaving
%%      releases its own (D6);
%%   6. with the switch off the same append is refused by the fencing guard and
%%      nothing is written locally (D8: byte-for-byte single-node behaviour).
%%
%% Every port is ephemeral and read back; no project port is touched. `peer' is
%% OTP stdlib, same machinery the other dist suites use, so the peers are
%% contacted by their real names over the distribution.
-module(broker_cluster_cross_node_dist_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("broker_core/include/broker_message.hrl").

-define(PARTITIONS, 2).
-define(NODES, 2).

all() ->
    [a_publish_on_a_non_owner_writes_only_on_the_owner,
     a_forwarded_call_on_a_non_owner_is_refused_and_fenced,
     a_cross_node_subscribe_wakes_a_remote_subscriber_whose_death_cleans_up,
     a_shared_group_is_refused_across_nodes_while_an_exclusive_one_is_not,
     committed_is_the_minimum_over_the_nodes_and_a_leave_releases_it,
     a_qos0_publish_on_a_non_owner_reaches_a_subscriber_on_another_member,
     with_routing_off_a_non_owner_write_is_refused_and_writes_nothing].

%%%_* Fixtures --------------------------------------------------------------

init_per_suite(Config) ->
    case node() of
        nonode@nohost ->
            %% Same distribution requirement as `broker_cluster_dist_SUITE':
            %% CT runs un-named by default, and a peer with a name is contacted
            %% over the distribution. See the note there about not re-applying
            %% `nocookie' after `net_kernel:start/1'.
            case net_kernel:start([unifiedmq_s12d_ct, shortnames]) of
                {ok, _} -> [{started_net, true} | Config];
                {error, Reason} ->
                    ct:fail({cannot_start_distribution, check_epmd, Reason})
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
    Nodes = [maps:get(node, P) || P <- Peers],
    [N1, N2] = Nodes,
    %% Both peers share one ownership view: `Owner' owns every partition. They
    %% also share a membership view for the QoS0 fan-out (D4): the env is the
    %% fallback `broker_cluster_meta:members/0' uses when the control process is
    %% not running, which is the case for these storage-only peers.
    ok = rpc(N1, application, set_env, [broker_cluster, members, Nodes]),
    ok = rpc(N2, application, set_env, [broker_cluster, members, Nodes]),
    ok = rpc(N1, ?MODULE, set_owner, [0, N2, Nodes]),
    ok = rpc(N2, ?MODULE, set_owner, [0, N2, Nodes]),
    [{peers, Peers}, {nodes, Nodes}, {owner, N2}, {non_owner, N1} | Config].

end_per_testcase(_Case, Config) ->
    stop_peers(?config(peers, Config)),
    ok.

%%%_* Cases ----------------------------------------------------------------

%% D2, validation #1: the write happens on the owner and nowhere else. Reading
%% the non-owner through its *own* storage handle -- not through `broker_queue',
%% which would faithfully forward and hide the local log -- is what makes "no
%% local copy" a statement about the disk rather than about the routing.
a_publish_on_a_non_owner_writes_only_on_the_owner(Config) ->
    N1 = ?config(non_owner, Config),
    N2 = ?config(owner, Config),
    {ok, 1} = rpc(N1, broker_queue, append, [0, msg(0, <<"from-n1">>), fsync]),
    %% The owner's log has it.
    ?assertEqual({ok, 0, 2}, local_watermarks(N2, 0)),
    ?assertEqual([{1, <<"from-n1">>}],
                 payloads(rpc(N2, broker_queue, read_from, [0, 1, 10]))),
    %% The non-owner's log does not: not the offsets, not the bytes.
    ?assertEqual({ok, 0, 1}, local_watermarks(N1, 0)),
    ?assertEqual({ok, [], 1}, local_page(N1, 0, 1, 10)),
    %% And reading through the facade on N1 *does* reach it -- that is the same
    %% call, answered from N2 because of the hook.
    ?assertEqual([{1, <<"from-n1">>}],
                 payloads(rpc(N1, broker_queue, read_from, [0, 1, 10]))),
    ?assertEqual({ok, 0, 2}, rpc(N1, broker_queue, watermarks, [0])),
    ok.

%% D2's fencing depth: a hook whose view has gone stale answers `{forward, N}'
%% while N is no longer the owner. If the forwarding layer trusted it, two nodes
%% would both write; instead the stale owner says `{not_owner, _}' and the call
%% is counted as a fencing rejection.
a_forwarded_call_on_a_non_owner_is_refused_and_fenced(Config) ->
    N1 = ?config(non_owner, Config),
    N2 = ?config(owner, Config),
    Nodes = ?config(nodes, Config),
    %% N2 now believes N1 owns it, but a call forwarded here must not be served.
    ok = rpc(N2, ?MODULE, set_owner, [0, N1, Nodes]),
    Before = fencing_rejected(N2),
    ?assertEqual({error, {not_owner, N1}},
                 rpc(N2, broker_queue, forwarded, [read_from, 0, [1, 10]])),
    After = fencing_rejected(N2),
    ?assertEqual(Before + 1, After),
    %% Restore the view: the flag is cleared per call, so a stale-owner refusal
    %% must not have poisoned anything for the next routed call.
    ok = rpc(N2, ?MODULE, set_owner, [0, N2, Nodes]),
    ?assertEqual({ok, 0, 1}, rpc(N2, broker_queue, watermarks, [0])),
    ok.

%% D3's wake half. The owner's partition worker holds the *remote* pid in `subs'
%% and monitors it, so a wake is delivered by an ordinary `!' and a crash is
%% handled by the existing `DOWN' clause -- no new mechanism, no subscriber index.
a_cross_node_subscribe_wakes_a_remote_subscriber_whose_death_cleans_up(Config) ->
    N1 = ?config(non_owner, Config),
    N2 = ?config(owner, Config),
    Parent = self(),
    Sub = rpc(N1, erlang, spawn, [?MODULE, subscriber, [0, Parent]]),
    ok = wait_until(fun() -> length(subs(N2, 0)) =:= 1 end),
    {ok, 1} = rpc(N2, broker_queue, append, [0, msg(0, <<"wake">>), fsync]),
    ?assertMatch({wake, 1, _}, receive_wake(5000)),
    %% Death on N1 shows up as a DOWN on N2 and the entry goes away.
    true = rpc(N1, erlang, exit, [Sub, kill]),
    ok = wait_until(fun() -> subs(N2, 0) =:= [] end),
    %% The log survives its only subscriber (it never acked, so committed is
    %% still 0; the point is the append did not depend on it).
    ?assertEqual({ok, 0, 2}, local_watermarks(N2, 0)),
    ok.

%% D5: `claim/4' hands out offsets from a worker-local row, so a shared group
%% joining from two nodes would each allocate the same offset -- a duplicate
%% that is silent. Refuse it instead. Exclusive has one reader and a
%% node-local cursor, so it is fine across nodes.
a_shared_group_is_refused_across_nodes_while_an_exclusive_one_is_not(Config) ->
    N1 = ?config(non_owner, Config),
    N2 = ?config(owner, Config),
    ?assertEqual({error, {cross_node_shared_unsupported, 0}},
                 rpc(N1, broker_subscription, join,
                     [shared, <<"grp">>, <<"c1">>, <<"f">>, 1])),
    %% Refusing on the subscribe path leaves no entity behind for that consumer.
    ?assertEqual([], rpc(N1, broker_subscription, subscriptions, [<<"c1">>])),
    %% The owner itself is allowed to hold it.
    ?assertMatch({ok, {shared, <<"grp">>, <<"f2">>}},
                 rpc(N2, broker_subscription, join,
                     [shared, <<"grp">>, <<"c2">>, <<"f2">>, 1])),
    %% And an exclusive subscription from N1 is not refused: its cursor stays on
    %% N1 and its reads route to the owner.
    ?assertMatch({ok, {exclusive, <<"c3">>, <<"f3">>}},
                 rpc(N1, broker_subscription, subscribe,
                     [<<"c3">>, <<"f3">>, 1])),
    %% The refused join did not disturb the partition set for a later,
    %% legitimate one either.
    ?assertMatch([_], rpc(N2, broker_subscription, subscriptions, [<<"c2">>])),
    ok.

%% D6: `committed' is a minimum, never a maximum, and the storage layer refuses to
%% move it backwards -- so the aggregation has to happen *before* the apply. The
%% case that would lose data is a fast owner-side consumer reporting 3 while a
%% slow remote node is at 1: applying 3 directly would let GC delete the slow
%% node's unread segment. The floor keeps it at 1; when the slow node leaves, the
%% floor is free to move.
committed_is_the_minimum_over_the_nodes_and_a_leave_releases_it(Config) ->
    N1 = ?config(non_owner, Config),
    N2 = ?config(owner, Config),
    %% Three messages exist on the owner only.
    [{ok, _} = rpc(N2, broker_queue, append, [0, msg(0, Payload), fsync])
     || Payload <- [<<"a">>, <<"b">>, <<"c">>]],
    %% The slow node (N1) reports first: it is the only reporter, so it applies.
    ok = rpc(N1, broker_queue, advance_committed, [0, 1]),
    ?assertEqual({ok, 1, 4}, local_watermarks(N2, 0)),
    %% The fast owner-side consumer reports 3. Without aggregation this would push
    %% committed to 3 and GC the segment N1 has not read; the floor holds it at 1.
    ok = rpc(N2, broker_queue, advance_committed, [0, 3]),
    ?assertEqual({ok, 1, 4}, local_watermarks(N2, 0)),
    %% N1 leaves: its row goes away and only N2's candidate remains, so the floor
    %% is free to move to 3.
    ok = rpc(N2, broker_cluster_cache, forget_committed, [N1]),
    ok = rpc(N2, broker_queue, advance_committed, [0, 3]),
    ?assertEqual({ok, 3, 4}, local_watermarks(N2, 0)),
    %% The mirror kept only the surviving node's row.
    ?assertEqual(3, rpc(N2, broker_cluster_cache, committed_floor, [0])),
    ok.

%% D8's default: the switch off means no router hook at all. The append is
%% refused by the fencing guard, and the non-owner's log is untouched -- the
%% pre-ADR-034 behaviour, not a new failure mode.
with_routing_off_a_non_owner_write_is_refused_and_writes_nothing(Config) ->
    N1 = ?config(non_owner, Config),
    Nodes = ?config(nodes, Config),
    %% Reinstall the hooks with routing off: guard in, router out.
    lists:foreach(fun(N) -> ok = rpc(N, broker_cluster, reinstall_hooks, [false]) end, Nodes),
    ?assertMatch({error, {fenced, _}},
                 rpc(N1, broker_queue, append, [0, msg(0, <<"no-write">>), fsync])),
    ?assertEqual({ok, 0, 1}, local_watermarks(N1, 0)),
    ?assertEqual({ok, [], 1}, local_page(N1, 0, 1, 10)),
    ok.

%% D4's whole point: a QoS0 publish on one node reaches a subscriber attached on
%% another. QoS0 never enters the log, so there is no owner to pull it from -- the
%% message is cast to every member and each re-routes it against its *own* index.
%% The publisher's local push is empty here (the subscriber is not on N2), which
%% is what makes the cross-node hop load-bearing rather than incidental. Neither
%% side writes anything: QoS0 has no offset to consume.
a_qos0_publish_on_a_non_owner_reaches_a_subscriber_on_another_member(Config) ->
    N1 = ?config(non_owner, Config),
    N2 = ?config(owner, Config),
    Topic = <<"sports/tennis/+">>,
    Parent = self(),
    Sub = rpc(N1, erlang, spawn, [?MODULE, qos0_subscriber, [Topic, Parent]]),
    ?assertEqual({attached, Sub}, receive_attached(5000)),
    ok = rpc(N2, broker_subscription, dispatch,
             [[], {undefined, undefined, qos0_msg(Topic)}]),
    ?assertEqual({delivered, Sub}, receive_d(5000)),
    %% QoS0 never enters the log on either side: nothing to own, nothing to pull.
    ?assertEqual({ok, 0, 1}, local_watermarks(N1, 0)),
    ?assertEqual({ok, 0, 1}, local_watermarks(N2, 0)),
    true = rpc(N1, erlang, exit, [Sub, kill]),
    ok.

%%%_* Peers ----------------------------------------------------------------

start_peers(Case, Config) ->
    Names = [peer_name(Case, I) || I <- lists:seq(1, ?NODES)],
    SuiteDir = filename:dirname(code:which(?MODULE)),
    CodePaths = lists:usort([SuiteDir | [D || D <- code:get_path(), is_code_dir(D)]]),
    lists:reverse(
      lists:foldl(
        fun({I, Name}, Acc) ->
              DataDir = filename:join([?config(priv_dir, Config),
                                       atom_to_list(Case),
                                       "n" ++ integer_to_list(I), "ra"]),
              try
                  {ok, Pid, Node} = peer:start_link(
                                      #{name => Name,
                                        args => ["-pa" | CodePaths],
                                        wait_boot => 20000}),
                  ok = rpc(Node, prepare_peer, [DataDir]),
                  ok = rpc(Node, start_data_plane, [true]),
                  [#{pid => Pid, node => Node, data_dir => DataDir} | Acc]
              catch
                  Class:Reason:Stack ->
                      stop_peers(Acc),
                      erlang:raise(Class, Reason, Stack)
              end
        end, [], lists:zip(lists:seq(1, ?NODES), Names))).

stop_peers(Peers) ->
    lists:foreach(fun(#{pid := Pid}) -> catch peer:stop(Pid) end, Peers),
    ok.

peer_name(Case, I) ->
    list_to_atom("s12d_" ++ sanitise(atom_to_list(Case)) ++ "_" ++ integer_to_list(I)).

is_code_dir(Dir) ->
    filename:basename(Dir) =:= "ebin".

sanitise(S) ->
    [case C of _ when C >= $a andalso C =< $z -> C;
                 _ when C >= $A andalso C =< $Z -> C;
                 _ when C >= $0 andalso C =< $9 -> C;
                 _ -> $_ end || C <- S].

%%%_* Peer-side setup (runs on the peer) ----------------------------------

prepare_peer(DataDir) ->
    _ = application:load(broker_cluster),
    ok = application:set_env(broker_cluster, data_dir, DataDir),
    ok = application:set_env(broker_cluster, members, []),
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

%% The data plane without any listener: storage, router, queue, subscription, and
%% the hooks `broker_cluster' installs. No port is opened, so no project port is
%% in play even though the peers are distributed.
start_data_plane(Routing) ->
    {ok, RaDir} = application:get_env(broker_cluster, data_dir),
    DataDir = filename:join(RaDir, "data"),
    ok = filelib:ensure_dir(filename:join([DataDir, "x"])),
    _ = [application:load(A)
         || A <- [broker_metrics, broker_core, broker_storage, broker_router,
                  broker_queue, broker_subscription, broker_cluster]],
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok = application:set_env(broker_storage, data_dir, DataDir),
    ok = application:set_env(broker_storage, wal, #{require_durable_fs => false}),
    ok = application:set_env(broker_queue, partitions, ?PARTITIONS),
    ok = application:set_env(broker_router, partitions, ?PARTITIONS),
    ok = application:set_env(broker_subscription, data_dir, DataDir),
    {ok, _} = application:ensure_all_started(broker_storage),
    [ok = broker_queue:create_partition(P) || P <- broker_queue:partition_ids()],
    {ok, _} = application:ensure_all_started(broker_router),
    {ok, _} = application:ensure_all_started(broker_queue),
    {ok, _} = application:ensure_all_started(broker_subscription),
    %% The routing hooks exist only with the switch on; `install_hooks/0' always
    %% installs the fencing guard, which is what case 6 leans on.
    ok = application:set_env(broker_cluster, cross_node_delivery, Routing),
    ok = application:set_env(broker_cluster, cross_node_timeout_ms, 2000),
    ok = broker_cluster:install_hooks(),
    ok.

%% Runs on one peer: that peer's cache only. The test calls it on every peer so
%% both mirrors agree.
set_owner(Partition, Owner, Nodes) ->
    ok = broker_cluster_cache:put(Partition, #{partition => Partition,
                                               owner => Owner,
                                               epoch => 1,
                                               replicas => Nodes,
                                               state => active}),
    ok.

%% @private Called on the non-owner: subscribe to the partition, tell the parent
%% about the first wake, then wait. The test kills the process from outside.
subscriber(Partition, Parent) ->
    ok = broker_queue:subscribe(Partition, self()),
    receive
        {new_messages, Partition, From, To} -> Parent ! {wake, From, To}
    after 5000 ->
        Parent ! {no_wake, self()}
    end.

%% @private Called on a peer (N1): subscribe exclusively to `Topic', attach this
%% process as the delivery target, then report that it is ready and wait for the
%% cross-node QoS0 push. The `{broker_deliver, ...}' frame is what
%% `dispatch_local/2' sends when `dispatch_qos0/1' re-routes the message here.
qos0_subscriber(Topic, Parent) ->
    {ok, _SubRef} = broker_subscription:subscribe(<<"cB">>, Topic, 0),
    {ok, _Prev} = broker_subscription:attach(<<"cB">>, self(), true),
    Parent ! {attached, self()},
    receive
        {broker_deliver, <<"cB">>, _QoS, _Refs, {undefined, undefined, _Msg}} ->
            Parent ! {delivered, self()};
        Other ->
            Parent ! {unexpected, self(), Other}
    after 5000 ->
        Parent ! {no_delivery, self()}
    end.

%%%_* Owner-side introspection (runs on the peer) ---------------------------

%% Read the partition's own storage, bypassing `broker_queue' so a routed read on
%% the non-owner cannot be used to prove anything about the non-owner's log.
local_entry(Node, Partition) ->
    rpc(Node, broker_queue_store, lookup, [Partition]).

local_watermarks(Node, Partition) ->
    {ok, Entry} = local_entry(Node, Partition),
    Backend = maps:get(backend, Entry),
    Handle = maps:get(handle, Entry),
    rpc(Node, Backend, watermarks, [Handle]).

local_page(Node, Partition, From, Limit) ->
    {ok, Entry} = local_entry(Node, Partition),
    Backend = maps:get(backend, Entry),
    Handle = maps:get(handle, Entry),
    rpc(Node, Backend, read_from, [Handle, From, Limit]).

subs(Node, Partition) ->
    {ok, Entry} = local_entry(Node, Partition),
    Worker = maps:get(worker, Entry),
    #{subs := Subs} = rpc(Node, sys, get_state, [Worker]),
    Subs.

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
                    expires_at = undefined}.

payloads({ok, Page, _Next}) ->
    [{Off, M#broker_message.payload} || {Off, M} <- Page].

%% A QoS0 message addressed to `Topic' -- the payload is irrelevant, only the
%% routing keys (destination / tenant) that `dispatch_qos0/1' re-resolves matter.
qos0_msg(Topic) ->
    #broker_message{message_id = <<"q">>,
                    tenant_id = <<"t">>,
                    destination = Topic,
                    partition_id = 0,
                    payload = <<"qos0">>,
                    payload_encoding = raw,
                    headers = #{},
                    proto_meta = #{},
                    persistence = durable,
                    reliability = best_effort,
                    created_at = 1,
                    expires_at = undefined}.

receive_attached(Timeout) ->
    receive
        {attached, P} -> {attached, P};
        Other -> {unexpected, Other}
    after Timeout ->
        timeout
    end.

receive_d(Timeout) ->
    receive
        {delivered, P} -> {delivered, P};
        {no_delivery, P} -> {no_delivery, P};
        {unexpected, P, _} -> {unexpected, P};
        Other -> {unexpected_frame, Other}
    after Timeout ->
        timeout
    end.

receive_wake(Timeout) ->
    receive
        {wake, From, To} -> {wake, From, To};
        Other -> {unexpected, Other}
    after Timeout ->
        timeout
    end.

wait_until(Pred) -> wait_until(Pred, 20000, 100).

wait_until(_Pred, 0, _Step) -> ct:fail({timeout, ?MODULE});
wait_until(Pred, Wait, Step) ->
    case catch Pred() of
        true -> ok;
        _ -> timer:sleep(Step), wait_until(Pred, Wait - Step, Step)
    end.

%%%_* RPC -------------------------------------------------------------------

rpc(Node, F, A) ->
    rpc(Node, ?MODULE, F, A).

rpc(Node, M, F, A) ->
    case rpc:call(Node, M, F, A, 30000) of
        {badrpc, Reason} -> ct:fail({badrpc, Node, M, F, Reason});
        Result -> Result
    end.

fencing_rejected(Node) ->
    Snapshot = rpc(Node, broker_metrics, snapshot, []),
    maps:get(cluster_fencing_rejected, Snapshot).
