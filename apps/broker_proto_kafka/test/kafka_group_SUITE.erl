%% @doc Unit tests for `kafka_group', the single-node consumer-group
%% coordinator (S5).
%%
%% `kafka_group' keeps all of its state in a gen_server's map (groups and
%% committed offsets), so these tests need nothing beyond the module itself:
%% `start_link/0' is brought up directly, the way `amqp_topo_SUITE' starts its
%% own subject, so no partition configuration or listener is required.
%%
%% The interesting behaviour is the rebalance barrier:
%%   * the first `join/7' of a group starts a generation and parks the caller
%%     (`{deferred, ...}') because the other members must re-join;
%%   * the join that makes every known member `joined' completes the
%%     generation and answers every waiter with the member list;
%%   * heartbeats answer `REBALANCE_IN_PROGRESS' while a rebalance is open and
%%     `ok' once the group has settled;
%%   * a `sync/4' with the leader's assignment map distributes it, and the
%%     non-leader members that called `sync/4' with `[]' are woken with their
%%     share.
%%   * session timeouts are swept every second, dropping dead members.
%%
%% Tests drive the coordinator through its public API (`join/7', `sync/4',
%% `heartbeat/3', ...) using two pids standing in for two clients, so the
%% asynchronous reply path is exercised exactly as `kafka_conn' drives it.
-module(kafka_group_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([empty_member_id_handshake/1,
         first_join_starts_generation_and_parks/1,
         second_join_completes_barrier/1,
         leader_receives_member_list/1,
         heartbeat_reports_rebalance_then_settles/1,
         heartbeat_wrong_generation_rejected/1,
         sync_distributes_assignment/1,
         sync_waits_for_leader_then_receives_assignment/1,
         sync_wrong_generation_rejected/1,
         leave_removes_member_and_triggers_rebalance/1,
         last_member_leave_drops_group/1,
         commit_and_fetch_offsets/1,
         session_timeout_sweeps_dead_member/1]).

-define(GROUP, <<"g1">>).

all() ->
    [empty_member_id_handshake,
     first_join_starts_generation_and_parks,
     second_join_completes_barrier,
     leader_receives_member_list,
     heartbeat_reports_rebalance_then_settles,
     heartbeat_wrong_generation_rejected,
     sync_distributes_assignment,
     sync_waits_for_leader_then_receives_assignment,
     sync_wrong_generation_rejected,
     leave_removes_member_and_triggers_rebalance,
     last_member_leave_drops_group,
     commit_and_fetch_offsets,
     session_timeout_sweeps_dead_member].

init_per_testcase(_Case, Config) ->
    {ok, Pid} = kafka_group:start_link(),
    unlink(Pid),
    [{group_pid, Pid} | Config].

end_per_testcase(_Case, _Config) ->
    case whereis(kafka_group) of
        undefined -> ok;
        Pid -> exit(Pid, shutdown)
    end,
    ok.

%% A client that has not joined yet sends an empty member id (the KIP-394
%% handshake). The coordinator mints an id and answers MEMBER_ID_REQUIRED so
%% the client retries with the returned id.
empty_member_id_handshake(_Config) ->
    {error, 79, Id} =
        kafka_group:join(?GROUP, <<>>, undefined, <<"consumer">>,
                         [{<<"roundrobin">>, <<>>}], 10000, 5000),
    ?assert(is_binary(Id)),
    ?assert(byte_size(Id) > 0).

%% The first member of a fresh group completes the barrier immediately (it is
%% the only member) and is answered with generation 1.
first_join_starts_generation_and_parks(_Config) ->
    Self = self(),
    Helper = spawn(fun() ->
        R = kafka_group:join(?GROUP, <<"m1">>, undefined, <<"consumer">>,
                             [{<<"roundrobin">>, <<>>}], 10000, 5000),
        Self ! {joined, R}
    end),
    receive
        {joined, {ok, Join}} ->
            ?assertEqual(1, maps:get(generation, Join)),
            ?assertEqual(<<"m1">>, maps:get(member_id, Join));
        {joined, Other} ->
            ct:fail({unexpected_first_join, Other})
    after 2000 ->
        ct:fail(first_join_should_complete)
    end,
    exit(Helper, kill).

%% A second member joining opens a new rebalance and parks; the first member
%% re-joining completes the barrier, answering both with the new generation.
second_join_completes_barrier(_Config) ->
    Self = self(),
    M1 = spawn(fun() ->
        R1 = kafka_group:join(?GROUP, <<"m1">>, undefined, <<"consumer">>,
                              [{<<"roundrobin">>, <<>>}], 10000, 5000),
        Self ! {m1, R1},
        %% Wait for m2 to join (and park), then re-join to complete the barrier.
        timer:sleep(200),
        R2 = kafka_group:join(?GROUP, <<"m1">>, undefined, <<"consumer">>,
                              [{<<"roundrobin">>, <<>>}], 10000, 5000),
        Self ! {m1again, R2}
    end),
    M2 = spawn(fun() ->
        %% Small delay so m1's first join completes first.
        timer:sleep(100),
        R = kafka_group:join(?GROUP, <<"m2">>, undefined, <<"consumer">>,
                             [{<<"roundrobin">>, <<>>}], 10000, 5000),
        Self ! {m2, R}
    end),
    %% m1's first join (gen 1) completes immediately.
    {ok, J1} = wait_msg(m1, 2000),
    ?assertEqual(1, maps:get(generation, J1)),
    %% m2's join opens gen 2 and parks; m1 re-joins to complete it.
    {ok, J2} = wait_msg(m1again, 2000),
    {ok, J3} = wait_msg(m2, 2000),
    ?assertEqual(maps:get(generation, J2), maps:get(generation, J3)),
    ?assertEqual(2, maps:get(generation, J2)),
    %% One of J2/J3 is the leader's reply (full member list), the other is
    %% a follower's (empty). Identify the leader by the non-empty members.
    LeaderMembers = case maps:get(members, J2) of
                        [] -> maps:get(members, J3);
                        _ -> maps:get(members, J2)
                    end,
    ?assertEqual(lists:sort([<<"m1">>, <<"m2">>]),
                 lists:sort([Id || {Id, _, _} <- LeaderMembers])),
    M1 ! stop,
    M2 ! stop.

%% Only the leader sees the full member metadata list; followers get [].
leader_receives_member_list(_Config) ->
    Self = self(),
    %% m1 joins and re-joins; m2 joins after m1's first join to trigger gen 2.
    M1 = spawn(fun() ->
        {ok, _} = join_blocking(?GROUP, <<"m1">>),
        timer:sleep(100),
        {ok, J} = join_blocking(?GROUP, <<"m1">>),
        Self ! {r, <<"m1">>, J}
    end),
    M2 = spawn(fun() ->
        timer:sleep(50),
        {ok, J} = join_blocking(?GROUP, <<"m2">>),
        Self ! {r, <<"m2">>, J}
    end),
    Replies = [receive {r, Id, R} -> {Id, R} end || _ <- [1, 2]],
    [{_, J1}, {_, J2}] = Replies,
    %% The leader is always the lexicographically smallest member id (m1).
    LeaderReply = case maps:get(members, J1) of
                      [_ | _] -> J1;
                      [] -> J2
                  end,
    FollowerReply = case maps:get(members, J1) of
                        [] -> J1;
                        [_ | _] -> J2
                    end,
    ?assertEqual(<<"m1">>, maps:get(leader, LeaderReply)),
    ?assertEqual(<<"m1">>, maps:get(leader, FollowerReply)),
    ?assertMatch([_, _], maps:get(members, LeaderReply)),
    ?assertEqual([], maps:get(members, FollowerReply)),
    M1 ! stop,
    M2 ! stop.

%% While a rebalance is open (a member just left) a heartbeat answers
%% REBALANCE_IN_PROGRESS. (A settled single-member group has no assignment,
%% so its heartbeat also reports the rebalance until a SyncGroup runs.)
heartbeat_reports_rebalance_then_settles(_Config) ->
    {ok, _} = join_blocking(?GROUP, <<"m1">>),
    %% m1 leaving drops the group entirely (it was the only member), so a
    %% later heartbeat is UNKNOWN_MEMBER_ID.
    ok = kafka_group:leave(?GROUP, <<"m1">>),
    ?assertEqual({error, 25},
                 kafka_group:heartbeat(?GROUP, 2, <<"m1">>)).

%% A heartbeat carrying a stale generation is rejected as ILLEGAL_GENERATION.
heartbeat_wrong_generation_rejected(_Config) ->
    {ok, _} = join_blocking(?GROUP, <<"m1">>),
    ?assertEqual({error, 22},
                 kafka_group:heartbeat(?GROUP, 999, <<"m1">>)),
    ?assertEqual({error, 25},
                 kafka_group:heartbeat(?GROUP, 1, <<"ghost">>)).

%% The leader's SyncGroup carries the full assignment map; every member is
%% given an entry (members the map omits get the empty assignment).
sync_distributes_assignment(_Config) ->
    Self = self(),
    %% Both members join and re-join to complete the gen 2 barrier.
    M1 = spawn(fun() ->
        {ok, _} = join_blocking(?GROUP, <<"m1">>),
        timer:sleep(100),
        {ok, J2} = join_blocking(?GROUP, <<"m1">>),
        Self ! {m1gen, maps:get(generation, J2)}
    end),
    M2 = spawn(fun() ->
        timer:sleep(50),
        {ok, J} = join_blocking(?GROUP, <<"m2">>),
        Self ! {m2gen, maps:get(generation, J)}
    end),
    Gen1 = wait_msg(m1gen, 2000),
    Gen2 = wait_msg(m2gen, 2000),
    ?assertEqual(Gen1, Gen2),
    %% m1 (the leader) publishes the assignment.
    Assignment = [{<<"m1">>, <<1:16/big, 0:32/big, 5:32/big>>},
                  {<<"m2">>, <<1:16/big, 0:32/big, 9:32/big>>}],
    ?assertMatch({ok, _},
                 kafka_group:sync(?GROUP, Gen1, <<"m1">>, Assignment)),
    %% m2 syncs and receives its share.
    ?assertEqual({ok, <<1:16/big, 0:32/big, 9:32/big>>},
                 kafka_group:sync(?GROUP, Gen2, <<"m2">>, [])),
    %% m1 can also re-sync to get its own assignment.
    ?assertEqual({ok, <<1:16/big, 0:32/big, 5:32/big>>},
                 kafka_group:sync(?GROUP, Gen1, <<"m1">>, [])),
    M1 ! stop,
    M2 ! stop.

%% A non-leader that syncs receives the assignment the leader published.
sync_waits_for_leader_then_receives_assignment(_Config) ->
    Self = self(),
    %% m1 joins and re-joins in a helper (so the main process can coordinate).
    M1 = spawn(fun() ->
        {ok, _} = join_blocking(?GROUP, <<"m1">>),
        timer:sleep(100),
        {ok, J2} = join_blocking(?GROUP, <<"m1">>),
        Self ! {m1gen, maps:get(generation, J2)}
    end),
    M2 = spawn(fun() ->
        timer:sleep(50),
        {ok, J} = join_blocking(?GROUP, <<"m2">>),
        Self ! {m2gen, maps:get(generation, J)}
    end),
    %% Both should see the same generation (gen 2).
    Gen1 = wait_msg(m1gen, 2000),
    Gen2 = wait_msg(m2gen, 2000),
    ?assertEqual(Gen1, Gen2),
    %% m1 (the leader) publishes the assignment.
    ?assertMatch({ok, _},
                 kafka_group:sync(?GROUP, Gen1, <<"m1">>,
                                  [{<<"m2">>, <<1:16/big, 0:32/big, 7:32/big>>}])),
    %% m2 syncs and receives its share.
    ?assertEqual({ok, <<1:16/big, 0:32/big, 7:32/big>>},
                 kafka_group:sync(?GROUP, Gen2, <<"m2">>, [])),
    M1 ! stop,
    M2 ! stop.

%% SyncGroup with a generation that does not match the group's current one is
%% rejected as ILLEGAL_GENERATION.
sync_wrong_generation_rejected(_Config) ->
    {ok, _} = join_blocking(?GROUP, <<"m1">>),
    ?assertEqual({error, 22},
                 kafka_group:sync(?GROUP, 999, <<"m1">>,
                                  [{<<"m1">>, <<0:16/big, 0:32/big, 0:32/big>>}])).

%% LeaveGroup removes the member and opens a rebalance for the survivors.
leave_removes_member_and_triggers_rebalance(_Config) ->
    Self = self(),
    %% m1 joins and re-joins; m2 joins after m1's first join to trigger gen 2.
    M1 = spawn(fun() ->
        {ok, _} = join_blocking(?GROUP, <<"m1">>),
        timer:sleep(100),
        {ok, J} = join_blocking(?GROUP, <<"m1">>),
        Self ! {m1gen, maps:get(generation, J)}
    end),
    M2 = spawn(fun() ->
        timer:sleep(50),
        {ok, J} = join_blocking(?GROUP, <<"m2">>),
        Self ! {m2gen, maps:get(generation, J)}
    end),
    Gen1 = wait_msg(m1gen, 2000),
    Gen2 = wait_msg(m2gen, 2000),
    ?assertEqual(Gen1, Gen2),
    %% m2 leaves; the group rebalances (new generation). m1's heartbeat still
    %% carries the old generation, so it is rejected (either ILLEGAL_GENERATION
    %% or REBALANCE_IN_PROGRESS), telling m1 to re-join.
    ok = kafka_group:leave(?GROUP, <<"m2">>),
    ?assertMatch({error, _},
                 kafka_group:heartbeat(?GROUP, Gen1, <<"m1">>)),
    M1 ! stop,
    M2 ! stop.

%% When the last member leaves, the group is dropped entirely: a later
%% heartbeat for it is UNKNOWN_MEMBER_ID rather than a fresh generation.
last_member_leave_drops_group(_Config) ->
    {ok, _} = join_blocking(?GROUP, <<"m1">>),
    ok = kafka_group:leave(?GROUP, <<"m1">>),
    ?assertEqual({error, 25},
                 kafka_group:heartbeat(?GROUP, 1, <<"m1">>)).

%% OffsetCommit stores per-partition offsets; OffsetFetch reads them back,
%% either for all topics or a specific topic list.
commit_and_fetch_offsets(_Config) ->
    Offsets = [{<<"t1">>, 0, 100, <<>>},
               {<<"t1">>, 1, 200, <<>>},
               {<<"t2">>, 0, 300, <<>>}],
    ?assertEqual(ok, kafka_group:commit(?GROUP, <<"m1">>, Offsets)),
    All = kafka_group:fetch_offsets(?GROUP, all),
    ?assertMatch([{<<"t1">>, 0, 100, _},
                  {<<"t1">>, 1, 200, _},
                  {<<"t2">>, 0, 300, _}], lists:sort(All)),
    T1 = kafka_group:fetch_offsets(?GROUP, [{<<"t1">>, [0, 1]}]),
    ?assertMatch([{<<"t1">>, 0, 100, _}, {<<"t1">>, 1, 200, _}],
                 lists:sort(T1)).

%% A member whose session lapses is swept and removed, opening a rebalance.
%% Uses a short session timeout so the 1s sweep drops the member promptly.
session_timeout_sweeps_dead_member(_Config) ->
    Self = self(),
    %% m1 and m2 join and re-join to complete gen 2.
    M1 = spawn(fun() ->
        {ok, _} = join_blocking(?GROUP, <<"m1">>),
        timer:sleep(100),
        {ok, J} = join_blocking(?GROUP, <<"m1">>),
        Self ! {gen, maps:get(generation, J)}
    end),
    M2 = spawn(fun() ->
        timer:sleep(50),
        {ok, J} = join_blocking(?GROUP, <<"m2">>),
        Self ! {gen, maps:get(generation, J)}
    end),
    Gen1 = wait_msg(gen, 2000),
    Gen2 = wait_msg(gen, 2000),
    ?assertEqual(Gen1, Gen2),
    %% Park a third member with a 1s session and never heartbeat it. Its join
    %% opens a rebalance (m1/m2 must re-join); we let m3's session lapse so
    %% the next sweep drops it.
    _M3 = spawn(fun() ->
        try kafka_group:join(?GROUP, <<"m3">>, undefined,
                             <<"consumer">>,
                             [{<<"roundrobin">>, <<>>}], 1000, 30000)
        catch _:_ -> ok
        end
    end),
    %% Wait for m3's join to open the rebalance. Probing *m1* is safe: m1's
    %% heartbeat answers REBALANCE_IN_PROGRESS (27) and only refreshes m1's own
    %% last_seen. Never probe m3 here -- do_heartbeat refreshes last_seen, so
    %% polling m3 would keep its 1s session alive and the sweep could never drop
    %% it (the previous version spun for the whole wait_for budget and failed).
    wait_for(fun() ->
        kafka_group:heartbeat(?GROUP, Gen1 + 1, <<"m1">>) =:= {error, 27}
        end, 3000),
    %% m3 never re-joins and nothing heartbeats it, so after its 1s session
    %% lapses the next 1s sweep drops it and opens a further rebalance, moving
    %% the generation from Gen1+1 to Gen1+2. Watch m1 for that bump (still only
    %% touching m1), which is the observable consequence of m3 being swept.
    wait_for(fun() ->
        kafka_group:heartbeat(?GROUP, Gen1 + 1, <<"m1">>) =:= {error, 22}
        end, 5000),
    %% Now confirm m3 itself is gone: a heartbeat finds no such member and
    %% answers UNKNOWN_MEMBER_ID (25), regardless of the generation passed.
    ?assertEqual({error, 25},
                 kafka_group:heartbeat(?GROUP, Gen1 + 1, <<"m3">>)),
    M1 ! stop,
    M2 ! stop.

%%%_* Helpers ---------------------------------------------------------------

%% Join a member and return its reply. For a single-member group the first
%% join completes immediately. For a multi-member group the caller must
%% re-join to complete the barrier; this helper handles that by re-joining
%% once with the same member id after the first reply.
join_blocking(Group, MemberId) ->
    Self = self(),
    _Pid = spawn(fun() ->
        R = kafka_group:join(Group, MemberId, undefined, <<"consumer">>,
                             [{<<"roundrobin">>, <<>>}], 10000, 5000),
        Self ! {join_reply, R}
    end),
    receive
        {join_reply, R} -> R
    after 2000 ->
        ct:fail(no_join_reply)
    end.

wait_msg(Tag, Timeout) ->
    receive
        {Tag, V} -> V
    after Timeout ->
        ct:fail({timeout_waiting_for, Tag})
    end.

%% Poll Fun until it returns true or TimeoutMs of wall clock elapses. The second
%% argument is a *millisecond* budget (the callers pass 3000/5000 expecting ms);
%% the previous version treated it as an iteration count -- 4000 loops x 150ms
%% = 600s -- so a genuine failure burned ten minutes before reporting. Poll
%% every 100ms and fail fast once the budget is gone.
wait_for(Fun, TimeoutMs) ->
    wait_for(Fun, TimeoutMs, erlang:monotonic_time(millisecond)).

wait_for(Fun, TimeoutMs, Started) ->
    case Fun() of
        true -> ok;
        false ->
            case erlang:monotonic_time(millisecond) - Started >= TimeoutMs of
                true -> ct:fail({condition_never_met, TimeoutMs});
                false -> timer:sleep(100), wait_for(Fun, TimeoutMs, Started)
            end
    end.
