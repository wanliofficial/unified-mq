%% @doc Kafka consumer-group coordinator (S5).
%%
%% Implements the classic (non-flexible) Kafka group protocol on one node:
%% FindCoordinator points every group at this broker, JoinGroup performs a
%% barrier-based rebalance, SyncGroup distributes the leader's assignment,
%% Heartbeat keeps members alive, LeaveGroup removes them, and
%% OffsetCommit/OffsetFetch store committed offsets in memory.
%%
%% How a rebalance runs here:
%%   * every JoinGroup starts a new generation and marks the other known
%%     members as "must re-join"; heartbeats answer REBALANCE_IN_PROGRESS so
%%     their clients do;
%%   * the join whose arrival makes every member "joined" completes the
%%     generation and answers all waiting JoinGroups with the member list
%%     (leader only) -- the same barrier Kafka's coordinator uses;
%%   * a rebalance timeout (the client's `rebalance_timeout_ms') forces
%%     completion with whoever re-joined, so a dead client cannot stall the
%%     group until its session expires.
%%
%% Deliberate v1 limits (documented in `docs/protocol-compatibility.md' §4.8):
%%   * a single coordinator (this node), no `__consumer_offsets' topic;
%%   * committed offsets are in-memory and reset on restart;
%%   * session timeouts are swept every second; the group protocol is
%%     `consumer' only.
-module(kafka_group).

-behaviour(gen_server).

-export([start_link/0,
         join/7, sync/4, heartbeat/3, leave/2, close_member/2,
         commit/3, fetch_offsets/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(SWEEP_MS, 1000).
-define(DEFAULT_SESSION_MS, 10000).
-define(DEFAULT_REBALANCE_MS, 5000).
-define(EMPTY_ASSIGNMENT, <<0:16/signed, 0:32/signed, -1:32/signed>>).

%% Kafka error codes used here.
-define(UNKNOWN_MEMBER_ID, 25).
-define(ILLEGAL_GENERATION, 22).
-define(REBALANCE_IN_PROGRESS, 27).
-define(MEMBER_ID_REQUIRED, 79).

%%%_* API ----------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Join (or re-join) a group. `Protocols' is `[{Name, Metadata}]' as the
%% client sent them; `Metadata' is opaque to this module. Returns
%% `{ok, Reply}' with the fields the JoinGroup response needs, or
%% `{error, Code, NewMemberId}' for the KIP-394 empty-id handshake.
join(Group, MemberId, InstanceId, ProtocolType, Protocols, SessionMs, RebalanceMs) ->
    gen_server:call(?MODULE, {join, Group, MemberId, InstanceId, ProtocolType,
                              Protocols, SessionMs, RebalanceMs, self()},
                    timer:seconds(40)).

%% @doc Register the leader's assignments (non-empty) or wait for one.
sync(Group, Generation, MemberId, Assignments) ->
    gen_server:call(?MODULE, {sync, Group, Generation, MemberId, Assignments,
                              self()},
                    timer:seconds(40)).

heartbeat(Group, Generation, MemberId) ->
    gen_server:call(?MODULE, {heartbeat, Group, Generation, MemberId}).

leave(Group, MemberId) ->
    gen_server:call(?MODULE, {leave, Group, MemberId}).

close_member(Group, MemberId) ->
    gen_server:call(?MODULE, {close_member, Group, MemberId}).

commit(Group, MemberId, Offsets) ->
    gen_server:call(?MODULE, {commit, Group, MemberId, Offsets}).

fetch_offsets(Group, Topics) ->
    gen_server:call(?MODULE, {fetch_offsets, Group, Topics}).

%%%_* gen_server ---------------------------------------------------------

init([]) ->
    erlang:send_after(?SWEEP_MS, self(), sweep),
    {ok, #{groups => #{}, offsets => #{}}}.

handle_call({join, _Group, <<>>, _InstanceId, _ProtocolType, _Protocols, _Sess,
             _Rebal, _Pid}, _From, State) ->
    %% KIP-394: an empty member id gets a generated one and MEMBER_ID_REQUIRED.
    Id = new_member_id(),
    {reply, {error, ?MEMBER_ID_REQUIRED, Id}, State};
handle_call({join, Group, MemberId, InstanceId, ProtocolType, Protocols, Sess,
             Rebal, _Pid}, From, State) ->
    case do_join(Group, MemberId, InstanceId, ProtocolType, Protocols, Sess,
                 Rebal, From, State) of
        {reply, Reply, State1} -> {reply, Reply, State1};
        {deferred, State1} -> {noreply, State1}
    end;
handle_call({sync, Group, Generation, MemberId, Assignments, Pid}, _From, State) ->
    {Reply, State1} = do_sync(Group, Generation, MemberId, Assignments, Pid, State),
    {reply, Reply, State1};
handle_call({heartbeat, Group, Generation, MemberId}, _From, State) ->
    {Reply, State1} = do_heartbeat(Group, Generation, MemberId, State),
    {reply, Reply, State1};
handle_call({leave, Group, MemberId}, _From, State) ->
    {reply, ok, remove_member(Group, MemberId, State)};
handle_call({close_member, Group, MemberId}, _From, State) ->
    {reply, ok, remove_member(Group, MemberId, State)};
handle_call({commit, Group, _MemberId, Offsets}, _From, State) ->
    Persisted = maps:get(offsets, State),
    New = lists:foldl(
            fun({Topic, Partition, Offset, Meta}, Acc) ->
                    Acc#{{Group, Topic, Partition} => {Offset, Meta}}
            end, Persisted, Offsets),
    {reply, ok, State#{offsets := New}};
handle_call({fetch_offsets, Group, Topics}, _From, State) ->
    Persisted = maps:get(offsets, State),
    Reply =
        case Topics of
            all ->
                [{T, P, O, M} || {{G, T, P}, {O, M}} <- maps:to_list(Persisted),
                                  G =:= Group];
            _ ->
                lists:flatmap(
                  fun({T, Partitions}) ->
                          [{T, P, O, M} || P <- Partitions,
                                           {O, M} <- [maps:get({Group, T, P},
                                                               Persisted,
                                                               {0, <<>>})]]
                  end, Topics)
        end,
    {reply, Reply, State};
handle_call(_Req, _From, State) ->
    {reply, {error, unsupported}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(sweep, State) ->
    Now = now_ms(),
    Groups = maps:map(fun(_G, Grp) -> sweep_group(Grp, Now) end,
                      maps:get(groups, State)),
    erlang:send_after(?SWEEP_MS, self(), sweep),
    {noreply, State#{groups := Groups}};
handle_info({rebalance_timeout, Group, Generation}, State) ->
    Groups = maps:get(groups, State),
    case maps:find(Group, Groups) of
        {ok, #{generation := Generation} = Grp} ->
            {Replies, State1} = complete_rebalance(Group, Grp, State),
            maps:foreach(fun(_Id, {From, Reply}) ->
                                 gen_server:reply(From, Reply)
                         end, Replies),
            {noreply, State1};
        _ ->
            {noreply, State}
    end;
handle_info(_Other, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%_* Join ---------------------------------------------------------------

%% Returns `{reply, Reply, State}' when this caller can be answered inline, or
%% `{deferred, State}' when its JoinGroup is parked until the barrier completes.
do_join(Group, MemberId, InstanceId, ProtocolType, Protocols, Sess, Rebal, From,
        State) ->
    Groups = maps:get(groups, State),
    Grp0 = maps:get(Group, Groups, new_group()),
    %% Start the rebalance *before* adding this member: begin_rebalance marks
    %% every known member as "must re-join", and the caller itself has already
    %% joined, so it must not be cleared again.
    Grp1 = case maps:get(rebalancing, Grp0, false) of
               true -> Grp0;
               false -> begin_rebalance(Grp0)
           end,
    Members0 = maps:get(members, Grp1),
    Member0 = maps:get(MemberId, Members0, #{}),
    Member = maps:merge(Member0, #{instance_id => InstanceId,
                                   protocols => Protocols,
                                   session_timeout => Sess,
                                   last_seen => now_ms(),
                                   joined => true}),
    Grp2 = Grp1#{members := Members0#{MemberId => Member},
                 protocol_type := ProtocolType,
                 rebalance_timeout := Rebal},
    %% Register this caller as a waiter *before* checking the barrier:
    %% complete_rebalance/3 answers exactly the waiters it finds.
    Waiters = maps:get(join_waiters, Grp2),
    Grp3 = Grp2#{join_waiters := Waiters#{MemberId => From}},
    Members = maps:get(members, Grp3),
    case lists:all(fun(M) -> maps:get(joined, M) end, maps:values(Members)) of
        true ->
            {Replies, State1} = complete_rebalance(Group, Grp3, State),
            case maps:take(MemberId, Replies) of
                {{_From, Reply}, Rest} ->
                    maps:foreach(fun(_Id, {OtherFrom, OtherReply}) ->
                                         gen_server:reply(OtherFrom, OtherReply)
                                 end, Rest),
                    {reply, Reply, State1};
                error ->
                    {reply, {error, ?UNKNOWN_MEMBER_ID}, State1}
            end;
        false ->
            Generation = maps:get(generation, Grp3),
            _ = erlang:send_after(rebalance_timeout(Grp3), self(),
                                  {rebalance_timeout, Group, Generation}),
            {deferred,
             State#{groups := (maps:get(groups, State))#{Group => Grp3}}}
    end.

begin_rebalance(Grp) ->
    Generation = maps:get(generation, Grp) + 1,
    Members = maps:map(fun(_Id, M) -> M#{joined => false} end,
                       maps:get(members, Grp)),
    Grp#{generation := Generation,
         members := Members,
         assignments := #{},
         rebalancing := true,
         join_waiters := #{}}.

%% Answers every waiting join with the same member list; members that did not
%% re-join in time are dropped (their session is over). Returns
%% `#{MemberId => {From, Reply}}' plus the new state, so the inline caller can
%% take its own reply instead of having it sent twice.
complete_rebalance(Group, Grp, State) ->
    Waiters = maps:get(join_waiters, Grp),
    Members0 = maps:get(members, Grp),
    Members = maps:filter(fun(_Id, M) -> maps:get(joined, M) end, Members0),
    Leader = leader(Grp, Members),
    Protocol = choose_protocol(Grp, Members),
    Replies =
        maps:from_list(
          [{MemberId,
            {From,
             case maps:is_key(MemberId, Members) of
                 true -> {ok, join_reply(MemberId, Leader, Protocol, Grp, Members)};
                 false -> {error, ?UNKNOWN_MEMBER_ID}
             end}}
           || {MemberId, From} <- maps:to_list(Waiters)]),
    Members1 = maps:map(fun(_Id, M) -> M#{joined => true} end, Members),
    Grp1 = Grp#{join_waiters := #{},
                members := Members1,
                leader := Leader,
                protocol := Protocol,
                rebalancing := false},
    {Replies, State#{groups := (maps:get(groups, State))#{Group => Grp1}}}.

join_reply(MemberId, Leader, Protocol, Grp, Members) ->
    #{generation => maps:get(generation, Grp),
      protocol => Protocol,
      leader => Leader,
      member_id => MemberId,
      members =>
          case MemberId =:= Leader of
              true ->
                  [{Id, maps:get(instance_id, M, undefined),
                    metadata_for(Protocol, M)}
                   || {Id, M} <- lists:sort(maps:to_list(Members))];
              false ->
                  []
          end}.

choose_protocol(#{protocol := P}, _Members) when P =/= undefined ->
    P;
choose_protocol(_Grp, Members) ->
    case [Name || M <- maps:values(Members),
                  {Name, _} <- maps:get(protocols, M, [])] of
        [Name | _] -> Name;
        [] -> <<"range">>
    end.

metadata_for(Protocol, Member) ->
    case lists:keyfind(Protocol, 1, maps:get(protocols, Member, [])) of
        {_, Meta} -> Meta;
        false -> <<>>
    end.

leader(Grp, Members) ->
    case maps:get(leader, Grp) of
        undefined ->
            first_member(Members);
        L ->
            case maps:is_key(L, Members) of
                true -> L;
                false -> first_member(Members)
            end
    end.

first_member(Members) ->
    case lists:sort(maps:keys(Members)) of
        [First | _] -> First;
        [] -> undefined
    end.

%%%_* Sync ---------------------------------------------------------------

do_sync(Group, Generation, MemberId, [], Pid, State) ->
    case maps:find(Group, maps:get(groups, State)) of
        error ->
            {{error, ?UNKNOWN_MEMBER_ID}, State};
        {ok, Grp} ->
            case maps:get(generation, Grp) of
                Generation ->
                    case maps:find(MemberId, maps:get(assignments, Grp)) of
                        {ok, Assignment} ->
                            {{ok, Assignment}, State};
                        error ->
                            Waiters = maps:get(sync_waiters, Grp),
                            Grp1 = Grp#{sync_waiters :=
                                            Waiters#{MemberId => {Pid, self()}}},
                            {{wait, MemberId},
                             State#{groups := (maps:get(groups, State))#{Group => Grp1}}}
                    end;
                _ ->
                    {{error, ?ILLEGAL_GENERATION}, State}
            end
    end;
do_sync(Group, Generation, MemberId, Assignments, _Pid, State) ->
    %% The leader's SyncGroup carries the full assignment map.
    case maps:find(Group, maps:get(groups, State)) of
        error ->
            {{error, ?UNKNOWN_MEMBER_ID}, State};
        {ok, Grp} ->
            case maps:get(generation, Grp) of
                Generation ->
                    Map = maps:from_list(Assignments),
                    AllMembers = maps:keys(maps:get(members, Grp)),
                    Complete = maps:merge(
                                 maps:from_list([{Id, ?EMPTY_ASSIGNMENT}
                                                 || Id <- AllMembers]), Map),
                    lists:foreach(
                      fun({Id, {P, _}}) ->
                              P ! {kafka_sync_result, Group, Id,
                                   maps:get(Id, Complete, ?EMPTY_ASSIGNMENT)}
                      end, maps:to_list(maps:get(sync_waiters, Grp))),
                    Grp1 = Grp#{assignments := Complete, sync_waiters := #{}},
                    State1 = State#{groups :=
                                        (maps:get(groups, State))#{Group => Grp1}},
                    {{ok, maps:get(MemberId, Complete, ?EMPTY_ASSIGNMENT)}, State1};
                _ ->
                    {{error, ?ILLEGAL_GENERATION}, State}
            end
    end.

%%%_* Heartbeat / membership --------------------------------------------

do_heartbeat(Group, Generation, MemberId, State) ->
    case maps:find(Group, maps:get(groups, State)) of
        error ->
            {{error, ?UNKNOWN_MEMBER_ID}, State};
        {ok, Grp} ->
            Members = maps:get(members, Grp),
            %% `maps:find/2' returns `error' | {ok, Member}, so both halves of
            %% the tuple have to be destructured explicitly.
            case {maps:find(MemberId, Members), maps:get(generation, Grp)} of
                {error, _} ->
                    {{error, ?UNKNOWN_MEMBER_ID}, State};
                {{ok, _}, G} when G =/= Generation ->
                    {{error, ?ILLEGAL_GENERATION}, State};
                {{ok, M}, _} ->
                    Members1 = Members#{MemberId => M#{last_seen => now_ms()}},
                    Grp1 = Grp#{members := Members1},
                    State1 = State#{groups :=
                                        (maps:get(groups, State))#{Group => Grp1}},
                    case maps:get(rebalancing, Grp1, false) orelse
                         map_size(maps:get(assignments, Grp1)) =:= 0 of
                        true -> {{error, ?REBALANCE_IN_PROGRESS}, State1};
                        false -> {{ok, undefined}, State1}
                    end
            end
    end.

remove_member(Group, MemberId, State) ->
    case maps:find(Group, maps:get(groups, State)) of
        error ->
            State;
        {ok, Grp} ->
            Members = maps:remove(MemberId, maps:get(members, Grp)),
            Grp1 = Grp#{members := Members,
                        assignments := maps:remove(MemberId,
                                                   maps:get(assignments, Grp)),
                        leader := case maps:get(leader, Grp) of
                                      MemberId -> undefined;
                                      L -> L
                                  end},
            case map_size(Members) of
                0 ->
                    %% Last member gone: keep committed offsets, drop the group.
                    State#{groups := maps:remove(Group, maps:get(groups, State))};
                _ ->
                    Grp2 = begin_rebalance(Grp1),
                    Generation = maps:get(generation, Grp2),
                    _ = erlang:send_after(rebalance_timeout(Grp2), self(),
                                          {rebalance_timeout, Group, Generation}),
                    State#{groups := (maps:get(groups, State))#{Group => Grp2}}
            end
    end.

%% Drop members whose session lapsed. A change forces a rebalance so the
%% remaining members re-derive their assignment.
sweep_group(Grp, Now) ->
    Members = maps:get(members, Grp),
    Alive = maps:filter(fun(_Id, M) -> member_alive(M, Now) end, Members),
    case map_size(Alive) =:= map_size(Members) of
        true ->
            Grp;
        false when map_size(Alive) =:= 0 ->
            Grp#{members := #{}, assignments := #{}};
        false ->
            Grp1 = Grp#{members := Alive,
                        assignments := maps:with(maps:keys(Alive),
                                                 maps:get(assignments, Grp))},
            begin_rebalance(Grp1)
    end.

member_alive(M, Now) ->
    Timeout = maps:get(session_timeout, M, ?DEFAULT_SESSION_MS),
    Now - maps:get(last_seen, M) =< Timeout.

rebalance_timeout(Grp) ->
    maps:get(rebalance_timeout, Grp, ?DEFAULT_REBALANCE_MS).

%%%_* Helpers ------------------------------------------------------------

new_group() ->
    #{generation => 0,
      leader => undefined,
      protocol => undefined,
      protocol_type => <<"consumer">>,
      members => #{},
      assignments => #{},
      join_waiters => #{},
      sync_waiters => #{},
      rebalancing => false,
      session_timeout => ?DEFAULT_SESSION_MS,
      rebalance_timeout => ?DEFAULT_REBALANCE_MS}.

new_member_id() ->
    <<"member-", (integer_to_binary(erlang:unique_integer([positive])))/binary,
      "-", (integer_to_binary(rand:uniform(1000000)))/binary>>.

now_ms() ->
    erlang:monotonic_time(millisecond).
