%% @doc One room shard: the serialization point for a subset of rooms
%% (ADR-028 D4, structured like `a2a_task_shard').
%%
%% Every membership change for a room goes through exactly one gen_server, so
%% "count the members, decide admit/refuse, insert, tell the others" is atomic
%% for that room without a lock. Rooms on different shards never wait for each
%% other.
%%
%% **Why the broadcast happens here**: `joined' must reach exactly the members
%% that were present *before* the join, and `left' exactly those that remain.
%% Computing that set anywhere else would race another join or leave on the same
%% room; inside the shard the set is the room state at that instant (R-S3).
%%
%% **Why the monitors live here**: a connection killed with `exit(Pid, kill)'
%% never runs `terminate/2', so the shard -- not the connection -- is what
%% guarantees exactly one `left' and no ghost member (ADR-028 D4). A `DOWN' for
%% a member already removed by an explicit `leave' finds no monitor left and is
%% ignored, so the frame is emitted once either way.
%%
%% The ETS table is `protected', so `webrtc_room_registry' -- and any reader --
%% resolves a peer without a call; only this process writes it.
-module(webrtc_room_shard).

-behaviour(gen_server).

-export([start_link/1, name/1, table_name/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SWEEP_MS, 30000).

-record(state, {index :: pos_integer(),
                table :: ets:tid(),
                %% MonRef => {Room, PeerId}, so a DOWN resolves to its member.
                monitors = #{} :: #{reference() => {binary(), binary()}}}).

-spec start_link(pos_integer()) -> {ok, pid()} | {error, term()}.
start_link(Index) ->
    gen_server:start_link({local, name(Index)}, ?MODULE, Index, []).

-spec name(pos_integer()) -> atom().
name(Index) ->
    list_to_atom("webrtc_room_shard_" ++ integer_to_list(Index)).

%% @doc Members table of shard `Index'. Derived, never stored, so a restarted
%% shard re-creates the same table and every reader keeps working.
-spec table_name(pos_integer()) -> atom().
table_name(Index) ->
    list_to_atom("webrtc_room_shard_" ++ integer_to_list(Index) ++ "_members").

-spec init(pos_integer()) -> {ok, #state{}}.
init(Index) ->
    process_flag(trap_exit, true),
    Table = ets:new(table_name(Index), [named_table, protected, set,
                                        {keypos, 1}]),
    erlang:send_after(?SWEEP_MS, self(), sweep),
    {ok, #state{index = Index, table = Table}}.

%% ---- calls --------------------------------------------------------------

handle_call({join, Room, PeerId, Pid, Username, MaxPeers}, _From, State) ->
    {Reply, State1} = do_join(Room, PeerId, Pid, Username, MaxPeers, State),
    {reply, Reply, State1};
handle_call({leave, Room, Pid}, _From, State) ->
    {reply, ok, do_leave(Room, Pid, State)};
handle_call(_Req, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%% A member's connection died without calling `leave' (kill, crash, socket
%% teardown race). Remove it and tell whoever is left.
handle_info({'DOWN', Ref, process, _Pid, _Reason}, State = #state{monitors = Mons}) ->
    case maps:take(Ref, Mons) of
        {{Room, PeerId}, Rest} ->
            {noreply, remove(Room, PeerId, State#state{monitors = Rest})};
        error ->
            {noreply, State}
    end;

%% Sweep members whose process is no longer alive. A shard crash wipes its
%% monitors, so a member that died while the shard was down would otherwise
%% linger as a ghost. Bounded by one sweep interval: a room never keeps a dead
%% peer "present" indefinitely.
handle_info(sweep, State = #state{table = Table}) ->
    Dead = try
               [{Room, PeerId}
                || {{Room, PeerId}, Pid, _User} <- ets:tab2list(Table),
                   not is_process_alive(Pid)]
           catch error:badarg -> []
           end,
    State1 = lists:foldl(fun({Room, PeerId}, Acc) ->
                                 remove(Room, PeerId, Acc)
                         end, State, Dead),
    erlang:send_after(?SWEEP_MS, self(), sweep),
    {noreply, State1};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    %% The table dies with the process (it is owned here), which is the intended
    %% semantics: no member outlives its shard.
    ok.

%% ---- membership ---------------------------------------------------------

do_join(Room, PeerId, Pid, Username, MaxPeers, State = #state{table = Table}) ->
    case ets:lookup(Table, {Room, PeerId}) of
        [{_, Pid, _User}] ->
            %% Idempotent re-join by the same connection: keep the binding and
            %% report the peers already here (excluding itself).
            {{ok, peer_ids_except(Room, Pid, Table)}, State};
        [{_, _OtherPid, _User}] ->
            {{error, peer_id_taken}, State};
        [] ->
            Current = peer_ids(Room, Table),
            case length(Current) >= MaxPeers of
                true ->
                    {{error, room_full}, State};
                false ->
                    true = ets:insert(Table, {{Room, PeerId}, Pid, Username}),
                    Ref = erlang:monitor(process, Pid),
                    Mons = maps:put(Ref, {Room, PeerId}, State#state.monitors),
                    %% R-S3: `joined' goes to the members that were already in
                    %% the room -- never to the joiner itself.
                    Frame = webrtc_signal:joined(Username, PeerId),
                    _ = [send_frame(P, Frame)
                         || P <- members_except(Room, Pid, Table)],
                    {{ok, Current}, State#state{monitors = Mons}}
            end
    end.

do_leave(Room, Pid, State) ->
    case find_by_pid(Room, Pid, State#state.table) of
        undefined -> State;
        PeerId    -> remove(Room, PeerId, State)
    end.

%% Remove one member: delete the row, demonitor, then tell the remaining
%% members. `left' excludes the departing connection (R-S3): it is about to be
%% closed, and the upstream "publish to everyone" behaviour only worked because
%% the frame was written before the socket died.
remove(Room, PeerId, State = #state{table = Table, monitors = Mons}) ->
    case ets:lookup(Table, {Room, PeerId}) of
        [] ->
            State;
        [{_, Pid, Username}] ->
            true = ets:delete(Table, {Room, PeerId}),
            Mons1 = demonitor_for(Room, PeerId, Mons),
            Frame = webrtc_signal:left(Username, PeerId),
            _ = [send_frame(P, Frame)
                 || P <- members_except(Room, Pid, Table)],
            State#state{monitors = Mons1}
    end.

%% ---- internals ----------------------------------------------------------

demonitor_for(Room, PeerId, Mons) ->
    case [Ref || {Ref, Key} <- maps:to_list(Mons), Key =:= {Room, PeerId}] of
        [] ->
            Mons;
        Refs ->
            _ = [erlang:demonitor(Ref, [flush]) || Ref <- Refs],
            maps:without(Refs, Mons)
    end.

find_by_pid(Room, Pid, Table) ->
    case ets:match(Table, {{Room, '$1'}, Pid, '_'}) of
        [[PeerId]] -> PeerId;
        _ -> undefined
    end.

%% `ets:match/2' returns one list of bound values per matching object, in the
%% order the `$N' variables appear in the pattern -- not tuples.
peer_ids(Room, Table) ->
    lists:sort([Id || [Id] <- ets:match(Table, {{Room, '$1'}, '_', '_'})]).

peer_ids_except(Room, Pid, Table) ->
    lists:sort([PeerId || [PeerId, P] <- ets:match(Table,
                                                  {{Room, '$1'}, '$2', '_'}),
                          P =/= Pid]).

members_except(Room, Pid, Table) ->
    [P || [P] <- ets:match(Table, {{Room, '_'}, '$1', '_'}),
          P =/= Pid].

%% The five frozen events are all text frames (design §2.3), so the fan-out
%% only ever sends `{text, Bin}'.
send_frame(Pid, {text, Bin}) ->
    Pid ! {text, Bin},
    ok.
