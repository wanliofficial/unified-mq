%% @doc Room registry: the peer set of every signaling room (S10-B).
%%
%% Sharded by `phash2(Room, Shards)' exactly like `a2a_task_registry', so two
%% rooms never contend on one gen_server. Each shard owns a `protected' ETS table
%% that any process can read without a call, which is what keeps `to' forwarding
%% at one lookup plus one `Pid ! Frame'.
%%
%% **Single node, no Mnesia, no boot replay** (ADR-028 D4): room membership is
%% connection state. After a node restart the connections are gone by
%% definition, so an empty registry is the *correct* state, not a loss.
%%
%% **Broadcast lives in the shard, not here** (ADR-028 D3): "who was already
%% here" has to be decided atomically with the join, so `webrtc_room_shard' sends
%% the `joined' / `left' frames while it is the only writer of the room state.
%% This module is the addressing surface: which shard owns a room, and what is in
%% it.
-module(webrtc_room_registry).

-behaviour(supervisor).

-export([start_link/0, init/1]).
-export([join/4, leave/2, lookup/2, member/2, peers/1, rooms/0,
         shard_count/0, shard_names/0, shard_for/1, table_name/1]).

-define(SHARDS, 8).

-type member() :: #{peer_id := binary(), username := binary(), pid := pid()}.
-export_type([member/0]).

%% @doc Register `PeerId' (owned by `Pid', displayed as `username') in `Room'
%% and tell the members that were already there.
%%
%% Returns the peer ids that were present *before* this join, so the caller can
%% report the room it entered. `Opts' carries the display name and the room
%% capacity (`max_peers', defaulting to the app env):
%%
%%   join(Room, PeerId, Pid, #{username := User, max_peers => N})
-spec join(binary(), binary(), pid(), #{username := binary(),
                                        max_peers => pos_integer()}) ->
          {ok, [binary()]} | {error, room_full | peer_id_taken | unavailable}.
join(Room, PeerId, Pid, Opts) when is_binary(PeerId), is_pid(Pid) ->
    Username = maps:get(username, Opts),
    MaxPeers = maps:get(max_peers, Opts, max_peers_per_room()),
    try gen_server:call(shard_for(Room), {join, Room, PeerId, Pid, Username, MaxPeers})
    catch exit:_ -> {error, unavailable}
    end.

%% @doc Drop the connection's membership. Idempotent and total: leaving twice,
%% leaving after the room emptied, or leaving while the shard is down is a no-op
%% (R-S9). Never raises, because it runs in `terminate/2'.
-spec leave(binary(), pid()) -> ok.
leave(Room, Pid) ->
    _ = catch gen_server:call(shard_for(Room), {leave, Room, Pid}),
    ok.

%% @doc Where is `PeerId' in `Room'? `undefined' for an unknown peer *and* for a
%% peer in another room -- the two are deliberately indistinguishable, so `to'
%% cannot be used to probe for peer ids elsewhere (R-X4).
-spec lookup(binary(), binary()) -> pid() | undefined.
lookup(Room, PeerId) ->
    case member(Room, PeerId) of
        undefined     -> undefined;
        #{pid := Pid} -> Pid
    end.

%% @doc The full membership record of one peer, or `undefined'.
-spec member(binary(), binary()) -> member() | undefined.
member(Room, PeerId) ->
    case lookup_row(Room, PeerId) of
        {_Key, Pid, Username} ->
            #{peer_id => PeerId, username => Username, pid => Pid};
        undefined ->
            undefined
    end.

%% @doc Every member of `Room', in peer-id order. An unknown (or already
%% emptied) room is `[]', not an error: a room exists only while it has members.
-spec peers(binary()) -> [member()].
peers(Room) ->
    Index = index_for(Room),
    %% `ets:match/2' answers with a list of bound values (not the match spec
    %% template's tuples), so the comprehension destructures lists.
    [#{peer_id => PeerId, username => Username, pid => Pid}
     || [PeerId, Pid, Username] <- lists:sort(rows(Index, Room))].

%% @doc Every non-empty room, sorted.
-spec rooms() -> [binary()].
rooms() ->
    lists:usort(lists:append([rooms_in(Index)
                              || Index <- lists:seq(1, shard_count())])).

%% ---- introspection ------------------------------------------------------

-spec shard_count() -> pos_integer().
shard_count() ->
    case application:get_env(broker_proto_webrtc, room_shards, undefined) of
        N when is_integer(N), N > 0 -> N;
        _ -> max(1, erlang:system_info(schedulers_online))
    end.

-spec shard_names() -> [atom()].
shard_names() ->
    [webrtc_room_shard:name(I) || I <- lists:seq(1, shard_count())].

%% @doc The shard process that owns `Room'.
-spec shard_for(binary()) -> atom().
shard_for(Room) ->
    webrtc_room_shard:name(index_for(Room)).

%% @doc The ETS table `Room' lives in. Exported so the room suite can assert
%% shard placement directly instead of inferring it.
-spec table_name(binary() | pos_integer()) -> atom().
table_name(Room) when is_binary(Room) ->
    webrtc_room_shard:table_name(index_for(Room));
table_name(Index) when is_integer(Index) ->
    webrtc_room_shard:table_name(Index).

%% ---- supervisor ---------------------------------------------------------

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% One-for-all: a shard that dies loses the members it tracked and its
    %% monitors. Restarting the whole set is the honest recovery -- each shard
    %% re-creates the table under the same (computed) name, members re-join on
    %% their next `join', and any member that died while its shard was down is
    %% reaped by the new shard's first sweep.
    SupFlags = #{strategy => one_for_all, intensity => 5, period => 10},
    {ok, {SupFlags, [shard_spec(I) || I <- lists:seq(1, shard_count())]}}.

shard_spec(Index) ->
    #{id => webrtc_room_shard:name(Index),
      start => {webrtc_room_shard, start_link, [Index]},
      restart => permanent,
      shutdown => 5000,
      type => worker,
      modules => [webrtc_room_shard]}.

%% ---- internals ----------------------------------------------------------

%% Shards are numbered from 1 (the child ids and the table names are built from
%% the number), so the hash is shifted: `phash2/2' is 0-based.
max_peers_per_room() ->
    application:get_env(broker_proto_webrtc, max_peers_per_room, 2).

index_for(Room) ->
    erlang:phash2(Room, shard_count()) + 1.

lookup_row(Room, PeerId) ->
    try ets:lookup(table_name(Room), {Room, PeerId}) of
        [Row] -> Row;
        []    -> undefined
    catch
        error:badarg -> undefined
    end.

rows(Index, Room) ->
    try ets:match(table_name(Index), {{Room, '$1'}, '$2', '$3'})
    catch
        error:badarg -> []
    end.

rooms_in(Index) ->
    try
        lists:usort([Room || {{Room, _PeerId}, _Pid, _User}
                                 <- ets:tab2list(table_name(Index))])
    catch
        error:badarg -> []
    end.
