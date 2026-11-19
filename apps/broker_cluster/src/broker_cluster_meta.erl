%% @doc ra-backed Metadata Raft Group (ADR-021 §D2/§D4).
%%
%% One group carries every partition's owner/epoch/replicas/migration state.
%% Commands are small and infrequent; data-plane appends never go through here
%% (ADR-008 §8.4). A timeout is reported as `{error, no_quorum}' -- that is the
%% honest answer when the group cannot commit, and it is what callers use to
%% refuse reliable writes instead of silently degrading them.
-module(broker_cluster_meta).

-behaviour(gen_server).

-export([start_link/0, command/1, command/2, members/0, join/1, join/2,
         leave/1, leave/2]).

-export_type([member_result/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% The ra server id is `{broker_cluster_meta, Node}', and ra registers each
%% server under that first element's local name. The controlling gen_server
%% therefore cannot register as `broker_cluster_meta' itself -- it would collide
%% with the ra server it just started.
-define(SERVER, ?MODULE).
-define(CTL, broker_cluster_meta_ctl).
-define(META_MACHINE, broker_cluster_meta_machine).

start_link() ->
    gen_server:start_link({local, ?CTL}, ?MODULE, [], []).

command(Command) ->
    command(Command, timeout()).

command(Command, Timeout) ->
    Server = {?SERVER, node()},
    try ra:process_command(Server, Command, Timeout) of
        {ok, Reply, _Leader} -> Reply;
        {timeout, _} -> no_quorum();
        {error, Reason} -> {error, Reason}
    catch
        Class:Reason ->
            {error, {metadata_unavailable, {Class, Reason}}}
    end.

%% @doc The committed members of the metadata group, as a live view (ADR-033).
%%
%% The members are read from the ra state machine via the control process, not
%% from `application:get_env(broker_cluster, members)': an operator changes the
%% membership at runtime with {@link join/1} / {@link leave/1}, and a node that
%% joined must be visible to the rebalance view on every survivor. When this
%% node's server id is absent from the committed membership -- i.e. a majority
%% of the group is down, so the leader's committed configuration does not
%% include us -- the answer is `{error, no_quorum}': the honest reply is that we
%% cannot know the membership, not that the configured list is the truth.
%%
%% This is the value `broker_rebalance:evaluate/0' / `step/0' / `status/0' read;
%% each maps `{error, no_quorum}' onto "empty view", so a rebalance during a
%% quorum loss is a no-op rather than a crash.
members() ->
    gen_server:call(?CTL, members, 5000).

%%%_* Online membership change (ADR-033) ----------------------------------
%%
%% Admin-driven, explicit node join/leave. This deliberately does NOT watch
%% nodeup/nodedown and does NOT auto-rebalance: an operator calls `join(Node)'
%% to add a node and `leave(Node)' to remove one. The sequence is the ra
%% documented two-step membership change (vendored README "Cluster Membership
%% Changes"): `add_member' records the new voter in the committed configuration
%% on the leader, then the joining node starts its ra server with the group as a
%% seed and catches up as a follower. `leave' is the mirror image -- drain the
%% node first (ADR-032 D5), then `remove_member'.
%%
%% `join(Node)' answers `{ok, already_member}' if the node is already in the
%% committed membership, `{error, no_quorum}' when the group cannot commit (the
%% node is *not* added), and `{error, {metadata_unavailable, _}}' when the
%% local metadata server is unreachable. It never silently reports success when
%% the node was not actually added.

-type member_result() :: ok | {ok, already_member} | {ok, not_member}
                         | {error, term()}.

-spec join(node()) -> member_result().
join(Node) when is_atom(Node) ->
    join(Node, timeout()).

-spec join(node(), timeout()) -> member_result().
join(Node, Timeout) when is_atom(Node) ->
    with_server(fun(Members) ->
                    case lists:member(Node, Members) of
                        true ->
                            %% Already a voter: the desired end-state holds, so
                            %% this is not an error.
                            {ok, already_member};
                        false ->
                            add_member(Node, Timeout)
                    end
                end).

-spec leave(node()) -> member_result().
leave(Node) when is_atom(Node) ->
    leave(Node, timeout()).

-spec leave(node(), timeout()) -> member_result().
leave(Node, Timeout) when is_atom(Node) ->
    with_server(fun(Members) ->
                    case lists:member(Node, Members) of
                        false ->
                            %% Not a voter: there is nothing to remove, so the
                            %% desired end-state holds.
                            {ok, not_member};
                        true ->
                            remove_member(Node, Timeout)
                    end
                end).

%% Run a membership query against this node's metadata server. `members/0'
%% already returns `{error, no_quorum}' on a quorum loss; the server-missing
%% and unexpected-reply cases are folded onto the same answer so callers get one
%% failure shape, not three.
with_server(Fun) ->
    case members() of
        {error, _} = Err ->
            Err;
        Ids when is_list(Ids) ->
            try Fun(Ids)
            catch
                Class:Reason ->
                    {error, {metadata_unavailable, {Class, Reason}}}
            end;
        Other ->
            {error, {metadata_unavailable, {unexpected, Other}}}
    end.

%% `add_member' / `remove_member' take a `ra_server_id()' (the local server id
%% is enough: ra follows a `{redirect, Leader}' reply to the leader). Their raw
%% replies are mapped onto our vocabulary: a redirect or a `{ok, _, _}' commit
%% is success, an explicit `not_member'/'already_member' is passed through, and
%% everything else (a timeout, `{error, cluster_change_not_permitted}', a server
%% that is not running) is an error the caller can act on.
add_member(Node, Timeout) ->
    case ra:add_member({?SERVER, node()}, {?SERVER, Node}, Timeout) of
        {ok, _, _} -> ok;
        {error, already_member} -> {error, already_member};
        {error, Reason} -> {error, Reason};
        {timeout, _} -> {error, no_quorum}
    end.

remove_member(Node, Timeout) ->
    case ra:remove_member({?SERVER, node()}, {?SERVER, Node}, Timeout) of
        {ok, _, _} -> ok;
        {error, not_member} -> {error, not_member};
        {error, Reason} -> {error, Reason};
        {timeout, _} -> {error, no_quorum}
    end.

%%%_* gen_server ----------------------------------------------------------

init([]) ->
    {ok, _} = application:ensure_all_started(ra),
    %% ra's own directory; the metadata log is tiny but must survive restart.
    DataDir = application:get_env(broker_cluster, data_dir, "data/ra"),
    application:set_env(ra, data_dir, filename:absname(DataDir)),
    ok = ensure_ra_system(),
    %% Local server first: `start_or_restart_cluster/5' restarts the head of the
    %% list directly and the rest over rpc, so a node bringing its own server
    %% back should not route the first call through another node.
    Ids = local_first([{?SERVER, N} || N <- members_env()]),
    Machine = {module, ?META_MACHINE, #{}},
    case ensure_group(Ids, Machine) of
        {ok, Started, NotStarted} ->
            {ok, #{members => members_env(), started => Started,
                   not_started => NotStarted}};
        {error, Reason} ->
            {stop, {metadata_cluster_not_formed, Reason}}
    end.

handle_call(members, _From, State) ->
    {reply, live_members(State), State};
handle_call(_Req, _From, St) ->
    {reply, {error, unsupported}, St}.

handle_cast(_Msg, St) -> {noreply, St}.
handle_info(_Info, St) -> {noreply, St}.
terminate(_Reason, _St) -> ok.
code_change(_OldVsn, St, _Extra) -> {ok, St}.

%%%_* Internal ------------------------------------------------------------

%% `application:ensure_all_started(ra)' starts the ra supervisor but not its
%% default system; `ra:start_server/2' needs the system registered. A stale
%% default system (a previous test, or an earlier data dir) is stopped first so
%% the configured directory actually wins.
ensure_ra_system() ->
    case ra_system:fetch(default) of
        undefined ->
            case ra_system:start_default() of
                {ok, _} -> ok;
                {error, {already_started, _}} -> ok;
                {error, Reason} -> {error, {ra_system, Reason}}
            end;
        _ ->
            ok
    end.

%% @doc True when this node's ra server is already running. In a multi-node
%% deployment the first node's bootstrap starts every member over rpc, so the
%% other nodes only attach: calling `start_or_restart_cluster/5' there would try
%% to form a *second* cluster and answer `cluster_not_formed'.
ensure_group(Ids, Machine) ->
    case whereis(?SERVER) of
        Pid when is_pid(Pid) ->
            {ok, [], []};
        undefined ->
            %% `ra:start_or_restart_cluster/5' is the bootstrap path: it restarts
            %% the head server, then "restarts" every other member over rpc. On a
            %% node coming back after a crash the peers never went down, so that
            %% second phase gets `{error, {already_started, Pid}}' for each of them
            %% and the list comprehension inside ra badmatches. The local server
            %% *has* restarted by then and will catch up from the leader, so treat
            %% that shape as success when the local server is registered -- a
            %% rejoining node must not need the whole group restarted.
            try ra:start_or_restart_cluster(default, broker_cluster, Machine, Ids) of
                {ok, Started, NotStarted} ->
                    {ok, Started, NotStarted};
                {error, Reason} ->
                    {error, Reason}
            catch
                error:{badmatch, {error, {already_started, _}}} ->
                    case whereis(?SERVER) of
                        Pid when is_pid(Pid) -> {ok, [], []};
                        undefined -> {error, {restart_failed, already_started}}
                    end
            end
    end.

local_first(Ids) ->
    {Local, Remote} = lists:partition(
                        fun({_Name, N}) -> N =:= node() end, Ids),
    Local ++ Remote.

%% The committed membership, read from the ra state machine, as a list of node
%% atoms. This is the live view ADR-033 replaces the startup-config view with.
%%
%% ra answers `members({local, ServerId})' with `{ok, Ids, _}' where each Id is
%% a `{Name, Node}' pair (the third element is the *caller's* server id, not the
%% leader's). If this node's own id is not among the committed voters we are not
%% in the current configuration -- which happens exactly when a majority is
%% down and the committed view does not include us -- and the only honest answer
%% is `{error, no_quorum}' rather than a stale configured list.
live_members(#{members := _Configured}) ->
    case ra:members({local, {?SERVER, node()}}) of
        {ok, Ids, _} ->
            Nodes = [N || {_Name, N} <- Ids],
            case lists:member(node(), Nodes) of
                true -> Nodes;
                false -> no_quorum()
            end;
        {error, _} ->
            no_quorum();
        {timeout, _} ->
            no_quorum()
    end.

members_env() ->
    case application:get_env(broker_cluster, members, []) of
        [] -> [node()];
        Members when is_list(Members) -> Members
    end.

timeout() ->
    application:get_env(broker_cluster, quorum_timeout_ms, 5000).

no_quorum() ->
    _ = (catch broker_metrics:inc(cluster_no_quorum)),
    {error, no_quorum}.
