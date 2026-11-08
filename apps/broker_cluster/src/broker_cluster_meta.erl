%% @doc ra-backed Metadata Raft Group (ADR-021 §D2/§D4).
%%
%% One group carries every partition's owner/epoch/replicas/migration state.
%% Commands are small and infrequent; data-plane appends never go through here
%% (ADR-008 §8.4). A timeout is reported as `{error, no_quorum}' -- that is the
%% honest answer when the group cannot commit, and it is what callers use to
%% refuse reliable writes instead of silently degrading them.
-module(broker_cluster_meta).

-behaviour(gen_server).

-export([start_link/0, command/1, command/2, members/0]).

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

members() ->
    gen_server:call(?CTL, members, 5000).

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

handle_call(members, _From, #{members := Members} = St) ->
    {reply, Members, St};
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
            ra:start_or_restart_cluster(default, broker_cluster, Machine, Ids)
    end.

local_first(Ids) ->
    {Local, Remote} = lists:partition(
                        fun({_Name, N}) -> N =:= node() end, Ids),
    Local ++ Remote.

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
