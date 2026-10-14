%% S0.1 - driver for the 3-node cluster failure scenarios.
%%
%% PHASED BY DESIGN. This module runs INSIDE node n1 via rpc, so it cannot
%% kill its own node: doing so would tear down the rpc connection before the
%% result could be returned. Instead each phase is a separate rpc call, and
%% run_cluster.sh performs the actual kill between phases.
%%
%% Phases:
%%   healthy        - establish leader, write, read back
%%   cost(N)        - replicated write latency (S0.2 multiplies this by group count)
%%   plan_leader    - report which node to kill
%%   after_kill(M)  - observe leadership and writability from a surviving node
%%   plan_follower  - report a non-leader node to kill
%%   plan_majority  - report all nodes, for the no-majority test
-module(s0_1_driver).

-export([healthy/0, cost/1, members/0, plan_leader/0, plan_follower/0,
         after_kill/1, finish/0, all_local/0, ping_leader/0, connectivity/0]).

%% Are the peer BEAM nodes actually connected? Raft cannot elect without this,
%% and a distribution failure looks identical to "no leader" from outside.
connectivity() ->
    Others = nodes(),
    #{self => node(),
      visible => Others,
      pings => [{N, net_adm:ping(N)} || N <- Others],
      ra_ping => ping_leader()}.

%% Diagnostic: every member's local view of membership, plus a ping to the
%% leader. Used to tell "no leader elected" apart from "leader elected but
%% not accepting commands".
all_local() ->
    H = host(),
    [{N, safe(fun() -> ra:members({local, {meta, N}}, 2000) end)}
     || N <- [list_to_atom("ra_n" ++ integer_to_list(I) ++ "@" ++ H)
               || I <- [1, 2, 3]]].

ping_leader() ->
    case members() of
        {ok, _M, Leader} -> safe(fun() -> ra:ping(Leader, 3000) end);
        Other -> {no_leader, Other}
    end.

safe(F) ->
    try F()
    catch C:R -> {C, R}
    end.

-define(CLUSTER, meta).

%% ---------------------------------------------------------------- phases

healthy() ->
    #{members => members(),
      write => write(hello),
      read => read()}.

cost(N) ->
    call(ra_node(), s0_1_cluster, bench, [?CLUSTER, N]).

members() ->
    call(ra_node(), s0_1_cluster, members, [?CLUSTER]).

%% Which node is the leader? The harness kills it between phases.
plan_leader() ->
    case members() of
        {ok, _Members, Leader} -> {kill, Leader};
        Other -> {error, {no_leader, Other}}
    end.

%% A non-leader node, so the majority of 2 must survive its death.
plan_follower() ->
    case members() of
        {ok, Members, Leader} ->
            case [M || M <- Members, M =/= Leader] of
                [Victim | _] -> {kill, Victim};
                [] -> {error, no_follower}
            end;
        Other -> {error, {no_leader, Other}}
    end.

%% Called on a SURVIVING node once some other node has been killed.
after_kill(KilledId) ->
    #{killed => KilledId,
      members => members(),
      write => write({after_kill, KilledId}),
      read => read()}.

finish() ->
    ok.

%% ---------------------------------------------------------------- helpers

write(Term) ->
    call(ra_node(), s0_1_cluster, write, [?CLUSTER, Term]).

read() ->
    call(ra_node(), s0_1_cluster, read, [?CLUSTER]).

%% All phases run on whichever node the harness picks as the survivor, so the
%% node name is derived at call time rather than hardcoded.
ra_node() ->
    list_to_atom("ra_" ++ node_suffix() ++ "@" ++ host()).

node_suffix() ->
    [N | _] = string:split(atom_to_list(node()), "@", all),
    case string:prefix(N, "ra_") of
        nomatch -> N;
        Rest -> Rest
    end.

host() ->
    case inet:gethostname() of
        {ok, H} -> H;
        _ -> "localhost"
    end.

call(N, M, F, A) ->
    try rpc:call(N, M, F, A, 15000)
    catch C:R -> {call_failed, C, R}
    end.