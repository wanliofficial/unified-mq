%% @doc The pre-change-1 envelope lookup, as a real `gen_server:call'.
%%
%% `broker_router_hash:current_env/0' used to be a call to the router store
%% before change 1 moved the envelope into `persistent_term'. Change 1's whole
%% benefit is the cost of this round trip, so the benchmark has to pay it for
%% real instead of asserting it in prose.
%%
%% Modelled as a separate module (rather than a raw `send'/`receive' loop in the
%% benchmark) because a `gen_server:call' carries a monitor and a reply hop that
%% a hand-rolled message loop does not -- using the cheap version would understate
%% the very cost being measured.
-module(flat_env).
-behaviour(gen_server).

-export([start/1, table/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

start(Tab) ->
    gen_server:start({local, ?MODULE}, ?MODULE, Tab, []).

%% Exactly one call per route: this is the pre-change read path's first step.
table() ->
    gen_server:call(?MODULE, table).

init(Tab) ->
    {ok, Tab}.

handle_call(table, _From, Tab) ->
    {reply, Tab, Tab}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.
