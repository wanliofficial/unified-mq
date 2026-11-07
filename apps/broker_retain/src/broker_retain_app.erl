%% @doc broker_retain application entry (ADR-018 §18.2).
%%
%% The supervisor starts the single `broker_retain' worker, whose `init/1' loads
%% the snapshot file. A corrupt or unknown-vsn file is a boot blocker on purpose:
%% booting with an empty retain tree would silently tell every new subscriber
%% "nothing was ever retained", which is the same lie `.subs' refuses to tell.
-module(broker_retain_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    broker_retain_sup:start_link().

stop(_State) ->
    ok.
