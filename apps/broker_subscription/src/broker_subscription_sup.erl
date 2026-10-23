-module(broker_subscription_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% Boot steps 2 and 3 of design §6: rebuild subscription entities (minting
%% SubRefs) from sessions/*.subs, then backfill the Router index via
%% broker_router:register/4. Both must complete before broker_proto_mqtt starts
%% its listener, otherwise R1b ("subscription set survives restart") races with
%% new SUBSCRIBE frames.
%%
%% The worker *is* steps 2+3: its init/1 is all-or-nothing and a failure stops
%% the application rather than serving a half-loaded session set. permanent,
%% because everything it owns is rebuilt from disk -- the tables die with it and
%% nothing outlives a restart.
init([]) ->
    {ok, {#{strategy => one_for_one, intensity => 5, period => 10},
          [#{id => broker_subscription_worker,
             start => {broker_subscription_worker, start_link, []},
             restart => permanent,
             shutdown => 5000,
             type => worker,
             modules => [broker_subscription_worker]}]}}.
