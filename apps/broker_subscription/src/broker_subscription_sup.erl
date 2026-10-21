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
init([]) ->
    {ok, {#{strategy => one_for_one, intensity => 5, period => 10}, []}}.
