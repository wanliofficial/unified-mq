-module(broker_meta_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Config = #{id => broker_meta_config,
               start => {broker_meta_config, start_link, []},
               restart => permanent,
               shutdown => 5000,
               type => worker,
               modules => [broker_meta_config]},
    Outbox = #{id => broker_meta_outbox,
               start => {broker_meta_outbox, start_link, []},
               restart => permanent,
               shutdown => 5000,
               type => worker,
               modules => [broker_meta_outbox]},
    {ok, {#{strategy => one_for_one, intensity => 5, period => 10},
          [Config, Outbox]}}.
