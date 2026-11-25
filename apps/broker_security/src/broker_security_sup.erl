-module(broker_security_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Audit = #{id => broker_audit,
              start => {broker_audit, start_link, []},
              restart => permanent,
              shutdown => 5000,
              type => worker,
              modules => [broker_audit]},
    Revocation = #{id => broker_revocation,
                   start => {broker_revocation, start_link, []},
                   restart => permanent,
                   shutdown => 5000,
                   type => worker,
                   modules => [broker_revocation]},
    {ok, {#{strategy => one_for_all, intensity => 1, period => 5},
          [Audit, Revocation]}}.
