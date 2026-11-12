%% @doc Supervisor for the ACP front end.
%%
%% One child per configured agent (`temporary'): a crashed agent is **not**
%% restarted, because the ACP agent owns its session state and a restarted process
%% would face a client that already believes the session is gone. The operator
%% re-launches it. With `agents => []' the supervisor has no children and the
%% application starts clean but inert.
-module(broker_proto_acp_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Agents = broker_proto_acp_app:agents(),
    {ok, {#{strategy => one_for_one, intensity => 1, period => 5},
          [child(Agent) || Agent <- Agents]}}.

child(#{name := Name} = Spec) ->
    #{id => Name,
      start => {acp_agent, start_link, [Spec]},
      restart => maps:get(restart, Spec, temporary),
      shutdown => 5000,
      type => worker,
      modules => [acp_agent]}.
