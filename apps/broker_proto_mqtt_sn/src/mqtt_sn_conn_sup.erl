%% @doc Supervisor for MQTT-SN sessions.
%%
%% `simple_one_for_one' because a session has no identity until it is started:
%% the listener spawns one per endpoint that CONNECTs, and the child list is
%% just "who is connected right now". `temporary' is the point -- a session that
%% ends (client disconnect, sleep expiry, keepalive death) must not be
%% resurrected. Nothing outside the listener talks to this supervisor.
-module(mqtt_sn_conn_sup).

-behaviour(supervisor).

-export([start_link/0, start_conn/5, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_conn(atom() | pid(), pid(), gen_udp:socket(),
                 {inet:ip_address(), inet:port_number()}, map()) ->
          {ok, pid()} | {error, term()}.
start_conn(Sup, Listener, Socket, Endpoint, Cfg) ->
    supervisor:start_child(Sup, [Listener, Socket, Endpoint, Cfg]).

init([]) ->
    Child = #{id => mqtt_sn_conn,
              start => {mqtt_sn_conn, start_link, []},
              restart => temporary,
              shutdown => 5000,
              type => worker,
              modules => [mqtt_sn_conn]},
    {ok, {#{strategy => simple_one_for_one, intensity => 10, period => 1},
          [Child]}}.
