%% @doc The MQTT connection fleet: one transient `mqtt_conn_fsm' per socket.
%% T12's ranch listener will call `start_conn/2' from its accept handshake
%% (`onaccept' → ranch:add_acceptor_pool aside, this is THE entry point a
%% transport hands a socket to the protocol side); the S1 test network layer
%% (mqtt_client.hrl) uses the same call, so T12 adds no new seam.
-module(mqtt_conn_sup).
-behaviour(supervisor).

-export([start_link/0, start_conn/2, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% The socket's controlling process must already be `Pid' (ranch hands this
%% over in `on_accept'; the test helper does it by hand) — a supervisor child
%% cannot inherit a socket it does not own. `undefined' is the ranch-shaped
%% start (T12): the socket arrives later in `{handshake, ...}', after ranch has
%% done the ownership transfer.
-spec start_conn(inet:socket() | undefined, map()) -> {ok, pid()} | {error, term()}.
start_conn(Socket, Opts) ->
    supervisor:start_child(?MODULE, [Socket, Opts]).

init([]) ->
    Child = {mqtt_conn_fsm,
             {mqtt_conn_fsm, start_link, []},
             %% temporary (the plan's "transient" intent, in the stronger form
             %% the supervisor offers): a connection that dies of a socket
             %% error or a protocol violation is GONE — restarting it would
             %% re-arm a dead socket and loop on the same peer.
             temporary, brutal_kill, worker, [mqtt_conn_fsm]},
    %% simple_one_for_one, no intensity cap: conn deaths are per-socket facts
    %% of life, not a fleet-wide failure signal.
    {ok, {#{strategy => simple_one_for_one, intensity => 0, period => 1},
          [Child]}}.
