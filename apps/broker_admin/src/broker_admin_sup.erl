-module(broker_admin_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% ADR-032 D2: exposing the management API beyond loopback with an empty
    %% token would let anyone who can reach the port create backups and reload
    %% TLS. That invariant used to live only in comments and docs; enforce it at
    %% boot so a misconfiguration fails loudly instead of serving.
    case check_bind_auth() of
        ok ->
            {ok, {#{strategy => one_for_one, intensity => 5, period => 10},
                  children()}};
        {error, Reason} ->
            {stop, Reason}
    end.

children() ->
    %% The drain state machine is part of the admin app regardless of `enabled':
    %% `/readyz' and the drain endpoints are the contract `/readyz' consumers
    %% rely on, and a node with the API switched off still needs a well-defined
    %% drain state (it is simply always `up').
    [{broker_drain, {broker_drain, start_link, []},
      permanent, 5000, worker, [broker_drain]} |
     case application:get_env(broker_admin, enabled, true) of
         true ->
             [{broker_admin_listener, {broker_admin_listener, start_link, []},
               permanent, 5000, worker, [broker_admin_listener]}];
         _ ->
             []
     end].

%%%_* Bind / auth invariant -----------------------------------------------

check_bind_auth() ->
    case application:get_env(broker_admin, enabled, true) of
        false ->
            %% No listener, no exposure.
            ok;
        _ ->
            Bind = application:get_env(broker_admin, bind, {127, 0, 0, 1}),
            case {is_loopback(Bind), configured_token()} of
                {true, _} -> ok;
                {false, <<>>} -> {error, {insecure_bind, Bind, empty_token}};
                {false, _Token} -> ok
            end
    end.

%% Anything that is not literally loopback is treated as wide, matching the
%% posture of the other listeners in this project (`broker_proto_mcp_app').
is_loopback({127, _, _, _}) -> true;
is_loopback({0, 0, 0, 0, 0, 0, 0, 1}) -> true;
is_loopback(<<"localhost">>) -> true;
is_loopback(_) -> false.

configured_token() ->
    case application:get_env(broker_admin, token, <<>>) of
        T when is_binary(T) -> T;
        T when is_list(T) -> list_to_binary(T);
        _ -> <<>>
    end.
