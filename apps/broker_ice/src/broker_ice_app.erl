%% @doc ICE helper (S10-C): STUN Binding + TURN over UDP (ADR-029).
%%
%% Two postures, both explicit:
%%
%%   * `enabled => false' (the ship default) starts the supervisor with no
%%     children at all -- zero sockets, zero processes, and nothing for an
%%     operator to firewall by accident.
%%   * `enabled => true' runs the full boot validation below, resolves one
%%     config map, and hands it to the supervisor so no other module re-reads
%%     (and possibly re-defaults) the environment.
%%
%% Validation is deliberately loud (ADR-029 §D3, same posture as
%% `broker_proto_a2a_app:check_auth/0'): a TURN server that cannot announce a
%% usable relay address is worse than one that refuses to start, because the
%% failure mode is a browser ICE candidate that silently never works. All four
%% refusals are boot failures:
%%
%%   1. `turn_ip' is neither configured nor resolvable from `hostname' (R-I1);
%%   2. `turn_ip' is an address family we do not implement (IPv6, R-I6);
%%   3. `relay_port_range' is not `{Min, Max}' inside 49152-65535 (ADR-029 §D2);
%%   4. `realm' is missing/empty, or a configured `nonce_secret' is under 16
%%      bytes.
%%
%% This app has no dependency on `broker_proto_webrtc' or on the message
%% kernel (ADR-029 §D1 / ADR-028): it authenticates against `broker_security'
%% and reads no broker_core record.
-module(broker_ice_app).
-behaviour(application).

-export([start/2, stop/1, validate/0, config/0]).

-define(RELAY_MIN, 49152).
-define(RELAY_MAX, 65535).

start(_Type, _Args) ->
    case config() of
        {ok, Cfg} -> broker_ice_sup:start_link(Cfg);
        {error, Reason} -> {error, Reason}
    end.

stop(_State) ->
    ok.

%% @doc Boot validation, exported so a test can pin every refusal without
%% starting a supervisor.
-spec validate() -> ok | {error, term()}.
validate() ->
    case config() of
        {ok, _Cfg} -> ok;
        {error, Reason} -> {error, Reason}
    end.

%% @doc The resolved configuration. `#{enabled => false}' carries nothing else,
%% because a disabled app must not require a realm or a relay address.
-spec config() -> {ok, map()} | {error, term()}.
config() ->
    case enabled() of
        false -> {ok, #{enabled => false}};
        true -> enabled_config()
    end.

enabled_config() ->
    case realm() of
        {ok, Realm} -> enabled_config_1(Realm);
        {error, Reason} -> {error, Reason}
    end.

enabled_config_1(Realm) ->
    case turn_ip() of
        {ok, Ip} -> enabled_config_2(Realm, Ip);
        {error, Reason} -> {error, Reason}
    end.

enabled_config_2(Realm, Ip) ->
    case relay_range() of
        {ok, {Min, Max}} -> enabled_config_3(Realm, Ip, {Min, Max});
        {error, Reason} -> {error, Reason}
    end.

enabled_config_3(Realm, Ip, Range) ->
    case nonce_secret() of
        {ok, Secret} -> {ok, settings(Realm, Ip, Range, Secret)};
        {error, Reason} -> {error, Reason}
    end.

settings(Realm, Ip, {RelayMin, RelayMax} = Range, Secret) ->
    #{enabled => true,
      port => env(port, 3478),
      turn_ip => Ip,
      realm => Realm,
      nonce_secret => Secret,
      relay_port_range => Range,
      first_relay_port => RelayMin,
      last_relay_port => RelayMax,
      max_allocations => env(max_allocations, 1024),
      allocation_lifetime_s => env(allocation_lifetime_s, 600),
      max_permissions => env(max_permissions_per_allocation, 16),
      max_channels => env(max_channels_per_allocation, 16),
      recbuf_bytes => env(recbuf_bytes, 262144)}.

enabled() ->
    application:get_env(broker_ice, enabled, false) =:= true.

env(Key, Default) ->
    application:get_env(broker_ice, Key, Default).

realm() ->
    case application:get_env(broker_ice, realm, undefined) of
        Realm when is_binary(Realm), byte_size(Realm) > 0 -> {ok, Realm};
        undefined -> {error, {missing_config, realm}};
        Other -> {error, {invalid_realm, Other}}
    end.

relay_range() ->
    Default = {?RELAY_MIN, ?RELAY_MAX},
    case application:get_env(broker_ice, relay_port_range, Default) of
        {Min, Max} = Range when is_integer(Min), is_integer(Max) ->
            case Min >= ?RELAY_MIN andalso Max =< ?RELAY_MAX
                andalso Min =< Max of
                true -> {ok, Range};
                false -> {error, {invalid_relay_port_range, {Min, Max}}}
            end;
        Other ->
            {error, {invalid_relay_port_range, Other}}
    end.

nonce_secret() ->
    case application:get_env(broker_ice, nonce_secret, undefined) of
        undefined -> {ok, crypto:strong_rand_bytes(32)};
        Secret when is_binary(Secret), byte_size(Secret) >= 16 -> {ok, Secret};
        Other -> {error, {invalid_nonce_secret, Other}}
    end.

%% `turn_ip' is required for a working relay: the Allocate response advertises
%% it, and a wrong value produces a candidate every peer fails to reach. When
%% it is absent we resolve the `hostname' env's first A record (R-I1) and fail
%% loudly if that yields nothing.
turn_ip() ->
    case application:get_env(broker_ice, turn_ip, undefined) of
        undefined -> resolve_turn_ip();
        {_, _, _, _} = Ip -> {ok, Ip};
        Other -> {error, {invalid_turn_ip, Other}}
    end.

resolve_turn_ip() ->
    case hostname() of
        undefined ->
            {error, {missing_config, turn_ip}};
        Hostname ->
            case inet_res:lookup(Hostname, in, a, []) of
                [{_, _, _, _} = Ip | _] -> {ok, Ip};
                [] -> {error, {turn_ip_unresolved, Hostname}}
            end
    end.

hostname() ->
    case application:get_env(broker_ice, hostname, undefined) of
        Bin when is_binary(Bin), byte_size(Bin) > 0 -> binary_to_list(Bin);
        Str when is_list(Str), Str =/= [] -> Str;
        _ -> undefined
    end.
