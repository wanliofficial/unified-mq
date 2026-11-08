%% @doc TLS configuration and certificate rotation (S6, README §10.1).
%%
%% One place decides how a listener terminates TLS, so every adapter can opt in
%% with the same options and the same rotation story. Configuration lives in
%% `broker_security.tls':
%%
%% ```
%% {tls, #{enabled      => false,
%%         certfile     => "config/tls/server.crt",
%%         keyfile      => "config/tls/server.key",
%%         cacertfile   => undefined,          % client CA, optional
%%         verify       => verify_none,        % verify_peer for mTLS
%%         fail_if_no_peer_cert => false,
%%         versions     => ['tlsv1.2', 'tlsv1.3']}}
%% '''
%%
%% Rotation: replace the PEM files at the same paths and call `reload/0'. The
%% call clears the `ssl' PEM cache, which is what makes the next handshake read
%% the new files -- no node restart, no listener restart. Existing connections
%% keep the certificate they handshook with, which is the standard TLS
%% behaviour and is documented in `docs/operations.md'.
-module(broker_security_tls).

-export([enabled/0, config/0, listener_opts/0, transport/0, validate/0,
         reload/0, redacted_config/0]).

-export_type([config/0]).

-type config() :: #{enabled := boolean(),
                    certfile := file:filename() | undefined,
                    keyfile := file:filename() | undefined,
                    cacertfile := file:filename() | undefined,
                    verify := verify_none | verify_peer,
                    fail_if_no_peer_cert := boolean(),
                    versions := [atom()]}.

-define(DEFAULTS, #{enabled => false,
                    certfile => undefined,
                    keyfile => undefined,
                    cacertfile => undefined,
                    verify => verify_none,
                    fail_if_no_peer_cert => false,
                    versions => ['tlsv1.2', 'tlsv1.3']}).

-spec config() -> config().
config() ->
    case application:get_env(broker_security, tls, #{}) of
        Map when is_map(Map) -> maps:merge(?DEFAULTS, Map);
        Other -> erlang:error({invalid_tls_config, Other})
    end.

-spec enabled() -> boolean().
enabled() ->
    maps:get(enabled, config()) =:= true.

%% @doc Options for `ranch_ssl' / `ssl:listen'. Empty when TLS is disabled.
%% Callers that support both transports choose `ranch_tcp' vs `ranch_ssl' with
%% `enabled/0' and pass these options to the latter.
-spec listener_opts() -> [ssl:tls_server_option()].
listener_opts() ->
    case enabled() of
        false ->
            [];
        true ->
            Cfg = config(),
            Base = [{certfile, maps:get(certfile, Cfg)},
                    {keyfile, maps:get(keyfile, Cfg)},
                    {verify, maps:get(verify, Cfg)},
                    {versions, maps:get(versions, Cfg)}],
            WithCa = case maps:get(cacertfile, Cfg) of
                         undefined -> Base;
                         Ca -> [{cacertfile, Ca} | Base]
                     end,
            case maps:get(verify, Cfg) of
                verify_peer ->
                    [{fail_if_no_peer_cert, maps:get(fail_if_no_peer_cert, Cfg)}
                     | WithCa];
                _ ->
                    WithCa
            end
    end.

%% @doc The ranch transport a listener should use, with the socket options the
%% transport needs. Disabled TLS yields `{ranch_tcp, []}' -- the pre-S6 shape --
%% so every adapter has exactly one call site and no listener can "know" about
%% TLS without actually consuming the configuration.
-spec transport() -> {ranch_tcp | ranch_ssl, [ssl:tls_server_option()]}.
transport() ->
    case enabled() of
        false -> {ranch_tcp, []};
        true -> {ranch_ssl, listener_opts()}
    end.

%% @doc Boot-time check. Only fails when TLS is enabled and a required file is
%% missing -- a disabled TLS block is always valid.
-spec validate() -> ok | {error, term()}.
validate() ->
    case enabled() of
        false ->
            ok;
        true ->
            Cfg = config(),
            Required = [{certfile, maps:get(certfile, Cfg)},
                        {keyfile, maps:get(keyfile, Cfg)}],
            case first_missing(Required) of
                none -> ok;
                {missing, Key, Path} -> {error, {tls_missing_file, Key, Path}}
            end
    end.

first_missing([]) ->
    none;
first_missing([{Key, undefined} | _]) ->
    {missing, Key, undefined};
first_missing([{Key, Path} | Rest]) ->
    case filelib:is_regular(Path) of
        true -> first_missing(Rest);
        false -> {missing, Key, Path}
    end.

%% @doc Re-read the certificate/key files on the next handshake. Safe to call
%% when TLS is disabled (a no-op).
-spec reload() -> ok.
reload() ->
    case enabled() of
        false -> ok;
        true -> ssl:clear_pem_cache()
    end.

%% @doc Admin/log view: paths and modes, never key material or passwords.
-spec redacted_config() -> map().
redacted_config() ->
    Cfg = config(),
    #{enabled => maps:get(enabled, Cfg),
      certfile => maps:get(certfile, Cfg),
      keyfile => maps:get(keyfile, Cfg),
      cacertfile => maps:get(cacertfile, Cfg),
      verify => maps:get(verify, Cfg),
      fail_if_no_peer_cert => maps:get(fail_if_no_peer_cert, Cfg),
      versions => maps:get(versions, Cfg)}.
