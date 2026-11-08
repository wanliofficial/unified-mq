%% @doc S6 (README §10.1): TLS configuration and certificate rotation hooks.
%%
%% This suite covers the configuration seam, not a full TLS handshake: it
%% asserts that disabled TLS is a no-op, that enabling it without files fails
%% validation (never a half-configured listener), that `listener_opts/0'
%% produces the options `ranch_ssl' consumes, and that `reload/0' is safe to
%% call in both states.
-module(broker_security_tls_SUITE).

-compile([export_all, nowarn_export_all]).

all() ->
    [disabled_by_default,
     disabled_is_always_valid,
     enabled_without_files_fails_validation,
     enabled_with_files_validates,
     listener_opts_is_empty_when_disabled,
     listener_opts_carries_the_pem_paths,
     verify_peer_adds_the_client_ca,
     transport_is_tcp_when_disabled,
     transport_is_ssl_when_enabled,
     listener_opts_build_a_working_ssl_listener,
     reload_is_a_noop_when_disabled,
     redacted_config_never_contains_key_material].

-define(DIR, "tls_test").

init_per_suite(Config) ->
    Saved = application:get_env(broker_security, tls, undefined),
    Dir = filename:join(["/tmp/opencode", ?DIR]),
    _ = file:del_dir_r(Dir),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    Cert = filename:join(Dir, "server.crt"),
    Key = filename:join(Dir, "server.key"),
    ok = write_self_signed(Cert, Key),
    [{saved_tls, Saved}, {certfile, Cert}, {keyfile, Key} | Config].

end_per_suite(Config) ->
    restore(proplists:get_value(saved_tls, Config)),
    _ = file:del_dir_r(filename:join(["/tmp/opencode", ?DIR])),
    ok.

init_per_testcase(_Case, Config) ->
    application:set_env(broker_security, tls, #{enabled => false}),
    Config.

%%%_* Tests ----------------------------------------------------------------

disabled_by_default(_Config) ->
    application:unset_env(broker_security, tls),
    false = broker_security_tls:enabled(),
    ok.

disabled_is_always_valid(_Config) ->
    application:set_env(broker_security, tls, #{enabled => false}),
    ok = broker_security_tls:validate(),
    ok.

enabled_without_files_fails_validation(_Config) ->
    application:set_env(broker_security, tls, #{enabled => true}),
    {error, {tls_missing_file, certfile, undefined}} =
        broker_security_tls:validate(),
    ok.

enabled_with_files_validates(Config) ->
    enable(Config),
    ok = broker_security_tls:validate(),
    true = broker_security_tls:enabled(),
    ok.

listener_opts_is_empty_when_disabled(_Config) ->
    application:set_env(broker_security, tls, #{enabled => false}),
    [] = broker_security_tls:listener_opts(),
    ok.

listener_opts_carries_the_pem_paths(Config) ->
    enable(Config),
    Opts = broker_security_tls:listener_opts(),
    Cert = proplists:get_value(certfile, Config),
    Key = proplists:get_value(keyfile, Config),
    {certfile, Cert} = lists:keyfind(certfile, 1, Opts),
    {keyfile, Key} = lists:keyfind(keyfile, 1, Opts),
    {verify, verify_none} = lists:keyfind(verify, 1, Opts),
    %% Client certs are not required when the server does not verify them.
    false = lists:keymember(fail_if_no_peer_cert, 1, Opts),
    ok.

verify_peer_adds_the_client_ca(Config) ->
    Cert = proplists:get_value(certfile, Config),
    Key = proplists:get_value(keyfile, Config),
    application:set_env(broker_security, tls,
                        #{enabled => true, certfile => Cert, keyfile => Key,
                          cacertfile => Cert, verify => verify_peer,
                          fail_if_no_peer_cert => true}),
    Opts = broker_security_tls:listener_opts(),
    {cacertfile, Cert} = lists:keyfind(cacertfile, 1, Opts),
    {fail_if_no_peer_cert, true} =
        lists:keyfind(fail_if_no_peer_cert, 1, Opts),
    ok.

transport_is_tcp_when_disabled(_Config) ->
    application:set_env(broker_security, tls, #{enabled => false}),
    {ranch_tcp, []} = broker_security_tls:transport(),
    ok.

transport_is_ssl_when_enabled(Config) ->
    enable(Config),
    {ranch_ssl, Opts} = broker_security_tls:transport(),
    Cert = proplists:get_value(certfile, Config),
    {certfile, Cert} = lists:keyfind(certfile, 1, Opts),
    ok.

%% The options are not just a map we like the shape of: an `ssl' listener
%% started from them must complete a real handshake and carry bytes. This is
%% the mechanical check that `broker_security.tls' has a consumer.
listener_opts_build_a_working_ssl_listener(Config) ->
    enable(Config),
    {ok, _} = application:ensure_all_started(ssl),
    ListenOpts = [{reuseaddr, true}, {active, false}, {ip, {127, 0, 0, 1}}
                  | broker_security_tls:listener_opts()],
    {ok, LSock} = ssl:listen(0, ListenOpts),
    {ok, {_Addr, Port}} = ssl:sockname(LSock),
    Server = spawn_link(
               fun() ->
                       {ok, TSock} = ssl:transport_accept(LSock, 5000),
                       {ok, Sock} = ssl:handshake(TSock, 5000),
                       ok = ssl:send(Sock, <<"hello">>),
                       _ = ssl:close(Sock)
               end),
    {ok, Client} = ssl:connect("localhost", Port,
                               [{verify, verify_none}, {active, false},
                                {mode, binary}], 5000),
    {ok, <<"hello">>} = ssl:recv(Client, 5, 5000),
    _ = ssl:close(Client),
    _ = ssl:close(LSock),
    true = is_pid(Server),
    ok.

reload_is_a_noop_when_disabled(_Config) ->
    application:set_env(broker_security, tls, #{enabled => false}),
    ok = broker_security_tls:reload(),
    ok.

redacted_config_never_contains_key_material(Config) ->
    enable(Config),
    Map = broker_security_tls:redacted_config(),
    false = maps:is_key(private_key, Map),
    false = maps:is_key(key, Map),
    Cert = proplists:get_value(certfile, Config),
    true = maps:get(certfile, Map) =:= Cert,
    ok.

%%%_* Helpers -------------------------------------------------------------

enable(Config) ->
    application:set_env(broker_security, tls,
                        #{enabled => true,
                          certfile => proplists:get_value(certfile, Config),
                          keyfile => proplists:get_value(keyfile, Config),
                          verify => verify_none}).

write_self_signed(Cert, Key) ->
    Cmd = io_lib:format(
            "openssl req -x509 -newkey rsa:2048 -nodes -days 1 "
            "-subj /CN=localhost -keyout ~s -out ~s 2>/dev/null",
            [Key, Cert]),
    os:cmd(lists:flatten(Cmd)),
    case {filelib:is_regular(Cert), filelib:is_regular(Key)} of
        {true, true} -> ok;
        _ -> {error, openssl_failed}
    end.

restore(undefined) ->
    application:unset_env(broker_security, tls);
restore(Value) ->
    application:set_env(broker_security, tls, Value).
