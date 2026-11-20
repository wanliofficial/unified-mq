%% @doc 跨节点 / 复制通道 TLS 的配置与校验（ADR-035）。
%%
%% 这个模块**不发一个字节、不开一个 socket**。节点间的 TLS 由 OTP 在 boot 期
%% 用 `-proto_dist inet_tls' + `-ssl_dist_optfile' 完成（`inet_tls_dist'）。
%% 它做的事只有三件：
%%
%%   1. 把 `broker_cluster.dist_tls' env 读成一份**声明**；
%%   2. 把节点**实际**的 boot 状态读出来（`init:get_argument/1' +
%%      `ssl_dist_opts' ETS，见 spec §1.1 第 3 条），与声明双向比对；
%%   3. 从同一份声明渲染 optfile 与 vm.args 两行，让运维不必手抄。
%%
%% 为什么要比对而不是「配了就当生效」：vm.args 与 sys.config 是两份独立的
%% 文件，运维改了其中一份而忘了另一份，是这类特性最常见的事故。`-proto_dist
%% inet_tls' 可被应用回读，所以这种漂移**能**在启动时被抓到，而不是等到第一次
%% 跨节点写入才暴露。默认关闭时本模块什么都不做（`validate/0' 立即 `ok'），
%% 这是「默认逐字节不变」的守门。
-module(broker_cluster_dist_tls).

-export([enabled/0, config/0, effective/0, validate/0, validate/1,
         redacted_config/0]).

-export_type([config/0]).

-type config() :: #{enabled := boolean(),
                    certfile := file:name() | undefined,
                    keyfile := file:name() | undefined,
                    cacertfile := file:name() | undefined,
                    verify := verify_peer | verify_none,
                    fail_if_no_peer_cert := boolean(),
                    versions := [atom()],
                    optfile := file:name() | undefined}.

-define(DEFAULTS, #{enabled => false,
                    certfile => undefined,
                    keyfile => undefined,
                    cacertfile => undefined,
                    verify => verify_peer,
                    fail_if_no_peer_cert => true,
                    versions => ['tlsv1.2', 'tlsv1.3'],
                    optfile => undefined}).

%% @doc The declaration, with the shipped defaults filled in.
-spec config() -> config().
config() ->
    case application:get_env(broker_cluster, dist_tls, #{}) of
        Map when is_map(Map) -> maps:merge(?DEFAULTS, Map);
        Other -> erlang:error({invalid_dist_tls_config, Other})
    end.

-spec enabled() -> boolean().
enabled() ->
    maps:get(enabled, config()) =:= true.

%% @doc Admin/log view: which files and modes, never key material. The certfile
%% is reduced to its basename so the view does not leak the deployment layout.
-spec redacted_config() -> map().
redacted_config() ->
    Cfg = config(),
    #{enabled => maps:get(enabled, Cfg),
      certfile => basename_or_undefined(maps:get(certfile, Cfg)),
      verify => maps:get(verify, Cfg),
      fail_if_no_peer_cert => maps:get(fail_if_no_peer_cert, Cfg),
      versions => maps:get(versions, Cfg)}.

basename_or_undefined(undefined) -> undefined;
basename_or_undefined(Path) -> unicode:characters_to_binary(filename:basename(Path)).

%% @doc What this node *actually* booted with, as opposed to what the env
%% declares. Every field here was measured on OTP 28.3.1 (spec §1.1):
%%
%%   * `proto_dist'      -- `init:get_argument(proto_dist)'; `error' means the
%%                          default `inet_tcp'. Present only when `-proto_dist'
%%                          was given at boot.
%%   * `tls_booted'      -- `ssl_dist_sup' is started by `inet_tls_dist' and by
%%                          nothing else, so its existence is proof that this
%%                          node's distribution terminates TLS.
%%   * `optfile'         -- `init:get_argument(ssl_dist_optfile)'.
%%   * `inline_opt'      -- `init:get_argument(ssl_dist_opt)' was used. That
%%                          form works but does NOT populate `ssl_dist_opts',
%%                          so its contents cannot be checked -- which is why
%%                          validate/0 refuses it.
%%   * `server_opts' / `client_opts' -- the options `inet_tls_dist' will really
%%                          hand to `ssl'; `undefined' when there is no table.
-spec effective() -> map().
effective() ->
    #{node_name => node(),
      longnames => net_kernel:longnames() =:= true,
      proto_dist => init:get_argument(proto_dist),
      optfile => init:get_argument(ssl_dist_optfile),
      inline_opt => init:get_argument(ssl_dist_opt),
      tls_booted => whereis(ssl_dist_sup) =/= undefined,
      server_opts => dist_opts(server),
      client_opts => dist_opts(client)}.

%% The table exists only in the optfile path (spec §1.1 finding 3), and only
%% once `ssl_dist_sup' has run. Absent means "no readable options", which is
%% not the same as "no options" -- hence `undefined' rather than `[]'.
dist_opts(Type) ->
    try ets:lookup(ssl_dist_opts, Type) of
        [{Type, Opts}] -> Opts;
        [] -> undefined
    catch
        error:badarg -> undefined
    end.

%%%_* Boot-time check -------------------------------------------------------

%% @doc Boot-time check, as the application calls it.
-spec validate() -> ok | {error, term()}.
validate() ->
    validate(effective()).

%% @doc The check itself, over an explicit boot state so a test can pin every
%% refusal without rebooting a node with different flags.
%%
%% The two directions are both drift and both are refused:
%%
%%   declared TLS + booted plain  -- the operator believes traffic is encrypted.
%%   booted TLS + declared plain  -- traffic is encrypted but nothing says so,
%%                                   so the admin view and the audit trail lie.
%%
%% Only when the two agree do the file checks run, and only when TLS is on: a
%% disabled block must never require PEM files, or the default deployment would
%% stop booting.
-spec validate(map()) -> ok | {error, term()}.
validate(#{proto_dist := ProtoDist} = Boot) ->
    Cfg = config(),
    case {maps:get(enabled, Cfg), proto_dist_mode(ProtoDist)} of
        {false, plain} ->
            check_disabled_boot(Boot);
        {true, plain} ->
            {error, {dist_tls_enabled_but_booted_plain, ProtoDist}};
        {true, tls} ->
            check_enabled_boot(Cfg, Boot);
        {false, tls} ->
            {error, {dist_tls_booted_with_tls_but_disabled,
                     proto_dist_name(ProtoDist)}}
    end.

%% A boot that is already encrypted without being declared is also drift --
%% whether the app is asked to validate or not. `plain' is the only state a
%% disabled declaration accepts.
check_disabled_boot(#{tls_booted := false}) -> ok;
check_disabled_boot(#{proto_dist := ProtoDist}) ->
    {error, {dist_tls_booted_with_tls_but_disabled,
             proto_dist_name(ProtoDist)}}.

check_enabled_boot(Cfg, #{inline_opt := Inline, optfile := OptFile} = Boot) ->
    case {Inline, OptFile} of
        {{ok, _}, error} ->
            %% Readable options are the point; the inline form has none.
            {error, {dist_tls_requires_optfile, see_renderer}};
        _ ->
            check_enabled_files(Cfg, Boot)
    end.

check_enabled_files(Cfg, Boot) ->
    Required = [{certfile, maps:get(certfile, Cfg)},
                {keyfile, maps:get(keyfile, Cfg)},
                {cacertfile, maps:get(cacertfile, Cfg)},
                {optfile, maps:get(optfile, Cfg)}],
    case first_missing(Required) of
        {missing, Which, Path} ->
            {error, {missing_file, Which, Path}};
        none ->
            case first_bad_pem([{certfile, maps:get(certfile, Cfg)},
                                {keyfile, maps:get(keyfile, Cfg)},
                                {cacertfile, maps:get(cacertfile, Cfg)}]) of
                {bad, Which, Path} -> {error, {bad_pem, Which, Path}};
                none -> check_drift(Cfg, Boot)
            end
    end.

%% The declaration and the options `inet_tls_dist' will really use must agree
%% field by field. Paths are normalised first: the optfile may legitimately
%% carry a relative path, and comparing raw strings would report drift for two
%% spellings of the same file.
check_drift(Cfg, #{server_opts := Server, client_opts := Client} = Boot) ->
    case {Server, Client} of
        {undefined, _} -> {error, {dist_tls_optfile_unreadable, server}};
        {_, undefined} -> {error, {dist_tls_optfile_unreadable, client}};
        _ ->
            case first_drift(declared_fields(Cfg), normalize_opts(Server),
                             normalize_opts(Client)) of
                none -> check_fail_if_no_peer_cert(Cfg, Boot);
                {drift, Field, Declared, Actual} ->
                    {error, {dist_tls_optfile_drift, Field, Declared, Actual}}
            end
    end.

%% Server and client must both carry the same view of identity: mTLS means the
%% client side also presents a certificate and verifies the peer's.
%% `fail_if_no_peer_cert' is deliberately absent here -- it is asymmetric (server
%% only) and is checked separately by check_fail_if_no_peer_cert/2.
declared_fields(Cfg) ->
    [{certfile, abs_path(maps:get(certfile, Cfg))},
     {keyfile, abs_path(maps:get(keyfile, Cfg))},
     {cacertfile, abs_path(maps:get(cacertfile, Cfg))},
     {verify, maps:get(verify, Cfg)},
     {versions, maps:get(versions, Cfg)}].

first_drift([], _Server, _Client) -> none;
first_drift([{Field, Declared} | Rest], Server, Client) ->
    Actual = proplists:get_value(Field, Server),
    ClientActual = proplists:get_value(Field, Client),
    case {same(Field, Declared, Actual), same(Field, Declared, ClientActual)} of
        {true, true} -> first_drift(Rest, Server, Client);
        {false, _} -> {drift, Field, Declared, Actual};
        {_, false} -> {drift, Field, Declared, ClientActual}
    end.

same(Field, Declared, Actual) -> Declared =:= normalize_value(Field, Actual).

%% `fail_if_no_peer_cert' is server-only in `ssl', so the two sides are checked
%% in opposite directions:
%%
%%   * server must carry it when the declaration says mTLS. Absent there means
%%     the listener accepts connections with no client certificate -- the exact
%%     opposite of what the operator declared.
%%   * client must NOT carry it. `ssl' rejects the option on a client socket at
%%     connect time, so a node that booted with it would look healthy and then
%%     fail every outbound handshake.
check_fail_if_no_peer_cert(Cfg, #{server_opts := Server, client_opts := Client}) ->
    Declared = maps:get(fail_if_no_peer_cert, Cfg),
    case {proplists:get_value(fail_if_no_peer_cert, Server),
          proplists:get_value(fail_if_no_peer_cert, Client)} of
        {Declared, undefined} -> ok;
        {ServerValue, _} when ServerValue =/= Declared ->
            {error, {dist_tls_optfile_drift, fail_if_no_peer_cert,
                     Declared, ServerValue}};
        {_, ClientValue} ->
            {error, {dist_tls_optfile_drift, fail_if_no_peer_cert,
                     undefined, ClientValue}}
    end.

normalize_value(certfile, V) -> abs_path(V);
normalize_value(keyfile, V) -> abs_path(V);
normalize_value(cacertfile, V) -> abs_path(V);
normalize_value(_Field, V) -> V.

normalize_opts(Opts) ->
    [{K, normalize_value(K, V)} || {K, V} <- Opts].

%% Not named abs/1: that BIF is auto-imported, and a local abs/1 makes every
%% call ambiguous (compiler warning).
abs_path(undefined) -> undefined;
abs_path(Path) -> filename:absname(Path).

proto_dist_mode({ok, [["inet_tls"]]}) -> tls;
proto_dist_mode({ok, [["inet6_tls"]]}) -> tls;
proto_dist_mode(_) -> plain.

proto_dist_name({ok, [[Name]]}) -> list_to_atom(Name);
proto_dist_name(Other) -> Other.

first_missing([]) -> none;
first_missing([{Which, undefined} | _]) -> {missing, Which, undefined};
first_missing([{Which, Path} | Rest]) ->
    case filelib:is_regular(Path) of
        true -> first_missing(Rest);
        false -> {missing, Which, Path}
    end.

%% `public_key:pem_decode/1' needs no running `ssl' application (measured), so
%% a boot check can parse the files before any listener exists.
first_bad_pem([]) -> none;
first_bad_pem([{Which, Path} | Rest]) ->
    case file:read_file(Path) of
        {ok, Bin} ->
            case public_key:pem_decode(Bin) of
                [] -> {bad, Which, Path};
                _ -> first_bad_pem(Rest)
            end;
        {error, _} ->
            %% Missing files were already caught by first_missing/1; a read
            %% error here means it disappeared in between.
            {bad, Which, Path}
    end.
