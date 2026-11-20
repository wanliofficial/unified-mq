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

-export([enabled/0, config/0, effective/0, redacted_config/0]).

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
