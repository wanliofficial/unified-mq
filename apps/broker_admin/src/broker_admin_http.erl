%% @doc Admin API router (S6, roadmap §8.2).
%%
%% Pure request -> response mapping; it never owns a socket. The listener does
%% the HTTP framing and calls `handle/4' in a short-lived process, so a slow
%% endpoint (a backup, for example) cannot block the acceptor.
%%
%% Endpoints:
%%
%%   GET  /healthz             liveness  -- process alive, always 200
%%   GET  /readyz              readiness -- required apps up, else 503
%%   GET  /metrics             Prometheus text exposition (closed key set)
%%   GET  /api/v1/status       node, partitions, cluster, retention
%%   GET  /api/v1/config       redacted config audit snapshot + sha256
%%   GET  /api/v1/version      release / OTP / data-format versions
%%   GET  /api/v1/backups      backup inventory
%%   POST /api/v1/backup       create a backup
%%   POST /api/v1/tls/reload   clear the ssl PEM cache (cert rotation)
%%
%% Authentication: when `broker_admin.token' is a non-empty binary, every
%% endpoint except `/healthz' and `/readyz' requires `Authorization: Bearer
%% <token>' (or `X-Admin-Token'). An empty token means "no auth" and is only
%% acceptable on the default loopback bind; `docs/operations.md' calls that out.
-module(broker_admin_http).

-export([handle/4, metrics_text/0]).

-define(PROTECTED_ALWAYS, [<<"/api/">>]).

%% @doc Map one parsed request to `{Status, ContentType, Body}'.
-spec handle(get | post | put | delete | atom() | binary(),
             binary(), map(), binary()) ->
          {non_neg_integer(), binary(), iodata()}.
handle(Method, Path0, Headers, Body) ->
    Path = strip_query(Path0),
    case authorize(Path, Headers) of
        ok ->
            route(Method, Path, Headers, Body);
        deny ->
            json(401, #{error => unauthorized})
    end.

%%%_* Routing --------------------------------------------------------------

route(get, <<"/healthz">>, _H, _B) ->
    json(200, #{status => ok});
route(get, <<"/readyz">>, _H, _B) ->
    case missing_apps() of
        [] -> json(200, #{status => ready});
        Missing -> json(503, #{status => not_ready, missing => Missing})
    end;
route(get, <<"/metrics">>, _H, _B) ->
    {200, <<"text/plain; version=0.0.4">>, metrics_text()};
route(get, <<"/api/v1/status">>, _H, _B) ->
    json(200, status());
route(get, <<"/api/v1/config">>, _H, _B) ->
    json(200, #{hash => broker_config_audit:hash(),
                config => broker_config_audit:snapshot()});
route(get, <<"/api/v1/version">>, _H, _B) ->
    json(200, broker_version:info());
route(get, <<"/api/v1/backups">>, _H, _B) ->
    json(200, #{backups => broker_backup:list()});
route(post, <<"/api/v1/backup">>, _H, _B) ->
    case broker_backup:create() of
        {ok, Name} -> json(201, #{backup => to_bin(Name)});
        {error, Reason} -> json(500, #{error => to_bin(io_lib:format("~p", [Reason]))})
    end;
route(post, <<"/api/v1/tls/reload">>, _H, _B) ->
    try broker_security_tls:reload() of
        ok -> json(200, #{reloaded => true})
    catch
        Class:Reason ->
            json(500, #{error => to_bin(io_lib:format("~p:~p", [Class, Reason]))})
    end;
route(_Method, <<"/healthz">>, _H, _B) ->
    json(405, #{error => method_not_allowed});
route(_Method, <<"/readyz">>, _H, _B) ->
    json(405, #{error => method_not_allowed});
route(_Method, <<"/metrics">>, _H, _B) ->
    json(405, #{error => method_not_allowed});
route(_Method, _Path, _H, _B) ->
    json(404, #{error => not_found}).

%%%_* Status ---------------------------------------------------------------

status() ->
    #{node => to_bin(atom_to_binary(node(), utf8)),
      uptime_seconds => uptime_seconds(),
      version => broker_version:info(),
      partitions => safe_partitions(),
      retention => retention(),
      cluster => cluster(),
      metrics => safe_snapshot()}.

%% broker_queue may not be running (the admin API is deliberately bootable
%% before the data plane), and a misconfigured partition count raises from
%% `partition_ids/0'. A status endpoint must always answer.
safe_partitions() ->
    try broker_queue:partition_ids() of
        Ps -> Ps
    catch
        _:_ -> []
    end.

retention() ->
    [ #{partition => P, oldest => O, bytes => B, reason => to_bin(atom_to_binary(R, utf8))}
      || {P, O, B, R} <- broker_queue:retention_status() ].

cluster() ->
    try broker_cluster:snapshot() of
        Nodes -> Nodes
    catch
        _:_ -> []
    end.

safe_snapshot() ->
    try broker_metrics:snapshot() of
        S -> S
    catch
        _:_ -> #{}
    end.

uptime_seconds() ->
    {Up, _SinceLast} = erlang:statistics(wall_clock),
    Up div 1000.

%%%_* Readiness -------------------------------------------------------------

%% The apps whose absence means "do not send me traffic". Metrics and admin
%% itself are deliberately excluded: a missing metrics reporter must not take
%% the broker out of a load balancer.
missing_apps() ->
    Required = [broker_storage, broker_router, broker_queue,
                broker_subscription, broker_event, broker_security],
    [A || A <- Required, not started(A)].

started(App) ->
    case application:get_application(App) of
        {ok, _} -> true;
        _ -> false
    end orelse lists:keymember(App, 1, application:which_applications()).

%%%_* Prometheus text exposition -------------------------------------------

%% @doc Render `broker_metrics:snapshot/0' as Prometheus text. The key set is
%% closed (ADR-016), so no attacker-controlled `source'/`subject'/`id' can ever
%% become a label -- that is the high-cardinality guard README §10.1 asks for.
-spec metrics_text() -> binary().
metrics_text() ->
    Snapshot = safe_snapshot(),
    Types = metric_types(),
    Lines =
        lists:flatmap(
          fun(Key) ->
                  case maps:get(Key, Snapshot, undefined) of
                      undefined -> [];
                      Value -> render_metric(Key, maps:get(Key, Types, gauge), Value)
                  end
          end, lists:sort(maps:keys(Snapshot))),
    iolist_to_binary(Lines).

metric_types() ->
    broker_metrics:key_types().

render_metric(Key, counter, Value) when is_integer(Value) ->
    [name(Key), <<" ">>, integer_to_binary(Value), <<"\n">>];
render_metric(Key, gauge, Value) when is_integer(Value) ->
    [name(Key), <<" ">>, integer_to_binary(Value), <<"\n">>];
render_metric(Key, histogram, #{count := Count, sum := Sum, buckets := Buckets}) ->
    Base = [name(Key), <<"_count ">>, integer_to_binary(Count), <<"\n">>,
            name(Key), <<"_sum ">>, integer_to_binary(Sum), <<"\n">>],
    Cum = cumulative(Buckets),
    BucketLines =
        [[name(Key), <<"_bucket{le=\"">>, bound(B), <<"\">} ">>,
          integer_to_binary(C), <<"\n">>]
         || {B, C} <- Cum],
    [Base, BucketLines];
render_metric(Key, _Type, Value) ->
    [name(Key), <<" ">>, to_bin(io_lib:format("~p", [Value])), <<"\n">>].

cumulative(Buckets) ->
    {Out, _} =
        lists:foldl(
          fun({B, C}, {Acc, Running}) ->
                  R = Running + C,
                  {[{B, R} | Acc], R}
          end, {[], 0}, Buckets),
    lists:reverse(Out).

bound(infinity) -> <<"+Inf">>;
bound(B) when is_integer(B) -> integer_to_binary(B).

name(Key) ->
    atom_to_binary(Key, utf8).

%%%_* Auth ----------------------------------------------------------------

authorize(<<"/healthz">>, _Headers) -> ok;
authorize(<<"/readyz">>, _Headers) -> ok;
authorize(_Path, Headers) ->
    case configured_token() of
        <<>> -> ok;
        Token -> token_ok(Headers, Token)
    end.

token_ok(Headers, Token) ->
    case header(Headers, <<"authorization">>) of
        <<"Bearer ", T/binary>> -> decision(T, Token);
        _ ->
            case header(Headers, <<"x-admin-token">>) of
                undefined -> deny;
                T -> decision(T, Token)
            end
    end.

decision(Provided, Expected) ->
    case secure_equal(Provided, Expected) of
        true -> ok;
        false -> deny
    end.

%% Constant-time comparison so a token cannot be guessed byte by byte by
%% timing the admin API.
secure_equal(A, B) when is_binary(A), is_binary(B) ->
    case byte_size(A) =:= byte_size(B) of
        false -> false;
        true -> secure_equal(A, B, 0) =:= 0
    end;
secure_equal(_, _) ->
    false.

secure_equal(<<>>, <<>>, Acc) -> Acc;
secure_equal(<<A, RestA/binary>>, <<B, RestB/binary>>, Acc) ->
    secure_equal(RestA, RestB, Acc bor (A bxor B));
secure_equal(_, _, _) -> 1.

configured_token() ->
    case application:get_env(broker_admin, token, <<>>) of
        T when is_binary(T) -> T;
        T when is_list(T) -> list_to_binary(T);
        _ -> <<>>
    end.

header(Headers, Name) ->
    maps:get(Name, Headers, undefined).

%%%_* Utilities ------------------------------------------------------------

strip_query(Path) ->
    case binary:split(Path, <<"?">>) of
        [P | _] -> P;
        [] -> Path
    end.

json(Status, Map) ->
    {Status, <<"application/json">>, json:encode(json_safe(Map))}.

%% JSON has no tuples, pids or refs. Every admin response goes through this so a
%% single un-encodable field (a cluster metadata tuple, a pid) degrades to a
%% string instead of a 500.
json_safe(M) when is_map(M) ->
    maps:from_list([{json_key(K), json_safe(V)} || {K, V} <- maps:to_list(M)]);
json_safe(L) when is_list(L) ->
    [json_safe(V) || V <- L];
json_safe(T) when is_tuple(T) ->
    json_safe(tuple_to_list(T));
json_safe(B) when is_binary(B) -> B;
json_safe(A) when is_atom(A) -> atom_to_binary(A, utf8);
json_safe(I) when is_integer(I) -> I;
json_safe(F) when is_float(F) -> F;
json_safe(P) when is_pid(P) -> list_to_binary(pid_to_list(P));
json_safe(R) when is_reference(R) -> list_to_binary(ref_to_list(R));
json_safe(Other) -> to_bin(io_lib:format("~p", [Other])).

json_key(K) when is_binary(K) -> K;
json_key(K) when is_atom(K) -> atom_to_binary(K, utf8);
json_key(K) when is_integer(K) -> integer_to_binary(K);
json_key(K) -> to_bin(io_lib:format("~p", [K])).

to_bin(B) when is_binary(B) -> B;
to_bin(L) when is_list(L) -> iolist_to_binary(L).
