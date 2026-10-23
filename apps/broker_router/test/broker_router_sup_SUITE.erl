%% @doc T7: what the router needs from the supervision tree at boot.
%%
%% The I12 lesson from T5, applied to the other side of the data path: the
%% subscription index writer was only ever started by test suites calling
%% `broker_router_hash:init/1', so in an assembled release boot step 3
%% (`register/4' backfill) had no index to write into and every `route/3' read
%% an empty table -- silently. This suite pins the three shapes that make the
%% release path real: the store runs under the supervisor, `broker_router:env/0'
%% reaches it without starting anything, and a bad backend name fails the
%% application instead of booting an index nothing writes to.
-module(broker_router_sup_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("broker_core/include/broker_message.hrl").
-include_lib("broker_router/include/broker_router.hrl").

all() ->
    [app_boots_the_store_and_env_reaches_it,
     env_refuses_when_the_store_is_not_running,
     unknown_backend_fails_the_application].

%%%_* Fixtures ------------------------------------------------------------

init_per_suite(Config) ->
    %% CT loads no sys.config; the partition count is app env and must be set
    %% for current_env/0 to have an answer. load is ignored when the umbrella
    %% (or an earlier suite's dependency start) already loaded it.
    _ = application:load(broker_router),
    ok = application:set_env(broker_router, partitions, 4),
    Config.

end_per_suite(_Config) ->
    stop_router(),
    ok.

init_per_testcase(_Case, Config) ->
    stop_router(),
    Config.

end_per_testcase(_Case, _Config) ->
    stop_router(),
    ok.

%% A leaked broker_router app would collide with suites that start the store
%% manually through broker_router_hash:init/1 (same registered name), so it is
%% stopped around every case.
stop_router() ->
    _ = application:stop(broker_router),
    ok.

%%%_* Tests ---------------------------------------------------------------

app_boots_the_store_and_env_reaches_it(_Config) ->
    ok = application:set_env(broker_router, backend, broker_router_hash),
    {ok, _Started} = application:ensure_all_started(broker_router),
    [{broker_router_hash_store, Pid, worker, [broker_router_hash_store]}] =
        supervisor:which_children(broker_router_sup),
    true = is_process_alive(Pid),
    Pid = whereis(broker_router_hash_store),
    %% Not merely started: usable, through the facade every consumer sees.
    {ok, Env} = broker_router:env(),
    Ctx = #route_ctx{tenant_id = <<"t0">>, publisher = undefined,
                     reliability = os_buffer},
    {ok, [{_, []}]} = broker_router_hash:route(<<"a/b">>, Ctx, Env),
    ok = broker_router_hash:register(Env, <<"a/b">>, {<<"c1">>, <<"a/b">>}, #{}),
    {ok, [{_, [{<<"c1">>, <<"a/b">>}]}]} =
        broker_router_hash:route(<<"a/b">>, Ctx, Env),
    ok.

env_refuses_when_the_store_is_not_running(_Config) ->
    %% The failure an un-assembled release used to hide: env/0 must say so
    %% rather than start a process nobody supervises.
    {error, router_store_not_started} = broker_router:env(),
    undefined = whereis(broker_router_hash_store),
    ok.

unknown_backend_fails_the_application(_Config) ->
    ok = application:set_env(broker_router, backend, broker_router_nope),
    {error, _Reason} = application:ensure_all_started(broker_router),
    undefined = whereis(broker_router_hash_store),
    ok = application:set_env(broker_router, backend, broker_router_hash),
    ok.
