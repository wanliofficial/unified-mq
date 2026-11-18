%% @doc What a publisher actually pays to acquire the router env.
%%
%% The read-path benchmark (`router_read_bench') hands its `env' and `trie' rows
%% an env map built once during setup, because its subject is `route/3'. But
%% `mqtt_conn_fsm:handle_publish_allowed/7' calls `broker_router:env()' on *every*
%% PUBLISH, and that path is not free:
%%
%%   broker_router:env/0            -> application:get_env(backend) (ETS read)
%%     broker_router_hash:current_env/0
%%       broker_router_hash_store:env/1   -> persistent_term:get + alive check
%%       application:get_env(partitions)  -> second ETS read
%%       Env#{partitions => ..., hash => ...}  -> map rebuild per publish
%%
%% So "change 1 turned one gen_server:call into one persistent_term read"
%% understates the per-publish cost, and the 21 -> 13 us in the read-path report
%% is the cost of routing only. This module measures the envelope separately so
%% the two can be added honestly instead of being confused.
%%
%% Everything is measured from a compiled module: doing this from `-eval' binds
%% every result in the shell's environment and inflates millisecond-scale work
%% by 10x.
-module(env_cost).

-export([run/0, run/1]).

-define(DEFAULT_ITERS, 200000).

run() ->
    run(#{}).

run(Opts) ->
    Iters = maps:get(iters, Opts, ?DEFAULT_ITERS),
    ok = ensure_started(),
    %% Warm up so the first sample does not pay for module loading and the first
    %% persistent_term read's code-path setup.
    _ = loop(fun baseline/0, 1000),
    _ = loop(fun env_facade/0, 1000),
    Rows =
        [{Name, measure(Fun, Iters)}
         || {Name, Fun} <- [{baseline, fun baseline/0},
                            {app_env_get, fun app_env_get/0},
                            {persistent_term_only, fun pt_only/0},
                            {store_env, fun store_env/0},
                            {current_env, fun current_env/0},
                            {broker_router_env, fun env_facade/0}]],
    io:format("~n~-22s ~-12s ~-12s~n", ["step", "ns/op", "us/op"]),
    [io:format("~-22s ~-12.1f ~-12.3f~n", [atom_to_list(N), Ns, Ns / 1000])
     || {N, Ns} <- Rows],
    Baseline = proplists:get_value(baseline, Rows),
    io:format("~n~-22s ~-12.1f ~-12.3f~n",
              ["net broker_router:env", proplists:get_value(broker_router_env, Rows) - Baseline,
               (proplists:get_value(broker_router_env, Rows) - Baseline) / 1000]),
    io:format("~nEach row is the mean over ~p iterations; `baseline' is an empty~n"
              "function call, subtracted in the last line.~n", [Iters]),
    Rows.

ensure_started() ->
    %% Start the way a release boots: app env first, then the supervisor, which is
    %% what publishes the backend cache and starts the store with the partition
    %% count. Calling `broker_router_hash:init/1' here instead would measure the
    %% unit-test shape, where `broker_router:env/0' falls back to reading app env
    %% for the backend -- i.e. exactly the cost this change removes.
    _ = application:load(broker_router),
    ok = application:set_env(broker_router, partitions, 4),
    ok = application:set_env(broker_router, backend, broker_router_hash),
    {ok, _Sup} = broker_router_sup:start_link(),
    ok.

%% The floor: a remote call to a function that returns a constant.
baseline() ->
    ok.

%% One static-config read. Before the fix in this commit, `current_env/0' did
%% this on every publish to learn the partition count, which is boot config.
app_env_get() ->
    application:get_env(broker_router, partitions, undefined).

%% Just the persistent_term read change 1 introduced, without the envelope.
pt_only() ->
    persistent_term:get({broker_router_hash_store, broker_router_hash_store, env}).

%% The store's own env/1: persistent_term read plus the dead-store check.
store_env() ->
    broker_router_hash_store:env(broker_router_hash_store).

%% current_env/0: adds the partitions ETS read and the per-publish map rebuild.
current_env() ->
    broker_router_hash:current_env().

%% What the MQTT publish path actually calls.
env_facade() ->
    broker_router:env().

measure(Fun, Iters) ->
    %% Time in native units so microseconds survive; a wall-clock ms timer would
    %% round sub-microsecond differences to zero.
    T0 = erlang:monotonic_time(),
    _ = loop(Fun, Iters),
    T1 = erlang:monotonic_time(),
    Ns = erlang:convert_time_unit(T1 - T0, native, nanosecond),
    Ns / Iters.

loop(_Fun, 0) ->
    ok;
loop(Fun, N) ->
    _ = Fun(),
    loop(Fun, N - 1).
