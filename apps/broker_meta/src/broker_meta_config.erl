%% @doc Config version owner (ADR-002, ADR-021 §D1).
%%
%% `bump/1' writes `config_version' and broadcasts `{broker_config, Version,
%% Changed}' to subscribers in one step, so a subscriber that reacts to the
%% event and one that reads the table later see the same version.
%%
%% `reconcile_ms' covers the other direction: an operator (or another node)
%% writing the table directly must still produce a notification. The timer
%% compares the table's version with the cached one and re-broadcasts on a
%% mismatch; it never invents a version of its own.
-module(broker_meta_config).

-behaviour(gen_server).

-export([start_link/0, subscribe/0, unsubscribe/1, bump/1,
         cached_version/0, reconcile_now/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(VERSION_KEY, global).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Subscribe the calling process. Returns a monitor ref the caller can
%% demonitor on exit; the server also cleans subscribers up when they die.
-spec subscribe() -> {ok, reference()}.
subscribe() ->
    gen_server:call(?SERVER, {subscribe, self()}, 5000).

unsubscribe(Ref) ->
    gen_server:call(?SERVER, {unsubscribe, Ref}, 5000).

bump(Changed) ->
    gen_server:call(?SERVER, {bump, Changed}, 5000).

cached_version() ->
    gen_server:call(?SERVER, cached_version, 5000).

reconcile_now() ->
    gen_server:call(?SERVER, reconcile, 5000).

%%%_* gen_server ----------------------------------------------------------

init([]) ->
    ensure_version_row(),
    Version = broker_meta:version(),
    Interval = application:get_env(broker_meta, reconcile_ms, 5000),
    Timer = start_timer(Interval),
    {ok, #{version => Version, subs => #{}, timer => Timer,
           interval => Interval}}.

handle_call({subscribe, Pid}, _From, #{subs := Subs} = St) ->
    Ref = monitor(process, Pid),
    {reply, {ok, Ref}, St#{subs := Subs#{Ref => Pid}}};

handle_call({unsubscribe, Ref}, _From, #{subs := Subs} = St) ->
    _ = demonitor(Ref, [flush]),
    {reply, ok, St#{subs := maps:remove(Ref, Subs)}};

handle_call({bump, Changed}, _From, #{version := V} = St) ->
    V1 = V + 1,
    case broker_meta:put(config_version,
                         {config_version, ?VERSION_KEY, V1, now_ms()}) of
        ok ->
            broadcast(V1, Changed, St),
            {reply, {ok, V1}, St#{version := V1}};
        {error, _} = Error ->
            {reply, Error, St}
    end;

handle_call(cached_version, _From, #{version := V} = St) ->
    {reply, V, St};

handle_call(reconcile, _From, St) ->
    {reply, ok, reconcile(St)};

handle_call(_Req, _From, St) ->
    {reply, {error, unsupported}, St}.

handle_cast(_Msg, St) ->
    {noreply, St}.

handle_info(reconcile, #{interval := Interval} = St) ->
    St1 = reconcile(St),
    {noreply, St1#{timer := start_timer(Interval)}};
handle_info({'DOWN', Ref, process, _Pid, _Reason}, #{subs := Subs} = St) ->
    {noreply, St#{subs := maps:remove(Ref, Subs)}};
handle_info(_Info, St) ->
    {noreply, St}.

terminate(_Reason, #{timer := T}) ->
    _ = erlang:cancel_timer(T),
    ok.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%%%_* Internal ------------------------------------------------------------

ensure_version_row() ->
    case broker_meta:get(config_version, ?VERSION_KEY) of
        {ok, _} -> ok;
        not_found ->
            _ = broker_meta:put(config_version,
                                {config_version, ?VERSION_KEY, 0, now_ms()}),
            ok
    end.

reconcile(#{version := Cached} = St) ->
    Case = broker_meta:version(),
    case Case > Cached of
        true ->
            broadcast(Case, reconcile, St),
            St#{version := Case};
        false ->
            St
    end.

broadcast(Version, Changed, #{subs := Subs}) ->
    Msg = {broker_config, Version, Changed},
    maps:foreach(fun(_Ref, Pid) -> Pid ! Msg end, Subs),
    ok.

start_timer(Interval) when is_integer(Interval), Interval > 0 ->
    erlang:send_after(Interval, self(), reconcile);
start_timer(_) ->
    undefined.

now_ms() ->
    erlang:system_time(millisecond).
