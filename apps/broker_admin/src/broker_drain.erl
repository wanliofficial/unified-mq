%% @doc Node-local drain state machine (S12-A, ADR-032 D1/D2).
%%
%% Three states, `up | draining | down', held on the node itself and never in
%% ra: a drain has to keep working *through* a loss of quorum (that is exactly
%% the moment an operator most needs to shed traffic), so it cannot depend on
%% the Raft group. The management API on 8090 is the single writer (D1).
%%
%%   * `up'       -- serving. `/readyz' answers 200 when the required apps are up.
%%   * `draining' -- stopped accepting new connections; in-use connections are
%%                   left to end naturally (or the grace period expires).
%%                   `/readyz' is 503 with `status => draining'.
%%   * `down'     -- the node is being removed. `/readyz' is 503 `status => down'.
%%
%% The mechanism for "stop accepting" is suspending every ranch listener on the
%% node. `ranch:suspend_listener/1' terminates only `ranch_acceptors_sup', so
%% `ranch_conns_sup' and every live connection survive -- D2's "不迁移连接、不断开
%% 在用连接". The admin API's own listener is a hand-written `gen_tcp' acceptor,
%% not a ranch child, so it stays reachable while draining; that is what makes
%% `undrain' callable mid-drain.
%%
%% State is mirrored into a `persistent_term' so the `/readyz' hot path never
%% calls into this process. This gen_server is the single writer, and it owns
%% the invariant "advertised state matches reality":
%%
%%   * `init/1' resumes every ranch listener, so a restart of this process puts
%%     the node back in rotation instead of leaving listeners suspended while
%%     `/readyz' claims `ready';
%%   * `terminate/2' resets the advertised state to `up'.
%%
%% Both are deliberate fail-open choices: a bookkeeping process must not be able
%% to take a node out of rotation by dying.
-module(broker_drain).
-behaviour(gen_server).

-export([start_link/0, drain/0, drain/1, undrain/0, status/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(PT, {?MODULE, state}).

%% `erlang:send_after/3' and the HTTP body validation share this ceiling.
-define(MAX_TIMEOUT_MS, 16#FFFFFFFF).

-type state() :: up | draining | down.
-export_type([state/0]).

-record(s, {state = up :: state(),
            suspended = [] :: [term()],
            timer = undefined :: reference() | undefined}).

%%%_* API -------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Current drain state. Reads the `persistent_term' mirror, so the
%% readiness probe never blocks on this process.
-spec status() -> state().
status() ->
    persistent_term:get(?PT, up).

%% @doc Drain using the configured `drain_timeout_ms' (from `broker_admin' env;
%% default `infinity' = stay draining until an explicit `undrain').
-spec drain() -> {ok, state(), [term()]} | {error, down}.
drain() ->
    gen_server:call(?SERVER, drain, 30000).

%% @doc Drain with an explicit grace period in ms, after which the node moves to
%% `down' on its own -- this is what consumes `drain_timeout_ms' (R-E6). Pass
%% `infinity' to never auto-advance, or `0' to go `down' immediately (both still
%% suspend the listeners first, and neither disconnects an in-use connection).
-spec drain(infinity | non_neg_integer()) ->
          {ok, state(), [term()]} | {error, down}.
drain(TimeoutMs) when TimeoutMs =:= infinity;
                      is_integer(TimeoutMs), TimeoutMs >= 0,
                      TimeoutMs =< ?MAX_TIMEOUT_MS ->
    gen_server:call(?SERVER, {drain, TimeoutMs}, 30000).

%% @doc Resume accepting and return to `up'.
-spec undrain() -> {ok, up}.
undrain() ->
    gen_server:call(?SERVER, undrain, 30000).

%%%_* gen_server ------------------------------------------------------------

init([]) ->
    process_flag(trap_exit, true),
    %% A previous incarnation may have died while listeners were suspended
    %% (`kill' skips `terminate/2'), which would leave the node deaf while this
    %% process advertises `up'. Resuming is a no-op for a listener that is
    %% already running, so this is safe on a clean boot too.
    _ = resume_all(ranch_refs()),
    persistent_term:put(?PT, up),
    {ok, #s{}}.

handle_call(drain, _From, S) ->
    do_drain(S, config_timeout());
handle_call({drain, Timeout}, _From, S) ->
    do_drain(S, Timeout);
handle_call(undrain, _From, S) ->
    do_undrain(S);
handle_call(_Req, _From, S) ->
    {reply, {error, unsupported}, S}.

handle_cast(_Msg, S) ->
    {noreply, S}.

%% The grace period elapsed: advance to `down'. In-use connections are
%% deliberately left alone (D2 forbids disconnecting them); `down' is the signal
%% that the node may now be stopped.
handle_info(drain_timeout, #s{state = draining} = S) ->
    set_state(down, S#s.state, S#s.suspended),
    {noreply, S#s{state = down, timer = undefined}};
handle_info(drain_timeout, S) ->
    {noreply, S};
handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, _S) ->
    %% A dying drain process must not leave the node advertising `draining'.
    persistent_term:put(?PT, up),
    ok.

code_change(_Old, S, _Extra) ->
    {ok, S}.

%%%_* Internals -------------------------------------------------------------

do_drain(#s{state = down} = S, _Timeout) ->
    {reply, {error, down}, S};
do_drain(#s{} = S, Timeout) ->
    Suspended = suspend_all(ranch_refs()),
    Timer = arm_timer(Timeout),
    NewS = #s{state = draining, suspended = Suspended, timer = Timer},
    set_state(draining, S#s.state, Suspended),
    {reply, {ok, draining, Suspended}, NewS}.

do_undrain(#s{} = S) ->
    _ = resume_all(S#s.suspended),
    NewS = #s{state = up, suspended = [], timer = cancel_timer(S#s.timer)},
    set_state(up, S#s.state, S#s.suspended),
    {reply, {ok, up}, NewS}.

%% @doc Every ranch listener on the node. Empty when ranch is not running (a
%% data-plane-less boot, or the admin suite), which makes drain a no-op there.
ranch_refs() ->
    case code:ensure_loaded(ranch) of
        {module, ranch} ->
            try maps:keys(ranch:info())
            catch _:_ -> []
            end;
        _ ->
            []
    end.

suspend_all(Refs) ->
    [R || R <- Refs, safe_suspend(R) =:= ok].

resume_all(Refs) ->
    [R || R <- Refs, safe_resume(R) =:= ok].

safe_suspend(Ref) ->
    try ranch:suspend_listener(Ref) of
        ok -> ok;
        _Other -> {error, suspend_failed}
    catch
        _:_ -> {error, suspend_failed}
    end.

safe_resume(Ref) ->
    try ranch:resume_listener(Ref) of
        ok -> ok;
        _Other -> {error, resume_failed}
    catch
        _:_ -> {error, resume_failed}
    end.

%% `0' means "go down now", so the grace period has to be armed rather than
%% skipped; only `infinity' means "wait for an explicit undrain".
arm_timer(infinity) -> undefined;
arm_timer(TimeoutMs) when is_integer(TimeoutMs), TimeoutMs >= 0 ->
    erlang:send_after(TimeoutMs, self(), drain_timeout).

cancel_timer(undefined) -> undefined;
cancel_timer(TRef) ->
    _ = erlang:cancel_timer(TRef),
    undefined.

%% Update the mirror, then log a real transition. `Old' is the state *before* the
%% change and `Suspended' is what the new state has suspended, so the line
%% describes the transition rather than repeating the new state twice (D2:
%% "用显式状态变更事件").
set_state(New, Old, Suspended) ->
    persistent_term:put(?PT, New),
    case Old =:= New of
        true -> ok;
        false ->
            logger:notice("broker_drain: ~s -> ~s (~b listener(s) suspended)",
                          [Old, New, length(Suspended)])
    end,
    ok.

config_timeout() ->
    case application:get_env(broker_admin, drain_timeout_ms, infinity) of
        T when T =:= infinity ->
            infinity;
        T when is_integer(T), T >= 0, T =< ?MAX_TIMEOUT_MS ->
            T;
        Other ->
            logger:warning("broker_drain: ignoring drain_timeout_ms=~p", [Other]),
            infinity
    end.
