%% @doc Deduplication window for CloudEvents identity (ADR-020 §D7).
%%
%% A supervised, bounded, in-memory window keyed by
%% `{Tenant, Destination, source, id}'. It is **off by default**: broker
%% redelivery is allowed to duplicate (ADR-007/ADR-019), and silently dropping
%% events is a semantics change an operator must opt into per destination.
%%
%% The process holds no message data -- only `{Key, ExpiresAt}' -- so a large
%% window costs memory but never retains payload bytes. Restarting the app
%% clears the window; cross-restart dedup is explicitly not promised.
-module(broker_event_dedup).

-behaviour(gen_server).

-export([start_link/0, check/2, reset/0, size/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(TAB, broker_event_dedup_table).

%%%_* API ---------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc `fresh' the first time a key is seen inside the window, `duplicate'
%% afterwards. A missing server is treated as "no window" rather than an error:
%% the publish path must stay total.
-spec check(term(), non_neg_integer()) -> fresh | duplicate.
check(Key, WindowMs) when is_integer(WindowMs), WindowMs >= 0 ->
    try gen_server:call(?MODULE, {check, Key, WindowMs}, 5000)
    catch
        _:_ -> fresh
    end.

reset() ->
    try gen_server:call(?MODULE, reset, 5000)
    catch
        _:_ -> ok
    end.

size() ->
    try ets:info(?TAB, size)
    catch
        _:_ -> 0
    end.

%%%_* gen_server --------------------------------------------------------------

init([]) ->
    Max = application:get_env(broker_event, dedup_max_entries, 100000),
    _ = ets:new(?TAB, [named_table, set, protected,
                       {read_concurrency, true}]),
    {ok, #{max => Max}}.

handle_call({check, Key, WindowMs}, _From, #{max := Max} = State) ->
    Now = erlang:monotonic_time(millisecond),
    Reply = case ets:lookup(?TAB, Key) of
                [{_, ExpiresAt}] when ExpiresAt > Now -> duplicate;
                _ -> fresh
            end,
    case Reply of
        duplicate ->
            ok;
        fresh ->
            prune(Now, Max),
            true = ets:insert(?TAB, {Key, Now + max(1, WindowMs)})
    end,
    {reply, Reply, State};
handle_call(reset, _From, State) ->
    true = ets:delete_all_objects(?TAB),
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) -> {noreply, State}.
handle_info(_Info, State) -> {noreply, State}.
terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%%_* Internal ----------------------------------------------------------------

prune(Now, Max) ->
    _ = ets:select_delete(
          ?TAB, [{{'_', '$1'}, [{'=<', '$1', Now}], [true]}]),
    case ets:info(?TAB, size) >= Max of
        true ->
            case ets:first(?TAB) of
                '$end_of_table' -> ok;
                Oldest -> ets:delete(?TAB, Oldest)
            end;
        false ->
            ok
    end.
