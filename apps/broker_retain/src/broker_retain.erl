%% @doc Retained messages: one `#broker_message{}' per topic, snapshot-persisted
%% (ADR-018 §18.2).
%%
%% Why a separate app: the retain tree is neither a subscription (it has no
%% cursor and no per-client state) nor a WAL artifact (a partition log is
%% ordered by offset, retained state is "the last value per topic"). Putting it
%% in `broker_subscription' would grow a 600-line worker with a fourth kind of
%% state; putting it in `broker_storage' would fight the frozen ADR-011 shape.
%%
%% Persistence contract (the reason `put/2' has a mode):
%%
%%   * `sync'  -- the snapshot is fsynced **before** the caller acks. A QoS 1/2
%%     retained publish must not have its PUBACK/PUBREC outrun the file
%%     (ADR-007: an ack means "durable"), so those callers use this mode.
%%   * `async' -- memory is updated immediately, the snapshot is rewritten on a
%%     `retain_flush_interval_ms' timer. QoS 0 has no durability promise, so a
%%     crash losing the last window is consistent with its level.
%%
%% The tree is one map in one process: a retained publish is a serialization
%% point. That is the accepted S2 cost (ADR-018 "后果"); retained publishes are
%% low-frequency state broadcasts, and S2-I measures the update latency.
%%
%% Topic-filter matching is deliberately **not** here: `match/1' would need
%% MQTT filter semantics, which live in `mqtt_filter' one layer up. The protocol
%% adapter walks `entries/0' and matches in its own process.
-module(broker_retain).
-behaviour(gen_server).

-include_lib("broker_core/include/broker_message.hrl").

-export([start_link/0, put/2, delete/2, lookup/1, entries/0, stats/0, flush/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).
-define(TIMER, retain_flush_tick).

-record(st, {tree = #{}      :: #{binary() => #broker_message{}},
             count = 0       :: non_neg_integer(),
             bytes = 0       :: non_neg_integer(),
             dirty = false   :: boolean(),
             timer           :: reference() | undefined}).

%%%_* API ---------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Store (or replace) the retained message of its own topic. `Mode' is
%% `sync' (fsync before replying) or `async' (timer). Returns `{error, overflow}'
%% when a configured cap rejects it -- the caller still publishes the message to
%% live subscribers, it just is not retained.
-spec put(#broker_message{}, sync | async) -> ok | {error, term()}.
put(#broker_message{} = Msg, Mode) ->
    gen_server:call(?SERVER, {put, Msg, Mode}).

-spec delete(binary(), sync | async) -> ok | {error, term()}.
delete(Topic, Mode) ->
    gen_server:call(?SERVER, {delete, Topic, Mode}).

-spec lookup(binary()) -> {ok, #broker_message{}} | error.
lookup(Topic) ->
    gen_server:call(?SERVER, {lookup, Topic}).

%% @doc Every retained topic, sorted by topic name: the protocol adapter filters
%% this list with its own `mqtt_filter:matches/2' and sends in this order, which
%% makes "one SUBSCRIBE, several retained topics" deterministic.
-spec entries() -> [{binary(), #broker_message{}}].
entries() ->
    gen_server:call(?SERVER, entries).

-spec stats() -> #{count := non_neg_integer(), bytes := non_neg_integer(),
                   dirty := boolean()}.
stats() ->
    gen_server:call(?SERVER, stats).

%% @doc Force a flush (tests and the graceful shutdown path).
-spec flush() -> ok | {error, term()}.
flush() ->
    gen_server:call(?SERVER, flush).

%%%_* gen_server --------------------------------------------------------------

init([]) ->
    case broker_retain_files:read() of
        {ok, Entries} ->
            {Tree, Count, Bytes} = load(Entries),
            {ok, #st{tree = Tree, count = Count, bytes = Bytes}};
        {error, Reason} ->
            {stop, {retain_boot_failed, Reason}}
    end.

load(Entries) ->
    lists:foldl(
      fun({Topic, #broker_message{} = Msg}, {T, C, B}) when is_binary(Topic) ->
              {T#{Topic => Msg}, C + 1, B + size_of(Msg)};
         (Other, _Acc) ->
              erlang:error({corrupt_retain_entry, Other})
      end, {#{}, 0, 0}, Entries).

handle_call({put, Msg, Mode}, _From, S) ->
    case check(Msg, S) of
        {error, overflow} = Err ->
            broker_metrics:inc(retain_overflow),
            log(warning, "retain_overflow topic=~p", [Msg#broker_message.destination]),
            {reply, Err, S};
        ok ->
            S1 = insert(Msg, S),
            broker_metrics:inc(retain_updates),
            finish(Mode, S1)
    end;
handle_call({delete, Topic, Mode}, _From, S) ->
    case maps:is_key(Topic, S#st.tree) of
        false ->
            {reply, ok, S};
        true ->
            broker_metrics:inc(retain_clears),
            finish(Mode, remove(Topic, S))
    end;
handle_call({lookup, Topic}, _From, S) ->
    {reply, maps:find(Topic, S#st.tree), S};
handle_call(entries, _From, S) ->
    {reply, lists:sort(maps:to_list(S#st.tree)), S};
handle_call(stats, _From, S) ->
    {reply, #{count => S#st.count, bytes => S#st.bytes, dirty => S#st.dirty}, S};
handle_call(flush, _From, S) ->
    case persist(S) of
        ok -> {reply, ok, S#st{dirty = false, timer = cancel(S#st.timer)}};
        {error, _} = Err -> {reply, Err, S}
    end;
handle_call(_Request, _From, S) ->
    {reply, {error, unsupported}, S}.

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info(?TIMER, S) ->
    S1 = case persist(S) of
             ok -> S#st{dirty = false, timer = undefined};
             {error, _} -> S#st{timer = undefined}
         end,
    {noreply, arm_timer(S1)};
handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, S) ->
    _ = persist(S),
    ok.

code_change(_OldVsn, S, _Extra) ->
    {ok, S}.

%%%_* Internal ----------------------------------------------------------------

insert(#broker_message{destination = Topic} = Msg, #st{tree = Tree} = S) ->
    OldBytes = case maps:find(Topic, Tree) of
                   {ok, Old} -> size_of(Old);
                   error -> 0
               end,
    S#st{tree = Tree#{Topic => Msg},
         count = S#st.count + case OldBytes of 0 -> 1; _ -> 0 end,
         bytes = S#st.bytes - OldBytes + size_of(Msg),
         dirty = true}.

remove(Topic, #st{tree = Tree} = S) ->
    case maps:take(Topic, Tree) of
        {Old, Rest} ->
            S#st{tree = Rest, count = S#st.count - 1,
                 bytes = max(0, S#st.bytes - size_of(Old)), dirty = true};
        error ->
            S
    end.

size_of(#broker_message{destination = Topic, payload = Payload}) ->
    byte_size(Topic) + byte_size(Payload).

%% Caps are checked BEFORE the mutation: refusing is observable and bounded,
%% while evicting an older entry would make "the last retained value" a random
%% one (ADR-018 §18.2).
check(#broker_message{destination = Topic, payload = Payload}, S) ->
    MaxPayload = env(max_payload_bytes, 262144),
    MaxCount = env(max_retained, 10000),
    MaxBytes = env(max_retained_bytes, 67108864),
    Size = byte_size(Topic) + byte_size(Payload),
    OldBytes = case maps:find(Topic, S#st.tree) of
                   {ok, Old} -> size_of(Old);
                   error -> 0
               end,
    NewCount = S#st.count + case OldBytes of 0 -> 1; _ -> 0 end,
    ProjectedBytes = S#st.bytes - OldBytes + Size,
    if
        byte_size(Payload) > MaxPayload -> {error, overflow};
        NewCount > MaxCount -> {error, overflow};
        ProjectedBytes > MaxBytes -> {error, overflow};
        true -> ok
    end.

finish(sync, S) ->
    case persist(S) of
        ok ->
            {reply, ok, S#st{dirty = false, timer = cancel(S#st.timer)}};
        {error, Reason} ->
            broker_metrics:inc(retain_store_errors),
            log(warning, "retain store failed: ~p", [Reason]),
            %% Keep it dirty: the next timer tick retries, and an in-memory
            %% value that is not on disk is exactly what the caller's missing
            %% ack is telling the client.
            {reply, {error, {retain_store, Reason}}, S}
    end;
finish(async, S) ->
    {reply, ok, arm_timer(S)}.

persist(#st{dirty = false}) ->
    ok;
persist(#st{tree = Tree}) ->
    broker_retain_files:write(lists:sort(maps:to_list(Tree))).

arm_timer(#st{timer = Ref} = S) when Ref =/= undefined ->
    S;
arm_timer(S) ->
    case env(retain_flush_interval_ms, 100) of
        0 ->
            %% Synchronous mode for the suite: an async put is durable before it
            %% answers, which is what a test asserting the file wants.
            _ = persist(S),
            S#st{dirty = false};
        Ms when is_integer(Ms), Ms > 0 ->
            S#st{timer = erlang:send_after(Ms, self(), ?TIMER)};
        Other ->
            erlang:error({invalid_retain_flush_interval_ms, Other})
    end.

cancel(undefined) -> undefined;
cancel(Ref) ->
    _ = erlang:cancel_timer(Ref),
    undefined.

env(Key, Default) ->
    application:get_env(broker_retain, Key, Default).

log(Level, Fmt, Args) ->
    logger:log(Level, Fmt, Args).
