%% @doc Pulsar subscription registry (S5).
%%
%% Pulsar subscriptions are broker-side state: a subscription has a cursor, a
%% set of attached consumers, per-consumer flow permits and per-message
%% acknowledgements. This gen_server owns that state for every
%% `{Topic, Subscription}' pair on this node.
%%
%% How delivery works here:
%%   * every topic maps to broker partition 0 with destination `pulsar/<topic>'
%%     (the same trade-off Kafka makes for topic-relative offsets);
%%   * each subscription keeps `scan' (the next broker offset to read),
%%     `index' (how many records of this topic were seen up to `scan') and
%%     `next' (the next topic-relative entry id to deliver);
%%   * a 20 ms sweep reads forward from `scan', filters by destination and
%%     hands records to consumers that still have flow permits;
%%   * an ack removes the entry from `unacked'; entries that are never acked
%%     are re-queued in `redeliver' when their consumer goes away, so a
%%     reconnecting consumer sees them again.
%%
%% Deliberate v1 limits (documented in `docs/protocol-compatibility.md' §4.9):
%%   * cursor state is in memory and reset on broker restart;
%%   * no BookKeeper/ledger storage (ADR-006) -- messages live in
%%     `broker_queue' like every other protocol's;
%%   * no Key_Shared hash ranges, no transaction markers, no compaction.
-module(pulsar_subscriptions).

-behaviour(gen_server).

-export([start_link/0,
         subscribe/7, unsubscribe/3, flow/4, ack/5, close_consumer/3,
         seek/4, close/0, stats/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-include_lib("broker_core/include/broker_message.hrl").

-define(SWEEP_MS, 20).
-define(PARTITION, 0).
-define(EMPTY_PAGE, 32).

-record(st, {subs = #{} :: map()}).

%%%_* API ----------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Attach `Pid' as consumer `ConsumerId' of `{Topic, Sub}'. InitialPosition
%% is `earliest' or `latest' and only applies to a subscription's first
%% consumer.
subscribe(Topic, Sub, Type, ConsumerId, Pid, Durable, InitialPosition) ->
    gen_server:call(?MODULE, {subscribe, Topic, Sub, Type, ConsumerId, Pid,
                              Durable, InitialPosition}, 10000).

unsubscribe(Topic, Sub, ConsumerId) ->
    gen_server:call(?MODULE, {unsubscribe, Topic, Sub, ConsumerId}, 10000).

flow(Topic, Sub, ConsumerId, Permits) ->
    gen_server:call(?MODULE, {flow, Topic, Sub, ConsumerId, Permits}, 10000).

ack(Topic, Sub, ConsumerId, AckType, Offsets) ->
    gen_server:call(?MODULE, {ack, Topic, Sub, ConsumerId, AckType, Offsets}, 10000).

close_consumer(Topic, Sub, ConsumerId) ->
    gen_server:call(?MODULE, {close_consumer, Topic, Sub, ConsumerId}, 10000).

seek(Topic, Sub, ConsumerId, EntryId) ->
    gen_server:call(?MODULE, {seek, Topic, Sub, ConsumerId, EntryId}, 10000).

close() ->
    gen_server:stop(?MODULE).

%% @doc Test/ops view of the registry.
stats() ->
    gen_server:call(?MODULE, stats, 10000).

%%%_* gen_server ---------------------------------------------------------

init([]) ->
    erlang:send_after(?SWEEP_MS, self(), sweep),
    {ok, #st{}}.

handle_call({subscribe, Topic, Sub, Type, ConsumerId, Pid, Durable, InitialPos},
            _From, #st{subs = Subs} = St) ->
    Key = {Topic, Sub},
    case maps:find(Key, Subs) of
        {ok, S} ->
            Consumers = maps:get(consumers, S),
            case Type =:= exclusive andalso map_size(Consumers) > 0 of
                true ->
                    {reply, {error, 5}, St};        % ConsumerBusy
                false ->
                    {reply, ok, St#st{subs = (Subs#{Key := add_consumer(S, ConsumerId, Pid)})}}
            end;
        error ->
            S0 = new_sub(Topic, Type, Durable, InitialPos),
            S1 = add_consumer(S0, ConsumerId, Pid),
            {reply, ok, St#st{subs = (Subs#{Key => S1})}}
    end;
handle_call({unsubscribe, Topic, Sub, ConsumerId}, _From, St) ->
    {reply, ok, do_close_consumer(Topic, Sub, ConsumerId, St)};
handle_call({close_consumer, Topic, Sub, ConsumerId}, _From, St) ->
    {reply, ok, do_close_consumer(Topic, Sub, ConsumerId, St)};
handle_call({flow, Key0, Sub0, ConsumerId, Permits}, _From, St) ->
    {reply, ok, pump(Key0, Sub0, ConsumerId, Permits, St)};
handle_call({ack, Topic, Sub, ConsumerId, AckType, Offsets}, _From, St) ->
    Key = {Topic, Sub},
    case maps:find(Key, St#st.subs) of
        error ->
            {reply, {error, 12}, St};               % SubscriptionNotFound
        {ok, S} ->
            case maps:is_key(ConsumerId, maps:get(consumers, S)) of
                false ->
                    {reply, {error, 13}, St};       % ConsumerNotFound
                true ->
                    S1 = do_ack(S, ConsumerId, AckType, Offsets),
                    {reply, ok, St#st{subs = ((St#st.subs)#{Key := S1})}}
            end
    end;
handle_call({seek, Topic, Sub, _ConsumerId, EntryId}, _From, St) ->
    Key = {Topic, Sub},
    case maps:find(Key, St#st.subs) of
        error ->
            {reply, {error, 12}, St};
        {ok, S} ->
            S1 = S#{scan := 0, index := 0, next := EntryId,
                    unacked := #{}, redeliver := []},
            {reply, ok, St#st{subs = ((St#st.subs)#{Key := S1})}}
    end;
handle_call(stats, _From, #st{subs = Subs} = St) ->
    Reply = [{Topic, Sub,
              map_size(maps:get(consumers, S)),
              maps:get(next, S),
              maps:get(scan, S),
              map_size(maps:get(unacked, S)),
              length(maps:get(redeliver, S))}
             || {{Topic, Sub}, S} <- maps:to_list(Subs)],
    {reply, Reply, St};
handle_call(_Req, _From, St) ->
    {reply, {error, unsupported}, St}.

handle_cast(_Msg, St) ->
    {noreply, St}.

handle_info(sweep, #st{subs = Subs} = St) ->
    Subs1 = maps:map(fun(_Key, S) -> sweep_sub(S) end, Subs),
    erlang:send_after(?SWEEP_MS, self(), sweep),
    {noreply, St#st{subs = Subs1}};
handle_info({'DOWN', _Ref, process, Pid, _Reason},
            #st{subs = Subs} = St) ->
    Subs1 = maps:map(fun(_Key, S) -> drop_pid(S, Pid) end, Subs),
    {noreply, St#st{subs = Subs1}};
handle_info(_Other, St) ->
    {noreply, St}.

terminate(_Reason, _St) ->
    ok.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%%%_* Subscription state -------------------------------------------------

new_sub(Topic, Type, Durable, InitialPosition) ->
    Destination = destination(Topic),
    {Scan, Index} =
        case InitialPosition of
            earliest ->
                {0, 0};
            latest ->
                Hedge = high_watermark(),
                {Hedge, count_topic(Destination, Hedge)}
        end,
    #{destination => Destination,
      type => Type,
      durable => Durable,
      consumers => #{},
      unacked => #{},
      redeliver => [],
      scan => Scan,
      index => Index,
      next => Index}.

add_consumer(S, ConsumerId, Pid) ->
    Ref = erlang:monitor(process, Pid),
    Consumers = maps:get(consumers, S),
    S#{consumers := Consumers#{ConsumerId => #{pid => Pid,
                                               ref => Ref,
                                               permits => 0}}}.

drop_pid(S, Pid) ->
    case [Id || {Id, #{pid := P}} <- maps:to_list(maps:get(consumers, S)), P =:= Pid] of
        [] -> S;
        [Id | _] -> close_consumer_state(S, Id)
    end.

%%%_* Delivery ------------------------------------------------------------

sweep_sub(S) ->
    case map_size(maps:get(consumers, S)) of
        0 -> S;
        _ -> pump_sub(S)
    end.

pump_sub(S) ->
    S1 = deliver_redelivery(S),
    case next_consumer(S1) of
        none -> S1;
        {ConsumerId, _Info} ->
            case take_record(S1) of
                {ok, EntryId, Msg, S2} ->
                    pump_sub(send_to(ConsumerId, EntryId, Msg, S2));
                empty ->
                    S1
            end
    end.

deliver_redelivery(S) ->
    case maps:get(redeliver, S) of
        [] ->
            S;
        [{EntryId, Msg} | Rest] ->
            case next_consumer(S) of
                none ->
                    S;
                {ConsumerId, _Info} ->
                    S1 = send_to(ConsumerId, EntryId, Msg, S#{redeliver := Rest}),
                    deliver_redelivery(S1)
            end
    end.

next_consumer(S) ->
    Consumers = maps:get(consumers, S),
    Candidates = lists:sort([{Id, C} || {Id, C} <- maps:to_list(Consumers),
                                        maps:get(permits, C) > 0]),
    case Candidates of
        [{Id, C} | _] -> {Id, C};
        [] -> none
    end.

send_to(ConsumerId, EntryId, Msg, S) ->
    Consumers = maps:get(consumers, S),
    #{ConsumerId := Info} = Consumers,
    Pid = maps:get(pid, Info),
    Pid ! {pulsar_deliver, ConsumerId, EntryId, Msg},
    Consumers1 = Consumers#{ConsumerId := Info#{permits := maps:get(permits, Info) - 1}},
    Unacked = maps:put(EntryId, {ConsumerId, Msg}, maps:get(unacked, S)),
    Next = maps:get(next, S),
    S#{consumers := Consumers1, unacked := Unacked,
       next := erlang:max(Next, EntryId + 1)}.

%% Read one record for this topic, updating `scan'/`index' as it goes.
take_record(#{scan := Scan, destination := Destination} = S) ->
    case high_watermark() of
        High when Scan >= High ->
            empty;
        _ ->
            case (catch broker_queue:read_from(?PARTITION, Scan, ?EMPTY_PAGE)) of
                {ok, [], _Next} ->
                    empty;
                {ok, Page, _Next} ->
                    walk(Page, Destination, S);
                {error, _} ->
                    empty
            end
    end.

walk([], _Destination, _S) ->
    empty;
walk([{Abs, Msg} | Rest], Destination, S) ->
    S1 = S#{scan := Abs + 1},
    case Msg#broker_message.destination =:= Destination of
        false ->
            walk(Rest, Destination, S1);
        true ->
            Index = maps:get(index, S1),
            S2 = S1#{index := Index + 1},
            case Index >= maps:get(next, S2) of
                true -> {ok, Index, Msg, S2};
                false -> walk(Rest, Destination, S2)
            end
    end.

pump(Topic, Sub, ConsumerId, Permits, St) ->
    Key = {Topic, Sub},
    case maps:find(Key, St#st.subs) of
        error ->
            St;
        {ok, S} ->
            Consumers = maps:get(consumers, S),
            case maps:find(ConsumerId, Consumers) of
                error ->
                    St;
                {ok, Info} ->
                    Info1 = Info#{permits := maps:get(permits, Info) + Permits},
                    S1 = S#{consumers := Consumers#{ConsumerId := Info1}},
                    S2 = pump_sub(S1),
                    St#st{subs = ((St#st.subs)#{Key := S2})}
            end
    end.

%%%_* Ack / close ---------------------------------------------------------

do_ack(S, ConsumerId, individual, Offsets) ->
    Unacked = maps:get(unacked, S),
    Unacked1 = lists:foldl(fun(EntryId, Acc) ->
                                   case maps:find(EntryId, Acc) of
                                       {ok, {ConsumerId, _}} -> maps:remove(EntryId, Acc);
                                       _ -> Acc
                                   end
                           end, Unacked, Offsets),
    S#{unacked := Unacked1};
do_ack(S, ConsumerId, cumulative, Offsets) ->
    case Offsets of
        [] ->
            S;
        _ ->
            Max = lists:max(Offsets),
            Unacked = maps:get(unacked, S),
            Unacked1 = maps:filter(fun(EntryId, {Owner, _Msg}) ->
                                           not (Owner =:= ConsumerId andalso EntryId =< Max)
                                   end, Unacked),
            S#{unacked := Unacked1}
    end.

do_close_consumer(Topic, Sub, ConsumerId, St) ->
    Key = {Topic, Sub},
    case maps:find(Key, St#st.subs) of
        error ->
            St;
        {ok, S} ->
            S1 = close_consumer_state(S, ConsumerId),
            Subs = St#st.subs,
            case map_size(maps:get(consumers, S1)) =:= 0 andalso
                 maps:get(durable, S1) =:= false of
                true -> St#st{subs = (maps:remove(Key, Subs))};
                false -> St#st{subs = (Subs#{Key := S1})}
            end
    end.

close_consumer_state(S, ConsumerId) ->
    Consumers = maps:get(consumers, S),
    case maps:take(ConsumerId, Consumers) of
        error ->
            S;
        {Info, Consumers1} ->
            _ = erlang:demonitor(maps:get(ref, Info), [flush]),
            Pending = lists:sort([{EntryId, Msg}
                                  || {EntryId, {Owner, Msg}} <- maps:to_list(maps:get(unacked, S)),
                                     Owner =:= ConsumerId]),
            Unacked = maps:filter(fun(_EntryId, {Owner, _Msg}) -> Owner =/= ConsumerId end,
                                  maps:get(unacked, S)),
            Redeliver = Pending ++ maps:get(redeliver, S),
            S#{consumers := Consumers1,
               unacked := Unacked,
               redeliver := Redeliver}
    end.

%%%_* Queue helpers -------------------------------------------------------

high_watermark() ->
    case broker_queue:watermarks(?PARTITION) of
        {ok, _Committed, High} -> High;
        {error, _} -> 0
    end.

count_topic(Destination, High) ->
    count_page(0, High, Destination, 0).

count_page(From, High, _Destination, Acc) when From >= High ->
    Acc;
count_page(From, High, Destination, Acc) ->
    case (catch broker_queue:read_from(?PARTITION, From, 128)) of
        {ok, [], _Next} ->
            Acc;
        {ok, Page, Next} ->
            Acc1 = Acc + length([ok || {_Abs, #broker_message{destination = D}} <- Page,
                                        D =:= Destination]),
            From1 = case Next > From of true -> Next; false -> From + length(Page) end,
            count_page(From1, High, Destination, Acc1);
        {error, _} ->
            Acc
    end.

destination(Topic) ->
    <<"pulsar/", Topic/binary>>.
