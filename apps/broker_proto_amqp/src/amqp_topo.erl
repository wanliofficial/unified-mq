%% @doc AMQP 0-9-1 topology (S5): exchanges, queues and bindings.
%%
%% The adapter keeps topology in ETS owned by this gen_server. v1 is
%% deliberately **transient**: exchanges/queues/bindings do not survive a
%% broker restart (documented in `docs/protocol-compatibility.md' §4.7).
%% Message bodies are still stored durably by `broker_queue' (fsync).
-module(amqp_topo).
-behaviour(gen_server).

-export([start_link/0]).
-export([declare_exchange/3, delete_exchange/1, declare_queue/3,
         delete_queue/1, bind/4, unbind/4, queues_for/2,
         exchange_type/1, queue_exists/1, queue_names/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(TAB, amqp_topo_tab).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

declare_exchange(Name, Type, Durable) ->
    gen_server:call(?MODULE, {declare_exchange, Name, Type, Durable}).
delete_exchange(Name) ->
    gen_server:call(?MODULE, {delete_exchange, Name}).
declare_queue(Name, Durable, Exclusive) ->
    gen_server:call(?MODULE, {declare_queue, Name, Durable, Exclusive}).
delete_queue(Name) ->
    gen_server:call(?MODULE, {delete_queue, Name}).
bind(Exchange, Queue, RoutingKey, Args) ->
    gen_server:call(?MODULE, {bind, Exchange, Queue, RoutingKey, Args}).
unbind(Exchange, Queue, RoutingKey, Args) ->
    gen_server:call(?MODULE, {unbind, Exchange, Queue, RoutingKey, Args}).
queues_for(Exchange, RoutingKey) ->
    gen_server:call(?MODULE, {queues_for, Exchange, RoutingKey}).
exchange_type(Name) ->
    gen_server:call(?MODULE, {exchange_type, Name}).
queue_exists(Name) ->
    gen_server:call(?MODULE, {queue_exists, Name}).
queue_names() ->
    gen_server:call(?MODULE, queue_names).

%%%_* gen_server -----------------------------------------------------------

init([]) ->
    _ = ets:new(?TAB, [named_table, public, set, {read_concurrency, true}]),
    %% The default exchange is always present and routes by queue name.
    true = ets:insert(?TAB, {{exchange, <<>>}, #{type => direct, durable => true}}),
    {ok, #{}}.

handle_call({declare_exchange, Name, Type, Durable}, _From, St) ->
    case ets:lookup(?TAB, {exchange, Name}) of
        [{_, #{type := Existing}}] when Type =:= <<>> orelse Type =:= Existing ->
            {reply, {ok, Existing}, St};
        [{_, #{type := Existing}}] ->
            {reply, {error, {type_mismatch, Existing, Type}}, St};
        [] ->
            true = ets:insert(?TAB, {{exchange, Name},
                                     #{type => normalize_type(Type), durable => Durable}}),
            {reply, ok, St}
    end;
handle_call({delete_exchange, Name}, _From, St) ->
    _ = ets:delete(?TAB, {exchange, Name}),
    _ = [ets:delete(?TAB, K) || {{binding, E, _, _}, _} = K <- ets:tab2list(?TAB), E =:= Name],
    {reply, ok, St};
handle_call({declare_queue, Name0, Durable, Exclusive}, _From, St) ->
    Name = case Name0 of
               <<>> -> generated_name();
               _ -> Name0
           end,
    case ets:lookup(?TAB, {queue, Name}) of
        [{_, #{durable := D}}] when D =/= Durable ->
            {reply, {error, {precondition_failed, durable_mismatch}}, St};
        [_] ->
            {reply, {ok, Name}, St};
        [] ->
            true = ets:insert(?TAB, {{queue, Name},
                                     #{durable => Durable, exclusive => Exclusive}}),
            {reply, {ok, Name}, St}
    end;
handle_call({delete_queue, Name}, _From, St) ->
    _ = ets:delete(?TAB, {queue, Name}),
    _ = [ets:delete(?TAB, K) || {{binding, _, Q, _}, _} = K <- ets:tab2list(?TAB), Q =:= Name],
    {reply, ok, St};
handle_call({bind, Exchange, Queue, RK, _Args}, _From, St) ->
    case {ets:member(?TAB, {queue, Queue}), ets:member(?TAB, {exchange, Exchange})} of
        {true, true} ->
            true = ets:insert(?TAB, {{binding, Exchange, Queue, RK}, true}),
            {reply, ok, St};
        {false, _} ->
            {reply, {error, no_queue}, St};
        _ ->
            {reply, {error, no_exchange}, St}
    end;
handle_call({unbind, Exchange, Queue, RK, _Args}, _From, St) ->
    _ = ets:delete(?TAB, {binding, Exchange, Queue, RK}),
    {reply, ok, St};
handle_call({queues_for, <<>>, RK}, _From, St) ->
    {reply, case ets:member(?TAB, {queue, RK}) of true -> [RK]; false -> [] end, St};
handle_call({queues_for, Exchange, RK}, _From, St) ->
    case ets:lookup(?TAB, {exchange, Exchange}) of
        [] ->
            {reply, [], St};
        [{_, #{type := Type}}] ->
            Queues = [Q || {{binding, E, Q, Key}, _} <- ets:tab2list(?TAB),
                           E =:= Exchange, key_matches(Type, Key, RK)],
            {reply, lists:usort(Queues), St}
    end;
handle_call({exchange_type, Name}, _From, St) ->
    Reply = case ets:lookup(?TAB, {exchange, Name}) of
                [{_, #{type := T}}] -> {ok, T};
                [] -> {error, no_exchange}
            end,
    {reply, Reply, St};
handle_call({queue_exists, Name}, _From, St) ->
    {reply, ets:member(?TAB, {queue, Name}), St};
handle_call(queue_names, _From, St) ->
    {reply, [N || {{queue, N}, _} <- ets:tab2list(?TAB)], St};
handle_call(_Req, _From, St) ->
    {reply, {error, unsupported}, St}.

handle_cast(_Msg, St) -> {noreply, St}.
handle_info(_Msg, St) -> {noreply, St}.
terminate(_Reason, _St) -> ok.
code_change(_OldVsn, St, _Extra) -> {ok, St}.

%%%_* Helpers --------------------------------------------------------------

normalize_type(<<>>) -> direct;
normalize_type(<<"direct">>) -> direct;
normalize_type(<<"fanout">>) -> fanout;
normalize_type(<<"topic">>) -> topic;
normalize_type(<<"headers">>) -> headers;
normalize_type(_) -> direct.

generated_name() ->
    <<"amq.gen-", (base64:encode(crypto:strong_rand_bytes(9)))/binary>>.

%% AMQP topic matching: `*' is exactly one word, `#' is zero or more words.
key_matches(direct, Binding, Routing) ->
    Binding =:= Routing;
key_matches(fanout, _Binding, _Routing) ->
    true;
key_matches(topic, Binding, Routing) ->
    topic_match(words(Binding), words(Routing));
key_matches(headers, _Binding, _Routing) ->
    false.

words(Bin) -> binary:split(Bin, <<".">>, [global]).

topic_match([], []) -> true;
topic_match([<<"#">>], _Rest) -> true;
topic_match([<<"#">> | B], R) -> topic_match(B, R) orelse (R =/= [] andalso topic_match([<<"#">> | B], tl(R)));
topic_match([<<"*">> | B], [_ | R]) -> topic_match(B, R);
topic_match([W | B], [W | R]) -> topic_match(B, R);
topic_match(_, _) -> false.
