%% @doc JetStream MVP (NATS-JS-001..003 minimal slice, 2026-11-17).
%%
%% Implemented, and honest about it:
%%
%%   * Stream management: `$JS.API.STREAM.CREATE.<name>' (StreamConfig JSON
%%     body, `subjects' is the only field honoured), `STREAM.INFO.<name>',
%%     `STREAM.DELETE.<name>', `STREAM.NAMES'. Stream data maps to the
%%     destination `js/<stream>'; the original NATS subject rides `proto_meta'.
%%   * Managed publish: a publish whose subject matches a stream is appended
%%     there and answered with a PubAck `{"stream":"<name>","seq":<n>}' on the
%%     request's reply inbox. Seq numbers are per-stream counters assigned
%%     here and persisted with the stream metadata.
%%   * Durable PULL consumers: `CONSUMER.CREATE.<stream>.<name>' (deliver
%%     policy `all'|`new', ack policy `explicit'|`none', `filter_subject',
%%     `ack_wait'), `CONSUMER.INFO/DELETE', and pull via
%%     `CONSUMER.MSG.NEXT.<stream>.<consumer>' (batch, expires, no_wait).
%%     Deliveries carry a v1
%%     `$JS.ACK.<stream>.<consumer>.<d>.<sseq>.<cseq>.<ts>.<pending>' reply;
%%     the client answers `+ACK' / `-NAK' / `+TERM' / `+WPI' on it.
%%     Deliveries come from a shared group (`broker_subscription' claim), so
%%     the ack floor / pending set / redelivery machinery is the broker's own
%%     cursor algebra.
%%
%% NOT implemented (see docs/protocol-compatibility.md §4.6): push consumers,
%% message dedup (`Nats-Msg-Id'), `STREAM.MSG.GET'/`DIRECT', deliver policies
%% beyond all/new, mirrors/sources, KV/Object Store, per-stream partitions.
%% Stream/consumer metadata persistence is a synchronous write per mutation
%% (and per publish for the seq counter): a correctness-first MVP boundary,
%% not a throughput claim.
%%
%% Wire notes (verified against nats.go v1.48.0): API replies need no `type'
%% field; errors must be `{"error":{"code":...,"err_code":...,"description":...}}';
%% the only PubAck validation client-side is JSON parseability and a non-empty
%% `stream'.
-module(nats_jetstream).

-behaviour(gen_server).

-export([start_link/0, managed/1, managed_publish/5]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-include_lib("broker_core/include/broker_message.hrl").
-include_lib("broker_router/include/broker_router.hrl").

-define(API_FILTER, <<"$JS/API/#">>).
-define(ACK_FILTER, <<"$JS/ACK/#">>).
-define(JS_CLIENT, <<"nats-js">>).
-define(API_PREFIX, <<"$JS/API/">>).
-define(ACK_PREFIX, <<"$JS/ACK/">>).

-record(st, {
    dir = <<"data/jetstream">> :: binary() | string(),
    %% Name => #{name, subjects, partition, last_seq, created}
    streams = #{} :: map(),
    %% CKey => #{stream, name, deliver_policy, ack_policy, ack_wait_ms,
    %%            max_deliver, filter_subject, ref, group, vclient, cseq,
    %%            created, refs => #{sseq => {P, O}}}
    consumers = #{} :: map(),
    fetches = #{} :: map()
}).

%%%_* API ---------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Does this NATS subject belong to a managed stream? Called on every
%% publish from `nats_conn'; a match redirects the write into the stream.
-spec managed(binary()) -> {ok, binary()} | nomatch.
managed(Subject) ->
    try
        gen_server:call(?MODULE, {managed, Subject}, 5000)
    catch
        _:_ -> nomatch
    end.

%% @doc Append a managed publish and answer the PubAck on the reply inbox.
-spec managed_publish(binary(), binary(), binary() | undefined, binary(),
                      binary()) -> ok | {error, term()}.
managed_publish(Stream, Subject, Reply, Payload, Tenant) ->
    gen_server:call(?MODULE, {js_publish, Stream, Subject, Reply, Payload,
                              Tenant}, 30000).

%%%_* gen_server --------------------------------------------------------------

init([]) ->
    process_flag(trap_exit, true),
    Dir = to_binary(application:get_env(broker_proto_nats, js_data_dir,
                                        <<"data/jetstream">>)),
    ok = filelib:ensure_dir(filename:join([Dir, "x"])),
    Streams = load_meta(filename:join([Dir, <<"streams.term">>])),
    Consumers = load_meta(filename:join([Dir, <<"consumers.term">>])),
    %% Re-join every consumer group (idempotent): the entity and its cursor
    %% live in broker_subscription's own files; this keeps membership honest
    %% across partial state loss.
    lists:foreach(fun rejoin_consumer/1, maps:values(Consumers)),
    _ = try broker_subscription:attach(?JS_CLIENT, self(), false) catch _:_ -> ok end,
    _ = try broker_subscription:subscribe(?JS_CLIENT, ?API_FILTER, 1) catch _:_ -> ok end,
    _ = try broker_subscription:subscribe(?JS_CLIENT, ?ACK_FILTER, 1) catch _:_ -> ok end,
    {ok, #st{dir = Dir, streams = Streams, consumers = Consumers}}.

handle_call({managed, Subject}, _From, #st{streams = Streams} = St) ->
    {reply, match_subject(Subject, Streams), St};
handle_call({js_publish, Stream, Subject, Reply, Payload, Tenant}, _From, St) ->
    case js_publish(Stream, Subject, Reply, Payload, Tenant, St) of
        {ok, St1} -> {reply, ok, St1};
        {error, Reason} -> {reply, {error, Reason}, St}
    end;
handle_call(_Req, _From, St) ->
    {reply, {error, unsupported}, St}.

handle_cast(_Msg, St) ->
    {noreply, St}.

%% Deliveries for the two internal subscriptions (both exclusive, both owned
%% by ?JS_CLIENT). Each processed message is self-acked: the exclusive cursor
%% must advance or it pins the partition committed watermark forever.
handle_info({broker_deliver, ?JS_CLIENT, _MaxQoS, Refs, {P, O, Msg}}, St) ->
    St1 = handle_internal(Msg, P, O, Refs, St),
    lists:foreach(fun(Ref) ->
                          _ = try
                              broker_subscription:ack(Ref, P, O,
                              ?JS_CLIENT)
                          catch
                              _:_ -> ok
                          end
                  end, Refs),
    {noreply, St1};
handle_info({broker_deliver, _Other, _MaxQoS, _Refs, _Delivery}, St) ->
    {noreply, St};
handle_info({timeout, _Ref, {js_fetch_deadline, Key}}, #st{fetches = F} = St) ->
    case maps:is_key(Key, F) of
        true -> {noreply, St#st{fetches = maps:remove(Key, F)}};
        false -> {noreply, St}
    end;
handle_info(_Other, St) ->
    {noreply, St}.

terminate(_Reason, _St) ->
    _ = try broker_subscription:unsubscribe(?JS_CLIENT, ?API_FILTER) catch _:_ -> ok end,
    _ = try broker_subscription:unsubscribe(?JS_CLIENT, ?ACK_FILTER) catch _:_ -> ok end,
    _ = try broker_subscription:detach(?JS_CLIENT, self()) catch _:_ -> ok end,
    ok.

code_change(_OldVsn, St, _Extra) -> {ok, St}.

%%%_* Internal deliveries -----------------------------------------------------

handle_internal(Msg, _P, _O, _Refs, St0) ->
    Destination = Msg#broker_message.destination,
    Reply = js_reply_of(Msg),
    case Destination of
        <<"$JS/ACK/", _/binary>> ->
            %% Slash form straight from the log: ack_tokens splits on "/".
            handle_ack(Destination, Msg#broker_message.payload, St0);
        <<"$JS/API/", _/binary>> ->
            handle_api(Destination, Msg#broker_message.payload, Reply, St0);
        _ ->
            St0
    end.

%%%_* Managed publish ---------------------------------------------------------

js_publish(Stream, Subject, Reply, Payload, Tenant, St) ->
    case maps:find(Stream, St#st.streams) of
        {ok, SMeta} ->
            Seq = maps:get(last_seq, SMeta) + 1,
            Destination = js_destination(Stream),
            Msg = #broker_message{
                     message_id = js_message_id(),
                     tenant_id = Tenant,
                     destination = Destination,
                     partition_id = 0,
                     payload = Payload,
                     payload_encoding = raw,
                     headers = #{},
                     proto_meta = #{<<"nats">> =>
                                        #{<<"publisher">> => ?JS_CLIENT,
                                          <<"subject">> => Subject,
                                          <<"stream_seq">> => Seq}},
                     persistence = durable,
                     reliability = fsync,
                     created_at = erlang:system_time(millisecond),
                     expires_at = undefined,
                     event = undefined},
            case js_append(Destination, Msg, Tenant) of
                {ok, _} ->
                    SMeta1 = SMeta#{last_seq => Seq},
                    St1 = persist_streams(
                            St#st{streams = maps:put(Stream, SMeta1,
                                                     St#st.streams)}),
                    St2 = serve_pending_fetches(Stream, St1),
                    case Reply of
                        R when is_binary(R), R =/= <<>> ->
                            AckJson = json:encode(#{<<"stream">> => Stream,
                                                    <<"seq">> => Seq}),
                            deliver_reply(R, AckJson, St2),
                            {ok, St2};
                        _ ->
                            {ok, St2}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        error ->
            {error, {no_such_stream, Stream}}
    end.

%% Append through the ordinary router path so the destination's partition and
%% its shared groups' wake-ups behave exactly like every other write.
js_append(Destination, Msg, Tenant) ->
    case broker_router:env() of
        {ok, Env} ->
            Backend = application:get_env(broker_router, backend,
                                          broker_router_hash),
            Ctx = #route_ctx{tenant_id = Tenant,
                             publisher = ?JS_CLIENT,
                             reliability = fsync},
            case Backend:route(Destination, Ctx, Env) of
                {ok, Routes} ->
                    store_routes(Routes, Msg, 0);
                {error, Reason} -> {error, {route, Reason}}
            end;
        {error, Reason} ->
            {error, {router_env, Reason}}
    end.

store_routes([], _Msg, Stored) when Stored > 0 -> {ok, Stored};
store_routes([], _Msg, 0) -> {error, nothing_stored};
store_routes([{P, Refs} | Rest], Msg, Stored) ->
    PartitionMsg = Msg#broker_message{partition_id = P},
    try broker_queue:append(P, PartitionMsg, fsync) of
        {ok, Offset} ->
            _ = try
                broker_subscription:dispatch(Refs,
                {P, Offset, PartitionMsg})
            catch
                _:_ -> ok
            end,
            store_routes(Rest, Msg, Stored + 1);
        _ ->
            store_routes(Rest, Msg, Stored)
    catch
        _:_ -> store_routes(Rest, Msg, Stored)
    end.

%% Reply to a request inbox. The reply subject is NATS-form (`_INBOX.x.y');
%% the broker destination is the `/` form. Inbox subscriptions are exclusive
%% and self-ack, so there is no cursor concern.
deliver_reply(ReplySubject, Payload0, St) ->
    Destination = subject_to_destination(ReplySubject),
    %% `json:encode/1' returns iodata; the log stores flat binaries.
    Payload = iolist_to_binary(Payload0),
    Msg = #broker_message{
             message_id = js_message_id(),
             tenant_id = <<"*">>,
             destination = Destination,
             partition_id = 0,
             payload = Payload,
             payload_encoding = raw,
             headers = #{},
             proto_meta = #{<<"nats">> => #{<<"publisher">> => ?JS_CLIENT}},
             persistence = durable,
             reliability = fsync,
             created_at = erlang:system_time(millisecond),
             expires_at = undefined,
             event = undefined},
    {ok, _} = js_append(Destination, Msg, <<"*">>),
    St.

%%%_* API requests ------------------------------------------------------------

handle_api(Destination, Payload, Reply, St) ->
    Tokens = binary:split(Destination, <<"/">>, [global]),
    case Tokens of
        [<<"$JS">>, <<"API">>, <<"INFO">>] ->
            reply_json(Reply, info_json(St), St), St;
        [<<"$JS">>, <<"API">>, <<"STREAM">>, <<"NAMES">>] ->
            %% nats.go locates a stream by subject through this API: filter the
            %% names by the request body's `subject' pattern.
            Wanted =
                try json:decode(Payload) of
                    #{<<"subject">> := Pat} when is_binary(Pat) ->
                        [Name || {Name, Meta} <- maps:to_list(St#st.streams),
                                 lists:any(fun(Subj) ->
                                               subject_matches(Subj, Pat)
                                           end,
                                           maps:get(subjects, Meta, []))];
                    _ -> maps:keys(St#st.streams)
                catch
                    _:_ -> maps:keys(St#st.streams)
                end,
            reply_json(Reply, json:encode(#{<<"streams">> => Wanted}), St),
            St;
        [<<"$JS">>, <<"API">>, <<"STREAM">>, <<"CREATE">>, Name] ->
            api_stream_create(Name, Payload, Reply, St);
        [<<"$JS">>, <<"API">>, <<"STREAM">>, <<"INFO">>, Name] ->
            case stream_info_json(Name, St) of
                {ok, Json} -> reply_json(Reply, Json, St), St;
                {error, not_found} -> reply_stream_not_found(Reply, St), St
            end;
        [<<"$JS">>, <<"API">>, <<"STREAM">>, <<"DELETE">>, Name] ->
            api_stream_delete(Name, Reply, St);
        [<<"$JS">>, <<"API">>, <<"CONSUMER">>, <<"CREATE">>, Stream, CName] ->
            api_consumer_create(Stream, CName, Payload, Reply, St);
        %% nats.go v1.48's PullSubscribe creates durable consumers through the
        %% legacy DURABLE.CREATE subject (body carries stream_name + config).
        [<<"$JS">>, <<"API">>, <<"CONSUMER">>, <<"DURABLE">>, <<"CREATE">>,
         Stream, CName] ->
            api_consumer_create(Stream, CName, Payload, Reply, St);
        [<<"$JS">>, <<"API">>, <<"CONSUMER">>, <<"INFO">>, Stream, CName] ->
            case consumer_info_json(Stream, CName, St) of
                {ok, Json} -> reply_json(Reply, Json, St), St;
                {error, not_found} -> reply_consumer_not_found(Reply, St), St
            end;
        [<<"$JS">>, <<"API">>, <<"CONSUMER">>, <<"DELETE">>, Stream, CName] ->
            api_consumer_delete(Stream, CName, Reply, St);
        [<<"$JS">>, <<"API">>, <<"CONSUMER">>, <<"MSG">>, <<"NEXT">>,
         Stream, CName] ->
            api_fetch(Stream, CName, Payload, Reply, St);
        _ ->
            logger:debug("unimplemented JetStream API: ~ts", [Destination]),
            reply_error(Reply, 503, 503, <<"unimplemented JetStream API">>, St),
            St
    end.

api_stream_create(Name, Payload, Reply, St) ->
    case parse_stream_config(Payload) of
        {ok, Subjects} ->
            case maps:find(Name, St#st.streams) of
                {ok, _Existing} ->
                    %% Idempotent same-name create is OK; config diffing is
                    %% out of MVP scope.
                    {ok, Json} = stream_info_json(Name, St),
                    reply_json(Reply, Json, St),
                    St;
                error ->
                    %% Discover the partition the stream's destination hashes
                    %% to, so fetches claim from the same partition writes
                    %% land in.
                    Partition = discover_partition(js_destination(Name),
                                                   to_binary(maps:get(
                                                        <<"tenant_id">>,
                                                        #{}, <<"*">>))),
                    Meta = #{name => Name,
                             subjects => Subjects,
                             partition => Partition,
                             last_seq => 0,
                             created => erlang:system_time(millisecond)},
                    St1 = persist_streams(
                            St#st{streams = maps:put(Name, Meta,
                                                     St#st.streams)}),
                    {ok, Json} = stream_info_json(Name, St1),
                    reply_json(Reply, Json, St1),
                    St1
            end;
        {error, Reason} ->
            reply_error(Reply, 400, 400, Reason, St),
            St
    end.

api_stream_delete(Name, Reply, St) ->
    case maps:is_key(Name, St#st.streams) of
        false ->
            reply_error(Reply, 404, 10059, <<"stream not found">>, St),
            St;
        true ->
            %% Drop the stream's consumers with it (leave the group AND the
            %% metadata, so CONSUMER.INFO answers 404 afterwards).
            Drops = [C || {_, C} <- maps:to_list(St#st.consumers),
                          maps:get(stream, C) =:= Name],
            St1 = lists:foldl(fun(C, Acc) -> consumer_teardown(C, Acc) end,
                              St, Drops),
            St2 = persist_streams(
                    St1#st{streams = maps:remove(Name, St1#st.streams),
                           consumers =
                               maps:filter(fun(_K, C) ->
                                                   maps:get(stream, C)
                                                       =/= Name
                                           end, St1#st.consumers)}),
            reply_json(Reply, json:encode(#{<<"success">> => true}), St2),
            St2
    end.

api_consumer_create(Stream, CName, Payload, Reply, St) ->
    CKey = consumer_key(Stream, CName),
    case maps:is_key(Stream, St#st.streams) of
        false ->
            reply_error(Reply, 404, 10059, <<"stream not found">>, St),
            St;
        true ->
            case maps:find(CKey, St#st.consumers) of
                {ok, _Existing} ->
                    {ok, Json} = consumer_info_json(Stream, CName, St),
                    reply_json(Reply, Json, St),
                    St;
                error ->
                    case parse_consumer_config(Payload) of
                        {ok, Cfg} ->
                            Consumer = Cfg#{stream => Stream, name => CName,
                                            created =>
                                                erlang:system_time(millisecond),
                                            cseq => 0, refs => #{}},
                            {ok, St1} = consumer_setup(Consumer, St),
                            St2 = persist_consumers(St1),
                            {ok, Json} = consumer_info_json(Stream, CName, St2),
                            reply_json(Reply, Json, St2),
                            St2;
                        {error, Reason} ->
                            reply_error(Reply, 400, 400, Reason, St),
                            St
                    end
            end
    end.

api_consumer_delete(Stream, CName, Reply, St) ->
    case maps:take(consumer_key(Stream, CName), St#st.consumers) of
        {Consumer, St1} ->
            St2 = persist_consumers(consumer_teardown(Consumer, St1)),
            reply_json(Reply, json:encode(#{<<"success">> => true}), St2),
            St2;
        error ->
            reply_consumer_not_found(Reply, St),
            St
    end.

%% Pull request. Serve what is ready now; if nothing matched and the client
%% waits (`no_wait' false), park the request and serve it on the next managed
%% publish for this stream until the deadline drops it.
api_fetch(Stream, CName, Payload, Reply, St0) ->
    case maps:find(consumer_key(Stream, CName), St0#st.consumers) of
        {ok, Consumer} ->
            {Batch, NoWait, ExpiresMs} = parse_fetch(Payload),
            case fetch_serve(Stream, CName, Consumer, Reply, Batch, St0) of
                {done, St1} ->
                    St1;
                {empty, St1} when NoWait ->
                    St1;
                {empty, St1} ->
                    Key = erlang:unique_integer([positive]),
                    Timer = erlang:start_timer(max(1, ExpiresMs), self(),
                                               {js_fetch_deadline, Key}),
                    St1#st{fetches =
                               maps:put(Key,
                                        #{stream => Stream,
                                          consumer => CName,
                                          reply => Reply,
                                          batch => Batch,
                                          timer => Timer},
                                        St1#st.fetches)}
            end;
        error ->
            reply_error(Reply, 404, 10014, <<"consumer not found">>, St0),
            St0
    end.

%% Serve up to Batch matching messages. `{done, St}' = at least one delivered;
%% `{empty, St}' = nothing matched.
fetch_serve(Stream, CName, Consumer, Reply, Batch, St0) ->
    fetch_serve(Stream, CName, Consumer, Reply, Batch, St0, 0).

fetch_serve(_Stream, _CName, _Consumer, _Reply, _Batch, St, 256) ->
    {empty, St};
fetch_serve(_Stream, _CName, _Consumer, _Reply, 0, St, _Rounds) ->
    {done, St};
fetch_serve(Stream, CName, Consumer, Reply, Remaining, St0, Rounds) ->
    Ref = maps:get(ref, Consumer),
    VClient = maps:get(vclient, Consumer),
    Partition = stream_partition_for(Stream, St0),
    Match = consumer_filter(Consumer),
    try broker_subscription:claim(Ref, Partition, Remaining, VClient) of
        {ok, Offsets} when Offsets =/= [] ->
            {Read, Missing} = read_offsets(Partition, Offsets),
            _ = case Missing of
                    [] -> ok;
                    _ -> try broker_subscription:release(Ref, Partition,
                                                         Missing, VClient)
                        catch _:_ -> ok
                        end
                end,
            {Delivered, Remaining1, St1} =
                lists:foldl(
                  fun({O, Msg}, {N, Rem, Acc}) ->
                          case js_msg_matches(Msg, Match) of
                              true ->
                                  %% fetch_deliver registers refs[seq] -> offset
                                  %% for the ack/NAK handshake; its state MUST
                                  %% flow on or the next ack finds refs empty.
                                  Acc1 = fetch_deliver(Reply, Stream, CName,
                                                       Partition, O, Msg, Acc),
                                  {N + 1, Rem, Acc1};
                              false ->
                                  %% Foreign/filtered: consume the offset
                                  %% without delivering (AMQP skip pattern).
                                  %% The batch counts DELIVERED messages only.
                                  _ = try
                                      broker_subscription:ack(
                                      Ref, Partition, O, VClient)
                                  catch
                                      _:_ -> ok
                                  end,
                                  {N, Rem, Acc}
                          end
                  end, {0, Remaining, St0}, Read),
            Remaining2 = max(0, Remaining1),
            case {Delivered, Remaining2} of
                {_, 0} -> {done, St1};
                {0, _} -> fetch_serve(Stream, CName, Consumer, Reply,
                                      Remaining2, St1, Rounds + 1);
                {_, _} -> {done, St1}
            end;
        _ ->
            {empty, St0}
    catch
        _:_ -> {empty, St0}
    end.

fetch_deliver(Reply, Stream, CName, Partition, O, Msg, St0) ->
    CKey = consumer_key(Stream, CName),
    Consumer = maps:get(CKey, St0#st.consumers),
    Seq = stream_seq_of(Msg),
    CSeq = maps:get(cseq, Consumer) + 1,
    AckSubj = js_ack_subject(Stream, CName, Seq, CSeq),
    Consumer1 = Consumer#{cseq => CSeq,
                         refs => maps:put(Seq, {Partition, O},
                                          maps:get(refs, Consumer, #{}))},
    St1 = persist_consumers(St0#st{consumers =
                                       maps:put(CKey, Consumer1,
                                                St0#st.consumers)}),
    InboxDest = subject_to_destination(Reply),
    Msg2 = #broker_message{
              message_id = js_message_id(),
              tenant_id = <<"*">>,
              destination = InboxDest,
              partition_id = 0,
              payload = Msg#broker_message.payload,
              payload_encoding = raw,
              headers = Msg#broker_message.headers,
              proto_meta = #{<<"nats">> =>
                                 #{<<"publisher">> => ?JS_CLIENT,
                                   <<"subject">> => js_original_subject(Msg),
                                   <<"reply">> => AckSubj}},
              persistence = durable,
              reliability = fsync,
              created_at = erlang:system_time(millisecond),
              expires_at = undefined,
              event = undefined},
    {ok, _} = js_append(InboxDest, Msg2, <<"*">>),
    St1.
handle_ack(Destination, Payload, St0) ->
    case ack_tokens(Destination) of
        {ok, Stream, CName, Seq} ->
            case maps:find(consumer_key(Stream, CName), St0#st.consumers) of
                {ok, Consumer} ->
                    Ref = maps:get(ref, Consumer),
                    VClient = maps:get(vclient, Consumer),
                    case maps:find(Seq, maps:get(refs, Consumer, #{})) of
                        {ok, {P, O}} ->
                            ack_apply(ack_action(Payload), Ref, P, O, VClient,
                                      Stream, CName, Seq, Consumer, St0);
                        error ->
                            logger:debug("JS ack for unknown delivery seq ~p (~ts/~ts)",
                                         [Seq, Stream, CName]),
                            St0
                    end;
                error ->
                    logger:debug("JS ack for unknown consumer ~ts/~ts",
                                 [Stream, CName]),
                    St0
            end;
        error ->
            logger:debug("JS ack with malformed subject: ~ts", [Destination]),
            St0
    end.

%% All branches return the bare state: handle_ack runs inside handle_internal,
%% whose result is stored directly as the gen_server state (a {noreply, _}
%% tuple here would corrupt it and silently degrade every later API call to
%% the catch-all error).
ack_apply(ack, Ref, P, O, VClient, Stream, CName, Seq, Consumer, St0) ->
    _ = try broker_subscription:ack(Ref, P, O, VClient) catch _:_ -> ok end,
    CKey = consumer_key(Stream, CName),
    Consumer1 = Consumer#{refs => maps:remove(Seq,
                                              maps:get(refs, Consumer, #{}))},
    persist_consumers(St0#st{consumers =
                                 maps:put(CKey, Consumer1,
                                          St0#st.consumers)});
ack_apply(nak, Ref, P, O, VClient, _Stream, _CName, _Seq, _Consumer, St) ->
    _ = try broker_subscription:release(Ref, P, [O], VClient) catch _:_ -> ok end,
    St;
ack_apply(term, Ref, P, O, VClient, Stream, CName, Seq, Consumer, St0) ->
    ack_apply(ack, Ref, P, O, VClient, Stream, CName, Seq, Consumer, St0);
ack_apply(wpi, _Ref, _P, _O, _VClient, _Stream, _CName, _Seq, _Consumer, St) ->
    St.

%%%_* Pending fetch service (on managed publish) ------------------------------

serve_pending_fetches(Stream, St0) ->
    Due = [Key || {Key, F} <- maps:to_list(St0#st.fetches),
                  maps:get(stream, F) =:= Stream],
    lists:foldl(
      fun(Key, Acc) ->
              F = maps:get(Key, Acc#st.fetches),
              _ = erlang:cancel_timer(maps:get(timer, F)),
              Acc1 = Acc#st{fetches = maps:remove(Key, Acc#st.fetches)},
              CKey = consumer_key(Stream, maps:get(consumer, F)),
              case maps:find(CKey, Acc1#st.consumers) of
                  {ok, Consumer} ->
                      {_N, St2} = fetch_serve(Stream, maps:get(consumer, F),
                                              Consumer, maps:get(reply, F),
                                              maps:get(batch, F), Acc1),
                      St2;
                  error ->
                      Acc1
              end
      end, St0, Due).

%%%_* Metadata / persistence --------------------------------------------------

js_destination(Stream) ->
    <<"js/", Stream/binary>>.

consumer_key(Stream, CName) ->
    <<Stream/binary, $,, CName/binary>>.

js_consumer_group(Stream, CName) ->
    <<"js-c-", Stream/binary, $,, CName/binary>>.

js_consumer_vclient(Stream, CName) ->
    <<"js-", Stream/binary, $,, CName/binary>>.

js_ack_subject(Stream, CName, Seq, CSeq) ->
    Ts = erlang:system_time(millisecond),
    <<"$JS.ACK.", Stream/binary, $., CName/binary, ".1.",
      (integer_to_binary(Seq))/binary, ".",
      (integer_to_binary(CSeq))/binary, ".",
      (integer_to_binary(Ts))/binary, ".0">>.

consumer_filter(Consumer) ->
    maps:get(filter_subject, Consumer, <<>>).

consumer_setup(Consumer, St) ->
    Stream = maps:get(stream, Consumer),
    CName = maps:get(name, Consumer),
    Group = js_consumer_group(Stream, CName),
    VClient = js_consumer_vclient(Stream, CName),
    Anchor = case maps:get(deliver_policy, Consumer, all) of
                 all -> #{anchor => earliest};
                 _ -> #{}
             end,
    try broker_subscription:join(shared, Group, VClient,
                                 js_destination(Stream), 1,
                                 <<"*">>, Anchor) of
        {ok, Ref} ->
            Consumer1 = Consumer#{ref => Ref, group => Group,
                                  vclient => VClient},
            {ok, St#st{consumers =
                           maps:put(consumer_key(Stream, CName), Consumer1,
                                    St#st.consumers)}};
        {error, Reason} ->
            logger:warning("nats js consumer join failed: ~p", [Reason]),
            {ok, St}
    catch
        Class:Reason ->
            logger:warning("nats js consumer join crashed ~p:~p", [Class, Reason]),
            {ok, St}
    end.

consumer_teardown(Consumer, St) ->
    _ = try
        broker_subscription:leave(maps:get(group, Consumer),
        maps:get(vclient, Consumer))
    catch
        _:_ -> ok
    end,
    St.

rejoin_consumer(Consumer) ->
    Stream = maps:get(stream, Consumer),
    CName = maps:get(name, Consumer),
    Anchor = case maps:get(deliver_policy, Consumer, all) of
                 all -> #{anchor => earliest};
                 _ -> #{}
             end,
    _ = try
        broker_subscription:join(shared, js_consumer_group(Stream, CName),
        js_consumer_vclient(Stream, CName),
        js_destination(Stream), 1,
        <<"*">>, Anchor)
    catch
        _:_ -> ok
    end,
    ok.

stream_seq_of(Msg) ->
    case js_meta(Msg) of
        #{<<"stream_seq">> := Seq} -> Seq;
        _ -> 0
    end.

js_original_subject(Msg) ->
    case js_meta(Msg) of
        #{<<"subject">> := S} -> S;
        _ -> <<>>
    end.

js_meta(#broker_message{proto_meta = #{<<"nats">> := M}}) when is_map(M) -> M;
js_meta(_) -> #{}.

js_reply_of(Msg) ->
    case js_meta(Msg) of
        #{<<"reply">> := R} -> R;
        _ -> <<>>
    end.

persist_streams(#st{dir = Dir, streams = Streams} = St) ->
    ok = file:write_file(filename:join([Dir, <<"streams.term">>]),
                         term_to_binary(Streams), [sync]),
    St.

persist_consumers(#st{dir = Dir, consumers = Consumers} = St) ->
    ok = file:write_file(filename:join([Dir, <<"consumers.term">>]),
                         term_to_binary(Consumers), [sync]),
    St.

load_meta(Path) ->
    case file:read_file(Path) of
        {ok, Bin} ->
            try binary_to_term(Bin) of
                Map when is_map(Map) -> Map;
                _ -> #{}
            catch
                _:_ -> #{}
            end;
        {error, _} ->
            #{}
    end.

%%%_* JSON / wire helpers -----------------------------------------------------

info_json(St) ->
    json:encode(#{<<"streams">> => maps:size(St#st.streams),
                  <<"consumers">> => maps:size(St#st.consumers)}).

stream_info_json(Name, St) ->
    case maps:find(Name, St#st.streams) of
        {ok, Meta} ->
            Last = maps:get(last_seq, Meta),
            ConsumerCount = length([C || {_, C} <- maps:to_list(St#st.consumers),
                                         maps:get(stream, C) =:= Name]),
            Json = json:encode(
                     #{<<"stream_name">> => Name,
                       <<"config">> =>
                           #{<<"name">> => Name,
                             <<"subjects">> => maps:get(subjects, Meta, [])},
                       <<"state">> =>
                           #{<<"messages">> => Last,
                             <<"bytes">> => 0,
                             <<"first_seq">> => 1,
                             <<"last_seq">> => Last,
                             <<"consumer_count">> => ConsumerCount}}),
            {ok, Json};
        error ->
            {error, not_found}
    end.

consumer_info_json(Stream, CName, St) ->
    case maps:find(consumer_key(Stream, CName), St#st.consumers) of
        {ok, C} ->
            Last = case maps:find(Stream, St#st.streams) of
                       {ok, M} -> maps:get(last_seq, M);
                       error -> 0
                   end,
            Json = json:encode(
                     #{<<"stream_name">> => Stream,
                       <<"name">> => CName,
                       <<"config">> =>
                           #{<<"deliver_policy">> =>
                                 atom_to_binary(maps:get(deliver_policy, C, all)),
                             <<"ack_policy">> =>
                                 atom_to_binary(maps:get(ack_policy, C, explicit)),
                             <<"ack_wait">> =>
                                 (maps:get(ack_wait_ms, C, 30000)) * 1000000,
                             <<"filter_subject">> =>
                                 maps:get(filter_subject, C, <<>>)},
                       <<"delivered">> =>
                           #{<<"consumer_seq">> => maps:get(cseq, C, 0),
                             <<"stream_seq">> => Last},
                       <<"ack_floor">> =>
                           #{<<"consumer_seq">> => 0,
                             <<"stream_seq">> => 0},
                       <<"num_ack_pending">> =>
                           maps:size(maps:get(refs, C, #{})),
                       <<"num_pending">> => 0}),
            {ok, Json};
        error ->
            {error, not_found}
    end.

reply_json(ReplySubject, Json, St) ->
    deliver_reply(ReplySubject, Json, St).

reply_error(ReplySubject, Code, ErrCode, Desc, St) ->
    Json = json:encode(#{<<"error">> =>
                             #{<<"code">> => Code,
                               <<"err_code">> => ErrCode,
                               <<"description">> => Desc}}),
    deliver_reply(ReplySubject, Json, St).

%% nats.go matches typed errors by `err_code' via errors.Is: 10014 = consumer
%% not found (ErrConsumerNotFound), 10059 = stream not found (ErrStreamNotFound).
%% Without the right err_code, PullSubscribe's lookup error is treated as fatal
%% and the consumer is never auto-created.
reply_consumer_not_found(ReplySubject, St) ->
    reply_error(ReplySubject, 404, 10014, <<"consumer not found">>, St).

reply_stream_not_found(ReplySubject, St) ->
    reply_error(ReplySubject, 404, 10059, <<"stream not found">>, St).

%% nats.go's stream create body IS the StreamConfig; `subjects' is all this
%% MVP honours.
parse_stream_config(Payload) ->
    try json:decode(Payload) of
        #{<<"subjects">> := Subjects} when is_list(Subjects),
                                           Subjects =/= [] ->
            {ok, [to_binary(S) || S <- Subjects]};
        _ ->
            {error, <<"subjects is required">>}
    catch
        _:_ -> {error, <<"invalid JSON body">>}
    end.

parse_consumer_config(Payload) ->
    try json:decode(Payload) of
        Body when is_map(Body) ->
            Cfg = maps:get(<<"config">>, Body, Body),
            Deliver = to_atom(maps:get(<<"deliver_policy">>, Cfg, <<"all">>)),
            Ack = to_atom(maps:get(<<"ack_policy">>, Cfg, <<"explicit">>)),
            case {lists:member(Deliver, [all, new]),
                  lists:member(Ack, [explicit, none])} of
                {true, true} ->
                    AckWaitNs = maps:get(<<"ack_wait">>, Cfg, 30000000000),
                    {ok, #{deliver_policy => Deliver,
                           ack_policy => Ack,
                           ack_wait_ms => max(1, AckWaitNs div 1000000),
                           max_deliver => maps:get(<<"max_deliver">>, Cfg, -1),
                           filter_subject =>
                               to_binary(maps:get(<<"filter_subject">>,
                                                  Cfg, <<>>))}};
                _ ->
                    {error, <<"unsupported deliver_policy/ack_policy (MVP: all|new, explicit|none)">>}
            end;
        _ ->
            {error, <<"invalid JSON body">>}
    catch
        _:_ -> {error, <<"invalid JSON body">>}
    end.

parse_fetch(Payload) ->
    try json:decode(Payload) of
        Body when is_map(Body) ->
            Batch = max(1, maps:get(<<"batch">>, Body, 1)),
            NoWait = maps:get(<<"no_wait">>, Body, false),
            ExpiresMs = case maps:get(<<"expires">>, Body, 0) of
                            0 -> 5000;
                            Ns -> max(1, Ns div 1000000)
                        end,
            {Batch, NoWait, ExpiresMs};
        _ ->
            {1, false, 5000}
    catch
        _:_ -> {1, false, 5000}
    end.

read_offsets(P, Offsets) ->
    {Read, Missing} = lists:foldl(
                        fun(O, {Acc, Miss}) ->
                                try broker_queue:read_from(P, O, 1) of
                                    {ok, [{O, Msg}], _Next} ->
                                        {[{O, Msg} | Acc], Miss};
                                    _ -> {Acc, [O | Miss]}
                                catch
                                    _:_ -> {Acc, [O | Miss]}
                                end
                        end, {[], []}, lists:sort(Offsets)),
    {lists:reverse(Read), lists:reverse(Missing)}.

%%%_* Subject helpers ---------------------------------------------------------

%% NATS wildcard subject matching (token based): `*' one token, `>` rest --
%% with `>` honoured as a wildcard in EITHER position, because stream-subject
%% matching asks "does this stream's subject cover the requested subject"
%% (stream `compatpull.>` covers request `compatpull.a`).
subject_matches(Subject, Pattern) when is_binary(Subject), is_binary(Pattern) ->
    tokens_match(binary:split(Subject, <<".">>, [global]),
                 binary:split(Pattern, <<".">>, [global]));
subject_matches(_, _) ->
    false.

tokens_match([], []) -> true;
tokens_match([<<">">>], _) -> true;
tokens_match(_, [<<">">>]) -> true;
tokens_match([<<"*">> | SRest], [_T | PRest]) ->
    tokens_match(SRest, PRest);
tokens_match([_T | SRest], [<<"*">> | PRest]) ->
    tokens_match(SRest, PRest);
tokens_match([T | SRest], [T | PRest]) ->
    tokens_match(SRest, PRest);
tokens_match(_, _) ->
    false.

match_subject(Subject, Streams) ->
    Candidates = [Name || {Name, Meta} <- maps:to_list(Streams),
                          lists:any(fun(Pat) ->
                                            subject_matches(Subject, Pat)
                                    end, maps:get(subjects, Meta, []))],
    case Candidates of
        [] -> nomatch;
        [Name | _] -> {ok, Name}
    end.

stream_partition_for(Stream, #st{streams = Streams}) ->
    case maps:find(Stream, Streams) of
        {ok, #{partition := P}} -> P;
        error -> 0
    end.

%% All of one stream's data hashes to the same `js/<stream>` destination, so
%% every stream lands on one deterministic partition. Ask the router (the
%% same backend publishes use) so fetch claims and writes always agree.
discover_partition(Destination, Tenant) ->
    case broker_router:env() of
        {ok, Env} ->
            Backend = application:get_env(broker_router, backend,
                                          broker_router_hash),
            Ctx = #route_ctx{tenant_id = Tenant,
                             publisher = ?JS_CLIENT,
                             reliability = fsync},
            case Backend:route(Destination, Ctx, Env) of
                {ok, [{P, _} | _]} -> P;
                _ -> 0
            end;
        _ ->
            0
    end.

%% Only MANAGED publishes (messages that carry a stream_seq) are ever
%% delivered to a consumer: the stream's partition can also hold unrelated
%% traffic (API replies share partitions when destinations collide), and an
%% empty original subject must never match a `>' filter.
js_msg_matches(Msg, FilterSubject) ->
    is_managed(Msg)
    andalso (case FilterSubject of
                 <<>> -> true;
                 Pat -> subject_matches(js_original_subject(Msg), Pat)
             end).

is_managed(Msg) ->
    stream_seq_of(Msg) =/= 0.

ack_action(Payload) ->
    case Payload of
        <<"+ACK", _/binary>> -> ack;
        <<"-NAK", _/binary>> -> nak;
        <<"+TERM", _/binary>> -> term;
        <<"+WPI", _/binary>> -> wpi;
        _ -> ack
    end.

ack_tokens(Destination) ->
    case binary:split(Destination, <<"/">>, [global]) of
        [<<"$JS">>, <<"ACK">>, Stream, CName, _D, SeqBin, _C, _Ts, _Pend] ->
            %% `try' rather than the deprecated `catch Expr' (OTP 29 warns);
            %% a malformed sequence number is simply not an ack.
            try binary_to_integer(SeqBin) of
                Seq when is_integer(Seq) -> {ok, Stream, CName, Seq}
            catch
                _:_ -> error
            end;
        _ ->
            error
    end.

subject_to_destination(Subject) ->
    binary:replace(Subject, <<".">>, <<"/">>, [global]).

to_binary(B) when is_binary(B) -> B;
to_binary(A) when is_atom(A) -> atom_to_binary(A, utf8);
to_binary(L) when is_list(L) -> iolist_to_binary(L);
to_binary(X) -> iolist_to_binary(io_lib:format("~p", [X])).

to_atom(B) when is_binary(B) ->
    try binary_to_existing_atom(B, utf8)
    catch _:_ -> binary_to_atom(B, utf8)
    end.

js_message_id() ->
    base64:encode(crypto:strong_rand_bytes(12)).
