%% @doc Pulsar binary protocol connection (S5, ADR-006, README §11.1).
%%
%% The adapter speaks enough of Pulsar's protobuf wire protocol for the
%% official Java client to connect, create producers and consumers, publish,
%% consume, ack and seek. Messages still travel the broker's normal path:
%% `broker_queue' (fsync) plus `pulsar_subscriptions' for delivery; there is
%% no Pulsar-specific storage (ADR-006).
%%
%% Topic model: a Pulsar topic `t' maps to destination `pulsar/t' on broker
%% partition 0. Entry ids are topic-relative offsets (the record's index among
%% this topic's records), the same convention the Kafka adapter uses.
%%
%% Version posture: connected with protocol version 15 (pre broker-entry
%% metadata), one non-flexible command set; scope and limits are documented in
%% `docs/protocol-compatibility.md' §4.9.
-module(pulsar_conn).

-behaviour(gen_server).

-export([start_link/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-include_lib("broker_core/include/broker_message.hrl").
-include_lib("broker_router/include/broker_router.hrl").

-define(SERVER_VERSION, <<"UnifiedMQ-Pulsar-0.1.0">>).
-define(PROTOCOL_VERSION, 15).
-define(TENANT, <<"t0">>).
-define(DEFAULT_HOST, <<"127.0.0.1">>).
-define(PARTITION, 0).

%% ServerError codes used by this adapter (PulsarApi.proto).
-define(ERROR_UNKNOWN, 0).
-define(ERROR_AUTHENTICATION, 3).
-define(ERROR_AUTHORIZATION, 4).
-define(ERROR_CONSUMER_BUSY, 5).
-define(ERROR_SUBSCRIPTION_NOT_FOUND, 12).
-define(ERROR_CONSUMER_NOT_FOUND, 13).

-record(st, {ref :: term(),
             transport :: module() | undefined,
             socket :: term() | undefined,
             client_version = <<>> :: binary(),
             producers = #{} :: #{integer() => binary()},
             consumers = #{} :: #{{integer(), integer()} => binary()},
             port = 0 :: inet:port_number()}).

%%%_* Lifecycle -----------------------------------------------------------

start_link(Ref, Transport, Opts) ->
    gen_server:start_link(?MODULE, {Ref, Transport, Opts}, []).

init({Ref, Transport, _Opts}) ->
    {ok, #st{ref = Ref, transport = Transport, port = ranch:get_port(Ref)}}.

terminate(_Reason, #st{consumers = Consumers}) ->
    maps:foreach(fun({Topic, Sub}, ConsumerId) ->
                         _ = (catch pulsar_subscriptions:close_consumer(Topic, Sub, ConsumerId))
                 end, Consumers),
    ok.

%%%_* gen_server ----------------------------------------------------------

handle_call(_Req, _From, St) ->
    {reply, {error, unsupported}, St}.

handle_cast(_Msg, St) ->
    {noreply, St}.

handle_info({handshake, _Ref, Transport, Socket, _Timeout}, St) ->
    St1 = St#st{transport = Transport, socket = Socket},
    %% ranch filters `packet` out of listen socket options, so an accepted
    %% socket is raw: switch it to 4-byte framing here, then arm the first
    %% active-once read.
    case Transport:setopts(Socket, [{packet, 4}]) of
        ok -> rearm(St1);
        {error, Reason} -> {stop, {socket_error, Reason}, St1}
    end;
handle_info({tcp, Socket, Frame}, #st{socket = Socket} = St) ->
    on_frame(Frame, St);
handle_info({ssl, Socket, Frame}, #st{socket = Socket} = St) ->
    on_frame(Frame, St);
handle_info({tcp_closed, Socket}, #st{socket = Socket} = St) ->
    {stop, normal, St};
handle_info({ssl_closed, Socket}, #st{socket = Socket} = St) ->
    {stop, normal, St};
handle_info({tcp_error, Socket, Reason}, #st{socket = Socket} = St) ->
    {stop, {socket_error, Reason}, St};
handle_info({ssl_error, Socket, Reason}, #st{socket = Socket} = St) ->
    {stop, {socket_error, Reason}, St};
handle_info({pulsar_deliver, ConsumerId, EntryId, Msg}, St) ->
    case find_consumer(ConsumerId, St) of
        error ->
            {noreply, St};
        {ok, _Topic, _Sub} ->
            {Command, Payload} =
                pulsar_codec:encode_message(ConsumerId, EntryId,
                                            Msg#broker_message.payload),
            Metadata = pulsar_codec:encode_message_metadata(metadata(Msg)),
            Frame = pulsar_codec:encode_payload_frame(Command, Metadata, Payload),
            send(Frame, St)
    end;
handle_info(_Other, St) ->
    {noreply, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%%%_* Input ---------------------------------------------------------------

on_frame(Frame, St) ->
    case pulsar_codec:decode_frame(Frame) of
        {ok, Command, Tail} ->
            case dispatch(Command, Tail, St) of
                {noreply, St1} -> rearm(St1);
                Other -> Other
            end;
        more ->
            rearm(St)
    end.

rearm(St) ->
    case set_active(St) of
        ok -> {noreply, St};
        {error, Reason} -> {stop, {socket_error, Reason}, St}
    end.

set_active(#st{transport = T, socket = S}) ->
    T:setopts(S, [{active, once}]).

dispatch(#{type := connect} = Cmd, _Tail, St) ->
    St1 = St#st{client_version = maps:get(client_version, Cmd, <<>>)},
    send(pulsar_codec:encode_connected(?SERVER_VERSION, ?PROTOCOL_VERSION), St1);
dispatch(#{type := partitioned_metadata} = Cmd, _Tail, St) ->
    Response = pulsar_codec:encode_partitioned_metadata_response(
                 maps:get(request_id, Cmd), 0),
    send(Response, St);
dispatch(#{type := lookup} = Cmd, _Tail, St) ->
    Url = broker_url(St),
    Response = pulsar_codec:encode_lookup_response(
                 maps:get(request_id, Cmd), Url, true),
    send(Response, St);
dispatch(#{type := producer} = Cmd, _Tail, St) ->
    Topic = maps:get(topic, Cmd),
    ProducerId = maps:get(producer_id, Cmd),
    Name = case maps:get(producer_name, Cmd) of
               <<>> -> producer_name(ProducerId);
               N -> N
           end,
    Response = pulsar_codec:encode_producer_success(
                 maps:get(request_id, Cmd), Name, -1),
    Producers = maps:put(ProducerId, Topic, St#st.producers),
    send(Response, St#st{producers = Producers});
dispatch(#{type := send} = Cmd, Tail, St) ->
    handle_send(Cmd, Tail, St);
dispatch(#{type := subscribe} = Cmd, _Tail, St) ->
    Topic = maps:get(topic, Cmd),
    Sub = maps:get(subscription, Cmd),
    ConsumerId = maps:get(consumer_id, Cmd),
    RequestId = maps:get(request_id, Cmd),
    case pulsar_subscriptions:subscribe(Topic, Sub, maps:get(sub_type, Cmd),
                                        ConsumerId, self(),
                                        maps:get(durable, Cmd),
                                        maps:get(initial_position, Cmd)) of
        ok ->
            Consumers = maps:put({ConsumerId, RequestId}, {Topic, Sub},
                                 St#st.consumers),
            send(pulsar_codec:encode_success(RequestId),
                 St#st{consumers = Consumers});
        {error, Code} ->
            send_error(RequestId, Code, <<"subscribe rejected">>, St)
    end;
dispatch(#{type := flow} = Cmd, _Tail, St) ->
    ConsumerId = maps:get(consumer_id, Cmd),
    case find_consumer(ConsumerId, St) of
        {ok, Topic, Sub} ->
            ok = pulsar_subscriptions:flow(Topic, Sub, ConsumerId,
                                           maps:get(permits, Cmd));
        error ->
            ok
    end,
    {noreply, St};
dispatch(#{type := ack} = Cmd, _Tail, St) ->
    ConsumerId = maps:get(consumer_id, Cmd),
    Offsets = [EntryId || {_Ledger, EntryId} <- maps:get(message_ids, Cmd)],
    case find_consumer(ConsumerId, St) of
        {ok, Topic, Sub} ->
            _ = pulsar_subscriptions:ack(Topic, Sub, ConsumerId,
                                         maps:get(ack_type, Cmd), Offsets);
        error ->
            ok
    end,
    case maps:get(request_id, Cmd, 0) of
        0 -> {noreply, St};
        RequestId -> send(pulsar_codec:encode_ack_response(ConsumerId, RequestId), St)
    end;
dispatch(#{type := close_producer} = Cmd, _Tail, St) ->
    ProducerId = maps:get(producer_id, Cmd),
    Producers = maps:remove(ProducerId, St#st.producers),
    send(pulsar_codec:encode_success(maps:get(request_id, Cmd)),
         St#st{producers = Producers});
dispatch(#{type := close_consumer} = Cmd, _Tail, St) ->
    close_consumer(maps:get(consumer_id, Cmd), maps:get(request_id, Cmd), St);
dispatch(#{type := unsubscribe} = Cmd, _Tail, St) ->
    RequestId = maps:get(request_id, Cmd),
    case close_consumer(maps:get(consumer_id, Cmd), RequestId, St) of
        {noreply, St1} ->
            send(pulsar_codec:encode_success(RequestId), St1);
        Other ->
            Other
    end;
dispatch(#{type := ping}, _Tail, St) ->
    send(pulsar_codec:encode_pong(), St);
dispatch(#{type := pong}, _Tail, St) ->
    {noreply, St};
dispatch(#{type := get_last_message_id} = Cmd, _Tail, St) ->
    ConsumerId = maps:get(consumer_id, Cmd),
    EntryId = case find_consumer(ConsumerId, St) of
                  {ok, Topic, _Sub} -> last_entry_id(destination(Topic));
                  error -> -1
              end,
    Response = pulsar_codec:encode_last_message_id_response(
                 maps:get(request_id, Cmd), EntryId),
    send(Response, St);
dispatch(#{type := seek} = Cmd, _Tail, St) ->
    ConsumerId = maps:get(consumer_id, Cmd),
    {_Ledger, EntryId} = maps:get(message_id, Cmd),
    case find_consumer(ConsumerId, St) of
        {ok, Topic, Sub} ->
            _ = pulsar_subscriptions:seek(Topic, Sub, ConsumerId, EntryId);
        error ->
            ok
    end,
    send(pulsar_codec:encode_success(maps:get(request_id, Cmd)), St);
dispatch(#{type := {unknown, Type}}, _Tail, St) ->
    {stop, {unsupported_command, Type}, St};
dispatch(_Other, _Tail, St) ->
    {noreply, St}.

%%%_* Produce -------------------------------------------------------------

handle_send(Cmd, Tail, St) ->
    ProducerId = maps:get(producer_id, Cmd),
    case maps:find(ProducerId, St#st.producers) of
        error ->
            send_error(maps:get(sequence_id, Cmd), ?ERROR_UNKNOWN,
                       <<"unknown producer">>, St);
        {ok, Topic} ->
            case pulsar_codec:decode_payload(Tail) of
                {ok, Meta, Payload} ->
                    Records = split_records(Meta, Payload,
                                            maps:get(num_messages, Cmd)),
                    Base = topic_count(destination(Topic)),
                    lists:foldl(
                      fun({Index, Record}, _Acc) ->
                              append_record(Topic, Base + Index, Record)
                      end, ok, lists:zip(lists:seq(0, length(Records) - 1),
                                         Records)),
                    Receipt = pulsar_codec:encode_send_receipt(
                                ProducerId,
                                maps:get(sequence_id, Cmd),
                                Base,
                                maps:get(highest_sequence_id, Cmd, 0)),
                    send(Receipt, St);
                {error, _Reason} ->
                    send_error(maps:get(sequence_id, Cmd), ?ERROR_UNKNOWN,
                               <<"bad payload">>, St)
            end
    end.

%% A SEND with num_messages > 1 carries a batch: each entry is
%% [metadata_size][SingleMessageMetadata][payload].
split_records(Meta, Payload, NumMessages) when NumMessages > 1 ->
    parse_batch(Meta, Payload, NumMessages, []);
split_records(Meta, Payload, _NumMessages) ->
    [{Meta, Payload}].

parse_batch(_Meta, <<>>, _Remaining, Acc) ->
    lists:reverse(Acc);
parse_batch(Meta, Bin, Remaining, Acc) when Remaining > 0 ->
    case Bin of
        <<MetaSize:32/signed, Rest/binary>> when byte_size(Rest) >= MetaSize ->
            <<MetaBin:MetaSize/binary, Tail/binary>> = Rest,
            Single = pulsar_codec:decode_single_metadata(MetaBin),
            PayloadSize = maps:get(payload_size, Single),
            case Tail of
                <<Payload:PayloadSize/binary, Rest1/binary>> ->
                    Merged = merge_meta(Meta, Single),
                    parse_batch(Meta, Rest1, Remaining - 1,
                                [{Merged, Payload} | Acc]);
                _ ->
                    lists:reverse(Acc)
            end;
        _ ->
            lists:reverse(Acc)
    end;
parse_batch(_Meta, _Bin, _Remaining, Acc) ->
    lists:reverse(Acc).

merge_meta(Meta, Single) ->
    Meta#{properties => maps:get(properties, Single,
                                 maps:get(properties, Meta, [])),
          partition_key => maps:get(partition_key, Single,
                                    maps:get(partition_key, Meta, undefined)),
          sequence_id => maps:get(sequence_id, Single,
                                  maps:get(sequence_id, Meta, 0)),
          event_time => maps:get(event_time, Single,
                                 maps:get(event_time, Meta, 0)),
          null_value => maps:get(null_value, Single,
                                 maps:get(null_value, Meta, false))}.

append_record(Topic, EntryId, {Meta, Payload}) ->
    _ = EntryId,
    Msg = #broker_message{message_id = message_id(),
                          tenant_id = ?TENANT,
                          destination = destination(Topic),
                          partition_id = ?PARTITION,
                          payload = Payload,
                          payload_encoding = raw,
                          headers = maps:from_list(maps:get(properties, Meta, [])),
                          proto_meta = #{<<"pulsar">> => #{producer_name => maps:get(producer_name, Meta, <<>>),
                                                           sequence_id => maps:get(sequence_id, Meta, 0),
                                                           publish_time => maps:get(publish_time, Meta, 0),
                                                           partition_key => maps:get(partition_key, Meta, undefined)}},
                          persistence = durable,
                          reliability = fsync,
                          created_at = created_at(Meta),
                          expires_at = undefined,
                          event = undefined},
    _ = (catch broker_queue:append(?PARTITION, Msg, fsync)),
    ok.

created_at(Meta) ->
    case maps:get(publish_time, Meta, 0) of
        0 -> erlang:system_time(millisecond);
        T -> T
    end.

metadata(Msg) ->
    Proto = maps:get(<<"pulsar">>, Msg#broker_message.proto_meta, #{}),
    Base = #{producer_name => maps:get(producer_name, Proto, <<"unified-mq">>),
             sequence_id => maps:get(sequence_id, Proto, 0),
             publish_time => Msg#broker_message.created_at,
             properties => maps:to_list(Msg#broker_message.headers),
             uncompressed_size => byte_size(Msg#broker_message.payload)},
    case maps:get(partition_key, Proto, undefined) of
        undefined -> Base;
        Key -> Base#{partition_key => Key}
    end.

%%%_* Helpers -------------------------------------------------------------

find_consumer(ConsumerId, #st{consumers = Consumers}) ->
    case [V || {{Id, _Req}, V} <- maps:to_list(Consumers), Id =:= ConsumerId] of
        [{Topic, Sub} | _] -> {ok, Topic, Sub};
        [] -> error
    end.

close_consumer(ConsumerId, RequestId, St) ->
    case find_consumer(ConsumerId, St) of
        {ok, Topic, Sub} ->
            _ = pulsar_subscriptions:close_consumer(Topic, Sub, ConsumerId),
            Consumers = maps:filter(fun({Id, _Req}, _V) -> Id =/= ConsumerId end,
                                    St#st.consumers),
            send(pulsar_codec:encode_success(RequestId),
                 St#st{consumers = Consumers});
        error ->
            send_error(RequestId, ?ERROR_CONSUMER_NOT_FOUND,
                       <<"unknown consumer">>, St)
    end.

send_error(RequestId, Code, Message, St) ->
    send(pulsar_codec:encode_error(RequestId, Code, Message), St).

send(Bin, #st{transport = T, socket = S} = St) ->
    case T:send(S, Bin) of
        ok -> {noreply, St};
        {error, Reason} -> {stop, {socket_error, Reason}, St}
    end.

broker_url(#st{port = Port}) ->
    Host = application:get_env(broker_proto_pulsar, advertised_host, ?DEFAULT_HOST),
    iolist_to_binary([<<"pulsar://">>, Host, <<":">>,
                      integer_to_binary(Port)]).

producer_name(ProducerId) ->
    iolist_to_binary([<<"producer-">>, integer_to_binary(ProducerId)]).

destination(Topic) ->
    <<"pulsar/", Topic/binary>>.

message_id() ->
    Unique = erlang:unique_integer([positive, monotonic]),
    iolist_to_binary(io_lib:format("pulsar-~p-~p", [erlang:system_time(microsecond),
                                                    Unique])).

topic_count(Destination) ->
    count_page(0, high_watermark(), Destination, 0).

last_entry_id(Destination) ->
    case topic_count(Destination) of
        0 -> -1;
        N -> N - 1
    end.

high_watermark() ->
    case broker_queue:watermarks(?PARTITION) of
        {ok, _Committed, High} -> High;
        {error, _} -> 0
    end.

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
