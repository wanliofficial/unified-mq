%% @doc Kafka protocol connection (S5, README §11.1).
%%
%% Implements the subset the official `kafka-clients' 3.7.1 producer and
%% consumer need: ApiVersions, Metadata, Produce, Fetch, ListOffsets,
%% FindCoordinator, JoinGroup, SyncGroup, Heartbeat, OffsetCommit,
%% OffsetFetch, LeaveGroup and InitProducerId. Messages still travel the
%% broker's normal path: `broker_queue' (fsync) plus a `broker_subscription'
%% wake-up for anyone else subscribed to the topic, so a Kafka publish is
%% visible to MQTT/AMQP/NATS subscribers and vice versa.
%%
%% Topic model: a Kafka topic `t' is the destination `kafka/t'. The broker's
%% partition set is exposed as the topic's partitions, so Produce(partition p)
%% appends to broker partition p and Fetch(partition p) reads broker partition
%% p. Consumer groups follow Kafka's own model (client-managed offsets,
%% `kafka_group' coordinator) rather than the broker's ack cursors.
%%
%% Version posture (documented in `docs/protocol-compatibility.md' §4.8):
%% every API advertises one exact non-flexible version, the last one before
%% the flexible encoding. Only ApiVersions v3 is flexible, as clients send it
%% before negotiating.
-module(kafka_conn).

-behaviour(gen_server).

-export([start_link/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-include_lib("broker_core/include/broker_message.hrl").
-include_lib("broker_router/include/broker_router.hrl").

-define(NODE_ID, 1).
-define(HOST, <<"127.0.0.1">>).

%% API keys.
-define(PRODUCE, 0).
-define(FETCH, 1).
-define(LIST_OFFSETS, 2).
-define(METADATA, 3).
-define(OFFSET_COMMIT, 8).
-define(OFFSET_FETCH, 9).
-define(FIND_COORDINATOR, 10).
-define(JOIN_GROUP, 11).
-define(HEARTBEAT, 12).
-define(LEAVE_GROUP, 13).
-define(SYNC_GROUP, 14).
-define(API_VERSIONS, 18).
-define(INIT_PRODUCER_ID, 22).

%% Advertised versions: exactly what this module implements.
-define(SUPPORTED,
        [{?PRODUCE, 8}, {?FETCH, 11}, {?LIST_OFFSETS, 5}, {?METADATA, 8},
         {?OFFSET_COMMIT, 7}, {?OFFSET_FETCH, 5}, {?FIND_COORDINATOR, 2},
         {?JOIN_GROUP, 5}, {?HEARTBEAT, 3}, {?LEAVE_GROUP, 3},
         {?SYNC_GROUP, 3}, {?INIT_PRODUCER_ID, 0}]).

-record(st, {
    ref :: term(),
    transport :: module() | undefined,
    socket :: term() | undefined,
    buffer = <<>> :: binary(),
    client_id :: binary() | undefined,
    tenant = <<"t0">> :: binary(),
    port = 0 :: inet:port_number(),
    groups = #{} :: #{binary() => binary()},   % group => member id
    pending_sync = #{} :: #{{binary(), binary()} => integer()},
    producer_id = 1 :: integer()
}).

%%%_* Lifecycle ------------------------------------------------------------

start_link(Ref, Transport, Opts) ->
    gen_server:start_link(?MODULE, {Ref, Transport, Opts}, []).

init({Ref, Transport, _Opts}) ->
    {ok, #st{ref = Ref, transport = Transport, port = ranch:get_port(Ref)}}.

terminate(_Reason, #st{groups = Groups}) ->
    maps:foreach(fun(Group, MemberId) ->
                         _ = (catch kafka_group:close_member(Group, MemberId))
                 end, Groups),
    ok.

%%%_* gen_server ---------------------------------------------------------

handle_call(_Req, _From, St) ->
    {reply, {error, unsupported}, St}.

handle_cast(_Msg, St) ->
    {noreply, St}.

handle_info({handshake, _Ref, Transport, Socket, _Timeout}, St) ->
    St1 = St#st{transport = Transport, socket = Socket},
    rearm(St1);
handle_info({tcp, Socket, Data}, #st{socket = Socket} = St) ->
    on_data(Data, St);
handle_info({ssl, Socket, Data}, #st{socket = Socket} = St) ->
    on_data(Data, St);
handle_info({tcp_closed, Socket}, #st{socket = Socket} = St) ->
    {stop, normal, St};
handle_info({ssl_closed, Socket}, #st{socket = Socket} = St) ->
    {stop, normal, St};
handle_info({tcp_error, Socket, Reason}, #st{socket = Socket} = St) ->
    {stop, {socket_error, Reason}, St};
handle_info({ssl_error, Socket, Reason}, #st{socket = Socket} = St) ->
    {stop, {socket_error, Reason}, St};
handle_info({kafka_sync_result, Group, MemberId, Assignment},
            #st{pending_sync = Pending} = St) ->
    case maps:take({Group, MemberId}, Pending) of
        {Corr, Rest} ->
            Body = <<0:32/signed, 0:16/signed,
                     (kafka_codec:bytes(Assignment))/binary>>,
            {noreply, St1} = send_response(Corr, Body, St),
            {noreply, St1#st{pending_sync = Rest}};
        error ->
            {noreply, St}
    end;
handle_info(_Other, St) ->
    {noreply, St}.

code_change(_OldVsn, St, _Extra) -> {ok, St}.

%%%_* Input ---------------------------------------------------------------

on_data(Data, St) ->
    case parse(<<(St#st.buffer)/binary, Data/binary>>, St#st{buffer = <<>>}) of
        {noreply, St1} -> rearm(St1);
        Other -> Other
    end.

rearm(St) ->
    case set_active(St) of
        ok -> {noreply, St};
        {error, Reason} -> {stop, {socket_error, Reason}, St}
    end.

parse(<<Size:32/signed, Rest/binary>>, St) when byte_size(Rest) >= Size ->
    <<Payload:Size/binary, Tail/binary>> = Rest,
    case handle_request(Payload, St) of
        {noreply, St1} -> parse(Tail, St1);
        Stop -> Stop
    end;
parse(Buffer, St) ->
    {noreply, St#st{buffer = Buffer}}.

handle_request(<<ApiKey:16/signed, ApiVersion:16/signed,
                 CorrelationId:32/signed, Rest/binary>> = _Payload, St) ->
    {Header, Body} = request_header(ApiKey, ApiVersion, Rest),
    ClientId = maps:get(client_id, Header, undefined),
    St1 = St#st{client_id = ClientId},
    dispatch(ApiKey, ApiVersion, CorrelationId, Body, St1);
handle_request(_Other, St) ->
    {stop, protocol_error, St}.

%% ApiVersions is sent before any version negotiation. Its request header is
%% the classic one and the body is version-specific, so nothing here is needed
%% to answer it (the reply only depends on the requested version). Skipping the
%% body keeps the adapter compatible with both header encodings.
request_header(?API_VERSIONS, _Version, _Bin) ->
    {#{client_id => undefined}, <<>>};
request_header(_ApiKey, _Version, Bin) ->
    {ClientId, R1} = kafka_codec:get_nullable_string(Bin),
    {#{client_id => ClientId}, R1}.

%%%_* Dispatch ------------------------------------------------------------

dispatch(?API_VERSIONS, Version, Corr, Body, St) ->
    handle_api_versions(Version, Corr, Body, St);
dispatch(?METADATA, 8, Corr, Body, St) ->
    handle_metadata(Corr, Body, St);
dispatch(?PRODUCE, 8, Corr, Body, St) ->
    handle_produce(Corr, Body, St);
dispatch(?FETCH, 11, Corr, Body, St) ->
    handle_fetch(Corr, Body, St);
dispatch(?LIST_OFFSETS, 5, Corr, Body, St) ->
    handle_list_offsets(Corr, Body, St);
dispatch(?FIND_COORDINATOR, 2, Corr, Body, St) ->
    handle_find_coordinator(Corr, Body, St);
dispatch(?JOIN_GROUP, 5, Corr, Body, St) ->
    handle_join_group(Corr, Body, St);
dispatch(?SYNC_GROUP, 3, Corr, Body, St) ->
    handle_sync_group(Corr, Body, St);
dispatch(?HEARTBEAT, 3, Corr, Body, St) ->
    handle_heartbeat(Corr, Body, St);
dispatch(?LEAVE_GROUP, 3, Corr, Body, St) ->
    handle_leave_group(Corr, Body, St);
dispatch(?OFFSET_COMMIT, 7, Corr, Body, St) ->
    handle_offset_commit(Corr, Body, St);
dispatch(?OFFSET_FETCH, 5, Corr, Body, St) ->
    handle_offset_fetch(Corr, Body, St);
dispatch(?INIT_PRODUCER_ID, 0, Corr, Body, St) ->
    handle_init_producer_id(Corr, Body, St);
dispatch(ApiKey, _Version, _Corr, _Body, St) ->
    {stop, {unsupported_api, ApiKey}, St}.

%%%_* ApiVersions ---------------------------------------------------------

handle_api_versions(Version, Corr, Body, St) ->
    %% ApiVersions is the one API a client sends before any version
    %% negotiation, so the reply must match the request's encoding: v0-v2 are
    %% non-flexible, v3+ are flexible. The request body (software name and
    %% version) is opaque here.
    _ = Body,
    Entries = lists:sort([{Key, 0, Ver} || {Key, Ver} <- ?SUPPORTED] ++
                         [{?API_VERSIONS, 0, 3}]),
    BodyOut =
        case Version >= 3 of
            true ->
                Encoded =
                    [<<Key:16/signed, Min:16/signed, Max:16/signed,
                       (kafka_codec:tagged_fields(#{}))/binary>>
                     || {Key, Min, Max} <- Entries],
                iolist_to_binary(
                  [<<0:16/signed>>,
                   kafka_codec:compact_array(fun(B) -> B end, Encoded),
                   <<0:32/signed>>,
                   kafka_codec:tagged_fields(#{})]);
            false ->
                Encoded =
                    [<<Key:16/signed, Min:16/signed, Max:16/signed>>
                     || {Key, Min, Max} <- Entries],
                Base = [<<0:16/signed>>,
                        kafka_codec:array(fun(B) -> B end, Encoded)],
                Tail = case Version of
                           0 -> [];
                           _ -> <<0:32/signed>>
                       end,
                iolist_to_binary([Base, Tail])
        end,
    send_response(Corr, BodyOut, St).

%%%_* Metadata ------------------------------------------------------------

handle_metadata(Corr, Body, St) ->
    {Topics, R1} = kafka_codec:get_nullable_array(fun kafka_codec:get_string/1, Body),
    {_AllowAuto, _R2} = kafka_codec:get_bool(R1),
    Names = case Topics of
                undefined -> [];
                List -> List
            end,
    Partitions = partition_count(),
    TopicEntries =
        [iolist_to_binary(
           [<<0:16/signed>>,                         % error: NONE
            kafka_codec:string(Name),
            <<0:8>>,                                 % is_internal
            kafka_codec:array(fun(P) -> partition_entry(P) end,
                              lists:seq(0, Partitions - 1)),
            <<-1:32/signed>>])                       % topic_authorized_operations
         || Name <- Names],
    Brokers = [iolist_to_binary([<<?NODE_ID:32/signed>>,
                                 kafka_codec:string(?HOST),
                                 <<(St#st.port):32/signed>>,
                                 kafka_codec:nullable_string(undefined)])],
    BodyOut = iolist_to_binary(
                [<<0:32/signed>>,                    % throttle
                 kafka_codec:array(fun(B) -> B end, Brokers),
                 kafka_codec:nullable_string(<<"unified-mq">>),
                 <<?NODE_ID:32/signed>>,
                 kafka_codec:array(fun(B) -> B end, TopicEntries),
                 <<-2147483648:32/signed>>]),        % cluster_authorized_operations
    send_response(Corr, BodyOut, St).

partition_entry(P) ->
    iolist_to_binary(
      [<<0:16/signed>>,                              % error: NONE
       <<P:32/signed>>,
       <<?NODE_ID:32/signed>>,
       <<-1:32/signed>>,                             % leader epoch: unknown
       kafka_codec:array(fun(X) -> <<X:32/signed>> end, [?NODE_ID]),
       kafka_codec:array(fun(X) -> <<X:32/signed>> end, [?NODE_ID]),
       kafka_codec:array(fun(X) -> <<X:32/signed>> end, [])]).

%%%_* Produce -------------------------------------------------------------

handle_produce(Corr, Body, St) ->
    {_TransactionalId, R1} = kafka_codec:get_nullable_string(Body),
    {Acks, R2} = kafka_codec:get_int16(R1),
    {_Timeout, R3} = kafka_codec:get_int32(R2),
    {Topics, _R4} = kafka_codec:get_array(fun get_produce_topic/1, R3),
    Responses =
        [begin
             {Name, Partitions} = Topic,
             Parts = [produce_partition(Name, P, St) || P <- Partitions],
             iolist_to_binary([kafka_codec:string(Name),
                               kafka_codec:array(fun(X) -> X end, Parts)])
         end || Topic <- Topics],
    BodyOut = iolist_to_binary(
                [kafka_codec:array(fun(X) -> X end, Responses),
                 <<0:32/signed>>]),
    case Acks of
        0 -> {noreply, St};
        _ -> send_response(Corr, BodyOut, St)
    end.

get_produce_topic(Bin) ->
    {Name, R1} = kafka_codec:get_string(Bin),
    {Parts, R2} = kafka_codec:get_array(fun get_produce_partition/1, R1),
    {{Name, Parts}, R2}.

get_produce_partition(Bin) ->
    {Index, R1} = kafka_codec:get_int32(Bin),
    {Records, R2} = kafka_codec:get_nullable_bytes(R1),
    {{Index, Records}, R2}.

produce_partition(Topic, {Index, Records}, St) ->
    case Index >= 0 andalso Index < partition_count() of
        false ->
            produce_error(Index, 3);                    % UNKNOWN_TOPIC_OR_PARTITION
        true ->
            case kafka_codec:decode_record_batches(Records) of
                {ok, Batches, _Tail} ->
                    append_batches(Topic, Index, Batches, St);
                {error, _Reason} ->
                    produce_error(Index, 2)                 % CORRUPT_MESSAGE
            end
    end.

%% Kafka offsets are per topic-partition and start at 0. The broker partition
%% is shared by every topic, so the Kafka offset of a record is its index
%% among that topic's records in the partition; Produce returns that index as
%% `base_offset' and Fetch/ListOffsets translate back the same way.
append_batches(Topic, Partition, Batches, St) ->
    Destination = destination(Topic),
    Refs = route_refs(Destination, St),
    Base = topic_count(Partition, Destination),
    Results = [append_record(Topic, Partition, Record, Destination, Refs, St)
               || Record <- lists:append([Records || #{records := Records} <- Batches])],
    case first_error(Results) of
        ok -> produce_ok(Partition, Base);
        {error, {cloudevent_rejected, _}} -> produce_error(Partition, 87); % INVALID_RECORD
        {error, _Reason} -> produce_error(Partition, 2)                    % CORRUPT_MESSAGE
    end.

first_error([]) -> ok;
first_error([ok | Rest]) -> first_error(Rest);
first_error([{error, _} = Error | _]) -> Error.

produce_ok(Partition, BaseOffset) ->
    iolist_to_binary(
      [<<Partition:32/signed, 0:16/signed>>,
       <<BaseOffset:64/signed>>,
       <<-1:64/signed>>,                        % log_append_time
       <<0:64/signed>>,                         % log_start_offset
       kafka_codec:array(fun(X) -> X end, []),  % record_errors
       kafka_codec:nullable_string(undefined)]).

append_record(Topic, Partition, Record, Destination, Refs, St) ->
    _ = Topic,
    Msg = #broker_message{message_id = message_id(),
                          tenant_id = St#st.tenant,
                          destination = Destination,
                          partition_id = Partition,
                          payload = value_of(Record),
                          payload_encoding = raw,
                          headers = headers_of(Record),
                          proto_meta = #{<<"kafka">> =>
                                             #{<<"topic">> => Topic,
                                               <<"partition">> => Partition,
                                               <<"key">> => maps:get(key, Record, undefined),
                                               <<"timestamp">> => maps:get(timestamp, Record, 0)}},
                          persistence = durable,
                          reliability = fsync,
                          created_at = maps:get(timestamp, Record,
                                                erlang:system_time(millisecond)),
                          expires_at = undefined,
                          event = undefined},
    case broker_event:apply_policy(Msg) of
        {ok, Accepted} ->
            case (catch broker_queue:append(Partition, Accepted, fsync)) of
                {ok, Offset} ->
                    _ = (catch broker_subscription:dispatch(
                                 Refs, {Partition, Offset, Accepted})),
                    ok;
                {error, Reason} ->
                    {error, Reason};
                Other ->
                    {error, {append_crashed, Other}}
            end;
        {error, Reason} ->
            {error, {cloudevent_rejected, Reason}}
    end.

value_of(Record) ->
    case maps:get(value, Record, undefined) of
        undefined -> <<>>;
        Value -> Value
    end.

headers_of(Record) ->
    maps:from_list([{K, V} || {K, V} <- maps:get(headers, Record, []),
                              K =/= undefined, V =/= undefined]).

produce_error(Partition, Code) ->
    iolist_to_binary(
      [<<Partition:32/signed, Code:16/signed>>,
       <<-1:64/signed>>,
       <<-1:64/signed>>,
       <<-1:64/signed>>,
       kafka_codec:array(fun(X) -> X end, []),
       kafka_codec:nullable_string(undefined)]).

%%%_* Fetch ---------------------------------------------------------------

handle_fetch(Corr, Body, St) ->
    {_ReplicaId, R1} = kafka_codec:get_int32(Body),
    {_MaxWait, R2} = kafka_codec:get_int32(R1),
    {_MinBytes, R3} = kafka_codec:get_int32(R2),
    {MaxBytes, R4} = kafka_codec:get_int32(R3),
    {_Isolation, R5} = kafka_codec:get_int8(R4),
    {_SessionId, R6} = kafka_codec:get_int32(R5),
    {_SessionEpoch, R7} = kafka_codec:get_int32(R6),
    {Topics, R8} = kafka_codec:get_array(fun get_fetch_topic/1, R7),
    {_Forgotten, R9} = kafka_codec:get_array(fun get_forgotten/1, R8),
    {_Rack, _R10} = kafka_codec:get_string(R9),
    Responses =
        [begin
             {Name, Parts} = Topic,
             Encoded = [fetch_partition(Name, P, MaxBytes) || P <- Parts],
             iolist_to_binary([kafka_codec:string(Name),
                               kafka_codec:array(fun(X) -> X end, Encoded)])
         end || Topic <- Topics],
    BodyOut = iolist_to_binary(
                [<<0:32/signed>>,                    % throttle
                 <<0:16/signed>>,                    % error: NONE (top level)
                 <<0:32/signed>>,                    % session id: sessionless
                 kafka_codec:array(fun(X) -> X end, Responses)]),
    send_response(Corr, BodyOut, St).

get_fetch_topic(Bin) ->
    {Name, R1} = kafka_codec:get_string(Bin),
    {Parts, R2} = kafka_codec:get_array(fun get_fetch_partition/1, R1),
    {{Name, Parts}, R2}.

get_fetch_partition(Bin) ->
    {Partition, R1} = kafka_codec:get_int32(Bin),
    {_CurrentEpoch, R2} = kafka_codec:get_int32(R1),
    {FetchOffset, R3} = kafka_codec:get_int64(R2),
    {_LogStart, R4} = kafka_codec:get_int64(R3),
    {MaxBytes, R5} = kafka_codec:get_int32(R4),
    {{Partition, FetchOffset, MaxBytes}, R5}.

get_forgotten(Bin) ->
    {Name, R1} = kafka_codec:get_string(Bin),
    {Parts, R2} = kafka_codec:get_array(fun kafka_codec:get_int32/1, R1),
    {{Name, Parts}, R2}.

fetch_partition(Topic, {Partition, FetchOffset, MaxBytes}, RequestMax) ->
    case Partition >= 0 andalso Partition < partition_count() of
        false ->
            fetch_error(Partition, 3);
        true ->
            Limit = max(1, min(MaxBytes, RequestMax div max(1, 8))),
            case topic_entries(Partition, destination(Topic)) of
                {ok, Entries, Total} ->
                    Candidates = [E || {K, _Abs, _Msg} = E <- Entries,
                                       K >= FetchOffset],
                    Selected = take_entries(Candidates, Limit, [], 0),
                    Records = [record_entry(K, Msg) || {K, _Abs, Msg} <- Selected],
                    Base = case Selected of
                               [{K0, _, _} | _] -> K0;
                               [] -> FetchOffset
                           end,
                    Batch = kafka_codec:encode_record_batch(Base, Records),
                    iolist_to_binary(
                      [<<Partition:32/signed, 0:16/signed>>,
                       <<Total:64/signed>>,              % high watermark
                       <<Total:64/signed>>,              % last stable offset
                       <<0:64/signed>>,                  % log start offset
                       kafka_codec:nullable_array(fun(X) -> X end, undefined),
                       <<-1:32/signed>>,                 % preferred read replica
                       kafka_codec:nullable_bytes(Batch)]);
                {error, _} ->
                    fetch_error(Partition, 1)
            end
    end.

%% The broker partition holds messages for several topics, so the Kafka
%% offset of a record is its index among this topic's records. Scan the whole
%% partition and hand back `{KafkaOffset, BrokerOffset, Msg}' triples plus the
%% current end offset (the topic's record count).
topic_entries(Partition, Destination) ->
    case broker_queue:watermarks(Partition) of
        {ok, _Committed, High} ->
            collect_entries(Partition, 0, High, Destination, 0, []);
        {error, Reason} ->
            {error, Reason}
    end.

collect_entries(_Partition, From, High, _Destination, K, Acc) when From >= High ->
    {ok, lists:reverse(Acc), K};
collect_entries(Partition, From, High, Destination, K, Acc) ->
    case (catch broker_queue:read_from(Partition, From, 256)) of
        {ok, [], _Next} ->
            {ok, lists:reverse(Acc), K};
        {ok, Page, Next} ->
            {K1, Acc1} = collect_page(Page, Destination, K, Acc),
            Next1 = case Next > From of
                        true -> Next;
                        false -> From + length(Page)
                    end,
            collect_entries(Partition, Next1, High, Destination, K1, Acc1);
        {error, Reason} ->
            {error, Reason}
    end.

collect_page([], _Destination, K, Acc) ->
    {K, Acc};
collect_page([{Abs, Msg} | Rest], Destination, K, Acc) ->
    case Msg#broker_message.destination =:= Destination of
        true ->
            collect_page(Rest, Destination, K + 1, [{K, Abs, Msg} | Acc]);
        false ->
            collect_page(Rest, Destination, K, Acc)
    end.

topic_count(Partition, Destination) ->
    case topic_entries(Partition, Destination) of
        {ok, _Entries, Total} -> Total;
        {error, _} -> 0
    end.

%% At most `Limit' bytes, but always at least one record so a consumer that
%% asks for a tiny partition budget still makes progress.
take_entries([], _Limit, Acc, _Bytes) ->
    lists:reverse(Acc);
take_entries([{_, _, Msg} = Entry | Rest], Limit, Acc, Bytes) ->
    Size = byte_size(value_of_msg(Msg)) + 64,
    case Acc =/= [] andalso Bytes + Size > Limit of
        true -> lists:reverse(Acc);
        false -> take_entries(Rest, Limit, [Entry | Acc], Bytes + Size)
    end.

value_of_msg(Msg) ->
    Msg#broker_message.payload.

record_entry(Offset, Msg) ->
    #{offset => Offset,
      timestamp => Msg#broker_message.created_at,
      key => kafka_key(Msg),
      value => value_of_msg(Msg),
      headers => kafka_headers(Msg)}.

kafka_key(Msg) ->
    case Msg#broker_message.proto_meta of
        #{<<"kafka">> := #{<<"key">> := Key}} -> Key;
        _ -> undefined
    end.

kafka_headers(Msg) ->
    maps:to_list(Msg#broker_message.headers).

fetch_error(Partition, Code) ->
    iolist_to_binary(
      [<<Partition:32/signed, Code:16/signed>>, <<-1:64/signed>>,
       <<-1:64/signed>>, <<-1:64/signed>>,
       kafka_codec:nullable_array(fun(X) -> X end, undefined),
       <<-1:32/signed>>,
       kafka_codec:nullable_bytes(undefined)]).

%%%_* ListOffsets ---------------------------------------------------------

handle_list_offsets(Corr, Body, St) ->
    {_ReplicaId, R1} = kafka_codec:get_int32(Body),
    {_Isolation, R2} = kafka_codec:get_int8(R1),
    {Topics, _R3} = kafka_codec:get_array(fun get_offsets_topic/1, R2),
    Responses =
        [begin
             {Name, Parts} = Topic,
             Encoded = [offsets_partition(Name, P) || P <- Parts],
             iolist_to_binary([kafka_codec:string(Name),
                               kafka_codec:array(fun(X) -> X end, Encoded)])
         end || Topic <- Topics],
    BodyOut = iolist_to_binary([<<0:32/signed>>,
                                kafka_codec:array(fun(X) -> X end, Responses)]),
    send_response(Corr, BodyOut, St).

get_offsets_topic(Bin) ->
    {Name, R1} = kafka_codec:get_string(Bin),
    {Parts, R2} = kafka_codec:get_array(fun get_offsets_partition/1, R1),
    {{Name, Parts}, R2}.

get_offsets_partition(Bin) ->
    {Partition, R1} = kafka_codec:get_int32(Bin),
    {_Epoch, R2} = kafka_codec:get_int32(R1),
    {Timestamp, R3} = kafka_codec:get_int64(R2),
    {{Partition, Timestamp}, R3}.

offsets_partition(Topic, {Partition, Timestamp}) ->
    case Partition >= 0 andalso Partition < partition_count() of
        false ->
            iolist_to_binary([<<Partition:32/signed, 3:16/signed>>,
                              <<-1:64/signed>>, <<-1:64/signed>>,
                              <<-1:32/signed>>]);
        true ->
            Count = topic_count(Partition, destination(Topic)),
            Offset = case Timestamp of
                         -2 -> 0;                      % earliest
                         _ -> Count                    % latest / unknown ts
                     end,
            iolist_to_binary([<<Partition:32/signed, 0:16/signed>>,
                              <<-1:64/signed>>,
                              <<Offset:64/signed>>,
                              <<-1:32/signed>>])
    end.

%%%_* FindCoordinator -----------------------------------------------------

handle_find_coordinator(Corr, Body, St) ->
    {_Key, R1} = kafka_codec:get_string(Body),
    {_KeyType, _R2} = kafka_codec:get_int8(R1),
    BodyOut = iolist_to_binary([<<0:32/signed>>,
                                <<0:16/signed>>,
                                kafka_codec:nullable_string(undefined),
                                <<?NODE_ID:32/signed>>,
                                kafka_codec:string(?HOST),
                                <<(St#st.port):32/signed>>]),
    send_response(Corr, BodyOut, St).

%%%_* Group protocol ------------------------------------------------------

handle_join_group(Corr, Body, St) ->
    {Group, R1} = kafka_codec:get_string(Body),
    {SessionMs, R2} = kafka_codec:get_int32(R1),
    {RebalanceMs, R3} = kafka_codec:get_int32(R2),
    {MemberId, R4} = kafka_codec:get_string(R3),
    {InstanceId, R5} = kafka_codec:get_nullable_string(R4),
    {ProtocolType, R6} = kafka_codec:get_string(R5),
    {Protocols, _R7} = kafka_codec:get_array(fun get_protocol/1, R6),
    case kafka_group:join(Group, MemberId, InstanceId, ProtocolType, Protocols,
                          SessionMs, RebalanceMs) of
        {ok, Reply} ->
            BodyOut = join_response(0, Reply),
            {noreply, send_noreply(Corr, BodyOut, remember_group(Group, Reply, St))};
        {error, Code, NewMemberId} ->
            BodyOut = iolist_to_binary(
                        [<<0:32/signed>>, <<Code:16/signed>>,
                         <<-1:32/signed>>,
                         %% JoinGroup v5's protocolName is non-nullable even on
                         %% the MEMBER_ID_REQUIRED error, so send "".
                         kafka_codec:string(<<>>),
                         kafka_codec:string(<<>>),
                         kafka_codec:string(NewMemberId),
                         kafka_codec:array(fun(X) -> X end, [])]),
            send_response(Corr, BodyOut, St)
    end.

get_protocol(Bin) ->
    {Name, R1} = kafka_codec:get_string(Bin),
    {Metadata, R2} = kafka_codec:get_bytes(R1),
    {{Name, Metadata}, R2}.

join_response(ErrorCode, Reply) ->
    Members = maps:get(members, Reply, []),
    MemberEntries =
        [iolist_to_binary([kafka_codec:string(Id),
                           kafka_codec:nullable_string(Instance),
                           kafka_codec:bytes(Meta)])
         || {Id, Instance, Meta} <- Members],
    iolist_to_binary(
      [<<0:32/signed>>,
       <<ErrorCode:16/signed>>,
       <<(maps:get(generation, Reply, 0)):32/signed>>,
       kafka_codec:string(maps:get(protocol, Reply, <<"range">>)),
       kafka_codec:string(maps:get(leader, Reply, <<>>)),
       kafka_codec:string(maps:get(member_id, Reply, <<>>)),
       kafka_codec:array(fun(X) -> X end, MemberEntries)]).

remember_group(Group, Reply, #st{groups = Groups} = St) ->
    St#st{groups = Groups#{Group => maps:get(member_id, Reply, <<>>)}}.

handle_sync_group(Corr, Body, St) ->
    {Group, R1} = kafka_codec:get_string(Body),
    {Generation, R2} = kafka_codec:get_int32(R1),
    {MemberId, R3} = kafka_codec:get_string(R2),
    {_InstanceId, R4} = kafka_codec:get_nullable_string(R3),
    {Assignments, _R5} = kafka_codec:get_array(fun get_assignment/1, R4),
    case kafka_group:sync(Group, Generation, MemberId, Assignments) of
        {ok, Assignment} ->
            BodyOut = iolist_to_binary([<<0:32/signed>>, <<0:16/signed>>,
                                        kafka_codec:bytes(Assignment)]),
            send_response(Corr, BodyOut, St);
        {wait, _} ->
            Pending = St#st.pending_sync,
            {noreply, St#st{groups = (St#st.groups)#{Group => MemberId},
                            pending_sync = Pending#{{Group, MemberId} => Corr}}};
        {error, Code} ->
            BodyOut = iolist_to_binary([<<0:32/signed>>, <<Code:16/signed>>,
                                        kafka_codec:bytes(<<>>)]),
            send_response(Corr, BodyOut, St)
    end.

get_assignment(Bin) ->
    {MemberId, R1} = kafka_codec:get_string(Bin),
    {Assignment, R2} = kafka_codec:get_bytes(R1),
    {{MemberId, Assignment}, R2}.

handle_heartbeat(Corr, Body, St) ->
    {Group, R1} = kafka_codec:get_string(Body),
    {Generation, R2} = kafka_codec:get_int32(R1),
    {MemberId, _R3} = kafka_codec:get_string(R2),
    Code = case kafka_group:heartbeat(Group, Generation, MemberId) of
               {ok, _} -> 0;
               {error, C} -> C
           end,
    send_response(Corr, iolist_to_binary([<<0:32/signed>>, <<Code:16/signed>>]), St).

handle_leave_group(Corr, Body, St) ->
    {Group, R1} = kafka_codec:get_string(Body),
    {Members, _R2} = kafka_codec:get_array(fun get_leave_member/1, R1),
    Results =
        [begin
             kafka_group:leave(Group, MemberId),
             iolist_to_binary([kafka_codec:string(MemberId),
                               kafka_codec:nullable_string(Instance),
                               <<0:16/signed>>])
         end || {MemberId, Instance} <- Members],
    BodyOut = iolist_to_binary([<<0:32/signed>>, <<0:16/signed>>,
                                kafka_codec:array(fun(X) -> X end, Results)]),
    send_response(Corr, BodyOut, St#st{groups = maps:remove(Group, St#st.groups)}).

get_leave_member(Bin) ->
    {MemberId, R1} = kafka_codec:get_string(Bin),
    {InstanceId, R2} = kafka_codec:get_nullable_string(R1),
    {{MemberId, InstanceId}, R2}.

handle_offset_commit(Corr, Body, St) ->
    {Group, R1} = kafka_codec:get_string(Body),
    {Generation, R2} = kafka_codec:get_int32(R1),
    {MemberId, R3} = kafka_codec:get_string(R2),
    {_InstanceId, R4} = kafka_codec:get_nullable_string(R3),
    {Topics, _R5} = kafka_codec:get_array(fun get_commit_topic/1, R4),
    Offsets = [{Topic, Partition, Offset, Meta}
               || {Topic, Parts} <- Topics,
                  {Partition, Offset, Meta} <- Parts],
    _ = kafka_group:commit(Group, MemberId, Offsets),
    _ = Generation,
    Responses =
        [iolist_to_binary(
           [kafka_codec:string(Topic),
            kafka_codec:array(
              fun({Partition, _Offset, _Meta}) ->
                      <<Partition:32/signed, 0:16/signed>>
              end, Parts)])
         || {Topic, Parts} <- Topics],
    BodyOut = iolist_to_binary([<<0:32/signed>>,
                                kafka_codec:array(fun(X) -> X end, Responses)]),
    send_response(Corr, BodyOut, St).

get_commit_topic(Bin) ->
    {Name, R1} = kafka_codec:get_string(Bin),
    {Parts, R2} = kafka_codec:get_array(fun get_commit_partition/1, R1),
    {{Name, Parts}, R2}.

get_commit_partition(Bin) ->
    {Partition, R1} = kafka_codec:get_int32(Bin),
    {Offset, R2} = kafka_codec:get_int64(R1),
    {_LeaderEpoch, R3} = kafka_codec:get_int32(R2),
    {Meta, R4} = kafka_codec:get_nullable_string(R3),
    {{Partition, Offset, case Meta of undefined -> <<>>; _ -> Meta end}, R4}.

handle_offset_fetch(Corr, Body, St) ->
    {Group, R1} = kafka_codec:get_string(Body),
    {Topics, _R2} = kafka_codec:get_nullable_array(fun get_fetch_offsets_topic/1, R1),
    Result = case Topics of
                 undefined -> kafka_group:fetch_offsets(Group, all);
                 _ -> kafka_group:fetch_offsets(Group, Topics)
             end,
    Grouped = group_offsets(Result),
    Responses =
        [iolist_to_binary(
           [kafka_codec:string(Topic),
            kafka_codec:array(
              fun({Partition, Offset, Meta}) ->
                      iolist_to_binary(
                        [<<Partition:32/signed>>,
                         <<Offset:64/signed>>,
                         <<-1:32/signed>>,
                         kafka_codec:nullable_string(Meta),
                         <<0:16/signed>>])
              end, Parts)])
         || {Topic, Parts} <- maps:to_list(Grouped)],
    BodyOut = iolist_to_binary([<<0:32/signed>>,
                                kafka_codec:array(fun(X) -> X end, Responses),
                                <<0:16/signed>>]),
    send_response(Corr, BodyOut, St).

get_fetch_offsets_topic(Bin) ->
    {Name, R1} = kafka_codec:get_string(Bin),
    {Parts, R2} = kafka_codec:get_array(fun kafka_codec:get_int32/1, R1),
    {{Name, Parts}, R2}.

group_offsets(Entries) ->
    lists:foldl(
      fun({Topic, Partition, Offset, Meta}, Acc) ->
              Parts = maps:get(Topic, Acc, []),
              Acc#{Topic => [{Partition, Offset, Meta} | Parts]}
      end, #{}, Entries).

%%%_* InitProducerId ------------------------------------------------------

handle_init_producer_id(Corr, Body, St) ->
    {_TransactionalId, R1} = kafka_codec:get_nullable_string(Body),
    {_Timeout, _R2} = kafka_codec:get_int32(R1),
    BodyOut = iolist_to_binary([<<0:32/signed>>, <<0:16/signed>>,
                                <<(St#st.producer_id):64/signed>>,
                                <<0:16/signed>>]),
    send_response(Corr, BodyOut, St#st{producer_id = St#st.producer_id + 1}).

%%%_* Helpers -------------------------------------------------------------

destination(Topic) -> <<"kafka/", Topic/binary>>.

partition_count() ->
    length(broker_queue:partition_ids()).

route_refs(Destination, _St) ->
    case broker_router:env() of
        {ok, Env} ->
            Backend = application:get_env(broker_router, backend, broker_router_hash),
            Ctx = #route_ctx{tenant_id = <<"t0">>, publisher = <<"kafka">>,
                             reliability = fsync},
            case Backend:route(Destination, Ctx, Env) of
                {ok, [{_Partition, Refs}]} -> Refs;
                _ -> []
            end;
        {error, _} ->
            []
    end.

message_id() ->
    <<(integer_to_binary(erlang:unique_integer([positive, monotonic])))/binary>>.

send_response(Corr, Body, St) ->
    Payload = <<Corr:32/signed, Body/binary>>,
    Frame = <<(byte_size(Payload)):32/signed, Payload/binary>>,
    _ = raw_send(Frame, St),
    {noreply, St}.

send_noreply(Corr, Body, St) ->
    {noreply, St1} = send_response(Corr, Body, St),
    St1.

raw_send(_Data, #st{socket = undefined}) -> ok;
raw_send(Data, #st{transport = T, socket = S}) -> T:send(S, Data).

set_active(#st{transport = T, socket = S}) ->
    T:setopts(S, [{active, once}]).
