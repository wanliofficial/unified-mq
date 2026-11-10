%% @doc S5 Kafka adapter over real sockets: ApiVersions, Metadata, Produce,
%% Fetch, ListOffsets, the consumer-group protocol and offset storage.
%%
%% The suite's client is intentionally byte-level: it speaks the documented
%% Kafka subset directly. The Gate S5 evidence for a third-party client comes
%% from `test/protocol-compatibility/kafka/' (Apache `kafka-clients` 3.7.1).
-module(kafka_conn_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(N, 2).
-define(REF, {kafka, 0}).

-record(c, {s :: gen_tcp:socket()}).

all() ->
    [api_versions_v3,
     api_versions_v0,
     metadata_lists_partitions,
     produce_then_fetch,
     produce_offsets_are_topic_relative,
     batch_offsets_are_contiguous,
     list_offsets_earliest_latest,
     empty_topic_fetch_is_empty,
     unknown_partition_is_an_error,
     find_coordinator_points_here,
     consumer_group_join_sync_heartbeat,
     offset_commit_and_fetch,
     leave_group_removes_member,
     correlation_id_is_echoed,
     sasl_plain_authenticates_against_the_shared_table,
     sasl_plain_with_a_wrong_password_is_refused,
     sasl_plain_with_an_unknown_mechanism_is_refused].

suite() -> [{timetrap, {seconds, 90}}].

init_per_suite(Config) ->
    _ = application:load(broker_storage),
    _ = application:load(broker_router),
    _ = application:load(broker_queue),
    _ = application:load(broker_subscription),
    _ = application:load(broker_event),
    _ = application:load(broker_security),
    _ = application:load(broker_retain),
    _ = application:load(broker_proto_kafka),
    Config.

end_per_suite(_Config) ->
    stop_all(),
    ok.

init_per_testcase(Case, Config) ->
    stop_all(),
    DataDir = filename:join([?config(priv_dir, Config), atom_to_list(Case), "data"]),
    ok = filelib:ensure_dir(filename:join([DataDir, "x"])),
    ok = application:set_env(broker_storage, backend, broker_storage_wal),
    ok = application:set_env(broker_storage, data_dir, DataDir),
    ok = application:set_env(broker_storage, wal, #{require_durable_fs => false}),
    ok = application:set_env(broker_queue, partitions, ?N),
    ok = application:set_env(broker_router, partitions, ?N),
    ok = application:set_env(broker_subscription, data_dir, DataDir),
    ok = application:set_env(broker_retain, data_dir, DataDir),
    ok = application:set_env(broker_event, policy, pass_through),
    %% S7: Kafka's SASL PLAIN credential resolves against the shared
    %% broker_security identity table, so the suite needs a user to
    %% authenticate as.
    ok = application:set_env(broker_security, users,
                             [{<<"alice">>, #{password => <<"pw">>, tenant_id => <<"t1">>}}]),
    ok = application:set_env(broker_security, acl, allow),
    ok = application:set_env(broker_security, require_auth, []),
    ok = application:set_env(broker_proto_kafka, listeners,
                             [#{port => 0, ref => ?REF,
                                num_acceptors => 4, max_connections => 128}]),
    {ok, _} = application:ensure_all_started(broker_storage),
    ok = create_partitions(),
    {ok, _} = application:ensure_all_started(broker_proto_kafka),
    [{port, listener_port()} | Config].

end_per_testcase(_Case, _Config) ->
    stop_all(),
    ok.

create_partitions() ->
    lists:foreach(
      fun(P) ->
              case broker_queue:create_partition(P) of
                  ok -> ok;
                  {error, {already_exists, P}} -> ok;
                  {error, Other} -> exit({create_partition, P, Other})
              end
      end, broker_queue:partition_ids()).

listener_port() ->
    case ranch:get_port(?REF) of
        Port when is_integer(Port) -> Port;
        Other -> exit({listener_port_unavailable, Other})
    end.

stop_all() ->
    _ = application:stop(broker_proto_kafka),
    _ = application:stop(broker_retain),
    _ = application:stop(broker_event),
    _ = application:stop(broker_security),
    _ = application:stop(broker_subscription),
    _ = application:stop(broker_queue),
    _ = application:stop(broker_router),
    _ = application:stop(broker_storage),
    ok.

%%%_* Cases -----------------------------------------------------------------

api_versions_v3(Config) ->
    C = connect(Config),
    Body = iolist_to_binary(
             [kafka_codec:compact_string(<<"ct">>),
              kafka_codec:compact_string(<<"1">>),
              kafka_codec:tagged_fields(#{})]),
    Resp = request(C, 18, 3, Body),
    {ok, Versions} = parse_api_versions(Resp, 3),
    ?assertEqual(8, proplists:get_value(0, Versions)),    % Produce
    ?assertEqual(11, proplists:get_value(1, Versions)),   % Fetch
    ?assertEqual(5, proplists:get_value(2, Versions)),    % ListOffsets
    ?assertEqual(8, proplists:get_value(3, Versions)),    % Metadata
    ?assertEqual(5, proplists:get_value(9, Versions)),    % OffsetFetch
    ?assertEqual(3, proplists:get_value(18, Versions)),   % ApiVersions
    ok.

api_versions_v0(Config) ->
    C = connect(Config),
    Resp = request(C, 18, 0, <<>>),
    {ok, Versions} = parse_api_versions(Resp, 0),
    ?assertEqual(8, proplists:get_value(0, Versions)),
    ?assertEqual(11, proplists:get_value(1, Versions)),
    ok.

metadata_lists_partitions(Config) ->
    C = connect(Config),
    Body = iolist_to_binary(
             [kafka_codec:nullable_array(fun kafka_codec:string/1,
                                         [<<"ct.meta">>]),
              <<1>>]),                                   % allow auto topic creation
    Resp = request(C, 3, 8, Body),
    {ok, Meta} = parse_metadata(Resp),
    ?assertEqual(2, maps:get(partition_count, Meta)),
    ?assertEqual(<<"127.0.0.1">>, maps:get(host, Meta)),
    ?assertEqual(?config(port, Config), maps:get(port, Meta)),
    ok.

produce_then_fetch(Config) ->
    C = connect(Config),
    {0, 0} = produce_one(C, <<"ct.pubsub">>, 0, <<"k">>, <<"hello-ct">>),
    {High, Records} = fetch(C, <<"ct.pubsub">>, 0, 0),
    ?assertEqual(1, High),
    ?assertMatch([#{offset := 0, value := <<"hello-ct">>}], Records),
    ok.

produce_offsets_are_topic_relative(Config) ->
    C = connect(Config),
    {0, 0} = produce_one(C, <<"ct.offsets">>, 0, undefined, <<"a">>),
    {0, 1} = produce_one(C, <<"ct.offsets">>, 0, undefined, <<"b">>),
    {0, 0} = produce_one(C, <<"ct.other">>, 0, undefined, <<"x">>),
    {2, Two} = fetch(C, <<"ct.offsets">>, 0, 0),
    ?assertEqual([0, 1], [maps:get(offset, R) || R <- Two]),
    {1, One} = fetch(C, <<"ct.other">>, 0, 0),
    ?assertEqual([0], [maps:get(offset, R) || R <- One]),
    ok.

batch_offsets_are_contiguous(Config) ->
    C = connect(Config),
    Records = [#{offset => N, timestamp => 1000 + N, key => undefined,
                 value => <<"v", (integer_to_binary(N))/binary>>,
                 headers => []} || N <- [0, 1, 2]],
    Batch = iolist_to_binary(kafka_codec:encode_record_batch(0, Records)),
    {0, 0} = produce_batch(C, <<"ct.batch">>, 0, Batch),
    {3, Got} = fetch(C, <<"ct.batch">>, 0, 0),
    ?assertEqual([0, 1, 2], [maps:get(offset, R) || R <- Got]),
    ?assertEqual([<<"v0">>, <<"v1">>, <<"v2">>],
                 [maps:get(value, R) || R <- Got]),
    ok.

list_offsets_earliest_latest(Config) ->
    C = connect(Config),
    [begin
         {0, K} = produce_one(C, <<"ct.list">>, 0, undefined, <<"x">>),
         _ = K
     end || _ <- [1, 2, 3]],
    {Earliest, Latest} = list_offsets(C, <<"ct.list">>, 0),
    ?assertEqual(0, Earliest),
    ?assertEqual(3, Latest),
    ok.

empty_topic_fetch_is_empty(Config) ->
    C = connect(Config),
    {High, Records} = fetch(C, <<"ct.empty">>, 0, 0),
    ?assertEqual(0, High),
    ?assertEqual([], Records),
    ok.

unknown_partition_is_an_error(Config) ->
    C = connect(Config),
    Records = [#{offset => 0, timestamp => 1, key => undefined,
                 value => <<"x">>, headers => []}],
    Batch = iolist_to_binary(kafka_codec:encode_record_batch(0, Records)),
    {3, _} = produce_batch(C, <<"ct.bad">>, 5, Batch),
    Body = fetch_body(<<"ct.bad">>, 5, 0),
    Resp = request(C, 1, 11, Body),
    {ok, [#{partitions := [#{error := 3}]}]} = parse_fetch(Resp),
    ok.

find_coordinator_points_here(Config) ->
    C = connect(Config),
    Body = iolist_to_binary([kafka_codec:string(<<"ct-group">>), <<0>>]),
    Resp = request(C, 10, 2, Body),
    {ok, Info} = parse_find_coordinator(Resp),
    ?assertEqual(<<"127.0.0.1">>, maps:get(host, Info)),
    ?assertEqual(?config(port, Config), maps:get(port, Info)),
    ok.

consumer_group_join_sync_heartbeat(Config) ->
    C = connect(Config),
    Group = <<"ct-group-1">>,
    Protocols = [{<<"range">>, <<1, 0, 0, 0>>}],
    %% Empty member id: KIP-394 gives one back with MEMBER_ID_REQUIRED.
    #{error := 79, member_id := MemberId} =
        join_group(C, Group, <<>>, Protocols),
    ?assert(byte_size(MemberId) > 0),
    #{error := 0, generation := Gen1, member_id := MemberId,
      leader := Leader, members := Members} =
        join_group(C, Group, MemberId, Protocols),
    ?assertEqual(1, Gen1),
    ?assertEqual(MemberId, Leader),
    ?assertEqual([MemberId], Members),
    Assignment = <<0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0,
                   255, 255, 255, 255>>,
    {0, Assignment} = sync_group(C, Group, Gen1, MemberId, [{MemberId, Assignment}]),
    ?assertEqual(0, heartbeat(C, Group, Gen1, MemberId)),
    ok.

offset_commit_and_fetch(Config) ->
    C = connect(Config),
    Group = <<"ct-commit">>,
    #{member_id := MemberId} = join_group(C, Group, <<>>, []),
    #{error := 0} = join_group(C, Group, MemberId, []),
    {0, _} = commit_offsets(C, Group, 1, MemberId,
                            [{<<"ct.commit">>, [{0, 2, <<"meta">>}]}]),
    {2, [{<<"ct.commit">>, 0, 2, <<"meta">>}]} =
        fetch_offsets(C, Group, [{<<"ct.commit">>, [0]}]),
    ok.

leave_group_removes_member(Config) ->
    C = connect(Config),
    Group = <<"ct-leave">>,
    #{member_id := MemberId} = join_group(C, Group, <<>>, []),
    #{error := 0} = join_group(C, Group, MemberId, []),
    {0, [MemberId]} = leave_group(C, Group, [MemberId]),
    ?assertEqual(25, heartbeat(C, Group, 1, MemberId)),
    ok.

correlation_id_is_echoed(Config) ->
    C = connect(Config),
    Corr = 424242,
    Body = iolist_to_binary(
             [kafka_codec:nullable_array(fun kafka_codec:string/1, []),
              <<0>>]),
    C2 = request_with_correlation(C, 3, 8, Body, Corr),
    ?assertEqual(Corr, C2),
    ok.

%%%_* S7: the shared identity table (SASL PLAIN) -----------------------------

sasl_plain_authenticates_against_the_shared_table(Config) ->
    C = connect(Config),
    %% SaslHandshake (API 17, v1): the client names PLAIN.
    Handshake = request(C, 17, 1, kafka_codec:string(<<"PLAIN">>)),
    <<ErrorCode:16/signed, _Mechanisms/binary>> = Handshake,
    ?assertEqual(0, ErrorCode),
    %% SaslAuthenticate (API 36, v1): the credentials.
    Auth = request(C, 36, 1, kafka_codec:bytes(<<0, "alice", 0, "pw">>)),
    <<AuthCode:16/signed, _Rest/binary>> = Auth,
    ?assertEqual(0, AuthCode),
    ok.

sasl_plain_with_a_wrong_password_is_refused(Config) ->
    C = connect(Config),
    <<_ErrorCode:16/signed, _/binary>> = request(C, 17, 1, kafka_codec:string(<<"PLAIN">>)),
    Auth = request(C, 36, 1, kafka_codec:bytes(<<0, "alice", 0, "wrong">>)),
    <<AuthCode:16/signed, _Rest/binary>> = Auth,
    %% 58 = SASL_AUTHENTICATION_FAILED.
    ?assertEqual(58, AuthCode),
    ok.

sasl_plain_with_an_unknown_mechanism_is_refused(Config) ->
    C = connect(Config),
    Handshake = request(C, 17, 1, kafka_codec:string(<<"SCRAM-SHA-256">>)),
    <<ErrorCode:16/signed, _/binary>> = Handshake,
    %% 33 = UNSUPPORTED_SASL_MECHANISM.
    ?assertEqual(33, ErrorCode),
    ok.

%%%_* Byte-level client ----------------------------------------------------

connect(Config) ->
    {ok, S} = gen_tcp:connect({127, 0, 0, 1}, ?config(port, Config),
                              [binary, {active, false}, {packet, raw}], 5000),
    #c{s = S}.

request(#c{s = S}, ApiKey, Version, Body) ->
    request_frame(S, ApiKey, Version, Body, erlang:unique_integer([positive])).

request_with_correlation(#c{s = S}, ApiKey, Version, Body, Corr) ->
    {Corr, _Resp} = request_frame_with_id(S, ApiKey, Version, Body, Corr),
    Corr.

request_frame(S, ApiKey, Version, Body, Corr) ->
    {_Corr, Resp} = request_frame_with_id(S, ApiKey, Version, Body, Corr),
    Resp.

request_frame_with_id(S, ApiKey, Version, Body, Corr) ->
    ClientId = <<"ct">>,
    Payload = <<ApiKey:16/signed, Version:16/signed, Corr:32/signed,
                (byte_size(ClientId)):16/signed, ClientId/binary, Body/binary>>,
    ok = gen_tcp:send(S, <<(byte_size(Payload)):32/signed, Payload/binary>>),
    {ok, <<Size:32/signed>>} = gen_tcp:recv(S, 4, 10000),
    {ok, Frame} = gen_tcp:recv(S, Size, 10000),
    <<Corr:32/signed, Resp/binary>> = Frame,
    {Corr, Resp}.

%%%_* Requests -------------------------------------------------------------

produce_one(C, Topic, Partition, Key, Value) ->
    Records = [#{offset => 0, timestamp => erlang:system_time(millisecond),
                 key => Key, value => Value, headers => []}],
    Batch = iolist_to_binary(kafka_codec:encode_record_batch(0, Records)),
    produce_batch(C, Topic, Partition, Batch).

produce_batch(C, Topic, Partition, Batch) ->
    Body = iolist_to_binary(
             [kafka_codec:nullable_string(undefined),
              kafka_codec:int16(1),                    % acks
              kafka_codec:int32(5000),
              kafka_codec:array(
                fun({T, P}) ->
                        iolist_to_binary(
                          [kafka_codec:string(T),
                           kafka_codec:array(
                             fun({Part, Records}) ->
                                     iolist_to_binary(
                                       [kafka_codec:int32(Part),
                                        kafka_codec:nullable_bytes(Records)])
                             end, [{P, Batch}])])
                end, [{Topic, Partition}])]),
    Resp = request(C, 0, 8, Body),
    {ok, [#{name := Topic, partitions := [#{error := Error,
                                            base_offset := Base}]}]} =
        parse_produce(Resp),
    {Error, Base}.

fetch(C, Topic, Partition, Offset) ->
    Resp = request(C, 1, 11, fetch_body(Topic, Partition, Offset)),
    {ok, [#{partitions := [#{error := Error, high := High,
                             records := Records}]}]} = parse_fetch(Resp),
    ?assertEqual(0, Error),
    {High, Records}.

fetch_body(Topic, Partition, Offset) ->
    iolist_to_binary(
      [kafka_codec:int32(-1),                      % replica id
       kafka_codec:int32(100),                     % max wait
       kafka_codec:int32(1),                       % min bytes
       kafka_codec:int32(1048576),                 % max bytes
       kafka_codec:int8(0),                        % isolation
       kafka_codec:int32(0),                       % session id
       kafka_codec:int32(0),                       % session epoch
       kafka_codec:array(
         fun({T, P}) ->
                 iolist_to_binary(
                   [kafka_codec:string(T),
                    kafka_codec:array(
                      fun({Part, Off}) ->
                              iolist_to_binary(
                                [kafka_codec:int32(Part),
                                 kafka_codec:int32(-1),
                                 kafka_codec:int64(Off),
                                 kafka_codec:int64(-1),
                                 kafka_codec:int32(1048576)])
                      end, [{P, Offset}])])
         end, [{Topic, Partition}]),
       kafka_codec:array(fun(X) -> X end, []),     % forgotten
       kafka_codec:string(<<>>)]).                 % rack id

list_offsets(C, Topic, Partition) ->
    Body = iolist_to_binary(
             [kafka_codec:int32(-1),
              kafka_codec:int8(0),
              kafka_codec:array(
                fun({T, Part}) ->
                        iolist_to_binary(
                          [kafka_codec:string(T),
                           kafka_codec:array(
                             fun(Ts) ->
                                     iolist_to_binary(
                                       [kafka_codec:int32(Part),
                                        kafka_codec:int32(-1),
                                        kafka_codec:int64(Ts)])
                             end, [-2, -1])])
                end, [{Topic, Partition}])]),
    Resp = request(C, 2, 5, Body),
    {ok, [#{partitions := Parts}]} = parse_list_offsets(Resp),
    [Earliest, Latest] = [O || #{offset := O} <- Parts],
    {Earliest, Latest}.

join_group(C, Group, MemberId, Protocols) ->
    Body = iolist_to_binary(
             [kafka_codec:string(Group),
              kafka_codec:int32(6000),
              kafka_codec:int32(10000),
              kafka_codec:string(MemberId),
              kafka_codec:nullable_string(undefined),
              kafka_codec:string(<<"consumer">>),
              kafka_codec:array(
                fun({Name, Meta}) ->
                        iolist_to_binary([kafka_codec:string(Name),
                                          kafka_codec:bytes(Meta)])
                end, Protocols)]),
    Resp = request(C, 11, 5, Body),
    {ok, #{error := Error, generation := Gen, member_id := NewId,
           leader := Leader, members := Members}} = parse_join_group(Resp),
    #{error => Error, generation => Gen, member_id => NewId,
      leader => Leader, members => Members}.

sync_group(C, Group, Gen, MemberId, Assignments) ->
    Body = iolist_to_binary(
             [kafka_codec:string(Group),
              kafka_codec:int32(Gen),
              kafka_codec:string(MemberId),
              kafka_codec:nullable_string(undefined),
              kafka_codec:array(
                fun({Id, Data}) ->
                        iolist_to_binary([kafka_codec:string(Id),
                                          kafka_codec:bytes(Data)])
                end, Assignments)]),
    Resp = request(C, 14, 3, Body),
    {ok, #{error := Error, assignment := Assignment}} = parse_sync_group(Resp),
    {Error, Assignment}.

heartbeat(C, Group, Gen, MemberId) ->
    Body = iolist_to_binary([kafka_codec:string(Group),
                             kafka_codec:int32(Gen),
                             kafka_codec:string(MemberId)]),
    Resp = request(C, 12, 3, Body),
    {ok, Error} = parse_simple_error(Resp),
    Error.

leave_group(C, Group, Members) ->
    Body = iolist_to_binary(
             [kafka_codec:string(Group),
              kafka_codec:array(
                fun(Id) ->
                        iolist_to_binary([kafka_codec:string(Id),
                                          kafka_codec:nullable_string(undefined)])
                end, Members)]),
    Resp = request(C, 13, 3, Body),
    {ok, Error, Left} = parse_leave_group(Resp),
    {Error, Left}.

commit_offsets(C, Group, Gen, MemberId, Topics) ->
    Body = iolist_to_binary(
             [kafka_codec:string(Group),
              kafka_codec:int32(Gen),
              kafka_codec:string(MemberId),
              kafka_codec:nullable_string(undefined),
              kafka_codec:array(
                fun({T, Parts}) ->
                        iolist_to_binary(
                          [kafka_codec:string(T),
                           kafka_codec:array(
                             fun({P, Offset, Meta}) ->
                                     iolist_to_binary(
                                       [kafka_codec:int32(P),
                                        kafka_codec:int64(Offset),
                                        kafka_codec:int32(-1),
                                        kafka_codec:nullable_string(Meta)])
                             end, Parts)])
                end, Topics)]),
    Resp = request(C, 8, 7, Body),
    {ok, Results} = parse_commit(Resp),
    {0, Results}.

fetch_offsets(C, Group, Topics) ->
    Body = iolist_to_binary(
             [kafka_codec:string(Group),
              kafka_codec:nullable_array(
                fun({T, Parts}) ->
                        iolist_to_binary(
                          [kafka_codec:string(T),
                           kafka_codec:array(fun kafka_codec:int32/1, Parts)])
                end, Topics)]),
    Resp = request(C, 9, 5, Body),
    {ok, Entries} = parse_fetch_offsets(Resp),
    Offset = case Entries of
                 [{_, _, O, _} | _] -> O;
                 [] -> -1
             end,
    {Offset, Entries}.

%%%_* Responses ------------------------------------------------------------

parse_api_versions(Bin, Version) ->
    {Error, R1} = kafka_codec:get_int16(Bin),
    {Entries, R2} =
        case Version >= 3 of
            true ->
                kafka_codec:get_compact_array(fun parse_api_entry_flexible/1, R1);
            false ->
                kafka_codec:get_array(fun parse_api_entry_plain/1, R1)
        end,
    {_Throttle, R3} =
        case Version of
            0 -> {0, R2};
            _ -> kafka_codec:get_int32(R2)
        end,
    {_Tags, _R4} =
        case Version >= 3 of
            true -> kafka_codec:get_tagged_fields(R3);
            false -> {#{}, R3}
        end,
    _ = Error,
    {ok, [{Key, Max} || {Key, _Min, Max} <- Entries]}.

parse_api_entry_plain(Bin) ->
    {Key, R1} = kafka_codec:get_int16(Bin),
    {Min, R2} = kafka_codec:get_int16(R1),
    {Max, R3} = kafka_codec:get_int16(R2),
    {{Key, Min, Max}, R3}.

parse_api_entry_flexible(Bin) ->
    {Key, R1} = kafka_codec:get_int16(Bin),
    {Min, R2} = kafka_codec:get_int16(R1),
    {Max, R3} = kafka_codec:get_int16(R2),
    {_Tags, R4} = kafka_codec:get_tagged_fields(R3),
    {{Key, Min, Max}, R4}.

parse_metadata(Bin) ->
    {_Throttle, R1} = kafka_codec:get_int32(Bin),
    {Brokers, R2} = kafka_codec:get_array(fun parse_broker/1, R1),
    {_ClusterId, R3} = kafka_codec:get_nullable_string(R2),
    {_Controller, R4} = kafka_codec:get_int32(R3),
    {Topics, _R5} = kafka_codec:get_array(fun parse_metadata_topic/1, R4),
    [Broker | _] = Brokers,
    [{_Name, Partitions, _TopicError} | _] = Topics,
    {ok, #{host => maps:get(host, Broker),
           port => maps:get(port, Broker),
           partition_count => length(Partitions)}}.

parse_broker(Bin) ->
    {Id, R1} = kafka_codec:get_int32(Bin),
    {Host, R2} = kafka_codec:get_string(R1),
    {Port, R3} = kafka_codec:get_int32(R2),
    {Rack, _R4} = kafka_codec:get_nullable_string(R3),
    {#{id => Id, host => Host, port => Port, rack => Rack}, _R4}.

parse_metadata_topic(Bin) ->
    {Error, R1} = kafka_codec:get_int16(Bin),
    {Name, R2} = kafka_codec:get_string(R1),
    {_Internal, R3} = kafka_codec:get_int8(R2),
    {Partitions, R4} = kafka_codec:get_array(fun parse_metadata_partition/1, R3),
    {_AuthOps, R5} = kafka_codec:get_int32(R4),
    {{Name, Partitions, Error}, R5}.

parse_metadata_partition(Bin) ->
    {Error, R1} = kafka_codec:get_int16(Bin),
    {Index, R2} = kafka_codec:get_int32(R1),
    {Leader, R3} = kafka_codec:get_int32(R2),
    {Epoch, R4} = kafka_codec:get_int32(R3),
    {Replicas, R5} = kafka_codec:get_array(fun kafka_codec:get_int32/1, R4),
    {Isr, R6} = kafka_codec:get_array(fun kafka_codec:get_int32/1, R5),
    {Offline, R7} = kafka_codec:get_array(fun kafka_codec:get_int32/1, R6),
    {#{error => Error, index => Index, leader => Leader, epoch => Epoch,
       replicas => Replicas, isr => Isr, offline => Offline}, R7}.

parse_produce(Bin) ->
    {Topics, R1} = kafka_codec:get_array(fun parse_produce_topic/1, Bin),
    {_Throttle, _R2} = kafka_codec:get_int32(R1),
    {ok, Topics}.

parse_produce_topic(Bin) ->
    {Name, R1} = kafka_codec:get_string(Bin),
    {Parts, R2} = kafka_codec:get_array(fun parse_produce_partition/1, R1),
    {#{name => Name, partitions => Parts}, R2}.

parse_produce_partition(Bin) ->
    {Partition, R1} = kafka_codec:get_int32(Bin),
    {Error, R2} = kafka_codec:get_int16(R1),
    {Base, R3} = kafka_codec:get_int64(R2),
    {_LogAppendTime, R4} = kafka_codec:get_int64(R3),
    {_LogStart, R5} = kafka_codec:get_int64(R4),
    {_RecordErrors, R6} = kafka_codec:get_array(fun parse_record_error/1, R5),
    {_ErrorMessage, R7} = kafka_codec:get_nullable_string(R6),
    {#{partition => Partition, error => Error, base_offset => Base}, R7}.

parse_record_error(Bin) ->
    {Index, R1} = kafka_codec:get_int32(Bin),
    {Message, R2} = kafka_codec:get_nullable_string(R1),
    {{Index, Message}, R2}.

parse_fetch(Bin) ->
    {_Throttle, R1} = kafka_codec:get_int32(Bin),
    {_Error, R2} = kafka_codec:get_int16(R1),
    {_Session, R3} = kafka_codec:get_int32(R2),
    {Topics, _R4} = kafka_codec:get_array(fun parse_fetch_topic/1, R3),
    {ok, Topics}.

parse_fetch_topic(Bin) ->
    {Name, R1} = kafka_codec:get_string(Bin),
    {Parts, R2} = kafka_codec:get_array(fun parse_fetch_partition/1, R1),
    {#{name => Name, partitions => Parts}, R2}.

parse_fetch_partition(Bin) ->
    {Partition, R1} = kafka_codec:get_int32(Bin),
    {Error, R2} = kafka_codec:get_int16(R1),
    {High, R3} = kafka_codec:get_int64(R2),
    {_Lso, R4} = kafka_codec:get_int64(R3),
    {_LogStart, R5} = kafka_codec:get_int64(R4),
    {_Aborted, R6} = kafka_codec:get_nullable_array(fun parse_aborted/1, R5),
    {_Preferred, R7} = kafka_codec:get_int32(R6),
    {BatchBytes, R8} = kafka_codec:get_nullable_bytes(R7),
    Records = case kafka_codec:decode_record_batches(BatchBytes) of
                  {ok, Batches} ->
                      lists:append([Rs || #{records := Rs} <- Batches]);
                  {ok, Batches, _Tail} ->
                      lists:append([Rs || #{records := Rs} <- Batches]);
                  {error, _} ->
                      []
              end,
    {#{partition => Partition, error => Error, high => High,
       records => Records}, R8}.

parse_aborted(Bin) ->
    {ProducerId, R1} = kafka_codec:get_int64(Bin),
    {FirstOffset, R2} = kafka_codec:get_int64(R1),
    {{ProducerId, FirstOffset}, R2}.

parse_list_offsets(Bin) ->
    {_Throttle, R1} = kafka_codec:get_int32(Bin),
    {Topics, _R2} = kafka_codec:get_array(fun parse_offsets_topic/1, R1),
    {ok, Topics}.

parse_offsets_topic(Bin) ->
    {Name, R1} = kafka_codec:get_string(Bin),
    {Parts, R2} = kafka_codec:get_array(fun parse_offsets_partition/1, R1),
    {#{name => Name, partitions => Parts}, R2}.

parse_offsets_partition(Bin) ->
    {Partition, R1} = kafka_codec:get_int32(Bin),
    {Error, R2} = kafka_codec:get_int16(R1),
    {Timestamp, R3} = kafka_codec:get_int64(R2),
    {Offset, R4} = kafka_codec:get_int64(R3),
    {LeaderEpoch, R5} = kafka_codec:get_int32(R4),
    {#{partition => Partition, error => Error, timestamp => Timestamp,
       offset => Offset, leader_epoch => LeaderEpoch}, R5}.

parse_find_coordinator(Bin) ->
    {_Throttle, R1} = kafka_codec:get_int32(Bin),
    {Error, R2} = kafka_codec:get_int16(R1),
    {_Message, R3} = kafka_codec:get_nullable_string(R2),
    {NodeId, R4} = kafka_codec:get_int32(R3),
    {Host, R5} = kafka_codec:get_string(R4),
    {Port, R6} = kafka_codec:get_int32(R5),
    {ok, #{error => Error, node_id => NodeId, host => Host, port => Port,
           rest => R6}}.

parse_join_group(Bin) ->
    {_Throttle, R1} = kafka_codec:get_int32(Bin),
    {Error, R2} = kafka_codec:get_int16(R1),
    {Gen, R3} = kafka_codec:get_int32(R2),
    {Protocol, R4} = kafka_codec:get_string(R3),
    {Leader, R5} = kafka_codec:get_string(R4),
    {MemberId, R6} = kafka_codec:get_string(R5),
    {Members, _R7} = kafka_codec:get_array(fun parse_join_member/1, R6),
    {ok, #{error => Error, generation => Gen, protocol => Protocol,
           leader => Leader, member_id => MemberId, members => Members}}.

parse_join_member(Bin) ->
    {Id, R1} = kafka_codec:get_string(Bin),
    {_Instance, R2} = kafka_codec:get_nullable_string(R1),
    {_Meta, R3} = kafka_codec:get_bytes(R2),
    {Id, R3}.

parse_sync_group(Bin) ->
    {_Throttle, R1} = kafka_codec:get_int32(Bin),
    {Error, R2} = kafka_codec:get_int16(R1),
    {Assignment, _R3} = kafka_codec:get_bytes(R2),
    {ok, #{error => Error, assignment => Assignment}}.

parse_simple_error(Bin) ->
    {_Throttle, R1} = kafka_codec:get_int32(Bin),
    {Error, _R2} = kafka_codec:get_int16(R1),
    {ok, Error}.

parse_leave_group(Bin) ->
    {_Throttle, R1} = kafka_codec:get_int32(Bin),
    {Error, R2} = kafka_codec:get_int16(R1),
    {Members, _R3} = kafka_codec:get_array(fun parse_leave_member/1, R2),
    {ok, Error, [Id || {Id, 0} <- Members]}.

parse_leave_member(Bin) ->
    {Id, R1} = kafka_codec:get_string(Bin),
    {_Instance, R2} = kafka_codec:get_nullable_string(R1),
    {Error, R3} = kafka_codec:get_int16(R2),
    {{Id, Error}, R3}.

parse_commit(Bin) ->
    {_Throttle, R1} = kafka_codec:get_int32(Bin),
    {Topics, _R2} = kafka_codec:get_array(fun parse_commit_topic/1, R1),
    {ok, Topics}.

parse_commit_topic(Bin) ->
    {Name, R1} = kafka_codec:get_string(Bin),
    {Parts, R2} = kafka_codec:get_array(fun parse_commit_partition/1, R1),
    {{Name, Parts}, R2}.

parse_commit_partition(Bin) ->
    {Partition, R1} = kafka_codec:get_int32(Bin),
    {Error, R2} = kafka_codec:get_int16(R1),
    {{Partition, Error}, R2}.

parse_fetch_offsets(Bin) ->
    {_Throttle, R1} = kafka_codec:get_int32(Bin),
    {Topics, R2} = kafka_codec:get_array(fun parse_fetch_offsets_topic/1, R1),
    {_Error, _R3} = kafka_codec:get_int16(R2),
    {ok, lists:append(Topics)}.

parse_fetch_offsets_topic(Bin) ->
    {Name, R1} = kafka_codec:get_string(Bin),
    {Parts, R2} = kafka_codec:get_array(fun parse_fetch_offsets_partition/1, R1),
    {[{Name, P, O, M} || {P, O, M} <- Parts], R2}.

parse_fetch_offsets_partition(Bin) ->
    {Partition, R1} = kafka_codec:get_int32(Bin),
    {Offset, R2} = kafka_codec:get_int64(R1),
    {_LeaderEpoch, R3} = kafka_codec:get_int32(R2),
    {Meta, R4} = kafka_codec:get_nullable_string(R3),
    {_Error, R5} = kafka_codec:get_int16(R4),
    {{Partition, Offset, case Meta of undefined -> <<>>; _ -> Meta end}, R5}.
