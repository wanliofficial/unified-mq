%% @doc S5 WebSocket adapter: RFC 6455 handshake + the broker's own JSON/binary
%% message API, over real sockets.
%%
%% The suite's client is intentionally byte-level (it speaks the documented
%% framing directly). The Gate S5 evidence for third-party transport comes from
%% `test/protocol-compatibility/ws/` (Python `websocket-client'); this suite
%% pins the application protocol and the refusal paths.
-module(ws_conn_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(N, 2).
-define(REF, {ws, 0}).
-define(POLL_MS, 25).
-define(POLL_BUDGET_MS, 5000).

all() ->
    [plain_http_is_rejected,
     connect_returns_connack,
     publish_qos1_roundtrip_and_ack,
     qos0_delivery_needs_no_ack,
     binary_frame_publish_and_delivery,
     unsubscribe_stops_delivery,
     offline_catch_up_after_clean_false,
     clean_start_discards_the_session,
     invalid_filter_is_refused,
     shared_subscription_is_refused,
     ping_pong,
     oversized_publish_is_refused,
     malformed_json_gets_an_error,
     acknowledgement_of_unknown_delivery_is_an_error].

%%%_* Fixtures --------------------------------------------------------------

init_per_suite(Config) ->
    _ = application:load(broker_storage),
    _ = application:load(broker_router),
    _ = application:load(broker_queue),
    _ = application:load(broker_subscription),
    _ = application:load(broker_event),
    _ = application:load(broker_security),
    _ = application:load(broker_proto_ws),
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
    ok = application:set_env(broker_subscription, cursor_max_regress, 64),
    ok = application:set_env(broker_subscription, cursor_flush_interval_ms, 20),
    ok = application:set_env(broker_subscription, inflight_flush_interval_ms, 20),
    ok = application:set_env(broker_event, policy, pass_through),
    ok = application:set_env(broker_security, users, []),
    ok = application:set_env(broker_security, acl, allow),
    ok = application:set_env(broker_proto_ws, listeners,
                             [#{port => 0, num_acceptors => 4, max_connections => 128}]),
    ok = application:set_env(broker_proto_ws, max_publish_bytes, 1024),
    {ok, _} = application:ensure_all_started(broker_storage),
    ok = create_partitions(),
    {ok, _} = application:ensure_all_started(broker_proto_ws),
    [{data_dir, DataDir}, {port, listener_port()} | Config].

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
    _ = application:stop(broker_proto_ws),
    _ = application:stop(broker_retain),
    _ = application:stop(broker_event),
    _ = application:stop(broker_security),
    _ = application:stop(broker_subscription),
    _ = application:stop(broker_queue),
    _ = application:stop(broker_router),
    _ = application:stop(broker_storage),
    ok.

%%%_* Cases -----------------------------------------------------------------

plain_http_is_rejected(Config) ->
    Port = ?config(port, Config),
    {ok, S} = gen_tcp:connect({127, 0, 0, 1}, Port, [binary, {active, false}]),
    ok = gen_tcp:send(S, <<"GET / HTTP/1.1\r\nHost: x\r\n\r\n">>),
    {ok, Response} = gen_tcp:recv(S, 0, 5000),
    <<"HTTP/1.1 400", _/binary>> = Response,
    gen_tcp:close(S),
    ok.

connect_returns_connack(Config) ->
    S = open(Config),
    #{<<"op">> := <<"connack">>, <<"session_present">> := false} =
        request(S, #{<<"op">> => <<"connect">>, <<"client_id">> => <<"c1">>}),
    close(S),
    ok.

publish_qos1_roundtrip_and_ack(Config) ->
    Sub = open(Config),
    Publisher = open(Config),
    connack(Sub, <<"sub1">>),
    connack(Publisher, <<"pub1">>),
    suback(Sub, <<"sport/#">>, 1),
    publish(Publisher, <<"sport/a">>, 1, <<"hello">>),
    Frame = recv_message(Sub),
    #{<<"op">> := <<"message">>, <<"destination">> := <<"sport/a">>,
      <<"payload_b64">> := B64, <<"delivery_id">> := Id, <<"qos">> := 1}
        = message_header(Frame),
    <<"hello">> = base64:decode(B64),
    ok = send_json(Sub, #{<<"op">> => <<"ack">>, <<"delivery_id">> => Id}),
    #{<<"op">> := <<"ackack">>} = recv_json(Sub),
    close(Sub),
    close(Publisher),
    ok.

qos0_delivery_needs_no_ack(Config) ->
    Sub = open(Config),
    Publisher = open(Config),
    connack(Sub, <<"sub0">>),
    connack(Publisher, <<"pub0">>),
    suback(Sub, <<"t/0">>, 0),
    publish(Publisher, <<"t/0">>, 0, <<"zero">>),
    Frame = message_header(recv_message(Sub)),
    #{<<"qos">> := 0, <<"delivery_id">> := null} = Frame,
    close(Sub),
    close(Publisher),
    ok.

binary_frame_publish_and_delivery(Config) ->
    Sub = open(Config),
    Publisher = open(Config),
    connack(Sub, <<"subb">>),
    connack(Publisher, <<"pubb">>),
    suback(Sub, <<"bin/#">>, 1),
    Header = #{<<"op">> => 1, <<"id">> => 7, <<"destination">> => <<"bin/a">>,
               <<"qos">> => 1},
    HeaderBin = iolist_to_binary(json:encode(Header)),
    Body = <<0, 1, 2, 255>>,
    HLen = byte_size(HeaderBin),
    ok = send_frame(Publisher, 2,
                    <<1:8, HLen:32, HeaderBin/binary, Body/binary>>),
    #{<<"op">> := <<"puback">>, <<"id">> := 7} = recv_json(Publisher),
    Frame = recv_frame(Sub),
    #{opcode := 2, payload := <<3:8, SHLen:32, H:SHLen/binary, SBody/binary>>} = Frame,
    #{<<"op">> := <<"message">>, <<"destination">> := <<"bin/a">>,
      <<"delivery_id">> := Id} = json:decode(H),
    <<0, 1, 2, 255>> = SBody,
    ok = send_binary(Sub, 2, #{<<"op">> => 2, <<"delivery_id">> => Id}, <<>>),
    #{<<"op">> := <<"ackack">>} = recv_json(Sub),
    close(Sub),
    close(Publisher),
    ok.

unsubscribe_stops_delivery(Config) ->
    Sub = open(Config),
    Publisher = open(Config),
    connack(Sub, <<"subu">>),
    connack(Publisher, <<"pubu">>),
    suback(Sub, <<"u/1">>, 1),
    ok = send_json(Sub, #{<<"op">> => <<"unsubscribe">>, <<"id">> => 2,
                          <<"destination">> => <<"u/1">>}),
    #{<<"op">> := <<"unsuback">>} = recv_json(Sub),
    publish(Publisher, <<"u/1">>, 1, <<"gone">>),
    receive
        {ws_frame, _} -> ct:fail(unexpected_delivery)
    after 300 -> ok
    end,
    close(Sub),
    close(Publisher),
    ok.

offline_catch_up_after_clean_false(Config) ->
    Sub = open(Config),
    connack(Sub, <<"off1">>, false),
    suback(Sub, <<"off/#">>, 1),
    close(Sub),
    wait_until(fun() -> broker_subscription:session(<<"off1">>) =/= {error, not_found} end),
    Publisher = open(Config),
    connack(Publisher, <<"puboff">>),
    publish(Publisher, <<"off/a">>, 1, <<"later">>),
    Sub2 = open(Config),
    #{<<"op">> := <<"connack">>, <<"session_present">> := true} =
        request(Sub2, #{<<"op">> => <<"connect">>, <<"client_id">> => <<"off1">>,
                        <<"clean_start">> => false}),
    Frame = recv_message(Sub2),
    #{<<"destination">> := <<"off/a">>, <<"delivery_id">> := Id} = message_header(Frame),
    ok = send_json(Sub2, #{<<"op">> => <<"ack">>, <<"delivery_id">> => Id}),
    #{<<"op">> := <<"ackack">>} = recv_json(Sub2),
    close(Sub2),
    close(Publisher),
    ok.

clean_start_discards_the_session(Config) ->
    Sub = open(Config),
    connack(Sub, <<"clean1">>, false),
    suback(Sub, <<"c/#">>, 1),
    close(Sub),
    wait_until(fun() -> broker_subscription:session(<<"clean1">>) =/= {error, not_found} end),
    Sub2 = open(Config),
    #{<<"op">> := <<"connack">>, <<"session_present">> := false} =
        request(Sub2, #{<<"op">> => <<"connect">>, <<"client_id">> => <<"clean1">>,
                        <<"clean_start">> => true}),
    close(Sub2),
    wait_until(fun() -> broker_subscription:session(<<"clean1">>) =:= {error, not_found} end),
    ok.

invalid_filter_is_refused(Config) ->
    S = open(Config),
    connack(S, <<"badf">>),
    #{<<"op">> := <<"suback">>, <<"granted">> := false} =
        request(S, #{<<"op">> => <<"subscribe">>, <<"id">> => 1,
                     <<"destination">> => <<"a/#/b">>, <<"qos">> => 1}),
    close(S),
    ok.

shared_subscription_is_refused(Config) ->
    S = open(Config),
    connack(S, <<"shared">>),
    #{<<"op">> := <<"suback">>, <<"granted">> := false,
      <<"reason">> := <<"shared subscriptions are not supported (S5 WebSocket v1)">>} =
        request(S, #{<<"op">> => <<"subscribe">>, <<"id">> => 1,
                     <<"destination">> => <<"s/#">>, <<"qos">> => 1,
                     <<"kind">> => <<"shared">>, <<"group">> => <<"g">>}),
    close(S),
    ok.

ping_pong(Config) ->
    S = open(Config),
    connack(S, <<"p1">>),
    #{<<"op">> := <<"pong">>} = request(S, #{<<"op">> => <<"ping">>}),
    close(S),
    ok.

oversized_publish_is_refused(Config) ->
    S = open(Config),
    connack(S, <<"big">>),
    #{<<"op">> := <<"error">>, <<"code">> := <<"payload_too_large">>} =
        request(S, #{<<"op">> => <<"publish">>, <<"id">> => 1,
                     <<"destination">> => <<"big/a">>,
                     <<"payload_b64">> => base64:encode(binary:copy(<<"x">>, 5000))}),
    close(S),
    ok.

malformed_json_gets_an_error(Config) ->
    S = open(Config),
    ok = send_frame(S, 1, <<"{not json">>),
    #{<<"op">> := <<"error">>, <<"code">> := <<"bad_json">>} = recv_json(S),
    close(S),
    ok.

acknowledgement_of_unknown_delivery_is_an_error(Config) ->
    S = open(Config),
    connack(S, <<"acku">>),
    #{<<"op">> := <<"error">>, <<"code">> := <<"unknown_delivery">>} =
        request(S, #{<<"op">> => <<"ack">>, <<"delivery_id">> => 999}),
    close(S),
    ok.

%%%_* Wire client -----------------------------------------------------------

open(Config) ->
    Port = ?config(port, Config),
    {ok, S} = gen_tcp:connect({127, 0, 0, 1}, Port,
                              [binary, {active, false}, {nodelay, true}]),
    Key = base64:encode(crypto:strong_rand_bytes(16)),
    Request = [<<"GET /ws HTTP/1.1\r\nHost: localhost\r\n"
                 "Upgrade: websocket\r\nConnection: Upgrade\r\n"
                 "Sec-WebSocket-Key: ">>, Key, <<"\r\n"
                 "Sec-WebSocket-Version: 13\r\n\r\n">>],
    ok = gen_tcp:send(S, Request),
    {ok, Response} = recv_until(S, <<"\r\n\r\n">>, <<>>),
    <<"HTTP/1.1 101", _/binary>> = Response,
    S.

recv_until(S, Needle, Acc) ->
    case binary:match(Acc, Needle) of
        nomatch ->
            case gen_tcp:recv(S, 0, 5000) of
                {ok, Data} -> recv_until(S, Needle, <<Acc/binary, Data/binary>>);
                Other -> Other
            end;
        _ ->
            {ok, Acc}
    end.

connack(S, ClientId) ->
    connack(S, ClientId, true).

connack(S, ClientId, Clean) ->
    #{<<"op">> := <<"connack">>} =
        request(S, #{<<"op">> => <<"connect">>, <<"client_id">> => ClientId,
                     <<"clean_start">> => Clean}),
    S.

suback(S, Filter, QoS) ->
    #{<<"op">> := <<"suback">>, <<"destination">> := Filter, <<"qos">> := QoS} =
        request(S, #{<<"op">> => <<"subscribe">>, <<"id">> => 1,
                     <<"destination">> => Filter, <<"qos">> => QoS}),
    ok.

publish(S, Dest, QoS, Payload) ->
    #{<<"op">> := <<"puback">>} =
        request(S, #{<<"op">> => <<"publish">>, <<"id">> => 1,
                     <<"destination">> => Dest, <<"qos">> => QoS,
                     <<"payload_b64">> => base64:encode(Payload)}),
    ok.

request(S, Map) ->
    ok = send_json(S, Map),
    recv_json(S).

send_json(S, Map) ->
    send_frame(S, 1, json:encode(Map)).

send_binary(S, Op, Header, Body) ->
    H = iolist_to_binary(json:encode(Header)),
    send_frame(S, 2, <<Op:8, (byte_size(H)):32, H/binary, Body/binary>>).

send_frame(S, Op, Payload) ->
    gen_tcp:send(S, ws_frame:encode(Op, Payload, true)).

recv_json(S) ->
    #{opcode := 1, payload := Payload} = recv_frame(S),
    json:decode(Payload).

recv_message(S) ->
    recv_frame(S).

message_header(#{opcode := 1, payload := Payload}) ->
    json:decode(Payload);
message_header(#{opcode := 2,
                 payload := <<3:8, HLen:32, H:HLen/binary, _Body/binary>>}) ->
    json:decode(H).

recv_frame(S) ->
    {ok, <<Fin:1, Rsv:3, Op:4, Mask:1, Len0:7>>} = gen_tcp:recv(S, 2, 5000),
    0 = Rsv,
    Len = case Len0 of
              126 -> {ok, <<L:16>>} = gen_tcp:recv(S, 2, 5000), L;
              127 -> {ok, <<L:64>>} = gen_tcp:recv(S, 8, 5000), L;
              _ -> Len0
          end,
    Key = case Mask of
              1 -> {ok, K} = gen_tcp:recv(S, 4, 5000), K;
              0 -> <<>>
          end,
    {ok, Payload0} = gen_tcp:recv(S, Len, 5000),
    Payload = case Mask of
                  1 -> unmask(Key, Payload0);
                  0 -> Payload0
              end,
    #{fin => Fin =:= 1, opcode => Op, payload => Payload}.

unmask(Key, Payload) ->
    Size = byte_size(Payload),
    Stream = binary:copy(Key, (Size div 4) + 1),
    crypto:exor(Payload, binary:part(Stream, 0, Size)).

close(S) ->
    catch gen_tcp:send(S, ws_frame:encode(8, ws_frame:close_payload(1000, <<>>), true)),
    catch gen_tcp:close(S),
    ok.

wait_until(Fun) -> wait_until(Fun, ?POLL_BUDGET_MS).

wait_until(Fun, 0) -> ?assert(Fun());
wait_until(Fun, Left) ->
    case Fun() of
        true -> ok;
        false -> timer:sleep(?POLL_MS), wait_until(Fun, Left - ?POLL_MS)
    end.
