%% @doc S5 AMQP 0-9-1 adapter over real sockets: handshake, channels,
%% exchanges/queues/bindings, publish/deliver/ack/nack, prefetch, channel
%% errors and frame-level robustness.
%%
%% The suite's client is intentionally byte-level: it speaks the documented
%% AMQP 0-9-1 subset directly. The Gate S5 evidence for a third-party client
%% comes from `test/protocol-compatibility/amqp/' (rabbitmq/amqp091-go).
-module(amqp_conn_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(N, 2).
-define(REF, {amqp, 0}).

-record(c, {s :: gen_tcp:socket(), buf = <<>> :: binary()}).

all() ->
    [handshake_and_channel_open,
     direct_publish_consume_ack,
     direct_routing_key_isolation,
     fanout_delivers_to_all,
     topic_star_and_hash,
     queue_unbind_stops_routing,
     queue_delete_then_passive_fails,
     passive_missing_exchange_is_channel_error,
     nack_requeue_redelivers,
     prefetch_limits_unacked,
     multi_channel_isolation,
     connection_close_gets_close_ok,
     heartbeat_frame_is_accepted,
     bad_frame_end_stops_connection].

suite() -> [{timetrap, {seconds, 90}}].

init_per_suite(Config) ->
    _ = application:load(broker_storage),
    _ = application:load(broker_router),
    _ = application:load(broker_queue),
    _ = application:load(broker_subscription),
    _ = application:load(broker_event),
    _ = application:load(broker_security),
    _ = application:load(broker_retain),
    _ = application:load(broker_proto_amqp),
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
    ok = application:set_env(broker_proto_amqp, listeners,
                             [#{port => 0, ref => ?REF,
                                num_acceptors => 4, max_connections => 128}]),
    ok = application:set_env(broker_proto_amqp, poll_ms, 25),
    {ok, _} = application:ensure_all_started(broker_storage),
    ok = create_partitions(),
    {ok, _} = application:ensure_all_started(broker_proto_amqp),
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
    _ = application:stop(broker_proto_amqp),
    _ = application:stop(broker_retain),
    _ = application:stop(broker_event),
    _ = application:stop(broker_security),
    _ = application:stop(broker_subscription),
    _ = application:stop(broker_queue),
    _ = application:stop(broker_router),
    _ = application:stop(broker_storage),
    ok.

%%%_* Cases -----------------------------------------------------------------

handshake_and_channel_open(Config) ->
    C0 = connect(Config),
    C1 = open_channel(C0, 1),
    {Ch, C2} = declare_queue(C1, 1, <<"ct.queue">>),
    ?assertEqual(<<"ct.queue">>, Ch),
    close(C2),
    ok.

direct_publish_consume_ack(Config) ->
    C0 = open_channel(connect(Config), 1),
    C1 = declare_exchange(C0, 1, <<"ct.direct">>, <<"direct">>),
    {_, C2} = declare_queue(C1, 1, <<"ct.direct.q">>),
    C3 = bind(C2, 1, <<"ct.direct.q">>, <<"ct.direct">>, <<"rk">>),
    {<<"ctag">>, C4} = consume(C3, 1, <<"ct.direct.q">>, <<"ctag">>, false),
    C5 = publish(C4, 1, <<"ct.direct">>, <<"rk">>, <<"hello-ct">>),
    {D, C6} = recv_deliver(C5),
    ?assertEqual(<<"hello-ct">>, maps:get(body, D)),
    ?assertEqual(<<"rk">>, maps:get(routing_key, D)),
    ?assertEqual(<<"ct.direct">>, maps:get(exchange, D)),
    C7 = ack(C6, 1, maps:get(delivery_tag, D)),
    ?assertEqual(timeout, recv_frame(C7, 600)),
    close(C7),
    ok.

direct_routing_key_isolation(Config) ->
    C0 = open_channel(connect(Config), 1),
    C1 = declare_exchange(C0, 1, <<"ct.rr">>, <<"direct">>),
    {_, C2} = declare_queue(C1, 1, <<"ct.rr.hit">>),
    {_, C3} = declare_queue(C2, 1, <<"ct.rr.miss">>),
    C4 = bind(C3, 1, <<"ct.rr.hit">>, <<"ct.rr">>, <<"yes">>),
    C5 = bind(C4, 1, <<"ct.rr.miss">>, <<"ct.rr">>, <<"no">>),
    {<<"hit-tag">>, C6} = consume(C5, 1, <<"ct.rr.hit">>, <<"hit-tag">>, false),
    {<<"miss-tag">>, C7} = consume(C6, 1, <<"ct.rr.miss">>, <<"miss-tag">>, false),
    C8 = publish(C7, 1, <<"ct.rr">>, <<"yes">>, <<"to-hit">>),
    {D, C9} = recv_deliver(C8),
    ?assertEqual(<<"to-hit">>, maps:get(body, D)),
    %% The miss queue never sees it.
    ?assertEqual(timeout, recv_frame(C9, 600)),
    close(C9),
    ok.

fanout_delivers_to_all(Config) ->
    C0 = open_channel(connect(Config), 1),
    C1 = declare_exchange(C0, 1, <<"ct.fanout">>, <<"fanout">>),
    {_, C2} = declare_queue(C1, 1, <<"ct.fanout.a">>),
    {_, C3} = declare_queue(C2, 1, <<"ct.fanout.b">>),
    C4 = bind(C3, 1, <<"ct.fanout.a">>, <<"ct.fanout">>, <<>>),
    C5 = bind(C4, 1, <<"ct.fanout.b">>, <<"ct.fanout">>, <<>>),
    {_, C6} = consume(C5, 1, <<"ct.fanout.a">>, <<"fan-a">>, false),
    {_, C7} = consume(C6, 1, <<"ct.fanout.b">>, <<"fan-b">>, false),
    C8 = publish(C7, 1, <<"ct.fanout">>, <<"ignored">>, <<"fan">>),
    {Da, C9} = recv_deliver(C8),
    {Db, C10} = recv_deliver(C9),
    ?assertEqual(<<"fan">>, maps:get(body, Da)),
    ?assertEqual(<<"fan">>, maps:get(body, Db)),
    close(C10),
    ok.

topic_star_and_hash(Config) ->
    C0 = open_channel(connect(Config), 1),
    C1 = declare_exchange(C0, 1, <<"ct.topic">>, <<"topic">>),
    {_, C2} = declare_queue(C1, 1, <<"ct.topic.star">>),
    {_, C3} = declare_queue(C2, 1, <<"ct.topic.hash">>),
    C4 = bind(C3, 1, <<"ct.topic.star">>, <<"ct.topic">>, <<"stock.*">>),
    C5 = bind(C4, 1, <<"ct.topic.hash">>, <<"ct.topic">>, <<"stock.#">>),
    {_, C6} = consume(C5, 1, <<"ct.topic.star">>, <<"s">>, false),
    {_, C7} = consume(C6, 1, <<"ct.topic.hash">>, <<"h">>, false),
    C8 = publish(C7, 1, <<"ct.topic">>, <<"stock.usd">>, <<"one">>),
    {Ds, C9} = recv_deliver(C8),
    {Dh, C10} = recv_deliver(C9),
    ?assertEqual(<<"one">>, maps:get(body, Ds)),
    ?assertEqual(<<"one">>, maps:get(body, Dh)),
    C11 = publish(C10, 1, <<"ct.topic">>, <<"stock.usd.nyse">>, <<"two">>),
    %% star matches exactly one word, so only hash receives this one.
    {Dh2, C12} = recv_deliver(C11),
    ?assertEqual(<<"two">>, maps:get(body, Dh2)),
    ?assertEqual(timeout, recv_frame(C12, 600)),
    close(C12),
    ok.

queue_unbind_stops_routing(Config) ->
    C0 = open_channel(connect(Config), 1),
    C1 = declare_exchange(C0, 1, <<"ct.unbind">>, <<"direct">>),
    {_, C2} = declare_queue(C1, 1, <<"ct.unbind.q">>),
    C3 = bind(C2, 1, <<"ct.unbind.q">>, <<"ct.unbind">>, <<"k">>),
    {_, C4} = consume(C3, 1, <<"ct.unbind.q">>, <<"u">>, false),
    C5 = publish(C4, 1, <<"ct.unbind">>, <<"k">>, <<"before">>),
    {D, C6} = recv_deliver(C5),
    ?assertEqual(<<"before">>, maps:get(body, D)),
    C7 = unbind(C6, 1, <<"ct.unbind.q">>, <<"ct.unbind">>, <<"k">>),
    C8 = publish(C7, 1, <<"ct.unbind">>, <<"k">>, <<"after">>),
    ?assertEqual(timeout, recv_frame(C8, 700)),
    close(C8),
    ok.

queue_delete_then_passive_fails(Config) ->
    C0 = open_channel(connect(Config), 1),
    {_, C1} = declare_queue(C0, 1, <<"ct.delete">>),
    C2 = queue_delete(C1, 1, <<"ct.delete">>),
    C3 = declare_queue_passive_expect_error(C2, 1, <<"ct.delete">>, 404),
    close(C3),
    ok.

passive_missing_exchange_is_channel_error(Config) ->
    C0 = open_channel(connect(Config), 1),
    Args = <<0:16, (amqp_codec:encode_shortstr(<<"ct.missing">>))/binary,
             (amqp_codec:encode_shortstr(<<"direct">>))/binary,
             (amqp_codec:encode_bits([true, false, false, false, false]))/binary,
             (amqp_codec:encode_table(#{}))/binary>>,
    C1 = send_method(C0, 1, 40, 10, Args),
    {1, 20, 40, CloseArgs, C2} = recv_method(C1),
    <<404:16, _/binary>> = CloseArgs,
    close(C2),
    ok.

nack_requeue_redelivers(Config) ->
    C0 = open_channel(connect(Config), 1),
    {_, C1} = declare_queue(C0, 1, <<"ct.nack">>),
    {_, C2} = consume(C1, 1, <<"ct.nack">>, <<"n">>, false),
    C3 = publish(C2, 1, <<>>, <<"ct.nack">>, <<"retry">>),
    {D1, C4} = recv_deliver(C3),
    ?assertEqual(<<"retry">>, maps:get(body, D1)),
    C5 = nack(C4, 1, maps:get(delivery_tag, D1), false, true),
    {D2, C6} = recv_deliver(C5),
    ?assertEqual(<<"retry">>, maps:get(body, D2)),
    C7 = ack(C6, 1, maps:get(delivery_tag, D2)),
    ?assertEqual(timeout, recv_frame(C7, 600)),
    close(C7),
    ok.

prefetch_limits_unacked(Config) ->
    C0 = open_channel(connect(Config), 1),
    {_, C1} = declare_queue(C0, 1, <<"ct.qos">>),
    C2 = qos(C1, 1, 1),
    {_, C3} = consume(C2, 1, <<"ct.qos">>, <<"q">>, false),
    C4 = publish(C3, 1, <<>>, <<"ct.qos">>, <<"q0">>),
    C5 = publish(C4, 1, <<>>, <<"ct.qos">>, <<"q1">>),
    {D1, C6} = recv_deliver(C5),
    %% prefetch=1: no second delivery until the first is acked.
    ?assertEqual(timeout, recv_frame(C6, 700)),
    C7 = ack(C6, 1, maps:get(delivery_tag, D1)),
    {D2, C8} = recv_deliver(C7),
    ?assertEqual(<<"q1">>, maps:get(body, D2)),
    close(C8),
    ok.

multi_channel_isolation(Config) ->
    C0 = open_channel(connect(Config), 1),
    C1 = open_channel(C0, 2),
    {_, C2} = declare_queue(C1, 1, <<"ct.mc.a">>),
    {_, C3} = declare_queue(C2, 2, <<"ct.mc.b">>),
    {_, C4} = consume(C3, 1, <<"ct.mc.a">>, <<"a">>, false),
    {_, C5} = consume(C4, 2, <<"ct.mc.b">>, <<"b">>, false),
    C6 = publish(C5, 1, <<>>, <<"ct.mc.a">>, <<"via-a">>),
    C7 = publish(C6, 2, <<>>, <<"ct.mc.b">>, <<"via-b">>),
    {Da, C8} = recv_deliver(C7),
    {Db, C9} = recv_deliver(C8),
    ?assertEqual(<<"via-a">>, maps:get(body, Da)),
    ?assertEqual(<<"via-b">>, maps:get(body, Db)),
    close(C9),
    ok.

connection_close_gets_close_ok(Config) ->
    C0 = connect(Config),
    Args = <<200:16, (amqp_codec:encode_shortstr(<<"bye">>))/binary, 0:16, 0:16>>,
    C1 = send_method(C0, 0, 10, 50, Args),
    {0, 10, 51, _, C2} = recv_method(C1),
    close(C2),
    ok.

heartbeat_frame_is_accepted(Config) ->
    C0 = open_channel(connect(Config), 1),
    %% A heartbeat is an empty type-8 frame; the connection must survive it.
    C1 = send_frame(C0, 8, 0, <<>>),
    {_, C2} = declare_queue(C1, 1, <<"ct.hb">>),
    close(C2),
    ok.

bad_frame_end_stops_connection(Config) ->
    C0 = connect(Config),
    %% A method frame whose terminator is not 0xCE.
    Bad = <<1:8, 0:16, 4:32, 0:16, 10:16, 16#FF:8>>,
    ok = gen_tcp:send(C0#c.s, Bad),
    ?assertEqual({error, closed}, gen_tcp:recv(C0#c.s, 0, 5000)),
    ok.

%%%_* Byte-level client -----------------------------------------------------

connect(Config) ->
    Port = ?config(port, Config),
    {ok, S} = gen_tcp:connect({127, 0, 0, 1}, Port,
                              [binary, {active, false}, {nodelay, true}]),
    C0 = #c{s = S},
    ok = gen_tcp:send(S, <<"AMQP", 0, 0, 9, 1>>),
    {0, 10, 10, _Start, C1} = recv_method(C0),
    StartOk = <<(amqp_codec:encode_table(#{<<"product">> => <<"ct">>}))/binary,
                (amqp_codec:encode_shortstr(<<"PLAIN">>))/binary,
                (amqp_codec:encode_longstr(<<0, "guest", 0, "guest">>))/binary,
                (amqp_codec:encode_shortstr(<<"en_US">>))/binary>>,
    C2 = send_method(C1, 0, 10, 11, StartOk),
    {0, 10, 30, _Tune, C3} = recv_method(C2),
    C4 = send_method(C3, 0, 10, 31, <<2047:16, 131072:32, 0:16>>),
    Open = <<(amqp_codec:encode_shortstr(<<"/">>))/binary,
             (amqp_codec:encode_shortstr(<<>>))/binary, 0:8>>,
    C5 = send_method(C4, 0, 10, 40, Open),
    {0, 10, 41, _, C6} = recv_method(C5),
    C6.

open_channel(C, Ch) ->
    C1 = send_method(C, Ch, 20, 10, amqp_codec:encode_shortstr(<<>>)),
    {Ch, 20, 11, _, C2} = recv_method(C1),
    C2.

declare_exchange(C, Ch, Name, Type) ->
    Args = <<0:16, (amqp_codec:encode_shortstr(Name))/binary,
             (amqp_codec:encode_shortstr(Type))/binary,
             (amqp_codec:encode_bits([false, false, false, false, false]))/binary,
             (amqp_codec:encode_table(#{}))/binary>>,
    C1 = send_method(C, Ch, 40, 10, Args),
    {Ch, 40, 11, _, C2} = recv_method(C1),
    C2.

declare_queue(C, Ch, Name) ->
    Args = <<0:16, (amqp_codec:encode_shortstr(Name))/binary,
             (amqp_codec:encode_bits([false, false, false, false, false]))/binary,
             (amqp_codec:encode_table(#{}))/binary>>,
    C1 = send_method(C, Ch, 50, 10, Args),
    {Ch, 50, 11, Reply, C2} = recv_method(C1),
    {Queue, _} = amqp_codec:get_shortstr(Reply),
    {Queue, C2}.

declare_queue_passive_expect_error(C, Ch, Name, Code) ->
    Args = <<0:16, (amqp_codec:encode_shortstr(Name))/binary,
             (amqp_codec:encode_bits([true, false, false, false, false]))/binary,
             (amqp_codec:encode_table(#{}))/binary>>,
    C1 = send_method(C, Ch, 50, 10, Args),
    {Ch, 20, 40, CloseArgs, C2} = recv_method(C1),
    <<Code:16, _/binary>> = CloseArgs,
    C2.

bind(C, Ch, Queue, Exchange, RK) ->
    Args = <<0:16, (amqp_codec:encode_shortstr(Queue))/binary,
             (amqp_codec:encode_shortstr(Exchange))/binary,
             (amqp_codec:encode_shortstr(RK))/binary,
             (amqp_codec:encode_bits([false]))/binary,
             (amqp_codec:encode_table(#{}))/binary>>,
    C1 = send_method(C, Ch, 50, 20, Args),
    {Ch, 50, 21, _, C2} = recv_method(C1),
    C2.

unbind(C, Ch, Queue, Exchange, RK) ->
    Args = <<0:16, (amqp_codec:encode_shortstr(Queue))/binary,
             (amqp_codec:encode_shortstr(Exchange))/binary,
             (amqp_codec:encode_shortstr(RK))/binary,
             (amqp_codec:encode_table(#{}))/binary>>,
    C1 = send_method(C, Ch, 50, 50, Args),
    {Ch, 50, 51, _, C2} = recv_method(C1),
    C2.

queue_delete(C, Ch, Queue) ->
    Args = <<0:16, (amqp_codec:encode_shortstr(Queue))/binary,
             (amqp_codec:encode_bits([false, false, false]))/binary>>,
    C1 = send_method(C, Ch, 50, 40, Args),
    {Ch, 50, 41, _, C2} = recv_method(C1),
    C2.

consume(C, Ch, Queue, Tag, NoAck) ->
    Args = <<0:16, (amqp_codec:encode_shortstr(Queue))/binary,
             (amqp_codec:encode_shortstr(Tag))/binary,
             (amqp_codec:encode_bits([false, NoAck, false, false]))/binary,
             (amqp_codec:encode_table(#{}))/binary>>,
    C1 = send_method(C, Ch, 60, 20, Args),
    {Ch, 60, 21, Reply, C2} = recv_method(C1),
    {Tag2, _} = amqp_codec:get_shortstr(Reply),
    {Tag2, C2}.

qos(C, Ch, Prefetch) ->
    C1 = send_method(C, Ch, 60, 10, <<0:32, Prefetch:16, 0:8>>),
    {Ch, 60, 11, _, C2} = recv_method(C1),
    C2.

publish(C, Ch, Exchange, RK, Body) ->
    Args = <<0:16, (amqp_codec:encode_shortstr(Exchange))/binary,
             (amqp_codec:encode_shortstr(RK))/binary,
             (amqp_codec:encode_bits([false, false]))/binary>>,
    C1 = send_method(C, Ch, 60, 40, Args),
    Header = amqp_codec:encode_content_header(byte_size(Body)),
    C2 = send_frame(C1, 2, Ch, Header),
    send_frame(C2, 3, Ch, Body).

ack(C, Ch, Tag) ->
    send_method(C, Ch, 60, 80, <<Tag:64, (amqp_codec:encode_bits([false]))/binary>>).

nack(C, Ch, Tag, Multiple, Requeue) ->
    send_method(C, Ch, 60, 120,
                <<Tag:64, (amqp_codec:encode_bits([Multiple, Requeue]))/binary>>).

recv_deliver(C) ->
    {Ch, 60, 60, Args, C1} = recv_method(C),
    {CTag, R1} = amqp_codec:get_shortstr(Args),
    <<DTag:64, Redelivered:8, R2/binary>> = R1,
    {Exchange, R3} = amqp_codec:get_shortstr(R2),
    {RK, _} = amqp_codec:get_shortstr(R3),
    {2, Ch, Header, C2} = recv_frame(C1),
    {60, _Weight, BodySize, _Props} = amqp_codec:decode_content_header(Header),
    {Body, C3} = recv_body(C2, Ch, BodySize),
    {#{consumer_tag => CTag, delivery_tag => DTag,
       redelivered => Redelivered =/= 0,
       exchange => Exchange, routing_key => RK, body => Body}, C3}.

recv_body(C, _Ch, 0) ->
    {<<>>, C};
recv_body(C, Ch, Size) ->
    recv_body(C, Ch, Size, <<>>).

recv_body(C, _Ch, Size, Acc) when byte_size(Acc) >= Size ->
    {Acc, C};
recv_body(C, Ch, Size, Acc) ->
    {3, Ch, Payload, C1} = recv_frame(C),
    recv_body(C1, Ch, Size, <<Acc/binary, Payload/binary>>).

send_method(C, Ch, Class, Method, Args) ->
    send_frame(C, 1, Ch, amqp_codec:encode_method(Class, Method, Args)).

send_frame(#c{s = S} = C, Type, Channel, Payload) ->
    ok = gen_tcp:send(S, amqp_codec:encode_frame(Type, Channel, Payload)),
    C.

recv_method(C) ->
    {1, Ch, Payload, C1} = recv_frame(C),
    {Class, Method, Args} = amqp_codec:decode_method(Payload),
    {Ch, Class, Method, Args, C1}.

recv_frame(C) ->
    recv_frame(C, 5000).

recv_frame(C, Timeout) ->
    case recv_exact(C, 7, Timeout) of
        timeout ->
            timeout;
        {<<Type:8, Channel:16, Size:32>>, C1} ->
            {Payload, C2} = recv_exact(C1, Size, Timeout),
            {<<16#CE>>, C3} = recv_exact(C2, 1, Timeout),
            {Type, Channel, Payload, C3}
    end.

recv_exact(C, N, Timeout) ->
    #c{s = S, buf = Buf} = C,
    case byte_size(Buf) >= N of
        true ->
            <<Part:N/binary, Rest/binary>> = Buf,
            {Part, C#c{buf = Rest}};
        false ->
            case gen_tcp:recv(S, 0, Timeout) of
                {ok, Data} ->
                    recv_exact(C#c{buf = <<Buf/binary, Data/binary>>}, N, Timeout);
                {error, timeout} ->
                    timeout;
                {error, closed} ->
                    ct:fail(socket_closed);
                {error, Reason} ->
                    ct:fail({recv, Reason})
            end
    end.

close(#c{s = S}) ->
    _ = gen_tcp:close(S),
    ok.
