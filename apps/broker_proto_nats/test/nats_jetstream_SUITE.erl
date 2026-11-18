%% @doc JetStream MVP wire-level tests (NATS-JS-001..003 minimal slice).
%%
%% The client is intentionally byte-level (same posture as nats_conn_SUITE):
%% raw PUB/SUB/MSG frames against a real broker node. Third-party evidence
%% lives in test/protocol-compatibility/nats (nats.go, real JetStream API).
-module(nats_jetstream_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(N, 2).
-define(REF, {nats, 0}).

-record(c, {s :: gen_tcp:socket(), buf = <<>> :: binary(),
            %% JetStream deliveries that raced a PONG: recv_msg/1 drains these
            %% before touching the socket.
            msgs = [] :: [tuple()]}).

all() ->
    [core_pubsub_sanity,
     stream_create_info_delete,
     managed_publish_gets_puback_with_monotonic_seq,
     pull_consumer_fetch_ack,
     pull_consumer_nak_redelivers,
     filter_subject_skips_other_subjects,
     deliver_policy_new_skips_history,
     stream_delete_drops_consumers,
     metadata_survives_a_restart].

suite() -> [{timetrap, {seconds, 120}}].

init_per_suite(Config) ->
    _ = application:load(broker_storage),
    _ = application:load(broker_router),
    _ = application:load(broker_queue),
    _ = application:load(broker_subscription),
    _ = application:load(broker_event),
    _ = application:load(broker_security),
    _ = application:load(broker_retain),
    _ = application:load(broker_proto_nats),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(Case, Config0) ->
    stop_all(),
    DataDir = filename:join([?config(priv_dir, Config0), atom_to_list(Case), "data"]),
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
    ok = application:set_env(broker_security, require_auth, []),
    ok = application:set_env(broker_proto_nats, listeners,
                             [#{port => 0, ref => ?REF,
                                num_acceptors => 4, max_connections => 128}]),
    ok = application:set_env(broker_proto_nats, js_data_dir, DataDir),
    {ok, _} = application:ensure_all_started(broker_storage),
    ok = create_partitions(),
    {ok, _} = application:ensure_all_started(broker_proto_nats),
    [{data_dir, DataDir}, {port, listener_port()} | Config0].

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
    %% The JetStream child sits before the listeners in the sup, so match the
    %% ranch listener ref directly rather than child order.
    try ranch:get_port({nats, 0}) of
        Port when is_integer(Port) -> Port
    catch
        _:_ -> exit(no_listener)
    end.

stop_all() ->
    _ = application:stop(broker_proto_nats),
    _ = application:stop(broker_retain),
    _ = application:stop(broker_event),
    _ = application:stop(broker_security),
    _ = application:stop(broker_subscription),
    _ = application:stop(broker_queue),
    _ = application:stop(broker_router),
    _ = application:stop(broker_storage),
    ok.

%%%_* Cases ------------------------------------------------------------------

%% Sanity: core NATS pub/sub works in this fixture before the JS flow.
core_pubsub_sanity(Config) ->
    C0 = open(Config),
    C1 = sub(C0, <<"sanity.sub">>, undefined, <<"1">>),
    C2 = pub(C1, <<"sanity.sub">>, undefined, <<"ping">>),
    {_Subject, _Sid, undefined, <<"ping">>, C3} = recv_msg(C2),
    close(C3),
    ok.

stream_create_info_delete(Config) ->
    C0 = open(Config),
    {R1, C1} = js_api(C0, <<"$JS.API.STREAM.CREATE.orders">>,
                      #{<<"name">> => <<"orders">>,
                        <<"subjects">> => [<<"orders.>">>]}),
    ?assertMatch(#{<<"config">> := #{<<"name">> := <<"orders">>}}, R1),
    {R2, C2} = js_api(C1, <<"$JS.API.STREAM.INFO.orders">>, <<>>),
    ?assertMatch(#{<<"state">> := #{<<"last_seq">> := 0}}, R2),
    {R3, C3} = js_api(C2, <<"$JS.API.STREAM.NAMES">>, <<>>),
    ?assertMatch(#{<<"streams">> := [<<"orders">>]}, R3),
    {R4, C4} = js_api(C3, <<"$JS.API.STREAM.DELETE.orders">>, <<>>),
    ?assertMatch(#{<<"success">> := true}, R4),
    {R5, C5} = js_api(C4, <<"$JS.API.STREAM.INFO.orders">>, <<>>),
    ?assertMatch(#{<<"error">> := #{<<"code">> := 404}}, R5),
    close(C5),
    ok.

managed_publish_gets_puback_with_monotonic_seq(Config) ->
    C0 = open(Config),
    {_, C1} = js_api(C0, <<"$JS.API.STREAM.CREATE.orders">>,
                     #{<<"name">> => <<"orders">>,
                       <<"subjects">> => [<<"orders.>">>]}),
    %% Publish WITH a reply inbox: the PubAck arrives as a MSG on it.
    {Ack1, C2} = js_api_pub(C1, <<"orders.new">>, <<"p1">>),
    ?assertMatch(#{<<"stream">> := <<"orders">>, <<"seq">> := 1}, Ack1),
    {Ack2, C3} = js_api_pub(C2, <<"orders.new">>, <<"p2">>),
    ?assertMatch(#{<<"stream">> := <<"orders">>, <<"seq">> := 2}, Ack2),
    {Info, C4} = js_api(C3, <<"$JS.API.STREAM.INFO.orders">>, <<>>),
    ?assertMatch(#{<<"state">> := #{<<"messages">> := 2, <<"last_seq">> := 2}},
                 Info),
    close(C4),
    ok.

pull_consumer_fetch_ack(Config) ->
    C0 = open(Config),
    {_, C1} = js_api(C0, <<"$JS.API.STREAM.CREATE.orders">>,
                     #{<<"name">> => <<"orders">>,
                       <<"subjects">> => [<<"orders.>">>]}),
    {AckA, C2} = js_api_pub(C1, <<"orders.a">>, <<"first">>),
    ?assertMatch(#{<<"seq">> := 1}, AckA),
    {AckB, C3} = js_api_pub(C2, <<"orders.b">>, <<"second">>),
    ?assertMatch(#{<<"seq">> := 2}, AckB),
    %% Durable pull consumer; nats.go sends the config wrapped under "config".
    {Cr, C4} = js_api(C3, <<"$JS.API.CONSUMER.CREATE.orders.durable-1">>,
                      #{<<"stream_name">> => <<"orders">>,
                        <<"config">> =>
                            #{<<"durable_name">> => <<"durable-1">>,
                              <<"deliver_policy">> => <<"all">>,
                              <<"ack_policy">> => <<"explicit">>}}),
    ?assertMatch(#{<<"name">> := <<"durable-1">>}, Cr),
    %% Pull: subscribe a fresh wildcard inbox, then publish the MSG.NEXT
    %% request with a token under it; deliveries arrive on that inbox.
    {_Prefix, C5} = js_fetch(C4, <<"$JS.API.CONSUMER.MSG.NEXT.orders.durable-1">>,
                             #{<<"batch">> => 10, <<"expires">> => 2000000000}),
    {_SubjectA, _SidA, AckReply1, <<"first">>, C7} = recv_msg(C5),
    ?assertMatch(<<"$JS.ACK.orders.durable-1.", _/binary>>, AckReply1),
    {_SubjectB, _SidB, AckReply2, <<"second">>, C7} = recv_msg(C5),
    ?assertMatch(<<"$JS.ACK.orders.durable-1.", _/binary>>, AckReply2),
    %% Ack both: +ACK goes to the delivery's reply subject.
    C9 = pub(C7, AckReply1, undefined, <<"+ACK">>),
    C10 = pub(C9, AckReply2, undefined, <<"+ACK">>),
    timer:sleep(150),
    %% Everything acked: the next pull answers nothing before its deadline,
    %% which for this case means an empty window.
    {_Prefix2, C11} = js_fetch(C10, <<"$JS.API.CONSUMER.MSG.NEXT.orders.durable-1">>,
                               #{<<"batch">> => 10, <<"no_wait">> => true}),
    {error, timeout} = recv_timeout(C11, 700),
    close(C11),
    ok.

pull_consumer_nak_redelivers(Config) ->
    C0 = open(Config),
    {_, C1} = js_api(C0, <<"$JS.API.STREAM.CREATE.orders">>,
                     #{<<"name">> => <<"orders">>,
                       <<"subjects">> => [<<"orders.>">>]}),
    {_, C2} = js_api_pub(C1, <<"orders.a">>, <<"redo-me">>),
    {_, C3} = js_api(C2, <<"$JS.API.CONSUMER.CREATE.orders.durable-nak">>,
                     #{<<"stream_name">> => <<"orders">>,
                       <<"config">> =>
                           #{<<"durable_name">> => <<"durable-nak">>,
                             <<"deliver_policy">> => <<"all">>,
                             <<"ack_policy">> => <<"explicit">>,
                             <<"ack_wait">> => 1000000000}}),
    {_Prefix, C4} = js_fetch(C3, <<"$JS.API.CONSUMER.MSG.NEXT.orders.durable-nak">>,
                             #{<<"batch">> => 1, <<"expires">> => 2000000000}),
    {_SubjectN, _SidN, AckReply, <<"redo-me">>, C6} = recv_msg(C4),
    %% NAK: the offset goes back to the pool.
    C7 = pub(C6, AckReply, undefined, <<"-NAK">>),
    timer:sleep(150),
    {_Prefix2, C9} = js_fetch(C7, <<"$JS.API.CONSUMER.MSG.NEXT.orders.durable-nak">>,
                              #{<<"batch">> => 1, <<"no_wait">> => true}),
    {_Subject2, _Sid2, _AckReply2, <<"redo-me">>, C10} = recv_msg(C9),
    close(C10),
    ok.

filter_subject_skips_other_subjects(Config) ->
    C0 = open(Config),
    {_, C1} = js_api(C0, <<"$JS.API.STREAM.CREATE.orders">>,
                     #{<<"name">> => <<"orders">>,
                       <<"subjects">> => [<<"orders.>">>]}),
    {_, C2} = js_api_pub(C1, <<"orders.eu">>, <<"eu-1">>),
    {_, C3} = js_api_pub(C2, <<"orders.us">>, <<"us-1">>),
    {_, C4} = js_api(C3, <<"$JS.API.CONSUMER.CREATE.orders.eu-only">>,
                     #{<<"stream_name">> => <<"orders">>,
                       <<"config">> =>
                           #{<<"durable_name">> => <<"eu-only">>,
                             <<"deliver_policy">> => <<"all">>,
                             <<"ack_policy">> => <<"explicit">>,
                             <<"filter_subject">> => <<"orders.eu">>}}),
    {_Prefix, C5} = js_fetch(C4, <<"$JS.API.CONSUMER.MSG.NEXT.orders.eu-only">>,
                             #{<<"batch">> => 10, <<"no_wait">> => true}),
    {_Subject, _Sid, _AckReply, <<"eu-1">>, C7} = recv_msg(C5),
    {error, timeout} = recv_timeout(C7, 700),
    close(C7),
    ok.

deliver_policy_new_skips_history(Config) ->
    C0 = open(Config),
    {_, C1} = js_api(C0, <<"$JS.API.STREAM.CREATE.orders">>,
                     #{<<"name">> => <<"orders">>,
                       <<"subjects">> => [<<"orders.>">>]}),
    {_, C2} = js_api_pub(C1, <<"orders.a">>, <<"history">>),
    {_, C3} = js_api(C2, <<"$JS.API.CONSUMER.CREATE.orders.new-only">>,
                     #{<<"stream_name">> => <<"orders">>,
                       <<"config">> =>
                           #{<<"durable_name">> => <<"new-only">>,
                             <<"deliver_policy">> => <<"new">>,
                             <<"ack_policy">> => <<"explicit">>}}),
    {_Prefix, C4} = js_fetch(C3, <<"$JS.API.CONSUMER.MSG.NEXT.orders.new-only">>,
                             #{<<"batch">> => 10, <<"no_wait">> => true}),
    {error, timeout} = recv_timeout(C4, 700),
    %% A live publish after the consumer exists IS delivered to a waiting pull.
    {_Prefix2, C6} = js_fetch(C4, <<"$JS.API.CONSUMER.MSG.NEXT.orders.new-only">>,
                              #{<<"batch">> => 10, <<"expires">> => 2000000000}),
    {_Pub, C7} = js_api_pub(C6, <<"orders.b">>, <<"fresh">>),
    {_Subject, _Sid, _AckReply, <<"fresh">>, C8} = recv_msg(C7),
    close(C8),
    ok.

stream_delete_drops_consumers(Config) ->
    C0 = open(Config),
    {_, C1} = js_api(C0, <<"$JS.API.STREAM.CREATE.orders">>,
                     #{<<"name">> => <<"orders">>,
                       <<"subjects">> => [<<"orders.>">>]}),
    {_, C2} = js_api(C1, <<"$JS.API.CONSUMER.CREATE.orders.d1">>,
                     #{<<"stream_name">> => <<"orders">>,
                       <<"config">> => #{<<"durable_name">> => <<"d1">>}}),
    {R3, C3} = js_api(C2, <<"$JS.API.STREAM.DELETE.orders">>, <<>>),
    ?assertMatch(#{<<"success">> := true}, R3),
    {R4, C4} = js_api(C3, <<"$JS.API.CONSUMER.INFO.orders.d1">>, <<>>),
    ?assertMatch(#{<<"error">> := #{<<"code">> := 404}}, R4),
    close(C4),
    ok.

%% Stream + consumer configs ride `js_data_dir' term files; a full app
%% restart restores them and the consumer's cursor keeps its place.
metadata_survives_a_restart(Config) ->
    C0 = open(Config),
    {_, C1} = js_api(C0, <<"$JS.API.STREAM.CREATE.orders">>,
                     #{<<"name">> => <<"orders">>,
                       <<"subjects">> => [<<"orders.>">>]}),
    {Ack1, C2} = js_api_pub(C1, <<"orders.a">>, <<"kept-1">>),
    ?assertMatch(#{<<"seq">> := 1}, Ack1),
    {Ack2, C3} = js_api_pub(C2, <<"orders.a">>, <<"kept-2">>),
    ?assertMatch(#{<<"seq">> := 2}, Ack2),
    {_, C4} = js_api(C3, <<"$JS.API.CONSUMER.CREATE.orders.durable-r">>,
                     #{<<"stream_name">> => <<"orders">>,
                       <<"config">> =>
                           #{<<"durable_name">> => <<"durable-r">>,
                             <<"deliver_policy">> => <<"all">>,
                             <<"ack_policy">> => <<"explicit">>}}),
    {_Prefix, C5} = js_fetch(C4, <<"$JS.API.CONSUMER.MSG.NEXT.orders.durable-r">>,
                             #{<<"batch">> => 1, <<"no_wait">> => true}),
    {_Subject, _Sid, AckReply, <<"kept-1">>, C6} = recv_msg(C5),
    C8 = pub(C6, AckReply, undefined, <<"+ACK">>),
    close(C8),
    %% The ack is processed asynchronously by the JS process; give it time to
    %% land before the shutdown tears the apps down (stop order kills
    %% broker_proto_nats before broker_subscription flushes).
    timer:sleep(300),
    %% Full app restart (same data dirs): stream, consumer and cursor return.
    stop_all(),
    {ok, _} = application:ensure_all_started(broker_storage),
    ok = create_partitions(),
    {ok, _} = application:ensure_all_started(broker_proto_nats),
    C9 = open(Config, listener_port()),
    {Info, C10} = js_api(C9, <<"$JS.API.STREAM.INFO.orders">>, <<>>),
    ?assertMatch(#{<<"state">> := #{<<"last_seq">> := 2}}, Info),
    {_Prefix2, C11} = js_fetch(C10, <<"$JS.API.CONSUMER.MSG.NEXT.orders.durable-r">>,
                               #{<<"batch">> => 10, <<"no_wait">> => true}),
    {_S3, _Sid3, _Ack3, <<"kept-2">>, _C13} = recv_msg(C11),
    {error, timeout} = recv_timeout(C11, 500),
    close(C11),
    ok.

%%%_* JS API helpers ----------------------------------------------------------

%% SUB an inbox, then PUB the API request with that inbox as the reply
%% subject; the reply arrives as a MSG whose payload is the JSON.
%% SUB a fresh inbox, PUB the API request with it as the reply subject; the
%% reply arrives as one MSG whose payload is the JSON. Returns
%% {ReplyMap, Conn}. NOTE: `sub/4' and `pub/4' both end in a PING/PONG, whose
%% {<<"PONG">>, Conn} pair must be destructured at every step.
%% Subscribe the wildcard inbox prefix once (nats.go does the same:
%% `_INBOX.<nuid>.*`), then publish each request to a unique token under it.
js_api(C0, Subject, Body0) ->
    Body = case is_map(Body0) of
               true -> iolist_to_binary(json:encode(Body0));
               false -> Body0
           end,
    {Prefix, C1} = js_inbox_sub(C0, <<"api">>),
    Reply = <<Prefix/binary, $., (integer_to_binary(erlang:unique_integer([positive])))/binary>>,
    C2 = pub(C1, Subject, Reply, Body),
    {_S, _Sid, _AckReply, Payload, C3} = recv_msg_on(<<Prefix/binary, ".">>, C2),
    {json:decode(Payload), C3}.

js_api_pub(C0, Subject, Payload0) ->
    Body = case is_map(Payload0) of
               true -> iolist_to_binary(json:encode(Payload0));
               false -> Payload0
           end,
    {Prefix, C1} = js_inbox_sub(C0, <<"pub">>),
    Reply = <<Prefix/binary, $., (integer_to_binary(erlang:unique_integer([positive])))/binary>>,
    C2 = pub(C1, Subject, Reply, Body),
    {_S, _Sid, _AckReply, PubAckJson, C3} = recv_msg_on(<<Prefix/binary, ".">>, C2),
    {json:decode(PubAckJson), C3}.

%% Next MSG on the given subject prefix (the request's reply inbox). A fetch
%% delivery that raced this request's PONG sits in the stash under a pull
%% inbox; skipped messages go back on the stash in arrival order so later
%% recv_msg calls still see them.
recv_msg_on(Prefix, C) ->
    recv_msg_on(Prefix, C, []).

%% SkippedRev accumulates in reverse arrival order.
recv_msg_on(Prefix, C = #c{msgs = Ms}, SkippedRev) ->
    case Ms of
        [{S, _, _, _, _} = M | Rest] ->
            case is_js_prefix(Prefix, S) of
                true ->
                    {S, Sid, R, P, _CAtPush} = M,
                    {S, Sid, R, P, C#c{msgs = lists:reverse(SkippedRev) ++ Rest}};
                false ->
                    recv_msg_on(Prefix, C#c{msgs = Rest}, [M | SkippedRev])
            end;
        [] ->
            {Line, C1} = recv_line(C),
            case Line of
                <<"MSG ", _/binary>> ->
                    {S, Sid, R, P, C2} = recv_msg_parse(C1, Line),
                    case is_js_prefix(Prefix, S) of
                        true ->
                            {S, Sid, R, P, C2#c{msgs = lists:reverse(SkippedRev)}};
                        false ->
                            recv_msg_on(Prefix, C2,
                                        [{S, Sid, R, P, C2} | SkippedRev])
                    end;
                _ ->
                    recv_msg_on(Prefix, C1, SkippedRev)
            end
    end.

is_js_prefix(Prefix, Subject) ->
    Size = byte_size(Prefix),
    case Subject of
        <<Prefix:Size/binary, _/binary>> -> true;
        _ -> false
    end.

%% MSG.NEXT pull: subscribe a fresh wildcard inbox, publish the request with
%% a token under it, and hand back {Prefix, Conn}. Deliveries arrive as MSGs
%% on the inbox (racing the PONG -- ping_recv stashes them), so the caller
%% reads them with recv_msg/1.
js_fetch(C, Subject, ReqBody) ->
    Prefix = <<"_INBOX.jspull.",
               (integer_to_binary(erlang:unique_integer([positive])))/binary>>,
    C1 = sub(C, <<Prefix/binary, ".*">>, undefined, js_sid()),
    Reply = <<Prefix/binary, $., (integer_to_binary(erlang:unique_integer([positive])))/binary>>,
    Body = case is_map(ReqBody) of
               true -> iolist_to_binary(json:encode(ReqBody));
               false -> ReqBody
           end,
    C2 = pub(C1, Subject, Reply, Body),
    {Prefix, C2}.

js_inbox_sub(C, Tag) ->
    Prefix = <<"_INBOX.js", Tag/binary, ".",
               (integer_to_binary(erlang:unique_integer([positive])))/binary>>,
    C1 = sub(C, <<Prefix/binary, ".*">>, undefined, js_sid()),
    {Prefix, C1}.

js_sid() ->
    integer_to_binary(erlang:unique_integer([positive]) rem 100000 + 1000).

%%%_* Client (copied from nats_conn_SUITE's byte-level client) ----------------

open(Config) ->
    open(Config, ?config(port, Config)).

open(_Config, Port) ->
    C0 = raw_open(Port),
    C1 = connect_raw(C0, #{}),
    {<<"PONG">>, C2} = ping(C1),
    C2.

raw_open(Port) when is_integer(Port) ->
    raw_open_port(Port);
raw_open(Config) ->
    raw_open_port(?config(port, Config)).

raw_open_port(Port) ->
    {ok, S} = gen_tcp:connect({127, 0, 0, 1}, Port,
                              [binary, {active, false}, {nodelay, true}]),
    C = #c{s = S},
    {<<"INFO ">>, _InfoJson, C1} = recv_prefixed(C, <<"INFO ">>),
    C1.

connect_raw(C, Opts) ->
    Map = maps:merge(#{<<"verbose">> => false, <<"pedantic">> => false,
                       <<"echo">> => true, <<"protocol">> => 1}, Opts),
    send_raw(C, [<<"CONNECT ">>, json:encode(Map), <<"\r\n">>]).

%% JetStream deliveries race the PONG (the reply is written by the
%% nats_jetstream process while this conn answers the PING), so a raced MSG is
%% parsed and STASHED on the record; recv_msg/1 drains the stash first, which
%% keeps the delivery without breaking the ping.
sub(C, Subject, Queue, Sid) ->
    Line = case Queue of
               undefined -> [<<"SUB ">>, Subject, <<" ">>, Sid, <<"\r\n">>];
               _ -> [<<"SUB ">>, Subject, <<" ">>, Queue, <<" ">>, Sid, <<"\r\n">>]
           end,
    C1 = send_raw(C, Line),
    %% NATS servers do not reply to SUB: flush with a PING so the caller
    %% knows the subscription is active (same trick as nats_conn_SUITE).
    {<<"PONG">>, C2} = ping(C1),
    C2.

pub(C, Subject, Reply, Payload) ->
    Size = integer_to_binary(byte_size(Payload)),
    Line = case Reply of
               undefined -> [<<"PUB ">>, Subject, <<" ">>, Size, <<"\r\n">>];
               _ -> [<<"PUB ">>, Subject, <<" ">>, Reply, <<" ">>, Size, <<"\r\n">>]
           end,
    C1 = send_raw(C, [Line, Payload, <<"\r\n">>]),
    %% The PING is what the server answers; ping_recv stashes any delivery
    %% that raced the PONG on the record instead of failing the match.
    C2 = send_raw(C1, <<"PING\r\n">>),
    {<<"PONG">>, C3} = ping_recv(C2),
    C3.

ping(C) ->
    C1 = send_raw(C, <<"PING\r\n">>),
    ping_recv(C1).

ping_recv(C) ->
    {Line, C1} = recv_line(C),
    case Line of
        <<"PONG">> ->
            {<<"PONG">>, C1};
        <<"MSG ", _/binary>> ->
            {Subject, Sid, Reply, Payload, C2} = recv_msg_parse(C1, Line),
            ping_recv(push_msg(C2, {Subject, Sid, Reply, Payload, C2}));
        _ ->
            ping_recv(C1)
    end.

push_msg(#c{msgs = Ms} = C, M) -> C#c{msgs = [M | Ms]}.

recv_msg(C = #c{msgs = [{S, Sid, R, P, _CAtPush} | Rest]}) ->
    %% Stashed delivery: its payload bytes are already off the socket, so the
    %% conn handed back is the current one minus this stash entry.
    {S, Sid, R, P, C#c{msgs = Rest}};
recv_msg(C) ->
    {Line, C1} = recv_line(C),
    case Line of
        <<"MSG ", _/binary>> -> recv_msg_parse(C1, Line);
        _ -> recv_msg(C)
    end.
recv_msg_parse(C, Line) ->
    case binary:split(Line, <<" ">>, [global]) of
        [<<"MSG">>, Subject, Sid, SizeBin] ->
            {Payload, C2} = recv_payload(C, binary_to_integer(SizeBin)),
            {Subject, Sid, undefined, Payload, C2};
        [<<"MSG">>, Subject, Sid, Reply, SizeBin] ->
            {Payload, C2} = recv_payload(C, binary_to_integer(SizeBin)),
            {Subject, Sid, Reply, Payload, C2};
        _ ->
            ct:fail({unexpected_line, Line})
    end.

recv_payload(C, Size) ->
    {Buf, C1} = recv_exact(C, Size + 2),
    <<Payload:Size/binary, "\r\n">> = Buf,
    {Payload, C1}.

close(#c{s = S}) ->
    gen_tcp:close(S).

send_raw(#c{s = S} = C, IoData) ->
    ok = gen_tcp:send(S, IoData),
    C.

recv_line(C) ->
    {Line, C1} = recv_until(C, <<"\r\n">>),
    {Line, C1}.

recv_prefixed(C, Prefix) ->
    {Line, C1} = recv_line(C),
    case Line of
        <<Prefix:(byte_size(Prefix))/binary, Rest/binary>> -> {Prefix, Rest, C1};
        _ -> ct:fail({unexpected_line, Line})
    end.

recv_until(#c{buf = Buf} = C, Sep) ->
    case binary:split(Buf, Sep) of
        [Line, Rest] ->
            {Line, C#c{buf = Rest}};
        [_] ->
            {ok, Data} = gen_tcp:recv(C#c.s, 0, 5000),
            recv_until(C#c{buf = <<Buf/binary, Data/binary>>}, Sep)
    end.

recv_exact(#c{buf = Buf} = C, N) when byte_size(Buf) >= N ->
    <<Part:N/binary, Rest/binary>> = Buf,
    {Part, C#c{buf = Rest}};
recv_exact(#c{buf = Buf} = C, N) ->
    {ok, Data} = gen_tcp:recv(C#c.s, 0, 5000),
    recv_exact(C#c{buf = <<Buf/binary, Data/binary>>}, N).

recv_timeout(C, Timeout) ->
    case gen_tcp:recv(csock(C), 0, Timeout) of
        {ok, Data} -> {ok, Data};
        {error, Reason} -> {error, Reason}
    end.

csock(#c{s = S}) -> S.
