%% @doc S5 NATS core adapter: the wire protocol (INFO/CONNECT/PING/PONG,
%% PUB/SUB/UNSUB, MSG, queue groups, request/reply) over real sockets.
%%
%% The suite's client is intentionally byte-level: it speaks the documented
%% NATS text protocol directly. The Gate S5 evidence for a third-party client
%% comes from `test/protocol-compatibility/nats/' (nats.go).
-module(nats_conn_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(N, 2).
-define(REF, {nats, 0}).

-record(c, {s :: gen_tcp:socket(), buf = <<>> :: binary()}).

all() ->
    [info_is_sent_first,
     connect_and_ping_pong,
     pubsub_roundtrip,
     wildcard_star_matches_one_token,
     queue_group_delivers_once,
     request_reply,
     new_subscription_does_not_replay_history,
     unsubscribe_stops_delivery,
     unsub_max_auto_unsubscribes,
     echo_false_suppresses_own_messages,
     invalid_subject_is_refused,
     oversized_payload_is_refused,
     auth_failure_is_refused,
     unknown_operation_is_refused,
     verbose_sends_ok].

suite() -> [{timetrap, {seconds, 60}}].

init_per_suite(Config) ->
    %% Same posture as the other protocol suites: load, never start, so a CT
    %% node running all suites in one VM cannot inherit a previous listener.
    _ = application:load(broker_storage),
    _ = application:load(broker_router),
    _ = application:load(broker_queue),
    _ = application:load(broker_subscription),
    _ = application:load(broker_event),
    _ = application:load(broker_security),
    _ = application:load(broker_proto_nats),
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
    ok = application:set_env(broker_proto_nats, listeners,
                             [#{port => 0, ref => ?REF,
                                num_acceptors => 4, max_connections => 128}]),
    ok = application:set_env(broker_proto_nats, max_payload, 64),
    ok = application:set_env(broker_proto_nats, shared_poll_ms, 50),
    ok = application:set_env(broker_proto_nats, auth_token, undefined),
    {ok, _} = application:ensure_all_started(broker_storage),
    ok = create_partitions(),
    {ok, _} = application:ensure_all_started(broker_proto_nats),
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
    _ = application:stop(broker_proto_nats),
    _ = application:stop(broker_retain),
    _ = application:stop(broker_event),
    _ = application:stop(broker_security),
    _ = application:stop(broker_subscription),
    _ = application:stop(broker_queue),
    _ = application:stop(broker_router),
    _ = application:stop(broker_storage),
    ok.

%%%_* Cases -----------------------------------------------------------------

info_is_sent_first(Config) ->
    Port = ?config(port, Config),
    {ok, S} = gen_tcp:connect({127, 0, 0, 1}, Port,
                              [binary, {active, false}, {nodelay, true}]),
    C = #c{s = S},
    {Line, _C1} = recv_line(C),
    <<"INFO ", Json/binary>> = Line,
    #{<<"server_id">> := _, <<"proto">> := 1, <<"max_payload">> := 64,
      <<"headers">> := false} = json:decode(Json),
    close(C),
    ok.

connect_and_ping_pong(Config) ->
    C0 = open(Config),
    {<<"PONG">>, C1} = ping(C0),
    close(C1),
    ok.

pubsub_roundtrip(Config) ->
    Sub = open(Config),
    Pub = open(Config),
    {<<"PONG">>, Sub1} = sub(Sub, <<"foo.bar">>, undefined, <<"1">>),
    {<<"PONG">>, Pub1} = pub(Pub, <<"foo.bar">>, undefined, <<"hello">>),
    {<<"foo.bar">>, <<"1">>, undefined, <<"hello">>, _} = recv_msg(Sub1),
    close(Sub1),
    close(Pub1),
    ok.

wildcard_star_matches_one_token(Config) ->
    Sub = open(Config),
    Pub = open(Config),
    {_, Sub1} = sub(Sub, <<"a.*">>, undefined, <<"1">>),
    {_, Pub1} = pub(Pub, <<"a.b">>, undefined, <<"in">>),
    {<<"a.b">>, <<"1">>, undefined, <<"in">>, Sub2} = recv_msg(Sub1),
    {_, Pub2} = pub(Pub1, <<"a.b.c">>, undefined, <<"out">>),
    assert_no_msg(Sub2, 300),
    close(Sub2),
    close(Pub2),
    ok.

queue_group_delivers_once(Config) ->
    A = open(Config),
    B = open(Config),
    Pub = open(Config),
    {_, A1} = sub(A, <<"jobs">>, <<"workers">>, <<"1">>),
    {_, B1} = sub(B, <<"jobs">>, <<"workers">>, <<"1">>),
    Pub1 = lists:foldl(
             fun(I, Acc) ->
                     {_, Acc1} = pub(Acc, <<"jobs">>, undefined,
                                     integer_to_binary(I)),
                     Acc1
             end, Pub, lists:seq(1, 10)),
    Seen = collect_queue([A1, B1], 10, [], 5000),
    ?assertEqual(lists:sort([integer_to_binary(I) || I <- lists:seq(1, 10)]),
                 lists:sort(Seen)),
    close(A1), close(B1), close(Pub1),
    ok.

request_reply(Config) ->
    Requester = open(Config),
    Responder = open(Config),
    {_, R1} = sub(Requester, <<"reply.to">>, undefined, <<"r1">>),
    {_, S1} = sub(Responder, <<"svc">>, undefined, <<"s1">>),
    {_, R2} = pub(R1, <<"svc">>, <<"reply.to">>, <<"ping">>),
    {<<"svc">>, <<"s1">>, <<"reply.to">>, <<"ping">>, S2} = recv_msg(S1),
    {_, S3} = pub(S2, <<"reply.to">>, undefined, <<"pong">>),
    {<<"reply.to">>, <<"r1">>, undefined, <<"pong">>, R3} = recv_msg(R2),
    close(R3), close(S3),
    ok.

new_subscription_does_not_replay_history(Config) ->
    Pub = open(Config),
    {_, Pub1} = pub(Pub, <<"fresh">>, undefined, <<"old">>),
    Sub = open(Config),
    {_, Sub1} = sub(Sub, <<"fresh">>, undefined, <<"1">>),
    assert_no_msg(Sub1, 300),
    {_, Pub2} = pub(Pub1, <<"fresh">>, undefined, <<"new">>),
    {<<"fresh">>, <<"1">>, undefined, <<"new">>, Sub2} = recv_msg(Sub1),
    close(Sub2), close(Pub2),
    ok.

unsubscribe_stops_delivery(Config) ->
    Sub = open(Config),
    Pub = open(Config),
    {_, Sub1} = sub(Sub, <<"u.a">>, undefined, <<"7">>),
    {_, Pub1} = pub(Pub, <<"u.a">>, undefined, <<"one">>),
    {<<"u.a">>, <<"7">>, undefined, <<"one">>, Sub2} = recv_msg(Sub1),
    Sub3 = unsub(Sub2, <<"7">>),
    {_, Pub2} = pub(Pub1, <<"u.a">>, undefined, <<"two">>),
    assert_no_msg(Sub3, 300),
    close(Sub3), close(Pub2),
    ok.

unsub_max_auto_unsubscribes(Config) ->
    Sub = open(Config),
    Pub = open(Config),
    {_, Sub1} = sub(Sub, <<"m.a">>, undefined, <<"9">>),
    Sub2 = unsub(Sub1, <<"9">>, 1),
    {_, Pub1} = pub(Pub, <<"m.a">>, undefined, <<"first">>),
    {<<"m.a">>, <<"9">>, undefined, <<"first">>, Sub3} = recv_msg(Sub2),
    {_, Pub2} = pub(Pub1, <<"m.a">>, undefined, <<"second">>),
    assert_no_msg(Sub3, 300),
    close(Sub3), close(Pub2),
    ok.

echo_false_suppresses_own_messages(Config) ->
    Self = open(Config, #{<<"echo">> => false}),
    Other = open(Config),
    {_, Self1} = sub(Self, <<"e.a">>, undefined, <<"1">>),
    {_, Other1} = sub(Other, <<"e.a">>, undefined, <<"2">>),
    %% The publisher is Self: it must not get its own message; Other must.
    {_, Self2} = pub(Self1, <<"e.a">>, undefined, <<"mine">>),
    {<<"e.a">>, <<"2">>, undefined, <<"mine">>, Other2} = recv_msg(Other1),
    assert_no_msg(Self2, 300),
    close(Self2), close(Other2),
    ok.

invalid_subject_is_refused(Config) ->
    C0 = open(Config),
    C1 = send_raw(C0, <<"SUB foo..bar 1\r\n">>),
    {Line, _C2} = recv_line(C1),
    <<"-ERR ", _/binary>> = Line,
    ok.

oversized_payload_is_refused(Config) ->
    C0 = open(Config),
    C1 = send_raw(C0, <<"PUB big 65\r\n">>),
    {Line, _C2} = recv_line(C1),
    <<"-ERR 'Maximum Payload Violation'", _/binary>> = Line,
    ok.

auth_failure_is_refused(Config) ->
    ok = application:set_env(broker_proto_nats, auth_token, <<"secret">>),
    C0 = raw_open(Config),
    C1 = connect_raw(C0, #{}),
    {Line, _C2} = recv_line(C1),
    <<"-ERR 'Authorization Violation'", _/binary>> = Line,
    ok.

unknown_operation_is_refused(Config) ->
    C0 = open(Config),
    C1 = send_raw(C0, <<"BOGUS x\r\n">>),
    {Line, _C2} = recv_line(C1),
    <<"-ERR 'Unknown Protocol Operation'", _/binary>> = Line,
    ok.

verbose_sends_ok(Config) ->
    C0 = raw_open(Config),
    C1 = connect_raw(C0, #{<<"verbose">> => true}),
    {<<"+OK">>, C2} = recv_line(C1),
    {<<"+OK">>, C3} = sub_verbose(C2, <<"v.a">>, <<"1">>),
    close(C3),
    ok.

sub_verbose(C, Subject, Sid) ->
    C1 = send_raw(C, <<"SUB ", Subject/binary, " ", Sid/binary, "\r\n">>),
    recv_line(C1).

%%%_* Queue collection -----------------------------------------------------

collect_queue(_Conns, 0, Seen, _Timeout) ->
    Seen;
collect_queue([A, B], N, Seen, Timeout) ->
    %% Both updated connections must be threaded through: a poll can read from
    %% both sockets, and dropping the one we did not take a message from would
    %% throw those already-delivered bytes away (the kernel has no copy).
    case recv_msg_any(A, B, Timeout) of
        {msg, a, _Subject, _Sid, _Reply, Payload, A1, B1} ->
            collect_queue([A1, B1], N - 1, [Payload | Seen], Timeout);
        {msg, b, _Subject, _Sid, _Reply, Payload, A1, B1} ->
            collect_queue([A1, B1], N - 1, [Payload | Seen], Timeout);
        {timeout, _A1, _B1} ->
            ct:fail({queue_group_lost_messages, N, lists:sort(Seen)})
    end.

recv_msg_any(A, B, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    recv_msg_any2(A, B, Deadline).

recv_msg_any2(A, B, Deadline) ->
    Left = Deadline - erlang:monotonic_time(millisecond),
    case Left =< 0 of
        true ->
            {timeout, A, B};
        false ->
            case has_complete_msg(A) of
                true ->
                    {Subject, Sid, Reply, Payload, A1} = recv_msg(A),
                    {msg, a, Subject, Sid, Reply, Payload, A1, B};
                false ->
                    case has_complete_msg(B) of
                        true ->
                            {Subject, Sid, Reply, Payload, B1} = recv_msg(B),
                            {msg, b, Subject, Sid, Reply, Payload, A, B1};
                        false ->
                            %% Nudge both sockets by reading with a short timeout.
                            A1 = fill(A, 50),
                            B1 = fill(B, 50),
                            recv_msg_any2(A1, B1, Deadline)
                    end
            end
    end.

has_complete_msg(#c{buf = Buf}) ->
    case binary:split(Buf, <<"\r\n">>) of
        [Line, Rest] ->
            case binary:split(Line, <<" ">>, [global]) of
                [<<"MSG">>, _Subject, _Sid, SizeBin] ->
                    byte_size(Rest) >= binary_to_integer(SizeBin) + 2;
                [<<"MSG">>, _Subject, _Sid, _Reply, SizeBin] ->
                    byte_size(Rest) >= binary_to_integer(SizeBin) + 2;
                _ ->
                    false
            end;
        _ ->
            false
    end.

%%%_* Client ---------------------------------------------------------------

open(Config) ->
    open(Config, #{}).

open(Config, Opts) ->
    C0 = raw_open(Config),
    C1 = connect_raw(C0, Opts),
    {<<"PONG">>, C2} = ping(C1),
    C2.

raw_open(Config) ->
    Port = ?config(port, Config),
    {ok, S} = gen_tcp:connect({127, 0, 0, 1}, Port,
                              [binary, {active, false}, {nodelay, true}]),
    C = #c{s = S},
    {<<"INFO ">>, InfoJson, C1} = recv_prefixed(C, <<"INFO ">>),
    _ = InfoJson,
    C1.

connect_raw(C, Opts) ->
    Map = maps:merge(#{<<"verbose">> => false, <<"pedantic">> => false,
                       <<"echo">> => true, <<"protocol">> => 1}, Opts),
    send_raw(C, [<<"CONNECT ">>, json:encode(Map), <<"\r\n">>]).

ping(C) ->
    C1 = send_raw(C, <<"PING\r\n">>),
    recv_line(C1).

sub(C, Subject, undefined, Sid) ->
    C1 = send_raw(C, [<<"SUB ">>, Subject, <<" ">>, Sid, <<"\r\n">>]),
    ping(C1);
sub(C, Subject, Queue, Sid) ->
    C1 = send_raw(C, [<<"SUB ">>, Subject, <<" ">>, Queue, <<" ">>, Sid, <<"\r\n">>]),
    ping(C1).

unsub(C, Sid) ->
    send_raw(C, [<<"UNSUB ">>, Sid, <<"\r\n">>]).

unsub(C, Sid, Max) ->
    send_raw(C, [<<"UNSUB ">>, Sid, <<" ">>, integer_to_binary(Max), <<"\r\n">>]).

pub(C, Subject, Reply, Payload) ->
    Size = integer_to_binary(byte_size(Payload)),
    Line = case Reply of
               undefined -> [<<"PUB ">>, Subject, <<" ">>, Size, <<"\r\n">>];
               _ -> [<<"PUB ">>, Subject, <<" ">>, Reply, <<" ">>, Size, <<"\r\n">>]
           end,
    C1 = send_raw(C, [Line, Payload, <<"\r\n">>]),
    ping(C1).

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

recv_msg(C) ->
    {Line, C1} = recv_line(C),
    case binary:split(Line, <<" ">>, [global]) of
        [<<"MSG">>, Subject, Sid, SizeBin] ->
            {Payload, C2} = recv_payload(C1, binary_to_integer(SizeBin)),
            {Subject, Sid, undefined, Payload, C2};
        [<<"MSG">>, Subject, Sid, Reply, SizeBin] ->
            {Payload, C2} = recv_payload(C1, binary_to_integer(SizeBin)),
            {Subject, Sid, Reply, Payload, C2};
        _ ->
            ct:fail({unexpected_line, Line})
    end.

recv_payload(C, Size) ->
    {Buf, C1} = recv_exact(C, Size + 2),
    <<Payload:Size/binary, "\r\n">> = Buf,
    {Payload, C1}.

recv_exact(#c{buf = Buf} = C, N) when byte_size(Buf) >= N ->
    <<Take:N/binary, Rest/binary>> = Buf,
    {Take, C#c{buf = Rest}};
recv_exact(#c{buf = Buf} = C, N) ->
    {ok, Data} = gen_tcp:recv(C#c.s, 0, 5000),
    recv_exact(C#c{buf = <<Buf/binary, Data/binary>>}, N).

fill(#c{buf = Buf} = C, Timeout) ->
    case gen_tcp:recv(C#c.s, 0, Timeout) of
        {ok, Data} ->
            C#c{buf = <<Buf/binary, Data/binary>>};
        {error, timeout} -> C;
        {error, closed} -> C
    end.

assert_no_msg(C, Timeout) ->
    C1 = fill(C, Timeout),
    case C1#c.buf of
        <<>> -> ok;
        Buf -> ct:fail({unexpected_bytes, Buf})
    end.
