%% @doc S10-D D.6 capacity bench: STUN/TURN allocation rate + relay throughput.
%%
%% Reuses the production TURN front end (`broker_ice') through the *same* byte
%% path a real TURN client speaks (`ice_test_client': the credential dance,
%% ChannelBind, ChannelData). Two measurements:
%%
%%   * **allocation rate** -- how long it takes to stand up K allocations
%%     (sequential and concurrent), i.e. the Allocate+401+Allocate round trips
%%     the supervised `turn_allocation' processes can absorb;
%%   * **relay throughput** -- datagrams/second and bytes/second the relay path
%%     (`broker_ice_udp' receive loop -> `turn_allocation' ChannelData route ->
%%     peer socket) sustains once one channel is bound. This is TURN application
%%     data relay (RFC 5766 sec. 14), NOT RTP media -- the media plane is
%%     explicitly out of scope (design 1.3), so there is no SRTP/DTLS here.
%%
%% Everything is loopback: the client and peer both bind 127.0.0.1 and the peer
%% permission is for 127.0.0.1, so the datagrams never leave the host. The
%% advertised relay IP is irrelevant to this bench -- the client talks to the
%% TURN port directly, it does not send *to* the relay address.
-module(ice_bench).

-export([run/5]).

%% Channel number for the bound relay channel (RFC 5766 sec. 14.2: 0x4000-0x7FFF).
-define(CHANNEL, 16#4000).

%% Host, StunPort, User, Password, Opts map.
%% Opts: #{alloc_seq => N, alloc_conc => N, relay_msgs => N, payload_bytes => N}
run(Host, StunPort, User, Password, Opts) ->
    AllocSeq = maps:get(alloc_seq, Opts, 50),
    AllocConc = maps:get(alloc_conc, Opts, 50),
    RelayMsgs = maps:get(relay_msgs, Opts, 5000),
    PayloadBytes = maps:get(payload_bytes, Opts, 256),
    io:format("{\"ice_bench\":~s}~n", [json_obj([
        {host, inet:ntoa(Host)},
        {stun_port, StunPort},
        {allocations, allocation_rate(Host, StunPort, User, Password,
                                      AllocSeq, AllocConc)},
        {relay, relay_throughput(Host, StunPort, User, Password,
                                 RelayMsgs, PayloadBytes)}
    ])]).

json_obj(Pairs) ->
    unicode:characters_to_binary(
        "{" ++ string:join([io_lib:format("\"~s\":~s", [K, json_val(V)])
                              || {K, V} <- Pairs], ",") ++ "}").

json_val(V) when is_integer(V) -> integer_to_list(V);
json_val(V) when is_float(V) -> io_lib:format("~.2f", [V]);
json_val(V) when is_atom(V) -> io_lib:format("\"~s\"", [V]);
json_val(V) when is_binary(V) -> io_lib:format("\"~s\"", [V]);
json_val(V) when is_list(V) -> io_lib:format("\"~s\"", [V]);
json_val(V) when is_map(V) -> json_obj(maps:to_list(V));
json_val(V) -> io_lib:format("\"~s\"", [io_lib:format("~0p", [V])]).

%% ---------------------------------------------------------------------------
%% Allocation rate
%% ---------------------------------------------------------------------------

allocation_rate(_Host, StunPort, User, Password, Seq, Conc) ->
    Creds = #{username => User, password => Password},
    SeqTimes = [time_alloc(StunPort, Creds) || _ <- lists:seq(1, Seq)],
    ConcTimes = concurrent_allocs(StunPort, Creds, Conc),
    #{sequential => latency_stats(SeqTimes),
      concurrent => latency_stats(ConcTimes),
      concurrent_processes => Conc,
      concurrent_errors => concurrent_failure_samples(ConcTimes)}.

format_error({error, {C, E, _Stack}}) ->
    iolist_to_binary(io_lib:format("~p:~p", [C, E]));
format_error({error, Other}) ->
    iolist_to_binary(io_lib:format("~0p", [Other]));
format_error({ok, _T}) ->
    <<"ok">>;
format_error(Other) ->
    iolist_to_binary(io_lib:format("~0p", [Other])).

%% Concurrent samples fail when the node's `max_allocations' is exhausted, so
%% report the actual failure values rather than an empty list.
concurrent_failure_samples(Times) ->
    [format_error(T) || T <- Times, not is_integer(T)].

%% Closing the client socket does NOT end the broker-side allocation: it lives
%% until its Lifetime expires (600 s by default) or a Refresh(0) deletes it
%% (RFC 5766 sec. 7.2). A bench that only closes the socket would leak one
%% allocation per sample and hit the node's `max_allocations' (16) with a 486
%% before the first ten samples. Every sample therefore frees its allocation
%% explicitly, and the free is timed *after* the measurement window -- the
%% reported figure is Allocate, not Allocate+Refresh.
time_alloc(StunPort, Creds) ->
    S = ice_test_client:open(),
    try
        T0 = erlang:monotonic_time(microsecond),
        {ok, Ctx} = ice_test_client:allocate(S, StunPort, Creds),
        T1 = erlang:monotonic_time(microsecond),
        _ = ice_test_client:refresh(S, StunPort, Ctx, 0),
        T1 - T0
    after
        _ = ice_test_client:close(S)
    end.

concurrent_allocs(StunPort, Creds, N) ->
    Parent = self(),
    Ref = make_ref(),
    Pids = [spawn(fun() ->
                S = ice_test_client:open(),
                T = try
                        T0 = erlang:monotonic_time(microsecond),
                        {ok, Ctx} = ice_test_client:allocate(S, StunPort, Creds),
                        T1 = erlang:monotonic_time(microsecond),
                        _ = ice_test_client:refresh(S, StunPort, Ctx, 0),
                        T1 - T0
                    catch
                        C:E:Stack -> {error, {C, E, Stack}}
                    after
                        _ = ice_test_client:close(S)
                    end,
                Parent ! {Ref, T}
            end) || _ <- lists:seq(1, N)],
    [receive {Ref, T} -> T end || _ <- Pids].

latency_stats(TimesUs) ->
    Ok = [T || T <- TimesUs, is_integer(T)],
    N = length(Ok),
    case N of
        0 ->
            #{count => 0, failures => length(TimesUs),
              mean_us => null, p50_us => null, p95_us => null,
              min_us => null, max_us => null};
        _ ->
            Sorted = lists:sort(Ok),
            #{count => N,
              failures => length(TimesUs) - N,
              mean_us => round(lists:sum(Ok) / N),
              p50_us => percentile(Sorted, 0.50),
              p95_us => percentile(Sorted, 0.95),
              min_us => hd(Sorted),
              max_us => lists:last(Sorted)}
    end.

percentile(Sorted, P) ->
    N = length(Sorted),
    case N of
        0 -> null;
        _ ->
            Idx = max(1, round(P * N)),
            lists:nth(min(Idx, N), Sorted)
    end.

%% ---------------------------------------------------------------------------
%% Relay throughput (ChannelData after a bound channel, loopback)
%% ---------------------------------------------------------------------------
%%
%% Measurement: the client blasts `Msgs' ChannelData frames with no pacing and
%% the peer socket counts them as they arrive; the run ends 500 ms after the
%% last one. The span is (first -> last datagram received), so the reported rate
%% is what the relay itself sustained, not what a sleep() would have capped it
%% at. Because the sender is unpaced, `messages_relayed' vs `messages_sent'
%% also reports loss straight from the wire: an unpaced burst that overflows
%% the loopback receive buffer loses datagrams, and the report says so instead
%% of hiding it in a smoothed average.

relay_throughput(_Host, StunPort, User, Password, Msgs, PayloadBytes) ->
    Creds = #{username => User, password => Password},
    %% Warm up the channel path once so the measured run is steady state.
    _ = do_relay(StunPort, Creds, min(Msgs, 1000), PayloadBytes),
    do_relay(StunPort, Creds, Msgs, PayloadBytes).

-define(IDLE_MS, 500).

do_relay(StunPort, Creds, Msgs, PayloadBytes) ->
    Channel = 16#4000,
    S1 = ice_test_client:open(),
    try
        {ok, Ctx} = ice_test_client:allocate(S1, StunPort, Creds),
        S2 = ice_test_client:open(),
        {ok, P2} = inet:port(S2),
        Peer = {{127, 0, 0, 1}, P2},
        {ok, _} = ice_test_client:create_permission(S1, StunPort, Ctx, [Peer]),
        {ok, _} = ice_test_client:channel_bind(S1, StunPort, Ctx, Channel, Peer),
        %% A megabyte of receive buffer so a normal burst is not dropped before
        %% it reaches the relay at all.
        _ = inet:setopts(S2, [{recbuf, 1048576}]),
        Payload = binary:copy(<<"x">>, PayloadBytes),
        Parent = self(),
        _Receiver = spawn_link(fun() ->
                         recv_all(S2, Payload, Parent, none, 0, 0)
                     end),
        timer:sleep(50),
        T0 = erlang:monotonic_time(microsecond),
        blast(S1, StunPort, Channel, Payload, Msgs),
        %% The receiver reports exactly once, when the peer has been quiet for
        %% ?IDLE_MS. That single message is the whole result: count plus the
        %% first/last receive timestamps. Waiting for "all N" would hang the
        %% bench whenever the unpaced burst had actually dropped something.
        {Count, TFirst, TLast, Bytes} = receive
                                             {relayed, C, TF, TL, B} ->
                                                 {C, TF, TL, B}
                                         after 120000 ->
                                             {0, 0, undefined, 0}
                                         end,
        T1 = erlang:monotonic_time(microsecond),
        %% The relay is finished when the peer has been silent for ?IDLE_MS.
        Secs = max(TLast - TFirst, 1) / 1000000,
        _ = ice_test_client:refresh(S1, StunPort, Ctx, 0),
        _ = ice_test_client:close(S2),
        #{payload_bytes => PayloadBytes,
          messages_sent => Msgs,
          messages_relayed => Count,
          bytes_relayed => Bytes,
          seconds => round(Secs * 1000) / 1000,
          datagrams_per_s => round(Count / max(Secs, 0.000001)),
          bytes_per_s => round(Bytes / max(Secs, 0.000001)),
          delivered_ratio => round(Count / max(Msgs, 1) * 1000) / 1000,
          send_wall_s => round((T1 - T0) / 1000000 * 1000) / 1000}
    after
        _ = ice_test_client:close(S1)
    end.

recv_all(S, Payload, Parent, T0, Count, Bytes) ->
    case ice_test_client:recv(S, ?IDLE_MS) of
        {ok, Bin} ->
            case Bin of
                Payload ->
                    Now = erlang:monotonic_time(microsecond),
                    T0b = case T0 of none -> Now; _ -> T0 end,
                    recv_all(S, Payload, Parent, T0b, Count + 1,
                             Bytes + byte_size(Bin));
                _Other ->
                    %% Not our payload (or a stray datagram): ignore it without
                    %% touching the clock, so the span stays first->last real
                    %% delivery.
                    recv_all(S, Payload, Parent, T0, Count, Bytes)
            end;
        timeout ->
            Now = erlang:monotonic_time(microsecond),
            T0b = case T0 of none -> Now; _ -> T0 end,
            Parent ! {relayed, Count, T0b, Now, Bytes}
    end.

blast(_S, _Port, _Ch, _Payload, 0) -> ok;
blast(S, Port, Ch, Payload, N) ->
    ice_test_client:channel_data(S, Port, Ch, Payload),
    blast(S, Port, Ch, Payload, N - 1).
