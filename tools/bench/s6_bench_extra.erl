%% @doc S6 capacity report support: two measurements the paho-driven matrix
%% cannot give.
%%
%%   * `backend/3' -- append throughput of the memory backend vs the WAL
%%     backend's durable (fsync) path.  The release harness in `s2_bench.py'
%%     always runs WAL; this runs both backends in one VM through the *same*
%%     `broker_queue:append/3' entry point (limits included), 8 concurrent
%%     callers so group commit is exercised, and then reads every record back.
%%   * `codec/1' -- CloudEvents codec cost (decode / validate / encode, both
%%     Structured and Binary Mode) and the term size of the decoded view.
%%
%% It is a benchmark, not a test: numbers are printed as Erlang terms for the
%% report to quote.  Compile and run from the repo root:
%%
%%   erlc -pa _build/default/lib/broker_core/ebin -o /tmp/opencode/ebin \
%%        tools/bench/s6_bench_extra.erl
%%   erl -pa _build/default/lib/*/ebin -pa /tmp/opencode/ebin -noshell \
%%       -eval 's6_bench_extra:run_all(), halt().'
-module(s6_bench_extra).

-export([run_all/0, backend/3, codec/1]).

-include_lib("broker_core/include/broker_message.hrl").

-define(PARTITIONS, 4).

%%%_* Backend comparison ---------------------------------------------------

run_all() ->
    io:format("~n=== backend append+read (1024 B, 8 callers, 4 partitions) ===~n"),
    Wal = backend(broker_storage_wal, 8000, 8),
    Mem = backend(broker_storage_memory, 8000, 8),
    io:format("wal  = ~p~n", [Wal]),
    io:format("mem  = ~p~n", [Mem]),
    io:format("=== cloudevents codec (1 KiB data, 2 extensions) ===~n"),
    Ce = codec(20000),
    io:format("ce   = ~p~n", [Ce]),
    ok.

backend(Backend, Total, Callers) ->
    stop_apps(),
    %% The WAL backend refuses tmpfs/ramfs (ADR-011), so the benchmark data dir
    %% lives under the repo's ext4 `data/' tree and is removed before/after.
    DataDir = filename:join([filename:absname("data"),
                             "s6-bench-" ++ atom_to_list(Backend) ++ "-" ++ os:getpid()]),
    _ = os:cmd("rm -rf '" ++ DataDir ++ "'"),
    application:load(broker_storage),
    application:set_env(broker_storage, backend, Backend),
    application:set_env(broker_storage, data_dir, DataDir),
    application:set_env(broker_storage, wal,
                        #{batch_size => 32, max_batch_size => 256,
                          batch_delay_ms => #{os_buffer => 0, fsync => 1},
                          index_anchor_interval => 256,
                          segment_max_records => 100000,
                          max_pending_appends => 4096}),
    application:set_env(broker_queue, partitions, ?PARTITIONS),
    {ok, _} = application:ensure_all_started(broker_storage),
    [ok = broker_queue:create_partition(P) || P <- lists:seq(0, ?PARTITIONS - 1)],
    {ok, _} = application:ensure_all_started(broker_queue),

    Mode = case Backend of
               broker_storage_wal -> fsync;          % durable path
               broker_storage_memory -> os_buffer    % no disk, by definition
           end,
    %% Warm every partition so the first append does not pay for segment
    %% creation / ETS growth.
    [begin {ok, _} = broker_queue:append(P, msg(P, 0), Mode) end
     || P <- lists:seq(0, ?PARTITIONS - 1)],

    Per = Total div Callers,
    Self = self(),
    T0 = erlang:monotonic_time(microsecond),
    Pids = [spawn_link(fun() ->
                           Res = append_many(C, Per, Mode),
                           Self ! {done, self(), Res}
                       end) || C <- lists:seq(0, Callers - 1)],
    [receive {done, P, _} -> ok end || P <- Pids],
    T1 = erlang:monotonic_time(microsecond),
    AppendUs = max(1, T1 - T0),

    %% Read everything back, page size 32 (the delivery page limit).
    R0 = erlang:monotonic_time(microsecond),
    Read = read_all(0, 0, 0),
    R1 = erlang:monotonic_time(microsecond),
    ReadUs = max(1, R1 - R0),

    stop_apps(),
    #{backend => Backend,
      messages => Total,
      append_ms => round(AppendUs / 1000),
      append_msg_s => round(Total * 1000000 / AppendUs),
      read_messages => Read,
      read_ms => round(ReadUs / 1000),
      read_msg_s => round(Read * 1000000 / ReadUs)}.

append_many(_Caller, 0, _Mode) ->
    ok;
append_many(Caller, N, Mode) ->
    P = Caller rem ?PARTITIONS,
    {ok, _} = broker_queue:append(P, msg(P, N), Mode),
    append_many(Caller, N - 1, Mode).

read_all(Partition, Offset, Acc) when Partition >= ?PARTITIONS ->
    _ = Offset,
    Acc;
read_all(Partition, Offset, Acc) ->
    case broker_queue:read_from(Partition, Offset, 32) of
        {ok, [], Next} ->
            read_all(Partition + 1, start_of(Partition + 1, Next), Acc);
        {ok, Page, Next} ->
            read_all(Partition, Next, Acc + length(Page))
    end.

%% Offsets restart per partition; a fresh partition starts at 0.  The `Next'
%% of an exhausted partition is only used to advance the partition index.
start_of(_NextPartition, _Next) -> 0.

msg(Partition, Seq) ->
    #broker_message{destination = <<"bench/", (integer_to_binary(Partition))/binary>>,
                    payload = payload(1024, Seq),
                    headers = #{},
                    tenant_id = <<"t0">>}.

payload(Size, Seq) ->
    Header = <<Seq:64>>,
    <<Header/binary, (binary:copy(<<"x">>, Size - byte_size(Header)))/binary>>.

stop_apps() ->
    _ = application:stop(broker_queue),
    _ = application:stop(broker_storage),
    _ = application:unload(broker_queue),
    _ = application:unload(broker_storage),
    ok.

%%%_* CloudEvents codec ----------------------------------------------------

codec(N) ->
    Event = event(0),
    {ok, Structured} = broker_event_codec:encode_structured(Event),
    {ok, _Data, _Props, _CT} = broker_event_codec:encode_binary(Event),

    Dec = time(N, fun() ->
                          {ok, _} = broker_event_codec:decode_structured(Structured)
                  end),
    Val = time(N, fun() -> ok = broker_event_codec:validate(Event) end),
    EncS = time(N, fun() ->
                           {ok, _} = broker_event_codec:encode_structured(Event)
                   end),
    EncB = time(N, fun() ->
                           {ok, _, _, _} = broker_event_codec:encode_binary(Event)
                   end),
    %% Policy seam: the same structured body under the two postures.
    Msg = #broker_message{destination = <<"bench/ce">>, payload = Structured,
                          headers = #{}, proto_meta = #{}, tenant_id = <<"t0">>},
    Pass = time(N, fun() ->
                           {ok, _} = broker_event:apply_policy(Msg, pass_through)
                   end),
    Req = time(N, fun() ->
                          {ok, _} = broker_event:apply_policy(Msg, required)
                  end),

    #{iterations => N,
      structured_bytes => byte_size(Structured),
      decoded_flat_bytes => flat_bytes(Event),
      decode_ops_s => ops_s(N, Dec),
      validate_ops_s => ops_s(N, Val),
      encode_structured_ops_s => ops_s(N, EncS),
      encode_binary_ops_s => ops_s(N, EncB),
      policy_pass_through_ops_s => ops_s(N, Pass),
      policy_required_ops_s => ops_s(N, Req),
      required_overhead_pct => pct(Req, Pass)}.

event(Seq) ->
    #{specversion => <<"1.0">>,
      id => integer_to_binary(Seq),
      source => <<"/s6/bench">>,
      type => <<"unifiedmq.bench.event">>,
      datacontenttype => <<"application/json">>,
      extensions => #{<<"traceid">> => <<"4bf92f3577b34da6a3ce929d0e0e4736">>,
                      <<"attempt">> => 1},
      data => #{<<"seq">> => Seq,
                <<"blob">> => binary:copy(<<"x">>, 900)}}.

time(N, Fun) ->
    T0 = erlang:monotonic_time(microsecond),
    loop(N, Fun),
    erlang:monotonic_time(microsecond) - T0.

loop(0, _Fun) -> ok;
loop(N, Fun) -> Fun(), loop(N - 1, Fun).

ops_s(N, Us) -> round(N * 1000000 / max(1, Us)).

pct(With, Without) -> round((With - Without) * 100 / max(1, Without)).

%% Words -> bytes for a decoded term; includes the map/list structure, so it is
%% an upper bound on the heap cost of one event view.
flat_bytes(Term) -> erts_debug:flat_size(Term) * erlang:system_info(wordsize).
