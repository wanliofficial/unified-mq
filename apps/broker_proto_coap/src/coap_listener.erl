%% @doc The CoAP UDP listener: one datagram in, one datagram out.
%%
%% The shape is MQTT-SN's (ADR-022), with the session layer removed, because
%% CoAP has no session: a datagram carries its own Message ID, token and
%% options, so the listener is a stateless-per-datagram router into
%% `coap_resource' plus three pieces of *bounded* bookkeeping that live here
%% and nowhere else:
%%
%%   * CON dedup -- RFC 7252 §4.8.2: a duplicate CON within EXCHANGE_LIFETIME
%%     must be answered with the original ACK, not re-executed. Keyed by
%%     {IP, Port, Id} (ADR-031 §D2: never `{Port, IP}' as a *session* key --
%%     here it is only a transport routing tuple, per-datagram).
%%   * per-endpoint token bucket -- UDP has no handshake, so the source address
%%     is unverifiable and a flood is only observable as a rate (R-X3).
%%   * Block1 reassembly -- RFC 7959 draft §2.1: a POST/PUT body larger than one
%%     datagram arrives as a sequence, and the assembled buffer is bounded.
%%
%% The listener owns the socket and is the only writer to it, so there is no
%% hand-off and no second process to race the dedup table. Replies go out with
%% `gen_udp:send/4', which any process may call on a UDP socket.
%%
%% Reliability (ADR-031 §D3) is per-datagram and is decided here, before the
%% resource layer runs: a CON request is answered with a piggybacked ACK, a NON
%% request gets no piggybacked ACK at all (its reply, if the resource has one,
%% is a separate NON response). Neither is "reliable delivery" in the MQTT QoS 1
%% sense.
%%
%% Every table here is capped and swept on the one-second tick, so a flood of
%% new source addresses or a client that abandons a Block1 stream cannot grow
%% memory without bound.
-module(coap_listener).

-behaviour(gen_server).

-export([start_link/1, port/1, stop/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_continue/2,
         handle_info/2, terminate/2, code_change/3]).

-record(ep, {tokens :: non_neg_integer(),
             refill :: integer()}).

%% How many datagrams may be queued in the mailbox before the socket goes
%% passive; same budget as `mqtt_sn_listener' (ADR-022).
-define(ACTIVE_N, 100).

-record(st, {
    socket :: gen_udp:socket(),
    port :: inet:port_number(),
    cfg :: map(),
    %% {IP, Port, Id} -> {ReplyBytes, AtMs} for a CON we already answered.
    dedup = #{} :: map(),
    %% {IP, Port} -> token-bucket state.
    endpoints = #{} :: map(),
    %% {IP, Port, Token, Destination} -> {Assembled, Total, AtMs} for a Block1
    %% stream. The token identifies the transfer (RFC 7959 draft §2.1); the
    %% Message ID changes from block to block and must not be the key.
    block1 = #{} :: map()
}).

start_link(Spec) ->
    gen_server:start_link(?MODULE, Spec, []).

port(Pid) ->
    gen_server:call(Pid, port).

stop(Pid) ->
    gen_server:stop(Pid).

init(Spec) when is_map(Spec) ->
    process_flag(trap_exit, true),
    %% App-wide settings come from the application environment; the listener
    %% spec may override any of them (a second listener with its own limits).
    Cfg = config(Spec),
    Port = maps:get(port, Cfg),
    Opts = [binary, {active, ?ACTIVE_N}, {reuseaddr, true},
            {recbuf, maps:get(recbuf_bytes, Cfg, 65536)}],
    case gen_udp:open(Port, Opts) of
        {ok, Socket} ->
            {ok, ActualPort} = inet:port(Socket),
            {ok, #st{socket = Socket, port = ActualPort, cfg = Cfg},
             {continue, refill}};
        {error, Reason} ->
            %% A listener that cannot bind is a boot failure (ADR-031 §D6,
            %% mirroring `broker_proto_mqtt_sn').
            {stop, {bind_failed, Port, Reason}}
    end.

handle_continue(refill, St) ->
    _ = erlang:start_timer(1000, self(), refill),
    {noreply, St}.

%% The app-wide defaults, with the listener spec able to override any of them.
config(Spec) ->
    Defaults = #{max_message_bytes => env(max_message_bytes, 65535),
                 dedup_window_ms => env(dedup_window_ms, 247000),
                 max_dedup_entries => env(max_dedup_entries, 4096),
                 max_requests_per_second => env(max_requests_per_second, 100),
                 max_endpoints => env(max_endpoints, 4096),
                 block_size => env(block_size, 1024),
                 max_block1_bytes => env(max_block1_bytes, 262144),
                 resources => env(resources, []),
                 recbuf_bytes => env(recbuf_bytes, 65536)},
    maps:merge(Defaults, Spec).

env(Key, Default) ->
    application:get_env(broker_proto_coap, Key, Default).

handle_call(port, _From, #st{port = P} = St) ->
    {reply, {ok, P}, St};
handle_call(_Req, _From, St) ->
    {reply, {error, unknown_call}, St}.

handle_cast(_Msg, St) ->
    {noreply, St}.

%% The one-second maintenance tick. Rate tokens are computed lazily on the
%% datagram path, so the tick only has to forget what has expired: CON replies
%% past EXCHANGE_LIFETIME, abandoned Block1 streams, and endpoints whose bucket
%% is full again (a full bucket is what a fresh entry would be anyway).
handle_info({timeout, _Ref, refill}, St) ->
    {noreply, prune(St), {continue, refill}};

handle_info({udp, _Socket, IP, Port, Bin}, #st{} = St0) ->
    St1 = on_datagram(IP, Port, Bin, St0),
    {noreply, St1};

%% `{active, N}' is a budget, not a mode: when it runs out the socket goes
%% passive and must be re-armed, or the listener silently stops reading. The
%% budget (not `{active, true}') is what keeps a flood from filling the mailbox.
handle_info({udp_passive, Socket}, #st{socket = Socket} = St) ->
    _ = inet:setopts(Socket, [{active, ?ACTIVE_N}]),
    {noreply, St};

handle_info({udp_error, Socket, Reason}, #st{socket = Socket} = St) ->
    logger:warning("coap listener socket error: ~p", [Reason]),
    {noreply, St};

handle_info(_Info, St) ->
    {noreply, St}.

terminate(_Reason, _St) ->
    ok.

code_change(_Old, St, _Extra) ->
    {ok, St}.

%%%_* Datagram path --------------------------------------------------------

on_datagram(IP, Port, Bin, #st{cfg = Cfg} = St) ->
    broker_metrics:inc(bytes_in, byte_size(Bin)),
    case byte_size(Bin) > maps:get(max_message_bytes, Cfg, 65535) of
        true ->
            broker_metrics:inc(frame_decode_errors),
            St;
        false ->
            case allowed(IP, Port, Cfg, St) of
                {false, St1} ->
                    broker_metrics:inc(protocol_errors),
                    St1;
                {true, St1} ->
                    dispatch(IP, Port, Bin, Cfg, St1)
            end
    end.

%% Token bucket, `max_requests_per_second' tokens refilled at that rate. The
%% bucket is consumed here, before decoding: work is what a flood must not buy.
%% An endpoint the table has never seen is refused once the table is full
%% (`max_endpoints'), so a spoofed-source flood cannot evict live buckets.
allowed(IP, Port, Cfg, #st{endpoints = Eps} = St) ->
    Max = maps:get(max_requests_per_second, Cfg, 100),
    Cap = maps:get(max_endpoints, Cfg, 4096),
    Key = {IP, Port},
    Now = erlang:monotonic_time(millisecond),
    case maps:find(Key, Eps) of
        error when map_size(Eps) >= Cap ->
            {false, St};
        Found ->
            Ep = case Found of
                     {ok, E} -> E;
                     error -> #ep{tokens = Max, refill = Now}
                 end,
            #ep{tokens = Tokens} = Ep1 = refill(Ep, Now, Max),
            case Tokens > 0 of
                true ->
                    {true, St#st{endpoints = maps:put(
                                            Key, Ep1#ep{tokens = Tokens - 1}, Eps)}};
                false ->
                    {false, St#st{endpoints = maps:put(Key, Ep1, Eps)}}
            end
    end.

refill(#ep{tokens = T, refill = Last} = Ep, Now, Max) ->
    case Now - Last >= 1000 of
        true -> Ep#ep{tokens = min(Max, T + (Now - Last) div 1000), refill = Now};
        false -> Ep
    end.

dispatch(IP, Port, Bin, Cfg, St) ->
    case coap_codec:decode(Bin) of
        {error, _} ->
            broker_metrics:inc(frame_decode_errors),
            St;
        {ok, Msg} ->
            handle_message(IP, Port, Msg, Cfg, St)
    end.

handle_message(IP, Port, Msg, Cfg, #st{dedup = Dedup} = St) ->
    Id = coap_codec:id(Msg),
    case coap_codec:type(Msg) of
        con ->
            %% Already answered this exact CON? Re-send the cached bytes without
            %% re-executing (RFC 7252 §4.8.2).
            case maps:find({IP, Port, Id}, Dedup) of
                {ok, {Cached, _At}} ->
                    send(IP, Port, Cached, St),
                    St;
                error ->
                    {Reply, St1} = respond(IP, Port, Msg, Cfg, St),
                    send(IP, Port, Reply, St1),
                    remember(IP, Port, Id, Reply, Cfg, St1)
            end;
        _Non ->
            %% NON/RST/ACK: no piggybacked reply is owed. A NON request still
            %% gets the resource's answer, as a separate NON response.
            {Reply, St1} = respond(IP, Port, Msg, Cfg, St),
            send(IP, Port, Reply, St1),
            St1
    end.

%% Run the datagram through Block1 reassembly, then through the resource layer,
%% and return the bytes to send. A mid-stream block is answered with 2.31
%% Continue; an oversized or incomplete stream with 4.13 / 4.08.
respond(IP, Port, Msg, Cfg, St) ->
    case block1_stream(IP, Port, Msg, Cfg, St) of
        {single, Req, St1} ->
            {resource_reply(Req, Cfg, {IP, Port}), St1};
        {done, Req, St1} ->
            {resource_reply(Req, Cfg, {IP, Port}), St1};
        {streaming, Continue, St1} ->
            {coap_codec:encode(Continue), St1};
        {refused, Refusal, St1} ->
            {coap_codec:encode(Refusal), St1}
    end.

send(IP, Port, Bin, #st{socket = Socket}) ->
    _ = gen_udp:send(Socket, IP, Port, Bin),
    ok.

resource_reply(Req, Cfg, Endpoint) ->
    {ReplyCode, Options, Payload} = run_resource(Req, Cfg, Endpoint),
    encode_reply(Req, reply_type(Req), ReplyCode, Options, Payload).

%% RFC 7252 §5.2.1: a reply to a CON rides in the ACK; a reply to a NON is a
%% separate NON response. Both echo the request's token.
reply_type(Req) ->
    case coap_codec:type(Req) of
        con -> ack;
        Other -> Other
    end.

encode_reply(Req, Type, ReplyCode, Options, Payload) ->
    Base = coap_codec:response(Type, ReplyCode, Payload),
    WithId = Base#{id => coap_codec:id(Req), token => coap_codec:token(Req)},
    WithOptions = lists:foldl(fun({Name, Val}, Acc) ->
                                     coap_codec:put_option(Name, Val, Acc)
                             end, WithId, Options),
    coap_codec:encode(WithOptions).

%%%_* Block1 reassembly ----------------------------------------------------

block1_stream(IP, Port, Msg, Cfg, #st{block1 = Streams} = St) ->
    case coap_codec:block(block1, Msg) of
        undefined ->
            {single, Msg, St};
        {Num, More, Size} ->
            Key = {IP, Port, coap_codec:token(Msg), coap_codec:destination(Msg)},
            Chunk = coap_codec:payload(Msg),
            Ceiling = maps:get(max_block1_bytes, Cfg, 262144),
            Now = erlang:monotonic_time(millisecond),
            case maps:find(Key, Streams) of
                {ok, {Acc, Total, _At}} ->
                    assemble(Msg, Num, More, Size, <<Acc/binary, Chunk/binary>>,
                             Total, Ceiling, Streams, Key, Now, St);
                error when Num =:= 0 ->
                    assemble(Msg, Num, More, Size, Chunk, 0, Ceiling,
                             Streams, Key, Now, St);
                error ->
                    %% A block after the first with no stream to append to: the
                    %% earlier blocks are gone (expired or never seen), so the
                    %% body cannot be completed -- 4.08, and do not dispatch a
                    %% truncated body as if it were whole.
                    {refused, refusal(Msg, {response, 4, 8}),
                     St#st{block1 = maps:remove(Key, Streams)}}
            end
    end.

assemble(Msg, Num, More, Size, Assembled, PrevTotal, Ceiling, Streams, Key, Now,
         St) ->
    Total = PrevTotal + byte_size(coap_codec:payload(Msg)),
    case Total > Ceiling of
        true ->
            %% RFC 7959 draft §2.2: a recipient MAY reject an oversized body.
            {refused, refusal(Msg, {response, 4, 13}),
             St#st{block1 = maps:remove(Key, Streams)}};
        false ->
            case More of
                true ->
                    Continue = continue_reply(Msg, Size, Num + 1),
                    {streaming, Continue,
                     St#st{block1 = maps:put(Key, {Assembled, Total, Now}, Streams)}};
                false ->
                    {done, Msg#{payload => Assembled},
                     St#st{block1 = maps:remove(Key, Streams)}}
            end
    end.

%% The 2.31 Continue echoes the request's type/id/token and the *next* block
%% number with M=1, telling the client "send me block N+1".
continue_reply(Msg, Size, NextNum) ->
    Resp = refusal_base(Msg, {response, 2, 31}),
    coap_codec:put_option(block1,
                         coap_codec:block_value(NextNum, true, Size), Resp).

refusal(Msg, Code) ->
    refusal_base(Msg, Code).

refusal_base(Msg, Code) ->
    Resp = coap_codec:response(reply_type(Msg), Code, <<>>),
    Resp#{id => coap_codec:id(Msg), token => coap_codec:token(Msg)}.

%%%_* Resource call ---------------------------------------------------------

run_resource(Req, Cfg, Endpoint) ->
    Ctx = #{endpoint => Endpoint,
            tenant => broker_security:default_tenant(),
            principal => undefined},
    case catch coap_resource:handle(Req, Cfg, Ctx) of
        {reply, Code, Options, Payload} ->
            {Code, Options, Payload};
        _ ->
            {{response, 5, 0}, [], <<>>}
    end.

%%%_* Bounded tables -------------------------------------------------------

%% Cache the reply for a CON. The table is capped by `max_dedup_entries' and
%% swept by `prune/1' after `dedup_window_ms' (EXCHANGE_LIFETIME), so the cache
%% is a window, not a growing log.
remember(IP, Port, Id, Reply, Cfg, #st{dedup = Dedup0} = St) ->
    Cap = maps:get(max_dedup_entries, Cfg, 4096),
    Now = erlang:monotonic_time(millisecond),
    Dedup1 = maps:put({IP, Port, Id}, {Reply, Now}, Dedup0),
    Dedup2 = cap(Dedup1, Cap),
    St#st{dedup = Dedup2}.

cap(Map, Cap) when map_size(Map) =< Cap -> Map;
cap(Map, _Cap) ->
    evict(map_size(Map) div 3, Map).

evict(0, Map) -> Map;
evict(N, Map) ->
    [K | _] = maps:keys(Map),
    evict(N - 1, maps:remove(K, Map)).

%% Called on the one-second tick: forget expired CON replies, abandoned Block1
%% streams, and idle endpoints (a full bucket is indistinguishable from the
%% default a new endpoint gets, so dropping it loses nothing).
prune(#st{dedup = Dedup, endpoints = Eps, block1 = Streams, cfg = Cfg} = St) ->
    Window = maps:get(dedup_window_ms, Cfg, 247000),
    Max = maps:get(max_requests_per_second, Cfg, 100),
    Cutoff = erlang:monotonic_time(millisecond) - Window,
    Dedup1 = maps:filter(fun(_K, {_Reply, At}) -> At >= Cutoff end, Dedup),
    Block1 = maps:filter(fun(_K, {_Acc, _Total, At}) -> At >= Cutoff end, Streams),
    Eps1 = maps:filter(fun(_K, #ep{tokens = T, refill = At}) ->
                               not (T >= Max andalso At < Cutoff)
                       end, Eps),
    St#st{dedup = Dedup1, endpoints = Eps1, block1 = Block1}.
