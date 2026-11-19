%% @doc AMQP 0-9-1 connection (S5, README §11.1).
%%
%% Implements the subset real clients need for exchange/queue/binding +
%% publish/consume/ack. Messages still travel the broker's normal path:
%% `broker_router' -> `broker_queue' (fsync) -> `broker_subscription'.
%% A queue maps to the destination `amqp/<queue>'; consumers of one queue
%% share a `shared' subscription so AMQP competing-consumer semantics hold.
%%
%% Scope (documented in `docs/protocol-compatibility.md' §4.7, updated by the
%% compatibility remediation pass):
%%   * connection/channel handshake, exchange.declare/delete,
%%     queue.declare/bind/unbind/purge/delete, basic.qos/consume/cancel,
%%     basic.publish, basic.deliver, basic.get, basic.ack/nack/reject,
%%     basic.recover, confirm.select with confirm ack/nack;
%%   * `mandatory' publishes get `basic.return' (312 NO_ROUTE);
%%     `immediate' is refused with 540, never silently ignored;
%%   * tx.select/commit/rollback refuse with 540, never silently ignored;
%%   * basic properties round-trip through `proto_meta' (`amqp_props');
%%   * redelivered=1 on every delivery that was released unacked before;
%%   * an unknown method is 540 (channel) / 503 (connection), never silence;
%%   * unparseable frames close the connection with 503;
%%   * exchanges: direct, fanout, topic (headers declared but not routed);
%%   * topology is transient (ETS), message bodies are durable.
-module(amqp_conn).

-behaviour(gen_server).

-export([start_link/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-include_lib("broker_core/include/broker_message.hrl").
-include_lib("broker_router/include/broker_router.hrl").

-define(FRAME_METHOD, 1).
-define(FRAME_HEADER, 2).
-define(FRAME_BODY, 3).
-define(FRAME_HEARTBEAT, 8).
-define(FRAME_MAX, 131072).
-define(PAYLOAD_MAX, (?FRAME_MAX - 8)).
-define(DEFAULT_TICK, 100).
%% §4.2.7: the server's proposed heartbeat (seconds). Clients may lower it in
%% tune-ok; a proposal of 0 from either side disables heartbeats entirely.
-define(DEFAULT_HEARTBEAT, 60).

-record(ch, {
    pending = undefined :: undefined | {binary(), binary(), 0 | 1,
                                        non_neg_integer(), binary()},
    pending_props = #{} :: map(),
    headers = #{} :: map(),
    deliveries = #{} :: map(),
    next_tag = 1 :: pos_integer(),
    prefetch = 0 :: non_neg_integer(),
    consumers = #{} :: map(),
    %% confirm.select: every publish gets a confirm ack/nack with a
    %% monotonically increasing delivery tag (RQ-07).
    confirm = false :: boolean(),
    confirm_seq = 0 :: non_neg_integer()
}).

-record(st, {
    ref :: term(),
    transport :: module() | undefined,
    socket :: term() | undefined,
    buffer = <<>> :: binary(),
    phase = start :: start | tune | open | ready,
    client_id :: binary() | undefined,
    tenant = <<"t0">> :: binary(),
    principal = undefined :: binary() | undefined,
    frame_max = ?FRAME_MAX :: pos_integer(),
    channels = #{} :: #{non_neg_integer() => #ch{}},
    tick :: reference() | undefined,
    %% §4.2.7 heartbeat negotiation, in milliseconds (0 = disabled, which is
    %% what a proposal of 0 from either side means). `hb_ref' is the watchdog
    %% timer and `hb_misses' counts consecutive ticks with no inbound traffic:
    %% two of them (2*Hb of silence) means the peer is dead and the connection
    %% closes. The pull `tick' above is a different thing (the poll interval)
    %% and must not be reused for this.
    heartbeat = 0 :: non_neg_integer(),
    hb_ref :: reference() | undefined,
    hb_misses = 0 :: non_neg_integer(),
    %% {Partition, Offset} this connection released unacked (nack requeue,
    %% recover, consumer drop). The next delivery of the same position is
    %% redelivered=1 (RQ-11). Connection-local by design: a consumer on
    %% another connection cannot see it yet.
    redelivered = #{} :: #{non_neg_integer() => true},
    %% S12-B (ADR-032 D7): whether this connection holds a row in
    %% `connections_current'. Metrics may be off, in which case nothing was
    %% added and `connection_closed/1' must not subtract.
    counted = false :: boolean(),
    %% S12-B: node-level admission lease, taken once the AMQP protocol header
    %% proves the peer speaks 0-9-1 (before that there is no frame we may send),
    %% released in `terminate/2'.
    admission = undefined :: undefined | broker_admission:lease()
}).

%%%_* Lifecycle ------------------------------------------------------------

start_link(Ref, Transport, Opts) ->
    gen_server:start_link(?MODULE, {Ref, Transport, Opts}, []).

init({Ref, Transport, _Opts}) ->
    {ok, #st{ref = Ref, transport = Transport,
             counted = broker_metrics:connection_opened()}}.

terminate(_Reason, #st{counted = Counted} = St) ->
    ok = broker_metrics:connection_closed(Counted),
    ok = broker_admission:release(St#st.admission),
    cleanup(St),
    ok.

%%%_* gen_server -----------------------------------------------------------

handle_call(_Req, _From, St) ->
    {reply, {error, unsupported}, St}.

handle_cast(_Msg, St) ->
    {noreply, St}.

handle_info({handshake, _Ref, Transport, Socket, _Timeout}, #st{phase = start} = St) ->
    %% Do not send connection.start yet: the client sends the 8-byte protocol
    %% header (`AMQP 0 0 9 1') first, and the spec has the server answer start
    %% after seeing it. `parse/2' recognises the header and calls send_start/1.
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
handle_info({broker_deliver, ClientId, _MaxQoS, _Refs, {_P, _O, _Msg}},
            #st{client_id = ClientId} = St) ->
    {noreply, arm_tick(pull_all(St))};
handle_info(pull_tick, St) ->
    {noreply, arm_tick(pull_all(St#st{tick = undefined}))};
handle_info(heartbeat_tick, St) ->
    hb_tick(St);
handle_info(_Other, St) ->
    {noreply, St}.

code_change(_OldVsn, St, _Extra) -> {ok, St}.

%%%_* Input ----------------------------------------------------------------

on_data(Data, St) ->
    Result = parse(<<(St#st.buffer)/binary, Data/binary>>, St#st{buffer = <<>>}),
    case Result of
        %% Any inbound bytes prove the peer is alive: restart the heartbeat
        %% watchdog (§4.2.7). A heartbeat frame we receive counts too -- that is
        %% exactly what keeps two peers that are otherwise idle from timing out.
        {noreply, St1} -> rearm(arm_heartbeat(St1));
        Other -> Other
    end.

rearm(St) ->
    case set_active(St) of
        ok -> {noreply, St};
        {error, Reason} -> {stop, {socket_error, Reason}, St}
    end.

parse(<<"AMQP", 0, 0, 9, 1, Rest/binary>>, #st{phase = start} = St) ->
    %% S12-B (ADR-032 D7): the protocol header is the first moment the peer's
    %% version is known, and therefore the first moment a refusal frame is legal
    %% -- connection.close with a 0-9-1 reply code the client can parse.
    case admit(St) of
        {ok, St0} ->
            %% Protocol header: answer with connection.start and move on.
            {noreply, St1} = send_start(St0),
            parse(Rest, St1);
        {error, St0} ->
            refuse_full(St0)
    end;
parse(Buffer, St) ->
    case amqp_codec:decode_frame(Buffer, St#st.frame_max) of
        more ->
            {noreply, St#st{buffer = Buffer}};
        {ok, Frame, Rest} ->
            case handle_frame(Frame, St) of
                {noreply, St1} -> parse(Rest, St1);
                Stop -> Stop
            end;
        {error, {frame_too_large, Size, Max}} ->
            %% §4.2.3: a frame over the negotiated maximum is a FRAME_ERROR on
            %% the connection. Answering with a close (rather than dropping the
            %% socket) is what tells the client why.
            connection_error(501,
                             iolist_to_binary(
                               io_lib:format("FRAME_ERROR - frame size ~p over "
                                             "the negotiated maximum ~p",
                                             [Size, Max])),
                             0, 0, St);
        {error, Reason} ->
            {stop, {protocol_error, Reason}, St}
    end.

%% Every frame is processed inside one guard: a decode shape this adapter does
%% not understand must close the connection with 503 (the spec's frame-error
%% posture), not crash the process or continue on corrupt state (RQ-04/RQ-19).
%% The swallowed exception is logged -- a silent conversion would hide a real
%% bug behind a protocol error.
handle_frame(Frame, St) ->
    try do_handle_frame(Frame, St)
    catch
        Class:Reason:Stack ->
            logger:log(warning,
                       "amqp frame handling failed ~p:~p~n~p",
                       [Class, Reason, min_stack(Stack)]),
            connection_error(503, <<"FRAME_ERROR - unparseable frame">>, 0, 0, St)
    end.

min_stack(Stack) ->
    [E || E <- lists:sublist(Stack, 6)].

do_handle_frame(#{type := ?FRAME_HEARTBEAT}, St) ->
    {noreply, St};
do_handle_frame(#{type := ?FRAME_METHOD, channel := Channel,
                  payload := Payload}, St) ->
    {Class, Method, Args} = amqp_codec:decode_method(Payload),
    handle_method(Channel, Class, Method, Args, St);
do_handle_frame(#{type := ?FRAME_HEADER, channel := Channel,
                  payload := Payload}, St) ->
    handle_header(Channel, Payload, St);
do_handle_frame(#{type := ?FRAME_BODY, channel := Channel,
                  payload := Payload}, St) ->
    handle_body(Channel, Payload, St);
do_handle_frame(_Frame, St) ->
    %% AMQP 0-9-1 §2.3.5: the frame types are fixed; anything else is a
    %% protocol violation on the connection.
    connection_error(503, <<"FRAME_ERROR - unknown frame type">>, 0, 0, St).

%%%_* Handshake ------------------------------------------------------------

%% @doc Resolve the start-ok credential against the shared broker_security
%% identity table (S7) -- the same table MQTT uses.
%%
%% AMQP 0-9-1's PLAIN mechanism carries the response as `\0user\0password';
%% that is the shape this adapter reads.  `AMQPLAIN' and `EXTERNAL' are
%% accepted as "no credential offered" -- the former names a mechanism this
%% adapter does not implement, the latter defers to an external layer.
%%
%% A client that offers no usable credential stays anonymous unless the
%% deployment requires one.
authenticate(Mechanism, Response) ->
    case plain_credentials(Mechanism, Response) of
        {ok, {user_pass, User, Pass}} ->
            finish(broker_security:authenticate_conn({user_pass, User, Pass}));
        none ->
            case broker_security:auth_required(<<"amqp">>) of
                true -> {error, unauthenticated};
                false -> {ok, anonymous_identity()}
            end
    end.

plain_credentials(<<"PLAIN">>, Response) when is_binary(Response) ->
    case binary:split(Response, <<0>>, [global]) of
        %% An empty user AND password is what a client with no credentials
        %% sends -- that is "no credential offered", not a wrong one.
        [_, <<>>, <<>>] -> none;
        [_, User, Pass] -> {ok, {user_pass, User, Pass}};
        _ -> none
    end;
plain_credentials(_Mechanism, _Response) ->
    none.

finish({ok, Identity}) -> {ok, Identity};
finish({error, _}) -> {error, unauthenticated}.

anonymous_identity() ->
    #{principal => undefined, tenant_id => broker_security:default_tenant()}.

send_start(St) ->
    %% RQ-21: capabilities only name what is actually implemented. Everything
    %% not listed here (consumer_cancel_notify, direct_reply_to,
    %% exchange_exchange_bindings, per_consumer_qos, ...) is intentionally
    %% absent because the behavior behind it does not exist yet.
    %% The capability map (not a pre-encoded table) so `entry/2' emits a $F
    %% field table, which is what clients read server-properties.capabilities as.
    Props = amqp_codec:encode_table(
              #{<<"capabilities">> =>
                    #{<<"publisher_confirms">> => true,
                      <<"basic.nack">> => true,
                      <<"authentication_failure_close">> => true},
                <<"product">> => <<"UnifiedMQ">>,
                <<"version">> => <<"0.1.0">>,
                <<"platform">> => <<"Erlang">>}),
    Args = <<0:8, 9:8, Props/binary,
             (amqp_codec:encode_longstr(<<"PLAIN">>))/binary,
             (amqp_codec:encode_longstr(<<"en_US">>))/binary>>,
    {noreply, send_method(0, 10, 10, Args, St)}.

handle_method(0, 10, 11, Args, #st{phase = start} = St) ->
    %% connection.start-ok: client-properties, mechanism, response, locale.
    {_Props, Rest1} = amqp_codec:get_table(Args),
    {Mechanism, Rest2} = amqp_codec:get_shortstr(Rest1),
    {Response, Rest3} = amqp_codec:get_longstr(Rest2),
    {_Locale, _} = amqp_codec:get_shortstr(Rest3),
    case authenticate(Mechanism, Response) of
        {ok, Identity} ->
            send_tune(St#st{phase = tune, tenant = maps:get(tenant_id, Identity),
                            principal = maps:get(principal, Identity)});
        {error, _} ->
            %% authentication_failure_close (RQ-20): the close frame is
            %% reply-code(short) + reply-text(shortstr) + class-id + method-id.
            %% The old build encoded the reply CODE as a shortstr string, which
            %% every real client failed to parse.
            connection_error(403,
                             <<"ACCESS_REFUSED - Login was refused using "
                               "authentication mechanism PLAIN.">>,
                             0, 0, St)
    end;
handle_method(0, 10, 31, Args, #st{phase = tune} = St) ->
    %% connection.tune-ok: channel-max, frame-max, heartbeat. The client may
    %% lower the heartbeat we proposed; 0 from either side disables it.
    <<_ChannelMax:16, FrameMax:32, ClientHb:16>> = Args,
    ServerHb = server_heartbeat(),
    Negotiated = case min(ServerHb, ClientHb) of
                     0 -> 0;
                     Hb -> Hb
                 end,
    %% Stored in milliseconds for the watchdog timer; 0 stays 0 (disabled).
    St1 = St#st{phase = open,
                frame_max = min(?FRAME_MAX, max(4096, FrameMax)),
                heartbeat = Negotiated * 1000},
    %% The connection is established now: start the heartbeat watchdog.
    {noreply, arm_heartbeat(St1)};
handle_method(0, 10, 40, Args, #st{phase = open} = St) ->
    {_VHost, Rest1} = amqp_codec:get_shortstr(Args),
    {_Capabilities, _Rest2} = amqp_codec:get_shortstr(Rest1),
    Args2 = amqp_codec:encode_shortstr(<<>>),
    St1 = send_method(0, 10, 41, Args2, St#st{phase = ready}),
    {noreply, arm_tick(ensure_client(St1))};
handle_method(0, 10, 50, _Args, St) ->
    %% connection.close -> close-ok then stop.
    _ = send_method(0, 10, 51, <<>>, St),
    {stop, normal, St};
handle_method(0, 10, 51, _Args, St) ->
    {stop, normal, St};
handle_method(Channel, 20, 10, _Args, #st{phase = ready} = St) ->
    %% channel.open -> open-ok.
    Ch = maps:get(Channel, St#st.channels, #ch{}),
    {noreply, send_method(Channel, 20, 11, amqp_codec:encode_longstr(<<>>),
                          St#st{channels = (St#st.channels)#{Channel => Ch}})};
handle_method(Channel, 20, 40, _Args, St) ->
    %% channel.close from the client -> close-ok.
    {noreply, send_method(Channel, 20, 41, <<>>, drop_channel(Channel, St))};
handle_method(_Channel, 20, 41, _Args, St) ->
    %% channel.close-ok (our answer to a server-side channel error).
    {noreply, St};
handle_method(Channel, Class, Method, Args, #st{phase = ready} = St) ->
    handle_channel_method(Channel, Class, Method, Args, St);
%% AMQP 0-9-1: an unknown method is an exception, never a silent drop.
%% Channel-level 540 NOT_IMPLEMENTED for channels, connection-level 503 for
%% channel 0 (RQ-19).
handle_method(0, Class, Method, _Args, St) ->
    connection_error(503,
                     unknown_method_text(Class, Method, <<"COMMAND_INVALID">>),
                     Class, Method, St);
handle_method(Channel, Class, Method, _Args, St) ->
    channel_error(Channel, 540,
                  unknown_method_text(Class, Method, <<"NOT_IMPLEMENTED">>),
                  Class, Method, St).

unknown_method_text(Class, Method, Prefix) ->
    iolist_to_binary(
      io_lib:format("~ts - method ~w-~w is not implemented", [Prefix, Class, Method])).

handle_channel_method(Channel, 40, 10, Args, St) ->
    %% exchange.declare
    <<_Ticket:16, Rest0/binary>> = Args,
    {Exchange, Rest1} = amqp_codec:get_shortstr(Rest0),
    {Type, Rest2} = amqp_codec:get_shortstr(Rest1),
    {[Passive, Durable, _Auto, _Internal, NoWait], Rest3} = amqp_codec:get_bits(Rest2, 5),
    {_Table, _} = amqp_codec:get_table(Rest3),
    case {Passive, amqp_topo:exchange_type(Exchange)} of
        {true, {error, no_exchange}} ->
            channel_error(Channel, 404, <<"NOT_FOUND - no exchange">>, 40, 10, St);
        _ ->
            case amqp_topo:declare_exchange(Exchange, Type, Durable) of
                ok -> {noreply, reply_ok(Channel, 40, 11, NoWait, St)};
                {ok, _} -> {noreply, reply_ok(Channel, 40, 11, NoWait, St)};
                {error, Reason} -> channel_error(Channel, 406, reason(Reason), 40, 10, St)
            end
    end;
handle_channel_method(Channel, 40, 20, Args, St) ->
    %% exchange.delete
    <<_Ticket:16, Rest0/binary>> = Args,
    {Exchange, Rest1} = amqp_codec:get_shortstr(Rest0),
    {[_IfUnused, NoWait], _} = amqp_codec:get_bits(Rest1, 2),
    ok = amqp_topo:delete_exchange(Exchange),
    {noreply, reply_ok(Channel, 40, 21, NoWait, St)};
handle_channel_method(Channel, 50, 10, Args, St) ->
    %% queue.declare
    <<_Ticket:16, Rest0/binary>> = Args,
    {Queue0, Rest1} = amqp_codec:get_shortstr(Rest0),
    {[Passive, Durable, Exclusive, _Auto, NoWait], Rest2} = amqp_codec:get_bits(Rest1, 5),
    {_Table, _} = amqp_codec:get_table(Rest2),
    case {Passive, amqp_topo:queue_exists(Queue0)} of
        {true, false} ->
            channel_error(Channel, 404, <<"NOT_FOUND - no queue">>, 50, 10, St);
        _ ->
            case amqp_topo:declare_queue(Queue0, Durable, Exclusive) of
                {ok, Queue} ->
                    Count = consumer_count(Queue, St),
                    ReplyArgs = <<(amqp_codec:encode_shortstr(Queue))/binary, 0:32, Count:32>>,
                    {noreply, reply_ok(Channel, 50, 11, NoWait, ReplyArgs, St)};
                {error, Reason} ->
                    channel_error(Channel, 406, reason(Reason), 50, 10, St)
            end
    end;
handle_channel_method(Channel, 50, 20, Args, St) ->
    %% queue.bind
    <<_Ticket:16, Rest0/binary>> = Args,
    {Queue, Rest1} = amqp_codec:get_shortstr(Rest0),
    {Exchange, Rest2} = amqp_codec:get_shortstr(Rest1),
    {RK, Rest3} = amqp_codec:get_shortstr(Rest2),
    {[NoWait], Rest4} = amqp_codec:get_bits(Rest3, 1),
    {Table, _} = amqp_codec:get_table(Rest4),
    case amqp_topo:bind(Exchange, Queue, RK, Table) of
        ok -> {noreply, reply_ok(Channel, 50, 21, NoWait, St)};
        {error, Reason} -> channel_error(Channel, 404, reason(Reason), 50, 20, St)
    end;
handle_channel_method(Channel, 50, 50, Args, St) ->
    %% queue.unbind: a missing binding is NOT_FOUND, not a crash (RQ-19).
    <<_Ticket:16, Rest0/binary>> = Args,
    {Queue, Rest1} = amqp_codec:get_shortstr(Rest0),
    {Exchange, Rest2} = amqp_codec:get_shortstr(Rest1),
    {RK, Rest3} = amqp_codec:get_shortstr(Rest2),
    {Table, _} = amqp_codec:get_table(Rest3),
    case amqp_topo:unbind(Exchange, Queue, RK, Table) of
        ok -> {noreply, send_method(Channel, 50, 51, <<>>, St)};
        {error, Reason} -> channel_error(Channel, 404, reason(Reason), 50, 50, St)
    end;
handle_channel_method(Channel, 50, 30, Args, St) ->
    %% queue.purge
    <<_Ticket:16, Rest0/binary>> = Args,
    {_Queue, Rest1} = amqp_codec:get_shortstr(Rest0),
    {[NoWait], _} = amqp_codec:get_bits(Rest1, 1),
    {noreply, reply_ok(Channel, 50, 31, NoWait, <<0:32>>, St)};
handle_channel_method(Channel, 50, 40, Args, St) ->
    %% queue.delete
    <<_Ticket:16, Rest0/binary>> = Args,
    {Queue, Rest1} = amqp_codec:get_shortstr(Rest0),
    {[IfUnused, _IfEmpty, NoWait], _} = amqp_codec:get_bits(Rest1, 3),
    case IfUnused andalso consumer_count(Queue, St) > 0 of
        true -> channel_error(Channel, 406, <<"PRECONDITION_FAILED - queue in use">>, 50, 40, St);
        false ->
            ok = amqp_topo:delete_queue(Queue),
            {noreply, reply_ok(Channel, 50, 41, NoWait, <<0:32>>, St)}
    end;
handle_channel_method(Channel, 60, 10, Args, St) ->
    %% basic.qos
    <<_PrefetchSize:32, PrefetchCount:16, _Global:8>> = Args,
    Ch = ch(Channel, St),
    {noreply, send_method(Channel, 60, 11, <<>>,
                          put_ch(Channel, Ch#ch{prefetch = PrefetchCount}, St))};
handle_channel_method(Channel, 60, 20, Args, St) ->
    %% basic.consume
    <<_Ticket:16, Rest0/binary>> = Args,
    {Queue, Rest1} = amqp_codec:get_shortstr(Rest0),
    {Tag0, Rest2} = amqp_codec:get_shortstr(Rest1),
    {[NoLocal, NoAck, _Exclusive, NoWait], Rest3} = amqp_codec:get_bits(Rest2, 4),
    {_Table, _} = amqp_codec:get_table(Rest3),
    Tag = case Tag0 of
              <<>> -> <<"ctag-", (integer_to_binary(erlang:unique_integer([positive])))/binary>>;
              _ -> Tag0
          end,
    case amqp_topo:queue_exists(Queue) of
        false ->
            channel_error(Channel, 404, <<"NOT_FOUND - no queue">>, 60, 20, St);
        true ->
            Filter = queue_filter(Queue),
            %% RQ-01: an AMQP queue is queue state, not a live feed -- the
            %% group anchors at 0 at entity birth so messages published before
            %% the first consumer joined stay deliverable.
            case broker_subscription:join(shared, Queue, St#st.client_id,
                                          Filter, 1, St#st.tenant,
                                          #{anchor => earliest}) of
                {ok, Ref} ->
                    Consumer = #{ref => Ref, queue => Queue, no_ack => NoAck,
                                 no_local => NoLocal},
                    Ch = ch(Channel, St),
                    Ch1 = Ch#ch{consumers = (Ch#ch.consumers)#{Tag => Consumer}},
                    St1 = put_ch(Channel, Ch1, St),
                    St2 = reply_ok(Channel, 60, 21, NoWait,
                                   amqp_codec:encode_shortstr(Tag), St1),
                    {noreply, arm_tick(pull_all(St2))};
                {error, Reason} ->
                    channel_error(Channel, 406, reason(Reason), 60, 20, St)
            end
    end;
handle_channel_method(Channel, 60, 30, Args, St) ->
    %% basic.cancel: an unknown consumer tag is a 404 channel error
    %% (RabbitMQ behavior; the old build silently answered success).
    {Tag, Rest1} = amqp_codec:get_shortstr(Args),
    {[NoWait], _} = amqp_codec:get_bits(Rest1, 1),
    Ch = ch(Channel, St),
    case maps:take(Tag, Ch#ch.consumers) of
        {#{ref := Ref}, Consumers} ->
            _ = (catch broker_subscription:leave(Ref, St#st.client_id)),
            St1 = put_ch(Channel, Ch#ch{consumers = Consumers}, St),
            {noreply, reply_ok(Channel, 60, 31, NoWait, amqp_codec:encode_shortstr(Tag), St1)};
        error ->
            channel_error(Channel, 404,
                          <<"NOT_FOUND - no consumer tag ">>, 60, 30, St)
    end;
handle_channel_method(Channel, 60, 40, Args, St) ->
    %% basic.publish: remember exchange/routing key/mandatory until the header
    %% and body frames arrive. `immediate' was removed from mainstream clients
    %% and RabbitMQ refuses it; refusing with 540 is the honest answer (RQ-05).
    <<_Ticket:16, Rest0/binary>> = Args,
    {Exchange, Rest1} = amqp_codec:get_shortstr(Rest0),
    {RK, Rest2} = amqp_codec:get_shortstr(Rest1),
    {[Mandatory, Immediate], _} = amqp_codec:get_bits(Rest2, 2),
    case Immediate of
        true ->
            channel_error(Channel, 540,
                          <<"NOT_IMPLEMENTED - immediate publishing">>, 60, 40, St);
        false ->
            Ch = ch(Channel, St),
            {noreply,
             put_ch(Channel, Ch#ch{pending = {Exchange, RK, bitval(Mandatory), 0, <<>>}}, St)}
    end;
handle_channel_method(Channel, 60, 70, Args, St) ->
    %% basic.get: claim one message off the queue, or get-empty when it has
    %% none (RQ-09; the old build always answered get-empty).
    <<_Ticket:16, Rest0/binary>> = Args,
    {Queue, Rest1} = amqp_codec:get_shortstr(Rest0),
    {[NoAck], _} = amqp_codec:get_bits(Rest1, 1),
    case amqp_topo:queue_exists(Queue) of
        false ->
            channel_error(Channel, 404, <<"NOT_FOUND - no queue">>, 60, 70, St);
        true ->
            {noreply, basic_get(Channel, Queue, bitval(NoAck), St)}
    end;
handle_channel_method(Channel, 60, 80, Args, St) ->
    %% basic.ack
    <<Tag:64, Rest/binary>> = Args,
    {[Multiple], _} = amqp_codec:get_bits(Rest, 1),
    {noreply, ack_deliveries(Channel, Tag, Multiple, ack, 80, St)};
handle_channel_method(Channel, 60, 90, Args, St) ->
    %% basic.reject
    <<Tag:64, Rest/binary>> = Args,
    {[Requeue], _} = amqp_codec:get_bits(Rest, 1),
    {noreply, ack_deliveries(Channel, Tag, false,
                             case Requeue of true -> release; false -> ack end,
                             90, St)};
handle_channel_method(Channel, 60, 120, Args, St) ->
    %% basic.nack
    <<Tag:64, Rest/binary>> = Args,
    {[Multiple, Requeue], _} = amqp_codec:get_bits(Rest, 2),
    {noreply, ack_deliveries(Channel, Tag, Multiple,
                             case Requeue of true -> release; false -> ack end,
                             120, St)};
handle_channel_method(Channel, 60, 110, _Args, St) ->
    %% basic.recover: requeue every unacked delivery of the channel; the next
    %% delivery of each is redelivered=1 (RQ-10/RQ-11; the old build only
    %% answered recover-ok).
    {noreply, basic_recover(Channel, St)};
%% confirm.select (class 85): switch the channel to confirm mode; every
%% completed publish then owes the client a basic.ack/basic.nack (RQ-07; the
%% old build dropped the method silently and clients waited forever).
handle_channel_method(Channel, 85, 10, Args, St) ->
    {[NoWait], _} = amqp_codec:get_bits(Args, 1),
    Ch = ch(Channel, St),
    %% confirm.select is idempotent: a second call must NOT reset the
    %% sequence. Resetting would reissue tags the client was already promised
    %% (confirm tags start at 1 per channel), so two publishes would share a
    %% tag and the client's outstanding-confirm bookkeeping would desync.
    Seq = case Ch#ch.confirm of true -> Ch#ch.confirm_seq; false -> 0 end,
    St1 = put_ch(Channel, Ch#ch{confirm = true, confirm_seq = Seq}, St),
    {noreply, reply_ok(Channel, 85, 11, NoWait, St1)};
handle_channel_method(Channel, 90, Method, _Args, St) ->
    %% tx.select/commit/rollback (class 90): transactions are not implemented,
    %% and the spec forbids pretending they are -- refuse with 540 (RQ-08).
    {noreply, send_channel_error(Channel, 540,
                                 <<"NOT_IMPLEMENTED - AMQP transactions">>,
                                 90, Method, St)};
handle_channel_method(Channel, Class, Method, _Args, St) ->
    %% A method inside a known class but not implemented by this adapter.
    {noreply, send_channel_error(Channel, 540,
                                 unknown_method_text(Class, Method,
                                                     <<"NOT_IMPLEMENTED">>),
                                 Class, Method, St)}.

reply_ok(_Channel, _Class, _Method, true, St) ->
    St;
reply_ok(Channel, Class, Method, false, St) ->
    send_method(Channel, Class, Method, <<>>, St).

reply_ok(Channel, Class, Method, true, _Args, St) ->
    send_method(Channel, Class, Method, <<>>, St),
    St;
reply_ok(Channel, Class, Method, false, Args, St) ->
    send_method(Channel, Class, Method, Args, St).

handle_header(Channel, Payload, St) ->
    case amqp_codec:decode_content_header(Payload) of
        {60, _Weight, BodySize, PropsBin} ->
            Ch = ch(Channel, St),
            case Ch#ch.pending of
                {Exchange, RK, Mandatory, _Old, _Acc} ->
                    {Props, _Rest} = amqp_codec:decode_properties(PropsBin),
                    Ch1 = Ch#ch{pending = {Exchange, RK, Mandatory, BodySize, <<>>},
                                pending_props = Props,
                                headers = props_to_headers(Props)},
                    St1 = put_ch(Channel, Ch1, St),
                    case BodySize of
                        0 -> {noreply, complete_publish(Channel, St1)};
                        _ -> {noreply, St1}
                    end;
                undefined ->
                    {noreply, St}
            end;
        _ ->
            {noreply, St}
    end.

%% AMQP basic-properties -> the broker's protocol-neutral header map. The
%% `headers' table is exactly where a CloudEvents client puts `ce-*' keys
%% (project binding, docs/protocol-compatibility.md §4.10); `content_type'
%% becomes the ordinary `content-type' header so the shared binding decoder
%% sees the same shape as the other protocols. The FULL property map also
%% rides `proto_meta' (see complete_publish) so a consumer reads back
%% correlation_id, reply_to & co. unchanged (RQ-03).
props_to_headers(Props) ->
    Table = case maps:get(headers, Props, #{}) of
                T when is_map(T) -> T;
                _ -> #{}
            end,
    case maps:get(content_type, Props, undefined) of
        undefined -> Table;
        CT -> Table#{<<"content-type">> => CT}
    end.

headers_to_props(#{<<"content-type">> := CT} = Headers) ->
    Table = maps:remove(<<"content-type">>, Headers),
    #{content_type => CT, headers => Table};
headers_to_props(Headers) when map_size(Headers) > 0 ->
    #{headers => Headers};
headers_to_props(_) ->
    #{}.

handle_body(Channel, Payload, St) ->
    Ch = ch(Channel, St),
    case Ch#ch.pending of
        {Exchange, RK, Mandatory, Size, Acc} ->
            Acc1 = <<Acc/binary, Payload/binary>>,
            case byte_size(Acc1) >= Size of
                true ->
                    <<Body:Size/binary, _/binary>> = Acc1,
                    Ch1 = Ch#ch{pending = {Exchange, RK, Mandatory, Size, Body}},
                    St1 = put_ch(Channel, Ch1, St),
                    {noreply, complete_publish(Channel, St1)};
                false ->
                    {noreply,
                     put_ch(Channel, Ch#ch{pending = {Exchange, RK, Mandatory, Size, Acc1}}, St)}
            end;
        undefined ->
            {noreply, St}
    end.

complete_publish(Channel, St0) ->
    Ch0 = ch(Channel, St0),
    case Ch0#ch.pending of
        {Exchange, RK, Mandatory, _Size, Body} ->
            Props = Ch0#ch.pending_props,
            Headers = Ch0#ch.headers,
            {St1, StoreOk} = publish(Channel, Exchange, RK, Mandatory, Body,
                                     Headers, Props, St0),
            Ch1 = ch(Channel, St1),
            Ch2 = Ch1#ch{pending = undefined, headers = #{},
                         pending_props = #{}},
            St2 = put_ch(Channel, Ch2, St1),
            %% Confirm mode (RQ-07): a monotonically increasing tag answers
            %% every publish -- ack after the store, nack on failure. A
            %% mandatory unroutable message still gets its ack after the
            %% return, matching RabbitMQ.
            St3 = case Ch2#ch.confirm andalso maps:is_key(Channel, St2#st.channels) of
                      true ->
                          Tag = Ch2#ch.confirm_seq + 1,
                          ConfirmSt = put_ch(Channel, Ch2#ch{confirm_seq = Tag}, St2),
                          case StoreOk of
                              true ->
                                  send_method(Channel, 60, 80, <<Tag:64, 0:8>>, ConfirmSt);
                              false ->
                                  send_method(Channel, 60, 120, <<Tag:64, 0:8, 0:8>>,
                                              ConfirmSt)
                          end;
                      false ->
                          St2
                  end,
            St3;
        undefined ->
            St0
    end.

%%%_* Publish ---------------------------------------------------------------

%% Returns {St, StoreOk}. A mandatory publish with no queue gets basic.return
%% before anything is stored (RQ-05: the old build dropped it silently).
publish(Channel, Exchange, RK, Mandatory, Body, Headers, Props, St0) ->
    Queues = amqp_topo:queues_for(Exchange, RK),
    St1 = case Mandatory =:= 1 andalso Queues =:= [] of
              true -> basic_return(Channel, Exchange, RK, Body, Props, St0);
              false -> St0
          end,
    {St2, Ok} =
        lists:foldl(
          fun(Q, {Acc, AllOk}) ->
                  case publish_to_queue(Channel, Q, Exchange, RK, Body,
                                        Headers, Props, Acc) of
                      {ok, Acc1} -> {Acc1, AllOk};
                      {{error, Reason}, Acc1} ->
                          logger:log(warning, "amqp publish to ~ts failed: ~p",
                                     [Q, Reason]),
                          {Acc1, false}
                  end
          end, {St1, true}, Queues),
    {St2, Ok}.

basic_return(Channel, Exchange, RK, Body, Props, St) ->
    %% basic.return = reply-code + reply-text + exchange + routing-key, then
    %% the content header/body frames of the returned message.
    Args = <<312:16, (amqp_codec:encode_shortstr(<<"NO_ROUTE">>))/binary,
             (amqp_codec:encode_shortstr(Exchange))/binary,
             (amqp_codec:encode_shortstr(RK))/binary>>,
    St1 = send_method(Channel, 60, 50, Args, St),
    send_content(Channel, Body, Props, St1).

publish_to_queue(Channel, Queue, Exchange, RK, Body, Headers, Props, St) ->
    Destination = queue_filter(Queue),
    Meta0 = #{<<"amqp">> => #{<<"exchange">> => Exchange,
                              <<"routing_key">> => RK,
                              <<"queue">> => Queue}},
    %% RQ-03: the full basic-property map travels in `proto_meta', so a
    %% consumer reads back what the publisher sent (RPC needs this).
    Meta = case Props of
               P when is_map(P), map_size(P) > 0 -> Meta0#{<<"amqp_props">> => P};
               _ -> Meta0
           end,
    Msg = #broker_message{message_id = message_id(),
                          tenant_id = St#st.tenant,
                          destination = Destination,
                          partition_id = 0,
                          payload = Body,
                          payload_encoding = raw,
                          headers = Headers,
                          proto_meta = Meta,
                          persistence = durable,
                          reliability = fsync,
                          created_at = erlang:system_time(millisecond),
                          expires_at = undefined,
                          event = undefined},
    Ctx = #route_ctx{tenant_id = St#st.tenant,
                     publisher = St#st.client_id,
                     reliability = fsync},
    case broker_event:apply_policy(Msg) of
        {ok, Accepted} ->
            case route_and_store(Destination, Accepted, Ctx) of
                {ok, _Stored} -> {ok, St};
                {error, Reason} -> {{error, Reason}, St}
            end;
        {error, EventReason} ->
            %% AMQP 0-9-1 basic.publish has no per-message negative ack; the
            %% honest signal is a channel error, not a silent drop.
            Text = iolist_to_binary(
                     io_lib:format("cloudevent rejected: ~p", [EventReason])),
            St1 = send_channel_error(Channel, 406, Text, 60, 40, St),
            {{error, {cloudevent, EventReason}}, St1}
    end.

route_and_store(Destination, Msg, Ctx) ->
    case broker_router:env() of
        {ok, Env} ->
            Backend = application:get_env(broker_router, backend, broker_router_hash),
            case Backend:route(Destination, Ctx, Env) of
                {ok, Routes} -> store_routes(Routes, Msg, 0);
                {error, Reason} -> {error, {route, Reason}}
            end;
        {error, Reason} ->
            {error, {router_env, Reason}}
    end.

store_routes([], _Msg, Stored) when Stored > 0 ->
    {ok, Stored};
store_routes([], _Msg, 0) ->
    %% Routed but nothing stored: not an append failure per se, but nothing
    %% durable happened, so a confirm must not ack it.
    {error, nothing_stored};
store_routes([{P, Refs} | Rest], Msg, Stored) ->
    PartitionMsg = Msg#broker_message{partition_id = P},
    case (catch broker_queue:append(P, PartitionMsg, fsync)) of
        {ok, Offset} ->
            _ = (catch broker_subscription:dispatch(Refs,
                                                    {P, Offset, PartitionMsg})),
            store_routes(Rest, Msg, Stored + 1);
        _ ->
            store_routes(Rest, Msg, Stored)
    end.

%%%_* Delivery --------------------------------------------------------------

pull_all(St) ->
    Consumers = all_consumers(St),
    lists:foldl(fun({Channel, Tag, Consumer}, Acc) ->
                        pull_consumer(Channel, Tag, Consumer, Acc)
                end, St, Consumers).

all_consumers(St) ->
    lists:flatmap(
      fun({Channel, #ch{consumers = Cs}}) ->
              [{Channel, Tag, C} || {Tag, C} <- maps:to_list(Cs)]
      end, maps:to_list(St#st.channels)).

pull_consumer(Channel, _Tag, #{ref := Ref, no_ack := NoAck, queue := Queue}, St) ->
    lists:foldl(
      fun(P, Acc) ->
              case room(Channel, NoAck, Acc) of
                  0 -> Acc;
                  Max -> pull_partition(Channel, Ref, Queue, NoAck, Max, P, Acc)
              end
      end, St, broker_queue:partition_ids()).

%% basic.qos prefetch counts unacked deliveries on the channel. NoAck
%% consumers are not limited (AMQP says prefetch applies only when acks are on).
room(_Channel, true, _St) ->
    32;
room(Channel, false, St) ->
    Ch = ch(Channel, St),
    case Ch#ch.prefetch of
        0 -> 32;
        N -> max(0, N - maps:size(Ch#ch.deliveries))
    end.

pull_partition(Channel, Ref, Queue, NoAck, Max, P, St) ->
    case (catch broker_subscription:claim(Ref, P, Max, St#st.client_id)) of
        {ok, []} ->
            St;
        {ok, Offsets} ->
            {Read, Missing} = read_offsets(P, Offsets),
            Expected = queue_filter(Queue),
            St1 = lists:foldl(
                    fun({O, Msg}, Acc) ->
                            case Msg#broker_message.destination =:= Expected of
                                true ->
                                    deliver(Channel, Ref, Queue, NoAck, P, O, Msg, Acc);
                                false ->
                                    %% Another AMQP queue's message landed in the
                                    %% same partition. A shared cursor scans the
                                    %% whole partition, so consume the offset for
                                    %% this group without delivering it.
                                    _ = (catch broker_subscription:ack(
                                                 Ref, P, O, St#st.client_id)),
                                    Acc
                            end
                    end, St, Read),
            case Missing of
                [] -> ok;
                _ -> _ = (catch broker_subscription:release(Ref, P, Missing, St1#st.client_id))
            end,
            St1;
        _ ->
            St
    end.

%% Ascending offset order, both lists: the claim comes back sorted, and the
%% deliver loop must not hand a 100-message backlog to the client backwards
%% (a latent pre-RQ-01 bug that only batches > 1 per round could expose).
read_offsets(P, Offsets) ->
    {Read, Missing} = lists:foldl(
                        fun(O, {Acc, Miss}) ->
                                case (catch broker_queue:read_from(P, O, 1)) of
                                    {ok, [{O, Msg}], _Next} ->
                                        {[{O, Msg} | Acc], Miss};
                                    _ -> {Acc, [O | Miss]}
                                end
                        end, {[], []}, lists:sort(Offsets)),
    {lists:reverse(Read), lists:reverse(Missing)}.

deliver(Channel, Ref, _Queue, NoAck, P, O, Msg, St) ->
    Ch = ch(Channel, St),
    Tag = Ch#ch.next_tag,
    Consumers = [T || {T, #{ref := R}} <- maps:to_list(Ch#ch.consumers), R =:= Ref],
    case Consumers of
        [] ->
            _ = (catch broker_subscription:release(Ref, P, [O], St#st.client_id)),
            mark_redelivered(P, O, St);
        [ConsumerTag | _] ->
            #{<<"amqp">> := #{<<"exchange">> := Exchange,
                              <<"routing_key">> := RK}} = Msg#broker_message.proto_meta,
            %% RQ-11: a position this connection already delivered once and
            %% released unacked is redelivered=1; first delivery stays 0.
            Redelivered = maps:is_key({P, O}, St#st.redelivered),
            DeliverArgs = <<(amqp_codec:encode_shortstr(ConsumerTag))/binary,
                            Tag:64, (bitval(Redelivered)):8,
                           (amqp_codec:encode_shortstr(Exchange))/binary,
                           (amqp_codec:encode_shortstr(RK))/binary>>,
            St1 = send_method(Channel, 60, 60, DeliverArgs, St),
            St2 = send_content(Channel, Msg#broker_message.payload,
                               props_of(Msg), St1),
            case NoAck of
                true ->
                    _ = (catch broker_subscription:ack(Ref, P, O, St2#st.client_id)),
                    put_ch(Channel, Ch#ch{next_tag = Tag + 1}, St2);
                false ->
                    Ch1 = Ch#ch{next_tag = Tag + 1,
                                deliveries = (Ch#ch.deliveries)#{Tag => {Ref, P, O}}},
                    put_ch(Channel, Ch1, St2)
            end
    end.

%% RQ-03: a message published over AMQP carries its original basic properties
%% in `proto_meta'; anything else falls back to the header map.
props_of(#broker_message{proto_meta = Meta} = Msg) ->
    case maps:get(<<"amqp_props">>, Meta, undefined) of
        P when is_map(P) -> P;
        _ -> headers_to_props(Msg#broker_message.headers)
    end.

%% RQ-09 (unblocked by RQ-01): claim one ready message off the queue. The
%% membership is created with `anchor => earliest' only when the group is new,
%% so pre-consumer backlog is readable, and is kept for as long as the
%% delivery can come back for an ack (see #ch.gets) -- `leave/2' releases
%% claimed-but-unacked offsets, so leaving early would hand the message to
%% another consumer while this channel still owes the ack.
basic_get(Channel, Queue, NoAck, St0) ->
    Client = St0#st.client_id,
    case (catch broker_subscription:join(shared, Queue, Client,
                                         queue_filter(Queue), 1,
                                         St0#st.tenant,
                                         #{anchor => earliest})) of
        {ok, Ref} ->
            get_scan(Channel, Ref, Queue, NoAck, broker_queue:partition_ids(), St0);
        _ ->
            get_empty(Channel, St0)
    end.

get_scan(Channel, Ref, _Queue, _NoAck, [], St) ->
    %% Nothing ready anywhere: drop the transient membership, answer empty.
    _ = (catch broker_subscription:leave(Ref, St#st.client_id)),
    get_empty(Channel, St);
get_scan(Channel, Ref, Queue, NoAck, [P | Ps], St0) ->
    Client = St0#st.client_id,
    case (catch broker_subscription:claim(Ref, P, 1, Client)) of
        {ok, [_ | _] = Offsets} ->
            {Read, Missing} = read_offsets(P, Offsets),
            _ = case Missing of
                    [] -> ok;
                    _ -> (catch broker_subscription:release(Ref, P, Missing, Client))
                end,
            Expected = queue_filter(Queue),
            case [{O, M} || {O, M} <- Read,
                            M#broker_message.destination =:= Expected] of
                [{O, Msg} | _] ->
                    get_deliver(Channel, Ref, Queue, NoAck, P, O, Msg, St0);
                [] ->
                    %% The offsets were another queue's traffic (consumed
                    %% above); keep scanning the remaining partitions.
                    get_scan(Channel, Ref, Queue, NoAck, Ps, St0)
            end;
        _ ->
            get_scan(Channel, Ref, Queue, NoAck, Ps, St0)
    end.

get_deliver(Channel, Ref, Queue, NoAck, P, O, Msg, St0) ->
    Ch = ch(Channel, St0),
    Tag = Ch#ch.next_tag,
    logger:log(warning, "DBG get_deliver tag=~p off=~p q=~p", [Tag, O, Queue]),
    #{<<"amqp">> := #{<<"exchange">> := Exchange,
                      <<"routing_key">> := RK}} = Msg#broker_message.proto_meta,
    Redelivered = maps:is_key({P, O}, St0#st.redelivered),
    %% message-count 0: the ready count is not tracked per queue yet.
    GetOkArgs = <<(amqp_codec:encode_shortstr(Queue))/binary,
                  Tag:64, (bitval(Redelivered)):8,
                  (amqp_codec:encode_shortstr(Exchange))/binary,
                  (amqp_codec:encode_shortstr(RK))/binary, 0:32>>,
    St1 = send_method(Channel, 60, 71, GetOkArgs, St0),
    St2 = send_content(Channel, Msg#broker_message.payload, props_of(Msg), St1),
    %% `NoAck' is `bitval/1''s output (0 | 1) -- the call site normalises it --
    %% so comparing against `true' here was dead: dialyzer proved it can never
    %% match, which is exactly the kind of half-normalised check that hides a
    %% real bug later.
    case NoAck =:= 1 of
        true ->
            _ = (catch broker_subscription:ack(Ref, P, O, St2#st.client_id)),
            put_ch(Channel, Ch#ch{next_tag = Tag + 1}, St2);
        _ ->
            put_ch(Channel, Ch#ch{next_tag = Tag + 1,
                                  deliveries =
                                      (Ch#ch.deliveries)#{Tag => {Ref, P, O}}},
                   St2)
    end.

get_empty(Channel, St) ->
    send_method(Channel, 60, 72, amqp_codec:encode_shortstr(<<>>), St).

%% RQ-10: recover requeues every unacked delivery of the channel and each
%% subsequent delivery of those positions is redelivered=1 (RQ-11). basic.get
%% deliveries resolve their transient membership too.
basic_recover(Channel, St0) ->
    Ch = ch(Channel, St0),
    Red = lists:foldl(
            fun({_T, {Ref, P, O}}, Acc) ->
                    _ = (catch broker_subscription:release(Ref, P, [O],
                                                           St0#st.client_id)),
                    Acc#{{P, O} => true}
            end, St0#st.redelivered, maps:to_list(Ch#ch.deliveries)),
    St1 = put_ch(Channel, Ch#ch{deliveries = #{}},
                 St0#st{redelivered = Red}),
    send_method(Channel, 60, 111, <<>>, St1).

%% RQ-19: an ack/nack/reject for a delivery tag this channel never issued is
%% a PRECONDITION_FAILED channel error, not silent success (RabbitMQ).
ack_deliveries(Channel, Tag, Multiple, Mode, MethodId, St0) ->
    Ch = ch(Channel, St0),
    {Do, Keep, N} = maps:fold(
                      fun(T, D, {Yes, No, Count}) ->
                              case Multiple of
                                  true when T =< Tag -> {[{T, D} | Yes], No, Count + 1};
                                  false when T =:= Tag -> {[{T, D} | Yes], No, Count + 1};
                                  _ -> {Yes, No#{T => D}, Count}
                              end
                      end, {[], #{}, 0}, Ch#ch.deliveries),
    case N of
        0 ->
            send_channel_error(Channel, 406,
                               <<"PRECONDITION_FAILED - unknown delivery tag">>,
                               60, MethodId, St0);
        _ ->
            {Red, St1} =
                lists:foldl(
                  fun({_T, {Ref, P, O}}, {Acc, StAcc}) ->
                          case Mode of
                              ack ->
                                  _ = (catch broker_subscription:ack(Ref, P, O,
                                                                     StAcc#st.client_id)),
                                  %% An acked position can never be redelivered on
                                  %% this connection (queue offsets are unique per
                                  %% append), so drop its marker. Keeping it made
                                  %% `redelivered' grow by one entry per message
                                  %% ever delivered -- an unbounded leak on a
                                  %% long-lived channel.
                                  {maps:remove({P, O}, Acc), StAcc};
                              release ->
                                  _ = (catch broker_subscription:release(Ref, P, [O],
                                                                         StAcc#st.client_id)),
                                  {Acc#{{P, O} => true}, StAcc}
                          end
                  end, {St0#st.redelivered, St0}, Do),
            put_ch(Channel, Ch#ch{deliveries = Keep},
                   St1#st{redelivered = Red})
    end.

mark_redelivered(P, O, St) ->
    St#st{redelivered = (St#st.redelivered)#{{P, O} => true}}.

%%%_* Wire helpers ---------------------------------------------------------

%% S12-B (ADR-032 D7): the node-level gate, asked once, and the refusal that
%% follows it.
%%
%% AMQP 0-9-1 has no "server unavailable" reply code of its own; the closest
%% honest one is 541 `internal-error` -- the connection-level close that says
%% "do not retry this connection". The text says what actually happened, because
%% that is the only place an operator can see a capacity refusal (D7 keeps
%% rejected connections out of the metric set).
refuse_full(St) ->
    %% connection.close args are `reply-code:short, reply-text:shortstr,
    %% class-id:short, method-id:short' -- not a pair of shortstrs. A client
    %% decodes the first field as an integer, so getting this wrong would hand it
    %% a reply code of 0x0E49 instead of 541. (The authentication-failure path in
    %% `handle_method/5' predates this and is tracked separately as RQ-20.)
    Args = iolist_to_binary(
             [<<541:16>>,
              amqp_codec:encode_shortstr(<<"server is at connection capacity">>),
              <<0:16, 0:16>>]),
    _ = send_method(0, 10, 50, Args, St#st{phase = start}),
    {stop, normal, St#st{phase = start}}.

admit(#st{admission = Lease} = St) when Lease =/= undefined ->
    {ok, St};
admit(St) ->
    case broker_admission:try_acquire() of
        {rejected, Why} ->
            logger:warning("amqp: connection refused by admission gate: ~p",
                           [Why]),
            {error, St};
        Lease ->
            {ok, St#st{admission = Lease}}
    end.

send_tune(St) ->
    %% The third field is the heartbeat the server proposes (seconds). 0 means
    %% "no heartbeat"; a client may lower it in tune-ok but not raise it.
    Args = <<2047:16, ?FRAME_MAX:32, (server_heartbeat()):16>>,
    {noreply, send_method(0, 10, 30, Args, St)}.

server_heartbeat() ->
    application:get_env(broker_proto_amqp, heartbeat, ?DEFAULT_HEARTBEAT).

send_method(Channel, Class, Method, Args, St) ->
    send_frame(?FRAME_METHOD, Channel, amqp_codec:encode_method(Class, Method, Args), St).

send_content(Channel, Body, Props, St) ->
    Header = amqp_codec:encode_content_header(byte_size(Body), Props),
    St1 = send_frame(?FRAME_HEADER, Channel, Header, St),
    send_body_frames(Channel, Body, St1).

send_body_frames(_Channel, <<>>, St) ->
    St;
send_body_frames(Channel, Body, St) ->
    Max = max(1, St#st.frame_max - 8),
    case byte_size(Body) =< Max of
        true ->
            send_frame(?FRAME_BODY, Channel, Body, St);
        false ->
            <<Chunk:Max/binary, Rest/binary>> = Body,
            St1 = send_frame(?FRAME_BODY, Channel, Chunk, St),
            send_body_frames(Channel, Rest, St1)
    end.

send_frame(Type, Channel, Payload, #st{socket = undefined} = St) ->
    _ = {Type, Channel, Payload},
    St;
send_frame(Type, Channel, Payload, #st{transport = T, socket = S} = St) ->
    _ = T:send(S, amqp_codec:encode_frame(Type, Channel, Payload)),
    St.

set_active(#st{transport = T, socket = S}) ->
    T:setopts(S, [{active, once}]).

%% Channel-level exception: close just this channel (§2.3.7 soft errors).
send_channel_error(Channel, Code, Text, Class, Method, St) ->
    Args = <<Code:16, (amqp_codec:encode_shortstr(Text))/binary,
             Class:16, Method:16>>,
    drop_channel(Channel, send_method(Channel, 20, 40, Args, St)).

channel_error(Channel, Code, Text, Class, Method, St) ->
    {noreply, send_channel_error(Channel, Code, Text, Class, Method, St)}.

%% Connection-level exception: close the whole connection (§2.3.8 hard
%% errors). RQ-20: reply-code is a SHORT, followed by shortstr + class-id +
%% method-id -- the old build wrote the code through encode_shortstr.
connection_error(Code, Text, Class, Method, St) ->
    Args = <<Code:16, (amqp_codec:encode_shortstr(Text))/binary,
             Class:16, Method:16>>,
    _ = send_method(0, 10, 50, Args, St),
    {stop, normal, St}.

%%%_* State helpers --------------------------------------------------------

ch(Channel, St) ->
    maps:get(Channel, St#st.channels, #ch{}).

put_ch(Channel, Ch, St) ->
    St#st{channels = (St#st.channels)#{Channel => Ch}}.

drop_channel(Channel, St) ->
    St#st{channels = maps:remove(Channel, St#st.channels)}.

consumer_count(Queue, St) ->
    length([1 || {_, #ch{consumers = Cs}} <- maps:to_list(St#st.channels),
                 #{queue := Q} <- maps:values(Cs), Q =:= Queue]).

ensure_client(#st{client_id = undefined} = St) ->
    ClientId = <<"amqp-", (integer_to_binary(erlang:unique_integer([positive])))/binary>>,
    {ok, _Prev} = broker_subscription:attach(ClientId, self(), false),
    St#st{client_id = ClientId};
ensure_client(St) ->
    St.

queue_filter(Queue) ->
    <<"amqp/", Queue/binary>>.

arm_tick(#st{} = St) ->
    case St#st.tick of
        undefined -> St#st{tick = erlang:send_after(tick_ms(), self(), pull_tick)};
        _ -> St
    end.

tick_ms() ->
    application:get_env(broker_proto_amqp, poll_ms, ?DEFAULT_TICK).

message_id() ->
    base64:encode(crypto:strong_rand_bytes(12)).

reason(Reason) when is_binary(Reason) -> Reason;
reason(Reason) -> iolist_to_binary(io_lib:format("~p", [Reason])).

%% A wire bit as an integer. Every call site passes what `get_bits/2' (or a
%% boolean expression) produces, so the contract is boolean-only; an integer
%% inlet would be a bug at the call site, not something to paper over here.
bitval(true) -> 1;
bitval(false) -> 0.

cleanup(#st{} = St) ->
    cancel_tick(St),
    _ = cancel_hb_timer(St),
    lists:foreach(
      fun({Channel, #ch{} = Ch}) ->
              lists:foreach(
                fun({_T, {Ref, P, O}}) ->
                        _ = (catch broker_subscription:release(Ref, P, [O], St#st.client_id))
                end, maps:to_list(Ch#ch.deliveries)),
              lists:foreach(
                fun({_T, #{ref := Ref}}) ->
                        _ = (catch broker_subscription:leave(Ref, St#st.client_id))
                end, maps:to_list(Ch#ch.consumers)),
              _ = Channel
      end, maps:to_list(St#st.channels)),
    case St#st.client_id of
        undefined -> ok;
        ClientId -> _ = (catch broker_subscription:detach(ClientId, self())), ok
    end.

arm_heartbeat(#st{heartbeat = 0} = St) ->
    %% Heartbeats disabled: no watchdog, nothing to do on inbound data.
    St;
arm_heartbeat(#st{heartbeat = Hb} = St) ->
    _ = cancel_hb_timer(St),
    St#st{hb_ref = erlang:send_after(Hb, self(), heartbeat_tick),
          hb_misses = 0}.

%% §4.2.7 watchdog. The timer fires every `heartbeat' ms, but ONLY inbound
%% traffic restarts it (see `on_data/2'). So a tick means "we have heard nothing
%% from the peer for a full interval":
%%   * first silent tick  -> answer with a heartbeat frame, remember the miss;
%%   * second silent tick -> 2*Hb of silence: the peer is gone, stop normal.
hb_tick(#st{heartbeat = _Hb, hb_misses = N} = St) when N >= 1 ->
    {stop, normal, St};
hb_tick(#st{heartbeat = _Hb, hb_misses = 0, socket = undefined} = St) ->
    %% Socket already gone (the tcp_closed handler will reap); don't touch it.
    {noreply, St};
hb_tick(#st{heartbeat = Hb} = St) ->
    %% First silent interval: send a heartbeat and re-arm WITHOUT clearing the
    %% miss, so the next tick still has the evidence of this one.
    _ = send_frame(?FRAME_HEARTBEAT, 0, <<>>, St),
    {noreply, St#st{hb_ref = erlang:send_after(Hb, self(), heartbeat_tick),
                    hb_misses = 1}}.

%% Cancel the watchdog timer, returning the state untouched so callers can thread
%% it (unlike a bare `cancel_timer/1', whose `ok' would poison a record update).
cancel_hb_timer(#st{hb_ref = undefined}) -> ok;
cancel_hb_timer(#st{hb_ref = Ref}) -> _ = erlang:cancel_timer(Ref), ok.

cancel_tick(#st{tick = undefined}) -> ok;
cancel_tick(#st{tick = Ref}) -> _ = erlang:cancel_timer(Ref), ok.
